#![cfg(target_os = "linux")]

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::any,
};
use lmt_core::{BundleFile, RunState, RunTrigger};
use lmt_protocol::v1alpha1::{
    AgentAction, ApplyRequest, ApplyResponse, BundleRequest, CredentialIssueRequest, CredentialIssueResponse,
    CredentialView, ManualRunRequest, PlanResponse, PollResponse, RunDetail, RunView,
};
use lmt_server::{AgentCredential, AppState, Clock, ServerConfig, build_router, initialize, initialize_with_clock};
use nix::{
    sys::signal::{Signal, kill, killpg},
    unistd::Pid,
};
use reqwest::Client;
use tempfile::TempDir;
use tokio::{
    fs,
    net::TcpListener,
    process::{Child, Command},
    task::JoinHandle,
};

const OPERATOR_TOKEN: &str = "operator-secret";
const AGENT_TOKEN: &str = "agent-secret";

struct Harness {
    directory: TempDir,
    server_config: ServerConfig,
    server_address: SocketAddr,
    server_task: Option<JoinHandle<()>>,
    server_state: Option<AppState>,
    proxy_task: JoinHandle<()>,
    client: Client,
    agent_config: PathBuf,
    agent_token: PathBuf,
    agent: Option<Child>,
    files: BTreeMap<String, String>,
    drop_accepted: Arc<AtomicUsize>,
    lose_terminal_ack: Arc<AtomicUsize>,
    drop_start_response: Arc<AtomicUsize>,
    clock: Option<Arc<TestClock>>,
}

#[derive(Clone)]
struct ProxyState {
    upstream: String,
    client: Client,
    drop_accepted: Arc<AtomicUsize>,
    lose_terminal_ack: Arc<AtomicUsize>,
    drop_start_response: Arc<AtomicUsize>,
}

struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Harness {
    async fn new() -> Self {
        Self::new_inner(None).await
    }

    async fn with_clock(now_ms: i64) -> Self {
        Self::new_inner(Some(Arc::new(TestClock(AtomicI64::new(now_ms))))).await
    }

    async fn new_inner(clock: Option<Arc<TestClock>>) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let server_listener = TcpListener::bind("127.0.0.1:0").await.expect("server bind");
        let server_address = server_listener.local_addr().expect("server address");
        drop(server_listener);
        let server_config = ServerConfig {
            bind: server_address.to_string(),
            database_path: directory.path().join("server/lmt.db"),
            log_dir: directory.path().join("server/logs"),
            operator_token: Some(OPERATOR_TOKEN.into()),
            operator_token_file: None,
            offline_after_seconds: 2,
            agents: vec![AgentCredential {
                node: "node-a".into(),
                token: AGENT_TOKEN.into(),
            }],
        };
        let (server_task, server_state) = start_server(&server_config, server_address, clock.clone()).await;
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let proxy_address = proxy_listener.local_addr().expect("proxy address");
        let drop_accepted = Arc::new(AtomicUsize::new(2));
        let lose_terminal_ack = Arc::new(AtomicUsize::new(1));
        let drop_start_response = Arc::new(AtomicUsize::new(0));
        let proxy_state = ProxyState {
            upstream: format!("http://{server_address}"),
            client: Client::new(),
            drop_accepted: drop_accepted.clone(),
            lose_terminal_ack: lose_terminal_ack.clone(),
            drop_start_response: drop_start_response.clone(),
        };
        let proxy_task = tokio::spawn(async move {
            axum::serve(
                proxy_listener,
                Router::new().fallback(any(proxy)).with_state(proxy_state),
            )
            .await
            .expect("proxy");
        });
        let proxy_url = format!("http://{proxy_address}");
        let token_path = directory.path().join("agent.token");
        fs::write(&token_path, AGENT_TOKEN).await.expect("token");
        let spool = directory.path().join("agent/spool");
        let mirror_root = directory.path().join("mirrors");
        fs::create_dir_all(&mirror_root).await.expect("mirror root");
        let agent_config = directory.path().join("agent.toml");
        fs::write(&agent_config, format!("[node]\nname='node-a'\n[server]\nurl='{proxy_url}'\ntoken_file='{}'\n[storage]\nmirror_root='{}'\nspool_dir='{}'\n[execution]\nmax_concurrent_runs=4\n[runner.process]\nenabled=true\n",
            token_path.display(), mirror_root.display(), spool.display())).await.expect("agent config");
        Self {
            directory,
            server_config,
            server_address,
            server_task: Some(server_task),
            server_state: Some(server_state),
            proxy_task,
            client: Client::new(),
            agent_config,
            agent_token: token_path,
            agent: None,
            files: BTreeMap::new(),
            drop_accepted,
            lose_terminal_ack,
            drop_start_response,
            clock,
        }
    }

    fn start_agent(&mut self) {
        self.agent = Some(
            Command::new(env!("CARGO_BIN_EXE_lmt-agent"))
                .arg("--config")
                .arg(&self.agent_config)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .expect("start agent"),
        );
    }

    async fn stop_agent(&mut self) {
        if let Some(mut agent) = self.agent.take() {
            let _ = agent.start_kill();
            let _ = agent.wait().await;
        }
    }

    async fn stop_server(&mut self) {
        if let Some(task) = self.server_task.take() {
            task.abort();
            let _ = task.await;
        }
        self.server_state = None;
    }
    async fn restart_server(&mut self) {
        let (task, state) = start_server(&self.server_config, self.server_address, self.clock.clone()).await;
        self.server_task = Some(task);
        self.server_state = Some(state);
    }

    async fn set_mirror(&mut self, name: &str, program: &str, args: &[String], timeout: u64) {
        let arguments = args
            .iter()
            .map(|arg| toml::Value::String(arg.clone()).to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.files.insert(format!("nodes/node-a/mirrors/{name}.toml"), format!(
            "[mirror]\nname='{name}'\ntarget='{name}'\n[sync]\ntype='command'\nprogram='{program}'\nargs=[{arguments}]\n[run]\ntimeout_seconds={timeout}\nmax_attempts=1\n"));
        self.apply().await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn set_command_policy(
        &mut self,
        name: &str,
        program: &str,
        args: &[String],
        timeout: u64,
        max_attempts: u32,
        retry_delay_seconds: u64,
        enabled: bool,
    ) {
        let arguments = args
            .iter()
            .map(|arg| toml::Value::String(arg.clone()).to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.files.insert(
            format!("nodes/node-a/mirrors/{name}.toml"),
            format!(
                "[mirror]\nname='{name}'\nenabled={enabled}\ntarget='{name}'\n[sync]\ntype='command'\nprogram='{program}'\nargs=[{arguments}]\n[run]\ntimeout_seconds={timeout}\nmax_attempts={max_attempts}\nretry_delay_seconds={retry_delay_seconds}\n"
            ),
        );
        self.apply().await;
    }

    async fn set_rsync_mirror(&mut self, name: &str, source: &Path) {
        self.files.insert(
            format!("nodes/node-a/mirrors/{name}.toml"),
            format!(
                "[mirror]\nname='{name}'\ntarget='{name}'\n[sync]\ntype='rsync'\nsource='{}/'\nargs=['--archive']\n",
                source.display()
            ),
        );
        self.apply().await;
    }

    async fn set_scheduled_mirror(&mut self, name: &str, program: &str, args: &[String]) {
        let arguments = args
            .iter()
            .map(|arg| toml::Value::String(arg.clone()).to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.files.insert(
            format!("nodes/node-a/mirrors/{name}.toml"),
            format!(
                "[mirror]\nname='{name}'\ntarget='{name}'\n[schedule]\ninterval='1m'\n[sync]\ntype='command'\nprogram='{program}'\nargs=[{arguments}]\n[run]\ntimeout_seconds=10\nmax_attempts=1\n"
            ),
        );
        self.apply().await;
    }

    async fn remove_mirror(&mut self, name: &str) {
        self.files.remove(&format!("nodes/node-a/mirrors/{name}.toml"));
        self.apply().await;
    }

    async fn apply(&self) {
        let files = self
            .files
            .iter()
            .map(|(path, contents)| BundleFile {
                path: path.clone(),
                contents: contents.clone(),
            })
            .collect::<Vec<_>>();
        let plan: PlanResponse = self
            .checked(
                self.client
                    .post(format!("{}/api/v1alpha1/config/plan", self.server_url()))
                    .bearer_auth(OPERATOR_TOKEN)
                    .json(&BundleRequest { files: files.clone() })
                    .send()
                    .await
                    .expect("plan request"),
            )
            .await
            .json()
            .await
            .expect("plan");
        let _: ApplyResponse = self
            .checked(
                self.client
                    .post(format!("{}/api/v1alpha1/config/apply", self.server_url()))
                    .bearer_auth(OPERATOR_TOKEN)
                    .json(&ApplyRequest {
                        files,
                        base_revision: plan.base_revision,
                        acknowledge_moves: true,
                    })
                    .send()
                    .await
                    .expect("apply request"),
            )
            .await
            .json()
            .await
            .expect("apply");
    }

    async fn run(&self, mirror: &str) -> String {
        let detail: serde_json::Value = self
            .checked(
                self.client
                    .post(format!("{}/api/v1alpha1/mirrors/{mirror}/runs", self.server_url()))
                    .bearer_auth(OPERATOR_TOKEN)
                    .json(&ManualRunRequest {
                        request_id: ulid::Ulid::new().to_string(),
                        trigger: RunTrigger::Manual,
                    })
                    .send()
                    .await
                    .expect("run request"),
            )
            .await
            .json()
            .await
            .expect("run");
        detail["id"].as_str().expect("run id").to_owned()
    }

    async fn wait_state(&self, id: &str, expected: RunState) -> RunDetail {
        let mut observed = None;
        for _ in 0..150 {
            if let Ok(response) = self
                .client
                .get(format!("{}/api/v1alpha1/runs/{id}", self.server_url()))
                .bearer_auth(OPERATOR_TOKEN)
                .send()
                .await
            {
                if response.status().is_success() {
                    let detail: RunDetail = response.json().await.expect("detail");
                    if detail.run.state == expected {
                        return detail;
                    }
                    observed = Some((detail.run.state, detail.attempts));
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let spool_dir = self.directory.path().join("agent/spool");
        let spool = std::fs::read_dir(&spool_dir)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| (entry.path(), std::fs::read_to_string(entry.path()).ok()))
            .collect::<Vec<_>>();
        panic!("run {id} did not reach {expected:?}; last state was {observed:?}; spool={spool:?}")
    }

    async fn wait_scheduled_run(&self, mirror: &str, expected: RunState) -> RunDetail {
        for _ in 0..150 {
            let response = self
                .client
                .get(format!("{}/api/v1alpha1/runs", self.server_url()))
                .bearer_auth(OPERATOR_TOKEN)
                .send()
                .await
                .expect("runs request");
            if response.status().is_success()
                && let Some(run) = response
                    .json::<Vec<RunView>>()
                    .await
                    .expect("runs")
                    .into_iter()
                    .find(|run| run.mirror_name == mirror && run.trigger == RunTrigger::Scheduled)
                && run.state == expected
            {
                return self.detail(&run.id).await.expect("scheduled detail");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("scheduled run for {mirror} did not reach {expected:?}")
    }

    fn advance_clock(&self, now_ms: i64) {
        self.clock
            .as_ref()
            .expect("deterministic clock")
            .0
            .store(now_ms, Ordering::SeqCst);
        self.server_state.as_ref().expect("server state").wake_scheduler();
    }

    async fn detail(&self, id: &str) -> Option<RunDetail> {
        let response = self
            .client
            .get(format!("{}/api/v1alpha1/runs/{id}", self.server_url()))
            .bearer_auth(OPERATOR_TOKEN)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        Some(response.json().await.expect("run detail"))
    }

    async fn wait_retry_delay(&self, id: &str) -> RunDetail {
        for _ in 0..150 {
            if let Some(detail) = self.detail(id).await
                && detail.run.state == RunState::Running
                && detail.run.retry_due_at.is_some()
                && detail.attempts.len() == 1
                && detail.attempts[0].state.is_terminal()
            {
                return detail;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("run {id} did not enter retry delay")
    }

    async fn cancel(&self, id: &str) {
        self.checked(
            self.client
                .post(format!("{}/api/v1alpha1/runs/{id}/cancel", self.server_url()))
                .bearer_auth(OPERATOR_TOKEN)
                .send()
                .await
                .expect("cancel request"),
        )
        .await;
    }

    async fn issue_credential(&self, node: &str, label: &str) -> CredentialIssueResponse {
        self.checked(
            self.client
                .post(format!("{}/api/v1alpha1/nodes/{node}/credentials", self.server_url()))
                .bearer_auth(OPERATOR_TOKEN)
                .json(&CredentialIssueRequest {
                    label: Some(label.into()),
                })
                .send()
                .await
                .expect("issue credential"),
        )
        .await
        .json()
        .await
        .expect("credential response")
    }

    async fn credentials(&self, node: &str) -> Vec<CredentialView> {
        self.checked(
            self.client
                .get(format!("{}/api/v1alpha1/nodes/{node}/credentials", self.server_url()))
                .bearer_auth(OPERATOR_TOKEN)
                .send()
                .await
                .expect("list credentials"),
        )
        .await
        .json()
        .await
        .expect("credentials")
    }

    async fn revoke_credential(&self, node: &str, id: &str) {
        self.checked(
            self.client
                .post(format!(
                    "{}/api/v1alpha1/nodes/{node}/credentials/{id}/revoke",
                    self.server_url()
                ))
                .bearer_auth(OPERATOR_TOKEN)
                .send()
                .await
                .expect("revoke credential"),
        )
        .await;
    }

    fn reload_agent(&mut self) {
        let pid = self
            .agent
            .as_ref()
            .and_then(tokio::process::Child::id)
            .expect("Agent PID");
        kill(Pid::from_raw(i32::try_from(pid).expect("PID")), Signal::SIGHUP).expect("reload Agent");
    }

    async fn logs(&self, id: &str) -> (String, bool) {
        let response = self
            .checked(
                self.client
                    .get(format!("{}/api/v1alpha1/runs/{id}/logs?attempt=1", self.server_url()))
                    .bearer_auth(OPERATOR_TOKEN)
                    .send()
                    .await
                    .expect("logs"),
            )
            .await;
        let complete = response
            .headers()
            .get("x-lmt-log-complete")
            .and_then(|value| value.to_str().ok())
            == Some("true");
        (response.text().await.expect("log text"), complete)
    }

    async fn wait_spool_retired(&self, id: &str) {
        let state = self.directory.path().join("agent/spool").join(format!("{id}-1.json"));
        let log = state.with_extension("log");
        for _ in 0..50 {
            if !state.exists() && !log.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("acknowledged spool artifacts were not retired for {id}");
    }

    fn server_url(&self) -> String {
        format!("http://{}", self.server_address)
    }
    async fn checked(&self, response: reqwest::Response) -> reqwest::Response {
        let status = response.status();
        if status.is_success() {
            response
        } else {
            panic!("HTTP {status}: {}", response.text().await.unwrap_or_default())
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
        self.proxy_task.abort();
        if let Some(agent) = &mut self.agent {
            let _ = agent.start_kill();
        }
    }
}

async fn start_server(
    config: &ServerConfig,
    address: SocketAddr,
    clock: Option<Arc<TestClock>>,
) -> (JoinHandle<()>, AppState) {
    let state = if let Some(clock) = clock {
        initialize_with_clock(config, clock).await.expect("initialize server")
    } else {
        initialize(config).await.expect("initialize server")
    };
    let listener = TcpListener::bind(address).await.expect("bind server");
    let served_state = state.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, build_router(served_state)).await.expect("server");
    });
    (task, state)
}

async fn proxy(State(state): State<ProxyState>, request: Request) -> Response {
    let method = request.method().clone();
    let path = request
        .uri()
        .path_and_query()
        .map_or("/", |value| value.as_str())
        .to_owned();
    let headers = request.headers().clone();
    let body = to_bytes(request.into_body(), 2 * 1024 * 1024).await.unwrap_or_default();
    let event_state = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value["state"].as_str().map(str::to_owned));
    if event_state.as_deref() == Some("accepted")
        && state
            .drop_accepted
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| value.checked_sub(1))
            .is_ok()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let mut outgoing = state.client.request(method, format!("{}{}", state.upstream, path));
    for (name, value) in &headers {
        if name != "host" {
            outgoing = outgoing.header(name, value);
        }
    }
    let Ok(upstream) = outgoing.body(body).send().await else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    if event_state
        .as_deref()
        .is_some_and(|value| matches!(value, "succeeded" | "failed" | "timed_out" | "interrupted" | "rejected"))
        && state
            .lose_terminal_ack
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| value.checked_sub(1))
            .is_ok()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let status = upstream.status();
    let response_headers = upstream.headers().clone();
    let bytes = upstream.bytes().await.unwrap_or_default();
    if serde_json::from_slice::<PollResponse>(&bytes).is_ok_and(|response| {
        response
            .actions
            .iter()
            .any(|action| matches!(action, AgentAction::StartAttempt { .. }))
    }) && state
        .drop_start_response
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| value.checked_sub(1))
        .is_ok()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let mut response = (status, bytes).into_response();
    for (name, value) in &response_headers {
        response.headers_mut().insert(name, value.clone());
    }
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // Keeping the ordered fault matrix in one test makes shared-process cleanup reliable.
async fn m1_release_fault_matrix() {
    let mut harness = Harness::new().await;
    harness.start_agent();

    let counter = harness.directory.path().join("counter");
    harness
        .set_mirror(
            "success",
            "/bin/sh",
            &[
                "-c".into(),
                format!("echo run >> '{}'; echo success-out; sleep 1", counter.display()),
            ],
            10,
        )
        .await;
    let success = harness.run("success").await;
    harness.wait_state(&success, RunState::Succeeded).await;
    assert_eq!(
        fs::read_to_string(&counter).await.expect("counter").lines().count(),
        1,
        "duplicate dispatch executed twice"
    );
    assert!(harness.logs(&success).await.0.contains("success-out"));

    harness
        .set_mirror(
            "failure",
            "/bin/sh",
            &["-c".into(), "echo visible-out; echo visible-err >&2; exit 1".into()],
            10,
        )
        .await;
    let failure = harness.run("failure").await;
    harness.wait_state(&failure, RunState::Failed).await;
    let (failure_log, complete) = harness.logs(&failure).await;
    assert!(failure_log.contains("[stdout] visible-out"));
    assert!(failure_log.contains("[stderr] visible-err"));
    assert!(complete);
    harness.wait_spool_retired(&failure).await;

    harness
        .set_mirror(
            "server-crash",
            "/bin/sh",
            &["-c".into(), "sleep 2; echo survived-server".into()],
            10,
        )
        .await;
    let server_crash = harness.run("server-crash").await;
    harness.wait_state(&server_crash, RunState::Running).await;
    harness.stop_server().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    harness.restart_server().await;
    harness.wait_state(&server_crash, RunState::Succeeded).await;
    assert!(harness.logs(&server_crash).await.0.contains("survived-server"));

    harness.set_mirror("empty", "/bin/true", &[], 10).await;
    let empty = harness.run("empty").await;
    harness.wait_state(&empty, RunState::Succeeded).await;
    let (empty_log, complete) = harness.logs(&empty).await;
    assert!(empty_log.is_empty());
    assert!(complete);

    let leader = harness.directory.path().join("leader.pid");
    let descendant = harness.directory.path().join("descendant.pid");
    harness
        .set_mirror(
            "agent-crash",
            "/bin/sh",
            &[
                "-c".into(),
                format!(
                    "echo $$ > '{}'; sleep 30 & echo $! > '{}'; wait",
                    leader.display(),
                    descendant.display()
                ),
            ],
            60,
        )
        .await;
    let agent_crash = harness.run("agent-crash").await;
    harness.wait_state(&agent_crash, RunState::Running).await;
    for _ in 0..30 {
        if leader.exists() && descendant.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let group: i32 = fs::read_to_string(&leader)
        .await
        .expect("leader pid")
        .trim()
        .parse()
        .expect("leader number");
    let child: i32 = fs::read_to_string(&descendant)
        .await
        .expect("descendant pid")
        .trim()
        .parse()
        .expect("child number");
    harness.stop_agent().await;
    // This is the systemd-equivalent cleanup performed by KillMode=control-group after MainPID death.
    let _ = killpg(Pid::from_raw(group), Signal::SIGKILL);
    for _ in 0..30 {
        if kill(Pid::from_raw(child), None).is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        kill(Pid::from_raw(child), None).is_err(),
        "Agent crash left a descendant alive"
    );
    harness.start_agent();
    harness.wait_state(&agent_crash, RunState::Failed).await;

    harness.stop_agent().await;
    let sentinel = harness.directory.path().join("mirrors/offline/sentinel");
    fs::create_dir_all(sentinel.parent().expect("parent"))
        .await
        .expect("data");
    fs::write(&sentinel, "keep").await.expect("sentinel");
    harness.set_mirror("offline", "/bin/true", &[], 10).await;
    let offline = harness.run("offline").await;
    harness.remove_mirror("offline").await;
    harness.wait_state(&offline, RunState::Cancelled).await;
    assert_eq!(fs::read_to_string(sentinel).await.expect("sentinel"), "keep");

    let service =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packaging/systemd/lmt-agent.service"))
            .await
            .expect("unit");
    let unit_section = service.split("[Service]").next().expect("unit section");
    let service_section = service.split("[Service]").nth(1).expect("service section");
    assert!(service.contains("ProtectSystem=full"));
    assert!(service.contains("KillMode=control-group"));
    assert!(unit_section.contains("StartLimitIntervalSec="));
    assert!(unit_section.contains("StartLimitBurst="));
    assert!(!service_section.contains("StartLimitIntervalSec="));
    assert!(!service_section.contains("StartLimitBurst="));
    assert_eq!(
        harness.drop_accepted.load(Ordering::SeqCst),
        0,
        "Accepted reports were not fault-injected"
    );
    assert_eq!(
        harness.lose_terminal_ack.load(Ordering::SeqCst),
        0,
        "terminal acknowledgement was not lost"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn m2_release_fault_matrix() {
    let mut harness = Harness::new().await;
    harness.drop_accepted.store(0, Ordering::SeqCst);
    harness.lose_terminal_ack.store(0, Ordering::SeqCst);
    let config = fs::read_to_string(&harness.agent_config).await.expect("agent config");
    fs::write(
        &harness.agent_config,
        config.replace("max_concurrent_runs=4", "max_concurrent_runs=1"),
    )
    .await
    .expect("capacity config");

    harness
        .set_command_policy("pending-cancel", "/bin/true", &[], 10, 1, 0, true)
        .await;
    let pending = harness.run("pending-cancel").await;
    harness.cancel(&pending).await;
    harness.cancel(&pending).await;
    let pending = harness.wait_state(&pending, RunState::Cancelled).await;
    assert!(pending.attempts.is_empty(), "cancel before dispatch created an Attempt");

    harness.start_agent();
    let retry_counter = harness.directory.path().join("retry-counter");
    harness
        .set_command_policy(
            "retry-success",
            "/bin/sh",
            &[
                "-c".into(),
                format!(
                    "echo run >> '{}'; [ $(wc -l < '{}') -ge 2 ]",
                    retry_counter.display(),
                    retry_counter.display()
                ),
            ],
            10,
            2,
            1,
            true,
        )
        .await;
    let retry = harness.run("retry-success").await;
    let retry = harness.wait_state(&retry, RunState::Succeeded).await;
    assert_eq!(retry.attempts.len(), 2);
    assert_eq!(retry.attempts[0].state, lmt_core::AttemptState::Failed);
    assert_eq!(retry.attempts[1].state, lmt_core::AttemptState::Succeeded);

    harness
        .set_command_policy("retry-restart", "/bin/false", &[], 10, 2, 2, true)
        .await;
    let retry_restart = harness.run("retry-restart").await;
    harness.wait_retry_delay(&retry_restart).await;
    harness.stop_server().await;
    tokio::time::sleep(Duration::from_millis(2_300)).await;
    harness.restart_server().await;
    let retry_restart = harness.wait_state(&retry_restart, RunState::Failed).await;
    assert_eq!(
        retry_restart.attempts.len(),
        2,
        "retry deadline was lost across restart"
    );

    harness
        .set_command_policy("disable-retry", "/bin/false", &[], 10, 2, 2, true)
        .await;
    let disabled = harness.run("disable-retry").await;
    harness.wait_retry_delay(&disabled).await;
    harness
        .set_command_policy("disable-retry", "/bin/false", &[], 10, 2, 2, false)
        .await;
    let disabled = harness.wait_state(&disabled, RunState::Failed).await;
    tokio::time::sleep(Duration::from_millis(2_300)).await;
    assert_eq!(disabled.attempts.len(), 1, "disabled mirror dispatched a retry");

    let crash_leader = harness.directory.path().join("m2-crash-leader.pid");
    let crash_child = harness.directory.path().join("m2-crash-child.pid");
    harness
        .set_command_policy(
            "interrupted-retry",
            "/bin/sh",
            &[
                "-c".into(),
                format!(
                    "if [ {{attempt}} -eq 1 ]; then echo $$ > '{}'; sleep 30 & echo $! > '{}'; wait; fi",
                    crash_leader.display(),
                    crash_child.display()
                ),
            ],
            60,
            2,
            1,
            true,
        )
        .await;
    let interrupted = harness.run("interrupted-retry").await;
    harness.wait_state(&interrupted, RunState::Running).await;
    for _ in 0..50 {
        if crash_leader.exists() && crash_child.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let crash_group = fs::read_to_string(&crash_leader)
        .await
        .expect("crash leader")
        .trim()
        .parse()
        .expect("pid");
    harness.stop_agent().await;
    let _ = killpg(Pid::from_raw(crash_group), Signal::SIGKILL);
    harness.start_agent();
    let interrupted = harness.wait_state(&interrupted, RunState::Succeeded).await;
    assert_eq!(interrupted.attempts.len(), 2);
    assert_eq!(interrupted.attempts[0].state, lmt_core::AttemptState::Interrupted);

    harness
        .set_command_policy(
            "capacity-a",
            "/bin/sh",
            &["-c".into(), "sleep 2".into()],
            10,
            1,
            0,
            true,
        )
        .await;
    harness
        .set_command_policy("capacity-b", "/bin/true", &[], 10, 1, 0, true)
        .await;
    let capacity_a = harness.run("capacity-a").await;
    harness.wait_state(&capacity_a, RunState::Running).await;
    let capacity_b = harness.run("capacity-b").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let waiting = harness.detail(&capacity_b).await.expect("capacity run");
    assert_eq!(waiting.run.state, RunState::Pending);
    assert!(waiting.attempts.is_empty(), "full Agent received a new Start");
    harness.wait_state(&capacity_a, RunState::Succeeded).await;
    harness.wait_state(&capacity_b, RunState::Succeeded).await;

    let cancel_leader = harness.directory.path().join("cancel-leader.pid");
    let cancel_child = harness.directory.path().join("cancel-child.pid");
    harness
        .set_command_policy(
            "active-cancel",
            "/bin/sh",
            &[
                "-c".into(),
                format!(
                    "echo $$ > '{}'; sleep 30 & echo $! > '{}'; wait",
                    cancel_leader.display(),
                    cancel_child.display()
                ),
            ],
            60,
            1,
            0,
            true,
        )
        .await;
    let cancelled = harness.run("active-cancel").await;
    harness.wait_state(&cancelled, RunState::Running).await;
    for _ in 0..50 {
        if cancel_child.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let cancel_child_pid = fs::read_to_string(&cancel_child)
        .await
        .expect("cancel child")
        .trim()
        .parse()
        .expect("pid");
    harness.cancel(&cancelled).await;
    harness.cancel(&cancelled).await;
    harness.wait_state(&cancelled, RunState::Cancelled).await;
    for _ in 0..30 {
        if kill(Pid::from_raw(cancel_child_pid), None).is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        kill(Pid::from_raw(cancel_child_pid), None).is_err(),
        "cancel left descendant alive"
    );

    let rsync_source = harness.directory.path().join("rsync-source");
    fs::create_dir_all(&rsync_source).await.expect("rsync source");
    fs::write(rsync_source.join("payload"), b"local-rsync\n")
        .await
        .expect("payload");
    harness.set_rsync_mirror("rsync-local", &rsync_source).await;
    let rsync = harness.run("rsync-local").await;
    harness.wait_state(&rsync, RunState::Succeeded).await;
    assert_eq!(
        fs::read(harness.directory.path().join("mirrors/rsync-local/payload"))
            .await
            .expect("rsync destination"),
        b"local-rsync\n"
    );

    let metrics = harness
        .checked(
            harness
                .client
                .get(format!("{}/metrics", harness.server_url()))
                .send()
                .await
                .expect("metrics"),
        )
        .await
        .text()
        .await
        .expect("metrics body");
    assert!(metrics.contains("lmt_retries_scheduled_total"));
    assert!(metrics.contains("lmt_cancellations_total{outcome=\"dispatched\"}"));
    assert!(metrics.contains("lmt_nodes_online 1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deterministic_interval_schedule_reaches_real_agent_execution() {
    let mut harness = Harness::with_clock(0).await;
    harness.drop_accepted.store(0, Ordering::SeqCst);
    harness.lose_terminal_ack.store(0, Ordering::SeqCst);
    let counter = harness.directory.path().join("scheduled-counter");
    harness
        .set_scheduled_mirror(
            "scheduled",
            "/bin/sh",
            &["-c".into(), format!("echo run >> '{}'", counter.display())],
        )
        .await;
    harness.start_agent();

    assert!(!counter.exists(), "schedule ran before its Server-clock deadline");
    harness.advance_clock(60_000);
    let detail = harness.wait_scheduled_run("scheduled", RunState::Succeeded).await;

    assert_eq!(detail.run.trigger, RunTrigger::Scheduled);
    assert_eq!(detail.run.scheduled_for_at.as_deref(), Some("1970-01-01T00:01:00Z"));
    assert_eq!(detail.attempts.len(), 1);
    assert_eq!(fs::read_to_string(counter).await.expect("counter"), "run\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_start_response_followed_by_cancel_executes_nothing_and_retires_tombstone() {
    let mut harness = Harness::new().await;
    harness.drop_accepted.store(0, Ordering::SeqCst);
    harness.lose_terminal_ack.store(0, Ordering::SeqCst);
    let counter = harness.directory.path().join("must-not-run");
    harness
        .set_command_policy(
            "lost-start",
            "/bin/sh",
            &["-c".into(), format!("echo run >> '{}'", counter.display())],
            10,
            1,
            0,
            true,
        )
        .await;
    let run_id = harness.run("lost-start").await;
    harness.drop_start_response.store(1, Ordering::SeqCst);
    harness.start_agent();
    for _ in 0..100 {
        if harness.drop_start_response.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        harness.drop_start_response.load(Ordering::SeqCst),
        0,
        "StartAttempt response-loss fault was not injected"
    );

    harness.cancel(&run_id).await;
    let detail = harness.wait_state(&run_id, RunState::Cancelled).await;
    harness.wait_spool_retired(&run_id).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        !counter.exists(),
        "lost StartAttempt response still executed the command"
    );
    assert_eq!(detail.attempts.len(), 1);
    assert_eq!(detail.attempts[0].state, lmt_core::AttemptState::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_credential_rotation_and_reload_preserve_active_execution() {
    let mut harness = Harness::new().await;
    harness.drop_accepted.store(0, Ordering::SeqCst);
    harness.lose_terminal_ack.store(0, Ordering::SeqCst);
    let marker = harness.directory.path().join("rotation-running");
    harness
        .set_command_policy(
            "credential-rotation",
            "/bin/sh",
            &["-c".into(), format!("touch '{}'; sleep 2", marker.display())],
            10,
            1,
            0,
            true,
        )
        .await;
    harness.start_agent();
    let run_id = harness.run("credential-rotation").await;
    harness.wait_state(&run_id, RunState::Running).await;
    for _ in 0..50 {
        if marker.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(marker.exists(), "long Run did not start");

    let issued = harness.issue_credential("node-a", "rotation-e2e").await;
    let temporary = harness.agent_token.with_extension("new");
    fs::write(&temporary, format!("{}\n", issued.token))
        .await
        .expect("new token");
    fs::rename(&temporary, &harness.agent_token)
        .await
        .expect("atomic token replacement");
    harness.reload_agent();
    let mut used = false;
    for _ in 0..100 {
        used = harness
            .credentials("node-a")
            .await
            .into_iter()
            .find(|credential| credential.id == issued.credential.id)
            .is_some_and(|credential| credential.last_used_at.is_some());
        if used {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(used, "Agent did not authenticate with reloaded credential");
    harness.revoke_credential("node-a", "bootstrap").await;

    harness.wait_state(&run_id, RunState::Succeeded).await;
    let follow_up = harness.run("credential-rotation").await;
    harness.wait_state(&follow_up, RunState::Succeeded).await;
    let bootstrap = harness
        .credentials("node-a")
        .await
        .into_iter()
        .find(|credential| credential.id == "bootstrap")
        .expect("bootstrap history");
    assert!(bootstrap.revoked_at.is_some());
}
