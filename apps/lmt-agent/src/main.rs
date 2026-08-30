use anyhow::{Context, bail};
use clap::Parser;
use lmt_core::{AttemptState, FailureKind, ProcessRunSpec};
use lmt_protocol::v1alpha1::{AgentAction, Capacity, EventRequest, PollRequest, PollResponse};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    fs,
    io::AsyncWriteExt,
    process::Command,
    sync::{Mutex, watch},
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "/etc/lmt/agent.toml")]
    config: PathBuf,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node: Node,
    server: Server,
    storage: Storage,
    execution: Execution,
    runner: Runner,
}
#[derive(Clone, Deserialize)]
struct Node {
    name: String,
}
#[derive(Clone, Deserialize)]
struct Server {
    url: String,
    token_file: PathBuf,
}
#[derive(Clone, Deserialize)]
struct Storage {
    mirror_root: PathBuf,
    spool_dir: PathBuf,
}
#[derive(Clone, Deserialize)]
struct Execution {
    max_concurrent_runs: u32,
}
#[derive(Clone, Deserialize)]
struct Runner {
    process: Policy,
}
#[derive(Clone, Deserialize)]
struct Policy {
    enabled: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Spool {
    run_id: String,
    attempt: u32,
    spec_hash: String,
    spec: ProcessRunSpec,
    state: AttemptState,
    sequence: u64,
    accepted_at: Option<String>,
    started_at: Option<String>,
    finished_at: Option<String>,
    exit_code: Option<i32>,
    failure_kind: Option<FailureKind>,
    failure_message: Option<String>,
    log_offset: u64,
    acknowledged: bool,
}
#[derive(Clone)]
struct Agent {
    config: Config,
    token: Arc<str>,
    instance: Arc<str>,
    client: Client,
    active: Arc<Mutex<HashSet<String>>>,
    shutdown: watch::Receiver<bool>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().json().with_env_filter("info").init();
    let source = fs::read_to_string(Args::parse().config).await?;
    let config: Config = toml::from_str(&source)?;
    lmt_core::NodeName::new(&config.node.name).context("invalid node.name")?;
    if config.execution.max_concurrent_runs == 0 {
        bail!("max_concurrent_runs must be positive")
    }
    fs::create_dir_all(&config.storage.spool_dir).await?;
    let token = fs::read_to_string(&config.server.token_file).await?.trim().to_owned();
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        signal().await;
        let _ = tx.send(true);
    });
    let agent = Agent {
        config,
        token: token.into(),
        instance: ulid::Ulid::new().to_string().into(),
        client: Client::builder().timeout(Duration::from_secs(35)).build()?,
        active: Arc::new(Mutex::new(HashSet::new())),
        shutdown: rx,
    };
    agent.recover().await;
    agent.poll().await
}
impl Agent {
    async fn recover(&self) {
        let Ok(mut entries) = fs::read_dir(&self.config.storage.spool_dir).await else {
            return;
        };
        while let Ok(Some(e)) = entries.next_entry().await {
            if e.path().extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            let Ok(mut s) = read(&e.path()).await else { continue };
            if !s.state.is_terminal() {
                terminal(
                    &mut s,
                    AttemptState::Interrupted,
                    None,
                    Some(FailureKind::Interrupted),
                    "agent restarted",
                );
                let _ = write(&e.path(), &s).await;
            }
            let _ = self.reconcile(&e.path(), &mut s).await;
        }
    }
    async fn poll(&self) -> anyhow::Result<()> {
        let mut sequence = 0;
        while !*self.shutdown.borrow() {
            self.resend_durable_results().await;
            sequence += 1;
            let request = PollRequest {
                protocol_version: "v1alpha1".into(),
                agent_version: env!("CARGO_PKG_VERSION").into(),
                agent_instance_id: self.instance.to_string(),
                poll_sequence: sequence,
                running: vec![],
                capacity: Capacity {
                    mirror_root_free_bytes: None,
                    active_runs: self.active.lock().await.len() as u32,
                },
                mirror_root: self.config.storage.mirror_root.to_string_lossy().into_owned(),
            };
            match self
                .client
                .post(format!("{}/api/v1alpha1/agent/poll", self.config.server.url))
                .bearer_auth(self.token.as_ref())
                .json(&request)
                .send()
                .await
            {
                Ok(r) if r.status() == StatusCode::NO_CONTENT => {}
                Ok(r) if r.status().is_success() => {
                    for action in r.json::<PollResponse>().await?.actions {
                        if let AgentAction::StartAttempt {
                            run_id,
                            attempt,
                            spec_hash,
                            spec,
                        } = action
                        {
                            self.start(run_id, attempt, spec_hash, spec).await;
                        }
                    }
                }
                Ok(r) => tracing::warn!(status=%r.status(),"poll rejected"),
                Err(e) => tracing::warn!(error=%e,"poll failed"),
            };
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }

    async fn resend_durable_results(&self) {
        let Ok(mut entries) = fs::read_dir(&self.config.storage.spool_dir).await else {
            return;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            if let Ok(mut spool) = read(&entry.path()).await
                && spool.state.is_terminal()
                && (!spool.acknowledged || fs::metadata(entry.path().with_extension("log")).await.is_ok())
            {
                let _ = self.reconcile(&entry.path(), &mut spool).await;
            }
        }
    }
    async fn start(&self, run_id: String, attempt: u32, spec_hash: String, spec: ProcessRunSpec) {
        let key = format!("{run_id}-{attempt}");
        let path = self.config.storage.spool_dir.join(format!("{key}.json"));
        if let Ok(mut old) = read(&path).await {
            if old.spec_hash != spec_hash {
                terminal(
                    &mut old,
                    AttemptState::Rejected,
                    None,
                    Some(FailureKind::Rejected),
                    "conflicting spec hash",
                );
                let _ = write(&path, &old).await;
            }
            let _ = self.reconcile(&path, &mut old).await;
            return;
        }
        let mut active = self.active.lock().await;
        if active.len() >= self.config.execution.max_concurrent_runs as usize {
            return;
        }
        if !self.config.runner.process.enabled || !safe(&self.config.storage.mirror_root, &spec) {
            drop(active);
            let mut s = spool(run_id, attempt, spec_hash, spec);
            terminal(
                &mut s,
                AttemptState::Rejected,
                None,
                Some(FailureKind::Rejected),
                "local policy rejected spec",
            );
            let _ = write(&path, &s).await;
            let _ = self.reconcile(&path, &mut s).await;
            return;
        }
        active.insert(key.clone());
        drop(active);
        let agent = self.clone();
        tokio::spawn(async move {
            let mut s = spool(run_id, attempt, spec_hash, spec);
            s.state = AttemptState::Accepted;
            s.sequence = 1;
            s.accepted_at = Some(now());
            if write(&path, &s).await.is_ok() {
                let _ = agent.report(&s).await;
                agent.execute(&path, &mut s).await;
                let _ = agent.reconcile(&path, &mut s).await;
            }
            agent.active.lock().await.remove(&key);
        });
    }
    async fn execute(&self, path: &Path, s: &mut Spool) {
        let mut command = Command::new(&s.spec.program);
        command.args(&s.spec.args).kill_on_drop(true);
        if let Some(cwd) = &s.spec.cwd {
            command.current_dir(cwd);
        }
        let child = match command.spawn() {
            Ok(c) => c,
            Err(e) => {
                terminal(
                    s,
                    AttemptState::Failed,
                    None,
                    Some(FailureKind::Process),
                    &e.to_string(),
                );
                let _ = write(path, s).await;
                return;
            }
        };
        s.state = AttemptState::Running;
        s.sequence = 2;
        s.started_at = Some(now());
        let _ = write(path, s).await;
        let _ = self.report(s).await;
        let result = tokio::time::timeout(Duration::from_secs(s.spec.timeout_seconds), child.wait_with_output()).await;
        match result {
            Ok(Ok(o)) => {
                let mut log = Vec::new();
                if !o.stdout.is_empty() {
                    log.extend_from_slice(b"[stdout] ");
                    log.extend_from_slice(&o.stdout)
                }
                if !o.stderr.is_empty() {
                    log.extend_from_slice(b"[stderr] ");
                    log.extend_from_slice(&o.stderr)
                }
                let _ = fs::write(path.with_extension("log"), log).await;
                if o.status.success() {
                    terminal(s, AttemptState::Succeeded, o.status.code(), None, "")
                } else {
                    terminal(
                        s,
                        AttemptState::Failed,
                        o.status.code(),
                        Some(FailureKind::Process),
                        "process exited non-zero",
                    )
                }
            }
            Ok(Err(e)) => terminal(
                s,
                AttemptState::Interrupted,
                None,
                Some(FailureKind::Interrupted),
                &e.to_string(),
            ),
            Err(_) => terminal(
                s,
                AttemptState::TimedOut,
                None,
                Some(FailureKind::Timeout),
                "attempt timed out",
            ),
        };
        let _ = write(path, s).await;
    }
    async fn reconcile(&self, path: &Path, s: &mut Spool) -> anyhow::Result<()> {
        if let Ok(log) = fs::read(path.with_extension("log")).await {
            if (s.log_offset as usize) < log.len() {
                let r = self
                    .client
                    .put(format!(
                        "{}/api/v1alpha1/agent/attempts/{}/{}/log",
                        self.config.server.url, s.run_id, s.attempt
                    ))
                    .bearer_auth(self.token.as_ref())
                    .header("x-lmt-log-offset", s.log_offset)
                    .header("x-lmt-log-complete", s.state.is_terminal().to_string())
                    .body(log[s.log_offset as usize..].to_vec())
                    .send()
                    .await?;
                if r.status().is_success() {
                    s.log_offset = r
                        .headers()
                        .get("x-lmt-log-next-offset")
                        .and_then(|x| x.to_str().ok())
                        .and_then(|x| x.parse().ok())
                        .unwrap_or(s.log_offset);
                    write(path, s).await?;
                }
            }
        }
        if s.sequence > 0 && !s.acknowledged && self.report(s).await.is_ok() && s.state.is_terminal() {
            s.acknowledged = true;
            write(path, s).await?;
        }
        Ok(())
    }
    async fn report(&self, s: &Spool) -> anyhow::Result<()> {
        let e = EventRequest {
            event_sequence: s.sequence,
            state: s.state,
            agent_instance_id: self.instance.to_string(),
            accepted_at: s.accepted_at.clone(),
            started_at: s.started_at.clone(),
            finished_at: s.finished_at.clone(),
            exit_code: s.exit_code,
            failure_kind: s.failure_kind,
            failure_message: s.failure_message.clone(),
        };
        self.client
            .post(format!(
                "{}/api/v1alpha1/agent/attempts/{}/{}/events",
                self.config.server.url, s.run_id, s.attempt
            ))
            .bearer_auth(self.token.as_ref())
            .json(&e)
            .send()
            .await?
            .error_for_status()
            .context("event rejected")?;
        Ok(())
    }
}
fn spool(run_id: String, attempt: u32, spec_hash: String, spec: ProcessRunSpec) -> Spool {
    Spool {
        run_id,
        attempt,
        spec_hash,
        spec,
        state: AttemptState::Queued,
        sequence: 0,
        accepted_at: None,
        started_at: None,
        finished_at: None,
        exit_code: None,
        failure_kind: None,
        failure_message: None,
        log_offset: 0,
        acknowledged: false,
    }
}
fn terminal(s: &mut Spool, state: AttemptState, code: Option<i32>, kind: Option<FailureKind>, message: &str) {
    s.state = state;
    s.sequence += 1;
    s.finished_at = Some(now());
    s.exit_code = code;
    s.failure_kind = kind;
    s.failure_message = (!message.is_empty()).then(|| message.into())
}
fn safe(root: &Path, s: &ProcessRunSpec) -> bool {
    s.runner == "process"
        && s.mirror_root == root.to_string_lossy()
        && Path::new(&s.target_dir).starts_with(root)
        && s.cwd.as_ref().is_none_or(|c| Path::new(c).starts_with(root))
}
async fn read(path: &Path) -> anyhow::Result<Spool> {
    Ok(serde_json::from_slice(&fs::read(path).await?)?)
}
async fn write(path: &Path, s: &Spool) -> anyhow::Result<()> {
    let temp = path.with_extension("tmp");
    let mut f = fs::File::create(&temp).await?;
    f.write_all(&serde_json::to_vec(s)?).await?;
    f.sync_all().await?;
    fs::rename(temp, path).await?;
    Ok(())
}
fn now() -> String {
    OffsetDateTime::now_utc().format(&Rfc3339).expect("format")
}
async fn signal() {
    let c = async { tokio::signal::ctrl_c().await.expect("signal") };
    let t = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal")
            .recv()
            .await;
    };
    tokio::select! { () = c => {}, () = t => {} }
}
