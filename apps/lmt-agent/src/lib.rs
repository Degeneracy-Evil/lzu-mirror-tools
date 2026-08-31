pub mod config;
mod executor;
mod spool;

use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use anyhow::bail;
use config::Config;
use lmt_core::{AttemptState, FailureKind, NodeName, ProcessRunSpec};
use lmt_protocol::v1alpha1::{AgentAction, Capacity, EventRequest, OwnedAttempt, PollRequest, PollResponse};
use reqwest::{Client, StatusCode};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::{Mutex, watch},
};

use spool::{SpoolRecord, log_path, read, retire, state_path, write};

#[derive(Clone)]
pub struct Agent {
    config: Config,
    token: Arc<str>,
    instance: Arc<str>,
    client: Client,
    active: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
    acceptance: Arc<Mutex<()>>,
    shutdown: watch::Receiver<bool>,
}

impl Agent {
    pub async fn new(config: Config, shutdown: watch::Receiver<bool>) -> anyhow::Result<Self> {
        NodeName::new(&config.node.name)?;
        if config.execution.max_concurrent_runs == 0 {
            bail!("max_concurrent_runs must be positive");
        }
        fs::create_dir_all(&config.storage.spool_dir).await?;
        let token = fs::read_to_string(&config.server.token_file).await?.trim().to_owned();
        Ok(Self {
            config,
            token: token.into(),
            instance: ulid::Ulid::new().to_string().into(),
            client: Client::builder().timeout(Duration::from_secs(35)).build()?,
            active: Arc::new(Mutex::new(HashMap::new())),
            acceptance: Arc::new(Mutex::new(())),
            shutdown,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        self.recover().await;
        let reconciler = self.clone();
        let reconciliation = tokio::spawn(async move {
            while !*reconciler.shutdown.borrow() {
                reconciler.reconcile_all().await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        let mut sequence = 0;
        while !*self.shutdown.borrow() {
            sequence += 1;
            let request = PollRequest {
                protocol_version: "v1alpha1".into(),
                agent_version: env!("CARGO_PKG_VERSION").into(),
                agent_instance_id: self.instance.to_string(),
                poll_sequence: sequence,
                running: self.owned().await,
                capacity: Capacity {
                    mirror_root_free_bytes: None,
                    active_runs: u32::try_from(self.active.lock().await.len()).unwrap_or(u32::MAX),
                    max_concurrent_runs: self.config.execution.max_concurrent_runs,
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
                Ok(response) if response.status() == StatusCode::NO_CONTENT => {}
                Ok(response) if response.status().is_success() => {
                    for action in response.json::<PollResponse>().await?.actions {
                        match action {
                            AgentAction::StartAttempt {
                                run_id,
                                attempt,
                                spec_hash,
                                spec,
                            } => self.accept(run_id, attempt, spec_hash, spec).await,
                            AgentAction::CancelAttempt {
                                run_id,
                                attempt,
                                spec_hash,
                            } => self.cancel(run_id, attempt, spec_hash).await,
                        }
                    }
                }
                Ok(response) => tracing::warn!(status=%response.status(), "poll rejected"),
                Err(error) => tracing::warn!(%error, "poll failed"),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        reconciliation.abort();
        Ok(())
    }

    async fn accept(&self, run_id: String, attempt: u32, spec_hash: String, spec: ProcessRunSpec) {
        let acceptance = self.acceptance.lock().await;
        let key = format!("{run_id}-{attempt}");
        let path = state_path(&self.config.storage.spool_dir, &run_id, attempt);
        if let Ok(mut existing) = read(&path).await {
            if existing.spec_hash != spec_hash {
                tracing::error!(%run_id, attempt, expected=%existing.spec_hash, received=%spec_hash,
                    "protocol integrity error: conflicting StartAttempt preserved original ownership");
                return;
            }
            drop(acceptance);
            let _ = self.reconcile(&path, &mut existing).await;
            return;
        }
        let mut active = self.active.lock().await;
        if active.contains_key(&key) || active.len() >= self.config.execution.max_concurrent_runs as usize {
            return;
        }
        if !self.config.runner.process.enabled || !safe_spec(&self.config.storage.mirror_root, &spec) {
            let mut record = SpoolRecord::accepted(run_id, attempt, spec_hash, spec, now());
            record.terminal(
                AttemptState::Rejected,
                None,
                Some(FailureKind::Rejected),
                Some("local policy rejected spec".into()),
                now(),
            );
            if write(&path, &record).await.is_ok() {
                drop(acceptance);
                let _ = self.reconcile(&path, &mut record).await;
            }
            return;
        }
        let record = SpoolRecord::accepted(run_id, attempt, spec_hash, spec, now());
        if let Err(error) = write(&path, &record).await {
            tracing::error!(%error, "failed to persist acceptance");
            return;
        }
        let (cancel, cancel_receiver) = watch::channel(false);
        active.insert(key.clone(), cancel);
        drop(active);
        let agent = self.clone();
        tokio::spawn(async move {
            let mut record = record;
            if let Err(error) = agent.reconcile(&path, &mut record).await {
                tracing::warn!(%error, "accepted reconciliation failed");
            }
            executor::execute(
                &path,
                &mut record,
                agent.shutdown.clone(),
                cancel_receiver,
                agent.acceptance.clone(),
            )
            .await;
            tracing::info!(run_id=%record.run_id, sequence=record.sequence, "execution reached terminal reconciliation");
            if let Err(error) = agent.reconcile(&path, &mut record).await {
                tracing::warn!(%error, "terminal reconciliation failed");
            }
            agent.active.lock().await.remove(&key);
        });
    }

    async fn cancel(&self, run_id: String, attempt: u32, spec_hash: String) {
        let acceptance = self.acceptance.lock().await;
        let key = format!("{run_id}-{attempt}");
        let path = state_path(&self.config.storage.spool_dir, &run_id, attempt);
        let mut record = match read(&path).await {
            Ok(record) => record,
            Err(_) if !path.exists() => {
                SpoolRecord::cancellation_tombstone(run_id.clone(), attempt, spec_hash.clone(), now())
            }
            Err(error) => {
                tracing::error!(%error, %run_id, attempt, "failed to read existing cancellation state");
                return;
            }
        };
        if record.spec_hash != spec_hash {
            tracing::error!(%run_id, attempt, expected=%record.spec_hash, received=%spec_hash,
                "protocol integrity error: conflicting CancelAttempt preserved original ownership");
            return;
        }
        if record.state.is_terminal() {
            if !path.exists()
                && let Err(error) = write(&path, &record).await
            {
                tracing::error!(%error, "failed to persist cancellation tombstone");
                return;
            }
            drop(acceptance);
            let _ = self.reconcile(&path, &mut record).await;
            return;
        }
        record.cancel_requested = true;
        if let Err(error) = write(&path, &record).await {
            tracing::error!(%error, "failed to persist cancellation intent");
            return;
        }
        let control = self.active.lock().await.get(&key).cloned();
        drop(acceptance);
        if let Some(control) = control {
            let _ = control.send(true);
        }
    }

    async fn recover(&self) {
        let Ok(paths) = spool_paths(&self.config.storage.spool_dir).await else {
            return;
        };
        for path in paths {
            if path.extension().and_then(|value| value.to_str()) == Some("retired") {
                let _ = fs::remove_file(log_path(&path)).await;
                let _ = fs::remove_file(&path).await;
                continue;
            }
            let Ok(mut record) = read(&path).await else {
                continue;
            };
            if !record.state.is_terminal() && record.cancel_requested {
                record.terminal(AttemptState::Cancelled, None, None, None, now());
                let _ = write(&path, &record).await;
            } else if !record.state.is_terminal() {
                record.terminal(
                    AttemptState::Interrupted,
                    None,
                    Some(FailureKind::Interrupted),
                    Some("agent restarted without terminal result".into()),
                    now(),
                );
                let _ = write(&path, &record).await;
            }
            let _ = self.reconcile(&path, &mut record).await;
        }
    }

    async fn reconcile_all(&self) {
        let Ok(paths) = spool_paths(&self.config.storage.spool_dir).await else {
            return;
        };
        for path in paths
            .into_iter()
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        {
            if let Ok(mut record) = read(&path).await {
                if let Err(error) = self.reconcile(&path, &mut record).await {
                    tracing::debug!(%error, "background reconciliation deferred");
                }
            }
        }
    }

    async fn reconcile(&self, path: &Path, record: &mut SpoolRecord) -> anyhow::Result<()> {
        if record.acknowledged_sequence < record.sequence {
            let report_accepted = record.acknowledged_sequence == 0 && record.sequence > 1;
            let event_sequence = if report_accepted { 1 } else { record.sequence };
            let event = EventRequest {
                event_sequence,
                state: if report_accepted {
                    AttemptState::Accepted
                } else {
                    record.state
                },
                agent_instance_id: self.instance.to_string(),
                accepted_at: record.accepted_at.clone(),
                started_at: record.started_at.clone(),
                finished_at: record.finished_at.clone(),
                exit_code: record.exit_code,
                failure_kind: record.failure_kind,
                failure_message: record.failure_message.clone(),
            };
            let response = self
                .client
                .post(format!(
                    "{}/api/v1alpha1/agent/attempts/{}/{}/events",
                    self.config.server.url, record.run_id, record.attempt
                ))
                .bearer_auth(self.token.as_ref())
                .json(&event)
                .send()
                .await?;
            if response.status().is_success() {
                let _guard = self.acceptance.lock().await;
                let mut latest = read(path).await?;
                latest.acknowledged_sequence = latest.acknowledged_sequence.max(event_sequence);
                write(path, &latest).await?;
                *record = latest;
            }
        }
        let local_log = log_path(path);
        let stored_bytes = match fs::metadata(&local_log).await {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        if record.log_offset > stored_bytes {
            bail!("acknowledged log offset exceeds local spool length");
        }
        let has_bytes = record.log_offset < stored_bytes;
        if has_bytes || (record.state.is_terminal() && !record.log_complete_acknowledged) {
            let remaining = stored_bytes.saturating_sub(record.log_offset).min(65_536);
            let mut bytes = vec![0; usize::try_from(remaining).unwrap_or(65_536)];
            if !bytes.is_empty() {
                let mut file = fs::File::open(&local_log).await?;
                file.seek(std::io::SeekFrom::Start(record.log_offset)).await?;
                file.read_exact(&mut bytes).await?;
            }
            let final_chunk = record.state.is_terminal() && record.log_offset + remaining == stored_bytes;
            let response = self
                .client
                .put(format!(
                    "{}/api/v1alpha1/agent/attempts/{}/{}/log",
                    self.config.server.url, record.run_id, record.attempt
                ))
                .bearer_auth(self.token.as_ref())
                .header("x-lmt-log-offset", record.log_offset)
                .header("x-lmt-log-complete", final_chunk.to_string())
                .body(bytes)
                .send()
                .await?;
            if response.status().is_success() {
                let acknowledged_offset = response
                    .headers()
                    .get("x-lmt-log-next-offset")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(record.log_offset);
                let _guard = self.acceptance.lock().await;
                let mut latest = read(path).await?;
                latest.log_offset = latest.log_offset.max(acknowledged_offset);
                latest.log_complete_acknowledged |= final_chunk;
                write(path, &latest).await?;
                *record = latest;
            }
        }
        if record.ready_for_cleanup() {
            retire(path).await?;
        }
        Ok(())
    }

    async fn owned(&self) -> Vec<OwnedAttempt> {
        let Ok(paths) = spool_paths(&self.config.storage.spool_dir).await else {
            return vec![];
        };
        let mut owned = Vec::new();
        for path in paths {
            if let Ok(record) = read(&path).await
                && !record.state.is_terminal()
            {
                owned.push(OwnedAttempt {
                    run_id: record.run_id,
                    attempt: record.attempt,
                    state: record.state,
                });
            }
        }
        owned
    }
}

async fn spool_paths(root: &Path) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut entries = fs::read_dir(root).await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        paths.push(entry.path());
    }
    Ok(paths)
}

fn safe_spec(root: &Path, spec: &ProcessRunSpec) -> bool {
    spec.runner == "process"
        && spec.mirror_root == root.to_string_lossy()
        && Path::new(&spec.target_dir).starts_with(root)
        && spec.cwd.as_ref().is_none_or(|cwd| Path::new(cwd).starts_with(root))
}

pub fn now() -> String {
    OffsetDateTime::now_utc().format(&Rfc3339).expect("format time")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Bytes,
        extract::State,
        http::{HeaderMap, HeaderValue, StatusCode},
        routing::{post, put},
    };
    use lmt_core::{
        AttemptNo, BundleFile, ConfigBundle, MirrorName, RunId, RunSpecContext, canonicalize_bundle,
        compile_process_run_spec,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    fn test_agent(root: &Path, shutdown: watch::Receiver<bool>, server_url: String) -> Agent {
        Agent {
            config: Config {
                node: config::Node { name: "node-a".into() },
                server: config::Server {
                    url: server_url,
                    token_file: root.join("token"),
                },
                storage: config::Storage {
                    mirror_root: root.join("mirrors"),
                    spool_dir: root.join("spool"),
                },
                execution: config::Execution { max_concurrent_runs: 4 },
                runner: config::Runner {
                    process: config::ProcessPolicy { enabled: true },
                },
            },
            token: "token".into(),
            instance: "instance".into(),
            client: Client::new(),
            active: Arc::new(Mutex::new(HashMap::new())),
            acceptance: Arc::new(Mutex::new(())),
            shutdown,
        }
    }

    fn spec(root: &Path, program: &str, args: Vec<String>, timeout_seconds: u64) -> ProcessRunSpec {
        ProcessRunSpec {
            runner: "process".into(),
            program: program.into(),
            args,
            cwd: None,
            timeout_seconds,
            mirror_root: root.to_string_lossy().into_owned(),
            target_dir: root.join("demo").to_string_lossy().into_owned(),
        }
    }

    #[test]
    fn nested_config_rejects_unknown_fields() {
        let source = "[node]\nname='n'\ntypo=true\n[server]\nurl='http://x'\ntoken_file='/x'\n[storage]\nmirror_root='/x'\nspool_dir='/y'\n[execution]\nmax_concurrent_runs=1\n[runner.process]\nenabled=true\n";
        assert!(toml::from_str::<Config>(source).is_err());
    }

    #[tokio::test]
    async fn concurrent_duplicate_start_executes_once_and_conflict_preserves_record() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (_sender, receiver) = watch::channel(false);
        let agent = test_agent(directory.path(), receiver, "http://127.0.0.1:1".into());
        fs::create_dir_all(&agent.config.storage.spool_dir)
            .await
            .expect("spool");
        fs::create_dir_all(&agent.config.storage.mirror_root)
            .await
            .expect("root");
        let counter = directory.path().join("counter");
        let command = spec(
            &agent.config.storage.mirror_root,
            "/bin/sh",
            vec!["-c".into(), format!("echo run >> '{}'; sleep 1", counter.display())],
            5,
        );
        let first = agent.accept(
            "01K00000000000000000000000".into(),
            1,
            "sha256:one".into(),
            command.clone(),
        );
        let duplicate = agent.accept("01K00000000000000000000000".into(), 1, "sha256:one".into(), command);
        tokio::join!(first, duplicate);
        tokio::time::sleep(Duration::from_millis(150)).await;
        agent
            .accept(
                "01K00000000000000000000000".into(),
                1,
                "sha256:conflict".into(),
                spec(&agent.config.storage.mirror_root, "/bin/false", vec![], 5),
            )
            .await;
        let path = state_path(&agent.config.storage.spool_dir, "01K00000000000000000000000", 1);
        let record = read(&path).await.expect("record");
        assert_eq!(record.spec_hash, "sha256:one");
        assert_ne!(record.state, AttemptState::Rejected);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(fs::read_to_string(counter).await.expect("counter").lines().count(), 1);
    }

    #[tokio::test]
    async fn cancel_before_start_is_a_durable_hash_bound_tombstone() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (_sender, receiver) = watch::channel(false);
        let agent = test_agent(directory.path(), receiver, "http://127.0.0.1:1".into());
        fs::create_dir_all(&agent.config.storage.spool_dir)
            .await
            .expect("spool");
        fs::create_dir_all(&agent.config.storage.mirror_root)
            .await
            .expect("root");
        let run_id = "01K00000000000000000000001";
        let counter = directory.path().join("counter");
        agent.cancel(run_id.into(), 1, "sha256:one".into()).await;
        let path = state_path(&agent.config.storage.spool_dir, run_id, 1);
        let tombstone = read(&path).await.expect("tombstone");
        assert_eq!(tombstone.state, AttemptState::Cancelled);
        assert!(tombstone.spec.is_none());

        let command = spec(
            &agent.config.storage.mirror_root,
            "/bin/sh",
            vec!["-c".into(), format!("touch '{}'", counter.display())],
            5,
        );
        agent
            .accept(run_id.into(), 1, "sha256:one".into(), command.clone())
            .await;
        agent.accept(run_id.into(), 1, "sha256:conflict".into(), command).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!counter.exists(), "delayed Start executed after cancellation");
        assert_eq!(read(&path).await.expect("preserved").spec_hash, "sha256:one");

        let (_sender, restarted_receiver) = watch::channel(false);
        let restarted = test_agent(directory.path(), restarted_receiver, "http://127.0.0.1:1".into());
        restarted.recover().await;
        let recovered = read(&path).await.expect("recovered tombstone");
        assert_eq!(recovered.state, AttemptState::Cancelled);
        assert!(recovered.spec.is_none());
    }

    #[tokio::test]
    async fn duplicate_active_cancel_kills_the_process_group_and_persists_cancelled() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (_sender, receiver) = watch::channel(false);
        let agent = test_agent(directory.path(), receiver, "http://127.0.0.1:1".into());
        fs::create_dir_all(&agent.config.storage.spool_dir)
            .await
            .expect("spool");
        fs::create_dir_all(&agent.config.storage.mirror_root)
            .await
            .expect("root");
        let run_id = "01K00000000000000000000002";
        let pid_file = directory.path().join("descendant.pid");
        let command = format!("sleep 30 & echo $! > '{}'; wait", pid_file.display());
        agent
            .accept(
                run_id.into(),
                1,
                "sha256:active".into(),
                spec(
                    &agent.config.storage.mirror_root,
                    "/bin/sh",
                    vec!["-c".into(), command],
                    60,
                ),
            )
            .await;
        for _ in 0..100 {
            if pid_file.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let pid: i32 = fs::read_to_string(&pid_file)
            .await
            .expect("descendant pid")
            .trim()
            .parse()
            .expect("number");
        agent.cancel(run_id.into(), 1, "sha256:active".into()).await;
        agent.cancel(run_id.into(), 1, "sha256:active".into()).await;
        let path = state_path(&agent.config.storage.spool_dir, run_id, 1);
        let mut final_record = None;
        for _ in 0..100 {
            if let Ok(record) = read(&path).await
                && record.state.is_terminal()
            {
                final_record = Some(record);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(final_record.expect("terminal record").state, AttemptState::Cancelled);
        assert!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err(),
            "cancelled descendant remained alive"
        );
    }

    #[tokio::test]
    async fn timeout_kills_process_group_descendant_and_streams_both_outputs() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (_sender, receiver) = watch::channel(false);
        let root = directory.path().join("mirrors");
        fs::create_dir_all(&root).await.expect("root");
        let state = directory.path().join("attempt.json");
        let pid_file = directory.path().join("descendant.pid");
        let command = format!(
            "echo out; echo err >&2; sleep 30 & echo $! > '{}'; wait",
            pid_file.display()
        );
        let mut record = SpoolRecord::accepted(
            "01K00000000000000000000000".into(),
            1,
            "hash".into(),
            spec(&root, "/bin/sh", vec!["-c".into(), command], 1),
            now(),
        );
        write(&state, &record).await.expect("write");
        let (_cancel, cancel_receiver) = watch::channel(false);
        executor::execute(&state, &mut record, receiver, cancel_receiver, Arc::new(Mutex::new(()))).await;
        assert_eq!(record.state, AttemptState::TimedOut);
        let log = fs::read_to_string(log_path(&state)).await.expect("log");
        assert!(log.contains("[stdout] out"));
        assert!(log.contains("[stderr] err"));
        let pid: i32 = fs::read_to_string(pid_file)
            .await
            .expect("pid")
            .trim()
            .parse()
            .expect("number");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err(),
            "descendant remained alive"
        );
    }

    #[tokio::test]
    async fn local_rsync_uses_the_native_executor_for_success_and_failure() {
        let directory = tempfile::tempdir().expect("tempdir");
        let mirror_root = directory.path().join("mirrors");
        let source = directory.path().join("source");
        fs::create_dir_all(&mirror_root).await.expect("mirror root");
        fs::create_dir_all(&source).await.expect("source");
        fs::write(source.join("hello.txt"), b"hello\n").await.expect("fixture");
        let bundle = canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: format!(
                    "[mirror]\nname='demo'\ntarget='demo'\n[sync]\ntype='rsync'\nsource='{}/'\nargs=['--archive']\n",
                    source.display()
                ),
            }],
        })
        .expect("bundle");
        let mirror = MirrorName::new("demo").expect("mirror");
        let node = NodeName::new("node-a").expect("node");
        let document = &bundle.mirrors[&mirror].document;
        let compiled = compile_process_run_spec(
            document,
            &RunSpecContext {
                mirror_name: &mirror,
                run_id: RunId::new(),
                attempt_no: AttemptNo::new(1).expect("attempt"),
                node_name: &node,
                mirror_root: &mirror_root,
            },
        );
        assert_eq!(compiled.program, "rsync");
        assert_eq!(compiled.args[compiled.args.len() - 2], format!("{}/", source.display()));

        let state = directory.path().join("rsync-success.json");
        let mut record = SpoolRecord::accepted("01K00000000000000000000003".into(), 1, "hash".into(), compiled, now());
        write(&state, &record).await.expect("write");
        let (_shutdown, shutdown_receiver) = watch::channel(false);
        let (_cancel, cancel_receiver) = watch::channel(false);
        executor::execute(
            &state,
            &mut record,
            shutdown_receiver,
            cancel_receiver,
            Arc::new(Mutex::new(())),
        )
        .await;
        assert_eq!(record.state, AttemptState::Succeeded);
        assert_eq!(
            fs::read(mirror_root.join("demo/hello.txt")).await.expect("copied"),
            b"hello\n"
        );

        let mut failed_spec = record.spec.clone().expect("spec");
        let source_index = failed_spec.args.len() - 2;
        failed_spec.args[source_index] = directory.path().join("missing/").to_string_lossy().into_owned();
        let failed_state = directory.path().join("rsync-failed.json");
        let mut failed = SpoolRecord::accepted(
            "01K00000000000000000000004".into(),
            1,
            "hash-failed".into(),
            failed_spec,
            now(),
        );
        write(&failed_state, &failed).await.expect("write failed");
        let (_shutdown, shutdown_receiver) = watch::channel(false);
        let (_cancel, cancel_receiver) = watch::channel(false);
        executor::execute(
            &failed_state,
            &mut failed,
            shutdown_receiver,
            cancel_receiver,
            Arc::new(Mutex::new(())),
        )
        .await;
        assert_eq!(failed.state, AttemptState::Failed);
    }

    #[derive(Clone)]
    struct MockState {
        saw_empty_complete: Arc<AtomicBool>,
    }

    async fn mock_event() -> StatusCode {
        StatusCode::OK
    }
    async fn mock_log(State(state): State<MockState>, headers: HeaderMap, body: Bytes) -> (StatusCode, HeaderMap) {
        if body.is_empty() && headers.get("x-lmt-log-complete").and_then(|v| v.to_str().ok()) == Some("true") {
            state.saw_empty_complete.store(true, Ordering::SeqCst);
        }
        let mut response = HeaderMap::new();
        response.insert("x-lmt-log-next-offset", HeaderValue::from_static("0"));
        (StatusCode::NO_CONTENT, response)
    }

    #[tokio::test]
    async fn empty_log_completion_ack_retires_spool() {
        let directory = tempfile::tempdir().expect("tempdir");
        let observed = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route("/api/v1alpha1/agent/attempts/{run}/{attempt}/events", post(mock_event))
            .route("/api/v1alpha1/agent/attempts/{run}/{attempt}/log", put(mock_log))
            .with_state(MockState {
                saw_empty_complete: observed.clone(),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        let (_sender, receiver) = watch::channel(false);
        let agent = test_agent(directory.path(), receiver, url);
        fs::create_dir_all(&agent.config.storage.spool_dir)
            .await
            .expect("spool");
        let path = state_path(&agent.config.storage.spool_dir, "01K00000000000000000000000", 1);
        let mut record = SpoolRecord::accepted(
            "01K00000000000000000000000".into(),
            1,
            "hash".into(),
            spec(&agent.config.storage.mirror_root, "/bin/true", vec![], 5),
            now(),
        );
        record.terminal(AttemptState::Succeeded, Some(0), None, None, now());
        write(&path, &record).await.expect("write");
        agent.reconcile(&path, &mut record).await.expect("reconcile");
        agent.reconcile(&path, &mut record).await.expect("terminal reconcile");
        assert!(observed.load(Ordering::SeqCst));
        assert!(!path.exists());
        assert!(!log_path(&path).exists());
    }
}
