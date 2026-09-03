use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use lmt_core::{AttemptEvent, ConfigBundle, NodeName, RunId, canonicalize_bundle};
use lmt_protocol::v1alpha1::*;
use lmt_store::{AttemptRecord, ChangeKind, ConfigPlan, NodeObservation, RunRecord, Store, StoreError};
use rand_core::{OsRng, RngCore};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, Notify},
};

pub mod backup;
mod process_lock;
mod services;

pub use process_lock::ProcessLock;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub database_path: PathBuf,
    pub log_dir: PathBuf,
    #[serde(default)]
    pub operator_token: Option<String>,
    #[serde(default)]
    pub operator_token_file: Option<PathBuf>,
    #[serde(default = "offline_default")]
    pub offline_after_seconds: u64,
    #[serde(default)]
    pub agents: Vec<AgentCredential>,
    #[serde(default)]
    pub run_logs: Option<RunLogsConfig>,
    #[serde(default)]
    pub backup: Option<BackupConfig>,
    #[serde(default)]
    pub status: Option<StatusConfig>,
    #[serde(default)]
    pub logging: Option<LoggingConfig>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupConfig {
    pub directory: PathBuf,
}
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StatusConfig {
    #[serde(default)]
    pub public: bool,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "logging_level_default")]
    pub level: String,
    #[serde(default = "logging_format_default")]
    pub format: LoggingFormat,
}
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoggingFormat {
    Json,
    Text,
}
fn logging_level_default() -> String {
    "info".into()
}
const fn logging_format_default() -> LoggingFormat {
    LoggingFormat::Json
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCredential {
    pub node: String,
    pub token: String,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunLogsConfig {
    pub retention: Option<String>,
    pub max_total_bytes: Option<u64>,
    #[serde(default = "log_maintenance_default")]
    pub maintenance_interval: String,
}
fn log_maintenance_default() -> String {
    "1h".into()
}
const fn offline_default() -> u64 {
    90
}
#[derive(Clone)]
pub struct AppState {
    store: Store,
    log_dir: PathBuf,
    operator_token: Arc<RwLock<String>>,
    operator_token_file: Option<PathBuf>,
    offline_after: Duration,
    notify: Arc<Notify>,
    log_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    metrics: Arc<AppMetrics>,
    publication_observations: Arc<Mutex<HashMap<String, PublicationObservation>>>,
    poll_wait: Duration,
    clock: Arc<dyn Clock>,
    run_log_policy: Option<RunLogPolicy>,
    database_path: PathBuf,
    backup_dir: Option<PathBuf>,
    backup_lock: Arc<Mutex<()>>,
    public_status: bool,
    deprecated_inline_credentials: bool,
}

#[derive(Debug, Clone, Copy)]
struct RunLogPolicy {
    retention: Option<Duration>,
    max_total_bytes: Option<u64>,
    maintenance_interval: Duration,
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        now_ms()
    }
}

#[derive(Default)]
struct AppMetrics {
    polls: AtomicU64,
    events: AtomicU64,
    uploaded_bytes: AtomicU64,
    log_failures: AtomicU64,
    scheduler_interval_due: AtomicU64,
    scheduler_interval_skipped: AtomicU64,
    scheduler_cron_due: AtomicU64,
    scheduler_cron_skipped: AtomicU64,
    retries_scheduled: AtomicU64,
    attempts_succeeded: AtomicU64,
    attempts_failed: AtomicU64,
    attempts_timed_out: AtomicU64,
    attempts_cancelled: AtomicU64,
    attempts_rejected: AtomicU64,
    attempts_interrupted: AtomicU64,
    cancellations_immediate: AtomicU64,
    cancellations_dispatched: AtomicU64,
    logs_expired: AtomicU64,
    backup_last_success_seconds: AtomicU64,
    backup_failures: AtomicU64,
    auth_failures: AtomicU64,
}

#[derive(Clone)]
struct PublicationObservation {
    agent_instance_id: String,
    agent_boot_id: String,
    atomic_capable: bool,
    last: PublicationHealth,
    cumulative: PublicationHealth,
}

impl PublicationObservation {
    fn new(agent_instance_id: String, agent_boot_id: String, atomic_capable: bool, health: PublicationHealth) -> Self {
        Self {
            agent_instance_id,
            agent_boot_id,
            atomic_capable,
            last: health.clone(),
            cumulative: health,
        }
    }

    fn update(
        &mut self,
        agent_instance_id: &str,
        agent_boot_id: &str,
        atomic_capable: bool,
        health: PublicationHealth,
    ) {
        let reset = self.agent_instance_id != agent_instance_id || self.agent_boot_id != agent_boot_id;
        self.cumulative.commits_succeeded_total = self.cumulative.commits_succeeded_total.saturating_add(
            counter_delta(health.commits_succeeded_total, self.last.commits_succeeded_total, reset),
        );
        self.cumulative.commits_failed_total = self.cumulative.commits_failed_total.saturating_add(counter_delta(
            health.commits_failed_total,
            self.last.commits_failed_total,
            reset,
        ));
        self.cumulative.visibility_to_durability_milliseconds_total = self
            .cumulative
            .visibility_to_durability_milliseconds_total
            .saturating_add(counter_delta(
                health.visibility_to_durability_milliseconds_total,
                self.last.visibility_to_durability_milliseconds_total,
                reset,
            ));
        self.cumulative.visibility_to_durability_samples_total = self
            .cumulative
            .visibility_to_durability_samples_total
            .saturating_add(counter_delta(
                health.visibility_to_durability_samples_total,
                self.last.visibility_to_durability_samples_total,
                reset,
            ));
        self.cumulative.preflight_rejections_total =
            self.cumulative.preflight_rejections_total.saturating_add(counter_delta(
                health.preflight_rejections_total,
                self.last.preflight_rejections_total,
                reset,
            ));
        self.cumulative.gc_failures_total = self.cumulative.gc_failures_total.saturating_add(counter_delta(
            health.gc_failures_total,
            self.last.gc_failures_total,
            reset,
        ));
        self.cumulative.publication_root_free_bytes = health.publication_root_free_bytes;
        self.cumulative.gc_backlog_generations = health.gc_backlog_generations;
        self.cumulative
            .admission_block_reason
            .clone_from(&health.admission_block_reason);
        self.cumulative.fenced_records = health.fenced_records;
        self.cumulative.recovery_records = health.recovery_records;
        self.cumulative.degraded = health.degraded;
        agent_instance_id.clone_into(&mut self.agent_instance_id);
        agent_boot_id.clone_into(&mut self.agent_boot_id);
        self.atomic_capable = atomic_capable;
        self.last = health;
    }
}

const fn counter_delta(current: u64, previous: u64, reset: bool) -> u64 {
    if reset || current < previous {
        current
    } else {
        current - previous
    }
}
impl AppState {
    pub fn new(store: Store, log_dir: PathBuf, token: String, offline_after: Duration) -> Self {
        Self {
            store,
            log_dir,
            operator_token: Arc::new(RwLock::new(token)),
            operator_token_file: None,
            offline_after,
            notify: Arc::new(Notify::new()),
            log_locks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(AppMetrics::default()),
            publication_observations: Arc::new(Mutex::new(HashMap::new())),
            poll_wait: Duration::from_secs(20),
            clock: Arc::new(SystemClock),
            run_log_policy: None,
            database_path: PathBuf::new(),
            backup_dir: None,
            backup_lock: Arc::new(Mutex::new(())),
            public_status: false,
            deprecated_inline_credentials: false,
        }
    }

    fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }

    pub fn wake_scheduler(&self) {
        self.notify.notify_waiters();
    }

    pub async fn reload_operator_token(&self) -> anyhow::Result<()> {
        let path = self
            .operator_token_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operator_token_file is not configured"))?;
        let token = fs::read_to_string(path).await?;
        let token = token.trim();
        if token.is_empty() {
            anyhow::bail!("operator token file is empty");
        }
        token.clone_into(&mut self.operator_token.write().expect("operator token lock poisoned"));
        Ok(())
    }
}
pub async fn initialize(c: &ServerConfig) -> anyhow::Result<AppState> {
    initialize_with_clock(c, Arc::new(SystemClock)).await
}

pub fn acquire_server_lock(c: &ServerConfig) -> anyhow::Result<ProcessLock> {
    let parent = c
        .database_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("database path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    ProcessLock::acquire(&parent.join("lmt-server.lock"))
}

pub async fn initialize_with_clock(c: &ServerConfig, clock: Arc<dyn Clock>) -> anyhow::Result<AppState> {
    if let Some(p) = c.database_path.parent() {
        fs::create_dir_all(p).await?;
    }
    fs::create_dir_all(&c.log_dir).await?;
    let store = Store::open(&c.database_path).await?;
    for a in &c.agents {
        if store
            .import_legacy_credential(&a.node, &a.token, clock.now_ms())
            .await?
        {
            tracing::warn!(node=%a.node, "imported deprecated inline Agent credential; remove it from server config");
        }
    }
    let operator_token = load_operator_token(c).await?;
    let mut state = AppState::new(
        store,
        c.log_dir.clone(),
        operator_token,
        Duration::from_secs(c.offline_after_seconds),
    );
    state.clock = clock;
    state.operator_token_file.clone_from(&c.operator_token_file);
    state.run_log_policy = c.run_logs.as_ref().map(parse_run_log_policy).transpose()?.flatten();
    state.database_path.clone_from(&c.database_path);
    state.backup_dir = c.backup.as_ref().map(|value| value.directory.clone());
    load_backup_recency(&state).await;
    state.public_status = c.status.as_ref().is_some_and(|status| status.public);
    state.deprecated_inline_credentials = !c.agents.is_empty() || c.operator_token.is_some();
    tokio::spawn(run_scheduler(state.clone()));
    if state.run_log_policy.is_some() {
        tokio::spawn(run_log_maintenance(state.clone()));
    }
    Ok(state)
}

async fn load_backup_recency(state: &AppState) {
    let Some(directory) = state.backup_dir.clone() else {
        return;
    };
    match tokio::task::spawn_blocking(move || backup::list(&directory)).await {
        Ok(Ok(backups)) => {
            if let Some(seconds) = backups.iter().filter_map(backup_manifest_timestamp_seconds).max() {
                state
                    .metrics
                    .backup_last_success_seconds
                    .store(seconds, Ordering::Relaxed);
            }
        }
        Ok(Err(error)) => {
            tracing::warn!(component = "server", error_code = "backup_invalid", %error, "failed to load backup recency")
        }
        Err(error) => {
            tracing::warn!(component = "server", error_code = "backup_invalid", %error, "backup recency task failed")
        }
    }
}

fn backup_manifest_timestamp_seconds(manifest: &BackupManifest) -> Option<u64> {
    OffsetDateTime::parse(&manifest.created_at, &Rfc3339)
        .ok()
        .and_then(|timestamp| u64::try_from(timestamp.unix_timestamp()).ok())
}

fn parse_run_log_policy(config: &RunLogsConfig) -> anyhow::Result<Option<RunLogPolicy>> {
    if config.retention.is_none() && config.max_total_bytes.is_none() {
        return Ok(None);
    }
    let retention = config.retention.as_deref().map(humantime::parse_duration).transpose()?;
    let maintenance_interval = humantime::parse_duration(&config.maintenance_interval)?;
    if maintenance_interval.is_zero() {
        anyhow::bail!("run_logs.maintenance_interval must be positive");
    }
    Ok(Some(RunLogPolicy {
        retention,
        max_total_bytes: config.max_total_bytes,
        maintenance_interval,
    }))
}

async fn load_operator_token(c: &ServerConfig) -> anyhow::Result<String> {
    let token = if let Some(path) = &c.operator_token_file {
        fs::read_to_string(path).await?
    } else if let Some(token) = &c.operator_token {
        tracing::warn!("deprecated inline operator_token is configured; use operator_token_file");
        token.clone()
    } else {
        anyhow::bail!("operator_token_file is required (deprecated operator_token is accepted for migration)");
    };
    let token = token.trim().to_owned();
    if token.is_empty() {
        anyhow::bail!("operator token is empty");
    }
    Ok(token)
}

async fn run_scheduler(state: AppState) {
    loop {
        let now = state.now_ms();
        match services::evaluate_schedules(&state.store, now).await {
            Ok(evaluated) => {
                state
                    .metrics
                    .scheduler_interval_due
                    .fetch_add(evaluated.interval_due, Ordering::Relaxed);
                state
                    .metrics
                    .scheduler_interval_skipped
                    .fetch_add(evaluated.interval_skipped, Ordering::Relaxed);
                state
                    .metrics
                    .scheduler_cron_due
                    .fetch_add(evaluated.cron_due, Ordering::Relaxed);
                state
                    .metrics
                    .scheduler_cron_skipped
                    .fetch_add(evaluated.cron_skipped, Ordering::Relaxed);
                let earliest = state.store.earliest_wakeup().await.unwrap_or(None);
                if evaluated.evaluated > 0 || earliest.is_some_and(|deadline| deadline <= now) {
                    state.notify.notify_waiters();
                }
                let wait = earliest
                    .filter(|deadline| *deadline > now)
                    .map_or(Duration::from_secs(30), |deadline| {
                        Duration::from_millis(u64::try_from(deadline - now).unwrap_or(u64::MAX))
                            .min(Duration::from_secs(30))
                    });
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = state.notify.notified() => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "scheduler evaluation failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn run_log_maintenance(state: AppState) {
    let interval = state.run_log_policy.expect("maintenance policy").maintenance_interval;
    loop {
        if let Err(error) = execute_log_maintenance(&state).await {
            tracing::error!(%error.message, "Run-log maintenance failed");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn plan_log_maintenance(state: &AppState) -> Result<LogMaintenancePlan, StoreError> {
    let entries = state.store.log_retention_entries().await?;
    let stored_bytes = entries
        .iter()
        .filter(|entry| entry.expired_at_ms.is_none())
        .map(|entry| entry.stored_bytes)
        .sum::<u64>();
    let Some(policy) = state.run_log_policy else {
        return Ok(LogMaintenancePlan {
            stored_bytes,
            expire_bytes: 0,
            candidates: vec![],
        });
    };
    let age_cutoff = policy.retention.map(|retention| {
        state
            .now_ms()
            .saturating_sub(i64::try_from(retention.as_millis()).unwrap_or(i64::MAX))
    });
    let mut candidates = Vec::new();
    let mut projected = stored_bytes;
    for entry in entries
        .iter()
        .filter(|entry| entry.expired_at_ms.is_none() && entry.eligible)
    {
        if age_cutoff.is_some_and(|cutoff| entry.updated_at_ms <= cutoff) {
            projected = projected.saturating_sub(entry.stored_bytes);
            candidates.push(LogExpirationView {
                run_id: entry.run_id.clone(),
                attempt: entry.attempt_no,
                stored_bytes: entry.stored_bytes,
            });
        }
    }
    if let Some(cap) = policy.max_total_bytes {
        for entry in entries
            .iter()
            .filter(|entry| entry.expired_at_ms.is_none() && entry.eligible)
        {
            if projected <= cap {
                break;
            }
            if candidates
                .iter()
                .any(|candidate| candidate.run_id == entry.run_id && candidate.attempt == entry.attempt_no)
            {
                continue;
            }
            projected = projected.saturating_sub(entry.stored_bytes);
            candidates.push(LogExpirationView {
                run_id: entry.run_id.clone(),
                attempt: entry.attempt_no,
                stored_bytes: entry.stored_bytes,
            });
        }
    }
    Ok(LogMaintenancePlan {
        stored_bytes,
        expire_bytes: candidates.iter().map(|candidate| candidate.stored_bytes).sum(),
        candidates,
    })
}

async fn execute_log_maintenance(state: &AppState) -> Result<LogMaintenancePlan, Failure> {
    for entry in state
        .store
        .log_retention_entries()
        .await?
        .into_iter()
        .filter(|entry| entry.expired_at_ms.is_some())
    {
        let lock = attempt_log_lock(state, &entry.run_id, entry.attempt_no).await;
        let _guard = lock.lock().await;
        remove_log_file(state, &entry.run_id, entry.attempt_no).await?;
    }
    let plan = plan_log_maintenance(state).await?;
    for candidate in &plan.candidates {
        let lock = attempt_log_lock(state, &candidate.run_id, candidate.attempt).await;
        let _guard = lock.lock().await;
        if state
            .store
            .mark_log_expired(&candidate.run_id, candidate.attempt, state.now_ms())
            .await?
        {
            state.metrics.logs_expired.fetch_add(1, Ordering::Relaxed);
        }
        remove_log_file(state, &candidate.run_id, candidate.attempt).await?;
    }
    Ok(plan)
}

async fn remove_log_file(state: &AppState, run_id: &str, attempt: u32) -> Result<(), Failure> {
    match fs::remove_file(log_path(&state.log_dir, run_id, attempt)).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Failure::io(error)),
    }
}

async fn attempt_log_lock(state: &AppState, run_id: &str, attempt: u32) -> Arc<Mutex<()>> {
    let key = format!("{run_id}-{attempt}");
    let mut locks = state.log_locks.lock().await;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}
pub fn build_router(s: AppState) -> Router {
    Router::new()
        .route(
            "/health/live",
            get(|| async { Json(HealthResponse { status: "live".into() }) }),
        )
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/api/v1alpha1/config/validate", post(validate))
        .route("/api/v1alpha1/config/plan", post(plan))
        .route("/api/v1alpha1/config/apply", post(apply))
        .route("/api/v1alpha1/mirrors", get(mirrors))
        .route("/api/v1alpha1/mirrors/{name}", get(mirror))
        .route("/api/v1alpha1/mirrors/{name}/runs", post(manual))
        .route("/api/v1alpha1/runs", get(runs))
        .route("/api/v1alpha1/runs/{id}", get(run))
        .route("/api/v1alpha1/runs/{id}/cancel", post(cancel_run))
        .route("/api/v1alpha1/runs/{id}/attempts", get(attempts))
        .route("/api/v1alpha1/runs/{id}/logs", get(read_log))
        .route("/api/v1alpha1/nodes", get(nodes))
        .route("/api/v1alpha1/nodes/{name}", get(node))
        .route("/api/v1alpha1/nodes/{name}/binding", post(replace_binding))
        .route(
            "/api/v1alpha1/nodes/{name}/credentials",
            get(credentials).post(issue_credential),
        )
        .route(
            "/api/v1alpha1/nodes/{name}/credentials/{id}/revoke",
            post(revoke_credential),
        )
        .route("/api/v1alpha1/maintenance/logs/plan", get(log_maintenance_plan))
        .route("/api/v1alpha1/maintenance/logs/run", post(log_maintenance_run))
        .route("/api/v1alpha1/backups", get(backups).post(create_backup))
        .route("/api/v1alpha1/backups/{id}/verify", post(verify_backup))
        .route("/api/v1alpha1/status", get(status))
        .route("/api/v1alpha1/doctor", get(doctor))
        .route("/api/v1alpha1/agent/poll", post(poll))
        .route("/api/v1alpha1/agent/attempts/{id}/{no}/events", post(event))
        .route("/api/v1alpha1/agent/attempts/{id}/{no}/log", put(upload_log))
        .with_state(s)
}
async fn ready(State(s): State<AppState>) -> Result<Json<HealthResponse>, Failure> {
    s.store.current_revision().await?;
    Ok(Json(HealthResponse { status: "ready".into() }))
}

async fn metrics(State(state): State<AppState>) -> Result<Response, Failure> {
    use std::fmt::Write as _;

    let counts = state.store.operational_counts().await?;
    let mirrors = state.store.mirror_operational_status().await?;
    let now = state.now_ms();
    let nodes = state.store.list_nodes().await?;
    let nodes_online = nodes
        .iter()
        .filter(|node| {
            node.last_seen_at_ms
                .is_some_and(|seen| now - seen <= i64::try_from(state.offline_after.as_millis()).unwrap_or(i64::MAX))
        })
        .count();
    let mut body = format!(
        "lmt_up 1\nlmt_runs_pending {}\nlmt_runs_running {}\nlmt_mirrors_due {}\nlmt_nodes_online {}\n\
lmt_scheduler_occurrences_total{{kind=\"interval\",outcome=\"due\"}} {}\n\
lmt_scheduler_occurrences_total{{kind=\"interval\",outcome=\"skipped\"}} {}\n\
lmt_scheduler_occurrences_total{{kind=\"cron\",outcome=\"due\"}} {}\n\
lmt_scheduler_occurrences_total{{kind=\"cron\",outcome=\"skipped\"}} {}\n\
lmt_retries_scheduled_total{{reason=\"retryable_failure\"}} {}\n\
lmt_attempts_terminal_total{{state=\"succeeded\"}} {}\n\
lmt_attempts_terminal_total{{state=\"failed\"}} {}\n\
lmt_attempts_terminal_total{{state=\"timed_out\"}} {}\n\
lmt_attempts_terminal_total{{state=\"cancelled\"}} {}\n\
lmt_attempts_terminal_total{{state=\"rejected\"}} {}\n\
lmt_attempts_terminal_total{{state=\"interrupted\"}} {}\n\
lmt_cancellations_total{{outcome=\"immediate\"}} {}\n\
lmt_cancellations_total{{outcome=\"dispatched\"}} {}\n\
lmt_agent_polls_total {}\nlmt_attempt_events_total {}\nlmt_log_uploaded_bytes_total {}\nlmt_log_upload_failures_total {}\n",
        counts.pending_runs,
        counts.running_runs,
        counts.due_mirrors,
        nodes_online,
        state.metrics.scheduler_interval_due.load(Ordering::Relaxed),
        state.metrics.scheduler_interval_skipped.load(Ordering::Relaxed),
        state.metrics.scheduler_cron_due.load(Ordering::Relaxed),
        state.metrics.scheduler_cron_skipped.load(Ordering::Relaxed),
        state.metrics.retries_scheduled.load(Ordering::Relaxed),
        state.metrics.attempts_succeeded.load(Ordering::Relaxed),
        state.metrics.attempts_failed.load(Ordering::Relaxed),
        state.metrics.attempts_timed_out.load(Ordering::Relaxed),
        state.metrics.attempts_cancelled.load(Ordering::Relaxed),
        state.metrics.attempts_rejected.load(Ordering::Relaxed),
        state.metrics.attempts_interrupted.load(Ordering::Relaxed),
        state.metrics.cancellations_immediate.load(Ordering::Relaxed),
        state.metrics.cancellations_dispatched.load(Ordering::Relaxed),
        state.metrics.polls.load(Ordering::Relaxed),
        state.metrics.events.load(Ordering::Relaxed),
        state.metrics.uploaded_bytes.load(Ordering::Relaxed),
        state.metrics.log_failures.load(Ordering::Relaxed)
    );
    writeln!(body, "lmt_run_logs_stored_bytes {}", counts.stored_log_bytes).expect("String write");
    writeln!(
        body,
        "lmt_backup_last_success_timestamp_seconds {}",
        state.metrics.backup_last_success_seconds.load(Ordering::Relaxed)
    )
    .expect("String write");
    writeln!(
        body,
        "lmt_backup_failures_total {}\nlmt_log_expired_total {}\nlmt_auth_failures_total {}",
        state.metrics.backup_failures.load(Ordering::Relaxed),
        state.metrics.logs_expired.load(Ordering::Relaxed),
        state.metrics.auth_failures.load(Ordering::Relaxed)
    )
    .expect("String write");
    let publication_observations = state
        .publication_observations
        .lock()
        .await
        .iter()
        .map(|(node, observation)| (node.clone(), observation.clone()))
        .collect::<Vec<_>>();
    append_publication_metrics(&mut body, &publication_observations);
    append_entity_metrics(&mut body, mirrors, nodes, now, state.offline_after);
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response())
}

fn append_publication_metrics(body: &mut String, observations: &[(String, PublicationObservation)]) {
    use std::fmt::Write as _;

    let capable = observations
        .iter()
        .filter(|(_, observation)| observation.atomic_capable)
        .count();
    let sum = |field: fn(&PublicationHealth) -> u64| {
        observations
            .iter()
            .map(|(_, observation)| field(&observation.cumulative))
            .fold(0_u64, u64::saturating_add)
    };
    writeln!(body, "lmt_agents_atomic_publication_capable {capable}").expect("String write");
    writeln!(
        body,
        "lmt_publication_commits_total{{outcome=\"succeeded\"}} {}",
        sum(|health| health.commits_succeeded_total)
    )
    .expect("String write");
    writeln!(
        body,
        "lmt_publication_commits_total{{outcome=\"failed\"}} {}",
        sum(|health| health.commits_failed_total)
    )
    .expect("String write");
    writeln!(
        body,
        "lmt_publication_visibility_to_durability_milliseconds_total {}",
        sum(|health| health.visibility_to_durability_milliseconds_total)
    )
    .expect("String write");
    writeln!(
        body,
        "lmt_publication_visibility_to_durability_samples_total {}",
        sum(|health| health.visibility_to_durability_samples_total)
    )
    .expect("String write");
    writeln!(
        body,
        "lmt_publication_preflight_rejections_total {}",
        sum(|health| health.preflight_rejections_total)
    )
    .expect("String write");
    writeln!(
        body,
        "lmt_publication_gc_failures_total {}",
        sum(|health| health.gc_failures_total)
    )
    .expect("String write");
    for (node, observation) in observations {
        let node = prometheus_label(node);
        let health = &observation.cumulative;
        if let Some(bytes) = health.publication_root_free_bytes {
            writeln!(body, "lmt_agent_publication_root_free_bytes{{node=\"{node}\"}} {bytes}").expect("String write");
        }
        writeln!(
            body,
            "lmt_agent_publication_gc_backlog_generations{{node=\"{node}\"}} {}",
            health.gc_backlog_generations
        )
        .expect("String write");
        writeln!(
            body,
            "lmt_agent_publication_degraded{{node=\"{node}\"}} {}",
            u8::from(health.degraded)
        )
        .expect("String write");
        writeln!(
            body,
            "lmt_agent_publication_fenced_records{{node=\"{node}\"}} {}",
            health.fenced_records
        )
        .expect("String write");
        writeln!(
            body,
            "lmt_agent_publication_recovery_records{{node=\"{node}\"}} {}",
            health.recovery_records
        )
        .expect("String write");
        if let Some(reason) = &health.admission_block_reason {
            writeln!(
                body,
                "lmt_agent_publication_admission_blocked{{node=\"{node}\",reason=\"{}\"}} 1",
                publication_admission_reason(reason)
            )
            .expect("String write");
        }
    }
}

const fn publication_admission_reason(reason: &PublicationAdmissionBlockReason) -> &'static str {
    match reason {
        PublicationAdmissionBlockReason::Fence => "fence",
        PublicationAdmissionBlockReason::Recovery => "recovery",
        PublicationAdmissionBlockReason::GenerationBound => "generation_bound",
        PublicationAdmissionBlockReason::FreeSpaceReserve => "free_space_reserve",
        PublicationAdmissionBlockReason::GcFailure => "gc_failure",
        PublicationAdmissionBlockReason::InvalidLocalState => "invalid_local_state",
    }
}

fn append_entity_metrics(
    body: &mut String,
    mirrors: Vec<lmt_store::MirrorOperationalRecord>,
    nodes: Vec<lmt_store::NodeRecord>,
    now: i64,
    offline_after: Duration,
) {
    use std::fmt::Write as _;

    for mirror in mirrors {
        let name = prometheus_label(&mirror.name);
        let node = prometheus_label(&mirror.owner_node);
        writeln!(
            *body,
            "lmt_mirror_due{{mirror=\"{name}\"}} {}",
            u8::from(mirror.due_since_ms.is_some())
        )
        .expect("String write");
        writeln!(
            *body,
            "lmt_mirror_last_success_timestamp_seconds{{mirror=\"{name}\",node=\"{node}\"}} {}",
            mirror.last_success_at_ms.unwrap_or(0) / 1000
        )
        .expect("String write");
        writeln!(
            *body,
            "lmt_mirror_last_terminal_timestamp_seconds{{mirror=\"{name}\",node=\"{node}\"}} {}",
            mirror.last_terminal_at_ms.unwrap_or(0) / 1000
        )
        .expect("String write");
    }
    for node in nodes {
        let name = prometheus_label(&node.name);
        let online = node.last_seen_at_ms.is_some_and(|seen| {
            now.saturating_sub(seen) <= i64::try_from(offline_after.as_millis()).unwrap_or(i64::MAX)
        });
        writeln!(*body, "lmt_node_online{{node=\"{name}\"}} {}", u8::from(online)).expect("String write");
        writeln!(
            *body,
            "lmt_node_last_seen_timestamp_seconds{{node=\"{name}\"}} {}",
            node.last_seen_at_ms.unwrap_or(0) / 1000
        )
        .expect("String write");
        writeln!(
            *body,
            "lmt_node_mirror_root_free_bytes{{node=\"{name}\"}} {}",
            node.mirror_root_free_bytes.unwrap_or(0)
        )
        .expect("String write");
    }
}

fn prometheus_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}
async fn validate(
    State(s): State<AppState>,
    h: HeaderMap,
    Json(r): Json<BundleRequest>,
) -> Result<Json<ValidationResponse>, Failure> {
    operator(&h, &s)?;
    Ok(Json(match canonicalize_bundle(&ConfigBundle { files: r.files }) {
        Ok(b) => ValidationResponse {
            valid: true,
            bundle_hash: Some(b.bundle_hash),
            errors: vec![],
        },
        Err(e) => ValidationResponse {
            valid: false,
            bundle_hash: None,
            errors: e.into_iter().map(|x| x.to_string()).collect(),
        },
    }))
}
async fn plan(
    State(s): State<AppState>,
    h: HeaderMap,
    Json(r): Json<BundleRequest>,
) -> Result<Json<PlanResponse>, Failure> {
    operator(&h, &s)?;
    let b = canonicalize_bundle(&ConfigBundle { files: r.files }).map_err(config_error)?;
    Ok(Json(plan_view(s.store.plan(&b).await?)))
}
async fn apply(
    State(s): State<AppState>,
    h: HeaderMap,
    Json(r): Json<ApplyRequest>,
) -> Result<Json<ApplyResponse>, Failure> {
    operator(&h, &s)?;
    let b = canonicalize_bundle(&ConfigBundle { files: r.files }).map_err(config_error)?;
    if s.store
        .plan(&b)
        .await?
        .changes
        .iter()
        .any(|c| c.kind == ChangeKind::Move)
        && !r.acknowledge_moves
    {
        return Err(Failure::conflict(
            "move_acknowledgement_required",
            "node move requires acknowledgement",
        ));
    }
    let p = s.store.apply(&b, r.base_revision, "api", s.now_ms()).await?;
    s.notify.notify_waiters();
    Ok(Json(ApplyResponse {
        revision: p.base_revision,
        bundle_hash: p.bundle_hash,
        changes: change_views(p.changes),
    }))
}
async fn mirrors(State(s): State<AppState>, h: HeaderMap) -> Result<Json<Vec<MirrorView>>, Failure> {
    operator(&h, &s)?;
    Ok(Json(
        s.store
            .list_mirrors()
            .await?
            .into_iter()
            .map(|m| MirrorView {
                name: m.name,
                managed: m.managed,
                enabled: m.enabled,
                owner_node: m.owner_node,
                current_generation: m.current_generation,
                next_due_at: m.next_due_at_ms.map(timestamp),
                scheduled_due_since: m.scheduled_due_since_ms.map(timestamp),
            })
            .collect(),
    ))
}
async fn mirror(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<MirrorView>, Failure> {
    operator(&h, &s)?;
    let m = s
        .store
        .get_mirror(&name)
        .await?
        .ok_or_else(|| Failure::not_found("mirror_not_found"))?;
    Ok(Json(MirrorView {
        name: m.name,
        managed: m.managed,
        enabled: m.enabled,
        owner_node: m.owner_node,
        current_generation: m.current_generation,
        next_due_at: m.next_due_at_ms.map(timestamp),
        scheduled_due_since: m.scheduled_due_since_ms.map(timestamp),
    }))
}
async fn manual(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(name): AxumPath<String>,
    Json(r): Json<ManualRunRequest>,
) -> Result<Json<RunView>, Failure> {
    operator(&h, &s)?;
    if r.trigger != lmt_core::RunTrigger::Manual || r.request_id.is_empty() {
        return Err(Failure::bad("invalid_request", "invalid manual request"));
    }
    let result = run_view(services::create_manual_run(&s.store, &name, &r.request_id, s.now_ms()).await?);
    s.notify.notify_waiters();
    Ok(Json(result))
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunListQuery {
    mirror: Option<String>,
    node: Option<String>,
    state: Option<lmt_core::RunState>,
    trigger: Option<lmt_core::RunTrigger>,
    limit: Option<u32>,
    before: Option<String>,
}

async fn runs(
    State(s): State<AppState>,
    h: HeaderMap,
    Query(query): Query<RunListQuery>,
) -> Result<Json<Vec<RunView>>, Failure> {
    operator(&h, &s)?;
    Ok(Json(
        s.store
            .query_runs(lmt_store::RunQuery {
                mirror: query.mirror,
                node: query.node,
                state: query.state,
                trigger: query.trigger,
                limit: query.limit.unwrap_or(50),
                before: query.before,
            })
            .await?
            .into_iter()
            .map(run_view)
            .collect(),
    ))
}
async fn run(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<RunDetail>, Failure> {
    operator(&h, &s)?;
    let r = s
        .store
        .get_run(&id)
        .await?
        .ok_or_else(|| Failure::not_found("run_not_found"))?;
    Ok(Json(RunDetail {
        run: run_view(r),
        attempts: s
            .store
            .list_attempts(&id)
            .await?
            .into_iter()
            .map(attempt_view)
            .collect(),
    }))
}
async fn cancel_run(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<RunView>, Failure> {
    operator(&h, &s)?;
    s.store
        .get_run(&id)
        .await?
        .ok_or_else(|| Failure::not_found("run_not_found"))?;
    let result = services::request_cancellation(&s.store, &id, s.now_ms()).await?;
    if result.newly_requested {
        let counter = if result.run.state == lmt_core::RunState::Cancelled {
            &s.metrics.cancellations_immediate
        } else {
            &s.metrics.cancellations_dispatched
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    s.notify.notify_waiters();
    Ok(Json(run_view(result.run)))
}
async fn attempts(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Vec<AttemptView>>, Failure> {
    operator(&h, &s)?;
    Ok(Json(
        s.store
            .list_attempts(&id)
            .await?
            .into_iter()
            .map(attempt_view)
            .collect(),
    ))
}
async fn nodes(State(s): State<AppState>, h: HeaderMap) -> Result<Json<Vec<NodeView>>, Failure> {
    operator(&h, &s)?;
    let now = s.now_ms();
    Ok(Json(
        s.store
            .list_nodes()
            .await?
            .into_iter()
            .map(|n| node_view(n, now, s.offline_after))
            .collect(),
    ))
}
async fn node(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<NodeView>, Failure> {
    operator(&h, &s)?;
    let n = s
        .store
        .list_nodes()
        .await?
        .into_iter()
        .find(|n| n.name == name)
        .ok_or_else(|| Failure::not_found("node_not_found"))?;
    Ok(Json(node_view(n, s.now_ms(), s.offline_after)))
}
async fn replace_binding(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<BindingReplaceRequest>,
) -> Result<Json<NodeView>, Failure> {
    operator(&h, &s)?;
    if !s.store.list_nodes().await?.iter().any(|node| node.name == name) {
        return Err(Failure::not_found("node_not_found"));
    }
    s.store
        .replace_agent_binding(&name, &request.agent_id, request.acknowledge_execution_risk)
        .await?;
    let node = s
        .store
        .list_nodes()
        .await?
        .into_iter()
        .find(|node| node.name == name)
        .ok_or_else(|| Failure::not_found("node_not_found"))?;
    Ok(Json(node_view(node, s.now_ms(), s.offline_after)))
}
async fn issue_credential(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<CredentialIssueRequest>,
) -> Result<Response, Failure> {
    operator(&h, &s)?;
    let name = NodeName::new(name).map_err(|error| Failure::bad("invalid_node_name", error.to_string()))?;
    let mut secret = [0_u8; 32];
    OsRng.try_fill_bytes(&mut secret).map_err(|_| {
        Failure::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "entropy_unavailable",
            "token generation failed",
        )
    })?;
    let token = format!("lmt_a_{}", hex::encode(secret));
    let credential = s
        .store
        .issue_credential(
            name.as_str(),
            &ulid::Ulid::new().to_string(),
            request.label.as_deref(),
            &token,
            s.now_ms(),
        )
        .await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(CredentialIssueResponse {
            credential: credential_view(credential),
            token,
        }),
    )
        .into_response())
}
async fn credentials(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<Vec<CredentialView>>, Failure> {
    operator(&h, &s)?;
    if !s.store.list_nodes().await?.iter().any(|node| node.name == name) {
        return Err(Failure::not_found("node_not_found"));
    }
    Ok(Json(
        s.store
            .list_credentials(&name)
            .await?
            .into_iter()
            .map(credential_view)
            .collect(),
    ))
}
async fn revoke_credential(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath((name, id)): AxumPath<(String, String)>,
) -> Result<Json<CredentialView>, Failure> {
    operator(&h, &s)?;
    Ok(Json(credential_view(
        s.store.revoke_credential(&name, &id, s.now_ms()).await?,
    )))
}
async fn log_maintenance_plan(State(s): State<AppState>, h: HeaderMap) -> Result<Json<LogMaintenancePlan>, Failure> {
    operator(&h, &s)?;
    Ok(Json(plan_log_maintenance(&s).await?))
}
async fn log_maintenance_run(State(s): State<AppState>, h: HeaderMap) -> Result<Json<LogMaintenancePlan>, Failure> {
    operator(&h, &s)?;
    Ok(Json(execute_log_maintenance(&s).await?))
}

fn backup_directory(state: &AppState) -> Result<PathBuf, Failure> {
    state.backup_dir.clone().ok_or_else(|| {
        Failure::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "backup_not_configured",
            "backup directory is not configured",
        )
    })
}

async fn create_backup(State(s): State<AppState>, h: HeaderMap) -> Result<Json<BackupManifest>, Failure> {
    operator(&h, &s)?;
    let directory = backup_directory(&s)?;
    let guard = s
        .backup_lock
        .clone()
        .try_lock_owned()
        .map_err(|_| Failure::conflict("backup_busy", "another backup is in progress"))?;
    let source = s.database_path.clone();
    let result = tokio::task::spawn_blocking(move || backup::create(&source, &directory)).await;
    drop(guard);
    let manifest = match result {
        Ok(Ok(manifest)) => manifest,
        Ok(Err(error)) => {
            s.metrics.backup_failures.fetch_add(1, Ordering::Relaxed);
            return Err(Failure::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "backup_invalid",
                error.to_string(),
            ));
        }
        Err(error) => {
            s.metrics.backup_failures.fetch_add(1, Ordering::Relaxed);
            return Err(Failure::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "backup_invalid",
                error.to_string(),
            ));
        }
    };
    if let Some(seconds) = backup_manifest_timestamp_seconds(&manifest) {
        s.metrics.backup_last_success_seconds.store(seconds, Ordering::Relaxed);
    }
    Ok(Json(manifest))
}

async fn backups(State(s): State<AppState>, h: HeaderMap) -> Result<Json<BackupListResponse>, Failure> {
    operator(&h, &s)?;
    let directory = backup_directory(&s)?;
    let backups = tokio::task::spawn_blocking(move || backup::list(&directory))
        .await
        .map_err(|error| Failure::new(StatusCode::INTERNAL_SERVER_ERROR, "backup_invalid", error.to_string()))?
        .map_err(|error| Failure::new(StatusCode::INTERNAL_SERVER_ERROR, "backup_invalid", error.to_string()))?;
    Ok(Json(BackupListResponse { backups }))
}

async fn verify_backup(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<BackupVerifyResponse>, Failure> {
    operator(&h, &s)?;
    let directory = backup_directory(&s)?;
    let manifest = tokio::task::spawn_blocking(move || backup::verify(&directory, &id))
        .await
        .map_err(|error| Failure::bad("backup_invalid", error.to_string()))?
        .map_err(|error| Failure::bad("backup_invalid", error.to_string()))?;
    Ok(Json(BackupVerifyResponse {
        backup: manifest,
        valid: true,
    }))
}

async fn operational_status(state: &AppState) -> Result<StatusResponse, Failure> {
    let counts = state.store.operational_counts().await?;
    let mirrors = state
        .store
        .mirror_operational_status()
        .await?
        .into_iter()
        .map(|mirror| MirrorStatusView {
            name: mirror.name,
            node: mirror.owner_node,
            enabled: mirror.enabled,
            current_run_state: mirror.current_run_state,
            current_run_created_at_ms: mirror.current_run_created_at_ms,
            last_run_state: mirror.last_run_state,
            last_terminal_at_ms: mirror.last_terminal_at_ms,
            last_success_at_ms: mirror.last_success_at_ms,
            next_due_at_ms: mirror.next_due_at_ms,
            due_since_ms: mirror.due_since_ms,
        })
        .collect();
    let now = state.now_ms();
    let offline_after = i64::try_from(state.offline_after.as_millis()).unwrap_or(i64::MAX);
    let publication_observations = state.publication_observations.lock().await.clone();
    let nodes = state
        .store
        .list_nodes()
        .await?
        .into_iter()
        .map(|node| {
            let publication = publication_observations.get(&node.name);
            NodeStatusView {
                name: node.name,
                online: node
                    .last_seen_at_ms
                    .is_some_and(|seen| now.saturating_sub(seen) <= offline_after),
                bound: node.bound_agent_id.is_some(),
                last_seen_at_ms: node.last_seen_at_ms,
                active_runs: node.active_runs,
                max_concurrent_runs: node.max_concurrent_runs,
                mirror_root_free_bytes: node.mirror_root_free_bytes,
                atomic_publication_capable: publication.map(|observation| observation.atomic_capable),
                publication_health: publication.map(|observation| observation.cumulative.clone()),
            }
        })
        .collect();
    let (schema_version, _) = state.store.database_diagnostics().await?;
    Ok(StatusResponse {
        version: env!("CARGO_PKG_VERSION").into(),
        schema_version,
        config_revision: state.store.current_revision().await?,
        runs_pending: counts.pending_runs,
        runs_running: counts.running_runs,
        mirrors_due: counts.due_mirrors,
        run_logs_stored_bytes: counts.stored_log_bytes,
        mirrors,
        nodes,
    })
}

async fn status(State(s): State<AppState>, h: HeaderMap) -> Result<Json<StatusResponse>, Failure> {
    if !s.public_status {
        operator(&h, &s)?;
    }
    Ok(Json(operational_status(&s).await?))
}

fn doctor_check(id: &str, status: DoctorCheckStatus, message: impl Into<String>) -> DoctorCheck {
    DoctorCheck {
        id: id.into(),
        status,
        message: message.into(),
    }
}

fn publication_doctor_checks(status: &StatusResponse) -> Vec<DoctorCheck> {
    let publication_nodes = status
        .nodes
        .iter()
        .filter(|node| node.publication_health.is_some())
        .count();
    let incapable = status
        .nodes
        .iter()
        .filter(|node| node.publication_health.is_some() && node.atomic_publication_capable != Some(true))
        .count();
    let degraded = status
        .nodes
        .iter()
        .filter(|node| node.publication_health.as_ref().is_some_and(|health| health.degraded))
        .count();
    let fenced = status
        .nodes
        .iter()
        .filter_map(|node| node.publication_health.as_ref())
        .fold(0_u32, |total, health| total.saturating_add(health.fenced_records));
    let recovery = status
        .nodes
        .iter()
        .filter_map(|node| node.publication_health.as_ref())
        .fold(0_u32, |total, health| total.saturating_add(health.recovery_records));
    vec![
        doctor_check(
            "publication.capability",
            if incapable == 0 {
                DoctorCheckStatus::Ok
            } else {
                DoctorCheckStatus::Critical
            },
            format!("{publication_nodes} publication-configured Agent(s), {incapable} without atomic_exchange_v1"),
        ),
        doctor_check(
            "publication.health",
            if fenced > 0 || recovery > 0 {
                DoctorCheckStatus::Critical
            } else if degraded > 0 {
                DoctorCheckStatus::Warning
            } else {
                DoctorCheckStatus::Ok
            },
            format!("{degraded} degraded Agent(s), {fenced} fence record(s), {recovery} recovery record(s)"),
        ),
    ]
}

fn filesystem_check(id: &str, path: &Path) -> DoctorCheck {
    match nix::sys::statvfs::statvfs(path) {
        Ok(stat) => doctor_check(
            id,
            DoctorCheckStatus::Ok,
            format!(
                "{} bytes available",
                stat.blocks_available().saturating_mul(stat.fragment_size())
            ),
        ),
        Err(error) => doctor_check(id, DoctorCheckStatus::Critical, format!("{}: {error}", path.display())),
    }
}

async fn doctor(State(s): State<AppState>, h: HeaderMap) -> Result<Json<DoctorResponse>, Failure> {
    operator(&h, &s)?;
    let status = operational_status(&s).await?;
    let (_, database_ok) = s.store.database_diagnostics().await?;
    let mut checks = vec![
        doctor_check(
            "server.version",
            DoctorCheckStatus::Ok,
            format!("LMT {} schema {}", status.version, status.schema_version),
        ),
        doctor_check(
            "database.quick_check",
            if database_ok {
                DoctorCheckStatus::Ok
            } else {
                DoctorCheckStatus::Critical
            },
            if database_ok {
                "SQLite quick_check passed"
            } else {
                "SQLite quick_check failed"
            },
        ),
        doctor_check(
            "config.revision",
            DoctorCheckStatus::Ok,
            format!("configuration revision {}", status.config_revision),
        ),
    ];
    if let Some(parent) = s.database_path.parent() {
        checks.push(filesystem_check("filesystem.database", parent));
    }
    checks.push(filesystem_check("filesystem.logs", &s.log_dir));
    if let Some(directory) = &s.backup_dir {
        checks.push(filesystem_check("filesystem.backups", directory));
    }
    checks.extend(doctor_operational_checks(&s, &status).await?);
    let healthy = checks.iter().all(|check| check.status == DoctorCheckStatus::Ok);
    Ok(Json(DoctorResponse { healthy, checks }))
}

async fn doctor_operational_checks(s: &AppState, status: &StatusResponse) -> Result<Vec<DoctorCheck>, Failure> {
    let offline = status.nodes.iter().filter(|node| !node.online).count();
    let mut checks = Vec::new();
    checks.push(doctor_check(
        "nodes.online",
        if offline == 0 {
            DoctorCheckStatus::Ok
        } else {
            DoctorCheckStatus::Critical
        },
        format!("{offline} offline Node(s)"),
    ));
    let unbound = status.nodes.iter().filter(|node| !node.bound).count();
    checks.push(doctor_check(
        "agents.binding",
        if unbound == 0 {
            DoctorCheckStatus::Ok
        } else {
            DoctorCheckStatus::Warning
        },
        format!("{unbound} unbound Node(s)"),
    ));
    checks.extend(publication_doctor_checks(status));
    checks.push(doctor_check(
        "mirrors.due",
        if status.mirrors_due == 0 {
            DoctorCheckStatus::Ok
        } else {
            DoctorCheckStatus::Warning
        },
        format!("{} due Mirror(s)", status.mirrors_due),
    ));
    let stale_threshold = i64::try_from(s.offline_after.as_millis())
        .unwrap_or(i64::MAX)
        .saturating_mul(2);
    let stale = status
        .mirrors
        .iter()
        .filter(|mirror| {
            mirror
                .current_run_created_at_ms
                .is_some_and(|created| s.now_ms().saturating_sub(created) > stale_threshold)
        })
        .count();
    checks.push(doctor_check(
        "runs.stale_nonterminal",
        if stale == 0 {
            DoctorCheckStatus::Ok
        } else {
            DoctorCheckStatus::Warning
        },
        format!("{stale} suspicious stale Run(s)"),
    ));
    let mut missing_logs = 0_u64;
    for entry in s.store.log_retention_entries().await? {
        if entry.expired_at_ms.is_none()
            && entry.stored_bytes > 0
            && fs::metadata(log_path(&s.log_dir, &entry.run_id, entry.attempt_no))
                .await
                .is_err()
        {
            missing_logs += 1;
        }
    }
    checks.push(doctor_check(
        "logs.files",
        if missing_logs == 0 {
            DoctorCheckStatus::Ok
        } else {
            DoctorCheckStatus::Critical
        },
        format!("{missing_logs} unexpected missing Run-log file(s)"),
    ));
    if let Some(check) = backup_doctor_check(s).await? {
        checks.push(check);
    }
    checks.push(doctor_check(
        "credentials.inline_deprecated",
        if s.deprecated_inline_credentials {
            DoctorCheckStatus::Warning
        } else {
            DoctorCheckStatus::Ok
        },
        if s.deprecated_inline_credentials {
            "deprecated inline credentials are configured"
        } else {
            "no deprecated inline credentials configured"
        },
    ));
    Ok(checks)
}

async fn backup_doctor_check(s: &AppState) -> Result<Option<DoctorCheck>, Failure> {
    let Some(directory) = s.backup_dir.clone() else {
        return Ok(None);
    };
    let backups = tokio::task::spawn_blocking(move || backup::list(&directory))
        .await
        .map_err(|error| Failure::new(StatusCode::INTERNAL_SERVER_ERROR, "backup_invalid", error.to_string()))?
        .map_err(|error| Failure::new(StatusCode::INTERNAL_SERVER_ERROR, "backup_invalid", error.to_string()))?;
    Ok(Some(doctor_check(
        "backup.latest",
        if backups.is_empty() {
            DoctorCheckStatus::Warning
        } else {
            DoctorCheckStatus::Ok
        },
        backups.first().map_or_else(
            || "no completed backup".into(),
            |backup| format!("latest backup {}", backup.created_at),
        ),
    )))
}
async fn poll(State(s): State<AppState>, h: HeaderMap, Json(r): Json<PollRequest>) -> Result<Response, Failure> {
    let credential = agent(&h, &s).await?;
    let node = credential.node;
    if r.protocol_version != "v1alpha1" {
        return Err(Failure::bad(
            "unsupported_protocol_version",
            "only v1alpha1 is supported",
        ));
    }
    s.store
        .observe_node(NodeObservation {
            node: node.clone(),
            agent_version: r.agent_version.clone(),
            agent_instance_id: r.agent_instance_id.clone(),
            agent_boot_id: r.agent_boot_id.clone(),
            active_runs: r.capacity.active_runs,
            max_concurrent_runs: r.capacity.max_concurrent_runs,
            mirror_root_free_bytes: r.capacity.mirror_root_free_bytes,
            mirror_root: r.mirror_root.clone(),
            observed_at_ms: s.now_ms(),
        })
        .await?;
    if let Some(health) = r.publication_health.clone() {
        let atomic_capable = r.capabilities.iter().any(|capability| capability == ATOMIC_EXCHANGE_V1);
        let mut observations = s.publication_observations.lock().await;
        observations
            .entry(node.clone())
            .and_modify(|observation| {
                observation.update(&r.agent_instance_id, &r.agent_boot_id, atomic_capable, health.clone());
            })
            .or_insert_with(|| {
                PublicationObservation::new(
                    r.agent_instance_id.clone(),
                    r.agent_boot_id.clone(),
                    atomic_capable,
                    health,
                )
            });
    } else {
        s.publication_observations.lock().await.remove(&node);
    }
    let _ = s
        .store
        .mark_credential_used(&node, &credential.credential_id, s.now_ms())
        .await?;
    s.metrics.polls.fetch_add(1, Ordering::Relaxed);
    s.notify.notify_waiters();
    let supports_execution_identity = r
        .capabilities
        .iter()
        .any(|capability| capability == EXECUTION_IDENTITY_V1);
    if let Some(a) = services::next_action_for_agent(
        &s.store,
        &node,
        &r.mirror_root,
        r.publication_root.as_deref(),
        &r.capabilities,
        s.now_ms(),
    )
    .await?
    {
        return Ok(action(a, supports_execution_identity).into_response());
    }
    let _ = tokio::time::timeout(s.poll_wait, s.notify.notified()).await;
    if let Some(a) = services::next_action_for_agent(
        &s.store,
        &node,
        &r.mirror_root,
        r.publication_root.as_deref(),
        &r.capabilities,
        s.now_ms(),
    )
    .await?
    {
        return Ok(action(a, supports_execution_identity).into_response());
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}
fn action(a: lmt_store::PollAction, supports_execution_identity: bool) -> Json<PollResponse> {
    Json(PollResponse {
        actions: vec![match a {
            lmt_store::PollAction::StartAttempt {
                run_id,
                attempt_no,
                mirror_name,
                spec_hash,
                spec,
            } => AgentAction::StartAttempt {
                run_id,
                attempt: attempt_no,
                spec_hash,
                execution_identity: supports_execution_identity.then_some(ExecutionIdentity { mirror: mirror_name }),
                spec,
            },
            lmt_store::PollAction::CancelAttempt {
                run_id,
                attempt_no,
                spec_hash,
            } => AgentAction::CancelAttempt {
                run_id,
                attempt: attempt_no,
                spec_hash,
            },
        }],
    })
}
async fn event(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath((id, no)): AxumPath<(String, u32)>,
    Json(r): Json<EventRequest>,
) -> Result<Json<EventResponse>, Failure> {
    let credential = agent(&h, &s).await?;
    attempt_auth(&s, &credential.node, &id, no).await?;
    let terminal_state = r.state;
    let applied = services::apply_attempt_event(
        &s.store,
        &id,
        no,
        &AttemptEvent {
            event_sequence: r.event_sequence,
            state: r.state,
            agent_instance_id: r.agent_instance_id,
            accepted_at_ms: parse_time(r.accepted_at.as_deref())?,
            started_at_ms: parse_time(r.started_at.as_deref())?,
            finished_at_ms: parse_time(r.finished_at.as_deref())?,
            exit_code: r.exit_code,
            failure_kind: r.failure_kind,
            failure_message: r.failure_message,
        },
        s.now_ms(),
    )
    .await?;
    s.notify.notify_waiters();
    s.metrics.events.fetch_add(1, Ordering::Relaxed);
    if terminal_state.is_terminal() && applied.newly_applied {
        let counter = match terminal_state {
            lmt_core::AttemptState::Succeeded => &s.metrics.attempts_succeeded,
            lmt_core::AttemptState::Failed => &s.metrics.attempts_failed,
            lmt_core::AttemptState::TimedOut => &s.metrics.attempts_timed_out,
            lmt_core::AttemptState::Cancelled => &s.metrics.attempts_cancelled,
            lmt_core::AttemptState::Rejected => &s.metrics.attempts_rejected,
            lmt_core::AttemptState::Interrupted => &s.metrics.attempts_interrupted,
            lmt_core::AttemptState::Queued | lmt_core::AttemptState::Accepted | lmt_core::AttemptState::Running => {
                unreachable!("non-terminal state passed terminal guard")
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if applied.retry_scheduled {
            s.metrics.retries_scheduled.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(Json(EventResponse {
        accepted_event_sequence: applied.accepted_event_sequence,
    }))
}
async fn upload_log(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath((id, no)): AxumPath<(String, u32)>,
    body: Bytes,
) -> Result<Response, Failure> {
    let credential = agent(&h, &s).await?;
    attempt_auth(&s, &credential.node, &id, no).await?;
    if body.len() > 1_048_576 {
        return Err(Failure::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "chunk too large",
        ));
    }
    let offset = header_u64(&h, "x-lmt-log-offset")?;
    let complete = h.get("x-lmt-log-complete").and_then(|v| v.to_str().ok()) == Some("true");
    let next = match append_log(&s, &id, no, offset, &body, complete).await {
        Ok(next) => next,
        Err(error) => {
            s.metrics.log_failures.fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
    };
    s.metrics
        .uploaded_bytes
        .fetch_add(u64::try_from(body.len()).unwrap_or(u64::MAX), Ordering::Relaxed);
    s.notify.notify_waiters();
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        "x-lmt-log-next-offset",
        HeaderValue::from_str(&next.to_string()).expect("valid"),
    );
    Ok(response)
}
#[derive(Deserialize)]
struct LogQuery {
    attempt: Option<u32>,
    #[serde(default)]
    offset: u64,
    limit: Option<u64>,
    wait: Option<String>,
}
async fn read_log(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<LogQuery>,
) -> Result<Response, Failure> {
    operator(&h, &s)?;
    let id = RunId::from_str(&id)
        .map_err(|_| Failure::not_found("run_not_found"))?
        .to_string();
    let attempts = s.store.list_attempts(&id).await?;
    let no = q
        .attempt
        .or_else(|| attempts.last().map(|attempt| attempt.attempt_no))
        .ok_or_else(|| Failure::not_found("attempt_not_found"))?;
    if !attempts.iter().any(|attempt| attempt.attempt_no == no) {
        return Err(Failure::not_found("attempt_not_found"));
    }
    let limit = q.limit.unwrap_or(65_536);
    if limit == 0 || limit > 1_048_576 {
        return Err(Failure::bad("invalid_log_limit", "limit must be between 1 and 1048576"));
    }
    let wait = q
        .wait
        .as_deref()
        .map(humantime::parse_duration)
        .transpose()
        .map_err(|_| Failure::bad("invalid_log_wait", "wait must be a duration"))?
        .unwrap_or_default()
        .min(Duration::from_secs(20));
    if !wait.is_zero() {
        let metadata = s.store.log_metadata(&id, no).await?;
        if metadata.as_ref().is_none_or(|metadata| {
            metadata.expired_at_ms.is_none() && !metadata.complete && q.offset >= metadata.stored_bytes
        }) {
            let _ = tokio::time::timeout(wait, s.notify.notified()).await;
        }
    }
    let lock = attempt_log_lock(&s, &id, no).await;
    let _guard = lock.lock().await;
    let mut data = vec![];
    let mut complete = false;
    let mut stored_bytes = 0;
    if let Some(metadata) = s.store.log_metadata(&id, no).await? {
        if metadata.expired_at_ms.is_some() {
            return Err(Failure::gone("log_expired", "Run log expired by retention policy"));
        }
        if let Err(error) = fs::metadata(log_path(&s.log_dir, &id, no)).await {
            return Err(if error.kind() == std::io::ErrorKind::NotFound {
                Failure::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "log_missing",
                    "Run log file is missing",
                )
            } else {
                Failure::io(error)
            });
        }
        complete = metadata.complete;
        stored_bytes = metadata.stored_bytes;
        if q.offset < metadata.stored_bytes {
            let take = (metadata.stored_bytes - q.offset).min(limit);
            let mut f = OpenOptions::new()
                .read(true)
                .open(log_path(&s.log_dir, &id, no))
                .await
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        Failure::new(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "log_missing",
                            "Run log file is missing",
                        )
                    } else {
                        Failure::io(error)
                    }
                })?;
            f.seek(std::io::SeekFrom::Start(q.offset)).await.map_err(Failure::io)?;
            data.resize(take as usize, 0);
            f.read_exact(&mut data).await.map_err(Failure::io)?;
        }
    }
    let next = q.offset + data.len() as u64;
    let complete = complete && next >= stored_bytes;
    let mut response = ([(header::CONTENT_TYPE, "application/octet-stream")], data).into_response();
    for (n, v) in [
        ("x-lmt-log-offset", q.offset.to_string()),
        ("x-lmt-log-next-offset", next.to_string()),
        ("x-lmt-log-complete", complete.to_string()),
    ] {
        response
            .headers_mut()
            .insert(n, HeaderValue::from_str(&v).expect("valid"));
    }
    Ok(response)
}
async fn append_log(s: &AppState, id: &str, no: u32, offset: u64, body: &[u8], complete: bool) -> Result<u64, Failure> {
    let id = RunId::from_str(id)
        .map_err(|_| Failure::bad("invalid_run_id", "invalid run id"))?
        .to_string();
    let lock = attempt_log_lock(s, &id, no).await;
    let _guard = lock.lock().await;
    if let Some(metadata) = s.store.log_metadata(&id, no).await?
        && metadata.expired_at_ms.is_some()
    {
        return Ok(metadata.stored_bytes);
    }
    let path = log_path(&s.log_dir, &id, no);
    if let Some(p) = path.parent() {
        fs::create_dir_all(p).await.map_err(Failure::io)?;
    }
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .await
        .map_err(Failure::io)?;
    let stored = f.metadata().await.map_err(Failure::io)?.len();
    if offset > stored {
        return Err(Failure::conflict("log_offset_ahead", "future offset"));
    }
    let overlap = (stored - offset).min(body.len() as u64) as usize;
    if overlap > 0 {
        f.seek(std::io::SeekFrom::Start(offset)).await.map_err(Failure::io)?;
        let mut old = vec![0; overlap];
        f.read_exact(&mut old).await.map_err(Failure::io)?;
        if old != body[..overlap] {
            return Err(Failure::conflict("log_content_conflict", "bytes differ"));
        }
    }
    let tail = &body[overlap..];
    if !tail.is_empty() {
        f.seek(std::io::SeekFrom::End(0)).await.map_err(Failure::io)?;
        f.write_all(tail).await.map_err(Failure::io)?;
        f.sync_data().await.map_err(Failure::io)?;
    }
    let next = stored + tail.len() as u64;
    s.store
        .update_log_metadata(&id, no, &format!("{id}/{no}.log"), next, complete, s.now_ms())
        .await?;
    Ok(next)
}
fn log_path(root: &Path, id: &str, no: u32) -> PathBuf {
    root.join(id).join(format!("{no}.log"))
}
fn bearer(h: &HeaderMap) -> Option<&str> {
    h.get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")
}
fn operator(h: &HeaderMap, s: &AppState) -> Result<(), Failure> {
    if bearer(h) == Some(s.operator_token.read().expect("operator token lock poisoned").as_str()) {
        Ok(())
    } else {
        s.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        Err(Failure::unauthorized())
    }
}
async fn agent(h: &HeaderMap, s: &AppState) -> Result<lmt_store::AuthenticatedCredential, Failure> {
    let Some(token) = bearer(h) else {
        s.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        return Err(Failure::unauthorized());
    };
    if let Some(credential) = s.store.authenticate_credential(token).await? {
        Ok(credential)
    } else {
        s.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        Err(Failure::unauthorized())
    }
}
async fn attempt_auth(s: &AppState, node: &str, id: &str, no: u32) -> Result<(), Failure> {
    if s.store.attempt_belongs_to_node(id, no, node).await? {
        Ok(())
    } else {
        Err(Failure::not_found("attempt_not_found"))
    }
}
fn header_u64(h: &HeaderMap, n: &str) -> Result<u64, Failure> {
    h.get(n)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| Failure::bad("invalid_log_offset", "invalid offset"))
}
fn parse_time(v: Option<&str>) -> Result<Option<i64>, Failure> {
    v.map(|x| {
        OffsetDateTime::parse(x, &Rfc3339)
            .map_err(|_| Failure::bad("invalid_timestamp", "RFC3339 required"))
            .map(|t| (t.unix_timestamp_nanos() / 1_000_000) as i64)
    })
    .transpose()
}
fn timestamp(ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .expect("timestamp")
        .format(&Rfc3339)
        .expect("format")
}
fn now_ms() -> i64 {
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
fn run_view(r: RunRecord) -> RunView {
    RunView {
        id: r.id,
        mirror_name: r.mirror_name,
        mirror_generation: r.mirror_generation,
        owner_node: r.owner_node,
        trigger: r.trigger,
        state: r.state,
        created_at: timestamp(r.created_at_ms),
        started_at: r.started_at_ms.map(timestamp),
        finished_at: r.finished_at_ms.map(timestamp),
        final_exit_code: r.final_exit_code,
        failure_kind: r.failure_kind,
        failure_message: r.failure_message,
        scheduled_for_at: r.scheduled_for_at_ms.map(timestamp),
        retry_due_at: r.retry_due_at_ms.map(timestamp),
        cancel_requested_at: r.cancel_requested_at_ms.map(timestamp),
    }
}
fn attempt_view(a: AttemptRecord) -> AttemptView {
    AttemptView {
        run_id: a.run_id,
        attempt_no: a.attempt_no,
        state: a.state,
        spec_hash: a.spec_hash,
        created_at: timestamp(a.created_at_ms),
        accepted_at: a.accepted_at_ms.map(timestamp),
        started_at: a.started_at_ms.map(timestamp),
        finished_at: a.finished_at_ms.map(timestamp),
        exit_code: a.exit_code,
        failure_kind: a.failure_kind,
        failure_message: a.failure_message,
        last_event_sequence: a.last_event_sequence,
    }
}
fn node_view(n: lmt_store::NodeRecord, now: i64, d: Duration) -> NodeView {
    NodeView {
        name: n.name,
        agent_version: n.agent_version,
        agent_instance_id: n.agent_instance_id,
        bound_agent_id: n.bound_agent_id,
        agent_boot_id: n.agent_boot_id,
        last_seen_at: n.last_seen_at_ms.map(timestamp),
        active_runs: n.active_runs,
        mirror_root_free_bytes: n.mirror_root_free_bytes,
        max_concurrent_runs: n.max_concurrent_runs,
        online: n.last_seen_at_ms.is_some_and(|x| now - x <= d.as_millis() as i64),
    }
}
fn credential_view(credential: lmt_store::CredentialRecord) -> CredentialView {
    CredentialView {
        id: credential.id,
        node: credential.node,
        label: credential.label,
        created_at: timestamp(credential.created_at_ms),
        last_used_at: credential.last_used_at_ms.map(timestamp),
        revoked_at: credential.revoked_at_ms.map(timestamp),
    }
}
fn plan_view(p: ConfigPlan) -> PlanResponse {
    PlanResponse {
        base_revision: p.base_revision,
        bundle_hash: p.bundle_hash,
        changes: change_views(p.changes),
    }
}
fn change_views(v: Vec<lmt_store::ConfigChange>) -> Vec<ConfigChange> {
    v.into_iter()
        .map(|c| ConfigChange {
            action: match c.kind {
                ChangeKind::Create => ChangeAction::Create,
                ChangeKind::Update => ChangeAction::Update,
                ChangeKind::Remove => ChangeAction::Remove,
                ChangeKind::Move => ChangeAction::Move,
            },
            mirror: c.mirror,
            from_generation: c.from_generation,
            to_generation: c.to_generation,
            from_node: c.from_node,
            to_node: c.to_node,
            publication_change: c.publication_change.map(|change| match change {
                lmt_store::PublicationChange::DirectToAtomic => PublicationChange::DirectToAtomic,
                lmt_store::PublicationChange::AtomicToDirect => PublicationChange::AtomicToDirect,
            }),
        })
        .collect()
}
fn config_error(e: Vec<lmt_core::ConfigError>) -> Failure {
    Failure::bad(
        "config_invalid",
        e.into_iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; "),
    )
}
#[derive(Debug)]
struct Failure {
    status: StatusCode,
    code: &'static str,
    message: String,
    details: BTreeMap<String, serde_json::Value>,
}
impl Failure {
    fn new(s: StatusCode, c: &'static str, m: impl Into<String>) -> Self {
        Self {
            status: s,
            code: c,
            message: m.into(),
            details: BTreeMap::new(),
        }
    }
    fn bad(c: &'static str, m: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, c, m)
    }
    fn conflict(c: &'static str, m: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, c, m)
    }
    fn not_found(c: &'static str) -> Self {
        Self::new(StatusCode::NOT_FOUND, c, "not found")
    }
    fn gone(c: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::GONE, c, message)
    }
    fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", "valid bearer token required")
    }
    fn io(e: std::io::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "log_io_error", e.to_string())
    }
}
impl From<StoreError> for Failure {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::RevisionConflict { current } => {
                let mut f = Self::conflict("config_revision_conflict", e.to_string());
                f.details.insert("current_revision".into(), current.into());
                f
            }
            StoreError::MirrorBusy { ref run_id } => {
                let mut f = Self::conflict("mirror_busy", e.to_string());
                f.details.insert("run_id".into(), run_id.clone().into());
                f
            }
            StoreError::MirrorNotFound => Self::not_found("mirror_not_found"),
            StoreError::MirrorIneligible => Self::conflict("mirror_ineligible", e.to_string()),
            StoreError::AgentBindingConflict {
                bound_agent_id,
                presented_agent_id,
            } => {
                let mut failure =
                    Self::conflict("agent_binding_conflict", "Node is bound to another Agent installation");
                failure.details.insert("bound_agent_id".into(), bound_agent_id.into());
                failure
                    .details
                    .insert("presented_agent_id".into(), presented_agent_id.into());
                failure
            }
            StoreError::BindingReplacementUnsafe => Self::conflict("binding_replacement_unsafe", e.to_string()),
            StoreError::CredentialNotFound => Self::not_found("credential_not_found"),
            StoreError::AttemptNotFound => Self::not_found("attempt_not_found"),
            StoreError::RequestConflict | StoreError::IllegalTransition { .. } => {
                Self::conflict("state_conflict", e.to_string())
            }
            StoreError::InvalidConfig(_) => Self::bad("config_invalid", e.to_string()),
            _ => Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal server error"),
        }
    }
}
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorEnvelope {
                error: ApiError {
                    code: self.code.into(),
                    message: self.message,
                    details: self.details,
                },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lmt_core::{BundleFile, ProcessRunSpec, RunState};
    use std::sync::atomic::AtomicI64;

    struct FakeClock(AtomicI64);

    fn direct_poll_action() -> lmt_store::PollAction {
        lmt_store::PollAction::StartAttempt {
            run_id: "01M30000000000000000000003".into(),
            attempt_no: 1,
            mirror_name: "example".into(),
            spec_hash: "sha256:m3-direct-fixture".into(),
            spec: ProcessRunSpec {
                runner: "process".into(),
                program: "/usr/bin/rsync".into(),
                args: vec![
                    "-aH".into(),
                    "--delete".into(),
                    "rsync://example.invalid/archive/".into(),
                    "/srv/mirrors/example".into(),
                ],
                cwd: None,
                timeout_seconds: 21_600,
                mirror_root: "/srv/mirrors".into(),
                target_dir: "/srv/mirrors/example".into(),
                publication: None,
            },
        }
    }

    #[test]
    fn start_attempt_identity_is_capability_gated_for_m3_agents() {
        let legacy_response = action(direct_poll_action(), false).0;
        assert_eq!(
            serde_json::to_string(&legacy_response).expect("legacy action bytes"),
            include_str!("../../../crates/lmt-protocol/tests/fixtures/m3/poll-response.json").trim_end()
        );
        let legacy = serde_json::to_value(legacy_response).expect("legacy action");
        assert!(legacy["actions"][0].get("execution_identity").is_none());

        let m4 = serde_json::to_value(action(direct_poll_action(), true).0).expect("M4 action");
        assert_eq!(m4["actions"][0]["execution_identity"]["mirror"], "example");
        assert!(m4["actions"][0]["spec"].get("publication").is_none());
    }

    #[test]
    fn production_server_example_has_explicit_valid_logging_and_secret_files() {
        let source = include_str!("../../../config/server.example.toml");
        let config: ServerConfig = toml::from_str(source).expect("production Server example");
        assert!(config.logging.is_some());
        assert!(config.operator_token.is_none());
        assert!(config.operator_token_file.is_some());
        assert!(toml::from_str::<ServerConfig>(&source.replace("format = \"json\"", "format = \"xml\"")).is_err());
    }

    impl Clock for FakeClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn success_event_and_duplicate_log_survive_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("lmt.db");
        let store = Store::open(&database).await.expect("store");
        fs::create_dir_all(directory.path().join("logs")).await.expect("logs");
        store
            .upsert_credential("node-a", "secret", 1)
            .await
            .expect("credential");
        let bundle = canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: "[mirror]\nname='demo'\ntarget='demo'\n[sync]\ntype='command'\nprogram='/bin/true'\n".into(),
            }],
        })
        .expect("bundle");
        store.apply(&bundle, 0, "test", 2).await.expect("apply");
        let run = services::create_manual_run(&store, "demo", "request", 3)
            .await
            .expect("run");
        services::next_action(&store, "node-a", "/tmp/mirrors", 4)
            .await
            .expect("poll")
            .expect("action");
        let state = AppState::new(
            store.clone(),
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        assert_eq!(
            append_log(&state, &run.id, 1, 0, b"[stdout] hello\n", false)
                .await
                .expect("append"),
            15
        );
        assert_eq!(
            append_log(&state, &run.id, 1, 0, b"[stdout] hello\n", true)
                .await
                .expect("retry"),
            15
        );
        services::apply_attempt_event(
            &store,
            &run.id,
            1,
            &AttemptEvent {
                event_sequence: 3,
                state: lmt_core::AttemptState::Succeeded,
                agent_instance_id: "instance".into(),
                accepted_at_ms: Some(1),
                started_at_ms: Some(2),
                finished_at_ms: Some(3),
                exit_code: Some(0),
                failure_kind: None,
                failure_message: None,
            },
            5,
        )
        .await
        .expect("event");
        drop(state);
        drop(store);
        let reopened = Store::open(database).await.expect("reopen");
        assert_eq!(
            reopened.get_run(&run.id).await.expect("query").expect("run").state,
            RunState::Succeeded
        );
        assert_eq!(
            fs::read(directory.path().join("logs").join(&run.id).join("1.log"))
                .await
                .expect("log"),
            b"[stdout] hello\n"
        );
    }

    #[tokio::test]
    async fn fake_server_clock_drives_durable_scheduled_run_and_interval_rearm() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("schedule.db");
        let store = Store::open(&database).await.expect("store");
        let bundle = canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: "[mirror]\nname='demo'\ntarget='demo'\n[schedule]\ninterval='1m'\n[sync]\ntype='command'\nprogram='/bin/true'\n[run]\nmax_attempts=2\nretry_delay_seconds=5\n".into(),
            }],
        })
        .expect("bundle");
        let clock = Arc::new(FakeClock(AtomicI64::new(0)));
        let mut state = AppState::new(
            store.clone(),
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        state.clock = clock.clone();
        store.apply(&bundle, 0, "test", state.now_ms()).await.expect("apply");
        clock.0.store(60_000, Ordering::SeqCst);
        assert_eq!(
            services::evaluate_schedules(&store, state.now_ms())
                .await
                .expect("tick")
                .evaluated,
            1
        );
        assert_eq!(
            store
                .get_mirror("demo")
                .await
                .expect("get")
                .expect("mirror")
                .scheduled_due_since_ms,
            Some(60_000)
        );
        drop(store);

        let reopened = Store::open(&database).await.expect("reopen");
        let action = services::next_action(&reopened, "node-a", "/tmp/mirrors", state.now_ms())
            .await
            .expect("poll")
            .expect("scheduled action");
        let run_id = match action {
            lmt_store::PollAction::StartAttempt { run_id, .. } => run_id,
            lmt_store::PollAction::CancelAttempt { .. } => panic!("unexpected cancellation"),
        };
        let run = reopened.get_run(&run_id).await.expect("get").expect("run");
        assert_eq!(run.trigger, lmt_core::RunTrigger::Scheduled);
        assert_eq!(run.scheduled_for_at_ms, Some(60_000));
        assert_eq!(run.max_attempts, 2);
        assert_eq!(reopened.list_runs().await.expect("runs").len(), 1);

        clock.0.store(70_000, Ordering::SeqCst);
        services::apply_attempt_event(
            &reopened,
            &run_id,
            1,
            &AttemptEvent {
                event_sequence: 1,
                state: lmt_core::AttemptState::Succeeded,
                agent_instance_id: "instance".into(),
                accepted_at_ms: None,
                started_at_ms: None,
                finished_at_ms: Some(70_000),
                exit_code: Some(0),
                failure_kind: None,
                failure_message: None,
            },
            state.now_ms(),
        )
        .await
        .expect("terminal");
        assert_eq!(
            reopened
                .get_mirror("demo")
                .await
                .expect("get")
                .expect("mirror")
                .next_due_at_ms,
            Some(130_000)
        );
    }

    #[tokio::test]
    async fn duplicate_terminal_events_and_cancellations_do_not_duplicate_semantic_metrics() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open_in_memory().await.expect("store");
        store
            .upsert_credential("node-a", "secret", 1)
            .await
            .expect("credential");
        let bundle = canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: "[mirror]\nname='demo'\ntarget='demo'\n[sync]\ntype='command'\nprogram='/bin/false'\n[run]\nmax_attempts=2\nretry_delay_seconds=5\n".into(),
            }],
        })
        .expect("bundle");
        store.apply(&bundle, 0, "test", 0).await.expect("apply");
        let run = services::create_manual_run(&store, "demo", "event-metrics", 1)
            .await
            .expect("run");
        services::next_action(&store, "node-a", "/tmp/mirrors", 2)
            .await
            .expect("poll")
            .expect("action");
        let mut state = AppState::new(
            store.clone(),
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        state.clock = Arc::new(FakeClock(AtomicI64::new(3)));
        let mut agent_headers = HeaderMap::new();
        agent_headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        let request = EventRequest {
            event_sequence: 1,
            state: lmt_core::AttemptState::Failed,
            agent_instance_id: "instance".into(),
            accepted_at: None,
            started_at: None,
            finished_at: None,
            exit_code: Some(1),
            failure_kind: None,
            failure_message: None,
        };

        for _ in 0..2 {
            let _ = event(
                State(state.clone()),
                agent_headers.clone(),
                AxumPath((run.id.clone(), 1)),
                Json(request.clone()),
            )
            .await
            .expect("event");
        }
        assert_eq!(state.metrics.events.load(Ordering::Relaxed), 2);
        assert_eq!(state.metrics.attempts_failed.load(Ordering::Relaxed), 1);
        assert_eq!(state.metrics.retries_scheduled.load(Ordering::Relaxed), 1);

        let cancel_store = Store::open_in_memory().await.expect("cancel store");
        cancel_store.apply(&bundle, 0, "test", 0).await.expect("apply");
        let cancellable = services::create_manual_run(&cancel_store, "demo", "cancel-metrics", 1)
            .await
            .expect("run");
        let cancel_state = AppState::new(
            cancel_store,
            directory.path().join("cancel-logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        let mut operator_headers = HeaderMap::new();
        operator_headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer operator"));
        for _ in 0..2 {
            let _ = cancel_run(
                State(cancel_state.clone()),
                operator_headers.clone(),
                AxumPath(cancellable.id.clone()),
            )
            .await
            .expect("cancel");
        }
        assert_eq!(cancel_state.metrics.cancellations_immediate.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn clean_install_credential_issue_then_authenticated_poll_establishes_binding() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open_in_memory().await.expect("store");
        let mut state = AppState::new(
            store.clone(),
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        state.poll_wait = Duration::ZERO;
        state.clock = Arc::new(FakeClock(AtomicI64::new(10)));
        let mut operator_headers = HeaderMap::new();
        operator_headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer operator"));

        let invalid = issue_credential(
            State(state.clone()),
            operator_headers.clone(),
            AxumPath("!invalid".into()),
            Json(CredentialIssueRequest { label: None }),
        )
        .await
        .expect_err("invalid Node name");
        assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid.code, "invalid_node_name");
        assert!(store.list_nodes().await.expect("nodes after invalid issue").is_empty());

        let response = issue_credential(
            State(state.clone()),
            operator_headers,
            AxumPath("node-a".into()),
            Json(CredentialIssueRequest {
                label: Some("first install".into()),
            }),
        )
        .await
        .expect("issue first credential");
        let issued: CredentialIssueResponse =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 16_384).await.expect("body"))
                .expect("issue response");

        let before_poll = store
            .list_nodes()
            .await
            .expect("nodes")
            .pop()
            .expect("bootstrapped node");
        assert_eq!(before_poll.name, "node-a");
        assert_eq!(before_poll.last_seen_at_ms, None);
        assert_eq!(before_poll.bound_agent_id, None);
        assert!(store.list_mirrors().await.expect("mirrors").is_empty());

        let request = PollRequest {
            protocol_version: "v1alpha1".into(),
            agent_version: "test-agent".into(),
            agent_instance_id: "installation-a".into(),
            agent_boot_id: "boot-a".into(),
            poll_sequence: 1,
            running: vec![],
            capacity: Capacity {
                mirror_root_free_bytes: Some(1_000),
                active_runs: 0,
                max_concurrent_runs: 1,
            },
            mirror_root: "/srv/mirrors".into(),
            capabilities: vec![],
            publication_root: None,
            publication_health: None,
        };
        let unauthenticated = poll(State(state.clone()), HeaderMap::new(), Json(request.clone()))
            .await
            .expect_err("unauthenticated poll");
        assert_eq!(unauthenticated.status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            store.list_nodes().await.expect("nodes")[0].bound_agent_id,
            None,
            "unauthenticated Agent poll established a binding"
        );

        let mut agent_headers = HeaderMap::new();
        agent_headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", issued.token)).expect("authorization header"),
        );
        let response = poll(State(state), agent_headers, Json(request))
            .await
            .expect("authenticated first poll");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let after_poll = store.list_nodes().await.expect("nodes").pop().expect("bound node");
        assert_eq!(after_poll.bound_agent_id.as_deref(), Some("installation-a"));
        assert_eq!(after_poll.agent_instance_id.as_deref(), Some("installation-a"));
        assert_eq!(after_poll.agent_boot_id.as_deref(), Some("boot-a"));
        assert_eq!(after_poll.last_seen_at_ms, Some(10));
        assert!(store.list_mirrors().await.expect("mirrors").is_empty());
    }

    #[tokio::test]
    async fn issued_credentials_are_secret_safe_revocable_and_operator_reload_is_fail_safe() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open_in_memory().await.expect("store");
        store
            .import_legacy_credential("node-a", "legacy", 1)
            .await
            .expect("legacy");
        let token_file = directory.path().join("operator.token");
        fs::write(&token_file, "operator\n").await.expect("operator token");
        let mut state = AppState::new(
            store.clone(),
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        state.operator_token_file = Some(token_file.clone());
        state.clock = Arc::new(FakeClock(AtomicI64::new(10)));
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer operator"));

        let response = issue_credential(
            State(state.clone()),
            headers.clone(),
            AxumPath("node-a".into()),
            Json(CredentialIssueRequest {
                label: Some("rotation".into()),
            }),
        )
        .await
        .expect("issue");
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        let issued: CredentialIssueResponse =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 16_384).await.expect("body"))
                .expect("issue response");
        assert!(issued.token.starts_with("lmt_a_"));
        assert_eq!(issued.token.len(), 70);
        let listed = credentials(State(state.clone()), headers.clone(), AxumPath("node-a".into()))
            .await
            .expect("list")
            .0;
        assert!(listed.iter().all(|credential| credential.id != issued.token));
        assert!(
            store
                .authenticate_credential(&issued.token)
                .await
                .expect("auth")
                .is_some()
        );

        let revoked = revoke_credential(
            State(state.clone()),
            headers,
            AxumPath(("node-a".into(), issued.credential.id)),
        )
        .await
        .expect("revoke")
        .0;
        assert!(revoked.revoked_at.is_some());
        assert!(
            store
                .authenticate_credential(&issued.token)
                .await
                .expect("auth")
                .is_none()
        );

        fs::write(&token_file, "\n").await.expect("empty replacement");
        assert!(state.reload_operator_token().await.is_err());
        assert_eq!(
            state.operator_token.read().expect("token").as_str(),
            "operator",
            "failed reload replaced the working credential"
        );
        fs::write(&token_file, "new-operator\n").await.expect("replacement");
        state.reload_operator_token().await.expect("reload");
        assert_eq!(state.operator_token.read().expect("token").as_str(), "new-operator");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn log_retention_is_safe_crash_recoverable_and_lock_registry_is_bounded() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Store::open_in_memory().await.expect("store");
        store
            .upsert_credential("node-a", "secret", 1)
            .await
            .expect("credential");
        let bundle = canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: "[mirror]\nname='demo'\ntarget='demo'\n[sync]\ntype='command'\nprogram='/bin/true'\n".into(),
            }],
        })
        .expect("bundle");
        store.apply(&bundle, 0, "test", 0).await.expect("apply");
        let clock = Arc::new(FakeClock(AtomicI64::new(0)));
        let mut state = AppState::new(
            store.clone(),
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        state.clock = clock.clone();
        let mut terminal_ids = Vec::new();
        for (index, (updated_at, bytes)) in [
            (10, b"1111".as_slice()),
            (20, b"22222"),
            (90, b"333333"),
            (95, b"4444444"),
        ]
        .into_iter()
        .enumerate()
        {
            let run = services::create_manual_run(&store, "demo", &format!("retention-{index}"), updated_at - 2)
                .await
                .expect("run");
            services::next_action(&store, "node-a", "/tmp/mirrors", updated_at - 1)
                .await
                .expect("poll")
                .expect("action");
            clock.0.store(updated_at, Ordering::SeqCst);
            append_log(&state, &run.id, 1, 0, bytes, true)
                .await
                .expect("complete log");
            services::apply_attempt_event(
                &store,
                &run.id,
                1,
                &AttemptEvent {
                    event_sequence: 1,
                    state: lmt_core::AttemptState::Succeeded,
                    agent_instance_id: "instance".into(),
                    accepted_at_ms: None,
                    started_at_ms: None,
                    finished_at_ms: Some(updated_at),
                    exit_code: Some(0),
                    failure_kind: None,
                    failure_message: None,
                },
                updated_at,
            )
            .await
            .expect("terminal");
            terminal_ids.push(run.id);
        }
        let active = services::create_manual_run(&store, "demo", "retention-active", 96)
            .await
            .expect("active");
        services::next_action(&store, "node-a", "/tmp/mirrors", 97)
            .await
            .expect("poll")
            .expect("action");
        clock.0.store(98, Ordering::SeqCst);
        append_log(&state, &active.id, 1, 0, b"active", false)
            .await
            .expect("active log");

        clock.0.store(100, Ordering::SeqCst);
        state.run_log_policy = Some(RunLogPolicy {
            retention: Some(Duration::from_millis(50)),
            max_total_bytes: None,
            maintenance_interval: Duration::from_secs(1),
        });
        let age_plan = plan_log_maintenance(&state).await.expect("age plan");
        assert_eq!(age_plan.candidates.len(), 2);
        assert!(
            age_plan
                .candidates
                .iter()
                .all(|candidate| candidate.run_id != active.id)
        );
        execute_log_maintenance(&state).await.expect("age retention");
        assert_eq!(
            store.operational_counts().await.expect("age counter").stored_log_bytes,
            19
        );
        assert_eq!(store.list_runs().await.expect("history").len(), 5);
        let expired_path = log_path(&state.log_dir, &terminal_ids[0], 1);
        assert!(!expired_path.exists());
        assert_eq!(
            append_log(&state, &terminal_ids[0], 1, 0, b"1111", true)
                .await
                .expect("late retransmission is acknowledged"),
            4
        );
        assert!(!expired_path.exists(), "late retransmission recreated an expired log");
        assert!(
            store
                .log_metadata(&terminal_ids[0], 1)
                .await
                .expect("metadata")
                .expect("expired metadata")
                .expired_at_ms
                .is_some()
        );
        let mut operator_headers = HeaderMap::new();
        operator_headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer operator"));
        let expired = read_log(
            State(state.clone()),
            operator_headers,
            AxumPath(terminal_ids[0].clone()),
            Query(LogQuery {
                attempt: None,
                offset: 0,
                limit: None,
                wait: None,
            }),
        )
        .await
        .expect_err("expired log");
        assert_eq!(expired.status, StatusCode::GONE);
        assert_eq!(expired.code, "log_expired");

        state.run_log_policy = Some(RunLogPolicy {
            retention: None,
            max_total_bytes: Some(14),
            maintenance_interval: Duration::from_secs(1),
        });
        let size_plan = plan_log_maintenance(&state).await.expect("size plan");
        assert_eq!(size_plan.candidates.len(), 1);
        assert_eq!(size_plan.candidates[0].run_id, terminal_ids[2]);
        execute_log_maintenance(&state).await.expect("size retention");
        assert_eq!(
            store.operational_counts().await.expect("size counter").stored_log_bytes,
            13
        );

        assert!(
            store
                .mark_log_expired(&terminal_ids[3], 1, 101)
                .await
                .expect("expire-before-unlink")
        );
        assert!(log_path(&state.log_dir, &terminal_ids[3], 1).exists());
        execute_log_maintenance(&state).await.expect("restart cleanup");
        assert!(!log_path(&state.log_dir, &terminal_ids[3], 1).exists());
        assert!(log_path(&state.log_dir, &active.id, 1).exists());

        fs::remove_file(log_path(&state.log_dir, &active.id, 1))
            .await
            .expect("inject unexpected missing log");
        let status = operational_status(&state).await.expect("status");
        let checks = doctor_operational_checks(&state, &status).await.expect("doctor checks");
        assert!(
            checks
                .iter()
                .any(|check| { check.id == "logs.files" && check.status == DoctorCheckStatus::Critical })
        );

        let transient = attempt_log_lock(&state, "transient-a", 1).await;
        drop(transient);
        let replacement = attempt_log_lock(&state, "transient-b", 1).await;
        assert!(state.log_locks.lock().await.len() <= 1);
        drop(replacement);
    }

    #[tokio::test]
    async fn metrics_and_public_status_are_bounded_and_sanitized_with_large_history() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("lmt.db");
        drop(Store::open(&database).await.expect("schema"));
        let mut connection = rusqlite::Connection::open(&database).expect("seed connection");
        let transaction = connection.transaction().expect("transaction");
        transaction
            .execute_batch(
                "INSERT INTO config_revisions(revision,bundle_hash,applied_at_ms,summary_json) VALUES(1,'h',1,'{}');
                 INSERT INTO nodes(name,registered_at_ms,last_seen_at_ms,active_runs,capabilities_json,bound_agent_id)
                   VALUES('node-a',1,1000,0,'{}','installation-a');
                 INSERT INTO mirrors(name,managed,enabled,owner_node,current_generation) VALUES('demo',1,1,'node-a',1);
                 INSERT INTO mirror_generations(mirror_name,generation,config_revision,owner_node,config_hash,config_toml,created_at_ms)
                   VALUES('demo',1,1,'node-a','h','source_url=\"https://secret@example.invalid/private\"\nmirror_root=\"/secret/path\"',1);
                 INSERT INTO mirror_schedule_state(mirror_name,next_due_at_ms,last_evaluated_at_ms,catch_up_pending,catch_up_since_ms)
                   VALUES('demo',2000,1000,1,1500);",
            )
            .expect("seed entities");
        {
            let mut insert_run = transaction
                .prepare("INSERT INTO runs(id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,finished_at_ms,max_attempts,retry_delay_ms) VALUES(?1,'demo',1,'node-a','manual','succeeded',?2,?2,1,0)")
                .expect("prepare runs");
            let mut insert_attempt = transaction
                .prepare("INSERT INTO attempts(run_id,attempt_no,state,spec_hash,spec_json,created_at_ms,finished_at_ms,dispatch_count) VALUES(?1,1,'succeeded','hash','{}',?2,?2,1)")
                .expect("prepare Attempts");
            let mut insert_log = transaction
                .prepare("INSERT INTO attempt_logs(run_id,attempt_no,relative_path,stored_bytes,complete,updated_at_ms) VALUES(?1,1,?1 || '/1.log',1,1,?2)")
                .expect("prepare log metadata");
            for index in 0..10_000_i64 {
                let id = format!("run-{index:05}");
                insert_run
                    .execute(rusqlite::params![id, index + 1])
                    .expect("historical Run");
                insert_attempt
                    .execute(rusqlite::params![id, index + 1])
                    .expect("historical Attempt");
                insert_log
                    .execute(rusqlite::params![id, index + 1])
                    .expect("historical log metadata");
            }
        }
        transaction.commit().expect("commit history");
        drop(connection);
        let store = Store::open(&database).await.expect("store");
        let mut state = AppState::new(
            store,
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        state.public_status = true;
        state.clock = Arc::new(FakeClock(AtomicI64::new(1_000)));
        let metrics_response = metrics(State(state.clone())).await.expect("metrics");
        let metrics_body = axum::body::to_bytes(metrics_response.into_body(), 128 * 1024)
            .await
            .expect("metrics body");
        let metrics_text = std::str::from_utf8(&metrics_body).expect("utf8");
        assert!(metrics_text.contains("lmt_runs_pending 0"));
        assert!(metrics_text.contains("lmt_mirrors_due 1"));
        assert!(metrics_text.contains("lmt_run_logs_stored_bytes 10000"));

        let projection = status(State(state), HeaderMap::new()).await.expect("public status").0;
        let json = serde_json::to_string(&projection).expect("status JSON");
        for forbidden in ["secret@example", "/secret/path", "source_url", "token"] {
            assert!(!json.contains(forbidden), "status leaked {forbidden}: {json}");
        }
        assert_eq!(projection.mirrors.len(), 1);
        assert_eq!(projection.nodes.len(), 1);

        let mut private_state = state_for_doctor(&database, directory.path().join("logs")).await;
        private_state.public_status = false;
        assert!(status(State(private_state.clone()), HeaderMap::new()).await.is_err());
        let before = private_state.store.operational_counts().await.expect("before doctor");
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer operator"));
        let diagnostic = doctor(State(private_state.clone()), headers).await.expect("doctor").0;
        assert!(diagnostic.checks.iter().all(|check| !check.id.is_empty()));
        assert_eq!(
            before,
            private_state.store.operational_counts().await.expect("after doctor")
        );
    }

    #[tokio::test]
    async fn publication_observations_are_idempotent_boot_aware_and_visible_to_doctor() {
        let mut first = publication_health_fixture();
        first.commits_succeeded_total = 5;
        let mut observation = PublicationObservation::new("agent-a".into(), "boot-a".into(), true, first.clone());
        observation.update("agent-a", "boot-a", true, first.clone());
        assert_eq!(observation.cumulative.commits_succeeded_total, 5);

        let mut advanced = first;
        advanced.commits_succeeded_total = 8;
        observation.update("agent-a", "boot-a", true, advanced);
        assert_eq!(observation.cumulative.commits_succeeded_total, 8);

        let mut restarted = publication_health_fixture();
        restarted.commits_succeeded_total = 2;
        restarted.recovery_records = 1;
        restarted.degraded = true;
        observation.update("agent-a", "boot-b", true, restarted);
        assert_eq!(observation.cumulative.commits_succeeded_total, 10);

        let mut metrics = String::new();
        append_publication_metrics(&mut metrics, &[("node-a".into(), observation.clone())]);
        assert!(metrics.contains("lmt_agents_atomic_publication_capable 1"));
        assert!(metrics.contains("lmt_publication_commits_total{outcome=\"succeeded\"} 10"));
        assert!(metrics.contains("lmt_agent_publication_recovery_records{node=\"node-a\"} 1"));

        let directory = tempfile::tempdir().expect("tempdir");
        let state = state_for_doctor(&directory.path().join("lmt.db"), directory.path().join("logs")).await;
        let status = StatusResponse {
            version: "test".into(),
            schema_version: 4,
            config_revision: 0,
            runs_pending: 0,
            runs_running: 0,
            mirrors_due: 0,
            run_logs_stored_bytes: 0,
            mirrors: Vec::new(),
            nodes: vec![NodeStatusView {
                name: "node-a".into(),
                online: true,
                bound: true,
                last_seen_at_ms: Some(1),
                active_runs: 0,
                max_concurrent_runs: 1,
                mirror_root_free_bytes: Some(1),
                atomic_publication_capable: Some(true),
                publication_health: Some(observation.cumulative),
            }],
        };
        let checks = doctor_operational_checks(&state, &status).await.expect("doctor checks");
        assert!(
            checks
                .iter()
                .any(|check| { check.id == "publication.health" && check.status == DoctorCheckStatus::Critical })
        );
    }

    #[tokio::test]
    async fn concurrent_online_backup_is_rejected_as_busy() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("lmt.db");
        let store = Store::open(&database).await.expect("store");
        let mut state = AppState::new(
            store,
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        state.database_path = database;
        state.backup_dir = Some(directory.path().join("backups"));
        let _in_progress = state.backup_lock.clone().lock_owned().await;
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer operator"));
        let failure = create_backup(State(state), headers).await.expect_err("backup busy");
        assert_eq!(failure.status, StatusCode::CONFLICT);
        assert_eq!(failure.code, "backup_busy");
    }

    #[tokio::test]
    async fn backup_recency_is_reloaded_from_published_manifest_after_restart() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("lmt.db");
        drop(Store::open(&database).await.expect("schema"));
        let backup_dir = directory.path().join("backups");
        let manifest = backup::create(&database, &backup_dir).expect("published backup");
        let expected = backup_manifest_timestamp_seconds(&manifest).expect("manifest timestamp");
        let mut restarted = AppState::new(
            Store::open(&database).await.expect("reopened store"),
            directory.path().join("logs"),
            "operator".into(),
            Duration::from_secs(90),
        );
        restarted.backup_dir = Some(backup_dir);
        assert_eq!(restarted.metrics.backup_last_success_seconds.load(Ordering::Relaxed), 0);
        load_backup_recency(&restarted).await;
        assert_eq!(
            restarted.metrics.backup_last_success_seconds.load(Ordering::Relaxed),
            expected
        );
    }

    async fn state_for_doctor(database: &Path, logs: PathBuf) -> AppState {
        let store = Store::open(database).await.expect("doctor store");
        AppState::new(store, logs, "operator".into(), Duration::from_secs(90))
    }

    fn publication_health_fixture() -> PublicationHealth {
        PublicationHealth {
            commits_succeeded_total: 0,
            commits_failed_total: 0,
            visibility_to_durability_milliseconds_total: 0,
            visibility_to_durability_samples_total: 0,
            preflight_rejections_total: 0,
            gc_failures_total: 0,
            publication_root_free_bytes: Some(1_000),
            gc_backlog_generations: 0,
            admission_block_reason: None,
            fenced_records: 0,
            recovery_records: 0,
            degraded: false,
        }
    }
}
