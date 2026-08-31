use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use lmt_core::{AttemptEvent, ConfigBundle, RunId, canonicalize_bundle};
use lmt_protocol::v1alpha1::*;
use lmt_store::{AttemptRecord, ChangeKind, ConfigPlan, NodeObservation, RunRecord, Store, StoreError};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
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

mod services;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub database_path: PathBuf,
    pub log_dir: PathBuf,
    pub operator_token: String,
    #[serde(default = "offline_default")]
    pub offline_after_seconds: u64,
    #[serde(default)]
    pub agents: Vec<AgentCredential>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCredential {
    pub node: String,
    pub token: String,
}
const fn offline_default() -> u64 {
    90
}
#[derive(Clone)]
pub struct AppState {
    store: Store,
    log_dir: PathBuf,
    operator_token: Arc<str>,
    offline_after: Duration,
    notify: Arc<Notify>,
    log_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    metrics: Arc<AppMetrics>,
    poll_wait: Duration,
    clock: Arc<dyn Clock>,
}

trait Clock: Send + Sync {
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
}
impl AppState {
    pub fn new(store: Store, log_dir: PathBuf, token: String, offline_after: Duration) -> Self {
        Self {
            store,
            log_dir,
            operator_token: token.into(),
            offline_after,
            notify: Arc::new(Notify::new()),
            log_locks: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(AppMetrics::default()),
            poll_wait: Duration::from_secs(20),
            clock: Arc::new(SystemClock),
        }
    }

    fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }
}
pub async fn initialize(c: &ServerConfig) -> anyhow::Result<AppState> {
    if let Some(p) = c.database_path.parent() {
        fs::create_dir_all(p).await?;
    }
    fs::create_dir_all(&c.log_dir).await?;
    let store = Store::open(&c.database_path).await?;
    for a in &c.agents {
        store.upsert_credential(&a.node, &a.token, now_ms()).await?;
    }
    let state = AppState::new(
        store,
        c.log_dir.clone(),
        c.operator_token.clone(),
        Duration::from_secs(c.offline_after_seconds),
    );
    tokio::spawn(run_scheduler(state.clone()));
    Ok(state)
}

async fn run_scheduler(state: AppState) {
    loop {
        let now = state.now_ms();
        match services::evaluate_schedules(&state.store, now).await {
            Ok(evaluated) => {
                let earliest = state.store.earliest_wakeup().await.unwrap_or(None);
                if evaluated > 0 || earliest.is_some_and(|deadline| deadline <= now) {
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
    let runs = state.store.list_runs().await?;
    let pending = runs
        .iter()
        .filter(|run| run.state == lmt_core::RunState::Pending)
        .count();
    let running = runs
        .iter()
        .filter(|run| run.state == lmt_core::RunState::Running)
        .count();
    let body = format!(
        "lmt_up 1\nlmt_runs_pending {}\nlmt_runs_running {}\nlmt_agent_polls_total {}\nlmt_attempt_events_total {}\nlmt_log_uploaded_bytes_total {}\nlmt_log_upload_failures_total {}\n",
        pending,
        running,
        state.metrics.polls.load(Ordering::Relaxed),
        state.metrics.events.load(Ordering::Relaxed),
        state.metrics.uploaded_bytes.load(Ordering::Relaxed),
        state.metrics.log_failures.load(Ordering::Relaxed)
    );
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response())
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
    }))
}
async fn manual(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(name): AxumPath<String>,
    Json(r): Json<ManualRunRequest>,
) -> Result<Json<RunView>, Failure> {
    operator(&h, &s)?;
    if r.trigger != "manual" || r.request_id.is_empty() {
        return Err(Failure::bad("invalid_request", "invalid manual request"));
    }
    let result = run_view(services::create_manual_run(&s.store, &name, &r.request_id, s.now_ms()).await?);
    s.notify.notify_waiters();
    Ok(Json(result))
}
async fn runs(State(s): State<AppState>, h: HeaderMap) -> Result<Json<Vec<RunView>>, Failure> {
    operator(&h, &s)?;
    Ok(Json(s.store.list_runs().await?.into_iter().map(run_view).collect()))
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
    let run = services::request_cancellation(&s.store, &id, s.now_ms()).await?;
    s.notify.notify_waiters();
    Ok(Json(run_view(run)))
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
async fn poll(State(s): State<AppState>, h: HeaderMap, Json(r): Json<PollRequest>) -> Result<Response, Failure> {
    let node = agent(&h, &s).await?;
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
            active_runs: r.capacity.active_runs,
            max_concurrent_runs: r.capacity.max_concurrent_runs,
            mirror_root_free_bytes: r.capacity.mirror_root_free_bytes,
            mirror_root: r.mirror_root.clone(),
            observed_at_ms: s.now_ms(),
        })
        .await?;
    s.metrics.polls.fetch_add(1, Ordering::Relaxed);
    s.notify.notify_waiters();
    if let Some(a) = services::next_action(&s.store, &node, &r.mirror_root, s.now_ms()).await? {
        return Ok(action(a).into_response());
    }
    let _ = tokio::time::timeout(s.poll_wait, s.notify.notified()).await;
    if let Some(a) = services::next_action(&s.store, &node, &r.mirror_root, s.now_ms()).await? {
        return Ok(action(a).into_response());
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}
fn action(a: lmt_store::PollAction) -> Json<PollResponse> {
    Json(PollResponse {
        actions: vec![match a {
            lmt_store::PollAction::StartAttempt {
                run_id,
                attempt_no,
                spec_hash,
                spec,
            } => AgentAction::StartAttempt {
                run_id,
                attempt: attempt_no,
                spec_hash,
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
    let node = agent(&h, &s).await?;
    attempt_auth(&s, &node, &id, no).await?;
    let accepted = services::apply_attempt_event(
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
    Ok(Json(EventResponse {
        accepted_event_sequence: accepted,
    }))
}
async fn upload_log(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath((id, no)): AxumPath<(String, u32)>,
    body: Bytes,
) -> Result<Response, Failure> {
    let node = agent(&h, &s).await?;
    attempt_auth(&s, &node, &id, no).await?;
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
}
async fn read_log(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<LogQuery>,
) -> Result<Response, Failure> {
    operator(&h, &s)?;
    let no = q.attempt.unwrap_or(1);
    let id = RunId::from_str(&id)
        .map_err(|_| Failure::not_found("run_not_found"))?
        .to_string();
    let mut data = vec![];
    let mut complete = false;
    if let Some((_, stored, c)) = s.store.log_metadata(&id, no).await? {
        complete = c;
        if q.offset < stored {
            let take = (stored - q.offset).min(q.limit.unwrap_or(65536).min(1_048_576));
            let mut f = OpenOptions::new()
                .read(true)
                .open(log_path(&s.log_dir, &id, no))
                .await
                .map_err(Failure::io)?;
            f.seek(std::io::SeekFrom::Start(q.offset)).await.map_err(Failure::io)?;
            data.resize(take as usize, 0);
            f.read_exact(&mut data).await.map_err(Failure::io)?;
        }
    }
    let next = q.offset + data.len() as u64;
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
    let key = format!("{id}-{no}");
    let lock = {
        let mut locks = s.log_locks.lock().await;
        locks.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
    };
    let _guard = lock.lock().await;
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
    if bearer(h) == Some(s.operator_token.as_ref()) {
        Ok(())
    } else {
        Err(Failure::unauthorized())
    }
}
async fn agent(h: &HeaderMap, s: &AppState) -> Result<String, Failure> {
    s.store
        .authenticate_node(bearer(h).ok_or_else(Failure::unauthorized)?)
        .await?
        .ok_or_else(Failure::unauthorized)
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
        last_seen_at: n.last_seen_at_ms.map(timestamp),
        active_runs: n.active_runs,
        mirror_root_free_bytes: n.mirror_root_free_bytes,
        online: n.last_seen_at_ms.is_some_and(|x| now - x <= d.as_millis() as i64),
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
    use lmt_core::{BundleFile, RunState};
    use std::sync::atomic::AtomicI64;

    struct FakeClock(AtomicI64);

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
                .expect("tick"),
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
        assert_eq!(run.trigger, "scheduled");
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
}
