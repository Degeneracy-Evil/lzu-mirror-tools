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
use lmt_store::{AttemptRecord, ChangeKind, ConfigPlan, RunRecord, Store, StoreError};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, Notify},
};

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
    log_lock: Arc<Mutex<()>>,
    poll_wait: Duration,
}
impl AppState {
    pub fn new(store: Store, log_dir: PathBuf, token: String, offline_after: Duration) -> Self {
        Self {
            store,
            log_dir,
            operator_token: token.into(),
            offline_after,
            notify: Arc::new(Notify::new()),
            log_lock: Arc::new(Mutex::new(())),
            poll_wait: Duration::from_secs(20),
        }
    }
}
pub async fn initialize(c: &ServerConfig) -> anyhow::Result<AppState> {
    if let Some(p) = c.database_path.parent() {
        fs::create_dir_all(p).await?;
    }
    fs::create_dir_all(&c.log_dir).await?;
    let store = Store::open(&c.database_path)?;
    for a in &c.agents {
        store.upsert_credential(&a.node, &a.token)?;
    }
    Ok(AppState::new(
        store,
        c.log_dir.clone(),
        c.operator_token.clone(),
        Duration::from_secs(c.offline_after_seconds),
    ))
}
pub fn build_router(s: AppState) -> Router {
    Router::new()
        .route(
            "/health/live",
            get(|| async { Json(HealthResponse { status: "live".into() }) }),
        )
        .route("/health/ready", get(ready))
        .route(
            "/metrics",
            get(|| async { ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], "lmt_up 1\n") }),
        )
        .route("/api/v1alpha1/config/validate", post(validate))
        .route("/api/v1alpha1/config/plan", post(plan))
        .route("/api/v1alpha1/config/apply", post(apply))
        .route("/api/v1alpha1/mirrors", get(mirrors))
        .route("/api/v1alpha1/mirrors/{name}", get(mirror))
        .route("/api/v1alpha1/mirrors/{name}/runs", post(manual))
        .route("/api/v1alpha1/runs", get(runs))
        .route("/api/v1alpha1/runs/{id}", get(run))
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
    s.store.current_revision()?;
    Ok(Json(HealthResponse { status: "ready".into() }))
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
    Ok(Json(plan_view(s.store.plan(&b)?)))
}
async fn apply(
    State(s): State<AppState>,
    h: HeaderMap,
    Json(r): Json<ApplyRequest>,
) -> Result<Json<ApplyResponse>, Failure> {
    operator(&h, &s)?;
    let b = canonicalize_bundle(&ConfigBundle { files: r.files }).map_err(config_error)?;
    if s.store.plan(&b)?.changes.iter().any(|c| c.kind == ChangeKind::Move) && !r.acknowledge_moves {
        return Err(Failure::conflict(
            "move_acknowledgement_required",
            "node move requires acknowledgement",
        ));
    }
    let p = s.store.apply(&b, r.base_revision, "api")?;
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
            .list_mirrors()?
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
        .get_mirror(&name)?
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
    let result = run_view(s.store.create_manual_run(&name, &r.request_id)?);
    s.notify.notify_waiters();
    Ok(Json(result))
}
async fn runs(State(s): State<AppState>, h: HeaderMap) -> Result<Json<Vec<RunView>>, Failure> {
    operator(&h, &s)?;
    Ok(Json(s.store.list_runs()?.into_iter().map(run_view).collect()))
}
async fn run(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<RunDetail>, Failure> {
    operator(&h, &s)?;
    let r = s
        .store
        .get_run(&id)?
        .ok_or_else(|| Failure::not_found("run_not_found"))?;
    Ok(Json(RunDetail {
        run: run_view(r),
        attempts: s.store.list_attempts(&id)?.into_iter().map(attempt_view).collect(),
    }))
}
async fn attempts(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Vec<AttemptView>>, Failure> {
    operator(&h, &s)?;
    Ok(Json(
        s.store.list_attempts(&id)?.into_iter().map(attempt_view).collect(),
    ))
}
async fn nodes(State(s): State<AppState>, h: HeaderMap) -> Result<Json<Vec<NodeView>>, Failure> {
    operator(&h, &s)?;
    let now = now_ms();
    Ok(Json(
        s.store
            .list_nodes()?
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
        .list_nodes()?
        .into_iter()
        .find(|n| n.name == name)
        .ok_or_else(|| Failure::not_found("node_not_found"))?;
    Ok(Json(node_view(n, now_ms(), s.offline_after)))
}
async fn poll(State(s): State<AppState>, h: HeaderMap, Json(r): Json<PollRequest>) -> Result<Response, Failure> {
    let node = agent(&h, &s)?;
    if r.protocol_version != "v1alpha1" {
        return Err(Failure::bad(
            "unsupported_protocol_version",
            "only v1alpha1 is supported",
        ));
    }
    s.store.observe_node(
        &node,
        &r.agent_version,
        &r.agent_instance_id,
        r.capacity.active_runs,
        r.capacity.mirror_root_free_bytes,
        &r.mirror_root,
    )?;
    if let Some(a) = s.store.poll_action(&node, &r.mirror_root)? {
        return Ok(action(a).into_response());
    }
    let _ = tokio::time::timeout(s.poll_wait, s.notify.notified()).await;
    if let Some(a) = s.store.poll_action(&node, &r.mirror_root)? {
        return Ok(action(a).into_response());
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}
fn action(a: lmt_store::PollAction) -> Json<PollResponse> {
    Json(PollResponse {
        actions: vec![AgentAction::StartAttempt {
            run_id: a.run_id,
            attempt: a.attempt_no,
            spec_hash: a.spec_hash,
            spec: a.spec,
        }],
    })
}
async fn event(
    State(s): State<AppState>,
    h: HeaderMap,
    AxumPath((id, no)): AxumPath<(String, u32)>,
    Json(r): Json<EventRequest>,
) -> Result<Json<EventResponse>, Failure> {
    let node = agent(&h, &s)?;
    attempt_auth(&s, &node, &id, no)?;
    let accepted = s.store.apply_event(
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
    )?;
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
    let node = agent(&h, &s)?;
    attempt_auth(&s, &node, &id, no)?;
    if body.len() > 1_048_576 {
        return Err(Failure::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "chunk too large",
        ));
    }
    let offset = header_u64(&h, "x-lmt-log-offset")?;
    let complete = h.get("x-lmt-log-complete").and_then(|v| v.to_str().ok()) == Some("true");
    let next = append_log(&s, &id, no, offset, &body, complete).await?;
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
    if let Some((_, stored, c)) = s.store.log_metadata(&id, no)? {
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
    let _guard = s.log_lock.lock().await;
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
        .update_log_metadata(&id, no, &format!("{id}/{no}.log"), next, complete)?;
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
fn agent(h: &HeaderMap, s: &AppState) -> Result<String, Failure> {
    s.store
        .authenticate_node(bearer(h).ok_or_else(Failure::unauthorized)?)?
        .ok_or_else(Failure::unauthorized)
}
fn attempt_auth(s: &AppState, node: &str, id: &str, no: u32) -> Result<(), Failure> {
    if s.store.attempt_belongs_to_node(id, no, node)? {
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
