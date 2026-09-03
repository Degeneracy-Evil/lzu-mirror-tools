use std::collections::BTreeMap;

use lmt_core::{AttemptState, BundleFile, FailureKind, ProcessRunSpec, RunState, RunTrigger};
use serde::{Deserialize, Serialize};

pub const ATOMIC_EXCHANGE_V1: &str = "atomic_exchange_v1";

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApiErrorEnvelope {
    pub error: ApiError,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub details: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    pub id: String,
    pub created_at: String,
    pub lmt_version: String,
    pub schema_version: u32,
    pub config_revision: u64,
    pub database_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackupListResponse {
    pub backups: Vec<BackupManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackupVerifyResponse {
    pub backup: BackupManifest,
    pub valid: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StatusResponse {
    pub version: String,
    pub schema_version: u32,
    pub config_revision: u64,
    pub runs_pending: u64,
    pub runs_running: u64,
    pub mirrors_due: u64,
    pub run_logs_stored_bytes: u64,
    pub mirrors: Vec<MirrorStatusView>,
    pub nodes: Vec<NodeStatusView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MirrorStatusView {
    pub name: String,
    pub node: String,
    pub enabled: bool,
    pub current_run_state: Option<RunState>,
    pub current_run_created_at_ms: Option<i64>,
    pub last_run_state: Option<RunState>,
    pub last_terminal_at_ms: Option<i64>,
    pub last_success_at_ms: Option<i64>,
    pub next_due_at_ms: Option<i64>,
    pub due_since_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NodeStatusView {
    pub name: String,
    pub online: bool,
    pub bound: bool,
    pub last_seen_at_ms: Option<i64>,
    pub active_runs: u32,
    pub max_concurrent_runs: u32,
    pub mirror_root_free_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atomic_publication_capable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_health: Option<PublicationHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DoctorCheckStatus {
    Ok,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DoctorCheck {
    pub id: String,
    pub status: DoctorCheckStatus,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DoctorResponse {
    pub healthy: bool,
    pub checks: Vec<DoctorCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BundleRequest {
    pub files: Vec<BundleFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ValidationResponse {
    pub valid: bool,
    pub bundle_hash: Option<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeAction {
    Create,
    Update,
    Remove,
    Move,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicationChange {
    DirectToAtomic,
    AtomicToDirect,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConfigChange {
    pub action: ChangeAction,
    pub mirror: String,
    pub from_generation: Option<u64>,
    pub to_generation: Option<u64>,
    pub from_node: Option<String>,
    pub to_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_change: Option<PublicationChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PlanResponse {
    pub base_revision: u64,
    pub bundle_hash: String,
    pub changes: Vec<ConfigChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplyRequest {
    pub files: Vec<BundleFile>,
    pub base_revision: u64,
    #[serde(default)]
    pub acknowledge_moves: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplyResponse {
    pub revision: u64,
    pub bundle_hash: String,
    pub changes: Vec<ConfigChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ManualRunRequest {
    pub request_id: String,
    pub trigger: RunTrigger,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunView {
    pub id: String,
    pub mirror_name: String,
    pub mirror_generation: u64,
    pub owner_node: String,
    pub trigger: RunTrigger,
    pub state: RunState,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub final_exit_code: Option<i32>,
    pub failure_kind: Option<String>,
    pub failure_message: Option<String>,
    pub scheduled_for_at: Option<String>,
    pub retry_due_at: Option<String>,
    pub cancel_requested_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AttemptView {
    pub run_id: String,
    pub attempt_no: u32,
    pub state: AttemptState,
    pub spec_hash: String,
    pub created_at: String,
    pub accepted_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<String>,
    pub failure_message: Option<String>,
    pub last_event_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunDetail {
    #[serde(flatten)]
    pub run: RunView,
    pub attempts: Vec<AttemptView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MirrorView {
    pub name: String,
    pub managed: bool,
    pub enabled: bool,
    pub owner_node: String,
    pub current_generation: u64,
    pub next_due_at: Option<String>,
    pub scheduled_due_since: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NodeView {
    pub name: String,
    pub agent_version: Option<String>,
    pub agent_instance_id: Option<String>,
    pub bound_agent_id: Option<String>,
    pub agent_boot_id: Option<String>,
    pub last_seen_at: Option<String>,
    pub active_runs: u32,
    pub mirror_root_free_bytes: Option<u64>,
    pub max_concurrent_runs: u32,
    pub online: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BindingReplaceRequest {
    pub agent_id: String,
    pub acknowledge_execution_risk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CredentialIssueRequest {
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CredentialView {
    pub id: String,
    pub node: String,
    pub label: Option<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CredentialIssueResponse {
    #[serde(flatten)]
    pub credential: CredentialView,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LogExpirationView {
    pub run_id: String,
    pub attempt: u32,
    pub stored_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LogMaintenancePlan {
    pub stored_bytes: u64,
    pub expire_bytes: u64,
    pub candidates: Vec<LogExpirationView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Capacity {
    pub mirror_root_free_bytes: Option<u64>,
    pub active_runs: u32,
    pub max_concurrent_runs: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicationAdmissionBlockReason {
    Fence,
    Recovery,
    GenerationBound,
    FreeSpaceReserve,
    GcFailure,
    InvalidLocalState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PublicationHealth {
    pub commits_succeeded_total: u64,
    pub commits_failed_total: u64,
    pub visibility_to_durability_milliseconds_total: u64,
    pub visibility_to_durability_samples_total: u64,
    pub preflight_rejections_total: u64,
    pub gc_failures_total: u64,
    pub publication_root_free_bytes: Option<u64>,
    pub gc_backlog_generations: u32,
    pub admission_block_reason: Option<PublicationAdmissionBlockReason>,
    pub fenced_records: u32,
    pub recovery_records: u32,
    pub degraded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OwnedAttempt {
    pub run_id: String,
    pub attempt: u32,
    pub state: AttemptState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PollRequest {
    pub protocol_version: String,
    pub agent_version: String,
    pub agent_instance_id: String,
    pub agent_boot_id: String,
    pub poll_sequence: u64,
    pub running: Vec<OwnedAttempt>,
    pub capacity: Capacity,
    pub mirror_root: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_health: Option<PublicationHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentAction {
    StartAttempt {
        run_id: String,
        attempt: u32,
        spec_hash: String,
        spec: ProcessRunSpec,
    },
    CancelAttempt {
        run_id: String,
        attempt: u32,
        spec_hash: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PollResponse {
    pub actions: Vec<AgentAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EventRequest {
    pub event_sequence: u64,
    pub state: AttemptState,
    pub agent_instance_id: String,
    pub accepted_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<FailureKind>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EventResponse {
    pub accepted_event_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HealthResponse {
    pub status: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use lmt_core::RunState;

    #[test]
    fn start_attempt_golden_json_is_stable() {
        let message = PollResponse {
            actions: vec![AgentAction::StartAttempt {
                run_id: "01K00000000000000000000000".into(),
                attempt: 1,
                spec_hash: "sha256:abc".into(),
                spec: ProcessRunSpec {
                    runner: "process".into(),
                    program: "/bin/true".into(),
                    args: vec![],
                    cwd: None,
                    timeout_seconds: 30,
                    mirror_root: "/srv/mirrors".into(),
                    target_dir: "/srv/mirrors/example".into(),
                    publication: None,
                },
            }],
        };
        let json = serde_json::to_value(message).expect("serialize");
        assert_eq!(json["actions"][0]["type"], "start_attempt");
        assert_eq!(json["actions"][0]["attempt"], 1);
    }

    #[test]
    fn cancel_attempt_carries_the_immutable_spec_identity() {
        let message = PollResponse {
            actions: vec![AgentAction::CancelAttempt {
                run_id: "01K00000000000000000000000".into(),
                attempt: 2,
                spec_hash: "sha256:abc".into(),
            }],
        };
        let json = serde_json::to_value(message).expect("serialize");
        assert_eq!(json["actions"][0]["type"], "cancel_attempt");
        assert_eq!(json["actions"][0]["spec_hash"], "sha256:abc");
    }

    #[test]
    fn run_state_round_trips_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&RunState::Succeeded).expect("serialize"),
            "\"succeeded\""
        );
        assert_eq!(
            serde_json::from_str::<RunState>("\"succeeded\"").expect("deserialize"),
            RunState::Succeeded
        );
    }

    #[test]
    fn unknown_poll_fields_are_rejected() {
        let json = r#"{"protocol_version":"v1alpha1","agent_version":"x","agent_instance_id":"i","poll_sequence":1,"running":[],"capacity":{"mirror_root_free_bytes":null,"active_runs":0,"max_concurrent_runs":1},"mirror_root":"/x","node":"spoof"}"#;
        assert!(serde_json::from_str::<PollRequest>(json).is_err());
    }

    #[test]
    fn publication_health_is_optional_and_strict() {
        let legacy = r#"{"protocol_version":"v1alpha1","agent_version":"x","agent_instance_id":"i","agent_boot_id":"b","poll_sequence":1,"running":[],"capacity":{"mirror_root_free_bytes":null,"active_runs":0,"max_concurrent_runs":1},"mirror_root":"/x"}"#;
        let request: PollRequest = serde_json::from_str(legacy).expect("legacy poll");
        assert_eq!(request.publication_health, None);
        assert!(
            !serde_json::to_string(&request)
                .expect("serialize legacy poll")
                .contains("publication_health")
        );

        let mut invalid: serde_json::Value = serde_json::from_str(legacy).expect("poll JSON");
        invalid["publication_health"] = serde_json::json!({
            "commits_succeeded_total": 0,
            "commits_failed_total": 0,
            "visibility_to_durability_milliseconds_total": 0,
            "visibility_to_durability_samples_total": 0,
            "preflight_rejections_total": 0,
            "gc_failures_total": 0,
            "publication_root_free_bytes": 1,
            "gc_backlog_generations": 0,
            "admission_block_reason": null,
            "fenced_records": 0,
            "recovery_records": 0,
            "degraded": false,
            "unknown": true,
        });
        assert!(serde_json::from_value::<PollRequest>(invalid).is_err());
    }
}
