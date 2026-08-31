use std::collections::BTreeMap;

use lmt_core::{AttemptState, BundleFile, FailureKind, ProcessRunSpec, RunState};
use serde::{Deserialize, Serialize};

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
#[serde(deny_unknown_fields)]
pub struct ConfigChange {
    pub action: ChangeAction,
    pub mirror: String,
    pub from_generation: Option<u64>,
    pub to_generation: Option<u64>,
    pub from_node: Option<String>,
    pub to_node: Option<String>,
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
    pub trigger: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunView {
    pub id: String,
    pub mirror_name: String,
    pub mirror_generation: u64,
    pub owner_node: String,
    pub trigger: String,
    pub state: RunState,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub final_exit_code: Option<i32>,
    pub failure_kind: Option<String>,
    pub failure_message: Option<String>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NodeView {
    pub name: String,
    pub agent_version: Option<String>,
    pub agent_instance_id: Option<String>,
    pub last_seen_at: Option<String>,
    pub active_runs: u32,
    pub mirror_root_free_bytes: Option<u64>,
    pub online: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Capacity {
    pub mirror_root_free_bytes: Option<u64>,
    pub active_runs: u32,
    pub max_concurrent_runs: u32,
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
    pub poll_sequence: u64,
    pub running: Vec<OwnedAttempt>,
    pub capacity: Capacity,
    pub mirror_root: String,
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
}
