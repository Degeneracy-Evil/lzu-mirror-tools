use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl RunState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Queued,
    Accepted,
    Running,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
    Rejected,
}

impl AttemptState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::Interrupted | Self::Rejected
        )
    }

    pub fn allows(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match self {
            Self::Queued => matches!(next, Self::Accepted | Self::Cancelled | Self::Rejected),
            Self::Accepted => matches!(
                next,
                Self::Running | Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::Interrupted
            ),
            Self::Running => matches!(
                next,
                Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::Interrupted
            ),
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled | Self::Interrupted | Self::Rejected => {
                false
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Process,
    Timeout,
    Interrupted,
    Rejected,
    InvalidResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProcessRunSpec {
    pub runner: String,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub timeout_seconds: u64,
    pub mirror_root: String,
    pub target_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AttemptEvent {
    pub event_sequence: u64,
    pub state: AttemptState,
    pub agent_instance_id: String,
    pub accepted_at_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<FailureKind>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("illegal attempt transition from {from:?} to {to:?}")]
pub struct TransitionError {
    pub from: AttemptState,
    pub to: AttemptState,
}

pub fn validate_attempt_transition(from: AttemptState, to: AttemptState) -> Result<(), TransitionError> {
    if from.allows(to) {
        Ok(())
    } else {
        Err(TransitionError { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_attempts_never_regress() {
        for state in [
            AttemptState::Succeeded,
            AttemptState::Failed,
            AttemptState::TimedOut,
            AttemptState::Cancelled,
            AttemptState::Interrupted,
            AttemptState::Rejected,
        ] {
            assert!(!state.allows(AttemptState::Running));
        }
    }

    #[test]
    fn terminal_snapshot_may_skip_running() {
        assert!(AttemptState::Accepted.allows(AttemptState::Succeeded));
        assert!(!AttemptState::Queued.allows(AttemptState::Succeeded));
    }
}
