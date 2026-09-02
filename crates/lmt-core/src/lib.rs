//! Infrastructure-independent domain types and transitions for LMT.

mod config;
mod ids;
mod schedule;
mod state;

pub use config::{
    BundleFile, CanonicalBundle, ConfigBundle, ConfigError, MirrorConfig, MirrorDocument, ProcessRunner,
    PublicationConfig, PublicationMode, RunPolicy, RunSpecContext, SyncConfig, canonicalize_bundle,
    compile_process_run_spec, publication_mode_from_toml,
};
pub use ids::{AgentInstanceId, AttemptNo, MirrorName, NodeName, RequestId, RunId};
pub use schedule::{
    DueEvaluation, RetryContext, RetryDecision, RunTrigger, ScheduleConfig, ScheduleRuntime, activate_schedule,
    decide_retry, evaluate_schedule_due, rearm_interval,
};
pub use state::{
    AtomicPublicationSpec, AttemptEvent, AttemptProjection, AttemptState, FailureKind, ProcessRunSpec, RunState,
    TransitionError, project_attempt_event, validate_attempt_transition,
};
