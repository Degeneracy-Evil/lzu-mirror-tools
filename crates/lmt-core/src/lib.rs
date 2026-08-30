//! Infrastructure-independent domain types and transitions for LMT.

mod config;
mod ids;
mod state;

pub use config::{
    BundleFile, CanonicalBundle, CommandSync, ConfigBundle, ConfigError, MirrorConfig, MirrorDocument, ProcessRunner,
    RunPolicy, RunSpecContext, canonicalize_bundle, compile_process_run_spec,
};
pub use ids::{AgentInstanceId, AttemptNo, MirrorName, NodeName, RequestId, RunId};
pub use state::{
    AttemptEvent, AttemptProjection, AttemptState, FailureKind, ProcessRunSpec, RunState, TransitionError,
    project_attempt_event, validate_attempt_transition,
};
