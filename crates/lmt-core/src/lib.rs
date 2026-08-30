//! Infrastructure-independent domain types and transitions for LMT.

mod config;
mod ids;
mod state;

pub use config::{
    BundleFile, CanonicalBundle, CommandSync, ConfigBundle, ConfigError, MirrorConfig, MirrorDocument, ProcessRunner,
    RunPolicy, canonicalize_bundle,
};
pub use ids::{AgentInstanceId, AttemptNo, MirrorName, NodeName, RequestId, RunId};
pub use state::{
    AttemptEvent, AttemptState, FailureKind, ProcessRunSpec, RunState, TransitionError, validate_attempt_transition,
};
