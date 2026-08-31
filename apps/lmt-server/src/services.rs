use std::path::Path;

use lmt_core::{AttemptNo, MirrorDocument, MirrorName, NodeName, RunId, RunSpecContext, compile_process_run_spec};
use lmt_store::{DispatchSource, PollAction, RunPolicySnapshot, RunRecord, Store, StoreError};
use sha2::{Digest, Sha256};

pub async fn next_action(
    store: &Store,
    node: &str,
    mirror_root: &str,
    now: i64,
) -> Result<Option<PollAction>, StoreError> {
    let owned_node = node.to_owned();
    let owned_root = mirror_root.to_owned();
    store
        .poll_action(node, now, move |source| compile(source, &owned_node, &owned_root))
        .await
}

pub async fn create_manual_run(
    store: &Store,
    mirror: &str,
    request_id: &str,
    now: i64,
) -> Result<RunRecord, StoreError> {
    store.create_manual_run(mirror, request_id, now, compile_policy).await
}

fn compile_policy(config_toml: &str) -> Result<RunPolicySnapshot, StoreError> {
    let document: MirrorDocument =
        toml::from_str(config_toml).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    Ok(RunPolicySnapshot {
        max_attempts: document.run.max_attempts,
        retry_delay_ms: document.run.retry_delay_seconds.saturating_mul(1_000),
    })
}

fn compile(
    source: &DispatchSource,
    node: &str,
    mirror_root: &str,
) -> Result<(lmt_core::ProcessRunSpec, String), StoreError> {
    let document: MirrorDocument =
        toml::from_str(&source.config_toml).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let mirror = MirrorName::new(&source.mirror_name).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let node = NodeName::new(node).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let run_id = source
        .run_id
        .parse::<RunId>()
        .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let attempt_no = AttemptNo::new(1).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let spec = compile_process_run_spec(
        &document,
        &RunSpecContext {
            mirror_name: &mirror,
            run_id,
            attempt_no,
            node_name: &node,
            mirror_root: Path::new(mirror_root),
        },
    );
    let bytes = serde_json::to_vec(&spec)?;
    let hash = format!("sha256:{}", hex::encode(Sha256::digest(bytes)));
    Ok((spec, hash))
}
