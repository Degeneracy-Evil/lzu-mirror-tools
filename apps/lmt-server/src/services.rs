use std::path::Path;

use lmt_core::{
    AttemptEvent, AttemptNo, MirrorDocument, MirrorName, NodeName, PublicationMode, RetryContext, RunId,
    RunSpecContext, compile_process_run_spec, decide_retry, evaluate_schedule_due, rearm_interval,
};
use lmt_protocol::v1alpha1::ATOMIC_EXCHANGE_V1;
use lmt_store::{
    AttemptEventApplyResult, CancellationApplyResult, DispatchSource, PollAction, RunPolicySnapshot, RunRecord, Store,
    StoreError, TerminalDecision,
};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[cfg(test)]
pub async fn next_action(
    store: &Store,
    node: &str,
    mirror_root: &str,
    now: i64,
) -> Result<Option<PollAction>, StoreError> {
    next_action_for_agent(store, node, mirror_root, None, &[], now).await
}

pub async fn next_action_for_agent(
    store: &Store,
    node: &str,
    mirror_root: &str,
    publication_root: Option<&str>,
    capabilities: &[String],
    now: i64,
) -> Result<Option<PollAction>, StoreError> {
    let owned_node = node.to_owned();
    let owned_root = mirror_root.to_owned();
    let owned_publication_root = publication_root.map(str::to_owned);
    let supports_atomic = capabilities.iter().any(|capability| capability == ATOMIC_EXCHANGE_V1);
    store
        .poll_action(node, now, move |source| {
            compile(
                source,
                &owned_node,
                &owned_root,
                owned_publication_root.as_deref(),
                supports_atomic,
            )
        })
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

pub async fn request_cancellation(
    store: &Store,
    run_id: &str,
    now: i64,
) -> Result<CancellationApplyResult, StoreError> {
    store
        .request_cancellation(run_id, now, move |config_toml| {
            let document: MirrorDocument =
                toml::from_str(config_toml).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
            document
                .schedule
                .as_ref()
                .map(|schedule| rearm_interval(schedule, now))
                .transpose()
                .map_err(StoreError::InvalidConfig)
                .map(Option::flatten)
        })
        .await
}

pub async fn apply_attempt_event(
    store: &Store,
    run_id: &str,
    attempt_no: u32,
    event: &AttemptEvent,
    now: i64,
) -> Result<AttemptEventApplyResult, StoreError> {
    store
        .apply_event(run_id, attempt_no, event, now, |source, server_now_ms| {
            let retry = decide_retry(RetryContext {
                outcome: source.outcome,
                attempt_no: source.attempt_no,
                max_attempts: source.max_attempts,
                retry_delay_seconds: source.retry_delay_ms / 1_000,
                cancel_requested: source.cancel_requested,
                mirror_eligible: source.mirror_eligible,
                owner_unchanged: source.owner_unchanged,
                server_now_ms,
            });
            let interval_next_due_at_ms = if source.mirror_eligible && source.owner_unchanged {
                let document: MirrorDocument = toml::from_str(&source.current_config_toml)
                    .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
                document
                    .schedule
                    .as_ref()
                    .map(|schedule| rearm_interval(schedule, server_now_ms))
                    .transpose()
                    .map_err(StoreError::InvalidConfig)?
                    .flatten()
            } else {
                None
            };
            Ok(TerminalDecision {
                retry,
                interval_next_due_at_ms,
            })
        })
        .await
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
    publication_root: Option<&str>,
    supports_atomic: bool,
) -> Result<Option<(lmt_core::ProcessRunSpec, String, RunPolicySnapshot)>, StoreError> {
    let document: MirrorDocument =
        toml::from_str(&source.config_toml).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let mirror = MirrorName::new(&source.mirror_name).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let node = NodeName::new(node).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let run_id = source
        .run_id
        .parse::<RunId>()
        .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let attempt_no = AttemptNo::new(source.attempt_no).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    if document.publication_mode() == PublicationMode::Atomic && (!supports_atomic || publication_root.is_none()) {
        return Ok(None);
    }
    let spec = compile_process_run_spec(
        &document,
        &RunSpecContext {
            mirror_name: &mirror,
            run_id,
            attempt_no,
            node_name: &node,
            mirror_root: Path::new(mirror_root),
            publication_root: publication_root.map(Path::new),
        },
    )
    .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
    let bytes = serde_json::to_vec(&spec)?;
    let hash = format!("sha256:{}", hex::encode(Sha256::digest(bytes)));
    let policy = RunPolicySnapshot {
        max_attempts: document.run.max_attempts,
        retry_delay_ms: document.run.retry_delay_seconds.saturating_mul(1_000),
    };
    Ok(Some((spec, hash, policy)))
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ScheduleEvaluationMetrics {
    pub evaluated: u64,
    pub interval_due: u64,
    pub interval_skipped: u64,
    pub cron_due: u64,
    pub cron_skipped: u64,
}

pub async fn evaluate_schedules(store: &Store, now: i64) -> Result<ScheduleEvaluationMetrics, StoreError> {
    let counts = Arc::new(std::array::from_fn::<_, 4, _>(|_| AtomicU64::new(0)));
    let callback_counts = counts.clone();
    let evaluated = store
        .evaluate_due_schedules(now, move |source| {
            let document: MirrorDocument =
                toml::from_str(&source.config_toml).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
            let schedule = document
                .schedule
                .as_ref()
                .ok_or_else(|| StoreError::InvalidConfig("scheduled mirror has no schedule".into()))?;
            let evaluation = evaluate_schedule_due(schedule, source.runtime, now, source.has_active_run)
                .map_err(StoreError::InvalidConfig)?;
            let index = match (schedule, evaluation.skipped_while_active) {
                (lmt_core::ScheduleConfig::Interval { .. }, false) => 0,
                (lmt_core::ScheduleConfig::Interval { .. }, true) => 1,
                (lmt_core::ScheduleConfig::Cron { .. }, false) => 2,
                (lmt_core::ScheduleConfig::Cron { .. }, true) => 3,
            };
            callback_counts[index].fetch_add(1, Ordering::Relaxed);
            Ok(evaluation.runtime)
        })
        .await?;
    Ok(ScheduleEvaluationMetrics {
        evaluated,
        interval_due: counts[0].load(Ordering::Relaxed),
        interval_skipped: counts[1].load(Ordering::Relaxed),
        cron_due: counts[2].load(Ordering::Relaxed),
        cron_skipped: counts[3].load(Ordering::Relaxed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(config_toml: &str) -> DispatchSource {
        DispatchSource {
            run_id: "01M40000000000000000000000".into(),
            attempt_no: 1,
            mirror_name: "demo".into(),
            mirror_generation: 1,
            config_toml: config_toml.into(),
        }
    }

    #[test]
    fn direct_specs_remain_legacy_shaped_without_atomic_capability() {
        let compiled = compile(
            &source("[mirror]\nname='demo'\ntarget='demo'\n[sync]\ntype='command'\nprogram='/bin/true'\n"),
            "node-a",
            "/srv/mirrors",
            None,
            false,
        )
        .expect("compile")
        .expect("Direct dispatch");
        let json = serde_json::to_value(compiled.0).expect("serialize Direct spec");
        assert!(json.get("publication").is_none());
    }

    #[test]
    fn atomic_specs_require_explicit_capability_and_publication_root() {
        let atomic = source(
            "[mirror]\nname='demo'\ntarget='demo'\n[sync]\ntype='command'\nprogram='/bin/true'\n[publication]\nmode='atomic'\n",
        );
        assert!(
            compile(&atomic, "node-a", "/srv/mirrors", Some("/srv/publication"), false)
                .expect("unsupported")
                .is_none()
        );
        assert!(
            compile(&atomic, "node-a", "/srv/mirrors", None, true)
                .expect("missing root")
                .is_none()
        );

        let compiled = compile(&atomic, "node-a", "/srv/mirrors", Some("/srv/publication"), true)
            .expect("compile")
            .expect("Atomic dispatch");
        let publication = compiled.0.publication.expect("publication extension");
        assert_eq!(publication.published_dir, "/srv/mirrors/demo");
        assert!(publication.candidate_dir.starts_with("/srv/publication/demo/attempts/"));
    }
}
