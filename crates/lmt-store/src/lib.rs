//! Central SQLite persistence. This is the only library crate that knows the schema.

use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use lmt_core::{
    AttemptEvent, AttemptState, CanonicalBundle, FailureKind, MirrorDocument, ProcessRunSpec, RetryDecision, RunId,
    RunState, ScheduleRuntime, activate_schedule, project_attempt_event,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Clone)]
pub struct Store {
    connection: tokio_rusqlite::Connection,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("database worker unavailable: {0}")]
    Worker(String),
    #[error("database schema version {found} is newer than supported version {supported}")]
    FutureSchema { found: u32, supported: u32 },
    #[error("configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("configuration revision conflict: current revision is {current}")]
    RevisionConflict { current: u64 },
    #[error("mirror not found")]
    MirrorNotFound,
    #[error("mirror is not managed or enabled")]
    MirrorIneligible,
    #[error("mirror already has active run {run_id}")]
    MirrorBusy { run_id: String },
    #[error("request id was already used for another mirror")]
    RequestConflict,
    #[error("run or attempt not found")]
    AttemptNotFound,
    #[error("Agent binding conflict: Node is bound to {bound_agent_id}, presented {presented_agent_id}")]
    AgentBindingConflict {
        bound_agent_id: String,
        presented_agent_id: String,
    },
    #[error("Node binding replacement may overlap dispatched work")]
    BindingReplacementUnsafe,
    #[error("credential not found")]
    CredentialNotFound,
    #[error("illegal state transition from {from:?} to {to:?}")]
    IllegalTransition { from: AttemptState, to: AttemptState },
    #[error("spec serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ChangeKind {
    Create,
    Update,
    Remove,
    Move,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfigChange {
    pub kind: ChangeKind,
    pub mirror: String,
    pub from_generation: Option<u64>,
    pub to_generation: Option<u64>,
    pub from_node: Option<String>,
    pub to_node: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConfigPlan {
    pub base_revision: u64,
    pub bundle_hash: String,
    pub changes: Vec<ConfigChange>,
}

#[derive(Debug, Clone)]
pub struct MirrorRecord {
    pub name: String,
    pub managed: bool,
    pub enabled: bool,
    pub owner_node: String,
    pub current_generation: u64,
    pub next_due_at_ms: Option<i64>,
    pub scheduled_due_since_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub name: String,
    pub agent_version: Option<String>,
    pub agent_instance_id: Option<String>,
    pub bound_agent_id: Option<String>,
    pub agent_boot_id: Option<String>,
    pub last_seen_at_ms: Option<i64>,
    pub active_runs: u32,
    pub mirror_root_free_bytes: Option<u64>,
    pub max_concurrent_runs: u32,
}

pub struct NodeObservation {
    pub node: String,
    pub agent_version: String,
    pub agent_instance_id: String,
    pub agent_boot_id: String,
    pub active_runs: u32,
    pub max_concurrent_runs: u32,
    pub mirror_root_free_bytes: Option<u64>,
    pub mirror_root: String,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CredentialRecord {
    pub node: String,
    pub id: String,
    pub label: Option<String>,
    pub created_at_ms: i64,
    pub last_used_at_ms: Option<i64>,
    pub revoked_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthenticatedCredential {
    pub node: String,
    pub credential_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LogMetadata {
    pub relative_path: String,
    pub stored_bytes: u64,
    pub complete: bool,
    pub updated_at_ms: i64,
    pub expired_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LogRetentionEntry {
    pub run_id: String,
    pub attempt_no: u32,
    pub stored_bytes: u64,
    pub updated_at_ms: i64,
    pub eligible: bool,
    pub expired_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RunRecord {
    pub id: String,
    pub mirror_name: String,
    pub mirror_generation: u64,
    pub owner_node: String,
    pub trigger: lmt_core::RunTrigger,
    pub state: RunState,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub final_exit_code: Option<i32>,
    pub failure_kind: Option<String>,
    pub failure_message: Option<String>,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub scheduled_for_at_ms: Option<i64>,
    pub retry_due_at_ms: Option<i64>,
    pub cancel_requested_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct RunQuery {
    pub mirror: Option<String>,
    pub node: Option<String>,
    pub state: Option<RunState>,
    pub trigger: Option<lmt_core::RunTrigger>,
    pub limit: u32,
    pub before: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OperationalCounts {
    pub pending_runs: u64,
    pub running_runs: u64,
    pub due_mirrors: u64,
    pub stored_log_bytes: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MirrorOperationalRecord {
    pub name: String,
    pub owner_node: String,
    pub enabled: bool,
    pub current_run_state: Option<RunState>,
    pub current_run_created_at_ms: Option<i64>,
    pub last_run_state: Option<RunState>,
    pub last_terminal_at_ms: Option<i64>,
    pub last_success_at_ms: Option<i64>,
    pub next_due_at_ms: Option<i64>,
    pub due_since_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AttemptRecord {
    pub run_id: String,
    pub attempt_no: u32,
    pub state: AttemptState,
    pub spec_hash: String,
    pub spec: ProcessRunSpec,
    pub created_at_ms: i64,
    pub accepted_at_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<String>,
    pub failure_message: Option<String>,
    pub last_event_sequence: u64,
}

#[derive(Debug, Clone)]
pub enum PollAction {
    StartAttempt {
        run_id: String,
        attempt_no: u32,
        spec_hash: String,
        spec: ProcessRunSpec,
    },
    CancelAttempt {
        run_id: String,
        attempt_no: u32,
        spec_hash: String,
    },
}

pub struct DispatchSource {
    pub run_id: String,
    pub attempt_no: u32,
    pub mirror_name: String,
    pub mirror_generation: u64,
    pub config_toml: String,
}

#[derive(Debug, Clone)]
pub struct TerminalDecisionSource {
    pub outcome: AttemptState,
    pub attempt_no: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub cancel_requested: bool,
    pub mirror_eligible: bool,
    pub owner_unchanged: bool,
    pub mirror_name: String,
    pub current_config_toml: String,
}

#[derive(Debug, Clone, Copy)]
pub struct TerminalDecision {
    pub retry: RetryDecision,
    pub interval_next_due_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CancellationApplyResult {
    pub run: RunRecord,
    pub newly_requested: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AttemptEventApplyResult {
    pub accepted_event_sequence: u64,
    pub newly_applied: bool,
    pub retry_scheduled: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct RunPolicySnapshot {
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
}

pub struct ScheduleTickSource {
    pub mirror_name: String,
    pub config_toml: String,
    pub runtime: ScheduleRuntime,
    pub has_active_run: bool,
}

impl Store {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = tokio_rusqlite::Connection::open(path.as_ref().to_owned())
            .await
            .map_err(|error| StoreError::Worker(error.to_string()))?;
        let store = Self { connection };
        store
            .call(move |connection| configure_and_migrate(connection, now_ms()))
            .await?;
        Ok(store)
    }

    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let connection = tokio_rusqlite::Connection::open_in_memory()
            .await
            .map_err(|error| StoreError::Worker(error.to_string()))?;
        let store = Self { connection };
        store
            .call(move |connection| configure_and_migrate(connection, now_ms()))
            .await?;
        Ok(store)
    }

    async fn call<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        match self.connection.call(operation).await {
            Ok(value) => Ok(value),
            Err(tokio_rusqlite::Error::Error(error)) => Err(error),
            Err(error) => Err(StoreError::Worker(error.to_string())),
        }
    }

    pub async fn current_revision(&self) -> Result<u64, StoreError> {
        self.call(|connection| {
            Ok(
                connection.query_row("SELECT COALESCE(MAX(revision), 0) FROM config_revisions", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
    }

    pub async fn earliest_wakeup(&self) -> Result<Option<i64>, StoreError> {
        self.call(|connection| {
            Ok(connection.query_row(
                "SELECT MIN(deadline) FROM (
                   SELECT MIN(next_due_at_ms) AS deadline FROM mirror_schedule_state WHERE next_due_at_ms IS NOT NULL
                   UNION ALL
                   SELECT MIN(retry_due_at_ms) AS deadline FROM runs
                     WHERE state='running' AND retry_due_at_ms IS NOT NULL
                 )",
                [],
                |row| row.get(0),
            )?)
        })
        .await
    }

    pub async fn evaluate_due_schedules<F>(&self, now: i64, mut evaluate: F) -> Result<u64, StoreError>
    where
        F: FnMut(&ScheduleTickSource) -> Result<ScheduleRuntime, StoreError> + Send + 'static,
    {
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            let sources = {
                let mut statement = transaction.prepare(
                    "SELECT m.name,g.config_toml,s.next_due_at_ms,s.last_evaluated_at_ms,
                       s.catch_up_pending,s.catch_up_since_ms,
                       EXISTS(SELECT 1 FROM runs r WHERE r.mirror_name=m.name AND r.state IN('pending','running'))
                     FROM mirror_schedule_state s
                     JOIN mirrors m ON m.name=s.mirror_name
                     JOIN mirror_generations g ON g.mirror_name=m.name AND g.generation=m.current_generation
                     WHERE m.managed=1 AND m.enabled=1 AND s.next_due_at_ms IS NOT NULL AND s.next_due_at_ms<=?1
                     ORDER BY s.next_due_at_ms,m.name",
                )?;
                statement
                    .query_map([now], |row| {
                        Ok(ScheduleTickSource {
                            mirror_name: row.get(0)?,
                            config_toml: row.get(1)?,
                            runtime: ScheduleRuntime {
                                next_due_at_ms: row.get(2)?,
                                last_evaluated_at_ms: row.get(3)?,
                                catch_up_pending: row.get(4)?,
                                catch_up_since_ms: row.get(5)?,
                            },
                            has_active_run: row.get(6)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            for source in &sources {
                let runtime = evaluate(source)?;
                transaction.execute(
                    "UPDATE mirror_schedule_state SET next_due_at_ms=?2,last_evaluated_at_ms=?3,
                       catch_up_pending=?4,catch_up_since_ms=?5 WHERE mirror_name=?1",
                    params![
                        source.mirror_name,
                        runtime.next_due_at_ms,
                        runtime.last_evaluated_at_ms,
                        runtime.catch_up_pending,
                        runtime.catch_up_since_ms
                    ],
                )?;
            }
            transaction.commit()?;
            Ok(u64::try_from(sources.len()).unwrap_or(u64::MAX))
        })
        .await
    }

    pub async fn plan(&self, bundle: &CanonicalBundle) -> Result<ConfigPlan, StoreError> {
        let bundle = bundle.clone();
        self.call(move |connection| plan_with_connection(connection, &bundle))
            .await
    }

    pub async fn apply(
        &self,
        bundle: &CanonicalBundle,
        base_revision: u64,
        actor: &str,
        now: i64,
    ) -> Result<ConfigPlan, StoreError> {
        let bundle = bundle.clone();
        let actor = actor.to_owned();
        self.call(move |connection| {
        let transaction = connection.transaction()?;
        let plan = plan_with_connection(&transaction, &bundle)?;
        if plan.base_revision != base_revision {
            return Err(StoreError::RevisionConflict {
                current: plan.base_revision,
            });
        }
        let summary = serde_json::to_string(
            &plan
                .changes
                .iter()
                .map(|change| format!("{:?}:{}", change.kind, change.mirror))
                .collect::<Vec<_>>(),
        )?;
        transaction.execute(
            "INSERT INTO config_revisions(bundle_hash, applied_at_ms, actor, summary_json) VALUES(?1, ?2, ?3, ?4)",
            params![bundle.bundle_hash, now, actor, summary],
        )?;
        let revision = u64::try_from(transaction.last_insert_rowid()).expect("positive row id");

        for change in &plan.changes {
            match change.kind {
                ChangeKind::Remove => {
                    transaction.execute(
                        "UPDATE mirrors SET managed=0, removed_at_ms=?2 WHERE name=?1",
                        params![change.mirror, now],
                    )?;
                    cancel_undispatched_pending(&transaction, &change.mirror, now)?;
                    finalize_ineligible_retries(&transaction, &change.mirror, now)?;
                    transaction.execute("DELETE FROM mirror_schedule_state WHERE mirror_name=?1", [&change.mirror])?;
                }
                ChangeKind::Create | ChangeKind::Update | ChangeKind::Move => {
                    let mirror = &bundle.mirrors[&lmt_core::MirrorName::new(&change.mirror)
                        .map_err(|error| StoreError::InvalidConfig(error.to_string()))?];
                    let generation = change.to_generation.expect("changed mirror has generation");
                    let previous: Option<(bool, bool, String)> = transaction
                        .query_row(
                            "SELECT managed,enabled,owner_node FROM mirrors WHERE name=?1",
                            [&change.mirror],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )
                        .optional()?;
                    transaction.execute(
                        "INSERT INTO mirrors(name, managed, enabled, owner_node, current_generation, removed_at_ms)
                         VALUES(?1,1,?2,?3,?4,NULL)
                         ON CONFLICT(name) DO UPDATE SET managed=1, enabled=excluded.enabled,
                           owner_node=excluded.owner_node, current_generation=excluded.current_generation, removed_at_ms=NULL",
                        params![change.mirror, mirror.document.mirror.enabled, mirror.owner_node.as_str(), generation],
                    )?;
                    if !mirror.document.mirror.enabled {
                        cancel_undispatched_pending(&transaction, &change.mirror, now)?;
                        finalize_ineligible_retries(&transaction, &change.mirror, now)?;
                    } else if change.kind == ChangeKind::Move {
                        finalize_ineligible_retries(&transaction, &change.mirror, now)?;
                    }
                    transaction.execute(
                        "INSERT INTO mirror_generations(mirror_name,generation,config_revision,owner_node,config_hash,config_toml,created_at_ms)
                         VALUES(?1,?2,?3,?4,?5,?6,?7)",
                        params![
                            change.mirror,
                            generation,
                            revision,
                            mirror.owner_node.as_str(),
                            mirror.config_hash,
                            mirror.canonical_toml,
                            now
                        ],
                    )?;
                    reconcile_schedule(
                        &transaction,
                        change,
                        &mirror.document,
                        mirror.owner_node.as_str(),
                        previous.as_ref(),
                        now,
                    )?;
                }
            }
        }
        transaction.commit()?;
        Ok(ConfigPlan {
            base_revision: revision,
            ..plan
        })
        })
        .await
    }

    pub async fn import_legacy_credential(&self, node: &str, token: &str, now: i64) -> Result<bool, StoreError> {
        let node = node.to_owned();
        let hash = token_hash(token);
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO nodes(name,registered_at_ms,active_runs,capabilities_json) VALUES(?1,?2,0,'{}') ON CONFLICT(name) DO NOTHING",
                params![node, now],
            )?;
            let history: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM node_credentials WHERE node_name=?1)",
                [&node],
                |row| row.get(0),
            )?;
            if history {
                transaction.commit()?;
                return Ok(false);
            }
            transaction.execute(
                "INSERT INTO node_credentials(node_name,credential_id,token_hash,created_at_ms,revoked_at_ms,label)
                 VALUES(?1,'bootstrap',?2,?3,NULL,'legacy-inline')",
                params![node, hash, now],
            )?;
            transaction.commit()?;
            Ok(true)
        })
        .await
    }

    pub async fn upsert_credential(&self, node: &str, token: &str, now: i64) -> Result<(), StoreError> {
        self.import_legacy_credential(node, token, now).await.map(|_| ())
    }

    pub async fn issue_credential(
        &self,
        node: &str,
        credential_id: &str,
        label: Option<&str>,
        token: &str,
        now: i64,
    ) -> Result<CredentialRecord, StoreError> {
        let node = node.to_owned();
        let credential_id = credential_id.to_owned();
        let label = label.map(str::to_owned);
        let hash = token_hash(token);
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            let exists: bool =
                transaction.query_row("SELECT EXISTS(SELECT 1 FROM nodes WHERE name=?1)", [&node], |row| {
                    row.get(0)
                })?;
            if !exists {
                return Err(StoreError::AttemptNotFound);
            }
            transaction.execute(
                "INSERT INTO node_credentials(node_name,credential_id,token_hash,created_at_ms,label)
                 VALUES(?1,?2,?3,?4,?5)",
                params![node, credential_id, hash, now, label],
            )?;
            transaction.commit()?;
            Ok(CredentialRecord {
                node,
                id: credential_id,
                label,
                created_at_ms: now,
                last_used_at_ms: None,
                revoked_at_ms: None,
            })
        })
        .await
    }

    pub async fn list_credentials(&self, node: &str) -> Result<Vec<CredentialRecord>, StoreError> {
        let node = node.to_owned();
        self.call(move |connection| {
            let mut statement = connection.prepare(
                "SELECT node_name,credential_id,label,created_at_ms,last_used_at_ms,revoked_at_ms
                 FROM node_credentials WHERE node_name=?1 ORDER BY created_at_ms DESC,credential_id DESC",
            )?;
            statement
                .query_map([node], |row| {
                    Ok(CredentialRecord {
                        node: row.get(0)?,
                        id: row.get(1)?,
                        label: row.get(2)?,
                        created_at_ms: row.get(3)?,
                        last_used_at_ms: row.get(4)?,
                        revoked_at_ms: row.get(5)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn revoke_credential(&self, node: &str, id: &str, now: i64) -> Result<CredentialRecord, StoreError> {
        let node = node.to_owned();
        let id = id.to_owned();
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            let existing = transaction
                .query_row(
                    "SELECT node_name,credential_id,label,created_at_ms,last_used_at_ms,revoked_at_ms
                     FROM node_credentials WHERE node_name=?1 AND credential_id=?2",
                    params![node, id],
                    |row| {
                        Ok(CredentialRecord {
                            node: row.get(0)?,
                            id: row.get(1)?,
                            label: row.get(2)?,
                            created_at_ms: row.get(3)?,
                            last_used_at_ms: row.get(4)?,
                            revoked_at_ms: row.get(5)?,
                        })
                    },
                )
                .optional()?
                .ok_or(StoreError::CredentialNotFound)?;
            let revoked_at_ms = existing.revoked_at_ms.unwrap_or(now);
            transaction.execute(
                "UPDATE node_credentials SET revoked_at_ms=COALESCE(revoked_at_ms,?3)
                 WHERE node_name=?1 AND credential_id=?2",
                params![node, id, now],
            )?;
            transaction.commit()?;
            Ok(CredentialRecord {
                revoked_at_ms: Some(revoked_at_ms),
                ..existing
            })
        })
        .await
    }

    pub async fn authenticate_credential(&self, token: &str) -> Result<Option<AuthenticatedCredential>, StoreError> {
        let hash = token_hash(token);
        self.call(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT node_name,credential_id FROM node_credentials WHERE token_hash=?1 AND revoked_at_ms IS NULL",
                    [hash],
                    |row| {
                        Ok(AuthenticatedCredential {
                            node: row.get(0)?,
                            credential_id: row.get(1)?,
                        })
                    },
                )
                .optional()?)
        })
        .await
    }

    pub async fn authenticate_node(&self, token: &str) -> Result<Option<String>, StoreError> {
        self.authenticate_credential(token)
            .await
            .map(|credential| credential.map(|value| value.node))
    }

    pub async fn mark_credential_used(&self, node: &str, id: &str, now: i64) -> Result<bool, StoreError> {
        let node = node.to_owned();
        let id = id.to_owned();
        self.call(move |connection| {
            Ok(connection.execute(
                "UPDATE node_credentials SET last_used_at_ms=?3
                 WHERE node_name=?1 AND credential_id=?2 AND revoked_at_ms IS NULL
                   AND (last_used_at_ms IS NULL OR last_used_at_ms<=?3-60000)",
                params![node, id, now],
            )? > 0)
        })
        .await
    }

    pub async fn observe_node(&self, observation: NodeObservation) -> Result<(), StoreError> {
        let capabilities = serde_json::json!({"mirror_root": observation.mirror_root}).to_string();
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            let bound: Option<String> = transaction.query_row(
                "SELECT bound_agent_id FROM nodes WHERE name=?1",
                [&observation.node],
                |row| row.get(0),
            )?;
            if let Some(bound_agent_id) = bound
                && bound_agent_id != observation.agent_instance_id
            {
                return Err(StoreError::AgentBindingConflict {
                    bound_agent_id,
                    presented_agent_id: observation.agent_instance_id,
                });
            }
            transaction.execute(
                "UPDATE nodes SET agent_version=?2,agent_instance_id=?3,bound_agent_id=COALESCE(bound_agent_id,?3),
             agent_boot_id=?4,last_seen_at_ms=?5,active_runs=?6,mirror_root_free_bytes=?7,
             max_concurrent_runs=?8,capabilities_json=?9 WHERE name=?1",
                params![
                    observation.node,
                    observation.agent_version,
                    observation.agent_instance_id,
                    observation.agent_boot_id,
                    observation.observed_at_ms,
                    observation.active_runs,
                    observation.mirror_root_free_bytes,
                    observation.max_concurrent_runs,
                    capabilities
                ],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn replace_agent_binding(
        &self,
        node: &str,
        agent_id: &str,
        acknowledge_execution_risk: bool,
    ) -> Result<(), StoreError> {
        let node = node.to_owned();
        let agent_id = agent_id.to_owned();
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            let exists: bool =
                transaction.query_row("SELECT EXISTS(SELECT 1 FROM nodes WHERE name=?1)", [&node], |row| {
                    row.get(0)
                })?;
            if !exists {
                return Err(StoreError::AttemptNotFound);
            }
            let potentially_executing: bool = transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM attempts a JOIN runs r ON r.id=a.run_id
                    WHERE r.owner_node=?1 AND r.state IN('pending','running')
                      AND a.dispatch_count>0
                      AND a.state IN('queued','accepted','running')
                )",
                [&node],
                |row| row.get(0),
            )?;
            if potentially_executing && !acknowledge_execution_risk {
                return Err(StoreError::BindingReplacementUnsafe);
            }
            transaction.execute(
                "UPDATE nodes SET bound_agent_id=?2,agent_instance_id=NULL,agent_boot_id=NULL WHERE name=?1",
                params![node, agent_id],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn list_nodes(&self) -> Result<Vec<NodeRecord>, StoreError> {
        self.call(|connection| {
            let mut statement = connection.prepare(
                "SELECT name,agent_version,agent_instance_id,bound_agent_id,agent_boot_id,last_seen_at_ms,
                 active_runs,mirror_root_free_bytes,max_concurrent_runs FROM nodes ORDER BY name",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(NodeRecord {
                    name: row.get(0)?,
                    agent_version: row.get(1)?,
                    agent_instance_id: row.get(2)?,
                    bound_agent_id: row.get(3)?,
                    agent_boot_id: row.get(4)?,
                    last_seen_at_ms: row.get(5)?,
                    active_runs: row.get(6)?,
                    mirror_root_free_bytes: row.get(7)?,
                    max_concurrent_runs: row.get(8)?,
                })
            })?;
            rows.collect::<Result<_, _>>().map_err(StoreError::from)
        })
        .await
    }

    pub async fn list_mirrors(&self) -> Result<Vec<MirrorRecord>, StoreError> {
        self.call(|connection| {
            let mut statement = connection.prepare(
                "SELECT m.name,m.managed,m.enabled,m.owner_node,m.current_generation,s.next_due_at_ms,
                     CASE WHEN s.catch_up_pending=1 THEN s.catch_up_since_ms END
                     FROM mirrors m LEFT JOIN mirror_schedule_state s ON s.mirror_name=m.name ORDER BY m.name",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(MirrorRecord {
                    name: row.get(0)?,
                    managed: row.get(1)?,
                    enabled: row.get(2)?,
                    owner_node: row.get(3)?,
                    current_generation: row.get(4)?,
                    next_due_at_ms: row.get(5)?,
                    scheduled_due_since_ms: row.get(6)?,
                })
            })?;
            rows.collect::<Result<_, _>>().map_err(StoreError::from)
        })
        .await
    }

    pub async fn get_mirror(&self, name: &str) -> Result<Option<MirrorRecord>, StoreError> {
        let name = name.to_owned();
        self.call(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT m.name,m.managed,m.enabled,m.owner_node,m.current_generation,s.next_due_at_ms,
                     CASE WHEN s.catch_up_pending=1 THEN s.catch_up_since_ms END
                     FROM mirrors m LEFT JOIN mirror_schedule_state s ON s.mirror_name=m.name WHERE m.name=?1",
                    [name],
                    |row| {
                        Ok(MirrorRecord {
                            name: row.get(0)?,
                            managed: row.get(1)?,
                            enabled: row.get(2)?,
                            owner_node: row.get(3)?,
                            current_generation: row.get(4)?,
                            next_due_at_ms: row.get(5)?,
                            scheduled_due_since_ms: row.get(6)?,
                        })
                    },
                )
                .optional()?)
        })
        .await
    }

    pub async fn create_manual_run<F>(
        &self,
        mirror: &str,
        request_id: &str,
        now: i64,
        policy: F,
    ) -> Result<RunRecord, StoreError>
    where
        F: FnOnce(&str) -> Result<RunPolicySnapshot, StoreError> + Send + 'static,
    {
        let mirror = mirror.to_owned();
        let request_id = request_id.to_owned();
        self.call(move |connection| {
        let transaction = connection.transaction()?;
        if let Some(existing) = find_run_by_request(&transaction, &request_id)? {
            if existing.mirror_name == mirror {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::RequestConflict);
        }
        let current = transaction
            .query_row(
                "SELECT m.managed,m.enabled,m.owner_node,m.current_generation,g.config_toml
                 FROM mirrors m JOIN mirror_generations g ON g.mirror_name=m.name AND g.generation=m.current_generation
                 WHERE m.name=?1",
                [&mirror],
                |row| {
                    Ok((
                        row.get::<_, bool>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, u64>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or(StoreError::MirrorNotFound)?;
        if !current.0 || !current.1 {
            return Err(StoreError::MirrorIneligible);
        }
        if let Some(run_id) = transaction
            .query_row(
                "SELECT id FROM runs WHERE mirror_name=?1 AND state IN ('pending','running')",
                [&mirror],
                |row| row.get(0),
            )
            .optional()?
        {
            return Err(StoreError::MirrorBusy { run_id });
        }
        let policy = policy(&current.4)?;
        let run_id = RunId::new().to_string();
        transaction.execute(
            "INSERT INTO runs(id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,max_attempts,retry_delay_ms,manual_request_id)
             VALUES(?1,?2,?3,?4,'manual','pending',?5,?6,?7,?8)",
            params![
                run_id,
                mirror,
                current.3,
                current.2,
                now,
                policy.max_attempts,
                policy.retry_delay_ms,
                request_id
            ],
        )?;
        transaction.execute(
            "UPDATE mirror_schedule_state SET catch_up_pending=0,catch_up_since_ms=NULL WHERE mirror_name=?1",
            [&mirror],
        )?;
        transaction.commit()?;
        connection
            .query_row(
                "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,finished_at_ms,
                 final_exit_code,failure_kind,failure_message,max_attempts,retry_delay_ms,scheduled_for_at_ms,
                 retry_due_at_ms,cancel_requested_at_ms FROM runs WHERE id=?1",
                [&run_id],
                map_run,
            )
            .optional()?
            .ok_or(StoreError::AttemptNotFound)
        })
        .await
    }

    pub async fn poll_action<F>(&self, node: &str, now: i64, compile: F) -> Result<Option<PollAction>, StoreError>
    where
        F: FnOnce(&DispatchSource) -> Result<(ProcessRunSpec, String, RunPolicySnapshot), StoreError> + Send + 'static,
    {
        let node = node.to_owned();
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            if let Some(action) = find_cancellation(&transaction, &node)? {
                transaction.commit()?;
                return Ok(Some(action));
            }
            let redelivery = transaction
                .query_row(
                    "SELECT a.run_id,a.attempt_no,a.spec_hash,a.spec_json
                     FROM attempts a JOIN runs r ON r.id=a.run_id
                     WHERE r.owner_node=?1 AND r.state IN('pending','running')
                       AND a.state='queued' AND a.dispatch_count>0
                     ORDER BY a.last_dispatch_at_ms,a.run_id,a.attempt_no LIMIT 1",
                    [&node],
                    map_poll_action,
                )
                .optional()?;
            if let Some(action) = redelivery {
                mark_redelivery(&transaction, &action, now)?;
                transaction.commit()?;
                return Ok(Some(action));
            }
            if !node_has_capacity(&transaction, &node)? {
                transaction.commit()?;
                return Ok(None);
            }

            let initial: Option<(String, String, u64, u32)> = transaction
                .query_row(
                    "SELECT r.id,r.mirror_name,r.mirror_generation,1
                     FROM runs r JOIN mirrors m ON m.name=r.mirror_name
                     WHERE r.owner_node=?1 AND r.state='pending' AND r.trigger='manual'
                       AND m.managed=1 AND m.enabled=1
                       AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.run_id=r.id)
                     ORDER BY r.created_at_ms LIMIT 1",
                    [&node],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let candidate = if initial.is_some() {
                initial
            } else {
                transaction
                    .query_row(
                        "SELECT r.id,r.mirror_name,r.mirror_generation,r.attempt_count+1
                         FROM runs r JOIN mirrors m ON m.name=r.mirror_name
                         WHERE r.owner_node=?1 AND r.state='running' AND r.retry_due_at_ms<=?2
                           AND r.cancel_requested_at_ms IS NULL AND m.managed=1 AND m.enabled=1
                           AND m.owner_node=r.owner_node AND r.attempt_count<r.max_attempts
                         ORDER BY r.retry_due_at_ms,r.created_at_ms LIMIT 1",
                        params![node, now],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?
            };
            let candidate = if candidate.is_some() {
                candidate
            } else {
                materialize_scheduled_candidate(&transaction, &node, now)?
            };
            let Some((run_id, mirror_name, generation, attempt_no)) = candidate else {
                transaction.commit()?;
                return Ok(None);
            };
            let config_toml = generation_config(&transaction, &mirror_name, generation)?;
            let source = DispatchSource {
                run_id: run_id.clone(),
                attempt_no,
                mirror_name,
                mirror_generation: generation,
                config_toml,
            };
            let (spec, spec_hash, policy) = compile(&source)?;
            transaction.execute(
                "UPDATE runs SET max_attempts=?2,retry_delay_ms=?3 WHERE id=?1 AND trigger='scheduled'",
                params![run_id, policy.max_attempts, policy.retry_delay_ms],
            )?;
            let spec_json = serde_json::to_string(&spec)?;
            transaction.execute(
                "INSERT INTO attempts(run_id,attempt_no,state,spec_hash,spec_json,created_at_ms,
                   last_event_sequence,dispatch_count,last_dispatch_at_ms)
                 VALUES(?1,?2,'queued',?3,?4,?5,0,1,?5)",
                params![run_id, attempt_no, spec_hash, spec_json, now],
            )?;
            transaction.execute(
                "UPDATE runs SET attempt_count=?2,retry_due_at_ms=NULL WHERE id=?1",
                params![run_id, attempt_no],
            )?;
            transaction.commit()?;
            Ok(Some(PollAction::StartAttempt {
                run_id,
                attempt_no,
                spec_hash,
                spec,
            }))
        })
        .await
    }

    pub async fn request_cancellation<F>(
        &self,
        run_id: &str,
        now: i64,
        rearm: F,
    ) -> Result<CancellationApplyResult, StoreError>
    where
        F: FnOnce(&str) -> Result<Option<i64>, StoreError> + Send + 'static,
    {
        let run_id = run_id.to_owned();
        self.call(move |connection| {
            let transaction = connection.transaction()?;
            let run = transaction
                .query_row(
                    "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,
                     finished_at_ms,final_exit_code,failure_kind,failure_message,max_attempts,retry_delay_ms,
                     scheduled_for_at_ms,retry_due_at_ms,cancel_requested_at_ms FROM runs WHERE id=?1",
                    [&run_id],
                    map_run,
                )
                .optional()?
                .ok_or(StoreError::AttemptNotFound)?;
            if run.state.is_terminal() {
                transaction.commit()?;
                return Ok(CancellationApplyResult {
                    run,
                    newly_requested: false,
                });
            }
            let newly_requested = run.cancel_requested_at_ms.is_none();
            transaction.execute(
                "UPDATE runs SET cancel_requested_at_ms=COALESCE(cancel_requested_at_ms,?2) WHERE id=?1",
                params![run_id, now],
            )?;
            let active_dispatched: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempts a JOIN runs r ON r.id=a.run_id
                  WHERE a.run_id=?1 AND a.attempt_no=r.attempt_count AND a.dispatch_count>0
                    AND a.state NOT IN('succeeded','failed','timed_out','cancelled','rejected','interrupted'))",
                [&run_id],
                |row| row.get(0),
            )?;
            if !active_dispatched {
                let interval_next_due_at_ms = transaction
                    .query_row(
                        "SELECT g.config_toml FROM runs r JOIN mirrors m ON m.name=r.mirror_name
                         JOIN mirror_generations g ON g.mirror_name=m.name AND g.generation=m.current_generation
                         WHERE r.id=?1 AND m.managed=1 AND m.enabled=1 AND m.owner_node=r.owner_node",
                        [&run_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .map(|config| rearm(&config))
                    .transpose()?
                    .flatten();
                transaction.execute(
                    "UPDATE attempts SET state='cancelled',finished_at_ms=COALESCE(finished_at_ms,?2)
                     WHERE run_id=?1 AND dispatch_count=0
                       AND state NOT IN('succeeded','failed','timed_out','cancelled','rejected','interrupted')",
                    params![run_id, now],
                )?;
                transaction.execute(
                    "UPDATE runs SET state='cancelled',finished_at_ms=?2,retry_due_at_ms=NULL,
                       final_exit_code=NULL,failure_kind=NULL,failure_message=NULL WHERE id=?1",
                    params![run_id, now],
                )?;
                if let Some(next_due) = interval_next_due_at_ms {
                    transaction.execute(
                        "UPDATE mirror_schedule_state SET next_due_at_ms=?2,last_evaluated_at_ms=?3,
                           catch_up_pending=0,catch_up_since_ms=NULL
                         WHERE mirror_name=(SELECT mirror_name FROM runs WHERE id=?1)",
                        params![run_id, next_due, now],
                    )?;
                }
            }
            let result = transaction.query_row(
                "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,
                 finished_at_ms,final_exit_code,failure_kind,failure_message,max_attempts,retry_delay_ms,
                 scheduled_for_at_ms,retry_due_at_ms,cancel_requested_at_ms FROM runs WHERE id=?1",
                [&run_id],
                map_run,
            )?;
            transaction.commit()?;
            Ok(CancellationApplyResult {
                run: result,
                newly_requested,
            })
        })
        .await
    }

    pub async fn apply_event<F>(
        &self,
        run_id: &str,
        attempt_no: u32,
        event: &AttemptEvent,
        now: i64,
        decide_terminal: F,
    ) -> Result<AttemptEventApplyResult, StoreError>
    where
        F: FnOnce(TerminalDecisionSource, i64) -> Result<TerminalDecision, StoreError> + Send + 'static,
    {
        let run_id = run_id.to_owned();
        let event = event.clone();
        self.call(move |connection| {
        let transaction = connection.transaction()?;
        let current: Option<(AttemptState, u64)> = transaction
            .query_row(
                "SELECT state,last_event_sequence FROM attempts WHERE run_id=?1 AND attempt_no=?2",
                params![run_id, attempt_no],
                |row| Ok((parse_attempt_state(&row.get::<_, String>(0)?)?, row.get(1)?)),
            )
            .optional()?;
        let Some((state, sequence)) = current else {
            return Err(StoreError::AttemptNotFound);
        };
        if event.event_sequence <= sequence {
            transaction.commit()?;
            return Ok(AttemptEventApplyResult {
                accepted_event_sequence: sequence,
                newly_applied: false,
                retry_scheduled: false,
            });
        }
        let projection = project_attempt_event(state, &event).map_err(|error| StoreError::IllegalTransition {
            from: error.from,
            to: error.to,
        })?;
        transaction.execute(
            "UPDATE attempts SET state=?3,last_event_sequence=?4,agent_instance_id=?5,
             accepted_at_ms=COALESCE(accepted_at_ms,?6),started_at_ms=COALESCE(started_at_ms,?7),
             finished_at_ms=COALESCE(finished_at_ms,?8),exit_code=?9,failure_kind=?10,failure_message=?11
             WHERE run_id=?1 AND attempt_no=?2",
            params![
                run_id,
                attempt_no,
                attempt_state_str(event.state),
                event.event_sequence,
                event.agent_instance_id,
                event.accepted_at_ms,
                event.started_at_ms,
                event.finished_at_ms,
                event.exit_code,
                event.failure_kind.map(failure_kind_str),
                event.failure_message,
            ],
        )?;
        let mut retry_scheduled = false;
        if projection.run_state == Some(RunState::Running) {
            transaction.execute(
                "UPDATE runs SET state='running',started_at_ms=COALESCE(started_at_ms,?2) WHERE id=?1 AND state='pending'",
                params![run_id, projection.run_started_at_ms.unwrap_or(now)],
            )?;
        } else if projection.run_state.is_some() {
            let source = terminal_source(&transaction, &run_id, attempt_no, event.state)?;
            let decision = decide_terminal(source.clone(), now)?;
            match decision.retry {
                RetryDecision::Schedule { retry_due_at_ms } => {
                    retry_scheduled = true;
                    transaction.execute(
                        "UPDATE runs SET state='running',started_at_ms=COALESCE(started_at_ms,?2),
                           retry_due_at_ms=?3,finished_at_ms=NULL,final_exit_code=NULL,
                           failure_kind=NULL,failure_message=NULL WHERE id=?1 AND state IN('pending','running')",
                        params![run_id, projection.run_started_at_ms, retry_due_at_ms],
                    )?;
                }
                RetryDecision::Final(run_state) => {
                    transaction.execute(
                        "UPDATE runs SET state=?2,started_at_ms=COALESCE(started_at_ms,?3),finished_at_ms=?4,
                           retry_due_at_ms=NULL,final_exit_code=?5,failure_kind=?6,failure_message=?7
                         WHERE id=?1 AND state IN('pending','running')",
                        params![
                            run_id,
                            run_state_str(run_state),
                            projection.run_started_at_ms,
                            projection.run_finished_at_ms.unwrap_or(now),
                            event.exit_code,
                            event.failure_kind.map(failure_kind_str),
                            event.failure_message,
                        ],
                    )?;
                    if let Some(next_due) = decision.interval_next_due_at_ms {
                        rearm_schedule(&transaction, &source.mirror_name, next_due, now)?;
                    }
                }
            }
        }
        transaction.commit()?;
        Ok(AttemptEventApplyResult {
            accepted_event_sequence: event.event_sequence,
            newly_applied: true,
            retry_scheduled,
        })
        })
        .await
    }

    pub async fn attempt_belongs_to_node(&self, run_id: &str, attempt_no: u32, node: &str) -> Result<bool, StoreError> {
        let run_id = run_id.to_owned();
        let node = node.to_owned();
        self.call(move |connection| {
            Ok(connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempts a JOIN runs r ON r.id=a.run_id
             WHERE a.run_id=?1 AND a.attempt_no=?2 AND r.owner_node=?3)",
                params![run_id, attempt_no, node],
                |row| row.get(0),
            )?)
        })
        .await
    }

    pub async fn get_run(&self, id: &str) -> Result<Option<RunRecord>, StoreError> {
        let id = id.to_owned();
        self.call(move |connection| Ok(connection.query_row(
            "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,finished_at_ms,
             final_exit_code,failure_kind,failure_message,max_attempts,retry_delay_ms,scheduled_for_at_ms,
             retry_due_at_ms,cancel_requested_at_ms FROM runs WHERE id=?1",
            [id], map_run,
        ).optional()?))
        .await
    }

    pub async fn list_runs(&self) -> Result<Vec<RunRecord>, StoreError> {
        self.call(|connection| {
        let mut statement = connection.prepare(
            "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,finished_at_ms,
             final_exit_code,failure_kind,failure_message,max_attempts,retry_delay_ms,scheduled_for_at_ms,
             retry_due_at_ms,cancel_requested_at_ms FROM runs ORDER BY created_at_ms DESC",
        )?;
        statement
            .query_map([], map_run)?
            .collect::<Result<_, _>>()
            .map_err(StoreError::from)
        })
        .await
    }

    pub async fn operational_counts(&self) -> Result<OperationalCounts, StoreError> {
        self.call(|connection| {
            connection
                .query_row(
                    "SELECT
                       (SELECT COUNT(*) FROM runs WHERE state='pending'),
                       (SELECT COUNT(*) FROM runs WHERE state='running'),
                       (SELECT COUNT(*) FROM mirror_schedule_state WHERE catch_up_pending=1),
                       (SELECT stored_log_bytes FROM operational_counters WHERE singleton=1)",
                    [],
                    |row| {
                        Ok(OperationalCounts {
                            pending_runs: row.get(0)?,
                            running_runs: row.get(1)?,
                            due_mirrors: row.get(2)?,
                            stored_log_bytes: row.get(3)?,
                        })
                    },
                )
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn mirror_operational_status(&self) -> Result<Vec<MirrorOperationalRecord>, StoreError> {
        self.call(|connection| {
            let mut statement = connection.prepare(
                "SELECT m.name,m.owner_node,m.enabled,
                   (SELECT state FROM runs WHERE mirror_name=m.name AND state IN('pending','running')
                     ORDER BY created_at_ms DESC,id DESC LIMIT 1),
                   (SELECT created_at_ms FROM runs WHERE mirror_name=m.name AND state IN('pending','running')
                     ORDER BY created_at_ms DESC,id DESC LIMIT 1),
                   (SELECT state FROM runs WHERE mirror_name=m.name AND state NOT IN('pending','running')
                     ORDER BY created_at_ms DESC,id DESC LIMIT 1),
                   (SELECT finished_at_ms FROM runs WHERE mirror_name=m.name AND state NOT IN('pending','running')
                     ORDER BY created_at_ms DESC,id DESC LIMIT 1),
                   (SELECT MAX(finished_at_ms) FROM runs WHERE mirror_name=m.name AND state='succeeded'),
                   s.next_due_at_ms,s.catch_up_since_ms
                 FROM mirrors m LEFT JOIN mirror_schedule_state s ON s.mirror_name=m.name
                 WHERE m.managed=1 ORDER BY m.name",
            )?;
            statement
                .query_map([], |row| {
                    let current: Option<String> = row.get(3)?;
                    let last: Option<String> = row.get(5)?;
                    Ok(MirrorOperationalRecord {
                        name: row.get(0)?,
                        owner_node: row.get(1)?,
                        enabled: row.get(2)?,
                        current_run_state: current.as_deref().map(parse_run_state).transpose()?,
                        current_run_created_at_ms: row.get(4)?,
                        last_run_state: last.as_deref().map(parse_run_state).transpose()?,
                        last_terminal_at_ms: row.get(6)?,
                        last_success_at_ms: row.get(7)?,
                        next_due_at_ms: row.get(8)?,
                        due_since_ms: row.get(9)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn database_diagnostics(&self) -> Result<(u32, bool), StoreError> {
        self.call(|connection| {
            let schema = connection.query_row("SELECT COALESCE(MAX(version),0) FROM schema_migrations", [], |row| {
                row.get(0)
            })?;
            let quick: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
            Ok((schema, quick == "ok"))
        })
        .await
    }

    pub async fn query_runs(&self, query: RunQuery) -> Result<Vec<RunRecord>, StoreError> {
        if query.limit == 0 || query.limit > 500 {
            return Err(StoreError::InvalidConfig(
                "Run query limit must be between 1 and 500".into(),
            ));
        }
        self.call(move |connection| {
            let cursor = query
                .before
                .as_ref()
                .map(|id| {
                    connection
                        .query_row("SELECT created_at_ms,id FROM runs WHERE id=?1", [id], |row| {
                            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                        })
                        .optional()
                })
                .transpose()?
                .flatten();
            if query.before.is_some() && cursor.is_none() {
                return Err(StoreError::AttemptNotFound);
            }
            let (cursor_created, cursor_id) = cursor.map_or((None, None), |(created, id)| (Some(created), Some(id)));
            let state = query.state.map(|state| run_state_str(state).to_owned());
            let trigger = query.trigger.map(|trigger| match trigger {
                lmt_core::RunTrigger::Manual => "manual".to_owned(),
                lmt_core::RunTrigger::Scheduled => "scheduled".to_owned(),
            });
            let mut statement = connection.prepare(
                "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,
                 finished_at_ms,final_exit_code,failure_kind,failure_message,max_attempts,retry_delay_ms,
                 scheduled_for_at_ms,retry_due_at_ms,cancel_requested_at_ms
                 FROM runs
                 WHERE (?1 IS NULL OR mirror_name=?1)
                   AND (?2 IS NULL OR owner_node=?2)
                   AND (?3 IS NULL OR state=?3)
                   AND (?4 IS NULL OR trigger=?4)
                   AND (?5 IS NULL OR created_at_ms<?5 OR (created_at_ms=?5 AND id<?6))
                 ORDER BY created_at_ms DESC,id DESC LIMIT ?7",
            )?;
            statement
                .query_map(
                    params![
                        query.mirror,
                        query.node,
                        state,
                        trigger,
                        cursor_created,
                        cursor_id,
                        query.limit
                    ],
                    map_run,
                )?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn list_attempts(&self, run_id: &str) -> Result<Vec<AttemptRecord>, StoreError> {
        let run_id = run_id.to_owned();
        self.call(move |connection| {
        let mut statement = connection.prepare(
            "SELECT run_id,attempt_no,state,spec_hash,spec_json,created_at_ms,accepted_at_ms,started_at_ms,finished_at_ms,
             exit_code,failure_kind,failure_message,last_event_sequence FROM attempts WHERE run_id=?1 ORDER BY attempt_no",
        )?;
        let rows = statement.query_map([run_id], |row| {
            let state_text: String = row.get(2)?;
            let spec_text: String = row.get(4)?;
            Ok(AttemptRecord {
                run_id: row.get(0)?,
                attempt_no: row.get(1)?,
                state: parse_attempt_state(&state_text)?,
                spec_hash: row.get(3)?,
                spec: serde_json::from_str(&spec_text).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        spec_text.len(),
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                created_at_ms: row.get(5)?,
                accepted_at_ms: row.get(6)?,
                started_at_ms: row.get(7)?,
                finished_at_ms: row.get(8)?,
                exit_code: row.get(9)?,
                failure_kind: row.get(10)?,
                failure_message: row.get(11)?,
                last_event_sequence: row.get(12)?,
            })
        })?;
        rows.collect::<Result<_, _>>().map_err(StoreError::from)
        })
        .await
    }

    pub async fn log_metadata(&self, run_id: &str, attempt_no: u32) -> Result<Option<LogMetadata>, StoreError> {
        let run_id = run_id.to_owned();
        self.call(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT relative_path,stored_bytes,complete,updated_at_ms,expired_at_ms
                     FROM attempt_logs WHERE run_id=?1 AND attempt_no=?2",
                    params![run_id, attempt_no],
                    |row| {
                        Ok(LogMetadata {
                            relative_path: row.get(0)?,
                            stored_bytes: row.get(1)?,
                            complete: row.get(2)?,
                            updated_at_ms: row.get(3)?,
                            expired_at_ms: row.get(4)?,
                        })
                    },
                )
                .optional()?)
        })
        .await
    }

    pub async fn log_retention_entries(&self) -> Result<Vec<LogRetentionEntry>, StoreError> {
        self.call(|connection| {
            let mut statement = connection.prepare(
                "SELECT l.run_id,l.attempt_no,l.stored_bytes,l.updated_at_ms,
                    CASE WHEN l.complete=1 AND a.state IN('succeeded','failed','timed_out','cancelled','interrupted','rejected')
                         THEN 1 ELSE 0 END,l.expired_at_ms
                 FROM attempt_logs l JOIN attempts a ON a.run_id=l.run_id AND a.attempt_no=l.attempt_no
                 ORDER BY l.updated_at_ms,l.run_id,l.attempt_no",
            )?;
            statement
                .query_map([], |row| {
                    Ok(LogRetentionEntry {
                        run_id: row.get(0)?,
                        attempt_no: row.get(1)?,
                        stored_bytes: row.get(2)?,
                        updated_at_ms: row.get(3)?,
                        eligible: row.get(4)?,
                        expired_at_ms: row.get(5)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    pub async fn mark_log_expired(&self, run_id: &str, attempt_no: u32, now: i64) -> Result<bool, StoreError> {
        let run_id = run_id.to_owned();
        self.call(move |connection| {
            Ok(connection.execute(
                "UPDATE attempt_logs SET expired_at_ms=?3
                 WHERE run_id=?1 AND attempt_no=?2 AND complete=1 AND expired_at_ms IS NULL
                   AND EXISTS(
                     SELECT 1 FROM attempts a WHERE a.run_id=?1 AND a.attempt_no=?2
                       AND a.state IN('succeeded','failed','timed_out','cancelled','interrupted','rejected')
                   )",
                params![run_id, attempt_no, now],
            )? > 0)
        })
        .await
    }

    pub async fn update_log_metadata(
        &self,
        run_id: &str,
        attempt_no: u32,
        relative_path: &str,
        stored_bytes: u64,
        complete: bool,
        now: i64,
    ) -> Result<(), StoreError> {
        let run_id = run_id.to_owned();
        let relative_path = relative_path.to_owned();
        self.call(move |connection| {
            connection.execute(
                "INSERT INTO attempt_logs(run_id,attempt_no,relative_path,stored_bytes,complete,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(run_id,attempt_no) DO UPDATE SET stored_bytes=excluded.stored_bytes,
             complete=MAX(attempt_logs.complete,excluded.complete),updated_at_ms=excluded.updated_at_ms",
                params![run_id, attempt_no, relative_path, stored_bytes, complete, now],
            )?;
            Ok(())
        })
        .await
    }
}

/// Normalizes a verified, offline restored snapshot before it can dispatch work.
/// The caller must hold the Server process lock.
pub fn normalize_restored_database(path: &Path, restored_at_ms: i64) -> Result<(), StoreError> {
    let mut connection = Connection::open(path)?;
    configure_and_migrate(&mut connection, restored_at_ms)?;
    let transaction = connection.transaction()?;
    let message = "interrupted by offline control-plane restore";
    transaction.execute(
        "UPDATE attempts SET state='interrupted',finished_at_ms=?1,exit_code=NULL,
           failure_kind='interrupted',failure_message=?2
         WHERE state IN('queued','accepted','running')",
        params![restored_at_ms, message],
    )?;
    transaction.execute(
        "UPDATE runs SET state='cancelled',finished_at_ms=?1,retry_due_at_ms=NULL,
           final_exit_code=NULL,failure_kind=NULL,failure_message=?2
         WHERE state='pending' AND NOT EXISTS(
           SELECT 1 FROM attempts WHERE attempts.run_id=runs.id AND attempts.dispatch_count > 0
         )",
        params![
            restored_at_ms,
            "cancelled by offline control-plane restore before dispatch"
        ],
    )?;
    transaction.execute(
        "UPDATE runs SET state='failed',finished_at_ms=?1,retry_due_at_ms=NULL,
           final_exit_code=NULL,failure_kind='interrupted',failure_message=?2
         WHERE state IN('pending','running')",
        params![restored_at_ms, message],
    )?;
    transaction.execute(
        "UPDATE nodes SET active_runs=0,agent_instance_id=NULL,agent_boot_id=NULL",
        [],
    )?;
    transaction.commit()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(())
}

fn reconcile_schedule(
    transaction: &Transaction<'_>,
    change: &ConfigChange,
    document: &MirrorDocument,
    owner_node: &str,
    previous: Option<&(bool, bool, String)>,
    now: i64,
) -> Result<(), StoreError> {
    let Some(schedule) = document.schedule.as_ref().filter(|_| document.mirror.enabled) else {
        transaction.execute(
            "DELETE FROM mirror_schedule_state WHERE mirror_name=?1",
            [&change.mirror],
        )?;
        return Ok(());
    };
    let schedule_hash = schedule.semantic_hash();
    let existing_hash: Option<String> = transaction
        .query_row(
            "SELECT schedule_hash FROM mirror_schedule_state WHERE mirror_name=?1",
            [&change.mirror],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let reset = matches!(change.kind, ChangeKind::Create | ChangeKind::Move)
        || previous.is_none_or(|(managed, enabled, owner)| !managed || !enabled || owner != owner_node)
        || existing_hash.as_deref() != Some(&schedule_hash);
    if reset {
        let runtime = activate_schedule(schedule, now).map_err(StoreError::InvalidConfig)?;
        transaction.execute(
            "INSERT INTO mirror_schedule_state(
               mirror_name,schedule_hash,next_due_at_ms,last_evaluated_at_ms,catch_up_pending,catch_up_since_ms
             ) VALUES(?1,?2,?3,?4,0,NULL)
             ON CONFLICT(mirror_name) DO UPDATE SET
               schedule_hash=excluded.schedule_hash,next_due_at_ms=excluded.next_due_at_ms,
               last_evaluated_at_ms=excluded.last_evaluated_at_ms,catch_up_pending=0,catch_up_since_ms=NULL",
            params![
                change.mirror,
                schedule_hash,
                runtime.next_due_at_ms,
                runtime.last_evaluated_at_ms
            ],
        )?;
    }
    Ok(())
}

fn plan_with_connection(connection: &Connection, bundle: &CanonicalBundle) -> Result<ConfigPlan, StoreError> {
    let base_revision = connection.query_row("SELECT COALESCE(MAX(revision),0) FROM config_revisions", [], |row| {
        row.get(0)
    })?;
    let mut statement = connection.prepare(
        "SELECT m.name,m.managed,m.owner_node,m.current_generation,g.config_hash FROM mirrors m
         JOIN mirror_generations g ON g.mirror_name=m.name AND g.generation=m.current_generation",
    )?;
    let current = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut changes = Vec::new();
    for (name, managed, node, generation, hash) in &current {
        let parsed = lmt_core::MirrorName::new(name).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
        match bundle.mirrors.get(&parsed) {
            None if *managed => changes.push(ConfigChange {
                kind: ChangeKind::Remove,
                mirror: name.clone(),
                from_generation: Some(*generation),
                to_generation: None,
                from_node: Some(node.clone()),
                to_node: None,
            }),
            Some(next) if !managed || next.config_hash != *hash || next.owner_node.as_str() != node => {
                let kind = if *managed && next.owner_node.as_str() != node {
                    ChangeKind::Move
                } else {
                    ChangeKind::Update
                };
                changes.push(ConfigChange {
                    kind,
                    mirror: name.clone(),
                    from_generation: Some(*generation),
                    to_generation: Some(generation + 1),
                    from_node: Some(node.clone()),
                    to_node: Some(next.owner_node.to_string()),
                });
            }
            _ => {}
        }
    }
    for (name, mirror) in &bundle.mirrors {
        if !current.iter().any(|current| current.0 == name.as_str()) {
            changes.push(ConfigChange {
                kind: ChangeKind::Create,
                mirror: name.to_string(),
                from_generation: None,
                to_generation: Some(1),
                from_node: None,
                to_node: Some(mirror.owner_node.to_string()),
            });
        }
    }
    changes.sort_by(|left, right| left.mirror.cmp(&right.mirror));
    Ok(ConfigPlan {
        base_revision,
        bundle_hash: bundle.bundle_hash.clone(),
        changes,
    })
}

fn cancel_undispatched_pending(transaction: &Transaction<'_>, mirror: &str, now: i64) -> Result<(), StoreError> {
    transaction.execute(
        "UPDATE attempts SET state='cancelled',finished_at_ms=?2
         WHERE run_id IN (SELECT id FROM runs WHERE mirror_name=?1 AND state='pending')
           AND dispatch_count=0 AND state='queued'",
        params![mirror, now],
    )?;
    transaction.execute(
        "UPDATE runs SET state='cancelled',finished_at_ms=?2,failure_kind='configuration_removed',
         failure_message='configuration disabled or removed before dispatch'
         WHERE mirror_name=?1 AND state='pending' AND NOT EXISTS(
           SELECT 1 FROM attempts WHERE attempts.run_id=runs.id AND dispatch_count>0)",
        params![mirror, now],
    )?;
    Ok(())
}

fn find_cancellation(transaction: &Transaction<'_>, node: &str) -> Result<Option<PollAction>, StoreError> {
    Ok(transaction
        .query_row(
            "SELECT a.run_id,a.attempt_no,a.spec_hash
             FROM attempts a JOIN runs r ON r.id=a.run_id
             WHERE r.owner_node=?1 AND r.state IN('pending','running')
               AND r.cancel_requested_at_ms IS NOT NULL
               AND a.attempt_no=r.attempt_count AND a.dispatch_count>0
               AND a.state NOT IN('succeeded','failed','timed_out','cancelled','rejected','interrupted')
             ORDER BY r.cancel_requested_at_ms,a.run_id LIMIT 1",
            [node],
            |row| {
                Ok(PollAction::CancelAttempt {
                    run_id: row.get(0)?,
                    attempt_no: row.get(1)?,
                    spec_hash: row.get(2)?,
                })
            },
        )
        .optional()?)
}

fn mark_redelivery(transaction: &Transaction<'_>, action: &PollAction, now: i64) -> Result<(), StoreError> {
    let PollAction::StartAttempt { run_id, attempt_no, .. } = action else {
        unreachable!("StartAttempt query returned another action")
    };
    transaction.execute(
        "UPDATE attempts SET dispatch_count=dispatch_count+1,last_dispatch_at_ms=?3
         WHERE run_id=?1 AND attempt_no=?2",
        params![run_id, attempt_no, now],
    )?;
    Ok(())
}

fn node_has_capacity(transaction: &Transaction<'_>, node: &str) -> Result<bool, StoreError> {
    Ok(transaction.query_row(
        "SELECT COALESCE((SELECT active_runs < max_concurrent_runs FROM nodes WHERE name=?1),1)",
        [node],
        |row| row.get(0),
    )?)
}

fn generation_config(transaction: &Transaction<'_>, mirror: &str, generation: u64) -> Result<String, StoreError> {
    Ok(transaction.query_row(
        "SELECT config_toml FROM mirror_generations WHERE mirror_name=?1 AND generation=?2",
        params![mirror, generation],
        |row| row.get(0),
    )?)
}

fn rearm_schedule(transaction: &Transaction<'_>, mirror: &str, next_due: i64, now: i64) -> Result<(), StoreError> {
    transaction.execute(
        "UPDATE mirror_schedule_state SET next_due_at_ms=?2,last_evaluated_at_ms=?3,
           catch_up_pending=0,catch_up_since_ms=NULL WHERE mirror_name=?1",
        params![mirror, next_due, now],
    )?;
    Ok(())
}

fn terminal_source(
    transaction: &Transaction<'_>,
    run_id: &str,
    attempt_no: u32,
    outcome: AttemptState,
) -> Result<TerminalDecisionSource, StoreError> {
    Ok(transaction.query_row(
        "SELECT r.max_attempts,r.retry_delay_ms,r.cancel_requested_at_ms IS NOT NULL,
           m.managed=1 AND m.enabled=1,m.owner_node=r.owner_node,m.name,g.config_toml
         FROM runs r JOIN mirrors m ON m.name=r.mirror_name
         JOIN mirror_generations g ON g.mirror_name=m.name AND g.generation=m.current_generation
         WHERE r.id=?1",
        [run_id],
        |row| {
            Ok(TerminalDecisionSource {
                outcome,
                attempt_no,
                max_attempts: row.get(0)?,
                retry_delay_ms: row.get(1)?,
                cancel_requested: row.get(2)?,
                mirror_eligible: row.get(3)?,
                owner_unchanged: row.get(4)?,
                mirror_name: row.get(5)?,
                current_config_toml: row.get(6)?,
            })
        },
    )?)
}

fn materialize_scheduled_candidate(
    transaction: &Transaction<'_>,
    node: &str,
    now: i64,
) -> Result<Option<(String, String, u64, u32)>, StoreError> {
    let due: Option<(String, u64, String, i64)> = transaction
        .query_row(
            "SELECT m.name,m.current_generation,m.owner_node,s.catch_up_since_ms
             FROM mirror_schedule_state s JOIN mirrors m ON m.name=s.mirror_name
             WHERE m.owner_node=?1 AND m.managed=1 AND m.enabled=1 AND s.catch_up_pending=1
               AND s.catch_up_since_ms IS NOT NULL
               AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.mirror_name=m.name AND r.state IN('pending','running'))
             ORDER BY s.catch_up_since_ms,m.name LIMIT 1",
            [node],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((mirror, generation, owner, scheduled_for)) = due else {
        return Ok(None);
    };
    let run_id = RunId::new().to_string();
    transaction.execute(
        "INSERT INTO runs(id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,
           max_attempts,retry_delay_ms,scheduled_for_at_ms)
         VALUES(?1,?2,?3,?4,'scheduled','pending',?5,1,0,?6)",
        params![run_id, mirror, generation, owner, now, scheduled_for],
    )?;
    transaction.execute(
        "UPDATE mirror_schedule_state SET catch_up_pending=0,catch_up_since_ms=NULL WHERE mirror_name=?1",
        [&mirror],
    )?;
    Ok(Some((run_id, mirror, generation, 1)))
}

fn finalize_ineligible_retries(transaction: &Transaction<'_>, mirror: &str, now: i64) -> Result<(), StoreError> {
    let waiting = {
        let mut statement = transaction.prepare(
            "SELECT r.id,a.state,a.exit_code,a.failure_kind,a.failure_message
             FROM runs r JOIN attempts a ON a.run_id=r.id AND a.attempt_no=r.attempt_count
             WHERE r.mirror_name=?1 AND r.state='running' AND r.retry_due_at_ms IS NOT NULL",
        )?;
        statement
            .query_map([mirror], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i32>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (run_id, attempt_state, exit_code, failure_kind, failure_message) in waiting {
        let state = if attempt_state == "timed_out" {
            "timed_out"
        } else {
            "failed"
        };
        transaction.execute(
            "UPDATE runs SET state=?2,finished_at_ms=?3,retry_due_at_ms=NULL,final_exit_code=?4,
               failure_kind=?5,failure_message=?6 WHERE id=?1",
            params![run_id, state, now, exit_code, failure_kind, failure_message],
        )?;
    }
    Ok(())
}

fn find_run_by_request(transaction: &Transaction<'_>, request_id: &str) -> Result<Option<RunRecord>, StoreError> {
    Ok(transaction.query_row(
        "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,finished_at_ms,
         final_exit_code,failure_kind,failure_message,max_attempts,retry_delay_ms,scheduled_for_at_ms,
         retry_due_at_ms,cancel_requested_at_ms FROM runs WHERE manual_request_id=?1",
        [request_id], map_run,
    ).optional()?)
}

fn map_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    let state: String = row.get(5)?;
    let trigger: String = row.get(4)?;
    Ok(RunRecord {
        id: row.get(0)?,
        mirror_name: row.get(1)?,
        mirror_generation: row.get(2)?,
        owner_node: row.get(3)?,
        trigger: parse_run_trigger(&trigger)?,
        state: parse_run_state(&state)?,
        created_at_ms: row.get(6)?,
        started_at_ms: row.get(7)?,
        finished_at_ms: row.get(8)?,
        final_exit_code: row.get(9)?,
        failure_kind: row.get(10)?,
        failure_message: row.get(11)?,
        max_attempts: row.get(12)?,
        retry_delay_ms: row.get(13)?,
        scheduled_for_at_ms: row.get(14)?,
        retry_due_at_ms: row.get(15)?,
        cancel_requested_at_ms: row.get(16)?,
    })
}

fn map_poll_action(row: &rusqlite::Row<'_>) -> rusqlite::Result<PollAction> {
    let spec_json: String = row.get(3)?;
    Ok(PollAction::StartAttempt {
        run_id: row.get(0)?,
        attempt_no: row.get(1)?,
        spec_hash: row.get(2)?,
        spec: serde_json::from_str(&spec_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(spec_json.len(), rusqlite::types::Type::Text, Box::new(error))
        })?,
    })
}

fn token_hash(token: &str) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(token.as_bytes())))
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn attempt_state_str(state: AttemptState) -> &'static str {
    match state {
        AttemptState::Queued => "queued",
        AttemptState::Accepted => "accepted",
        AttemptState::Running => "running",
        AttemptState::Succeeded => "succeeded",
        AttemptState::Failed => "failed",
        AttemptState::TimedOut => "timed_out",
        AttemptState::Cancelled => "cancelled",
        AttemptState::Interrupted => "interrupted",
        AttemptState::Rejected => "rejected",
    }
}

fn parse_attempt_state(value: &str) -> rusqlite::Result<AttemptState> {
    match value {
        "queued" => Ok(AttemptState::Queued),
        "accepted" => Ok(AttemptState::Accepted),
        "running" => Ok(AttemptState::Running),
        "succeeded" => Ok(AttemptState::Succeeded),
        "failed" => Ok(AttemptState::Failed),
        "timed_out" => Ok(AttemptState::TimedOut),
        "cancelled" => Ok(AttemptState::Cancelled),
        "interrupted" => Ok(AttemptState::Interrupted),
        "rejected" => Ok(AttemptState::Rejected),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn run_state_str(state: RunState) -> &'static str {
    match state {
        RunState::Pending => "pending",
        RunState::Running => "running",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Cancelled => "cancelled",
        RunState::TimedOut => "timed_out",
    }
}

fn parse_run_state(value: &str) -> rusqlite::Result<RunState> {
    match value {
        "pending" => Ok(RunState::Pending),
        "running" => Ok(RunState::Running),
        "succeeded" => Ok(RunState::Succeeded),
        "failed" => Ok(RunState::Failed),
        "cancelled" => Ok(RunState::Cancelled),
        "timed_out" => Ok(RunState::TimedOut),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn parse_run_trigger(value: &str) -> rusqlite::Result<lmt_core::RunTrigger> {
    match value {
        "manual" => Ok(lmt_core::RunTrigger::Manual),
        "scheduled" => Ok(lmt_core::RunTrigger::Scheduled),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

const fn failure_kind_str(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Process => "process",
        FailureKind::Timeout => "timeout",
        FailureKind::Interrupted => "interrupted",
        FailureKind::Rejected => "rejected",
        FailureKind::InvalidResult => "invalid_result",
    }
}

const MIGRATIONS: &[(u32, &str)] = &[
    (1, include_str!("../migrations/0001_m1.sql")),
    (2, include_str!("../migrations/0002_m2.sql")),
    (3, include_str!("../migrations/0003_m3.sql")),
    (4, include_str!("../migrations/0004_m3_hardening.sql")),
];

fn configure_and_migrate(connection: &mut Connection, migration_time_ms: i64) -> Result<(), StoreError> {
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    migrate(connection, MIGRATIONS, migration_time_ms)
}

fn migrate(connection: &mut Connection, migrations: &[(u32, &str)], migration_time_ms: i64) -> Result<(), StoreError> {
    let has_migrations: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations')",
        [],
        |row| row.get(0),
    )?;
    let current = if has_migrations {
        connection.query_row("SELECT COALESCE(MAX(version),0) FROM schema_migrations", [], |row| {
            row.get::<_, u32>(0)
        })?
    } else {
        0
    };
    let supported = migrations.last().map_or(0, |(version, _)| *version);
    if current > supported {
        return Err(StoreError::FutureSchema {
            found: current,
            supported,
        });
    }
    for &(version, sql) in migrations.iter().filter(|(version, _)| *version > current) {
        let transaction = connection.transaction()?;
        transaction.execute_batch(sql)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version,applied_at_ms) VALUES(?1,?2)",
            params![version, migration_time_ms],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lmt_core::{BundleFile, ConfigBundle, canonicalize_bundle};

    fn bundle(program: &str) -> CanonicalBundle {
        canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: format!(
                    "[mirror]\nname='demo'\ntarget='demo'\n[sync]\ntype='command'\nprogram='{program}'\n"
                ),
            }],
        })
        .expect("valid")
    }

    #[tokio::test]
    async fn offline_restore_normalizes_execution_without_erasing_schedule_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("restored.db");
        drop(Store::open(&path).await.expect("initialize schema"));
        let connection = Connection::open(&path).expect("open seed database");
        connection
            .execute_batch(
                "INSERT INTO config_revisions(revision,bundle_hash,applied_at_ms,summary_json) VALUES(1,'h',1,'{}');
                 INSERT INTO nodes(name,registered_at_ms,agent_instance_id,agent_boot_id,active_runs,capabilities_json)
                   VALUES('node-a',1,'instance','boot',2,'{}');
                 INSERT INTO mirrors(name,managed,enabled,owner_node,current_generation) VALUES
                   ('undispatched',1,1,'node-a',1),('active',1,1,'node-a',1);
                 INSERT INTO mirror_generations(mirror_name,generation,config_revision,owner_node,config_hash,config_toml,created_at_ms) VALUES
                   ('undispatched',1,1,'node-a','h1','x',1),('active',1,1,'node-a','h2','x',1);
                 INSERT INTO mirror_schedule_state(mirror_name,next_due_at_ms,last_evaluated_at_ms,catch_up_pending,catch_up_since_ms)
                   VALUES('active',9000,8000,1,7000);
                 INSERT INTO runs(id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,max_attempts,retry_delay_ms,retry_due_at_ms) VALUES
                   ('run-undispatched','undispatched',1,'node-a','manual','pending',1,1,0,5000),
                   ('run-active','active',1,'node-a','scheduled','running',2,2,100,5000);
                 INSERT INTO attempts(run_id,attempt_no,state,spec_hash,spec_json,created_at_ms,dispatch_count)
                   VALUES('run-active',1,'running','spec','{}',2,1);",
            )
            .expect("seed recovery state");
        drop(connection);

        normalize_restored_database(&path, 10_000).expect("normalize restored database");
        let connection = Connection::open(&path).expect("inspect");
        let pending: (String, Option<i64>, Option<String>) = connection
            .query_row(
                "SELECT state,retry_due_at_ms,failure_message FROM runs WHERE id='run-undispatched'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("undispatched");
        assert_eq!(pending.0, "cancelled");
        assert_eq!(pending.1, None);
        assert!(pending.2.expect("message").contains("restore"));
        let active: (String, String) = connection
            .query_row("SELECT state,failure_kind FROM runs WHERE id='run-active'", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .expect("active Run");
        assert_eq!(active, ("failed".into(), "interrupted".into()));
        assert_eq!(
            connection
                .query_row("SELECT state FROM attempts WHERE run_id='run-active'", [], |row| row
                    .get::<_, String>(
                    0
                ))
                .expect("Attempt"),
            "interrupted"
        );
        let node: (u32, Option<String>, Option<String>) = connection
            .query_row(
                "SELECT active_runs,agent_instance_id,agent_boot_id FROM nodes WHERE name='node-a'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("Node");
        assert_eq!(node, (0, None, None));
        assert_eq!(
            connection
                .query_row(
                    "SELECT next_due_at_ms FROM mirror_schedule_state WHERE mirror_name='active'",
                    [],
                    |row| { row.get::<_, i64>(0) }
                )
                .expect("schedule state"),
            9000
        );
    }

    fn scheduled_bundle(program: &str, interval: &str, max_attempts: u32) -> CanonicalBundle {
        canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: format!(
                    "[mirror]\nname='demo'\ntarget='demo'\n[schedule]\ninterval='{interval}'\n[sync]\ntype='command'\nprogram='{program}'\n[run]\nmax_attempts={max_attempts}\nretry_delay_seconds=5\n"
                ),
            }],
        })
        .expect("valid")
    }

    fn cron_bundle(program: &str) -> CanonicalBundle {
        canonicalize_bundle(&ConfigBundle {
            files: vec![BundleFile {
                path: "nodes/node-a/mirrors/demo.toml".into(),
                contents: format!(
                    "[mirror]\nname='demo'\ntarget='demo'\n[schedule]\ncron='* * * * *'\ntimezone='UTC'\n[sync]\ntype='command'\nprogram='{program}'\n"
                ),
            }],
        })
        .expect("valid")
    }

    async fn poll(store: &Store) -> PollAction {
        poll_at(store, 20).await
    }

    async fn poll_at(store: &Store, now: i64) -> PollAction {
        store
            .poll_action("node-a", now, |_| {
                Ok((
                    ProcessRunSpec {
                        runner: "process".into(),
                        program: "/bin/true".into(),
                        args: vec![],
                        cwd: None,
                        timeout_seconds: 30,
                        mirror_root: "/tmp/mirrors".into(),
                        target_dir: "/tmp/mirrors/demo".into(),
                    },
                    "sha256:test".into(),
                    RunPolicySnapshot {
                        max_attempts: 3,
                        retry_delay_ms: 5_000,
                    },
                ))
            })
            .await
            .expect("poll")
            .expect("action")
    }

    fn start_fields(action: &PollAction) -> (&str, u32, &str) {
        match action {
            PollAction::StartAttempt {
                run_id,
                attempt_no,
                spec_hash,
                ..
            } => (run_id, *attempt_no, spec_hash),
            PollAction::CancelAttempt { .. } => panic!("expected StartAttempt"),
        }
    }

    fn policy(config: &str) -> Result<RunPolicySnapshot, StoreError> {
        let document: MirrorDocument =
            toml::from_str(config).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
        Ok(RunPolicySnapshot {
            max_attempts: document.run.max_attempts,
            retry_delay_ms: document.run.retry_delay_seconds * 1_000,
        })
    }

    fn decide(source: TerminalDecisionSource, now: i64) -> TerminalDecision {
        TerminalDecision {
            retry: lmt_core::decide_retry(lmt_core::RetryContext {
                outcome: source.outcome,
                attempt_no: source.attempt_no,
                max_attempts: source.max_attempts,
                retry_delay_seconds: source.retry_delay_ms / 1_000,
                cancel_requested: source.cancel_requested,
                mirror_eligible: source.mirror_eligible,
                owner_unchanged: source.owner_unchanged,
                server_now_ms: now,
            }),
            interval_next_due_at_ms: None,
        }
    }

    #[tokio::test]
    async fn migrations_and_restart_preserve_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("lmt.db");
        let store = Store::open(&path).await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        drop(store);
        assert_eq!(
            Store::open(path)
                .await
                .expect("reopen")
                .list_mirrors()
                .await
                .expect("list")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn populated_m1_database_upgrades_through_m3() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("m1.db");
        {
            let mut connection = Connection::open(&path).expect("open M1 database");
            connection
                .pragma_update(None, "foreign_keys", "ON")
                .expect("foreign keys");
            migrate(&mut connection, &MIGRATIONS[..1], 1).expect("M1 migration");
            connection
                .execute(
                    "INSERT INTO nodes(name,registered_at_ms,active_runs,capabilities_json) VALUES('node-a',1,0,'{}')",
                    [],
                )
                .expect("populate M1");
        }

        let store = Store::open(&path).await.expect("upgrade");
        let (version, node_count, capacity): (u32, u32, u32) = store
            .call(|connection| {
                Ok((
                    connection.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))?,
                    connection.query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?,
                    connection.query_row("SELECT max_concurrent_runs FROM nodes WHERE name='node-a'", [], |row| {
                        row.get(0)
                    })?,
                ))
            })
            .await
            .expect("query upgraded state");
        assert_eq!((version, node_count, capacity), (4, 1, 1));
    }

    #[tokio::test]
    async fn frozen_populated_v2_fixture_upgrades_through_m3_hardening_without_reconstruction() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("accepted-v2.db");
        Connection::open(&path)
            .expect("open fixture")
            .execute_batch(include_str!("../tests/fixtures/accepted_v2.sql"))
            .expect("load immutable v2 fixture");

        let store = Store::open(&path).await.expect("upgrade fixture");
        let migrated: (u32, Option<String>, Option<String>, Option<String>, Option<i64>) = store
            .call(|connection| {
                Ok((
                    connection.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))?,
                    connection.query_row("SELECT bound_agent_id FROM nodes WHERE name='node-a'", [], |row| {
                        row.get(0)
                    })?,
                    connection.query_row("SELECT agent_boot_id FROM nodes WHERE name='node-a'", [], |row| {
                        row.get(0)
                    })?,
                    connection.query_row(
                        "SELECT label FROM node_credentials WHERE node_name='node-a' AND credential_id='bootstrap'",
                        [],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT expired_at_ms FROM attempt_logs WHERE run_id='01K000000000000000000000V2' AND attempt_no=1",
                        [],
                        |row| row.get(0),
                    )?,
                ))
            })
            .await
            .expect("query v3 columns");
        assert_eq!(migrated, (4, None, None, None, None));
        assert_eq!(store.list_mirrors().await.expect("mirrors").len(), 1);
        assert_eq!(store.list_runs().await.expect("runs").len(), 1);
        assert_eq!(
            store
                .list_attempts("01K000000000000000000000V2")
                .await
                .expect("attempts")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn future_schema_version_is_refused() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("future.db");
        {
            let mut connection = Connection::open(&path).expect("open");
            migrate(&mut connection, MIGRATIONS, 1).expect("migrate");
            connection
                .execute("INSERT INTO schema_migrations(version,applied_at_ms) VALUES(99,2)", [])
                .expect("future marker");
        }
        assert!(matches!(
            Store::open(path).await,
            Err(StoreError::FutureSchema {
                found: 99,
                supported: 4
            })
        ));
    }

    #[test]
    fn failed_migration_rolls_back_schema_and_version() {
        let mut connection = Connection::open_in_memory().expect("open");
        migrate(&mut connection, &MIGRATIONS[..1], 1).expect("M1");
        let broken = [
            MIGRATIONS[0],
            (2, "CREATE TABLE must_rollback(value INTEGER) STRICT; INVALID SQL;"),
        ];
        assert!(migrate(&mut connection, &broken, 2).is_err());
        let table_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='must_rollback')",
                [],
                |row| row.get(0),
            )
            .expect("table query");
        let version: u32 = connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))
            .expect("version");
        assert!(!table_exists);
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn config_apply_is_atomic_and_semantic_noop_has_no_generation() {
        let store = Store::open_in_memory().await.expect("open");
        let first = bundle("/bin/true");
        store.apply(&first, 0, "test", 10).await.expect("apply");
        let plan = store.plan(&first).await.expect("plan");
        assert!(plan.changes.is_empty());
        let changed = bundle("/bin/false");
        let plan = store.plan(&changed).await.expect("plan");
        assert_eq!(plan.changes[0].to_generation, Some(2));
        assert!(matches!(
            store.apply(&changed, 0, "stale", 20).await,
            Err(StoreError::RevisionConflict { .. })
        ));
    }

    #[tokio::test]
    async fn schedule_state_is_coalesced_and_preserved_by_unrelated_updates() {
        let store = Store::open_in_memory().await.expect("open");
        let initial = scheduled_bundle("/bin/true", "1h", 3);
        store.apply(&initial, 0, "test", 1_000).await.expect("apply");
        let mirror = store.get_mirror("demo").await.expect("query").expect("mirror");
        assert_eq!(mirror.next_due_at_ms, Some(3_601_000));
        assert_eq!(store.earliest_wakeup().await.expect("wakeup"), Some(3_601_000));

        let evaluated = store
            .evaluate_due_schedules(10_000_000, |source| {
                let document: lmt_core::MirrorDocument = toml::from_str(&source.config_toml)
                    .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
                Ok(lmt_core::evaluate_schedule_due(
                    document.schedule.as_ref().expect("schedule"),
                    source.runtime,
                    10_000_000,
                    source.has_active_run,
                )
                .map_err(StoreError::InvalidConfig)?
                .runtime)
            })
            .await
            .expect("tick");
        assert_eq!(evaluated, 1);
        let due = store.get_mirror("demo").await.expect("query").expect("mirror");
        assert_eq!(due.scheduled_due_since_ms, Some(3_601_000));
        assert_eq!(due.next_due_at_ms, None);

        let unrelated = scheduled_bundle("/bin/false", "60m", 3);
        store.apply(&unrelated, 1, "test", 20_000_000).await.expect("update");
        let preserved = store.get_mirror("demo").await.expect("query").expect("mirror");
        assert_eq!(preserved.scheduled_due_since_ms, Some(3_601_000));
        assert_eq!(preserved.next_due_at_ms, None);
        let action = poll_at(&store, 20_000_001).await;
        let run = store
            .get_run(start_fields(&action).0)
            .await
            .expect("get")
            .expect("scheduled run");
        assert_eq!(run.mirror_generation, 2, "due intent used a stale generation");
        assert_eq!(run.scheduled_for_at_ms, Some(3_601_000));
    }

    #[tokio::test]
    async fn cron_misses_skip_while_busy_then_coalesce_offline_and_wait_at_capacity() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("cron.db");
        let store = Store::open(&database).await.expect("open");
        store
            .apply(&cron_bundle("/bin/true"), 0, "test", 0)
            .await
            .expect("apply");
        let active = store
            .create_manual_run("demo", "active", 10, policy)
            .await
            .expect("active run");
        store
            .evaluate_due_schedules(600_000, |source| {
                let document: MirrorDocument = toml::from_str(&source.config_toml)
                    .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
                Ok(lmt_core::evaluate_schedule_due(
                    document.schedule.as_ref().expect("schedule"),
                    source.runtime,
                    600_000,
                    source.has_active_run,
                )
                .expect("evaluate")
                .runtime)
            })
            .await
            .expect("busy tick");
        let skipped = store.get_mirror("demo").await.expect("get").expect("mirror");
        assert_eq!(skipped.scheduled_due_since_ms, None);
        assert_eq!(skipped.next_due_at_ms, Some(660_000));
        store
            .request_cancellation(&active.id, 610_000, |_| Ok(None))
            .await
            .expect("clear active");

        for now in [1_200_000, 1_800_000] {
            store
                .evaluate_due_schedules(now, move |source| {
                    let document: MirrorDocument = toml::from_str(&source.config_toml)
                        .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
                    Ok(lmt_core::evaluate_schedule_due(
                        document.schedule.as_ref().expect("schedule"),
                        source.runtime,
                        now,
                        source.has_active_run,
                    )
                    .expect("evaluate")
                    .runtime)
                })
                .await
                .expect("offline tick");
        }
        let coalesced = store.get_mirror("demo").await.expect("get").expect("mirror");
        assert_eq!(coalesced.scheduled_due_since_ms, Some(660_000));
        drop(store);

        let store = Store::open(&database).await.expect("restart");
        assert_eq!(
            store
                .get_mirror("demo")
                .await
                .expect("get")
                .expect("mirror")
                .scheduled_due_since_ms,
            Some(660_000)
        );
        store
            .upsert_credential("node-a", "secret", 1_800_001)
            .await
            .expect("node");
        store
            .observe_node(NodeObservation {
                node: "node-a".into(),
                agent_version: "test".into(),
                agent_instance_id: "instance".into(),
                agent_boot_id: "boot".into(),
                active_runs: 1,
                max_concurrent_runs: 1,
                mirror_root_free_bytes: None,
                mirror_root: "/tmp/mirrors".into(),
                observed_at_ms: 1_800_001,
            })
            .await
            .expect("full");
        assert!(poll_optional(&store, 1_800_002).await.is_none());
        assert!(
            store
                .list_runs()
                .await
                .expect("runs")
                .iter()
                .all(|run| run.id == active.id)
        );
        assert_eq!(
            store
                .get_mirror("demo")
                .await
                .expect("get")
                .expect("mirror")
                .scheduled_due_since_ms,
            Some(660_000)
        );
    }

    #[tokio::test]
    async fn due_marker_materializes_one_scheduled_run_and_interval_rearms_on_terminal() {
        let store = Store::open_in_memory().await.expect("open");
        store
            .apply(&scheduled_bundle("/bin/true", "1h", 3), 0, "test", 0)
            .await
            .expect("apply");
        store
            .evaluate_due_schedules(3_600_000, |source| {
                let document: MirrorDocument = toml::from_str(&source.config_toml)
                    .map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
                Ok(lmt_core::evaluate_schedule_due(
                    document.schedule.as_ref().expect("schedule"),
                    source.runtime,
                    3_600_000,
                    source.has_active_run,
                )
                .expect("evaluate")
                .runtime)
            })
            .await
            .expect("tick");
        let due = store.get_mirror("demo").await.expect("get").expect("mirror");
        assert_eq!(due.next_due_at_ms, None);
        assert_eq!(due.scheduled_due_since_ms, Some(3_600_000));
        assert!(store.list_runs().await.expect("runs").is_empty());

        let action = poll_at(&store, 3_600_100).await;
        let (run_id, attempt_no, _) = start_fields(&action);
        assert_eq!(attempt_no, 1);
        let run = store.get_run(run_id).await.expect("get").expect("run");
        assert_eq!(run.trigger, lmt_core::RunTrigger::Scheduled);
        assert_eq!(run.scheduled_for_at_ms, Some(3_600_000));
        assert_eq!(run.max_attempts, 3);
        assert_eq!(
            store
                .get_mirror("demo")
                .await
                .expect("get")
                .expect("mirror")
                .scheduled_due_since_ms,
            None
        );

        store
            .apply_event(
                &run.id,
                1,
                &terminal_event(AttemptState::Succeeded, 3),
                4_000_000,
                |source, now| {
                    Ok(TerminalDecision {
                        retry: lmt_core::decide_retry(lmt_core::RetryContext {
                            outcome: source.outcome,
                            attempt_no: source.attempt_no,
                            max_attempts: source.max_attempts,
                            retry_delay_seconds: source.retry_delay_ms / 1_000,
                            cancel_requested: source.cancel_requested,
                            mirror_eligible: source.mirror_eligible,
                            owner_unchanged: source.owner_unchanged,
                            server_now_ms: now,
                        }),
                        interval_next_due_at_ms: Some(now + 3_600_000),
                    })
                },
            )
            .await
            .expect("terminal");
        assert_eq!(
            store
                .get_mirror("demo")
                .await
                .expect("get")
                .expect("mirror")
                .next_due_at_ms,
            Some(7_600_000)
        );
    }

    #[tokio::test]
    async fn manual_run_snapshots_generation_policy_and_clears_due_intent() {
        let store = Store::open_in_memory().await.expect("open");
        store
            .apply(&scheduled_bundle("/bin/true", "1h", 3), 0, "test", 0)
            .await
            .expect("apply");
        store
            .evaluate_due_schedules(3_600_000, |source| {
                Ok(ScheduleRuntime {
                    next_due_at_ms: None,
                    last_evaluated_at_ms: Some(3_600_000),
                    catch_up_pending: true,
                    catch_up_since_ms: source.runtime.next_due_at_ms,
                })
            })
            .await
            .expect("due");
        let run = store
            .create_manual_run("demo", "request", 4_000_000, |config| {
                let document: lmt_core::MirrorDocument =
                    toml::from_str(config).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
                Ok(RunPolicySnapshot {
                    max_attempts: document.run.max_attempts,
                    retry_delay_ms: document.run.retry_delay_seconds * 1_000,
                })
            })
            .await
            .expect("run");
        assert_eq!(run.max_attempts, 3);
        assert_eq!(run.retry_delay_ms, 5_000);
        assert_eq!(
            store
                .get_mirror("demo")
                .await
                .expect("query")
                .expect("mirror")
                .scheduled_due_since_ms,
            None
        );
    }

    #[tokio::test]
    async fn manual_request_and_active_run_are_idempotent() {
        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        let first = store
            .create_manual_run("demo", "request-1", 20, policy)
            .await
            .expect("run");
        assert_eq!(
            store
                .create_manual_run("demo", "request-1", 30, policy)
                .await
                .expect("same")
                .id,
            first.id
        );
        assert!(matches!(
            store.create_manual_run("demo", "request-2", 40, policy).await,
            Err(StoreError::MirrorBusy { .. })
        ));
    }

    #[tokio::test]
    async fn bounded_run_keyset_pagination_has_no_duplicates_or_skips() {
        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        for index in 0..6 {
            let run = store
                .create_manual_run("demo", &format!("page-{index}"), 100 + i64::from(index / 2), policy)
                .await
                .expect("run");
            poll_at(&store, 200 + i64::from(index)).await;
            store
                .apply_event(
                    &run.id,
                    1,
                    &terminal_event(AttemptState::Succeeded, 1),
                    300 + i64::from(index),
                    |source, now| Ok(decide(source, now)),
                )
                .await
                .expect("terminal");
        }
        let expected = store
            .query_runs(RunQuery {
                limit: 6,
                ..RunQuery::default()
            })
            .await
            .expect("all")
            .into_iter()
            .map(|run| run.id)
            .collect::<Vec<_>>();
        let mut actual = Vec::new();
        let mut before = None;
        loop {
            let page = store
                .query_runs(RunQuery {
                    limit: 2,
                    before,
                    ..RunQuery::default()
                })
                .await
                .expect("page");
            if page.is_empty() {
                break;
            }
            before = page.last().map(|run| run.id.clone());
            actual.extend(page.into_iter().map(|run| run.id));
        }
        assert_eq!(actual, expected);
        assert!(matches!(
            store
                .query_runs(RunQuery {
                    limit: 501,
                    ..RunQuery::default()
                })
                .await,
            Err(StoreError::InvalidConfig(_))
        ));
    }

    #[tokio::test]
    async fn duplicate_terminal_event_cannot_regress() {
        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        let run = store
            .create_manual_run("demo", "request", 20, policy)
            .await
            .expect("run");
        poll(&store).await;
        let terminal = AttemptEvent {
            event_sequence: 3,
            state: AttemptState::Succeeded,
            agent_instance_id: "agent-1".into(),
            accepted_at_ms: Some(1),
            started_at_ms: Some(2),
            finished_at_ms: Some(3),
            exit_code: Some(0),
            failure_kind: None,
            failure_message: None,
        };
        assert_eq!(
            store
                .apply_event(&run.id, 1, &terminal, 30, |source, now| Ok(decide(source, now)))
                .await
                .expect("event"),
            AttemptEventApplyResult {
                accepted_event_sequence: 3,
                newly_applied: true,
                retry_scheduled: false,
            }
        );
        let late = AttemptEvent {
            event_sequence: 2,
            state: AttemptState::Running,
            ..terminal
        };
        assert_eq!(
            store
                .apply_event(&run.id, 1, &late, 40, |source, now| Ok(decide(source, now)))
                .await
                .expect("duplicate"),
            AttemptEventApplyResult {
                accepted_event_sequence: 3,
                newly_applied: false,
                retry_scheduled: false,
            }
        );
        assert_eq!(
            store.get_run(&run.id).await.expect("get").expect("run").state,
            RunState::Succeeded
        );
    }

    fn terminal_event(state: AttemptState, sequence: u64) -> AttemptEvent {
        AttemptEvent {
            event_sequence: sequence,
            state,
            agent_instance_id: "agent-1".into(),
            accepted_at_ms: Some(1),
            started_at_ms: Some(2),
            finished_at_ms: Some(3),
            exit_code: (state == AttemptState::Succeeded).then_some(0).or(Some(1)),
            failure_kind: None,
            failure_message: None,
        }
    }

    #[tokio::test]
    async fn retryable_outcomes_create_later_attempt_in_the_same_run() {
        for outcome in [AttemptState::Failed, AttemptState::TimedOut, AttemptState::Interrupted] {
            let store = Store::open_in_memory().await.expect("open");
            store
                .apply(&scheduled_bundle("/bin/true", "1h", 3), 0, "test", 0)
                .await
                .expect("apply");
            let run = store
                .create_manual_run("demo", &format!("request-{outcome:?}"), 10, policy)
                .await
                .expect("run");
            assert_eq!(start_fields(&poll(&store).await).1, 1);
            store
                .apply_event(&run.id, 1, &terminal_event(outcome, 3), 100, |source, now| {
                    Ok(decide(source, now))
                })
                .await
                .expect("terminal");
            let waiting = store.get_run(&run.id).await.expect("query").expect("run");
            assert_eq!(waiting.state, RunState::Running);
            assert_eq!(waiting.retry_due_at_ms, Some(5_100));
            assert!(
                store
                    .poll_action("node-a", 5_099, |_| unreachable!("retry not due"))
                    .await
                    .expect("poll")
                    .is_none()
            );
            assert_eq!(start_fields(&poll_at(&store, 5_100).await).1, 2);
            store
                .apply_event(
                    &run.id,
                    2,
                    &terminal_event(AttemptState::Succeeded, 3),
                    6_000,
                    |source, now| Ok(decide(source, now)),
                )
                .await
                .expect("success");
            let complete = store.get_run(&run.id).await.expect("query").expect("run");
            assert_eq!(complete.state, RunState::Succeeded);
            assert_eq!(complete.id, run.id);
            assert_eq!(store.list_attempts(&run.id).await.expect("attempts").len(), 2);
        }
    }

    #[tokio::test]
    async fn retry_deadline_survives_restart_and_config_removal_suppresses_it() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("retry.db");
        let store = Store::open(&path).await.expect("open");
        store
            .apply(&scheduled_bundle("/bin/true", "1h", 3), 0, "test", 0)
            .await
            .expect("apply");
        let run = store
            .create_manual_run("demo", "request", 10, policy)
            .await
            .expect("run");
        poll(&store).await;
        store
            .apply_event(
                &run.id,
                1,
                &terminal_event(AttemptState::Failed, 3),
                100,
                |source, now| Ok(decide(source, now)),
            )
            .await
            .expect("failed");
        drop(store);

        let store = Store::open(&path).await.expect("reopen");
        assert_eq!(store.earliest_wakeup().await.expect("wakeup"), Some(5_100));
        let empty = canonicalize_bundle(&ConfigBundle { files: vec![] }).expect("empty");
        store.apply(&empty, 1, "remove", 200).await.expect("remove");
        let final_run = store.get_run(&run.id).await.expect("query").expect("run");
        assert_eq!(final_run.state, RunState::Failed);
        assert_eq!(final_run.retry_due_at_ms, None);
        assert!(poll_optional(&store, 6_000).await.is_none());
    }

    async fn poll_optional(store: &Store, now: i64) -> Option<PollAction> {
        store
            .poll_action("node-a", now, |_| unreachable!("no dispatch expected"))
            .await
            .expect("poll")
    }

    #[tokio::test]
    async fn rejected_attempt_never_retries() {
        let store = Store::open_in_memory().await.expect("open");
        store
            .apply(&scheduled_bundle("/bin/true", "1h", 3), 0, "test", 0)
            .await
            .expect("apply");
        let run = store
            .create_manual_run("demo", "rejected", 10, policy)
            .await
            .expect("run");
        poll(&store).await;
        store
            .apply_event(
                &run.id,
                1,
                &terminal_event(AttemptState::Rejected, 1),
                100,
                |source, now| Ok(decide(source, now)),
            )
            .await
            .expect("rejected");
        let run = store.get_run(&run.id).await.expect("query").expect("run");
        assert_eq!(run.state, RunState::Failed);
        assert_eq!(run.retry_due_at_ms, None);
    }

    #[tokio::test]
    async fn cancellation_is_idempotent_immediate_before_dispatch_and_during_retry_delay() {
        let store = Store::open_in_memory().await.expect("open");
        store
            .apply(&scheduled_bundle("/bin/true", "1h", 3), 0, "test", 0)
            .await
            .expect("apply");
        let pending = store
            .create_manual_run("demo", "pending-cancel", 10, policy)
            .await
            .expect("run");
        let cancelled = store
            .request_cancellation(&pending.id, 20, |_| Ok(None))
            .await
            .expect("cancel");
        assert_eq!(cancelled.run.state, RunState::Cancelled);
        assert_eq!(cancelled.run.cancel_requested_at_ms, Some(20));
        assert!(cancelled.newly_requested);
        assert_eq!(
            store
                .request_cancellation(&pending.id, 30, |_| Ok(None))
                .await
                .expect("duplicate")
                .run
                .cancel_requested_at_ms,
            Some(20)
        );

        let retrying = store
            .create_manual_run("demo", "retry-cancel", 40, policy)
            .await
            .expect("run");
        poll_at(&store, 50).await;
        store
            .apply_event(
                &retrying.id,
                1,
                &terminal_event(AttemptState::Failed, 3),
                100,
                |source, now| Ok(decide(source, now)),
            )
            .await
            .expect("failed");
        let cancelled = store
            .request_cancellation(&retrying.id, 200, |_| Ok(None))
            .await
            .expect("cancel retry");
        assert_eq!(cancelled.run.state, RunState::Cancelled);
        assert_eq!(cancelled.run.retry_due_at_ms, None);
        assert!(poll_optional(&store, 10_000).await.is_none());
    }

    #[tokio::test]
    async fn cancellation_of_terminal_runs_does_not_mutate_history() {
        for outcome in [
            AttemptState::Succeeded,
            AttemptState::Failed,
            AttemptState::TimedOut,
            AttemptState::Cancelled,
        ] {
            let store = Store::open_in_memory().await.expect("open");
            store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
            let run = store
                .create_manual_run("demo", &format!("terminal-{outcome:?}"), 20, policy)
                .await
                .expect("run");
            poll_at(&store, 30).await;
            store
                .apply_event(&run.id, 1, &terminal_event(outcome, 1), 40, |source, now| {
                    Ok(decide(source, now))
                })
                .await
                .expect("terminal event");
            let before = store.get_run(&run.id).await.expect("get").expect("run");

            let result = store
                .request_cancellation(&run.id, 50, |_| Ok(Some(999)))
                .await
                .expect("terminal cancellation");

            assert!(!result.newly_requested, "{outcome:?}");
            assert_eq!(result.run, before, "{outcome:?}");
            assert_eq!(store.get_run(&run.id).await.expect("get"), Some(before), "{outcome:?}");
        }
    }

    #[tokio::test]
    async fn dispatched_cancellation_has_priority_and_repeats_until_terminal() {
        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        let first_run = store.create_manual_run("demo", "first", 20, policy).await.expect("run");
        let start = poll_at(&store, 30).await;
        let (_, _, hash) = start_fields(&start);
        let cancelled = store
            .request_cancellation(&first_run.id, 40, |_| Ok(None))
            .await
            .expect("cancel");
        assert_eq!(cancelled.run.state, RunState::Pending);

        for now in [41, 42] {
            match poll_at(&store, now).await {
                PollAction::CancelAttempt {
                    run_id,
                    attempt_no,
                    spec_hash,
                } => {
                    assert_eq!(run_id, first_run.id);
                    assert_eq!(attempt_no, 1);
                    assert_eq!(spec_hash, hash);
                }
                PollAction::StartAttempt { .. } => panic!("cancel must outrank Start redelivery"),
            }
        }
        store
            .apply_event(
                &first_run.id,
                1,
                &terminal_event(AttemptState::Cancelled, 1),
                50,
                |source, now| Ok(decide(source, now)),
            )
            .await
            .expect("cancelled event");
        assert_eq!(
            store.get_run(&first_run.id).await.expect("get").expect("run").state,
            RunState::Cancelled
        );
        assert!(poll_optional(&store, 60).await.is_none());
    }

    #[tokio::test]
    async fn full_agent_blocks_new_start_but_not_cancellation() {
        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        store.upsert_credential("node-a", "secret", 10).await.expect("node");
        let observe = |active_runs| NodeObservation {
            node: "node-a".into(),
            agent_version: "test".into(),
            agent_instance_id: "instance".into(),
            agent_boot_id: "boot".into(),
            active_runs,
            max_concurrent_runs: 1,
            mirror_root_free_bytes: None,
            mirror_root: "/tmp/mirrors".into(),
            observed_at_ms: 20,
        };
        store.observe_node(observe(1)).await.expect("full");
        let run = store
            .create_manual_run("demo", "capacity", 30, policy)
            .await
            .expect("run");
        assert!(poll_optional(&store, 40).await.is_none());

        store.observe_node(observe(0)).await.expect("free");
        assert!(matches!(poll_at(&store, 50).await, PollAction::StartAttempt { .. }));
        store.observe_node(observe(1)).await.expect("full again");
        store
            .request_cancellation(&run.id, 60, |_| Ok(None))
            .await
            .expect("cancel");
        assert!(matches!(poll_at(&store, 61).await, PollAction::CancelAttempt { .. }));
    }

    #[tokio::test]
    async fn durable_agent_binding_fences_conflicts_and_replacement_is_safety_gated() {
        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        store.upsert_credential("node-a", "secret", 10).await.expect("node");
        let observation = |agent: &str, boot: &str, now| NodeObservation {
            node: "node-a".into(),
            agent_version: "test".into(),
            agent_instance_id: agent.into(),
            agent_boot_id: boot.into(),
            active_runs: 0,
            max_concurrent_runs: 1,
            mirror_root_free_bytes: None,
            mirror_root: "/tmp/mirrors".into(),
            observed_at_ms: now,
        };
        store
            .observe_node(observation("installation-a", "boot-a", 20))
            .await
            .expect("first bind");
        assert!(matches!(
            store.observe_node(observation("installation-b", "boot-b", 30)).await,
            Err(StoreError::AgentBindingConflict { .. })
        ));
        let bound = store.list_nodes().await.expect("nodes").pop().expect("node");
        assert_eq!(bound.bound_agent_id.as_deref(), Some("installation-a"));
        assert_eq!(bound.agent_boot_id.as_deref(), Some("boot-a"));
        assert_eq!(bound.last_seen_at_ms, Some(20));

        let run = store
            .create_manual_run("demo", "binding", 40, policy)
            .await
            .expect("run");
        assert!(matches!(poll_at(&store, 50).await, PollAction::StartAttempt { .. }));
        assert!(matches!(
            store.replace_agent_binding("node-a", "installation-b", false).await,
            Err(StoreError::BindingReplacementUnsafe)
        ));
        store
            .replace_agent_binding("node-a", "installation-b", true)
            .await
            .expect("acknowledged replacement");
        assert_eq!(
            store
                .list_nodes()
                .await
                .expect("nodes")
                .pop()
                .expect("node")
                .bound_agent_id
                .as_deref(),
            Some("installation-b")
        );
        assert_eq!(
            store.get_run(&run.id).await.expect("run").expect("run").state,
            RunState::Pending
        );
    }

    #[tokio::test]
    async fn credential_history_prevents_legacy_resurrection_and_usage_is_throttled() {
        let store = Store::open_in_memory().await.expect("open");
        assert!(
            store
                .import_legacy_credential("node-a", "legacy-secret", 10)
                .await
                .expect("import")
        );
        assert_eq!(
            store
                .authenticate_credential("legacy-secret")
                .await
                .expect("authenticate")
                .expect("credential")
                .credential_id,
            "bootstrap"
        );
        let revoked = store
            .revoke_credential("node-a", "bootstrap", 20)
            .await
            .expect("revoke");
        assert_eq!(revoked.revoked_at_ms, Some(20));
        assert_eq!(
            store
                .revoke_credential("node-a", "bootstrap", 30)
                .await
                .expect("idempotent")
                .revoked_at_ms,
            Some(20)
        );
        assert!(
            !store
                .import_legacy_credential("node-a", "legacy-secret", 40)
                .await
                .expect("stale import")
        );
        assert!(
            store
                .authenticate_credential("legacy-secret")
                .await
                .expect("revoked authentication")
                .is_none()
        );

        let issued = store
            .issue_credential("node-a", "new-id", Some("rotation"), "new-secret", 50)
            .await
            .expect("issue");
        assert_eq!(issued.label.as_deref(), Some("rotation"));
        let stored_hash: String = store
            .call(|connection| {
                Ok(connection.query_row(
                    "SELECT token_hash FROM node_credentials WHERE credential_id='new-id'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .expect("stored digest");
        assert_ne!(stored_hash, "new-secret");
        assert!(!stored_hash.contains("new-secret"));
        assert!(
            store
                .mark_credential_used("node-a", "new-id", 60)
                .await
                .expect("first use")
        );
        assert!(
            !store
                .mark_credential_used("node-a", "new-id", 61)
                .await
                .expect("throttled")
        );
        assert!(
            store
                .mark_credential_used("node-a", "new-id", 60_060)
                .await
                .expect("later use")
        );
        assert_eq!(
            store.list_credentials("node-a").await.expect("list")[0].last_used_at_ms,
            Some(60_060)
        );
    }

    #[tokio::test]
    async fn removal_cancels_only_never_dispatched_pending_work() {
        let empty = canonicalize_bundle(&ConfigBundle { files: vec![] }).expect("empty bundle");

        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        let undispatched = store
            .create_manual_run("demo", "request-1", 20, policy)
            .await
            .expect("run");
        store.apply(&empty, 1, "remove", 30).await.expect("remove");
        assert_eq!(
            store.get_run(&undispatched.id).await.expect("get").expect("run").state,
            RunState::Cancelled
        );

        let store = Store::open_in_memory().await.expect("open");
        store.apply(&bundle("/bin/true"), 0, "test", 10).await.expect("apply");
        let dispatched = store
            .create_manual_run("demo", "request-2", 20, policy)
            .await
            .expect("run");
        let first = poll(&store).await;
        store.apply(&empty, 1, "remove", 30).await.expect("remove");
        assert_eq!(
            store.get_run(&dispatched.id).await.expect("get").expect("run").state,
            RunState::Pending
        );
        let redelivered = poll(&store).await;
        assert_eq!(start_fields(&redelivered).0, start_fields(&first).0);
        assert_eq!(start_fields(&redelivered).2, start_fields(&first).2);
    }
}
