//! Central SQLite persistence. This is the only library crate that knows the schema.

use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use lmt_core::{
    AttemptEvent, AttemptState, CanonicalBundle, FailureKind, MirrorDocument, ProcessRunSpec, RunId, RunState,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("database lock is poisoned")]
    Poisoned,
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
}

#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub name: String,
    pub agent_version: Option<String>,
    pub agent_instance_id: Option<String>,
    pub last_seen_at_ms: Option<i64>,
    pub active_runs: u32,
    pub mirror_root_free_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RunRecord {
    pub id: String,
    pub mirror_name: String,
    pub mirror_generation: u64,
    pub owner_node: String,
    pub trigger: String,
    pub state: RunState,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub final_exit_code: Option<i32>,
    pub failure_kind: Option<String>,
    pub failure_message: Option<String>,
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
pub struct PollAction {
    pub run_id: String,
    pub attempt_no: u32,
    pub spec_hash: String,
    pub spec: ProcessRunSpec,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }

    pub fn current_revision(&self) -> Result<u64, StoreError> {
        Ok(self
            .connection()?
            .query_row("SELECT COALESCE(MAX(revision), 0) FROM config_revisions", [], |row| {
                row.get(0)
            })?)
    }

    pub fn plan(&self, bundle: &CanonicalBundle) -> Result<ConfigPlan, StoreError> {
        let connection = self.connection()?;
        plan_with_connection(&connection, bundle)
    }

    pub fn apply(&self, bundle: &CanonicalBundle, base_revision: u64, actor: &str) -> Result<ConfigPlan, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let plan = plan_with_connection(&transaction, bundle)?;
        if plan.base_revision != base_revision {
            return Err(StoreError::RevisionConflict {
                current: plan.base_revision,
            });
        }
        let now = now_ms();
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
                }
                ChangeKind::Create | ChangeKind::Update | ChangeKind::Move => {
                    let mirror = &bundle.mirrors[&lmt_core::MirrorName::new(&change.mirror)
                        .map_err(|error| StoreError::InvalidConfig(error.to_string()))?];
                    let generation = change.to_generation.expect("changed mirror has generation");
                    transaction.execute(
                        "INSERT INTO mirrors(name, managed, enabled, owner_node, current_generation, removed_at_ms)
                         VALUES(?1,1,?2,?3,?4,NULL)
                         ON CONFLICT(name) DO UPDATE SET managed=1, enabled=excluded.enabled,
                           owner_node=excluded.owner_node, current_generation=excluded.current_generation, removed_at_ms=NULL",
                        params![change.mirror, mirror.document.mirror.enabled, mirror.owner_node.as_str(), generation],
                    )?;
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
                }
            }
        }
        transaction.commit()?;
        Ok(ConfigPlan {
            base_revision: revision,
            ..plan
        })
    }

    pub fn upsert_credential(&self, node: &str, token: &str) -> Result<(), StoreError> {
        let hash = token_hash(token);
        let now = now_ms();
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO nodes(name,registered_at_ms,active_runs,capabilities_json) VALUES(?1,?2,0,'{}') ON CONFLICT(name) DO NOTHING",
            params![node, now],
        )?;
        connection.execute(
            "INSERT INTO node_credentials(node_name,credential_id,token_hash,created_at_ms,revoked_at_ms)
             VALUES(?1,'bootstrap',?2,?3,NULL)
             ON CONFLICT(node_name,credential_id) DO UPDATE SET token_hash=excluded.token_hash,revoked_at_ms=NULL",
            params![node, hash, now],
        )?;
        Ok(())
    }

    pub fn authenticate_node(&self, token: &str) -> Result<Option<String>, StoreError> {
        let hash = token_hash(token);
        let connection = self.connection()?;
        Ok(connection
            .query_row(
                "SELECT node_name FROM node_credentials WHERE token_hash=?1 AND revoked_at_ms IS NULL",
                [hash],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn observe_node(
        &self,
        node: &str,
        agent_version: &str,
        instance: &str,
        active_runs: u32,
        free_bytes: Option<u64>,
        mirror_root: &str,
    ) -> Result<(), StoreError> {
        let capabilities = serde_json::json!({"mirror_root": mirror_root}).to_string();
        self.connection()?.execute(
            "UPDATE nodes SET agent_version=?2,agent_instance_id=?3,last_seen_at_ms=?4,active_runs=?5,
             mirror_root_free_bytes=?6,capabilities_json=?7 WHERE name=?1",
            params![
                node,
                agent_version,
                instance,
                now_ms(),
                active_runs,
                free_bytes,
                capabilities
            ],
        )?;
        Ok(())
    }

    pub fn list_nodes(&self) -> Result<Vec<NodeRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT name,agent_version,agent_instance_id,last_seen_at_ms,active_runs,mirror_root_free_bytes FROM nodes ORDER BY name",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(NodeRecord {
                name: row.get(0)?,
                agent_version: row.get(1)?,
                agent_instance_id: row.get(2)?,
                last_seen_at_ms: row.get(3)?,
                active_runs: row.get(4)?,
                mirror_root_free_bytes: row.get(5)?,
            })
        })?;
        rows.collect::<Result<_, _>>().map_err(StoreError::from)
    }

    pub fn list_mirrors(&self) -> Result<Vec<MirrorRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT name,managed,enabled,owner_node,current_generation FROM mirrors ORDER BY name")?;
        let rows = statement.query_map([], |row| {
            Ok(MirrorRecord {
                name: row.get(0)?,
                managed: row.get(1)?,
                enabled: row.get(2)?,
                owner_node: row.get(3)?,
                current_generation: row.get(4)?,
            })
        })?;
        rows.collect::<Result<_, _>>().map_err(StoreError::from)
    }

    pub fn get_mirror(&self, name: &str) -> Result<Option<MirrorRecord>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT name,managed,enabled,owner_node,current_generation FROM mirrors WHERE name=?1",
                [name],
                |row| {
                    Ok(MirrorRecord {
                        name: row.get(0)?,
                        managed: row.get(1)?,
                        enabled: row.get(2)?,
                        owner_node: row.get(3)?,
                        current_generation: row.get(4)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn create_manual_run(&self, mirror: &str, request_id: &str) -> Result<RunRecord, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some(existing) = find_run_by_request(&transaction, request_id)? {
            if existing.mirror_name == mirror {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::RequestConflict);
        }
        let current = transaction
            .query_row(
                "SELECT managed,enabled,owner_node,current_generation FROM mirrors WHERE name=?1",
                [mirror],
                |row| {
                    Ok((
                        row.get::<_, bool>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, u64>(3)?,
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
                [mirror],
                |row| row.get(0),
            )
            .optional()?
        {
            return Err(StoreError::MirrorBusy { run_id });
        }
        let run_id = RunId::new().to_string();
        let now = now_ms();
        transaction.execute(
            "INSERT INTO runs(id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,max_attempts,retry_delay_ms,manual_request_id)
             SELECT ?1,?2,?3,?4,'manual','pending',?5,1,0,?6",
            params![run_id, mirror, current.3, current.2, now, request_id],
        )?;
        transaction.commit()?;
        drop(connection);
        self.get_run(&run_id)?.ok_or(StoreError::AttemptNotFound)
    }

    pub fn poll_action(&self, node: &str, mirror_root: &str) -> Result<Option<PollAction>, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let pending: Option<(String, String, u64)> = transaction
            .query_row(
                "SELECT id,mirror_name,mirror_generation FROM runs WHERE owner_node=?1 AND state='pending' ORDER BY created_at_ms LIMIT 1",
                [node],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((run_id, mirror_name, generation)) = pending else {
            transaction.commit()?;
            return Ok(None);
        };
        let existing: Option<PollAction> = transaction
            .query_row(
                "SELECT spec_hash,spec_json FROM attempts WHERE run_id=?1 AND attempt_no=1 AND state='queued'",
                [&run_id],
                |row| {
                    let spec_json: String = row.get(1)?;
                    let spec = serde_json::from_str(&spec_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            spec_json.len(),
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok(PollAction {
                        run_id: run_id.clone(),
                        attempt_no: 1,
                        spec_hash: row.get(0)?,
                        spec,
                    })
                },
            )
            .optional()?;
        if let Some(mut action) = existing {
            transaction.execute(
                "UPDATE attempts SET dispatch_count=dispatch_count+1,last_dispatch_at_ms=?2 WHERE run_id=?1 AND attempt_no=1",
                params![run_id, now_ms()],
            )?;
            action.run_id = run_id;
            transaction.commit()?;
            return Ok(Some(action));
        }
        let config_toml: String = transaction.query_row(
            "SELECT config_toml FROM mirror_generations WHERE mirror_name=?1 AND generation=?2",
            params![mirror_name, generation],
            |row| row.get(0),
        )?;
        let document: MirrorDocument =
            toml::from_str(&config_toml).map_err(|error| StoreError::InvalidConfig(error.to_string()))?;
        let target_dir = Path::new(mirror_root).join(&document.mirror.target);
        let target_dir = target_dir.to_string_lossy().into_owned();
        let resolve = |value: &str| {
            value
                .replace("{mirror_name}", &mirror_name)
                .replace("{run_id}", &run_id)
                .replace("{attempt}", "1")
                .replace("{node_name}", node)
                .replace("{mirror_root}", mirror_root)
                .replace("{target_dir}", &target_dir)
        };
        let spec = ProcessRunSpec {
            runner: "process".into(),
            program: resolve(&document.sync.program),
            args: document.sync.args.iter().map(|value| resolve(value)).collect(),
            cwd: document.sync.cwd.as_deref().map(resolve),
            timeout_seconds: document.run.timeout_seconds,
            mirror_root: mirror_root.into(),
            target_dir,
        };
        let spec_json = serde_json::to_string(&spec)?;
        let spec_hash = format!("sha256:{}", hex::encode(Sha256::digest(spec_json.as_bytes())));
        let now = now_ms();
        transaction.execute(
            "INSERT INTO attempts(run_id,attempt_no,state,spec_hash,spec_json,created_at_ms,last_event_sequence,dispatch_count,last_dispatch_at_ms)
             VALUES(?1,1,'queued',?2,?3,?4,0,1,?4)",
            params![run_id, spec_hash, spec_json, now],
        )?;
        transaction.execute("UPDATE runs SET attempt_count=1 WHERE id=?1", [&run_id])?;
        transaction.commit()?;
        Ok(Some(PollAction {
            run_id,
            attempt_no: 1,
            spec_hash,
            spec,
        }))
    }

    pub fn apply_event(&self, run_id: &str, attempt_no: u32, event: &AttemptEvent) -> Result<u64, StoreError> {
        let mut connection = self.connection()?;
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
            return Ok(sequence);
        }
        let terminal_snapshot_skip = state == AttemptState::Queued && event.state.is_terminal();
        if !state.allows(event.state) && !terminal_snapshot_skip {
            return Err(StoreError::IllegalTransition {
                from: state,
                to: event.state,
            });
        }
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
        if matches!(event.state, AttemptState::Accepted | AttemptState::Running) {
            transaction.execute(
                "UPDATE runs SET state='running',started_at_ms=COALESCE(started_at_ms,?2) WHERE id=?1 AND state='pending'",
                params![run_id, event.accepted_at_ms.or(event.started_at_ms).unwrap_or_else(now_ms)],
            )?;
        } else if event.state.is_terminal() {
            let run_state = match event.state {
                AttemptState::Succeeded => RunState::Succeeded,
                AttemptState::TimedOut => RunState::TimedOut,
                AttemptState::Cancelled => RunState::Cancelled,
                AttemptState::Failed | AttemptState::Interrupted | AttemptState::Rejected => RunState::Failed,
                AttemptState::Queued | AttemptState::Accepted | AttemptState::Running => unreachable!(),
            };
            transaction.execute(
                "UPDATE runs SET state=?2,finished_at_ms=?3,final_exit_code=?4,failure_kind=?5,failure_message=?6
                 WHERE id=?1 AND state IN ('pending','running')",
                params![
                    run_id,
                    run_state_str(run_state),
                    event.finished_at_ms.unwrap_or_else(now_ms),
                    event.exit_code,
                    event.failure_kind.map(failure_kind_str),
                    event.failure_message,
                ],
            )?;
        }
        transaction.commit()?;
        Ok(event.event_sequence)
    }

    pub fn get_run(&self, id: &str) -> Result<Option<RunRecord>, StoreError> {
        Ok(self.connection()?.query_row(
            "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,finished_at_ms,
             final_exit_code,failure_kind,failure_message FROM runs WHERE id=?1",
            [id], map_run,
        ).optional()?)
    }

    pub fn list_runs(&self) -> Result<Vec<RunRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,finished_at_ms,
             final_exit_code,failure_kind,failure_message FROM runs ORDER BY created_at_ms DESC",
        )?;
        statement
            .query_map([], map_run)?
            .collect::<Result<_, _>>()
            .map_err(StoreError::from)
    }

    pub fn list_attempts(&self, run_id: &str) -> Result<Vec<AttemptRecord>, StoreError> {
        let connection = self.connection()?;
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
    }

    pub fn log_metadata(&self, run_id: &str, attempt_no: u32) -> Result<Option<(String, u64, bool)>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT relative_path,stored_bytes,complete FROM attempt_logs WHERE run_id=?1 AND attempt_no=?2",
                params![run_id, attempt_no],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?)
    }

    pub fn update_log_metadata(
        &self,
        run_id: &str,
        attempt_no: u32,
        relative_path: &str,
        stored_bytes: u64,
        complete: bool,
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO attempt_logs(run_id,attempt_no,relative_path,stored_bytes,complete,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(run_id,attempt_no) DO UPDATE SET stored_bytes=excluded.stored_bytes,
             complete=MAX(attempt_logs.complete,excluded.complete),updated_at_ms=excluded.updated_at_ms",
            params![run_id, attempt_no, relative_path, stored_bytes, complete, now_ms()],
        )?;
        Ok(())
    }
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

fn find_run_by_request(transaction: &Transaction<'_>, request_id: &str) -> Result<Option<RunRecord>, StoreError> {
    Ok(transaction.query_row(
        "SELECT id,mirror_name,mirror_generation,owner_node,trigger,state,created_at_ms,started_at_ms,finished_at_ms,
         final_exit_code,failure_kind,failure_message FROM runs WHERE manual_request_id=?1",
        [request_id], map_run,
    ).optional()?)
}

fn map_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    let state: String = row.get(5)?;
    Ok(RunRecord {
        id: row.get(0)?,
        mirror_name: row.get(1)?,
        mirror_generation: row.get(2)?,
        owner_node: row.get(3)?,
        trigger: row.get(4)?,
        state: parse_run_state(&state)?,
        created_at_ms: row.get(6)?,
        started_at_ms: row.get(7)?,
        finished_at_ms: row.get(8)?,
        final_exit_code: row.get(9)?,
        failure_kind: row.get(10)?,
        failure_message: row.get(11)?,
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

const fn failure_kind_str(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::Process => "process",
        FailureKind::Timeout => "timeout",
        FailureKind::Interrupted => "interrupted",
        FailureKind::Rejected => "rejected",
        FailureKind::InvalidResult => "invalid_result",
    }
}

fn migrate(connection: &Connection) -> Result<(), StoreError> {
    connection.execute_batch(include_str!("migration.sql"))?;
    connection.execute(
        "INSERT OR IGNORE INTO schema_migrations(version,applied_at_ms) VALUES(1,?1)",
        [now_ms()],
    )?;
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

    #[test]
    fn migrations_and_restart_preserve_state() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("lmt.db");
        let store = Store::open(&path).expect("open");
        store.apply(&bundle("/bin/true"), 0, "test").expect("apply");
        drop(store);
        assert_eq!(
            Store::open(path).expect("reopen").list_mirrors().expect("list").len(),
            1
        );
    }

    #[test]
    fn config_apply_is_atomic_and_semantic_noop_has_no_generation() {
        let store = Store::open_in_memory().expect("open");
        let first = bundle("/bin/true");
        store.apply(&first, 0, "test").expect("apply");
        let plan = store.plan(&first).expect("plan");
        assert!(plan.changes.is_empty());
        let changed = bundle("/bin/false");
        let plan = store.plan(&changed).expect("plan");
        assert_eq!(plan.changes[0].to_generation, Some(2));
        assert!(matches!(
            store.apply(&changed, 0, "stale"),
            Err(StoreError::RevisionConflict { .. })
        ));
    }

    #[test]
    fn manual_request_and_active_run_are_idempotent() {
        let store = Store::open_in_memory().expect("open");
        store.apply(&bundle("/bin/true"), 0, "test").expect("apply");
        let first = store.create_manual_run("demo", "request-1").expect("run");
        assert_eq!(store.create_manual_run("demo", "request-1").expect("same").id, first.id);
        assert!(matches!(
            store.create_manual_run("demo", "request-2"),
            Err(StoreError::MirrorBusy { .. })
        ));
    }

    #[test]
    fn duplicate_terminal_event_cannot_regress() {
        let store = Store::open_in_memory().expect("open");
        store.apply(&bundle("/bin/true"), 0, "test").expect("apply");
        let run = store.create_manual_run("demo", "request").expect("run");
        store
            .poll_action("node-a", "/tmp/mirrors")
            .expect("poll")
            .expect("action");
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
        assert_eq!(store.apply_event(&run.id, 1, &terminal).expect("event"), 3);
        let late = AttemptEvent {
            event_sequence: 2,
            state: AttemptState::Running,
            ..terminal
        };
        assert_eq!(store.apply_event(&run.id, 1, &late).expect("duplicate"), 3);
        assert_eq!(
            store.get_run(&run.id).expect("get").expect("run").state,
            RunState::Succeeded
        );
    }
}
