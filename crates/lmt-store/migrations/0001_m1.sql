CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL) STRICT;
CREATE TABLE config_revisions(
 revision INTEGER PRIMARY KEY AUTOINCREMENT, bundle_hash TEXT NOT NULL, applied_at_ms INTEGER NOT NULL,
 actor TEXT, summary_json TEXT NOT NULL
) STRICT;
CREATE TABLE mirrors(
 name TEXT PRIMARY KEY, managed INTEGER NOT NULL CHECK(managed IN(0,1)), enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),
 owner_node TEXT NOT NULL, current_generation INTEGER NOT NULL, removed_at_ms INTEGER
) STRICT;
CREATE TABLE mirror_generations(
 mirror_name TEXT NOT NULL, generation INTEGER NOT NULL, config_revision INTEGER NOT NULL, owner_node TEXT NOT NULL,
 config_hash TEXT NOT NULL, config_toml TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
 PRIMARY KEY(mirror_name,generation), FOREIGN KEY(mirror_name) REFERENCES mirrors(name),
 FOREIGN KEY(config_revision) REFERENCES config_revisions(revision)
) STRICT;
CREATE TABLE mirror_schedule_state(
 mirror_name TEXT PRIMARY KEY, next_due_at_ms INTEGER, last_evaluated_at_ms INTEGER,
 catch_up_pending INTEGER NOT NULL DEFAULT 0 CHECK(catch_up_pending IN(0,1)), catch_up_since_ms INTEGER,
 FOREIGN KEY(mirror_name) REFERENCES mirrors(name)
) STRICT;
CREATE TABLE nodes(
 name TEXT PRIMARY KEY, registered_at_ms INTEGER NOT NULL, agent_version TEXT, agent_instance_id TEXT,
 last_seen_at_ms INTEGER, mirror_root_total_bytes INTEGER, mirror_root_free_bytes INTEGER,
 active_runs INTEGER NOT NULL DEFAULT 0, capabilities_json TEXT NOT NULL DEFAULT '{}'
) STRICT;
CREATE TABLE node_credentials(
 node_name TEXT NOT NULL, credential_id TEXT NOT NULL, token_hash TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
 revoked_at_ms INTEGER, PRIMARY KEY(node_name,credential_id), FOREIGN KEY(node_name) REFERENCES nodes(name)
) STRICT;
CREATE UNIQUE INDEX idx_active_token_hash ON node_credentials(token_hash) WHERE revoked_at_ms IS NULL;
CREATE TABLE runs(
 id TEXT PRIMARY KEY, mirror_name TEXT NOT NULL, mirror_generation INTEGER NOT NULL, owner_node TEXT NOT NULL,
 trigger TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN('pending','running','succeeded','failed','cancelled','timed_out')),
 created_at_ms INTEGER NOT NULL, started_at_ms INTEGER, finished_at_ms INTEGER, max_attempts INTEGER NOT NULL,
 retry_delay_ms INTEGER NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0, final_exit_code INTEGER,
 failure_kind TEXT, failure_message TEXT, cancel_requested_at_ms INTEGER, manual_request_id TEXT UNIQUE,
 FOREIGN KEY(mirror_name,mirror_generation) REFERENCES mirror_generations(mirror_name,generation)
) STRICT;
CREATE UNIQUE INDEX one_active_run_per_mirror ON runs(mirror_name) WHERE state IN('pending','running');
CREATE INDEX idx_runs_mirror_created ON runs(mirror_name,created_at_ms DESC);
CREATE INDEX idx_runs_state_created ON runs(state,created_at_ms);
CREATE INDEX idx_runs_node_created ON runs(owner_node,created_at_ms DESC);
CREATE TABLE attempts(
 run_id TEXT NOT NULL, attempt_no INTEGER NOT NULL CHECK(attempt_no > 0),
 state TEXT NOT NULL CHECK(state IN('queued','accepted','running','succeeded','failed','timed_out','cancelled','interrupted','rejected')),
 spec_hash TEXT NOT NULL, spec_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL, accepted_at_ms INTEGER,
 started_at_ms INTEGER, finished_at_ms INTEGER, agent_instance_id TEXT, exit_code INTEGER, failure_kind TEXT,
 failure_message TEXT, last_event_sequence INTEGER NOT NULL DEFAULT 0, dispatch_count INTEGER NOT NULL DEFAULT 0,
 last_dispatch_at_ms INTEGER, PRIMARY KEY(run_id,attempt_no), FOREIGN KEY(run_id) REFERENCES runs(id)
) STRICT;
CREATE INDEX idx_attempts_state ON attempts(state);
CREATE TABLE attempt_logs(
 run_id TEXT NOT NULL, attempt_no INTEGER NOT NULL, relative_path TEXT NOT NULL, stored_bytes INTEGER NOT NULL DEFAULT 0,
 complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN(0,1)), checksum TEXT, updated_at_ms INTEGER NOT NULL,
 PRIMARY KEY(run_id,attempt_no), FOREIGN KEY(run_id,attempt_no) REFERENCES attempts(run_id,attempt_no)
) STRICT;
CREATE INDEX idx_nodes_last_seen ON nodes(last_seen_at_ms);
