# Central Database Schema v0.2

LMT v0.1 uses one authoritative SQLite database on the machine running `lmt-server`.

The database stores control-plane state and queryable history. Large Run stdout/stderr payloads are stored in the central log directory and referenced from the database rather than stored as BLOBs.

This document describes the logical schema. Exact SQL may evolve during implementation, but the ownership and invariants should remain stable.

## 1. SQLite operating mode

The server should configure SQLite approximately as follows:

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = 5000;
```

The workload has one application writer (`lmt-server`) and relatively low write volume, so SQLite is a good fit.

The database file must remain on a local filesystem. It must not be shared between hosts over NFS.

Where practical, tables should use SQLite STRICT mode.

## 2. Time representation

Persistent timestamps are UTC Unix milliseconds stored as INTEGER values.

Reasons:

- deterministic ordering and comparison;
- no timezone ambiguity in storage;
- efficient indexes;
- conversion to RFC 3339 happens only at API/UI boundaries.

Configuration timezones remain explicit IANA timezone names such as `Asia/Shanghai`.

## 3. Core tables

The initial schema consists of these logical groups:

```text
schema_migrations

config_revisions
mirrors
mirror_generations
mirror_schedule_state

nodes
node_credentials

runs
attempts
attempt_logs
```

The separation is intentional:

- configuration history is immutable;
- current desired state is small;
- runtime scheduling state can change frequently;
- execution history remains queryable after a Mirror is removed from management.

## 4. `schema_migrations`

Tracks database schema versions applied by the LMT binary.

Conceptual schema:

```sql
CREATE TABLE schema_migrations (
    version       INTEGER PRIMARY KEY,
    applied_at_ms INTEGER NOT NULL
) STRICT;
```

Migrations are bundled with the binary and run transactionally on startup before the server begins serving requests.

Downgrade migrations are not required for v0.1. Database backup before an irreversible migration is recommended.

## 5. `config_revisions`

Every successful authoritative configuration apply creates one deployment-wide revision.

```text
revision 41
revision 42
revision 43
```

Conceptual fields:

```sql
CREATE TABLE config_revisions (
    revision       INTEGER PRIMARY KEY AUTOINCREMENT,
    bundle_hash    TEXT NOT NULL,
    applied_at_ms  INTEGER NOT NULL,
    actor          TEXT,
    summary_json   TEXT NOT NULL
) STRICT;
```

`bundle_hash` is a canonical hash of the complete managed configuration bundle.

A revision exists even when many Mirrors change together. This lets one apply be treated as one atomic configuration operation.

## 6. `mirrors`

This table represents the current control-plane identity/lifecycle of each Mirror name.

Conceptual fields:

```sql
CREATE TABLE mirrors (
    name                TEXT PRIMARY KEY,
    managed             INTEGER NOT NULL CHECK (managed IN (0, 1)),
    enabled             INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    owner_node          TEXT NOT NULL,
    current_generation  INTEGER NOT NULL,
    removed_at_ms       INTEGER
) STRICT;
```

A removed Mirror is not immediately physically deleted from this table. Instead:

```text
managed = 0
removed_at_ms = ...
```

This preserves stable historical identity and foreign-key relationships.

If the same Mirror name is later reintroduced, LMT can continue generation numbering rather than creating an unrelated history.

## 7. `mirror_generations`

Mirror generations are immutable snapshots of applied configuration.

Conceptual fields:

```sql
CREATE TABLE mirror_generations (
    mirror_name      TEXT NOT NULL,
    generation       INTEGER NOT NULL,
    config_revision  INTEGER NOT NULL,
    owner_node       TEXT NOT NULL,
    config_hash      TEXT NOT NULL,
    config_toml      TEXT NOT NULL,
    created_at_ms    INTEGER NOT NULL,

    PRIMARY KEY (mirror_name, generation),

    FOREIGN KEY (mirror_name)
        REFERENCES mirrors(name),

    FOREIGN KEY (config_revision)
        REFERENCES config_revisions(revision)
) STRICT;
```

The stored TOML should be canonical/normalized by LMT, not an arbitrary byte-for-byte copy containing irrelevant whitespace.

Runs reference a specific `(mirror_name, generation)`.

## 8. `mirror_schedule_state`

Schedule runtime state is separate from Mirror configuration.

Conceptual fields:

```sql
CREATE TABLE mirror_schedule_state (
    mirror_name           TEXT PRIMARY KEY,
    next_due_at_ms        INTEGER,
    last_evaluated_at_ms  INTEGER,
    catch_up_pending      INTEGER NOT NULL DEFAULT 0
                          CHECK (catch_up_pending IN (0, 1)),
    catch_up_since_ms     INTEGER,

    FOREIGN KEY (mirror_name)
        REFERENCES mirrors(name)
) STRICT;
```

This table allows scheduler state to survive a server restart.

The scheduler never needs to persist every missed cron occurrence. `catch_up_pending` represents coalesced scheduled intent.

## 9. `nodes`

All queryable Node state is centralized here.

Conceptual fields:

```sql
CREATE TABLE nodes (
    name                    TEXT PRIMARY KEY,
    registered_at_ms        INTEGER NOT NULL,
    agent_version           TEXT,
    agent_instance_id       TEXT,
    last_seen_at_ms          INTEGER,

    mirror_root_total_bytes INTEGER,
    mirror_root_free_bytes  INTEGER,
    active_runs             INTEGER NOT NULL DEFAULT 0,

    capabilities_json       TEXT NOT NULL DEFAULT '{}'
) STRICT;
```

The `online/offline` status is derived from `last_seen_at_ms` and the configured liveness timeout. It is not a separately authoritative boolean.

`agent_instance_id` is regenerated every time the agent daemon starts. A changed instance ID lets the server recognize a daemon restart independently from a machine identity.

A configuration namespace may refer to a node before that node has ever registered. The Mirror can be applied, but no Run can execute until the matching authenticated agent appears.

## 10. `node_credentials`

Agent authentication credentials are separated from normal Node status.

A minimal v0.1 representation is:

```sql
CREATE TABLE node_credentials (
    node_name       TEXT NOT NULL,
    credential_id   TEXT NOT NULL,
    token_hash      TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL,
    revoked_at_ms   INTEGER,

    PRIMARY KEY (node_name, credential_id),

    FOREIGN KEY (node_name)
        REFERENCES nodes(name)
) STRICT;
```

This shape permits credential rotation without replacing Node identity.

Plain bearer tokens must never be stored in the database.

## 11. `runs`

A Run is one logical synchronization request.

Conceptual fields:

```sql
CREATE TABLE runs (
    id                       TEXT PRIMARY KEY,
    mirror_name              TEXT NOT NULL,
    mirror_generation        INTEGER NOT NULL,
    owner_node               TEXT NOT NULL,

    trigger                   TEXT NOT NULL,
    state                     TEXT NOT NULL,

    created_at_ms             INTEGER NOT NULL,
    started_at_ms             INTEGER,
    finished_at_ms            INTEGER,

    max_attempts              INTEGER NOT NULL,
    retry_delay_ms            INTEGER NOT NULL,
    attempt_count             INTEGER NOT NULL DEFAULT 0,

    final_exit_code           INTEGER,
    failure_kind              TEXT,
    failure_message           TEXT,

    cancel_requested_at_ms    INTEGER,
    manual_request_id         TEXT UNIQUE,

    FOREIGN KEY (mirror_name, mirror_generation)
        REFERENCES mirror_generations(mirror_name, generation)
) STRICT;
```

Important fields are snapshotted into the Run even if they could theoretically be recovered from a generation. Runtime behavior must not change because a later configuration is applied.

`manual_request_id` makes a manual sync request idempotent across HTTP retries. The CLI generates a unique request ID before submitting the operation.

## 12. `attempts`

Attempts represent concrete executions/retries of a Run.

The key is:

```text
(run_id, attempt_no)
```

Conceptual fields:

```sql
CREATE TABLE attempts (
    run_id               TEXT NOT NULL,
    attempt_no           INTEGER NOT NULL,
    state                TEXT NOT NULL,

    spec_hash            TEXT NOT NULL,
    spec_json            TEXT NOT NULL,

    created_at_ms        INTEGER NOT NULL,
    accepted_at_ms       INTEGER,
    started_at_ms        INTEGER,
    finished_at_ms       INTEGER,

    agent_instance_id    TEXT,

    exit_code            INTEGER,
    failure_kind         TEXT,
    failure_message      TEXT,

    last_event_sequence  INTEGER NOT NULL DEFAULT 0,
    dispatch_count       INTEGER NOT NULL DEFAULT 0,
    last_dispatch_at_ms  INTEGER,

    PRIMARY KEY (run_id, attempt_no),

    FOREIGN KEY (run_id)
        REFERENCES runs(id)
) STRICT;
```

The server creates an Attempt before dispatch. Re-delivery of the same Attempt is safe because the execution identity does not change.

`spec_json` is the immutable resolved RunSpec for that Attempt. `spec_hash` allows the agent to reject a dangerous protocol inconsistency where the same execution key is ever paired with different execution content.

## 13. `attempt_logs`

Large log content lives outside SQLite.

Conceptual metadata:

```sql
CREATE TABLE attempt_logs (
    run_id             TEXT NOT NULL,
    attempt_no         INTEGER NOT NULL,
    relative_path      TEXT NOT NULL,
    stored_bytes       INTEGER NOT NULL DEFAULT 0,
    complete           INTEGER NOT NULL DEFAULT 0
                       CHECK (complete IN (0, 1)),
    checksum           TEXT,
    updated_at_ms      INTEGER NOT NULL,

    PRIMARY KEY (run_id, attempt_no),

    FOREIGN KEY (run_id, attempt_no)
        REFERENCES attempts(run_id, attempt_no)
) STRICT;
```

A typical physical path may be:

```text
/var/lib/lmt/logs/2026/08/<run-id>/<attempt>.log
```

The database stores a relative path so that the whole state directory can be relocated or restored from backup.

## 14. Important indexes

At minimum:

```sql
CREATE INDEX idx_runs_mirror_created
    ON runs(mirror_name, created_at_ms DESC);

CREATE INDEX idx_runs_state_created
    ON runs(state, created_at_ms);

CREATE INDEX idx_runs_node_created
    ON runs(owner_node, created_at_ms DESC);

CREATE INDEX idx_attempts_state
    ON attempts(state);

CREATE INDEX idx_nodes_last_seen
    ON nodes(last_seen_at_ms);
```

More indexes should be added only after query plans demonstrate a need.

## 15. Transaction boundaries

These operations must be transactional:

### Configuration apply

One transaction should:

1. create `config_revisions`;
2. create changed `mirror_generations`;
3. update current `mirrors`;
4. mark removed Mirrors unmanaged;
5. update/reset affected scheduler state.

A partial apply is not acceptable.

### Run creation

One transaction should:

1. verify the Mirror is currently eligible;
2. enforce the one-nonterminal-Run invariant;
3. create the Run;
4. create the first Attempt only when dispatch preparation is appropriate.

### Attempt terminal report

One transaction should:

1. validate event sequence/idempotency;
2. update Attempt terminal state;
3. decide retry vs Run terminal state;
4. update Run timestamps/final result;
5. update interval scheduling state when the Run becomes terminal.

## 16. Enforcing one non-terminal Run per Mirror

SQLite supports partial unique indexes. LMT should use one to make the invariant durable instead of relying only on application code.

Conceptually:

```sql
CREATE UNIQUE INDEX one_active_run_per_mirror
ON runs(mirror_name)
WHERE state IN ('pending', 'running');
```

This protects against scheduler/manual-request races.

## 17. Database backup

Because the database is authoritative control-plane state, LMT should eventually provide a supported backup command.

The first implementation may wrap SQLite's online backup API rather than relying on copying a live WAL database by hand.

Mirror files themselves are not part of this control-plane backup.


## 18. M2 asynchronous Store boundary

M2 retains one SQLite connection but moves connection execution to a dedicated background thread behind an async Store handle.

This prevents synchronous SQLite work from blocking Axum/Tokio worker threads without adding a pool or changing databases.

## 19. M2 ordered migrations

The accepted M1 schema becomes ordered migration 0001. M2 additions are migration 0002.

The migration runner applies missing versions transactionally and refuses a database newer than the binary.

M1-to-M2 upgrade with populated state is a release-gating test.

## 20. M2 schema additions

Conceptually schema v2 adds:

~~~text
mirror_schedule_state.schedule_hash
nodes.max_concurrent_runs
runs.scheduled_for_at_ms
runs.retry_due_at_ms
~~~

plus indexes for earliest schedule and retry deadlines.

The existing catch_up_pending/catch_up_since_ms fields remain and represent one coalesced scheduled due intent.

No queue table is introduced.

## 21. M2 clock rule

Store operations that persist scheduler/retry deadlines receive Server time explicitly.

Agent timestamps do not determine future deadlines.

## 22. M2 transactional operations

M2 adds four especially important transaction classes:

- schedule due evaluation;
- Scheduled Run materialization plus first dispatch;
- terminal Attempt retry/final decision plus interval re-arm;
- retry Attempt creation/dispatch.

Cancellation intent is transactional and terminalizes immediately only when no Attempt may already have been dispatched.

See m2-design.md for exact eligibility and priority rules.
