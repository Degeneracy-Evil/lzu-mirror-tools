# Rust Implementation Design v0.2

This document translates the frozen LMT architecture into an implementation structure without defining application code.

The primary goal is to preserve dependency direction. LMT should remain understandable as a small distributed mirror manager, not gradually become a framework.

## 1. Workspace shape

The initial Rust workspace should contain three library crates and three binaries:

```text
lzu-mirror-tools/
├── Cargo.toml
├── crates/
│   ├── lmt-core/
│   ├── lmt-protocol/
│   └── lmt-store/
├── apps/
│   ├── lmt-server/
│   ├── lmt-agent/
│   └── lmt-cli/
├── docs/
├── config/
└── packaging/
    └── systemd/
```

The exact directory names may vary, but the dependency boundaries should not.

## 2. Dependency direction

The intended dependency graph is:

```text
                    lmt-core
                   /   |    \
                  /    |     \
                 v     v      v
        lmt-protocol  lmt-store
             |           |
             |           |
             +-----+-----+
                   |
                   v
              lmt-server

lmt-cli ------> lmt-protocol
    \              ^
     \             |
      +----------> lmt-core

lmt-agent ----> lmt-protocol
     \             ^
      +----------> lmt-core
```

Important rules:

1. `lmt-core` does not depend on Tokio, Axum, SQLx, Reqwest, or systemd-specific libraries.
2. `lmt-protocol` does not depend on Axum or Reqwest; it defines wire types, not transport clients/servers.
3. `lmt-store` does not depend on HTTP or CLI code.
4. `lmt-server` is the composition root for API, scheduling, state transitions, and persistence.
5. `lmt-agent` owns process supervision and durable spool behavior.
6. `lmt-cli` owns human/operator UX but never reimplements server business rules.

A code review should treat a reversed dependency as an architecture issue, not a style preference.

## 3. `lmt-core`

`lmt-core` contains domain meaning.

Suggested modules:

```text
lmt-core
├── config
│   ├── model
│   ├── validate
│   ├── canonicalize
│   └── placeholders
├── mirror
├── node
├── run
├── attempt
├── schedule
├── state
├── run_spec
├── ids
├── time
└── error
```

Responsibilities include:

- TOML schema types;
- configuration validation;
- canonical configuration representation/hash input;
- node namespace/path validation;
- placeholder validation/resolution rules;
- Mirror/Node/Run/Attempt domain types;
- public/internal state enums;
- legal state transition functions;
- scheduler calculations as pure functions where practical;
- immutable RunSpec representation;
- ULID wrappers and typed identifiers;
- domain errors.

The core crate may depend on small value-level libraries such as:

- Serde;
- TOML parsing/serialization;
- ULID;
- IANA timezone/date-time support;
- duration parsing;
- error derivation.

The core crate should avoid async code unless a concrete domain requirement proves it necessary.

## 4. Typed identifiers

Avoid passing unrelated identifiers as arbitrary `String` values throughout the core.

Conceptually use newtypes for:

```text
MirrorName
NodeName
RunId
AttemptNo
ConfigRevision
MirrorGeneration
AgentInstanceId
RequestId
```

This prevents accidental interchange and gives one place to enforce validation.

Human-facing names such as Mirror/Node should have conservative character rules suitable for paths, APIs, and CLI use.

## 5. State transition ownership

State transitions should be centralized.

Bad pattern:

```text
HTTP handler -> UPDATE runs SET state = 'failed'
scheduler    -> UPDATE runs SET state = 'running'
report route -> UPDATE runs SET state = 'succeeded'
```

Preferred pattern:

```text
event/input
   |
   v
domain transition function
   |
   v
transition/result
   |
   v
repository transaction
```

The implementation should make illegal transitions difficult to express.

The database remains the durable invariant layer, but the Rust domain layer should reject invalid transitions before SQL is attempted.

## 6. `lmt-protocol`

This crate contains versioned HTTP wire contracts shared by server, CLI, and Agent.

Suggested shape:

```text
lmt-protocol
├── v1alpha1
│   ├── common
│   ├── mirrors
│   ├── runs
│   ├── nodes
│   ├── config
│   └── agent
└── error
```

Responsibilities:

- request/response DTOs;
- API error envelope;
- protocol version constants;
- Agent action/event types;
- log-offset headers/constants;
- compatibility/version negotiation fields where needed.

It must not contain:

- Axum routers;
- Reqwest clients;
- SQL queries;
- process execution;
- scheduler implementation.

Keeping DTOs transport-neutral makes protocol tests easy and avoids coupling the shared API model to one HTTP framework.

## 7. `lmt-store`

This crate is the only crate that knows the central SQLite schema.

Suggested modules:

```text
lmt-store
├── migrations
├── transaction
├── config_repo
├── mirror_repo
├── node_repo
├── run_repo
├── attempt_repo
├── schedule_repo
└── log_index_repo
```

Responsibilities:

- database opening and PRAGMAs;
- migrations;
- transaction helpers;
- persistence of domain state;
- queries used by server services;
- invariant-friendly SQL;
- backup integration later.

Do not expose SQLx row types as the public interface of the crate. Convert to/from domain/persistence structs at the boundary.

Avoid a generic repository framework. Repository APIs should reflect LMT operations.

## 8. `lmt-server`

The server is an application/composition layer rather than a place for domain rules.

Suggested internal modules:

```text
lmt-server
├── app
├── api
│   ├── health
│   ├── mirrors
│   ├── runs
│   ├── nodes
│   ├── config
│   └── agent
├── services
│   ├── config_service
│   ├── run_service
│   ├── agent_service
│   └── log_service
├── scheduler
├── auth
├── logs
├── metrics
└── shutdown
```

The scheduler should be one explicit component, not timer logic spread across handlers.

Long-poll waiters are ephemeral optimization state only. Correctness must remain reconstructible from SQLite after restart.

## 9. `lmt-agent`

Suggested internal modules:

```text
lmt-agent
├── config
├── client
├── poll
├── executor
│   └── process
├── supervision
├── spool
├── log_upload
├── capacity
├── recovery
└── shutdown
```

The v0.1 executor supports the native process runner only.

A future OCI/container runner should be a second executor implementation, not a change to Mirror semantics.

The Agent must keep business state minimal:

- local config;
- temporary durable spool;
- current supervised processes.

Everything queryable by operators is reported to the central server.

## 10. Process supervision

The process runner must execute a whole supervised process group rather than only tracking a direct child PID.

It must support:

- stdout/stderr capture;
- timeout;
- cancellation;
- graceful termination period if configured;
- forced group termination;
- exit status collection;
- no orphan children after Agent restart.

The systemd unit and runtime process-group behavior should be designed together and tested on Linux.

Service hardening must not silently make the configured `mirror_root` read-only. In particular, if the packaged unit uses `ProtectSystem=strict`, installation must explicitly make the configured mirror root writable. Because LMT intentionally avoids hidden duplicate configuration, the simpler v0.1 default may instead use a hardening level that relies on the dedicated Agent user's normal filesystem permissions for mirror data while keeping the Agent state directory protected.

Per-Attempt process groups are still required even when systemd uses control-group killing for the Agent service: unit-level cleanup handles Agent death, while per-Attempt groups handle timeout/cancellation without killing unrelated Attempts.

The project should not attempt to support non-Linux Agents in v0.1.

## 11. Combined Run logs

The initial implementation should define one ordered combined Run log stream for CLI retrieval.

A practical framing approach is to prefix chunks/records with stream identity and timestamp rather than maintaining unrelated stdout/stderr offsets.

The exact binary/text framing format is still an implementation detail, but requirements are:

- preserve stdout vs stderr identity;
- preserve ordering as observed by the Agent;
- append-only;
- resumable by byte offset;
- human-readable through `lmt run logs`;
- safe to upload/retry idempotently.

This format should be documented before log transport implementation.

Full-text indexing is not a responsibility of SQLite. Optional Loki collection can provide cross-Run text search.

For M1, the combined stream is UTF-8-oriented bytes framed by the Agent as
`[stdout] ` or `[stderr] ` followed by the captured bytes. The framing prefix is
part of the uploaded byte stream and therefore participates in offset-based
idempotency. A bounded channel serializes chunks in the order the Agent observes
them from the two pipes; that observed order is preserved in the spool and in
central storage.

## 12. Configuration compilation

Configuration handling has three stages:

```text
TOML bundle
   |
   v
validated/canonical domain configuration
   |
   v
Mirror generation
   |
   v
immutable RunSpec
```

Only the server compiles Mirror semantics such as built-in rsync into execution RunSpecs.

The Agent receives a resolved RunSpec and never re-reads central Mirror TOML.

## 13. Built-in rsync

The built-in rsync sync type should be deliberately thin.

It is configuration sugar that compiles to the normal process runner.

It should not create a second rsync-specific execution path inside the Agent.

This preserves one execution model:

```text
rsync config ----\
                  +--> RunSpec --> process executor
command config --/
```

## 14. Error taxonomy

Avoid returning raw anyhow/string errors across domain boundaries.

At minimum distinguish:

- configuration validation errors;
- conflict errors;
- authentication errors;
- temporary node/capacity errors;
- permanent local-policy rejection;
- execution failure;
- infrastructure interruption;
- persistence/database failure;
- protocol/version failure.

Human-readable context can wrap structured categories.

## 15. Concurrency model

LMT does not need a highly parallel internal architecture.

Prefer explicit tasks with clear ownership:

Server:

- HTTP server;
- scheduler loop;
- liveness/reconciliation loop;
- graceful shutdown.

Agent:

- poll loop;
- one task per accepted Attempt;
- log uploader per active Attempt or a bounded shared uploader;
- recovery/shutdown coordination.

Use bounded channels where asynchronous queues are needed. Avoid unbounded queues because mirror logs and run events can be large/long-lived.

## 16. Graceful shutdown

Server shutdown:

1. stop accepting new mutating requests;
2. stop creating/dispatching new Attempts;
3. finish current DB transactions;
4. close HTTP cleanly.

Agent shutdown:

1. stop polling for new work;
2. mark shutdown intent;
3. terminate active supervised process groups according to v0.1 semantics;
4. durably record interrupted/terminal spool state;
5. make a best-effort report/upload;
6. exit so systemd can restart or stop the unit.

The distinction between intentional service stop and crash may be recorded in failure metadata, but both must remain safe.

## 17. Configuration files

Expected local files:

Server:

```text
/etc/lmt/server.toml
/var/lib/lmt/lmt.db
/var/lib/lmt/logs/
```

Agent:

```text
/etc/lmt/agent.toml
/etc/lmt/agent.token
/var/lib/lmt-agent/spool/
```

Mirror configuration itself normally lives in an operator-managed Git repository and is submitted through `lmt config apply`.

## 18. Dependency discipline checklist

A proposed change should be questioned if it causes any of these:

- `lmt-core` imports Axum/Tokio/SQLite infrastructure;
- Agent imports central SQLite code;
- CLI contains scheduler/business logic;
- server HTTP handler performs direct ad-hoc state updates;
- repository-specific behavior enters Agent execution code;
- logs are stored as SQLite BLOBs;
- background correctness depends on in-memory long-poll sessions;
- configuration is mutated outside the authoritative apply model.

These are architecture regressions unless accompanied by an explicit design decision update.


## 19. M2 persistence execution

M2 keeps the same six-crate workspace.

The central Store changes from a synchronous connection called on async workers to an async handle backed by one dedicated SQLite thread.

The preferred current implementation is tokio-rusqlite.

Do not add a pool or new database.

## 20. M2 domain libraries

lmt-core may add small value-level dependencies for strict cron evaluation, IANA timezones, and human duration parsing.

The current intended choices are Croner, chrono-tz, and humantime.

LMT must wrap and restrict dependency syntax so dependency features do not silently expand the public TOML contract.

## 21. M2 scheduler module

Server gets one explicit scheduler module/task.

It owns wakeup orchestration only.

Pure due/retry decisions live in lmt-core; persistence and transactional invariants live in lmt-store.

No in-memory correctness-critical job queue is introduced.

## 22. M2 Agent cancellation control

The active Agent registry needs per-Attempt cancellation control handles, not only execution keys.

Durable spool state remains the crash-recovery authority.

Cancel-before-Start requires a durable tombstone representation.

## 23. M2 rsync boundary

Only core/Server understands sync.type=rsync.

Agent continues to execute ordinary ProcessRunSpec.

No rsync-specific Agent module or execution path is allowed.


## 24. Attempt process-group closure

Attempt ownership does not end merely because the direct child process has exited.

Before recording a normal Succeeded or Failed terminal result, the executor must ensure that no ordinary descendants remain alive in the Attempt's supervised process group.

A background descendant must not outlive a terminal Run and overlap a future synchronization.

Timeout, cancellation, shutdown, and normal direct-child completion all have to close the Attempt process ownership boundary safely.


## 25. M3 process locks

Server and Agent binaries acquire advisory exclusive locks around their local correctness state.

The lock primitive belongs in small Linux/runtime infrastructure modules, not lmt-core.

Server offline backup/restore commands acquire the same Server lock as normal service startup.

Agent disaster-recovery spool maintenance acquires the same Agent lock as normal Agent startup.

## 26. M3 durable Agent identity

Agent installation identity is persisted atomically under Agent state and loaded before polling.

A per-process boot ID may remain ephemeral.

The durable identity is passed through protocol DTOs and Node binding checks but is not Mirror placement configuration.

## 27. M3 credential reload

Bearer secret state becomes reloadable without replacing the whole Agent/Server application object.

Use a small synchronized/atomic credential holder.

SIGHUP/reload should:

- read the configured token file;
- validate non-empty content;
- atomically replace only after successful read/validation;
- preserve the previous credential on failure.

Do not turn SIGHUP into arbitrary TOML hot reload.

## 28. M3 backup module

Use rusqlite's backup feature / SQLite Online Backup API.

Online backup may use a transient dedicated SQLite source/destination connection in blocking infrastructure; it does not create a second authoritative application database.

Backup file/manifest publication is fsync + atomic rename based.

Restore remains a local lmt-server maintenance path, not HTTP handler logic.

## 29. M3 log maintenance

Retention selection is Store/domain policy; file unlinking belongs in Server log infrastructure.

Intentional DB expiration is committed before unlink.

Attempt log-lock ownership must coordinate append and delete without a permanently growing strong-reference registry.

## 30. M3 CLI architecture

The CLI should stop being one large response-printing main.rs.

Keep one binary/crate but split internal modules for:

- client config/auth;
- API client;
- human rendering;
- JSON rendering;
- commands;
- log streaming;
- exit-code mapping.

Do not create a generic SDK/framework solely for M3 CLI polish.

## 31. M3 metrics/status

Operational DB projections belong in lmt-store semantic queries.

Server metrics/status handlers format those projections.

Avoid repeated full-history scans or duplicating business logic in Prometheus collectors.


## 32. M3 crash-safe file publication

Small durable local files such as Agent installation identity and CLI-created bearer-token files must tolerate a process crash between temporary-file fsync and final rename.

A fixed create_new temporary name without stale-temp recovery is not sufficient.

The publication helper should use unique temporary names or explicit stale-artifact recovery under the appropriate local lock.

Credential issue additionally needs compensation: if the Server has issued a credential but local secret publication fails, the CLI should revoke that credential best-effort and clearly surface its credential ID when cleanup cannot be confirmed.

## 33. M3 expired-log upload behavior

append-log handling must consult log expiration metadata before creating or writing a file.

An intentionally expired log is terminal from the central storage-policy perspective. Late Agent retransmission can advance/ack recovery state but cannot rehydrate the expired file.

## 34. M3 bounded log streaming

CLI log commands use a shared offset/chunk loop.

Normal display consumes until complete EOF; follow mode additionally long-polls at incomplete EOF.

Presentation layers must not accumulate an unbounded Run log into one in-memory object.
