# Architecture Decisions

This document records current design decisions for LMT. They are intentionally lightweight rather than formal ADR files at this stage.

## Accepted decisions

### D001 - Rust for the core implementation

Status: accepted.

`lmt-server`, `lmt-agent`, and `lmt` are implemented in Rust.

Expected foundational crates include Tokio, Axum, Serde, rusqlite/tokio-rusqlite, Clap, and tracing.

Rationale:

- strong modeling for state machines and protocol enums;
- memory/concurrency safety for long-lived daemons;
- good CLI and async networking ecosystem;
- long-term correctness is more valuable than the minimum initial code volume.

Custom synchronization programs are not required to use Rust.

### D002 - TOML configuration

Status: accepted.

Human-authored LMT configuration uses TOML.

Git is recommended for configuration version history, but LMT has no Git integration and does not automatically pull repositories.

### D003 - No plugin SDK in v1

Status: accepted.

Custom synchronization is performed through ordinary executables. Built-in sync types such as rsync compile down to the same generic execution model.

A plugin API will only be designed if actual third-party requirements justify one.

### D004 - Single server, multiple agents

Status: accepted.

The initial architecture contains one authoritative `lmt-server` and any number of `lmt-agent` nodes.

Multi-controller HA is outside v1.

### D005 - One central SQLite database on the main server

Status: accepted.

The authoritative/queryable database exists only on the main machine running `lmt-server`.

All Mirror, Node, Run, Attempt, and history records are stored there in separate tables.

Agents do not run local databases. They may keep small file-based durable spools for execution crash recovery and retransmission.

SQLite files are never shared through NFS or another network filesystem.

### D006 - HTTP/JSON agent protocol

Status: accepted.

Agents initiate communication to the server using HTTP + JSON.

Control uses bounded long polling. State reports use normal HTTP POST requests.

gRPC and WebSocket are not required.

### D007 - Node-scoped configuration instead of placement fields

Status: accepted.

Each agent represents one node. Mirror ownership is derived from the configuration namespace, for example:

```text
config/nodes/mirror01/mirrors/ubuntu.toml
```

Mirror TOML files do not repeat the owner node in a `[placement]` field.

Moving a file between node namespaces is an explicit reassignment and must be surfaced as a high-impact configuration change.

The project will not implement Kubernetes-like dynamic scheduling unless real mirror deployments later justify it.

### D008 - Controller understands Mirrors; Agent understands execution

Status: accepted.

The server compiles Mirror configuration into immutable RunSpecs.

The agent implements execution/runners and local policy, but does not implement repository-specific semantics.

### D009 - CLI-first administration

Status: accepted.

All administrative functionality must be available through the CLI/API.

A web frontend is optional and expected to be read-only/status-oriented.

### D010 - Control plane is never in the serving path

Status: accepted.

Nginx or another serving layer reads mirror data directly. LMT failure must not stop clients from downloading already-available files.

### D011 - No automatic cross-node failover in v1

Status: accepted.

If a node disappears while a mirror synchronization is active, LMT does not automatically start the same mirror on another node.

Avoiding duplicate writers is more important than control-plane availability.

### D012 - No implicit LMT environment variables

Status: accepted.

LMT does not inject hidden LMT-specific environment variables into custom synchronization processes.

Runtime values such as target path or Run ID are exposed through explicit placeholders referenced in TOML. User-defined environment variables are allowed only when explicitly configured.

This keeps all synchronization dependencies visible in configuration.

### D013 - Configuration apply is authoritative and prunes removed Mirrors

Status: accepted.

The applied TOML tree is the authoritative desired Mirror set for its managed scope.

If a previously managed Mirror file is removed, the next successful apply removes that Mirror from active management. Historical Run records remain.

Configuration pruning never implicitly deletes mirror data from disk; destructive data removal is a separate explicit operation.

### D014 - Daemons automatically restart through systemd

Status: accepted.

Official Linux service units for `lmt-server` and `lmt-agent` use automatic restart-on-failure, with rate limiting/backoff and optional watchdog support.

An agent restart must be idempotent. v0.1 terminates/safely interrupts its supervised child executions, then lets the server retry with a new attempt number if policy allows.

### D015 - Run logs are centralized but not stored as SQLite blobs

Status: accepted.

Agents upload stdout/stderr incrementally to the main server using an idempotent chunk/offset protocol.

The server stores log bytes in a central filesystem log store and keeps only indexes/metadata in SQLite.

Daemon logs remain normal structured observability logs suitable for journald/Loki.

### D016 - Public Run state is small; protocol detail belongs to Attempt

Status: accepted.

The public Run state is `Pending / Running / Succeeded / Failed / Cancelled / TimedOut`.

Attempts use the more precise internal states `Queued / Accepted / Running / Succeeded / Failed / TimedOut / Cancelled / Interrupted / Rejected`.

This prevents network/execution protocol details from becoming permanent operator-facing API complexity.

### D017 - Pre-stable HTTP API uses `/api/v1alpha1`

Status: accepted.

The CLI and Agents use the same versioned HTTP API family. LMT will not claim a stable `/api/v1` compatibility contract before the first stable project release.

### D018 - Configuration apply uses optimistic revision checking

Status: accepted.

`config plan` returns a deployment-wide base revision. `config apply` carries that revision and fails with a conflict if another apply occurred in the meantime.

The server recomputes and commits the authoritative bundle atomically.

### D019 - Manual mutating requests carry client request IDs

Status: accepted.

Operations such as manual Run creation use a client-generated request ID so retrying an HTTP POST after a lost response does not duplicate operator intent.

### D020 - Three library crates and three binaries

Status: accepted.

The initial Rust workspace uses `lmt-core`, `lmt-protocol`, and `lmt-store` as library crates, with `lmt-server`, `lmt-agent`, and `lmt-cli` as binaries.

`lmt-core` must not depend on HTTP, SQL, or async-runtime infrastructure.

The project should not split into more crates until a real dependency or ownership boundary justifies it.

### D021 - Native process runner is the v0.1 execution primitive

Status: accepted.

The Agent initially implements one native Linux process runner.

Built-in rsync and custom commands both compile into the same immutable process RunSpec.

An OCI/container runner is deferred until a real deployment requires it.

### D022 - Implementation must preserve testable time and state semantics

Status: accepted.

Scheduler/state-machine logic should be expressible as deterministic domain logic and tested with an injected/fake clock where appropriate.

Wall-clock sleeps and in-memory connection state must not be required for correctness tests.

### D023 - Dispatch is the config-reconciliation revocation boundary

Status: accepted.

With at-least-once command delivery, the Server cannot assume an unacknowledged StartAttempt was never received.

Therefore disable/remove reconciliation may automatically cancel only Pending work for which no Attempt has yet been dispatched.

Once an Attempt has been dispatched, it is treated as potentially executing until reconciliation proves otherwise. Configuration disable/removal prevents new Attempts and retries, but stopping an already-dispatched Attempt requires the explicit cancellation protocol.

This rule is about configuration reconciliation only; it does not weaken operator-requested cancellation.

### D024 - M2 scheduler persists due intent and materializes Scheduled Runs on Agent poll

Status: accepted.

A wall-clock schedule occurrence becomes one coalesced durable due marker rather than immediately creating a Run.

The Scheduled Run and its first Attempt are materialized atomically only when the owning Agent polls with free execution capacity.

This makes offline/capacity misses naturally coalesce and lets delayed scheduled work use the latest Mirror generation.

### D025 - SQLite remains single-connection behind an asynchronous background-thread boundary

Status: accepted.

M2 keeps one authoritative SQLite connection. It does not add a pool, PostgreSQL, or a second source of truth.

SQLite operations move behind an async Store handle backed by a dedicated database thread so synchronous SQLite work does not block Tokio/Axum worker threads.

The current preferred implementation is tokio-rusqlite because it matches the project's single-connection architecture and current rusqlite line.

### D026 - M2 introduces ordered forward-only schema migrations

Status: accepted.

The accepted M1 schema becomes migration 0001 and M2 schema changes are migration 0002.

Missing migrations apply transactionally in ascending order. The Server refuses to open a database whose schema version is newer than the running binary.

Downgrade migrations are not required.

### D027 - Server clock owns schedule and retry deadlines

Status: accepted.

Scheduler and retry deadlines are derived from Server time, not Agent timestamps.

Domain calculations receive explicit time and are tested deterministically. Agent timestamps remain execution observations only.

### D028 - M2 cron is a strict five-field timezone-aware Vixie/POSIX subset

Status: accepted.

Cron uses exactly five minute-granularity fields and requires an explicit IANA timezone.

M2 accepts the normal wildcard/list/range/step/name subset and rejects aliases plus extended L/W/#/+/? syntax even if an underlying parser supports it.

DST behavior is explicitly documented and release-gated by tests.

### D029 - Retry is a persisted Run deadline, not another job queue

Status: accepted.

A retryable terminal Attempt leaves its Run in Running state and stores `retry_due_at`.

The next Attempt is created only after the deadline when the owner Agent polls with free capacity.

Retries never create a second Run and do not require an in-memory retry queue.

### D030 - CancelAttempt carries spec hash and supports Cancel-before-Start tombstones

Status: accepted.

For dispatched work, cancellation is at-least-once and idempotent.

CancelAttempt includes the immutable spec hash. An Agent receiving Cancel before the corresponding Start durably records a cancellation tombstone so a delayed Start cannot execute later.

### D031 - Built-in rsync is explicit configuration sugar

Status: accepted.

Rsync options remain visible in TOML.

The Server compiles rsync configuration into the same normal ProcessRunSpec used by command Mirrors. The Agent has no rsync-specific path.

LMT preserves the configured rsync source string, including trailing-slash semantics, and supplies the Mirror target directory as the destination.

### D032 - Config apply does not implicitly execute newly scheduled Mirrors

Status: accepted.

Adding, re-enabling, changing, or moving a schedule initializes its next future due time.

Configuration reconciliation does not immediately run synchronization merely because config was applied.

Operators can request immediate synchronization explicitly with the CLI.

### D033 - Creation needs request identity; cancellation is intrinsically idempotent

Status: accepted.

Operations that create a new durable resource or intent, such as manual Run creation, use a client request ID.

Cancellation targets an existing Run and is idempotent by Run identity. Repeated cancel requests preserve one persistent cancellation intent and do not require a separate request ID.


### D034 - M3 is an operations milestone, not a new scheduler milestone

Status: accepted.

M3 preserves the accepted M2 execution/scheduler model and focuses on production administration, credential lifecycle, backup/recovery, logs, metrics, diagnostics, and service hardening.

M4/M5 features must not be pulled into M3 for convenience.

### D035 - M3 does not add application-level Run-log compression

Status: accepted.

Run logs retain simple append/range/follow semantics.

M3 implements retention by age/size only. Storage compression should use transparent filesystem compression where desired.

This avoids adding compressed-offset indexes, dual-format crash recovery, and repeated decompression complexity without measured need.

### D036 - Production operator secret is file-based; Agent credentials are centrally managed

Status: accepted.

Production server.toml references operator_token_file rather than embedding the raw operator secret.

Agent raw tokens remain in per-Agent token files, while Server stores only digests and credential metadata.

M3 does not introduce multi-user operator roles/OIDC.

### D037 - Nodes are fenced to a durable Agent installation identity

Status: accepted.

A valid Node credential is not sufficient to authorize two independent Agent installations.

The first M3 Agent installation binds the Node to a durable Agent ID. A different installation receives agent_binding_conflict and no execution action.

Replacement is explicit; there is no automatic takeover.

### D038 - Server and Agent enforce local single-instance locks

Status: accepted.

lmt-server refuses a second process owning the same control plane.

lmt-agent refuses a second process owning the same spool/install state.

Correctness must not rely solely on systemd convention.

### D039 - Agent/operator credential reload must not interrupt active Runs

Status: accepted.

SIGHUP/systemd reload may re-read bearer-token files.

It does not reload arbitrary scheduler/storage/execution configuration.

A failed credential reload preserves the previous valid secret.

### D040 - Run history is durable while Run-log files may expire

Status: accepted.

M3 retention may delete only eligible terminal complete Run-log files.

Runs/Attempts and failure metadata remain in SQLite.

Intentional log expiration is represented explicitly and returns log_expired rather than masquerading as missing data.

### D041 - SQLite Online Backup API is the live database-backup mechanism

Status: accepted.

M3 does not copy the bare WAL-mode database file while lmt-server is live.

Online backups are consistent SQLite snapshots with integrity/checksum verification and atomic publication.

Run-log files are excluded from the database backup.

### D042 - Control-plane restore is offline and quiesced

Status: accepted.

There is no remote HTTP restore.

Server and related Agents are stopped, Agent Attempt spools are reset/archived while preserving Agent identity, and restored non-terminal execution state is normalized before service resumes.

Mirror data is never rolled back by LMT restore.

### D043 - Operational queries and metrics must have bounded history cost

Status: accepted.

Run history is paginated with bounded limits/keyset cursors.

Prometheus scrapes use aggregate/indexed Store queries rather than loading all historical Runs.

Per-Mirror/Node labels are allowed; Run/Attempt/credential IDs are not metric labels.

### D044 - Public status is an explicit sanitized opt-in

Status: accepted.

Administrative API remains authenticated.

A small read-only status projection may be unauthenticated only when public status is explicitly enabled, and it must omit source URLs, paths, RunSpecs, logs, and secrets.

### D045 - Legacy inline Agent credentials are import-once compatibility only

Status: accepted.

M3 may bridge M2 [[agents]] config only when the Node has no credential history.

A revoked credential must never be resurrected by stale legacy config.

The bridge is temporary before stable v1.

### D046 - M3 CLI configuration remains file/flag based with no environment overrides

Status: accepted.

The operator client may use a visible client TOML plus explicit CLI flags.

Normal LMT behavior is not configured through hidden environment-variable overrides.

### D047 - Mirror enable/disable remains TOML desired state

Status: accepted.

M3 does not add imperative CLI commands that mutate Mirror enabled state outside the authoritative config bundle.

Operational CLI convenience must not create config drift.

### D048 - first Agent credential issuance bootstraps the Node record

Status: accepted after controlled-production-trial finding T001.

A clean LMT control plane must be able to enroll its first Agent without legacy inline credentials.

An operator-authenticated credential issue for a valid Node name atomically ensures the Node record exists and creates the new credential.

The Node is still considered offline and has no Agent installation binding until the first successfully authenticated poll. The first valid poll establishes the durable binding according to D037.

This does not permit unauthenticated Agent self-registration and does not move Mirror ownership out of TOML configuration.


## Current open questions

The remaining questions are intentionally deferred beyond M3 unless production-trial evidence requires earlier resolution:

1. Stable API/version compatibility policy before the first public release.
2. Final distribution/package/release artifact strategy.
3. Whether the stable release should introduce multi-user operator identity/RBAC or retain one root operator credential.
4. Whether a future release should parse rsync statistics into structured metrics; M3 explicitly does not.
5. Whether measured real workloads justify stronger per-Attempt cgroup containment beyond the accepted Linux process-group contract.
6. Whether measured Run-log storage behavior justifies application-level compression beyond transparent filesystem compression.
7. Whether real concurrent rsync + serving requires an explicit staging/snapshot/publication layer.
8. If publication is required, whether Mirror should represent a logical published resource separately from a physical Node-owned data tree.
9. What minimal lifecycle/verification hooks are justified by real publication/operations requirements.
10. Whether large-core-count production hosts justify an explicit bounded Tokio worker policy for Server and Agent.

## Development principle

When choosing between a broader abstraction and a smaller design, prefer the smaller design until a real mirror workload demonstrates the need for the abstraction.

## Proposed M4 decisions

These remain design proposals. The second M4 review found the core architecture
acceptable but requested final recovery/GC boundary changes. This section now
reflects revision 3.

### D049 - Mirror denotes the logical published mirror resource

Status: proposed.

Mirror identity is not tied to one concrete directory inode or historical
synchronization tree.

### D050 - Atomic publication is an Attempt commit phase

Status: proposed.

For atomic Mirrors, AttemptSucceeded is emitted only after synchronization,
atomic visibility commit, and namespace durability complete. No public
Publication resource/state machine is introduced.

### D051 - M4 atomic publication uses real-directory exchange

Status: proposed.

M4 uses fresh private candidates plus Linux renameat2(RENAME_EXCHANGE) for
existing published directories and no-overwrite rename for first publication.

### D052 - Atomic candidates are fresh per Attempt

Status: proposed.

Failed/interrupted candidates are never reused by later Attempts and the
currently published tree is never an LMT synchronization destination in atomic
mode.

### D053 - Atomic built-in rsync uses fresh-generation materialization semantics

Status: proposed.

Atomic rsync intentionally differs from direct existing-destination semantics.
It uses an LMT-controlled link-dest basis and accepts only an audited atomic
rsync option profile.

### D054 - Hard-linked atomic generations are immutable

Status: proposed.

Published/previous atomic generations may share inodes and are therefore
immutable from LMT's perspective and must be treated as immutable by operators.
Previous is a namespace generation, not an isolated snapshot.

### D055 - M4 version compatibility is forward-only

Status: proposed.

Supported rolling upgrade is M3 Server+Agent -> M4 Server+M3 Agent Direct ->
M4 Server+M4 Agent. M3 Server+M4 Agent is unsupported. Downgrade is offline
restore/runbook, not in-place Server rollback.

### D056 - Mirror targets may not overlap on one Node

Status: proposed.

Exact or ancestor/descendant target overlap on one owner Node is rejected.

### D057 - Publication separates visibility from durability

Status: proposed.

Visibility commit is the successful atomic rename/exchange. AttemptSucceeded is
not emitted until required parent-directory fsync completes. M4 guarantees
atomic local visibility and daemon/process crash recovery, not recursive
power-loss durability of all repository data.

### D058 - Managed atomic published paths have one supported namespace writer

Status: proposed.

LMT is the only supported namespace writer for a managed atomic target.
Inode/device checks are best-effort invariant detection, not compare-and-swap.

### D059 - Move requires a quiescent Mirror

Status: proposed.

A Mirror with a Pending or Running Run cannot change owner Node. Move
acknowledgement cannot override this safety gate.

### D060 - Atomic GC and storage health are bounded correctness concerns

Status: proposed.

Stale private generations cannot accumulate indefinitely. GC backlog, cleanup
failure, free-space health, and admission blocking are explicit operational
semantics.

### D061 - M4 upgrade is Server-first and downgrade is restore-based

Status: proposed.

Before M4 rollout, operators create a control-plane backup. Server upgrades
first, old Agents continue Direct work, then Agents upgrade and advertise atomic
capability. Returning to M3 uses offline restore of compatible pre-M4 state and
matching binaries rather than binary rollback over M4 state.


### D062 - ready-to-commit is a durable write-ahead publication record

Status: proposed.

Atomic visibility commit is forbidden until the Agent has durably persisted
ready_to_commit with the candidate/prior-published identities and commit intent.
Publication-recovery phases bypass generic restart-to-Interrupted normalization.

### D063 - post-visibility durability ambiguity has explicit abandon/fence recovery

Status: proposed.

Persistent visible_pending_durability may be explicitly abandoned only through a
high-risk operator action. The Run terminates Failed without rollback, the
Attempt performs no later namespace operation, and a local publication fence
plus recovery evidence remains until explicitly cleared.

### D064 - publication recovery evidence survives spool reset/restore/downgrade

Status: proposed.

ready_to_commit, visible_pending_durability, committed_pending_report, and
abandoned_fenced records are protected correctness state. Generic spool cleanup
must refuse to delete them. Downgrade requires their prior resolution using M4
semantics.

### D065 - atomic GC has a frozen protected set and fail-closed admission gate

Status: proposed.

Current published, stable previous, every live/recoverable-spool path, and every
path referenced by publication recovery/fence state are non-GCable. If GC cannot
bring private-generation count below the hard bound or publication free space
above reserve, new atomic Attempts remain blocked.

### D066 - the fixed exchange slot determines previous-generation ownership

Status: proposed.

A fresh candidate is staged into one fixed private exchange slot immediately
before commit. RENAME_EXCHANGE with the published target makes that same slot
contain the immediately previous published tree after commit. Spool phase and
inode identities disambiguate pre-commit crash states without a separate
publication manifest.

### D067 - quiescent Move is one transactional Store decision

Status: proposed.

The active-Run check and owner-node update occur in the same Store transaction as
Move reconciliation. Concurrent Run creation either wins and rejects Move, or
Move wins and no old-owner Run can be created.

### D068 - M3 compatibility is tested from frozen historical artifacts

Status: proposed.

M4 compatibility gates consume verbatim M3 PollRequest, PollResponse, and Direct
ProcessRunSpec fixtures captured from the accepted M3 baseline rather than
serializing M4 structs into an imagined legacy shape. Downgrade also restores
the matching pre-M4 authoritative TOML bundle.


### D069 - pre-visibility abort restores the stable exchange slot

Status: proposed.

If commit preparation has moved the fresh candidate into the fixed exchange slot
and rotated the former previous generation, but cancellation or another
precondition wins before visibility commit, the Agent must restore the stable
previous-generation layout before reporting a terminal Attempt. Failure to
restore that private layout is a fail-closed local publication-recovery state
that blocks new atomic admission.
