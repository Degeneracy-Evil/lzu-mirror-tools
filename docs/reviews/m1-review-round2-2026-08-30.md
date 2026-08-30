# M1 Code Review Round 2 — 2026-08-30

Reviewed hardening head:

```text
e2c27dbdc573dc374c94902255265adc81b2ae10
```

Previous blocking review:

```text
docs/reviews/m1-review-2026-08-30.md
```

## 1. Verdict

**M1 is accepted.**

The hardening pass resolves the release-blocking correctness issues identified in the first review without expanding into M2 or changing the intended architecture.

The implementation is now a credible minimal vertical slice for LMT:

```text
TOML
 -> transactional central state
 -> manual Run
 -> idempotent dispatch
 -> durable Agent acceptance
 -> supervised process group
 -> bounded stdout/stderr spool
 -> central idempotent log upload
 -> terminal reconciliation
 -> restart/fault recovery
```

This is sufficient to proceed to M2 scheduling/retry work.

Acceptance does **not** mean the code is production-final. Several non-blocking engineering items remain and are listed below.

## 2. Resolution of previous release blockers

### B1 — stdout/stderr capture

**Resolved.**

The process executor now uses explicit piped stdout/stderr and bounded asynchronous pumps.

Output is written incrementally to the local Attempt log spool instead of buffering the entire process output in memory.

The E2E failure case verifies both stdout and stderr arrive through the central Run log API.

### B2 — process-group supervision

**Resolved for M1.**

Each Attempt process is created in its own Unix process group.

Timeout/shutdown termination targets the whole group with TERM followed by KILL.

A test spawns a descendant and verifies it is not left alive after timeout.

The packaged Agent service retains cgroup-level cleanup for Agent process death.

### B3 — duplicate StartAttempt race

**Resolved.**

Acceptance is serialized before execution ownership is established.

The Agent durably writes the Accepted spool record before spawning execution and checks existing durable/in-memory ownership.

A concurrent duplicate test proves one execution for the same `(run_id, attempt_no)`.

The E2E fault matrix also forces repeated dispatch while Accepted reporting is unavailable and verifies the sync command executes once.

### B4 — conflicting spec hash

**Resolved.**

A conflicting StartAttempt no longer mutates the original spool record to Rejected.

The Agent preserves the original execution ownership and logs the conflict as a protocol-integrity error.

The duplicate/conflict unit test verifies the original spec/state survives.

### B5 — systemd write semantics

**Resolved for the documented v0.1 layout.**

The Agent unit moved from `ProtectSystem=strict` to `ProtectSystem=full`, allowing a normal `/srv/mirrors` deployment to remain writable under ordinary Unix permissions.

Start-limit directives are in the unit section and `KillMode=control-group` remains enabled.

This matches the current configuration model without duplicating `mirror_root` in hidden systemd policy.

### B6 — release-gating E2E/fault tests

**Resolved.**

A Linux E2E harness now exercises a real Server and Agent over HTTP with temporary SQLite/log/spool state.

The matrix covers:

- normal successful execution;
- stdout/stderr-visible failure;
- repeated dispatch caused by dropped Accepted reports;
- lost terminal acknowledgement and retransmission;
- Server crash/restart during active execution;
- empty-log completion;
- Agent crash/restart and descendant cleanup semantics;
- authoritative config removal of undispatched work;
- preservation of mirror data.

The current GitHub CI run for the reviewed head completed successfully and executes the workspace test suite containing this Linux E2E test.

### B7 — config disable/remove dispatch boundary

**Resolved.**

Configuration apply now cancels Pending Runs only when no Attempt has been dispatched.

The store's dispatch query allows an already-dispatched Pending Attempt to continue reconciliation even after the Mirror becomes disabled/unmanaged.

This matches D023: dispatch is the config-reconciliation revocation boundary.

### B8 — empty log completion

**Resolved.**

Terminal reconciliation can send an empty log request with the completion marker.

The Agent test verifies an empty terminal log is acknowledged and the spool can retire.

The E2E matrix verifies a `/bin/true` Run exposes an empty but complete central log.

### B9 — spool retirement

**Resolved.**

The spool record tracks:

- acknowledged event sequence;
- acknowledged log offset;
- log completion acknowledgement.

Cleanup requires terminal state plus acknowledged terminal event and complete log.

Retirement uses a rename step and directory syncs so restart recovery can finish cleanup safely.

### B10 — intermediate event reconciliation

**Resolved for M1.**

The spool now records acknowledged event sequence and the background reconciler retransmits unacknowledged state.

Poll requests also report locally owned non-terminal Attempts rather than always sending an empty set.

Lost Accepted reporting is exercised by the E2E fault proxy.

## 3. Architecture review

The hardening pass improved rather than weakened the intended dependency model.

Notable improvements:

- RunSpec compilation moved from `lmt-store` into `lmt-core` plus a Server service;
- Attempt -> Run projection moved into `lmt-core`;
- Agent implementation was split into configuration, executor, spool, library orchestration, and a small binary entry point;
- log upload locking is now per Attempt instead of one global log lock.

`lmt-store` still contains transactional dispatch selection and persistence coordination. That is appropriate: the store may enforce transactional invariants as long as pure domain meaning continues moving toward `lmt-core` rather than accumulating in SQL-facing code.

No new crate is needed.

## 4. Remaining non-blocking engineering debt

These items do not block M1 acceptance, but should remain visible while designing M2.

### N1 — SQLite calls still block async HTTP worker threads

The Server still calls synchronous `rusqlite` operations directly from async handlers through a mutex.

For current M1 scale this is acceptable.

Before M2 grows scheduler activity and concurrent operational traffic, introduce a clear blocking boundary, for example a dedicated DB worker or `spawn_blocking` service boundary. Do not replace SQLite or move to an async distributed database merely to solve this.

### N2 — per-Attempt log-lock registry is not retired

The Server's in-memory map of per-Attempt log mutexes grows as new Run logs are seen.

The per-log locking model is correct, but completed entries should eventually be evicted.

This is a memory-lifetime cleanup issue, not a protocol issue.

### N3 — replayed Accepted snapshots carry later fields

When a terminal/running spool has never had sequence 1 acknowledged, the Agent reconstructs an Accepted event from the latest spool record.

The event may contain later timestamps/result fields that do not semantically belong to Accepted.

The Server remains correct because state sequencing and later terminal replay are idempotent, but protocol snapshots should eventually emit fields appropriate to the replayed state.

### N4 — Agent local path policy is lexical

`safe_spec` uses path-component containment, which is adequate for trusted v0.1 local policy and the current compiled target model.

Future security hardening should consider symlink/reparse behavior if LMT is expected to defend against hostile local filesystem layouts.

### N5 — service state directories should receive explicit restrictive modes

The official units use dedicated service users, but spool/log state can contain command arguments or operational output.

Before production trial, set/document appropriate `StateDirectoryMode`, umask, and ownership expectations.

### N6 — credential management remains bootstrap-grade

Server TOML credentials are sufficient for M1.

Token enrollment/rotation/revocation remains a later operational feature and should not be conflated with the M2 scheduler work.

### N7 — schema migration framework must evolve before schema v2

The current migration mechanism is valid for the initial schema.

The first schema-changing M2 feature must introduce ordered versioned migrations and upgrade tests before merging schema version 2.

## 5. M1 acceptance record

M1 is accepted against:

```text
implementation head:
e2c27dbdc573dc374c94902255265adc81b2ae10

CI:
GitHub Actions run 33316194425
result: success
```

The acceptance establishes the following baseline invariants for future reviews:

1. one Mirror has at most one non-terminal Run;
2. one `(run_id, attempt_no)` has at most one execution on an Agent;
3. dispatch is durable/idempotent under repeated delivery;
4. process timeout/shutdown acts on the Attempt process group;
5. Run logs are incrementally captured and centrally retrievable;
6. log/event retransmission is safe after lost acknowledgements;
7. Agent/Server restart does not silently duplicate work;
8. authoritative config removal does not delete mirror data;
9. the control plane remains outside the client download path.

M2 must preserve all of these.

## 6. Next step

Proceed to **M2 design refinement before implementation**.

M2 should add scheduling, retry/cancellation, and built-in rsync on top of the accepted M1 execution substrate rather than modifying its safety model.

The first M2 design task should also decide the small preflight infrastructure changes required by N1 and N7 so scheduler work does not accumulate on top of blocking DB calls or an unversioned migration mechanism.
