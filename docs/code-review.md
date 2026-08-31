# Code Review Guide

This guide is intended for maintainers reviewing LMT implementation changes.

The primary review question is not "does this compile?" but "does this preserve the system invariants documented in `docs/`?"

## 1. Architecture boundary checks

Reject or question changes that:

- place Axum/SQLx/Tokio concerns into `lmt-core`;
- let the Agent open the central database;
- put repository-specific synchronization semantics inside the Agent;
- add hidden environment-based configuration;
- mutate managed Mirror configuration outside authoritative bundle apply;
- put LMT into the user download path;
- introduce automatic cross-node failover without fencing;
- create a second execution path that bypasses Run/Attempt semantics.

## 2. Configuration review

Check:

- TOML remains the complete visible configuration source;
- new runtime values are explicit, not hidden;
- canonicalization is deterministic;
- semantically identical configuration does not create meaningless generations;
- config apply remains atomic;
- file removal prunes management state;
- pruning never deletes repository data;
- node moves are visible/high-impact operations;
- target paths cannot escape node mirror root.

## 3. State-machine review

Every change to Run/Attempt state should answer:

- what exact input/event caused this transition?
- is it legal from every possible previous state?
- what if the event is duplicated?
- what if the event arrives late/out of order?
- what happens after server restart?
- what happens after Agent restart?
- does any terminal state regress?
- can the transition accidentally create two writers?

Do not accept direct state writes spread across handlers without centralized validation.

## 4. Idempotency review

For every mutating network operation, ask:

> what happens if the peer completed it but the HTTP response was lost?

Critical keys include:

- manual request ID;
- `(run_id, attempt_no)`;
- Attempt event sequence;
- log byte offset;
- config base revision/bundle hash.

Retries must return/reconcile existing intent rather than duplicate it.

## 5. Process lifecycle review

Agent execution changes must ensure:

- no shell interpolation unless explicitly requested;
- direct argv semantics;
- child process-group ownership;
- timeout terminates the group;
- cancel terminates the group;
- Agent crash/restart cannot leave an unmanaged writer;
- terminal result is durably spooled before it can be forgotten;
- retry creates a new Attempt rather than silently restarting the old identity.

## 6. Database review

Check:

- foreign keys remain enabled;
- schema migrations are forward-only and tested;
- transactions match documented atomic boundaries;
- one-active-Run invariant remains database-enforced;
- history survives Mirror removal;
- large logs do not enter SQLite BLOBs;
- query additions have justified indexes;
- raw SQL does not leak throughout application layers.

## 7. Scheduler review

Check:

- same Mirror never has concurrent non-terminal Runs;
- interval remains completion-relative;
- cron while active Run is skipped;
- unavailable-node/server misfires coalesce;
- manual intent is durable;
- disabled/removed Mirrors do not generate new retries;
- active Run generation remains immutable;
- logic is testable with an injected/fake clock rather than wall-clock sleeps.

## 8. Protocol/API review

Check:

- v1alpha1 wire changes are deliberate and tested;
- machine-readable error codes exist;
- Agent authenticated identity cannot be overridden by request body;
- command delivery may repeat safely;
- terminal events are self-contained;
- log upload is idempotent by offset;
- request/response body limits exist;
- no endpoint depends on in-memory connection state for correctness.

## 9. Observability review

A production failure should be diagnosable from central state.

Check that important operations expose:

- Run ID;
- Attempt number;
- Mirror name;
- Node name;
- config generation;
- failure category;
- relevant timestamps.

Daemon logs should be structured.

Business history belongs in the central DB; daemon diagnostics belong in journald/Loki; Run stdout/stderr belongs in central Run logs.

## 10. Security review

Check:

- no plaintext bearer token storage;
- secrets are not logged;
- paths are derived safely;
- command configuration cannot produce accidental shell injection;
- filesystem permissions are least-privilege practical;
- destructive data deletion is never implied by config pruning;
- API mutation endpoints are authenticated;
- malformed/oversized inputs are bounded.

## 11. Complexity review

Before accepting a new subsystem, ask:

1. Which current production problem requires it?
2. Can the same need be solved through an external standard tool?
3. Does it introduce another source of truth?
4. Does it create a new failure mode?
5. Can it be deferred without blocking the core mirror manager?

LMT intentionally prefers a small core with strong contracts.

## 12. Documentation rule

If code changes any architecture invariant, state transition, scheduler behavior, protocol contract, configuration semantics, or persistence meaning, the corresponding `docs/` document must change in the same review.

Code and design documentation must not intentionally drift.


## 13. M2 review additions

For M2 changes, additionally check the following.

### Scheduler

- schedule due state is persisted before notification;
- no one-Run-per-tick backlog is introduced;
- Scheduled Runs materialize only through the documented Agent-poll transaction;
- delayed Scheduled work uses the latest generation;
- cron active-Run occurrences are skipped, not queued;
- interval is Server-completion-relative;
- scheduler correctness survives restart without an in-memory queue;
- all time-dependent logic is testable with explicit/manual time.

### Retry

- retries stay inside one Run;
- no Attempt N+1 exists before retry deadline dispatch;
- retry deadline uses Server time;
- Failed/TimedOut/Interrupted retry only when eligible;
- Rejected/Cancelled do not retry;
- disable/remove/move suppress later retry;
- attempt numbers remain monotonic.

### Cancellation

- undispatched cancellation is immediate;
- any dispatched Attempt is treated as potentially executing;
- CancelAttempt carries spec_hash;
- Cancel-before-Start creates a durable tombstone;
- delayed Start after tombstone cannot execute;
- active cancel terminates the entire Attempt process group;
- natural terminal result already durably observed by Agent is not overwritten.

### Persistence

- populated M1-to-M2 migration is tested;
- future schema version is rejected;
- SQLite remains one authoritative connection/database;
- async Store execution does not leak SQLite concerns into core or HTTP contracts.

### rsync

- args remain visible in TOML;
- source string is preserved exactly, including trailing slash;
- destination is the Mirror target directory;
- Agent contains no rsync-specific execution logic;
- tests use local resources only.

The authoritative M2 reference for these checks is docs/m2-design.md.


## 14. Idempotent observability

Because Agent event delivery is at-least-once, semantic metrics must be updated from newly applied state transitions rather than HTTP request count unless a metric is explicitly named and documented as a request counter.

In particular, retransmitted terminal Attempt events after a lost acknowledgement must not double-count Attempt outcomes or newly scheduled retries.
