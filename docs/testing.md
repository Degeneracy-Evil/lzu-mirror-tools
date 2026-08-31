# Testing Strategy v0.2

LMT is small enough that correctness should be established primarily through deterministic tests rather than a large staging framework.

The highest-risk areas are not raw performance; they are state transitions, idempotency, crash recovery, configuration reconciliation, and process lifecycle.

## 1. Test layers

The project should use four layers:

1. pure domain/unit tests;
2. persistence/protocol integration tests;
3. local multi-process end-to-end tests;
4. fault-injection and Linux service tests.

External internet access must not be required for normal CI.

## 2. Core/domain tests

`lmt-core` should have dense unit tests for:

- Mirror/Node name validation;
- target path traversal rejection;
- TOML schema validation;
- cron vs interval mutual exclusion;
- timezone validation;
- placeholder validation and resolution;
- canonical configuration generation;
- configuration hashes;
- scheduler calculations;
- Run/Attempt state transitions;
- retry eligibility;
- cancellation races expressed as deterministic transition cases.

These tests should be fast enough to run on every edit.

## 3. Property-based tests

Use property-based testing selectively where the state space is combinatorial.

Good candidates:

- arbitrary target paths never escape `mirror_root`;
- canonicalize(parse(canonicalize(x))) is stable;
- duplicate/out-of-order Attempt events cannot regress state;
- scheduler catch-up never creates more than one coalesced due marker;
- state-transition sequences preserve terminal-state monotonicity.

`proptest` is a reasonable Rust tool for these tests.

Do not use property testing merely for coverage numbers; use it where invariants are clearer than enumerating examples.

## 4. Database tests

Every repository/transaction behavior should run against a real temporary SQLite database with migrations applied.

Critical cases include:

- migration from empty DB to latest schema;
- foreign keys enabled;
- one-active-Run partial unique index;
- atomic config apply;
- stale config revision conflict;
- Mirror removal preserves generation/Run history;
- reintroducing a Mirror preserves identity and advances generation;
- idempotent manual `request_id`;
- duplicate Attempt event sequence;
- retry transaction creates at most one next Attempt;
- server restart can reconstruct scheduler state.

Tests should assert database state, not implementation-specific call order.

## 5. Protocol serialization tests

For every v1alpha1 request/response/action/event type:

- JSON round-trip;
- required/optional field behavior;
- unknown-field policy where important;
- stable error-code serialization;
- protocol-version rejection behavior.

Golden JSON fixtures are useful for high-value Agent protocol messages because they make accidental wire-format changes visible in review.

## 6. Agent executor tests

The native process executor should be tested using local helper commands, never real upstream mirrors.

Cases:

- exit 0;
- non-zero exit;
- stdout only;
- stderr only;
- interleaved stdout/stderr;
- large output;
- timeout;
- cancellation;
- child process spawning grandchildren;
- process-group termination;
- no orphan process after Agent shutdown;
- command path/argument handling without shell interpolation.

Prefer direct argv execution. Do not invoke a shell unless the user explicitly configures a shell as the program.

## 7. Durable spool tests

The Agent spool is a safety mechanism and deserves direct crash-point tests.

Test durable states around:

```text
receive
validate
write ACCEPTED
start process
write terminal result
upload log
report result
receive server acknowledgement
cleanup
```

Important scenarios:

- duplicate StartAttempt before ACCEPTED;
- duplicate StartAttempt after ACCEPTED;
- process start fails after ACCEPTED;
- crash after terminal result but before report acknowledgement;
- restart with unacknowledged result;
- restart with accepted/running state and no surviving supervised process;
- conflicting RunSpec hash for the same execution key.

The desired outcome must always be deterministic and safe.

## 8. Log transport tests

Test the central log path independently from execution.

Cases:

- first chunk;
- sequential chunks;
- duplicate retransmission;
- overlapping/old offset;
- missing/future offset;
- server crash/restart between chunks;
- final completion marker;
- CLI read from offset;
- follow/long-poll behavior;
- log cleanup only after full acknowledgement where applicable.

The server must never silently create duplicated bytes after retransmission.

## 9. First end-to-end vertical slice

The first end-to-end scenario should intentionally use a trivial command rather than rsync.

Environment:

```text
temporary lmt-server
temporary central SQLite
temporary central log directory
temporary lmt-agent
temporary agent spool
```

Configuration contains one Mirror owned by one Node and a command equivalent to `/bin/true`.

Flow:

1. start server;
2. start Agent and register/poll;
3. validate and apply TOML bundle;
4. create a manual Run through the same API used by CLI;
5. Agent polls and receives Attempt 1;
6. Agent durably accepts;
7. process exits successfully;
8. Agent uploads terminal event/log;
9. server marks Attempt Succeeded and Run Succeeded;
10. query Run through API;
11. verify restart of server preserves the result.

This scenario is the minimum proof that the architecture works end-to-end.

## 10. Second end-to-end scenario: observable failure

Use a helper command that writes known lines to stdout/stderr and exits non-zero.

Verify:

- Run enters Running;
- Attempt becomes Failed;
- retry behavior follows configured max attempts;
- central log can be read through the operator log API;
- final Run state/failure data are correct.

## 11. Third end-to-end scenario: Agent crash

Use a long-running helper command.

Flow:

1. Run starts;
2. kill the Agent process unexpectedly;
3. verify the supervised child does not remain as an unmanaged writer;
4. systemd-equivalent test harness restarts Agent;
5. Agent reads spool and reports Interrupted;
6. server creates a new Attempt if retry policy allows;
7. second Attempt succeeds;
8. one Run exists with two Attempts.

This is one of the release-gating tests for v0.1.

## 12. Fourth end-to-end scenario: Server crash

Flow:

1. Agent starts a long-running Attempt;
2. kill server;
3. Attempt continues;
4. terminal result/log remains locally durable;
5. restart server against the same SQLite/log directory;
6. Agent reconnects and retransmits;
7. final state becomes correct without duplicate execution.

## 13. Network fault tests

A lightweight test proxy can intentionally:

- drop a StartAttempt HTTP response;
- duplicate an Attempt event;
- delay poll responses;
- drop a terminal event acknowledgement;
- drop log upload acknowledgement.

The expected rule is always:

```text
messages may repeat; execution/result bytes must not duplicate semantically
```

These tests are more valuable than attempting exactly-once networking.

## 14. Scheduler tests

Test cron and interval with a controllable fake clock rather than sleeping in real time.

Required scenarios:

- interval measured from terminal completion;
- cron fires normally;
- cron while active Run is skipped;
- Node offline creates one catch-up marker;
- ten missed occurrences remain one catch-up marker;
- recovery materializes one catch-up Run using latest generation;
- server downtime across schedule times coalesces correctly;
- disable clears catch-up;
- removal stops future work;
- config update during active Run does not mutate that Run.

Design scheduler code so time can be injected/tested deterministically.

## 15. Configuration reconciliation tests

Bundle-level cases:

- create one Mirror;
- update one Mirror;
- remove one Mirror;
- move Mirror between node namespaces;
- multi-Mirror atomic update;
- invalid one-file change rejects whole bundle;
- stale base revision;
- identical bundle is no-op;
- normalized-but-semantically-identical TOML does not create a new generation;
- historical Runs survive prune.

## 16. Local rsync integration tests

When built-in rsync is implemented, use a local rsync daemon/module or two temporary directories.

Do not depend on Ubuntu/Debian public servers in CI.

Verify the built-in rsync configuration compiles to the expected normal RunSpec and uses the same native process executor as custom commands.

## 17. systemd tests

Because process ownership/restart semantics rely on systemd, a small Linux VM/containerized integration environment should validate the official unit files.

Test:

- Agent restart-on-failure;
- restart rate limiting;
- child cgroup cleanup;
- clean stop;
- server automatic restart;
- state directory permissions.

These need not run on every fast CI job; they can run in a dedicated Linux integration job/release gate.

## 18. Security tests

At minimum:

- token hashes only, no plaintext server persistence;
- Agent cannot impersonate another Node through request JSON;
- invalid/revoked token rejected;
- path traversal rejected;
- shell injection impossible in direct argv mode;
- log path derived from trusted IDs, not unsanitized arbitrary input;
- oversized API/log bodies bounded;
- invalid protocol state cannot mutate history.

## 19. Performance tests

Performance is secondary, but regression tests should eventually cover:

- 1,000 Mirrors loaded/applied;
- 100,000+ historical Runs queried with indexes;
- concurrent long polls from dozens/hundreds of Agents;
- sustained log upload without unbounded memory growth.

These are capacity checks, not reasons to prematurely replace SQLite.

## 20. Release gates

Before a release is considered production-usable, CI should demonstrate:

- formatting/lints clean;
- all unit/property tests pass;
- migrations work from supported previous schema;
- E2E success/failure tests pass;
- Agent crash recovery passes;
- Server crash recovery passes;
- duplicate-message tests pass;
- scheduler deterministic tests pass;
- no process leak after cancellation/timeout/restart.

Correctness and recovery tests are release blockers.


## 21. M2 release-gating additions

M2 keeps every accepted M1 fault test and adds deterministic coverage for:

- interval activation and completion-relative re-arm;
- manual Run interaction with interval;
- timezone-aware five-field cron;
- spring-forward and fall-back DST behavior;
- cron occurrence skip while a Run is active;
- many missed occurrences coalescing to one due intent;
- Server restart recovery of due state;
- latest generation used when delayed Scheduled work materializes;
- Failed, TimedOut, and Interrupted retry;
- Rejected no-retry;
- multi-Attempt same-Run identity;
- retry deadline across Server restart;
- disable/remove/move retry suppression;
- cancel before dispatch;
- active process-group cancellation including descendants;
- duplicate cancellation;
- Cancel-before-delayed-Start tombstone;
- Agent restart with cancellation tombstone;
- Agent capacity;
- populated M1 database migration to M2;
- local built-in rsync without internet.

Scheduler tests must use injected/manual time. Real minute/hour sleeps are not acceptable.

## 22. M2 persistence sanity

Add a lightweight schedule-row scale test to ensure earliest-deadline queries are indexed and scheduler ticks do not scan full Run history.

This is a regression sanity check, not a reason to replace SQLite.
