# M2 Code Review Round 2 — 2026-08-31

Reviewed hardening head:

~~~text
7ad3886c1c11d011ae6fd76df2fa6ecc1b87bdaf
~~~

GitHub Actions reviewed:

~~~text
run 33403795199
conclusion: success
~~~

## 1. Verdict

**M2 is accepted.**

The focused hardening pass resolves every release blocker and release-gating test gap from the first M2 review without changing the core M2 architecture or introducing M3 scope.

Accepted implementation baseline:

~~~text
7ad3886c1c11d011ae6fd76df2fa6ecc1b87bdaf
~~~

M2 now provides a production-oriented foundation for periodic command/rsync mirror synchronization with deterministic scheduling, persistent retry, idempotent cancellation, bounded Agent concurrency, centralized state/logs, and crash recovery.

## 2. Resolution of first-review blockers

### B1 — terminal Run cancellation mutating history: resolved

request_cancellation now checks terminal Run state before writing cancellation intent.

A later cancellation request against Succeeded, Failed, TimedOut, or Cancelled Run state returns the existing Run unchanged and reports no newly-created cancellation intent.

Dedicated Store tests verify all terminal classes remain unchanged.

### B2 — Cancel-before-Start tombstone leak: resolved

Spool cleanup no longer requires a RunSpec to exist.

A cancellation tombstone remains durable until:

- its terminal Cancelled event is acknowledged;
- log completion is acknowledged.

After both acknowledgements, it is retired through the normal spool retirement path.

The hardening E2E fault test also verifies the tombstone actually disappears after Start-response-loss followed by cancellation.

### B3 — direct child exit leaving descendants alive: resolved

The process runner now closes Attempt process-group ownership even after normal direct-child completion.

Important implementation details:

- Agent enables Linux child-subreaper behavior;
- every Attempt still receives its own process group;
- after any direct-child outcome, the process group is closed;
- remaining descendants receive TERM then KILL;
- adopted descendants in that process group are reaped;
- terminal Attempt persistence happens only after this closure path.

A regression test starts a detached-looking background sleep with stdio redirected away, lets the shell exit successfully, and verifies the background process is gone before the Attempt is recorded Succeeded.

This closes the practical one-writer hole identified in review round 1.

### B4 — semantic metrics double-counting duplicate events: resolved

Store event application now returns semantic application metadata:

- accepted event sequence;
- whether the event was newly applied;
- whether a retry was newly scheduled.

The Server increments terminal and retry semantic counters only for newly-applied events.

Cancellation application similarly reports whether cancellation intent was newly requested, so repeated idempotent cancellation requests do not inflate cancellation metrics.

HTTP/request-level event counting remains separate and may count retransmissions by design.

Tests explicitly replay terminal events and cancellation calls and verify semantic counters increment once.

## 3. Resolution of test gaps

### T1 — real scheduled execution E2E: resolved

A deterministic Server clock is now injectable into the real Server harness.

The new E2E path verifies:

~~~text
interval configured
-> deadline not reached: no execution
-> fake Server clock advanced
-> scheduler persists due intent
-> real Agent poll
-> Scheduled Run materialized
-> real process executed
-> Run Succeeded
~~~

No real one-minute sleep is used.

### T2 — Start-response-loss then cancellation E2E: resolved

The fault proxy can now drop an actual PollResponse containing StartAttempt after the Server has durably recorded dispatch.

The E2E verifies:

~~~text
Server records StartAttempt
-> response is lost
-> Agent never accepts Start
-> operator requests cancellation
-> Agent receives CancelAttempt
-> cancellation tombstone is created
-> command executes zero times
-> Server records Attempt Cancelled
-> acknowledged tombstone retires
~~~

This exercises the actual network/control-plane path rather than only direct Agent methods.

### T3 — wildcard/step DST tests: resolved

The core schedule suite now includes wildcard/step tests across both spring-forward and fall-back transitions, in addition to the existing fixed-time DST tests.

The behavior is deterministic under the currently locked Croner version.

## 4. Regression verification

The hardening preserves:

- M1 duplicate Start protection;
- Accepted/event/log acknowledgement recovery;
- Server crash recovery;
- Agent interruption recovery;
- bounded streaming logs;
- process-group timeout cleanup;
- config removal dispatch boundary;
- same-Run retry semantics;
- capacity enforcement;
- local rsync execution;
- scheduler persistence;
- cancellation idempotency.

No M3 functionality was introduced.

## 5. CI verification

For head:

~~~text
7ad3886c1c11d011ae6fd76df2fa6ecc1b87bdaf
~~~

GitHub Actions run:

~~~text
33403795199
~~~

completed successfully.

CI gates passed:

- cargo fmt --all -- --check;
- cargo clippy --all-targets --all-features --locked -- -D warnings;
- cargo test --all-features --locked;
- clean working-tree/index checks.

The Agent also reports the full workspace build passing locally.

## 6. Non-blocking debt carried forward

These are not M2 blockers.

### N1 — metrics scrape still loads full Run history

Pending/running gauges currently derive from list_runs and filter historical records in memory.

Before sustained production, replace this with aggregate Store queries or equivalent bounded-cost metrics collection.

### N2 — Server log-lock registry still lacks eviction

Per-Attempt log locks accumulate in memory.

Add lifecycle eviction before long-running production deployment.

### N3 — spool reconciliation uses a fixed 200 ms scan

This is simple and correct at current scale, but large spool directories would benefit from less frequent/event-driven reconciliation or bounded indexing.

Do not optimize this before measurement.

### N4 — DST overlap contract should be tightened before public stable release

M2 now has deterministic fixed and wildcard/step DST regression tests, which is sufficient for M2 acceptance.

However, the wording "may match both passes" is intentionally permissive and the dependency behavior is part of the current implementation.

Before a stable public API/config contract, decide whether LMT promises a specific duplicated-hour execution policy independent of the cron library and document that policy precisely.

### N5 — migration fixtures should become immutable version artifacts

M1-to-M2 compatibility was reviewed against the accepted M1 SQL and is safe.

For M3+ schema changes, keep frozen previous-version DB/schema fixtures so migration tests cannot accidentally redefine historical schema.

### N6 — Linux process-group supervision is the M2 runner contract

The hardening closes ordinary descendants in the Attempt process group.

A process that intentionally escapes supervision with a new session/process group is outside the M2 process-runner contract.

If future real syncers require stronger containment, evaluate per-Attempt cgroups rather than complicating M2 now.

## 7. M2 acceptance summary

The accepted M2 system has the following shape:

~~~text
TOML desired state
        |
        v
lmt-server
  scheduler
  retry/cancel state
  async single SQLite
        |
        | long poll / JSON
        v
lmt-agent
  durable spool
  capacity policy
  process-group supervision
        |
        v
command / rsync
~~~

The key correctness properties are now covered by automated tests rather than design intent alone.

## 8. Next milestone

M3 may now be designed.

Do not begin M3 implementation until the M3 scope and contracts are written and frozen.

Likely M3 design topics from the existing roadmap and carried debt include:

- operator/production ergonomics;
- daemon log and Run-log retention;
- credential enrollment/rotation;
- metrics/query scalability;
- service/systemd hardening;
- backup/recovery procedures;
- status/operational surfaces.

M2 should remain a stable accepted baseline while M3 is designed.
