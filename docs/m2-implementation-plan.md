# M2 Implementation Plan

Status: design-ready; implementation not started.

This plan implements the frozen M2 behavior in docs/m2-design.md.

## M2.0 — Persistence and time preflight

Do this before scheduler behavior:

- create ordered migration framework;
- preserve M1 schema as migration 0001;
- add migration 0002;
- move Store execution to a dedicated SQLite background-thread async handle;
- make Store operations async where needed;
- introduce Server-owned time injection for scheduler/retry semantics;
- remove scheduler/retry wall-clock decisions from Store internals.

Acceptance:

- populated M1 DB upgrades in place;
- future schema version is rejected;
- no synchronous SQLite call blocks Axum/Tokio worker threads;
- deterministic tests can pass explicit timestamps;
- M1 fault matrix remains green.

## M2.1 — Core domain/config

Add in lmt-core:

- ScheduleConfig: none, interval, cron;
- human interval duration;
- strict five-field cron validation;
- IANA timezone validation;
- semantic schedule hash;
- RunTrigger enum;
- retry decision;
- scheduler due-state pure functions;
- rsync sync variant;
- rsync RunSpec compilation.

Validation:

- interval 1m through 365d;
- cron five fields;
- M2 extended cron syntax rejected;
- max_attempts 1 through 10;
- retry delay no more than 86400 seconds;
- non-empty rsync source.

Acceptance:

- semantic/canonical schedule tests;
- timezone/DST tests;
- no async/DB dependency enters lmt-core.

## M2.2 — Schema v2 and Store operations

Migration 0002 adds:

- schedule hash;
- Node max concurrency;
- Run scheduled-for time;
- Run retry-due time;
- scheduler/retry indexes.

Implement Store operations for:

- earliest wakeup;
- schedule tick persistence;
- cancellation intent;
- retry deadline;
- dispatch candidate selection;
- Scheduled Run materialization.

Acceptance:

- transaction boundaries match M2 Design;
- one-active-Run DB invariant remains authoritative;
- no in-memory task queue.

## M2.3 — Retry

Implement Attempt terminal to Run decision using lmt-core.

Flow:

~~~
Attempt terminal
-> apply event
-> terminal or retry decision
-> persist Run terminal or retry_due
-> notify
~~~

Retry Attempt is created only when deadline has arrived and owner Agent polls with a free slot.

Acceptance:

- Failed retry;
- TimedOut retry;
- Interrupted retry;
- Rejected no retry;
- attempts exhausted;
- disable/remove/move suppress retry;
- Server restart during delay;
- same Run ID, increasing Attempt numbers.

## M2.4 — Cancellation

Server:

- cancel endpoint;
- CLI cancel command;
- persistent cancellation intent;
- immediate cancellation before dispatch;
- CancelAttempt priority for dispatched work.

Protocol:

~~~
CancelAttempt {
  run_id,
  attempt,
  spec_hash
}
~~~

Agent:

- per-Attempt cancellation control;
- process-group cancellation;
- durable cancel-before-Start tombstone;
- duplicate cancel idempotency;
- terminal-before-cancel preservation.

Acceptance:

- pending cancel;
- retry-delay cancel;
- active process cancel;
- descendant cleanup;
- duplicate/lost cancel;
- Cancel before delayed Start;
- conflicting hash after tombstone;
- Agent restart with tombstone.

## M2.5 — Scheduler

Implement one Server scheduler task.

Responsibilities:

- calculate/persist due state;
- wake at earliest schedule/retry deadline;
- coalesce missed occurrences;
- never execute commands itself;
- notify long-poll waiters.

Implement interval first, then cron.

Acceptance:

- fake clock only;
- interval completion-relative;
- manual Run resets interval;
- cron active skip;
- missed cron coalescing;
- downtime recovery;
- schedule reset semantics;
- move behavior;
- latest generation at materialization.

## M2.6 — Agent capacity

Poll capacity adds max_concurrent_runs.

Server offers Start only when free capacity exists.

Cancel ignores capacity.

Acceptance:

- full Agent gets no new Start;
- free slot releases manual/retry/scheduled work;
- full Agent still receives cancel.

## M2.7 — Built-in rsync

Add:

~~~toml
[sync]
type = "rsync"
source = "..."
args = [...]
~~~

Compile to normal process RunSpec:

~~~
rsync <args> -- <source-exactly> <target_dir>/
~~~

No Agent rsync-specific code.

Tests use temporary local directories or local rsync daemon only.

Acceptance:

- source trailing slash preserved;
- destination treated as directory;
- central logs work;
- nonzero rsync result uses ordinary retry;
- command Mirrors unchanged.

## M2.8 — API, CLI, metrics

Expose:

- Mirror next due / due since;
- Run trigger;
- scheduled_for;
- retry_due;
- cancel_requested;
- Node max concurrency.

CLI adds lmt run cancel and useful show fields.

Metrics add scheduler/retry/cancel aggregate counters and gauges without per-Run labels.

## M2.9 — Final fault matrix

Extend the M1 harness.

Required end-to-end cases:

1. interval scheduled Run;
2. cron Run with injected time;
3. Node offline across multiple due occurrences;
4. Server restart with one due marker;
5. retry creates Attempt 2;
6. retry delay across restart;
7. Agent crash to Interrupted to retry;
8. disable during retry delay;
9. active cancel with descendants;
10. cancel-before-delayed-Start;
11. duplicate cancel;
12. config generation changes while due;
13. local rsync success/failure;
14. all M1 cases.

## Completion

M2 is complete only when:

- every M2 invariant has automated coverage;
- M1 gates remain green;
- M1-to-M2 DB migration is tested;
- no scheduler/retry correctness depends on in-memory state;
- no automatic cross-node behavior exists;
- docs match implementation.

After M2, a small mirror site should be able to operate normal periodic command/rsync mirrors without external cron.
