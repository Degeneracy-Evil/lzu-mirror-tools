# M2 Design — Scheduling, Retry, Cancellation, and rsync

Status: frozen for M2 implementation.

This document is the authoritative M2 behavioral specification. If an older generic design document still contains M1-era wording that conflicts with this file, this file wins for M2. After implementation stabilizes, the narrower documents should be reconciled to match it.

M1 established the safe distributed execution substrate. M2 turns that substrate into an unattended mirror manager.

The M2 goal is:

> A small mirror site should be able to replace cron jobs with LMT while preserving every M1 execution, idempotency, and crash-recovery guarantee.

M2 adds scheduling, retry, cancellation, built-in rsync configuration sugar, persistent scheduler recovery, and the small persistence/runtime changes needed to support those features safely.

M2 does not add automatic node placement, cross-node failover, container execution, storage pools, snapshots, workflow/DAG execution, PostgreSQL/controller HA, plugin APIs, or rsync-output parsing.

## 1. M1 invariants remain binding

M2 must preserve all accepted M1 invariants:

1. One Mirror has at most one non-terminal Run.
2. One execution key, defined as run_id plus attempt_no, has at most one Agent execution.
3. Command delivery may repeat safely.
4. A Run is tied to one immutable Mirror generation.
5. Agent disappearance never authorizes execution on another node.
6. The control plane is never in the client download path.
7. All queryable business state remains centralized.
8. Run logs remain centrally retrievable.
9. Config pruning never deletes mirror data.
10. Local Agent policy remains authoritative.

M2 complexity must be layered on top of these rules rather than bypassing them.

## 2. M2 preflight: asynchronous SQLite boundary

M1 uses synchronous rusqlite calls directly from async Server handlers. That was acceptable for the M1 vertical slice, but scheduler activity should not build on that boundary.

M2 keeps exactly one central SQLite connection, but executes it on a dedicated background thread through an asynchronous handle.

Preferred current implementation: tokio-rusqlite.

Architecture:

~~~
Axum handlers / scheduler
          |
          | await Store operation
          v
async Store handle
          |
          v
dedicated SQLite thread
          |
          v
single SQLite connection
~~~

Rules:

- still one authoritative SQLite DB;
- no pool;
- no PostgreSQL;
- lmt-store remains the only crate that knows the schema;
- lmt-store may become async;
- lmt-core stays synchronous and has no Tokio/SQLite dependency;
- Server code receives semantic Store methods rather than raw connection access.

This solves blocking-runtime behavior without changing the database architecture.

## 3. M2 preflight: ordered migrations

M2 is the first schema-changing milestone.

Use embedded ordered migrations:

~~~
crates/lmt-store/migrations/
  0001_m1.sql
  0002_m2.sql
~~~

The migration runner must:

1. read the highest applied schema version;
2. refuse to start if the database version is newer than the binary;
3. apply missing migrations in ascending order;
4. apply each migration transactionally;
5. record the version only after success.

No downgrade migration is required.

The existing M1 schema becomes migration 0001 without semantic changes.

Required tests:

- empty DB to latest;
- populated M1 DB to M2;
- failed migration rollback;
- future DB version startup refusal.

## 4. Server clock owns scheduler correctness

Scheduler and retry deadlines must not depend on Agent clocks.

The Server owns all future-deadline calculations.

Production uses system UTC. Tests use explicit/manual time.

Store methods whose semantics depend on time receive now_ms from the Server service layer. The Store must not independently invent scheduler or retry deadlines from its own wall clock.

Agent timestamps remain execution observations only.

Consequences:

- retry_due is ServerNow plus retry delay;
- interval next_due is Server terminal-processing time plus interval;
- cron evaluation is based on Server UTC plus configured timezone.

This prevents clock skew on an Agent from changing control-plane behavior.

## 5. TOML schedule schema

A Mirror has either no schedule, one interval schedule, or one cron schedule.

### 5.1 Manual-only

No schedule section means manual-only.

### 5.2 Interval

Example:

~~~toml
[schedule]
interval = "2h"
~~~

Rules:

- minimum 1 minute;
- maximum 365 days;
- human duration syntax;
- semantic canonicalization should make equivalent durations stable where practical.

### 5.3 Cron

Example:

~~~toml
[schedule]
cron = "15 * * * *"
timezone = "Asia/Shanghai"
~~~

Rules:

- exactly five fields;
- minute granularity;
- timezone required;
- timezone must be an IANA name;
- never use machine-local timezone implicitly;
- aliases such as @hourly are rejected in M2;
- guaranteed syntax is the normal Vixie/POSIX subset: wildcard, lists, ranges, steps, numbers, month names, weekday names;
- extended L, W, #, +, and ? syntax is rejected in M2;
- day-of-month and day-of-week use normal Vixie OR semantics.

The intended evaluator is Croner with chrono-tz, but the public contract is the restricted LMT syntax above, not every feature supported by the dependency.

## 6. Cron DST contract

DST behavior is explicit and tested.

Spring-forward gap:

- fixed-time jobs that fall inside the gap execute at the first valid minute immediately after the gap on the same calendar day;
- wildcard/step occurrences inside the nonexistent interval are skipped.

Fall-back overlap:

- fixed-time jobs execute once, at the first wall-clock occurrence;
- wildcard/step schedules may match both passes through the duplicated interval.

Tests must use a DST-observing timezone such as America/New_York.

## 7. The scheduler persists due intent, not a Run queue

This is the central M2 simplification.

When a schedule becomes due, the scheduler does not immediately create a Run.

Instead it persists one coalesced due marker:

~~~
catch_up_pending = true
catch_up_since_ms = earliest unserved due time
~~~

The historical column name catch_up_pending remains for schema compatibility, but the M2 semantic meaning is broader:

> One scheduled synchronization is currently due.

That same state covers:

- an on-time occurrence waiting for Agent poll;
- Node offline;
- Agent full;
- Server restart;
- many cron misses.

There is never one queued Run per timer tick.

## 8. Scheduled Runs materialize on Agent poll

A Scheduled Run is created only when the owning Agent polls and has free execution capacity.

Flow:

~~~
wall-clock due
    |
    v
persistent due marker
    |
    | may wait through offline/capacity outage
    v
Agent poll with free slot
    |
    v
one transaction:
  verify Mirror eligible
  verify no active Run
  read latest generation
  create Scheduled Run
  snapshot Run policy
  create Attempt 1
  mark dispatch
  clear due marker
    |
    v
StartAttempt
~~~

Benefits:

- no large scheduled Pending queue;
- missed occurrences naturally coalesce;
- delayed scheduled work uses the latest Mirror generation;
- capacity is evaluated at dispatch time;
- lost StartAttempt responses remain ordinary M1 re-delivery.

Manual Runs remain real durable Pending Runs because they represent explicit operator intent.

## 9. Schedule runtime state

Logical fields:

~~~
mirror_name
schedule_hash
next_due_at_ms
last_evaluated_at_ms
catch_up_pending
catch_up_since_ms
~~~

schedule_hash is a semantic fingerprint of schedule configuration only.

next_due_at_ms is the next future occurrence to evaluate.

catch_up_since_ms is the earliest occurrence represented by the single due marker.

When a Scheduled Run is materialized, its scheduled_for_at is catch_up_since_ms, not materialization time.

## 10. Config apply and schedule state

Config apply remains configuration reconciliation, not an execution command.

Therefore applying config does not immediately start synchronization.

New, re-enabled, changed, or moved interval schedule:

~~~
next_due = apply_time + interval
catch_up = false
~~~

New, re-enabled, changed, or moved cron schedule:

~~~
next_due = first matching occurrence strictly after apply_time
catch_up = false
~~~

Schedule removed or Mirror disabled:

- clear active schedule runtime state.

Schedule semantic hash unchanged:

- preserve runtime schedule state.

Unrelated source, args, timeout, description, or other non-schedule change:

- do not force an immediate sync.

If a due marker already exists and other config changes, eventual Scheduled Run uses the latest generation.

A node move resets schedule timing on the new owner but does not run immediately. An operator can explicitly request immediate bootstrap with the CLI.

## 11. Interval semantics

After activation, interval scheduling is completion-relative.

Example:

~~~
interval = 2h
Run terminal processed by Server at 12:37
next_due = 14:37
~~~

Any terminal Run on the current owner becomes the freshness anchor:

- Scheduled or Manual;
- Succeeded;
- Failed;
- TimedOut;
- Cancelled.

Retries belong to the same Run, so interval is re-armed only after the whole Run becomes terminal.

If a Run finishes on an old owner after the Mirror was moved, that old-owner Run does not re-arm the new owner schedule.

When interval due arrives:

- active Run exists: skip that due point; the active Run will re-arm the interval when terminal;
- no active Run: set one due marker and set next_due NULL until a Run materializes and later terminates.

## 12. Cron semantics

Cron is wall-clock based.

At each due occurrence:

If no active Run:

- set or retain one due marker;
- advance next_due to the next future occurrence.

If a non-terminal Run already exists:

- skip the occurrence;
- do not create catch-up debt;
- advance next_due.

If several occurrences elapsed during Server downtime:

- if a Run is currently active, elapsed occurrences are skipped;
- otherwise one due marker represents all elapsed occurrences;
- catch_up_since keeps the earliest represented occurrence;
- next_due jumps directly to the next future occurrence rather than replaying every minute.

## 13. Manual Run interaction

Creating a manual Run clears one existing scheduled due marker for the same Mirror.

Reason:

> Explicit operator synchronization supersedes one pending scheduler freshness request.

For interval schedules, terminal completion of that manual Run re-arms interval.

For cron schedules, future wall-clock cron timing is unchanged.

A manual Run still freezes the Mirror generation at creation time.

## 14. Scheduler task

The Server has one explicit scheduler task.

It is wakeup-driven with a safety bound.

Conceptually:

~~~
query earliest:
  schedule next_due
  retry_due

sleep until earliest deadline
OR Notify
OR safety wake, no longer than about 30s

evaluate using Server clock
persist due transitions
notify Agent poll waiters
repeat
~~~

Notify is only a latency optimization.

Correctness remains reconstructible entirely from SQLite after restart.

Relevant operations notify the scheduler/poll waiters:

- config apply;
- manual Run creation;
- cancellation request;
- Agent poll or reconnect;
- Attempt terminal event;
- schedule due transition;
- retry deadline.

## 15. Agent capacity becomes explicit

Poll capacity adds:

~~~
active_runs
max_concurrent_runs
mirror_root_free_bytes
~~~

The Server must not offer a new StartAttempt when active_runs is greater than or equal to max_concurrent_runs.

Cancellation is never blocked by capacity.

The Agent remains the final local-policy authority.

## 16. Poll action priority

Each poll returns at most one control action.

Priority:

1. CancelAttempt for cancellation already requested on dispatched non-terminal work.
2. Re-delivery of an already-dispatched StartAttempt.
3. Initial dispatch for an existing manual Run.
4. Retry dispatch whose retry_due is reached.
5. Materialization and dispatch of the oldest Scheduled due marker on that node.

Safety and explicit durable intent take priority over new schedule work.

One action per poll keeps the protocol and transaction logic simple. Agents with more free slots poll again immediately.

## 17. Retry policy

M2 enables multi-Attempt policy.

Example:

~~~toml
[run]
timeout_seconds = 21600
max_attempts = 3
retry_delay_seconds = 300
~~~

Validation:

- max_attempts from 1 through 10;
- retry_delay_seconds from 0 through 86400;
- timeout_seconds remains from 1 through 604800.

A Run snapshots these values when created. Later config changes do not mutate that Run.

## 18. Retry is a Run deadline, not another queue

After a retryable Attempt ends:

~~~
Attempt N terminal
     |
     +-- retry forbidden/exhausted --> Run terminal
     |
     +-- retry allowed
             |
             v
        Run stays Running
        retry_due_at = ServerNow + delay
~~~

No new Attempt exists during the delay.

When the deadline has arrived and the owner Agent polls with free capacity, the Server atomically creates Attempt N+1 using the same frozen Run generation.

This means Server restart during retry delay requires no reconstruction beyond reading retry_due_at.

## 19. Retryable outcomes

Retryable:

- Failed;
- TimedOut;
- Interrupted.

Not retryable:

- Succeeded;
- Cancelled;
- Rejected;
- permanent InvalidResult/protocol failure.

Retry requires all of:

- attempt_no less than max_attempts;
- no explicit cancellation;
- Mirror still managed;
- Mirror enabled;
- current Mirror owner equals Run owner;
- Run non-terminal.

If retry becomes ineligible, Run finalizes according to the last Attempt outcome.

Examples:

~~~
TimedOut -> retry -> Succeeded
Run = Succeeded

TimedOut -> retry -> Failed with attempts exhausted
Run = Failed

Interrupted -> Mirror moved before retry
no retry
Run = Failed

Failed -> Mirror disabled before retry
no retry
Run = Failed
~~~

## 20. Retry deadline uses Server time

retry_due_at is:

~~~
ServerNow when terminal event is applied
+ retry delay
~~~

Do not derive it from Agent finished_at.

Agent timestamps remain history only.

## 21. Explicit cancellation

M2 implements:

~~~
POST /api/v1alpha1/runs/{run_id}/cancel
lmt run cancel <run_id>
~~~

Cancellation is intrinsically idempotent by Run ID.

Server stores cancel_requested_at once. Repeated requests do not move the timestamp.

No cancellation request ID is required.

## 22. Cancellation before dispatch

If no Attempt was ever dispatched:

- any undispatched Queued Attempt can be marked Cancelled;
- Run becomes Cancelled immediately;
- retry deadline clears;
- no Agent message is required.

A Run waiting between retries can also be cancelled immediately because no active dispatched Attempt exists.

## 23. Cancellation after dispatch

Once dispatch_count is greater than zero, the Server must assume the Attempt may exist on the Agent even if Server state is still Queued.

Run remains non-terminal with cancel intent.

Server repeatedly offers:

~~~
CancelAttempt {
  run_id,
  attempt,
  spec_hash
}
~~~

until terminal reconciliation.

Cancel action priority is above all Start actions.

## 24. Cancel-before-Start reordering

This is a release-gating M2 invariant.

Possible history:

~~~
Server dispatches StartAttempt
Start response is delayed or lost

operator cancels

Agent receives CancelAttempt before it has ever seen StartAttempt
~~~

Agent must persist a cancellation tombstone keyed by:

~~~
run_id
attempt_no
spec_hash
~~~

If the delayed Start later arrives:

- same hash: never execute; reconcile Cancelled;
- different hash: protocol-integrity error; never execute.

The exact local Rust representation is implementation detail. The durable behavior is mandatory.

## 25. Active cancellation

For an active execution:

1. persist local cancellation intent;
2. signal the Attempt-specific cancel control;
3. TERM the Attempt process group;
4. wait the grace interval;
5. KILL the process group if required;
6. reap;
7. persist Attempt Cancelled;
8. upload remaining logs and terminal event.

Repeated CancelAttempt is harmless.

## 26. Cancellation race with natural completion

Do not resolve this using Server/Agent wall-clock timestamps.

Agent durable local observation order wins:

- if the process result was already durably terminal before CancelAttempt was processed, preserve that result;
- otherwise cancellation takes control and the Attempt becomes Cancelled.

Therefore a cancel request can return an eventual Succeeded or Failed if the process naturally completed just before cancellation reached the Agent.

A cancellation request always suppresses future retries.

## 27. Disable/remove is different from explicit cancel

Config disable/removal does not implicitly terminate already-dispatched work.

Rules:

- initial Pending Run with no dispatch: Cancelled;
- Run waiting between retries: stop retry and finalize from the previous Attempt;
- already-dispatched Attempt: allowed to finish;
- after it finishes, no further retry;
- success remains Succeeded;
- failure/timeout/interruption finalizes normally.

Config reconciliation never sends an implicit CancelAttempt for dispatched work.

## 28. Built-in rsync

M2 adds thin rsync configuration sugar.

Example:

~~~toml
[sync]
type = "rsync"
source = "rsync://archive.example.org/project/"
args = [
  "--archive",
  "--hard-links",
  "--delete",
  "--numeric-ids",
]
~~~

Important principle:

> LMT does not hide a large opinionated rsync option set.

The operator-visible TOML contains rsync options.

LMT only supplies:

- program = rsync;
- configured args in order;
- option terminator;
- configured source;
- destination target_dir with trailing slash.

Conceptually:

~~~
rsync <configured args...> -- <source exactly as configured> <target_dir>/
~~~

No shell.

## 29. rsync path semantics

The source string is preserved exactly, including whether it has a trailing slash.

LMT must not normalize source/path and source/path/ into one another because rsync gives them different meanings.

The destination is always the Mirror target directory and is rendered as a directory path with trailing slash.

Complex multi-source rsync invocations can use sync.type = command instead.

M2 does not parse rsync statistics into structured metrics.

## 30. Public model additions

Run trigger becomes typed:

- Manual;
- Scheduled.

Run status adds nullable:

- scheduled_for_at;
- retry_due_at;
- cancel_requested_at.

Node observed capacity adds max_concurrent_runs.

CancelAttempt adds spec_hash.

Public Run state remains unchanged.

## 31. Schema version 2

M2 conceptually adds:

~~~
mirror_schedule_state.schedule_hash

nodes.max_concurrent_runs

runs.scheduled_for_at_ms
runs.retry_due_at_ms
~~~

plus indexes for earliest schedule and retry wakeups.

No queue table is needed.

The existing catch_up_pending and catch_up_since_ms columns remain.

## 32. Transaction boundaries

Schedule due evaluation:

- read current schedule state;
- check active Run;
- update due marker and next occurrence;
- commit.

Scheduled materialization:

- verify Agent capacity at decision boundary;
- verify Mirror eligible/current owner;
- verify due marker;
- verify no active Run;
- create Scheduled Run with latest generation;
- snapshot policy;
- create Attempt 1;
- mark dispatch;
- clear due marker;
- commit.

Terminal Attempt event:

- update Attempt;
- evaluate retry versus terminal;
- set retry deadline or terminal Run;
- re-arm interval where eligible;
- commit.

Retry dispatch:

- verify retry deadline;
- verify no cancel;
- verify current owner/eligibility;
- create Attempt N+1;
- clear retry deadline;
- mark dispatch;
- commit.

Cancellation:

- set cancellation intent;
- if safe before dispatch, terminalize immediately;
- otherwise wait for Agent cancellation;
- commit.

## 33. Dispatch planner

The database is the durable queue.

Do not introduce an in-memory job queue.

Candidate classes:

~~~
cancel
existing Start redelivery
manual initial
retry due
scheduled due
~~~

Selection and mutation for one returned action must be transactional.

This is a major concurrency boundary and must have race tests.

## 34. CLI and API minimum

M2 adds:

~~~
lmt run cancel <run-id>
~~~

Mirror show should include next due and scheduled-due-since.

Run show should include retry due, cancel requested, trigger, scheduled_for, and Attempts.

Run list should support trigger filtering when practical.

No separate scheduler administration command is required. Scheduler behavior comes entirely from TOML desired state.

## 35. Metrics

M2 should move the growing metrics endpoint onto the existing Prometheus registry/library rather than keep hand-building exposition text.

Useful aggregate metrics:

~~~
lmt_runs_pending
lmt_runs_running
lmt_nodes_online

lmt_scheduler_occurrences_total{kind,outcome}
lmt_retries_scheduled_total{reason}
lmt_attempts_terminal_total{state}
lmt_cancellations_total{outcome}

lmt_agent_polls_total
lmt_log_uploaded_bytes_total
lmt_log_upload_failures_total
~~~

Do not add per-Run labels.

## 36. Summary table

| Situation | M2 behavior |
| --- | --- |
| interval enabled | next due = apply + interval |
| interval Run terminal | next due = Server terminal time + interval |
| cron due while idle | one due marker, advance cron |
| cron due while active | skip, advance cron |
| Node offline | due marker waits |
| Agent full | due marker waits |
| many cron misses | still one marker |
| config changes while due | later Scheduled Run uses latest generation |
| manual Run created while due | manual clears due marker |
| retryable failure | Run remains Running, retry_due persisted |
| Server restart in retry delay | deadline recovered |
| disabled/removed/moved before retry | no retry |
| cancel before dispatch | immediate Run Cancelled |
| cancel after dispatch | repeated CancelAttempt |
| Cancel arrives before delayed Start | tombstone blocks execution |
| old-owner Run finishes after move | does not re-arm new-owner interval |
| rsync source trailing slash | preserved exactly |

## 37. M2 release gates

M2 is not complete until automated tests prove:

1. deterministic interval scheduling;
2. deterministic timezone-aware cron;
3. DST edge behavior;
4. cron active-Run skip;
5. many missed cron occurrences coalesce;
6. Server restart recovers due/retry state;
7. offline/full Agent leaves one due intent;
8. Scheduled materialization uses latest generation;
9. retries stay inside one Run and increment Attempt number;
10. retry deadline survives restart;
11. Rejected never retries;
12. disable/remove/move suppress future retry;
13. cancel before dispatch is immediate;
14. active cancellation kills descendants;
15. cancel-before-delayed-Start cannot execute;
16. cancellation is idempotent;
17. rsync compiles to the same native process executor;
18. local rsync integration uses no internet;
19. every accepted M1 fault test remains green;
20. one-writer/idempotency invariants remain intact.

Only then may M2 be accepted.
