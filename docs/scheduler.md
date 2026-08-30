# Scheduler Semantics v0.1

LMT schedules long-lived mirror synchronization, not stateless batch jobs. The scheduler therefore prefers freshness, predictability, and avoiding duplicate writers over preserving every timer tick.

## 1. Fundamental invariant

At most one non-terminal Run may exist for a Mirror at a time.

LMT v0.1 never intentionally runs two synchronizations of the same Mirror concurrently.

This applies across scheduled and manual triggers.

## 2. Trigger types

v0.1 has two Run trigger classes:

- **scheduled**: created by a cron/interval schedule;
- **manual**: explicitly requested by an operator.

Retries are Attempts inside the same Run, not new trigger events.

## 3. Interval schedule

An interval schedule is completion-relative.

Example:

```toml
[schedule]
interval = "1h"
```

If a Run finishes at 12:37, the next due time is 13:37.

This avoids schedule drift caused by treating an interval as a fixed wall-clock timetable and naturally prevents overlapping interval Runs.

The next interval is measured from the terminal completion of the Run, regardless of whether its final outcome is success or failure. Retries already happen inside the Run.

## 4. Cron schedule

Cron is wall-clock based.

Example:

```toml
[schedule]
cron = "15 * * * *"
timezone = "Asia/Shanghai"
```

If a cron occurrence happens while the same Mirror already has an active Run, that occurrence is skipped.

Rationale: the Mirror is already being synchronized at that moment, and blindly queueing another Run can create an endless back-to-back loop when synchronization duration exceeds the configured cron period.

## 5. Misfires and catch-up

A scheduled occurrence can be missed because execution was impossible even though the Mirror was not already running, for example:

- the owner node was offline;
- the node had no execution capacity;
- the agent rejected dispatch because of a temporary local resource condition;
- the server itself was down across one or more due times.

These missed occurrences are **coalesced**.

The server stores at most one due/catch-up marker for the Mirror. Ten missed timer occurrences do not create ten historical Runs.

When the blocking condition disappears, the scheduler creates one Run using the **latest applied Mirror generation**, clears the catch-up marker, and resumes normal scheduling.

This models the real goal: make the mirror fresh again, not replay every historical timer tick.

## 6. Scheduled trigger while a Run is active

This is different from a misfire caused by unavailability.

If a cron occurrence happens while the Mirror has a Run in progress, the occurrence is simply skipped and does not set the catch-up marker.

Interval schedules cannot naturally overlap because their next due time is based on completion.

## 7. Manual trigger

`lmt mirror sync <name>` creates an explicit operator Run.

If the Mirror already has a non-terminal Run, v0.1 returns a conflict containing the existing Run ID. There is no force-concurrent mode.

If the owner node is temporarily offline, the manual Run may remain Pending and represents durable operator intent. It starts when the node becomes available.

A manual Run freezes the current Mirror generation when it is created.

## 8. Disabled Mirror

`enabled = false` means:

- no new scheduled Runs are generated;
- scheduled catch-up state is cleared;
- new manual sync requests are rejected by default;
- an already-running Run is allowed to finish unless explicitly cancelled;
- a Pending Run that has not started is cancelled when the disabled configuration is applied.

Disabling does not delete mirror data.

## 9. Mirror removal

Removing a Mirror from the authoritative configuration behaves like disabling it for future execution:

- no future schedule exists;
- pending not-yet-started work is cancelled;
- active work may finish;
- no new retry Attempt is created after removal;
- historical Run/Attempt records remain queryable.

Removing management state never removes repository data from disk.

## 10. Configuration update during a Run

A Run is immutable with respect to its configuration generation.

If generation 12 is running and generation 13 is applied:

- the active Run continues with generation 12;
- future scheduled Runs use generation 13;
- a coalesced schedule marker is not tied to an old generation and uses generation 13 when materialized;
- an already-created manual Pending Run remains tied to the generation it explicitly captured.

## 11. Retry semantics

Retries belong to one Run.

```text
Run
  Attempt 1 -> failed
  wait retry_delay
  Attempt 2 -> failed
  wait retry_delay
  Attempt 3 -> succeeded
```

The server owns retry timing and attempt numbering.

The agent never retries autonomously.

If the Mirror is disabled or removed before the next retry is created, retry stops.

## 12. Capacity handling

Node concurrency limits are local policy.

The server uses observed capacity to avoid obviously invalid dispatches, but the agent is the final authority and may reject a StartAttempt because it is temporarily busy.

A temporary capacity rejection does not consume an execution Attempt and does not turn the Run into a failure. The Run remains Pending until dispatch is accepted.

## 13. Scheduler persistence

Scheduler correctness must survive server restart.

The central database stores enough scheduling state to reconstruct:

- next interval/cron due time;
- last schedule evaluation point;
- whether a coalesced catch-up is pending;
- any non-terminal manual Run.

On startup, the server evaluates elapsed schedule time and coalesces missed eligible occurrences rather than replaying one Run per missed tick.

## 14. Summary table

| Situation | v0.1 behavior |
| --- | --- |
| interval Run completes | next due = completion + interval |
| cron fires while same Mirror runs | skip occurrence |
| node offline at due time | set one catch-up marker |
| 10 due times missed while node offline | still one catch-up marker |
| server restarts after missed due times | coalesce to one catch-up |
| manual sync while Mirror active | reject with conflict |
| manual sync while node offline | one Pending manual Run |
| Mirror disabled | stop future scheduling; active Run may finish |
| Mirror removed | prune management; preserve history/data |
| config changes during Run | current Run keeps old generation |
| retry needed | new Attempt under same Run |
