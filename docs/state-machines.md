# State Machines v0.1

This document defines the externally visible Run state and the more detailed internal Attempt state.

The key design rule is:

> Protocol detail belongs to Attempt; operator-facing lifecycle belongs to Run.

This keeps the public model stable while still allowing precise crash recovery.

## 1. Run state

The public Run states are:

```text
Pending
Running
Succeeded
Failed
Cancelled
TimedOut
```

Terminal states are:

```text
Succeeded
Failed
Cancelled
TimedOut
```

## 2. Run state machine

The normal shape is:

```text
             +----------------------+
             |                      |
             v                      |
Pending ---------> Running ---------+
  |                  |
  |                  +--> Succeeded
  |                  +--> Failed
  |                  +--> Cancelled
  |                  +--> TimedOut
  |
  +--------------------> Cancelled
  +--------------------> Failed
```

Examples of `Pending -> Failed` include a permanent execution rejection before any process can start.

## 3. Meaning of `Pending`

A Run is Pending when the operator/scheduler has durable execution intent but the first execution has not begun.

Examples:

- a manual Run exists while its owner node is offline;
- the node is temporarily at its concurrency limit;
- an Attempt has been created but not yet accepted by the agent.

Scheduled catch-up markers are not Runs and therefore do not appear as Pending Runs.

## 4. Meaning of `Running`

A Run becomes Running when its first Attempt is durably accepted/started by an agent.

After a Run has entered Running, it stays Running during:

- retry delay;
- creation of later Attempts;
- temporary dispatch waits for retry Attempts.

It does not oscillate back to Pending between retries.

This makes the operator-facing lifecycle monotonic.

## 5. Final Run result

### Succeeded

Any Attempt succeeds. No later retry occurs.

### Failed

The Run cannot make further progress and the final reason is a normal/permanent failure, including:

- retry attempts exhausted after process failures;
- agent interruption with no retries left;
- permanent RunSpec/local-policy rejection;
- invalid execution result.

### TimedOut

Retry policy is exhausted and the final decisive Attempt ended because its configured execution timeout expired.

### Cancelled

Operator/configuration cancellation wins before success.

A cancellation request is stored separately as intent; the Run does not need a public `Cancelling` state.

## 6. Attempt state

Internal Attempt states are:

```text
Queued
Accepted
Running

Succeeded
Failed
TimedOut
Cancelled
Interrupted
Rejected
```

Terminal Attempt states are:

```text
Succeeded
Failed
TimedOut
Cancelled
Interrupted
Rejected
```

## 7. Attempt state machine

Normal transitions:

```text
Queued
  |
  +--> Accepted
  |      |
  |      +--> Running
  |      |      |
  |      |      +--> Succeeded
  |      |      +--> Failed
  |      |      +--> TimedOut
  |      |      +--> Cancelled
  |      |      +--> Interrupted
  |      |
  |      +--> Succeeded / Failed / TimedOut / Cancelled / Interrupted
  |           (terminal report may arrive before a Running event)
  |
  +--> Cancelled
  +--> Rejected
```

A terminal report may legally skip the observed `Running` transition because network delivery and process completion can race.

## 8. `Queued`

The Attempt exists in the server database and has an immutable RunSpec, but the agent has not durably accepted ownership.

The server may redeliver the same `(run_id, attempt_no)` while it remains Queued.

Temporary dispatch inability does not create a new Attempt.

Examples:

- agent has no free execution slot;
- poll response carrying the action was lost;
- server has not yet observed an acceptance report.

## 9. `Accepted`

The agent has:

1. validated local policy;
2. checked that the same execution key is not already active with a different spec hash;
3. durably recorded ownership in its local spool.

The process may not have emitted a `Running` event yet.

This state closes the critical duplicate-execution window.

## 10. `Interrupted`

Interrupted means execution ownership existed but the attempt could not produce a trustworthy normal process result.

Typical reasons:

- agent daemon crash/restart;
- host reboot;
- supervised process disappears unexpectedly;
- local execution supervision failure.

Interrupted is retryable according to the Run policy.

It is distinct from Failed because it describes infrastructure interruption rather than the synchronization program returning failure.

## 11. `Rejected`

Rejected is a permanent refusal to execute this immutable RunSpec on the selected node.

Examples:

- runner disabled by local policy;
- unsafe target path;
- unsupported execution mode;
- spec hash conflict for an already-known execution key.

A temporary capacity condition is **not** Rejected.

Rejected normally terminates the Run as Failed without consuming pointless retries, because retrying the same immutable spec on the same owner node cannot fix a permanent policy violation.

## 12. Timeout

Timeout is enforced by the agent for each Attempt.

When timeout expires:

1. agent terminates the whole supervised process group;
2. agent durably records TimedOut;
3. logs/result are uploaded;
4. server applies retry policy.

If retries remain, the Run stays Running and a later Attempt may start.

## 13. Cancellation

Cancellation is desired intent stored by the server.

### Pending Run

If no Attempt has been accepted, cancellation can immediately make the Run Cancelled and any Queued Attempt Cancelled.

### Active Attempt

The server repeatedly delivers `CancelAttempt` until the agent confirms a terminal state.

The agent must treat duplicate cancellation commands as harmless.

If the Attempt exits successfully before cancellation takes effect, the server resolves the race using durable event order and cancellation intent. v0.1 rule:

> a success durably completed before the cancellation request is Succeeded; otherwise a cancellation that takes control of the active process results in Cancelled.

## 14. Retry rules

Only the server creates new Attempt numbers.

Retryable Attempt terminal states:

- Failed;
- TimedOut;
- Interrupted.

Non-retryable by default:

- Succeeded;
- Cancelled;
- Rejected.

A new Attempt is created only if all are true:

- Run is not terminal;
- no cancellation is pending;
- Mirror is still managed and enabled;
- `attempt_count < max_attempts`;
- retry delay has elapsed.

## 15. Agent event sequence

Each Attempt event has a monotonically increasing sequence number generated by the owning agent execution record.

The server stores `last_event_sequence`.

Rules:

- lower/equal duplicate sequence: acknowledge and ignore;
- next/newer sequence consistent with state machine: apply;
- newer sequence containing a valid terminal snapshot may skip missing intermediate network events;
- state regression is rejected/ignored and logged.

Every terminal event includes enough timestamps/result information to stand alone.

## 16. Agent instance identity

Every agent process startup generates a new random `agent_instance_id`.

An Attempt records which instance accepted it.

If a new instance reconnects while the server believes an Attempt belongs to an old instance, the new agent uses its spool to report either:

- a previously durable terminal result; or
- Interrupted.

It never silently resumes ownership under the same Attempt without evidence.

## 17. Node liveness

Node liveness is derived, not a persistent state transition machine.

Conceptually:

```text
Online  = now - last_seen <= offline_after
Offline = otherwise
```

The configured threshold should be comfortably larger than the long-poll cycle so one delayed request does not flap Node status.

An offline Node does not cause automatic cross-node execution.

## 18. Rust representation guidance

Public API types should use enums rather than arbitrary strings internally.

Conceptually:

```rust
enum RunState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

enum AttemptState {
    Queued,
    Accepted,
    Running,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
    Rejected,
}
```

Database/API conversion should be explicit and tested exhaustively.

The implementation should centralize state-transition validation rather than scattering direct SQL updates throughout handlers.
