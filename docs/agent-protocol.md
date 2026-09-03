# Server-Agent Protocol v0.1

This document defines the intended reliability semantics between `lmt-server` and `lmt-agent`.

The protocol is deliberately simple: HTTP + JSON, initiated by the agent.

## 1. Why agent-initiated HTTP

Mirror nodes are expected to live on trusted or semi-trusted infrastructure networks, often behind firewalls or NAT.

The server therefore never needs to open a connection to an agent.

```text
agent -----> server
```

This gives:

- simple firewall rules;
- trivial reconnection after network loss;
- easy debugging with normal HTTP tools;
- no required gRPC/WebSocket infrastructure;
- no persistent in-memory session required for correctness.

## 2. Authentication

Each node has an independent random bearer token.

Example:

```http
Authorization: Bearer <node-token>
```

The server stores only a suitable verifier/hash.

TLS may be terminated directly by LMT or by a reverse proxy. A trusted-LAN deployment may initially run plain HTTP, but authentication remains part of the protocol so that the design does not assume an always-trusted network.

mTLS, OIDC, SPIFFE, and similar systems are intentionally outside v0.1.

## 3. Control long poll

The primary control endpoint is conceptually:

```text
POST /api/v1/agent/poll
```

The request acts as both a heartbeat and an observed-state report.

Example request shape:

```json
{
  "node": "mirror02",
  "agent_version": "0.1.0",
  "poll_sequence": 3812,
  "running": [
    {
      "run_id": "01K...",
      "attempt": 1,
      "state": "running"
    }
  ],
  "capacity": {
    "mirror_root_free_bytes": 123456789,
    "active_runs": 1
  }
}
```

If the server has no action, it may hold the request for a bounded period (for example 20-30 seconds) and then return `204 No Content`.

If there is work, it returns immediately.

Example:

```json
{
  "actions": [
    {
      "type": "start_attempt",
      "run_id": "01K...",
      "attempt": 1,
      "spec_hash": "sha256:...",
      "spec": {
        "runner": "process",
        "program": "rsync",
        "args": ["..."],
        "cwd": "/srv/mirrors/ubuntu",
        "timeout_seconds": 21600
      }
    }
  ]
}
```

After any response the agent immediately begins another poll.

This provides near-immediate dispatch without maintaining a custom streaming protocol.

## 4. State reports

Run/Attempt transitions should be reported independently from the long poll so that completion is not delayed by an outstanding poll.

Conceptually:

```text
POST /api/v1/agent/attempts/report
```

Reports carry the execution key:

```text
(run_id, attempt)
```

and a monotonically increasing local event sequence for that attempt.

Example:

```json
{
  "run_id": "01K...",
  "attempt": 1,
  "event_sequence": 4,
  "state": "finished",
  "exit_code": 0,
  "started_at": "...",
  "finished_at": "..."
}
```

The server must accept duplicate reports idempotently.

## 5. Delivery semantics

The protocol intentionally provides **at-least-once delivery of commands** and **idempotent execution**.

It does not attempt exactly-once message delivery.

A `start_attempt` response may be lost after the agent receives it. Therefore the server may return the same action again.

The agent must guarantee:

> the same `(run_id, attempt)` is never started twice.

The acceptance order is:

```text
receive StartAttempt
       |
       v
validate local policy
       |
       v
durably record ACCEPTED
       |
       v
start process
       |
       v
report RUNNING
```

Persisting acceptance before process creation closes the most important duplicate-execution window.

## 6. Attempt ownership

The idempotency key is:

```text
(run_id, attempt_number)
```

Retries use a new attempt number under the same Run.

Example:

```text
(01KABC, 1)  failed
(01KABC, 2)  running
```

The agent never invents attempt numbers and never retries by itself.

The server owns retry policy.

## 7. Agent durable spool

Agents do not run a local database.

Instead, each agent keeps a deliberately small durable spool of ordinary files, for example:

```text
/var/lib/lmt-agent/spool/
└── <run-id>/
    └── <attempt>/
        ├── state.json
        ├── result.json
        └── log.offset
```

The spool exists only to make execution/retransmission safe across an agent restart. It may record:

- execution key;
- RunSpec hash;
- accepted timestamp;
- process identity;
- current local state;
- terminal result;
- last uploaded log offset;
- whether the terminal result has been acknowledged by the server.

Writes that change execution ownership must use an atomic durable pattern (temporary file, fsync, rename, and directory fsync where required).

The spool is **not** authoritative/queryable deployment state. All Mirrors, Nodes, Runs, Attempts, and historical indexes live in the central server database.

## 8. Agent crash and automatic restart semantics

The agent is a long-running system daemon and should recover automatically from ordinary process failures.

The official systemd unit should use restart-on-failure with a short backoff and may use the systemd watchdog. Operators should not normally need to manually restart a crashed agent.

v0.1 prioritizes safety over preserving an execution across an agent-daemon crash. Attempt subprocesses must be supervised as part of the agent service/process group so that an agent unit failure/restart cannot leave an unmanaged writer behind.

After systemd restarts the agent, it scans the durable spool:

- a terminal `result.json` that was not acknowledged is retransmitted;
- an accepted/running attempt without a terminal result is reported as `interrupted`;
- the server decides whether to create a new attempt according to the Run retry policy.

Retries use a new attempt number. This preserves idempotence and prevents concurrent duplicate writers.

A full node reboot follows the same logical recovery path.

A future execution shim or independent systemd transient-runner model may preserve running work across an agent restart, but that complexity is not required for v0.1.

## 9. Server crash semantics

The server must commit a Run/Attempt to SQLite before dispatching it.

If the server crashes:

1. agents continue or finish currently owned executions;
2. agents keep terminal reports in the local durable spool until acknowledged;
3. systemd should automatically restart the server after an ordinary process failure;
4. after server restart, agents reconnect by polling;
5. agents resend observed state and unacknowledged terminal reports;
6. the server reconstructs current state from its durable database plus agent reports.

No agent execution depends on an in-memory server session.

## 10. Network partition semantics

A node missing several poll windows is marked offline/stale.

However:

> node offline does not mean its running attempt is safe to duplicate.

If a node disappears while an attempt is running, the server does **not** automatically start that same mirror elsewhere.

The run remains unresolved/stale until:

- the node returns and reports a result;
- the operator explicitly resolves/cancels it;
- a future fencing mechanism proves the old writer cannot continue.

This rule prevents duplicate writers and is more important than automatic failover for mirror infrastructure.

## 11. Pending work when a node is offline

Because v1 placement is explicit, if a mirror's node is offline:

- scheduled Runs may remain Pending;
- no other node automatically receives them;
- operators can see that the blocking reason is node unavailability.

The scheduler may coalesce repeated scheduled triggers so that a long outage does not create hundreds of useless pending Runs. The exact coalescing policy is an implementation detail to define before scheduler implementation.

## 12. Cancellation

Cancellation is also state-reconciled rather than fire-and-forget.

The server records cancellation intent and may repeatedly return:

```json
{
  "type": "cancel_attempt",
  "run_id": "01K...",
  "attempt": 1
}
```

until the agent reports a terminal state.

Receiving the same cancellation more than once must be harmless.

The agent should cancel the whole process group, not only the direct child process.

## 13. Local policy validation

An agent must validate every RunSpec against local policy before accepting it.

RunSpecs contain already-resolved execution values. LMT-specific runtime context is not communicated to child processes through implicit environment variables; any such value must have been explicitly referenced by the Mirror TOML and compiled into the RunSpec.

Examples include:

- target path must remain under the configured mirror root;
- process runner must be enabled;
- command/container use may be restricted;
- local concurrency limit must not be exceeded;
- configured path constraints must be respected.

A server request that violates policy is rejected with a structured reason.

The controller is not allowed to bypass local safety policy.

## 14. Protocol invariants

1. Commands are at-least-once; execution is idempotent.
2. `(run_id, attempt)` identifies exactly one execution attempt.
3. Acceptance is durable before process start.
4. Terminal results remain durable until the server acknowledges them.
5. Node disappearance never automatically implies permission to duplicate a writer.
6. Server restart cannot erase an already-created Run.
7. Agent restart cannot silently forget an accepted attempt.
8. Retry policy belongs to the server.
9. Local policy belongs to the agent.
10. Protocol correctness does not depend on a persistent TCP session.


## 15. Run log transport

Run stdout/stderr must be centrally retrievable even when an operator is not logged into the execution node.

The agent therefore captures output locally and uploads it incrementally to the server.

A conceptual endpoint is:

```text
PUT /api/v1alpha1/agent/attempts/{run_id}/{attempt}/log
```

Log upload uses byte offsets or chunk sequence numbers so retransmission is idempotent. The server acknowledges the highest durably stored offset.

The server stores the log bytes in its central log directory rather than inside SQLite. The database stores only metadata such as:

- run/attempt key;
- log path or object key;
- stored byte count;
- completion state;
- optional checksum.

The local agent copy is a temporary spool and may be removed after the server has acknowledged both the terminal result and all log bytes.

This built-in central Run log path is distinct from daemon observability logs. `tracing` output from `lmt-server` and `lmt-agent` still goes to journald and may optionally be collected by Loki.


## 16. M2 Agent capacity

Poll capacity additionally reports max_concurrent_runs.

The Server does not offer new StartAttempt work while active_runs is greater than or equal to max_concurrent_runs. Cancellation remains deliverable regardless of capacity.

## 17. M2 CancelAttempt

Cancel action becomes:

~~~text
CancelAttempt {
  run_id,
  attempt,
  spec_hash
}
~~~

The spec hash protects against message reordering and integrity conflicts.

If an Agent receives CancelAttempt for an execution key it has never seen, it persists a durable cancellation tombstone containing the key and hash. A later StartAttempt with the same hash is never executed and reconciles Cancelled. A later StartAttempt with a different hash is a protocol-integrity error and is never executed.

For an active Attempt, cancellation is persisted locally, then the Agent terminates the Attempt-specific process group and records Attempt Cancelled. Duplicate CancelAttempt messages are harmless.

## 18. M2 action priority

Each poll returns at most one action.

Priority:

1. cancellation;
2. already-dispatched Start redelivery;
3. manual initial dispatch;
4. retry dispatch;
5. Scheduled due materialization.

The database remains the durable action source. Long-poll memory state is never authoritative.

## 19. M2 retry responsibility

The Agent never calculates retry deadlines and never creates retry Attempts.

Retry remains entirely a Server decision. The Agent only executes immutable Attempt RunSpecs and reports results.


## 20. Cancellation tombstone retirement

A Cancel-before-Start tombstone is durable recovery state, not permanent history.

It must remain until both:

- the Server has acknowledged the terminal Cancelled Attempt event;
- the empty or remaining Run log completion has been acknowledged.

After both acknowledgements, the Agent may retire the tombstone because the authoritative Server Attempt is terminal and StartAttempt will no longer be dispatched.

The tombstone must still survive Agent restart before that acknowledgement boundary.


## 21. M3 durable Agent installation identity

The Agent presents a durable installation identity that survives process restart and credential rotation.

A separate boot/process identifier may be sent for diagnostics.

The durable identity is state under the Agent spool/state directory, not user-authored placement configuration.

## 22. M3 Node binding fence

After bearer authentication, the Server verifies the presented durable Agent identity matches nodes.bound_agent_id.

If the Node is unbound, the first M3 Agent may bind it atomically.

If the identity differs:

~~~text
409 agent_binding_conflict
~~~

and the Server returns no StartAttempt or CancelAttempt.

A credential alone cannot bypass the binding fence.

## 23. Credential authentication metadata

Agent authentication resolves:

~~~text
node
credential_id
~~~

The successful poll path may update last_used_at with write throttling.

Events/log uploads need not generate heartbeat-style credential writes.

Revocation causes future authenticated requests to fail.

## 24. Credential reload

Agent SIGHUP/systemd reload re-reads only the configured credential file and safe logging-related state.

It does not terminate active Attempts or reload arbitrary runner/storage policy.

If credential reload fails, the Agent keeps the previous credential and reports the error.

## 25. M3 local single-instance rule

The Agent must own an exclusive local state/spool lock before recovery and polling.

This complements, but does not replace, the Server-side durable Agent binding.

## 26. M4 publication capability and health observation

An M4 Agent poll may include:

- `capabilities`, including `atomic_exchange_v1` only after the real local
  filesystem preflight succeeds;
- `publication_root` as the configured private-root observation;
- `publication_health` as an optional bounded snapshot.

`publication_health` contains process-lifetime commit success/failure counters,
visibility-to-durability timing totals/samples, preflight rejection and GC
failure counters, publication-root available bytes, bounded GC backlog,
admission-block reason, and counts of local recovery/fence records.

M4 Server accepts M3 poll bodies that omit all M4 fields. Direct dispatch remains
available to those Agents. Atomic dispatch still requires the explicit
`atomic_exchange_v1` capability; health fields never grant execution authority.

The health snapshot is operational observation, not authoritative Run state or
a durable action queue. Server aggregation de-duplicates repeated polls and
uses the Agent boot identity to recognize process-counter resets.
