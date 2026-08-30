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
        "env": {},
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

## 7. Agent local journal

Each agent keeps a small local durable database, expected to be SQLite:

```text
/var/lib/lmt/agent.db
```

It records enough information to make command execution idempotent and to recover after an agent restart:

- execution key;
- RunSpec hash;
- accepted timestamp;
- process identity;
- current state;
- terminal result;
- last event sequence;
- whether the terminal result has been acknowledged by the server.

This database is local to one agent and is never shared.

## 8. Agent crash semantics

v0.1 prioritizes safety over preserving a running native process after the agent itself dies.

The packaged service must ensure child execution is tied to the agent service/process group. On an unexpected agent termination, its active child executions should also be terminated rather than becoming unmanaged writers.

After restart, the agent inspects its local journal.

An attempt that was durably marked running but no longer has a valid supervised process becomes `interrupted` and is reported to the server.

The server then applies normal retry policy.

This is intentionally simpler and safer than attempting to reconnect to arbitrary orphan processes.

A future execution shim/systemd runner may provide stronger crash-survival semantics without changing the network protocol.

## 9. Server crash semantics

The server must commit a Run/Attempt to SQLite before dispatching it.

If the server crashes:

1. agents continue or finish currently owned executions;
2. agents keep terminal reports in the local journal until acknowledged;
3. after server restart, agents reconnect by polling;
4. agents resend observed state and unacknowledged terminal reports;
5. the server reconstructs current state from its durable database plus agent reports.

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
