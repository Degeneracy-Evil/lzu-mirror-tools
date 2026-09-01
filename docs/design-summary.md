# LMT Design Summary

This is the concise frozen architecture summary for LZU Mirror Tools.

Detailed behavior remains defined by the other documents in this directory.

## 1. Product definition

LMT is:

> a small distributed, CLI-first control plane for reliably scheduling, executing, and observing public software mirror synchronization across multiple Linux hosts.

It is not a CDN, reverse proxy, repository database, monitoring stack, or general cluster orchestrator.

## 2. Core deployment

```text
                operator Git repository
                         |
                       TOML
                         |
                  lmt config apply
                         |
                         v
                  +--------------+
                  |  lmt-server  |
                  |              |
                  | scheduler    |
                  | state        |
                  | SQLite       |
                  | central logs |
                  +------+-------+
                         |
                  HTTP/JSON long poll
                         |
             +-----------+-----------+
             |           |           |
             v           v           v
         lmt-agent   lmt-agent   lmt-agent
             |           |           |
             v           v           v
          process      process      process
           rsync       scripts       etc.

client downloads:
client -> nginx -> mirror files

observability:
Prometheus <- /metrics
journald/Loki <- daemon structured logs
lmt CLI <- central Run history/logs
```

The control plane is never in the user download path.

## 3. Language and implementation

The core implementation language is Rust.

Initial workspace:

```text
libraries:
  lmt-core
  lmt-protocol
  lmt-store

binaries:
  lmt-server
  lmt-agent
  lmt-cli
```

`lmt-core` contains domain meaning and must remain independent from HTTP, async runtime, and SQL infrastructure.

## 4. Configuration

Human-authored configuration is TOML.

GitHub/Git is used only for normal versioning/review. LMT itself has no Git dependency.

Node ownership is expressed by directory namespace:

```text
config/nodes/mirror01/mirrors/ubuntu.toml
config/nodes/mirror02/mirrors/pypi.toml
```

Mirror TOML does not repeat a placement field.

The applied configuration tree is authoritative.

```text
new file     -> create managed Mirror
changed file -> new generation
removed file -> prune Mirror from active management
moved file   -> explicit node ownership change
```

Pruning configuration never deletes mirror data.

No hidden LMT environment variables are injected. Runtime values are visible through explicit TOML placeholders.

## 5. Core domain

Public resources are deliberately small:

- Mirror;
- Node;
- Run.

Attempts are internal execution records associated with a Run.

Mirror configuration is immutable per generation.

Every Run captures exactly one generation and is never mutated by later config applies.

## 6. Scheduling

One Mirror has at most one non-terminal Run.

Interval:

```text
next due = previous Run terminal completion + interval
```

Cron:

- normal wall-clock schedule;
- if the Mirror is already running, skip that occurrence.

Misfires caused by unavailable execution capacity/node/server are coalesced into one catch-up marker.

Manual sync creates a durable Pending Run if the owner Node is temporarily offline.

There is no automatic cross-node failover in v1.

## 7. Server-Agent protocol

Agents initiate HTTP/JSON communication.

Control uses bounded long polling.

Correctness does not depend on a persistent connection.

Command delivery is at-least-once, while execution is idempotent by:

```text
(run_id, attempt_no)
```

A duplicate StartAttempt never creates a second concurrent execution.

Agents keep only a tiny file-based durable spool for crash recovery; all authoritative/queryable state lives on the main server.

## 8. Failure semantics

Server crash:

- systemd restarts server;
- Agent processes continue;
- Agents retain unacknowledged results/log progress;
- reconnect/reconcile after server returns.

Agent crash:

- systemd automatically restarts Agent;
- supervised child work is not allowed to remain an unmanaged writer;
- interrupted Attempt is reported;
- server may retry with a new Attempt number.

Network partition:

- current Agent work may continue;
- offline status never authorizes duplicate execution on another Node.

Safety is preferred over automatic failover.

## 9. Database and logs

One authoritative SQLite database lives on the main server.

Core logical tables:

```text
config_revisions
mirrors
mirror_generations
mirror_schedule_state
nodes
node_credentials
runs
attempts
attempt_logs
```

SQLite stores business state/history.

Run stdout/stderr is uploaded centrally but stored as files, not SQLite BLOBs.

Daemon logs go to structured journald and may be collected into Loki.

Prometheus is used for metrics.

## 10. State machines

Public Run:

```text
Pending -> Running -> Succeeded
                   -> Failed
                   -> Cancelled
                   -> TimedOut
```

Internal Attempt:

```text
Queued -> Accepted -> Running
                   -> Succeeded
                   -> Failed
                   -> TimedOut
                   -> Cancelled
                   -> Interrupted

Queued -> Rejected
```

Protocol detail stays out of the public Run API.

## 11. CLI/API

The CLI is the primary administration interface.

Representative commands:

```text
lmt config validate
lmt config plan
lmt config apply

lmt mirror list
lmt mirror show
lmt mirror sync

lmt node list
lmt node show

lmt run list
lmt run show
lmt run cancel
lmt run logs --follow
```

Pre-stable API path:

```text
/api/v1alpha1
```

Manual mutations carry request IDs to make HTTP retries idempotent.

Config apply uses optimistic base revision checking.

## 12. Execution model

The Agent initially supports only the native process runner.

Mirror sync semantics compile on the server into immutable RunSpecs.

Built-in rsync is configuration sugar over the same generic process executor.

Custom synchronization can use Python, shell scripts, Rust, Go, or any executable without a plugin SDK.

A container runner may be added later only if needed.

## 13. External systems

LMT integrates with, but does not reimplement:

- Nginx: file serving/gateway;
- Prometheus: metrics collection;
- Grafana: dashboards;
- journald/Loki: daemon log aggregation/search;
- GitHub/Git: configuration history;
- optional read-only status website.

The project should leave clean APIs for these integrations.

## 14. Architecture invariants

These should be treated as non-negotiable unless an explicit future design decision changes them:

1. LMT is not in the download serving path.
2. One server is authoritative in v1.
3. Agents never access the central DB directly.
4. Configuration is visible, TOML-based, and authoritative by bundle.
5. Mirror data is never destroyed by config pruning.
6. One Mirror never has concurrent active Runs.
7. A Run is tied to one immutable config generation.
8. Duplicate network delivery never creates duplicate execution.
9. Node disappearance never implies permission for cross-node retry.
10. Agent local policy cannot be bypassed by the server.
11. Business history is centralized.
12. Run logs are centralized but not SQLite BLOBs.
13. Repository semantics stay out of Agent execution code.
14. Complexity requires a demonstrated operational need.

## 15. What is deliberately deferred

Not part of the initial architecture:

- generic plugin SDK;
- automatic placement;
- cross-node failover;
- storage orchestration;
- snapshot/publication engine;
- PostgreSQL/controller HA;
- OIDC/RBAC;
- container runner;
- workflow/DAG system.

These can be reconsidered after LZU and community deployments establish real requirements.

## 16. Current milestone status

M1 and M2 are accepted implementation baselines.

M3 production-operations design remains frozen; its implementation is complete
and awaiting acceptance review/controlled production-trial evidence.

For M3 work, read:

- `m3-design.md` as the authoritative behavioral specification;
- `m3-implementation-plan.md` for M3.0 through M3.9 development order;
- `operations/` for production-trial runbooks;
- `code-review.md` for release-review gates.

M3 deliberately preserves the accepted scheduler/execution model and adds operational safety: credential lifecycle, Agent fencing, backup/restore, log lifecycle, bounded observability, CLI ergonomics, diagnostics, and service hardening.

When implementation evidence exposes a bad assumption, update the design and Architecture Decisions explicitly before changing the contract in code.
