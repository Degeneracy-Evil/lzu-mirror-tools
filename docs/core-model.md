# Core Model and Configuration v0.1

LMT v0.1 deliberately starts with only three public core resources:

- **Mirror**: desired long-lived mirror configuration;
- **Node**: an observed execution host;
- **Run**: one logical synchronization operation.

Internal implementation objects such as attempts are not necessarily public API resources.

## 1. Mirror

A Mirror answers:

- what is being synchronized;
- where its data lives relative to the selected node;
- when it should run;
- which node owns it;
- how the synchronization command is produced;
- what execution policy applies.

One mirror is normally stored in one TOML file under a node-scoped configuration directory.

Example repository layout:

```text
config/
└── nodes/
    ├── mirror01/
    │   └── mirrors/
    │       ├── ubuntu.toml
    │       └── debian.toml
    └── mirror02/
        └── mirrors/
            └── pypi.toml
```

The directory namespace is authoritative: `config/nodes/mirror01/mirrors/ubuntu.toml` means that the `ubuntu` mirror is owned by node `mirror01`. The mirror file does not repeat that fact.

Example:

```toml
[mirror]
name = "ubuntu"
enabled = true
description = "Ubuntu archive mirror"
target = "ubuntu"

[schedule]
cron = "15 * * * *"
timezone = "Asia/Shanghai"

[sync]
type = "rsync"
source = "rsync://archive.ubuntu.com/ubuntu/"
args = [
  "-aH",
  "--delete",
  "--numeric-ids",
]

[runner]
type = "process"

[run]
timeout = "6h"
max_attempts = 2
retry_delay = "5m"
```

### 1.1 Identity

`mirror.name` is the stable human-facing identifier.

Operators interact with names:

```text
lmt mirror show ubuntu
lmt mirror sync ubuntu
```

Database implementation IDs must not leak into normal CLI/API usage.

### 1.2 Target paths

`mirror.target` is always relative to the node's configured mirror root.

If:

```text
target = "ubuntu"
node mirror_root = "/srv/mirrors"
```

the effective target is:

```text
/srv/mirrors/ubuntu
```

Another node may use `/data/mirrors` without changing central mirror configuration.

Target validation must reject:

- absolute paths;
- `..` traversal;
- any canonicalized result outside the configured mirror root.

Central configuration must never become arbitrary remote filesystem access.

## 2. Scheduling

A mirror may use either a cron schedule or an interval schedule.

Cron example:

```toml
[schedule]
cron = "15 * * * *"
timezone = "Asia/Shanghai"
```

Interval example:

```toml
[schedule]
interval = "2h"
```

The two forms are mutually exclusive.

If `[schedule]` is absent, the mirror is manual-only.

## 3. Node ownership

v1 does not have a `[placement]` section in a Mirror file.

Node ownership is explicit in the configuration namespace:

```text
config/nodes/mirror01/mirrors/ubuntu.toml
                         |
                         +--> ubuntu is owned by mirror01
```

The server records the derived owner node when configuration is applied and dispatches that mirror's Runs only to the matching agent.

This keeps one source of truth for ownership and avoids contradictory configuration such as placing a file under `mirror01/` while declaring `node = "mirror02"` inside it.

Moving a mirror file between node directories is an explicit ownership change. Because this can cause a large re-synchronization on another host, `lmt config apply` must surface the move clearly and may require an explicit acknowledgement flag before applying it.

The server does not automatically move a mirror when a node becomes unavailable. Large mirrors are persistent data, not stateless jobs.

## 4. Synchronization specification

v1 has two central synchronization forms:

### 4.1 rsync

```toml
[sync]
type = "rsync"
source = "rsync://example.org/ubuntu/"
args = ["-aH", "--delete"]
```

The server compiles this into an ordinary process RunSpec. The agent does not need an rsync-specific implementation.

### 4.2 command

```toml
[sync]
type = "command"
program = "python3"
args = [
  "/opt/lmt-sync/pypi.py",
  "--target",
  "{target_dir}",
]
cwd = "{mirror_root}"
```

This intentionally keeps custom synchronization open to any executable language.

LMT does **not** inject hidden LMT-specific environment variables by default. Runtime values are available only through explicit placeholders written in the configuration. Initial placeholders are expected to include:

```text
{mirror_name}
{run_id}
{attempt}
{node_name}
{mirror_root}
{target_dir}
```

The server resolves placeholders while compiling an immutable RunSpec.

If a custom program needs a value, that dependency is therefore visible in the TOML file itself. For example, a script only receives the target directory if the configuration explicitly includes `"{target_dir}"` in an argument or other supported field.

Ordinary user-defined environment variables may still be configured explicitly in TOML if a synchronization program requires them; LMT must not create implicit configuration channels.

## 5. Runner

Synchronization semantics and execution mechanism are separate concepts.

```text
sync   = what command should be executed
runner = how it is executed on the node
```

v0.1 requires:

```toml
[runner]
type = "process"
```

A future OCI/container runner may be added without changing Mirror semantics.

The agent may understand runner types, but must not understand repository-specific sync types.

## 6. Run policy

Example:

```toml
[run]
timeout = "6h"
max_attempts = 3
retry_delay = "5m"
```

This describes policy for future Runs. It does not contain current runtime state.

## 7. Configuration generations

Every successfully applied change to a Mirror creates a new configuration generation.

Conceptually:

```text
ubuntu generation 1
ubuntu generation 2
ubuntu generation 3
```

A Run records the exact generation from which its RunSpec was compiled.

This gives a durable answer to:

> Which configuration was this failed run using?

Git remains the long-term source of configuration history; LMT generations provide runtime traceability.

## 8. Configuration application

Expected workflow:

```text
lmt config validate config/
lmt config plan config/
lmt config apply config/
```

Validation includes at minimum:

- TOML parsing;
- schema/type validation;
- valid node namespaces;
- unique mirror names within the deployment;
- safe target paths;
- valid cron/timezone or interval;
- valid runner;
- valid placeholder references;
- duration and retry bounds.

An applied configuration tree is an **authoritative desired set** for its managed scope.

Therefore `lmt config apply config/` reconciles the server to the files that currently exist:

- a new file creates a managed Mirror;
- a changed file creates a new Mirror generation;
- a removed file removes that Mirror from active management;
- moving a file between node directories changes its owner node.

This prevents configuration drift between the repository and LMT's applied state.

`lmt config plan` must show creates, updates, removals, and node moves before application. Automation may apply directly, while interactive CLI usage may ask for confirmation for high-impact changes such as node moves.

Removing a Mirror from management stops future scheduling but does not erase historical Runs.

For reconciliation purposes, **dispatch is the revocation boundary**: a Pending Run with no dispatched Attempt can be cancelled immediately, while an Attempt that has already been delivered to an Agent is treated as potentially executing even if its Accepted event has not yet reached the Server. Config pruning therefore does not implicitly revoke already-dispatched Attempts; stopping them requires the explicit cancellation protocol.

Most importantly, pruning control-plane configuration does **not** delete mirror data from disk. Data removal is a separate, explicit destructive operation and is outside normal configuration reconciliation.

## 9. Node

Node local configuration lives on each host, for example:

```text
/etc/lmt/agent.toml
```

Example:

```toml
[node]
name = "mirror01"

[server]
url = "http://10.0.0.10:8080"
token_file = "/etc/lmt/agent.token"

[storage]
mirror_root = "/srv/mirrors"

[execution]
max_concurrent_runs = 2

[runner.process]
enabled = true
```

Node local configuration is **local policy**.

The server-side Node record is **observed state**, such as:

- name;
- agent version;
- agent instance/epoch;
- online/offline state;
- last seen time;
- currently running attempts;
- mirror-root capacity/free bytes;
- supported runner capabilities.

All queryable Node state is stored centrally by `lmt-server`. The server should not attempt to duplicate every local configuration option into its database.

## 10. Run

A Run is one logical synchronization operation.

Typical fields include:

- run ID;
- mirror name;
- configuration generation;
- owner/execution node;
- trigger;
- state;
- current/total attempts;
- created/start/finish timestamps;
- final exit code;
- failure category/message;
- duration;
- optional execution statistics.

Run IDs should use ULIDs so that they are globally unique and approximately time ordered.

### 10.1 Public Run state

The public v0.1 state machine is intentionally small:

```text
Pending
   |
   v
Running
   |
   +--> Succeeded
   +--> Failed
   +--> Cancelled
   +--> TimedOut
```

Internal protocol states may be more detailed without becoming permanent public API states.

### 10.2 Attempts

Retries belong to one Run.

```text
Run 01...
  Attempt 1 -> failed
  Attempt 2 -> failed
  Attempt 3 -> success

Final Run state: Succeeded
```

The execution idempotency key is:

```text
(run_id, attempt_number)
```

The server makes retry decisions. The agent never creates a new attempt on its own.

## 11. Deliberately deferred concepts

The following concepts are intentionally absent from the v0.1 public model:

- plugin SDK;
- automatic node placement;
- storage pools;
- publication/snapshot resources;
- mirror replicas;
- dependency graphs;
- hooks/pipelines;
- controller HA;
- automatic cross-node failover.

They should only be added after real requirements establish their correct shape.


## 12. M2 model extensions

M2 does not add a new public resource. Mirror, Node, and Run remain the core public model; Attempts remain execution records inside a Run.

A Mirror optionally has one schedule: interval, cron, or none. No schedule means manual-only. Runtime due state is Server-owned and is not embedded in the Mirror configuration.

M2 supports both command and rsync sync configuration. Rsync is Server-side configuration sugar and compiles into the same generic process RunSpec; the Agent remains repository-agnostic.

Run trigger becomes a typed Manual/Scheduled value. Run metadata may additionally expose scheduled_for_at, retry_due_at, and cancel_requested_at. Public Run state remains Pending/Running/Succeeded/Failed/Cancelled/TimedOut.

Retries remain Attempts inside one Run. Retry delay is represented by persistent Run retry_due state; no retry queue/resource is introduced.

Node observed state adds max_concurrent_runs reported by the Agent. This remains local policy observed by the Server, not central placement configuration.

For complete M2 semantics, see m2-design.md.


## 13. M4 publication model extension

M4 does not add a new public resource.

Mirror is now explicitly interpreted as the **logical published mirror
resource**, not one concrete directory inode/tree. The owner Node realizes that
logical resource using its local storage.

Publication remains an internal durable commit phase of an Attempt.

For Direct mode, `{target_dir}` remains the live target.

For Atomic mode, each Attempt writes a fresh private candidate and the Agent
publishes it only through the frozen contract in
`m4-publication-design.md`. AttemptSucceeded is emitted only after visibility
commit and required namespace durability complete.

Atomic mode also freezes these model rules:

- Atomic rsync uses fresh-generation materialization semantics rather than
  Direct existing-destination semantics.
- Published/previous generations may share hard-linked inodes and are immutable
  from LMT's perspective.
- Same-Node Mirror targets may not overlap.
- Ownership Move requires a quiescent Mirror and the quiescent check plus owner
  update are one Store transaction.
- Publication is not a public resource, generic workflow phase, replica model,
  or automatic cross-node migration mechanism.

For exact recovery, fencing, GC, compatibility, and filesystem semantics, the
frozen `m4-publication-design.md` is authoritative.
