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

One mirror is normally stored in one TOML file.

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

[placement]
node = "mirror01"

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

## 3. Placement

v1 uses explicit placement:

```toml
[placement]
node = "mirror02"
```

The server does not automatically move a mirror to another node when that node becomes unavailable.

This is intentional: large mirrors are persistent data, not stateless jobs.

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
args = ["/opt/lmt-sync/pypi.py", "--mode", "mirror"]
```

This intentionally keeps custom synchronization open to any executable language.

The execution environment provides stable metadata such as:

```text
LMT_MIRROR_NAME
LMT_RUN_ID
LMT_ATTEMPT
LMT_NODE_NAME
LMT_MIRROR_ROOT
LMT_TARGET_DIR
```

These environment variables are a small process contract, not a plugin SDK.

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
lmt config validate mirrors/
lmt config apply mirrors/
```

Validation includes at minimum:

- TOML parsing;
- schema/type validation;
- unique mirror names;
- safe target paths;
- valid cron/timezone or interval;
- valid node reference;
- valid runner;
- duration and retry bounds.

An apply operation creates or updates configuration. Missing local files do **not** implicitly delete server-side mirrors.

Deletion must be explicit:

```text
lmt mirror delete <name>
```

Deleting mirror metadata must never automatically delete mirror files.

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
- online/offline state;
- last seen time;
- currently running attempts;
- mirror-root capacity/free bytes;
- supported runner capabilities.

The server should not attempt to duplicate every local configuration option into its database.

## 10. Run

A Run is one logical synchronization operation.

Typical fields include:

- run ID;
- mirror name;
- configuration generation;
- assigned node;
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
