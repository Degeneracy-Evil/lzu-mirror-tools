# M1 Implementation Plan

M1 is the first implementation milestone.

Its purpose is not to implement a complete mirror platform. It exists to prove the LMT architecture through one reliable end-to-end vertical slice.

## 1. M1 goal

The milestone is complete when this flow works reliably:

```text
TOML bundle
   |
validate / plan / apply
   |
   v
lmt-server + central SQLite
   |
manual Run
   |
Agent long poll
   |
immutable Attempt/RunSpec
   |
native process execution
   |
central log upload
   |
terminal event
   |
Run Succeeded/Failed
   |
server restart
   |
state remains correct
```

The first successful executable should be a trivial local command such as `/bin/true`, not rsync.

## 2. M1 scope

M1 includes:

- Rust workspace scaffolding;
- `lmt-core`;
- `lmt-protocol`;
- `lmt-store`;
- `lmt-server`;
- `lmt-agent`;
- `lmt-cli`;
- SQLite migrations/opening;
- Server/Agent configuration parsing;
- bearer-token Node authentication;
- Agent poll/heartbeat;
- authoritative config validate/plan/apply;
- command-type Mirrors;
- native process runner;
- manual Run creation;
- Attempt dispatch/idempotency;
- success/failure reporting;
- Agent durable spool;
- centralized Run log upload/read;
- Run/Attempt query APIs;
- minimal CLI for M1 operations;
- graceful shutdown;
- systemd service drafts;
- automatic restart semantics;
- health endpoints;
- basic metrics required to operate/debug M1;
- required unit/integration/E2E tests.

## 3. Explicitly out of M1

Do not implement:

- cron scheduler;
- interval scheduler;
- scheduler catch-up;
- built-in rsync configuration sugar;
- automatic retries beyond what is needed to validate Attempt mechanics;
- container runner;
- automatic placement;
- cross-node failover;
- PostgreSQL;
- controller HA;
- web frontend;
- OIDC/RBAC;
- storage pools/snapshots/publication;
- generic plugin system.

Scheduler/retry semantics are already designed for M2, but M1 should not expand scope merely because the documents exist.

## 4. Recommended implementation sequence

### Step 1 - Workspace and domain skeleton

Create the six planned crates/binaries with correct dependencies.

Implement only enough `lmt-core` types to express:

- typed IDs;
- Mirror configuration for command sync;
- Node name;
- Run/Attempt states;
- process RunSpec;
- validation errors.

Acceptance:

- workspace builds;
- dependency direction matches `implementation-design.md`;
- core state types have unit tests.

### Step 2 - Central store

Implement:

- database opening/PRAGMAs;
- migration framework;
- initial M1 tables from `database.md`;
- repository/transaction APIs needed by M1.

Acceptance:

- empty DB migrates to latest;
- restart reopens existing DB;
- foreign keys/constraints are tested;
- no HTTP types leak into `lmt-store`.

### Step 3 - Protocol contracts

Implement `v1alpha1` DTOs needed by M1:

- error envelope;
- config validate/plan/apply;
- Node poll;
- StartAttempt;
- Attempt event;
- manual Run;
- Run/Attempt query;
- log upload/read.

Acceptance:

- JSON round-trip/golden tests;
- protocol types do not depend on Axum/Reqwest.

### Step 4 - Server boot and Node authentication

Implement:

- server config;
- database startup/migrations;
- health endpoints;
- bearer-token Node authentication;
- Agent poll endpoint;
- Node observed-state persistence.

Acceptance:

- authenticated Agent appears in Node query;
- wrong/revoked credential fails;
- authenticated identity cannot be spoofed by request JSON.

### Step 5 - Authoritative config apply

Implement:

- TOML bundle loading in CLI;
- validation;
- canonicalization/hash;
- plan;
- atomic apply;
- config revision conflict handling;
- command Mirrors only.

Acceptance:

- create/update/remove/no-op are tested;
- invalid bundle applies nothing;
- stale base revision conflicts;
- semantic no-op does not create pointless generation.

### Step 6 - Manual Run creation

Implement:

- `POST /mirrors/{name}/runs`;
- client request ID;
- one-active-Run invariant;
- immutable config generation capture;
- first Queued Attempt/RunSpec.

Acceptance:

- duplicate request ID returns same Run;
- database prevents two active Runs for the same Mirror.

### Step 7 - Agent native process execution

Implement:

- Agent local config;
- poll loop;
- durable file spool;
- StartAttempt idempotency;
- direct argv execution;
- process-group supervision;
- stdout/stderr capture;
- terminal result;
- timeout/cancel primitives only as needed by the process supervisor.

Acceptance:

- `/bin/true` succeeds;
- `/bin/false` fails;
- duplicate StartAttempt never creates duplicate process;
- conflicting spec hash is rejected;
- Agent shutdown leaves no orphan process.

### Step 8 - Event reporting and central Run completion

Implement:

- Accepted/Running/terminal Attempt events;
- event sequence handling;
- Run state projection;
- idempotent duplicate/out-of-order event handling.

Acceptance:

- successful Attempt produces Succeeded Run;
- failure produces correct Failed Run for the M1 policy;
- terminal state cannot regress.

### Step 9 - Central Run logs

Implement:

- local capture spool;
- offset-based upload;
- central log file;
- attempt log metadata;
- operator read endpoint;
- CLI `lmt run logs`.

`--follow` may be implemented in M1 if the bounded long-poll read path is already simple; otherwise it can be completed at M3, but offset correctness is required now.

Acceptance:

- retransmitted chunk does not duplicate bytes;
- server restart preserves central log;
- operator can read log without connecting to Agent.

### Step 10 - Crash recovery

Implement/test:

Server crash:

- running Agent work continues;
- Server restarts against same DB;
- terminal result/log can be retransmitted;
- no duplicate Attempt starts.

Agent crash:

- supervised child is not left unmanaged;
- Agent restarts through test harness/systemd semantics;
- spool produces Interrupted/result reconciliation;
- no same-Attempt duplicate execution.

This step is a release gate.

### Step 11 - CLI minimum UX

M1 CLI should include at least:

```text
lmt config validate <dir>
lmt config plan <dir>
lmt config apply <dir>

lmt mirror list
lmt mirror show <name>
lmt mirror sync <name>

lmt node list
lmt node show <name>

lmt run list
lmt run show <id>
lmt run logs <id>
```

Human output may remain simple, but mutating operations and errors must use stable API semantics.

Machine-readable JSON output can be introduced early if it does not complicate M1.

## 5. M1 E2E acceptance scenarios

The milestone is not complete until these pass automatically.

### Scenario A - success

```text
apply command Mirror using /bin/true
-> manual sync
-> Agent accepts
-> process succeeds
-> Run Succeeded
-> query from CLI/API
```

### Scenario B - observable failure

```text
helper writes stdout/stderr and exits 1
-> central logs contain expected content
-> Attempt Failed
-> Run Failed according to M1 policy
```

### Scenario C - lost StartAttempt response

```text
Agent receives StartAttempt
-> HTTP response is lost
-> Server re-delivers same Attempt
-> only one process executes
```

### Scenario D - lost terminal acknowledgement

```text
Agent reports terminal result
-> acknowledgement lost
-> Agent retransmits
-> DB/history remains single and correct
```

### Scenario E - Agent crash

```text
long process active
-> Agent killed
-> child not left unmanaged
-> Agent restarts
-> attempt reconciles as Interrupted
-> no duplicate writer
```

### Scenario F - Server crash

```text
Attempt active
-> Server killed/restarted
-> Agent work remains safe
-> reconnect/retransmit
-> final central state correct
```

### Scenario G - config reconciliation

```text
apply Mirror
-> update it
-> remove file
-> apply
-> Mirror unmanaged
-> historical Run remains queryable
-> mirror data path untouched
```

## 6. Review checkpoints

After each major step, review implementation against:

- `docs/code-review.md`;
- `docs/state-machines.md`;
- `docs/database.md`;
- `docs/agent-protocol.md`.

Do not postpone architecture review until M1 is completely implemented.

## 7. M1 completion criterion

M1 is complete when:

- all acceptance scenarios pass;
- binaries run under documented Linux service layout;
- no architecture invariant is knowingly violated;
- docs reflect any intentional design adjustment;
- the repository is ready to proceed to scheduler/rsync work in M2.

A large amount of code is not a completion criterion. A small, reliable vertical slice is.
