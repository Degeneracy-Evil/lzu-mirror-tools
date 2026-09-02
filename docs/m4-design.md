# M4 Design Draft

Status: design in progress. Implementation is not authorized by this document.

M4 is the first milestone designed from controlled-production-trial evidence rather than hypothetical requirements.

## 1. Goal

> Close the architecture and operations gaps exposed by the LZU controlled production trial while preserving the small Server/Agent model.

M4 has three workstreams:

1. publication correctness;
2. reproducible installation and upgrade;
3. small runtime/operations fixes observed on real hosts.

M4 is not a general feature milestone.

## 2. Trial findings carried into M4

### Already resolved during the trial

The following findings are closed and should remain regression-tested:

- fresh-install Node bootstrap during first credential issuance;
- shared `/etc/lmt` traversal/ownership layout;
- Agent mirror-root free-space reporting.

### Open issue A - repository publication is not atomic

The real Nginx + kernel.org experiment showed that rsync updates files in the serving tree independently.

The current model is:

~~~text
sync tree == serving tree
~~~

This gives useful per-file replacement behavior, but does not publish a repository generation atomically.

M4 must define the exact guarantee LMT wants to provide before choosing a filesystem mechanism.

### Open issue B - installation is too manual

The controlled trial required repeated manual work for:

- building/copying binaries;
- creating `lmt` and `lmt-agent` users;
- creating state/config directories with exact ownership/modes;
- installing systemd units;
- creating Server/Agent TOML;
- issuing and moving Agent credentials;
- choosing the Server management bind address;
- keeping Agent Server URLs consistent when that address changes;
- creating convenient CLI client configuration;
- enabling/restarting services.

The repository currently contains systemd units but no installer/deployer.

M4 should provide an idempotent local installer/upgrade path.

It must not turn lmt-server into a remote SSH orchestration system.

### Open issue C - Agent shutdown can wait for a long poll

During an idle Agent restart on n01, systemd spent roughly 18 seconds stopping the service.

The Agent HTTP client has a 35-second request timeout while Server long polling waits up to 20 seconds. The polling await is not selected against Agent shutdown.

M4 should make shutdown interrupt an outstanding poll promptly.

This is an operations fix, not a protocol redesign.

### Open issue D - Tokio default worker count scales badly on very large hosts

On the 240-logical-CPU trial hosts, Server and Agent each created roughly one Tokio worker per available CPU.

No correctness failure or material memory pressure was observed, but this is unnecessary for a small control plane.

M4 should adopt a deliberately bounded runtime policy and test that normal Agent/poll/log latency remains comfortable.

## 3. Architecture direction for publication

The production trial changes one important domain interpretation:

> A Mirror should be treated as the logical published mirror resource, not as one concrete directory inode/tree.

A physical synchronization tree is an implementation artifact owned by the current Node.

This keeps Mirror identity stable when publication generations, staging trees, or later ownership changes occur.

### Run semantics

For Mirrors using atomic publication, a Run means:

> synchronize a candidate generation and make that generation the published state.

Therefore a Run must not become Succeeded merely because rsync/process execution returned success if publication has not completed.

Conceptually:

~~~text
Run
 |
 +-- Sync Attempt 1
 |      Failed
 |
 +-- Sync Attempt 2
 |      Succeeded
 |
 +-- Publication
        Published
 |
 +-- Run Succeeded
~~~

### Publication is not a new top-level user resource

The detailed design in `docs/m4-publication-design.md` refines this further.

Publication commit is the final durable local phase of an Attempt, not a separate Server-side resource or state machine.

The Agent reports AttemptSucceeded only after the atomic publication commit. This preserves the existing Run/Attempt projection and keeps publication recovery inside the Agent spool, where local execution ownership already lives.

### Fixed lifecycle, not a workflow engine

M4 should use a fixed lifecycle:

~~~text
prepare candidate
      |
      v
sync Attempts
      |
      v
publish
      |
      v
best-effort cleanup
~~~

Repository-specific verification pipelines, arbitrary hooks, and DAG/workflow execution remain deferred.

A future explicit verify step may be added only when a real repository requires it.

### Compatibility

Existing direct publication must remain representable:

~~~text
publication = direct
rsync -> serving tree
~~~

Atomic publication should be explicit configuration, not a silent behavior change for every existing Mirror.

## 4. Publication mechanism design gate

Do not implement until the mechanism is frozen.

The selected M4 generic mechanism is:

1. fresh private candidate tree;
2. atomic directory exchange using Linux renameat2(RENAME_EXCHANGE);
3. one previous tree retained internally;
4. filesystem-specific snapshot/clone mechanisms remain future optimizations.

The mechanism must satisfy all of these:

- live serving tree is never partially updated by synchronization;
- publication switch is atomic for pathname resolution;
- Agent/Server crash after the switch is recoverable without accidentally switching back;
- retry cannot overlap a writer with the published generation;
- cleanup cannot delete the active generation;
- ownership Move does not imply hidden cross-node data migration;
- implementation remains generic and repository-agnostic;
- storage amplification is acceptable for large public mirrors.

For built-in rsync, generation directories may use rsync snapshot techniques such as `--link-dest` to avoid full duplicate data, but only if the isolation semantics and incompatible rsync options are explicitly defined and tested.

## 5. Deployment automation

M4 should ship an idempotent local installer rather than a site-specific cluster orchestrator.

Target interface conceptually:

~~~text
sudo ./install.sh server ...
sudo ./install.sh agent ...
sudo ./install.sh all ...
sudo ./install.sh upgrade ...
~~~

Responsibilities:

- install released binaries;
- create service users/groups;
- create standard config/state/runtime directories;
- install systemd units;
- enforce documented ownership and modes;
- write initial explicit TOML only from supplied inputs;
- install token files without exposing secrets in process arguments;
- enable/start requested services;
- perform preflight checks and show actionable failures;
- support repeat execution without destroying existing state.

Safety rules:

- never overwrite existing secrets/config silently;
- never delete Mirror data;
- never modify firewall, routing, Docker, Kubernetes, Nginx, or unrelated host state;
- never invent a public bind address;
- never implement remote SSH orchestration inside the LMT control plane;
- upgrade should normally replace binaries/units and preserve configuration/state.

For multi-host site automation, Ansible or another external configuration-management system may invoke the local installer.

## 6. Runtime maintenance

### Agent poll shutdown

The Agent poll request should be cancellable by the existing shutdown signal.

Stopping an idle Agent should not wait for the Server's long-poll deadline.

Active Attempt shutdown semantics remain unchanged: process-group closure and durable Interrupted reconciliation are still required.

### Tokio worker policy

Server and Agent should use an intentional bounded worker policy instead of inheriting every logical CPU on very large hosts.

The exact bound is an implementation-design decision and should be justified by a small benchmark rather than host CPU count.

Do not introduce a large runtime-tuning configuration surface unless measurements show operators need it.

## 7. Explicit non-goals

M4 does not add:

- controller HA;
- automatic failover or placement;
- replicas;
- automatic cross-node data migration;
- multiple storage pools;
- generic workflow/DAG execution;
- plugin SDK;
- container runner;
- PostgreSQL;
- OIDC/RBAC;
- application-level log compression.

## 8. Design sequence

Freeze M4 in this order:

1. exact publication consistency guarantee;
2. Mirror/Run/Publication state model;
3. filesystem/generation layout and atomic switch mechanism;
4. crash/retry/cancel/Move semantics around publication;
5. configuration/API/DB changes;
6. installer/upgrade contract;
7. runtime shutdown/worker fixes;
8. implementation plan and acceptance tests.

Only after items 1-7 are reviewed should M4 implementation begin.

## 9. Detailed publication design

The current detailed proposal is `docs/m4-publication-design.md`. Review and freeze that document before implementation planning.
