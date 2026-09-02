# Controlled Production Trial

Status: active after M3 acceptance.

Purpose: validate the accepted M3 system on realistic Linux hosts before M4 design. This phase is evidence collection, not feature expansion.

## 1. Test host

One normal Linux server is enough for the first trial.

Minimum practical host:

~~~text
CPU:      2 cores
Memory:   4 GiB
Disk:     50 GiB local usable space
Network:  100 Mbps+
OS:       modern systemd-based Linux
Access:   root/sudo
~~~

Recommended host:

~~~text
CPU:      4 cores
Memory:   8 GiB
Disk:     100-200 GiB SSD/NVMe
Network:  1 Gbps
OS:       Ubuntu Server 24.04 LTS or equivalent
Access:   root/sudo
~~~

No GPU or unusual hardware is required.

Cloud VM and bare metal are both valid. A cloud VM is sufficient for normal Server/Agent, scheduler, credential, log, backup, restore, and rsync tests. Bare metal is slightly preferable for later disk-pressure, filesystem, Nginx+rsync concurrency, and snapshot/publication experiments.

The authoritative SQLite database must remain on local storage.

## 2. Initial shape

Start with one host running lmt-server, lmt-agent, Nginx, SQLite, and 1-3 small mirrors.

Do not begin with multi-terabyte repositories. Tens of GiB total is enough for repeated synchronization and fault experiments.

A second host is useful later for multi-Node ownership, Agent binding/fencing, credential replacement, and offline-node tests, but is not required for first bring-up.

## 3. Trial phases

Phase A - normal operation:

- install and configure services;
- config validate/plan/apply;
- scheduled and manual rsync;
- Run/Attempt history and log follow;
- status, doctor, metrics, and journald.

Phase B - operations:

- Server and Agent restart;
- credential rotation/revocation;
- backup create/verify;
- offline restore rehearsal;
- log retention;
- Agent binding conflict;
- disk-space pressure.

Phase C - controlled faults:

- Server unavailable;
- Agent unavailable;
- network interruption;
- upstream failure;
- timeout/cancellation;
- disk pressure;
- repeated/lost control traffic where practical.

Already-present mirror files must remain servable while the control plane is unavailable.

## 4. Publication-consistency experiment

The most important post-M3 architecture question is whether LMT needs an explicit publication layer.

Current topology:

~~~text
rsync -> live mirror directory <- nginx
~~~

Possible future topology:

~~~text
sync -> staging/snapshot
          |
        verify
          |
     atomic publish
          |
nginx -> published generation
~~~

During real rsync while Nginx serves the same tree, record:

- whether metadata becomes visible before referenced files;
- whether files disappear while metadata still references them;
- whether clients observe transient repository errors;
- duration of any inconsistent window;
- whether behavior differs by repository type;
- whether rsync flags/upstream update behavior change the result.

Do not implement snapshot/publication during the trial merely because it appears architecturally attractive. Use measured evidence to decide whether it is needed and what guarantee it must provide.

## 5. Mirror identity question

Today Mirror effectively combines a logical mirror resource with one Node-owned physical data tree.

This is sufficient for static ownership.

Before adding staging trees, published generations, replicas, migration, or failover, explicitly decide whether Mirror denotes the logical published resource or a physical synchronized copy.

Do not add those concepts before that domain definition is frozen.

## 6. Lifecycle/hook question

Real deployments may eventually need:

~~~text
sync -> verify -> metadata generation -> snapshot -> publish -> cleanup -> notification
~~~

This is a real future requirement area, but do not copy a generic Tunasync-style hook lifecycle or create a DAG/workflow engine prematurely.

After the trial, derive the smallest lifecycle model from actual publication and verification needs.

## 7. Architecture boundaries to protect

Trial-driven fixes must preserve:

1. Server remains the single authoritative control plane.
2. Agent remains a generic executor, not repository-aware.
3. Agent spool remains crash-recovery state, not a second queryable database.
4. ProcessRunSpec remains a mirror-execution primitive, not a generic job platform.
5. SQLite remains central persistence unless measured evidence disproves it.
6. Server downtime may pause new scheduling; do not add Agent-local scheduling without a demonstrated problem.
7. Control plane stays outside the download path.
8. Config pruning never deletes mirror data.

## 8. Evidence to record

For meaningful experiments/incidents capture:

~~~text
time
LMT commit/version
host/filesystem
Mirror config
Run/Attempt IDs
introduced fault/change
expected behavior
actual behavior
Run logs
daemon logs
metrics/doctor output
serving impact
recovery steps
~~~

For performance observations also capture active Run/rsync counts, CPU, memory, disk throughput/latency, and network throughput.

The purpose is to separate control-plane limits from disk/network/upstream limits.

## 9. Questions before M4

1. Is one Server operationally sufficient at real mirror scale?
2. Does Server unavailability cause meaningful freshness problems?
3. Does static Node ownership create real operational pain?
4. Does Agent remain sufficiently generic for real mirrors?
5. Is SQLite comfortable under real Run/log history?
6. Does concurrent rsync + serving create observable repository inconsistency?
7. Is a snapshot/publication layer required?
8. If yes, what exact consistency guarantee is needed?
9. What lifecycle steps are actually required around synchronization?
10. Which M3 operator workflows deserve M4 polish?

M4 should be designed from these answers rather than hypothetical scalability concerns.

## 10. Trial findings

### T001 - fresh-install Agent enrollment bootstrap gap

Observed during the first n01 deployment on 2026-09-02.

Accepted M3 behavior before the trial required `POST /nodes/{node}/credentials` to target an existing Node row. A fresh control plane has no Node rows, while a new Agent cannot poll until it has a credential. Configuration apply also does not establish Node rows.

This creates a bootstrap cycle on a clean install.

Resolution contract:

- the first operator-authenticated credential issue for a valid Node name may create the Node record atomically with the credential;
- the new Node remains offline/unbound until its first authenticated Agent poll;
- first valid Agent poll establishes the durable Agent installation binding;
- no unauthenticated Agent self-registration is introduced;
- legacy inline credentials are not required for fresh installation.

This is a trial-driven maintenance fix inside the accepted M3 architecture, not M4 scope.

### T002 - shared /etc/lmt directory traversal

Observed during the same deployment.

A shared `/etc/lmt` directory owned `root:lmt` with mode `0750` prevents the separate `lmt-agent` service user from traversing the directory, even if its own files are `root:lmt-agent 0640`.

Production-trial layout is therefore:

~~~text
/etc/lmt/                    root:root      0755
/etc/lmt/server.toml         root:lmt       0640
/etc/lmt/operator.token      root:lmt       0640
/etc/lmt/agent.toml          root:lmt-agent 0640
/etc/lmt/agent.token         root:lmt-agent 0640
~~~

Directory traversal is public; secret contents remain protected by file ownership/mode.

### T003 - Agent does not report mirror-root free space

Observed on n01 after first authenticated Agent poll.

`lmt node show n01` reported `mirror_root_free_bytes = null` even though the configured mirror root is on a mounted XFS filesystem with approximately 2.6 TiB free.

Code inspection showed this is not an environmental failure: the Agent currently constructs every PollRequest with `capacity.mirror_root_free_bytes = None`.

This contradicts the M3 observability/doctor contract and prevents useful disk-pressure diagnostics.

Required maintenance fix:

- measure available bytes for the configured `mirror_root` on every poll or at a safely bounded refresh interval;
- report the value through the existing Capacity field;
- measurement failure must not stop Agent polling; report `None` and emit an operational warning instead;
- add a filesystem-backed test.

Treat this as M3 production-trial maintenance, not M4 scope.

### T004 - Tokio default worker count is excessive on very large hosts

Observed on the 240-logical-CPU n01 host.

Both lmt-server and lmt-agent showed roughly 240 runtime worker threads because the default multi-thread Tokio runtime scales its worker count from available CPUs.

This is not currently a correctness problem and memory usage remained small, so no immediate runtime tuning is authorized.

During the trial, measure whether a small explicit worker count would reduce operational overhead without hurting poll/log/control-plane latency. Do not choose a fixed value from intuition alone.

### T005 - first complete real-host Run succeeded

Observed on n01 on 2026-09-02 after T001 and T003 maintenance fixes.

Environment:

~~~text
host: n01.cluster.test
OS: Ubuntu 24.04 LTS
filesystem: XFS on /mnt/data
mirror_root: /mnt/data/lmt-trial/mirrors
Agent max_concurrent_runs: 2
~~~

Mirror:

~~~text
name: local-smoke
owner: n01
sync: local rsync
generation: 1
config revision: 1
~~~

Run:

~~~text
Run ID: 01M1G4XJMG3FW0SX87SVGKXYAA
trigger: manual
Attempt: 1
final state: succeeded
exit code: 0
created:  2026-09-02T04:09:31.535Z
accepted: 2026-09-02T04:09:31.538Z
started:  2026-09-02T04:09:31.544Z
finished: 2026-09-02T04:09:31.590Z
~~~

Observed behavior:

- Pending Run was dispatched immediately to the online n01 Agent.
- Attempt 1 transitioned through accepted/running to succeeded.
- Central Run log contained rsync itemized output.
- Target files were present with expected contents.
- Agent spool retained only the process lock and durable Agent installation ID after terminal reconciliation.
- Server status reported no pending/running Runs and retained successful Run history.
- `run_logs_stored_bytes` reported 190 bytes.
- `mirror_root_free_bytes` reported 2,846,064,881,664 bytes, confirming the T003 capacity fix on the real XFS mirror root.

This establishes the first real-host normal-operation baseline before restart/retry/cancellation/network/disk/publication-consistency fault experiments.

### T006 - incremental rsync update/delete succeeded

Observed on n01 on 2026-09-02 after the first successful local-smoke Run.

Upstream changes:

- modified `hello.txt`;
- added `new.txt`;
- removed `subdir/nested.txt`.

Run:

~~~text
Run ID: 01M1G52PSYCAG2GXFWWE5V8ZQ2
Mirror generation: 1
Attempt: 1
final state: succeeded
exit code: 0
created:  2026-09-02T04:12:19.646Z
finished: 2026-09-02T04:12:19.697Z
~~~

Observed rsync log:

~~~text
>f.st...... hello.txt
>f+++++++++ new.txt
*deleting   subdir/nested.txt
~~~

The target tree matched the updated upstream contents, deletion propagated correctly, and the Mirror remained on generation 1 because only upstream data changed; LMT configuration did not.

This establishes the normal incremental-sync baseline before fault injection.

### T007 - Server restart during an active Attempt preserved execution

Observed on n01 on 2026-09-02.

Setup:

- `local-smoke` was changed to generation 2 with rsync `--bwlimit=10240`.
- a 512 MiB `large.bin` was added to the local upstream to keep the Attempt active long enough for fault injection.

Run:

~~~text
Run ID: 01M1G56M6EDVWE8WQX07G8NJQ0
Mirror generation: 2
Attempt: 1
created:  2026-09-02T04:14:28.046Z
started:  2026-09-02T04:14:28.054Z
finished: 2026-09-02T04:15:19.231Z
final state: succeeded
exit code: 0
~~~

Fault:

`lmt-server` was restarted while Attempt 1 was actively transferring `large.bin`.

Observed behavior:

- `lmt-agent` stayed running.
- The active rsync process group remained running across the Server restart.
- After the Server returned, Agent reconciliation continued automatically.
- The original Run remained the same Run ID.
- The original Attempt remained Attempt 1; no Attempt 2 was created.
- Final state was Succeeded with exit code 0.
- Central Run logs were readable after the restart.
- `large.bin` was fully present in the target tree.

`pgrep` showed three rsync PIDs during execution. They were all in the same `lmt-agent.service` cgroup and the control plane recorded only one Attempt. This is consistent with rsync's internal multi-process execution rather than duplicate LMT dispatch. Future writer-count experiments should capture PPID/process-tree information as well as raw PID count.

The `journalctl --since "5 minutes ago"` command used after completion returned no entries, so future fault experiments should use an explicit absolute time window around the injected fault rather than a relative window.

This validates the accepted architectural property that Server/control-plane restart does not terminate an already-owned Agent execution.
