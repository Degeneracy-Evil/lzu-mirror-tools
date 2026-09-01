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
