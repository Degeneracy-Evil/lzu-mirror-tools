# M4 Implementation Plan

Status: **ready for implementation**.

This plan implements the frozen M4 publication architecture plus the other
production-trial issues already accepted into M4.

Authoritative publication semantics live in
`docs/m4-publication-design.md`. If this plan conflicts with that document,
the frozen publication design wins.

## 1. Completion goal

M4 is complete when LMT can:

- preserve every accepted M1/M2/M3 behavior and fault test;
- keep Direct Mirrors working during the supported M3->M4 rolling upgrade;
- run Atomic Mirrors using the frozen write-ahead/exchange/fence/GC contract;
- install and upgrade Server/Agent through an idempotent local installer;
- stop an idle Agent promptly rather than waiting for long-poll timeout;
- avoid one Tokio worker per logical CPU on very large hosts;
- pass frozen M3 compatibility fixtures and the M4 publication fault matrix.

Do not add M5 work: HA, replicas, dynamic placement, storage pools, snapshot
backends, workflow/DAG execution, generic verification, containers,
PostgreSQL, or OIDC/RBAC.

## 2. Ordered vertical slices

### M4.0 - Freeze M3 compatibility artifacts

Before changing protocol structs, capture verbatim fixtures from accepted M3
baseline `8d0c032c37d6bb34c1e398e6d68e31c20ef28881`:

- M3 PollRequest JSON;
- M3 PollResponse JSON;
- M3 Direct ProcessRunSpec JSON.

Tests must consume those historical bytes. Do not generate imaginary "legacy"
fixtures from M4 structs.

Where practical, add an integration check using an Agent built from the accepted
M3 baseline against an M4 Server.

### M4.1 - Core config and wire extension

Mirror config:

~~~toml
[publication]
mode = "atomic"
~~~

Absent publication config means Direct.

Agent storage:

~~~toml
[storage]
mirror_root = "/srv/mirrors"
publication_root = "/srv/lmt-publication"
spool_dir = "/var/lib/lmt-agent/spool"
publication_max_private_generations = 4
publication_reserve_bytes = 10737418240
~~~

The numeric values above are examples, **not implicit defaults**.

Atomic capability is advertised only when publication root, private-generation
bound, reserve bytes, and filesystem preflight are all valid.

Protocol:

- PollRequest adds optional/default-empty capabilities and optional publication
  root observation;
- M4 Server accepts frozen M3 poll bodies;
- M4 Agent advertises `atomic_exchange_v1`;
- ProcessRunSpec gains an optional publication extension;
- Direct serialization omits the field entirely, never serializes
  `"publication": null`;
- Atomic spec is sent only to an Agent advertising the capability;
- publication fields participate in spec hash.

For Atomic mode, process `target_dir` is the fresh candidate. Add
`{published_dir}` for trusted custom commands that explicitly need the live
path.

### M4.2 - Validation and high-impact planning

Add bundle-level validation:

- exact same-Node target overlap rejected;
- ancestor/descendant same-Node target overlap rejected;
- identical relative targets on different Nodes remain valid;
- Atomic rsync args pass the audited profile below.

Config plan must clearly flag:

- owner Move;
- Direct -> Atomic;
- Atomic -> Direct.

Move acknowledgement never overrides quiescent safety. Publication-mode changes
also require quiescence.

### M4.3 - Transactional Store changes

Move and publication-mode reconciliation must combine the active-Run check and
config update in one SQLite transaction.

Valid Move race outcomes:

~~~text
Move wins -> owner changes; no old-owner Run exists
Run wins  -> Move rejected
~~~

Race-test manual Run creation, scheduled materialization, and mode changes.

Do not add leases/epochs.

No Publication table is added. Add a central migration only if a real new
Server-side durable field is required; do not create an empty milestone
migration.

### M4.4 - Linux publication filesystem primitive

Keep low-level publication FS code in a narrow Agent module.

Use the existing `nix` dependency for:

- `renameat2(RENAME_EXCHANGE)`;
- no-overwrite first publication;
- device/inode identity reads;
- directory `fsync`.

Probe actual behavior using disposable same-filesystem directories.

Reject Atomic capability when:

- mirror/publication roots are on different mounted filesystems;
- required rename flags fail;
- publication root is below mirror root;
- published target is an invalid type or mount point;
- required paths are not writable.

Network filesystems are outside this backend contract.

### M4.5 - Durable Atomic spool state machine

M4 must still load legacy Direct/M3 spool records.

Atomic phases include at least:

~~~text
executing
preparing_exchange
ready_to_commit
visible_pending_durability
committed_pending_report
abandoned_fenced
~~~

Implement the frozen ordering exactly:

~~~text
sync success
-> durable preparing_exchange
-> mutate exchange/gc namespace
-> durable ready_to_commit
-> recheck cancellation/preconditions
-> visibility commit
-> parent-directory fsync
-> durable committed_pending_report
-> AttemptSucceeded
~~~

Durable spool publication fsyncs record data and its parent directory.

Publication-recovery phases bypass generic restart -> Interrupted handling.

Fault injection must exist after every durable write and namespace mutation.

### M4.6 - One per-Mirror publication lock and full-writer fence

One local publication lock serializes:

- commit preparation;
- visibility commit;
- recovery;
- pre-visibility restoration;
- abandon/fence;
- fence-clear;
- GC protected-set scan;
- GC deletion;
- Atomic admission.

A durable fence binds exactly:

~~~text
mirror
run_id
attempt_no
spec_hash
~~~

Abandon rejects mismatches.

Persist/fsync the fence before reporting terminal failure to Server.

While fenced on an old Node, **all LMT writers for that Mirror are blocked**,
including Direct execution.

Fence-clear uses the same lock and exact fence identity.

### M4.7 - Fixed exchange-slot commit/recovery

Private layout:

~~~text
publication_root/<mirror>/
├── attempts/<run>-<attempt>/
├── exchange/
└── gc/
~~~

Stable state:

~~~text
published = current
exchange/ = previous
~~~

Update:

1. durable `preparing_exchange`;
2. rotate previous from `exchange/` to a protected unique GC path;
3. stage fresh candidate into `exchange/`;
4. durable `ready_to_commit` with all identities;
5. recheck cancel/preconditions;
6. `RENAME_EXCHANGE(exchange, published)`;
7. fsync required parents;
8. durable `committed_pending_report`;
9. report success.

After exchange, `exchange/` is deterministically the immediate previous
generation.

First publication uses no-overwrite rename while preserving the same recovery
invariants.

If cancellation wins after staging but before visibility commit, restore the
stable previous slot before terminalizing. If restoration fails, remain
fail-closed and block admission.

### M4.8 - Atomic rsync profile

Atomic rsync is **fresh-generation materialization**, not Direct
existing-destination semantics.

LMT owns `--link-dest`; destination begins fresh.

The parser must understand supported short clusters such as `-aH` and long
options. Unknown/unclassified options are rejected.

#### Safe / supported

Preservation/traversal:

~~~text
-a / --archive
-r / --recursive
-l / --links
-p / --perms
-t / --times
-g / --group
-o / --owner
-D
-H / --hard-links
-A / --acls
-X / --xattrs
--numeric-ids
~~~

Source selection, explicitly with fresh-generation meaning:

~~~text
--include
--exclude
--filter
--include-from
--exclude-from
--files-from
--prune-empty-dirs
--max-size
--min-size
~~~

Transport/performance/comparison:

~~~text
--bwlimit
--timeout
--contimeout
--compress / -z
--whole-file
--checksum
--size-only
--ignore-times
--block-size
--checksum-choice
--compress-choice
--protect-args / -s
~~~

Observability:

~~~text
--itemize-changes
--stats
--human-readable
--verbose / -v
--quiet / -q
--progress
~~~

Source-link interpretation may be supported after explicit tests:

~~~text
--copy-links
--safe-links
--copy-unsafe-links
~~~

#### Safe but may reduce dedup efficiency

Document rather than reject:

~~~text
--checksum
--ignore-times
--whole-file
attribute combinations that prevent link-dest matches
~~~

#### Meaningless under fresh-generation semantics: reject

~~~text
--delete
--delete-before
--delete-during
--delete-delay
--delete-after
--delete-excluded
--max-delete
--force
--ignore-errors
--existing
--ignore-existing
--ignore-non-existing
--update
~~~

Errors should explain the fresh-generation semantic difference.

#### Unsafe / rejected

~~~text
--inplace
--append
--append-verify
--write-devices
--link-dest
--copy-dest
--compare-dest
--backup
--backup-dir
--suffix
--partial
--partial-dir
--remove-source-files
--remove-sent-files
--dry-run / -n
--list-only
~~~

Never silently strip options.

Keep this profile in `lmt-core`, not Agent repository logic.

### M4.9 - GC protected set and admission

Under the per-Mirror publication lock, GC must never delete/mutate:

- current published tree;
- stable `exchange/` previous;
- paths referenced by live/recoverable spool;
- `preparing_exchange` paths;
- `ready_to_commit` paths;
- `visible_pending_durability` paths;
- `committed_pending_report` paths;
- `abandoned_fenced` evidence/paths;
- rotated previous paths referenced by in-progress commit.

Before Atomic admission:

1. verify no local writer fence;
2. run eligible GC;
3. recompute protected/garbage sets;
4. enforce explicit `publication_max_private_generations`;
5. enforce explicit `publication_reserve_bytes`.

If GC cannot reduce below bound, remain blocked.

Reserve is only an admission floor, not a promise the candidate fits.

ENOSPC before visibility commit fails without changing published data.

Expose GC backlog/failures, publication free bytes, admission-block reason, and
fenced/degraded state.

### M4.10 - Local recovery UX

Do not add a remote workflow engine or admin socket.

Emergency recovery is an offline/local `lmt-agent` maintenance interface. The
daemon is stopped so the command can acquire the normal spool lock.

Provide commands conceptually equivalent to:

~~~text
lmt-agent publication status --mirror <name>

lmt-agent publication retry-durability \
  --mirror <name> --run <id> --attempt <n> --spec-hash <hash>

lmt-agent publication abandon \
  --mirror <name> --run <id> --attempt <n> --spec-hash <hash> \
  --acknowledge-visible-publication-risk

lmt-agent publication fence-clear \
  --mirror <name> --run <id> --attempt <n> --spec-hash <hash>
~~~

Exact spelling may be polished; identity/risk semantics may not.

Abandon order:

1. acquire Agent spool/single-instance lock;
2. acquire per-Mirror publication lock;
3. verify exact identity/phase;
4. durably write/fsync `abandoned_fenced`;
5. only then report terminal failure;
6. if Server is unavailable, retain evidence and let daemon reconcile later.

Generic reset/restore/downgrade refuses protected publication evidence.

### M4.11 - Restore / forward upgrade / downgrade

Forward rollout:

~~~text
create control-plane backup
-> upgrade Server
-> verify M3 Agents still execute Direct Mirrors
-> upgrade Agents
-> observe atomic_exchange_v1
-> enable Atomic Mirrors
~~~

Downgrade is offline restore, never in-place binary rollback:

- resolve protected publication evidence with M4 tooling;
- stop Server/Agents;
- restore pre-M4 DB backup;
- restore matching pre-M4 authoritative TOML bundle;
- install matching M3 binaries;
- preserve mirror data;
- archive/reset only spool proven safe for M3.

### M4.12 - Idempotent local installer

Ship top-level/release-root `install.sh`.

Supported roles:

~~~text
sudo ./install.sh server ...
sudo ./install.sh agent ...
sudo ./install.sh all ...
sudo ./install.sh upgrade ...
~~~

It is local installation, not SSH/cluster orchestration.

Responsibilities:

- install release binaries;
- create `lmt` and `lmt-agent` users;
- create standard config/state/runtime paths;
- install/update systemd units;
- enforce accepted ownership/modes;
- create initial TOML only from explicit inputs;
- install secrets from generated file/stdin without exposing secrets in argv;
- run preflight;
- enable/start requested services;
- be idempotent.

Server install:

- explicit bind address required;
- securely create operator token only if absent;
- never overwrite config/token silently;
- create DB/log/backup directories;
- may create a root client TOML so `sudo lmt` avoids repeated flags.

Agent install:

- explicit node name, Server URL, mirror root;
- credential supplied via stdin/protected file, not CLI token argument;
- Atomic support additionally requires explicit publication root,
  private-generation bound, and reserve bytes;
- run publication preflight before capability is usable.

`all` combines local Server+Agent and may issue the local Agent credential
through the newly started Server without exposing it in argv.

Upgrade preserves config, secrets, DB, mirror data, publication state, and spool.
It follows Server-first M3->M4 ordering.

Installer must never modify firewall/routing, Nginx, Docker/Podman/Kubernetes,
filesystem formatting/mounts, or unrelated services.

External Ansible may invoke it for multi-host deployment.

### M4.13 - Prompt Agent shutdown

Fix the observed idle restart delay.

The poll loop must select between HTTP completion and shutdown signal. Shutdown
drops/cancels the outstanding request immediately rather than waiting for the
Server 20-second poll or 35-second Reqwest timeout.

Preserve active process-group termination and publication recovery semantics.

Test with bounded fake time/server behavior; do not sleep 20 real seconds.

### M4.14 - Bounded Tokio runtime

Use an explicit **4-worker Tokio runtime** for `lmt-server` and `lmt-agent`.

Do not expose worker count as M4 user configuration.

SQLite already runs behind its dedicated DB thread and control paths are
predominantly async I/O. Four workers remove the ~240-thread behavior observed
on the trial host while keeping headroom.

Add small concurrency evidence and re-check on the large-core host when
practical. Revisit only if measurements justify a later decision.

### M4.15 - Observability / doctor

Add bounded diagnostics for:

- Atomic capability;
- commit success/failure;
- visibility-to-durability duration;
- preflight rejection;
- publication free bytes;
- GC backlog/failure;
- admission block reason;
- local fence/degraded state.

No Run/Attempt IDs in Prometheus labels.

Doctor checks config completeness, same-filesystem roots, exchange/no-replace
probe, publication root outside mirror root, writable paths, target type,
protected recovery/fence state, and admission health.

### M4.16 - Release docs/artifacts

Before acceptance:

- update Server/Agent/Mirror examples;
- document Direct vs Atomic semantics prominently;
- document every rsync profile category;
- update backup/restore/downgrade runbook;
- add install/upgrade guide;
- document Nginx open-file-cache caveat;
- document previous-generation immutability/non-snapshot semantics;
- include frozen M3 fixtures;
- release tarball contains binaries, systemd units, installer, and essential
  examples/docs.

## 3. Cross-cutting hard invariants

1. no exchange/gc mutation before durable `preparing_exchange`;
2. no visibility commit before durable `ready_to_commit`;
3. no AttemptSucceeded before namespace durability and durable terminal evidence;
4. publication recovery never falls through generic Interrupted;
5. post-visibility failure never auto-rolls back or creates a duplicate writer;
6. abandon/fence binds exact Mirror/Run/Attempt/spec identity;
7. fence is durable before Server failure and blocks every old-Node LMT writer;
8. GC/admission/recovery/commit/fence-clear share one publication lock;
9. protected paths are never GCed;
10. Move/mode-change quiescence is transactional;
11. Direct remains compatible with M3 Agents behind M4 Server;
12. Atomic dispatch requires explicit capability;
13. config pruning never deletes published mirror data;
14. M4 does not claim upstream snapshot consistency or full repository
    power-loss durability.

## 4. Required test matrix

Crash/fault injection around:

~~~text
process success
durable preparing_exchange
rotate previous
stage exchange
durable ready_to_commit
visibility exchange
parent fsync
durable committed_pending_report
Server terminal event
~~~

At each point verify filesystem visibility, spool phase, restart convergence,
previous ownership, no duplicate writer, and GC protected set.

Also test:

- persistent fsync failure -> retry durability -> abandon/fence -> fence-clear;
- pre-visibility restoration failure;
- ENOSPC;
- cancel vs ready/commit;
- GC vs commit/recovery/fence-clear;
- admission vs recovery;
- Move vs manual/scheduled Run;
- mode change vs Run;
- frozen M3 fixtures;
- Direct RunSpec has no publication field;
- M4 never sends Atomic to M3;
- XFS/ext4/Btrfs rename probes where practical.

Normal CI must not require public internet.

## 5. Release gates

M4 is not accepted until:

1. all M1/M2/M3 tests stay green;
2. frozen M3 fixtures pass;
3. Direct behavior is unchanged;
4. every write-ahead/crash boundary has fault coverage;
5. fence/protected-spool destructive-operation tests pass;
6. GC protected-set/admission race tests pass;
7. transactional Move/mode races pass;
8. Atomic rsync options are fully classified/tested;
9. installer is idempotence-tested where systemd-capable integration is
   practical;
10. Agent idle shutdown is prompt;
11. bounded runtime policy is verified;
12. docs/runbooks/examples match implementation;
13. one small real-host Atomic smoke test succeeds after automated acceptance.

Do not begin M5 during M4 hardening.
