# M4 Publication Architecture Design

Status: **proposed revision 2 after adversarial review**.

This document is not frozen and does not authorize implementation.

The revision addresses the release blockers raised in the adversarial review:
crash durability, fresh-generation rsync semantics, hard-link immutability,
Move races, and forward-only version compatibility.

## 1. Problem proven by the production trial

The controlled LZU trial demonstrated that:

~~~text
rsync -> serving tree <- nginx
~~~

is operationally reliable but not repository-atomic.

Different files become visible at different times, so clients may observe a local
tree composed from more than one synchronization generation.

M4 solves this local publication problem only.

It does not turn LMT into a repository transaction engine, workflow system,
storage orchestrator, or traffic manager.

## 2. Consistency contract

Atomic publication provides this guarantee:

> LMT does not expose the private candidate through the configured published
> path before the visibility commit. The published pathname changes from the
> previous complete local tree to the candidate complete local tree through one
> atomic namespace operation.

The guarantee does not include:

- multi-request client session consistency;
- upstream point-in-time snapshot consistency;
- recursive durability of all candidate file data across sudden power loss;
- immediate invalidation of Nginx or another server's open-file cache;
- atomicity across several Mirrors;
- semantic repository validation beyond the configured synchronization command.

The guarantee is therefore:

~~~text
atomic local namespace visibility
+
daemon/process crash recovery
~~~

not:

~~~text
full power-loss transactional repository durability
~~~

## 3. Three distinct commit concepts

M4 explicitly separates three boundaries.

### 3.1 Synchronization completion

The configured process exits successfully and the candidate is complete according
to that synchronization program.

No public path has changed yet.

### 3.2 Visibility commit

For an existing published tree:

~~~text
renameat2(RENAME_EXCHANGE)
~~~

successfully exchanges the candidate pathname and the published pathname.

This is the publication linearization point.

For first publication, the equivalent visibility commit is a no-overwrite rename
of the candidate into an absent published path.

### 3.3 Namespace durability boundary

After the visibility commit, the Agent fsyncs the directory metadata needed to
make the pathname update durable under normal local-filesystem semantics.

AttemptSucceeded is not emitted until this boundary completes.

This fsync is not a recursive fsync of a multi-terabyte repository.

M4 therefore does not promise that every newly synchronized file byte survives
an arbitrary sudden power loss merely because the Run had succeeded.

## 4. Crash and power-loss contract

### Daemon/process crash before visibility commit

The old published tree remains visible.

Recovery reconciles the durable Agent spool with the Server before any commit.

A retry uses a fresh Attempt candidate.

### Daemon/process crash after visibility commit but before namespace durability

The candidate may already be visible at the published path.

The durable spool records the candidate identity and the internal phase.

On restart, the Agent identifies which pathname contains the candidate, completes
the required directory fsync operations, and only then reports AttemptSucceeded.

It must not re-run synchronization or automatically exchange back to the old tree.

### Directory fsync error after visibility commit

A post-visibility fsync failure is not treated as an ordinary retryable sync
failure because publication may already be visible.

The Attempt remains locally owned in an internal
`visible_pending_durability` recovery phase.

The Agent:

- keeps the public Run non-terminal;
- retries the durability operation;
- reports critical health/diagnostic state;
- blocks another atomic Attempt for that Mirror until the ambiguity is resolved.

M4 does not invent a public Run state for this rare storage fault.

### Sudden power loss

M4 does not claim a full power-loss transaction.

After reboot the Agent recovers from the filesystem state that survived:

- if the candidate identity is at the published path, it completes namespace
  durability/reconciliation and converges toward success;
- if the candidate remains private, the visibility commit did not survive and
  normal pre-commit reconciliation applies;
- if identity evidence is inconsistent or missing, the Mirror enters a
  fail-closed local publication-health condition and new atomic Attempts are
  blocked until operator repair.

M4 does not recursively fsync every candidate file and therefore makes no
stronger data-durability claim.

## 5. Mirror identity

A Mirror is the logical synchronized-and-published mirror resource.

It is not one concrete directory inode or one historical synchronized tree.

The current owner Node realizes the Mirror using local storage.

Ownership Move changes future control-plane ownership. It does not migrate data,
delete the previous Node's data, redirect Nginx, or switch external traffic.

## 6. Publication remains an Attempt commit phase

Publication is not a top-level public resource.

For direct mode:

~~~text
Attempt
  -> execute process
  -> terminal result
~~~

For atomic mode:

~~~text
Attempt
  -> prepare fresh candidate
  -> execute process into candidate
  -> visibility commit
  -> namespace durability boundary
  -> terminal result
~~~

The Agent reports AttemptSucceeded only after the durability boundary.

The public Attempt states remain unchanged:

~~~text
Queued
Accepted
Running
Succeeded
Failed
TimedOut
Cancelled
Interrupted
Rejected
~~~

Internal durable phases are Agent-spool implementation state, not protocol
lifecycle states.

## 7. Selected generic backend

M4 keeps the public target as a real directory and uses Linux
`renameat2(RENAME_EXCHANGE)` for updates.

Example:

~~~text
mirror_root      = /srv/mirrors
publication_root = /srv/lmt-publication

published:
/srv/mirrors/ubuntu

private:
/srv/lmt-publication/ubuntu/attempts/<run>-<attempt>/root
~~~

After successful synchronization:

~~~text
RENAME_EXCHANGE(
    candidate,
    published
)
~~~

After the visibility commit:

~~~text
published path = new tree
candidate path = old published tree
~~~

First publication uses a no-overwrite rename into an absent target.

The public pathname remains a normal directory.

## 8. Why directory exchange remains preferred

Compared with an atomic symlink pointer, directory exchange does not change the
public target into a symlink and does not introduce serving-policy dependencies
such as Nginx symlink restrictions.

Compared with bind mounts, it does not require CAP_SYS_ADMIN or mount lifecycle
management.

Compared with Btrfs/ZFS snapshots, it keeps the M4 semantic contract independent
of one storage stack.

Filesystem-native CoW/snapshot backends remain possible future optimizations.

## 9. Filesystem requirements

Atomic-exchange mode requires:

- Linux;
- `mirror_root` and `publication_root` on the same mounted filesystem;
- successful real probes for the required rename flags;
- valid ordinary directory targets or an absent first-publication target;
- `publication_root` outside the publicly served `mirror_root`;
- local storage semantics suitable for the backend.

The Agent probes behavior rather than trusting filesystem-name strings.

Network filesystems are outside the M4 atomic-exchange contract.

## 10. Agent storage configuration

Atomic publication introduces one explicit private root:

~~~toml
[storage]
mirror_root = "/srv/mirrors"
publication_root = "/srv/lmt-publication"
spool_dir = "/var/lib/lmt-agent/spool"
~~~

No private `.lmt` hierarchy is placed under the served mirror root.

A storage-safety reserve/admission policy must be frozen in the implementation
plan before code is accepted.

## 11. Mirror configuration

Publication is explicit desired state:

~~~toml
[publication]
mode = "atomic"
~~~

Absent publication configuration remains direct mode.

Changing direct <-> atomic is a high-impact config change because atomic rsync
also has different destination semantics.

Publication-mode changes require the Mirror to be quiescent.

## 12. Atomic rsync has fresh-generation materialization semantics

This is an explicit M4 contract.

Atomic rsync does **not** preserve direct mode's existing-destination semantics.

Each Attempt starts with an empty candidate hierarchy.

The candidate represents a freshly materialized generation selected from the
upstream by the configured rsync selection/preservation rules.

Therefore:

- local files that exist only in the previous published tree do not carry
  forward automatically;
- excluded/protected old destination files do not survive merely because they
  existed in the previous tree;
- delete semantics against old destination contents are not the governing
  model;
- receiver-state options that depend on a preexisting candidate are not
  equivalent to direct mode.

This semantic difference must be visible in documentation and config planning.

Operators that require existing-destination behavior should use direct mode or
a trusted custom command that explicitly materializes the desired candidate.

## 13. Built-in rsync and link-dest

A full second physical copy is not acceptable for normal large mirrors.

For built-in atomic rsync, LMT controls an alternate basis using
`--link-dest`.

Conceptually:

~~~text
attempt/
├── root/     # fresh candidate
└── basis     # reference to current published tree
~~~

Rsync materializes the fresh candidate.

Files that are identical in all preserved attributes may be hard-linked from
the basis.

Changed or attribute-different files are independently materialized.

This is an implementation of fresh-generation semantics, not an attempt to
preserve direct-mode destination history.

## 14. Audited rsync profile

Atomic mode does not accept arbitrary rsync argv.

Configuration may remain explicit `args = [...]`, but every option must belong
to an audited M4 atomic allowlist/profile.

Unknown or unclassified options are rejected.

The implementation plan must contain a complete compatibility table.

At minimum, atomic mode must reject destination-history or LMT-owned alternate
destination behavior such as:

- user-provided `--link-dest`, `--copy-dest`, `--compare-dest`;
- `--inplace`;
- `--append` and `--append-verify`;
- `--write-devices`;
- `--existing`, `--ignore-non-existing`, `--ignore-existing`;
- `--update`;
- `--backup`, `--backup-dir`, and suffix-based destination backup behavior;
- partial/resume options whose value depends on reusing a previous Attempt
  destination;
- deletion-limit/protection options whose intended semantics depend on
  preexisting destination contents;
- source-removal behavior.

Source-selection options such as include/exclude/filter/files-from may be
supported only with the documented meaning that they define the contents of the
fresh generation rather than preserving excluded local destination files.

The allowlist is a correctness boundary, not convenience validation.

## 15. Hard-link generation immutability is a first-class invariant

`--link-dest` can make current, previous, and candidate generations share an
inode.

Therefore:

> published and previous atomic generations are immutable from LMT's
> perspective and must be treated as immutable by operators and serving tools.

No LMT future Run writes into a published or previous tree.

The serving plane must be content-read-only.

Manual chmod/chown/xattr/ACL/content repair on a hard-linked published
generation can affect several generations at once and is outside the atomic
contract.

`previous/` is not an isolated filesystem snapshot and must never be described
as one.

## 16. Single namespace writer is the correctness rule

The earlier design treated inode identity checks too strongly.

`stat -> verify -> RENAME_EXCHANGE` is not a compare-and-swap operation.

M4 therefore freezes this invariant:

> LMT is the only supported namespace writer for a managed atomic published
> pathname.

Per-Mirror Agent locking prevents two LMT publication paths from racing.

Pre-commit inode/device identity checks remain useful best-effort detection for
manual replacement or invariant violation, but they are not the correctness
primitive and cannot eliminate the external TOCTOU race.

External replacement/rename of a managed atomic target is unsupported.

## 17. Cancellation boundary

Cancellation and commit are serialized by the Agent's durable Attempt ownership
state.

Cancellation wins if it is durable locally before visibility commit.

Once the visibility commit succeeds, publication wins and is never
automatically rolled back because of a later cancellation.

If the Agent is offline, a cancellation that exists only on the Server cannot
retroactively undo an already completed visibility commit.

## 18. Retry behavior

Before visibility commit, normal retry rules apply.

Failed/TimedOut/Interrupted Attempts never publish their private candidate.

A retry always receives a fresh Attempt candidate.

A publication preflight rejection is non-retryable.

A failure before visibility commit may be retryable normally.

After visibility commit, the Agent must finish/recover the same commit rather
than create another writer.

## 19. Move is quiescent-only in M4

M4 deliberately chooses the small rule:

> A Mirror may move between Nodes only when it has no active non-terminal Run.

`config plan` may show the desired Move, but `config apply` rejects it while
the Mirror has a Pending or Running Run.

`--acknowledge-moves` does not override this safety rule.

Operators must wait for terminal state or explicitly cancel and wait for
terminal reconciliation before applying the Move.

This prevents an old owner in `ready_to_commit` or publication recovery from
publishing after ownership has moved.

M4 does not introduce leases, Move effective timestamps, traffic barriers, or
cross-node publication coordination.

## 20. Previous generation and GC

After exchange, the old published tree is retained as one internal previous
namespace generation.

It is not an independent snapshot and there is no automatic rollback API.

Older retired private trees and abandoned candidates are garbage.

GC is not allowed to become unbounded best effort.

M4 requires:

- bounded per-Mirror private-generation count;
- GC backlog metrics;
- GC failure metrics;
- publication-storage free-space reporting;
- health degradation when stale garbage cannot be reclaimed;
- admission control that blocks new atomic Attempts when unresolved commit
  state or unsafe GC/storage conditions exist.

The exact free-space reserve policy and cleanup bounds are implementation-plan
decisions and must be explicit rather than hidden heuristics.

ENOSPC during candidate construction fails the Attempt without changing the
published tree.

## 21. Serving-plane behavior

An already-open file descriptor may continue reading the old inode after the
directory exchange.

New pathname resolution sees the namespace before or after the atomic exchange.

LMT does not pin multiple HTTP requests to one generation.

Serving software may extend old-version visibility through its own file cache.

Publication does not require Nginx reload/API integration and LMT remains
outside the download path.

## 22. Upstream consistency is separate

Atomic local publication does not prove that the upstream source was a
point-in-time snapshot.

M4 does not introduce a generic Verify workflow merely to hide this limitation.

A trusted custom command may perform its own candidate synchronization and
validation before returning success.

A first-class verification phase remains evidence-driven future work.

## 23. Disable and remove semantics

Disable prevents future Runs and leaves the published tree available.

Remove stops LMT management and never deletes the published mirror data.

Private publication garbage is an explicit maintenance concern and is not
deleted merely because config pruning removed a Mirror.

## 24. Target-overlap invariant

Managed Mirror targets on one Node may not overlap exactly or through
ancestor/descendant relationships.

For example these are invalid on one owner Node:

~~~text
ubuntu
ubuntu/pool
~~~

The same target on different Nodes is valid because the physical roots differ.

## 25. Protocol compatibility: forward-only rolling upgrade

M4 does not claim bidirectional mixed-version compatibility.

The supported matrix is:

| Server | Agent | Direct | Atomic | Supported |
|---|---|---:|---:|---|
| M3 | M3 | yes | no | yes |
| M4 | M3 | yes | no | yes during rolling upgrade |
| M4 | M4 | yes | yes | yes |
| M3 | M4 | no guarantee | no | no |

M4 Server must accept legacy M3 Agent poll bodies with no capability field.

M4 Agent advertises publication capability explicitly.

M4 Server dispatches atomic work only to Agents advertising the required
capability.

For Direct Mirrors dispatched to M3 Agents, M4 Server serializes the legacy
ProcessRunSpec shape exactly. An optional publication extension must be omitted,
not serialized as `null`.

## 26. Upgrade and downgrade contract

Upgrade is forward-only:

~~~text
backup control plane
-> upgrade Server
-> verify M3 Agents still run Direct Mirrors
-> upgrade Agents
-> verify atomic capability
-> enable atomic Mirrors
~~~

M4 does not promise that an M3 Server can run safely on state produced by an M4
control plane or communicate with an M4 Agent.

Downgrade therefore means an offline recovery procedure, not binary rollback in
place:

- stop Server/Agents;
- restore the pre-M4 control-plane backup using the existing restore contract;
- install the matching older binaries/configuration;
- preserve Mirror data;
- reset/reconcile Agent spool according to the downgrade runbook.

This matches LMT's existing forward-only migration philosophy.

## 27. Capability negotiation

Atomic publication uses an explicit Agent capability such as:

~~~text
atomic_exchange_v1
~~~

M4 poll decoding treats the capability field as optional/default-empty so M3
Agents remain accepted by an M4 Server.

No capability is inferred from an Agent version string.

## 28. ProcessRunSpec compatibility

Atomic M4 Attempts require an optional publication extension in the immutable
RunSpec.

For direct mode it is absent from serialized JSON.

For atomic mode it includes the private candidate/publication information needed
by the M4 Agent and is covered by the spec hash.

M4 Server never sends the extended atomic shape to an M3 Agent.

Because M3 Server + M4 Agent is outside the supported matrix, M4 does not distort
the protocol merely to make reverse rollback appear compatible.

## 29. Database impact

No Publication table and no public Publication ID are required.

No public Run state is added.

Mirror configuration history records publication mode in canonical TOML.

Attempt immutable spec history contains the exact atomic execution contract.

A new publication-specific failure/health category may be introduced in M4,
with compatibility assessed only inside the supported forward-upgrade matrix.

Any schema migration remains forward-only.

## 30. Doctor and observability

M4 observability should include:

- publication commit success/failure counters;
- visibility-to-durability commit duration;
- publication preflight rejection;
- GC backlog and cleanup failures;
- publication-storage free bytes;
- degraded publication-health state;
- Agent atomic capability.

Run/Attempt IDs remain structured-log fields, not Prometheus labels.

`doctor` checks:

- publication_root configured;
- publication_root is outside mirror_root;
- required roots are writable;
- roots are on the same mounted filesystem;
- exchange/no-replace probes succeed;
- target type is valid;
- no unresolved local publication-health fault exists.

## 31. Installation automation interaction

The M4 local installer accepts publication storage explicitly.

It never guesses a private publication path and never places it under the served
tree.

It does not modify Nginx, firewall, routing, Docker, Kubernetes, or storage
formatting.

The installer follows the forward-only Server-first upgrade contract.

## 32. Rejected M4 alternatives

M4 does not add:

- a public Publication resource/API;
- generic phase/DAG execution;
- automatic rollback;
- snapshot-specific semantic dependency;
- bind-mount publication;
- current-symlink publication;
- cross-node data migration;
- publication leases;
- traffic switching;
- generic repository verification.

## 33. Acceptance requirements

Automated acceptance must cover at least:

1. direct mode unchanged;
2. fresh first atomic publication;
3. exchange from an existing live directory;
4. process failure never exposes candidate;
5. cancel before visibility commit prevents publication;
6. crash before visibility commit does not expose candidate;
7. crash after exchange but before directory fsync resumes the same commit;
8. AttemptSucceeded is impossible before the durability boundary;
9. retry always uses a fresh candidate;
10. hard-link basis is never mutated by LMT atomic rsync;
11. audited rsync profile rejects unclassified/incompatible options;
12. fresh-generation semantics differ from direct mode exactly as documented;
13. Move is rejected while Mirror is non-quiescent;
14. target overlap is rejected;
15. best-effort inode identity check is not treated as CAS;
16. GC backlog degrades health and can block admission;
17. M4 Server + M3 Agent Direct mode works;
18. M4 Server never sends atomic spec to an M3 Agent;
19. Direct ProcessRunSpec sent to M3 Agent contains no new publication field;
20. supported XFS/ext4/Btrfs rename probes where integration infrastructure permits.

A small real-host smoke test is sufficient after automated coverage.

## 34. External semantics relied upon

The design relies on standard Linux and rsync behavior:

- `renameat2(RENAME_EXCHANGE)` atomically exchanges two existing pathnames;
- rename cannot cross mount points;
- open file descriptors are unaffected by rename;
- directory fsync is required to persist directory-entry changes;
- `--link-dest` hard-links unchanged files into a fresh destination hierarchy;
- existing hard-linked destination entries can be affected by attribute
  mutation, which is why atomic candidates begin fresh and published generations
  are immutable.

Reference documentation:

- Linux rename(2): https://man7.org/linux/man-pages/man2/rename.2.html
- Linux fsync(2): https://man7.org/linux/man-pages/man2/fsync.2.html
- rsync(1): https://download.samba.org/pub/rsync/rsync.1
