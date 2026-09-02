# M4 Publication Architecture Design

Status: **frozen for M4 implementation planning**.

Publication architecture is accepted after three adversarial review rounds.
Implementation is not authorized until the M4 implementation plan is reviewed.

The frozen design includes the final pre-namespace write-ahead phase,
full-writer fencing, and publication-lock serialization requested in the final
review.

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

## 4. Crash, recovery, and write-ahead contract

### 4.1 Required write-ahead ordering

Atomic publication must obey this ordering:

~~~text
1. synchronization process completes successfully
2. candidate identity + current exchange/previous identity + published identity
   are written durably to Agent spool as preparing_exchange
3. only after preparing_exchange is durable may the first exchange/gc namespace
   mutation occur
4. rotate the old stable exchange/previous path to a protected GC path when needed
5. stage the fresh candidate into the fixed exchange slot
6. persist the resulting exchange/candidate/rotated-path identities durably as
   ready_to_commit
7. cancellation/preconditions are rechecked under the same per-Mirror
   publication lock
8. visibility commit executes
9. required parent-directory fsync operations complete
10. Agent spool is written durably as committed/terminal-pending-report
11. AttemptSucceeded is emitted to Server
~~~

There are two write-ahead invariants:

> `preparing_exchange` must be durable before the first mutation of the private
> exchange/gc namespace.

and:

> `ready_to_commit` must be durable before the visibility commit is allowed.

Durable spool write means the record itself and the spool namespace update are
persisted using the Agent's crash-safe file-publication procedure.

`preparing_exchange` records the exact paths and identities needed to reconstruct
or restore the stable private layout if the daemon crashes while rotating the
old previous generation or staging the new candidate.

If the daemon crashes after visibility commit but before a later spool update,
the durable `ready_to_commit` record plus stored path identities is sufficient
to discover that visibility commit already happened.

### 4.2 Recovery phases override generic Interrupted recovery

The existing generic rule that a restarted Agent turns owned non-terminal process
execution into Interrupted applies only to pre-commit execution phases.

A spool record in any publication-recovery phase, including:

~~~text
preparing_exchange
ready_to_commit
visible_pending_durability
committed_pending_report
~~~

must not be normalized to Interrupted merely because the Agent restarted.

Publication recovery reconciles the existing Attempt first.

### 4.3 Daemon/process crash before visibility commit

The old published tree remains visible.

If the durable phase is ordinary execution, existing Interrupted semantics apply.

If the durable phase is `preparing_exchange`, the Agent reconstructs the private
namespace from the recorded identities while holding the per-Mirror publication
lock. It either completes staging into a durable `ready_to_commit` state or
restores the stable previous slot. It must not normalize the Attempt to
Interrupted until that private namespace is made stable.

If the durable phase is `ready_to_commit`, the Agent first reconciles with the
Server so a cancellation already persisted by the Server can be delivered before
commit.

A later retry, if any, always uses a fresh Attempt candidate.

If commit preparation has already staged the fresh candidate into the fixed
`exchange/` slot and rotated the old previous generation to a protected GC
path, but cancellation/precondition failure wins before visibility commit, the
Agent must restore the stable private layout before terminalizing the Attempt:

~~~text
exchange/ candidate -> discarded/private garbage
rotated prior previous -> exchange/
fsync private publication parent
then terminal Cancelled/Failed
~~~

If that private-layout restoration cannot be completed, the Attempt remains in
a local pre-visibility recovery/fenced phase and new atomic admission stays
blocked. The public serving tree is still unchanged, but LMT must not pretend
the private publication state is clean.

### 4.4 Daemon/process crash after visibility commit

The candidate may already be visible at the published path while the durable spool
still says `ready_to_commit` or `visible_pending_durability`.

Recovery compares stored filesystem identities with the fixed publication paths.

If the candidate identity is at the published path, the visibility commit is
treated as having happened. The Agent must:

- never exchange back automatically;
- never re-run synchronization for the same Attempt;
- complete namespace durability/finalization;
- converge toward AttemptSucceeded for the same Attempt.

### 4.5 Directory fsync failure after visibility commit

A post-visibility fsync failure is not an ordinary retryable sync failure because
the new tree may already be public.

The Attempt remains locally owned in:

~~~text
visible_pending_durability
~~~

The Agent:

- keeps the public Run non-terminal;
- retries the durability operation;
- reports critical publication-health state;
- blocks every new atomic Attempt for that Mirror;
- blocks publication mode change;
- keeps Move blocked while the Run remains non-terminal.

### 4.6 Explicit operator recovery and abandon/fence

`visible_pending_durability` must have an operator-controlled termination path.

The recovery choices are:

~~~text
visible_pending_durability
├── durability later succeeds
│     -> same Attempt Succeeded
├── operator repairs storage
│     -> retry durability
└── explicit abandon/fence
      -> same Attempt terminal Failed
      -> no automatic rollback
      -> local publication fence remains until separately cleared
~~~

Abandon/fence is a high-risk explicit operation against one exact immutable
execution identity:

~~~text
mirror
run_id
attempt_no
spec_hash
~~~

The operator action is rejected unless all four identities match the local
protected spool record.

Abandon/fence runs under the same per-Mirror publication lock used by commit,
recovery, GC, admission, and fence-clear. It is never automatic.

Its contract is:

- it acknowledges that visibility may already have committed;
- it does not claim namespace durability succeeded;
- it writes and fsyncs a durable local `abandoned_fenced` record **before** any
  terminal failure is reported to Server;
- the durable fence binds the exact Run/Attempt/spec identity being abandoned;
- it terminates the public Run with a publication-durability failure category;
- it guarantees no further namespace operation will be performed by that
  abandoned Attempt;
- it releases the Mirror from the public non-terminal Run so an operator may
  perform controlled Move/recovery;
- on that old Node, the fence blocks **all LMT writers for the Mirror**, including
  direct-mode execution, atomic execution, recovery publication, and any future
  LMT write path, until fence-clear succeeds;
- the old Node retains the local publication fence and recovery evidence until an
  explicit fence-clear operation confirms the local publication paths are safe.

A fence-clear operation takes the same per-Mirror publication lock and is allowed
only after doctor/preflight can establish a stable local namespace, no recoverable
commit remains, and the exact durable fence record being cleared still matches
the local Mirror state.

The explicit abandon path exists to escape permanent EIO without lying that the
Attempt succeeded or silently discarding evidence.

### 4.7 Sudden power loss

M4 does not claim a full power-loss transaction.

After reboot the Agent recovers from the filesystem state that survived:

- candidate identity at published path -> recover post-visibility state;
- candidate identity still private -> visibility commit did not survive;
- inconsistent/missing identity evidence -> fail closed and require operator
  publication recovery/fence handling.

M4 does not recursively fsync every candidate file and therefore makes no
stronger data-durability claim.

### 4.8 Publication recovery evidence is protected operational state

The following spool phases are correctness evidence, not disposable retry state:

~~~text
preparing_exchange
ready_to_commit
visible_pending_durability
committed_pending_report
abandoned_fenced
~~~

Ordinary spool reset, restore cleanup, Agent replacement cleanup, or downgrade
cleanup must not delete them.

Any operation that would archive/reset Agent Attempt spool must first scan for
protected publication evidence and fail closed when it exists.

The operator must resolve the Attempt through normal publication recovery or the
explicit abandon/fence procedure before destructive spool maintenance can
proceed.

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
/srv/lmt-publication/ubuntu/
├── attempts/<run>-<attempt>/root
├── exchange/
└── gc/
~~~

Each Attempt is built in a fresh `attempts/<run>-<attempt>/root` candidate.

Immediately before an update commit, the Agent prepares one fixed private
`exchange/` slot:

1. any stable previous generation currently in `exchange/` is renamed to a
   uniquely named protected GC path;
2. the fresh candidate is renamed into `exchange/`;
3. the new candidate identity and all rotated paths are persisted in the durable
   `ready_to_commit` spool record;
4. the visibility commit is:

~~~text
RENAME_EXCHANGE(
    /srv/lmt-publication/ubuntu/exchange,
    /srv/mirrors/ubuntu
)
~~~

After a successful exchange:

~~~text
published path = new tree
exchange/      = immediately previous published tree
~~~

Thus the fixed `exchange/` pathname is also the stable previous-generation slot
after every completed/recovered commit.

First publication uses a no-overwrite rename into an absent target and then
establishes an empty/absent previous slot according to the same recovery rules.

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

Atomic admission is fail-closed.

Before creating a new candidate, the Agent must run GC and then reject admission
when any of these remain true:

- unresolved publication recovery/fence state exists for the Mirror;
- the configured/documented hard bound on private generations is reached;
- publication-root free space is below the explicit publication reserve.

The exact numeric default for the generation bound and free-space reserve belongs
to the implementation plan, but the gate behavior is architectural and is not a
hidden heuristic.

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

One per-Mirror publication lock serializes every local operation that can change
or retire publication state:

~~~text
commit preparation
visibility commit
publication recovery
pre-visibility restore
abandon/fence
fence-clear
GC scan/delete
atomic admission
~~~

The same lock also gates all LMT writer admission while an old-Node fence exists.

This lock is a local correctness primitive. It prevents two LMT publication,
recovery, GC, or admission paths from racing with each other.

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

The quiescent check and owner-node update must occur in the **same Store
transaction** as the config reconciliation that performs the Move.

Run creation and Move therefore race through one authoritative SQLite state
transition. The only valid outcomes are:

~~~text
Move wins -> owner changes, no old-owner Run is created
Run wins  -> active Run exists, Move is rejected
~~~

There is no valid interleaving in which an old-owner Run is created after a
successful quiescent check but before ownership changes.

Operators must wait for terminal state or explicitly cancel and wait for
terminal reconciliation before applying the Move.

An `abandoned_fenced` publication Attempt is terminal and performs no future
namespace operation, so it no longer blocks the control-plane quiescent test.
The old Node's local publication fence remains an operational condition until
explicitly cleared.

M4 does not introduce leases, Move effective timestamps, traffic barriers, or
cross-node publication coordination.

## 20. Previous generation, protected set, GC, and admission

### 20.1 Deterministic previous-generation ownership

The fixed private `exchange/` pathname is the stable previous-generation slot
after a successful or recovered update commit.

The commit preparation may temporarily place the new candidate in `exchange/`
before visibility commit. During that window the durable spool phase and stored
inode identities distinguish candidate from previous.

After `RENAME_EXCHANGE(exchange, published)` succeeds, `exchange/`
deterministically contains the old published tree.

No separate publication manifest is required for previous-generation identity.

### 20.2 GC protected set

GC scan, protected-set construction, deletion, and admission checks all execute
under the same per-Mirror publication lock used by commit/recovery/fence-clear.

GC MUST NOT delete or mutate:

- the current published tree;
- the stable previous generation in `exchange/`;
- any candidate/path referenced by a live or recoverable spool record;
- any path referenced by `preparing_exchange`;
- any path referenced by `ready_to_commit`;
- any path referenced by `visible_pending_durability`;
- any path referenced by `committed_pending_report`;
- any path/evidence protected by `abandoned_fenced`;
- any rotated old-previous path still referenced by an in-progress commit.

Everything outside this protected set is eligible garbage only after normal
ownership/age checks.

### 20.3 Hard private-generation bound

Private-generation accumulation is bounded.

Before a new atomic Attempt is admitted, while holding the per-Mirror publication
lock:

1. verify no old-Node writer fence exists;
2. run eligible GC;
3. recompute the protected and garbage sets;
4. compare the remaining private-generation count with the hard bound;
5. check the publication free-space reserve.

If the hard bound is still reached, the new atomic Attempt is not created.

If GC cannot restore the private state below the bound, admission remains
blocked and health remains degraded.

The gate is per owner Node/Mirror storage, not a Server-side estimate of remote
filesystem usage.

### 20.4 Space pressure

Publication-storage free bytes are operational capacity.

Below the explicit reserve, new atomic Attempts are blocked before candidate
creation.

The reserve does not promise that a future candidate will fit; link-dest can
fall back to real copies and repository changes are not predictable.

ENOSPC during pre-visibility candidate construction fails the Attempt without
changing the published tree.

Post-visibility storage/durability failures use publication recovery semantics
rather than ordinary retry.

### 20.5 Previous is not rollback state

The previous generation is retained for serving-reference grace, diagnosis, and
operator recovery.

It is a hard-link-sharing namespace generation, not an isolated snapshot.

M4 provides no automatic rollback API.

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

Compatibility tests must use frozen M3 wire fixtures captured from the accepted
M3 implementation baseline, not JSON generated by M4 structs.

At minimum the repository freezes verbatim fixtures for:

~~~text
M3 PollRequest JSON
M3 PollResponse JSON
M3 Direct ProcessRunSpec JSON
~~~

Those fixtures are immutable compatibility artifacts. Where practical, CI should
also run an integration check against an Agent built from the accepted M3
baseline, but the frozen wire fixtures are the minimum release gate.

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
place.

Before downgrade, the M4 tooling must prove there is no protected publication
recovery evidence. If `preparing_exchange`, `ready_to_commit`,
`visible_pending_durability`, `committed_pending_report`, or
`abandoned_fenced` exists, downgrade is refused until M4 recovery/abandon procedures resolve it.

A valid downgrade then requires:

- stop Server/Agents;
- restore the pre-M4 control-plane backup using the existing restore contract;
- restore the matching pre-M4 authoritative TOML bundle/configuration;
- install matching older binaries;
- preserve Mirror data;
- archive/reset only spool records proven safe for M3.

This prevents M3 restore tooling from discarding publication evidence it does
not understand and prevents M3 from parsing an M4 `[publication]` config.

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
20. supported XFS/ext4/Btrfs rename probes where integration infrastructure permits;
21. ready_to_commit is durably recorded before every visibility commit;
22. restart recovery does not convert publication-recovery phases to Interrupted;
23. persistent post-visibility fsync failure can be explicitly abandoned/fenced;
24. reset-spool/restore/downgrade refuse protected publication evidence;
25. GC never deletes the frozen protected set;
26. hard private-generation bound blocks admission until GC recovers;
27. previous generation is reconstructed deterministically through the fixed
    exchange slot;
28. Move apply versus manual/scheduled Run creation is transactional and has only
    the two valid race outcomes;
29. compatibility tests consume frozen M3 wire fixtures, not M4-generated legacy
    structs;
30. downgrade restores matching pre-M4 TOML as well as database/binaries;
31. preparing_exchange is durable before the first exchange/gc namespace mutation;
32. crash during exchange-slot preparation recovers or restores the private
    namespace without losing previous-generation ownership;
33. abandon/fence rejects mismatched Run/Attempt/spec identity;
34. abandon/fence and fence-clear serialize through the same publication lock;
35. durable local fence exists before Server terminalization;
36. an old-Node fence blocks every LMT writer for that Mirror, including direct
    mode;
37. GC scan/delete/admission race tests share the publication lock with
    commit/recovery/fence-clear;
38. preparing_exchange paths are always part of the GC protected set.

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
