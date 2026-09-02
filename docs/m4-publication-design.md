# M4 Publication Architecture Design

Status: **proposed for M4 design review**.

This document is the detailed architecture proposal derived from the controlled LZU production trial. It is not an implementation authorization.

## 1. Evidence and problem statement

The production trial demonstrated that the current direct model:

~~~text
rsync -> serving tree <- nginx
~~~

is operationally reliable but not repository-atomic.

In the real kernel.org + Nginx experiment, rsync replaced different files at different times. This means clients can observe a local serving tree composed from more than one synchronization generation.

The problem is therefore not process execution reliability. It is publication visibility.

M4 should solve exactly this problem without turning LMT into a workflow engine, storage orchestrator, or filesystem-specific platform.

## 2. Consistency contract

Atomic publication provides this guarantee:

> LMT never exposes the candidate synchronization tree through the configured published path before the publication commit. The published path changes from the previous complete local tree to the new complete local tree through one atomic namespace operation.

The guarantee is intentionally narrower than transactional repository sessions.

It does **not** guarantee:

- that a client making several HTTP requests is pinned to one generation;
- that a request already holding an old file descriptor switches to the new generation;
- that Nginx or another serving layer immediately invalidates its own open-file cache;
- that the upstream rsync source itself is a point-in-time snapshot;
- semantic repository validity beyond the synchronization program returning success;
- atomicity across several different Mirrors.

This distinction is fundamental.

LMT prevents a partially synchronized **local candidate** from becoming the live serving tree. It cannot manufacture source-side snapshot consistency or cross-request session consistency.

Nginx has `open_file_cache off` by default. If an operator enables open-file caching, an old open descriptor may remain visible until Nginx revalidates it. That is serving-cache staleness, not candidate-tree leakage.

## 3. Mirror identity

M4 should freeze this definition:

> A Mirror is the logical synchronized-and-published mirror resource.

A Mirror is not identified with one concrete directory inode or one historical synchronized tree.

The current owner Node realizes that logical resource using local storage.

Consequences:

- Mirror identity survives Run retries;
- Mirror identity survives config-generation changes;
- an ownership Move does not create a new Mirror;
- a Move changes the owner of future synchronization/publication work only;
- old data on the previous Node remains ordinary unmanaged physical data until an operator handles it.

M4 does not introduce Replica or PhysicalMirror as first-class resources. They still have no independent v1 lifecycle.

## 4. Publication is not a top-level resource

The first M4 draft considered a durable Publication record nested under Run.

The smaller design is better:

> publication commit is the final local phase of an Attempt.

An Attempt means one physical attempt to realize the Run on its owner Node.

For direct mode:

~~~text
Attempt
  |
  +-- execute process
  |
  +-- terminal result
~~~

For atomic mode:

~~~text
Attempt
  |
  +-- prepare fresh candidate
  |
  +-- execute process into candidate
  |
  +-- publication commit
  |
  +-- terminal result
~~~

The Agent reports `AttemptSucceeded` only after the publication commit is complete.

Therefore the existing public projection remains valid:

~~~text
Attempt Succeeded -> Run Succeeded
~~~

No new public Run state, Publication resource, Publication scheduler, or generic phase/DAG model is required.

The publication recovery phase exists only in the Agent durable spool because it is local execution ownership state.

## 5. Public state machine remains small

The public Attempt states remain:

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

Atomic publication uses internal durable Agent phases such as:

~~~text
prepared
executing
ready_to_commit
committed
~~~

These are not wire-visible Attempt states.

`Running` covers both process execution and publication commit.

This avoids exposing implementation phases as user-visible lifecycle states.

## 6. Publication linearization point

For atomic mode, the publication linearization point is the successful atomic namespace switch.

Before that operation:

- old published tree remains authoritative;
- candidate is private;
- cancellation may prevent publication.

After that operation:

- new published tree is authoritative;
- publication must never be automatically rolled back because Server state appears stale;
- restart recovery must converge toward reporting success for that committed Attempt.

The filesystem commit, not a later HTTP event, defines which tree is live.

## 7. Selected generic backend: directory exchange

M4 should implement one generic Linux publication backend first:

> fresh candidate directory + `renameat2(RENAME_EXCHANGE)`.

Assume:

~~~text
mirror_root      = /srv/mirrors
publication_root = /srv/lmt-publication
~~~

A Mirror named `ubuntu` with target `ubuntu` uses:

~~~text
published:
/srv/mirrors/ubuntu

private:
/srv/lmt-publication/ubuntu/
├── attempts/
│   └── <run>-<attempt>/
│       ├── root/
│       └── basis
├── previous/
└── gc-*/
~~~

### Existing published tree

After the candidate has completed successfully:

~~~text
RENAME_EXCHANGE(
  /srv/lmt-publication/ubuntu/attempts/<id>/root,
  /srv/mirrors/ubuntu
)
~~~

The operation atomically exchanges the two directory entries.

After the exchange:

~~~text
/srv/mirrors/ubuntu
    = new published tree

.../attempts/<id>/root
    = old published tree
~~~

The public path remains a normal directory.

### First publication

If the public target does not yet exist, publication uses a no-overwrite rename:

~~~text
candidate -> published
~~~

The operation must fail rather than unexpectedly replacing a path that appeared concurrently.

## 8. Why directory exchange is preferred

### Versus an atomic symlink pointer

A symlink pointer is simpler to inspect after a crash, but it changes the public target from a real directory into a symlink.

That introduces serving-policy dependencies such as Nginx `disable_symlinks`, changes the expectations of external tools, and makes migration of an existing live directory less elegant.

Directory exchange keeps:

~~~text
/srv/mirrors/<target>
~~~

as a real directory forever.

Existing direct-mode data can become the first basis without an offline symlink migration.

### Versus bind mounts

Bind-mount publication requires mount-management privileges and creates new interactions with mount namespaces, systemd sandboxing, propagation, and recovery.

LMT does not need CAP_SYS_ADMIN merely to publish files.

### Versus Btrfs/ZFS snapshots

Btrfs and ZFS snapshots are excellent storage-specific optimization mechanisms.

Btrfs snapshots are CoW subvolumes; ZFS snapshots provide atomic point-in-time dataset snapshots.

However, making either one the M4 semantic foundation would couple the core architecture to storage selection and would require additional mount/dataset lifecycle management.

The M4 semantic contract should remain filesystem-independent.

Snapshot/reflink publication backends may be added later as optimizations if production measurements justify them.

## 9. Filesystem requirements

Atomic-exchange mode requires:

- Linux;
- a local filesystem with working atomic rename semantics;
- `mirror_root` and `publication_root` on the same mounted filesystem;
- `RENAME_EXCHANGE` support;
- both roots writable where required by the Agent;
- published targets must be ordinary directories or absent;
- published targets must not be mount points;
- `publication_root` must not be inside the publicly served `mirror_root`.

The Agent should not infer support from filesystem names.

It should perform a real preflight probe using disposable directories.

If the probe fails, atomic work is rejected before synchronization starts.

Direct publication remains available on unsupported storage.

Network filesystems are out of scope for the M4 atomic-exchange backend.

## 10. Agent storage configuration

Agent configuration gains one explicit optional path:

~~~toml
[storage]
mirror_root = "/srv/mirrors"
publication_root = "/srv/lmt-publication"
spool_dir = "/var/lib/lmt-agent/spool"
~~~

`publication_root` is required only for Mirrors using atomic publication.

No implicit `.lmt` directory is created inside the served mirror root.

This protects private candidates from accidental HTTP exposure and follows the project's preference for visible configuration.

## 11. Mirror configuration

Publication is an explicit desired-state property:

~~~toml
[publication]
mode = "atomic"
~~~

Default remains:

~~~toml
[publication]
mode = "direct"
~~~

or equivalently the section may be absent.

This preserves backward compatibility.

Atomic mode requests a consistency guarantee, not a particular filesystem technology.

The Agent's local capability determines whether it can satisfy that guarantee.

## 12. Candidate semantics

Every Attempt receives a fresh candidate directory.

A candidate is never reused by another Attempt.

An interrupted or failed candidate is never resumed as the destination of a later Attempt.

This invariant is important for both crash reasoning and rsync hard-link safety.

For command Mirrors:

~~~text
{target_dir}
~~~

resolves to the fresh candidate in atomic mode.

A new placeholder:

~~~text
{published_dir}
~~~

may expose the current live path to trusted custom commands when explicitly needed.

Custom commands remain trusted site code. LMT cannot prevent an arbitrary executable from intentionally modifying another writable path.

## 13. Rsync candidate construction

A full second physical copy of multi-terabyte mirrors is not acceptable as the normal M4 design.

For built-in rsync, the Server should compile the candidate transfer using an LMT-controlled `--link-dest` basis.

The Agent prepares an attempt-local basis path:

~~~text
attempt/
├── root/   # empty candidate
└── basis
~~~

If a published tree exists, `basis` points to it.

If no published tree exists, `basis` is an empty directory.

The immutable rsync argv can therefore always include:

~~~text
--link-dest=<attempt>/basis
~~~

The destination `root/` is empty when rsync begins.

Unchanged regular files can be hard-linked to the previous published tree, while changed files are created as new destination files.

This substantially reduces data duplication while keeping future LMT writes away from the published directory.

## 14. Rsync compatibility boundary

Atomic rsync mode is not semantically identical to running arbitrary rsync options against an existing destination tree.

M4 must validate options that conflict with the fresh-candidate model.

At minimum, atomic mode must reject user control over LMT-owned alternate-destination semantics such as:

- `--link-dest`;
- `--copy-dest`;
- `--compare-dest`.

It should also reject options whose semantics depend on mutating or preserving an existing destination inode/tree, including at least:

- `--inplace`;
- `--append`;
- `--append-verify`;
- `--write-devices`;
- `--existing` / `--ignore-non-existing`;
- destination-newness preservation such as `--update` unless its atomic-mode semantics are explicitly defined.

The implementation plan must contain an audited rsync compatibility table before code is accepted.

Mirrors that require incompatible destination semantics may remain in direct mode or use a trusted custom synchronization command designed for candidate output.

## 15. Published-tree immutability invariant

In atomic mode:

> LMT never synchronizes future Runs into the currently published directory.

All future writes go to fresh candidates.

The published tree is effectively immutable from LMT's perspective until it is exchanged out.

This is what makes hard-link sharing with `--link-dest` safe.

The serving plane should be read-only with respect to mirror content.

Manual in-place administrator mutation of the published tree is outside the atomic-publication contract.

## 16. Detecting external replacement

Before building a candidate, the Agent records the identity of the current published directory when it exists.

Immediately before commit, it verifies that the published directory still has the same filesystem/device and inode identity.

If it changed unexpectedly, publication is rejected rather than exchanging against an object LMT did not synchronize against.

This detects:

- manual replacement;
- accidental second writers;
- unexpected storage remount/replacement;
- broken local invariants.

## 17. Crash recovery

The Agent spool remains the durable local execution record.

For atomic Attempts it additionally records:

- candidate path;
- candidate device/inode identity;
- prior published identity when present;
- internal publication phase.

### Crash while process is executing

The candidate is not published.

Recovery follows the existing Interrupted semantics.

A later retry uses a new Attempt and a new candidate.

### Crash after process exit but before commit

If the spool durably reached `ready_to_commit`, the candidate contains a completed process result.

The Agent must not blindly publish it immediately on startup.

It first re-enters normal Server reconciliation so an already-persisted cancellation can win.

If the Server redelivers the matching Start and no cancellation exists, the Agent may continue the same Attempt's commit without re-running synchronization.

### Crash during/after atomic exchange

Namespace exchange itself is atomic.

Recovery compares the stored candidate inode identity with both possible pathnames.

If the candidate inode is now at the published path, publication committed.

The Agent converges toward `AttemptSucceeded` and must not create a duplicate writer or roll back the tree.

If the candidate inode remains at the private candidate path, publication did not commit.

If neither path matches the recorded identity, local publication invariants are broken and the Attempt must fail safely with a publication/storage diagnostic.

## 18. Cancellation race

Cancellation and publication commit must be serialized by the Agent's durable Attempt lock.

The rule is:

> cancellation wins if it becomes durable locally before the publication linearization point; publication wins once the atomic exchange has committed.

A cancellation that exists only on the Server but has not yet reached an offline Agent cannot retroactively undo a commit that already occurred.

This is consistent with the existing at-least-once control protocol.

## 19. Retry behavior

If synchronization fails before commit:

- Attempt becomes Failed/TimedOut/Interrupted as today;
- candidate is private;
- retry policy may create Attempt N+1;
- Attempt N+1 receives a fresh candidate.

A structural publication preflight failure is Rejected and non-retryable.

A runtime publication I/O failure after a successful process should use a new operational failure category such as `publication`.

It may remain retryable under the normal Run policy because the retry is safe, although it may repeat synchronization work.

## 20. Previous generation and cleanup

M4 should keep exactly one immediate previous published tree as an internal safety generation.

After a successful exchange:

~~~text
published = new
candidate path = old published
~~~

The old published tree is normalized to:

~~~text
publication_root/<mirror>/previous/
~~~

Any older `previous` is moved to a garbage path and removed asynchronously.

This provides:

- a generous grace period for serving processes that may still hold old directory/file references;
- useful incident forensics;
- a simple emergency manual recovery source;
- bounded steady-state retention.

There is no automatic rollback API in M4.

Cleanup is best-effort and is not part of Run success.

Agent startup maintenance may remove stale garbage and candidates not referenced by a live spool record.

Configuration pruning never deletes the currently published mirror tree.

## 21. Serving-plane behavior

An in-flight download that already opened an old file may continue reading the old inode after publication. Linux rename does not invalidate open file descriptors.

New pathname resolution sees the public path before or after the directory-entry exchange.

LMT does not promise that several HTTP requests from one client see the same generation.

Serving software can extend old-version visibility through its own caches.

For Nginx, `open_file_cache` is off by default. Sites enabling it must choose cache validation intervals compatible with their desired freshness.

Publication does not require Nginx reloads or API calls.

LMT remains outside the download path.

## 22. Upstream consistency remains separate

Atomic local publication does not prove that the candidate is semantically self-consistent.

For example, an upstream repository may change while rsync is traversing it.

M4 does not introduce a generic Verify phase merely to hide this fact.

If a repository needs special verification today, a trusted custom synchronization command may perform:

~~~text
sync candidate
verify candidate
exit 0
~~~

and LMT publishes only after that command exits successfully.

A first-class verification phase should be added only when real repositories justify a shared lifecycle abstraction.

## 23. Ownership Move semantics

Atomic publication remains Node-local.

Moving:

~~~text
nodes/n01/mirrors/ubuntu.toml
->
nodes/n02/mirrors/ubuntu.toml
~~~

means future Runs belong to n02.

It does not:

- copy n01's publication tree;
- delete n01's publication tree;
- redirect Nginx;
- move external traffic;
- automatically publish on n02.

The old n01 serving tree remains frozen data.

The operator must ensure n02 has a successful publication before moving external serving traffic if that is required.

## 24. Disable, remove, and mode changes

Disable:

- prevents future Runs;
- leaves the published tree available.

Remove:

- removes managed control-plane desired state;
- never deletes published mirror data.

Changing direct -> atomic:

- the existing real published directory may be used as the first basis;
- no symlink migration is required.

Changing atomic -> direct:

- future Runs once again write into the live target;
- the atomic consistency guarantee is lost;
- this should be surfaced as a high-impact config-plan warning.

Private publication-root cleanup is an explicit maintenance concern and must never be confused with config pruning.

## 25. Target-overlap invariant

On one Node, managed Mirror targets must not overlap.

Invalid examples:

~~~text
Mirror A target = ubuntu
Mirror B target = ubuntu

Mirror A target = ubuntu
Mirror B target = ubuntu/pool
~~~

These create independent Run lifecycles that write/exchange the same serving subtree.

M4 config validation should reject exact and ancestor/descendant target overlap per owner Node.

The same relative target on different Nodes is valid because the physical roots are different.

## 26. Protocol capability negotiation

Atomic publication requires a newer Agent.

The Agent should advertise an explicit capability such as:

~~~text
atomic_exchange_v1
~~~

rather than inferring support from version strings.

The existing Node `capabilities_json` field can persist this observation.

A Server must never dispatch an atomic publication spec to an Agent that did not advertise the capability.

Because current v1alpha1 request structs reject unknown fields, the mixed-version upgrade contract should be:

1. upgrade Server first;
2. new Server accepts old Agent polls with missing optional capability fields;
3. upgrade Agents;
4. enable atomic publication only after capability is visible.

Direct-mode Mirrors remain executable by old Agents during the compatibility window.

## 27. ProcessRunSpec extension

The immutable execution spec should gain an optional publication description rather than a new action protocol.

Conceptually:

~~~text
ProcessRunSpec
├── runner/program/args/cwd/timeout
├── target_dir
└── publication
    ├── mode
    ├── published_dir
    ├── candidate_dir
    ├── basis_dir
    └── publication_root
~~~

For direct mode the optional publication section is omitted when serialized, preserving compatibility with older Agents.

The spec hash covers publication paths and mode.

Server still decides.

Agent still executes one immutable local ownership unit.

## 28. Database impact

No Publication table is required.

No public Publication ID is required.

No new Run state is required.

Mirror configuration history already persists publication mode through canonical TOML.

Run/Attempt history already records the exact immutable spec.

M4 may add publication-specific operational metadata only where it materially improves diagnosis.

A `publication` failure kind is justified.

This deliberately avoids creating a second lifecycle in SQLite.

## 29. Observability

Add operational evidence for:

- publication commits succeeded/failed;
- publication preflight rejection;
- commit duration;
- stale candidate/garbage cleanup failures;
- current Agent publication capability.

Do not use Run IDs or Attempt IDs as Prometheus labels.

Structured daemon logs should identify Run/Attempt IDs for incident correlation.

`doctor` should test:

- publication_root configured;
- roots are writable;
- roots are on the same mounted filesystem;
- exchange probe succeeds;
- published target type is valid;
- private publication_root is not below the public mirror_root.

## 30. Durability boundary

The namespace commit should fsync the affected parent directories after rename so directory-entry changes are made durable according to normal local-filesystem semantics.

This does not recursively fsync every byte in a multi-terabyte candidate.

M4 guarantees atomic local publication visibility and robust daemon/reboot recovery under normal filesystem guarantees.

It does not turn mirror synchronization into a fully synchronous power-loss transaction.

Sites requiring stronger storage durability should address that through the filesystem/storage layer.

## 31. M4 installation/upgrade automation interaction

The installer must understand the optional publication root.

For Agent installation with atomic publication support, the operator supplies it explicitly:

~~~text
--mirror-root /srv/mirrors
--publication-root /srv/lmt-publication
~~~

The installer:

- creates both paths with correct ownership;
- never guesses a publication root;
- does not place private generations inside the served tree;
- runs the same exchange preflight used by `doctor`;
- does not modify Nginx.

Upgrade remains Server-first when protocol fields/capabilities change.

## 32. Rejected alternatives for M4

Do not implement in this milestone:

- a generic Publication resource/API;
- workflow/DAG phases;
- automatic repository-specific verification;
- automatic rollback;
- filesystem-specific snapshot providers;
- bind-mount publication;
- symlink-current publication;
- cross-node publication migration;
- replicated Mirrors;
- traffic switching.

These may be revisited only with evidence.

## 33. Acceptance model

Before M4 implementation is accepted, automated tests must cover at least:

1. direct mode remains unchanged;
2. fresh atomic first publication;
3. exchange from an existing live directory;
4. candidate is never published on process failure;
5. cancel before commit prevents exchange;
6. crash before commit does not expose candidate;
7. crash after exchange but before terminal event recovers as the same successful Attempt;
8. retry uses a fresh candidate;
9. old published identity changing underneath the Attempt blocks commit;
10. target overlap is rejected;
11. unsupported Agent capability blocks atomic dispatch;
12. previous-generation cleanup never deletes current published tree;
13. server-first mixed-version direct-mode compatibility;
14. real XFS/ext4/Btrfs exchange probe where CI/integration infrastructure permits.

Only a very small real-host smoke test is needed after automated coverage.

## 34. External semantics relied upon

The design relies on standard Linux/POSIX behaviors:

- Linux `renameat2(RENAME_EXCHANGE)` atomically exchanges two existing pathnames;
- rename does not invalidate already-open file descriptors;
- rename cannot cross mounted filesystem boundaries;
- durable directory-entry persistence requires fsync of containing directories;
- rsync `--link-dest` hard-links unchanged files from an alternate destination and works best with a fresh destination hierarchy;
- rsync `--inplace` deliberately preserves hard links and is therefore not part of the safe atomic-mode contract;
- Nginx can cache open file descriptors when `open_file_cache` is enabled, while the directive is off by default.

Reference documentation:

- Linux rename(2): https://man7.org/linux/man-pages/man2/rename.2.html
- Linux fsync(2): https://man7.org/linux/man-pages/man2/fsync.2.html
- rsync(1): https://download.samba.org/pub/rsync/rsync.1
- Nginx core module: https://nginx.org/en/docs/http/ngx_http_core_module.html
- Btrfs subvolumes/snapshots: https://btrfs.readthedocs.io/en/latest/btrfs-subvolume.html
- OpenZFS snapshots/clones: https://openzfs.github.io/openzfs-docs/Basic%20Concepts/Datasets/Snapshots%20and%20Clones.html
