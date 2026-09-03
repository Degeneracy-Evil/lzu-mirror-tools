# Post-M4 Production Rollout and v1 Stabilization

Status: **active release phase; no M5 feature development authorized**.

Accepted M4 implementation baseline:

~~~text
7eaeaff92f17e3184543bdf32e50d99881f7d70d
~~~

M4 solved the evidence-backed control-plane and publication problems. The next
step is to use that baseline in the real LZU mirror infrastructure and stabilize
the public release contract. This is deliberately not a new feature milestone.

## 1. Release-candidate boundary

Before real production rollout:

1. change the project version from the historical `0.1.0-alpha.1` development
   value to `0.9.0-rc.1`;
2. build the deterministic release archive from the accepted code baseline plus
   current documentation;
3. record the Git commit and SHA-256 together;
4. tag the exact release commit;
5. make no feature changes in the RC cut.

The earlier alpha artifact is not the production deployment artifact because M4
acceptance fixes landed afterwards.

## 2. Production rollout principle

Do not enable the old synchronization system and LMT as concurrent writers for
the same Mirror.

LMT failure does not remove already-published files from the serving path, so a
control-plane incident normally calls for disabling new synchronization and
diagnosing it, not immediately starting a second writer.

Roll out Mirror by Mirror.

Recommended order:

1. install Server/Agent with no production Mirror writing yet;
2. run doctor/preflight and verify status/metrics/logging;
3. import/review desired configuration;
4. choose one small, low-risk real Mirror;
5. stop its old scheduler/writer;
6. perform one LMT synchronization and publication;
7. verify serving through the real Nginx path;
8. then migrate a more active Mirror;
9. migrate a large Mirror only after real space amplification/GC behavior is
   understood.

Do not perform broad destructive fault injection on the live site. M1-M4
automated fault matrices already cover crash/retry/cancel boundaries.

## 3. Mirror migration classification

Every existing Mirror should be classified before migration.

### Atomic-compatible rsync

Use Atomic mode when the existing sync requirements fit the audited
fresh-generation rsync profile.

Remember:

~~~text
Atomic rsync != existing-destination rsync
~~~

Old destination-only files do not carry forward.

### Direct-only rsync

Keep Direct mode when the Mirror depends on receiver-history semantics or an
option rejected by the Atomic profile.

Direct remains a supported mode; Atomic is not mandatory.

### Custom candidate producer

Use a trusted custom command when repository-specific synchronization or
validation can explicitly materialize a complete candidate.

Do not add generic hooks/workflows merely to migrate one repository.

## 4. Storage/serving requirements for the real site

The full LZU mirror storage design must preserve these M4 assumptions:

- `mirror_root` and `publication_root` for one Atomic Agent are on the same
  local mounted filesystem;
- `publication_root` is outside the Nginx-served namespace;
- the serving stack treats Atomic published content as read-only;
- free-space reserve and private-generation bound are explicit site policy;
- Nginx open-file caching is reviewed separately from LMT publication;
- external tools do not mutate managed Atomic published trees in place.

This means storage/filesystem design is now a first-order dependency of the
whole mirror-site rebuild, not an LMT implementation detail.

## 5. Production evidence to collect

Collect normal operating evidence, not synthetic feature requests:

- actual sync duration and transfer volume by Mirror;
- candidate/publication disk amplification;
- hard-link dedup effectiveness;
- GC backlog and publication-root free space;
- scheduler delay and Agent capacity;
- Run/Attempt failure categories;
- Nginx serving behavior across publication;
- operational effort for upgrade/recovery.

If these measurements expose a missing independent lifecycle/resource, that is
evidence for a future milestone.

## 6. Filesystem support before v1

XFS has real M4 publication smoke coverage.

Before advertising ext4 or Btrfs as fully production-validated for v1, run the
same real rename/no-replace/fsync/exchange smoke on those filesystems. Loopback
or disposable local filesystems are sufficient for this compatibility check;
new hardware is not required.

Alternatively, document them as capability-probed but not production-validated.

Do not hardcode filesystem-name allowlists.

## 7. v1 stabilization work

After real rollout begins, complete a stable-release review that is separate
from M5 feature work.

Required topics:

### Version/API contract

- decide the final `v1.0.0` compatibility promise;
- define supported Server/Agent rolling-upgrade windows;
- decide when `/api/v1alpha1` becomes a stable API family;
- keep compatibility promises narrower than what is actually tested.

### Security/threat model

Document:

- trusted management-network assumption;
- bearer-token exposure risk over plaintext HTTP;
- when operators must use a TLS reverse proxy, VPN, or otherwise protected
  transport;
- operator-token and Agent-token trust boundaries;
- filesystem/service-user boundaries;
- backup and credential handling.

Do not claim a security review merely because authentication tests pass.

### Platform/support matrix

State the tested baseline explicitly, including:

- supported Linux distributions;
- systemd requirement;
- local-filesystem requirement for Atomic mode;
- XFS/ext4/Btrfs validation status;
- rsync requirements;
- upgrade/downgrade support.

### Distribution

The deterministic tarball plus idempotent installer is sufficient for the first
RC.

Distribution-specific DEB/RPM packaging is useful but not a blocker unless real
operators require it.

## 8. What does not belong in this phase

Do not start:

- OCI/container runner;
- automatic placement/failover;
- multiple storage pools;
- PostgreSQL/controller HA;
- OIDC/RBAC;
- generic repository validation pipeline;
- workflow/DAG engine;
- filesystem-specific publication backend.

Those remain evidence-driven M5 candidates.

## 9. When M5 may start

M5 should be designed only after real deployment demonstrates a concrete missing
capability.

Examples:

- several independent disks on one Agent become operationally painful ->
  consider StoragePool;
- repository metadata needs repeatable shared verification semantics ->
  consider a Verify lifecycle;
- node outages make manual ownership Move unacceptable ->
  revisit failover/leases;
- many independent operators require differentiated permissions -> consider
  RBAC;
- synchronization programs genuinely require isolated container images ->
  consider OCI runner.

The production problem chooses M5; the roadmap does not.
