# M3 Design — Production Operations and Hardening

Status: frozen for M3 implementation.

M1 established a safe execution substrate. M2 made LMT an unattended mirror scheduler. M3 makes the accepted M2 system comfortable and safe to operate as a serious LZU production trial.

M3 is an **operations milestone**, not another scheduling milestone.

The M3 goal is:

> An operator should be able to install, authenticate, observe, back up, rotate credentials, inspect incidents, retain logs, and recover the LMT control plane without understanding internal implementation details or editing the database by hand.

M3 must preserve every accepted M1/M2 correctness invariant.

## 1. M3 scope

M3 adds:

- production-oriented CLI configuration and output;
- bounded/paginated operational queries;
- Run log follow;
- Run log retention by age and/or storage cap;
- Agent credential issue/rotation/revocation;
- file-based operator secret configuration;
- credential reload without interrupting active synchronization;
- durable Agent installation identity and Node binding;
- local single-instance process locks;
- safe SQLite online backup;
- explicit offline/quiesced restore procedure;
- schema-v3 migration and frozen v2 fixture;
- bounded-cost metrics and useful per-Mirror/per-Node status;
- operator diagnostics/doctor command;
- structured daemon logging configuration;
- conservative systemd/service hardening;
- public/read-only status surface suitable for a later status frontend;
- representative production examples and operational documentation.

M3 does **not** add:

- HA controller;
- PostgreSQL;
- automatic node placement or failover;
- OCI/container runner;
- storage/snapshot orchestration;
- OIDC/RBAC;
- generic audit database;
- application-level Run-log compression;
- bundled Prometheus/Grafana/Loki;
- stable-v1 compatibility guarantees;
- distribution packaging/release artifacts.

Those remain M4/M5 concerns unless production trial evidence changes the plan.

## 2. M1/M2 invariants remain binding

M3 must not weaken:

1. serving remains independent from LMT;
2. one Mirror has at most one non-terminal Run;
3. one execution key has at most one supervised Agent execution;
4. at-least-once delivery remains idempotent;
5. retries remain Attempts inside one Run;
6. cross-node failover never happens automatically;
7. config pruning never deletes mirror data;
8. all queryable business state remains centralized;
9. Run logs remain outside SQLite BLOB storage;
10. scheduler/retry/cancel correctness is reconstructible from durable state;
11. an Agent process tree is closed before its Attempt becomes terminal;
12. config remains visible in TOML rather than hidden environment-variable behavior.

Operational convenience cannot bypass these rules.

## 3. M3 schema version 3

M3 introduces migration 0003_m3.sql.

Before implementation begins, commit an immutable accepted-v2 schema/data fixture under the Store tests. Future migration tests must create the previous database from that frozen artifact instead of reconstructing history from the current migration source.

Schema v3 conceptually adds:

### node_credentials

~~~text
label TEXT
last_used_at_ms INTEGER
~~~

Existing credential rows remain valid. Legacy M1/M2 credential IDs such as bootstrap remain queryable and revocable.

### nodes

~~~text
bound_agent_id TEXT
agent_boot_id TEXT
~~~

bound_agent_id is a durable Agent installation binding described below.

agent_boot_id is current-process diagnostics only.

### attempt_logs

~~~text
expired_at_ms INTEGER
~~~

A historical Attempt log may therefore be:

- present;
- complete but expired by policy;
- unexpectedly missing.

Run/Attempt records themselves are not deleted by M3 log retention.

### indexes

Add only indexes demonstrated by M3 query paths, including:

- active credential lookup/listing;
- retention candidate ordering;
- Run trigger/history pagination where needed.

M3 must not create generic maintenance/job queue tables.

## 4. Server and Agent single-instance enforcement

systemd usually prevents duplicate service instances, but M3 must not rely on that convention for correctness.

### Server

lmt-server acquires an advisory exclusive process lock before opening the authoritative database for normal service or restore operations.

The lock path is derived from the Server runtime/state layout rather than being another hidden configuration source.

If the lock cannot be acquired:

~~~text
another lmt-server or offline restore operation owns this control plane
=> refuse startup
~~~

This prevents accidental manual double-start of the single-controller architecture.

### Agent

lmt-agent acquires an exclusive lock inside its spool state directory before recovery or polling.

Two processes using the same Agent state cannot run concurrently.

The lock is automatically released by the kernel if the process dies.

This protects the local durable spool from cross-process races.

## 5. Durable Agent installation identity

M2 generated an Agent instance ID per process. That is insufficient as a production fencing identity.

M3 introduces a durable random **Agent installation ID** stored under the Agent state/spool directory.

Properties:

- generated once on first startup;
- persisted atomically;
- mode/parent permissions follow Agent state policy;
- survives service restart and credential rotation;
- not operator-authored configuration;
- never changes merely because the process crashes.

A separate random boot/process ID may be reported for diagnostics.

The existing wire name agent_instance_id may remain during v1alpha1 if implementation churn would otherwise be excessive, but its M3 semantic meaning becomes the durable installation identity. If an explicit agent_id field is introduced instead, all M3 docs/tests must use the new meaning consistently.

## 6. Node-to-Agent binding

A bearer token proves which Node identity is authorized. It does not by itself prove that only one Agent installation is using that identity.

Without binding, two independently configured Agents with the same Node credential and different spool directories can receive the same StartAttempt and execute concurrently.

M3 closes this production safety hole.

### First bind

nodes.bound_agent_id initially NULL after the v3 migration.

The first authenticated M3 Agent poll for that Node atomically binds the Node to the presented durable Agent ID.

### Normal poll

If:

~~~text
presented agent_id == bound_agent_id
~~~

poll proceeds.

### Conflict

If a different Agent installation presents a valid credential for the same Node:

~~~text
HTTP 409
code = agent_binding_conflict
details:
  bound_agent_id
  presented_agent_id
~~~

No Start/Cancel action is dispatched to the conflicting Agent.

This is fencing against accidental duplicate installations, not HA failover.

### Replacement

Hardware/reinstallation replacement is explicit.

The conflicting new Agent ID can be learned from the error/Node diagnostic surface.

Operator command concept:

~~~text
lmt node binding replace mirror01 --agent-id <new-id>
~~~

By default replacement is allowed only when no potentially-executing dispatched Attempt remains on the Node.

If such work exists, replacement requires an explicit high-risk acknowledgement flag. The command must clearly state that LMT cannot fence a still-running old process from another installation.

The old Agent should be stopped or isolated before binding replacement.

There is no automatic instance takeover.


## 7. Credential architecture

M3 keeps bearer-token authentication because the deployment is still a small trusted-infrastructure control plane.

It does not introduce OIDC, mTLS PKI, Vault, or RBAC.

The credential model is intentionally split.

### Operator root credential

The primary operator credential remains a single root/admin bearer secret configured by file path:

~~~toml
operator_token_file = "/etc/lmt/operator.token"
~~~

The raw secret is not written inside production server.toml.

Permissions should normally be:

~~~text
root:lmt
0640
~~~

M3 does not add multi-user operator identities or roles. That is a future stable/security decision.

### Agent credentials

Agent credentials are dynamically managed in central SQLite.

The Agent still reads its raw secret from:

~~~toml
[server]
token_file = "/etc/lmt/agent.token"
~~~

The Server stores only a cryptographic digest.

## 8. Random Agent token format

New M3 Agent tokens are always Server-generated from the operating-system CSPRNG.

Use at least 256 bits of random secret material.

A human-recognizable prefix is recommended, for example:

~~~text
lmt_a_<base64url-secret>
~~~

The raw token is returned exactly once when issued.

Because the generated token has full cryptographic entropy, SHA-256 storage is sufficient as a one-way lookup digest; M3 does not treat these tokens as low-entropy passwords.

Raw bearer secrets must never be:

- written to SQLite;
- logged;
- exposed in list/show APIs;
- embedded in metrics.

Credential issue responses must use Cache-Control: no-store.

## 9. Agent credential lifecycle

One Node may have multiple simultaneously-active credentials.

That overlap is deliberate and enables safe rotation.

### Issue

Conceptual API:

~~~text
POST /api/v1alpha1/nodes/{node}/credentials
~~~

Request:

~~~json
{
  "label": "rotation-2026-09"
}
~~~

Response contains:

- credential ID;
- Node;
- label;
- created time;
- raw token exactly once.

Credential IDs should be opaque IDs such as ULIDs.

### List

~~~text
GET /api/v1alpha1/nodes/{node}/credentials
~~~

Returns metadata only:

- ID;
- label;
- created_at;
- last_used_at;
- revoked_at.

### Revoke

~~~text
POST /api/v1alpha1/nodes/{node}/credentials/{id}/revoke
~~~

Revocation is idempotent.

A revoked token is rejected immediately on future requests.

Revocation does **not** remotely kill a process already executing on that Node.

### last_used

The Server records which credential successfully authenticated an Agent poll.

last_used_at should be write-throttled, for example no more than once per minute per credential, to avoid turning heartbeat traffic into unnecessary SQLite write load.

Events/log uploads do not need to update last_used.

## 10. Safe Agent token rotation

Recommended zero-interruption workflow:

~~~text
1. lmt node credential issue mirror01 --label ...
2. CLI writes new raw secret to a requested local file with mode 0600
3. deploy/atomically replace /etc/lmt/agent.token
4. systemctl reload lmt-agent
5. wait until the new credential's last_used_at advances
6. revoke the old credential
~~~

Agent reload changes the bearer secret used for subsequent HTTP requests without killing active Attempts.

The Agent does not reload arbitrary execution/storage policy on SIGHUP. Only credential/logging-safe reloadable state should be re-read.

## 11. Operator token reload

lmt-server also supports credential reload:

~~~text
atomically replace /etc/lmt/operator.token
systemctl reload lmt-server
~~~

No database or scheduler restart is required.

Other Server configuration changes still require a normal restart unless explicitly documented otherwise.

A failed reload must keep the previous valid credential active and emit a structured error.

## 12. M2 configuration migration

M3 production examples no longer put raw Agent credentials in server.toml.

However, M2 users may still have:

~~~toml
operator_token = "..."
[[agents]]
node = "mirror01"
token = "..."
~~~

M3 provides one alpha-stage compatibility bridge.

### operator_token

M3 may accept deprecated inline operator_token if operator_token_file is absent.

It emits a clear warning.

Production documentation requires moving to operator_token_file.

### legacy agents

Legacy [[agents]] entries may be imported only once per Node.

Rule:

~~~text
if the Node already has ANY credential history:
    do not re-import or re-enable from config
else:
    import one legacy credential
~~~

Checking all historical rows, including revoked rows, is important: leaving an old token in server.toml must never resurrect a credential that an operator revoked through the M3 API.

A warning tells the operator to remove legacy inline credentials.

This compatibility bridge can be removed before stable v1.

## 13. CLI client configuration

Operators should not have to type --server and --token-file on every command.

M3 adds a visible client TOML, conceptually:

~~~toml
server = "https://mirror-admin.example.edu"
token_file = "/etc/lmt/operator.token"
output = "human"
~~~

Default user location may be ~/.config/lmt/client.toml, with an explicit --config override.

Configuration precedence is simple and documented:

~~~text
explicit CLI flag
> client TOML
> built-in non-secret default
~~~

M3 does not add environment-variable configuration overrides.

Secrets remain in separate token files.

## 14. CLI output contract

M3 makes human output the normal CLI experience.

Global:

~~~text
--output human|json
~~~

Default: human.

JSON mode is for automation and should preserve API field names rather than scrape human tables.

Do not select output semantics implicitly based on TTY detection.

### Stable exit-code categories for M3

~~~text
0  success
2  local usage/config error
3  authentication/authorization error
4  requested resource not found
5  conflict/precondition failure
6  transport/server unavailable
7  server/internal operational failure
8  doctor completed but found unhealthy conditions
~~~

These are pre-v1 but should be documented and regression-tested.

## 15. Bounded Run history queries

M3 stops returning all Run history by default.

Run API supports bounded filtering:

~~~text
mirror
node
state
trigger
limit
before
~~~

Default limit: 50.

Maximum limit: 500.

before is a Run ID cursor. The Store resolves its created time/ID and uses keyset pagination over:

~~~text
created_at_ms DESC, id DESC
~~~

Avoid large OFFSET pagination.

CLI examples:

~~~text
lmt run list
lmt run list --mirror ubuntu --state failed --limit 100
lmt run list --before <run-id>
~~~

Mirror and Node lists remain naturally small.


## 16. Run log follow

The existing log API already uses logical byte offsets.

M3 extends it to efficient bounded long-polling.

Conceptual request:

~~~text
GET /runs/{id}/logs?attempt=N&offset=K&limit=65536&wait=20s
~~~

If new data is immediately available, return it.

If:

- offset == current EOF;
- log is not complete;
- wait > 0;

the Server may wait up to the bounded timeout for a log notification, then re-check durable metadata/file length.

Notification is a latency optimization only.

No correctness state lives in the Notify object.

### CLI

~~~text
lmt run logs <id>
lmt run logs <id> --attempt 2
lmt run logs <id> --follow
~~~

If --attempt is omitted, M3 uses the latest Attempt rather than assuming Attempt 1.

--follow streams bytes incrementally and exits when the log is complete and EOF has been consumed.

## 17. Run log retention

M3 retains Run/Attempt history indefinitely unless a future milestone explicitly introduces history pruning.

Large Run log files are different.

Server configuration may include:

~~~toml
[run_logs]
retention = "90d"
max_total_bytes = 107374182400
maintenance_interval = "1h"
~~~

All retention controls are optional.

If neither retention nor max_total_bytes is configured:

> LMT never deletes Run log files automatically.

This safe default avoids surprising destructive behavior.

### Age policy

Only complete logs belonging to terminal Attempts are eligible.

If older than retention, the Server may expire them.

### Size cap

If non-expired complete Run logs exceed max_total_bytes:

- choose oldest eligible terminal logs;
- expire until projected usage is below the cap.

Never expire:

- incomplete upload;
- non-terminal Attempt log;
- file currently under append/maintenance lock.

### Expiration semantics

Expiration removes only the log file.

The Run, Attempt, failure information, timestamps, and log-expiration metadata remain queryable.

Log API returns:

~~~text
HTTP 410
code = log_expired
~~~

for intentionally expired logs.

If metadata says a log should exist but the file is unexpectedly missing, that is an operational error, not normal expiration. It must be visible to doctor/metrics/logging.

## 18. No application-level Run-log compression in M3

M3 deliberately does **not** add .zst log objects.

Reason:

- live logs are append-only;
- API/CLI use logical byte offsets;
- --follow depends on efficient incremental reads;
- compressed random offset reads would require indexing or repeated decompression;
- crash consistency would require dual-format lifecycle logic.

That complexity does not improve core mirror correctness.

Recommended alternatives:

- put log_dir on ZFS/btrfs with transparent compression;
- use filesystem-level compression/storage policy;
- export daemon logs to journald/Loki, whose retention/compression is external.

This decision may be revisited only with measured Run-log storage evidence.

## 19. Log-lock registry lifetime

The Server keeps a per-Attempt lock so duplicate/retransmitted log writes cannot race.

M3 changes the registry to hold weak references or otherwise evict locks after no request owns them.

The registry itself must not grow once per historical Attempt forever.

Retention and log reads/writes must coordinate through the same Attempt log ownership boundary.

## 20. Online SQLite backup

M3 adds a real database backup operation.

Never copy only lmt.db with cp while a WAL-mode Server is active.

M3 uses SQLite's Online Backup API to create a consistent database snapshot while the live Server continues operating.

The SQLite documentation explicitly provides the Online Backup API for live consistent copies, and rusqlite exposes it behind its backup feature.

### Online operator command

~~~text
lmt backup create
lmt backup list
lmt backup verify <backup-id>
~~~

The remote API does not accept an arbitrary filesystem output path.

Server configuration owns the backup directory:

~~~toml
[backup]
directory = "/var/lib/lmt/backups"
~~~

The Server generates filenames under that directory.

### Backup creation

Conceptual flow:

~~~text
generate backup ID
create .tmp destination mode 0600
SQLite Online Backup API -> destination
PRAGMA integrity_check on destination
read schema version + config revision
SHA-256 final database file
fsync file
atomic rename to final .sqlite
fsync directory
write/rename manifest
~~~

Manifest includes at least:

- backup ID;
- created_at;
- LMT version;
- schema version;
- config revision;
- database size;
- SHA-256 checksum.

Only one backup creation runs at a time.

A crash may leave temporary files; startup/backup-list code ignores and may clean stale .tmp objects.


## 21. What database backup includes

The SQLite backup contains:

- desired Mirror state;
- configuration generations/revisions;
- scheduler state;
- Nodes;
- credential digests/history;
- Runs/Attempts;
- log metadata.

It does **not** contain Run log files.

Run logs are operational artifacts rather than control-plane correctness state.

Operators wanting log disaster recovery should back up log_dir with normal filesystem backup tooling.

The backup database contains token hashes, not raw Agent tokens, but it can still contain sensitive Mirror configuration and should be protected as a secret-bearing infrastructure backup.

M3 does not implement backup encryption.

## 22. Backup placement

The default/example backup directory may be local for convenience, but:

> A backup on the same disk is not disaster recovery.

Production-trial documentation should recommend copying successful .sqlite + manifest pairs off-host or onto a separate backup filesystem using existing institutional backup tooling.

LMT does not implement cloud/object-storage upload.

## 23. Offline backup/restore tools

M3 also gives lmt-server local maintenance subcommands for control-plane disaster operations.

Conceptually:

~~~text
lmt-server backup --config ... --output ...
lmt-server restore --config ... --from ...
~~~

These acquire the same Server process lock and refuse to operate while a normal lmt-server owns the control plane.

This is useful for pre-upgrade backups and disaster recovery without needing the HTTP API.

## 24. Restore is deliberately offline and quiesced

M3 does not expose remote HTTP restore.

Restoring an old central snapshot while Agents continue executing newer Attempts is unsafe.

The supported recovery procedure is:

~~~text
1. stop lmt-server
2. stop every lmt-agent that belongs to this control plane
3. verify all Agent child process groups are gone
4. archive/reset only Attempt spool records on Agents
   (preserve durable Agent installation IDs)
5. verify backup checksum + SQLite integrity
6. restore DB offline
7. run restore-recovery normalization
8. start lmt-server
9. run lmt doctor
10. start Agents
11. verify binding/credentials and schedules
~~~

Mirror data files are never rolled back or deleted by control-plane restore.

## 25. Restore-recovery normalization

A backup may have been taken while Runs were active.

Before a restored database is placed back into service, the offline restore tool normalizes stale non-terminal execution state.

Safe default:

- Pending Run with no dispatched Attempt -> Cancelled due to control-plane restore;
- potentially-dispatched/Running Run -> terminal Failed with Interrupted failure category/message;
- retry_due_at cleared;
- non-terminal Attempt rows normalized consistently;
- Node active_runs reset to zero;
- current boot/process diagnostic state cleared.

Scheduler due/next state remains and can be safely reevaluated after startup.

This prevents an old backup from blindly redelivering an Attempt that belonged to a pre-restore execution world.

The restore tool records a clear failure_message explaining that the Run was interrupted by control-plane restore.

## 26. Agent spool reset for restore

Because an Agent may contain spool records newer than the restored Server snapshot, M3 should provide a local maintenance command or equivalent safe helper that:

- acquires the Agent single-instance lock;
- refuses while Agent service owns the spool;
- removes/archives Attempt JSON/log recovery artifacts;
- preserves the durable Agent installation ID;
- never touches mirror_root.

This operation requires an explicit acknowledgement flag.

It exists only for disaster recovery, not normal maintenance.

## 27. Metrics must have bounded query cost

M2 /metrics loads complete Run history to calculate simple gauges.

M3 removes that behavior.

The Store exposes bounded aggregate/operational queries for:

- pending/running Runs;
- due Mirrors;
- online Nodes;
- log bytes;
- latest success/failure status per Mirror.

A Prometheus scrape must not be O(total historical Runs).

### Useful aggregate metrics

~~~text
lmt_runs_pending
lmt_runs_running
lmt_mirrors_due
lmt_nodes_online
lmt_run_logs_stored_bytes

lmt_backup_last_success_timestamp_seconds
lmt_backup_failures_total
lmt_log_expired_total
lmt_auth_failures_total
~~~

### Bounded-cardinality entity metrics

Per-Mirror and per-Node labels are acceptable because those entity sets are operator-controlled and small.

Useful examples:

~~~text
lmt_mirror_last_success_timestamp_seconds{mirror,node}
lmt_mirror_last_terminal_timestamp_seconds{mirror,node}
lmt_mirror_due{mirror}
lmt_node_online{node}
lmt_node_last_seen_timestamp_seconds{node}
lmt_node_mirror_root_free_bytes{node}
~~~

Never label metrics by:

- Run ID;
- Attempt number;
- credential ID;
- arbitrary failure message.


## 28. Read-only operational status API

M3 adds a small status-oriented projection suitable for CLI status and a future read-only web page.

It reports facts, not an opaque "health score".

Per Mirror useful fields:

- name;
- enabled;
- current Run state if any;
- last Run state/time;
- last successful Run time;
- next due;
- due since.

Do not expose:

- source URLs containing credentials;
- RunSpec;
- logs;
- filesystem paths;
- bearer credentials.

Public access is opt-in:

~~~toml
[status]
public = false
~~~

When public=false, status endpoints require operator auth.

When public=true, only the explicitly sanitized status projection becomes unauthenticated.

Administrative APIs remain authenticated.

## 29. Doctor diagnostics

M3 adds:

~~~text
lmt doctor
~~~

Doctor is read-only.

It reports checks such as:

- Server reachable/version/schema;
- database quick/integrity sanity;
- config revision;
- DB/log/backup filesystem free space where available;
- Node online/offline state;
- duplicate/conflicting Agent binding state;
- overdue/due Mirrors;
- suspicious stale non-terminal Runs;
- unexpected missing Run-log files;
- last successful backup;
- deprecated inline credentials still configured.

Doctor never automatically repairs state.

Exit code 8 means the diagnostic command completed successfully but found one or more unhealthy conditions.

JSON output contains stable check IDs.

## 30. Daemon structured logging

M3 keeps daemon logs separate from Run stdout/stderr.

Server/Agent TOML gains explicit logging configuration:

~~~toml
[logging]
level = "info"
format = "json"
~~~

No hidden RUST_LOG/environment configuration is required for normal operation.

Important structured fields should be present when relevant:

- component;
- version;
- node;
- mirror;
- run_id;
- attempt;
- credential_id;
- error_code.

Raw bearer tokens must never be logged.

Production services continue writing to stdout/stderr so systemd/journald owns daemon-log storage.

Loki remains optional external integration.

## 31. journald/Loki boundary

LMT does not configure host-global journald retention.

Operational docs explain how an administrator may configure journald and an external Loki collector if desired.

Responsibilities remain:

~~~text
Run stdout/stderr -> LMT central Run-log files
LMT daemon events -> journald -> optional Loki
metrics -> Prometheus
dashboards -> Grafana
~~~

Do not duplicate daemon logs into SQLite.

## 32. systemd/service hardening

M3 promotes the unit drafts into production-trial units.

### Server

Server can use stronger sandboxing because it does not execute arbitrary sync workloads.

Expected hardening includes where supported:

- dedicated lmt user/group;
- NoNewPrivileges=true;
- ProtectSystem=strict;
- ProtectHome=true;
- PrivateTmp=true;
- PrivateDevices=true;
- ProtectKernelTunables=true;
- ProtectKernelModules=true;
- ProtectControlGroups=true;
- RestrictSUIDSGID=true;
- restrictive UMask;
- explicit StateDirectory/RuntimeDirectory modes;
- restart-on-failure.

Do not add a restriction that breaks SQLite/log/backup state directories.

### Agent

Agent sandboxing is intentionally more conservative because all sync child processes inherit its service sandbox.

Keep:

- dedicated lmt-agent user/group;
- NoNewPrivileges=true;
- ProtectSystem=full;
- ProtectHome=true;
- PrivateTmp=true;
- KillMode=control-group;
- restrictive Agent state permissions;
- restart-on-failure.

Avoid aggressive syscall/capability restrictions unless real rsync/custom-command fixtures prove they are compatible.

mirror_root writability continues to be controlled by normal Unix permissions rather than duplicating its path in a hidden systemd override.

### reload

Units gain ExecReload for credential reload semantics.

## 33. Recommended filesystem/permission layout

Server:

~~~text
/etc/lmt/server.toml          root:lmt       0640
/etc/lmt/operator.token       root:lmt       0640
/var/lib/lmt/                 lmt:lmt        0750
/var/lib/lmt/lmt.db           lmt:lmt        0640
/var/lib/lmt/logs/            lmt:lmt        0750
/var/lib/lmt/backups/         lmt:lmt        0750
/run/lmt/                     lmt:lmt        runtime lock/state
~~~

Agent:

~~~text
/etc/lmt/agent.toml           root:lmt-agent 0640
/etc/lmt/agent.token          root:lmt-agent 0640
/var/lib/lmt-agent/           lmt-agent      0700
/var/lib/lmt-agent/spool/     lmt-agent      0700
/srv/mirrors/                 site policy: writable by lmt-agent, readable by serving stack
~~~

Exact distro user-management/package installation belongs to M4, but these ownership assumptions are the M3 production-trial contract.


## 34. CLI M3 command surface

Conceptual M3 surface:

~~~text
lmt status
lmt doctor

lmt config validate|plan|apply

lmt mirror list|show|sync

lmt node list|show
lmt node credential issue|list|revoke
lmt node binding show|replace

lmt run list|show|cancel
lmt run logs [--attempt N] [--follow]

lmt backup create|list|verify

lmt maintenance logs plan
lmt maintenance logs run
~~~

M3 does not add imperative Mirror enable/disable commands because TOML remains authoritative desired state.

## 35. Failure/error categories

M3 standardizes API error codes and CLI mapping enough for operations.

Representative error codes:

~~~text
unauthorized
forbidden
not_found
config_invalid
config_revision_conflict
mirror_busy
mirror_ineligible
agent_binding_conflict
credential_not_found
credential_revoked
log_expired
log_missing
backup_not_configured
backup_busy
backup_invalid
restore_requires_offline
state_lock_busy
~~~

Internal SQL/I/O details are logged structurally but not exposed as unstable public semantics.

## 36. Production examples

M3 should include examples for:

- simple rsync mirror;
- rsync with delete/hard-links/numeric IDs;
- command-based custom sync;
- interval schedule;
- cron schedule with explicit timezone;
- retry/timeout policy;
- multi-node config tree;
- production server/agent/client TOML;
- Prometheus scrape;
- Grafana overview;
- journald/Loki integration outline.

Examples must not contain real secrets.

## 37. Operational documentation set

M3 should create and maintain:

~~~text
docs/operations/
  production-layout.md
  credentials.md
  backup-restore.md
  log-retention.md
  observability.md
  incident-diagnosis.md
~~~

These are production-trial runbooks, not distribution-specific package guides.

M4 can later turn them into stable external installation/release documentation.

## 38. M3 fault/release gates

M3 is not accepted until automated tests prove at least:

### credentials

1. Server-generated token has required entropy/format.
2. raw token is never stored in DB.
3. issue -> Agent use -> last_used -> revoke works.
4. revoked token cannot authenticate.
5. overlapping old/new credentials support rotation.
6. legacy config does not resurrect a revoked credential.
7. credential reload does not kill active Attempt.

### Agent identity/fencing

8. two Agent processes cannot own the same spool.
9. first Agent installation binds Node.
10. different Agent installation with same valid token is rejected.
11. binding replacement follows safety preconditions.
12. normal restart preserves Agent installation ID.

### logs

13. --follow crosses multiple incremental uploads and terminates at complete EOF.
14. latest Attempt is default log selection.
15. age retention never deletes active/incomplete logs.
16. size retention deletes oldest eligible logs only.
17. expired log returns 410 without deleting Run history.
18. log-lock registry does not grow with historical Attempts.
19. unexpected missing log is visible to diagnostics.

### backup/restore

20. online backup remains valid while DB writes occur.
21. backup integrity/checksum verification detects corruption.
22. temp/incomplete backup is never advertised as valid.
23. backup excludes raw credentials.
24. restore refuses while Server lock is held.
25. restore normalization eliminates stale non-terminal dispatch.
26. Agent spool reset preserves durable Agent ID and mirror data.

### observability/CLI

27. /metrics cost does not scale by loading all Run history.
28. Run list default/max bounds are enforced.
29. keyset pagination has no duplicate/skip across stable history.
30. public status projection contains no sensitive config/path fields.
31. doctor has deterministic check IDs and unhealthy exit semantics.
32. JSON CLI output is machine-readable and human output remains usable.

### regression

33. every accepted M1 fault test remains green.
34. every accepted M2 fault test remains green.
35. no M4/M5 features are introduced.

## 39. M3 acceptance definition

M3 is accepted when a production-trial operator can:

~~~text
install/configure services
-> enroll Agent credentials
-> rotate credentials without killing sync work
-> detect duplicate Agent installation safely
-> observe mirrors/nodes/runs
-> tail a Run live
-> bound Run-log disk use
-> create/verify online DB backups
-> follow a safe disaster-restore runbook
-> diagnose unhealthy state
-> use Prometheus/Grafana/journald integrations
~~~

without hand-editing SQLite, relying on hidden environment variables, or bypassing the documented state machines.

M3 completion means LMT is ready for a serious LZU production trial.

It does **not** yet mean stable v1/community release.


## 40. Hardening clarifications from implementation review

The first M3 implementation review exposed several lifecycle details that are now part of the M3 contract.

### Crash-safe local identity/secret publication

Durable Agent installation-ID creation must recover from a crash that leaves a temporary publication file but no final file.

CLI credential token-file publication must likewise use a crash-safe temporary-file strategy.

Credential issuance is a distributed two-step operation: Server credential creation followed by local raw-secret persistence. If local persistence fails after issuance, the CLI must best-effort revoke the newly issued credential and surface the credential ID if cleanup cannot be confirmed.

### Complete and bounded Run-log streaming

The Run-log API is chunked by design.

Both normal log display and follow mode must iterate logical offsets rather than assuming one response contains the full log.

No CLI output mode may require buffering an arbitrarily large Run log in memory merely for presentation.

For Run logs specifically, human output is streamed as log bytes and
`--output json` is newline-delimited JSON with one bounded chunk object per
line (`offset`, `next_offset`, `complete`, and `data`). It is intentionally not
one unbounded JSON string.

### Retention finality

Once attempt_logs.expired_at_ms is set, a late Agent log retransmission must not recreate the expired central file.

The Server must still respond idempotently enough for an old Agent spool to reach its acknowledgement/retirement boundary.

### Bounded stored-log metrics

Prometheus current-log-byte reporting must not perform a full historical attempt_logs aggregate on every scrape when log metadata can grow indefinitely.

M3 requires bounded/O(1)-style current aggregate state or an equivalent bounded-cost design.

The hardening implementation uses ordered corrective migration
`0004_m3_hardening.sql` to initialize and transactionally maintain one current
stored-log-byte counter. Migration `0003_m3.sql` remains immutable.

### Restore rollback safety

Offline restore treats the previous SQLite main database plus any relevant WAL/SHM state as one recovery unit.

The implementation must not delete committed old WAL state before the previous control plane has been safely archived/checkpointed.

If installation of the restored database fails, the previous coherent database state must remain recoverable.

### Persistent backup recency

Backup-recency metrics are operational facts derived from published backup manifests, not process-lifetime counters.

Server restart must not reset the last-success metric to zero when valid backups remain on disk.
