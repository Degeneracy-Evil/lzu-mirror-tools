# Backup and Restore

M3 and M4 distinguish **online backup** from **offline restore**.

## What is authoritative

The central SQLite database contains control-plane correctness state:

- Mirror desired state and generations;
- scheduler state;
- Nodes and credential digests;
- Runs and Attempts;
- Run-log metadata.

Run-log files themselves are not inside the database backup.

Mirror content under mirror_root is also not inside the control-plane backup.

## Online backup

Use:

~~~text
lmt backup create
lmt backup list
lmt backup verify <backup-id>
~~~

The Server creates backups only under the configured backup directory.

It does not accept an arbitrary remote filesystem path from the HTTP API.

M3 uses SQLite's Online Backup API so a live WAL-mode database is copied as a consistent snapshot.

Do not use a bare cp of lmt.db while the Server is active.

A valid backup is published only after:

- backup copy completes;
- SQLite integrity check succeeds;
- checksum is calculated;
- file is fsynced;
- final rename succeeds;
- manifest is written atomically.

Temporary/incomplete objects are not valid backups.

## Backup manifest

Manifest records:

- backup ID;
- creation time;
- LMT version;
- schema version;
- config revision;
- database size;
- SHA-256 checksum.

Raw Agent/operator tokens are not included.

The database can still contain sensitive Mirror configuration and should be protected accordingly.

## Off-host copy

A backup on the same database disk is only a local recovery copy.

Production trial should copy completed database+manifest pairs to another filesystem/host using institutional backup tooling.

LMT does not upload to object storage in M3.

## Restore safety boundary

There is no HTTP restore endpoint.

Restore is offline and quiesced because an older DB snapshot cannot safely be combined with newer Agent executions/spools.

Supported sequence:

~~~text
1. stop lmt-server
2. stop every related lmt-agent
3. verify Agent child processes are gone
4. inspect every Agent with `lmt-agent publication status --mirror <name>` and
   resolve all protected publication recovery/fence evidence
5. archive/reset only Attempt spool artifacts that `lmt-agent reset-spool`
   proves safe, while preserving Agent installation IDs
6. verify backup checksum and SQLite integrity
7. run offline restore
8. normalize stale non-terminal execution state
9. start lmt-server
10. run lmt doctor
11. start Agents
12. verify bindings, credentials, schedules, and fresh synchronization
~~~

Restore never deletes or rolls back mirror_root.

## Restore normalization

A backup may contain a Run that was active when the snapshot was taken.

Before the restored DB is served:

- undispatched Pending work becomes Cancelled;
- potentially-dispatched/Running work becomes Failed/Interrupted;
- retry deadlines are cleared;
- non-terminal Attempt rows are normalized consistently;
- Node active-run/process diagnostic state is reset.

This prevents stale StartAttempt redelivery from a historical control-plane snapshot.

## Agent spool reset

M3 provides a safe local maintenance path to clear/archive Attempt recovery records after a control-plane restore.

It must acquire the Agent lock, refuse while the Agent runs, preserve durable Agent installation identity, and never touch mirror_root.

M4 publication phases `preparing_exchange`, `ready_to_commit`,
`pre_visibility_recovery`, `visible_pending_durability`,
`committed_pending_report`, and `abandoned_fenced` are correctness evidence.
`reset-spool` refuses to remove them. Complete normal recovery, or use the exact
offline retry/abandon/fence-clear procedure, before retrying reset. Never remove
these JSON records or their referenced private paths manually.

## M3 to M4 forward upgrade

Use this order:

~~~text
create and verify a control-plane backup
-> upgrade lmt-server
-> verify existing M3 Agents still execute Direct Mirrors
-> upgrade lmt-agent on each Node
-> confirm atomic_exchange_v1 is reported after filesystem preflight
-> enable Atomic mode only for selected quiescent Mirrors
~~~

The M4 Server keeps the accepted M3 Direct wire shape. It does not dispatch an
Atomic RunSpec until the owning Agent explicitly advertises the capability and
publication root.

## M4 to M3 downgrade

In-place binary rollback is unsupported. Restore a matching pre-M4 database
backup and authoritative TOML bundle while Server and all Agents are stopped.
Before installing M3 binaries, use M4 tooling to resolve every protected
publication record and run the guarded spool reset. Preserve `mirror_root`,
`publication_root`, Agent installation IDs, credentials, and backups; downgrade
does not migrate or delete mirror content.

## Pre-upgrade backup

The local lmt-server maintenance command can create a backup while the normal Server is stopped and before a schema-changing startup.

This is the recommended pre-migration safety step for future upgrades.
