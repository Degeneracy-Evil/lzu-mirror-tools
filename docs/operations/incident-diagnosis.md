# Incident Diagnosis

M3 operational diagnosis should start with the CLI and API, not SQLite shell edits.

## First command

~~~text
lmt doctor
~~~

Doctor is read-only and returns stable check IDs.

Use JSON output when attaching diagnostics to automation or issue reports.

## Server unavailable

Check:

1. systemd service state;
2. daemon journal;
3. single-instance lock conflict;
4. database path and permissions;
5. filesystem free space;
6. schema/future-version error;
7. operator token file readability.

Serving of existing mirror files should remain unaffected.

## Node offline

Check:

1. Agent service;
2. Agent journal;
3. Agent token validity;
4. credential last_used;
5. Agent binding conflict;
6. Server URL/connectivity;
7. spool directory lock/permissions.

Do not reassign the Mirror automatically.

## agent_binding_conflict

This means a second Agent installation is presenting a credential for the same Node.

Do not immediately replace the binding.

Verify the old Agent is stopped or isolated.

Only then explicitly bind the intended installation.

If a potentially executing old Attempt exists, treat force replacement as a duplicate-writer risk.

## Mirror overdue/due

Inspect:

~~~text
lmt mirror show <name>
lmt run list --mirror <name>
lmt node show <owner>
~~~

Determine whether the cause is:

- Node offline;
- Agent capacity full;
- active long Run;
- retries;
- upstream failure;
- disabled/removed config;
- scheduling/config issue.

## Run failed

Use:

~~~text
lmt run show <id>
lmt run logs <id>
~~~

Check each Attempt and failure category.

A retrying Run remains one logical Run with multiple Attempts.

## Missing/expired logs

log_expired is expected retention behavior.

log_missing is unexpected storage inconsistency and should be investigated.

Run/Attempt metadata remains available even when a log file expired.

## Backup unhealthy

Use:

~~~text
lmt backup list
lmt backup verify <id>
~~~

A local backup on the same disk is not disaster recovery.

Copy verified backups off-host.

## Restore incident

Never restore while Agents continue to execute.

Follow backup-restore.md exactly.

Mirror data is not part of control-plane restore and should not be deleted as part of this procedure.

## Do not repair with ad-hoc SQL

If an operational state cannot be resolved through documented APIs/runbooks, capture doctor output and daemon logs first.

For an Atomic publication incident, stop the local Agent and capture:

~~~text
lmt-agent --config /etc/lmt/agent.toml doctor
lmt-agent --config /etc/lmt/agent.toml publication status --mirror MIRROR
~~~

Follow `atomic-publication.md` for exact retry-durability, abandon/fence, and
fence-clear semantics. Do not delete protected spool records or manually rename
the published/exchange directories.

A missing operator workflow should become a product/design issue rather than an undocumented SQLite mutation.
