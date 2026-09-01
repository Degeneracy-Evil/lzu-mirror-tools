# Run Log Retention

M3 separates Run/Attempt history from large stdout/stderr files.

## History

Run and Attempt database records are retained.

Expiration of a log never deletes:

- Run identity;
- Attempt identity;
- state/failure information;
- timestamps;
- config generation;
- exit code.

## Policy

Server configuration may specify:

~~~toml
[run_logs]
retention = "90d"
max_total_bytes = 107374182400
maintenance_interval = "1h"
~~~

All destructive limits are optional.

If neither retention nor max_total_bytes is configured, LMT does not automatically delete Run logs.

## Eligible logs

Only logs that are:

- complete;
- associated with a terminal Attempt;

may expire.

Never expire an active/incomplete upload.

## Age

Logs older than configured retention are eligible.

## Size cap

When total non-expired complete log storage exceeds max_total_bytes:

- choose oldest eligible logs;
- expire until projected storage is under the cap.

Age retention and size-cap retention may both be enabled.

## Crash ordering

Database expiration state is committed before file unlink.

If the process crashes after marking expired but before unlinking, the API still treats the log as expired and later maintenance may remove the leftover file.

This favors correctness and avoids exposing a file that policy has already declared expired.

## API

Intentional expiration:

~~~text
HTTP 410
error code: log_expired
~~~

Unexpected file absence while metadata says the log should exist is different:

~~~text
log_missing
~~~

and must appear in diagnostics/metrics.

## No application compression

M3 keeps Run logs as normal uncompressed files.

Reason:

- offset-based API;
- live follow;
- append/retransmit semantics;
- simple crash behavior.

Use filesystem transparent compression if storage reduction is desired.

Daemon logs remain journald/Loki concerns.

## Lock ownership

Append, read where needed, and maintenance deletion coordinate through the same per-Attempt log ownership mechanism.

The lock registry must be evictable/weak so historical Runs do not cause permanent in-memory growth.

## Manual maintenance

M3 exposes a dry-run plan and explicit maintenance trigger:

~~~text
lmt maintenance logs plan
lmt maintenance logs run
~~~

The background maintenance loop uses the same policy/selection code.

Manual maintenance does not override eligibility safety rules.
