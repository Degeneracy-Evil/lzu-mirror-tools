# M3 Code Review Round 2 — 2026-09-01

Reviewed implementation head:

~~~text
8d0c032c37d6bb34c1e398e6d68e31c20ef28881
~~~

GitHub Actions:

~~~text
run 33525259296
conclusion: success
~~~

## Verdict

**M3 is accepted.**

The focused hardening pass resolves every release blocker from \`m3-review-2026-09-01.md\` without changing the accepted M1/M2 architecture or introducing M4/M5 scope.

This commit is the accepted M3 implementation baseline.

## Blocker closure

### B1 — Agent installation identity publication

Resolved.

Agent identity creation now uses a unique temporary file, fsync, atomic publication, parent-directory fsync, and canonical ULID validation.

Stale legacy temporary artifacts no longer block startup.

The Agent lock remains held around identity creation.

### B2 — credential issuance/local publication

Resolved.

The CLI now:

- preflights the target and parent directory before central issuance;
- creates a unique mode-0600 temporary file;
- publishes without overwriting an existing target;
- compensates a post-issuance local failure with best-effort credential revocation;
- reports the credential ID when cleanup cannot be confirmed;
- never prints the raw secret in the failure path.

Failure and cleanup behavior are tested.

### B3 — complete/bounded Run-log streaming

Resolved.

Normal and follow log display share the same logical-offset chunk reader.

Completed logs larger than one 64 KiB response are fully consumed.

Human mode writes chunks directly.

Machine mode emits bounded JSON Lines per chunk rather than buffering an arbitrarily large log into one object.

### B4 — expired-log retransmission

Resolved.

Once \`expired_at_ms\` is set, a late Agent upload is acknowledged using the historical stored offset without recreating the central file or clearing expiration.

The retention test now explicitly exercises late retransmission after unlink.

### B5 — bounded stored-log metrics

Resolved.

Corrective M3 migration \`0004_m3_hardening.sql\` adds a singleton \`operational_counters\` row maintained by SQLite triggers.

\`operational_counts()\` reads \`stored_log_bytes\` directly rather than scanning historical \`attempt_logs\` during every Prometheus scrape.

The large-history metrics test now includes 10,000 Run, Attempt, and log-metadata rows.

### B6 — WAL-coherent offline restore

Resolved.

The restore path now:

- verifies and normalizes the replacement snapshot first;
- requires a complete \`wal_checkpoint(TRUNCATE)\` of the old control-plane database under the Server lock;
- fsyncs the coherent old main database;
- archives it before replacement;
- removes stale sidecars before installing the replacement;
- restores the archived old database if replacement installation fails.

A real maintenance-path test creates committed old WAL state in a subprocess, injects installation failure, verifies rollback data, verifies Server-lock exclusion, performs the actual restore, and proves restored stale work cannot be redispatched.

### B7 — persistent backup recency

Resolved.

Server initialization reconstructs \`lmt_backup_last_success_timestamp_seconds\` from published backup manifests.

The metric no longer resets to zero merely because the Server process restarted.

## Regression status

The accepted M1/M2 fault matrices remain present.

The hardening suite includes the production-operation regressions required by the first M3 review.

GitHub Actions run \`33525259296\` passed:

- formatting;
- all-target/all-feature Clippy with warnings denied;
- locked all-feature test suite;
- clean-worktree checks.

The development Agent additionally reported a successful locked all-target workspace build and 84 tests, including all six inherited real-process E2E/fault scenarios.

## Non-blocking observations

The following remain intentionally non-blocking and should be evaluated during the controlled production trial rather than expanded now:

- human CLI tables can be made prettier from operator feedback;
- doctor warning thresholds may need tuning after observing real mirror freshness;
- doctor performs a relatively heavy log-file consistency scan, which is acceptable because it is operator-invoked;
- backup recency proves a backup was successfully published, while explicit \`backup verify\` remains the integrity check for current backup contents.

None justify delaying M3 acceptance.

## Accepted baseline

~~~text
M1: e2c27dbdc573dc374c94902255265adc81b2ae10
M2: 7ad3886c1c11d011ae6fd76df2fa6ecc1b87bdaf
M3: 8d0c032c37d6bb34c1e398e6d68e31c20ef28881
~~~

LMT may now enter a **controlled LZU production trial**.

M4 is not authorized for implementation yet. Production-trial evidence should be collected before M4 design is frozen.
