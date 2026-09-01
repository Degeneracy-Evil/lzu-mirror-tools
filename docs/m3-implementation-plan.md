# M3 Implementation Plan

Status: implementation candidate landed at `73d90897733a1a2e98aa655c3dda0f562ed33d33`; focused hardening is complete and awaiting review round 2.

Authoritative behavior: docs/m3-design.md.

M1 and M2 are accepted regression baselines and must remain green throughout M3.

## M3.0 — schema-v3 and historical fixture

Before operational features:

- freeze accepted schema-v2 test fixture;
- add 0003_m3.sql;
- add node credential metadata;
- add Node Agent-binding fields;
- add log expiration metadata;
- add required pagination/retention indexes;
- keep forward-only migration behavior.

Acceptance:

- populated frozen v2 fixture upgrades;
- future schema still fails closed;
- v1/v2 data remains queryable;
- no historical fixture is regenerated from current migrations during the test.

## M3.1 — process locks and durable Agent identity

Implement:

- Server exclusive control-plane lock;
- Agent spool-directory exclusive lock;
- durable Agent installation ID;
- per-process boot ID diagnostics;
- Node binding on first M3 poll;
- conflicting Agent rejection;
- explicit binding replacement API/CLI with safety preconditions.

Acceptance:

- duplicate local Server refuses;
- duplicate local Agent refuses;
- Agent restart keeps installation ID;
- different installation using same valid Node token never gets a dispatch;
- high-risk binding replacement requires acknowledgement when work may still execute.

## M3.2 — production credentials

Server config:

- operator_token_file;
- deprecated M2 inline compatibility bridge;
- remove production dependence on inline Agent tokens.

Store/API:

- issue/list/revoke Agent credentials;
- random 256-bit token generation;
- last_used tracking with write throttling;
- idempotent revocation;
- one-time raw token response.

CLI:

- credential issue writes requested token file atomically mode 0600;
- list/revoke UX.

Reload:

- Agent credential reload;
- Server operator-token reload;
- failed reload preserves old credential.

Acceptance:

- complete rotation E2E while a long Run remains alive;
- legacy inline Agent entry cannot resurrect revoked DB credential;
- no raw token in DB/logs/metrics.

## M3.3 — CLI operational UX and bounded queries

Add client TOML and explicit output mode.

Implement:

- human tables;
- --output json;
- documented exit codes;
- Run filters;
- default/max limit;
- keyset before cursor;
- latest-Attempt default for logs.

Acceptance:

- no command requires environment-variable configuration;
- JSON output is parseable/stable enough for M3 automation;
- pagination tests prove no duplicate/skip.

## M3.4 — Run log follow and lifecycle

Implement bounded long-poll log reading and CLI --follow.

Replace permanent strong log-lock registry entries with weak/evictable ownership.

Add log retention policy:

- optional age;
- optional total byte cap;
- terminal+complete logs only;
- DB expired_at before unlink;
- HTTP 410 for expired logs.

No application-level compression.

Acceptance:

- live E2E log follow;
- retry Attempt selection;
- age/size retention matrix;
- restart after expire-before-unlink;
- no active/incomplete deletion;
- registry lifetime test.

## M3.5 — online backup and offline restore tooling

Enable rusqlite backup feature.

Online Server operation:

- backup create/list/verify under configured backup directory;
- temp -> integrity -> checksum -> fsync -> rename -> manifest;
- one backup at a time.

Local lmt-server maintenance:

- offline backup;
- restore verification;
- exclusive Server lock;
- restore-recovery normalization.

Local Agent recovery helper:

- reset/archive Attempt spool while preserving Agent ID;
- refuse if Agent lock is held;
- never touch mirror_root.

Acceptance:

- concurrent-write online backup test;
- corruption verification;
- incomplete temp ignored;
- restore lock failure while Server active;
- restored active Runs cannot redeliver stale work;
- restore workflow integration test with stopped Agents.

## M3.6 — metrics, status, and doctor

Replace history-scanning /metrics paths with bounded Store aggregate queries.

Add useful bounded-cardinality per-Mirror/per-Node metrics.

Add sanitized status projection with public opt-in disabled by default.

Implement lmt status and lmt doctor.

Doctor checks DB, storage, Nodes, Mirrors, stale state, backups, logs, binding, and deprecated config.

Acceptance:

- large Run-history metrics sanity test;
- public status leaks no source/path/secret fields;
- doctor unhealthy exit code = 8;
- doctor itself is read-only.

## M3.7 — daemon logging and systemd production units

Add explicit TOML logging level/format.

Ensure structured context for important operations and never log bearer secrets.

Promote systemd drafts:

- permissions/modes;
- RuntimeDirectory/StateDirectory;
- server hardening;
- conservative Agent hardening;
- reload action;
- process lock paths;
- restart semantics.

Add production layout docs.

Acceptance:

- service fixtures run under testable Linux/systemd assumptions where practical;
- Agent unit still executes representative command/rsync workloads;
- Server unit can write DB/log/backup state;
- credential reload works with unit reload signal.

## M3.8 — observability/examples/runbooks

Add:

- Prometheus scrape example;
- Grafana overview dashboard/example;
- journald/Loki guidance;
- representative mirror config examples;
- credential runbook;
- backup/restore runbook;
- log-retention runbook;
- incident-diagnosis runbook.

Do not bundle or manage Prometheus/Grafana/Loki.

## M3.9 — final production-trial fault matrix

Run all previous tests plus:

- credential rotation/revoke;
- duplicate Agent fencing;
- binding replacement;
- log follow;
- retention crash points;
- online backup during writes;
- restore normalization;
- public status sanitization;
- bounded history/metrics;
- operator reload;
- Agent reload while Run active.

M3 is complete only when:

- every M3 release gate in m3-design.md passes;
- M1/M2 matrices remain green;
- no manual SQLite mutation is required by documented normal operations;
- production-trial configuration/runbooks are complete;
- no M4/M5 feature is introduced.

After M3 acceptance, LMT may enter a controlled LZU production trial before M4 community-release hardening.
