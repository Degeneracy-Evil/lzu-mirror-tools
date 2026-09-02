# Development Roadmap

This roadmap is intentionally incremental. It preserves the final architecture while delivering useful vertical slices early.

The design phase described in the other documents is considered M0.

## M0 - Architecture and contracts

Status: **complete**.

Deliverables:

- architecture boundaries;
- core model;
- authoritative TOML configuration;
- scheduler semantics;
- state machines;
- central SQLite model;
- Server-Agent protocol;
- HTTP API;
- Rust workspace/dependency design;
- testing strategy.

No production implementation is required in M0.

## M1 - Minimal vertical slice

Status: **accepted** at implementation commit `e2c27dbdc573dc374c94902255265adc81b2ae10` after the second M1 code review.

Goal:

> prove that one server and one Agent can reliably execute one configured Mirror Run end-to-end.

Scope:

- Rust workspace;
- `lmt-core`, `lmt-protocol`, `lmt-store`;
- server startup and migrations;
- Agent startup and bearer authentication;
- Node poll/heartbeat;
- process runner;
- authoritative configuration validate/plan/apply for command Mirrors;
- manual Run creation;
- Attempt dispatch;
- success/failure reporting;
- central Run log upload/read;
- central Run/Attempt history;
- CLI commands required for the vertical slice;
- systemd unit drafts;
- automatic restart behavior;
- basic Prometheus health/runtime metrics.

Explicitly not required:

- cron/interval scheduler;
- built-in rsync sugar;
- container runner;
- web frontend;
- PostgreSQL;
- controller HA.

M1 completion test:

```text
apply TOML
  -> manual sync
  -> Agent executes
  -> central logs
  -> Run Succeeded
  -> restart server
  -> state remains correct
```

## M2 - Mirror scheduling and reliable retries

Status: **accepted** at implementation baseline `7ad3886c1c11d011ae6fd76df2fa6ecc1b87bdaf`. at design commit `76e0a0065c655373ab7aa26fded03c0e4138f71b`.

Authoritative design: `docs/m2-design.md`.

Implementation sequence: `docs/m2-implementation-plan.md`.

Goal:

> make LMT usable as a real unattended mirror synchronization manager.

Scope:

- interval scheduler;
- cron scheduler with timezone support;
- catch-up/coalescing;
- retry delay/max attempts;
- timeout;
- cancellation;
- disable/remove semantics;
- server restart schedule reconstruction;
- Agent crash/interruption recovery;
- built-in rsync sync type compiling to the process runner;
- scheduler/Run Prometheus metrics;
- config generation/revision UX polished.

M2 completion means a small mirror site can replace a collection of cron jobs.

## M3 - Operational hardening

Status: **accepted** at implementation baseline `8d0c032c37d6bb34c1e398e6d68e31c20ef28881`. See `docs/reviews/m3-review-round2-2026-09-01.md`.

Goal:

> make everyday administration and incident diagnosis comfortable.

Scope:

- complete CLI list/show/filter UX;
- `lmt run logs --follow`;
- structured error/failure categories;
- database backup command;
- log retention/rotation/compression policy;
- token enrollment/rotation/revocation UX;
- migration policy/testing;
- service hardening options;
- Grafana dashboard examples;
- Loki/journald integration documentation;
- read-only status API polish;
- configuration examples for representative mirror types.

M3 is accepted for a controlled LZU production trial. The active trial plan and architecture evidence checklist are documented in docs/production-trial.md. Do not begin M4 design solely from hypothetical requirements; collect trial evidence first.

## M4 - Production architecture and release hardening

Status: **design in progress** after the controlled LZU production trial. The publication architecture is frozen; implementation planning is next.

Authoritative design: `docs/m4-design.md`.

Frozen publication contract: `docs/m4-publication-design.md`.

Implementation sequence: `docs/m4-implementation-plan.md`.

Goal:

> close the evidence-backed production gaps before the stable-release push.

Scope:

- repository publication consistency;
- idempotent installation/upgrade automation;
- Agent long-poll shutdown responsiveness;
- bounded Tokio runtime policy on very large hosts;
- stable installation/upgrade guide;
- compatibility window between CLI/server/Agent versions;
- supported deployment layout;
- failure-mode runbook;
- contributor architecture guide;
- benchmark/capacity baseline;
- security review;
- reproducible release artifacts;
- packaging for major Linux distributions where practical;
- API cleanup before stable `v1`.

A small read-only status frontend may be built here or as a separate project. It is not a blocker for the core release.

## M5 - Optional extensions driven by real deployments

Only implement these if real requirements justify them:

- OCI/container runner;
- automatic placement;
- multiple storage roots/pools;
- repository-specific validation pipelines;
- richer resource scheduling;
- PostgreSQL/multi-controller mode;
- OIDC/RBAC;
- event/webhook integrations.

These are explicitly not promises for v1.

## Versioning suggestion

A reasonable mapping is:

```text
M1 -> v0.1.0-alpha
M2 -> v0.2.0 / first functional mirror-manager preview
M3 -> v0.3.x production trial
M4 -> v0.9.x release candidates
stable API review -> v1.0.0
```

The exact version numbers are less important than not declaring compatibility stability too early.

## Development order inside a milestone

Prefer vertical slices over finishing one technical layer globally.

For example, do not implement every database table before any HTTP flow works.

A healthy M1 progression is:

1. core types/config validation;
2. SQLite migration/open;
3. Node authentication/poll;
4. one immutable RunSpec;
5. process execution;
6. terminal event persistence;
7. manual Run API/CLI;
8. central logs;
9. crash/idempotency tests;
10. polish.

Every step should keep the architecture runnable and testable.

## Definition of done for a feature

A feature is not complete when only its happy-path code exists.

It should include:

- documented semantics;
- domain validation;
- persistence/migration changes;
- API/CLI behavior;
- failure behavior;
- idempotency behavior where applicable;
- tests;
- metrics/logging needed for operations;
- upgrade compatibility consideration.

This is especially important for scheduler and Agent execution changes.
