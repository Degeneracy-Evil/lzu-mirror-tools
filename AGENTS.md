# AGENTS.md

These instructions apply to the entire repository.

## 1. Read the design before coding

Before making implementation changes, read at minimum:

1. `docs/design-summary.md`
2. `docs/architecture.md`
3. `docs/core-model.md`
4. `docs/scheduler.md`
5. `docs/state-machines.md`
6. `docs/agent-protocol.md`
7. `docs/database.md`
8. `docs/api.md`
9. `docs/implementation-design.md`
10. `docs/testing.md`
11. `docs/m1-implementation-plan.md`
12. `docs/m2-design.md`
13. `docs/m2-implementation-plan.md`
14. `docs/code-review.md`
15. `docs/decisions.md`

M1 is an accepted baseline. For M2-specific semantics, **`docs/m2-design.md` is authoritative** if an older generic document still contains M1-era wording that conflicts with it. Do not resolve such conflicts by guessing; preserve M2 Design behavior and update the narrower document when appropriate.

## 2. Scope

Implement the current milestone only.

M1 and M2 are accepted implementation baselines.

**There is currently no authorized M3 implementation target.** M3 must be designed and documented before code work begins.

Until M3 design is frozen:

- preserve all M1/M2 behavior and release-gating tests;
- do not introduce M3 features opportunistically;
- documentation/review work may prepare the M3 design;
- any maintenance fix must remain compatible with the accepted M2 contracts.

Do not implement deferred features such as:

- generic plugin SDK;
- automatic placement;
- cross-node failover;
- storage orchestration;
- snapshot/publication engine;
- PostgreSQL/controller HA;
- OIDC/RBAC;
- container runner;
- workflow/DAG system.

unless the design documents are explicitly changed first.

## 3. Architecture boundaries

Preserve these dependency rules:

- `lmt-core` must not depend on Axum, SQLite, Reqwest, or Tokio infrastructure.
- `lmt-protocol` defines wire contracts, not HTTP framework code.
- `lmt-store` is the only library crate that knows the central SQLite schema.
- `lmt-agent` never directly accesses the central database.
- repository-specific synchronization semantics must stay out of Agent execution code.
- CLI/API handlers must not duplicate domain state-machine logic.
- M2 must not introduce an in-memory correctness-critical job queue.

## 4. Correctness rules

Treat the following as hard invariants:

- the control plane is never in the mirror download path;
- one Mirror has at most one non-terminal Run;
- a Run is tied to one immutable configuration generation;
- command delivery may repeat, but execution is idempotent by `(run_id, attempt_no)`;
- Agent disappearance never authorizes cross-node duplicate execution;
- config pruning never deletes mirror data;
- all authoritative/queryable state is centralized on `lmt-server`;
- Run stdout/stderr is centralized but not stored as SQLite BLOBs;
- Agent local policy cannot be bypassed by Server requests;
- scheduler/retry correctness survives Server restart from SQLite alone;
- retries remain Attempts inside the same Run;
- cancellation is safe under duplicate delivery and Cancel-before-Start reordering.

## 5. Configuration

Human-authored configuration uses TOML.

Configuration files are authoritative by bundle.

Do not introduce hidden LMT-specific environment variables. Runtime values must be visible through explicit TOML placeholders.

Git is only version control; LMT itself has no Git integration.

M2 config apply must not implicitly execute a new/changed/moved scheduled Mirror.

## 6. Implementation style

Prefer:

- typed IDs/newtypes;
- enums for states/protocol variants;
- explicit domain transitions;
- direct argv process execution;
- bounded queues;
- small modules with clear ownership;
- deterministic/manual-clock scheduler tests;
- transactional persistence;
- one dedicated SQLite background-thread execution boundary.

Avoid:

- generic framework abstractions without a demonstrated need;
- raw stringly-typed state transitions scattered across handlers;
- shell interpolation by default;
- correctness that depends on in-memory HTTP sessions;
- unbounded channels;
- duplicating configuration models in the database;
- real minute/hour sleeps in scheduler tests.

## 7. Testing

A feature is not complete without its failure/idempotency tests.

M2 must preserve the entire M1 fault matrix and additionally cover:

- deterministic interval scheduling;
- timezone-aware cron and DST semantics;
- coalesced scheduled due intent;
- scheduler recovery after Server restart;
- multi-Attempt retry deadlines;
- retry suppression after disable/remove/move;
- explicit cancellation;
- Cancel-before-delayed-Start tombstones;
- Agent capacity;
- M1-to-M2 schema migration;
- local built-in rsync integration without internet.

Use local helper processes/directories. Normal CI must not require public mirror servers or internet access.

## 8. Documentation changes

If code changes any of these, update the corresponding document in the same change:

- architecture invariants;
- configuration semantics;
- scheduler behavior;
- Run/Attempt state transitions;
- database meaning/transactions;
- Agent protocol;
- HTTP API contract;
- dependency boundaries.

Do not allow design documents and implementation to intentionally drift.

## 9. Development workflow

Follow the M2 implementation order rather than implementing all scheduler features at once.

Work in small vertical slices and logical commits.

Keep the workspace buildable and tests runnable after each logical step.

Do not optimize or generalize ahead of the milestone requirements.

When uncertain between a broader abstraction and a smaller implementation, choose the smaller implementation and preserve the documented extension boundary.
