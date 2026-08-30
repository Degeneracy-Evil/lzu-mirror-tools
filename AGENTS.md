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
12. `docs/code-review.md`

`docs/decisions.md` records accepted architecture decisions.

## 2. Scope

Implement the current milestone only.

The initial development target is **M1**, defined in:

- `docs/roadmap.md`
- `docs/m1-implementation-plan.md`

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

- `lmt-core` must not depend on Axum, SQLx, Reqwest, or Tokio infrastructure.
- `lmt-protocol` defines wire contracts, not HTTP framework code.
- `lmt-store` is the only library crate that knows the central SQLite schema.
- `lmt-agent` never directly accesses the central database.
- repository-specific synchronization semantics must stay out of Agent execution code.
- CLI/API handlers must not duplicate domain state-machine logic.

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
- Agent local policy cannot be bypassed by Server requests.

## 5. Configuration

Human-authored configuration uses TOML.

Configuration files are authoritative by bundle.

Do not introduce hidden LMT-specific environment variables. Runtime values must be visible through explicit TOML placeholders.

Git is only version control; LMT itself has no Git integration.

## 6. Implementation style

Prefer:

- typed IDs/newtypes;
- enums for states/protocol variants;
- explicit domain transitions;
- direct argv process execution;
- bounded queues;
- small modules with clear ownership;
- deterministic/fake-clock tests for scheduler logic;
- transactional persistence.

Avoid:

- generic framework abstractions without a demonstrated need;
- raw stringly-typed state transitions scattered across handlers;
- shell interpolation by default;
- correctness that depends on in-memory HTTP sessions;
- unbounded channels;
- duplicating configuration models in the database.

## 7. Testing

A feature is not complete without its failure/idempotency tests.

M1 must cover at least:

- config apply;
- manual Run;
- Agent poll/dispatch;
- process success/failure;
- centralized logs;
- Server restart recovery;
- Agent crash/interruption recovery;
- duplicate StartAttempt behavior;
- duplicate event/log upload behavior.

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

Work in small vertical slices.

Keep the workspace buildable and tests runnable after each logical step.

Do not optimize or generalize ahead of the milestone requirements.

When uncertain between a broader abstraction and a smaller implementation, choose the smaller implementation and preserve the documented extension boundary.
