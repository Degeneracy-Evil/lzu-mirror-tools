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
14. `docs/m3-design.md`
15. `docs/m3-implementation-plan.md`
16. `docs/code-review.md`
17. `docs/decisions.md`
18. `docs/m4-design.md`
19. `docs/m4-publication-design.md`
20. `docs/m4-implementation-plan.md`
21. `docs/v1-rollout-plan.md`

M1/M2/M3 are accepted baselines. For M4 publication semantics,
**`docs/m4-publication-design.md` is authoritative**. For implementation order
and remaining M4 choices, follow **`docs/m4-implementation-plan.md`**. If an
older generic document conflicts with an accepted newer decision, do not guess:
preserve the newer accepted contract and update the narrower stale document.

## 2. Scope

Implement the current milestone only.

M1, M2, and M3 are accepted implementation baselines.

**M4 is accepted. There is currently no authorized M5 implementation target.**
The active work is production rollout and v1 stabilization under
`docs/v1-rollout-plan.md`. M4 publication semantics remain frozen in
`docs/m4-publication-design.md`.

Post-M4 stabilization work must:

- preserve the accepted M4 baseline and all M1-M4 regression gates;
- limit code changes to production blockers, release/version/API compatibility,
  security hardening justified by review, packaging, or documentation;
- not weaken publication write-ahead, fence, GC protected-set, transactional
  Move, or M3->M4 rolling-upgrade invariants;
- avoid new product resources/features until a later accepted M5 design exists.

Do not implement deferred M5 features such as:

- generic plugin SDK;
- automatic placement or cross-node failover;
- multiple storage pools/orchestration;
- filesystem-specific snapshot backends;
- PostgreSQL/controller HA;
- OIDC/RBAC;
- container runner;
- workflow/DAG system;
- generic verification pipelines.

unless a later accepted design explicitly authorizes them.

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

The accepted M3 baseline must preserve the complete M1/M2 fault matrices and the release gates in `docs/m3-design.md`, especially:

- frozen historical schema migration through the current M3 schema;
- Server/Agent single-instance locks and durable Agent binding;
- credential issue/rotation/revocation and reload;
- bounded Run queries and CLI output/exit semantics;
- Run-log follow, retention, expiration, and lock lifetime;
- online backup and offline restore normalization;
- bounded metrics, sanitized public status, and doctor diagnostics;
- production systemd/permission assumptions.

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

M1-M4 are complete and accepted. The active phase is `docs/v1-rollout-plan.md`. Do not start M5 or expand M4 opportunistically; production evidence must precede any new milestone design.

Work in small vertical slices and logical commits.

Keep the workspace buildable and tests runnable after each logical step.

Do not optimize or generalize ahead of the milestone requirements.

When uncertain between a broader abstraction and a smaller implementation, choose the smaller implementation and preserve the documented extension boundary.
