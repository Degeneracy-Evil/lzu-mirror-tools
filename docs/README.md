# LMT Design Documentation

Recommended reading order:

1. [Design Summary](design-summary.md) — frozen high-level picture.
2. [Architecture](architecture.md) — system boundaries and deployment model.
3. [Core Model](core-model.md) — Mirror, Node, Run, TOML semantics.
4. [Scheduler](scheduler.md) — cron/interval/manual/catch-up behavior.
5. [State Machines](state-machines.md) — Run and Attempt lifecycle.
6. [Agent Protocol](agent-protocol.md) — idempotency and failure recovery.
7. [Database](database.md) — central SQLite persistence model.
8. [HTTP API](api.md) — CLI/automation/Agent wire contract.
9. [Rust Implementation Design](implementation-design.md) — workspace and dependency boundaries.
10. [Testing Strategy](testing.md) — correctness and fault-injection plan.
11. [M1 Implementation Plan](m1-implementation-plan.md) — exact first development slice and acceptance scenarios.
12. [Development Roadmap](roadmap.md) — implementation milestones.
13. [Code Review Guide](code-review.md) — invariants to enforce during review.
14. [Architecture Decisions](decisions.md) — accepted decisions and remaining questions.

The documentation is intentionally more detailed than the initial codebase. LMT is a long-lived infrastructure project, so behavior should be designed and testable before implementation complexity accumulates.


## Implementation reviews

- [M1 Code Review — 2026-08-30](reviews/m1-review-2026-08-30.md) — review of implementation commit `409ea99502dcc56f0abdbec110cd9529629a4498`, including release blockers and required hardening before M2.
