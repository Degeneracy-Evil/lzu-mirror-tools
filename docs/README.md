# LMT Design Documentation

Recommended reading order:

1. [Design Summary](design-summary.md) — high-level product/architecture picture.
2. [Architecture](architecture.md) — system boundaries and deployment model.
3. [Core Model](core-model.md) — Mirror, Node, Run, TOML semantics.
4. [Scheduler](scheduler.md) — baseline cron/interval/manual/catch-up behavior.
5. [State Machines](state-machines.md) — Run and Attempt lifecycle.
6. [Agent Protocol](agent-protocol.md) — idempotency and failure recovery.
7. [Database](database.md) — central SQLite persistence model.
8. [HTTP API](api.md) — CLI/automation/Agent wire contract.
9. [Rust Implementation Design](implementation-design.md) — workspace and dependency boundaries.
10. [Testing Strategy](testing.md) — correctness and fault-injection plan.
11. [M1 Implementation Plan](m1-implementation-plan.md) — accepted first vertical slice.
12. [M2 Design](m2-design.md) — authoritative M2 scheduling/retry/cancellation/rsync semantics.
13. [M2 Implementation Plan](m2-implementation-plan.md) — exact M2 implementation sequence and release gates.
14. [Development Roadmap](roadmap.md) — implementation milestones.
15. [Code Review Guide](code-review.md) — invariants to enforce during review.
16. [Architecture Decisions](decisions.md) — accepted decisions and remaining questions.

During M2, `m2-design.md` takes precedence over older M1-era wording in narrower generic documents where a conflict exists.

The documentation is intentionally detailed. LMT is a long-lived infrastructure project, so behavior should be designed and testable before implementation complexity accumulates.

## Implementation reviews

- [M1 Code Review — 2026-08-30](reviews/m1-review-2026-08-30.md) — historical blocking review of the initial M1 implementation.
- [M1 Code Review Round 2 — 2026-08-30](reviews/m1-review-round2-2026-08-30.md) — acceptance review of hardening commit `e2c27dbdc573dc374c94902255265adc81b2ae10`; M1 accepted.

- [M2 Code Review — 2026-08-31](reviews/m2-review-2026-08-31.md) — blocking review of implementation commit `68d56837454f5902d97fe012508b32e282556df0`; M2 hardening required before acceptance.
