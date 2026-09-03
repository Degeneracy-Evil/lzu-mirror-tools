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
14. [M3 Design](m3-design.md) — authoritative production-operations and hardening contract.
15. [M3 Implementation Plan](m3-implementation-plan.md) — M3.0–M3.9 implementation order and gates.
16. [Development Roadmap](roadmap.md) — implementation milestones.
17. [Code Review Guide](code-review.md) — invariants to enforce during review.
18. [Architecture Decisions](decisions.md) — accepted decisions and remaining questions.
19. [M4 Design](m4-design.md) — current production-architecture/release-hardening milestone.
20. [M4 Publication Architecture](m4-publication-design.md) — frozen atomic-publication contract.
21. [M4 Implementation Plan](m4-implementation-plan.md) — ordered M4 development plan and release gates.

For the current milestone, `m4-design.md`, the frozen `m4-publication-design.md`, and `m4-implementation-plan.md` are authoritative for M4 behavior. Accepted M1/M2/M3 contracts remain regression baselines unless an explicit newer Architecture Decision changes them.

The documentation is intentionally detailed. LMT is a long-lived infrastructure project, so behavior should be designed and testable before implementation complexity accumulates.

## Implementation reviews

- [M1 Code Review — 2026-08-30](reviews/m1-review-2026-08-30.md) — historical blocking review of the initial M1 implementation.
- [M1 Code Review Round 2 — 2026-08-30](reviews/m1-review-round2-2026-08-30.md) — acceptance review of hardening commit `e2c27dbdc573dc374c94902255265adc81b2ae10`; M1 accepted.

- [M2 Code Review — 2026-08-31](reviews/m2-review-2026-08-31.md) — blocking review of implementation commit `68d56837454f5902d97fe012508b32e282556df0`; M2 hardening required before acceptance.

- [M2 Code Review Round 2 — 2026-08-31](reviews/m2-review-round2-2026-08-31.md) — acceptance review of hardening commit `7ad3886c1c11d011ae6fd76df2fa6ecc1b87bdaf`; M2 accepted.


## M3 production-trial operations

- [Production Layout](operations/production-layout.md)
- [Credentials](operations/credentials.md)
- [Backup and Restore](operations/backup-restore.md)
- [Run Log Retention](operations/log-retention.md)
- [Observability](operations/observability.md)
- [Incident Diagnosis](operations/incident-diagnosis.md)

## M4 release operations

- [Atomic Publication](operations/atomic-publication.md) - Direct/Atomic
  guarantees, storage requirements, rsync profile, serving caveats, and local
  recovery/fence workflow.
- [Installation and Upgrade](operations/install-upgrade.md) - release archive,
  idempotent local installer, Server-first rollout, and restore-based downgrade.

- [M3 Code Review — 2026-09-01](reviews/m3-review-2026-09-01.md) — blocking review of implementation candidate `73d90897733a1a2e98aa655c3dda0f562ed33d33`; focused hardening required before production-trial acceptance.

- [M3 Code Review Round 2 — 2026-09-01](reviews/m3-review-round2-2026-09-01.md) — acceptance review of hardening commit `8d0c032c37d6bb34c1e398e6d68e31c20ef28881`; M3 accepted for controlled production trial.

## Controlled production trial

- [Production Trial Record](production-trial.md) — completed host/fault evidence that motivated M4 publication design.

- [M4 Design](m4-design.md) — post-trial publication, deployment, and runtime-hardening design.
- [M4 Publication Architecture](m4-publication-design.md) — frozen atomic publication contract.
- [M4 Implementation Plan](m4-implementation-plan.md) — implementation order, compatibility profile, recovery/GC gates, installer, and runtime polish.
