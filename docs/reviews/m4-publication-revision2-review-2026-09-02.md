# M4 Publication Design — Revision 2 Review

Status: received 2026-09-02.

Verdict: core architecture accepted in direction, but not yet ready to freeze.

The review accepted:

- RENAME_EXCHANGE;
- fresh-generation atomic rsync semantics;
- audited rsync profile;
- hard-link generation immutability;
- quiescent Move as the M4 ownership model;
- forward-only Server-first compatibility.

Required changes before freeze:

1. give visible_pending_durability an explicit operator recovery/abandon exit;
2. freeze durable ready_to_commit -> exchange -> fsync -> terminal ordering;
3. protect publication recovery evidence from reset-spool/restore/downgrade;
4. freeze GC protected paths and hard admission behavior;
5. make previous-generation reconstruction deterministic after crash;
6. make quiescent-check + Move owner update one Store transaction;
7. test compatibility against frozen historical M3 wire artifacts.

Revision 3 of docs/m4-publication-design.md addresses these boundaries. It
remains a freeze candidate until reviewed.
