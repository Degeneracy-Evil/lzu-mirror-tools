# M4 Publication Design — Adversarial Architecture Review

Status: received 2026-09-02.

The full review was provided externally and concluded **REQUEST CHANGES**.

The release blockers were:

1. separate namespace visibility, daemon/process crash recovery, and power-loss
   durability;
2. recognize that fresh candidate + link-dest changes rsync destination
   semantics and define an audited atomic profile;
3. distinguish forward-only upgrade ordering from bidirectional mixed-version
   compatibility.

Major concerns were:

- inode identity checking is not compare-and-swap;
- hard-link generation immutability is a correctness invariant;
- Move should be quiescent-only.

Revision 2 of `docs/m4-publication-design.md` addresses these points. The design
remains proposed and is not yet implementation-authorized.
