# M4 Publication Final Amendments — Freeze Record

Status: **accepted** on 2026-09-02.

The final review required three last correctness conditions before freeze:

1. durable `preparing_exchange` before the first mutation of `exchange/` or
   `gc/`;
2. abandon/fence bound to exact Mirror + Run + Attempt + spec identity, using the
   same publication lock, persisting the local fence before Server
   terminalization, and blocking every LMT writer for the Mirror on the old
   Node;
3. GC scan/delete/admission serialized with publication/recovery/fence-clear
   under the per-Mirror publication lock, with every `preparing_exchange` path
   in the protected set.

All three are incorporated into the frozen
`docs/m4-publication-design.md` and accepted decisions D070-D072.

Verdict: **ACCEPT / FREEZE PUBLICATION ARCHITECTURE**.
