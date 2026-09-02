# M4 Publication Design — Final Freeze Review

Status: accepted 2026-09-02.

The final reviewer accepted the architecture subject to three last correctness
requirements:

1. durable `preparing_exchange` must precede the first mutation of exchange/gc;
2. abandon/fence must bind exact Run/Attempt/spec identity, serialize through the
   publication lock, persist the fence before Server terminalization, and block
   all LMT writers for that Mirror on the old Node;
3. GC scan/delete/admission must share the per-Mirror publication lock with
   publication/recovery/fence-clear, and `preparing_exchange` paths must be in
   the protected set.

Those requirements are incorporated in the frozen publication design and
D070-D072.

Verdict: **ACCEPT / FREEZE PUBLICATION ARCHITECTURE**.
