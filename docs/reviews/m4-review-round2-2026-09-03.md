# M4 Code Review Round 2 — 2026-09-03

Status: **ACCEPTED**.

Accepted implementation baseline:

~~~text
7eaeaff92f17e3184543bdf32e50d99881f7d70d
~~~

GitHub Actions run `33742691488` completed successfully for this exact SHA.

This round re-reviewed only the blockers from
`m4-review-2026-09-03.md` and compatibility regressions they could introduce.

## Closed blockers

### Full-writer fence

Accepted.

M4 introduces capability-gated `execution_identity_v1`. For Agents advertising
that capability, StartAttempt carries an explicit Mirror identity outside
`ProcessRunSpec`. Agent admission persists and validates that identity and uses
it to enforce abandoned publication fences.

The review confirmed:

- Direct work for the fenced Mirror remains blocked after target changes;
- moving a Mirror away and later back to the old Node does not bypass the fence;
- unrelated Mirrors remain admissible;
- Atomic publication identity must agree with execution identity;
- conflicting duplicate StartAttempt identity is rejected without replacing
  durable ownership;
- legacy spool remains readable.

M3 Agents do not advertise the capability. The M4 Server therefore omits the new
field, and the serialized StartAttempt remains byte-for-byte equal to the frozen
M3 PollResponse fixture.

### Installer permissions

Accepted.

The installer now restores the production-trial T002 invariant:

~~~text
/etc/lmt/               root:root      0755
server.toml             root:lmt       0640
operator.token          root:lmt       0640
agent.toml              root:lmt-agent 0640
agent.token             root:lmt-agent 0640
~~~

Server, Agent, combined installation and upgrade paths converge to this layout.
The installer gate covers repeated install, repair after permission drift,
service-user readability and cross-secret non-readability where the environment
permits the real users.

### Publication abandon reconciliation

Accepted.

The maintenance operation first persists exact `abandoned_fenced` evidence,
then immediately attempts terminal reconciliation with the Server. It can
reconstruct the required Accepted event before terminal Failed when needed.

If the Server is unavailable or rejects the reconciliation, the command returns
a clear pending result while retaining the durable writer fence. Successful
terminal reporting also leaves the fence in place; only explicit fence-clear may
retire it.

This preserves the frozen ordering:

~~~text
durable local fence
-> Server terminalization attempt
-> explicit fence-clear later
~~~

## Regression checks

The review also reconfirmed:

- frozen M3 PollRequest/PollResponse/Direct ProcessRunSpec fixtures remain stable;
- the new identity extension is capability-gated rather than version-string
  inferred;
- no application-level redesign of the frozen M4 publication state machine was
  introduced by the fixes.

CI run `33742691488` passed:

- `cargo fmt --all -- --check`;
- strict all-target/all-feature Clippy;
- all-feature locked Rust tests;
- installer gate;
- deterministic release archive gate;
- clean-worktree checks.

The implementation report additionally records the complete local M1/M2
fault/E2E matrix and M4 publication/recovery tests as passing.

## Remaining non-blocking coverage debt

Real publication smoke has been exercised on XFS.

Real ext4 and Btrfs smoke remains environment coverage debt. This does not block
M4 acceptance because support is capability-probed through real rename/fsync
operations rather than inferred from filesystem names.

The intentionally excluded guarantees remain excluded: upstream point-in-time
snapshot consistency, recursive whole-tree power-loss durability, and serving
cache/open-file invalidation.

## Verdict

> **M4 is accepted.**

The frozen M4 publication architecture and implementation baseline
`7eaeaff92f17e3184543bdf32e50d99881f7d70d` are ready to become the next
production baseline.

Do not reopen M4 architecture without new production evidence. Further feature
work belongs to a separately designed milestone.
