# Release Freeze and v0.9.0-rc.1 Cut

Status: **authorized execution contract for the post-M4 release freeze**.

This document is the handoff for the local implementation/release agent. It is
not a new milestone design and does not authorize M5 work.

Accepted M4 implementation baseline:

~~~text
7eaeaff92f17e3184543bdf32e50d99881f7d70d
~~~

The release candidate must remain a descendant of that baseline and preserve all
accepted M1-M4 contracts. Documentation commits after the accepted baseline are
allowed. Functional changes are not part of the RC cut.

## 1. What is being frozen

M4 architecture and feature development are complete. The next public version is
`v0.9.0-rc.1`, not `v1.0.0`.

The freeze means:

- no M5 feature implementation;
- no opportunistic M4 redesign;
- no new resources, workflow engines, placement/failover, storage-pool,
  PostgreSQL/HA, OIDC/RBAC, container-runner, or generic verification work;
- no refactor or dependency update merely because the release is being cut;
- accepted publication, fencing, GC/admission, Move, and M3-to-M4 compatibility
  semantics remain unchanged.

After the RC is cut, production rollout and stabilization continue under
`docs/v1-rollout-plan.md`.

## 2. Changes authorized for the RC cut

The local agent may make only release-mechanical changes required to identify the
accepted code as `v0.9.0-rc.1`:

1. change the workspace package version in `Cargo.toml` from the historical
   development value to `0.9.0-rc.1`;
2. update only the corresponding workspace-package version metadata in
   `Cargo.lock` if Cargo requires it;
3. make a narrowly necessary documentation correction if the old version string
   would otherwise make the release instructions false.

The version metadata change is release bookkeeping, not authorization for source
changes.

Do **not** modify Rust source, `install.sh`, packaging scripts, CI workflows,
systemd units, configuration semantics, protocol fixtures, migrations, or tests
as part of the freeze. If any such change appears necessary, stop the RC cut and
report the blocker for review instead of fixing it opportunistically.

Do not update dependency versions while refreshing `Cargo.lock`.

## 3. Pre-freeze gate

Start from a clean, current `main`:

~~~text
git fetch origin
git checkout main
git pull --ff-only origin main
git status --short
git rev-parse HEAD
git rev-parse origin/main
git merge-base --is-ancestor 7eaeaff92f17e3184543bdf32e50d99881f7d70d HEAD
git diff --name-status 7eaeaff92f17e3184543bdf32e50d99881f7d70d..HEAD
~~~

Record the starting SHA. Review every path changed since the accepted M4 code
baseline. Any post-acceptance functional implementation change that has not been
separately reviewed is a stop condition.

Run the same release gates as CI before changing the version:

~~~text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
sh packaging/tests/install.sh
sh packaging/tests/release.sh
git diff --exit-code
git diff --cached --exit-code
~~~

All must pass from a clean tree.

## 4. Cut the release commit

Change only the authorized release metadata, then inspect the complete diff.
The lockfile refresh, if needed, must not change dependency resolution.

Run the complete gate from section 3 again. Then create one release commit, for
example:

~~~text
git commit -am "release: cut v0.9.0-rc.1"
~~~

If untracked documentation was intentionally required, stage it explicitly
rather than relying on `-a`.

Push `main`, record the resulting release SHA, and require the GitHub Actions CI
run for that **exact SHA** to complete successfully before tagging it.

Do not amend or add another commit after CI and then reuse the old CI result.

## 5. Build and record the release artifact

From the exact clean release commit on Linux:

~~~text
rm -rf dist
packaging/build-release.sh --output-dir dist
sha256sum dist/lmt-0.9.0-rc.1-*.tar.gz
~~~

Record together:

- release commit SHA;
- artifact filename;
- SHA-256;
- architecture/build host information sufficient to identify the artifact;
- successful CI run URL/ID for the exact release SHA.

`packaging/build-release.sh` makes the archive layout deterministic for a fixed
set of inputs by normalizing tar ordering/metadata and gzip metadata. This freeze
does **not** claim that independently compiled Rust binaries are bit-for-bit
reproducible across arbitrary build hosts unless that is separately verified.
The SHA-256 identifies the artifact actually published.

## 6. Tagging and publication

Before creating the tag, verify that `v0.9.0-rc.1` does not already exist. If it
exists and does not point to the intended release SHA, stop; do not move or
force-replace a published release tag.

After the exact release SHA has passed CI, create and push an annotated tag:

~~~text
git tag -a v0.9.0-rc.1 <release-sha> -m "LMT v0.9.0-rc.1"
git push origin v0.9.0-rc.1
~~~

If GitHub Releases is used as the distribution channel, create a **pre-release**
for that tag and attach the tarball plus a checksum file. The release notes must
state that this is the M4-accepted production candidate, that rollout is
Mirror-by-Mirror, and that `v1.0.0` compatibility is not yet promised.

Do not create a `v1.0.0` tag in this step.

## 7. Abort conditions

Stop the freeze and report instead of silently repairing the tree if any of the
following occurs:

- the worktree is not clean for reasons unrelated to the release cut;
- current `main` is not a descendant of the accepted M4 baseline;
- an unreviewed functional change exists after the accepted M4 baseline;
- any CI/release gate fails;
- cutting the version appears to require Rust/source, installer, packaging,
  protocol, database, state-machine, or CI behavior changes;
- dependency resolution changes unexpectedly;
- the release artifact cannot be built from the exact release SHA;
- the intended tag already exists at another commit;
- the exact release commit does not receive a successful CI result.

A blocker found here is evidence for a stabilization fix. It is not permission to
expand the freeze task.

## 8. Definition of freeze complete

The freeze is complete only when all of the following are true:

1. `main` contains the release-only version commit;
2. the exact release SHA passes the complete CI gate;
3. the worktree and index are clean;
4. local `main` and `origin/main` resolve to the same release SHA;
5. `v0.9.0-rc.1` points exactly to that SHA;
6. the release archive was built from that SHA and its SHA-256 recorded;
7. no feature or architecture change was introduced during the cut.

GitHub pre-release publication may immediately follow tagging; if credentials or
repository policy prevent it, report that separately rather than altering the
release commit.

## 9. Required final report from the local agent

Return a compact evidence report containing:

~~~text
starting main SHA:
release commit SHA:
origin/main SHA:
tag + target SHA:
version:
files changed by the RC cut:
CI run ID/URL + conclusion:
artifact filename:
artifact SHA-256:
GitHub pre-release URL/status:
worktree status:
~~~

Also state explicitly whether any source, protocol, database, installer,
packaging, workflow, dependency, or feature behavior changed. The expected
answer for `v0.9.0-rc.1` is **no**.

## 10. Policy after the freeze

`v0.9.0-rc.1` begins production rollout; it does not end stabilization.

After the tag:

- documentation and production evidence may continue to accumulate;
- a concrete correctness, security, compatibility, installation, or release
  blocker may justify a narrowly reviewed stabilization fix;
- any functional fix after `rc.1` requires a new RC (`rc.2`, `rc.3`, ...); never
  retag or replace an existing RC;
- speculative feature work remains prohibited;
- M5 starts only after real deployment demonstrates a concrete missing
  capability and a new design is separately accepted;
- `v1.0.0` is cut only after the stable API/version, threat model, platform
  support matrix, and real production rollout review in `docs/v1-rollout-plan.md`.
