#!/bin/sh
set -eu

REPOSITORY=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT HUP INT TERM
BINARIES=$TEST_ROOT/bin
OUTPUT=$TEST_ROOT/output
mkdir -p "$BINARIES"
for binary in lmt lmt-server lmt-agent; do
    cp /bin/true "$BINARIES/$binary"
done

ARCHIVE=$("$REPOSITORY/packaging/build-release.sh" \
    --binary-dir "$BINARIES" --output-dir "$OUTPUT" --version test-version)
NAME=lmt-test-version-$(uname -m)-unknown-linux-gnu
MANIFEST=$TEST_ROOT/manifest
tar -tzf "$ARCHIVE" >"$MANIFEST"

for path in \
    "$NAME/lmt" \
    "$NAME/lmt-server" \
    "$NAME/lmt-agent" \
    "$NAME/install.sh" \
    "$NAME/packaging/systemd/lmt-server.service" \
    "$NAME/packaging/systemd/lmt-agent.service" \
    "$NAME/config/server.example.toml" \
    "$NAME/config/agent.example.toml" \
    "$NAME/config/agent.atomic.example.toml" \
    "$NAME/docs/operations/atomic-publication.md" \
    "$NAME/docs/operations/install-upgrade.md" \
    "$NAME/crates/lmt-protocol/tests/fixtures/m3/poll-request.json"; do
    grep -Fxq "$path" "$MANIFEST"
done

FIRST=$(sha256sum "$ARCHIVE" | cut -d' ' -f1)
"$REPOSITORY/packaging/build-release.sh" \
    --binary-dir "$BINARIES" --output-dir "$OUTPUT" --version test-version >/dev/null
SECOND=$(sha256sum "$ARCHIVE" | cut -d' ' -f1)
test "$FIRST" = "$SECOND"
