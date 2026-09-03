#!/bin/sh
set -eu

REPOSITORY=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT HUP INT TERM
BINARIES=$TEST_ROOT/bin
DESTINATION=$TEST_ROOT/root
mkdir -p "$BINARIES"
for binary in lmt lmt-server lmt-agent; do
    cp /bin/true "$BINARIES/$binary"
done
TOKEN_SOURCE=$TEST_ROOT/agent.token
printf 'test-credential\n' >"$TOKEN_SOURCE"
chmod 0600 "$TOKEN_SOURCE"

"$REPOSITORY/install.sh" all \
    --root "$DESTINATION" \
    --binary-dir "$BINARIES" \
    --bind 127.0.0.1:8080 \
    --node node-a \
    --server-url http://127.0.0.1:8080 \
    --mirror-root /srv/mirrors \
    --credential-file "$TOKEN_SOURCE" \
    --publication-root /srv/lmt-publication \
    --max-private-generations 4 \
    --reserve-bytes 1073741824

SERVER_CONFIG=$DESTINATION/etc/lmt/server.toml
AGENT_CONFIG=$DESTINATION/etc/lmt/agent.toml
OPERATOR_TOKEN=$DESTINATION/etc/lmt/operator.token
AGENT_TOKEN=$DESTINATION/etc/lmt/agent.token
test -x "$DESTINATION/usr/bin/lmt-server"
test -x "$DESTINATION/usr/bin/lmt-agent"
test -s "$OPERATOR_TOKEN"
test "$(stat -c '%a' "$AGENT_TOKEN")" = 640
grep -q 'publication_root = "/srv/lmt-publication"' "$AGENT_CONFIG"
grep -q 'bind = "127.0.0.1:8080"' "$SERVER_CONFIG"

BEFORE=$(sha256sum "$SERVER_CONFIG" "$AGENT_CONFIG" "$OPERATOR_TOKEN" "$AGENT_TOKEN")
"$REPOSITORY/install.sh" all \
    --root "$DESTINATION" \
    --binary-dir "$BINARIES" \
    --bind 127.0.0.1:9999 \
    --node different-node \
    --server-url http://invalid \
    --mirror-root /srv/different \
    --credential-file "$TOKEN_SOURCE" \
    --publication-root /srv/different-publication \
    --max-private-generations 9 \
    --reserve-bytes 9
AFTER=$(sha256sum "$SERVER_CONFIG" "$AGENT_CONFIG" "$OPERATOR_TOKEN" "$AGENT_TOKEN")
test "$BEFORE" = "$AFTER"

printf 'authoritative-data\n' >"$DESTINATION/var/lib/lmt/keep"
printf 'spool-data\n' >"$DESTINATION/var/lib/lmt-agent/spool/keep"
"$REPOSITORY/install.sh" upgrade --root "$DESTINATION" --binary-dir "$BINARIES"
test "$(cat "$DESTINATION/var/lib/lmt/keep")" = authoritative-data
test "$(cat "$DESTINATION/var/lib/lmt-agent/spool/keep")" = spool-data

UNSAFE_TOKEN=$TEST_ROOT/unsafe.token
printf 'unsafe\n' >"$UNSAFE_TOKEN"
chmod 0644 "$UNSAFE_TOKEN"
if "$REPOSITORY/install.sh" agent \
    --root "$TEST_ROOT/unsafe-root" \
    --binary-dir "$BINARIES" \
    --node node-a \
    --server-url http://127.0.0.1:8080 \
    --mirror-root /srv/mirrors \
    --credential-file "$UNSAFE_TOKEN"; then
    echo 'installer accepted an unsafe credential file' >&2
    exit 1
fi
