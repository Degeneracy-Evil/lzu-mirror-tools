#!/bin/sh
set -eu

REPOSITORY=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUTPUT_DIR=$REPOSITORY/dist
BINARY_DIR=
VERSION=

usage() {
    cat <<'EOF'
usage: packaging/build-release.sh [--output-dir DIR] [--binary-dir DIR] [--version VERSION]

Without --binary-dir, release binaries are built with Cargo from the locked workspace.
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-dir) [ "$#" -ge 2 ] || exit 2; OUTPUT_DIR=$2; shift 2 ;;
        --binary-dir) [ "$#" -ge 2 ] || exit 2; BINARY_DIR=$2; shift 2 ;;
        --version) [ "$#" -ge 2 ] || exit 2; VERSION=$2; shift 2 ;;
        --help) usage; exit 0 ;;
        *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

if [ -z "$VERSION" ]; then
    VERSION=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' "$REPOSITORY/Cargo.toml")
fi
case "$VERSION" in ''|*[!A-Za-z0-9._-]*) echo 'release version is empty or unsafe' >&2; exit 2 ;; esac

if [ -z "$BINARY_DIR" ]; then
    cargo build --manifest-path "$REPOSITORY/Cargo.toml" --workspace --release --locked
    BINARY_DIR=$REPOSITORY/target/release
fi
for binary in lmt lmt-server lmt-agent; do
    [ -x "$BINARY_DIR/$binary" ] || { printf 'missing executable %s/%s\n' "$BINARY_DIR" "$binary" >&2; exit 1; }
done

ARCH=$(uname -m)
NAME=lmt-$VERSION-$ARCH-unknown-linux-gnu
STAGING=$(mktemp -d)
trap 'rm -rf "$STAGING"' EXIT HUP INT TERM
ROOT=$STAGING/$NAME
mkdir -p "$ROOT/packaging/systemd" "$ROOT/config" "$ROOT/docs/operations" \
    "$ROOT/crates/lmt-protocol/tests/fixtures/m3"

install -m 0755 "$BINARY_DIR/lmt" "$BINARY_DIR/lmt-server" "$BINARY_DIR/lmt-agent" "$ROOT/"
install -m 0755 "$REPOSITORY/install.sh" "$ROOT/install.sh"
install -m 0644 "$REPOSITORY/LICENSE" "$REPOSITORY/README.md" "$ROOT/"
install -m 0644 "$REPOSITORY/packaging/systemd/"*.service "$ROOT/packaging/systemd/"
cp -R "$REPOSITORY/config/." "$ROOT/config/"
install -m 0644 \
    "$REPOSITORY/docs/design-summary.md" \
    "$REPOSITORY/docs/m4-design.md" \
    "$REPOSITORY/docs/m4-publication-design.md" \
    "$REPOSITORY/docs/m4-implementation-plan.md" \
    "$ROOT/docs/"
install -m 0644 "$REPOSITORY/docs/operations/"*.md "$ROOT/docs/operations/"
install -m 0644 "$REPOSITORY/crates/lmt-protocol/tests/fixtures/m3/"*.json \
    "$ROOT/crates/lmt-protocol/tests/fixtures/m3/"

mkdir -p "$OUTPUT_DIR"
ARCHIVE=$OUTPUT_DIR/$NAME.tar.gz
tar --sort=name --mtime='@0' --owner=0 --group=0 --numeric-owner -C "$STAGING" -cf - "$NAME" \
    | gzip -n >"$ARCHIVE"
printf '%s\n' "$ARCHIVE"
