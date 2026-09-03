#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
usage:
  sudo ./install.sh server --bind ADDRESS [--binary-dir DIR] [--no-start]
  sudo ./install.sh agent --node NAME --server-url URL --mirror-root DIR
       (--credential-file FILE | --credential-stdin)
       [--publication-root DIR --max-private-generations N --reserve-bytes N]
       [--binary-dir DIR] [--no-start]
  sudo ./install.sh all <server and agent options>
  sudo ./install.sh upgrade [--binary-dir DIR] [--no-start]

--root DIR stages files below DIR without users, preflight, or systemd actions.
EOF
}

[ "$#" -ge 1 ] || { usage >&2; exit 2; }
ROLE=$1
shift
case "$ROLE" in server|agent|all|upgrade) ;; *) usage >&2; exit 2 ;; esac

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BINARY_DIR=$SCRIPT_DIR
INSTALL_ROOT=
START_SERVICES=1
BIND=
NODE=
SERVER_URL=
MIRROR_ROOT=
CREDENTIAL_FILE=
CREDENTIAL_STDIN=0
PUBLICATION_ROOT=
MAX_PRIVATE_GENERATIONS=
RESERVE_BYTES=

while [ "$#" -gt 0 ]; do
    case "$1" in
        --binary-dir) [ "$#" -ge 2 ] || exit 2; BINARY_DIR=$2; shift 2 ;;
        --root) [ "$#" -ge 2 ] || exit 2; INSTALL_ROOT=$2; shift 2 ;;
        --no-start) START_SERVICES=0; shift ;;
        --bind) [ "$#" -ge 2 ] || exit 2; BIND=$2; shift 2 ;;
        --node) [ "$#" -ge 2 ] || exit 2; NODE=$2; shift 2 ;;
        --server-url) [ "$#" -ge 2 ] || exit 2; SERVER_URL=$2; shift 2 ;;
        --mirror-root) [ "$#" -ge 2 ] || exit 2; MIRROR_ROOT=$2; shift 2 ;;
        --credential-file) [ "$#" -ge 2 ] || exit 2; CREDENTIAL_FILE=$2; shift 2 ;;
        --credential-stdin) CREDENTIAL_STDIN=1; shift ;;
        --publication-root) [ "$#" -ge 2 ] || exit 2; PUBLICATION_ROOT=$2; shift 2 ;;
        --max-private-generations) [ "$#" -ge 2 ] || exit 2; MAX_PRIVATE_GENERATIONS=$2; shift 2 ;;
        --reserve-bytes) [ "$#" -ge 2 ] || exit 2; RESERVE_BYTES=$2; shift 2 ;;
        *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done

if [ -n "$INSTALL_ROOT" ]; then
    case "$INSTALL_ROOT" in /*) ;; *) echo '--root must be absolute' >&2; exit 2 ;; esac
    [ "$INSTALL_ROOT" != / ] || { echo '--root / is not a staging root' >&2; exit 2; }
elif [ "$(id -u)" -ne 0 ]; then
    echo 'installation requires root (or explicit --root staging)' >&2
    exit 1
fi

target() { printf '%s%s\n' "$INSTALL_ROOT" "$1"; }
require_value() { [ -n "$2" ] || { printf '%s is required for %s\n' "$1" "$ROLE" >&2; exit 2; }; }
require_binary() { [ -x "$BINARY_DIR/$1" ] || { printf 'missing executable %s/%s\n' "$BINARY_DIR" "$1" >&2; exit 1; }; }
safe_toml_string() {
    if printf '%s' "$2" | LC_ALL=C grep -q '[[:cntrl:]"\\]'; then
        printf '%s contains characters unsafe for TOML generation\n' "$1" >&2
        exit 2
    fi
}
unsigned_number() {
    case "$2" in ''|*[!0-9]*) printf '%s must be an unsigned integer\n' "$1" >&2; exit 2 ;; esac
}

install_accounts() {
    [ -n "$INSTALL_ROOT" ] && return
    getent group lmt >/dev/null 2>&1 || groupadd --system lmt
    id lmt >/dev/null 2>&1 || useradd --system --gid lmt --home-dir /var/lib/lmt --shell /usr/sbin/nologin lmt
    getent group lmt-agent >/dev/null 2>&1 || groupadd --system lmt-agent
    id lmt-agent >/dev/null 2>&1 || useradd --system --gid lmt-agent --home-dir /var/lib/lmt-agent --shell /usr/sbin/nologin lmt-agent
}

install_units() {
    install -d -m 0755 "$(target /etc/systemd/system)"
    install -m 0644 "$SCRIPT_DIR/packaging/systemd/lmt-server.service" "$(target /etc/systemd/system/lmt-server.service)"
    install -m 0644 "$SCRIPT_DIR/packaging/systemd/lmt-agent.service" "$(target /etc/systemd/system/lmt-agent.service)"
}

install_shared_config_dir() {
    install -d -m 0755 "$(target /etc/lmt)"
    if [ -z "$INSTALL_ROOT" ]; then
        chown root:root /etc/lmt
        chmod 0755 /etc/lmt
    fi
}

enforce_config_permissions() {
    for NAME in server.toml operator.token; do
        FILE=$(target "/etc/lmt/$NAME")
        [ ! -e "$FILE" ] || chmod 0640 "$FILE"
        [ -n "$INSTALL_ROOT" ] || [ ! -e "$FILE" ] || chown root:lmt "$FILE"
    done
    for NAME in agent.toml agent.token; do
        FILE=$(target "/etc/lmt/$NAME")
        [ ! -e "$FILE" ] || chmod 0640 "$FILE"
        [ -n "$INSTALL_ROOT" ] || [ ! -e "$FILE" ] || chown root:lmt-agent "$FILE"
    done
}

install_server() {
    require_binary lmt-server
    require_binary lmt
    [ -z "$BIND" ] || safe_toml_string --bind "$BIND"
    install -d -m 0755 "$(target /usr/bin)"
    install -m 0755 "$BINARY_DIR/lmt-server" "$(target /usr/bin/lmt-server)"
    install -m 0755 "$BINARY_DIR/lmt" "$(target /usr/bin/lmt)"
    install -d -m 0750 "$(target /var/lib/lmt)" \
        "$(target /var/lib/lmt/logs)" "$(target /var/lib/lmt/backups)"
    TOKEN=$(target /etc/lmt/operator.token)
    if [ ! -e "$TOKEN" ]; then
        umask 077
        od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$TOKEN"
        printf '\n' >>"$TOKEN"
        chmod 0640 "$TOKEN"
    fi
    CONFIG=$(target /etc/lmt/server.toml)
    if [ ! -e "$CONFIG" ]; then
        require_value --bind "$BIND"
        umask 027
        cat >"$CONFIG" <<EOF
bind = "$BIND"
database_path = "/var/lib/lmt/lmt.db"
log_dir = "/var/lib/lmt/logs"
operator_token_file = "/etc/lmt/operator.token"
offline_after_seconds = 90

[logging]
level = "info"
format = "json"

[status]
public = false

[backup]
directory = "/var/lib/lmt/backups"

[run_logs]
retention = "30d"
max_total_bytes = 10737418240
maintenance_interval = "1h"
EOF
        chmod 0640 "$CONFIG"
    fi
    if [ -z "$INSTALL_ROOT" ]; then
        chown -R lmt:lmt /var/lib/lmt
        chown root:lmt /etc/lmt/server.toml /etc/lmt/operator.token
    fi
}

install_agent_secret() {
    DESTINATION=$(target /etc/lmt/agent.token)
    [ -e "$DESTINATION" ] && return
    [ "$CREDENTIAL_STDIN" -eq 0 ] || [ -z "$CREDENTIAL_FILE" ] || {
        echo 'choose only one of --credential-file and --credential-stdin' >&2; exit 2;
    }
    umask 077
    TEMPORARY=$(mktemp "$(target /etc/lmt)/.agent-token.XXXXXX")
    trap 'rm -f "$TEMPORARY"' EXIT HUP INT TERM
    if [ "$CREDENTIAL_STDIN" -eq 1 ]; then
        cat >"$TEMPORARY"
    else
        require_value --credential-file "$CREDENTIAL_FILE"
        MODE=$(stat -c '%a' "$CREDENTIAL_FILE")
        case "$MODE" in *[1-7][0-9]|*[0-9][1-7]) echo 'credential file must not grant group/other access' >&2; exit 1 ;; esac
        cp "$CREDENTIAL_FILE" "$TEMPORARY"
    fi
    [ -s "$TEMPORARY" ] || { echo 'Agent credential is empty' >&2; exit 1; }
    install -m 0640 "$TEMPORARY" "$DESTINATION"
    rm -f "$TEMPORARY"
    trap - EXIT HUP INT TERM
}

install_agent() {
    require_binary lmt-agent
    require_value --node "$NODE"
    require_value --server-url "$SERVER_URL"
    require_value --mirror-root "$MIRROR_ROOT"
    safe_toml_string --node "$NODE"
    safe_toml_string --server-url "$SERVER_URL"
    safe_toml_string --mirror-root "$MIRROR_ROOT"
    printf '%s' "$NODE" | LC_ALL=C grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$' || {
        echo '--node is not a valid LMT Node name' >&2; exit 2;
    }
    case "$MIRROR_ROOT" in /*) ;; *) echo '--mirror-root must be absolute' >&2; exit 2 ;; esac
    [ "$MIRROR_ROOT" != / ] || { echo '--mirror-root must not be /' >&2; exit 2; }
    ATOMIC_COUNT=0
    [ -z "$PUBLICATION_ROOT" ] || ATOMIC_COUNT=$((ATOMIC_COUNT + 1))
    [ -z "$MAX_PRIVATE_GENERATIONS" ] || ATOMIC_COUNT=$((ATOMIC_COUNT + 1))
    [ -z "$RESERVE_BYTES" ] || ATOMIC_COUNT=$((ATOMIC_COUNT + 1))
    [ "$ATOMIC_COUNT" -eq 0 ] || [ "$ATOMIC_COUNT" -eq 3 ] || {
        echo 'Atomic installation requires publication root, generation bound, and reserve bytes together' >&2; exit 2;
    }
    if [ "$ATOMIC_COUNT" -eq 3 ]; then
        safe_toml_string --publication-root "$PUBLICATION_ROOT"
        case "$PUBLICATION_ROOT" in /*) ;; *) echo '--publication-root must be absolute' >&2; exit 2 ;; esac
        [ "$PUBLICATION_ROOT" != / ] || { echo '--publication-root must not be /' >&2; exit 2; }
        unsigned_number --max-private-generations "$MAX_PRIVATE_GENERATIONS"
        [ "$MAX_PRIVATE_GENERATIONS" -gt 0 ] || { echo '--max-private-generations must be positive' >&2; exit 2; }
        unsigned_number --reserve-bytes "$RESERVE_BYTES"
    fi
    install -d -m 0755 "$(target /usr/bin)"
    install -m 0755 "$BINARY_DIR/lmt-agent" "$(target /usr/bin/lmt-agent)"
    install -d -m 0700 "$(target /var/lib/lmt-agent)" "$(target /var/lib/lmt-agent/spool)"
    install -d -m 0755 "$(target "$MIRROR_ROOT")"
    [ -z "$PUBLICATION_ROOT" ] || install -d -m 0700 "$(target "$PUBLICATION_ROOT")"
    install_agent_secret
    CONFIG=$(target /etc/lmt/agent.toml)
    if [ ! -e "$CONFIG" ]; then
        umask 077
        cat >"$CONFIG" <<EOF
[node]
name = "$NODE"

[server]
url = "$SERVER_URL"
token_file = "/etc/lmt/agent.token"

[storage]
mirror_root = "$MIRROR_ROOT"
spool_dir = "/var/lib/lmt-agent/spool"
EOF
        if [ "$ATOMIC_COUNT" -eq 3 ]; then
            cat >>"$CONFIG" <<EOF
publication_root = "$PUBLICATION_ROOT"
publication_max_private_generations = $MAX_PRIVATE_GENERATIONS
publication_reserve_bytes = $RESERVE_BYTES
EOF
        fi
        cat >>"$CONFIG" <<'EOF'

[execution]
max_concurrent_runs = 1

[runner.process]
enabled = true

[logging]
level = "info"
format = "json"
EOF
        chmod 0640 "$CONFIG"
    fi
    if [ -z "$INSTALL_ROOT" ]; then
        chown -R lmt-agent:lmt-agent /var/lib/lmt-agent "$MIRROR_ROOT"
        [ -z "$PUBLICATION_ROOT" ] || chown -R lmt-agent:lmt-agent "$PUBLICATION_ROOT"
        chown root:lmt-agent /etc/lmt/agent.toml /etc/lmt/agent.token
        if [ "$ATOMIC_COUNT" -eq 3 ]; then
            runuser -u lmt-agent -- /usr/bin/lmt-agent --config /etc/lmt/agent.toml publication preflight
        fi
    fi
}

install_accounts
install_units
install_shared_config_dir
case "$ROLE" in
    server) install_server ;;
    agent) install_agent ;;
    all) install_server; install_agent ;;
    upgrade)
        [ -e "$(target /etc/lmt/server.toml)" ] && { require_binary lmt-server; require_binary lmt; install -m 0755 "$BINARY_DIR/lmt-server" "$(target /usr/bin/lmt-server)"; install -m 0755 "$BINARY_DIR/lmt" "$(target /usr/bin/lmt)"; }
        [ -e "$(target /etc/lmt/agent.toml)" ] && { require_binary lmt-agent; install -m 0755 "$BINARY_DIR/lmt-agent" "$(target /usr/bin/lmt-agent)"; }
        ;;
esac
enforce_config_permissions

if [ -z "$INSTALL_ROOT" ] && [ "$START_SERVICES" -eq 1 ]; then
    systemctl daemon-reload
    case "$ROLE" in
        server) systemctl enable --now lmt-server ;;
        agent) systemctl enable --now lmt-agent ;;
        all) systemctl enable --now lmt-server; systemctl enable --now lmt-agent ;;
        upgrade)
            systemctl is-enabled lmt-server >/dev/null 2>&1 && systemctl restart lmt-server || true
            systemctl is-enabled lmt-agent >/dev/null 2>&1 && systemctl restart lmt-agent || true
            ;;
    esac
fi

echo "LMT $ROLE installation complete"
