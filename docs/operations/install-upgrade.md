# Installation and Upgrade

M4 ships a local idempotent installer. It configures one host; it is not an SSH
or cluster orchestrator. External configuration management may invoke it on
multiple hosts.

## Release archive

Build the locked release and deterministic archive on Linux:

~~~text
packaging/build-release.sh --output-dir dist
~~~

The archive contains `lmt`, `lmt-server`, `lmt-agent`, `install.sh`, systemd
units, configuration examples, essential operations/design documents, and the
frozen M3 wire fixtures. Verify the release checksum through the distribution
channel before extracting it on a host.

## Server installation

Choose the management bind address explicitly:

~~~text
sudo ./install.sh server --bind 127.0.0.1:8080
~~~

The installer creates the service account, state/log/backup directories,
systemd unit, initial TOML, and an operator token only when each is absent. It
does not overwrite existing configuration, tokens, or database state.

## Agent installation

Issue a Node credential with the operator CLI and store the one-time value in a
root-only file. Then install a Direct-only Agent:

~~~text
sudo ./install.sh agent \
  --node mirror01 \
  --server-url http://127.0.0.1:8080 \
  --mirror-root /srv/mirrors \
  --credential-file /root/mirror01.token
~~~

The input credential file must not grant group/other access. Alternatively pass
the credential on standard input with `--credential-stdin`; never place it in a
command argument.

For an Atomic-capable Agent, provide all storage policy values explicitly:

~~~text
sudo ./install.sh agent \
  --node mirror01 \
  --server-url http://127.0.0.1:8080 \
  --mirror-root /srv/mirrors \
  --credential-file /root/mirror01.token \
  --publication-root /srv/lmt-publication \
  --max-private-generations 4 \
  --reserve-bytes 10737418240
~~~

The installer runs publication preflight as the Agent service user before
starting the service. It never guesses a publication root or places private
state below the served mirror root.

`all` installs Server and Agent on one host using the union of their explicit
arguments. `--no-start` installs without enabling/starting services. `--root`
is reserved for safe staging/testing below a non-root directory.

## M3 to M4 rolling upgrade

The supported order is forward-only:

1. Save the exact authoritative TOML bundle and create/verify a control-plane
   backup.
2. Upgrade the Server first with `sudo ./install.sh upgrade`.
3. Confirm M3 Agents still run Direct Mirrors through the M4 Server.
4. Upgrade Agents and confirm `atomic_exchange_v1` in status/doctor.
5. Run `lmt-agent doctor` locally on Atomic-capable Nodes.
6. Enable Atomic mode only for selected quiescent Mirrors and review the config
   plan's mode-change marker.

Upgrade replaces installed binaries/units while preserving configuration,
secrets, SQLite state, Run logs, Agent identity/spool, mirror data, and private
publication state.

M3 Server with M4 Agent is unsupported. An M4-to-M3 downgrade is not an in-place
binary rollback. Follow [Backup and Restore](backup-restore.md): resolve all
protected publication evidence using M4 tooling, stop all components, restore
the matching pre-M4 database and TOML bundle, then install matching M3 binaries.

## Installer boundaries

The installer does not modify firewall/routing, TLS gateways, Nginx,
Docker/Podman/Kubernetes, filesystem formats/mounts, or unrelated services.
Those remain explicit site administration responsibilities.
