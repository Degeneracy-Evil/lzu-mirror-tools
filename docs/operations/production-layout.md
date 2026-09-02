# Production-Trial Layout

This document defines the M3 production-trial filesystem and service assumptions.

It is not yet a distribution packaging guide.

## Server

Recommended ownership:

~~~text
/etc/lmt/                     root:root      0755
/etc/lmt/server.toml          root:lmt       0640
/etc/lmt/operator.token       root:lmt       0640

/var/lib/lmt/                 lmt:lmt        0750
/var/lib/lmt/lmt.db           lmt:lmt        0640
/var/lib/lmt/logs/            lmt:lmt        0750
/var/lib/lmt/backups/         lmt:lmt        0750

/run/lmt/                     lmt:lmt        runtime state/lock
~~~

Production server.toml should reference token files rather than embedding raw secrets.

The Server remains a single-controller process.

## Agent

Recommended ownership:

~~~text
/etc/lmt/agent.toml           root:lmt-agent 0640
/etc/lmt/agent.token          root:lmt-agent 0640

/var/lib/lmt-agent/           lmt-agent      0700
/var/lib/lmt-agent/spool/     lmt-agent      0700

/srv/mirrors/                 site policy
~~~

mirror_root must be writable by the Agent execution user and readable by the serving stack.

The Agent should normally run as a dedicated non-root user.

## Serving plane

Nginx or another file server reads mirror_root directly.

LMT is not on the client download path.

A control-plane outage must not make already-synchronized mirror files unavailable.

## Network

Recommended M3 production patterns:

- bind lmt-server to loopback and reverse-proxy it through the site's existing HTTPS/admin gateway; or
- bind to a trusted private management network.

M3 does not implement its own TLS/PKI stack.

The public mirror HTTP/HTTPS endpoint is separate from the LMT admin/Agent API.

## Service configuration

Server may use strong systemd sandboxing because it never launches repository sync commands.

Agent hardening is intentionally more conservative because child sync processes inherit the Agent service sandbox.

Do not add a systemd write-path override that silently duplicates mirror_root configuration.

## State disks

The authoritative SQLite database must remain on a local filesystem.

The central Run-log directory may be placed on a filesystem with transparent compression such as ZFS/btrfs.

Backups should ultimately be copied off the database disk; a local backup directory is staging, not disaster recovery.

## Single-instance locks

M3 Server and Agent acquire advisory process locks.

A lock conflict is an operational error and must cause startup refusal rather than allowing a second control-plane/Agent instance to race.

## Configuration visibility

Normal runtime configuration lives in TOML files.

M3 does not add environment-variable overrides for ordinary LMT configuration.

Internal systemd/kernel protocol environment variables are not considered user configuration.

## Production-trial examples

Start from `config/server.example.toml` and `config/agent.example.toml`, then
install secrets separately with the ownership and modes above. Representative
Mirror documents live under `config/nodes/mirror01/mirrors/`:

- `rsync-simple.toml` demonstrates minimal periodic rsync;
- `rsync-production.toml` demonstrates delete, hard-link, numeric-ID, and cron settings;
- `command-hook.toml` is disabled by default and demonstrates an explicit site command.

Review source URLs, target names, deletion semantics, time zones, timeouts, and
the command executable before applying them. The `archive.example.org` sources
are documentation placeholders.

The shared `/etc/lmt` directory must be traversable by both service users. Keep the directory `root:root 0755`; protect secrets and service-specific configuration with the individual file modes above.
