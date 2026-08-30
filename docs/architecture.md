# LMT Architecture v0.1

LMT (LZU Mirror Tools) is a small distributed control plane for operating public software mirrors across multiple Linux hosts.

The project intentionally does **not** attempt to become a repository platform, CDN, reverse proxy, monitoring database, log database, or general-purpose cluster scheduler. Its job is narrower:

- define mirrors;
- schedule mirror synchronization;
- dispatch executions to mirror nodes;
- keep durable run history;
- expose operational state through a CLI/API;
- provide standard observability interfaces.

## 1. Design goals

LMT is designed for long-lived community mirror infrastructure rather than ephemeral compute workloads.

The main goals are:

1. **Multi-node operation.** Mirrors can be distributed across different hosts.
2. **Per-node local policy.** Each node may have different paths, concurrency limits, runtimes, and local restrictions.
3. **Operational simplicity.** A small installation should require only the LMT binaries and local SQLite databases.
4. **Strong execution semantics.** A run must never be duplicated because of a lost HTTP response or temporary node disconnection.
5. **Good observability.** Run history, metrics, and structured logs are first-class design concerns.
6. **CLI-first operation.** The CLI is the primary administrative interface. Any web frontend is read-only/status-oriented.
7. **Composable infrastructure.** LMT integrates with external components instead of reimplementing them.

## 2. Non-goals

LMT v1 is not intended to implement:

- HTTP file serving;
- a reverse proxy or gateway;
- a log storage system;
- a metrics database;
- a full identity provider;
- automatic storage orchestration;
- Kubernetes-like dynamic placement;
- controller high availability;
- automatic cross-node failover;
- a generic plugin SDK.

Typical deployment integrations are expected to include:

- Nginx for serving mirror files;
- Prometheus for metrics;
- journald and optionally Loki for logs;
- Grafana for dashboards;
- Git for configuration history.

These are integrations, not hard dependencies.

## 3. Components

The core project contains three user-facing programs:

```text
                           lmt
                           CLI
                            |
                         HTTP/JSON
                            |
                            v
                    +----------------+
                    |   lmt-server   |
                    |                |
                    | config         |
                    | scheduler      |
                    | run state      |
                    | node state     |
                    | SQLite         |
                    +-------+--------+
                            |
                   HTTP long polling
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
         lmt-agent      lmt-agent      lmt-agent
          node-a         node-b         node-c
              |             |             |
              v             v             v
        process/container execution
```

### 3.1 `lmt-server`

The server is the single authoritative control-plane process for a deployment.

It owns:

- applied mirror configuration;
- schedules;
- node registry and liveness;
- creation of Runs and Attempts;
- retry policy;
- execution dispatch;
- durable run history;
- the administrative and agent HTTP APIs.

The server does **not** access mirror files directly.

### 3.2 `lmt-agent`

An agent runs on every mirror node.

It owns local execution concerns:

- receiving execution requests from the server;
- enforcing local configuration and policy;
- starting and cancelling processes;
- timeout enforcement;
- local concurrency limits;
- stdout/stderr capture;
- durable local attempt journal;
- reporting execution state back to the server.

The agent understands execution, not repository semantics. It should not contain Debian/PyPI/Fedora-specific logic.

The packaged Linux service is expected to run under systemd with automatic restart-on-failure and watchdog support. A daemon crash should normally self-heal without operator intervention.

### 3.3 `lmt`

The CLI is the primary operator interface.

Expected commands include:

```text
lmt config validate <path>
lmt config apply <path>

lmt mirror list
lmt mirror show <name>
lmt mirror sync <name>
lmt mirror enable <name>
lmt mirror disable <name>

lmt node list
lmt node show <name>

lmt run list
lmt run show <id>
lmt run cancel <id>
```

A web status page may consume the same read-only API, but administrative operations do not depend on a web UI.

## 4. Control plane and serving plane

The most important reliability boundary is:

> LMT must never be in the user download path.

A normal serving path is:

```text
client -> nginx -> mirror filesystem
```

not:

```text
client -> lmt-server -> mirror filesystem
```

If LMT, Prometheus, or the configuration database is unavailable, already-published mirror files must remain downloadable.

## 5. Source of truth

Mirror definitions are stored as node-scoped TOML files.

```text
config/
└── nodes/
    ├── mirror01/mirrors/*.toml
    └── mirror02/mirrors/*.toml
             |
             | lmt config apply
             v
authoritative applied configuration in lmt-server
             |
             v
SQLite + runtime state
```

The directory namespace defines which node owns each Mirror. Mirror TOML files do not contain a redundant placement field.

Git is recommended for versioning those TOML files, but LMT does not know about Git and never performs `git pull`.

This deliberately separates:

- **configuration history**: Git;
- **desired configuration set**: TOML files;
- **currently applied configuration**: LMT server;
- **runtime/history state**: LMT database.

A successful apply reconciles the managed server configuration to the authoritative TOML tree, including pruning Mirrors whose files were removed. Pruning management state never implicitly deletes mirror data from disk.

## 6. Database model

The default v1 deployment uses one authoritative SQLite database on the main machine running `lmt-server`.

```text
agent-a ----\
agent-b -----+--> HTTP --> lmt-server --> /var/lib/lmt/lmt.db
agent-c ----/
```

All queryable control-plane and historical state is centralized there and separated by tables, including Mirrors, generations, Nodes, Runs, and Attempts.

Agents do **not** maintain independent databases. They may keep a small local durable spool made of ordinary files for crash recovery and retransmission, but this spool is not authoritative state and is not intended for operator queries.

Run stdout/stderr is also centralized, but not stored as large database blobs. Agents upload log chunks to the server, which stores them under a central log directory (for example `/var/lib/lmt/logs/`). SQLite stores only log metadata/index information.

No SQLite database file is shared over NFS or another network filesystem.

PostgreSQL support may be added later if real deployments require multiple active controllers. It is not a v1 requirement.

## 7. Scheduling and node ownership

LMT v1 intentionally separates multi-node support from dynamic cluster scheduling.

Each agent represents one node, and each Mirror belongs to exactly one node-scoped configuration namespace. The server derives ownership from that namespace and dispatches Runs to that node only.

```text
config/nodes/mirror01/mirrors/ubuntu.toml
        |
        +--> server records owner_node = mirror01
        |
        +--> Runs go only to the mirror01 agent
```

This is desirable for mirror infrastructure because data placement is stable and operators generally want to know where a large repository physically resides.

Moving a configuration file to another node namespace is the explicit way to request a node reassignment. LMT does not perform automatic storage migration or automatic failover.

Automatic scoring, storage-aware placement, and cross-node migration are future features only if real deployments require them.

## 8. Execution boundary

The controller understands Mirrors. The agent understands Runs.

```text
Mirror configuration
       |
       | compile
       v
immutable RunSpec
       |
       v
lmt-agent
       |
       v
process / container
```

A running attempt is always based on an immutable RunSpec. Updating a mirror configuration while a run is active affects only future runs.

## 9. External integrations

LMT exposes standard integration points:

- `/metrics` for Prometheus;
- structured daemon logs for journald/log collectors;
- centrally retrievable Run stdout/stderr through the LMT API/CLI;
- read-only status API for status pages;
- JSON HTTP APIs for automation;
- stable command exit codes and machine-readable CLI output.

LMT should prefer integration contracts over bundled infrastructure.

## 10. Architecture invariants

The following rules should be treated as architecture invariants:

1. Serving does not depend on the control plane.
2. Agents never directly access the server database.
3. A delivered execution request is idempotent.
4. A temporary network failure must not create a duplicate writer.
5. Mirror configuration and runtime Run state are separate concepts.
6. A Run is immutable with respect to the configuration generation from which it was created.
7. Local node policy cannot be bypassed by central configuration.
8. Logs/metrics integrations are optional; durable business state is not.
9. Cross-node failover is not automatic in v1.
10. Complexity must be justified by a demonstrated operational requirement.
