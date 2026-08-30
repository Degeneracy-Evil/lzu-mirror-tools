# Architecture Decisions

This document records current design decisions for LMT. They are intentionally lightweight rather than formal ADR files at this stage.

## Accepted decisions

### D001 - Rust for the core implementation

Status: accepted.

`lmt-server`, `lmt-agent`, and `lmt` are implemented in Rust.

Expected foundational crates include Tokio, Axum, Serde, SQLx, Clap, and tracing.

Rationale:

- strong modeling for state machines and protocol enums;
- memory/concurrency safety for long-lived daemons;
- good CLI and async networking ecosystem;
- long-term correctness is more valuable than the minimum initial code volume.

Custom synchronization programs are not required to use Rust.

### D002 - TOML configuration

Status: accepted.

Human-authored LMT configuration uses TOML.

Git is recommended for configuration version history, but LMT has no Git integration and does not automatically pull repositories.

### D003 - No plugin SDK in v1

Status: accepted.

Custom synchronization is performed through ordinary executables. Built-in sync types such as rsync compile down to the same generic execution model.

A plugin API will only be designed if actual third-party requirements justify one.

### D004 - Single server, multiple agents

Status: accepted.

The initial architecture contains one authoritative `lmt-server` and any number of `lmt-agent` nodes.

Multi-controller HA is outside v1.

### D005 - SQLite by default

Status: accepted.

The server uses a local SQLite database. Agents do not directly access it.

Each agent may also use its own independent local SQLite journal.

SQLite files are never shared through NFS or another network filesystem.

### D006 - HTTP/JSON agent protocol

Status: accepted.

Agents initiate communication to the server using HTTP + JSON.

Control uses bounded long polling. State reports use normal HTTP POST requests.

gRPC and WebSocket are not required.

### D007 - Explicit mirror placement

Status: accepted.

v1 Mirror configuration names the target node explicitly.

The project will not implement Kubernetes-like dynamic scheduling unless real mirror deployments later justify it.

### D008 - Controller understands Mirrors; Agent understands execution

Status: accepted.

The server compiles Mirror configuration into immutable RunSpecs.

The agent implements execution/runners and local policy, but does not implement repository-specific semantics.

### D009 - CLI-first administration

Status: accepted.

All administrative functionality must be available through the CLI/API.

A web frontend is optional and expected to be read-only/status-oriented.

### D010 - Control plane is never in the serving path

Status: accepted.

Nginx or another serving layer reads mirror data directly. LMT failure must not stop clients from downloading already-available files.

### D011 - No automatic cross-node failover in v1

Status: accepted.

If a node disappears while a mirror synchronization is active, LMT does not automatically start the same mirror on another node.

Avoiding duplicate writers is more important than control-plane availability.

## Current open questions

The following points should be resolved before or during the first implementation milestone:

1. Exact TOML schema and validation rules.
2. Whether v0.1 ships only the process runner or also an OCI container runner.
3. Exact run-log retention model and how `lmt run logs` should work without making SQLite a log database.
4. Scheduler behavior when multiple periodic triggers occur while one mirror is already running or its node is offline.
5. Whether rsync statistics should be parsed into structured Run metrics in the first milestone.
6. Agent enrollment/token provisioning UX.
7. Database migration/versioning strategy.
8. Stable API versioning rules before the first public release.

## Development principle

When choosing between a broader abstraction and a smaller design, prefer the smaller design until a real mirror workload demonstrates the need for the abstraction.
