# LZU Mirror Tools

LZU Mirror Tools (LMT) is a distributed, CLI-first control plane for reliably scheduling, executing, and observing public software mirror synchronization across multiple Linux hosts.

The project is being designed and maintained for long-term community use, initially for the Lanzhou University mirror infrastructure.

## Status

**Current phase: M1 accepted; M2 implementation candidate is under hardening review.**

The accepted M1 implementation baseline is `e2c27dbdc573dc374c94902255265adc81b2ae10`. See the [M1 acceptance review](docs/reviews/m1-review-round2-2026-08-30.md).

M2 implementation landed at `68d56837454f5902d97fe012508b32e282556df0`, but M2 is not accepted until the release blockers in [the M2 code review](docs/reviews/m2-review-2026-08-31.md) are resolved. The frozen M2 behavior remains defined in:

1. [M2 Design](docs/m2-design.md)
2. [M2 Implementation Plan](docs/m2-implementation-plan.md)

## What LMT does

LMT manages:

- authoritative TOML mirror configuration;
- multi-node mirror ownership;
- scheduling and retries;
- Server-Agent execution;
- Run/Attempt history;
- centralized Run logs;
- CLI/API operations;
- standard observability interfaces.

LMT intentionally does not replace mature infrastructure such as:

- Nginx for serving mirror files;
- Prometheus/Grafana for metrics and dashboards;
- journald/Loki for daemon log aggregation;
- Git/GitHub for configuration history.

The LMT control plane is never in the user download path.

## Core topology

```text
Git/TOML configuration
        |
        v
   lmt-server
   + scheduler
   + state engine
   + central SQLite
   + central Run logs
        |
     HTTP/JSON
    long polling
        |
  +-----+-----+
  |           |
lmt-agent  lmt-agent
  |           |
process     process
  |           |
rsync / scripts / executables

client -> nginx -> mirror files
```

## Implementation language

The core implementation is Rust.

Workspace:

```text
libraries:
  lmt-core
  lmt-protocol
  lmt-store

binaries:
  lmt-server
  lmt-agent
  lmt-cli
```

## Documentation

Start with:

1. [Design Summary](docs/design-summary.md)
2. [Documentation Index](docs/README.md)
3. [M2 Design](docs/m2-design.md)
4. [M2 Implementation Plan](docs/m2-implementation-plan.md)

All architecture, state-machine, protocol, database, scheduler, API, testing, and code-review contracts live under `docs/`.

## Development rule

If implementation experience reveals that a documented design assumption is wrong, do not silently work around it in code.

Update the relevant design document and architecture decision first, then change implementation.

## License

See [LICENSE](LICENSE).
