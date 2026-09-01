# Observability

LMT has three intentionally separate observability channels.

## 1. Run stdout/stderr

Repository sync output is stored as central LMT Run-log files.

Operators use:

~~~text
lmt run logs <run-id>
lmt run logs <run-id> --follow
~~~

Both commands consume the complete chunked API stream. Human mode writes log
chunks directly. `--output json` emits newline-delimited JSON chunk objects so
automation can process arbitrarily long logs with bounded memory.

These logs are not daemon logs and are not stored as SQLite BLOBs.

## 2. Daemon logs

lmt-server and lmt-agent emit structured logs to stdout/stderr.

Under production systemd units, journald stores them.

Optional Loki collection can consume journald through the site's standard log collector.

LMT does not bundle Loki or modify host-global journald retention.

Useful daemon context includes:

- component/version;
- node;
- mirror;
- run_id;
- attempt;
- credential ID;
- stable error code.

Bearer secrets are never logged.

## 3. Prometheus metrics

/metrics is the machine monitoring surface.

M3 requires bounded-cost Store queries instead of loading full Run history during every scrape.

Useful aggregate metrics include:

~~~text
lmt_runs_pending
lmt_runs_running
lmt_mirrors_due
lmt_nodes_online
lmt_run_logs_stored_bytes
lmt_backup_last_success_timestamp_seconds
lmt_backup_failures_total
lmt_log_expired_total
lmt_auth_failures_total
~~~

Bounded entity labels are allowed for Mirror/Node state:

~~~text
lmt_mirror_last_success_timestamp_seconds{mirror,node}
lmt_mirror_last_terminal_timestamp_seconds{mirror,node}
lmt_mirror_due{mirror}
lmt_node_online{node}
lmt_node_last_seen_timestamp_seconds{node}
lmt_node_mirror_root_free_bytes{node}
~~~

Never label by Run ID, Attempt number, credential ID, or free-form error messages.

## Grafana

Import `examples/monitoring/grafana-overview.json` into an existing Grafana and
select the site's Prometheus data source. This is an example overview dashboard,
not a dashboard engine managed by LMT.

The dashboard should emphasize:

- mirrors currently due/stale;
- last success age;
- current Runs;
- failures/retries;
- Node liveness/free storage;
- control-plane log storage;
- backup recency.

## Alerts

Representative alerts:

- Server not ready;
- Node offline;
- Mirror has exceeded expected freshness window;
- repeated Run failures;
- mirror-root free space low;
- central log/database filesystem low;
- no recent successful backup.

Exact site thresholds belong to deployment configuration.

## Scrape example

`examples/monitoring/prometheus.yml` contains the smallest static scrape job.
Merge its `scrape_configs` entry into the site's Prometheus configuration; do
not replace an existing configuration wholesale. The example assumes the
Server is reachable at `127.0.0.1:8080` from Prometheus. If metrics traverse a
reverse proxy, protect that route using the site's management-network policy.

## journald to Loki example

LMT deliberately does not ship a collector. An existing Promtail-compatible
collector can select only the two systemd units before forwarding to Loki:

~~~yaml
scrape_configs:
  - job_name: lmt-journal
    journal:
      labels:
        job: lmt-daemons
    relabel_configs:
      - source_labels: [__journal__systemd_unit]
        regex: lmt-(server|agent)\.service
        action: keep
      - source_labels: [__journal__systemd_unit]
        target_label: systemd_unit
~~~

Collector positions, Loki credentials, transport TLS, retention, and tenant
labels remain deployment responsibilities. Do not scrape central Run-log files
with this job: users retrieve those through `lmt run logs` and LMT retention.

Useful local daemon-log commands are:

~~~text
journalctl -u lmt-server.service --since today
journalctl -u lmt-agent.service --since today
journalctl -u lmt-agent.service -f -o json
~~~

## Read-only status

The status API is a sanitized projection for human/status-page consumption.

It should expose facts used by dashboards without exposing source URLs, local paths, RunSpec, logs, or secrets.
