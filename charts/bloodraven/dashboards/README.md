# Bloodraven Grafana dashboards

The chart ships six dashboards. The MySQL Workload dashboard adds per-site metrics from the optional `mysqld_exporter`.

| File | UID | Purpose |
|---|---|---|
| `overview.json` | `bloodraven-overview` | Start here. Writable/unreachable sites, replication lag, backup + archiver freshness, failover activity, paced rollout of an operator release. |
| `failover.json` | `bloodraven-failover` | Auto + planned failovers, duration/lag-wait histograms, state-transition flap detector, DNS flips, taints, split-brain auto-resolves. |
| `replication.json` | `bloodraven-replication` | Replication lag per site, IO/SQL thread up/down state timeline, divergent transactions, reclones, poll latency. |
| `backups.json` | `bloodraven-backups` | Backup runs by result, duration p50/p95, size, verification freshness and PITR replay lag. |
| `archiver.json` | `bloodraven-archiver` | Per-site PITR archiver: age of last upload, backlog files, upload failure rate. |
| `mysql-workload.json` | `bloodraven-mysql-workload` | Per-site connection use against `max_connections`, row lock waits, query throughput and site role. Needs the opt-in `mysqld_exporter` (see Prerequisites); the other five do not. |

The dashboards share datasource, namespace, group, and site filters. Links keep the selected time range and filters.

## Install — three paths

### 1. Helm + Grafana sidecar (kube-prometheus-stack users)

```bash
helm upgrade --install bloodraven bloodraven/bloodraven \
  --namespace bloodraven --create-namespace \
  --set grafanaDashboards.enabled=true
```

One ConfigMap per JSON file is rendered with the `grafana_dashboard: "1"` label. The Grafana sidecar picks them up within ~30s and they appear in a "Bloodraven" folder. Tweakable via `grafanaDashboards.namespace`, `.label`, `.labelValue`, and `.folder`.

### 2. File-based provisioning

Copy the JSON into your provisioning path and point a file provider at it. Example provider config:

```yaml
apiVersion: 1
providers:
  - name: bloodraven
    folder: Bloodraven
    type: file
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards/bloodraven
```

### 3. Manual UI import

Grafana → Dashboards → New → Import → paste JSON → pick datasource. Repeat per file.

## Prerequisites

A Prometheus scraping the operator. The chart ships a `ServiceMonitor` — enable with `--set metrics.serviceMonitor.enabled=true`. See [`../templates/servicemonitor.yaml`](../templates/servicemonitor.yaml) and [`site/content/docs/8.observability/2.monitoring.md`](../../../site/content/docs/8.observability/2.monitoring.md) for alternatives (`PodMonitor`, raw scrape job).

### MySQL workload dashboard

The MySQL Workload dashboard uses `bloodraven_site_state` for site role and the optional exporter for workload measurements.

1. Deploy the exporter and configure the scrape job. [`examples/mysqld-exporter.yaml`](../../../examples/mysqld-exporter.yaml) and [`examples/mysqld-exporter-scrape.yaml`](../../../examples/mysqld-exporter-scrape.yaml). Setup steps are in [`site/content/docs/8.observability/3.monitoring-prometheus.md`](../../../site/content/docs/8.observability/3.monitoring-prometheus.md).
2. Set the Prometheus datasource's scrape interval to `30s`, the interval of the example job.
3. If a panel is empty, check `up{job="bloodraven-mysql"}`, `mysql_up` and `mysql_exporter_collector_success`. A gap means no value was produced; a line at `0` is a measured zero. The states are in [`site/content/docs/8.observability/2.monitoring.md`](../../../site/content/docs/8.observability/2.monitoring.md) and the troubleshooting table in [`4.monitoring-grafana.md`](../../../site/content/docs/8.observability/4.monitoring-grafana.md).

The dashboard holds no credentials. Only the exporter reads the group's `monitorSecretName` Secret.

## Updating the dashboards

Edit the JSON here, bump the `version` field inside the file, and send a PR. If you edited a dashboard in the Grafana UI and want to export the result back:

1. Grafana → dashboard → share → export → "Save to file"
2. Open the JSON and **remove** the `id` field (Grafana assigns a fresh one on import).
3. Keep the `uid` stable so cross-dashboard links keep working.
4. Prefer PromQL that matches the metric names in [`internal/metrics/metrics.go`](../../../internal/metrics/metrics.go) literally — changes to labels there will silently break panels.
