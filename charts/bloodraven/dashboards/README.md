# Bloodraven Grafana dashboards

Five dashboards covering every metric the operator publishes on `:8080/metrics`.

| File | UID | Purpose |
|---|---|---|
| `overview.json` | `bloodraven-overview` | Start here. Writable/unreachable sites, replication lag, backup + archiver freshness, failover activity. |
| `failover.json` | `bloodraven-failover` | Auto + planned failovers, duration/lag-wait histograms, state-transition flap detector, DNS flips, taints, split-brain auto-resolves. |
| `replication.json` | `bloodraven-replication` | Replication lag per site, IO/SQL thread up/down state timeline, divergent transactions, reclones, poll latency. |
| `backups.json` | `bloodraven-backups` | Backup runs by result, duration p50/p95, size, verification freshness and PITR replay lag. |
| `archiver.json` | `bloodraven-archiver` | Per-site PITR archiver: age of last upload, backlog files, upload failure rate. |

All five dashboards use a `datasource` template variable, share a sensible `site` / `group` / `namespace` filter set, and cross-link in the top-left so you can roam between them without losing the time range.

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

## Updating the dashboards

Edit the JSON here, bump the `version` field inside the file, and send a PR. If you edited a dashboard in the Grafana UI and want to export the result back:

1. Grafana → dashboard → share → export → "Save to file"
2. Open the JSON and **remove** the `id` field (Grafana assigns a fresh one on import).
3. Keep the `uid` stable so cross-dashboard links keep working.
4. Prefer PromQL that matches the metric names in [`internal/metrics/metrics.go`](../../../internal/metrics/metrics.go) literally — changes to labels there will silently break panels.
