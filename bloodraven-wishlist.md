# Bloodraven Wishlist

Requirements and feature requests for adopting Bloodraven as the MySQL operator and failover
orchestrator for the ShipStream platform. This replaces the planned mysql-watcher component.

## Context

ShipStream runs a multi-tenant WMS platform on k3s across two DCs per availability zone. Each AZ
has one MySQL primary and one async replica. Failover must be fast, safe, and automated. Tenants
run warm-standby pods in both DCs — web pods serve a maintenance page when MySQL is read-only,
and php-cron/runner pods are evicted via node taints to the writable DC.

Bloodraven already covers the core topology (2-site async replication, taint-based eviction,
DNSEndpoint integration, sidecar self-fencing). This document covers what's missing or needs
to change before we can run real tenant workloads on it.

---

## P0 — Must have before first production tenant

### 1. DNS flip before MySQL promotion (reverse the failover sequence)

**Current behavior:** The failover sequence promotes MySQL first (steps 1-6), then updates DNS
(step 7), then updates taints (step 8).

**Required behavior:** DNS and taints should update *first*, then MySQL promotion happens. Our web
pods are warm in both DCs and independently detect `@@read_only` — they serve a maintenance page
when MySQL is read-only. Users see maintenance for a few seconds while MySQL promotes rather than
seeing errors while DNS propagates after promotion.

Proposed failover sequence:
1. Fence old primary (`SET GLOBAL super_read_only=ON`) — skip if unreachable
2. Update DNSEndpoint (point to new active site's LB IP)
3. Update node taints (taint old site, untaint new site)
4. Drain relay logs on candidate (30s timeout)
5. `STOP REPLICA`
6. `RESET REPLICA ALL`
7. `SET GLOBAL read_only=0` (promote)
8. Confirm promotion on next poll cycle

This way, DNS propagation happens *in parallel* with relay log draining and promotion, shaving
30+ seconds off user-visible downtime.

### 2. MySQL configuration management

Add a `spec.mysqlConfig` field (map of string to string) that the operator renders into a
ConfigMap and mounts as `/etc/mysql/conf.d/bloodraven.cnf` in the MySQL container.

Required for production tuning:
```yaml
spec:
  mysqlConfig:
    innodb_buffer_pool_size: "8G"
    max_connections: "500"
    binlog_format: "ROW"
    gtid_mode: "ON"
    enforce_gtid_consistency: "ON"
    innodb_flush_log_at_trx_commit: "1"
    sync_binlog: "1"
    # etc.
```

Changes to `mysqlConfig` should trigger an ordered rolling restart (respecting `updateStrategy:
OrderedUpdate`).

### 3. Container resource requests and limits

Add `spec.resources` (for the MySQL container) and `spec.sidecarResources` (for the sidecar)
using standard Kubernetes resource requirements:

```yaml
spec:
  resources:
    requests:
      cpu: "2"
      memory: "16Gi"
    limits:
      cpu: "4"
      memory: "16Gi"
  sidecarResources:
    requests:
      cpu: "100m"
      memory: "64Mi"
    limits:
      cpu: "200m"
      memory: "128Mi"
```

Non-negotiable for multi-tenant isolation and capacity planning.

### 4. Multiple MySQL credentials

Replace the single `secretName` (with one `dsn` key) with structured credential management:

```yaml
spec:
  credentials:
    operatorSecretName: mysql-operator-creds    # replication + operator management
    appSecretName: mysql-app-creds              # application read-write
    readOnlySecretName: mysql-readonly-creds    # application read-only
    monitorSecretName: mysql-monitor-creds      # prometheus exporter
    backupSecretName: mysql-backup-creds        # xtrabackup / mysqldump
```

The operator should create these MySQL users with appropriate GRANTs during initial setup and
rotate them when the referenced Secrets change. At minimum, the operator and app credentials
must be separate — the operator user needs `SUPER`/`REPLICATION_SLAVE_ADMIN` privileges that
should never be in application connection strings.

### 5. Backup and restore

The operator must support automated backups. Proposed CRD additions:

```yaml
spec:
  backup:
    schedule: "0 */6 * * *"           # cron schedule
    method: xtrabackup                # or mysqldump
    storage:
      s3:
        bucket: shipstream-backups
        prefix: "{{ .FailoverGroup }}/{{ .Site }}"
        secretName: s3-credentials
    retention:
      count: 28                       # keep last N backups
      days: 30                        # or retention by age
    pitr: true                        # archive binlogs for point-in-time recovery
```

Backups should run on the replica site to avoid impacting the primary. The operator should
track backup status in the CRD status and expose Prometheus metrics (last backup time, backup
duration, backup size, last successful backup age).

A restore procedure (even if manual via kubectl plugin or documented runbook) is also required.

### 6. Reduce default failover cooldown

Change the default `failoverCooldown` from `60m` to `5m`. For a WMS processing orders, 60
minutes of suppressed automatic failover after a single event is unacceptable. If site B dies
5 minutes after failing over from site A, we need automatic recovery — not a human paged at
3am.

The cooldown should also support an escalating backoff: e.g., first failover cooldown is 5m,
second within an hour is 15m, third within an hour suppresses further automatic failovers and
alerts for manual intervention. This prevents flapping without sacrificing availability.

### 7. Data loss detection and reporting on emergency failover

When the old primary is unreachable and we promote the replica, there may be committed
transactions on the old primary that never replicated. The operator must:

1. Record the promoted replica's `GTID_EXECUTED` at promotion time in status
2. When the old primary comes back online, compare GTID sets using `GTID_SUBTRACT`
3. Report the number of lost transactions in status and as a Prometheus metric
4. Fire an alert if `lost_transactions > 0`
5. Document the recovery procedure for reconciling divergent GTID sets

This is the hardest operational scenario and it cannot be handwaved.

### 8. Old primary recovery procedure

After an emergency failover, the old primary may come back with divergent transactions. The
operator needs a documented (and ideally automated) recovery path:

1. Old primary comes back online — operator detects it's writable (or was writable)
2. Operator fences it immediately (`SET GLOBAL super_read_only=ON`)
3. Operator compares GTID sets
4. If no divergence: reconfigure as replica of new primary, `START REPLICA`
5. If divergence detected: alert, do NOT automatically reconfigure — require manual intervention
   (the operator should expose a `spec.sites[].forceRejoin: true` field or similar to allow
   an admin to explicitly accept data loss and rejoin)

Currently `RESET REPLICA ALL` during failover destroys all replication config, making
automatic re-establishment impossible without this logic.

---

## P1 — Required before multi-tenant scale

### 9. Shared-node support (per-failover-group taint scoping)

The current placement contract requires nodes dedicated to a single failover group. At scale
(10+ tenants per AZ), this means 10+ node pools per site, which is cost-prohibitive.

We need the taint to be scoped per failover group:

```
shipstream.io/db-readonly-<failover-group>=true:NoExecute
```

For example: `shipstream.io/db-readonly-orders=true:NoExecute`

This allows multiple failover groups to share nodes. Application pods only need to not-tolerate
the taint for their specific failover group. A failover in the `orders` group doesn't evict
pods belonging to the `inventory` group.

This is the single biggest blocker for multi-tenant economics.

### 10. Per-site extra containers and init containers

We need to run a Prometheus mysqld_exporter sidecar alongside MySQL. The CRD should support:

```yaml
spec:
  extraContainers:
    - name: mysqld-exporter
      image: prom/mysqld-exporter:latest
      env:
        - name: DATA_SOURCE_NAME
          valueFrom:
            secretKeyRef:
              name: mysql-monitor-creds
              key: dsn
      ports:
        - containerPort: 9104
  extraInitContainers:
    - name: restore-from-backup
      image: percona/percona-xtrabackup:8.0
      # ...
```

This avoids needing to build custom MySQL images just to add exporters or init logic.

### 11. Pod annotations and labels passthrough

Add `spec.podAnnotations` and `spec.podLabels` so we can attach:
- Prometheus scrape annotations (`prometheus.io/scrape: "true"`, `prometheus.io/port: "9104"`)
- Network policy labels
- Cost allocation labels
- Any other metadata the operator shouldn't need to know about

### 12. PodDisruptionBudget management

The operator should create a PDB for each failover group ensuring at least one MySQL instance
is always available during voluntary disruptions (node drains, upgrades). Currently if someone
runs `kubectl drain` on a node hosting the primary, it'll get evicted with no protection.

### 13. Configurable service types and annotations

The generated Services (`mysql-<name>-primary`, etc.) need configurable types and annotations:

```yaml
spec:
  serviceTemplate:
    type: ClusterIP                    # or LoadBalancer, NodePort
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-internal: "true"
```

Needed for environments where MySQL must be exposed via LoadBalancer with specific cloud
provider annotations.

### 14. Webhook-based notifications

**Resolution:** Rather than building custom webhook support into the operator, Bloodraven emits
comprehensive Kubernetes Events for all critical operations (failover, split brain, data loss,
backup lifecycle, restore, etc.). Standard Kubernetes-native tools forward these events to any
notification target:

- [Kubewatch](https://github.com/robusta-dev/kubewatch) — Slack, PagerDuty, webhooks
- [Argo Events](https://argoproj.github.io/argo-events/) — event-driven workflows with rich
  filtering and routing
- [Event Router](https://github.com/heptiolabs/eventrouter) — forwards events to sinks like
  Elasticsearch or Google Cloud Logging

This approach avoids reinventing notification plumbing in the operator while giving users more
flexibility in how they route and filter events. See the
[Monitoring docs](docs/docs/monitoring.mdx#kubernetes-events) for the full event reference and
a Kubewatch integration example.

---

## P2 — Nice to have

### 15. Automatic initial clone

When adding a new site (or re-initializing after data loss), the operator should support
automatic cloning from the existing primary using xtrabackup streaming, rather than requiring
manual data seeding. The `cloneTimeout` field already exists in the CRD — implement the
actual clone logic behind it.

### 16. Read replica lag-aware routing

The `-replicas` Service currently just checks `shipstream.io/healthy: yes`. Add support for
lag-weighted routing or lag-threshold-based endpoint removal that's more granular than the
current binary healthy/unhealthy. Expose `maxLagSeconds` as a first-class metric with
alerting integration.

### 17. Maintenance mode

Add a `spec.maintenanceMode: true` field that:
- Sets both sites to `super_read_only=ON`
- Taints all nodes in both sites
- Suppresses all automatic failover
- Clearly surfaces the state in status and metrics

Useful for planned maintenance windows (schema migrations, MySQL upgrades, etc.).

### 18. Operator high availability

The single-replica operator is a SPOF for *recovery* (not safety — the sidecar handles
safety). For production, the operator should support leader election so multiple replicas
can run with automatic leader failover. If the active operator pod dies, another takes over
within seconds rather than waiting for Kubernetes to reschedule.

### 19. kubectl plugin

A `kubectl bloodraven` plugin for common operations:
- `kubectl bloodraven status <group>` — human-readable failover group status
- `kubectl bloodraven failover <group> --to <site>` — manual failover
- `kubectl bloodraven fence <group> <site>` — emergency fencing
- `kubectl bloodraven rejoin <group> <site>` — rejoin after divergence
- `kubectl bloodraven backup <group>` — trigger ad-hoc backup

### 20. Helm chart with ArgoCD compatibility

The operator should ship as a Helm chart that works cleanly with ArgoCD:
- CRDs managed via `crds/` directory (ArgoCD handles CRD lifecycle)
- Proper `app.kubernetes.io/*` labels
- Configurable via values.yaml, not just the CRD
- Health checks registered so ArgoCD can track operator health

---

## Compatibility Requirements

- **MySQL version:** Must support MySQL 9.x (Oracle, not Percona). We're on 9.6 today.
- **Kubernetes:** k3s 1.30+
- **DNS:** Cloudflare via external-dns (DNS-only mode, no proxy)
- **TLS:** cert-manager with Let's Encrypt ClusterIssuer
- **Secrets:** Kubernetes-native secrets (no external secret stores)
- **GitOps:** ArgoCD with ApplicationSets + Helm

## Non-goals

- Group Replication / InnoDB Cluster — we use async replication intentionally
- Multi-source replication — one primary, one replica, period
- More than 2 sites per failover group — not needed
- Percona Server for MySQL — sticking with Oracle MySQL
