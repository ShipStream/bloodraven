I want to use MySQL 9.6 on a k8s cluster and I want to support async replication using the clone plugin for seeding, gtid auto mode, and ssl. There are other operators out there based on Orchestrator, but these are either no longer well supported or they are for Percona, but Percona doesn't support the latest MySQL features yet. So I'm thinking about building my own operator that has no GUI at all, supports only this narrow feature set and basically just handles: bootstrapping and failover. I may add backups later. I don't want to use a router (the access to mysql will always be direct). How hard will this be?2:40 PMThis is a well-scoped project and very doable — probably easier than you think, given how narrow you're keeping it. Here's a dense breakdown.

Core complexity: moderate. You're essentially building a stateful controller that manages a MySQL StatefulSet and reacts to topology changes. The hard part isn't Kubernetes — it's getting MySQL failover right. But by choosing GTID auto-positioning and the clone plugin, you've already eliminated the two nastiest problems (log file/position tracking and snapshot-based seeding).
What the operator actually needs to do:
Bootstrapping: Create a StatefulSet with N pods. Pod-0 becomes primary (bootstrap with --initialize). Pods 1–N use CLONE INSTANCE from pod-0 to seed, then CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1 with SSL options (SOURCE_SSL=1, SOURCE_SSL_CA, etc.). You'll need an init container or entrypoint script that checks whether the data directory is empty and branches accordingly. Store topology state (who's primary) in a ConfigMap or a lightweight CRD status field.
Health checking: A sidecar or the operator itself polls SHOW REPLICA STATUS and checks Seconds_Behind_Source, Replica_IO_Running, Replica_SQL_Running. Also monitor the primary with a simple SELECT 1 or mysqladmin ping. This is your failure detection loop.
Failover: This is the only genuinely tricky part. When the primary dies: stop replication on all replicas, compare GTID_EXECUTED sets to pick the most advanced replica, promote it (STOP REPLICA; RESET REPLICA ALL), repoint the remaining replicas to the new primary. Update a headless Service or just the ConfigMap so your app knows the new primary endpoint. The GTID comparison logic is ~20 lines — you're looking for the superset, or if none is a strict superset, the one with the highest transaction count.
SSL: Generate certs with cert-manager, mount them into the pods. MySQL config just needs require_secure_transport=ON and the ssl-ca, ssl-cert, ssl-key paths. Replication channels get the SOURCE_SSL_* options. Not complicated, just tedious to wire up the first time.
What you can skip since you're going no-router, no-GUI: no ProxySQL/MySQL Router config, no web dashboard, no read/write splitting logic. Your "service discovery" is literally a headless Service + a ConfigMap that says which pod is primary. Apps connect directly.
Tech choices for the operator itself: Use Go with controller-runtime (kubebuilder scaffolding) if you want it production-grade, or Python with kopf if you want to iterate faster. kopf is surprisingly capable for a single-CRD operator and you'd probably have a working prototype in a weekend. The reconciliation loop is simple: desired state (1 primary, N-1 replicas, all healthy) vs. actual state (query each pod's MySQL). Diff and act.
Rough estimate: A competent developer who knows MySQL replication and has some k8s operator experience could have bootstrapping + basic failover working in 1–2 weeks. Hardening (edge cases like split-brain prevention, fencing the old primary, handling network partitions, crash-during-failover) adds another 2–4 weeks. Backups later is easy — just a CronJob that runs mysqldump or mysqlsh dump against the most-caught-up replica.
The one thing to be careful about: fencing. When you promote a new primary, you must ensure the old primary can't accept writes if it comes back. The simplest approach is SET GLOBAL super_read_only=ON as part of your health-check when a pod detects it's no longer the designated primary (check the ConfigMap). Belt-and-suspenders: also kill its Service endpoint membership.
Bottom line — this is a 4–8 week project for a solid MVP, not a multi-month odyssey. The narrow scope is exactly what makes it tractable.

----

I've cloned the Percona Orchestrator app (open source) into ./orchestrator and the Bitpoke MySQL Operator into ./bitpoke. Start by exploring those to learn what you can about tricks of async failover. Keep in mind, we do not care at all to support group replication, galera, sync replication, etc. We're trying to simplify as much as possible. Consider the possibility of forking the Bitpoke operator and just removing unwanted functionality and updating it to support the MySQL 9.6 image (it may already?), although I think the Orchestrator layer is unwanted complexity.

----

The boss wants this to be "rock friggin solid", so we need to build some sort of test harness. I have a k3d cluster running already that you can use to test. We'll always have our main pair (primary and secondary) and may occasionally attach a third for analytics or backups but won't have complex topologies that it needs to manage, basically just if the primary goes down, fail over to the secondary.

----

# Architecture Spec

## Findings from Bitpoke & Orchestrator

We explored both codebases to extract patterns worth adopting and complexity worth eliminating.

**Patterns to adopt from Bitpoke:**
- Syncer pattern for declarative resource reconciliation (StatefulSet, Services, ConfigMap, etc.)
- Pod label-based service routing (`role=primary`/`role=replica` labels drive Service endpoint selection)
- Node conditions in CRD status (per-node state: IsPrimary, IsReplicating, IsLagged, IsReadOnly)
- Sidecar for MySQL management (HTTP endpoints for health, status)
- Idempotent initialization via a `configured` flag in a status table
- GTID-based replication with AUTO_POSITION=1
- `super_read_only` fencing to prevent split-brain
- Pod anti-affinity across nodes
- ConfigMap-driven my.cnf with resourceVersion annotation for pod restart on config change
- Processing replicas before primary in pod label sync to prevent dual-primary window

**Complexity to eliminate:**
- **No Orchestrator** — operator polls MySQL directly via sidecar HTTP endpoints
- **Clone plugin instead of xtrabackup** — MySQL 9.6 native, no Percona tools needed
- **No pt-heartbeat** — `SHOW REPLICA STATUS` for lag (sufficient for 2-3 nodes)
- **No pt-kill** — out of scope
- **No cloud backup** — out of scope for MVP
- **No metrics exporter** — out of scope for MVP (easy to add later)
- **Simpler CRD** — fewer fields, focused on our narrow use case
- **MySQL 9.6 syntax** — `CHANGE REPLICATION SOURCE TO`, `SHOW REPLICA STATUS`, `log-replica-updates`, etc.

**Key failover lessons from Orchestrator:**
- Compare GTID_EXECUTED sets to pick the most advanced replica for promotion
- Use recovery grace periods and anti-flap cooldowns to prevent cascading failures
- Fence old primary with super_read_only before promoting new primary
- Check pod/instance freshness before making topology decisions

## Decisions

- **Language**: Go + kubebuilder
- **GTID mode**: `gtid-mode=ON` (strict — requires GTID-compatible SQL from all clients)
- **MySQL image**: Official `mysql:9.6` (not Percona)
- **Topology**: Primary + 1-2 replicas (max 5 nodes, default 2)

## Project Structure

```
mysql-simple-operator/
  api/v1alpha1/
    mysqlcluster_types.go          # CRD type definitions
  cmd/
    operator/main.go               # Operator binary
    sidecar/main.go                # Sidecar binary
  internal/
    controller/
      mysqlcluster_controller.go   # Main reconciler (resource syncing)
      topology.go                  # Topology polling loop (replaces Orchestrator)
      failover.go                  # Failover orchestration
      node_init.go                 # Node initialization (clone, replication setup)
      syncer/
        statefulset.go
        configmap.go
        secret.go
        service_headless.go
        service_primary.go
        service_replicas.go
        pod_labels.go
        certificate.go             # cert-manager Certificate CR
    mysql/
      client.go                    # MySQL connection wrapper
      replication.go               # CHANGE REPLICATION SOURCE, START/STOP REPLICA
      clone.go                     # CLONE INSTANCE logic
      gtid.go                      # GTID set parsing and comparison
    sidecar/
      server.go                    # HTTP health + status endpoints
      init.go                      # Init container logic
      config.go                    # my.cnf generation, server-id
  config/
    crd/                           # Generated CRD YAML
    rbac/                          # RBAC roles
    manager/                       # Operator Deployment
    samples/                       # Example MysqlCluster CRs
  test/
    e2e/                           # k3d-based e2e tests
  Dockerfile                       # Multi-stage: operator + sidecar binaries
  Makefile
```

## CRD: MysqlCluster

```go
type MysqlClusterSpec struct {
    // Replicas: total instances (primary + replicas). Default 2, max 5.
    Replicas           *int32                          `json:"replicas,omitempty"`
    // SecretName: K8s secret with ROOT_PASSWORD, REPLICATION_USER, REPLICATION_PASSWORD, OPERATOR_USER, OPERATOR_PASSWORD
    SecretName         string                          `json:"secretName"`
    // Image: MySQL container image. Default "mysql:9.6"
    Image              string                          `json:"image,omitempty"`
    // SidecarImage: sidecar container image
    SidecarImage       string                          `json:"sidecarImage"`
    // MysqlConf: extra my.cnf [mysqld] key-value overrides
    MysqlConf          map[string]string               `json:"mysqlConf,omitempty"`
    // Resources for the MySQL container
    Resources          corev1.ResourceRequirements      `json:"resources,omitempty"`
    // VolumeClaimTemplate for persistent storage
    VolumeClaimTemplate corev1.PersistentVolumeClaimSpec `json:"volumeClaimTemplate"`
    // MaxReplicaLagSeconds: lag threshold for "lagged" condition. Default 30.
    MaxReplicaLagSeconds *int64                         `json:"maxReplicaLagSeconds,omitempty"`
    // ServerIDOffset: base for server-id assignment. Default 100.
    ServerIDOffset     *int32                          `json:"serverIDOffset,omitempty"`
    // TLS: cert-manager configuration
    TLS                *TLSSpec                        `json:"tls,omitempty"`
    // Scheduling
    Affinity           *corev1.Affinity                `json:"affinity,omitempty"`
    Tolerations        []corev1.Toleration             `json:"tolerations,omitempty"`
    NodeSelector       map[string]string               `json:"nodeSelector,omitempty"`
}

type TLSSpec struct {
    IssuerRef certmanagerv1.ObjectReference `json:"issuerRef"`
}

type MysqlClusterStatus struct {
    ReadyNodes int                `json:"readyNodes,omitempty"`
    Primary    string             `json:"primary,omitempty"`       // FQDN of current primary
    Conditions []ClusterCondition `json:"conditions,omitempty"`
    Nodes      []NodeStatus       `json:"nodes,omitempty"`
}

// Cluster conditions: Ready, FailoverInProgress, ReadOnly
// Node conditions: Primary, Replicating, Lagged, ReadOnly
```

## Pod Architecture

Each pod in the StatefulSet contains:

### Init Container: `init` (sidecar image)
- Writes `/etc/mysql/conf.d/10-identity.cnf` with `server-id` (ordinal + offset) and `report-host` (pod FQDN)
- Copies base `my.cnf` from ConfigMap mount to active config directory
- Does NOT handle cloning — clone plugin requires a running MySQL server

### Container: `mysql` (mysql:9.6)
- Port 3306
- Mounts: `/var/lib/mysql` (data PVC), `/etc/mysql/conf.d` (config), `/etc/mysql-tls` (TLS certs)
- Liveness: `mysqladmin ping` (60s initial delay, 5s timeout)
- Readiness: SQL check for `sys_operator.status WHERE configured=1` (5s initial delay)
- Starts with `--initialize` on first boot (empty data dir)

### Container: `sidecar` (sidecar image)
- Port 8080 (HTTP)
- `GET /health` — 200 OK if MySQL is connectable
- `GET /status` — JSON: `{role, gtid_executed, replica_io_running, replica_sql_running, seconds_behind_source, read_only, super_read_only, uptime, server_id}`
- Also performs startup self-check: if pod's label says `role!=primary` but MySQL has `read_only=OFF`, sets `super_read_only=ON` (fencing safety net)

## Controller Design

### Resource Reconciler (event-driven, via controller-runtime watches)
Triggered by changes to MysqlCluster CR or owned resources. Syncs in order:
1. Secret (operator-generated credentials)
2. ConfigMap (my.cnf generation)
3. cert-manager Certificate CR (if TLS configured)
4. Headless Service
5. Primary Service (selector: `role=primary`)
6. Replicas Service (selector: `role=replica, healthy=yes`)
7. StatefulSet (with `config_rev` and `secret_rev` annotations for restart-on-change)
8. Pod labels (`role`, `healthy`) from `status.nodes[]` conditions — replicas first, then primary

### Topology Manager (5s polling goroutine, replaces Orchestrator)
For each pod, HTTP GET sidecar `/status`. Builds topology state and updates CRD `status.nodes[]`:
- `NodeConditionPrimary`: true if this node is the current primary
- `NodeConditionReplicating`: true if Replica_IO_Running AND Replica_SQL_Running
- `NodeConditionLagged`: true if Seconds_Behind_Source > maxReplicaLagSeconds
- `NodeConditionReadOnly`: true if read_only=ON

Failure detection: 3 consecutive sidecar poll failures (15s) + Kubernetes pod status confirmation → trigger failover.

### Node Initializer (watches Running-but-not-Ready pods)
1. Connect to MySQL via operator credentials
2. Check `sys_operator.status` for `configured=1` → skip if already done
3. **Bootstrap (pod-0, no existing primary)**: disable super_read_only, set read_only=OFF, mark configured, update status as Primary
4. **New replica**: 
   - Set `clone_valid_donor_list` to primary host
   - Run `CLONE INSTANCE FROM 'repl_user'@'primary_host':3306 IDENTIFIED BY 'repl_password' REQUIRE SSL`
   - MySQL auto-restarts after clone completes (container exits, StatefulSet restarts it)
   - On reconnect: `CHANGE REPLICATION SOURCE TO SOURCE_HOST='primary', SOURCE_USER='repl', SOURCE_PASSWORD='...', SOURCE_AUTO_POSITION=1, SOURCE_SSL=1`
   - `START REPLICA`
   - Mark `configured=1`

### Failover Controller
Triggered when topology manager detects primary failure.

**Sequence:**
1. Set `ClusterCondition FailoverInProgress=True` (blocks node initializer)
2. Fence old primary: attempt `SET GLOBAL super_read_only=ON` (fails silently if unreachable — that's fine)
3. Stop replication on all replicas: `STOP REPLICA`, wait for SQL thread to finish applying relay logs
4. Select best candidate: `SELECT @@global.gtid_executed` from each replica, pick the one with the most transactions (superset GTID set)
5. Promote: `RESET REPLICA ALL; SET GLOBAL read_only=OFF; SET GLOBAL super_read_only=OFF`
6. Repoint remaining replicas to new primary: `CHANGE REPLICATION SOURCE TO ...; START REPLICA`
7. Update CRD status (new primary hostname, node conditions)
8. Pod label syncer updates `role=primary` → Primary Service endpoint changes automatically
9. Clear `FailoverInProgress` after 30s grace confirming new primary accepts connections
10. Anti-flap: block additional failovers for 60 minutes

## Split-Brain Prevention

Four layers:
1. **super_read_only fencing**: Old primary set to super_read_only before new primary is promoted. Even if it comes back, it can't accept writes.
2. **Service selector**: Old primary loses `role=primary` label → Primary Service stops routing to it.
3. **Sidecar self-check**: On startup, if pod is not designated primary but MySQL has read_only=OFF, sidecar sets super_read_only=ON.
4. **Conservative partition handling**: If operator can't reach any node, it does NOTHING. Network partitions require human intervention. Consistency over availability (CP).

## MySQL Configuration (generated my.cnf)

```ini
[mysqld]
# Replication
gtid-mode                       = ON
enforce-gtid-consistency        = ON
log-bin                         = /var/lib/mysql/mysql-bin
log-replica-updates             = ON
skip-replica-start              = ON
binlog-format                   = ROW
sync-binlog                     = 1
binlog-expire-logs-seconds      = 1209600

# Clone plugin
plugin-load-add                 = mysql_clone.so

# SSL
require-secure-transport        = ON
ssl-ca                          = /etc/mysql-tls/ca.crt
ssl-cert                        = /etc/mysql-tls/tls.crt
ssl-key                         = /etc/mysql-tls/tls.key

# InnoDB
default-storage-engine          = InnoDB
innodb-flush-method             = O_DIRECT
innodb-flush-log-at-trx-commit  = 2
innodb-file-per-table           = 1

# Safety
max-allowed-packet              = 64M
max-connect-errors              = 1000000
skip-name-resolve
skip-host-cache

# Connections
max-connections                 = 500
thread-cache-size               = 50

# Character set
character-set-server            = utf8mb4
collation-server                = utf8mb4_unicode_ci
```

User-provided `spec.mysqlConf` overrides these defaults.

## SSL/TLS

The operator creates a cert-manager `Certificate` CR per cluster:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {cluster}-mysql-tls
spec:
  secretName: {cluster}-mysql-tls
  issuerRef: {from spec.tls.issuerRef}
  dnsNames:
    - "*.{cluster}-mysql.{namespace}.svc.cluster.local"
    - "{cluster}-mysql-primary.{namespace}.svc.cluster.local"
    - "{cluster}-mysql-replicas.{namespace}.svc.cluster.local"
  usages: [server auth, client auth]
```

All pods share the same cert (wildcard SAN). Mounted at `/etc/mysql-tls/`. Replication uses `SOURCE_SSL=1`. Cert rotation triggers rolling restart via `resourceVersion` annotation on the StatefulSet pod template.

## Services

| Service | Type | Selector | Purpose |
|---------|------|----------|---------|
| `{cluster}-mysql` (headless) | ClusterIP: None | `app=mysql, cluster={name}` | StatefulSet DNS, pod-to-pod communication |
| `{cluster}-mysql-primary` | ClusterIP | `cluster={name}, role=primary` | Application writes |
| `{cluster}-mysql-replicas` | ClusterIP | `cluster={name}, role=replica, healthy=yes` | Application reads |

Headless service uses `publishNotReadyAddresses: true` so pods are reachable during initialization (for clone donor).

## Key Risks & Mitigations

1. **Clone plugin restart**: When `CLONE INSTANCE` completes, MySQL auto-restarts → container exits → StatefulSet restarts it. The node initializer must detect this (clone completed vs crash) by checking if the data directory now contains donor data. After restart, it reconnects and sets up replication.

2. **Single operator instance**: Runs as a Deployment with leader election. During operator downtime, MySQL keeps running but no automated failover occurs. Acceptable for narrow scope.

3. **Network partitions**: Operator does nothing if it can't reach nodes. Human intervention required. This is deliberate — automated action during partitions risks split-brain.

4. **Errant transactions**: If a replica has transactions not on the primary, replication to/from it will fail. Operator detects via `SHOW REPLICA STATUS` errors and surfaces in CRD status. No auto-fix — human decision required.
