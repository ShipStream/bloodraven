# Bloodraven

MySQL operator + platform watcher, combined into one controller.

## Why combine them

The watcher and operator both need to detect MySQL state and act on failover. If they're
separate processes, they create a coordination problem: who's allowed to promote? What if they
disagree about topology? What if the operator promotes but the watcher hasn't noticed yet?

A single controller eliminates this. It owns the full lifecycle — pod creation, bootstrap,
replication, health checking, failover, AND the platform reactions (taints, DNS, Device Hub
websocket). One process, one source of truth, one decision-maker.

## Infrastructure assumptions

- **Non-HA control plane.** The k8s master node is a single node, not in either DC. It will go
  down occasionally (maintenance, hardware) for minutes to hours. The MySQL pair must tolerate
  this gracefully — hold state, keep replicating, keep serving traffic. Bloodraven being down
  is normal, not an emergency.
- **Two DCs** per AZ, each with one or more worker nodes.
- **One MySQL instance per DC**, forming an async replication pair.
- **Bloodraven runs on the master node** alongside the k8s control plane. If the master is
  down, Bloodraven is down. The system is designed for this.

```
Master node (control plane)         DC1 nodes              DC2 nodes
┌──────────────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  k8s API server          │   │  mysql-lion-dc1   │   │  mysql-lion-dc2   │
│  etcd                    │   │  (Pod + sidecar)  │   │  (Pod + sidecar)  │
│  Bloodraven (Deployment) │   │  PVC: data        │   │  PVC: data        │
│                          │   │                   │   │                   │
│  - topology manager      │◄─►│  sidecar :8080    │   │  sidecar :8080    │
│  - platform reactor      │   │  mysql  :3306     │   │  mysql  :3306     │
│  - WS hub                │   └──────────────────┘   └──────────────────┘
└──────────────────────────┘         ▲     ▲                 ▲     ▲
                                     │     └─────────────────┘     │
                                     │   sidecar-to-sidecar peer   │
                                     │        health check         │
                                     └─────────────────────────────┘
```

## What exists today

The `mysql-watcher` service (implemented, tested, working) handles:

- Parallel polling of two MySQL instances (one per DC) via `SELECT @@read_only`
- Per-DC state machine: writable / read-only / unreachable
- Debounced transitions (failure threshold, recovery threshold)
- Combined state matrix evaluation (9 cases including split brain, double-readonly, total loss)
- Node taints (`shipstream.io/db-readonly=true:NoExecute`) by zone
- Device Hub websocket broadcast (`/ws/status` — JSON `{dc, status}`)
- Cloudflare DNS flip (`{az}.az.shipstream.app` A record)
- MySQL promotion (`STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only=0`)
- Promotion cooldown (DNS deferred until `read_only=0` confirmed)
- Prometheus metrics, health/readiness probes, status endpoint

Code lives in `services/mysql-watcher/`. ~900 lines of Go, 22 tests.

## What Bloodraven adds

The operator half manages MySQL pod lifecycle via a CRD and adds sidecar-based split-brain
protection. This is new code that absorbs and extends the watcher.

### CRD: MysqlReplicaPair

The topology is always a 2-node async pair (one per DC), optionally with a third read replica.
This is not a generic "N replicas" operator — it's purpose-built for our cross-DC model.

```yaml
apiVersion: shipstream.io/v1alpha1
kind: MysqlReplicaPair
metadata:
  name: lion
  namespace: shared-lion
spec:
  image: mysql:9.6
  sidecarImage: ghcr.io/shipstream/bloodraven-sidecar:latest

  # Per-DC instance configuration
  dc1:
    name: dc1
    zone: lion-dc1                     # topology.kubernetes.io/zone value
    lbIP: 203.0.113.1                  # for DNS flip
    storage:
      storageClassName: local-dc1
      size: 100Gi
    resources:
      requests: { cpu: "2", memory: 8Gi }

  dc2:
    name: dc2
    zone: lion-dc2
    lbIP: 203.0.113.2
    storage:
      storageClassName: local-dc2
      size: 100Gi
    resources:
      requests: { cpu: "2", memory: 8Gi }

  # Credentials
  secretName: mysql-credentials
  # Secret keys: ROOT_PASSWORD, REPLICATION_USER, REPLICATION_PASSWORD,
  #              OPERATOR_USER, OPERATOR_PASSWORD

  # TLS
  tls:
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer

  # Platform integration
  az: lion
  cloudflare:
    apiTokenSecretRef:
      name: cloudflare-credentials
      key: api-token
    zoneID: abc123

  # Tuning
  pollInterval: 2s
  failureThreshold: 3
  recoveryThreshold: 2
  failoverCooldown: 60m

  # Sidecar self-fencing
  sidecar:
    leaseTimeout: 20s           # how long before sidecar considers Bloodraven "gone"
    peerCheckInterval: 5s       # how often sidecar pings the other instance

  # MySQL config overrides (merged into generated my.cnf)
  mysqlConf:
    max-connections: "500"
    innodb-buffer-pool-size: "4G"

status:
  primaryDC: dc1
  dc1:
    state: writable         # writable | read-only | unreachable | unknown
    lastSeen: "2026-04-07T12:00:00Z"
    gtidExecuted: "uuid:1-45839"
    replicating: false
    secondsBehindSource: 0
  dc2:
    state: read-only
    lastSeen: "2026-04-07T12:00:00Z"
    gtidExecuted: "uuid:1-45837"
    replicating: true
    secondsBehindSource: 2
  conditions:
    - type: Ready
      status: "True"
    - type: FailoverInProgress
      status: "False"
  lastFailover: "2026-03-15T08:30:00Z"
  lastFailoverTarget: dc1
  websocketClients: 4
```

### What Bloodraven creates per DC

Bloodraven creates and owns all MySQL resources. When a MysqlReplicaPair CR is created,
the reconciler syncs these resources per DC:

```
Deployment:  mysql-{pair}-{dc}        (replicas: 1, nodeSelector: zone={az}-{dc})
  Pod:
    initContainer: init               (sidecar image — writes my.cnf, server-id)
    container: mysql                  (mysql:9.6, port 3306)
    container: sidecar                (sidecar image, port 8080)
PVC:     mysql-{pair}-{dc}-data       (per-DC storageClass)
Service: mysql-{pair}-{dc}            (ClusterIP, port 3306 — stable DNS for this instance)
```

Shared across both DCs:

```
Service: mysql-{pair}-primary         (selector: role=primary, port 3306)
Service: mysql-{pair}-replicas        (selector: role=replica, healthy=yes, port 3306)
ConfigMap: mysql-{pair}-config        (generated my.cnf)
Certificate: mysql-{pair}-tls         (cert-manager, wildcard SAN)
```

Not a StatefulSet — a separate Deployment (replicas: 1) per DC. Each DC has its own storage
class, scheduling constraints, and potentially different hardware. The two pods have different
roles and are not interchangeable.

### What the controller does

Three concerns, one process:

**1. Topology Manager** (polling loop — from watcher, extended)

The existing watcher poll loop becomes the topology manager. It already handles parallel polling,
debounce, and the state matrix. New additions:

- Poll via sidecar HTTP `/status` instead of direct MySQL (richer data: GTID, lag, uptime)
- Fall back to direct `SELECT @@read_only` if sidecar unreachable (graceful degradation)
- Write topology state to CRD `status` on every cycle
- Emit Kubernetes events on state transitions ("dc1 became unreachable", "failover initiated")

What carries over unchanged from the watcher:
- `DCState` enum and state machine
- `EvalPerDCTransition()` per-DC action logic
- `EvalCrossDC()` combined state matrix (all 9 cases)
- `computeState()` debounce logic (failure/recovery thresholds)
- Promotion cooldown (DNS deferred until confirmation)

**2. Platform Reactor** (from watcher, unchanged)

These components move into Bloodraven with minimal changes:

- **Node taints**: `SetTaint()` applies/removes `shipstream.io/db-readonly=true:NoExecute`
  by zone label. Uses strategic merge patch. Accumulates errors across nodes.
- **Websocket hub**: `/ws/status` broadcasts `{dc, status}` to Device Hubs.
  Same protocol, same fail-safe (disconnect = offline).
- **DNS flip**: Cloudflare A record update. Same API, same deferred-until-confirmed behavior.
- **Prometheus metrics**: poll latency, state transitions, taint ops, WS clients, DNS flips.

**3. Lifecycle Manager** (new)

New code that handles pod creation, bootstrap, and recovery:

- **Pod creation**: Reconciler creates Deployments, PVCs, Services, ConfigMap, and Certificate
  CR per the resource list above. Pod labels (`role=primary`/`role=replica`, `healthy=yes`/`no`)
  are synced from CRD status — replicas relabeled before primary to prevent dual-primary window
  in Service endpoints.

- **Bootstrap**: When a new MysqlReplicaPair is created and the secondary has no data,
  Bloodraven orchestrates clone plugin seeding:
  1. Verify primary is writable and healthy
  2. On secondary: `SET GLOBAL clone_valid_donor_list = '{primary_host}:3306'`
  3. `CLONE INSTANCE FROM '{repl_user}'@'{primary_host}':3306 IDENTIFIED BY '...' REQUIRE SSL`
  4. MySQL auto-restarts after clone. Bloodraven detects restart via poll failure + recovery.
  5. After restart: `CHANGE REPLICATION SOURCE TO SOURCE_HOST='{primary}', SOURCE_AUTO_POSITION=1, SOURCE_SSL=1, ...`
  6. `START REPLICA`
  7. Update CRD status

- **Failover** (extends watcher promotion):
  The watcher already does `STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only=0`.
  Bloodraven adds:
  1. Set CRD condition `FailoverInProgress=True`
  2. Attempt to fence old primary: `SET GLOBAL super_read_only=ON` (fail silently if unreachable)
  3. Check `SHOW REPLICA STATUS` on candidate — wait for SQL thread to finish relay logs
  4. Existing promotion sequence (STOP REPLICA, RESET REPLICA ALL, SET GLOBAL read_only=0)
  5. Platform actions (taints, WS broadcast) — already handled by per-DC transition logic
  6. DNS flip deferred until promotion confirmed — already implemented
  7. Update CRD status (new primaryDC), relabel pods
  8. Anti-flap: block additional failovers for `failoverCooldown` duration
  9. Clear `FailoverInProgress` after grace period

- **Old primary recovery** (new):
  When a previously-unreachable primary comes back:
  1. Immediately fence: `SET GLOBAL super_read_only=ON`
  2. Reconfigure as replica of new primary:
     `CHANGE REPLICATION SOURCE TO SOURCE_HOST='{new_primary}', SOURCE_AUTO_POSITION=1, ...`
  3. `START REPLICA`
  4. Wait for it to catch up (poll `Seconds_Behind_Source`)
  5. Once caught up, per-DC state transitions to read-only (taint stays, which is correct)
  6. CRD status updated

### Sidecar

A small HTTP server that runs alongside each MySQL instance. Gives Bloodraven richer
observability than a raw `SELECT @@read_only`, and provides autonomous split-brain protection
when Bloodraven is down.

**HTTP endpoints** (consumed by Bloodraven):

```
GET /health  -> 200 if MySQL is connectable, 503 if not
GET /status  -> JSON:
{
  "role": "primary",
  "read_only": false,
  "super_read_only": false,
  "gtid_executed": "uuid:1-45839",
  "replica_io_running": false,
  "replica_sql_running": false,
  "seconds_behind_source": null,
  "server_id": 101,
  "uptime": 86400
}
```

**Peer health endpoint** (consumed by the other sidecar):

```
GET /peer/ping  -> 200 OK (proves this instance is reachable)
```

**Sidecar self-fencing** (autonomous, no Bloodraven required):

See the dedicated section below.

**Startup safety net**: On startup, if the sidecar sees `read_only=OFF` but the CRD says this
instance should not be primary, it sets `super_read_only=ON`. Belt-and-suspenders against
split brain after a crash.

The sidecar is optional for initial deployment — Bloodraven falls back to direct MySQL queries
if the sidecar isn't running. This lets us roll out incrementally. Self-fencing obviously
requires the sidecar.

## Sidecar self-fencing

The k8s master node (and therefore Bloodraven) is non-HA. It may be down for hours during
maintenance. During this time, the MySQL pair must hold steady — keep replicating, keep serving
traffic, don't change anything. Self-fencing must NOT trigger just because Bloodraven is offline.

Self-fencing exists for one specific scenario: **the primary is network-partitioned from
everything else.** If the primary can't reach Bloodraven AND can't reach the other MySQL
instance, it's the one that's isolated. From everyone else's perspective, the primary has
disappeared — and Bloodraven (if it's running) will promote the replica. The partitioned primary
must fence itself to prevent split-brain writes.

### Decision matrix

Each sidecar continuously monitors two things:
- **Bloodraven**: HTTP poll or websocket heartbeat to Bloodraven's health endpoint
- **Peer**: HTTP GET to the other sidecar's `/peer/ping`

The self-fencing rule applies **only to the primary** sidecar:

| Bloodraven | Peer (other MySQL) | Action |
|------------|-------------------|--------|
| reachable  | reachable         | Normal. Bloodraven is in control. |
| reachable  | unreachable       | Normal. Bloodraven sees this and handles it. |
| unreachable | reachable        | **Hold steady.** Master node is down. Topology is fine — both MySQL instances can see each other. Do nothing, wait for Bloodraven to come back. |
| unreachable | unreachable      | **Self-fence.** I'm isolated. `SET GLOBAL super_read_only=ON`. |

The replica sidecar never needs to self-fence — it's already read-only. It just keeps
replicating (or not, if it can't reach the primary) and waits.

### Timing

- Sidecar polls Bloodraven and peer every `peerCheckInterval` (default 5s).
- Self-fence triggers after `leaseTimeout` (default 20s) of both being unreachable.
  This is ~4 consecutive failed checks.
- The lease timeout must be **longer** than Bloodraven's failure threshold (3 polls x 2s = 6s)
  to give Bloodraven first shot at handling a real failure. If Bloodraven can reach the primary,
  it fences it remotely. The sidecar self-fence is the fallback for when Bloodraven can't.
- After self-fencing, the sidecar logs loudly and keeps checking. If Bloodraven or the peer
  become reachable again, the sidecar does NOT un-fence — only Bloodraven can restore a
  primary. This prevents oscillation.

### Why not Raft

Raft solves "N nodes agree on who the leader is." Our problem is "is this MySQL truly down, or
am I just partitioned from it?" — a different question. Raft with 3 voters (Bloodraven + two
sidecars) adds complexity without improving the answer:

- If DC1 is partitioned from both Bloodraven and DC2, that's 2/3 saying "DC1 is down" — same
  conclusion Bloodraven already reaches alone.
- Raft requires leader election, log replication, and persistent state across DCs. The sidecar
  peer-check achieves the same safety property with a simple HTTP ping.
- The fundamental problem Raft doesn't solve: even with consensus that DC1 is down, you can't
  fence it if you can't reach it. The sidecar solves this by self-fencing locally.

## Split-brain prevention

Five layers:

1. **Bloodraven remote fencing**: Before promoting a replica, Bloodraven attempts
   `SET GLOBAL super_read_only=ON` on the old primary. If unreachable, layers 2-5 cover it.
2. **Sidecar self-fencing**: Primary self-fences when it can reach neither Bloodraven nor the
   other MySQL instance (the partition scenario). See decision matrix above.
3. **Sidecar startup safety net**: On startup, if the sidecar sees `read_only=OFF` but the
   CRD says this instance should not be primary, it sets `super_read_only=ON`.
4. **Service routing**: Old primary loses `role=primary` pod label. The primary Service stops
   routing traffic to it.
5. **Conservative partitions**: If Bloodraven can't reach either MySQL, it does NOTHING for
   promotion. Only taints + alerts. Human intervention required. CP over AP.

### The partition timeline

When DC1 (primary) becomes partitioned from the master node and DC2:

```
T=0s    DC1 partitioned. Bloodraven and DC2 lose contact with DC1.
        DC1's sidecar loses contact with Bloodraven and DC2's sidecar.

T=6s    Bloodraven hits failure threshold (3 polls). DC1 -> unreachable.
        Bloodraven tries to fence DC1: fails (unreachable). Proceeds anyway.
        Bloodraven promotes DC2: STOP REPLICA, RESET REPLICA ALL, SET read_only=0.

T=8s    Bloodraven confirms DC2 writable. Flips DNS. Updates taints. Broadcasts WS.

T=20s   DC1's sidecar hits lease timeout. Both Bloodraven and peer unreachable.
        Self-fences: SET GLOBAL super_read_only=ON.
        Split-brain window: T=6s to T=20s (~14 seconds).
        During this window, DC1 can still accept writes from clients that
        route directly to it (not via DNS). In practice this is limited to
        connections that were already established before the partition.

T=???   Partition heals. DC1 becomes reachable again.
        Bloodraven detects DC1 is back. Fences it (already fenced by sidecar).
        Reconfigures as replica of DC2. Starts replication. Catches up.
```

The 14-second window is the worst case for split-brain writes. Mitigation: web pods check
`@@read_only` on every request (existing behavior), so even during this window, most writes
are blocked at the application layer. Only raw MySQL connections that bypass the read_only
check could write during the gap.

## Project structure

```
services/bloodraven/
  cmd/
    bloodraven/main.go          # Operator binary (controller-runtime)
    sidecar/main.go             # Sidecar binary
  api/v1alpha1/
    types.go                    # MysqlReplicaPair CRD types
    groupversion_info.go
    zz_generated.deepcopy.go
  internal/
    controller/
      reconciler.go             # Main reconciler (CRD -> desired state, pod creation)
      topology.go               # Polling loop (from watcher.go, extended)
      failover.go               # Failover orchestration (new)
      recovery.go               # Old primary recovery (new)
      bootstrap.go              # Clone plugin seeding (new)
    platform/
      tainter.go                # Node taints (from watcher, unchanged)
      dns.go                    # Cloudflare DNS (from watcher, unchanged)
      websocket.go              # Device Hub WS hub (from watcher, unchanged)
    mysql/
      checker.go                # MySQL polling (from watcher mysql.go, extended)
      replication.go            # CHANGE REPLICATION SOURCE, START/STOP REPLICA
      clone.go                  # CLONE INSTANCE
      gtid.go                   # GTID set parsing and comparison
    state/
      machine.go                # DCState, transitions (from watcher state.go, unchanged)
      matrix.go                 # Cross-DC evaluation (from watcher state.go, unchanged)
    metrics/
      metrics.go                # Prometheus metrics (from watcher, extended)
    sidecar/
      server.go                 # Sidecar HTTP server + self-fencing loop
  config/
    crd/                        # Generated CRD YAML
    rbac/                       # ClusterRole (nodes + CRD + pods)
    manager/                    # Operator Deployment manifest
    samples/                    # Example MysqlReplicaPair CRs
  test/
    e2e/                        # k3d-based integration tests
  Dockerfile                    # Multi-stage: bloodraven + sidecar binaries
  Makefile
  go.mod
```

## What moves from the watcher, unchanged

| Watcher file       | Bloodraven location                  | Changes          |
|--------------------|--------------------------------------|------------------|
| `state.go`         | `internal/state/machine.go`          | None             |
| `state.go` (cross) | `internal/state/matrix.go`           | None             |
| `tainter.go`       | `internal/platform/tainter.go`       | None             |
| `websocket.go`     | `internal/platform/websocket.go`     | None             |
| `dns.go`           | `internal/platform/dns.go`           | None             |
| `metrics.go`       | `internal/metrics/metrics.go`        | Add new metrics  |
| `*_test.go`        | Corresponding test files             | Same assertions  |

## What changes from the watcher

| Watcher file     | Bloodraven location                  | What changes                              |
|------------------|--------------------------------------|-------------------------------------------|
| `watcher.go`     | `internal/controller/topology.go`    | Poll via sidecar, write CRD status, emit events |
| `mysql.go`       | `internal/mysql/checker.go`          | Add sidecar HTTP client, richer status    |
| `config.go`      | `api/v1alpha1/types.go`              | CRD spec replaces env vars               |
| `main.go`        | `cmd/bloodraven/main.go`             | controller-runtime manager, leader election |

## What's new

| File                                     | Purpose                              |
|------------------------------------------|--------------------------------------|
| `api/v1alpha1/types.go`                  | MysqlReplicaPair CRD                 |
| `internal/controller/reconciler.go`      | CRD reconciler (pod/svc/cm creation) |
| `internal/controller/failover.go`        | Failover orchestration + fencing     |
| `internal/controller/recovery.go`        | Old primary reconfiguration          |
| `internal/controller/bootstrap.go`       | Clone plugin seeding                 |
| `internal/mysql/replication.go`          | Replication SQL commands             |
| `internal/mysql/clone.go`               | Clone plugin commands                |
| `internal/mysql/gtid.go`                | GTID comparison (from Orchestrator patterns) |
| `internal/sidecar/server.go`            | Sidecar HTTP + self-fencing          |
| `test/e2e/`                              | k3d integration tests                |

## RBAC

Bloodraven needs broader permissions than the watcher since it creates pods:

```yaml
# Cluster-scoped
- nodes: get, list, patch                              # Taints
- mysqlreplicapairs: get, list, watch, update, patch   # CRD
- mysqlreplicapairs/status: update, patch              # CRD status

# Namespace-scoped (shared-{az})
- deployments: get, list, watch, create, update, patch, delete
- services: get, list, watch, create, update, patch, delete
- configmaps: get, list, watch, create, update, patch, delete
- persistentvolumeclaims: get, list, watch, create, update, patch
- secrets: get, list, watch
- pods: get, list, watch, patch                        # label updates
- events: create, patch
- certificates.cert-manager.io: get, list, watch, create, update, patch
```

## Deployment model

- Single-replica Deployment on master/control-plane node
- Runs in `shared-{az}` namespace
- `nodeSelector: node-role.kubernetes.io/control-plane` + appropriate tolerations
- Leader election via controller-runtime (safe during rollout overlap)
- **Non-HA by design.** When the master node is down, Bloodraven is down. This is expected.
  The system is safe: taints persist, web pods self-detect read_only, Device Hubs go offline
  on WS disconnect, sidecars self-fence if truly partitioned. The system cannot *recover* or
  *failover* until the master comes back, but it won't make things worse.

## Rollout plan

### Phase 1: Rename + restructure

Move watcher code into the Bloodraven project layout. No new functionality — just reorganize
files into the package structure above. All existing tests pass in new locations. Deploy as
the watcher replacement (same behavior, new binary name).

### Phase 2: CRD + reconciler + pod creation

Add the MysqlReplicaPair CRD. Reconciler creates Deployments, PVCs, Services, ConfigMap per DC.
The topology manager reads config from the CRD spec instead of env vars. CRD status gets
populated on every poll. Platform actions (taints, DNS, WS) still work exactly as before.

### Phase 3: Sidecar

Deploy the sidecar alongside each MySQL instance. Topology manager polls sidecar `/status` for
GTID, lag, and replication state. Falls back to direct MySQL if sidecar unavailable. CRD status
gets richer fields (gtidExecuted, secondsBehindSource, replicating).

### Phase 4: Sidecar self-fencing

Add peer health check and the self-fencing decision matrix. Sidecar monitors both Bloodraven
and the peer sidecar. Self-fences only when truly isolated (both unreachable). Test thoroughly
with simulated partitions on k3d.

### Phase 5: Failover hardening

Add relay log drain before promotion, GTID comparison for candidate selection (matters if we
ever have 3 nodes), anti-flap cooldown, FailoverInProgress condition, and old-primary-recovery
logic (fence, reconfigure as replica, start replication, catch up).

### Phase 6: Bootstrap

Add clone plugin seeding for new/replacement replicas. This is the last piece — creating a
new pair from scratch or replacing a failed instance.

### Phase 7: E2E test harness

k3d-based integration tests that exercise:
- Normal operation (primary writable, replica read-only, replication healthy)
- Primary failure -> failover -> DNS flip -> old primary recovery
- Master node down -> both MySQL instances hold steady -> master returns
- Network partition -> sidecar self-fencing -> partition heals -> recovery
- Split brain detection -> alert, no automated action
- Debounce (single blip doesn't trigger failover)
- Promotion cooldown (DNS waits for confirmation)
- Clone plugin bootstrap of a fresh replica
- Full chaos sequence (multiple failures in succession)

## MySQL configuration

Generated `my.cnf` with GTID, clone plugin, SSL, InnoDB tuning. User overrides via
`spec.mysqlConf`.

```ini
[mysqld]
gtid-mode                       = ON
enforce-gtid-consistency        = ON
log-bin                         = /var/lib/mysql/mysql-bin
log-replica-updates             = ON
skip-replica-start              = ON
binlog-format                   = ROW
sync-binlog                     = 1
binlog-expire-logs-seconds      = 1209600
plugin-load-add                 = mysql_clone.so
require-secure-transport        = ON
ssl-ca                          = /etc/mysql-tls/ca.crt
ssl-cert                        = /etc/mysql-tls/tls.crt
ssl-key                         = /etc/mysql-tls/tls.key
```

## Open questions

1. **Third replica.** The spec mentions "occasionally attach a third for analytics or backups."
   The state matrix is currently hard-coded for 2 DCs. Supporting a third read replica that
   doesn't participate in failover (never promoted, no DNS, no taints) is straightforward —
   it's just another instance to monitor for lag and replication health. No state matrix changes.
   Could be modeled as a list in the CRD (`spec.extraReplicas[]`) or a separate lightweight CR.

2. **Backups.** Out of scope for MVP. When added, it's a CronJob that runs `mysqldump` or
   `mysqlsh dump` against the most-caught-up replica. Bloodraven's CRD status already exposes
   lag per instance, so the backup job can pick the right target.

3. **Sidecar peer address discovery.** Each sidecar needs the other's address. Options:
   (a) Injected as env var by Bloodraven when creating the Deployment, derived from the CRD
   spec. Simplest. (b) Sidecar reads the CRD directly. Adds RBAC complexity.
   Leaning toward (a).
