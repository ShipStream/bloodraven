# Bloodraven Chaos Testing Scenarios

## Prerequisites

1. Create k3d cluster: `k3d cluster create bloodraven --agents 2 --k3s-arg '--tls-san=<hostname>@server:0'`
2. Run playground setup: `./playground/setup.sh`
3. Verify healthy state: `kubectl -n bloodraven-playground get mysqlfailovergroups -o wide` shows one writable, one read-only

Key playground config: `pollInterval=2s`, `failureThreshold=3` (~6s detection), `failoverCooldown=30s`, sidecar `leaseTimeout=20s`, `peerCheckInterval=5s`.

## Tooling Notes

- **`kill-site` vs `scale --replicas=0`**: `kill-site` deletes the pod but Deployment recreates it in <5s. Use `scale --replicas=0` when you need a site to stay down (self-fencing, anti-flap, sustained outage tests).
- **Network partition**: `chaos.sh network-partition` uses a NetworkPolicy deny-all. This blocks at the pod network level (not host netns). Cleanup: `chaos.sh recover`.
- **Relay log drain**: Failover takes ~37s total when primary is dead (30s drain timeout + detection + promotion). Plan wait times accordingly.
- **Replication user**: Survives pod restarts but not PVC wipes. After `reset-mysql.sh` the init-users script recreates it on MySQL first boot.
- **Verify replication between scenarios**: After any failover, always verify replication is actually working (`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status` should show `ON`) before proceeding. GTID divergence from the respawn race (see scenario 1 note) can silently break replication, causing false results in subsequent scenarios.
- **db-readonly taint blocks PVC provisioning**: The operator applies `shipstream.io/db-readonly:NoExecute` to non-writable nodes. The local-path-provisioner's helper pod does not tolerate this taint and gets evicted, blocking PVC creation. After `reset-mysql.sh`, always check for and clear this taint on both nodes: `kubectl taint nodes k3d-bloodraven-agent-0 shipstream.io/db-readonly- 2>/dev/null; kubectl taint nodes k3d-bloodraven-agent-1 shipstream.io/db-readonly- 2>/dev/null`
- **Distroless containers**: The sidecar and operator images use distroless base images with no shell or userspace tools. Use `kubectl debug --target=<container> --image=busybox` to get a shell with tools like `kill`, `ps`, etc.
- **JSON patch vs merge patch**: When patching the MysqlFailoverGroup CR, always use `--type json` (JSON Patch). Merge patches on the `spec.sites` array drop required fields (lbIP, storage, zone) and fail validation.

---

## Scenarios 1-10: Core Failure Modes

### 1. Clean Primary Failure
**Category**: Basic failover | **Risk**: Low

**Hypothesis**: Kill primary pod -> operator detects in ~6s -> promotes replica -> old primary rejoins as replica after respawn.

**Injection**: `./playground/chaos.sh kill-site iad`

**Verify**: activeSite flips, new primary read_only=0, DNS updated, old primary auto-recovers as replica (IO=Yes, SQL=Yes). Counter data preserved.

**Timing**: ~37s to failover complete (30s relay drain timeout on dead primary). Recovery adds ~5s after old primary pod respawns.

**Caution — GTID divergence race**: When the old primary pod respawns, MySQL commits 1-3 internal transactions (system table updates, auto-increment counters) before the operator can fence it. This may create GTID divergence that blocks auto-recovery, requiring `reset-mysql.sh`. Whether divergence occurs is timing-dependent — it doesn't happen every time. If recovery logs "GTID divergence detected" instead of "no GTID divergence, auto-recovering", the scenario was affected.

---

### 2. Operator Kill and Restart
**Category**: Operator resilience | **Risk**: Low

**Hypothesis**: Kill operator while healthy -> restarts within 20s leaseTimeout -> no spurious failover, no self-fencing.

**Injection**: `./playground/chaos.sh kill-operator`

**Verify**: activeSite unchanged, both MySQL sites unchanged, zero "SELF-FENC" in sidecar logs, operator logs show clean startup.

---

### 3. Network Partition of Primary
**Category**: Network partition | **Risk**: Medium

**Hypothesis**: NetworkPolicy blocks all traffic to primary pod -> operator can't reach it -> promotes replica -> primary sidecar self-fences at ~T+20s (both operator and peer unreachable beyond leaseTimeout).

**Injection**: `./playground/chaos.sh network-partition <active-site>`

**Verify**: Replica promoted. Primary sidecar logs "SELF-FENCING" + "SELF-FENCED", primary super_read_only=1 (verified via `kubectl exec` which bypasses NetworkPolicy). Cleanup: `./playground/chaos.sh recover`.

**Note**: `kubectl exec` uses the API server, not the pod network, so you can still query MySQL on the partitioned pod.

**Note**: After partition removal, recovery may log transient errors ("invalid connection", "connection refused") as the old primary's MySQL connections re-establish. These retry and succeed — don't be alarmed by 1-2 failed recovery attempts in the logs.

---

### 4. Data Integrity Under Failover
**Category**: Data integrity | **Risk**: Low

**Hypothesis**: Write known counter value -> kill primary -> no committed writes lost on new primary.

**Injection**:
```bash
# Write test data
kubectl -n bloodraven-playground exec deploy/mysql-playground-<primary> -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "CREATE DATABASE IF NOT EXISTS chaos_test; ..."
# Record value, kill primary, check value on new primary
```

**Verify**: Post-failover counter >= pre-failover counter. Compare GTID sets to quantify async replication gap (typically 0 transactions lost when replica is caught up).

**Critical**: Before running this scenario, verify replication is actually running on the replica (`SELECT SERVICE_STATE FROM performance_schema.replication_connection_status` → `ON`). If a previous scenario caused GTID divergence and recovery was blocked, the replica won't be replicating and writes on the primary won't propagate — giving a false data-loss result.

---

### 5. Operator Kill During Active Failover
**Category**: Operator resilience (advanced) | **Risk**: Medium

**Hypothesis**: Kill primary + kill operator 1s later (mid-failover). Restarted operator converges to exactly one writable site.

**Injection**: `./playground/chaos.sh kill-site iad && sleep 1 && ./playground/chaos.sh kill-operator`

**Verify**: After ~45s convergence, exactly one site has read_only=0. No split-brain.

**Observation**: In practice, the primary pod often respawns before the new operator finishes starting. The new operator discovers the original topology already restored (iad=writable, pdx=read-only) and resumes normal monitoring without triggering a failover. Replication may be in CONNECTING state briefly while the IO thread reconnects to the restarted primary's new pod IP via the Service DNS.

---

### 6. Self-Fencing Validation
**Category**: Split-brain prevention | **Risk**: Medium

**Hypothesis**: Fully isolate primary (scale operator=0, scale replica=0). Sidecar self-fences at ~T+20s.

**Injection**:
```bash
NS=bloodraven-playground
kubectl -n $NS scale deployment bloodraven --replicas=0
kubectl -n $NS scale deployment mysql-playground-pdx --replicas=0
# Wait 25s
```

**Verify**: Primary super_read_only=1, sidecar logs "SELF-FENCED", writes fail. Cleanup: scale both back to 1.

**Important**: Must use `scale --replicas=0`, not `kill-site`. Deployment recreates killed pods in <5s, which refreshes the sidecar's peer timer and prevents self-fencing.

**After cleanup**: When the operator and replica come back, the operator will typically promote the OTHER site (not the self-fenced one) as primary. Expect the active site to flip. The self-fenced site becomes a replica.

---

### 7. Recovery/Rejoin After Failover (No Divergence)
**Category**: Recovery | **Risk**: Low

**Hypothesis**: After failover, old primary returns read-only with no replication. Operator compares GTIDs, finds no divergence, auto-recovers as replica.

**Injection**: Run scenario 1, then verify recovery happened. To re-test: `STOP REPLICA; RESET REPLICA ALL;` on old primary to re-trigger recovery.

**Verify**: Old primary shows Replica_IO_Running=Yes, Source_Host=<new-primary>. Data replicates. No RecoveryBlocked condition.

**Note**: This also works across operator restarts now that `lastFailoverTarget` is persisted in CR status.

---

### 8. GTID Divergence Detection
**Category**: Data integrity / split-brain aftermath | **Risk**: Medium

**Hypothesis**: Inject rogue writes on old primary -> operator detects divergence -> blocks recovery.

**Injection**: With new primary active and old primary not replicating:
```bash
kubectl exec ... -e "SET GLOBAL read_only=OFF; INSERT INTO divergence_test.rogue VALUES (1); SET GLOBAL read_only=ON;"
```

**Verify**: Status shows RecoveryPending with reason DivergentTransactions, operator logs "GTID divergence", exact divergent GTID set reported. Cleanup: `./playground/reset-mysql.sh`.

**Important**: `reset-mysql.sh` is **required** after this scenario — divergent GTID state cannot be auto-recovered. Clear db-readonly taints on both nodes after reset if PVCs fail to provision.

---

### 9. Rapid Successive Failures (Anti-Flap)
**Category**: Flap detection | **Risk**: High

**Hypothesis**: Failover to pdx, then immediately make pdx unreachable within 30s cooldown. Operator logs "failover blocked by anti-flap cooldown" and does NOT ping-pong back.

**Injection**:
```bash
./playground/chaos.sh kill-site iad
# Wait for "failover complete" in operator logs (~37s)
kubectl -n bloodraven-playground scale deployment mysql-playground-pdx --replicas=0
# Wait 15s
```

**Verify**: Operator logs "anti-flap cooldown". No second failover during cooldown window. System has no writable MySQL until cooldown expires.

**Important**: Use `scale --replicas=0` for the second kill, not `kill-site`. The pod must stay down longer than failureThreshold (6s) for the operator to even consider a second failover.

**Known difficulty**: This scenario has been INCONCLUSIVE across 3 rounds of testing. Three approaches fail:
1. `scale --replicas=0` — the operator reconciler immediately scales back to 1
2. NetworkPolicy on both sites — results in TOTAL LOSS (both unreachable), not a failover candidate, so the anti-flap check is never evaluated
3. Sequential partition (partition primary → failover → recover → partition new primary) — after recovering from the first partition, both sites can end up self-fenced in "NO PRIMARY" state

The anti-flap code (topology.go) is unit-tested but requires a scenario where one site is unreachable and the other is a valid promotion candidate within the cooldown window — difficult to achieve in the playground.

---

### 10. Full Bootstrap After Data Wipe (CLONE INSTANCE)
**Category**: Bootstrap / disaster recovery | **Risk**: High

**Hypothesis**: Wipe replica PVC -> operator detects fresh-deploy state -> CLONE INSTANCE -> replication setup.

**Injection**: `./playground/reset-mysql.sh` (wipes both sites and bootstraps from scratch)

**Verify**: Bootstrap phases progress (Cloning -> WaitingForRestart -> SetupReplication -> Done). Replica ends up read-only with replication running, data matches primary. GTID sets aligned. Typically takes 30-60s.

---

## Scenarios 11-15: Advanced Failure Modes

### 11. Simultaneous Both-Site Kill
**Category**: Total loss / graceful degradation | **Risk**: High

**Hypothesis**: Both MySQL deployments scaled to 0 -> TOTAL LOSS alert fires, no panic -> both come back -> operator re-establishes topology without manual intervention.

**Injection**:
```bash
NS=bloodraven-playground
kubectl -n $NS scale deployment mysql-playground-iad mysql-playground-pdx --replicas=0
# Wait 15s for TOTAL LOSS alert
kubectl -n $NS scale deployment mysql-playground-iad mysql-playground-pdx --replicas=1
```

**Verify**: Operator logs "TOTAL LOSS: both sites are unreachable" during outage. After recovery, one site writable, one read-only, replication running. No operator crash or permanent degraded state.

---

### 12. Rolling Update During Healthy State
**Category**: Zero-downtime operations | **Risk**: Medium

**Hypothesis**: Spec change (e.g. resource limits) triggers ordered update: standby rolls first, waits for replication health, then primary rolls with failover for zero write downtime.

**Injection**:
```bash
kubectl -n bloodraven-playground patch mysqlfailovergroup playground --type json \
  -p '[{"op":"replace","path":"/spec/sites/0/resources/requests/memory","value":"300Mi"},{"op":"replace","path":"/spec/sites/1/resources/requests/memory","value":"300Mi"}]'
```

**Verify**: Both pods end up with new resource spec. Replication running. One site writable, one read-only.

**Current behavior (as of April 2026)**: The `UpdateController` exists in `internal/controller/updater.go` with ordered-update logic (standby first, failover, old primary) but is **not wired into the reconciler**. Both Deployments use `RecreateDeploymentStrategyType` and restart simultaneously, causing a brief TOTAL LOSS window (~10-15s). After both pods return, the operator detects split brain and re-establishes the topology. The active site may flip during the update. Ordered zero-downtime updates require wiring in the `UpdateController`.

**Cleanup**: Revert the resource change after testing:
```bash
kubectl -n bloodraven-playground patch mysqlfailovergroup playground --type json \
  -p '[{"op":"replace","path":"/spec/sites/0/resources/requests/memory","value":"256Mi"},{"op":"replace","path":"/spec/sites/1/resources/requests/memory","value":"256Mi"}]'
```

---

### 13. Operator Kill During Bootstrap
**Category**: Bootstrap resilience | **Risk**: High

**Hypothesis**: Kill operator while CLONE INSTANCE is running -> new operator detects partial state -> either resumes waiting for clone completion or retries cleanly.

**Injection**:
```bash
# Trigger a fresh bootstrap (reset-mysql.sh or delete replica PVC)
# Watch for "cloning from primary" in operator logs
# Kill operator immediately: ./playground/chaos.sh kill-operator
```

**Verify**: New operator either waits for the in-progress clone to finish (MySQL tracks clone state in `performance_schema.clone_status`) or starts a fresh clone. End state: healthy primary/replica pair. No stuck bootstrap phase.

**Caution**: Before deleting the replica PVC, clear the `shipstream.io/db-readonly` taint from the replica's node. Otherwise the local-path-provisioner's helper pod gets evicted by the NoExecute taint and the replacement PVC never provisions, leaving the replica pod stuck in Pending.

---

### 14. Failover With Replication Lag (Async Data Loss Window)
**Category**: Data loss quantification | **Risk**: Medium

**Hypothesis**: With artificial replication lag, killing the primary loses exactly the lagged transactions. This quantifies the real-world data loss window of async replication.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=<active-site>  # iad or pdx
REPLICA=<other-site>

# Stop the SQL applier thread on the replica (IO thread keeps fetching)
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "STOP REPLICA SQL_THREAD;"

# Write 20 transactions on primary
for i in $(seq 1 20); do
  kubectl -n $NS exec deploy/mysql-playground-$PRIMARY -c mysql -- \
    mysql -uroot -pplayground-root-pw -e \
    "INSERT INTO chaos_test.lag_test VALUES ($i, NOW())"
done

# Verify lag: replica has fetched relay logs but not applied them
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e \
  "SELECT COUNT(*) FROM chaos_test.lag_test"
# Expected: 0 (SQL thread stopped)

# Kill primary
./playground/chaos.sh kill-site $PRIMARY
# Wait for failover (~37s)

# Check how many transactions survived on the new primary
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e \
  "SELECT COUNT(*) FROM chaos_test.lag_test"
```

**Verify**: After failover, the relay log drain phase (30s timeout) attempts to apply pending relay logs before promotion. If drain succeeds: all 20 rows survive. If drain times out (old primary unreachable, relay logs incomplete): some rows lost. Compare GTID sets to quantify exact loss.

**Testing challenge**: `kill-site` causes the primary pod to respawn in <5s. The operator detects the respawned pod before completing the failover, and the system may restore the original topology via split-brain recovery instead of completing the failover to the replica. This means the 20 rows survive via normal replication from the restarted primary, not via relay log drain. To force the primary to stay dead, try: scale operator to 0, scale primary to 0, then scale operator back to 1. The reconciler will eventually scale the primary back to 1, but the failover should complete before that.

**Alternative — SOURCE_DELAY**: Instead of stopping the SQL thread, use `CHANGE REPLICATION SOURCE TO SOURCE_DELAY=10` to add a fixed 10-second lag. Then write transactions over 5 seconds and kill the primary. The last ~10s of writes should be in the relay log but unapplied. Relay log drain should recover them if the relay logs are intact (they are — only the primary is dead, not the replica).

```bash
# Set 10-second replication delay
kubectl -n $NS exec deploy/mysql-playground-$REPLICA -c mysql -- \
  mysql -uroot -pplayground-root-pw -e \
  "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=10; START REPLICA;"

# Write transactions for 5 seconds, then kill primary
# The relay log has all transactions but SQL thread is 10s behind
# Relay log drain should apply them all (up to 30s timeout)
```

---

### 15. Sidecar Crash (Container Kill, Not Pod Kill)
**Category**: Degraded pod handling | **Risk**: Low

**Hypothesis**: Killing just the sidecar container (not the pod) leaves MySQL running but health checks stop. Kubernetes restarts the sidecar container. Operator should see brief unreachability on the sidecar HTTP endpoint but MySQL remains connectable on port 3306.

**Injection**:
```bash
NS=bloodraven-playground
SITE=<active-site>
POD=$(kubectl -n $NS get pods -l shipstream.io/site=$SITE -o name)

# Kill PID 1 in the sidecar container (triggers container restart, not pod restart)
# Distroless container has no kill binary — use kubectl debug:
kubectl -n $NS debug $POD --target=sidecar --image=busybox -- kill 1
```

**Verify**: MySQL stays up (port 3306 connectable throughout). Sidecar container restarts (check `RESTARTS` count). Operator may log brief health check failures but does NOT trigger failover (MySQL itself is still reachable). Sidecar re-initializes fencing timers on restart (grace period).

**Note**: `kubectl exec $POD -c sidecar -- kill 1` does NOT work on distroless images (no `kill` binary). Use `kubectl debug --target=sidecar --image=busybox` to get an ephemeral container that shares the sidecar's PID namespace.

---

## Scenarios 16-18: Additional Failure Modes

### 16. MySQL Process Kill (Not Pod Kill)
**Category**: Process-level failure | **Risk**: Medium

**Hypothesis**: Killing the mysqld process (via `mysqladmin shutdown`) causes the container to exit and restart. If MySQL recovers within failureThreshold (~6s), no failover. If slower, failover triggers correctly as fail-fast behavior.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=$(kubectl -n $NS get mysqlfailovergroups playground -o jsonpath='{.status.activeSite}')
POD=$(kubectl -n $NS get pods -l shipstream.io/site=$PRIMARY -o jsonpath='{.items[0].metadata.name}')

# Record restart count
kubectl -n $NS get pod/$POD -o jsonpath='{.status.containerStatuses[?(@.name=="mysql")].restartCount}'

# Kill mysqld (container will restart, pod stays)
kubectl -n $NS exec pod/$POD -c mysql -- mysqladmin -uroot -pplayground-root-pw shutdown
```

**Verify**: MySQL container restarts (RESTARTS count increments). Check whether failover triggered — with 2s poll interval and failureThreshold=3, even a 2-4 second outage may trigger failover. Either outcome is acceptable: no failover (MySQL recovered in time) or failover (correct fail-fast). Document which occurs and the timing.

**Note**: `kill -9 1` via `kubectl debug --target=mysql` does NOT work — PID 1 (init process) cannot be killed from within its PID namespace. Use `mysqladmin shutdown` instead.

---

### 17. Network Partition of Replica (Not Primary)
**Category**: Asymmetric failure — replica isolation | **Risk**: Medium

**Hypothesis**: Partitioning the replica causes the operator to log an alert but NOT trigger failover (primary is healthy). The replica sidecar does NOT self-fence (already read-only — `evaluate()` skips read-only nodes). After partition removal, replication resumes and data written during the partition catches up.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=$(kubectl -n $NS get mysqlfailovergroups playground -o jsonpath='{.status.activeSite}')
REPLICA=$([ "$PRIMARY" = "iad" ] && echo "pdx" || echo "iad")

# Write test data before partition
kubectl -n $NS exec deploy/mysql-playground-$PRIMARY -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "UPDATE chaos_test.counter SET val = 200 WHERE id = 1;"

# Partition the replica
./playground/chaos.sh network-partition $REPLICA

# Wait 15s for detection, then write more data during partition
kubectl -n $NS exec deploy/mysql-playground-$PRIMARY -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "UPDATE chaos_test.counter SET val = 300 WHERE id = 1;"

# Remove partition
./playground/chaos.sh recover
```

**Verify**: Active site unchanged (no failover). Replica sidecar logs show zero "SELF-FENC" entries. After partition removal, replica catches up — val=300 visible on replica. MySQL IO thread auto-reconnects via Service DNS.

---

### 18. Rapid CR Spec Changes During Active Failover
**Category**: Reconciler contention / state machine stress | **Risk**: High

**Hypothesis**: Concurrent CR spec patches during an active failover should not break convergence. The reconciler may log conflict errors ("Operation cannot be fulfilled on deployments.apps") but retries succeed. The system converges to exactly one writable site.

**Injection**:
```bash
NS=bloodraven-playground
PRIMARY=$(kubectl -n $NS get mysqlfailovergroups playground -o jsonpath='{.status.activeSite}')

# Kill primary to start failover
./playground/chaos.sh kill-site $PRIMARY

# Immediately hammer the CR with 5 spec changes
for i in $(seq 1 5); do
  sleep 2
  MEM=$((256 + i * 10))
  kubectl -n $NS patch mysqlfailovergroup playground --type json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/sites/0/resources/requests/memory\",\"value\":\"${MEM}Mi\"},{\"op\":\"replace\",\"path\":\"/spec/sites/1/resources/requests/memory\",\"value\":\"${MEM}Mi\"}]"
done
```

**Verify**: After ~60s, exactly one site has read_only=0. Check operator logs for "cannot be fulfilled" conflict errors (expected, should be retried). No stuck state or split-brain. Revert resource patch after test.

**Note**: Use JSON patch (`--type json`) instead of merge patch — merge patch drops required fields from the sites array.

---

## Execution Plan

1. **Setup**: `k3d cluster create` + `./playground/setup.sh`
2. **Run scenarios 1-10 first** (core failure modes, each builds confidence for later ones)
3. **Run 11-15** (advanced, may need reset between scenarios)
4. **Between scenarios**: `./playground/chaos.sh status` to confirm clean state; `./playground/reset-mysql.sh` if topology is broken
5. **For each scenario**: Document actual vs. expected, note timing and any bugs
6. **After code fixes**: `./playground/rebuild.sh operator` (or `sidecar`), then re-run affected scenario

## Files to Monitor

| File | Purpose |
|------|---------|
| `playground/chaos.sh` | Chaos injection primitives |
| `playground/setup.sh` | Playground deployment |
| `playground/rebuild.sh` | Selective image rebuild |
| `playground/reset-mysql.sh` | Data wipe and restart |
| `internal/controller/topology.go` | Polling loop, failover decisions |
| `internal/controller/failover.go` | Failover orchestration |
| `internal/controller/recovery.go` | Old primary recovery |
| `internal/controller/bootstrap.go` | CLONE INSTANCE bootstrap |
| `internal/sidecar/fencing.go` | Self-fencing monitor |
| `internal/state/machine.go` | Per-site state machine |
