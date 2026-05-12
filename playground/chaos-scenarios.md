# Bloodraven Chaos Testing Scenarios

## Automated runner

A subset of the scenarios below are wired into a deterministic Go test runner at `cmd/playground-chaos`. The runner injects failure, polls the live cluster (CR status, sidecar HTTP, operator metrics, structured logs), and asserts the documented outcomes with deadlines. It bails on the first assertion failure and dumps a forensic capture (`cluster.yaml`, `pods.yaml`, `events.yaml`, `operator.log`, `sidecar-<site>.log`, `metrics.txt`, `scenario.log`, `failure.txt`) under `playground/chaos-results/<timestamp>/<scenario-id>/` for an operator or AI agent to triage.

Run from the repo root:

```
make chaos-list                           # list registered scenarios
make chaos-check                          # verify the playground baseline is healthy
make chaos-run SCENARIO=01-clean-primary-kill
make chaos-run-all                        # bail on first failure (default)
```

The runner stamps an in-progress marker (`chaos.playground.bloodraven.io/in-progress` annotation on the MFG) after Precheck and clears it on cleanup or on the pass path. A subsequent run that finds a leftover marker refuses to start and tells you whether the prior owner is still alive (same host + live pid), abandoned (same host + dead pid), or on another host. Override with:

- `--force` — delete any prior marker before preflight (banner printed).
- `--auto-reset` — on Precheck failure, shell out to `./playground/reset-mysql.sh && ./playground/setup.sh`, then retry the scenario once. Wipes data; 3-second confirmation pause unless `CI=1`.

`chaos-check` runs the same structural baseline scenarios use (stuck scale-to-0 deployments, bogus `lastFailoverTarget`, anti-flap cooldown still ticking, both-sites-read-only `NoPrimary` symptom, replication off on a non-active candidate) and prints `inProgress: yes/no + summary`. Each error includes the exact remediation command, so `chaos-check` is the fastest way to decide whether to re-run, `--force`, or reset.

After each scenario, standard cleanup waits for the MFG `Ready=True` condition, stable MySQL site states, and, when `spec.dragonfly.enabled=true`, `status.dragonfly.phase=Ready` with exactly one master and healthy replicas. This keeps `run-all` from starting the next scenario while Dragonfly is still recovering from an expected REPLTAKEOVER or pod restart.

Some bootstrap scenarios exercise operator branches that only exist before any failover has stamped `status.lastFailoverTarget`. When `run-all` reaches those scenarios (`10-full-bootstrap-after-data-wipe` and `13-operator-kill-during-bootstrap`), the runner performs an explicit `reset` first so the precondition is real rather than hidden behind a broad auto-reset. Direct `run <scenario-id>` invocations still fail precheck on a non-pristine cluster and tell you to reset manually.

Currently automated (every `runner.Register` entry in `internal/playground/scenarios/`):

- `01-clean-primary-kill` (§1 below; uses `scale --replicas=0` for determinism, asserts failover only)
- `02-operator-kill-restart` (§2; negative-assertion — verifies activeSite stable and no SELF-FENCING during operator restart)
- `02-planned-switchover` (§S; planned-failover state machine — annotates the MFG with `bloodraven.shipstream.io/planned-failover=<peer>`, asserts `Validating→Draining→WaitingForLag→Promoting→Resuming→Succeeded`, `transactionsLost==0`, and the `bloodraven_planned_failovers_total{result="success"}` increment)
- `04-data-integrity-on-failover` (§4; seeds rows, blocks on `WAIT_FOR_EXECUTED_GTID_SET`, kills primary, asserts `GTID_SUBSET(pre, post)=1` and full row count on the new primary)
- `05-operator-kill-during-failover` (§5; scales the active primary to 0, sleeps 1s, kills the operator pod, asserts the cluster reconverges to 1 writable + 1 read-only with `Ready=True` and neither sidecar emitted SELF-FENC during the operator-down gap)
- `05-split-brain-auto-resolve` (§SBR; requires `spec.splitBrainPolicy.sitePriorities` set — clears `super_read_only` on the read-only site to force both writable, asserts the operator fences the non-preferred site, increments `bloodraven_split_brain_auto_resolve_total{prefer_site=...}`, and logs `split-brain auto-resolve`)
- `06-self-fence-isolated-primary` (§6; scales operator AND peer to 0 — true isolation path, complements `09-`)
- `08-gtid-divergence-detection` (§8; manufactures a rogue write on the old primary, asserts `recoveryState=RecoveryBlocked` + `divergentTransactionCount>0` + `divergence detected` log; auto-reclones in cleanup)
- `09-anti-flap-cooldown` (§9; force-deletes the active primary, waits for failover + the original primary to come back as a reachable read-only candidate, then scales the new primary to 0 inside `failoverCooldown` and asserts `failovers_total` increments by exactly 1 across all sites — best-effort log scan for `failover blocked by anti-flap cooldown`)
- `09-network-partition-self-fence` (§3 of this doc, NetworkPolicy partition path)
- `10-full-bootstrap-after-data-wipe` (§10; scales replica to 0, scrubs the per-FG readonly taint from every node, deletes the replica's data PVC, and waits for the operator's `Bootstrapping` condition to cycle through `Cloning` → `WaitingForRestart` → `SetupReplication` → `Done` with replica IO+SQL threads ON at the end)
- `11-total-loss-recovery` (§11; scales both sites to 0, asserts `TOTAL LOSS: all sites are unreachable` log + reconvergence)
- `12-old-primary-recovery-no-divergence` (§7 of this doc; recovery without divergence)
- `12-rolling-update-healthy-state` (§12; patches `spec.sites[*].resources.requests.memory` and asserts `status.updatePhase` engages then returns to empty, both deployments end at the new memory request, no `TOTAL LOSS` log fires during the roll)
- `13-operator-kill-during-bootstrap` (§13; wipes the read-only site's PVC, scales it back up, then kills the operator pod once `Bootstrapping=True` and asserts the replacement operator drives the clone to `Status=False Reason=Done` with replica IO+SQL threads ON — runs `ResetBeforeRunAll` so `lastFailoverTarget` is empty when the precondition gate runs)
- `14-failover-with-replication-lag` (§14; pauses the replica SQL applier, seeds rows over 5s on the primary so they sit in the relay log unapplied, then scales the primary to 0 and asserts the new primary's relay-log drain recovered every transaction — `GTID_SUBSET(post-write, current)=1` and full row count)
- `15-sidecar-crash-no-failover` (§15; ephemeral container `kill 1` against the sidecar PID namespace, asserts restartCount increments and activeSite/SELF-FENC/failover all stay quiet)
- `16-mysql-process-kill` (§16; issues SQL `SHUTDOWN` against the active site's mysqld and asserts the mysql container restartCount increments and the cluster never enters split-brain — accepts either "no failover" or "failover" outcomes, but rejects `>1 writable`, `RecoveryBlocked`, or sustained zero-writable)
- `17-partition-replica-no-failover` (§17; asymmetric partition — asserts NO failover and NO self-fence on read-only site)
- `18-rapid-cr-spec-changes-during-failover` (§18; triggers a failover and applies five rapid memory-request JSON patches at 2s intervals; asserts the cluster converges to {1 writable, 1 read-only, no RecoveryBlocked} with the last-applied memory request landing on every site deployment)
- `19-reclone-interlock` (§19; self-contained — manufactures divergence, then exercises rejected/accepted reclone annotation cases)
- `20-shared-node-selector-isolation` (§20; labels primary node into a fake `inventory` FG and asserts that a playground failover taints only `db-readonly-playground`, leaving `db-readonly-inventory` absent and the inventory canary still Running)
- `21-noexecute-eviction-semantics` (§21; deploys tolerating + non-tolerating canaries on the primary's node, asserts the non-tolerating one is deletion-marked or gone post-failover and the tolerating one stays Running)
- `22-planned-dragonfly-switchover` (§24; planned MySQL+Dragonfly failover, session-preservation status, active Service endpoint convergence, and Dragonfly promotion metrics)
- `22-replication-status-after-recovery` (§22; WISHLIST #36 guard — clean primary kill + old-primary auto-recovery, asserts the read-only site's `status.sites[].replicating` becomes true within 30s without an operator restart and cross-checks against the sidecar `/status` endpoint)
- `23-dragonfly-master-kill` (§25; Dragonfly-only master kill promotes the replica without changing MySQL activeSite)
- `23-failover-state-durability` (§23; WISHLIST #38 guard — clean failover, then kills the operator pod; asserts post-restart `lastFailoverTarget`, `activeSite`, and `lastFailover` survive, with up to 90s tolerance on the stamp to absorb a status-enrichment rewrite)
- `24-emergency-mysql-dragonfly-down` (§26; all Dragonfly StatefulSets scaled to 0, MySQL emergency failover still completes)
- `25-operator-restart-mid-dragonfly-failover` (§27; operator restart while planned failover is in Dragonfly sync/promotion phase resumes safely)
- `26-planned-dragonfly-sync-timeout-proceed` (§28; `onSyncTimeout=proceed` marks sessions not preserved and still completes MySQL failover)
- `27-dragonfly-rolling-image-update` (§30; ordinary `spec.dragonfly.image` patch rolls one pod at a time and promotes the updated replica before rolling the old active pod)
- `29-dragonfly-snapshot-upgrade` (§29; D6a snapshot-restore upgrade using the playground RustFS bucket)

`make chaos-list` is the authoritative inventory — when adding a scenario, also append it here so the doc and the registry stay in lock-step.

Sections marked `§S` (planned-switchover) and `§SBR` (split-brain auto-resolve) are documented as appendix-style sections below — they exist as scenarios but don't fit the §1-§30 numbered failure-mode grid.

The runner refuses to mutate any kubectl context that does not match the same allowlist as `playground/_guard.sh` (`k3d-*`, `kind-*`, `minikube*`, or names listed in `BLOODRAVEN_PLAYGROUND_CONTEXTS`). Markdown is the source of truth for hypotheses and prose; the runner's assertions are the operational ones documented under each scenario's "Verify" section.

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
- **db-readonly taint blocks PVC provisioning**: The operator applies `shipstream.io/db-readonly-playground:NoExecute` to non-writable nodes. The local-path-provisioner's helper pod does not tolerate this taint and gets evicted, blocking PVC creation. `reset-mysql.sh` now scales the operator to 0 before stripping taints (single-shot, no race), so a manual taint clear after a reset is no longer needed. If you wipe state by hand, mirror the same order: scale operator → 0, scale MySQL → 0, delete PVCs, strip taints, scale MySQL → 1, clear stale `status.lastFailover{,Target}`/`promotionGtidExecuted`/`plannedFailover`, scale operator → 1.
- **`reset-mysql.sh` dumps on timeout**: If the post-reset wait loop times out without both pods Ready, the script writes pods/events/PVC+PV/node-taint/per-container-log forensics under `playground/chaos-results/reset-<timestamp>/` and exits non-zero — no more "two commands you should have remembered" footer.
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

**Verify**: Status shows RecoveryPending with reason DivergentTransactions, operator logs "GTID divergence", exact divergent GTID set reported. Cleanup: run scenario 19 (Reclone Safety Interlock) to exercise the reclone path against the divergent state this scenario leaves behind, or skip straight to `./playground/reset-mysql.sh`.

**Important**: `reset-mysql.sh` is **required** after this scenario (or after scenario 19 if you chain) — divergent GTID state cannot be auto-recovered without a reclone. Clear db-readonly taints on both nodes after reset if PVCs fail to provision.

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

**Current behavior**: The `UpdateController` (`internal/controller/updater.go`) performs an ordered replica-first → failover → old-primary roll. Healthy case produces a brief writer flip but no TOTAL LOSS window.

**Issue #46 safety net (April 2026)**: If the standby is not actually replicating (`super_read_only=ON` but threads stopped / no source configured), the updater now **refuses to start** via an `isHealthyReplica()` debounce in the topology poll. If the new standby pod comes up writable with no source mid-update, `waitForReplicaReady` **fails fast** after ~30s instead of holding `isUpdating()=true` for the full 5-minute deadline; cross-site split-brain recovery then takes over. Expect either (a) a clean ordered update, or (b) an aborted update followed by reclone/fence on the next reconcile — not a 5-minute split-brain hang.

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

**Caution**: Before deleting the replica PVC, clear the `shipstream.io/db-readonly-playground` taint from the replica's node. Otherwise the local-path-provisioner's helper pod gets evicted by the NoExecute taint and the replacement PVC never provisions, leaving the replica pod stuck in Pending.

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

### 19. Reclone Safety Interlock
**Category**: Admin-action safety / divergent-GTID confirmation | **Risk**: Medium

**Hypothesis**: The `bloodraven.shipstream.io/reclone-site` annotation rejects bare site names and mismatched GTID prefixes when the target site has a non-empty `status.sites[].divergentGtid`, preventing a fat-fingered `reclone-site=<wrong-site>` from destroying the wrong replica. Cold reclones (no divergence recorded) still accept the bare form.

**Prerequisite**: Scenario 8 (GTID Divergence Detection) — it creates a site with `divergentGtid` and `recoveryState=RecoveryBlocked` that the interlock gates can be exercised against. Do NOT run `reset-mysql.sh` until this scenario completes; the interlock needs the divergent state scenario 8 leaves behind.

**Injection (four sub-cases)**:

```bash
NS=bloodraven-playground
FG=playground

# Identify the divergent site from scenario 8's aftermath.
SITE=$(kubectl -n $NS get mfg $FG -o jsonpath='{range .status.sites[?(@.recoveryState=="RecoveryBlocked")]}{.name}{end}')
DG=$(kubectl -n $NS get mfg $FG -o jsonpath="{.status.sites[?(@.name==\"$SITE\")].divergentGtid}")
echo "divergent site=$SITE gtid=$DG"

# --- A) Rejected: bare site name against a divergent site ---
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE
sleep 5
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp | tail -5
kubectl -n $NS get mfg $FG -o jsonpath='{.metadata.annotations.bloodraven\.shipstream\.io/reclone-site}'; echo

# --- B) Rejected: mismatched GTID prefix ---
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE:deadbeef
sleep 5
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp | tail -5

# --- C) Accepted: matching 8-char prefix of divergentGtid ---
PREFIX=$(echo -n "$DG" | cut -c1-8)
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE:$PREFIX
# Watch for RecloneRequested and the Bootstrapping condition entering Cloning.
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp -w &
EVENT_WATCH_PID=$!
sleep 90
kill $EVENT_WATCH_PID 2>/dev/null

# --- D) Cold reclone still works (no divergence) ---
# After sub-case C completes the site has rejoined as replica and no longer
# has divergentGtid in status. The bare form should now be accepted.
kubectl -n $NS annotate --overwrite mfg $FG bloodraven.shipstream.io/reclone-site=$SITE
sleep 5
kubectl -n $NS get events --field-selector involvedObject.name=$FG --sort-by=.lastTimestamp | tail -3
```

**Verify**:

| Sub-case | Expected outcome |
|---|---|
| A. bare site, divergent | `RecloneRejected` Warning event with "annotation must include the divergent-GTID prefix" text; annotation removed from CR; no `Bootstrapping` condition transition; `divergentGtid` unchanged. |
| B. mismatched prefix | `RecloneRejected` Warning event with "does not match the observed divergentGtid" text; annotation removed; site still fenced; no clone executed. |
| C. matching prefix | `RecloneRequested` Normal event with site name; `Bootstrapping` condition cycles through `Cloning` → `WaitingForRestart` → `SetupReplication` → `Done`; `RecoveryPending` clears; site comes back as replica with `SERVICE_STATE=ON`. Wall-clock: 30–60s total. |
| D. cold bare site | `RecloneRequested` Normal event (no rejection); another full bootstrap cycle runs. This confirms we didn't accidentally make the interlock mandatory in the non-divergent case. |

**Timing**: Sub-cases A and B resolve within one sync cycle (~30s; the runner's `sync()` ticker). Sub-case C takes 30-60s for the clone + restart. Sub-case D is the same.

**Important — sync interval**: The runner re-reads CRs every 30s, so the annotation may sit briefly before being processed. If you see no event after 5s, wait up to 35s before concluding rejection isn't firing.

**Cleanup**: `./playground/reset-mysql.sh` is advisable after sub-case D to restore a clean matrix for subsequent scenarios. Clear the `shipstream.io/db-readonly-playground` taint from both nodes after the reset.

---

### 20. Shared-Node Selector Isolation
**Category**: Placement / multi-tenant isolation | **Risk**: Low

**Hypothesis**: A node can advertise membership in multiple failover groups at the same site, and a failover for `playground` only applies `shipstream.io/db-readonly-playground`. Workloads for an unrelated group that tolerate `playground`'s taint are not evicted.

**Injection**:
```bash
NS=bloodraven-playground
FG=playground
PRIMARY=$(kubectl -n $NS get mfg $FG -o jsonpath='{.status.activeSite}')
NODE=$(kubectl get nodes -l "shipstream.io/failover-group.playground=true,shipstream.io/site.playground=$PRIMARY" -o jsonpath='{.items[0].metadata.name}')

# Mark the same physical node as also serving an unrelated inventory group.
kubectl label node $NODE \
  shipstream.io/failover-group.inventory=true \
  shipstream.io/site.inventory=$PRIMARY \
  --overwrite

# Simulated inventory workload: it tolerates playground's taint because
# playground failovers should not evict inventory pods on shared nodes.
cat <<YAML | kubectl -n $NS apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: inventory-shared-node-canary
  labels:
    app: inventory-shared-node-canary
spec:
  nodeSelector:
    shipstream.io/failover-group.inventory: "true"
    shipstream.io/site.inventory: "$PRIMARY"
  tolerations:
    - key: shipstream.io/db-readonly-playground
      operator: Exists
      effect: NoExecute
  containers:
    - name: sleep
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
YAML

kubectl -n $NS wait --for=condition=Ready pod/inventory-shared-node-canary --timeout=60s
./playground/chaos.sh kill-site $PRIMARY
sleep 45
```

**Verify**:
```bash
# The old-primary node should have the playground taint only.
kubectl get node $NODE -o jsonpath='{.spec.taints[*].key}{"\n"}'
# Expected includes: shipstream.io/db-readonly-playground
# Expected excludes: shipstream.io/db-readonly-inventory

# The unrelated inventory pod should still be running on the shared node.
kubectl -n $NS get pod inventory-shared-node-canary -o wide
# Expected: STATUS=Running, NODE=$NODE
```

**Cleanup**:
```bash
kubectl -n $NS delete pod inventory-shared-node-canary --ignore-not-found
kubectl label node $NODE shipstream.io/failover-group.inventory- shipstream.io/site.inventory- 2>/dev/null || true
./playground/chaos.sh recover
```

---

### 21. NoExecute Eviction Semantics
**Category**: Placement / eviction contract | **Risk**: Low

**Hypothesis**: During failover, the old active site's selected nodes receive `shipstream.io/db-readonly-playground=true:NoExecute`. Pods without that toleration are evicted; pods with the toleration remain.

**Injection**:
```bash
NS=bloodraven-playground
FG=playground
PRIMARY=$(kubectl -n $NS get mfg $FG -o jsonpath='{.status.activeSite}')

cat <<YAML | kubectl -n $NS apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: noexecute-evict-canary
  labels:
    app: noexecute-evict-canary
spec:
  nodeSelector:
    shipstream.io/failover-group.playground: "true"
    shipstream.io/site.playground: "$PRIMARY"
  containers:
    - name: sleep
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
---
apiVersion: v1
kind: Pod
metadata:
  name: noexecute-tolerate-canary
  labels:
    app: noexecute-tolerate-canary
spec:
  nodeSelector:
    shipstream.io/failover-group.playground: "true"
    shipstream.io/site.playground: "$PRIMARY"
  tolerations:
    - key: shipstream.io/db-readonly-playground
      operator: Exists
      effect: NoExecute
  containers:
    - name: sleep
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
YAML

kubectl -n $NS wait --for=condition=Ready pod/noexecute-evict-canary --timeout=60s
kubectl -n $NS wait --for=condition=Ready pod/noexecute-tolerate-canary --timeout=60s
./playground/chaos.sh kill-site $PRIMARY
sleep 45
```

**Verify**:
```bash
kubectl -n $NS get pods noexecute-evict-canary noexecute-tolerate-canary -o wide
# Expected: noexecute-evict-canary is gone/Failed or has a deletion timestamp.
# Expected: noexecute-tolerate-canary remains Running on the old-primary node.

kubectl -n $NS get pod noexecute-evict-canary >/dev/null 2>&1 && \
  kubectl -n $NS get pod noexecute-evict-canary -o jsonpath='{.metadata.deletionTimestamp}{"\n"}' || \
  echo "noexecute-evict-canary removed as expected"

kubectl -n $NS get pod noexecute-tolerate-canary -o jsonpath='{.status.phase}{"\n"}'
# Expected: Running
```

**Cleanup**:
```bash
kubectl -n $NS delete pod noexecute-evict-canary noexecute-tolerate-canary --ignore-not-found --grace-period=0 --force
./playground/chaos.sh recover
```

---

## Scenarios 22-23: Status durability regressions (from WISHLIST)

These scenarios were extracted from `WISHLIST.md` items #36 and #38 — both flag specific contracts that the chaos runner was not previously asserting. They are registered with the same `playground-chaos` runner as 1–21 and follow the same Inject → Observe → Verify shape.

Fail-back behavior is intentionally GTID-freshest/current-state-driven. When an original primary returns, it is not treated specially as "the old primary that must rejoin as a replica"; it can become writable again only by winning the same promotion candidate selection used for any failover, subject to anti-flap cooldown and configured site-priority tie-breaking. Scenario assertions should therefore track the current writable site and current healthy replica, not fixed site identities.

### 22. Replication Status After Recovery
**Category**: CR-status enrichment regression | **Risk**: Low

**Hypothesis**: After a clean primary kill and old-primary auto-recovery, the read-only site's `status.sites[].replicating` becomes true within 30s — without requiring an operator restart. During recovery, `status.sites[].recoveryState=RecoveryInProgress` may be visible until MySQL reports healthy replication. This is the contract that `internal/controller/planned_failover.go`'s `TargetUnhealthy` safety check depends on.

**Why this regression test exists**: WISHLIST #36 documented that `replicating` and `gtidExecuted` stopped being populated on a post-recovery read-only site even though the sidecar `/status` endpoint reported replication threads running. The bug silently broke any planned switchover after the first chaos event in the same operator lifecycle. This scenario now guards that status-enrichment contract.

**Injection**: `make chaos-run SCENARIO=22-replication-status-after-recovery`. The runner scales the active primary to 0, waits for failover, scales it back up, waits for re-convergence, then reads the CR.

**Verify**: Within 30s of `writable=1 read-only=1`, `mfg.Status.Sites[i].Replicating == true` for the read-only site. The scenario also probes the sidecar `/status` endpoint as a cross-check; if the sidecar reports replication running but the CR field is false, that is exactly the bug WISHLIST #36 describes.

**Cleanup**: Standard runner cleanup. No manual steps required.

---

### 23. Failover State Durability
**Category**: CR-status persistence | **Risk**: Low

**Hypothesis**: After a clean failover, killing and restarting the operator pod must NOT clear `status.lastFailoverTarget`, `status.lastFailover`, or an in-flight old-primary `RecoveryInProgress` marker. The post-restart operator must rehydrate those fields from the CR; `activeSite` must remain at the post-failover primary, and old-primary recovery must either clear when replication is healthy or retry when it is not.

**Why this regression test exists**: WISHLIST #38 noted that failover history and old-primary-recovery dispatch keys were partly in-memory. The operator now persists and rehydrates failover timestamps plus `RecoveryInProgress`/`RecoveryBlocked`, so a restart during recovery keeps the CR-visible lifecycle and retries safely instead of waiting for another failover.

**Injection**: `make chaos-run SCENARIO=23-failover-state-durability`. The runner scales the primary to 0, waits for failover, snapshots `lastFailoverTarget` / `lastFailover` / `activeSite`, kills the operator pod, sleeps 30s for the new pod to come up and reconcile, then re-reads the CR.

**Verify**: Post-restart values match the pre-restart snapshot. `lastFailover` is allowed to advance by up to 90s within tolerance (a status-enrichment loop may rewrite the field with a slightly later timestamp); a regression to zero or a "fresh failover happened just now" jump is rejected.

**Cleanup**: Standard runner cleanup. The operator's auto-recovery brings the original primary back up as a replica.

---

## Scenarios 24-30: Dragonfly integration

These scenarios cover the Bloodraven-owned Dragonfly topology enabled by `spec.dragonfly`. They assume the playground MFG has Dragonfly enabled and that the baseline check reports `status.dragonfly.phase=Ready`, one master, linked replicas, and no unreachable sites.

Live k3d status from May 3, 2026: scenarios 22, 23, 24, 25, 26, 27, and 29 pass in isolation after runner cleanup was updated to wait for Dragonfly reconvergence. Scenario 29 provisions the RustFS bucket and temporary `spec.dragonfly.snapshot` config itself; the default playground still does not enable snapshots at baseline.

### 24. Planned Dragonfly Switchover
**Category**: Coordinated planned failover | **Risk**: Medium

**Hypothesis**: A planned failover promotes the target Dragonfly replica with `REPLTAKEOVER`, preserves the seeded session key, flips `status.dragonfly.activeSite`, and converges the active Service endpoints to the new master pod.

**Injection**: `make chaos-run SCENARIO=22-planned-dragonfly-switchover`

**Verify**: `plannedFailover.phase=Succeeded`, `plannedFailover.dragonfly.PromotionMethod=REPLTAKEOVER`, `SessionsPreserved=true`, the seeded key is readable on the target, active Service endpoints select only the target Dragonfly pod, and `bloodraven_dragonfly_promotions_total{result="success"}` increments.

---

### 25. Dragonfly Master Kill
**Category**: Dragonfly-only HA | **Risk**: Low

**Hypothesis**: Force-deleting the active Dragonfly pod promotes the surviving replica and leaves MySQL untouched.

**Injection**: `make chaos-run SCENARIO=23-dragonfly-master-kill`

**Verify**: `status.dragonfly.activeSite` flips to the peer, MySQL `status.activeSite` remains unchanged, the seeded key survives on the promoted replica, the respawned old master rejoins as `role=replica` with `linkStatus=up`, and the Dragonfly promotion metric increments.

---

### 26. Emergency MySQL Failover With Dragonfly Down
**Category**: Emergency fallback | **Risk**: High

**Hypothesis**: Scaling every Dragonfly StatefulSet to 0 does not block emergency MySQL promotion.

**Injection**: `make chaos-run SCENARIO=24-emergency-mysql-dragonfly-down`

**Verify**: `status.dragonfly.phase` leaves `Ready`, the active MySQL pod is deleted, and MySQL `status.activeSite` flips to the peer within the normal emergency budget. Dragonfly recovery is handled by cleanup and global recovery, not by the MySQL critical path.

---

### 27. Operator Restart Mid-Dragonfly Failover
**Category**: Operator resilience | **Risk**: Medium

**Hypothesis**: If the operator restarts while planned failover is waiting for Dragonfly sync, the replacement operator resumes from CR status and converges without swapping targets or leaving split-brain Dragonfly state.

**Injection**: `make chaos-run SCENARIO=25-operator-restart-mid-dragonfly-failover`

The scenario temporarily scales the target Dragonfly StatefulSet to 0 and patches the sync budget to 45s so `WaitingForDragonflySync` is deterministic, kills the operator after observing that fresh phase, then restores the target Dragonfly pod so the replacement operator can complete the same planned failover.

**Verify**: Planned failover reaches `Succeeded` with the original target, MySQL and Dragonfly active sites both equal that target, and `status.dragonfly.phase` returns to `Ready` with exactly one master.

---

### 28. Planned Dragonfly Sync Timeout Proceeds
**Category**: Degraded planned failover | **Risk**: Medium

**Hypothesis**: With `maxSyncWait=1ms`, target Dragonfly scaled to 0, and `onSyncTimeout=proceed`, the planned failover does not claim session preservation but still completes MySQL promotion.

**Injection**: `make chaos-run SCENARIO=26-planned-dragonfly-sync-timeout-proceed`

**Verify**: Planned failover reaches `Succeeded`, MySQL `status.activeSite` flips to the target, `plannedFailover.dragonfly.SessionsPreserved=false`, the reason is `DragonflySyncTimeout` or `DragonflyPromotionFailed`, and `bloodraven_dragonfly_promotions_total{result="failed"}` increments. The settle step force-respawns the old Dragonfly source and waits up to 4 minutes for it to rejoin as a replica, because this timeout path can briefly lag status convergence after MySQL recovery.

---

### 29. Dragonfly Snapshot-Restore Upgrade (D6a)
**Category**: Planned maintenance | **Risk**: Medium

**Hypothesis**: With `spec.dragonfly.snapshot.dir` configured to an S3-compatible bucket and the Dragonfly pods running under a ServiceAccount that can read/write it, Bloodraven can perform a short-outage planned upgrade by saving a snapshot, replacing the active Dragonfly pod on the target image, waiting for restore, then replacing and reattaching replicas.

**Current status**: Live-passing in k3d as of May 3, 2026: `PASS 29-dragonfly-snapshot-upgrade`, duration 4m26.292s. The baseline playground manifest intentionally omits `spec.dragonfly.snapshot` so normal Dragonfly pods do not startup-fail when RustFS or credentials are unavailable. Scenario 29 provisions and validates its snapshot backend itself before requesting the upgrade.

**Injection**:

```bash
kubectl -n bloodraven-playground annotate --overwrite mysqlfailovergroup playground \
  bloodraven.shipstream.io/dragonfly-snapshot-upgrade=docker.dragonflydb.io/dragonflydb/dragonfly:<target>
```

**Verify**: `status.dragonfly.upgrade.phase` walks `Pending → SavingSnapshot → UpdatingActive → WaitingForActiveRestore → ReattachingReplicas → Succeeded`; the active Service has no endpoints while restore is in progress; `SAVE` completes before the active pod is deleted; final Dragonfly pods run the target image; `status.dragonfly.phase` returns to `Ready`; the seeded key is present after restore.

**Automation status**: Registered as `29-dragonfly-snapshot-upgrade`. The playground deploys RustFS during setup; the scenario creates the `dragonfly` bucket on demand, temporarily patches `spec.dragonfly.snapshot`, waits for Dragonfly pods to run with `--dir=s3://dragonfly/playground`, and then runs the upgrade.

---

### 30. Dragonfly Rolling Image Update (D6b)
**Category**: Zero-downtime operations | **Risk**: Medium

**Hypothesis**: A normal `spec.dragonfly.image` change rolls one Dragonfly pod at a time: the non-active site updates first, Bloodraven promotes that updated replica, and the old active site updates only after traffic has moved.

**Current status**: Live-passing in k3d as of May 3, 2026: `PASS 27-dragonfly-rolling-image-update`, duration 16.159s.

**Injection**: `make chaos-run SCENARIO=27-dragonfly-rolling-image-update`

The scenario patches `spec.dragonfly.image` to the digest reference already reported by a running Dragonfly pod. That changes the pod template and exercises the image rollout path without depending on a new external image pull.

**Verify**: No more than one Dragonfly pod is unavailable at any observed point; both StatefulSets and pods reach the target image; `status.dragonfly.phase` returns to `Ready`; and the runner cleanup restores the original tag image.

---

## Appendix scenarios (state-machine coverage)

These two scenarios cover state-machine paths that don't fit the §1-§30 emergency-failover grid. They are registered with the same `playground-chaos` runner and follow the same Inject → Observe → Verify shape; their cited `§S` and `§SBR` anchors are referenced from the "Currently automated" list above.

### S. Planned Switchover (`02-planned-switchover`) {#planned-switchover}
**Category**: Planned-failover state machine | **Risk**: Low

**Hypothesis**: Annotating the MFG with `bloodraven.shipstream.io/planned-failover=<peer>` walks `status.plannedFailover.phase` through `Validating → Draining → WaitingForLag → Promoting → Resuming → Succeeded` with `status.plannedFailover.transactionsLost == 0` (committed-and-replicated rows survive a planned switchover by construction).

**Injection**: `make chaos-run SCENARIO=02-planned-switchover`. The scenario picks the peer of the current active site as the target and patches the annotation.

**Verify**:

- `status.plannedFailover.phase` reaches `Succeeded` with `target=<peer>` (stale plannedFailover blocks left over from prior runs are ignored via the `StartTime` check, so this assertion is robust under rerun).
- `status.plannedFailover.transactionsLost` is populated and equals 0.
- `bloodraven_planned_failovers_total{target_site=<peer>,result="success"}` increments.

**Note**: Single-scenario `run` will fail precheck if the cluster already has an in-flight `plannedFailover` in a non-terminal phase. The runner's standard cleanup waits for `Ready=True` before the next scenario starts, so `run-all` does not need to reset between this and the next entry.

---

### SBR. Split-Brain Auto-Resolve (`05-split-brain-auto-resolve`) {#split-brain-auto-resolve}
**Category**: Split-brain auto-resolve | **Risk**: Medium

**Hypothesis**: With `spec.splitBrainPolicy.sitePriorities` set, forcing both sites simultaneously writable (clear `super_read_only` on the standby) makes the operator fence the non-preferred site within one reconcile, increment `bloodraven_split_brain_auto_resolve_total{prefer_site=<preferred>}`, and emit the canonical `split-brain auto-resolve` log line.

**Precondition**: `spec.splitBrainPolicy.sitePriorities` is empty on the baseline `playground` MFG. The scenario's `Precheck` returns a specific error when it is missing — set it before running:

```bash
kubectl -n bloodraven-playground patch mysqlfailovergroup playground --type json \
  -p '[{"op":"add","path":"/spec/splitBrainPolicy","value":{"sitePriorities":["iad","pdx"]}}]'
```

**Injection**: `make chaos-run SCENARIO=05-split-brain-auto-resolve`. The scenario also briefly scales the operator to 0 to clear `status.lastFailover{,Target}` so the cooldown branch does not suppress the auto-resolve.

**Verify**:

- The non-preferred site stops being writable; only the priority site remains `state=writable`.
- `status.activeSite` equals `spec.splitBrainPolicy.sitePriorities[0]`.
- The `bloodraven_split_brain_auto_resolve_total{prefer_site=...}` counter increments above the pre-injection baseline.
- The operator log contains `split-brain auto-resolve` (canonical msg from `internal/controller/topology.go`).

**Cleanup**: None scenario-level — the runner's `GlobalRecover` and the operator's next reconcile re-assert `super_read_only` on the standby.

---

## Execution Plan

1. **Setup**: `k3d cluster create` + `./playground/setup.sh`
2. **Run scenarios 1-10 first** (core failure modes, each builds confidence for later ones)
3. **Run 11-18** (advanced, may need reset between scenarios)
4. **Run 19 immediately after scenario 8** (scenario 8 leaves behind the divergent state it needs as a prerequisite — don't reset in between)
5. **Run 20-21 after a clean reset** (placement selector and `NoExecute` eviction semantics)
6. **Run 22-23 anywhere they fit** (CR-status regressions; both expect a healthy baseline and clean up after themselves)
7. **Between scenarios**: `./playground/chaos.sh status` to confirm clean state; `./playground/reset-mysql.sh` if topology is broken
8. **For each scenario**: Document actual vs. expected, note timing and any bugs
9. **After code fixes**: `./playground/rebuild.sh operator` (or `sidecar`), then re-run affected scenario
