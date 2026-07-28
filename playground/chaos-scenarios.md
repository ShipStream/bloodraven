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

`chaos-check` runs the same structural baseline scenarios use (stuck scale-to-0 deployments, bogus `lastFailoverTarget`, anti-flap cooldown still ticking, all-sites-read-only `NoPrimary` symptom, replication off on a non-active candidate) and prints `inProgress: yes/no + summary`. Each error includes the exact remediation command, so `chaos-check` is the fastest way to decide whether to re-run, `--force`, or reset.

After each scenario, standard cleanup waits for the MFG `Ready=True` condition, stable MySQL site states, and, when `spec.dragonfly.enabled=true`, `status.dragonfly.phase=Ready` with exactly one master and healthy replicas. This keeps `run-all` from starting the next scenario while Dragonfly is still recovering from an expected REPLTAKEOVER or pod restart.

Some bootstrap scenarios exercise operator branches that only exist before any failover has stamped `status.lastFailoverTarget`. When `run-all` reaches those scenarios (`10-full-bootstrap-after-data-wipe` and `13-operator-kill-during-bootstrap`), the runner performs an explicit `reset` first so the precondition is real rather than hidden behind a broad auto-reset. Direct `run <scenario-id>` invocations still fail precheck on a non-pristine cluster and tell you to reset manually.

Currently automated (every `runner.Register` entry in `internal/playground/scenarios/`):

- `01-clean-primary-kill` (§1 below; uses `scale --replicas=0` for determinism, asserts failover only)
- `02-operator-kill-restart` (§2; negative-assertion — verifies activeSite stable and no SELF-FENCING during operator restart)
- `02-planned-switchover` (§S; planned-failover state machine — annotates the MFG with `bloodraven.shipstream.io/planned-failover=<peer>`, asserts `Validating→Draining→WaitingForLag→Promoting→Resuming→Succeeded`, `transactionsLost==0`, and the `bloodraven_planned_failovers_total{result="success"}` increment)
- `04-data-integrity-on-failover` (§4; seeds rows, blocks on `WAIT_FOR_EXECUTED_GTID_SET`, kills primary, asserts `GTID_SUBSET(pre, post)=1` and full row count on the new primary)
- `05-operator-kill-during-failover` (§5; scales the active primary to 0, sleeps 1s, kills the operator pod, asserts the cluster reconverges to 1 writable + all followers read-only with `Ready=True` and no sidecar emitted SELF-FENC during the operator-down gap)
- `05-split-brain-auto-resolve` (§SBR; requires `spec.splitBrainPolicy.sitePriorities` set — clears `super_read_only` on the read-only site to force both writable, asserts the operator fences the non-preferred site, increments `bloodraven_split_brain_auto_resolve_total{prefer_site=...}`, and logs `split-brain auto-resolve`)
- `06-self-fence-isolated-primary` (§6; scales operator AND every non-self peer, including the reader, to 0 — true isolation path, complements `09-`)
- `08-gtid-divergence-detection` (§8; manufactures a rogue write on the old primary, asserts `recoveryState=RecoveryBlocked` + `divergentTransactionCount>0` + `divergence detected` log; auto-reclones in cleanup)
- `09-anti-flap-cooldown` (§9; force-deletes the active primary, waits for failover + the original primary to come back as a reachable read-only candidate, then scales the new primary to 0 inside `failoverCooldown` and asserts `failovers_total` increments by exactly 1 across all sites — best-effort log scan for `failover blocked by anti-flap cooldown`)
- `09-network-partition-self-fence` (§3 of this doc, NetworkPolicy partition path)
- `10-full-bootstrap-after-data-wipe` (§10; scales a candidate replica to 0, scrubs the per-FG readonly taint from every node, deletes that replica's data PVC, and waits for the operator's `Bootstrapping` condition to cycle through `Cloning` → `WaitingForRestart` → `SetupReplication` → `Done` with replica IO+SQL threads ON at the end)
- `11-total-loss-recovery` (§11; scales every site to 0, asserts `TOTAL LOSS: all sites are unreachable` log + reconvergence)
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
- `27-dragonfly-rolling-image-update` (§D6b; ordinary `spec.dragonfly.image` patch rolls one pod at a time and promotes the updated replica before rolling the old active pod)
- `29-dragonfly-snapshot-upgrade` (§29; D6a snapshot-restore upgrade using the playground RustFS bucket)
- `30-backup-verification-rustfs` (§30; configures a RustFS backup profile, creates a real `MysqlBackup`, pins `MysqlBackupVerification.spec.backupRef`, and asserts marker rows restore)
- `31-pitr-verification-rustfs` (§31; enables RustFS PITR, archives sealed binlogs, verifies timestamp replay includes pre-target rows and excludes post-target rows)
- `32-mfg-status-write-denial-emergency-promotion` (§32; denies the operator patch/update on `mysqlfailovergroups/status`, scales the active primary to 0, asserts MySQL promotes at the data layer while the CR status is frozen, then self-heals to the promoted site within 90s after RBAC restore)
- `33-scoped-dns-outage` (§33; NetworkPolicy blocking only kube-dns egress from the active MySQL pod — proven with a DNS canary first — asserts the outage is DNS-scoped, not a pod partition and not the DNSEndpoint-API denial of §38)
- `34-operator-kill-during-wait-replica` (§34; force-deletes the operator while an ordered update is held at `WaitReplica`; the replacement operator re-derives remaining drift from Deployment spec-hash mismatch and finishes with no double-roll and no `TOTAL LOSS`)
- `35-planned-switchover-lag-timeout-rollback` (§35; `SOURCE_DELAY=60` on the target + `maxLagWait=5s` drives `WaitingForLag`→`Failed{LagTimeout}`; source stays active, `bloodraven_planned_failovers_total{result="failed_timeout"}` increments)
- `36-rustfs-outage-during-restore-in-place` (§36; a per-schema in-place restore whose RustFS storage is scaled to 0 mid-run terminates `Failed` with the topology frozen; the untouched canary schema survives and the terminal `Failed` holds on the same confirm — a retry needs a new monotonic confirm — `ResetBeforeRunAll`)
- `37-pitr-archive-handoff-across-failover` (§37; marker A archived on the old active, emergency failover, marker B archived on the new active; a timestamp verification replays baseline+A+B and excludes C across both per-site manifests — `ResetBeforeRunAll`)
- `38-dnsendpoint-write-denial-during-failover` (§38; denies the operator write verbs on `dnsendpoints`, forces promotion; the DNSEndpoint target stays stale during denial then heals to the promoted LBIP within 90s after RBAC restore with no re-failover)
- `39-dragonfly-master-partition` (§39; deny-all NetworkPolicy on only the Dragonfly master pod promotes the surviving replica; MySQL `status.activeSite` and failover counters are unchanged)
- `40-reader-data-loss-reclone` (§40; release-profile reader PVC replacement, continuous `Ready=True`, auto-clone donor/internal-host log assertion, direct-source/thread/lag/marker recovery, client EndpointSlice shedding, and internal endpoint publication)

Scenarios `32`–`39` are **full-profile-only** by allowlist omission — none are in the `smoke` or `release` subsets — until they accumulate broader repeated live-pass history with no destructive leakage. As of 2026-07-11, all eight have passed on the k3d playground; scenarios `34`, `35`, `36`, and `38` also passed post-review reruns with the final code. Scenario `36` needed three diagnosed-and-fixed failed attempts first (see §36's "Actual live result"). All eight remain full-profile-only while this new, high-risk coverage matures. Scenarios `36` and `37` additionally run `ResetBeforeRunAll` because they exercise backup/PITR/restore-in-place state that can leak into later scenarios if cleanup is interrupted.

`make chaos-list` is the authoritative inventory — when adding a scenario, also append it here so the doc and the registry stay in lock-step.

Sections marked `§S` (planned-switchover) and `§SBR` (split-brain auto-resolve) are documented as appendix-style sections below — they exist as scenarios but don't fit the §1-§39 numbered failure-mode grid. The Dragonfly rolling image update, formerly numbered §32, is now the unnumbered §D6b appendix item so §32 could open the new emergency/denial band (§32-§39).

The runner refuses to mutate any kubectl context that does not match the same allowlist as `playground/_guard.sh` (`k3d-*`, `kind-*`, `minikube*`, or names listed in `BLOODRAVEN_PLAYGROUND_CONTEXTS`). Markdown is the source of truth for hypotheses and prose; the runner's assertions are the operational ones documented under each scenario's "Verify" section.

## Prerequisites

1. Create k3d cluster: `k3d cluster create bloodraven --agents 3 --k3s-arg '--tls-san=<hostname>@server:0'`
2. Run playground setup: `./playground/setup.sh`
3. Verify healthy state: `kubectl -n bloodraven-playground get mysqlfailovergroups -o wide` shows one writable and two read-only followers

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

**Verify**: activeSite unchanged, every MySQL site unchanged, zero "SELF-FENC" in sidecar logs, operator logs show clean startup.

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

**Hypothesis**: Fully isolate primary (scale operator=0, scale every non-self peer including the reader=0). Sidecar self-fences at ~T+20s.

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

**Hypothesis**: Every MySQL deployment scaled to 0 -> TOTAL LOSS alert fires, no panic -> all come back -> operator re-establishes topology without manual intervention.

**Injection**:
```bash
NS=bloodraven-playground
kubectl -n $NS scale deployment mysql-playground-iad mysql-playground-pdx mysql-playground-reader --replicas=0
# Wait 15s for TOTAL LOSS alert
kubectl -n $NS scale deployment mysql-playground-iad mysql-playground-pdx mysql-playground-reader --replicas=1
```

**Verify**: Operator logs a total-loss alert during the outage. After recovery, one site is writable and two followers are read-only with replication running. No operator crash or permanent degraded state.

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

**Verify**: Within 30s of `writable=1 read-only=N-1`, `mfg.Status.Sites[i].Replicating == true` for the recovered candidate follower. The scenario also probes the sidecar `/status` endpoint as a cross-check; if the sidecar reports replication running but the CR field is false, that is exactly the bug WISHLIST #36 describes.

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

### 30. Backup Verification Against RustFS
**Category**: Backup/restore verification | **Risk**: Medium

**Hypothesis**: A one-off `MysqlBackup` written to the in-cluster RustFS bucket can be restored by a pinned `MysqlBackupVerification`, and the restored database contains deterministic marker rows from the full dump.

**Injection**: `make chaos-run SCENARIO=30-backup-verification-rustfs`

The scenario creates/ensures bucket `bloodraven-backup-e2e`, patches only `MysqlFailoverGroup.spec.backup` with profile `rustfs-e2e`, and isolates objects under `e2e/30-backup-verification-rustfs/<run-stamp>/`. It seeds `chaos_s30_backup.marker`, waits for replica GTID catch-up, creates one named `MysqlBackup`, then creates one `MysqlBackupVerification` with `spec.backupRef.name` pinned to that backup instead of relying on latest-success resolution.

**Verify**: The backup reaches `Succeeded` with `status.storageType=S3`, `status.location=<prefix>/<backup>`, and a Job name. The verification reaches `Succeeded`, reports `status.backupRef.name` for the created backup, runs the sanity query, returns marker count `2`, and has `Verified=True`.

**Timing**: Scenario timeout is 18 minutes; backup and verification waits each have a 12-minute sub-budget. Cleanup deletes verification then backup CRs, drops `chaos_s30_backup`, restores the original `spec.backup`, and waits for a healthy baseline.

---

### 31. PITR Verification Against RustFS
**Category**: Backup/PITR verification | **Risk**: Medium

**Hypothesis**: With PITR enabled against RustFS, Bloodraven archives sealed binlogs and a pinned `MysqlBackupVerification` can replay to a timestamp so baseline and before-target rows exist while after-target rows are absent.

**Injection**: `make chaos-run SCENARIO=31-pitr-verification-rustfs`

The scenario uses the same RustFS bucket/profile/credential Secret as scenario 30 but isolates objects under `e2e/31-pitr-verification-rustfs/<run-stamp>/` and enables `spec.backup.pitr` (`profileName=rustfs-e2e`, `maxBinlogSize=1M`, `archivePollInterval=2s`). It waits for the active sidecar `/archiver/status` endpoint to report `enabled=true`, `primary=true`, `storageType=S3`, and manifest prefix `<prefix>/binlogs` before taking the baseline backup.

**Verify**: After the full backup succeeds, the scenario inserts a before-target marker row, captures a MySQL UTC timestamp, sleeps 2 seconds, inserts an after-target marker, executes `FLUSH BINARY LOGS`, and waits for sidecar archiver coverage with zero backlog. The pinned PITR verification uses `spec.pointInTime.mode=timestamp` and a sanity scalar that returns `1` only when baseline and before-target rows are present and the after-target row is absent. Success also requires `status.replayedThroughBinlog` and `Verified=True`.

**Timing**: Scenario timeout is 24 minutes; PITR rollout/archiver readiness has a 6-minute sub-budget, archive coverage has 3 minutes, and backup/verification waits each have 12 minutes. Cleanup deletes CRs, drops `chaos_s31_pitr`, restores the original `spec.backup` so PITR does not leak to later scenarios, and waits for a healthy baseline.

---

## Scenarios 32-39: Denial, restore, and Dragonfly-partition band

These eight scenarios add RBAC-denial, scoped-DNS, ordered-update-kill, planned-lag-timeout, restore-in-place, PITR-handoff, and Dragonfly-partition coverage. All are **full-profile-only** (omitted from `smoke`/`release`); §36 and §37 also run `ResetBeforeRunAll`. Each scenario below now records its live-run history on the k3d playground (2026-07-11) under "Actual live result" — prior failure evidence is preserved for the scenarios that needed a fix before their final pass.

Three of these (§32, §36, §38) exposed a real operator defect and shipped a minimal fix; see the **Observed defect and fix** notes under those sections. §33 exposed a real playground/tooling defect (a NetworkPolicy that this CNI evaluates post-DNAT), also documented under **Observed defect and fix**. §35 and §36's first live attempt each hit a scenario-only assertion-timing race (not an operator bug), fixed in the scenario's wait/verify logic — see the per-scenario notes below.

### 32. MFG Status-Write Denial During Emergency Promotion
**Category**: RBAC denial / status durability | **Risk**: High | **Profile**: full-only

**Hypothesis**: Denying the operator `patch`/`update` on `shipstream.io/mysqlfailovergroups/status` and then scaling the active primary to 0 still completes MySQL promotion (the promotion is SQL, not a CR write). The CR status stays stale during the denial; after the ClusterRole is restored, status catches up to the promoted site within 90s with no second failover.

**Injection**: Resolve the ClusterRole bound to ServiceAccount `bloodraven-playground/bloodraven`; remove `patch`/`update` from only `mysqlfailovergroups/status` (siblings like `mysqlbackups/status` keep their verbs via rule-splitting); scale the active MySQL Deployment to 0.

**Verify**: The peer becomes writable at the MySQL layer (direct probe, not the frozen CR); `bloodraven_failovers_total{target_site=<peer>}` increments; the operator logs a forbidden `update fg status` error. After RBAC restore: `status.activeSite`, `lastFailoverTarget`, and `promotionGtidExecuted` heal to the promoted site within 90s and Ready returns True; the old primary rejoins without `RecoveryBlocked`.

**Timing**: 8-minute timeout; 90s status-heal budget after RBAC restore.

**Cleanup**: LIFO RBAC restore (idempotent closure), scale old primary back to 1, global recover, wait out anti-flap cooldown to a healthy baseline.

**Observed defect and fix**: The operator wrote CR status only on transition events; a status write denied mid-failover was logged and dropped with nothing re-attempting it once the cluster stabilized. Fix (`internal/controller/runner.go`, `topology.go`): `updateCRStatus` now returns its error, the `StatusCallback` records it, and `Poll` re-fires the status callback every cycle while a prior write is known-failed (the writer already `DeepEqual`-skips no-ops, so a healthy cluster pays only a diff). Covered by `TestUpdateCRStatus_DeniedWriteReturnsErrorThenHeals` and `TestPoll_StatusRetryReFiresCallbackUntilCleared`.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T004152Z`) with the status-write-retry fix above already in place: the ClusterRole edit denied `patch`/`update` on `mysqlfailovergroups/status` only, the active primary was scaled to 0, and MySQL promoted the peer at the data layer while the CR status stayed frozen during the denial. After RBAC restore, `status.activeSite`, `lastFailoverTarget`, and `promotionGtidExecuted` healed to the promoted site inside the 90s budget with no second failover and no `RecoveryBlocked` on rejoin.

---

### 33. Scoped Cluster-DNS Outage
**Category**: Network / DNS isolation | **Risk**: Medium | **Profile**: full-only

**Hypothesis**: A NetworkPolicy that blocks only kube-dns egress from the active MySQL pod (allowing all other egress) is a DNS-resolution outage, not a pod partition: the operator's DNSEndpoint API writes still succeed, no split-brain occurs, and MySQL either stays stable or the sidecar self-fences cleanly.

**Injection**: Discover the `kube-system/kube-dns` ClusterIP *and* its backend pod IPs (from the `kube-system/kube-dns` Endpoints); run a busybox **canary** pod with a `nslookup` probe loop and apply the policy shape (egress `0.0.0.0/0` with `except: [<kube-dns ClusterIP>/32, <pod IP>/32, ...]`) to prove this CNI enforces the exception; only then apply the same policy to the active MySQL pod.

**Verify**: Across >2× `leaseTimeout`, writable count never exceeds 1 and no site is `RecoveryBlocked`; the operator log shows **no** forbidden `apply DNSEndpoint` error (that is §38); the DNSEndpoint stays readable via the Kubernetes API. Sidecar DNS-resolution failures are captured as best-effort evidence. Both a stable-activeSite and a clean self-fence-then-failover are acceptable end states.

**Timing**: 6-minute timeout (canary ~90s + 45s hold + recovery).

**Cleanup**: Remove the DNS NetworkPolicy (canary pod and policy torn down in the same step); global recover verifies no chaos NetworkPolicy remains and waits for Ready with a replicating peer.

**Observed defect and fix**: The first live run's canary proved this CNI (k3d's default) enforces NetworkPolicy *after* Service DNAT: the deny rule excepted only the kube-dns ClusterIP, but the packet's destination is already a CoreDNS pod IP by the time the CNI evaluates it, so the exception never matched and DNS kept resolving — the outage silently never happened. Fix (`internal/playground/kube/networkpolicy.go`): `BuildDNSEgressDenyPolicy`/`BuildDNSEgressDenyPolicyForSelector` now accept a list of deny IPs and except all of them; a new `DiscoverKubeDNSEndpointIPs` resolves the live CoreDNS backend pod IPs from the `kube-system/kube-dns` Endpoints, and the scenario excepts the ClusterIP plus every backend pod IP so the policy blocks DNS regardless of whether the CNI enforces pre- or post-DNAT. Covered by `TestBuildDNSEgressDenyPolicy`, `TestBuildDNSEgressDenyPolicyForSelector_DedupesAndSkipsEmpty`, `TestDiscoverKubeDNSEndpointIPs(_NoBackends)`, and `TestDenyDNSEgressAppliesAndReverts`.

**Actual live result**: First live attempt (`20260711T004244Z`) failed precheck's own canary step after 50.294s — the canary kept resolving DNS (`dns=ok`) through the full 45s hold, confirming the post-DNAT root cause above rather than silently proceeding against a no-op policy (see `live-33-final.md`). After the pod-IP-except fix (`DiscoverKubeDNSEndpointIPs` plus the multi-IP `Except` list), the scenario passed live on `20260711T005348Z`: the canary reached `dns=fail`, the outage stayed DNS-scoped across the hold window (writable count never exceeded 1, no `RecoveryBlocked`), and the operator logged no forbidden `apply DNSEndpoint` error.

---

### 34. Operator Kill During Ordered-Update WaitReplica
**Category**: Ordered update / operator resilience | **Risk**: High | **Profile**: full-only

**Hypothesis**: Killing the operator while an ordered update is held at `WaitReplica` does not double-roll all MySQL pods or cause `TOTAL LOSS`. The replacement operator, whose in-memory update phase was lost, re-derives remaining drift from the Deployment spec-hash mismatch and completes the roll.

**Injection**: Patch `spec.sites[*].resources.requests.memory`; wait for `status.updatePhase` to engage (preferably `WaitReplica`); apply an **ingress-only** deny NetworkPolicy to the standby MySQL pod so the operator's health check of the freshly-rolled standby fails (egress/replication stays open, satisfying the updater's replicating-standby precondition once lifted); force-delete the operator pod; after the replacement is Available, remove the hold.

**Verify**: Never 0 writable (no double-roll / `TOTAL LOSS` window) and never >1 writable throughout; final `status.updatePhase == ""`; both Deployments run the patched memory request; one writable + one read-only; no `TOTAL LOSS` operator log. Then restore original memory and wait for the revert roll to settle.

**Timing**: 12-minute timeout; 8-minute completion sub-budget.

**Cleanup**: Remove the standby hold policy (also swept by global recover), restore memory, wait for a healthy baseline.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T005458Z`) with no operator defect surfaced. The operator pod was force-deleted while `status.updatePhase` was held at `WaitReplica` behind the standby's ingress-only deny; the replacement operator re-derived the remaining drift from the Deployment spec-hash mismatch and completed the roll — writable count never dropped to 0 or rose above 1, `status.updatePhase` returned to empty, both Deployments landed on the patched memory request, and no `TOTAL LOSS` log fired. Re-run live on `20260711T033652Z` after the post-review scenario fix (threading the step context into `s34DeploymentsAt` instead of `context.Background()`): passed again with the same outcome.

---

### 35. Planned Switchover Lag-Timeout Rollback
**Category**: Planned failover state machine | **Risk**: Medium | **Profile**: full-only

**Hypothesis**: With `SOURCE_DELAY=60` on the target replica and a `maxLagWait=5s` annotation override, a planned switchover fences the source, waits for the target GTID to cover the fenced GTID, times out, and rolls back: `plannedFailover.phase=Failed` `reason=LagTimeout`, source stays active, and the failover history does not advance to the target.

**Injection**: Set `SOURCE_DELAY=60` on the target (both threads still running, so the operator's replica-health check stays true); write a marker on the source **after** the delay is active (so the fenced GTID cannot be covered in time); annotate `bloodraven.shipstream.io/planned-failover=<target>:maxLagWait=5s`.

**Verify**: `plannedFailover.phase` reaches terminal `Failed` with `reason=LagTimeout` (intermediate `Validating` is not asserted — it may not persist); `bloodraven_planned_failovers_total{result="failed_timeout"}` increments; `status.activeSite` stays the source; `lastFailoverTarget` does not advance to the target; the source becomes writable again (eventual — unfencing clears `super_read_only` first); the target stays read-only and replicating.

**Timing**: 8-minute timeout.

**Cleanup**: Clear `SOURCE_DELAY` on the read-only site, wait for the marker to replicate, drop `chaos_s35`, clear the annotation (already consumed by the terminal state machine).

**Actual live result**: First live attempt (`20260711T005733Z`) reached the correct `plannedFailover.phase=Failed{LagTimeout}` outcome in 7.007s, then failed the very next verify check 21ms later on `status.activeSite=""` — a scenario-side assertion-timing race, not an operator bug: rollback unfences the source in two MySQL-level steps (`super_read_only` clears before `read_only`), and `activeSiteLocked()` reports no active site in the gap between them (see `live-35-final.md`). After replacing the one-shot check with the `s35RollbackConverged` poll (90s budget, hard-fails immediately if `activeSite`/`lastFailoverTarget` actually advance to the target), the scenario passed live on `20260711T011112Z`: the source settled back to sole-writable/active, failover history never advanced to the target, and the target stayed read-only and replicating. Re-run live on `20260711T033630Z` after the post-review cleanup fix (clearing `SOURCE_DELAY` on the stashed rollback target rather than the observed read-only site): passed again with the same outcome.

---

### 36. RustFS Outage During restoreInPlace
**Category**: Restore-in-place / storage outage | **Risk**: High | **Profile**: full-only, `ResetBeforeRunAll`

**Hypothesis**: A per-schema in-place restore whose RustFS storage is scaled to 0 mid-run terminates `Failed` with the topology frozen (no emergency failover, no reclone). The untouched safe schema survives on the active primary. A genuine execution failure records `confirmTokenUsed` (the executed confirm), so the terminal `Failed` **holds** on the same confirm — the operator does not auto-re-arm the destructive restore; a retry requires a new monotonic confirm. Only a validation rejection (an invalid confirm that never ran) leaves `confirmTokenUsed` empty.

**Injection**: Configure an isolated RustFS profile under `e2e/36-…/<run-stamp>`; seed `chaos_s36_restore` and `chaos_s36_safe`; take a valid `MysqlBackup`; mutate only `chaos_s36_restore`; **confirm-token gate substep** — apply an invalid `confirm` and assert `Failed` with no Job and empty `confirmTokenUsed`; apply a fresh RFC3339 `confirm` with `source.mysqlBackupRef` and `loadOptions.includeSchemas=[chaos_s36_restore]`; wait until the restore Job is active (`phase=Restoring`, `jobName` set); scale the `rustfs` Deployment to 0.

**Verify**: Terminal `phase=Failed`; `targetSite` equals the active site at preflight; `status.activeSite` unchanged; no `bloodraven.shipstream.io/reclone-site` annotation; exactly one writable; no `RecoveryBlocked`; `chaos_s36_safe.canary` still reads `must-survive`; `confirmTokenUsed` equals the executed confirm and the terminal `Failed` holds across a sampling window (no auto-re-arm on the same confirm). Restore Job pod logs are captured as evidence of the S3/RustFS read failure.

**Timing**: 24-minute timeout.

**Cleanup**: Reverter scales RustFS back to 1 and waits available; remove `spec.restoreInPlace`; delete the restore Job; sweep the fixed-name in-place restore Job on every site (capturing pod logs first) so nothing leaks into the next run; delete backup CRs; restore the original backup spec; drop the scenario schemas. The inject step also sweeps a retained restore Job before arming a fresh `confirm`. If cleanup cannot prove the safe canary survived, `playground-chaos reset` is the documented remediation; `ResetBeforeRunAll` isolates the next attempt.

**Observed defect and fix**: The live runs exposed three real operator defects that compounded. (1) **Stale fixed-name Job inheritance** — the in-place restore Job name is fixed (`mysql-<fg>-<site>-inplace-restore`), so a terminal Job from a prior attempt survives across runs (and the playground reset). `inPlaceRestoring` read that leftover Job's phase and marked the *fresh* confirmed request `Failed` (with the prior backup path) without ever running it. (2) **Loader never enabled `local_infile`** — `util.loadDump()` loads via `LOAD DATA LOCAL INFILE`, which the server rejects unless `@@GLOBAL.local_infile=ON`; live primaries run the hardened MySQL-8 default `OFF`, so a per-schema restore DROPped the target schema in preflight and then failed inside the load — losing the schema. (3) **Auto-re-arm on failure** — once (1)/(2) let the Job genuinely fail, the terminal `Failed` left `confirmTokenUsed` empty (`stampTerminalFailure` never set it), so `confirmAdvances()` treated the unchanged `confirm` as fresh and re-armed the destructive restore (`Failed→Preflight→Fencing→Restoring`); the verify step then observed `Preflight`. This also violated the `RestoreInPlaceFailed`/`ConfirmTokenUsed` API contract, which says a failed restore holds until `confirm` advances. Fixes: `internal/controller/restore_inplace.go` stamps the `confirm` token on the Job (`bloodraven.shipstream.io/restore-confirm`) and binds each run to the confirm its Job was **accepted** under. A Job that is not this run's is removed only when it is *terminal* — an in-flight destructive Job is never deleted, whoever it belongs to, because killing a pod mid-`DROP`/mid-load would leave the schema half-gone and race a second loader over the wreckage; it is allowed to finish, and its terminal status is recorded against the confirm it actually ran with (so a `confirm` the user changed mid-run is not marked consumed by a run that never requested it — it re-arms its own run once the first is terminal). `stampTerminalFailure` records the executed `confirm` on a genuine execution failure (creds/build/Job) while leaving it empty for a pure validation rejection, so a failed-but-executed restore holds terminal and only a strictly-newer `confirm` retries (invalid-timestamp behavior preserved). `internal/controller/restore_script.py` treats `local_infile` as a **precondition of the load, not a best-effort nicety**: `_prepare_local_infile` enables and verifies it *before* the destructive preflight and aborts (exit 2, nothing dropped) if it cannot — the drop-then-fail hole that lost the schema is closed at the source — and the prior value is restored in a `finally` (a no-op on the verification path, whose throwaway mysqld already runs `--local-infile=1`), preserving the hardened posture outside the load window. Covered by `TestReconcileInPlaceRestore_StaleJobNotInherited`, `TestReconcileInPlaceRestore_MatchingFailedJobIsHonored`, `TestReconcileInPlaceRestore_RunningJobSurvivesConfirmChange`, `TestReconcileInPlaceRestore_TerminalTokenComesFromTheJob`, `TestReconcileInPlaceRestore_FailedJobRecordsTheJobsConfirm`, `TestInPlaceRestoreJobIsForConfirm`, `TestReconcileInPlaceRestore_FailedJobRecordsConfirmAndHolds`, `TestReconcileInPlaceRestore_TerminalFailedStaysIdleOnSameConfirm`, and `TestReconcileInPlaceRestore_TerminalFailedRearmsOnNewerConfirm`; the scenario samples the phase for 20s after `Failed` to prove no auto-re-arm.

**Actual live result**: Four live attempts, three diagnosed-and-fixed iterations before the final pass:

- `20260711T011139Z` failed in the inject step after 83ms with a stale `Failed` status from the *previous* substep's deliberately-invalid confirm — a scenario-side wait-helper race (`waitRestoreInPlaceActive` treated any observed terminal status as the fresh request's outcome, not just a fresh one), not an operator bug. Fixed by making the wait require positive evidence (`restoreInPlaceStatusChanged`) that the observed status post-dates the stale snapshot before honoring a terminal phase; see `live-36-final.md`.
- `20260711T012539Z` failed the same inject step after 4.092s, this time on a genuine terminal `Failed: Job has reached the specified backoff limit` — two compounding operator defects: (1) the in-place restore Job's fixed name let a leftover terminal Job from an earlier attempt be inherited and misattributed to the fresh confirmed request without ever running it, and (2) the loader never enabled `local_infile`, so every restore Job — leftover or fresh — failed inside `util.load_dump()` after already dropping the target schema. Fixed by stamping a `restore-confirm` annotation on the Job and binding each run to the confirm its Job was accepted under, and by making `local_infile` a verified precondition of the load; see **Observed defect and fix** above for the shipped behavior (review replaced the first cut's delete-on-mismatch with terminal-only removal, so an in-flight destructive Job is never killed) and `live-36-job-final.md` for the live diagnosis.
- `20260711T015238Z` reached the intended `Failed` outcome from the RustFS outage in 29.035s, then failed verify 12ms later: the terminal `Failed` had already auto-re-armed to `Preflight` because `stampTerminalFailure` never stamped `confirmTokenUsed` on a failure path, so `confirmAdvances(confirm, "")` treated the unchanged confirm as fresh. Fixed by recording the executed confirm on genuine execution failures (holding terminal on the same confirm) while leaving it empty on pure validation rejections; see `live-36-rearm-final.md`.
- `20260711T021732Z` passed live with all three fixes in place: the restore terminated `Failed` because of the RustFS outage, `targetSite`/`activeSite` and writable count were unaffected, no reclone annotation appeared, the safe canary (`chaos_s36_safe.canary`) still read `must-survive`, and `confirmTokenUsed` matched the executed confirm and held `Failed` across the 20s post-terminal sampling window with no auto-re-arm.
- `20260711T033531Z` re-ran live after review replaced the first cut's delete-on-mismatch Job handling with terminal-only removal: the restore Job log shows `BLOODRAVEN_LOCAL_INFILE_ENABLED prev=OFF` immediately before the drop (precondition verified ahead of any destructive step), then `BLOODRAVEN_LOAD_FAILED: While 'Opening dump': Failed to connect to rustfs...svc.cluster.local port 9000: Connection refused` from the RustFS outage, and `BLOODRAVEN_LOCAL_INFILE_RESTORED value=OFF` on the way out — the reviewed fix passed live with the same outcome as the prior final pass.

---

### 37. PITR Archive Handoff Across Failover
**Category**: PITR continuity across failover | **Risk**: Medium | **Profile**: full-only, `ResetBeforeRunAll`

**Hypothesis**: With PITR enabled, marker A archived on the old active and marker B archived on the new active — each in its own per-site manifest under the same prefix — are both restorable to a timestamp after B and before C. A pinned timestamp verification includes baseline+A+B and excludes C, proving archive continuity across an emergency failover.

**Injection**: Configure PITR under `e2e/37-…/<run-stamp>`; wait for active archiver readiness; seed baseline and take a full `MysqlBackup`; insert marker A on the current active, `FLUSH BINARY LOGS`, wait archive coverage; scale the active to 0 to force failover; wait for the peer to become active and its sidecar archiver `primary=true` on the **same** prefix; insert marker B, flush, wait coverage; capture the PITR target after B; insert marker C after the target, flush, wait coverage.

**Verify**: Both `manifest-<oldActive>.json` and `manifest-<newActive>.json` exist under `<prefix>/binlogs` with ≥1 file each (read straight from RustFS via `sidecar.ManifestKey`); the pinned `MysqlBackupVerification` (`mode=timestamp`) reports `ResultRow=1` for the sanity query (baseline+A+B present, C absent) and `ReplayedThroughBinlog` set.

**Timing**: 30-minute timeout.

**Cleanup**: Reverter scales the old active back to 1; delete backup + verification CRs; drop `chaos_s37_pitr` and wait for the drop to replicate; restore the original backup/PITR spec; wait for a healthy baseline, absorbing any anti-flap cooldown from the failover.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T021839Z`) with no operator defect surfaced. Both `manifest-<oldActive>.json` and `manifest-<newActive>.json` existed under the shared `<prefix>/binlogs` with the expected files, the sidecar archiver flipped `primary=true` on the new active after the forced failover, and the pinned timestamp `MysqlBackupVerification` reported `ResultRow=1` for baseline+A+B with C excluded and `ReplayedThroughBinlog` set — confirming archive continuity across the emergency failover.

---

### 38. DNSEndpoint Write Denial During Failover
**Category**: RBAC denial / DNS failover | **Risk**: High | **Profile**: full-only

**Hypothesis**: Denying the operator `create`/`patch`/`update`/`delete` on `externaldns.k8s.io/dnsendpoints` (keeping `get`/`list`/`watch`) then forcing an emergency promotion still promotes the peer and writes CR status normally (status is not denied), but the DNSEndpoint target stays stale. After RBAC restore the DNSEndpoint flips to the promoted site's LBIP within 90s with no second failover.

**Injection**: Capture the current DNSEndpoint target; remove the four write verbs from `dnsendpoints` only; scale the active MySQL Deployment to 0.

**Verify**: `status.activeSite` and `lastFailoverTarget` flip to the peer; the DNSEndpoint target stays stale (never the peer LBIP) during denial; the operator logs `DNS flip failed after successful promotion` / forbidden `apply DNSEndpoint`. After RBAC restore: the DNSEndpoint target becomes the promoted LBIP within 90s and neither `activeSite` nor `lastFailover` advances (no re-failover). Finally the old primary rejoins and the DNS target matches the final active site.

**Timing**: 10-minute timeout; 90s DNS-heal budget.

**Cleanup**: LIFO RBAC restore, scale old primary back to 1, wait for recovery, verify DNSEndpoint equals the final active site, healthy baseline.

**Observed defect and fix**: The operator applied the DNSEndpoint only at promotion/re-assert time; a promotion-time apply denied by RBAC was logged and dropped with no per-poll reconcile to retry it once the group stabilized. Fix (`internal/controller/topology.go`, `internal/platform/dns.go`): DNS is now reconciled level-triggered. `reconcileDNS` runs on every poll, derives the desired target from live topology (the single writable site, or a promotion this process just performed), compares it against the live DNSEndpoint (read back through the new optional `platform.DNSRecordReader`), and re-applies only on a real divergence — healing DNS without re-running promotion or touching MySQL. Deriving the target rather than memoizing it buys two properties a pending-target retry could not: the heal **survives an operator restart** (nothing needs to be remembered — the live record and the live topology are enough), and it can **never replay a superseded target** at a site that is now read-only. Every DNS write goes through one `applyDNS` helper, so `bloodraven_dns_flips_total{site}` counts exactly the target applied, and only when the record's value really changed. Covered by `TestReconcileDNS_HealsStaleRecordAfterOperatorRestart`, `TestReconcileDNS_NeverReplaysSupersededTarget`, `TestReconcileDNS_MatchingRecordIsNeverRewritten`, `TestReconcileDNS_DeniedPromotionFlipHealsWithoutRefailover`, `TestReconcileDNS_HealsWithoutRecordReadCapability`, and `TestDNSEndpointUpdater_CurrentDNSRecord`.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T022237Z`), with a first-cut version of the heal in place (a promotion-time pending target re-applied every poll): the four write verbs were denied on `dnsendpoints`, the active primary was scaled to 0, and `status.activeSite`/`lastFailoverTarget` flipped to the peer while the DNSEndpoint target stayed stale and the operator logged the forbidden `apply DNSEndpoint` error. After RBAC restore the DNSEndpoint healed to the promoted LBIP inside the 90s budget with no second failover, and the old primary rejoined with the DNS target matching the final active site. That first cut was superseded in review by the level-triggered `reconcileDNS` described above — same observable behavior for this scenario, plus restart-safety and no stale-target replay. The replacement was re-run live on `20260711T033413Z` (RBAC deny/restore captured in `s38-clusterrole-original.json`/`s38-clusterrole-patched.json`): passed again with the same outcome — DNS stayed stale during the denial and healed to the promoted LBIP within the 90s budget after RBAC restore, with no second failover.

---

### 39. Live Dragonfly Master Partition
**Category**: Dragonfly HA / partition | **Risk**: Medium | **Profile**: full-only

**Hypothesis**: A deny-all NetworkPolicy on only the Dragonfly master pod makes Bloodraven see it `Unreachable` and promote the surviving replica: `status.dragonfly.activeSite` flips, the active Dragonfly Service endpoints move to the promoted pod, `bloodraven_dragonfly_promotions_total` increments, and MySQL `status.activeSite` and failover counters are unchanged.

**Injection**: Seed a key on the current Dragonfly master and confirm it replicated to the peer; apply a deny-all (ingress+egress) NetworkPolicy selecting only the master site's Dragonfly pod labels. MySQL pods and Services are untouched.

**Verify**: The old master goes `Unreachable`; `status.dragonfly.activeSite` flips to the peer; the seeded key reads back on the new master; the active Dragonfly Service endpoints converge to only the promoted pod; `dragonfly_promotions_total{result="success"}` increments; MySQL `status.activeSite` is unchanged and no MySQL failover metric increments.

**Timing**: 8-minute timeout.

**Cleanup**: Remove the partition; wait for the old master to rejoin as a healthy replica (`linkStatus=up`). If it returns a stale master and does not reconfigure within the settle budget, force-delete only the old Dragonfly pod (documented conservative recovery) so the StatefulSet recreates it cleanly.

**Actual live result**: Passed on the k3d playground on the first live attempt (`20260711T022509Z`) with no operator defect surfaced. The deny-all NetworkPolicy on the master's Dragonfly pod drove it `Unreachable`, `status.dragonfly.activeSite` flipped to the peer, the seeded key read back on the new master, the active Dragonfly Service endpoints converged to only the promoted pod, and `bloodraven_dragonfly_promotions_total{result="success"}` incremented — while MySQL `status.activeSite` and failover counters stayed unchanged throughout.

---

### D6b. Dragonfly Rolling Image Update
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
- Direct `SHOW REPLICA STATUS` on every non-active follower reports both threads running and `Source_Host=mysql-playground-<new-active>-internal.bloodraven-playground.svc.cluster.local`; this explicitly covers the demoted old primary and the reader.
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

### 40. Reader Data Loss and Auto-Clone {#40-reader-data-loss-and-auto-clone}
**Category**: Reader recovery and endpoint safety | **Risk**: High

**Hypothesis**: Losing only the dedicated `read-only` reader's local data does not affect failover-group readiness or the writable primary. The operator removes the unhealthy reader from its client Service, recreates it from the confirmed active primary, converges it to a direct source, and republishes it only after threads and lag are healthy.

**Injection**: `make chaos-run SCENARIO=40-reader-data-loss-reclone`. The scenario verifies that the reader is on the `zone-reader` worker, scales its Deployment to zero, deletes and observes replacement of its PVC UID, then scales it back to one. This deterministic local-PV replacement is the playground interpretation of reader node/storage loss; it avoids relying on literal node deletion and cloud volume behavior.

**Verify**:

- Exactly one spec site has effective role `read-only`; its pod is on a dedicated `zone-reader` worker.
- A marker written to the active primary is visible on the reader before loss and after recovery.
- A continuous observer records any group `Ready` invariant violation during scale-down, empty-datadir detection, clone, or catch-up; `stopAndCheck` reports the first recorded error before the step completes.
- Operator logs contain `starting bootstrap` with `source=auto-clone`, `donor=<captured-active>`, `recipient=reader`, and `donorHost=mysql-playground-<captured-active>-internal.bloodraven-playground.svc.cluster.local`.
- The active site never changes. Final reader status is `State=read-only`, `SourceConvergenceState=Converged`, direct `SourceHost`, known lag at or below `readOnlyMaxLagSeconds`, and live MySQL IO/SQL threads running.
- The reader client Service exposes only MySQL and has no serving EndpointSlice target while unhealthy. After convergence it has exactly the replacement reader pod.
- The reader internal Service remains `ClusterIP`, exposes MySQL and sidecar, has `publishNotReadyAddresses=true`, does not use the healthy selector, and publishes the replacement pod while it is unready.

**Cleanup**: The PVC is intentionally replaced. The Deployment scale is restored immediately and the scenario does not finish until direct healthy replication and the client endpoint return. Standard runner forensics and cleanup remain active on every failure path.

---

### 41. Reader Availability During Unplanned Failover {#41-reader-availability-during-unplanned-failover}
**Category**: Reader recovery and endpoint safety | **Risk**: Medium

Issue #115 chaos proposal R3.

**Hypothesis**: During the unplanned-failover window the reader keeps serving (stale) reads — staleness is allowed, availability is required. After promotion, source convergence repoints the reader directly at the new primary; at no point does the reader apply events relayed through the demoted primary.

**Injection**: `make chaos-run SCENARIO=41-reader-availability-during-failover`. Seeds a marker on the active primary and confirms it reaches the reader, then scales the active primary's Deployment to 0 (sustained outage — pod-kill respawns in seconds and tests nothing).

**Verify**:

- A continuous observer answers a marker `SELECT` against the reader every 500ms throughout the window. One reconnect is allowed per tick (idle port-forward tunnels can drop); two consecutive failures fail the scenario. At least 20 successful reads are required so the observer provably covered the window.
- `status.activeSite` flips to the promotable standby and nowhere else.
- The reader's `SourceHost` history only ever moves old-primary → new-primary: any third host, or a return to the demoted primary after the repoint, fails the run.
- The reader never enters `SourceConvergenceState=Blocked` or `RecoveryBlocked`.
- Final reader status passes the serving contract against the new primary (`read-only`, replicating, `Converged`, lag ≤ `readOnlyMaxLagSeconds`), a marker written on the new primary replicates to the reader, and the client Service publishes exactly the reader pod again.

**Cleanup**: The executor restores the old primary's scale; scenario cleanup reuses the scenario-08 auto-reclone path if the returning primary is divergent.

---

### 42. Reader Stall Does Not Degrade the Group {#42-reader-stall-does-not-degrade-the-group}
**Category**: Reader recovery and endpoint safety | **Risk**: Low

Issue #115 chaos proposal R4. Smoke-profile member: fast, no clone wait.

**Hypothesis**: A wedged OLAP reader is invisible to the failover machinery — lag grows unbounded with zero group-level effect. The only reactions are endpoint shedding and alertable (not page-able) per-site status and metrics.

**Injection**: `make chaos-run SCENARIO=42-reader-stall-no-group-degradation`. Applies `CHANGE REPLICATION SOURCE TO SOURCE_DELAY=600` on the reader in a single `STOP REPLICA; ...; START REPLICA` batch. SOURCE_DELAY — not `STOP REPLICA SQL_THREAD` — is the one lag injection the operator will not heal: both threads keep running on the correct source, so convergence sees a converged-but-lagging reader instead of a stopped one (same reasoning as scenario 14).

**Verify** (soak of 3× `maxLagSeconds`, one primary write per second to grow applied lag):

- Group `Ready` stays `True` every tick; `status.activeSite` and `status.lastFailover` never change (no failover, no anti-flap cooldown consumed).
- The promotable standby keeps replicating.
- The reader stays `read-only` and never enters `SourceConvergenceState=Blocked` or `RecoveryBlocked`.
- Observed reader lag exceeds `readOnlyMaxLagSeconds` and the client Service sheds the reader endpoint.
- `bloodraven_replication_lag_seconds{site=reader}` exceeds the reader threshold while `bloodraven_replication_source_state{site=reader,state="converged"}` stays 1 — the metrics report the stall honestly.

**Rollback**: `SOURCE_DELAY=0`; the reader must catch up, pass the serving contract, and rejoin its client Service. Cleanup always clears the delay and drops the soak database through the primary.

---

### 43. Writable Reader Fence {#43-writable-reader-fence}
**Category**: Reader recovery and endpoint safety | **Risk**: Medium

Issue #115 chaos proposal R5 — the regression gate for role semantics under the worst input (spec gap #2).

**Hypothesis**: A reader that somehow becomes writable is fenced like a `dr-only` loser without debounce, its errant GTID trips the convergence containment gate instead of a silent repoint, and it is never a promotion target — not even by explicit admin request.

Two independent actors can close the writable window and both are correct: the operator's poll loop (`pollInterval`, 2s in the playground) fences any writable non-promotable site without debounce, and the reader's own sidecar fencing monitor (`PEER_CHECK_INTERVAL`, 5s) self-fences on topology mismatch. Whichever ticks first wins, so the scenario asserts the invariant plus a fence from *either* actor rather than racing one specific log line (issue #119).

**Injection**: `make chaos-run SCENARIO=43-writable-reader-fence`. One multi-statement batch on the reader: `super_read_only=OFF`, `read_only=OFF`, then an errant `CREATE DATABASE`/`CREATE TABLE`/`INSERT` (reader-scoped split-brain). Later, `STOP REPLICA` on the reader forces the convergence invariant to act on the diverged follower.

**Verify**:

- `super_read_only` is back `ON` on the reader within 60s, and the fence was deliberate: either the operator logged `fenced writable non-promotable site` with `site=<reader>`, or the reader's sidecar logged `SELF-FENCING: topology mismatch`. Group `Ready` and `activeSite` untouched.
- The fence costs the reader nothing but its writability: both replication threads are still running 5s later. A fence that kills the site's own `system user` threads strands it — permanently, once it is diverged enough for source convergence to be `Blocked`.
- That check is then repeated against the sidecar deterministically: the operator is scaled to 0, the reader is made writable again (no write this time), and the sidecar's own `SELF-FENCED` path is what restores `super_read_only`. Only the sidecar kills connections — the operator's fence does not — so without forcing the actor this way the regression would only be exercised on the runs the sidecar happened to win. The operator is restored before the remaining steps, and its log tailer rebound to the replacement pod. Skipped when the sidecar already fenced during the observe step (established from its terminal `SELF-FENCED` line, not from whichever actor's log line was matched first): the path is covered, and a second forced fence would deadlock — rearming a writable instance clears the sidecar's topology cache, and with the operator down no peer view is new enough to repopulate it.
- Annotating a planned failover targeting the reader lands `status.plannedFailover.phase=Failed` carrying the "only primary-candidate sites may be promoted" role error, and `bloodraven_planned_failovers_total{target_site=<reader>,result="rejected"}` increments.
- After `STOP REPLICA`, the operator logs `replication source convergence blocked`, the reader reaches `SourceConvergenceState=Blocked` with reason `GTIDDiverged` (never a silent restart), `bloodraven_replication_source_state{site=reader,state="blocked"}` flips to 1, and the reader is shed from its client Service.

**Cleanup**: The scenario reconciles the errant transactions by committing them as empty transactions on the active primary (refusing if the errant set carries any UUID other than the reader's — that demands `kubectl bloodraven reclone`), waits for the convergence invariant to restart the reader, then drops the scenario database through the primary so replication removes the errant row itself.

---

### 44. Reader Source Convergence Invariant {#44-reader-source-convergence-invariant}
**Category**: Reader recovery and endpoint safety | **Risk**: Low

Issue #115 chaos proposal R9.

**Hypothesis**: Direct-source convergence is a periodic invariant of the poll loop, not a one-shot switchover event — *any* wrong-source state (operator drift, a partially-applied runbook, a pre-fix chained reader left over from an upgrade) heals with no failover. The divergent-wrong-source counterpart (must go `Blocked`, never silently repoint) is scenario 43's final phase.

**Injection**: `make chaos-run SCENARIO=44-reader-source-convergence-invariant`. Repoints the reader's replication at the promotable standby (`CHANGE REPLICATION SOURCE TO SOURCE_HOST=<standby-internal>` in one batch; credentials and GTID auto-positioning carry over), producing a working chained topology.

**Verify**:

- Operator log contains `replication source convergence started` with `site=<reader>`, `currentSource=<standby-internal>`, `expectedSource=<active-internal>`, followed by `replication source convergence complete` back onto the active primary — the documented log-schema events.
- Reader serving status and live `SHOW REPLICA STATUS` converge back onto the active primary; `bloodraven_replication_source_state{site=reader,state="converged"}` is 1.
- Group `Ready` stays `True`; `status.activeSite` and `status.lastFailover` are untouched.
- A marker written on the primary replicates to the reader over the repaired direct channel, and the client Service publishes exactly the reader pod.

**Cleanup**: None needed for the wrong-source state itself — repairing it is exactly what the invariant does. Defensive cleanup restarts the reader's replication channel if the scenario failed with it stopped.

---

### Planned reader scenarios (not yet automated)

Issue #115 proposals R6-R8 remain manual/full-profile follow-ups:

- **45 (R6) — Bootstrap ordering with reader present**: fresh 3-site deploy must clone the promotable standby and reach failover-capable `Ready=True` *before* the reader clones; a mid-clone reader failure retries without blocking bootstrap. Run manually via `./playground/reset-mysql.sh` + `./playground/setup.sh` while watching `starting bootstrap` ordering in operator logs.
- **46 (R7) — Primary dies mid-reader-clone**: trigger scenario 40's re-clone, hard-kill the donor primary while `CLONE INSTANCE` is in flight; failover must proceed un-wedged and the clone state machine must retry against the new primary. The playground's small dataset makes the in-flight window sub-second, so automation needs an artificial data volume first.
- **47 (R8) — `externalTrafficPolicy: Local` endpoint semantics**: probe the reader NodePort (30306) from reader and non-reader nodes across pod-down/up; requires per-node canary pods and is infra-dependent (kube-proxy implementation).

---

## 48. Keyring Seal And Rotation

**Automated**: `make chaos-run SCENARIO=48-keyring-seal-and-rotation`

**Quarantined from every batch profile.** It needs a playground brought up
with TLS and `spec.encryptionAtRest` enabled, which is not the baseline
the other scenarios assume:

```bash
./playground/setup.sh
./playground/enable-encryption.sh --fresh   # wipe + encrypt from birth
make chaos-run SCENARIO=48-keyring-seal-and-rotation
```

`--fresh` wipes MySQL first so every tablespace is encrypted from birth.
Without it the script converts in place, which leaves pre-existing tables
plaintext — fine for exercising the keyring lifecycle, not representative
of a production adoption.

**Hypothesis**: every site reports `phase=Sealed` with a read-only keyring
component; `ALTER INSTANCE ROTATE INNODB MASTER KEY` is rejected on a
sealed site; annotating the replica for rotation mints escrow version N+1
and returns it to `Sealed` with data intact; annotating the active primary
is refused with a `KeyringRotationRefused` event.

**What it actually proves**, in order:

1. **Sealed means sealed.** Checks `performance_schema.keyring_component_status`
   on each site for `Read_only=Yes`, not just the operator's status field.
   A rendering bug could otherwise leave a writable keyring behind a Secret
   mount and the operator would call it sealed.
2. **The key is not on the data volume.** MySQL's own `Data_file` must be
   outside `/var/lib/mysql`, and the rendered pod must project the keyring
   from a Secret rather than an emptyDir. This is the whole at-rest claim:
   a stolen PVC must not carry the key that decrypts it.
3. **The seal is enforced by the engine.** An ad-hoc
   `ALTER INSTANCE ROTATE INNODB MASTER KEY` must fail. This is what makes
   "no unescrowed key can exist in the steady state" true rather than
   aspirational.
4. **Rotation on the primary is refused.** Rotation is the one lifecycle
   operation whose failure window would cost data rather than a re-clone,
   and only on the primary.
5. **Rotation on a replica is safe and complete.** A new immutable escrow
   version is minted, the previous version survives for rollback, the
   site returns to `Sealed`, and the data still decrypts.

**Expected duration**: ~5-8 minutes (dominated by the pod roll for the
unseal and the roll back to sealed).

**Manual observation** while it runs:

```bash
watch -n2 "kubectl -n bloodraven-playground get mysqlfailovergroup playground \
  -o jsonpath='{range .status.encryptionAtRest.sites[*]}{.name}{\"\t\"}{.phase}{\"\t\"}{.keyringSecret}{\"\n\"}{end}'"

./playground/enable-encryption.sh --status
```

**Known follow-ups** not automated here:

- **Clone into an encrypted recipient**: covered implicitly whenever a
  reclone runs on an encrypted playground (the operator unseals the
  recipient first), but there is no dedicated scenario asserting the
  unseal → clone → re-escrow → seal ordering.
- **Escrow Secret loss**: deleting a sealed site's escrow Secret should
  raise `KeyringEscrowMissing` and phase `Failed`. Destructive and
  easy to get wrong by hand; see
  `docs/docs/runbooks.mdx#keyring-escrow-lost`.
- **Node loss with an unsealed site**: verifies the bounded-loss claim
  (a lost keyring costs a re-clone, never data). Needs real node
  eviction, not a pod kill.

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
