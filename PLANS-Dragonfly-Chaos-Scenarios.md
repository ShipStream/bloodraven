# Dragonfly Chaos Scenarios (Planning)

> **Partially runnable.** Core Dragonfly scenarios are registered in
> `cmd/playground-chaos` and documented in `playground/chaos-scenarios.md`.
> D3/D4/D5/D7/D11 passed live k3d runs on May 3, 2026. Remaining scenarios
> here are backlog/regression ideas informed by known engine and operator bugs
> documented in `PLANS-Dragonfly-Upstream-Discoveries.md`.

---

## Prerequisites

1. `spec.dragonfly.enabled: true` in the playground `MysqlFailoverGroup`.
2. `./playground/setup.sh` with Dragonfly manifests deployed.
3. Healthy baseline: one Dragonfly master per active site, one replica per candidate site,
   `status.dragonfly.phase: Ready`.

Relevant config knobs: `maxSyncWait`, `onSyncTimeout`, `replicas.perSite`, polling interval
for Dragonfly health checks.

Runnable today through `cmd/playground-chaos`:

- D3: `22-planned-dragonfly-switchover`
- D4: `26-planned-dragonfly-sync-timeout-proceed`
- D5: `24-emergency-mysql-dragonfly-down`
- D7: `23-dragonfly-master-kill`
- D11: `25-operator-restart-mid-dragonfly-failover`

---

## Scenarios

---

### D1. Dragonfly Replica Attachment (LOADING State)
**Category**: Bootstrap correctness | **Risk**: Low

**Hypothesis**: When Bloodraven issues `REPLICAOF <master>` to a fresh Dragonfly pod, the pod
enters LOADING state. Any `CONFIG SET` calls that happen after `REPLICAOF` (e.g. clearing
`snapshot_cron`) will fail with "Dragonfly is loading the dataset in memory" and leave the pod
unconfigured.

**What to verify**: The operator clears `snapshot_cron` and any other per-role config
**before** issuing `REPLICAOF`, not after. The replica pod reaches a stable `role=replica` label
and `master_link_status=up` without requiring manual intervention.

**Regression guard**: This is the exact bug fixed in dragonfly-operator PR #505 (Issue #498).
Any refactor of the role-assignment sequence must preserve the pre-`REPLICAOF` ordering.

**Verify**:
- `INFO REPLICATION` on replica shows `master_link_status:up`, `master_sync_in_progress:0`.
- No operator log errors about CONFIG commands failing during LOADING.
- `snapshot_cron` is empty on replica, non-empty on master (if configured).

---

### D2. Snapshot Cron Restored After Promotion
**Category**: Configuration correctness | **Risk**: Low

**Hypothesis**: When a replica is promoted to master (during planned or emergency failover),
the snapshot cron schedule that was cleared on replica attachment must be restored.

**Injection**: Trigger a planned failover from site A to site B. Site B's Dragonfly pod was
a replica with snapshot_cron cleared.

**Verify**:
- Post-promotion: `CONFIG GET snapshot_cron` on the new master returns the configured schedule.
- Old master (now replica): `CONFIG GET snapshot_cron` returns empty.
- No snapshot silently stops running after failover.

**Source**: dragonfly-operator PR #513. Silent snapshot loss after promotion is a known
upstream gotcha.

---

### D3. Planned MySQL + Dragonfly Coordinated Failover (implemented: `22-planned-dragonfly-switchover`)
**Category**: Core integration | **Risk**: Medium

**Hypothesis**: A planned failover coordinates MySQL and Dragonfly promotion atomically.
Sequence: drain Dragonfly traffic on old master → wait for replica sync (up to `maxSyncWait`)
→ promote Dragonfly replica → promote MySQL replica → update DNS → re-enable traffic.

**Injection**: Trigger a planned site failover via the standard Bloodraven failover mechanism.

**Verify**:
- Dragonfly master switches to the target site before or at the same time as MySQL.
- No window where MySQL points to site B but Dragonfly master is still on site A.
- `status.dragonfly.activeSite` matches `status.activeSite` after convergence.
- Client connections to Dragonfly are disrupted for at most the `maxSyncWait` duration.
- `INFO REPLICATION` on new master shows no `master_sync_in_progress`.

**Timing note**: If `onSyncTimeout: proceed`, failover completes even if Dragonfly sync is
incomplete. Document observed session loss window.

---

### D4. Dragonfly Sync Timeout During Planned Failover (implemented: `26-planned-dragonfly-sync-timeout-proceed`)
**Category**: Degraded failover | **Risk**: Medium

**Hypothesis**: If Dragonfly replica cannot sync within `maxSyncWait`, and `onSyncTimeout` is
`proceed`, MySQL failover completes anyway and Dragonfly data is partially lost. The operator
emits a warning event but does not block MySQL promotion.

**Injection**:
1. Patch `spec.dragonfly.plannedFailover.maxSyncWait=1ms` and `onSyncTimeout=proceed`.
2. Scale the target Dragonfly StatefulSet to 0.
3. Trigger a planned failover.

**Verify**:
- Planned failover reaches `Succeeded`.
- MySQL promotes successfully.
- `status.plannedFailover.dragonfly.SessionsPreserved=false`.
- Reason is `DragonflySyncTimeout` or `DragonflyPromotionFailed`.
- `bloodraven_dragonfly_promotions_total{result="failed"}` increments.

---

### D5. Dragonfly Unreachable During Emergency MySQL Failover (implemented: `24-emergency-mysql-dragonfly-down`)
**Category**: Emergency fallback | **Risk**: High

**Hypothesis**: When MySQL requires emergency failover and Dragonfly is completely unreachable
(pod down, network partition), MySQL failover must NOT be blocked indefinitely. Bloodraven
proceeds with MySQL promotion and leaves Dragonfly in a degraded state to be reconciled
afterward.

**Injection**:
- Scale Dragonfly deployments on both sites to 0.
- Trigger emergency MySQL failover (kill active MySQL site).

**Verify**:
- MySQL failover completes within normal timing (~37s).
- Operator logs indicate Dragonfly was unreachable and failover proceeded anyway.
- `status.dragonfly.phase` reflects a degraded/unknown state.
- After Dragonfly pods are restored, operator reconciles topology without manual intervention.

**Critical**: Dragonfly must never be on the critical path for emergency MySQL failover.

---

### D6. Dragonfly Rolling Update (One Pod at a Time) (implemented: `27-dragonfly-rolling-image-update`)
**Category**: Zero-downtime operations | **Risk**: Medium

**Hypothesis**: When the Dragonfly image or config changes, pods roll one at a time — replica
first, then master (with promotion). No window where all Dragonfly pods are simultaneously
unavailable.

**Injection**: Update `spec.dragonfly.image` to a new tag.

**Verify**:
- Replica pod updates first. Master pod continues serving.
- Master pod updates second, triggering a brief Dragonfly promotion to the updated replica.
- At no point are both pods terminating simultaneously.
- After rollout: both pods running new image, master serving, replica replicating.

**Regression guard**: Upstream dragonfly-operator deleted all stale replicas in one pass
(Issue #504, PR #507). Bloodraven must delete exactly one pod per reconcile pass and requeue.

**Live status**: passing in k3d as of May 3, 2026: `PASS 27-dragonfly-rolling-image-update`,
duration 16.159s. The scenario patches `spec.dragonfly.image` to the digest reference already cached
by the running Dragonfly pod, so it exercises the image-rollout path without relying on an external
image pull. The operator updates the non-active site first, waits for its StatefulSet rollout counters,
promotes that updated replica, and only then updates the old active site's StatefulSet.

### D6a. Snapshot-Restore Planned Dragonfly Upgrade (Short Outage Accepted) (implemented: `29-dragonfly-snapshot-upgrade`)
**Category**: Planned maintenance | **Risk**: Medium

**Hypothesis**: For rare Dragonfly image upgrades where a short cache/session outage is acceptable,
Bloodraven can use Dragonfly's native snapshot directory support (`--dir=s3://bucket[/prefix]`) as a
simpler alternative to true rolling upgrade. The operator should quiesce traffic, issue `SAVE` on the
active master, recreate the Dragonfly pods with the new image, let the new active pod restore from S3,
then attach other sites as replicas.

**Implementation**: `spec.dragonfly.snapshot.dir` renders as Dragonfly `--dir`, and
`spec.dragonfly.snapshot.serviceAccountName` is assigned to the Dragonfly pods so IRSA or equivalent
cloud IAM can grant S3 access without static credentials. The one-shot state machine is annotation
driven: set `bloodraven.shipstream.io/dragonfly-snapshot-upgrade=<target-image>`. Bloodraven records
`status.dragonfly.upgrade`, sheds the active Service endpoint, issues `SAVE`, restarts the active pod on
the target image, waits for it to finish loading/restoring as master, updates replica StatefulSets,
issues `REPLICAOF` as needed, and restores traffic only after replicas are linked.

**Live status**: passing in k3d as of May 3, 2026: `PASS 29-dragonfly-snapshot-upgrade`,
duration 4m26.292s. The baseline playground manifest still omits `spec.dragonfly.snapshot`
so ordinary Dragonfly startup does not depend on RustFS/S3. Scenario 29 provisions and validates the
snapshot backend itself: it ensures the RustFS `dragonfly` bucket exists through the runner's
SigV4 S3 client, temporarily patches `spec.dragonfly.snapshot`, waits for Dragonfly pods to run with
`--dir=s3://dragonfly/playground`, then requests the upgrade.

**Injection**: Annotate the MFG with a requested Dragonfly image:

```bash
kubectl -n bloodraven-playground annotate --overwrite mysqlfailovergroup playground \
  bloodraven.shipstream.io/dragonfly-snapshot-upgrade=docker.dragonflydb.io/dragonflydb/dragonfly:<target>
```

**Verify**:
- Active Service has no endpoint while restore is in progress; clients see a planned outage rather than
  writes to a half-restored pod.
- `SAVE` completes before the old active pod is terminated.
- Replacement active pod starts with the new image and restores from the configured S3 snapshot dir.
- Replicas attach only after restore completes, avoiding the D13 restore-after-replica-attach data-loss
  trap.
- Final state is one master, linked replicas, all pods on the new image.

---

### D7. Dragonfly Master Kill (Without MySQL Failover) (implemented: `23-dragonfly-master-kill`)
**Category**: Dragonfly-only HA | **Risk**: Low

**Hypothesis**: Killing the Dragonfly master pod (without touching MySQL) causes the operator
to promote the replica and update the Dragonfly service. MySQL is unaffected.

**Injection**: `kubectl -n bloodraven-playground delete pod <dragonfly-master-pod>`

**Verify**:
- `status.dragonfly.activeSite` flips within one reconcile cycle.
- Dragonfly service now routes to the promoted pod.
- MySQL `status.activeSite` unchanged.
- Old master pod (after respawn) re-attaches as replica without manual intervention.

---

### D8. Dragonfly Full Resync Latency Impact on Master
**Category**: Performance / observability | **Risk**: Low

**Hypothesis**: During initial Dragonfly replica attachment or after a topology change, full
resync causes ~80x write latency increase on the master (upstream Issue #4809). This should be
observable in metrics and should not trigger false MySQL health alarms.

**Injection**: Force a full Dragonfly resync by restarting a replica pod with a large dataset.

**Verify**:
- Dragonfly master write latency spikes during sync (document p99).
- MySQL health checks are unaffected (Dragonfly latency does not cascade to MySQL checks).
- Operator does not misinterpret the latency spike as a Dragonfly failure.
- Resync completes and latency returns to baseline.

---

### D9. Silent Key Loss Validation After Initial Sync
**Category**: Data integrity | **Risk**: Low

**Hypothesis**: After initial Dragonfly replication sync completes, the replica DBSIZE should
match the master DBSIZE. Upstream Issue #5508 documents silent key loss with no error
indication.

**Injection**: Load a known dataset onto the Dragonfly master. Attach a replica. Wait for
`master_sync_in_progress:0`. Compare DBSIZE.

**Verify**:
- `DBSIZE` on replica == `DBSIZE` on master (within eventual-consistency window).
- Operator logs or status surface a mismatch if DBSIZE diverges beyond a threshold.
- Spot-check a sample of keys to verify values match, not just count.

**Note**: This is a validation test, not a failure injection test. It guards against silent
replication data loss which has been observed in the Dragonfly engine.

---

### D10. Orphaned Downstream Replicas After Topology Change
**Category**: Topology correctness | **Risk**: Medium

**Hypothesis**: When the Dragonfly master is changed (old master receives `REPLICAOF <new>` to
become a replica), its existing downstream replicas are orphaned — they do not automatically
reconnect. Bloodraven must explicitly re-issue `REPLICAOF` on each replica after topology
changes.

**Injection**: Trigger a failover that promotes a replica to master. The old master receives
`REPLICAOF <new-master>`. If there were multiple replicas, verify they all reconnect.

**Verify** (relevant if `replicas.perSite > 1`):
- All replicas show `master_link_status:up` pointing to the new master.
- No replica is stuck pointing to the old master or showing `master_link_status:down`.
- This survives operator restart (topology is re-derived from CR status, not in-memory state).

**Source**: Upstream Issue #2044 (OPEN, architectural). Bloodraven must handle explicit
reconnection — it cannot rely on Dragonfly to self-heal after master topology changes.

---

### D11. Operator Restart Mid-Dragonfly-Failover (implemented: `25-operator-restart-mid-dragonfly-failover`)
**Category**: Operator resilience | **Risk**: Medium

**Hypothesis**: If the operator is killed while a Dragonfly failover is in progress, the
restarted operator converges correctly — either completing the in-progress promotion or
detecting an already-promoted master and reconciling from that state.

**Injection**:
1. Patch the Dragonfly sync budget to 45s and scale the planned-failover target's Dragonfly
   StatefulSet to 0, creating a deterministic `WaitingForDragonflySync` window.
2. Trigger a planned failover.
3. Kill the operator after observing the fresh in-flight Dragonfly sync phase.
4. Restore the target Dragonfly StatefulSet so the replacement operator can complete the failover.

**Verify**:
- Planned failover reaches `Succeeded` with the original target.
- MySQL and Dragonfly active sites both equal that target.
- No split-brain: exactly one Dragonfly pod has `role=master` after convergence.
- No manual intervention required.

---

### D12. Dragonfly Maxmemory Exhaustion Mid-Failover
**Category**: Resource limits | **Risk**: Medium

**Hypothesis**: If the Dragonfly replica's maxmemory is too low to hold a full copy of the
master's dataset during initial sync, the replica fails to sync and enters a retry loop. The
operator should surface this clearly and not loop infinitely.

**Injection**: Set `spec.dragonfly.maxMemoryMb` to a value lower than the master's current
dataset size. Trigger a replication attachment.

**Verify**:
- Replica logs show OOM or eviction during sync.
- Operator surfaces a Warning event or degraded status condition rather than silently retrying.
- System does not enter an unbreakable crash loop.
- Increasing maxMemoryMb (spec update) allows recovery without manual pod deletion.

**Source**: Upstream Issue #5845 — Dragonfly starts empty when maxmemory is hit during load,
with no error surfaced to the caller.

---

### D13. DFLY LOAD Before Replica Attach (Snapshot Restore Path)
**Category**: Restore correctness | **Risk**: Medium

**Hypothesis**: If Bloodraven uses `DFLY LOAD` to restore a snapshot onto the master before
replicas attach, all replicas will have the full dataset after initial sync. If `DFLY LOAD` is
issued while replicas are already attached, those replicas silently miss the loaded data.

**Injection (safe path)**: Load snapshot, then attach replicas. Verify DBSIZE matches.

**Injection (unsafe path)**: Attach replicas, then load snapshot on master. Verify DBSIZE
diverges and operator detects / corrects this.

**Verify**:
- Safe path: replica DBSIZE == master DBSIZE after sync.
- Unsafe path: operator detects divergence and triggers full resync on affected replicas.

**Source**: Upstream Issues #6739 and #2975. `DFLY LOAD` bypasses the replication journal —
replicas only see journal-based changes, not snapshot-loaded keys.

---

## Execution Notes

- Implemented scenarios are integrated into `playground/chaos-scenarios.md`; backlog
  scenarios should be moved there when they become runner-backed.
- Scenarios D1, D2, D6, D9, D10 are validation/regression tests (low risk, run first).
- Scenarios D3, D5, D7 are the core integration scenarios (run after D1/D2 pass).
- Scenarios D4, D8, D11–D13 are edge cases (run last, may require reset between runs).
- After any scenario that leaves Dragonfly in a degraded state, verify `status.dragonfly.phase`
  returns to `Ready` before proceeding to the next scenario.
- Use `kubectl -n bloodraven-playground get mysqlfailovergroup playground -o yaml` to inspect
  both `status.activeSite` and `status.dragonfly` simultaneously.
