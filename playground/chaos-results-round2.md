# Bloodraven Chaos Testing Results — Round 2

**Date**: 2026-04-14
**Environment**: k3d cluster `bloodraven` (1 server + 2 agents), playground config (pollInterval=2s, failureThreshold=3, failoverCooldown=30s, sidecar leaseTimeout=20s, peerCheckInterval=5s)
**Operator**: Built from commit 5ef9301 + 3 local fixes applied during testing (see Bugs Fixed section)

## Summary

| # | Scenario | Result | Notes |
|---|----------|--------|-------|
| 1 | Clean Primary Failure | **PASS** | Failover + auto-recovery in ~38s |
| 3 | Network Partition of Primary | **PASS** | NetworkPolicy works; self-fencing + failover + auto-recovery |
| 4 | Data Integrity Under Failover | **PASS** | counter=42 survived two consecutive failovers |
| 6 | Self-Fencing Validation | **PASS** | super_read_only=ON after 25s isolation |
| 7 | Recovery/Rejoin After Failover | **PASS** | Validated across 4 operator restarts (persisted lastFailoverTarget) |
| 9 | Rapid Successive Failures (Anti-Flap) | **INCONCLUSIVE** | Operator reconciler overrides `scale --replicas=0`, preventing sustained outage |
| 11 | Simultaneous Both-Site Kill | **PASS** | TOTAL LOSS alert, auto-recovery after scale-up |
| 14 | Failover With Replication Lag | **FAIL** | Relay log drain returns immediately when SQL thread is stopped — 10/10 rows lost |
| 15 | Sidecar Crash (Container Kill) | **PASS** | MySQL stayed up, sidecar restarted, no failover triggered |

Scenarios 2, 5, 8, 10, 12, 13 were not re-tested (passed or low priority in round 1).

---

## Detailed Results

### Scenario 1: Clean Primary Failure — PASS

- **T+0s**: Killed iad (primary) via `chaos.sh kill-site iad`
- **T+8s**: Operator detected iad unreachable (3 consecutive poll failures), initiated failover to pdx
- **T+8s**: Failed to fence old primary (expected — iad dead), failed to kill connections
- **T+38s**: Relay log drain timed out (30s, expected — iad dead), pdx promoted
- **T+42s**: iad pod respawned writable → split brain detected → operator fenced iad
- **T+44s**: No GTID divergence → auto-recovered iad as replica of pdx
- **Final**: pdx=writable (primary), iad=read-only (replica), replication running

### Scenario 3: Network Partition of Primary — PASS

Previously INCONCLUSIVE in round 1 (iptables approach didn't block kube-proxy DNAT). Now fixed with NetworkPolicy deny-all approach.

- **T+0s**: Applied NetworkPolicy deny-all to pdx (primary)
- **T+20s**: pdx sidecar self-fenced — both Bloodraven and peer unreachable beyond 20s lease timeout
- **T+20s**: Sidecar set super_read_only=ON, killed 2 app connections
- **T+24s**: Operator detected pdx unreachable, initiated failover to iad
- **T+24s**: Relay log drain completed instantly (iad already had all data), iad promoted
- **T+24s**: `kubectl exec` on pdx confirmed super_read_only=1 (kubectl bypasses NetworkPolicy)
- **Post-cleanup**: Removed NetworkPolicy → operator detected pdx read-only → auto-recovered as replica
- **Bug found**: Recovery initially failed with Error 1872 (stale applier metadata from self-fencing kill). Fixed by adding `RESET REPLICA ALL` before `CHANGE REPLICATION SOURCE TO` in recovery path.

### Scenario 4: Data Integrity Under Failover — PASS

- Wrote counter=42 to pdx (primary), verified replicated to iad
- Killed pdx → failover to iad (relay drain timeout 30s)
- Post-failover counter on iad: **42** (zero data loss)
- pdx respawned → auto-recovered as replica, no GTID divergence

### Scenario 6: Self-Fencing Validation — PASS

- Scaled operator to 0 and pdx to 0, fully isolating iad (primary)
- After 25s: sidecar set super_read_only=ON
- Logged: `SELF-FENCING: both Bloodraven and peer unreachable beyond lease timeout`
- Logged: `SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore`
- Killed 1 app connection
- After restoring operator + pdx: operator correctly detected self-fenced iad, recovered as replica of pdx

### Scenario 7: Recovery/Rejoin After Failover — PASS

Previously PARTIAL FAIL in round 1 (`lastFailoverTarget` not persisted across restarts). Fix from commit 5ef9301 confirmed working.

- Validated across 4+ operator restarts during this session
- Operator logs show `restored lastFailoverTarget from CR status` on every startup
- Auto-recovery correctly identifies old primary and initiates CHANGE REPLICATION SOURCE

### Scenario 9: Rapid Successive Failures (Anti-Flap) — INCONCLUSIVE

- Killed iad (primary), waited for failover to pdx (~37s)
- Scaled pdx to 0 within cooldown window
- **Problem**: Operator reconciler immediately scales pdx back to 1 (reconcileDeployment sets replicas=1)
- pdx respawned within seconds, never stayed down long enough for failure threshold
- Anti-flap cooldown code path never reached because the second site never became unreachable
- **To properly test**: Would need to either disable the reconciler's replica enforcement or use a NetworkPolicy partition (which takes ~8s to detect, within the 30s cooldown)

### Scenario 11: Simultaneous Both-Site Kill — PASS

- Scaled both MySQL deployments to 0
- **T+6s**: Both sites transitioned to unreachable
- **T+6s**: TOTAL LOSS alert fired: "TOTAL LOSS: both sites are unreachable"
- No operator crash or panic
- Scaled both back to 1
- **T+20s**: pdx came up writable first, then iad
- Split brain detected → operator fenced iad (old primary per lastFailoverTarget)
- No GTID divergence → auto-recovered iad as replica of pdx
- **Final**: pdx=writable, iad=read-only, replication running

### Scenario 14: Failover With Replication Lag — FAIL

- Stopped SQL thread on iad (replica): `STOP REPLICA SQL_THREAD`
- Wrote 10 transactions on pdx (primary)
- Confirmed 0 rows on iad (SQL thread stopped, relay logs fetched but not applied)
- Killed pdx → failover to iad
- **Result**: Relay log drain returned immediately — **0 of 10 rows survived**
- **Root cause**: `WaitForRelayLogDrain()` at replication.go:228 returns success when `!rs.SQLRunning`, assuming "stopped = nothing to drain". But when SQL thread was manually stopped with pending relay logs, this is wrong.
- The drain should detect unapplied relay logs (compare Exec position vs Read position) and restart the SQL thread before declaring drain complete.

### Scenario 15: Sidecar Crash (Container Kill, Not Pod Kill) — PASS

Tested via `kubectl debug --target=sidecar -- kill 1` (ephemeral debug container shares PID namespace with sidecar).

- **T+0s**: Killed sidecar PID 1 on iad (primary) — sidecar container exited
- MySQL container stayed up throughout (port 3306 connectable, `read_only=0, super_read_only=0`)
- Kubernetes restarted sidecar container (RESTARTS count incremented, brief CrashLoopBackOff)
- Sidecar re-initialized cleanly: "this is the active site, no action needed"
- **No failover triggered** — operator saw brief health check gap but MySQL itself remained reachable on port 3306
- MFG status unchanged: iad=writable (primary), pdx=read-only (replica)
- Sidecar fencing timers reset on restart (grace period)

---

## Bugs Found During Round 2

### Fixed During Testing

1. **Fresh-deploy bootstrap fails when both sites have independent GTIDs** (topology.go `selectDonor`)
   - Two independently initialized MySQL instances have disjoint GTID sets (different server UUIDs)
   - `selectDonor()` treated this as "both have data — cannot auto-clone" instead of recognizing it as a fresh deploy
   - **Fix**: Added `HasCommonUUIDs()` check to GTIDSet. Disjoint GTID sets treated as fresh deploy (site[0] chosen as donor by convention).

2. **Dynamic pod labels in Deployment template cause rolling restarts** (reconciler.go:598-609)
   - `role` and `healthy` labels were set based on `fg.Status.ActiveSite` in the Deployment pod template
   - Every status change (failover, recovery, bootstrap) changed the template, triggering a new ReplicaSet and rolling restart
   - During bootstrap, this created a restart cycle preventing the system from ever stabilizing
   - **Fix**: Replaced dynamic labels with static defaults (`role=replica, healthy=no`). `syncPodLabels()` already handles live pod label updates via the Pod API.

3. **Recovery fails with Error 1872 after self-fencing** (recovery.go)
   - After self-fencing kills connections, MySQL replication applier metadata becomes corrupted
   - `RecoverOldPrimary()` does `STOP REPLICA` + `CHANGE REPLICATION SOURCE` + `START REPLICA` but doesn't clear stale metadata
   - **Fix**: Added `RESET REPLICA ALL` between `STOP REPLICA` and `CHANGE REPLICATION SOURCE TO`.

### Found But Not Fixed

4. **Relay log drain returns immediately when SQL thread is stopped** (replication.go:228)
   - `WaitForRelayLogDrain()` treats `!SQLRunning` as "nothing to drain" and returns nil
   - When SQL thread was manually stopped (or crashed), there may be unapplied relay logs
   - This causes **data loss** during failover with replication lag
   - **Fix needed**: Check if `Exec_Master_Log_Pos < Read_Master_Log_Pos` (or equivalent GTID comparison). If gap exists and SQL thread is stopped, restart it and wait for drain. Only return nil on stopped SQL thread if positions are equal.

5. **Anti-flap cooldown untestable in playground** (topology.go:664-669)
   - The operator reconciler always sets Deployment replicas=1, overriding `scale --replicas=0`
   - Cannot simulate a sustained outage without shutting down the entire k3d node
   - The anti-flap code exists and is unit-tested, but cannot be integration-tested in the playground
   - **Possible fix**: Add a chaos-testing mode that skips replica enforcement, or test via NetworkPolicy partition

6. **Reconciler Deployment conflict errors** (reconciler.go)
   - Frequent "Operation cannot be fulfilled on deployments.apps" errors during rapid state changes
   - The reconciler's CreateOrUpdate races with external Deployment modifications (scale commands, pod terminations)
   - Not a correctness issue (retries succeed), but creates noisy logs

## Comparison with Round 1

| Scenario | Round 1 | Round 2 | Change |
|----------|---------|---------|--------|
| 1 - Clean Primary Failure | PASS | PASS | Consistent |
| 3 - Network Partition | INCONCLUSIVE | **PASS** | Fixed (NetworkPolicy approach works) |
| 4 - Data Integrity | PASS | PASS | Consistent |
| 6 - Self-Fencing | PASS | PASS | Consistent |
| 7 - Recovery/Rejoin | PARTIAL FAIL | **PASS** | Fixed (lastFailoverTarget persisted) |
| 9 - Anti-Flap | INCONCLUSIVE | INCONCLUSIVE | Different root cause (operator overrides scale) |
| 11 - Both-Site Kill | Not tested | **PASS** | New |
| 14 - Replication Lag | Not tested | **FAIL** | New — data loss bug found |
| 15 - Sidecar Crash | Not tested | **PASS** | New — tested via `kubectl debug --target` |
