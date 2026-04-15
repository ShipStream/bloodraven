# Bloodraven Chaos Testing Results

**Date**: 2026-04-14
**Environment**: k3d cluster (1 server + 2 agents), playground config (pollInterval=2s, failureThreshold=3, failoverCooldown=30s, sidecar leaseTimeout=20s)

## Summary

| # | Scenario | Result | Notes |
|---|----------|--------|-------|
| 1 | Clean Primary Failure | **PASS** | Failover + recovery in ~43s total |
| 2 | Operator Kill and Restart | **PASS** | No spurious failover, no self-fencing |
| 3 | Network Partition of Primary | **INCONCLUSIVE** | iptables on host netns doesn't block kube-proxy DNAT |
| 4 | Data Integrity Under Failover | **PASS** | Zero committed data lost (counter=10 preserved) |
| 5 | Operator Kill During Failover | **PASS** | Exactly one writable site after convergence |
| 6 | Self-Fencing Validation | **PASS** | super_read_only=ON after 20s isolation |
| 7 | Recovery/Rejoin (No Divergence) | **PARTIAL FAIL** | Works within operator lifecycle; fails after operator restart |
| 8 | GTID Divergence Detection | **PASS** | 3 divergent txns detected, recovery blocked |
| 9 | Rapid Successive Failures | **INCONCLUSIVE** | k8s recreates pods too fast (<5s) for cooldown test |
| 10 | Full Bootstrap (CLONE INSTANCE) | **PASS** | Tested during setup; clone + replication setup end-to-end |

## Detailed Results

### Scenario 1: Clean Primary Failure — PASS

- **T+5s**: Operator detected iad unreachable, initiated failover to pdx
- **T+37s**: Relay log drain timed out (30s, expected — iad was dead), pdx promoted
- **T+41s**: Promotion confirmed (pdx writable)
- **T+41s**: Brief split-brain when iad pod respawned writable → operator fenced iad immediately
- **T+43s**: GTID comparison showed no divergence → auto-recovery, iad rejoined as replica of pdx
- **Observation**: DNS flip failed (RBAC missing for `dnsendpoints` patch) — non-blocking but needs fix

### Scenario 2: Operator Kill and Restart — PASS

- Operator restarted within ~15s (ready at T+16s, within 20s leaseTimeout)
- Active site unchanged (pdx), both MySQL sites stable
- Zero self-fencing events on either sidecar
- Operator resumed polling cleanly with no false alarms

### Scenario 3: Network Partition — INCONCLUSIVE

- `chaos.sh network-partition` uses iptables INPUT/OUTPUT rules on port 3306 in the host netns
- Operator connects via k8s Service (ClusterIP) which uses kube-proxy's iptables DNAT
- These DNAT rules operate in a different iptables chain, so the partition doesn't block the operator
- **Fix needed**: Either use NetworkPolicy-based partitioning or block at the pod network namespace level

### Scenario 4: Data Integrity Under Failover — PASS

- Created test table, wrote counter=10 on pdx (primary)
- Verified counter=10 replicated to iad
- Killed pdx → failover to iad
- Post-failover counter on iad: **10** (no data loss)
- After pdx pod respawned, operator cloned from iad → pdx, counter=10 on both sides
- **Additional observation**: Primary Service (`mysql-playground-primary`) has no endpoints because pods lack `shipstream.io/role: primary` label — counter-app can't connect

### Scenario 5: Operator Kill During Active Failover — PASS

- Killed iad (primary), then killed operator 1s later
- iad pod respawned before new operator completed failover
- New operator discovered existing topology (iad=writable, pdx=read-only), resumed normal monitoring
- **Invariant held**: Exactly one writable site at all times

### Scenario 6: Self-Fencing Validation — PASS

- Scaled operator to 0 and pdx to 0, fully isolating iad
- After 20s leaseTimeout: sidecar set `super_read_only=ON`
- Logged: `SELF-FENCING: both Bloodraven and peer unreachable beyond lease timeout`
- Logged: `SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore`
- **Note**: Must use `scale --replicas=0` not `kill-site` — Deployment controller recreates killed pods in <5s

### Scenario 7: Recovery/Rejoin After Failover — PARTIAL FAIL

- Within same operator lifecycle: auto-recovery works perfectly (shown in Scenario 1)
- **Bug found**: After operator restart, `lastFailoverTarget` is empty (volatile, not persisted to CR status)
- `checkRecovery()` at topology.go:894 returns early when `lastFailoverTarget == ""`
- Old primary sits as read-only with no replication indefinitely until next failover
- **Fix needed**: Persist `lastFailoverTarget` in CR `.status` and reload on startup

### Scenario 8: GTID Divergence Detection — PASS

- Failover from pdx → iad, then injected 3 rogue writes on pdx (old primary)
- Operator detected divergence: `divergentTransactions: 3`
- Reported exact divergent GTID set: `20de46e4-382d-11f1-8314-ca354ebe0acb:14-16`
- Set `RecoveryPending` condition with reason `DivergentTransactions`
- Auto-recovery correctly blocked — requires manual wipe/re-clone

### Scenario 9: Rapid Successive Failures — INCONCLUSIVE

- Killed iad, waited 38s for failover to pdx (relay log drain took 30s), then killed pdx
- Kubernetes Deployment controller recreated pdx pod within ~5s
- pdx was only unreachable for < failureThreshold (6s), so no second failover triggered
- Anti-flap cooldown never tested because pdx recovered before it was declared unreachable
- **To properly test**: Use `scale --replicas=0` to hold site down, or set failureThreshold=1

### Scenario 10: Full Bootstrap After Data Wipe — PASS

- Tested end-to-end during every playground setup and reset
- Fresh MySQL instances detected as "both writable, no replication" → `isFreshDeploy()` returns true
- CLONE INSTANCE from primary to replica, MySQL restarts (Error 3707 handled correctly)
- Operator waits for restart, then sets up replication with GTID auto-positioning
- Full data transfer verified (chaos_test.counter=10 survived clone)

## Bugs Found

### Fixed During Testing (committed as 510ba94)

1. **server-id literal `\n`** (reconciler.go:924): `echo` → `printf` for init container
2. **Clone Error 3707 not handled** (topology.go:873): Added "Restart server failed" to `isCloneConnectionDrop`
3. **Clone stuck at DROP DATA** (bootstrap.go:49): Kill connections before CLONE INSTANCE
4. **Podman `localhost/` image prefix** (setup.sh): Added `IMG_PREFIX` variable and sed patching
5. **Replicator user not created** (setup.sh, reset-mysql.sh): Added user creation after MySQL ready
6. **Taint race during reset** (reset-mysql.sh): Scale operator to 0 before reset

### Found But Not Fixed

7. **`lastFailoverTarget` not persisted** (topology.go:894): After operator restart, old primary recovery doesn't trigger because failover history is lost. Need to persist in CR status and reload on startup.

8. **DNS flip RBAC missing** (Helm chart ClusterRole): Operator can't patch `dnsendpoints` resource. ClusterRole needs `patch` verb for `dnsendpoints` in `externaldns.k8s.io` API group.

9. **Primary Service has no endpoints**: Pods lack `shipstream.io/role: primary` label that the `mysql-playground-primary` Service selector matches on. Role labels may not be applied after fresh-deploy bootstrap.

10. **Network partition chaos tool doesn't work**: iptables rules in host netns don't block kube-proxy DNAT traffic. Need pod-level network manipulation or NetworkPolicy-based approach.

11. **Reset script race**: PVCs not recreated if operator scale-back-up happens before reconciler creates them. Needs explicit reconcile trigger or PVC creation before scaling MySQL back up.

12. **Replicator user lost on PVC wipe**: In `secretName` mode, no init script creates the replication user. Every reset/bootstrap requires manual user creation. Should switch playground to `credentials` mode or add Docker entrypoint init SQL.
