# Bloodraven Chaos Testing — Scenarios Unique to a 3-Site Topology

These scenarios exercise behaviours that only exist (or only matter)
when `spec.sites[]` contains more than two entries or when at least one
site has `role: dr-only`. The 2-site scenarios in
[`chaos-scenarios.md`](./chaos-scenarios.md) cover the baseline
failover, split-brain, and recovery paths and are not repeated here.

## Prerequisites

1. k3d cluster, operator image, and dashboard already deployed per
   `playground/setup.sh`.
2. Swap the default 2-site manifest for the 3-site variant:
   ```bash
   NS=bloodraven-playground
   kubectl -n $NS apply -f playground/manifests/failovergroup-3site.yaml
   ```
   The variant declares three sites: `iad` and `pdx` as
   `primary-candidate`, `fra` as `dr-only`, with
   `spec.splitBrainPolicy.sitePriorities: [iad, pdx]`.
3. Wait for initial bootstrap to finish. Expected healthy state:
   ```
   kubectl -n $NS get mfg playground -o wide
   # ACTIVE=iad  READY=True
   kubectl -n $NS get mfg playground -o jsonpath='{.status.sites[*].state}'
   # writable read-only read-only     (iad, pdx, fra respectively)
   ```
4. Verify the star topology wired up: both `pdx` and `fra` should be
   replicating from the `mysql-playground-iad` service.
   ```bash
   for SITE in pdx fra; do
     echo "=== $SITE ==="
     kubectl -n $NS exec deploy/mysql-playground-$SITE -c mysql -- \
       mysql -uroot -pplayground-root-pw -N -e \
       "SELECT SERVICE_STATE, SOURCE_HOST FROM performance_schema.replication_connection_status;"
   done
   # Both should show:  ON  mysql-playground-iad.bloodraven-playground.svc.cluster.local
   ```

Key 3-site config: `pollInterval=2s`, `failureThreshold=3`
(~6s detection), `failoverCooldown=30s`, `leaseTimeout=20s`,
`peerCheckInterval=5s`, `sitePriorities=[iad, pdx]`.

## Tooling notes specific to 3-site

- **Three PEER_ADDRESSES entries.** Each site's sidecar now has two
  peers — from `iad`'s perspective, both `pdx` and `fra`. Self-fencing
  only triggers when the operator AND *all* peers are unreachable; a
  single reachable peer is a legal quorum.
- **`chaos.sh` takes any site name.** The existing script already
  accepts arbitrary site names, so `kill-site fra`,
  `network-partition fra`, etc. work without modification.
- **Patching `sitePriorities` mid-incident uses a JSON patch.** Merge
  patches on the nested `splitBrainPolicy` field silently drop siblings
  in some kubectl versions; always use
  `--type json -p '[{"op":"replace","path":"/spec/splitBrainPolicy/sitePriorities","value":["pdx","iad"]}]'`.
- **Don't rely on declared-order for promotion expectations.** The
  picker ranks by GTID freshness first; priority order is a
  tiebreaker. Always check `SHOW REPLICA STATUS` to confirm which
  replica was fresher *before* asserting on which one got promoted.
- **dr-only sites never receive the `role=primary` pod label.** The
  reconciler's `syncPodLabels` only flips a site to primary when
  `status.activeSite` matches its name, and a dr-only site cannot
  become the active site through auto-promotion.

---

## Scenarios 1-9: 3-Site-Specific Failure Modes

### 1. GTID freshness picks the stale-priority winner
**Category**: Failover picker correctness | **Risk**: Medium

**Hypothesis**: Failover ranks by GTID freshness before priority.
Given `sitePriorities: [iad, pdx]` and an `iad`-primary cluster, if
`pdx` is artificially lagged behind and `iad` dies while `fra` is
caught up, the picker should still refuse `fra` (dr-only, never
promoted) — but should pick **the freshest primary-candidate replica
even when priority would favour a staler one**.

To exercise the ordering without changing roles, temporarily patch
`pdx` to `primary-candidate` so there are three PCs, lag `iad` behind
`pdx` and `fra` (via replication delay on the standbys), then kill
the active site.

**Setup**: Expand to three PCs for this scenario:
```bash
NS=bloodraven-playground
kubectl -n $NS patch mfg playground --type json \
  -p '[{"op":"replace","path":"/spec/sites/2/role","value":"primary-candidate"}]'
# Wait for the CR to settle; no pod restart is triggered by a role flip.
```

**Injection**: Add a 10s delay to the *priority-preferred* replica
(`pdx`), catch `fra` (not preferred) up, then kill `iad`:
```bash
# pdx: 10s artificial apply delay
kubectl -n $NS exec deploy/mysql-playground-pdx -c mysql -- \
  mysql -uroot -pplayground-root-pw -e \
  "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=10; START REPLICA;"

# Write 20 rows on iad — pdx will apply these ~10s late, fra immediately.
kubectl -n $NS exec deploy/mysql-playground-iad -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "CREATE TABLE IF NOT EXISTS chaos_test.freshness (id INT PRIMARY KEY, ts DATETIME);"
for i in $(seq 1 20); do
  kubectl -n $NS exec deploy/mysql-playground-iad -c mysql -- \
    mysql -uroot -pplayground-root-pw -e \
    "INSERT INTO chaos_test.freshness VALUES ($i, NOW());"
done

# Capture the gap — fra is caught up, pdx is behind.
for SITE in pdx fra; do
  echo "=== $SITE GTID_EXECUTED ==="
  kubectl -n $NS exec deploy/mysql-playground-$SITE -c mysql -- \
    mysql -uroot -pplayground-root-pw -N -e "SELECT @@gtid_executed;"
done

# Kill iad.
kubectl -n $NS scale deployment mysql-playground-iad --replicas=0
```

**Verify**: After ~37s, operator logs should read
`failover picker: selected promotion target by GTID freshness
site=fra ...`. `status.activeSite` should be `fra`, not `pdx`, even
though `pdx` is first in `sitePriorities`. DNS should point to
`fra`'s `lbIP`. The event `FailoverExecuted` should name `fra`.

**Teardown**:
```bash
# Revert the role flip.
kubectl -n $NS patch mfg playground --type json \
  -p '[{"op":"replace","path":"/spec/sites/2/role","value":"dr-only"}]'
# Restore iad and let it rejoin. Clear SOURCE_DELAY on pdx.
kubectl -n $NS scale deployment mysql-playground-iad --replicas=1
# Wait for pdx to rebecome read-only replica of fra, then:
kubectl -n $NS exec deploy/mysql-playground-pdx -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0; START REPLICA;"
```

---

### 2. All primary-candidates down — dr-only is NOT promoted
**Category**: Role-based promotion gate | **Risk**: High

**Hypothesis**: When every `primary-candidate` site is unreachable,
the operator alerts `NO PRIMARY` and refuses to auto-promote `fra`
even though it is reachable and read-only. `fra` staying read-only
with no primary is the correct safety posture — promoting a
cross-region DR follower without explicit operator action is never
correct.

**Injection**:
```bash
NS=bloodraven-playground
kubectl -n $NS scale deployment mysql-playground-iad mysql-playground-pdx --replicas=0
```

**Verify**: Within ~15s the operator logs
`ALERT message="NO PRIMARY: no writable site available"`.
`status.activeSite` clears to "". `status.conditions[type=Degraded]`
flips to `True` with `Reason=NoPrimary`. `fra`'s MySQL stays
`read_only=1`. No `FailoverExecuted` event is emitted.

```bash
kubectl -n $NS get mfg playground -o jsonpath='{.status.activeSite}'; echo
kubectl -n $NS exec deploy/mysql-playground-fra -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e "SELECT @@read_only, @@super_read_only;"
# Expected: 1 0   (read_only=1, super_read_only=0 — still a replica)
```

**Recovery**:
```bash
# Bring either primary-candidate back. Operator resumes writes on iad
# because its data is current (fra never took writes).
kubectl -n $NS scale deployment mysql-playground-iad --replicas=1
```

**Documented behaviour**: This is the safety property Wishlist #10
built in — losing both PCs escalates to human intervention. Manual
promotion of a dr-only site is a separate, explicit operator action
(pending the planned-failover API in Wishlist #11).

---

### 3. Star rewiring: dr-only re-parents to new primary
**Category**: Replication topology integrity | **Risk**: Medium

**Hypothesis**: After `iad → pdx` failover, `fra` (dr-only) must
re-point its replication source to `mysql-playground-pdx`. The
operator should issue `CHANGE REPLICATION SOURCE TO` on `fra` exactly
once, driven by the same post-failover recovery path that handles
primary-candidate replicas.

**Injection**: Standard failover.
```bash
./playground/chaos.sh kill-site iad
```

**Verify**: After failover settles (~37s), both `pdx` (the new
primary) and `fra` (dr-only) should be reachable.
```bash
# New primary:
kubectl -n $NS exec deploy/mysql-playground-pdx -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e "SELECT @@read_only;"
# Expected: 0

# fra now replicates from pdx:
kubectl -n $NS exec deploy/mysql-playground-fra -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e \
  "SELECT SOURCE_HOST, SERVICE_STATE FROM performance_schema.replication_connection_status;"
# Expected: mysql-playground-pdx.bloodraven-playground.svc.cluster.local  ON

# Replication actually applying — write on pdx, read on fra:
kubectl -n $NS exec deploy/mysql-playground-pdx -c mysql -- \
  mysql -uroot -pplayground-root-pw -e "INSERT INTO chaos_test.counter VALUES (999, NOW()) ON DUPLICATE KEY UPDATE ts=NOW();"
sleep 3
kubectl -n $NS exec deploy/mysql-playground-fra -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e "SELECT COUNT(*) FROM chaos_test.counter WHERE id=999;"
# Expected: 1
```

**Caution — staging of source swap**: The operator's recovery path
fences the old primary first, then issues `CHANGE REPLICATION
SOURCE`. On `fra` the IO thread may disconnect briefly during the
swap. Expect `SERVICE_STATE` to transition `ON → CONNECTING → ON` in
a 5-15s window, not an instantaneous flip.

---

### 4. Split-brain winner fences both other writable sites
**Category**: Split-brain resolution across 3 writable sites | **Risk**: High

**Hypothesis**: If every site is simultaneously writable and
`sitePriorities=[iad, pdx]`, the operator:
1. picks `iad` as the winner (first entry in priority list that is
   currently writable and `primary-candidate`);
2. fences both `pdx` AND `fra` via `SET GLOBAL super_read_only=ON`,
   even though `fra` is dr-only;
3. synthesises a promotion of `iad` through the standard path
   (respecting the anti-flap cooldown and emitting the usual events).

**Injection**: Manually make every site writable. This requires
bypassing the operator because `fra` would normally fence itself
back into read-only.
```bash
NS=bloodraven-playground
# Stop the operator first so it can't intervene.
kubectl -n bloodraven-system scale deployment bloodraven --replicas=0

for SITE in iad pdx fra; do
  kubectl -n $NS exec deploy/mysql-playground-$SITE -c mysql -- \
    mysql -uroot -pplayground-root-pw -e \
    "STOP REPLICA; SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;"
done

# Re-enable the operator.
kubectl -n bloodraven-system scale deployment bloodraven --replicas=1
```

**Verify**: Within ~10s:
- Operator logs `ALERT message="SPLIT BRAIN: 3 sites are writable (iad, pdx, fra)"`.
- Operator logs two `split-brain auto-resolve: fencing non-preferred
  site per spec.splitBrainPolicy.sitePriorities` entries — one for
  `pdx`, one for `fra`.
- `metric bloodraven_split_brain_auto_resolve_total{prefer_site="iad"}` increments by 1.
- Post-resolution state: `iad` writable, `pdx` read-only (fenced +
  super_read_only=ON), `fra` read-only + super_read_only=ON.
- `FailoverExecuted` event names `iad`.

```bash
for SITE in iad pdx fra; do
  STATE=$(kubectl -n $NS exec deploy/mysql-playground-$SITE -c mysql -- \
    mysql -uroot -pplayground-root-pw -N -e "SELECT @@read_only, @@super_read_only;")
  echo "$SITE: $STATE"
done
# Expected:
#   iad: 0 0
#   pdx: 1 1
#   fra: 1 1
```

**Critical correctness check**: This is the scenario Copilot flagged
on PR #53 — the `SiteRole` doc now explicitly records that writable
`dr-only` sites ARE fenced as split-brain losers. Verify `fra`'s
super_read_only is `1`; if it is `0`, the role is leaking past the
safety check.

**Recovery**: Operator-driven. Once `iad` is writable, `pdx` and
`fra` should auto-recover as replicas (divergent-GTID detection may
intervene if writes happened during the split — in that case expect
`RecoveryBlocked` on `pdx` and/or `fra` and follow the scenario 8 /
19 remediation from the main chaos doc).

---

### 5. Writable dr-only anomaly — operator fences it
**Category**: dr-only safety invariant | **Risk**: Medium

**Hypothesis**: A dr-only site should never be writable. If somehow
it becomes writable (manual error, partial bootstrap bug) while the
legitimate primary-candidate primary is still active, the operator
should detect this as a 2-site split-brain (iad + fra), pick `iad`
per priority, and fence `fra`.

**Injection**:
```bash
NS=bloodraven-playground
# iad remains the operator-recognised primary; force fra writable.
kubectl -n $NS exec deploy/mysql-playground-fra -c mysql -- \
  mysql -uroot -pplayground-root-pw -e \
  "STOP REPLICA; SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;"
```

**Verify**: Within ~10s:
- `ALERT message="SPLIT BRAIN: 2 sites are writable (iad, fra)"`.
- One auto-resolve log line fencing `fra`.
- `fra` back to `read_only=1, super_read_only=1`, replication
  restarts from `iad` on the next sidecar + operator recovery cycle.
- `iad` still primary — no promotion event.

**Why it matters**: This proves the split-brain fencing is
role-agnostic on the loser side. It complements scenario 4
(dr-only as part of 3-way SB) and makes the invariant explicit:
any writable non-winner is fenced regardless of role.

---

### 6. Sidecar quorum: one peer up keeps primary writable
**Category**: Self-fencing quorum across N-1 peers | **Risk**: High

**Hypothesis**: The sidecar fences its MySQL only when the operator
AND every peer are unreachable beyond `leaseTimeout` (20s). With
three sites, `iad`'s sidecar has two peers: `pdx` and `fra`. If
`iad` can still reach even one of those peers, the primary stays
writable even when Bloodraven is unreachable.

**Injection — part A (keep fra reachable)**: Partition `iad` from the
operator and from `pdx`, but leave the `iad ↔ fra` path open.
```bash
NS=bloodraven-playground

# Block sidecar traffic from iad to pdx only, and block operator.
# Using a NetworkPolicy that permits egress to fra but not pdx and
# not the bloodraven-system namespace.
cat <<'YAML' | kubectl -n $NS apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: iad-isolated-except-fra
spec:
  podSelector:
    matchLabels:
      shipstream.io/site: iad
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector:
            matchLabels:
              shipstream.io/site: fra
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
    - ports: [{protocol: UDP, port: 53}]
YAML

# Wait past the lease timeout — fence must NOT trigger.
sleep 40
```

**Verify — part A**:
```bash
kubectl -n $NS exec deploy/mysql-playground-iad -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e "SELECT @@super_read_only;"
# Expected: 0   (primary still writable — fra is reachable)

kubectl -n $NS logs deploy/mysql-playground-iad -c sidecar --tail=20 | \
  grep -E 'SELF-FENC|fencing:' || echo "no self-fence log — correct"
```

**Injection — part B (cut fra too)**: Now remove the exception,
isolating `iad` completely.
```bash
kubectl -n $NS delete networkpolicy iad-isolated-except-fra
cat <<'YAML' | kubectl -n $NS apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: iad-fully-isolated
spec:
  podSelector:
    matchLabels:
      shipstream.io/site: iad
  policyTypes: [Egress]
  egress:
    - ports: [{protocol: UDP, port: 53}]
YAML
sleep 25
```

**Verify — part B**:
```bash
kubectl -n $NS exec deploy/mysql-playground-iad -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e "SELECT @@super_read_only;"
# Expected: 1   (self-fenced — operator AND both peers all down)
kubectl -n $NS logs deploy/mysql-playground-iad -c sidecar --tail=50 | \
  grep 'SELF-FENC'
# Expected: SELF-FENCING: Bloodraven and every peer unreachable beyond lease timeout
```

**Cleanup**:
```bash
kubectl -n $NS delete networkpolicy iad-fully-isolated
# Once connectivity is restored the operator may take over with a
# failover; if fenced iad was no longer active, `iad` will recover as
# replica via checkRecovery.
```

**Why it matters**: In a 2-site topology "one peer down" is "all
peers down". In a 3+ site topology a single peer outage is a *valid*
operating state and must not cause the primary to fence. This
scenario is the regression test for the quorum rule added in Wishlist
#10.

---

### 7. dr-only PVC wipe — re-bootstrap from active primary
**Category**: Bootstrap resilience for passive followers | **Risk**: Medium

**Hypothesis**: Wiping `fra`'s PVC should trigger an auto-clone from
the current active primary (not from another replica). `pdx`
(also a replica) should NOT be selected as donor — the operator
clones only from a writable site with data.

**Injection**:
```bash
NS=bloodraven-playground

# Clear the db-readonly taint on fra's node before the PVC delete,
# otherwise local-path-provisioner helper pods get evicted.
NODE=$(kubectl -n $NS get pods -l shipstream.io/site=fra \
  -o jsonpath='{.items[0].spec.nodeName}')
kubectl taint nodes $NODE shipstream.io/db-readonly- 2>/dev/null || true

# Stop fra, wipe its PVC, let it respawn.
kubectl -n $NS scale deployment mysql-playground-fra --replicas=0
kubectl -n $NS delete pvc mysql-playground-fra-data
kubectl -n $NS scale deployment mysql-playground-fra --replicas=1
```

**Verify**:
- `status.conditions[type=Bootstrapping]` transitions through
  `Cloning → WaitingForRestart → SetupReplication → Done` on `fra`
  only.
- Operator log: `starting bootstrap source=auto-clone donor=iad
  recipient=fra` (or `donor=<current-active>`, whichever it is).
  Donor must match `status.activeSite`, never `pdx`.
- After ~60-90s, `fra` re-established with
  `SOURCE_HOST=mysql-playground-<active>.bloodraven-playground.svc.cluster.local`
  and data in sync.

```bash
kubectl -n $NS get mfg playground -o jsonpath='{.status.activeSite}' | xargs -I {} echo "Active: {}"
kubectl -n $NS exec deploy/mysql-playground-fra -c mysql -- \
  mysql -uroot -pplayground-root-pw -N -e \
  "SELECT SOURCE_HOST, SERVICE_STATE FROM performance_schema.replication_connection_status;"
```

---

### 8. Scale out: add a dr-only site to a running cluster
**Category**: Day-2 expansion | **Risk**: Medium

**Hypothesis**: Starting from the default 2-site manifest, appending
`fra` with `role: dr-only` causes the operator to provision the
Deployment/Service/PVC and auto-clone from the current primary —
without impact to the existing primary/replica pair.

**Setup**: Apply the 2-site manifest first (not the 3-site one):
```bash
kubectl -n $NS apply -f playground/manifests/failovergroup.yaml
# Wait for steady-state iad (primary) + pdx (replica).
```

**Injection**: Patch in the third site.
```bash
kubectl -n $NS patch mfg playground --type json -p '[
  {"op":"add","path":"/spec/sites/-","value":{
    "name":"fra","role":"dr-only","zone":"zone-fra","lbIP":"10.96.100.30",
    "storage":{"storageClassName":"playground-fast","size":"1Gi"},
    "resources":{
      "requests":{"cpu":"100m","memory":"256Mi"},
      "limits":{"cpu":"500m","memory":"512Mi"}
    }
  }}
]'
```

**Verify**:
- `kubectl -n $NS get deployments` shows three deployments within 10s.
- Reconciler creates `mysql-playground-fra`, PVC, and the two
  ClusterIP services.
- Operator triggers auto-clone to populate `fra`'s empty instance;
  `status.sites[2].state` transitions `unknown → writable → read-only`
  over the course of the clone.
- `status.activeSite` stays on the original primary throughout — no
  disruption.
- `sitePriorities` (if present) is untouched and still only names
  primary-candidates.

**Timing**: ~60-90s end-to-end (PVC provision + clone + replication
setup). Watch for the cluster to remain healthy (`Ready=True`)
continuously — the scale-out should be a non-event for the existing
pair.

---

### 9. Scale in: remove a dr-only site cleanly
**Category**: Day-2 contraction | **Risk**: Medium

**Hypothesis**: Removing `fra` from `spec.sites[]` tears down its
Deployment, Service, and PVC via owner-reference garbage collection.
The remaining primary/replica pair is untouched, and
`status.sites[]` contracts accordingly.

**Injection**:
```bash
kubectl -n $NS patch mfg playground --type json \
  -p '[{"op":"remove","path":"/spec/sites/2"}]'
```

**Verify**:
- `kubectl -n $NS get deployment mysql-playground-fra` → NotFound
  within ~10s.
- `kubectl -n $NS get svc,pvc -l shipstream.io/site=fra` → empty.
- `status.sites[]` now has two entries.
- `status.activeSite` unchanged; primary still writable.
- Dashboard reflects the two-site layout without reload errors.

**Caution — if fra had been recently promoted**: Removing an active
site is undefined; it should fail CRD validation (only `iad` and
`pdx` are primary-candidates in this manifest, so `fra` cannot be the
active site) but if it somehow did, a pre-removal planned-failover
is the correct precondition. Don't test active-site removal here —
that's a Wishlist #11 scenario.

**Cleanup / reset to 3-site**: Reapply
`playground/manifests/failovergroup-3site.yaml` to restore the full
topology for subsequent scenarios.

---

## Execution Plan

1. **Starting state**: the 3-site manifest is applied and `fra` has
   finished its initial clone (healthy 3-site steady state).
   Scenarios 1-7 assume this as their entry state.
   ```bash
   kubectl -n bloodraven-playground apply -f playground/manifests/failovergroup-3site.yaml
   ```
2. **Run 1 (GTID-first picker)** before any other promotion scenario
   — the role-flip setup makes it a self-contained test, and verifying
   the picker early prevents confused state in later scenarios.
3. **Run 2, 3, 5 in order** — each leaves the cluster in a healthy
   steady state.
4. **Run 4 (three-way split brain)** only after the above, and budget
   time for `reset-mysql.sh` afterward if divergent-GTID detection
   kicks in.
5. **Run 6 (quorum)** after restoring a clean state post-4. The
   NetworkPolicy side effects are the fiddliest part of this doc.
6. **Run 7 (PVC wipe on fra)** last among the in-place scenarios; it
   re-exercises the bootstrap path end-to-end.
7. **Scale tests (8 and 9)** are independent of the others and safe
   to run at any point, but should start from the 2-site manifest
   (see scenario 8 setup).
8. **After everything**: `./playground/teardown.sh` or
   `./playground/reset-mysql.sh` + re-apply `failovergroup-3site.yaml`
   to leave the environment clean.
