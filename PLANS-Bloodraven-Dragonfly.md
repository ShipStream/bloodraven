# Bloodraven Dragonfly Integration Plan

This plan describes how Bloodraven should become the opinionated MySQL + Dragonfly + DNS failover operator for ShipStream tenant runtimes.

The goal is not to build a generic Dragonfly operator. The goal is to give Bloodraven enough first-class Dragonfly control to preserve cache/session continuity during planned site failovers, while keeping MySQL as the only durable source of truth and allowing emergency failover to proceed even when Dragonfly cannot be preserved.

## Executive Summary

Bloodraven should own Dragonfly directly instead of depending on the upstream Dragonfly operator as the long-term control plane.

> **See also:** [`PLANS-Dragonfly-Upstream-Discoveries.md`](PLANS-Dragonfly-Upstream-Discoveries.md) — bugs, race conditions, and hard-learned lessons from the upstream operator's issue/PR history. Consult this during development to avoid repeating known mistakes.

The upstream Dragonfly operator is useful as a reference implementation for resource shape, labels, replication commands, readiness checks, and rollout behavior. It is not a clean actuator for Bloodraven because it has no declarative promotion API, no pause/fence API, no selected-candidate API, no useful per-pod replication status on the CR, and no planned switchover status model. Failover is driven through pod lifecycle side effects such as deleting the master pod.

Bloodraven already owns the higher-level decision that matters: which site is active. Dragonfly should participate in that same failover transaction instead of running an independent election policy.

## Wishlist Checklist

- [x] 34. Add `spec.dragonfly` to `MysqlFailoverGroup`. _(slice 1)_
- [x] 35. Add `status.dragonfly` and per-site Dragonfly status. _(slice 1; full `PlannedFailoverDragonflyStatus` field expansion still slice 3)_
- [ ] 36. Move tenant Dragonfly ownership from the platform tenant chart into Bloodraven.
- [x] 37. Implement minimal Dragonfly client commands and parsers. _(slice 1; extended in slice 2 with `CLIENT KILL` and `connected_slaves` parsing)_
- [x] 38. Reconcile per-site Dragonfly workloads, Services, auth, PDBs, and placement. _(slice 1; PDBs deferred)_
- [x] 39. Configure active-site Dragonfly master with async replicas in candidate sites. _(slice 1)_
- [x] 40. Extend planned failover with Dragonfly drain/sync/promotion phases. _(slice 1; tightened with traffic-gate label + CLIENT KILL in slice 2)_
- [x] 41. Add emergency failover behavior where Dragonfly is best-effort and never blocks MySQL recovery indefinitely. _(slice 1; mirrored strip→takeover→kill sequence in slice 2)_
- [x] 42. Add old-site-return handling for stale Dragonfly masters. _(slice 2: auto-rejoin gated on `connected_slaves=0 AND master_repl_offset=0`; force-restart/data-wipe still future work)_
- [x] 43. Add metrics, Events, dashboards, and runbook docs for Dragonfly state. _(metrics + Events done; dashboards + runbook deferred)_
- [ ] 44. Add k3d E2E and chaos scenarios covering MySQL + Dragonfly + DNS failover. _(playground wiring done: `spec.dragonfly` in playground FG, setup.sh waits for StatefulSets, `chaos.sh kill-dragonfly|dragonfly-status`, counter-app demonstrates cache continuity via Dragonfly INCR. Slice 3: unit-level no-`READONLY`-mid-flight regression delivered via `TestPlannedFailoverPromotingDragonfly_NoReadOnlyMidFlight` (asserts the active-Service selector invariant by intercepting every Pod label patch and tracking the Dragonfly master pointer through `ReplTakeover`). Still pending: exercising the scenarios in `PLANS-Dragonfly-Chaos-Scenarios.md` against the live cluster — see item 45.)_
- [ ] 45. Integrate Dragonfly into the chaos test harness from PR #68 (`cmd/playground-chaos`).
  - **How to run**: `make build-playground-chaos` then `./bin/playground-chaos check` (baseline) and `./bin/playground-chaos run 22-planned-dragonfly-switchover` (or `chaos-run-all`). The runner refuses to mutate any context that does not match the playground allowlist (see `internal/playground/kube/guard.go`); pass `--namespace`/`--fg`/`--context` if your playground deviates from defaults (`bloodraven-playground` / `playground` / current-context).
  - **Baseline assumptions**: `playground/manifests/failovergroup.yaml` has `spec.dragonfly.enabled: true` with `maxSyncWait: 30s`, `onSyncTimeout: proceed`, image `docker.dragonflydb.io/dragonflydb/dragonfly:v1.25.5`, ports 6379/9999. `AssertHealthyBaseline` (`internal/playground/scenarios/common.go`) requires `status.dragonfly.phase=Ready`, exactly one master, every other site `role=replica + linkStatus=up + !syncInProgress`, no `unreachable` sites — fix any drift before scenarios will pre-pass.
  - [x] Harness primitives shipped (commit `11cf391`)
    - `internal/playground/kube/dragonfly.go` — `DragonflyStatefulSetName`, `DragonflySiteServiceName`, `DragonflyActiveServiceName`, `DragonflyPodSelector`, `ListDragonflyPods`, `GetSiteDragonflyPod`, `DeleteSiteDragonflyPod`, `ScaleDragonflyStatefulSet`, `DragonflyActiveServiceEndpointPods` (Endpoints reader for kube-proxy convergence assertions). Names locked against operator side via `internal/playground/kube/dragonfly_test.go`.
    - `internal/playground/dragonfly/{site,resp}.go` — port-forwarded RESP client. `Open(ctx, k, ns, fg, site, password)` returns a `SiteClient` with `Ping`/`Set`/`Get`/`Incr`/`DBSize`/`InfoReplication`. `IsReadOnlyError` and `IsConnDropped` are the discriminators chaos scenarios pattern-match on (READONLY = real bug; conn-dropped = expected after CLIENT KILL). Auth password is currently passed empty — wire through `spec.dragonfly.auth.SecretName` decode here when the playground enables AUTH.
    - `internal/playground/chaos/actions.go` — `KillDragonflyPod(site)`, `ScaleDragonflyToZero(site)` (pushes reverter), `ScaleAllDragonflyToZero()` (iterates `mfg.Spec.Sites`), and a Dragonfly sweep in `GlobalRecover` (skipped when `spec.dragonfly` disabled).
    - `runner.Env.Dragonfly` opener wired in `executor.buildEnv` — each call dials a fresh port-forward; the executor closes still-open clients on scenario exit. NOT cached, intentionally: master-kill / failover scenarios need to re-resolve after pod respawns.
  - [x] `AssertHealthyBaseline` extended for Dragonfly (commit `11cf391`, `assertDragonflyBaselineHealthy` in `common.go`) — folds in D1 (LOADING-state replica attachment), D2 (snapshot_cron preserved), D9 (silent-key-loss / DBSIZE) baseline assertions so every Dragonfly scenario inherits them rather than rebuilding from scratch.
  - [x] Scenario 22 registered (D3 — planned coordinated failover): `internal/playground/scenarios/s22_planned_dragonfly_switchover.go`. Seeds a unique-per-run counter on the active master, annotates planned failover, asserts (a) `status.dragonfly.activeSite` flips, (b) `status.plannedFailover.dragonfly.PromotionMethod=="REPLTAKEOVER"`, (c) `SessionsPreserved=true`, (d) seed counter readable on new master at original value, (e) active Service Endpoints converge to the new master pod (kube-proxy did the right thing), (f) `bloodraven_dragonfly_promotions_total{result="success"}>=1`. Live-cluster sibling of `TestPlannedFailoverPromotingDragonfly_NoReadOnlyMidFlight` (commit `4ea6bb0`).
  - [x] Scenario 23 registered (D7 — master kill, MySQL untouched): `s23_dragonfly_master_kill.go`. Seeds counter, force-deletes Dragonfly master pod, asserts (a) `status.dragonfly.activeSite` flips, (b) `mfg.Status.ActiveSite` (MySQL) UNCHANGED, (c) seed counter readable on promoted replica (replication path was healthy = D9 in disguise), (d) respawned old master rejoins as replica with `linkStatus=up + !syncInProgress + reachable=true` (D2 in disguise), (e) promotions metric incremented.
  - [x] Scenario 24 registered (D5 — emergency MySQL with Dragonfly down): `s24_emergency_mysql_dragonfly_down.go`. Scales every Dragonfly StatefulSet to 0 (reverters pushed), force-kills active MySQL primary, asserts MySQL `activeSite` flips within 90s budget (covers 30s relay-log drain on dead primary). Establishes the safety contract: Dragonfly is never on MySQL critical path.
  - [ ] Scenario 22 passes against live k3d
    - Expected timing: <2 min total. REPLTAKEOVER drain ≤30s + label flips + metric scrape.
    - Likely flake source: Endpoints convergence step polls 500ms × 60 — kube-proxy can lag. If this is the only flake, raise the deadline before cutting the assertion.
    - Failure modes worth distinguishing: `SessionsPreserved=false` (slice-3 source-offset capture broke), `PromotionMethod` empty (REPLTAKEOVER failed → check operator log for `dragonfly: REPLTAKEOVER` errors), counter missing on new master (replication wasn't actually keeping up — D9 manifesting).
  - [ ] Scenario 23 passes against live k3d
    - Watch for: respawn time of the killed StatefulSet pod (if >2min, the settle step times out — check `--break_replication_on_master_restart` is actually set on the args). If MySQL `activeSite` ALSO flips, slice-1's "Dragonfly is best-effort" claim is broken; that's a hard regression worth opening a ticket on immediately.
  - [ ] Scenario 24 passes against live k3d
    - Watch for: Dragonfly `Phase` not leaving Ready within 60s — DragonflyManager poll interval is 1s by default but the FG might not have one explicitly set; check `mfg.Spec.Dragonfly.PollInterval` and operator startup args.
    - 30s relay-log drain is unavoidable per `playground/chaos-results.md`. Any failover faster than ~37s end-to-end is suspicious.
  - [ ] D1 / D2 follow-up scenarios — currently folded into the baseline. Promote to standalone scenarios only if the baseline-fail mode produces unhelpful diagnostics. Likely path: `s_df_replica_attach_loading.go` and `s_df_snapshot_cron_restored.go` with deterministic injection (clear snapshot_cron pre-REPLICAOF, then verify CONFIG GET on both pods).
  - [ ] D6 (rolling update) scenario — patch `mfg.Spec.Dragonfly.Image`, observe one-pod-at-a-time rollout, assert no window where both pods are simultaneously NotReady. Needs a new `pgchaos.PatchDragonflyImage(image)` action with reverter.
  - [x] D11 (operator restart mid-Dragonfly-failover) scenario registered — `internal/playground/scenarios/s25_operator_restart_mid_dragonfly_failover.go`. Triggers a planned failover via annotation, then a 100ms-tick poll waits for `plannedFailover.phase` to enter `WaitingForDragonflySync` (PromotingDragonfly accepted as a fallback if the kill loop loses the race) and immediately fires `Chaos.KillOperator`. Asserts (a) plannedFailover converges to `Succeeded` with the original target (catches a resumed operator silently swapping targets), (b) MySQL `status.activeSite` and `status.dragonfly.activeSite` both flipped to the target, (c) `status.dragonfly.phase` returns to `Ready` with exactly one master on the target (no stale-master / split-brain residue from the resumed REPLTAKEOVER). The kill window is explicit in the inject step and bails if the phase reaches a terminal state before the kill, so a misconfigured playground (Dragonfly disabled) or a 0-budget `maxSyncWait` doesn't silently no-op the assertion. Live-k3d soak still pending.
  - [ ] Scenario 25 passes against live k3d
    - Watch for: a phase transition through `WaitingForDragonflySync` faster than the 100ms tick (race lost — bail message points at this). If consistently lost, drop the tick to 50ms or move the kill into a goroutine fired from the `WaitingForLag` predicate so it lands on the very first reconcile that would advance to `WaitingForDragonflySync`.
    - Resume budget for `observePlannedFailoverConverges` is 4 minutes — operator restart in this environment is ~10s + remaining MySQL+Dragonfly state machine. If consistently brushing the deadline, the resumed operator is likely re-running an earlier phase from scratch (e.g. re-issuing a fresh source-offset capture) — diagnose before raising the budget.
  - [ ] After D3/D5/D7 soak, fold the live-cluster outcomes back into `playground/chaos-scenarios.md` (currently only documents MySQL scenarios) and into `PLANS-Dragonfly-Chaos-Scenarios.md` (mark D3/D5/D7 as "implemented", strike the "Not yet runnable" header).

## Priority 0: Control-Plane Decision

### 34. Make Dragonfly A First-Class Bloodraven Subsystem

Problem:
- The platform currently deploys Dragonfly from the tenant Helm chart as one Dragonfly CR per DC.
- That gives local cache/session HA but does not preserve sessions across site failover unless the target site already has a replica of the active site's Dragonfly data.
- The upstream Dragonfly operator does not expose the control surface Bloodraven needs for planned site failover.

Decision:
- Bloodraven should own Dragonfly topology, replication, promotion, service routing, and failover status for tenant runtime groups.
- Bloodraven should use the upstream Dragonfly operator as a source reference, not as the primary long-term dependency.

Non-goals:
- Do not implement a general-purpose Dragonfly operator.
- Do not implement sharding.
- Do not implement backups or PITR for Dragonfly.
- Do not implement multi-master cache semantics.
- Do not block emergency MySQL failover indefinitely to preserve sessions.
- Do not make Dragonfly data part of the durable application correctness model.

Acceptance criteria:
- A `MysqlFailoverGroup` can declare Dragonfly as part of the runtime group.
- Bloodraven can determine and report the active Dragonfly master site.
- Bloodraven can promote a target site's Dragonfly replica during planned failover.
- Emergency failover still succeeds when Dragonfly is lost, stale, or unreachable.

## Why Not Use The Upstream Dragonfly Operator As The Actuator?

The upstream operator is good at autonomous in-cluster HA. It is not designed as a subordinate failover actuator under a site-level orchestrator.

Observed behavior:
- The `Dragonfly` CR status only exposes `phase` and `isRollingUpdate`.
- There are no Kubernetes-style CR conditions for current master, replica sync, lag, selected candidate, or last failover.
- The client Service selects Pods with `role=master`.
- Master/replica identity is stored in Pod labels and annotations.
- Failover is triggered by Pod lifecycle, commonly by deleting the current master Pod.
- Candidate choice is internal to the operator.
- Replication readiness is exposed as a Pod readiness gate, not as a useful parent-level CR status.
- There is no CR field or subresource for planned switchover.

Consequences for Bloodraven:
- Bloodraven cannot say, declaratively, "promote the Dragonfly replica in site dc2 now".
- Bloodraven cannot pause the Dragonfly operator's autonomous election while MySQL, app pods, DNS, and cache are being coordinated.
- Bloodraven would have to watch and mutate operator-owned Pods and infer state from implementation labels.
- Bloodraven would couple to internals rather than a stable API.

Recommendation:
- Do not integrate against the Dragonfly operator as the durable control-plane boundary.
- Reuse its implementation ideas where they are proven.
- Keep the first Bloodraven implementation narrow and explicitly ShipStream-oriented.

## Priority 1: API Shape

### 35. Add `spec.dragonfly`

Add an optional Dragonfly block to `MysqlFailoverGroupSpec`.

Proposed initial shape:

```yaml
spec:
  dragonfly:
    enabled: true
    image: docker.dragonflydb.io/dragonflydb/dragonfly:v1.25.5
    port: 6379
    adminPort: 9999
    maxMemoryMb: 256
    proactorThreads: 1
    args: []
    auth:
      secretName: tenant-dragonfly
      passwordKey: password
    replicas:
      perSite: 1
    plannedFailover:
      maxSyncWait: 30s
      onSyncTimeout: proceed
    resources:
      requests:
        cpu: 250m
        memory: 256Mi
      limits:
        cpu: "1"
        memory: 512Mi
    serviceTemplate: {}
    podLabels: {}
    podAnnotations: {}
    podSecurityContext: {}
    containerSecurityContext: {}
```

Field guidance:
- `enabled` defaults to `false` initially, then can default to `true` once production behavior is proven.
- `image` must be pinned. Do not default to `latest`.
- `port` is the client Redis-compatible port.
- `adminPort` is used for operator-side commands when needed.
- `auth` should be supported before production.
- `replicas.perSite` should start at `1`; additional local replicas can be added later if useful.
- `plannedFailover.maxSyncWait` is the bounded wait for session-preserving failover.
- `plannedFailover.onSyncTimeout` should support `proceed` and maybe `fail`.

Recommended enum for `onSyncTimeout`:
- `proceed`: continue MySQL failover and accept possible session loss.
- `fail`: abort planned failover before MySQL promotion if Dragonfly cannot catch up.

Default recommendation:
- Default planned failover timeout behavior to `proceed` because Dragonfly is not durable business state.

Acceptance criteria:
- CRD validation rejects invalid ports, empty image when enabled, invalid timeout policy, and missing auth reference when auth is required.
- Defaults are generated into the CRD.
- Existing MySQL-only users can omit `spec.dragonfly` with no behavior change.

### 36. Add `status.dragonfly`

Add enough status to debug and automate safely.

Proposed shape:

```yaml
status:
  dragonfly:
    enabled: true
    activeSite: dc1
    phase: Ready
    message: "active master dc1, dc2 replica linked"
    lastPromotionTime: "2026-04-27T15:12:00Z"
    lastPromotionTarget: dc2
    sites:
      - name: dc1
        role: master
        reachable: true
        serviceName: orders-dragonfly-dc1
        podName: orders-dragonfly-dc1-0
        podIP: 10.42.1.25
        replicationState: master
        replicationOffset: 123456789
        linkStatus: ""
        syncInProgress: false
        ready: true
        message: "serving writes"
      - name: dc2
        role: replica
        reachable: true
        serviceName: orders-dragonfly-dc2
        podName: orders-dragonfly-dc2-0
        podIP: 10.42.2.31
        replicationState: replica
        replicationOffset: 123456700
        linkStatus: up
        syncInProgress: false
        ready: true
        message: "replicating from dc1"
```

Recommended phases:
- `Disabled`
- `Reconciling`
- `ConfiguringReplication`
- `Ready`
- `Degraded`
- `Promoting`
- `RecoveringOldSite`
- `Failed`

Recommended per-site roles:
- `unknown`
- `master`
- `replica`
- `stale-master`
- `unconfigured`
- `unreachable`

Acceptance criteria:
- `kubectl get mysqlfailovergroup -o yaml` shows the active Dragonfly site and useful per-site state.
- Status updates only on meaningful changes to avoid API churn.
- Status never claims session preservation when the target was promoted without confirmed replication freshness.

## Priority 2: Topology And Resource Ownership

### 37. Move Dragonfly Out Of The Tenant Chart

Problem:
- The platform tenant chart currently renders Dragonfly CRs directly.
- If Bloodraven owns failover coordination, tenant chart ownership creates two sources of truth.

Recommended work:
- Add Bloodraven-owned Dragonfly resources.
- Add an intermediate migration mode where the tenant chart can disable its Dragonfly templates.
- Update tenant app configuration to consume Bloodraven-created service names.
- Eventually remove or permanently disable tenant-chart Dragonfly ownership for production.

Desired service model:
- Per-site Dragonfly Services for internal control and replication.
- One active/write Service consumed by application pods.
- Optional per-site direct Services for debugging and controlled admin access.

Example names:
- `<group>-dragonfly` active write Service.
- `<group>-dragonfly-dc1` site-local Service.
- `<group>-dragonfly-dc2` site-local Service.

Important design choice:
- If app pods are deployed per site and only the active site should serve writes, the app should use the active Dragonfly endpoint associated with the active MySQL site.
- Avoid independent per-DC cache stores if session preservation across site failover is required.

Acceptance criteria:
- There is only one owner for production Dragonfly resources.
- App pods can get the correct Dragonfly host from deterministic service names.
- A planned failover moves both MySQL and Dragonfly active endpoints to the same target site.

### 38. Reconcile Minimal Dragonfly Workloads

Bloodraven should reconcile a small, explicit set of resources per failover group.

Recommended resources:
- One StatefulSet or Deployment per site.
- One Service per site.
- One active/write Service for application clients.
- One PodDisruptionBudget per group or per site.
- Optional NetworkPolicy if the cluster enforces tenant isolation.
- Optional Secret reference for auth, but Bloodraven should not generate passwords unless that becomes a separate requirement.

StatefulSet vs Deployment:
- StatefulSet gives stable names and stable identity, which makes status, debugging, and replication targeting easier.
- Dragonfly data is non-durable here, so PVCs are not required.
- A StatefulSet with ephemeral storage is acceptable.

Placement:
- Reuse `spec.sites[].taintNodeSelector` and site labels so Dragonfly follows the same site placement contract as MySQL.
- Dragonfly pods should tolerate the same site taints needed to stay resident during read-only/failover states.
- Do not let unrelated failover groups share service selectors.

Acceptance criteria:
- Bloodraven-created Dragonfly resources have owner references to the `MysqlFailoverGroup`.
- Resource names are stable and predictable.
- Site placement matches the corresponding MySQL site.
- Deleting/recreating a Dragonfly pod does not affect MySQL correctness.

## Priority 3: Dragonfly Client And State Detection

### 39. Implement A Minimal Dragonfly Client

Implement a small internal package, for example `internal/dragonfly`, similar in spirit to `internal/mysql`.

Required commands:
- `PING`
- `INFO replication`
- `INFO persistence`
- `SLAVEOF <host> <port>` or `REPLICAOF <host> <port>` depending on Dragonfly command support/version
- `SLAVEOF NO ONE` or `REPLICAOF NO ONE`
- `REPLTAKEOVER <timeout-ms>` if selected as the planned promotion primitive
- `CLIENT KILL` or connection-drain equivalent only if needed and safe
- `AUTH` when `spec.dragonfly.auth` is configured

Replication fields to parse:
- `role`
- `master_host`
- `master_port`
- `master_link_status`
- `master_sync_in_progress`
- `master_last_io_seconds_ago`
- `slave_repl_offset` or equivalent replica offset
- master replication offset if exposed

Persistence/loading fields to parse:
- `loading`
- `load_state`

Readiness definition:
- Master is ready when reachable, role is master, and it accepts writes.
- Replica is ready when reachable, role is replica, master link is up, not syncing, not loading, and connected to the expected active master.

Acceptance criteria:
- Unit tests cover `INFO replication` parsing across master, healthy replica, syncing replica, broken link, and malformed output.
- Client methods are context-aware and have bounded timeouts.
- Auth and no-auth modes are both covered by tests.

## Priority 4: Replication Model

### 40. Configure Active Master Plus Cross-Site Replicas

The session-preserving topology should be:

```text
active site:
  Dragonfly master

candidate site(s):
  Dragonfly replica(s) following the active site's master

application write endpoint:
  points to active Dragonfly master
```

This aligns Dragonfly's active write location with MySQL's active write location.

Initial scope:
- One Dragonfly pod per site.
- The active MySQL site should normally be the active Dragonfly master site.
- Every promotable candidate site should run a Dragonfly replica following the active master.
- DR-only sites can either run a replica or be excluded initially; decide explicitly in `spec.dragonfly` if needed.

Do not attempt:
- independent per-site masters
- multi-master session stores
- conflict resolution
- bidirectional replication

Acceptance criteria:
- On steady state, exactly one site is Dragonfly master.
- Candidate sites are replicas of the active master.
- Bloodraven detects and reports split-brain Dragonfly masters.
- Bloodraven can reconfigure a stale old master into a replica after failover.

## Priority 5: Planned Failover Integration

### 41. Extend Planned Failover Phases

Bloodraven already has planned failover concepts for MySQL. Dragonfly should be integrated into that state machine.

Recommended phase model:

```text
Pending
  -> Validating
  -> DrainingApps
  -> FencingMysqlSource
  -> WaitingForMysqlLag
  -> WaitingForDragonflySync
  -> PromotingDragonfly
  -> PromotingMysql
  -> SwitchingServicesAndDNS
  -> ReconfiguringOldSite
  -> Resuming
  -> Succeeded
```

The exact phase names can be shorter to match existing API conventions, but the ordering matters.

Recommended ordering rationale:
- Drain app writes before final cache/session sync if session preservation is desired.
- Fence MySQL before MySQL promotion to prevent durable split brain.
- Wait for MySQL target catch-up for data safety.
- Wait briefly for Dragonfly target sync for session continuity.
- Promote Dragonfly close to MySQL promotion so app traffic resumes against aligned active services.

Validation requirements:
- Target site must be a `primary-candidate` site.
- Target MySQL must be reachable and replicating.
- Target Dragonfly must exist unless `spec.dragonfly.plannedFailover.onSyncTimeout=proceed` allows degraded continuity.
- No restore, ordered update, or other destructive operation may be active.
- Anti-flap cooldown applies to the whole runtime group, not only MySQL.

Dragonfly planned switchover behavior:
- Record source Dragonfly replication offset after app drain begins.
- Poll target Dragonfly replica until it reaches or exceeds the recorded source offset if the offsets are comparable.
- If exact offset comparison is not reliable, use stable replica criteria: link up, sync not in progress, recent IO, no loading, and bounded quiet period after app drain.
- Promote target Dragonfly with `REPLTAKEOVER` when supported and appropriate; otherwise use `SLAVEOF NO ONE` after the replica is confirmed stable.
- Switch the active Dragonfly Service selector to the target site only after promotion succeeds or after the policy decides to proceed without preservation.

Timeout behavior:
- If Dragonfly sync wait times out and policy is `proceed`, continue MySQL failover and stamp status with `sessionsPreserved: false`.
- If policy is `fail`, abort before MySQL promotion when safe to do so, roll back the source MySQL fence, and leave active site unchanged.
- Once MySQL promotion has begun, Dragonfly failure must not leave MySQL half-promoted.

Acceptance criteria:
- Planned failover has an audit trail showing whether sessions were preserved.
- A Dragonfly timeout does not silently masquerade as a clean session-preserving failover.
- Planned MySQL failover behavior remains safe even if Dragonfly is degraded.

### 42. Add Planned Failover Status Fields For Dragonfly

Extend `PlannedFailoverStatus` or add a nested runtime component block.

Suggested fields:

```yaml
status:
  plannedFailover:
    dragonfly:
      enabled: true
      sourceRole: master
      targetRoleBeforePromotion: replica
      sourceOffsetAtDrain: 123456789
      targetOffsetAtPromotion: 123456789
      syncWaitSeconds: 4
      sessionsPreserved: true
      promotionMethod: REPLTAKEOVER
      reason: ""
      message: "target replica caught up before promotion"
```

Acceptance criteria:
- Operators can tell whether a planned failover preserved sessions.
- Metrics and Events carry the same outcome classification.

## Priority 6: Emergency Failover And Disaster Recovery

### 43. Dragonfly Must Be Best-Effort During Emergency Failover

Emergency priority order:
- Restore durable MySQL write service.
- Move DNS/app traffic.
- Provide a working Dragonfly endpoint.
- Preserve sessions only if doing so is quick and safe.

Emergency behavior:
- If the old Dragonfly master is reachable, try to promote the target replica cleanly within a short timeout.
- If the old Dragonfly master is unreachable, promote or start Dragonfly in the target site and accept session loss.
- If the target Dragonfly replica is stale or missing, start it as an empty master and accept session loss.
- If Dragonfly cannot be made available quickly, MySQL failover should still proceed; app pods may temporarily fail cache/session operations or force re-login depending on app behavior.

Status should explicitly say:
- `sessionsPreserved: true|false|unknown`
- `dragonflyRecoveryMode: promoted-replica|empty-master|unavailable|skipped`

Acceptance criteria:
- A dead Dragonfly source never blocks emergency MySQL promotion past a bounded timeout.
- Emergency status distinguishes durable failover success from session preservation success.

### 44. Handle Old Site Return

After failover, the old site may return with a Dragonfly pod that still believes it is master.

Required behavior:
- Detect any non-active site reporting Dragonfly master role.
- Remove it from client routing immediately.
- Reconfigure it as a replica of the active site or restart/reinitialize it if reconfiguration fails.
- Treat old Dragonfly data as discardable after failover unless it can safely rejoin as a replica.

Do not attempt to merge cache/session writes from an old master.

Acceptance criteria:
- Returning stale Dragonfly masters cannot receive application traffic.
- Bloodraven reports stale-master detection and recovery in Events/status.
- Rejoin behavior is idempotent across operator restarts.

## Priority 7: Services, App Integration, And Cutover

### 45. Define Stable App-Facing Dragonfly Endpoints

Application pods need stable connection settings that do not require chart changes during failover.

Recommended app-facing values:
- `REDIS_HOST=<group>-dragonfly`
- `REDIS_PORT=6379`
- `REDIS_PASSWORD` from tenant Secret when auth is enabled
- separate app-level key prefixes for sessions and cache

Service switching options:
- Selector-based Service: active Service selects `shipstream.io/dragonfly-role=master` and `shipstream.io/failover-group=<group>`.
- EndpointSlice-managed Service: Bloodraven writes endpoints directly for stricter control.

Recommendation:
- Start with selector-based Service if labels are exclusively Bloodraven-owned.
- Switch to EndpointSlice-managed routing only if selector timing proves too loose for planned failover.

Application behavior requirements:
- Redis clients must reconnect on connection drop or `READONLY`/role errors.
- PHP session handler should tolerate reconnect and retry once where safe.
- Durable workflow state must not be stored only in session.

Acceptance criteria:
- App pods keep a stable Redis host across failover.
- Bloodraven can force client reconnects by switching Service endpoints and optionally killing old connections.
- Cache/session prefixes are tenant/environment scoped.

## Priority 8: Observability

### 46. Add Metrics

Recommended metrics:
- `bloodraven_dragonfly_site_up{group,site}`
- `bloodraven_dragonfly_site_role{group,site,role}`
- `bloodraven_dragonfly_replication_link_up{group,site}`
- `bloodraven_dragonfly_replication_sync_in_progress{group,site}`
- `bloodraven_dragonfly_replication_offset{group,site}`
- `bloodraven_dragonfly_promotions_total{group,target_site,result}`
- `bloodraven_dragonfly_promotion_duration_seconds{group,target_site}`
- `bloodraven_dragonfly_session_preservation_total{group,result}` where result is `preserved`, `lost`, or `unknown`.

Acceptance criteria:
- Dashboards can show active Dragonfly site and replica health.
- Planned failover dashboards can show whether sessions were preserved.
- Alerts can detect no Dragonfly master, multiple Dragonfly masters, broken replica link, and stale old master.

### 47. Add Kubernetes Events

Recommended Events:
- `DragonflyConfigured`
- `DragonflyReplicationConfigured`
- `DragonflyReplicaCaughtUp`
- `DragonflyPromotionStarted`
- `DragonflyPromotionCompleted`
- `DragonflyPromotionFailed`
- `DragonflySessionPreservationSkipped`
- `DragonflyStaleMasterDetected`
- `DragonflyOldSiteReconfigured`

Acceptance criteria:
- `kubectl describe mysqlfailovergroup` tells the Dragonfly story during and after failover.

## Priority 9: Testing And Chaos Scenarios

### 48. Add Unit Tests

Required unit tests:
- Dragonfly `INFO replication` parser.
- Dragonfly `INFO persistence` parser.
- Site role classification.
- Candidate sync readiness logic.
- Planned failover timeout policy.
- Old-site-return classification.

### 49. Add Envtest Tests

Required envtest coverage:
- `spec.dragonfly` defaults and validation.
- Resource reconciliation and owner references.
- Status patching.
- Service selector changes.
- Planned failover status fields.

### 50. Add k3d E2E Tests

Required real-cluster scenarios:
- Fresh deploy creates MySQL and Dragonfly topology.
- Planned failover from dc1 to dc2 preserves sessions when target replica is healthy.
- Planned failover records `sessionsPreserved=false` when Dragonfly sync times out and policy is `proceed`.
- Emergency failover succeeds when Dragonfly source is dead.
- Old Dragonfly master returns after failover and is reconfigured or discarded.
- Operator restarts during `WaitingForDragonflySync` and resumes correctly.
- App client reconnects to new Dragonfly active Service.

Acceptance criteria:
- Dragonfly integration is covered by the same real-cluster gate proposed for Bloodraven production adoption.

## Migration Plan For ShipStream Platform

Phase 1: Compatibility
- Add `spec.dragonfly` to Bloodraven but keep disabled by default.
- Add tenant chart option to disable Dragonfly CR rendering.
- Keep existing tenant chart behavior for current local development.

Phase 2: Bloodraven-Owned Dragonfly In Test
- Enable Bloodraven Dragonfly in testing clusters.
- Point app pods at Bloodraven-created active Dragonfly Service.
- Verify planned and emergency failover behavior.

Phase 3: Remove Tenant Chart Ownership In Production Path
- Disable tenant chart Dragonfly templates for production values.
- Document Bloodraven as the owner of tenant cache/session runtime state.
- Update platform architecture docs to show MySQL + Dragonfly coordinated by Bloodraven.

Phase 4: Hardening
- Add auth by default.
- Add NetworkPolicy if tenant namespace isolation requires it.
- Add dashboards and alerts.
- Add chaos runs to release process.

## Failure Mode Matrix

| Failure | Required Behavior |
|---|---|
| Target Dragonfly replica healthy | Planned failover promotes target and preserves sessions. |
| Target Dragonfly replica behind | Wait up to `maxSyncWait`; then follow timeout policy. |
| Target Dragonfly unreachable before MySQL promotion | Abort planned failover if policy is `fail`; proceed with session loss if policy is `proceed`. |
| Source Dragonfly dies during planned drain | Continue MySQL failover; promote/start target; mark sessions `lost` or `unknown`. |
| Dragonfly promotion command fails | Do not leave active Service pointing to an unverified master; continue/abort according to phase and policy. |
| MySQL promotion succeeds but Dragonfly fails | Keep MySQL active; start empty Dragonfly if needed; mark session preservation failed. |
| Old Dragonfly master returns | Fence from routing; reconfigure as replica or restart empty. |
| Operator restarts mid-failover | Resume from CR status phase; never infer success without checking live state. |
| Multiple Dragonfly masters detected | Route only the site matching `status.activeSite`; mark degraded; reconfigure non-active masters. |
| No Dragonfly master detected | Start or promote active-site Dragonfly; do not affect MySQL durability. |

## Open Questions

- Which Dragonfly command should be the canonical planned promotion primitive for the pinned Dragonfly version: `REPLTAKEOVER`, `SLAVEOF NO ONE`, or another documented path?
- Are replication offsets comparable enough for strict session-preservation status, or should Bloodraven use a conservative readiness-plus-quiescence model?
- Should Bloodraven manage Dragonfly as StatefulSets or Deployments? StatefulSet is recommended initially for stable identity.
- Should `spec.dragonfly.enabled` eventually default to true for ShipStream platform installs?
- Should platform app pods always use one active Dragonfly Service, or should site-local app pods use site-local Services that Bloodraven rewires during failover?
- Should `MysqlFailoverGroup` eventually be renamed to a broader `RuntimeFailoverGroup` if Dragonfly and DNS are no longer optional add-ons?

## Recommended First Implementation Slice

Build the smallest useful vertical slice:

1. Add `spec.dragonfly.enabled`, image, port, adminPort, auth reference, resources, and planned failover timeout policy.
2. Add `status.dragonfly.activeSite`, phase, message, and per-site role/reachable/link status.
3. Reconcile one Dragonfly StatefulSet and Service per site plus one active Service.
4. Implement Dragonfly client `PING`, `INFO replication`, `INFO persistence`, `SLAVEOF`, and `SLAVEOF NO ONE`.
5. Configure non-active sites as replicas of the active site.
6. Extend planned failover with `WaitingForDragonflySync` and `PromotingDragonfly`.
7. Add best-effort emergency behavior that starts/promotes target Dragonfly but never blocks MySQL failover indefinitely.
8. Add unit tests and one k3d planned-failover scenario.

This slice proves the core design without committing to advanced rollout, NetworkPolicy, multi-replica-per-site, or extensive dashboard work.

## Slice 2 (delivered)

Both items below were delivered in slice 2. Code references are anchors for future review; the live source is the authoritative description.

### Active Service traffic-gate label (upstream PR #455) — done

The active app-facing Service (`<group>-dragonfly`) now AND-gates `shipstream.io/dragonfly-role=master` AND `shipstream.io/dragonfly-traffic=enabled` (`internal/controller/dragonfly_resources.go` `reconcileDragonflyActiveService`). The StatefulSet pod template seeds both labels at creation time. Removing the traffic label sheds an endpoint atomically without disturbing the role label, which closes the dual-master selector window during planned `REPLTAKEOVER`.

The `PromotingDragonfly` phase handler (`internal/controller/planned_failover_df.go` `plannedFailoverPromotingDragonfly`) executes a four-step sequence atomically within one reconcile pass:
1. Strip `dragonfly-traffic` from the source pod.
2. `REPLTAKEOVER` against the target.
3. On success, stamp `role=master, traffic=enabled` on target; demote source `role=replica` and restore source `traffic=enabled`.
4. Best-effort `CLIENT KILL TYPE NORMAL` against the old master so application clients reconnect through the active Service.

`syncDragonflyPodLabels` enforces both labels' steady state on every reconcile, with an explicit `plannedFailoverDragonflyStripActive` gate that prevents it from re-stamping the source's traffic label between strip and takeover. On takeover failure (proceed mode) or rollback (fail mode), `bestEffortRestoreSourceTraffic` re-attaches the source so the active Service still has an endpoint.

`DragonflyManager.TryEmergencyPromote` mirrors the same sequence with looser timeouts and tolerance for an unreachable old source (`internal/controller/dragonfly_topology.go`).

### Auto-reconfigure stale old masters (wishlist 44) — done

`DragonflyManager.reconcileReplication` now auto-attaches stale masters that pass the safety gate `connected_slaves=0 AND master_repl_offset=0` (provably never accepted writes since restart) by issuing `REPLICAOF <active-master-host> <port>`. The new `DragonflyOldSiteReconfigured` Event fires on success. Stale masters that fail the gate are still shed from the active Service via traffic-label removal but left for human intervention. Both behaviors live in `attemptStaleMasterReconfigure` and the stale-master branch of `reconcileReplication`.

`ReplicationInfo.ConnectedSlaves` was added to `internal/dragonfly/info.go` to drive the gate; it accepts both `connected_slaves` and `connected_replicas` keys for forward-compat with newer Dragonfly INFO output.

## Required Before Next Slice (deferred from slice 2)

A regression test that issues continuous writes through the active Service while a failover runs (asserting no `READONLY` mid-flight) is still pending. It belongs in either the k3d e2e suite or as a component-level test against a fake Dragonfly. The unit-level coverage in `planned_failover_df_test.go` and `dragonfly_topology_test.go` exercises the label/state transitions but does not exercise the live selector behaviour against a real `kube-proxy`/endpoint controller.

## Success Definition

The Dragonfly implementation is successful when a planned Bloodraven failover can move MySQL, Dragonfly sessions/cache, app routing, and DNS to the target site as one observable operation, while emergency failover remains safe and bounded even if Dragonfly data is discarded.

MySQL remains the correctness boundary. Dragonfly improves user continuity. Bloodraven owns the coordination between them.
