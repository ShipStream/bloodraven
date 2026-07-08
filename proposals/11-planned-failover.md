# Proposal: Graceful Planned-Failover API

**Wishlist item:** [#11](../WISHLIST.md)
**Status:** Draft
**Branch:** `wishlist/planned-failover`

## Motivation

Manual promotion in Bloodraven today is a three-step `kubectl exec` dance documented at `docs/docs/operations.mdx:9-38`: `SET GLOBAL super_read_only=ON` on the current primary, `STOP REPLICA; RESET REPLICA ALL;` on the target, `SET GLOBAL read_only=0`, then let the topology manager notice on its next poll and reconcile Services, DNS, and taints. The docs also carry a caution that this path **bypasses the anti-flap cooldown**, so an operator performing routine maintenance can unintentionally leave the cluster exposed to a follow-on emergency failover that the operator will refuse to execute.

Beyond the cooldown gap, the `kubectl exec` dance has three real problems:

1. **No atomicity.** The three steps are separate. A dropped connection, a kubectl typo, or operator impatience mid-sequence leaves the cluster fenced-on-both-sides or promoting a replica that has not yet drained its relay logs.
2. **No lag gate.** There is no mechanical check that the target is caught up; the cautionary line in `operations.mdx` just asks the admin to "understand the current replication state" before promoting. Data-loss on a planned switchover is avoidable; we should not make it a manual check.
3. **No audit trail.** The three `kubectl exec` commands leave no trace on the `MysqlFailoverGroup` CR — no event, no status field, no metric tick. `bloodraven_failovers_total` (the automatic-failover counter) does not increment, so dashboards and post-incident reviews do not see planned switchovers.

The goal of this proposal is to replace the dance with a single declarative trigger — an annotation on the `MysqlFailoverGroup` — that drains writes on the current primary, waits for the target to catch up, promotes atomically through the existing `FailoverController.Execute` machinery, and records everything on the CR. Crucially, the planned path **must respect the anti-flap cooldown** at `internal/controller/topology.go:923-927`; the cautionary note in today's docs becomes a mechanical guarantee instead of an admin-trust exercise.

## Goals

1. Single-command planned switchover via a `kubectl annotate` on the `MysqlFailoverGroup`.
2. Full reuse of `internal/controller/failover.go`'s `FailoverController.Execute` plus the topology manager's DNS-flip + label-sync path, so planned and emergency failovers converge on one code path for the destructive steps.
3. Explicit zero-lag gate before promotion, with a bounded timeout and clean rollback on timeout (no stuck fence).
4. Anti-flap cooldown enforced — **not bypassed**. A planned failover that would violate `spec.failoverCooldown` is rejected up-front with a clear event, not queued silently and not forced through.
5. Full observability: phase-tracked status block on the CR, Kubernetes Events on every transition, Prometheus counters + duration histogram labelled like the existing `bloodraven_failovers_total` family.
6. Idempotent against double-annotation / fat-finger retyping — follow the consume-and-clear pattern already established by `RecloneAnnotation` (see `internal/controller/runner.go:245-297`).

## Non-goals

- **Replacing emergency failover.** This is the human-triggered happy path; the topology manager's automatic failover on `StateUnreachable` stays exactly as it is.
- **Scheduled / queued planned failovers.** "Annotate now, run at 02:00" is a nice-to-have (Phase 3). Phase 1 rejects if the cluster is not in a state to switch *right now* — the admin retries when the cooldown or in-flight operation clears.
- **Cross-region "promote DR site" semantics.** Planned failover targets a `primary-candidate` site, never a `dr-only` site. The DR promotion story is Wishlist #7.
- **Application-visible write continuity.** In-flight transactions on the old primary are killed (`KillAppConnections`, same as emergency). Zero-downtime application-level failover is an app-side concern (retry on `ER_OPTION_PREVENTS_STATEMENT`).
- **Replacing the `kubectl bloodraven` CLI plugin.** The annotation is the API. The plugin (Wishlist #18) will later wrap `kubectl annotate` with nicer UX; it does not belong in this proposal.

## API

### Trigger: annotation on `MysqlFailoverGroup`

Following the one-shot annotation pattern from `internal/controller/reconciler.go:53` (`RecloneAnnotation`):

```go
// PlannedFailoverAnnotation triggers a graceful, fat-finger-safe
// switchover to the named primary-candidate site. Consumed and
// cleared on the next reconcile, like RecloneAnnotation.
PlannedFailoverAnnotation = "bloodraven.shipstream.io/planned-failover"
```

Usage:

```bash
kubectl annotate mysqlfailovergroup orders \
  bloodraven.shipstream.io/planned-failover=pdx
```

Annotation values:

- `<siteName>` — graceful switchover to `<siteName>` using default knobs.
- `<siteName>:maxLagWait=<duration>` — optional override of the zero-lag wait timeout. Mirrors the reclone `<site>:<gtidPrefix>` key=value grammar.

The operator parses, validates, consumes, and clears the annotation exactly the way `handleRecloneAnnotation` does (runner.go:250-297) — same RetryOnConflict, same "clear on rejection so the admin sees one event per mistake" behaviour, same `removeRecloneAnnotation`-style helper.

### Spec-level defaults: `spec.plannedFailover`

Operational knobs live on the CR so cluster-wide defaults don't have to be spelled into every `kubectl annotate`:

```yaml
spec:
  plannedFailover:
    # Max time to wait for the target to catch up with the fenced
    # primary's GTID_EXECUTED before rolling back. Default: 5m.
    maxLagWait: 5m

    # Max time to wait for the -primary Service to shed connections
    # after stripping the primary role label. Default: 30s. Implemented
    # as an upper bound on KillAppConnections retries.
    drainTimeout: 30s

    # Behaviour when a planned failover is rejected by cooldown. One of:
    #   reject  — emit PlannedFailoverRejected, clear annotation (default)
    #   defer   — leave annotation in place, retry on every reconcile
    #             until cooldown clears (Phase 2)
    # Phase 1 only implements "reject".
    onCooldown: reject
```

Every field is optional; omitting the block is equivalent to `{maxLagWait: 5m, drainTimeout: 30s, onCooldown: reject}`.

### Status: `status.plannedFailover`

Phase-tracked block parallel to `status.restoreInPlace` (see `api/v1alpha1/backup_types.go:702-733`):

```yaml
status:
  plannedFailover:
    phase: Succeeded              # Pending|Validating|Draining|WaitingForLag|Promoting|Resuming|Succeeded|Failed
    target: pdx                   # site that was promoted (or attempted)
    sourcePrimary: iad            # site that was fenced
    sourceGtidAtFence: "abc-...:1-9182731"
    targetGtidAtPromotion: "abc-...:1-9182731"
    startTime: "2026-04-20T14:32:00Z"
    completionTime: "2026-04-20T14:32:47Z"
    durationSeconds: 47
    transactionsLost: 0           # len(sourceGtid \ targetGtid); 0 on a clean planned switchover
    message: "promoted pdx, 0 transactions lost"
```

State survives reconciles so `kubectl describe mysqlfailovergroup` tells the whole story after the fact. On re-arm (next `kubectl annotate`), the block is replaced, not appended — only the most recent planned failover is kept. Prior planned failovers are reconstructable from events and metrics.

### RBAC

No new ClusterRole verbs — the reconciler already has `get;patch;update` on `mysqlfailovergroups/status` and manipulates pods/services/dnsendpoints through existing rules (`charts/bloodraven/templates/clusterrole.yaml:41-73`). Planned failover uses the same primitives as emergency failover.

## Lifecycle

```
  Pending ──► Validating ──► Draining ──► WaitingForLag ──► Promoting ──► Resuming ──► Succeeded
                   │             │               │               │             │
                   └─ fail ──────┴─ fail ────────┴─ fail/timeout ┴─ fail ──────┴──► Failed
                                                                                      │
                                                                       rollback fence ┘
```

1. **Pending** — annotation observed, CR status block initialised with `phase: Pending`, event `PlannedFailoverStarted` emitted. Finalizer/owner refs are unchanged — this is a pure status-driven flow on the existing CR.

2. **Validating** — parse and validate (`parsePlannedFailoverAnnotation`, modelled on `parseRecloneAnnotation` at `internal/controller/reclone.go:37-46`). Rejections clear the annotation and emit `PlannedFailoverRejected` with a message suitable for `kubectl describe`:
   - Target site must name a `primary-candidate` entry in `spec.sites`.
   - Target must not equal `status.activeSite` (idempotent: already-active is a no-op, event `PlannedFailoverSkipped` rather than `Rejected`).
   - Target's observed state in `status.sites[].state` must be `read-only` (i.e. actively replicating), not `unreachable` or `unknown`.
   - Target's `replicating` flag must be true.
   - `status.restoreInPlace.phase` must be terminal or unset (no concurrent destructive restore).
   - `status.updatePhase` must be empty (no in-flight ordered update).
   - Anti-flap cooldown: `now - status.lastFailover < spec.failoverCooldown` rejects with reason `CooldownActive` and a `retryAfter` timestamp in the event message. **This is the rule the wishlist item explicitly calls out; it mirrors the check at `internal/controller/topology.go:923-927` rather than bypassing it.**

3. **Draining** — set `super_read_only=ON` on the old primary (`mysql.Checker.SetSuperReadOnly`, already used by `FailoverController.Execute` step 1). Strip the `shipstream.io/mysql-role: primary` label from the old primary's Pod so `reconcilePrimaryService` stops directing writes to it — same mechanism used by in-place restore's Fencing phase (`internal/controller/restore_inplace.go:345-360`, syncPodLabels). Record the fenced primary's `GTID_EXECUTED` in `status.plannedFailover.sourceGtidAtFence`.

4. **WaitingForLag** — poll the target's `GTID_EXECUTED` every `pollInterval` until it contains the source's fenced `GTID_EXECUTED` (GTID subset semantics, the same comparison used by `pickFreshestCandidate` at `internal/controller/topology.go:1010-1054`). On success, proceed to Promoting with `transactionsLost: 0` (by construction). On timeout at `maxLagWait`, **roll back**:
   - Clear `super_read_only=OFF` on old primary.
   - Re-apply primary role label.
   - Do not flip DNS (never flipped yet).
   - Stamp `phase: Failed`, `message: "target pdx did not reach source GTID within 5m; fence released, primary iad still active"`.
   - Event `PlannedFailoverFailed{reason: LagTimeout}`.
   - Metric `bloodraven_planned_failovers_total{result="failed_timeout"}` increments.

5. **Promoting** — call `FailoverController.Execute(ctx, target, oldPrimary, targetName)` at `internal/controller/failover.go:23`. This reuses the same promotion sequence as emergency failover: fence (already fenced — idempotent), kill app connections on old primary, `WaitForRelayLogDrain`, `STOP REPLICA`, `RESET REPLICA ALL`, capture promotion GTID, clear super_read_only, `read_only=0`. Because the target was already caught up in WaitingForLag, `WaitForRelayLogDrain` returns nearly instantly. DNS is flipped via `tm.dns.UpdateDNSRecord` only after the MySQL steps complete and the target is verified writable.

6. **Resuming** — sync primary role label onto the new primary Pod (piggybacks on the next `syncPodLabels` tick), update `status.activeSite`, `status.lastFailoverTarget`, `status.lastFailover`, `status.promotionGtidExecuted` *identically* to the emergency path at `internal/controller/runner.go:593-597`. These fields are the same fields — the `lastFailover` stamp is what makes the cooldown apply to any follow-on emergency, which is exactly the behaviour the wishlist item asks for.

7. **Succeeded** — stamp terminal status, emit `PlannedFailoverCompleted`, clear annotation. Terminal status stays on the CR until the next planned failover replaces it.

Phase advancement is one-phase-per-reconcile (same discipline as `reconcileInPlaceRestore` at `internal/controller/restore_inplace.go:259-272`), so operator restarts always land on a well-defined observable state. The full sequence typically completes in 2-5 reconciles (seconds in the common zero-lag case), but the state machine is correct across arbitrary restart timing.

### Where the phase transitions live

- Annotation parsing + validation: new file `internal/controller/planned_failover.go` (mirrors `internal/controller/reclone.go`).
- Annotation handling (consume/validate/clear): new method `handlePlannedFailoverAnnotation` on `TopologyManagerRunner` in `internal/controller/runner.go`, invoked from the same reconcile path that calls `handleRecloneAnnotation` (runner.go:245-273).
- State machine driver: new file `internal/controller/planned_failover_reconciler.go` with `reconcilePlannedFailover` entrypoint, called from the `MysqlFailoverGroupReconciler.Reconcile` loop alongside `reconcileInPlaceRestore` (restore_inplace.go:203).
- Topology manager coordination: when a planned failover is in `Draining`, `WaitingForLag`, or `Promoting`, the topology manager **must not** trigger an automatic failover on the same CR. The existing `tm.promotedSite != ""` guard (topology.go:922) is insufficient; add a `tm.plannedFailoverActive` flag set by the runner during Draining→Resuming.

## Rollback and failure modes

**The fencing must never strand the cluster.** Every path out of Draining / WaitingForLag / Promoting either completes a promotion or restores the old primary to writable:

| Failure | Rollback |
|---|---|
| Target unreachable during Validating | No fence applied; emit `PlannedFailoverRejected`. |
| Cooldown active during Validating | No fence applied; emit `PlannedFailoverRejected{CooldownActive, retryAfter: T}`. |
| `super_read_only=ON` on old primary fails | Retry; on persistent failure, stamp Failed. Old primary is still writable (the fence never took); cluster state unchanged. |
| Zero-lag wait times out | Clear `super_read_only=OFF`, re-apply primary label, stamp Failed. Old primary resumes writes. |
| Old primary dies mid-drain | Hand off to emergency failover path. The topology manager observes `StateUnreachable` on the fenced primary; its usual automatic flow takes over. Stamp planned-failover as `Failed{reason: SourceCrashed}` but allow the emergency machinery to promote the (caught-up) target. |
| `FailoverController.Execute` fails mid-promotion | Stamp Failed. Manual recovery required — this is the same failure mode as emergency failover failing mid-sequence, documented in the failure-mode matrix. |
| DNS flip fails after promotion | Promotion has completed, but planned failover fails instead of stamping success. Operator/Event output must surface that DNS did not move so an operator can retry or repair the DNS provider. |

Operator restart during any phase resumes from `status.plannedFailover.phase` on reconcile — the state machine is idempotent at phase boundaries by construction.

## Interaction with the topology manager

The topology manager at `internal/controller/topology.go` runs its poll loop independently of the reconciler. While a planned failover is active, the poll loop must **not** see the fenced primary as a candidate for automatic action. Two protections:

1. **`plannedFailoverActive` flag.** Set by `reconcilePlannedFailover` when entering Draining and cleared on Succeeded / Failed. Checked at the top of the topology manager's decision step, same place as `tm.promotedSite != ""` (topology.go:922). Prevents automatic promotion of a *different* candidate while the planned path is mid-flight.

2. **Shared `lastFailover` / `lastFailoverTarget`.** The planned path writes these fields on success, so the topology manager's cooldown check applies to *both* kinds of failover uniformly. An emergency failover triggered within `failoverCooldown` of a planned failover is blocked; an automatic failover during an active planned failover is blocked by the flag above.

The inverse (planned failover attempted while emergency failover is mid-flight) is blocked at Validating by the `status.lastFailover` cooldown check plus a rejection if the topology manager's in-memory `promotedSite` is non-empty. The runner exposes `tm.isPromoting()` for this check.

## Metrics

Added to `internal/metrics/metrics.go` in the same style as the existing failover metrics (lines 32-35):

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `bloodraven_planned_failovers_total` | counter | `target_site`, `result` (`success`\|`rejected`\|`failed_timeout`\|`failed_other`) | Lifetime planned-failover attempts. Separate from `bloodraven_failovers_total` so dashboards can tell "operator did this" from "admin did this". |
| `bloodraven_planned_failover_duration_seconds` | histogram | `target_site` | End-to-end wall-clock duration, measured from `status.plannedFailover.startTime` to `completionTime`. Buckets tuned around typical planned switchovers: `[1, 2, 5, 10, 30, 60, 120, 300]`. |
| `bloodraven_planned_failover_lag_wait_seconds` | histogram | `target_site` | Time spent in `WaitingForLag` alone. Tells the admin "how long did the target take to catch up before we promoted". Buckets: `[0.5, 1, 2, 5, 10, 30, 60, 120, 300]`. |

`bloodraven_failovers_total` (automatic only) and `bloodraven_dns_flips_total` continue to behave exactly as today — the planned path also increments `bloodraven_dns_flips_total{site=target}` because `tm.dns.UpdateDNSRecord` is called from the shared code path.

Rationale for a separate counter (not a new `kind=` label on `bloodraven_failovers_total`): existing dashboards and alerts reference that metric without a `kind` label, and silently adding one changes the meaning of `sum(rate(bloodraven_failovers_total[5m]))`. Keep the old contract intact; publish the new counter alongside it.

## Events

Emitted by `r.recorder.Eventf` on the `MysqlFailoverGroup`, following the `RecloneRequested`/`RecloneRejected` and `RestoreInPlaceStarted`/`RestoreInPlaceRejected` precedents:

| Event | Type | Reason | Triggered |
|---|---|---|---|
| `PlannedFailoverStarted` | Normal | admin requested graceful switchover | Pending → Validating |
| `PlannedFailoverRejected` | Warning | target invalid, cooldown active, concurrent op, etc. | Validating → terminal (clear annotation) |
| `PlannedFailoverSkipped` | Normal | target already equals activeSite | idempotent no-op |
| `PlannedFailoverDraining` | Normal | old primary fenced; waiting for target lag | Draining entry |
| `PlannedFailoverLagOK` | Normal | target caught up; proceeding to promote | WaitingForLag → Promoting |
| `PlannedFailoverCompleted` | Normal | promotion done, N transactions lost | Succeeded |
| `PlannedFailoverFailed` | Warning | carries reason (LagTimeout, SourceCrashed, ExecuteFailed) | Failed |

## Docs

`docs/docs/operations.mdx:9-38` gets rewritten top-down: the `kubectl exec` dance becomes an appendix ("if the operator is unreachable and you must promote by hand"), and the primary flow is:

```bash
kubectl annotate mysqlfailovergroup orders \
  bloodraven.shipstream.io/planned-failover=pdx

kubectl get mysqlfailovergroup orders -o jsonpath='{.status.plannedFailover}'
```

New page `docs/docs/planned-failover.mdx` covers: when to use it, pre-flight checks, what to expect in Events, how to interpret `transactionsLost`, the cooldown-rejection path, and the rollback behaviour on lag timeout.

`docs/docs/failure-mode-matrix.mdx` gains a new row: *planned failover* × *each failure point* × *observable signal* × *operator action*.

`docs/docs/durability-and-rpo.mdx` gets a paragraph clarifying that planned failover is the one Bloodraven operation with an RPO of zero by construction: the zero-lag gate guarantees `transactionsLost: 0` on success; any scenario that would produce loss routes to the Failed rollback instead.

## Testing

Following the AGENTS.md pre-PR gate:

1. Unit tests:
   - `internal/controller/planned_failover_test.go`: annotation parsing (including `:maxLagWait=` form), validation matrix (unknown site, dr-only target, active-site no-op, cooldown active, in-flight restore, in-flight update), cooldown interaction with `status.lastFailover`.
   - `internal/controller/planned_failover_reconciler_test.go`: phase-transition table tests (Pending → ... → Succeeded / Failed paths), rollback on LagTimeout (fence released, label restored, old primary writable), rollback on SourceCrashed hand-off, operator-restart resumption from each phase.
   - `internal/controller/topology_test.go` augment: `plannedFailoverActive` blocks automatic promotion; `status.lastFailover` written by planned path blocks emergency failover within cooldown.
2. Envtest (`make test-envtest`): CRD validation (`spec.plannedFailover` defaults, status subresource updates), annotation-driven reconcile triggers status transitions.
3. E2E in `test/e2e/planned_failover_test.go`: against the playground-style two-site cluster, annotate, assert `phase: Succeeded` with `transactionsLost: 0`, assert `bloodraven_planned_failovers_total{result="success"}` advanced, assert that an immediate follow-on planned annotation is rejected with `CooldownActive`.
4. Playground integration: `playground/planned-failover.sh <target-site>` wraps the annotate + status-watch flow for demos.
5. Chaos follow-up in `playground/chaos-results.md`: "planned failover while lag is artificially high" → assert LagTimeout rollback, primary remains writable.

## Phased rollout

- **Phase 1** (this PR, single branch):
  - `PlannedFailoverAnnotation` constant, parser, validator.
  - `status.plannedFailover` CRD types + deepcopy + CRD YAML (both `config/crd/bases/` and `charts/bloodraven/crds/`).
  - State machine driver + rollback paths.
  - `plannedFailoverActive` topology-manager flag.
  - Metrics + events.
  - Docs: rewrite `operations.mdx#manual-promotion`, new `planned-failover.mdx`, matrix row.
  - Unit + envtest + playground script.
- **Phase 2**:
  - `spec.plannedFailover.onCooldown: defer` behaviour (retry on cooldown expiry instead of rejecting).
  - `spec.plannedFailover` knobs fully respected (maxLagWait / drainTimeout).
  - E2E in `test/e2e`.
  - Grafana dashboard panel (part of Wishlist #16).
- **Phase 3**:
  - `kubectl bloodraven promote <group> <site>` subcommand (Wishlist #18) — trivial wrapper around `kubectl annotate` + status watch.
  - Optional queued / scheduled planned failovers ("promote at 02:00 UTC").
  - Pre-flight `--dry-run` mode that runs Validating only and reports what would happen.

## Open questions

1. **Do we need a confirm-token to guard against `kubectl annotate ... pdx` typos?** `RecloneAnnotation` uses a `:<gtidPrefix>` safety interlock because reclone destroys data. Planned failover doesn't destroy data on either side — the rollback paths guarantee the old primary stays consistent. Proposal: **no confirm token**. The cost of a mistaken target is ~47s of (rolled-back) read-only time on the current primary; the admin simply re-annotates with the correct site. If practice proves this too loose, Phase 2 can add a `:confirmSource=<currentActiveSite>` key=value similar to reclone's gtid prefix.

2. **Should the planned path use `super_read_only=ON` or strip the primary label first?** Ordering matters for in-flight transactions. Proposal: strip the label *first* (new connections stop arriving at the old primary), then `super_read_only=ON` (in-flight writes fail), then `KillAppConnections` (force reconnect via DNS/Service). This matches the restore-in-place Fencing phase ordering (`internal/controller/restore_inplace.go:345-360`) and minimises the "fenced but still attracting connections" window.

3. **What if `spec.failoverCooldown` is set very short (e.g. 0) for testing?** Cooldown check becomes a no-op. That's fine — documented behaviour. The chaos / playground docs can note that production clusters should keep the default 5m.

4. **Is `status.plannedFailover` part of the `MysqlFailoverGroupStatus` API contract?** Yes — follow the `status.restoreInPlace` precedent (`api/v1alpha1/types.go:442-447`). Add the field to the existing Status struct so `kubectl describe` surfaces it. Stable at v1alpha1.

5. **Should `transactionsLost` ever be non-zero on a successful planned failover?** No, by construction — we only enter Promoting after the target's `GTID_EXECUTED` ⊇ source's fenced `GTID_EXECUTED`. The field is kept for symmetry with emergency-failover's data-loss accounting (`status.promotionGtidExecuted`) and to catch any bug where the lag gate lets through a less-than-full subset.

6. **Interaction with `spec.updateStrategy: OrderedUpdate` mid-rollout?** Reject during Validating if `status.updatePhase` is non-empty. Rollouts and switchovers don't compose cleanly in Phase 1; revisit for Phase 2 if users hit this in practice.

## Acceptance criteria

- `kubectl annotate mysqlfailovergroup orders bloodraven.shipstream.io/planned-failover=<target-site>` triggers a graceful switchover ending in `status.plannedFailover.phase: Succeeded` and `status.activeSite: <target-site>` on the happy path.
- `bloodraven_planned_failovers_total{target_site=<t>, result="success"}` increments on success; `bloodraven_planned_failover_duration_seconds` records wall-clock duration.
- `status.lastFailover` is populated by the planned path, and a follow-on planned *or* emergency failover within `spec.failoverCooldown` is rejected/blocked — the cooldown is honoured, not bypassed.
- Lag-timeout rollback leaves the old primary writable, labels restored, no DNS flip, `status.plannedFailover.phase: Failed{reason: LagTimeout}`.
- Source-primary crash mid-drain hands off cleanly to emergency failover; the CR shows `status.plannedFailover.phase: Failed{reason: SourceCrashed}` alongside the emergency promotion's stamps.
- Double-annotation / typo flows: invalid annotations emit a `PlannedFailoverRejected` event and are cleared, mirroring `handleRecloneAnnotation`.
- Pre-PR gate (AGENTS.md): `make generate && make manifests` clean, `make vet`, `make lint`, `make test`, `make test-envtest` all pass.
- Helm chart ClusterRole requires no changes (planned failover uses existing primitives).
- Helm chart CRD mirror: `config/crd/bases/shipstream.io_mysqlfailovergroups.yaml` regeneration copied to `charts/bloodraven/crds/`.
- Docs added / updated: `docs/docs/planned-failover.mdx` (new), `operations.mdx` (rewritten manual-promotion section), `failure-mode-matrix.mdx` (new row), `durability-and-rpo.mdx` (zero-RPO paragraph).
