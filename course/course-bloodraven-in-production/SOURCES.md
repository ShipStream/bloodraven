# Sources

Grounding ledger for **Bloodraven in Production**. Every load-bearing number, API, and claim in this
course traces to a row here. Rows were produced by a six-angle grounding expedition against the
Bloodraven repository at `v0.9.1` (`main` @ `ecb1799`, `git describe` = `v0.9.1-3-gecb1799`), the
shipped CRDs and Helm chart, the recorded chaos-run forensics, the public GitHub issue tracker, and
current upstream documentation.

**Repo paths** are relative to the Bloodraven repository root.

**A number that does not appear below may not appear in the course.** Arithmetic derived from these
figures is allowed where the derivation is shown.

## Primary

- **The Bloodraven source tree** — `internal/controller/`, `internal/state/`, `internal/sidecar/`,
  `internal/mysql/`, `api/v1alpha1/` — the authoritative description of what the operator does.
  Where the docs and the code disagree, this course teaches the code and says so.
- **`config/crd/bases/shipstream.io_mysqlfailovergroups.yaml`** and
  **`charts/bloodraven/crds/`** — the defaults and CEL rules that actually ship.
- **`internal/playground/scenarios/`** (49 registered chaos scenarios) and
  **`playground/chaos-results/`** — real measured timings against a live cluster.
- **`docs/docs/*.mdx`** — the official documentation, used as a cross-check. Rows below record where
  it drifted from the code.

## Ledger

### Poll loop, state machine, and the decision matrix

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 1 | `spec.pollInterval` default | `// +kubebuilder:default="2s"` | `api/v1alpha1/types.go:86-87` | A1 |
| 2 | Operator hard-defaults 2s when the pointer is nil | `pollInterval := int64(2 * time.Second)` | `internal/controller/reconciler.go:1962-1965` | A1 |
| 3 | `spec.failureThreshold` default 3 | `site.failCount++; if site.failCount >= tm.cfg.FailureThreshold { return state.StateUnreachable }` | `internal/controller/topology.go:1536-1540`; default `api/v1alpha1/types.go:89-92` | A1/A2 |
| 4 | `spec.recoveryThreshold` default 2 — gates the transition **to writable**, not failure detection | `site.recoveryCount++; if site.recoveryCount >= tm.cfg.RecoveryThreshold { return state.StateWritable }` | `internal/controller/topology.go:1553-1557`; default `api/v1alpha1/types.go:94-97` | A1/A2 |
| 5 | Detection delay = `pollInterval × failureThreshold` = 2 s × 3 = **6 s**. `recoveryThreshold` is not a term in this sum | `computeState` at `topology.go:1532-1559`; loop at `:878-901` | derived, A2 | A2 |
| 6 | Transition to `read-only` is immediate (single poll) and resets the recovery counter | `if readOnly { site.recoveryCount = 0; return state.StateReadOnly }` | `internal/controller/topology.go:1546-1549` | A1 |
| 7 | Four per-site states | `StateUnknown` / `StateWritable // read_only=0` / `StateReadOnly // read_only=1` / `StateUnreachable // connection failed` | `internal/state/machine.go:7-11` | A1 |
| 8 | The poll's SQL | `err := m.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)` | `internal/mysql/checker.go:178` | A1 |
| 9 | Per-site probe ceiling 5 s, all sites polled in parallel | `pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)` | `internal/controller/topology.go:943-960` | A1 |
| 10 | **Poll interval is adaptive and undocumented**: doubles per failure past `failureThreshold`, exponent capped at 4, hard cap 30 s | `interval := base * time.Duration(1<<uint(backoffFails))` … `if cap := 30 * time.Second; interval > cap { return cap }`; `const maxPollBackoffExponent = 4` | `internal/controller/topology.go:876, 918-929` | A1 |
| 11 | A writable observation on a **non-promotable** site bypasses `recoveryThreshold` entirely | `// A successful writable observation on a non-promotable site is an immediate safety fact. Do not debounce authority invalidation or fencing behind the normal recovery threshold.` | `internal/controller/topology.go:972-977` | A1 |
| 12 | Replication health has its own separate 2-tick debounce | `const replicatingStreakThreshold = 2` | `internal/controller/topology.go:1071, 1097-1101` | A1 |
| 13 | Matrix: fence-first early return preempts every other row | `if len(action.FenceSites) > 0 { action.Alert = fmt.Sprintf("writable non-promotable site requires fencing (%s)", …); action.Reason = "Degraded"; return action }` | `internal/state/matrix.go:107-114` | A1 |
| 14 | Matrix: TotalLoss | `if len(unreachable) == coreCount { action.Alert = "TOTAL LOSS: all sites are unreachable"; action.Reason = "TotalLoss" }` | `internal/state/matrix.go:117-121` | A1 |
| 15 | Matrix: SplitBrain is strictly `len(writable) > 1` among core sites | `if len(writable) > 1 { action.SplitBrain = true; action.Alert = fmt.Sprintf("SPLIT BRAIN: %d sites are writable (%s)", …) }` | `internal/state/matrix.go:124-130` | A1 |
| 16 | Matrix: Failover requires zero writable **and** ≥1 unreachable **and** ≥1 read-only; its `Reason` is `"Degraded"`, not `"Failover"` | `if len(unreachable) > 0 && len(readOnly) > 0 { candidates := RankPromotionCandidates(readOnly, sitePriorities); … action.Reason = "Degraded" }` | `internal/state/matrix.go:137-144` | A1 |
| 17 | All-read-only with no unreachable peer refuses to elect, by design | `// Without any unreachable peer we refuse to auto-elect a primary (all-read-only is a startup or recovery state that needs human input).` | `internal/state/matrix.go:132-136` | A1 |
| 18 | Matrix: NoPrimary, with a two-site-specific message | `if len(readOnly) == 2 && len(unreachable) == 0 { action.Alert = "NO PRIMARY: both sites are read-only" } else { action.Alert = "NO PRIMARY: no writable site available" }` | `internal/state/matrix.go:149-154` | A1 |
| 19 | Matrix: **Degraded (primary up, peer down)** — a real row absent from the docs table | `if len(unreachable) > 0 { action.Alert = fmt.Sprintf("%s unreachable while %s is primary", …); action.Reason = "Degraded" }` | `internal/state/matrix.go:159-163` | A1 |
| 20 | Matrix: Healthy = exactly one writable, zero unreachable | `action.Reason = "Healthy"` | `internal/state/matrix.go:165-166` | A1 |
| 21 | `EvalCrossSite` is pure; split-brain auto-resolution is layered on by the caller | `// The function is pure: it never considers history or policy beyond the supplied priorities.` | `internal/state/matrix.go:63-67` | A1 |
| 22 | The matrix is evaluated every poll; mutating actions only on a transition | `// Evaluate every poll so all status snapshots carry the current topology condition. Mutating cross-site actions remain transition-driven.` | `internal/controller/topology.go:1023-1029` | A1 |
| 23 | Status condition reasons come straight from the matrix `Reason` | `DegradedReason string // one of "Healthy", "Degraded", "SplitBrain", "NoPrimary", "TotalLoss", or ""` | `internal/controller/topology.go:166`; `internal/controller/runner.go:910, 1298-1301` | A1 |

### The failover sequence as implemented

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 24 | Step 1 — fence old primary; error only **warns** | `if err := oldPrimary.SetSuperReadOnly(ctx, true); err != nil { f.logger.Warn("failed to fence old primary (may be unreachable)", "error", err) }`; SQL `SET GLOBAL super_read_only = ON` | `internal/controller/failover.go:25-30`; `internal/mysql/replication.go:31-35` | A1 |
| 25 | Step 2 — **undocumented**: kill application connections on the old primary | `SELECT id FROM information_schema.processlist WHERE id != CONNECTION_ID() AND command NOT IN ('Binlog Dump', 'Binlog Dump GTID')` then `KILL %d` | `internal/controller/failover.go:32-37`; `internal/mysql/replication.go:44-58` | A1 |
| 26 | Step 3 — relay-log drain, 30 s timeout, **non-fatal** on failure | `if err := candidate.WaitForRelayLogDrain(ctx, 30*time.Second); err != nil { f.logger.Warn("relay log drain did not complete cleanly, proceeding with promotion", …) }` | `internal/controller/failover.go:41-45` | A1/A2 |
| 27 | Drain exits early when the SQL thread is running and caught up | `if rs.SQLRunning { if rs.SecondsBehindSource != nil && *rs.SecondsBehindSource == 0 { return nil } }` | `internal/mysql/replication.go:263-284` | A1 |
| 28 | Drain internals: 500 ms first wait, doubling to a 4 s ceiling, one SQL-thread restart with retry | `interval := 500 * time.Millisecond`; `const maxInterval = 4 * time.Second` | `internal/mysql/replication.go:251-302` | A1 |
| 29 | Steps 4–5 — `STOP REPLICA` then `RESET REPLICA ALL`, both **fatal** | `if err := candidate.StopReplica(ctx); err != nil { return "", err }` | `internal/controller/failover.go:48-55`; SQL `internal/mysql/replication.go:67, 75` | A1 |
| 30 | Step 6 — record promotion GTID, **non-fatal** | `promotionGtid, err := candidate.GetGtidExecuted(ctx); if err != nil { f.logger.Warn("failed to record promotion GTID", "error", err) }`; SQL `SELECT @@global.gtid_executed` | `internal/controller/failover.go:58-61`; `internal/mysql/replication.go:190` | A1 |
| 31 | Steps 7–8 — promotion is **two** statements, both fatal | `SetSuperReadOnly(ctx, false)` then `SetReadOnly(ctx, false)` → `SET GLOBAL super_read_only = OFF`, `SET GLOBAL read_only = OFF` | `internal/controller/failover.go:63-71`; `internal/mysql/replication.go:31-35, 82-87` | A1 |
| 32 | Completion log | `f.logger.Info("failover complete", "promotedSite", candidateSite, "promotionGtid", promotionGtid)` | `internal/controller/failover.go:73` | A1 |
| 33 | Writable confirmation is **synchronous**, in the same call stack — not the next poll | `if err := tm.confirmWritable(ctx, candidate); err != nil { tm.logger.Error("promotion succeeded but writable confirmation failed; DNS not flipped", …); return }` | `internal/controller/topology.go:1778-1782, 851-862` | A1 |
| 34 | The durable failover record and counter are stamped **before** the DNS flip, deliberately | `// … so a DNS-provider outage cannot erase the fact that a promotion happened.` | `internal/controller/topology.go:1784-1802` | A1 |
| 35 | Node taints are **not** a step of the failover sequence — they are a pure function of per-site transitions applied earlier in the same poll | `tm.applyPerSiteAction(...)` at `topology.go:1006` vs `tm.applyCrossSiteAction(...)` at `:1028` | `internal/controller/topology.go:1000-1008, 1027-1029` | A1 |
| 36 | Taint action is a pure function of the transition | `case curr == StateWritable: … a.Taint = &f` / `case curr == StateReadOnly \|\| curr == StateUnreachable: if prev == StateWritable \|\| prev == StateUnknown { … a.Taint = &t }` / `// read-only <-> unreachable: no new action` | `internal/state/machine.go:44-56` | A1/A6 |
| 37 | Source convergence is **not** part of the promotion sequence — independent poll stage, own 20 s budget | `const sourceConvergenceOperationTimeout = 20 * time.Second` | `internal/controller/topology.go:1112-1118`; `internal/controller/source_convergence.go:14` | A1 |
| 38 | Convergence demands a **direct** source; replication chains are not accepted | `if current == expected && repl.IORunning && repl.SQLRunning { … sourceConvergenceConverged … }` else `tm.repointReplica(...)` | `internal/controller/source_convergence.go:72-90` | A1 |
| 39 | GTID freshness is the **primary** promotion selector; the priority list is only a tiebreaker | `promote = tm.pickFreshestCandidate(ctx, action.PromotionCandidates)`; `// GTID freshness is the primary selector (minimise data loss on promotion); ties or incomparable sets fall back to candidate order` | `internal/controller/topology.go:1726, 2010-2016` | A1 |
| 40 | Measured emergency-failover time, clean primary kill: **12.0 s** to `activeSite` flip, reproducible across ≥9 independent runs (12.004, 12.005 ×3, 12.006 ×2, 12.007, 12.011, 12.02 s; one at 13.008 s). Conditions: k3d playground, 2 s poll / 3 failures, candidate caught up, injection = scale-to-0 or force-delete | `playground/chaos-results/live-20260502T051703Z/20260502T051703Z/01-clean-primary-kill/scenario.log` | A2 |
| 41 | Measured failover time **with unapplied relay logs**: **36.005 s** — scenario 14 pauses the replica SQL applier and seeds 5 s of writes, then kills the primary. This is when the 30 s drain budget is actually spent | `playground/chaos-results/20260430T203117Z/14-failover-with-replication-lag/scenario.log`; `internal/playground/scenarios/s14_failover_with_replication_lag.go` | A2 |

### Cooldown, history, and the re-assert exception

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 42 | `spec.failoverCooldown` default 5m | `// +kubebuilder:default="5m"`; `cooldown := time.Duration(cfg.FailoverCooldown); if cooldown == 0 { cooldown = 5 * time.Minute }` | `api/v1alpha1/types.go:100`; `internal/controller/topology.go:491-494` | A1/A2 |
| 43 | Cooldown enforcement point and log msg | `if !lastFailover.IsZero() && tm.clock.Since(lastFailover) < tm.failoverCooldown { tm.logger.Info("failover blocked by anti-flap cooldown", "lastFailover", lastFailover, "cooldown", tm.failoverCooldown); return }` | `internal/controller/topology.go:1741-1745` | A1 |
| 44 | Cooldown does **not** suppress: split-brain fencing, non-promotable fencing, source convergence, old-primary recovery, reclone, DNS reconcile | Poll call sites at `topology.go:1012, 1043, 1114, 1121, 1133, 1136` — none consults `tm.failoverCooldown` | `internal/controller/topology.go` | A1 |
| 45 | The **ordered-update handoff promotion is not cooldown-gated**, yet still records a failover and increments the counter | `tm.recordFailover(...)` / `metrics.FailoversTotal.WithLabelValues(target).Inc()` inside the handoff callback; `grep -n cooldown internal/controller/updater.go` → no match | `internal/controller/topology.go:2483-2492` | A1 |
| 46 | Durable location #1 — CR status subresource | `rec := FailoverRecord{LastFailoverTarget: fg.Status.LastFailoverTarget}` | `internal/controller/failover_state.go:200-206` | A1 |
| 47 | Durable location #2 — object annotations, RFC3339 second precision, written as a pair by JSON merge patch | `LastFailoverAnnotation = "bloodraven.shipstream.io/last-failover"`; `LastFailoverTargetAnnotation = "bloodraven.shipstream.io/last-failover-target"` | `internal/controller/failover_state.go:40-41, 134, 163-165` | A1 |
| 48 | The duplication is deliberate — status is a subresource with its own RBAC and admission chain | `// The duplication is the point. status is a subresource: writes to it travel a separate API path with its own RBAC rule (mysqlfailovergroups/status) …` | `internal/controller/failover_state.go:17-39` | A1 |
| 49 | Restart rehydration takes the **later** copy, with a 5 m future-clock guard; ties go to status | `FailoverClockSkewGrace = 5 * time.Minute`; `winner := NewerFailoverRecord(statusRecord, oobRecord)` | `internal/controller/failover_state.go:46, 252, 288-292` | A1 |
| 50 | Re-assert preconditions: subsystem gates, rate limit, every non-target peer read-only, target read-only **and** promotable, target GTID contains the recorded promotion GTID **and** every peer's GTID | `if tm.bootstrapBlocksCrossSite() \|\| tm.isUpdating() \|\| tm.isTopologyFrozen() \|\| tm.isPlannedFailoverActive() { return false }`; `if !targetGtid.Contains(promotionGtid) { … return false }` | `internal/controller/topology.go:3262-3284, 3303-3312, 3337-3341, 3360-3364` | A1 |
| 51 | An unparseable recorded promotion GTID **refuses** rather than skipping the gate | `tm.logger.Warn("primary re-assert refused: recorded promotion GTID set failed to parse — status corrupted or manually edited?", …)` | `internal/controller/topology.go:3328-3336` | A1 |
| 52 | Re-assert log msg, verbatim | `tm.logger.Warn("re-asserting fenced promoted primary: no site is writable and the last failover target is GTID-complete; restoring writability", "site", target)` | `internal/controller/topology.go:3373-3374` | A1 |
| 53 | Re-assert metric | `metrics.PrimaryReassertTotal.WithLabelValues(target).Inc()` → `Name: "bloodraven_primary_reassert_total"`, labels `[]string{"site"}` | `internal/controller/topology.go:3388`; `internal/metrics/metrics.go:82-84` | A1 |
| 54 | The re-assert rate limit uses a **separate** timer, so a re-assert can fire inside the failover cooldown window | `lastReassert := tm.lastReassert` … never compared against `tm.lastFailover` | `internal/controller/topology.go:3269, 3282` | A1 |
| 55 | Cross-site mutation is suppressed wholesale during in-place restore, planned failover, topology-relevant bootstrap, or ordered update | `if tm.isTopologyFrozen() { … return }` / `if tm.isPlannedFailoverActive() { … return }` | `internal/controller/topology.go:1619-1645` | A1 |

### Site roles

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 56 | Three roles, enum-validated, default `primary-candidate` | `// +kubebuilder:validation:Enum=primary-candidate;dr-only;read-only`; `// +kubebuilder:default="primary-candidate"` | `api/v1alpha1/types.go:296-311, 323-327` | A1 |
| 57 | Promotability is exactly `role == primary-candidate` | `func (t *siteTracker) isPromotable() bool { return t.role == state.SiteRolePrimaryCandidate }` | `internal/controller/topology.go:221-223` | A1 |
| 58 | `role: read-only` sites are excluded from `coreCount` and all three tallies; `dr-only` sites are **not** excluded | `if obs.Role != SiteRoleReadOnly { coreCount++ }` | `internal/state/matrix.go:77-86` | A1 |
| 59 | Any writable site that is not `primary-candidate` is routed to FenceSites | `if obs.State == StateWritable && obs.Role != SiteRolePrimaryCandidate { action.FenceSites = append(action.FenceSites, obs.Name); continue }` | `internal/state/matrix.go:80-83` | A1 |
| 60 | A writable reader/dr-only site is fenced **every poll**, without debounce, independent of any transition | `// A writable non-promotable site is never authoritative. Enforce this on every poll so a failed fence is retried without waiting for a transition.` | `internal/controller/topology.go:1010-1012, 1586-1604` | A1 |
| 61 | Fence log strings | `tm.logger.Warn("fenced writable non-promotable site", "site", site.name, "role", site.role)` | `internal/controller/topology.go:1598-1601` | A1 |
| 62 | A single writable reader invalidates `activeSite` immediately (authority is ambiguous), except during its own clone | `if tm.sites[i].state == state.StateWritable { if active != nil { return "" } … }` | `internal/controller/topology.go:722-753` | A1 |
| 63 | Final belt-and-braces refusal at the promotion call site | `if !candidate.isPromotable() { tm.logger.Error("promotion target is not a primary-candidate — refusing", "site", candidate.name, "role", candidate.role); return }` | `internal/controller/topology.go:1752-1756` | A1 |
| 64 | Readers never taint | `if site.role == state.SiteRoleReadOnly \|\| site.taintSelector == "" { return }` | `internal/controller/topology.go:1569-1571` | A1 |
| 65 | A planned failover targeting a reader is hard-refused: `only primary-candidate sites may be promoted` | | `internal/controller/planned_failover.go:177`; chaos scenario 43 | A4 |

### Split brain

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 66 | **`spec.splitBrainPolicy.preferSite` does not exist.** The real field is `sitePriorities`, an ordered list | `SitePriorities []string` is the only member of `SplitBrainPolicySpec`, with the JSON tag `sitePriorities,omitempty`. Grepping `preferSite` across `config/crd/bases/` and `charts/bloodraven/crds/` returns no match | `api/v1alpha1/types.go:246-260`; `config/crd/bases/shipstream.io_mysqlfailovergroups.yaml:6390-6405` | A1/A6 |
| 67 | Field doc: empty `sitePriorities` = manual resolution, alert only | `// When omitted, or when SitePriorities is empty, the operator takes no automated action and alerts only (manual resolution required).` | `api/v1alpha1/types.go:165-172` | A6 |
| 68 | Tier 1 — prior failover history fences immediately, regardless of policy, but **only if the recorded target is itself live, writable, and promotable** | `if keepSite := tm.getSite(lastFailoverTarget); keepSite != nil && keepSite.state == state.StateWritable && keepSite.isPromotable() { tm.fenceSitesExcept(ctx, lastFailoverTarget, false) }` | `internal/controller/topology.go:1682-1688` | A6 |
| 69 | Tier 2 — no history + `sitePriorities` → fence the losers, re-promote the winner through the standard path | `winner, losers := state.ResolveSplitBrain(writable, tm.cfg.SitePriorities)` | `internal/controller/topology.go:1704-1731` | A6 |
| 70 | Split-brain winner selection deliberately does **not** consult GTID | `// GTID freshness is intentionally not consulted here — split-brain winner selection is policy-driven because every writable side may carry unique writes` | `internal/controller/topology.go:1699-1702` | A1/A4 |
| 71 | `ResolveSplitBrain` refuses non-candidates and refuses to guess with an empty priority list; it never falls back to declared order | `if len(sitePriorities) == 0 { return "", nil }`; `// It never falls back to declared order` | `internal/state/matrix.go:200-226` | A1 |
| 72 | CEL validation: priority entries must name `primary-candidate` sites | `- message: splitBrainPolicy.sitePriorities entries must match the names of sites with role 'primary-candidate'` | `charts/bloodraven/crds/shipstream.io_mysqlfailovergroups.yaml:6468-6472` | A6 |
| 73 | Emitted split-brain log msg names `sitePriorities` and carries `winner`/`fencedSite` | `tm.logger.Warn("split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities", "winner", winner, "fencedSite", loser)` | `internal/controller/topology.go:1710-1711` | A1 |
| 74 | Split-brain fencing is retried on **non**-transition polls, counting writable candidates directly rather than trusting `action.SplitBrain` | `if !action.SplitBrain && writableCandidates < 2 { return false }` | `internal/controller/topology.go:1861-1878` | A1 |
| 75 | Priority-based resolution is a **policy** decision, not a safety feature | `:::danger` … "makes split-brain resolution fast and deterministic at the cost of silently losing the loser's unreplicated writes. The loss is surfaced loudly but not prevented." | `docs/docs/failover.mdx:262-268` | A4 |

### Old-primary recovery, divergence, reclone

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 76 | Recovery states are exactly two strings plus a bare `""` backoff marker | `recoveryStateInProgress = "RecoveryInProgress"`; `recoveryStateBlocked = "RecoveryBlocked"` | `internal/controller/topology.go:467-468, 3477-3480` | A1 |
| 77 | Re-verification cadence is **30 s** | `recoveryRetryDelay = 30 * time.Second` | `internal/controller/topology.go:469-472` | A1 |
| 78 | Containment comparison: no divergence ⇔ the new primary's GTID set **contains** the old primary's | `if newGtid.Contains(oldGtid) { tm.logger.Info("no GTID divergence, auto-recovering old primary as replica", "site", oldPrimary.name)` | `internal/controller/topology.go:3622-3639` | A1 |
| 79 | Divergence path computes the set difference and count | `divergent := oldGtid.Subtract(newGtid)`; `count := divergent.TransactionCount()` | `internal/controller/topology.go:3642-3674` | A1 |
| 80 | The comparison runs against a GTID re-read **after** a defensive fence, so the set cannot grow underneath it | `// Re-read after the fence: this is the authoritative set the divergence comparison runs against (the fence guarantees it can no longer grow).` | `internal/controller/topology.go:3577-3588` | A1 |
| 81 | Recovery is deliberately **not** gated on `lastFailoverTarget`; safety comes from a directly confirmed unique writable primary | `// Deliberately NOT gated on lastFailoverTarget:` | `internal/controller/topology.go:3145-3161` | A1 |
| 82 | The rejoin SQL sequence, in order | 1. `SET GLOBAL super_read_only = ON` 2. `STOP REPLICA` 3. `RESET REPLICA ALL` 4. `CHANGE REPLICATION SOURCE TO` 5. `START REPLICA` | `internal/controller/recovery.go:10-44` | A1 |
| 83 | `CHANGE REPLICATION SOURCE` always uses `SOURCE_AUTO_POSITION=1`; adds `GET_SOURCE_PUBLIC_KEY=1` when TLS is off | `"CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1"` | `internal/mysql/replication.go:214-226` | A1 |
| 84 | `RecoveryInProgress` is persisted **before** the mutation runs, as the restart handoff | `// … this early write is the durable handoff for operator restarts that happen inside the STOP/RESET/CHANGE/START sequence.` | `internal/controller/topology.go:3631-3638` | A1 |
| 85 | Status conditions: `RecoveryInProgress` → reason `RecoveryInProgress`; `RecoveryBlocked` → reason `DivergentTransactions` | `Reason: "DivergentTransactions", Message: fmt.Sprintf("Old primary %s has %d divergent transactions — annotate with bloodraven.shipstream.io/reclone-site=%s to recover", …)` | `internal/controller/runner.go:1014-1031` | A1 |
| 86 | Divergence gauge | `metrics.DivergentTransactions.WithLabelValues(oldPrimary.name).Set(float64(count))` → `bloodraven_divergent_transactions` | `internal/controller/topology.go:3677`; `internal/metrics/metrics.go:111-114` | A1 |
| 87 | An "empty" site is decided from **shared GTID UUIDs**, not from absent user schemas | `empty = preGtid.IsEmpty()`; `if !empty && !tm.sharesHistory(ctx, preGtid, newPrimary) { … empty = !hasSchemas }` | `internal/controller/topology.go:3531-3573` | A1 |
| 88 | Why: a schema-only test would have cloned over a diverged old primary | "A cluster legitimately has no user schemas before its first app write… a *diverged* old primary would have been cloned over rather than reported as `RecoveryBlocked`." | issue [#130](https://github.com/ShipStream/bloodraven/issues/130); commit `1daffd6`; PR [#129](https://github.com/ShipStream/bloodraven/pull/129) | A4 |
| 89 | Reclone annotation key and two accepted value forms | `RecloneAnnotation = "bloodraven.shipstream.io/reclone-site"`; `reclone-site=<siteName>` or `reclone-site=<siteName>:<divergentGtidPrefix>` | `internal/controller/reconciler.go:65`; `internal/controller/reclone.go:17-46` | A1 |
| 90 | Hot reclone requires a ≥8-character prefix matching `status.sites[].divergentGtid` | `const minRecloneGtidPrefix = 8`; `if !strings.HasPrefix(divergentGtid, req.GtidPrefix) { … "does not match the observed divergentGtid" }` | `internal/controller/reclone.go:15, 106-123` | A1 |
| 91 | Cold reclone requires a literal confirm token equal to the failover-group name | `if req.GtidPrefix != "confirm="+fg.Name { return fmt.Errorf("reclone of %q rejected: cold reclone wipes the datadir and must be confirmed — set annotation bloodraven.shipstream.io/reclone-site=%s:confirm=%s", …) }` | `internal/controller/reclone.go:91-103` | A1 |
| 92 | The interlock keys on `divergentGtid` presence only, not on `RecoveryState` | `// Only the presence of divergentGtid matters for the interlock — RecoveryState ("RecoveryBlocked") is a downstream UX field and could be transiently unset during a reconcile.` | `internal/controller/reclone.go:80-90` | A1 |
| 93 | A rejected annotation emits `RecloneRejected` and is **deleted** so it cannot spam | `r.recorder.Eventf(fg, corev1.EventTypeWarning, "RecloneRejected", "%s", err.Error())` | `internal/controller/runner.go:363-384` | A1 |
| 94 | Reclone refuses the active primary and requires a confirmed writable donor | `if donor.name == recipient.name { tm.logger.Error("cannot reclone the active primary", "site", site) … }` | `internal/controller/topology.go:3753-3769` | A1 |
| 95 | `CLONE INSTANCE` statement and clone timeout default 3600 s | `fmt.Sprintf("CLONE INSTANCE FROM '%s'@'%s':3306 IDENTIFIED BY '%s'", …)` + `" REQUIRE SSL"` when TLS | `internal/mysql/clone.go:26-71`; `api/v1alpha1/types.go:112` | A1 |

### Durability and RPO

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 96 | The RPO contract in one line | "An emergency failover can lose every transaction that committed on the dying primary but had not yet replicated to the surviving site." | `docs/docs/durability-and-rpo.mdx:16` | A2 |
| 97 | `sync_binlog=1` is an **overridable default**, not a guarantee — set in the base my.cnf map, applied *before* `spec.mysqlConf` overrides | base map `internal/controller/reconciler.go:698`; overrides applied `:738-745` | | A2 |
| 98 | `innodb_flush_log_at_trx_commit=2`, also overridable | `internal/controller/reconciler.go:703` | | A2 |
| 99 | The **un-weakenable** invariants, written after user overrides: `gtid_mode=ON`, `enforce_gtid_consistency=ON`, `log_replica_updates=ON`, `log_bin`, `skip_replica_start=ON`, `plugin-load-add=mysql_clone.so`. `skip-log-bin` and `disable-log-bin` aliases are deleted outright | `internal/controller/reconciler.go:749-755, 765-771` | | A2 |
| 100 | `binlog-expire-logs-seconds = 1209600` (14 days), overridable | `internal/controller/reconciler.go:699` | | A2 |
| 101 | `spec.replication.maxLagSeconds` default 300, and it drives **only** the `ReplicationLagging` Degraded condition | `maxLagSeconds := freshFG.Spec.EffectiveMaxLagSeconds()` … `Reason: "ReplicationLagging"` | `internal/controller/runner.go:932, 965-974`; `api/v1alpha1/types.go:265-268` | A1/A2 |
| 102 | It is **not** a promotion gate | "If the primary dies while the replica is beyond the threshold, **Bloodraven still promotes the replica**" — the alternative of no writable site at all is almost always worse | `docs/docs/durability-and-rpo.mdx:201-208`; never consulted by `pickFreshestCandidate` (`internal/controller/topology.go:1690-1730`) | A4 |
| 103 | `readOnlyMaxLagSeconds` has **no** default; nil inherits `maxLagSeconds`, but an explicit `0` is meaningful (requires zero reported lag) | `api/v1alpha1/types.go:270-275`; `api/v1alpha1/site_helpers.go:86-93` | | A2 |
| 104 | Planned failover is RPO 0 **by construction** | `// TransactionsLost … By construction this is 0 on a successful planned switchover`; mechanism = fence source → snapshot its GTID → promote only when target `GTID_EXECUTED` ⊇ that snapshot | `api/v1alpha1/planned_failover_types.go:140-143`; `internal/controller/planned_failover_reconciler.go:460-463, 607` | A6 |
| 105 | The lag gate is a true GTID-set superset test, not a lag-seconds heuristic | `caughtUp, cmpErr := gtidContains(targetGtid, cur.SourceGtidAtFence)`; `gtidContains` → `return super.Contains(sub), nil` | `internal/controller/planned_failover_reconciler.go:607, 923-933` | A6 |
| 106 | PVC loss → PITR does **not** recover the tail | "The previously-active binlog lived on the destroyed PVC. It is gone forever" | `docs/docs/durability-and-rpo.mdx:163-181` | A2/A4 |
| 107 | PITR cannot reach back past the async-replication cutoff | "Transactions the old primary committed but never shipped are not in the replica's binlog stream and therefore not in PITR's replay material." | `docs/docs/durability-and-rpo.mdx:170-174` | A4 |

### Application integration, DNS, taints, Dragonfly

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 108 | Four Service **kinds** per group; object count is `2×len(sites) + 2` | `reconcileSiteService` / `reconcileInternalSiteService` (per site) then `reconcilePrimaryService` / `reconcileReplicasService` | `internal/controller/reconciler.go:283, 286, 295, 298` | A6 |
| 109 | `-primary` selector is 2 keys — instance + `role=primary`, no `healthy` | `map[string]string{ labelInstance: fg.Name, labelRole: "primary" }`; `PublishNotReadyAddresses = false` | `internal/controller/reconciler.go:1435, 1451-1453, 1462` | A6 |
| 110 | `-replicas` selector is 3 keys — instance + `role=replica` + `healthy=yes` | `labelInstance: fg.Name, labelRole: "replica", labelHealthy: "yes"` | `internal/controller/reconciler.go:1487-1490` | A6 |
| 111 | Reader endpoint eligibility requires five conjuncts: converged source, replicating, non-nil lag, canonical direct source host, and lag ≤ `EffectiveReadOnlyMaxLagSeconds()` | `internal/controller/reconciler.go:1771-1781` | | A6 |
| 112 | The fence mechanism at the Service layer: restore and planned failover stamp `role = "fenced"`, which matches **neither** selector | `internal/controller/reconciler.go:1697-1699, 1714, 1725-1748` | | A6 |
| 113 | Invalid authority deliberately sheds every endpoint | `// Invalid or incomplete authority deliberately leaves every site non-primary and every reader non-serving, shedding stale Service endpoints rather than returning early` | `internal/controller/reconciler.go:1725-1748` | A6 |
| 114 | DNSEndpoint CR: apiVersion and kind | `"apiVersion": "externaldns.k8s.io/v1alpha1"`, `"kind": "DNSEndpoint"` | `internal/platform/dns.go:68-69` | A6 |
| 115 | Fields set, always an `A` record; object named `bloodraven-<group>` | `"dnsName": d.hostname, "recordType": "A", "targets": []interface{}{ip}, "recordTTL": d.ttl` | `internal/platform/dns.go:50, 55-59, 73-78` | A6 |
| 116 | `spec.dns.ttl` default 60 | `// +kubebuilder:default=60` | `api/v1alpha1/types.go:419-422` | A6 |
| 117 | There is no create/update split — it is an idempotent server-side apply on **every poll**, which self-heals a rejected write | `d.client.Patch(ctx, obj, client.Apply, client.FieldOwner("bloodraven"), client.ForceOwnership)`; `// The desired target is always re-derived from live topology instead` | `internal/platform/dns.go:87`; `internal/controller/topology.go:288-301, 1231-1236` | A1/A6 |
| 118 | `applyDNS` is the sole writer | `// applyDNS is the ONLY place the DNS record is written.` | `internal/controller/topology.go:1238-1256` | A1 |
| 119 | Taint key format, value, and effect | `TaintKeyPrefix = "shipstream.io/db-readonly-"`; `TaintValue = "true"`; `Effect: corev1.TaintEffectNoExecute` | `internal/platform/tainter.go:19-26, 76-80` | A6 |
| 120 | NoExecute eviction is verified end to end | "the old-primary node's `db-readonly-playground:NoExecute` taint evicts a non-tolerating canary … while a canary that tolerates the same taint stays Running." | `internal/playground/scenarios/s21_noexecute_eviction_semantics.go:41-58` | A6 |
| 121 | Planned-failover phases, in order | `"";Pending;Deferred;Validating;Draining;WaitingForLag;WaitingForDragonflySync;PromotingDragonfly;Promoting;Resuming;Succeeded;Failed` | `api/v1alpha1/planned_failover_types.go:47` | A6 |
| 122 | Planned-failover annotation key | `PlannedFailoverAnnotation = "bloodraven.shipstream.io/planned-failover"` | `internal/controller/planned_failover.go:32` | A6 |
| 123 | Rollback (unfence the source) exists **only** in `WaitingForLag`; `Promoting`/`Resuming` failures fail without unfencing | three call sites, all inside `plannedFailoverWaitingForLag`; reasons `"LagTimeout"`, `"InvalidGTID"` | `internal/controller/planned_failover_reconciler.go:600, 612, 643, 745-769` | A6 |
| 124 | `spec.plannedFailover` defaults: `maxLagWait` 5m, `drainTimeout` 30s, `onCooldown` `reject` | `api/v1alpha1/planned_failover_types.go:15, 25, 37` | | A2 |
| 125 | The connection drain proceeds even when the budget is exhausted | `"drain budget exhausted after %s with %d connection(s) remaining on %q; proceeding"` | `internal/controller/planned_failover_reconciler.go:524-527` | A6 |
| 126 | Dragonfly active Service AND-gates two labels; shedding **deletes** the traffic key rather than setting a disabled value | `labelDragonflyRole = "shipstream.io/dragonfly-role"`, `labelDragonflyTraffic = "shipstream.io/dragonfly-traffic"`; `// Removing the label (set=false) is preferred over stamping a "disabled" value because the active-Service selector is an exists-and-equals check on "enabled"` | `internal/controller/dragonfly_resources.go:478-479, 506, 512-513`; `internal/controller/dragonfly_labels.go:20-27, 105-110` | A6 |
| 127 | Emergency Dragonfly promotion: `REPLTAKEOVER` first, `REPLICAOF NO ONE` fallback (sessions lost) | `"dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)"` | `internal/controller/dragonfly_topology.go:649-651, 712, 726, 751` | A6 |
| 128 | The **planned** path has no `REPLICAOF NO ONE` fallback | `next.Dragonfly.PromotionMethod = "REPLTAKEOVER"` only | `internal/controller/planned_failover_df.go:343, 413, 485` | A6 |
| 129 | Emergency Dragonfly budget is 10 s, hard-coded, and never blocks MySQL | `const budget = 10 * time.Second`; `// never returns an error to the caller, never blocks longer than a small bounded budget, and never leaves Dragonfly in a state that affects MySQL durability` | `internal/controller/dragonfly_topology.go:641-643, 666-667` | A2/A6 |
| 130 | `sessionsPreserved` is tri-state | `// SessionsPreserved is a tri-state: true if … false if sessions were lost … nil when the field is unknown` → `SessionsPreserved *bool` | `api/v1alpha1/planned_failover_types.go:236-244` | A6 |
| 131 | `maxSyncWait` default 30 s, also used as the `REPLTAKEOVER` timeout argument; the client adds 5 s of I/O grace | `api/v1alpha1/dragonfly_types.go:158-165`; `internal/dragonfly/client.go:152` | | A2/A6 |
| 132 | `onSyncTimeout` enum `proceed;fail`, default `proceed` | `api/v1alpha1/dragonfly_types.go:167-178` | | A6 |
| 133 | Dragonfly v1.38.0+ is a **support policy, not a guardrail** — nothing in `api/`, `internal/`, or the chart enforces or checks a version. The only CEL rules are "image required when enabled" and "no `:latest`" | `api/v1alpha1/dragonfly_types.go:14-15`; playground pin `playground/manifests/failovergroup.yaml:87` | | A2 |

### Fencing and the sidecar

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 134 | The `FencingMonitor` implements exactly **two** rules | `// self-fences (sets super_read_only=ON) when one of two conditions holds:` — topology mismatch, lease expiry | `internal/sidecar/fencing.go:24-38` | A6 |
| 135 | The startup safety net is a separate one-shot in `Server`, completing **before** the monitor is constructed | `internal/sidecar/server.go:225-276`; `cmd/sidecar/main.go:128-131` | | A6 |
| 136 | Rule #1 (topology mismatch) fires first and returns, so a mismatch fences without consulting the lease | `// Rule #1 — topology mismatch` at `fencing.go:411`; `// Rule #2 — lease expiry` at `:430` | `internal/sidecar/fencing.go:374-455` | A6 |
| 137 | Read-only instances never self-fence | `readOnly, err := f.mysql.IsReadOnly(ctx)` … `if readOnly { return }` | `internal/sidecar/fencing.go:376-408` | A6 |
| 138 | Peer topology is adopted only when **strictly newer** | `if f.topology.Adopt(snap.ActiveSite, snap.ObservedAt)`; `Adopt`: `if !observedAt.After(c.observedAt) { return false }`. Operator reads use `Set` unconditionally — `// the operator is always authoritative` | `internal/sidecar/fencing.go:317, 346`; `internal/sidecar/topology_cache.go:38-59` | A6 |
| 139 | The startup net is fail-closed by **staying** fenced | `"safety net: could not query active site, staying fenced"` / `"safety net: no active site reported by operator, staying fenced"` / `"safety net: confirmed standby site, staying fenced"` | `internal/sidecar/server.go:240-275` | A6 |
| 140 | `leaseTimeout` default 20 s — operator **and every peer** must be silent for the full window. One reachable peer keeps the site writable | `api/v1alpha1/types.go:436-439`; `internal/sidecar/config.go:15, 39-43` | | A2 |
| 141 | `peerCheckInterval` default 5 s | `api/v1alpha1/types.go:441-444`; `internal/sidecar/config.go:16` | | A2 |
| 142 | CEL invariants: `peerCheckInterval ≥ 1s`, `leaseTimeout ≥ 3s`, `leaseTimeout ≥ 3 × peerCheckInterval`. The shipped 20 s / 5 s sits exactly at the 3× floor | `api/v1alpha1/types.go:432-434`; `internal/sidecar/config.go:12-14, 265-283` | | A2 |
| 143 | The peer rule is explicitly **not a quorum**, and a reader counts as a peer | "A reachable peer without fresh authoritative topology can still suppress the lease-only all-peers-unreachable fence. **This is retained compatibility behavior, not a quorum guarantee**" | `docs/docs/multi-site.mdx:170-186` | A4 |
| 144 | Fencing does not close sockets — a surviving session can serve **stale reads** until the site is next promoted or demoted | `docs/docs/log-schema.mdx:192`; `internal/sidecar/mysql.go:197` (`killableConnection`) | | A4 |
| 145 | `KillAppConnections` is best-effort, single-pass, and skipped when the old primary is unreachable. "an autonomous sidecar self-fence has no operator-side connection drain… Only planned failover actually drains. Nothing else does." | `internal/controller/failover.go:25-38`; issue [#123](https://github.com/ShipStream/bloodraven/issues/123) (OPEN); PR [#137](https://github.com/ShipStream/bloodraven/pull/137) (unmerged) | | A4 |

### Operator availability and partitions

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 146 | The data plane does not need the operator | "A healthy primary and replica keep serving reads and writes with zero operator involvement. The operator is on the failure-detection and promotion path, not the request path." | `docs/docs/operator-availability.mdx:24-27` | A6 |
| 147 | Availability and correctness fail separately | "Correctness — no split brain, no silent divergence … — is preserved by the sidecar fencing layer regardless of how long the operator is gone." / "Availability is not preserved during operator downtime." | `docs/docs/operator-availability.mdx:34-38, 116-119` | A6 |
| 148 | Single replica with leader election | `replicaCount: 1`; `leaderElection:` enabled | `charts/bloodraven/values.yaml:7, 83-85` | A2/A6 |
| 149 | Partition scenario coverage: A exercised (chaos 09, 06 + DST `partitionOperatorSite`); B exercised (chaos 17); C **DST only** (`partitionPair`); **D asymmetric is analysis-only** — `pairKey` is symmetric by construction, so DST cannot express one-way reachability; E only indirectly (chaos 11) | `internal/playground/scenarios/s09_*.go`, `s06_*.go`, `s17_*.go`, `s11_*.go`; `internal/dst/schedule.go:18, 21`; `internal/dst/sim.go:227` | | A6 |
| 150 | Host-netns iptables does not partition Kubernetes Service traffic | "Host-level iptables rules in a k3d node are not reliable for Kubernetes Service traffic" | `docs/docs/network-partitions.mdx:121-126` | A4 |
| 151 | A NetworkPolicy can be a silent no-op: chaos 33 found a CNI evaluating it **post-DNAT**, so the canary "kept resolving DNS through the full 45s hold" | `playground/chaos-scenarios.md` §33 "Observed defect and fix" | | A4 |
| 152 | Issue #93: the operator reported `activeSite=iad, state=writable, Ready=True` for two minutes under a deny-all NetworkPolicy, because `Poll()` froze on a hung MySQL read — `database/sql` ctx cancellation does not reliably abort a read parked on a blackholed socket, and `Poll` waits on every site | issue [#93](https://github.com/ShipStream/bloodraven/issues/93) (CLOSED); fix commit `8bb66dd` / PR [#95](https://github.com/ShipStream/bloodraven/pull/95) | | A4 |
| 153 | The first fix attempted was wrong: `SetConnMaxLifetime(10s)` "could not help: a connection parked in a blocked read is never returned to the pool, so it is never recycled" | commit `8bb66dd` body; issue #93 | | A4 |
| 154 | Issue #128: a clone into a non-promotable reader suppressed every cross-site action **including emergency failover**, so a primary dying mid-clone left the group with zero writable sites for the clone duration | issues [#118](https://github.com/ShipStream/bloodraven/issues/118), [#128](https://github.com/ShipStream/bloodraven/issues/128); PR [#121](https://github.com/ShipStream/bloodraven/pull/121) commit `c7e828a` | | A4 |
| 155 | Issue #46: a site reported as a healthy `read-only` peer had empty `slave_master_info` and was not replicating at all — replica health was inferred from `super_read_only` without verifying replication | issue [#46](https://github.com/ShipStream/bloodraven/issues/46) | | A4 |
| 156 | A newly created or cloned MySQL pod comes up **writable** for seconds: "T+22s — new `pdx` pod Running, but **writable**… T+33s — `ALERT: SPLIT BRAIN`" | issues #46, #128 | | A4 |
| 157 | A `SET GLOBAL` that returns an error may still have landed: "cancelling the context tears down the client connection, it does not roll back a write the server already applied." Treating it as failed made the monitor re-fence the site it had just promoted | commit `ddf0087` (PR [#122](https://github.com/ShipStream/bloodraven/pull/122)) | | A4 |
| 158 | DNS propagation is external-dns's job: "**The operator cannot accelerate DNS propagation.** A stuck external-dns is an outage for writes even after the operator has 'finished'." Chaos 38 proves the CR can promote correctly while DNS stays stale under an RBAC denial | `docs/docs/failure-mode-matrix.mdx:37`; `playground/chaos-scenarios.md` §38 | | A4 |
| 159 | Replication breaking cross-site triggers **no** automatic action: "This mode is indistinguishable, from the operator's point of view, from 'replica fell behind because of I/O pressure'. Human judgement decides." | `docs/docs/failure-mode-matrix.mdx:31` | | A4 |
| 160 | The anti-flap cooldown survives a restart only if at least one of the two durable paths worked; `CooldownViolated(restart+stateLost)` is a documented inherent DST finding class | `docs/docs/known-limitations.mdx:51-74`; `internal/dst/README.md:145-166` | | A4 |

### Backup, PITR, verification, restore, encryption, DR

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 161 | Backup source selection: replica first, primary fallback, with exact reason strings `"override"` / `"replica-preferred"` / `"primary-fallback"` | `// selectSourceSite picks the MySQL site to dump from. Replica first, primary fallback.` | `internal/controller/backup_reconciler.go:1107-1163` | A6 |
| 162 | `read-only` reader sites are excluded as backup sources, and an explicit override naming one is rejected | `return "", "", fmt.Errorf("sourceSiteOverride %q names a read-only site, which cannot be a backup source", override)` | `internal/controller/backup_reconciler.go:1123-1125, 1142-1145` | A6 |
| 163 | `maxLagSecondsForSource` default 300 | `api/v1alpha1/backup_types.go:53` | | A2 |
| 164 | The PITR archiver watches the **directory**, not the index file, and the reason is teachable | `watcher.Add(a.cfg.BinlogDir)` with `// We watch the directory, not the .index file itself: MySQL rewrites the index atomically (write to .index.tmp, rename), so a file-level watch would lose the watch after the first rotate.` | `internal/sidecar/binlog_archiver.go:163-166, 191-194` | A6 |
| 165 | inotify is an optimisation, not the only path — a poll ticker runs alongside it, plus a best-effort initial scan | `// runPollOnly is the fallback path when inotify is unavailable … Rotation is detected with worse latency but never missed.` | `internal/sidecar/binlog_archiver.go:152-153, 172-173, 209-222` | A6 |
| 166 | `archivePollInterval` default 60 s | `api/v1alpha1/backup_types.go:148` | | A2 |
| 167 | Only the primary archives, gated on `@@read_only` | `// Role gate: only the primary archives.` → `ro, err := a.mysql.IsReadOnly(ctx)` … `if ro { … return }` | `internal/sidecar/binlog_archiver.go:231-250` | A6 |
| 168 | Only **sealed** binlogs upload — the active one is dropped | `// Drop the last entry: it is the active binlog (tail of the index file is what MySQL is currently writing to).` → `sealed := entries[:len(entries)-1]` | `internal/sidecar/binlog_archiver.go:257-270` | A6 |
| 169 | `maxBinlogSize` default `"100M"`, applied only when PITR is enabled and written **before** user overrides so `spec.mysqlConf` can still win | `api/v1alpha1/backup_types.go:142`; `internal/controller/pitr.go:18`; `internal/controller/reconciler.go:730-736` | | A2 |
| 170 | `/pitr-cutoff` endpoint with three query params | `url := fmt.Sprintf("http://%s/pitr-cutoff?namespace=%s&group=%s&profile=%s", …)` | `internal/sidecar/binlog_archiver.go:429-433`; `cmd/bloodraven/main.go:430, 517` | A6 |
| 171 | Pruning is rate-limited (default 1 h) and fails silently by design; without operator wiring there is **no** pruning at all | `if a.retentionCfg == nil { return }`; `// Errors are logged but not surfaced via setError so a transient 503 from the operator doesn't leak into the archiver status.` | `internal/sidecar/binlog_archiver.go:350-364, 384-402` | A6 |
| 172 | What a verification proves | "A verification restores a MysqlBackup artifact into an ephemeral, throwaway MySQL instance to prove the backup can actually be loaded. Unverified backups are schrödinger backups" | `api/v1alpha1/mysqlbackupverification_types.go:19-32` | A6 |
| 173 | Mechanism: ephemeral PVC + in-pod mysqld bound to 127.0.0.1, no Service; PVC floor 10 GiB | `verificationMinPVCSize = int64(10 * 1024 * 1024 * 1024) // 10 GiB` | `api/v1alpha1/mysqlbackupverification_types.go:26-32`; `internal/controller/backup_verification_job.go:46-50, 64-68` | A6 |
| 174 | Sanity check runs via `mysql -B -N -e` wrapped in `timeout`, expects one row/one column, treats an empty result as scalar 0, and fails on `SanityCheckFailed` / `SanityCheckTimeout` | `api/v1alpha1/mysqlbackupverification_types.go:115-121, 146-152` | | A6 |
| 175 | What verification does **not** prove: logical equivalence with the live primary, application-level rehearsal of writes or traffic cutover | `docs/docs/backup-verification.mdx:52-58` | | A6 |
| 176 | Verification shipped with a real bug proving the point: scenario 31 failed with `ERROR 1062 Duplicate entry` because the verify mysqld ran `gtid_mode=OFF`, defeating GTID dedup on replay | issue [#101](https://github.com/ShipStream/bloodraven/issues/101) → PR [#105](https://github.com/ShipStream/bloodraven/pull/105) | | A4 |
| 177 | There is **no restore CR**. Restore is two fields on the group: `spec.initFromBackup` (one-shot, gates bootstrap) and `spec.restoreInPlace` (re-runnable, no teardown/rename cycle) | `api/v1alpha1/types.go:158-163`; `api/v1alpha1/backup_types.go:694-762` | A6 |
| 178 | In-place restore phases and the anti-fat-finger token | `+kubebuilder:validation:Enum="";Preflight;Fencing;Restoring;Resuming;Succeeded;Failed`; `// Confirm is a required anti-fat-finger token … Must be an RFC 3339 timestamp … strictly greater than the timestamp recorded in status.restoreInPlace.confirmTokenUsed` | `api/v1alpha1/backup_types.go:723-732, 775-782` | A6 |
| 179 | `pointInTime` is rejected when `spec.backup.pitr.enabled=false`, for **both** restore entry points, as a reconciler error rather than admission | `return out, fmt.Errorf("pointInTime is set but spec.backup.pitr.enabled=false; PITR restore requires the failover group to have continuous binlog archival configured on the source")` | `internal/controller/pitr.go:325-343` | A6 |
| 180 | Keyring phases — **five**, including `Failed` | `+kubebuilder:validation:Enum="";Pending;Unsealed;Escrowed;Sealed;Failed` | `api/v1alpha1/encryption_types.go:169-195` | A6 |
| 181 | Rotation re-enters `Unsealed` from `Sealed` | `if site.Phase == v1alpha1.KeyringPhaseSealed && rotateTarget == site.Name` | `internal/controller/encryption_reconcile.go:197-204` | A6 |
| 182 | The escrow lives in per-site **versioned Kubernetes Secrets**, owner-ref'd, with the digest recomputed rather than trusted | `labelKeyringVersion = "shipstream.io/keyring-version"`; `annotationKeyringDigest = "shipstream.io/keyring-digest"` | `internal/controller/encryption_escrow.go:24-32, 74-93, 191` | A6 |
| 183 | etcd therefore joins your key custody | "The live keyring is projected from a Kubernetes Secret. **Kubernetes stores Secrets unencrypted in etcd by default.** Without API-server encryption at rest, enabling this feature does not protect your keys — it just moves them from the MySQL data disk to the control-plane disk." … "None of these are optional. Bloodraven cannot verify them for you." | `docs/docs/encryption-at-rest.mdx:62-85` | A6 |
| 184 | `encryptionAtRest` requires `spec.tls` as a **hard CEL rejection** | `- message: 'spec.encryptionAtRest.enabled requires spec.tls: MySQL requires a secure connection to clone encrypted data'` | `charts/bloodraven/crds/shipstream.io_mysqlfailovergroups.yaml:6497-6500` | A6 |
| 185 | Rotation is refused on the active primary | `// The operator refuses to rotate the active primary. Rotation necessarily runs with a writable keyring, and the only window in which a keyring can be lost is that one` | `internal/controller/encryption_escrow.go:34-58` | A6 |
| 186 | `MysqlStandbyCluster` is **observability only** today | `// … publishes two conditions — BucketReadable and SourceConfigKnown … **No MySQL contact, no restore jobs, no activation in Phase 1.**` | `api/v1alpha1/mysqlstandbycluster_types.go:16-22` | A6 |
| 187 | The DR source-fencing checklist requires at least two of three independent signals, and "Bloodraven v1 does not automatically detect or resolve cross-cluster split-brain" | `docs/docs/multi-cluster-dr.mdx:200-237` | | A6 |
| 188 | `kubectl bloodraven` subcommands: `status`, `promote`, `reclone`, `backup`, `verify-backup`, `version`, `help` | `switch args[0] { case "help", …: case "version": case "status": case "promote": case "reclone": case "backup": case "verify-backup": …}` | `cmd/kubectl-bloodraven/main.go:32-39, 72-90` | A6 |
| 189 | The plugin only writes resources the operator already reads | `// The plugin only writes the resources the operator already reads … It never talks to MySQL directly` | `cmd/kubectl-bloodraven/main.go:6-9` | A6 |

### Environment and versions

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 190 | Release under study: **v0.9.1**; chart `version: 0.9.1`, `appVersion: "0.9.1"`, `kubeVersion: ">=1.26.0"` | `charts/bloodraven/Chart.yaml:5-6, 25`; `git tag` | | A2 |
| 191 | MySQL image default `mysql:9.7`; sidecar image `ghcr.io/shipstream/bloodraven-sidecar:0.9.1` | `api/v1alpha1/types.go:46, 50` | | A2 |
| 192 | `spec.sites` MinItems 2 / MaxItems 16 | `api/v1alpha1/types.go:63-64` | | A2 |
| 193 | **49 registered chaos scenarios** (49 files, each with exactly one `runner.Register`). Profiles: smoke = 4, release = 17, full = 49 | `internal/playground/scenarios/*.go`; `internal/playground/runner/profile.go:34-38, 48-67, 97-98` | | A2 |
| 194 | The playground overrides shipped defaults: `failoverCooldown: 30s` (vs 5 m), `replication.maxLagSeconds: 30` (vs 300), `dns.ttl: 10` (vs 60). It matches defaults for `pollInterval`, `failureThreshold`, `recoveryThreshold`, `leaseTimeout`, `peerCheckInterval`, `maxSyncWait` | `playground/manifests/failovergroup.yaml:10-13, 18, 76-79, 92-94` | | A2 |
| 195 | The playground needs 3 worker nodes; the third is dedicated to the reader so storage-loss testing is deterministic | `docs/docs/playground.mdx:34` | | A2 |
| 196 | `chaos.sh kill-site` does `kubectl delete pod -l shipstream.io/site=<site> --grace-period=0 --force`, and deletes MySQL **and** Dragonfly pods at the site | `playground/chaos.sh:52-56` | | A2 |
| 197 | Pod force-delete **does** trigger failover — scenario 09b hard-waits up to 90 s for `activeSite` to flip and `lastFailover` to stamp before reaching its real assertion, and passes | `internal/playground/scenarios/s09b_anti_flap_cooldown.go:119-127, 148-172` | | A2 |
| 198 | Why the <5 s pod respawn does not save it: the Deployment recreates the **pod object** in ~5 s, but the debounce watches whether **mysqld answers `CheckReadOnly`**. A cold container start plus InnoDB recovery exceeds the 6 s window | `internal/controller/topology.go:1532-1545`; `internal/playground/scenarios/s09b_anti_flap_cooldown.go:38-43, 180-190` | | A2 |
| 199 | Scenario 01 uses scale-to-0 for **determinism**, not because delete fails: "pod-delete races the Deployment respawn" and can restore the original topology via split-brain recovery instead of completing the failover | `internal/playground/scenarios/s01_clean_primary_kill.go:18-21`; `playground/chaos-scenarios.md:116, 367` | | A2 |
| 200 | The genuine no-failover case is a **container restart in place**: scenario 16 issues SQL `SHUTDOWN`, pod/PVC/IP survive, and the scenario explicitly accepts either outcome "depending on how fast Kubernetes restarts the mysql container vs the operator's pollInterval × failureThreshold (~6s in playground config)" | `internal/playground/scenarios/s16_mysql_process_kill.go:18-42` | | A2 |
| 201 | Pod crash with PVC intact is RPO 0 and **no failover at all** — the operator sees the primary return `writable` and keeps it | `docs/docs/durability-and-rpo.mdx:131` | | A2/A4 |

### External behaviour and misconceptions from the wider world

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 202 | A pool can hand out a read-only connection after failover — open upstream issue titled "got a read-only connection from the connection pool after the db failover" | https://github.com/brettwooldridge/HikariCP/issues/1802 | | A4 |
| 203 | A pool validation query **passes** against a demoted primary: the node is alive, just read-only. Drivers added `rejectReadOnly` handling precisely because the connection looks healthy until a write returns ERROR 1290/1792 | https://github.com/awslabs/aws-mysql-jdbc/issues/214 | | A4 |
| 204 | A short DNS TTL is not enough: the JVM's default DNS cache can be infinite for the process lifetime; AWS documents forcing `networkaddress.cache.ttl` ≤ 60 s | https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/jvm-ttl-dns.html | | A4 |
| 205 | A proxy does not move existing sessions; with `fast_forward=1` connections keep talking to the old master and hit read-only errors | https://github.com/sysown/proxysql/issues/2590 | | A4 |
| 206 | `read_only=ON` is bypassed by anyone with `SUPER`; `super_read_only` is the actual barrier | https://www.percona.com/blog/mysql-101-using-super_read_only/ | | A4 |
| 207 | GitHub's October 2018 incident: a 43-second partition left East and West each holding writes the other never saw; recovery took over 24 hours | https://github.blog/2018-10-30-oct21-post-incident-analysis/ | | A4 |
| 208 | Quorum alone is not enough: "the loss of quorum can take an unbounded amount of time to detect and react to… The ultimate cure is to use **fencing** and lock the other side out" | https://clusterlabs.org/projects/pacemaker/doc/2.1/Pacemaker_Explained/html/fencing.html | | A4 |
| 209 | A *graceful* takeover can still split-brain: orchestrator #854, demoted master left holding 2–7 transactions the cluster never got, because the new master was made writable before the old was set read-only | https://github.com/openark/orchestrator/issues/854 | | A4 |
| 210 | Semi-sync does not give zero RPO: MySQL bug #99370, closed as a **documentation** bug — an unacknowledged transaction still executes on restart, and the failed master "should not be reused as the replication master, and should be discarded" | https://bugs.mysql.com/bug.php?id=99370 | | A4 |
| 211 | `Seconds_Behind_Source = 0` does not mean caught up: it compares last-executed against last-*downloaded* relay event, so it reads 0 when the IO thread has stalled or the replica is idle | https://www.percona.com/blog/possible-reasons-when-mysql-replication-lag-is-flapping-between-0-and-xxxxx/ | | A4 |
| 212 | Errant GTIDs break rejoin: the new primary cannot supply transactions it never saw. Detected with `GTID_SUBSET()` / `GTID_SUBTRACT()` | https://www.percona.com/blog/errant-transactions-major-hurdle-for-gtid-based-failover-in-mysql-5-6/ | | A4 |
| 213 | The tempting fix destroys your PITR chain: `gtid-errant-reset-master` "destroys the binary logs". The safe fix is injecting empty transactions | https://www.percona.com/blog/fixing-errant-gtid-with-orchestrator-the-easy-way-out/ | | A4 |
| 214 | GitLab's January 2017 outage: five backup mechanisms, none usable — `pg_dump` silently failing on a version mismatch, empty S3 uploads, misconfigured alert emails. Recovered from an incidental 6-hour-old staging snapshot | https://about.gitlab.com/blog/postmortem-of-database-outage-of-january-31/ | | A4 |
| 215 | PITR reaches only the last binlog event you actually retained and shipped; retention or a purge sets a hard floor | https://dev.mysql.com/doc/refman/8.0/en/point-in-time-recovery.html | | A4 |
| 216 | Kubernetes will not delete pods merely because a node is unreachable — the pod sits `Terminating`/`Unknown` indefinitely, to protect at-most-one identity | https://github.com/kubernetes/kubernetes/issues/74689 | | A4 |
| 217 | Force-deleting it breaks at-most-one: the API object goes away while the process may still be running and **writing** on the partitioned node | https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/ | | A4 |
| 218 | `Terminating` status is set by the API server, not the kubelet. On an unreachable node the container keeps running and keeps writing to the PV | https://discuss.kubernetes.io/t/stateful-set-pod-remain-stuck-in-terminating-state-in-case-node-went-to-notready-state/22768 | | A4 |
| 219 | ReadWriteOnce means one **node**, not one pod; storage attach is not fencing | https://github.com/kubernetes/kubernetes/issues/61832 · https://access.redhat.com/solutions/2214541 | | A4 |
| 220 | Single-observer polling is the documented false-positive trap: orchestrator's holistic detection requires the master be unreachable **and** its replicas independently confirm they cannot see it | https://github.com/openark/orchestrator/blob/master/docs/failure-detection.md | | A4 |
| 221 | Detection and recovery are separate: a detected `DeadMaster` can sit for minutes with no promotion, gated by auto-failover lists, admin blocks, anti-flapping, and failure type | https://github.com/openark/orchestrator/blob/master/docs/failure-detection.md | | A4 |
| 222 | `kubectl delete pod` as an HA test "might produce misleading results" — a graceful delete runs a clean shutdown; `--grace-period=0` removes the API object without proving the process died | https://cloudnative-pg.io/documentation/1.23/failure_modes/ | | A4 |
| 223 | Control plane and data plane fail separately in practice: Cloudflare's November 2023 incident kept the data plane serving for roughly two days of control-plane outage | https://blog.cloudflare.com/post-mortem-on-cloudflare-control-plane-and-analytics-outage/ | | A4 |

### Upstream current-state check (verified August 2026)

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 224 | MySQL 9.7 is the current **LTS**, not the latest MySQL. The Innovation line moved to calendar versioning and is at 26.7.0; latest 9.7 patch is 9.7.2 (2026-07-28) | "MySQL July 2026 GA Releases … MySQL 26.7.0 Innovation, MySQL 9.7.2 LTS, MySQL 8.4.11 LTS" | https://blogs.oracle.com/mysql/mysql-july-2026-ga-releases-now-available ; https://dev.mysql.com/doc/relnotes/mysql/9.7/en/ | A3 |
| 225 | `read_only` permits updates from `CONNECTION_ADMIN` / deprecated `SUPER` | "the server permits no client updates except from users who have the `CONNECTION_ADMIN` privilege (or the deprecated `SUPER` privilege). This variable is disabled by default." | https://dev.mysql.com/doc/refman/9.7/en/server-system-variables.html#sysvar_read_only | A3 |
| 226 | `super_read_only` is the actual barrier — it prohibits updates **even from** `CONNECTION_ADMIN` or `SUPER` | "If the `super_read_only` system variable is also enabled, the server prohibits client updates even from users who have `CONNECTION_ADMIN` or `SUPER`." | same page, `#sysvar_super_read_only` | A3 |
| 227 | Setting `super_read_only=ON` implicitly forces `read_only=ON`; setting `read_only=OFF` implicitly forces `super_read_only=OFF` | "Setting `super_read_only` to `ON` implicitly forces `read_only` to `ON`." … "Setting `read_only` to `OFF` implicitly forces `super_read_only` to `OFF`." | same page | A3 |
| 228 | **Fencing blocks; it does not cut off writers.** The SET waits out in-flight work and can fail outright | "The attempt fails and an error occurs if you have any explicit locks (acquired with `LOCK TABLES`) or have a pending transaction." … "The attempt blocks while other clients have any ongoing statement, active `LOCK TABLES WRITE`, or ongoing commit, until the locks are released and the statements and transactions end." | same page, `#sysvar_read_only` | A3 |
| 229 | Replication threads keep writing under `super_read_only` | "Updates performed by replication threads, if the server is a replica." (listed under operations still permitted) | same page | A3 |
| 230 | The legacy statements were **removed in MySQL 8.4.0**, not merely deprecated | "The following SQL statements have been removed (replacements in brackets): `START SLAVE` (`START REPLICA`); `STOP SLAVE` (`STOP REPLICA`); `SHOW SLAVE STATUS` (`SHOW REPLICA STATUS`); … `CHANGE MASTER TO` (`CHANGE REPLICATION SOURCE TO`) …" | https://dev.mysql.com/doc/relnotes/mysql/8.4/en/news-8-4-0.html | A3 |
| 231 | Asymmetry worth teaching: `log_slave_updates` survives as a **deprecated alias** for `log_replica_updates`, while the statements were removed outright | "`log_slave_updates` … Deprecated: Yes … Deprecated alias for `log_replica_updates`." | https://dev.mysql.com/doc/refman/9.7/en/replication-options-binary-log.html | A3 |
| 232 | Both threads must be stopped before `CHANGE REPLICATION SOURCE TO … SOURCE_AUTO_POSITION = 1` | "Both the receiver thread and the applier thread must be stopped before issuing a `CHANGE REPLICATION SOURCE TO` statement that employs `SOURCE_AUTO_POSITION = 1` …" | https://dev.mysql.com/doc/refman/9.7/en/change-replication-source-to.html | A3 |
| 233 | `RESET REPLICA ALL` removes all connection parameters; a later `CHANGE REPLICATION SOURCE TO` is required to use the instance as a replica again | "If you want to remove all of the replication connection parameters, use `RESET REPLICA ALL`. … When you have used `RESET REPLICA ALL`, if you want to use the instance as a replica again, you need to issue a `CHANGE REPLICATION SOURCE TO` statement" | https://dev.mysql.com/doc/refman/9.7/en/reset-replica.html | A3 |
| 234 | `CLONE INSTANCE` privileges: donor needs `BACKUP_ADMIN`, recipient needs `CLONE_ADMIN`, which implies `BACKUP_ADMIN` and `SHUTDOWN` | "The `CLONE_ADMIN` privilege includes `BACKUP_ADMIN` and `SHUTDOWN` privileges implicitly." | https://dev.mysql.com/doc/refman/9.7/en/clone-plugin-remote.html | A3 |
| 235 | Error 3707 is documented and does **not** indicate a clone failure | "ERROR 3707 (HY000): Restart server failed (mysqld is not managed by supervisor process)." … "This error does not indicate a cloning failure. It means that the recipient MySQL server instance must be started again manually after the data is cloned." | same page | A3 |
| 236 | A secure connection is genuinely required to clone encrypted data | "A secure connection is required when cloning encrypted data regardless of whether this clause is specified." | same page | A3 |
| 237 | GTID set grammar in MySQL 9.x includes **tags**, and a tag is part of the identity | `uuid_set: uuid:tag:interval:[tag:interval]...` ; "When constructing a GTID set, a user-defined tag is treated as part of the UUID." Example: `3E11FA47-…-C80AA9429562:Domain_1:1-3:15-21:Domain_2:8-52` | https://dev.mysql.com/doc/refman/9.7/en/replication-gtids-concepts.html | A3 |
| 238 | `GTID_SUBSET()` semantics | "Given two sets of global transaction identifiers `set1` and `set2`, returns true if all GTIDs in `set1` are also in `set2`." | https://dev.mysql.com/doc/refman/9.7/en/gtid-functions.html | A3 |
| 239 | `GTID_SUBTRACT()` semantics — the divergence primitive | "returns only those GTIDs from `set1` that are not in `set2`" | same page | A3 |
| 240 | `sync_binlog=1` guarantee, verbatim | "This is the safest setting… In the event of a power failure or operating system crash, transactions that are missing from the binary log are only in a prepared state. This permits the automatic recovery routine to roll back the transactions, which guarantees that no transaction is lost from the binary log." Default Value: `1` | https://dev.mysql.com/doc/refman/9.7/en/replication-options-binary-log.html#sysvar_sync_binlog | A3 |
| 241 | `innodb_flush_log_at_trx_commit=2` — the manual attributes loss to **any unexpected mysqld process exit**, not only power loss, and recommends `=1` | "With a setting of 2, logs are written after each transaction commit and flushed to disk once per second. Transactions for which logs have not been flushed can be lost in a crash." … "any unexpected **mysqld** process exit can erase up to N seconds of transactions." … "If binary logging is enabled, set `sync_binlog=1`. Always set `innodb_flush_log_at_trx_commit=1`." | https://dev.mysql.com/doc/refman/9.7/en/innodb-parameters.html#sysvar_innodb_flush_log_at_trx_commit | A3 |
| 242 | Current MySQL keyring is `component_keyring_file`; the `keyring_file` **plugin was removed in 8.4.0** | 9.7 manual lists only components: "`component_keyring_file`: Stores keyring data in a file local to the server host." 8.4.0 notes: "`keyring_file`: Use `component_keyring_file` instead… The `keyring_file_data` system variable has also been removed." | https://dev.mysql.com/doc/refman/9.7/en/keyring.html ; https://dev.mysql.com/doc/relnotes/mysql/8.4/en/news-8-4-0.html | A3 |
| 243 | Oracle's own caveat on file-based keyrings | "the `component_keyring_file` and `component_keyring_encrypted_file` components are not intended as a regulatory compliance solution. Security standards such as PCI, F[IPS]…" | https://dev.mysql.com/doc/refman/9.7/en/keyring.html | A3 |
| 244 | external-dns `DNSEndpoint` is still `externaldns.k8s.io/v1alpha1`; an **approved** proposal targets `v1beta1` with no date | "apiVersion: externaldns.k8s.io/v1alpha1"; proposal "status: approved" showing `externaldns.k8s.io/v1beta1` | https://kubernetes-sigs.github.io/external-dns/latest/docs/sources/crd/ ; https://kubernetes-sigs.github.io/external-dns/latest/docs/proposal/003-dnsendpoint-graduation-to-beta/ | A3 |
| 245 | `NoExecute` semantics | "pods that do not tolerate the taint are evicted immediately"; "pods that tolerate the taint with a specified `tolerationSeconds` remain bound for the specified amount of time. After that time elapses, the node lifecycle controller evicts the Pods" | https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/ | A3 |
| 246 | PDB protects only against **voluntary** evictions | "The budget can only protect against voluntary evictions, not all causes of unavailability." | https://kubernetes.io/docs/tasks/run-application/configure-pdb/ | A3 |
| 247 | `REPLTAKEOVER` exists, is an **ADMIN-port, GLOBAL_TRANS** command taking a timeout in seconds, and was introduced in Dragonfly **v1.5.0** (2023-07-03). It has no official docs page | `CI{"REPLTAKEOVER", CO::ADMIN \| CO::GLOBAL_TRANS, -2, 0, 0, acl::kReplTakeOver}`; `// REPLTAKEOVER <seconds> [SAVE]`; v1.5.0 notes "feat: Support atomic replica takeover … pull/1314" | `dragonflydb/dragonfly` `src/server/server_family.cc:4104, 3493`; GitHub releases API | A3 |
| 248 | The Dragonfly pin has **drifted two minors**: repo pins `v1.38.0` (2026-04-14), current stable is `v1.40.1` (2026-08-06) | `docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0` vs upstream `v1.40.1 2026-08-06T06:54:05Z` | `playground/manifests/failovergroup.yaml:87`; GitHub releases API | A3 |
| 249 | `sigs.k8s.io/controller-runtime v0.24.1` is the current latest; `k8s.io/*` at v0.36.2 is current minor, one patch behind. No material deprecation affects this operator | `go.mod:14-18`; GitHub releases API | A3 |
| 250 | Bloodraven **v0.9.1**, published 2026-08-11. The repo is public and not archived | `"tag_name": "v0.9.1"`, `"published_at": "2026-08-11T21:45:05Z"`; `"private": false`, `"visibility": "public"`, `"archived": false` | https://api.github.com/repos/ShipStream/bloodraven | A3 |
| 251 | **Bloodraven has no licence.** `"license": null` from the GitHub API and no LICENSE file in the repo root. It is public source, all rights reserved — not open-source | repo root contents listing contains no LICENSE entry | https://api.github.com/repos/ShipStream/bloodraven ; https://api.github.com/repos/ShipStream/bloodraven/contents/ | A3 |

### The playground counter application

| # | Claim | Value / verbatim quote | Source | Angle |
|---|---|---|---|---|
| 252 | The counter application's pool is deliberately small and short-lived, which is what makes its failover behaviour observable in a lab | `SetMaxOpenConns(5)`, `SetConnMaxLifetime(30 * time.Second)` | `playground/counter-app/main.go:89-90` | A1 |
| 253 | The counter application reports which site served a request and whether that site is writable, which is how a stale read is detected at all | probes `@@global.read_only` and `@@hostname` | `playground/counter-app/main.go:179-186` | A1 |
| 255 | Playground site load-balancer IPs, used as the DNSEndpoint A-record targets: `iad` is `10.96.100.10`, `pdx` is `10.96.100.20` | `- lbIP: 10.96.100.10` (iad) and `- lbIP: 10.96.100.20` (pdx) in the captured group spec | `playground/chaos-results/20260429T142107Z/01-clean-primary-kill/cluster.yaml:93,110` | A2 |
| 256 | Playground DNS hostname and TTL | `"dns":{"hostname":"playground-db.example.local","ttl":10}` | same file, line 4 | A2 |
| 254 | The counter application writes to **both** MySQL and Dragonfly on every increment; the MySQL counter is durable and the Dragonfly counter is the session-continuity signal. After a planned failover with `sessionsPreserved=true` the Dragonfly counter survives; after an emergency failover it usually resets to 0 | `docs/docs/playground.mdx:291` | | A2 |

## Ungrounded

Claims considered and not used, with what was done about each.

- **"Pod-kill failover takes about 30–45 seconds"** (`docs/docs/playground.mdx:101`) — unsourced in the
  docs and contradicted by nine-plus recorded runs at 12.0 s. **Cut**; the course teaches the measured
  12.0 s clean / 36.0 s with-relay-backlog pair from rows 40–41 instead.
- **"~37 s total failover"** (`playground/chaos-scenarios.md:108`, `failure-mode-matrix.mdx:25`) —
  arithmetically defensible as 6 s detect + 30 s drain, but not what the suite measures. **Taught as a
  worst case only**, always beside the measured typical.
- **"30+ chaos scenarios"** (`docs/docs/playground.mdx:231`) — stale. **Corrected** to 49 (row 193).
- **`spec.splitBrainPolicy.preferSite`** (`docs/docs/failover.mdx:228-275`) — the field does not exist.
  **Cut entirely**; the course teaches `sitePriorities` (row 66) and flags the doc error in Unit 5.
- **`Manual` / `PreferSite` as policy modes** (`docs/docs/failure-mode-matrix.mdx:35`) — no such enum.
  **Cut.**
- **`spec.sites[].priority`** — no such field. **Cut**; ordering comes from the group-level
  `spec.splitBrainPolicy.sitePriorities` (row 66).
- **`spec.updateStrategy` behaviour** (`docs/docs/operations.mdx:366`, `failover.mdx:340`,
  `crd-reference.mdx:67`) — the field exists in the CRD but no Go code outside the type definition
  reads it; ordered update triggers on spec drift alone, so setting `Recreate` changes nothing.
  **Flagged in Unit 2** as a documented-but-inert field rather than taught as a control.
- **`bloodraven_dr_restorable_timestamp_seconds`** — does not exist in `internal/`; the docs correctly
  scope it to Phase 2 under WISHLIST #7. **Cut** from Unit 6.
- **Dragonfly "v1.38.0 minimum supported"** — nothing enforces or checks a version. **Taught as a
  support policy**, not a guardrail (row 133).
- **Issue #93 as a hands-on lab exercise** — reproduces on kind+Calico, masked on k3d/kube-router
  which flushes conntrack on policy change. **Converted to a forensics exercise** in Unit 2 using
  captured artefacts, rather than a live reproduction learners are told to attempt.
- **A stated wall-clock for "how long until my application recovers"** — depends on pool
  configuration, driver, and DNS caching, none of which Bloodraven controls. **Taught as a method**
  (measure your own write-gap in the Unit 4 project) rather than a number.
