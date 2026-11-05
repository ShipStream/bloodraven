# Deterministic Simulation Testing (DST)

This package runs the **real** failover control loop — `TopologyManager.Poll`,
`FailoverController.Execute`, old-primary recovery, source convergence, DNS
reconcile, primary re-assert — against an in-memory model of a multi-site
MySQL cluster, under seeded random fault schedules. It is the fast,
reproducible complement to the playground chaos suite: a trial takes
milliseconds instead of minutes, and every failure replays exactly from its
seed.

## Running

```sh
make test            # includes the quick sweep (TestDST_Quick, 400 seeds, shared with the 600-seed actor-reachability sweep)
make dst             # full saturation campaign; report → playground/chaos-results/dst-report.txt
DST_SEED=61 make dst-repro              # replay one trial with events + operator logs
DST_SEED=61 DST_SKIP=1,3 make dst-repro # replay with schedule ops 1 and 3 masked out
DST_SEED=61 go test ./internal/dst -run PlannedRepro -v  # replay one seed with the planned-failover actors on
```

`make test` also runs `TestDST_PlannedQuick`: 300 seeds with the
planned-failover actors enabled (see below; `DST_PLANNED_TRIALS` overrides
the count).

Campaign knobs (env): `DST_BATCH` (default 1000), `DST_DRY_BATCHES` (default
3), `DST_MAX_TRIALS` (500000), `DST_WALL_SECONDS` (1800), `DST_REPORT`.

The campaign stops at **coverage saturation**: a batch is "dry" when it
produces no new failure fingerprint and fewer than 1% of its trials yield a
new behavior signature; after `DST_DRY_BATCHES` consecutive dry batches, more
trials are diminishing returns. Failures are deduplicated by the set of
violated invariants (see the fingerprint table under Known finding classes)
and shrunk to a minimal fault schedule.

Every actor added to the model widens the signature space, so keep an eye on
the stop reason: `wall-clock cap` rather than `saturated` means the run was
cut off mid-exploration, and either `DST_WALL_SECONDS` needs raising or a
newly added signature dimension is too fine-grained to ever go dry. The
signature is deliberately coarse for exactly this reason — fault-injection
events are excluded entirely, and mid-`Execute` deaths collapse to one
dimension rather than one per statement.

## How a trial works

1. `GenerateTrial(seed)` derives everything from the seed: 2 or 3 sites and
   their roles, split-brain priorities, anti-flap cooldown, whether failover
   history is rehydrated, the sidecar deployment shape (lease timeout,
   topology-aware fencing on/off, whether its tick lands before or after the
   operator's poll), and an explicit fault-op schedule (crashes with
    fenced/writable boot modes, operator↔site partitions, symmetric
    site↔site partitions (`partitionPair`, both directed cuts) and one-way
    peer cuts (`partitionOneWay`),
   operator restarts, mid-`Execute` operator deaths, ambiguous mutations —
   applied but the operator saw an error — failing mutations, replication
   fetch/apply stalls, relay-drain stalls, rogue fences, rogue `read_only=0`,
   CR-status write outages, out-of-band anti-flap store outages, DNS
   outages). Fault kinds are drawn from the `faultWeights` table, whose
   weights must sum to 100 (checked at `init`). Execution consumes no
   randomness after generation.
2. The harness builds a production `TopologyManager` via
   `NewTopologyManagerWithClock` with a `FakeClock`, the simulated
   `mysql.Checker`s, DNS updater/reader, tainter, and out-of-band anti-flap
   store. `StatusCallback` snapshots act as the simulated CR status
   subresource and `simFailoverStore` as the annotation store; an
   operator-restart fault rebuilds the manager and rehydrates from whichever
   durable copy is newer, exactly the way `runner.go` does.
3. Every site also runs a **real `sidecar.FencingMonitor`** (`sidecar.go`)
   on the same fake clock — the real `Check` → `evaluate` → `doFence` path,
   the real `TopologyCache` including peer relay, and the operator's
   `/healthz` + `/active-site` and the peers' `/peer/ping` +
   `/peer/active-site` served in-process by an `http.RoundTripper`. It is
   driven synchronously once per poll rather than by `Run`, so ordering
   stays exact.
4. Each poll: apply due fault ops → restart the operator if a mid-`Execute`
   death's down window has expired → advance the data plane one tick (app
   writes land on every writable site; replication fetch/apply moves GTID
   ranges subject to links and stalls) → tick every live sidecar and run
   `tm.Poll(ctx)`, in the trial's chosen order → run per-poll invariants →
   advance the clock one poll interval. All faults heal at `HealAt`; the
   settle window outlasts the cooldown, the 30s recovery stabilization
   delay, and debounce thresholds, so end-state invariants judge a genuinely
   quiesced cluster.

## Planned-failover actors (#235)

`GeneratePlannedTrial(seed)` is `GenerateTrial(seed)` plus `Trial.Actors`,
drawn from a PRNG stream of its own (`rand.NewPCG(seed, 0x9f1a2b3c)`), so the
fault schedule of every seed is byte-identical with or without the actors
(`TestGenerateTrial_ScheduleUnchangedByActors` pins a hash of seeds
1..2000). Only the settle window is longer (at least 150 polls), so a request
made just before `HealAt` can wait out its admission deadline. The zero
`Actors` (what `GenerateTrial` returns) disables the whole machinery, and
those trials run exactly as before. With the actors on, a trial runs the
production planned-failover path end to end:

- **The engine and the claim.** The production `PlannedFailoverEngine`
  (deterministic run IDs through `SetRunIDSourceForSim`) over the production
  facade (`NewPlannedFailoverTopology`) on `simRegistry`, the real
  `PlannedClaimSlot`, and `SimHooks.Yield` wired to the scheduler on both.
  The engine passes when its requeue is due or when the object's
  resourceVersion moved since its last pass (a watch event). A restart
  builds a new engine and facade: the memos die with the process.
- **The object and the store.** `simPlannedStore` is the simulated API
  server for the group: one `MysqlFailoverGroup` (spec sites and roles,
  cooldown, a 20s lag wait; annotations and status) with an integer
  `resourceVersion`, implementing `controller.PlannedFailoverStore`
  (conditioned writes, settlement through the status-fence annotation, the
  terminal marker, the durable deadline annotations, the hold). The
  topology manager's snapshot writes merge into it through the production
  `MergeTopologySnapshotStatus` (a replaced manager's writes are dropped, as
  the runner drops them), and the production out-of-band recorder
  (`NewAnnotationFailoverStateRecorder`) writes the failover record through
  `storeClient`, a client serving only Get and a metadata merge Patch
  conditioned on the resourceVersion. Every writer contends on one
  `resourceVersion`; a no-op write does not move it.
- **Restore.** A new manager is restored by the production
  `restoreFailoverState` / `restorePlannedFailoverState` logic
  (`RestoreFailoverStateForSim`, `RestorePlannedFailoverStateForSim`) from an
  uncached read of the object, and wired with `WireRegisteredManager`.
- **The registry and manager replacement.** `simRegistry`
  (`controller.ManagerRegistry`) is one operator process: the current
  manager, retiring managers and one slot. `replaceManager` retires the
  current manager (`RetireForSim`); it keeps polling for 0–3 polls (a Poll
  in flight), its successor sees it through `ManagerWiring.RetiringBusy`
  (`ChangingSitesForSim`) and `RetiringUpdateInFlight`, and the slot is
  shared. `managerAbsent` unregisters the manager for N polls. A restart
  builds a new registry (the slot is dropped).
- **Ordered updates.** `update_actor.go` is a step model of
  `ExecuteTargets`' handoff over the real claim API (`BeginOrderedUpdateForSim`,
  `ClaimOrderedUpdateForSim`, `RecordHandoffForSim`, `FinishOrderedUpdateForSim`):
  fence the active, wait for the standby, then STOP, RESET and
  read_only=OFF on one pinned session, recording the handoff in the same
  step as the callback does. It keeps stepping on a retired manager; when it
  ends, the successor's predecessor guard is cleared.
- **Status-write loss.** `pfWriteLost`, `pfWriteAmbiguous`, `pfWriteLate`,
  `pfReadOutage` and `pfStatusDenied` (with restarts inside the outage)
  script the store; the fault stream's status and out-of-band outages apply
  to the same object.
- **Pinned sessions.** `simChecker.PinSession` returns a `simSession` pinned
  to the site's server incarnation; a crash, restart or pod replacement
  moves the incarnation and every later call on the session fails. Pooled
  calls reach whichever server runs now.
- **Actor attribution.** Every statement carries its actor (`WithActor`:
  `tm`, `pf`, `upd`, `sidecar`; untagged is `tm`), recorded on the event.
- **Slow statements and the scheduler.** A statement the trial marks slow
  yields between issue and apply and between apply and acknowledgement, with
  `Cluster.mu` released. At each yield the scheduler steps a PRNG-chosen
  subset of the other actors (the managers' Poll, the engine, the update
  actor, one sidecar, rogue writability inside the fault window), at most
  three deep, never re-entering an actor on the stack, and panics if a
  tracked mutex (`Cluster.mu`, the store's and registry's, and every
  manager's `tm.mu`, `failoverStateWriteMu`, `restoreProtectionMu`,
  `restoreFenceMu`) is held. A manager polls once per tick: on a scheduler
  draw the engine pass runs first and a yield inside it may pull the tick's
  Poll forward, but never adds a second one (that would advance debounce
  with no time passing). Only the planned and update actors' statements can
  be slow: the topology manager probes from per-site goroutines, so there
  are no TM-internal yields (F2).
- **Ops.** `plannedFailover`, `orderedUpdate`, `replaceManager`,
  `managerAbsent`, the five store faults, `oobLateWrite` (a late duplicate
  of an older record through the production recorder, which must refuse
  it), `restartPromotedTarget` and `replacePod` (optionally onto a regressed
  data directory under a fresh `server_uuid`; never the writable site or the
  last copy of a transaction).
- **Invariants.** `plannedMonitor` checks the §15.1 planned invariants
  (`PlannedFenceWithoutAuthority`, `PlannedPromoteBesideWritable`,
  `SucceededBesideWritable`, `UnfenceBesideWritable`, `UnfenceUnattributed`,
  `ForeignMutationUnderClaim`, `ClaimRevoked`, `ZombieClaim`,
  `OutcomeDishonest`, `RepeatedPromotion`, `WatermarkRegressed`,
  `AutomationHeldByPendingRequest`, `HoldNotHonoured`, `MarkedRunResumed`,
  `UnconfirmedBlocksEmergency`, `HoldWithoutDataAtRisk`,
  `FencedSourceNeverHealed`, `PlannedRunStuck`) on the events, the landed
  mutations, the store's commits, the yields and the slot
  (`HeldForSim`/`ReleasesForSim`). All of them count rogue writability, with
  the documented scope:
  - a rogue `read_only=0`, or a pod booting writable, landing after the
    facade's last peer probe is the §18 R2 residual (`RogueRaceResidual`),
    not a violation;
  - outcomes are judged with what the run could know: `Succeeded` at its
    confirm probe (the claim is released at Publishing), `SourceLeftFenced`
    with an unreachable source by its reason (§6.2), and a cross-process
    re-run's `PromotedUnconfirmed` / `ManualRecoveryRequired` without the
    promotion memo (§7.1);
  - `FencedSourceNeverHealed` judges only the run's own fence (landed, in
    the source's current incarnation, still set, never healed) under the
    re-assert's preconditions;
  - a standing automation hold is the documented manual-recovery state: the
    end-state invariants and the dual-writable check do not judge it.

  The planned harness also adjusts three pre-existing invariants for the
  actors: promotions by the planned run and the update actor are detected
  across polls and armed for anti-flap bookkeeping when recorded; an ordered
  update is not held by the cooldown (it is a maintenance handoff); and a
  window with no registered manager is no operator, like a dead one.

## Invariants

Per poll:
- **PromoteWhileObservedWritable** — never grant writability while another
  site is observed writable, unless it was fenced in the same cycle.
- **FenceAfterPromote** — fencing of other sites precedes the writable grant.
- **DualWritableUnresolved** — a dual-writable state that is fully
  operator-observable, with a resolution policy available (failover history
  or split-brain priorities), must be fenced within a grace window.
- **RepointDivergent** (model-level) — `CHANGE REPLICATION SOURCE` must never
  point a site at a source missing that site's transactions.

At end of trial (post-heal, post-settle):
- **EndSplitBrain / WedgedNoPrimary / NonConvergence** — the cluster
  converges to exactly one writable primary with healthy, caught-up replicas,
  unless the end state is an explicitly reported blocked condition
  (`RecoveryBlocked` divergence, source-convergence `Blocked`) or a
  documented needs-human state (no history, no writable, no priorities).
- **SilentDataLoss** — every application-acked transaction is on the final
  primary, or covered by a reported divergence, or held only by sites that
  never came back (inherent async-DR RPO). Anything else is data loss the
  operator failed to surface.
- **CooldownViolated** — successive promotions respect the anti-flap
  cooldown.
- **StatusClaimMismatch / DNSStale** — the CR's `activeSite` claim and the
  DNS record match ground truth once settled.

At every operator restart:
- **AntiFlapStateLost** — a restarted process must rehydrate an anti-flap
  record at least as new as the last promotion the harness observed, unless
  both durable paths were unavailable to preserve it: the out-of-band store
  had an unhealed rejection, and the status path never landed a
  post-promotion copy either. The operator writes the out-of-band record at
  promotion time and retries it every poll, so a healthy store means a
  current record; and a status write the promoting process landed without
  the record is the operator dropping it, not the path failing.

## Model fidelity notes

Faithful where correctness depends on it: `super_read_only=ON` forces
`read_only=ON` and `read_only=OFF` clears `super_read_only`; `RESET REPLICA
ALL` / `CHANGE REPLICATION SOURCE` require stopped threads (error 3081) and
purge relay logs; a replica whose GTID set exceeds its source breaks the IO
thread like error 1236; a receiver whose source is unreachable reports a
connect error in `LastIOError`; relay drain aborts only on an applier
(`LastSQLError`) error and applies local backlog even when the source is
unreachable or the receiver has errored; crashed sites keep durable state and can boot fenced
or writable (both happen in production).

Operator death is modeled as "the process stops interacting", not as a
panic: the fatal statement lands (or does not, per the op's `PreApply`), and
from then until the harness builds a replacement every operator-side call —
reads, mutations, DNS, status, the anti-flap store — fails or is dropped, so
the remainder of the dying `Poll` cannot change the model. Unwinding the
real stack would be no more faithful (the manager is discarded either way)
and would risk unwinding through one of `Poll`'s per-site goroutines. The
sidecars are unaffected: they are separate processes, and a dead operator is
exactly what their lease rule exists for.

Not modeled (excluded from trials, guarded by loud errors): CLONE /
bootstrap (the bootstrap controller is real and production-shaped, but the
model never produces an empty site so no clone goroutine can start),
Dragonfly (the engine skips its Dragonfly phases with no Dragonfly ops; the
Dragonfly rows of the #235 table are controller unit tests), deployment
leases, and wall-clock `context.WithTimeout` expiry (timeouts are modeled as
injected call errors instead; the engine's pass bounds are wall time and
never fire in a trial). Planned failovers, ordered updates, manager
replacement and status-write loss are modeled in trials with the actors on.
The update actor is a step model of `ExecuteTargets`' handoff over the real
claim API: it exercises the claim protocol and predecessor tracking, not
`ExecuteHandoff`'s internals, which the component tests cover with the
production updater. There are no yields inside the topology manager, so
interleavings inside one `Poll` are component-test territory (rows 1–3 and
9 of the #235 interleaving table). The sidecars' view of the operator
(`/active-site`) follows the trial's current manager even while
`managerAbsent` has unregistered it, and an update actor keeps stepping on a
manager inside a `managerAbsent` window.

The simulator only contains failure modes we teach it. When the playground or
production surfaces a new MySQL behavior, encode it here too — and keep the
playground E2E suite as the fidelity check for this model.

## Known finding classes (expected in campaign output)

- **`CooldownViolated(restart+stateLost)`** — a restart between the two
  promotions that came up without the durable anti-flap record. That
  requires **both** durable paths (CR status and the out-of-band annotation
  store) to have been rejecting writes at once; nothing could then have
  carried the cooldown across the restart. With the actors on,
  `pfStatusDenied` deliberately puts restarts inside a status outage, so a
  trial whose fault stream also denies the out-of-band store lands here;
  `TestDST_PlannedQuick` allows exactly this fingerprint and nothing else. See
  `site/content/docs/3.get-started/1.known-limitations.md` → Operator availability.

- **`…(replacement)`: `CooldownViolated(replacement)`,
  `AntiFlapStateLost(replacement)`, `WedgedNoPrimary(replacement)`** (actors
  on; open finding DST-1). An ordered update's handoff recorded on a manager
  that is then replaced, or that finishes on a retired manager, never
  reaches its replacement: the successor promotes inside the cooldown or
  judges a wedge from the older record, and with the out-of-band store down
  a restart loses it.
- **`DualWritableUnresolved(confirmedClaim)`** (actors on; open finding
  DST-4). A planned run's claim is held until its Publishing record commits
  (§2.3); an external writer that makes the old source writable after the
  confirm, while status writes stall, cannot be fenced by the manager until
  then.

`TestDST_PlannedQuick` allows exactly these classes and
`CooldownViolated(restart+stateLost)`; anything else fails it. When either
finding is fixed, remove its class from `plannedAllowedClasses`
(`planned_test.go`) and pin the shrunk trial in
`test/component/dst_regression_test.go`.

`CooldownViolated` splits three ways so the inherent class cannot absorb a
regression, and only the third is expected:

| Fingerprint | Meaning | Verdict |
|---|---|---|
| `CooldownViolated` | no restart between the promotions | regression |
| `CooldownViolated(restart)` | a restart, but the record survived it | regression |
| `CooldownViolated(restart+stateLost)` | a restart with every durable path denied | inherent |

`CooldownViolated(restart)` was the expected class before the out-of-band
store existed. It is a regression now: the restarted process had the record
and promoted anyway.

## Model coverage gaps (documented, not yet closed)

- The cluster is always seeded with baseline data, so **empty/fresh sites
  never occur in trials** — the operator's empty-site guards and clone paths
  are covered by component tests only (CLONE is unmodeled).
- Fault injection applies to mutations; **reads never fail independently**,
  so "mutation applied, subsequent read fails" interleavings inside a single
  operator action are unexplored. The sidecar shares the operator's
  ambiguous/failing-mutation counters, so its own fence write can be
  ambiguous, but its reads cannot fail independently either.
- Sidecars tick exactly once per operator poll, at a per-trial phase. Real
  monitors run on their own interval, so sub-poll interleavings (two sidecar
  ticks inside one operator action) are unexplored.
- **Keyring rotation is unmodeled.** Encryption phase, `UnsealReason`, and
  the rotate-keyring annotation do not exist in the simulator. Adding a
  random `OpKind` would change `faultWeights` and shift every seed, and
  would still only test a synthetic gate. The coupling "do not promote a
  site with `UnsealReason=Rotation`" is pinned by component tests in
  `test/component/keyring_promotion_gate_test.go`. `PromotionBlocked`
  defaults false, so existing DST seeds are unaffected.
