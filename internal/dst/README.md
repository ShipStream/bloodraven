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
DST_SEED=61 DST_SKIP=2 DST_ACTOR_SKIP=0,4 go test ./internal/dst -run PlannedRepro -v  # mask fault ops and actor ops
DST_SEED=61 DST_LOG=1 go test ./internal/dst -run PlannedRepro -v  # with topology-manager and engine logs
DST_PLANNED=1 make dst                  # saturation campaign over actors-on trials, shrunk over both schedules
DST_PLANNED_START=16001 DST_PLANNED_TRIALS=16000 go test ./internal/dst -run 'TestDST_PlannedQuick$' -v  # a wider planned sweep, one range per process
```

`make test` also runs `TestDST_PlannedQuick`: 300 seeds with the
planned-failover actors enabled (see below; `DST_PLANNED_TRIALS` overrides
the count). The planned fidelity tests share one 400-seed actors-on sweep,
run in parallel; `TestPlannedSweep_ReachesEveryOutcome` searches further
seeds, in parallel batches up to seed 3000, for any terminal outcome the
shared range misses: `PromotedUnconfirmed` and `ManualRecoveryRequired` need
a fault inside the few polls between a target's `read_only=OFF` and its
confirmation, so an engine change that moves a run's timing by one poll
moves them to other seeds.

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
   weights must sum to 100 (checked at `init`). The binary log rotation
   faults (`failFlush`, `slowFlush`) and the pre-reclone wait
   (`recloneWait`) are drawn from a PRNG stream of their own
   (`flushFaults`), so they add ops to a seed's schedule without changing
   the ones it had before they existed
   (`TestGenerateTrial_FlushFaultsOnlyAdd`). The data-directory regress
   (`regressSite`, with the `regressBoot` that boots the site again) is
   drawn from a third stream (`ownLossFaults`,
   `TestGenerateTrial_OwnLossFaultsOnlyAdd`). Execution consumes no
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
made just before `HealAt` can wait out its admission deadline, and it starts
after the last long outage ends (`Trial.settleStart`) rather than at
`HealAt`. The zero
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
  The snapshot write is the runner's two steps (`updateCRStatus`): read and
  merge, a scheduler yield (`store.snapshotCommit`), then a re-read that
  keeps the planned block, the hold and `AutomaticFailoverHeld` from the
  fresh object and merges the failover record again over the fresh one
  (K058), so a newer record committed between the two steps (a run's
  terminal write) is not moved back.
- **Restore.** Every manager start reads the object uncached through the
  store (`startView`), so a read outage or a dead process fails it, as the
  runner's `plannedFailoverView` does: a fresh start with an empty slot
  fails and the next reconcile retries it (the restart's anti-flap check
  waits for the start that succeeds); a replacement, or a start whose slot
  already holds a claim, proceeds with no view (no hydration, no planned
  restore). The manager is then restored by the production
  `restoreFailoverState` / `restorePlannedFailoverState` logic
  (`RestoreFailoverStateForSim` from the cached object,
  `RestorePlannedFailoverStateForSim` from the view) and wired with
  `WireRegisteredManager`.
- **The registry and manager replacement.** `simRegistry`
  (`controller.ManagerRegistry`) is one operator process: the current
  manager, retiring managers and one slot. `replaceManager` retires the
  current manager (`RetireForSim`); it keeps polling for 0–3 polls (a Poll
  in flight), its successor sees it through `ManagerWiring.RetiringBusy`
  (`ChangingSitesForSim`) and `RetiringUpdateInFlight`, and the slot is
  shared. The registry keeps the runner's retired-record stash and passes it
  as `ManagerWiring.RetiredFailover`. `managerAbsent` retires the current
  manager and leaves the group without one for N polls (its replacement's
  starts fail); the window's end starts a fresh manager, restored from the
  object. While no manager is registered the operator's `/active-site`
  answers 503. A restart builds a new registry (the slot and the stash are
  dropped).
- **Ordered updates.** `update_actor.go` is a step model of
  `ExecuteTargets`' handoff over the real claim API (`BeginOrderedUpdateForSim`,
  `ClaimOrderedUpdateForSim`, `RecordHandoffForSim`, `FinishOrderedUpdateForSim`):
  fence the active, wait for the standby, then STOP, RESET and
  read_only=OFF on one pinned session, recording the handoff in the same
  step as the callback does. A read_only=OFF that returns an error is
  recorded too when a pooled probe then finds the standby writable, as
  `ExecuteTargets` does (N2); without that evidence nothing is recorded.
  It keeps stepping on a retired manager; when it ends, the successor's
  predecessor guard is cleared.
- **Status-write loss.** `pfWriteLost`, `pfWriteAmbiguous`, `pfWriteLate`,
  `pfReadOutage` and `pfStatusDenied` (with restarts inside the outage)
  script the store; the fault stream's status and out-of-band outages apply
  to the same object. Half the `pfWriteLate` draws are a next-poll late
  commit paired with a settlement read outage (`pfSettleOutage`) and a lost
  metadata write, so the settlement ends Unknown and the late commit lands
  while the engine is alive. `pfMetaFault` makes the engine's next metadata
  writes (marker, request consumption, deadline annotations, ack cleanup)
  time out, commit with the response lost, or be forbidden; a no-op write is
  decided from its base and cannot meet one. A settlement fence always
  moves the resourceVersion (a fence that changes nothing is Unknown).
- **Long outages.** A quarter of the status denials and manager absences
  are long (`pfStatusDeniedLong` 130-169 polls, `managerAbsentLong` 310-349
  polls), start just after a planned request, and outlive `HealAt`, so the
  2m status-stall marker and the 5m `ManagerUnavailable` end are generated.
- **Hold acknowledgements.** `pfAckHold` acknowledges a standing hold K
  polls after it appears (some values whitespace-padded), inside the fault
  window only.
- **Pinned sessions.** `simChecker.PinSession` returns a `simSession` pinned
  to the site's server incarnation; a crash, restart or pod replacement
  moves the incarnation and every later call on the session fails, and so
  does every call after a transport failure on the session (a timed-out
  statement, applied or not), as go-sql-driver discards the connection. A
  MySQL error leaves it usable. Pooled calls reach whichever server runs
  now.
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
  `restoreFenceMu`) is held. The scheduler tracks the running actor, so a
  claim release is attributed to the actor that made it. The production
  yield points are aliases of the controller's `Yield*ForSim` constants. A
  manager polls once per tick: on a scheduler
  draw the engine pass runs first and a yield inside it may pull the tick's
  Poll forward, but never adds a second one (that would advance debounce
  with no time passing). Only the planned and update actors' statements can
  be slow: the topology manager probes from per-site goroutines, so there
  are no TM-internal yields (F2).
- **Ops.** `plannedFailover`, `orderedUpdate`, `replaceManager`,
  `managerAbsent` (and its long form), the store faults above,
  `oobLateWrite` (an older record replayed through the production recorder,
  and its original patch delivered late with its resourceVersion
  precondition; both must leave the newer record), `restartPromotedTarget`
  (after the run's `read_only=OFF` an application write lands on the target
  and it restarts, so it boots `read_only=ON` with its own transactions: the
  §7.1 `WasWritable` stage), `replacePod` (optionally onto a regressed data
  directory under a fresh `server_uuid`; never the writable site or the
  last copy of a transaction) and `pfAckHold`.
- **Invariants.** `plannedMonitor` checks the §15.1 planned invariants
  (`PlannedFenceWithoutAuthority`, `PlannedPromoteBesideWritable`,
  `PlannedPromoteWithoutGate`, `SucceededBesideWritable`,
  `UnfenceBesideWritable`, `UnfenceUnattributed`,
  `ForeignMutationUnderClaim`, `PlannedMutationWithoutRun`, `ClaimRevoked`,
  `ZombieClaim`, `OutcomeDishonest`, `RepeatedPromotion`,
  `WatermarkRegressed`, `StatusRecordRegressed`, `OOBRecordRegressed`,
  `AutomationHeldByPendingRequest`, `HoldNotHonoured`, `MarkedRunResumed`,
  `UnconfirmedBlocksEmergency`, `HoldWithoutDataAtRisk`,
  `FencedSourceNeverHealed`, `PlannedRunStuck`) on the events, the landed
  mutations, the store's commits, the yields and the slot
  (`HeldForSim`/`ReleasesForSim`). All of them count rogue writability, with
  the documented scope:
  - a rogue `read_only=0`, or a pod booting writable, landing after the
    run's own last peer probe is the §18 R2 residual (`RogueRaceResidual`),
    not a violation; a writable peer the operator cannot read does not
    block a rollback's unfence (§9 step 1; `UnfenceBesideUnreadable`);
  - a read-only reader the operator cannot read is allowed at the fence,
    the gate and the confirmation (§4.1: only primary-candidate and dr-only
    peers must answer), so a reader made writable where the operator cannot
    see it does not count against `PlannedFenceWithoutAuthority`,
    `PlannedPromoteBesideWritable`, `SucceededBesideWritable` or
    `OutcomeDishonest` (`UnreadableReaderWritable`); a reader it can read,
    and every promotable site, still count;
  - a writable primary-candidate or dr-only site (the source included) the
    operator cannot read at the confirmation does not count against
    `SucceededBesideWritable` or `OutcomeDishonest` only when the confirming
    process last read it read-only, at its pre-write gate or an earlier
    confirm probe of the run, and an external writer made it writable
    after that probe (§7.3; `UnreadablePeerResidual`). A site that process
    never read for the run, last read writable, or that only an earlier
    process read, counts: those are the `(unreadablePeer)` classes;
  - a target unfence needs a pre-write gate probe in the same engine pass;
    the gate window closes at the end of the pass and of the run;
  - a run ended by its terminal marker is judged when the marker commits,
    and a `ManualRecoveryRequired` marker is judged as a hold then;
  - every landed planned mutation belongs to a live run; only the engine
    releases the claim; an ordered update and another run are bound by a
    standing hold like the topology manager;
  - outcomes are judged with what the run could know: `Succeeded` at its
    confirm probe (the claim is released at Publishing), `SourceLeftFenced`
    with an unreachable source by its reason (§6.2) or ended while no
    manager was registered, and a cross-process re-run's
    `PromotedUnconfirmed` / `ManualRecoveryRequired` without the promotion
    memo (§7.1). `RolledBack` over a source that is not writable is honest
    when the source was writable at a yield of the rollback (the run read
    it so; its sidecar may fence it again before the record commits), and
    a `read_only=OFF` the process died before sending does not make a
    `RolledBack` dishonest, nor does one sent to a server that was then
    replaced (the target answers under another `server_uuid`: the DST-3
    ruling's rollback);
  - a hold is justified (`HoldWithoutDataAtRisk` is quiet) when a promotion
    intent exists, the deciding pass cannot read the target and another
    site is writable (`UnreadableTargetHold`): the target may hold the
    run's writes beside that writer. A hold over a target found replaced
    (another `server_uuid`) is justified when the deciding process cannot
    prove that no clear reached the replaced server (another process than
    the one that wrote `Promoting`, or that process after it sent one) and
    the DST-3 ruling's evidence is missing: the run's own record is
    durable, a site is writable or unreachable, the source is not fenced
    by the run or lacks part of F, or the replacement holds transactions
    outside F (`ReplacedTargetHold`). With that evidence the hold is
    `HoldWithoutDataAtRisk`, and so is one set by the process that wrote
    `Promoting` and sent no clear;
  - `FencedSourceNeverHealed` judges only the run's own fence (landed, in
    the source's current incarnation, still set, never healed) under the
    re-assert's preconditions, and only until the topology manager takes
    authority away from the source: it counts another site as the group's
    primary (its `Status().ActiveSite` after a poll names another site:
    the one writable promotable site it observed, or one it promoted), or
    it fences the source or repoints it at another site itself. After that
    the re-assert no longer names the source and the manager owns it. A
    site that is only writable for a while without being adopted does not
    count: a read-only reader an external writer opened (the manager fences
    it and never adopts it), or a site writable while no manager polled;
  - `UnconfirmedBlocksEmergency` is judged over a window in which, at every
    poll, no site was writable, the target stayed unreachable, an operator
    with a manager ran, and the source was reachable and complete; the
    operator owes the source's promotion within the grace of such a window.
    The window opens when the run ends `PromotedUnconfirmed`, whether or
    not its target answers at that moment.
    The source becoming writable, or the manager counting any site as the
    primary, ends the judgment. Everything else only starts the window
    again, at that poll: a target that answers (for one poll or for good),
    a writable site the manager has not adopted, an operator death or
    restart, a poll without a manager, and an injected fault on a promotion
    statement the manager sent to the source (the relay drain, `STOP
    REPLICA`, `RESET REPLICA ALL`, the two read-only clears). A failing or
    abandoned `FLUSH BINARY LOGS` does not: a rotation never stops a
    promotion;
  - a standing automation hold is the documented manual-recovery state: the
    end-state convergence invariants and the dual-writable check do not
    judge it, but every acknowledged transaction must still exist on some
    site (`SilentDataLoss`);
  - while a run holds the claim with its status writes stalled, the
    manager's split-brain fence is deferred up to the 2m stall deadline
    (§6.4), and `DualWritableUnresolved` counts only past it; the same
    holds while a run that is rolling back keeps the claim, up to the 30s
    rollback deadline (§9: it retries a target it cannot read back at
    `super_read_only=ON`);
  - a request annotation left unconsumed after the settle window is
    `PlannedRunStuck`.

  The planned harness also adjusts three pre-existing invariants for the
  actors: promotions by the planned run and the update actor are detected
  across polls and armed for anti-flap bookkeeping when recorded; an ordered
  update is not held by the cooldown (it is a maintenance handoff); and a
  window with no registered manager is no operator, like a dead one.

## Binary log rotations (#229, #264)

The operator's best-effort `FLUSH BINARY LOGS` (the promotion candidate
before its writes are enabled, the fenced source at a planned failover or an
ordered update's handoff, a reclone recipient before CLONE) runs through the
production `FailoverController.rotateBinlog`, bounded by the trial's fake
clock: the harness builds the controller with
`NewFailoverControllerWithClock`.

- **The model.** Each site has a file counter and the active file's
  `Previous_gtids` (the executed set when a rotation opened it). A rotation
  changes nothing else, works on a fenced replica, and spends neither the
  mutation-fault counters nor the mid-`Execute` crash countdown, so a fault
  armed for a promotion statement still hits that statement.
- **Faults.** `failFlush site n`: the next n rotations fail, nothing
  rotated. `slowFlush site n [k=1]`: the next n outlive the operator's
  bound. The simulated call advances the fake clock by
  `controller.BinlogRotationTimeout`, which fires the operator's timer, and
  then blocks until the operator cancels it; with `k=1` the server completes
  the rotation at the next tick, after the operator has moved on (the
  promotion may have enabled writes by then). `rotateBinlog` calls the
  flush from a goroutine and waits for it or the timer, so the model still
  sees one actor at a time, and nothing touches the model after the
  cancellation.
- **Time inside a poll.** A slow rotation takes one second of fake time
  inside a poll, so a poll is no longer always one second. The cooldown
  invariant is judged on the fake clock for that reason (the window of each
  promotion's poll), not on poll numbers.
- **The pre-reclone wait.** `recloneWait site n k` requests a reclone of a
  site with PITR archiving "on": the manager's `checkReclone` reads the
  site's simulated sidecar status, rotates the site's binary log
  (`RotateBinaryLog`, which also names the file the rotation opened) and
  waits for a Drained scan of that file. In production each step of the
  wait (the status request and the rotation) runs on a goroutine of its own
  and a later poll picks its result up; a trial runs the step inline
  (`SetRecloneTailStepRunnerForSim`), inside the Poll that started it, so
  the model still sees one actor at a time and a seed replays exactly. What
  that leaves out, a step that returns between two polls or never, is
  covered by controller and component tests
  (`reclone_archive_tail_test.go`,
  `TestReclone_HungRecipientDoesNotSlowThePollLoop`). The simulated sidecar
  never reports the tail archived (k: unreachable, still uploading, blocked,
  or a scan from before the rotation), and the harness withdraws the request
  n polls later or at heal-all, before the wait's deadline (2 minutes after
  the acknowledged rotation), because CLONE is not modelled
  (`HarnessRecloneWaitExpired` guards that). The request is applied again
  to every manager that replaces the one it was given to, as the annotation
  would be, so a restart or a manager replacement mid-wait starts the wait
  over. What is covered: the wait interleaved with failovers, planned runs
  and restarts. What is not: a wait that ends (archived, or at its bound)
  and the clone after it; component tests cover those
  (`test/component/binlog_rotation_test.go`).
- **Invariants.** `FlushAfterWriteGrant`: an actor never rotates a site
  after it enabled that site's writes. The grant stands until the site is
  fenced again by anyone (an operator actor, its sidecar, an external
  writer) or its server restarts or is replaced. A rotation that failed or
  was abandoned counts like one that rotated; one the server completes late
  does not. A planned failover's and an ordered update's grants are carried
  across polls while the site stays writable (a `super_read_only` clear
  that left `read_only` on ends with its poll, so a new process that finds
  the target read-only may rotate it again). The topology manager's are
  judged inside the poll only: its promotion is one call in one poll, and
  the two rotations it sends to a site it made writable in an earlier poll
  are by design. The split-brain priorities path fences the losers and runs
  the whole promotion on the policy winner, which is already writable, so
  that rotation lands on a writable site and the file it opens need not
  start at the recorded promotion GTID (carried across polls, the rule
  flagged it in 85 of 112,000 planned trials; nothing writes inside a poll
  in the model, so a trial cannot show the difference). The pre-reclone
  rotation's recipient can be a former primary an operator partition has
  kept writable. With
  the actors on: a planned run's rotation of a target it made writable,
  while that target is still writable or holds its own transactions
  outside F, is `RepeatedPromotion` (a promotion resumed from `WasWritable`
  or `Writable` sends none; a target that restarted read-only with exactly
  F before any write landed is `ReplicaReset` again, and a new process
  rotates it once more); every rotation by the planned
  path belongs to a live run (`PlannedMutationWithoutRun`,
  `MarkedRunResumed`), and a run that ends `NotStarted` sent none
  (`OutcomeDishonest`); a rotation by the topology manager or an ordered
  update while a planned run holds the claim is `ForeignMutationUnderClaim`
  (the pre-reclone rotation waits for the claim for this reason).
- **Signature.** A rotation adds two keys to the behavior signature
  (rotated, not rotated); why it did not rotate is repro detail, and a late
  rotation is the model's doing, like a fault.

## A site that lost its own transactions, and its reclone

A MySQL replication receiver drops every event that carries the replica's
own `server_id`, whatever `server_uuid` issued it (measured on MySQL 9.7).
A site that restarts on an older data directory while another site holds
what it issued can therefore never get those transactions back by
replication: it replicates everything else, both threads run, no error is
raised, and promoted it issues the same GTIDs again. The operator's answer
is `controller/own_transactions_lost.go`. It asks two questions with
different evidence: against the confirmed primary, under each
`server_uuid` the site has had, whether the site can be a replica (if not:
never start its replication, never promote it, reclone it); and against
every other site that answers (a witness), under the `server_uuid`s no
clone of the site has superseded, whether the site may be made writable
(if not: never promote it).

- **The model** (`ownloss.go`). `Cluster.Tick` no longer lets a site fetch
  a stream it owns: the stream under its original uuid and under every
  `server_uuid` a pod replacement or a clone gave it (`ownsStream`).
  Without that the model let a regressed site refetch its own lost tail,
  which no server does, and the defect could not show.
- **The fault.** `regressSite site k` crashes the site and rolls its data
  directory back `k` ticks under the same `server_uuid` (a volume that came
  back older); `regressBoot` boots it one to twelve polls later, fenced or
  with the read_only boot. One trial in four has one, three times out of
  five on the initial primary. Like `replacePod`'s regress it never
  destroys the last copy of a transaction. Unlike it, the site keeps its
  identity and may have been the writable primary.
- **The reclone.** CLONE is modelled as its effect, applied in one step
  inside the `Poll` that starts it: `Cluster.Reclone` gives the recipient
  the donor's executed set, no channel and a new `server_uuid`, and refuses
  when the operator cannot reach the recipient or the recipient cannot
  reach a running donor. The operator's side is production code up to that
  point: the request (`requestOwnLossReclone`, made again every poll),
  `checkReclone`'s donor confirmation, its claim checks, the check before
  the clone (`verifyOwnLossReclone`: the fenced recipient is read, then
  the donor, and the clone runs only if the donor holds everything the
  recipient holds and still holds transactions it issued and lacks) and
  the pre-reclone archive-tail wait. `SetRecloneRunnerForSim` replaces
  only the bootstrap goroutine (CLONE INSTANCE, the wait for the restart,
  the replication setup, and that goroutine's second run of the check),
  which runs on wall-clock retries a trial cannot replay. After the clone
  the site has no channel, and old-primary recovery configures it on a
  later poll, as after an operator restart between a clone and its
  replication setup. Not covered: a clone that fails or is killed
  part-way, an operator restart during a clone, the clone identity record
  and the keyring hold; unit and component tests cover those
  (`own_transactions_lost_test.go`, `own_transactions_lost_evidence_test.go`,
  `clone_server_uuid_record_test.go`,
  `test/component/own_transactions_lost_test.go`).
- **Durable state.** Each site's timeline, its `server_uuid`s and where a
  clone into it was issued, outlives every manager of a trial
  (`simSiteIdentityStore`, the group annotation in production). A manager
  loads it when it starts, as the runner does, so a manager that starts
  into an outage knows it before its first promotion decision.
- **The witness wait** runs on the manager's clock (250 ms in production).
  The trial clock stands still during a poll, so a promotion decision
  waits for the witness reads themselves, which return at once in the
  model; no special value is configured.
- **Invariant `PromotedWithoutOwnTransactions`** (model-level, at the
  statement). An operator actor never makes a site writable while a site
  the operator can read holds transactions of one of that site's own
  streams that the site lost. A statement on a site that was writable
  already is no grant. A holder the operator cannot read (crashed, or cut
  off from it) is not counted: with no evidence the promotion is the
  documented residual, see below. A stream a clone into the site
  superseded is not judged (`supersededByClone`): after a clone the site
  holds of its earlier streams exactly what its donor held and never
  receives more, so what another site holds beyond that the site gave up
  at the clone. Those are the transactions an administrator discards by
  recloning a divergent site, and refusing the recloned site for them
  would be the operator's defect, not its duty. Three constructed cases
  pin what the invariant still catches
  (`TestModel_PromotedWithoutOwnTransactions_ClonedSites`): a site regressed under
  its own `server_uuid`, a site whose pod was replaced on an older data
  directory under a fresh `server_uuid` (no clone superseded the earlier
  one), and a site that was recloned and then regressed.
- **The update actor** (`update_actor.go`) is a step model, but the
  invariant judges two things it used to do differently from
  `ExecuteHandoff`, so it follows production there: it reads the
  standby's handoff eligibility (`HandoffRefusalForSim`, the recorded
  blocks), confirms the active site is writable and reads the standby's
  and the active site's sets before it fences anything, and an abort
  lifts only the handoff's own fence (a source with `read_only` but not
  `super_read_only`, or without part of the set the handoff read from it,
  restarted in between and is left read-only).
- **What the sweeps found in the operator**, fixed with the changes:
  - an emergency failover promoted the regressed candidate although a
    disaster-recovery site that answered held its lost transactions (the
    picker reads the other sites too);
  - a planned failover's rollback unfenced a source that had restarted on
    an older data directory under the same `server_uuid` (it ends
    `SourceLeftFenced`);
  - the automatic reclone ran on a comparison made before a hold: a site
    that took acknowledged writes meanwhile (made writable by an external
    writer while partitioned) was cloned over, and those writes existed
    nowhere (`ModelHole`). The clone now rests on the check made when it
    starts;
  - the primary re-assert set a peer's recorded divergence aside before
    it compared, so it re-asserted a primary although the peer held
    transactions that primary had issued and lost;
  - an ordered update's abort unfenced an active site that had restarted
    on an older data directory during the handoff;
  - an ordered update handed off to a standby that lacked transactions it
    had issued which only a third, divergent site held: nothing compared
    the standby with that site. The audit and recovery now report such a
    standby when they find the divergent site;
  - a manager that started into an outage chose a promotion target before
    it had loaded the sites' earlier `server_uuid`s;
  - a site with an own-loss report that an external writer made writable
    was left writable beside the primary when the split-brain policy
    preferred it (the operator refused to resolve). It is fenced.
- **Residual, by design.** A site promoted while every holder of its lost
  transactions is down or unreachable: the operator cannot know. A trial
  then ends with one GTID naming two transactions, which the model's
  per-stream counters cannot show and no invariant flags. A recorded
  primary that restarts in place without part of its history while its
  replica holds it ends with no writable site and the primary reported
  `RecloneRequired`: re-assert refuses a target a peer is ahead of, and the
  operator elects nobody among read-only sites (`WedgedNoPrimary` exempts a
  target that is not GTID-complete, as before).
- **Not modelled.** An in-place restore and its reclone queue (the model
  has none), so the rule that a queued site is no witness is covered by
  unit tests only. A planned failover's rollback that unfences a source
  which regressed while the target cannot be read and another site holds
  the evidence is a planned-failover finding, see the planned README
  sections.

## Invariants

Per poll:
- **PromoteWhileObservedWritable** — never grant writability while another
  site is observed writable, unless it was fenced in the same cycle.
- **FenceAfterPromote** — fencing of other sites precedes the writable grant.
  A fence sent before the grant counts whatever its outcome (the same poll
  retries a failed one after the grant), and a site an external writer made
  writable after the grant is not a fence the promotion owed.
- **DualWritableUnresolved** — a dual-writable state that is fully
  operator-observable, with a resolution policy available (failover history
  or split-brain priorities), must be fenced within a grace window.
- **RepointDivergent** (model-level) — `CHANGE REPLICATION SOURCE` must never
  point a site at a source missing that site's transactions.
- **PromotedWithoutOwnTransactions** (model-level) — an operator actor never
  makes a site writable while a site the operator can read holds
  transactions the site issued itself and lost; a stream a clone into the
  site superseded is not judged (see above).

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
  the record is the operator dropping it, not the path failing. Only a
  snapshot taken after the record was made counts: a status write already
  in flight when an ordered update records its handoff commits without it.
  A planned run's Publishing record that commits after a later promotion
  does not move the harness's target back, as it does not move the
  operator's.

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

Not modeled (excluded from trials, guarded by loud errors): the bootstrap
goroutine and CLONE INSTANCE as statements (the bootstrap controller is
real and production-shaped, but the model never produces an empty site so
no clone goroutine can start; a reclone the operator requests for a site
that lost its own transactions is applied as its effect, see above; and a
scripted reclone request is withdrawn before its pre-reclone wait ends),
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
9 of the #235 interleaving table). An update actor keeps stepping on a
manager retired by a `managerAbsent` window, as the runner's retired
manager's update worker does. The topology manager's planned deferrals are
observed through their log line (`deferralTap`): a deferral through another
path without that line would go unseen.

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

Fixed findings, now regressions (`TestDST_PlannedQuick` fails on them):

- **`…(replacement)`: `CooldownViolated(replacement)`,
  `AntiFlapStateLost(replacement)`, `WedgedNoPrimary(replacement)`** (DST-1,
  fixed). An ordered update's handoff recorded on a manager that is then
  replaced, or that finishes on a retired manager, used to never reach its
  replacement. The record is now carried into the successor
  (`ManagerWiring.Predecessors`, `CarryRetiredFailoverRecord`, the runner's
  retired-record stash). Pinned in `test/component/dst_regression_test.go`.
  The `(replacement)` tags stay so a regression reports where the record
  was lost; `CooldownViolated(restart+stateLost)` keeps precedence over the
  tag.
- **`DualWritableUnresolved(confirmedClaim)`** (DST-4, fixed). A confirmed
  run's claim used to be held through a stalled Publishing write, blocking
  the manager's split-brain fence. The confirmed tail now releases the claim
  once the run's own record is durable. Pinned in
  `test/component/dst_regression_test.go`.

- **`HoldWithoutDataAtRisk(writableTargetWithChannel)`** (fixed; seeds
  12285 and 77871 of 1–112,000). The operator restarts in `Promoting`
  before `RESET REPLICA ALL`, and before the new process classifies the
  target an external `read_only=0` makes it writable. Writable with a
  channel and no memo used to be `Inconsistent(ForeignWritable)`: a hold.
  The run clears the target's flags only after `RESET REPLICA ALL` and the
  gate's no-channel read, so no process of the run made that target
  writable; the new process now ends `SourceLeftFenced{ForeignWritable}`
  without a hold, as the process with the memo does. Only the process
  whose memo says it sent a clear still holds.
- **`HoldWithoutDataAtRisk(replacedTargetSameProcess)`** (fixed; seeds
  29144, 59761 and 78553). `super_read_only=OFF` is applied but answers
  with an error, then the target pod is replaced under a new `server_uuid`
  and reads read-only inside F. The process that sent the clear used to
  hold, where another process rolls back by the DST-3 ruling on the same
  evidence. Both now take the facade's one decision
  (`replacedTargetRollbackSafe`); without the ruling's evidence the hold
  stands and is counted (`ReplacedTargetHold`). Of the eleven trials of
  this class in seeds 1–336,000, ten now roll back and one still holds
  (237169: the source is down when the target is found replaced). That
  trial is the only one in those seeds that the monitor rules changed with
  these fixes judge differently: with the three of them switched off (the
  replaced-target hold of the process that sent a clear, the confirmation's
  residual, `RolledBack` over a replaced target) the fixed engine fails
  237169 and nothing else new.
- **`OutcomeDishonest(unreadablePeer)+SucceededBesideWritable(unreadablePeer)`**
  (fixed; seeds 9297 and 104013). The confirmation used to take every
  unreadable primary-candidate as a warning. It now does so only for a site
  the confirming process last read read-only at its gate or an earlier
  confirm probe; a site it last read writable (seed 9297: the run's
  `ManualRecoveryRequired` write did not commit, then the peer stopped
  answering) is still a writer, and a site it never read (seed 104013: a new
  process finds the target writable and cannot read the source) leaves the
  run `Promoting` until the site answers or the promotion deadline ends it
  `PromotedUnconfirmed`.

The three are pinned in `test/component/dst_regression_test.go`
(`TestDSTPF_WritableTargetWithChannelLeavesSourceFenced`,
`TestDSTPF_ReplacedTargetAfterALostClearRollsBack`,
`TestDSTPF_NoSuccessBesideAnUnreadWriter`), and
`TestFixedFindings_AreClassesOfTheirOwn` keeps each fingerprint firing on
the history it names.

`TestDST_PlannedQuick` allows only `CooldownViolated(restart+stateLost)`
(`plannedAllowedClasses` in `planned_test.go`); anything else fails it. When
a finding is fixed, pin its shrunk trial in
`test/component/dst_regression_test.go`.

### Open planned-failover findings

`TestDST_PlannedQuick`'s 300 seeds are clean, and after the three fixes
above so are seeds 1–112,000, apart from the inherent class. Seeds
112,001–336,000 are not: besides more trials of the three fixed classes
(14 on the base, none after the fixes) they hold five trials in four
classes that the first 112,000 seeds never reached. They fail the same way
before and after the fixes, none is allowed by the gate, and none has been
analysed further than the table says. Each has a fingerprint of its own:

| Fingerprint | Seeds | What happens | First reading |
|---|---|---|---|
| `OutcomeDishonest(targetLacksFencedSet)` | 166664, 207074 | The run confirms its promotion; in the next poll an external writer fences the target and its pod is replaced on a regressed data directory, before the confirmed tail records the promotion. The monitor compares the fenced set with the target as it is when the terminal record lands, not as it was at the confirm probe. | Probably the harness: `replacePod` never replaces a writable site, but here the site was fenced a moment earlier. The engine recording a promotion its confirmation is one poll old for is by design. |
| `OutcomeDishonest(rolledBackAfterClear)` | 265453 | Both clears reach the target; before any transaction commits on it an external writer fences it again and makes the source writable, and the target crashes. A new process finds the target read-only with exactly F and rolls back by the re-proof's rule ("no transaction of the run committed"). The monitor counts every `read_only=OFF` that reached the target's current server against `RolledBack`. | The monitor and the re-proof disagree about what "nothing was written" means for a target that was writable for a moment and holds no transaction of its own. Needs a decision. |
| `OutcomeDishonest(rolledBackFencedSource)` | 310992 | The fence is never sent (the source is cut off from the operator from `Fencing` on); the source's sidecar fences it on lease expiry. After a restart and the heal the run ends `RolledBack`, which says the source is writable, while the source is read-only by its sidecar's fence. | The misleading-outcome family ("ends `SourceLeftFenced` over a source that was never fenced", seen in the earlier sweep): the terminal outcome is decided from what the run sent, not from what the source reads when the record lands. |
| `FencedSourceNeverHealed` | 199484 | The fence lands, but its `Draining` record does not commit (a late commit refused after settlement). The source is then cut off, the operator restarts twice and the run rolls back `SourceLost`. The new process has neither the fence memo nor an `Applied` fence record, so the source is not named for the re-assert; with the other primary-candidate crashed and the third site dr-only, no site is writable after the settle window. | The operator's: a fence proven only in a dead process's memory is not attributable after a restart, and nothing heals it. Needs a design decision (attribute by the durable intent and the `server_uuid` when naming the source). |

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
  are covered by component tests only (CLONE is modeled only as the effect
  of a reclone of a site that lost its own transactions).
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
