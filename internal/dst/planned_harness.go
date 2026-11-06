package dst

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// plannedHarness is the planned-failover actor machinery of one trial
// (§15.1): the simulated object and store, the process's manager registry,
// the production PlannedFailoverEngine over the production facade
// (NewPlannedFailoverTopology on the registry), the update actors, the
// production out-of-band recorder over the simulated object, the yield
// scheduler and the invariant monitor. It exists only when
// Trial.Actors.Enabled.
type plannedHarness struct {
	r     *trialRunner
	nn    types.NamespacedName
	store *simPlannedStore
	reg   *simRegistry
	sched *yieldScheduler
	mon   *plannedMonitor
	sink  *simSink

	// eng and facade belong to the current process; a restart builds new
	// ones (the engine and facade memos die with the process).
	eng    *controller.PlannedFailoverEngine
	facade controller.PlannedFailoverTopology
	runSeq int
	// nextEngine is when the engine's requeue is due (zero: none);
	// lastEngineRV is the object's resourceVersion at the last pass, so a
	// change to the object (a watch event) triggers a pass.
	nextEngine   time.Time
	lastEngineRV int64

	// updates are the live update actors of this process.
	updates []*updateActor

	// recorder is the production out-of-band recorder over the simulated
	// object; oobRecs are the records it accepted, for OpOOBLateWrite.
	recorder controller.FailoverStateRecorder
	oobRecs  []controller.FailoverRecord

	ops  []Op
	next int

	// unsupported counts actor ops that could not run (no manager to
	// start an update on, no earlier record to replay).
	unsupported map[OpKind]int
	counts      map[string]int

	restart *restartArm

	sidecarNext       int
	builtSinceRestart bool

	// pendingReset is the pf or upd actor whose RESET REPLICA ALL landed on
	// a site, until its read_only=OFF lands there (a promotion).
	pendingReset map[string]Actor

	// tmPolled and retiringPolled record that this tick's Poll of the
	// current and of the retiring managers ran: a manager polls once per
	// poll interval, so a yield may pull its Poll forward (into an engine
	// pass that runs first) but never adds a second one to the tick, which
	// would advance its debounce with no time passing.
	tmPolled, retiringPolled bool
	// firstTick forces the poll-first order on the first tick of a process:
	// its first Poll precedes its first engine pass (§15.1).
	firstTick bool
}

// actorPromotion is a planned run's or an ordered update's promotion.
type actorPromotion struct {
	site  string
	actor Actor
}

// promotions detects pf and upd promotions across polls: a RESET REPLICA
// ALL and then a read_only=OFF by the same actor on the same site.
func (pl *plannedHarness) promotions(events []Event) []actorPromotion {
	var out []actorPromotion
	for _, e := range events {
		a := effectiveActor(e.Actor)
		if (a != ActorPF && a != ActorUpdate) || !effectiveOutcome(e.Outcome) {
			continue
		}
		switch {
		case e.Kind == EvResetReplica:
			pl.pendingReset[e.Site] = a
		case e.Kind == EvSetRO && e.Detail == "on=false" && pl.pendingReset[e.Site] == a:
			delete(pl.pendingReset, e.Site)
			out = append(out, actorPromotion{site: e.Site, actor: a})
		}
	}
	return out
}

// restartArm is an armed OpRestartPromotedTarget: once the run clears
// read-only on its target, the target restarts k yields later.
type restartArm struct {
	k      int
	target string
	fired  bool
}

// plannedSettlePolls is the minimum settle window of a trial with actors: a
// request made just before HealAt may wait out its admission deadline (2m)
// before it ends.
const plannedSettlePolls = 150

// Planned-failover settings of the simulated group: the lag wait and drain
// are short so a run fits a trial.
var (
	simMaxLagWait   = metav1.Duration{Duration: 20 * time.Second}
	simDrainTimeout = metav1.Duration{Duration: 3 * time.Second}
)

func newPlannedHarness(r *trialRunner) *plannedHarness {
	t := r.trial
	nn := types.NamespacedName{Namespace: r.cfg.Namespace, Name: r.cfg.Name}
	obj := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: nn.Namespace, Name: nn.Name}}
	for i, name := range t.SiteNames {
		obj.Spec.Sites = append(obj.Spec.Sites, v1alpha1.SiteSpec{Name: name, Role: v1alpha1.SiteRole(t.Roles[i])})
	}
	cooldown := metav1.Duration{Duration: time.Duration(t.CooldownSec) * time.Second}
	obj.Spec.FailoverCooldown = &cooldown
	obj.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{MaxLagWait: &simMaxLagWait, DrainTimeout: &simDrainTimeout}
	if p := r.persisted; p != nil && !p.LastFailover.IsZero() {
		lf := metav1.NewTime(p.LastFailover.UTC().Truncate(time.Second))
		obj.Status.LastFailover = &lf
		obj.Status.LastFailoverTarget = p.LastFailoverTarget
	}
	if f := r.persistedFailover; f != nil {
		if a, err := controller.FailoverRecordAnnotations(*f); err == nil {
			obj.SetAnnotations(a)
		}
	}

	pl := &plannedHarness{
		r:            r,
		nn:           nn,
		store:        newSimPlannedStore(obj, r.clk),
		sched:        newYieldScheduler(t.Actors.SchedSeed),
		mon:          newPlannedMonitor(r.cluster, t.SiteNames, t.Roles),
		sink:         &simSink{},
		ops:          append([]Op(nil), t.Actors.ActiveOps()...),
		unsupported:  map[OpKind]int{},
		counts:       map[string]int{},
		lastEngineRV: -1,
		pendingReset: map[string]Actor{},
	}
	pl.newProcess()
	pl.mon.cur = pl.store.Object()
	pl.mon.claim = slotView{pl}
	pl.mon.onRecorded = func(target string, confirmedAt time.Time) { r.armPromotion(target, confirmedAt) }
	pl.store.dead = r.cluster.OperatorDead
	pl.store.clusterStatusDenied = r.cluster.StatusDenied
	pl.store.onCommit = pl.mon.onCommit
	pl.recorder = controller.NewAnnotationFailoverStateRecorder(
		&storeClient{store: pl.store, denied: r.cluster.StateDenied}, nil, nn)

	r.cluster.setPlannedHooks(plannedHooks{
		yield:      pl.sched.Yield,
		slow:       t.Actors.isSlow,
		onMutation: pl.onMutation,
		onEvent:    pl.mon.onEvent,
	})

	pl.sched.trackLock("Cluster.mu", r.cluster.tryLockCluster)
	pl.sched.trackLock("simPlannedStore.mu", func() bool {
		if !pl.store.mu.TryLock() {
			return false
		}
		pl.store.mu.Unlock()
		return true
	})
	pl.sched.trackLock("simRegistry.mu", func() bool {
		if !pl.reg.mu.TryLock() {
			return false
		}
		pl.reg.mu.Unlock()
		return true
	})
	for _, probe := range []struct {
		name string
		try  func(*controller.TopologyManager) bool
	}{
		{"tm.mu", (*controller.TopologyManager).TryLockManagerForSim},
		{"failoverStateWriteMu", (*controller.TopologyManager).TryLockFailoverStateWriteForSim},
		{"restoreProtectionMu", (*controller.TopologyManager).TryLockRestoreProtectionForSim},
		{"restoreFenceMu", (*controller.TopologyManager).TryLockRestoreFenceForSim},
	} {
		pl.sched.trackLock(probe.name, func() bool {
			for _, tm := range pl.managers() {
				if !probe.try(tm) {
					return false
				}
			}
			return true
		})
	}
	pl.sched.observe(pl.mon.onYield)
	pl.sched.observe(pl.onYield)
	pl.registerSchedActors()

	// The topology manager logs its planned deferrals; the tap turns them
	// into monitor input (AutomationHeldByPendingRequest).
	r.logger = slog.New(&deferralTap{next: r.logger.Handler(), fn: pl.mon.noteDeferral})
	return pl
}

// newProcess builds the per-process state: the registry (slot), the facade
// and the engine. Called at setup and at every operator restart.
func (pl *plannedHarness) newProcess() {
	if pl.reg != nil {
		pl.reg.shutdown()
	}
	pl.reg = newSimRegistry(pl.nn)
	pl.reg.retire = (*controller.TopologyManager).RetireForSim
	pl.reg.changingSites = (*controller.TopologyManager).ChangingSitesForSim
	hooks := controller.SimHooks{Yield: pl.sched.Yield}
	pl.facade = controller.NewPlannedFailoverTopology(pl.reg)
	controller.SetSimHooksForSim(pl.facade, hooks)
	pl.eng = controller.NewPlannedFailoverEngine(pl.store, pl.facade, pl.sink, pl.r.clk, hooks)
	pl.eng.SetRunIDSourceForSim(func() string {
		pl.runSeq++
		return fmt.Sprintf("run-%04d", pl.runSeq)
	})
	pl.updates = nil
	pl.nextEngine = time.Time{}
	pl.lastEngineRV = -1
	pl.builtSinceRestart = false
	pl.firstTick = true
}

// managers returns every manager of the process: the current one and the
// retiring ones.
func (pl *plannedHarness) managers() []*controller.TopologyManager {
	snap := pl.reg.Snapshot(pl.nn)
	out := append([]*controller.TopologyManager(nil), snap.Retiring...)
	if pl.r.tm != nil {
		out = append(out, pl.r.tm)
	}
	return out
}

// registerSchedActors registers the actors a yield may step, in step order.
func (pl *plannedHarness) registerSchedActors() {
	r := pl.r
	pl.sched.addActor(&schedActor{
		name:  "tm",
		pct:   30,
		ready: func() bool { return !r.cluster.OperatorDead() && !pl.tmPolled && pl.reg.Current() != nil },
		step:  func(ctx context.Context) { pl.pollCurrent(ctx) },
	})
	pl.sched.addActor(&schedActor{
		name:  "retiring",
		pct:   30,
		ready: func() bool { return !r.cluster.OperatorDead() && !pl.retiringPolled },
		step:  func(ctx context.Context) { pl.pollRetiring(ctx) },
	})
	pl.sched.addActor(&schedActor{
		name:  string(ActorPF),
		pct:   40,
		ready: func() bool { return !r.cluster.OperatorDead() },
		step:  func(ctx context.Context) { pl.stepEngine(ctx) },
	})
	pl.sched.addActor(&schedActor{
		name:  string(ActorUpdate),
		pct:   40,
		ready: func() bool { return !r.cluster.OperatorDead() && len(pl.liveUpdates()) > 0 },
		step: func(ctx context.Context) {
			if live := pl.liveUpdates(); len(live) > 0 {
				live[0].stepOnce(WithActor(ctx, ActorUpdate))
				pl.afterUpdates()
			}
		},
	})
	pl.sched.addActor(&schedActor{
		name: string(ActorSidecar),
		pct:  20,
		step: func(ctx context.Context) { pl.tickOneSidecar(ctx) },
	})
	if r.trial.Actors.RogueAtYield {
		pl.sched.addActor(&schedActor{
			name: string(ActorRogue),
			pct:  5,
			// Only inside the fault window: warmup and the settle window
			// after HealAt are fault-free by construction.
			ready: func() bool {
				p := r.cluster.Poll()
				return p >= r.trial.WarmupPolls && p < r.trial.HealAt
			},
			step: func(context.Context) {
				names := r.trial.SiteNames
				r.cluster.RogueWritable(names[pl.sched.rng.IntN(len(names))])
			},
		})
	}
}

// slotView is the monitor's claimView over the live process's slot.
type slotView struct{ pl *plannedHarness }

func (v slotView) Held() (string, bool, bool) { return v.pl.reg.Slot(v.pl.nn).HeldForSim() }

func (v slotView) Releases() []claimRelease {
	var out []claimRelease
	for _, n := range v.pl.reg.Slot(v.pl.nn).ReleasesForSim() {
		out = append(out, claimRelease{RunID: n.RunID, Why: n.Why})
	}
	return out
}

// registerManager makes tm the current manager, wired from the uncached
// object exactly as the runner's startManager wires it.
func (pl *plannedHarness) registerManager(tm *controller.TopologyManager) {
	pl.reg.register(tm, pl.wiring())
	pl.builtSinceRestart = true
}

// restoreManager is startManager's restore for a new manager: the failover
// record, watermark and observed authority from the object (production
// restoreFailoverRecordFrom), then the planned-failover restore. It
// returns the record the manager started from.
func (pl *plannedHarness) restoreManager(tm *controller.TopologyManager, rec controller.FailoverStateRecorder) controller.FailoverRecord {
	obj := pl.store.Object()
	tm.RestoreFailoverStateForSim(obj, rec)
	tm.RestorePlannedFailoverStateForSim(obj)
	eff, _, _ := controller.EffectiveFailoverRecord(obj, pl.r.clk.Now())
	return eff
}

// wiring builds the ManagerWiring from the object (an uncached read).
func (pl *plannedHarness) wiring() controller.ManagerWiring {
	obj := pl.store.Object()
	var w controller.ManagerWiring
	if ownsSites(obj) {
		w.HydrateFrom = obj.Status.PlannedFailover.DeepCopy()
	}
	w.HoldFrom = plannedHoldOf(obj)
	return w
}

// plannedHoldOf is the controller's PlannedHoldFrom: either durable hold
// record holds.
func plannedHoldOf(obj *v1alpha1.MysqlFailoverGroup) *controller.PlannedHold {
	return controller.PlannedHoldFrom(obj)
}

// restartProcess is an operator restart: a new process with a new registry
// (the slot is gone), a new engine and facade (their memos are gone), no
// update actors, and leadership re-acquired. Its first engine pass runs
// after the new manager's first Poll (the per-poll order guarantees it).
func (pl *plannedHarness) restartProcess() {
	pl.mon.noteRestart()
	pl.newProcess()
}

// beginPoll advances the store (timed faults, late commits).
func (pl *plannedHarness) beginPoll(p int) { pl.store.BeginPoll(p) }

// applyDueOps applies the actor ops due at poll p.
func (pl *plannedHarness) applyDueOps(p int) {
	for pl.next < len(pl.ops) && pl.ops[pl.next].At <= p {
		pl.applyOp(pl.ops[pl.next], p)
		pl.next++
	}
}

func (pl *plannedHarness) applyOp(op Op, poll int) {
	r := pl.r
	switch op.Kind {
	case OpPlannedFailover:
		pl.store.SetRequest(op.Site)
	case OpOrderedUpdate:
		tm := pl.reg.Current()
		if tm == nil || r.cluster.OperatorDead() {
			pl.unsupported[op.Kind]++
			return
		}
		if a := startUpdateActor(r, tm); a != nil {
			pl.updates = append(pl.updates, a)
			pl.counts["updatesStarted"]++
		} else {
			pl.counts["updatesRefused"]++
		}
	case OpReplaceManager:
		if r.cluster.OperatorDead() {
			pl.unsupported[op.Kind]++
			return
		}
		pl.mon.noteReplace()
		if !r.lastPromotionAt.IsZero() {
			// Cleared by the next status write that carries the promotion.
			r.replacedBeforeRecord = true
		}
		old := r.tm
		tm := r.newManager()
		var inFlight func() bool
		if old != nil {
			inFlight = old.OrderedUpdateClaimedForSim
		}
		if !pl.reg.replace(tm, pl.wiring(), op.N, inFlight) {
			pl.unsupported[op.Kind]++
		}
		pl.builtSinceRestart = true
		r.tm = tm
		pl.counts["managersReplaced"]++
	case OpManagerAbsent:
		pl.reg.setAbsent(op.N)
	case OpPFWriteLost:
		pl.mon.noteFault()
		pl.store.LoseWrites(op.N)
	case OpPFWriteAmbiguous:
		pl.mon.noteFault()
		pl.store.AmbiguousWrites(op.N)
	case OpPFWriteLate:
		pl.mon.noteFault()
		pl.store.LateWrites(op.N, op.K)
	case OpPFReadOutage:
		pl.mon.noteFault()
		pl.store.ReadOutage(op.N)
	case OpPFStatusDenied:
		pl.mon.noteFault()
		pl.store.DenyStatus(op.N)
	case opPFSettleOutage:
		pl.mon.noteFault()
		pl.store.SettleOutage(op.N)
	case OpOOBLateWrite:
		pl.replayOOB()
	case OpRestartPromotedTarget:
		pl.restart = &restartArm{k: op.K}
	case OpReplacePod:
		r.cluster.ReplacePod(op.Site, op.K)
	case OpRestartOperator:
		r.restartOperator(poll)
	default:
		panic(fmt.Sprintf("dst: plannedHarness.applyOp unhandled kind %q", op.Kind))
	}
}

// recordOOB runs the production out-of-band recorder for rec.
func (pl *plannedHarness) recordOOB(ctx context.Context, rec controller.FailoverRecord) error {
	if err := pl.recorder.RecordFailoverState(ctx, rec); err != nil {
		return err
	}
	pl.oobRecs = append(pl.oobRecs, rec)
	return nil
}

// storedOOB is the out-of-band record on the object.
func (pl *plannedHarness) storedOOB() (controller.FailoverRecord, bool) {
	obj := pl.store.Object()
	if obj == nil {
		return controller.FailoverRecord{}, false
	}
	rec, err := controller.FailoverRecordFromAnnotations(obj.GetAnnotations())
	if err != nil || rec.IsZero() {
		return controller.FailoverRecord{}, false
	}
	return rec, true
}

// replayOOB delivers a late duplicate of an older out-of-band record
// write through the production recorder, which must refuse to overwrite
// the newer record (§5.3).
func (pl *plannedHarness) replayOOB() {
	if len(pl.oobRecs) < 2 {
		pl.unsupported[OpOOBLateWrite]++
		return
	}
	old := pl.oobRecs[len(pl.oobRecs)-2]
	_ = pl.recorder.RecordFailoverState(WithActor(context.Background(), ActorTM), old)
	pl.counts["oobReplayed"]++
}

// onMutation feeds the monitor and arms the restart-promoted-target fault
// (c.mu held: no cluster calls here).
func (pl *plannedHarness) onMutation(e Event, before, after SiteTruth) {
	pl.mon.onMutation(e, before, after)
	if pl.restart == nil || pl.restart.target != "" || effectiveActor(e.Actor) != ActorPF {
		return
	}
	if e.Kind == EvSetRO && e.Detail == "on=false" {
		if run := pl.mon.activeRun(); run != nil && run.target == e.Site {
			pl.restart.target = e.Site
		}
	}
}

// onYield counts the restart arm down and fires it (no lock is held at a
// yield).
func (pl *plannedHarness) onYield(string) {
	a := pl.restart
	if a == nil || a.target == "" || a.fired {
		return
	}
	if a.k > 0 {
		a.k--
		return
	}
	a.fired = true
	pl.counts["promotedTargetRestarted"]++
	pl.r.cluster.RestartSite(a.target)
}

// liveUpdates is the update actors still running, retiring managers'
// first.
func (pl *plannedHarness) liveUpdates() []*updateActor {
	var retiring, current []*updateActor
	for _, a := range pl.updates {
		if !a.active() {
			continue
		}
		if a.tm == pl.r.tm {
			current = append(current, a)
		} else {
			retiring = append(retiring, a)
		}
	}
	return append(retiring, current...)
}

// afterUpdates drops finished actors; when a replaced manager's update has
// ended and no other replaced manager holds one, the current manager's
// predecessor guard is cleared, as retireManagerLocked's goroutine does.
func (pl *plannedHarness) afterUpdates() {
	ended := false
	var endedOn []*controller.TopologyManager
	kept := pl.updates[:0]
	for _, a := range pl.updates {
		if a.active() {
			kept = append(kept, a)
			continue
		}
		if a.tm != pl.r.tm {
			ended = true
			endedOn = append(endedOn, a.tm)
		}
	}
	pl.updates = kept
	if !ended {
		return
	}
	// As retireManagerLocked's goroutine after the retired manager's update
	// workers exit: the handoff the update recorded there reaches the
	// current manager (DST-1).
	if cur := pl.r.tm; cur != nil {
		for _, a := range endedOn {
			controller.CarryRetiredFailoverRecord(cur, a)
		}
	}
	for _, a := range pl.updates {
		if a.tm != pl.r.tm && a.claimed() {
			return
		}
	}
	// The trial's manager, even inside a managerAbsent window: the sim's
	// absent manager comes back as the same object, while production would
	// register a fresh one that never had the guard.
	if cur := pl.r.tm; cur != nil {
		cur.SetPredecessorUpdateInFlightForSim(false)
	}
}

// pollActors is the per-poll actor order: the current manager's Poll, the
// retiring managers', the update actors' steps (retiring first), then the
// engine pass when its requeue is due or the object changed.
//
// The reconcile and the update goroutine run concurrently with Poll in
// production, so on a scheduler draw the update steps and the engine pass
// run first and the tick's Polls run at one of their yields (or after
// them); never on a process's first tick.
func (pl *plannedHarness) pollActors(ctx context.Context) {
	pl.tmPolled, pl.retiringPolled = false, false
	early := !pl.firstTick && pl.sched.rng.IntN(2) == 0
	pl.firstTick = false
	if !early {
		pl.pollCurrent(ctx)
		pl.pollRetiring(ctx)
	}
	for _, a := range pl.liveUpdates() {
		if pl.r.cluster.OperatorDead() {
			break
		}
		pl.sched.run(string(ActorUpdate), func() { a.stepOnce(WithActor(ctx, ActorUpdate)) })
	}
	pl.afterUpdates()
	if pl.engineDue() {
		pl.sched.run(string(ActorPF), func() { pl.stepEngine(ctx) })
	}
	pl.pollCurrent(ctx)
	pl.pollRetiring(ctx)
}

// pollCurrent runs this tick's Poll of the current manager, once.
func (pl *plannedHarness) pollCurrent(ctx context.Context) {
	if pl.tmPolled || pl.r.cluster.OperatorDead() {
		return
	}
	pl.tmPolled = true
	if tm := pl.reg.Current(); tm != nil {
		pl.sched.run("tm", func() { tm.Poll(WithActor(ctx, ActorTM)) })
	}
}

// pollRetiring runs this tick's Polls of the retiring managers, once.
func (pl *plannedHarness) pollRetiring(ctx context.Context) {
	if pl.retiringPolled || pl.r.cluster.OperatorDead() {
		return
	}
	pl.retiringPolled = true
	for _, old := range pl.reg.retiringToPoll() {
		pl.counts["retiringPolls"]++
		pl.sched.run("retiring", func() { old.Poll(WithActor(ctx, ActorTM)) })
	}
}

// engineDue reports whether a reconcile of the group would run now: its
// requeue is due, or the object changed since the last pass.
func (pl *plannedHarness) engineDue() bool {
	if pl.r.cluster.OperatorDead() {
		return false
	}
	if pl.store.ResourceVersion() != pl.lastEngineRV {
		return true
	}
	return !pl.nextEngine.IsZero() && !pl.r.clk.Now().Before(pl.nextEngine)
}

// stepEngine runs one engine pass and schedules the next.
func (pl *plannedHarness) stepEngine(ctx context.Context) {
	pl.lastEngineRV = pl.store.ResourceVersion()
	requeue, _ := pl.eng.Step(WithActor(ctx, ActorPF), pl.nn)
	if requeue > 0 {
		pl.nextEngine = pl.r.clk.Now().Add(requeue)
	} else {
		pl.nextEngine = time.Time{}
	}
	pl.counts["enginePasses"]++
	pl.mon.noteEnginePass()
}

// tickOneSidecar ticks the next live site's sidecar (a scheduler step).
func (pl *plannedHarness) tickOneSidecar(ctx context.Context) {
	r := pl.r
	n := len(r.trial.SiteNames)
	for range n {
		i := pl.sidecarNext % n
		pl.sidecarNext++
		name := r.trial.SiteNames[i]
		if r.cluster.SiteCrashed(name) {
			r.sidecars[i] = nil
			continue
		}
		if r.sidecars[i] == nil {
			r.sidecars[i] = r.newSidecar(name)
		}
		r.sidecars[i].monitor.Check(WithActor(ctx, ActorSidecar))
		return
	}
}

// endPoll fires a pending restart arm and runs the per-poll invariants.
func (pl *plannedHarness) endPoll(p int) {
	if a := pl.restart; a != nil && a.target != "" && !a.fired {
		a.fired = true
		pl.counts["promotedTargetRestarted"]++
		pl.r.cluster.RestartSite(a.target)
	}
	pl.mon.endPoll(p, pl.reg.Current() != nil && pl.builtSinceRestart)
	pl.reg.endPoll()
}

// healAll clears the actor faults at HealAt.
func (pl *plannedHarness) healAll() {
	pl.store.ClearFaults()
	if pl.restart != nil && !pl.restart.fired {
		pl.restart = nil // an armed restart must not fire in the settle window
	}
	pl.reg.mu.Lock()
	pl.reg.absentPolls = 0
	pl.reg.mu.Unlock()
}

// finish runs the end-of-trial planned invariants.
func (pl *plannedHarness) finish() []Violation {
	if pl.mon == nil {
		return nil
	}
	return pl.mon.checkEnd(pl.r.trial.Polls - 1)
}

// signature adds the planned dimensions: per-run outcome classes, the phase
// of the first store fault, a replacement while the claim was held, and
// the yield classes at which another actor stepped.
func (pl *plannedHarness) signature() string {
	steps := make([]string, 0, len(pl.sched.steps))
	for c := range pl.sched.steps {
		steps = append(steps, c)
	}
	sort.Strings(steps)
	return pl.mon.signature() + "yieldSteps=" + strings.Join(steps, ",") + ";"
}

// result is the planned part of a TrialResult.
func (pl *plannedHarness) result() *PlannedResult {
	unsupported := map[OpKind]int{}
	for k, v := range pl.unsupported {
		unsupported[k] = v
	}
	yields := map[string]int{}
	for k, v := range pl.sched.yields {
		yields[k] = v
	}
	counts := map[string]int{}
	for k, v := range pl.counts {
		counts[k] = v
	}
	steps := map[string]int{}
	for k, v := range pl.sched.steps {
		steps[k] = v
	}
	var outcomes []string
	for _, id := range pl.mon.order {
		if r := pl.mon.runs[id]; r.terminal {
			outcomes = append(outcomes, string(r.outcome))
		}
	}
	return &PlannedResult{
		Object:      pl.store.Object(),
		StoreLog:    pl.store.Log(),
		Yields:      yields,
		Steps:       steps,
		Stats:       pl.mon.stats,
		Unsupported: unsupported,
		Counts:      counts,
		Outcomes:    outcomes,
		SinkEvents:  append([]string(nil), pl.sink.events...),
	}
}

// PlannedResult is what a trial with actors reports beyond TrialResult.
type PlannedResult struct {
	Object   *v1alpha1.MysqlFailoverGroup
	StoreLog []storeOp
	Yields   map[string]int
	// Steps counts, per yield class, the yields at which another actor
	// stepped.
	Steps       map[string]int
	Stats       plannedStats
	Unsupported map[OpKind]int
	// Counts are harness counters: enginePasses, updatesStarted,
	// updatesRefused, managersReplaced, retiringPolls, oobReplayed,
	// promotedTargetRestarted.
	Counts map[string]int
	// Outcomes are the terminal outcomes of the trial's runs, in run order.
	Outcomes   []string
	SinkEvents []string
}

// ---------------------------------------------------------------------------
// Sink and log tap
// ---------------------------------------------------------------------------

// simSink is the DST's controller.PlannedFailoverSink: it records what the
// engine reports.
type simSink struct {
	events    []string
	terminals []v1alpha1.PlannedFailoverStatus
}

var _ controller.PlannedFailoverSink = (*simSink)(nil)

func (s *simSink) Event(_ *v1alpha1.MysqlFailoverGroup, eventType, reason, message string) {
	s.events = append(s.events, eventType+"/"+reason+": "+message)
}

func (s *simSink) RunTerminal(_ types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) {
	if pf != nil {
		s.terminals = append(s.terminals, *pf.DeepCopy())
	}
}

func (s *simSink) LagWaitObserved(types.NamespacedName, string, time.Duration) {}

func (s *simSink) Deferred(_ types.NamespacedName, reason v1alpha1.PlannedFailoverDeferReason) {
	s.events = append(s.events, "deferred/"+string(reason))
}

// plannedDeferralMsg is the topology manager's planned-failover deferral
// log line (a log-schema contract message).
const plannedDeferralMsg = "cross-site action deferred: planned failover in progress"

// deferralTap forwards to next and reports every planned deferral line.
type deferralTap struct {
	next slog.Handler
	fn   func()
}

func (h *deferralTap) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= slog.LevelInfo || h.next.Enabled(ctx, l)
}

func (h *deferralTap) Handle(ctx context.Context, rec slog.Record) error {
	if rec.Message == plannedDeferralMsg {
		h.fn()
	}
	if h.next.Enabled(ctx, rec.Level) {
		return h.next.Handle(ctx, rec)
	}
	return nil
}

func (h *deferralTap) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &deferralTap{next: h.next.WithAttrs(attrs), fn: h.fn}
}

func (h *deferralTap) WithGroup(name string) slog.Handler {
	return &deferralTap{next: h.next.WithGroup(name), fn: h.fn}
}

// confirmedClaimHeld reports whether the live slot holds a run that has
// already made its target writable (its Publishing record not yet
// committed).
func (pl *plannedHarness) confirmedClaimHeld() bool {
	id, hold, ok := pl.reg.Slot(pl.nn).HeldForSim()
	if !ok || hold {
		return false
	}
	r := pl.mon.runs[id]
	return r != nil && r.targetWritable
}
