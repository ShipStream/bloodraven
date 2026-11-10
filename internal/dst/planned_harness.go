package dst

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"

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
	recorder  controller.FailoverStateRecorder
	oobClient *storeClient
	oobRecs   []controller.FailoverRecord

	ops  []Op
	next int

	// unsupported counts actor ops that could not run (no manager to
	// start an update on, no earlier record to replay).
	unsupported map[OpKind]int
	counts      map[string]int

	restart *restartArm

	sidecarNext       int
	builtSinceRestart bool

	// pendingReset is the pf run or update whose RESET REPLICA ALL landed
	// on a site, until its read_only=OFF lands there (a promotion) or the
	// attempt ends (K175).
	pendingReset map[string]resetOwner

	// tmPolled and retiringPolled record that this tick's Poll of the
	// current and of the retiring managers ran: a manager polls once per
	// poll interval, so a yield may pull its Poll forward (into an engine
	// pass that runs first) but never adds a second one to the tick, which
	// would advance its debounce with no time passing.
	tmPolled, retiringPolled bool
	// firstTick forces the poll-first order on the first tick of a process:
	// its first Poll precedes its first engine pass (§15.1).
	firstTick bool

	// startPending: the group has no registered manager and the next
	// reconcile starts one (retryStart): an earlier start's uncached read
	// failed, or an absence window ended. restartCheckPending: that start
	// completes an operator restart, whose rehydration is judged then.
	startPending        bool
	restartCheckPending bool

	// acks are the armed hold acknowledgements (OpPFAckHold).
	acks []*ackArm
	// releases are the slot's release notes attributed to the actor that
	// made them, until the monitor reads them.
	releases []claimRelease
}

// ackArm is an armed human acknowledgement of an automation hold.
type ackArm struct {
	delay      int
	padded     bool
	holdSeenAt int // first poll the hold was seen standing; -1 none yet
	done       bool
}

// tickAcks writes each armed acknowledgement delay polls after a hold first
// stands, naming the held run as a human would read it off status, before
// the settle window only (K174).
func (pl *plannedHarness) tickAcks(p int) {
	for _, a := range pl.acks {
		if a.done {
			continue
		}
		if p >= pl.r.trial.settleStart() {
			a.done = true
			continue
		}
		obj := pl.store.Object()
		h := controller.PlannedHoldFrom(obj)
		if h == nil || h.RunID == "" {
			continue
		}
		if a.holdSeenAt < 0 {
			a.holdSeenAt = p
		}
		if p < a.holdSeenAt+a.delay {
			continue
		}
		a.done = true
		v := h.RunID
		if a.padded {
			v = " " + v + "\n"
		}
		if err := pl.store.PatchAnnotations("holdAck", map[string]string{v1alpha1.AcknowledgePlannedFailoverHoldAnnotation: v}); err == nil {
			pl.counts["holdAcks"]++
		}
	}
}

// actorPromotion is a planned run's or an ordered update's promotion.
type actorPromotion struct {
	site  string
	actor Actor
	runID string // the planned run (pf promotions)
}

// resetOwner is the attempt a pending RESET REPLICA ALL belongs to: a
// planned run (by runID) or the update actors.
type resetOwner struct {
	actor Actor
	runID string
}

// promotions detects pf and upd promotions across polls: a RESET REPLICA
// ALL and then a read_only=OFF by the same attempt on the same site. A
// RESET whose attempt ended without that read_only=OFF is dropped, so a
// later run's rollback unfence of that site is not paired with it (K175).
func (pl *plannedHarness) promotions(events []Event) []actorPromotion {
	var out []actorPromotion
	owner := func(a Actor) resetOwner {
		o := resetOwner{actor: a}
		if a == ActorPF {
			if r := pl.mon.blockRun(); r != nil {
				o.runID = r.id
			}
		}
		return o
	}
	for _, e := range events {
		a := effectiveActor(e.Actor)
		if (a != ActorPF && a != ActorUpdate) || !effectiveOutcome(e.Outcome) {
			continue
		}
		switch {
		case e.Kind == EvResetReplica:
			pl.pendingReset[e.Site] = owner(a)
		case e.Kind == EvSetRO && e.Detail == "on=false":
			if pr, ok := pl.pendingReset[e.Site]; ok && pr == owner(a) {
				delete(pl.pendingReset, e.Site)
				out = append(out, actorPromotion{site: e.Site, actor: a, runID: pr.runID})
			}
		}
	}
	// Drop the RESETs of attempts that ended (after this poll's events,
	// which may pair the attempt's last read_only=OFF).
	for site, pr := range pl.pendingReset {
		switch pr.actor {
		case ActorPF:
			if r := pl.mon.runs[pr.runID]; r == nil || r.terminal || r.marked {
				delete(pl.pendingReset, site)
			}
		case ActorUpdate:
			if len(pl.liveUpdates()) == 0 {
				delete(pl.pendingReset, site)
			}
		}
	}
	return out
}

// restartArm is an armed OpRestartPromotedTarget: once the run clears
// read-only on its target, the target takes an application write and
// restarts k yields later (K028), so it boots read_only=ON holding its own
// post-promotion transactions (§7.1 WasWritable).
type restartArm struct {
	k      int
	target string
	runID  string
	fired  bool
	// rewritten: the run made the restarted target writable again.
	rewritten bool
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
		obj.Spec.Sites = append(obj.Spec.Sites, v1alpha1.SiteSpec{Name: name, ServerID: int64(101 + i), Role: controller.APISiteRole(t.Roles[i])})
	}
	cooldown := metav1.Duration{Duration: time.Duration(t.CooldownSec) * time.Second}
	obj.Spec.FailoverCooldown = &cooldown
	obj.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{MaxLagWait: &simMaxLagWait, DrainTimeout: &simDrainTimeout}
	if p := r.persisted; p != nil && !p.LastFailover.IsZero() {
		lf := metav1.NewTime(p.LastFailover.UTC().Truncate(time.Second))
		obj.Status.SetLastFailover(&lf, p.LastFailoverTarget, "")
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
		pendingReset: map[string]resetOwner{},
	}
	pl.newProcess()
	pl.mon.cur = pl.store.Object()
	pl.mon.claim = slotView{pl}
	pl.mon.onRecorded = r.armRecordedPromotion
	pl.mon.noManager = pl.noManager
	pl.store.dead = r.cluster.OperatorDead
	pl.store.yield = pl.sched.Yield
	pl.store.clusterStatusDenied = r.cluster.StatusDenied
	pl.store.onCommit = func(kind string, before, after *v1alpha1.MysqlFailoverGroup) {
		pl.mon.onCommit(kind, before, after)
		pl.noteCommit(before, after)
	}
	pl.oobClient = &storeClient{store: pl.store, denied: r.cluster.StateDenied}
	pl.recorder = controller.NewAnnotationFailoverStateRecorderWithClock(pl.oobClient, nil, nn, r.clk)

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
	pl.sched.onSwitch = pl.attributeReleases
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
	// The tm and retiring steps are the Poll bodies: the scheduler has
	// already put the actor on the stack, so wrapping them in sched.run
	// again would find it there and skip the Poll (K019).
	pl.sched.addActor(&schedActor{
		name:  "tm",
		pct:   30,
		ready: func() bool { return !r.cluster.OperatorDead() && !pl.tmPolled && pl.reg.Current() != nil },
		step:  func(ctx context.Context) { pl.pollCurrentBody(ctx) },
	})
	pl.sched.addActor(&schedActor{
		name:  "retiring",
		pct:   30,
		ready: func() bool { return !r.cluster.OperatorDead() && !pl.retiringPolled },
		step:  func(ctx context.Context) { pl.pollRetiringBody(ctx) },
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

// Releases returns the releases attributed since the last call, plus any
// not yet attributed (made at top level, outside every actor).
func (v slotView) Releases() []claimRelease {
	v.pl.attributeReleases("")
	out := v.pl.releases
	v.pl.releases = nil
	return out
}

// attributeReleases drains the slot's release notes and attributes them to
// owner, the actor that ran since the previous scheduler switch (K177).
func (pl *plannedHarness) attributeReleases(owner string) {
	for _, n := range pl.reg.Slot(pl.nn).ReleasesForSim() {
		pl.releases = append(pl.releases, claimRelease{RunID: n.RunID, Why: n.Why, By: owner})
	}
}

// startView is the start's uncached read of the group (the runner's
// plannedFailoverView), through the store, so an injected read outage or a
// dead process fails it. On an error a replacement (replacing) or a start
// whose slot already holds a claim proceeds with no view (nil, true): no
// hydration, no planned restore. Any other start fails (ok=false) and the
// reconcile retries it.
func (pl *plannedHarness) startView(replacing bool) (view *v1alpha1.MysqlFailoverGroup, ok bool) {
	obj, err := pl.store.Load(context.Background(), pl.nn)
	if err == nil {
		return obj, true
	}
	if _, _, held := pl.reg.Slot(pl.nn).HeldForSim(); replacing || held {
		pl.counts["managerStartNoView"]++
		return nil, true
	}
	pl.counts["managerStartFailed"]++
	return nil, false
}

// startManager is the runner's startManager for a group with no running
// manager (a process's first start, a restart, the end of an absence): read
// the view, build and restore the manager, and register it. It reports
// whether the start succeeded; a failed one leaves startPending set.
func (pl *plannedHarness) startManager() bool {
	view, ok := pl.startView(false)
	if !ok {
		pl.startPending = true
		return false
	}
	tm := pl.r.newManager(view)
	pl.r.tm = tm
	pl.reg.register(tm, pl.wiringFrom(view))
	pl.builtSinceRestart = true
	pl.startPending = false
	return true
}

// retryStart is the reconcile retrying a pending start (no lock held).
func (pl *plannedHarness) retryStart(poll int) {
	if !pl.startPending || pl.r.cluster.OperatorDead() || pl.reg.absent() {
		return
	}
	pl.counts["managerStartRetries"]++
	if !pl.startManager() {
		return
	}
	if pl.restartCheckPending {
		pl.restartCheckPending = false
		pl.r.checkRehydratedAntiFlap(poll)
	}
}

// restoreManager is startManager's restore for a new manager: the failover
// record, watermark and observed authority from the reconcile's (cached)
// object (production restoreFailoverRecordFrom), then the planned-failover
// restore from the start's uncached view (nil: skipped, as
// restorePlannedFailoverState does). It returns the record the manager
// started from.
func (pl *plannedHarness) restoreManager(tm *controller.TopologyManager, rec controller.FailoverStateRecorder,
	view *v1alpha1.MysqlFailoverGroup) controller.FailoverRecord {
	cached := pl.store.Object()
	tm.RestoreFailoverStateForSim(cached, rec)
	tm.RestorePlannedFailoverStateForSim(view)
	eff, _, _ := controller.EffectiveFailoverRecord(cached, pl.r.clk.Now())
	return eff
}

// wiringFrom builds the ManagerWiring from the start's uncached view, as
// registerManagerWith does (nil: nothing to hydrate).
func (pl *plannedHarness) wiringFrom(view *v1alpha1.MysqlFailoverGroup) controller.ManagerWiring {
	var w controller.ManagerWiring
	if view == nil {
		return w
	}
	if controller.PlannedRunNeedsClaim(view, pl.r.clk.Now()) {
		w.HydrateFrom = view.Status.PlannedFailover.DeepCopy()
	}
	w.HoldFrom = plannedHoldOf(view)
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

// applyDueOps applies the actor ops due at poll p and the armed
// acknowledgements.
func (pl *plannedHarness) applyDueOps(p int) {
	for pl.next < len(pl.ops) && pl.ops[pl.next].At <= p {
		pl.applyOp(pl.ops[pl.next], p)
		pl.next++
	}
	pl.tickAcks(p)
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
			// No process runs to reconcile: nothing to replace.
			pl.counts["replaceWhileDead"]++
			return
		}
		if pl.startPending || pl.reg.absent() {
			// No manager runs to replace: the reconcile this op models is
			// one more start attempt, which retryStart or the absence
			// window already accounts for.
			pl.counts["replaceWithoutManager"]++
			return
		}
		pl.mon.noteReplace()
		if !r.lastPromotionAt.IsZero() {
			// Cleared by the next status write that carries the promotion.
			r.replacedBeforeRecord = true
		}
		old := r.tm
		view, _ := pl.startView(true)
		tm := r.newManager(view)
		var inFlight func() bool
		if old != nil {
			inFlight = old.OrderedUpdateClaimedForSim
		}
		if !pl.reg.replace(tm, pl.wiringFrom(view), op.N, inFlight) {
			pl.unsupported[op.Kind]++
		}
		pl.builtSinceRestart = true
		r.tm = tm
		pl.counts["managersReplaced"]++
	case OpManagerAbsent, OpManagerAbsentLong:
		var inFlight func() bool
		if cur := pl.reg.Current(); cur != nil {
			inFlight = cur.OrderedUpdateClaimedForSim
		}
		if op.Kind == OpManagerAbsentLong {
			pl.counts["longManagerAbsences"]++
			pl.reg.setAbsentLong(op.N, inFlight)
		} else {
			pl.reg.setAbsent(op.N, inFlight)
		}
		// The window's end starts a fresh manager (endPoll).
		pl.startPending = true
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
	case OpPFStatusDeniedLong:
		pl.mon.noteFault()
		pl.counts["longStatusDenials"]++
		pl.store.DenyStatusLong(op.N)
	case OpPFMetaFault:
		pl.mon.noteFault()
		pl.store.MetaFaults(op.N, op.K)
	case OpPFAckHold:
		pl.acks = append(pl.acks, &ackArm{delay: op.K, padded: op.N == 1, holdSeenAt: -1})
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

// replayOOB replays an older out-of-band record twice, and checks that
// neither moves the stored record backwards (K037): through the production
// recorder (a stale caller's read-compare-patch, which must refuse to
// overwrite the newer record, §5.3), and as the late delivery of that
// record's original patch, whose resourceVersion precondition must make
// the server refuse it.
func (pl *plannedHarness) replayOOB() {
	if len(pl.oobRecs) < 2 {
		pl.unsupported[OpOOBLateWrite]++
		return
	}
	ctx := WithActor(context.Background(), ActorTM)
	old := pl.oobRecs[len(pl.oobRecs)-2]
	before, _ := pl.storedOOB()
	_ = pl.recorder.RecordFailoverState(ctx, old)
	pl.counts["oobReplayed"]++
	pl.checkOOBNotRegressed(before, "a stale recorder call")

	if n := len(pl.oobClient.delivered); n >= 2 {
		before, _ = pl.storedOOB()
		obj := pl.store.Object()
		if obj == nil {
			return
		}
		err := pl.oobClient.Patch(ctx, obj, client.RawPatch(types.MergePatchType, pl.oobClient.delivered[n-2]))
		pl.counts["oobLateDelivered"]++
		if apierrors.IsConflict(err) {
			pl.counts["oobLateRefused"]++
		}
		pl.checkOOBNotRegressed(before, "a late patch delivery")
	}
}

// checkOOBNotRegressed is OOBRecordRegressed: the stored out-of-band record
// never moves to an older failover, nor changes target without a newer one.
func (pl *plannedHarness) checkOOBNotRegressed(before controller.FailoverRecord, how string) {
	if before.IsZero() {
		return
	}
	after, ok := pl.storedOOB()
	regressed := !ok || after.LastFailover.Before(before.LastFailover) ||
		(!after.LastFailover.After(before.LastFailover) && after.LastFailoverTarget != before.LastFailoverTarget)
	if regressed {
		pl.mon.violate("OOBRecordRegressed", fmt.Sprintf("p%d", pl.r.cluster.Poll()), fmt.Sprintf(
			"%s moved the out-of-band record from %s@%s to %s@%s", how,
			before.LastFailoverTarget, before.LastFailover.Format(time.RFC3339),
			after.LastFailoverTarget, after.LastFailover.Format(time.RFC3339)))
	}
}

// onMutation feeds the monitor and arms the restart-promoted-target fault
// (c.mu held: no cluster calls here).
func (pl *plannedHarness) onMutation(e Event, before, after SiteTruth) {
	pl.mon.onMutation(e, before, after)
	if a := pl.restart; a != nil && a.fired && !a.rewritten && e.Site == a.target && effectiveActor(e.Actor) == ActorPF &&
		e.Kind == EvSetRO && e.Detail == "on=false" && after.Writable() {
		if run := pl.mon.activeRun(); run != nil && run.id == a.runID {
			// The run made its restarted target writable again: the
			// WasWritable stage's re-promotion (§7.1).
			a.rewritten = true
		}
	}
	if pl.restart == nil || pl.restart.target != "" || effectiveActor(e.Actor) != ActorPF {
		return
	}
	if e.Kind == EvSetRO && e.Detail == "on=false" {
		if run := pl.mon.activeRun(); run != nil && run.target == e.Site {
			pl.restart.target = e.Site
			pl.restart.runID = run.id
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
	pl.fireRestart()
}

// fireRestart fires the armed restart: an application write lands on the
// still-writable target, then it restarts.
func (pl *plannedHarness) fireRestart() {
	a := pl.restart
	a.fired = true
	pl.counts["promotedTargetRestarted"]++
	if pl.r.cluster.AppWrite(a.target) {
		pl.counts["promotedTargetRestartedWithWrites"]++
	}
	pl.r.cluster.RestartSite(a.target)
}

// noteCommit counts the terminal markers that end a run by a status stall,
// split by deadline: a marker landing at least the 2m stall deadline after
// the run's durable stall-since time is a stallMarker2m, otherwise (the 10s
// released-run deadline) a stallMarkerReleased.
func (pl *plannedHarness) noteCommit(before, after *v1alpha1.MysqlFailoverGroup) {
	key := v1alpha1.PlannedFailoverTerminalAnnotation
	v := after.GetAnnotations()[key]
	if v == "" || v == before.GetAnnotations()[key] {
		return
	}
	m, ok := controller.ParseTerminalMarker(v)
	if !ok {
		return
	}
	id, since, ok := parseDeadlineStart(before.GetAnnotations()[v1alpha1.PlannedFailoverStallSinceAnnotation])
	if !ok || id != m.RunID {
		return
	}
	if pl.r.clk.Now().Sub(since) >= plannedStatusStallDeadline {
		pl.counts["stallMarker2m"]++
		if m.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
			pl.counts["stallMarker2mNotSucceeded"]++
		}
	} else {
		pl.counts["stallMarkerReleased"]++
	}
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
	for _, a := range endedOn {
		pl.reg.stash(a)
	}
	if cur := pl.reg.Current(); cur != nil {
		for _, a := range endedOn {
			controller.CarryRetiredFailoverRecord(cur, a)
		}
	}
	for _, a := range pl.updates {
		if a.tm != pl.r.tm && a.claimed() {
			return
		}
	}
	// The registered manager; inside an absence window there is none, and
	// the fresh one started at its end is wired from the retiring list.
	if cur := pl.reg.Current(); cur != nil {
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
	pl.retryStart(pl.r.cluster.Poll())
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

// pollCurrent runs this tick's Poll of the current manager, once, as the
// tm actor's top-level step.
func (pl *plannedHarness) pollCurrent(ctx context.Context) {
	pl.sched.run("tm", func() { pl.pollCurrentBody(ctx) })
}

// pollCurrentBody is the tm actor's step: the Poll itself, with the actor
// already on the scheduler's stack (top level or a yield).
func (pl *plannedHarness) pollCurrentBody(ctx context.Context) {
	if pl.tmPolled || pl.r.cluster.OperatorDead() {
		return
	}
	pl.tmPolled = true
	if tm := pl.reg.Current(); tm != nil {
		pl.counts["tmPolls"]++
		if pl.sched.depth > 0 {
			pl.counts["tmPollsAtYield"]++
		}
		tm.Poll(WithActor(ctx, ActorTM))
	}
}

// pollRetiring runs this tick's Polls of the retiring managers, once, as
// the retiring actor's top-level step.
func (pl *plannedHarness) pollRetiring(ctx context.Context) {
	pl.sched.run("retiring", func() { pl.pollRetiringBody(ctx) })
}

// pollRetiringBody is the retiring actor's step (see pollCurrentBody).
func (pl *plannedHarness) pollRetiringBody(ctx context.Context) {
	if pl.retiringPolled || pl.r.cluster.OperatorDead() {
		return
	}
	pl.retiringPolled = true
	for _, old := range pl.reg.retiringToPoll() {
		pl.counts["retiringPolls"]++
		if pl.sched.depth > 0 {
			pl.counts["retiringPollsAtYield"]++
		}
		old.Poll(WithActor(ctx, ActorTM))
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

// stepEngine runs one engine pass and schedules the next. The engine logs
// through controller-runtime's log.FromContext; the pass's context carries
// a logger over the trial's handler, so a repro captures those lines with
// the topology manager's (K176).
func (pl *plannedHarness) stepEngine(ctx context.Context) {
	pl.lastEngineRV = pl.store.ResourceVersion()
	ctx = crlog.IntoContext(ctx, logr.FromSlogHandler(pl.r.logger.Handler()))
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
		pl.fireRestart()
	}
	pl.mon.endPoll(p, pl.reg.Current() != nil && pl.builtSinceRestart)
	pl.reg.endPoll()
}

// noManager reports whether the group has no registered manager now: the
// operator's status endpoints answer 503 then (runner.Status not found).
func (pl *plannedHarness) noManager() bool { return pl.reg.Current() == nil }

// healAll clears the actor faults at HealAt; a long outage (K036) runs its
// course, and the trial's settle window starts after it.
func (pl *plannedHarness) healAll() {
	pl.store.ClearFaults()
	if pl.restart != nil && !pl.restart.fired {
		pl.restart = nil // an armed restart must not fire in the settle window
	}
	pl.reg.healAbsence()
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
	// The restarted target's own run (K028): how it ended.
	if a := pl.restart; a != nil && a.fired && a.runID != "" {
		if run := pl.mon.runs[a.runID]; run != nil && run.terminal {
			counts["restartedRun"+string(run.outcome)]++
			if a.rewritten && run.outcome == v1alpha1.PlannedFailoverOutcomeSucceeded {
				counts["restartedRunRewrittenSucceeded"]++
			}
		}
	}
	steps := map[string]int{}
	for k, v := range pl.sched.steps {
		steps[k] = v
	}
	// A settlement that ended Unknown followed by a late commit of the
	// trial (K041).
	unknown := false
	for _, op := range pl.store.Log() {
		switch {
		case op.Op == "settle" && op.Outcome == "unknown":
			unknown = true
		case unknown && op.Op == "lateCommit" && op.Outcome == "committed":
			counts["lateCommitAfterUnknown"]++
			unknown = false
		}
	}
	stepsBy := map[string]int{}
	for k, v := range pl.sched.stepsBy {
		stepsBy[k] = v
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
		StepsBy:     stepsBy,
		Stats:       pl.mon.stats,
		Unsupported: unsupported,
		Counts:      counts,
		Outcomes:    outcomes,
		SinkEvents:  append([]string(nil), pl.sink.events...),
		Terminals:   append([]v1alpha1.PlannedFailoverStatus(nil), pl.sink.terminals...),
	}
}

// PlannedResult is what a trial with actors reports beyond TrialResult.
type PlannedResult struct {
	Object   *v1alpha1.MysqlFailoverGroup
	StoreLog []storeOp
	Yields   map[string]int
	// Steps counts, per yield class, the yields at which another actor
	// stepped.
	Steps map[string]int
	// StepsBy counts, per actor, the steps it took at a yield.
	StepsBy     map[string]int
	Stats       plannedStats
	Unsupported map[OpKind]int
	// Counts are harness counters: enginePasses, updatesStarted,
	// updatesRefused, managersReplaced, tmPolls, tmPollsAtYield,
	// retiringPolls, retiringPollsAtYield, oobReplayed,
	// promotedTargetRestarted.
	Counts map[string]int
	// Outcomes are the terminal outcomes of the trial's runs, in run order.
	Outcomes   []string
	SinkEvents []string
	// Terminals are the terminal records the engine reported (RunTerminal).
	Terminals []v1alpha1.PlannedFailoverStatus
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

// stalledClaimSlack is how long past the status-stall deadline the stall
// marker may take to land and release the claim (a pass or two).
const stalledClaimSlack = 15 * time.Second

// plannedStatusStallDeadline mirrors the engine's 2m status-stall deadline
// (plannedFailoverStatusStallDeadline, §6.4).
const plannedStatusStallDeadline = 2 * time.Minute

// stalledClaimWithinDeadline reports whether the live slot holds a run's
// claim while that run's status writes are stalled (its durable
// stall-since names it) and the stall deadline, plus slack, has not passed.
func (pl *plannedHarness) stalledClaimWithinDeadline() bool {
	id, hold, ok := pl.reg.Slot(pl.nn).HeldForSim()
	if !ok || hold {
		return false
	}
	obj := pl.store.Object()
	if obj == nil {
		return false
	}
	run, since, ok := parseDeadlineStart(obj.GetAnnotations()[v1alpha1.PlannedFailoverStallSinceAnnotation])
	if !ok || run != id {
		return false
	}
	return pl.r.clk.Now().Sub(since) <= plannedStatusStallDeadline+stalledClaimSlack
}

// plannedRollbackDeadline mirrors the engine's 30s rollback deadline
// (plannedFailoverRollbackDeadline, §9).
const plannedRollbackDeadline = 30 * time.Second

// rollbackClaimWithinDeadline reports whether the live slot holds a run
// whose durable block reads RollingBack and whose rollback deadline, plus
// slack, has not passed. Until then the rollback may keep the claim while
// it retries a step it cannot verify (a target it cannot read back at
// super_read_only=ON), and the claim defers the manager's cross-site
// actions as in every other phase.
func (pl *plannedHarness) rollbackClaimWithinDeadline() bool {
	id, hold, ok := pl.reg.Slot(pl.nn).HeldForSim()
	if !ok || hold {
		return false
	}
	obj := pl.store.Object()
	if obj == nil || obj.Status.PlannedFailover == nil {
		return false
	}
	pf := obj.Status.PlannedFailover
	if pf.RunID != id || pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback == nil {
		return false
	}
	return pl.r.clk.Now().Sub(pf.Rollback.IntentTime.Time) <= plannedRollbackDeadline+stalledClaimSlack
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
