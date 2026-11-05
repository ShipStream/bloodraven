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
// the yield scheduler and the invariant monitor. It exists only when
// Trial.Actors.Enabled.
//
// The planned failover engine and the ordered-update step actor plug in
// through engineStep and updateStart/updateStep. Until they are wired the
// actor ops that need them are counted as unsupported and otherwise
// skipped, and the trial still exercises the store, the registry, manager
// replacement and absence, pod replacement, the request annotation and the
// existing invariants under all of it.
type plannedHarness struct {
	r     *trialRunner
	nn    types.NamespacedName
	store *simPlannedStore
	reg   *simRegistry
	sched *yieldScheduler
	mon   *plannedMonitor
	sink  *simSink

	ops  []Op
	next int

	// engineStep runs one engine pass and returns the requeue delay; nil
	// until the engine is wired. nextEngine is when the next pass is due on
	// the fake clock.
	engineStep func(ctx context.Context) (time.Duration, error)
	nextEngine time.Time

	// updateStart starts the update actor on tm (false: refused where
	// startUpdate would refuse); updateStep runs one statement and reports
	// whether the actor is still active. nil until the …ForSim hooks land.
	updateStart func(tm *controller.TopologyManager) bool
	updateStep  func(ctx context.Context) bool

	// unsupported counts actor ops skipped because their seam is not wired.
	unsupported map[OpKind]int

	// oob is every accepted out-of-band record write with the
	// resourceVersion its request was conditioned on (OpOOBLateWrite).
	oob []oobWrite

	restart *restartArm

	sidecarNext       int
	builtSinceRestart bool
}

// oobWrite is one accepted out-of-band failover-record write.
type oobWrite struct {
	annotations map[string]string
	baseRV      int64
}

// restartArm is an armed OpRestartPromotedTarget: once the run clears
// read-only on its target, the target restarts k yields later.
type restartArm struct {
	k      int
	target string
	fired  bool
}

func newPlannedHarness(r *trialRunner) *plannedHarness {
	t := r.trial
	nn := types.NamespacedName{Namespace: r.cfg.Namespace, Name: r.cfg.Name}
	if nn.Name == "" {
		nn = types.NamespacedName{Namespace: "sim", Name: "sim"}
	}
	obj := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: nn.Namespace, Name: nn.Name}}
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
		r:           r,
		nn:          nn,
		store:       newSimPlannedStore(obj, r.clk),
		reg:         newSimRegistry(nn),
		sched:       newYieldScheduler(t.Actors.SchedSeed),
		mon:         newPlannedMonitor(r.cluster, t.SiteNames, t.Roles),
		sink:        &simSink{},
		ops:         append([]Op(nil), t.Actors.ActiveOps()...),
		unsupported: map[OpKind]int{},
	}
	pl.mon.cur = pl.store.Object()
	pl.store.dead = r.cluster.OperatorDead
	pl.store.clusterStatusDenied = r.cluster.StatusDenied
	pl.store.onCommit = pl.mon.onCommit

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
	pl.sched.observe(pl.mon.onYield)
	pl.sched.observe(pl.onYield)
	pl.registerSchedActors()

	// The topology manager logs its planned deferrals; the tap turns them
	// into monitor input (AutomationHeldByPendingRequest).
	r.logger = slog.New(&deferralTap{next: r.logger.Handler(), fn: pl.mon.noteDeferral})
	return pl
}

// registerSchedActors registers the actors a yield may step, in step order.
func (pl *plannedHarness) registerSchedActors() {
	r := pl.r
	pl.sched.addActor(&schedActor{
		name:  "tm",
		pct:   30,
		ready: func() bool { return !r.cluster.OperatorDead() && pl.reg.Current() != nil },
		step: func(ctx context.Context) {
			if tm := pl.reg.Current(); tm != nil {
				tm.Poll(WithActor(ctx, ActorTM))
			}
		},
	})
	pl.sched.addActor(&schedActor{
		name:  "retiring",
		pct:   30,
		ready: func() bool { return !r.cluster.OperatorDead() && pl.reg.retiringCount() > 0 },
		step: func(ctx context.Context) {
			if snap := pl.reg.Snapshot(pl.nn); len(snap.Retiring) > 0 {
				snap.Retiring[0].Poll(WithActor(ctx, ActorTM))
			}
		},
	})
	pl.sched.addActor(&schedActor{
		name:  string(ActorPF),
		pct:   40,
		ready: func() bool { return !r.cluster.OperatorDead() && pl.engineStep != nil },
		step:  func(ctx context.Context) { pl.stepEngine(ctx) },
	})
	pl.sched.addActor(&schedActor{
		name:  string(ActorUpdate),
		pct:   40,
		ready: func() bool { return !r.cluster.OperatorDead() && pl.updateStep != nil },
		step:  func(ctx context.Context) { pl.updateStep(WithActor(ctx, ActorUpdate)) },
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
			step: func(context.Context) {
				names := r.trial.SiteNames
				r.cluster.RogueWritable(names[pl.sched.rng.IntN(len(names))])
			},
		})
	}
}

// registerManager makes tm the current manager, wired from the uncached
// object exactly as the runner's startManager wires it.
func (pl *plannedHarness) registerManager(tm *controller.TopologyManager) {
	pl.reg.register(tm, pl.wiring())
	pl.builtSinceRestart = true
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

// plannedHoldOf mirrors the controller's plannedHoldFrom (unexported) for
// the wiring: either durable hold record holds.
func plannedHoldOf(obj *v1alpha1.MysqlFailoverGroup) *controller.PlannedHold {
	if !holdStands(obj) {
		return nil
	}
	h := &controller.PlannedHold{}
	if holdConditionTrue(obj) {
		h.FromCondition = true
		for _, c := range obj.Status.Conditions {
			if c.Type == v1alpha1.ConditionAutomaticFailoverHeld {
				if f := strings.Fields(c.Message); len(f) > 3 && f[0] == "planned" {
					h.RunID = f[3]
				}
			}
		}
	}
	parts := strings.Split(obj.GetAnnotations()[controller.PlannedFailoverTerminalAnnotation], "/")
	if len(parts) == 3 && v1alpha1.PlannedFailoverOutcome(parts[1]) == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		if h.RunID == "" || h.RunID == parts[0] {
			h.RunID = parts[0]
			h.FromMarker = true
			if h.Reason == "" {
				h.Reason = v1alpha1.PlannedFailoverReason(parts[2])
			}
		}
	}
	return h
}

// restartProcess is an operator restart: a new process with a new registry
// (the slot and the engine memo are gone) and leadership re-acquired.
func (pl *plannedHarness) restartProcess() {
	pl.reg.shutdown()
	retire, changing := pl.reg.retire, pl.reg.changingSites
	pl.reg = newSimRegistry(pl.nn)
	pl.reg.retire, pl.reg.changingSites = retire, changing
	pl.builtSinceRestart = false
	pl.mon.noteRestart()
	// The engine is rebuilt by the new process; its first pass runs after
	// the new manager's first Poll (the per-poll order guarantees it).
	pl.nextEngine = time.Time{}
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
		if pl.updateStart == nil || tm == nil {
			pl.unsupported[op.Kind]++
			return
		}
		pl.updateStart(tm)
	case OpReplaceManager:
		pl.mon.noteReplace()
		tm := r.newManager()
		if !pl.reg.replace(tm, pl.wiring(), op.N, nil) {
			pl.unsupported[op.Kind]++ // replaced, but without the in-flight window
		}
		r.tm = tm
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

// recordOOB lands an accepted out-of-band record write on the object.
func (pl *plannedHarness) recordOOB(annotations map[string]string) {
	base := pl.store.ResourceVersion()
	if err := pl.store.PatchAnnotations("failoverRecord", annotations); err != nil {
		return
	}
	pl.oob = append(pl.oob, oobWrite{annotations: annotations, baseRV: base})
}

// replayOOB delivers a late duplicate of the previous out-of-band record
// write. The record is written conditioned on the resourceVersion its
// request read (§5.3), so the API server refuses the duplicate unless
// nothing has moved the object since; the sim applies the same check.
func (pl *plannedHarness) replayOOB() {
	if len(pl.oob) < 2 {
		return
	}
	old := pl.oob[len(pl.oob)-2]
	if pl.store.ResourceVersion() != old.baseRV {
		pl.store.mu.Lock()
		pl.store.logLocked("oobLateWrite", "refused (conditioned)")
		pl.store.mu.Unlock()
		return
	}
	_ = pl.store.PatchAnnotations("oobLateWrite", old.annotations)
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
	pl.r.cluster.RestartSite(a.target)
}

// pollActors is the per-poll actor order: the current manager's Poll, the
// retiring managers', the update actor's step, then the engine pass when
// its requeue is due.
func (pl *plannedHarness) pollActors(ctx context.Context) {
	if tm := pl.reg.Current(); tm != nil {
		pl.sched.run("tm", func() { tm.Poll(WithActor(ctx, ActorTM)) })
	}
	for _, old := range pl.reg.retiringToPoll() {
		pl.sched.run("retiring", func() { old.Poll(WithActor(ctx, ActorTM)) })
	}
	if pl.updateStep != nil && !pl.r.cluster.OperatorDead() {
		pl.sched.run(string(ActorUpdate), func() { pl.updateStep(WithActor(ctx, ActorUpdate)) })
	}
	if pl.engineStep != nil && !pl.r.cluster.OperatorDead() && !pl.r.clk.Now().Before(pl.nextEngine) {
		pl.sched.run(string(ActorPF), func() { pl.stepEngine(ctx) })
	}
}

// stepEngine runs one engine pass and schedules the next.
func (pl *plannedHarness) stepEngine(ctx context.Context) {
	requeue, _ := pl.engineStep(WithActor(ctx, ActorPF))
	pl.nextEngine = pl.r.clk.Now().Add(requeue)
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
		pl.r.cluster.RestartSite(a.target)
	}
	pl.mon.endPoll(p, pl.reg.Current() != nil && pl.builtSinceRestart)
	pl.reg.endPoll()
}

// healAll clears the actor faults at HealAt.
func (pl *plannedHarness) healAll() {
	pl.store.ClearFaults()
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
	return &PlannedResult{
		Object:      pl.store.Object(),
		StoreLog:    pl.store.Log(),
		Yields:      yields,
		Stats:       pl.mon.stats,
		Unsupported: unsupported,
		SinkEvents:  append([]string(nil), pl.sink.events...),
	}
}

// PlannedResult is what a trial with actors reports beyond TrialResult.
type PlannedResult struct {
	Object      *v1alpha1.MysqlFailoverGroup
	StoreLog    []storeOp
	Yields      map[string]int
	Stats       plannedStats
	Unsupported map[OpKind]int
	SinkEvents  []string
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
