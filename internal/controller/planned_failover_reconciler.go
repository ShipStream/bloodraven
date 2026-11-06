// Package controller — the planned-failover reconciler adapter.
//
// reconcilePlannedFailover is invoked from MysqlFailoverGroupReconciler.
// Reconcile. It is a thin adapter: the cached object only decides whether a
// pass is due at all ("cached reads may refuse"); the pass itself is the
// step engine (planned_failover_engine.go), which reads uncached, writes
// conditioned, and acts on MySQL only through the topology facade under
// the group's planned-failover claim.
package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// plannedFailoverTerminal reports whether the given status is in a
// terminal phase (Succeeded or Failed). A terminal marker is not
// considered here; plannedFailoverRun reports it.
func plannedFailoverTerminal(s *v1alpha1.PlannedFailoverStatus) bool {
	if s == nil {
		return false
	}
	return s.Phase == v1alpha1.PlannedFailoverPhaseSucceeded ||
		s.Phase == v1alpha1.PlannedFailoverPhaseFailed
}

// reconcilePlannedFailover runs one engine pass for the group when one is
// due. A positive duration asks for a requeue.
func (r *MysqlFailoverGroupReconciler) reconcilePlannedFailover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if !plannedFailoverPassDue(fg) && !r.plannedClaimHeld(nn) {
		return 0, nil
	}
	return plannedStepResult(ctx, nn)(r.plannedFailoverEngine().Step(ctx, nn))
}

// plannedStepResult keeps the engine's own retry interval (I002). A pass
// that failed transiently (its uncached load, a facade error) and asked for
// a bounded requeue is logged and requeued on that interval; returning the
// error instead would hand the retry to the controller's exponential
// backoff, which can delay a fenced run's deadlines and its stall marker
// far past their budgets. An error with no requeue is returned as before.
func plannedStepResult(ctx context.Context, nn types.NamespacedName) func(time.Duration, error) (time.Duration, error) {
	return func(d time.Duration, err error) (time.Duration, error) {
		if err == nil || d <= 0 {
			return d, err
		}
		log.FromContext(ctx).Error(err, "planned failover: pass failed; retrying on the planned-failover cadence",
			"fg", nn.String(), "requeueAfter", d.String())
		return d, nil
	}
}

// reconcilePlannedFailoverInFlight runs one engine pass for a reconcile
// that returns before its regular planned-failover step: for a run already
// in flight, a hold, a terminal marker, a durable deadline annotation, or a
// held claim, never to admit a new request. ran reports whether a pass ran.
func (r *MysqlFailoverGroupReconciler) reconcilePlannedFailoverInFlight(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, bool, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if !plannedFailoverInFlightDue(fg) && !r.plannedClaimHeld(nn) {
		return 0, false, nil
	}
	d, err := plannedStepResult(ctx, nn)(r.plannedFailoverEngine().step(ctx, nn, false))
	return d, true, err
}

// plannedFailoverPassRuns reports whether reconcilePlannedFailover runs a
// pass for fg.
func (r *MysqlFailoverGroupReconciler) plannedFailoverPassRuns(fg *v1alpha1.MysqlFailoverGroup) bool {
	return plannedFailoverPassDue(fg) || r.plannedClaimHeld(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
}

// plannedFailoverHoldsReconcile reports whether a planned failover holds
// up the rest of the reconcile: a run that owns the sites or is
// Publishing, or a Run-mode claim (a run's, or its provisional grant).
// A run that owns nothing (Admitting or Deferred without a claim) and an
// automation hold do not.
func (r *MysqlFailoverGroupReconciler) plannedFailoverHoldsReconcile(fg *v1alpha1.MysqlFailoverGroup) bool {
	if plannedFailoverOwnsSites(fg) {
		return true
	}
	if pf, marked := plannedFailoverRun(fg); pf != nil && !marked && pf.Phase == v1alpha1.PlannedFailoverPhasePublishing {
		return true
	}
	return r.plannedRunClaimHeld(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
}

// plannedPostPassView returns the group as the planned-failover pass left
// it, read uncached, for the label syncs that follow the pass: the cached
// object still shows the phase before the pass, so the source's role
// label would otherwise follow a newly recorded fence only on a later
// reconcile. fg itself is returned when no pass ran or the read fails.
func (r *MysqlFailoverGroupReconciler) plannedPostPassView(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, ran bool) *v1alpha1.MysqlFailoverGroup {
	if !ran {
		return fg
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var fresh v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil || fresh.UID != fg.UID {
		return fg
	}
	return &fresh
}

// drivePlannedFailoverOnEarlyReturn runs the in-flight planned-failover
// pass for a Reconcile that returned before its regular planned-failover
// step, and merges the pass's requeue into the result. While a run owns
// the sites (or a Run-mode claim is held), an error of the earlier step is
// logged and the reconcile requeues on the run's cadence instead of the
// controller's exponential backoff, which would starve the run's deadlines.
func (r *MysqlFailoverGroupReconciler) drivePlannedFailoverOnEarlyReturn(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	result ctrl.Result, err error) (ctrl.Result, error) {
	pfRequeue, ran, pfErr := r.reconcilePlannedFailoverInFlight(ctx, fg)
	if !ran {
		return result, err
	}
	logger := log.FromContext(ctx)
	if pfErr != nil {
		if err == nil {
			return ctrl.Result{}, fmt.Errorf("reconcile planned failover: %w", pfErr)
		}
		logger.Error(pfErr, "planned failover: pass on an early-returning reconcile failed")
		return result, err
	}
	if err != nil {
		if pfRequeue > 0 && r.plannedFailoverHoldsReconcile(fg) {
			logger.Error(err, "reconcile failed while a planned failover is in flight; retrying on the planned-failover cadence",
				"requeueAfter", pfRequeue.String())
			return ctrl.Result{RequeueAfter: pfRequeue}, nil
		}
		return result, err
	}
	result.RequeueAfter = minPositiveDuration(result.RequeueAfter, pfRequeue)
	return result, nil
}

// StepPlannedFailoverForTest runs one planned-failover engine pass for nn,
// as reconcilePlannedFailover does when a pass is due. Not for production
// use.
func (r *MysqlFailoverGroupReconciler) StepPlannedFailoverForTest(ctx context.Context, nn types.NamespacedName) (time.Duration, error) {
	return r.plannedFailoverEngine().Step(ctx, nn)
}

// plannedFailoverPassDue reports, from the cached object, whether anything
// planned-failover related is pending: a request or acknowledgement
// annotation, a non-terminal block, a terminal marker, a durable deadline
// annotation, or an automation hold. The engine re-reads uncached; the cache
// only lets a quiet group skip the read.
func plannedFailoverPassDue(fg *v1alpha1.MysqlFailoverGroup) bool {
	if _, ok := fg.GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		return true
	}
	return plannedFailoverInFlightDue(fg)
}

// plannedFailoverInFlightDue is plannedFailoverPassDue without a new
// request: an acknowledgement, a non-terminal block, a terminal marker, a
// durable deadline annotation, or an automation hold.
func plannedFailoverInFlightDue(fg *v1alpha1.MysqlFailoverGroup) bool {
	a := fg.GetAnnotations()
	for _, k := range []string{v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, v1alpha1.PlannedFailoverTerminalAnnotation,
		v1alpha1.PlannedFailoverStallSinceAnnotation, v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation, v1alpha1.PlannedFailoverConfirmedAnnotation} {
		if _, ok := a[k]; ok {
			return true
		}
	}
	if pf := fg.Status.PlannedFailover; pf != nil && !plannedFailoverTerminal(pf) {
		return true
	}
	return PlannedHoldFrom(fg) != nil
}

// plannedClaimHeld reports whether the group's planned-failover claim slot
// is held (a run, a provisional grant, or a hold). Every reconcile-side
// claim question goes through the engine's topology facade (K164), the
// runner's facade in production, so an injected facade answers them all.
func (r *MysqlFailoverGroupReconciler) plannedClaimHeld(nn types.NamespacedName) bool {
	return r.plannedTopologyFacade().PlannedClaimHeld(nn)
}

// plannedTopologyFacade is the facade the engine uses (the same one
// plannedFailoverEngine wires in); asking it does not build the engine.
func (r *MysqlFailoverGroupReconciler) plannedTopologyFacade() PlannedFailoverTopology {
	if r.PlannedTopology != nil {
		return r.PlannedTopology
	}
	return productionPlannedTopology(r.Runner)
}

// plannedOwnsDragonfly reports whether a planned failover owns fg's
// Dragonfly for a reconcile-side decision. Once the group's topology
// manager is registered its claim slot is hydrated from an uncached read,
// so the slot alone decides (a run that released its claim after recording
// its promotion owns nothing, whatever status says); before that, or with
// no runner, the status form (plannedFailoverOwnsDragonfly) decides.
func (r *MysqlFailoverGroupReconciler) plannedOwnsDragonfly(fg *v1alpha1.MysqlFailoverGroup) bool {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if r.Runner == nil {
		return plannedFailoverOwnsDragonfly(fg, false)
	}
	held := r.plannedClaimHeld(nn)
	if r.Runner.Snapshot(nn).Current != nil {
		return held
	}
	return plannedFailoverOwnsDragonfly(fg, held)
}

// plannedRunClaimHeld reports whether the group's slot holds a Run-mode
// claim (a run or its provisional grant); a hold does not count.
func (r *MysqlFailoverGroupReconciler) plannedRunClaimHeld(nn types.NamespacedName) bool {
	_, ok := r.plannedTopologyFacade().HeldRunClaim(nn)
	return ok
}

// plannedFailoverEngine returns the reconciler's engine, built on first use.
// The engine is long-lived: its memo (what this process sent) must survive
// between passes.
func (r *MysqlFailoverGroupReconciler) plannedFailoverEngine() *PlannedFailoverEngine {
	r.plannedEngineOnce.Do(func() {
		clk := r.plannedClock
		if clk == nil {
			clk = clock.RealClock{}
		}
		topo := r.PlannedTopology
		if topo == nil {
			topo = productionPlannedTopology(r.Runner)
		}
		e := NewPlannedFailoverEngine(newAPIPlannedFailoverStore(r.Client, r.APIReader, clk), topo,
			reconcilerPlannedSink{r: r}, clk, SimHooks{})
		e.dragonfly = reconcilerDragonflyOps{r: r}
		e.admission = leaseAdmissionGuard{r: r}
		if r.Runner != nil {
			// D-46 on the planned path: when the run's Succeeded write is
			// the one that moved status.activeSite, the topology status
			// writer later sees no change, so the warning fires here (once:
			// from is the persisted pre-write value).
			e.activeSitePersisted = r.Runner.emitPITRArchiveReissueRisk
		}
		r.plannedEngine = e
	})
	return r.plannedEngine
}

// reconcilerPlannedSink reports the engine's events and metrics through the
// reconciler's Recorder and internal/metrics.
type reconcilerPlannedSink struct {
	r *MysqlFailoverGroupReconciler
}

var _ PlannedFailoverSink = reconcilerPlannedSink{}

func (s reconcilerPlannedSink) Event(fg *v1alpha1.MysqlFailoverGroup, eventType, reason, message string) {
	if s.r.Recorder != nil {
		s.r.Recorder.Eventf(fg, eventType, reason, "%s", message)
	}
}

func (s reconcilerPlannedSink) RunTerminal(_ types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) {
	if pf == nil {
		return
	}
	metrics.PlannedFailoversTotal.WithLabelValues(pf.TargetSite, mapOutcomeToMetricResult(pf.Result)).Inc()
	if !pf.StartTime.IsZero() && pf.Result != nil {
		if d := pf.Result.CompletionTime.Sub(pf.StartTime.Time); d > 0 {
			metrics.PlannedFailoverDurationSeconds.WithLabelValues(pf.TargetSite).Observe(d.Seconds())
		}
	}
}

func (s reconcilerPlannedSink) LagWaitObserved(_ types.NamespacedName, target string, d time.Duration) {
	metrics.PlannedFailoverLagWaitSeconds.WithLabelValues(target).Observe(d.Seconds())
}

func (s reconcilerPlannedSink) Deferred(nn types.NamespacedName, reason v1alpha1.PlannedFailoverDeferReason) {
	metrics.PlannedFailoversDeferredTotal.WithLabelValues(nn.Name, string(reason)).Inc()
}

// mapOutcomeToMetricResult maps a run's result to the existing values of
// bloodraven_planned_failovers_total{result}: success, rejected,
// failed_timeout and failed_other.
func mapOutcomeToMetricResult(r *v1alpha1.PlannedFailoverResult) string {
	if r == nil {
		return "failed_other"
	}
	switch r.Reason {
	case v1alpha1.PlannedFailoverReasonLagTimeout, v1alpha1.PlannedFailoverReasonAdmissionTimeout,
		v1alpha1.PlannedFailoverReasonDragonflySyncTimeout:
		return "failed_timeout"
	}
	switch r.Outcome {
	case v1alpha1.PlannedFailoverOutcomeSucceeded:
		return "success"
	case v1alpha1.PlannedFailoverOutcomeNotStarted:
		return "rejected"
	}
	return "failed_other"
}

// productionPlannedTopology is the facade the reconciler's engine uses
// when none is injected: the runner's process-wide facade, so the memos of
// what this process sent survive between passes. Without a runner (tests
// that build a bare reconciler) it is a facade over a registry with no
// manager, which sends nothing: every run waits in admission.
func productionPlannedTopology(r *TopologyManagerRunner) PlannedFailoverTopology {
	if r == nil {
		return NewPlannedFailoverTopology(&noManagerRegistry{})
	}
	return r.PlannedTopology()
}

// noManagerRegistry is a ManagerRegistry with no topology manager.
type noManagerRegistry struct {
	mu    sync.Mutex
	slots map[types.NamespacedName]*PlannedClaimSlot
}

func (n *noManagerRegistry) Snapshot(nn types.NamespacedName) RegistrySnapshot {
	return RegistrySnapshot{Slot: n.Slot(nn)}
}

func (n *noManagerRegistry) Slot(nn types.NamespacedName) *PlannedClaimSlot {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.slots == nil {
		n.slots = make(map[types.NamespacedName]*PlannedClaimSlot)
	}
	if n.slots[nn] == nil {
		n.slots[nn] = &PlannedClaimSlot{}
	}
	return n.slots[nn]
}

func (n *noManagerRegistry) LeaderContext() context.Context { return context.Background() }

// truncateGtidHint returns a short prefix of a GTID set suitable for
// status.message. Keeping it short avoids flooding the event / status
// with ~thousands of interval boundaries.
func truncateGtidHint(s string) string {
	const hintLen = 48
	if len(s) <= hintLen {
		return s
	}
	return s[:hintLen] + "…"
}

// truncateDur rounds a duration to the nearest 100ms for concise
// human-readable display in messages and events.
func truncateDur(d time.Duration) time.Duration {
	return d.Round(100 * time.Millisecond)
}
