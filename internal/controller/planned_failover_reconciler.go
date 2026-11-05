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
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

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

// plannedFailoverInFlight is the status-derived guard of the old state
// machine. Nothing in the planned-failover engine or its status consumers
// calls it; it is removed together with the runner's guard.
func plannedFailoverInFlight(s *v1alpha1.PlannedFailoverStatus) bool {
	if s == nil {
		return false
	}
	switch s.Phase {
	case v1alpha1.PlannedFailoverPhasePending,
		v1alpha1.PlannedFailoverPhaseValidating,
		v1alpha1.PlannedFailoverPhaseDraining,
		v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhaseResuming:
		return true
	}
	return false
}

// reconcilePlannedFailover runs one engine pass for the group when one is
// due. A positive duration asks for a requeue.
func (r *MysqlFailoverGroupReconciler) reconcilePlannedFailover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	e := r.plannedFailoverEngine()
	if !plannedFailoverPassDue(fg) && !e.topo.PlannedClaimHeld(nn) {
		return 0, nil
	}
	return e.Step(ctx, nn)
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
	a := fg.GetAnnotations()
	for _, k := range []string{PlannedFailoverAnnotation, AckPlannedFailoverHoldAnnotation, PlannedFailoverTerminalAnnotation,
		PlannedFailoverStallSinceAnnotation, PlannedFailoverManagerAbsentSinceAnnotation} {
		if _, ok := a[k]; ok {
			return true
		}
	}
	if pf := fg.Status.PlannedFailover; pf != nil && !plannedFailoverTerminal(pf) {
		return true
	}
	c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld)
	return c != nil && c.Status == "True"
}

// plannedClaimHeld reports whether the group's planned-failover claim slot
// is held (a run, a provisional grant, or a hold).
func (r *MysqlFailoverGroupReconciler) plannedClaimHeld(nn types.NamespacedName) bool {
	return r.Runner != nil && r.Runner.PlannedClaimHeld(nn)
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
			topo = newRunnerPlannedTopology(r.Runner)
		}
		e := NewPlannedFailoverEngine(newAPIPlannedFailoverStore(r.Client, r.APIReader, clk), topo,
			reconcilerPlannedSink{r: r}, clk, SimHooks{})
		e.dragonfly = reconcilerDragonflyOps{r: r}
		e.admission = leaseAdmissionGuard{r: r}
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
	metrics.PlannedFailoversTotal.WithLabelValues(pf.Target, mapOutcomeToMetricResult(pf.Result)).Inc()
	if pf.StartTime != nil && pf.Result != nil {
		if d := pf.Result.CompletionTime.Sub(pf.StartTime.Time); d > 0 {
			metrics.PlannedFailoverDurationSeconds.WithLabelValues(pf.Target).Observe(d.Seconds())
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

// newRunnerPlannedTopology builds the production topology facade over the
// runner. Until the facade over the runner's claim slot is wired in, it is
// pendingRunnerPlannedTopology: the runner's claim stubs, and errManagerAbsent
// for every MySQL operation, so a run waits in admission and nothing is sent.
var newRunnerPlannedTopology = func(r *TopologyManagerRunner) PlannedFailoverTopology {
	return pendingRunnerPlannedTopology{r: r}
}

// pendingRunnerPlannedTopology is the facade placeholder described at
// newRunnerPlannedTopology. It sends nothing.
type pendingRunnerPlannedTopology struct {
	r *TopologyManagerRunner
}

var _ PlannedFailoverTopology = pendingRunnerPlannedTopology{}

func (t pendingRunnerPlannedTopology) ClaimPlannedFailover(ctx context.Context, nn types.NamespacedName, req PlannedClaimRequest) (PlannedClaimDecision, error) {
	if t.r == nil {
		return PlannedClaimDecision{Kind: ClaimWait, WaitReason: v1alpha1.PlannedFailoverWaitManagerAbsent}, nil
	}
	return t.r.ClaimPlannedFailover(ctx, nn, req)
}
func (t pendingRunnerPlannedTopology) EnsureClaim(context.Context, PlannedRunRef, *v1alpha1.MysqlFailoverGroup) (PlannedClaimDecision, error) {
	return PlannedClaimDecision{}, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) ReleasePlannedFailoverClaim(nn types.NamespacedName, runID string, why ClaimReleaseReason) bool {
	return t.r != nil && t.r.ReleasePlannedFailoverClaim(nn, runID, why)
}
func (t pendingRunnerPlannedTopology) ConvertToHold(types.NamespacedName, string) bool { return false }
func (t pendingRunnerPlannedTopology) PlannedClaimHeld(nn types.NamespacedName) bool {
	return t.r != nil && t.r.PlannedClaimHeld(nn)
}
func (t pendingRunnerPlannedTopology) Reprove(context.Context, PlannedRunRef, *v1alpha1.MysqlFailoverGroup) (ReproveResult, error) {
	return ReproveResult{}, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) Fence(context.Context, PlannedRunRef, string) (FenceProof, error) {
	return FenceProof{}, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) ProveFence(context.Context, PlannedRunRef, v1alpha1.PlannedFailoverFenceRecord) (FenceProof, error) {
	return FenceProof{}, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) KillAppConnections(context.Context, PlannedRunRef) (int32, error) {
	return 0, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) ProbeLag(context.Context, PlannedRunRef, string) (LagProbe, error) {
	return LagProbe{}, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) ReadTargetServerUUID(context.Context, PlannedRunRef) (string, error) {
	return "", errManagerAbsent
}
func (t pendingRunnerPlannedTopology) Promote(context.Context, PlannedRunRef, *v1alpha1.PlannedFailoverStatus) (PromoteOutcome, error) {
	return PromoteOutcome{}, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) RecordPlannedPromotion(context.Context, PlannedRunRef, time.Time, string) error {
	return errManagerAbsent
}
func (t pendingRunnerPlannedTopology) ApplyDNS(context.Context, PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
	return "", errManagerAbsent
}
func (t pendingRunnerPlannedTopology) NameUnconfirmedTarget(PlannedRunRef) error {
	return errManagerAbsent
}
func (t pendingRunnerPlannedTopology) NameFencedSource(PlannedRunRef) error { return errManagerAbsent }
func (t pendingRunnerPlannedTopology) Rollback(context.Context, PlannedRunRef, *v1alpha1.PlannedFailoverStatus) (RollbackResult, error) {
	return RollbackResult{}, errManagerAbsent
}
func (t pendingRunnerPlannedTopology) ActiveSite(nn types.NamespacedName) (string, bool) {
	if t.r == nil {
		return "", false
	}
	return t.r.ActiveSite(nn)
}

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
