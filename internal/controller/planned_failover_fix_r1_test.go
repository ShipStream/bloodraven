package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/state"
)

// Regression tests for the #235 fix round 1 (review findings G-*, O-* and
// the DST rulings).

// O-M1: a standing automation hold contributes a requeue but does not
// short-circuit the rest of Reconcile: backup schedules are still
// reconciled while the hold waits for its acknowledgement.
func TestReconcile_StandingHoldStillReconcilesBackups(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := newTestFG()
	fg.Spec.Backup = fgWithBackup().Spec.Backup
	now := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		RunID: "run-1", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseFailed, TargetSite: "pdx", SourceSite: "iad", StartTime: now,
		Result: &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
			Reason: v1alpha1.PlannedFailoverReasonForeignWritable, CompletionTime: now},
	}
	setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		SourceSite: "iad", TargetSite: "pdx", AmbiguousSites: []string{"pdx", "iad"}, HoldTime: now}, fg.Generation)

	r, c := newReconciler(fg)
	r.Recorder = record.NewFakeRecorder(64)
	topo := newPFFakeTopology()
	topo.setHeld("run-1", true)
	r.PlannedTopology = topo
	r.plannedClock = clock.NewFakeClock(time.Now())
	// The runner's slot holds the hold too, so the "any claim held" check
	// the early return used before the fix would see it.
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	runner := &TopologyManagerRunner{logger: slog.Default()}
	runner.Slot(nn).toHold("run-1")
	r.Runner = runner
	if !r.plannedClaimHeld(nn) {
		t.Fatal("setup: the runner's slot does not report the hold")
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("a standing hold should still ask for a requeue, got %+v", res)
	}
	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: scheduleCronJobName("lion", "nightly")}, &cj); err != nil {
		t.Fatalf("backup schedule not reconciled while a hold stands: %v", err)
	}
	if run, hold := topo.held(); run != "run-1" || !hold {
		t.Errorf("hold lost: run=%q hold=%v", run, hold)
	}
}

// ---------- shared helpers ----------

// fixR1LogBuffer is a goroutine-safe log sink (the facade probes sites
// concurrently).
type fixR1LogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *fixR1LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *fixR1LogBuffer) count(msg string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), "msg=\""+msg+"\"")
}

// fixR1SlowLoad delays every uncached load (G-M6).
type fixR1SlowLoad struct {
	PlannedFailoverStore
	delay time.Duration
}

func (s fixR1SlowLoad) Load(ctx context.Context, nn types.NamespacedName) (*v1alpha1.MysqlFailoverGroup, error) {
	time.Sleep(s.delay)
	return s.PlannedFailoverStore.Load(ctx, nn)
}

// fixR1Admission refuses every admission with err (O-notes).
type fixR1Admission struct{ err error }

func (a fixR1Admission) BeginPlanned(context.Context, *v1alpha1.MysqlFailoverGroup) (*DeploymentLeaseView, func(), error) {
	return nil, func() {}, a.err
}
func (a fixR1Admission) Now() time.Time { return time.Now() }

// ---------- G-B1 ----------

// A confirmed run whose Publishing write stalls is ended by the terminal
// marker only once its failover record (watermark included) is durable,
// and the terminal status rebuilt from the marker carries confirmedAt and
// applies the record.
func TestFixR1_ConfirmedStallMarksOnlyDurableRecord(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	confirmedAt := h.clk.Now()
	h.step() // Confirmed
	h.faults.statusDenied = true
	h.step() // record, DNS, release, Publishing denied: the stall starts
	h.clk.Advance(plannedFailoverStatusStallDeadline + time.Second)
	for range 3 {
		h.step()
	}
	if m := h.fg().GetAnnotations()[PlannedFailoverTerminalAnnotation]; m != "" {
		t.Fatalf("the stall ended a confirmed run with marker %q while its failover record was not durable", m)
	}
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Fatalf("block = %+v", pf)
	}
	// The out-of-band record lands (recordFailover's flush).
	h.setAnnotation(LastFailoverAnnotation, failoverStampFormat(confirmedAt))
	h.setAnnotation(LastFailoverTargetAnnotation, "pdx")
	h.setAnnotation(PromotionGtidExecutedAnnotation, "uuid-iad:1-10")
	h.setAnnotation(PromotionGtidExecutedForAnnotation, failoverWatermarkBinding(confirmedAt, "pdx"))
	h.step()
	if m := h.fg().GetAnnotations()[PlannedFailoverTerminalAnnotation]; m != `{"schemaVersion":1,"runID":"run-1","outcome":"Succeeded","warnings":["StatusStalled"]}` {
		t.Fatalf("marker = %q, want run-1/Succeeded/ once the record is durable", m)
	}
	// A new process recovers the run from the marker and applies its record.
	h.restart()
	h.faults.statusDenied = false
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	fg := h.fg()
	pf := fg.Status.PlannedFailover
	if pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded || pf.Promotion == nil || pf.Promotion.ConfirmationTime == nil {
		t.Fatalf("terminal block = %+v", pf)
	}
	if fg.Status.LastFailoverTarget != "pdx" || fg.Status.PromotionGtidExecuted != "uuid-iad:1-10" || fg.Status.LastFailover == nil {
		t.Fatalf("status record = %v %q %q; the marker-ended Succeeded record must apply the failover record",
			fg.Status.LastFailover, fg.Status.LastFailoverTarget, fg.Status.PromotionGtidExecuted)
	}
}

// ---------- DST-4 ----------

// The confirmed tail releases the claim right after the record and DNS:
// a stalled Publishing write holds nothing, and later passes do not
// hydrate the claim back from the Promoting block.
func TestFixR1_ConfirmedTailReleasesBeforePublishing(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.faults.statusDenied = true
	h.step() // Confirmed
	h.step() // tail
	if got := h.topo.releasesCopy(); len(got) != 1 || got[0] != "run-1:publishing" {
		t.Fatalf("releases = %v, want run-1:publishing before Publishing commits", got)
	}
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Fatalf("setup: the Publishing write committed: %+v", pf)
	}
	ensures, records := h.topo.count("ensure"), h.topo.count("record")
	for range 3 {
		h.step()
	}
	if run, _ := h.topo.held(); run != "" {
		t.Fatalf("the claim came back (%q) while only the Publishing write was left", run)
	}
	if h.topo.count("ensure") != ensures || h.topo.count("record") != records {
		t.Fatalf("released passes re-claimed or re-recorded: ensure %d→%d record %d→%d", ensures, h.topo.count("ensure"),
			records, h.topo.count("record"))
	}
	h.faults.statusDenied = false
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	if r := h.pf().Result; r.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("result = %+v", r)
	}
}

// ---------- G-M6 ----------

// The pass clock starts before the uncached load: a slow load leaves the
// Promoting step only the rest of its 30s budget.
func TestFixR1_PassClockStartsBeforeLoad(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	const delay = 300 * time.Millisecond
	h.e.store = fixR1SlowLoad{PlannedFailoverStore: h.e.store, delay: delay}
	var deadline time.Time
	h.topo.onCtx = func(op string, ctx context.Context) {
		if op == "promote" {
			deadline, _ = ctx.Deadline()
		}
	}
	start := time.Now()
	h.step()
	if deadline.IsZero() {
		t.Fatal("Promote was not called")
	}
	if got := deadline.Sub(start); got > plannedPassPromoting+delay/3 {
		t.Fatalf("Promote's deadline is %s after the pass began, want at most the %s pass bound", got, plannedPassPromoting)
	}
}

// ---------- O-notes ----------

// The confirmed tail re-runs §4.4 check 1: a failover recorded over the
// run's baseline (not the run's own) is never recorded over.
func TestFixR1_ConfirmedTailRechecksDurableRecord(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	fg := h.fg()
	at := metav1.NewTime(h.clk.Now().Add(-time.Second))
	fg.Status.LastFailover, fg.Status.LastFailoverTarget = &at, "iad"
	if err := h.c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	h.stepUntil(6, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	if r := h.pf().Result; r.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired || r.Reason != v1alpha1.PlannedFailoverReasonAuthorityChanged {
		t.Fatalf("result = %+v, want ManualRecoveryRequired{AuthorityChanged}", r)
	}
	if n := h.topo.count("record"); n != 0 {
		t.Fatalf("RecordPlannedPromotion ran %d times over a changed record", n)
	}
}

// A deployment-lease refusal at admission is an admission wait reason,
// never a reconcile error with controller backoff.
func TestFixR1_AdmissionLeaseRefusalIsAWait(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
	h.e.admission = fixR1Admission{err: &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: "TopologyPersistencePending"}}
	if _, err := h.e.Step(context.Background(), h.nn); err != nil {
		t.Fatalf("Step returned %v; a lease refusal must be a wait", err)
	}
	if a := h.pf().Admission; a == nil || a.WaitReason != v1alpha1.PlannedFailoverWaitAuthorityUnsettled {
		t.Fatalf("admission = %+v, want waitReason AuthorityUnsettled", a)
	}
}

// ---------- O-m6 ----------

// A Fencing run whose only fence attempts sent nothing (an unreadable peer)
// and whose status then stalls ends RolledBack{StatusStalled}: the facade's
// memo, not "Fence was called", decides that a SET was sent.
func TestFixR1_FencingStallWithNoSETRollsBack(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
	h.topo.fence = func(PlannedRunRef) (FenceProof, error) {
		return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonPeerUnreadable, UnreadablePeers: []string{"dr"}}, nil
	}
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover.Phase == v1alpha1.PlannedFailoverPhaseFencing
	})
	h.faults.statusDenied = true
	for i := 0; i < 400 && h.fg().GetAnnotations()[PlannedFailoverTerminalAnnotation] == ""; i++ {
		h.step()
		h.clk.Advance(time.Second)
	}
	if m := h.fg().GetAnnotations()[PlannedFailoverTerminalAnnotation]; m != `{"schemaVersion":1,"runID":"run-1","outcome":"RolledBack","reason":"StatusStalled","warnings":["StatusStalled","UnreachablePeers"]}` {
		t.Fatalf("marker = %q, want run-1/RolledBack/StatusStalled (nothing was sent)", m)
	}
}

// A WasWritable gate refusal does not claim that nothing was written.
func TestFixR1_WasWritableRefusalMessageIsHonest(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
		return PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonPeerUnreadable,
			Terminal: v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, TargetReachable: true, MayHaveWritten: true}, nil
	}
	h.stepUntil(4, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	if msg := h.pf().Message; strings.Contains(msg, "nothing was written") {
		t.Fatalf("message %q claims nothing was written after a WasWritable refusal", msg)
	}
}

// Warnings survive a restart in Publishing: the Publishing record carries
// them (promotion.warnings) and a failed DNS record implies DNSUpdateFailed.
func TestFixR1_PublishingRestartKeepsWarnings(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
		return PromoteOutcome{Kind: PromoteConfirmed, TargetReachable: true, MayHaveWritten: true,
			Warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningSourceReplaced}}, nil
	}
	h.topo.dns = func(PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
		return v1alpha1.PlannedFailoverDNSFailed, errors.New("dns down")
	}
	h.stepUntil(4, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover.Phase == v1alpha1.PlannedFailoverPhasePublishing
	})
	if w := h.pf().Promotion.Warnings; !slices.Contains(w, v1alpha1.PlannedFailoverWarningSourceReplaced) {
		t.Fatalf("Publishing promotion.warnings = %v", w)
	}
	h.restart()
	h.stepUntil(4, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	w := h.pf().Result.Warnings
	if !slices.Contains(w, v1alpha1.PlannedFailoverWarningSourceReplaced) || !slices.Contains(w, v1alpha1.PlannedFailoverWarningDNSUpdateFailed) {
		t.Fatalf("Succeeded after a restart in Publishing has warnings %v, want SourceReplaced and DNSUpdateFailed", w)
	}
}

// ---------- O-M3 (engine) ----------

// The facade's fence memo makes the run's fence attributable (proof.Sent
// sets setSent). A later FenceNotSent, though, means the source no longer
// holds that fence (boot read-only after a restart, or a replaced server:
// the facade attributes a standing super_read_only before it reports
// FenceNotSent), so since the ultra review's K143 such an end carries
// FenceNotAttributable and names nothing, in-process as after a restart.
func TestFixR1_FacadeFenceMemoIsFenceSent(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseFencing)
	calls := 0
	h.topo.fence = func(PlannedRunRef) (FenceProof, error) {
		calls++
		if calls == 1 {
			return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonFenceFailed, SourceServerUUID: "uuid-iad", Sent: true}, nil
		}
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, Sent: true}, nil
	}
	h.topo.prove = func(PlannedRunRef) (FenceProof, error) {
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, Sent: true}, nil
	}
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	if !h.e.runMemo("run-1").fence.setSent {
		t.Fatal("proof.Sent did not reach the engine's memo")
	}
	if slices.Contains(h.topo.named, "source:iad") {
		t.Fatalf("named = %v; a source that no longer holds the run's fence is not named", h.topo.named)
	}
	if r := h.pf().Result; !slices.Contains(r.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) {
		t.Fatalf("result = %+v, want FenceNotAttributable", r)
	}
}

// ---------- O-m2 (engine) ----------

// One failed source read in the re-proof is a loss-window sample, not a
// lost source.
func TestFixR1_ReproveSourceBlipUsesLossWindow(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.topo.needsProof = true
	h.topo.reprove = func(PlannedRunRef) (ReproveResult, error) {
		h.topo.mu.Lock()
		h.topo.needsProof = true
		h.topo.mu.Unlock()
		return ReproveResult{SourceUnreachable: true}, nil
	}
	h.topo.probe = func(PlannedRunRef) (LagProbe, error) {
		return LagProbe{SourceReachable: true, TargetReachable: true}, nil
	}
	h.step()
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForLag {
		t.Fatalf("one failed source read moved the run on: %+v", pf)
	}
	h.clk.Advance(plannedFailoverLossWindow + time.Second)
	h.step()
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonSourceLost {
		t.Fatalf("after the loss window: %+v, want RollingBack{SourceLost}", pf)
	}
}

// ---------- G-M5 (engine) ----------

// A run whose source or target left the topology ends at once, safely and
// released, instead of retrying a re-proof that can never pass.
func TestFixR1_RunSiteRemovedEndsTerminally(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.topo.needsProof = true
	h.topo.reprove = func(PlannedRunRef) (ReproveResult, error) { return ReproveResult{}, errSiteNotFound }
	h.stepUntil(3, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	if r := h.pf().Result; r.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced || r.Reason != v1alpha1.PlannedFailoverReasonUnknownSite {
		t.Fatalf("result = %+v, want SourceLeftFenced{UnknownSite}", r)
	}
	if got := h.topo.releasesCopy(); !slices.Contains(got, "run-1:terminal") {
		t.Fatalf("releases = %v", got)
	}
}

// ---------- DST-3 (engine) ----------

// A replaced target before the run's record was durable rolls back; once
// the run's own record is durable it holds.
func TestFixR1_ReplacedTargetRollsBackOnlyWhenUnrecorded(t *testing.T) {
	inconsistent := func(PlannedRunRef) (PromoteOutcome, error) {
		return PromoteOutcome{Kind: PromoteInconsistent, Stage: PromotionStageInconsistent, Reason: v1alpha1.PlannedFailoverReasonExecuteFailed,
			TargetReachable: true, MayHaveWritten: true, RollbackIfUnrecorded: true}, nil
	}
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = inconsistent
	h.step()
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonTargetChanged {
		t.Fatalf("block = %+v, want RollingBack{TargetChanged}", pf)
	}

	h = pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = inconsistent
	h.setAnnotation(LastFailoverAnnotation, failoverStampFormat(h.clk.Now()))
	h.setAnnotation(LastFailoverTargetAnnotation, "pdx")
	h.setAnnotation(PromotionGtidExecutedAnnotation, "uuid-iad:1-10")
	h.setAnnotation(PromotionGtidExecutedForAnnotation, failoverWatermarkBinding(h.clk.Now(), "pdx"))
	h.step()
	if r := h.pf().Result; r == nil || r.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		t.Fatalf("result = %+v, want ManualRecoveryRequired once the run's own record is durable", r)
	}
}

// ---------- O-M4 / G-M3 (Dragonfly) ----------

func pfDFTakeoverFixture(policy string, intendedAt time.Time) *v1alpha1.MysqlFailoverGroup {
	fg := plannedFailoverFGWithDragonfly(policy)
	pf := fg.Status.PlannedFailover
	pf.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	off := int64(100)
	pf.Dragonfly.SourceOffsetState, pf.Dragonfly.SourceOffset = v1alpha1.DragonflyOffsetCaptured, &off
	pf.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntentTime: metav1.NewTime(intendedAt)}
	return fg
}

// PromotingDragonfly has its own deadline from takeover.intendedAt: past
// it the onSyncTimeout policy applies instead of another takeover attempt
// with the source fenced, and its pass bound covers the gate wait,
// REPLTAKEOVER and a write.
func TestFixR1_DragonflyTakeoverDeadline(t *testing.T) {
	for _, tc := range []struct {
		policy string
		want   v1alpha1.PlannedFailoverPhase
	}{
		{"Fail", v1alpha1.PlannedFailoverPhaseRollingBack},
		{"Proceed", v1alpha1.PlannedFailoverPhasePromoting},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			probe := pfDFTakeoverFixture(tc.policy, time.Now())
			late := time.Now().Add(-plannedDragonflyTakeoverDeadline(probe.Status.PlannedFailover) - 5*time.Second)
			fg := pfDFTakeoverFixture(tc.policy, late)
			sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
			targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
			r, _, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
			target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 100}}}
			installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target,
				pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}})
			pfDFTakeoverPasses(t, r, fgNN(fg))
			pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
			if pf.Phase != tc.want || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed {
				t.Fatalf("block = %s %+v, want %s with the takeover Failed", pf.Phase, pf.Dragonfly.Takeover, tc.want)
			}
			if target.takeovers != 0 {
				t.Fatalf("REPLTAKEOVER sent %d times past the takeover deadline", target.takeovers)
			}
		})
	}
	// Past the deadline a target already reporting master is not finished
	// either (fix round 5): only a REPLTAKEOVER success reply this process
	// received finishes a takeover, and that is recorded before the
	// deadline matters. The fail policy rolls MySQL back; the source keeps
	// serving.
	t.Run("target already master", func(t *testing.T) {
		probe := pfDFTakeoverFixture("Fail", time.Now())
		late := time.Now().Add(-plannedDragonflyTakeoverDeadline(probe.Status.PlannedFailover) - 5*time.Second)
		fg := pfDFTakeoverFixture("Fail", late)
		sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
		targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
		r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
		source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
		target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}}
		installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})
		pfDFTakeoverPasses(t, r, fgNN(fg))
		pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
		if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed {
			t.Fatalf("block = %s %+v, want RollingBack with the takeover Failed", pf.Phase, pf.Dragonfly.Takeover)
		}
		if target.takeovers != 0 {
			t.Fatalf("REPLTAKEOVER sent %d times past the takeover deadline", target.takeovers)
		}
		pfDFAssertSourceKept(t, c, source, sourcePod, targetPod)
	})
	e := &PlannedFailoverEngine{runs: map[string]*plannedRunMemo{}}
	pf := pfDFTakeoverFixture("Proceed", time.Now()).Status.PlannedFailover
	if got, want := e.passBound(pf), defaultDragonflyGateWait+plannedDragonflyMaxSyncWait(pf)+plannedDragonflyTakeoverSlack+plannedPassOrdinary; got != want {
		t.Fatalf("PromotingDragonfly pass bound = %s, want %s", got, want)
	}
}

// The takeover runs only under a re-proven claim: a hydrated claim
// re-proves before any role-changing Dragonfly step, and a lost claim sends
// nothing.
func TestFixR1_DragonflyTakeoverRequiresReprovenClaim(t *testing.T) {
	fg := pfDFTakeoverFixture("Proceed", time.Now())
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
	r, _, topo := pfDFReconciler(t, fg, sourcePod, targetPod)
	topo.probeUnguarded = true // production ProbeLag needs no claim
	topo.needsProof = true
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 100}}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target,
		pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}})
	nn := fgNN(fg)
	pfDFPass(t, r, nn)
	if target.takeovers != 0 || topo.count("reprove") != 1 {
		t.Fatalf("hydrated claim: takeovers=%d reproves=%d; the re-proof must come first", target.takeovers, topo.count("reprove"))
	}
	pfDFPass(t, r, nn)
	if target.takeovers != 1 {
		t.Fatalf("after the re-proof: takeovers=%d, want 1", target.takeovers)
	}

	fg2 := pfDFTakeoverFixture("Proceed", time.Now())
	r2, _, topo2 := pfDFReconciler(t, fg2, makeDragonflyPod(fg2.Name, "iad", "master", true), makeDragonflyPod(fg2.Name, "pdx", "replica", true))
	topo2.probeUnguarded = true
	// The run's claim is gone (here: converted to a hold) although its
	// claim pass was answered Granted.
	topo2.setHeld("run-1", true)
	topo2.ensure = func(ref PlannedRunRef) (PlannedClaimDecision, error) {
		return PlannedClaimDecision{Kind: ClaimGranted, Source: ref.Source}, nil
	}
	target2 := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 100}}}
	installFakeDragonflyConn(t, r2, map[string]DragonflyConnection{pfDFPdx: target2,
		pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}})
	pfDFPass(t, r2, fgNN(fg2))
	if target2.takeovers != 0 {
		t.Fatalf("a run without its claim sent REPLTAKEOVER %d times (calls %v)", target2.takeovers, topo2.calls)
	}
}

// ---------- O-m8 ----------

// A registered manager that has not polled yet names no active site: the
// upgrade's supersede check falls back to the durable record.
func TestFixR1_UpgradeSupersedeFallsBackBeforeFirstPoll(t *testing.T) {
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	during := upgradeDuringEmergency(v1alpha1.DragonflyUpgradePhaseSavingSnapshot, start, start.Add(10*time.Second))
	nn := types.NamespacedName{Namespace: during.Namespace, Name: during.Name}
	f := newOpsFixture(t, "")
	f.tm.mu.Lock()
	for i := range f.tm.sites {
		f.tm.sites[i].state = state.StateUnknown
	}
	f.tm.mu.Unlock()
	r := &MysqlFailoverGroupReconciler{Runner: &TopologyManagerRunner{logger: slog.Default(),
		managers: map[types.NamespacedName]*managedTopology{nn: {tm: f.tm}}}}
	if site, ok := r.Runner.ActiveSite(nn); !ok || site != "" {
		t.Fatalf("setup: active site %q (%v), want a registered manager naming none", site, ok)
	}
	if why, active := r.dragonflyUpgradeSupersededByMySQL(during, during.Status.Dragonfly.Upgrade); why == "" || active != "dc2" {
		t.Fatalf("the durable emergency failover did not supersede the upgrade (why %q, active %q)", why, active)
	}
}

// The rollout promotion re-checks, under the gate, a planned-failover claim
// or a standing hold, which the status-only blocked check cannot see.
func TestFixR1_RolloutPromotionRespectsClaimAndHold(t *testing.T) {
	for _, viaCondition := range []bool{false, true} {
		mgr, c, _ := newEmergencyManagerWithPods(t)
		fresh := getFGByKey(t, c, mgr.fgKey)
		runner := &TopologyManagerRunner{}
		if viaCondition {
			setPlannedHold(&fresh.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
				AmbiguousSites: []string{"dc2"}, HoldTime: metav1.Now()}, fresh.Generation)
			if err := c.Status().Update(context.Background(), fresh); err != nil {
				t.Fatal(err)
			}
		} else {
			runner.Slot(mgr.fgKey).toHold("run-1")
		}
		r := &MysqlFailoverGroupReconciler{Client: c, APIReader: c, Runner: runner, Recorder: record.NewFakeRecorder(64)}
		dials := 0
		r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) {
			dials++
			return &fakeDragonflyConn{}, nil
		}
		if promoted, deferred := r.promoteDragonflyForRollout(context.Background(), fresh, "dc2", "dc1"); promoted || !deferred || dials != 0 {
			t.Errorf("hold (condition=%v): promoted=%v deferred=%v dials=%d, want deferred with nothing dialled", viaCondition, promoted, deferred, dials)
		}
	}
}

// ---------- facade (ops fixture) ----------

// O-M3: a Fence re-run whose identity read fails is a source that did not
// answer (the loss window), never "nothing was sent"; once it answers, the
// run's own fence (this process's memo) goes on to the post-fence proof.
func TestFixR1_FenceRerunIdentityReadFailure(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	f.topo.noteFenceSent("run-1", opsUUIDA) // an earlier attempt's SET, readback lost
	f.src.sroReadErr = errors.New("i/o timeout")
	proof, err := f.topo.Fence(context.Background(), f.ref, opsUUIDA)
	if err != nil || proof.Kind != FenceSourceUnreachable || !proof.Sent {
		t.Fatalf("Fence with a failed identity read = %+v, %v; want SourceUnreachable with Sent", proof, err)
	}
	f.src.sroReadErr = nil
	proof, err = f.topo.Fence(context.Background(), f.ref, opsUUIDA)
	if err != nil || proof.Kind != FenceApplied || proof.Attribution != FenceByMemo || proof.GtidSet != opsF {
		t.Fatalf("Fence over the run's own fence = %+v, %v; want Applied by memo", proof, err)
	}
	if got := f.src.statements(); len(got) != 0 {
		t.Fatalf("Fence re-sent %v over the run's own fence", got)
	}
	// Cross process (no memo): the durable intent's uuid attributes it.
	g := newOpsFixture(t, "")
	g.hold(false)
	g.fenced()
	proof, err = g.topo.Fence(context.Background(), g.ref, opsUUIDA)
	if err != nil || proof.Kind != FenceApplied || proof.Attribution != FenceByIntentAndUUID {
		t.Fatalf("cross-process Fence = %+v, %v; want Applied by intent and uuid", proof, err)
	}
}

// O-m2: in the re-proof, an unreadable reader is no lack of evidence, and
// a source read failure before Promoting is a retry.
func TestFixR1_ReproveEvidence(t *testing.T) {
	f := newOpsFixture(t, state.SiteRoleReadOnly)
	f.hold(true)
	f.fenced()
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false // promoted
	f.third.down = true
	res, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(opsRun(v1alpha1.PlannedFailoverPhasePromoting)))
	if err != nil || !res.OK {
		t.Fatalf("Reprove with an unreadable reader = %+v, %v; want OK", res, err)
	}

	g := newOpsFixture(t, "")
	g.hold(true)
	g.fenced()
	g.src.down = true
	res, err = g.topo.Reprove(context.Background(), g.ref, opsGroup(opsRun(v1alpha1.PlannedFailoverPhaseWaitingForLag)))
	if err != nil || !res.SourceUnreachable || res.RollBack != nil {
		t.Fatalf("Reprove with the source unreadable = %+v, %v; want SourceUnreachable, no rollback", res, err)
	}
}

// O-m3: a rollback that leaves the source fenced beside a writable peer
// still reports a fence that is not the run's.
func TestFixR1_RollbackFlagsUnattributedFenceBesideWritablePeer(t *testing.T) {
	f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
	f.hold(false)
	f.fenced()
	f.src.uuid = opsUUIDC // replaced and fenced by someone else
	f.third.ro, f.third.sro = false, false
	res, err := f.topo.Rollback(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhaseRollingBack))
	if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
		!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) {
		t.Fatalf("Rollback = %+v, %v; want SourceLeftFenced with FenceNotAttributable", res, err)
	}
}

// G-M5: Reprove and Promote with an endpoint gone from the topology return
// errSiteNotFound, without a nil dereference.
func TestFixR1_RunSiteRemovedFacade(t *testing.T) {
	f := newOpsFixture(t, "")
	f.ref.Target = "dc9"
	f.hold(true)
	pf := opsRun(v1alpha1.PlannedFailoverPhaseWaitingForLag)
	pf.TargetSite = "dc9"
	if _, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(pf)); !errors.Is(err, errSiteNotFound) {
		t.Fatalf("Reprove with the target gone = %v, want errSiteNotFound", err)
	}
	g := newOpsFixture(t, "")
	g.ref.Source = "dc9"
	g.hold(false)
	g.tgt.repl = nil
	pf = opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	pf.SourceSite = "dc9"
	if _, err := g.topo.Promote(context.Background(), g.ref, pf); !errors.Is(err, errSiteNotFound) {
		t.Fatalf("Promote with the source gone = %v, want errSiteNotFound", err)
	}
	if got := g.tgt.statements(); len(got) != 0 {
		t.Fatalf("Promote sent %v with the source gone", got)
	}
}

// G-M4: the claim is checked again after the gate's probes, immediately
// before SET super_read_only=OFF.
func TestFixR1_ClaimRecheckedAfterGateProbes(t *testing.T) {
	for _, lose := range []string{"claim", "leader"} {
		t.Run(lose, func(t *testing.T) {
			f := newOpsFixture(t, "")
			f.hold(false)
			f.fenced()
			if _, err := f.topo.ReadTargetServerUUID(context.Background(), f.ref); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.reg.leader = ctx
			f.topo.hooks = SimHooks{Yield: func(point string) {
				if point != YieldGateProbeForSim {
					return
				}
				if lose == "claim" {
					f.reg.slot.release("run-1", ReleaseZombie)
				} else {
					cancel()
				}
			}}
			_, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
			if !errors.Is(err, errClaimLost) && !errors.Is(err, errLeaderLost) {
				t.Fatalf("Promote = %v, want the lost claim or leader", err)
			}
			if got := f.tgt.statements(); slices.Contains(got, "sro=OFF") || slices.Contains(got, "ro=OFF") {
				t.Fatalf("target statements %v: a write was sent after the %s was lost", got, lose)
			}
		})
	}
}

// O-m4: each planned promotion statement is bounded on its own, and the
// guard runs before it.
func TestFixR1_PromoteHooksBoundAndGuard(t *testing.T) {
	h := promoteHooks{statementTimeout: 20 * time.Millisecond}
	start := time.Now()
	err := h.send(context.Background(), promoteMemoStopSent, func(c context.Context) error { <-c.Done(); return c.Err() })
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("bounded statement = %v after %s", err, time.Since(start))
	}
	sent := false
	h = promoteHooks{guard: func() error { return errClaimLost }, before: func(promoteMemoStage) { sent = true }}
	if err := h.send(context.Background(), promoteMemoSROClearSent, func(context.Context) error { sent = true; return nil }); !errors.Is(err, errClaimLost) || sent {
		t.Fatalf("guarded statement = %v, sent=%v", err, sent)
	}
}

// O-m5: a name-only unconfirmed target reaches the replayed status
// snapshot.
func TestFixR1_NameOnlyTargetReachesRetrySnapshot(t *testing.T) {
	f := newOpsFixture(t, "")
	f.tm.mu.Lock()
	f.tm.statusRetrySnapshot = &TopologySnapshot{LastFailoverTarget: "dc1", TopologyGeneration: f.tm.topologyGeneration}
	f.tm.mu.Unlock()
	f.tm.namePlannedTargetUnconfirmed("dc2")
	f.tm.mu.RLock()
	defer f.tm.mu.RUnlock()
	if got := f.tm.statusRetrySnapshot.LastFailoverTarget; got != "dc2" {
		t.Fatalf("retry snapshot names %q, want dc2", got)
	}
}

// O-m6: a cross-process STOP/RESET proof mismatch at a stage with the
// channel still configured proves no SRO clear ran: Aborted, not
// Unconfirmed.
func TestFixR1_CrossProcessProofMismatchAborts(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	f.tgt.gtid = opsUUIDA + ":1-9" // missing one transaction of F
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonTargetChanged || out.MayHaveWritten {
		t.Fatalf("Promote = %+v, %v; want Aborted{TargetChanged} with nothing written", out, err)
	}
	if got := f.tgt.statements(); slices.Contains(got, "sro=OFF") {
		t.Fatalf("statements = %v", got)
	}
}

// O-m7: re-asserting a standing hold records nothing new.
func TestFixR1_StandingHoldIsIdempotent(t *testing.T) {
	f := newOpsFixture(t, "")
	if !f.topo.ConvertToHold(f.nn, "run-1") {
		t.Fatal("the first conversion did not install the hold")
	}
	for range 5 {
		if f.topo.ConvertToHold(f.nn, "run-1") {
			t.Fatal("a standing hold was converted again")
		}
	}
	if n := len(f.reg.slot.ReleaseNotes()); n != 1 {
		t.Fatalf("release notes = %d, want 1", n)
	}
}

// O-m1: a claim's recorded promotion is not applied to a replacement
// manager without its watermark.
func TestFixR1_WireSkipsPromotionWithoutWatermark(t *testing.T) {
	f := newOpsFixture(t, "")
	slot := &PlannedClaimSlot{}
	slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2",
		promotion: &recordedPromotion{confirmedAt: f.clk.Now()}})
	WireRegisteredManager(f.tm, ManagerWiring{Slot: slot}) // the uncached read failed: no HydrateFrom
	f.tm.mu.RLock()
	defer f.tm.mu.RUnlock()
	if !f.tm.lastFailover.IsZero() {
		t.Fatalf("lastFailover moved to %s beside an unknown watermark", f.tm.lastFailover)
	}
}

// O-M2: with no manager registered, EnsureClaim waits ManagerAbsent
// whether or not the slot holds the run (and hydrates an empty slot).
func TestFixR1_EnsureClaimWithoutManagerWaits(t *testing.T) {
	f := newOpsFixture(t, "")
	f.reg.mu.Lock()
	f.reg.current = nil
	f.reg.mu.Unlock()
	fg := opsGroup(opsRun(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	for i := range 2 {
		d, err := f.topo.EnsureClaim(context.Background(), f.ref, fg)
		if err != nil || d.Kind != ClaimWait || d.WaitReason != v1alpha1.PlannedFailoverWaitManagerAbsent {
			t.Fatalf("pass %d: EnsureClaim = %+v, %v; want Wait{ManagerAbsent}", i, d, err)
		}
		if run, _, ok := f.reg.slot.HeldForSim(); !ok || run != "run-1" {
			t.Fatalf("pass %d: the slot was not hydrated (%q)", i, run)
		}
	}
}

// G-M2 / O-M5: own-record recognition takes the watermark from the record
// EffectiveFailoverRecord selected: status records the run's own promotion
// while the out-of-band copy still holds an earlier failover.
func TestFixR1_OwnRecordUsesSelectedWatermark(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(true)
	f.fenced()
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	t0 := pf.Promotion.IntentTime.Add(-time.Hour).UTC().Truncate(time.Second)
	t0m := metav1.NewTime(t0)
	pf.ClaimBaseline = &v1alpha1.FailoverRecordRef{Time: &t0m, Target: "dc1"}
	fg := opsReproveGroup(f, pf, pf.Promotion.IntentTime.Add(2*time.Second), "dc2")
	fg.Status.PromotionGtidExecuted = opsF
	fg.Annotations = map[string]string{
		LastFailoverAnnotation:             failoverStampFormat(t0),
		LastFailoverTargetAnnotation:       "dc1",
		PromotionGtidExecutedAnnotation:    opsUUIDA + ":1-5",
		PromotionGtidExecutedForAnnotation: failoverWatermarkBinding(t0, "dc1"),
	}
	res, err := f.topo.Reprove(context.Background(), f.ref, fg)
	if err != nil || !res.OK {
		t.Fatalf("Reprove over the run's own status record = %+v, %v; want OK", res, err)
	}
}

// DST-3 (facade): a cross-process re-run that finds the target replaced
// marks the verdict rollback-safe only when the source holds F under the
// run's fence and no site is writable.
func TestFixR1_ReplacedTargetRollbackSafe(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(f *opsFixture)
		want   bool
	}{
		{"safe", func(*opsFixture) {}, true},
		{"writable peer", func(f *opsFixture) { f.third.ro, f.third.sro = false, false }, false},
		{"unreadable promotable peer", func(f *opsFixture) { f.third.down = true }, false},
		{"source lacks F", func(f *opsFixture) { f.src.gtid = opsUUIDA + ":1-9" }, false},
		{"fence not the run's", func(f *opsFixture) { f.src.uuid = opsUUIDC }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
			f.hold(false)
			f.fenced()
			f.tgt.uuid = opsUUIDC // replaced, regressed
			f.tgt.gtid = opsUUIDA + ":1-5"
			tc.mutate(f)
			out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
			if err != nil || out.Kind != PromoteInconsistent || out.RollbackIfUnrecorded != tc.want {
				t.Fatalf("Promote = %+v, %v; want Inconsistent with RollbackIfUnrecorded=%v", out, err, tc.want)
			}
			if got := f.tgt.statements(); len(got) != 0 {
				t.Fatalf("statements = %v", got)
			}
		})
	}
}

// G-m8: the planned path logs the documented "failover complete" only
// after its live confirmation.
func TestFixR1_FailoverCompleteOnlyAfterConfirm(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
		logs := &fixR1LogBuffer{}
		f.tm.failover = NewFailoverController(slog.New(slog.NewTextHandler(logs, nil)))
		f.hold(false)
		f.fenced()
		f.tgt.repl = nil
		if foreign {
			f.tgt.beforeSRO = func() {
				f.third.mu.Lock()
				f.third.ro, f.third.sro = false, false
				f.third.mu.Unlock()
			}
		}
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if foreign {
			want = 0
		}
		if n := logs.count("failover complete"); n != want {
			t.Fatalf("foreign=%v (outcome %v): %d \"failover complete\" lines, want %d", foreign, out.Kind, n, want)
		}
	}
}

// DST-1: a retiring manager's failover record reaches its replacement at
// registration and when carried after the retired manager's workers exit.
func TestFixR1_CarryRetiredFailoverRecord(t *testing.T) {
	old := newOpsFixture(t, "")
	at := old.clk.Now().Add(-time.Minute)
	old.tm.recordFailover(context.Background(), at, "dc2", opsF)
	repl := newOpsFixture(t, "")
	WireRegisteredManager(repl.tm, ManagerWiring{Slot: &PlannedClaimSlot{}, Predecessors: []*TopologyManager{old.tm}})
	repl.tm.mu.RLock()
	got, target, wm := repl.tm.lastFailover, repl.tm.lastFailoverTarget, repl.tm.promotionGtidExecuted
	dirty := repl.tm.failoverStateDirty
	repl.tm.mu.RUnlock()
	if !got.Equal(at) || target != "dc2" || wm != opsF || !dirty {
		t.Fatalf("replacement record = %s %q %q dirty=%v, want the predecessor's", got, target, wm, dirty)
	}
	// A later handoff on the retired manager is carried afterwards; an older
	// record never overwrites a newer one.
	later := at.Add(30 * time.Second)
	old.tm.recordFailover(context.Background(), later, "dc1", opsF)
	if !CarryRetiredFailoverRecord(repl.tm, old.tm) {
		t.Fatal("the later record was not carried")
	}
	if CarryRetiredFailoverRecord(repl.tm, old.tm) {
		t.Fatal("an equal record was carried again")
	}
	stale := newOpsFixture(t, "")
	stale.tm.recordFailover(context.Background(), at.Add(-time.Hour), "dc2", opsF)
	if CarryRetiredFailoverRecord(repl.tm, stale.tm) {
		t.Fatal("an older record overwrote a newer one")
	}
	repl.tm.mu.RLock()
	defer repl.tm.mu.RUnlock()
	if repl.tm.lastFailoverTarget != "dc1" || !repl.tm.lastFailover.Equal(later) {
		t.Fatalf("record = %s %q, want the later dc1 handoff", repl.tm.lastFailover, repl.tm.lastFailoverTarget)
	}
}

// DST-1 (runner): a predecessor that left retiring before its replacement
// registered still hands its record over through the stash.
func TestFixR1_RunnerStashesRetiredFailoverRecord(t *testing.T) {
	old := newOpsFixture(t, "")
	at := old.clk.Now().Add(-time.Minute)
	old.tm.recordFailover(context.Background(), at, "dc2", opsF)
	r := &TopologyManagerRunner{logger: slog.Default(), managers: map[types.NamespacedName]*managedTopology{}}
	nn := old.nn
	done := make(chan struct{})
	close(done)
	r.mu.Lock()
	r.retireManagerLocked(nn, &managedTopology{tm: old.tm, done: done})
	r.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.RLock()
		n := len(r.retiring[nn])
		r.mu.RUnlock()
		if n == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	repl := newOpsFixture(t, "")
	r.registerManagerWith(nn, nil, &managedTopology{tm: repl.tm})
	repl.tm.mu.RLock()
	defer repl.tm.mu.RUnlock()
	if repl.tm.lastFailoverTarget != "dc2" || !repl.tm.lastFailover.Equal(at) {
		t.Fatalf("replacement record = %s %q, want the retired manager's", repl.tm.lastFailover, repl.tm.lastFailoverTarget)
	}
}
