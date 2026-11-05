//go:build envtest

package envtest

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// pfEnvTopology is a happy-path PlannedFailoverTopology with a one-run slot.
type pfEnvTopology struct {
	mu       sync.Mutex
	held     string
	hold     bool
	promote  func() controller.PromoteOutcome
	calls    []string
	releases []string
}

func (f *pfEnvTopology) note(c string) { f.mu.Lock(); f.calls = append(f.calls, c); f.mu.Unlock() }

func (f *pfEnvTopology) ClaimPlannedFailover(_ context.Context, _ types.NamespacedName, req controller.PlannedClaimRequest) (controller.PlannedClaimDecision, error) {
	f.note("claim")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held = req.RunID
	return controller.PlannedClaimDecision{Kind: controller.ClaimGranted, Source: "dc1", SourceServerUUID: "uuid-dc1", Baseline: req.DurableBaseline}, nil
}
func (f *pfEnvTopology) EnsureClaim(_ context.Context, ref controller.PlannedRunRef, _ *v1alpha1.MysqlFailoverGroup) (controller.PlannedClaimDecision, error) {
	f.note("ensure")
	return controller.PlannedClaimDecision{Kind: controller.ClaimGranted, Source: ref.Source}, nil
}
func (f *pfEnvTopology) ReleasePlannedFailoverClaim(_ types.NamespacedName, runID string, why controller.ClaimReleaseReason) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held != runID || (f.hold && why != controller.ReleaseAck) {
		return false
	}
	f.held, f.hold = "", false
	f.releases = append(f.releases, string(why))
	return true
}
func (f *pfEnvTopology) ConvertToHold(_ types.NamespacedName, runID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held, f.hold = runID, true
	return true
}
func (f *pfEnvTopology) PlannedClaimHeld(types.NamespacedName) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held != ""
}
func (f *pfEnvTopology) Reprove(context.Context, controller.PlannedRunRef, *v1alpha1.MysqlFailoverGroup) (controller.ReproveResult, error) {
	return controller.ReproveResult{OK: true}, nil
}
func (f *pfEnvTopology) Fence(context.Context, controller.PlannedRunRef, string) (controller.FenceProof, error) {
	f.note("fence")
	return controller.FenceProof{Kind: controller.FenceApplied, SourceServerUUID: "uuid-dc1", GtidSet: "uuid-dc1:1-5"}, nil
}
func (f *pfEnvTopology) ProveFence(context.Context, controller.PlannedRunRef, v1alpha1.PlannedFailoverFenceRecord) (controller.FenceProof, error) {
	return controller.FenceProof{Kind: controller.FenceApplied, SourceServerUUID: "uuid-dc1", GtidSet: "uuid-dc1:1-5"}, nil
}
func (f *pfEnvTopology) KillAppConnections(context.Context, controller.PlannedRunRef) (int32, error) {
	return 0, nil
}
func (f *pfEnvTopology) ProbeLag(context.Context, controller.PlannedRunRef, string) (controller.LagProbe, error) {
	return controller.LagProbe{SourceReachable: true, TargetReachable: true, CaughtUp: true}, nil
}
func (f *pfEnvTopology) ReadTargetServerUUID(context.Context, controller.PlannedRunRef) (string, error) {
	return "uuid-dc2", nil
}
func (f *pfEnvTopology) Promote(context.Context, controller.PlannedRunRef, *v1alpha1.PlannedFailoverStatus) (controller.PromoteOutcome, error) {
	f.note("promote")
	if f.promote != nil {
		return f.promote(), nil
	}
	return controller.PromoteOutcome{Kind: controller.PromoteConfirmed, TargetReachable: true, MayHaveWritten: true}, nil
}
func (f *pfEnvTopology) RecordPlannedPromotion(context.Context, controller.PlannedRunRef, time.Time, string) error {
	return nil
}
func (f *pfEnvTopology) ApplyDNS(context.Context, controller.PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
	return v1alpha1.PlannedFailoverDNSApplied, nil
}
func (f *pfEnvTopology) NameUnconfirmedTarget(controller.PlannedRunRef) error { return nil }
func (f *pfEnvTopology) NameFencedSource(controller.PlannedRunRef) error      { return nil }
func (f *pfEnvTopology) Rollback(context.Context, controller.PlannedRunRef, *v1alpha1.PlannedFailoverStatus) (controller.RollbackResult, error) {
	return controller.RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack}, nil
}
func (f *pfEnvTopology) ActiveSite(types.NamespacedName) (string, bool) { return "", false }

var _ controller.PlannedFailoverTopology = (*pfEnvTopology)(nil)

// pfEnvGroup creates a two-site group with dc1 active and dc2 a healthy
// replica, and returns its key.
func pfEnvGroup(t *testing.T, prefix string, annotations map[string]string) types.NamespacedName {
	t.Helper()
	ns := createNamespace(t, prefix)
	fg := newTestFG(ns)
	fg.Annotations = annotations
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: "writable"}, {Name: "dc2", State: "read-only", Replicating: true}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	return client.ObjectKeyFromObject(fg)
}

func pfEnvGet(t *testing.T, nn types.NamespacedName) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, nn, &fg); err != nil {
		t.Fatal(err)
	}
	return &fg
}

func pfEnvReconciler(topo controller.PlannedFailoverTopology) *controller.MysqlFailoverGroupReconciler {
	return &controller.MysqlFailoverGroupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme,
		Recorder: record.NewFakeRecorder(256), PlannedTopology: topo}
}

func pfEnvStepUntil(t *testing.T, r *controller.MysqlFailoverGroupReconciler, nn types.NamespacedName, n int,
	done func(*v1alpha1.MysqlFailoverGroup) bool) {
	t.Helper()
	for i := 0; i < n; i++ {
		if done(pfEnvGet(t, nn)) {
			return
		}
		if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if !done(pfEnvGet(t, nn)) {
		t.Fatalf("not reached after %d steps: %+v", n, pfEnvGet(t, nn).Status.PlannedFailover)
	}
}

// The reconciler end to end against the API server with a fake facade:
// Admitting → Succeeded, the annotation removed only once the acceptance
// record committed, revisions strictly increasing, and the failover record
// written with the terminal record.
func TestEnvtest_PlannedFailoverEngine_EndToEnd(t *testing.T) {
	nn := pfEnvGroup(t, "pf-e2e", map[string]string{controller.PlannedFailoverAnnotation: "dc2"})
	topo := &pfEnvTopology{}
	r := pfEnvReconciler(topo)
	if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
		t.Fatal(err)
	}
	fg := pfEnvGet(t, nn)
	pf := fg.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.RunID == "" || pf.Revision != 1 {
		t.Fatalf("admitting record = %+v", pf)
	}
	if _, ok := fg.Annotations[controller.PlannedFailoverAnnotation]; ok {
		t.Fatal("annotation kept after the Admitting record committed")
	}
	runID, rev := pf.RunID, pf.Revision
	for i := 0; i < 20; i++ {
		if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
			t.Fatal(err)
		}
		cur := pfEnvGet(t, nn).Status.PlannedFailover
		if cur.RunID != runID || cur.Revision < rev {
			t.Fatalf("block regressed: run %s rev %d after rev %d", cur.RunID, cur.Revision, rev)
		}
		rev = cur.Revision
		if cur.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
			break
		}
	}
	fg = pfEnvGet(t, nn)
	pf = fg.Status.PlannedFailover
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("final = %+v", pf)
	}
	if fg.Status.LastFailoverTarget != "dc2" || fg.Status.PromotionGtidExecuted != "uuid-dc1:1-5" || fg.Status.LastFailover == nil {
		t.Errorf("failover record = %v %q %q", fg.Status.LastFailover, fg.Status.LastFailoverTarget, fg.Status.PromotionGtidExecuted)
	}
}

// §13: a block an older operator wrote mid-run is refused and nothing is
// hydrated from it.
func TestEnvtest_PlannedFailover_LegacyRefused(t *testing.T) {
	nn := pfEnvGroup(t, "pf-legacy", nil)
	fg := pfEnvGet(t, nn)
	start := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining, Target: "dc2",
		SourcePrimary: "dc1", StartTime: &start, SourceGtidAtFence: "uuid-dc1:1-5"}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	topo := &pfEnvTopology{}
	r := pfEnvReconciler(topo)
	if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
		t.Fatal(err)
	}
	pf := pfEnvGet(t, nn).Status.PlannedFailover
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
		pf.Result.Reason != v1alpha1.PlannedFailoverReasonLegacyRunRefused || pf.RunID == "" {
		t.Fatalf("block = %+v", pf)
	}
	if len(topo.calls) != 0 || topo.PlannedClaimHeld(nn) {
		t.Errorf("a legacy block reached the topology: %v", topo.calls)
	}
	found := false
	for len(r.Recorder.(*record.FakeRecorder).Events) > 0 {
		if ev := <-r.Recorder.(*record.FakeRecorder).Events; strings.Contains(ev, "PlannedFailoverLegacyRefused") {
			found = true
		}
	}
	if !found {
		t.Error("no PlannedFailoverLegacyRefused event")
	}
}

// The no-op trap and the fence: a status update that changes no bytes does
// not move resourceVersion, while the conditioned metadata fence does, and
// a late write conditioned on the fenced version is refused.
func TestEnvtest_PlannedFailoverStatusFence_NoOpTrapAndLateCommit(t *testing.T) {
	nn := pfEnvGroup(t, "pf-fence", nil)
	base := pfEnvGet(t, nn)
	noop := base.DeepCopy()
	if err := k8sClient.Status().Update(ctx, noop); err != nil {
		t.Fatal(err)
	}
	if noop.ResourceVersion != base.ResourceVersion {
		t.Fatalf("a no-op status update moved resourceVersion %s → %s; the fence must change bytes", base.ResourceVersion, noop.ResourceVersion)
	}
	for _, key := range []string{controller.PlannedFailoverStatusFenceAnnotation, controller.PlannedFailoverTerminalAnnotation} {
		cur := pfEnvGet(t, nn)
		fenced := cur.DeepCopy()
		if fenced.Annotations == nil {
			fenced.Annotations = map[string]string{}
		}
		fenced.Annotations[key] = "run-1/SourceLeftFenced/StatusStalled"
		if err := k8sClient.Patch(ctx, fenced, client.MergeFromWithOptions(cur.DeepCopy(), client.MergeFromWithOptimisticLock{})); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		late := cur.DeepCopy()
		late.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseDraining, Target: "dc2"}
		if err := k8sClient.Status().Update(ctx, late); !apierrors.IsConflict(err) {
			t.Fatalf("%s: late commit after the fence = %v, want a conflict", key, err)
		}
	}
}

// §10.5: the hold condition survives a runner status write, and the
// acknowledgement clears it, releases the slot and deletes itself.
func TestEnvtest_PlannedFailover_HoldSurvivesRunnerWriteAndAck(t *testing.T) {
	nn := pfEnvGroup(t, "pf-hold", map[string]string{controller.PlannedFailoverAnnotation: "dc2"})
	topo := &pfEnvTopology{promote: func() controller.PromoteOutcome {
		return controller.PromoteOutcome{Kind: controller.PromoteInconsistent, Reason: v1alpha1.PlannedFailoverReasonForeignWritable, MayHaveWritten: true}
	}}
	r := pfEnvReconciler(topo)
	pfEnvStepUntil(t, r, nn, 20, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover != nil && fg.Status.PlannedFailover.Result != nil
	})
	fg := pfEnvGet(t, nn)
	pf := fg.Status.PlannedFailover
	if pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		t.Fatalf("result = %+v", pf.Result)
	}
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("hold condition = %+v", c)
	}

	runner := controller.NewTopologyManagerRunner(k8sClient, nil, nil, nil, slog.New(slog.DiscardHandler))
	snap := controller.TopologySnapshot{ActiveSite: "dc1", TopologyGeneration: fg.Status.TopologyGeneration}
	if err := runner.WriteStatusForTest(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	if c := apimeta.FindStatusCondition(pfEnvGet(t, nn).Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); c == nil {
		t.Fatal("a runner status write dropped the hold condition")
	}

	fg = pfEnvGet(t, nn)
	fg.Annotations = map[string]string{controller.AckPlannedFailoverHoldAnnotation: pf.RunID}
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	pfEnvStepUntil(t, r, nn, 5, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		_, ack := fg.Annotations[controller.AckPlannedFailoverHoldAnnotation]
		return !ack
	})
	fg = pfEnvGet(t, nn)
	if apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld) != nil {
		t.Error("hold condition kept after the acknowledgement")
	}
	if topo.PlannedClaimHeld(nn) {
		t.Error("slot still held after the acknowledgement")
	}
}

// §6.4 / §6.3: a durable deadline annotation is written once per run and
// never moved forward by a restarted reconciler; it is removed after the
// next committed status write.
func TestEnvtest_PlannedFailover_DurableDeadlineAnnotations(t *testing.T) {
	nn := pfEnvGroup(t, "pf-deadline", nil)
	fg := pfEnvGet(t, nn)
	now := metav1.Now()
	f := "uuid-dc1:1-5"
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 3, Phase: v1alpha1.PlannedFailoverPhaseWaitingForLag,
		Target: "dc2", Source: "dc1", StartTime: &now,
		Settings: &v1alpha1.PlannedFailoverSettings{MaxLagWait: metav1.Duration{Duration: time.Hour}, DrainTimeout: metav1.Duration{Duration: time.Second}},
		Fence: &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceApplied, IntendedAt: now, SourceServerUUID: "uuid-dc1",
			AppliedAt: &now, GtidSet: &f},
		Drain: &v1alpha1.PlannedFailoverDrainRecord{StartTime: now, CompletedAt: &now}, LagWait: &v1alpha1.PlannedFailoverLagWaitRecord{StartTime: now}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	absent := &pfEnvAbsentTopology{pfEnvTopology: &pfEnvTopology{held: "run-1"}}
	var first string
	for i := 0; i < 3; i++ {
		r := pfEnvReconciler(absent) // a restarted operator each time
		if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
			t.Fatal(err)
		}
		got := pfEnvGet(t, nn).Annotations[controller.PlannedFailoverManagerAbsentSinceAnnotation]
		if got == "" {
			t.Fatal("no manager-absent-since annotation")
		}
		if first == "" {
			first = got
		} else if got != first {
			t.Fatalf("restart %d moved the absent-since start: %q → %q", i, first, got)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	// A manager is back: the annotation goes.
	r := pfEnvReconciler(absent.pfEnvTopology)
	if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
		t.Fatal(err)
	}
	if v, ok := pfEnvGet(t, nn).Annotations[controller.PlannedFailoverManagerAbsentSinceAnnotation]; ok {
		t.Errorf("absent-since kept once a manager answered: %q", v)
	}
}

// pfEnvAbsentTopology reports no registered manager.
type pfEnvAbsentTopology struct{ *pfEnvTopology }

func (f *pfEnvAbsentTopology) EnsureClaim(context.Context, controller.PlannedRunRef, *v1alpha1.MysqlFailoverGroup) (controller.PlannedClaimDecision, error) {
	return controller.PlannedClaimDecision{Kind: controller.ClaimWait, WaitReason: v1alpha1.PlannedFailoverWaitManagerAbsent}, nil
}
