//go:build envtest

package envtest

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// TestCRD_PlannedFailoverRoundTrip pins the final shape of
// status.plannedFailover: every field and enum value persists unpruned, the
// run identity and parameters are required, and invalid phase, outcome,
// reason and wait-reason values (including the removed phases) are rejected
// by the API server.
func TestCRD_PlannedFailoverRoundTrip(t *testing.T) {
	ns := createNamespace(t, "pf-roundtrip")
	ts := func(sec int) metav1.Time {
		return metav1.NewTime(time.Date(2026, 11, 4, 12, 0, sec, 0, time.UTC))
	}
	ptrTime := func(sec int) *metav1.Time { t := ts(sec); return &t }
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42"
	empty := ""
	remaining := int32(2)
	srcOffset, tgtOffset, syncSecs := int64(100), int64(100), int64(3)

	full := &v1alpha1.PlannedFailoverStatus{
		RunID:    "6f1d0c3a-5b8e-4c1f-9a2d-0e7b3c4d5e6f",
		Revision: 7,
		Phase:    v1alpha1.PlannedFailoverPhasePublishing,
		Target:   "dc2",
		Settings: v1alpha1.PlannedFailoverSettings{
			MaxLagWait:   metav1.Duration{Duration: 5 * time.Minute},
			DrainTimeout: metav1.Duration{Duration: 30 * time.Second},
			Dragonfly: &v1alpha1.PlannedFailoverDragonflySettings{
				MaxSyncWait: metav1.Duration{Duration: 30 * time.Second}, OnSyncTimeout: "proceed",
			},
		},
		StartTime:     ts(0),
		Source:        "dc1",
		ClaimedAt:     ptrTime(1),
		ClaimBaseline: &v1alpha1.PlannedFailoverRecordRef{LastFailover: ptrTime(2), LastFailoverTarget: "dc1"},
		Admission: &v1alpha1.PlannedFailoverAdmission{
			Deadline: ptrTime(3), WaitReason: v1alpha1.PlannedFailoverWaitRecoveryInFlight,
			DeferReason: v1alpha1.PlannedFailoverDeferCooldownActive, RetryAfter: ptrTime(4),
		},
		Fence: &v1alpha1.PlannedFailoverFenceRecord{
			State: v1alpha1.PlannedFailoverFenceApplied, IntendedAt: ts(5), SourceServerUUID: "uuid-src",
			AppliedAt: ptrTime(6), GtidSet: &gtid,
		},
		Drain:   &v1alpha1.PlannedFailoverDrainRecord{StartTime: ts(7), CompletedAt: ptrTime(8), RemainingConnections: &remaining},
		LagWait: &v1alpha1.PlannedFailoverLagWaitRecord{StartTime: ts(9), CaughtUpAt: ptrTime(10)},
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{
			SyncWaitStartTime: ptrTime(11),
			SourceOffsetState: v1alpha1.DragonflyOffsetCaptured,
			SourceOffset:      &srcOffset,
			SyncWaitSeconds:   &syncSecs,
			Takeover: &v1alpha1.DragonflyTakeoverRecord{
				State: v1alpha1.DragonflyTakeoverSucceeded, IntendedAt: ts(12), CompletedAt: ptrTime(13),
				Method: v1alpha1.DragonflyTakeoverMethodReplTakeover, TargetOffset: &tgtOffset,
			},
			Sessions: v1alpha1.DragonflySessionsPreserved,
		},
		Promotion: &v1alpha1.PlannedFailoverPromotionRecord{
			State: v1alpha1.PlannedFailoverPromotionConfirmed, IntendedAt: ts(14), TargetServerUUID: "uuid-tgt",
			ConfirmedAt: ptrTime(15),
		},
		DNS:      &v1alpha1.PlannedFailoverDNSRecord{State: v1alpha1.PlannedFailoverDNSFailed, AttemptedAt: ts(16), LastError: "timeout"},
		Rollback: &v1alpha1.PlannedFailoverRollbackRecord{Reason: v1alpha1.PlannedFailoverReasonLagTimeout, IntendedAt: ts(17)},
		Result: &v1alpha1.PlannedFailoverResult{
			Outcome: v1alpha1.PlannedFailoverOutcomeSucceeded, CompletionTime: ts(18),
			Warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningDNSUpdateFailed, v1alpha1.PlannedFailoverWarningUnreachablePeers},
		},
		Message: "published",
	}

	create := func(name string) *v1alpha1.MysqlFailoverGroup {
		t.Helper()
		fg := newTestFG(ns)
		fg.Name = name
		if err := k8sClient.Create(ctx, fg); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		return fg
	}
	writeStatus := func(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) error {
		fg.Status.PlannedFailover = pf.DeepCopy()
		return k8sClient.Status().Update(ctx, fg)
	}
	readBack := func(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.PlannedFailoverStatus {
		t.Helper()
		var got v1alpha1.MysqlFailoverGroup
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
			t.Fatal(err)
		}
		return got.Status.PlannedFailover
	}

	fg := create("roundtrip")
	if err := writeStatus(fg, full); err != nil {
		t.Fatalf("status update with every new field: %v", err)
	}
	if got := readBack(fg); !equality.Semantic.DeepEqual(got, full) {
		t.Fatalf("plannedFailover did not round-trip:\n got %+v\nwant %+v", got, full)
	}

	// Every phase in the enum, and an empty gtidSet, persist.
	for _, phase := range []v1alpha1.PlannedFailoverPhase{
		v1alpha1.PlannedFailoverPhaseAdmitting, v1alpha1.PlannedFailoverPhaseDeferred, v1alpha1.PlannedFailoverPhaseFencing,
		v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync, v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting, v1alpha1.PlannedFailoverPhasePublishing,
		v1alpha1.PlannedFailoverPhaseRollingBack, v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverPhaseFailed,
	} {
		pf := full.DeepCopy()
		pf.Phase = phase
		pf.Fence.GtidSet = &empty
		if err := writeStatus(fg, pf); err != nil {
			t.Fatalf("phase %s: %v", phase, err)
		}
		if got := readBack(fg); got.Phase != phase || got.Fence.GtidSet == nil || *got.Fence.GtidSet != "" {
			t.Fatalf("phase %s: read back phase %q gtidSet %v", phase, got.Phase, got.Fence.GtidSet)
		}
	}

	// A new block without its run identity or parameters is rejected:
	// runId, revision, phase, target, settings and startTime are required.
	for name, mutate := range map[string]func(pf *v1alpha1.PlannedFailoverStatus){
		"no runId":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.RunID = "" },
		"no revision":  func(pf *v1alpha1.PlannedFailoverStatus) { pf.Revision = 0 },
		"no phase":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.Phase = "" },
		"no target":    func(pf *v1alpha1.PlannedFailoverStatus) { pf.Target = "" },
		"no settings":  func(pf *v1alpha1.PlannedFailoverStatus) { pf.Settings = v1alpha1.PlannedFailoverSettings{} },
		"no startTime": func(pf *v1alpha1.PlannedFailoverStatus) { pf.StartTime = metav1.Time{} },
		"old phase":    func(pf *v1alpha1.PlannedFailoverStatus) { pf.Phase = "Validating" },
	} {
		target := create("required-" + strings.ToLower(strings.ReplaceAll(name, " ", "-")))
		pf := full.DeepCopy()
		mutate(pf)
		if err := writeStatus(target, pf); !apierrors.IsInvalid(err) {
			t.Errorf("%s: status update error = %v; want Invalid", name, err)
		}
	}

	// Invalid values are rejected server-side.
	invalid := map[string]func(pf *v1alpha1.PlannedFailoverStatus){
		"phase":        func(pf *v1alpha1.PlannedFailoverStatus) { pf.Phase = "Exploding" },
		"outcome":      func(pf *v1alpha1.PlannedFailoverStatus) { pf.Result.Outcome = "Partial" },
		"reason":       func(pf *v1alpha1.PlannedFailoverStatus) { pf.Result.Reason = "SourceCrashed" },
		"rollback":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.Rollback.Reason = "Whatever" },
		"wait reason":  func(pf *v1alpha1.PlannedFailoverStatus) { pf.Admission.WaitReason = "Sleeping" },
		"defer reason": func(pf *v1alpha1.PlannedFailoverStatus) { pf.Admission.DeferReason = "LagTimeout" },
		"warning": func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result.Warnings = []v1alpha1.PlannedFailoverWarning{"Spooky"}
		},
		"fence state": func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.State = "Maybe" },
		"zero uuid":   func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.SourceServerUUID = "" },
		"revision":    func(pf *v1alpha1.PlannedFailoverStatus) { pf.Revision = -1 },
		"on sync":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.Settings.Dragonfly.OnSyncTimeout = "Proceed" },
		"df takeover": func(pf *v1alpha1.PlannedFailoverStatus) { pf.Dragonfly.Takeover.State = "Halfway" },
	}
	for name, mutate := range invalid {
		target := create("invalid-" + strings.ReplaceAll(name, " ", "-"))
		pf := full.DeepCopy()
		mutate(pf)
		if err := writeStatus(target, pf); !apierrors.IsInvalid(err) {
			t.Errorf("%s: status update error = %v; want Invalid", name, err)
		}
	}
}

// pfEnvInterceptedReconciler is pfEnvReconciler over a client whose status
// updates go through hook first. hook may perform the write itself (or not)
// and return what the engine sees; nil passes the write through.
func pfEnvInterceptedReconciler(t *testing.T, topo controller.PlannedFailoverTopology,
	hook func(ctx context.Context, c client.Client, obj client.Object, opts ...client.SubResourceUpdateOption) (handled bool, err error)) *controller.MysqlFailoverGroupReconciler {
	t.Helper()
	base, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	c := interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "status" {
				if handled, err := hook(ctx, c, obj, opts...); handled {
					return err
				}
			}
			return c.Status().Update(ctx, obj, opts...)
		},
	})
	r := pfEnvReconciler(topo)
	r.Client, r.APIReader = c, c
	return r
}

// enginePFWrite reports whether obj is a planned-failover status write of
// the given phase by the engine.
func enginePFWrite(obj client.Object, phase v1alpha1.PlannedFailoverPhase) bool {
	fg, ok := obj.(*v1alpha1.MysqlFailoverGroup)
	return ok && fg.Status.PlannedFailover != nil && fg.Status.PlannedFailover.Phase == phase
}

// §15.4 item 2: a concurrent status writer moves resourceVersion under the
// engine's conditioned write; the write conflicts, reloads and lands, the
// other writer's change survives, and revisions only increase.
func TestEnvtest_PlannedFailoverStore_ConditionedWriteNeverRegresses(t *testing.T) {
	nn := pfEnvGroup(t, "pf-cond", map[string]string{controller.PlannedFailoverAnnotation: "dc2"})
	raced := false
	r := pfEnvInterceptedReconciler(t, &pfEnvTopology{}, func(ctx context.Context, c client.Client, obj client.Object, _ ...client.SubResourceUpdateOption) (bool, error) {
		if raced || !enginePFWrite(obj, v1alpha1.PlannedFailoverPhaseFencing) {
			return false, nil
		}
		raced = true
		// Another writer (the runner) lands first.
		var cur v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &cur); err != nil {
			return true, err
		}
		cur.Status.Sites[1].GtidExecuted = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42"
		if err := c.Status().Update(ctx, &cur); err != nil {
			return true, err
		}
		return false, nil // the engine's write now carries a stale resourceVersion
	})
	var revs []int64
	pfEnvStepUntil(t, r, nn, 40, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		if pf := fg.Status.PlannedFailover; pf != nil {
			revs = append(revs, pf.Revision)
			return pf.Result != nil
		}
		return false
	})
	if !raced {
		t.Fatal("setup: the concurrent write never ran")
	}
	for i := 1; i < len(revs); i++ {
		if revs[i] < revs[i-1] {
			t.Fatalf("revision regressed: %v", revs)
		}
	}
	got := pfEnvGet(t, nn)
	if got.Status.PlannedFailover.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("result %+v", got.Status.PlannedFailover.Result)
	}
	if got.Status.Sites[1].GtidExecuted != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42" {
		t.Fatal("the engine's retried write overwrote the concurrent writer's change")
	}
}

// §15.4 item 3: a write that commits and then times out is settled as
// committed, and the run proceeds.
func TestEnvtest_PlannedFailoverStatusFence_CommitThenTimeoutProceeds(t *testing.T) {
	nn := pfEnvGroup(t, "pf-commit-timeout", map[string]string{controller.PlannedFailoverAnnotation: "dc2"})
	done := false
	r := pfEnvInterceptedReconciler(t, &pfEnvTopology{}, func(ctx context.Context, c client.Client, obj client.Object, opts ...client.SubResourceUpdateOption) (bool, error) {
		if done || !enginePFWrite(obj, v1alpha1.PlannedFailoverPhaseFencing) {
			return false, nil
		}
		done = true
		if err := c.Status().Update(ctx, obj, opts...); err != nil {
			return true, err
		}
		return true, context.DeadlineExceeded
	})
	pfEnvStepUntil(t, r, nn, 40, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover != nil && fg.Status.PlannedFailover.Result != nil
	})
	got := pfEnvGet(t, nn)
	if got.Status.PlannedFailover.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("result %+v", got.Status.PlannedFailover.Result)
	}
	if got.Annotations[controller.PlannedFailoverStatusFenceAnnotation] != "" {
		t.Fatal("a committed write was fenced")
	}
}

// §15.4 item 3: a write dropped without committing is settled by the
// status fence; replaying the dropped request with its old resourceVersion
// afterwards is a 409.
func TestEnvtest_PlannedFailoverStatusFence_DroppedThenReplayed409(t *testing.T) {
	nn := pfEnvGroup(t, "pf-dropped", map[string]string{controller.PlannedFailoverAnnotation: "dc2"})
	var dropped *v1alpha1.MysqlFailoverGroup
	topo := &pfEnvTopology{}
	r := pfEnvInterceptedReconciler(t, topo, func(_ context.Context, _ client.Client, obj client.Object, _ ...client.SubResourceUpdateOption) (bool, error) {
		if dropped != nil || !enginePFWrite(obj, v1alpha1.PlannedFailoverPhaseFencing) {
			return false, nil
		}
		dropped = obj.(*v1alpha1.MysqlFailoverGroup).DeepCopy()
		return true, context.DeadlineExceeded // never reached the server
	})
	pfEnvStepUntil(t, r, nn, 5, func(*v1alpha1.MysqlFailoverGroup) bool { return dropped != nil })
	if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
		t.Fatal(err)
	}
	if pfEnvGet(t, nn).Annotations[controller.PlannedFailoverStatusFenceAnnotation] == "" {
		t.Fatal("settlement did not fence the dropped write")
	}
	if err := k8sClient.Status().Update(ctx, dropped); !apierrors.IsConflict(err) {
		t.Fatalf("replaying the dropped write after settlement = %v, want 409", err)
	}
	pfEnvStepUntil(t, r, nn, 40, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover != nil && fg.Status.PlannedFailover.Result != nil
	})
}

// §15.4 item 5: the out-of-band record's conditioned writer refuses to
// replace a newer record with an older one, and stores a newer one.
func TestEnvtest_FailoverStateRecorder_ConditionedRefusesOlder(t *testing.T) {
	nn := pfEnvGroup(t, "pf-oob", nil)
	rec := controller.NewAnnotationFailoverStateRecorder(k8sClient, k8sClient, nn)
	t1 := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	newer := controller.FailoverRecord{LastFailover: t1.Add(time.Minute), LastFailoverTarget: "dc2", PromotionGtidExecuted: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42"}
	older := controller.FailoverRecord{LastFailover: t1, LastFailoverTarget: "dc1"}
	if err := rec.RecordFailoverState(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := rec.RecordFailoverState(ctx, older); err != nil {
		t.Fatalf("a refused older record must be a no-op success: %v", err)
	}
	got, err := controller.FailoverRecordFromAnnotations(pfEnvGet(t, nn).Annotations)
	if err != nil {
		t.Fatal(err)
	}
	if got.LastFailoverTarget != "dc2" || !got.LastFailover.Equal(newer.LastFailover) || got.PromotionGtidExecuted != newer.PromotionGtidExecuted {
		t.Fatalf("stored record %+v, want the newer one", got)
	}
	newest := controller.FailoverRecord{LastFailover: t1.Add(2 * time.Minute), LastFailoverTarget: "dc1"}
	if err := rec.RecordFailoverState(ctx, newest); err != nil {
		t.Fatal(err)
	}
	if got, _ := controller.FailoverRecordFromAnnotations(pfEnvGet(t, nn).Annotations); got.LastFailoverTarget != "dc1" {
		t.Fatalf("a newer record was not stored: %+v", got)
	}
}
