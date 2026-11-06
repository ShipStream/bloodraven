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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// TestCRD_PlannedFailoverRoundTrip pins the final shape of
// status.plannedFailover: every field and enum value persists unpruned, the
// run identity and parameters are optional in the schema (rollback safety),
// and invalid phase, outcome,
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
		RunID:      "6f1d0c3a-5b8e-4c1f-9a2d-0e7b3c4d5e6f",
		Revision:   7,
		Phase:      v1alpha1.PlannedFailoverPhasePublishing,
		TargetSite: "dc2",
		Settings: v1alpha1.PlannedFailoverSettings{
			MaxLagWait:   metav1.Duration{Duration: 5 * time.Minute},
			DrainTimeout: metav1.Duration{Duration: 30 * time.Second},
			Dragonfly: &v1alpha1.PlannedFailoverDragonflySettings{
				MaxSyncWait: metav1.Duration{Duration: 30 * time.Second}, OnSyncTimeout: v1alpha1.DragonflySyncTimeoutProceed,
			},
		},
		StartTime:     ts(0),
		SourceSite:    "dc1",
		ClaimTime:     ptrTime(1),
		ClaimBaseline: &v1alpha1.FailoverRecordRef{Time: ptrTime(2), Target: "dc1"},
		Admission: &v1alpha1.PlannedFailoverAdmission{
			DeadlineTime: ptrTime(3), WaitReason: v1alpha1.PlannedFailoverWaitRecoveryInFlight,
			DeferReason: v1alpha1.PlannedFailoverDeferCooldownActive, RetryTime: ptrTime(4),
		},
		Fence: &v1alpha1.PlannedFailoverFenceRecord{
			State: v1alpha1.PlannedFailoverFenceApplied, IntentTime: ts(5), SourceServerUUID: "uuid-src",
			ApplyTime: ptrTime(6), GtidSet: &gtid,
		},
		Drain:   &v1alpha1.PlannedFailoverDrainRecord{StartTime: ts(7), CompletionTime: ptrTime(8), RemainingConnections: &remaining},
		LagWait: &v1alpha1.PlannedFailoverLagWaitRecord{StartTime: ts(9), CatchUpTime: ptrTime(10)},
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{
			SyncWaitStartTime: ptrTime(11),
			SourceOffsetState: v1alpha1.DragonflyOffsetCaptured,
			SourceOffset:      &srcOffset,
			SyncWaitSeconds:   &syncSecs,
			Takeover: &v1alpha1.DragonflyTakeoverRecord{
				State: v1alpha1.DragonflyTakeoverSucceeded, IntentTime: ts(12), CompletionTime: ptrTime(13),
				Method: v1alpha1.DragonflyTakeoverMethodReplTakeover, TargetOffset: &tgtOffset,
			},
			Sessions: v1alpha1.DragonflySessionsPreserved,
		},
		Promotion: &v1alpha1.PlannedFailoverPromotionRecord{
			State: v1alpha1.PlannedFailoverPromotionConfirmed, IntentTime: ts(14), TargetServerUUID: "uuid-tgt",
			ConfirmationTime: ptrTime(15),
		},
		DNS:      &v1alpha1.PlannedFailoverDNSRecord{State: v1alpha1.PlannedFailoverDNSFailed, AttemptTime: ts(16), LastError: "timeout"},
		Rollback: &v1alpha1.PlannedFailoverRollbackRecord{Reason: v1alpha1.PlannedFailoverReasonLagTimeout, IntentTime: ts(17)},
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

	// The run identity and parameters are not required by the schema: an
	// operator rolled back across this release writes the block back
	// without them, and requiring them would fail every one of its status
	// writes (TestCRD_PlannedFailoverRollbackWriteBack). The operator always
	// writes them and refuses a non-terminal block without a runID.
	for name, mutate := range map[string]func(pf *v1alpha1.PlannedFailoverStatus){
		"no runID":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.RunID = "" },
		"no revision":  func(pf *v1alpha1.PlannedFailoverStatus) { pf.Revision = 0 },
		"no phase":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.Phase = "" },
		"no target":    func(pf *v1alpha1.PlannedFailoverStatus) { pf.TargetSite = "" },
		"no settings":  func(pf *v1alpha1.PlannedFailoverStatus) { pf.Settings = v1alpha1.PlannedFailoverSettings{} },
		"no startTime": func(pf *v1alpha1.PlannedFailoverStatus) { pf.StartTime = metav1.Time{} },
	} {
		target := create("optional-" + strings.ToLower(strings.ReplaceAll(name, " ", "-")))
		pf := full.DeepCopy()
		mutate(pf)
		if err := writeStatus(target, pf); err != nil {
			t.Errorf("%s: status update error = %v; want accepted", name, err)
		}
	}

	// Invalid values are rejected server-side.
	invalid := map[string]func(pf *v1alpha1.PlannedFailoverStatus){
		"old phase":    func(pf *v1alpha1.PlannedFailoverStatus) { pf.Phase = "Validating" },
		"phase":        func(pf *v1alpha1.PlannedFailoverStatus) { pf.Phase = "Exploding" },
		"outcome":      func(pf *v1alpha1.PlannedFailoverStatus) { pf.Result.Outcome = "Partial" },
		"reason":       func(pf *v1alpha1.PlannedFailoverStatus) { pf.Result.Reason = "SourceCrashed" },
		"rollback":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.Rollback.Reason = "Whatever" },
		"wait reason":  func(pf *v1alpha1.PlannedFailoverStatus) { pf.Admission.WaitReason = "Sleeping" },
		"defer reason": func(pf *v1alpha1.PlannedFailoverStatus) { pf.Admission.DeferReason = "LagTimeout" },
		"warning": func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result.Warnings = []v1alpha1.PlannedFailoverWarning{"Spooky"}
		},
		"fence state":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.State = "Maybe" },
		"zero uuid":       func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.SourceServerUUID = "" },
		"revision":        func(pf *v1alpha1.PlannedFailoverStatus) { pf.Revision = -1 },
		"on sync":         func(pf *v1alpha1.PlannedFailoverStatus) { pf.Settings.Dragonfly.OnSyncTimeout = "proceed" },
		"df method":       func(pf *v1alpha1.PlannedFailoverStatus) { pf.Dragonfly.Takeover.Method = "REPLTAKEOVER" },
		"reason spelling": func(pf *v1alpha1.PlannedFailoverStatus) { pf.Result.Reason = "Cancelled" },
		"df takeover":     func(pf *v1alpha1.PlannedFailoverStatus) { pf.Dragonfly.Takeover.State = "Halfway" },
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

// TestCRD_PlannedFailoverRollbackWriteBack: an operator rolled back across
// #235 with the CRD left in place (a Helm rollback never downgrades CRDs)
// decodes status.plannedFailover into its older struct and writes the whole
// status back. That write must be accepted: the block loses the fields the
// older struct does not know, and the rest of status (activeSite, sites,
// conditions, the failover record) keeps updating.
func TestCRD_PlannedFailoverRollbackWriteBack(t *testing.T) {
	ns := createNamespace(t, "pf-rollback")
	fg := newTestFG(ns)
	fg.Name = "rollback"
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	start := metav1.NewTime(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		RunID: "run-1", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseSucceeded, TargetSite: "dc2", StartTime: start,
		Settings: v1alpha1.PlannedFailoverSettings{MaxLagWait: metav1.Duration{Duration: time.Minute}, DrainTimeout: metav1.Duration{Duration: time.Second}},
		Result:   &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeSucceeded, CompletionTime: start},
		Message:  "done",
	}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}

	// The pre-#235 struct: phase, target, sourcePrimary, startTime,
	// completionTime, reason, message (no runID, revision, targetSite,
	// settings or result).
	var u unstructured.Unstructured
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &u); err != nil {
		t.Fatal(err)
	}
	old := map[string]any{
		"phase": "Succeeded", "target": "dc2", "sourcePrimary": "dc1", "startTime": "2026-11-04T12:00:00Z",
		"completionTime": "2026-11-04T12:00:00Z", "message": "done",
	}
	if err := unstructured.SetNestedField(u.Object, old, "status", "plannedFailover"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(u.Object, "dc2", "status", "activeSite"); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Status().Update(ctx, &u); err != nil {
		t.Fatalf("an older operator's status write-back was rejected: %v", err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ActiveSite != "dc2" || got.Status.PlannedFailover == nil || got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseSucceeded ||
		got.Status.PlannedFailover.RunID != "" {
		t.Fatalf("status after the write-back = activeSite %q plannedFailover %+v", got.Status.ActiveSite, got.Status.PlannedFailover)
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

// TestCRD_PlannedFailoverHoldRoundTrip pins status.plannedFailoverHold: every
// field persists unpruned, and a hold without its run id is rejected.
func TestCRD_PlannedFailoverHoldRoundTrip(t *testing.T) {
	ns := createNamespace(t, "pf-hold-roundtrip")
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	hold := &v1alpha1.PlannedFailoverHold{
		RunID: "6f1d0c3a-5b8e-4c1f-9a2d-0e7b3c4d5e6f", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		SourceSite: "dc1", TargetSite: "dc2", AmbiguousSites: []string{"dc1", "dc2"},
		HoldTime: metav1.NewTime(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)),
	}
	fg.Status.PlannedFailoverHold = hold.DeepCopy()
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("status update with a hold: %v", err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(got.Status.PlannedFailoverHold, hold) {
		t.Fatalf("plannedFailoverHold did not round-trip:\n got %+v\nwant %+v", got.Status.PlannedFailoverHold, hold)
	}
	got.Status.PlannedFailoverHold.RunID = ""
	if err := k8sClient.Status().Update(ctx, &got); !apierrors.IsInvalid(err) {
		t.Fatalf("a hold without runID: status update error = %v; want Invalid", err)
	}
}
