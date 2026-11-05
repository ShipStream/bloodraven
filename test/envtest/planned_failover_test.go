//go:build envtest

package envtest

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestCRD_PlannedFailoverRoundTrip_Additive pins the durable-claim fields of
// status.plannedFailover: every new field and enum value persists unpruned
// beside the legacy fields, a legacy-only block is still accepted, and
// invalid phase, outcome, reason and wait-reason values are rejected by the
// API server.
func TestCRD_PlannedFailoverRoundTrip_Additive(t *testing.T) {
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
		Settings: &v1alpha1.PlannedFailoverSettings{
			MaxLagWait:   metav1.Duration{Duration: 5 * time.Minute},
			DrainTimeout: metav1.Duration{Duration: 30 * time.Second},
			Dragonfly: &v1alpha1.PlannedFailoverDragonflySettings{
				MaxSyncWait: metav1.Duration{Duration: 30 * time.Second}, OnSyncTimeout: "proceed",
			},
		},
		StartTime:     ptrTime(0),
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
		// Legacy fields stay beside the new ones until the old shape is removed.
		SourcePrimary: "dc1",
		Message:       "published",
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

	// A legacy-only block (what today's operator writes) is still accepted.
	legacy := &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhaseValidating, Target: "dc2", StartTime: ptrTime(0), Reason: "CooldownActive",
	}
	if err := writeStatus(fg, legacy); err != nil {
		t.Fatalf("legacy block: %v", err)
	}
	if got := readBack(fg); !equality.Semantic.DeepEqual(got, legacy) {
		t.Fatalf("legacy block did not round-trip:\n got %+v\nwant %+v", got, legacy)
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
