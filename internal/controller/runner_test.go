package controller

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

func TestSetCondition_PreservesLastTransitionTime(t *testing.T) {
	oldTime := metav1.NewTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	newTime := metav1.NewTime(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC))

	conditions := []metav1.Condition{
		{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: oldTime,
			Reason:             "TopologyPolled",
			Message:            "At least one site is writable",
		},
	}

	// Same status -> LastTransitionTime should be preserved.
	setCondition(&conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: newTime,
		Reason:             "TopologyPolled",
		Message:            "At least one site is writable",
	})

	if !conditions[0].LastTransitionTime.Equal(&oldTime) {
		t.Errorf("expected LastTransitionTime to be preserved as %v, got %v",
			oldTime, conditions[0].LastTransitionTime)
	}

	// Different status -> LastTransitionTime should change.
	setCondition(&conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: newTime,
		Reason:             "TopologyPolled",
		Message:            "No site is writable",
	})

	if !conditions[0].LastTransitionTime.Equal(&newTime) {
		t.Errorf("expected LastTransitionTime to be updated to %v, got %v",
			newTime, conditions[0].LastTransitionTime)
	}
}

func TestSetCondition_AddsNewCondition(t *testing.T) {
	var conditions []metav1.Condition

	now := metav1.Now()
	setCondition(&conditions, metav1.Condition{
		Type:               "Degraded",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: now,
		Reason:             "Healthy",
		Message:            "No cross-site alerts",
	})

	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}
	if conditions[0].Type != "Degraded" {
		t.Errorf("expected type Degraded, got %s", conditions[0].Type)
	}
}

func TestStatusDeepEqual_IdenticalStatuses(t *testing.T) {
	now := metav1.Now()
	lastSeen := metav1.NewTime(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))

	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Sites: []v1alpha1.SiteStatus{
			{
				Name:     "dc1",
				State:    "writable",
				LastSeen: &lastSeen,
			},
			{
				Name:     "dc2",
				State:    "read-only",
				LastSeen: &lastSeen,
			},
		},
		Conditions: []metav1.Condition{
			{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "TopologyPolled",
				Message:            "At least one site is writable",
			},
		},
		LastFailoverTarget: "dc1",
	}

	existing := status.DeepCopy()

	if !equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected identical statuses to be equal")
	}
}

func TestStatusDeepEqual_DifferentActiveSite(t *testing.T) {
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", State: "writable"},
			{Name: "dc2", State: "read-only"},
		},
	}

	existing := status.DeepCopy()
	status.ActiveSite = "dc2"

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different ActiveSite to be unequal")
	}
}

func TestStatusDeepEqual_DifferentSiteState(t *testing.T) {
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", State: "writable"},
			{Name: "dc2", State: "read-only"},
		},
	}

	existing := status.DeepCopy()
	status.Sites[0].State = "unreachable"

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different Sites[0] state to be unequal")
	}
}

func TestStatusDeepEqual_DifferentConditionStatus(t *testing.T) {
	now := metav1.Now()
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Conditions: []metav1.Condition{
			{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "TopologyPolled",
			},
		},
	}

	existing := status.DeepCopy()
	status.Conditions[0].Status = metav1.ConditionFalse

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different condition status to be unequal")
	}
}

func TestStatusDeepEqual_LastFailoverChange(t *testing.T) {
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
	}

	existing := status.DeepCopy()
	failoverTime := metav1.NewTime(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))
	status.LastFailover = &failoverTime

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected status with new LastFailover to be unequal")
	}
}

func TestUpdateCRStatus_IsNotFound_NoPanic(t *testing.T) {
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "gone-fg", Namespace: "default"}
	snap := TopologySnapshot{
		SiteNames:  [2]string{"dc1", "dc2"},
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateReadOnly},
	}

	runner.updateCRStatus(context.Background(), nn, snap)
}

func TestUpdateCRStatus_ExistingCR(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		SiteNames:  [2]string{"dc1", "dc2"},
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateReadOnly},
		ActiveSite: "dc1",
	}

	runner.updateCRStatus(context.Background(), nn, snap)

	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatalf("failed to get fg: %v", err)
	}
	if updated.Status.ActiveSite != "dc1" {
		t.Errorf("expected ActiveSite=dc1, got %q", updated.Status.ActiveSite)
	}
}

func TestUpdateCRStatus_DeletedMidUpdate(t *testing.T) {
	// CR exists when Get is called but is deleted before Status().Update.
	// Uses SubResourceUpdate interceptor to simulate mid-update deletion.
	fg := newTestFG()
	scheme := testScheme()

	deleteCalled := false
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, underlying client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if !deleteCalled {
					deleteCalled = true
					_ = underlying.Delete(ctx, fg)
				}
				return underlying.Status().Update(ctx, obj, opts...)
			},
		}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		SiteNames:  [2]string{"dc1", "dc2"},
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateReadOnly},
	}

	// Must not panic even though CR is deleted mid-update.
	runner.updateCRStatus(context.Background(), nn, snap)

	if !deleteCalled {
		t.Error("expected interceptor to be called for SubResourceUpdate")
	}
}

func TestStopManager(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runner := &TopologyManagerRunner{
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "test-pair", Namespace: "default"}
	_, cancel := context.WithCancel(context.Background())
	runner.managers[nn] = &managedTopology{
		cancel: cancel,
	}

	if len(runner.managers) != 1 {
		t.Fatalf("expected 1 manager, got %d", len(runner.managers))
	}

	runner.StopManager(nn)

	if len(runner.managers) != 0 {
		t.Errorf("expected 0 managers after StopManager, got %d", len(runner.managers))
	}

	runner.StopManager(nn)
}

func TestStopManager_NotFound(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runner := &TopologyManagerRunner{
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "nonexistent", Namespace: "default"}
	runner.StopManager(nn)
}

func TestSync_RemovesDeletedCRManagers(t *testing.T) {
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "stale-pair", Namespace: "default"}
	_, cancel := context.WithCancel(context.Background())
	runner.managers[nn] = &managedTopology{
		cancel: cancel,
		cfg:    TopologyConfig{},
	}

	if err := runner.sync(context.Background()); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	if len(runner.managers) != 0 {
		t.Errorf("expected 0 managers after sync, got %d", len(runner.managers))
	}
}

func TestUpdateCRStatus_SetsConditions(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		SiteNames:  [2]string{"dc1", "dc2"},
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateReadOnly},
		ActiveSite: "dc1",
	}

	runner.updateCRStatus(context.Background(), nn, snap)

	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatalf("failed to get fg: %v", err)
	}

	foundReady := false
	for _, cond := range updated.Status.Conditions {
		if cond.Type == "Ready" {
			foundReady = true
			if cond.Status != metav1.ConditionTrue {
				t.Errorf("expected Ready=True, got %s", cond.Status)
			}
		}
	}
	if !foundReady {
		t.Error("Ready condition not found in status")
	}
}

func drainRunnerEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestEmitFailoverEvents_FailoverExecuted(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		LastFailoverTarget: "",
	}
	snap := TopologySnapshot{
		LastFailoverTarget: "dc2",
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "FailoverExecuted") {
		t.Errorf("want FailoverExecuted event, got %s", events[0])
	}
	if !strings.Contains(events[0], "dc2") {
		t.Errorf("want event to mention dc2, got %s", events[0])
	}
}

func TestEmitFailoverEvents_NoEventOnSameTarget(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		LastFailoverTarget: "dc2",
	}
	snap := TopologySnapshot{
		LastFailoverTarget: "dc2",
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events when target unchanged, got %v", events)
	}
}

func TestEmitFailoverEvents_DataLossDetected(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", RecoveryState: ""},
			{Name: "dc2", RecoveryState: ""},
		},
	}
	snap := TopologySnapshot{
		RecoveryState:     "RecoveryBlocked",
		RecoverySite:      "dc1",
		DivergentTxnCount: 5,
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "DataLossDetected") {
		t.Errorf("want DataLossDetected event, got %s", events[0])
	}
	if !strings.Contains(events[0], "5 divergent") {
		t.Errorf("want event to mention 5 divergent transactions, got %s", events[0])
	}
}

func TestEmitFailoverEvents_NoDataLossEventWhenAlreadyBlocked(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", RecoveryState: "RecoveryBlocked"},
			{Name: "dc2", RecoveryState: ""},
		},
	}
	snap := TopologySnapshot{
		RecoveryState:     "RecoveryBlocked",
		RecoverySite:      "dc1",
		DivergentTxnCount: 5,
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events when already blocked, got %v", events)
	}
}

func TestEmitFailoverEvents_RecoveryComplete(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", RecoveryState: "RecoveryBlocked"},
			{Name: "dc2", RecoveryState: ""},
		},
	}
	snap := TopologySnapshot{
		RecoveryState: "",
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "RecoveryComplete") {
		t.Errorf("want RecoveryComplete event, got %s", events[0])
	}
	if !strings.Contains(events[0], "dc1") {
		t.Errorf("want event to mention dc1, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_SplitBrain(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Conditions: []metav1.Condition{
			{Type: "Degraded", Status: metav1.ConditionFalse, Reason: "Healthy"},
		},
	}
	snap := TopologySnapshot{
		Alert:      "SPLIT BRAIN: both sites are writable",
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateWritable},
	}

	runner.emitDegradedTransitionEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "SplitBrainDetected") {
		t.Errorf("want SplitBrainDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_NoPrimary(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Conditions: []metav1.Condition{
			{Type: "Degraded", Status: metav1.ConditionFalse, Reason: "Healthy"},
		},
	}
	snap := TopologySnapshot{
		Alert:      "NO PRIMARY: both sites are read-only",
		SiteStates: [2]state.SiteState{state.StateReadOnly, state.StateReadOnly},
	}

	runner.emitDegradedTransitionEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "NoPrimaryDetected") {
		t.Errorf("want NoPrimaryDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_TotalLoss(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Conditions: []metav1.Condition{
			{Type: "Degraded", Status: metav1.ConditionFalse, Reason: "Healthy"},
		},
	}
	snap := TopologySnapshot{
		Alert:      "TOTAL LOSS: both sites are unreachable",
		SiteStates: [2]state.SiteState{state.StateUnreachable, state.StateUnreachable},
	}

	runner.emitDegradedTransitionEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "TotalLossDetected") {
		t.Errorf("want TotalLossDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_SiteRecovered(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Conditions: []metav1.Condition{
			{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "SplitBrain"},
		},
	}
	snap := TopologySnapshot{
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateReadOnly},
	}

	runner.emitDegradedTransitionEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "SiteRecovered") {
		t.Errorf("want SiteRecovered event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_NoEventOnSameReason(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Conditions: []metav1.Condition{
			{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "SplitBrain"},
		},
	}
	snap := TopologySnapshot{
		Alert:      "SPLIT BRAIN: both sites are writable",
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateWritable},
	}

	runner.emitDegradedTransitionEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events when reason unchanged, got %v", events)
	}
}

func TestEmitDegradedTransitionEvents_TransitionBetweenAlerts(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Conditions: []metav1.Condition{
			{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "SplitBrain"},
		},
	}
	snap := TopologySnapshot{
		Alert:      "TOTAL LOSS: both sites are unreachable",
		SiteStates: [2]state.SiteState{state.StateUnreachable, state.StateUnreachable},
	}

	runner.emitDegradedTransitionEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "TotalLossDetected") {
		t.Errorf("want TotalLossDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_NoRecoveryEventOnFreshCR(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{}
	snap := TopologySnapshot{
		SiteStates: [2]state.SiteState{state.StateWritable, state.StateReadOnly},
	}

	runner.emitDegradedTransitionEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events on fresh CR becoming healthy, got %v", events)
	}
}

func TestEmitFailoverEvents_NilRecorder(t *testing.T) {
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: nil}

	existing := &v1alpha1.MysqlFailoverGroupStatus{}
	snap := TopologySnapshot{LastFailoverTarget: "dc2"}

	// Must not panic.
	runner.emitFailoverEvents(fg, existing, snap)
}

func TestUpdateCRStatus_EmitsFailoverEvent(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	rec := record.NewFakeRecorder(10)

	runner := &TopologyManagerRunner{
		client:   c,
		recorder: rec,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	failoverTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	snap := TopologySnapshot{
		SiteNames:          [2]string{"dc1", "dc2"},
		SiteStates:         [2]state.SiteState{state.StateUnreachable, state.StateWritable},
		ActiveSite:         "dc2",
		LastFailover:       failoverTime,
		LastFailoverTarget: "dc2",
	}

	runner.updateCRStatus(context.Background(), nn, snap)
	events := drainRunnerEvents(rec)

	found := false
	for _, e := range events {
		if strings.Contains(e, "FailoverExecuted") {
			found = true
		}
	}
	if !found {
		t.Errorf("want FailoverExecuted event from updateCRStatus, got %v", events)
	}
}
