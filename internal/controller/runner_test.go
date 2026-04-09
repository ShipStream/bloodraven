package controller

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
			Message:            "At least one DC is writable",
		},
	}

	// Same status -> LastTransitionTime should be preserved.
	setCondition(&conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: newTime,
		Reason:             "TopologyPolled",
		Message:            "At least one DC is writable",
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
		Message:            "No DC is writable",
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
		Message:            "No cross-DC alerts",
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

	status := v1alpha1.MysqlReplicaPairStatus{
		PrimaryDC: "dc1",
		DC1: v1alpha1.DCInstanceStatus{
			State:    "writable",
			LastSeen: &lastSeen,
		},
		DC2: v1alpha1.DCInstanceStatus{
			State:    "read-only",
			LastSeen: &lastSeen,
		},
		Conditions: []metav1.Condition{
			{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "TopologyPolled",
				Message:            "At least one DC is writable",
			},
		},
		LastFailoverTarget: "dc1",
	}

	existing := status.DeepCopy()

	if !equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected identical statuses to be equal")
	}
}

func TestStatusDeepEqual_DifferentPrimaryDC(t *testing.T) {
	status := v1alpha1.MysqlReplicaPairStatus{
		PrimaryDC: "dc1",
		DC1:       v1alpha1.DCInstanceStatus{State: "writable"},
		DC2:       v1alpha1.DCInstanceStatus{State: "read-only"},
	}

	existing := status.DeepCopy()
	status.PrimaryDC = "dc2"

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different PrimaryDC to be unequal")
	}
}

func TestStatusDeepEqual_DifferentDCState(t *testing.T) {
	status := v1alpha1.MysqlReplicaPairStatus{
		PrimaryDC: "dc1",
		DC1:       v1alpha1.DCInstanceStatus{State: "writable"},
		DC2:       v1alpha1.DCInstanceStatus{State: "read-only"},
	}

	existing := status.DeepCopy()
	status.DC1.State = "unreachable"

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different DC1 state to be unequal")
	}
}

func TestStatusDeepEqual_DifferentConditionStatus(t *testing.T) {
	now := metav1.Now()
	status := v1alpha1.MysqlReplicaPairStatus{
		PrimaryDC: "dc1",
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
	status := v1alpha1.MysqlReplicaPairStatus{
		PrimaryDC: "dc1",
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
		WithStatusSubresource(&v1alpha1.MysqlReplicaPair{}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "gone-pair", Namespace: "default"}
	snap := TopologySnapshot{
		DC1Name:  "dc1",
		DC1State: state.StateWritable,
		DC2Name:  "dc2",
		DC2State: state.StateReadOnly,
	}

	runner.updateCRStatus(context.Background(), nn, snap)
}

func TestUpdateCRStatus_ExistingCR(t *testing.T) {
	pair := newTestPair()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlReplicaPair{}).
		WithObjects(pair).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: pair.Name, Namespace: pair.Namespace}
	snap := TopologySnapshot{
		DC1Name:    "dc1",
		DC1State:   state.StateWritable,
		DC2Name:    "dc2",
		DC2State:   state.StateReadOnly,
		PrimaryDC:  "dc1",
	}

	runner.updateCRStatus(context.Background(), nn, snap)

	var updated v1alpha1.MysqlReplicaPair
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatalf("failed to get pair: %v", err)
	}
	if updated.Status.PrimaryDC != "dc1" {
		t.Errorf("expected PrimaryDC=dc1, got %q", updated.Status.PrimaryDC)
	}
}

func TestUpdateCRStatus_DeletedMidUpdate(t *testing.T) {
	pair := newTestPair()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlReplicaPair{}).
		WithObjects(pair).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: pair.Name, Namespace: pair.Namespace}

	if err := c.Delete(context.Background(), pair); err != nil {
		t.Fatalf("failed to delete pair: %v", err)
	}

	snap := TopologySnapshot{
		DC1Name:  "dc1",
		DC1State: state.StateWritable,
		DC2Name:  "dc2",
		DC2State: state.StateReadOnly,
	}

	runner.updateCRStatus(context.Background(), nn, snap)
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
		WithStatusSubresource(&v1alpha1.MysqlReplicaPair{}).
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
	pair := newTestPair()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlReplicaPair{}).
		WithObjects(pair).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: pair.Name, Namespace: pair.Namespace}
	snap := TopologySnapshot{
		DC1Name:   "dc1",
		DC1State:  state.StateWritable,
		DC2Name:   "dc2",
		DC2State:  state.StateReadOnly,
		PrimaryDC: "dc1",
	}

	runner.updateCRStatus(context.Background(), nn, snap)

	var updated v1alpha1.MysqlReplicaPair
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatalf("failed to get pair: %v", err)
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
