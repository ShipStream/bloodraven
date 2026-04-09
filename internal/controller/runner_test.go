package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
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
