package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

func TestUpdateController_ExecuteOrder(t *testing.T) {
	logger := testutil.TestLogger()
	failoverCtl := NewFailoverController(logger)

	replicaMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:           true,
			SQLRunning:          true,
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	var updateOrder []string
	var mu sync.Mutex
	applyUpdate := func(_ context.Context, dcName string) error {
		mu.Lock()
		updateOrder = append(updateOrder, dcName)
		mu.Unlock()
		return nil
	}

	err := uc.Execute(context.Background(), "dc1", "dc2", replicaMySQL, primaryMySQL, applyUpdate)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify update order: replica first, then old primary
	if len(updateOrder) != 2 {
		t.Fatalf("expected 2 updates, got %d: %v", len(updateOrder), updateOrder)
	}
	if updateOrder[0] != "dc2" {
		t.Errorf("first update should be replica (dc2), got %s", updateOrder[0])
	}
	if updateOrder[1] != "dc1" {
		t.Errorf("second update should be old primary (dc1), got %s", updateOrder[1])
	}
}

func TestUpdateController_IsUpdating(t *testing.T) {
	logger := testutil.TestLogger()
	failoverCtl := NewFailoverController(logger)

	replicaMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:           true,
			SQLRunning:          true,
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	if uc.IsUpdating() {
		t.Error("should not be updating before Execute")
	}

	// Track IsUpdating during execute via the applyUpdate callback
	var wasUpdating bool
	applyUpdate := func(_ context.Context, _ string) error {
		wasUpdating = uc.IsUpdating()
		return nil
	}

	err := uc.Execute(context.Background(), "dc1", "dc2", replicaMySQL, primaryMySQL, applyUpdate)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if !wasUpdating {
		t.Error("IsUpdating should be true during Execute")
	}
	if uc.IsUpdating() {
		t.Error("should not be updating after Execute completes")
	}
}

func TestUpdateController_ConcurrentReject(t *testing.T) {
	logger := testutil.TestLogger()
	failoverCtl := NewFailoverController(logger)

	replicaMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:           true,
			SQLRunning:          true,
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	// Block the first Execute in the applyUpdate callback
	blockCh := make(chan struct{})
	firstStarted := make(chan struct{})

	applyFirst := func(_ context.Context, _ string) error {
		select {
		case firstStarted <- struct{}{}:
		default:
		}
		<-blockCh
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- uc.Execute(context.Background(), "dc1", "dc2", replicaMySQL, primaryMySQL, applyFirst)
	}()

	// Wait for first Execute to start
	<-firstStarted

	// Try second Execute - should be rejected
	err := uc.Execute(context.Background(), "dc1", "dc2", replicaMySQL, primaryMySQL,
		func(_ context.Context, _ string) error { return nil })
	if err == nil {
		t.Error("concurrent Execute should return error")
	}

	// Unblock first Execute
	close(blockCh)
	if err := <-errCh; err != nil {
		t.Fatalf("first Execute returned error: %v", err)
	}
}

func TestUpdateController_PhaseProgression(t *testing.T) {
	logger := testutil.TestLogger()
	failoverCtl := NewFailoverController(logger)

	replicaMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:           true,
			SQLRunning:          true,
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	if uc.Phase() != UpdatePhaseNone {
		t.Errorf("initial phase should be None, got %s", uc.Phase())
	}

	var phases []UpdatePhase
	applyUpdate := func(_ context.Context, _ string) error {
		phases = append(phases, uc.Phase())
		return nil
	}

	err := uc.Execute(context.Background(), "dc1", "dc2", replicaMySQL, primaryMySQL, applyUpdate)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// The applyUpdate callback is called during UpdateReplica and UpdateOldPrimary phases
	if len(phases) != 2 {
		t.Fatalf("expected 2 phase captures, got %d: %v", len(phases), phases)
	}
	if phases[0] != UpdatePhaseUpdateReplica {
		t.Errorf("first apply should be in UpdateReplica phase, got %s", phases[0])
	}
	if phases[1] != UpdatePhaseUpdateOldPrimary {
		t.Errorf("second apply should be in UpdateOldPrimary phase, got %s", phases[1])
	}

	// After completion, phase should be None
	if uc.Phase() != UpdatePhaseNone {
		t.Errorf("phase after Execute should be None, got %s", uc.Phase())
	}
}

func TestUpdateController_ApplyUpdateError(t *testing.T) {
	logger := testutil.TestLogger()
	failoverCtl := NewFailoverController(logger)

	replicaMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:           true,
			SQLRunning:          true,
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	applyUpdate := func(_ context.Context, dcName string) error {
		if dcName == "dc2" {
			return fmt.Errorf("simulated deploy error")
		}
		return nil
	}

	err := uc.Execute(context.Background(), "dc1", "dc2", replicaMySQL, primaryMySQL, applyUpdate)
	if err == nil {
		t.Fatal("Execute should return error when applyUpdate fails")
	}

	// After error, should not be in updating state
	if uc.IsUpdating() {
		t.Error("should not be updating after error")
	}
}

func TestUpdateController_FailoverExecuted(t *testing.T) {
	logger := testutil.TestLogger()
	failoverCtl := NewFailoverController(logger)

	replicaMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:           true,
			SQLRunning:          true,
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	applyUpdate := func(_ context.Context, _ string) error { return nil }

	err := uc.Execute(context.Background(), "dc1", "dc2", replicaMySQL, primaryMySQL, applyUpdate)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify failover was executed on the replica (candidate)
	calls := replicaMySQL.GetCalls()
	hasStopReplica := false
	hasResetReplicaAll := false
	for _, c := range calls {
		if c == "StopReplica" {
			hasStopReplica = true
		}
		if c == "ResetReplicaAll" {
			hasResetReplicaAll = true
		}
	}
	if !hasStopReplica {
		t.Error("failover should have called StopReplica on replica")
	}
	if !hasResetReplicaAll {
		t.Error("failover should have called ResetReplicaAll on replica")
	}

	// Verify old primary was fenced
	primaryCalls := primaryMySQL.GetCalls()
	hasFence := false
	for _, c := range primaryCalls {
		if c == "SetSuperReadOnly(ON)" {
			hasFence = true
		}
	}
	if !hasFence {
		t.Error("failover should have fenced old primary with SetSuperReadOnly(ON)")
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}
