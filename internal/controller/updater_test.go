package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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
			SourceHost:          "dc1",
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	var updateOrder []string
	var mu sync.Mutex
	applyUpdate := func(_ context.Context, siteName string) error {
		mu.Lock()
		updateOrder = append(updateOrder, siteName)
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
			SourceHost:          "dc1",
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
			SourceHost:          "dc1",
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
			SourceHost:          "dc1",
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
			SourceHost:          "dc1",
			SecondsBehindSource: int64Ptr(0),
		},
	}
	primaryMySQL := &testutil.FakeMySQL{
		ReadOnlyVal: false,
	}

	uc := NewUpdateController(failoverCtl, logger)

	applyUpdate := func(_ context.Context, siteName string) error {
		if siteName == "dc2" {
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
			SourceHost:          "dc1",
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

// TestUpdateController_PreconditionRejectsWritableStandby verifies issue #46 fix:
// an ordered update must not start when the standby is writable with no replication.
func TestUpdateController_PreconditionRejectsWritableStandby(t *testing.T) {
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)

	// Standby reports writable — classic stale/unreconfigured pod.
	standby := &testutil.FakeMySQL{ReadOnlyVal: false}
	active := &testutil.FakeMySQL{ReadOnlyVal: false}

	var applyCalls int
	applyUpdate := func(_ context.Context, _ string) error {
		applyCalls++
		return nil
	}

	err := uc.Execute(context.Background(), "dc1", "dc2", standby, active, applyUpdate)
	if err == nil {
		t.Fatal("Execute should reject a writable standby")
	}
	if !strings.Contains(err.Error(), "precondition") {
		t.Errorf("expected precondition error, got: %v", err)
	}
	if applyCalls != 0 {
		t.Errorf("applyUpdate must not be called on precondition failure, got %d calls", applyCalls)
	}
	if uc.IsUpdating() {
		t.Error("IsUpdating must be false after precondition abort")
	}
}

// TestUpdateController_PreconditionRejectsStoppedReplication covers issue #46 case
// where super_read_only=ON but replication threads are not actually running.
func TestUpdateController_PreconditionRejectsStoppedReplication(t *testing.T) {
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)

	standby := &testutil.FakeMySQL{
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:  false,
			SQLRunning: false,
		},
	}
	active := &testutil.FakeMySQL{ReadOnlyVal: false}

	err := uc.Execute(context.Background(), "dc1", "dc2", standby, active,
		func(_ context.Context, _ string) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "not replicating") {
		t.Fatalf("expected 'not replicating' precondition error, got: %v", err)
	}
	if uc.IsUpdating() {
		t.Error("IsUpdating must be false after precondition abort")
	}
}

// TestUpdateController_PreconditionTolerates ProbeError ensures a transient MySQL
// probe error does not add a new reason to fail — existing behaviour preserved.
func TestUpdateController_PreconditionToleratesProbeError(t *testing.T) {
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = 5 * time.Millisecond

	// Probe errors on first CheckReadOnly; subsequent calls in waitForReplicaReady
	// see a healthy replica, so Execute proceeds through to completion.
	standby := &testutil.FakeMySQL{
		Err:         errors.New("conn refused"),
		ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{
			IORunning:           true,
			SQLRunning:          true,
			SourceHost:          "active",
			SecondsBehindSource: int64Ptr(0),
		},
	}
	active := &testutil.FakeMySQL{ReadOnlyVal: false}

	// Clear the error after precondition probe so waitForReplicaReady succeeds.
	go func() {
		time.Sleep(2 * time.Millisecond)
		standby.SetError(nil)
	}()

	err := uc.Execute(context.Background(), "dc1", "dc2", standby, active,
		func(_ context.Context, _ string) error { return nil })
	if err != nil {
		t.Fatalf("probe error must not block Execute, got: %v", err)
	}
}

// TestWaitForReplicaReady_FailFastOnWritableStandby verifies fail-fast aborts in
// ~30s-equivalent of ticks rather than running out the full 5-minute deadline.
func TestWaitForReplicaReady_FailFastOnWritableStandby(t *testing.T) {
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = 5 * time.Millisecond
	uc.failFastDuration = 30 * time.Millisecond

	// Writable with no replication source — will never become healthy.
	checker := &testutil.FakeMySQL{
		ReadOnlyVal:      false,
		ReplicaStatusVal: &mysql.ReplicaStatus{}, // empty — no SourceHost
	}

	start := time.Now()
	err := uc.waitForReplicaReady(context.Background(), checker, 5*time.Minute)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("waitForReplicaReady should abort on writable standby")
	}
	if !strings.Contains(err.Error(), "writable") {
		t.Errorf("expected writable abort error, got: %v", err)
	}
	// Derived threshold (30ms/5ms) × probe time; cap at 1s to catch regression
	// back to 5-min deadline.
	if elapsed > time.Second {
		t.Errorf("fail-fast took too long: %v (expected well under 1s)", elapsed)
	}
}

// TestWaitForReplicaReady_CounterResetsOnProbeError ensures a flaky connection
// doesn't trigger the writable-abort path (unreachable != writable).
func TestWaitForReplicaReady_CounterResetsOnProbeError(t *testing.T) {
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = 2 * time.Millisecond
	uc.failFastDuration = 12 * time.Millisecond // threshold = 6 ticks

	checker := &flappingChecker{
		writable:   true,
		errorEvery: 3, // error every 3rd tick — max 2 consecutive writable
	}

	// Outer deadline short enough to bail out of the test; fail-fast should NOT
	// fire because the counter resets on each probe error.
	start := time.Now()
	err := uc.waitForReplicaReady(context.Background(), checker, 200*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected deadline timeout (not writable abort)")
	}
	if strings.Contains(err.Error(), "writable") {
		t.Errorf("counter should have reset on probe errors; got writable abort: %v", err)
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("expected timeout error, got: %v", err)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("expected to hit the deadline, exited too early: %v", elapsed)
	}
}

// flappingChecker is a minimal mysql.Checker that alternates writable responses
// with probe errors to verify the fail-fast counter's reset-on-error behaviour.
type flappingChecker struct {
	mu         sync.Mutex
	writable   bool
	errorEvery int
	tick       int
}

func (f *flappingChecker) CheckReadOnly(_ context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tick++
	if f.errorEvery > 0 && f.tick%f.errorEvery == 0 {
		return false, errors.New("conn refused")
	}
	return !f.writable, nil
}

func (f *flappingChecker) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	return &mysql.ReplicaStatus{}, nil
}

func (f *flappingChecker) Promote(_ context.Context) error                  { return nil }
func (f *flappingChecker) SetSuperReadOnly(_ context.Context, _ bool) error { return nil }
func (f *flappingChecker) SetReadOnly(_ context.Context, _ bool) error      { return nil }
func (f *flappingChecker) StopReplica(_ context.Context) error              { return nil }
func (f *flappingChecker) ResetReplicaAll(_ context.Context) error          { return nil }
func (f *flappingChecker) ChangeReplicationSource(_ context.Context, _ mysql.ReplicationSourceOpts) error {
	return nil
}
func (f *flappingChecker) StartReplica(_ context.Context) error          { return nil }
func (f *flappingChecker) StartReplicaSQLThread(_ context.Context) error { return nil }
func (f *flappingChecker) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	return nil
}
func (f *flappingChecker) SetCloneDonorList(_ context.Context, _ string) error { return nil }
func (f *flappingChecker) GetGtidExecuted(_ context.Context) (string, error)   { return "", nil }
func (f *flappingChecker) KillAppConnections(_ context.Context) (int, error)   { return 0, nil }
func (f *flappingChecker) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
	return nil
}
func (f *flappingChecker) Close() error { return nil }
