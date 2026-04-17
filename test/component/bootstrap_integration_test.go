package component

import (
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// newBootstrapHarness creates a test harness with auto-bootstrap enabled.
func newBootstrapHarness(t *testing.T, dc1, dc2 *mockMySQL) *testHarness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	bc := controller.NewBootstrapController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	cfg := controller.TopologyConfig{
		Name:              "lion",
		Sites:             defaultTwoSiteConfig(),
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
	bootCfg := controller.BootstrapConfig{
		ReplUser:     "replicator",
		ReplPassword: "secret",
		UseSSL:       false,
		CloneTimeout: 30 * time.Second,
	}

	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, bc, bootCfg, tainter, hub, dns, logger, clk)

	return &testHarness{
		tm:       tm,
		dc1MySQL: dc1,
		dc2MySQL: dc2,
		tainter:  tainter,
		dns:      dns,
		hub:      hub,
		logger:   logger,
		clock:    clk,
	}
}

// waitForPhase polls the manager's bootstrap phase until it matches want or timeout.
func waitForPhase(t *testing.T, tm *controller.TopologyManager, want controller.BootstrapPhase, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tm.BootstrapPhase() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for bootstrap phase %q, current = %q", want, tm.BootstrapPhase())
}

// TestFreshDeploy_TriggersBootstrap: both sites writable with no replication
// history should trigger an auto-bootstrap and end with replication configured
// on the replica.
func TestFreshDeploy_TriggersBootstrap(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false} // writable, no replica status
	dc2 := &mockMySQL{readOnly: false} // writable, no replica status
	h := newBootstrapHarness(t, dc1, dc2)

	// Drive poll loop until split-brain is detected (state transition from
	// StateUnknown -> StateWritable requires recoveryThreshold=2 polls).
	h.pollN(3)

	// Bootstrap should have been kicked off — wait for it to complete.
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)

	// Clone should have been called on dc2 (the replica by convention).
	dc2.mu.Lock()
	cloneCalled := dc2.cloneInstanceCalled
	donorList := dc2.cloneDonorList
	replSourceSet := dc2.replicationSourceSet
	replStarted := dc2.replicaStarted
	replOpts := dc2.changeReplicationOpts
	dc2.mu.Unlock()

	if !cloneCalled {
		t.Error("expected CloneInstance to be called on dc2")
	}
	if donorList == "" {
		t.Error("expected SetCloneDonorList to be called on dc2")
	}
	if !replSourceSet {
		t.Error("expected ChangeReplicationSource to be called on dc2")
	}
	if !replStarted {
		t.Error("expected StartReplica to be called on dc2")
	}
	if replOpts.Host != "mysql-lion-dc1.default.svc.cluster.local" {
		t.Errorf("expected replication source host dc1 service, got %q", replOpts.Host)
	}
	if replOpts.User != "replicator" {
		t.Errorf("expected repl user replicator, got %q", replOpts.User)
	}

	// Clone should NOT have been called on dc1 (the primary).
	dc1.mu.Lock()
	if dc1.cloneInstanceCalled {
		t.Error("CloneInstance should not be called on the primary")
	}
	dc1.mu.Unlock()
}

// TestFreshDeploy_NoReplCredentials_NoBootstrap: with no replication credentials,
// auto-bootstrap is disabled and the split-brain alert is just logged.
func TestFreshDeploy_NoReplCredentials_NoBootstrap(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: false}
	// Use the standard harness — no bootstrap controller wired.
	h := newTestHarnessWithMySQL(t, dc1, dc2)

	h.pollN(3)

	dc2.mu.Lock()
	cloneCalled := dc2.cloneInstanceCalled
	dc2.mu.Unlock()
	if cloneCalled {
		t.Error("bootstrap should not run when credentials are missing")
	}
}

// TestSplitBrain_WithPriorReplication_NoBootstrap: if one site previously had
// replication configured (non-nil ShowReplicaStatus), this is a true split-brain
// with potentially diverged data and must NOT be auto-bootstrapped.
func TestSplitBrain_WithPriorReplication_NoBootstrap(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	// dc2 previously replicated from dc1 but is now writable (diverged).
	dc2 := &mockMySQL{
		readOnly:      false,
		replicaStatus: &mysql.ReplicaStatus{IORunning: false, SQLRunning: false, LastError: "replication stopped"},
	}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(3)

	// Give any goroutine a moment just in case it was erroneously kicked off.
	time.Sleep(50 * time.Millisecond)

	dc2.mu.Lock()
	cloneCalled := dc2.cloneInstanceCalled
	dc2.mu.Unlock()
	if cloneCalled {
		t.Error("bootstrap should not run on true split-brain with prior replication")
	}
	if h.tm.BootstrapPhase() != controller.BootstrapPhaseNone {
		t.Errorf("expected BootstrapPhase none, got %q", h.tm.BootstrapPhase())
	}
}

// TestBootstrap_FailAndRetry: a clone failure marks the phase as Failed and
// leaves it retryable on the next split-brain detection.
func TestBootstrap_FailAndRetry(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: false, cloneInstanceErr: errors.New("disk full")}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(3)

	waitForPhase(t, h.tm, controller.BootstrapPhaseFailed, 2*time.Second)

	// Clear the clone error so the retry can succeed, and reset the called flag.
	dc2.mu.Lock()
	dc2.cloneInstanceErr = nil
	dc2.cloneInstanceCalled = false
	dc2.replicaStarted = false
	dc2.mu.Unlock()

	// A state transition is needed for applyCrossSiteAction to run again.
	// Flip one site read-only and back to trigger transitions.
	dc1.setReadOnly(true)
	h.pollN(3)
	dc1.setReadOnly(false)
	h.pollN(3)

	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)

	dc2.mu.Lock()
	cloneCalled := dc2.cloneInstanceCalled
	replStarted := dc2.replicaStarted
	dc2.mu.Unlock()
	if !cloneCalled {
		t.Error("expected retry to re-invoke CloneInstance")
	}
	if !replStarted {
		t.Error("expected retry to complete through SetupReplication")
	}
}

// TestBootstrap_ConnectionDropTreatedAsSuccess: a connection drop during clone
// (which is what MySQL produces on its post-clone auto-restart) is treated as
// expected success and the bootstrap proceeds to SetupReplication.
func TestBootstrap_ConnectionDropTreatedAsSuccess(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: false, cloneInstanceErr: errors.New("invalid connection")}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(3)

	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)

	dc2.mu.Lock()
	replStarted := dc2.replicaStarted
	dc2.mu.Unlock()
	if !replStarted {
		t.Error("expected SetupReplication to run after connection-drop clone error")
	}
}
