package controller

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// --- Mock MySQL ---

type mockMySQL struct {
	mu       sync.Mutex
	readOnly bool
	err      error
	promoted bool
}

func (m *mockMySQL) CheckReadOnly(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly, m.err
}

func (m *mockMySQL) Promote(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.promoted = true
	m.readOnly = false
	return nil
}

func (m *mockMySQL) Close() error { return nil }

func (m *mockMySQL) SetSuperReadOnly(_ context.Context, _ bool) error { return nil }
func (m *mockMySQL) StopReplica(_ context.Context) error             { return nil }
func (m *mockMySQL) ResetReplicaAll(_ context.Context) error         { return nil }
func (m *mockMySQL) SetReadOnly(_ context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly = on
	return nil
}
func (m *mockMySQL) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	return nil, nil
}
func (m *mockMySQL) ChangeReplicationSource(_ context.Context, _ mysql.ReplicationSourceOpts) error {
	return nil
}
func (m *mockMySQL) StartReplica(_ context.Context) error { return nil }
func (m *mockMySQL) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	return nil
}
func (m *mockMySQL) SetCloneDonorList(_ context.Context, _ string) error { return nil }
func (m *mockMySQL) CloneInstance(_ context.Context, _, _, _ string, _ bool) error {
	return nil
}

type failingPromoteMySQL struct {
	*mockMySQL
}

func (f *failingPromoteMySQL) Promote(_ context.Context) error {
	return errors.New("promote failed")
}

// Override SetReadOnly so the FailoverController.Execute fails during promotion.
func (f *failingPromoteMySQL) SetReadOnly(_ context.Context, _ bool) error {
	return errors.New("promote failed")
}

func (m *mockMySQL) setReadOnly(ro bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly = ro
	m.err = nil
}

func (m *mockMySQL) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// --- Mock Tainter ---

type mockTainter struct {
	mu     sync.Mutex
	taints map[string]bool // zone -> tainted
}

func newMockTainter() *mockTainter {
	return &mockTainter{taints: make(map[string]bool)}
}

func (m *mockTainter) SetTaint(_ context.Context, zone string, taint bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taints[zone] = taint
	return nil
}

func (m *mockTainter) isTainted(zone string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taints[zone]
}

// --- Mock DNS ---

type mockDNS struct {
	mu     sync.Mutex
	lastIP string
	calls  int
	err    error
}

func (m *mockDNS) UpdateAZRecord(_ context.Context, ip string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.lastIP = ip
	m.calls++
	return nil
}

func (m *mockDNS) getLastIP() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIP
}

// --- Test helpers ---

func testTopologyConfig() TopologyConfig {
	return TopologyConfig{
		AZ:                "lion",
		DC1:               DCTopologyConfig{Name: "dc1", Zone: "lion-dc1", LBIP: "1.1.1.1"},
		DC2:               DCTopologyConfig{Name: "dc2", Zone: "lion-dc2", LBIP: "2.2.2.2"},
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func newTestTopologyManager(dc1, dc2 *mockMySQL) (*TopologyManager, *mockTainter, *mockDNS) {
	cfg := testTopologyConfig()
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	tm := NewTopologyManager(cfg, dc1, dc2, fc, tainter, hub, dns, testLogger())
	// Use a very short cooldown for tests so failovers aren't blocked.
	tm.failoverCooldown = 0
	return tm, tainter, dns
}

// pollN runs n poll cycles synchronously.
func pollN(tm *TopologyManager, n int) {
	ctx := context.Background()
	for i := 0; i < n; i++ {
		tm.Poll(ctx)
	}
}

// --- Tests ---

func TestNormalDC1Primary(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, tainter, _ := newTestTopologyManager(dc1, dc2)

	// Need RecoveryThreshold polls for dc1 to confirm writable
	pollN(tm, 2)

	if tainter.isTainted("lion-dc1") {
		t.Error("dc1 should not be tainted")
	}
	if !tainter.isTainted("lion-dc2") {
		t.Error("dc2 should be tainted")
	}

	s := tm.Status()
	if s.DC1State != "writable" {
		t.Errorf("dc1 state: got %s, want writable", s.DC1State)
	}
	if s.DC2State != "read-only" {
		t.Errorf("dc2 state: got %s, want read-only", s.DC2State)
	}
}

func TestFailoverDC1Down(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, tainter, dns := newTestTopologyManager(dc1, dc2)

	// Establish normal state
	pollN(tm, 2)

	// dc1 goes down
	dc1.setError(errors.New("connection refused"))

	// Need FailureThreshold polls for unreachable
	pollN(tm, 3)

	if !tainter.isTainted("lion-dc1") {
		t.Error("dc1 should be tainted after failure")
	}
	// FailoverController.Execute sets readOnly=false via SetReadOnly.
	dc2.mu.Lock()
	dc2RO := dc2.readOnly
	dc2.mu.Unlock()
	if dc2RO {
		t.Error("dc2 should have been promoted (readOnly should be false)")
	}

	// DNS flip is deferred until promotion is confirmed.
	// SetReadOnly(false) sets readOnly=false, but we need RecoveryThreshold polls to confirm.
	if dns.getLastIP() != "" {
		t.Error("DNS should not flip before promotion confirmation")
	}

	// Poll to confirm promotion (recovery threshold = 2 polls of read_only=0)
	pollN(tm, 2)

	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should point to dc2 after confirmation, got %s", dns.getLastIP())
	}
}

func TestPromotionNotRepeated(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(dc1, dc2)

	// Establish normal state
	pollN(tm, 2)

	// dc1 goes down
	dc1.setError(errors.New("connection refused"))
	pollN(tm, 3) // reach threshold, triggers promotion

	dc2.mu.Lock()
	dc2RO := dc2.readOnly
	dc2.mu.Unlock()
	if dc2RO {
		t.Fatal("dc2 should have been promoted (readOnly should be false)")
	}

	// Poll again while dc1 still down, dc2 recovering
	pollN(tm, 5)

	// DNS should only flip once (after confirmation)
	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()
	if calls > 1 {
		t.Errorf("DNS should flip at most once, got %d calls", calls)
	}
}

func TestPromotionFailureSkipsDNS(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(dc1, dc2)

	// Override dc2 to fail on promote
	failDC2 := &failingPromoteMySQL{mockMySQL: dc2}
	tm.dc2.mysql = failDC2

	pollN(tm, 2) // establish normal

	dc1.setError(errors.New("connection refused"))
	pollN(tm, 3) // trigger failover attempt

	// Promotion failed, so no DNS flip should happen
	pollN(tm, 5)
	if dns.getLastIP() != "" {
		t.Error("DNS should not flip when promotion fails")
	}
}

func TestReadiness(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(dc1, dc2)

	if tm.Ready() {
		t.Error("should not be ready before first poll")
	}

	pollN(tm, 1)

	if !tm.Ready() {
		t.Error("should be ready after first poll")
	}
}

func TestDebouncePreventsPrematureFailover(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(dc1, dc2)

	// Establish normal
	pollN(tm, 2)

	// Single failure should not trigger failover
	dc1.setError(errors.New("timeout"))
	pollN(tm, 1)

	if dns.getLastIP() != "" {
		t.Error("single failure should not trigger DNS flip")
	}

	// Recovery before threshold
	dc1.setReadOnly(false)
	pollN(tm, 2)

	if tm.dc1.state != state.StateWritable {
		t.Errorf("dc1 should recover to writable, got %s", tm.dc1.state)
	}
}

func TestRecoveryDebounce(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true}
	dc2 := &mockMySQL{readOnly: false}
	tm, tainter, _ := newTestTopologyManager(dc1, dc2)

	// Establish dc2 primary
	pollN(tm, 2)

	if !tainter.isTainted("lion-dc1") {
		t.Error("dc1 should be tainted (read-only)")
	}

	// dc1 becomes writable - needs RecoveryThreshold confirmations
	dc1.setReadOnly(false)
	pollN(tm, 1)

	// After 1 poll, still not recovered (threshold=2)
	if tm.dc1.state == state.StateWritable {
		t.Error("dc1 should not yet be writable after 1 recovery poll")
	}

	pollN(tm, 1)

	if tm.dc1.state != state.StateWritable {
		t.Errorf("dc1 should be writable after 2 recovery polls, got %s", tm.dc1.state)
	}
}

func TestSplitBrainNoAction(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: false}
	tm, _, dns := newTestTopologyManager(dc1, dc2)

	pollN(tm, 2)

	// Both writable = split brain, DNS should NOT be flipped
	if dns.getLastIP() != "" {
		t.Error("split brain should not flip DNS")
	}
}

func TestDoubleReadOnlyNoAction(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true}
	dc2 := &mockMySQL{readOnly: true}
	tm, tainter, dns := newTestTopologyManager(dc1, dc2)

	pollN(tm, 2)

	// Both read-only, both should be tainted, no DNS flip
	if !tainter.isTainted("lion-dc1") {
		t.Error("dc1 should be tainted")
	}
	if !tainter.isTainted("lion-dc2") {
		t.Error("dc2 should be tainted")
	}
	if dns.getLastIP() != "" {
		t.Error("double read-only should not flip DNS")
	}
}

func TestTotalLoss(t *testing.T) {
	dc1 := &mockMySQL{err: errors.New("down")}
	dc2 := &mockMySQL{err: errors.New("down")}
	tm, tainter, _ := newTestTopologyManager(dc1, dc2)

	pollN(tm, 3) // reach failure threshold

	if !tainter.isTainted("lion-dc1") {
		t.Error("dc1 should be tainted")
	}
	if !tainter.isTainted("lion-dc2") {
		t.Error("dc2 should be tainted")
	}
}

func TestTopologyManagerRunCancellation(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(dc1, dc2)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tm.Run(ctx)
		close(done)
	}()

	// Let it run a few cycles
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("topology manager did not stop after context cancellation")
	}
}
