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
	mu               sync.Mutex
	readOnly         bool
	err              error
	promoted         bool
	replicaStatusVal *mysql.ReplicaStatus
	replicaStatusErr error
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
func (m *mockMySQL) StopReplica(_ context.Context) error              { return nil }
func (m *mockMySQL) ResetReplicaAll(_ context.Context) error          { return nil }
func (m *mockMySQL) SetReadOnly(_ context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly = on
	return nil
}
func (m *mockMySQL) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.replicaStatusVal, m.replicaStatusErr
}
func (m *mockMySQL) ChangeReplicationSource(_ context.Context, _ mysql.ReplicationSourceOpts) error {
	return nil
}
func (m *mockMySQL) StartReplica(_ context.Context) error { return nil }
func (m *mockMySQL) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	return nil
}
func (m *mockMySQL) SetCloneDonorList(_ context.Context, _ string) error { return nil }
func (m *mockMySQL) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
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

func (m *mockTainter) SetTaint(_ context.Context, selector string, taint bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taints[selector] = taint
	return nil
}

func (m *mockTainter) isTainted(selector string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taints[selector]
}

// --- Mock DNS ---

type mockDNS struct {
	mu     sync.Mutex
	lastIP string
	calls  int
	err    error
}

func (m *mockDNS) UpdateDNSRecord(_ context.Context, ip string) error {
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

// taintSelector returns the expected taint selector for a given site name,
// matching the format produced by TopologyManager.taintSelector.
func taintSelector(siteName string) string {
	return "shipstream.io/failover-group=lion,shipstream.io/site=" + siteName
}

func testTopologyConfig() TopologyConfig {
	return TopologyConfig{
		Name:  "lion",
		Sites: [2]SiteTopologyConfig{
			{Name: "dc1", Zone: "lion-dc1", LBIP: "1.1.1.1"},
			{Name: "dc2", Zone: "lion-dc2", LBIP: "2.2.2.2"},
		},
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func newTestTopologyManager(site0, site1 *mockMySQL) (*TopologyManager, *mockTainter, *mockDNS) {
	cfg := testTopologyConfig()
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	tm := NewTopologyManager(cfg, site0, site1, fc, nil, BootstrapConfig{}, tainter, hub, dns, testLogger())
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

func TestNormalSite0Primary(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, tainter, _ := newTestTopologyManager(site0, site1)

	// Need RecoveryThreshold polls for site0 to confirm writable
	pollN(tm, 2)

	if tainter.isTainted(taintSelector("dc1")) {
		t.Error("site0 should not be tainted")
	}
	if !tainter.isTainted(taintSelector("dc2")) {
		t.Error("site1 should be tainted")
	}

	s := tm.Status()
	if s.Sites[0].State != "writable" {
		t.Errorf("site0 state: got %s, want writable", s.Sites[0].State)
	}
	if s.Sites[1].State != "read-only" {
		t.Errorf("site1 state: got %s, want read-only", s.Sites[1].State)
	}
}

func TestFailoverSite0Down(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, tainter, dns := newTestTopologyManager(site0, site1)

	// Establish normal state
	pollN(tm, 2)

	// site0 goes down
	site0.setError(errors.New("connection refused"))

	// Need FailureThreshold polls for unreachable
	pollN(tm, 3)

	if !tainter.isTainted(taintSelector("dc1")) {
		t.Error("site0 should be tainted after failure")
	}
	// FailoverController.Execute sets readOnly=false via SetReadOnly.
	site1.mu.Lock()
	site1RO := site1.readOnly
	site1.mu.Unlock()
	if site1RO {
		t.Error("site1 should have been promoted (readOnly should be false)")
	}

	// DNS flip is deferred until promotion is confirmed.
	// SetReadOnly(false) sets readOnly=false, but we need RecoveryThreshold polls to confirm.
	if dns.getLastIP() != "" {
		t.Error("DNS should not flip before promotion confirmation")
	}

	// Poll to confirm promotion (recovery threshold = 2 polls of read_only=0)
	pollN(tm, 2)

	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should point to site1 after confirmation, got %s", dns.getLastIP())
	}
}

func TestPromotionNotRepeated(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1)

	// Establish normal state
	pollN(tm, 2)

	// site0 goes down
	site0.setError(errors.New("connection refused"))
	pollN(tm, 3) // reach threshold, triggers promotion

	site1.mu.Lock()
	site1RO := site1.readOnly
	site1.mu.Unlock()
	if site1RO {
		t.Fatal("site1 should have been promoted (readOnly should be false)")
	}

	// Poll again while site0 still down, site1 recovering
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
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1)

	// Override site1 to fail on promote
	failSite1 := &failingPromoteMySQL{mockMySQL: site1}
	tm.sites[1].mysql = failSite1

	pollN(tm, 2) // establish normal

	site0.setError(errors.New("connection refused"))
	pollN(tm, 3) // trigger failover attempt

	// Promotion failed, so no DNS flip should happen
	pollN(tm, 5)
	if dns.getLastIP() != "" {
		t.Error("DNS should not flip when promotion fails")
	}
}

func TestReadiness(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	if tm.Ready() {
		t.Error("should not be ready before first poll")
	}

	pollN(tm, 1)

	if !tm.Ready() {
		t.Error("should be ready after first poll")
	}
}

func TestDebouncePreventsPrematureFailover(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1)

	// Establish normal
	pollN(tm, 2)

	// Single failure should not trigger failover
	site0.setError(errors.New("timeout"))
	pollN(tm, 1)

	if dns.getLastIP() != "" {
		t.Error("single failure should not trigger DNS flip")
	}

	// Recovery before threshold
	site0.setReadOnly(false)
	pollN(tm, 2)

	if tm.sites[0].state != state.StateWritable {
		t.Errorf("site0 should recover to writable, got %s", tm.sites[0].state)
	}
}

func TestRecoveryDebounce(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: false}
	tm, tainter, _ := newTestTopologyManager(site0, site1)

	// Establish site1 primary
	pollN(tm, 2)

	if !tainter.isTainted(taintSelector("dc1")) {
		t.Error("site0 should be tainted (read-only)")
	}

	// site0 becomes writable - needs RecoveryThreshold confirmations
	site0.setReadOnly(false)
	pollN(tm, 1)

	// After 1 poll, still not recovered (threshold=2)
	if tm.sites[0].state == state.StateWritable {
		t.Error("site0 should not yet be writable after 1 recovery poll")
	}

	pollN(tm, 1)

	if tm.sites[0].state != state.StateWritable {
		t.Errorf("site0 should be writable after 2 recovery polls, got %s", tm.sites[0].state)
	}
}

func TestSplitBrainNoAction(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: false}
	tm, _, dns := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	// Both writable = split brain, DNS should NOT be flipped
	if dns.getLastIP() != "" {
		t.Error("split brain should not flip DNS")
	}
}

func TestDoubleReadOnlyNoAction(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: true}
	tm, tainter, dns := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	// Both read-only, both should be tainted, no DNS flip
	if !tainter.isTainted(taintSelector("dc1")) {
		t.Error("site0 should be tainted")
	}
	if !tainter.isTainted(taintSelector("dc2")) {
		t.Error("site1 should be tainted")
	}
	if dns.getLastIP() != "" {
		t.Error("double read-only should not flip DNS")
	}
}

func TestTotalLoss(t *testing.T) {
	site0 := &mockMySQL{err: errors.New("down")}
	site1 := &mockMySQL{err: errors.New("down")}
	tm, tainter, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 3) // reach failure threshold

	if !tainter.isTainted(taintSelector("dc1")) {
		t.Error("site0 should be tainted")
	}
	if !tainter.isTainted(taintSelector("dc2")) {
		t.Error("site1 should be tainted")
	}
}

func TestTopologyManagerRunCancellation(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	// Run calls Poll synchronously on start, so after Run returns from
	// initial poll the manager is ready. We just need to verify it stops
	// cleanly on cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tm.Run(ctx)
		close(done)
	}()

	// Wait for readiness with a timeout. The initial Poll is synchronous
	// inside Run, so ready should be set almost immediately.
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(1 * time.Millisecond)
	defer tick.Stop()
	for !tm.Ready() {
		select {
		case <-deadline:
			t.Fatal("topology manager did not become ready within timeout")
		case <-tick.C:
		}
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("topology manager did not stop after context cancellation")
	}
}

func TestPollChecksReplicaStatus(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning:  true,
		SQLRunning: true,
	}}
	tm, _, _ := newTestTopologyManager(site0, site1)

	var captured TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		captured = snap
	}

	// Need RecoveryThreshold polls for site0 writable + state change
	pollN(tm, 2)

	if captured.SiteReplication[1] == nil {
		t.Fatal("expected site1 replication status to be populated")
	}
	if !captured.SiteReplication[1].IORunning || !captured.SiteReplication[1].SQLRunning {
		t.Error("expected site1 replication IO and SQL threads running")
	}
	if captured.SiteReplication[0] != nil {
		t.Error("site0 is primary, should not have replication status")
	}
}

func TestReplicationBrokenInSnapshot(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning:  true,
		SQLRunning: false, // SQL thread stopped
	}}
	tm, _, _ := newTestTopologyManager(site0, site1)

	var captured TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		captured = snap
	}

	pollN(tm, 2)

	if captured.SiteReplication[1] == nil {
		t.Fatal("expected site1 replication status to be populated")
	}
	if captured.SiteReplication[1].SQLRunning {
		t.Error("expected site1 SQL thread to be stopped")
	}
}

func TestReplicationNotCheckedOnWritableSite(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning:  false,
		SQLRunning: false,
	}}
	site1 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning:  true,
		SQLRunning: true,
	}}
	tm, _, _ := newTestTopologyManager(site0, site1)

	var captured TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		captured = snap
	}

	pollN(tm, 2)

	// site0 is writable, so its replication should NOT be checked
	if captured.SiteReplication[0] != nil {
		t.Error("writable site should not have replication status checked")
	}
}
