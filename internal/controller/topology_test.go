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
	gtidExecuted     string
	gtidExecutedErr  error
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

func (m *mockMySQL) SetSuperReadOnly(_ context.Context, _ bool) error  { return nil }
func (m *mockMySQL) KillAppConnections(_ context.Context) (int, error) { return 0, nil }
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
func (m *mockMySQL) StartReplica(_ context.Context) error          { return nil }
func (m *mockMySQL) StartReplicaSQLThread(_ context.Context) error { return nil }
func (m *mockMySQL) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	return nil
}
func (m *mockMySQL) GetGtidExecuted(_ context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gtidExecuted, m.gtidExecutedErr
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

func (m *mockMySQL) setGtidExecuted(gtid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gtidExecuted = gtid
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

func newTestTopologyManagerWithBootstrap(site0, site1 *mockMySQL) (*TopologyManager, *mockTainter, *mockDNS) {
	cfg := testTopologyConfig()
	cfg.SiteHosts = [2]string{"mysql-dc1", "mysql-dc2"}
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	bc := NewBootstrapController(testLogger())
	bcfg := BootstrapConfig{
		ReplUser:     "repl",
		ReplPassword: "replpass",
		CloneTimeout: 10 * time.Second,
	}
	tm := NewTopologyManager(cfg, site0, site1, fc, bc, bcfg, tainter, hub, dns, testLogger())
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

	// DNS should have flipped immediately at failover trigger (before promotion).
	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should flip at failover trigger, got %s", dns.getLastIP())
	}

	// DNS should flip exactly once even after additional polls.
	pollN(tm, 2)

	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()
	if calls != 1 {
		t.Errorf("DNS should flip exactly once, got %d calls", calls)
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

	// DNS should have flipped at trigger time
	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should flip at failover trigger, got %s", dns.getLastIP())
	}

	// Poll again while site0 still down, site1 recovering
	pollN(tm, 5)

	// DNS should only flip once (at trigger time)
	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()
	if calls > 1 {
		t.Errorf("DNS should flip at most once, got %d calls", calls)
	}
}

func TestPromotionFailure_DNSStillFlips(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1)

	// Override site1 to fail on promote
	failSite1 := &failingPromoteMySQL{mockMySQL: site1}
	tm.sites[1].mysql = failSite1

	pollN(tm, 2) // establish normal

	site0.setError(errors.New("connection refused"))
	pollN(tm, 3) // trigger failover attempt

	// DNS flips before promotion — even if promotion fails, DNS was already updated.
	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should flip at trigger time even when promotion fails, got %q", dns.getLastIP())
	}

	// promotedSite should NOT be set since Execute returned an error.
	if tm.promotedSite != "" {
		t.Errorf("promotedSite should be empty after failed promotion, got %q", tm.promotedSite)
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

func TestStatusActiveSiteOneWritable(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	s := tm.Status()
	if s.ActiveSite != "dc1" {
		t.Errorf("expected active_site=dc1, got %q", s.ActiveSite)
	}
}

func TestStatusActiveSiteBothReadOnly(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	s := tm.Status()
	if s.ActiveSite != "" {
		t.Errorf("expected empty active_site when both read-only, got %q", s.ActiveSite)
	}
}

func TestStatusActiveSiteBothWritable(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: false}
	tm, _, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	s := tm.Status()
	if s.ActiveSite != "" {
		t.Errorf("expected empty active_site during split-brain, got %q", s.ActiveSite)
	}
}

func TestSnapshotActiveSiteMatchesStatus(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	var captured TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		captured = snap
	}

	pollN(tm, 2)

	s := tm.Status()
	if captured.ActiveSite != s.ActiveSite {
		t.Errorf("snapshot active_site=%q does not match status active_site=%q",
			captured.ActiveSite, s.ActiveSite)
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

func TestAdaptivePollInterval(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	base := tm.cfg.PollIntervalDuration() // 50ms in test config

	// No failures → base interval.
	if got := tm.adaptivePollInterval(base); got != base {
		t.Errorf("healthy: expected %v, got %v", base, got)
	}

	// Failures below threshold → still base interval.
	tm.sites[0].failCount = tm.cfg.FailureThreshold - 1
	if got := tm.adaptivePollInterval(base); got != base {
		t.Errorf("below threshold: expected %v, got %v", base, got)
	}

	// At threshold → still base (backoff starts after threshold).
	tm.sites[0].failCount = tm.cfg.FailureThreshold
	if got := tm.adaptivePollInterval(base); got != base {
		t.Errorf("at threshold: expected %v, got %v", base, got)
	}

	// One failure past threshold → 2x base.
	tm.sites[0].failCount = tm.cfg.FailureThreshold + 1
	if got := tm.adaptivePollInterval(base); got != 2*base {
		t.Errorf("threshold+1: expected %v, got %v", 2*base, got)
	}

	// Two past threshold → 4x base.
	tm.sites[0].failCount = tm.cfg.FailureThreshold + 2
	if got := tm.adaptivePollInterval(base); got != 4*base {
		t.Errorf("threshold+2: expected %v, got %v", 4*base, got)
	}

	// Capped at 2^maxPollBackoffExponent * base.
	tm.sites[0].failCount = tm.cfg.FailureThreshold + 100
	got := tm.adaptivePollInterval(base)
	maxInterval := base * time.Duration(1<<maxPollBackoffExponent)
	if got != maxInterval {
		t.Errorf("cap: expected %v, got %v", maxInterval, got)
	}

	// Recovery resets interval: clear failures.
	tm.sites[0].failCount = 0
	if got := tm.adaptivePollInterval(base); got != base {
		t.Errorf("after recovery: expected %v, got %v", base, got)
	}
}

// --- selectDonor tests ---

func TestSelectDonor_Site0HasData(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa:1-100"}
	site1 := &mockMySQL{readOnly: false}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	primary, replica, err := tm.selectDonor(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if primary != 0 || replica != 1 {
		t.Errorf("expected donor=0 replica=1, got donor=%d replica=%d", primary, replica)
	}
}

func TestSelectDonor_Site1HasData(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb:1-50"}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	primary, replica, err := tm.selectDonor(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if primary != 1 || replica != 0 {
		t.Errorf("expected donor=1 replica=0, got donor=%d replica=%d", primary, replica)
	}
}

func TestSelectDonor_BothEmpty(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: false}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	primary, replica, err := tm.selectDonor(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if primary != 0 || replica != 1 {
		t.Errorf("expected donor=0 replica=1 for both-empty, got donor=%d replica=%d", primary, replica)
	}
}

func TestSelectDonor_BothHaveData_DisjointGTIDs(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-10"}
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "bbbb:1-10"}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	primary, replica, err := tm.selectDonor(context.Background())
	if err != nil {
		t.Fatalf("disjoint GTIDs should be treated as fresh deploy, got error: %v", err)
	}
	if primary != 0 || replica != 1 {
		t.Errorf("expected primary=0, replica=1; got primary=%d, replica=%d", primary, replica)
	}
}

func TestSelectDonor_BothHaveData_OverlappingGTIDs(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-10"}
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-5,bbbb:1-10"}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	_, _, err := tm.selectDonor(context.Background())
	if err == nil {
		t.Fatal("expected error when both sites have overlapping GTIDs")
	}
}

// --- detectEmptySite tests ---

func TestDetectEmptySite_PostPVCWipe(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{readOnly: false} // empty after PVC wipe
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateWritable

	donorIdx, emptyIdx := tm.detectEmptySite(context.Background())
	if donorIdx != 0 || emptyIdx != 1 {
		t.Errorf("expected donor=0 empty=1, got donor=%d empty=%d", donorIdx, emptyIdx)
	}
}

func TestDetectEmptySite_Site0Empty(t *testing.T) {
	site0 := &mockMySQL{readOnly: false} // empty
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "bbbb:1-50"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateWritable

	donorIdx, emptyIdx := tm.detectEmptySite(context.Background())
	if donorIdx != 1 || emptyIdx != 0 {
		t.Errorf("expected donor=1 empty=0, got donor=%d empty=%d", donorIdx, emptyIdx)
	}
}

func TestDetectEmptySite_BothHaveData(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-10"}
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "bbbb:1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateWritable

	donorIdx, emptyIdx := tm.detectEmptySite(context.Background())
	if donorIdx != -1 || emptyIdx != -1 {
		t.Errorf("expected -1,-1 when both have data, got %d,%d", donorIdx, emptyIdx)
	}
}

func TestDetectEmptySite_SiteUnreachable(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{err: errors.New("down")}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateUnreachable

	donorIdx, emptyIdx := tm.detectEmptySite(context.Background())
	if donorIdx != -1 || emptyIdx != -1 {
		t.Errorf("expected -1,-1 when site unreachable, got %d,%d", donorIdx, emptyIdx)
	}
}

func TestDetectEmptySite_EmptySiteReadOnly(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{readOnly: true} // empty but fenced by sidecar
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly

	donorIdx, emptyIdx := tm.detectEmptySite(context.Background())
	if donorIdx != 0 || emptyIdx != 1 {
		t.Errorf("expected donor=0 empty=1 (read-only empty site allowed), got donor=%d empty=%d", donorIdx, emptyIdx)
	}
}

// --- checkReclone tests ---

func TestReclone_HappyPath(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	// Establish normal state.
	pollN(tm, 2)

	tm.SetRecloneSite("dc2")

	recloneStarted := tm.checkReclone(context.Background())
	if !recloneStarted {
		t.Fatal("expected reclone to start")
	}
	phase := tm.BootstrapPhase()
	if phase == BootstrapPhaseNone || phase == BootstrapPhaseFailed {
		t.Errorf("expected bootstrap to be in progress or completed successfully, got %q", phase)
	}
	tm.mu.RLock()
	src := tm.bootstrapSource
	tm.mu.RUnlock()
	if src != "reclone" {
		t.Errorf("expected bootstrapSource=reclone, got %q", src)
	}
}

func TestReclone_CannotReclonePrimary(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	pollN(tm, 2)

	tm.SetRecloneSite("dc1") // dc1 is the writable primary

	recloneStarted := tm.checkReclone(context.Background())
	if recloneStarted {
		t.Fatal("should not reclone the active primary")
	}

	// Pending should be cleared.
	tm.mu.RLock()
	pending := tm.reclonePendingSite
	tm.mu.RUnlock()
	if pending != "" {
		t.Errorf("reclonePendingSite should be cleared, got %q", pending)
	}
}

func TestReclone_ClearsRecoveryState(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	pollN(tm, 2)

	// Simulate RecoveryBlocked state on dc2.
	tm.mu.Lock()
	tm.recoveryPendingSite = "dc2"
	tm.recoveryDivergentGtid = "aaaa:50-55"
	tm.recoveryDivergentCount = 6
	tm.mu.Unlock()

	tm.SetRecloneSite("dc2")
	tm.checkReclone(context.Background())

	tm.mu.RLock()
	recoverySite := tm.recoveryPendingSite
	divergentGtid := tm.recoveryDivergentGtid
	divergentCount := tm.recoveryDivergentCount
	tm.mu.RUnlock()

	if recoverySite != "" {
		t.Errorf("recoveryPendingSite should be cleared, got %q", recoverySite)
	}
	if divergentGtid != "" {
		t.Errorf("recoveryDivergentGtid should be cleared, got %q", divergentGtid)
	}
	if divergentCount != 0 {
		t.Errorf("recoveryDivergentCount should be 0, got %d", divergentCount)
	}
}

func TestReclone_BlockedDuringBootstrap(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	pollN(tm, 2)

	// Simulate an in-progress bootstrap.
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.mu.Unlock()

	tm.SetRecloneSite("dc2")

	recloneStarted := tm.checkReclone(context.Background())
	if recloneStarted {
		t.Fatal("reclone should be deferred during active bootstrap")
	}

	// Pending should still be set (deferred, not cleared).
	tm.mu.RLock()
	pending := tm.reclonePendingSite
	tm.mu.RUnlock()
	if pending != "dc2" {
		t.Errorf("reclonePendingSite should be preserved for retry, got %q", pending)
	}
}

func TestReclone_UnknownSite(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)

	pollN(tm, 2)

	tm.SetRecloneSite("nonexistent")

	recloneStarted := tm.checkReclone(context.Background())
	if recloneStarted {
		t.Fatal("should not start reclone for unknown site")
	}

	tm.mu.RLock()
	pending := tm.reclonePendingSite
	tm.mu.RUnlock()
	if pending != "" {
		t.Errorf("reclonePendingSite should be cleared for unknown site, got %q", pending)
	}
}
