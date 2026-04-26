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
func (m *mockMySQL) StopReplica(_ context.Context) error               { return nil }
func (m *mockMySQL) ResetReplicaAll(_ context.Context) error           { return nil }
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

func (m *mockTainter) SetTaint(_ context.Context, selector string, _ string, taint bool) error {
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
	return "shipstream.io/failover-group.lion=true,shipstream.io/site.lion=" + siteName
}

func testTopologyConfig() TopologyConfig {
	return TopologyConfig{
		Name: "lion",
		Sites: []SiteTopologyConfig{
			{Name: "dc1", Zone: "lion-dc1", LBIP: "1.1.1.1", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("dc1")},
			{Name: "dc2", Zone: "lion-dc2", LBIP: "2.2.2.2", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("dc2")},
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
	tm := NewTopologyManager(cfg, []mysql.Checker{site0, site1}, fc, nil, nil, BootstrapConfig{}, tainter, hub, dns, testLogger())
	// Use a very short cooldown for tests so failovers aren't blocked.
	tm.failoverCooldown = 0
	return tm, tainter, dns
}

func newTestTopologyManagerWithBootstrap(site0, site1 *mockMySQL) (*TopologyManager, *mockTainter, *mockDNS) {
	cfg := testTopologyConfig()
	cfg.Sites[0].Host = "mysql-dc1"
	cfg.Sites[1].Host = "mysql-dc2"
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
	tm := NewTopologyManager(cfg, []mysql.Checker{site0, site1}, fc, nil, bc, bcfg, tainter, hub, dns, testLogger())
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

// TestSplitBrainAfterFailover_NoFreshDeployBootstrap is a regression test for a
// bug where, after a successful failover, a respawned old primary triggered
// the isFreshDeploy auto-bootstrap path (both sites writable, no replication
// configured on either). That path would select the old primary as clone donor
// and silently revert the failover. With lastFailoverTarget set, this shape
// must be handled by the fence-returning-old-primary branch, not a bootstrap.
func TestSplitBrainAfterFailover_NoFreshDeployBootstrap(t *testing.T) {
	// Initial topology: site0 writable (primary), site1 read-only replicating.
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "abc:1-10"}
	site1 := &mockMySQL{
		readOnly: true,
		replicaStatusVal: &mysql.ReplicaStatus{
			IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1",
		},
		gtidExecuted: "abc:1-10",
	}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	pollN(tm, 2)

	// A prior failover promoted site1. After operator restart, this is
	// restored from CR status; in tests we set it directly.
	tm.lastFailoverTarget = "dc2"

	// Simulate post-failover split-brain: site1 was promoted (replica threads
	// cleared by RESET REPLICA ALL) and site0 (old primary) respawned writable
	// before the operator could fence it. Both now look writable with no
	// replication configured — the exact shape that tricked isFreshDeploy.
	site1.setReadOnly(false)
	site1.mu.Lock()
	site1.replicaStatusVal = nil
	site1.mu.Unlock()

	pollN(tm, 2)

	tm.mu.RLock()
	phase := tm.bootstrapPhase
	tm.mu.RUnlock()
	if phase != BootstrapPhaseNone {
		t.Fatalf("bootstrap must not start during post-failover split-brain, got phase=%q", phase)
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

	if captured.Sites[1].Replication == nil {
		t.Fatal("expected site1 replication status to be populated")
	}
	if !captured.Sites[1].Replication.IORunning || !captured.Sites[1].Replication.SQLRunning {
		t.Error("expected site1 replication IO and SQL threads running")
	}
	if captured.Sites[0].Replication != nil {
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

	if captured.Sites[1].Replication == nil {
		t.Fatal("expected site1 replication status to be populated")
	}
	if captured.Sites[1].Replication.SQLRunning {
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
	if captured.Sites[0].Replication != nil {
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

// --- detectEmptySite tests ---
//
// The N-site donor selector is purely empty-detection: it walks the
// sites once and returns the first writable-with-data donor plus the
// first reachable empty recipient. Every case below asserts that
// bookkeeping on the site-name level.

func TestDetectEmptySite_PostPVCWipe(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{readOnly: false} // empty after PVC wipe
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateWritable

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "dc1" || empty != "dc2" {
		t.Errorf("expected donor=dc1 empty=dc2, got donor=%q empty=%q", donor, empty)
	}
}

func TestDetectEmptySite_Site0Empty(t *testing.T) {
	site0 := &mockMySQL{readOnly: false} // empty
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "bbbb:1-50"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateWritable

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "dc2" || empty != "dc1" {
		t.Errorf("expected donor=dc2 empty=dc1, got donor=%q empty=%q", donor, empty)
	}
}

func TestDetectEmptySite_BothHaveData(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-10"}
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "bbbb:1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateWritable

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "" || empty != "" {
		t.Errorf("expected empty/empty when both have data, got donor=%q empty=%q", donor, empty)
	}
}

func TestDetectEmptySite_SiteUnreachable(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{err: errors.New("down")}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateUnreachable

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "" || empty != "" {
		t.Errorf("expected empty/empty when a site is unreachable, got donor=%q empty=%q", donor, empty)
	}
}

func TestDetectEmptySite_EmptySiteReadOnly(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{readOnly: true} // empty but fenced by sidecar
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "dc1" || empty != "dc2" {
		t.Errorf("expected donor=dc1 empty=dc2 (read-only empty site allowed), got donor=%q empty=%q", donor, empty)
	}
}

// --- pickFreshestCandidate tests ---

// TestPickFreshestCandidate_FresherGtidBeatsPriority: a
// higher-priority site with a stale GTID loses to a lower-priority
// site with a strictly newer GTID. This is the critical correctness
// property of the failover picker — priority is a tiebreaker, not a
// dominant factor, because promoting a stale replica means losing
// transactions that actually exist on a fresher one.
func TestPickFreshestCandidate_FresherGtidBeatsPriority(t *testing.T) {
	// dc1 is the priority-first candidate but is 10 transactions
	// behind; dc2 has the full GTID set.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-40"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-50"}
	tm, _, _ := newTestTopologyManager(site0, site1)

	winner := tm.pickFreshestCandidate(context.Background(), []string{"dc1", "dc2"})
	if winner != "dc2" {
		t.Fatalf("expected picker to promote dc2 (fresher GTID) despite dc1 priority, got %q", winner)
	}
}

// TestPickFreshestCandidate_EqualGtidKeepsPriority: when every
// candidate has the same GTID set, the earliest-by-priority wins.
func TestPickFreshestCandidate_EqualGtidKeepsPriority(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-50"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-50"}
	tm, _, _ := newTestTopologyManager(site0, site1)

	winner := tm.pickFreshestCandidate(context.Background(), []string{"dc2", "dc1"})
	if winner != "dc2" {
		t.Fatalf("expected priority tiebreaker to pick dc2 on equal GTIDs, got %q", winner)
	}
}

// TestPickFreshestCandidate_SkipsUnreachable: a candidate whose GTID
// query fails is skipped; the freshest reachable replica wins.
func TestPickFreshestCandidate_SkipsUnreachable(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, gtidExecutedErr: errors.New("connection refused")}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-30"}
	tm, _, _ := newTestTopologyManager(site0, site1)

	winner := tm.pickFreshestCandidate(context.Background(), []string{"dc1", "dc2"})
	if winner != "dc2" {
		t.Fatalf("expected picker to skip unreachable dc1 and choose dc2, got %q", winner)
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

// TestIsHealthyReplica_TruthTable covers issue #46 Part 1: the site-level health
// signal must require both read-only state and active replication.
func TestIsHealthyReplica_TruthTable(t *testing.T) {
	cases := []struct {
		name        string
		st          state.SiteState
		replicating bool
		want        bool
	}{
		{"readonly-and-replicating", state.StateReadOnly, true, true},
		{"readonly-but-not-replicating", state.StateReadOnly, false, false},
		{"writable-even-if-flag-set", state.StateWritable, true, false},
		{"unreachable", state.StateUnreachable, true, false},
		{"unknown", state.StateUnknown, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &siteTracker{state: tc.st, replicating: tc.replicating}
			if got := tr.isHealthyReplica(); got != tc.want {
				t.Errorf("isHealthyReplica=%v want %v", got, tc.want)
			}
		})
	}
}

// TestPoll_SetsReplicatingFlagWithDebounce verifies the streak-based debounce
// for the replicating flag on the read-only site.
func TestPoll_SetsReplicatingFlagWithDebounce(t *testing.T) {
	site0 := &mockMySQL{readOnly: false} // primary
	site1 := &mockMySQL{
		readOnly: true,
		replicaStatusVal: &mysql.ReplicaStatus{
			IORunning:  true,
			SQLRunning: true,
			SourceHost: "dc1",
		},
	}
	tm, _, _ := newTestTopologyManager(site0, site1)

	// First poll promotes site1 to StateReadOnly and records streak=1; replicating still false.
	pollN(tm, 1)
	tm.mu.RLock()
	replicating := tm.sites[1].replicating
	streak := tm.sites[1].replicatingStreak
	tm.mu.RUnlock()
	if replicating {
		t.Errorf("replicating should still be false after 1 healthy tick (debounce), streak=%d", streak)
	}

	// Need enough polls to get RecoveryThreshold (2) transitions plus one more healthy
	// tick for the debounce. Three polls is enough to cover both.
	pollN(tm, 3)
	tm.mu.RLock()
	replicating = tm.sites[1].replicating
	tm.mu.RUnlock()
	if !replicating {
		t.Error("replicating should be true after consecutive healthy ticks")
	}
}

// TestPoll_ClearsReplicatingWhenReplicationStopped covers issue #46 Part 1's core
// case: super_read_only=ON but replication threads stopped must NOT register as
// a healthy replica.
func TestPoll_ClearsReplicatingWhenReplicationStopped(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{
		readOnly: true,
		replicaStatusVal: &mysql.ReplicaStatus{
			IORunning:  true,
			SQLRunning: true,
			SourceHost: "dc1",
		},
	}
	tm, _, _ := newTestTopologyManager(site0, site1)
	pollN(tm, 4)

	tm.mu.RLock()
	healthy := tm.sites[1].isHealthyReplica()
	tm.mu.RUnlock()
	if !healthy {
		t.Fatal("setup: expected site1 to be a healthy replica before simulated breakage")
	}

	// Simulate replication threads stopping — super_read_only stays ON.
	site1.mu.Lock()
	site1.replicaStatusVal = &mysql.ReplicaStatus{
		IORunning:  false,
		SQLRunning: false,
		SourceHost: "dc1",
	}
	site1.mu.Unlock()

	pollN(tm, 1)

	tm.mu.RLock()
	replicating := tm.sites[1].replicating
	streak := tm.sites[1].replicatingStreak
	healthy = tm.sites[1].isHealthyReplica()
	tm.mu.RUnlock()
	if replicating || healthy || streak != 0 {
		t.Errorf("expected replicating=false streak=0 after broken replication, got replicating=%v streak=%d", replicating, streak)
	}
}

// TestCheckUpdate_DefersWhenStandbyNotReplicating verifies the issue #46 gate:
// even with spec drift and both sites in the right states, checkUpdate must NOT
// start an ordered update against a stale standby.
func TestCheckUpdate_DefersWhenStandbyNotReplicating(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{
		readOnly:         true,
		replicaStatusVal: &mysql.ReplicaStatus{}, // empty — threads stopped, no source
	}
	tm, _, _ := newTestTopologyManager(site0, site1)
	// Attach an UpdateController and ApplyUpdate callback so checkUpdate doesn't
	// early-return on nil dependencies.
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.ApplyUpdate = func(_ context.Context, _ string) error {
		t.Fatal("ApplyUpdate must not be called when standby is not replicating")
		return nil
	}

	// Poll enough to settle states (site1 -> StateReadOnly with RecoveryThreshold=2).
	pollN(tm, 3)

	// Drift on standby — the classic trigger for an ordered update.
	tm.SetSpecDriftSites([]string{"dc2"})

	started := tm.checkUpdate(context.Background())
	if started {
		t.Fatal("checkUpdate must defer when the standby is not a healthy replica")
	}
	if tm.updater.IsUpdating() {
		t.Error("UpdateController must not enter updating state on deferred start")
	}
}

// TestPoll_ZerosReplicatingOnStateLeave makes sure a tracker that was a healthy
// replica does not carry replicating=true into a later writable/unreachable state.
func TestPoll_ZerosReplicatingOnStateLeave(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{
		readOnly: true,
		replicaStatusVal: &mysql.ReplicaStatus{
			IORunning:  true,
			SQLRunning: true,
			SourceHost: "dc1",
		},
	}
	tm, _, _ := newTestTopologyManager(site0, site1)
	pollN(tm, 4)

	tm.mu.RLock()
	initialReplicating := tm.sites[1].replicating
	tm.mu.RUnlock()
	if !initialReplicating {
		t.Fatal("setup: expected site1.replicating=true after healthy polls")
	}

	// Simulate site1 becoming unreachable — ShowReplicaStatus path will emit
	// a warn and clear replicating; even without that, the state transition to
	// StateUnreachable must zero the flag.
	site1.mu.Lock()
	site1.err = errors.New("conn refused")
	site1.mu.Unlock()

	// FailureThreshold=3 ticks to confirm unreachable transition.
	pollN(tm, 5)

	tm.mu.RLock()
	replicating := tm.sites[1].replicating
	streak := tm.sites[1].replicatingStreak
	st := tm.sites[1].state
	tm.mu.RUnlock()
	if st == state.StateReadOnly {
		t.Fatal("setup: site1 should have left StateReadOnly after unreachable ticks")
	}
	if replicating || streak != 0 {
		t.Errorf("replicating should be cleared on state leave, got replicating=%v streak=%d", replicating, streak)
	}
}
