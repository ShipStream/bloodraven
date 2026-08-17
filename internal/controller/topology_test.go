package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
	"k8s.io/apimachinery/pkg/types"
)

// --- Mock MySQL ---

type mockMySQL struct {
	mu                       sync.Mutex
	readOnly                 bool
	err                      error
	promoted                 bool
	replicaStatusVal         *mysql.ReplicaStatus
	replicaStatusErr         error
	gtidExecuted             string
	gtidExecutedErr          error
	gtidExecutedHadDeadline  bool
	hasUserSchemas           *bool
	userSchemasErr           error
	stopReplicaCalls         int
	resetReplicaCalls        int
	changeSourceCalls        int
	startReplicaCalls        int
	stopReplicaErr           error
	stopReplicaCancel        context.CancelFunc
	changeSourceErr          error
	startReplicaErr          error
	startReplicaCtxErrs      []error
	respectContext           bool
	gtidSequence             []string
	gtidCalls                int
	changeDoesNotUpdate      bool
	superReadOnlyCalls       int
	superReadOnlyErr         error
	superReadOnlyHadDeadline bool
	clonePrimaryHost         string
	killConnectionResults    []int
	killConnectionErrs       []error
	killConnectionCalls      int
}

func testBoolPtr(v bool) *bool {
	return &v
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

func (m *mockMySQL) SetSuperReadOnly(ctx context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.superReadOnlyCalls++
	_, m.superReadOnlyHadDeadline = ctx.Deadline()
	if m.superReadOnlyErr != nil {
		return m.superReadOnlyErr
	}
	if on {
		m.readOnly = true
	}
	return nil
}

func TestReadOnlyRoleSafety(t *testing.T) {
	primaryA := &mockMySQL{readOnly: true}
	primaryB := &mockMySQL{readOnly: true}
	reader := &mockMySQL{readOnly: false}
	tm := newConvergenceManager(t,
		[]state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly},
		primaryA, primaryB, reader)
	tm.sites[0].state = state.StateReadOnly
	tm.sites[1].state = state.StateReadOnly
	tm.sites[2].state = state.StateWritable
	if got := tm.activeSiteLocked(); got != "" {
		t.Fatalf("writable reader became active: %q", got)
	}
	if !tm.fenceWritableNonPromotableSites(context.Background()) || reader.superReadOnlyCalls != 1 {
		t.Fatalf("reader fence calls = %d", reader.superReadOnlyCalls)
	}
	if !reader.superReadOnlyHadDeadline {
		t.Fatal("reader fencing call did not receive a bounded context")
	}
	taint := true
	tm.applyPerSiteAction(context.Background(), &tm.sites[2], state.Action{Taint: &taint})
	if got := len(tm.tainter.(*mockTainter).taints); got != 0 {
		t.Fatalf("reader caused %d taint operations", got)
	}
}

func TestPoll_ReplicaStatusProbeFailureInterlocksRecovery(t *testing.T) {
	primary := &mockMySQL{readOnly: false, gtidExecuted: convergenceTestGTID}
	follower := &mockMySQL{
		readOnly:         true,
		gtidExecuted:     convergenceTestGTID,
		replicaStatusErr: errors.New("temporary replica probe failure"),
	}
	tm := newConvergenceManager(t,
		[]state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate},
		primary, follower,
	)
	tm.lastFailoverTarget = "primary"

	tm.Poll(context.Background())
	if got := tm.sites[1].sourceConvergenceState; got != sourceConvergencePending {
		t.Fatalf("source state = %q, want Pending", got)
	}
	if got := tm.sites[1].sourceConvergenceReason; got != sourceReasonProbeFailed {
		t.Fatalf("source reason = %q, want ProbeFailed", got)
	}
	if follower.stopReplicaCalls != 0 || follower.resetReplicaCalls != 0 || follower.changeSourceCalls != 0 || follower.startReplicaCalls != 0 {
		t.Fatalf("probe failure mutated replication: stop=%d reset=%d change=%d start=%d",
			follower.stopReplicaCalls, follower.resetReplicaCalls, follower.changeSourceCalls, follower.startReplicaCalls)
	}

	follower.replicaStatusErr = nil
	follower.replicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "old-primary"}
	tm.Poll(context.Background())
	if got := tm.sites[1].sourceConvergenceState; got != sourceConvergenceConverged {
		t.Fatalf("source state after successful retry = %q, want Converged", got)
	}
	if follower.stopReplicaCalls != 1 || follower.changeSourceCalls != 1 || follower.startReplicaCalls != 1 || follower.resetReplicaCalls != 0 {
		t.Fatalf("retry calls stop=%d reset=%d change=%d start=%d",
			follower.stopReplicaCalls, follower.resetReplicaCalls, follower.changeSourceCalls, follower.startReplicaCalls)
	}
}

func TestRecoveryRejectsWritableNonPromotableAuthority(t *testing.T) {
	for _, role := range []state.SiteRole{state.SiteRoleReadOnly, state.SiteRoleDROnly} {
		t.Run(string(role), func(t *testing.T) {
			candidate := &mockMySQL{readOnly: true, gtidExecuted: convergenceTestGTID}
			anomaly := &mockMySQL{readOnly: false, gtidExecuted: convergenceTestGTID}
			tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, role}, candidate, anomaly)
			tm.sites[0].state = state.StateReadOnly
			tm.sites[1].state = state.StateWritable
			tm.lastFailoverTarget = "follower-a"

			if tm.checkRecoveryWithConvergence(context.Background(), []*mysql.ReplicaStatus{nil, nil}, nil) {
				t.Fatal("recovery changed state using non-promotable authority")
			}
			if candidate.gtidCalls != 0 || anomaly.gtidCalls != 0 || candidate.stopReplicaCalls != 0 || candidate.resetReplicaCalls != 0 || candidate.changeSourceCalls != 0 || candidate.startReplicaCalls != 0 {
				t.Fatalf("recovery probed or mutated against %s authority", role)
			}
		})
	}
}

func TestPoll_FirstWritableNonPromotableObservationFencesImmediately(t *testing.T) {
	for _, role := range []state.SiteRole{state.SiteRoleReadOnly, state.SiteRoleDROnly} {
		for _, fenceFails := range []bool{false, true} {
			name := fmt.Sprintf("%s/fence-fails-%t", role, fenceFails)
			t.Run(name, func(t *testing.T) {
				primary := &mockMySQL{readOnly: false}
				anomaly := &mockMySQL{readOnly: false}
				if fenceFails {
					anomaly.superReadOnlyErr = errors.New("fence failed")
				}
				tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, role}, primary, anomaly)
				tm.sites[1].state = state.StateReadOnly
				tm.cfg.RecoveryThreshold = 3
				var snapshot TopologySnapshot
				tm.StatusCallback = func(s TopologySnapshot) { snapshot = s }

				tm.Poll(context.Background())
				if anomaly.superReadOnlyCalls != 1 {
					t.Fatalf("first writable observation made %d fence calls, want 1", anomaly.superReadOnlyCalls)
				}
				if tm.sites[1].state != state.StateWritable || tm.activeSiteLocked() != "" {
					t.Fatalf("anomaly did not immediately invalidate authority: state=%s active=%q", tm.sites[1].state, tm.activeSiteLocked())
				}
				if snapshot.DegradedReason != "Degraded" {
					t.Fatalf("degraded reason = %q, want Degraded", snapshot.DegradedReason)
				}
			})
		}
	}
}

func TestEmitStatusSnapshotPreservesPersistentTopologyDegradation(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: true}, &mockMySQL{readOnly: true})
	tm.sites[0].state = state.StateReadOnly
	tm.sites[1].state = state.StateReadOnly
	var snapshot TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) { snapshot = s }

	// This is the callback path used by update completion; no state transition
	// occurs while the current no-primary topology remains unchanged.
	tm.emitStatusSnapshot()
	if snapshot.DegradedReason != "NoPrimary" || snapshot.Alert == "" {
		t.Fatalf("update-only snapshot cleared degradation: %+v", snapshot)
	}
}
func (m *mockMySQL) KillAppConnections(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := m.killConnectionCalls
	m.killConnectionCalls++
	var killed int
	if i < len(m.killConnectionResults) {
		killed = m.killConnectionResults[i]
	}
	var err error
	if i < len(m.killConnectionErrs) {
		err = m.killConnectionErrs[i]
	}
	return killed, err
}
func (m *mockMySQL) StopReplica(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopReplicaCalls++
	if m.stopReplicaCancel != nil {
		m.stopReplicaCancel()
	}
	return m.stopReplicaErr
}
func (m *mockMySQL) ResetReplicaAll(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetReplicaCalls++
	return nil
}
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
func (m *mockMySQL) ChangeReplicationSource(_ context.Context, opts mysql.ReplicationSourceOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changeSourceCalls++
	if m.changeSourceErr != nil {
		return m.changeSourceErr
	}
	if m.changeDoesNotUpdate {
		return nil
	}
	if m.replicaStatusVal == nil {
		m.replicaStatusVal = &mysql.ReplicaStatus{}
	}
	m.replicaStatusVal.SourceHost = opts.Host
	return nil
}
func (m *mockMySQL) StartReplica(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startReplicaCalls++
	m.startReplicaCtxErrs = append(m.startReplicaCtxErrs, ctx.Err())
	if m.startReplicaErr != nil {
		return m.startReplicaErr
	}
	if m.replicaStatusVal != nil {
		m.replicaStatusVal.IORunning = true
		m.replicaStatusVal.SQLRunning = true
	}
	return nil
}
func (m *mockMySQL) StartReplicaSQLThread(_ context.Context) error { return nil }
func (m *mockMySQL) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	return nil
}
func (m *mockMySQL) GetGtidExecuted(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, m.gtidExecutedHadDeadline = ctx.Deadline()
	if m.respectContext && ctx.Err() != nil {
		return "", ctx.Err()
	}
	if len(m.gtidSequence) > 0 {
		idx := m.gtidCalls
		if idx >= len(m.gtidSequence) {
			idx = len(m.gtidSequence) - 1
		}
		m.gtidCalls++
		return m.gtidSequence[idx], m.gtidExecutedErr
	}
	return m.gtidExecuted, m.gtidExecutedErr
}
func (m *mockMySQL) HasUserSchemas(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hasUserSchemas != nil {
		return *m.hasUserSchemas, m.userSchemasErr
	}
	return m.gtidExecuted != "", m.userSchemasErr
}
func (m *mockMySQL) EnsureClonePlugin(_ context.Context) error           { return nil }
func (m *mockMySQL) SetCloneDonorList(_ context.Context, _ string) error { return nil }

func (m *mockMySQL) CloneInstance(_ context.Context, _, primaryHost, _ string, _ bool, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clonePrimaryHost = primaryHost
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

// stuckReadOnlyMySQL lets FailoverController.Execute complete (every step
// returns nil) but keeps the site read_only, so the post-promotion
// confirmWritable check fails — simulating a promotion that ran but never
// actually accepted writes.
type stuckReadOnlyMySQL struct {
	*mockMySQL
}

// SetReadOnly is a no-op success: Execute's "SET read_only = 0" appears to
// succeed, but the embedded mock's readOnly flag is left true, so
// CheckReadOnly keeps reporting the site as read_only.
func (s *stuckReadOnlyMySQL) SetReadOnly(_ context.Context, _ bool) error { return nil }

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

func (m *mockMySQL) startReplicaCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startReplicaCalls
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
			{Name: "dc1", Zone: "lion-dc1", LBIP: "1.1.1.1", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("dc1"), Host: "mysql-dc1"},
			{Name: "dc2", Zone: "lion-dc2", LBIP: "2.2.2.2", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("dc2"), Host: "mysql-dc2"},
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

func newTestTopologyManagerWithBootstrapClock(site0, site1 *mockMySQL, clk *clock.FakeClock) (*TopologyManager, *mockTainter, *mockDNS) {
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
	tm := NewTopologyManagerWithClock(cfg, []mysql.Checker{site0, site1}, fc, nil, bc, bcfg, tainter, hub, dns, testLogger(), clk)
	tm.failoverCooldown = 0
	return tm, tainter, dns
}

func setRecoveredTopology(tm *TopologyManager) {
	tm.mu.Lock()
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly
	tm.lastFailoverTarget = "dc1"
	tm.mu.Unlock()
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

func TestStatusIncludesSourceConvergence(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	tm.SetSourceConvergence("dc2", "mysql-lion-dc1-internal.ns.svc.cluster.local", sourceConvergenceConverged, "")
	tm.mu.Lock()
	tm.sites[1].servingHealthy = true
	tm.mu.Unlock()

	site := tm.Status().Sites[1]
	if got, want := site.SourceHost, "mysql-lion-dc1-internal.ns.svc.cluster.local"; got != want {
		t.Fatalf("sourceHost = %q, want %q", got, want)
	}
	if got, want := site.SourceConvergenceState, string(sourceConvergenceConverged); got != want {
		t.Fatalf("sourceConvergenceState = %q, want %q", got, want)
	}
	if !site.ServingHealthy {
		t.Fatal("servingHealthy = false, want true")
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

	// DNS should flip after successful promotion and writable confirmation.
	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should flip after promotion, got %s", dns.getLastIP())
	}

	// Poll again while site0 still down, site1 recovering
	pollN(tm, 5)

	// DNS should only flip once for the successful promotion.
	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()
	if calls > 1 {
		t.Errorf("DNS should flip at most once, got %d calls", calls)
	}
}

func TestPromotionFailure_DNSDoesNotFlip(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1)

	// Override site1 to fail on promote
	failSite1 := &failingPromoteMySQL{mockMySQL: site1}
	tm.sites[1].mysql = failSite1

	pollN(tm, 2) // establish normal

	site0.setError(errors.New("connection refused"))
	pollN(tm, 3) // trigger failover attempt

	// DNS must not flip when promotion fails.
	if dns.getLastIP() != "" {
		t.Errorf("DNS should not flip when promotion fails, got %q", dns.getLastIP())
	}

	// promotedSite should NOT be set since Execute returned an error.
	if tm.promotedSite != "" {
		t.Errorf("promotedSite should be empty after failed promotion, got %q", tm.promotedSite)
	}
}

// TestPromotionConfirmWritableFails_DNSDoesNotFlip covers the path where
// FailoverController.Execute succeeds but the promoted site never leaves
// read_only, so confirmWritable fails. DNS must not flip and no failover
// state may be recorded — the promotion is treated as unconfirmed.
func TestPromotionConfirmWritableFails_DNSDoesNotFlip(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1)

	// Execute will "succeed" against site1, but it stays read_only.
	tm.sites[1].mysql = &stuckReadOnlyMySQL{mockMySQL: site1}

	pollN(tm, 2) // establish normal

	site0.setError(errors.New("connection refused"))
	pollN(tm, 3) // trigger failover: Execute succeeds, confirmWritable fails

	// DNS must not flip when writable confirmation fails.
	if dns.getLastIP() != "" {
		t.Errorf("DNS should not flip when confirmWritable fails, got %q", dns.getLastIP())
	}

	// No failover state may be recorded for an unconfirmed promotion.
	if tm.promotedSite != "" {
		t.Errorf("promotedSite should be empty when confirmWritable fails, got %q", tm.promotedSite)
	}
	if tm.lastFailoverTarget != "" {
		t.Errorf("lastFailoverTarget should be empty when confirmWritable fails, got %q", tm.lastFailoverTarget)
	}
}

// TestPromotionDNSFlipFails_StatePreserved covers the path where promotion
// and writable confirmation both succeed but the DNS provider is down.
// The failover state (promotedSite / lastFailoverTarget / lastFailover)
// must be recorded despite the DNS failure so split-brain fencing,
// anti-flap cooldown, and metrics survive a DNS-provider outage.
func TestPromotionDNSFlipFails_StatePreserved(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(site0, site1)

	// DNS provider is unavailable: every UpdateDNSRecord call errors.
	dns.mu.Lock()
	dns.err = errors.New("dns provider unavailable")
	dns.mu.Unlock()

	pollN(tm, 2) // establish normal

	site0.setError(errors.New("connection refused"))
	pollN(tm, 3) // trigger failover: Execute + confirmWritable succeed, DNS flip fails

	// site1 was promoted at the MySQL level.
	site1.mu.Lock()
	site1RO := site1.readOnly
	site1.mu.Unlock()
	if site1RO {
		t.Fatal("site1 should have been promoted (readOnly should be false)")
	}

	// DNS never recorded the new IP because the provider errored.
	if dns.getLastIP() != "" {
		t.Errorf("DNS lastIP should be empty when the provider errors, got %q", dns.getLastIP())
	}

	// Critically, the failover state must still be recorded.
	if tm.promotedSite != "dc2" {
		t.Errorf("promotedSite should be set despite DNS failure, got %q", tm.promotedSite)
	}
	if tm.lastFailoverTarget != "dc2" {
		t.Errorf("lastFailoverTarget should be set despite DNS failure, got %q", tm.lastFailoverTarget)
	}
	if tm.lastFailover.IsZero() {
		t.Error("lastFailover should be set despite DNS failure (anti-flap cooldown)")
	}
}

func TestCanSkipCloneGTIDPredicate(t *testing.T) {
	tests := []struct {
		name      string
		donor     string
		recipient string
		want      bool
	}{
		{"equal", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", true},
		{"recipient behind donor", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", true},
		{"recipient ahead of donor", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20", false},
		{"disjoint", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", "AAAAAAAA-71CA-11E1-9E33-C80AA9429562:1-10", false},
		{"empty donor", "", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", false},
		{"empty recipient", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", "", false},
		{"malformed donor", "not-a-gtid", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", false},
		{"malformed recipient", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10", "not-a-gtid", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			donor := &mockMySQL{gtidExecuted: tt.donor}
			recipient := &mockMySQL{gtidExecuted: tt.recipient}
			tm, _, _ := newTestTopologyManager(donor, recipient)
			if got := tm.canSkipClone(context.Background(), donor, recipient); got != tt.want {
				t.Fatalf("canSkipClone() = %v, want %v", got, tt.want)
			}
		})
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

func TestStatusActiveSiteUsesPendingPromotion(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	tm.mu.Lock()
	tm.promotedSite = "dc2"
	tm.promotedAt = tm.clock.Now()
	tm.mu.Unlock()

	s := tm.Status()
	if s.ActiveSite != "dc2" {
		t.Errorf("expected pending promoted site in status activeSite, got %q", s.ActiveSite)
	}
}

func TestPendingPromotionExpiresFromStatusActiveSite(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	tm.mu.Lock()
	tm.promotedSite = "dc2"
	tm.promotedAt = tm.clock.Now().Add(-pendingPromotionActiveSiteTTL - time.Second)
	tm.mu.Unlock()

	s := tm.Status()
	if s.ActiveSite != "" {
		t.Errorf("expected expired pending promotion to be hidden from status activeSite, got %q", s.ActiveSite)
	}
}

func TestSnapshotActiveSiteDoesNotUsePendingPromotion(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	tm.mu.Lock()
	tm.promotedSite = "dc2"
	tm.promotedAt = tm.clock.Now()
	tm.mu.Unlock()

	snap := tm.buildSnapshot(nil)
	if snap.ActiveSite != "" {
		t.Errorf("expected CR snapshot activeSite to reflect observed writable site only, got %q", snap.ActiveSite)
	}
	if got := tm.Status().ActiveSite; got != "dc2" {
		t.Errorf("expected aux status activeSite to use fresh pending promotion, got %q", got)
	}
}

func TestPendingPromotionClearsWhenDifferentSiteWritable(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	pollN(tm, 2)

	tm.mu.Lock()
	tm.promotedSite = "dc2"
	tm.promotedAt = tm.clock.Now()
	tm.reconcilePendingPromotionLocked()
	pending := tm.promotedSite
	tm.mu.Unlock()

	if pending != "" {
		t.Fatalf("expected pending promotion to clear after dc1 was observed writable, got %q", pending)
	}
}

type blockingReadOnlyMySQL struct {
	*mockMySQL
	started chan<- struct{}
	release <-chan struct{}
}

func (m *blockingReadOnlyMySQL) CheckReadOnly(context.Context) (bool, error) {
	m.mu.Lock()
	readOnly, err := m.readOnly, m.err
	m.mu.Unlock()
	m.started <- struct{}{}
	<-m.release
	return readOnly, err
}

func TestPoll_DoesNotSupersedeConcurrentPromotionWithStaleObservations(t *testing.T) {
	oldPrimary := &mockMySQL{readOnly: false}
	newPrimary := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(oldPrimary, newPrimary)
	pollN(tm, 2)

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	tm.sites[0].mysql = &blockingReadOnlyMySQL{mockMySQL: oldPrimary, started: started, release: release}
	tm.sites[1].mysql = &blockingReadOnlyMySQL{mockMySQL: newPrimary, started: started, release: release}

	pollDone := make(chan struct{})
	go func() {
		tm.Poll(context.Background())
		close(pollDone)
	}()
	<-started
	<-started

	oldPrimary.mu.Lock()
	oldPrimary.readOnly = true
	oldPrimary.mu.Unlock()
	newPrimary.mu.Lock()
	newPrimary.readOnly = false
	newPrimary.mu.Unlock()
	tm.recordFailover(context.Background(), tm.clock.Now(), "dc2", "uuid:1-10")

	close(release)
	<-pollDone

	tm.mu.RLock()
	pending := tm.promotedSite
	tm.mu.RUnlock()
	if pending != "dc2" {
		t.Fatalf("stale poll superseded pending promotion: got %q, want dc2", pending)
	}

	tm.sites[0].mysql = oldPrimary
	tm.sites[1].mysql = newPrimary
	tm.Poll(context.Background())
	if got := tm.Status().ActiveSite; got != "dc2" {
		t.Fatalf("active site after fresh poll = %q, want dc2", got)
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
// The N-site donor selector is empty-detection plus source selection:
// it prefers the first writable-with-data donor and falls back to an
// empty writable donor for freshly reset clusters. Every case below
// asserts that bookkeeping on the site-name level.

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

func TestDetectEmptySite_EmptyWritableDonorAfterReset(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "dc1" || empty != "dc2" {
		t.Errorf("expected empty writable donor dc1 to bootstrap empty dc2, got donor=%q empty=%q", donor, empty)
	}
}

func TestDetectEmptySite_FreshInitializedReplicaHasSetupGTIDs(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "bbbb:1-9", hasUserSchemas: testBoolPtr(false)} // MySQL init statements only
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "dc1" || empty != "dc2" {
		t.Errorf("expected donor=dc1 empty=dc2 for fresh replica with setup GTIDs, got donor=%q empty=%q", donor, empty)
	}
}

func TestDetectEmptySite_UserSchemaBlocksFreshInitializedReplica(t *testing.T) {
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "bbbb:1-9", hasUserSchemas: testBoolPtr(true)}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "" || empty != "" {
		t.Errorf("expected no clone when read-only site has user schemas, got donor=%q empty=%q", donor, empty)
	}
}

// TestDetectEmptySite_SharedHistorySchemalessNotEmpty guards the residual
// #130 path: a returning member that shares the cluster's GTID UUIDs but has
// no user schemas must not be auto-cloned. Without the sharesHistory gate,
// detectEmptySite would hand it to clone on the same poll that
// initiateRecovery correctly RecoveryBlocked it — wiping the only copy of
// divergent transactions.
func TestDetectEmptySite_SharedHistorySchemalessNotEmpty(t *testing.T) {
	clusterGTID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-12"
	// Diverged but same UUID family as the primary — sharesHistory must
	// keep this off the auto-clone path even with no user schemas.
	oldGTID := clusterGTID + ",aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:13-15"
	site0 := &mockMySQL{readOnly: false, gtidExecuted: clusterGTID}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: oldGTID, hasUserSchemas: testBoolPtr(false)}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "" || empty != "" {
		t.Errorf("shared-history schemaless site must not be empty, got donor=%q empty=%q", donor, empty)
	}
	site0.mu.Lock()
	historyProbeHadDeadline := site0.gtidExecutedHadDeadline
	site0.mu.Unlock()
	if !historyProbeHadDeadline {
		t.Fatal("shared-history probe did not receive a bounded context")
	}
}

// TestDetectEmptySite_SkipsSitesUnderRecovery ensures auto-clone cannot
// race a RecoveryBlocked / RecoveryInProgress report on the same poll.
func TestDetectEmptySite_SkipsSitesUnderRecovery(t *testing.T) {
	// Foreign UUID + no schemas would otherwise look fresh.
	site0 := &mockMySQL{readOnly: false, gtidExecuted: "aaaa:1-100"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "bbbb:1-9", hasUserSchemas: testBoolPtr(false)}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly
	tm.recovery["dc2"] = &siteRecovery{state: recoveryStateBlocked, divergentGtid: "bbbb:1-9", divergentCount: 9}

	donor, empty := tm.detectEmptySite(context.Background())
	if donor != "" || empty != "" {
		t.Errorf("RecoveryBlocked site must not be auto-cloned, got donor=%q empty=%q", donor, empty)
	}
}

func TestBootstrapIdlePhaseIncludesDone(t *testing.T) {
	for _, phase := range []BootstrapPhase{BootstrapPhaseNone, BootstrapPhaseDone, BootstrapPhaseFailed} {
		if !bootstrapIdlePhase(phase) {
			t.Fatalf("phase %q should be idle", phase)
		}
	}
	for _, phase := range []BootstrapPhase{BootstrapPhaseCloning, BootstrapPhaseRestarting, BootstrapPhaseSetupRepl} {
		if bootstrapIdlePhase(phase) {
			t.Fatalf("phase %q should not be idle", phase)
		}
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

type testKeyringGate struct {
	ready       bool
	calls       atomic.Int32
	notifyCalls atomic.Int32
	notifyErr   error
}

func (g *testKeyringGate) RequestKeyringUnseal(context.Context, types.NamespacedName, string) (bool, error) {
	g.calls.Add(1)
	return g.ready, nil
}

func (g *testKeyringGate) NotifyCloneComplete(context.Context, types.NamespacedName, string) error {
	g.notifyCalls.Add(1)
	return g.notifyErr
}

func TestReclone_HappyPath(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	tm.autoBootstrapSuppressed = true

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

func TestReclone_PreservesPendingRequestUntilKeyringUnsealed(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	replica := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(primary, replica)
	tm.autoBootstrapSuppressed = true
	pollN(tm, 2)
	gate := &testKeyringGate{}
	tm.SetKeyringGate(gate)
	var consumed atomic.Int32
	tm.RecloneCompleteCallback = func(context.Context, string) error {
		consumed.Add(1)
		return nil
	}
	tm.SetRecloneSite("dc2")

	if tm.checkReclone(context.Background()) {
		t.Fatal("reclone started before the keyring was unsealed")
	}
	if gate.notifyCalls.Load() != 0 {
		t.Fatal("NotifyCloneComplete must not run before bootstrap starts")
	}
	tm.mu.RLock()
	pending := tm.reclonePendingSite
	tm.mu.RUnlock()
	if pending != "dc2" || consumed.Load() != 0 {
		t.Fatalf("pending reclone was lost: %q", pending)
	}

	gate.ready = true
	if !tm.checkReclone(context.Background()) {
		t.Fatal("reclone did not start after the keyring became ready")
	}
	deadline := time.Now().Add(time.Second)
	for consumed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	tm.mu.RLock()
	pending = tm.reclonePendingSite
	tm.mu.RUnlock()
	if pending != "" || gate.calls.Load() != 2 || consumed.Load() != 1 {
		t.Fatalf("pending=%q calls=%d consumed=%d", pending, gate.calls.Load(), consumed.Load())
	}
	if gate.notifyCalls.Load() != 1 {
		t.Fatalf("NotifyCloneComplete calls = %d, want 1 after bootstrap finishes", gate.notifyCalls.Load())
	}
}

func TestReclone_AnnotationCleanupFailureDoesNotRepeatSuccessfulClone(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	replica := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(primary, replica)
	tm.autoBootstrapSuppressed = true
	pollN(tm, 2)
	var cleanupCalls atomic.Int32
	tm.RecloneCompleteCallback = func(context.Context, string) error {
		if cleanupCalls.Add(1) == 1 {
			return errors.New("api unavailable")
		}
		return nil
	}
	tm.SetRecloneSite("dc2")
	if !tm.checkReclone(context.Background()) {
		t.Fatal("expected reclone to start")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		tm.mu.RLock()
		completed := tm.recloneCompletedSite
		tm.mu.RUnlock()
		if completed == "dc2" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if tm.checkReclone(context.Background()) {
		t.Fatal("annotation cleanup retry started a second clone")
	}
	tm.mu.RLock()
	completed := tm.recloneCompletedSite
	tm.mu.RUnlock()
	if completed != "" || cleanupCalls.Load() != 2 {
		t.Fatalf("completed=%q cleanupCalls=%d", completed, cleanupCalls.Load())
	}
}

func TestReleaseCloneHold_RetriesNotifyWithoutCloning(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	replica := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(primary, replica)
	tm.autoBootstrapSuppressed = true
	pollN(tm, 2)
	gate := &testKeyringGate{ready: true, notifyErr: errors.New("status conflict")}
	tm.SetKeyringGate(gate)
	tm.SetRecloneSite("dc2")
	if !tm.checkReclone(context.Background()) {
		t.Fatal("expected reclone to start")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		tm.mu.RLock()
		pending := tm.cloneHoldReleaseSite
		phase := tm.bootstrapPhase
		tm.mu.RUnlock()
		if pending == "dc2" && bootstrapIdlePhase(phase) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	firstNotifies := gate.notifyCalls.Load()
	if firstNotifies < 1 {
		t.Fatal("expected NotifyCloneComplete after bootstrap")
	}
	gate.notifyErr = nil
	tm.releaseCloneHold(context.Background())
	tm.mu.RLock()
	pending := tm.cloneHoldReleaseSite
	phase := tm.bootstrapPhase
	tm.mu.RUnlock()
	if pending != "" {
		t.Fatalf("cloneHoldReleaseSite = %q after successful retry", pending)
	}
	if gate.notifyCalls.Load() != firstNotifies+1 {
		t.Fatalf("notify retry calls = %d, want %d", gate.notifyCalls.Load(), firstNotifies+1)
	}
	if phase == BootstrapPhaseCloning {
		t.Fatal("notify retry started another clone")
	}
}

func TestStartBootstrap_ClearsStaleCloneHoldRelease(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	replica := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(primary, replica)
	tm.autoBootstrapSuppressed = true
	pollN(tm, 2)
	gate := &testKeyringGate{ready: true}
	tm.SetKeyringGate(gate)
	tm.mu.Lock()
	tm.cloneHoldReleaseSite = "dc2"
	tm.mu.Unlock()

	if !tm.startBootstrapByName(context.Background(), "dc1", "dc2", "auto-clone") {
		t.Fatal("expected bootstrap to start")
	}
	tm.mu.RLock()
	pending := tm.cloneHoldReleaseSite
	tm.mu.RUnlock()
	if pending != "" {
		t.Fatalf("cloneHoldReleaseSite = %q after starting a new clone; a stale release would reseal mid-CLONE", pending)
	}
}

func TestReclone_ReadOnlyRecipientUsesActivePrimary(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	reader := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(primary, reader)
	tm.autoBootstrapSuppressed = true
	tm.sites[1].role = state.SiteRoleReadOnly
	tm.cfg.Sites[1].Role = state.SiteRoleReadOnly
	pollN(tm, 2)

	tm.SetRecloneSite("dc2")
	if !tm.checkReclone(context.Background()) {
		t.Fatal("expected reader reclone to start")
	}
	tm.mu.RLock()
	source := tm.bootstrapSource
	tm.mu.RUnlock()
	var primaryHost string
	deadline := time.Now().Add(time.Second)
	for primaryHost == "" && time.Now().Before(deadline) {
		reader.mu.Lock()
		primaryHost = reader.clonePrimaryHost
		reader.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	if source != "reclone" || primaryHost != "mysql-dc1" {
		t.Fatalf("bootstrap = source %q primary host %q, want reclone/mysql-dc1", source, primaryHost)
	}
}

func TestReclone_CannotReclonePrimary(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	tm.autoBootstrapSuppressed = true

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

func TestReclone_PreservesRecoveryStateUntilReplicationHealthy(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	tm.autoBootstrapSuppressed = true

	pollN(tm, 2)

	// Simulate RecoveryBlocked state on dc2.
	tm.mu.Lock()
	tm.recovery["dc2"] = &siteRecovery{state: recoveryStateBlocked, divergentGtid: "aaaa:50-55", divergentCount: 6}
	tm.mu.Unlock()

	tm.SetRecloneSite("dc2")
	tm.checkReclone(context.Background())

	tm.mu.RLock()
	rec := tm.recovery["dc2"]
	tm.mu.RUnlock()

	if rec == nil || rec.state != recoveryStateBlocked {
		t.Fatalf("dc2 recovery should remain blocked until replication is healthy, got %+v", rec)
	}
	if rec.divergentGtid != "aaaa:50-55" {
		t.Errorf("divergentGtid should remain until replication is healthy, got %q", rec.divergentGtid)
	}
	if rec.divergentCount != 6 {
		t.Errorf("divergentCount should remain until replication is healthy, got %d", rec.divergentCount)
	}
}

func TestCheckRecovery_ClearsHealthyRecoverySiteWithoutLastFailoverTarget(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	tm.autoBootstrapSuppressed = true

	pollN(tm, 2)

	tm.mu.Lock()
	tm.recovery["dc2"] = &siteRecovery{state: recoveryStateBlocked, divergentGtid: "aaaa:50-55", divergentCount: 6}
	tm.lastFailoverTarget = ""
	tm.sites[1].sourceConvergenceState = sourceConvergenceConverged
	tm.mu.Unlock()

	changed := tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{
		nil,
		{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"},
	})
	if !changed {
		t.Fatal("checkRecovery should report a status change after clearing healthy recovery state")
	}

	tm.mu.RLock()
	rec := tm.recovery["dc2"]
	tm.mu.RUnlock()
	if rec != nil {
		t.Fatalf("recovery state not cleared: %+v", rec)
	}
}

func TestCheckRecovery_ClearsHealthyRecoverySiteWithoutReplicationCredentials(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)

	tm.mu.Lock()
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly
	tm.recovery["dc2"] = &siteRecovery{state: recoveryStateInProgress}
	tm.lastFailoverTarget = "dc1"
	tm.bootstrapCfg = BootstrapConfig{}
	tm.sites[1].sourceConvergenceState = sourceConvergenceConverged
	tm.mu.Unlock()

	changed := tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{
		nil,
		{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"},
	})
	if !changed {
		t.Fatal("checkRecovery should clear healthy recovery state even when retry credentials are unavailable")
	}

	tm.mu.RLock()
	rec := tm.recovery["dc2"]
	tm.mu.RUnlock()
	if rec != nil {
		t.Fatalf("recovery state not cleared: %+v", rec)
	}
}

func TestCheckRecovery_PreservesBlockedRecordedTargetDuringSplitBrain(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: false}
	tm, _, _ := newTestTopologyManager(site0, site1)

	tm.mu.Lock()
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateWritable
	tm.recovery["dc2"] = &siteRecovery{
		state:          recoveryStateBlocked,
		divergentGtid:  "aaaa:50-55",
		divergentCount: 6,
		drainComplete:  true,
	}
	tm.lastFailoverTarget = "dc2"
	tm.mu.Unlock()

	if tm.clearHealthyRecoverySites(context.Background(), nil) {
		t.Fatal("split-brain must not clear blocked divergence from the recorded target")
	}
	tm.mu.RLock()
	rec := tm.recovery["dc2"]
	tm.mu.RUnlock()
	if rec == nil || rec.divergentGtid != "aaaa:50-55" {
		t.Fatalf("divergence evidence was lost without unique authority: %+v", rec)
	}
	if rec.drainComplete {
		t.Fatal("writable rogue site retained stale drain completion")
	}

	site0.readOnly = true
	tm.sites[0].state = state.StateReadOnly
	if !tm.clearHealthyRecoverySites(context.Background(), nil) {
		t.Fatal("unique, directly confirmed recorded target should clear stale divergence")
	}
}

func TestCheckRecovery_DrainsSurvivingConnectionsBeforeCompletion(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{
		readOnly:              true,
		killConnectionResults: []int{2, 1, 0},
	}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.mu.Lock()
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly
	tm.recovery["dc2"] = &siteRecovery{state: recoveryStateInProgress}
	tm.lastFailoverTarget = "dc1"
	tm.sites[1].sourceConvergenceState = sourceConvergenceConverged
	tm.mu.Unlock()

	repl := []*mysql.ReplicaStatus{
		nil,
		{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"},
	}
	for attempt := 1; attempt <= 3; attempt++ {
		changed := tm.checkRecovery(context.Background(), repl)
		if attempt < 3 && changed {
			t.Fatalf("recovery completed after drain pass %d, before the empty pass", attempt)
		}
		if attempt == 3 && !changed {
			t.Fatal("healthy recovery state was not completed after the empty pass")
		}
	}
	if site1.killConnectionCalls != 3 {
		t.Fatalf("connection drain calls = %d, want 3 passes ending at zero", site1.killConnectionCalls)
	}
	tm.mu.RLock()
	rec := tm.recovery["dc2"]
	tm.mu.RUnlock()
	if rec != nil {
		t.Fatalf("recovery state cleared before drain completion: %+v", rec)
	}
}

func TestCheckRecovery_DrainsBeforeOldPrimaryMutation(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	site0 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	site1 := &mockMySQL{
		readOnly:              true,
		gtidExecuted:          gtid,
		killConnectionResults: []int{1, 0},
	}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	setRecoveredTopology(tm)

	if changed := tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil}); changed {
		t.Fatal("recovery mutation started before the drain reached an empty pass")
	}
	if got := site1.startReplicaCallCount(); got != 0 {
		t.Fatalf("recovery mutated old primary during drain: %d sequences", got)
	}
	if changed := tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil}); !changed {
		t.Fatal("expected recovery to start after the empty drain pass")
	}
	if site1.killConnectionCalls != 2 {
		t.Fatalf("connection drain calls = %d, want 2", site1.killConnectionCalls)
	}
	if got := site1.startReplicaCallCount(); got != 1 {
		t.Fatalf("recovery did not proceed after clean drain: %d sequences", got)
	}
}

func TestCheckRecovery_DrainRetriesAfterKillError(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	site0 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	site1 := &mockMySQL{
		readOnly:              true,
		gtidExecuted:          gtid,
		killConnectionResults: []int{0, 1, 0},
		killConnectionErrs:    []error{errors.New("process list unavailable"), nil, nil},
	}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	setRecoveredTopology(tm)

	for attempt := 1; attempt <= 3; attempt++ {
		changed := tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
		if attempt < 3 && changed {
			t.Fatalf("recovery completed after drain attempt %d", attempt)
		}
		if attempt == 3 && !changed {
			t.Fatal("recovery did not resume after retry reached an empty pass")
		}
	}
	if site1.killConnectionCalls != 3 {
		t.Fatalf("connection drain calls = %d, want error, eviction, then empty pass", site1.killConnectionCalls)
	}
	if got := site1.startReplicaCallCount(); got != 1 {
		t.Fatalf("recovery sequence count = %d, want 1", got)
	}
}

func TestCheckRecovery_PersistsInProgressAndSuppressesImmediateRetry(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	site0 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: gtid}
	clk := clock.NewFakeClock(time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(site0, site1, clk)
	setRecoveredTopology(tm)

	var snapshots []TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) {
		snapshots = append(snapshots, s)
	}

	changed := tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
	if !changed {
		t.Fatal("expected recovery to start")
	}
	if got := site1.startReplicaCallCount(); got != 1 {
		t.Fatalf("expected one recovery sequence, got %d", got)
	}
	if len(snapshots) == 0 || snapshots[0].RecoverySite != "dc2" || snapshots[0].RecoveryState != recoveryStateInProgress {
		t.Fatalf("expected RecoveryInProgress snapshot before recovery, got %#v", snapshots)
	}

	changed = tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
	if changed {
		t.Fatal("recovery should not retry during stabilization window")
	}
	if got := site1.startReplicaCallCount(); got != 1 {
		t.Fatalf("expected recovery sequence to remain suppressed, got %d", got)
	}

	clk.Advance(recoveryRetryDelay + time.Second)
	changed = tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
	if !changed {
		t.Fatal("expected recovery retry after stabilization window")
	}
	if got := site1.startReplicaCallCount(); got != 2 {
		t.Fatalf("expected recovery sequence to retry once, got %d", got)
	}
}

func TestCheckRecovery_RestoredInProgressRetriesImmediatelyWhenUnhealthy(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	site0 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: gtid}
	clk := clock.NewFakeClock(time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(site0, site1, clk)
	setRecoveredTopology(tm)
	tm.SetRecoveryInProgress("dc2")

	changed := tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
	if !changed {
		t.Fatal("expected restored in-progress recovery to retry when unhealthy")
	}
	if got := site1.startReplicaCallCount(); got != 1 {
		t.Fatalf("expected one recovery sequence after rehydrate, got %d", got)
	}
}

// --- checkPrimaryReassert ---

// setWedgedTopology puts the manager into the fenced-promoted-primary
// wedge: every site reachable and read-only after a failover to target.
func setWedgedTopology(tm *TopologyManager, target string) {
	tm.mu.Lock()
	tm.sites[0].state = state.StateReadOnly
	tm.sites[1].state = state.StateReadOnly
	tm.lastFailoverTarget = target
	tm.mu.Unlock()
}

func TestPrimaryReassert_RestoresFencedPromotedTarget(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8"}
	tm, _, dns := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.mu.Lock()
	tm.promotionGtidExecuted = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9"
	tm.mu.Unlock()

	if !tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("expected re-assert to restore the fenced promoted target")
	}
	if ro, _ := site0.CheckReadOnly(context.Background()); ro {
		t.Error("target should be writable after re-assert")
	}
	if ro, _ := site1.CheckReadOnly(context.Background()); !ro {
		t.Error("peer must stay read-only")
	}
	if got := dns.getLastIP(); got != "1.1.1.1" {
		t.Errorf("DNS should point at the re-asserted target LB, got %q", got)
	}
}

func TestPrimaryReassert_NoFailoverHistoryNoOp(t *testing.T) {
	// Without failover history the all-read-only state is the startup
	// condition EvalCrossSite refuses to elect from; re-assert must
	// preserve that invariant.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "")

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must not fire without a lastFailoverTarget")
	}
	if ro, _ := site0.CheckReadOnly(context.Background()); !ro {
		t.Error("no site may be made writable without failover history")
	}
}

func TestPrimaryReassert_RefusesWhenPeerWritable(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: false, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.mu.Lock()
	tm.sites[1].state = state.StateWritable
	tm.mu.Unlock()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must not fire while another site is writable")
	}
	if ro, _ := site0.CheckReadOnly(context.Background()); !ro {
		t.Error("target must stay read-only while a peer is writable")
	}
}

func TestPrimaryReassert_RefusesWhenPeerUnreachable(t *testing.T) {
	// An unreachable peer hands the decision to the normal
	// unreachable+read-only promotion path in EvalCrossSite.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.mu.Lock()
	tm.sites[1].state = state.StateUnreachable
	tm.mu.Unlock()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must not fire while a peer is unreachable")
	}
}

func TestPrimaryReassert_RefusesOnDivergentPeer(t *testing.T) {
	// The peer carries a transaction the target lacks: restoring the
	// target would abandon it. Human review required.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must not fire when a peer has transactions the target lacks")
	}
	if ro, _ := site0.CheckReadOnly(context.Background()); !ro {
		t.Error("target must stay read-only on divergence")
	}
}

func TestPrimaryReassert_RefusesWhenTargetLostPromotionGtid(t *testing.T) {
	// The recorded promotion GTID set is no longer contained in the
	// target — it was wiped or restored since the promotion, so the
	// failover history no longer describes this data lineage.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "cccccccc-cccc-cccc-cccc-cccccccccccc:1-3"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "cccccccc-cccc-cccc-cccc-cccccccccccc:1-2"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.mu.Lock()
	tm.promotionGtidExecuted = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9"
	tm.mu.Unlock()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must not fire when the target lost the recorded promotion GTID set")
	}
}

func TestPrimaryReassert_RefusesOnMalformedPromotionGtid(t *testing.T) {
	// status.promotionGtidExecuted is written by the operator from MySQL
	// itself; a parse failure means corruption or manual tampering, and
	// the safety argument depends on that value — refuse, don't skip.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.mu.Lock()
	tm.promotionGtidExecuted = "not-a-gtid-set"
	tm.mu.Unlock()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must refuse when the recorded promotion GTID set is malformed")
	}
	if ro, _ := site0.CheckReadOnly(context.Background()); !ro {
		t.Error("target must stay read-only when the recorded promotion GTID set is malformed")
	}
}

func TestPrimaryReassert_CooldownRateLimits(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8"}
	clk := clock.NewFakeClock(time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(site0, site1, clk)
	tm.failoverCooldown = 30 * time.Second
	setWedgedTopology(tm, "dc1")

	if !tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("expected first re-assert to fire")
	}

	// The sidecar fenced the target again for a persistent reason: the
	// operator must not fight it at poll frequency.
	site0.setReadOnly(true)
	clk.Advance(5 * time.Second)
	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must be rate-limited by the failover cooldown")
	}

	clk.Advance(30 * time.Second)
	if !tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("expected re-assert to fire again after the cooldown")
	}
}

func TestPrimaryReassert_DefersDuringPlannedFailover(t *testing.T) {
	// A planned failover fences the source deliberately; while it is
	// active the whole group can legitimately be read-only.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.SetPlannedFailoverActive(true)

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must defer while a planned failover is active")
	}
	if ro, _ := site0.CheckReadOnly(context.Background()); !ro {
		t.Error("target must stay read-only during a planned failover")
	}
}

func TestPrimaryReassert_DefersDuringPendingPromotion(t *testing.T) {
	// A pending promotion is still being confirmed; the promotion
	// pipeline owns the topology until the guard clears or expires.
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.mu.Lock()
	tm.promotedSite = "dc1"
	tm.promotedAt = time.Now()
	tm.mu.Unlock()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must defer while a promotion is pending confirmation")
	}
}

func TestReclone_BlockedDuringBootstrap(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	tm.autoBootstrapSuppressed = true

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
	tm.autoBootstrapSuppressed = true

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
		SourceHost: "mysql-dc1",
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

func TestPoll_StatusCallbackFiresWhenReplicationBecomesHealthy(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{
		readOnly: true,
		replicaStatusVal: &mysql.ReplicaStatus{
			IORunning:  false,
			SQLRunning: false,
			SourceHost: "",
		},
	}
	tm, _, _ := newTestTopologyManager(site0, site1)

	// Settle site states with replication still unhealthy, then start
	// counting callbacks so the assertion is about replication-only status.
	pollN(tm, 3)

	var callbacks int
	var captured TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		callbacks++
		captured = snap
	}

	site1.mu.Lock()
	site1.replicaStatusVal = &mysql.ReplicaStatus{
		IORunning:  true,
		SQLRunning: true,
		SourceHost: "dc1",
	}
	site1.mu.Unlock()

	pollN(tm, 1)
	if callbacks != 1 {
		t.Fatalf("first healthy tick should persist source convergence, got %d callbacks", callbacks)
	}
	pollN(tm, 1)
	if callbacks != 2 {
		t.Fatalf("debounced replication health should trigger a second callback, got %d", callbacks)
	}
	if captured.Sites[1].State != state.StateReadOnly {
		t.Fatalf("site1 state = %s, want read-only", captured.Sites[1].State)
	}
	if captured.Sites[1].Replication == nil || !captured.Sites[1].Replication.IORunning || !captured.Sites[1].Replication.SQLRunning {
		t.Fatalf("callback snapshot did not include healthy replication: %#v", captured.Sites[1].Replication)
	}
}

func TestPoll_StatusCallbackFiresWhenReplicaStatusErrorsAfterHealthy(t *testing.T) {
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
	replicating := tm.sites[1].replicating
	tm.mu.RUnlock()
	if !replicating {
		t.Fatal("setup: expected site1.replicating=true before replica-status error")
	}

	var callbacks int
	var captured TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		callbacks++
		captured = snap
	}

	site1.mu.Lock()
	site1.replicaStatusErr = errors.New("replica status unavailable")
	site1.mu.Unlock()

	pollN(tm, 1)

	if callbacks != 1 {
		t.Fatalf("replica-status error after healthy state should trigger one callback, got %d", callbacks)
	}
	if captured.Sites[1].State != state.StateReadOnly {
		t.Fatalf("site1 state = %s, want read-only", captured.Sites[1].State)
	}
	if captured.Sites[1].Replication != nil {
		t.Fatalf("replication snapshot on error = %#v, want nil", captured.Sites[1].Replication)
	}
	tm.mu.RLock()
	replicating = tm.sites[1].replicating
	tm.mu.RUnlock()
	if replicating {
		t.Fatal("replicating should be false after replica-status error")
	}
}

// TestCheckUpdate_RetainsUnhealthyStandbyDrift verifies an unsafe follower is
// retained by the ordered plan without applying its Deployment update.
func TestCheckUpdate_RetainsUnhealthyStandbyDrift(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{
		readOnly:         true,
		replicaStatusVal: &mysql.ReplicaStatus{}, // empty — threads stopped, no source
	}
	tm, _, _ := newTestTopologyManager(site0, site1)
	// Attach an UpdateController and ApplyUpdate callback so checkUpdate doesn't
	// early-return on nil dependencies.
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	applied := make(chan string, 1)
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		applied <- site
		return nil
	}
	// Poll enough to settle states (site1 -> StateReadOnly with RecoveryThreshold=2).
	pollN(tm, 3)

	// Drift on standby — the classic trigger for an ordered update.
	tm.SetSpecDriftSites([]string{"dc2"})
	done := make(chan struct{})
	tm.StatusCallback = func(TopologySnapshot) { close(done) }

	started := tm.checkUpdate(context.Background())
	if !started {
		t.Fatal("checkUpdate must start so independently safe targets can be processed")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ordered plan did not complete")
	}
	select {
	case site := <-applied:
		t.Fatalf("unsafe standby %s was updated", site)
	default:
	}
	tm.mu.RLock()
	drift := append([]string(nil), tm.specDriftSites...)
	tm.mu.RUnlock()
	if len(drift) != 1 || drift[0] != "dc2" {
		t.Fatalf("unhealthy standby drift was not retained: %v", drift)
	}
}

func TestCheckUpdate_ProcessesHealthyCandidateBeforeUnhealthyReader(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	candidate := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-primary-internal.ns.svc.cluster.local",
	}}
	reader := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning: false, SQLRunning: false, SourceHost: "mysql-primary-internal.ns.svc.cluster.local",
	}}
	tm := newConvergenceManager(t, []state.SiteRole{
		state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly,
	}, primary, candidate, reader)
	tm.sites[1].replicating = true
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	var applied []string
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		applied = append(applied, site)
		return nil
	}
	tm.SetSpecDriftSites([]string{"follower-a", "follower-b"})
	done := make(chan struct{})
	tm.StatusCallback = func(TopologySnapshot) { close(done) }

	if !tm.checkUpdate(context.Background()) {
		t.Fatal("ordered plan did not start")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ordered plan did not finish")
	}
	if got := strings.Join(applied, ","); got != "follower-a" {
		t.Fatalf("applied = %q, want healthy candidate only", got)
	}
	tm.mu.RLock()
	drift := append([]string(nil), tm.specDriftSites...)
	tm.mu.RUnlock()
	if len(drift) != 1 || drift[0] != "follower-b" {
		t.Fatalf("remaining drift = %v, want unhealthy reader only", drift)
	}
}

// TestCheckUpdate_UpdatesHealthyReaderWhenActiveHasNoStandby pins the
// v1.1.0 encryption-adoption stall: after the unsealed roll, the new
// primary is drifted onto the sealed hash, the only promotable standby
// is divergent, and the reader is a healthy direct replica. The plan
// must still update the reader instead of returning before ExecuteTargets.
func TestCheckUpdate_UpdatesHealthyReaderWhenActiveHasNoStandby(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	divergent := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{}}
	reader := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-primary-internal.ns.svc.cluster.local",
	}}
	tm := newConvergenceManager(t, []state.SiteRole{
		state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly,
	}, primary, divergent, reader)
	tm.sites[2].replicating = true
	tm.sites[2].sourceHost = "mysql-primary-internal.ns.svc.cluster.local"
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	var mu sync.Mutex
	var applied []string
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		mu.Lock()
		applied = append(applied, site)
		mu.Unlock()
		return nil
	}
	tm.SetSpecDriftSites([]string{"primary", "follower-a", "follower-b"})
	done := make(chan struct{})
	tm.StatusCallback = func(TopologySnapshot) { close(done) }

	if !tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate must start so the healthy reader can be sealed")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ordered plan did not finish")
	}
	mu.Lock()
	got := strings.Join(applied, ",")
	mu.Unlock()
	if got != "follower-b" {
		t.Fatalf("applied = %q, want reader only", got)
	}
	tm.mu.RLock()
	drift := append([]string(nil), tm.specDriftSites...)
	tm.mu.RUnlock()
	if len(drift) != 2 {
		t.Fatalf("remaining drift = %v, want active + divergent standby", drift)
	}
	remaining := map[string]bool{}
	for _, name := range drift {
		remaining[name] = true
	}
	if !remaining["primary"] || !remaining["follower-a"] || remaining["follower-b"] {
		t.Fatalf("remaining drift = %v, want primary and follower-a", drift)
	}
}

func TestCheckUpdate_DefersWhenRemainingDriftIsUnsafeFollowers(t *testing.T) {
	// After the reader has already been sealed, leftover drift is the
	// active site plus a divergent standby. Starting the plan every
	// poll would only fail requireDirectReplica and log "ordered update
	// failed" forever.
	primary := &mockMySQL{readOnly: false}
	divergent := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{}}
	reader := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-primary-internal.ns.svc.cluster.local",
	}}
	tm := newConvergenceManager(t, []state.SiteRole{
		state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly,
	}, primary, divergent, reader)
	tm.sites[2].replicating = true
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		t.Fatalf("unexpected apply of %s", site)
		return nil
	}
	tm.SetSpecDriftSites([]string{"primary", "follower-a"})
	if tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate must not restart for a drifted-but-unhealthy follower")
	}
}

func TestCheckUpdate_WrongSourcePromotableIsNotAStandby(t *testing.T) {
	// A promotable replica with threads up but pointed at the wrong
	// source must not count as haveStandby. Otherwise checkUpdate starts
	// a handoff that requireDirectReplica immediately refuses.
	primary := &mockMySQL{readOnly: false}
	wrongSource := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-stale-internal.ns.svc.cluster.local",
	}}
	tm := newConvergenceManager(t, []state.SiteRole{
		state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate,
	}, primary, wrongSource)
	tm.sites[1].replicating = true
	tm.sites[1].sourceHost = "mysql-stale-internal.ns.svc.cluster.local"
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		t.Fatalf("unexpected apply of %s", site)
		return nil
	}
	tm.SetSpecDriftSites([]string{"primary"})
	if tm.checkUpdate(context.Background()) {
		t.Fatal("wrong-source promotable standby must not unlock an active-site update")
	}
}

func TestCheckUpdate_DefersWhenDriftedFollowerHasWrongSource(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	wrongSource := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-stale-internal.ns.svc.cluster.local",
	}}
	tm := newConvergenceManager(t, []state.SiteRole{
		state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly,
	}, primary, wrongSource)
	tm.sites[1].replicating = true
	tm.sites[1].sourceHost = "mysql-stale-internal.ns.svc.cluster.local"
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		t.Fatalf("unexpected apply of %s", site)
		return nil
	}
	tm.SetSpecDriftSites([]string{"primary", "follower-a"})
	if tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate must not restart for a drifted follower replicating from the wrong source")
	}
}

func TestCheckUpdate_DefersWhenOnlyActiveDriftedWithoutStandby(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	divergent := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{}}
	tm := newConvergenceManager(t, []state.SiteRole{
		state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate,
	}, primary, divergent)
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		t.Fatalf("unexpected apply of %s", site)
		return nil
	}
	tm.SetSpecDriftSites([]string{"primary"})
	if tm.checkUpdate(context.Background()) {
		t.Fatal("checkUpdate must not start a handoff with no standby and no follower drift")
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

// TestCheckRecovery_SchemalessOldPrimaryStillRecovers guards the scenario-12
// regression: a returning old primary that shares the cluster's GTID history
// but holds no user schemas was classified as a fresh datadir and silently
// skipped, leaving auto-clone to adopt it without ever running the divergence
// comparison. A cluster legitimately has no user schemas before its first app
// write, or after the last database is dropped.
func TestCheckRecovery_SchemalessOldPrimaryStillRecovers(t *testing.T) {
	clusterGTID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-12"

	tests := []struct {
		name             string
		oldGTID          string
		wantRecovery     bool
		wantState        string
		wantStartReplica bool
	}{
		{
			// Contained by the new primary's set: safe to rejoin as a replica.
			name:             "shares cluster history: recover despite no user schemas",
			oldGTID:          clusterGTID,
			wantRecovery:     true,
			wantState:        recoveryStateInProgress,
			wantStartReplica: true,
		},
		{
			// The whole point of the fix: a returning member carrying
			// transactions the new primary never saw must be BLOCKED, not
			// silently cloned over. Asserting only "recovery engaged" would
			// let an unsafe auto-recovery pass.
			name:             "diverged but shares history: blocked, never auto-started",
			oldGTID:          clusterGTID + ",aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:13-15",
			wantRecovery:     true,
			wantState:        recoveryStateBlocked,
			wantStartReplica: false,
		},
		{
			name:         "fresh datadir GTIDs under its own uuid: left to auto-clone",
			oldGTID:      "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-3",
			wantRecovery: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			site0 := &mockMySQL{readOnly: false, gtidExecuted: clusterGTID}
			site1 := &mockMySQL{readOnly: true, gtidExecuted: tt.oldGTID, hasUserSchemas: testBoolPtr(false)}
			clk := clock.NewFakeClock(time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC))
			tm, _, _ := newTestTopologyManagerWithBootstrapClock(site0, site1, clk)
			setRecoveredTopology(tm)

			tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})

			started := site1.startReplicaCallCount() > 0
			tm.mu.RLock()
			rec := tm.recovery["dc2"]
			tm.mu.RUnlock()

			got := started || rec != nil
			if got != tt.wantRecovery {
				t.Fatalf("recovery engaged = %v, want %v (startReplica calls=%d, state=%+v)",
					got, tt.wantRecovery, site1.startReplicaCallCount(), rec)
			}
			if !tt.wantRecovery {
				return
			}
			gotState := ""
			if rec != nil {
				gotState = rec.state
			}
			if gotState != tt.wantState {
				t.Errorf("recovery state = %q, want %q (rec=%+v)", gotState, tt.wantState, rec)
			}
			if started != tt.wantStartReplica {
				t.Errorf("StartReplica called = %v, want %v (calls=%d)",
					started, tt.wantStartReplica, site1.startReplicaCallCount())
			}
		})
	}
}

// TestCheckRecovery_UnreachablePrimaryFailsSafeTowardComparison ensures a
// slow or unreachable new primary can never demote a returning old primary to
// a fresh-datadir verdict. sharesHistory answers "shares" on probe failure so
// the schema check is not consulted and recovery proceeds to its own bounded
// comparison (which then backs off), rather than silently skipping into
// auto-clone. Without this, a schemaless site whose only UUIDs look foreign
// while the primary is briefly unreachable would be wiped.
func TestCheckRecovery_UnreachablePrimaryFailsSafeTowardComparison(t *testing.T) {
	// Foreign-looking GTIDs: if the primary probe failed open toward
	// "fresh", the schema check would mark this empty and skip recovery.
	oldGTID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-3"
	site0 := &mockMySQL{readOnly: false, gtidExecutedErr: errors.New("primary probe timeout")}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: oldGTID, hasUserSchemas: testBoolPtr(false)}
	clk := clock.NewFakeClock(time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(site0, site1, clk)
	setRecoveredTopology(tm)

	tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})

	site1.mu.Lock()
	fenced := site1.superReadOnlyCalls
	site1.mu.Unlock()
	if fenced == 0 {
		t.Fatal("expected recovery to fence the returning site when the new primary is unreachable; " +
			"got a fresh-datadir skip instead (would hand the site to auto-clone)")
	}
	if site1.startReplicaCallCount() != 0 {
		t.Fatalf("StartReplica must not run without a successful GTID comparison, got %d calls",
			site1.startReplicaCallCount())
	}
}

// TestActiveSiteLocked_WritableReaderDoesNotClearActiveSite guards the
// scenario-40 regression: a reader returns writable for a few seconds after
// its CLONE-induced restart (the fresh datadir starts before super_read_only
// is reapplied). Counting it as a second writable site dropped
// status.activeSite to "" — a spurious NoPrimary for DNS steering and every
// status consumer — while the promotable primary was healthy and unambiguous.
func TestActiveSiteLocked_WritableReaderDoesNotClearActiveSite(t *testing.T) {
	tests := []struct {
		name        string
		readerRole  state.SiteRole
		dc2State    state.SiteState
		cloningInto bool
		want        string
	}{
		{
			name:        "writable reader mid-clone leaves the primary active",
			readerRole:  state.SiteRoleReadOnly,
			dc2State:    state.StateWritable,
			cloningInto: true,
			want:        "dc1",
		},
		{
			name:       "reader writable on its own still invalidates authority",
			readerRole: state.SiteRoleReadOnly,
			dc2State:   state.StateWritable,
			want:       "",
		},
		{
			name:       "read-only reader leaves the primary active",
			readerRole: state.SiteRoleReadOnly,
			dc2State:   state.StateReadOnly,
			want:       "dc1",
		},
		{
			name:        "two writable candidates is still ambiguous",
			readerRole:  state.SiteRolePrimaryCandidate,
			dc2State:    state.StateWritable,
			cloningInto: true,
			want:        "",
		},
	}

	// Every non-idle phase, not just Cloning: the writable window opens when
	// the cloned datadir starts (Restarting) and does not close until
	// SetupReplication's first statement — SetSuperReadOnly — lands, which is
	// after the phase has already flipped to SetupRepl. The canSkipClone path
	// enters SetupRepl without passing through Cloning at all.
	activePhases := []BootstrapPhase{BootstrapPhaseCloning, BootstrapPhaseRestarting, BootstrapPhaseSetupRepl}

	for _, tt := range tests {
		for _, phase := range activePhases {
			t.Run(tt.name+"/"+string(phase), func(t *testing.T) {
				tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{})
				tm.mu.Lock()
				tm.sites[0].role = state.SiteRolePrimaryCandidate
				tm.sites[0].state = state.StateWritable
				tm.sites[1].role = tt.readerRole
				tm.sites[1].state = tt.dc2State
				if tt.cloningInto {
					tm.bootstrapRecipient = tm.sites[1].name
					tm.bootstrapPhase = phase
				}
				got := tm.activeSiteLocked()
				tm.mu.Unlock()
				if got != tt.want {
					t.Errorf("activeSiteLocked() = %q, want %q", got, tt.want)
				}
			})
		}
	}

	// Idle phases must never grant the bypass: a reader that is writable with
	// no bootstrap in flight is a real authority anomaly.
	for _, phase := range []BootstrapPhase{BootstrapPhaseNone, BootstrapPhaseDone, BootstrapPhaseFailed} {
		t.Run("idle phase still invalidates/"+string(phase), func(t *testing.T) {
			tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{})
			tm.mu.Lock()
			tm.sites[0].role = state.SiteRolePrimaryCandidate
			tm.sites[0].state = state.StateWritable
			tm.sites[1].role = state.SiteRoleReadOnly
			tm.sites[1].state = state.StateWritable
			tm.bootstrapRecipient = tm.sites[1].name
			tm.bootstrapPhase = phase
			got := tm.activeSiteLocked()
			tm.mu.Unlock()
			if got != "" {
				t.Errorf("activeSiteLocked() = %q, want %q for idle phase %q", got, "", phase)
			}
		})
	}
}
