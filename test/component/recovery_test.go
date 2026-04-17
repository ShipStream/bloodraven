package component

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// newRecoveryHarness creates a harness with replication credentials so that
// the recovery code path is enabled. dc1 is writable (primary), dc2 is
// read-only (replica).
func newRecoveryHarness(t *testing.T, dc1, dc2 *mockMySQL) *testHarness {
	t.Helper()
	h := newTestHarnessWithMySQL(t, dc1, dc2)
	// The default harness has no bootstrap config, which disables recovery.
	// Re-create with replication credentials.
	h.tm.Stop()
	cfg := controller.TopologyConfig{
		Name:              "lion",
		Sites:             defaultTwoSiteConfig(),
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		FailoverCooldown:  0,
	}
	bootstrapCfg := controller.BootstrapConfig{
		ReplUser:     "repl",
		ReplPassword: "replpass",
		UseSSL:       false,
	}
	fc := controller.NewFailoverController(h.logger)
	h.tm = controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, nil, bootstrapCfg, h.tainter, h.hub, h.dns, h.logger, h.clock)
	return h
}

func TestRecovery_OldPrimaryReturnsReadOnly_NoDivergence(t *testing.T) {
	// dc1 primary (writable), dc2 replica (read-only)
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	// Establish normal: dc1 writable, dc2 read-only.
	h.pollN(2)
	s := h.tm.Status()
	if s.Sites[0].State != "writable" || s.Sites[1].State != "read-only" {
		t.Fatalf("setup: dc1=%s dc2=%s, want writable/read-only", s.Sites[0].State, s.Sites[1].State)
	}

	// dc1 goes down.
	dc1.setError(errDown)
	h.pollN(3) // failure threshold → dc1 unreachable, dc2 promoted

	if dc2.isReadOnly() {
		t.Fatal("dc2 should have been promoted")
	}

	// Confirm promotion.
	h.pollN(2)
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Fatalf("DNS should point to dc2, got %s", h.dns.getLastIP())
	}

	// dc1 comes back as read-only (sidecar fenced it), no replication configured.
	dc1.setError(nil)
	dc1.setReadOnly(true)
	// GTID matches new primary — no divergence.
	dc1.mu.Lock()
	dc1.gtidExecuted = "uuid1:1-10"
	dc1.mu.Unlock()

	// Poll to detect recovery.
	h.pollN(2) // recovery threshold for dc1 to become read-only

	// dc1 should now have replication configured (recovery auto-ran).
	dc1.mu.Lock()
	replConfigured := dc1.replicationSourceSet && dc1.replicaStarted
	dc1.mu.Unlock()

	if !replConfigured {
		t.Error("dc1 should have been auto-recovered as replica (replication configured)")
	}
}

func TestRecovery_OldPrimaryReturnsWritable_NoDivergence(t *testing.T) {
	// dc1 primary, dc2 replica.
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2) // establish

	// dc1 goes down.
	dc1.setError(errDown)
	h.pollN(3) // failover to dc2
	h.pollN(2) // confirm

	// dc1 comes back WRITABLE (power was cut, sidecar never fenced).
	dc1.setError(nil)
	dc1.setReadOnly(false)
	dc1.mu.Lock()
	dc1.gtidExecuted = "uuid1:1-10"
	dc1.mu.Unlock()

	// dc1 recoveryThreshold polls to become writable → split brain detected → fenced.
	h.pollN(2)

	// dc1 should have been fenced (super_read_only=ON → read_only=true).
	if !dc1.isReadOnly() {
		t.Error("dc1 should have been fenced after returning writable")
	}

	// Next poll: dc1 is now read-only, recovery should detect no replication and auto-rejoin.
	h.pollN(1)

	dc1.mu.Lock()
	replConfigured := dc1.replicationSourceSet && dc1.replicaStarted
	dc1.mu.Unlock()

	if !replConfigured {
		t.Error("dc1 should have been auto-recovered as replica after fencing")
	}
}

func TestRecovery_Divergence_BlocksRecovery(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2) // establish

	dc1.setError(errDown)
	h.pollN(3) // failover
	h.pollN(2) // confirm

	// dc1 comes back read-only with DIVERGENT transactions.
	dc1.setError(nil)
	dc1.setReadOnly(true)
	dc1.mu.Lock()
	dc1.gtidExecuted = "uuid1:1-10,uuid_divergent:1-5" // 5 extra transactions
	dc1.mu.Unlock()

	h.pollN(2) // detect recovery needed

	// dc1 should NOT have replication configured — recovery should be blocked.
	dc1.mu.Lock()
	replConfigured := dc1.replicationSourceSet
	dc1.mu.Unlock()

	if replConfigured {
		t.Error("dc1 should NOT have replication configured when divergence is detected")
	}

	// Verify the topology snapshot carries divergence metadata via status callback.
	h.tm.StatusCallback = func(snap controller.TopologySnapshot) {
		if snap.RecoveryState != "RecoveryBlocked" {
			t.Errorf("expected RecoveryBlocked, got %q", snap.RecoveryState)
		}
		if snap.RecoverySite != "dc1" {
			t.Errorf("expected RecoverySite dc1, got %q", snap.RecoverySite)
		}
		if snap.DivergentTxnCount != 5 {
			t.Errorf("expected 5 divergent txns, got %d", snap.DivergentTxnCount)
		}
	}
	// Force a state transition to trigger the status callback.
	dc1.setError(errDown)
	h.pollN(3) // dc1 unreachable → transition fires callback
	dc1.setError(nil)
	dc1.setReadOnly(true)
	dc1.mu.Lock()
	dc1.gtidExecuted = "uuid1:1-10,uuid_divergent:1-5"
	dc1.mu.Unlock()
	h.pollN(1) // dc1 back → transition fires callback
}

func TestRecovery_AlreadyReplicating_Skipped(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{
		readOnly:     true,
		gtidExecuted: "uuid1:1-10",
		replicaStatus: &mysql.ReplicaStatus{
			IORunning:  true,
			SQLRunning: true,
		},
	}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2) // establish

	// Simulate a prior failover target so recovery logic is active.
	h.tm.SetLastFailoverForTest(h.clock.Now().Add(-10 * time.Minute))

	h.pollN(3) // more polls

	// dc2 is already replicating, so recovery should NOT touch it.
	dc2.mu.Lock()
	stoppedReplica := dc2.stoppedReplica
	dc2.mu.Unlock()

	if stoppedReplica {
		t.Error("dc2 should not have had StopReplica called — it's already replicating")
	}
}

func TestRecovery_NoCredentials_Skipped(t *testing.T) {
	// Use the default harness which has NO replication credentials.
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newTestHarnessWithMySQL(t, dc1, dc2)

	h.pollN(2) // establish

	// dc1 goes down, dc2 promoted.
	dc1.setError(errDown)
	h.pollN(3)
	h.pollN(2) // confirm

	// dc1 comes back read-only with no divergence.
	dc1.setError(nil)
	dc1.setReadOnly(true)
	h.pollN(2)

	// Without replication credentials, recovery should be skipped.
	dc1.mu.Lock()
	replConfigured := dc1.replicationSourceSet
	dc1.mu.Unlock()

	if replConfigured {
		t.Error("recovery should be skipped when no replication credentials are configured")
	}
}

func TestRecovery_StatusResponse_CarriesRecoveryFields(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2) // establish

	dc1.setError(errDown)
	h.pollN(3) // failover
	h.pollN(2) // confirm

	// dc1 comes back read-only with DIVERGENT transactions.
	dc1.setError(nil)
	dc1.setReadOnly(true)
	dc1.mu.Lock()
	dc1.gtidExecuted = "uuid1:1-10,uuid_divergent:1-5"
	dc1.mu.Unlock()

	h.pollN(2) // detect recovery needed

	s := h.tm.Status()

	// dc1 (index 0) should have recovery fields populated.
	if s.Sites[0].RecoveryState != "RecoveryBlocked" {
		t.Errorf("expected dc1 RecoveryState=RecoveryBlocked, got %q", s.Sites[0].RecoveryState)
	}
	if s.Sites[0].DivergentGtid == "" {
		t.Error("expected dc1 DivergentGtid to be populated")
	}
	if s.Sites[0].DivergentTransactionCount == nil || *s.Sites[0].DivergentTransactionCount != 5 {
		t.Errorf("expected dc1 DivergentTransactionCount=5, got %v", s.Sites[0].DivergentTransactionCount)
	}

	// dc2 (index 1) should have no recovery fields.
	if s.Sites[1].RecoveryState != "" {
		t.Errorf("expected dc2 RecoveryState empty, got %q", s.Sites[1].RecoveryState)
	}

	// PromotionGtidExecuted should be populated from the failover.
	if s.PromotionGtidExecuted == "" {
		t.Error("expected PromotionGtidExecuted to be populated after failover")
	}
}

func TestRecovery_DivergentTransactionsMetric(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2) // establish

	dc1.setError(errDown)
	h.pollN(3) // failover
	h.pollN(2) // confirm

	// dc1 comes back read-only with divergence.
	dc1.setError(nil)
	dc1.setReadOnly(true)
	dc1.mu.Lock()
	dc1.gtidExecuted = "uuid1:1-10,uuid_divergent:1-3"
	dc1.mu.Unlock()

	h.pollN(2) // detect recovery

	val := testutil.ToFloat64(metrics.DivergentTransactions.WithLabelValues("dc1"))
	if val != 3 {
		t.Errorf("expected DivergentTransactions metric=3 for dc1, got %v", val)
	}
}

func TestRecovery_StatusCleared_AfterAutoRecovery(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2) // establish

	dc1.setError(errDown)
	h.pollN(3) // failover
	h.pollN(2) // confirm

	// dc1 comes back with NO divergence (GTID matches).
	dc1.setError(nil)
	dc1.setReadOnly(true)
	dc1.mu.Lock()
	dc1.gtidExecuted = "uuid1:1-10"
	dc1.mu.Unlock()

	h.pollN(2)

	s := h.tm.Status()

	// No recovery fields should be set (auto-recovered).
	if s.Sites[0].RecoveryState != "" {
		t.Errorf("expected dc1 RecoveryState empty after auto-recovery, got %q", s.Sites[0].RecoveryState)
	}
	if s.Sites[0].DivergentTransactionCount != nil {
		t.Errorf("expected dc1 DivergentTransactionCount nil after auto-recovery, got %v", s.Sites[0].DivergentTransactionCount)
	}
}
