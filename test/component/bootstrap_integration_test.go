package component

import (
	"context"
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

// TestFreshDeploy_AllReadOnlyTriggersBootstrap: the sidecar fences MySQL on
// startup until the operator names an active site, so a fresh deploy can
// show every site read-only instead of writable. The operator must still
// bootstrap it, lifting the seed's fence itself, rather than wait on the
// all-read-only NoPrimary guard forever.
func TestFreshDeploy_AllReadOnlyTriggersBootstrap(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	h := newBootstrapHarness(t, dc1, dc2)

	// More polls than the transition needs: the all-read-only state is
	// stable, so a bootstrap that only ran on the transition poll and
	// missed it would never run.
	h.pollN(6)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)

	dc1.mu.Lock()
	seedReadOnly, seedSuper, seedCloned := dc1.readOnly, dc1.superReadOnly, dc1.cloneInstanceCalled
	dc1.mu.Unlock()
	if seedReadOnly || seedSuper {
		t.Errorf("seed dc1 not made writable: read_only=%v super_read_only=%v", seedReadOnly, seedSuper)
	}
	if seedCloned {
		t.Error("CloneInstance must not run on the seed")
	}
	dc2.mu.Lock()
	cloned, replStarted := dc2.cloneInstanceCalled, dc2.replicaStarted
	dc2.mu.Unlock()
	if !cloned || !replStarted {
		t.Errorf("dc2 not bootstrapped: cloned=%v replicaStarted=%v", cloned, replStarted)
	}
}

// TestFreshDeploy_AllReadOnlyPopulatedNoBootstrap: every site read-only with
// no replication metadata but holding data is restart amnesia, not a fresh
// deploy. It must stay NoPrimary for a human, never seed and clone.
func TestFreshDeploy_AllReadOnlyPopulatedNoBootstrap(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: "uuid1:1-100"}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: "uuid1:1-100,uuid2:1-50"}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(6)
	time.Sleep(50 * time.Millisecond)

	if phase := h.tm.BootstrapPhase(); phase != controller.BootstrapPhaseNone {
		t.Fatalf("fresh-deploy bootstrap fired on a populated all-read-only group (phase %q)", phase)
	}
	for name, m := range map[string]*mockMySQL{"dc1": dc1, "dc2": dc2} {
		m.mu.Lock()
		cloned, writable := m.cloneInstanceCalled, !m.readOnly
		m.mu.Unlock()
		if cloned || writable {
			t.Errorf("%s touched: cloned=%v writable=%v", name, cloned, writable)
		}
	}
}

// seedFenced reports whether the fresh-deploy seed is read-only.
func seedFenced(m *mockMySQL) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly || m.superReadOnly
}

// expirePendingPromotion moves the fake clock past the window in which the
// bootstrap's pending promotion reports the seed as active. While it is
// pending, checkPrimaryReassert defers to it, as it does after a failover.
func expirePendingPromotion(h *testHarness) {
	h.clock.Advance(31 * time.Second)
}

// TestFreshDeploy_SeedPublishedBeforeObserved: the bootstrap reports the
// seed as the active site as soon as it lifts the seed's fence. A seed
// sidecar whose startup safety net is still pending asks /active-site and
// fences on "", so waiting for RecoveryThreshold polls to observe the seed
// writable is the window the sidecar fences in.
func TestFreshDeploy_SeedPublishedBeforeObserved(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(1)
	if got := h.tm.Status().ActiveSite; got != "dc1" {
		t.Fatalf("active site right after the bootstrap made the seed writable = %q, want dc1", got)
	}
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
}

// TestFreshDeploy_SeedFencedDuringBootstrapIsRestored: the seed's sidecar
// can fence it after the operator lifted the fence, before the operator
// reports the seed as active. The bootstrap must not end with every site
// read-only and no failover history to resolve it.
func TestFreshDeploy_SeedFencedDuringBootstrapIsRestored(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2.onClone = func() { _ = dc1.SetSuperReadOnly(context.Background(), true) }
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(1)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
	h.pollN(3)
	expirePendingPromotion(h)
	h.pollN(2)

	if seedFenced(dc1) {
		t.Error("seed dc1 left fenced after the bootstrap")
	}
}

// TestFreshDeploy_SeedFencedAfterBootstrapIsRestored: a fence that lands
// after the bootstrap completed, before any poll observed the seed
// writable. The recipient now has replication metadata, so the fresh-deploy
// check no longer applies and lastFailoverTarget is empty; the retained
// seed authority is what restores the seed.
func TestFreshDeploy_SeedFencedAfterBootstrapIsRestored(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(1)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
	_ = dc1.SetSuperReadOnly(context.Background(), true)

	h.pollN(3)
	if !seedFenced(dc1) {
		t.Fatal("seed restored while the bootstrap's pending promotion was still unexpired; the test no longer exercises the retained authority")
	}
	expirePendingPromotion(h)
	h.pollN(2)
	if seedFenced(dc1) {
		t.Fatal("seed dc1 left fenced: retained fresh-deploy seed authority did not restore it")
	}
	h.pollN(2)
	if got := h.tm.Status().ActiveSite; got != "dc1" {
		t.Errorf("active site after the restore = %q, want dc1", got)
	}
}

// TestFreshDeploy_SeedFencedAfterConfirmationIsRestored: the authority
// outlives the poll that confirmed the seed writable, so a seed fenced
// later (the same stale-sidecar fence checkPrimaryReassert heals after a
// failover) is restored too.
func TestFreshDeploy_SeedFencedAfterConfirmationIsRestored(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(1)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
	h.pollN(3)
	if got := h.tm.Status().ActiveSite; got != "dc1" {
		t.Fatalf("active site after the bootstrap = %q, want dc1", got)
	}

	_ = dc1.SetSuperReadOnly(context.Background(), true)
	h.pollN(2)
	if seedFenced(dc1) {
		t.Fatal("seed dc1 left fenced after polls observed the fence")
	}
}

// TestFreshDeploy_SeedRestoreRetriesTransientFailure: a restore whose SET
// fails leaves the seed fenced for that poll only. The authority is kept
// and the next attempt (rate-limited by failoverCooldown, as every
// re-assert is) restores it.
func TestFreshDeploy_SeedRestoreRetriesTransientFailure(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(1)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
	h.pollN(3)

	_ = dc1.SetSuperReadOnly(context.Background(), true)
	dc1.mu.Lock()
	dc1.superROFailN = 1
	dc1.mu.Unlock()
	h.pollN(2)
	if !seedFenced(dc1) {
		t.Fatal("seed restored despite the injected SET failure; the test no longer exercises the retry")
	}

	h.clock.Advance(5*time.Minute + time.Second)
	h.pollN(1)
	if seedFenced(dc1) {
		t.Fatal("seed dc1 left fenced after the cooldown: the restore was not retried")
	}
}

// TestFreshDeploy_SeedAuthoritySupersededByNewPrimary: once a different
// site is the unique writable primary, the seed no longer names the
// primary, and the new site was only observed, not made primary by the
// operator. If that site is then fenced, the operator restores neither: a
// fence without failover history waits on the NoPrimary guard for a human.
func TestFreshDeploy_SeedAuthoritySupersededByNewPrimary(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(1)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
	h.pollN(3)

	// A human moves the primary to dc2 outside the operator.
	_ = dc1.SetSuperReadOnly(context.Background(), true)
	dc2.mu.Lock()
	dc2.readOnly, dc2.superReadOnly = false, false
	dc2.mu.Unlock()
	h.pollN(3)
	if got := h.tm.Status().ActiveSite; got != "dc2" {
		t.Fatalf("active site = %q, want dc2", got)
	}

	_ = dc2.SetSuperReadOnly(context.Background(), true)
	expirePendingPromotion(h)
	h.pollN(3)
	if !seedFenced(dc1) {
		t.Error("superseded seed dc1 was made writable again")
	}
	if !seedFenced(dc2) {
		t.Error("observed primary dc2 lifted from a fence without failover history")
	}
}

// TestPrimaryRestart_BootReadOnlyIsRestored: mysqld boots read_only=ON, so
// a primary whose MySQL restarts comes back read-only (super_read_only
// OFF). In a group that has never failed over, the operator restores the
// last active site from that boot state on the next poll instead of
// leaving NoPrimary for a human.
func TestPrimaryRestart_BootReadOnlyIsRestored(t *testing.T) {
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: gtid,
		replicaStatus: &mysql.ReplicaStatus{SourceHost: "mysql-lion-dc1.default.svc.cluster.local", IORunning: true, SQLRunning: true}}
	h := newBootstrapHarness(t, dc1, dc2)

	h.pollN(3)
	if got := h.tm.Status().ActiveSite; got != "dc1" {
		t.Fatalf("active site = %q, want dc1", got)
	}

	// dc1's MySQL restarts: one failed probe, then it answers read-only.
	dc1.mu.Lock()
	dc1.err = errDown
	dc1.mu.Unlock()
	h.pollN(1)
	dc1.mu.Lock()
	dc1.err, dc1.readOnly, dc1.superReadOnly = nil, true, false
	dc1.mu.Unlock()
	h.pollN(2)

	if seedFenced(dc1) {
		t.Fatal("primary left read-only after a MySQL restart")
	}
	if h.tm.BootstrapPhase() != controller.BootstrapPhaseNone {
		t.Errorf("bootstrap phase = %q; a populated group must not be bootstrapped", h.tm.BootstrapPhase())
	}
}

// TestFreshDeploy_InitFromBackupSeedsThenClonesAfterRestore: a new group
// with spec.initFromBackup boots every site fenced, and the restore Job can
// only load into a writable seed. While the restore is in flight the
// operator must seed (lift the seed's fence and publish it active) without
// cloning, since a clone taken before the load would carry none of it; once
// the restore succeeds the empty site is cloned from the loaded seed.
func TestFreshDeploy_InitFromBackupSeedsThenClonesAfterRestore(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
	dc2 := &mockMySQL{readOnly: true, superReadOnly: true}
	h := newBootstrapHarness(t, dc1, dc2)
	h.tm.SetAutoBootstrapSuppressed(true)

	h.pollN(6)
	if seedFenced(dc1) {
		t.Fatal("seed dc1 still fenced while the initFromBackup restore waits for a writable target")
	}
	if got := h.tm.Status().ActiveSite; got != "dc1" {
		t.Fatalf("active site = %q, want the seed dc1 published for the sidecar's safety net", got)
	}
	if !seedFenced(dc2) {
		t.Error("non-seed dc2 must stay fenced")
	}

	// The restore Job loads the dump into the seed. Nothing is cloned
	// while the restore is in flight.
	const loaded = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10"
	dc1.setGtidExecuted(loaded)
	dc2.onClone = func() { dc2.setGtidExecuted(loaded) }
	h.pollN(6)
	time.Sleep(50 * time.Millisecond)
	dc2.mu.Lock()
	clonedEarly := dc2.cloneInstanceCalled
	dc2.mu.Unlock()
	if clonedEarly || h.tm.BootstrapPhase() != controller.BootstrapPhaseNone {
		t.Fatalf("clone started while the restore was in flight (phase %q)", h.tm.BootstrapPhase())
	}
	if seedFenced(dc1) {
		t.Fatal("seed dc1 fenced during the restore")
	}
	// The seed is writable and active, but not the application's database
	// until the restore succeeds: the group hostname must not point at it.
	if n := h.dns.getCalls(); n != 0 {
		t.Fatalf("DNS written %d times (last %q) while the restore was in flight", n, h.dns.getLastIP())
	}

	h.tm.SetAutoBootstrapSuppressed(false)
	h.pollN(2)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
	if ip := h.dns.getLastIP(); ip != "1.1.1.1" {
		t.Fatalf("DNS = %q after the restore succeeded, want the seed dc1 (1.1.1.1)", ip)
	}
	dc2.mu.Lock()
	cloned, replStarted, host := dc2.cloneInstanceCalled, dc2.replicaStarted, dc2.changeReplicationOpts.Host
	dc2.mu.Unlock()
	if !cloned || !replStarted || host != "mysql-lion-dc1.default.svc.cluster.local" {
		t.Fatalf("dc2 not cloned from the loaded seed: cloned=%v replicaStarted=%v source=%q", cloned, replStarted, host)
	}
	dc1.mu.Lock()
	seedCloned := dc1.cloneInstanceCalled
	dc1.mu.Unlock()
	if seedCloned || seedFenced(dc1) {
		t.Fatalf("seed dc1 touched: cloned=%v fenced=%v", seedCloned, seedFenced(dc1))
	}
}

// TestFreshDeploy_InitFromBackupSeedRequiresFreshDeploy: the seed-only path
// keeps every fresh-deploy guard. A populated all-read-only group (restart
// amnesia) or one with replication history is not seeded.
func TestFreshDeploy_InitFromBackupSeedRequiresFreshDeploy(t *testing.T) {
	cases := []struct {
		name     string
		dc1, dc2 *mockMySQL
	}{
		{
			name: "populated",
			dc1:  &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: "uuid1:1-100"},
			dc2:  &mockMySQL{readOnly: true, superReadOnly: true},
		},
		{
			name: "replication history",
			dc1:  &mockMySQL{readOnly: true, superReadOnly: true},
			dc2:  &mockMySQL{readOnly: true, superReadOnly: true, replicaStatus: &mysql.ReplicaStatus{}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBootstrapHarness(t, tc.dc1, tc.dc2)
			h.tm.SetAutoBootstrapSuppressed(true)
			h.pollN(6)
			if !seedFenced(tc.dc1) {
				t.Error("dc1 unfenced")
			}
			if got := h.tm.Status().ActiveSite; got != "" {
				t.Errorf("active site = %q, want none", got)
			}
		})
	}
}

// TestInitFromBackup_OperatorRestartAfterPartialLoad: the operator seeds
// dc1 and the restore Job loads part of the dump into it. The operator then
// restarts, and while it is away dc1's sidecar restarts and fences it
// (super_read_only=ON). The new operator rehydrates the seed from the
// persisted status (runner: initRestoreSeedSite) and must lift that fence:
// dc1 already holds loaded schemas, so the fresh-deploy seeding no longer
// applies, and dc2 holds first-boot transactions under its own server_uuid
// that dc1 lacks, which the ordinary re-assert would refuse. Nothing is
// cloned and DNS stays unpublished until the restore succeeds. Without the
// rehydrated seed the group stays wedged with no writable site.
func TestInitFromBackup_OperatorRestartAfterPartialLoad(t *testing.T) {
	const loaded = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-40"
	const dc2Setup = "dddddddd-dddd-dddd-dddd-dddddddddddd:1-3"
	for _, rehydrated := range []bool{true, false} {
		name := "seed rehydrated from status"
		if !rehydrated {
			name = "no seed authority (wedged)"
		}
		t.Run(name, func(t *testing.T) {
			noSchemas := false
			dc1 := &mockMySQL{readOnly: true, superReadOnly: true}
			dc2 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: dc2Setup, hasUserSchemas: &noSchemas}

			first := newBootstrapHarness(t, dc1, dc2)
			first.tm.SetAutoBootstrapSuppressed(true)
			first.pollN(6)
			if seedFenced(dc1) || first.tm.Status().ActiveSite != "dc1" {
				t.Fatal("first operator did not seed dc1")
			}
			// Part of the load lands, then the seed's sidecar restarts
			// while the operator is away and fences it.
			dc1.setGtidExecuted(loaded)
			dc1.mu.Lock()
			dc1.readOnly, dc1.superReadOnly = true, true
			dc1.mu.Unlock()

			second := newBootstrapHarness(t, dc1, dc2)
			second.tm.SetAutoBootstrapSuppressed(true)
			if rehydrated {
				second.tm.SetInitRestoreSeed("dc1")
			}
			second.pollN(8)

			if seedFenced(dc1) != !rehydrated {
				t.Fatalf("seed dc1 fenced = %v after the restart, want %v", seedFenced(dc1), !rehydrated)
			}
			if !seedFenced(dc2) {
				t.Fatal("dc2 made writable")
			}
			dc2.mu.Lock()
			cloned := dc2.cloneInstanceCalled
			dc2.mu.Unlock()
			if cloned || second.tm.BootstrapPhase() != controller.BootstrapPhaseNone {
				t.Fatalf("partial load cloned to dc2 (phase %q)", second.tm.BootstrapPhase())
			}
			if n := second.dns.getCalls(); n != 0 {
				t.Fatalf("DNS written %d times during the unfinished restore", n)
			}
			if !rehydrated {
				return
			}
			if got := second.tm.Status().ActiveSite; got != "dc1" {
				t.Fatalf("active site = %q, want dc1 so the restore Job can resume", got)
			}

			// The restore succeeds: dc2 is cloned from the seed and DNS
			// published for it.
			dc2.onClone = func() { dc2.setGtidExecuted(loaded) }
			second.tm.SetAutoBootstrapSuppressed(false)
			second.pollN(2)
			waitForPhase(t, second.tm, controller.BootstrapPhaseDone, 2*time.Second)
			dc2.mu.Lock()
			cloned, host := dc2.cloneInstanceCalled, dc2.changeReplicationOpts.Host
			dc2.mu.Unlock()
			if !cloned || host != "mysql-lion-dc1.default.svc.cluster.local" {
				t.Fatalf("dc2 not cloned from the seed: cloned=%v source=%q", cloned, host)
			}
			if ip := second.dns.getLastIP(); ip != "1.1.1.1" {
				t.Fatalf("DNS = %q after the restore, want 1.1.1.1", ip)
			}
		})
	}
}
