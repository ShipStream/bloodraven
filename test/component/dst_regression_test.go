package component

// Regression tests for defects found by the deterministic simulation engine
// (internal/dst). Each test pins the minimal scenario the shrunk DST repro
// produced, so the fixes stay covered even as the DST schedule generator
// evolves.

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

func newQuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// newTestHarnessWithPrioritiesAndMySQL is newTestHarnessWithPriorities with
// caller-supplied mocks.
func newTestHarnessWithPrioritiesAndMySQL(t *testing.T, priorities []string, dc1, dc2 *mockMySQL) *testHarness {
	t.Helper()
	logger := newQuietLogger()
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	cfg := controller.TopologyConfig{
		Name: "lion", Sites: defaultTwoSiteConfig(), PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: 0, SitePriorities: priorities,
	}
	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)
	return &testHarness{tm: tm, dc1MySQL: dc1, dc2MySQL: dc2, tainter: tainter, dns: dns, hub: hub, logger: logger, clock: clk}
}

// DST finding 1 (seed 11): the returning-old-primary fence was only attempted
// on the poll that observed the state transition. If that one attempt failed
// transiently, the stable split-brain never produced another transition and
// the fence was never retried — two writable sites diverged until a human
// intervened. The fence must be retried on subsequent polls.
func TestSplitBrainFence_RetriedAfterTransientFailure(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newTestHarnessWithMySQL(t, dc1, dc2)

	h.pollN(2) // establish dc1 primary

	// dc1 dies; dc2 is promoted.
	dc1.setError(errDown)
	h.pollN(5)
	if dc2.isReadOnly() {
		t.Fatal("setup: dc2 should have been promoted")
	}

	// dc1 respawns writable (read_only=0 in server config, sidecar fence not
	// yet in effect). The first fence attempt fails transiently.
	dc1.respawn(false)
	dc1.failNextSuperRO(1)
	h.pollN(2) // recovery threshold → writable transition → fence attempt fails
	if dc1.isSuperReadOnly() {
		t.Fatal("setup: fence should have failed transiently on the transition poll")
	}

	// No further state transition occurs; the poll-driven retry must fence
	// dc1 anyway.
	h.pollN(1)
	if !dc1.isSuperReadOnly() {
		t.Error("returning old primary was not re-fenced after a transient fence failure (split brain left unresolved)")
	}
}

// DST finding 2 (seed 61): once RecoveryBlocked was recorded for a divergent
// old primary, the divergence report was never re-verified. A blocked site
// that respawned writable, took more writes, and was re-fenced kept its stale
// (smaller) DivergentGtid in status — under-reporting what a human must
// extract before discarding the site. The report must refresh, and externally
// resolved divergence must unblock recovery.
func TestRecovery_BlockedDivergenceReport_Refreshes(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2) // establish
	dc1.setError(errDown)
	h.pollN(5) // failover to dc2 + confirm

	// dc1 returns fenced with 4 divergent transactions.
	dc1.respawn(true)
	dc1.setGtidExecuted("uuid1:1-14")
	h.pollN(3)

	divergent := func() string {
		for _, s := range h.tm.Status().Sites {
			if s.Name == "dc1" {
				return s.DivergentGtid
			}
		}
		return ""
	}
	if got := divergent(); got != "uuid1:11-14" {
		t.Fatalf("setup: expected divergent gtid uuid1:11-14, got %q", got)
	}

	// dc1 diverges further (respawned writable briefly, was re-fenced). The
	// blocked report must refresh on the recovery retry cadence.
	dc1.setGtidExecuted("uuid1:1-20")
	h.clock.Advance(31 * time.Second)
	h.pollN(2)
	if got := divergent(); got != "uuid1:11-20" {
		t.Errorf("blocked divergence report did not refresh: got %q, want uuid1:11-20", got)
	}

	// A human replays the divergent transactions onto the new primary; the
	// re-check must notice containment and auto-recover instead of staying
	// blocked forever.
	dc2.setGtidExecuted("uuid1:1-25")
	h.clock.Advance(31 * time.Second)
	h.pollN(2)
	for _, s := range h.tm.Status().Sites {
		if s.Name == "dc1" && s.RecoveryState == "RecoveryBlocked" {
			t.Error("recovery still blocked after divergence was externally resolved")
		}
	}
	dc1.mu.Lock()
	rejoined := dc1.replicationSourceSet && dc1.replicaStarted
	dc1.mu.Unlock()
	if !rejoined {
		t.Error("dc1 was not auto-recovered as a replica after divergence resolution")
	}
}

// DST finding 3 (seed 769): recovery state was a single slot. With two former
// primaries needing recovery at once (3-site group, consecutive failovers),
// the second site's blocked divergence report overwrote the first — one
// site's lost transactions were silently dropped from status. Recovery state
// must be tracked per site.
func TestRecovery_TwoDivergentSites_BothReported(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10,uuid_a:1-3"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10,uuid_b:1-5"}
	dc3 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}

	logger := newQuietLogger()
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	sites := defaultTwoSiteConfig()
	sites = append(sites, controller.SiteTopologyConfig{
		Name: "dc3", Zone: "lion-dc3", LBIP: "3.3.3.3", Role: state.SiteRolePrimaryCandidate,
		TaintSelector: taintSelector("dc3"), Host: "mysql-lion-dc3.default.svc.cluster.local",
	})
	cfg := controller.TopologyConfig{
		Name: "lion", Sites: sites, PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: 0,
	}
	bootstrapCfg := controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass"}
	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2, dc3}, fc, nil, nil, bootstrapCfg, tainter, hub, dns, logger, clk)
	tm.SetLastFailoverTarget("dc3")

	poll := func(n int) {
		for i := 0; i < n; i++ {
			tm.Poll(t.Context())
		}
	}

	// dc1 and dc2 are both divergent ex-primaries; recovery processes one
	// site per poll cycle.
	poll(4)
	divergent := map[string]string{}
	for _, s := range tm.Status().Sites {
		if s.RecoveryState == "RecoveryBlocked" {
			divergent[s.Name] = s.DivergentGtid
		}
	}
	if divergent["dc1"] != "uuid_a:1-3" {
		t.Errorf("dc1 divergence report = %q, want uuid_a:1-3", divergent["dc1"])
	}
	if divergent["dc2"] != "uuid_b:1-5" {
		t.Errorf("dc2 divergence report = %q, want uuid_b:1-5 (single-slot recovery state would have dropped one report)", divergent["dc2"])
	}
}

// DST finding 4 (seed 1148): recovery was gated on failover history
// (lastFailoverTarget). A replica that respawned writable and was adopted as
// the de-facto primary — no promotion event, so no history — left the fenced
// ex-primary orphaned forever: read-only, not replicating, unreported.
// Recovery must run whenever a unique confirmed primary exists; empty sites
// stay the responsibility of bootstrap/auto-clone.
func TestRecovery_NoFailoverHistory_OrphanedExPrimaryRejoins(t *testing.T) {
	// dc2 was adopted as primary (writable, has everything); dc1 is the
	// fenced ex-primary: read-only, no replication metadata, GTID contained.
	dc1 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-20"}
	dc2 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-20,uuid2:1-5"}
	h := newRecoveryHarness(t, dc1, dc2)
	// Deliberately no SetLastFailoverTarget: no failover ever happened.

	h.pollN(4)

	dc1.mu.Lock()
	rejoined := dc1.replicationSourceSet && dc1.replicaStarted
	dc1.mu.Unlock()
	if !rejoined {
		t.Error("orphaned ex-primary was not recovered without failover history")
	}
}

// Companion guard for finding 4: an EMPTY read-only site must NOT be adopted
// by recovery (replicating from scratch depends on unpurged binlogs — that is
// what CLONE/bootstrap is for).
func TestRecovery_EmptySite_LeftToBootstrap(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, gtidExecuted: ""}
	dc2 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-20"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(4)

	dc1.mu.Lock()
	configured := dc1.replicationSourceSet
	dc1.mu.Unlock()
	if configured {
		t.Error("recovery configured replication on an empty site; that must be left to bootstrap/auto-clone")
	}
}

// Ultra-review finding (high): a persistently-skipped site (empty datadir)
// earlier in scan order must not consume the recovery cycle and starve a
// divergent site behind it out of ever being reported.
func TestRecovery_EmptySiteDoesNotStarveDivergentSite(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true, gtidExecuted: ""}                      // empty, scan-first, skipped forever
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10,uuid_b:1-5"} // divergent ex-primary
	dc3 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}           // active primary

	logger := newQuietLogger()
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	sites := defaultTwoSiteConfig()
	sites = append(sites, controller.SiteTopologyConfig{
		Name: "dc3", Zone: "lion-dc3", LBIP: "3.3.3.3", Role: state.SiteRolePrimaryCandidate,
		TaintSelector: taintSelector("dc3"), Host: "mysql-lion-dc3.default.svc.cluster.local",
	})
	cfg := controller.TopologyConfig{
		Name: "lion", Sites: sites, PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: 0,
	}
	bootstrapCfg := controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass"}
	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2, dc3}, fc, nil, nil, bootstrapCfg, tainter, hub, dns, logger, clk)
	tm.SetLastFailoverTarget("dc3")

	for i := 0; i < 4; i++ {
		tm.Poll(t.Context())
	}

	blocked := ""
	for _, s := range tm.Status().Sites {
		if s.Name == "dc2" {
			blocked = s.RecoveryState
		}
	}
	if blocked != "RecoveryBlocked" {
		t.Errorf("divergent dc2 not reported (state %q) — starved by the skipped empty site ahead of it in scan order", blocked)
	}
}

// Ultra-review finding (high): a RecoveryBlocked divergence report is live
// evidence and must survive the site going rogue-writable; only the report of
// a site that became the failover target again may be dropped.
func TestRecovery_BlockedReport_PreservedWhileRogueWritable(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newRecoveryHarness(t, dc1, dc2)

	h.pollN(2)
	dc1.setError(errDown)
	h.pollN(5) // failover to dc2

	dc1.respawn(true)
	dc1.setGtidExecuted("uuid1:1-14")
	h.pollN(3) // divergence detected and blocked

	divergent := func() string {
		for _, s := range h.tm.Status().Sites {
			if s.Name == "dc1" {
				return s.DivergentGtid
			}
		}
		return ""
	}
	if divergent() == "" {
		t.Fatal("setup: expected dc1 blocked with a divergence report")
	}

	// dc1 goes rogue-writable. The report must persist on every poll while
	// the split-brain fence brings it back — never a window where the human
	// loses the divergence data.
	dc1.respawn(false)
	for i := 0; i < 6; i++ {
		h.pollN(1)
		if divergent() == "" {
			t.Fatalf("divergence report vanished at poll %d while dc1 was rogue-writable", i)
		}
	}
	if !dc1.isSuperReadOnly() {
		t.Error("dc1 was not re-fenced after going rogue-writable")
	}
}

// Ultra-review finding (high): a rogue-writable READER must not gate
// split-brain fencing of the primary-candidates — EvalCrossSite's FenceSites
// early return reports SplitBrain=false in that state, so the retry must
// count writable candidates directly.
func TestSplitBrainFence_ReaderWritableDoesNotBlockCandidateFencing(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"} // failover target, writable
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}  // candidate replica
	dc3 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}  // reader role

	logger := newQuietLogger()
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	sites := defaultTwoSiteConfig()
	sites = append(sites, controller.SiteTopologyConfig{
		Name: "dc3", Zone: "lion-dc3", LBIP: "3.3.3.3", Role: state.SiteRoleReadOnly,
		TaintSelector: taintSelector("dc3"), Host: "mysql-lion-dc3.default.svc.cluster.local",
	})
	cfg := controller.TopologyConfig{
		Name: "lion", Sites: sites, PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: 0,
	}
	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2, dc3}, fc, nil, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)
	tm.SetLastFailoverTarget("dc1")

	poll := func(n int) {
		for i := 0; i < n; i++ {
			tm.Poll(t.Context())
		}
	}
	poll(2) // establish

	// Reader and candidate both go rogue-writable; the reader's fence fails
	// persistently. Candidate fencing must proceed anyway.
	dc3.respawn(false)
	dc3.failNextSuperRO(50)
	dc2.respawn(false)
	poll(4)

	if !dc2.isSuperReadOnly() {
		t.Error("candidate dc2 not fenced while the rogue-writable reader's fence kept failing (FenceSites early-return gating)")
	}
}

// DST finding 5 (seed 2669): the no-history, priorities-based split-brain
// resolution was transition-gated. If the loser fence failed transiently AND
// the winner promotion aborted partway (so no failover target was recorded),
// nothing ever retried — permanent dual-writable. The poll-driven fence retry
// must cover the priorities path too.
func TestSplitBrainFence_PrioritiesPath_RetriedAfterFailure(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newTestHarnessWithPrioritiesAndMySQL(t, []string{"dc1"}, dc1, dc2)

	h.pollN(2) // establish dc1 primary, dc2 replica

	// dc2 goes rogue-writable. The loser fence will fail twice (transition
	// attempt + Execute's best-effort fence), and the winner promotion
	// aborts at STOP REPLICA — so no failover target is recorded.
	dc2.respawn(false)
	dc2.failNextSuperRO(2)
	dc1.failNextStopReplica(1)
	h.pollN(2) // dc2 observed writable → transition → resolution attempt fails
	if dc2.isSuperReadOnly() {
		t.Fatal("setup: loser fence should have failed transiently")
	}

	// No further transition occurs; the poll-driven retry must fence dc2.
	h.pollN(1)
	if !dc2.isSuperReadOnly() {
		t.Error("split-brain loser not re-fenced after transient failures on the priorities path")
	}
}
