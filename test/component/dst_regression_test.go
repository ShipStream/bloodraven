package component

// Regression tests for defects found by the deterministic simulation engine
// (internal/dst). Each test pins the minimal scenario the shrunk DST repro
// produced, so the fixes stay covered even as the DST schedule generator
// evolves.

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
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
		Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3", Role: state.SiteRolePrimaryCandidate,
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
		Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3", Role: state.SiteRolePrimaryCandidate,
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
			blocked = string(s.RecoveryState)
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

// Ultra-review finding (high, via the post-fix DST campaign): isFreshDeploy
// must require emptiness. Restart amnesia — failover during a status-write
// outage, operator restart, old primary respawning writable — reproduces the
// all-writable, no-replication-metadata signature on a POPULATED cluster; the
// fresh-deploy path would then seed by sitePriorities and clone the newer
// side from the stale one, destroying post-failover writes.
func TestFreshDeploy_NotTriggeredByRestartAmnesia(t *testing.T) {
	// Both writable, no replication metadata, but both hold data — the
	// amnesia signature. dc2 has post-failover writes dc1 lacks.
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-100"}
	dc2 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-100,uuid2:1-50"}

	logger := newQuietLogger()
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	cfg := controller.TopologyConfig{
		Name: "lion", Sites: defaultTwoSiteConfig(), PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: 0,
		SitePriorities: []string{"dc1"},
	}
	bootstrap := controller.NewBootstrapController(logger)
	bootstrapCfg := controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass"}
	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, bootstrap, bootstrapCfg, tainter, hub, dns, logger, clk)

	for i := 0; i < 4; i++ {
		tm.Poll(t.Context())
	}

	if phase := tm.BootstrapPhase(); phase != controller.BootstrapPhaseNone {
		t.Fatalf("fresh-deploy bootstrap fired on a populated amnesia cluster (phase %q) — this is the clone-the-stale-side data-loss path", phase)
	}
	// The split-brain must instead resolve by policy: dc1 wins, dc2 fenced,
	// its divergent writes preserved for the recovery report.
	if !dc2.isSuperReadOnly() {
		t.Error("policy loser dc2 was not fenced")
	}
	dc2.mu.Lock()
	cloned := dc2.cloneInstanceCalled
	dc2.mu.Unlock()
	if cloned {
		t.Error("dc2 was cloned — post-failover writes destroyed")
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
		Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3", Role: state.SiteRoleReadOnly,
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

// DST finding 9 (the CooldownViolated(restart) class): anti-flap state lived
// only in CR status. A status-write outage that spanned an operator restart
// left the replacement process with no cooldown at all, and it would promote
// again immediately. The record is now written out of band as well, and a
// restart rehydrates from whichever durable copy is newer.
//
// This pins the operator-level contract the fix rests on: a promotion writes
// the record through the out-of-band recorder even when it is the ONLY path
// available, and a fresh manager rehydrated from that record refuses to
// promote inside the cooldown.
func TestAntiFlapRecordSurvivesStatusOutage(t *testing.T) {
	// recorder stands in for the annotation store: independent of the
	// status subresource, exactly as in production.
	var recorded []controller.FailoverRecord
	rec := recorderFunc(func(_ context.Context, r controller.FailoverRecord) error {
		recorded = append(recorded, r)
		return nil
	})

	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newTestHarnessWithMySQL(t, dc1, dc2)
	h.tm.SetFailoverStateRecorder(rec)
	// Every CR /status write is rejected for the whole test, so the status
	// copy of the record never lands.
	h.tm.StatusCallback = func(controller.TopologySnapshot) {
		h.tm.MarkStatusWriteResult(errors.New("mysqlfailovergroups/status is forbidden"))
	}

	h.pollN(2) // dc1 is primary
	dc1.setError(errDown)
	h.pollN(5) // dc2 promoted

	if dc2.isReadOnly() {
		t.Fatal("setup: dc2 should have been promoted")
	}
	if len(recorded) == 0 {
		t.Fatal("promotion did not write the anti-flap record out of band; a restart would lose the cooldown")
	}
	last := recorded[len(recorded)-1]
	if last.LastFailoverTarget != "dc2" {
		t.Errorf("out-of-band record names %q, want dc2", last.LastFailoverTarget)
	}
	if last.LastFailover.IsZero() {
		t.Error("out-of-band record carries no timestamp, so it cannot bound a cooldown")
	}

	// The replacement process: a fresh manager with a real cooldown,
	// rehydrated from the out-of-band record alone (status wrote nothing).
	fresh := newTestHarnessWithMySQL(t, dc1, dc2)
	fresh.tm.SetLastFailover(last.LastFailover)
	fresh.tm.SetLastFailoverTarget(last.LastFailoverTarget)

	// dc2 now dies too, giving the fresh process a reason to promote dc1 —
	// which the cooldown must forbid.
	dc1.respawn(true) // clears the error and brings dc1 back fenced
	dc2.setError(errDown)
	fresh.pollN(10)

	if !dc1.isReadOnly() {
		t.Error("fresh operator promoted inside the anti-flap cooldown; the rehydrated out-of-band record did not bound it")
	}

	// Negative control: the same cluster state driven by a process that
	// rehydrated nothing must promote dc1. Without this, the assertion
	// above could pass because no promotion was reachable at all — a
	// debounce or candidate-selection change — rather than because the
	// rehydrated record forbade it.
	control := newTestHarnessWithMySQL(t, dc1, dc2)
	control.pollN(10)
	if dc1.isReadOnly() {
		t.Error("control operator with no rehydrated record did not promote dc1, so the cooldown assertion proves nothing")
	}
}

// recorderFunc adapts a function to controller.FailoverStateRecorder.
type recorderFunc func(context.Context, controller.FailoverRecord) error

func (f recorderFunc) RecordFailoverState(ctx context.Context, r controller.FailoverRecord) error {
	return f(ctx, r)
}

// ---------------------------------------------------------------------------
// Planned-failover durable claim (#235)
// ---------------------------------------------------------------------------
//
// Findings of the actors-on DST sweep (internal/dst TestDST_PlannedQuick,
// GeneratePlannedTrial) that were fixed are pinned below, one test per
// finding, named TestDSTPF_<what the operator must do>, with the shrunk
// trial's shape in the doc comment. No test here runs a DST seed or depends
// on one: a seed's schedule changes whenever a fault or actor generator
// does, so the shape is written out and built by hand. Build them on
// the planned-failover harness in planned_failover_claim_test.go: the real
// TopologyManagers, the production facade over a test ManagerRegistry, the
// real engine and the in-memory store with scripted faults, and mockMySQL's
// pinned sessions, replacePod and blockOn for the interleaving the DST
// reached through a yield.
//
// The DST model issues found while building the actors (a regressing pod
// replacement destroying the last copy of an acknowledged write; a replaced
// manager polling unretired beside its successor) were harness defects, not
// operator findings, and are pinned in internal/dst (TestReplacePod_* and
// TestRegistry_*).

// TestDSTPF_CrossProcessReplacedTargetRuling pins the DST-3 ruling (K195)
// across a process restart and a target pod replaced under a new
// server_uuid with regressed data:
//
//   - The process that wrote the Promoting intent dies before any promotion
//     statement. The run's own failover record is not durable (it never
//     reached RecordPlannedPromotion), the source still holds F under the
//     run's fence and no site is writable, so the re-run rolls back:
//     TargetChanged, the source writable again, nothing sent to the new pod
//     and no hold.
//   - The run confirmed and its own record is durable, only the Publishing
//     write was left (status writes denied). The restarted process does not
//     hydrate the claim for it (O-m1) and finishes it from the durable
//     record: Succeeded. It never rolls back beside a target that was
//     writable, so the source stays fenced and the replaced pod gets
//     nothing; the manager's recovery owns the replacement from here.
//   - The record is not durable and a promotable peer does not answer the
//     new process. Its re-proof finds the replaced pod read-only inside the
//     fenced set, which says nothing about what a clear let onto the pod
//     that is gone, and the ruling's evidence is incomplete (a peer that
//     does not answer may be a writer): the run holds, as the process that
//     sent the clear does in the same state
//     (TestDSTPF_ReplacedTargetAfterALostClearRollsBack). The re-proof used
//     to roll back there and unfence the source.
func TestDSTPF_CrossProcessReplacedTargetRuling(t *testing.T) {
	regressed := pfUUIDA + ":1-60"
	replaced := podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004", gtidExecuted: &regressed}
	t.Run("recordNotDurableAPeerDownHolds", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{third: state.SiteRoleDROnly})
		h.request("dc2")
		h.runToPromotingIntent()
		h.newProcess()
		h.tgt.replacePod(replaced)
		h.third.setError(errors.New("dial tcp: i/o timeout"))
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonExecuteFailed)
		if !h.holdCondition() || !h.src.isSuperReadOnly() || h.tgt.writableGrants() != 0 {
			t.Fatalf("hold %v, source fenced %v, target grants %d; want the hold, the source fenced, nothing sent to the new pod",
				h.holdCondition(), h.src.isSuperReadOnly(), h.tgt.writableGrants())
		}
	})
	t.Run("recordNotDurableRollsBack", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.request("dc2")
		h.runToPromotingIntent()
		h.newProcess() // the cross-process re-run: no promotion memo
		h.tgt.replacePod(replaced)
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonTargetChanged)
		if h.tgt.writableGrants() != 0 || h.src.isReadOnly() || h.holdCondition() {
			t.Fatalf("target grants %d, source read-only %v, hold %v", h.tgt.writableGrants(), h.src.isReadOnly(), h.holdCondition())
		}
	})
	t.Run("recordDurableFinishes", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.request("dc2")
		h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhasePromoting))
		h.store.DenyStatus(1 << 20) // the Publishing write cannot land
		for range 4 {
			h.pass()
		}
		if h.tgt.isReadOnly() || h.pf().Phase != v1alpha1.PlannedFailoverPhasePromoting {
			t.Fatalf("setup: want dc2 promoted with only the Publishing write left (dc2 read-only %v, phase %s)",
				h.tgt.isReadOnly(), h.pf().Phase)
		}
		if !controller.PlannedOwnRecordDurableForSim(h.store.Object(), h.pf(), h.clk.Now()) {
			t.Fatal("setup: the run's own record is not durable")
		}
		h.newProcess()
		h.tgt.replacePod(replaced)
		grants := h.tgt.writableGrants()
		h.store.ClearFaults()
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
		if h.tgt.writableGrants() != grants || !h.src.isReadOnly() {
			t.Fatalf("after the restart: target grants %d -> %d, source read-only %v",
				grants, h.tgt.writableGrants(), h.src.isReadOnly())
		}
	})
}

// TestDSTPF_ManagerAbsenceDeadlineRunsWhileSlotHeld pins DST-2: with this
// process's slot holding the run and no manager registered, EnsureClaim
// returned Granted, so every pass cleared and re-marked the
// manager-absent-since annotation and the §6.3 deadline never fired (the
// trial showed mark and clear alternating every pass). The run must
// keep one durable start and end SourceLeftFenced{ManagerUnavailable} after
// five minutes.
func TestDSTPF_ManagerAbsenceDeadlineRunsWhileSlotHeld(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.tgt.setGtidExecuted(pfUUIDA + ":1-50") // lags: the run holds the claim in WaitingForLag
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	h.setCurrent(nil)
	h.pass()
	start := h.store.Object().GetAnnotations()[v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation]
	if start == "" {
		t.Fatal("no manager-absent-since annotation")
	}
	for range 12 {
		h.clk.Advance(30 * time.Second)
		h.pass()
		if pf := h.pf(); pf.Result != nil {
			break
		}
		if got := h.store.Object().GetAnnotations()[v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation]; got != start {
			t.Fatalf("the absent-since start moved: %q → %q", start, got)
		}
	}
	pf := h.pf()
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, v1alpha1.PlannedFailoverReasonManagerUnavailable)
}

// TestDSTPF_ConfirmedRunReleasesBeforePublishing pins DST-4: the run
// confirmed dc2 and recorded the promotion,
// an external writer then made the old source dc1 writable, and a status
// outage kept the Publishing write from landing. The claim was held until
// Publishing committed, so the topology manager's split-brain fence of dc1
// was refused ("cross-site action deferred: planned failover in progress")
// and two sites stayed writable for as long as status writes failed. The
// claim is now released right after the record and DNS (Publishing is
// bookkeeping): the manager fences dc1 within its debounce while the
// Publishing write is still failing, and the run still ends Succeeded.
func TestDSTPF_ConfirmedRunReleasesBeforePublishing(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhasePromoting))
	h.store.DenyStatus(1 << 20) // the Publishing write cannot land
	for range 4 {
		h.pass()
	}
	if h.tgt.isReadOnly() {
		t.Fatal("setup: the target was not promoted")
	}
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Fatalf("setup: the Publishing write landed: %+v", pf)
	}
	if run, _, ok := h.held(); ok {
		t.Fatalf("the claim of %s is still held with only the Publishing write left", run)
	}
	h.src.setRO(false, false) // an external writer: two writable sites
	fenced := false
	for range 10 {
		h.tick()
		h.pass()
		if h.src.isSuperReadOnly() {
			fenced = true
			break
		}
	}
	if !fenced {
		t.Fatalf("dc1 stayed writable beside the promoted dc2 while the Publishing write stalled; dc1 statements %v",
			h.src.statements())
	}
	if h.tgt.isReadOnly() {
		t.Fatal("the split-brain fence fenced the promoted target")
	}
	h.store.ClearFaults()
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
}

// TestDSTPF_RetiredManagerHandoffReachesReplacement pins DST-1: an ordered
// update started on
// manager A hands the primary off to dc2 after A was replaced. The handoff
// was recorded only in A's memory: A's status writes are dropped (it is not
// the current manager) and its out-of-band flush was denied, so the
// replacement B never learned of it and, when dc2 then failed, promoted dc1
// inside the failover cooldown. The record is now carried into B under the
// timestamp-monotonic rule, at registration (the runner passes the retiring
// managers as Predecessors) and again once A's update workers exit (the
// runner's retire goroutine). B refuses the emergency promotion inside the
// cooldown and persists the handoff with its own next status write.
func TestDSTPF_RetiredManagerHandoffReachesReplacement(t *testing.T) {
	h := newPFHarness(t, pfOpts{updater: true, cooldown: 10 * time.Minute})
	b := h.startHandoffBlockedInFence(t)
	h.stateDenied = true // the out-of-band store is down for the whole window
	old := h.tm
	old.RetireForSim()
	h.tm = h.newManager()
	w := h.wiring(h.reg.slot)
	w.RetiringUpdateInFlight = old.OrderedUpdateClaimedForSim()
	w.RetiringBusy = func() bool { return old.ChangingSitesForSim() }
	w.Predecessors = []*controller.TopologyManager{old} // as registerManagerWith
	controller.WireRegisteredManager(h.tm, w)
	h.setCurrent(h.tm)
	b.Release()
	h.waitIdle(t) // the handoff to dc2 finishes on the retired manager
	if h.src.isPromoted() || h.tgt.isReadOnly() {
		t.Fatalf("setup: the handoff did not move the primary to dc2 (dc1 ro=%v, dc2 ro=%v)", h.src.isReadOnly(), h.tgt.isReadOnly())
	}
	// As retireManagerLocked's goroutine once the old workers have exited.
	if !controller.CarryRetiredFailoverRecord(h.tm, old) {
		t.Fatal("the retired manager's handoff record was not carried into the replacement")
	}
	h.tm.SetPredecessorUpdateInFlightForSim(false)
	for range 3 {
		h.tick()
	}
	h.tgt.setError(errors.New("dc2 down"))
	for range 15 {
		h.tick()
	}
	if !h.src.isReadOnly() {
		t.Fatalf("the replacement made dc1 writable %s after the handoff, inside the 10m cooldown; dc1 statements %v",
			15*time.Second, h.src.statements())
	}
	if got := h.store.Object().Status.LastFailoverTarget(); got != "dc2" {
		t.Fatalf("status.lastFailoverTarget = %q, want the carried handoff target dc2", got)
	}
}

// The three findings below are from a wider planned sweep (about one trial
// in 15,000). Each changed a documented outcome row; the reasoning is in the
// planned-failover documentation and on the code the test names.

// TestDSTPF_WritableTargetWithChannelLeavesSourceFenced pins
// HoldWithoutDataAtRisk(writableTargetWithChannel). The shrunk trial: the
// operator dies in Promoting right after STOP
// REPLICA, an external read_only=0 makes the target writable, and the new
// process, which has no promotion memo, classifies a writable target with
// its replication channel. It used to end ManualRecoveryRequired with the
// automation hold. The run clears the target's flags only after RESET
// REPLICA ALL and the gate's no-channel read, so the channel shows that no
// process of the run made the target writable: the run ends
// SourceLeftFenced{ForeignWritable} as the process with the memo does, sends
// nothing more to either site, and holds nothing.
func TestDSTPF_WritableTargetWithChannelLeavesSourceFenced(t *testing.T) {
	for _, stopped := range []bool{true, false} {
		name := "restart before STOP REPLICA"
		if stopped {
			name = "restart after STOP REPLICA"
		}
		t.Run(name, func(t *testing.T) {
			h := newPFHarness(t, pfOpts{})
			h.request("dc2")
			h.runToPromotingIntent()
			if stopped {
				h.tgt.failNextCalls("ResetReplicaAll", 1) // the process gets no further than STOP REPLICA
				h.pass()
				if got := h.tgt.statements(); !slices.Equal(got, []string{"StopReplica"}) {
					t.Fatalf("setup: target statements %v, want only StopReplica", got)
				}
			}
			h.newProcess()
			h.tgt.setRO(false, false) // an external read_only=0; the channel is still configured
			sent := len(h.tgt.statements())
			pf := h.runUntil(20, terminal)
			h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, v1alpha1.PlannedFailoverReasonForeignWritable)
			if h.holdCondition() {
				t.Fatal("the run set the automation hold over a target someone else made writable")
			}
			if _, _, ok := h.held(); ok {
				t.Fatal("the claim is still held")
			}
			if got := h.tgt.statements(); len(got) != sent || h.tgt.writableGrants() != 0 {
				t.Fatalf("the new process sent %v to the target", got[sent:])
			}
			if !h.src.isSuperReadOnly() {
				t.Fatal("the source was unfenced beside the writable target")
			}
			// The manager owns the writable target from here; it never makes
			// the source a second writer.
			for range 10 {
				h.tick()
				if !h.src.isReadOnly() {
					t.Fatal("the source became writable beside the writable target")
				}
			}
		})
	}
}

// TestDSTPF_ReplacedTargetAfterALostClearRollsBack pins
// HoldWithoutDataAtRisk(replacedTargetSameProcess). The shrunk trial:
// super_read_only=OFF is applied on the target
// but answers with an error, and the target pod is then replaced under a new
// server_uuid, read-only, with a data directory inside the fenced set. The
// process that sent the clear used to hold (no writable site until a human
// acted) where another process rolls back by the DST-3 ruling on the same
// evidence. It now takes the same decision: the run's record is not durable,
// the source holds the fenced set under the run's fence and no site is
// writable, so it rolls back. Without that evidence the hold stands.
func TestDSTPF_ReplacedTargetAfterALostClearRollsBack(t *testing.T) {
	const replacement = "dddddddd-0000-0000-0000-000000000004"
	regressed := pfUUIDA + ":1-60"
	// lostClear runs the promotion up to a super_read_only=OFF that applies
	// and whose reply is lost because the pod is replaced under it.
	lostClear := func(t *testing.T, o pfOpts, p podReplacement, during func(h *pfHarness)) *pfHarness {
		h := newPFHarness(t, o)
		h.request("dc2")
		h.runToPromotingIntent()
		b := h.tgt.blockOn(t, "SetSuperReadOnly", true)
		h.passBlocked(b, func() {
			h.tgt.replacePod(p)
			if during != nil {
				during(h)
			}
		})
		if got := h.tgt.statements(); !slices.Equal(got, []string{"StopReplica", "ResetReplicaAll", "SetSuperReadOnly:off"}) {
			t.Fatalf("setup: target statements %v", got)
		}
		if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
			t.Fatalf("setup: phase %s after the lost clear", pf.Phase)
		}
		return h
	}
	t.Run("the ruling's evidence", func(t *testing.T) {
		h := lostClear(t, pfOpts{}, podReplacement{serverUUID: replacement, gtidExecuted: &regressed}, nil)
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonTargetChanged)
		if h.src.isReadOnly() || !h.tgt.isReadOnly() || h.holdCondition() {
			t.Fatalf("source read-only %v, target read-only %v, hold %v; want the source writable again, the replacement read-only, no hold",
				h.src.isReadOnly(), h.tgt.isReadOnly(), h.holdCondition())
		}
		if h.tgt.writableGrants() != 1 {
			t.Fatalf("writable grants on the target: %d, want only the lost clear", h.tgt.writableGrants())
		}
	})
	t.Run("a promotable peer does not answer", func(t *testing.T) {
		h := lostClear(t, pfOpts{third: state.SiteRolePrimaryCandidate}, podReplacement{serverUUID: replacement, gtidExecuted: &regressed},
			func(h *pfHarness) { h.third.setError(errors.New("dial tcp: i/o timeout")) })
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonExecuteFailed)
		if !h.holdCondition() || !h.src.isSuperReadOnly() {
			t.Fatalf("hold %v, source fenced %v; want the hold and the source left fenced", h.holdCondition(), h.src.isSuperReadOnly())
		}
	})
	t.Run("the same data directory under a new server_uuid", func(t *testing.T) {
		// Not a replaced data directory: the server kept the transaction it
		// accepted after the clear, so it is not inside the fenced set.
		kept := pfF + "," + pfUUIDB + ":1"
		h := lostClear(t, pfOpts{}, podReplacement{serverUUID: replacement, gtidExecuted: &kept}, nil)
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, "")
		if !h.holdCondition() || !h.src.isSuperReadOnly() {
			t.Fatalf("hold %v, source fenced %v; want the hold and the source left fenced", h.holdCondition(), h.src.isSuperReadOnly())
		}
	})
}

// TestDSTPF_NoSuccessBesideAnUnreadWriter pins
// OutcomeDishonest(unreadablePeer)+SucceededBesideWritable(unreadablePeer),
// which two trials reached in two ways. The confirmation used to take every
// primary-candidate that did not answer as a warning. That is the
// documented residual only for a site that answered read-only at the gate:
//
//   - a writer the run saw: the run's confirmation sees a peer writable beside its own
//     write and decides ManualRecoveryRequired, the status write is refused,
//     and the peer then stops answering. The next pass confirmed and the run
//     ended Succeeded beside the second writer. A site last seen writable
//     now counts as writable until it answers read-only.
//   - a site never read: a new process finds the target writable with exactly the
//     fenced set and no channel (an external writer, after the first process
//     got as far as RESET REPLICA ALL) and cannot read the source, which an
//     external writer made writable too. It confirmed. A process that passed
//     no gate now needs an answer from the source and every promotable peer:
//     the run stays Promoting, and ends PromotedUnconfirmed at the promotion
//     deadline if the site never answers.
func TestDSTPF_NoSuccessBesideAnUnreadWriter(t *testing.T) {
	down := errors.New("dial tcp: i/o timeout")
	t.Run("a writer the run saw stops answering", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate})
		h.request("dc2")
		h.runToPromotingIntent()
		h.store.DenyStatus(1 << 20) // the hold the run is about to decide cannot land
		b := h.tgt.blockOn(t, "SetReadOnly", true)
		h.passBlocked(b, func() { h.third.setRO(false, false) }) // after the gate read dc3 read-only
		if h.tgt.isReadOnly() || h.holdCondition() || h.pf().Phase != v1alpha1.PlannedFailoverPhasePromoting {
			t.Fatalf("setup: target read-only %v, hold %v, phase %s", h.tgt.isReadOnly(), h.holdCondition(), h.pf().Phase)
		}
		h.third.setError(down)
		for range 8 {
			h.tick()
			h.pass()
		}
		if run, hold, ok := h.held(); !ok || hold || run != h.pf().RunID {
			t.Fatalf("slot %q hold=%v ok=%v; the run released its claim (confirmed) beside a writer it had seen", run, hold, ok)
		}
		h.store.ClearFaults()
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonForeignWritable)
		hold := h.store.Object().Status.PlannedFailoverHold
		if !h.holdCondition() || hold == nil || !slices.Contains(hold.AmbiguousSites, "dc3") {
			t.Fatalf("hold condition %v, hold %+v; want the hold naming dc3", h.holdCondition(), hold)
		}
	})
	// newProcessBesideAnUnreadSource is the second trial's state: the first
	// process got as far as RESET REPLICA ALL, external writers then made
	// the target and the source writable, and the source does not answer the
	// new process.
	newProcessBesideAnUnreadSource := func(t *testing.T) *pfHarness {
		h := newPFHarness(t, pfOpts{})
		h.request("dc2")
		h.runToPromotingIntent()
		h.tgt.failNextCalls("SetSuperReadOnly", 1)
		h.pass()
		if got := h.tgt.statements(); !slices.Equal(got, []string{"StopReplica", "ResetReplicaAll"}) {
			t.Fatalf("setup: target statements %v", got)
		}
		h.tgt.setRO(false, false)
		h.src.setRO(false, false)
		h.src.setError(down)
		h.newProcess()
		for range 10 {
			h.pass()
			h.tick()
			if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Promotion.State != v1alpha1.PlannedFailoverPromotionIntended {
				t.Fatalf("the new process moved on beside a source it cannot read: phase %s, promotion %+v, result %+v",
					pf.Phase, pf.Promotion, pf.Result)
			}
		}
		if h.tgt.writableGrants() != 0 {
			t.Fatalf("setup: the operator cleared the target's flags: %v", h.tgt.statements())
		}
		return h
	}
	t.Run("a new process cannot read the source, which then answers writable", func(t *testing.T) {
		h := newProcessBesideAnUnreadSource(t)
		h.src.setError(nil)
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonForeignWritable)
		if !h.holdCondition() {
			t.Fatal("no hold over two writable sites")
		}
	})
	t.Run("a new process cannot read the source until the promotion deadline", func(t *testing.T) {
		h := newProcessBesideAnUnreadSource(t)
		pf := h.runUntil(200, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, v1alpha1.PlannedFailoverReasonPromotionUnconfirmed)
		if h.holdCondition() {
			t.Fatal("PromotedUnconfirmed set the hold")
		}
	})
	t.Run("a peer that answered read-only at the gate stays the residual", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate})
		h.request("dc2")
		h.runToPromotingIntent()
		b := h.tgt.blockOn(t, "SetReadOnly", true)
		h.passBlocked(b, func() { h.third.setError(down) }) // after the gate read dc3 read-only
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
		if !slices.Contains(pf.Result.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers) {
			t.Fatalf("warnings %v, want UnreachablePeers", pf.Result.Warnings)
		}
	})
}

// The findings below are from the sweeps run after the model learned that a
// site can restart on an older data directory under the same server_uuid
// (internal/dst/README.md, "A site that lost its own transactions").

// TestDSTPF_RollbackHoldsOverASourceThatLostTransactions pins
// RepointDivergent. The shrunk trial: the run fences dc1; dc1 restarts on
// an older data directory under the same server_uuid and comes up writable
// (a pod that boots writable, or an external read_only=0); the run rolls
// back (the pre-write gate refuses a source that is not fenced, or the lag
// wait times out). The rollback's "source already writable" branch checked
// only the server_uuid and gave the target, which holds the transactions
// the source lost, a channel from that source: the source issues those
// GTIDs again and the target skips them without an error.
//
// The rollback now compares the source with the fenced set and with what
// the target holds. A writable source that lacks part of either has begun
// a second history: the run ends ManualRecoveryRequired{SourceUnfenced}
// with the automation hold, names the lost transactions, sends nothing to
// the source, and neither creates nor starts the target's channel. The
// hold also keeps the topology manager from joining the target to the
// source afterwards.
func TestDSTPF_RollbackHoldsOverASourceThatLostTransactions(t *testing.T) {
	short := pfUUIDA + ":1-97"
	lost := pfUUIDA + ":98-100"
	// regressSource restarts dc1 on the older directory, writable.
	regressSource := func(h *pfHarness) {
		h.src.setGtidExecuted(short)
		h.src.respawn(false)
	}
	wantHold := func(t *testing.T, h *pfHarness, pf *v1alpha1.PlannedFailoverStatus, sentBefore int) {
		t.Helper()
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonSourceUnfenced)
		if !h.holdCondition() {
			t.Fatal("no automation hold over a source that began a second history")
		}
		if !strings.Contains(pf.Message, lost) {
			t.Fatalf("message %q does not name the lost transactions %s", pf.Message, lost)
		}
		for range 10 {
			h.tick() // the hold defers the manager's convergence of dc2 onto dc1
		}
		for _, s := range h.tgt.statements()[sentBefore:] {
			if s == "ChangeReplicationSource" || s == "StartReplica" {
				t.Fatalf("target statements %v: the target was joined to a source that lacks %s", h.tgt.statements(), lost)
			}
		}
		if got := h.src.statements(); slices.Contains(got, "SetSuperReadOnly:off") || slices.Contains(got, "SetReadOnly:off") {
			t.Fatalf("source statements %v: the run unfenced a source it did not find fenced", got)
		}
	}
	t.Run("after RESET REPLICA ALL", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.request("dc2")
		h.runToPromotingIntent()
		h.tgt.failNextCalls("SetSuperReadOnly", 1) // the promotion gets as far as RESET REPLICA ALL
		h.pass()
		if got := h.tgt.statements(); !slices.Equal(got, []string{"StopReplica", "ResetReplicaAll"}) {
			t.Fatalf("setup: target statements %v", got)
		}
		regressSource(h)
		pf := h.runUntil(40, terminal)
		wantHold(t, h, pf, 2)
		if !slices.Contains(pf.Result.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
			t.Fatalf("warnings %v, want TargetReplicationNotRestored", pf.Result.Warnings)
		}
	})
	t.Run("during the lag wait", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.tgt.setGtidExecuted(pfUUIDA + ":1-50") // lags: the run waits, then rolls back
		h.request("dc2")
		h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
		regressSource(h)
		pf := h.runUntil(60, terminal)
		wantHold(t, h, pf, 0)
	})
	t.Run("a writable source that holds everything is still rolled back onto", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.request("dc2")
		h.runToPromotingIntent()
		h.tgt.failNextCalls("SetSuperReadOnly", 1)
		h.pass()
		h.src.respawn(false) // restarted writable on its own data
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonSourceUnfenced)
		if h.holdCondition() {
			t.Fatal("a hold over a source that lost nothing")
		}
		if got := h.tgt.statements(); !slices.Equal(got[2:], []string{"ChangeReplicationSource", "StartReplica"}) {
			t.Fatalf("target statements %v, want its channel restored from the source", got)
		}
	})
}

// TestDSTPF_FenceKnownOnlyByItsIntentIsHealed pins FencedSourceNeverHealed.
// The shrunk trial: the run's fence lands on dc1, its Draining record never
// commits, dc1 is cut off from the operator, the operator restarts, and the
// run ends SourceLeftFenced{SourceLost} with only the fence intent durable.
// dc2 is down too, so nothing fails over. Both sites then return: dc1 is
// healthy, complete and still behind the run's fence, and every site is
// read-only. The process that ended the run had neither the fence memo nor
// an Applied record, so it did not name dc1 for the re-assert, and nothing
// ever lifted the fence: no writable site, no hold and no warning.
//
// The run now names its source from the durable intent, bound to the
// server the intent names, as a restarted manager already did from the same
// record. The re-assert lifts the fence under its usual preconditions
// (every site reachable and read-only, the source complete, no hold), and
// only from that server: a replaced one is left for a human.
func TestDSTPF_FenceKnownOnlyByItsIntentIsHealed(t *testing.T) {
	down := errors.New("dial tcp: i/o timeout")
	// leftFenced runs the trial up to the terminal record.
	leftFenced := func(t *testing.T) *pfHarness {
		h := newPFHarness(t, pfOpts{third: state.SiteRoleDROnly})
		h.request("dc2")
		h.runUntil(20, inPhase(v1alpha1.PlannedFailoverPhaseFencing))
		h.store.LoseWrites(1) // the Draining record
		h.pass()
		if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseFencing || pf.Fence.State != v1alpha1.PlannedFailoverFenceIntended ||
			!h.src.isSuperReadOnly() {
			t.Fatalf("setup: phase %s, fence %s, dc1 fenced %v; want the fence landed and only its intent durable",
				pf.Phase, pf.Fence.State, h.src.isSuperReadOnly())
		}
		h.src.setError(down)
		h.tgt.setError(down)
		h.newProcess()
		pf := h.runUntil(60, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, v1alpha1.PlannedFailoverReasonSourceLost)
		if pf.Fence.State != v1alpha1.PlannedFailoverFenceIntended {
			t.Fatalf("setup: fence state %s at the end, want only the intent", pf.Fence.State)
		}
		if slices.Contains(pf.Result.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) || h.holdCondition() {
			t.Fatalf("warnings %v, hold %v", pf.Result.Warnings, h.holdCondition())
		}
		if !strings.Contains(pf.Message, "may or may not have sent") {
			t.Fatalf("message %q does not say the fence was never read back", pf.Message)
		}
		return h
	}
	t.Run("the source returns behind the run's fence", func(t *testing.T) {
		h := leftFenced(t)
		h.src.setError(nil)
		h.tgt.setError(nil)
		healed := false
		for range 20 {
			h.tick()
			h.pass()
			if !h.src.isReadOnly() {
				healed = true
				break
			}
		}
		if !healed {
			t.Fatalf("dc1 stayed fenced with every site read-only; dc1 statements %v", h.src.statements())
		}
		if !h.tgt.isReadOnly() {
			t.Fatal("dc2 was made writable")
		}
	})
	t.Run("another server returns in its place, fenced", func(t *testing.T) {
		h := leftFenced(t)
		h.src.replacePod(podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004"})
		h.src.setRO(true, true) // read-only behind a fence that is not the run's
		h.tgt.setError(nil)
		for range 20 {
			h.tick()
			h.pass()
			if !h.src.isReadOnly() {
				t.Fatalf("the re-assert lifted a fence from a server the run never fenced; dc1 statements %v", h.src.statements())
			}
		}
	})
}

// TestDSTPF_RegressedTargetAfterAPossibleClearRollsBack pins
// HoldWithoutDataAtRisk(regressedTarget). The shrunk trial: the operator
// dies in Promoting after RESET REPLICA ALL; the target then restarts on an
// older data directory under the same server_uuid, so it reads read-only,
// without a channel, with part of the fenced set missing and nothing
// outside it. A new process cannot prove that no clear was sent, and used
// to hold: no writable site until a human acted, with the source intact
// under the run's fence. A target on an older copy of its data is the same
// finding as a replaced pod (what a clear let onto the promoted copy is
// gone either way), so the same ruling now decides: with the run's record
// not durable, the source holding the fenced set under the run's fence and
// every promotable peer answering read-only, the run rolls back; without
// that evidence the hold stands.
func TestDSTPF_RegressedTargetAfterAPossibleClearRollsBack(t *testing.T) {
	regressed := pfUUIDA + ":1-60"
	afterReset := func(t *testing.T, o pfOpts) *pfHarness {
		h := newPFHarness(t, o)
		h.request("dc2")
		h.runToPromotingIntent()
		h.tgt.failNextCalls("SetSuperReadOnly", 1)
		h.pass()
		if got := h.tgt.statements(); !slices.Equal(got, []string{"StopReplica", "ResetReplicaAll"}) {
			t.Fatalf("setup: target statements %v", got)
		}
		h.newProcess()
		h.tgt.replacePod(podReplacement{gtidExecuted: &regressed}) // the same server_uuid, older data
		return h
	}
	t.Run("the ruling's evidence", func(t *testing.T) {
		h := afterReset(t, pfOpts{})
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonTargetChanged)
		if h.src.isReadOnly() || !h.tgt.isReadOnly() || h.holdCondition() || h.tgt.writableGrants() != 0 {
			t.Fatalf("source read-only %v, target read-only %v, hold %v, target grants %d; want the source writable again and no hold",
				h.src.isReadOnly(), h.tgt.isReadOnly(), h.holdCondition(), h.tgt.writableGrants())
		}
		// The restarted target boots at super_read_only=OFF: it goes back
		// behind it before it gets its channel.
		if got := h.tgt.statements(); !slices.Equal(got[2:], []string{"SetSuperReadOnly:on", "ChangeReplicationSource", "StartReplica"}) {
			t.Fatalf("target statements %v, want its channel restored from the source, which holds everything it holds", got)
		}
	})
	t.Run("a promotable peer does not answer", func(t *testing.T) {
		h := afterReset(t, pfOpts{third: state.SiteRoleDROnly})
		h.third.setError(errors.New("dial tcp: i/o timeout"))
		pf := h.runUntil(40, terminal)
		h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonExecuteFailed)
		if !h.holdCondition() || !h.src.isSuperReadOnly() {
			t.Fatalf("hold %v, source fenced %v; want the hold and the source left fenced", h.holdCondition(), h.src.isSuperReadOnly())
		}
	})
}
