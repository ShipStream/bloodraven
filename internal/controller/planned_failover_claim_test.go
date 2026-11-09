package controller

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/state"
)

// claimRequest is a claim for dc2 with the manager's own record as the
// durable baseline.
func claimRequest(tm *TopologyManager, runID string) PlannedClaimRequest {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return PlannedClaimRequest{RunID: runID, Target: "dc2",
		DurableBaseline: newFailoverRecordRef(tm.lastFailover, tm.lastFailoverTarget)}
}

func TestClaimPlannedFailover_GrantAndIdempotence(t *testing.T) {
	f := newOpsFixture(t, "")
	d, err := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1"))
	if err != nil || d.Kind != ClaimGranted || d.Source != "dc1" || d.SourceServerUUID != opsUUIDA {
		t.Fatalf("claim = %+v, %v; want Granted{dc1, uuid A}", d, err)
	}
	v, ok := f.reg.slot.View()
	if !ok || v.RunID != "run-1" || !v.Provisional || v.Mode != "Run" {
		t.Fatalf("slot = %+v; want run-1 provisional", v)
	}
	again, err := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1"))
	if err != nil || again.Kind != ClaimGranted || again.SourceServerUUID != opsUUIDA {
		t.Fatalf("same run again = %+v, %v; want an idempotent Granted with the uuid", again, err)
	}
	other, _ := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-2"))
	if other.Kind != ClaimWait || other.WaitReason != v1alpha1.PlannedFailoverWaitClaimHeld {
		t.Fatalf("another run = %+v; want Wait{ClaimHeld}", other)
	}
	if !f.topo.ReleasePlannedFailoverClaim(f.nn, "run-1", ReleaseNotCommitted) || f.reg.slot.Held() {
		t.Fatal("a proven non-commit did not release the provisional grant")
	}
	if notes := f.reg.slot.ReleaseNotes(); len(notes) != 1 || notes[0].Why != ReleaseNotCommitted {
		t.Fatalf("release notes = %+v", notes)
	}
	f.reg.slot.toHold("run-9")
	held, _ := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-3"))
	if held.Kind != ClaimReject || held.RejectReason != v1alpha1.PlannedFailoverReasonManualRecoveryPending {
		t.Fatalf("claim during a hold = %+v; want Reject{ManualRecoveryPending}", held)
	}
	if f.topo.ReleasePlannedFailoverClaim(f.nn, "run-9", ReleaseZombie) {
		t.Fatal("a hold was released by something other than an acknowledgement")
	}
}

// Strict peer evidence at the grant (§4.1).
func TestClaimPlannedFailover_PeerEvidence(t *testing.T) {
	cases := []struct {
		name   string
		role   state.SiteRole
		mutate func(f *opsFixture)
		kind   ClaimDecisionKind
		reason v1alpha1.PlannedFailoverWaitReason
	}{
		{"unreadable dr-only site", state.SiteRoleDROnly, func(f *opsFixture) { f.third.down = true }, ClaimWait, v1alpha1.PlannedFailoverWaitPeerUnreadable},
		{"writable dr-only site", state.SiteRoleDROnly, func(f *opsFixture) { f.third.ro, f.third.sro = false, false }, ClaimWait, v1alpha1.PlannedFailoverWaitAuthorityUnsettled},
		{"unreadable reader is allowed", state.SiteRoleReadOnly, func(f *opsFixture) { f.third.down = true }, ClaimGranted, ""},
		{"writable reader", state.SiteRoleReadOnly, func(f *opsFixture) { f.third.ro, f.third.sro = false, false }, ClaimWait, v1alpha1.PlannedFailoverWaitAuthorityUnsettled},
		{"target not replicating", "", func(f *opsFixture) { f.tgt.repl.SQLRunning = false }, ClaimWait, v1alpha1.PlannedFailoverWaitTargetNotReady},
		{"target replicates elsewhere", "", func(f *opsFixture) { f.tgt.repl.SourceHost = "mysql-dc3" }, ClaimWait, v1alpha1.PlannedFailoverWaitTargetNotReady},
		{"source turned read-only", "", func(f *opsFixture) { f.src.ro = true }, ClaimWait, v1alpha1.PlannedFailoverWaitAuthorityUnsettled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, tc.role)
			tc.mutate(f)
			d, err := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1"))
			if err != nil || d.Kind != tc.kind || d.WaitReason != tc.reason {
				t.Fatalf("claim = %+v, %v; want kind %d reason %q", d, err, tc.kind, tc.reason)
			}
			if tc.kind != ClaimGranted && f.reg.slot.Held() {
				t.Fatal("a refused claim holds the slot")
			}
		})
	}
}

// TestPF_BaselineMatchesAfterNanosecondFailover: a planned failover admitted
// after an emergency failover in the same process, on a nanosecond clock,
// matches the durable (second-precision) record.
func TestPF_BaselineMatchesAfterNanosecondFailover(t *testing.T) {
	f := newOpsFixture(t, "")
	f.clk.Advance(123456789 * time.Nanosecond)
	f.tm.recordFailover(context.Background(), f.clk.Now(), "dc1", opsF)
	f.tm.mu.Lock()
	f.tm.promotedSite, f.tm.deploymentAwaitingObservation = "", false // observed by a poll
	f.tm.mu.Unlock()
	durable := newFailoverRecordRef(f.clk.Now(), "dc1") // truncated, as both durable copies are
	d, err := f.topo.ClaimPlannedFailover(context.Background(), f.nn, PlannedClaimRequest{RunID: "run-1", Target: "dc2", DurableBaseline: durable})
	if err != nil || d.Kind != ClaimGranted {
		t.Fatalf("claim = %+v, %v; want Granted", d, err)
	}
	if !d.Baseline.LastFailover.Equal(durable.LastFailover) {
		t.Fatalf("baseline %s is not at second precision (%s)", d.Baseline.LastFailover, durable.LastFailover)
	}
	stale := newFailoverRecordRef(f.clk.Now().Add(-time.Hour), "dc1")
	f.reg.slot.release("run-1", ReleaseNotCommitted)
	d, _ = f.topo.ClaimPlannedFailover(context.Background(), f.nn, PlannedClaimRequest{RunID: "run-2", Target: "dc2", DurableBaseline: stale})
	if d.Kind != ClaimWait || d.WaitReason != v1alpha1.PlannedFailoverWaitAuthorityUnsettled {
		t.Fatalf("claim over a stale durable record = %+v; want Wait{AuthorityUnsettled}", d)
	}
}

// TestPF_GateIsNonBlocking / TestMutationGate_TryAdmitTTL: a running
// follower mutation makes the claim wait at once; while the entry stands no
// new mutation is admitted; it lapses on the manager's clock.
func TestMutationGate_TryAdmitTTL(t *testing.T) {
	clk := clock.NewFakeClock(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC))
	g := &mutationGate{clock: clk}
	if !g.enter() {
		t.Fatal("setup: mutation not admitted")
	}
	published := false
	start := time.Now()
	if err := g.tryAdmit("run-1", 15*time.Second, func() { published = true }); err != errGateBusy || published {
		t.Fatalf("tryAdmit beside a running mutation = %v (published=%v); want errGateBusy", err, published)
	}
	if time.Since(start) > time.Second {
		t.Fatal("tryAdmit blocked")
	}
	g.exit()
	if g.enter() {
		t.Fatal("a mutation was admitted while the claim's entry stands")
	}
	clk.Advance(15 * time.Second)
	if !g.enter() {
		t.Fatal("the lapsed entry still refuses mutations")
	}
	g.exit()
	if err := g.tryAdmit("run-1", 15*time.Second, func() { published = true }); err != nil || !published {
		t.Fatalf("tryAdmit on an idle gate = %v (published=%v)", err, published)
	}
	if !g.enter() {
		t.Fatal("the entry was not cleared after publish")
	}
	g.exit()
}

func TestPF_GateIsNonBlocking(t *testing.T) {
	f := newOpsFixture(t, "")
	release, ok := f.tm.beginFollowerMutation()
	if !ok {
		t.Fatal("setup: follower mutation not admitted")
	}
	start := time.Now()
	d, _ := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1"))
	if d.Kind != ClaimWait || d.WaitReason != v1alpha1.PlannedFailoverWaitRecoveryInFlight || time.Since(start) > time.Second {
		t.Fatalf("claim beside a running mutation = %+v after %s; want an immediate Wait{RecoveryInFlight}", d, time.Since(start))
	}
	if _, ok := f.tm.beginFollowerMutation(); ok {
		t.Fatal("a new follower mutation was admitted while the claim waits at the gate")
	}
	release()
	// Attempts are spaced: an immediate retry waits without touching the gate.
	if d, _ := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1")); d.Kind != ClaimWait {
		t.Fatalf("retry inside the attempt interval = %+v; want Wait", d)
	}
	f.clk.Advance(plannedClaimGateInterval)
	if d, _ := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1")); d.Kind != ClaimGranted {
		t.Fatalf("retry after the mutation exited = %+v; want Granted", d)
	}
}

// TestClaimPlannedFailover_NoCallbackUnderLocks: the predecessor callback
// (which takes r.mu in production) and the probes run with neither r.mu
// nor tm.mu held.
func TestClaimPlannedFailover_NoCallbackUnderLocks(t *testing.T) {
	f := newClaimFixture(t)
	called := 0
	f.tm.mu.Lock()
	f.tm.predecessorBusy = func() bool {
		called++
		if !f.tm.mu.TryLock() {
			t.Error("predecessorBusy called with tm.mu held")
		} else {
			f.tm.mu.Unlock()
		}
		if !f.r.mu.TryLock() {
			t.Error("predecessorBusy called with r.mu held")
		} else {
			f.r.mu.Unlock()
		}
		return false
	}
	f.tm.mu.Unlock()
	topo := f.r.PlannedTopology()
	SetSimHooksForSim(topo, SimHooks{Yield: func(point string) {
		if !f.tm.mu.TryLock() {
			t.Errorf("yield %s with tm.mu held", point)
		} else {
			f.tm.mu.Unlock()
		}
	}})
	d, err := f.r.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1"))
	if err != nil || d.Kind != ClaimGranted {
		t.Fatalf("claim = %+v, %v; want Granted", d, err)
	}
	if called == 0 {
		t.Fatal("the predecessor callback was never consulted")
	}
}

// TestAutoPromotion_ClaimBetweenEligibilityAndClaimStopsIt, PF variant: an
// ordered update or an automatic promotion claimed between the planned
// failover's live probe and its publish makes the grant wait, holding
// nothing.
func TestPlannedClaim_ClaimBetweenProbeAndPublishStopsIt(t *testing.T) {
	intruders := map[string]func(t *testing.T, tm *TopologyManager){
		"ordered update": func(t *testing.T, tm *TopologyManager) {
			tm.mu.RLock()
			epoch, active := tm.authorityEpoch, tm.activeSiteLocked()
			tm.mu.RUnlock()
			if claimed, err := tm.claimOrderedUpdate(context.Background(), epoch, active); !claimed || err != nil {
				t.Fatalf("setup: update claim %v %v", claimed, err)
			}
		},
		"automatic promotion": func(t *testing.T, tm *TopologyManager) {
			if began, _ := tm.beginAutoPromotionOf("dc2"); !began {
				t.Fatal("setup: auto promotion not claimed")
			}
		},
	}
	for name, intrude := range intruders {
		t.Run(name, func(t *testing.T) {
			f := newOpsFixture(t, "")
			fired := false
			SetSimHooksForSim(f.topo, SimHooks{Yield: func(point string) {
				if point == YieldPeerProbeForSim && !fired {
					fired = true
					intrude(t, f.tm)
				}
			}})
			d, err := f.topo.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "run-1"))
			if !fired {
				t.Fatal("setup: the hook never ran")
			}
			if err != nil || d.Kind == ClaimGranted || f.reg.slot.Held() {
				t.Fatalf("claim after a %s took the sites = %+v, %v; want a Wait holding nothing", name, d, err)
			}
		})
	}
}

// TestPlannedClaim_WaitingOwnsNothing: a request that is only waiting
// (Admitting or Deferred) holds no claim, so every other claimant is
// allowed and the topology manager defers nothing for it.
func TestPlannedClaim_WaitingOwnsNothing(t *testing.T) {
	for _, phase := range []v1alpha1.PlannedFailoverPhase{v1alpha1.PlannedFailoverPhaseAdmitting, v1alpha1.PlannedFailoverPhaseDeferred} {
		t.Run(string(phase), func(t *testing.T) {
			fg := newTestFG()
			fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: phase, TargetSite: "dc2"}
			for i, col := range claimants {
				f := newClaimFixture(t)
				// Whatever the cached status says, nothing hydrates a
				// waiting run: registering the manager from it holds nothing.
				f.r.registerManagerWith(f.nn, fg, &managedTopology{tm: f.tm})
				if f.r.PlannedClaimHeld(f.nn) {
					t.Fatal("a waiting request holds the claim")
				}
				if got := col.claim(t, f); got != claimAllowed {
					t.Fatalf("%s (column %d) while a request waits: %c, want A", col.name, i, got)
				}
			}
		})
	}
}

func TestWireRegisteredManager_HydratesHoldOverRun(t *testing.T) {
	f := newOpsFixture(t, "")
	slot := &PlannedClaimSlot{}
	pf := opsRun(v1alpha1.PlannedFailoverPhaseDraining)
	WireRegisteredManager(f.tm, ManagerWiring{Slot: slot, HydrateFrom: pf, HoldFrom: &PlannedHold{RunID: "run-0", FromStatus: true}})
	if v, _ := slot.View(); v.Mode != "Hold" || v.RunID != "run-0" {
		t.Fatalf("slot = %+v; want the hold", v)
	}
	// Hydration never replaces a held claim.
	WireRegisteredManager(f.tm, ManagerWiring{Slot: slot, HydrateFrom: pf})
	if v, _ := slot.View(); v.RunID != "run-0" {
		t.Fatalf("hydration replaced a held claim: %+v", v)
	}
	// A legacy block (no runId) is never hydrated.
	legacy := &PlannedClaimSlot{}
	WireRegisteredManager(f.tm, ManagerWiring{Slot: legacy, HydrateFrom: &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining, TargetSite: "dc2"}})
	if legacy.Held() {
		t.Fatal("a legacy block was hydrated")
	}
}

// EnsureClaim hydrates an owns-sites run with needsReprove, refuses a
// marked run, and waits while an ordered update holds the sites.
func TestEnsureClaim(t *testing.T) {
	f := newOpsFixture(t, "")
	pf := opsRun(v1alpha1.PlannedFailoverPhaseWaitingForLag)
	fg := opsGroup(pf)
	marked := fg.DeepCopy()
	marked.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: TerminalMarker{RunID: "run-1",
		Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled}.String()}
	if _, err := f.topo.EnsureClaim(context.Background(), f.ref, marked); err != errClaimLost {
		t.Fatalf("EnsureClaim on a marked run = %v; want errClaimLost", err)
	}
	f.tm.mu.Lock()
	f.tm.deploymentUpdateRunning = true
	f.tm.mu.Unlock()
	if d, _ := f.topo.EnsureClaim(context.Background(), f.ref, fg); d.Kind != ClaimWait || f.reg.slot.Held() {
		t.Fatalf("EnsureClaim beside an ordered update = %+v; want Wait holding nothing", d)
	}
	f.tm.mu.Lock()
	f.tm.deploymentUpdateRunning = false
	f.tm.mu.Unlock()
	d, err := f.topo.EnsureClaim(context.Background(), f.ref, fg)
	if err != nil || d.Kind != ClaimGranted {
		t.Fatalf("EnsureClaim = %+v, %v; want Granted", d, err)
	}
	if v, _ := f.reg.slot.View(); !v.NeedsReprove {
		t.Fatal("a hydrated claim does not need a re-proof")
	}
	// No manager: the slot is still taken, so the next manager is blocked.
	g := newOpsFixture(t, "")
	g.reg.current = nil
	if d, _ := g.topo.EnsureClaim(context.Background(), g.ref, fg); d.Kind != ClaimWait || d.WaitReason != v1alpha1.PlannedFailoverWaitManagerAbsent || !g.reg.slot.Held() {
		t.Fatalf("EnsureClaim with no manager = %+v (held=%v)", d, g.reg.slot.Held())
	}
}

// TestSync_NeverTouchesClaim: a runner sync, reading the cache, neither
// sets nor clears the claim, whatever status says.
func TestSync_NeverTouchesClaim(t *testing.T) {
	r, c, fg, _ := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	mustSync(t, r)
	if registeredManager(r, nn) == nil {
		t.Fatal("setup: no manager")
	}
	r.Slot(nn).hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"})
	var live v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &live); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	live.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: v1alpha1.PlannedFailoverPhaseFailed, TargetSite: "dc2",
		Result: &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, CompletionTime: now}}
	if err := c.Status().Update(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	mustSync(t, r)
	if v, ok := r.Slot(nn).View(); !ok || v.RunID != "run-1" {
		t.Fatal("a sync released the claim")
	}
	r.Slot(nn).release("run-1", ReleaseTerminal)
	live.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-2", Phase: v1alpha1.PlannedFailoverPhaseDraining, TargetSite: "dc2", SourceSite: "dc1"}
	if err := c.Status().Update(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	mustSync(t, r)
	if r.Slot(nn).Held() {
		t.Fatal("a sync hydrated a claim from the cache")
	}
}

// TestNoStatusDerivedPlannedGuard: the status-derived planned-failover
// guard is gone. No source file sets or reads a manager flag, re-derives
// the guard from status, or pauses Dragonfly from it; the claim slot is the
// only guard. The plannedFailoverActive(fg) status predicate, used for
// informational and conservative refusals, shares the name and is allowed.
func TestNoStatusDerivedPlannedGuard(t *testing.T) {
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`\.plannedFailoverActive\b`),
		regexp.MustCompile(`\bplannedFailoverActive\s+bool\b`),
		regexp.MustCompile(`\bSetPlannedFailoverActive\b`),
		regexp.MustCompile(`\bplannedFailoverInFlight\b`),
		regexp.MustCompile(`\bdragonflyMaintenanceActive\b`),
		regexp.MustCompile(`\bisPlannedFailoverActive\b`),
	}
	var files []string
	for _, dir := range []string{".", "../../cmd/bloodraven", "../../cmd/kubectl-bloodraven", "../dst"} {
		matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue
			}
			for _, re := range forbidden {
				if re.MatchString(line) {
					t.Errorf("%s:%d uses the status-derived planned guard (%s): %s", name, i+1, re, strings.TrimSpace(line))
				}
			}
		}
	}
}
