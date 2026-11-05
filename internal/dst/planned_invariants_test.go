package dst

import (
	"context"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/state"
)

// The planned invariants are judged on synthetic feeds: a real Cluster and
// store wired to a monitor, a run block written through the store, and
// statements issued through the simulated checkers with an actor tag. Each
// invariant gets at least one firing and one quiet case, so a check that
// can never fire (or always fires) is caught before the engine exists.

type monFixture struct {
	t     *testing.T
	c     *Cluster
	store *simPlannedStore
	mon   *plannedMonitor
	rev   int64
}

func newMonFixture(t *testing.T, roles ...state.SiteRole) *monFixture {
	t.Helper()
	specs := []SiteSpec{
		{Name: "alpha", Host: "alpha.mysql.sim", LBIP: "10.0.0.1", UUID: "00000000-0000-0000-0000-000000000001"},
		{Name: "beta", Host: "beta.mysql.sim", LBIP: "10.0.0.2", UUID: "00000000-0000-0000-0000-000000000002"},
	}
	names := []string{"alpha", "beta"}
	if len(roles) == 3 {
		specs = append(specs, SiteSpec{Name: "gamma", Host: "gamma.mysql.sim", LBIP: "10.0.0.3", UUID: "00000000-0000-0000-0000-000000000003"})
		names = append(names, "gamma")
	}
	if len(roles) == 0 {
		roles = []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate}
	}
	c := NewCluster(specs, 100)
	store, _ := newTestStore(t)
	mon := newPlannedMonitor(c, names, roles)
	mon.cur = store.Object()
	store.onCommit = mon.onCommit
	c.setPlannedHooks(plannedHooks{onMutation: mon.onMutation, onEvent: mon.onEvent})
	return &monFixture{t: t, c: c, store: store, mon: mon}
}

// write commits the run block (phase, plus edit) through the store.
func (f *monFixture) write(phase v1alpha1.PlannedFailoverPhase, edit func(*v1alpha1.PlannedFailoverStatus),
	mutate func(*v1alpha1.MysqlFailoverGroupStatus)) {
	f.t.Helper()
	f.rev++
	pf := v1alpha1.PlannedFailoverStatus{RunID: "r1", Revision: f.rev, Phase: phase, Target: "beta", Source: "alpha",
		Fence: &v1alpha1.PlannedFailoverFenceRecord{State: "Intended", SourceServerUUID: "00000000-0000-0000-0000-000000000001"}}
	if cur := f.store.Object().Status.PlannedFailover; cur != nil {
		pf = *cur.DeepCopy()
		pf.Revision = f.rev
		pf.Phase = phase
	}
	if edit != nil {
		edit(&pf)
	}
	base, _ := f.store.Load(context.Background(), f.store.nn)
	if out, err := f.store.Write(context.Background(), base, pf, mutate); out != controller.WriteCommitted {
		f.t.Fatalf("write %s: %s %v", phase, out, err)
	}
}

func (f *monFixture) terminal(outcome v1alpha1.PlannedFailoverOutcome) {
	f.t.Helper()
	f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: outcome}
	}, nil)
}

func (f *monFixture) fenceSet() string { return truthNamed(f.c.Truth(), "alpha").Executed.String() }

func (f *monFixture) pf(site string) *simChecker { return f.c.NewChecker(site).(*simChecker) }

func (f *monFixture) names() []string {
	var out []string
	for _, v := range f.mon.violations {
		out = append(out, v.Invariant)
	}
	return out
}

func (f *monFixture) want(invariants ...string) {
	f.t.Helper()
	got := strings.Join(f.names(), ",")
	if got != strings.Join(invariants, ",") {
		f.t.Fatalf("violations %v, want %v", f.mon.violations, invariants)
	}
}

func setHold(st *v1alpha1.MysqlFailoverGroupStatus) {
	apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionAutomaticFailoverHeld, Status: metav1.ConditionTrue, Reason: "ManualRecoveryRequired",
		Message: "planned failover run r1 ended ManualRecoveryRequired (ForeignWritable); writable or ambiguous sites: alpha, beta",
	})
}

func TestPlannedInvariant_FenceAuthority(t *testing.T) {
	ctx := pfCtx()
	t.Run("uniqueWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.want()
	})
	t.Run("besideWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		f.c.RogueWritable("beta")
		f.mon.onYield(YieldPeerProbe)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.want("PlannedFenceWithoutAuthority")
	})
	t.Run("drOnlyFenceByTheManagerIsAllowed", func(t *testing.T) {
		f := newMonFixture(t, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleDROnly)
		f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, nil)
		_ = f.pf("gamma").SetSuperReadOnly(context.Background(), true)
		f.want()
	})
	t.Run("alreadyFenced", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		f.c.RogueFence("alpha")
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.want("PlannedFenceWithoutAuthority")
	})
}

func TestPlannedInvariant_UnfenceAttribution(t *testing.T) {
	ctx := pfCtx()
	t.Run("sameProcessMemo", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, false)
		f.want()
	})
	t.Run("crossProcessIntentAndUUID", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.mon.noteRestart()
		_ = f.pf("alpha").SetSuperReadOnly(ctx, false)
		f.want()
	})
	t.Run("crossProcessUUIDMismatch", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Fence.SourceServerUUID = "ffffffff-0000-0000-0000-000000000001"
		}, nil)
		f.c.RogueFence("alpha") // a fence that is not the run's
		f.mon.noteRestart()
		_ = f.pf("alpha").SetSuperReadOnly(ctx, false)
		f.want("UnfenceUnattributed")
	})
	t.Run("besideWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.c.RogueWritable("beta")
		f.mon.onYield(YieldPeerProbe) // the rollback's probe saw beta writable
		_ = f.pf("alpha").SetSuperReadOnly(ctx, false)
		f.want("UnfenceBesideWritable")
	})
	t.Run("rogueAfterTheProbeIsTheResidual", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.mon.onYield(YieldPeerProbe)
		f.c.RogueWritable("beta") // lands between the probe and the unfence
		_ = f.pf("alpha").SetSuperReadOnly(ctx, false)
		f.want()
		if f.mon.stats.RogueRaceResidual != 1 {
			t.Fatalf("RogueRaceResidual %d, want 1", f.mon.stats.RogueRaceResidual)
		}
	})
}

func TestPlannedInvariant_RepeatedPromotion(t *testing.T) {
	ctx := pfCtx()
	t.Run("onceIsFine", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
		f.want()
	})
	t.Run("stopAfterWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		b := f.pf("beta")
		_ = b.SetReadOnly(ctx, false)
		_ = b.StopReplica(ctx)
		f.want("RepeatedPromotion")
	})
	t.Run("killAfterWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		b := f.pf("beta")
		_ = b.SetReadOnly(ctx, false)
		_, _ = b.KillAppConnections(ctx)
		f.want("RepeatedPromotion")
	})
	t.Run("secondReset", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.want("RepeatedPromotion")
	})
}

func TestPlannedInvariant_MarkedRunResumed(t *testing.T) {
	f := newMonFixture(t)
	f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, nil, nil)
	base, _ := f.store.Load(context.Background(), f.store.nn)
	_, _ = f.store.WriteTerminalMarker(context.Background(), base, controller.TerminalMarker{
		RunID: "r1", Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled})
	_, _ = f.pf("alpha").KillAppConnections(pfCtx())
	f.want("MarkedRunResumed")
}

func TestPlannedInvariant_ForeignMutationAndHold(t *testing.T) {
	tmCtx := context.Background()
	t.Run("tmRepointUnderClaim", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, nil)
		_ = f.pf("beta").StopReplica(tmCtx)
		f.want("ForeignMutationUnderClaim")
	})
	t.Run("updateUnderClaim", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(WithActor(tmCtx, ActorUpdate), true)
		f.want("ForeignMutationUnderClaim")
	})
	t.Run("readerFenceAllowed", func(t *testing.T) {
		f := newMonFixture(t, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly)
		f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, nil)
		_ = f.pf("gamma").SetSuperReadOnly(tmCtx, true)
		f.want()
	})
	t.Run("pendingOwnsNothing", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseAdmitting, nil, nil)
		_ = f.pf("beta").StopReplica(tmCtx)
		f.want()
	})
	t.Run("holdNotHonoured", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
		_ = f.pf("beta").SetSuperReadOnly(tmCtx, true)
		names := f.names()
		if len(names) == 0 || names[len(names)-1] != "HoldNotHonoured" {
			t.Fatalf("violations %v, want HoldNotHonoured last", f.mon.violations)
		}
	})
}

func TestPlannedInvariant_PreWriteGate(t *testing.T) {
	ctx := pfCtx()
	t.Run("sourceStillWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.mon.onYield(YieldGateProbe)
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		f.want("PlannedPromoteBesideWritable")
	})
	t.Run("sourceFenced", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.c.RogueFence("alpha")
		f.mon.onYield(YieldGateProbe)
		f.c.RogueWritable("alpha") // after the probe: the R2 residual, a statistic
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		f.want()
		if f.mon.stats.RogueRaceResidual != 1 {
			t.Fatalf("RogueRaceResidual %d, want 1", f.mon.stats.RogueRaceResidual)
		}
	})
}

func TestPlannedInvariant_OutcomeHonesty(t *testing.T) {
	ctx := pfCtx()
	t.Run("notStartedAfterAMutation", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.terminal(v1alpha1.PlannedFailoverOutcomeNotStarted)
		f.want("OutcomeDishonest")
	})
	t.Run("rolledBackHonest", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.want()
	})
	t.Run("rolledBackWithSourceFenced", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.want("OutcomeDishonest")
	})
	t.Run("sourceLeftFencedButWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		f.want("OutcomeDishonest")
	})
	t.Run("unconfirmedWithoutAnAttempt", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed)
		f.want("OutcomeDishonest")
	})
	t.Run("manualRecoveryWithoutAStatement", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired)
		f.want("OutcomeDishonest")
	})
	t.Run("succeededHonest", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		a, b := f.pf("alpha"), f.pf("beta")
		_ = a.SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		_ = b.SetReadOnly(ctx, false)
		f.mon.onYield(YieldConfirmProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		f.want()
	})
	t.Run("succeededBesideWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		_ = f.pf("beta").SetReadOnly(ctx, false)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		f.want("OutcomeDishonest", "SucceededBesideWritable")
	})
}

func TestPlannedInvariant_HoldWithoutDataAtRisk(t *testing.T) {
	hold := func(f *monFixture) {
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Fence.GtidSet = &fence
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
	}
	t.Run("nothingAtRisk", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		_ = f.pf("beta").SetReadOnly(pfCtx(), false) // a promotion statement, so the outcome is honest
		f.c.RogueFence("alpha")
		hold(f)
		f.want("HoldWithoutDataAtRisk")
	})
	t.Run("twoWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		_ = f.pf("beta").SetReadOnly(pfCtx(), false)
		hold(f)
		f.want()
	})
}

func TestPlannedInvariant_WatermarkRegressed(t *testing.T) {
	f := newMonFixture(t)
	fence := f.fenceSet()
	confirmed := metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 5, 0, time.UTC))
	f.write(v1alpha1.PlannedFailoverPhasePublishing, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.Fence.GtidSet = &fence
		pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: "Confirmed", ConfirmedAt: &confirmed}
	}, func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.PromotionGtidExecuted = fence
		st.LastFailover = &confirmed
	})
	f.want()
	// A runner snapshot with the same lastFailover and an older watermark
	// cannot regress it: the merge moves only the target on equal times
	// (OPEN-2).
	if err := f.store.MergeSnapshot(controller.TopologySnapshot{
		LastFailover: confirmed.Time, LastFailoverTarget: "beta", PromotionGtidExecuted: "00000000-0000-0000-0000-000000000001:1-50",
	}); err != nil {
		t.Fatal(err)
	}
	f.want()
	// A status writer that bypasses the merge does, and the monitor sees it.
	f.write(v1alpha1.PlannedFailoverPhasePublishing, nil, func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.PromotionGtidExecuted = "00000000-0000-0000-0000-000000000001:1-50"
	})
	f.want("WatermarkRegressed")

	// A later promotion record supersedes the watermark.
	g := newMonFixture(t)
	g.write(v1alpha1.PlannedFailoverPhasePublishing, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.Fence.GtidSet = &fence
		pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: "Confirmed", ConfirmedAt: &confirmed}
	}, func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PromotionGtidExecuted = fence })
	_ = g.store.MergeSnapshot(controller.TopologySnapshot{
		LastFailover: confirmed.Add(time.Minute), LastFailoverTarget: "alpha", PromotionGtidExecuted: "00000000-0000-0000-0000-000000000002:1-3",
	})
	g.want()
}

func TestPlannedInvariant_UnconfirmedBlocksEmergency(t *testing.T) {
	f := newMonFixture(t)
	f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
	_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
	_ = f.pf("beta").SetReadOnly(pfCtx(), false)
	f.c.Crash("beta")
	f.terminal(v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed)
	for p := 1; p <= unconfirmedGrace; p++ {
		f.mon.endPoll(p, true)
	}
	f.want()
	f.mon.endPoll(unconfirmedGrace+1, true)
	f.want("UnconfirmedBlocksEmergency")
}

func TestPlannedInvariant_EndOfTrial(t *testing.T) {
	t.Run("fencedSourceNeverHealed", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		f.mon.checkEnd(100)
		f.want("FencedSourceNeverHealed")
	})
	t.Run("healed", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		_ = f.pf("alpha").SetReadOnly(context.Background(), false) // the manager's re-assert
		f.mon.checkEnd(100)
		f.want()
	})
	t.Run("runStuck", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, nil, nil)
		f.mon.checkEnd(100)
		f.want("PlannedRunStuck")
	})
}

// fakeClaim is a claimView for the slot-introspecting invariants.
type fakeClaim struct {
	runID    string
	hold, ok bool
	releases []claimRelease
}

func (c *fakeClaim) Held() (string, bool, bool) { return c.runID, c.hold, c.ok }
func (c *fakeClaim) Releases() []claimRelease {
	r := c.releases
	c.releases = nil
	return r
}

func TestPlannedInvariant_ClaimView(t *testing.T) {
	t.Run("revokedByForeignRelease", func(t *testing.T) {
		f := newMonFixture(t)
		f.mon.claim = &fakeClaim{releases: []claimRelease{{RunID: "r1", Why: "sync"}, {RunID: "r1", Why: controller.ReleaseTerminal}}}
		f.mon.endPoll(1, true)
		f.want("ClaimRevoked")
	})
	t.Run("revokedSlotEmptyWhileOwning", func(t *testing.T) {
		f := newMonFixture(t)
		f.mon.claim = &fakeClaim{}
		f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, nil)
		f.mon.endPoll(1, false) // no manager yet: not judged
		f.want()
		f.mon.endPoll(2, true)
		f.want("ClaimRevoked")
	})
	t.Run("zombie", func(t *testing.T) {
		f := newMonFixture(t)
		f.mon.claim = &fakeClaim{runID: "r1", ok: true}
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		_ = f.pf("beta").SetReadOnly(pfCtx(), false)
		f.terminal(v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed)
		for range zombiePasses {
			f.mon.noteEnginePass()
		}
		f.want()
		f.mon.noteEnginePass()
		f.want("ZombieClaim")
	})
	t.Run("deferralWithEmptySlot", func(t *testing.T) {
		f := newMonFixture(t)
		claim := &fakeClaim{runID: "r1", ok: true}
		f.mon.claim = claim
		f.mon.noteDeferral()
		f.want()
		claim.ok = false
		f.mon.noteDeferral()
		f.want("AutomationHeldByPendingRequest")
	})
	t.Run("stuckSlot", func(t *testing.T) {
		f := newMonFixture(t)
		f.mon.claim = &fakeClaim{runID: "r1", ok: true}
		f.mon.checkEnd(100)
		f.want("PlannedRunStuck")
	})
}
