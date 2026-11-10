package dst

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/state"
)

// A planned sweep far past the gate's range (112,000 seeds, on main and on
// the binary log rotation change) failed about one trial in 2,000. Most of
// those trials judged a correct operator wrong: the harness or the monitor
// compared the operator with something it could not have known or was not
// asked to do. The tests below pin each such defect with a firing and a
// quiet case, so the relaxed check is still shown to catch what it was
// written for. The findings that are the operator's are listed in the
// README (Open planned-failover findings) and still fail the sweep.

// handRunner is a trial runner set up over a two-site hand trial.
func handRunner(t *testing.T, actors bool) *trialRunner {
	t.Helper()
	tr := twoSiteHandTrial(15, true, 20)
	tr.Actors = Actors{Enabled: actors}
	r := &trialRunner{trial: tr, logger: discardLogger(), operatorDeadUntil: -1,
		kindsSeen: map[string]struct{}{}, reasonsSeen: map[string]struct{}{}}
	r.setup(false)
	return r
}

// TestArmRecordedPromotion_LateRecordKeepsTheNewerPromotion: a planned run's
// Publishing record can commit long after its promotion (a status outage),
// and after an ordered update handed the primary back. The operator's
// failover record is timestamp-monotonic, so the late record must not move
// the harness's idea of the operator's target (and of what a restart must
// rehydrate) back to the older promotion: WedgedNoPrimary then judged a
// group whose real target needed a human against the wrong site.
func TestArmRecordedPromotion_LateRecordKeepsTheNewerPromotion(t *testing.T) {
	r := &trialRunner{}
	confirmed := mustFakeNow()
	handoff := confirmed.Add(18 * time.Second)
	r.armRecordedPromotion("beta", confirmed)
	if r.currentTarget != "beta" || !r.lastPromotionAt.Equal(confirmed) {
		t.Fatalf("the first record was not armed: target %q at %v", r.currentTarget, r.lastPromotionAt)
	}
	r.armPromotion("alpha", handoff)
	r.statusDroppedPromotion = true
	r.armRecordedPromotion("beta", confirmed)
	if r.currentTarget != "alpha" || !r.lastPromotionAt.Equal(handoff) || r.promotionArms != 2 || !r.statusDroppedPromotion {
		t.Fatalf("a late record of the older promotion replaced the newer one: target %q at %v, arms %d",
			r.currentTarget, r.lastPromotionAt, r.promotionArms)
	}
	r.armRecordedPromotion("beta", handoff.Add(time.Minute))
	if r.currentTarget != "beta" || r.promotionArms != 3 {
		t.Fatalf("a newer planned record was not armed: target %q, arms %d", r.currentTarget, r.promotionArms)
	}
}

// TestStatusWriteInFlightAcrossAPromotionIsNotADrop: AntiFlapStateLost blames
// the operator when the promoting process lands a status write without the
// promotion's record. The snapshot write is two steps with a yield between
// them, and an ordered update can record its handoff at that yield: the
// write in flight was computed before the record existed and cannot carry
// it. Only a snapshot taken after the record was made can have dropped it.
func TestStatusWriteInFlightAcrossAPromotionIsNotADrop(t *testing.T) {
	r := handRunner(t, true)
	stale := controller.TopologySnapshot{ActiveSite: "alpha", ObservedAt: r.clk.Now()}
	r.pl.store.yield = func(point string) {
		if point == YieldSnapshotCommit && r.promotionArms == 0 {
			r.armPromotion("beta", r.clk.Now())
		}
	}
	r.tm.StatusCallback(stale)
	if r.promotionArms != 1 {
		t.Fatal("the snapshot write did not yield between its read and its commit")
	}
	if r.statusDroppedPromotion {
		t.Fatal("a status write in flight when the promotion was recorded was judged to have dropped it")
	}
	r.pl.store.yield = nil
	r.tm.StatusCallback(stale)
	if !r.statusDroppedPromotion {
		t.Fatal("a snapshot taken after the promotion, still without its record, was not judged a drop")
	}
}

// TestCheckPoll_FenceAfterPromote: the fence of another site belongs before
// the writable grant. A fence the promotion already attempted before the
// grant (failed: the old primary did not answer) and retries after it is in
// order, and so is fencing a site an external writer made writable after
// the grant. A site first fenced after the grant is still out of order.
func TestCheckPoll_FenceAfterPromote(t *testing.T) {
	fence := func(seq int, outcome string) Event {
		return Event{Seq: seq, Site: "beta", Kind: EvSetSuperRO, Detail: "on=true", Outcome: outcome}
	}
	promotion := []Event{
		{Seq: 3, Site: "alpha", Kind: EvResetReplica},
		{Seq: 4, Site: "alpha", Kind: EvSetRO, Detail: "on=false"},
	}
	rogue := func(seq int) Event { return Event{Seq: seq, Site: "beta", Kind: EvRogueWritable} }
	cases := []struct {
		name   string
		events []Event
		want   int
	}{
		{"fencedOnlyAfterTheGrant", append([]Event{}, append(promotion, fence(5, ""))...), 1},
		{"retryOfAnAttemptBeforeTheGrant", append([]Event{fence(1, "unreachable")}, append(promotion, fence(5, "unreachable"))...), 0},
		{"externalWriterAfterTheGrant", append([]Event{}, append(promotion, rogue(5), fence(6, ""))...), 0},
		{"externalWriterBeforeTheGrant", append([]Event{rogue(1)}, append(promotion, fence(5, ""))...), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := handRunner(t, false)
			r.checkPoll(9, r.tm.Status(), tc.events)
			if got := violationsNamed(r.violations, "FenceAfterPromote"); len(got) != tc.want {
				t.Fatalf("FenceAfterPromote %d, want %d: %v", len(got), tc.want, r.violations)
			}
		})
	}
}

// TestRollbackClaimWithinDeadline: a run that is rolling back keeps the claim
// while it retries a step it cannot verify, up to the 30s rollback deadline,
// and the claim defers the manager's split-brain fence meanwhile;
// DualWritableUnresolved counts only past that documented bound.
func TestRollbackClaimWithinDeadline(t *testing.T) {
	r := handRunner(t, true)
	pl := r.pl
	block := func(phase v1alpha1.PlannedFailoverPhase, rev int64) v1alpha1.PlannedFailoverStatus {
		pf := v1alpha1.PlannedFailoverStatus{RunID: "r1", Revision: rev, Phase: phase, TargetSite: "beta", SourceSite: "alpha"}
		if phase == v1alpha1.PlannedFailoverPhaseRollingBack {
			pf.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
				IntentTime: metav1.NewTime(r.clk.Now())}
		}
		return pf
	}
	write := func(pf v1alpha1.PlannedFailoverStatus) {
		t.Helper()
		base, err := pl.store.Load(context.Background(), pl.nn)
		if err != nil {
			t.Fatal(err)
		}
		if out, err := pl.store.Write(context.Background(), base, pf, nil); out != controller.WriteCommitted {
			t.Fatalf("write %s: %s %v", pf.Phase, out, err)
		}
	}
	draining := block(v1alpha1.PlannedFailoverPhaseDraining, 1)
	write(draining)
	pl.reg.register(r.newManager(nil), controller.ManagerWiring{HydrateFrom: &draining})
	if id, hold, ok := pl.reg.Slot(pl.nn).HeldForSim(); !ok || hold || id != "r1" {
		t.Fatalf("slot %q hold=%v ok=%v, want r1's run claim", id, hold, ok)
	}
	if pl.rollbackClaimWithinDeadline() {
		t.Fatal("a claim held in Draining is not a rollback: not exempt")
	}
	write(block(v1alpha1.PlannedFailoverPhaseRollingBack, 2))
	if !pl.rollbackClaimWithinDeadline() {
		t.Fatal("a run rolling back with the claim, inside its deadline: exempt")
	}
	r.clk.Advance(plannedRollbackDeadline + stalledClaimSlack + time.Second)
	if pl.rollbackClaimWithinDeadline() {
		t.Fatal("past the rollback deadline the claim must have been released: not exempt")
	}
}

// promoted writes a Promoting block with the fenced set and runs the
// promotion statements on beta, as the planned path.
func promoted(f *monFixture) *simChecker {
	ctx := pfCtx()
	f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
	_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
	fence := f.fenceSet()
	f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
	b := f.pf("beta")
	_ = b.StopReplica(ctx)
	_ = b.ResetReplicaAll(ctx)
	f.passGate()
	_ = b.SetSuperReadOnly(ctx, false)
	_ = b.SetReadOnly(ctx, false)
	return b
}

// TestPlannedInvariant_RotationOfAPromotedTarget: the promotion's rotation
// goes to a target that is read-only and holds exactly F. A target the run
// made writable gets none while it is writable or holds its own writes
// (RepeatedPromotion). A target that restarted read-only before any write
// landed is the ReplicaReset stage again, and a new process rotates it once
// more; a target an external writer makes writable between the pass's
// classification and its rotation is the rogue race, which the gate after
// the rotation refuses.
func TestPlannedInvariant_RotationOfAPromotedTarget(t *testing.T) {
	ctx := pfCtx()
	t.Run("externalWriterAfterTheClassification", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.c.RogueWritable("beta")
		if err := b.FlushBinaryLogs(ctx); err != nil {
			t.Fatal(err)
		}
		f.want()
	})
	t.Run("whileWritable", func(t *testing.T) {
		f := newMonFixture(t)
		b := promoted(f)
		_ = b.FlushBinaryLogs(ctx)
		f.want("RepeatedPromotion")
	})
	t.Run("restartedReadOnlyWithExactlyF", func(t *testing.T) {
		f := newMonFixture(t)
		promoted(f)
		f.c.RestartSite("beta")
		f.mon.noteRestart()
		b := f.pf("beta")
		if err := b.FlushBinaryLogs(ctx); err != nil {
			t.Fatal(err)
		}
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
		f.want()
	})
	t.Run("restartedWithItsOwnTransactions", func(t *testing.T) {
		f := newMonFixture(t)
		promoted(f)
		if !f.c.AppWrite("beta") {
			t.Fatal("no application write landed on the promoted target")
		}
		f.c.RestartSite("beta")
		f.mon.noteRestart()
		_ = f.pf("beta").FlushBinaryLogs(ctx)
		f.want("RepeatedPromotion")
	})
}

// unconfirmed ends run r1 PromotedUnconfirmed with its target beta crashed
// and its source alpha fenced, reachable and complete.
func unconfirmed(t *testing.T) *monFixture {
	f := newMonFixture(t)
	f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
	_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
	f.passGate()
	_ = f.pf("beta").SetReadOnly(pfCtx(), false)
	f.c.Crash("beta")
	f.terminal(v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed)
	return f
}

// TestPlannedInvariant_UnconfirmedBlocksEmergency_Window: the rule is that a
// live operator promotes the reachable, complete source within the grace
// while the unconfirmed target stays unreachable. It is judged over a
// window in which that held at every poll: a primary that existed inside
// the window, a target that answers again, an operator restart and a failed
// promotion attempt all end or restart it.
func TestPlannedInvariant_UnconfirmedBlocksEmergency_Window(t *testing.T) {
	polls := func(f *monFixture, from, to int) {
		for p := from; p <= to; p++ {
			f.mon.endPoll(p, true)
		}
	}
	t.Run("aPrimaryInsideTheWindow", func(t *testing.T) {
		f := unconfirmed(t)
		polls(f, 1, 2)
		_ = f.c.NewChecker("alpha").SetReadOnly(context.Background(), false) // the emergency promotion
		polls(f, 3, 3)
		_ = f.c.NewChecker("alpha").SetSuperReadOnly(WithActor(context.Background(), ActorUpdate), true) // a later handoff's fence
		polls(f, 4, unconfirmedGrace+5)
		f.want()
	})
	t.Run("theTargetAnswersAgain", func(t *testing.T) {
		f := unconfirmed(t)
		polls(f, 1, 4)
		f.c.Recover("beta", true, false)
		polls(f, 5, unconfirmedGrace+5)
		f.want()
	})
	t.Run("anOperatorRestartStartsItAgain", func(t *testing.T) {
		f := unconfirmed(t)
		polls(f, 1, 10)
		f.mon.noteRestart()
		polls(f, 11, 11+unconfirmedGrace)
		f.want()
		polls(f, 12+unconfirmedGrace, 12+unconfirmedGrace)
		f.want("UnconfirmedBlocksEmergency")
	})
	t.Run("aFailedAttemptStartsItAgain", func(t *testing.T) {
		f := unconfirmed(t)
		polls(f, 1, 9)
		f.c.AddFailMutations("alpha", 1)
		if err := f.c.NewChecker("alpha").StopReplica(context.Background()); err == nil {
			t.Fatal("the injected fault did not fail the manager's statement")
		}
		polls(f, 10, 10+unconfirmedGrace)
		f.want()
		polls(f, 11+unconfirmedGrace, 11+unconfirmedGrace)
		f.want("UnconfirmedBlocksEmergency")
	})
	t.Run("noOperatorDoesNotCount", func(t *testing.T) {
		f := unconfirmed(t)
		for p := 1; p <= unconfirmedGrace+5; p++ {
			f.mon.endPoll(p, false) // no manager registered
		}
		f.want()
		polls(f, unconfirmedGrace+6, 2*unconfirmedGrace+5)
		f.want()
		polls(f, 2*unconfirmedGrace+6, 2*unconfirmedGrace+6)
		f.want("UnconfirmedBlocksEmergency")
	})
}

// TestPlannedInvariant_FencedSourceSuperseded: FencedSourceNeverHealed is the
// run's own fence wedging the group. Once another site was writable after
// the run ended, the group had a primary and the source is the topology
// manager's to demote; what became of that primary is not the run's fence.
func TestPlannedInvariant_FencedSourceSuperseded(t *testing.T) {
	f := newMonFixture(t)
	f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
	_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
	f.terminal(v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
	f.c.RogueWritable("beta")
	f.mon.endPoll(1, true)
	f.c.RogueFence("beta")
	f.mon.endPoll(2, true)
	f.mon.checkEnd(100)
	f.want()
}

// TestPlannedInvariant_OutcomeHonesty_WhatTheRunRead: outcomes are judged
// with what the run could know when it decided.
func TestPlannedInvariant_OutcomeHonesty_WhatTheRunRead(t *testing.T) {
	ctx := pfCtx()
	t.Run("rolledBackThenTheSourceWasFencedAgain", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.c.RogueWritable("alpha")
		f.mon.onYield(YieldPeerProbe) // the rollback reads the source writable: no unfence is sent
		_ = f.c.NewChecker("alpha").SetSuperReadOnly(WithActor(context.Background(), ActorSidecar), true)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.want()
	})
	t.Run("rolledBackOverASourceNeverWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.mon.onYield(YieldPeerProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.want("OutcomeDishonest")
	})
	t.Run("rolledBackAfterAClearTheProcessDiedBeforeSending", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.passGate()
		if !f.c.ArmOperatorCrash(0, true) {
			t.Fatal("could not arm the operator crash")
		}
		if err := f.pf("beta").SetReadOnly(ctx, false); err == nil {
			t.Fatal("the process did not die on the statement")
		}
		if truthNamed(f.c.Truth(), "beta").Writable() {
			t.Fatal("setup: the statement applied")
		}
		f.c.ReviveOperator()
		f.mon.noteRestart()
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.want()
	})
	t.Run("rolledBackAfterAClearThatReachedTheServer", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.passGate()
		f.c.AddFailMutations("beta", 1)
		_ = f.pf("beta").SetReadOnly(ctx, false)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.want("OutcomeDishonest")
	})
	t.Run("sourceLeftFencedWithNoManager", func(t *testing.T) {
		f := newMonFixture(t)
		f.mon.noManager = func() bool { return true }
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		f.want()
	})
}

// TestPlannedInvariant_UnreadableReader: a planned failover is strict about
// its promotable peers only. A read-only reader the operator cannot read is
// allowed at the fence, the gate and the confirmation (with a warning), so
// a reader made writable where the operator cannot see it is not a decision
// the run got wrong. A reader it can read, and an unreadable dr-only site,
// still count.
func TestPlannedInvariant_UnreadableReader(t *testing.T) {
	ctx := pfCtx()
	reader := []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}
	drOnly := []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleDROnly}
	rogueGamma := func(f *monFixture, partitioned bool) {
		if partitioned {
			f.c.SetOperatorLink("gamma", true)
		}
		f.c.RogueWritable("gamma")
		f.mon.onYield(YieldPeerProbe) // the run probed after the rogue: no race residual
	}
	t.Run("fenceBesideAnUnreadableReader", func(t *testing.T) {
		f := newMonFixture(t, reader...)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		rogueGamma(f, true)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.want()
		if f.mon.stats.UnreadableReaderWritable == 0 {
			t.Fatal("the unreadable writable reader was not counted")
		}
	})
	t.Run("fenceBesideAReadableReader", func(t *testing.T) {
		f := newMonFixture(t, reader...)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		rogueGamma(f, false)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.want("PlannedFenceWithoutAuthority")
	})
	t.Run("fenceBesideAnUnreadableDROnlySite", func(t *testing.T) {
		f := newMonFixture(t, drOnly...)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		rogueGamma(f, true)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.want("PlannedFenceWithoutAuthority")
	})
	promote := func(f *monFixture, partitioned bool) {
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		rogueGamma(f, partitioned)
		f.mon.onYield(YieldGateProbe)
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
		f.mon.onYield(YieldConfirmProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
	}
	t.Run("promoteAndSucceedBesideAnUnreadableReader", func(t *testing.T) {
		f := newMonFixture(t, reader...)
		promote(f, true)
		f.want()
	})
	t.Run("promoteAndSucceedBesideAReadableReader", func(t *testing.T) {
		f := newMonFixture(t, reader...)
		promote(f, false)
		f.want("PlannedPromoteBesideWritable", "OutcomeDishonest", "SucceededBesideWritable")
	})
}

// TestPlannedInvariant_HoldWithAnUnreadableTarget: a hold is justified when
// a promotion intent exists, the deciding pass cannot read the target and
// another site is writable: the target may hold the run's writes beside
// that writer. With nobody else writable the hold protects nothing.
func TestPlannedInvariant_HoldWithAnUnreadableTarget(t *testing.T) {
	three := []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate}
	setup := func(t *testing.T) *monFixture {
		f := newMonFixture(t, three...)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		f.c.Crash("beta")
		f.mon.noteRestart() // the re-proof after hydration decides, in a new process
		return f
	}
	hold := func(f *monFixture) {
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
	}
	t.Run("besideAForeignWriter", func(t *testing.T) {
		f := setup(t)
		f.c.RogueWritable("gamma")
		hold(f)
		f.want()
		if f.mon.stats.UnreadableTargetHold != 1 {
			t.Fatalf("UnreadableTargetHold %d, want 1", f.mon.stats.UnreadableTargetHold)
		}
	})
	t.Run("withNobodyWritable", func(t *testing.T) {
		f := setup(t)
		hold(f)
		f.want("HoldWithoutDataAtRisk")
		if d := f.mon.violations[0].Detail; !strings.Contains(d, "channel=true") || !strings.Contains(d, "another process") {
			t.Fatalf("the detail does not describe the target: %s", d)
		}
	})
}

// TestOpenFindings_AreClassesOfTheirOwn: the planned-failover findings that
// are the operator's (README, Open planned-failover findings) keep failing
// the sweep, each under a fingerprint that names it.
func TestOpenFindings_AreClassesOfTheirOwn(t *testing.T) {
	ctx := pfCtx()
	three := []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate}
	const betaUUID = "00000000-0000-0000-0000-000000000002"
	promoting := func(f *monFixture) {
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Fence.GtidSet = &fence
			pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionIntended, TargetServerUUID: betaUUID}
		}, nil)
	}
	hold := func(f *monFixture) {
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
	}
	t.Run("holdOverAWritableTargetWithItsChannel", func(t *testing.T) {
		// A new process finds the target writable (an external read_only=0)
		// with its replication channel still configured, and holds.
		f := newMonFixture(t)
		promoting(f)
		f.mon.noteRestart()
		f.c.RogueWritable("beta")
		hold(f)
		if fp := fingerprint(f.mon.violations); fp != "HoldWithoutDataAtRisk(writableTargetWithChannel)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("holdOverATargetReplacedInTheSameProcess", func(t *testing.T) {
		// The process that wrote Promoting sent super_read_only=OFF, the
		// target pod was then replaced under a new server_uuid and reads
		// read-only inside F, and the run holds.
		f := newMonFixture(t)
		for range 3 {
			f.c.Tick() // history, so the replacement pod can come up regressed
		}
		promoting(f)
		f.passGate()
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		f.c.ReplacePod("beta", 1)
		if truthNamed(f.c.Truth(), "beta").ServerUUID == betaUUID {
			t.Fatal("setup: the replacement pod kept the server_uuid")
		}
		hold(f)
		if fp := fingerprint(f.mon.violations); fp != "HoldWithoutDataAtRisk(replacedTargetSameProcess)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	succeed := func(f *monFixture, partitioned bool) string {
		promoting(f)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.passGate()
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
		if partitioned {
			f.c.SetOperatorLink("gamma", true)
		}
		f.c.RogueWritable("gamma")
		f.mon.onYield(YieldConfirmProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		return fingerprint(f.mon.violations)
	}
	t.Run("succeededBesideAnUnreadableCandidate", func(t *testing.T) {
		if fp := succeed(newMonFixture(t, three...), true); fp != "OutcomeDishonest(unreadablePeer)+SucceededBesideWritable(unreadablePeer)" {
			t.Fatalf("fingerprint %s", fp)
		}
	})
	t.Run("succeededBesideAReadableCandidate", func(t *testing.T) {
		if fp := succeed(newMonFixture(t, three...), false); fp != "OutcomeDishonest+SucceededBesideWritable" {
			t.Fatalf("fingerprint %s", fp)
		}
	})
}
