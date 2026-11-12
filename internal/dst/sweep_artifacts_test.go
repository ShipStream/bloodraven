package dst

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// Planned sweeps far past the gate's range failed about one trial in 2,000
// at first. Most of those trials judged a correct operator wrong: the
// harness or the monitor compared the operator with something it could not
// have known or was not asked to do. The tests below pin each such defect
// with a firing and a quiet case, so the relaxed check is still shown to
// catch what it was written for. The findings that were the operator's are
// listed in the README (Finding classes), are fixed, and fail the
// sweep if they come back.

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
	return unconfirmedIn(newMonFixture(t))
}

// unconfirmedWithReader is unconfirmed in a group with a read-only reader,
// gamma.
func unconfirmedWithReader(t *testing.T) *monFixture {
	return unconfirmedIn(newMonFixture(t, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly))
}

func unconfirmedIn(f *monFixture) *monFixture {
	f.t.Helper()
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
// window in which that held at every poll. The source promoted, or a site
// the manager adopted, ends it; a target that answers, a site that is
// writable without being adopted, an operator restart and a failed
// promotion attempt start it again; a failed rotation does neither.
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
		polls(f, 5, 3*unconfirmedGrace)
		f.want()
	})
	// r1 M3: the target answers for one poll (read-only) and is gone again.
	// That poll is no emergency, but it does not end the judgment: from the
	// next poll a live operator owes the promotion within a new grace.
	t.Run("theTargetAnswersForOnePoll", func(t *testing.T) {
		f := unconfirmed(t)
		polls(f, 1, 4)
		f.c.Recover("beta", true, false)
		polls(f, 5, 5)
		f.c.Crash("beta")
		polls(f, 6, 5+unconfirmedGrace)
		f.want()
		polls(f, 6+unconfirmedGrace, 6+unconfirmedGrace)
		f.want("UnconfirmedBlocksEmergency")
	})
	// The same when the target still answered at the moment the run ended
	// and is gone a poll later: the window is open from the run's end.
	t.Run("theTargetAnsweredWhenTheRunEnded", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
		f.passGate()
		f.c.AddFailMutations("beta", 1)
		_ = f.pf("beta").SetReadOnly(pfCtx(), false) // sent, not applied: the target stays read-only
		f.terminal(v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed)
		polls(f, 1, 1)
		f.c.Crash("beta")
		polls(f, 2, 1+unconfirmedGrace)
		f.want()
		polls(f, 2+unconfirmedGrace, 2+unconfirmedGrace)
		f.want("UnconfirmedBlocksEmergency")
	})
	t.Run("theTargetAnsweredWhenTheRunEndedAndStillDoes", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
		f.passGate()
		f.c.AddFailMutations("beta", 1)
		_ = f.pf("beta").SetReadOnly(pfCtx(), false)
		f.terminal(v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed)
		polls(f, 1, 3*unconfirmedGrace)
		f.want()
	})
	// A read-only reader an external writer opens for one poll is not a
	// primary the manager adopts: it starts the window again and no more.
	t.Run("aReaderWritableForOnePoll", func(t *testing.T) {
		f := unconfirmedWithReader(t)
		polls(f, 1, 4)
		f.c.RogueWritable("gamma")
		polls(f, 5, 5)
		f.c.RogueFence("gamma")
		polls(f, 6, 5+unconfirmedGrace)
		f.want()
		polls(f, 6+unconfirmedGrace, 6+unconfirmedGrace)
		f.want("UnconfirmedBlocksEmergency")
	})
	// A promotable site the manager counted as the primary ends the
	// judgment, whatever becomes of that site afterwards.
	t.Run("aSiteTheManagerAdopted", func(t *testing.T) {
		f := unconfirmedWithReader(t)
		adopted := ""
		f.mon.activeSite = func() string { return adopted }
		polls(f, 1, 4)
		f.c.Recover("beta", false, true) // the target boots writable and the manager adopts it
		adopted = "beta"
		polls(f, 5, 5)
		f.c.Crash("beta")
		adopted = ""
		polls(f, 6, 3*unconfirmedGrace)
		f.want()
	})
	// A writable site the manager did not adopt (it never observed it)
	// does not end the judgment either.
	t.Run("aWritableSiteTheManagerNeverSaw", func(t *testing.T) {
		f := unconfirmedWithReader(t)
		f.mon.activeSite = func() string { return "" }
		polls(f, 1, 4)
		f.c.RogueWritable("gamma")
		polls(f, 5, 5)
		f.c.RogueFence("gamma")
		polls(f, 6, 6+unconfirmedGrace)
		f.want("UnconfirmedBlocksEmergency")
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
	// A rotation is best effort: one that fails or is abandoned on the
	// source does not stop its promotion, so it does not restart the window.
	t.Run("aFailedRotationDoesNotStartItAgain", func(t *testing.T) {
		f := unconfirmed(t)
		polls(f, 1, 9)
		f.c.AddFlushFailures("alpha", 1)
		if err := f.c.NewChecker("alpha").FlushBinaryLogs(context.Background()); err == nil {
			t.Fatal("the injected fault did not fail the manager's rotation")
		}
		polls(f, 10, unconfirmedGrace)
		f.want()
		polls(f, unconfirmedGrace+1, unconfirmedGrace+1)
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
// run's own fence wedging the group. It stops being that once the topology
// manager takes authority away from the source: it counts another site as
// the primary, or it fences or repoints the source itself. What became of
// that primary is then not the run's fence. A site that is only writable
// for a while without being adopted (r1 M2: a read-only reader an external
// writer opened for one poll, a candidate the manager never observed)
// leaves the run's fence to be healed, and a source nobody heals is
// reported.
func TestPlannedInvariant_FencedSourceSuperseded(t *testing.T) {
	// leftFenced ends run r1 SourceLeftFenced with its own fence on alpha.
	leftFenced := func(t *testing.T, roles ...state.SiteRole) *monFixture {
		f := newMonFixture(t, roles...)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		return f
	}
	settle := func(f *monFixture, from int) {
		for p := from; p <= 200; p++ {
			f.mon.endPoll(p, true)
		}
		f.mon.checkEnd(201)
	}
	withReader := []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}

	t.Run("theManagerAdoptedAnotherSite", func(t *testing.T) {
		f := leftFenced(t)
		adopted := ""
		f.mon.activeSite = func() string { return adopted }
		f.c.RogueWritable("beta")
		adopted = "beta"
		f.mon.endPoll(1, true)
		f.c.RogueFence("beta")
		adopted = ""
		settle(f, 2)
		f.want()
	})
	t.Run("aCandidateWritableAtAPoll", func(t *testing.T) {
		// No manager view wired: the monitor derives it from the truth, and
		// a manager polling now would count the one writable candidate.
		f := leftFenced(t)
		f.c.RogueWritable("beta")
		f.mon.endPoll(1, true)
		f.c.RogueFence("beta")
		settle(f, 2)
		f.want()
	})
	t.Run("theManagerRepointedTheSource", func(t *testing.T) {
		f := leftFenced(t)
		f.mon.activeSite = func() string { return "" }
		tm := f.c.NewChecker("alpha")
		if err := tm.ChangeReplicationSource(context.Background(), mysql.ReplicationSourceOpts{Host: "beta.mysql.sim"}); err != nil {
			t.Fatalf("the manager's CHANGE REPLICATION SOURCE: %v", err)
		}
		settle(f, 1)
		f.want()
	})
	t.Run("theManagerFencedTheSource", func(t *testing.T) {
		f := leftFenced(t)
		f.mon.activeSite = func() string { return "" }
		if err := f.c.NewChecker("alpha").SetSuperReadOnly(context.Background(), true); err != nil {
			t.Fatalf("the manager's fence: %v", err)
		}
		settle(f, 1)
		f.want()
	})
	t.Run("aReaderWritableForOnePoll", func(t *testing.T) {
		f := leftFenced(t, withReader...)
		f.c.RogueWritable("gamma")
		f.mon.endPoll(1, true)
		f.c.RogueFence("gamma")
		settle(f, 2)
		f.want("FencedSourceNeverHealed")
	})
	t.Run("aCandidateTheManagerNeverObserved", func(t *testing.T) {
		f := leftFenced(t)
		f.mon.activeSite = func() string { return "" }
		f.c.RogueWritable("beta")
		f.mon.endPoll(1, true)
		f.c.RogueFence("beta")
		settle(f, 2)
		f.want("FencedSourceNeverHealed")
	})
	t.Run("aCandidateWritableWhileNoManagerRan", func(t *testing.T) {
		f := leftFenced(t)
		f.c.RogueWritable("beta")
		f.mon.endPoll(1, false)
		f.c.RogueFence("beta")
		settle(f, 2)
		f.want("FencedSourceNeverHealed")
	})
	t.Run("aSidecarFenceOfTheSourceIsNotTheManagers", func(t *testing.T) {
		f := leftFenced(t)
		f.mon.activeSite = func() string { return "" }
		_ = f.c.NewChecker("alpha").SetSuperReadOnly(WithActor(context.Background(), ActorSidecar), true)
		settle(f, 1)
		f.want("FencedSourceNeverHealed")
	})
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
	// A read_only=OFF that reached the target is judged by what the target
	// held when the rollback was decided, not by the statement.
	clearThenRollBack := func(t *testing.T, between func(f *monFixture)) *monFixture {
		f := newMonFixture(t)
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
		if !truthNamed(f.c.Truth(), "beta").Writable() {
			t.Fatal("setup: the clears did not make the target writable")
		}
		between(f)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		// The rollback finds the source already writable and sends no unfence.
		f.c.RogueWritable("alpha")
		f.mon.onYield(YieldPeerProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		return f
	}
	t.Run("rolledBackAfterAClearThatFailed", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.passGate()
		f.c.AddFailMutations("beta", 1)
		_ = f.pf("beta").SetReadOnly(ctx, false) // sent, refused: the target stays read-only
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.want()
	})
	t.Run("rolledBackOverATargetWritableForAMomentThatTookNothing", func(t *testing.T) {
		// Both clears landed; before any transaction committed the target
		// was fenced again. The same server holds exactly the fenced set:
		// nothing was written.
		clearThenRollBack(t, func(f *monFixture) { f.c.RogueFence("beta") }).want()
	})
	t.Run("rolledBackOverATargetStillWritableByTheRunsClear", func(t *testing.T) {
		clearThenRollBack(t, func(*monFixture) {}).want("OutcomeDishonest")
	})
	t.Run("rolledBackOverATargetThatTookAWrite", func(t *testing.T) {
		clearThenRollBack(t, func(f *monFixture) {
			if !f.c.AppWrite("beta") {
				t.Fatal("setup: no write landed on the target")
			}
			f.c.RogueFence("beta")
		}).want("OutcomeDishonest")
	})
	t.Run("rolledBackOverATargetAnExternalWriterOpenedAfterTheClear", func(t *testing.T) {
		// The run's clear was undone and an external read_only=0 followed:
		// the target is writable when the rollback is decided, but not by
		// the run's doing.
		clearThenRollBack(t, func(f *monFixture) {
			f.c.RogueFence("beta")
			f.c.RogueWritable("beta")
		}).want()
	})
	// A run that ends RolledBack straight from Fencing ("authority moved
	// before the fence was sent") is judged by what its last pass read at
	// its peer probe.
	fromFencing := func(t *testing.T, pass func(f *monFixture)) *monFixture {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		pass(f)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		return f
	}
	sidecarFence := func(f *monFixture) {
		_ = f.c.NewChecker("alpha").SetSuperReadOnly(WithActor(context.Background(), ActorSidecar), true)
	}
	t.Run("rolledBackFromFencingThenTheSourceWasFenced", func(t *testing.T) {
		fromFencing(t, func(f *monFixture) {
			f.mon.onYield(YieldPeerProbe) // the pass reads the source writable
			sidecarFence(f)               // its sidecar fences it before the record commits
		}).want()
	})
	t.Run("rolledBackFromFencingOverASourceFencedBeforeThePass", func(t *testing.T) {
		fromFencing(t, func(f *monFixture) {
			sidecarFence(f)
			f.mon.onYield(YieldPeerProbe)
		}).want("OutcomeDishonest")
	})
	t.Run("rolledBackFromFencingOverASourceReadWritableInAnEarlierPass", func(t *testing.T) {
		fromFencing(t, func(f *monFixture) {
			f.mon.onYield(YieldPeerProbe)
			f.mon.noteEnginePass() // that pass ended; the next one read nothing
			sidecarFence(f)
		}).want("OutcomeDishonest")
	})
	t.Run("rolledBackFromFencingAfterTheRunsOwnFence", func(t *testing.T) {
		fromFencing(t, func(f *monFixture) {
			f.mon.onYield(YieldPeerProbe)
			_ = f.pf("alpha").SetSuperReadOnly(ctx, true) // the run fenced it after the probe
		}).want("OutcomeDishonest")
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

// TestFixedFindings_AreClassesOfTheirOwn: the first three planned-failover
// findings that were the operator's (README, Finding classes) still
// fail the sweep if the engine produces them again, each under a
// fingerprint that names it. TestPlannedInvariant_HoldOverARegressedTarget
// does the same for the regressed-target class.
func TestFixedFindings_AreClassesOfTheirOwn(t *testing.T) {
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
	t.Run("holdOverATargetReplacedInTheSameProcessBeforeAnyClear", func(t *testing.T) {
		// The process that wrote Promoting sent no clear: it knows nothing
		// was written to the server the intent names, so the replaced-target
		// exception is not its to claim, whatever the other sites read.
		f := newMonFixture(t)
		for range 3 {
			f.c.Tick()
		}
		promoting(f)
		_ = f.pf("beta").StopReplica(ctx)
		f.c.ReplacePod("beta", 1)
		f.c.SetOperatorLink("alpha", true) // no DST-3 evidence either
		hold(f)
		if fp := fingerprint(f.mon.violations); fp != "HoldWithoutDataAtRisk(replacedTargetSameProcess)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("holdOverAReplacedTargetWithoutTheRulingsEvidence", func(t *testing.T) {
		// The same process after a clear, the target replaced, and the
		// source out of the operator's reach: the DST-3 ruling's evidence
		// is not there, so the hold is the documented one, as for another
		// process (counted, not a violation).
		f := newMonFixture(t)
		for range 3 {
			f.c.Tick()
		}
		promoting(f)
		f.passGate()
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		f.c.ReplacePod("beta", 1)
		f.c.SetOperatorLink("alpha", true)
		hold(f)
		f.want()
		if f.mon.stats.ReplacedTargetHold != 1 {
			t.Fatalf("ReplacedTargetHold %d, want 1", f.mon.stats.ReplacedTargetHold)
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
		// No gate probe of the confirming process read gamma (passGate
		// records none): it never read the site it confirmed beside.
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

// TestPlannedInvariant_UnreadablePeerAtConfirm: a Succeeded run confirmed
// beside a writable primary-candidate the operator could not read is the
// documented residual only when the confirming process last read that site
// read-only, at its pre-write gate or an earlier confirm probe, and an
// external writer made it writable afterwards (§7.3). A site the process
// last read writable, never read, or read before a restart is a second
// writer the run had no right to confirm beside.
func TestPlannedInvariant_UnreadablePeerAtConfirm(t *testing.T) {
	ctx := pfCtx()
	three := []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate}
	const tagged = "OutcomeDishonest(unreadablePeer)+SucceededBesideWritable(unreadablePeer)"
	// promote runs r1 up to its clears, through a gate probe that reads
	// every site read-only.
	promote := func(f *monFixture) {
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.mon.onYield(YieldGateProbe)
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
	}
	hide := func(f *monFixture, site string) {
		f.c.SetOperatorLink(site, true)
		f.c.RogueWritable(site)
	}
	confirm := func(f *monFixture) string {
		f.mon.onYield(YieldConfirmProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		return fingerprint(f.mon.violations)
	}
	t.Run("readOnlyAtTheGateThenUnreadable", func(t *testing.T) {
		f := newMonFixture(t, three...)
		promote(f)
		hide(f, "gamma")
		if fp := confirm(f); fp != "" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
		if f.mon.stats.UnreadablePeerResidual != 1 {
			t.Fatalf("UnreadablePeerResidual %d, want 1", f.mon.stats.UnreadablePeerResidual)
		}
	})
	t.Run("theSourceReadOnlyAtTheGateThenUnreadable", func(t *testing.T) {
		f := newMonFixture(t)
		promote(f)
		hide(f, "alpha")
		if fp := confirm(f); fp != "" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("readOnlyAtAnEarlierConfirmThenUnreadable", func(t *testing.T) {
		// Another process confirms: its first confirm probe reads gamma
		// read-only, its second cannot read it.
		f := newMonFixture(t, three...)
		promote(f)
		f.mon.noteRestart()
		f.mon.onYield(YieldConfirmProbe)
		hide(f, "gamma")
		if fp := confirm(f); fp != "" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("lastReadWritable", func(t *testing.T) {
		// The run saw gamma writable after its own write (the hold it
		// decided did not commit), then gamma stopped answering.
		f := newMonFixture(t, three...)
		promote(f)
		f.c.RogueWritable("gamma")
		f.mon.onYield(YieldConfirmProbe)
		// The external writer acts again where the operator cannot see it:
		// a later rogue does not turn the writable answer into a residual.
		hide(f, "gamma")
		if fp := confirm(f); fp != tagged {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("neverReadByTheConfirmingProcess", func(t *testing.T) {
		// A new process finds the target writable (an external writer, not
		// the run: no clear was sent) and confirms beside a source it
		// cannot read.
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.mon.noteRestart()
		f.c.RogueWritable("beta")
		hide(f, "alpha")
		if fp := confirm(f); fp != tagged {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("readBeforeARestart", func(t *testing.T) {
		// The gate's answers died with the process that read them.
		f := newMonFixture(t, three...)
		promote(f)
		f.mon.noteRestart()
		hide(f, "gamma")
		if fp := confirm(f); fp != tagged {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("writableBeforeTheGateWhereTheGateCouldNotRead", func(t *testing.T) {
		// gamma was already out of reach and writable at the gate: no
		// probe ever read it.
		f := newMonFixture(t, three...)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		hide(f, "gamma")
		f.mon.onYield(YieldPeerProbe) // the run probed after the rogue: no race residual
		f.mon.onYield(YieldGateProbe)
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
		f.mon.onYield(YieldConfirmProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		if got := f.names(); !slices.Contains(got, "OutcomeDishonest") || !slices.Contains(got, "SucceededBesideWritable") {
			t.Fatalf("violations %v, want OutcomeDishonest and SucceededBesideWritable among them", f.mon.violations)
		}
	})
	t.Run("madeWritableByTheOperator", func(t *testing.T) {
		// The residual is an external writer's doing. A site the operator
		// itself made writable after the gate is not excused by the gate's
		// answer.
		f := newMonFixture(t, three...)
		promote(f)
		_ = f.c.NewChecker("gamma").SetReadOnly(context.Background(), false) // the topology manager
		f.c.SetOperatorLink("gamma", true)
		f.mon.onYield(YieldConfirmProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		if got := f.names(); !slices.Contains(got, "OutcomeDishonest") || !slices.Contains(got, "SucceededBesideWritable") {
			t.Fatalf("violations %v, want OutcomeDishonest and SucceededBesideWritable among them", f.mon.violations)
		}
	})
	t.Run("readableAndWritable", func(t *testing.T) {
		f := newMonFixture(t, three...)
		promote(f)
		f.c.RogueWritable("gamma")
		if fp := confirm(f); fp != "OutcomeDishonest+SucceededBesideWritable" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
}

// TestPlannedInvariant_RolledBackOverAReplacedTarget: RolledBack after a
// read_only=OFF is judged against the server that stood in for the target
// when the rollback was decided (its RollingBack record). A target that
// took a write, or is still writable by the run's clear, is dishonest; one
// replaced before the decision, read-only inside the fenced set, is the
// DST-3 ruling's rollback. A pod replaced after the decision, before the
// terminal record, hides nothing: the rollback was decided over the server
// the run had made writable. (The model replaces a pod on an older data
// directory only when no transaction is lost for good, so the replaced
// cases use a target that took no write.)
func TestPlannedInvariant_RolledBackOverAReplacedTarget(t *testing.T) {
	ctx := pfCtx()
	const betaUUID = "00000000-0000-0000-0000-000000000002"
	// fenceBefore: the target is fenced again before the rollback is
	// decided; otherwise it is still writable by the run's clear then, and
	// fenced only afterwards.
	run := func(t *testing.T, write, fenceBefore, replaceBefore, replaceAfter bool) *monFixture {
		f := newMonFixture(t)
		for range 3 {
			f.c.Tick() // history, so the replacement pod can come up regressed
		}
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Fence.GtidSet = &fence
			pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionIntended, TargetServerUUID: betaUUID}
		}, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.passGate()
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false) // both clears applied: beta was writable
		if write && !f.c.AppWrite("beta") {
			t.Fatal("setup: no write landed on the target")
		}
		replace := func() {
			f.c.ReplacePod("beta", 1)
			if truthNamed(f.c.Truth(), "beta").ServerUUID == betaUUID {
				t.Fatal("setup: the replacement kept the server_uuid")
			}
		}
		if fenceBefore {
			f.c.RogueFence("beta")
		}
		if replaceBefore {
			replace()
		}
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		// The rollback finds the source already writable and sends no
		// unfence.
		f.c.RogueWritable("alpha")
		f.mon.onYield(YieldPeerProbe)
		if !fenceBefore {
			f.c.RogueFence("beta")
		}
		if replaceAfter {
			replace()
		}
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		return f
	}
	flagged := func(t *testing.T, f *monFixture) {
		t.Helper()
		if fp := fingerprint(f.mon.violations); fp != "OutcomeDishonest(rolledBackAfterClear)" {
			t.Fatalf("fingerprint %q: %v", fp, f.mon.violations)
		}
	}
	t.Run("theSameServerTookAWrite", func(t *testing.T) {
		flagged(t, run(t, true, true, false, false))
	})
	t.Run("theSameServerStillWritable", func(t *testing.T) {
		flagged(t, run(t, false, false, false, false))
	})
	t.Run("theSameServerTookNothing", func(t *testing.T) {
		run(t, false, true, false, false).want()
	})
	t.Run("replacedBeforeTheRollbackWasDecided", func(t *testing.T) {
		run(t, false, true, true, false).want()
	})
	t.Run("replacedAfterTheRollbackWasDecided", func(t *testing.T) {
		// Judged when the terminal record lands, the replacement would
		// stand in for the server the run had left writable.
		flagged(t, run(t, false, false, false, true))
	})
}

// TestPlannedInvariant_ReplacedTargetHoldBesideAnUnreadableReader: the
// DST-3 ruling needs every promotable peer to answer. A read-only-role
// reader that does not answer is allowed there as everywhere, so it does
// not excuse a hold over a replaced target; an unreadable dr-only site
// does.
func TestPlannedInvariant_ReplacedTargetHoldBesideAnUnreadableReader(t *testing.T) {
	ctx := pfCtx()
	const betaUUID = "00000000-0000-0000-0000-000000000002"
	run := func(t *testing.T, third state.SiteRole) *monFixture {
		f := newMonFixture(t, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, third)
		for range 3 {
			f.c.Tick()
		}
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Fence.GtidSet = &fence
			pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionIntended, TargetServerUUID: betaUUID}
		}, nil)
		f.passGate()
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		f.c.ReplacePod("beta", 1)
		f.c.SetOperatorLink("gamma", true) // out of the operator's reach
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
		return f
	}
	t.Run("aReader", func(t *testing.T) {
		f := run(t, state.SiteRoleReadOnly)
		if fp := fingerprint(f.mon.violations); fp != "HoldWithoutDataAtRisk(replacedTargetSameProcess)" {
			t.Fatalf("fingerprint %q: %v", fp, f.mon.violations)
		}
	})
	t.Run("aDROnlySite", func(t *testing.T) {
		f := run(t, state.SiteRoleDROnly)
		f.want()
		if f.mon.stats.ReplacedTargetHold != 1 {
			t.Fatalf("ReplacedTargetHold %d, want 1", f.mon.stats.ReplacedTargetHold)
		}
	})
}

// TestPlannedInvariant_HoldOverASourceThatLostTransactions: a hold set by
// a rollback that read the source writable without part of the fenced set
// is justified (two histories), also when the run sent no promotion
// statement. The same hold over a source that holds the fenced set is not.
func TestPlannedInvariant_HoldOverASourceThatLostTransactions(t *testing.T) {
	ctx := pfCtx()
	run := func(t *testing.T, regress bool) *monFixture {
		f := newMonFixture(t)
		for range 4 {
			f.c.Tick()
		}
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		for range 3 {
			f.c.Tick() // beta applies the fenced set
		}
		if !containsSet(truthNamed(f.c.Truth(), "beta"), fence) {
			t.Fatal("setup: the target did not catch up")
		}
		if regress {
			if !f.c.RegressSite("alpha", 5) {
				t.Fatal("setup: the source did not regress")
			}
			f.c.Recover("alpha", false, false)
		}
		f.c.RogueWritable("alpha")
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.mon.onYield(YieldPeerProbe) // the rollback's probe, the source writable
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
		return f
	}
	t.Run("theSourceLostPartOfTheFencedSet", func(t *testing.T) {
		f := run(t, true)
		f.want()
		if f.mon.stats.SourceLostHold != 1 {
			t.Fatalf("SourceLostHold %d, want 1", f.mon.stats.SourceLostHold)
		}
	})
	t.Run("theSourceHoldsTheFencedSet", func(t *testing.T) {
		f := run(t, false)
		f.want("OutcomeDishonest", "HoldWithoutDataAtRisk")
	})
	t.Run("theSourceLostItsOwnTransactionsBeforeTheFence", func(t *testing.T) {
		// The source restarts on the older directory before the fence is
		// read, so the fenced set is the short one and the source holds all
		// of it. The target holds what the source issued and lost.
		f := newMonFixture(t)
		for range 6 {
			f.c.Tick()
		}
		if !f.c.RegressSite("alpha", 3) {
			t.Fatal("setup: the source did not regress")
		}
		f.c.Recover("alpha", false, true) // boots writable
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		src, tgt := truthNamed(f.c.Truth(), "alpha"), truthNamed(f.c.Truth(), "beta")
		if !containsSet(src, fence) || !sourceLacksHistory(src, []SiteTruth{tgt}, fence) {
			t.Fatalf("setup: source %s, target %s, fence %s; want the source to hold the fenced set and lack what the target holds of its stream",
				src.Executed, tgt.Executed, fence)
		}
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		f.c.RogueWritable("alpha")
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.mon.onYield(YieldPeerProbe)
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
		f.want()
		if f.mon.stats.SourceLostHold != 1 {
			t.Fatalf("SourceLostHold %d, want 1", f.mon.stats.SourceLostHold)
		}
	})
	// The rollback compares the source with every site that answers, not
	// only the target. The source lost its own transactions before the
	// fence was read; the target is out of the operator's reach, so only
	// the disaster-recovery site can show it.
	witnessed := func(t *testing.T, gammaReadable bool) *monFixture {
		f := newMonFixture(t, state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate, state.SiteRoleDROnly)
		for range 6 {
			f.c.Tick()
		}
		if !f.c.RegressSite("alpha", 3) {
			t.Fatal("setup: the source did not regress")
		}
		f.c.Recover("alpha", false, true)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.c.SetOperatorLink("beta", true)
		if !gammaReadable {
			f.c.SetOperatorLink("gamma", true)
		}
		src := truthNamed(f.c.Truth(), "alpha")
		if !containsSet(src, fence) || sourceLacksHistory(src, f.c.Truth(), fence) != gammaReadable {
			t.Fatalf("setup: source %s, fence %s, gamma readable %v; want the source to hold the fenced set and gamma to be the only evidence",
				src.Executed, fence, gammaReadable)
		}
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
		f.c.RogueWritable("alpha")
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.mon.onYield(YieldPeerProbe)
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
		return f
	}
	t.Run("aSiteTheOperatorCanReadHoldsTheSourcesOwnTransactions", func(t *testing.T) {
		f := witnessed(t, true)
		f.want()
		if f.mon.stats.SourceLostHold != 1 {
			t.Fatalf("SourceLostHold %d, want 1", f.mon.stats.SourceLostHold)
		}
	})
	t.Run("noSiteTheOperatorCanReadHoldsThem", func(t *testing.T) {
		// The run could not have known: the hold is not the documented one.
		f := witnessed(t, false)
		if f.mon.stats.SourceLostHold != 0 {
			t.Fatalf("SourceLostHold %d, want 0", f.mon.stats.SourceLostHold)
		}
		f.want("OutcomeDishonest")
	})
}

// TestSourceLacksHistory_WhoseSetIsEvidence: the model's form of the
// rollback's comparison takes the fenced set whole, and another site's set
// only when the operator can read the site and only under a stream of the
// source that no clone into it superseded.
func TestSourceLacksHistory_WhoseSetIsEvidence(t *testing.T) {
	const (
		first  = "00000000-0000-0000-0000-000000000001" // the source's original stream
		second = "00000002-0000-0000-0000-000000000001" // its stream after incarnation 2
		other  = "00000000-0000-0000-0000-000000000002"
	)
	src := SiteTruth{Name: "alpha", ServerUUID: second, Executed: gtidVec{first: 100, second: 5, other: 7}}
	holder := func(mut func(*SiteTruth)) []SiteTruth {
		h := SiteTruth{Name: "gamma", ServerUUID: "00000000-0000-0000-0000-000000000003", Executed: gtidVec{first: 100, second: 5, other: 7}}
		mut(&h)
		return []SiteTruth{src, h}
	}
	cloned := src
	cloned.ClonedAt = 2
	for _, tc := range []struct {
		name   string
		src    SiteTruth
		others []SiteTruth
		fence  string
		want   bool
	}{
		{"nothing more anywhere", src, holder(func(*SiteTruth) {}), "", false},
		{"more under the stream it runs under", src, holder(func(h *SiteTruth) { h.Executed[second] = 6 }), "", true},
		{"more under an earlier stream, no clone since", src, holder(func(h *SiteTruth) { h.Executed[first] = 104 }), "", true},
		{"more under an earlier stream a clone superseded", cloned, holder(func(h *SiteTruth) { h.Executed[first] = 104 }), "", false},
		{"more under the stream a clone gave it", cloned, holder(func(h *SiteTruth) { h.Executed[second] = 6 }), "", true},
		{"more under another server's stream", src, holder(func(h *SiteTruth) { h.Executed[other] = 9 }), "", false},
		{"the holder is down", src, holder(func(h *SiteTruth) { h.Executed[second] = 6; h.Crashed = true }), "", false},
		{"the holder is cut off from the operator", src, holder(func(h *SiteTruth) { h.Executed[second] = 6; h.OpLinkDown = true }), "", false},
		{"short of the fenced set under a superseded stream", cloned, holder(func(*SiteTruth) {}), first + ":1-104", true},
		{"holds the fenced set", cloned, holder(func(*SiteTruth) {}), first + ":1-100," + second + ":1-5", false},
	} {
		if got := sourceLacksHistory(tc.src, tc.others, tc.fence); got != tc.want {
			t.Errorf("%s: sourceLacksHistory = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestPlannedInvariant_HoldOverARegressedTarget: a hold over the intent's
// server on an older copy of its data is judged like one over a replaced
// pod: a violation, under its own fingerprint, when the DST-3 ruling's
// evidence is complete; the documented hold when it is not.
func TestPlannedInvariant_HoldOverARegressedTarget(t *testing.T) {
	ctx := pfCtx()
	const betaUUID = "00000000-0000-0000-0000-000000000002"
	run := func(t *testing.T, sourceDown bool) *monFixture {
		f := newMonFixture(t)
		for range 4 {
			f.c.Tick()
		}
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		for range 3 {
			f.c.Tick() // beta applies the fenced set
		}
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Fence.GtidSet = &fence
			pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionIntended, TargetServerUUID: betaUUID}
		}, nil)
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.mon.noteRestart() // another process decides
		if !f.c.RegressSite("beta", 6) {
			t.Fatal("setup: the target did not regress")
		}
		f.c.Recover("beta", true, false)
		tgt := truthNamed(f.c.Truth(), "beta")
		if tgt.ServerUUID != betaUUID || containsSet(tgt, fence) {
			t.Fatalf("setup: target uuid %s, holds the fenced set %v; want the same server short of it", tgt.ServerUUID, containsSet(tgt, fence))
		}
		if sourceDown {
			f.c.SetOperatorLink("alpha", true)
		}
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
		return f
	}
	t.Run("theEvidenceComplete", func(t *testing.T) {
		f := run(t, false)
		if fp := fingerprint(f.mon.violations); fp != "HoldWithoutDataAtRisk(regressedTarget)" {
			t.Fatalf("fingerprint %q: %v", fp, f.mon.violations)
		}
	})
	t.Run("theSourceOutOfReach", func(t *testing.T) {
		f := run(t, true)
		f.want()
		if f.mon.stats.ReplacedTargetHold != 1 {
			t.Fatalf("ReplacedTargetHold %d, want 1", f.mon.stats.ReplacedTargetHold)
		}
	})
}

// TestOutcomeFindings_AreClassesOfTheirOwn: the three outcome findings of
// the wider sweep (README, Fixed in the monitor) turned out to
// be the monitor's: each judged an honest record against something the run
// could not have read. The rules were narrowed, not removed: each still
// fires, under the fingerprint that names it, on the history it was written
// for, and is quiet on the one the sweep hit.
func TestOutcomeFindings_AreClassesOfTheirOwn(t *testing.T) {
	ctx := pfCtx()
	promoting := func(f *monFixture) {
		for range 3 {
			f.c.Tick() // history, so a replacement pod can come up regressed
		}
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) { pf.Fence.GtidSet = &fence }, nil)
	}
	clears := func(f *monFixture) {
		b := f.pf("beta")
		_ = b.StopReplica(ctx)
		_ = b.ResetReplicaAll(ctx)
		f.mon.onYield(YieldGateProbe)
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
	}
	t.Run("succeededOverATargetThatLostTransactionsAfterTheConfirmation", func(t *testing.T) {
		// The sweep's history: the run confirms over a target that holds
		// the fenced set; the target then loses part of it (its pod is
		// replaced, or it restarts, on an older data directory) before the
		// terminal record lands.
		for _, lose := range []struct {
			name string
			do   func(f *monFixture)
		}{
			{"replaced", func(f *monFixture) { f.c.RogueFence("beta"); f.c.ReplacePod("beta", 2) }},
			{"regressed", func(f *monFixture) {
				if !f.c.RegressSite("beta", 2) {
					t.Fatal("setup: the target did not regress")
				}
			}},
		} {
			f := newMonFixture(t)
			promoting(f)
			clears(f)
			f.mon.onYield(YieldConfirmProbe)
			lose.do(f)
			if containsSet(truthNamed(f.c.Truth(), "beta"), f.mon.runs["r1"].fenceSet) {
				t.Fatalf("setup (%s): the target still holds the fenced set", lose.name)
			}
			f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
			if fp := fingerprint(f.mon.violations); fp != "" {
				t.Fatalf("%s: fingerprint %s: %v", lose.name, fp, f.mon.violations)
			}
		}
	})
	t.Run("succeededOverATargetThatLackedTransactionsAtTheConfirmation", func(t *testing.T) {
		f := newMonFixture(t)
		promoting(f)
		clears(f)
		if !f.c.RegressSite("beta", 2) {
			t.Fatal("setup: the target did not regress")
		}
		f.c.Recover("beta", false, false)
		f.c.RogueWritable("beta")
		f.mon.onYield(YieldConfirmProbe) // the run confirms over the short target
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		if fp := fingerprint(f.mon.violations); fp != "OutcomeDishonest(targetLacksFencedSet)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("rolledBackAfterAClearOverATargetThatTookNothing", func(t *testing.T) {
		// The sweep's history: both clears land, the target is fenced again
		// before a transaction commits, and a new process rolls back over
		// the same server holding exactly the fenced set.
		f := newMonFixture(t)
		promoting(f)
		clears(f)
		f.c.RogueFence("beta")
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		_ = f.pf("alpha").SetReadOnly(ctx, false)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		if fp := fingerprint(f.mon.violations); fp != "" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("rolledBackAfterAClearOverATargetStillWritable", func(t *testing.T) {
		f := newMonFixture(t)
		promoting(f)
		clears(f)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.c.RogueWritable("alpha") // found writable: the rollback sends no unfence
		f.mon.onYield(YieldPeerProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		if fp := fingerprint(f.mon.violations); fp != "OutcomeDishonest(rolledBackAfterClear)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("rolledBackFromFencingOverASourceTheRunReadWritable", func(t *testing.T) {
		// The sweep's history: the fence was never sent; the pass that ends
		// the run reads the source writable beside another writer, and the
		// source's sidecar fences it before the record commits.
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		f.mon.onYield(YieldPeerProbe)
		_ = f.c.NewChecker("alpha").SetSuperReadOnly(WithActor(context.Background(), ActorSidecar), true)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		if fp := fingerprint(f.mon.violations); fp != "" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("rolledBackOverAReadOnlySource", func(t *testing.T) {
		f := newMonFixture(t)
		promoting(f)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		if fp := fingerprint(f.mon.violations); fp != "OutcomeDishonest(rolledBackFencedSource)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	// A run ended by a status stall reads nothing. Its RolledBack says
	// that the process which granted the claim sent no fence, and is
	// judged by that.
	stalled := func(f *monFixture, reason v1alpha1.PlannedFailoverReason) {
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, Reason: reason}
		}, nil)
	}
	t.Run("stalledBeforeTheFenceOverASourceItsSidecarFenced", func(t *testing.T) {
		// The sweep's history: the fence is never sent; the source restarts
		// and its sidecar fences it; no pass reads it writable; status
		// writes stall and the run ends "the source is untouched".
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.c.NewChecker("alpha").SetSuperReadOnly(WithActor(context.Background(), ActorSidecar), true)
		stalled(f, v1alpha1.PlannedFailoverReasonStatusStalled)
		if fp := fingerprint(f.mon.violations); fp != "" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
		if f.mon.stats.StalledBeforeFence != 1 {
			t.Fatalf("StalledBeforeFence %d, want 1", f.mon.stats.StalledBeforeFence)
		}
	})
	t.Run("stalledAfterTheRunsFenceLanded", func(t *testing.T) {
		// The run fenced the source: "untouched" is false, whatever the
		// reason says.
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		stalled(f, v1alpha1.PlannedFailoverReasonStatusStalled)
		if fp := fingerprint(f.mon.violations); fp != "OutcomeDishonest(rolledBackFencedSource)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
	t.Run("notStalledOverASourceItsSidecarFenced", func(t *testing.T) {
		// Any other RolledBack claims a source the run read writable.
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.c.NewChecker("alpha").SetSuperReadOnly(WithActor(context.Background(), ActorSidecar), true)
		stalled(f, v1alpha1.PlannedFailoverReasonAuthorityChanged)
		if fp := fingerprint(f.mon.violations); fp != "OutcomeDishonest(rolledBackFencedSource)" {
			t.Fatalf("fingerprint %s: %v", fp, f.mon.violations)
		}
	})
}
