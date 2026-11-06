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
	"github.com/shipstream/bloodraven/internal/mysql"
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
	pf := v1alpha1.PlannedFailoverStatus{RunID: "r1", Revision: f.rev, Phase: phase, TargetSite: "beta", SourceSite: "alpha",
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

// passGate records a pre-write gate for run r1 that found no foreign
// writable site, so a fixture judging another invariant can promote its
// target without PlannedPromoteWithoutGate or PlannedPromoteBesideWritable
// (the gate's own invariants have their own fixtures).
func (f *monFixture) passGate() {
	f.t.Helper()
	r := f.mon.runs["r1"]
	if r == nil {
		f.t.Fatal("passGate: no run r1")
	}
	r.gateWritable, r.gateJudged = []string{}, false
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
	setHoldRecord(st, "r1", "alpha", "beta")
}

// setHoldRecord writes status.plannedFailoverHold for runID and its True
// AutomaticFailoverHeld condition, as the controller's terminal write does.
func setHoldRecord(st *v1alpha1.MysqlFailoverGroupStatus, runID string, sites ...string) {
	st.PlannedFailoverHold = &v1alpha1.PlannedFailoverHold{RunID: runID, Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		AmbiguousSites: sites, HoldTime: metav1.Now()}
	apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionAutomaticFailoverHeld, Status: metav1.ConditionTrue,
		Reason: v1alpha1.ReasonPlannedFailoverManualRecoveryRequired, Message: "held",
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
	// K078: the same-process memo attributes the fence only to the server
	// it landed on; a replacement under a fresh server_uuid is not it.
	t.Run("sameProcessReplacedSource", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.c.mu.Lock()
		f.c.byName["alpha"].serverUUID = "ffffffff-0000-0000-0000-000000000001"
		f.c.mu.Unlock()
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
	// §9 step 1: a writable peer the operator cannot read does not block
	// the rollback's unfence; it is counted, not flagged.
	t.Run("besideUnreadableWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseRollingBack, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, true)
		f.c.RogueWritable("beta")
		f.c.SetOperatorLink("beta", true)
		f.mon.onYield(YieldPeerProbe)
		_ = f.pf("alpha").SetSuperReadOnly(ctx, false)
		f.want()
		if f.mon.stats.UnfenceBesideUnreadable != 1 {
			t.Fatalf("UnfenceBesideUnreadable %d, want 1", f.mon.stats.UnfenceBesideUnreadable)
		}
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
		f.passGate()
		_ = b.SetSuperReadOnly(ctx, false)
		_ = b.SetReadOnly(ctx, false)
		f.want()
	})
	t.Run("stopAfterWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		b := f.pf("beta")
		f.passGate()
		_ = b.SetReadOnly(ctx, false)
		_ = b.StopReplica(ctx)
		f.want("RepeatedPromotion")
	})
	t.Run("killAfterWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		b := f.pf("beta")
		f.passGate()
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
	_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true) // the run's fence: the marker below is honest
	base, _ := f.store.Load(context.Background(), f.store.nn)
	_, _ = f.store.WriteTerminalMarker(context.Background(), base, controller.TerminalMarker{
		RunID: "r1", Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled})
	_, _ = f.pf("alpha").KillAppConnections(pfCtx())
	f.want("MarkedRunResumed")
}

// TestPlannedInvariant_MarkerJudgedAtCommit: a run ended by its terminal
// marker is judged against truth when the marker commits (K039), not when
// status writes heal and the result is copied into status.
func TestPlannedInvariant_MarkerJudgedAtCommit(t *testing.T) {
	t.Run("honestMarkerLaterHealed", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, nil, nil)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true)
		base, _ := f.store.Load(context.Background(), f.store.nn)
		_, _ = f.store.WriteTerminalMarker(context.Background(), base, controller.TerminalMarker{
			RunID: "r1", Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled})
		// The topology manager's re-assert heals the source before status
		// writes recover; the copied result must not be judged against it.
		_ = f.pf("alpha").SetSuperReadOnly(context.Background(), false)
		_ = f.pf("alpha").SetReadOnly(context.Background(), false)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		f.want()
	})
	t.Run("dishonestMarker", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, nil, nil)
		base, _ := f.store.Load(context.Background(), f.store.nn)
		_, _ = f.store.WriteTerminalMarker(context.Background(), base, controller.TerminalMarker{
			RunID: "r1", Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled})
		f.want("OutcomeDishonest")
	})
}

// TestPlannedInvariant_MutationWithoutRun: a landed pf mutation with no
// recorded run, or after its run's terminal record, is reported (K040).
func TestPlannedInvariant_MutationWithoutRun(t *testing.T) {
	t.Run("noRun", func(t *testing.T) {
		f := newMonFixture(t)
		_ = f.pf("beta").StopReplica(pfCtx())
		f.want("PlannedMutationWithoutRun")
	})
	t.Run("afterTerminal", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseWaitingForLag, nil, nil)
		f.terminal(v1alpha1.PlannedFailoverOutcomeNotStarted)
		_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), false)
		f.want("PlannedMutationWithoutRun")
	})
	t.Run("liveRunQuiet", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, nil)
		_ = f.pf("beta").StopReplica(pfCtx())
		f.want()
	})
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
	// K060: an ordered update is automation the hold excludes too.
	t.Run("updateUnderHold", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseFailed, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
		}, setHold)
		_ = f.pf("beta").SetSuperReadOnly(WithActor(tmCtx, ActorUpdate), false)
		names := f.names()
		if len(names) == 0 || names[len(names)-1] != "HoldNotHonoured" {
			t.Fatalf("violations %v, want HoldNotHonoured last", f.mon.violations)
		}
	})
	// K060: a new run never acts while another run's hold stands.
	t.Run("newRunUnderAnotherRunsHold", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, func(st *v1alpha1.MysqlFailoverGroupStatus) {
			setHoldRecord(st, "r0", "alpha", "beta")
		})
		_ = f.pf("beta").StopReplica(pfCtx())
		f.want("HoldNotHonoured")
	})
	t.Run("ownHoldQuiet", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, setHold)
		_ = f.pf("beta").StopReplica(pfCtx())
		f.want()
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

// TestPlannedInvariant_GateWindow: a target unfence needs a pre-write gate
// in the same pass (K180); the SRO/RO pair shares one gate; the gate window
// closes at the end of the pass and of the run (K179).
func TestPlannedInvariant_GateWindow(t *testing.T) {
	ctx := pfCtx()
	t.Run("noGate", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.c.RogueFence("alpha")
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		f.want("PlannedPromoteWithoutGate")
	})
	t.Run("pairSharesTheGate", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.c.RogueFence("alpha")
		f.mon.onYield(YieldGateProbe)
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		_ = f.pf("beta").SetReadOnly(ctx, false)
		f.want()
	})
	t.Run("gateClosesAtPassEnd", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.c.RogueFence("alpha")
		f.mon.onYield(YieldGateProbe)
		f.mon.noteEnginePass()
		_ = f.pf("beta").SetSuperReadOnly(ctx, false)
		f.want("PlannedPromoteWithoutGate")
	})
	t.Run("refusedGateClosesAtRunEnd", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.mon.onYield(YieldGateProbe) // a gate that refuses: nothing is sent
		f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
		f.c.RogueWritable("beta")
		if f.mon.stats.RogueRaceResidual != 0 {
			t.Fatalf("a rogue after the run ended counted as its gate residual (%d)", f.mon.stats.RogueRaceResidual)
		}
	})
}

// TestPlannedInvariant_ProbeFreshnessPerRun: only a run's own peer probe
// makes a later rogue writability the R2 residual; an earlier run's probe
// does not exempt a later run's fence (K183).
func TestPlannedInvariant_ProbeFreshnessPerRun(t *testing.T) {
	f := newMonFixture(t)
	f.write(v1alpha1.PlannedFailoverPhaseFencing, nil, nil)
	f.mon.onYield(YieldPeerProbe) // r1's probe
	f.terminal(v1alpha1.PlannedFailoverOutcomeNotStarted)
	f.c.RogueWritable("beta")
	f.write(v1alpha1.PlannedFailoverPhaseFencing, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.RunID, pf.Result = "r2", nil
	}, nil)
	_ = f.pf("alpha").SetSuperReadOnly(pfCtx(), true) // r2 fences with no probe of its own
	f.want("PlannedFenceWithoutAuthority")
}

// TestPlannedInvariant_ReplacedTargetRollbackDue: the DST-3 roll-back
// prediction needs positive evidence from every site (K178): it holds with
// every site reachable, and not with a peer or the source cut off.
func TestPlannedInvariant_ReplacedTargetRollbackDue(t *testing.T) {
	setup := func() (*monFixture, mysql.GTIDSet) {
		f := newMonFixture(t)
		fence := f.fenceSet()
		f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) {
			pf.Fence.GtidSet = &fence
		}, nil)
		f.c.RogueFence("alpha")
		F, err := mysql.ParseGTIDSet(fence)
		if err != nil {
			t.Fatal(err)
		}
		return f, F
	}
	f, F := setup()
	if !f.mon.replacedTargetRollbackDue(f.mon.runs["r1"], f.c.Truth(), F) {
		t.Fatal("every site reachable and read-only, the source fenced with F: roll back is due")
	}
	for _, site := range []string{"beta", "alpha"} {
		f, F := setup()
		f.c.SetOperatorLink(site, true)
		if f.mon.replacedTargetRollbackDue(f.mon.runs["r1"], f.c.Truth(), F) {
			t.Fatalf("%s unreachable: a hold is right, roll back must not be predicted", site)
		}
	}
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
		f.passGate()
		_ = b.SetReadOnly(ctx, false)
		f.mon.onYield(YieldConfirmProbe)
		f.terminal(v1alpha1.PlannedFailoverOutcomeSucceeded)
		f.want()
	})
	t.Run("succeededBesideWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.passGate()
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
		f.passGate()
		_ = f.pf("beta").SetReadOnly(pfCtx(), false) // a promotion statement, so the outcome is honest
		f.c.RogueFence("alpha")
		hold(f)
		f.want("HoldWithoutDataAtRisk")
	})
	t.Run("twoWritable", func(t *testing.T) {
		f := newMonFixture(t)
		f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
		f.passGate()
		_ = f.pf("beta").SetReadOnly(pfCtx(), false)
		hold(f)
		f.want()
	})
}

// TestPlannedInvariant_WatermarkNeverWritten: a Publishing record whose
// status copy carries the run's promotion but never its watermark is
// flagged at once, not only after a containing value was seen (K038).
func TestPlannedInvariant_WatermarkNeverWritten(t *testing.T) {
	f := newMonFixture(t)
	fence := f.fenceSet()
	confirmed := metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 5, 0, time.UTC))
	f.write(v1alpha1.PlannedFailoverPhasePublishing, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.Fence.GtidSet = &fence
		pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: "Confirmed", ConfirmationTime: &confirmed}
	}, func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.LastFailover = &confirmed // the record, without promotionGtidExecuted
	})
	f.want("WatermarkRegressed")
}

func TestPlannedInvariant_WatermarkRegressed(t *testing.T) {
	f := newMonFixture(t)
	fence := f.fenceSet()
	confirmed := metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 5, 0, time.UTC))
	f.write(v1alpha1.PlannedFailoverPhasePublishing, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.Fence.GtidSet = &fence
		pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: "Confirmed", ConfirmationTime: &confirmed}
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
		pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: "Confirmed", ConfirmationTime: &confirmed}
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
	f.passGate()
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
	// K177: an approved reason from an actor other than the engine.
	t.Run("revokedByAnotherActor", func(t *testing.T) {
		f := newMonFixture(t)
		f.mon.claim = &fakeClaim{releases: []claimRelease{{RunID: "r1", Why: controller.ReleaseTerminal, By: "tm"}}}
		f.mon.endPoll(1, true)
		f.want("ClaimRevoked")
	})
	t.Run("engineReleaseQuiet", func(t *testing.T) {
		f := newMonFixture(t)
		f.mon.claim = &fakeClaim{releases: []claimRelease{{RunID: "r1", Why: controller.ReleaseTerminal, By: string(ActorPF)}}}
		f.mon.endPoll(1, true)
		f.want()
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
		f.passGate()
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

// TestPlannedInvariant_MarkerHoldJudged: a ManualRecoveryRequired terminal
// marker is a hold on its own, so its justification is judged when it
// commits, without waiting for the status condition (K077).
func TestPlannedInvariant_MarkerHoldJudged(t *testing.T) {
	f := newMonFixture(t)
	fence := f.fenceSet()
	f.write(v1alpha1.PlannedFailoverPhasePromoting, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.Fence.GtidSet = &fence
	}, nil)
	_ = f.pf("beta").StopReplica(pfCtx()) // a promotion statement: the outcome is honest
	base, _ := f.store.Load(context.Background(), f.store.nn)
	_, _ = f.store.WriteTerminalMarker(context.Background(), base, controller.TerminalMarker{
		RunID: "r1", Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, Reason: v1alpha1.PlannedFailoverReasonStatusStalled})
	f.want("HoldWithoutDataAtRisk")
}

// TestOwnsSites_Domain pins the DST's ownership predicate over every phase,
// marked or not, with and without a runID (K181): the owning phases of an
// unmarked, named run own the sites; nothing else does. It runs through the
// controller's predicate, so this also pins that predicate's domain.
func TestOwnsSites_Domain(t *testing.T) {
	owning := map[v1alpha1.PlannedFailoverPhase]bool{
		v1alpha1.PlannedFailoverPhaseFencing: true, v1alpha1.PlannedFailoverPhaseDraining: true,
		v1alpha1.PlannedFailoverPhaseWaitingForLag: true, v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync: true,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly: true, v1alpha1.PlannedFailoverPhasePromoting: true,
		v1alpha1.PlannedFailoverPhaseRollingBack: true,
	}
	phases := []v1alpha1.PlannedFailoverPhase{
		v1alpha1.PlannedFailoverPhaseAdmitting, v1alpha1.PlannedFailoverPhaseDeferred, v1alpha1.PlannedFailoverPhaseFencing,
		v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync, v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting, v1alpha1.PlannedFailoverPhaseRollingBack,
		v1alpha1.PlannedFailoverPhasePublishing, v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverPhaseFailed,
	}
	for _, phase := range phases {
		for _, runID := range []string{"", "r1"} {
			for _, marked := range []bool{false, true} {
				obj := &v1alpha1.MysqlFailoverGroup{}
				obj.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: runID, Phase: phase}
				if marked {
					obj.SetAnnotations(map[string]string{controller.PlannedFailoverTerminalAnnotation: controller.TerminalMarker{
						RunID: runID, Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced}.String()})
				}
				want := owning[phase] && runID != "" && !marked
				if got := ownsSites(obj); got != want {
					t.Errorf("phase=%s runID=%q marked=%v: ownsSites=%v, want %v", phase, runID, marked, got, want)
				}
			}
		}
	}
	if ownsSites(nil) || ownsSites(&v1alpha1.MysqlFailoverGroup{}) {
		t.Error("an object without a block owns the sites")
	}
}

// TestPlannedInvariant_UnconsumedRequestIsStuck: a request annotation still
// on the object at the end never produced a run, and is reported (K182).
func TestPlannedInvariant_UnconsumedRequestIsStuck(t *testing.T) {
	f := newMonFixture(t)
	f.store.SetRequest("beta")
	f.mon.checkEnd(100)
	f.want("PlannedRunStuck")

	g := newMonFixture(t)
	g.mon.checkEnd(100)
	g.want()
}

// TestPromotions_ResetScopedToItsAttempt: a RESET REPLICA ALL from a run
// that ended without promoting is not paired with a later run's unfence of
// the same site (K175); a run's own RESET and read_only=OFF are.
func TestPromotions_ResetScopedToItsAttempt(t *testing.T) {
	f := newMonFixture(t)
	pl := &plannedHarness{mon: f.mon, pendingReset: map[string]resetOwner{}}
	reset := Event{Site: "beta", Kind: EvResetReplica, Actor: ActorPF}
	roOff := Event{Site: "beta", Kind: EvSetRO, Detail: "on=false", Actor: ActorPF}

	f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
	if got := pl.promotions([]Event{reset}); len(got) != 0 {
		t.Fatalf("a RESET alone is a promotion: %v", got)
	}
	f.terminal(v1alpha1.PlannedFailoverOutcomeRolledBack)
	pl.promotions(nil) // the poll in which r1's end is seen
	f.write(v1alpha1.PlannedFailoverPhaseRollingBack, func(pf *v1alpha1.PlannedFailoverStatus) {
		pf.RunID, pf.Result = "r2", nil
	}, nil)
	if got := pl.promotions([]Event{roOff}); len(got) != 0 {
		t.Fatalf("r2's read_only=OFF was paired with r1's RESET: %v", got)
	}

	f.write(v1alpha1.PlannedFailoverPhasePromoting, nil, nil)
	if got := pl.promotions([]Event{reset, roOff}); len(got) != 1 || got[0].site != "beta" {
		t.Fatalf("a run's own RESET and read_only=OFF: promotions %v, want one on beta", got)
	}
}

// TestPlannedInvariant_StatusRecordRegressed: no write moves the status
// failover record to an older failover (K042).
func TestPlannedInvariant_StatusRecordRegressed(t *testing.T) {
	f := newMonFixture(t)
	newer := metav1.NewTime(time.Date(2030, 1, 1, 0, 1, 0, 0, time.UTC))
	older := metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, func(st *v1alpha1.MysqlFailoverGroupStatus) { st.LastFailover = &newer })
	f.want()
	f.write(v1alpha1.PlannedFailoverPhaseDraining, nil, func(st *v1alpha1.MysqlFailoverGroupStatus) { st.LastFailover = &older })
	f.want("StatusRecordRegressed")
}
