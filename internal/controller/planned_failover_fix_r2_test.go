package controller

import (
	"context"
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// setStatus applies mut to the stored group's status.
func (h *pfEngineHarness) setStatus(mut func(*v1alpha1.MysqlFailoverGroupStatus)) {
	h.t.Helper()
	fg := h.fg()
	mut(&fg.Status)
	if err := h.c.Status().Update(context.Background(), fg); err != nil {
		h.t.Fatalf("set status: %v", err)
	}
}

// midEmergencyFailover is the status an emergency failover promoting pdx
// publishes before activeSite (scenario 64): no active site, the record
// naming pdx, pdx read-only and no longer replicating.
func midEmergencyFailover(at time.Time) func(*v1alpha1.MysqlFailoverGroupStatus) {
	return func(st *v1alpha1.MysqlFailoverGroupStatus) {
		rec := metav1.NewTime(at)
		st.ActiveSite = ""
		st.LastFailover, st.LastFailoverTarget = &rec, "pdx"
		st.Sites = []v1alpha1.SiteStatus{
			{Name: "iad", State: "unreachable"},
			{Name: "pdx", State: "read-only", Replicating: false},
		}
	}
}

func publishedPDX(st *v1alpha1.MysqlFailoverGroupStatus) {
	st.ActiveSite = "pdx"
	st.Sites = []v1alpha1.SiteStatus{
		{Name: "iad", State: "unreachable"},
		{Name: "pdx", State: "writable"},
	}
}

// O-M1 / G-M2 (scenario 64): a pending request meeting an emergency
// failover of its own target waits while authority is unsettled, instead of
// ending TargetUnhealthy, and ends AlreadyActive once the target is
// published.
func TestFixR2_AdmissionWaitsWhileAuthorityUnsettled(t *testing.T) {
	assertWaiting := func(t *testing.T, h *pfEngineHarness, runID string) {
		t.Helper()
		pf := h.pf()
		if pf == nil || pf.Result != nil || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission == nil ||
			pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitAuthorityUnsettled || pf.Admission.DeadlineTime == nil {
			t.Fatalf("block = %+v (admission %+v), want Admitting{AuthorityUnsettled} with a deadline", pf, pf.Admission)
		}
		if runID != "" && pf.RunID != runID {
			t.Fatalf("run = %q, want %q", pf.RunID, runID)
		}
		if h.topo.count("claim") != 0 {
			t.Error("claim requested while authority is unsettled")
		}
	}
	assertAlreadyActive := func(t *testing.T, h *pfEngineHarness) {
		t.Helper()
		pf := h.pf()
		if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeNotStarted ||
			pf.Result.Reason != v1alpha1.PlannedFailoverReasonAlreadyActive {
			t.Fatalf("block = %+v, want NotStarted/AlreadyActive", pf)
		}
	}

	t.Run("deferred", func(t *testing.T) {
		h := newPFEngineHarness(t, plannedFailoverFG("pdx"))
		pf := pfRunAt(v1alpha1.PlannedFailoverPhaseDeferred, h.clk.Now())
		pf.Admission = &v1alpha1.PlannedFailoverAdmission{DeferReason: v1alpha1.PlannedFailoverDeferDeploymentHold}
		h.setBlock(pf)
		h.clk.Advance(10 * time.Second)
		h.setStatus(midEmergencyFailover(h.clk.Now()))
		h.step()
		assertWaiting(t, h, "run-1")
		if got := h.pf().Admission.DeadlineTime.Time; !got.Equal(h.clk.Now().Add(plannedFailoverAdmissionTimeout)) {
			t.Errorf("deadline = %v, want now+%s", got, plannedFailoverAdmissionTimeout)
		}
		h.clk.Advance(2 * time.Second)
		h.step()
		assertWaiting(t, h, "run-1")
		h.setStatus(publishedPDX)
		h.step()
		assertAlreadyActive(t, h)
	})

	t.Run("admitting", func(t *testing.T) {
		h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
		h.clk.Advance(10 * time.Second)
		h.setStatus(midEmergencyFailover(h.clk.Now()))
		h.step()
		assertWaiting(t, h, "run-1")
		h.setStatus(publishedPDX)
		h.step()
		assertAlreadyActive(t, h)
	})

	t.Run("new request", func(t *testing.T) {
		h := newPFEngineHarness(t, plannedFailoverFG("pdx"))
		h.setStatus(midEmergencyFailover(h.clk.Now()))
		h.step()
		assertWaiting(t, h, "")
		if _, ok := h.fg().Annotations[PlannedFailoverAnnotation]; ok {
			t.Error("request annotation kept after Admitting committed")
		}
		h.setStatus(publishedPDX)
		h.step()
		assertAlreadyActive(t, h)
	})

	// The record is published but activeSite still names the old primary.
	t.Run("record newer than the run", func(t *testing.T) {
		h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
		h.clk.Advance(10 * time.Second)
		h.setStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
			midEmergencyFailover(h.clk.Now())(st)
			st.ActiveSite = "iad"
		})
		h.step()
		assertWaiting(t, h, "run-1")
	})

	// The wait is bounded by the admission deadline.
	t.Run("deadline", func(t *testing.T) {
		h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
		h.setStatus(midEmergencyFailover(h.clk.Now().Add(time.Second)))
		h.step()
		assertWaiting(t, h, "run-1")
		h.clk.Advance(plannedFailoverAdmissionTimeout)
		h.step()
		if pf := h.pf(); pf.Result == nil || pf.Result.Reason != v1alpha1.PlannedFailoverReasonAdmissionTimeout {
			t.Fatalf("block = %+v, want NotStarted/AdmissionTimeout", pf)
		}
	})

	// A static refusal does not wait.
	t.Run("unknown site still rejected", func(t *testing.T) {
		h := newPFEngineHarness(t, plannedFailoverFG("nowhere"))
		h.setStatus(midEmergencyFailover(h.clk.Now()))
		h.step()
		if pf := h.pf(); pf == nil || pf.Result == nil || pf.Result.Reason != v1alpha1.PlannedFailoverReasonUnknownSite {
			t.Fatalf("block = %+v, want NotStarted/UnknownSite", pf)
		}
	})

	// An old record naming another site does not hold a new run.
	t.Run("record older than the run", func(t *testing.T) {
		h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
		h.setStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
			rec := metav1.NewTime(h.clk.Now().Add(-time.Hour))
			st.LastFailover, st.LastFailoverTarget = &rec, "pdx"
		})
		h.step()
		if pf := h.pf(); pf.Admission != nil && pf.Admission.WaitReason == v1alpha1.PlannedFailoverWaitAuthorityUnsettled {
			t.Fatalf("block = %+v: an hour-old record held admission", pf)
		}
	})
}

// O-m1(a): once the confirmed tail released the claim, a Dragonfly emergency
// follow-up runs although status still reads Promoting (the Publishing write
// stalled): with the runner's slot wired, the slot alone decides. A held
// slot still blocks it, and without a wired slot the durable block does.
func TestFixR2_DragonflyFollowUpRunsAfterRelease(t *testing.T) {
	newMgr := func(t *testing.T, slot *PlannedClaimSlot) (*DragonflyManager, *noOneRecorder) {
		t.Helper()
		fg := fgWithDragonflyEnabledAndActive()
		now := metav1.NewTime(time.Now().Add(-time.Minute)) // the run was claimed before the request
		f := "uuid-dc1:1-10"
		fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 7, Phase: v1alpha1.PlannedFailoverPhasePromoting,
			SourceSite: "dc1", TargetSite: "dc2", StartTime: now, ClaimTime: &now,
			Fence:     &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceApplied, IntentTime: now, GtidSet: &f},
			Promotion: &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionIntended, IntentTime: now}}
		cb, key := newDragonflyFakeClient(fg)
		mgr := NewDragonflyManager(cb.Build(), record.NewFakeRecorder(32), slog.Default(), key, 50*time.Millisecond)
		shrinkEmergencyWaits(mgr)
		if slot != nil {
			mgr.SetPlannedOwner(slot.Held)
		}
		target := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
		conns := &emergencyConnector{}
		conns.queue(emergencyTargetAddr, target)
		mgr.SetConnector(conns.connect)
		return mgr, target
	}

	t.Run("released slot", func(t *testing.T) {
		mgr, target := newMgr(t, &PlannedClaimSlot{})
		if ok := mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1"); !ok || !target.called {
			t.Fatalf("follow-up skipped (ok=%v, REPLICAOF NO ONE sent=%v) after the claim was released", ok, target.called)
		}
	})
	t.Run("held slot", func(t *testing.T) {
		slot := &PlannedClaimSlot{}
		slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"})
		mgr, target := newMgr(t, slot)
		if ok := mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1"); ok || target.called {
			t.Fatal("follow-up ran while the run holds the claim")
		}
	})
	t.Run("no runner", func(t *testing.T) {
		mgr, target := newMgr(t, nil)
		if ok := mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1"); ok || target.called {
			t.Fatal("follow-up ran beside an unrecorded Promoting block with no slot to ask")
		}
	})
}

// O-m1(a), reconcile side: once the group's manager is registered, the
// slot alone decides whether a planned failover owns the group's Dragonfly;
// before that the durable block does.
func TestFixR2_ReconcilerDragonflyOwnershipUsesSlot(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	now := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 7, Phase: v1alpha1.PlannedFailoverPhasePromoting,
		SourceSite: "dc1", TargetSite: "dc2", StartTime: now}
	nn := fgNN(fg)
	runner := NewTopologyManagerRunner(nil, nil, nil, record.NewFakeRecorder(8), slog.Default())
	r := &MysqlFailoverGroupReconciler{Runner: runner}
	if !r.plannedOwnsDragonfly(fg) {
		t.Fatal("before registration, an unrecorded Promoting block must own Dragonfly")
	}
	runner.SetManagerForTest(nn, &TopologyManager{})
	if r.plannedOwnsDragonfly(fg) {
		t.Fatal("with a registered manager and an empty slot, a released Promoting run owns Dragonfly")
	}
	runner.Slot(nn).hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"})
	if !r.plannedOwnsDragonfly(fg) {
		t.Fatal("a held slot must own Dragonfly")
	}
}
