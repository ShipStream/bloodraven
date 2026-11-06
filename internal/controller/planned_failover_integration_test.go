package controller

import (
	"context"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

// The engine/facade seams the integration commit closed.

// A claim startManager hydrated for a run this process never saw live (a
// fresh process, whose block was then cleared or ended elsewhere) is a
// zombie: the engine releases it from the slot's own run.
func TestEngine_ZombieClearReleasesHydratedClaimNeverSeen(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block *v1alpha1.PlannedFailoverStatus
	}{
		{"terminal block", &v1alpha1.PlannedFailoverStatus{RunID: "run-hydrated", Revision: 5, Phase: v1alpha1.PlannedFailoverPhaseFailed, TargetSite: "pdx",
			Result: &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, CompletionTime: metav1.Now()}}},
		{"absent block", nil},
		{"another run's block", &v1alpha1.PlannedFailoverStatus{RunID: "run-other", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseFailed, TargetSite: "pdx",
			Result: &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, CompletionTime: metav1.Now()}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPFEngineHarness(t, plannedFailoverFG(""))
			h.setBlock(tc.block)
			h.topo.setHeld("run-hydrated", false)
			h.step()
			if run, _ := h.topo.held(); run != "" {
				t.Fatalf("the hydrated zombie claim is still held by %q", run)
			}
			if got := h.topo.releasesCopy(); !slices.Contains(got, "run-hydrated:"+string(ReleaseZombie)) {
				t.Fatalf("releases = %v, want run-hydrated released as a zombie", got)
			}
		})
	}
	t.Run("a hold is never a zombie", func(t *testing.T) {
		fg := plannedFailoverFG("")
		setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-held",
			Reason: v1alpha1.PlannedFailoverReasonForeignWritable, HoldTime: metav1.Now()}, 1)
		h := newPFEngineHarness(t, fg)
		h.topo.setHeld("run-held", true)
		h.step()
		if run, hold := h.topo.held(); run != "run-held" || !hold {
			t.Fatal("a durably recorded hold was cleared")
		}
	})
	// K001/K002/K004: a hold with no durable record left (and no ack to
	// finish) is an orphan, released as such rather than as a zombie.
	t.Run("an unrecorded hold is released as an orphan", func(t *testing.T) {
		h := newPFEngineHarness(t, plannedFailoverFG(""))
		h.topo.setHeld("run-held", true)
		h.step()
		if run, _ := h.topo.held(); run != "" {
			t.Fatalf("the orphaned hold of %q is still held", run)
		}
		if got := h.topo.releasesCopy(); !slices.Equal(got, []string{"run-held:" + string(ReleaseOrphanHold)}) {
			t.Fatalf("releases = %v, want only run-held:%s", got, ReleaseOrphanHold)
		}
	})
}

// The terminal Succeeded write takes status.activeSite and
// topologyGeneration from the current manager beside the failover record,
// with the snapshot merge's rule (§5.3, §12).
func TestEngine_SucceededWriteMergesManagerAuthority(t *testing.T) {
	e := NewPlannedFailoverEngine(nil, &pfFakeTopology{authority: &PlannedAuthority{ActiveSite: "", DeploymentActiveSite: "pdx", TopologyGeneration: 8}},
		&pfFakeSink{}, clock.NewFakeClock(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)), SimHooks{})
	confirmed := metav1.NewTime(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC))
	gtid := "uuid-iad:1-10"
	next := &v1alpha1.PlannedFailoverStatus{RunID: "run-1", TargetSite: "pdx",
		Fence:     &v1alpha1.PlannedFailoverFenceRecord{GtidSet: &gtid},
		Promotion: &v1alpha1.PlannedFailoverPromotionRecord{ConfirmationTime: &confirmed},
		Result:    &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeSucceeded}}
	st := &v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "iad", TopologyGeneration: 7}
	e.terminalMutate(&v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "fg"}}, next, nil)(st)
	if st.ActiveSite != "pdx" || st.TopologyGeneration != 8 || st.LastFailoverTarget != "pdx" || st.PromotionGtidExecuted != gtid {
		t.Fatalf("status = activeSite %q gen %d target %q watermark %q; want pdx/8/pdx/F",
			st.ActiveSite, st.TopologyGeneration, st.LastFailoverTarget, st.PromotionGtidExecuted)
	}
	stale := &v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "pdx", TopologyGeneration: 9}
	if MergePlannedAuthority(stale, PlannedAuthority{ActiveSite: "iad", TopologyGeneration: 8}) || stale.ActiveSite != "pdx" {
		t.Fatal("an older manager generation moved status.activeSite")
	}
}

// End to end over the real facade, slot and topology manager: a request
// is admitted, claimed, fenced, drained, waits for lag, promotes, records,
// publishes and ends Succeeded with the claim released and status naming
// the target.
func TestPlannedFailover_EngineOverRealFacadeSucceeds(t *testing.T) {
	f := newOpsFixture(t, "")
	fg := newTestFG()
	fg.Annotations = map[string]string{PlannedFailoverAnnotation: "dc2"}
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: "writable"}, {Name: "dc2", State: "read-only", Replicating: true}}
	faults := &pfFaults{}
	c := newPFFakeClient(faults, fg)
	clk := clock.NewFakeClock(f.clk.Now())
	e := NewPlannedFailoverEngine(newAPIPlannedFailoverStore(c, c, clk), f.topo, &pfFakeSink{}, clk, SimHooks{})
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	var last *v1alpha1.PlannedFailoverStatus
	for i := 0; i < 60; i++ {
		d, err := e.Step(context.Background(), nn)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		var cur v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), nn, &cur); err != nil {
			t.Fatal(err)
		}
		last = cur.Status.PlannedFailover
		if last != nil && last.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
			if cur.Status.ActiveSite != "dc2" || cur.Status.LastFailoverTarget != "dc2" || cur.Status.PromotionGtidExecuted != opsF {
				t.Fatalf("status after Succeeded = activeSite %q lastFailoverTarget %q watermark %q",
					cur.Status.ActiveSite, cur.Status.LastFailoverTarget, cur.Status.PromotionGtidExecuted)
			}
			if f.reg.slot.Held() {
				t.Fatal("the claim is still held after Succeeded")
			}
			if got := f.tgt.statements(); !slices.Equal(got, []string{"stop", "reset", "sro=OFF", "ro=OFF"}) {
				t.Fatalf("target statements = %v", got)
			}
			return
		}
		if d <= 0 {
			d = time.Second
		}
		clk.Advance(d)
		f.clk.Advance(d)
	}
	t.Fatalf("not Succeeded after 60 steps; last block = %+v", last)
}
