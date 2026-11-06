package controller

import (
	"context"
	"maps"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

// Regression tests for interactions between the ultra-review fix groups
// (#235): each pins a behaviour that only the combination of two groups'
// changes got wrong.

// A request refused beside a standing hold whose removal did not commit is
// retried at the poll cadence, not the hold's 10-minute Warning cadence: a
// refused request left in place would start a switchover once the hold is
// acknowledged (the N1 class, on the hold path).
func TestMergeUltra_HeldRequestConsumptionIsRetried(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	h.setBlock(ultraAHeldTerminal(h.clk.Now()))
	fg := h.fg()
	setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		SourceSite: "iad", TargetSite: "pdx", HoldTime: metav1.NewTime(h.clk.Now())}, fg.Generation)
	if err := h.c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	h.setAnnotation(PlannedFailoverAnnotation, "pdx")
	h.faults.metaFaults = []pfWriteFault{pfWriteForbidden}
	d := h.step()
	if _, ok := h.fg().GetAnnotations()[PlannedFailoverAnnotation]; !ok {
		t.Fatal("setup: the request's removal was meant to fail")
	}
	if d <= 0 || d > plannedRequeuePoll {
		t.Fatalf("requeue = %s after a failed consumption; want at most %s", d, plannedRequeuePoll)
	}
	h.clk.Advance(d)
	h.step()
	if _, ok := h.fg().GetAnnotations()[PlannedFailoverAnnotation]; ok {
		t.Fatal("the refused request was not consumed on the retry")
	}
	if d := h.step(); d < time.Minute {
		t.Fatalf("requeue = %s with nothing left to consume; want the hold cadence", d)
	}
}

// K029: settlement counts a fence as proof only when it moved the object's
// resourceVersion. The API server skips a patch that changes nothing
// without moving it, so a nil error alone leaves the pending write's
// precondition intact.
func TestMergeUltra_K029_FenceMustMoveTheResourceVersion(t *testing.T) {
	fg := plannedFailoverFG("")
	fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, time.Now())
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			// Every patch "succeeds" as a server-side no-op: nothing is
			// applied and the object keeps its resourceVersion.
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
				var cur v1alpha1.MysqlFailoverGroup
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &cur); err != nil {
					return err
				}
				cur.DeepCopyInto(obj.(*v1alpha1.MysqlFailoverGroup))
				return nil
			},
		}).Build()
	nn := fgNN(fg)
	store := newAPIPlannedFailoverStore(c, c, clock.NewFakeClock(time.Now()))
	base, err := store.Load(context.Background(), nn)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := store.Settle(context.Background(), nn, base.ResourceVersion, "run-1", base.Status.PlannedFailover.Revision+1)
	if out != WriteUnknown {
		t.Fatalf("Settle = %s after a fence that moved nothing; want Unknown", out)
	}
}

// K029: two fences in one clock tick still change the annotation, so the
// second one is not a no-op the server would skip.
func TestMergeUltra_K029_FenceValueAlwaysAdvances(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	first := nextStatusFenceValue("", now)
	second := nextStatusFenceValue(first, now)
	if second == first {
		t.Fatalf("fence value did not advance within one tick: %q", first)
	}
	if got := nextStatusFenceValue(second, now.Add(time.Second)); got != FormatStatusFenceValue(now.Add(time.Second)) {
		t.Fatalf("fence value = %q once the clock moved; want the clock's instant", got)
	}
	// A client that, like the API server, skips a patch that changes
	// nothing without moving the resourceVersion.
	fg := plannedFailoverFG("")
	fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, now)
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				var cur v1alpha1.MysqlFailoverGroup
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &cur); err != nil {
					return err
				}
				if maps.Equal(cur.GetAnnotations(), obj.GetAnnotations()) {
					cur.DeepCopyInto(obj.(*v1alpha1.MysqlFailoverGroup))
					return nil
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).Build()
	nn := fgNN(fg)
	store := newAPIPlannedFailoverStore(c, c, clock.NewFakeClock(now))
	load := func() *v1alpha1.MysqlFailoverGroup {
		got, err := store.Load(context.Background(), nn)
		if err != nil || got == nil {
			t.Fatalf("load: %v %v", got, err)
		}
		return got
	}
	for i := 0; i < 2; i++ {
		base := load()
		if out, _ := store.Settle(context.Background(), nn, base.ResourceVersion, "run-1", base.Status.PlannedFailover.Revision+1); out != WriteNotCommitted {
			t.Fatalf("settle %d in one tick = %s, want NotCommitted", i, out)
		}
		if load().ResourceVersion == base.ResourceVersion {
			t.Fatalf("settle %d in one tick did not move the resourceVersion", i)
		}
	}
}

// K044: a run that ends with the source fenced by a fence that is not its
// own may leave every site read-only, where a new request only waits
// AuthorityUnsettled and ends NotStarted. Its message says to unfence a
// site by hand first, never to re-annotate as an alternative.
func TestMergeUltra_K044_NotAttributableAdvisesManualUnfence(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseFencing)
	h.topo.fence = func(PlannedRunRef) (FenceProof, error) {
		return FenceProof{Kind: FenceNotAttributable}, nil
	}
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	pf := h.pf()
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
		t.Fatalf("block = %+v, want SourceLeftFenced", pf)
	}
	if !strings.Contains(pf.Message, plannedManualUnfenceAdvice) || strings.Contains(pf.Message, "unfence the source or re-annotate") {
		t.Fatalf("message = %q; want the manual-unfence advice", pf.Message)
	}
}
