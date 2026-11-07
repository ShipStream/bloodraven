package controller

import (
	"context"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// A pod stall reads as Updating=False/OrderedUpdateStalled with the cause,
// and fires one Warning Event per new stall: not on every retry that finds
// the same cause, and again only after an attempt got past it.
func TestUpdateCRStatus_ReportsOrderedUpdateStall(t *testing.T) {
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	rec := record.NewFakeRecorder(10)
	runner := &TopologyManagerRunner{client: c, recorder: rec, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	base := TopologySnapshot{
		Sites:      []SiteSnapshot{{Name: "dc1", State: state.StateWritable}, {Name: "dc2", State: state.StateReadOnly}},
		ActiveSite: "dc1",
	}
	stalled := base
	stalled.UpdateStalled = "site dc2 was updated but its pod mysql-lion-dc2-1 is not Ready (" + sidecarCrashLoop + ")"
	running := base
	running.UpdatePhase = v1alpha1.UpdatePhaseWaitReplica

	write := func(snap TopologySnapshot) (*metav1.Condition, []string) {
		t.Helper()
		if err := runner.updateCRStatus(context.Background(), nn, snap); err != nil {
			t.Fatalf("updateCRStatus() error = %v", err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), nn, &got); err != nil {
			t.Fatal(err)
		}
		return apimeta.FindStatusCondition(got.Status.Conditions, "Updating"), drainRunnerEvents(rec)
	}

	cond, events := write(stalled)
	wantMsg := "Ordered update stalled: " + stalled.UpdateStalled +
		". No other site is rolled and the active site is not handed off until that pod has been Ready for 30s; the update retries every 30s"
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != v1alpha1.ReasonOrderedUpdateStalled || cond.Message != wantMsg {
		t.Fatalf("Updating = %+v, want False/%s %q", cond, v1alpha1.ReasonOrderedUpdateStalled, wantMsg)
	}
	if len(events) != 1 || !strings.HasPrefix(events[0], "Warning OrderedUpdateStalled "+wantMsg) {
		t.Fatalf("events = %q, want one OrderedUpdateStalled Warning", events)
	}

	if _, events = write(stalled); len(events) != 0 {
		t.Fatalf("a retry with the same cause fired %q", events)
	}

	cond, events = write(running)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != string(v1alpha1.UpdatePhaseWaitReplica) || len(events) != 0 {
		t.Fatalf("Updating = %+v events %q while an attempt runs", cond, events)
	}
	if _, events = write(stalled); len(events) != 1 {
		t.Fatalf("events = %q, want the stall reported again after an attempt", events)
	}

	cond, events = write(base)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "NotUpdating" || len(events) != 0 {
		t.Fatalf("Updating = %+v events %q after the stall cleared", cond, events)
	}
}
