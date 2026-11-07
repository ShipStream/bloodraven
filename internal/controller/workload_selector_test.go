package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// drainEventReasons returns the reasons of the Events the fake recorder
// holds, emptying it.
func drainEventReasons(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			if f := strings.Fields(e); len(f) > 1 {
				out = append(out, f[1])
			}
		default:
			return out
		}
	}
}

// TestReconcile_RefusesDeploymentWithForeignSelector pins A-77's
// immutable-selector rule: a site Deployment whose selector differs from
// the rendered one (here, a pre-A-77 label key) is not updated, the group
// reports Degraded=True/WorkloadSelectorMismatch with one Warning Event
// naming the object, the reconcile returns no error (no retry loop), and a
// later reconcile neither writes the object nor repeats the Event. Deleting
// the object lets the operator re-create it and clears the refusal.
func TestReconcile_RefusesDeploymentWithForeignSelector(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	fg.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
	name := resourceName(fg.Name, "dc1")
	old := map[string]string{labelAppName: "mysql", labelInstance: fg.Name, "shipstream.io/site": "dc1"}
	foreign := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: fg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: old},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: old},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:9.7"}}},
			},
		},
	}
	r, c := newReconciler(fg, foreign)
	rec := record.NewFakeRecorder(64)
	r.Runner = NewTopologyManagerRunner(c, nil, nil, rec, testLogger())
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	key := types.NamespacedName{Name: name, Namespace: fg.Namespace}

	var before appsv1.Deployment
	if err := c.Get(ctx, key, &before); err != nil {
		t.Fatal(err)
	}
	for pass := range 2 {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("pass %d: reconcile returned %v; a refused workload is not retried", pass, err)
		}
		var got appsv1.Deployment
		if err := c.Get(ctx, key, &got); err != nil {
			t.Fatal(err)
		}
		if got.ResourceVersion != before.ResourceVersion {
			t.Fatalf("pass %d: the refused Deployment was written", pass)
		}
		events := drainEventReasons(rec)
		want := 0
		if pass == 0 {
			want = 1
		}
		n := 0
		for _, e := range events {
			if e == v1alpha1.EventWorkloadSelectorMismatch {
				n++
			}
		}
		if n != want {
			t.Fatalf("pass %d: %d WorkloadSelectorMismatch Events (%v), want %d", pass, n, events, want)
		}
	}
	// The other site's Deployment is reconciled as usual.
	var other appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: resourceName(fg.Name, "dc2"), Namespace: fg.Namespace}, &other); err != nil {
		t.Fatalf("dc2 Deployment not created: %v", err)
	}

	snap := TopologySnapshot{
		ActiveSite: "dc1",
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged},
		},
		DegradedReason: v1alpha1.ReasonHealthy,
	}
	degraded := func() *metav1.Condition {
		t.Helper()
		if err := r.Runner.updateCRStatus(ctx, nn, snap); err != nil {
			t.Fatal(err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		return apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDegraded)
	}
	if d := degraded(); d == nil || d.Status != metav1.ConditionTrue || d.Reason != v1alpha1.ReasonWorkloadSelectorMismatch ||
		!strings.Contains(d.Message, "Deployment "+name) || !strings.Contains(d.Message, "shipstream.io/site=dc1") {
		t.Fatalf("Degraded = %+v, want True/WorkloadSelectorMismatch naming %s", d, name)
	}

	if err := c.Delete(ctx, &before); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatal(err)
	}
	var recreated appsv1.Deployment
	if err := c.Get(ctx, key, &recreated); err != nil {
		t.Fatalf("Deployment not re-created: %v", err)
	}
	if recreated.Spec.Selector.MatchLabels[v1alpha1.SiteLabel] != "dc1" {
		t.Fatalf("re-created selector = %v", recreated.Spec.Selector)
	}
	if d := degraded(); d == nil || d.Reason == v1alpha1.ReasonWorkloadSelectorMismatch {
		t.Fatalf("Degraded = %+v after the object was re-created", d)
	}
}

// TestCheckWorkloadSelector: a live object mismatches only when it has a
// selector other than the rendered one, and the message says how to fix it.
func TestCheckWorkloadSelector(t *testing.T) {
	fg := newTestFG()
	want := mysqlDeploymentSelector(fg, "dc1")
	if checkWorkloadSelector("Deployment", "d", "dc1", nil, want) != nil {
		t.Error("an object without a selector (being created) mismatched")
	}
	if checkWorkloadSelector("Deployment", "d", "dc1", want.DeepCopy(), want) != nil {
		t.Error("the rendered selector mismatched")
	}
	other := want.DeepCopy()
	other.MatchLabels["extra"] = "x"
	m := checkWorkloadSelector("Deployment", "d", "dc1", other, want)
	if m == nil || !strings.Contains(m.message(), "kubectl delete deployment d") {
		t.Fatalf("mismatch = %v", m)
	}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "s", ResourceVersion: "1"},
		Spec: appsv1.StatefulSetSpec{Selector: mysqlDeploymentSelector(fg, "dc1")}}
	if statefulSetSelectorMismatch(sts, fg, "dc1") == nil {
		t.Error("a Dragonfly StatefulSet with the MySQL selector matched")
	}
}
