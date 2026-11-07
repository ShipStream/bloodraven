//go:build envtest

package envtest

import (
	"log/slog"
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
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/state"
)

// TestEnvtest_WorkloadSelectorMismatchRefused pins A-77 against a real API
// server, where a Deployment's selector is immutable: a site Deployment
// left with a pre-A-77 selector is refused, not patched. The group reports
// Degraded=True/WorkloadSelectorMismatch with one Warning Event naming the
// Deployment, and the reconcile returns no error, so nothing retries an
// update the API server would reject.
func TestEnvtest_WorkloadSelectorMismatchRefused(t *testing.T) {
	ns := "envtest-selector-mismatch"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	old := map[string]string{"app.kubernetes.io/name": "mysql", "app.kubernetes.io/instance": "lion", "shipstream.io/site": "dc1"}
	foreign := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-lion-dc1", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: old},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: old},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:9.7"}}},
			},
		},
	}
	if err := k8sClient.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(64)
	runner := controller.NewTopologyManagerRunner(k8sClient, nil, nil, rec, slog.New(slog.DiscardHandler))
	for _, strategy := range []v1alpha1.UpdateStrategyType{"", v1alpha1.UpdateStrategyRecreate} {
		var cur v1alpha1.MysqlFailoverGroup
		nn := types.NamespacedName{Name: fg.Name, Namespace: ns}
		if err := k8sClient.Get(ctx, nn, &cur); err != nil {
			t.Fatal(err)
		}
		cur.Spec.UpdateStrategy.Type = strategy
		if err := k8sClient.Update(ctx, &cur); err != nil {
			t.Fatal(err)
		}
		r := &controller.MysqlFailoverGroupReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(64), Runner: runner}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("strategy %q: reconcile returned %v; a refused workload is not retried", strategy, err)
		}
		var got appsv1.Deployment
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: foreign.Name, Namespace: ns}, &got); err != nil {
			t.Fatal(err)
		}
		if got.Generation != 1 || got.Spec.Selector.MatchLabels["shipstream.io/site"] != "dc1" {
			t.Fatalf("strategy %q: the refused Deployment was updated (generation %d, selector %v)", strategy, got.Generation, got.Spec.Selector)
		}
	}
	n := 0
	for len(rec.Events) > 0 {
		if e := <-rec.Events; strings.Contains(e, v1alpha1.EventWorkloadSelectorMismatch) && strings.Contains(e, "mysql-lion-dc1") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d WorkloadSelectorMismatch Events, want 1", n)
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: ns}
	snap := controller.TopologySnapshot{
		ActiveSite: "dc1",
		Sites: []controller.SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true, SourceConvergenceState: v1alpha1.SourceConvergenceConverged},
		},
		DegradedReason: v1alpha1.ReasonHealthy,
	}
	if err := runner.WriteStatusForTest(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, nn, &got); err != nil {
		t.Fatal(err)
	}
	d := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDegraded)
	if d == nil || d.Status != metav1.ConditionTrue || d.Reason != v1alpha1.ReasonWorkloadSelectorMismatch || !strings.Contains(d.Message, "mysql-lion-dc1") {
		t.Fatalf("Degraded = %+v, want True/WorkloadSelectorMismatch naming mysql-lion-dc1", d)
	}
}
