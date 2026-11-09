package controller

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// K015: a standing workload refusal gates deployment leases on its own,
// even when a reason that does not gate deployments (replica-audit
// divergence) holds the Degraded condition, and its message stays in that
// condition.
func TestWorkloadRefusalGatesLeasesBehindHigherRankedDegraded(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	runner := NewTopologyManagerRunner(nil, nil, nil, record.NewFakeRecorder(8), slog.Default())
	m.SetGroupRefusals(runner)
	if reason := m.UnstableReason(fg); reason != "" {
		t.Fatalf("baseline UnstableReason = %q, want stable", reason)
	}
	runner.SetGroupRefusal(fg, workloadRefusalKey("Deployment", "lion-dc2"), v1alpha1.ReasonWorkloadSelectorMismatch, "Deployment lion-dc2 is refused; delete it")
	if reason := m.UnstableReason(fg); reason != v1alpha1.ReasonWorkloadSelectorMismatch {
		t.Fatalf("UnstableReason with a refused workload = %q, want %s", reason, v1alpha1.ReasonWorkloadSelectorMismatch)
	}
	if _, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30); err == nil {
		t.Fatal("a lease was granted while a workload is refused")
	}

	conds := []metav1.Condition{{Type: v1alpha1.ConditionDegraded, Status: metav1.ConditionTrue,
		Reason: v1alpha1.ReasonDivergentTransactions, Message: "dc2 holds transactions the primary lacks."}}
	runner.appendRefusalDetail(&conds, client.ObjectKeyFromObject(fg))
	runner.appendRefusalDetail(&conds, client.ObjectKeyFromObject(fg))
	c := apimeta.FindStatusCondition(conds, v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonDivergentTransactions || strings.Count(c.Message, "Deployment lion-dc2 is refused; delete it") != 1 {
		t.Fatalf("Degraded = %s %q, want the divergence reason with the refusal's remedy once", c.Reason, c.Message)
	}

	runner.ClearGroupRefusal(client.ObjectKeyFromObject(fg), workloadRefusalKey("Deployment", "lion-dc2"))
	if reason := m.UnstableReason(fg); reason != "" {
		t.Fatalf("UnstableReason after the refusal cleared = %q, want stable", reason)
	}
}

// K069: a stalled ordered update (phase cleared, Updating=True/
// OrderedUpdateStalled) is still an ordered update for deployment
// stability and for other operations' admission; the ordered update's own
// retry is admitted.
func TestStalledOrderedUpdateGatesDeploymentsAndOperations(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	fg.Status.Conditions = append(fg.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionUpdating, Status: metav1.ConditionTrue,
		Reason: v1alpha1.ReasonOrderedUpdateStalled, Message: "waiting for pod", LastTransitionTime: metav1.Now()})
	if err := m.reader.(client.Client).Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if reason := DeploymentUnstableReason(fg); reason != v1alpha1.UnstableReasonOrderedUpdate {
		t.Fatalf("DeploymentUnstableReason = %q, want %s", reason, v1alpha1.UnstableReasonOrderedUpdate)
	}
	if _, err := m.BeginPlanned(context.Background(), fg, deploymentOperationPlannedFailover); err == nil {
		t.Fatal("a planned failover was admitted during a stalled ordered update")
	}
	if _, err := m.BeginPlanned(context.Background(), fg, deploymentOperationOrderedUpdate); err != nil {
		t.Fatalf("the ordered update's own retry was refused: %v", err)
	}
	m.EndPlanned(fg)
}

// K019: a workload refusal of a site that left spec.sites is cleared, so
// Degraded and the lease gate do not stay on it until a restart; refusals
// of spec sites stay.
func TestPruneRemovedSiteSelectorRefusals(t *testing.T) {
	fg := newTestFG()
	r, _ := newReconciler(fg)
	r.Runner = NewTopologyManagerRunner(nil, nil, nil, record.NewFakeRecorder(8), slog.Default())
	nn := client.ObjectKeyFromObject(fg)
	kept := workloadRefusalKey("Deployment", resourceName(fg.Name, fg.Spec.Sites[0].Name))
	gone := []string{workloadRefusalKey("Deployment", resourceName(fg.Name, "dc9")), workloadRefusalKey("StatefulSet", dragonflyStatefulSetName(fg.Name, "dc9"))}
	for _, k := range append([]string{kept}, gone...) {
		r.Runner.SetGroupRefusal(fg, k, v1alpha1.ReasonWorkloadSelectorMismatch, k)
	}
	r.Runner.SetGroupRefusal(fg, leaseRefusalKey("some-lease"), v1alpha1.ReasonMetadataVersionUnsupported, "lease")
	r.pruneRemovedSiteSelectorRefusals(fg)
	if keys := r.Runner.GroupRefusalKeys(nn, workloadRefusalPrefix); len(keys) != 1 || keys[0] != kept {
		t.Fatalf("workload refusals = %v, want only %s", keys, kept)
	}
	if keys := r.Runner.GroupRefusalKeys(nn, leaseRefusalPrefix); len(keys) != 1 {
		t.Fatalf("lease refusals = %v, want the lease refusal untouched", keys)
	}
	r.Runner.ClearGroupRefusal(nn, kept)
	r.Runner.ClearGroupRefusal(nn, leaseRefusalKey("some-lease"))
	if reason, _ := r.Runner.refusalDegraded(nn); reason != "" {
		t.Fatalf("Degraded refusal reason %q after every refusal cleared", reason)
	}
}
