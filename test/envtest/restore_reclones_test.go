//go:build envtest

package envtest

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// status.restoreReclones round-trips through the real CRD schema, and its
// phase enum rejects unknown values.
func TestRestoreReclonesStatusSchema(t *testing.T) {
	ns := createNamespace(t, "restore-reclones")
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, fg) })

	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: "2026-04-17T14:00:00Z",
		Phase:        v1alpha1.RestoreReclonesDraining,
		Pending:      []string{"dc2"},
		Requested:    "dc2",
	}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("status update: %v", err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
		t.Fatal(err)
	}
	q := got.Status.RestoreReclones
	if q == nil || q.Phase != v1alpha1.RestoreReclonesDraining || !slices.Equal(q.Pending, []string{"dc2"}) || q.Requested != "dc2" {
		t.Fatalf("round-trip = %+v", q)
	}

	got.Status.RestoreReclones.Phase = "Bogus"
	if err := k8sClient.Status().Update(ctx, &got); err == nil {
		t.Fatal("an unknown restoreReclones.phase must be rejected by the CRD schema")
	}
}

// #212: the fenced gtid_executed round-trips, and a failed full-instance
// restore whose Job ran leaves the queue Held (the peers keep their
// receivers stopped), decided from persisted status alone: reconcilers that
// start fresh (an operator restart) keep it Held.
func TestRestoreReclonesHeldAfterFailedRestoreAcrossRestarts(t *testing.T) {
	ns := createNamespace(t, "restore-reclones-held")
	ensureSecret(t, ns)
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, fg) })

	const fenced = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	now := metav1.Now()
	fg.Status.ActiveSite = "dc1"
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceFailed, TargetSite: "dc1", Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}, JobName: "mysql-lion-dc1-inplace-restore",
		ConfirmTokenUsed: "2026-04-17T14:00:00Z", StartTime: &now, CompletionTime: &now,
	}
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: "2026-04-17T14:00:00Z", Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"dc2"}, FencedGtidExecuted: fenced,
	}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("status update: %v", err)
	}
	for i := 0; i < 2; i++ {
		r := &controller.MysqlFailoverGroupReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
			t.Fatal(err)
		}
		q := got.Status.RestoreReclones
		if q == nil || q.Phase != v1alpha1.RestoreReclonesHeld || !slices.Equal(q.Pending, []string{"dc2"}) || q.FencedGtidExecuted != fenced {
			t.Fatalf("reconcile %d: queue = %+v, want Held with dc2 and the fenced gtid_executed kept", i, q)
		}
	}
}
