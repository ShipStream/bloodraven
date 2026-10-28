//go:build envtest

package envtest

import (
	"slices"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
