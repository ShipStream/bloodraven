package controller

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The operator's RBAC grants list and delete but not deletecollection, so
// teardown must never call DeleteAllOf: a group with Dragonfly disabled
// reconciles teardown on every pass, and a forbidden DeleteAllOf failed
// every reconcile of such a group.
func TestTeardownDragonflyResourcesNeedsNoDeleteCollection(t *testing.T) {
	ns := "ns"
	owned := map[string]string{labelAppName: dragonflyAppName, labelInstance: "lion"}
	other := map[string]string{labelAppName: dragonflyAppName, labelInstance: "tiger"}
	objs := []client.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "lion-dragonfly-a", Namespace: ns, Labels: owned}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "tiger-dragonfly-a", Namespace: ns, Labels: other}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "lion-dragonfly-pdb", Namespace: ns, Labels: owned}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "lion-dragonfly-active", Namespace: ns, Labels: owned}},
	}
	base := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		DeleteAllOf: func(context.Context, client.WithWatch, client.Object, ...client.DeleteAllOfOption) error {
			return errors.New(`statefulsets.apps is forbidden: cannot deletecollection`)
		},
	})
	r := &MysqlFailoverGroupReconciler{Client: c}
	fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: ns}}
	ctx := context.Background()

	if err := r.teardownDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("teardown: %v", err)
	}
	var stss appsv1.StatefulSetList
	if err := base.List(ctx, &stss, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	if len(stss.Items) != 1 || stss.Items[0].Name != "tiger-dragonfly-a" {
		t.Fatalf("statefulsets left: %+v; want only the other group's", stss.Items)
	}
	var pdbs policyv1.PodDisruptionBudgetList
	if err := base.List(ctx, &pdbs, client.InNamespace(ns)); err != nil || len(pdbs.Items) != 0 {
		t.Fatalf("pdbs left: %d (err %v)", len(pdbs.Items), err)
	}
	var svcs corev1.ServiceList
	if err := base.List(ctx, &svcs, client.InNamespace(ns)); err != nil || len(svcs.Items) != 0 {
		t.Fatalf("services left: %d (err %v)", len(svcs.Items), err)
	}
	// Idempotent with nothing left to delete.
	if err := r.teardownDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("second teardown: %v", err)
	}
}
