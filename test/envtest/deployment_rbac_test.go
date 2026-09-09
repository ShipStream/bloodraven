//go:build envtest

package envtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

func TestDeploymentLeaseRBAC(t *testing.T) {
	ns := createNamespace(t, "deploy-rbac")
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "operator", Namespace: ns}}
	if err := k8sClient.Create(ctx, sa); err != nil {
		t.Fatal(err)
	}
	impersonated := rest.CopyConfig(cfg)
	impersonated.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + ns + ":" + sa.Name,
		Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:" + ns, "system:authenticated"},
	}
	operator, err := kubernetes.NewForConfig(impersonated)
	if err != nil {
		t.Fatal(err)
	}
	leases := operator.CoordinationV1().Leases(ns)
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "deployment-migration", Namespace: ns}}
	if _, err := leases.Create(ctx, lease, metav1.CreateOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("unbound ServiceAccount create Lease = %v, want forbidden", err)
	}
	tokenReview := func() *authenticationv1.TokenReview {
		return &authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: "invalid-test-token", Audiences: []string{"bloodraven-deploy"}}}
	}
	if _, err := operator.AuthenticationV1().TokenReviews().Create(ctx, tokenReview(), metav1.CreateOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("unbound ServiceAccount create TokenReview = %v, want forbidden", err)
	}

	// Exercise generated operator permissions, not a hand-written test Role.
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.UnmarshalStrict(data, &role); err != nil {
		t.Fatal(err)
	}
	role.Name = ns + "-operator"
	if err := k8sClient.Create(ctx, &role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, &role) })
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: role.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: ns, Name: sa.Name}},
	}
	if err := k8sClient.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, binding) })
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := leases.List(ctx, metav1.ListOptions{})
		if apierrors.IsForbidden(err) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		t.Fatalf("wait for RBAC propagation: %v", err)
	}

	created, err := leases.Create(ctx, lease, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create Lease: %v", err)
	}
	if _, err := leases.Get(ctx, lease.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("get Lease: %v", err)
	}
	listed, err := leases.List(ctx, metav1.ListOptions{})
	if err != nil || len(listed.Items) != 1 {
		t.Fatalf("list Leases = %v, %v", listed, err)
	}
	watchCtx, stopWatch := context.WithTimeout(ctx, 10*time.Second)
	defer stopWatch()
	watcher, err := leases.Watch(watchCtx, metav1.ListOptions{ResourceVersion: listed.ResourceVersion})
	if err != nil {
		t.Fatalf("watch Leases: %v", err)
	}
	defer watcher.Stop()
	holder := "deployment-attempt"
	created.Spec.HolderIdentity = &holder
	updated, err := leases.Update(ctx, created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update Lease: %v", err)
	}
	select {
	case event, ok := <-watcher.ResultChan():
		observed, isLease := event.Object.(*coordinationv1.Lease)
		if !ok || event.Type != watch.Modified || !isLease || observed.Spec.HolderIdentity == nil || *observed.Spec.HolderIdentity != holder {
			t.Fatalf("watch event = %+v, want updated holder", event)
		}
	case <-watchCtx.Done():
		t.Fatal("Lease watch did not deliver update")
	}
	patched, err := leases.Patch(ctx, updated.Name, types.MergePatchType, []byte(`{"spec":{"leaseDurationSeconds":30}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("patch Lease: %v", err)
	}
	if patched.Spec.LeaseDurationSeconds == nil || *patched.Spec.LeaseDurationSeconds != 30 {
		t.Fatal("Lease patch was not persisted")
	}
	if err := leases.Delete(ctx, lease.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Lease: %v", err)
	}
	if _, err := leases.Get(ctx, lease.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted Lease get = %v", err)
	}
	if _, err := operator.AuthenticationV1().TokenReviews().Create(ctx, tokenReview(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create TokenReview: %v", err)
	}

	// Binding the operator must not authorize a different ServiceAccount.
	impersonated.Impersonate.UserName = "system:serviceaccount:" + ns + ":deploy-client"
	outsider, err := kubernetes.NewForConfig(impersonated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outsider.CoordinationV1().Leases(ns).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("unbound deploy client list Leases = %v, want forbidden", err)
	}
	if _, err := outsider.AuthenticationV1().TokenReviews().Create(ctx, tokenReview(), metav1.CreateOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("unbound deploy client create TokenReview = %v, want forbidden", err)
	}
}
