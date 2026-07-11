package chaos

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

func rustfsReplicaPtr(v int32) *int32 { return &v }

// TestScaleRustFSToZeroAndRevert proves the RustFS scale helper takes storage
// offline and its reverter restores it — the cleanup contract scenario 36
// relies on so a failed in-place restore does not leak a scaled-to-0 RustFS.
func TestScaleRustFSToZeroAndRevert(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: rustfsDeployment, Namespace: pgkube.PlaygroundNamespace},
		Spec:       appsv1.DeploymentSpec{Replicas: rustfsReplicaPtr(1)},
		// availableReplicas stays 1 in the fake (no controllers run), so the
		// scale-up wait returns immediately.
		Status: appsv1.DeploymentStatus{AvailableReplicas: 1},
	}
	cs := fake.NewSimpleClientset(dep)
	k := &pgkube.Client{Kubernetes: cs}
	a := New(k, pgkube.PlaygroundNamespace, pgkube.FailoverGroupName)

	ctx := context.Background()
	if _, err := a.ScaleRustFSToZero(ctx); err != nil {
		t.Fatalf("ScaleRustFSToZero: %v", err)
	}
	got, err := cs.AppsV1().Deployments(pgkube.PlaygroundNamespace).Get(ctx, rustfsDeployment, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get rustfs deployment: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("rustfs replicas after scale-to-zero = %v, want 0", got.Spec.Replicas)
	}
	if pend := a.PendingReverts(); len(pend) != 1 {
		t.Fatalf("expected 1 pending reverter after scale-to-zero, got %v", pend)
	}

	if err := a.Revert(ctx); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	got, err = cs.AppsV1().Deployments(pgkube.PlaygroundNamespace).Get(ctx, rustfsDeployment, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get rustfs deployment after revert: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("rustfs replicas after revert = %v, want 1", got.Spec.Replicas)
	}
}

// TestRecoverRustFS is the GlobalRecover backstop: if the process that scaled
// RustFS to 0 dies before its reverter runs, the safety net still brings object
// storage back — otherwise every backup/restore scenario after it fails.
func TestRecoverRustFS(t *testing.T) {
	ctx := context.Background()

	t.Run("scaled to zero is restored", func(t *testing.T) {
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: rustfsDeployment, Namespace: pgkube.PlaygroundNamespace},
			Spec:       appsv1.DeploymentSpec{Replicas: rustfsReplicaPtr(0)},
			Status:     appsv1.DeploymentStatus{AvailableReplicas: 1},
		}
		cs := fake.NewSimpleClientset(dep)
		a := New(&pgkube.Client{Kubernetes: cs}, pgkube.PlaygroundNamespace, pgkube.FailoverGroupName)

		// No reverter was ever pushed — this is the crashed-runner case.
		if err := a.RecoverRustFS(ctx); err != nil {
			t.Fatalf("RecoverRustFS: %v", err)
		}
		got, err := cs.AppsV1().Deployments(pgkube.PlaygroundNamespace).Get(ctx, rustfsDeployment, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get rustfs deployment: %v", err)
		}
		if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
			t.Fatalf("rustfs replicas after recover = %v, want 1", got.Spec.Replicas)
		}
	})

	t.Run("already up is a no-op", func(t *testing.T) {
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: rustfsDeployment, Namespace: pgkube.PlaygroundNamespace},
			Spec:       appsv1.DeploymentSpec{Replicas: rustfsReplicaPtr(1)},
			Status:     appsv1.DeploymentStatus{AvailableReplicas: 1},
		}
		cs := fake.NewSimpleClientset(dep)
		a := New(&pgkube.Client{Kubernetes: cs}, pgkube.PlaygroundNamespace, pgkube.FailoverGroupName)

		if err := a.RecoverRustFS(ctx); err != nil {
			t.Fatalf("RecoverRustFS: %v", err)
		}
		for _, action := range cs.Actions() {
			if action.GetVerb() == "update" {
				t.Errorf("a healthy RustFS must not be written to; got %s on %s",
					action.GetVerb(), action.GetResource().Resource)
			}
		}
	})

	t.Run("absent RustFS is not an error", func(t *testing.T) {
		cs := fake.NewSimpleClientset()
		a := New(&pgkube.Client{Kubernetes: cs}, pgkube.PlaygroundNamespace, pgkube.FailoverGroupName)

		if err := a.RecoverRustFS(ctx); err != nil {
			t.Errorf("a playground without RustFS must be a no-op, got %v", err)
		}
	})
}
