//go:build envtest

package envtest

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

func newTestPair(namespace string) *v1alpha1.MysqlReplicaPair {
	return &v1alpha1.MysqlReplicaPair{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lion",
			Namespace: namespace,
		},
		Spec: v1alpha1.MysqlReplicaPairSpec{
			Image: "mysql:9.6",
			DC1: v1alpha1.DCInstanceSpec{
				Name: "dc1",
				Zone: "lion-dc1",
				LBIP: "203.0.113.1",
				Storage: v1alpha1.StorageSpec{
					StorageClassName: "standard",
					Size:             resource.MustParse("10Gi"),
				},
			},
			DC2: v1alpha1.DCInstanceSpec{
				Name: "dc2",
				Zone: "lion-dc2",
				LBIP: "203.0.113.2",
				Storage: v1alpha1.StorageSpec{
					StorageClassName: "standard",
					Size:             resource.MustParse("10Gi"),
				},
			},
			SecretName: "mysql-credentials",
			AZ:         "lion",
			Cloudflare: v1alpha1.CloudflareSpec{
				APITokenSecretRef: v1alpha1.SecretKeyRef{
					Name: "cloudflare-credentials",
					Key:  "api-token",
				},
				ZoneID: "abc123",
			},
			PollInterval:      &metav1.Duration{Duration: 2 * time.Second},
			FailureThreshold:  3,
			RecoveryThreshold: 2,
			FailoverCooldown:  &metav1.Duration{Duration: 60 * time.Minute},
		},
	}
}

// ensureNamespace creates a namespace for test isolation.
func ensureNamespace(t *testing.T, name string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8sClient.Create(ctx, ns); err != nil {
		// Ignore already-exists errors.
		return
	}
}

// ensureSecret creates the MySQL credentials secret.
func ensureSecret(t *testing.T, namespace string) {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mysql-credentials",
			Namespace: namespace,
		},
		Data: map[string][]byte{
			"dsn": []byte("root:password@tcp(localhost:3306)/mysql"),
		},
	}
	if err := k8sClient.Create(ctx, secret); err != nil {
		return
	}
}

func TestEnvtest_CRCreationAndSchemaAcceptance(t *testing.T) {
	ns := "envtest-cr-creation"
	ensureNamespace(t, ns)

	pair := newTestPair(ns)
	if err := k8sClient.Create(ctx, pair); err != nil {
		t.Fatalf("failed to create MysqlReplicaPair: %v", err)
	}

	// Verify we can read it back
	var fetched v1alpha1.MysqlReplicaPair
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lion", Namespace: ns}, &fetched); err != nil {
		t.Fatalf("failed to get MysqlReplicaPair: %v", err)
	}

	if fetched.Spec.DC1.Name != "dc1" {
		t.Errorf("expected DC1 name 'dc1', got %q", fetched.Spec.DC1.Name)
	}
	if fetched.Spec.DC2.Name != "dc2" {
		t.Errorf("expected DC2 name 'dc2', got %q", fetched.Spec.DC2.Name)
	}
	if fetched.Spec.AZ != "lion" {
		t.Errorf("expected AZ 'lion', got %q", fetched.Spec.AZ)
	}
}

func TestEnvtest_StatusSubresourceWrites(t *testing.T) {
	ns := "envtest-status-write"
	ensureNamespace(t, ns)

	pair := newTestPair(ns)
	if err := k8sClient.Create(ctx, pair); err != nil {
		t.Fatalf("failed to create MysqlReplicaPair: %v", err)
	}

	// Update status subresource
	pair.Status.PrimaryDC = "dc1"
	pair.Status.DC1 = v1alpha1.DCInstanceStatus{State: "writable"}
	pair.Status.DC2 = v1alpha1.DCInstanceStatus{State: "read-only"}
	now := metav1.Now()
	pair.Status.LastFailover = &now
	pair.Status.LastFailoverTarget = "dc1"

	if err := k8sClient.Status().Update(ctx, pair); err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	// Read back and verify
	var fetched v1alpha1.MysqlReplicaPair
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lion", Namespace: ns}, &fetched); err != nil {
		t.Fatalf("failed to get pair: %v", err)
	}

	if fetched.Status.PrimaryDC != "dc1" {
		t.Errorf("expected PrimaryDC 'dc1', got %q", fetched.Status.PrimaryDC)
	}
	if fetched.Status.DC1.State != "writable" {
		t.Errorf("expected DC1 state 'writable', got %q", fetched.Status.DC1.State)
	}
	if fetched.Status.DC2.State != "read-only" {
		t.Errorf("expected DC2 state 'read-only', got %q", fetched.Status.DC2.State)
	}
	if fetched.Status.LastFailoverTarget != "dc1" {
		t.Errorf("expected LastFailoverTarget 'dc1', got %q", fetched.Status.LastFailoverTarget)
	}
}

func TestEnvtest_ReconcilerCreatesResources(t *testing.T) {
	ns := "envtest-reconciler"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)

	pair := newTestPair(ns)
	if err := k8sClient.Create(ctx, pair); err != nil {
		t.Fatalf("failed to create pair: %v", err)
	}

	// Run the reconciler against the real API server
	r := &controller.MysqlReplicaPairReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: ns},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify ConfigMap was created with owner reference
	var cm corev1.ConfigMap
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name: "mysql-lion-config", Namespace: ns,
	}, &cm); err != nil {
		t.Fatalf("ConfigMap not created: %v", err)
	}
	if len(cm.OwnerReferences) == 0 {
		t.Error("ConfigMap should have owner reference to MysqlReplicaPair")
	} else if cm.OwnerReferences[0].Name != "lion" {
		t.Errorf("ConfigMap owner ref: got %q, want 'lion'", cm.OwnerReferences[0].Name)
	}

	// Verify Deployments
	for _, dc := range []string{"dc1", "dc2"} {
		var deploy appsv1.Deployment
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: "mysql-lion-" + dc, Namespace: ns,
		}, &deploy); err != nil {
			t.Errorf("Deployment for %s not created: %v", dc, err)
			continue
		}

		// Verify owner reference
		if len(deploy.OwnerReferences) == 0 {
			t.Errorf("Deployment %s should have owner reference", dc)
		}

		// Verify two containers (mysql + sidecar)
		if len(deploy.Spec.Template.Spec.Containers) != 2 {
			t.Errorf("Deployment %s: expected 2 containers, got %d", dc, len(deploy.Spec.Template.Spec.Containers))
		}
	}

	// Verify Services
	for _, svcName := range []string{"mysql-lion-dc1", "mysql-lion-dc2", "mysql-lion-primary", "mysql-lion-replicas"} {
		var svc corev1.Service
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: svcName, Namespace: ns,
		}, &svc); err != nil {
			t.Errorf("Service %s not created: %v", svcName, err)
			continue
		}
		if len(svc.OwnerReferences) == 0 {
			t.Errorf("Service %s should have owner reference", svcName)
		}
	}

	// Verify PVCs
	for _, dc := range []string{"dc1", "dc2"} {
		var pvc corev1.PersistentVolumeClaim
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: "mysql-lion-" + dc + "-data", Namespace: ns,
		}, &pvc); err != nil {
			t.Errorf("PVC for %s not created: %v", dc, err)
			continue
		}
		if len(pvc.OwnerReferences) == 0 {
			t.Errorf("PVC %s should have owner reference", dc)
		}
	}
}

func TestEnvtest_ReconcilerIdempotent(t *testing.T) {
	ns := "envtest-idempotent"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)

	pair := newTestPair(ns)
	if err := k8sClient.Create(ctx, pair); err != nil {
		t.Fatalf("failed to create pair: %v", err)
	}

	r := &controller.MysqlReplicaPairReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	nn := types.NamespacedName{Name: "lion", Namespace: ns}

	// First reconcile
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	// Second reconcile should succeed (idempotent)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	// Third reconcile for good measure
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("third reconcile failed: %v", err)
	}
}

func TestEnvtest_ReconcilerHandlesSpecChange(t *testing.T) {
	ns := "envtest-spec-change"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)

	pair := newTestPair(ns)
	if err := k8sClient.Create(ctx, pair); err != nil {
		t.Fatalf("failed to create pair: %v", err)
	}

	r := &controller.MysqlReplicaPairReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	nn := types.NamespacedName{Name: "lion", Namespace: ns}

	// Initial reconcile
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}

	// Change the image
	var fetched v1alpha1.MysqlReplicaPair
	if err := k8sClient.Get(ctx, nn, &fetched); err != nil {
		t.Fatalf("failed to get pair: %v", err)
	}
	fetched.Spec.Image = "mysql:8.4"
	if err := k8sClient.Update(ctx, &fetched); err != nil {
		t.Fatalf("failed to update pair: %v", err)
	}

	// Re-reconcile should update the deployment
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("re-reconcile after spec change failed: %v", err)
	}

	// Verify the deployment was updated
	var deploy appsv1.Deployment
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name: "mysql-lion-dc1", Namespace: ns,
	}, &deploy); err != nil {
		t.Fatalf("deployment not found: %v", err)
	}
	if deploy.Spec.Template.Spec.Containers[0].Image != "mysql:8.4" {
		t.Errorf("expected image mysql:8.4, got %s", deploy.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestEnvtest_ReconcilerMissingSecretRequeues(t *testing.T) {
	ns := "envtest-no-secret"
	ensureNamespace(t, ns)
	// Intentionally don't create the secret

	pair := newTestPair(ns)
	if err := k8sClient.Create(ctx, pair); err != nil {
		t.Fatalf("failed to create pair: %v", err)
	}

	r := &controller.MysqlReplicaPairReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: ns},
	})
	if err != nil {
		t.Fatalf("reconcile should not error on missing secret: %v", err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Errorf("expected requeue after 30s, got %v", result.RequeueAfter)
	}
}
