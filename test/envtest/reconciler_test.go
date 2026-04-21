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

func newTestFG(namespace string) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lion",
			Namespace: namespace,
		},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Image: "mysql:9.6",
			Sites: []v1alpha1.SiteSpec{
				{
					Name: "dc1",
					Zone: "lion-dc1",
					LBIP: "203.0.113.1",
					Storage: v1alpha1.StorageSpec{
						StorageClassName: "standard",
						Size:             resource.MustParse("10Gi"),
					},
				},
				{
					Name: "dc2",
					Zone: "lion-dc2",
					LBIP: "203.0.113.2",
					Storage: v1alpha1.StorageSpec{
						StorageClassName: "standard",
						Size:             resource.MustParse("10Gi"),
					},
				},
			},
			SecretName: "mysql-credentials",
			DNS: v1alpha1.DNSSpec{
				Hostname: "lion.az.example.com",
				TTL:      60,
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

	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("failed to create MysqlFailoverGroup: %v", err)
	}

	// Verify we can read it back
	var fetched v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lion", Namespace: ns}, &fetched); err != nil {
		t.Fatalf("failed to get MysqlFailoverGroup: %v", err)
	}

	if fetched.Spec.Sites[0].Name != "dc1" {
		t.Errorf("expected Sites[0] name 'dc1', got %q", fetched.Spec.Sites[0].Name)
	}
	if fetched.Spec.Sites[1].Name != "dc2" {
		t.Errorf("expected Sites[1] name 'dc2', got %q", fetched.Spec.Sites[1].Name)
	}
	if fetched.Spec.DNS.Hostname != "lion.az.example.com" {
		t.Errorf("expected DNS hostname 'lion.az.example.com', got %q", fetched.Spec.DNS.Hostname)
	}
}

func TestEnvtest_StatusSubresourceWrites(t *testing.T) {
	ns := "envtest-status-write"
	ensureNamespace(t, ns)

	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("failed to create MysqlFailoverGroup: %v", err)
	}

	// Update status subresource
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "dc1", State: "writable"},
		{Name: "dc2", State: "read-only"},
	}
	now := metav1.Now()
	fg.Status.LastFailover = &now
	fg.Status.LastFailoverTarget = "dc1"

	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	// Read back and verify
	var fetched v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lion", Namespace: ns}, &fetched); err != nil {
		t.Fatalf("failed to get fg: %v", err)
	}

	if fetched.Status.ActiveSite != "dc1" {
		t.Errorf("expected ActiveSite 'dc1', got %q", fetched.Status.ActiveSite)
	}
	if fetched.Status.Sites[0].State != "writable" {
		t.Errorf("expected Sites[0] state 'writable', got %q", fetched.Status.Sites[0].State)
	}
	if fetched.Status.Sites[1].State != "read-only" {
		t.Errorf("expected Sites[1] state 'read-only', got %q", fetched.Status.Sites[1].State)
	}
	if fetched.Status.LastFailoverTarget != "dc1" {
		t.Errorf("expected LastFailoverTarget 'dc1', got %q", fetched.Status.LastFailoverTarget)
	}
}

func TestEnvtest_PlannedFailoverSpecAndStatus(t *testing.T) {
	ns := "envtest-planned-failover"
	ensureNamespace(t, ns)

	fg := newTestFG(ns)
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		MaxLagWait:   &metav1.Duration{Duration: 2 * time.Minute},
		DrainTimeout: &metav1.Duration{Duration: 10 * time.Second},
		OnCooldown:   "defer",
	}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("failed to create fg with spec.plannedFailover: %v", err)
	}

	// Stamp a Deferred status with all the fields that need schema validation.
	now := metav1.Now()
	retry := metav1.NewTime(now.Add(2 * time.Minute))
	lagWrap := metav1.Duration{Duration: 2 * time.Minute}
	drainWrap := metav1.Duration{Duration: 10 * time.Second}
	lost := int64(0)
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:            v1alpha1.PlannedFailoverPhaseDeferred,
		Target:           "dc2",
		SourcePrimary:    "dc1",
		StartTime:        &now,
		MaxLagWait:       &lagWrap,
		DrainTimeout:     &drainWrap,
		RetryAfter:       &retry,
		Reason:           "CooldownActive",
		Message:          "cooldown active; retrying at " + retry.Format(time.RFC3339),
		TransactionsLost: &lost,
	}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("failed to update planned-failover status: %v", err)
	}

	var fetched v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lion", Namespace: ns}, &fetched); err != nil {
		t.Fatalf("failed to read back: %v", err)
	}
	if fetched.Spec.PlannedFailover == nil ||
		fetched.Spec.PlannedFailover.OnCooldown != "defer" ||
		fetched.Spec.PlannedFailover.MaxLagWait.Duration != 2*time.Minute ||
		fetched.Spec.PlannedFailover.DrainTimeout.Duration != 10*time.Second {
		t.Errorf("spec.plannedFailover round-trip mismatch: %+v", fetched.Spec.PlannedFailover)
	}
	if fetched.Status.PlannedFailover == nil ||
		fetched.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseDeferred {
		t.Fatalf("status.plannedFailover round-trip mismatch: %+v", fetched.Status.PlannedFailover)
	}
	if fetched.Status.PlannedFailover.Reason != "CooldownActive" {
		t.Errorf("reason = %q, want CooldownActive", fetched.Status.PlannedFailover.Reason)
	}
	if fetched.Status.PlannedFailover.RetryAfter == nil {
		t.Error("RetryAfter not round-tripped")
	}
}

func TestEnvtest_PlannedFailoverOnCooldownEnumRejectsGarbage(t *testing.T) {
	ns := "envtest-planned-failover-enum"
	ensureNamespace(t, ns)

	fg := newTestFG(ns)
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		OnCooldown: "bogus",
	}
	err := k8sClient.Create(ctx, fg)
	if err == nil {
		t.Fatal("expected CRD enum validation to reject onCooldown=bogus")
	}
}

func TestEnvtest_PlannedFailoverPhaseEnumRejectsGarbage(t *testing.T) {
	ns := "envtest-planned-failover-phase"
	ensureNamespace(t, ns)

	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create: %v", err)
	}
	now := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:     v1alpha1.PlannedFailoverPhase("NotARealPhase"),
		Target:    "dc2",
		StartTime: &now,
	}
	err := k8sClient.Status().Update(ctx, fg)
	if err == nil {
		t.Fatal("expected CRD enum validation to reject an unknown phase")
	}
}

func TestEnvtest_ReconcilerCreatesResources(t *testing.T) {
	ns := "envtest-reconciler"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)

	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("failed to create fg: %v", err)
	}

	// Run the reconciler against the real API server
	r := &controller.MysqlFailoverGroupReconciler{
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
		t.Error("ConfigMap should have owner reference to MysqlFailoverGroup")
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

	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("failed to create fg: %v", err)
	}

	r := &controller.MysqlFailoverGroupReconciler{
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

	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("failed to create fg: %v", err)
	}

	r := &controller.MysqlFailoverGroupReconciler{
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
	var fetched v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, nn, &fetched); err != nil {
		t.Fatalf("failed to get fg: %v", err)
	}
	fetched.Spec.Image = "mysql:8.4"
	if err := k8sClient.Update(ctx, &fetched); err != nil {
		t.Fatalf("failed to update fg: %v", err)
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

	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("failed to create fg: %v", err)
	}

	r := &controller.MysqlFailoverGroupReconciler{
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
