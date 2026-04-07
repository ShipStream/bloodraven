package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

func newTestPair() *v1alpha1.MysqlReplicaPair {
	return &v1alpha1.MysqlReplicaPair{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lion",
			Namespace: "shared-lion",
			UID:       "test-uid-123",
		},
		Spec: v1alpha1.MysqlReplicaPairSpec{
			Image: "mysql:9.6",
			DC1: v1alpha1.DCInstanceSpec{
				Name: "dc1",
				Zone: "lion-dc1",
				LBIP: "203.0.113.1",
				Storage: v1alpha1.StorageSpec{
					StorageClassName: "local-dc1",
					Size:             resource.MustParse("100Gi"),
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("8Gi"),
					},
				},
			},
			DC2: v1alpha1.DCInstanceSpec{
				Name: "dc2",
				Zone: "lion-dc2",
				LBIP: "203.0.113.2",
				Storage: v1alpha1.StorageSpec{
					StorageClassName: "local-dc2",
					Size:             resource.MustParse("100Gi"),
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("8Gi"),
					},
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

func newReconciler(objs ...client.Object) (*MysqlReplicaPairReconciler, client.Client) {
	scheme := testScheme()
	cb := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlReplicaPair{})
	if len(objs) > 0 {
		cb = cb.WithObjects(objs...)
	}
	c := cb.Build()
	r := &MysqlReplicaPairReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	return r, c
}

func TestReconcile_CreatesConfigMap(t *testing.T) {
	pair := newTestPair()
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-config", Namespace: "shared-lion",
	}, &cm); err != nil {
		t.Fatalf("configmap not created: %v", err)
	}

	myCnf, ok := cm.Data["my.cnf"]
	if !ok {
		t.Fatal("my.cnf not found in configmap data")
	}

	// Check for key config values
	for _, expected := range []string{"gtid-mode=ON", "enforce-gtid-consistency=ON", "max-connections=500"} {
		if !strings.Contains(myCnf, expected) {
			t.Errorf("my.cnf missing expected setting: %s", expected)
		}
	}
}

func TestReconcile_CreatesPVCs(t *testing.T) {
	pair := newTestPair()
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	for _, dc := range []string{"dc1", "dc2"} {
		var pvc corev1.PersistentVolumeClaim
		if err := c.Get(context.Background(), types.NamespacedName{
			Name: "mysql-lion-" + dc + "-data", Namespace: "shared-lion",
		}, &pvc); err != nil {
			t.Errorf("PVC for %s not created: %v", dc, err)
		}
	}
}

func TestReconcile_CreatesDeployments(t *testing.T) {
	pair := newTestPair()
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	for _, dc := range []string{"dc1", "dc2"} {
		var deploy appsv1.Deployment
		if err := c.Get(context.Background(), types.NamespacedName{
			Name: "mysql-lion-" + dc, Namespace: "shared-lion",
		}, &deploy); err != nil {
			t.Errorf("Deployment for %s not created: %v", dc, err)
			continue
		}

		// Check replicas
		if *deploy.Spec.Replicas != 1 {
			t.Errorf("deployment %s: expected 1 replica, got %d", dc, *deploy.Spec.Replicas)
		}

		// Check container image
		containers := deploy.Spec.Template.Spec.Containers
		if len(containers) == 0 {
			t.Errorf("deployment %s: no containers", dc)
			continue
		}
		if containers[0].Image != "mysql:9.6" {
			t.Errorf("deployment %s: expected image mysql:9.6, got %s", dc, containers[0].Image)
		}

		// Check node selector
		ns := deploy.Spec.Template.Spec.NodeSelector
		expectedZone := "lion-" + dc
		if ns["topology.kubernetes.io/zone"] != expectedZone {
			t.Errorf("deployment %s: expected zone %s, got %s", dc, expectedZone, ns["topology.kubernetes.io/zone"])
		}

		// Check init container exists
		initContainers := deploy.Spec.Template.Spec.InitContainers
		if len(initContainers) == 0 {
			t.Errorf("deployment %s: no init containers", dc)
		}
	}
}

func TestReconcile_CreatesServices(t *testing.T) {
	pair := newTestPair()
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	expectedServices := []string{
		"mysql-lion-dc1",
		"mysql-lion-dc2",
		"mysql-lion-primary",
		"mysql-lion-replicas",
	}

	for _, svcName := range expectedServices {
		var svc corev1.Service
		if err := c.Get(context.Background(), types.NamespacedName{
			Name: svcName, Namespace: "shared-lion",
		}, &svc); err != nil {
			t.Errorf("Service %s not created: %v", svcName, err)
		}
	}
}

func TestReconcile_PrimaryServiceSelector(t *testing.T) {
	pair := newTestPair()
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var svc corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-primary", Namespace: "shared-lion",
	}, &svc); err != nil {
		t.Fatalf("primary service not found: %v", err)
	}

	if svc.Spec.Selector[labelRole] != "primary" {
		t.Errorf("primary service selector role: expected 'primary', got %q", svc.Spec.Selector[labelRole])
	}
}

func TestReconcile_ReplicasServiceSelector(t *testing.T) {
	pair := newTestPair()
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var svc corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-replicas", Namespace: "shared-lion",
	}, &svc); err != nil {
		t.Fatalf("replicas service not found: %v", err)
	}

	if svc.Spec.Selector[labelRole] != "replica" {
		t.Errorf("replicas service selector role: expected 'replica', got %q", svc.Spec.Selector[labelRole])
	}
	if svc.Spec.Selector[labelHealthy] != "yes" {
		t.Errorf("replicas service selector healthy: expected 'yes', got %q", svc.Spec.Selector[labelHealthy])
	}
}

func TestReconcile_NotFound(t *testing.T) {
	r, _ := newReconciler()

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "does-not-exist", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconcile should not error on not found: %v", err)
	}
	if result.Requeue {
		t.Error("should not requeue on not found")
	}
}

func TestReconcile_Idempotent(t *testing.T) {
	pair := newTestPair()
	r, _ := newReconciler(pair)
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}

	// First reconcile
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	// Second reconcile should also succeed (idempotent)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
}

func TestReconcile_TLSConfig(t *testing.T) {
	pair := newTestPair()
	pair.Spec.TLS = &v1alpha1.TLSSpec{
		IssuerRef: v1alpha1.IssuerRef{
			Name: "letsencrypt",
			Kind: "ClusterIssuer",
		},
	}
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-config", Namespace: "shared-lion",
	}, &cm); err != nil {
		t.Fatalf("configmap not found: %v", err)
	}

	myCnf := cm.Data["my.cnf"]
	if !strings.Contains(myCnf, "require-secure-transport=ON") {
		t.Error("TLS-enabled config should contain require-secure-transport=ON")
	}
	if !strings.Contains(myCnf, "ssl-cert=/etc/mysql/tls/tls.crt") {
		t.Error("TLS-enabled config should contain ssl-cert path")
	}
}

func TestReconcile_MysqlConfOverrides(t *testing.T) {
	pair := newTestPair()
	pair.Spec.MysqlConf = map[string]string{
		"max-connections": "1000",
		"custom-setting":  "value",
	}
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-config", Namespace: "shared-lion",
	}, &cm); err != nil {
		t.Fatalf("configmap not found: %v", err)
	}

	myCnf := cm.Data["my.cnf"]
	if !strings.Contains(myCnf, "max-connections=1000") {
		t.Error("override max-connections=1000 should be present")
	}
	if strings.Contains(myCnf, "max-connections=500") {
		t.Error("default max-connections=500 should be overridden")
	}
	if !strings.Contains(myCnf, "custom-setting=value") {
		t.Error("custom-setting=value should be present")
	}
}

func TestReconcile_DefaultImage(t *testing.T) {
	pair := newTestPair()
	pair.Spec.Image = "" // should default to mysql:9.6
	r, c := newReconciler(pair)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var deploy appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-dc1", Namespace: "shared-lion",
	}, &deploy); err != nil {
		t.Fatalf("deployment not found: %v", err)
	}

	if deploy.Spec.Template.Spec.Containers[0].Image != "mysql:9.6" {
		t.Errorf("expected default image mysql:9.6, got %s", deploy.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestCRConfigToTopologyConfig(t *testing.T) {
	pair := newTestPair()
	tc := CRConfigToTopologyConfig(pair)

	if tc.AZ != "lion" {
		t.Errorf("expected AZ 'lion', got %s", tc.AZ)
	}
	if tc.DC1.Name != "dc1" {
		t.Errorf("expected DC1 name 'dc1', got %s", tc.DC1.Name)
	}
	if tc.DC2.Name != "dc2" {
		t.Errorf("expected DC2 name 'dc2', got %s", tc.DC2.Name)
	}
	if tc.FailureThreshold != 3 {
		t.Errorf("expected failure threshold 3, got %d", tc.FailureThreshold)
	}
	if tc.RecoveryThreshold != 2 {
		t.Errorf("expected recovery threshold 2, got %d", tc.RecoveryThreshold)
	}
	if time.Duration(tc.PollInterval) != 2*time.Second {
		t.Errorf("expected poll interval 2s, got %v", time.Duration(tc.PollInterval))
	}
}

func TestCRConfigToTopologyConfig_Defaults(t *testing.T) {
	pair := newTestPair()
	pair.Spec.PollInterval = nil
	pair.Spec.FailureThreshold = 0
	pair.Spec.RecoveryThreshold = 0
	tc := CRConfigToTopologyConfig(pair)

	if tc.FailureThreshold != 3 {
		t.Errorf("expected default failure threshold 3, got %d", tc.FailureThreshold)
	}
	if tc.RecoveryThreshold != 2 {
		t.Errorf("expected default recovery threshold 2, got %d", tc.RecoveryThreshold)
	}
	if time.Duration(tc.PollInterval) != 2*time.Second {
		t.Errorf("expected default poll interval 2s, got %v", time.Duration(tc.PollInterval))
	}
}

func TestGenerateMyCnf(t *testing.T) {
	pair := newTestPair()
	cnf := generateMyCnf(pair)

	if !strings.HasPrefix(cnf, "[mysqld]\n") {
		t.Error("my.cnf should start with [mysqld]")
	}

	// Verify sorted output
	lines := strings.Split(strings.TrimSpace(cnf), "\n")
	if len(lines) < 10 {
		t.Errorf("expected at least 10 config lines, got %d", len(lines))
	}

	// Check first line after [mysqld] is alphabetically before the last
	if len(lines) > 2 {
		if lines[1] > lines[len(lines)-1] {
			t.Error("config lines should be sorted alphabetically")
		}
	}
}
