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
	"github.com/shipstream/bloodraven/internal/testutil"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

func newTestFG() *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lion",
			Namespace: "shared-lion",
			UID:       "test-uid-123",
		},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Image: "mysql:9.6",
			Sites: []v1alpha1.SiteSpec{
				{
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
				{
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

// newTestSecret returns the secret that the reconciler expects to find.
func newTestSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mysql-credentials",
			Namespace: "shared-lion",
		},
		Data: map[string][]byte{
			"dsn": []byte("root:password@tcp(localhost:3306)/mysql"),
		},
	}
}

func newReconciler(objs ...client.Object) (*MysqlFailoverGroupReconciler, client.Client) {
	scheme := testScheme()
	// Always include the test secret so the reconciler's validation passes.
	allObjs := append([]client.Object{newTestSecret()}, objs...)
	cb := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(allObjs...)
	c := cb.Build()
	r := &MysqlFailoverGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	return r, c
}

func TestReconcile_CreatesConfigMap(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	r, _ := newReconciler(fg)
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
	fg := newTestFG()
	fg.Spec.TLS = &v1alpha1.TLSSpec{
		IssuerRef: v1alpha1.IssuerRef{
			Name: "letsencrypt",
			Kind: "ClusterIssuer",
		},
	}
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	fg.Spec.MysqlConf = map[string]string{
		"max-connections": "1000",
		"custom-setting":  "value",
	}
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	fg.Spec.Image = "" // should default to mysql:9.6
	r, c := newReconciler(fg)

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
	fg := newTestFG()
	tc := CRConfigToTopologyConfig(fg)

	if tc.Sites[0].Name != "dc1" {
		t.Errorf("expected Sites[0] name 'dc1', got %s", tc.Sites[0].Name)
	}
	if tc.Sites[1].Name != "dc2" {
		t.Errorf("expected Sites[1] name 'dc2', got %s", tc.Sites[1].Name)
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
	fg := newTestFG()
	fg.Spec.PollInterval = nil
	fg.Spec.FailureThreshold = 0
	fg.Spec.RecoveryThreshold = 0
	tc := CRConfigToTopologyConfig(fg)

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

func TestReconcile_AddsFinalizer(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatalf("get fg: %v", err)
	}

	found := false
	for _, f := range updated.Finalizers {
		if f == finalizerName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected finalizer %q to be present, got finalizers: %v", finalizerName, updated.Finalizers)
	}
}

func TestReconcile_GracefulShutdownOnDeletion(t *testing.T) {
	fg := newTestFG()
	fg.Finalizers = []string{finalizerName}
	now := metav1.Now()
	fg.DeletionTimestamp = &now

	tainter := testutil.NewFakeTainter()
	tainter.Taints["shipstream.io/failover-group=lion,shipstream.io/site=dc1"] = true
	tainter.Taints["shipstream.io/failover-group=lion,shipstream.io/site=dc2"] = true

	scheme := testScheme()
	allObjs := []client.Object{newTestSecret(), fg}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(allObjs...).Build()
	recorder := record.NewFakeRecorder(10)
	r := &MysqlFailoverGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
		Tainter:  tainter,
	}

	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify taints were removed for both sites
	if tainter.IsTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc1") {
		t.Error("expected taint to be removed for dc1")
	}
	if tainter.IsTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("expected taint to be removed for dc2")
	}

	// After removing the last finalizer on an object with DeletionTimestamp,
	// the fake client deletes the object, so a NotFound is expected.
	var fetched v1alpha1.MysqlFailoverGroup
	err = c.Get(context.Background(), nn, &fetched)
	if err == nil {
		for _, f := range fetched.Finalizers {
			if f == finalizerName {
				t.Error("finalizer should have been removed after graceful shutdown")
			}
		}
	}
}

func TestReconcile_DeletionWithoutFinalizer(t *testing.T) {
	fg := newTestFG()
	now := metav1.Now()
	fg.DeletionTimestamp = &now
	fg.Finalizers = []string{"some-other-finalizer"}

	r, _ := newReconciler(fg)
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if result.Requeue {
		t.Error("should not requeue on deletion without our finalizer")
	}
}

func TestReconcile_DeletionWithNilTainter(t *testing.T) {
	fg := newTestFG()
	fg.Finalizers = []string{finalizerName}
	now := metav1.Now()
	fg.DeletionTimestamp = &now

	scheme := testScheme()
	allObjs := []client.Object{newTestSecret(), fg}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(allObjs...).Build()
	r := &MysqlFailoverGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		Tainter:  nil,
	}

	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("reconcile should succeed even without tainter: %v", err)
	}

	var fetched2 v1alpha1.MysqlFailoverGroup
	err = c.Get(context.Background(), nn, &fetched2)
	if err == nil {
		for _, f := range fetched2.Finalizers {
			if f == finalizerName {
				t.Error("finalizer should have been removed")
			}
		}
	}
}

func TestReconcile_TerminationGracePeriod(t *testing.T) {
	fg := newTestFG()
	gracePeriod := int64(60)
	fg.Spec.TerminationGracePeriodSeconds = &gracePeriod
	r, c := newReconciler(fg)

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

	tgps := deploy.Spec.Template.Spec.TerminationGracePeriodSeconds
	if tgps == nil {
		t.Fatal("expected TerminationGracePeriodSeconds to be set")
	}
	if *tgps != 60 {
		t.Errorf("expected TerminationGracePeriodSeconds=60, got %d", *tgps)
	}
}

func TestGenerateMyCnf(t *testing.T) {
	fg := newTestFG()
	cnf := generateMyCnf(fg)

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

func TestGenerateMyCnf_NoCloneDDLTimeout(t *testing.T) {
	fg := newTestFG()
	// clone_ddl_timeout was removed in MySQL 9.x; it should not appear in my.cnf.
	// The CloneTimeout spec field is still used for the CLONE INSTANCE session timeout in bootstrap.go.
	cnf := generateMyCnf(fg)
	if strings.Contains(cnf, "clone_ddl_timeout") {
		t.Error("my.cnf should not contain clone_ddl_timeout (removed in MySQL 9.x)")
	}
}
