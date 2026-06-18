package controller

import (
	"context"
	"io"
	"log/slog"
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
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
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
			Image: "mysql:9.7",
			Sites: []v1alpha1.SiteSpec{
				{
					Name:              "dc1",
					Zone:              "lion-dc1",
					LBIP:              "203.0.113.1",
					TaintNodeSelector: map[string]string{"shipstream.io/failover-group.lion": "true", "shipstream.io/site.lion": "dc1"},
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
					Name:              "dc2",
					Zone:              "lion-dc2",
					LBIP:              "203.0.113.2",
					TaintNodeSelector: map[string]string{"shipstream.io/failover-group.lion": "true", "shipstream.io/site.lion": "dc2"},
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

	myCnf, ok := cm.Data["bloodraven.cnf"]
	if !ok {
		t.Fatal("bloodraven.cnf not found in configmap data")
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
		if containers[0].Image != "mysql:9.7" {
			t.Errorf("deployment %s: expected image mysql:9.7, got %s", dc, containers[0].Image)
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

		// Verify per-group taint toleration key
		tolerations := deploy.Spec.Template.Spec.Tolerations
		expectedKey := platform.TaintKeyForGroup("lion")
		found := false
		for _, tol := range tolerations {
			if tol.Key == expectedKey && tol.Operator == corev1.TolerationOpExists && tol.Effect == "" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("deployment %s: expected toleration for %s, got %v", dc, expectedKey, tolerations)
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

	myCnf := cm.Data["bloodraven.cnf"]
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

	myCnf := cm.Data["bloodraven.cnf"]
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
	fg.Spec.Image = "" // should default to mysql:9.7
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

	if deploy.Spec.Template.Spec.Containers[0].Image != "mysql:9.7" {
		t.Errorf("expected default image mysql:9.7, got %s", deploy.Spec.Template.Spec.Containers[0].Image)
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
	if tc.Sites[0].TaintSelector != "shipstream.io/failover-group.lion=true,shipstream.io/site.lion=dc1" {
		t.Errorf("expected dc1 taint selector, got %q", tc.Sites[0].TaintSelector)
	}
	if tc.Sites[1].TaintSelector != "shipstream.io/failover-group.lion=true,shipstream.io/site.lion=dc2" {
		t.Errorf("expected dc2 taint selector, got %q", tc.Sites[1].TaintSelector)
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
	tainter.Taints["shipstream.io/failover-group.lion=true,shipstream.io/site.lion=dc1"] = true
	tainter.Taints["shipstream.io/failover-group.lion=true,shipstream.io/site.lion=dc2"] = true

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
	if tainter.IsTainted("shipstream.io/failover-group.lion=true,shipstream.io/site.lion=dc1") {
		t.Error("expected taint to be removed for dc1")
	}
	if tainter.IsTainted("shipstream.io/failover-group.lion=true,shipstream.io/site.lion=dc2") {
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
		return
	}
	if *tgps != 60 {
		t.Errorf("expected TerminationGracePeriodSeconds=60, got %d", *tgps)
	}

	// Verify MySQL container has a pre-stop hook that sets super_read_only.
	mysqlContainer := deploy.Spec.Template.Spec.Containers[0]
	if mysqlContainer.Lifecycle == nil || mysqlContainer.Lifecycle.PreStop == nil {
		t.Fatal("expected MySQL container to have a PreStop lifecycle hook")
	}
	hook := mysqlContainer.Lifecycle.PreStop
	if hook.Exec == nil {
		t.Fatal("expected PreStop hook to use exec")
	}
	cmd := strings.Join(hook.Exec.Command, " ")
	if !strings.Contains(cmd, "super_read_only") {
		t.Errorf("expected PreStop command to set super_read_only, got: %s", cmd)
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

func TestGenerateMyCnf_PITRInjectsMaxBinlogSize(t *testing.T) {
	fg := newTestFG()
	if fg.Spec.Backup == nil {
		fg.Spec.Backup = &v1alpha1.BackupSpec{}
	}
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "p", MaxBinlogSize: "128M"}
	cnf := generateMyCnf(fg)
	if !strings.Contains(cnf, "max-binlog-size=128M") {
		t.Errorf("my.cnf should contain max-binlog-size=128M when PITR is enabled; got:\n%s", cnf)
	}
}

func TestGenerateMyCnf_PITRDefaultMaxBinlogSize(t *testing.T) {
	fg := newTestFG()
	if fg.Spec.Backup == nil {
		fg.Spec.Backup = &v1alpha1.BackupSpec{}
	}
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "p"}
	cnf := generateMyCnf(fg)
	// Default when MaxBinlogSize is unset should be 100M per the PITR design.
	if !strings.Contains(cnf, "max-binlog-size=100M") {
		t.Errorf("my.cnf should contain default max-binlog-size=100M; got:\n%s", cnf)
	}
}

func TestGenerateMyCnf_PITRDisabledLeavesDefault(t *testing.T) {
	fg := newTestFG()
	// With PITR off, MySQL's own default (1 GB) applies — we must not
	// emit the knob ourselves.
	cnf := generateMyCnf(fg)
	if strings.Contains(cnf, "max-binlog-size=") {
		t.Errorf("my.cnf should not set max-binlog-size when PITR is disabled; got:\n%s", cnf)
	}
}

func TestReconcile_TLSSecretHashTriggersRollout(t *testing.T) {
	fg := newTestFG()
	fg.Spec.TLS = &v1alpha1.TLSSpec{
		IssuerRef:  v1alpha1.IssuerRef{Name: "letsencrypt", Kind: "ClusterIssuer"},
		SecretName: "mysql-tls",
	}
	tlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-tls", Namespace: "shared-lion"},
		Data: map[string][]byte{
			"tls.crt": []byte("cert-v1"),
			"tls.key": []byte("key-v1"),
			"ca.crt":  []byte("ca-v1"),
		},
	}
	r, c := newReconciler(fg, tlsSecret)

	// First reconcile — capture the spec hash.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var deploy appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-dc1", Namespace: "shared-lion",
	}, &deploy); err != nil {
		t.Fatalf("deployment not found: %v", err)
	}
	hashBefore := deploy.Spec.Template.Annotations[specHashAnnotation]
	if hashBefore == "" {
		t.Fatal("expected spec-hash annotation to be set")
	}

	// Simulate cert rotation by fetching a fresh copy and updating.
	var rotatedSecret corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-tls", Namespace: "shared-lion",
	}, &rotatedSecret); err != nil {
		t.Fatalf("get TLS secret before update: %v", err)
	}
	rotatedSecret.Data["tls.crt"] = []byte("cert-v2")
	if err := c.Update(context.Background(), &rotatedSecret); err != nil {
		t.Fatalf("update TLS secret: %v", err)
	}

	// Reconcile again — hash should change.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("reconcile after cert rotation failed: %v", err)
	}

	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-dc1", Namespace: "shared-lion",
	}, &deploy); err != nil {
		t.Fatalf("deployment not found after re-reconcile: %v", err)
	}
	hashAfter := deploy.Spec.Template.Annotations[specHashAnnotation]
	if hashAfter == hashBefore {
		t.Error("expected spec-hash to change after TLS certificate rotation")
	}
}

func TestComputeSpecHash_StableWithoutTLS(t *testing.T) {
	fg := newTestFG()
	site := fg.Spec.Sites[0]
	h1 := ComputeSpecHash(fg, site, nil, nil)
	h2 := ComputeSpecHash(fg, site, nil, nil)
	if h1 != h2 {
		t.Errorf("hash should be stable: got %s and %s", h1, h2)
	}

	// Adding TLS data should change the hash.
	h3 := ComputeSpecHash(fg, site, map[string][]byte{"tls.crt": []byte("cert")}, nil)
	if h3 == h1 {
		t.Error("hash should differ when TLS data is provided")
	}
}

func TestComputeSpecHash_IncludesCredentialData(t *testing.T) {
	fg := newTestFG()
	site := fg.Spec.Sites[0]
	h1 := ComputeSpecHash(fg, site, nil, nil)

	credData := map[string]map[string][]byte{
		"op-secret": {"username": []byte("admin"), "password": []byte("pass1")},
	}
	h2 := ComputeSpecHash(fg, site, nil, credData)
	if h2 == h1 {
		t.Error("hash should differ when credential data is provided")
	}

	credData2 := map[string]map[string][]byte{
		"op-secret": {"username": []byte("admin"), "password": []byte("pass2")},
	}
	h3 := ComputeSpecHash(fg, site, nil, credData2)
	if h3 == h2 {
		t.Error("hash should differ when password changes")
	}
}

func TestReconcileDeployment_UsesStableRelayLogNames(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{Name: "mysql-lion-dc1", Namespace: fg.Namespace}, &d); err != nil {
		t.Fatalf("deployment: %v", err)
	}
	args := strings.Join(d.Spec.Template.Spec.Containers[0].Args, "\n")
	for _, want := range []string{
		"--relay-log=/var/lib/mysql/mysql-relay-bin",
		"--relay-log-index=/var/lib/mysql/mysql-relay-bin.index",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("mysql args missing %q; args:\n%s", want, args)
		}
	}
}

func TestComputeSpecHash_IncludesPITRProfileStorage(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly"},
		Profiles: []v1alpha1.BackupProfile{{
			Name: "nightly",
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3,
				S3: &v1alpha1.S3Storage{
					Bucket:            "backups",
					Prefix:            "pitr/a",
					Region:            "us-east-1",
					EndpointURL:       "https://s3.example.com",
					CredentialsSecret: "aws-creds",
				},
			},
		}},
	}
	site := fg.Spec.Sites[0]
	h1 := ComputeSpecHash(fg, site, nil, nil)

	fg2 := fg.DeepCopy()
	fg2.Spec.Backup.Profiles[0].Storage.S3.Prefix = "pitr/b"
	h2 := ComputeSpecHash(fg2, site, nil, nil)
	if h2 == h1 {
		t.Error("hash should differ when the PITR profile storage prefix changes")
	}

	fg3 := fg.DeepCopy()
	fg3.Spec.Backup.Profiles[0].Storage.S3.CredentialsSecret = "aws-creds-v2"
	h3 := ComputeSpecHash(fg3, site, nil, nil)
	if h3 == h1 {
		t.Error("hash should differ when the PITR profile credentials secret changes")
	}
}

func newTestFGWithCredentials() *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Spec.SecretName = ""
	fg.Spec.Credentials = &v1alpha1.CredentialsSpec{
		OperatorSecret: "mysql-operator-creds",
		AppSecret:      "mysql-app-creds",
		BackupSecret:   "mysql-backup-creds",
	}
	return fg
}

func newTestCredentialSecrets() []*corev1.Secret {
	return []*corev1.Secret{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mysql-operator-creds",
				Namespace: "shared-lion",
			},
			Data: map[string][]byte{
				"username":            []byte("bloodraven"),
				"password":            []byte("operator-pass"),
				"MYSQL_ROOT_PASSWORD": []byte("root-pass"),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mysql-app-creds",
				Namespace: "shared-lion",
			},
			Data: map[string][]byte{
				"username": []byte("app"),
				"password": []byte("app-pass"),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mysql-backup-creds",
				Namespace: "shared-lion",
			},
			Data: map[string][]byte{
				"username": []byte("backup"),
				"password": []byte("backup-pass"),
			},
		},
	}
}

func TestReconcile_CredentialsMode_CreatesDeployment(t *testing.T) {
	fg := newTestFGWithCredentials()
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, c := newReconciler(objs...)

	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}

	// First reconcile adds the finalizer.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	// Second reconcile creates resources.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var deploy appsv1.Deployment
	deployNN := types.NamespacedName{Name: "mysql-lion-dc1", Namespace: "shared-lion"}
	if err := c.Get(context.Background(), deployNN, &deploy); err != nil {
		t.Fatalf("get deployment: %v", err)
	}

	// Verify the MySQL container uses individual env vars, not envFrom.
	mysqlContainer := deploy.Spec.Template.Spec.Containers[0]
	if mysqlContainer.Name != "mysql" {
		t.Fatalf("expected mysql container, got %s", mysqlContainer.Name)
	}
	if len(mysqlContainer.EnvFrom) != 0 {
		t.Error("credentials mode should not use envFrom")
	}
	foundRootPw := false
	for _, e := range mysqlContainer.Env {
		if e.Name == "MYSQL_ROOT_PASSWORD" {
			foundRootPw = true
			if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
				t.Error("MYSQL_ROOT_PASSWORD should be a secretKeyRef")
			} else if e.ValueFrom.SecretKeyRef.Name != "mysql-operator-creds" {
				t.Errorf("MYSQL_ROOT_PASSWORD should ref operator secret, got %s", e.ValueFrom.SecretKeyRef.Name)
			}
		}
	}
	if !foundRootPw {
		t.Error("expected MYSQL_ROOT_PASSWORD env var in credentials mode")
	}

	// Verify sidecar uses MYSQL_USER/MYSQL_PASSWORD.
	sidecar := deploy.Spec.Template.Spec.Containers[1]
	if sidecar.Name != "sidecar" {
		t.Fatalf("expected sidecar container, got %s", sidecar.Name)
	}
	foundUser := false
	for _, e := range sidecar.Env {
		if e.Name == "MYSQL_USER" {
			foundUser = true
		}
		if e.Name == "MYSQL_DSN" {
			t.Error("sidecar should not have MYSQL_DSN in credentials mode")
		}
	}
	if !foundUser {
		t.Error("expected MYSQL_USER env var in sidecar")
	}

	// Verify credential volumes exist.
	foundCredsOperator := false
	foundInitUsers := false
	for _, v := range deploy.Spec.Template.Spec.Volumes {
		if v.Name == "creds-operator" {
			foundCredsOperator = true
		}
		if v.Name == "init-users" {
			foundInitUsers = true
		}
	}
	if !foundCredsOperator {
		t.Error("expected creds-operator volume")
	}
	if !foundInitUsers {
		t.Error("expected init-users volume")
	}
}

func TestReconcile_CredentialsMode_CreatesInitUsersConfigMap(t *testing.T) {
	fg := newTestFGWithCredentials()
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, c := newReconciler(objs...)

	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}

	// First reconcile adds finalizer, second creates resources.
	r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var cm corev1.ConfigMap
	cmNN := types.NamespacedName{Name: "mysql-lion-init-users", Namespace: "shared-lion"}
	if err := c.Get(context.Background(), cmNN, &cm); err != nil {
		t.Fatalf("get init-users configmap: %v", err)
	}

	script, ok := cm.Data["01-bloodraven-users.sh"]
	if !ok {
		t.Fatal("missing init script key")
	}
	if !strings.Contains(script, "create_user_with_grants operator") {
		t.Error("init script should create operator user")
	}
	if !strings.Contains(script, "create_user_with_grants app") {
		t.Error("init script should create app user")
	}
	if !strings.Contains(script, "create_user_with_grants backup") {
		t.Error("init script should create backup user")
	}
}

func TestSecretToFailoverGroup_CredentialsMode(t *testing.T) {
	fg := newTestFGWithCredentials()
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, _ := newReconciler(objs...)

	tests := []struct {
		secretName string
		wantMatch  bool
	}{
		{"mysql-operator-creds", true},
		{"mysql-app-creds", true},
		{"mysql-backup-creds", true},
		{"unrelated-secret", false},
	}

	for _, tt := range tests {
		t.Run(tt.secretName, func(t *testing.T) {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.secretName,
					Namespace: "shared-lion",
				},
			}
			requests := r.secretToFailoverGroup(context.Background(), secret)
			if tt.wantMatch && len(requests) == 0 {
				t.Error("expected match")
			}
			if !tt.wantMatch && len(requests) > 0 {
				t.Error("expected no match")
			}
		})
	}
}

func TestBuildSiteDSNFromCreds(t *testing.T) {
	fg := newTestFG()
	dsn := buildSiteDSNFromCreds("myuser", "mypass", fg, fg.Spec.Sites[0])
	if !strings.Contains(dsn, "myuser:mypass@") {
		t.Errorf("DSN should contain credentials: %s", dsn)
	}
	if !strings.Contains(dsn, "mysql-lion-dc1.shared-lion.svc.cluster.local") {
		t.Errorf("DSN should contain site host: %s", dsn)
	}
}

// TestReconcile_DefersDeploymentUpdateWhenManagerRunning verifies that when a
// topology manager is registered for a CR, the reconciler skips per-site
// Deployment updates so the ordered update path can pick them up via
// checkSpecDrift. This prevents the "both Deployments restart simultaneously"
// race that causes a TOTAL LOSS window during rolling updates.
func TestReconcile_DefersDeploymentUpdateWhenManagerRunning(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	r, c := newReconciler(fg)
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}

	// Initial reconcile creates Deployments via the fast path (no manager).
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}

	captureSite := func(site string) (string, string) {
		t.Helper()
		var d appsv1.Deployment
		if err := c.Get(ctx, types.NamespacedName{Name: "mysql-lion-" + site, Namespace: "shared-lion"}, &d); err != nil {
			t.Fatalf("get deployment %s: %v", site, err)
		}
		if len(d.Spec.Template.Spec.Containers) == 0 {
			t.Fatalf("deployment %s has no containers", site)
		}
		return d.Annotations[specHashAnnotation], d.Spec.Template.Spec.Containers[0].Image
	}

	hashDC1Before, imgDC1Before := captureSite("dc1")
	hashDC2Before, imgDC2Before := captureSite("dc2")
	if hashDC1Before == "" || hashDC2Before == "" {
		t.Fatal("expected initial spec hash on Deployment annotations")
	}

	// Wire a runner with a registered manager so HasManager returns true.
	discardLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := &TopologyManagerRunner{
		client:   c,
		logger:   discardLog,
		managers: make(map[types.NamespacedName]*managedTopology),
	}
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	fc := NewFailoverController(discardLog)
	cfg := testTopologyConfig()
	tm := NewTopologyManager(cfg, []internalmysql.Checker{site0, site1}, fc, nil, nil, BootstrapConfig{},
		newMockTainter(), platform.NewHub(discardLog), &mockDNS{}, discardLog)
	runner.managers[nn] = &managedTopology{tm: tm, cancel: func() {}}
	r.Runner = runner

	// Mutate spec to trigger drift.
	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &fresh); err != nil {
		t.Fatalf("get CR for mutation: %v", err)
	}
	fresh.Spec.Image = "mysql:9.7.1"
	if err := c.Update(ctx, &fresh); err != nil {
		t.Fatalf("update CR: %v", err)
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	// Both Deployments must be untouched by the reconciler.
	if h, img := captureSite("dc1"); h != hashDC1Before || img != imgDC1Before {
		t.Errorf("dc1 deployment was modified: hash %s->%s, image %s->%s",
			hashDC1Before, h, imgDC1Before, img)
	}
	if h, img := captureSite("dc2"); h != hashDC2Before || img != imgDC2Before {
		t.Errorf("dc2 deployment was modified: hash %s->%s, image %s->%s",
			hashDC2Before, h, imgDC2Before, img)
	}

	// checkSpecDrift must surface both sites to the topology manager.
	runner.checkSpecDrift(ctx, &fresh, tm)
	tm.mu.RLock()
	drift := append([]string(nil), tm.specDriftSites...)
	tm.mu.RUnlock()
	if len(drift) != 2 {
		t.Fatalf("expected 2 drift sites, got %d: %v", len(drift), drift)
	}
	gotDC1, gotDC2 := false, false
	for _, s := range drift {
		switch s {
		case "dc1":
			gotDC1 = true
		case "dc2":
			gotDC2 = true
		}
	}
	if !gotDC1 || !gotDC2 {
		t.Errorf("expected drift sites [dc1, dc2], got %v", drift)
	}
}

// TestReconcile_AppliesDeploymentUpdateWhenNoManager verifies that when no
// topology manager is running (initial deploy or pre-leader-election state),
// the reconciler still applies Deployment updates directly. This is the
// bootstrap path that must keep working even after the deferred-update change.
func TestReconcile_AppliesDeploymentUpdateWhenNoManager(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	r, c := newReconciler(fg)
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("initial reconcile failed: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &fresh); err != nil {
		t.Fatalf("get CR for mutation: %v", err)
	}
	fresh.Spec.Image = "mysql:9.7.1"
	if err := c.Update(ctx, &fresh); err != nil {
		t.Fatalf("update CR: %v", err)
	}

	// No runner, no manager → reconciler must apply the update.
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	var d appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Name: "mysql-lion-dc1", Namespace: "shared-lion"}, &d); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if d.Spec.Template.Spec.Containers[0].Image != "mysql:9.7.1" {
		t.Errorf("expected image mysql:9.7.1 after reconcile (no manager), got %s",
			d.Spec.Template.Spec.Containers[0].Image)
	}
}

// TestWaitForDeploymentRollout_ReadyReturnsImmediately verifies the wait
// returns without error when the Deployment's status already matches "rolled
// out" (ObservedGeneration caught up, desired replicas updated and available).
func TestWaitForDeploymentRollout_ReadyReturnsImmediately(t *testing.T) {
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mysql-lion-dc1",
			Namespace:  "shared-lion",
			Generation: 3,
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 3,
			Replicas:           1,
			UpdatedReplicas:    1,
			AvailableReplicas:  1,
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dep).Build()
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: scheme}

	nn := types.NamespacedName{Namespace: "shared-lion", Name: "mysql-lion-dc1"}
	start := time.Now()
	if err := r.waitForDeploymentRollout(context.Background(), nn, 5*time.Second); err != nil {
		t.Fatalf("waitForDeploymentRollout should return nil when rollout is complete: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("waitForDeploymentRollout should return immediately when ready, took %v", d)
	}
}

// TestWaitForDeploymentRollout_StaleGenerationTimesOut verifies the wait
// does not falsely succeed when the Deployment controller hasn't yet observed
// the latest generation — this is the shape that causes the rolling-update
// race (old pod serves the Service while the new pod is still starting).
func TestWaitForDeploymentRollout_StaleGenerationTimesOut(t *testing.T) {
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mysql-lion-dc1",
			Namespace:  "shared-lion",
			Generation: 3,
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 2,
			Replicas:           1,
			UpdatedReplicas:    0,
			AvailableReplicas:  0,
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dep).Build()
	r := &MysqlFailoverGroupReconciler{
		Client:              c,
		Scheme:              scheme,
		rolloutPollInterval: 10 * time.Millisecond,
	}

	nn := types.NamespacedName{Namespace: "shared-lion", Name: "mysql-lion-dc1"}
	err := r.waitForDeploymentRollout(context.Background(), nn, 100*time.Millisecond)
	if err == nil {
		t.Fatal("waitForDeploymentRollout should time out when rollout incomplete")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("expected timeout error, got: %v", err)
	}
}

// TestWaitForDeploymentRollout_UpdatedButNotAvailable verifies a Deployment
// whose new ReplicaSet has been created but whose pods aren't Ready yet (i.e.
// MySQL is still starting up) does NOT count as a complete rollout. Letting
// this satisfy the wait would recreate the bug where the ordered update
// proceeds to failover against a half-started pod.
func TestWaitForDeploymentRollout_UpdatedButNotAvailable(t *testing.T) {
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mysql-lion-dc1",
			Namespace:  "shared-lion",
			Generation: 1,
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			UpdatedReplicas:    1,
			AvailableReplicas:  0,
		},
	}
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dep).Build()
	r := &MysqlFailoverGroupReconciler{
		Client:              c,
		Scheme:              scheme,
		rolloutPollInterval: 10 * time.Millisecond,
	}

	nn := types.NamespacedName{Namespace: "shared-lion", Name: "mysql-lion-dc1"}
	if err := r.waitForDeploymentRollout(context.Background(), nn, 100*time.Millisecond); err == nil {
		t.Fatal("waitForDeploymentRollout must not return nil while AvailableReplicas < desired")
	}
}
