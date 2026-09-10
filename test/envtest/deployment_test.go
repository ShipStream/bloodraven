//go:build envtest

package envtest

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestDeploymentClientsValidation(t *testing.T) {
	ns := createNamespace(t, "deploy-clients")
	entry := func(namespace, account string) map[string]any {
		return map[string]any{"namespace": namespace, "serviceAccount": account}
	}
	// A 253-character DNS subdomain whose labels each stay within 63 characters.
	maxServiceAccount := strings.Join([]string{
		strings.Repeat("b", 63), strings.Repeat("b", 63), strings.Repeat("b", 63), strings.Repeat("b", 61),
	}, ".")
	tests := []struct {
		name    string
		clients []any
		valid   bool
	}{
		{"omitted", nil, true},
		{"empty list", []any{}, true},
		{"valid", []any{entry("tenant-acme", "acme.deploy")}, true},
		{"single character", []any{entry("1", "2")}, true},
		{"maximum lengths", []any{entry(strings.Repeat("a", 63), maxServiceAccount)}, true},
		{"maximum service account label", []any{entry("tenant", strings.Repeat("b", 63))}, true},
		{"distinct namespaces", []any{entry("tenant-acme", "deploy"), entry("tenant-beta", "deploy")}, true},
		{"missing namespace", []any{map[string]any{"serviceAccount": "deploy"}}, false},
		{"missing service account", []any{map[string]any{"namespace": "tenant"}}, false},
		{"empty namespace", []any{entry("", "deploy")}, false},
		{"empty service account", []any{entry("tenant", "")}, false},
		{"long namespace", []any{entry(strings.Repeat("a", 64), "deploy")}, false},
		{"long service account", []any{entry("tenant", strings.Repeat("a", 254))}, false},
		// Kubernetes caps each DNS-subdomain label at 63 characters.
		{"long service account label", []any{entry("tenant", strings.Repeat("b", 64))}, false},
		{"long service account interior label", []any{entry("tenant", "deploy."+strings.Repeat("b", 64)+".sa")}, false},
		{"namespace subdomain", []any{entry("tenant.acme", "deploy")}, false},
		{"namespace uppercase", []any{entry("Tenant", "deploy")}, false},
		{"namespace leading hyphen", []any{entry("-tenant", "deploy")}, false},
		{"namespace trailing hyphen", []any{entry("tenant-", "deploy")}, false},
		{"service account uppercase", []any{entry("tenant", "Deploy")}, false},
		{"service account underscore", []any{entry("tenant", "deploy_sa")}, false},
		{"service account identity", []any{entry("tenant", "system:serviceaccount:tenant:deploy")}, false},
		{"service account empty label", []any{entry("tenant", "deploy..sa")}, false},
		{"service account leading hyphen", []any{entry("tenant", "-deploy")}, false},
		{"service account trailing dot", []any{entry("tenant", "deploy.")}, false},
		{"duplicate namespace", []any{entry("tenant", "deploy"), entry("tenant", "other")}, false},
		{"duplicate identity", []any{entry("tenant", "deploy"), entry("tenant", "deploy")}, false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := newMysqlDatabaseCR(ns, fmt.Sprintf("tenant-%d", i))
			obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cr)
			if err != nil {
				t.Fatal(err)
			}
			u := &unstructured.Unstructured{Object: obj}
			u.SetAPIVersion("shipstream.io/v1alpha1")
			u.SetKind("MysqlDatabase")
			if tt.clients != nil {
				if err := unstructured.SetNestedSlice(u.Object, tt.clients, "spec", "deploymentClients"); err != nil {
					t.Fatal(err)
				}
			}
			err = k8sClient.Create(ctx, u)
			if !tt.valid {
				if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "deploymentClients") {
					t.Fatalf("create error = %v, want invalid deploymentClients", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(ctx, u) })
			var got v1alpha1.MysqlDatabase
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(u), &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Spec.DeploymentClients) != len(tt.clients) {
				t.Fatalf("clients pruned: %v", got.Spec.DeploymentClients)
			}
			for j, c := range got.Spec.DeploymentClients {
				if !reflect.DeepEqual(entry(c.Namespace, c.ServiceAccount), tt.clients[j]) {
					t.Fatalf("client = %+v, want %v", c, tt.clients[j])
				}
			}
		})
	}
}

func TestDeploymentPrivilegesAdmission(t *testing.T) {
	ns := createNamespace(t, "deploy-privileges")
	for _, field := range []string{"owner", "users", "grants"} {
		t.Run(field, func(t *testing.T) {
			cr := newMysqlDatabaseCR(ns, "tenant-"+field)
			privs := []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeCreateTemporaryTables, v1alpha1.PrivilegeCreateView}
			switch field {
			case "owner":
				cr.Spec.Owner.Privileges = privs
			case "users":
				cr.Spec.Users = []v1alpha1.MysqlDatabaseUser{{SecretName: "app", Privileges: privs}}
			case "grants":
				cr.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "reporting", Privileges: privs}}
			}
			if err := k8sClient.Create(ctx, cr); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })
			var got v1alpha1.MysqlDatabase
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), &got); err != nil {
				t.Fatal(err)
			}
			var actual []v1alpha1.MysqlPrivilege
			switch field {
			case "owner":
				actual = got.Spec.Owner.Privileges
			case "users":
				actual = got.Spec.Users[0].Privileges
			case "grants":
				actual = got.Spec.Grants[0].Privileges
			}
			if !reflect.DeepEqual(actual, privs) {
				t.Fatalf("privileges = %v, want %v", actual, privs)
			}
			for _, invalid := range []v1alpha1.MysqlPrivilege{"CREATE VIEW WITH GRANT OPTION", "CREATE TEMPORARY TABLES; GRANT ALL"} {
				actual[1] = invalid
				if err := k8sClient.Update(ctx, &got); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "privileges") {
					t.Fatalf("invalid privilege %q: error = %v", invalid, err)
				}
			}
		})
	}
}

func TestTopologyGenerationStatusPersistence(t *testing.T) {
	ns := createNamespace(t, "deploy-generation")
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, fg) })
	if fg.Status.TopologyGeneration != 0 {
		t.Fatalf("initial generation = %d", fg.Status.TopologyGeneration)
	}

	// A spec writer cannot forge the generation through the main endpoint.
	fg.Status.TopologyGeneration = 99
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.TopologyGeneration != 0 {
		t.Fatal("main resource update changed status")
	}
	if err := k8sClient.Status().Patch(ctx, fg, client.RawPatch(types.MergePatchType, []byte(`{"status":{"topologyGeneration":0}}`))); err != nil {
		t.Fatalf("explicit zero generation: %v", err)
	}
	fg.Status.TopologyGeneration = -1
	if err := k8sClient.Status().Update(ctx, fg); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "topologyGeneration") {
		t.Fatalf("negative generation: error = %v", err)
	}
	const generation int64 = 1 << 40
	fg.Status.TopologyGeneration = generation
	fg.Status.ActiveSite = "dc1"
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}

	// Restart controller-runtime managers/caches, not the API server: status
	// must reload from storage without process-local state or precision loss.
	for range 2 {
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
		if err != nil {
			t.Fatal(err)
		}
		runCtx, stop := context.WithTimeout(ctx, 15*time.Second)
		done := make(chan error, 1)
		go func() { done <- mgr.Start(runCtx) }()
		var got v1alpha1.MysqlFailoverGroup
		if mgr.GetCache().WaitForCacheSync(runCtx) {
			err = mgr.GetClient().Get(runCtx, client.ObjectKeyFromObject(fg), &got)
		} else {
			err = fmt.Errorf("manager cache did not start: %w", runCtx.Err())
		}
		stop()
		if startErr := <-done; startErr != nil {
			t.Fatalf("manager: %v", startErr)
		}
		if err != nil {
			t.Fatal(err)
		}
		if got.Status.TopologyGeneration != generation || got.Status.ActiveSite != "dc1" {
			t.Fatalf("status after manager restart = %+v", got.Status)
		}
	}
}
