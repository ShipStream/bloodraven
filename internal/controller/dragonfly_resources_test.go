package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/platform"
)

// fgWithDragonfly returns a baseline failover group with Dragonfly enabled
// and one auth-less default config.
func fgWithDragonfly(opts ...func(*v1alpha1.MysqlFailoverGroup)) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{
		Enabled:         true,
		Image:           "docker.dragonflydb.io/dragonflydb/dragonfly:v1.25.5",
		Port:            6379,
		AdminPort:       9999,
		MaxMemoryMb:     256,
		ProactorThreads: 2,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
		},
	}
	for _, o := range opts {
		o(fg)
	}
	return fg
}

func TestReconcileDragonflyStatefulSet_CreatesPerSite(t *testing.T) {
	fg := fgWithDragonfly()
	r, c := newReconciler(fg)
	ctx := context.Background()
	if err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("reconcileDragonflyResources: %v", err)
	}

	for _, siteName := range []string{"dc1", "dc2"} {
		var sts appsv1.StatefulSet
		if err := c.Get(ctx, types.NamespacedName{Name: "lion-dragonfly-" + siteName, Namespace: "shared-lion"}, &sts); err != nil {
			t.Fatalf("statefulset for %s not created: %v", siteName, err)
		}
		if sts.Spec.Template.Spec.Containers[0].Image != fg.Spec.Dragonfly.Image {
			t.Errorf("%s: image = %q, want %q", siteName, sts.Spec.Template.Spec.Containers[0].Image, fg.Spec.Dragonfly.Image)
		}
		// NodeSelector pinned to site zone.
		if got := sts.Spec.Template.Spec.NodeSelector["topology.kubernetes.io/zone"]; got != "lion-"+siteName {
			t.Errorf("%s: nodeSelector zone = %q, want lion-%s", siteName, got, siteName)
		}
		// Tolerates the failover group's db-readonly taint.
		taintKey := platform.TaintKeyForGroup(fg.Name)
		found := false
		for _, tol := range sts.Spec.Template.Spec.Tolerations {
			if tol.Key == taintKey && tol.Operator == corev1.TolerationOpExists {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s: missing toleration for taint %q", siteName, taintKey)
		}
		// Pod label includes operator-assigned dragonfly-role=replica.
		if got := sts.Spec.Template.ObjectMeta.Labels[labelDragonflyRole]; got != "replica" {
			t.Errorf("%s: dragonfly-role label = %q, want replica", siteName, got)
		}
		// Owner reference back to the failover group.
		if len(sts.OwnerReferences) == 0 || sts.OwnerReferences[0].Kind != "MysqlFailoverGroup" {
			t.Errorf("%s: missing owner reference", siteName)
		}
		// Container args carry the operator-managed flags, including the
		// load-bearing --break_replication_on_master_restart guard.
		args := sts.Spec.Template.Spec.Containers[0].Args
		joined := joinArgs(args)
		for _, want := range []string{
			"--port=6379",
			"--admin_port=9999",
			"--maxmemory=256mb",
			"--proactor_threads=2",
			"--break_replication_on_master_restart",
		} {
			if !contains(joined, want) {
				t.Errorf("%s: args missing %q (got %v)", siteName, want, args)
			}
		}
	}
}

func TestReconcileDragonflyStatefulSet_AuthEnvWiredFromSecret(t *testing.T) {
	fg := fgWithDragonfly(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Dragonfly.Auth = &v1alpha1.DragonflyAuthSpec{
			SecretName:  "lion-dragonfly-auth",
			PasswordKey: "password",
		}
	})
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflyStatefulSet(context.Background(), fg, fg.Spec.Sites[0]); err != nil {
		t.Fatalf("reconcileDragonflyStatefulSet: %v", err)
	}

	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-dc1", Namespace: "shared-lion"}, &sts); err != nil {
		t.Fatalf("statefulset not created: %v", err)
	}
	env := sts.Spec.Template.Spec.Containers[0].Env
	var found *corev1.EnvVar
	for i, e := range env {
		if e.Name == DragonflyAuthEnvVar {
			found = &env[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("env var %s not wired", DragonflyAuthEnvVar)
	}
	if found.ValueFrom == nil || found.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("env var %s missing SecretKeyRef", DragonflyAuthEnvVar)
	}
	if found.ValueFrom.SecretKeyRef.LocalObjectReference.Name != "lion-dragonfly-auth" {
		t.Errorf("env secretRef = %q, want lion-dragonfly-auth", found.ValueFrom.SecretKeyRef.Name)
	}
	if found.ValueFrom.SecretKeyRef.Key != "password" {
		t.Errorf("env secretKey = %q, want password", found.ValueFrom.SecretKeyRef.Key)
	}
	// --requirepass is wired to expand the env var.
	args := sts.Spec.Template.Spec.Containers[0].Args
	joined := joinArgs(args)
	if !contains(joined, "--requirepass=$("+DragonflyAuthEnvVar+")") {
		t.Errorf("args missing requirepass wiring (got %v)", args)
	}
}

func TestReconcileDragonflySiteService(t *testing.T) {
	fg := fgWithDragonfly()
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflySiteService(context.Background(), fg, fg.Spec.Sites[0]); err != nil {
		t.Fatalf("reconcileDragonflySiteService: %v", err)
	}

	var svc corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-dc1", Namespace: "shared-lion"}, &svc); err != nil {
		t.Fatalf("site service not created: %v", err)
	}
	wantSel := map[string]string{
		labelAppName:  dragonflyAppName,
		labelInstance: "lion",
		labelSite:     "dc1",
	}
	for k, v := range wantSel {
		if got := svc.Spec.Selector[k]; got != v {
			t.Errorf("selector[%s] = %q, want %q", k, got, v)
		}
	}
	// Selector must NOT carry dragonfly-role: per-site Service is for
	// debugging and replication wiring, not for app traffic.
	if _, ok := svc.Spec.Selector[labelDragonflyRole]; ok {
		t.Errorf("site Service selector unexpectedly includes dragonfly-role")
	}
}

func TestReconcileDragonflyActiveService_SelectsMasterRole(t *testing.T) {
	fg := fgWithDragonfly()
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflyActiveService(context.Background(), fg); err != nil {
		t.Fatalf("reconcileDragonflyActiveService: %v", err)
	}
	var svc corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly", Namespace: "shared-lion"}, &svc); err != nil {
		t.Fatalf("active service not created: %v", err)
	}
	// Selector includes dragonfly-role=master so endpoints follow the
	// label flip during failover.
	if got := svc.Spec.Selector[labelDragonflyRole]; got != "master" {
		t.Errorf("active service selector dragonfly-role = %q, want master", got)
	}
	// Selector does NOT pin a specific site (the active site is
	// determined by which pod carries the master label).
	if _, ok := svc.Spec.Selector[labelSite]; ok {
		t.Errorf("active service selector unexpectedly pins labelSite")
	}
}

func TestReconcileDragonflyResources_DisabledIsNoOp(t *testing.T) {
	fg := newTestFG() // no Dragonfly block
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflyResources(context.Background(), fg); err != nil {
		t.Fatalf("reconcileDragonflyResources: %v", err)
	}
	var sts appsv1.StatefulSet
	err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-dc1", Namespace: "shared-lion"}, &sts)
	if err == nil {
		t.Fatal("disabled config unexpectedly created a StatefulSet")
	}
}

func joinArgs(args []string) string {
	out := ""
	for _, a := range args {
		out += a + " "
	}
	return out
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
