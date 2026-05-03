package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/platform"
)

// fgWithDragonfly returns a baseline failover group with Dragonfly enabled
// and one auth-less default config.
func fgWithDragonfly(opts ...func(*v1alpha1.MysqlFailoverGroup)) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{
		Enabled:         true,
		Image:           "docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0",
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
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
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

	var pdb policyv1.PodDisruptionBudget
	if err := c.Get(ctx, types.NamespacedName{Name: "lion-dragonfly", Namespace: "shared-lion"}, &pdb); err != nil {
		t.Fatalf("dragonfly PDB not created: %v", err)
	}
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntVal != 1 {
		t.Fatalf("dragonfly PDB minAvailable=%v, want 1", pdb.Spec.MinAvailable)
	}
	if got := pdb.Spec.Selector.MatchLabels[labelAppName]; got != dragonflyAppName {
		t.Errorf("dragonfly PDB selector app=%q, want %q", got, dragonflyAppName)
	}
	if got := pdb.Spec.Selector.MatchLabels[labelInstance]; got != fg.Name {
		t.Errorf("dragonfly PDB selector instance=%q, want %q", got, fg.Name)
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

func TestReconcileDragonflyStatefulSet_SnapshotS3WiresDirAndServiceAccount(t *testing.T) {
	fg := fgWithDragonfly(func(fg *v1alpha1.MysqlFailoverGroup) {
		useHTTPS := false
		signPayload := false
		fg.Spec.Dragonfly.Snapshot = &v1alpha1.DragonflySnapshotSpec{
			Dir:                   "s3://tenant-dragonfly/orders/prod",
			ServiceAccountName:    "dragonfly-backup",
			CredentialsSecretName: "dragonfly-s3",
			S3Endpoint:            "rustfs.bloodraven-playground.svc.cluster.local:9000",
			S3UseHTTPS:            &useHTTPS,
			S3SignPayload:         &signPayload,
		}
		fg.Spec.Dragonfly.Args = []string{"--dir=/tmp/ignored", "--s3_endpoint=ignored"}
	})
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflyStatefulSet(context.Background(), fg, fg.Spec.Sites[0]); err != nil {
		t.Fatalf("reconcileDragonflyStatefulSet: %v", err)
	}

	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-dc1", Namespace: "shared-lion"}, &sts); err != nil {
		t.Fatalf("statefulset not created: %v", err)
	}
	if got := sts.Spec.Template.Spec.ServiceAccountName; got != "dragonfly-backup" {
		t.Errorf("serviceAccountName=%q want dragonfly-backup", got)
	}
	joined := joinArgs(sts.Spec.Template.Spec.Containers[0].Args)
	if !contains(joined, "--dir=s3://tenant-dragonfly/orders/prod") {
		t.Errorf("args missing snapshot dir: %v", sts.Spec.Template.Spec.Containers[0].Args)
	}
	if contains(joined, "--dir=/tmp/ignored") {
		t.Errorf("user-supplied --dir overrode operator-owned snapshot dir: %v", sts.Spec.Template.Spec.Containers[0].Args)
	}
	for _, want := range []string{
		"--s3_endpoint=rustfs.bloodraven-playground.svc.cluster.local:9000",
		"--s3_use_https=false",
		"--s3_sign_payload=false",
	} {
		if !contains(joined, want) {
			t.Errorf("args missing %q: %v", want, sts.Spec.Template.Spec.Containers[0].Args)
		}
	}
	if contains(joined, "--s3_endpoint=ignored") {
		t.Errorf("user-supplied --s3_endpoint overrode operator-owned endpoint: %v", sts.Spec.Template.Spec.Containers[0].Args)
	}
	env := sts.Spec.Template.Spec.Containers[0].Env
	if !hasEnvFromSecret(env, "AWS_ACCESS_KEY_ID", "dragonfly-s3") || !hasEnvFromSecret(env, "AWS_SECRET_ACCESS_KEY", "dragonfly-s3") {
		t.Errorf("snapshot credentials env not wired from dragonfly-s3: %#v", env)
	}
}

func hasEnvFromSecret(env []corev1.EnvVar, name, secret string) bool {
	for _, e := range env {
		if e.Name == name && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil && e.ValueFrom.SecretKeyRef.Name == secret {
			return true
		}
	}
	return false
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

func TestReconcileDragonflyActiveService_SelectsMasterRoleAndTraffic(t *testing.T) {
	fg := fgWithDragonfly()
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflyActiveService(context.Background(), fg); err != nil {
		t.Fatalf("reconcileDragonflyActiveService: %v", err)
	}
	var svc corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly", Namespace: "shared-lion"}, &svc); err != nil {
		t.Fatalf("active service not created: %v", err)
	}
	// Selector AND-gates role=master AND traffic=enabled. Both labels
	// must be present so that removing the traffic label sheds an
	// endpoint atomically without depending on role-flip ordering.
	if got := svc.Spec.Selector[labelDragonflyRole]; got != "master" {
		t.Errorf("active service selector dragonfly-role = %q, want master", got)
	}
	if got := svc.Spec.Selector[labelDragonflyTraffic]; got != dragonflyTrafficEnabled {
		t.Errorf("active service selector dragonfly-traffic = %q, want %q", got, dragonflyTrafficEnabled)
	}
	// Selector does NOT pin a specific site (the active site is
	// determined by which pod carries the master label).
	if _, ok := svc.Spec.Selector[labelSite]; ok {
		t.Errorf("active service selector unexpectedly pins labelSite")
	}
}

func TestReconcileDragonflyStatefulSet_PodTemplateSeedsTrafficEnabled(t *testing.T) {
	fg := fgWithDragonfly()
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflyStatefulSet(context.Background(), fg, fg.Spec.Sites[0]); err != nil {
		t.Fatalf("reconcileDragonflyStatefulSet: %v", err)
	}
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-dc1", Namespace: "shared-lion"}, &sts); err != nil {
		t.Fatalf("statefulset not created: %v", err)
	}
	if got := sts.Spec.Template.ObjectMeta.Labels[labelDragonflyTraffic]; got != dragonflyTrafficEnabled {
		t.Errorf("pod template traffic label = %q, want %q", got, dragonflyTrafficEnabled)
	}
}

func TestReconcileDragonflyResources_DisabledIsNoOp(t *testing.T) {
	fg := newTestFG() // no Dragonfly block
	r, c := newReconciler(fg)
	if _, err := r.reconcileDragonflyResources(context.Background(), fg); err != nil {
		t.Fatalf("reconcileDragonflyResources: %v", err)
	}
	var sts appsv1.StatefulSet
	err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-dc1", Namespace: "shared-lion"}, &sts)
	if err == nil {
		t.Fatal("disabled config unexpectedly created a StatefulSet")
	}
}

// TestReconcileDragonflyResources_DisabledTearsDownPriorResources
// regression-tests B4: flipping spec.dragonfly.enabled=false used to
// be a no-op, leaving orphan StatefulSets and Services routing live
// traffic. Owner-ref GC only fires when the owning object is deleted,
// not when its .spec mutates. Verify reconcileDragonflyResources now
// actively deletes the per-site StatefulSets, the per-site Services,
// and the active Service when disabled.
func TestReconcileDragonflyResources_DisabledTearsDownPriorResources(t *testing.T) {
	fg := fgWithDragonfly()
	r, c := newReconciler(fg)
	ctx := context.Background()
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("reconcile (enabled): %v", err)
	}
	// Verify StatefulSets and Services exist before disable.
	var sts appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{Name: "lion-dragonfly-dc1", Namespace: "shared-lion"}, &sts); err != nil {
		t.Fatalf("pre-disable: dc1 StatefulSet missing: %v", err)
	}

	// Now flip enabled=false (simulating user mutation).
	fg.Spec.Dragonfly.Enabled = false
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("reconcile (disabled): %v", err)
	}
	// All Dragonfly StatefulSets must be gone.
	var stsList appsv1.StatefulSetList
	if err := c.List(ctx, &stsList); err != nil {
		t.Fatalf("list statefulsets: %v", err)
	}
	for _, s := range stsList.Items {
		if s.Labels[labelAppName] == dragonflyAppName {
			t.Errorf("StatefulSet %q survived disable", s.Name)
		}
	}
	// All Dragonfly Services (per-site + active) must be gone.
	var svcList corev1.ServiceList
	if err := c.List(ctx, &svcList); err != nil {
		t.Fatalf("list services: %v", err)
	}
	for _, s := range svcList.Items {
		if s.Labels[labelAppName] == dragonflyAppName {
			t.Errorf("Service %q survived disable", s.Name)
		}
	}
	var pdbList policyv1.PodDisruptionBudgetList
	if err := c.List(ctx, &pdbList); err != nil {
		t.Fatalf("list pdbs: %v", err)
	}
	for _, p := range pdbList.Items {
		if p.Labels[labelAppName] == dragonflyAppName {
			t.Errorf("PodDisruptionBudget %q survived disable", p.Name)
		}
	}
}

func TestReconcileDragonflyResources_SerializesImageRolloutReplicaBeforeActive(t *testing.T) {
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1", Phase: v1alpha1.DragonflyPhaseReady}
	r, c := newReconciler(fg)
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}})
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}})
	r.dragonflyConnector = conn.connect
	ctx := context.Background()
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	if err := c.Create(ctx, dragonflyPodForTest(fg, "dc1", fg.Spec.Dragonfly.Image, true)); err != nil {
		t.Fatalf("create dc1 pod: %v", err)
	}
	if err := c.Create(ctx, dragonflyPodForTest(fg, "dc2", fg.Spec.Dragonfly.Image, true)); err != nil {
		t.Fatalf("create dc2 pod: %v", err)
	}

	fg.Spec.Dragonfly.Image = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"
	requeue, err := r.reconcileDragonflyResources(ctx, fg)
	if err != nil {
		t.Fatalf("first rollout reconcile: %v", err)
	}
	if requeue == 0 {
		t.Fatal("first rollout reconcile requeue=0, want non-zero")
	}
	if got := dragonflyStatefulSetImageForTest(t, c, "dc2"); got != fg.Spec.Dragonfly.Image {
		t.Fatalf("replica image=%q, want target image", got)
	}
	if got := dragonflyStatefulSetImageForTest(t, c, "dc1"); got == fg.Spec.Dragonfly.Image {
		t.Fatalf("active image advanced before replica rollout completed")
	}

	markDragonflyStatefulSetRolledOutForTest(t, c, "dc2")
	requeue, err = r.reconcileDragonflyResources(ctx, fg)
	if err != nil {
		t.Fatalf("second rollout reconcile: %v", err)
	}
	if requeue == 0 {
		t.Fatal("second rollout reconcile requeue=0, want non-zero")
	}
	var promoted v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &promoted); err != nil {
		t.Fatalf("get promoted fg: %v", err)
	}
	if promoted.Status.Dragonfly == nil || promoted.Status.Dragonfly.ActiveSite != "dc2" {
		t.Fatalf("dragonfly activeSite=%v, want dc2 after replica promotion", promoted.Status.Dragonfly)
	}
	if got := dragonflyStatefulSetImageForTest(t, c, "dc1"); got == fg.Spec.Dragonfly.Image {
		t.Fatalf("old active image advanced in the same reconcile as promotion")
	}
	promoted.Spec.Dragonfly.Image = fg.Spec.Dragonfly.Image

	requeue, err = r.reconcileDragonflyResources(ctx, &promoted)
	if err != nil {
		t.Fatalf("third rollout reconcile: %v", err)
	}
	if requeue == 0 {
		t.Fatal("third rollout reconcile requeue=0, want non-zero")
	}
	if got := dragonflyStatefulSetImageForTest(t, c, "dc1"); got != fg.Spec.Dragonfly.Image {
		t.Fatalf("old active image=%q, want target image after promotion", got)
	}
}

func TestReconcileDragonflyResources_HoldsActiveImageUntilReplicaReady(t *testing.T) {
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1", Phase: v1alpha1.DragonflyPhaseReady}
	r, c := newReconciler(fg)
	ctx := context.Background()
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	fg.Spec.Dragonfly.Image = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("replica rollout reconcile: %v", err)
	}
	requeue, err := r.reconcileDragonflyResources(ctx, fg)
	if err != nil {
		t.Fatalf("active-hold reconcile: %v", err)
	}
	if requeue == 0 {
		t.Fatal("active-hold reconcile requeue=0, want non-zero")
	}
	if got := dragonflyStatefulSetImageForTest(t, c, "dc1"); got == fg.Spec.Dragonfly.Image {
		t.Fatalf("active image advanced while replica rollout was not complete")
	}
}

// TestBuildDragonflyArgs_FiltersOperatorOwnedFlags regression-tests
// B9: user-supplied spec.Args containing operator-owned safety flags
// could override them under gflags last-wins parsing — most notably
// --break_replication_on_master_restart=false would silently disable
// the split-brain guard. Verify the filter strips matching flags.
func TestBuildDragonflyArgs_FiltersOperatorOwnedFlags(t *testing.T) {
	spec := &v1alpha1.DragonflySpec{
		MaxMemoryMb: 256,
		Args: []string{
			// Forms gflags accepts: try both for each flag we filter.
			"--break_replication_on_master_restart=false",
			"--bind=192.168.1.1",
			"--port", "9999",
			"--admin_port=8888",
			"--requirepass=letmein",
			// Innocent passthrough flag (must survive).
			"--cluster_mode=emulated",
		},
	}
	args := buildDragonflyArgs(spec, 6379, 9999)
	joined := joinArgs(args)
	for _, banned := range []string{
		"--break_replication_on_master_restart=false",
		"--bind=192.168.1.1",
		"--admin_port=8888",
		"--requirepass=letmein",
	} {
		if contains(joined, banned) {
			t.Errorf("operator-owned flag leaked through filter: %q in %q", banned, joined)
		}
	}
	// `--port` followed by a value is the space-separated form; both
	// the flag and its value should be stripped.
	for i, a := range args {
		if a == "--port" && i+1 < len(args) && args[i+1] == "9999" {
			t.Errorf("space-separated --port 9999 leaked through filter: %v", args)
		}
	}
	// Innocent passthrough preserved.
	if !contains(joined, "--cluster_mode=emulated") {
		t.Errorf("innocent user arg dropped; args = %q", joined)
	}
	// Operator's own --break_replication_on_master_restart still
	// present (the safety guard is the whole point of B9).
	if !contains(joined, "--break_replication_on_master_restart") {
		t.Errorf("operator safety flag missing from args; got %q", joined)
	}
}

// TestBuildDragonflyEnv_EmptySecretNameOmitsEnv regression-tests B8:
// when auth.SecretName is empty (unset by partial in-place patch or
// admission-webhook bypass), the env var must not be emitted with an
// empty SecretKeyRef name — that produces a pod that fails to start
// with a secret-not-found event rather than a clean reconcile error.
// The corresponding --requirepass arg must also be omitted.
func TestBuildDragonflyEnv_EmptySecretNameOmitsEnv(t *testing.T) {
	spec := &v1alpha1.DragonflySpec{
		Auth: &v1alpha1.DragonflyAuthSpec{SecretName: ""},
	}
	if env := buildDragonflyEnv(spec); env != nil {
		t.Errorf("expected nil env when SecretName empty; got %+v", env)
	}
	args := buildDragonflyArgs(spec, 6379, 9999)
	if contains(joinArgs(args), "--requirepass") {
		t.Errorf("--requirepass emitted with empty SecretName; args = %q", joinArgs(args))
	}
}

func joinArgs(args []string) string {
	out := ""
	for _, a := range args {
		out += a + " "
	}
	return out
}

// contains is a thin wrapper around strings.Contains; kept as a named
// helper because several test files in this package call it. New tests
// can use strings.Contains directly.
func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

func dragonflyStatefulSetImageForTest(t *testing.T, c client.Client, site string) string {
	t.Helper()
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-" + site, Namespace: "shared-lion"}, &sts); err != nil {
		t.Fatalf("get StatefulSet %s: %v", site, err)
	}
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("StatefulSet %s has no containers", site)
	}
	return sts.Spec.Template.Spec.Containers[0].Image
}

func markDragonflyStatefulSetRolledOutForTest(t *testing.T, c client.Client, site string) {
	t.Helper()
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-" + site, Namespace: "shared-lion"}, &sts); err != nil {
		t.Fatalf("get StatefulSet %s: %v", site, err)
	}
	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.Replicas = desired
	sts.Status.UpdatedReplicas = desired
	sts.Status.ReadyReplicas = desired
	sts.Status.CurrentReplicas = desired
	if err := c.Status().Update(context.Background(), &sts); err != nil {
		t.Fatalf("update StatefulSet %s status: %v", site, err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion-dragonfly-" + site, Namespace: "shared-lion"}, &sts); err != nil {
		t.Fatalf("reload StatefulSet %s: %v", site, err)
	}
	if !dragonflyStatefulSetRolloutComplete(&sts) {
		t.Fatalf("StatefulSet %s was not marked rolled out: status=%+v generation=%d replicas=%v", site, sts.Status, sts.Generation, sts.Spec.Replicas)
	}
}
