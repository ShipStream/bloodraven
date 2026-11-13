package scenarios

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// pacedTestEnv is a scenario Env on fake clients: the playground group, the
// operator Deployment (rolled out, as the fake never moves its status) and
// the given nodes.
func pacedTestEnv(t *testing.T, nodes ...corev1.Node) *runner.Env {
	t.Helper()
	mfg := testBaselineMFG(false)
	mfg.Spec.Sites[0].Zone, mfg.Spec.Sites[1].Zone = "zone-iad", "zone-pdx"
	env := testScenarioEnv(t, mfg)
	one := int32(1)
	objs := []k8sruntime.Object{&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: mfg.Namespace, Name: pacedOperatorDeployment},
		Spec: appsv1.DeploymentSpec{Replicas: &one, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: pacedOperatorContainer,
			Env: []corev1.EnvVar{
				{Name: pacedEnvSidecarImage, Value: "bloodraven-sidecar:playground"},
				{Name: pacedEnvPacing, Value: "true"},
				{Name: pacedEnvSoak, Value: "1m"},
				{Name: pacedEnvStallAfter, Value: "30m"},
			},
		}}}}},
		Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, Replicas: 1, AvailableReplicas: 1},
	}}
	for i := range nodes {
		objs = append(objs, &nodes[i])
	}
	env.Kube.Kubernetes = fake.NewSimpleClientset(objs...)
	env.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	env.Capture = &runner.Capture{Dir: t.TempDir()}
	return env
}

// The cluster-side cleanup of scenario 73 undoes what a run changed, from
// what the cluster shows, and is a no-op on a clean cluster: it runs before
// every run (for an abandoned one) and after it.
func TestS73RestoreClusterUndoesARun(t *testing.T) {
	ctx := context.Background()
	env := pacedTestEnv(t)

	// A clean cluster: nothing to do, nothing changed.
	if err := s73RestoreCluster(ctx, env); err != nil {
		t.Fatalf("cleanup of a clean cluster: %v", err)
	}
	image, pacing, err := pacedOperatorSidecarDefault(ctx, env)
	if err != nil || image != "bloodraven-sidecar:playground" || !pacing {
		t.Fatalf("operator default = %q pacing=%v err=%v", image, pacing, err)
	}

	// What a run does: the pin, the auxiliary groups, two releases.
	if err := pacedPinSidecar(ctx, env, image); err != nil {
		t.Fatalf("pin: %v", err)
	}
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		t.Fatal(err)
	}
	if fg.Spec.Sidecar.Image != image || fg.Annotations[pacedPinAnnotation] == "" {
		t.Fatalf("after the pin: image=%q annotations=%v", fg.Spec.Sidecar.Image, fg.Annotations)
	}
	// Pinning again is a no-op, not a refusal.
	if err := pacedPinSidecar(ctx, env, image); err != nil {
		t.Fatalf("second pin: %v", err)
	}
	if err := pacedCreateAuxGroups(ctx, env, "zone-iad"); err != nil {
		t.Fatalf("create auxiliary groups: %v", err)
	}
	if names, err := pacedListAuxGroups(ctx, env); err != nil || strings.Join(names, ",") != "paced-a,paced-b" {
		t.Fatalf("auxiliary groups = %v, %v", names, err)
	}
	for _, release := range []map[string]string{
		{pacedEnvSidecarImage: "bloodraven-sidecar:playground-s73-missing", pacedEnvSoak: s73Soak, pacedEnvStallAfter: s73BadStallAfter},
		{pacedEnvSidecarImage: "bloodraven-sidecar:playground-next", pacedEnvSoak: s73Soak, pacedEnvStallAfter: s73GoodStallAfter},
	} {
		if err := pacedSetOperatorEnv(ctx, env, release); err != nil {
			t.Fatalf("set operator env: %v", err)
		}
	}
	if image, _, _ := pacedOperatorSidecarDefault(ctx, env); image != "bloodraven-sidecar:playground-next" {
		t.Fatalf("operator default after the good release = %q", image)
	}

	// The cleanup, as after a run or before the next one.
	for pass := 1; pass <= 2; pass++ {
		if err := s73RestoreCluster(ctx, env); err != nil {
			t.Fatalf("cleanup pass %d: %v", pass, err)
		}
		if names, err := pacedListAuxGroups(ctx, env); err != nil || len(names) != 0 {
			t.Fatalf("pass %d: auxiliary groups left: %v, %v", pass, names, err)
		}
		dep, err := env.Kube.GetDeployment(ctx, env.Namespace, pacedOperatorDeployment)
		if err != nil {
			t.Fatal(err)
		}
		c, err := pacedOperatorContainerOf(dep)
		if err != nil {
			t.Fatal(err)
		}
		for name, want := range map[string]string{pacedEnvSidecarImage: "bloodraven-sidecar:playground", pacedEnvSoak: "1m", pacedEnvStallAfter: "30m", pacedEnvPacing: "true"} {
			if got, _ := pacedEnvValue(c, name); got != want {
				t.Errorf("pass %d: %s = %q, want %q", pass, name, got, want)
			}
		}
		if dep.Annotations[pacedOriginalEnvAnnotation] != "" {
			t.Errorf("pass %d: the original-env annotation was left on the operator Deployment", pass)
		}
		fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			t.Fatal(err)
		}
		if fg.Spec.Sidecar.Image != "" || fg.Annotations[pacedPinAnnotation] != "" {
			t.Errorf("pass %d: the pin was left: image=%q annotations=%v", pass, fg.Spec.Sidecar.Image, fg.Annotations)
		}
	}
}

// A pin the scenario did not set is not the scenario's to remove, and a
// group pinned to another image is refused.
func TestPacedPinLeavesAForeignPinAlone(t *testing.T) {
	ctx := context.Background()
	env := pacedTestEnv(t)
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{Op: "add", Path: "/spec/sidecar/image", Value: "registry.example/sidecar:v9"}}); err != nil {
		t.Fatal(err)
	}
	if err := pacedPinSidecar(ctx, env, "bloodraven-sidecar:playground"); err == nil || !strings.Contains(err.Error(), "foreign pin") {
		t.Fatalf("pin over a foreign pin: %v", err)
	}
	if err := pacedUnpinSidecar(ctx, env); err != nil {
		t.Fatal(err)
	}
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		t.Fatal(err)
	}
	if fg.Spec.Sidecar.Image != "registry.example/sidecar:v9" {
		t.Fatalf("the foreign pin was removed: %q", fg.Spec.Sidecar.Image)
	}
}

// The spare zone of scenario 74: a node outside the playground group is
// labelled, the label is removed again, and a cluster without such a node
// is refused with the reason.
func TestPacedSpareZone(t *testing.T) {
	ctx := context.Background()
	ready := corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}
	site := func(name, s string) corev1.Node {
		return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{pacedZoneLabel: "zone-" + s}}, Status: ready}
	}
	env := pacedTestEnv(t, site("w1", "iad"), site("w2", "pdx"))
	if _, err := pacedEnsureSpareZone(ctx, env); err == nil || !strings.Contains(err.Error(), "no spare node") {
		t.Fatalf("a cluster with no spare node: %v", err)
	}

	env = pacedTestEnv(t, site("w1", "iad"), site("w2", "pdx"), corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "w4"}, Status: ready})
	zone, err := pacedEnsureSpareZone(ctx, env)
	if err != nil || zone != pacedSpareZone {
		t.Fatalf("spare zone = %q, %v", zone, err)
	}
	node, err := env.Kube.GetNode(ctx, "w4")
	if err != nil {
		t.Fatal(err)
	}
	if node.Labels[pacedZoneLabel] != pacedSpareZone || node.Annotations[pacedSpareZoneAnnotation] == "" {
		t.Fatalf("spare node: labels=%v annotations=%v", node.Labels, node.Annotations)
	}
	// A second call finds the zone it set.
	if zone, err := pacedEnsureSpareZone(ctx, env); err != nil || zone != pacedSpareZone {
		t.Fatalf("second call: spare zone = %q, %v", zone, err)
	}
	if err := pacedRemoveSpareZone(ctx, env); err != nil {
		t.Fatal(err)
	}
	node, err = env.Kube.GetNode(ctx, "w4")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := node.Labels[pacedZoneLabel]; ok || node.Annotations[pacedSpareZoneAnnotation] != "" {
		t.Fatalf("spare node after cleanup: labels=%v annotations=%v", node.Labels, node.Annotations)
	}
	// The sites' own zone labels are not the scenario's.
	node, err = env.Kube.GetNode(ctx, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if node.Labels[pacedZoneLabel] != "zone-iad" {
		t.Fatalf("a site node lost its zone label: %v", node.Labels)
	}
}

// A sample reads the pods, the site Deployments and the record of a group,
// and the observer collects such samples.
func TestPacedReadGroupAndObserver(t *testing.T) {
	ctx := context.Background()
	env := pacedTestEnv(t)
	k8s := env.Kube.Kubernetes
	for _, s := range []string{"iad", "pdx"} {
		dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Namespace: env.Namespace, Name: pgkube.MysqlDeploymentName(env.FG, s),
			Annotations: map[string]string{v1alpha1.RenderedByAnnotation: "r1", v1alpha1.SpecHashAnnotation: "h-" + s},
		}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "mysql", Image: "mysql:9.7"}, {Name: "sidecar", Image: "bloodraven-sidecar:playground"}}}}}}
		if _, err := k8s.AppsV1().Deployments(env.Namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: env.Namespace, Name: "mysql-" + s, UID: types.UID("uid-" + s),
			Labels: map[string]string{"app.kubernetes.io/name": "mysql", "app.kubernetes.io/instance": env.FG, v1alpha1.SiteLabel: s},
		}, Status: corev1.PodStatus{
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "mysql", RestartCount: 1}, {Name: "sidecar", RestartCount: 2}},
		}}
		if _, err := k8s.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	g, err := pacedReadGroup(ctx, env.Kube, env.Namespace, env.FG)
	if err != nil {
		t.Fatal(err)
	}
	if g.ActiveSite != "iad" || !g.Ready || g.Rollout != nil {
		t.Errorf("group: active=%q ready=%v rollout=%v", g.ActiveSite, g.Ready, g.Rollout)
	}
	if want := (pacedPod{UID: "uid-pdx", Restarts: 3, Ready: true}); len(g.Pods["pdx"]) != 1 || g.Pods["pdx"][0] != want {
		t.Errorf("pods of pdx = %+v, want %+v", g.Pods["pdx"], want)
	}
	if want := (pacedWorkload{RenderedBy: "r1", SpecHash: "h-iad", SidecarImage: "bloodraven-sidecar:playground"}); g.Workloads["iad"] != want {
		t.Errorf("deployment of iad = %+v, want %+v", g.Workloads["iad"], want)
	}

	o := pacedStartObserver(env.Kube, env.Namespace, []string{env.FG})
	deadline := time.Now().Add(5 * time.Second)
	for len(o.Samples()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	samples := o.Stop()
	if len(samples) == 0 {
		t.Fatal("the observer took no sample")
	}
	if n, err := o.Failures(); n != 0 {
		t.Errorf("%d samples failed: %v", n, err)
	}
	if got := samples[0].Groups[env.FG].Pods["iad"]; len(got) != 1 || got[0].UID != "uid-iad" {
		t.Errorf("observer sample: %+v", samples[0].Groups[env.FG])
	}
	// Stop is safe to call again (the scenario's cleanup does, after the
	// verify step stopped it).
	if again := o.Stop(); len(again) != len(samples) {
		t.Errorf("a second Stop returned %d samples, want %d", len(again), len(samples))
	}

	// The failure capture writes what the runner's own capture lacks.
	pacedDumpForensics(ctx, env, []string{env.FG}, samples)
	for _, name := range []string{"paced-rollout-groups.yaml", "paced-rollout-timeline.txt", "paced-rollout-pods.txt", "operator-deployment.yaml"} {
		if _, err := os.Stat(filepath.Join(env.Capture.Dir, name)); err != nil {
			t.Errorf("capture file %s: %v", name, err)
		}
	}
}
