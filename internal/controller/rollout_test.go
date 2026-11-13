package controller

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// oldReleaseHash stands for the spec hash an earlier release stamped: a
// render-version bump changes the hash of every group without any spec edit.
const oldReleaseHash = "0ldre1ease0hash0"

func rolloutReconcile(t *testing.T, r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}); err != nil {
		t.Fatalf("Reconcile(%s): %v", fg.Name, err)
	}
}

func siteDeployment(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) *appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, site)}, &d); err != nil {
		t.Fatalf("get deployment %s/%s: %v", fg.Name, site, err)
	}
	return &d
}

func siteConfigMapData(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) map[string]string {
	t.Helper()
	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: siteConfigMapName(fg.Name, site)}, &cm); err != nil {
		t.Fatalf("get configmap %s/%s: %v", fg.Name, site, err)
	}
	return cm.Data
}

// ageDeployments makes fg's site Deployments look as an earlier operator
// release left them: no renderer annotation, their rollout complete and,
// with oldHash set, another spec hash.
func ageDeployments(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, oldHash string) {
	t.Helper()
	for _, site := range fg.Spec.Sites {
		d := siteDeployment(t, c, fg, site.Name)
		delete(d.Annotations, v1alpha1.RenderedByAnnotation)
		if oldHash != "" {
			d.Annotations[v1alpha1.SpecHashAnnotation] = oldHash
			d.Spec.Template.Annotations[v1alpha1.SpecHashAnnotation] = oldHash
		}
		if err := c.Update(context.Background(), d); err != nil {
			t.Fatalf("age deployment %s: %v", d.Name, err)
		}
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
		if err := c.Status().Update(context.Background(), d); err != nil {
			t.Fatalf("age deployment %s status: %v", d.Name, err)
		}
	}
}

func freshGroup(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	var out v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(fg), &out); err != nil {
		t.Fatalf("get group %s: %v", fg.Name, err)
	}
	return &out
}

// admitGroup writes the admission the pacer would.
func admitGroup(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	fresh := freshGroup(t, c, fg)
	fresh.Status.Rollout = &v1alpha1.RolloutStatus{
		Renderer: rendererIdentity(), Phase: v1alpha1.RolloutPhaseAdmitted, Reason: rolloutReasonAdmitted,
		AdmittedTime: rolloutTime(time.Now()),
	}
	if err := c.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("admit %s: %v", fg.Name, err)
	}
}

func rolloutRunner(c client.Client) *TopologyManagerRunner {
	runner := NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(100), testutil.TestLogger())
	runner.SetRolloutPacing(true)
	return runner
}

func driftView(t *testing.T, runner *TopologyManagerRunner, c client.Client, fg *v1alpha1.MysqlFailoverGroup) (drift, held []string) {
	t.Helper()
	drift, _, held, ok := runner.specDriftView(context.Background(), c, freshGroup(t, c, fg))
	if !ok {
		t.Fatalf("specDriftView(%s) could not read its inputs", fg.Name)
	}
	return drift, held
}

// R-2: a group that is not admitted keeps its rendered pod spec, through an
// unrelated spec edit too, and the drift check and the write path decide by
// the same record: no drift is reported and the write is refused. Once
// admitted, both open, and the first write latches the start (R-4).
func TestRollout_UnadmittedGroupKeepsItsPods(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)
	rolloutReconcile(t, r, fg)
	ageDeployments(t, c, fg, oldReleaseHash)
	before := map[string]*appsv1.Deployment{}
	beforeConf := map[string]map[string]string{}
	for _, site := range fg.Spec.Sites {
		before[site.Name] = siteDeployment(t, c, fg, site.Name)
		beforeConf[site.Name] = siteConfigMapData(t, c, fg, site.Name)
	}

	r.RolloutPacing = true
	runner := rolloutRunner(c)
	unchanged := func(when string) {
		t.Helper()
		for _, site := range fg.Spec.Sites {
			d := siteDeployment(t, c, fg, site.Name)
			if !equality.Semantic.DeepEqual(d.Spec, before[site.Name].Spec) || !equality.Semantic.DeepEqual(d.Annotations, before[site.Name].Annotations) {
				t.Fatalf("%s: deployment of %s changed while the group is not admitted", when, site.Name)
			}
			if !equality.Semantic.DeepEqual(siteConfigMapData(t, c, fg, site.Name), beforeConf[site.Name]) {
				t.Fatalf("%s: configmap of %s changed while the group is not admitted", when, site.Name)
			}
		}
	}

	rolloutReconcile(t, r, fg)
	unchanged("after the operator upgrade")
	if drift, held := driftView(t, runner, c, fg); len(drift) != 0 || !slices.Equal(held, []string{"dc1", "dc2"}) {
		t.Fatalf("drift = %v, held = %v; want no drift and both sites held", drift, held)
	}

	// An unrelated spec edit: it changes my.cnf and the hash.
	edited := freshGroup(t, c, fg)
	edited.Spec.MysqlConf = map[string]v1alpha1.MysqlConfValue{"max_connections": "999"}
	if err := c.Update(context.Background(), edited); err != nil {
		t.Fatal(err)
	}
	rolloutReconcile(t, r, fg)
	unchanged("after a spec edit")
	if drift, held := driftView(t, runner, c, fg); len(drift) != 0 || len(held) != 2 {
		t.Fatalf("after a spec edit: drift = %v, held = %v", drift, held)
	}
	nn := client.ObjectKeyFromObject(fg)
	if err := r.ReconcileSiteDeployment(context.Background(), nn, "dc2"); !errors.Is(err, errRolloutHeld) {
		t.Fatalf("ReconcileSiteDeployment on an unadmitted group = %v, want errRolloutHeld", err)
	}
	unchanged("after a refused site write")
	if freshGroup(t, c, fg).Status.Rollout != nil {
		t.Fatal("the write path wrote a rollout record")
	}

	admitGroup(t, c, fg)
	if drift, held := driftView(t, runner, c, fg); !slices.Equal(drift, []string{"dc1", "dc2"}) || len(held) != 0 {
		t.Fatalf("admitted: drift = %v, held = %v; want both sites drifted", drift, held)
	}
	if err := r.applySiteDeployment(context.Background(), nn, "dc2"); err != nil {
		t.Fatalf("applySiteDeployment on the admitted group: %v", err)
	}
	rolled := siteDeployment(t, c, fg, "dc2")
	if rolled.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() || rolled.Annotations[v1alpha1.SpecHashAnnotation] == oldReleaseHash {
		t.Fatalf("dc2 annotations = %v, want this renderer and a new hash", rolled.Annotations)
	}
	if conf := siteConfigMapData(t, c, fg, "dc2")["bloodraven.cnf"]; !containsLine(conf, "max-connections=999") {
		t.Fatalf("dc2's ConfigMap was not written with its pod:\n%s", conf)
	}
	if d := siteDeployment(t, c, fg, "dc1"); !equality.Semantic.DeepEqual(d.Spec, before["dc1"].Spec) {
		t.Fatal("dc1 changed with dc2's write")
	}
	if !equality.Semantic.DeepEqual(siteConfigMapData(t, c, fg, "dc1"), beforeConf["dc1"]) {
		t.Fatal("dc1's ConfigMap changed before its pod was rolled")
	}
	rec := freshGroup(t, c, fg).Status.Rollout
	if !rec.Started() || rec.Phase != v1alpha1.RolloutPhaseRolling {
		t.Fatalf("rollout record after the first write = %+v, want the start latched", rec)
	}
	if drift, held := driftView(t, runner, c, fg); !slices.Equal(drift, []string{"dc1"}) || len(held) != 0 {
		t.Fatalf("after dc2: drift = %v, held = %v", drift, held)
	}
}

func containsLine(text, line string) bool {
	for _, l := range splitLines(text) {
		if l == line {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// A release that changes nothing a group renders only restamps the group's
// workloads: one annotation, no template write, no admission.
func TestRollout_RestampsWhenNothingChanges(t *testing.T) {
	for _, strategy := range []v1alpha1.UpdateStrategyType{v1alpha1.UpdateStrategyOrderedUpdate, v1alpha1.UpdateStrategyRecreate} {
		t.Run(string(strategy), func(t *testing.T) {
			fg := newTestFG()
			fg.Spec.UpdateStrategy.Type = strategy
			r, c := newReconciler(fg)
			rolloutReconcile(t, r, fg)
			ageDeployments(t, c, fg, "")
			before := siteDeployment(t, c, fg, "dc1")

			r.RolloutPacing = true
			rolloutReconcile(t, r, fg)
			for _, site := range fg.Spec.Sites {
				d := siteDeployment(t, c, fg, site.Name)
				if d.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() {
					t.Fatalf("%s was not restamped: %v", site.Name, d.Annotations)
				}
			}
			if d := siteDeployment(t, c, fg, "dc1"); !equality.Semantic.DeepEqual(d.Spec, before.Spec) {
				t.Fatal("the restamp changed the Deployment spec")
			}
			if freshGroup(t, c, fg).Status.Rollout != nil {
				t.Fatal("a restamp wrote a rollout record")
			}
			if drift, held := driftView(t, rolloutRunner(c), c, fg); len(drift) != 0 || len(held) != 0 {
				t.Fatalf("drift = %v, held = %v, want none", drift, held)
			}
			st, err := r.RolloutGroupState(context.Background(), c, freshGroup(t, c, fg))
			if err != nil || len(st.Pending) != 0 {
				t.Fatalf("RolloutGroupState = %+v, %v; want nothing pending", st, err)
			}
		})
	}
}

// R-3: a Recreate group is held and admitted like any other. Its reconciler
// writes every site in one pass, but only once the group holds the slot.
func TestRollout_RecreateGroupWaitsForAdmission(t *testing.T) {
	fg := newTestFG()
	fg.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
	r, c := newReconciler(fg)
	rolloutReconcile(t, r, fg)
	ageDeployments(t, c, fg, oldReleaseHash)
	before := siteDeployment(t, c, fg, "dc1")

	r.RolloutPacing = true
	rolloutReconcile(t, r, fg)
	for _, site := range fg.Spec.Sites {
		d := siteDeployment(t, c, fg, site.Name)
		if d.Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash || d.Annotations[v1alpha1.RenderedByAnnotation] != "" {
			t.Fatalf("%s was written before the group was admitted: %v", site.Name, d.Annotations)
		}
	}
	if d := siteDeployment(t, c, fg, "dc1"); !equality.Semantic.DeepEqual(d.Spec, before.Spec) {
		t.Fatal("dc1's spec changed before admission")
	}
	st, err := r.RolloutGroupState(context.Background(), c, freshGroup(t, c, fg))
	if err != nil || !slices.Equal(st.Pending, []string{"mysql/dc1", "mysql/dc2"}) {
		t.Fatalf("RolloutGroupState = %+v, %v; want both sites pending", st, err)
	}

	admitGroup(t, c, fg)
	rolloutReconcile(t, r, fg)
	for _, site := range fg.Spec.Sites {
		d := siteDeployment(t, c, fg, site.Name)
		if d.Annotations[v1alpha1.SpecHashAnnotation] == oldReleaseHash || d.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() {
			t.Fatalf("%s was not rolled after admission: %v", site.Name, d.Annotations)
		}
	}
	if rec := freshGroup(t, c, fg).Status.Rollout; !rec.Started() {
		t.Fatalf("rollout record = %+v, want the start latched", rec)
	}
}

// Under Recreate the reconciler writes the whole template on every pass, so
// a release that changes a part of it the hash does not cover would restart
// every such group at once. The dry run finds it and the group waits.
func TestRollout_RecreateTemplateOnlyChangeIsPaced(t *testing.T) {
	fg := newTestFG()
	fg.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
	r, c := newReconciler(fg)
	rolloutReconcile(t, r, fg)
	ageDeployments(t, c, fg, "")
	// What the earlier release rendered differs outside the hash.
	d := siteDeployment(t, c, fg, "dc1")
	d.Spec.Template.Spec.Containers[0].Env = append(d.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "OLD_RELEASE_ONLY", Value: "1"})
	if err := c.Update(context.Background(), d); err != nil {
		t.Fatal(err)
	}

	r.RolloutPacing = true
	rolloutReconcile(t, r, fg)
	got := siteDeployment(t, c, fg, "dc1")
	if !equality.Semantic.DeepEqual(got.Spec.Template, d.Spec.Template) || got.Annotations[v1alpha1.RenderedByAnnotation] != "" {
		t.Fatal("dc1's template was rewritten without admission")
	}
	if other := siteDeployment(t, c, fg, "dc2"); other.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() {
		t.Fatal("dc2, which does not change, was not restamped")
	}
	st, err := r.RolloutGroupState(context.Background(), c, freshGroup(t, c, fg))
	if err != nil || !slices.Equal(st.Pending, []string{"mysql/dc1"}) {
		t.Fatalf("RolloutGroupState = %+v, %v; want dc1 pending", st, err)
	}

	admitGroup(t, c, fg)
	rolloutReconcile(t, r, fg)
	got = siteDeployment(t, c, fg, "dc1")
	if got.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() {
		t.Fatal("dc1 was not rolled after admission")
	}
	for _, e := range got.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "OLD_RELEASE_ONLY" {
			t.Fatal("dc1 still carries the old template")
		}
	}
	if st, _ := r.RolloutGroupState(context.Background(), c, freshGroup(t, c, fg)); len(st.Pending) != 0 {
		t.Fatalf("pending after the roll = %v", st.Pending)
	}
}

// R-1, R-7: a new default sidecar image and a render-version bump together.
// A group that pins its sidecar is held for the render-version change all
// the same.
func TestRollout_SidecarDefaultAndRenderVersionChangeTogether(t *testing.T) {
	t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })
	SetSidecarImageDefault("registry.example/bloodraven-sidecar:1.7.0")
	oldRenderer := rendererIdentity()

	unpinned := newTestFG()
	pinned := newTestFG()
	pinned.Name, pinned.UID = "tiger", "test-uid-tiger"
	pinned.Spec.Sidecar.Image = "registry.example/bloodraven-sidecar:pinned"
	r, c := newReconciler(unpinned, pinned)
	rolloutReconcile(t, r, unpinned)
	rolloutReconcile(t, r, pinned)
	for _, fg := range []*v1alpha1.MysqlFailoverGroup{unpinned, pinned} {
		ageDeployments(t, c, fg, "")
		// Stamped by the old release.
		for _, site := range fg.Spec.Sites {
			d := siteDeployment(t, c, fg, site.Name)
			d.Annotations[v1alpha1.RenderedByAnnotation] = oldRenderer
			if fg == pinned {
				// The pin keeps the sidecar image; the render version moved.
				d.Annotations[v1alpha1.SpecHashAnnotation] = oldReleaseHash
			}
			if err := c.Update(context.Background(), d); err != nil {
				t.Fatal(err)
			}
		}
	}

	// The upgrade: a new default sidecar image.
	SetSidecarImageDefault("registry.example/bloodraven-sidecar:1.8.0")
	if rendererIdentity() == oldRenderer {
		t.Fatal("a new default sidecar image did not change the renderer identity")
	}
	r.RolloutPacing = true
	runner := rolloutRunner(c)
	for _, fg := range []*v1alpha1.MysqlFailoverGroup{unpinned, pinned} {
		rolloutReconcile(t, r, fg)
		if drift, held := driftView(t, runner, c, fg); len(drift) != 0 || len(held) != 2 {
			t.Fatalf("%s: drift = %v, held = %v; want both sites held", fg.Name, drift, held)
		}
		for _, site := range fg.Spec.Sites {
			if d := siteDeployment(t, c, fg, site.Name); d.Annotations[v1alpha1.RenderedByAnnotation] != oldRenderer {
				t.Fatalf("%s/%s was written without admission", fg.Name, site.Name)
			}
		}
	}
}

// A site added to a group that waits gets its Deployment at once: a new pod
// is not a restart of a running one.
func TestRollout_NewSiteIsCreatedWhileGroupWaits(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)
	rolloutReconcile(t, r, fg)
	ageDeployments(t, c, fg, oldReleaseHash)
	if err := c.Delete(context.Background(), siteDeployment(t, c, fg, "dc2")); err != nil {
		t.Fatal(err)
	}
	r.RolloutPacing = true
	rolloutReconcile(t, r, fg)
	if d := siteDeployment(t, c, fg, "dc2"); d.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() {
		t.Fatalf("dc2 annotations = %v", d.Annotations)
	}
	if d := siteDeployment(t, c, fg, "dc1"); d.Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash {
		t.Fatal("dc1 was rolled with the new site")
	}
}

// With pacing off the operator rolls as releases before v1.8.0 did.
func TestRollout_PacingOffRollsAtOnce(t *testing.T) {
	fg := newTestFG()
	fg.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
	r, c := newReconciler(fg)
	rolloutReconcile(t, r, fg)
	ageDeployments(t, c, fg, oldReleaseHash)
	rolloutReconcile(t, r, fg)
	for _, site := range fg.Spec.Sites {
		if d := siteDeployment(t, c, fg, site.Name); d.Annotations[v1alpha1.SpecHashAnnotation] == oldReleaseHash {
			t.Fatalf("%s was not rolled with pacing off", site.Name)
		}
	}
	runner := NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(10), testutil.TestLogger())
	ordered := newTestFG()
	ordered.Name, ordered.UID = "tiger", "uid-tiger"
	if err := c.Create(context.Background(), ordered); err != nil {
		t.Fatal(err)
	}
	rolloutReconcile(t, r, ordered)
	ageDeployments(t, c, ordered, oldReleaseHash)
	drift, _, held, ok := runner.specDriftView(context.Background(), c, freshGroup(t, c, ordered))
	if !ok || len(drift) != 2 || len(held) != 0 {
		t.Fatalf("pacing off: drift = %v, held = %v, ok = %v; want both sites drifted", drift, held, ok)
	}
}

func TestRolloutHoldsSite(t *testing.T) {
	fg := newTestFG()
	other := &appsv1.Deployment{}
	mine := &appsv1.Deployment{}
	stampRenderedBy(mine)
	if !rolloutHoldsSite(true, fg, "dc1", other) {
		t.Fatal("an unadmitted group's site from another renderer is not held")
	}
	if rolloutHoldsSite(false, fg, "dc1", other) {
		t.Fatal("held with pacing off")
	}
	if rolloutHoldsSite(true, fg, "dc1", mine) {
		t.Fatal("a site this renderer wrote is held: its change comes from the group")
	}
	fg.Status.Rollout = &v1alpha1.RolloutStatus{Renderer: rendererIdentity(), AdmittedTime: rolloutTime(time.Now())}
	if rolloutHoldsSite(true, fg, "dc1", other) {
		t.Fatal("an admitted group's site is held")
	}
	fg.Status.Rollout.Renderer = "another-release"
	if !rolloutHoldsSite(true, fg, "dc1", other) {
		t.Fatal("an admission for another renderer opened the gate")
	}
	fg.Status.Rollout.Renderer = rendererIdentity()
	fg.Status.Rollout.CompletedTime = rolloutTime(time.Now())
	if !rolloutHoldsSite(true, fg, "dc1", other) {
		t.Fatal("a completed admission opened the gate")
	}

	// A clone recipient's pod is rolled without admission.
	enabled := true
	fg.Status.Rollout = nil
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: enabled}
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{Sites: []v1alpha1.SiteEncryptionStatus{
		{Name: "dc2", UnsealReason: v1alpha1.UnsealReasonClone},
	}}
	if rolloutHoldsSite(true, fg, "dc2", other) {
		t.Fatal("a clone recipient that must restart with a writable keyring is held")
	}
	if !rolloutHoldsSite(true, fg, "dc1", other) {
		t.Fatal("the clone exemption reached another site")
	}
}

func TestRendererIdentity(t *testing.T) {
	t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })
	a := rendererIdentity()
	if a == "" || a != rendererIdentity() {
		t.Fatalf("identity %q is not stable", a)
	}
	SetSidecarImageDefault("registry.example/other:1")
	if rendererIdentity() == a {
		t.Fatal("the default sidecar image is not part of the identity")
	}
	SetSidecarImageDefault(testSidecarImage)
	if rendererIdentity() != a {
		t.Fatal("the identity did not return with the image")
	}
	prev := operatorImageFromEnv
	t.Cleanup(func() { SetOperatorImageDefaults(prev, "") })
	SetOperatorImageDefaults("ghcr.io/shipstream/bloodraven:9.9.9", "")
	if rendererIdentity() == a {
		t.Fatal("the operator image is not part of the identity")
	}
}

// stubUnstable is the pacer's stability check in tests that do not run a
// topology manager.
type stubUnstable map[string]string

func (s stubUnstable) reason(fg *v1alpha1.MysqlFailoverGroup) string { return s[fg.Name] }

// The pacer against the API: it admits one group, the reconciler rolls it,
// and the roll completes once the workloads report it.
func TestRolloutPacer_AdmitsRollsAndCompletes(t *testing.T) {
	lion := newTestFG()
	lion.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
	tiger := newTestFG()
	tiger.Name, tiger.UID = "tiger", "uid-tiger"
	tiger.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
	r, c := newReconciler(lion, tiger)
	for _, fg := range []*v1alpha1.MysqlFailoverGroup{lion, tiger} {
		rolloutReconcile(t, r, fg)
		ageDeployments(t, c, fg, oldReleaseHash)
	}
	r.RolloutPacing = true
	recorder := record.NewFakeRecorder(100)
	unstable := stubUnstable{}
	now := rolloutT0
	pacer := NewRolloutPacer(c, c, recorder, testutil.TestLogger(), r, unstable.reason, RolloutPacerConfig{Soak: time.Minute})
	pacer.SetClock(func() time.Time { return now })
	tick := func() {
		t.Helper()
		if err := pacer.Tick(context.Background()); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	phase := func(fg *v1alpha1.MysqlFailoverGroup) v1alpha1.RolloutPhase {
		if rec := freshGroup(t, c, fg).Status.Rollout; rec != nil {
			return rec.Phase
		}
		return ""
	}

	tick()
	if phase(lion) != v1alpha1.RolloutPhaseAdmitted || phase(tiger) != v1alpha1.RolloutPhasePending {
		t.Fatalf("after the first pass: lion %q, tiger %q", phase(lion), phase(tiger))
	}
	rolloutReconcile(t, r, lion)
	rolloutReconcile(t, r, tiger)
	if phase(lion) != v1alpha1.RolloutPhaseRolling {
		t.Fatalf("lion = %q after its reconcile, want Rolling", phase(lion))
	}
	if d := siteDeployment(t, c, tiger, "dc1"); d.Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash {
		t.Fatal("tiger was rolled while lion holds the slot")
	}

	// lion's pods are not back yet.
	for _, site := range lion.Spec.Sites {
		d := siteDeployment(t, c, lion, site.Name)
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1}
		if err := c.Status().Update(context.Background(), d); err != nil {
			t.Fatal(err)
		}
	}
	unstable["lion"] = v1alpha1.UnstableReasonNoPrimary
	now = now.Add(time.Minute)
	tick()
	if phase(lion) != v1alpha1.RolloutPhaseRolling || phase(tiger) != v1alpha1.RolloutPhasePending {
		t.Fatalf("mid-roll: lion %q, tiger %q", phase(lion), phase(tiger))
	}

	// They come back and the group is stable.
	for _, site := range lion.Spec.Sites {
		d := siteDeployment(t, c, lion, site.Name)
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
		if err := c.Status().Update(context.Background(), d); err != nil {
			t.Fatal(err)
		}
	}
	delete(unstable, "lion")
	now = now.Add(time.Minute)
	tick()
	if phase(lion) != v1alpha1.RolloutPhaseComplete {
		t.Fatalf("lion = %q, want Complete", phase(lion))
	}
	tick()
	if phase(tiger) != v1alpha1.RolloutPhasePending {
		t.Fatalf("tiger = %q during the soak, want Pending", phase(tiger))
	}
	now = now.Add(time.Minute)
	tick()
	if phase(tiger) != v1alpha1.RolloutPhaseAdmitted {
		t.Fatalf("tiger = %q after the soak, want Admitted", phase(tiger))
	}
	rolloutReconcile(t, r, tiger)
	if d := siteDeployment(t, c, tiger, "dc1"); d.Annotations[v1alpha1.RenderedByAnnotation] != rendererIdentity() {
		t.Fatal("tiger was not rolled after its admission")
	}

	var reasons []string
	for len(recorder.Events) > 0 {
		reasons = append(reasons, eventReason(<-recorder.Events))
	}
	want := []string{v1alpha1.EventRolloutAdmitted, v1alpha1.EventRolloutCompleted, v1alpha1.EventRolloutAdmitted}
	if !slices.Equal(reasons, want) {
		t.Fatalf("events = %v, want %v", reasons, want)
	}
}

func eventReason(e string) string {
	// "<type> <reason> <message>"
	parts := splitN(e, ' ', 3)
	if len(parts) < 2 {
		return e
	}
	return parts[1]
}

func splitN(s string, sep byte, n int) []string {
	var out []string
	for len(out) < n-1 {
		i := indexByte(s, sep)
		if i < 0 {
			break
		}
		out = append(out, s[:i])
		s = s[i+1:]
	}
	return append(out, s)
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// A CRD older than the operator prunes status.rollout. Nothing is admitted
// then, so nothing rolls, and the operator says why once.
func TestRolloutPacer_RecordPrunedByOldCRD(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	objs := withTestPassphraseSecrets([]client.Object{newTestSecret(), fg})
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if g, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
					g.Status.Rollout = nil
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	rolloutReconcile(t, r, fg)
	ageDeployments(t, c, fg, oldReleaseHash)
	r.RolloutPacing = true

	recorder := record.NewFakeRecorder(100)
	pacer := NewRolloutPacer(c, c, recorder, testutil.TestLogger(), r, stubUnstable{}.reason, RolloutPacerConfig{})
	for i := 0; i < 3; i++ {
		if err := pacer.Tick(context.Background()); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	if len(recorder.Events) != 1 {
		t.Fatalf("%d events, want one RolloutRecordNotStored", len(recorder.Events))
	}
	if got := eventReason(<-recorder.Events); got != v1alpha1.EventRolloutRecordNotStored {
		t.Fatalf("event = %s", got)
	}
	if err := r.ReconcileSiteDeployment(context.Background(), client.ObjectKeyFromObject(fg), "dc2"); !errors.Is(err, errRolloutHeld) {
		t.Fatalf("site write with no stored admission = %v, want errRolloutHeld", err)
	}
	if d := siteDeployment(t, c, fg, "dc2"); d.Annotations[v1alpha1.SpecHashAnnotation] != oldReleaseHash {
		t.Fatal("dc2 was rolled without a stored admission")
	}
}

// The pacer's default stability check is the deployment API's.
func TestRolloutPacer_DefaultStabilityCheck(t *testing.T) {
	p := NewRolloutPacer(nil, nil, nil, testutil.TestLogger(), nil, nil, RolloutPacerConfig{})
	fg := newTestFG()
	if got := p.unstable(fg); got != DeploymentUnstableReason(fg) || got == "" {
		t.Fatalf("unstable = %q, want %q", got, DeploymentUnstableReason(fg))
	}
	fg.Status.Rollout = &v1alpha1.RolloutStatus{AdmittedTime: &metav1.Time{Time: rolloutT0}}
	if !fg.Status.Rollout.InFlight() {
		t.Fatal("an admission without completion is not in flight")
	}
}

// Only the value "true" arms the administrator's annotations: a
// rollout-admit set to "false" must not admit a group past the queue.
func TestRolloutPacer_AnnotationValueMustBeTrue(t *testing.T) {
	lion := newTestFG()
	tiger := newTestFG()
	tiger.Name, tiger.UID = "tiger", "uid-tiger"
	tiger.Annotations = map[string]string{v1alpha1.RolloutAdmitAnnotation: "false"}
	r, c := newReconciler(lion, tiger)
	for _, fg := range []*v1alpha1.MysqlFailoverGroup{lion, tiger} {
		rolloutReconcile(t, r, fg)
		ageDeployments(t, c, fg, oldReleaseHash)
	}
	r.RolloutPacing = true
	pacer := NewRolloutPacer(c, c, record.NewFakeRecorder(100), testutil.TestLogger(), r, stubUnstable{}.reason, RolloutPacerConfig{Soak: -1})
	tick := func() {
		t.Helper()
		if err := pacer.Tick(context.Background()); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	tick()
	if rec := freshGroup(t, c, tiger).Status.Rollout; rec == nil || rec.Phase != v1alpha1.RolloutPhasePending {
		t.Fatalf("tiger with rollout-admit=false: %+v, want Pending behind lion", rec)
	}

	fresh := freshGroup(t, c, tiger)
	fresh.Annotations[v1alpha1.RolloutAdmitAnnotation] = "true"
	if err := c.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	tick()
	if rec := freshGroup(t, c, tiger).Status.Rollout; rec == nil || rec.Reason != rolloutReasonAdmittedByAnnotation {
		t.Fatalf("tiger with rollout-admit=true: %+v, want AdmittedByAnnotation", rec)
	}
}
