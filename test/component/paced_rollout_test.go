package component

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// The paced rollout (issue #244, proposals/23-paced-rollouts.md) across the
// components that make it up, each through what another package can see of
// it: the RolloutPacer, the group reconciler and its Deployment write path,
// the runner's drift check and status write, and one topology manager per
// group with the production ordered updater. The API server is
// controller-runtime's fake client; the Deployment controller and the
// kubelet are the few lines of pacedFleet.rolled.

const (
	pacedSidecarBefore = "bloodraven-sidecar:component"
	pacedSidecarAfter  = "bloodraven-sidecar:component-next"
	// pacedSidecarSecond is the default sidecar image of a second upgrade
	// that lands while the first is still rolling out.
	pacedSidecarSecond = "bloodraven-sidecar:component-second"
	pacedSidecarPinned = "registry.example/bloodraven-sidecar:pinned"
	// pacedHashBeforeBump stands for the spec hash an earlier
	// deploymentPodRenderVersion produced: the render versions are
	// constants of the binary under test, so the bump is told by what it
	// leaves behind.
	pacedHashBeforeBump = "hash-before-render-version-bump"
)

// pacedGroupSpec is one failover group of a paced-rollout scenario.
type pacedGroupSpec struct {
	name     string
	recreate bool
	// pinnedSidecar pins spec.sidecar.image, so a new default sidecar
	// image changes nothing this group renders.
	pinnedSidecar string
	// renderVersionBump: the release also bumped a render version, which
	// changes the spec hash of every group, pinned or not.
	renderVersionBump bool
}

type pacedGroup struct {
	spec  pacedGroupSpec
	key   types.NamespacedName
	cfg   controller.TopologyConfig
	mysql map[string]*mockMySQL
	tm    *controller.TopologyManager
	apply func(context.Context, string) error

	mu      sync.Mutex
	applied []string
}

func (g *pacedGroup) appliedSites() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.applied...)
}

// pacedFleet is an operator process over a fleet. restart replaces
// everything the process holds in memory; the API objects, the MySQL
// servers and the pods stay.
type pacedFleet struct {
	t      *testing.T
	c      client.Client
	scheme *runtime.Scheme
	clk    *clock.FakeClock
	logger *slog.Logger
	groups []*pacedGroup

	rec    *controller.MysqlFailoverGroupReconciler
	runner *controller.TopologyManagerRunner
	pacer  *controller.RolloutPacer

	mu sync.Mutex
	// notReadyAfterRoll names the "<group>/<site>" pods that do not become
	// Ready on a new template: the bad build. podNotReady is the pods that
	// are not Ready now.
	notReadyAfterRoll map[string]bool
	podNotReady       map[string]bool
	// mysqlDownOnRoll: a pod replacement takes the site's MySQL down until
	// the test brings it back (podsBack). Otherwise the servers are left
	// alone by a roll, which is all the pacing assertions need.
	mysqlDownOnRoll bool
}

func pacedSites() []string { return []string{"dc1", "dc2"} }

func newPacedFleet(t *testing.T, specs ...pacedGroupSpec) *pacedFleet {
	t.Helper()
	f := &pacedFleet{
		t: t, clk: clock.NewFakeClock(time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)),
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		notReadyAfterRoll: map[string]bool{}, podNotReady: map[string]bool{},
	}
	f.scheme = runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(f.scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(f.scheme); err != nil {
		t.Fatal(err)
	}
	objs := []client.Object{&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-credentials", Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("bloodraven"), "password": []byte("password"), "MYSQL_ROOT_PASSWORD": []byte("root-password")},
	}}
	for gi, spec := range specs {
		fg := &v1alpha1.MysqlFailoverGroup{
			ObjectMeta: metav1.ObjectMeta{Name: spec.name, Namespace: "default", UID: types.UID("uid-" + spec.name), Generation: 1},
			Spec: v1alpha1.MysqlFailoverGroupSpec{
				Image:       "mysql:9.7",
				Credentials: v1alpha1.CredentialsSpec{OperatorSecretName: "mysql-credentials"},
				Sidecar:     v1alpha1.SidecarSpec{Image: spec.pinnedSidecar},
			},
		}
		if spec.recreate {
			fg.Spec.UpdateStrategy.Type = v1alpha1.UpdateStrategyRecreate
		}
		for si, site := range pacedSites() {
			fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{
				Name: site, ServerID: int64(100*(gi+1) + si), Zone: spec.name + "-" + site,
				LoadBalancerIP: fmt.Sprintf("203.0.113.%d", 10*(gi+1)+si), Storage: v1alpha1.StorageSpec{Size: "1Gi"},
			})
		}
		cfg, err := controller.CRConfigToTopologyConfig(fg)
		if err != nil {
			t.Fatal(err)
		}
		g := &pacedGroup{spec: spec, key: client.ObjectKeyFromObject(fg), cfg: cfg, mysql: map[string]*mockMySQL{
			"dc1": {gtidExecuted: handoffFenced, clearChannelOnReset: true, stopClearsThreads: true},
			"dc2": {readOnly: true, superReadOnly: true, gtidExecuted: handoffFenced, clearChannelOnReset: true, stopClearsThreads: true,
				replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: cfg.Sites[0].Host}},
		}}
		f.groups = append(f.groups, g)
		objs = append(objs, fg)
	}
	f.c = fake.NewClientBuilder().WithScheme(f.scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}, &appsv1.Deployment{}).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := c.Create(ctx, obj, opts...); err != nil {
					return err
				}
				if d, ok := obj.(*appsv1.Deployment); ok {
					f.rolled(ctx, c, d, true)
				}
				return nil
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				o := &client.UpdateOptions{}
				o.ApplyOptions(opts)
				d, ok := obj.(*appsv1.Deployment)
				if !ok || len(o.DryRun) > 0 {
					return c.Update(ctx, obj, opts...)
				}
				var before appsv1.Deployment
				if err := c.Get(ctx, client.ObjectKeyFromObject(d), &before); err != nil {
					return err
				}
				if err := c.Update(ctx, obj, opts...); err != nil {
					return err
				}
				if !equality.Semantic.DeepEqual(before.Spec.Template, d.Spec.Template) {
					f.rolled(ctx, c, d, false)
				}
				return nil
			},
		}).Build()
	return f
}

// rolled is the Deployment controller and the kubelet: a Deployment whose
// pod template was written gets a new pod, which is Ready at once unless
// the test said this pod cannot run the new template.
func (f *pacedFleet) rolled(ctx context.Context, c client.Client, d *appsv1.Deployment, created bool) {
	key := d.Labels[v1alpha1.FailoverGroupLabel] + "/" + d.Labels[v1alpha1.SiteLabel]
	f.mu.Lock()
	f.podNotReady[key] = !created && f.notReadyAfterRoll[key]
	down := !created && f.mysqlDownOnRoll
	f.mu.Unlock()
	if down {
		for _, g := range f.groups {
			if m := g.mysql[d.Labels[v1alpha1.SiteLabel]]; m != nil && g.spec.name == d.Labels[v1alpha1.FailoverGroupLabel] {
				m.setError(errors.New("dial tcp: connect: connection refused (the pod is being replaced)"))
			}
		}
	}
	f.writeDeploymentStatus(ctx, c, d)
}

// podsBack starts the MySQL servers of a group whose pods were replaced,
// the way mysqld boots: read_only=ON, super_read_only OFF, the replication
// channel as it was.
func (f *pacedFleet) podsBack(group string) {
	f.t.Helper()
	for _, m := range f.group(group).mysql {
		m.respawn(true)
	}
}

func (f *pacedFleet) writeDeploymentStatus(ctx context.Context, c client.Client, d *appsv1.Deployment) {
	f.mu.Lock()
	notReady := f.podNotReady[d.Labels[v1alpha1.FailoverGroupLabel]+"/"+d.Labels[v1alpha1.SiteLabel]]
	f.mu.Unlock()
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if notReady {
		d.Status.ReadyReplicas, d.Status.AvailableReplicas, d.Status.UnavailableReplicas = 0, 0, 1
	}
	if err := c.Status().Update(ctx, d); err != nil {
		f.t.Errorf("write status of deployment %s: %v", d.Name, err)
	}
}

// fixPod makes a pod Ready: the build was fixed, as far as this pod goes.
func (f *pacedFleet) fixPod(group, site string) {
	f.t.Helper()
	f.mu.Lock()
	delete(f.notReadyAfterRoll, group+"/"+site)
	delete(f.podNotReady, group+"/"+site)
	f.mu.Unlock()
	f.writeDeploymentStatus(context.Background(), f.c, f.deployment(group, site))
}

func (f *pacedFleet) group(name string) *pacedGroup {
	f.t.Helper()
	for _, g := range f.groups {
		if g.spec.name == name {
			return g
		}
	}
	f.t.Fatalf("no group %s", name)
	return nil
}

func (f *pacedFleet) failoverGroup(name string) *v1alpha1.MysqlFailoverGroup {
	f.t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := f.c.Get(context.Background(), f.group(name).key, &fg); err != nil {
		f.t.Fatalf("get group %s: %v", name, err)
	}
	return &fg
}

func (f *pacedFleet) record(name string) *v1alpha1.RolloutStatus {
	f.t.Helper()
	if rec := f.failoverGroup(name).Status.Rollout; rec != nil {
		return rec
	}
	return &v1alpha1.RolloutStatus{}
}

func (f *pacedFleet) deployment(group, site string) *appsv1.Deployment {
	f.t.Helper()
	var d appsv1.Deployment
	if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "mysql-" + group + "-" + site}, &d); err != nil {
		f.t.Fatalf("get deployment %s/%s: %v", group, site, err)
	}
	return &d
}

// createWorkloads is the release the fleet runs before the upgrade: it
// renders every group, unpaced, and its pods are Ready.
func (f *pacedFleet) createWorkloads() {
	f.t.Helper()
	earlier := &controller.MysqlFailoverGroupReconciler{Client: f.c, APIReader: f.c, Scheme: f.scheme, Recorder: &record.FakeRecorder{}}
	for _, g := range f.groups {
		if _, err := earlier.Reconcile(context.Background(), ctrl.Request{NamespacedName: g.key}); err != nil {
			f.t.Fatalf("Reconcile(%s) by the earlier release: %v", g.spec.name, err)
		}
	}
}

// upgrade is the operator release under test: a new default sidecar image
// and, for the groups that say so, a render-version bump. The upgrade
// restarts the operator.
func (f *pacedFleet) upgrade(sidecar string) {
	f.t.Helper()
	for _, g := range f.groups {
		if !g.spec.renderVersionBump {
			continue
		}
		for _, site := range pacedSites() {
			d := f.deployment(g.spec.name, site)
			d.Annotations[v1alpha1.SpecHashAnnotation] = pacedHashBeforeBump
			d.Spec.Template.Annotations[v1alpha1.SpecHashAnnotation] = pacedHashBeforeBump
			if err := f.c.Update(context.Background(), d); err != nil {
				f.t.Fatalf("stamp %s with the earlier render version: %v", d.Name, err)
			}
		}
	}
	controller.SetSidecarImageDefault(sidecar)
	f.restart()
}

// restart is an operator restart: a new reconciler, runner, pacer and
// topology managers over the same API objects and MySQL servers.
func (f *pacedFleet) restart() {
	f.t.Helper()
	ctx := context.Background()
	recorder := &record.FakeRecorder{} // nil channel: events are dropped
	f.rec = &controller.MysqlFailoverGroupReconciler{Client: f.c, APIReader: f.c, Scheme: f.scheme, Recorder: recorder,
		RolloutPacing: true, Now: f.clk.Now}
	// The rollout wait is on the wall clock; a pod that is not Ready stays so.
	f.rec.SetRolloutWaitForSim(20*time.Millisecond, time.Millisecond)
	f.runner = controller.NewTopologyManagerRunner(f.c, nil, nil, recorder, f.logger)
	f.runner.SetRolloutPacing(true)
	// The stability check is the production one (DeploymentUnstableReason)
	// on the status the managers below write through the runner.
	f.pacer = controller.NewRolloutPacer(f.c, f.c, recorder, f.logger, f.rec, nil,
		controller.RolloutPacerConfig{Soak: -1, StallAfter: 2 * time.Minute})
	f.pacer.SetClock(f.clk.Now)
	for _, g := range f.groups {
		g := g
		fc := controller.NewFailoverController(f.logger)
		uc := controller.NewUpdateController(fc, f.logger)
		uc.SetTickIntervalForTest(time.Millisecond)
		uc.SetRolledSiteTimeoutForTest(50 * time.Millisecond)
		tm := controller.NewTopologyManagerWithClock(g.cfg,
			[]mysql.Checker{g.mysql["dc1"], g.mysql["dc2"]}, fc, uc, nil,
			controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
			newMockTainter(), platform.NewHub(f.logger), &mockDNS{}, f.logger, f.clk)
		tm.RestoreFailoverStateForSim(f.failoverGroup(g.spec.name), controller.NewAnnotationFailoverStateRecorder(f.c, f.c, g.key))
		tm.SetAppliedGeneration(1)
		tm.PodReady = func(_ context.Context, site string) (controller.PodReadiness, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			pod := "mysql-" + g.spec.name + "-" + site + "-0"
			if f.podNotReady[g.spec.name+"/"+site] {
				return controller.PodReadiness{Pod: pod, Reason: "sidecar not ready: crash looping, last exit code 1 (Error)"}, nil
			}
			return controller.PodReadiness{Ready: true, Pod: pod}, nil
		}
		tm.StatusCallback = func(snap controller.TopologySnapshot) {
			tm.MarkStatusWriteResult(f.runner.WriteStatusForTest(ctx, g.key, snap))
		}
		rec := f.rec
		g.apply = func(ctx context.Context, site string) error {
			g.mu.Lock()
			g.applied = append(g.applied, site)
			g.mu.Unlock()
			return rec.ReconcileSiteDeployment(ctx, g.key, site)
		}
		g.tm = tm
	}
}

// pass is one round of the operator's loops: the pacer, then for each
// group its reconcile, its manager's poll, the runner's drift check and
// the ordered update attempt the poll would start, then the pacer again on
// what they wrote. The attempt is started after the poll returned
// (CheckUpdateForSim) and waited for, so a pass has one outcome.
func (f *pacedFleet) pass() {
	f.t.Helper()
	ctx := context.Background()
	if err := f.pacer.Tick(ctx); err != nil {
		f.t.Fatalf("pacer: %v", err)
	}
	for _, g := range f.groups {
		if _, err := f.rec.Reconcile(ctx, ctrl.Request{NamespacedName: g.key}); err != nil {
			f.t.Fatalf("Reconcile(%s): %v", g.spec.name, err)
		}
		g.tm.Poll(ctx)
		g.tm.WaitOrderedUpdateIdleForSim()
		f.runner.CheckSpecDriftForSim(ctx, f.failoverGroup(g.spec.name), g.tm)
		g.tm.ApplyUpdate = g.apply
		g.tm.CheckUpdateForSim(ctx)
		g.tm.WaitOrderedUpdateIdleForSim()
		g.tm.ApplyUpdate = nil
	}
	if err := f.pacer.Tick(ctx); err != nil {
		f.t.Fatalf("pacer: %v", err)
	}
}

// passes runs n rounds, each past the ordered updater's stall back-off.
func (f *pacedFleet) passes(n int) {
	f.t.Helper()
	for i := 0; i < n; i++ {
		f.clk.Advance(31 * time.Second)
		f.pass()
	}
}

// passUntil runs rounds until done reports true.
func (f *pacedFleet) passUntil(what string, done func() bool) {
	f.t.Helper()
	for i := 0; i < 20; i++ {
		if done() {
			return
		}
		f.clk.Advance(31 * time.Second)
		f.pass()
	}
	if !done() {
		var state []string
		for _, g := range f.groups {
			fg := f.failoverGroup(g.spec.name)
			state = append(state, fmt.Sprintf("%s: rollout=%+v unstable=%q applied=%v", g.spec.name, f.record(g.spec.name),
				controller.DeploymentUnstableReason(fg), g.appliedSites()))
		}
		f.t.Fatalf("after 20 passes the fleet did not reach: %s\n%s", what, strings.Join(state, "\n"))
	}
}

// podImages lists the images of a Deployment's containers.
func podImages(d *appsv1.Deployment) []string {
	var images []string
	for _, c := range d.Spec.Template.Spec.InitContainers {
		images = append(images, c.Image)
	}
	for _, c := range d.Spec.Template.Spec.Containers {
		images = append(images, c.Image)
	}
	return images
}

// authorityStatements filters a MySQL server's applied statements down to
// those that move or withdraw write authority, or re-point replication.
func authorityStatements(m *mockMySQL) []string {
	var out []string
	for _, s := range m.statements() {
		switch {
		case s == "Promote", s == "StopReplica", s == "ResetReplicaAll", s == "ChangeReplicationSource", s == "KillAppConnections",
			s == "SetReadOnly:off":
			out = append(out, s)
		}
	}
	return out
}

// pacedSnapshot is a group's workloads and MySQL authority at one moment.
type pacedSnapshot struct {
	deployments map[string]*appsv1.Deployment
	applied     int
}

func (f *pacedFleet) snapshot(group string) pacedSnapshot {
	f.t.Helper()
	snap := pacedSnapshot{deployments: map[string]*appsv1.Deployment{}, applied: len(f.group(group).appliedSites())}
	for _, site := range pacedSites() {
		snap.deployments[site] = f.deployment(group, site)
	}
	return snap
}

// requireUntouched fails unless the group's Deployments are exactly what
// they were at snap, its ordered updater applied nothing since, the drift
// check reports nothing for it, and its MySQL servers are where the
// earlier release left them: dc1 writable and never fenced, dc2 a replica.
func (f *pacedFleet) requireUntouched(group string, snap pacedSnapshot, when string) {
	f.t.Helper()
	g := f.group(group)
	for _, site := range pacedSites() {
		was, now := snap.deployments[site], f.deployment(group, site)
		if !equality.Semantic.DeepEqual(was.Spec, now.Spec) || !equality.Semantic.DeepEqual(was.Annotations, now.Annotations) {
			f.t.Fatalf("%s: the Deployment of %s/%s changed outside the group's slot:\nannotations %v -> %v\nimages %v -> %v",
				when, group, site, was.Annotations, now.Annotations, podImages(was), podImages(now))
		}
	}
	if got := g.appliedSites(); len(got) != snap.applied {
		f.t.Fatalf("%s: the ordered updater of %s applied %v", when, group, got[snap.applied:])
	}
	if drift := g.tm.SpecDriftSites(); len(drift) > 0 {
		f.t.Fatalf("%s: the drift check reports %v for %s, which is not admitted", when, drift, group)
	}
	f.requirePrimaryServing(group, when)
	if got := authorityStatements(g.mysql["dc2"]); len(got) > 0 {
		f.t.Fatalf("%s: the replica of %s received %v", when, group, got)
	}
}

// requireWriteRefused fails unless the Deployment write path refuses to
// roll a site of the group, by the same record the drift check holds it by
// (R-2): the ordered updater's ApplyUpdate of a group that is not admitted
// writes nothing.
func (f *pacedFleet) requireWriteRefused(group, site, when string) {
	f.t.Helper()
	err := f.rec.ReconcileSiteDeployment(context.Background(), f.group(group).key, site)
	if err == nil || !strings.Contains(err.Error(), "held until the group is admitted") {
		f.t.Fatalf("%s: ReconcileSiteDeployment(%s, %s) = %v, want the paced rollout's refusal", when, group, site, err)
	}
}

// requirePrimaryServing fails unless the group's dc1 is writable, was never
// fenced or re-pointed, and dc2 was not promoted.
func (f *pacedFleet) requirePrimaryServing(group, when string) {
	f.t.Helper()
	g := f.group(group)
	primary, replica := g.mysql["dc1"], g.mysql["dc2"]
	if primary.isReadOnly() || primary.isSuperReadOnly() {
		f.t.Fatalf("%s: the primary of %s is fenced", when, group)
	}
	for _, s := range primary.statements() {
		if s == "SetSuperReadOnly:on" || s == "SetReadOnly:on" || s == "ChangeReplicationSource" {
			f.t.Fatalf("%s: the primary of %s received %s (statements %v)", when, group, s, primary.statements())
		}
	}
	if replica.isPromoted() || !replica.isReadOnly() || slices.Contains(replica.statements(), "SetReadOnly:off") {
		f.t.Fatalf("%s: the replica of %s was promoted (statements %v)", when, group, replica.statements())
	}
}

// R-2, R-3, R-4, R-5 and R-7 through the exported API: a release whose
// pods do not become Ready stops at the first group, for every update
// strategy and whatever part of the release changes a group's pods, and
// neither an operator restart nor a second upgrade changes that.
//
// lion's dc2 gets the release's pod and is never Ready. Until that pod is
// fixed lion holds the slot with its roll started and recorded as stalled,
// its primary keeps serving (an ordered update neither fences nor rolls
// it), and tiger keeps every pod it had and has its MySQL left alone:
// through the updater's retries, through an operator restart and through a
// second upgrade. The drift check reports nothing for tiger and the write
// path refuses it, by the same record. With the pod fixed lion completes
// and tiger is rolled next.
func TestPacedRollout_BadBuildHaltsTheFleetAcrossAnOperatorRestart(t *testing.T) {
	cases := []struct {
		name        string
		lion, tiger pacedGroupSpec
		// stallReason is how lion's stall is recorded: by the ordered
		// updater's report of the pod, or by the pacer's own deadline when
		// no updater runs (Recreate).
		stallReason string
		// lionRolled are the sites of lion that carry the release's pod
		// template while it is stalled.
		lionRolled []string
		// tigerSidecar is the sidecar image tiger runs before its roll;
		// tigerSidecarAfter the one it runs after it when it pins one
		// (empty: the release's default).
		tigerSidecar, tigerSidecarAfter string
	}{
		{
			name:         "ordered updates and a new default sidecar image",
			lion:         pacedGroupSpec{name: "lion"},
			tiger:        pacedGroupSpec{name: "tiger"},
			stallReason:  v1alpha1.ReasonOrderedUpdateStalled,
			lionRolled:   []string{"dc2"},
			tigerSidecar: pacedSidecarBefore,
		},
		{
			// R-3: a Recreate group is admitted, latched and completed like
			// any other. R-7: tiger pins its sidecar, so the new default
			// changes nothing it renders, but the render-version bump of the
			// same release does, and that roll waits for the slot too.
			name:         "a Recreate group, then a pinned sidecar with a render-version bump",
			lion:         pacedGroupSpec{name: "lion", recreate: true},
			tiger:        pacedGroupSpec{name: "tiger", pinnedSidecar: pacedSidecarPinned, renderVersionBump: true},
			stallReason:  "StallTimeout",
			lionRolled:   []string{"dc1", "dc2"},
			tigerSidecar: pacedSidecarPinned, tigerSidecarAfter: pacedSidecarPinned,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() { controller.SetSidecarImageDefault(pacedSidecarBefore) })
			controller.SetSidecarImageDefault(pacedSidecarBefore)
			f := newPacedFleet(t, tc.lion, tc.tiger)
			f.createWorkloads()
			f.restart()
			f.passes(4)
			for _, name := range []string{"lion", "tiger"} {
				if fg := f.failoverGroup(name); controller.DeploymentUnstableReason(fg) != "" || fg.Status.Rollout != nil {
					t.Fatalf("before the upgrade %s is unstable (%q) or has a rollout record (%+v)", name,
						controller.DeploymentUnstableReason(fg), fg.Status.Rollout)
				}
			}
			earlierRenderer := f.deployment("lion", "dc1").Annotations[v1alpha1.RenderedByAnnotation]

			f.notReadyAfterRoll["lion/dc2"] = true
			release := pacedSidecarAfter
			f.upgrade(release)
			tiger := f.snapshot("tiger")
			lionPrimary := f.deployment("lion", "dc1")

			halted := func(when string, stalledAt *metav1.Time) {
				t.Helper()
				rec := f.record("lion")
				if rec.Phase != v1alpha1.RolloutPhaseStalled || rec.Reason != tc.stallReason || !rec.Started() {
					t.Fatalf("%s: lion's record is %+v, want its roll started and Stalled/%s", when, rec, tc.stallReason)
				}
				if stalledAt != nil && !rec.StalledTime.Equal(stalledAt) {
					t.Fatalf("%s: lion's stall moved from %s to %s", when, stalledAt, rec.StalledTime)
				}
				if rec := f.record("tiger"); rec.Phase != v1alpha1.RolloutPhasePending || rec.Reason != "RolloutHalted" || rec.InFlight() {
					t.Fatalf("%s: tiger's record is %+v, want Pending/RolloutHalted", when, rec)
				}
				for _, site := range pacedSites() {
					f.requireWriteRefused("tiger", site, when)
				}
				f.requireUntouched("tiger", tiger, when)
				if !slices.Contains(podImages(f.deployment("tiger", "dc1")), tc.tigerSidecar) {
					t.Fatalf("%s: tiger/dc1 runs %v, want its sidecar %s", when, podImages(f.deployment("tiger", "dc1")), tc.tigerSidecar)
				}
				// lion: the sites the release reached, and no further.
				for _, site := range pacedSites() {
					d := f.deployment("lion", site)
					rolled := d.Annotations[v1alpha1.RenderedByAnnotation] != earlierRenderer && slices.Contains(podImages(d), release)
					if rolled != slices.Contains(tc.lionRolled, site) {
						t.Fatalf("%s: lion/%s rolled=%v (renderer %q, images %v), want the release on %v only", when, site, rolled,
							d.Annotations[v1alpha1.RenderedByAnnotation], podImages(d), tc.lionRolled)
					}
				}
				f.requirePrimaryServing("lion", when)
				if !tc.lion.recreate {
					// The ordered updater stopped on the replica's pod: it
					// did not go on to hand the primary over and roll it.
					if now := f.deployment("lion", "dc1"); !equality.Semantic.DeepEqual(now.Spec, lionPrimary.Spec) {
						t.Fatalf("%s: lion's primary was rolled while its replica's pod is not Ready", when)
					}
					if got := f.group("lion").appliedSites(); slices.Contains(got, "dc1") {
						t.Fatalf("%s: lion's ordered updater applied %v", when, got)
					}
				} else if got := f.group("lion").appliedSites(); len(got) > 0 {
					t.Fatalf("%s: an ordered update ran for the Recreate group lion: %v", when, got)
				}
			}

			f.passUntil("lion stalled", func() bool { return f.record("lion").StalledTime != nil })
			if rec := f.record("lion"); rec.StartedTime == nil || rec.AdmittedTime.After(rec.StartedTime.Time) {
				t.Fatalf("lion's record %+v: the start was not latched after the admission", rec)
			}
			stalledAt := f.record("lion").StalledTime.DeepCopy()
			f.pass()
			halted("after lion stalled", stalledAt)
			lionApplied := f.group("lion").appliedSites()

			// Retries change nothing.
			f.passes(3)
			halted("after retries", stalledAt)
			if got := f.group("lion").appliedSites(); !slices.Equal(got, lionApplied) {
				t.Fatalf("retries applied %v (was %v): the stalled site was rolled again", got, lionApplied)
			}

			// An operator restart: the admission, the latch and the halt
			// come back from the API, and the new process rolls nothing.
			f.restart()
			f.passes(4)
			halted("after an operator restart", stalledAt)
			if got := f.group("lion").appliedSites(); !slices.Equal(got, lionApplied) {
				t.Fatalf("after the restart lion's ordered updater applied %v (was %v)", got, lionApplied)
			}

			// A second upgrade mid-rollout: another default sidecar image,
			// so another renderer and another pod template for lion. The
			// unfinished admission is retargeted: lion keeps the slot and
			// its stall, and gets the newer template where the release had
			// reached; tiger still waits.
			firstRenderer := f.record("lion").Renderer
			release = pacedSidecarSecond
			f.upgrade(release)
			f.passes(4)
			halted("after a second upgrade", stalledAt)
			if rec := f.record("lion"); rec.Renderer == firstRenderer || rec.Renderer != f.record("tiger").Renderer {
				t.Fatalf("after a second upgrade lion's record names renderer %q (the first upgrade's was %q, tiger's names %q)",
					rec.Renderer, firstRenderer, f.record("tiger").Renderer)
			}
			if got := f.group("lion").appliedSites(); slices.Contains(got, "dc1") {
				t.Fatalf("after a second upgrade lion's ordered updater applied %v: its primary was rolled", got)
			}

			// The pod is fixed: lion finishes, then tiger gets the slot.
			f.fixPod("lion", "dc2")
			f.passUntil("both groups complete", func() bool {
				return f.record("lion").Phase == v1alpha1.RolloutPhaseComplete && f.record("tiger").Phase == v1alpha1.RolloutPhaseComplete
			})
			renderer := f.deployment("lion", "dc1").Annotations[v1alpha1.RenderedByAnnotation]
			if renderer == earlierRenderer || renderer == "" {
				t.Fatalf("lion/dc1 is rendered by %q after its roll (the earlier release was %q)", renderer, earlierRenderer)
			}
			for _, name := range []string{"lion", "tiger"} {
				if rec := f.record(name); rec.StalledTime != nil || rec.CompletedTime == nil || rec.StartedTime == nil {
					t.Fatalf("%s's record is %+v, want a completed roll", name, rec)
				}
				for _, site := range pacedSites() {
					d := f.deployment(name, site)
					want := release
					if name == "tiger" && tc.tigerSidecarAfter != "" {
						want = tc.tigerSidecarAfter
					}
					if d.Annotations[v1alpha1.RenderedByAnnotation] != renderer || d.Annotations[v1alpha1.SpecHashAnnotation] == pacedHashBeforeBump ||
						!slices.Contains(podImages(d), want) {
						t.Fatalf("%s/%s after the rollout: renderer %q, hash %q, images %v; want the release's template with sidecar %s",
							name, site, d.Annotations[v1alpha1.RenderedByAnnotation], d.Annotations[v1alpha1.SpecHashAnnotation], podImages(d), want)
					}
				}
			}
			if lionDone, tigerAdmitted := f.record("lion").CompletedTime, f.record("tiger").AdmittedTime; tigerAdmitted.Time.Before(lionDone.Time) {
				t.Fatalf("tiger was admitted at %s, before lion completed at %s", tigerAdmitted, lionDone)
			}
			// tiger is an OrderedUpdate group in both cases: its roll went
			// replica first, then the handoff, then the old primary.
			if got := f.group("tiger").appliedSites(); !slices.Equal(got, []string{"dc2", "dc1"}) {
				t.Fatalf("tiger's ordered updater applied %v, want dc2 then dc1", got)
			}
			if g := f.group("tiger"); !g.mysql["dc2"].isPromoted() && g.mysql["dc2"].isReadOnly() {
				t.Fatal("tiger's roll did not hand the active site to dc2")
			}
		})
	}
}

// A finding of the paced-rollout simulation (internal/dst, README "Paced
// rollouts"; first seen as rollout seed 77), outside the pacing itself.
//
// A Recreate roll replaces the pods of both sites at once. While they are
// away the group has no primary and its status says so: status.activeSite
// is empty. The operator that rolled them remembers which site was the
// primary (lastActiveSite) and makes it writable again once mysqld is back
// with the read_only=ON it boots with. An operator that restarts in
// between (a second helm upgrade, an eviction, a change of leader) has
// only the status to go by, and a group that never failed over has no
// failover record either: it finds two read-only sites and no authority on
// record, and elects nobody. The group stays without a primary until a
// human acts (the documented needs-human state of the failover trials,
// checkWedge: no history), and the rollout stays halted behind it: the
// group's roll started and cannot complete.
//
// The first case is the limitation; it fails when a restarted operator
// re-asserts the primary, and README and rolloutSim.needsHuman then need
// the same update. The second case is the control: without the restart the
// same roll heals by itself.
func TestKnownLimitationOperatorRestartDuringRecreateRollLeavesNoPrimary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart bool
	}{
		{name: "the operator restarts while the pods are away", restart: true},
		{name: "the operator keeps running", restart: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() { controller.SetSidecarImageDefault(pacedSidecarBefore) })
			controller.SetSidecarImageDefault(pacedSidecarBefore)
			f := newPacedFleet(t, pacedGroupSpec{name: "lion", recreate: true}, pacedGroupSpec{name: "tiger"})
			f.createWorkloads()
			f.restart()
			f.passes(4)
			if got := f.failoverGroup("lion").Status.ActiveSite; got != "dc1" {
				t.Fatalf("before the upgrade lion's status.activeSite = %q, want dc1", got)
			}
			if fg := f.failoverGroup("lion"); fg.Status.LastFailover != nil {
				t.Fatalf("lion has a failover record before the upgrade: %+v", fg.Status.LastFailover)
			}

			f.mysqlDownOnRoll = true
			f.upgrade(pacedSidecarAfter)
			tiger := f.snapshot("tiger")
			f.passUntil("lion's pods replaced and its status without an active site", func() bool {
				return f.record("lion").Started() && f.failoverGroup("lion").Status.ActiveSite == ""
			})
			if tc.restart {
				f.restart()
			}
			f.podsBack("lion")
			// Only lion's pod replacement is of interest here.
			f.mu.Lock()
			f.mysqlDownOnRoll = false
			f.mu.Unlock()
			f.passes(8)

			lion := f.group("lion")
			fg := f.failoverGroup("lion")
			if !tc.restart {
				if lion.mysql["dc1"].isReadOnly() || fg.Status.ActiveSite != "dc1" {
					t.Fatalf("the operator that rolled lion did not make dc1 writable again: readOnly=%v status.activeSite=%q",
						lion.mysql["dc1"].isReadOnly(), fg.Status.ActiveSite)
				}
				f.passUntil("both groups complete", func() bool {
					return f.record("lion").Phase == v1alpha1.RolloutPhaseComplete && f.record("tiger").Phase == v1alpha1.RolloutPhaseComplete
				})
				return
			}
			if !lion.mysql["dc1"].isReadOnly() || !lion.mysql["dc2"].isReadOnly() {
				t.Fatalf("lion has a writable site again (dc1 readOnly=%v, dc2 readOnly=%v): the limitation is gone; "+
					"update this test, internal/dst/README.md and rolloutSim.needsHuman",
					lion.mysql["dc1"].isReadOnly(), lion.mysql["dc2"].isReadOnly())
			}
			if got := controller.DeploymentUnstableReason(fg); got != v1alpha1.UnstableReasonNoPrimary || fg.Status.ActiveSite != "" {
				t.Fatalf("lion: unstable=%q status.activeSite=%q, want NoPrimary and no active site", got, fg.Status.ActiveSite)
			}
			// The rollout is halted behind it, as behind any roll that
			// started and cannot complete.
			if rec := f.record("lion"); !rec.Started() || rec.StalledTime == nil {
				t.Fatalf("lion's record is %+v, want its roll started and stalled", rec)
			}
			if rec := f.record("tiger"); rec.Phase != v1alpha1.RolloutPhasePending || rec.Reason != "RolloutHalted" {
				t.Fatalf("tiger's record is %+v, want Pending/RolloutHalted", rec)
			}
			f.requireUntouched("tiger", tiger, "behind the group without a primary")
		})
	}
}
