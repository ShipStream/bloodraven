package controller

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// driftFixture is a group with its digest key and one Deployment per site
// that carries the hash (and pitr-render annotation) of render.
func driftFixture(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, annotate bool) []client.Object {
	t.Helper()
	key := bytes.Repeat([]byte{1}, digestKeyBytes)
	objs := []client.Object{
		fg.DeepCopy(),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: digestKeySecretName(fg.Name), Namespace: fg.Namespace}, Data: map[string][]byte{digestKeyDataKey: key}},
	}
	render := testPITRRender(fg)
	for _, site := range fg.Spec.Sites {
		ann := map[string]string{v1alpha1.SpecHashAnnotation: ComputeSpecHash(fg, site, secretContentDigest(key, map[string]map[string][]byte{}), render)}
		if annotate {
			ann[v1alpha1.PITRRenderAnnotation] = render.Canonical()
		}
		objs = append(objs, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(fg.Name, site.Name), Namespace: fg.Namespace,
			Labels: commonLabels(fg.Name, site.Name), Annotations: ann,
		}})
	}
	return objs
}

// S5: drift seen on a lagging listed copy of the group (here: a spec the
// Deployments have already moved past) is confirmed against an uncached
// read; only sites that still differ are recorded.
func TestCheckSpecDriftConfirmsUncached(t *testing.T) {
	ctx := context.Background()
	fresh := pitrRolloutFG(false)
	stale := fresh.DeepCopy()
	stale.Spec.Image = "mysql:9.6" // the cached copy predates the image change
	objs := driftFixture(t, fresh, true)
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).Build()

	cachedOnly := &TopologyManagerRunner{client: c, logger: testLogger()}
	tm := &TopologyManager{}
	cachedOnly.checkSpecDrift(ctx, stale, tm)
	if len(tm.specDriftSites) == 0 {
		t.Fatal("the stale copy shows no drift; the fixture does not exercise the confirm")
	}

	r := &TopologyManagerRunner{client: c, apiReader: c, logger: testLogger()}
	tm = &TopologyManager{}
	r.checkSpecDrift(ctx, stale, tm)
	if len(tm.specDriftSites) != 0 {
		t.Fatalf("drift from a stale listed copy recorded: %v", tm.specDriftSites)
	}

	// Real drift (an image change) survives the confirm.
	var g v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, client.ObjectKeyFromObject(fresh), &g); err != nil {
		t.Fatal(err)
	}
	g.Spec.Image = "mysql:9.8"
	if err := c.Update(ctx, &g); err != nil {
		t.Fatal(err)
	}
	r.checkSpecDrift(ctx, &g, tm)
	if strings.Join(tm.specDriftSites, ",") != "dc1,dc2" {
		t.Fatalf("real drift = %v, want both sites", tm.specDriftSites)
	}
}

// A failed uncached re-read keeps the current drift set.
func TestCheckSpecDriftFailedConfirmKeepsSet(t *testing.T) {
	ctx := context.Background()
	fg := pitrRolloutFG(false)
	objs := driftFixture(t, fg, true)
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
	failing := fake.NewClientBuilder().WithScheme(testScheme()).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("apiserver unavailable")
		},
	}).Build()
	changed := fg.DeepCopy()
	changed.Spec.Image = "mysql:9.8"
	r := &TopologyManagerRunner{client: c, apiReader: failing, logger: testLogger()}
	tm := &TopologyManager{specDriftSites: []string{"dc2"}}
	r.checkSpecDrift(ctx, changed, tm)
	if strings.Join(tm.specDriftSites, ",") != "dc2" {
		t.Fatalf("failed re-read replaced the drift set: %v", tm.specDriftSites)
	}
}

// §4.6 row 4d: Deployments without a decodable pitr-render annotation (a
// pre-#203 render) while the write binding is not Bound are deferred, and a
// deferred site is never drift, whatever else changed.
func TestCheckSpecDriftSkipsDeferredSites(t *testing.T) {
	ctx := context.Background()
	fg := pitrRolloutFG(false)
	fg.Status.PITRArchiveBindings[0].State = v1alpha1.PITRArchiveBindingPending
	fg.Status.PITRArchiveBindings[0].Reason = v1alpha1.PITRArchiveBindingReasonAuthenticating
	objs := driftFixture(t, fg, false)
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
	if _, action := resolvePITRRender(fg, deploymentsOf(objs)); action != pitrRenderDefer {
		t.Fatalf("action = %s, want Defer", action)
	}
	changed := fg.DeepCopy()
	changed.Spec.Image = "mysql:9.8"
	r := &TopologyManagerRunner{client: c, apiReader: c, logger: testLogger()}
	tm := &TopologyManager{}
	r.checkSpecDrift(ctx, changed, tm)
	if len(tm.specDriftSites) != 0 {
		t.Fatalf("deferred sites recorded as drift: %v", tm.specDriftSites)
	}
}

func deploymentsOf(objs []client.Object) []appsv1.Deployment {
	var out []appsv1.Deployment
	for _, o := range objs {
		if d, ok := o.(*appsv1.Deployment); ok {
			out = append(out, *d)
		}
	}
	return out
}

// Δ13: the runner's status write keeps the bindings and the binding
// condition from the object it re-reads, never from the older copy its
// snapshot was built on.
func TestRunnerStatusPreservesPITRBindings(t *testing.T) {
	ctx := context.Background()
	fg := pitrRolloutFG(false)
	fg.Status.ActiveSite = "dc1"
	setCondition(&fg.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionPITRArchiveBindingReady, Status: metav1.ConditionTrue, Reason: string(v1alpha1.PITRArchiveBindingReasonBound), Message: "bound"})
	want := fg.Status.PITRArchiveBindings[0]
	gets := 0
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg.DeepCopy()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if g, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
					gets++
					if gets == 1 {
						// The first read is the stale copy: no binding, no condition.
						g.Status.PITRArchiveBindings = nil
						removeCondition(&g.Status.Conditions, v1alpha1.ConditionPITRArchiveBindingReady)
					}
				}
				return nil
			},
		}).Build()
	r := &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	snap := TopologySnapshot{Sites: []SiteSnapshot{{Name: "dc1", State: state.StateWritable}, {Name: "dc2", State: state.StateReadOnly}}, ActiveSite: "dc1"}
	if err := r.updateCRStatus(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.PITRArchiveBindings) != 1 || got.Status.PITRArchiveBindings[0].ID != want.ID || got.Status.PITRArchiveBindings[0].State != want.State {
		t.Fatalf("bindings after the runner's write = %+v, want %+v", got.Status.PITRArchiveBindings, want)
	}
	if c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionPITRArchiveBindingReady); c == nil || c.Message != "bound" {
		t.Fatalf("binding condition after the runner's write = %+v", c)
	}
}

// The runner's status write replaces the status with the copy it read first.
// status.rollout is not the runner's: the copy can be from before the write
// path latched the start of the roll, and writing it back would let the
// pacer withdraw an admission that has changed a workload, and hand the slot
// to another group.
func TestUpdateCRStatusKeepsTheLiveRolloutRecord(t *testing.T) {
	ctx := context.Background()
	fg := pitrRolloutFG(false)
	fg.Status.ActiveSite = "dc1"
	started := metav1.Now()
	fg.Status.Rollout = &v1alpha1.RolloutStatus{
		Renderer: rendererIdentity(), Phase: v1alpha1.RolloutPhaseRolling, Reason: rolloutReasonStarted,
		AdmittedTime: &started, StartedTime: &started,
	}
	gets := 0
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg.DeepCopy()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if g, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
					gets++
					if gets == 1 {
						// The first read is the stale copy: admitted, not latched.
						g.Status.Rollout.StartedTime = nil
						g.Status.Rollout.Phase = v1alpha1.RolloutPhaseAdmitted
					}
				}
				return nil
			},
		}).Build()
	r := &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	snap := TopologySnapshot{Sites: []SiteSnapshot{{Name: "dc1", State: state.StateWritable}, {Name: "dc2", State: state.StateReadOnly}}, ActiveSite: "dc1"}
	if err := r.updateCRStatus(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	if gets < 2 {
		t.Fatalf("the stale read was not staged (%d reads)", gets)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Rollout == nil || got.Status.Rollout.StartedTime == nil || got.Status.Rollout.Phase != v1alpha1.RolloutPhaseRolling {
		t.Fatalf("status.rollout = %+v, want the live record with its start latch", got.Status.Rollout)
	}
}
