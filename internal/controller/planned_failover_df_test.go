package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// plannedFailoverFGWithDragonfly returns a fixture whose run (run-1) is
// in WaitingForDragonflySync: two sites, active=iad, target=pdx, source
// fenced, MySQL caught up, accepted with the Dragonfly settings.
func plannedFailoverFGWithDragonfly(activePolicy string) *v1alpha1.MysqlFailoverGroup {
	fg := plannedFailoverFG("")
	fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{
		Enabled:   true,
		Image:     "docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0",
		Port:      6379,
		AdminPort: 9999,
		PlannedFailover: &v1alpha1.DragonflyPlannedFailoverSpec{
			MaxSyncWait:   &metav1.Duration{Duration: 100 * time.Millisecond},
			OnSyncTimeout: activePolicy,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
	}
	now := metav1.Now()
	pf := pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, now.Time)
	pf.Phase = v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync
	pf.LagWait.CaughtUpAt = &now
	settings := plannedFailoverSettingsFor(fg, PlannedFailoverRequest{Site: "pdx"})
	pf.Settings = &settings
	pf.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{SyncWaitStartTime: &now}
	fg.Status.PlannedFailover = pf
	return fg
}

// installFakeDragonflyConn wires a fake connector returning the given
// per-site connections.
func installFakeDragonflyConn(t *testing.T, r *MysqlFailoverGroupReconciler, byAddr map[string]DragonflyConnection) {
	t.Helper()
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		if c, ok := byAddr[addr]; ok {
			return c, nil
		}
		return nil, errors.New("no programmed conn for " + addr)
	}
}

const (
	pfDFPdx = "lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379"
	pfDFIad = "lion-dragonfly-iad.shared-lion.svc.cluster.local:6379"
)

// pfTakeoverConn is a target replica that becomes master on REPLTAKEOVER.
type pfTakeoverConn struct {
	*fakeDragonflyConn
	takeovers int
}

func (c *pfTakeoverConn) ReplTakeover(ctx context.Context, timeout time.Duration) error {
	c.takeovers++
	if err := c.fakeDragonflyConn.ReplTakeover(ctx, timeout); err != nil {
		return err
	}
	c.info.Role = "master"
	return nil
}

// pfDFReconciler returns a reconciler over fg (and objs) whose engine runs
// with the production Dragonfly operations and a scripted MySQL facade
// holding run-1's claim.
func pfDFReconciler(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, objs ...client.Object) (*MysqlFailoverGroupReconciler, client.Client, *pfFakeTopology) {
	t.Helper()
	r, c := newReconciler(append([]client.Object{fg}, objs...)...)
	r.Recorder = record.NewFakeRecorder(64)
	topo := newPFFakeTopology()
	topo.setHeld("run-1", false)
	r.PlannedTopology = topo
	r.plannedClock = clock.NewFakeClock(time.Now())
	return r, c, topo
}

func pfDFPass(t *testing.T, r *MysqlFailoverGroupReconciler, nn types.NamespacedName) {
	t.Helper()
	if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
		t.Fatalf("pass: %v", err)
	}
}

func TestPlannedFailoverDragonflySync_DisabledSkips(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Spec.Dragonfly = nil
	r, _, _ := pfDFReconciler(t, fg)
	pfDFPass(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly == nil || pf.Dragonfly.Takeover != nil {
		t.Fatalf("block = %+v", pf)
	}
}

// Capture the source offset, see the target in sync, then take over: the
// MySQL Promoting intent records the takeover's success.
func TestPlannedFailoverDragonfly_HappyPath(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
	r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
		Role: "slave", MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0, SlaveReplOffset: 100}}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})
	nn := fgNN(fg)

	pfDFPass(t, r, nn)
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Dragonfly.SourceOffsetState != v1alpha1.DragonflyOffsetCaptured || pf.Dragonfly.SourceOffset == nil || *pf.Dragonfly.SourceOffset != 100 {
		t.Fatalf("offset capture = %+v", pf.Dragonfly)
	}
	pfDFPass(t, r, nn)
	pf = fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly || pf.Dragonfly.Takeover == nil ||
		pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverIntended {
		t.Fatalf("after sync = %+v %+v", pf.Phase, pf.Dragonfly)
	}
	if !plannedFailoverDragonflyStripActive(fetchFG(t, r, nn)) {
		t.Error("strip predicate false while the takeover is Intended")
	}
	pfDFPass(t, r, nn)
	got := fetchFG(t, r, nn)
	pf = got.Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded ||
		pf.Dragonfly.Takeover.Method != v1alpha1.DragonflyTakeoverMethodReplTakeover || pf.Dragonfly.Sessions != v1alpha1.DragonflySessionsPreserved ||
		pf.Promotion == nil || pf.Promotion.TargetServerUUID != "uuid-pdx" {
		t.Fatalf("after takeover = %+v %+v %+v", pf.Phase, pf.Dragonfly, pf.Promotion)
	}
	if target.takeovers != 1 {
		t.Errorf("REPLTAKEOVER sent %d times, want 1", target.takeovers)
	}
	if plannedFailoverDragonflyStripActive(got) || effectiveDragonflyMasterSite(got) != "pdx" {
		t.Errorf("strip=%v master=%q after a successful takeover", plannedFailoverDragonflyStripActive(got), effectiveDragonflyMasterSite(got))
	}
	var gotTarget corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(targetPod), &gotTarget); err != nil {
		t.Fatal(err)
	}
	if gotTarget.Labels[labelDragonflyRole] != "master" || gotTarget.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("target labels = %v", gotTarget.Labels)
	}
	if len(source.clientKillTypes) == 0 || source.clientKillTypes[0] != "NORMAL" {
		t.Errorf("CLIENT KILL on the old master = %v", source.clientKillTypes)
	}
}

// A re-run whose takeover intent is durable but whose result was lost
// finds the target master and never sends a second REPLTAKEOVER.
func TestPlannedFailoverDragonfly_RerunNeverRepeatsTakeover(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	now := metav1.Now()
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly.SourceOffsetState = v1alpha1.DragonflyOffsetCaptured
	fg.Status.PlannedFailover.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: now}
	r, _, _ := pfDFReconciler(t, fg)
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: &fakeDragonflyConn{}})
	pfDFPass(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded {
		t.Fatalf("block = %+v %+v", pf.Phase, pf.Dragonfly)
	}
	if target.takeovers != 0 {
		t.Errorf("REPLTAKEOVER re-sent %d times on a target already master", target.takeovers)
	}
}

// onSyncTimeout=proceed goes on to the takeover with sessions lost.
func TestPlannedFailoverDragonflySync_TimeoutProceedAdvances(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Dragonfly.SourceOffsetState = v1alpha1.DragonflyOffsetCaptured
	offset := int64(500)
	fg.Status.PlannedFailover.Dragonfly.SourceOffset = &offset
	r, _, _ := pfDFReconciler(t, fg)
	lagging := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 10}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: lagging, pfDFIad: &fakeDragonflyConn{}})
	r.plannedClock.(*clock.FakeClock).Advance(time.Second)
	pfDFPass(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly || pf.Dragonfly.Sessions != v1alpha1.DragonflySessionsLost ||
		pf.Dragonfly.Reason != string(v1alpha1.DragonflyPlannedReasonSyncTimeout) {
		t.Fatalf("block = %+v %+v", pf.Phase, pf.Dragonfly)
	}
}

// An unreachable source offset proceeds with sessions lost.
func TestPlannedFailoverDragonflySync_OffsetCaptureFailsProceedsWithSessionsLost(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	r, _, _ := pfDFReconciler(t, fg)
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{})
	pfDFPass(t, r, fgNN(fg))
	if pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; pf.Dragonfly.SourceOffsetState != v1alpha1.DragonflyOffsetUnavailable {
		t.Fatalf("dragonfly = %+v", pf.Dragonfly)
	}
	pfDFPass(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly || pf.Dragonfly.Sessions != v1alpha1.DragonflySessionsLost {
		t.Fatalf("block = %+v %+v", pf.Phase, pf.Dragonfly)
	}
}

// onSyncTimeout=fail rolls back.
func TestPlannedFailoverDragonflySync_TimeoutFailRollsBack(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("fail")
	fg.Status.PlannedFailover.Dragonfly.SourceOffsetState = v1alpha1.DragonflyOffsetUnavailable
	r, _, _ := pfDFReconciler(t, fg)
	pfDFPass(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonDragonflySyncTimeout {
		t.Fatalf("block = %+v %+v", pf.Phase, pf.Rollback)
	}
}

// A failed strip short-circuits the takeover (no dial), restores nothing it
// did not change, and proceeds with the takeover Failed.
func TestPlannedFailoverDragonfly_StripFailsProceeds(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	now := metav1.Now()
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: now}
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, newTestSecret(), sourcePod, makeDragonflyPod(fg.Name, "pdx", "replica", true)).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, under client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if pod, ok := obj.(*corev1.Pod); ok && pod.Name == sourcePod.Name {
					if _, has := pod.Labels[labelDragonflyTraffic]; !has {
						return errors.New("simulated strip-patch failure")
					}
				}
				return under.Update(ctx, obj, opts...)
			},
		}).Build()
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
	topo := newPFFakeTopology()
	topo.setHeld("run-1", false)
	r.PlannedTopology = topo
	r.plannedClock = clock.NewFakeClock(time.Now())
	dials := 0
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) {
		dials++
		return nil, errors.New("must not dial")
	}
	pfDFPass(t, r, fgNN(fg))
	if dials != 0 {
		t.Errorf("dialled %d times after a failed strip", dials)
	}
	got := fetchFG(t, r, fgNN(fg))
	pf := got.Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed ||
		pf.Dragonfly.Reason != ReasonDragonflyPromotionFailed || !strings.Contains(pf.Dragonfly.Message, "strip source traffic label") {
		t.Fatalf("block = %+v %+v", pf.Phase, pf.Dragonfly)
	}
	if effectiveDragonflyMasterSite(got) != "iad" {
		t.Errorf("master = %q after a failed takeover, want the source", effectiveDragonflyMasterSite(got))
	}
}

// A failed REPLTAKEOVER restores the source's traffic label; with
// onSyncTimeout=fail the run rolls back with the takeover recorded Failed.
func TestPlannedFailoverDragonfly_TakeoverFailsFailRollsBack(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("fail")
	now := metav1.Now()
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: now}
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	r, c, _ := pfDFReconciler(t, fg, sourcePod, makeDragonflyPod(fg.Name, "pdx", "replica", true))
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: errors.New("takeover refused")}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: &fakeDragonflyConn{}})
	pfDFPass(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed ||
		pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed {
		t.Fatalf("block = %+v %+v %+v", pf.Phase, pf.Rollback, pf.Dragonfly)
	}
	var gotSource corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(sourcePod), &gotSource); err != nil {
		t.Fatal(err)
	}
	if gotSource.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("source traffic label not restored after a failed takeover: %v", gotSource.Labels)
	}
}

// The source-loss window still applies in the Dragonfly phases.
func TestPlannedFailoverDragonfly_SourceLossRollsBack(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Settings.Dragonfly.MaxSyncWait = metav1.Duration{Duration: time.Hour}
	r, _, topo := pfDFReconciler(t, fg)
	topo.probe = func(PlannedRunRef) (LagProbe, error) { return LagProbe{TargetReachable: true}, nil }
	lagging := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 10}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: lagging,
		pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 500}}})
	clk := r.plannedClock.(*clock.FakeClock)
	for i := 0; i < 12 && fetchFG(t, r, fgNN(fg)).Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseRollingBack; i++ {
		pfDFPass(t, r, fgNN(fg))
		clk.Advance(time.Second)
	}
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonSourceLost {
		t.Fatalf("block = %+v %+v", pf.Phase, pf.Rollback)
	}
}

// The strip predicate and the Dragonfly master follow the takeover record,
// and a marked run strips nothing.
func TestPlannedFailoverDragonflyPredicates(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	pf := fg.Status.PlannedFailover
	now := metav1.Now()
	pf.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	pf.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: now}
	if !plannedFailoverDragonflyStripActive(fg) || plannedFailoverSourceSite(fg) != "iad" || effectiveDragonflyMasterSite(fg) != "iad" {
		t.Errorf("intended: strip=%v source=%q master=%q", plannedFailoverDragonflyStripActive(fg), plannedFailoverSourceSite(fg), effectiveDragonflyMasterSite(fg))
	}
	for _, phase := range []v1alpha1.PlannedFailoverPhase{v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhasePublishing, v1alpha1.PlannedFailoverPhaseRollingBack} {
		pf.Phase = phase
		pf.Dragonfly.Takeover.State = v1alpha1.DragonflyTakeoverSucceeded
		if plannedFailoverDragonflyStripActive(fg) || effectiveDragonflyMasterSite(fg) != "pdx" {
			t.Errorf("%s/succeeded: strip=%v master=%q", phase, plannedFailoverDragonflyStripActive(fg), effectiveDragonflyMasterSite(fg))
		}
	}
	pf.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	pf.Dragonfly.Takeover.State = v1alpha1.DragonflyTakeoverIntended
	fg.Annotations = map[string]string{PlannedFailoverTerminalAnnotation: "run-1/SourceLeftFenced/StatusStalled"}
	if plannedFailoverDragonflyStripActive(fg) || plannedFailoverSourceSite(fg) != "" {
		t.Error("a marked run still strips the source")
	}
}

// pfDragonflyTakeoverPass runs the engine's PromotingDragonfly step for the
// group's stored run, the way a reconcile pass of that phase does: the
// takeover intent is seeded when the fixture lacks it, and an empty facade
// fake holds the run's claim unless the test injected one.
func pfDragonflyTakeoverPass(t *testing.T, r *MysqlFailoverGroupReconciler, ctx context.Context, nn types.NamespacedName) (time.Duration, error) {
	t.Helper()
	if r.PlannedTopology == nil && r.plannedEngine == nil {
		topo := newPFFakeTopology()
		r.PlannedTopology = topo
	}
	var live v1alpha1.MysqlFailoverGroup
	if err := r.Client.Get(ctx, nn, &live); err != nil {
		t.Fatal(err)
	}
	pf := live.Status.PlannedFailover
	if pf.RunID == "" {
		pf.RunID = "run-1"
	}
	if pf.Source == "" {
		pf.Source = pf.SourcePrimary
	}
	if pf.Settings == nil {
		settings := plannedFailoverSettingsFor(&live, PlannedFailoverRequest{Site: pf.Target})
		pf.Settings = &settings
	}
	if pf.Dragonfly == nil {
		pf.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{}
	}
	if pf.Dragonfly.Takeover == nil {
		pf.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: metav1.Now()}
	}
	if err := r.Client.Status().Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	if err := r.Client.Get(ctx, nn, &live); err != nil {
		t.Fatal(err)
	}
	e := r.plannedFailoverEngine()
	if topo, ok := e.topo.(*pfFakeTopology); ok {
		topo.setHeld(live.Status.PlannedFailover.RunID, false)
	}
	pf = live.Status.PlannedFailover
	return e.dragonflyTakeoverStep(ctx, &live, pf, plannedRunRef(nn, pf))
}
