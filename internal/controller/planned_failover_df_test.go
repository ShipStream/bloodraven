package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// plannedFailoverFGWithDragonfly returns a fixture that's mid-flight
// with PlannedFailoverPhaseWaitingForDragonflySync queued. Two sites,
// active=iad, target=pdx, source already fenced, MySQL caught up.
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
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:             v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		Target:            "pdx",
		SourcePrimary:     "iad",
		SourceGtidAtFence: "uuid:1-100",
		StartTime:         &now,
		LagWaitStartTime:  &now,
	}
	return fg
}

// installFakeDragonflyConn wires a fake connector returning the given
// per-site connections.
func installFakeDragonflyConn(t *testing.T, r *MysqlFailoverGroupReconciler, byAddr map[string]*fakeDragonflyConn) {
	t.Helper()
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		if c, ok := byAddr[addr]; ok {
			return c, nil
		}
		return nil, errors.New("no programmed conn for " + addr)
	}
}

func TestPlannedFailoverWaitingForDragonflySync_DisabledSkips(t *testing.T) {
	fg := plannedFailoverFG("") // no spec.dragonfly
	now := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:             v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		Target:            "pdx",
		SourcePrimary:     "iad",
		SourceGtidAtFence: "uuid:1-100",
		StartTime:         &now,
	}
	r, _ := newReconciler(fg)
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Errorf("phase = %q, want Promoting (skipped)", got.Status.PlannedFailover.Phase)
	}
}

func TestPlannedFailoverWaitingForDragonflySync_HappyPath(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	r, _ := newReconciler(fg)

	dc1 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	dc2 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
		Role:             "slave",
		MasterLinkStatus: "up",
		SlaveReplOffset:  100,
	}}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-iad.shared-lion.svc.cluster.local:6379": dc1,
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": dc2,
	})

	// Tick 1: initialise dragonfly status.
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("tick1: %v", err)
	}
	fg = fetchFG(t, r, fgNN(fg))
	// Tick 2: capture source offset.
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("tick2: %v", err)
	}
	fg = fetchFG(t, r, fgNN(fg))
	if fg.Status.PlannedFailover.Dragonfly == nil || fg.Status.PlannedFailover.Dragonfly.SourceOffsetAtDrain == nil {
		t.Fatalf("source offset not captured: %+v", fg.Status.PlannedFailover.Dragonfly)
	}
	if got := *fg.Status.PlannedFailover.Dragonfly.SourceOffsetAtDrain; got != 100 {
		t.Errorf("source offset = %d, want 100", got)
	}
	// Tick 3: poll target — caught up → advance to PromotingDragonfly.
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("tick3: %v", err)
	}
	fg = fetchFG(t, r, fgNN(fg))
	if fg.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		t.Errorf("phase = %q, want PromotingDragonfly", fg.Status.PlannedFailover.Phase)
	}
}

// TestPlannedFailoverWaitingForDragonflySync_StampsFreshStartTime
// regression-tests B2: the previous code computed the maxSyncWait
// budget against LagWaitStartTime, which is stamped on entry to
// WaitingForLag (not WaitingForDragonflySync). When the MySQL lag wait
// took longer than the Dragonfly budget the sync window was
// pre-expired on the first poll, silently breaking session
// preservation under proceed mode and causing spurious rollback under
// fail mode.
//
// We simulate the scenario by setting LagWaitStartTime far enough in
// the past that the budget would be exhausted, but with no Dragonfly
// status block yet. The first reconcile must stamp a fresh
// SyncWaitStartTime; subsequent reconciles must measure elapsed
// against that stamp, not against the borrowed lag-wait clock.
func TestPlannedFailoverWaitingForDragonflySync_StampsFreshStartTime(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	// Lag-wait was long: borrowing this clock would pre-expire the
	// 100ms maxSyncWait budget set by plannedFailoverFGWithDragonfly.
	longAgo := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	fg.Status.PlannedFailover.LagWaitStartTime = &longAgo
	fg.Status.PlannedFailover.StartTime = &longAgo
	r, _ := newReconciler(fg)

	// Tick 1: initialise the dragonfly status block. Must stamp
	// SyncWaitStartTime to ~now.
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("tick1: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Dragonfly == nil || got.Status.PlannedFailover.Dragonfly.SyncWaitStartTime == nil {
		t.Fatalf("SyncWaitStartTime not stamped: %+v", got.Status.PlannedFailover.Dragonfly)
	}
	stamped := got.Status.PlannedFailover.Dragonfly.SyncWaitStartTime.Time
	if age := time.Since(stamped); age > 5*time.Second {
		t.Errorf("SyncWaitStartTime is %s old; expected ~now (lag-wait stamp must NOT be borrowed)", age)
	}
}

func TestPlannedFailoverWaitingForDragonflySync_TimeoutProceedAdvances(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	// Force the lag-wait start far enough in the past that the budget
	// has elapsed regardless of how fast the test runs.
	past := metav1.NewTime(time.Now().Add(-5 * time.Second))
	fg.Status.PlannedFailover.LagWaitStartTime = &past
	fg.Status.PlannedFailover.StartTime = &past
	// Pre-populate the dragonfly sub-status so the handler skips the
	// initialisation/source-offset captures and reaches the budget check.
	srcOffset := int64(1000)
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{
		Enabled:             true,
		SourceOffsetAtDrain: &srcOffset,
	}
	r, _ := newReconciler(fg)

	// Target is behind: never catches up.
	dc2 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
		Role:             "slave",
		MasterLinkStatus: "up",
		SlaveReplOffset:  0,
	}}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-iad.shared-lion.svc.cluster.local:6379": {info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 1_000}},
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": dc2,
	})
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		t.Errorf("phase = %q, want PromotingDragonfly (timeout proceed)", got.Status.PlannedFailover.Phase)
	}
	if got.Status.PlannedFailover.Dragonfly == nil || got.Status.PlannedFailover.Dragonfly.SessionsPreserved == nil ||
		*got.Status.PlannedFailover.Dragonfly.SessionsPreserved {
		t.Errorf("expected sessionsPreserved=false on timeout proceed: %+v", got.Status.PlannedFailover.Dragonfly)
	}
	if got.Status.PlannedFailover.Dragonfly.Reason != ReasonDragonflySyncTimeout {
		t.Errorf("reason = %q, want %q", got.Status.PlannedFailover.Dragonfly.Reason, ReasonDragonflySyncTimeout)
	}
}

// TestPlannedFailoverWaitingForDragonflySync_OffsetCaptureFails_ProceedsWithSessionsLost
// regression-tests B6: the previous code stamped SourceOffsetAtDrain=0
// when the source dial or INFO replication failed at drain capture.
// CandidateSyncReady(target offset, persistence, sourceOffset=0)
// returned true unconditionally because target offset >= 0 always
// holds — so the state machine advanced to PromotingDragonfly with
// sessionsPreserved=true, falsely claiming session continuity.
//
// The fix flags OffsetCaptureFailed instead. The proceed-mode
// timeout handler sets sessionsPreserved=false and advances; the
// fail-mode handler rolls back. We verify the proceed branch here.
func TestPlannedFailoverWaitingForDragonflySync_OffsetCaptureFails_ProceedsWithSessionsLost(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	// Pre-populate the Dragonfly status block (skipping the init tick).
	now := metav1.Now()
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{
		Enabled:           true,
		SyncWaitStartTime: &now,
	}
	r, _ := newReconciler(fg)
	// No connector programmed for the source addr → dial fails.
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		return nil, errors.New("source unreachable: " + addr)
	}

	// Tick: capture step. Must mark OffsetCaptureFailed and not stamp a
	// misleading zero offset.
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("tick capture: %v", err)
	}
	fg = fetchFG(t, r, fgNN(fg))
	if !fg.Status.PlannedFailover.Dragonfly.OffsetCaptureFailed {
		t.Fatalf("OffsetCaptureFailed not set: %+v", fg.Status.PlannedFailover.Dragonfly)
	}
	if fg.Status.PlannedFailover.Dragonfly.SourceOffsetAtDrain != nil {
		t.Errorf("SourceOffsetAtDrain stamped despite capture-fail: %v", *fg.Status.PlannedFailover.Dragonfly.SourceOffsetAtDrain)
	}

	// Tick: with capture-failed sentinel set, we must skip the
	// sync-readiness gate entirely and route through the timeout
	// handler. proceed mode → sessionsPreserved=false, advance to
	// PromotingDragonfly.
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("tick post-capture: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		t.Errorf("phase = %q, want PromotingDragonfly (proceed after capture-fail)", got.Status.PlannedFailover.Phase)
	}
	if got.Status.PlannedFailover.Dragonfly.SessionsPreserved == nil || *got.Status.PlannedFailover.Dragonfly.SessionsPreserved {
		t.Errorf("expected sessionsPreserved=false on capture-fail proceed; got %+v", got.Status.PlannedFailover.Dragonfly)
	}
}

func TestPlannedFailoverWaitingForDragonflySync_TimeoutFailRollsBack(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("fail")
	past := metav1.NewTime(time.Now().Add(-5 * time.Second))
	fg.Status.PlannedFailover.LagWaitStartTime = &past
	fg.Status.PlannedFailover.StartTime = &past
	srcOffset := int64(1000)
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{
		Enabled:             true,
		SourceOffsetAtDrain: &srcOffset,
	}
	r, _ := newReconciler(fg)
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-iad.shared-lion.svc.cluster.local:6379": {info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 1_000}},
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": {info: dragonfly.ReplicationInfo{
			Role:             "slave",
			MasterLinkStatus: "up",
			SlaveReplOffset:  0,
		}},
	})
	if _, err := r.plannedFailoverWaitingForDragonflySync(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Errorf("phase = %q, want Failed (timeout fail)", got.Status.PlannedFailover.Phase)
	}
	if got.Status.PlannedFailover.Reason != ReasonDragonflySyncTimeout {
		t.Errorf("reason = %q, want %q", got.Status.PlannedFailover.Reason, ReasonDragonflySyncTimeout)
	}
}

func TestPlannedFailoverPromotingDragonfly_Success(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	// Seed both pods with the steady-state labels: source has
	// role=master+traffic=enabled, target has role=replica+traffic=enabled.
	// The handler's strip→takeover→restore sequence should rewrite them.
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
	r, c := newReconciler(fg, sourcePod, targetPod)

	target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 200}}
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": target,
		"lion-dragonfly-iad.shared-lion.svc.cluster.local:6379": source,
	})
	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Errorf("phase = %q, want Promoting", got.Status.PlannedFailover.Phase)
	}
	if got.Status.PlannedFailover.Dragonfly.PromotionMethod != "REPLTAKEOVER" {
		t.Errorf("promotionMethod = %q, want REPLTAKEOVER", got.Status.PlannedFailover.Dragonfly.PromotionMethod)
	}
	if got.Status.PlannedFailover.Dragonfly.SessionsPreserved == nil || !*got.Status.PlannedFailover.Dragonfly.SessionsPreserved {
		t.Errorf("expected sessionsPreserved=true: %+v", got.Status.PlannedFailover.Dragonfly)
	}

	// Pod labels should reflect the post-promotion topology: target is
	// the new master with traffic enabled; source is deleted so the
	// StatefulSet creates a fresh pod without kubelet CrashLoopBackOff.
	var gotTarget corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: targetPod.Name, Namespace: targetPod.Namespace}, &gotTarget); err != nil {
		t.Fatalf("get target pod: %v", err)
	}
	if gotTarget.Labels[labelDragonflyRole] != "master" {
		t.Errorf("target role = %q, want master", gotTarget.Labels[labelDragonflyRole])
	}
	if gotTarget.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("target traffic = %q, want enabled", gotTarget.Labels[labelDragonflyTraffic])
	}
	var gotSource corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: sourcePod.Name, Namespace: sourcePod.Namespace}, &gotSource); client.IgnoreNotFound(err) != nil {
		t.Fatalf("get source pod: %v", err)
	} else if err == nil {
		t.Errorf("source pod still exists after takeover; want deleted to reset CrashLoopBackOff")
	}

	// CLIENT KILL was issued against the old master.
	if len(source.clientKillTypes) == 0 || source.clientKillTypes[0] != "NORMAL" {
		t.Errorf("expected CLIENT KILL TYPE NORMAL on source, got %v", source.clientKillTypes)
	}
}

// TestPlannedFailoverPromotingDragonfly_StripFails_ProceedsWithFailureHandler
// regression-tests B14: the previous version of this test left the
// source pod absent and asserted only on phase advance — so the strip
// step succeeded vacuously (no pods to patch) and the failure handler
// ran because the takeover dial failed. The strip-fail control path
// (which must skip the takeover entirely and never call dragonflyDial)
// was not exercised.
//
// Inject a fake-client interceptor that errors on Update of the source
// pod when the strip step removes the traffic label, and assert:
//   - The takeover dial is NEVER invoked (strip-fail short-circuits the
//     promotion sequence).
//   - The proceed-mode failure handler advances phase to Promoting and
//     the failure reason is the strip error.
func TestPlannedFailoverPromotingDragonfly_StripFails_ProceedsWithFailureHandler(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)

	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, newTestSecret(), sourcePod, targetPod).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, underlying client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				// Fail the strip step: source pod being updated with the
				// dragonfly-traffic label absent (i.e. removed).
				if pod, ok := obj.(*corev1.Pod); ok && pod.Name == sourcePod.Name {
					if _, has := pod.Labels[labelDragonflyTraffic]; !has {
						return errors.New("simulated strip-patch failure")
					}
				}
				return underlying.Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &MysqlFailoverGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	dialAttempts := 0
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		dialAttempts++
		return nil, errors.New("must not dial: strip should have failed first (addr=" + addr + ")")
	}

	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}
	if dialAttempts != 0 {
		t.Errorf("dragonflyDial called %d times after strip-fail; want 0 (strip-fail must short-circuit the promotion)", dialAttempts)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Errorf("phase = %q, want Promoting (proceed)", got.Status.PlannedFailover.Phase)
	}
	if got.Status.PlannedFailover.Dragonfly.PromotionMethod != "" {
		t.Errorf("promotionMethod = %q, want empty (strip failed → effectiveDragonflyMasterSite must keep returning source)", got.Status.PlannedFailover.Dragonfly.PromotionMethod)
	}
	if got.Status.PlannedFailover.Dragonfly.Reason != ReasonDragonflyPromotionFailed {
		t.Errorf("reason = %q, want %q", got.Status.PlannedFailover.Dragonfly.Reason, ReasonDragonflyPromotionFailed)
	}
	if got.Status.PlannedFailover.Dragonfly.Message == "" || !contains(got.Status.PlannedFailover.Dragonfly.Message, "strip source traffic label") {
		t.Errorf("expected strip-fail wording in message; got %q", got.Status.PlannedFailover.Dragonfly.Message)
	}
}

// TestPlannedFailoverPromotingDragonfly_DemoteFails_DoesNotRestoreSourceTraffic
// regression-tests the bug where the success path restored the source's
// traffic label after a failed role-demote patch. The result was a pod
// still labelled dragonfly-role=master with traffic=enabled — selectable
// by the active Service alongside the newly-promoted target. Split-brain
// at the routing layer.
//
// We inject a fake-client interceptor that errors on Update of the
// source pod when the demote role label is being set, and assert the
// source's traffic label remains stripped (no restore).
func TestPlannedFailoverPromotingDragonfly_DemoteFails_DoesNotRestoreSourceTraffic(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)

	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, newTestSecret(), sourcePod, targetPod).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, underlying client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				// Fail the demote patch: source pod being updated with
				// dragonfly-role=replica. Let everything else through —
				// strip (delete traffic label) and target master-stamp
				// must still succeed.
				if pod, ok := obj.(*corev1.Pod); ok && pod.Name == sourcePod.Name {
					if pod.Labels[labelDragonflyRole] == "replica" {
						return errors.New("simulated demote-patch failure")
					}
				}
				return underlying.Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &MysqlFailoverGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 200}}
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": target,
		"lion-dragonfly-iad.shared-lion.svc.cluster.local:6379": source,
	})
	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Demote failed → source must NOT have its traffic label restored.
	// Otherwise it sits with role=master+traffic=enabled and joins the
	// active Service alongside the new master.
	var gotSource corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: sourcePod.Name, Namespace: sourcePod.Namespace}, &gotSource); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	if _, has := gotSource.Labels[labelDragonflyTraffic]; has {
		t.Errorf("source traffic label = %q, want absent (demote failed → must not restore traffic)", gotSource.Labels[labelDragonflyTraffic])
	}
}

func TestPlannedFailoverPromotingDragonfly_TakeoverFails_RestoresSourceTraffic(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
	r, c := newReconciler(fg, sourcePod, targetPod)

	target := &fakeDragonflyConn{
		info:            dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: errors.New("REPLTAKEOVER refused"),
	}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": target,
	})
	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Source's traffic label should be restored after takeover failure
	// so the active Service still routes to it under the proceed
	// policy. Without this restore the Service would have no endpoint.
	var gotSource corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: sourcePod.Name, Namespace: sourcePod.Namespace}, &gotSource); err != nil {
		t.Fatalf("get source pod: %v", err)
	}
	if gotSource.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("source traffic = %q, want enabled (restored after takeover failure)", gotSource.Labels[labelDragonflyTraffic])
	}
}

func TestPlannedFailoverPromotingDragonfly_FailureProceeds(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	r, _ := newReconciler(fg)
	target := &fakeDragonflyConn{
		info:            dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: errors.New("REPLTAKEOVER refused"),
	}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": target,
	})
	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Errorf("phase = %q, want Promoting (best-effort proceed)", got.Status.PlannedFailover.Phase)
	}
	if got.Status.PlannedFailover.Dragonfly.SessionsPreserved == nil || *got.Status.PlannedFailover.Dragonfly.SessionsPreserved {
		t.Errorf("expected sessionsPreserved=false on REPLTAKEOVER fail: %+v", got.Status.PlannedFailover.Dragonfly)
	}
	if got.Status.PlannedFailover.Dragonfly.Reason != ReasonDragonflyPromotionFailed {
		t.Errorf("reason = %q, want %q", got.Status.PlannedFailover.Dragonfly.Reason, ReasonDragonflyPromotionFailed)
	}
}

// takeoverHookConn embeds fakeDragonflyConn and fires onTakeover at
// the moment ReplTakeover would commit on the server side. Used by
// regression tests that need to flip a "real Dragonfly master"
// pointer in lockstep with the protocol-level takeover.
type takeoverHookConn struct {
	*fakeDragonflyConn
	onTakeover func()
}

func (c *takeoverHookConn) ReplTakeover(ctx context.Context, t time.Duration) error {
	if c.onTakeover != nil {
		c.onTakeover()
	}
	return c.fakeDragonflyConn.ReplTakeover(ctx, t)
}

// TestPlannedFailoverPromotingDragonfly_NoReadOnlyMidFlight is the
// regression guard for the safety invariant called out in
// PLANS-Bloodraven-Dragonfly.md "Required Before Next Slice": during
// the strip → REPLTAKEOVER → re-label sequence, the active Service
// selector (role=master AND traffic=enabled) must never match a
// Dragonfly pod that is not the real Dragonfly master at that
// instant.
//
// "READONLY mid-flight" is the failure mode in which an application
// write reaches a Dragonfly pod that is currently a replica — either
// because REPLTAKEOVER hasn't happened yet (so the pod the selector
// points to is still a replica) or because the post-takeover
// demotion of the old master raced ahead of the active-Service
// label flip (so two pods match while one is now a replica).
//
// The test intercepts every Pod Update against the fake client and
// re-evaluates the selector after each label patch. The "real
// Dragonfly master" pointer flips inside ReplTakeover, mirroring
// server-side semantics. Any selector match against a pod whose
// site is not the current real master is recorded as an invariant
// violation.
//
// This is a unit-level proxy for the deferred k3d e2e test that runs
// continuous writes against the active Service through a real
// failover; the unit-level coverage exercises the label/state
// transitions but not the live kube-proxy/endpoint controller. The
// remaining live-cluster scenario lives in
// PLANS-Dragonfly-Chaos-Scenarios.md (D3).
func TestPlannedFailoverPromotingDragonfly_NoReadOnlyMidFlight(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)

	// realMaster tracks which site is the real Dragonfly master at any
	// given instant. It flips inside the target's ReplTakeover hook.
	// violations is appended to whenever the selector matches a pod
	// that isn't the real master.
	var (
		invMu      sync.Mutex
		realMaster = "iad"
		violations []string
		checks     int
	)

	checkActiveServiceInvariant := func(c client.Reader, label string) {
		var pods corev1.PodList
		if err := c.List(context.Background(), &pods,
			client.InNamespace(sourcePod.Namespace),
			client.MatchingLabels{
				labelAppName:          dragonflyAppName,
				labelInstance:         fg.Name,
				labelDragonflyRole:    "master",
				labelDragonflyTraffic: dragonflyTrafficEnabled,
			}); err != nil {
			return
		}
		invMu.Lock()
		defer invMu.Unlock()
		checks++
		for _, p := range pods.Items {
			site := p.Labels[labelSite]
			if site != realMaster {
				violations = append(violations, fmt.Sprintf(
					"after %s: pod %q (site=%s) matches active-Service selector but real Dragonfly master is %q (READONLY mid-flight)",
					label, p.Name, site, realMaster))
			}
		}
	}

	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, newTestSecret(), sourcePod, targetPod).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, underlying client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if err := underlying.Update(ctx, obj, opts...); err != nil {
					return err
				}
				if pod, ok := obj.(*corev1.Pod); ok {
					checkActiveServiceInvariant(underlying, "update "+pod.Name)
				}
				return nil
			},
		}).
		Build()
	r := &MysqlFailoverGroupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
	}

	target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 200}}
	targetWithHook := &takeoverHookConn{
		fakeDragonflyConn: target,
		onTakeover: func() {
			invMu.Lock()
			realMaster = "pdx"
			invMu.Unlock()
		},
	}
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		switch addr {
		case "lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379":
			return targetWithHook, nil
		case "lion-dragonfly-iad.shared-lion.svc.cluster.local:6379":
			return source, nil
		}
		return nil, errors.New("no programmed conn for " + addr)
	}

	// Initial state must already hold the invariant: the source pod
	// (iad) matches the selector and is the real master.
	checkActiveServiceInvariant(c, "initial")

	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("plannedFailoverPromotingDragonfly: %v", err)
	}

	// Final state: only target should match the selector, and it is
	// the real master. Intermediate states are covered by the Update
	// interceptor.
	checkActiveServiceInvariant(c, "final")

	invMu.Lock()
	defer invMu.Unlock()
	for _, v := range violations {
		t.Errorf("invariant violation: %s", v)
	}
	// Sanity: at least the initial, the strip-update, the target role
	// flip, the source role flip, the source traffic restore, and the
	// final check should each have been observed. Five mutating Pod
	// Updates plus initial + final = 7. Use a relaxed lower bound to
	// stay tolerant to idempotent-skip optimisations in the label
	// helpers, but assert the interceptor actually fired.
	if checks < 4 {
		t.Errorf("invariant checker only fired %d times; expected several Pod-Update interceptions plus initial+final", checks)
	}
}

func TestPlannedFailoverPromotingDragonfly_FailureFailRollsBack(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("fail")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	r, _ := newReconciler(fg)
	target := &fakeDragonflyConn{
		info:            dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: errors.New("REPLTAKEOVER refused"),
	}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": target,
	})
	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatalf("err: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if got.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Errorf("phase = %q, want Failed (fail policy)", got.Status.PlannedFailover.Phase)
	}
	if got.Status.PlannedFailover.Reason != ReasonDragonflyPromotionFailed {
		t.Errorf("reason = %q, want %q", got.Status.PlannedFailover.Reason, ReasonDragonflyPromotionFailed)
	}
}
