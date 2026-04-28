package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
		Image:     "docker.dragonflydb.io/dragonflydb/dragonfly:v1.25.5",
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
	r, _ := newReconciler(fg)

	target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 200}}
	installFakeDragonflyConn(t, r, map[string]*fakeDragonflyConn{
		"lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379": target,
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
