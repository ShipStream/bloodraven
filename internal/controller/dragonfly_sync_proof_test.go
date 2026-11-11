package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	syncProofDC1 = "lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379"
	syncProofDC2 = "lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379"
)

// syncProofReplica is a replica whose own INFO answers pass every link
// check, with the replication position given: the state of a replica cut
// off from its master without a reset (captured from v1.38 and v2.1).
func syncProofReplica(replID string, flows ...int64) *fakeDragonflyConn {
	return &fakeDragonflyConn{
		info:     dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0, MasterReplID: replID},
		position: &dragonfly.Position{Role: "slave", Flows: flows, ReplID: replID},
	}
}

func syncProofMaster(replID string, flows ...int64) *fakeDragonflyConn {
	return &fakeDragonflyConn{
		info:     dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1, MasterReplID: replID},
		position: &dragonfly.Position{Role: "master", Flows: flows, ReplID: replID},
	}
}

// dragonflyTargetReached is the check the planned failover and the rollout
// both rest on: the replica's link answers and its position against the
// source's, per flow. A position that cannot be read, or compared, is not
// ready.
func TestDragonflyTargetReached(t *testing.T) {
	source := dragonfly.Position{Role: "master", Flows: []int64{24, 28}, ReplID: "a"}
	cases := []struct {
		name       string
		target     *fakeDragonflyConn
		wantReady  bool
		wantDetail string
		wantErr    string
	}{
		{name: "caught up on every flow", target: syncProofReplica("a", 24, 28), wantReady: true},
		{name: "healthy link, both flows behind", target: syncProofReplica("a", 1, 1), wantDetail: "target flow 0 at 1 behind source 24"},
		{name: "sum reached, one flow behind", target: syncProofReplica("a", 60, 1), wantDetail: "target flow 1 at 1 behind source 28"},
		{name: "follows an earlier incarnation of the source", target: syncProofReplica("b", 24, 28), wantDetail: `replication id "b"`},
		{name: "position unreadable", target: &fakeDragonflyConn{
			info:        dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up"},
			positionErr: errors.New("ERR unknown subcommand")}, wantErr: "target replication position"},
		{name: "link down", target: func() *fakeDragonflyConn {
			c := syncProofReplica("a", 24, 28)
			c.info.MasterLinkStatus = "down"
			return c
		}(), wantDetail: "target not in stable sync"},
		{name: "full sync in progress", target: func() *fakeDragonflyConn {
			c := syncProofReplica("a", 24, 28)
			c.info.MasterSyncInProgress = true
			return c
		}(), wantDetail: "target not in stable sync"},
		{name: "loading", target: func() *fakeDragonflyConn {
			c := syncProofReplica("a", 24, 28)
			c.persist.Loading = true
			return c
		}(), wantDetail: "target not in stable sync"},
		{name: "target is a master", target: syncProofMaster("a", 24, 28), wantDetail: "target not in stable sync"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithDragonfly()
			r, _ := newReconciler(fg)
			installFakeDragonflyConn(t, r, map[string]DragonflyConnection{syncProofDC2: tc.target})
			ready, detail, err := r.dragonflyTargetReached(context.Background(), fg, "dc2", source)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || ready {
					t.Fatalf("ready=%v detail=%q err=%v, want an error containing %q", ready, detail, err, tc.wantErr)
				}
				return
			}
			if err != nil || ready != tc.wantReady || !strings.Contains(detail, tc.wantDetail) {
				t.Fatalf("ready=%v detail=%q err=%v, want ready=%v detail containing %q", ready, detail, err, tc.wantReady, tc.wantDetail)
			}
		})
	}
}

// A source that answers but gives no position, or is not a master, has no
// position to capture. Zero would be "nothing to catch up to".
func TestDragonflySourcePosition(t *testing.T) {
	cases := []struct {
		name    string
		source  *fakeDragonflyConn
		wantErr string
		want    int64
	}{
		{name: "master", source: syncProofMaster("a", 24, 28), want: 52},
		{name: "position unreadable", source: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"},
			positionErr: errors.New("ERR unknown subcommand")}, wantErr: "source replication position"},
		{name: "source is a replica", source: syncProofReplica("a", 24, 28), wantErr: `source reports role "slave", not master`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithDragonfly()
			r, _ := newReconciler(fg)
			installFakeDragonflyConn(t, r, map[string]DragonflyConnection{syncProofDC1: tc.source})
			pos, err := r.dragonflySourcePosition(context.Background(), fg, "dc1")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("pos=%+v err=%v, want an error containing %q", pos, err, tc.wantErr)
				}
				return
			}
			if err != nil || pos.Total() != tc.want {
				t.Fatalf("pos=%+v err=%v, want total %d", pos, err, tc.want)
			}
		})
	}
}

// The planned failover's sync wait refuses a target whose link looks
// healthy and whose flows are behind the source, and records the source's
// position as the sum over its shards. Before, the source's
// master_repl_offset was compared, which a Dragonfly master does not
// report: it read as 0 and every target passed.
func TestPlannedFailoverDragonflySync_HealthyLinkBehindSourceDoesNotPass(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("Fail")
	fg.Status.PlannedFailover.Settings.Dragonfly.MaxSyncWait = metav1.Duration{Duration: time.Hour}
	r, _, _ := pfDFReconciler(t, fg)
	source, target := syncProofMaster("a", 24, 28), syncProofReplica("a", 1, 1)
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFIad: source, pfDFPdx: target})
	nn := fgNN(fg)

	pfDFPass(t, r, nn)
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Dragonfly.SourceOffsetState != v1alpha1.DragonflyOffsetCaptured || pf.Dragonfly.SourceOffset == nil || *pf.Dragonfly.SourceOffset != 52 {
		t.Fatalf("offset capture = %+v, want Captured with the sum 52", pf.Dragonfly)
	}
	// The source writes on. The goal stays the position read when the wait
	// began: one read again on a later poll would keep moving away on a
	// source that takes writes.
	source.position.Flows = []int64{90, 90}
	for i := 0; i < 3; i++ {
		pfDFPass(t, r, nn)
		if pf = fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync {
			t.Fatalf("phase = %s with the target behind on both flows, want the sync wait to continue", pf.Phase)
		}
	}
	// The target catches up to the captured position: the wait ends.
	target.position.Flows = []int64{24, 28}
	pfDFPass(t, r, nn)
	if pf = fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly ||
		pf.Dragonfly.Sessions == v1alpha1.DragonflySessionsLost {
		t.Fatalf("phase = %s dragonfly = %+v, want PromotingDragonfly once the target reached the source", pf.Phase, pf.Dragonfly)
	}
}

// A source that gives no position is recorded Unavailable, not Captured at
// zero, and the run applies onSyncTimeout.
func TestPlannedFailoverDragonflySync_SourceWithoutPositionIsUnavailable(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("Fail")
	r, _, _ := pfDFReconciler(t, fg)
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, positionErr: errors.New("ERR unknown subcommand")}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFIad: source, pfDFPdx: syncProofReplica("a", 24, 28)})
	nn := fgNN(fg)

	pfDFPass(t, r, nn)
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Dragonfly.SourceOffsetState != v1alpha1.DragonflyOffsetUnavailable || pf.Dragonfly.SourceOffset != nil {
		t.Fatalf("dragonfly = %+v, want sourceOffsetState Unavailable and no offset", pf.Dragonfly)
	}
	pfDFPass(t, r, nn)
	if pf = fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack {
		t.Fatalf("phase = %s, want RollingBack under onSyncTimeout=Fail", pf.Phase)
	}
}

// The per-flow position lives in the process that read it. A process that
// takes the run over mid-wait (an operator restart) reads the source
// again, so the target has to reach the later position; it does not pass
// on the sum the record holds.
func TestPlannedFailoverDragonflySync_RestartReadsTheSourceAgain(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("Fail")
	fg.Status.PlannedFailover.Settings.Dragonfly.MaxSyncWait = metav1.Duration{Duration: time.Hour}
	fg.Status.PlannedFailover.Dragonfly.SourceOffsetState = v1alpha1.DragonflyOffsetCaptured
	recorded := int64(52)
	fg.Status.PlannedFailover.Dragonfly.SourceOffset = &recorded
	r, _, _ := pfDFReconciler(t, fg)
	// The source moved on to 40/40; the target is at 30/30, past the
	// recorded sum of 52 and behind the source.
	source := syncProofMaster("a", 40, 40)
	target := syncProofReplica("a", 30, 30)
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFIad: source, pfDFPdx: target})
	nn := fgNN(fg)

	pfDFPass(t, r, nn)
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync {
		t.Fatalf("phase = %s, want the wait to continue: the target is behind the source read after the restart", pf.Phase)
	}
	// The position is read once per process: the source moving on does not
	// move the goal again.
	source.position.Flows = []int64{90, 90}
	target.position.Flows = []int64{40, 40}
	pfDFPass(t, r, nn)
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		t.Fatalf("phase = %s, want PromotingDragonfly: the target reached the position read after the restart", pf.Phase)
	}
}

// After a restart, a source that cannot be read leaves the run without a
// position: it waits, and at maxSyncWait applies onSyncTimeout.
func TestPlannedFailoverDragonflySync_RestartWithUnreadableSourceTimesOut(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("Fail")
	fg.Status.PlannedFailover.Settings.Dragonfly.MaxSyncWait = metav1.Duration{Duration: 30 * time.Second}
	fg.Status.PlannedFailover.Dragonfly.SourceOffsetState = v1alpha1.DragonflyOffsetCaptured
	recorded := int64(0)
	fg.Status.PlannedFailover.Dragonfly.SourceOffset = &recorded
	r, _, _ := pfDFReconciler(t, fg)
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: syncProofReplica("a", 24, 28)})
	nn := fgNN(fg)

	pfDFPass(t, r, nn)
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync {
		t.Fatalf("phase = %s, want the wait to continue while the source cannot be read", pf.Phase)
	}
	r.plannedClock.(*clock.FakeClock).Advance(time.Minute)
	pfDFPass(t, r, nn)
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack {
		t.Fatalf("phase = %s, want RollingBack after maxSyncWait", pf.Phase)
	}
}

// syncProofCatchingUp is a replica that reaches flows on its nth position
// read and is at behind before.
type syncProofCatchingUp struct {
	*fakeDragonflyConn
	reads  int
	nth    int
	behind []int64
	flows  []int64
}

func (c *syncProofCatchingUp) ReplicationPosition(context.Context) (dragonfly.Position, error) {
	c.reads++
	flows := c.behind
	if c.reads >= c.nth {
		flows = c.flows
	}
	return dragonfly.Position{Role: "slave", Flows: flows, ReplID: c.info.MasterReplID}, nil
}

// The rollout check reads the source once and gives the target a short
// time to reach that position: a replica is always a round trip behind a
// source that takes writes. One that stays behind is not ready.
func TestDragonflyRolloutCandidateSyncReady(t *testing.T) {
	fg := fgWithDragonfly()
	ctx := context.Background()

	t.Run("reaches the source within the wait", func(t *testing.T) {
		r, _ := newReconciler(fg)
		target := &syncProofCatchingUp{fakeDragonflyConn: syncProofReplica("a"), nth: 3, behind: []int64{20, 28}, flows: []int64{24, 28}}
		installFakeDragonflyConn(t, r, map[string]DragonflyConnection{syncProofDC1: syncProofMaster("a", 24, 28), syncProofDC2: target})
		ready, err := r.dragonflyRolloutCandidateSyncReady(ctx, fg, "dc2", "dc1")
		if err != nil || !ready {
			t.Fatalf("ready=%v err=%v, want ready after the target's third read", ready, err)
		}
		if target.reads != 3 {
			t.Errorf("target position read %d times, want 3", target.reads)
		}
	})
	t.Run("stays behind", func(t *testing.T) {
		r, _ := newReconciler(fg)
		installFakeDragonflyConn(t, r, map[string]DragonflyConnection{syncProofDC1: syncProofMaster("a", 24, 28), syncProofDC2: syncProofReplica("a", 1, 1)})
		start := time.Now()
		ready, err := r.dragonflyRolloutCandidateSyncReady(ctx, fg, "dc2", "dc1")
		if err != nil || ready {
			t.Fatalf("ready=%v err=%v, want not ready for a target behind on both flows", ready, err)
		}
		if waited := time.Since(start); waited > dragonflyRolloutSyncWait+time.Second {
			t.Errorf("waited %s, want at most about %s", waited, dragonflyRolloutSyncWait)
		}
	})
	t.Run("source without a position", func(t *testing.T) {
		r, _ := newReconciler(fg)
		source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, positionErr: errors.New("ERR unknown subcommand")}
		installFakeDragonflyConn(t, r, map[string]DragonflyConnection{syncProofDC1: source, syncProofDC2: syncProofReplica("a", 24, 28)})
		if ready, err := r.dragonflyRolloutCandidateSyncReady(ctx, fg, "dc2", "dc1"); err == nil || ready {
			t.Fatalf("ready=%v err=%v, want an error: the source gave no position", ready, err)
		}
	})
}

// A rollout does not promote a replica that is behind the active master:
// the active site keeps its role and its old image until the replica has
// caught up.
func TestReconcileDragonflyResources_RolloutDoesNotPromoteReplicaBehindTheMaster(t *testing.T) {
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", Phase: v1alpha1.DragonflyPhaseReady}
	r, c := newReconciler(fg)
	target := syncProofReplica("a", 1, 1)
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{syncProofDC1: syncProofMaster("a", 24, 28), syncProofDC2: target})
	ctx := context.Background()
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	for _, site := range []string{"dc1", "dc2"} {
		if err := c.Create(ctx, dragonflyPodForTest(fg, site, fg.Spec.Dragonfly.Image, true)); err != nil {
			t.Fatalf("create %s pod: %v", site, err)
		}
	}
	oldImage := fg.Spec.Dragonfly.Image
	fg.Spec.Dragonfly.Image = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("replica rollout reconcile: %v", err)
	}
	markDragonflyStatefulSetRolledOutForTest(t, c, "dc2")

	if requeue, err := r.reconcileDragonflyResources(ctx, fg); err != nil || requeue == 0 {
		t.Fatalf("reconcile with the replica behind: requeue=%v err=%v, want a requeue", requeue, err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly.ActiveSite != "dc1" {
		t.Fatalf("dragonfly activeSite = %q, want dc1: a replica behind the master must not be promoted", got.Status.Dragonfly.ActiveSite)
	}
	if img := dragonflyStatefulSetImageForTest(t, c, "dc1"); img != oldImage {
		t.Fatalf("active image = %q, want %q until the replica is promoted", img, oldImage)
	}

	target.position.Flows = []int64{24, 28}
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("reconcile with the replica caught up: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly.ActiveSite != "dc2" {
		t.Fatalf("dragonfly activeSite = %q, want dc2 once the replica reached the master", got.Status.Dragonfly.ActiveSite)
	}
}
