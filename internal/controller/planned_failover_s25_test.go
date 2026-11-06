package controller

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// assertDragonflyStatusConsistent fails when status.dragonfly reads Ready
// while the site it lists as the master is not status.dragonfly.activeSite
// (DragonflyPhaseReady: exactly one master serving, the active site's).
func assertDragonflyStatusConsistent(t *testing.T, when string, df *v1alpha1.DragonflyStatus) {
	t.Helper()
	if df == nil || df.Phase != v1alpha1.DragonflyPhaseReady {
		return
	}
	var masters []string
	for _, s := range df.Sites {
		if s.Role == v1alpha1.DragonflyRoleMaster {
			masters = append(masters, s.Name)
		}
	}
	if len(masters) != 1 || masters[0] != df.ActiveSite {
		t.Fatalf("%s: status.dragonfly reads Ready with master(s) %v but activeSite %q (message %q)",
			when, masters, df.ActiveSite, df.Message)
	}
}

// Playground scenario 25 (operator restart mid-Dragonfly-failover), end to
// end in one process: the target Dragonfly was held offline, the operator
// restarted in WaitingForDragonflySync, and the target came back as an
// empty standalone master. The manager is paused while the run owns
// Dragonfly, so nothing wires it as a replica and the sync wait times out;
// onSyncTimeout=Proceed then finds the target reporting role:master that
// this process never promoted, and refuses it (the source keeps the
// cache). MySQL moves; Dragonfly follows afterwards
// through the manager: the empty target is re-attached as a replica, syncs,
// and is promoted. At no point may status.dragonfly read Ready with the
// master on a site other than its activeSite: the promotion stamp moves
// activeSite before the next observation refreshes the sites, and the
// scenario failed on exactly that read ("dragonfly master is pdx but
// expected target iad").
func TestS25_RestartMidSyncThenFollowUpKeepsDragonflyStatusConsistent(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("Proceed")
	pf := fg.Status.PlannedFailover
	// Before the restart: the source offset was captured, and the sync wait
	// has been running past maxSyncWait.
	started := metav1.NewTime(time.Now().Add(-time.Minute))
	off := int64(100)
	pf.Dragonfly.SyncWaitStartTime = &started
	pf.Dragonfly.SourceOffsetState, pf.Dragonfly.SourceOffset = v1alpha1.DragonflyOffsetCaptured, &off
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)

	// A fresh reconciler is the restarted process: an empty memo, its claim
	// hydrated (run-1 held).
	r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 500}}
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}}
	byAddr := map[string]DragonflyConnection{pfDFIad: source, pfDFPdx: target}
	installFakeDragonflyConn(t, r, byAddr)
	nn := fgNN(fg)

	// The sync wait times out (the target is no replica) and Proceed records
	// the takeover intent.
	pfDFPass(t, r, nn)
	pf = fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly || pf.Dragonfly.Reason != v1alpha1.DragonflyPlannedReasonSyncTimeout {
		t.Fatalf("after the sync wait: %s %+v, want PromotingDragonfly{SyncTimeout}", pf.Phase, pf.Dragonfly)
	}
	// The takeover refuses the empty standalone master: no REPLTAKEOVER, the
	// source is neither demoted nor deleted, and MySQL goes on (Proceed).
	pfDFTakeoverPasses(t, r, nn)
	pf = fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed {
		t.Fatalf("after the takeover: %s %+v, want Promoting with the takeover Failed", pf.Phase, pf.Dragonfly.Takeover)
	}
	if target.takeovers != 0 || pfDFPodGone(t, c, sourcePod) {
		t.Fatalf("empty target taken over (%d REPLTAKEOVER) or source deleted", target.takeovers)
	}

	// The run finishes: MySQL is on the target, the run is terminal, and the
	// Dragonfly master is still the source.
	cur := fetchFG(t, r, nn)
	cur.Status.ActiveSite = "pdx"
	cur.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhaseSucceeded
	cur.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "iad"}
	if err := c.Status().Update(context.Background(), cur); err != nil {
		t.Fatalf("finish the run: %v", err)
	}

	mgr := NewDragonflyManager(c, record.NewFakeRecorder(64), slog.Default(), nn, 50*time.Millisecond)
	shrinkEmergencyWaits(mgr)
	mgr.SetConnector(func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		if conn, ok := byAddr[addr]; ok {
			return conn, nil
		}
		return nil, errors.New("no programmed conn for " + addr)
	})
	status := func() *v1alpha1.DragonflyStatus { return fetchFG(t, r, nn).Status.Dragonfly }

	// Tick 1: the empty target is a stale master that never took a write;
	// it is re-attached to the source.
	mgr.Tick(context.Background())
	assertDragonflyStatusConsistent(t, "tick 1", status())
	if got := target.replicaOfArgs; len(got) != 2 || got[0] != "lion-dragonfly-iad.shared-lion.svc.cluster.local" {
		t.Fatalf("target REPLICAOF = %v, want the source", got)
	}

	// Tick 2: the target is a synced replica; Dragonfly follows MySQL to it.
	target.info = dragonfly.ReplicationInfo{Role: "slave", MasterHost: "lion-dragonfly-iad.shared-lion.svc.cluster.local",
		MasterPort: 6379, MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0, SlaveReplOffset: 500}
	mgr.Tick(context.Background())
	if target.takeovers != 1 {
		t.Fatalf("follow-up REPLTAKEOVER sent %d times, want 1", target.takeovers)
	}
	df := status()
	if df.ActiveSite != "pdx" {
		t.Fatalf("status.dragonfly.activeSite = %q after the follow-up, want pdx", df.ActiveSite)
	}
	assertDragonflyStatusConsistent(t, "after the follow-up promotion", df)

	// Tick 3: the old source rejoined as a replica of the target: Ready,
	// with the single master on the target.
	source.info = dragonfly.ReplicationInfo{Role: "slave", MasterHost: "lion-dragonfly-pdx.shared-lion.svc.cluster.local",
		MasterPort: 6379, MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0, SlaveReplOffset: 500}
	mgr.Tick(context.Background())
	df = status()
	if df.Phase != v1alpha1.DragonflyPhaseReady || df.ActiveSite != "pdx" {
		t.Fatalf("converged status = %s on %q (%q), want Ready on pdx", df.Phase, df.ActiveSite, df.Message)
	}
	assertDragonflyStatusConsistent(t, "converged", df)
}
