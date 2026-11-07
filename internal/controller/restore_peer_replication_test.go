package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// Tests for #212: a full-instance in-place restore stops the peers'
// replication receivers before its Job can drop anything, and nothing in
// the operator restarts them while they await their post-restore reclone.

const peerUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func peerGTID(r string) string { return peerUUID + ":" + r }

// replicatingPeer is a peer whose receiver and applier run against iad.
func replicatingPeer(retrieved string) *mysql.ReplicaStatus {
	return &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-iad",
		RetrievedGtidSet: retrieved, ExecutedGtidSet: retrieved,
	}
}

// peerFencingRestore is fencingRestore with the peer replicating and the
// primary at gtid 1-10.
func peerFencingRestore(t *testing.T, retrieved string) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *mockMySQL, *mockMySQL, *TopologyManager) {
	t.Helper()
	fg := fgInPlaceRestore("")
	site, peer := fg.Spec.Sites[0].Name, fg.Spec.Sites[1].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceFencing, TargetSite: site, Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}, StartTime: ptrTime(time.Now()),
	}
	no := false
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{
		Site: site, ConfirmToken: fg.Spec.RestoreInPlace.Confirm, PriorOfflineMode: &no,
	}
	r, c := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret(), primaryFencedPod(fg.Name, site))
	tm, mocks := withFenceRunner(r, c, fg)
	r.Recorder = record.NewFakeRecorder(100)
	mocks[site].gtidExecuted = peerGTID("1-10")
	mocks[peer].replicaStatusVal = replicatingPeer(retrieved)
	return r, c, fg, mocks[site], mocks[peer], tm
}

func restorePass(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
}

// backdatePeerStop makes the peer-stop step look as if it started d ago.
func backdatePeerStop(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
	r.restorePeerStopElapsed(fg)
	nn := client.ObjectKeyFromObject(fg)
	r.restorePeerStopMu.Lock()
	w := r.restorePeerStopStart[nn]
	w.since = time.Now().Add(-d)
	r.restorePeerStopStart[nn] = w
	r.restorePeerStopMu.Unlock()
}

func TestInPlaceFencing_StopsPeerReceiversBeforeRestoring(t *testing.T) {
	r, c, fg, _, peer, tm := peerFencingRestore(t, peerGTID("1-10"))
	peerName := fg.Spec.Sites[1].Name

	restorePass(t, r, c, fg)
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || fg.Status.RestoreInPlace.JobName != "" {
		t.Fatalf("status = %+v, want Restoring before the Job", fg.Status.RestoreInPlace)
	}
	q := fg.Status.RestoreReclones
	if q == nil || q.Phase != v1alpha1.RestoreReclonesArmed || !slices.Equal(q.Pending, []string{peerName}) || q.FencedGtidExecuted != peerGTID("1-10") {
		t.Fatalf("queue = %+v, want Armed with the peer and the fenced gtid_executed", q)
	}
	if peer.stopIOCalls != 1 || peer.replicaStatusVal.IORunning || !peer.replicaStatusVal.SQLRunning {
		t.Fatalf("stops=%d status=%+v, want the receiver stopped and the applier still running", peer.stopIOCalls, peer.replicaStatusVal)
	}
	if !tm.restorePeerHeldSite(peerName) {
		t.Fatal("peer not held after its receiver was stopped")
	}
	if events := strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"); !strings.Contains(events, "RestorePeerReplicationStopped") ||
		strings.Contains(events, "RestorePeerReplicationStoppedBehind") {
		t.Fatalf("events %q, want RestorePeerReplicationStopped only", events)
	}
}

func TestInPlaceFencing_PerSchemaRestoreLeavesPeersReplicating(t *testing.T) {
	fg := fgInPlaceRestore("tenant")
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceFencing, TargetSite: site, Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeSchema, Schema: "tenant"}, StartTime: ptrTime(time.Now()),
	}
	r, c := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret())
	_, mocks := withFenceRunner(r, c, fg)
	peer := mocks[fg.Spec.Sites[1].Name]
	peer.replicaStatusVal = replicatingPeer(peerGTID("1-10"))
	restorePass(t, r, c, fg)
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || peer.stopIOCalls != 0 {
		t.Fatalf("phase %s stops %d, want Restoring with the peer's receiver untouched", fg.Status.RestoreInPlace.Phase, peer.stopIOCalls)
	}
}

func TestInPlaceFencing_LetsRunningReceiverCatchUp(t *testing.T) {
	r, c, fg, _, peer, _ := peerFencingRestore(t, peerGTID("1-5"))

	restorePass(t, r, c, fg)
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing || peer.stopIOCalls != 0 ||
		!strings.Contains(fg.Status.RestoreInPlace.Message, "receiving the primary's last transactions") {
		t.Fatalf("status %+v stops %d, want Fencing while the receiver catches up", fg.Status.RestoreInPlace, peer.stopIOCalls)
	}

	peer.mu.Lock()
	peer.replicaStatusVal.RetrievedGtidSet = peerGTID("1-10")
	peer.mu.Unlock()
	restorePass(t, r, c, fg)
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || peer.stopIOCalls != 1 {
		t.Fatalf("phase %s stops %d, want Restoring once caught up", fg.Status.RestoreInPlace.Phase, peer.stopIOCalls)
	}
}

func TestInPlaceFencing_CatchUpTimeoutStopsAndWarns(t *testing.T) {
	r, c, fg, _, peer, _ := peerFencingRestore(t, peerGTID("1-5"))
	backdatePeerStop(r, fg, restorePeerCatchUpTimeout+time.Second)

	restorePass(t, r, c, fg)
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || peer.stopIOCalls != 1 {
		t.Fatalf("phase %s stops %d, want Restoring with the receiver stopped after the catch-up bound", fg.Status.RestoreInPlace.Phase, peer.stopIOCalls)
	}
	events := strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n")
	if !strings.Contains(events, "RestorePeerReplicationStoppedBehind") || !strings.Contains(events, peerGTID("6-10")) {
		t.Fatalf("events %q, want an incomplete warning naming the missing transactions", events)
	}
}

// A legacy replicator is disconnected by offline_mode, so its receiver is
// Connecting and can never catch up: it is stopped at once.
func TestInPlaceFencing_ConnectingReceiverStoppedAtOnce(t *testing.T) {
	r, c, fg, _, peer, _ := peerFencingRestore(t, peerGTID("1-8"))
	peer.replicaStatusVal.IORunning, peer.replicaStatusVal.IOConnecting = false, true

	restorePass(t, r, c, fg)
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || peer.stopIOCalls != 1 || peer.replicaStatusVal.IOConnecting {
		t.Fatalf("phase %s stops %d status %+v, want the connecting receiver stopped without waiting", fg.Status.RestoreInPlace.Phase, peer.stopIOCalls, peer.replicaStatusVal)
	}
	if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestorePeerReplicationStoppedBehind") {
		t.Fatal("want RestorePeerReplicationStoppedBehind for the transactions it never received")
	}
}

func TestInPlaceFencing_ReceiverThatKeepsRunningIsNotTrusted(t *testing.T) {
	r, c, fg, _, peer, _ := peerFencingRestore(t, peerGTID("1-10"))
	peer.stopIOIgnored = true
	restorePass(t, r, c, fg)
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing || !strings.Contains(fg.Status.RestoreInPlace.Message, "still running") {
		t.Fatalf("status %+v, want Fencing while the receiver is not verified stopped", fg.Status.RestoreInPlace)
	}
}

// An unreachable peer cannot be verified stopped. Fencing waits, bounded:
// offline_mode is ON, so the run ends Failed without a Job instead of
// holding the application down.
func TestInPlaceFencing_UnreachablePeerEndsRunWithoutJob(t *testing.T) {
	r, c, fg, _, peer, _ := peerFencingRestore(t, peerGTID("1-10"))
	peer.replicaStatusErr = errors.New("connection refused")

	restorePass(t, r, c, fg)
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFencing || !strings.Contains(st.Message, "waiting to stop replication on peer") {
		t.Fatalf("status %+v, want Fencing waiting for the peer", st)
	}

	backdatePeerStop(r, fg, restorePeerStopTimeout+time.Second)
	restorePass(t, r, c, fg)
	st := fg.Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlaceFailed || st.JobName != "" || st.JobCreating || st.ConfirmTokenUsed != fg.Spec.RestoreInPlace.Confirm {
		t.Fatalf("status %+v, want Failed without a Job and the confirm consumed", st)
	}
	if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreInPlacePeerStopFailed") {
		t.Fatal("want RestoreInPlacePeerStopFailed")
	}
	// No Job ran, so the queue settles as unchanged and the peers resume.
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q != nil {
		t.Fatalf("queue = %+v, want discarded", q)
	}
}

func TestInPlaceFencing_PeerAddedAfterAcceptanceCancels(t *testing.T) {
	r, c, fg, _, _, _ := peerFencingRestore(t, peerGTID("1-10"))
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: fg.Spec.RestoreInPlace.Confirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{fg.Spec.Sites[1].Name},
	}
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
	extra := fg.Spec.Sites[1]
	extra.Name = "sea"
	fg.Spec.Sites = append(fg.Spec.Sites, extra)
	if err := c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	restorePass(t, r, c, fg)
	st := fg.Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "peer set changed") || st.ConfirmTokenUsed != "" {
		t.Fatalf("status %+v, want cancelled without consuming the confirm", st)
	}
}

// A run accepted by an operator that did not arm the queue at Preflight
// is armed in Fencing with the confirm it runs under.
func TestInPlaceFencing_ArmsMissingQueueWithRunConfirm(t *testing.T) {
	r, c, fg, _, _, _ := peerFencingRestore(t, peerGTID("1-10"))
	restorePass(t, r, c, fg)
	if q := fg.Status.RestoreReclones; q == nil || q.ConfirmToken != fg.Spec.RestoreInPlace.Confirm {
		t.Fatalf("queue = %+v, want armed with spec.confirm %s", q, fg.Spec.RestoreInPlace.Confirm)
	}
}

func TestReconcileInPlaceRestore_PreflightRejectsPeerWithoutStopPrivilege(t *testing.T) {
	fg := fgInPlaceRestore("")
	r, c := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret())
	_, mocks := withFenceRunner(r, c, fg)
	peer := fg.Spec.Sites[1].Name
	mocks[peer].missingIOStopPriv = []string{"REPLICATION_SLAVE_ADMIN"}

	restorePass(t, r, c, fg)
	st := fg.Status.RestoreInPlace
	if st == nil || st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "REPLICATION_SLAVE_ADMIN on "+peer) || st.ConfirmTokenUsed != "" {
		t.Fatalf("status %+v, want a non-consuming rejection naming the peer", st)
	}
	if !strings.HasPrefix(st.Message, restorePrivilegeRejectionPrefix) {
		t.Fatalf("message %q must keep the rate-limited privilege prefix", st.Message)
	}
}

func TestReconcileInPlaceRestore_PreflightWaitsForUnreachablePeerAndUpdates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(tm *TopologyManager, peer *mockMySQL)
		want  string
	}{
		{"unreachable peer", func(_ *TopologyManager, peer *mockMySQL) { peer.ioStopPrivErr = errors.New("connection refused") }, "to be reachable"},
		{"clone running", func(tm *TopologyManager, _ *mockMySQL) {
			tm.bootstrapPhase, tm.bootstrapRecipient = BootstrapPhaseCloning, "pdx"
		}, "clone of pdx"},
		{"clone admission in progress", func(tm *TopologyManager, _ *mockMySQL) { tm.bootstrapAdmitting = "pdx" }, "clone admission of pdx"},
		{"ordered update running", func(tm *TopologyManager, _ *mockMySQL) { tm.deploymentUpdateRunning = true }, "ordered update"},
		{"planned failover running", func(tm *TopologyManager, _ *mockMySQL) { holdPlannedClaimLockedForTest(tm, true) }, "planned failover"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("")
			r, c := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret())
			tm, mocks := withFenceRunner(r, c, fg)
			tc.setup(tm, mocks[fg.Spec.Sites[1].Name])
			restorePass(t, r, c, fg) // -> Preflight
			restorePass(t, r, c, fg)
			st := fg.Status.RestoreInPlace
			if st == nil || st.Phase != v1alpha1.RestoreInPlacePreflight || !strings.Contains(st.Message, tc.want) {
				t.Fatalf("status %+v, want Preflight waiting (%s)", st, tc.want)
			}
			if fg.Status.RestoreOfflineFence != nil {
				t.Fatal("fence recorded while Preflight waits")
			}
		})
	}
}

// --- topology manager: hold, gate, re-stop ---

func heldTopologyManager(t *testing.T) (*TopologyManager, *mockMySQL, *mockMySQL) {
	t.Helper()
	primary := &mockMySQL{readOnly: false, gtidExecuted: peerGTID("1-10")}
	peer := &mockMySQL{readOnly: true, gtidExecuted: peerGTID("1-10"),
		replicaStatusVal: &mysql.ReplicaStatus{SQLRunning: true, SourceHost: "mysql-dc1", RetrievedGtidSet: peerGTID("1-10"), ExecutedGtidSet: peerGTID("1-10")}}
	tm, _, _ := newTestTopologyManager(primary, peer)
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
	return tm, primary, peer
}

// Source convergence would restart a stopped receiver that points at the
// primary; a held site is left alone with reason RestorePeerHold.
func TestSourceConvergence_LeavesHeldPeerStopped(t *testing.T) {
	tm, _, peer := heldTopologyManager(t)
	tm.SetRestorePeerHold([]string{"dc2"}, nil, true)
	for i := 0; i < 3; i++ {
		tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, peer.replicaStatusVal})
	}
	if peer.startReplicaCalls != 0 {
		t.Fatalf("START REPLICA issued %d times on a held peer", peer.startReplicaCalls)
	}
	if tm.sites[1].sourceConvergenceReason != sourceReasonRestoreHold {
		t.Fatalf("reason = %q, want %s", tm.sites[1].sourceConvergenceReason, sourceReasonRestoreHold)
	}

	tm.SetRestorePeerHold(nil, nil, true)
	tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, peer.replicaStatusVal})
	if peer.startReplicaCalls != 1 {
		t.Fatalf("START REPLICA issued %d times after the hold was released, want 1", peer.startReplicaCalls)
	}
}

// blockingGTIDChecker blocks GetGtidExecuted until release is closed, to
// hold a convergence operation in flight.
type blockingGTIDChecker struct {
	*mockMySQL
	entered chan struct{}
	release chan struct{}
}

func (b *blockingGTIDChecker) GetGtidExecuted(ctx context.Context) (string, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return b.mockMySQL.GetGtidExecuted(ctx)
}

// A convergence restart admitted before the hold finishes before the
// restore can stop the receiver, and none is admitted after: a start can
// never land after the restore verified the receiver stopped.
func TestReplicaStartGate_StopWaitsForAdmittedStartAndBlocksLaterOnes(t *testing.T) {
	primary := &mockMySQL{readOnly: false, gtidExecuted: peerGTID("1-10")}
	inner := &mockMySQL{readOnly: true, gtidExecuted: peerGTID("1-10"),
		replicaStatusVal: &mysql.ReplicaStatus{SQLRunning: true, SourceHost: "mysql-dc1"}}
	peer := &blockingGTIDChecker{mockMySQL: inner, entered: make(chan struct{}, 1), release: make(chan struct{})}
	cfg := testTopologyConfig()
	tm := NewTopologyManager(cfg, []mysql.Checker{primary, peer}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), nil, &mockDNS{}, testLogger())
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly

	done := make(chan struct{})
	go func() {
		defer close(done)
		tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, inner.replicaStatusVal})
	}()
	<-peer.entered // the restart is admitted and mid-operation

	if _, err := tm.StopRestorePeerReplication(context.Background(), "dc2", peerGTID("1-10"), false); !errors.Is(err, errRestorePeerStartBusy) {
		t.Fatalf("stop while a start is in flight: err = %v, want errRestorePeerStartBusy", err)
	}
	close(peer.release)
	<-done
	if inner.startReplicaCalls != 1 {
		t.Fatalf("admitted start ran %d times, want 1", inner.startReplicaCalls)
	}

	out, err := tm.StopRestorePeerReplication(context.Background(), "dc2", peerGTID("1-10"), false)
	if err != nil || !out.Stopped {
		t.Fatalf("stop = %+v, %v", out, err)
	}
	inner.mu.Lock()
	io := inner.replicaStatusVal.IORunning
	inner.mu.Unlock()
	if io {
		t.Fatal("receiver running after a verified stop")
	}
	tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, inner.replicaStatusVal})
	if inner.startReplicaCalls != 1 {
		t.Fatalf("START REPLICA after the verified stop: %d calls", inner.startReplicaCalls)
	}
}

func TestRecovery_SkipsHeldPeerWithoutChannel(t *testing.T) {
	tm, _, peer := heldTopologyManager(t)
	peer.replicaStatusVal = nil // no channel: recovery would CHANGE SOURCE + START
	tm.SetRestorePeerHold([]string{"dc2"}, nil, true)
	tm.checkRecoveryWithConvergence(context.Background(), []*mysql.ReplicaStatus{nil, nil}, nil)
	if peer.startReplicaCalls != 0 || peer.changeSourceCalls != 0 {
		t.Fatalf("recovery touched a held peer: change=%d start=%d", peer.changeSourceCalls, peer.startReplicaCalls)
	}
	if err := tm.withReplicaStartGate("dc2", func() error { return nil }); !errors.Is(err, errRestorePeerHeld) {
		t.Fatalf("gate err = %v, want errRestorePeerHeld", err)
	}
}

func TestUpdater_DirectReplicaReadyStartsThroughGate(t *testing.T) {
	tm, _, peer := heldTopologyManager(t)
	u := NewUpdateController(NewFailoverController(testLogger()), testLogger())
	u.SetReplicaStartGate(tm.withReplicaStartGate)
	tm.SetRestorePeerHold([]string{"dc2"}, nil, true)
	target := UpdateTarget{Name: "dc2", Checker: peer}
	if directReplicaReady(context.Background(), peer, "mysql-dc1", func() error { return u.startReplica(context.Background(), target) }) {
		t.Fatal("held follower reported replicating")
	}
	if peer.startReplicaCalls != 0 {
		t.Fatalf("ordered update started a held follower's replication (%d calls)", peer.startReplicaCalls)
	}
	tm.SetRestorePeerHold(nil, nil, true)
	directReplicaReady(context.Background(), peer, "mysql-dc1", func() error { return u.startReplica(context.Background(), target) })
	if peer.startReplicaCalls != 1 {
		t.Fatalf("start calls = %d after release, want 1", peer.startReplicaCalls)
	}
}

func TestPoll_ReStopsHeldPeerReceiver(t *testing.T) {
	tm, _, peer := heldTopologyManager(t)
	peer.replicaStatusVal.IORunning = true // started by hand
	tm.SetRestorePeerHold([]string{"dc2"}, []string{"dc2"}, true)
	tm.reassertRestorePeerReplicationStop(context.Background())
	if peer.stopIOCalls != 1 || peer.replicaStatusVal.IORunning {
		t.Fatalf("stops=%d io=%v, want the receiver re-stopped", peer.stopIOCalls, peer.replicaStatusVal.IORunning)
	}
	// Held but not in the re-stop set (Draining): left alone.
	peer.replicaStatusVal.IORunning = true
	tm.SetRestorePeerHold([]string{"dc2"}, nil, true)
	tm.reassertRestorePeerReplicationStop(context.Background())
	if peer.stopIOCalls != 1 {
		t.Fatalf("a Draining site's receiver was re-stopped (%d)", peer.stopIOCalls)
	}
}

func TestRestorePeerHoldFromStatus(t *testing.T) {
	q := func(phase v1alpha1.RestoreReclonesPhase) *v1alpha1.RestoreReclonesStatus {
		return &v1alpha1.RestoreReclonesStatus{Phase: phase, Pending: []string{"pdx", "sea"}}
	}
	for _, tc := range []struct {
		name       string
		queue      *v1alpha1.RestoreReclonesStatus
		phase      v1alpha1.RestoreInPlacePhase
		wantRestop bool
	}{
		{"armed in fencing", q(v1alpha1.RestoreReclonesArmed), v1alpha1.RestoreInPlaceFencing, false},
		{"armed in restoring", q(v1alpha1.RestoreReclonesArmed), v1alpha1.RestoreInPlaceRestoring, true},
		{"armed after failure", q(v1alpha1.RestoreReclonesArmed), v1alpha1.RestoreInPlaceFailed, true},
		{"held", q(v1alpha1.RestoreReclonesHeld), v1alpha1.RestoreInPlaceFailed, true},
		{"draining", q(v1alpha1.RestoreReclonesDraining), v1alpha1.RestoreInPlaceSucceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgThreeSiteRestore()
			fg.Status.RestoreReclones = tc.queue
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: tc.phase}
			held, restop := restorePeerHoldFromStatus(fg)
			if !slices.Equal(held, []string{"pdx", "sea"}) || (len(restop) > 0) != tc.wantRestop {
				t.Fatalf("held=%v restop=%v, want held both and restop=%v", held, restop, tc.wantRestop)
			}
		})
	}
	if held, restop := restorePeerHoldFromStatus(fgThreeSiteRestore()); held != nil || restop != nil {
		t.Fatalf("no queue: held=%v restop=%v", held, restop)
	}
}

// A stale cached view cannot drop a held site; only a view confirmed
// through the API server can.
func TestRunner_StaleViewDoesNotReleasePeerHold(t *testing.T) {
	fg := fgThreeSiteRestore()
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"pdx"}}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, apiReader: c, logger: testLogger()}
	tm, _, _ := heldTopologyManager(t)

	_ = runner.applyRestoreProtection(context.Background(), tm, fg, false)
	if !tm.restorePeerHeldSite("pdx") {
		t.Fatal("pdx not held")
	}
	stale := fg.DeepCopy()
	stale.Status.RestoreReclones = nil
	_ = runner.applyRestoreProtection(context.Background(), tm, stale, false)
	if !tm.restorePeerHeldSite("pdx") {
		t.Fatal("a stale view released the hold while the API server still has the queue")
	}

	fresh := fg.DeepCopy()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(fg), fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Status.RestoreReclones = nil
	if err := c.Status().Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	_ = runner.applyRestoreProtection(context.Background(), tm, stale, false)
	if tm.restorePeerHeldSite("pdx") {
		t.Fatal("hold kept after the API server confirmed the queue is gone")
	}
}

func TestStartBootstrap_RefusesArmedOrHeldRecipient(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	peer := &mockMySQL{readOnly: true}
	tm := NewTopologyManager(testTopologyConfig(), []mysql.Checker{primary, peer}, NewFailoverController(testLogger()), nil,
		NewBootstrapController(testLogger()), BootstrapConfig{ReplUser: "repl"}, newMockTainter(), nil, &mockDNS{}, testLogger())
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
	tm.SetRestoreRecloneBlocked([]string{"dc2"})
	for _, source := range []string{"auto-clone", "fresh-deploy", "reclone"} {
		if tm.startBootstrapByName(context.Background(), "dc1", "dc2", source) {
			t.Fatalf("%s bootstrap started into a held recipient", source)
		}
	}
	if phase := tm.BootstrapPhase(); !bootstrapIdlePhase(phase) {
		t.Fatalf("bootstrap phase = %s", phase)
	}
	if peer.cloneCalls != 0 {
		t.Fatal("cloned into a held recipient")
	}
	// Frozen refuses too, for any recipient.
	tm.SetRestoreRecloneBlocked(nil)
	tm.SetTopologyFrozen(true)
	if tm.startBootstrapByName(context.Background(), "dc1", "dc2", "auto-clone") {
		t.Fatal("bootstrap started while an in-place restore froze the topology")
	}
	// A Draining site (pending but not blocked) is admitted: its clone is
	// the reclone that takes it over.
	tm.SetTopologyFrozen(false)
	tm.SetRestoreReclonePending([]string{"dc2"})
	tm.SetRestorePeerHold([]string{"dc2"}, nil, true)
	if !tm.startBootstrapByName(context.Background(), "dc1", "dc2", "reclone") {
		t.Fatal("the reclone of a Draining site must be admitted")
	}
	waitBootstrapIdle(t, tm)
	if tm.restoreMutationInFlight(false) != "" {
		t.Fatalf("admission state left behind: %q", tm.restoreMutationInFlight(false))
	}
}

func waitBootstrapIdle(t *testing.T, tm *TopologyManager) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !bootstrapIdlePhase(BootstrapPhase(tm.BootstrapPhase())) {
		if time.Now().After(deadline) {
			t.Fatalf("bootstrap did not finish (phase %s)", tm.BootstrapPhase())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- settlement ---

func failedRestoreFG(jobRan bool, fenced string) *v1alpha1.MysqlFailoverGroup {
	fg := fgThreeSiteRestore()
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceFailed, TargetSite: "iad", Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}, ConfirmTokenUsed: testRestoreConfirm,
	}
	if jobRan {
		fg.Status.RestoreInPlace.JobName = "mysql-lion-iad-inplace-restore"
	}
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx", "sea"},
		FencedGtidExecuted: fenced,
	}
	return fg
}

func TestSettle_FailedRestoreThatChangedPrimaryHoldsPeers(t *testing.T) {
	fg := failedRestoreFG(true, peerGTID("1-10"))
	r, c := newReconciler(fg, operatorCredsSecret())
	_, mocks := withFenceRunner(r, c, fg)
	mocks["iad"].gtidExecuted = peerGTID("1-14") // the Job's DROPs
	if _, err := r.settleArmedRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	q := getFG(t, c, fg).Status.RestoreReclones
	if q == nil || q.Phase != v1alpha1.RestoreReclonesHeld || !slices.Equal(q.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("queue = %+v, want Held with every peer", q)
	}
	if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreReclonesHeld") {
		t.Fatal("want RestoreReclonesHeld")
	}
}

func TestSettle_FailedRestoreThatLeftPrimaryUnchangedResumesPeers(t *testing.T) {
	fg := failedRestoreFG(true, peerGTID("1-10"))
	r, c := newReconciler(fg, operatorCredsSecret())
	_, mocks := withFenceRunner(r, c, fg)
	mocks["iad"].gtidExecuted = peerGTID("1-10")
	if _, err := r.settleArmedRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q != nil {
		t.Fatalf("queue = %+v, want discarded", q)
	}
	if events := strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"); !strings.Contains(events, "RestoreReclonesDiscarded") ||
		!strings.Contains(events, "unchanged") {
		t.Fatalf("events %q, want RestoreReclonesDiscarded naming the unchanged primary", events)
	}
}

func TestSettle_UnreadablePrimaryOrMissingSnapshotHolds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fenced string
		gtidEr error
	}{
		{"no snapshot", "", nil},
		{"primary unreachable", peerGTID("1-10"), errors.New("connection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := failedRestoreFG(true, tc.fenced)
			r, c := newReconciler(fg, operatorCredsSecret())
			_, mocks := withFenceRunner(r, c, fg)
			mocks["iad"].gtidExecuted, mocks["iad"].gtidExecutedErr = peerGTID("1-10"), tc.gtidEr
			// The read has been failing for longer than the retry bound.
			backdateGTIDReadFailure(r, fg, restoreGTIDReadBound+time.Second)
			if _, err := r.settleArmedRestoreReclones(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesHeld {
				t.Fatalf("queue = %+v, want Held", q)
			}
		})
	}
}

// The settlement runs before the fence is released, so application writes
// cannot move gtid_executed between the end of the restore and the check.
func TestReleaseFence_SettlesQueueFirst(t *testing.T) {
	fg := failedRestoreFG(true, peerGTID("1-10"))
	no := false
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "iad", ConfirmToken: testRestoreConfirm, PriorOfflineMode: &no, SessionsEvicted: true}
	r, c := newReconciler(fg, operatorCredsSecret())
	_, mocks := withFenceRunner(r, c, fg)
	primary := mocks["iad"]
	primary.offlineMode = true
	primary.gtidExecuted = peerGTID("1-10")
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fresh := getFG(t, c, fg)
	if fresh.Status.RestoreReclones != nil || fresh.Status.RestoreOfflineFence != nil || primary.offlineMode {
		t.Fatalf("queue %+v fence %+v offline=%v, want the queue settled and the fence released", fresh.Status.RestoreReclones, fresh.Status.RestoreOfflineFence, primary.offlineMode)
	}
	// The settlement compared gtid_executed while offline_mode was still ON.
	if len(primary.offlineAtGTIDRead) == 0 || slices.Contains(primary.offlineAtGTIDRead, false) {
		t.Fatalf("offline_mode at the gtid_executed reads = %v, want every read made under the fence", primary.offlineAtGTIDRead)
	}
}

func TestHeldQueue_KeepsSiteRemovedFromSpec(t *testing.T) {
	fg := fgThreeSiteRestore()
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, TargetSite: "iad"}
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"pdx", "sea"}}
	fg.Spec.Sites = fg.Spec.Sites[:2] // sea removed
	r, c := newReconciler(fg, operatorCredsSecret())
	if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || !slices.Equal(q.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("queue = %+v, want sea kept in a Held queue", q)
	}
}

// --- signals ---

func TestDeploymentUnstableReason_RestoreReclonePending(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	peer := fg.Status.Sites[1].Name
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{Phase: v1alpha1.RestoreReclonesDraining, Pending: []string{peer}}
	if err := m.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if got := DeploymentUnstableReason(fg); got != "RestoreReclonePending" {
		t.Fatalf("DeploymentUnstableReason = %q, want RestoreReclonePending", got)
	}
}

func TestUpdateCRStatus_RestoreReclonePendingDegradedReason(t *testing.T) {
	fg := newTestFG()
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"dc2"}}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, recorder: record.NewFakeRecorder(10), logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{}}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		ActiveSite: "dc1",
		Sites: []SiteSnapshot{
			{Name: "dc1", State: state.StateWritable},
			{Name: "dc2", State: state.StateReadOnly, SourceConvergenceState: sourceConvergencePending,
				Replication: &mysql.ReplicaStatus{SQLRunning: true, SourceHost: "mysql-dc1"}},
		},
	}
	runner.updateCRStatus(context.Background(), nn, snap)
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatal(err)
	}
	var degraded *metav1.Condition
	for i := range got.Status.Conditions {
		if got.Status.Conditions[i].Type == "Degraded" {
			degraded = &got.Status.Conditions[i]
		}
	}
	if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != "RestoreReclonePending" {
		t.Fatalf("Degraded = %+v, want RestoreReclonePending instead of ReplicationBroken", degraded)
	}
}
