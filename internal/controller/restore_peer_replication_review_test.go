package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// Review follow-ups for #212: admission and settlement races, manager
// replacement, stale snapshots, and unsettled queues.

// blockingReadOnlyChecker blocks CheckReadOnly (the donor confirmation in
// bootstrap admission) until release is closed, once armed.
type blockingReadOnlyChecker struct {
	*mockMySQL
	armed   chan struct{}
	entered chan struct{}
	release chan struct{}
}

func (b *blockingReadOnlyChecker) CheckReadOnly(ctx context.Context) (bool, error) {
	select {
	case <-b.armed:
		select {
		case b.entered <- struct{}{}:
		default:
		}
		<-b.release
	default:
	}
	return b.mockMySQL.CheckReadOnly(ctx)
}

// A clone whose admission is still confirming its donor when a restore
// reaches Fencing is visible to the restore's quiescence check, and is
// refused when it commits after the freeze: it can never start (and run
// its replication setup) under a restore that saw no clone running.
func TestBootstrapAdmission_ReservedAcrossDonorConfirmationAndRefusedAfterFreeze(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(tm *TopologyManager)
	}{
		{"topology frozen", func(tm *TopologyManager) { tm.SetTopologyFrozen(true) }},
		{"recipient queued Armed", func(tm *TopologyManager) { tm.SetRestoreRecloneBlocked([]string{"dc2"}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			donor := &blockingReadOnlyChecker{mockMySQL: &mockMySQL{readOnly: false}, armed: make(chan struct{}),
				entered: make(chan struct{}, 1), release: make(chan struct{})}
			peer := &mockMySQL{readOnly: true}
			tm := NewTopologyManager(testTopologyConfig(), []mysql.Checker{donor, peer}, NewFailoverController(testLogger()), nil,
				NewBootstrapController(testLogger()), BootstrapConfig{ReplUser: "repl", ReplPassword: "pw", CloneTimeout: 5 * time.Second},
				newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
			tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
			close(donor.armed)

			started := make(chan bool, 1)
			go func() { started <- tm.startBootstrapByName(context.Background(), "dc1", "dc2", "auto-clone") }()
			<-donor.entered // admission reserved, donor confirmation in flight

			if wait := tm.restoreMutationInFlight(false); !strings.Contains(wait, "clone admission of dc2") {
				t.Fatalf("quiescence = %q, want it to wait for the clone admission", wait)
			}
			tc.apply(tm) // the restore freezes / queues the recipient
			if wait := tm.restoreMutationInFlight(true); wait == "" {
				t.Fatal("Fencing saw no mutation in flight while a clone admission was pending")
			}
			close(donor.release)
			if <-started {
				t.Fatal("the clone started after the restore froze the topology or queued its recipient")
			}
			if phase := tm.BootstrapPhase(); !bootstrapIdlePhase(phase) || peer.cloneCalls != 0 {
				t.Fatalf("phase=%s clones=%d, want no clone", phase, peer.cloneCalls)
			}
			if wait := tm.restoreMutationInFlight(false); strings.Contains(wait, "admission") {
				t.Fatalf("admission reservation left behind: %q", wait)
			}
		})
	}
}

// unreachableAPIReader fails every read, as an unreachable API server does.
type unreachableAPIReader struct{ client.Reader }

func (unreachableAPIReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("api server unavailable")
}

// A new or replacement manager takes its restore protection from an
// uncached read, not from a cached list item that predates the queue.
func TestInitRestoreProtection_ReadsUncached(t *testing.T) {
	fresh := fgThreeSiteRestore()
	fresh.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, TargetSite: "iad"}
	fresh.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"pdx", "sea"}}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fresh).Build()
	cached := fresh.DeepCopy()
	cached.Status.RestoreReclones = nil

	runner := &TopologyManagerRunner{client: c, apiReader: c, logger: testLogger()}
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	if err := runner.initRestoreProtection(context.Background(), tm, cached, nil); err != nil {
		t.Fatal(err)
	}
	p := tm.restoreProtectionSnapshot()
	if !slices.Equal(p.held, []string{"pdx", "sea"}) || !slices.Equal(p.restop, []string{"pdx", "sea"}) || !slices.Equal(p.blocked, []string{"pdx", "sea"}) {
		t.Fatalf("protection = %+v, want the Held queue read uncached", p)
	}

	// The read fails: a replacement keeps its predecessor's protection...
	failing := &TopologyManagerRunner{client: c, apiReader: unreachableAPIReader{}, logger: testLogger()}
	replacement, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	if err := failing.initRestoreProtection(context.Background(), replacement, cached, &managedTopology{tm: tm}); err != nil {
		t.Fatal(err)
	}
	if got := replacement.restoreProtectionSnapshot(); !got.equal(p) {
		t.Fatalf("replacement protection = %+v, want the predecessor's %+v", got, p)
	}
	// ...and a brand-new manager is not started.
	brandNew, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	if err := failing.initRestoreProtection(context.Background(), brandNew, cached, nil); err == nil {
		t.Fatal("a new manager must not start on a cached view it could not confirm")
	}
}

// After the queue moved on (Draining, then recloned), a stale cached
// Armed/Restoring view must not re-arm the re-stop and stop the
// replication the legitimate reclone started.
func TestApplyRestoreProtection_StaleViewDoesNotReArmReStop(t *testing.T) {
	stale := fgThreeSiteRestore()
	stale.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: "iad"}
	stale.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"dc2"}}

	for _, tc := range []struct {
		name  string
		queue *v1alpha1.RestoreReclonesStatus
	}{
		{"draining", &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesDraining, Pending: []string{"dc2"}}},
		{"recloned", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := stale.DeepCopy()
			current.Status.RestoreInPlace.Phase = v1alpha1.RestoreInPlaceSucceeded
			current.Status.RestoreReclones = tc.queue
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(current).Build()
			runner := &TopologyManagerRunner{client: c, apiReader: c, logger: testLogger()}
			tm, _, peer := heldTopologyManager(t)
			if err := runner.applyRestoreProtection(context.Background(), tm, current, true); err != nil {
				t.Fatal(err)
			}
			// The reclone started dc2's replication.
			peer.replicaStatusVal.IORunning = true

			if err := runner.applyRestoreProtection(context.Background(), tm, stale, false); err != nil {
				t.Fatal(err)
			}
			if _, restop := tm.restorePeerHoldSets(); len(restop) != 0 {
				t.Fatalf("restop = %v after a stale Armed view, want none", restop)
			}
			tm.reassertRestorePeerReplicationStop(context.Background())
			if peer.stopIOCalls != 0 || !peer.replicaStatusVal.IORunning {
				t.Fatalf("the reclone's replication was stopped (%d stops)", peer.stopIOCalls)
			}
		})
	}
}

// A stale cached view cannot clear the clone block either.
func TestApplyRestoreProtection_StaleViewDoesNotUnblockClones(t *testing.T) {
	fg := fgThreeSiteRestore()
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"pdx"}}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, apiReader: c, logger: testLogger()}
	tm, _, _ := heldTopologyManager(t)
	_ = runner.applyRestoreProtection(context.Background(), tm, fg, false)
	stale := fg.DeepCopy()
	stale.Status.RestoreReclones = nil
	_ = runner.applyRestoreProtection(context.Background(), tm, stale, false)
	if !tm.restoreRecloneBlockedSite("pdx") {
		t.Fatal("a stale view unblocked clones into a held site")
	}
}

// Run A's Job ran and failed; its queue was never settled (an operator
// restart) when run B started. B's Preflight releases A's fence, and B then
// ends without a Job. A's peers must stay held: A may have dropped the
// primary's schemas.
func TestUnsettledEarlierQueueIsHeldWhenNextRunEndsWithoutJob(t *testing.T) {
	const confirmA, confirmB = "2026-04-17T14:00:00Z", "2026-04-18T10:00:00Z"
	fg := fgInPlaceRestore("")
	fg.Spec.RestoreInPlace.Confirm = confirmB
	site, peerName := fg.Spec.Sites[0].Name, fg.Spec.Sites[1].Name
	no := false
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full", ConfirmTokenUsed: confirmA, RunConfirm: confirmB, RunID: "run-b",
		StartTime: ptrTime(time.Now()),
	}
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: confirmA, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{peerName}}
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: site, ConfirmToken: confirmA, RunID: "run-a", PriorOfflineMode: &no, SessionsEvicted: true}
	r, c := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret(), primaryFencedPod(fg.Name, site))
	_, mocks := withFenceRunner(r, c, fg)
	r.Recorder = record.NewFakeRecorder(100)
	mocks[site].gtidExecuted = peerGTID("1-10")
	mocks[peerName].replicaStatusErr = errors.New("connection refused") // B cannot stop the peer

	for i := 0; i < 8 && fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing; i++ {
		restorePass(t, r, c, fg)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Fatalf("run B did not reach Fencing: %+v", fg.Status.RestoreInPlace)
	}
	if q := fg.Status.RestoreReclones; q == nil || q.ConfirmToken != confirmB || q.Superseded == nil || !q.Superseded.Held ||
		!slices.Equal(q.Superseded.Pending, []string{peerName}) {
		t.Fatalf("queue = %+v, want B's queue carrying A's as Held", q)
	}
	restorePass(t, r, c, fg)
	backdatePeerStop(r, fg, restorePeerStopTimeout+time.Second)
	restorePass(t, r, c, fg)
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || st.JobName != "" || st.JobCreating {
		t.Fatalf("run B status = %+v, want Failed without a Job", st)
	}
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	q := getFG(t, c, fg).Status.RestoreReclones
	if q == nil || q.Phase != v1alpha1.RestoreReclonesHeld || !slices.Equal(q.Pending, []string{peerName}) {
		t.Fatalf("queue = %+v, want A's peers Held", q)
	}
}

// An Armed queue under another confirm is carried as Held when a new run
// arms its own.
func TestArmRestoreReclones_CarriesUnsettledArmedQueueAsHeld(t *testing.T) {
	fg := fgThreeSiteRestore()
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx"},
		Superseded: &v1alpha1.RestoreReclonesCarryover{ConfirmToken: "2026-04-16T00:00:00Z", Pending: []string{"sea"}}}
	r, c := newReconciler(fg, operatorCredsSecret())
	if err := r.armRestoreReclones(context.Background(), fg, "iad", "2026-04-18T10:00:00Z"); err != nil {
		t.Fatal(err)
	}
	sup := getFG(t, c, fg).Status.RestoreReclones.Superseded
	if sup == nil || !sup.Held || sup.ConfirmToken != testRestoreConfirm || !slices.Equal(sup.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("superseded = %+v, want the unsettled queue (and the queue it replaced) carried Held", sup)
	}
}

func jobLostFG(completed time.Time) *v1alpha1.MysqlFailoverGroup {
	fg := failedRestoreFG(true, peerGTID("1-10"))
	fg.Status.RestoreInPlace.Message = "restore Job mysql-lion-iad-inplace-restore no longer exists; the restore outcome is unknown"
	fg.Status.RestoreInPlace.CompletionTime = &metav1.Time{Time: completed}
	no := false
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "iad", ConfirmToken: testRestoreConfirm, PriorOfflineMode: &no, SessionsEvicted: true}
	return fg
}

func restorePod(phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-lion-iad-inplace-restore-x", Namespace: "ns",
			Labels: map[string]string{"batch.kubernetes.io/job-name": "mysql-lion-iad-inplace-restore"}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// JobLost with an unchanged gtid_executed: an orphaned or terminating pod
// may still run the restore script, so the queue stays Armed and the fence
// stays up until every pod terminated; only then are the peers resumed.
func TestSettle_JobLostWaitsForRestorePodsBeforeDiscarding(t *testing.T) {
	fg := jobLostFG(time.Now())
	pod := restorePod(corev1.PodRunning)
	r, c := newReconciler(fg, operatorCredsSecret(), pod)
	_, mocks := withFenceRunner(r, c, fg)
	primary := mocks["iad"]
	primary.offlineMode, primary.gtidExecuted = true, peerGTID("1-10")

	wait, err := r.reconcileRestoreOfflineFence(context.Background(), fg)
	if err != nil || wait == 0 {
		t.Fatalf("wait=%s err=%v, want a requeue while the restore pod runs", wait, err)
	}
	fresh := getFG(t, c, fg)
	if q := fresh.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed || fresh.Status.RestoreOfflineFence == nil || !primary.offlineMode {
		t.Fatalf("queue %+v fence %+v offline=%v, want Armed with the fence kept", q, fresh.Status.RestoreOfflineFence, primary.offlineMode)
	}

	pod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	*fg = *fresh
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fresh = getFG(t, c, fg)
	if fresh.Status.RestoreReclones != nil || fresh.Status.RestoreOfflineFence != nil {
		t.Fatalf("queue %+v fence %+v, want discarded (primary unchanged, no pod left) and released", fresh.Status.RestoreReclones, fresh.Status.RestoreOfflineFence)
	}
}

// A restore pod that never terminates within the bound is not proof of an
// unchanged primary: the peers are held.
func TestSettle_JobLostWithPodsAliveBeyondBoundHolds(t *testing.T) {
	fg := jobLostFG(time.Now().Add(-restoreSettleBound - time.Minute))
	r, c := newReconciler(fg, operatorCredsSecret(), restorePod(corev1.PodRunning))
	_, mocks := withFenceRunner(r, c, fg)
	mocks["iad"].gtidExecuted = peerGTID("1-10")
	if _, err := r.settleArmedRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesHeld {
		t.Fatalf("queue = %+v, want Held", q)
	}
}

// A momentary failure reading the primary is retried (fence up) before
// the peers are held.
func TestSettle_GTIDReadFailureRetriesBeforeHolding(t *testing.T) {
	fg := jobLostFG(time.Now())
	r, c := newReconciler(fg, operatorCredsSecret())
	_, mocks := withFenceRunner(r, c, fg)
	mocks["iad"].gtidExecuted, mocks["iad"].gtidExecutedErr = peerGTID("1-10"), errors.New("connection reset")
	wait, err := r.settleArmedRestoreReclones(context.Background(), fg)
	if err != nil || wait == 0 {
		t.Fatalf("wait=%s err=%v, want a retry", wait, err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
		t.Fatalf("queue = %+v, want still Armed while retrying", q)
	}
	mocks["iad"].gtidExecutedErr = nil // it recovers: unchanged, so discarded
	*fg = *getFG(t, c, fg)
	if _, err := r.settleArmedRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q != nil {
		t.Fatalf("queue = %+v, want discarded once the read succeeded", q)
	}

	late := jobLostFG(time.Now().Add(-restoreGTIDReadBound - time.Second))
	r2, c2 := newReconciler(late, operatorCredsSecret())
	_, mocks2 := withFenceRunner(r2, c2, late)
	mocks2["iad"].gtidExecutedErr = errors.New("connection reset")
	backdateGTIDReadFailure(r2, late, restoreGTIDReadBound+time.Second)
	if wait, err := r2.settleArmedRestoreReclones(context.Background(), late); err != nil || wait != 0 {
		t.Fatalf("wait=%s err=%v past the bound, want a decision", wait, err)
	}
	if q := getFG(t, c2, late).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesHeld {
		t.Fatalf("queue = %+v, want Held after the bound", q)
	}
}

// noStopperChecker hides every optional interface of the mock.
type noStopperChecker struct{ mysql.Checker }

func TestReconcileInPlaceRestore_PreflightRejectsPeerWithoutStopper(t *testing.T) {
	fg := fgInPlaceRestore("")
	r, c := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret())
	cfg := testTopologyConfig()
	cfg.Sites = []SiteTopologyConfig{
		{Name: fg.Spec.Sites[0].Name, Role: state.SiteRolePrimaryCandidate, Host: "mysql-a"},
		{Name: fg.Spec.Sites[1].Name, Role: state.SiteRolePrimaryCandidate, Host: "mysql-b"},
	}
	tm := NewTopologyManager(cfg, []mysql.Checker{&mockMySQL{}, noStopperChecker{&mockMySQL{readOnly: true}}}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	r.Runner = &TopologyManagerRunner{client: c, recorder: r.Recorder, logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{client.ObjectKeyFromObject(fg): {tm: tm}}}
	restorePass(t, r, c, fg)
	st := fg.Status.RestoreInPlace
	if st == nil || st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "cannot stop the replication receiver of peer") || st.ConfirmTokenUsed != "" {
		t.Fatalf("status = %+v, want a non-consuming rejection", st)
	}
}

func TestInitiateRecovery_HeldSiteNotMarkedInProgress(t *testing.T) {
	tm, _, peer := heldTopologyManager(t)
	peer.replicaStatusVal = nil
	tm.SetRestorePeerHold([]string{"dc2"}, nil, true)
	changed, acted, rewrite := tm.initiateRecovery(context.Background(), 1, 0, []*mysql.ReplicaStatus{nil, nil})
	if changed || acted || rewrite {
		t.Fatalf("changed=%v acted=%v rewrite=%v, want recovery skipped", changed, acted, rewrite)
	}
	tm.mu.RLock()
	rec := tm.recovery["dc2"]
	tm.mu.RUnlock()
	if rec != nil && rec.state == v1alpha1.RecoveryStateInProgress {
		t.Fatal("a held site was marked RecoveryInProgress")
	}
}

// blockingStopChecker blocks StopReplica (the first step of old-primary
// recovery and of a repoint) until release is closed.
type blockingStopChecker struct {
	*mockMySQL
	entered chan struct{}
	release chan struct{}
}

func (b *blockingStopChecker) StopReplica(ctx context.Context) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return b.mockMySQL.StopReplica(ctx)
}

// An old-primary recovery admitted before the hold finishes (including its
// START) before the restore can stop the receiver; later ones are refused.
func TestReplicaStartGate_RecoverOldPrimary(t *testing.T) {
	primary := &mockMySQL{readOnly: false, gtidExecuted: peerGTID("1-10")}
	inner := &mockMySQL{readOnly: true, gtidExecuted: peerGTID("1-10")}
	peer := &blockingStopChecker{mockMySQL: inner, entered: make(chan struct{}, 1), release: make(chan struct{})}
	tm := NewTopologyManager(testTopologyConfig(), []mysql.Checker{primary, peer}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), nil, &mockDNS{}, testLogger())
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly

	done := make(chan struct{})
	go func() { defer close(done); tm.executeRecovery(context.Background(), 1, 0) }()
	<-peer.entered
	if _, err := tm.StopRestorePeerReplication(context.Background(), "dc2", peerGTID("1-10"), false); !errors.Is(err, errRestorePeerStartBusy) {
		t.Fatalf("stop during recovery: err = %v, want busy", err)
	}
	close(peer.release)
	<-done
	if inner.startReplicaCalls != 1 {
		t.Fatalf("admitted recovery started replication %d times, want 1", inner.startReplicaCalls)
	}
	if out, err := tm.StopRestorePeerReplication(context.Background(), "dc2", peerGTID("1-10"), false); err != nil || !out.Stopped {
		t.Fatalf("stop = %+v, %v", out, err)
	}
	peer.release = make(chan struct{})
	close(peer.release)
	tm.executeRecovery(context.Background(), 1, 0)
	if inner.startReplicaCalls != 1 {
		t.Fatalf("recovery restarted a held site (%d starts)", inner.startReplicaCalls)
	}
}

// A repoint whose source change fails rolls back with START REPLICA. The
// rollback start runs inside the gate, so the restore waits for it and then
// stops the receiver it started.
func TestReplicaStartGate_RepointRollbackStart(t *testing.T) {
	primary := &mockMySQL{readOnly: false, gtidExecuted: peerGTID("1-10")}
	inner := &mockMySQL{readOnly: true, gtidExecuted: peerGTID("1-10"), changeSourceErr: errors.New("change failed"),
		replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "old-hop"}}
	peer := &blockingStopChecker{mockMySQL: inner, entered: make(chan struct{}, 1), release: make(chan struct{})}
	cfg := testTopologyConfig()
	tm := NewTopologyManager(cfg, []mysql.Checker{primary, peer}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), nil, &mockDNS{}, testLogger())
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly

	done := make(chan struct{})
	go func() {
		defer close(done)
		tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, {IORunning: true, SQLRunning: true, SourceHost: "old-hop"}})
	}()
	<-peer.entered
	if _, err := tm.StopRestorePeerReplication(context.Background(), "dc2", "", false); !errors.Is(err, errRestorePeerStartBusy) {
		t.Fatalf("stop during repoint: err = %v, want busy", err)
	}
	close(peer.release)
	<-done
	if inner.startReplicaCalls != 1 {
		t.Fatalf("rollback start ran %d times, want 1", inner.startReplicaCalls)
	}
	out, err := tm.StopRestorePeerReplication(context.Background(), "dc2", "", false)
	if err != nil || !out.Stopped {
		t.Fatalf("stop = %+v, %v", out, err)
	}
	inner.mu.Lock()
	io := inner.replicaStatusVal.IORunning
	inner.mu.Unlock()
	if io {
		t.Fatal("the rollback's receiver kept running after the verified stop")
	}
}

// Draining: each queued site's reclone restarts its own replication; no
// operator path starts another queued site, and Poll does not re-stop the
// recloned one.
func TestDrainingReclone_StartsOnlyItsOwnSite(t *testing.T) {
	gtid := peerGTID("1-10")
	stopped := func() *mysql.ReplicaStatus {
		return &mysql.ReplicaStatus{SQLRunning: true, SourceHost: "mysql-dc1", RetrievedGtidSet: gtid, ExecutedGtidSet: gtid}
	}
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: gtid, replicaStatusVal: stopped()}
	dc3 := &mockMySQL{readOnly: true, gtidExecuted: gtid, replicaStatusVal: stopped()}
	cfg := testTopologyConfig()
	cfg.Sites[0].Host, cfg.Sites[1].Host = "mysql-dc1", "mysql-dc2"
	cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: "dc3", Zone: "lion-dc3", LBIP: "3.3.3.3", Role: state.SiteRolePrimaryCandidate,
		TaintSelector: taintSelector("dc3"), Host: "mysql-dc3"})
	tm := NewTopologyManager(cfg, []mysql.Checker{dc1, dc2, dc3}, NewFailoverController(testLogger()), nil,
		NewBootstrapController(testLogger()), BootstrapConfig{ReplUser: "repl", ReplPassword: "pw", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	tm.failoverCooldown = 0
	tm.autoBootstrapSuppressed = true
	tm.SetRestorePeerHold([]string{"dc2", "dc3"}, nil, true)
	tm.SetRestoreReclonePending([]string{"dc2", "dc3"})
	pollN(tm, 2)

	done := make(chan RecloneOutcome, 1)
	tm.RecloneCompleteCallback = func(_ context.Context, site string, outcome RecloneOutcome) error {
		if site == "dc2" {
			done <- outcome
		}
		return nil
	}
	tm.SetRecloneSiteFor("dc2", "c1")
	if !tm.checkReclone(context.Background()) {
		t.Fatal("expected the reclone of dc2 to start")
	}
	select {
	case outcome := <-done:
		if outcome.Err != nil {
			t.Fatalf("reclone failed: %v", outcome.Err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("reclone did not complete")
	}
	pollN(tm, 3)
	if dc2.startReplicaCalls == 0 || dc2.stopIOCalls != 0 {
		t.Fatalf("dc2 starts=%d io-stops=%d, want its reclone's replication running and not re-stopped", dc2.startReplicaCalls, dc2.stopIOCalls)
	}
	if dc3.startReplicaCalls != 0 {
		t.Fatalf("dc3 (still queued) was started %d times", dc3.startReplicaCalls)
	}
}
