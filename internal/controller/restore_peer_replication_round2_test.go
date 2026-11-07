package controller

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// Second review round for #212.

const restoreRoundTwoUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// A planned failover already in flight when a full-instance restore is
// requested is still driven while the restore waits in Preflight, reaches
// a terminal phase, and only then does the restore start Fencing.
func TestReconcile_PlannedFailoverInFlightFinishesBeforeRestoreFences(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fenced := restoreRoundTwoUUID + ":1-10"
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		RunID: "run-before-restore", Revision: 3, Phase: v1alpha1.PlannedFailoverPhaseDraining, TargetSite: "pdx", SourceSite: "iad",
		StartTime: start, ClaimTime: &start, ClaimBaseline: &v1alpha1.FailoverRecordRef{},
		Settings: v1alpha1.PlannedFailoverSettings{DrainTimeout: metav1.Duration{Duration: 30 * time.Second}, MaxLagWait: metav1.Duration{Duration: time.Nanosecond}},
		Fence:    &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceApplied, IntentTime: start, SourceServerUUID: restoreRoundTwoUUID, GtidSet: &fenced},
		Drain:    &v1alpha1.PlannedFailoverDrainRecord{StartTime: start},
	}
	fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{
		Confirm: testRestoreConfirm,
		Source:  v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "seed"}},
	}
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: false, gtidExecuted: restoreRoundTwoUUID + ":1-10"}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: restoreRoundTwoUUID + ":1-5"}
	r, c := newReconciler(fg)
	r.Recorder = record.NewFakeRecorder(200)
	r.Runner = plannedDrainRunner(nn, iad, pdx)
	r.Runner.client, r.Runner.recorder = c, r.Recorder
	if _, err := r.Runner.restoreFenceManager(nn); err != nil {
		t.Fatal(err)
	}

	pfTerminalBeforeFencing := false
	for pass := 0; pass < 12; pass++ {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		cur := getFG(t, c, fg)
		pfDone := cur.Status.PlannedFailover != nil && plannedFailoverTerminal(cur.Status.PlannedFailover)
		if rip := cur.Status.RestoreInPlace; rip != nil && rip.Phase == v1alpha1.RestoreInPlaceFencing {
			pfTerminalBeforeFencing = pfDone
			break
		}
		drainEvents(r.Recorder.(*record.FakeRecorder))
	}
	cur := getFG(t, c, fg)
	if pf := cur.Status.PlannedFailover; pf == nil || !plannedFailoverTerminal(pf) {
		t.Fatalf("planned failover = %+v, want it driven to a terminal phase (it deadlocked behind the restore's Preflight)", pf)
	}
	if rip := cur.Status.RestoreInPlace; rip == nil || rip.Phase != v1alpha1.RestoreInPlaceFencing || !pfTerminalBeforeFencing {
		t.Fatalf("restore = %+v (planned failover terminal before Fencing: %v), want Fencing only after the failover finished", rip, pfTerminalBeforeFencing)
	}
}

// guardsRunner returns a runner whose cached list (client) and uncached
// reader (apiReader) hold different views of the group.
func guardsRunner(cached, authoritative *v1alpha1.MysqlFailoverGroup) *TopologyManagerRunner {
	cachedClient := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(cached).Build()
	reader := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(authoritative).Build()
	return &TopologyManagerRunner{client: cachedClient, apiReader: reader, logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{}}
}

// heldGroupViews returns a group whose authoritative status holds dc2 after
// a failed full-instance restore, and a stale cached copy without any
// restore state.
func heldGroupViews() (cached, authoritative *v1alpha1.MysqlFailoverGroup) {
	authoritative = newTestFG()
	authoritative.Status.ActiveSite = "dc1"
	authoritative.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, TargetSite: "dc1", Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}}
	authoritative.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"dc2"}}
	cached = authoritative.DeepCopy()
	cached.Status.RestoreInPlace, cached.Status.RestoreReclones = nil, nil
	return cached, authoritative
}

// Every restore guard of a new manager comes from the uncached read, and a
// later sync with the stale cached list item releases none of them: with
// the primary gone, the held peer is not promoted.
func TestRestoreGuards_StaleCacheNeverReleasesThem_NoPromotionOfHeldPeer(t *testing.T) {
	cached, authoritative := heldGroupViews()
	runner := guardsRunner(cached, authoritative)
	primary := &mockMySQL{readOnly: false, gtidExecuted: peerGTID("1-10")}
	peer := &mockMySQL{readOnly: true, gtidExecuted: peerGTID("1-10"),
		replicaStatusVal: &mysql.ReplicaStatus{SQLRunning: true, SourceHost: "mysql-dc1"}}
	tm, _, _ := newTestTopologyManager(primary, peer)

	if err := runner.initRestoreProtection(context.Background(), tm, cached, nil); err != nil {
		t.Fatal(err)
	}
	// What sync does on every pass for a running manager.
	for i := 0; i < 3; i++ {
		_ = runner.applyRestoreProtection(context.Background(), tm, cached, false)
	}
	p := tm.restoreProtectionSnapshot()
	if !slices.Equal(p.held, []string{"dc2"}) || !slices.Equal(p.restop, []string{"dc2"}) || !slices.Equal(p.blocked, []string{"dc2"}) ||
		!slices.Equal(p.pending, []string{"dc2"}) {
		t.Fatalf("guards = %+v after syncs with a stale cache, want dc2 held, re-stopped, blocked and pending", p)
	}

	pollN(tm, 3)
	primary.mu.Lock()
	primary.err = errors.New("connection refused")
	primary.mu.Unlock()
	pollN(tm, 6)
	if !peer.readOnly || peer.promoted {
		t.Fatal("the held peer was promoted after the primary failed")
	}
	if peer.startReplicaCalls != 0 {
		t.Fatalf("the held peer's replication was started %d times", peer.startReplicaCalls)
	}
}

// The freeze and the offline_mode fence are guards too: a stale cache does
// not unfreeze a running restore or stop re-asserting its fence.
func TestRestoreGuards_StaleCacheKeepsFreezeAndFence(t *testing.T) {
	no := false
	authoritative := newTestFG()
	authoritative.Status.ActiveSite = "dc1"
	authoritative.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: "dc1", Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}, RunID: "run-1"}
	authoritative.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"dc2"}}
	authoritative.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "dc1", ConfirmToken: testRestoreConfirm, RunID: "run-1", PriorOfflineMode: &no, SessionsEvicted: true}
	cached := authoritative.DeepCopy()
	cached.Status.RestoreInPlace, cached.Status.RestoreReclones, cached.Status.RestoreOfflineFence = nil, nil, nil
	runner := guardsRunner(cached, authoritative)
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})

	if err := runner.initRestoreProtection(context.Background(), tm, cached, nil); err != nil {
		t.Fatal(err)
	}
	_ = runner.applyRestoreProtection(context.Background(), tm, cached, false)
	p := tm.restoreProtectionSnapshot()
	if !p.frozen || p.fenceSite != "dc1" || p.fenceOwner != "run-1" {
		t.Fatalf("guards = %+v, want frozen with dc1's fence", p)
	}
}

// When the uncached read fails, a replacement inherits every guard of its
// predecessor, not only the hold sets.
func TestInitRestoreProtection_InheritsEveryGuardOnReadFailure(t *testing.T) {
	cached, _ := heldGroupViews()
	prev, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	prev.setRestoreProtection(restoreProtection{held: []string{"dc2"}, restop: []string{"dc2"}, blocked: []string{"dc2"},
		pending: []string{"dc2"}, frozen: true, fenceSite: "dc1", fenceOwner: "run-1"})
	runner := &TopologyManagerRunner{client: fake.NewClientBuilder().WithScheme(testScheme()).Build(), apiReader: unreachableAPIReader{}, logger: testLogger()}
	next, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	if err := runner.initRestoreProtection(context.Background(), next, cached, &managedTopology{tm: prev}); err != nil {
		t.Fatal(err)
	}
	if got, want := next.restoreProtectionSnapshot(), prev.restoreProtectionSnapshot(); !got.equal(want) {
		t.Fatalf("replacement guards = %+v, want the predecessor's %+v", got, want)
	}
}

// Through the runner's own sync: a new manager started from a stale cached
// list item gets its guards from the uncached read, and later syncs keep
// them.
func TestSync_NewManagerTakesRestoreGuardsFromUncachedRead(t *testing.T) {
	cached, authoritative := heldGroupViews()
	secret := newTestSecret()
	secret.Data = lifecycleSecretData()
	cachedClient := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(cached, secret).Build()
	reader := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(authoritative).Build()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	runner := &TopologyManagerRunner{client: cachedClient, apiReader: reader, logger: logger, hub: platform.NewHub(logger),
		managers: map[types.NamespacedName]*managedTopology{}}
	t.Cleanup(runner.stopAll)
	nn := FailoverGroupNamespacedName(cached)

	for i := 0; i < 2; i++ {
		if err := runner.sync(context.Background()); err != nil {
			t.Fatalf("sync %d: %v", i, err)
		}
	}
	mt := registeredManager(runner, nn)
	if mt == nil {
		t.Fatal("no manager started")
	}
	p := mt.tm.restoreProtectionSnapshot()
	if !slices.Equal(p.held, []string{"dc2"}) || !slices.Equal(p.pending, []string{"dc2"}) || !slices.Equal(p.blocked, []string{"dc2"}) {
		t.Fatalf("guards = %+v, want dc2 held, pending and blocked from the uncached read", p)
	}
	mt.tm.mu.RLock()
	excluded := mt.tm.restoreReclonePendingLocked("dc2")
	mt.tm.mu.RUnlock()
	if !excluded {
		t.Fatal("dc2 is not excluded from promotion")
	}
}

// Run A's Job ran and failed, and its queue was not settled yet when the
// user bumped confirm. Run B then ends inside its first Preflight pass
// (invalid spec, a peer privilege rejection, or a confirm revert). A's
// queue must end Held: its peers may be the only intact copies.
func TestEarlierRunQueueHeldWhenNextRunEndsInPreflight(t *testing.T) {
	const confirmA, confirmB = "2026-04-17T14:00:00Z", "2026-04-18T10:00:00Z"
	for _, tc := range []struct {
		name  string
		setup func(fg *v1alpha1.MysqlFailoverGroup, peer *mockMySQL)
	}{
		{"invalid spec", func(fg *v1alpha1.MysqlFailoverGroup, _ *mockMySQL) {
			fg.Spec.RestoreInPlace.LoadOptions = &v1alpha1.LoadOptions{IncludeSchemas: []string{"a", "b"}}
		}},
		{"peer privilege rejection", func(_ *v1alpha1.MysqlFailoverGroup, peer *mockMySQL) {
			peer.missingIOStopPriv = []string{"REPLICATION_SLAVE_ADMIN"}
		}},
		{"confirm reverted after B started", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("")
			fg.Spec.RestoreInPlace.Confirm = confirmB
			site, peerName := fg.Spec.Sites[0].Name, fg.Spec.Sites[1].Name
			no := false
			now := metav1.Now()
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
				Phase: v1alpha1.RestoreInPlaceFailed, TargetSite: site, Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}, JobName: inPlaceRestoreJobName(fg.Name, site),
				ConfirmTokenUsed: confirmA, CompletionTime: &now,
			}
			fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: confirmA, Phase: v1alpha1.RestoreReclonesArmed,
				Pending: []string{peerName}, FencedGtidExecuted: peerGTID("1-10")}
			fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: site, ConfirmToken: confirmA, RunID: "run-a", PriorOfflineMode: &no, SessionsEvicted: true}
			r, c := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret())
			tm, mocks := withFenceRunner(r, c, fg)
			r.Recorder = record.NewFakeRecorder(100)
			mocks[site].gtidExecuted = peerGTID("1-14") // A's DROPs
			mocks[site].offlineMode = true
			if tc.setup != nil {
				tc.setup(fg, mocks[peerName])
				if err := c.Update(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}

			*fg = *getFG(t, c, fg)
			if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			if tc.setup == nil {
				// B is in Preflight; the confirm is reverted to A's.
				*fg = *getFG(t, c, fg)
				fg.Spec.RestoreInPlace.Confirm = confirmA
				if err := c.Update(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 3; i++ {
				*fg = *getFG(t, c, fg)
				if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
				if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
				if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}
			fresh := getFG(t, c, fg)
			q := fresh.Status.RestoreReclones
			if q == nil || q.Phase != v1alpha1.RestoreReclonesHeld || !slices.Contains(q.Pending, peerName) {
				t.Fatalf("queue = %+v (restore %+v), want A's peers Held", q, fresh.Status.RestoreInPlace)
			}
			if !tm.restorePeerHeldSite(peerName) {
				t.Fatal("the peer's replication is no longer held")
			}
			if strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreReclonesDiscarded") {
				t.Fatal("A's queue was discarded")
			}
		})
	}
}

// Self-audit: a planned failover to a queued site is refused from the
// queue itself, even when the observed status still says it replicates.
func TestValidatePlannedFailover_RefusesQueuedTarget(t *testing.T) {
	fg := plannedFailoverFG("")
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{Phase: v1alpha1.RestoreReclonesDraining, Pending: []string{"pdx"}}
	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
	if result != PlannedFailoverReject || reason != "TargetUnhealthy" || err == nil || !strings.Contains(err.Error(), "post-restore reclone") {
		t.Fatalf("result=%v reason=%q err=%v, want TargetUnhealthy for a queued target", result, reason, err)
	}
}

// Self-audit: a stale cached view without the queue does not put a queued
// site back into the reader Services; an uncached read confirms first.
func TestSyncPodLabels_StaleCacheKeepsQueuedSiteOutOfReaderServices(t *testing.T) {
	for _, tc := range []struct {
		name        string
		queued      bool
		wantHealthy string
	}{
		{"still queued", true, v1alpha1.LabelValueFalse},
		{"queue gone", false, v1alpha1.LabelValueTrue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cached := newTestFG()
			cached.Status.ActiveSite = "dc1"
			cached.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: v1alpha1.SiteStateWritable}, {Name: "dc2", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy}}
			authoritative := cached.DeepCopy()
			if tc.queued {
				authoritative.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"dc2"}}
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mysql-lion-dc2-x", Namespace: cached.Namespace, Labels: map[string]string{
				labelAppName: "mysql", labelInstance: cached.Name, v1alpha1.SiteLabel: "dc2", v1alpha1.RoleLabel: "replica", v1alpha1.HealthyLabel: v1alpha1.LabelValueFalse}}}
			r, c := newReconciler(cached, pod)
			r.APIReader = fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(authoritative).Build()
			if err := r.syncPodLabels(context.Background(), getFG(t, c, cached)); err != nil {
				t.Fatal(err)
			}
			var got corev1.Pod
			if err := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &got); err != nil {
				t.Fatal(err)
			}
			if got.Labels[v1alpha1.HealthyLabel] != tc.wantHealthy {
				t.Fatalf("healthy = %q, want %q", got.Labels[v1alpha1.HealthyLabel], tc.wantHealthy)
			}
		})
	}
}
