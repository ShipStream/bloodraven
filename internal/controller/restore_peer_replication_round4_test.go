package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Fourth review round for #212.

// backdateGTIDReadFailure makes fg's settlement look as if its first GTID
// read failed d ago.
func backdateGTIDReadFailure(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
	r.restoreGTIDReadFailedSince(fg)
	nn := client.ObjectKeyFromObject(fg)
	r.restorePeerStopMu.Lock()
	f := r.restoreGTIDReadFailures[nn]
	f.since = time.Now().Add(-d)
	r.restoreGTIDReadFailures[nn] = f
	r.restorePeerStopMu.Unlock()
}

// The GTID-read retry is measured from the first failed read, not from the
// run's completion: a pod wait that took five minutes must not turn the
// first read error into a permanent Held.
func TestSettle_GTIDReadRetryCountsFromFirstFailure(t *testing.T) {
	fg := jobLostFG(time.Now().Add(-5 * time.Minute))
	r, c := newReconciler(fg, operatorCredsSecret(), restorePod(corev1.PodSucceeded))
	_, mocks := withFenceRunner(r, c, fg)
	mocks["iad"].gtidExecuted, mocks["iad"].gtidExecutedErr = peerGTID("1-10"), errors.New("connection reset")
	wait, err := r.settleArmedRestoreReclones(context.Background(), fg)
	if err != nil || wait == 0 {
		t.Fatalf("wait=%s err=%v, want a retry on the first failed read", wait, err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
		t.Fatalf("queue = %+v, want still Armed", q)
	}
}

// No topology manager yet (operator start, replacement) is a wait within
// the pod bound, not an unreadable primary.
func TestSettle_MissingManagerWaits(t *testing.T) {
	fg := jobLostFG(time.Now().Add(-5 * time.Minute))
	r, c := newReconciler(fg, operatorCredsSecret())
	r.Runner = &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	backdateGTIDReadFailure(r, fg, restoreGTIDReadBound+time.Second) // irrelevant: no read was made
	wait, err := r.settleArmedRestoreReclones(context.Background(), fg)
	if err != nil || wait == 0 {
		t.Fatalf("wait=%s err=%v, want a wait for the manager", wait, err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
		t.Fatalf("queue = %+v, want still Armed", q)
	}
	late := jobLostFG(time.Now().Add(-restoreSettleBound - time.Minute))
	r2, c2 := newReconciler(late, operatorCredsSecret())
	r2.Runner = &TopologyManagerRunner{client: c2, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	if wait, err := r2.settleArmedRestoreReclones(context.Background(), late); err != nil || wait != 0 {
		t.Fatalf("wait=%s err=%v past the bound, want a decision", wait, err)
	}
	if q := getFG(t, c2, late).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesHeld {
		t.Fatalf("queue = %+v, want Held past the bound", q)
	}
}

// A failed discard write keeps the fence up: application writes must not
// move gtid_executed while the queue is still Armed.
func TestSettle_FailedDiscardWriteKeepsFence(t *testing.T) {
	fg := jobLostFG(time.Now())
	r, base := newReconciler(fg, operatorCredsSecret())
	failing := true
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if m, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && failing && m.Status.RestoreReclones == nil {
				return errors.New("etcdserver: request timed out")
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	r.Client = c
	_, mocks := withFenceRunner(r, c, fg)
	primary := mocks["iad"]
	primary.offlineMode, primary.gtidExecuted = true, peerGTID("1-10")

	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err == nil {
		t.Fatal("want the failed discard write reported")
	}
	fresh := getFG(t, base, fg)
	if fresh.Status.RestoreReclones == nil || fresh.Status.RestoreOfflineFence == nil || !primary.offlineMode {
		t.Fatalf("queue %+v fence %+v offline=%v, want both kept and offline_mode ON", fresh.Status.RestoreReclones, fresh.Status.RestoreOfflineFence, primary.offlineMode)
	}

	failing = false
	*fg = *fresh
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fresh = getFG(t, base, fg); fresh.Status.RestoreReclones != nil || fresh.Status.RestoreOfflineFence != nil || primary.offlineMode {
		t.Fatalf("queue %+v fence %+v offline=%v, want discarded then released", fresh.Status.RestoreReclones, fresh.Status.RestoreOfflineFence, primary.offlineMode)
	}
}

// A peer added after acceptance ends the run before offline_mode goes up.
func TestInPlaceFencing_PeerSetChangeCancelsBeforeOfflineFence(t *testing.T) {
	r, c, fg, primary, _, _ := peerFencingRestore(t, peerGTID("1-10"))
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: fg.Status.RestoreInPlace.TargetSite, ConfirmToken: fg.Spec.RestoreInPlace.Confirm}
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
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "peer set changed") {
		t.Fatalf("status %+v, want cancelled", st)
	}
	if primary.offlineMode || len(primary.offlineModeSets) != 0 || primary.evictCalls != 0 {
		t.Fatalf("offline_mode=%v sets=%v evictions=%d, want the fence never applied", primary.offlineMode, primary.offlineModeSets, primary.evictCalls)
	}
}

// interleavingReader returns a view taken before the fence's prior value
// was recorded and, while serving it, lets the restore arm the fence: the
// interleaving "snapshot read, EnforceRestoreOfflineFence, stale write".
type interleavingReader struct {
	client.Reader
	view    *v1alpha1.MysqlFailoverGroup
	enforce func()
	done    chan struct{}
}

func (i *interleavingReader) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	i.view.DeepCopyInto(obj.(*v1alpha1.MysqlFailoverGroup))
	if i.enforce != nil {
		enforce := i.enforce
		i.enforce = nil
		go func() { defer close(i.done); enforce() }()
		// Give Enforce the chance to run before this read returns. With
		// the guard lock it blocks until the application below finished.
		select {
		case <-i.done:
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil
}

// The fence armed by the restore while a guard application holds an older
// view is not cleared by that application.
func TestApplyRestoreProtection_FenceArmedDuringReadIsKept(t *testing.T) {
	running := fgThreeSiteRestore()
	running.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFencing, TargetSite: "iad", Scope: "full", RunID: "run-1"}
	running.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx", "sea"}}
	running.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "iad", ConfirmToken: testRestoreConfirm, RunID: "run-1"} // prior not yet recorded

	tm, mocks := fenceTopologyManager(running)
	reader := &interleavingReader{view: running, done: make(chan struct{})}
	reader.enforce = func() {
		if _, _, err := tm.EnforceRestoreOfflineFence(context.Background(), "iad", "run-1"); err != nil {
			t.Error(err)
		}
	}
	runner := &TopologyManagerRunner{apiReader: reader, logger: testLogger(), recorder: record.NewFakeRecorder(10),
		managers: map[types.NamespacedName]*managedTopology{fgNN(running): {tm: tm}}}
	if err := runner.applyRestoreProtection(context.Background(), tm, running, true); err != nil {
		t.Fatal(err)
	}
	<-reader.done
	if p := tm.restoreProtectionSnapshot(); p.fenceSite != "iad" || p.fenceOwner != "run-1" {
		t.Fatalf("fence guard = %+v, want the fence armed during the read kept", p)
	}
	mocks["iad"].mu.Lock()
	mocks["iad"].offlineMode = false
	mocks["iad"].mu.Unlock()
	tm.reassertRestoreOfflineFence(context.Background())
	mocks["iad"].mu.Lock()
	on := mocks["iad"].offlineMode
	mocks["iad"].mu.Unlock()
	if !on {
		t.Fatal("the armed fence is no longer re-asserted")
	}
}
