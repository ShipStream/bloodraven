package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Third review round for #212.

// While settlement waits for a failed restore's Job pods (terminal status,
// Armed queue, fence still recorded), the topology manager keeps
// re-asserting offline_mode: a mysqld restart in that window must not
// readmit applications while a restore pod may still drop schemas. Once
// settled, the fence is released and no longer re-asserted.
func TestSettlementWait_KeepsReassertingOfflineFence(t *testing.T) {
	fg := jobLostFG(time.Now())
	pod := restorePod(corev1.PodRunning)
	r, c := newReconciler(fg, dsnSecret(), pod)
	tm, mocks := withFenceRunner(r, c, fg)
	primary := mocks["iad"]
	primary.offlineMode, primary.gtidExecuted = true, peerGTID("1-10")

	if wait, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil || wait == 0 {
		t.Fatalf("wait=%s err=%v, want settlement waiting on the live pod", wait, err)
	}
	r.Runner.SetRestorePeerHold(context.Background(), getFG(t, c, fg)) // what every sync and status write does
	if p := tm.restoreProtectionSnapshot(); p.fenceSite != "iad" {
		t.Fatalf("fence guard = %+v, want iad re-asserted while settlement waits", p)
	}

	primary.mu.Lock()
	primary.offlineMode = false // mysqld restarted
	primary.mu.Unlock()
	tm.Poll(context.Background())
	primary.mu.Lock()
	on := primary.offlineMode
	primary.mu.Unlock()
	if !on {
		t.Fatal("offline_mode not re-asserted after a mysqld restart during the settlement wait")
	}

	// The pod ends: settlement discards the queue (primary unchanged) and
	// releases the fence, which is then no longer re-asserted.
	pod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	r.Runner.SetRestorePeerHold(context.Background(), getFG(t, c, fg))
	if p := tm.restoreProtectionSnapshot(); p.fenceSite != "" {
		t.Fatalf("fence guard = %+v after release, want none", p)
	}
}

func TestRestoreOfflineFenceGuard(t *testing.T) {
	no := false
	for _, tc := range []struct {
		name  string
		phase v1alpha1.RestoreInPlacePhase
		queue v1alpha1.RestoreReclonesPhase
		prior *bool
		want  string
	}{
		{"running restore", v1alpha1.RestoreInPlaceRestoring, v1alpha1.RestoreReclonesArmed, &no, "iad"},
		{"failed, queue still Armed (settling)", v1alpha1.RestoreInPlaceFailed, v1alpha1.RestoreReclonesArmed, &no, "iad"},
		{"failed, queue settled Held", v1alpha1.RestoreInPlaceFailed, v1alpha1.RestoreReclonesHeld, &no, ""},
		{"succeeded, Draining", v1alpha1.RestoreInPlaceSucceeded, v1alpha1.RestoreReclonesDraining, &no, ""},
		{"prior never recorded", v1alpha1.RestoreInPlaceFailed, v1alpha1.RestoreReclonesArmed, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgThreeSiteRestore()
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: tc.phase, TargetSite: "iad", Scope: "full"}
			fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{Phase: tc.queue, Pending: []string{"pdx"}}
			fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "iad", ConfirmToken: testRestoreConfirm, PriorOfflineMode: tc.prior}
			if got, _ := restoreOfflineFenceGuard(fg); got != tc.want {
				t.Fatalf("guard = %q, want %q", got, tc.want)
			}
		})
	}
}

// A confirm bump while the previous queue is still settling does not start
// the new run, and does not stop the rest of the reconcile pass either: it
// emits one waiting Event and later steps still run.
func TestConfirmBumpDuringSettlement_DoesNotStallReconcile(t *testing.T) {
	const confirmB = "2026-04-18T10:00:00Z"
	fg := jobLostFG(time.Now())
	fg.Spec.RestoreInPlace.Confirm = confirmB
	fg.SetAnnotations(map[string]string{PlannedFailoverAnnotation: "pdx"})
	r, c := newReconciler(fg, dsnSecret(), restorePod(corev1.PodRunning))
	r.Recorder = record.NewFakeRecorder(200)
	_, mocks := withFenceRunner(r, c, fg)
	mocks["iad"].offlineMode, mocks["iad"].gtidExecuted = true, peerGTID("1-10")

	var events []string
	for i := 0; i < 2; i++ {
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: fgNN(fg)})
		if err != nil {
			t.Fatal(err)
		}
		if res.RequeueAfter <= 0 {
			t.Fatalf("pass %d: no requeue while the previous queue settles", i)
		}
		events = append(events, drainEvents(r.Recorder.(*record.FakeRecorder))...)
	}
	fresh := getFG(t, c, fg)
	if rip := fresh.Status.RestoreInPlace; rip.Phase != v1alpha1.RestoreInPlaceFailed || rip.RunConfirm != "" {
		t.Fatalf("restore = %+v, want the previous run still shown; the new one not started", rip)
	}
	if q := fresh.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed || fresh.Status.RestoreOfflineFence == nil {
		t.Fatalf("queue %+v fence %+v, want Armed with the fence up while the pod lives", q, fresh.Status.RestoreOfflineFence)
	}
	if fresh.Status.PlannedFailover == nil {
		t.Fatal("the planned-failover step did not run: the pass stopped at the restore")
	}
	if n := strings.Count(strings.Join(events, "\n"), "RestoreInPlaceWaitingForPreviousQueue"); n != 1 {
		t.Fatalf("waiting events = %d, want exactly one (%q)", n, events)
	}
}
