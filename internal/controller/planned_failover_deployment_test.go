package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

// A standing deployment failover-hold defers a request (keeping its
// annotation) and keeps the run's start time across re-stamps; once the
// hold is gone the same run proceeds to admission.
func TestDeploymentHoldDefersRequest(t *testing.T) {
	ctx := context.Background()
	m, fg, now := deploymentLeaseFixture(t)
	grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	hold := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 30)
	fg.Annotations = map[string]string{PlannedFailoverAnnotation: "pdx:maxLagWait=45s"}
	if err := m.client.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
	runner.SetDeploymentLeases(m)
	r := &MysqlFailoverGroupReconciler{Client: m.client, Runner: runner, Recorder: record.NewFakeRecorder(100)}
	r.PlannedTopology = newPFFakeTopology()
	r.plannedClock = clock.NewFakeClock(*now)

	if delay, err := r.reconcilePlannedFailover(ctx, fetchFG(t, r, fgNN(fg))); err != nil || delay <= 0 {
		t.Fatalf("reconcile: delay=%s, error=%v", delay, err)
	}
	fresh := fetchFG(t, r, fgNN(fg))
	pf := fresh.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Admission == nil ||
		pf.Admission.DeferReason != v1alpha1.PlannedFailoverDeferDeploymentHold || pf.RunID == "" {
		t.Fatalf("expected deployment deferral, got %+v", pf)
	}
	if pf.Admission.RetryTime == nil || !pf.Admission.RetryTime.Time.Equal(hold.ExpiresAt.UTC().Truncate(time.Second)) {
		t.Fatalf("retryTime = %v, want %s", pf.Admission.RetryTime, hold.ExpiresAt)
	}
	if !pf.StartTime.Time.Equal(now.Truncate(time.Second)) {
		t.Fatalf("startTime = %v, want %s", pf.StartTime, *now)
	}
	// The deferral is recorded in status only: the user's annotation is
	// kept byte for byte, never rewritten (A-50 item 4).
	if v := fresh.Annotations[PlannedFailoverAnnotation]; v != "pdx:maxLagWait=45s" {
		t.Fatalf("request annotation = %q, want it unchanged", v)
	}
	req, err := parsePlannedFailoverAnnotation(fresh.Annotations[PlannedFailoverAnnotation])
	if err != nil || req.Site != "pdx" || req.MaxLagWait != 45*time.Second {
		t.Fatalf("request not retained: %+v, %v", req, err)
	}
	runID, start := pf.RunID, pf.StartTime.DeepCopy()

	// A re-stamp under the same hold changes nothing.
	if _, err := r.reconcilePlannedFailover(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if again := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; again.Revision != pf.Revision || !again.StartTime.Equal(start) {
		t.Errorf("unchanged deferral rewritten: %+v", again)
	}

	// The hold ends: the same run is admitted with its start time.
	*now = hold.ExpiresAt.Add(time.Second)
	r.plannedClock.(*clock.FakeClock).Advance(now.Sub(r.plannedClock.Now()))
	if _, err := r.reconcilePlannedFailover(ctx, fetchFG(t, r, fgNN(fg))); err != nil {
		t.Fatal(err)
	}
	got := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if got.RunID != runID || got.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || !got.StartTime.Equal(start) {
		t.Fatalf("after the hold = %+v, want run %s in Admitting with its start time", got, runID)
	}
}
