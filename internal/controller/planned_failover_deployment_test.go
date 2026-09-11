package controller

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func TestDeploymentHoldStartTime(t *testing.T) {
	for _, tc := range []struct {
		name         string
		phase        v1alpha1.PlannedFailoverPhase
		reason       string
		missingStart bool
		preserve     bool
	}{
		{name: "nil status"},
		{name: "succeeded", phase: v1alpha1.PlannedFailoverPhaseSucceeded},
		{name: "failed", phase: v1alpha1.PlannedFailoverPhaseFailed, reason: "LagTimeout"},
		{name: "cancelled", phase: v1alpha1.PlannedFailoverPhaseFailed, reason: "Cancelled"},
		{name: "skipped", phase: v1alpha1.PlannedFailoverPhaseFailed, reason: "AlreadyActive"},
		{name: "deferred", phase: v1alpha1.PlannedFailoverPhaseDeferred, reason: "DeploymentHold", preserve: true},
		{name: "pending", phase: v1alpha1.PlannedFailoverPhasePending, preserve: true},
		{name: "validating", phase: v1alpha1.PlannedFailoverPhaseValidating, preserve: true},
		{name: "missing start", phase: v1alpha1.PlannedFailoverPhaseDeferred, missingStart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m, fg, now := deploymentLeaseFixture(t)
			grantDeploymentLease(t, m, fg, "migration", "op1", 120)
			hold := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 30)
			oldStart := metav1.NewTime(now.Add(-15 * time.Minute))
			if tc.phase != "" {
				fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
					Phase: tc.phase, Reason: tc.reason, Target: "pdx", SourcePrimary: "iad",
					StartTime: &oldStart, MaxLagWait: &metav1.Duration{Duration: 45 * time.Second},
				}
				if tc.missingStart {
					fg.Status.PlannedFailover.StartTime = nil
				}
				if plannedFailoverTerminal(fg.Status.PlannedFailover) {
					completed := metav1.NewTime(oldStart.Add(time.Minute))
					fg.Status.PlannedFailover.CompletionTime = &completed
				}
				if err := m.client.Status().Update(ctx, fg); err != nil {
					t.Fatal(err)
				}
			}
			if !plannedFailoverInFlight(fg.Status.PlannedFailover) {
				fg.Annotations = map[string]string{PlannedFailoverAnnotation: "pdx:maxLagWait=45s"}
				if err := m.client.Update(ctx, fg); err != nil {
					t.Fatal(err)
				}
			}
			runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
			runner.SetDeploymentLeases(m)
			r := &MysqlFailoverGroupReconciler{Client: m.client, Runner: runner, Recorder: record.NewFakeRecorder(100)}
			if delay, err := r.reconcilePlannedFailover(ctx, fg); err != nil || delay <= 0 {
				t.Fatalf("reconcile: delay=%s, error=%v", delay, err)
			}
			fresh := fetchFG(t, r, fgNN(fg))
			pf := fresh.Status.PlannedFailover
			if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Reason != "DeploymentHold" {
				t.Fatalf("expected deployment deferral, got %+v", pf)
			}
			wantStart := *now
			if tc.preserve {
				wantStart = oldStart.Time
			}
			if pf.StartTime == nil || !pf.StartTime.Time.Equal(wantStart) {
				t.Fatalf("startTime = %v, want %s", pf.StartTime, wantStart)
			}
			if pf.CompletionTime != nil || pf.RetryAfter == nil || !pf.RetryAfter.Time.Equal(hold.ExpiresAt) {
				t.Fatalf("incorrect deferred timing: %+v", pf)
			}
			req, err := parsePlannedFailoverAnnotation(fresh.Annotations[PlannedFailoverAnnotation])
			if err != nil || req.Site != "pdx" || req.MaxLagWait != 45*time.Second {
				t.Fatalf("request not retained/restored: %+v, %v", req, err)
			}
		})
	}
}
