package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
)

func fastPlannedWait(t *testing.T) {
	t.Helper()
	poll, ev := plannedWaitPoll, refusalEventWait
	plannedWaitPoll, refusalEventWait = 20*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { plannedWaitPoll, refusalEventWait = poll, ev })
}

// baselineFixture is a group whose last run (run-0) ended with outcome,
// already carrying the new request annotation for pdx.
func baselineFixture(phase v1alpha1.PlannedFailoverPhase, outcome v1alpha1.PlannedFailoverOutcome) *v1alpha1.MysqlFailoverGroup {
	fg := waitFixture("", phase)
	fg.Status.PlannedFailover.RunID = "run-0"
	if outcome != "" {
		fg.Status.PlannedFailover.Result = &v1alpha1.PlannedFailoverResult{Outcome: outcome, CompletionTime: metav1.Now()}
	}
	return fg
}

// patchAnnotation and setRun also run from goroutines, so they report with
// t.Error, never t.Fatal.
func patchAnnotation(t *testing.T, cl client.Client, set bool) {
	t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "lion"}, &fg); err != nil {
		t.Error(err)
		return
	}
	if set {
		if fg.Annotations == nil {
			fg.Annotations = map[string]string{}
		}
		fg.Annotations[PlannedFailoverAnnotation] = "pdx"
	} else {
		delete(fg.Annotations, PlannedFailoverAnnotation)
	}
	if err := cl.Update(context.Background(), &fg); err != nil {
		t.Error(err)
	}
}

func setRun(t *testing.T, cl client.Client, pf *v1alpha1.PlannedFailoverStatus) {
	t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "lion"}, &fg); err != nil {
		t.Error(err)
		return
	}
	fg.Status.PlannedFailover = pf
	if err := cl.Status().Update(context.Background(), &fg); err != nil {
		t.Error(err)
	}
}

func refusalEvent(name, reason, msg string, count int32) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID(name)},
		InvolvedObject: corev1.ObjectReference{Kind: "MysqlFailoverGroup", Namespace: "ns", Name: "lion"},
		Reason:         reason, Message: msg, Count: count, Type: corev1.EventTypeWarning,
		LastTimestamp: metav1.Now(),
	}
}

// O-m3: --wait ignores the run that was in status before the request and
// reports this request's own result.
func TestWaitForPlannedFailover_IgnoresPreviousRun(t *testing.T) {
	fastPlannedWait(t)
	ctx := context.Background()

	t.Run("previous success, new run refused", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
		cl := waitClient(t, fg).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		patchAnnotation(t, cl, true)
		go func() {
			time.Sleep(100 * time.Millisecond)
			setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseFailed,
				TargetSite: "pdx", StartTime: metav1.Now(), Result: &v1alpha1.PlannedFailoverResult{
					Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonCooldownActive,
					CompletionTime: metav1.Now()}})
		}()
		var out, errOut bytes.Buffer
		err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, b)
		if err == nil || !strings.Contains(err.Error(), "CooldownActive") {
			t.Fatalf("err = %v (stdout %q), want this request's CooldownActive refusal", err, out.String())
		}
		if !strings.Contains(out.String(), "Request admitted as planned failover run-1") {
			t.Errorf("stdout = %q", out.String())
		}
	})

	t.Run("previous failure, new run succeeds", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseFailed, v1alpha1.PlannedFailoverOutcomeRolledBack)
		cl := waitClient(t, fg).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		patchAnnotation(t, cl, true)
		go func() {
			time.Sleep(100 * time.Millisecond)
			setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
				TargetSite: "pdx", StartTime: metav1.Now()})
		}()
		var out, errOut bytes.Buffer
		if err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, b); err != nil {
			t.Fatalf("err = %v, want success of the new run", err)
		}
	})

	// The previous run's terminal marker is not this request's result.
	t.Run("previous marker ignored", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhasePromoting, "")
		fg.Annotations = map[string]string{brcontroller.PlannedFailoverTerminalAnnotation: `{"schemaVersion":1,"runID":"run-0","outcome":"Succeeded"}`}
		cl := waitClient(t, fg).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		patchAnnotation(t, cl, true)
		var out, errOut bytes.Buffer
		err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 300*time.Millisecond, b)
		if err == nil || !strings.Contains(err.Error(), "has not admitted the request") {
			t.Fatalf("err = %v, want a not-admitted timeout", err)
		}
	})

	t.Run("refused with an Event only", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
		old := refusalEvent("ev-old", v1alpha1.EventPlannedFailoverRejected, "an old refusal", 1)
		cl := waitClient(t, fg).WithObjects(old).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		patchAnnotation(t, cl, true)
		go func() {
			time.Sleep(100 * time.Millisecond)
			patchAnnotation(t, cl, false)
			time.Sleep(50 * time.Millisecond) // the Event lands after the annotation is gone
			if err := cl.Create(ctx, refusalEvent("ev-new", v1alpha1.EventPlannedFailoverRejected, "planned failover refused: automation is held", 1)); err != nil {
				t.Error(err)
			}
		}()
		var out, errOut bytes.Buffer
		err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, b)
		if err == nil || !strings.Contains(err.Error(), "automation is held") {
			t.Fatalf("err = %v, want the new refusal Event", err)
		}
	})

	// An aggregated Event whose count grew is a new refusal.
	t.Run("aggregated Event", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
		ev := refusalEvent("ev-1", v1alpha1.EventPlannedFailoverSkipped, `site "pdx" is already the active primary`, 1)
		cl := waitClient(t, fg).WithObjects(ev).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		patchAnnotation(t, cl, true)
		patchAnnotation(t, cl, false)
		var stored corev1.Event
		if err := cl.Get(ctx, client.ObjectKeyFromObject(ev), &stored); err != nil {
			t.Fatal(err)
		}
		stored.Count = 2
		if err := cl.Update(ctx, &stored); err != nil {
			t.Fatal(err)
		}
		var out, errOut bytes.Buffer
		if err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, b); err != nil {
			t.Fatalf("err = %v, want a skip (exit 0)", err)
		}
		if !strings.Contains(out.String(), "Planned failover skipped") {
			t.Errorf("stdout = %q", out.String())
		}
	})

	t.Run("consumed without an Event", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
		cl := waitClient(t, fg).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		var out, errOut bytes.Buffer
		err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, b)
		if err == nil || !strings.Contains(err.Error(), "without starting a run") {
			t.Fatalf("err = %v, want a consumed-without-run error", err)
		}
	})

	// A pending run to the same site absorbs the request: follow it.
	t.Run("joins a pending run", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseAdmitting, "")
		cl := waitClient(t, fg).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		if !b.follow {
			t.Fatal("an Admitting run to the same site is not followed")
		}
		if newPlannedWaitBaseline(ctx, cl, fg, "iad").follow {
			t.Fatal("an Admitting run to another site is followed")
		}
		go func() {
			time.Sleep(100 * time.Millisecond)
			setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-0", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
				TargetSite: "pdx", StartTime: metav1.Now()})
		}()
		var out, errOut bytes.Buffer
		if err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, b); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
}
