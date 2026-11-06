package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func requestFixture() *v1alpha1.MysqlFailoverGroup {
	fg := waitFixture("", v1alpha1.PlannedFailoverPhaseSucceeded)
	fg.Status.PlannedFailover.RunID = "run-0"
	fg.Spec.Sites = []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}
	return fg
}

func runWait(t *testing.T, cl client.Client, b plannedWaitBaseline) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := waitForPlannedFailover(&out, &errOut, cl, "ns", "lion", 5*time.Second, b)
	return out.String(), errOut.String(), err
}

// K020: a Deferred run re-reads the annotation when it resumes, so a
// request to another site is carried by that run (same runID, retargeted).
func TestWaitForPlannedFailover_FollowsDeferredRunForAnotherSite(t *testing.T) {
	fastPlannedWait(t)
	ctx := context.Background()

	fg := baselineFixture(v1alpha1.PlannedFailoverPhaseDeferred, "")
	fg.Status.PlannedFailover.TargetSite = "iad"
	cl := waitClient(t, fg).Build()
	b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
	if !b.follow || b.deferredFor != "iad" {
		t.Fatalf("baseline = %+v; want to follow the Deferred run deferred for iad", b)
	}

	t.Run("still deferred", func(t *testing.T) {
		out, _, err := runWait(t, cl, b)
		if err == nil || !strings.Contains(err.Error(), "deferred") {
			t.Fatalf("err = %v; want the deferred exit", err)
		}
		if !strings.Contains(out, `replaces the target of deferred planned failover run-0 (was "iad")`) {
			t.Errorf("stdout = %q", out)
		}
	})

	t.Run("resumed retargeted", func(t *testing.T) {
		setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-0", Revision: 7, Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
			TargetSite: "pdx", StartTime: metav1.Now()})
		patchAnnotation(t, cl, false)
		if _, _, err := runWait(t, cl, b); err != nil {
			t.Fatalf("err = %v; want the resumed run's success", err)
		}
	})
}

// K021: a run admitted for another site's request is not this request's
// result, even though its runID is new.
func TestWaitForPlannedFailover_IgnoresRunForAnotherSite(t *testing.T) {
	fastPlannedWait(t)
	ctx := context.Background()
	fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
	cl := waitClient(t, fg).Build()
	b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
	setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
		TargetSite: "iad", StartTime: metav1.Now()})
	patchAnnotation(t, cl, false)
	if err := cl.Create(ctx, refusalEvent("ev-stale", v1alpha1.EventPlannedFailoverRejected,
		`planned-failover annotation ignored: a previous planned failover is still running (phase="Fencing")`, 1)); err != nil {
		t.Fatal(err)
	}
	out, _, err := runWait(t, cl, b)
	if err == nil || !strings.Contains(err.Error(), "previous planned failover is still running") {
		t.Fatalf("err = %v (stdout %q); want this request's refusal, not run-1's success", err, out)
	}
	if strings.Contains(out, "run-1") {
		t.Errorf("stdout = %q; adopted the other site's run", out)
	}

	t.Run("no Event names the other run", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
		cl := waitClient(t, fg).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseAdmitting,
			TargetSite: "iad", StartTime: metav1.Now()})
		_, _, err := runWait(t, cl, b)
		if err == nil || !strings.Contains(err.Error(), `planned failover run-1 runs for site "iad"`) {
			t.Fatalf("err = %v", err)
		}
	})
}

// K021: the request is written conditioned on the resourceVersion the
// baseline was read at; a change in between (here a run admitted for
// another request) re-reads and re-baselines.
func TestAnnotatePlannedFailover_ConflictRebaselines(t *testing.T) {
	ctx := context.Background()
	fg := requestFixture()
	base := waitClient(t, fg).Build()
	patches := 0
	cl := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patches++
			if patches == 1 {
				// A concurrent request is admitted between the read and the write.
				setRun(t, c, &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseAdmitting,
					TargetSite: "iad", StartTime: metav1.Now()})
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	b, err := annotatePlannedFailover(ctx, cl, "ns", "lion", "pdx", "pdx", true)
	if err != nil {
		t.Fatal(err)
	}
	if patches != 2 {
		t.Fatalf("patches = %d; want a conflict and one retry", patches)
	}
	if b.runID != "run-1" || b.follow {
		t.Fatalf("baseline = %+v; want run-1 (another site's Admitting run, not followed)", b)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := cl.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[PlannedFailoverAnnotation] != "pdx" {
		t.Fatalf("annotations = %v", got.Annotations)
	}
}

// K021 follow variant: a followed run that ends while the request is still
// set did not absorb it; the wait reports the new run the request starts.
func TestWaitForPlannedFailover_FollowedRunEndsWithoutRequest(t *testing.T) {
	fastPlannedWait(t)
	ctx := context.Background()
	fg := baselineFixture(v1alpha1.PlannedFailoverPhaseAdmitting, "")
	cl := waitClient(t, fg).Build()
	b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
	if !b.follow {
		t.Fatal("the Admitting run to the same site is not followed")
	}
	patchAnnotation(t, cl, true)
	setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-0", Revision: 3, Phase: v1alpha1.PlannedFailoverPhaseFailed,
		TargetSite: "pdx", StartTime: metav1.Now(), Result: &v1alpha1.PlannedFailoverResult{
			Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonAdmissionTimeout}})
	go func() {
		time.Sleep(150 * time.Millisecond)
		setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 5, Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
			TargetSite: "pdx", StartTime: metav1.Now()})
		patchAnnotation(t, cl, false)
	}()
	out, _, err := runWait(t, cl, b)
	if err != nil {
		t.Fatalf("err = %v (stdout %q); want run-1's success, not run-0's AdmissionTimeout", err, out)
	}
	if !strings.Contains(out, "Request admitted as planned failover run-1") {
		t.Errorf("stdout = %q", out)
	}

	t.Run("the ending run consumed the request", func(t *testing.T) {
		fg := baselineFixture(v1alpha1.PlannedFailoverPhaseDeferred, "")
		cl := waitClient(t, fg).Build()
		b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
		setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-0", Revision: 3, Phase: v1alpha1.PlannedFailoverPhaseFailed,
			TargetSite: "pdx", StartTime: metav1.Now(), Result: &v1alpha1.PlannedFailoverResult{
				Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonTargetUnhealthy}})
		patchAnnotation(t, cl, false)
		_, _, err := runWait(t, cl, b)
		if err == nil || !strings.Contains(err.Error(), "TargetUnhealthy") {
			t.Fatalf("err = %v; want the followed run's TargetUnhealthy", err)
		}
	})
}

// K062: an operator older than the plugin writes no runID; the wait
// follows the run it started by its start time.
func TestWaitForPlannedFailover_LegacyOperator(t *testing.T) {
	fastPlannedWait(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		phase   v1alpha1.PlannedFailoverPhase
		wantErr string
	}{
		{name: "succeeded", phase: v1alpha1.PlannedFailoverPhaseSucceeded},
		{name: "failed", phase: v1alpha1.PlannedFailoverPhaseFailed, wantErr: "lag wait exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := requestFixture()
			fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
				StartTime: metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))}
			cl := waitClient(t, fg).Build()
			b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
			patchAnnotation(t, cl, true)
			go func() {
				time.Sleep(100 * time.Millisecond)
				setRun(t, cl, &v1alpha1.PlannedFailoverStatus{Phase: "Pending", StartTime: metav1.NewTime(time.Now().Truncate(time.Second))})
				patchAnnotation(t, cl, false)
				time.Sleep(100 * time.Millisecond)
				setRun(t, cl, &v1alpha1.PlannedFailoverStatus{Phase: tc.phase, Message: "lag wait exceeded",
					StartTime: metav1.NewTime(time.Now().Truncate(time.Second))})
			}()
			out, errOut, err := runWait(t, cl, b)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("err = %v (stdout %q)", err, out)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("err = %v; want %q", err, tc.wantErr)
			}
			if !strings.Contains(errOut, "older than this plugin") {
				t.Errorf("stderr = %q", errOut)
			}
		})
	}

	// A runID-less block whose start time did not change is the baseline's,
	// not a new legacy run.
	fg := requestFixture()
	start := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseSucceeded, StartTime: start}
	b := newPlannedWaitBaseline(ctx, waitClient(t, fg).Build(), fg, "pdx")
	if b.legacyRun(&v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseFailed, StartTime: start}) {
		t.Fatal("the baseline block counted as a new legacy run")
	}
}

// K092: a run that ended NotStarted because its target became the active
// primary is reported like a skipped request (exit 0).
func TestWaitForPlannedFailover_AlreadyActiveIsSkip(t *testing.T) {
	fastPlannedWait(t)
	ctx := context.Background()
	fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
	cl := waitClient(t, fg).Build()
	b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
	setRun(t, cl, &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 2, Phase: v1alpha1.PlannedFailoverPhaseFailed,
		TargetSite: "pdx", StartTime: metav1.Now(), Result: &v1alpha1.PlannedFailoverResult{
			Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonAlreadyActive}})
	out, _, err := runWait(t, cl, b)
	if err != nil {
		t.Fatalf("err = %v; want exit 0 like a Skipped request", err)
	}
	if !strings.Contains(out, `Planned failover skipped: site "pdx" became the active primary`) {
		t.Errorf("stdout = %q", out)
	}
}

// K116: an Event list the caller may not perform is reported, not hidden
// behind "no Event was found".
func TestWaitForPlannedFailover_EventListForbidden(t *testing.T) {
	fastPlannedWait(t)
	ctx := context.Background()
	fg := baselineFixture(v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverOutcomeSucceeded)
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", nil)
	cl := interceptor.NewClient(waitClient(t, fg).Build(), interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return forbidden
		},
	})
	b := newPlannedWaitBaseline(ctx, cl, fg, "pdx")
	if b.eventsErr == nil {
		t.Fatal("the Event list error was dropped")
	}
	_, _, err := runWait(t, cl, b)
	if err == nil || !strings.Contains(err.Error(), "could not be read") || !apierrors.IsForbidden(errorsUnwrapAll(err)) {
		t.Fatalf("err = %v; want the Forbidden list error", err)
	}
}

func errorsUnwrapAll(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok || u.Unwrap() == nil {
			return err
		}
		err = u.Unwrap()
	}
}
