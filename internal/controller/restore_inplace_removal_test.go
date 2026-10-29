package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const previousRestoreConfirm = "2026-04-16T09:00:00Z"

// The guards follow the run the operator accepted (status), not the
// latest request (spec): removing or editing spec.restoreInPlace while a
// destructive Job may be running must not unfreeze the topology, unfence
// the primary Service, or reopen tenant DDL.
func TestInPlaceRestoreGuards_FollowAcceptedRun(t *testing.T) {
	full := &v1alpha1.RestoreInPlaceSpec{Confirm: testRestoreConfirm}
	perSchema := &v1alpha1.RestoreInPlaceSpec{Confirm: testRestoreConfirm, LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{"orders"}}}
	for _, tc := range []struct {
		name                    string
		spec                    *v1alpha1.RestoreInPlaceSpec
		status                  *v1alpha1.RestoreInPlaceStatus
		inFlight, frozen, fence bool
	}{
		{name: "spec removed mid full restore", status: &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring, Scope: "full"},
			inFlight: true, frozen: true, fence: true},
		{name: "spec removed mid per-schema restore", status: &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring, Scope: "schema:orders"},
			inFlight: true, frozen: true},
		{name: "spec removed while resuming", status: &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceResuming, Scope: "full"},
			inFlight: true, frozen: true, fence: true},
		{name: "scope edited to per-schema mid full restore", spec: perSchema,
			status:   &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring, Scope: "full"},
			inFlight: true, frozen: true, fence: true},
		{name: "scope edited to full mid per-schema restore", spec: full,
			status:   &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring, Scope: "schema:orders"},
			inFlight: true, frozen: true},
		{name: "spec removed after success", status: &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceSucceeded, Scope: "full"}},
		{name: "nothing requested"},
		{name: "requested, not yet observed", spec: full, inFlight: true, fence: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := &v1alpha1.MysqlFailoverGroup{}
			fg.Spec.RestoreInPlace = tc.spec
			fg.Status.RestoreInPlace = tc.status
			if got := inPlaceRestoreInFlight(fg); got != tc.inFlight {
				t.Errorf("inPlaceRestoreInFlight = %v, want %v", got, tc.inFlight)
			}
			if got := inPlaceRestoreFreezesTopology(fg); got != tc.frozen {
				t.Errorf("inPlaceRestoreFreezesTopology = %v, want %v", got, tc.frozen)
			}
			if got := inPlaceRestoreFencesPrimaryService(fg); got != tc.fence {
				t.Errorf("inPlaceRestoreFencesPrimaryService = %v, want %v", got, tc.fence)
			}
			if _, _, fenced := groupFenced(fg); fenced != tc.inFlight {
				t.Errorf("groupFenced = %v, want %v", fenced, tc.inFlight)
			}
		})
	}
}

func runningInPlaceJob(fg *v1alpha1.MysqlFailoverGroup, site, confirm string, created time.Time) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace,
		Annotations:       map[string]string{restoreInPlaceConfirmAnnotation: confirm},
		CreationTimestamp: metav1.NewTime(created),
	}}
}

func completeJob(t *testing.T, r *MysqlFailoverGroupReconciler, job *batchv1.Job) {
	t.Helper()
	var fresh batchv1.Job
	if err := r.Get(context.Background(), types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &fresh); err != nil {
		t.Fatal(err)
	}
	fresh.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := r.Status().Update(context.Background(), &fresh); err != nil {
		t.Fatal(err)
	}
}

// Removing spec.restoreInPlace while its Job runs keeps every guard until
// the Job finishes, then completes the run normally (Resuming, Succeeded,
// peer reclone queue released) and keeps the terminal status so the old
// manifest cannot run the restore again.
func TestReconcileInPlaceRestore_SpecRemovedMidJobHoldsUntilJobFinishes(t *testing.T) {
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	start := time.Now().Add(-time.Minute)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "full",
		JobName: inPlaceRestoreJobName(fg.Name, site), StartTime: ptrTime(start),
		ConfirmTokenUsed: previousRestoreConfirm,
	}
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx"},
	}
	job := runningInPlaceJob(fg, site, testRestoreConfirm, start.Add(5*time.Second))
	pod := primaryFencedPod(fg.Name, site)
	fg.Spec.RestoreInPlace = nil
	r, c := newReconciler(fg, job, pod, dsnSecret())
	rec := r.Recorder.(*record.FakeRecorder)

	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatal(err)
	}
	if d <= 0 {
		t.Errorf("want a requeue while the Job runs, got %s", d)
	}
	fresh := getFG(t, c, fg)
	st := fresh.Status.RestoreInPlace
	if st == nil || st.Phase != v1alpha1.RestoreInPlaceRestoring {
		t.Fatalf("status = %+v, want Restoring held", st)
	}
	if !strings.Contains(st.Message, "spec.restoreInPlace was removed") {
		t.Errorf("status message should say why the fence holds, got %q", st.Message)
	}
	if !inPlaceRestoreFreezesTopology(fresh) || !inPlaceRestoreFencesPrimaryService(fresh) {
		t.Fatal("topology freeze and Service fence must hold while the Job runs")
	}
	if _, _, fenced := groupFenced(fresh); !fenced {
		t.Fatal("tenant DDL must stay blocked while the Job runs")
	}
	if err := r.syncPodLabels(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	var gotPod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &gotPod); err != nil {
		t.Fatal(err)
	}
	if gotPod.Labels[labelRole] != "fenced" {
		t.Fatalf("primary pod role = %q, want fenced while the Job runs", gotPod.Labels[labelRole])
	}
	if !strings.Contains(strings.Join(drainEvents(rec), "\n"), "RestoreInPlaceSpecRemoved") {
		t.Error("want a RestoreInPlaceSpecRemoved event")
	}
	if _, err := r.reconcileRestoreReclones(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
		t.Fatalf("the reclone queue must survive while its restore runs, got %+v", q)
	}

	completeJob(t, r, job)
	*fg = *getFG(t, c, fg)
	for i := 0; i < 2; i++ {
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatalf("reconcile %d after completion: %v", i, err)
		}
	}
	fresh = getFG(t, c, fg)
	st = fresh.Status.RestoreInPlace
	if st == nil || st.Phase != v1alpha1.RestoreInPlaceSucceeded || st.ConfirmTokenUsed != testRestoreConfirm {
		t.Fatalf("status = %+v, want Succeeded recording the Job's confirm %s", st, testRestoreConfirm)
	}
	if q := fresh.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesDraining {
		t.Fatalf("the finished restore must release its reclone queue, got %+v", q)
	}

	// The terminal status stays, so re-applying the old manifest is a no-op.
	if _, err := r.reconcileInPlaceRestore(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st == nil || st.ConfirmTokenUsed != testRestoreConfirm {
		t.Fatalf("terminal status must survive a spec-less reconcile, got %+v", st)
	}
	reapplied := getFG(t, c, fg)
	reapplied.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: testRestoreConfirm, Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"}}}
	if _, err := r.reconcileInPlaceRestore(context.Background(), reapplied); err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceSucceeded {
		t.Fatalf("re-applying the consumed confirm started a new run: phase %s", st.Phase)
	}
}

// A Job created just before a crash, before the status recorded its name,
// still belongs to the run: the spec-less reconcile adopts it instead of
// cancelling or deleting it.
func TestReconcileInPlaceRestore_SpecRemovedAdoptsJobFromCrashWindow(t *testing.T) {
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "full", StartTime: ptrTime(start),
	}
	job := runningInPlaceJob(fg, site, testRestoreConfirm, start.Add(5*time.Second))
	fg.Spec.RestoreInPlace = nil
	r, c := newReconciler(fg, job, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	st := getFG(t, c, fg).Status.RestoreInPlace
	if st == nil || st.Phase != v1alpha1.RestoreInPlaceRestoring || st.JobName != job.Name {
		t.Fatalf("status = %+v, want Restoring adopting Job %s", st, job.Name)
	}
	var still batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &still); err != nil {
		t.Fatalf("the run's Job must not be deleted: %v", err)
	}
}

// Before any restore Job exists nothing destructive has run, so removing
// the spec cancels the run. The cancellation keeps the last consumed
// confirm, so an older manifest still cannot re-run that earlier restore.
func TestReconcileInPlaceRestore_SpecRemovedBeforeJobCancels(t *testing.T) {
	for _, tc := range []struct {
		name     string
		phase    v1alpha1.RestoreInPlacePhase
		staleJob bool
	}{
		{name: "preflight", phase: v1alpha1.RestoreInPlacePreflight},
		{name: "fencing", phase: v1alpha1.RestoreInPlaceFencing},
		{name: "restoring, no job yet", phase: v1alpha1.RestoreInPlaceRestoring},
		{name: "restoring, only a job from an earlier run", phase: v1alpha1.RestoreInPlaceRestoring, staleJob: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("")
			site := fg.Spec.Sites[0].Name
			start := time.Now().Add(-time.Minute).Truncate(time.Second)
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
				Phase: tc.phase, TargetSite: site, Scope: "full", StartTime: ptrTime(start),
				ConfirmTokenUsed: previousRestoreConfirm,
			}
			fg.Spec.RestoreInPlace = nil
			r, c := newReconciler(fg, dsnSecret())
			if tc.staleJob {
				stale := runningInPlaceJob(fg, site, previousRestoreConfirm, start.Add(-time.Hour))
				stale.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
				if err := c.Create(context.Background(), stale); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			fresh := getFG(t, c, fg)
			st := fresh.Status.RestoreInPlace
			if st == nil || st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "cancelled") {
				t.Fatalf("status = %+v, want a cancelled terminal", st)
			}
			if st.ConfirmTokenUsed != previousRestoreConfirm {
				t.Fatalf("confirmTokenUsed = %q, want the last consumed %q kept", st.ConfirmTokenUsed, previousRestoreConfirm)
			}
			if inPlaceRestoreFreezesTopology(fresh) || inPlaceRestoreFencesPrimaryService(fresh) {
				t.Fatal("a cancelled run must release its guards")
			}
			if !tc.staleJob {
				var job batchv1.Job
				if err := c.Get(context.Background(), types.NamespacedName{Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}, &job); !apierrors.IsNotFound(err) {
					t.Fatalf("no Job may be created for a cancelled run, got err=%v", err)
				}
			}
			if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreInPlaceCancelled") {
				t.Error("want a RestoreInPlaceCancelled event")
			}

			// Re-applying the earlier, already executed confirm does nothing.
			old := getFG(t, c, fg)
			old.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: previousRestoreConfirm, Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"}}}
			if _, err := r.reconcileInPlaceRestore(context.Background(), old); err != nil {
				t.Fatal(err)
			}
			if got := getFG(t, c, fg).Status.RestoreInPlace.Phase; got != v1alpha1.RestoreInPlaceFailed {
				t.Fatalf("re-applying the consumed confirm started a run: phase %s", got)
			}
		})
	}
}

// A new run keeps the last consumed confirm in status until it consumes
// its own, and a validation rejection does too. Otherwise the earlier
// restore's manifest could run again after the new request goes away.
func TestReconcileInPlaceRestore_NewRunKeepsLastConsumedConfirm(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceSucceeded, Scope: "full", ConfirmTokenUsed: previousRestoreConfirm,
	}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	st := getFG(t, c, fg).Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlacePreflight || st.ConfirmTokenUsed != previousRestoreConfirm {
		t.Fatalf("status = %+v, want Preflight keeping confirmTokenUsed %s", st, previousRestoreConfirm)
	}

	rejected := fgInPlaceRestore("")
	rejected.Spec.RestoreInPlace.PointInTime = &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-17T13:00:00Z"}
	rejected.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceSucceeded, Scope: "full", ConfirmTokenUsed: previousRestoreConfirm,
	}
	r2, c2 := newReconciler(rejected, succeededSeedBackup(), dsnSecret())
	if _, err := r2.reconcileInPlaceRestore(context.Background(), rejected); err != nil {
		t.Fatal(err)
	}
	st = getFG(t, c2, rejected).Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlaceFailed || st.ConfirmTokenUsed != previousRestoreConfirm {
		t.Fatalf("rejection status = %+v, want Failed keeping confirmTokenUsed %s", st, previousRestoreConfirm)
	}
}

// Editing includeSchemas after the restore was accepted must not build a
// Job whose scope differs from the fence that was applied.
func TestReconcileInPlaceRestore_ScopeChangedBeforeJobIsRejected(t *testing.T) {
	fg := fgInPlaceRestore("orders") // the spec now asks for a per-schema restore
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "full", StartTime: ptrTime(time.Now()),
		ConfirmTokenUsed: previousRestoreConfirm,
	}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	st := getFG(t, c, fg).Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "scope changed") || st.ConfirmTokenUsed != previousRestoreConfirm {
		t.Fatalf("status = %+v, want a scope-change rejection that consumes nothing", st)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}, &job); !apierrors.IsNotFound(err) {
		t.Fatalf("no Job may be built for a changed scope, got err=%v", err)
	}
}

// A restore Job from an earlier request that is still running keeps the
// guards up even though the current run never started its own.
func TestReconcileInPlaceRestore_SpecRemovedWaitsForForeignRunningJob(t *testing.T) {
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "full", StartTime: ptrTime(start),
	}
	fg.Spec.RestoreInPlace = nil
	foreign := runningInPlaceJob(fg, site, previousRestoreConfirm, start.Add(-time.Hour))
	r, c := newReconciler(fg, foreign, dsnSecret())
	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatal(err)
	}
	fresh := getFG(t, c, fg)
	if d <= 0 || fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || !inPlaceRestoreFreezesTopology(fresh) {
		t.Fatalf("requeue %s, status %+v: the guards must hold while any restore Job runs", d, fresh.Status.RestoreInPlace)
	}
}

// A run whose Job disappears after the spec was removed may have run its
// destructive load, so JobLost consumes that run's confirm (carried from
// Preflight as runConfirm). Re-applying the removed manifest must not run
// the restore again.
func TestReconcileInPlaceRestore_JobLostConsumesTheRunsConfirm(t *testing.T) {
	for _, tc := range []struct {
		name  string
		prior *v1alpha1.RestoreInPlaceStatus
	}{
		{name: "after an earlier restore", prior: &v1alpha1.RestoreInPlaceStatus{
			Phase: v1alpha1.RestoreInPlaceSucceeded, Scope: "schema:orders", ConfirmTokenUsed: previousRestoreConfirm,
		}},
		{name: "first restore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("orders")
			fg.Status.RestoreInPlace = tc.prior
			manifest := fg.Spec.RestoreInPlace.DeepCopy()
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())

			for i := 0; i < 4 && (fg.Status.RestoreInPlace == nil || fg.Status.RestoreInPlace.JobName == ""); i++ {
				if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
					t.Fatalf("reconcile %d: %v", i, err)
				}
				*fg = *getFG(t, c, fg)
				if st := fg.Status.RestoreInPlace; !inPlaceRestoreTerminal(st) && st.RunConfirm != manifest.Confirm {
					t.Fatalf("phase %s: runConfirm = %q, want the run's confirm %q", st.Phase, st.RunConfirm, manifest.Confirm)
				}
			}
			st := fg.Status.RestoreInPlace
			if st.Phase != v1alpha1.RestoreInPlaceRestoring || st.JobName == "" {
				t.Fatalf("status = %+v, want Restoring with its Job created", st)
			}

			// The spec is removed and the Job is deleted before the run
			// sees how it ended.
			fg.Spec.RestoreInPlace = nil
			if err := c.Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: st.JobName, Namespace: fg.Namespace}}
			if err := c.Delete(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			*fg = *getFG(t, c, fg)
			if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			st = getFG(t, c, fg).Status.RestoreInPlace
			if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "outcome is unknown") {
				t.Fatalf("status = %+v, want JobLost", st)
			}
			if st.ConfirmTokenUsed != manifest.Confirm {
				t.Fatalf("confirmTokenUsed = %q, want the lost run's %q consumed", st.ConfirmTokenUsed, manifest.Confirm)
			}

			reapplied := getFG(t, c, fg)
			reapplied.Spec.RestoreInPlace = manifest
			if _, err := r.reconcileInPlaceRestore(context.Background(), reapplied); err != nil {
				t.Fatal(err)
			}
			if got := getFG(t, c, fg).Status.RestoreInPlace.Phase; got != v1alpha1.RestoreInPlaceFailed {
				t.Fatalf("re-applying the lost run's manifest started a new run: phase %s", got)
			}
		})
	}
}

// A confirm bumped before the Job exists re-authorizes the run: the Job
// is built from the current spec and runs under the new confirm, which is
// bound into status before Create. A JobLost then consumes that confirm,
// so re-applying the bumped manifest cannot run the restore again.
func TestReconcileInPlaceRestore_ConfirmBumpedBeforeJobIsBoundToTheJob(t *testing.T) {
	const bumped = "2026-04-18T10:00:00Z"
	fg := fgInPlaceRestore("orders")
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	step := func() {
		t.Helper()
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
		*fg = *getFG(t, c, fg)
	}
	for i := 0; i < 3 && (fg.Status.RestoreInPlace == nil || fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring); i++ {
		step()
	}
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceRestoring || st.JobName != "" {
		t.Fatalf("status = %+v, want Restoring before the Job", st)
	}

	fg.Spec.RestoreInPlace.Confirm = bumped
	if err := c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	step()
	if st := fg.Status.RestoreInPlace; st.RunConfirm != bumped || st.JobName != "" {
		t.Fatalf("status = %+v, want runConfirm bound to %s before the Job is created", st, bumped)
	}
	step()
	st := fg.Status.RestoreInPlace
	if st.JobName == "" || st.RunConfirm != bumped {
		t.Fatalf("status = %+v, want the Job created under %s", st, bumped)
	}
	job := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: st.JobName, Namespace: fg.Namespace}, job); err != nil {
		t.Fatal(err)
	}
	if got := inPlaceRestoreJobConfirm(job); got != bumped {
		t.Fatalf("Job confirm annotation = %q, want %q", got, bumped)
	}

	manifest := fg.Spec.RestoreInPlace.DeepCopy()
	fg.Spec.RestoreInPlace = nil
	if err := c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	step()
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || st.ConfirmTokenUsed != bumped {
		t.Fatalf("status = %+v, want JobLost consuming %s", st, bumped)
	}
	fg.Spec.RestoreInPlace = manifest
	if err := c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	step()
	if got := fg.Status.RestoreInPlace.Phase; got != v1alpha1.RestoreInPlaceFailed {
		t.Fatalf("re-applying the bumped manifest started a new run: phase %s", got)
	}
}

// A Job adopted from the crash window between Create and the status write
// carries the confirm it runs with; adoption records it, so a later
// JobLost consumes that confirm rather than the one from Preflight.
func TestReconcileInPlaceRestore_AdoptedJobRecordsItsConfirm(t *testing.T) {
	const bumped = "2026-04-18T10:00:00Z"
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "full", StartTime: ptrTime(start),
		RunConfirm: testRestoreConfirm,
	}
	job := runningInPlaceJob(fg, site, bumped, start.Add(5*time.Second))
	fg.Spec.RestoreInPlace = nil
	r, c := newReconciler(fg, job, dsnSecret())
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.JobName != job.Name || st.RunConfirm != bumped {
		t.Fatalf("status = %+v, want Job %s adopted with runConfirm %s", st, job.Name, bumped)
	}
	if err := c.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || st.ConfirmTokenUsed != bumped {
		t.Fatalf("status = %+v, want JobLost consuming the adopted Job's %s", st, bumped)
	}
}

func TestRebindRestoreReclones(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx"}}
	r, c := newReconciler(fg)
	if err := r.rebindRestoreReclones(context.Background(), fg, testRestoreConfirm, "2026-04-18T10:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q.ConfirmToken != "2026-04-18T10:00:00Z" {
		t.Fatalf("queue confirm = %q, want rebound", q.ConfirmToken)
	}
}

// failStatusWrites makes r's status updates fail while fail reports true
// for the object being written.
func failStatusWrites(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fail func(*v1alpha1.MysqlFailoverGroup) bool) {
	t.Helper()
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if fg, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && fail(fg) {
				return errors.New("injected status write failure")
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

// driveToJobCreate runs a per-schema restore up to Restoring before its
// Job exists.
func driveToJobCreate(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	for i := 0; i < 3 && (fg.Status.RestoreInPlace == nil || fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring); i++ {
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
		*fg = *getFG(t, c, fg)
	}
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceRestoring || st.JobName != "" {
		t.Fatalf("status = %+v, want Restoring before the Job", st)
	}
}

// The Job is created but the status write that records its name is lost
// (or the operator crashes right after Create). The write-ahead
// jobCreating, made before Create, still shows that the run may have
// executed: when the spec and the Job then disappear the run ends JobLost
// and consumes the Job's confirm, so re-applying the manifest cannot run
// the restore again.
func TestReconcileInPlaceRestore_LostPostCreateWriteStillCountsAsExecuted(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	manifest := fg.Spec.RestoreInPlace.DeepCopy()
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	driveToJobCreate(t, r, c, fg)

	failStatusWrites(t, r, c, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.RestoreInPlace != nil && fg.Status.RestoreInPlace.JobName != ""
	})
	_, _ = r.reconcileInPlaceRestore(context.Background(), fg)
	r.Client = c
	*fg = *getFG(t, c, fg)
	st := fg.Status.RestoreInPlace
	jobKey := types.NamespacedName{Name: inPlaceRestoreJobName(fg.Name, fg.Spec.Sites[0].Name), Namespace: fg.Namespace}
	var job batchv1.Job
	if err := c.Get(context.Background(), jobKey, &job); err != nil {
		t.Fatalf("the Job should have been created: %v", err)
	}
	if st.JobName != "" || !st.JobCreating || st.RunConfirm != manifest.Confirm {
		t.Fatalf("status = %+v, want no JobName but jobCreating with runConfirm %s", st, manifest.Confirm)
	}

	fg.Spec.RestoreInPlace = nil
	if err := c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	st = getFG(t, c, fg).Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "outcome is unknown") || st.ConfirmTokenUsed != manifest.Confirm {
		t.Fatalf("status = %+v, want JobLost consuming %s", st, manifest.Confirm)
	}
	reapplied := getFG(t, c, fg)
	reapplied.Spec.RestoreInPlace = manifest
	if err := c.Update(context.Background(), reapplied); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if got := getFG(t, c, fg).Status.RestoreInPlace.Phase; got != v1alpha1.RestoreInPlaceFailed {
		t.Fatalf("re-applying the manifest of a Job that was created started a new run: phase %s", got)
	}
}

// No Job is created unless the write-ahead succeeded.
func TestReconcileInPlaceRestore_NoJobWithoutWriteAhead(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	driveToJobCreate(t, r, c, fg)
	failStatusWrites(t, r, c, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.RestoreInPlace != nil && fg.Status.RestoreInPlace.JobCreating
	})
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err == nil {
		t.Fatal("want an error when the write-ahead cannot be recorded")
	}
	var job batchv1.Job
	key := types.NamespacedName{Name: inPlaceRestoreJobName(fg.Name, fg.Spec.Sites[0].Name), Namespace: fg.Namespace}
	if err := c.Get(context.Background(), key, &job); !apierrors.IsNotFound(err) {
		t.Fatalf("a Job was created without the write-ahead: err=%v", err)
	}
}

// With the spec still applied, a run whose Job is gone after the
// write-ahead never builds a second Job: the first may have run.
func TestReconcileInPlaceRestore_LostJobIsNeverRecreated(t *testing.T) {
	for _, tc := range []struct {
		name    string
		jobName bool
	}{{name: "name never recorded"}, {name: "name recorded", jobName: true}} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("orders")
			site := fg.Spec.Sites[0].Name
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
				Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "schema:orders", StartTime: ptrTime(time.Now()),
				RunConfirm: fg.Spec.RestoreInPlace.Confirm, JobCreating: true,
			}
			if tc.jobName {
				fg.Status.RestoreInPlace.JobName = inPlaceRestoreJobName(fg.Name, site)
			}
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
			if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			st := getFG(t, c, fg).Status.RestoreInPlace
			if st.Phase != v1alpha1.RestoreInPlaceFailed || st.ConfirmTokenUsed != fg.Spec.RestoreInPlace.Confirm || !st.JobCreating {
				t.Fatalf("status = %+v, want JobLost consuming the run's confirm", st)
			}
			var job batchv1.Job
			key := types.NamespacedName{Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}
			if err := c.Get(context.Background(), key, &job); !apierrors.IsNotFound(err) {
				t.Fatalf("a second Job was built for a run whose Job may have run: err=%v", err)
			}
		})
	}
}

// The API server refusing the Job outright (validation, admission) proves
// none was created: the write-ahead is withdrawn, so removing the spec
// afterwards is still a clean cancellation that consumes nothing.
func TestReconcileInPlaceRestore_RefusedCreateWithdrawsWriteAhead(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	driveToJobCreate(t, r, c, fg)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return apierrors.NewForbidden(batchv1.Resource("jobs"), obj.GetName(), errors.New("admission denied"))
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err == nil {
		t.Fatal("want the create error")
	}
	r.Client = c
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.JobCreating {
		t.Fatalf("status = %+v, want the write-ahead withdrawn after a refused create", st)
	}
	*fg = *getFG(t, c, fg)
	fg.Spec.RestoreInPlace = nil
	if err := c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "cancelled") || st.ConfirmTokenUsed != "" {
		t.Fatalf("status = %+v, want a clean cancellation", st)
	}
}

// Full-instance settlement: restore B superseded A's draining queue, its
// Job was created but the name never recorded, and the Job disappeared.
// B may have dropped or half-loaded the primary, so A's queue is put back
// Held and nothing is cloned. Without the write-ahead the empty JobName
// looked like a run that never executed and A's queue drained from B's
// primary.
func TestReconcileInPlaceRestore_UnrecordedLostJobHoldsSupersededQueue(t *testing.T) {
	const confirmB = "2026-04-18T10:00:00Z"
	fg := fgDrainingRestore("pdx")
	site := fg.Status.ActiveSite
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: confirmB, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx", "sea"},
		Superseded: &v1alpha1.RestoreReclonesCarryover{ConfirmToken: testRestoreConfirm, Pending: []string{"pdx"}},
	}
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "full", StartTime: ptrTime(time.Now()),
		ConfirmTokenUsed: testRestoreConfirm, RunConfirm: confirmB, JobCreating: true,
	}
	fg.Spec.RestoreInPlace = nil
	r, c := newReconciler(fg, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || st.ConfirmTokenUsed != confirmB {
		t.Fatalf("status = %+v, want JobLost consuming %s", st, confirmB)
	}
	for i := 0; i < 3; i++ {
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	fresh := getFG(t, c, fg)
	if q := fresh.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesHeld || q.ConfirmToken != testRestoreConfirm {
		t.Fatalf("queue = %+v, want A's queue held", q)
	}
	if got := fresh.GetAnnotations()[RecloneAnnotation]; got != "" {
		t.Fatalf("recloned from the primary of a restore whose Job may have run: annotation %q", got)
	}
}

// staleJobCache makes r's cached client miss every Job, as an informer
// that has not yet observed a just-created Job does, while r.APIReader
// reads the real objects.
func staleJobCache(r *MysqlFailoverGroupReconciler, c client.Client) {
	r.APIReader = c
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return apierrors.NewNotFound(batchv1.Resource("jobs"), key.Name)
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
}

// A cache miss right after Create is not proof that the Job disappeared:
// the Job informer can lag behind the status write. Ending the run there
// would release the fence, the topology freeze and the other guards under
// a destructive Job that is still running. The Job lookup is authoritative
// (APIReader), so the run keeps observing the live Job, whether its name
// was recorded or only the write-ahead was, and with or without the spec.
func TestReconcileInPlaceRestore_CachedJobMissDoesNotEndLiveRun(t *testing.T) {
	for _, tc := range []struct {
		name          string
		loseNameWrite bool
		removeSpec    bool
	}{
		{name: "name recorded, spec applied"},
		{name: "name recorded, spec removed", removeSpec: true},
		{name: "only write-ahead, spec applied", loseNameWrite: true},
		{name: "only write-ahead, spec removed", loseNameWrite: true, removeSpec: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("orders")
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
			driveToJobCreate(t, r, c, fg)
			if tc.loseNameWrite {
				failStatusWrites(t, r, c, func(fg *v1alpha1.MysqlFailoverGroup) bool {
					return fg.Status.RestoreInPlace != nil && fg.Status.RestoreInPlace.JobName != ""
				})
			}
			_, _ = r.reconcileInPlaceRestore(context.Background(), fg)
			r.Client = c
			*fg = *getFG(t, c, fg)
			if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceRestoring || !st.JobCreating || (st.JobName != "") == tc.loseNameWrite {
				t.Fatalf("setup: status = %+v", st)
			}
			if tc.removeSpec {
				fg.Spec.RestoreInPlace = nil
				if err := c.Update(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}

			staleJobCache(r, c)
			for i := 0; i < 2; i++ {
				*fg = *getFG(t, c, fg)
				if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}
			fresh := getFG(t, c, fg)
			if st := fresh.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceRestoring {
				t.Fatalf("status = %+v: a cached Job miss ended the run while the Job runs", st)
			}
			if !inPlaceRestoreFreezesTopology(fresh) {
				t.Fatal("the topology freeze was released under a running Job")
			}
			if tc.loseNameWrite && fresh.Status.RestoreInPlace.JobName == "" && tc.removeSpec {
				t.Fatal("the live Job was not adopted")
			}
		})
	}
}

// A failed authoritative read keeps the run as it is and retries.
func TestReconcileInPlaceRestore_JobReadErrorKeepsGuards(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	driveToJobCreate(t, r, c, fg)
	_, _ = r.reconcileInPlaceRestore(context.Background(), fg)
	r.APIReader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return errors.New("apiserver unavailable")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err == nil {
		t.Fatal("want the read error returned so the reconcile retries")
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceRestoring {
		t.Fatalf("status = %+v, want the run left Restoring", st)
	}
}

// Reverting the manifest to the previous, already executed restore (a
// GitOps "cancel") while the newer run has not created its Job cancels
// that run: building its Job from the reverted spec would restore what the
// revert was meant to undo. An invalid confirm cancels the same way.
func TestReconcileInPlaceRestore_ConfirmMovedBackBeforeJobCancels(t *testing.T) {
	const newer = "2026-04-18T09:00:00Z"
	for _, tc := range []struct {
		name    string
		phase   v1alpha1.RestoreInPlacePhase
		confirm string
	}{
		{name: "reverted in preflight", phase: v1alpha1.RestoreInPlacePreflight},
		{name: "reverted in fencing", phase: v1alpha1.RestoreInPlaceFencing},
		{name: "reverted in restoring before the job", phase: v1alpha1.RestoreInPlaceRestoring},
		{name: "invalid confirm", phase: v1alpha1.RestoreInPlaceFencing, confirm: "not-a-timestamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("orders")
			site := fg.Spec.Sites[0].Name
			executed := fg.Spec.RestoreInPlace.Confirm
			if tc.confirm != "" {
				fg.Spec.RestoreInPlace.Confirm = tc.confirm
			}
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
				Phase: tc.phase, TargetSite: site, Scope: "schema:orders", StartTime: ptrTime(time.Now()),
				ConfirmTokenUsed: executed, RunConfirm: newer,
			}
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
			for i := 0; i < 3; i++ {
				*fg = *getFG(t, c, fg)
				if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}
			st := getFG(t, c, fg).Status.RestoreInPlace
			if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "does not advance past it") || st.ConfirmTokenUsed != executed {
				t.Fatalf("status = %+v, want a cancellation that keeps confirmTokenUsed %s", st, executed)
			}
			var job batchv1.Job
			key := types.NamespacedName{Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}
			if err := c.Get(context.Background(), key, &job); !apierrors.IsNotFound(err) {
				t.Fatalf("a restore Job was built from the reverted spec: err=%v", err)
			}
			if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreInPlaceCancelled") {
				t.Error("want a RestoreInPlaceCancelled event")
			}
		})
	}
}

// Once the Job runs it is never interrupted. A confirm moved back while it
// runs starts nothing afterwards, and the status says so instead of
// promising a new run.
func TestReconcileInPlaceRestore_ConfirmMovedBackWhileJobRunsStartsNothing(t *testing.T) {
	const newer = "2026-04-18T09:00:00Z"
	fg := fgInPlaceRestore("orders")
	site := fg.Spec.Sites[0].Name
	start := time.Now().Add(-time.Minute)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceRestoring, TargetSite: site, Scope: "schema:orders", StartTime: ptrTime(start),
		JobName: inPlaceRestoreJobName(fg.Name, site), JobCreating: true,
		ConfirmTokenUsed: fg.Spec.RestoreInPlace.Confirm, RunConfirm: newer,
	}
	job := runningInPlaceJob(fg, site, newer, start.Add(5*time.Second))
	r, c := newReconciler(fg, job, succeededSeedBackup(), dsnSecret())
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	st := getFG(t, c, fg).Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlaceRestoring || !strings.Contains(st.Message, "does not advance past it and starts nothing") {
		t.Fatalf("status = %+v, want the running Job kept and the message saying the old confirm starts nothing", st)
	}
}
