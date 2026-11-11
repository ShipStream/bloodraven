package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The restore hold's four conditional writes (#275). Each test lets another
// writer in between the read a write was decided on and the write itself;
// each fails when that write's MergeFromWithOptimisticLock is replaced by a
// plain merge patch. test/envtest/restore_hold_test.go repeats them against
// a real API server.

// onceBefore returns a hook that runs fn the first time match accepts the
// object being written, before the write is forwarded.
func onceBefore(t *testing.T, match func(client.Object) bool, fn func(ctx context.Context, c client.Client)) (hook func(context.Context, client.Client, client.Object), ran *bool) {
	t.Helper()
	ran = new(bool)
	return func(ctx context.Context, c client.Client, obj client.Object) {
		if *ran || !match(obj) {
			return
		}
		*ran = true
		fn(ctx, c)
	}, ran
}

func isBackup(obj client.Object) bool {
	_, ok := obj.(*v1alpha1.MysqlBackup)
	return ok
}

func isGroup(obj client.Object) bool {
	_, ok := obj.(*v1alpha1.MysqlFailoverGroup)
	return ok
}

// beforeBackupPatch wraps c so that fn runs once, right before the first
// patch of a MysqlBackup reaches the API.
func beforeBackupPatch(t *testing.T, c client.Client, fn func(ctx context.Context, c client.Client)) (client.Client, *bool) {
	t.Helper()
	hook, ran := onceBefore(t, isBackup, fn)
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			hook(ctx, c, obj)
			return c.Patch(ctx, obj, patch, opts...)
		},
	}), ran
}

// The stamp. The backup is deleted (it has the finalizer, so it turns
// Terminating) between the restore's uncached read of it and the stamp:
// the backup's finalizer, which found no reader, may already be cleaning
// up. Only the stamp's resourceVersion precondition keeps the hold off the
// backup and the Job from being created.
func TestRestoreHold_StampLosesToADeleteSinceItsRead(t *testing.T) {
	ctx := context.Background()
	seed := restoreHoldSeed()
	seed.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
	r, c, jobName := initRestoreFixture(t, seed)
	var deleted *bool
	r.Client, deleted = beforeBackupPatch(t, c, func(ctx context.Context, c client.Client) {
		if err := c.Delete(ctx, fetchBackup(t, c, "seed")); err != nil {
			t.Fatal(err)
		}
	})
	requeue, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion"))
	if err != nil || requeue != restoreHoldRetry || !*deleted {
		t.Fatalf("requeue = %s, err = %v, deleted = %v, want the stamp to lose and the pass to retry", requeue, err, *deleted)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created from a backup that was deleted before the hold was stamped")
	}
	if b := fetchBackup(t, c, "seed"); b.DeletionTimestamp.IsZero() || len(restoreHoldAnnotations(b)) != 0 {
		t.Fatalf("backup deleting = %v, holds = %v, want a Terminating backup without the hold", !b.DeletionTimestamp.IsZero(), restoreHoldAnnotations(b))
	}
	// The delete won: the next pass ends the attempt.
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	if st := fetchGroup(t, c, "lion").Status.Restore; st.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(st.Message, "the backup is being deleted") {
		t.Fatalf("status.restore = %+v, want Failed (being deleted)", st)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created from a backup being deleted that this restore never held")
	}
}

// The bootstrap pin. Between the uncached read of the group the pin is
// decided on and the pin's write, the stored status moves on (another
// attempt pinned, ran and failed). The pin is written with that read's
// resourceVersion, so it loses: nothing is stamped or created under an
// attempt the API server never recorded as this pass saw it.
func TestRestoreHold_PinLosesToAStatusWriteSinceItsRead(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
	other := &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: "an-earlier-job", Message: "an attempt this pass did not see",
		BackupRef: &v1alpha1.RestoreBackupRef{Name: "seed", UID: "uid-seed", HoldID: "other-attempt"}}
	hook, wrote := onceBefore(t, isGroup, func(ctx context.Context, c client.Client) {
		fg := fetchGroup(t, c, "lion")
		fg.Status.Restore = other.DeepCopy()
		if err := c.Status().Update(ctx, fg); err != nil {
			t.Fatal(err)
		}
	})
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			hook(ctx, c, obj)
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	})
	requeue, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion"))
	if err != nil || requeue != restoreHoldRetry || !*wrote {
		t.Fatalf("requeue = %s, err = %v, wrote = %v, want the pin to lose and the pass to retry", requeue, err, *wrote)
	}
	if st := fetchGroup(t, c, "lion").Status.Restore; st == nil || st.Phase != other.Phase || st.JobName != other.JobName ||
		st.BackupRef == nil || *st.BackupRef != *other.BackupRef {
		t.Fatalf("status.restore = %+v, want the status written since the read, untouched", st)
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 0 {
		t.Fatalf("hold stamped under a pin that was not recorded: %v", keys)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created under a pin that was not recorded")
	}
}

// The release. A terminal restore's hold is the backup's only annotation,
// so a plain merge patch that removes it is {"annotations": null}: it
// would also remove a hold another reader stamped after the release read
// the backup. The release carries that read's resourceVersion and loses
// instead; the next pass removes its own hold only.
func TestRestoreHold_ReleaseKeepsAHoldStampedSinceItsRead(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	job, _ := restoreJobExists(t, c, jobName)
	endRestoreJob(t, c, job, batchv1.JobComplete)
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	own := restoreHoldKey(fetchGroup(t, c, "lion").Status.Restore.BackupRef.HoldID)
	if seed := fetchBackup(t, c, "seed"); len(seed.Annotations) != 1 || seed.Annotations[own] != "lion" {
		t.Fatalf("annotations before the release = %v, want this restore's hold alone", seed.Annotations)
	}
	const verification = v1alpha1.VerificationHoldAnnotationPrefix + "uid-v"
	var stamped *bool
	r.Client, stamped = beforeBackupPatch(t, c, func(ctx context.Context, c client.Client) {
		seed := fetchBackup(t, c, "seed")
		seed.Annotations[verification] = "v"
		if err := c.Update(ctx, seed); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	seed := fetchBackup(t, c, "seed")
	if !*stamped || seed.Annotations[verification] != "v" {
		t.Fatalf("stamped = %v, annotations = %v: the release dropped a hold stamped after it read the backup", *stamped, seed.Annotations)
	}
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	if seed = fetchBackup(t, c, "seed"); len(seed.Annotations) != 1 || seed.Annotations[verification] != "v" {
		t.Fatalf("annotations after the retried release = %v, want the other reader's hold alone", seed.Annotations)
	}
}

// The strip on a live backup. The dead hold is the backup's only
// annotation, so without the precondition the strip would be
// {"annotations": null} and take a hold stamped since with it.
func TestBackupReconcile_StripKeepsAHoldStampedSinceItsRead(t *testing.T) {
	ctx := context.Background()
	backup := heldBackup()
	backup.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
	dead := restoreHoldKey("dead")
	backup.Annotations = map[string]string{dead: "deleted-group"}
	// The live restore pinned before it stamps, as the protocol has it.
	r, c := newBackupReconciler(t, fgWithBackup(), backup, restoringGroup(v1alpha1.BackupPhaseRunning, holdBackupUID))
	live := restoreHoldKey(restoreHoldTestID)
	var stamped *bool
	r.Client, stamped = beforeBackupPatch(t, c, func(ctx context.Context, c client.Client) {
		b := fetchBackup(t, c, "held")
		b.Annotations[live] = "restorer"
		if err := c.Update(ctx, b); err != nil {
			t.Fatal(err)
		}
	})
	key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "held"}}
	res, err := r.Reconcile(ctx, key)
	if err != nil || !*stamped {
		t.Fatalf("err = %v, stamped = %v", err, *stamped)
	}
	if b := fetchBackup(t, c, "held"); b.Annotations[live] != "restorer" {
		t.Fatalf("annotations = %v: the strip dropped a hold stamped after it read the backup", b.Annotations)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > restoreHoldRecheck {
		t.Fatalf("requeue = %s, want another look after the lost strip", res.RequeueAfter)
	}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	if b := fetchBackup(t, c, "held"); len(b.Annotations) != 1 || b.Annotations[live] != "restorer" {
		t.Fatalf("annotations after the retried strip = %v, want the live hold alone", b.Annotations)
	}
}

// --- A backup deleted while a restore holds it (R4 A3, A4) -----------------

// backupFinalizerFor is the MysqlBackup reconciler on the group fixture's
// API state.
func backupFinalizerFor(c client.Client) *MysqlBackupReconciler {
	return &MysqlBackupReconciler{Client: c, Scheme: testScheme(), Recorder: record.NewFakeRecorder(50)}
}

// deleteHeldSeed deletes the seed backup by hand. It has the finalizer, so
// it turns Terminating, and its finalizer must defer to the restore that
// holds it.
func deleteHeldSeed(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()
	if err := c.Delete(ctx, fetchBackup(t, c, "seed")); err != nil {
		t.Fatal(err)
	}
	seed := fetchBackup(t, c, "seed")
	if seed.DeletionTimestamp.IsZero() {
		t.Fatal("seed is not Terminating after the delete")
	}
	done, wait, err := backupFinalizerFor(c).finalize(ctx, seed)
	if err != nil || done || wait <= 0 {
		t.Fatalf("finalize: done=%v wait=%s err=%v, want the release deferred while the restore holds the backup", done, wait, err)
	}
	// (The group fixture's API does not serve MysqlBackup status as a
	// subresource, so the condition is read off the reconciled object.)
	cond := apiMeta.FindStatusCondition(seed.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if cond == nil || cond.Reason != v1alpha1.ReasonHeldByRestore || !strings.Contains(cond.Message, "the restore of group lion") {
		t.Fatalf("ArtifactCleanup condition = %+v, want HeldByRestore naming the restoring group", cond)
	}
}

// In place: a backup deleted by hand after Preflight took the hold does not
// fail the run after its fence. The hold keeps the backup Terminating and
// the artifact in place, the Job is created from it, and the deletion goes
// on once the run has ended.
func TestRestoreHold_InPlace_BackupDeletedAfterPreflightIsStillRead(t *testing.T) {
	ctx := context.Background()
	seed := restoreHoldSeed()
	seed.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
	r, c, jobName := inPlaceHoldFixture(t, seed)
	if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	ref := fetchGroup(t, c, "lion").Status.RestoreInPlace.BackupRef
	if ref == nil || fetchBackup(t, c, "seed").Annotations[restoreHoldKey(ref.HoldID)] != "lion" {
		t.Fatalf("pin = %+v, want the seed held in Preflight", ref)
	}
	deleteHeldSeed(t, c)

	fg := driveInPlace(t, r, c, func(s *v1alpha1.RestoreInPlaceStatus) bool { return s.JobName != "" || inPlaceRestoreTerminal(s) })
	if st := fg.Status.RestoreInPlace; inPlaceRestoreTerminal(st) {
		t.Fatalf("status.restoreInPlace = %+v: the run ended over a backup its own hold protects", st)
	}
	job, ok := restoreJobExists(t, c, jobName)
	if !ok || job.Annotations[v1alpha1.RestoreBackupUIDAnnotation] != "uid-seed" {
		t.Fatalf("restore Job missing or not built from the held backup: %+v", job)
	}
	if names, err := artifactReaders(ctx, c, fetchBackup(t, c, "seed")); err != nil || len(names) != 1 || names[0] != "lion" {
		t.Fatalf("readers of the Terminating backup = %v, %v, want the restoring group", names, err)
	}

	endRestoreJob(t, c, job, batchv1.JobComplete)
	driveInPlace(t, r, c, inPlaceRestoreTerminal)
	if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	seedNow := fetchBackup(t, c, "seed")
	if keys := restoreHoldAnnotations(seedNow); len(keys) != 0 {
		t.Fatalf("hold not released after the run ended: %v", keys)
	}
	if names, err := artifactReaders(ctx, c, seedNow); err != nil || len(names) != 0 {
		t.Fatalf("readers after the run ended = %v, %v", names, err)
	}
}

// In place: a backup that is being deleted and does not carry this run's
// hold is still refused in Restoring (a run that entered Restoring without
// a pin, under an operator that did not take one).
func TestRestoreHold_InPlace_UnheldBackupBeingDeletedIsRefusedInRestoring(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := inPlaceHoldFixture(t, terminatingBackup(restoreHoldSeed()))
	fg := fetchGroup(t, c, "lion")
	fg.Status.RestoreInPlace.Phase = v1alpha1.RestoreInPlaceRestoring
	fg.Status.RestoreInPlace.TargetSite = fg.Spec.Sites[0].Name
	if err := c.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fg = driveInPlace(t, r, c, inPlaceRestoreTerminal)
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "the backup is being deleted") {
		t.Fatalf("status.restoreInPlace = %+v, want Failed (being deleted)", st)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created from a backup being deleted that the run never held")
	}
}

// Bootstrap: an attempt that pinned and stamped but has no Job yet (the
// create keeps failing, then its target is not ready) keeps its hold, so a
// backup deleted by hand stays Terminating. Both objects say so: the
// backup's ArtifactCleanup condition names the group, the restore's message
// names the backup. Once the restore can run it reads the held backup.
func TestRestoreHold_InitFromBackup_WaitingHolderOfADeletedBackup(t *testing.T) {
	ctx := context.Background()
	seed := restoreHoldSeed()
	seed.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
	r, c, jobName := initRestoreFixture(t, seed)
	failCreate := true
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok && failCreate {
				return apierrors.NewForbidden(batchv1.Resource("jobs"), obj.GetName(), errors.New("exceeded quota"))
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err == nil {
		t.Fatal("want the create error")
	}
	st := fetchGroup(t, c, "lion").Status.Restore
	if st.Phase != v1alpha1.BackupPhasePending || st.BackupRef == nil || !strings.Contains(st.Message, "creating the restore Job failed") ||
		!strings.Contains(st.Message, "exceeded quota") || strings.Contains(st.Message, "is being deleted") {
		t.Fatalf("status.restore = %+v, want Pending, pinned, naming the failed create", st)
	}
	ref := *st.BackupRef
	deleteHeldSeed(t, c)

	// The create fails again: the message now names the waiting backup.
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err == nil {
		t.Fatal("want the create error")
	}
	const note = "MysqlBackup seed, which this restore has pinned, is being deleted"
	if st = fetchGroup(t, c, "lion").Status.Restore; st.BackupRef == nil || *st.BackupRef != ref || !strings.Contains(st.Message, note) {
		t.Fatalf("status.restore = %+v, want the same pin and a message naming the backup being deleted", st)
	}

	// The target stops being ready: the attempt waits, still holding.
	var deploy appsv1.Deployment
	deployKey := types.NamespacedName{Namespace: "ns", Name: resourceName("lion", "iad")}
	if err := c.Get(ctx, deployKey, &deploy); err != nil {
		t.Fatal(err)
	}
	deploy.Status.ReadyReplicas = 0
	if err := c.Status().Update(ctx, &deploy); err != nil {
		t.Fatal(err)
	}
	failCreate = false
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	if st = fetchGroup(t, c, "lion").Status.Restore; st.Phase != v1alpha1.BackupPhasePending || st.BackupRef == nil || *st.BackupRef != ref ||
		!strings.Contains(st.Message, "waiting for target MySQL Deployment") || !strings.Contains(st.Message, note) {
		t.Fatalf("waiting status.restore = %+v, want the pin kept and both reasons named", st)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created while its target is not ready")
	}
	if names, err := artifactReaders(ctx, c, fetchBackup(t, c, "seed")); err != nil || len(names) != 1 {
		t.Fatalf("readers while the holder waits = %v, %v", names, err)
	}

	// The target is ready again: the restore reads the backup it holds.
	deploy.Status.ReadyReplicas = 1
	if err := c.Status().Update(ctx, &deploy); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	job, ok := restoreJobExists(t, c, jobName)
	if !ok || job.Annotations[v1alpha1.RestoreBackupUIDAnnotation] != "uid-seed" {
		t.Fatalf("restore Job missing or not built from the held backup: %+v", job)
	}
	if st = fetchGroup(t, c, "lion").Status.Restore; st.Phase != v1alpha1.BackupPhaseRunning || st.BackupRef == nil || *st.BackupRef != ref {
		t.Fatalf("status.restore = %+v, want Running under the same pin", st)
	}
}

// --- setRestoreStatus writes against the stored status (R4 A5) ------------

// A pass whose cached group is one write behind must not decide the pin.
func TestSetRestoreStatus_PinIsCarriedFromTheStoredStatus(t *testing.T) {
	pin := func(holdID string) *v1alpha1.RestoreBackupRef {
		return &v1alpha1.RestoreBackupRef{Name: "seed", UID: "uid-seed", HoldID: holdID}
	}
	for _, tc := range []struct {
		name    string
		cached  *v1alpha1.RestoreStatus
		stored  *v1alpha1.RestoreStatus
		write   *v1alpha1.RestoreStatus
		wantPin *v1alpha1.RestoreBackupRef
	}{
		{
			// The attempt ended (Failed) and its Job was deleted; the cache
			// still shows it Running. Its hold has been seen dead: the
			// Pending status of the retry must not carry it.
			name:   "an ended attempt's pin is not revived",
			cached: &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning, JobName: "job", BackupRef: pin("ended")},
			stored: &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: "job", BackupRef: pin("ended")},
			write:  &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending, Message: "waiting"},
		},
		{
			// A new attempt pinned; the cache still shows the Failed one
			// before it. The waiting status must keep the new pin.
			name:    "a newer attempt's pin is not erased",
			cached:  &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, BackupRef: pin("ended")},
			stored:  &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending, BackupRef: pin("current")},
			write:   &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending, Message: "waiting"},
			wantPin: pin("current"),
		},
		{
			name:    "the terminal status keeps the stored attempt's pin",
			cached:  &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending},
			stored:  &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning, JobName: "job", BackupRef: pin("current")},
			write:   &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: "job"},
			wantPin: pin("current"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, c, _ := initRestoreFixture(t, restoreHoldSeed())
			stored := fetchGroup(t, c, "lion")
			stored.Status.Restore = tc.stored.DeepCopy()
			if err := c.Status().Update(ctx, stored); err != nil {
				t.Fatal(err)
			}
			// The pass's copy: the group as an earlier write left it.
			cached := fetchGroup(t, c, "lion")
			cached.Status.Restore = tc.cached.DeepCopy()
			cached.ResourceVersion = "1"
			write := tc.write.DeepCopy()
			if !r.setRestoreStatus(ctx, cached, write) {
				t.Fatal("setRestoreStatus reported no write")
			}
			if write.BackupRef != nil {
				t.Fatalf("the caller's status was modified: %+v", write)
			}
			got := fetchGroup(t, c, "lion").Status.Restore
			if got == nil || got.Phase != tc.write.Phase {
				t.Fatalf("stored status.restore = %+v, want phase %s", got, tc.write.Phase)
			}
			switch {
			case tc.wantPin == nil && got.BackupRef != nil:
				t.Fatalf("stored pin = %+v, want none", got.BackupRef)
			case tc.wantPin != nil && (got.BackupRef == nil || *got.BackupRef != *tc.wantPin):
				t.Fatalf("stored pin = %+v, want %+v", got.BackupRef, tc.wantPin)
			}
			// The pass goes on with the group as stored.
			if cached.Status.Restore == nil || cached.Status.Restore.Phase != tc.write.Phase || cached.ResourceVersion != fetchGroup(t, c, "lion").ResourceVersion {
				t.Fatalf("the pass's copy was not refreshed: %+v rv=%s", cached.Status.Restore, cached.ResourceVersion)
			}
		})
	}
}

// A write to the group between the status write's read and its patch makes
// it decide again on the new stored status. Here the attempt ends (Failed)
// in between: a patch computed from the first read would leave the ended
// attempt's pin on the Pending status it writes, and turn a dead hold live.
func TestSetRestoreStatus_DecidesAgainAfterAConflict(t *testing.T) {
	ctx := context.Background()
	r, c, _ := initRestoreFixture(t, restoreHoldSeed())
	ended := &v1alpha1.RestoreBackupRef{Name: "seed", UID: "uid-seed", HoldID: "ended"}
	fg := fetchGroup(t, c, "lion")
	fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning, JobName: "job", BackupRef: ended.DeepCopy()}
	if err := c.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	hook, wrote := onceBefore(t, isGroup, func(ctx context.Context, c client.Client) {
		fg := fetchGroup(t, c, "lion")
		fg.Status.Restore.Phase = v1alpha1.BackupPhaseFailed
		if err := c.Status().Update(ctx, fg); err != nil {
			t.Fatal(err)
		}
	})
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			hook(ctx, c, obj)
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	})
	r.APIReader = c
	if !r.setRestoreStatus(ctx, fetchGroup(t, c, "lion"), &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending, Message: "waiting"}) {
		t.Fatal("setRestoreStatus reported no write")
	}
	got := fetchGroup(t, c, "lion").Status.Restore
	if !*wrote || got == nil || got.Phase != v1alpha1.BackupPhasePending || got.Message != "waiting" || got.BackupRef != nil {
		t.Fatalf("wrote = %v, status.restore = %+v, want a Pending status without the pin of the attempt that ended meanwhile", *wrote, got)
	}
}

// --- Releases that leave the artifact in storage wait for readers (R4 A2) --

// A backup whose artifact cannot be cleaned up (its profile or its group is
// gone, or no destination is recorded) is still released only once nothing
// reads it: the object is what keeps the PITR binlog cutoff bounded for a
// replay in progress.
func TestFinalize_SkippedCleanupWaitsForReaders(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(t *testing.T, f *cleanupFixture)
		kept   string
		skip   string
	}{
		{
			name: "profile gone from the group",
			break_: func(t *testing.T, f *cleanupFixture) {
				fg := fetchGroup(t, f.c, "lion")
				fg.Spec.Backup.Profiles[0].Name = "renamed"
				if err := f.c.Update(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			},
			kept: `profile "nightly-s3" is gone from group "lion"`,
			skip: `profile "nightly-s3" gone from group "lion"`,
		},
		{
			name: "group gone",
			break_: func(t *testing.T, f *cleanupFixture) {
				if err := f.c.Delete(context.Background(), fetchGroup(t, f.c, "lion")); err != nil {
					t.Fatal(err)
				}
			},
			kept: `failover group "lion" is gone`,
			skip: `failover group "lion" gone`,
		},
		{
			name: "no recorded destination",
			break_: func(t *testing.T, f *cleanupFixture) {
				b := fetchBackup(t, f.c, f.mb.Name)
				b.Status.ArtifactStorage = nil
				if err := f.c.Status().Update(context.Background(), b); err != nil {
					t.Fatal(err)
				}
			},
			kept: "no artifact destination is recorded",
			skip: "no recorded artifact destination",
		},
	} {
		for _, reader := range []string{"restore", "verification"} {
			t.Run(tc.name+"/"+reader, func(t *testing.T) {
				ctx := context.Background()
				f := newCleanupFixture(t)
				tc.break_(t, f)
				var end func()
				wantReason, wantReader := v1alpha1.ReasonHeldByRestore, "the restore of group restorer"
				if reader == "restore" {
					// Another group replays from this backup.
					group := restoringGroup(v1alpha1.BackupPhaseRunning, string(f.mb.UID))
					group.Spec.InitFromBackup.Source.BackupRef.Name = f.mb.Name
					group.Status.Restore.BackupRef.Name = f.mb.Name
					status := group.Status.DeepCopy()
					if err := f.c.Create(ctx, group); err != nil {
						t.Fatal(err)
					}
					group.Status = *status
					if err := f.c.Status().Update(ctx, group); err != nil {
						t.Fatal(err)
					}
					end = func() {
						group.Status.Restore.Phase = v1alpha1.BackupPhaseSucceeded
						if err := f.c.Status().Update(ctx, group); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					wantReason, wantReader = v1alpha1.ReasonHeldByVerification, "verification v"
					v := holdVerification("v", v1alpha1.VerificationPhaseRestoring, string(f.mb.UID))
					v.Status.BackupRef.Name = f.mb.Name
					if err := f.c.Create(ctx, v); err != nil {
						t.Fatal(err)
					}
					end = func() {
						if err := f.c.Delete(ctx, v); err != nil {
							t.Fatal(err)
						}
					}
				}
				for range 2 {
					if done, requeue := f.finalize(t); done || requeue <= 0 {
						t.Fatalf("done=%v requeue=%s, want the release deferred while the backup is read", done, requeue)
					}
				}
				cond := cleanupCondition(t, f)
				if cond == nil || cond.Reason != wantReason || !strings.Contains(cond.Message, wantReader) ||
					!strings.Contains(cond.Message, tc.kept+", so the artifact stays in storage and only the release of this backup waits") {
					t.Fatalf("condition = %+v, want %s naming %q and why the artifact stays (%s)", cond, wantReason, wantReader, tc.kept)
				}
				events := drainEvents(f.rec)
				deferred, skipped := 0, 0
				for _, e := range events {
					if strings.Contains(e, v1alpha1.EventArtifactCleanupDeferred) {
						deferred++
					}
					if strings.Contains(e, v1alpha1.EventArtifactCleanupSkipped) {
						skipped++
					}
				}
				if deferred != 1 || skipped != 0 {
					t.Fatalf("events while read = %q, want one ArtifactCleanupDeferred and no ArtifactCleanupSkipped", events)
				}

				end()
				if done, _ := f.finalize(t); !done {
					t.Fatal("backup not released once nothing reads it")
				}
				if events = drainEvents(f.rec); len(events) != 1 || !strings.Contains(events[0], v1alpha1.EventArtifactCleanupSkipped) || !strings.Contains(events[0], tc.skip) {
					t.Fatalf("events at the release = %q, want one ArtifactCleanupSkipped (%s)", events, tc.skip)
				}
				if _, ok := f.cleanupJob(t); ok {
					t.Fatal("a cleanup Job was created for an artifact that cannot be cleaned up")
				}
			})
		}
	}
}
