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
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// runningBackup reconciles a fresh backup of the S3 profile until its Job
// runs and returns the reconciler, the client and the backup's key.
func runningBackup(t *testing.T, name string) (*MysqlBackupReconciler, client.Client, types.NamespacedName) {
	t.Helper()
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: v1alpha1.SiteStateWritable},
			{Name: "pdx", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy},
		},
	}
	mb := backupCR(name, "lion", "nightly-s3")
	mb.UID = types.UID(name + "-uid")
	r, c := newBackupReconciler(t, fg, mb, operatorCredsSecret())
	reconcileUntilStable(t, r, name)
	key := types.NamespacedName{Namespace: "ns", Name: name}
	var got v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != v1alpha1.BackupPhaseRunning || got.Status.ArtifactStorage == nil {
		t.Fatalf("setup: phase %q storage %+v, want Running with a recorded destination", got.Status.Phase, got.Status.ArtifactStorage)
	}
	return r, c, key
}

func readBackupByKey(t *testing.T, c client.Client, key types.NamespacedName) *v1alpha1.MysqlBackup {
	t.Helper()
	var got v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	return &got
}

// finishBackupJob marks the backup's Job Complete or Failed.
func finishBackupJob(t *testing.T, c client.Client, backup string, condition batchv1.JobConditionType) {
	t.Helper()
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: backupJobName(backup)}, &job); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now}}
	if condition == batchv1.JobComplete {
		job.Status.Succeeded = 1
	} else {
		job.Status.Failed = 1
	}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
}

// loseLaunchAttribution simulates the status patch after the Job's create
// being lost: the backup's status keeps nothing the patch would have
// written about the destination and encryption.
func loseLaunchAttribution(t *testing.T, c client.Client, key types.NamespacedName) {
	t.Helper()
	b := readBackupByKey(t, c, key)
	b.Status.ArtifactStorage, b.Status.Encryption = nil, nil
	if err := c.Status().Update(context.Background(), b); err != nil {
		t.Fatal(err)
	}
}

// TestBackupLostLaunchStatusRecoversAttribution (K046): when the status
// patch after the Job's create is lost, the destination and encryption
// attribution are recovered from the owned Job, while it runs and when it
// fails, so status.artifactStorage and status.encryption are present for
// the abandoned cleanup, restores and verifications.
func TestBackupLostLaunchStatusRecoversAttribution(t *testing.T) {
	for _, tc := range []struct {
		name     string
		terminal batchv1.JobConditionType
	}{{name: "running"}, {name: "failed", terminal: batchv1.JobFailed}} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, key := runningBackup(t, "lost-launch-"+tc.name)
			want := readBackupByKey(t, c, key).Status.ArtifactStorage
			loseLaunchAttribution(t, c, key)
			if tc.terminal != "" {
				finishBackupJob(t, c, key.Name, tc.terminal)
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			got := readBackupByKey(t, c, key)
			if tc.terminal == batchv1.JobFailed && got.Status.Phase != v1alpha1.BackupPhaseFailed {
				t.Fatalf("phase %q, want Failed", got.Status.Phase)
			}
			if got.Status.ArtifactStorage == nil || *got.Status.ArtifactStorage != *want {
				t.Fatalf("artifactStorage = %+v, want %+v recovered from the Job", got.Status.ArtifactStorage, want)
			}
			if got.Status.Encryption == nil || got.Status.Encryption.Mode != v1alpha1.EncryptionModeNone {
				t.Fatalf("encryption = %+v, want None recovered from the plaintext Job", got.Status.Encryption)
			}
		})
	}
}

// TestBackupReplacementJobRefusedAtAnotherDestination (K008): a backup
// whose Job is deleted mid-run is never re-launched at a destination other
// than the one recorded at launch (status.artifactStorage cannot change):
// it fails BuildJobFailed and no replacement Job is created.
func TestBackupReplacementJobRefusedAtAnotherDestination(t *testing.T) {
	r, c, key := runningBackup(t, "moved-profile")
	ctx := context.Background()
	var fg v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "lion"}, &fg); err != nil {
		t.Fatal(err)
	}
	fg.Spec.Backup.Profiles[0].Storage.S3.Bucket = "another-bucket"
	if err := c.Update(ctx, &fg); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: backupJobName(key.Name)}}
	if err := c.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	got := readBackupByKey(t, c, key)
	ready := apiMeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady)
	if got.Status.Phase != v1alpha1.BackupPhaseFailed || ready == nil || ready.Reason != v1alpha1.ReasonBuildJobFailed {
		t.Fatalf("phase %q Ready %+v, want Failed/BuildJobFailed", got.Status.Phase, ready)
	}
	if !strings.Contains(ready.Message, "another-bucket") || !strings.Contains(ready.Message, "recorded at launch") {
		t.Fatalf("Ready message %q does not name the replacement's destination", ready.Message)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("a replacement Job was created at another destination: %v", err)
	}
}

// TestBackupCompletionAtAnotherDestinationFails (K008): a Job that
// succeeded but did not write to the recorded destination never certifies
// it: the backup fails DestinationMismatch instead of Succeeded.
func TestBackupCompletionAtAnotherDestinationFails(t *testing.T) {
	r, c, key := runningBackup(t, "stale-record")
	b := readBackupByKey(t, c, key)
	b.Status.ArtifactStorage.Bucket = "first-bucket"
	if err := c.Status().Update(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	finishBackupJob(t, c, key.Name, batchv1.JobComplete)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	got := readBackupByKey(t, c, key)
	ready := apiMeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady)
	if got.Status.Phase != v1alpha1.BackupPhaseFailed || ready == nil || ready.Status != metav1.ConditionFalse ||
		ready.Reason != v1alpha1.ReasonDestinationMismatch {
		t.Fatalf("phase %q Ready %+v, want Failed with Ready=False/DestinationMismatch", got.Status.Phase, ready)
	}
	if got.Status.ArtifactStorage.Bucket != "first-bucket" {
		t.Fatalf("the recorded destination changed: %+v", got.Status.ArtifactStorage)
	}
}

// TestBackupTerminalVerdictStartsGraceBeforeTheWrite (K010): the retention
// grace starts before the terminal status Patch, so a Patch that commits
// but whose response is lost cannot let the retry's keep-none prune delete
// the backup at once.
func TestBackupTerminalVerdictStartsGraceBeforeTheWrite(t *testing.T) {
	r, c, key := runningBackup(t, "lost-verdict-reply")
	finishBackupJob(t, c, key.Name, batchv1.JobFailed)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if err := c.SubResource(sub).Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}
			return errors.New("connection reset after the write committed")
		},
	})
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err == nil {
		t.Fatal("the lost reply was not reported")
	}
	got := readBackupByKey(t, c, key)
	if got.Status.Phase != v1alpha1.BackupPhaseFailed {
		t.Fatalf("setup: phase %q, want the committed Failed verdict", got.Status.Phase)
	}
	if left := r.graceClock.left(got, nil, time.Now()); left <= 0 {
		t.Fatal("no retention grace was recorded for a verdict whose write committed")
	}
}

// TestTerminalBackupAcknowledgesSpecEdits (K110): editing a terminal
// backup's spec (deletionPolicy is read at deletion) moves
// status.observedGeneration and Ready's observedGeneration to the new
// generation, keeping the verdict and its transition time, so Argo CD
// health does not read Progressing for as long as the backup exists.
func TestTerminalBackupAcknowledgesSpecEdits(t *testing.T) {
	for _, terminal := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run(string(terminal), func(t *testing.T) {
			r, c, key := runningBackup(t, "edited-"+strings.ToLower(string(terminal)))
			finishBackupJob(t, c, key.Name, terminal)
			reconcileUntilStable(t, r, key.Name)
			before := readBackupByKey(t, c, key)
			readyBefore := apiMeta.FindStatusCondition(before.Status.Conditions, v1alpha1.ConditionReady)
			if readyBefore == nil {
				t.Fatal("setup: no Ready condition")
			}
			before.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
			before.Generation++
			if err := c.Update(context.Background(), before); err != nil {
				t.Fatal(err)
			}
			edited := readBackupByKey(t, c, key)
			if edited.Status.ObservedGeneration >= edited.Generation {
				t.Fatalf("setup: observedGeneration %d already at generation %d", edited.Status.ObservedGeneration, edited.Generation)
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			got := readBackupByKey(t, c, key)
			ready := apiMeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady)
			if got.Status.ObservedGeneration != got.Generation || ready.ObservedGeneration != got.Generation {
				t.Fatalf("observedGeneration %d / Ready %d, want generation %d", got.Status.ObservedGeneration, ready.ObservedGeneration, got.Generation)
			}
			if got.Status.Phase != before.Status.Phase || ready.Status != readyBefore.Status || ready.Reason != readyBefore.Reason ||
				!ready.LastTransitionTime.Equal(&readyBefore.LastTransitionTime) {
				t.Fatalf("verdict changed: phase %q Ready %+v, was %q %+v", got.Status.Phase, ready, before.Status.Phase, readyBefore)
			}
		})
	}
}

// TestFinalize_LateRetainAfterCleanupStartedIsNotApplied (K009): a
// deletionPolicy changed from Delete to Retain after the cleanup Job was
// created is not applied: that Job may already have deleted part of the
// artifact, and releasing the finalizer would garbage-collect it
// mid-delete. The cleanup runs to its end and its Event says so.
func TestFinalize_LateRetainAfterCleanupStartedIsNotApplied(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if done, _ := f.finalize(t); done {
		t.Fatal("released before the cleanup ran")
	}
	if _, ok := f.cleanupJob(t); !ok {
		t.Fatal("setup: no cleanup Job")
	}
	f.mb.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := f.c.Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	drainEvents(f.rec)
	if done, _ := f.finalize(t); done {
		t.Fatal("a late Retain released the backup while its cleanup Job runs")
	}
	if n := countEvents(f.rec, "ArtifactRetained"); n != 0 {
		t.Fatal("ArtifactRetained emitted although the cleanup had started")
	}
	f.finishCleanupJob(t, batchv1.JobComplete, 0, "")
	drainEvents(f.rec)
	if done, _ := f.finalize(t); !done {
		t.Fatal("not released once the cleanup succeeded")
	}
	events := drainEvents(f.rec)
	if len(events) != 1 || !strings.Contains(events[0], "ArtifactCleanupSucceeded") || !strings.Contains(events[0], "changed to Retain after the cleanup started") {
		t.Fatalf("events = %v, want one ArtifactCleanupSucceeded naming the ignored Retain", events)
	}
}

// TestFinalize_LateRetainAfterFailedCleanupIsNotApplied (K009): between a
// failed cleanup Job's deletion and its replacement no Job exists, but the
// ArtifactCleanup condition records the failed run: Retain still does not
// apply.
func TestFinalize_LateRetainAfterFailedCleanupIsNotApplied(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.r.setArtifactCleanupCondition(ctx, f.mb, v1alpha1.ReasonCleanupFailed, "cleanup Job x failed: boom"); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := f.c.Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	drainEvents(f.rec)
	if done, _ := f.finalize(t); done {
		t.Fatal("a late Retain released a backup whose cleanup already ran")
	}
	if n := countEvents(f.rec, "ArtifactRetained"); n != 0 {
		t.Fatal("ArtifactRetained emitted although a cleanup had run")
	}
	if _, ok := f.cleanupJob(t); !ok {
		t.Fatal("the cleanup was not resumed")
	}
}

// TestFinalize_RetainRecordsLostDestinationBeforeStoppingProducer (K011):
// a Retain backup that never completed and whose launch status was lost
// records its destination from the producing Job before stopping (and so
// deleting) it, so the retained status and the ArtifactRetained Event name
// where the leftovers are.
func TestFinalize_RetainRecordsLostDestinationBeforeStoppingProducer(t *testing.T) {
	r, c, key := runningBackup(t, "retain-lost-launch")
	ctx := context.Background()
	want := readBackupByKey(t, c, key).Status.ArtifactStorage.Location
	loseLaunchAttribution(t, c, key)
	b := readBackupByKey(t, c, key)
	b.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := c.Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(50)
	r.Recorder = rec
	for i := 0; i < 5; i++ {
		b = readBackupByKey(t, c, key)
		done, _, err := r.finalize(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	got := readBackupByKey(t, c, key)
	if got.Status.ArtifactStorage == nil || got.Status.ArtifactStorage.Location != want {
		t.Fatalf("artifactStorage = %+v, want the producing Job's destination %q", got.Status.ArtifactStorage, want)
	}
	events := drainEvents(rec)
	found := false
	for _, e := range events {
		if strings.Contains(e, "ArtifactRetained") && strings.Contains(e, want) {
			found = true
		}
	}
	if !found {
		t.Fatalf("events = %v, want ArtifactRetained naming %q", events, want)
	}
}

// TestFinalize_LateRetainAfterCleanupJobRemovedIsNotApplied (K009,
// verification V004): a cleanup Job deleted with Orphan propagation leaves
// its pod deleting with no Job to find and no ArtifactCleanup reason
// recorded. status.cleanupStartTime, written before the Job was created,
// still marks the cleanup as started: a late Retain is not applied and
// ArtifactRetained is not emitted while that pod may still write. The
// cleanup waits for the pod, then resumes with a new Job.
func TestFinalize_LateRetainAfterCleanupJobRemovedIsNotApplied(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if done, _ := f.finalize(t); done {
		t.Fatal("released before the cleanup ran")
	}
	job, ok := f.cleanupJob(t)
	if !ok {
		t.Fatal("setup: no cleanup Job")
	}
	job.UID = types.UID("cleanup-uid")
	pod := cleanupPodFor(job, corev1.PodRunning, 0, "")
	pod.Status.ContainerStatuses = nil
	if err := f.c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	orphan := metav1.DeletePropagationOrphan
	if err := f.c.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &orphan}); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := f.c.Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	drainEvents(f.rec)
	if done, _ := f.finalize(t); done {
		t.Fatal("a late Retain released the backup while its orphaned cleanup pod still runs")
	}
	if n := countEvents(f.rec, "ArtifactRetained"); n != 0 {
		t.Fatal("ArtifactRetained emitted while a cleanup pod may still delete the artifact")
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("a replacement cleanup Job was created while the orphaned pod still runs")
	}
	pod.Status.Phase = corev1.PodSucceeded
	if err := f.c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if done, _ := f.finalize(t); done {
		t.Fatal("released without finishing the cleanup")
	}
	if _, ok := f.cleanupJob(t); !ok {
		t.Fatal("the cleanup did not resume once the orphaned pod stopped")
	}
	if n := countEvents(f.rec, "ArtifactRetained"); n != 0 {
		t.Fatal("ArtifactRetained emitted although the cleanup had started")
	}
	if f.mb.Status.CleanupStartTime == nil {
		t.Fatal("status.cleanupStartTime not recorded")
	}
}

// TestFinalize_RetainBeforeCleanupStartWinsTheRace (K009, V004): a switch
// to Retain that lands after the finalize pass read the backup but before
// it recorded the cleanup start makes the cleanupStartTime write conflict,
// so no cleanup Job is created and the next pass applies Retain.
func TestFinalize_RetainBeforeCleanupStartWinsTheRace(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	var edited v1alpha1.MysqlBackup
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), &edited); err != nil {
		t.Fatal(err)
	}
	edited.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := f.c.Update(ctx, &edited); err != nil {
		t.Fatal(err)
	}
	// f.mb is the pass's stale read, still Delete.
	done, wait, err := f.r.finalize(ctx, f.mb)
	if err != nil || done || wait == 0 {
		t.Fatalf("finalize on a stale Delete read = done %v wait %s err %v, want a requeue", done, wait, err)
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("a cleanup Job was created against a spec that had changed to Retain")
	}
	drainEvents(f.rec)
	if done, _ := f.finalize(t); !done {
		t.Fatal("Retain was not applied on the next pass")
	}
	if n := countEvents(f.rec, "ArtifactRetained"); n != 1 {
		t.Fatal("no ArtifactRetained for the Retain that won the race")
	}
}

// TestFinalize_RetainWaitsForForeignCleanupPod (V004): with no cleanup
// started by this deletion, Retain still does not report the artifact as
// left in storage while a cleanup pod of the same backup name is running.
func TestFinalize_RetainWaitsForForeignCleanupPod(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	f.mb.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := f.c.Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	pod := cleanupPodFor(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "earlier-cleanup", Namespace: "ns",
		Labels: map[string]string{v1alpha1.MysqlBackupLabel: f.mb.Name}, UID: "earlier-uid"}}, corev1.PodRunning, 0, "")
	pod.Status.ContainerStatuses = nil
	if err := f.c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	drainEvents(f.rec)
	if done, _ := f.finalize(t); done {
		t.Fatal("Retain released the backup while a cleanup pod still runs")
	}
	if n := countEvents(f.rec, "ArtifactRetained"); n != 0 {
		t.Fatal("ArtifactRetained emitted while a cleanup pod may still write")
	}
	pod.Status.Phase = corev1.PodSucceeded
	if err := f.c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if done, _ := f.finalize(t); !done {
		t.Fatal("Retain not applied once the pod stopped")
	}
	if n := countEvents(f.rec, "ArtifactRetained"); n != 1 {
		t.Fatal("no ArtifactRetained once the pod stopped")
	}
}
