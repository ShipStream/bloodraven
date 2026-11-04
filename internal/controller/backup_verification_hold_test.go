package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const holdBackupUID = "uid-held"

func heldBackup(holders ...*v1alpha1.MysqlBackupVerification) *v1alpha1.MysqlBackup {
	b := successfulBackup("held", "lion", "nightly-s3")
	b.UID = holdBackupUID
	for _, v := range holders {
		if b.Annotations == nil {
			b.Annotations = map[string]string{}
		}
		b.Annotations[verificationHoldKey(v)] = v.Name
	}
	return b
}

func holdVerification(name string, phase v1alpha1.VerificationPhase, refUID string) *v1alpha1.MysqlBackupVerification {
	v := verifyCR(name, "lion", "nightly-s3")
	v.UID = types.UID("uid-" + name)
	v.Status.Phase = phase
	if refUID != "" {
		v.Status.BackupRef = &v1alpha1.VerificationBackupRef{Name: "held", UID: refUID, Location: "lion/nightly/held/", StorageType: v1alpha1.BackupStorageS3}
	}
	return v
}

func holdJob(verification string, condition batchv1.JobConditionType) *batchv1.Job {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: verificationJobName(verification), Namespace: "ns"}}
	if condition != "" {
		job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue}}
	}
	return job
}

func holdPod(verification string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: verification + "-pod-" + strings.ToLower(string(phase)), Namespace: "ns",
			Labels: map[string]string{labelMysqlBackupVerification: verification}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func TestArtifactReaders(t *testing.T) {
	running := holdVerification("running", v1alpha1.VerificationPhaseRestoring, holdBackupUID)
	failed := holdVerification("failed", v1alpha1.VerificationPhaseFailed, holdBackupUID)
	done := holdVerification("done", v1alpha1.VerificationPhaseSucceeded, holdBackupUID)
	gone := holdVerification("gone", v1alpha1.VerificationPhaseRestoring, holdBackupUID)
	successor := holdVerification("again", v1alpha1.VerificationPhaseSucceeded, "uid-other")
	predecessor := successor.DeepCopy()
	predecessor.UID = "uid-again-previous"

	tests := []struct {
		name string
		objs []client.Object
		want string
	}{
		{name: "stamped, not yet running (no Job)", objs: []client.Object{heldBackup(running), running}, want: "running"},
		{name: "stamped, Failed while its Job still runs", objs: []client.Object{heldBackup(failed), failed, holdJob("failed", ""), holdPod("failed", corev1.PodRunning)}, want: "failed"},
		{name: "stamped, Failed Job with a pod still running", objs: []client.Object{heldBackup(failed), failed, holdJob("failed", batchv1.JobFailed), holdPod("failed", corev1.PodRunning)}, want: "failed"},
		{name: "stamped, finished and quiescent", objs: []client.Object{heldBackup(done), done, holdJob("done", batchv1.JobComplete), holdPod("done", corev1.PodSucceeded)}, want: ""},
		{name: "stamped, Job already deleted", objs: []client.Object{heldBackup(done), done}, want: ""},
		{name: "holder deleted, orphan pod running", objs: []client.Object{heldBackup(gone), holdPod("gone", corev1.PodRunning)}, want: "gone"},
		{name: "holder deleted, nothing running", objs: []client.Object{heldBackup(gone)}, want: ""},
		{name: "same-name successor, previous run stopped", objs: []client.Object{heldBackup(predecessor), successor}, want: ""},
		{name: "unstamped running verification (older operator)", objs: []client.Object{heldBackup(), running}, want: "running"},
		{name: "unstamped verification of another backup", objs: []client.Object{heldBackup(), holdVerification("other", v1alpha1.VerificationPhaseRestoring, "uid-other")}, want: ""},
		{name: "not yet pinned, same group and profile", objs: []client.Object{heldBackup(), holdVerification("pending", "", "")}, want: "pending"},
		{name: "not yet pinned, other profile", objs: []client.Object{heldBackup(), func() client.Object {
			v := holdVerification("pending", "", "")
			v.Spec.ProfileName = "daily-local"
			return v
		}()}, want: ""},
		{name: "not yet pinned to another backup by name", objs: []client.Object{heldBackup(), func() client.Object {
			v := holdVerification("pending", "", "")
			v.Spec.BackupRef = &corev1.LocalObjectReference{Name: "another"}
			return v
		}()}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(tc.objs...).Build()
			var backup v1alpha1.MysqlBackup
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "held"}, &backup); err != nil {
				t.Fatal(err)
			}
			got, err := artifactReaders(context.Background(), c, &backup)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, ",") != tc.want {
				t.Fatalf("readers = %v, want %q", got, tc.want)
			}
		})
	}
}

func TestArtifactReaders_FailsClosedOnAPIErrors(t *testing.T) {
	failed := holdVerification("failed", v1alpha1.VerificationPhaseFailed, holdBackupUID)
	for _, kind := range []string{"pods", "verifications", "jobs"} {
		t.Run(kind, func(t *testing.T) {
			base := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(heldBackup(failed), failed).Build()
			c := interceptor.NewClient(base, interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.PodList); ok && kind == "pods" {
						return errors.New("injected")
					}
					if _, ok := list.(*v1alpha1.MysqlBackupVerificationList); ok && kind == "verifications" {
						return errors.New("injected")
					}
					return c.List(ctx, list, opts...)
				},
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*batchv1.Job); ok && kind == "jobs" {
						return errors.New("injected")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			if _, err := artifactReaders(context.Background(), c, heldBackup(failed)); err == nil {
				t.Fatal("an API error was treated as no readers")
			}
		})
	}
}

func TestPruneSuccessful_SkipsBackupsBeingRead(t *testing.T) {
	now := time.Now()
	backups := makeBackups(t, now, 0, time.Hour, 2*time.Hour)
	for i := range backups {
		backups[i].UID = types.UID("uid-" + backups[i].Name)
	}
	reader := holdVerification("reader", v1alpha1.VerificationPhaseRestoring, "uid-b-b")
	objs := append(asClientObjects(backups), reader)
	r, _ := newBackupReconciler(t, objs...)
	if err := r.pruneSuccessful(context.Background(), backups, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	// b-b is out of the count window but is being verified; b-c is not.
	assertEqualSet(t, listBackupNames(t, r), []string{"b-a", "b-b"})
	if n := countEvents(r.Recorder.(*record.FakeRecorder), "RetentionDeferred"); n != 1 {
		t.Fatalf("RetentionDeferred events = %d, want 1", n)
	}
}

func TestPruneSuccessful_DoesNotDeleteSameNameReplacement(t *testing.T) {
	now := time.Now()
	backups := makeBackups(t, now, 0, time.Hour)
	for i := range backups {
		backups[i].UID = types.UID("uid-" + backups[i].Name)
	}
	r, c := newBackupReconciler(t, asClientObjects(backups)...)
	// The cached candidate for b-b is stale: the live object is a new
	// incarnation with another UID.
	stale := backups
	stale[1].UID = "uid-b-b-previous"
	if err := r.pruneSuccessful(context.Background(), stale, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	var live v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "b-b"}, &live); err != nil {
		t.Fatalf("replacement was deleted: %v", err)
	}
}

func TestResolveBackup_SkipsTerminatingBackups(t *testing.T) {
	fg := verifyFG()
	older := successfulBackup("older", fg.Name, "nightly-s3")
	newer := successfulBackup("newer", fg.Name, "nightly-s3")
	newer.Status.CompletionTime = &metav1.Time{Time: older.Status.CompletionTime.Add(time.Hour)}
	now := metav1.Now()
	newer.DeletionTimestamp = &now
	newer.Finalizers = []string{mysqlBackupFinalizer}

	latest := verifyCR("latest", fg.Name, "nightly-s3")
	explicit := verifyCR("explicit", fg.Name, "nightly-s3")
	explicit.Spec.BackupRef = &corev1.LocalObjectReference{Name: "newer"}
	r, _ := newVerificationReconciler(t, fg, older, newer)

	got, err := r.resolveBackup(context.Background(), latest)
	if err != nil || got.Name != "older" {
		t.Fatalf("latest resolved to %v, %v; want older", got, err)
	}
	if _, err := r.resolveBackup(context.Background(), explicit); err == nil || !strings.Contains(err.Error(), "being deleted") {
		t.Fatalf("explicit ref to a Terminating backup: %v", err)
	}
}

func TestVerificationHold_StampedOnLaunchAndReleasedWhenStopped(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	b := successfulBackup("backup-a", fg.Name, "nightly-s3")
	v := verifyCR("hold", fg.Name, "nightly-s3")
	v.UID = "uid-hold"
	r, c := newVerificationReconciler(t, fg, b, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	if b.Annotations[verificationHoldKey(v)] != v.Name {
		t.Fatalf("launch did not stamp the hold: %v", b.Annotations)
	}

	// The run fails while its pod still runs: the hold stays.
	var job batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: verificationJobName(v.Name)}, &job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := c.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	pod := holdPod(v.Name, corev1.PodRunning)
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatal(err)
	}
	if v.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Fatalf("phase %s, want Failed", v.Status.Phase)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	if b.Annotations[verificationHoldKey(v)] == "" {
		t.Fatal("hold released while a verification pod still runs")
	}

	pod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	if _, held := b.Annotations[verificationHoldKey(v)]; held {
		t.Fatal("hold not released after the run stopped")
	}
}

func TestVerificationHold_RefusesTerminatingBackupAtLaunch(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	b := successfulBackup("backup-a", fg.Name, "nightly-s3")
	now := metav1.Now()
	b.DeletionTimestamp = &now
	b.Finalizers = []string{mysqlBackupFinalizer}
	v := verifyCR("late", fg.Name, "nightly-s3")
	v.Spec.BackupRef = &corev1.LocalObjectReference{Name: b.Name}
	r, c := newVerificationReconciler(t, fg, b, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatal(err)
	}
	cond := apiMeta.FindStatusCondition(v.Status.Conditions, ConditionVerified)
	if v.Status.Phase != v1alpha1.VerificationPhaseFailed || cond == nil || cond.Reason != "BackupNotAvailable" {
		t.Fatalf("status %+v, want Failed/BackupNotAvailable", v.Status)
	}
	var jobs batchv1.JobList
	if err := c.List(ctx, &jobs); err != nil || len(jobs.Items) != 0 {
		t.Fatalf("a Job was launched against a backup being deleted: %v %d", err, len(jobs.Items))
	}
}

func TestVerificationHold_DeletionWaitsForPods(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	b := successfulBackup("backup-a", fg.Name, "nightly-s3")
	v := verifyCR("deleting", fg.Name, "nightly-s3")
	v.UID = "uid-deleting"
	r, c := newVerificationReconciler(t, fg, b, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	pod := holdPod(v.Name, corev1.PodRunning)
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, v); err != nil {
		t.Fatal(err)
	}
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatalf("finalizer released while a pod still runs: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil || b.Annotations[verificationHoldKey(v)] == "" {
		t.Fatalf("hold released early: %v %v", err, b.Annotations)
	}
	if err := c.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	if _, held := b.Annotations[verificationHoldKey(v)]; held {
		t.Fatal("hold not released after the verification's pods were gone")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err == nil {
		t.Fatal("finalizer not released")
	}
}

func TestFinalize_DefersWhileVerificationReads(t *testing.T) {
	f := newCleanupFixture(t)
	reader := verifyCR("reader", "lion", "nightly-s3")
	reader.Status.Phase = v1alpha1.VerificationPhaseRestoring
	reader.Status.BackupRef = &v1alpha1.VerificationBackupRef{Name: f.mb.Name, UID: string(f.mb.UID), Location: f.mb.Status.Location, StorageType: v1alpha1.BackupStorageS3}
	if err := f.c.Create(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if done, requeue := f.finalize(t); done || requeue != 30*time.Second {
			t.Fatalf("done=%v requeue=%s, want a 30s deferral", done, requeue)
		}
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("cleanup started while a verification reads the artifact")
	}
	cond := cleanupCondition(t, f)
	if cond == nil || cond.Reason != cleanupReasonHeldByVerification || !strings.Contains(cond.Message, "reader") {
		t.Fatalf("condition %+v", cond)
	}
	if n := countEvents(f.rec, "ArtifactCleanupDeferred"); n != 1 {
		t.Fatalf("ArtifactCleanupDeferred events = %d, want 1", n)
	}

	reader.Status.Phase = v1alpha1.VerificationPhaseSucceeded
	// This client has no status subresource for verifications.
	if err := f.c.Update(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	f.finalize(t)
	if _, ok := f.cleanupJob(t); !ok {
		t.Fatal("cleanup did not start after the verification finished")
	}
}

// A crash after the pin was persisted but before the hold was stamped: the
// next pass stamps the same pinned backup before launching.
func TestVerificationHold_StampedAfterCrashBetweenPinAndStamp(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	pinned := successfulBackup("backup-a", fg.Name, "nightly-s3")
	newer := successfulBackup("backup-b", fg.Name, "nightly-s3")
	newer.Status.CompletionTime = &metav1.Time{Time: pinned.Status.CompletionTime.Add(time.Hour)}
	v := verifyCR("crashed", fg.Name, "nightly-s3")
	v.UID = "uid-crashed"
	v.Status.Phase = v1alpha1.VerificationPhaseProvisioning
	v.Status.BackupRef = verificationBackupSnapshot(pinned)
	r, c := newVerificationReconciler(t, fg, pinned, newer, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	for _, b := range []*v1alpha1.MysqlBackup{pinned, newer} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
			t.Fatal(err)
		}
	}
	if pinned.Annotations[verificationHoldKey(v)] != v.Name || newer.Annotations[verificationHoldKey(v)] != "" {
		t.Fatalf("hold not on the pinned backup: pinned=%v newer=%v", pinned.Annotations, newer.Annotations)
	}
}

// A release that loses a resourceVersion race is retried, not dropped.
func TestVerificationHold_ReleaseConflictIsRetried(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	b := successfulBackup("backup-a", fg.Name, "nightly-s3")
	v := verifyCR("conflict", fg.Name, "nightly-s3")
	v.UID = "uid-conflict"
	r, c := newVerificationReconciler(t, fg, b, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: verificationJobName(v.Name)}, &job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := c.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	conflicts := 1
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*v1alpha1.MysqlBackup); ok && conflicts > 0 {
				conflicts--
				return apierrors.NewConflict(schema.GroupResource{Resource: "mysqlbackups"}, obj.GetName(), errors.New("injected"))
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	r.APIReader = c
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	if _, held := b.Annotations[verificationHoldKey(v)]; !held || res.RequeueAfter == 0 {
		t.Fatalf("conflicting release: held=%v requeue=%s", held, res.RequeueAfter)
	}
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	if _, held := b.Annotations[verificationHoldKey(v)]; held {
		t.Fatal("release was not retried after a conflict")
	}
}

// A crash after the verification Job was created but before status.jobName
// and the phase were written: the observe path stamps the hold on the next
// pass, before anything else could prune the backup.
func TestVerificationHold_StampedOnObserveAfterCrash(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	b := successfulBackup("backup-a", fg.Name, "nightly-s3")
	v := verifyCR("observe", fg.Name, "nightly-s3")
	v.UID = "uid-observe"
	r, c := newVerificationReconciler(t, fg, b, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	// Rewind to the crash point: Job exists, status and hold do not.
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	b.Annotations = nil
	if err := c.Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatal(err)
	}
	v.Status.JobName, v.Status.Phase = "", v1alpha1.VerificationPhaseProvisioning
	if err := c.Status().Update(ctx, v); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(b), b); err != nil {
		t.Fatal(err)
	}
	if b.Annotations[verificationHoldKey(v)] != v.Name {
		t.Fatalf("observe path did not stamp the hold: %v", b.Annotations)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil || v.Status.JobName == "" || v.Status.Phase != v1alpha1.VerificationPhaseRestoring {
		t.Fatalf("status not recovered: %+v %v", v.Status, err)
	}
}

func TestPruneSuccessful_LogsHeldBackup(t *testing.T) {
	now := time.Now()
	backups := makeBackups(t, now, 0, time.Hour)
	for i := range backups {
		backups[i].UID = types.UID("uid-" + backups[i].Name)
	}
	reader := holdVerification("reader", v1alpha1.VerificationPhaseRestoring, "uid-b-b")
	r, _ := newBackupReconciler(t, append(asClientObjects(backups), reader)...)
	var lines []string
	logger := funcr.New(func(prefix, args string) { lines = append(lines, args) }, funcr.Options{})
	ctx := log.IntoContext(context.Background(), logger)
	if err := r.pruneSuccessful(ctx, backups, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if strings.Contains(line, `"msg"="backup prune held by verification"`) &&
			strings.Contains(line, `"backup"="b-b"`) && strings.Contains(line, `"backupUID"="uid-b-b"`) &&
			strings.Contains(line, `"verifications"=["reader"]`) && strings.Contains(line, `"verificationUIDs"=["uid-reader"]`) {
			return
		}
	}
	t.Fatalf("missing structured log line; got %q", lines)
}
