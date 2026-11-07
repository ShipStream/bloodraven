//go:build envtest

package envtest

import (
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const holdAnnotationPrefix = "hold.verification.shipstream.io/"

func hasHold(b *v1alpha1.MysqlBackup) bool {
	for key := range b.Annotations {
		if strings.HasPrefix(key, holdAnnotationPrefix) {
			return true
		}
	}
	return false
}

func createHeldTestBackup(t *testing.T, ns, fgName, name string, completed time.Time) *v1alpha1.MysqlBackup {
	t.Helper()
	b := newSucceededBackup(ns, fgName, "nightly-s3")
	b.Name = name
	b.Finalizers = []string{"shipstream.io/mysqlbackup"}
	if err := k8sClient.Create(ctx, b); err != nil {
		t.Fatal(err)
	}
	done := metav1.NewTime(completed)
	b.Status = v1alpha1.MysqlBackupStatus{
		Phase: v1alpha1.BackupPhaseSucceeded, CompletionTime: &done, StartTime: &done,
		ArtifactStorage: &v1alpha1.BackupArtifactStorage{Type: v1alpha1.BackupStorageS3, Location: "lion/" + name, Bucket: "bloodraven"},
	}
	if err := k8sClient.Status().Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	return b
}

// TestEnvtest_VerificationHoldOrdersAgainstDelete pins the API semantics the
// hold relies on: a hold stamped after retention read the backup makes the
// delete fail its precondition, and a delete that lands first makes the
// stamp's optimistic lock fail.
func TestEnvtest_VerificationHoldOrdersAgainstDelete(t *testing.T) {
	ns := createNamespace(t, "verification-hold-order")
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}

	stampFirst := createHeldTestBackup(t, ns, fg.Name, "stamp-first", time.Now())
	retentionView := stampFirst.DeepCopy()
	patch := client.MergeFromWithOptions(stampFirst.DeepCopy(), client.MergeFromWithOptimisticLock{})
	stampFirst.Annotations = map[string]string{holdAnnotationPrefix + "uid-v": "v"}
	if err := k8sClient.Patch(ctx, stampFirst, patch); err != nil {
		t.Fatal(err)
	}
	uid, rv := retentionView.UID, retentionView.ResourceVersion
	if err := k8sClient.Delete(ctx, retentionView, client.Preconditions{UID: &uid, ResourceVersion: &rv}); !apierrors.IsConflict(err) {
		t.Fatalf("delete with a pre-stamp resourceVersion: %v, want Conflict", err)
	}

	deleteFirst := createHeldTestBackup(t, ns, fg.Name, "delete-first", time.Now())
	verificationView := deleteFirst.DeepCopy()
	if err := k8sClient.Delete(ctx, deleteFirst); err != nil {
		t.Fatal(err)
	}
	patch = client.MergeFromWithOptions(verificationView.DeepCopy(), client.MergeFromWithOptimisticLock{})
	verificationView.Annotations = map[string]string{holdAnnotationPrefix + "uid-v": "v"}
	if err := k8sClient.Patch(ctx, verificationView, patch); !apierrors.IsConflict(err) {
		t.Fatalf("stamp with a pre-delete resourceVersion: %v, want Conflict", err)
	}
}

// TestEnvtest_VerificationHoldLifecycle runs both reconcilers against a
// real API server: a manual delete during a run is deferred, a run that
// fails while its pod still runs keeps the hold, and cleanup starts only
// after the pod stops and the hold is released.
func TestEnvtest_VerificationHoldLifecycle(t *testing.T) {
	controller.SetOperatorImageDefaults("registry.test/bloodraven:envtest", "bloodraven")
	t.Cleanup(func() { controller.SetOperatorImageDefaults("", "") })
	ns := createNamespace(t, "verification-hold-lifecycle")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	backup := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-read", time.Now())
	v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: ns}, Spec: v1alpha1.MysqlBackupVerificationSpec{
		FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fg.Name}, ProfileName: "nightly-s3",
		BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: backup.Name},
	}}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	vr := &controller.MysqlBackupVerificationReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}
	vKey, bKey := client.ObjectKeyFromObject(v), client.ObjectKeyFromObject(backup)
	// Every later step uses a fresh reconciler instance, as after an
	// operator restart: all state the hold needs lives in the API server.
	reconcileBackup := func() {
		t.Helper()
		fresh := &controller.MysqlBackupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}
		if _, err := fresh.Reconcile(ctx, ctrl.Request{NamespacedName: bKey}); err != nil {
			t.Fatalf("backup reconcile: %v", err)
		}
	}
	reconcileVerification := func() {
		t.Helper()
		fresh := &controller.MysqlBackupVerificationReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}
		if _, err := fresh.Reconcile(ctx, ctrl.Request{NamespacedName: vKey}); err != nil {
			t.Fatalf("verification reconcile: %v", err)
		}
	}
	cleanupKey := client.ObjectKey{Namespace: ns, Name: "mysqlcleanup-" + backup.Name}

	reconcileUntil(t, vr, vKey, func() bool {
		if err := k8sClient.Get(ctx, vKey, v); err != nil {
			t.Fatal(err)
		}
		return v.Status.Phase == v1alpha1.VerificationPhaseRestoring
	})
	if err := k8sClient.Get(ctx, bKey, backup); err != nil || !hasHold(backup) {
		t.Fatalf("launch did not stamp the hold: %v %v", err, backup.Annotations)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "reader-pod", Namespace: ns, Labels: map[string]string{"shipstream.io/mysqlbackup-verification": v.Name}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "verify", Image: "verify"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}

	// A manual delete during the run is deferred.
	if err := k8sClient.Delete(ctx, backup); err != nil {
		t.Fatal(err)
	}
	reconcileBackup()
	var cleanup batchv1.Job
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup Job created while a verification reads the artifact: %v", err)
	}
	if err := k8sClient.Get(ctx, bKey, backup); err != nil {
		t.Fatal(err)
	}
	if cond := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup); cond == nil || cond.Reason != "HeldByVerification" {
		t.Fatalf("ArtifactCleanup condition = %+v", cond)
	}

	// The run fails while its pod is still running: the hold stays.
	var job batchv1.Job
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: v.Status.JobName}, &job); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded"},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded"},
	}
	if err := k8sClient.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	reconcileVerification()
	reconcileVerification()
	if err := k8sClient.Get(ctx, vKey, v); err != nil || v.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Fatalf("verification phase %s (%v), want Failed", v.Status.Phase, err)
	}
	if err := k8sClient.Get(ctx, bKey, backup); err != nil || !hasHold(backup) {
		t.Fatalf("hold released while the verification pod still runs: %v", err)
	}
	reconcileBackup()
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup started while the failed run's pod still runs: %v", err)
	}

	// The pod stops: the hold is released and cleanup starts.
	pod.Status.Phase = corev1.PodFailed
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	reconcileVerification()
	if err := k8sClient.Get(ctx, bKey, backup); err != nil || hasHold(backup) {
		t.Fatalf("hold not released after the run stopped: %v %v", err, backup.Annotations)
	}
	reconcileBackup()
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); err != nil {
		t.Fatalf("cleanup did not start after the hold was released: %v", err)
	}
}

// TestEnvtest_RetentionHoldsUnstampedRun covers a verification started by
// an operator that did not stamp holds: retention finds it by scanning,
// and the run's next reconcile stamps the hold.
func TestEnvtest_RetentionHoldsUnstampedRun(t *testing.T) {
	ns := createNamespace(t, "verification-hold-upgrade")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 1}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	older := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-older", time.Now().Add(-2*time.Hour))
	v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: ns}, Spec: v1alpha1.MysqlBackupVerificationSpec{
		FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fg.Name}, ProfileName: "nightly-s3",
		BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: older.Name},
	}}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	vr := &controller.MysqlBackupVerificationReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}
	vKey, oKey := client.ObjectKeyFromObject(v), client.ObjectKeyFromObject(older)
	reconcileUntil(t, vr, vKey, func() bool {
		if err := k8sClient.Get(ctx, vKey, v); err != nil {
			t.Fatal(err)
		}
		return v.Status.Phase == v1alpha1.VerificationPhaseRestoring
	})
	// Simulate the older operator: no hold on the backup.
	if err := k8sClient.Get(ctx, oKey, older); err != nil {
		t.Fatal(err)
	}
	older.Annotations = nil
	if err := k8sClient.Update(ctx, older); err != nil {
		t.Fatal(err)
	}

	newer := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-newer", time.Now())
	rec := record.NewFakeRecorder(50)
	br := &controller.MysqlBackupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: rec}
	if _, err := br.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newer)}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil || !older.DeletionTimestamp.IsZero() {
		t.Fatalf("retention deleted a backup an unstamped run is reading: %v deleting=%v", err, older.DeletionTimestamp)
	}
	deferred := false
	for len(rec.Events) > 0 {
		if strings.Contains(<-rec.Events, "RetentionDeferred") {
			deferred = true
		}
	}
	if !deferred {
		t.Fatal("no RetentionDeferred event")
	}

	if _, err := vr.Reconcile(ctx, ctrl.Request{NamespacedName: vKey}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil || !hasHold(older) {
		t.Fatalf("observe did not stamp the hold: %v %v", err, older.Annotations)
	}
}

// TestEnvtest_VerificationDeletionWaitsForForegroundJobDeletion pins that a
// Job under foreground deletion is not "stopped": the Job controller may
// still have a pod creation in flight. The verification keeps its finalizer
// and its hold until the Job is gone.
func TestEnvtest_VerificationDeletionWaitsForForegroundJobDeletion(t *testing.T) {
	ns := createNamespace(t, "verification-hold-foreground")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	backup := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-fg", time.Now())
	v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "deleting", Namespace: ns}, Spec: v1alpha1.MysqlBackupVerificationSpec{
		FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fg.Name}, ProfileName: "nightly-s3",
		BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: backup.Name},
	}}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	vr := &controller.MysqlBackupVerificationReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}
	vKey, bKey := client.ObjectKeyFromObject(v), client.ObjectKeyFromObject(backup)
	reconcileUntil(t, vr, vKey, func() bool {
		if err := k8sClient.Get(ctx, vKey, v); err != nil {
			t.Fatal(err)
		}
		return v.Status.Phase == v1alpha1.VerificationPhaseRestoring
	})
	jobKey := client.ObjectKey{Namespace: ns, Name: v.Status.JobName}

	if err := k8sClient.Delete(ctx, v); err != nil {
		t.Fatal(err)
	}
	// No GC runs in envtest: the foreground-deleted Job keeps its
	// foregroundDeletion finalizer, nonterminal and with no pods visible.
	for range 3 {
		if _, err := vr.Reconcile(ctx, ctrl.Request{NamespacedName: vKey}); err != nil {
			t.Fatal(err)
		}
	}
	var job batchv1.Job
	if err := k8sClient.Get(ctx, jobKey, &job); err != nil || job.DeletionTimestamp == nil {
		t.Fatalf("expected the Job to be pending foreground deletion: %v", err)
	}
	if err := k8sClient.Get(ctx, vKey, v); err != nil {
		t.Fatalf("verification finalizer released while its Job is still being deleted: %v", err)
	}
	if err := k8sClient.Get(ctx, bKey, backup); err != nil || !hasHold(backup) {
		t.Fatalf("hold released while the Job is still being deleted: %v", err)
	}

	// The garbage collector finishes: the Job is gone.
	job.Finalizers = nil
	if err := k8sClient.Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	if _, err := vr.Reconcile(ctx, ctrl.Request{NamespacedName: vKey}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, bKey, backup); err != nil || hasHold(backup) {
		t.Fatalf("hold not released after the Job was gone: %v %v", err, backup.Annotations)
	}
	if err := k8sClient.Get(ctx, vKey, v); !apierrors.IsNotFound(err) {
		t.Fatalf("verification not released after the Job was gone: %v", err)
	}
}

// TestEnvtest_ArtifactStorageImmutable pins the CEL rule on the MysqlBackup
// root: once the artifact location is recorded, status.artifactStorage cannot
// be edited or removed, including by removing or nulling the whole status
// through the status subresource and then writing a different destination.
func TestEnvtest_ArtifactStorageImmutable(t *testing.T) {
	ns := createNamespace(t, "artifact-storage-immutable")
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	b := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-cel", time.Now())
	key := client.ObjectKeyFromObject(b)
	recorded := *b.Status.ArtifactStorage

	moved := b.DeepCopy()
	moved.Status.ArtifactStorage.Location = "elsewhere/" + b.Name
	if err := k8sClient.Status().Update(ctx, moved); err == nil {
		t.Fatal("API server allowed the recorded artifact location to change")
	}
	removed := b.DeepCopy()
	removed.Status.ArtifactStorage = nil
	if err := k8sClient.Status().Update(ctx, removed); err == nil {
		t.Fatal("API server allowed status.artifactStorage to be removed")
	}
	for _, p := range []client.Patch{
		client.RawPatch(types.JSONPatchType, []byte(`[{"op":"remove","path":"/status"}]`)),
		client.RawPatch(types.MergePatchType, []byte(`{"status":null}`)),
		client.RawPatch(types.MergePatchType, []byte(`{"status":{"artifactStorage":null}}`)),
		client.RawPatch(types.MergePatchType, []byte(`{"status":{"artifactStorage":{"location":"team-b/`+b.Name+`","bucket":"other"}}}`)),
	} {
		target := &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: b.Name, Namespace: ns}}
		err := k8sClient.Status().Patch(ctx, target, p)
		if err == nil {
			t.Fatalf("API server accepted status patch %T that drops or replaces the recorded destination", p)
		}
		if !strings.Contains(err.Error(), "status.artifactStorage cannot be changed or removed") {
			t.Fatalf("status patch rejected for another reason: %v", err)
		}
		var got v1alpha1.MysqlBackup
		if err := k8sClient.Get(ctx, key, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.ArtifactStorage == nil || *got.Status.ArtifactStorage != recorded {
			t.Fatalf("recorded destination changed: %+v", got.Status.ArtifactStorage)
		}
	}
	// Unrelated status changes still go through.
	if err := k8sClient.Get(ctx, key, b); err != nil {
		t.Fatal(err)
	}
	b.Status.Message = "still writable"
	if err := k8sClient.Status().Update(ctx, b); err != nil {
		t.Fatalf("unrelated status update rejected: %v", err)
	}
}
