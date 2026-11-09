//go:build envtest

package envtest

import (
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestEnvtest_ArtifactCleanupRetriesFailedJob drives the finalizer against a
// real API server: the cleanup Job (with its pod failure policy) is admitted,
// a failed Job is replaced only after its foreground deletion completes, and
// the replacement is a fresh Job.
func TestEnvtest_ArtifactCleanupRetriesFailedJob(t *testing.T) {
	controller.SetOperatorImageDefaults("registry.test/bloodraven:envtest", "bloodraven")
	t.Cleanup(func() { controller.SetOperatorImageDefaults("", "") })

	ns := createNamespace(t, "backup-artifact-cleanup")
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	backup := newSucceededBackup(ns, fg.Name, "nightly-s3")
	backup.Finalizers = []string{"bloodraven.shipstream.io/mysqlbackup"}
	if err := k8sClient.Create(ctx, backup); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	backup.Status = v1alpha1.MysqlBackupStatus{
		Phase: v1alpha1.BackupPhaseSucceeded, CompletionTime: &now,
		ArtifactStorage: &v1alpha1.BackupArtifactStorage{Type: v1alpha1.BackupStorageS3, Location: "lion/" + backup.Name, Bucket: "bloodraven"},
	}
	if err := k8sClient.Status().Update(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Delete(ctx, backup); err != nil {
		t.Fatal(err)
	}

	r := &controller.MysqlBackupReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}
	key := client.ObjectKeyFromObject(backup)
	jobKey := client.ObjectKey{Namespace: ns, Name: "mysqlcleanup-" + backup.Name}
	reconcile := func() ctrl.Result {
		t.Helper()
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		return res
	}

	reconcile()
	var job batchv1.Job
	if err := k8sClient.Get(ctx, jobKey, &job); err != nil {
		t.Fatalf("cleanup Job not admitted: %v", err)
	}
	if job.Spec.PodFailurePolicy == nil || job.Spec.Template.Spec.Containers[0].Image != "registry.test/bloodraven:envtest" {
		t.Fatalf("unexpected cleanup Job spec: %+v", job.Spec)
	}
	firstUID := job.UID

	// The Job failed long enough ago that its retry is due.
	failedAt := metav1.NewTime(time.Now().Add(-time.Hour))
	job.Status.StartTime = &failedAt
	job.Status.Failed = 3
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: failedAt, Reason: "BackoffLimitExceeded"},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: failedAt, Reason: "BackoffLimitExceeded"},
	}
	if err := k8sClient.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}

	if res := reconcile(); res.RequeueAfter <= 0 {
		t.Fatalf("failed cleanup must requeue, got %+v", res)
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	cond := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if cond == nil || cond.Reason != "CleanupFailed" {
		t.Fatalf("ArtifactCleanup condition = %+v", cond)
	}
	if err := k8sClient.Get(ctx, jobKey, &job); err != nil {
		t.Fatal(err)
	}
	if job.DeletionTimestamp == nil {
		t.Fatal("failed cleanup Job was not deleted for retry")
	}

	// Without a garbage collector the foreground deletion stays pending;
	// no replacement may start meanwhile.
	reconcile()
	var still batchv1.Job
	if err := k8sClient.Get(ctx, jobKey, &still); err != nil || still.UID != firstUID {
		t.Fatalf("replacement started before the old Job was gone: %v", err)
	}

	// Complete the foreground deletion as the garbage collector would.
	still.Finalizers = nil
	if err := k8sClient.Update(ctx, &still); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, jobKey, &still); !apierrors.IsNotFound(err) {
		t.Fatalf("old Job still present: %v", err)
	}
	reconcile()
	var replacement batchv1.Job
	if err := k8sClient.Get(ctx, jobKey, &replacement); err != nil {
		t.Fatalf("replacement Job not created: %v", err)
	}
	if replacement.UID == firstUID {
		t.Fatal("replacement reused the failed Job")
	}
}
