//go:build envtest

package envtest

import (
	"testing"

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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestEnvtest_UnrecoverableHistoricalBackupCanBeDeleted(t *testing.T) {
	ns := createNamespace(t, "backup-unrecoverable-review")
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	backup := newSucceededBackup(ns, fg.Name, "nightly-s3")
	backup.Finalizers = []string{"shipstream.io/mysqlbackup"}
	if err := k8sClient.Create(ctx, backup); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	backup.Status = v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded, CompletionTime: &now, StorageType: v1alpha1.BackupStorageS3}
	if err := k8sClient.Status().Update(ctx, backup); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "mysqlbackup-" + backup.Name, Namespace: ns},
		Spec:       batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "mysqlsh", Image: "mysql/mysql-shell:8.0.34"}}}}},
	}
	if err := controllerutil.SetControllerReference(backup, job, scheme); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	job.Status.StartTime, job.Status.CompletionTime = &now, &now
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now}, {Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now}}
	if err := k8sClient.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	r := &controller.MysqlBackupReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	key := client.ObjectKeyFromObject(backup)
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("historical repair returned a permanent error: %v", err)
		}
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(backup.Status.Conditions, controller.ConditionBackupMetadataComplete)
	if condition == nil || condition.Reason != "DestinationUnavailable" {
		t.Fatalf("missing durable repair outcome: %+v", backup.Status)
	}
	if err := k8sClient.Delete(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("deletion wedged on missing destination: %v", err)
	}
	if err := k8sClient.Get(ctx, key, backup); !apierrors.IsNotFound(err) {
		t.Fatalf("backup finalizer not released: %v", err)
	}
}
