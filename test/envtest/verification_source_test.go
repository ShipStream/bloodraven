//go:build envtest

package envtest

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestEnvtest_VerificationSourcePinnedAcrossLaunchAndCompletion(t *testing.T) {
	ns := createNamespace(t, "verification-source-pin")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	a := newSucceededBackup(ns, fg.Name, "nightly-s3")
	a.Name = "backup-a"
	if err := k8sClient.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	a.Status = v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded, Location: "prod_backups/a", StorageType: v1alpha1.BackupStorageS3, CompletionTime: &metav1.Time{Time: time.Now().Add(-time.Hour)}}
	if err := k8sClient.Status().Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "pinned", Namespace: ns}, Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name}, ProfileName: "nightly-s3", TTLSecondsAfterFinished: 3600}}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(v)
	r := &controller.MysqlBackupVerificationReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	reconcileUntil(t, r, key, func() bool {
		if err := k8sClient.Get(ctx, key, v); err != nil {
			t.Fatal(err)
		}
		return v.Status.Phase == v1alpha1.VerificationPhaseProvisioning
	})
	want := &v1alpha1.VerificationBackupRef{Name: a.Name, UID: string(a.UID), Location: a.Status.Location, StorageType: a.Status.StorageType}
	b := newSucceededBackup(ns, fg.Name, "nightly-s3")
	b.Name = "backup-b"
	if err := k8sClient.Create(ctx, b); err != nil {
		t.Fatal(err)
	}
	b.Status = a.Status
	b.Status.Location = "prod_backups/b"
	b.Status.CompletionTime = &metav1.Time{Time: time.Now()}
	if err := k8sClient.Status().Update(ctx, b); err != nil {
		t.Fatal(err)
	}
	r = &controller.MysqlBackupVerificationReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	reconcileUntil(t, r, key, func() bool {
		if err := k8sClient.Get(ctx, key, v); err != nil {
			t.Fatal(err)
		}
		return v.Status.Phase == v1alpha1.VerificationPhaseRestoring
	})
	var job batchv1.Job
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: v.Status.JobName}, &job); err != nil {
		t.Fatal(err)
	}
	var snapshot v1alpha1.VerificationBackupRef
	if err := json.Unmarshal([]byte(job.Spec.Template.Annotations["shipstream.io/verification-backup"]), &snapshot); err != nil || !reflect.DeepEqual(&snapshot, want) {
		t.Fatalf("Job snapshot does not match persisted pin: err=%v snapshot=%+v want=%+v", err, snapshot, want)
	}
	modified := job.DeepCopy()
	modified.Spec.Template.Annotations["shipstream.io/verification-backup"] = "{}"
	if err := k8sClient.Update(ctx, modified); err == nil {
		t.Fatal("API server allowed the execution source snapshot to change")
	}
	if err := k8sClient.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), fg); err != nil {
		t.Fatal(err)
	}
	fg.Spec.Backup = nil
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"mysql-credentials", "mysqlverify-pinned-creds"} {
		if err := k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	v.Spec.BackupRef = &corev1.LocalObjectReference{Name: b.Name}
	if err := k8sClient.Update(ctx, v); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, v); err != nil {
		t.Fatal(err)
	}
	if v.Status.Phase != v1alpha1.VerificationPhaseRestoring || !reflect.DeepEqual(v.Status.BackupRef, want) {
		t.Fatalf("running Job lost original source: %+v", v.Status)
	}
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now}, {Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now}}
	if err := k8sClient.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, v); err != nil {
		t.Fatal(err)
	}
	if v.Status.Phase != v1alpha1.VerificationPhaseSucceeded || !reflect.DeepEqual(v.Status.BackupRef, want) {
		t.Fatalf("verification certified the wrong persisted identity: %+v", v.Status)
	}
}

func TestEnvtest_VerificationRejectsRecreatedSourceBeforeLaunch(t *testing.T) {
	ns := createNamespace(t, "verification-source-replaced")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	a := newSucceededBackup(ns, fg.Name, "nightly-s3")
	if err := k8sClient.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	a.Status = v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded, Location: "prod_backups/a", StorageType: v1alpha1.BackupStorageS3, CompletionTime: &metav1.Time{Time: time.Now().Add(-time.Hour)}}
	if err := k8sClient.Status().Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "pinned", Namespace: ns}, Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name}, ProfileName: "nightly-s3"}}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(v)
	r := &controller.MysqlBackupVerificationReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	reconcileUntil(t, r, key, func() bool {
		if err := k8sClient.Get(ctx, key, v); err != nil {
			t.Fatal(err)
		}
		return v.Status.Phase == v1alpha1.VerificationPhaseProvisioning
	})
	if err := k8sClient.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	replacement := newSucceededBackup(ns, fg.Name, "nightly-s3")
	if err := k8sClient.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	replacement.Status = a.Status
	if err := k8sClient.Status().Update(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, v); err != nil {
		t.Fatal(err)
	}
	if v.Status.Phase != v1alpha1.VerificationPhaseFailed || v.Status.BackupRef.UID != string(a.UID) || v.Status.BackupRef.UID == string(replacement.UID) {
		t.Fatalf("recreated source was accepted: %+v", v.Status)
	}
	var job batchv1.Job
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: "mysqlverify-pinned"}, &job); !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected Job after source replacement: %v", err)
	}
}
