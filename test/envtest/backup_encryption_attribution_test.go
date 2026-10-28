//go:build envtest

package envtest

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestEnvtest_BackupEncryptionAttributionSurvivesProfileChange(t *testing.T) {
	ns := createNamespace(t, "backup-encryption-attribution")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: "writable"}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	backup := newSucceededBackup(ns, fg.Name, "nightly-s3")
	if err := k8sClient.Create(ctx, backup); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(backup)
	r := &controller.MysqlBackupReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	for i := 0; i < 5; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	if backup.Status.Phase != v1alpha1.BackupPhaseRunning || backup.Status.JobName == "" {
		t.Fatalf("expected launched backup, got %+v", backup.Status)
	}
	var job batchv1.Job
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: backup.Status.JobName}, &job); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), fg); err != nil {
		t.Fatal(err)
	}
	fg.Spec.Backup.Profiles[0].Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "new-passphrase"}}
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
	}
	if err := k8sClient.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	if backup.Status.Phase != v1alpha1.BackupPhaseSucceeded || backup.Status.Encrypted || backup.Status.EncryptionAlgorithm != "" {
		t.Fatalf("plaintext Job misattributed after profile change: %+v", backup.Status)
	}
	// Reproduce a historical bad status and repair through a fresh reconciler.
	backup.Status.Encrypted = true
	backup.Status.EncryptionAlgorithm = "AES-256-GCM"
	if err := k8sClient.Status().Update(ctx, backup); err != nil {
		t.Fatal(err)
	}
	r = &controller.MysqlBackupReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	if backup.Status.Encrypted || backup.Status.EncryptionAlgorithm != "" {
		t.Fatalf("historical attribution not repaired: %+v", backup.Status)
	}
}
