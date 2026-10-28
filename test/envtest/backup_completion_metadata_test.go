//go:build envtest

package envtest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestEnvtest_BackupCompletionMetadataRecoversAfterRestart(t *testing.T) {
	ns := createNamespace(t, "backup-completion-metadata")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	fg.Spec.Backup.Profiles[0].Storage.S3.Prefix = "prod_backups/db dumps"
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
	recorder := record.NewFakeRecorder(20)
	r := &controller.MysqlBackupReconciler{Client: k8sClient, Scheme: scheme, Recorder: recorder}
	for i := 0; i < 5; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: backup.Status.JobName}, &job); err != nil {
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
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "completed", Namespace: ns, Labels: job.Spec.Template.Labels}, Spec: job.Spec.Template.Spec}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodSucceeded
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	location := "prod_backups/db dumps/" + backup.Name
	var available atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !available.Load() {
			http.Error(w, "log outage", 500)
			return
		}
		_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: location, SizeBytes: 42, GtidExecuted: "uuid:my_tag:1-3"}))
	}))
	defer server.Close()
	logClient, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	r.Clientset = logClient
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	if backup.Status.Phase != v1alpha1.BackupPhaseSucceeded || backup.Status.Location != location || result.RequeueAfter <= 0 {
		t.Fatalf("successful artifact lost during log outage: result=%+v status=%+v", result, backup.Status)
	}
	completion := backup.Status.CompletionTime.DeepCopy()
	// Reproduce a historical Succeeded record with no location, then restart.
	backup.Status.Location = ""
	if err := k8sClient.Status().Update(ctx, backup); err != nil {
		t.Fatal(err)
	}
	available.Store(true)
	r = &controller.MysqlBackupReconciler{Client: k8sClient, Scheme: scheme, Recorder: recorder, Clientset: logClient}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, backup); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(backup.Status.Conditions, controller.ConditionBackupMetadataComplete)
	if backup.Status.Location != location || backup.Status.SizeBytes != 42 || backup.Status.GtidExecuted != "uuid:my_tag:1-3" || condition == nil || condition.Status != metav1.ConditionTrue || !backup.Status.CompletionTime.Equal(completion) {
		t.Fatalf("completion metadata was not durably recovered: %+v", backup.Status)
	}
}
