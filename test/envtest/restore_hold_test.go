//go:build envtest

package envtest

import (
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Restore holds against a real API server (#275): the pin survives the CRD
// schema, the pin and the stamp are conditional writes the API server
// honours, and retention, artifact cleanup and the release behave as the
// unit tests say with the status subresource and admission in the way.

func restoreHolds(b *v1alpha1.MysqlBackup) []string {
	var keys []string
	for key := range b.Annotations {
		if strings.HasPrefix(key, v1alpha1.RestoreHoldAnnotationPrefix) {
			keys = append(keys, key)
		}
	}
	return keys
}

func retentionDeferredEvents(rec *record.FakeRecorder) []string {
	var out []string
	for len(rec.Events) > 0 {
		if e := <-rec.Events; strings.Contains(e, "RetentionDeferred") {
			out = append(out, e)
		}
	}
	return out
}

// TestEnvtest_RestoreBackupRefStatusSchema pins that both restore statuses
// keep backupRef through the status subresource: a field the CRD schema
// does not know is pruned, and a pruned pin reads as "nothing held".
func TestEnvtest_RestoreBackupRefStatusSchema(t *testing.T) {
	ns := createNamespace(t, "restore-backupref-schema")
	fg := newVerificationFG(ns)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "seed"}}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	want := v1alpha1.RestoreBackupRef{Name: "seed", UID: "5b1f0a3e-0000-4000-8000-000000000275", HoldID: "0d9f1c1e-0000-4000-8000-000000000275"}
	fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending, BackupRef: want.DeepCopy()}
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlacePreflight, StartTime: &now, BackupRef: want.DeepCopy()}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("status update: %v", err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Restore == nil || got.Status.Restore.BackupRef == nil || *got.Status.Restore.BackupRef != want {
		t.Fatalf("status.restore.backupRef = %+v, want %+v", got.Status.Restore, want)
	}
	if got.Status.RestoreInPlace == nil || got.Status.RestoreInPlace.BackupRef == nil || *got.Status.RestoreInPlace.BackupRef != want {
		t.Fatalf("status.restoreInPlace.backupRef = %+v, want %+v", got.Status.RestoreInPlace, want)
	}
}

// TestEnvtest_RestoreHoldLifecycle runs the group and the backup
// reconcilers against a real API server, each step with fresh reconciler
// instances (an operator restart): retention keeps the backup a bootstrap
// restore is about to read and then reads, the restore pins it by UID and
// holds it before its Job exists, a manual delete during the restore is
// deferred, a restore that failed while its pod still runs keeps the hold,
// and cleanup starts only after the pod stopped and the hold was released.
func TestEnvtest_RestoreHoldLifecycle(t *testing.T) {
	controller.SetOperatorImageDefaults("registry.test/bloodraven:envtest", "bloodraven")
	t.Cleanup(func() { controller.SetOperatorImageDefaults("", "") })
	ns := createNamespace(t, "restore-hold-lifecycle")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 1}
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "lion-nightly-older"}}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fgKey := client.ObjectKeyFromObject(fg)
	older := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-older", time.Now().Add(-2*time.Hour))
	newer := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-newer", time.Now())
	oKey := client.ObjectKeyFromObject(older)
	cleanupKey := client.ObjectKey{Namespace: ns, Name: "mysqlcleanup-" + older.Name}

	reconcileGroup := func() {
		t.Helper()
		r := &controller.MysqlFailoverGroupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: fgKey}); err != nil {
			t.Fatalf("group reconcile: %v", err)
		}
		if err := k8sClient.Get(ctx, fgKey, fg); err != nil {
			t.Fatal(err)
		}
	}
	reconcileBackup := func(key client.ObjectKey) []string {
		t.Helper()
		rec := record.NewFakeRecorder(50)
		r := &controller.MysqlBackupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: rec}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("backup reconcile: %v", err)
		}
		return retentionDeferredEvents(rec)
	}

	// The restore waits for its seed. It has pinned nothing yet, but it
	// names the backup: retention keeps it past keepLast.
	reconcileGroup()
	reconcileGroup()
	if fg.Status.Restore == nil || fg.Status.Restore.Phase != v1alpha1.BackupPhasePending || fg.Status.Restore.BackupRef != nil {
		t.Fatalf("status.restore = %+v, want Pending and not pinned before the Job can be built", fg.Status.Restore)
	}
	if events := reconcileBackup(client.ObjectKeyFromObject(newer)); len(events) != 1 || !strings.Contains(events[0], "the restore of group lion") {
		t.Fatalf("RetentionDeferred events = %q, want one naming the restoring group", events)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil || !older.DeletionTimestamp.IsZero() {
		t.Fatalf("retention deleted the backup a pending restore names: %v deleting=%v", err, older.DeletionTimestamp)
	}

	// The seed is published and its Deployment is ready: the restore pins
	// the backup, holds it and creates its Job.
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: v1alpha1.SiteStateWritable}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	var deploy appsv1.Deployment
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: "mysql-lion-dc1"}, &deploy); err != nil {
		t.Fatal(err)
	}
	deploy.Status.Replicas, deploy.Status.ReadyReplicas, deploy.Status.UpdatedReplicas, deploy.Status.AvailableReplicas = 1, 1, 1, 1
	deploy.Status.ObservedGeneration = deploy.Generation
	if err := k8sClient.Status().Update(ctx, &deploy); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5 && (fg.Status.Restore == nil || fg.Status.Restore.JobName == ""); i++ {
		reconcileGroup()
	}
	rs := fg.Status.Restore
	if rs == nil || rs.JobName == "" || rs.BackupRef == nil {
		t.Fatalf("status.restore = %+v, want a Job and a pinned backup", rs)
	}
	ref := *rs.BackupRef
	if ref.Name != older.Name || ref.UID != string(older.UID) || ref.HoldID == "" {
		t.Fatalf("status.restore.backupRef = %+v, want %s pinned by UID %s", ref, older.Name, older.UID)
	}
	holdKey := v1alpha1.RestoreHoldAnnotationPrefix + ref.HoldID
	if err := k8sClient.Get(ctx, oKey, older); err != nil || older.Annotations[holdKey] != fg.Name {
		t.Fatalf("hold not stamped before the Job: %v %v", err, older.Annotations)
	}
	var job batchv1.Job
	jobKey := client.ObjectKey{Namespace: ns, Name: rs.JobName}
	if err := k8sClient.Get(ctx, jobKey, &job); err != nil || job.Annotations[v1alpha1.RestoreBackupUIDAnnotation] != string(older.UID) {
		t.Fatalf("restore Job: %v, annotations %v, want it built from the pinned backup", err, job.Annotations)
	}
	if events := reconcileBackup(client.ObjectKeyFromObject(newer)); len(events) != 1 {
		t.Fatalf("RetentionDeferred events while the restore runs = %q", events)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil || !older.DeletionTimestamp.IsZero() {
		t.Fatalf("retention deleted the backup a restore reads: %v deleting=%v", err, older.DeletionTimestamp)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-pod", Namespace: ns, Labels: map[string]string{
			"app.kubernetes.io/name": "mysql-restore", v1alpha1.FailoverGroupLabel: fg.Name}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "restore", Image: "restore"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}

	// A manual delete during the restore is deferred.
	if err := k8sClient.Delete(ctx, older); err != nil {
		t.Fatal(err)
	}
	reconcileBackup(oKey)
	var cleanup batchv1.Job
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup Job created while a restore reads the artifact: %v", err)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil {
		t.Fatal(err)
	}
	if cond := apiMeta.FindStatusCondition(older.Status.Conditions, v1alpha1.ConditionArtifactCleanup); cond == nil || cond.Reason != "HeldByRestore" {
		t.Fatalf("ArtifactCleanup condition = %+v, want HeldByRestore", cond)
	}

	// The restore fails while its pod is still running: the hold stays.
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
	reconcileGroup()
	reconcileGroup()
	if fg.Status.Restore.Phase != v1alpha1.BackupPhaseFailed || fg.Status.Restore.BackupRef == nil || *fg.Status.Restore.BackupRef != ref {
		t.Fatalf("status.restore = %+v, want Failed with the pin kept", fg.Status.Restore)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil || older.Annotations[holdKey] != fg.Name {
		t.Fatalf("hold released while the restore pod still runs: %v %v", err, older.Annotations)
	}
	reconcileBackup(oKey)
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup started while the failed restore's pod still runs: %v", err)
	}

	// The pod stops: the hold is released and cleanup starts.
	pod.Status.Phase = corev1.PodFailed
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	reconcileGroup()
	if err := k8sClient.Get(ctx, oKey, older); err != nil || len(restoreHolds(older)) != 0 {
		t.Fatalf("hold not released after the restore stopped: %v %v", err, older.Annotations)
	}
	reconcileBackup(oKey)
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); err != nil {
		t.Fatalf("cleanup did not start after the hold was released: %v", err)
	}
}

// TestEnvtest_RestoreHoldStrippedWhenGroupIsGone covers a restore whose
// group was deleted before it released its hold: the backup's own
// reconciler strips the hold, and the next retention pass prunes the
// backup. A backup of the same name created afterwards is not protected by
// a restore that pinned its predecessor.
func TestEnvtest_RestoreHoldStrippedWhenGroupIsGone(t *testing.T) {
	ns := createNamespace(t, "restore-hold-group-gone")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 1}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	older := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-older", time.Now().Add(-2*time.Hour))
	oKey := client.ObjectKeyFromObject(older)
	// Another group restores from it: pinned, held, Job running.
	restorer := newVerificationFG(ns)
	restorer.Name = "tiger"
	restorer.Spec.DNS.Hostname = "tiger.az.example.com"
	restorer.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: older.Name}}}
	if err := k8sClient.Create(ctx, restorer); err != nil {
		t.Fatal(err)
	}
	const holdID = "0d9f1c1e-0000-4000-8000-000000000275"
	restorer.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning, JobName: "mysql-tiger-dc1-restore",
		BackupRef: &v1alpha1.RestoreBackupRef{Name: older.Name, UID: string(older.UID), HoldID: holdID}}
	if err := k8sClient.Status().Update(ctx, restorer); err != nil {
		t.Fatal(err)
	}
	older.Annotations = map[string]string{v1alpha1.RestoreHoldAnnotationPrefix + holdID: restorer.Name}
	if err := k8sClient.Update(ctx, older); err != nil {
		t.Fatal(err)
	}
	newer := createHeldTestBackup(t, ns, fg.Name, "lion-nightly-newer", time.Now())

	reconcileBackup := func(key client.ObjectKey) (ctrl.Result, []string) {
		t.Helper()
		rec := record.NewFakeRecorder(50)
		r := &controller.MysqlBackupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: rec}
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		if err != nil {
			t.Fatalf("backup reconcile: %v", err)
		}
		return res, retentionDeferredEvents(rec)
	}

	// While the restore runs the hold stays, the backup is looked at
	// again, and retention defers.
	if res, _ := reconcileBackup(oKey); res.RequeueAfter <= 0 || res.RequeueAfter > time.Minute {
		t.Fatalf("requeue = %s, want a recheck within a minute while a restore hold is live", res.RequeueAfter)
	}
	if _, events := reconcileBackup(client.ObjectKeyFromObject(newer)); len(events) != 1 || !strings.Contains(events[0], "the restore of group tiger") {
		t.Fatalf("RetentionDeferred events = %q", events)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil || len(restoreHolds(older)) != 1 || !older.DeletionTimestamp.IsZero() {
		t.Fatalf("held backup: %v holds=%v deleting=%v", err, restoreHolds(older), older.DeletionTimestamp)
	}

	// The restoring group is deleted mid-restore; nothing of it runs.
	if err := k8sClient.Delete(ctx, restorer); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(restorer), restorer); !apierrors.IsNotFound(err) {
		t.Fatalf("restoring group still exists: %v", err)
	}
	reconcileBackup(oKey)
	if err := k8sClient.Get(ctx, oKey, older); err != nil || len(restoreHolds(older)) != 0 {
		t.Fatalf("hold not stripped after its group was deleted: %v %v", err, older.Annotations)
	}
	if _, events := reconcileBackup(client.ObjectKeyFromObject(newer)); len(events) != 0 {
		t.Fatalf("retention still deferred: %q", events)
	}
	if err := k8sClient.Get(ctx, oKey, older); err != nil || older.DeletionTimestamp.IsZero() {
		t.Fatalf("backup not pruned once nothing reads it: %v", err)
	}
}
