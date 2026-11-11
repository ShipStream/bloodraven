//go:build envtest

package envtest

import (
	"context"
	"errors"
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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

// interceptedClient is a client on the test API server whose writes pass
// through funcs first.
func interceptedClient(t *testing.T, funcs interceptor.Funcs) client.Client {
	t.Helper()
	base, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return interceptor.NewClient(base, funcs)
}

// beforeBackupPatch returns a client that runs fn once, right before the
// first patch of a MysqlBackup that match accepts reaches the API server,
// and whether it ran.
func beforeBackupPatch(t *testing.T, match func(*v1alpha1.MysqlBackup) bool, fn func()) (client.Client, *bool) {
	t.Helper()
	ran := new(bool)
	return interceptedClient(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if b, ok := obj.(*v1alpha1.MysqlBackup); ok && !*ran && match(b) {
				*ran = true
				fn()
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}), ran
}

// restoreHoldFixture is a group whose initFromBackup reads the Succeeded
// backup "lion-nightly-seed", with everything in place for the next group
// reconcile to pin the backup, hold it and create the restore Job.
type restoreHoldFixture struct {
	t      *testing.T
	ns     string
	fg     *v1alpha1.MysqlFailoverGroup
	fgKey  client.ObjectKey
	seed   *v1alpha1.MysqlBackup
	seedAt client.ObjectKey
}

func newRestoreHoldFixture(t *testing.T, name string) *restoreHoldFixture {
	t.Helper()
	controller.SetOperatorImageDefaults("registry.test/bloodraven:envtest", "bloodraven")
	t.Cleanup(func() { controller.SetOperatorImageDefaults("", "") })
	ns := createNamespace(t, name)
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "lion-nightly-seed"}}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	f := &restoreHoldFixture{t: t, ns: ns, fg: fg, fgKey: client.ObjectKeyFromObject(fg)}
	f.seed = createHeldTestBackup(t, ns, fg.Name, "lion-nightly-seed", time.Now().Add(-time.Hour))
	f.seedAt = client.ObjectKeyFromObject(f.seed)
	// The first passes create the Deployments; the restore waits.
	f.reconcileGroup(k8sClient)
	f.reconcileGroup(k8sClient)
	if rs := f.fg.Status.Restore; rs == nil || rs.Phase != v1alpha1.BackupPhasePending || rs.BackupRef != nil {
		t.Fatalf("status.restore = %+v, want Pending and not pinned before the Job can be built", rs)
	}
	f.fg.Status.ActiveSite = "dc1"
	f.fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: v1alpha1.SiteStateWritable}}
	if err := k8sClient.Status().Update(ctx, f.fg); err != nil {
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
	return f
}

// reconcileGroup runs one pass of a new group reconciler (an operator
// restart) through c and reads the group back. A reconcile error is
// returned, not fatal: some tests inject one.
func (f *restoreHoldFixture) reconcileGroup(c client.Client) error {
	f.t.Helper()
	r := &controller.MysqlFailoverGroupReconciler{Client: c, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: f.fgKey})
	if getErr := k8sClient.Get(ctx, f.fgKey, f.fg); getErr != nil {
		f.t.Fatal(getErr)
	}
	return err
}

func (f *restoreHoldFixture) mustReconcileGroup(c client.Client) {
	f.t.Helper()
	if err := f.reconcileGroup(c); err != nil {
		f.t.Fatalf("group reconcile: %v", err)
	}
}

func (f *restoreHoldFixture) reconcileBackup(c client.Client) ctrl.Result {
	f.t.Helper()
	r := &controller.MysqlBackupReconciler{Client: c, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: f.seedAt})
	if err != nil {
		f.t.Fatalf("backup reconcile: %v", err)
	}
	return res
}

func (f *restoreHoldFixture) getSeed() *v1alpha1.MysqlBackup {
	f.t.Helper()
	if err := k8sClient.Get(ctx, f.seedAt, f.seed); err != nil {
		f.t.Fatal(err)
	}
	return f.seed
}

func (f *restoreHoldFixture) restoreJobs() []batchv1.Job {
	f.t.Helper()
	var jobs batchv1.JobList
	if err := k8sClient.List(ctx, &jobs, client.InNamespace(f.ns), client.MatchingLabels{"app.kubernetes.io/name": "mysql-restore"}); err != nil {
		f.t.Fatal(err)
	}
	return jobs.Items
}

// runToJob reconciles until the restore Job exists and returns it.
func (f *restoreHoldFixture) runToJob() *batchv1.Job {
	f.t.Helper()
	for i := 0; i < 5 && (f.fg.Status.Restore == nil || f.fg.Status.Restore.JobName == ""); i++ {
		f.mustReconcileGroup(k8sClient)
	}
	jobs := f.restoreJobs()
	if len(jobs) != 1 || f.fg.Status.Restore.BackupRef == nil {
		f.t.Fatalf("restore Jobs = %d, status.restore = %+v, want one Job and a pinned backup", len(jobs), f.fg.Status.Restore)
	}
	return &jobs[0]
}

func failJob(t *testing.T, job *batchv1.Job) {
	t.Helper()
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded"},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded"},
	}
	if err := k8sClient.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
}

// TestEnvtest_RestoreHoldWritesArePreconditioned pins, against a real API
// server, the four conditional writes the restore hold relies on (R4 A1).
// Each case lets another writer in between the read a write was decided on
// and the write: the write must lose. Without its resourceVersion
// precondition the stamp would land on a backup whose cleanup may have
// started, the pin would overwrite a status this pass never read, and the
// release and the strip, which remove the last annotation with
// {"annotations": null}, would drop a hold stamped meanwhile.
func TestEnvtest_RestoreHoldWritesArePreconditioned(t *testing.T) {
	t.Run("stamp loses to a delete since its read", func(t *testing.T) {
		f := newRestoreHoldFixture(t, "restore-hold-stamp")
		c, deleted := beforeBackupPatch(t, func(*v1alpha1.MysqlBackup) bool { return true }, func() {
			if err := k8sClient.Delete(ctx, f.getSeed()); err != nil {
				t.Fatal(err)
			}
		})
		f.mustReconcileGroup(c)
		if !*deleted {
			t.Fatal("the stamp was never attempted")
		}
		if jobs := f.restoreJobs(); len(jobs) != 0 {
			t.Fatalf("restore Job %s created from a backup that was deleted before the hold was stamped", jobs[0].Name)
		}
		if seed := f.getSeed(); seed.DeletionTimestamp.IsZero() || len(restoreHolds(seed)) != 0 {
			t.Fatalf("backup deleting = %v, holds = %v, want a Terminating backup without the hold", !seed.DeletionTimestamp.IsZero(), restoreHolds(seed))
		}
		// The delete won: the attempt ends, and nothing is created.
		f.mustReconcileGroup(c)
		if rs := f.fg.Status.Restore; rs.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(rs.Message, "the backup is being deleted") {
			t.Fatalf("status.restore = %+v, want Failed (being deleted)", rs)
		}
		if jobs := f.restoreJobs(); len(jobs) != 0 {
			t.Fatalf("restore Job %s created from a backup being deleted that the restore never held", jobs[0].Name)
		}
	})

	t.Run("pin loses to a status write since its read", func(t *testing.T) {
		f := newRestoreHoldFixture(t, "restore-hold-pin")
		other := v1alpha1.RestoreBackupRef{Name: f.seed.Name, UID: string(f.seed.UID), HoldID: "0d9f1c1e-0000-4000-8000-0000000002a1"}
		wrote := false
		c := interceptedClient(t, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				// The pin is the status patch that carries a new backupRef.
				if fg, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && !wrote && fg.Status.Restore != nil && fg.Status.Restore.BackupRef != nil {
					wrote = true
					var stored v1alpha1.MysqlFailoverGroup
					if err := k8sClient.Get(ctx, f.fgKey, &stored); err != nil {
						t.Fatal(err)
					}
					stored.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: "an-earlier-job",
						Message: "an attempt this pass did not see", BackupRef: other.DeepCopy()}
					if err := k8sClient.Status().Update(ctx, &stored); err != nil {
						t.Fatal(err)
					}
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		})
		f.mustReconcileGroup(c)
		if !wrote {
			t.Fatal("the pin was never attempted")
		}
		if rs := f.fg.Status.Restore; rs == nil || rs.Phase != v1alpha1.BackupPhaseFailed || rs.JobName != "an-earlier-job" ||
			rs.BackupRef == nil || *rs.BackupRef != other {
			t.Fatalf("status.restore = %+v, want the status written since the read, untouched", rs)
		}
		if holds := restoreHolds(f.getSeed()); len(holds) != 0 {
			t.Fatalf("hold stamped under a pin that was not recorded: %v", holds)
		}
		if jobs := f.restoreJobs(); len(jobs) != 0 {
			t.Fatalf("restore Job %s created under a pin that was not recorded", jobs[0].Name)
		}
	})

	t.Run("release keeps a hold stamped since its read", func(t *testing.T) {
		f := newRestoreHoldFixture(t, "restore-hold-release")
		job := f.runToJob()
		own := v1alpha1.RestoreHoldAnnotationPrefix + f.fg.Status.Restore.BackupRef.HoldID
		failJob(t, job)
		f.mustReconcileGroup(k8sClient)
		if rs := f.fg.Status.Restore; rs.Phase != v1alpha1.BackupPhaseFailed {
			t.Fatalf("status.restore = %+v, want Failed", rs)
		}
		if seed := f.getSeed(); len(seed.Annotations) != 1 || seed.Annotations[own] != f.fg.Name {
			t.Fatalf("annotations before the release = %v, want this restore's hold alone", seed.Annotations)
		}
		const verification = holdAnnotationPrefix + "uid-v"
		c, stamped := beforeBackupPatch(t, func(b *v1alpha1.MysqlBackup) bool { _, held := b.Annotations[own]; return !held }, func() {
			seed := f.getSeed()
			seed.Annotations[verification] = "v"
			if err := k8sClient.Update(ctx, seed); err != nil {
				t.Fatal(err)
			}
		})
		f.mustReconcileGroup(c)
		if seed := f.getSeed(); !*stamped || seed.Annotations[verification] != "v" {
			t.Fatalf("stamped = %v, annotations = %v: the release dropped a hold stamped after it read the backup", *stamped, seed.Annotations)
		}
		f.mustReconcileGroup(k8sClient)
		if seed := f.getSeed(); len(seed.Annotations) != 1 || seed.Annotations[verification] != "v" {
			t.Fatalf("annotations after the retried release = %v, want the other reader's hold alone", seed.Annotations)
		}
	})

	t.Run("strip keeps a hold stamped since its read", func(t *testing.T) {
		f := newRestoreHoldFixture(t, "restore-hold-strip")
		// A hold whose group is gone, and nothing else on the backup.
		dead := v1alpha1.RestoreHoldAnnotationPrefix + "0d9f1c1e-0000-4000-8000-0000000002a2"
		seed := f.getSeed()
		seed.Annotations = map[string]string{dead: "a-deleted-group"}
		if err := k8sClient.Update(ctx, seed); err != nil {
			t.Fatal(err)
		}
		// The restore pins, then stamps, while the strip is in flight.
		var live string
		c, stamped := beforeBackupPatch(t, func(b *v1alpha1.MysqlBackup) bool { _, has := b.Annotations[dead]; return !has }, func() {
			job := f.runToJob()
			live = v1alpha1.RestoreHoldAnnotationPrefix + f.fg.Status.Restore.BackupRef.HoldID
			if job.Annotations[v1alpha1.RestoreBackupUIDAnnotation] != string(f.seed.UID) {
				t.Fatalf("restore Job built from %q", job.Annotations[v1alpha1.RestoreBackupUIDAnnotation])
			}
		})
		res := f.reconcileBackup(c)
		if seed := f.getSeed(); !*stamped || seed.Annotations[live] != f.fg.Name {
			t.Fatalf("stamped = %v, annotations = %v: the strip dropped a hold stamped after it read the backup", *stamped, seed.Annotations)
		}
		if res.RequeueAfter <= 0 || res.RequeueAfter > time.Minute {
			t.Fatalf("requeue = %s, want another look after the lost strip", res.RequeueAfter)
		}
		f.reconcileBackup(k8sClient)
		if seed := f.getSeed(); len(seed.Annotations) != 1 || seed.Annotations[live] != f.fg.Name {
			t.Fatalf("annotations after the retried strip = %v, want the live hold alone", seed.Annotations)
		}
	})
}

// TestEnvtest_RestoreHoldOutlastsADelete covers a backup deleted by hand
// after the restore stamped its hold and before its Job exists (the Job's
// create is refused once): the backup stays Terminating, both objects name
// each other, the restore reads the backup it holds, and the deletion goes
// on once the restore has ended and released (R4 A3, A4).
func TestEnvtest_RestoreHoldOutlastsADelete(t *testing.T) {
	f := newRestoreHoldFixture(t, "restore-hold-outlasts-delete")
	refused := false
	c := interceptedClient(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if job, ok := obj.(*batchv1.Job); ok && !refused && job.Labels["app.kubernetes.io/name"] == "mysql-restore" {
				refused = true
				return apierrors.NewForbidden(batchv1.Resource("jobs"), job.Name, errors.New("exceeded quota"))
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	if err := f.reconcileGroup(c); err == nil || !refused {
		t.Fatalf("err = %v, refused = %v, want the refused create", err, refused)
	}
	rs := f.fg.Status.Restore
	if rs == nil || rs.Phase != v1alpha1.BackupPhasePending || rs.BackupRef == nil || !strings.Contains(rs.Message, "creating the restore Job failed") {
		t.Fatalf("status.restore = %+v, want Pending, pinned, naming the failed create", rs)
	}
	ref := *rs.BackupRef
	holdKey := v1alpha1.RestoreHoldAnnotationPrefix + ref.HoldID
	if seed := f.getSeed(); seed.Annotations[holdKey] != f.fg.Name {
		t.Fatalf("hold not stamped before the create: %v", seed.Annotations)
	}

	// Deleted by hand: the finalizer defers to the restore that holds it.
	if err := k8sClient.Delete(ctx, f.getSeed()); err != nil {
		t.Fatal(err)
	}
	f.reconcileBackup(k8sClient)
	seed := f.getSeed()
	cond := apiMeta.FindStatusCondition(seed.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if seed.DeletionTimestamp.IsZero() || cond == nil || cond.Reason != "HeldByRestore" || !strings.Contains(cond.Message, "the restore of group lion") {
		t.Fatalf("deleting = %v, ArtifactCleanup = %+v, want a Terminating backup held by the restore of group lion", !seed.DeletionTimestamp.IsZero(), cond)
	}
	cleanupKey := client.ObjectKey{Namespace: f.ns, Name: "mysqlcleanup-" + seed.Name}
	var cleanup batchv1.Job
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup Job created while the restore holds the backup: %v", err)
	}

	// The restore reads the backup it holds, under the same pin.
	job := f.runToJob()
	if got := f.fg.Status.Restore.BackupRef; got == nil || *got != ref || job.Annotations[v1alpha1.RestoreBackupUIDAnnotation] != string(seed.UID) {
		t.Fatalf("pin = %+v (was %+v), Job built from %q", got, ref, job.Annotations[v1alpha1.RestoreBackupUIDAnnotation])
	}
	f.reconcileBackup(k8sClient)
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup Job created while the restore runs: %v", err)
	}

	// The restore ends: the hold is released and the cleanup starts.
	failJob(t, job)
	f.mustReconcileGroup(k8sClient)
	f.mustReconcileGroup(k8sClient)
	if seed := f.getSeed(); len(restoreHolds(seed)) != 0 {
		t.Fatalf("hold not released after the restore ended: %v", seed.Annotations)
	}
	f.reconcileBackup(k8sClient)
	if err := k8sClient.Get(ctx, cleanupKey, &cleanup); err != nil {
		t.Fatalf("cleanup did not start once the restore had ended: %v", err)
	}
}
