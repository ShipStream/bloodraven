package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// --- autoSizeVerificationPVC ---------------------------------------------

func TestAutoSizeVerificationPVC_ExplicitSizeWins(t *testing.T) {
	v := &v1alpha1.MysqlBackupVerification{
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			Storage: &v1alpha1.VerificationStorage{Size: resource.MustParse("42Gi")},
		},
	}
	got := autoSizeVerificationPVC(v, &v1alpha1.MysqlBackup{
		Status: v1alpha1.MysqlBackupStatus{SizeBytes: 500 << 30}, // 500Gi
	})
	if got.String() != "42Gi" {
		t.Errorf("want 42Gi, got %s", got.String())
	}
}

func TestAutoSizeVerificationPVC_NoBackupSize_FallsBackToMinimum(t *testing.T) {
	v := &v1alpha1.MysqlBackupVerification{}
	got := autoSizeVerificationPVC(v, &v1alpha1.MysqlBackup{})
	want := int64(10 * 1024 * 1024 * 1024)
	if gotBytes, _ := got.AsInt64(); gotBytes != want {
		t.Errorf("want minimum %d bytes, got %s", want, got.String())
	}
}

func TestAutoSizeVerificationPVC_RoundsUpToTenGiBBlocks(t *testing.T) {
	v := &v1alpha1.MysqlBackupVerification{}
	// 12 GiB backup * 1.5 = 18 GiB, rounds up to 20 GiB.
	got := autoSizeVerificationPVC(v, &v1alpha1.MysqlBackup{
		Status: v1alpha1.MysqlBackupStatus{SizeBytes: 12 * 1024 * 1024 * 1024},
	})
	want := int64(20 * 1024 * 1024 * 1024)
	if gotBytes, _ := got.AsInt64(); gotBytes != want {
		t.Errorf("want %d bytes (20Gi), got %s", want, got.String())
	}
}

// --- verificationLoadOptions ---------------------------------------------

func TestVerificationLoadOptions_ResetsProgressSkipsBinlog(t *testing.T) {
	got := verificationLoadOptions()
	if got.Threads != 4 {
		t.Errorf("want threads=4, got %d", got.Threads)
	}
	if got.ResetProgress == nil || !*got.ResetProgress {
		t.Errorf("want resetProgress=true")
	}
	if got.SkipBinlog == nil || !*got.SkipBinlog {
		t.Errorf("want skipBinlog=true")
	}
}

// --- buildVerificationJob ------------------------------------------------

func verifyFG() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	return fg
}

func successfulBackup(name, fg, profile string) *v1alpha1.MysqlBackup {
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", UID: types.UID("uid-" + name),
			Labels: map[string]string{
				labelFailoverGroup: fg,
				labelBackupProfile: profile,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg},
			ProfileName:      profile,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseSucceeded,
			CompletionTime: &metav1.Time{Time: time.Now()},
			Location:       "lion/nightly/name/",
			StorageType:    v1alpha1.BackupStorageS3,
			SizeBytes:      1 * 1024 * 1024 * 1024,
		},
	}
}

func TestBuildVerificationJob_MountsDatadirAndScripts(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-happy", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "mysqlverify-verify-happy-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
		PVCName:              "mysqlverify-verify-happy-data",
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}

	c := job.Spec.Template.Spec.Containers[0]
	if len(c.Command) == 0 || !strings.Contains(strings.Join(c.Command, " "), "verify.sh") {
		t.Errorf("want verify.sh in container command, got %v", c.Command)
	}

	volByName := map[string]corev1.Volume{}
	for _, vol := range job.Spec.Template.Spec.Volumes {
		volByName[vol.Name] = vol
	}
	if _, ok := volByName["datadir"]; !ok {
		t.Errorf("want datadir volume")
	}
	if dv := volByName["datadir"].PersistentVolumeClaim; dv == nil || dv.ClaimName != "mysqlverify-verify-happy-data" {
		t.Errorf("datadir volume does not reference expected PVC: %+v", volByName["datadir"])
	}
	if _, ok := volByName["scripts"]; !ok {
		t.Errorf("want scripts volume")
	}
	if _, ok := volByName["aws-creds"]; !ok {
		t.Errorf("S3-backed verification should mount aws-creds")
	}

	mountByName := map[string]corev1.VolumeMount{}
	for _, m := range c.VolumeMounts {
		mountByName[m.Name] = m
	}
	if m, ok := mountByName["datadir"]; !ok || m.MountPath != verificationDataMountPath {
		t.Errorf("datadir not mounted at %s: %+v", verificationDataMountPath, m)
	}
}

func TestBuildVerificationJob_PVCBackedBackup_MountsBackupPVC(t *testing.T) {
	fg := verifyFG()
	// The fg has two profiles; the second is PVC-backed ("daily-local").
	profile := fg.Spec.Backup.Profiles[1]
	backup := successfulBackup("pvc-happy", "lion", profile.Name)
	backup.Status.StorageType = v1alpha1.BackupStoragePVC
	backup.Status.Location = "/backups/pvc-happy"

	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-pvc", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      profile.Name,
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              profile,
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "mysqlverify-verify-pvc-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
		PVCName:              verificationPVCName("verify-pvc"),
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}
	var hasBackupSrc bool
	for _, vol := range job.Spec.Template.Spec.Volumes {
		if vol.Name == "backup-src" {
			hasBackupSrc = true
		}
	}
	if !hasBackupSrc {
		t.Errorf("PVC-backed verification should mount backup-src")
	}
}

func TestBuildVerificationJob_MissingLocation_Errors(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("missing-loc", "lion", "nightly-s3")
	backup.Status.Location = ""
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	_, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "c",
		ScriptsConfigMapName: "s",
		PVCName:              "p",
	})
	if err == nil {
		t.Fatal("expected error for backup without status.location")
	}
}

// --- Reconciler end-to-end (fake client) ---------------------------------

func newVerificationReconciler(t *testing.T, objs ...client.Object) (*MysqlBackupVerificationReconciler, client.Client) {
	t.Helper()
	scheme := testScheme()
	cb := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlBackupVerification{}, &v1alpha1.MysqlBackup{}, &v1alpha1.MysqlFailoverGroup{}).
		WithObjects(objs...)
	c := cb.Build()
	r := &MysqlBackupVerificationReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	return r, c
}

func verifyCR(name, fgName, profile string) *v1alpha1.MysqlBackupVerification {
	return &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fgName},
			ProfileName:      profile,
		},
	}
}

func reconcileVerifUntilStable(t *testing.T, r *MysqlBackupVerificationReconciler, name string) {
	t.Helper()
	for i := 0; i < 6; i++ {
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: "ns"},
		})
		if err != nil {
			t.Fatalf("reconcile #%d failed: %v", i, err)
		}
	}
}

func TestVerificationReconciler_GroupNotFound_TransitionsToFailed(t *testing.T) {
	v := verifyCR("orphan", "ghost", "x")
	r, c := newVerificationReconciler(t, v)
	reconcileVerifUntilStable(t, r, "orphan")

	var got v1alpha1.MysqlBackupVerification
	_ = c.Get(context.Background(), types.NamespacedName{Name: "orphan", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Errorf("want Failed, got %q (message=%s)", got.Status.Phase, got.Status.Message)
	}
}

func TestVerificationReconciler_ProfileNotFound_TransitionsToFailed(t *testing.T) {
	fg := verifyFG()
	v := verifyCR("bad-prof", "lion", "does-not-exist")
	r, c := newVerificationReconciler(t, fg, v, dsnSecret())
	reconcileVerifUntilStable(t, r, "bad-prof")

	var got v1alpha1.MysqlBackupVerification
	_ = c.Get(context.Background(), types.NamespacedName{Name: "bad-prof", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Errorf("want Failed, got %q", got.Status.Phase)
	}
}

func TestVerificationReconciler_NoSucceededBackup_TransitionsToFailed(t *testing.T) {
	fg := verifyFG()
	v := verifyCR("no-backup", "lion", "nightly-s3")
	r, c := newVerificationReconciler(t, fg, v, dsnSecret())
	reconcileVerifUntilStable(t, r, "no-backup")

	var got v1alpha1.MysqlBackupVerification
	_ = c.Get(context.Background(), types.NamespacedName{Name: "no-backup", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Errorf("want Failed (no backup available), got %q", got.Status.Phase)
	}
}

func TestVerificationReconciler_CreatesJobAndPVC(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("nightly-happy", "lion", "nightly-s3")
	v := verifyCR("verify-happy", "lion", "nightly-s3")
	r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
	reconcileVerifUntilStable(t, r, "verify-happy")

	var got v1alpha1.MysqlBackupVerification
	if err := c.Get(context.Background(), types.NamespacedName{Name: "verify-happy", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.BackupRef == nil || got.Status.BackupRef.Name != "nightly-happy" {
		t.Errorf("backupRef not resolved: %+v", got.Status.BackupRef)
	}
	if got.Status.Phase != v1alpha1.VerificationPhaseRestoring {
		t.Errorf("want Restoring, got %q", got.Status.Phase)
	}

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: verificationJobName("verify-happy"), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("verification job not created: %v", err)
	}

	var pvc corev1.PersistentVolumeClaim
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: verificationPVCName("verify-happy"), Namespace: "ns",
	}, &pvc); err != nil {
		t.Fatalf("verification pvc not created: %v", err)
	}
}

func TestVerificationReconciler_JobSucceeded_TransitionsToSucceeded(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("nightly-happy", "lion", "nightly-s3")
	v := verifyCR("verify-happy", "lion", "nightly-s3")
	r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
	reconcileVerifUntilStable(t, r, "verify-happy")

	// Simulate successful Job completion.
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: verificationJobName("verify-happy"), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("job: %v", err)
	}
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
	}
	job.Status.Succeeded = 1
	job.Status.StartTime = &metav1.Time{Time: time.Now().Add(-30 * time.Second)}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job: %v", err)
	}

	reconcileVerifUntilStable(t, r, "verify-happy")

	var got v1alpha1.MysqlBackupVerification
	_ = c.Get(context.Background(), types.NamespacedName{Name: "verify-happy", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.VerificationPhaseSucceeded {
		t.Errorf("want Succeeded, got %q (message=%s)", got.Status.Phase, got.Status.Message)
	}
	if got.Status.CompletionTime == nil {
		t.Errorf("CompletionTime not set")
	}
}

func TestVerificationReconciler_JobFailed_TransitionsToFailed(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("nightly-happy", "lion", "nightly-s3")
	v := verifyCR("verify-sad", "lion", "nightly-s3")
	r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
	reconcileVerifUntilStable(t, r, "verify-sad")

	var job batchv1.Job
	_ = c.Get(context.Background(), types.NamespacedName{
		Name: verificationJobName("verify-sad"), Namespace: "ns",
	}, &job)
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now(), Message: "boom"},
	}
	job.Status.Failed = 5
	_ = c.Status().Update(context.Background(), &job)

	reconcileVerifUntilStable(t, r, "verify-sad")

	var got v1alpha1.MysqlBackupVerification
	_ = c.Get(context.Background(), types.NamespacedName{Name: "verify-sad", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Errorf("want Failed, got %q", got.Status.Phase)
	}
}

func TestVerificationReconciler_ConcurrentRun_SecondBlocked(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("nightly-happy", "lion", "nightly-s3")

	earlier := verifyCR("verify-first", "lion", "nightly-s3")
	earlier.CreationTimestamp = metav1.NewTime(time.Now().Add(-1 * time.Minute))
	earlier.Labels = map[string]string{
		labelFailoverGroup: "lion",
		labelBackupProfile: "nightly-s3",
	}
	earlier.Status.Phase = v1alpha1.VerificationPhaseRestoring

	later := verifyCR("verify-second", "lion", "nightly-s3")
	later.CreationTimestamp = metav1.NewTime(time.Now())

	r, c := newVerificationReconciler(t, fg, backup, earlier, later, dsnSecret())
	reconcileVerifUntilStable(t, r, "verify-second")

	var got v1alpha1.MysqlBackupVerification
	_ = c.Get(context.Background(), types.NamespacedName{Name: "verify-second", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Errorf("want Failed due to concurrency, got %q", got.Status.Phase)
	}
	found := false
	for _, cond := range got.Status.Conditions {
		if cond.Reason == "BlockedByActiveVerification" {
			found = true
		}
	}
	if !found {
		t.Errorf("want BlockedByActiveVerification condition reason, got %+v", got.Status.Conditions)
	}
}

func TestVerificationReconciler_RetentionPrunesOldSuccesses(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{
		Enabled:  true,
		Schedule: "0 5 * * *",
		RetentionPolicy: &v1alpha1.VerificationRetentionPolicy{
			KeepSuccessful: 2,
			KeepFailures:   10,
		},
	}

	// 4 terminal-Succeeded CRs, decreasing completion time.
	var crs []client.Object
	crs = append(crs, fg)
	for i := 0; i < 4; i++ {
		name := "old-" + string(rune('a'+i))
		c := verifyCR(name, "lion", "nightly-s3")
		c.Labels = map[string]string{
			labelFailoverGroup: "lion",
			labelBackupProfile: "nightly-s3",
		}
		c.Status.Phase = v1alpha1.VerificationPhaseSucceeded
		ct := metav1.NewTime(time.Now().Add(time.Duration(-i) * time.Hour))
		c.Status.CompletionTime = &ct
		crs = append(crs, c)
	}
	// Trigger CR (the fresh one to reconcile).
	trigger := verifyCR("trigger", "lion", "nightly-s3")
	trigger.Labels = map[string]string{
		labelFailoverGroup: "lion",
		labelBackupProfile: "nightly-s3",
	}
	trigger.Status.Phase = v1alpha1.VerificationPhaseSucceeded
	ct := metav1.NewTime(time.Now().Add(time.Hour))
	trigger.Status.CompletionTime = &ct
	crs = append(crs, trigger, dsnSecret())

	r, c := newVerificationReconciler(t, crs...)
	reconcileVerifUntilStable(t, r, "trigger")

	var list v1alpha1.MysqlBackupVerificationList
	if err := c.List(context.Background(), &list, client.InNamespace("ns")); err != nil {
		t.Fatalf("list: %v", err)
	}
	succeeded := 0
	for _, it := range list.Items {
		if it.Status.Phase == v1alpha1.VerificationPhaseSucceeded && it.DeletionTimestamp.IsZero() {
			succeeded++
		}
	}
	if succeeded > 2 {
		t.Errorf("retention did not prune: %d Succeeded remain (want <=2)", succeeded)
	}
}
