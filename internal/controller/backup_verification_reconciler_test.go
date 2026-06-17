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
	// The ephemeral datadir is an emptyDir (fsGroup makes it writable to the
	// non-root verify user). It must NOT carry a SizeLimit (a size-limited
	// emptyDir is a separate mount that is not always fsGroup-chowned).
	datadir := volByName["datadir"]
	if datadir.PersistentVolumeClaim != nil {
		t.Errorf("datadir volume must not reference a PVC: %+v", datadir)
	}
	switch dv := datadir.EmptyDir; {
	case dv == nil:
		t.Errorf("datadir volume is not an emptyDir: %+v", datadir)
	case dv.SizeLimit != nil:
		t.Errorf("datadir emptyDir must not set SizeLimit: %+v", dv)
	}
	if _, ok := volByName["scripts"]; !ok {
		t.Errorf("want scripts volume")
	}
	if _, ok := volByName["aws-creds"]; !ok {
		t.Errorf("S3-backed verification should mount aws-creds")
	}
	// The mysqld socket/pid dir is a dedicated emptyDir at /run/mysqld.
	if _, ok := volByName["run-mysqld"]; !ok {
		t.Errorf("want run-mysqld volume for the socket/pid dir")
	}

	mountByName := map[string]corev1.VolumeMount{}
	for _, m := range c.VolumeMounts {
		mountByName[m.Name] = m
	}
	// The datadir must mount at /var/lib/mysql and the socket dir at
	// /run/mysqld — the only paths the stock mysqld AppArmor profile permits
	// for the datadir and unix socket respectively.
	if m, ok := mountByName["datadir"]; !ok || m.MountPath != verificationDataMountPath {
		t.Errorf("datadir not mounted at %s: %+v", verificationDataMountPath, m)
	}
	if verificationDataMountPath != "/var/lib/mysql" {
		t.Errorf("datadir mount path must be /var/lib/mysql (AppArmor), got %q", verificationDataMountPath)
	}
	if m, ok := mountByName["run-mysqld"]; !ok || m.MountPath != verificationRunMountPath {
		t.Errorf("run-mysqld not mounted at %s: %+v", verificationRunMountPath, m)
	}
	if verificationRunMountPath != "/run/mysqld" {
		t.Errorf("socket dir mount path must be /run/mysqld (AppArmor), got %q", verificationRunMountPath)
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

func TestVerificationReconciler_CreatesJobWithEmptyDirDatadir(t *testing.T) {
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

	// The ephemeral datadir is an emptyDir on the Job pod, not a
	// standalone PVC. Confirm the Job carries an emptyDir datadir and
	// that the reconciler provisioned no PVCs at all.
	var datadir *corev1.Volume
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == "datadir" {
			datadir = &job.Spec.Template.Spec.Volumes[i]
		}
	}
	if datadir == nil || datadir.EmptyDir == nil {
		t.Fatalf("datadir volume is not an emptyDir: %+v", datadir)
	}
	if datadir.PersistentVolumeClaim != nil {
		t.Errorf("datadir volume must not reference a PVC: %+v", datadir)
	}

	var pvcs corev1.PersistentVolumeClaimList
	if err := c.List(context.Background(), &pvcs, client.InNamespace("ns")); err != nil {
		t.Fatalf("list pvcs: %v", err)
	}
	if len(pvcs.Items) != 0 {
		t.Errorf("verification must not create any PVC, found %d: %+v", len(pvcs.Items), pvcs.Items)
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

// --- Phase 2: PITR, sanity, and sentinel parsing ------------------------

func TestVerificationPITRSpec_ModeNoneIsNil(t *testing.T) {
	out, err := verificationPITRSpec(&v1alpha1.PointInTimeVerificationSpec{Mode: "none"})
	if err != nil || out != nil {
		t.Errorf("mode=none should yield (nil, nil), got (%+v, %v)", out, err)
	}
	out, err = verificationPITRSpec(nil)
	if err != nil || out != nil {
		t.Errorf("nil spec should yield (nil, nil), got (%+v, %v)", out, err)
	}
}

func TestVerificationPITRSpec_Latest_UsesFarFutureStop(t *testing.T) {
	out, err := verificationPITRSpec(&v1alpha1.PointInTimeVerificationSpec{Mode: "latest"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil || !strings.HasPrefix(out.StopDatetime, "9999-") {
		t.Errorf("mode=latest should sentinel-stop far-future, got %+v", out)
	}
}

func TestVerificationPITRSpec_Timestamp_RequiresValue(t *testing.T) {
	if _, err := verificationPITRSpec(&v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp"}); err == nil {
		t.Error("mode=timestamp without Timestamp should error")
	}
	out, err := verificationPITRSpec(&v1alpha1.PointInTimeVerificationSpec{
		Mode: "timestamp", Timestamp: "2026-04-20T01:00:00Z",
	})
	if err != nil || out == nil || out.StopDatetime != "2026-04-20T01:00:00Z" {
		t.Errorf("timestamp plumb: out=%+v err=%v", out, err)
	}
}

func TestBuildVerificationJob_PITRTimestamp_AddsInitContainer(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-pitr", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			PointInTime: &v1alpha1.PointInTimeVerificationSpec{
				Mode: "timestamp", Timestamp: "2026-04-20T01:00:00Z",
			},
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "c",
		ScriptsConfigMapName: "s",
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}
	// The only init container is the PITR download container.
	initCs := job.Spec.Template.Spec.InitContainers
	if len(initCs) != 1 {
		t.Fatalf("want 1 init container (pitr-download), got %d", len(initCs))
	}
	if initCs[0].Name != restorePITRInitContainerName {
		t.Errorf("want init container %q, got %q",
			restorePITRInitContainerName, initCs[0].Name)
	}
	var haveMode, haveLocalDir, haveStop bool
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		switch e.Name {
		case "BLOODRAVEN_VERIFY_PITR_MODE":
			haveMode = e.Value == "timestamp"
		case "BLOODRAVEN_PITR_LOCAL_DIR":
			haveLocalDir = e.Value != ""
		case "BLOODRAVEN_PITR_STOP_DATETIME":
			haveStop = e.Value == "2026-04-20T01:00:00Z"
		}
	}
	if !haveMode || !haveLocalDir || !haveStop {
		t.Errorf("missing PITR env vars: mode=%v localDir=%v stop=%v", haveMode, haveLocalDir, haveStop)
	}
}

func TestBuildVerificationJob_PITRRequiresPITREnabled(t *testing.T) {
	fg := verifyFG() // no fg.Spec.Backup.PITR set
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-bad-pitr", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			PointInTime: &v1alpha1.PointInTimeVerificationSpec{
				Mode: "timestamp", Timestamp: "2026-04-20T01:00:00Z",
			},
		},
	}
	_, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "c",
		ScriptsConfigMapName: "s",
	})
	if err == nil {
		t.Fatal("want error when pointInTime is set but PITR not enabled")
	}
}

func TestBuildVerificationJob_SanityCheck_SetsEnvVars(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-sanity", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			SanityCheck: &v1alpha1.SanityCheckSpec{
				Query: "SELECT COUNT(*) FROM orders.orders",
				Expect: &v1alpha1.SanityCheckExpectation{
					MinRows:            5,
					MaxDurationSeconds: 30,
				},
			},
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "c",
		ScriptsConfigMapName: "s",
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}
	env := map[string]string{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["BLOODRAVEN_VERIFY_SANITY_QUERY"] != "SELECT COUNT(*) FROM orders.orders" {
		t.Errorf("sanity query env not set: %q", env["BLOODRAVEN_VERIFY_SANITY_QUERY"])
	}
	if env["BLOODRAVEN_VERIFY_SANITY_MIN_ROWS"] != "5" {
		t.Errorf("sanity min_rows env: %q", env["BLOODRAVEN_VERIFY_SANITY_MIN_ROWS"])
	}
	if env["BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS"] != "30" {
		t.Errorf("sanity max_seconds env: %q", env["BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS"])
	}
}

func TestValidateSanityQuery(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "single statement", in: "SELECT 1", want: "SELECT 1"},
		{name: "trailing semicolon stripped", in: "SELECT COUNT(*) FROM o ;", want: "SELECT COUNT(*) FROM o"},
		{name: "leading trailing whitespace", in: "   SELECT 1  ", want: "SELECT 1"},
		{name: "empty", in: "   ", wantErr: true},
		{name: "multi-statement", in: "SELECT 1; DELETE FROM t", wantErr: true},
		{name: "newline rejected", in: "SELECT\n1", wantErr: true},
		{name: "carriage return rejected", in: "SELECT\r1", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateSanityQuery(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got err=%v", tc.wantErr, err)
			}
			if err == nil && got != tc.want {
				t.Errorf("want %q got %q", tc.want, got)
			}
		})
	}
}

func TestBuildVerificationJob_SanityCheck_RejectsMultiStatement(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-bad-sanity", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			SanityCheck: &v1alpha1.SanityCheckSpec{
				Query: "SELECT 1; DROP TABLE orders",
			},
		},
	}
	if _, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "c",
		ScriptsConfigMapName: "s",
	}); err == nil {
		t.Fatal("expected multi-statement sanity query to be rejected")
	}
}

func TestParseReplaySentinel_HappyPath(t *testing.T) {
	mark, ok := parseReplaySentinel(
		"BLOODRAVEN_VERIFY_REPLAY_COMPLETE file=mysql-bin.000412 position=9183001 timestamp=2026-04-20T01:59:57Z")
	if !ok {
		t.Fatal("want ok")
	}
	if mark.File != "mysql-bin.000412" || mark.Position != 9183001 {
		t.Errorf("file/pos mismatch: %+v", mark)
	}
	if mark.Timestamp == nil || mark.Timestamp.Format(time.RFC3339) != "2026-04-20T01:59:57Z" {
		t.Errorf("timestamp not parsed: %+v", mark.Timestamp)
	}
}

func TestParseReplaySentinel_PrefixMismatch(t *testing.T) {
	if mark, ok := parseReplaySentinel("hello world"); ok || mark != nil {
		t.Errorf("non-sentinel line accepted: %+v", mark)
	}
}

func TestParseSanitySentinel_Scalar(t *testing.T) {
	res, ok := parseSanitySentinel("BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=140 resultRow=42")
	if !ok || res == nil {
		t.Fatal("want ok")
	}
	if !res.Ran || res.DurationMs != 140 || res.ResultRow != "42" || res.Error != "" {
		t.Errorf("sanity fields: %+v", res)
	}
}

func TestParseSanitySentinel_Timeout(t *testing.T) {
	res, ok := parseSanitySentinel("BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=60000 error=timeout")
	if !ok || res == nil {
		t.Fatal("want ok")
	}
	if res.Error != "timeout" {
		t.Errorf("want error=timeout, got %q", res.Error)
	}
}

func TestParseSanitySentinel_WhitespaceEscape(t *testing.T) {
	res, ok := parseSanitySentinel("BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=12 error=ERROR_1146_(42S02):_Table_'x.y'_doesn't_exist")
	if !ok {
		t.Fatal("want ok")
	}
	if !strings.Contains(res.Error, "Table 'x.y' doesn't exist") {
		t.Errorf("error did not round-trip: %q", res.Error)
	}
}
