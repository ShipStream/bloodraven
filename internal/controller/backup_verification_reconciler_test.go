package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// --- verificationLoadOptions ---------------------------------------------

func TestVerificationLoadOptions_SkipsBinlog(t *testing.T) {
	got := verificationLoadOptions()
	if got.Threads != 4 {
		t.Errorf("want threads=4, got %d", got.Threads)
	}
	if got.SkipBinlog == nil || !*got.SkipBinlog {
		t.Errorf("want skipBinlog=true")
	}
}

// --- buildVerificationJob ------------------------------------------------

func verifyFG() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	// PITR verifications read the archive through its read binding (#203).
	bindAllTestPITRArchives(fg)
	return fg
}

func successfulBackup(name, fg, profile string) *v1alpha1.MysqlBackup {
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", UID: types.UID("uid-" + name),
			Labels: map[string]string{
				v1alpha1.FailoverGroupLabel: fg,
				v1alpha1.BackupProfileLabel: profile,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fg},
			ProfileName:      profile,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:            v1alpha1.BackupPhaseSucceeded,
			GtidExecuted:     "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10",
			FailoverGroupUID: "lion-uid",
			// Completed before the fixed PITR targets the tests request.
			CompletionTime:  &metav1.Time{Time: time.Date(2026, 4, 15, 8, 0, 0, 0, time.UTC)},
			ArtifactStorage: testArtifactStorage(v1alpha1.BackupStorageS3, "lion/nightly/name/"),
			SizeBytes:       1 * 1024 * 1024 * 1024,
		},
	}
}

func TestBuildVerificationJob_MountsDatadirAndScripts(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-happy", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
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

// TestBuildVerificationJob_NoStorageDiskBoundedByResources pins A-56: a
// verification has no storage of its own. Its Job mounts no
// PersistentVolumeClaim for an S3 backup, and node disk is bounded only
// through spec.resources.limits.ephemeral-storage, forwarded verbatim.
func TestBuildVerificationJob_NoStorageDiskBoundedByResources(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("bounded", "lion", "nightly-s3")
	limits := corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("50Gi")}
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-bounded", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			Resources:        corev1.ResourceRequirements{Limits: limits},
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "mysqlverify-verify-bounded-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}
	for _, vol := range job.Spec.Template.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			t.Errorf("verification Job of an S3 backup mounts PVC volume %q: %+v", vol.Name, vol)
		}
		if vol.EmptyDir != nil && vol.EmptyDir.SizeLimit != nil {
			t.Errorf("emptyDir %q sets SizeLimit %s; disk is bounded by spec.resources", vol.Name, vol.EmptyDir.SizeLimit)
		}
	}
	got := job.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceEphemeralStorage]
	if got.Cmp(resource.MustParse("50Gi")) != 0 {
		t.Errorf("ephemeral-storage limit = %s, want 50Gi from spec.resources", got.String())
	}
}

func TestBuildVerificationJob_PVCBackedBackup_MountsBackupPVC(t *testing.T) {
	fg := verifyFG()
	// The fg has two profiles; the second is PVC-backed ("daily-local").
	profile := fg.Spec.Backup.Profiles[1]
	backup := successfulBackup("pvc-happy", "lion", profile.Name)
	setTestArtifactType(backup, v1alpha1.BackupStoragePVC)
	setTestArtifactLocation(backup, "/backups/pvc-happy")

	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-pvc", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
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
	setTestArtifactLocation(backup, "")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fgName},
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
	r, c := newVerificationReconciler(t, fg, v, operatorCredsSecret())
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
	r, c := newVerificationReconciler(t, fg, v, operatorCredsSecret())
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
	r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
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
	r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
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
	r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
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
		v1alpha1.FailoverGroupLabel: "lion",
		v1alpha1.BackupProfileLabel: "nightly-s3",
	}
	earlier.Status.Phase = v1alpha1.VerificationPhaseRestoring

	later := verifyCR("verify-second", "lion", "nightly-s3")
	later.CreationTimestamp = metav1.NewTime(time.Now())

	r, c := newVerificationReconciler(t, fg, backup, earlier, later, operatorCredsSecret())
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
		Retention: v1alpha1.VerificationRetention{
			KeepLast:   new(int32(2)),
			KeepFailed: new(int32(10)),
		},
	}

	// 4 terminal-Succeeded CRs, decreasing completion time.
	var crs []client.Object
	crs = append(crs, fg)
	for i := 0; i < 4; i++ {
		name := "old-" + string(rune('a'+i))
		c := verifyCR(name, "lion", "nightly-s3")
		c.Labels = map[string]string{
			v1alpha1.FailoverGroupLabel: "lion",
			v1alpha1.BackupProfileLabel: "nightly-s3",
		}
		c.Status.Phase = v1alpha1.VerificationPhaseSucceeded
		ct := metav1.NewTime(time.Now().Add(time.Duration(-i) * time.Hour))
		c.Status.CompletionTime = &ct
		crs = append(crs, c)
	}
	// Trigger CR (the fresh one to reconcile).
	trigger := verifyCR("trigger", "lion", "nightly-s3")
	trigger.Labels = map[string]string{
		v1alpha1.FailoverGroupLabel: "lion",
		v1alpha1.BackupProfileLabel: "nightly-s3",
	}
	trigger.Status.Phase = v1alpha1.VerificationPhaseSucceeded
	ct := metav1.NewTime(time.Now().Add(time.Hour))
	trigger.Status.CompletionTime = &ct
	crs = append(crs, trigger, operatorCredsSecret())

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

// A-55: verification retention counts Succeeded and Failed runs
// separately; an omitted field (or verification block) means the default
// 30 / 10 and an explicit 0 keeps none.
func TestVerificationRetention(t *testing.T) {
	withRetention := func(rt v1alpha1.VerificationRetention) *v1alpha1.BackupProfile {
		return &v1alpha1.BackupProfile{Verification: &v1alpha1.VerificationSpec{Retention: rt}}
	}
	for _, tc := range []struct {
		name                 string
		profile              *v1alpha1.BackupProfile
		wantLast, wantFailed int32
	}{
		{name: "missing profile", profile: nil, wantLast: 30, wantFailed: 10},
		{name: "no verification block", profile: &v1alpha1.BackupProfile{}, wantLast: 30, wantFailed: 10},
		{name: "omitted fields", profile: withRetention(v1alpha1.VerificationRetention{}), wantLast: 30, wantFailed: 10},
		{name: "set", profile: withRetention(v1alpha1.VerificationRetention{KeepLast: new(int32(2)), KeepFailed: new(int32(3))}), wantLast: 2, wantFailed: 3},
		{name: "explicit zeros keep none", profile: withRetention(v1alpha1.VerificationRetention{KeepLast: new(int32(0)), KeepFailed: new(int32(0))}), wantLast: 0, wantFailed: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			last, failed := resolveVerificationRetention(tc.profile)
			if last != tc.wantLast || failed != tc.wantFailed {
				t.Fatalf("resolveVerificationRetention = (%d, %d), want (%d, %d)", last, failed, tc.wantLast, tc.wantFailed)
			}
		})
	}

	t.Run("keepLast 0 prunes every success, keepFailed omitted keeps 10", func(t *testing.T) {
		fg := verifyFG()
		fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{
			Retention: v1alpha1.VerificationRetention{KeepLast: new(int32(0))},
		}
		objs := []client.Object{fg}
		mk := func(name string, phase v1alpha1.VerificationPhase, age time.Duration) {
			v := verifyCR(name, fg.Name, "nightly-s3")
			v.Status.Phase = phase
			ct := metav1.NewTime(time.Now().Add(-age))
			v.Status.CompletionTime = &ct
			objs = append(objs, v)
		}
		for i := 0; i < 3; i++ {
			mk(fmt.Sprintf("ok-%02d", i), v1alpha1.VerificationPhaseSucceeded, time.Duration(i)*time.Hour)
		}
		for i := 0; i < 12; i++ {
			mk(fmt.Sprintf("bad-%02d", i), v1alpha1.VerificationPhaseFailed, time.Duration(i)*time.Hour)
		}
		mk("running", v1alpha1.VerificationPhaseRestoring, 0)
		r, c := newVerificationReconciler(t, objs...)
		if err := r.pruneRetention(context.Background(), verifyCR("trigger", fg.Name, "nightly-s3")); err != nil {
			t.Fatal(err)
		}
		var list v1alpha1.MysqlBackupVerificationList
		if err := c.List(context.Background(), &list, client.InNamespace("ns")); err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, it := range list.Items {
			got = append(got, it.Name)
		}
		want := []string{"running"}
		for i := 0; i < 10; i++ {
			want = append(want, fmt.Sprintf("bad-%02d", i))
		}
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("kept %v, want %v", got, want)
		}
	})
}

// --- Phase 2: PITR, sanity, and sentinel parsing ------------------------

func TestVerificationPITRRequest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      *v1alpha1.PointInTimeVerificationSpec
		want    *pitrReplayRequest
		wantErr string
	}{
		{name: "nil", in: nil},
		// A present block needs a mode; none is gone (no block = no replay).
		{name: "none", in: &v1alpha1.PointInTimeVerificationSpec{Mode: "none"}, wantErr: "not one of"},
		{name: "lower-case latest", in: &v1alpha1.PointInTimeVerificationSpec{Mode: "latest"}, wantErr: "not one of"},
		{name: "empty mode", in: &v1alpha1.PointInTimeVerificationSpec{}, wantErr: "not one of"},
		{name: "latest has no stop", in: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest, ExcludeGtids: " 3e11fa47-71ca-11e1-9e33-c80aa9429562:1 "},
			want: &pitrReplayRequest{Mode: "latest", ExcludeGtids: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1", Verification: true}},
		{name: "timestamp canonical", in: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: stopAtPtr("2026-04-20T03:00:00+02:00")},
			want: &pitrReplayRequest{Mode: "timestamp", Stop: "2026-04-20T01:00:00.000000Z", Verification: true}},
		// C2 / A-39: whole seconds only.
		{name: "fraction refused", in: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: stopAtPtr("2026-04-20T01:00:00.5Z")}, wantErr: "PITRTargetInvalid"},
		{name: "timestamp required", in: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeTimestamp}, wantErr: "PITRTargetInvalid"},
		{name: "seven digits", in: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: stopAtPtr("2026-04-20T01:00:00.1234567Z")}, wantErr: "PITRTargetInvalid"},
		{name: "latest forbids stopTime", in: &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest, StopTime: stopAtPtr("2026-04-20T01:00:00Z")}, wantErr: "PITRTargetInvalid"},
		{name: "unknown mode", in: &v1alpha1.PointInTimeVerificationSpec{Mode: "head"}, wantErr: "not one of"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := verificationPITRRequest(tc.in)
			if tc.wantErr != "" {
				if err == nil || !strings.HasPrefix(err.Error(), tc.wantErr) && !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (got == nil) != (tc.want == nil) || got != nil && *got != *tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestBuildVerificationJob_PITRTimestamp_AddsInitContainer(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-pitr", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			PointInTime: &v1alpha1.PointInTimeVerificationSpec{
				Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: stopAtPtr("2026-04-20T01:00:00Z"),
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
	// dump-meta runs first, then pitr-download (§6.3 init order).
	initCs := job.Spec.Template.Spec.InitContainers
	if len(initCs) != 2 || initCs[0].Name != restorePITRDumpMetaContainerName || initCs[1].Name != restorePITRInitContainerName {
		names := []string{}
		for _, c := range initCs {
			names = append(names, c.Name)
		}
		t.Fatalf("init containers = %v, want [dump-meta pitr-download]", names)
	}
	dumpEnv := containerEnv(initCs[:1], restorePITRDumpMetaContainerName)
	if dumpEnv["BLOODRAVEN_DUMP_EXPECTED_BACKUP_UID"] != string(backup.UID) || dumpEnv["BLOODRAVEN_DUMP_EXPECTED_GTID_EXECUTED"] != backup.Status.GtidExecuted ||
		dumpEnv["BLOODRAVEN_DUMP_META_OUTPUT_DIR"] != restorePITRDumpMetaDir || dumpEnv["BLOODRAVEN_SOURCE_PREFIX"] == "" {
		t.Errorf("dump-meta env: %v", dumpEnv)
	}
	initEnv := containerEnv(initCs[1:], restorePITRInitContainerName)
	if _, ok := initEnv["BLOODRAVEN_PITR_START_GTIDS"]; ok {
		t.Error("pitr-download must not receive START_GTIDS; B comes from dump.json")
	}
	if initEnv["BLOODRAVEN_PITR_DUMP_META"] != restorePITRDumpMetaDir+"/dump.json" || initEnv["BLOODRAVEN_PITR_MODE"] != "timestamp" {
		t.Errorf("pitr-download env: %v", initEnv)
	}
	main := containerEnv(job.Spec.Template.Spec.Containers, job.Spec.Template.Spec.Containers[0].Name)
	if main["BLOODRAVEN_VERIFY_PITR_MODE"] != "timestamp" || main["BLOODRAVEN_PITR_LOCAL_DIR"] == "" ||
		main["BLOODRAVEN_PITR_STOP_DATETIME"] != "2026-04-20T01:00:00.000000Z" || main["BLOODRAVEN_PITR_ALLOW_PARTIAL"] != "false" {
		t.Errorf("main PITR env: %v", main)
	}
	if _, ok := main["BLOODRAVEN_SOURCE_GTID_EXECUTED"]; ok {
		t.Error("a verification Job must not render BLOODRAVEN_SOURCE_GTID_EXECUTED: its PITR baseline comes from dump.json (dump-meta), not the backup's status.gtidExecuted")
	}
	// F040: three containers share the kubelet's 12 KiB of termination
	// messages, capped at 4 KiB each.
	if got := main[terminationMessageLimitEnv]; got != "4096" {
		t.Errorf("%s = %q, want 4096 (12288/3 capped at 4096)", terminationMessageLimitEnv, got)
	}
}

func TestBuildVerificationJob_PITRRequiresPITREnabled(t *testing.T) {
	fg := verifyFG() // no fg.Spec.Backup.PITR set
	backup := successfulBackup("happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-bad-pitr", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			PointInTime: &v1alpha1.PointInTimeVerificationSpec{
				Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: stopAtPtr("2026-04-20T01:00:00Z"),
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			SanityCheck: &v1alpha1.SanityCheckSpec{
				Query:     "SELECT COUNT(*) FROM orders.orders",
				MinResult: new(int64(5)),
				Timeout:   &metav1.Duration{Duration: 1500 * time.Millisecond},
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
	// A sub-second remainder rounds up to the next whole second.
	if env["BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS"] != "2" {
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
		{name: "lowercase select", in: "select 1", want: "select 1"},
		{name: "cte", in: "WITH x AS (SELECT 1) SELECT * FROM x", want: "WITH x AS (SELECT 1) SELECT * FROM x"},
		{name: "parenthesized", in: "(SELECT 1)", want: "(SELECT 1)"},
		{name: "shell escape command", in: "SELECT 1 \\! id", wantErr: true},
		{name: "leading shell escape", in: "\\! id", wantErr: true},
		{name: "system command", in: "system id", wantErr: true},
		{name: "source command", in: "source /etc/passwd", wantErr: true},
		{name: "selector prefix is not select", in: "selectx 1", wantErr: true},
		{name: "non-select statement", in: "DROP DATABASE orders", wantErr: true},
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
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

// B8: verify.sh no longer prints the replay-complete sentinel (replay
// detail comes from the PITR termination verdict); the log-schema lists it
// under breaking changes, and the reconciler no longer parses it.
func TestVerifyReplaySentinelRemoved(t *testing.T) {
	if strings.Contains(verifyScript, "BLOODRAVEN_VERIFY_REPLAY_COMPLETE") {
		t.Fatal("verify.sh still prints the removed BLOODRAVEN_VERIFY_REPLAY_COMPLETE sentinel")
	}
	if _, ok := parseSanitySentinel("BLOODRAVEN_VERIFY_REPLAY_COMPLETE file=mysql-bin.000412 position=1"); ok {
		t.Fatal("the replay line parsed as a sanity sentinel")
	}
}

func TestParseSanitySentinel_Scalar(t *testing.T) {
	res, ok := parseSanitySentinel("BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=140 resultRow=42")
	if !ok || res == nil {
		t.Fatal("want ok")
	}
	if res.Outcome != v1alpha1.SanityCheckOutcomePassed || res.DurationMilliseconds == nil || *res.DurationMilliseconds != 140 || res.Result != "42" || res.Error != "" {
		t.Errorf("sanity fields: %+v", res)
	}
}

func TestParseSanitySentinel_Timeout(t *testing.T) {
	res, ok := parseSanitySentinel("BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 durationMs=60000 error=timeout")
	if !ok || res == nil {
		t.Fatal("want ok")
	}
	if res.Error != "timeout" || res.Outcome != v1alpha1.SanityCheckOutcomeTimedOut {
		t.Errorf("want error=timeout outcome=TimedOut, got %+v", res)
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

// B4 (§7.1): a PITR verification of a backup without dump identity is
// refused at build time with PITRBackupIdentityUnknown, before dump-meta.
func TestBuildVerificationJob_PITRRefusesBackupWithoutIdentity(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	backup := successfulBackup("old", "lion", "nightly-s3")
	backup.Status.FailoverGroupUID = ""
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-pitr", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			PointInTime:      &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest},
		},
	}
	_, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup,
		CredsSecretName: "c", ScriptsConfigMapName: "s",
	})
	if err == nil || pitrFailureReason(err.Error()) != "PITRBackupIdentityUnknown" {
		t.Fatalf("err = %v, want PITRBackupIdentityUnknown", err)
	}
}

// F028: a PITR refusal from the reconcile-time build is recorded by its
// catalog token, token first, not as BuildJobFailed.
func TestVerificationReconciler_PITRBuildRefusalKeepsToken(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(fg *v1alpha1.MysqlFailoverGroup, b *v1alpha1.MysqlBackup, v *v1alpha1.MysqlBackupVerification)
		want   string
	}{
		{name: "backup without source identity", mutate: func(_ *v1alpha1.MysqlFailoverGroup, b *v1alpha1.MysqlBackup, _ *v1alpha1.MysqlBackupVerification) {
			b.Status.FailoverGroupUID = ""
		}, want: "PITRBackupIdentityUnknown"},
		{name: "PITR disabled on the group", mutate: func(fg *v1alpha1.MysqlFailoverGroup, _ *v1alpha1.MysqlBackup, _ *v1alpha1.MysqlBackupVerification) {
			fg.Spec.Backup.PITR.Enabled = false
		}, want: "PITRTargetInvalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := verifyFG()
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
			backup := successfulBackup("nightly-pitr", "lion", "nightly-s3")
			v := verifyCR("verify-pitr", "lion", "nightly-s3")
			v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest}
			tc.mutate(fg, backup, v)
			r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
			reconcileVerifUntilStable(t, r, "verify-pitr")
			var got v1alpha1.MysqlBackupVerification
			if err := c.Get(context.Background(), types.NamespacedName{Name: "verify-pitr", Namespace: "ns"}, &got); err != nil {
				t.Fatal(err)
			}
			cond := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionVerified)
			if got.Status.Phase != v1alpha1.VerificationPhaseFailed || cond == nil || cond.Reason != tc.want ||
				!strings.HasPrefix(cond.Message, tc.want+": ") {
				t.Fatalf("phase %s condition %+v, want Verified=False/%s token first", got.Status.Phase, cond, tc.want)
			}
			if ready := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady); ready == nil ||
				ready.Status != cond.Status || ready.Reason != cond.Reason || ready.Message != cond.Message {
				t.Fatalf("Ready %+v, want a mirror of Verified %+v", ready, cond)
			}
		})
	}
}

// TestSetVerificationReady pins A-70 for MysqlBackupVerification: Ready is
// False/InProgress until Verified is set, then mirrors it, and carries the
// generation it was evaluated for.
func TestSetVerificationReady(t *testing.T) {
	var st v1alpha1.MysqlBackupVerificationStatus
	setVerificationReady(&st, 4)
	ready := apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != v1alpha1.ReasonInProgress || ready.ObservedGeneration != 4 {
		t.Fatalf("Ready before a verdict = %+v, want False/InProgress at generation 4", ready)
	}
	for _, verified := range []metav1.Condition{
		{Type: v1alpha1.ConditionVerified, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonSucceeded, Message: "restored", LastTransitionTime: metav1.Now()},
		{Type: v1alpha1.ConditionVerified, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonSanityCheckFailed, Message: "no tables", LastTransitionTime: metav1.Now()},
	} {
		apimeta.SetStatusCondition(&st.Conditions, verified)
		setVerificationReady(&st, 5)
		ready = apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionReady)
		if ready == nil || ready.Status != verified.Status || ready.Reason != verified.Reason || ready.Message != verified.Message || ready.ObservedGeneration != 5 {
			t.Fatalf("Ready = %+v, want a mirror of %+v at generation 5", ready, verified)
		}
	}
}

func TestTokenFirstMessage(t *testing.T) {
	for in, want := range map[string]string{
		"PITRTargetInvalid: x":                       "PITRTargetInvalid: x",
		"verification job: PITRTargetInvalid: x":     "PITRTargetInvalid: verification job: x",
		"a: b: PITRTargetInvalid: detail; procedure": "PITRTargetInvalid: a: b: detail; procedure",
	} {
		if got := tokenFirstMessage("PITRTargetInvalid", in); got != want {
			t.Errorf("tokenFirstMessage(%q) = %q, want %q", in, got, want)
		}
	}
}
