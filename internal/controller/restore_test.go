package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// helper: a MysqlFailoverGroup with initFromBackup pointing at a MysqlBackup.
func fgInitFromMysqlBackup() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	return fg
}

// succeededSeedBackup is a previously-completed MysqlBackup the restore
// can use as its source.
func succeededSeedBackup() *v1alpha1.MysqlBackup {
	now := metav1.NewTime(time.Now())
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "seed", Namespace: "ns",
			Labels: map[string]string{
				labelFailoverGroup: "lion",
				labelBackupProfile: "nightly-s3",
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseSucceeded,
			CompletionTime: &now,
			Location:       "lion/seed/",
		},
	}
}

// primaryReadyDeployment returns a Deployment matching the first site with
// ReadyReplicas=1 so reconcileRestoreJob will proceed.
func primaryReadyDeployment(fgName, site string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(fgName, site), Namespace: "ns",
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 1,
		},
	}
}

func TestReconcileRestoreJob_NoInitFromBackup_IsNoOp(t *testing.T) {
	fg := fgWithBackup()
	r, _ := newReconciler(fg)
	d, err := r.reconcileRestoreJob(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d != 0 {
		t.Errorf("want no requeue, got %s", d)
	}
}

func TestReconcileRestoreJob_WaitsForPrimaryReady(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	// No deployment object -> not found path.
	r, _ := newReconciler(fg, seed, dsnSecret())

	d, err := r.reconcileRestoreJob(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d == 0 {
		t.Errorf("expected requeue when primary not ready")
	}
}

func TestReconcileRestoreJob_CreatesRestoreJob(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	deploy := primaryReadyDeployment("lion", fg.Spec.Sites[0].Name)

	r, c := newReconciler(fg, seed, deploy, dsnSecret())

	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatalf("reconcileRestoreJob: %v", err)
	}

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: restoreJobName("lion", fg.Spec.Sites[0].Name), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("restore job not created: %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]
	envMap := map[string]string{}
	for _, e := range container.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["BLOODRAVEN_INPUT_URL"] != "lion/seed" {
		t.Errorf("want input url resolved from seed.status.location, got %q", envMap["BLOODRAVEN_INPUT_URL"])
	}
	if envMap["BLOODRAVEN_S3_BUCKET"] != "bloodraven-backups" {
		t.Errorf("want S3 bucket resolved from profile, got %q", envMap["BLOODRAVEN_S3_BUCKET"])
	}
	if len(container.Command) == 0 || container.Command[len(container.Command)-1] != backupScriptsMountPath+"/restore.py" {
		t.Errorf("want restore script command, got %v", container.Command)
	}

	// Status should reflect Running phase.
	if fg.Status.Restore == nil || fg.Status.Restore.Phase != v1alpha1.BackupPhaseRunning {
		t.Errorf("want Restore.Running, got %+v", fg.Status.Restore)
	}
}

// --- Fixes from Copilot review -----------------------------------------

func TestBuildRestoreJob_S3Source_NormalizesTrailingSlash(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			S3: &v1alpha1.S3Storage{
				Bucket:            "my-bucket",
				Prefix:            "dumps/preprod",
				CredentialsSecret: "s3-creds",
			},
		},
	}
	r, _ := newReconciler(fg)
	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "BLOODRAVEN_INPUT_URL" {
			if e.Value != "dumps/preprod" {
				t.Errorf("expected S3 input without trailing slash, got %q", e.Value)
			}
			return
		}
	}
	t.Fatal("BLOODRAVEN_INPUT_URL env not found")
}

func TestBuildRestoreJob_MysqlBackupRef_MissingS3Profile_Errors(t *testing.T) {
	fg := fgWithBackup()
	// Drop the nightly-s3 profile so the lookup fails.
	fg.Spec.Backup.Profiles = fg.Spec.Backup.Profiles[1:]
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	// Seed backup references the now-missing profile.
	seed := succeededSeedBackup()
	r, _ := newReconciler(fg, seed)

	_, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err == nil {
		t.Fatal("expected error for missing S3 profile")
	}
	if !strings.Contains(err.Error(), "profile") {
		t.Errorf("want profile-related error, got %v", err)
	}
}

func TestBuildRestoreJob_MysqlBackupRef_PVCStorage_MountsBackupPVC(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	seed := succeededSeedBackup()
	seed.Spec.ProfileName = "daily-local"
	seed.Status.StorageType = v1alpha1.BackupStoragePVC
	seed.Status.Location = backupPVCMountPath + "/seed/"
	r, _ := newReconciler(fg, seed)

	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]
	envMap := map[string]string{}
	for _, e := range container.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["BLOODRAVEN_INPUT_URL"] != backupPVCMountPath+"/seed/" {
		t.Errorf("want PVC input url preserved, got %q", envMap["BLOODRAVEN_INPUT_URL"])
	}

	var restoreSrcVolume *corev1.Volume
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == "restore-src" {
			restoreSrcVolume = &job.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	if restoreSrcVolume == nil || restoreSrcVolume.PersistentVolumeClaim == nil {
		t.Fatal("expected restore-src PVC volume for mysqlBackupRef PVC source")
	}
	if got, want := restoreSrcVolume.PersistentVolumeClaim.ClaimName, ownedBackupPVCName("lion", "daily-local"); got != want {
		t.Errorf("restore-src claim = %q, want %q", got, want)
	}
	if !restoreSrcVolume.PersistentVolumeClaim.ReadOnly {
		t.Error("expected restore-src PVC volume to be read-only")
	}

	var restoreSrcMount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].Name == "restore-src" {
			restoreSrcMount = &container.VolumeMounts[i]
			break
		}
	}
	if restoreSrcMount == nil {
		t.Fatal("expected restore-src volume mount for mysqlBackupRef PVC source")
	}
	if restoreSrcMount.MountPath != backupPVCMountPath {
		t.Errorf("restore-src mount path = %q, want %q", restoreSrcMount.MountPath, backupPVCMountPath)
	}
	if !restoreSrcMount.ReadOnly {
		t.Error("expected restore-src mount to be read-only")
	}
}

func TestBuildRestoreJob_PVCSource_EmptyClaimName_Errors(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			PVC: &v1alpha1.InitFromBackupPVCSource{SubPath: "dumps"},
		},
	}
	r, _ := newReconciler(fg)
	_, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err == nil {
		t.Fatal("expected error for empty claimName")
	}
	if !strings.Contains(err.Error(), "claimName is required") {
		t.Errorf("want claimName error, got %v", err)
	}
}

func TestEnsureTrailingSlash(t *testing.T) {
	cases := map[string]string{
		"":          "",
		"foo":       "foo/",
		"foo/":      "foo/",
		"s3://b/p":  "s3://b/p/",
		"s3://b/p/": "s3://b/p/",
	}
	for in, want := range cases {
		if got := ensureTrailingSlash(in); got != want {
			t.Errorf("ensureTrailingSlash(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIsS3Location(t *testing.T) {
	cases := map[string]bool{
		"":                 false,
		"/backups/foo/":    false,
		"pvc://foo":        false,
		"s3://bucket/key/": true,
		"lion/seed/":       true,
	}
	for in, want := range cases {
		if got := isS3Location(in); got != want {
			t.Errorf("isS3Location(%q) = %v, want %v", in, got, want)
		}
	}
}

// ------------------------------------------------------------------------

func TestReconcileRestoreJob_JobSucceeded_UpdatesStatus(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	deploy := primaryReadyDeployment("lion", fg.Spec.Sites[0].Name)

	r, c := newReconciler(fg, seed, deploy, dsnSecret())

	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatalf("first pass: %v", err)
	}

	// Mark the job succeeded and re-reconcile.
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: restoreJobName("lion", fg.Spec.Sites[0].Name), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	now := metav1.NewTime(time.Now())
	job.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		LastProbeTime: now, LastTransitionTime: now,
	}}
	job.Status.Succeeded = 1
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	// Re-fetch fg to mirror the state the reconciler would see.
	var fresh v1alpha1.MysqlFailoverGroup
	_ = c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh)

	if _, err := r.reconcileRestoreJob(context.Background(), &fresh); err != nil {
		t.Fatalf("second pass: %v", err)
	}

	if fresh.Status.Restore == nil || fresh.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded {
		t.Errorf("want Succeeded, got %+v", fresh.Status.Restore)
	}
}
