package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
				Prefix:            "dumps/preprod/",
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
		return
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

// TestBuildRestoreJob_DisablesLoadProgressFile covers every restore
// source through both builders. mysqlsh's default progressFile is
// written beside the dump, but restore sources are read-only (source
// PVCs and the decrypted staging dir), and for S3 sources a stale
// progress file in the backup prefix would let a re-run skip the load
// after an in-place DROP. Restore Jobs run once (RestartPolicyNever,
// backoffLimit 0), so they must always disable progress tracking.
func TestBuildRestoreJob_DisablesLoadProgressFile(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	decryption := &v1alpha1.BackupDecryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "restore-pass", Key: "passphrase"}}
	encryptProfile := func(fg *v1alpha1.MysqlFailoverGroup, name string) {
		findProfile(fg, name).Encryption = &v1alpha1.BackupEncryptionSpec{
			Algorithm:        "AES-256-GCM",
			PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "backup-passphrase", Key: "passphrase"},
		}
	}
	refSrc := v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"}}

	sources := []struct {
		name       string
		src        v1alpha1.InitFromBackupSource
		decryption *v1alpha1.BackupDecryptionSpec
		setup      func(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlBackup
		roMount    string // source mount the main container reads; "" for S3
	}{
		{name: "direct s3", src: v1alpha1.InitFromBackupSource{S3: &v1alpha1.S3Storage{Bucket: "b", Prefix: "dumps/x", CredentialsSecret: "s3-creds"}}},
		{name: "direct pvc", src: v1alpha1.InitFromBackupSource{PVC: &v1alpha1.InitFromBackupPVCSource{ClaimName: "dump-pvc", SubPath: "x"}}, roMount: "/restore"},
		{name: "direct pvc encrypted", src: v1alpha1.InitFromBackupSource{PVC: &v1alpha1.InitFromBackupPVCSource{ClaimName: "dump-pvc", SubPath: "x"}}, decryption: decryption, roMount: "/restore-decrypted"},
		{name: "ref s3", src: refSrc, setup: func(*v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlBackup { return succeededSeedBackup() }},
		{name: "ref pvc", src: refSrc, roMount: backupPVCMountPath, setup: func(*v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlBackup {
			seed := succeededSeedBackup()
			seed.Spec.ProfileName = "daily-local"
			seed.Status.StorageType = v1alpha1.BackupStoragePVC
			seed.Status.Location = backupPVCMountPath + "/seed/"
			return seed
		}},
		{name: "ref s3 encrypted", src: refSrc, roMount: "/restore-decrypted", setup: func(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlBackup {
			encryptProfile(fg, "nightly-s3")
			return encryptedRefSeed("nightly-s3", v1alpha1.BackupStorageS3)
		}},
		{name: "ref pvc encrypted", src: refSrc, roMount: "/restore-decrypted", setup: func(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlBackup {
			encryptProfile(fg, "daily-local")
			return encryptedRefSeed("daily-local", v1alpha1.BackupStoragePVC)
		}},
	}
	callers := []struct {
		name  string
		build func(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource, dec *v1alpha1.BackupDecryptionSpec) (*batchv1.Job, error)
	}{
		{"bootstrap", func(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource, dec *v1alpha1.BackupDecryptionSpec) (*batchv1.Job, error) {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: src, Decryption: dec}
			return r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
		}},
		{"in-place full-instance", func(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource, dec *v1alpha1.BackupDecryptionSpec) (*batchv1.Job, error) {
			fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "2026-04-17T14:00:00Z", Source: src, Decryption: dec}
			return r.buildInPlaceRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds", true, "")
		}},
		{"in-place per-schema", func(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource, dec *v1alpha1.BackupDecryptionSpec) (*batchv1.Job, error) {
			fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{
				Confirm: "2026-04-17T14:00:00Z", Source: src, Decryption: dec,
				LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{"orders"}},
			}
			return r.buildInPlaceRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds", false, "orders")
		}},
	}

	for _, s := range sources {
		for _, c := range callers {
			t.Run(s.name+"/"+c.name, func(t *testing.T) {
				fg := fgWithBackup()
				var objs []client.Object
				if s.setup != nil {
					objs = append(objs, s.setup(fg))
				}
				r, _ := newReconciler(append([]client.Object{fg}, objs...)...)
				job, err := c.build(r, fg, s.src, s.decryption)
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				main := job.Spec.Template.Spec.Containers[0]
				var progress *corev1.EnvVar
				for i := range main.Env {
					if main.Env[i].Name == "BLOODRAVEN_LOAD_PROGRESS_FILE" {
						progress = &main.Env[i]
					}
				}
				if progress == nil {
					t.Fatal("BLOODRAVEN_LOAD_PROGRESS_FILE is not set; mysqlsh would write its progress file beside the read-only dump")
				}
				if progress.Value != "" {
					t.Errorf("BLOODRAVEN_LOAD_PROGRESS_FILE = %q, want \"\" (progress tracking disabled)", progress.Value)
				}
				if s.roMount == "" {
					return
				}
				var found bool
				for _, m := range main.VolumeMounts {
					if m.MountPath == s.roMount {
						found = true
						if !m.ReadOnly {
							t.Errorf("source mount %s must stay read-only", s.roMount)
						}
					}
				}
				if !found {
					t.Errorf("main container has no source mount at %s", s.roMount)
				}
			})
		}
	}
}

// jobLoadOptions decodes the main container's BLOODRAVEN_LOAD_OPTIONS.
func jobLoadOptions(t *testing.T, job *batchv1.Job) map[string]any {
	t.Helper()
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name != "BLOODRAVEN_LOAD_OPTIONS" {
			continue
		}
		opts := map[string]any{}
		if err := json.Unmarshal([]byte(e.Value), &opts); err != nil {
			t.Fatalf("decode BLOODRAVEN_LOAD_OPTIONS %q: %v", e.Value, err)
		}
		return opts
	}
	t.Fatal("BLOODRAVEN_LOAD_OPTIONS not set")
	return nil
}

// TestBuildRestoreJob_PITRLoadsDumpGtidBaseline covers the GTID baseline
// a bootstrap PITR restore needs. The archive replay keeps original
// GTIDs and the downloaded binlogs include transactions already in the
// dump, so the fresh target must have the dump's gtidExecuted in
// gtid_purged (updateGtidSet:"replace", which mysqlsh requires to run
// with skipBinlog:true) before replay, or MySQL re-executes them. The
// baseline is bootstrap-PITR only: without PITR nothing needs it, and a
// live in-place target already has its own GTID history.
func TestBuildRestoreJob_PITRLoadsDumpGtidBaseline(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	f, tr := false, true
	pit := &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z"}

	for _, tc := range []struct {
		name        string
		pit         *v1alpha1.PointInTimeSpec
		loadOptions *v1alpha1.LoadOptions
		wantGtidSet bool
		wantSkipBin any // nil = key absent
		wantThreads any
		wantErr     string
	}{
		{name: "pitr, loadOptions omitted", pit: pit, wantGtidSet: true, wantSkipBin: true},
		{name: "pitr, skipBinlog unset", pit: pit, loadOptions: &v1alpha1.LoadOptions{Threads: 8}, wantGtidSet: true, wantSkipBin: true, wantThreads: float64(8)},
		{name: "pitr, skipBinlog true", pit: pit, loadOptions: &v1alpha1.LoadOptions{SkipBinlog: &tr}, wantGtidSet: true, wantSkipBin: true},
		{name: "pitr, skipBinlog false rejected", pit: pit, loadOptions: &v1alpha1.LoadOptions{SkipBinlog: &f}, wantErr: "initFromBackup.loadOptions.skipBinlog"},
		{name: "no pitr, loadOptions omitted", wantSkipBin: nil},
		{name: "no pitr, skipBinlog false kept", loadOptions: &v1alpha1.LoadOptions{SkipBinlog: &f}, wantSkipBin: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInitFromMysqlBackup()
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
			fg.Spec.InitFromBackup.PointInTime = tc.pit
			fg.Spec.InitFromBackup.LoadOptions = tc.loadOptions
			r, _ := newReconciler(fg, succeededSeedBackup())

			job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error mentioning %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildRestoreJob: %v", err)
			}
			opts := jobLoadOptions(t, job)
			if got, ok := opts["updateGtidSet"]; tc.wantGtidSet != ok || (ok && got != "replace") {
				t.Errorf("updateGtidSet = %v (present=%v), want present=%v with \"replace\"", got, ok, tc.wantGtidSet)
			}
			if got := opts["skipBinlog"]; got != tc.wantSkipBin {
				t.Errorf("skipBinlog = %v, want %v", got, tc.wantSkipBin)
			}
			if got := opts["threads"]; got != tc.wantThreads {
				t.Errorf("threads = %v, want %v", got, tc.wantThreads)
			}
		})
	}
}

// TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline pins that
// a restore into a live primary (in-place) never gets updateGtidSet, PITR
// or not: "replace" would collide with the history that primary already
// executed. It also pins the builder's refusal to combine the fresh-target
// baseline with the in-place destructive preflight env.
func TestBuildRestoreJobSpec_LiveTargetNeverLoadsDumpGtidBaseline(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	pit := &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z"}

	newFG := func() *v1alpha1.MysqlFailoverGroup {
		fg := fgInPlaceRestore("")
		fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
		return fg
	}

	for _, tc := range []struct {
		name     string
		schema   string
		fullInst bool
	}{
		{name: "full-instance", fullInst: true},
		{name: "per-schema", schema: "orders"},
	} {
		t.Run("in-place "+tc.name, func(t *testing.T) {
			fg := newFG()
			if tc.schema != "" {
				fg.Spec.RestoreInPlace.LoadOptions = &v1alpha1.LoadOptions{IncludeSchemas: []string{tc.schema}}
			}
			r, _ := newReconciler(fg, succeededSeedBackup())
			job, err := r.buildInPlaceRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds", tc.fullInst, tc.schema)
			if err != nil {
				t.Fatalf("buildInPlaceRestoreJob: %v", err)
			}
			if got, ok := jobLoadOptions(t, job)["updateGtidSet"]; ok {
				t.Errorf("in-place restore must not set updateGtidSet, got %v", got)
			}
		})
	}

	t.Run("live target with pitr", func(t *testing.T) {
		fg := newFG()
		r, _ := newReconciler(fg, succeededSeedBackup())
		job, err := r.buildRestoreJobSpec(context.Background(), fg, restoreJobInputs{
			TargetSite:  fg.Spec.Sites[0].Name,
			CredsName:   "creds",
			Source:      fg.Spec.RestoreInPlace.Source,
			PointInTime: pit,
			FieldPath:   "restoreInPlace",
		})
		if err != nil {
			t.Fatalf("buildRestoreJobSpec: %v", err)
		}
		if got, ok := jobLoadOptions(t, job)["updateGtidSet"]; ok {
			t.Errorf("a non-fresh target must not set updateGtidSet, got %v", got)
		}
	})

	t.Run("fresh target with destructive preflight refused", func(t *testing.T) {
		fg := newFG()
		r, _ := newReconciler(fg, succeededSeedBackup())
		_, err := r.buildRestoreJobSpec(context.Background(), fg, restoreJobInputs{
			TargetSite:  fg.Spec.Sites[0].Name,
			CredsName:   "creds",
			Source:      fg.Spec.RestoreInPlace.Source,
			PointInTime: pit,
			FreshTarget: true,
			ExtraEnv:    []corev1.EnvVar{{Name: "BLOODRAVEN_DROP_ALL_USER_SCHEMAS", Value: "1"}},
		})
		if err == nil || !strings.Contains(err.Error(), "in-place preflight BLOODRAVEN_DROP_ALL_USER_SCHEMAS") {
			t.Fatalf("want the fresh-target/in-place-preflight guard error, got %v", err)
		}
	})
}
