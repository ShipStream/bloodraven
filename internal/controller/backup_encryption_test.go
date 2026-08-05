package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// fgWithEncryptedBackup returns a MysqlFailoverGroup with a single S3
// profile carrying encryption config. Covers the common "encrypt the
// nightly" setup.
func fgWithEncryptedBackup() *v1alpha1.MysqlFailoverGroup {
	base := fgWithBackup()
	base.Spec.Backup.Profiles[0].Encryption = &v1alpha1.BackupEncryptionSpec{
		Algorithm: "AES-256-GCM",
		PassphraseSecret: v1alpha1.PassphraseSecretRef{
			Name: "backup-passphrase",
			Key:  "passphrase",
		},
	}
	return base
}

// TestBuildBackupJob_EncryptedS3_ShapeChanges verifies that turning on
// encryption reshapes the Job into an init-container + main-container
// layout, mounts the passphrase Secret only on the uploader, and
// routes mysqlsh's output at a staging emptyDir.
func TestBuildBackupJob_EncryptedS3_ShapeChanges(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgWithEncryptedBackup()
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-nightly-abc", Namespace: "ns"},
		Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
	}
	job, err := BuildBackupJob(BackupJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Backup:               mb,
		SourceSite:           "pdx",
		CredsSecretName:      "mysqlbackup-lion-nightly-abc-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
	})
	if err != nil {
		t.Fatalf("BuildBackupJob: %v", err)
	}

	if got := len(job.Spec.Template.Spec.InitContainers); got != 1 {
		t.Fatalf("want 1 init container, got %d", got)
	}
	init := job.Spec.Template.Spec.InitContainers[0]
	if init.Name != backupDumpInitContainerName {
		t.Errorf("init container name: got %q want %q", init.Name, backupDumpInitContainerName)
	}
	if init.Image != "mysql/mysql-shell:8.0.34" {
		t.Errorf("init image: got %q", init.Image)
	}

	initEnv := envMap(init.Env)
	if !strings.HasPrefix(initEnv["BLOODRAVEN_OUTPUT_URL"], backupStagingMountPath+"/") {
		t.Errorf("mysqlsh dump must write to staging, got %q", initEnv["BLOODRAVEN_OUTPUT_URL"])
	}
	if initEnv["BLOODRAVEN_STORAGE_TYPE"] != "PVC" {
		t.Errorf("mysqlsh should use PVC (local) storage type in encrypted flow, got %q", initEnv["BLOODRAVEN_STORAGE_TYPE"])
	}
	// The mysqlsh dump container must NOT mount the passphrase
	// Secret — defense in depth.
	for _, m := range init.VolumeMounts {
		if m.Name == "backup-passphrase" {
			t.Errorf("mysqlsh init container must not mount the passphrase Secret")
		}
	}

	if got := len(job.Spec.Template.Spec.Containers); got != 1 {
		t.Fatalf("want 1 main container, got %d", got)
	}
	main := job.Spec.Template.Spec.Containers[0]
	if main.Name != backupEncryptUploadContainerName {
		t.Errorf("main container name: got %q want %q", main.Name, backupEncryptUploadContainerName)
	}
	if main.Image != "bloodraven:test" {
		t.Errorf("main image: got %q want bloodraven:test", main.Image)
	}
	if got := main.Command; len(got) < 2 || got[0] != "bloodraven" || got[1] != "encrypt-upload" {
		t.Errorf("main command: got %v", got)
	}

	mainEnv := envMap(main.Env)
	if mainEnv["BLOODRAVEN_SOURCE_DIR"] == "" {
		t.Errorf("missing BLOODRAVEN_SOURCE_DIR")
	}
	if mainEnv["BLOODRAVEN_STORAGE_TYPE"] != "S3" {
		t.Errorf("main storage type: got %q want S3", mainEnv["BLOODRAVEN_STORAGE_TYPE"])
	}
	if mainEnv["BLOODRAVEN_BACKUP_PASSPHRASE_FILE"] == "" {
		t.Errorf("main should reference BLOODRAVEN_BACKUP_PASSPHRASE_FILE")
	}
	if mainEnv["BLOODRAVEN_ENCRYPTION_ALGORITHM"] != "AES-256-GCM" {
		t.Errorf("main algorithm env: got %q", mainEnv["BLOODRAVEN_ENCRYPTION_ALGORITHM"])
	}
	if mainEnv["BLOODRAVEN_S3_BUCKET"] == "" {
		t.Errorf("main must carry S3 bucket env")
	}

	// Staging volume must exist; passphrase Secret must be attached
	// and mounted only on the uploader.
	var hasStaging, hasPass bool
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "staging" {
			hasStaging = true
		}
		if v.Name == "backup-passphrase" {
			hasPass = true
			if v.Secret == nil || v.Secret.SecretName != "backup-passphrase" {
				t.Errorf("backup-passphrase volume: %+v", v.Secret)
			}
		}
	}
	if !hasStaging || !hasPass {
		t.Errorf("staging=%v passphrase=%v; both required", hasStaging, hasPass)
	}

	// Labels should carry the encrypted marker.
	if got := job.Labels[labelBackupEncrypted]; got != "true" {
		t.Errorf("encrypted label: got %q want true", got)
	}
}

// TestBuildBackupJob_EncryptedRequiresOperatorImage ensures we refuse
// to build an encrypted Job when the operator image env isn't set up,
// because the main container needs the bloodraven binary.
func TestBuildBackupJob_EncryptedRequiresOperatorImage(t *testing.T) {
	SetOperatorImageDefaults("", "")
	fg := fgWithEncryptedBackup()
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-nightly-abc", Namespace: "ns"},
		Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
	}
	_, err := BuildBackupJob(BackupJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Backup:               mb,
		SourceSite:           "pdx",
		CredsSecretName:      "mysqlbackup-lion-nightly-abc-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
	})
	if err == nil {
		t.Fatalf("expected error when operator image is unset")
	}
	if !strings.Contains(err.Error(), "operator image is not configured") {
		t.Errorf("error message: %v", err)
	}
}

// TestBuildBackupJob_EncryptedRequiresSecretName covers the zero-value
// Secret reference; the builder should fail fast with a clear error
// rather than producing a volume with an empty SecretName.
func TestBuildBackupJob_EncryptedRequiresSecretName(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgWithEncryptedBackup()
	fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name = ""
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-nightly-abc", Namespace: "ns"},
		Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
	}
	_, err := BuildBackupJob(BackupJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Backup:               mb,
		SourceSite:           "pdx",
		CredsSecretName:      "mysqlbackup-lion-nightly-abc-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
	})
	if err == nil {
		t.Fatalf("expected error when passphraseSecret.name is empty")
	}
}

// TestValidateEncryptionSecret exercises the configuration-validation
// helper the failover-group reconciler runs on every pass. Missing
// Secret, missing key, and empty value should all surface as errors
// (and then as BackupEncryptionInvalid events in the higher layer).
func TestValidateEncryptionSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	fg := fgWithEncryptedBackup()

	// Scenario: Secret missing entirely.
	r := &MysqlFailoverGroupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
	}
	err := r.validateEncryptionSecret(context.Background(), fg, &fg.Spec.Backup.Profiles[0])
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("missing Secret: want 'not found' error, got %v", err)
	}

	// Scenario: Secret present but missing the expected key.
	bad := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-passphrase", Namespace: "ns"},
		Data:       map[string][]byte{"other-key": []byte("hello")},
	}
	r = &MysqlFailoverGroupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(bad).Build(),
		Scheme: scheme,
	}
	err = r.validateEncryptionSecret(context.Background(), fg, &fg.Spec.Backup.Profiles[0])
	if err == nil || !strings.Contains(err.Error(), "does not contain key") {
		t.Errorf("wrong key: want 'does not contain key' error, got %v", err)
	}

	// Scenario: Secret present but value is empty/whitespace.
	empty := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-passphrase", Namespace: "ns"},
		Data:       map[string][]byte{"passphrase": []byte("   \n")},
	}
	r = &MysqlFailoverGroupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(empty).Build(),
		Scheme: scheme,
	}
	err = r.validateEncryptionSecret(context.Background(), fg, &fg.Spec.Backup.Profiles[0])
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty value: want 'empty' error, got %v", err)
	}

	// Happy path.
	good := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-passphrase", Namespace: "ns"},
		Data:       map[string][]byte{"passphrase": []byte("correct-horse\n")},
	}
	r = &MysqlFailoverGroupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(good).Build(),
		Scheme: scheme,
	}
	if err := r.validateEncryptionSecret(context.Background(), fg, &fg.Spec.Backup.Profiles[0]); err != nil {
		t.Errorf("happy path: %v", err)
	}
}

// TestParseDumpCompleteLine_Encrypted checks that the log-tail parser
// correctly rehydrates the `encrypted` + `algorithm` tokens the
// `bloodraven encrypt-upload` container adds to its DUMP_COMPLETE line.
func TestParseDumpCompleteLine_Encrypted(t *testing.T) {
	line := "BLOODRAVEN_DUMP_COMPLETE location=s3://bucket/prefix/ sizeBytes=1572864 " +
		"size=1.4_GiB gtidExecuted=abc:1-10 binlogFile=mysql-bin.000042 binlogPos=118 " +
		"ciphertextBytes=1580000 files=42 encrypted=true algorithm=AES-256-GCM"
	meta, ok := parseDumpCompleteLine(line)
	if !ok {
		t.Fatalf("parse failed")
	}
	if !meta.Encrypted {
		t.Errorf("encrypted flag not parsed")
	}
	if meta.EncryptionAlgorithm != "AES-256-GCM" {
		t.Errorf("algorithm: got %q", meta.EncryptionAlgorithm)
	}
}

// silence unused import warnings when the test file grows
var _ reconcile.Reconciler = (*MysqlFailoverGroupReconciler)(nil)

// TestBuildRestoreJob_EncryptedSource_AddsDecryptInitContainer covers
// the restore-side shape change: when the source MysqlBackup declares
// status.Encrypted=true the builder must add a `decrypt-download`
// init container, mount the passphrase Secret on it, and rewrite the
// main container's BLOODRAVEN_INPUT_URL to point at the decrypted
// staging emptyDir instead of the S3 bucket.
func TestBuildRestoreJob_EncryptedSource_AddsDecryptInitContainer(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgWithEncryptedBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	seed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "seed", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:               v1alpha1.BackupPhaseSucceeded,
			Location:            "lion/seed/",
			StorageType:         v1alpha1.BackupStorageS3,
			Encrypted:           true,
			EncryptionAlgorithm: "AES-256-GCM",
		},
	}
	r, _ := newReconciler(fg, seed)

	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}

	// decrypt-download init container must be present.
	var sawDecrypt bool
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.Name == "decrypt-download" {
			sawDecrypt = true
			if c.Image != "bloodraven:test" {
				t.Errorf("decrypt image: got %q", c.Image)
			}
			env := envMap(c.Env)
			if env["BLOODRAVEN_STORAGE_TYPE"] != "S3" {
				t.Errorf("decrypt storage type: got %q", env["BLOODRAVEN_STORAGE_TYPE"])
			}
			if env["BLOODRAVEN_SOURCE_PREFIX"] == "" {
				t.Errorf("decrypt source prefix missing")
			}
			if env["BLOODRAVEN_BACKUP_PASSPHRASE_FILE"] == "" {
				t.Errorf("decrypt passphrase file missing")
			}
		}
	}
	if !sawDecrypt {
		t.Fatalf("decrypt-download init container missing")
	}

	// Main container's INPUT_URL must be pointed at the decrypted dir
	// rather than the S3 prefix. S3 env vars must have been scrubbed.
	main := job.Spec.Template.Spec.Containers[0]
	env := envMap(main.Env)
	if env["BLOODRAVEN_INPUT_URL"] != "/restore-decrypted" {
		t.Errorf("main INPUT_URL: got %q want /restore-decrypted", env["BLOODRAVEN_INPUT_URL"])
	}
	if _, present := env["BLOODRAVEN_S3_BUCKET"]; present {
		t.Errorf("main must not carry BLOODRAVEN_S3_BUCKET after decrypt rewiring")
	}
	if _, present := env["BLOODRAVEN_AWS_CREDS_DIR"]; present {
		t.Errorf("main must not carry BLOODRAVEN_AWS_CREDS_DIR after decrypt rewiring")
	}

	// Passphrase volume and decrypted staging volume must be attached
	// to the pod.
	var hasPass, hasStaging bool
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "backup-passphrase" {
			hasPass = true
		}
		if v.Name == "restore-decrypted" {
			hasStaging = true
		}
	}
	if !hasPass || !hasStaging {
		t.Errorf("passphrase=%v staging=%v; both required", hasPass, hasStaging)
	}
}

// TestBuildRestoreJob_EncryptedSource_NoPassphraseSecretErrors verifies
// that the restore fails fast when the source is encrypted but the
// profile has no passphrase Secret (for example, the profile was
// stripped between the backup and the restore).
func TestBuildRestoreJob_EncryptedSource_NoPassphraseSecretErrors(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgWithBackup() // profile has no encryption config.
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	seed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "seed", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:       v1alpha1.BackupPhaseSucceeded,
			Location:    "lion/seed/",
			StorageType: v1alpha1.BackupStorageS3,
			Encrypted:   true,
		},
	}
	r, _ := newReconciler(fg, seed)
	_, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err == nil {
		t.Fatalf("expected error when encrypted source lacks a passphrase Secret")
	}
	if !strings.Contains(err.Error(), "passphrase") {
		t.Errorf("error should mention passphrase, got %v", err)
	}
}
