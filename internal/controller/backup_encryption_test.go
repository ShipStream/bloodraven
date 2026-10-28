package controller

import (
	"context"
	"fmt"
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
	if got := main.Command; len(got) < 2 || got[0] != operatorBinaryPath || got[1] != "encrypt-upload" {
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

	// Scenario: Secret value shorter than the minimum passphrase length.
	short := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-passphrase", Namespace: "ns"},
		Data:       map[string][]byte{"passphrase": []byte("correct-horse\n")},
	}
	r = &MysqlFailoverGroupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(short).Build(),
		Scheme: scheme,
	}
	err = r.validateEncryptionSecret(context.Background(), fg, &fg.Spec.Backup.Profiles[0])
	if err == nil || !strings.Contains(err.Error(), "13-byte passphrase") {
		t.Errorf("short value: want passphrase length error, got %v", err)
	}

	// Happy path.
	good := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-passphrase", Namespace: "ns"},
		Data:       map[string][]byte{"passphrase": []byte(strings.Repeat("k", 32) + "\n")},
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

func TestSplitS3Location(t *testing.T) {
	cases := []struct{ loc, bucket, key string }{
		{"lion/seed/", "", "lion/seed"},
		{"lion/seed", "", "lion/seed"},
		{"s3://bkt/lion/seed/", "bkt", "lion/seed"},
		{"s3://bkt/lion/seed", "bkt", "lion/seed"},
		{"s3://bkt/", "bkt", ""},
		{"s3://bkt", "bkt", ""},
	}
	for _, tc := range cases {
		bucket, key := splitS3Location(tc.loc)
		if bucket != tc.bucket || key != tc.key {
			t.Errorf("splitS3Location(%q) = (%q, %q), want (%q, %q)", tc.loc, bucket, key, tc.bucket, tc.key)
		}
	}
}

// TestEncryptedS3Location_DecryptSourceIsObjectKey covers the
// verification and restore halves of the encrypted S3 round trip.
// encrypt-upload records status.location as "s3://<bucket>/<prefix>/",
// and decrypt-download must get the bare object-key prefix plus that
// bucket. Passing the URL through made RustFS reject the listing with
// InvalidArgument (found by the 30-encrypted-backup-verification-rustfs
// playground scenario).
func TestEncryptedS3Location_DecryptSourceIsObjectKey(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	for _, tc := range []struct {
		name, location, wantBucket string
	}{
		{"url in profile bucket", "s3://bloodraven-backups/lion/seed/", "bloodraven-backups"},
		{"url in a bucket the profile no longer names", "s3://old-bucket/lion/seed/", "old-bucket"},
		{"bare prefix", "lion/seed/", "bloodraven-backups"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithEncryptedBackup()
			seed := successfulBackup("seed", fg.Name, "nightly-s3")
			seed.Status.Location = tc.location
			seed.Status.Encrypted = true
			seed.Status.EncryptionAlgorithm = "AES-256-GCM"

			verify := &v1alpha1.MysqlBackupVerification{
				ObjectMeta: metav1.ObjectMeta{Name: "verify", Namespace: "ns"},
				Spec:       v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name}, ProfileName: "nightly-s3"},
			}
			vjob, err := buildVerificationJob(verificationJobInputs{
				FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: verify, Backup: seed,
				CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
			})
			if err != nil {
				t.Fatalf("buildVerificationJob: %v", err)
			}

			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
				Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: seed.Name}},
			}
			r, _ := newReconciler(fg, seed)
			rjob, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
			if err != nil {
				t.Fatalf("buildRestoreJob: %v", err)
			}

			for kind, spec := range map[string]corev1.PodSpec{"verification": vjob.Spec.Template.Spec, "restore": rjob.Spec.Template.Spec} {
				var saw bool
				for _, c := range spec.InitContainers {
					if c.Name != "decrypt-download" {
						continue
					}
					saw = true
					env := envMap(c.Env)
					if got := env["BLOODRAVEN_SOURCE_PREFIX"]; got != "lion/seed" {
						t.Errorf("%s: BLOODRAVEN_SOURCE_PREFIX = %q, want %q", kind, got, "lion/seed")
					}
					if got := env["BLOODRAVEN_S3_BUCKET"]; got != tc.wantBucket {
						t.Errorf("%s: BLOODRAVEN_S3_BUCKET = %q, want %q", kind, got, tc.wantBucket)
					}
				}
				if !saw {
					t.Errorf("%s: decrypt-download init container missing", kind)
				}
			}
		})
	}
}

// encryptedRefSeed returns a Succeeded, encrypted MysqlBackup written by
// profileName to the given backend.
func encryptedRefSeed(profileName string, storage v1alpha1.BackupStorageType) *v1alpha1.MysqlBackup {
	seed := succeededSeedBackup()
	seed.Spec.ProfileName = profileName
	seed.Status.StorageType = storage
	seed.Status.Encrypted = true
	seed.Status.EncryptionAlgorithm = "AES-256-GCM"
	if storage == v1alpha1.BackupStoragePVC {
		seed.Status.Location = backupPVCMountPath + "/seed/"
	}
	return seed
}

// TestBuildRestoreJob_EncryptedRef_ProfileDrift_ReturnsError covers an
// encrypted mysqlBackupRef whose profile was deleted or moved to the
// other storage backend after the backup ran. Both restore builders must
// return the source-resolution error instead of dereferencing the
// profile's storage pointer for the backend the backup was written to.
func TestBuildRestoreJob_EncryptedRef_ProfileDrift_ReturnsError(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	explicit := &v1alpha1.BackupDecryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "restore-pass", Key: "passphrase"}}
	encryption := &v1alpha1.BackupEncryptionSpec{
		Algorithm:        "AES-256-GCM",
		PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "backup-passphrase", Key: "passphrase"},
	}
	s3Storage := v1alpha1.BackupStorage{
		Type: v1alpha1.BackupStorageS3,
		S3:   &v1alpha1.S3Storage{Bucket: "moved", Prefix: "lion", CredentialsSecret: "s3-creds"},
	}
	pvcStorage := v1alpha1.BackupStorage{
		Type: v1alpha1.BackupStoragePVC,
		PVC:  &v1alpha1.PVCStorage{ClaimName: "moved"},
	}

	type backend struct {
		name        string
		profile     string
		storage     v1alpha1.BackupStorageType
		other       v1alpha1.BackupStorage
		otherName   string
		sourceField string
	}
	backends := []backend{
		{"s3", "nightly-s3", v1alpha1.BackupStorageS3, pvcStorage, "PVC", "source.s3"},
		{"pvc", "daily-local", v1alpha1.BackupStoragePVC, s3Storage, "S3", "source.pvc"},
	}
	setups := []struct {
		name       string
		decryption *v1alpha1.BackupDecryptionSpec
		mutate     func(fg *v1alpha1.MysqlFailoverGroup, b backend)
		wantMsg    func(b backend) string
	}{
		{
			name:       "profile missing, explicit decryption",
			decryption: explicit,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup, b backend) {
				kept := fg.Spec.Backup.Profiles[:0]
				for _, p := range fg.Spec.Backup.Profiles {
					if p.Name != b.profile {
						kept = append(kept, p)
					}
				}
				fg.Spec.Backup.Profiles = kept
			},
			wantMsg: func(b backend) string { return "is missing from spec.backup.profiles" },
		},
		{
			name: "profile moved to other backend, encryption retained",
			mutate: func(fg *v1alpha1.MysqlFailoverGroup, b backend) {
				p := findProfile(fg, b.profile)
				p.Storage = b.other
				p.Encryption = encryption
			},
			wantMsg: func(b backend) string { return "now stores to " + b.otherName },
		},
		{
			name:       "profile moved to other backend, explicit decryption",
			decryption: explicit,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup, b backend) {
				findProfile(fg, b.profile).Storage = b.other
			},
			wantMsg: func(b backend) string { return "now stores to " + b.otherName },
		},
	}
	callers := []struct {
		name      string
		fieldPath string
		build     func(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource, dec *v1alpha1.BackupDecryptionSpec) error
	}{
		{"bootstrap", "initFromBackup", func(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource, dec *v1alpha1.BackupDecryptionSpec) error {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: src, Decryption: dec}
			_, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
			return err
		}},
		{"in-place", "restoreInPlace", func(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource, dec *v1alpha1.BackupDecryptionSpec) error {
			fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "2026-04-17T14:00:00Z", Source: src, Decryption: dec}
			_, err := r.buildInPlaceRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds", true, "")
			return err
		}},
	}

	for _, b := range backends {
		for _, s := range setups {
			for _, c := range callers {
				t.Run(b.name+"/"+s.name+"/"+c.name, func(t *testing.T) {
					fg := fgWithBackup()
					s.mutate(fg, b)
					seed := encryptedRefSeed(b.profile, b.storage)
					r, _ := newReconciler(fg, seed)
					src := v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: seed.Name}}

					var err error
					func() {
						defer func() {
							if p := recover(); p != nil {
								t.Fatalf("restore builder panicked instead of returning an error: %v", p)
							}
						}()
						err = c.build(r, fg, src, s.decryption)
					}()
					if err == nil {
						t.Fatal("expected a source-resolution error, got nil")
					}
					for _, want := range []string{
						fmt.Sprintf("%q", b.profile),
						s.wantMsg(b),
						c.fieldPath + "." + b.sourceField,
					} {
						if !strings.Contains(err.Error(), want) {
							t.Errorf("error %q does not mention %q", err, want)
						}
					}
				})
			}
		}
	}
}

// TestBuildRestoreJob_EncryptedPVCRef_AddsDecryptInitContainer is the
// PVC control for the drift test above: an intact encrypted PVC profile
// still produces a PVC decrypt step reading the profile's claim.
func TestBuildRestoreJob_EncryptedPVCRef_AddsDecryptInitContainer(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgWithBackup()
	p := findProfile(fg, "daily-local")
	p.Encryption = &v1alpha1.BackupEncryptionSpec{
		Algorithm:        "AES-256-GCM",
		PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "backup-passphrase", Key: "passphrase"},
	}
	seed := encryptedRefSeed("daily-local", v1alpha1.BackupStoragePVC)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: seed.Name}},
	}
	r, _ := newReconciler(fg, seed)

	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}
	var saw bool
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.Name != "decrypt-download" {
			continue
		}
		saw = true
		env := envMap(c.Env)
		if got := env["BLOODRAVEN_STORAGE_TYPE"]; got != "PVC" {
			t.Errorf("decrypt storage type = %q, want PVC", got)
		}
		if got := env["BLOODRAVEN_SOURCE_PREFIX"]; got != "seed" {
			t.Errorf("decrypt source prefix = %q, want %q", got, "seed")
		}
	}
	if !saw {
		t.Fatal("decrypt-download init container missing")
	}
	var claim string
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			claim = v.PersistentVolumeClaim.ClaimName
		}
	}
	if want := ownedBackupPVCName(fg.Name, "daily-local"); claim != want {
		t.Errorf("decrypt source claim = %q, want %q", claim, want)
	}
}
