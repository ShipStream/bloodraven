package controller

import (
	"context"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// A-96/D-12/Q11b: restores, verifications and the PITR dump-meta source
// read the bucket, endpoint, region and claim the producing Job recorded in
// status.artifactStorage, never the profile's current destination. Only
// the S3 credentials come from the profile.

func recordedS3Seed() *v1alpha1.MysqlBackup {
	seed := succeededSeedBackup()
	seed.Status.ArtifactStorage = &v1alpha1.BackupArtifactStorage{
		Type: v1alpha1.BackupStorageS3, Location: "recorded-prefix/seed",
		Bucket: "recorded-bucket", EndpointURL: "https://recorded.example", Region: "eu-west-9",
	}
	return seed
}

func recordedPVCSeed() *v1alpha1.MysqlBackup {
	seed := succeededSeedBackup()
	seed.Spec.ProfileName = "daily-local"
	seed.Status.ArtifactStorage = &v1alpha1.BackupArtifactStorage{
		Type: v1alpha1.BackupStoragePVC, Location: backupPVCMountPath + "/recorded/seed", ClaimName: "recorded-claim",
	}
	return seed
}

func claimNames(vols []corev1.Volume) []string {
	var out []string
	for _, v := range vols {
		if v.PersistentVolumeClaim != nil {
			out = append(out, v.PersistentVolumeClaim.ClaimName)
		}
	}
	return out
}

func requireRecordedS3Env(t *testing.T, what string, env map[string]string) {
	t.Helper()
	for k, want := range map[string]string{
		"BLOODRAVEN_S3_BUCKET":            "recorded-bucket",
		"BLOODRAVEN_S3_ENDPOINT_OVERRIDE": "https://recorded.example",
		"AWS_REGION":                      "eu-west-9",
	} {
		if env[k] != want {
			t.Errorf("%s: %s = %q, want the recorded %q", what, k, env[k], want)
		}
	}
}

func TestVerificationJob_ReadsRecordedDestination(t *testing.T) {
	fg := verifyFG() // nightly-s3 now names bloodraven-backups, minio.local, us-east-1
	seed := recordedS3Seed()
	job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Backup: seed,
		Verification: verifyCR("verify", fg.Name, "nightly-s3"), CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil {
		t.Fatal(err)
	}
	env := envMap(job.Spec.Template.Spec.Containers[0].Env)
	requireRecordedS3Env(t, "verification", env)
	if env["BLOODRAVEN_INPUT_URL"] != mysqlShellDumpInput("recorded-prefix/seed", v1alpha1.BackupStorageS3) {
		t.Errorf("verification input %q", env["BLOODRAVEN_INPUT_URL"])
	}
	if v := findVolume(job.Spec.Template.Spec.Volumes, "aws-creds"); v == nil || v.Secret.SecretName != "s3-creds" {
		t.Errorf("credentials do not come from the profile: %+v", v)
	}

	pvc := recordedPVCSeed()
	job, err = buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[1], Backup: pvc,
		Verification: verifyCR("verify", fg.Name, "daily-local"), CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil {
		t.Fatal(err)
	}
	if got := claimNames(job.Spec.Template.Spec.Volumes); len(got) != 1 || got[0] != "recorded-claim" {
		t.Errorf("verification mounts %v, want the recorded claim", got)
	}
}

func TestRestoreJob_ReadsRecordedDestination(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := recordedS3Seed()
	r, _ := newReconciler(fg, seed)
	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatal(err)
	}
	env := envMap(job.Spec.Template.Spec.Containers[0].Env)
	requireRecordedS3Env(t, "restore", env)
	if v := findVolume(job.Spec.Template.Spec.Volumes, "aws-creds"); v == nil || v.Secret.SecretName != "s3-creds" {
		t.Errorf("credentials do not come from the profile: %+v", v)
	}

	fg = fgInitFromMysqlBackup()
	pvc := recordedPVCSeed()
	fg.Spec.InitFromBackup.Source.BackupRef.Name = pvc.Name
	r, _ = newReconciler(fg, pvc)
	job, err = r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatal(err)
	}
	if got := claimNames(job.Spec.Template.Spec.Volumes); len(got) != 1 || got[0] != "recorded-claim" {
		t.Errorf("restore mounts %v, want the recorded claim", got)
	}
}

func TestPITRDumpSource_ReadsRecordedDestination(t *testing.T) {
	fg := fgWithBackup()
	src, err := pitrDumpSourceForBackup(recordedS3Seed(), &fg.Spec.Backup.Profiles[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if src.S3Bucket != "recorded-bucket" || src.S3EndpointURL != "https://recorded.example" || src.S3Region != "eu-west-9" ||
		src.Prefix != "recorded-prefix/seed" || src.AWSCredsSecret != "s3-creds" {
		t.Errorf("S3 dump source %+v", src)
	}
	src, err = pitrDumpSourceForBackup(recordedPVCSeed(), &fg.Spec.Backup.Profiles[1], nil)
	if err != nil {
		t.Fatal(err)
	}
	if src.PVCClaim != "recorded-claim" || src.Prefix != "recorded/seed" {
		t.Errorf("PVC dump source %+v", src)
	}
	none := succeededSeedBackup()
	none.Status.ArtifactStorage = nil
	if _, err := pitrDumpSourceForBackup(none, &fg.Spec.Backup.Profiles[0], nil); err == nil {
		t.Error("a backup with no recorded destination was given a dump source")
	}
}
