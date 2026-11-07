package controller

import (
	"context"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// expandContainerEnv applies kubelet's single-pass $(VAR) expansion to a
// container's env in declaration order and returns the resolved values;
// a value that depends on an unknown input is reported as such.
func expandContainerEnv(t *testing.T, c corev1.Container) map[string]backupEnvironmentValue {
	t.Helper()
	seen := map[string]backupEnvironmentValue{}
	for _, e := range c.Env {
		if e.ValueFrom != nil {
			seen[e.Name] = backupEnvironmentValue{}
			continue
		}
		seen[e.Name] = expandBackupEnvironment(e.Value, seen)
	}
	return seen
}

// TestRecordedS3StorageReachesReadersLiterally pins K065: the producer
// records an already-expanded endpoint and region (a raw
// "$$(BLOODRAVEN_STORAGE_TYPE)" becomes the literal
// "$(BLOODRAVEN_STORAGE_TYPE)"), and every reader Job (verification,
// backupRef restore and its dump-meta) must resolve exactly that literal,
// not expand it a second time into "S3". A direct restore source keeps
// its one expansion.
func TestRecordedS3StorageReachesReadersLiterally(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	const (
		rawEndpoint = "http://minio.example/$$(BLOODRAVEN_STORAGE_TYPE)"
		rawRegion   = "r-$$(BLOODRAVEN_STORAGE_TYPE)"
		endpoint    = "http://minio.example/$(BLOODRAVEN_STORAGE_TYPE)"
		region      = "r-$(BLOODRAVEN_STORAGE_TYPE)"
	)
	fixture := testArtifactStorage(v1alpha1.BackupStorageS3, "lion/seed/")
	producer := &batchv1.Job{}
	producer.Spec.Template.Spec.Containers = []corev1.Container{{Name: backupJobContainerName, Env: []corev1.EnvVar{
		{Name: "BLOODRAVEN_STORAGE_TYPE", Value: "S3"},
		{Name: "BLOODRAVEN_S3_BUCKET", Value: fixture.Bucket},
		{Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: rawEndpoint},
		{Name: "AWS_REGION", Value: rawRegion},
		{Name: "BLOODRAVEN_OUTPUT_URL", Value: fixture.Location},
	}}}
	recorded, err := artifactStorageFromJob(producer)
	if err != nil {
		t.Fatalf("artifactStorageFromJob: %v", err)
	}
	if recorded.EndpointURL != endpoint || recorded.Region != region {
		t.Fatalf("producer recorded endpoint %q region %q, want %q %q", recorded.EndpointURL, recorded.Region, endpoint, region)
	}

	wantLiteral := func(t *testing.T, where string, c corev1.Container) bool {
		t.Helper()
		env := expandContainerEnv(t, c)
		for name, want := range map[string]string{
			"BLOODRAVEN_S3_BUCKET":            recorded.Bucket,
			"BLOODRAVEN_S3_ENDPOINT_OVERRIDE": endpoint,
			"AWS_REGION":                      region,
		} {
			got, ok := env[name]
			if !ok {
				continue
			}
			if !got.known || got.value != want {
				t.Errorf("%s: %s resolves to %q (known %v), want the recorded %q", where, name, got.value, got.known, want)
			}
		}
		_, rendered := env["BLOODRAVEN_S3_ENDPOINT_OVERRIDE"]
		return rendered
	}

	t.Run("verification", func(t *testing.T) {
		fg := verifyFG()
		backup := successfulBackup("happy", "lion", "nightly-s3")
		backup.Status.ArtifactStorage = recorded.DeepCopy()
		v := verifyCR("verify-literal", "lion", "nightly-s3")
		job, err := buildVerificationJob(verificationJobInputs{
			FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup,
			CredsSecretName: "c", ScriptsConfigMapName: "s",
		})
		if err != nil {
			t.Fatalf("buildVerificationJob: %v", err)
		}
		readers := 0
		for _, c := range append(job.Spec.Template.Spec.InitContainers, job.Spec.Template.Spec.Containers...) {
			if wantLiteral(t, "verification "+c.Name, c) {
				readers++
			}
		}
		if readers == 0 {
			t.Fatal("no verification container renders the S3 endpoint")
		}
	})

	t.Run("backupRef restore with dump-meta", func(t *testing.T) {
		fg := fgWithBackup()
		fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
		archive := addSourceArchiveProfile(fg)
		fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
			Source:      v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "seed"}},
			PointInTime: &v1alpha1.PointInTimeSpec{StopTime: stopAt("2026-04-15T09:30:00Z"), ArchiveProfileName: archive},
		}
		seed := succeededSeedBackup()
		seed.UID = "uid-seed"
		seed.Status.ArtifactStorage = recorded.DeepCopy()
		r, _ := newReconciler(fg, seed)
		job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
		if err != nil {
			t.Fatalf("buildRestoreJob: %v", err)
		}
		dumpMeta, readers := false, 0
		for _, c := range append(job.Spec.Template.Spec.InitContainers, job.Spec.Template.Spec.Containers...) {
			rendered := wantLiteral(t, "restore "+c.Name, c)
			if rendered {
				readers++
			}
			if _, ok := expandContainerEnv(t, c)["BLOODRAVEN_DUMP_META_OUTPUT_DIR"]; ok && rendered {
				dumpMeta = true
			}
		}
		if !dumpMeta || readers < 2 {
			t.Fatalf("want the dump-meta and the loading container to render the S3 endpoint (dump-meta %v, readers %d)", dumpMeta, readers)
		}
	})

	t.Run("direct source keeps its one expansion", func(t *testing.T) {
		fg := fgWithBackup()
		fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{S3: &v1alpha1.S3Storage{
			Bucket: "direct", Prefix: "lion/seed", EndpointURL: rawEndpoint, Region: rawRegion, CredentialsSecretName: "aws",
		}}}
		r, _ := newReconciler(fg)
		job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
		if err != nil {
			t.Fatalf("buildRestoreJob: %v", err)
		}
		env := expandContainerEnv(t, job.Spec.Template.Spec.Containers[0])
		if got := env["BLOODRAVEN_S3_ENDPOINT_OVERRIDE"]; !got.known || got.value != endpoint {
			t.Fatalf("direct endpoint resolves to %q (known %v), want one expansion %q", got.value, got.known, endpoint)
		}
	})
}
