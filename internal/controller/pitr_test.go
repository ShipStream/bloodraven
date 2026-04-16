package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestBuildPITRSidecarFragments_Disabled asserts that the whole helper
// is a no-op when PITR isn't enabled — every field returns the zero
// value, so the reconciler can splice unconditionally without caring
// whether PITR is on.
func TestBuildPITRSidecarFragments_Disabled(t *testing.T) {
	cases := []struct {
		name string
		fg   *v1alpha1.MysqlFailoverGroup
	}{
		{"nil backup", &v1alpha1.MysqlFailoverGroup{}},
		{"nil pitr", withBackup(nil)},
		{"disabled pitr", withBackup(&v1alpha1.PITRSpec{Enabled: false, ProfileName: "p"})},
		{"enabled without profile name", withBackup(&v1alpha1.PITRSpec{Enabled: true})},
		{"enabled, unknown profile", withBackup(&v1alpha1.PITRSpec{Enabled: true, ProfileName: "ghost"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frags, err := buildPITRSidecarFragments(tc.fg)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if len(frags.SidecarEnv) != 0 || len(frags.SidecarVolumeMounts) != 0 || len(frags.PodVolumes) != 0 {
				t.Errorf("want empty fragments, got %+v", frags)
			}
		})
	}
}

// TestBuildPITRSidecarFragments_S3 checks the env and mount additions
// for an S3-backed profile with a credentials secret.
func TestBuildPITRSidecarFragments_S3(t *testing.T) {
	fg := withBackup(&v1alpha1.PITRSpec{Enabled: true, ProfileName: "primary"})
	fg.Spec.Backup.Profiles = []v1alpha1.BackupProfile{
		{
			Name: "primary",
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3,
				S3: &v1alpha1.S3Storage{
					Bucket:            "lion",
					Prefix:            "dumps",
					Region:            "us-east-1",
					EndpointURL:       "https://s3.example.com",
					CredentialsSecret: "aws-creds",
				},
			},
		},
	}

	frags, err := buildPITRSidecarFragments(fg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_ENABLED", "1")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_STORAGE_TYPE", "S3")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_MANIFEST_PREFIX", "dumps/binlogs")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_S3_BUCKET", "lion")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_S3_REGION", "us-east-1")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_S3_ENDPOINT_URL", "https://s3.example.com")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_PROFILE_NAME", "primary")

	// Data PVC + AWS creds mounted into sidecar.
	var sawData, sawAWS bool
	for _, m := range frags.SidecarVolumeMounts {
		if m.Name == "data" && m.ReadOnly {
			sawData = true
		}
		if m.Name == "pitr-aws-creds" {
			sawAWS = true
		}
	}
	if !sawData {
		t.Errorf("want read-only data mount in sidecar, got %+v", frags.SidecarVolumeMounts)
	}
	if !sawAWS {
		t.Errorf("want pitr-aws-creds mount in sidecar, got %+v", frags.SidecarVolumeMounts)
	}

	// Pod volume for AWS creds should appear — one entry, sourced from
	// a secret matching the profile's CredentialsSecret.
	if len(frags.PodVolumes) != 1 {
		t.Fatalf("want 1 pod volume, got %d", len(frags.PodVolumes))
	}
	v := frags.PodVolumes[0]
	if v.Name != "pitr-aws-creds" || v.Secret == nil || v.Secret.SecretName != "aws-creds" {
		t.Errorf("unexpected pod volume: %+v", v)
	}
}

// TestBuildPITRSidecarFragments_PVC covers the alternate backend.
func TestBuildPITRSidecarFragments_PVC(t *testing.T) {
	fg := withBackup(&v1alpha1.PITRSpec{Enabled: true, ProfileName: "local"})
	fg.Spec.Backup.Profiles = []v1alpha1.BackupProfile{
		{
			Name: "local",
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStoragePVC,
				PVC: &v1alpha1.PVCStorage{
					ClaimName: "backup-pvc",
					SubPath:   "shard1",
				},
			},
		},
	}

	frags, err := buildPITRSidecarFragments(fg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_MANIFEST_PREFIX", "shard1/binlogs")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_PVC_MOUNT_PATH", pitrPVCMountPath)

	// pitr-archive volume with the user-supplied claim.
	if len(frags.PodVolumes) != 1 {
		t.Fatalf("want 1 pod volume, got %d", len(frags.PodVolumes))
	}
	v := frags.PodVolumes[0]
	if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "backup-pvc" {
		t.Errorf("unexpected pvc source: %+v", v)
	}
}

// TestBuildRestorePITRExtras_RequiresSourceArchive fails cleanly when a
// PITR restore is requested but the failover group doesn't archive
// binlogs — nothing to replay.
func TestBuildRestorePITRExtras_RequiresSourceArchive(t *testing.T) {
	fg := withBackup(nil)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		PointInTime: &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z"},
	}
	_, _, _, err := buildRestorePITRExtras(fg)
	if err == nil {
		t.Fatalf("want error when pitr archive is not configured on source")
	}
}

// ---- helpers ----

func withBackup(p *v1alpha1.PITRSpec) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Backup: &v1alpha1.BackupSpec{PITR: p},
		},
	}
}

func assertEnvEquals(t *testing.T, env []corev1.EnvVar, name, want string) {
	t.Helper()
	for _, e := range env {
		if e.Name == name {
			if e.Value != want {
				t.Errorf("env %s: want %q, got %q", name, want, e.Value)
			}
			return
		}
	}
	t.Errorf("env %s not found", name)
}
