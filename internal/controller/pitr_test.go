package controller

import (
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestReconcileDeployment_PITRPassphraseReadableBySidecar(t *testing.T) {
	for _, storageType := range []v1alpha1.BackupStorageType{v1alpha1.BackupStorageS3, v1alpha1.BackupStoragePVC} {
		for _, contextName := range []string{"default", "explicit non-root", "fsGroup"} {
			t.Run(string(storageType)+"/"+contextName, func(t *testing.T) {
				fg := newTestFG()
				storage := v1alpha1.BackupStorage{Type: storageType}
				if storageType == v1alpha1.BackupStorageS3 {
					storage.S3 = &v1alpha1.S3Storage{Bucket: "backups", CredentialsSecret: "aws-creds"}
				} else {
					storage.PVC = &v1alpha1.PVCStorage{ClaimName: "backups"}
				}
				fg.Spec.Backup = &v1alpha1.BackupSpec{
					PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "encrypted"},
					Profiles: []v1alpha1.BackupProfile{{
						Name: "encrypted", Storage: storage,
						Encryption: &v1alpha1.BackupEncryptionSpec{
							PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "archive-key", Key: "selected-key"},
						},
					}},
				}
				uid, gid := int64(1234), int64(2345)
				switch contextName {
				case "explicit non-root":
					fg.Spec.ContainerSecurityContext = &corev1.SecurityContext{RunAsUser: &uid, RunAsGroup: &gid}
				case "fsGroup":
					fg.Spec.PodSecurityContext = &corev1.PodSecurityContext{FSGroup: &gid}
				}
				r, _ := newReconciler(fg)
				reconcileOnce(t, r)
				pod := getDeployment(t, r, "dc1").Spec.Template.Spec
				sidecar := containerByName(pod.Containers, "sidecar")
				if sidecar == nil || sidecar.SecurityContext == nil || sidecar.SecurityContext.RunAsUser == nil {
					t.Fatal("expected a sidecar with an explicit non-root UID")
				}
				wantUID := mysqlDataUID
				if contextName == "explicit non-root" {
					wantUID = uid
				}
				if got := *sidecar.SecurityContext.RunAsUser; got != wantUID {
					t.Fatalf("sidecar UID: got %d, want %d", got, wantUID)
				}
				if contextName != "fsGroup" && pod.SecurityContext != nil && pod.SecurityContext.FSGroup != nil {
					t.Fatal("PITR must not inject an fsGroup onto the MySQL PVC")
				}
				vol := findVolume(pod.Volumes, "pitr-passphrase")
				if vol == nil || vol.Secret == nil || vol.Secret.DefaultMode == nil {
					t.Fatal("expected passphrase Secret with an explicit projection mode")
				}
				if vol.Secret.SecretName != "archive-key" || len(vol.Secret.Items) != 1 ||
					vol.Secret.Items[0].Key != "selected-key" || vol.Secret.Items[0].Path != backupPassphraseFileName {
					t.Fatalf("unexpected passphrase projection: %+v", vol.Secret)
				}
				mode := os.FileMode(*vol.Secret.DefaultMode)
				if (pod.SecurityContext == nil || pod.SecurityContext.FSGroup == nil) && mode&0o004 == 0 {
					t.Errorf("root-owned passphrase mode %#o is unreadable by sidecar UID %d without fsGroup", mode, wantUID)
				}
				if mode != 0o444 {
					t.Errorf("passphrase mode: got %#o, want 0444 to match PITR S3 credentials", mode)
				}
				var mounted bool
				for _, mount := range sidecar.VolumeMounts {
					if mount.Name == vol.Name && mount.ReadOnly && mount.MountPath == backupPassphraseMountPath {
						mounted = true
					}
				}
				if !mounted {
					t.Fatal("sidecar must mount the passphrase read-only at the configured path")
				}
				for _, container := range pod.Containers {
					if container.Name == "sidecar" {
						continue
					}
					for _, mount := range container.VolumeMounts {
						if mount.Name == vol.Name {
							t.Errorf("passphrase must only be mounted in the sidecar, found in %s", container.Name)
						}
					}
				}
				assertEnvEquals(t, sidecar.Env, "BLOODRAVEN_PITR_PASSPHRASE_FILE", backupPassphraseMountPath+"/"+backupPassphraseFileName)
			})
		}
	}
}

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
	if v.Secret.DefaultMode == nil || *v.Secret.DefaultMode != 0o444 {
		t.Errorf("pitr aws creds mode: want 0444 for non-root sidecar readability, got %v", v.Secret.DefaultMode)
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

// TestBuildRestorePITRFragments_RequiresSourceArchive fails cleanly
// when a PITR restore is requested but the failover group doesn't
// archive binlogs — nothing to replay.
func TestBuildRestorePITRFragments_RequiresSourceArchive(t *testing.T) {
	fg := withBackup(nil)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		PointInTime: &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z"},
	}
	_, err := buildRestorePITRFragments(fg)
	if err == nil {
		t.Fatalf("want error when pitr archive is not configured on source")
	}
}

// TestBuildRestorePITRFragments_S3BuildsInitContainer verifies the
// init container is emitted with the right image/command and that
// both the AWS creds secret and the shared emptyDir are mounted.
func TestBuildRestorePITRFragments_S3BuildsInitContainer(t *testing.T) {
	fg := withBackup(&v1alpha1.PITRSpec{Enabled: true, ProfileName: "primary"})
	fg.Spec.Sites = []v1alpha1.SiteSpec{{Name: "us-east-1a"}, {Name: "us-east-1b"}}
	fg.Spec.Backup.Profiles = []v1alpha1.BackupProfile{{
		Name: "primary",
		Storage: v1alpha1.BackupStorage{
			Type: v1alpha1.BackupStorageS3,
			S3: &v1alpha1.S3Storage{
				Bucket:            "lion",
				Prefix:            "dumps",
				Region:            "us-east-1",
				CredentialsSecret: "aws-creds",
			},
		},
	}}
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		PointInTime: &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z"},
	}

	frags, err := buildRestorePITRFragments(fg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if frags.InitContainer == nil {
		t.Fatal("want init container, got nil")
	}
	ic := frags.InitContainer
	if ic.Name != restorePITRInitContainerName {
		t.Errorf("init container name: want %q, got %q", restorePITRInitContainerName, ic.Name)
	}
	if len(ic.Command) < 2 || ic.Command[0] != "/bloodraven" || ic.Command[1] != "pitr-download" {
		t.Errorf("init container command: want [/bloodraven pitr-download ...], got %v", ic.Command)
	}
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_STOP_DATETIME", "2026-04-15T09:30:00Z")
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_STORAGE_TYPE", "S3")
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_SITES", "us-east-1a,us-east-1b")
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_OUTPUT_DIR", restorePITRLocalDir)
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_S3_BUCKET", "lion")
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_AWS_CREDS_DIR", pitrAWSCredsMountDir)

	// Must mount the emptyDir (RW) and the AWS creds secret.
	var sawShared, sawAWS bool
	for _, m := range ic.VolumeMounts {
		if m.Name == "pitr-binlogs" && !m.ReadOnly {
			sawShared = true
		}
		if m.Name == "pitr-aws-creds" {
			sawAWS = true
		}
	}
	if !sawShared || !sawAWS {
		t.Errorf("init container mounts: want RW pitr-binlogs + pitr-aws-creds, got %+v", ic.VolumeMounts)
	}

	// Main container gets RO shared volume + BLOODRAVEN_PITR_LOCAL_DIR.
	assertEnvEquals(t, frags.MainEnv, "BLOODRAVEN_PITR_LOCAL_DIR", restorePITRLocalDir)
	assertEnvEquals(t, frags.MainEnv, "BLOODRAVEN_PITR_STOP_DATETIME", "2026-04-15T09:30:00Z")
	assertEnvEquals(t, frags.MainEnv, "TZ", "UTC")
	if len(frags.MainMounts) != 1 || !frags.MainMounts[0].ReadOnly {
		t.Errorf("want 1 RO main mount, got %+v", frags.MainMounts)
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
