package controller

import (
	"fmt"
	"os"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
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
	// §3.7: the archive identity the sidecar writes and verifies.
	dest, err := pitrarchive.NewS3Destination("lion", "dumps", "https://s3.example.com")
	if err != nil {
		t.Fatal(err)
	}
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_FAILOVER_GROUP_UID", "uid-lion")
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_DESTINATION", dest.String())
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_ARCHIVE_ID", pitrarchive.CreateArchiveID("uid-lion", dest))
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_ENCRYPTION_MODE", "None")

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

	// Pod volumes: the sidecar's state emptyDir (A3) and the AWS creds,
	// sourced from a secret matching the profile's CredentialsSecret.
	assertPITRStateVolume(t, frags)
	if len(frags.PodVolumes) != 2 {
		t.Fatalf("want 2 pod volumes, got %d", len(frags.PodVolumes))
	}
	v := frags.PodVolumes[1]
	if v.Name != "pitr-aws-creds" || v.Secret == nil || v.Secret.SecretName != "aws-creds" {
		t.Errorf("unexpected pod volume: %+v", v)
	}
	if v.Secret.DefaultMode == nil || *v.Secret.DefaultMode != 0o444 {
		t.Errorf("pitr aws creds mode: want 0444 for non-root sidecar readability, got %v", v.Secret.DefaultMode)
	}
}

// assertPITRStateVolume checks A3's pod-lifetime state dir: an emptyDir
// mounted writable into the sidecar at BLOODRAVEN_PITR_STATE_DIR, so a
// pending history-integrity marker survives a sidecar container restart.
func assertPITRStateVolume(t *testing.T, frags pitrSidecarFragments) {
	t.Helper()
	assertEnvEquals(t, frags.SidecarEnv, "BLOODRAVEN_PITR_STATE_DIR", pitrStateMountPath)
	var vol, mount bool
	for _, v := range frags.PodVolumes {
		if v.Name == pitrStateVolumeName && v.EmptyDir != nil {
			vol = true
		}
	}
	for _, m := range frags.SidecarVolumeMounts {
		if m.Name == pitrStateVolumeName && m.MountPath == pitrStateMountPath && !m.ReadOnly {
			mount = true
		}
	}
	if !vol || !mount {
		t.Fatalf("sidecar state emptyDir missing: volumes %+v mounts %+v", frags.PodVolumes, frags.SidecarVolumeMounts)
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

	// The state emptyDir (A3), then the pitr-archive volume with the
	// user-supplied claim.
	assertPITRStateVolume(t, frags)
	if len(frags.PodVolumes) != 2 {
		t.Fatalf("want 2 pod volumes, got %d", len(frags.PodVolumes))
	}
	v := frags.PodVolumes[1]
	if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "backup-pvc" {
		t.Errorf("unexpected pvc source: %+v", v)
	}
}

// TestBuildRestorePITRFragments_RequiresSourceArchive fails cleanly
// when a PITR restore names no source archive, and when a replay with no
// archiveProfileName (a verification) runs on a group that does not
// archive binlogs: nothing to replay (F152, F032).
func TestBuildRestorePITRFragments_RequiresSourceArchive(t *testing.T) {
	fg := withBackup(nil)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		PointInTime: &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z"},
	}
	_, err := buildRestorePITRFragments(fg)
	if err == nil || !strings.HasPrefix(err.Error(), string(pitrarchive.TokenTargetInvalid)+": pointInTime.archiveProfileName is required") {
		t.Fatalf("err = %v, want the archiveProfileName refusal", err)
	}

	for _, tc := range []struct {
		name         string
		verification bool
		want         string
		wantNot      string
	}{
		{name: "restore", want: "pointInTime.archiveProfileName must name the archive's profile"},
		{name: "verification", verification: true, want: "a verification replays the group's own PITR archive", wantNot: "archiveProfileName"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildRestorePITRFragmentsFor(withBackup(nil), &pitrReplayRequest{Mode: "latest", Verification: tc.verification})
			if err == nil || !strings.HasPrefix(err.Error(), string(pitrarchive.TokenTargetInvalid)+": ") ||
				!strings.Contains(err.Error(), "spec.backup.pitr.enabled=false") || !strings.Contains(err.Error(), tc.want) ||
				(tc.wantNot != "" && strings.Contains(err.Error(), tc.wantNot)) {
				t.Fatalf("err = %v, want the PITR-disabled refusal containing %q", err, tc.want)
			}
		})
	}
}

// F030, F031: excludeGtids sources must be UUIDs before tags are expanded,
// and the canonical set must fit one container env string.
func TestCanonicalExcludeGtidsBounds(t *testing.T) {
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	manyTags := func(n int) string {
		var b strings.Builder
		b.WriteString(u)
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, ":t%d:1", i)
		}
		return b.String()
	}
	tests := []struct {
		name string
		raw  string
		want string // "" = accepted
	}{
		{name: "plain", raw: u + ":1-5"},
		{name: "uppercase and multi-line", raw: strings.ToUpper(u) + ":1-5,\n" + u + ":7"},
		{name: "tags under one UUID", raw: manyTags(10)},
		{name: "non-UUID source", raw: strings.Repeat("a", 60000) + ":t0:1:t1:1", want: "is not a server UUID"},
		{name: "short source", raw: "aaaa:1-5", want: "is not a server UUID"},
		{name: "canonical expansion too large", raw: manyTags(4000), want: "over the 65536-byte limit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := canonicalExcludeGtids("pointInTime.excludeGtids", tt.raw)
			if tt.want == "" {
				if err != nil || got == "" || len(got) > maxExcludeGtidsEnvBytes {
					t.Fatalf("got %q, %v", got, err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), string(pitrarchive.TokenTargetInvalid)+": ") || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			if len(err.Error()) > 1024 {
				t.Fatalf("refusal message is %d bytes", len(err.Error()))
			}
		})
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
	// The restore reads the source group's archive through its own
	// profile; the restored group archives into "primary" (D-10).
	source := *fg.Spec.Backup.Profiles[0].DeepCopy()
	source.Name = "source"
	source.Storage.S3.Prefix = "source-dumps"
	fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, source)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		PointInTime: &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z", ArchiveProfileName: "source"},
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
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_STOP_DATETIME", "2026-04-15T09:30:00.000000Z")
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_MODE", "timestamp")
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_STORAGE_TYPE", "S3")
	dest, err := pitrarchive.NewS3Destination("lion", "source-dumps", "")
	if err != nil {
		t.Fatal(err)
	}
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_DESTINATION", dest.String())
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_MANIFEST_PREFIX", "source-dumps/binlogs")
	assertEnvEquals(t, ic.Env, "BLOODRAVEN_PITR_ENCRYPTION_MODE", "None")
	if _, ok := envValue(ic.Env, "BLOODRAVEN_PITR_SITES"); ok {
		t.Error("BLOODRAVEN_PITR_SITES is rendered; replay considers every manifest (D-9)")
	}
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
	assertEnvEquals(t, frags.MainEnv, "BLOODRAVEN_PITR_STOP_DATETIME", "2026-04-15T09:30:00.000000Z")
	assertEnvEquals(t, frags.MainEnv, "TZ", "UTC")
	if len(frags.MainMounts) != 1 || !frags.MainMounts[0].ReadOnly {
		t.Errorf("want 1 RO main mount, got %+v", frags.MainMounts)
	}
}

// D-10: initFromBackup names its archive profile, and that profile may not
// resolve to the group's own active PITR destination, however it is
// spelled.
func TestValidateRestoreArchiveProfile(t *testing.T) {
	s3 := func(name, bucket, prefix, endpoint string) v1alpha1.BackupProfile {
		return v1alpha1.BackupProfile{Name: name, Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3,
			S3: &v1alpha1.S3Storage{Bucket: bucket, Prefix: prefix, EndpointURL: endpoint}}}
	}
	tests := []struct {
		name     string
		pitr     *v1alpha1.PITRSpec
		profiles []v1alpha1.BackupProfile
		archive  string
		want     string
	}{
		{name: "missing archiveProfileName", pitr: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "own"}, profiles: []v1alpha1.BackupProfile{s3("own", "b", "own", "")}, want: "archiveProfileName is required"},
		{name: "unknown profile", profiles: []v1alpha1.BackupProfile{s3("own", "b", "own", "")}, archive: "ghost", want: "not found"},
		{name: "same profile as the active archive", pitr: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "own"}, profiles: []v1alpha1.BackupProfile{s3("own", "b", "own", "")}, archive: "own", want: "PITRArchiveDestinationShared"},
		{name: "alias of the active destination", pitr: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "own"},
			profiles: []v1alpha1.BackupProfile{s3("own", "b", "own", "https://S3.example.com:443/"), s3("alias", "b", "/own/", "https://s3.example.com")}, archive: "alias", want: "PITRArchiveDestinationShared"},
		{name: "a different prefix", pitr: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "own"}, profiles: []v1alpha1.BackupProfile{s3("own", "b", "own", ""), s3("source", "b", "source", "")}, archive: "source"},
		{name: "own profile while PITR is disabled", pitr: &v1alpha1.PITRSpec{Enabled: false, ProfileName: "own"}, profiles: []v1alpha1.BackupProfile{s3("own", "b", "own", "")}, archive: "own"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fg := withBackup(tt.pitr)
			fg.Spec.Backup.Profiles = tt.profiles
			err := validateRestoreArchiveProfile(fg, &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z", ArchiveProfileName: tt.archive})
			if tt.want == "" {
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

// ---- helpers ----

func withBackup(p *v1alpha1.PITRSpec) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: "shared-lion", UID: "uid-lion"},
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

// excludeGtids reaches pitr-download and restore.py in the planner's
// canonical form, so restore.py's comparison with coverage.json "excluded"
// holds for any spelling MySQL accepts; a set that does not parse is
// refused at reconcile.
func TestPITRRequestCanonicalizesExcludeGtids(t *testing.T) {
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const v = "4f22ab58-82db-22f2-af44-d91bba530673"
	for _, tc := range []struct {
		name, in, want string
		wantErr        bool
	}{
		{name: "empty", in: "", want: ""},
		{name: "trimmed", in: " " + u + ":1 ", want: u + ":1"},
		{name: "MySQL multi-line, upper case, unsorted", in: v + ":2-3,\n3E11FA47-71CA-11E1-9E33-C80AA9429562:5:1-4", want: u + ":1-5," + v + ":2-3"},
		{name: "tags", in: u + ":1:Blue:7", want: u + ":1," + u + ":blue:7"},
		{name: "unparsable", in: u + ":9-2", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restore, rerr := pitrRequestFromSpec(&v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z", ExcludeGtids: tc.in})
			verify, verr := verificationPITRRequest(&v1alpha1.PointInTimeVerificationSpec{Mode: "latest", ExcludeGtids: tc.in})
			for name, err := range map[string]error{"restore": rerr, "verification": verr} {
				if tc.wantErr {
					if err == nil || pitrFailureReason(err.Error()) != string(pitrarchive.TokenTargetInvalid) {
						t.Fatalf("%s: err = %v, want %s", name, err, pitrarchive.TokenTargetInvalid)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			}
			if tc.wantErr {
				return
			}
			if restore.ExcludeGtids != tc.want || verify.ExcludeGtids != tc.want {
				t.Fatalf("excludeGtids = %q (restore), %q (verification); want %q", restore.ExcludeGtids, verify.ExcludeGtids, tc.want)
			}
		})
	}
	if req, err := verificationPITRRequest(&v1alpha1.PointInTimeVerificationSpec{Mode: "none", ExcludeGtids: "not a set:x-y"}); err != nil || req != nil {
		t.Fatalf("mode none = %+v, %v; want no replay and no error", req, err)
	}
}

// F029: kubelet expands $(VAR) and $$ in EnvVar.Value, so the archive
// identity env is rendered literal-escaped: what the sidecar and the
// pitr-download init container see after expansion is the destination the
// archive ID was derived from, and the manifest prefix is its key prefix.
func TestPITRIdentityEnvSurvivesKubeletExpansion(t *testing.T) {
	for _, prefix := range []string{"cash$$", "team/$(POD_NAMESPACE)/x", "plain"} {
		t.Run(prefix, func(t *testing.T) {
			fg := withBackup(&v1alpha1.PITRSpec{Enabled: true, ProfileName: "primary"})
			fg.UID = "uid-lion"
			fg.Spec.Backup.Profiles = []v1alpha1.BackupProfile{
				{Name: "primary", Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3,
					S3: &v1alpha1.S3Storage{Bucket: "lion", Prefix: prefix, EndpointURL: "https://s3.example.com"}}},
				{Name: "other", Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3,
					S3: &v1alpha1.S3Storage{Bucket: "lion", Prefix: "elsewhere", EndpointURL: "https://s3.example.com"}}},
			}
			ident, ok := activePITRArchiveIdentity(fg)
			if !ok {
				t.Fatal("no active archive")
			}
			check := func(t *testing.T, env []corev1.EnvVar) {
				t.Helper()
				seen := map[string]backupEnvironmentValue{"POD_NAMESPACE": {value: "ns", known: true}}
				got := map[string]string{}
				for _, e := range env {
					v := expandBackupEnvironment(e.Value, seen)
					seen[e.Name] = v
					got[e.Name] = v.value
				}
				dest, err := pitrarchive.ParseDestination(got["BLOODRAVEN_PITR_DESTINATION"])
				if err != nil {
					t.Fatalf("expanded destination %q does not parse: %v", got["BLOODRAVEN_PITR_DESTINATION"], err)
				}
				if dest != ident.Destination || got["BLOODRAVEN_PITR_MANIFEST_PREFIX"] != dest.KeyPrefix {
					t.Fatalf("expanded destination %q prefix %q, want %q prefix %q", dest.String(),
						got["BLOODRAVEN_PITR_MANIFEST_PREFIX"], ident.Destination.String(), ident.Destination.KeyPrefix)
				}
				if id, ok := got["BLOODRAVEN_PITR_ARCHIVE_ID"]; ok && !pitrarchive.IsDerivedArchiveID(id, fg.UID, dest) {
					t.Fatalf("archive ID %s is not derived from the expanded destination %s", id, dest.String())
				}
			}
			frags, err := buildPITRSidecarFragments(fg)
			if err != nil {
				t.Fatal(err)
			}
			check(t, frags.SidecarEnv)

			// A restore of another group reading this archive.
			reader := fg.DeepCopy()
			reader.UID = "uid-reader"
			reader.Spec.Backup.PITR.ProfileName = "other"
			restore, err := buildRestorePITRFragmentsFor(reader, &pitrReplayRequest{Mode: "latest", ArchiveProfileName: "primary"})
			if err != nil {
				t.Fatal(err)
			}
			check(t, restore.InitContainer.Env)
			job := &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{*restore.InitContainer}}}}}
			if got := renderedPITRRequest(job).Destination; got != ident.Destination.String() {
				t.Fatalf("rendered request destination %q, want %q", got, ident.Destination.String())
			}
		})
	}
}
