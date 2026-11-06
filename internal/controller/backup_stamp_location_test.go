package controller

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
)

func stampTestEnv(c *corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func jobContainer(job *batchv1.Job, name string) *corev1.Container {
	spec := &job.Spec.Template.Spec
	for i := range spec.InitContainers {
		if spec.InitContainers[i].Name == name {
			return &spec.InitContainers[i]
		}
	}
	for i := range spec.Containers {
		if spec.Containers[i].Name == name {
			return &spec.Containers[i]
		}
	}
	return nil
}

// B2: an S3 profile prefix with a leading (or trailing) "/" is normalized
// once, so the backup-stamp writer, the dump, status.location (through
// artifactStorageFromJob), dump-meta and artifact cleanup all derive the
// same stamp key. Before, the writer kept the leading "/" and dump-meta
// trimmed it, so every PITR of such a profile refused
// PITRBackupIdentityUnknown.
func TestBackupStampKeyAgreesForSlashedS3Prefix(t *testing.T) {
	SetOperatorImageDefaults(cleanupTestImage, "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })
	for _, encrypted := range []bool{false, true} {
		for _, prefix := range []string{"prod", "/prod", "prod/", "/prod/", "//prod//"} {
			name := prefix + map[bool]string{false: " plain", true: " encrypted"}[encrypted]
			t.Run(name, func(t *testing.T) {
				fg := fgWithBackup()
				if encrypted {
					fg = fgWithEncryptedBackup()
				}
				fg.UID = "fg-uid"
				profile := fg.Spec.Backup.Profiles[0]
				profile.Storage.S3.Prefix = prefix
				mb := backupCR("snap", fg.Name, profile.Name)
				mb.UID = "snap-uid"
				job, err := BuildBackupJob(BackupJobInputs{
					DumpKeySecretName: testDumpKeySecret,
					FailoverGroup:     fg, Profile: profile, Backup: mb,
					SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
				})
				if err != nil {
					t.Fatal(err)
				}
				stamp := jobContainer(job, backupStampInitContainerName)
				if stamp == nil {
					t.Fatal("no backup-stamp init container")
				}
				writerURL, _ := stampTestEnv(stamp, "BLOODRAVEN_OUTPUT_URL")
				if writerURL != "prod/snap" {
					t.Fatalf("stamp writer BLOODRAVEN_OUTPUT_URL = %q, want prod/snap", writerURL)
				}
				main := backupJobMainContainer(job)
				if got, _ := stampTestEnv(main, "BLOODRAVEN_OUTPUT_URL"); got != writerURL {
					t.Fatalf("dump destination %q differs from the stamp writer's %q", got, writerURL)
				}

				storage, err := artifactStorageFromJob(job)
				if err != nil {
					t.Fatal(err)
				}
				mb.Status.Location = storage.Location
				mb.Status.StorageType = v1alpha1.BackupStorageS3
				mb.Status.FailoverGroupUID = fg.UID
				src, err := pitrDumpSourceForBackup(fg, mb, &profile, nil)
				if err != nil {
					t.Fatal(err)
				}
				// The writer's key is StampKey(TrimSuffix(outputURL, "/"))
				// (backupStampStoreConfig); dump-meta reads StampKey(prefix).
				writerKey := backupmeta.StampKey(strings.TrimSuffix(writerURL, "/"))
				if readerKey := backupmeta.StampKey(src.Prefix); readerKey != writerKey {
					t.Fatalf("dump-meta reads %q, the backup Job writes %q", readerKey, writerKey)
				}
				if _, key, _ := backupmeta.SplitS3Location(storage.Location); strings.HasPrefix(key, "/") {
					t.Fatalf("status.location %q has an absolute key, which artifact cleanup refuses", storage.Location)
				}
			})
		}
	}
}
