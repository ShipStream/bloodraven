package controller

// Regression tests for the PR #259 review findings (U##) on backup
// finalization and verification dump keys.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// encryptedAbandonedFixture is an abandoned (never succeeded) encrypted
// backup whose producer is gone and whose dump-key snapshot is recorded;
// withKey creates the snapshot.
func encryptedAbandonedFixture(t *testing.T, withKey bool) *cleanupFixture {
	t.Helper()
	f := abandonedFixture(t, v1alpha1.BackupPhaseFailed)
	ctx := context.Background()
	f.mb.Status.Encrypted = true
	f.mb.Status.ArtifactStorage.PassphraseSecret = &v1alpha1.BackupArtifactPassphrase{Name: dumpKeySnapshotName(f.mb), Key: pitrKeySnapshotDataKey}
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if withKey {
		snap := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: f.mb.Namespace, Name: dumpKeySnapshotName(f.mb),
			Labels: map[string]string{labelAppName: appNameDumpKeySnapshot, labelMysqlBackupUID: string(f.mb.UID)}},
			Data: map[string][]byte{pitrKeySnapshotDataKey: []byte("k")}}
		if err := f.c.Create(ctx, snap); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func dumpKeyExists(t *testing.T, f *cleanupFixture) bool {
	t.Helper()
	var s corev1.Secret
	return f.c.Get(context.Background(), types.NamespacedName{Namespace: f.mb.Namespace, Name: dumpKeySnapshotName(f.mb)}, &s) == nil
}

// U05: an abandoned cleanup that could not prove the location's objects
// are the backup's deletes nothing and says so (skipped verdict): the
// dump-key snapshot that may decrypt them is kept and named. A complete
// verdict (nothing of the backup is there) still releases the key.
func TestFinalize_AbandonedSkippedKeepsDumpKey(t *testing.T) {
	t.Run("skipped", func(t *testing.T) {
		f := encryptedAbandonedFixture(t, true)
		if done, _ := f.finalize(t); done {
			t.Fatal("released before the cleanup ran")
		}
		f.finishCleanupJob(t, batchv1.JobComplete, 0, backupmeta.FormatCleanupSkipped("no backup stamp at lion/art.stamp names this backup, but 3 objects remain under lion/art/; nothing was deleted"))
		drainEvents(f.rec)
		if done, _ := f.finalize(t); !done {
			t.Fatal("a skipped cleanup held the deletion")
		}
		if !dumpKeyExists(t, f) {
			t.Fatal("the dump-key snapshot was deleted although nothing proved the artifacts gone")
		}
		events := strings.Join(drainEvents(f.rec), "\n")
		if !strings.Contains(events, "ArtifactCleanupSkipped") || !strings.Contains(events, dumpKeySnapshotName(f.mb)) || !strings.Contains(events, "3 objects remain") {
			t.Fatalf("events %s", events)
		}
	})
	t.Run("complete", func(t *testing.T) {
		f := encryptedAbandonedFixture(t, true)
		f.finalize(t)
		f.finishCleanupJob(t, batchv1.JobComplete, 0, backupmeta.FormatCleanupComplete(0))
		if done, _ := f.finalize(t); !done {
			t.Fatal("not released")
		}
		if dumpKeyExists(t, f) {
			t.Fatal("the dump-key snapshot of a backup with nothing left was kept")
		}
	})
}

// U11: the recorded dump key of an encrypted abandoned backup is gone: the
// cleanup blocks (finalizer kept, PassphraseSecretMissing) until it is
// restored, never skipping and orphaning the artifacts.
func TestFinalize_AbandonedRecordedKeyMissingBlocks(t *testing.T) {
	f := encryptedAbandonedFixture(t, false)
	if done, _ := f.finalize(t); done {
		t.Fatal("the deletion was released with the recorded key missing")
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("a cleanup Job was created without its key")
	}
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	c := apiMeta.FindStatusCondition(f.mb.Status.Conditions, ConditionArtifactCleanup)
	if c == nil || c.Reason != backupReasonPassphraseSecretMissing {
		t.Fatalf("condition %+v, want %s", c, backupReasonPassphraseSecretMissing)
	}
	// Restoring the Secret lets the cleanup run.
	if err := f.c.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: f.mb.Namespace, Name: dumpKeySnapshotName(f.mb)},
		Data: map[string][]byte{pitrKeySnapshotDataKey: []byte("k")}}); err != nil {
		t.Fatal(err)
	}
	f.finalize(t)
	if _, ok := f.cleanupJob(t); !ok {
		t.Fatal("no cleanup Job once the key is back")
	}
}

// U13: the dump-key Secret an encrypted backup recorded is gone: the
// verification fails at once with PITRBackupDumpKeyMissing instead of a Job
// whose pod waits on a FailedMount until its deadline.
func TestVerificationRecordedDumpKeyGone(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgWithEncryptedBackup()
	backup := successfulBackup("seed", "lion", "nightly-s3")
	backup.Status.Location = "s3://bloodraven-backups/lion/seed/"
	backup.Status.StorageType = v1alpha1.BackupStorageS3
	backup.Status.Encrypted = true
	backup.Status.ArtifactStorage = &v1alpha1.BackupArtifactStorage{PassphraseSecret: &v1alpha1.BackupArtifactPassphrase{Name: "mysqlbackup-seed-dump-key-gone", Key: pitrKeySnapshotDataKey}}
	v := verifyCR("verify-gone", "lion", "nightly-s3")
	r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	got := getVerification(t, c, v)
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed || verifiedReason(got) != string(pitrarchive.TokenBackupDumpKeyMissing) {
		t.Fatalf("phase %s reason %q message %q", got.Status.Phase, verifiedReason(got), got.Status.Message)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: verificationJobName(v.Name)}, &job); err == nil {
		t.Fatal("a Job was created without its dump key")
	}
}

// U50, U59: the CRD help no longer promises a live-profile fallback for
// restores, and qualifies where the profile Secret is still mounted.
func TestCRDDecryptionHelpIsCurrent(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "shipstream.io_mysqlfailovergroups.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	crd := string(raw)
	flat := strings.Join(strings.Fields(crd), " ")
	for _, stale := range []string{"the profile's own PassphraseSecret if Decryption is omitted", "passphrase from the source profile's own PassphraseSecret"} {
		if strings.Contains(flat, stale) {
			t.Errorf("CRD help still says %q", stale)
		}
	}
	if !strings.Contains(flat, "else the restore is refused (PITRBackupDumpKeyMissing)") {
		t.Error("CRD help lacks the dump-key resolution order")
	}
	// The PassphraseSecretRef type doc (Go API reference) names the one
	// Job that still mounts a profile's Secret.
	src, err := os.ReadFile(filepath.Join("..", "..", "api", "v1alpha1", "backup_types.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(strings.Fields(strings.ReplaceAll(string(src), "//", "")), " "), "runs its abandoned-artifact cleanup Job with this Secret") {
		t.Error("PassphraseSecretRef doc does not qualify the legacy abandoned cleanup")
	}
}
