package controller

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// retainFixture is a cleanup fixture whose backup is encrypted (its
// recorded key is the dump-key snapshot, which exists) and has
// spec.deletionPolicy: Retain.
func retainFixture(t *testing.T) (*cleanupFixture, *corev1.Secret) {
	t.Helper()
	f := newCleanupFixture(t)
	ctx := context.Background()
	snap := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: dumpKeySnapshotName(f.mb), Namespace: f.mb.Namespace,
		Labels: map[string]string{labelAppName: appNameDumpKeySnapshot, v1alpha1.MysqlBackupUIDLabel: string(f.mb.UID)}},
		Data: map[string][]byte{pitrKeySnapshotDataKey: []byte("passphrase")}}
	if err := f.c.Create(ctx, snap); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := f.c.Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Status.ArtifactStorage.PassphraseSecretKeyRef = &v1alpha1.SecretKeyRef{Name: snap.Name, Key: pitrKeySnapshotDataKey}
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	return f, snap
}

// TestFinalize_RetainKeepsArtifactAndDumpKey pins A-98: deleting a Retain
// backup runs no cleanup Job, keeps the dump-key snapshot that decrypts
// the retained artifact, releases the finalizer at once and says so in one
// Normal Event naming both.
func TestFinalize_RetainKeepsArtifactAndDumpKey(t *testing.T) {
	f, snap := retainFixture(t)
	drainEvents(f.rec)
	if done, _ := f.finalize(t); !done {
		t.Fatal("a Retain backup was not released")
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("a cleanup Job ran for a Retain backup")
	}
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(snap), &corev1.Secret{}); err != nil {
		t.Fatalf("the dump-key snapshot of a retained artifact was not kept: %v", err)
	}
	events := drainEvents(f.rec)
	if len(events) != 1 || !strings.HasPrefix(events[0], "Normal ArtifactRetained") ||
		!strings.Contains(events[0], `"lion/art"`) || !strings.Contains(events[0], snap.Name) {
		t.Fatalf("events = %v, want one Normal ArtifactRetained naming the location and %s", events, snap.Name)
	}
}

// TestFinalize_DeleteIsTheDefault: a backup with no deletionPolicy (the
// CRD defaults it to Delete) still cleans up its artifact.
func TestFinalize_DeleteIsTheDefault(t *testing.T) {
	for _, policy := range []v1alpha1.DeletionPolicy{"", v1alpha1.DeletionPolicyDelete} {
		t.Run(string(policy)+"/", func(t *testing.T) {
			f := newCleanupFixture(t)
			ctx := context.Background()
			if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
				t.Fatal(err)
			}
			f.mb.Spec.DeletionPolicy = policy
			if err := f.c.Update(ctx, f.mb); err != nil {
				t.Fatal(err)
			}
			if done, _ := f.finalize(t); done {
				t.Fatal("released before the artifact was cleaned up")
			}
			if _, ok := f.cleanupJob(t); !ok {
				t.Fatal("no cleanup Job")
			}
			if n := countEvents(f.rec, "ArtifactRetained"); n != 0 {
				t.Fatal("ArtifactRetained emitted for a Delete backup")
			}
		})
	}
}

// TestFinalize_RetainUnfinishedWaitsForProducer: an unfinished Retain
// backup is released only once its producing Job is stopped and its pods
// are gone, so a same-name successor cannot start while the old uploader
// may still write to the destination. Nothing is cleaned up.
func TestFinalize_RetainUnfinishedWaitsForProducer(t *testing.T) {
	f, snap := retainFixture(t)
	ctx := context.Background()
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Status.Phase, f.mb.Status.CompletionTime = v1alpha1.BackupPhaseRunning, nil
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	producer := f.producerJob(t, "")
	pod := f.runningProducerPod(t, nil)

	if done, _ := f.finalize(t); done {
		t.Fatal("released before the producing Job was stopped")
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(producer), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("producing Job not deleted: %v", err)
	}
	if done, _ := f.finalize(t); done {
		t.Fatal("released while the dump pod still runs")
	}
	if err := f.c.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	drainEvents(f.rec)
	if done, _ := f.finalize(t); !done {
		t.Fatal("not released once the producer stopped")
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("a cleanup Job ran for a Retain backup")
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(snap), &corev1.Secret{}); err != nil {
		t.Fatalf("dump-key snapshot not kept: %v", err)
	}
	if n := countEvents(f.rec, "Normal ArtifactRetained"); n != 1 {
		t.Fatal("no ArtifactRetained Event")
	}
}
