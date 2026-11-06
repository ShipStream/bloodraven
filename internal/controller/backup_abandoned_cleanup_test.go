package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
)

// abandonedFixture is a cleanup fixture whose backup never succeeded: no
// status.location, but status.artifactStorage records the planned
// destination its Job wrote the backup stamp to.
func abandonedFixture(t *testing.T, phase v1alpha1.BackupPhase) *cleanupFixture {
	t.Helper()
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Status.Phase, f.mb.Status.Location, f.mb.Status.CompletionTime = phase, "", nil
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	return f
}

// producerJob creates the backup's own Job (controlled by it) in the given
// terminal state ("" = still running).
func (f *cleanupFixture) producerJob(t *testing.T, terminal batchv1.JobConditionType, volumes ...corev1.Volume) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: backupJobName(f.mb.Name), Namespace: f.mb.Namespace, UID: "producer-uid",
			Labels: map[string]string{labelMysqlBackup: f.mb.Name, labelResourceKind: "backup"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "MysqlBackup",
				Name: f.mb.Name, UID: f.mb.UID, Controller: boolPtr(true)}},
		},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: volumes}}},
	}
	if err := f.c.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if terminal != "" {
		job.Status.Conditions = []batchv1.JobCondition{{Type: terminal, Status: corev1.ConditionTrue}}
		if err := f.c.Status().Update(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	return job
}

func (f *cleanupFixture) finishCleanupJob(t *testing.T, condition batchv1.JobConditionType, code int32, message string) {
	t.Helper()
	ctx := context.Background()
	job, ok := f.cleanupJob(t)
	if !ok {
		t.Fatal("no cleanup Job")
	}
	job.UID = "cleanup-uid"
	job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := f.c.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	phase := corev1.PodSucceeded
	if condition == batchv1.JobFailed {
		phase = corev1.PodFailed
	}
	if err := f.c.Create(ctx, cleanupPodFor(job, phase, code, message)); err != nil {
		t.Fatal(err)
	}
}

// Review B1: deleting a backup that never succeeded cleans up what its Job
// left at the planned destination, in abandoned mode (the cleanup Job
// deletes only when the stamp there names this backup's UID), so a
// re-created backup of the same name does not find this backup's stamp.
// The cleanup is best effort: a failure releases the finalizer with a
// warning instead of holding the deletion.
func TestFinalize_AbandonedBackupCleanup(t *testing.T) {
	t.Run("failed backup runs an abandoned cleanup at the planned destination", func(t *testing.T) {
		f := abandonedFixture(t, v1alpha1.BackupPhaseFailed)
		f.producerJob(t, batchv1.JobFailed)
		if done, _ := f.finalize(t); done {
			t.Fatal("released before the abandoned stamp was cleaned up")
		}
		job, ok := f.cleanupJob(t)
		if !ok {
			t.Fatal("no cleanup Job")
		}
		env := envMap(job.Spec.Template.Spec.Containers[0].Env)
		if env["BLOODRAVEN_CLEANUP_MODE"] != "abandoned" || env["BLOODRAVEN_BACKUP_UID"] != "art-uid" ||
			env["BLOODRAVEN_OUTPUT_URL"] != "lion/art" || env["BLOODRAVEN_BACKUP_NAME"] != "art" {
			t.Fatalf("cleanup env %v", env)
		}
		if _, ok := env["BLOODRAVEN_BACKUP_PASSPHRASE_FILE"]; ok {
			t.Fatal("a plaintext backup's cleanup mounts a passphrase")
		}
		if f.mb.Status.Location != "" {
			t.Fatalf("status.location = %q, want empty", f.mb.Status.Location)
		}

		f.finishCleanupJob(t, batchv1.JobComplete, 0, backupmeta.FormatCleanupComplete(3))
		drainEvents(f.rec)
		if done, _ := f.finalize(t); !done {
			t.Fatal("not released after the abandoned cleanup succeeded")
		}
		if n := countEvents(f.rec, "Normal ArtifactCleanupSucceeded cleanup of the unfinished backup"); n != 1 {
			t.Fatal("no success event")
		}
	})

	t.Run("failed cleanup releases with a warning and is not retried", func(t *testing.T) {
		f := abandonedFixture(t, v1alpha1.BackupPhaseFailed)
		f.finalize(t)
		f.finishCleanupJob(t, batchv1.JobFailed, backupmeta.CleanupExitRejected, backupmeta.CleanupFailedPrefix+" decrypt backup stamp: wrong passphrase")
		drainEvents(f.rec)
		if done, _ := f.finalize(t); !done {
			t.Fatal("a failed abandoned cleanup held the deletion")
		}
		events := strings.Join(drainEvents(f.rec), "\n")
		if !strings.Contains(events, "ArtifactCleanupSkipped") || !strings.Contains(events, "wrong passphrase") ||
			!strings.Contains(events, "lion/art"+backupmeta.StampSuffix) {
			t.Fatalf("events %s", events)
		}
	})

	t.Run("a running backup's Job is stopped first", func(t *testing.T) {
		f := abandonedFixture(t, v1alpha1.BackupPhaseRunning)
		producer := f.producerJob(t, "")
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "dump-pod", Namespace: "ns",
			Labels: map[string]string{labelMysqlBackup: f.mb.Name, labelResourceKind: "backup"}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning}}
		if err := f.c.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
		if done, requeue := f.finalize(t); done || requeue != 5*time.Second {
			t.Fatalf("done=%v requeue=%s, want a short wait", done, requeue)
		}
		if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(producer), &batchv1.Job{}); !apierrors.IsNotFound(err) {
			t.Fatalf("producing Job not deleted: %v", err)
		}
		// Its pod still runs: no cleanup may start.
		if done, _ := f.finalize(t); done {
			t.Fatal("released while the dump pod runs")
		}
		if _, ok := f.cleanupJob(t); ok {
			t.Fatal("cleanup started while the dump pod runs")
		}
		if err := f.c.Delete(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
		f.finalize(t)
		if _, ok := f.cleanupJob(t); !ok {
			t.Fatal("no cleanup Job once the dump pod is gone")
		}
	})

	t.Run("encrypted backup mounts the producing Job's passphrase", func(t *testing.T) {
		f := abandonedFixture(t, v1alpha1.BackupPhaseFailed)
		f.mb.Status.Encrypted = true
		if err := f.c.Status().Update(context.Background(), f.mb); err != nil {
			t.Fatal(err)
		}
		f.producerJob(t, batchv1.JobFailed, corev1.Volume{Name: "backup-passphrase", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "launch-passphrase", Items: []corev1.KeyToPath{{Key: "k", Path: backupPassphraseFileName}}}}})
		f.finalize(t)
		job, ok := f.cleanupJob(t)
		if !ok {
			t.Fatal("no cleanup Job")
		}
		env := envMap(job.Spec.Template.Spec.Containers[0].Env)
		if env["BLOODRAVEN_BACKUP_PASSPHRASE_FILE"] != backupPassphraseMountPath+"/"+backupPassphraseFileName {
			t.Fatalf("env %v", env)
		}
		found := false
		for _, v := range job.Spec.Template.Spec.Volumes {
			if v.Name == "backup-passphrase" && v.Secret != nil && v.Secret.SecretName == "launch-passphrase" {
				found = true
			}
		}
		if !found {
			t.Fatal("passphrase volume not taken from the producing Job")
		}
	})

	t.Run("encrypted backup without a passphrase source is skipped", func(t *testing.T) {
		f := abandonedFixture(t, v1alpha1.BackupPhaseFailed)
		f.mb.Status.Encrypted = true
		if err := f.c.Status().Update(context.Background(), f.mb); err != nil {
			t.Fatal(err)
		}
		drainEvents(f.rec)
		if done, _ := f.finalize(t); !done {
			t.Fatal("held the deletion though the stamp cannot be read")
		}
		if n := countEvents(f.rec, "ArtifactCleanupSkipped"); n != 1 {
			t.Fatal("no skip warning")
		}
		if _, ok := f.cleanupJob(t); ok {
			t.Fatal("a cleanup Job was created without a passphrase")
		}
	})

	t.Run("a backup whose Job never ran is released at once", func(t *testing.T) {
		f := abandonedFixture(t, v1alpha1.BackupPhaseFailed)
		f.mb.Status.ArtifactStorage = nil
		if err := f.c.Status().Update(context.Background(), f.mb); err != nil {
			t.Fatal(err)
		}
		if done, _ := f.finalize(t); !done {
			t.Fatal("not released")
		}
		if err := f.c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: cleanupJobName("art")}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
			t.Fatal("cleanup Job created for a backup that wrote nothing")
		}
	})
}
