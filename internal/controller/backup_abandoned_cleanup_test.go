package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
)

// abandonedFixture is a cleanup fixture whose backup never succeeded: no
// completed artifact, but status.artifactStorage records the planned
// destination its Job wrote the backup stamp to.
func abandonedFixture(t *testing.T, phase v1alpha1.BackupPhase) *cleanupFixture {
	t.Helper()
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Status.Phase, f.mb.Status.CompletionTime = phase, nil
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
			Labels: map[string]string{v1alpha1.MysqlBackupLabel: f.mb.Name, v1alpha1.ResourceLabel: "backup"},
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
			Labels: map[string]string{v1alpha1.MysqlBackupLabel: f.mb.Name, v1alpha1.ResourceLabel: "backup"}},
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
		f.mb.Status.Encryption = &v1alpha1.BackupEncryptionStatus{Mode: v1alpha1.EncryptionModePassphrase, Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM}
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
		f.mb.Status.Encryption = &v1alpha1.BackupEncryptionStatus{Mode: v1alpha1.EncryptionModePassphrase, Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM}
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

// runningProducerPod creates a still-running pod of the backup's producer.
func (f *cleanupFixture) runningProducerPod(t *testing.T, mutate func(*corev1.Pod)) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "dump-pod", Namespace: "ns",
		Labels: map[string]string{v1alpha1.MysqlBackupLabel: f.mb.Name, v1alpha1.ResourceLabel: "backup"}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if mutate != nil {
		mutate(pod)
	}
	if err := f.c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

// F002: every abandoned-mode release waits for the producer to stop, even
// the early ones (group or profile gone, destination mismatch), since a
// release lets a backup of the same name reuse the destination while the
// old uploader may still write to it.
func TestFinalize_AbandonedEarlyReleaseStopsProducerFirst(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, f *cleanupFixture)
	}{
		{"profile gone", func(t *testing.T, f *cleanupFixture) {
			f.fg.Spec.Backup.Profiles[0].Name = "renamed"
			if err := f.c.Update(context.Background(), f.fg); err != nil {
				t.Fatal(err)
			}
		}},
		{"group gone", func(t *testing.T, f *cleanupFixture) {
			if err := f.c.Delete(context.Background(), f.fg); err != nil {
				t.Fatal(err)
			}
		}},
		{"destination mismatch", func(t *testing.T, f *cleanupFixture) {
			ctx := context.Background()
			if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
				t.Fatal(err)
			}
			f.mb.Status.ArtifactStorage.Location = "lion/other"
			if err := f.c.Status().Update(ctx, f.mb); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := abandonedFixture(t, v1alpha1.BackupPhaseRunning)
			producer := f.producerJob(t, "")
			pod := f.runningProducerPod(t, nil)
			tc.mutate(t, f)
			if done, _ := f.finalize(t); done {
				t.Fatal("released before the producing Job was stopped")
			}
			if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(producer), &batchv1.Job{}); !apierrors.IsNotFound(err) {
				t.Fatalf("producing Job not deleted: %v", err)
			}
			if done, _ := f.finalize(t); done {
				t.Fatal("released while the dump pod still runs")
			}
			if err := f.c.Delete(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			drainEvents(f.rec)
			if done, _ := f.finalize(t); !done {
				t.Fatal("not released once the producer stopped")
			}
			if n := countEvents(f.rec, "ArtifactCleanupSkipped"); n != 1 {
				t.Fatal("no skip warning")
			}
			if _, ok := f.cleanupJob(t); ok {
				t.Fatal("a cleanup Job ran although cleanup cannot proceed")
			}
		})
	}
}

// F024: a lost post-create status patch leaves status.artifactStorage
// empty; the failed producer's own destination is recovered so its stamp
// and partial dump are cleaned up instead of released silently.
func TestFinalize_AbandonedRecoversDestinationFromFailedProducer(t *testing.T) {
	f := abandonedFixture(t, v1alpha1.BackupPhaseFailed)
	ctx := context.Background()
	producer, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: f.fg, Profile: f.fg.Spec.Backup.Profiles[0], Backup: f.mb,
		SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(f.mb, producer, f.r.Scheme); err != nil {
		t.Fatal(err)
	}
	producer.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := f.c.Create(ctx, producer); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Status.ArtifactStorage = nil
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if done, _ := f.finalize(t); done {
		t.Fatal("released the failed producer's artifacts without cleanup")
	}
	if s := f.mb.Status.ArtifactStorage; s == nil || s.Location != "lion/art" || s.Bucket != "bloodraven-backups" {
		t.Fatalf("planned destination not recovered from the producer: %+v", s)
	}
	job, ok := f.cleanupJob(t)
	if !ok {
		t.Fatal("no cleanup Job")
	}
	if env := envMap(job.Spec.Template.Spec.Containers[0].Env); env["BLOODRAVEN_CLEANUP_MODE"] != "abandoned" || env["BLOODRAVEN_OUTPUT_URL"] != "lion/art" {
		t.Fatalf("cleanup env %v", env)
	}
}

// F025: the passphrase an encrypted producer mounted is recorded with its
// destination, so cleanup after the stopped Job is gone still reads the
// stamp with it rather than with a profile edited since.
func TestFinalize_AbandonedUsesRecordedPassphrase(t *testing.T) {
	t.Run("artifactStorageFromJob records the mounted passphrase", func(t *testing.T) {
		SetOperatorImageDefaults(cleanupTestImage, "bloodraven")
		t.Cleanup(func() { SetOperatorImageDefaults("", "") })
		fg := fgWithBackup()
		profile := fg.Spec.Backup.Profiles[0]
		profile.Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "secret-a", Key: "pp"}}
		mb := backupCR("art", "lion", profile.Name)
		job, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: fg, Profile: profile, Backup: mb,
			SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
		if err != nil {
			t.Fatal(err)
		}
		storage, err := artifactStorageFromJob(job)
		if err != nil {
			t.Fatal(err)
		}
		// The Job mounts the backup's dump-key snapshot, never the
		// profile's Secret (#203), and records it.
		if got := storage.PassphraseSecretKeyRef; got == nil || got.Name != testDumpKeySecret || got.Key != pitrKeySnapshotDataKey {
			t.Fatalf("passphrase not recorded: %+v", got)
		}
		profile.Encryption = nil
		plain, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: fg, Profile: profile, Backup: mb,
			SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
		if err != nil {
			t.Fatal(err)
		}
		if storage, err := artifactStorageFromJob(plain); err != nil || storage.PassphraseSecretKeyRef != nil {
			t.Fatalf("plaintext Job records a passphrase: %+v %v", storage, err)
		}
	})

	t.Run("cleanup after the producer is gone mounts the recorded Secret", func(t *testing.T) {
		f := abandonedFixture(t, v1alpha1.BackupPhaseRunning)
		ctx := context.Background()
		f.fg.Spec.Backup.Profiles[0].Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "secret-b"}}
		if err := f.c.Update(ctx, f.fg); err != nil {
			t.Fatal(err)
		}
		if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
			t.Fatal(err)
		}
		f.mb.Status.Encryption = &v1alpha1.BackupEncryptionStatus{Mode: v1alpha1.EncryptionModePassphrase, Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM}
		f.mb.Status.ArtifactStorage.PassphraseSecretKeyRef = &v1alpha1.SecretKeyRef{Name: "secret-a", Key: "pp"}
		if err := f.c.Status().Update(ctx, f.mb); err != nil {
			t.Fatal(err)
		}
		f.producerJob(t, "", corev1.Volume{Name: "backup-passphrase", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "secret-a", Items: []corev1.KeyToPath{{Key: "pp", Path: backupPassphraseFileName}}}}})
		// The recorded Secret must exist (Δ17).
		if err := f.c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "secret-a"}, Data: map[string][]byte{"pp": []byte("k")}}); err != nil {
			t.Fatal(err)
		}
		f.finalize(t) // stops the producer
		f.finalize(t)
		job, ok := f.cleanupJob(t)
		if !ok {
			t.Fatal("no cleanup Job")
		}
		for _, v := range job.Spec.Template.Spec.Volumes {
			if v.Name == "backup-passphrase" {
				if v.Secret == nil || v.Secret.SecretName != "secret-a" || len(v.Secret.Items) != 1 || v.Secret.Items[0].Key != "pp" {
					t.Fatalf("passphrase volume %+v, want the launch Secret secret-a/pp", v.Secret)
				}
				return
			}
		}
		t.Fatal("no passphrase volume")
	})
}

// F026: a producer pod that never stops (an unreachable node) keeps the
// deletion held, since it may still write, but after producerStopStuckAfter
// the wait is reported with the remedy instead of requeueing silently.
func TestFinalize_AbandonedReportsStuckProducer(t *testing.T) {
	f := abandonedFixture(t, v1alpha1.BackupPhaseRunning)
	stopping := metav1.NewTime(time.Now().Add(-producerStopStuckAfter - time.Minute))
	pod := f.runningProducerPod(t, func(p *corev1.Pod) {
		p.Finalizers = []string{"test/hold"}
		p.Status.Phase = corev1.PodUnknown
	})
	// The pod is Terminating on an unreachable node.
	if err := f.c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if done, requeue := f.finalize(t); done || requeue != 5*time.Second {
		t.Fatalf("done=%v requeue=%s, want a quiet short wait before the deadline", done, requeue)
	}
	if n := countEvents(f.rec, "ArtifactCleanupProducerStuck"); n != 0 {
		t.Fatal("stuck warning before the deadline")
	}
	f.r.now = func() time.Time { return time.Now().Add(producerStopStuckAfter + time.Minute) }
	drainEvents(f.rec)
	done, requeue := f.finalize(t)
	if done || requeue != 30*time.Second {
		t.Fatalf("done=%v requeue=%s, want a held deletion", done, requeue)
	}
	cond := apiMeta.FindStatusCondition(f.mb.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if cond == nil || cond.Reason != v1alpha1.ReasonProducerStopStuck || !strings.Contains(cond.Message, "dump-pod") ||
		!strings.Contains(cond.Message, "--force") {
		t.Fatalf("condition %+v", cond)
	}
	if n := countEvents(f.rec, "Warning ArtifactCleanupProducerStuck"); n != 1 {
		t.Fatal("no stuck warning")
	}
	f.finalize(t)
	if n := countEvents(f.rec, "ArtifactCleanupProducerStuck"); n != 0 {
		t.Fatal("stuck warning repeated for an unchanged wait")
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("cleanup started while the producer may write")
	}

	// A fresh pod is not reported yet.
	if got := stuckProducerPods([]corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}},
		&metav1.Time{Time: time.Now()}, time.Now()); got != "" {
		t.Fatalf("fresh pod reported stuck: %q", got)
	}
	if got := stuckProducerPods([]corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}},
		&stopping, time.Now()); got != "" {
		t.Fatalf("terminated pod reported stuck: %q", got)
	}
}
