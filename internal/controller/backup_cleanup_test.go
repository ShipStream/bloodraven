package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
)

const cleanupTestImage = "registry.test/bloodraven:cleanup"

// cleanupTestInputs configures the operator image and a recorded
// destination so builder tests exercise the Job shape, not the guards.
func cleanupTestInputs(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, profile *v1alpha1.BackupProfile, backup *v1alpha1.MysqlBackup) cleanupJobInputs {
	t.Helper()
	SetOperatorImageDefaults(cleanupTestImage, "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })
	return cleanupJobInputs{
		FailoverGroup: fg, Profile: profile, Backup: backup,
		Storage: &v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups", ClaimName: "lion-backups"},
	}
}

func TestArtifactStorageFromJob(t *testing.T) {
	SetOperatorImageDefaults(cleanupTestImage, "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })
	build := func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, profile int) *batchv1.Job {
		t.Helper()
		mb := backupCR("snap", fg.Name, fg.Spec.Backup.Profiles[profile].Name)
		job, err := BuildBackupJob(BackupJobInputs{
			FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[profile], Backup: mb,
			SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
		})
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	withEndpoint := fgWithBackup()
	withEndpoint.Spec.Backup.Profiles[0].Storage.S3.EndpointURL = "https://minio.example:9000"
	withEndpoint.Spec.Backup.Profiles[0].Storage.S3.Region = "us-west-2"
	awsDefault := fgWithBackup()
	awsDefault.Spec.Backup.Profiles[0].Storage.S3.EndpointURL = ""
	awsDefault.Spec.Backup.Profiles[0].Storage.S3.Region = ""

	tests := []struct {
		name    string
		job     func(t *testing.T) *batchv1.Job
		want    v1alpha1.BackupArtifactStorage
		wantErr string
	}{
		{name: "plain S3", job: func(t *testing.T) *batchv1.Job { return build(t, fgWithBackup(), 0) },
			want: v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups", EndpointURL: "https://minio.local", Region: "us-east-1"}},
		{name: "plain S3 on the AWS default endpoint", job: func(t *testing.T) *batchv1.Job { return build(t, awsDefault, 0) },
			want: v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups"}},
		{name: "plain S3 with endpoint and region", job: func(t *testing.T) *batchv1.Job { return build(t, withEndpoint, 0) },
			want: v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups", EndpointURL: "https://minio.example:9000", Region: "us-west-2"}},
		{name: "encrypted S3 reads the uploader", job: func(t *testing.T) *batchv1.Job { return build(t, fgWithEncryptedBackup(), 0) },
			want: v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups", EndpointURL: "https://minio.local", Region: "us-east-1"}},
		{name: "PVC claim", job: func(t *testing.T) *batchv1.Job { return build(t, fgWithBackup(), 1) },
			want: v1alpha1.BackupArtifactStorage{ClaimName: ownedBackupPVCName("lion", "daily-local")}},
		{name: "bucket from a runtime-only source", job: func(t *testing.T) *batchv1.Job {
			job := build(t, fgWithBackup(), 0)
			for i, env := range job.Spec.Template.Spec.Containers[0].Env {
				if env.Name == "BLOODRAVEN_S3_BUCKET" {
					job.Spec.Template.Spec.Containers[0].Env[i] = corev1.EnvVar{Name: env.Name, ValueFrom: &corev1.EnvVarSource{}}
				}
			}
			return job
		}, wantErr: "unavailable runtime environment"},
		{name: "PVC without a claim volume", job: func(t *testing.T) *batchv1.Job {
			job := build(t, fgWithBackup(), 1)
			job.Spec.Template.Spec.Volumes = nil
			return job
		}, wantErr: "no backups PersistentVolumeClaim"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := artifactStorageFromJob(tc.job(t))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || *got != tc.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

func TestBuildCleanupJob_RecordedDestinationAndShape(t *testing.T) {
	fg := fgWithBackup()
	s3Backup := backupCR("art", "lion", "nightly-s3")
	s3Backup.Status = v1alpha1.MysqlBackupStatus{Location: "lion/art", StorageType: v1alpha1.BackupStorageS3,
		ArtifactStorage: &v1alpha1.BackupArtifactStorage{Bucket: "old-bucket", EndpointURL: "https://old.example", Region: "eu-west-1"}}
	pvcBackup := backupCR("art", "lion", "daily-local")
	pvcBackup.Status = v1alpha1.MysqlBackupStatus{Location: "/backups/art", StorageType: v1alpha1.BackupStoragePVC,
		ArtifactStorage: &v1alpha1.BackupArtifactStorage{ClaimName: "old-claim"}}
	noCreds := fg.Spec.Backup.Profiles[0]
	noCreds.Storage.S3 = &v1alpha1.S3Storage{Bucket: "bloodraven-backups"}

	tests := []struct {
		name    string
		profile v1alpha1.BackupProfile
		backup  *v1alpha1.MysqlBackup
		image   string
		check   func(t *testing.T, job *batchv1.Job)
		wantErr error
	}{
		{
			name: "S3 uses the recorded bucket, endpoint and region", profile: fg.Spec.Backup.Profiles[0], backup: s3Backup, image: cleanupTestImage,
			check: func(t *testing.T, job *batchv1.Job) {
				env := envMap(job.Spec.Template.Spec.Containers[0].Env)
				if env["BLOODRAVEN_S3_BUCKET"] != "old-bucket" || env["BLOODRAVEN_S3_ENDPOINT_OVERRIDE"] != "https://old.example" || env["AWS_REGION"] != "eu-west-1" {
					t.Errorf("destination followed the live profile: %v", env)
				}
				var secret string
				for _, v := range job.Spec.Template.Spec.Volumes {
					if v.Name == "aws-creds" {
						secret = v.Secret.SecretName
					}
				}
				if secret != "s3-creds" {
					t.Errorf("credentials must come from the live profile, got %q", secret)
				}
			},
		},
		{
			name: "PVC uses the recorded claim", profile: fg.Spec.Backup.Profiles[1], backup: pvcBackup, image: cleanupTestImage,
			check: func(t *testing.T, job *batchv1.Job) {
				for _, v := range job.Spec.Template.Spec.Volumes {
					if v.Name == "backups" && v.PersistentVolumeClaim.ClaimName != "old-claim" {
						t.Errorf("claim followed the live profile: %s", v.PersistentVolumeClaim.ClaimName)
					}
				}
			},
		},
		{name: "operator image unset", profile: fg.Spec.Backup.Profiles[0], backup: s3Backup, wantErr: errOperatorImageUnset},
		{name: "S3 credentials missing", profile: noCreds, backup: s3Backup, image: cleanupTestImage, wantErr: errCleanupCredentialsMissing},
		{name: "no recorded destination", profile: fg.Spec.Backup.Profiles[0], image: cleanupTestImage, wantErr: errCleanupDestinationNotStored,
			backup: &v1alpha1.MysqlBackup{ObjectMeta: s3Backup.ObjectMeta, Status: v1alpha1.MysqlBackupStatus{Location: "lion/art", StorageType: v1alpha1.BackupStorageS3}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			SetOperatorImageDefaults(tc.image, "bloodraven")
			t.Cleanup(func() { SetOperatorImageDefaults("", "") })
			profile := tc.profile
			job, err := buildCleanupJob(cleanupJobInputs{FailoverGroup: fg, Profile: &profile, Backup: tc.backup})
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			spec := job.Spec.Template.Spec
			c := spec.Containers[0]
			if c.Name != cleanupContainerName || c.Image != cleanupTestImage || c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
				t.Errorf("unexpected container %s %s %s", c.Name, c.Image, c.TerminationMessagePolicy)
			}
			env := envMap(c.Env)
			if env["BLOODRAVEN_BACKUP_NAME"] != "art" || env["HOME"] != "" {
				t.Errorf("unexpected env %v", env)
			}
			for _, v := range spec.Volumes {
				if v.Name == "mysql-creds" || v.Name == "scripts" {
					t.Errorf("cleanup mounts %s; it never talks to MySQL", v.Name)
				}
			}
			rules := job.Spec.PodFailurePolicy.Rules
			if len(rules) != 1 || rules[0].Action != batchv1.PodFailurePolicyActionFailJob || rules[0].OnExitCodes.Values[0] != backupmeta.CleanupExitRejected {
				t.Errorf("exit %d must fail the Job without in-Job retries: %+v", backupmeta.CleanupExitRejected, rules)
			}
			tc.check(t, job)
		})
	}
}

func TestArtifactIdentityProblem(t *testing.T) {
	SetOperatorImageDefaults(cleanupTestImage, "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })
	fg := fgWithBackup()
	mb := backupCR("art", "lion", "nightly-s3")
	producer, err := BuildBackupJob(BackupJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Backup: mb,
		SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil {
		t.Fatal(err)
	}
	s3 := &v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups"}
	tests := []struct {
		name, location string
		storageType    v1alpha1.BackupStorageType
		producer       *batchv1.Job
		want           string
	}{
		{name: "matches the producing Job", location: "lion/art", storageType: v1alpha1.BackupStorageS3, producer: producer},
		{name: "no producer, plain S3", location: "lion/art", storageType: v1alpha1.BackupStorageS3},
		{name: "encrypted S3", location: "s3://bloodraven-backups/lion/art/", storageType: v1alpha1.BackupStorageS3},
		{name: "PVC", location: "/backups/sub/art", storageType: v1alpha1.BackupStoragePVC},
		{name: "parent prefix", location: "lion", storageType: v1alpha1.BackupStorageS3, want: "does not end in the backup name"},
		{name: "another backup", location: "lion/art-2", storageType: v1alpha1.BackupStorageS3, want: "does not end in the backup name"},
		{name: "encrypted S3 other bucket", location: "s3://other/lion/art/", storageType: v1alpha1.BackupStorageS3, want: "differs from the recorded bucket"},
		{name: "PVC outside the mount", location: "/var/art", storageType: v1alpha1.BackupStoragePVC, want: "is not under /backups"},
		{name: "differs from the producing Job", location: "elsewhere/art", storageType: v1alpha1.BackupStorageS3, producer: producer, want: "differs from the producing Job"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := mb.DeepCopy()
			b.Status.Location, b.Status.StorageType = tc.location, tc.storageType
			got := artifactIdentityProblem(b, s3, tc.producer)
			if tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

type cleanupFixture struct {
	r   *MysqlBackupReconciler
	c   client.Client
	fg  *v1alpha1.MysqlFailoverGroup
	mb  *v1alpha1.MysqlBackup
	rec *record.FakeRecorder
}

func newCleanupFixture(t *testing.T) *cleanupFixture {
	t.Helper()
	SetOperatorImageDefaults(cleanupTestImage, "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })
	fg := fgWithBackup()
	mb := backupCR("art", "lion", "nightly-s3")
	mb.UID = types.UID("art-uid")
	now := metav1.Now()
	mb.Status = v1alpha1.MysqlBackupStatus{
		Phase: v1alpha1.BackupPhaseSucceeded, CompletionTime: &now,
		Location: "lion/art", StorageType: v1alpha1.BackupStorageS3,
		ArtifactStorage: &v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups"},
	}
	r, c := newBackupReconciler(t, fg, mb)
	rec := record.NewFakeRecorder(50)
	r.Recorder = rec
	return &cleanupFixture{r: r, c: c, fg: fg, mb: mb, rec: rec}
}

func (f *cleanupFixture) finalize(t *testing.T) (bool, time.Duration) {
	t.Helper()
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	done, requeue, err := f.r.finalize(context.Background(), f.mb)
	if err != nil {
		t.Fatalf("finalize returned an error, want a requeue: %v", err)
	}
	return done, requeue
}

func (f *cleanupFixture) cleanupJob(t *testing.T) (*batchv1.Job, bool) {
	t.Helper()
	var job batchv1.Job
	err := f.c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: cleanupJobName(f.mb.Name)}, &job)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return &job, true
}

// failCleanupJob marks the cleanup Job failed `ago` and gives it a pod whose
// container exited with code (and message).
func (f *cleanupFixture) failCleanupJob(t *testing.T, job *batchv1.Job, ago time.Duration, code int32, message string) {
	t.Helper()
	ctx := context.Background()
	job.UID = types.UID("cleanup-uid-" + job.ResourceVersion)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-ago)), Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}
	if err := f.c.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
		t.Fatal(err)
	}
	if code < 0 {
		return
	}
	pod := cleanupPodFor(job, corev1.PodFailed, code, message)
	if err := f.c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
}

func cleanupPodFor(job *batchv1.Job, phase corev1.PodPhase, code int32, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: job.Name + "-pod", Namespace: job.Namespace,
			Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name, labelMysqlBackup: job.Labels[labelMysqlBackup], labelResourceKind: "backup-cleanup"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: boolPtr(true)}},
		},
		Status: corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{{
			Name:  cleanupContainerName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: message}},
		}}},
	}
}

func cleanupCondition(t *testing.T, f *cleanupFixture) *metav1.Condition {
	t.Helper()
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	return apiMeta.FindStatusCondition(f.mb.Status.Conditions, ConditionArtifactCleanup)
}

func countEvents(rec *record.FakeRecorder, needle string) int {
	n := 0
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, needle) {
			n++
		}
	}
	return n
}

func TestFinalize_FailedCleanupIsRecordedOnceAndRetried(t *testing.T) {
	tests := []struct {
		name        string
		ago         time.Duration
		code        int32
		message     string
		wantReason  string
		wantDeleted bool
		wantWait    time.Duration
	}{
		{name: "refusal waits 30m", ago: time.Minute, code: backupmeta.CleanupExitRejected,
			message: "BLOODRAVEN_CLEANUP_FAILED: init S3 store: bad creds", wantReason: cleanupReasonRejected, wantWait: cleanupRetryAfterRejection - time.Minute},
		{name: "transient waits 5m", ago: time.Minute, code: backupmeta.CleanupExitTransient,
			message: "BLOODRAVEN_CLEANUP_FAILED: list: timeout", wantReason: cleanupReasonFailed, wantWait: cleanupRetryAfterFailure - time.Minute},
		{name: "transient past its delay is replaced", ago: 6 * time.Minute, code: backupmeta.CleanupExitTransient,
			message: "BLOODRAVEN_CLEANUP_FAILED: list: timeout", wantReason: cleanupReasonFailed, wantDeleted: true},
		{name: "no pod left (deadline, eviction) counts as transient", ago: time.Minute, code: -1, wantReason: cleanupReasonFailed, wantWait: cleanupRetryAfterFailure - time.Minute},
		{name: "legacy cleanup.py rmdump failure long ago is replaced", ago: 48 * time.Hour, code: backupmeta.CleanupExitRejected,
			message: "", wantReason: cleanupReasonRejected, wantDeleted: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCleanupFixture(t)
			if done, _ := f.finalize(t); done {
				t.Fatal("released before cleanup ran")
			}
			job, ok := f.cleanupJob(t)
			if !ok {
				t.Fatal("cleanup Job not created")
			}
			f.failCleanupJob(t, job, tc.ago, tc.code, tc.message)
			counter := bmetrics.BackupArtifactCleanupFailuresTotal.WithLabelValues("lion", "nightly-s3", tc.wantReason)
			before := testutil.ToFloat64(counter)
			drainEvents(f.rec)

			for range 2 {
				done, requeue := f.finalize(t)
				if done {
					t.Fatal("released after a failed cleanup")
				}
				if !tc.wantDeleted && (requeue > tc.wantWait || requeue < tc.wantWait-time.Minute) {
					t.Fatalf("requeue %s, want about %s", requeue, tc.wantWait)
				}
				if _, exists := f.cleanupJob(t); exists == tc.wantDeleted {
					t.Fatalf("cleanup Job exists=%v, want deleted=%v", exists, tc.wantDeleted)
				}
				if tc.wantDeleted {
					break
				}
			}
			if got := testutil.ToFloat64(counter) - before; got != 1 {
				t.Fatalf("failure counted %v times, want once per Job", got)
			}
			cond := cleanupCondition(t, f)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != tc.wantReason {
				t.Fatalf("condition %+v, want reason %s", cond, tc.wantReason)
			}
			if tc.message != "" && !strings.Contains(cond.Message, strings.TrimPrefix(tc.message, backupmeta.CleanupFailedPrefix+" ")) {
				t.Fatalf("condition message %q lacks the cause", cond.Message)
			}
			if n := countEvents(f.rec, "ArtifactCleanupFailed"); n != 1 {
				t.Fatalf("ArtifactCleanupFailed events = %d, want 1", n)
			}
			if tc.wantDeleted {
				// The replacement runs the new command.
				if done, _ := f.finalize(t); done {
					t.Fatal("released without a successful cleanup")
				}
				job, ok := f.cleanupJob(t)
				if !ok || job.Spec.Template.Spec.Containers[0].Name != cleanupContainerName {
					t.Fatalf("replacement Job not created: %v", ok)
				}
			}
		})
	}
}

func TestFinalize_ReplacementWaitsForOldPods(t *testing.T) {
	f := newCleanupFixture(t)
	f.finalize(t)
	job, _ := f.cleanupJob(t)
	pod := cleanupPodFor(job, corev1.PodRunning, 0, "")
	pod.Status.ContainerStatuses = nil
	if err := f.c.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if done, requeue := f.finalize(t); done || requeue != 5*time.Second {
		t.Fatalf("done=%v requeue=%s, want a short wait", done, requeue)
	}
	if _, exists := f.cleanupJob(t); exists {
		t.Fatal("replacement started while the old pod still runs")
	}
}

func TestFinalize_SucceededCleanupVerdict(t *testing.T) {
	tests := []struct {
		name, message, wantEvent string
	}{
		{name: "objects deleted", message: backupmeta.FormatCleanupComplete(12), wantEvent: "Normal ArtifactCleanupSucceeded artifact cleanup removed 12 objects"},
		{name: "nothing found warns", message: backupmeta.FormatCleanupComplete(0), wantEvent: "Warning ArtifactCleanupNothingFound"},
		{name: "legacy job without a verdict", message: "", wantEvent: "Normal ArtifactCleanupSucceeded artifact cleanup succeeded"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCleanupFixture(t)
			f.finalize(t)
			job, _ := f.cleanupJob(t)
			job.UID = "cleanup-ok"
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			if err := f.c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			if err := f.c.Create(context.Background(), cleanupPodFor(job, corev1.PodSucceeded, 0, tc.message)); err != nil {
				t.Fatal(err)
			}
			drainEvents(f.rec)
			if done, _ := f.finalize(t); !done {
				t.Fatal("successful cleanup did not release the finalizer")
			}
			if n := countEvents(f.rec, tc.wantEvent); n != 1 {
				t.Fatalf("want one %q event", tc.wantEvent)
			}
		})
	}
}

func TestFinalize_BlockedCleanup(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T, f *cleanupFixture)
		wantReason string
	}{
		{
			name: "no snapshot and no producing Job",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.mb.Status.ArtifactStorage = nil
			},
			wantReason: cleanupReasonDestinationUnverified,
		},
		{
			name: "location is not this backup's artifact",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.mb.Status.Location = "lion"
			},
			wantReason: cleanupReasonDestinationMismatch,
		},
		{
			name: "operator image unset",
			setup: func(t *testing.T, f *cleanupFixture) {
				SetOperatorImageDefaults("", "")
			},
			wantReason: cleanupReasonOperatorImageUnset,
		},
		{
			name: "profile lost its S3 credentials",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.fg.Spec.Backup.Profiles[0].Storage.S3.CredentialsSecret = ""
				if err := f.c.Update(context.Background(), f.fg); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: cleanupReasonCredentialsMissing,
		},
		{
			name: "foreign cleanup Job with our name",
			setup: func(t *testing.T, f *cleanupFixture) {
				foreign := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: cleanupJobName(f.mb.Name), Namespace: "ns",
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "shipstream.io/v1alpha1", Kind: "MysqlBackup", Name: f.mb.Name, UID: "previous-incarnation", Controller: boolPtr(true)}}}}
				if err := f.c.Create(context.Background(), foreign); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: cleanupReasonJobConflict,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newCleanupFixture(t)
			if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
				t.Fatal(err)
			}
			tc.setup(t, f)
			if err := f.c.Status().Update(context.Background(), f.mb); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if done, requeue := f.finalize(t); done || requeue <= 0 {
					t.Fatalf("done=%v requeue=%s, want a blocked requeue", done, requeue)
				}
			}
			cond := cleanupCondition(t, f)
			if cond == nil || cond.Reason != tc.wantReason {
				t.Fatalf("condition %+v, want reason %s", cond, tc.wantReason)
			}
			if n := countEvents(f.rec, "ArtifactCleanupBlocked "+tc.wantReason); n != 1 {
				t.Fatalf("ArtifactCleanupBlocked events = %d, want 1 per distinct cause", n)
			}
			if job, ok := f.cleanupJob(t); ok && metav1.IsControlledBy(job, f.mb) {
				t.Fatal("a cleanup Job was created while blocked")
			}
		})
	}
}

func TestFinalize_RecoversSnapshotFromProducingJob(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	producer, err := BuildBackupJob(BackupJobInputs{FailoverGroup: f.fg, Profile: f.fg.Spec.Backup.Profiles[0], Backup: f.mb,
		SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(f.mb, producer, f.r.Scheme); err != nil {
		t.Fatal(err)
	}
	producer.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := f.c.Create(ctx, producer); err != nil {
		t.Fatal(err)
	}
	// The profile moved to another bucket after the backup ran.
	f.fg.Spec.Backup.Profiles[0].Storage.S3.Bucket = "new-bucket"
	if err := f.c.Update(ctx, f.fg); err != nil {
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
		t.Fatal("released before cleanup ran")
	}
	if f.mb.Status.ArtifactStorage == nil || f.mb.Status.ArtifactStorage.Bucket != "bloodraven-backups" {
		t.Fatalf("snapshot not recovered from the producing Job: %+v", f.mb.Status.ArtifactStorage)
	}
	job, ok := f.cleanupJob(t)
	if !ok || envMap(job.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_S3_BUCKET"] != "bloodraven-backups" {
		t.Fatal("cleanup did not target the bucket the backup was written to")
	}
}

func TestParseCleanupVerdictRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 4096} {
		if got, ok := backupmeta.ParseCleanupComplete("noise\n" + backupmeta.FormatCleanupComplete(n) + "\n"); !ok || got != n {
			t.Fatalf("round trip %d: got %d %v", n, got, ok)
		}
	}
	for _, msg := range []string{"", "BLOODRAVEN_CLEANUP_COMPLETE prefix=x/", "BLOODRAVEN_CLEANUP_COMPLETE objects=-1", backupmeta.CleanupFailedPrefix + " boom"} {
		if _, ok := backupmeta.ParseCleanupComplete(msg); ok {
			t.Fatalf("parsed %q as a success verdict", msg)
		}
	}
}
