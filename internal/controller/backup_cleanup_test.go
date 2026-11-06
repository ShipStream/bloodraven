package controller

import (
	"context"
	"errors"
	"reflect"
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
	ctrl "sigs.k8s.io/controller-runtime"
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
			DumpKeySecretName: testDumpKeySecret,
			FailoverGroup:     fg, Profile: fg.Spec.Backup.Profiles[profile], Backup: mb,
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
			want: v1alpha1.BackupArtifactStorage{Location: "lion/snap", Bucket: "bloodraven-backups", EndpointURL: "https://minio.local", Region: "us-east-1"}},
		{name: "plain S3 on the AWS default endpoint", job: func(t *testing.T) *batchv1.Job { return build(t, awsDefault, 0) },
			want: v1alpha1.BackupArtifactStorage{Location: "lion/snap", Bucket: "bloodraven-backups"}},
		{name: "plain S3 with endpoint and region", job: func(t *testing.T) *batchv1.Job { return build(t, withEndpoint, 0) },
			want: v1alpha1.BackupArtifactStorage{Location: "lion/snap", Bucket: "bloodraven-backups", EndpointURL: "https://minio.example:9000", Region: "us-west-2"}},
		{name: "encrypted S3 reads the uploader", job: func(t *testing.T) *batchv1.Job { return build(t, fgWithEncryptedBackup(), 0) },
			want: v1alpha1.BackupArtifactStorage{Location: "s3://bloodraven-backups/lion/snap/", Bucket: "bloodraven-backups", EndpointURL: "https://minio.local", Region: "us-east-1",
				PassphraseSecret: &v1alpha1.BackupArtifactPassphrase{Name: testDumpKeySecret, Key: pitrKeySnapshotDataKey}}},
		{name: "PVC claim", job: func(t *testing.T) *batchv1.Job { return build(t, fgWithBackup(), 1) },
			want: v1alpha1.BackupArtifactStorage{Location: "/backups/snap", ClaimName: ownedBackupPVCName("lion", "daily-local")}},
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
			if err != nil || !reflect.DeepEqual(*got, tc.want) {
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
	producer, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Backup: mb,
		SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, location, recorded string
		storageType              v1alpha1.BackupStorageType
		producer                 *batchv1.Job
		want                     string
	}{
		{name: "matches the producing Job", location: "lion/art", recorded: "lion/art", storageType: v1alpha1.BackupStorageS3, producer: producer},
		{name: "no producer, plain S3", location: "lion/art", recorded: "lion/art", storageType: v1alpha1.BackupStorageS3},
		{name: "encrypted S3", location: "s3://bloodraven-backups/lion/art/", recorded: "s3://bloodraven-backups/lion/art/", storageType: v1alpha1.BackupStorageS3},
		{name: "PVC", location: "/backups/sub/art", recorded: "/backups/sub/art", storageType: v1alpha1.BackupStoragePVC},
		{name: "no recorded location", location: "lion/art", recorded: "", storageType: v1alpha1.BackupStorageS3, want: "no recorded artifact location"},
		// Same basename under another prefix: another namespace's backup
		// of the same name in a shared bucket or claim.
		{name: "S3 same name, other prefix, producer gone", location: "team-b/nightly/art", recorded: "team-a/nightly/art", storageType: v1alpha1.BackupStorageS3, want: "differs from the recorded artifact location"},
		{name: "encrypted S3 same name, other prefix", location: "s3://bloodraven-backups/team-b/art/", recorded: "s3://bloodraven-backups/team-a/art/", storageType: v1alpha1.BackupStorageS3, want: "differs from the recorded artifact location"},
		{name: "PVC same name, other subPath, producer gone", location: "/backups/team-b/art", recorded: "/backups/team-a/art", storageType: v1alpha1.BackupStoragePVC, want: "differs from the recorded artifact location"},
		{name: "recorded parent prefix", location: "lion", recorded: "lion", storageType: v1alpha1.BackupStorageS3, want: "does not end in the backup name"},
		{name: "encrypted S3 other bucket", location: "s3://other/lion/art/", recorded: "s3://other/lion/art/", storageType: v1alpha1.BackupStorageS3, want: "differs from the recorded bucket"},
		{name: "PVC outside the mount", location: "/var/art", recorded: "/var/art", storageType: v1alpha1.BackupStoragePVC, want: "is not under /backups"},
		{name: "differs from the producing Job", location: "elsewhere/art", recorded: "elsewhere/art", storageType: v1alpha1.BackupStorageS3, producer: producer, want: "differs from the producing Job"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := mb.DeepCopy()
			b.Status.Location, b.Status.StorageType = tc.location, tc.storageType
			storage := &v1alpha1.BackupArtifactStorage{Location: tc.recorded, Bucket: "bloodraven-backups", ClaimName: "claim"}
			got := artifactIdentityProblem(b, b.Status.Location, storage, tc.producer)
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
		ArtifactStorage: &v1alpha1.BackupArtifactStorage{Location: "lion/art", Bucket: "bloodraven-backups"},
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
			Labels:          map[string]string{"batch.kubernetes.io/job-name": job.Name, v1alpha1.MysqlBackupLabel: job.Labels[v1alpha1.MysqlBackupLabel], v1alpha1.ResourceLabel: "backup-cleanup"},
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
	return apiMeta.FindStatusCondition(f.mb.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
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
			message: "BLOODRAVEN_CLEANUP_FAILED: init S3 store: bad creds", wantReason: v1alpha1.ReasonCleanupRejected, wantWait: cleanupRetryAfterRejection - time.Minute},
		{name: "transient waits 5m", ago: time.Minute, code: backupmeta.CleanupExitTransient,
			message: "BLOODRAVEN_CLEANUP_FAILED: list: timeout", wantReason: v1alpha1.ReasonCleanupFailed, wantWait: cleanupRetryAfterFailure - time.Minute},
		{name: "transient past its delay is replaced", ago: 6 * time.Minute, code: backupmeta.CleanupExitTransient,
			message: "BLOODRAVEN_CLEANUP_FAILED: list: timeout", wantReason: v1alpha1.ReasonCleanupFailed, wantDeleted: true},
		{name: "no pod left (deadline, eviction) counts as transient", ago: time.Minute, code: -1, wantReason: v1alpha1.ReasonCleanupFailed, wantWait: cleanupRetryAfterFailure - time.Minute},
		// cleanup.py exited 2 when util.rmdump was missing; 2 is also a Go
		// panic, so it is an unclassified (transient) failure now.
		{name: "legacy cleanup.py rmdump failure long ago is replaced", ago: 48 * time.Hour, code: 2,
			message: "", wantReason: v1alpha1.ReasonCleanupFailed, wantDeleted: true},
		{name: "crash (exit 2) is not a refusal", ago: time.Minute, code: 2,
			message: "", wantReason: v1alpha1.ReasonCleanupFailed, wantWait: cleanupRetryAfterFailure - time.Minute},
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
			wantReason: v1alpha1.ReasonDestinationUnverified,
		},
		{
			name: "snapshot without a location and no producing Job",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.mb.Status.ArtifactStorage.Location = ""
			},
			wantReason: v1alpha1.ReasonDestinationUnverified,
		},
		{
			name: "producing Job owned by another object",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.mb.Status.ArtifactStorage = nil
				producer, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: f.fg, Profile: f.fg.Spec.Backup.Profiles[0], Backup: f.mb,
					SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
				if err != nil {
					t.Fatal(err)
				}
				producer.OwnerReferences = []metav1.OwnerReference{{APIVersion: "shipstream.io/v1alpha1", Kind: "MysqlBackup", Name: f.mb.Name, UID: "previous-incarnation", Controller: boolPtr(true)}}
				if err := f.c.Create(context.Background(), producer); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: v1alpha1.ReasonDestinationUnverified,
		},
		{
			name: "recorded destination cannot become a Job",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.mb.Status.ArtifactStorage.Bucket = ""
			},
			wantReason: v1alpha1.ReasonCleanupJobInvalid,
		},
		{
			name: "same name under another prefix, producer gone",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.mb.Status.Location = "team-b/art"
			},
			wantReason: v1alpha1.ReasonDestinationMismatch,
		},
		{
			name: "location is not this backup's artifact",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.mb.Status.Location = "lion"
			},
			wantReason: v1alpha1.ReasonDestinationMismatch,
		},
		{
			name: "operator image unset",
			setup: func(t *testing.T, f *cleanupFixture) {
				SetOperatorImageDefaults("", "")
			},
			wantReason: v1alpha1.ReasonOperatorImageUnset,
		},
		{
			name: "profile lost its S3 credentials",
			setup: func(t *testing.T, f *cleanupFixture) {
				f.fg.Spec.Backup.Profiles[0].Storage.S3.CredentialsSecret = ""
				if err := f.c.Update(context.Background(), f.fg); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: v1alpha1.ReasonCredentialsUnavailable,
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
			wantReason: v1alpha1.ReasonCleanupJobConflict,
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
	producer, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: f.fg, Profile: f.fg.Spec.Backup.Profiles[0], Backup: f.mb,
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

func TestFinalize_WaitsOutGarbageCollectorPropagation(t *testing.T) {
	for _, tc := range []struct {
		finalizer, reason string
	}{
		{metav1.FinalizerDeleteDependents, v1alpha1.ReasonForegroundDeletion},
		{metav1.FinalizerOrphanDependents, v1alpha1.ReasonOrphanDeletion},
	} {
		t.Run(tc.finalizer, func(t *testing.T) {
			f := newCleanupFixture(t)
			if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
				t.Fatal(err)
			}
			f.mb.Finalizers = append(f.mb.Finalizers, tc.finalizer)
			if err := f.c.Update(context.Background(), f.mb); err != nil {
				t.Fatal(err)
			}
			if done, requeue := f.finalize(t); done || requeue != 10*time.Second {
				t.Fatalf("done=%v requeue=%s", done, requeue)
			}
			if _, ok := f.cleanupJob(t); ok {
				t.Fatal("a cleanup Job was created while the garbage collector processes dependents")
			}
			if cond := cleanupCondition(t, f); cond == nil || cond.Reason != tc.reason {
				t.Fatalf("condition %+v, want %s", cond, tc.reason)
			}
			f.mb.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
			if err := f.c.Update(context.Background(), f.mb); err != nil {
				t.Fatal(err)
			}
			f.finalize(t)
			if job, ok := f.cleanupJob(t); !ok || !metav1.IsControlledBy(job, f.mb) {
				t.Fatal("cleanup did not start (owned) after the garbage collector finished")
			}
		})
	}
}

func TestFinalize_ReportsPodStuckCreating(t *testing.T) {
	f := newCleanupFixture(t)
	f.finalize(t)
	job, _ := f.cleanupJob(t)
	job.UID = "cleanup-creating"
	pod := cleanupPodFor(job, corev1.PodPending, 0, "")
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-3 * time.Minute))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: cleanupContainerName, State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
	}}}
	if err := f.c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	f.finalize(t)
	cond := cleanupCondition(t, f)
	if cond == nil || cond.Reason != v1alpha1.ReasonCleanupPending || !strings.Contains(cond.Message, "still ContainerCreating since ") ||
		!strings.Contains(cond.Message, "FailedMount") {
		t.Fatalf("condition %+v", cond)
	}
	// The message is stable: later passes do not rewrite status.
	rv := f.mb.ResourceVersion
	f.finalize(t)
	if f.mb.ResourceVersion != rv {
		t.Fatal("CleanupPending was rewritten on an unchanged pod")
	}
	// The pod starts: the condition is cleared.
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: cleanupContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := f.c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	f.finalize(t)
	if cond := cleanupCondition(t, f); cond != nil {
		t.Fatalf("stale CleanupPending after the pod started: %+v", cond)
	}
}

func TestFinalize_RestoresBlankedLocationFromSnapshot(t *testing.T) {
	ctx := context.Background()
	f := newCleanupFixture(t)
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Status.Location = ""
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if done, _ := f.finalize(t); done {
		t.Fatal("released the finalizer and orphaned an artifact whose destination is recorded")
	}
	if f.mb.Status.Location != "lion/art" {
		t.Fatalf("location not restored: %q", f.mb.Status.Location)
	}
	if job, ok := f.cleanupJob(t); !ok || envMap(job.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_OUTPUT_URL"] != "lion/art" {
		t.Fatal("cleanup did not target the recorded artifact")
	}
	if n := countEvents(f.rec, "ArtifactLocationRestored"); n != 1 {
		t.Fatalf("ArtifactLocationRestored events = %d", n)
	}

	// A failed backup's location is never restored: its leftovers (the
	// backup stamp, part of a dump) are cleaned up in abandoned mode at
	// the planned destination instead (TestFinalize_AbandonedBackupCleanup).
	failed := newCleanupFixture(t)
	if err := failed.c.Get(ctx, client.ObjectKeyFromObject(failed.mb), failed.mb); err != nil {
		t.Fatal(err)
	}
	failed.mb.Status.Phase, failed.mb.Status.Location = v1alpha1.BackupPhaseFailed, ""
	if err := failed.c.Status().Update(ctx, failed.mb); err != nil {
		t.Fatal(err)
	}
	if done, _ := failed.finalize(t); done {
		t.Fatal("a failed backup was released before its leftovers were cleaned up")
	}
	if failed.mb.Status.Location != "" {
		t.Fatalf("a failed backup acquired status.location %q", failed.mb.Status.Location)
	}
	if job, ok := failed.cleanupJob(t); !ok || envMap(job.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_CLEANUP_MODE"] != "abandoned" {
		t.Fatal("no abandoned-mode cleanup Job for the failed backup")
	}
}

func TestCleanupPodStuck(t *testing.T) {
	now := time.Now()
	pending := func(age time.Duration, mutate func(*corev1.Pod)) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", CreationTimestamp: metav1.NewTime(now.Add(-age))},
			Status: corev1.PodStatus{Phase: corev1.PodPending}}
		if mutate != nil {
			mutate(p)
		}
		return p
	}
	creating := func(p *corev1.Pod) {
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "c", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}
	}
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{name: "creating within the grace", pod: pending(30*time.Second, creating), want: ""},
		{name: "creating past the grace", pod: pending(2*time.Minute, creating), want: "still ContainerCreating since " + now.Add(-2*time.Minute).UTC().Format(time.RFC3339)},
		{name: "unschedulable", pod: pending(2*time.Minute, func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "volume node affinity conflict"}}
		}), want: ": Unschedulable: volume node affinity conflict"},
		{name: "image pull, immediately", pod: pending(time.Second, func(p *corev1.Pod) {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "c", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"}}}}
		}), want: "waiting: ErrImagePull"},
		{name: "running", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := cleanupPodStuck(tc.pod, now)
			if tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFinalize_StripsDeadHoldsKeepsLiveOnes(t *testing.T) {
	ctx := context.Background()
	f := newCleanupFixture(t)
	live := verifyCR("live", "lion", "nightly-s3")
	live.UID = "uid-live"
	live.Status.Phase = v1alpha1.VerificationPhaseRestoring
	if err := f.c.Create(ctx, live); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	// Cleanup is blocked, so the CR stays Terminating: the case where a
	// dead hold would pin the PITR cutoff indefinitely.
	f.mb.Status.ArtifactStorage = nil
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Annotations = map[string]string{
		v1alpha1.VerificationHoldAnnotationPrefix + "uid-gone": "gone",
		v1alpha1.VerificationHoldAnnotationPrefix + "uid-live": "live",
	}
	if err := f.c.Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if done, _ := f.finalize(t); done {
		t.Fatal("released a blocked cleanup")
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	if _, kept := f.mb.Annotations[v1alpha1.VerificationHoldAnnotationPrefix+"uid-gone"]; kept {
		t.Fatal("dead hold not removed")
	}
	if f.mb.Annotations[v1alpha1.VerificationHoldAnnotationPrefix+"uid-live"] != "live" {
		t.Fatal("live hold removed")
	}
	if !BackupHasVerificationHold(f.mb) {
		t.Fatal("PITR would no longer see the live hold")
	}
}

func TestRepairTerminalAttribution_BackfillsArtifactStorage(t *testing.T) {
	ctx := context.Background()
	f := newCleanupFixture(t)
	producer, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: f.fg, Profile: f.fg.Spec.Backup.Profiles[0], Backup: f.mb,
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
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	// A backup that succeeded before status.artifactStorage existed.
	f.mb.Status.ArtifactStorage = nil
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.repairTerminalAttribution(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	if s := f.mb.Status.ArtifactStorage; s == nil || s.Location != "lion/art" || s.Bucket != "bloodraven-backups" {
		t.Fatalf("artifactStorage not backfilled before deletion: %+v", s)
	}
}

func TestFinalize_ReportsContainerThatCannotStart(t *testing.T) {
	f := newCleanupFixture(t)
	f.finalize(t)
	job, _ := f.cleanupJob(t)
	job.UID = "cleanup-pending"
	pod := cleanupPodFor(job, corev1.PodPending, 0, "")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: cleanupContainerName, State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image"},
	}}}
	if err := f.c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if done, _ := f.finalize(t); done {
		t.Fatal("released while cleanup has not run")
	}
	cond := cleanupCondition(t, f)
	if cond == nil || cond.Reason != v1alpha1.ReasonCleanupPending || !strings.Contains(cond.Message, "ImagePullBackOff") {
		t.Fatalf("condition %+v", cond)
	}
}

func TestFinalize_LegacyCleanupJobName(t *testing.T) {
	ctx := context.Background()
	legacyJob := func(f *cleanupFixture, ownerUID types.UID) *batchv1.Job {
		return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: legacyCleanupJobName(f.mb.Name), Namespace: "ns", UID: "legacy-uid",
			Labels:          map[string]string{v1alpha1.MysqlBackupLabel: f.mb.Name, v1alpha1.ResourceLabel: "backup-cleanup"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "shipstream.io/v1alpha1", Kind: "MysqlBackup", Name: f.mb.Name, UID: ownerUID, Controller: boolPtr(true)}}},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-48 * time.Hour))}}}}
	}
	t.Run("own legacy Job is observed and replaced", func(t *testing.T) {
		f := newCleanupFixture(t)
		legacy := legacyJob(f, f.mb.UID)
		if err := f.c.Create(ctx, legacy); err != nil {
			t.Fatal(err)
		}
		if err := f.c.Create(ctx, cleanupPodFor(legacy, corev1.PodFailed, 2, "")); err != nil {
			t.Fatal(err)
		}
		f.finalize(t)
		var got batchv1.Job
		if err := f.c.Get(ctx, client.ObjectKeyFromObject(legacy), &got); err == nil {
			t.Fatal("the failed legacy cleanup Job was not replaced")
		}
		if _, ok := f.cleanupJob(t); ok {
			t.Fatal("replacement created in the same pass as the deletion")
		}
		f.finalize(t)
		if _, ok := f.cleanupJob(t); !ok {
			t.Fatal("replacement not created under the new name")
		}
	})
	t.Run("another backup's Job with the legacy name is ignored", func(t *testing.T) {
		f := newCleanupFixture(t)
		other := legacyJob(f, "uid-of-backup-art-cleanup")
		if err := f.c.Create(ctx, other); err != nil {
			t.Fatal(err)
		}
		f.finalize(t)
		if _, ok := f.cleanupJob(t); !ok {
			t.Fatal("cleanup did not start under the new name")
		}
		var got batchv1.Job
		if err := f.c.Get(ctx, client.ObjectKeyFromObject(other), &got); err != nil {
			t.Fatalf("another backup's Job was touched: %v", err)
		}
	})
}

func TestFinalize_FailureRecordedOnceFromStaleCache(t *testing.T) {
	f := newCleanupFixture(t)
	f.finalize(t)
	job, _ := f.cleanupJob(t)
	f.failCleanupJob(t, job, time.Minute, backupmeta.CleanupExitTransient, "")
	stale := job.DeepCopy() // what a lagging cache would still return
	counter := bmetrics.BackupArtifactCleanupFailuresTotal.WithLabelValues("lion", "nightly-s3", v1alpha1.ReasonCleanupFailed)
	before := testutil.ToFloat64(counter)
	f.finalize(t)
	if _, _, err := f.r.retryFailedCleanup(context.Background(), f.mb, stale); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("failure counted %v times from a stale cache, want 1", got)
	}
}

func TestReconcile_DoesNotAdoptAnotherBackupsJob(t *testing.T) {
	fg := fgWithBackup()
	mb := backupCR("art-cleanup", "lion", "nightly-s3")
	mb.UID = "uid-art-cleanup"
	mb.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
	mb.Labels = map[string]string{v1alpha1.FailoverGroupLabel: "lion", v1alpha1.BackupProfileLabel: "nightly-s3", labelManagedBy: managerName}
	mb.OwnerReferences = []metav1.OwnerReference{{APIVersion: "shipstream.io/v1alpha1", Kind: "MysqlFailoverGroup", Name: "lion", UID: "fg", Controller: boolPtr(true)}}
	// The legacy cleanup Job of backup "art" has this backup's Job name.
	foreign := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: backupJobName(mb.Name), Namespace: "ns",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "shipstream.io/v1alpha1", Kind: "MysqlBackup", Name: "art", UID: "uid-art", Controller: boolPtr(true)}}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	r, c := newBackupReconciler(t, fg, mb, foreign, dsnSecret())
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec
	for range 2 {
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)})
		if err != nil || res.RequeueAfter != 15*time.Second {
			t.Fatalf("res=%+v err=%v", res, err)
		}
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	ready := apiMeta.FindStatusCondition(mb.Status.Conditions, v1alpha1.ConditionReady)
	if mb.Status.Phase != v1alpha1.BackupPhasePending || ready == nil || ready.Reason != "BackupJobConflict" || mb.Status.JobName != "" {
		t.Fatalf("want Pending/BackupJobConflict without adopting the Job, got %+v", mb.Status)
	}
	if n := countEvents(rec, "BackupJobConflict"); n != 1 {
		t.Fatalf("BackupJobConflict events = %d, want 1 (on the transition)", n)
	}

	// The pending deadline applies: an orphaned Job that never goes away
	// fails the backup instead of leaving it pending forever.
	mb.CreationTimestamp = metav1.NewTime(time.Now().Add(-maxBackupPendingDuration - time.Minute))
	if _, err := r.pendBackup(context.Background(), mb, "BackupJobConflict", "still conflicting"); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	if mb.Status.Phase != v1alpha1.BackupPhaseFailed {
		t.Fatalf("phase %s, want Failed after the pending deadline", mb.Status.Phase)
	}
}

// F147: a succeeded backup whose status lost its source identity gets it
// back from the Job that pinned it, as the PITRBackupIdentityUnknown
// refusal promises.
func TestRepairTerminalAttribution_RestoresLostIdentity(t *testing.T) {
	ctx := context.Background()
	f := newCleanupFixture(t)
	producer, err := BuildBackupJob(BackupJobInputs{DumpKeySecretName: testDumpKeySecret, FailoverGroup: f.fg, Profile: f.fg.Spec.Backup.Profiles[0], Backup: f.mb,
		SourceSite: "pdx", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(f.mb, producer, f.r.Scheme); err != nil {
		t.Fatal(err)
	}
	producer.Annotations[v1alpha1.BackupJobFailoverGroupUIDAnnotation] = "fg-uid"
	producer.Annotations[v1alpha1.BackupJobPITRArchiveIDAnnotation] = "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa"
	producer.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := f.c.Create(ctx, producer); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Status.FailoverGroupUID, f.mb.Status.PITRArchiveID = "", ""
	if err := f.c.Status().Update(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.repairTerminalAttribution(ctx, f.mb); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	if f.mb.Status.FailoverGroupUID != "fg-uid" || f.mb.Status.PITRArchiveID != "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("identity not restored: uid=%q archive=%q", f.mb.Status.FailoverGroupUID, f.mb.Status.PITRArchiveID)
	}
}
