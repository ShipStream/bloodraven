package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func withBackup(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlFailoverGroup {
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		Schedule: "0 */6 * * *",
		Method:   "xtrabackup",
		Storage: v1alpha1.BackupStorageSpec{
			S3: &v1alpha1.S3StorageSpec{
				Bucket:     "shipstream-backups",
				Prefix:     "{{ .FailoverGroup }}/{{ .Site }}",
				SecretName: "s3-credentials",
			},
		},
		Retention: &v1alpha1.BackupRetentionSpec{
			Count: 28,
			Days:  30,
		},
	}
	return fg
}

func TestReconcile_CreatesBackupCronJob(t *testing.T) {
	fg := withBackup(newTestFG())
	r, c := newReconciler(fg)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-backup", Namespace: "shared-lion",
	}, &cj); err != nil {
		t.Fatalf("backup cronjob not created: %v", err)
	}

	if cj.Spec.Schedule != "0 */6 * * *" {
		t.Errorf("expected schedule %q, got %q", "0 */6 * * *", cj.Spec.Schedule)
	}
	if cj.Spec.ConcurrencyPolicy != batchv1.ForbidConcurrent {
		t.Errorf("expected ConcurrencyPolicy=Forbid, got %s", cj.Spec.ConcurrencyPolicy)
	}
	if cj.Labels[labelComponent] != componentBackup {
		t.Errorf("expected component=backup label, got %q", cj.Labels[labelComponent])
	}

	// Verify the Job template has the expected env vars and container image.
	containers := cj.Spec.JobTemplate.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("expected 1 backup container, got %d", len(containers))
	}
	c0 := containers[0]
	if c0.Image != defaultXtrabackupImage {
		t.Errorf("expected xtrabackup default image, got %q", c0.Image)
	}

	envMap := map[string]string{}
	for _, e := range c0.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["S3_BUCKET"] != "shipstream-backups" {
		t.Errorf("S3_BUCKET env: got %q", envMap["S3_BUCKET"])
	}
	if envMap["S3_PREFIX"] != "lion/dc2" {
		t.Errorf("rendered prefix: got %q, want lion/dc2", envMap["S3_PREFIX"])
	}
	if envMap["RETENTION_COUNT"] != "28" {
		t.Errorf("RETENTION_COUNT env: got %q", envMap["RETENTION_COUNT"])
	}
	if envMap["MYSQL_HOST"] != "mysql-lion-dc2.shared-lion.svc.cluster.local" {
		t.Errorf("MYSQL_HOST env: got %q", envMap["MYSQL_HOST"])
	}
}

func TestReconcile_BackupTargetsReplicaSite(t *testing.T) {
	fg := withBackup(newTestFG())
	// Simulate dc2 as active — backup should then target dc1.
	fg.Status.ActiveSite = "dc2"
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "dc1", State: "read-only"},
		{Name: "dc2", State: "writable"},
	}
	r, c := newReconciler(fg)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-backup", Namespace: "shared-lion",
	}, &cj); err != nil {
		t.Fatalf("backup cronjob not created: %v", err)
	}

	var mysqlHost string
	for _, e := range cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "MYSQL_HOST" {
			mysqlHost = e.Value
		}
	}
	if mysqlHost != "mysql-lion-dc1.shared-lion.svc.cluster.local" {
		t.Errorf("expected backup to target dc1 replica, got MYSQL_HOST=%q", mysqlHost)
	}
	if cj.Labels[labelSite] != "dc1" {
		t.Errorf("expected site label dc1, got %q", cj.Labels[labelSite])
	}
}

func TestReconcile_BackupCleanupWhenRemoved(t *testing.T) {
	fg := withBackup(newTestFG())
	r, c := newReconciler(fg)

	// First reconcile creates the CronJob.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-backup", Namespace: "shared-lion",
	}, &cj); err != nil {
		t.Fatalf("cronjob not created on first reconcile: %v", err)
	}

	// Now clear backup spec and reconcile again.
	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion", Namespace: "shared-lion"}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	fresh.Spec.Backup = nil
	if err := c.Update(context.Background(), &fresh); err != nil {
		t.Fatalf("clear backup: %v", err)
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-backup", Namespace: "shared-lion",
	}, &cj)
	if err == nil {
		t.Fatal("expected cronjob to be deleted, still exists")
	}
	if !errors.IsNotFound(err) {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestReconcile_BackupInvalidS3Config(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		Schedule: "0 */6 * * *",
		Storage:  v1alpha1.BackupStorageSpec{S3: nil}, // missing
	}
	r, _ := newReconciler(fg)

	// Should not return an error — just record an event.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("reconcile should tolerate invalid backup config, got: %v", err)
	}
}

func TestRenderBackupPrefix(t *testing.T) {
	fg := newTestFG()
	site := fg.Spec.Sites[1]

	cases := []struct {
		name    string
		tmpl    string
		want    string
		wantErr bool
	}{
		{"empty", "", "shared-lion/lion/dc2", false},
		{"simple", "{{ .FailoverGroup }}/{{ .Site }}", "lion/dc2", false},
		{"with-ns", "{{ .Namespace }}/{{ .FailoverGroup }}", "shared-lion/lion", false},
		{"bad-template", "{{ .Unknown }}", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderBackupPrefix(tc.tmpl, fg, site)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got prefix=%q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReplicaSite(t *testing.T) {
	fg := newTestFG()

	// No active site yet — defaults to sites[1].
	if got := replicaSite(fg).Name; got != "dc2" {
		t.Errorf("no active site: got %q, want dc2", got)
	}

	fg.Status.ActiveSite = "dc1"
	if got := replicaSite(fg).Name; got != "dc2" {
		t.Errorf("active dc1: got %q, want dc2", got)
	}

	fg.Status.ActiveSite = "dc2"
	if got := replicaSite(fg).Name; got != "dc1" {
		t.Errorf("active dc2: got %q, want dc1", got)
	}
}

func TestBackupImage(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Backup = &v1alpha1.BackupSpec{Method: "xtrabackup"}
	if got := backupImage(fg); got != defaultXtrabackupImage {
		t.Errorf("xtrabackup default: got %q", got)
	}

	fg.Spec.Backup.Image = "custom/xtrabackup:1.2.3"
	if got := backupImage(fg); got != "custom/xtrabackup:1.2.3" {
		t.Errorf("custom image: got %q", got)
	}

	fg.Spec.Backup.Image = ""
	fg.Spec.Backup.Method = "mysqldump"
	if got := backupImage(fg); got != "mysql:9.6" {
		t.Errorf("mysqldump falls back to spec.image: got %q", got)
	}
}

func TestBackupStatusFromJob_Success(t *testing.T) {
	fg := newTestFG()
	start := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	end := metav1.NewTime(time.Now())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "mysql-lion-backup-x1",
			Labels: map[string]string{labelSite: "dc2"},
		},
		Status: batchv1.JobStatus{
			StartTime:      &start,
			CompletionTime: &end,
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
		},
	}

	got := backupStatusFromJob(job, fg)
	if got.LastBackupResult != "Success" {
		t.Errorf("expected Success, got %q", got.LastBackupResult)
	}
	if got.LastBackupSite != "dc2" {
		t.Errorf("expected site dc2, got %q", got.LastBackupSite)
	}
	if got.LastBackupDurationSeconds == nil || *got.LastBackupDurationSeconds < 290 || *got.LastBackupDurationSeconds > 310 {
		t.Errorf("unexpected duration: %v", got.LastBackupDurationSeconds)
	}
	if got.LastSuccessfulBackup == nil {
		t.Error("expected LastSuccessfulBackup to be set")
	}
}

func TestBackupStatusFromJob_Failure(t *testing.T) {
	prev := metav1.NewTime(time.Now().Add(-24 * time.Hour))
	fg := newTestFG()
	fg.Status.Backup = &v1alpha1.BackupStatus{
		LastSuccessfulBackup: &prev,
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelSite: "dc2"}},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type:    batchv1.JobFailed,
					Status:  corev1.ConditionTrue,
					Message: "backoff limit exceeded",
				},
			},
		},
	}

	got := backupStatusFromJob(job, fg)
	if got.LastBackupResult != "Failure" {
		t.Errorf("expected Failure, got %q", got.LastBackupResult)
	}
	if got.LastBackupMessage != "backoff limit exceeded" {
		t.Errorf("unexpected message: %q", got.LastBackupMessage)
	}
	// A failure must not clobber the existing LastSuccessfulBackup.
	if got.LastSuccessfulBackup == nil || !got.LastSuccessfulBackup.Equal(&prev) {
		t.Errorf("failure should preserve LastSuccessfulBackup")
	}
}

func TestReconcileBackupStatus_UpdatesFromJob(t *testing.T) {
	fg := withBackup(newTestFG())
	fg.Status.ActiveSite = "dc1" // replica = dc2
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "dc1", State: "writable"},
		{Name: "dc2", State: "read-only"},
	}

	// Pre-seed a completed backup Job that belongs to the CronJob.
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	end := metav1.NewTime(time.Now())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mysql-lion-backup-1",
			Namespace: "shared-lion",
			Labels: map[string]string{
				labelAppName:       "mysql",
				labelInstance:      "lion",
				labelFailoverGroup: "lion",
				labelSite:          "dc2",
				labelManagedBy:     managerName,
				labelComponent:     componentBackup,
			},
			CreationTimestamp: start,
		},
		Status: batchv1.JobStatus{
			StartTime:      &start,
			CompletionTime: &end,
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
		},
	}

	r, c := newReconciler(fg, job)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "lion", Namespace: "shared-lion"}, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Backup == nil {
		t.Fatal("expected fg.Status.Backup to be populated")
	}
	if got.Status.Backup.LastBackupResult != "Success" {
		t.Errorf("expected Success, got %q", got.Status.Backup.LastBackupResult)
	}
	if got.Status.Backup.LastBackupSite != "dc2" {
		t.Errorf("expected site dc2, got %q", got.Status.Backup.LastBackupSite)
	}
}

// Ensure that batchv1 is registered in the test scheme via clientgoscheme.
func TestBackupCronJobOwnerReference(t *testing.T) {
	fg := withBackup(newTestFG())
	r, c := newReconciler(fg)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-backup", Namespace: "shared-lion",
	}, &cj); err != nil {
		t.Fatalf("get cronjob: %v", err)
	}

	if len(cj.OwnerReferences) == 0 {
		t.Fatal("cronjob should have owner reference to MysqlFailoverGroup")
	}
	if cj.OwnerReferences[0].Name != "lion" || cj.OwnerReferences[0].Kind != "MysqlFailoverGroup" {
		t.Errorf("unexpected owner: %+v", cj.OwnerReferences[0])
	}
}

