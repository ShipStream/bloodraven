package controller

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"sort"
	"text/template"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// Backup and restore overview
//
// When spec.backup is configured, the operator creates a CronJob named
// "mysql-<fg>-backup" that on schedule runs a Job targeting the replica site's
// MySQL Service. The Job runs an embedded shell script (backupScript) that:
//
//  1. Runs xtrabackup (streaming via xbstream+gzip) or mysqldump (piped through
//     gzip) and uploads the stream directly to S3 at
//     s3://<bucket>/<prefix>/<timestamp>.<ext>
//  2. Writes a metadata sidecar at <timestamp>.meta.json describing the backup
//     method, duration, and GTID_EXECUTED at backup time.
//  3. Enforces retention: deletes backup objects that exceed the configured
//     count or age threshold.
//
// The backup target site is re-evaluated on every reconcile from
// fg.Status.ActiveSite, so after a failover the CronJob's Job template is
// updated to point at the new replica. Running Jobs are unaffected by template
// changes. ConcurrencyPolicy: Forbid prevents overlapping runs.
//
// # Restore procedure (manual)
//
// Bloodraven does not automate full database restore. The operator supports
// restore as a documented manual procedure:
//
//   xtrabackup:
//     1. kubectl scale deployment/mysql-<fg>-<site> --replicas=0
//     2. kubectl delete pvc mysql-<fg>-<site>-data
//     3. Apply a restore Job (see docs) that:
//          - Creates a fresh PVC with the same name
//          - aws s3 cp s3://<bucket>/<prefix>/<ts>.xbstream.gz - | gunzip | xbstream -x -C /restore
//          - xtrabackup --prepare --target-dir=/restore
//          - xtrabackup --move-back --target-dir=/restore --datadir=/var/lib/mysql
//     4. kubectl scale deployment/mysql-<fg>-<site> --replicas=1
//     5. Replication re-establishes via GTID auto-positioning.
//
//   mysqldump:
//     1. kubectl scale deployment/mysql-<fg>-<site> --replicas=0
//     2. kubectl delete pvc mysql-<fg>-<site>-data
//     3. kubectl scale deployment/mysql-<fg>-<site> --replicas=1 (empty datadir)
//     4. aws s3 cp s3://<bucket>/<prefix>/<ts>.sql.gz - | gunzip |
//          mysql -h mysql-<fg>-<site> -u root -p<pw>
//     5. Replication re-establishes via GTID auto-positioning.
//
// After either restore, verify GTID_EXECUTED alignment with the primary before
// accepting traffic.

const (
	labelComponent = "shipstream.io/component"
	componentBackup = "backup"

	backupContainerName = "backup"
	// defaultXtrabackupImage is used when spec.backup.image is empty and
	// method is xtrabackup. The image must contain xtrabackup, awscli, and gzip.
	defaultXtrabackupImage = "percona/percona-xtrabackup:8.0"
)

//go:embed backup_script.sh
var backupScript string

// backupCronJobName returns the CronJob name for a failover group.
func backupCronJobName(fgName string) string {
	return fmt.Sprintf("mysql-%s-backup", fgName)
}

// reconcileBackup orchestrates the backup CronJob and status update.
// It is a no-op when spec.backup is nil; any previously-created CronJob is
// cleaned up in that case.
func (r *MysqlFailoverGroupReconciler) reconcileBackup(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if fg.Spec.Backup == nil {
		return r.cleanupBackupResources(ctx, fg)
	}

	if fg.Spec.Backup.Storage.S3 == nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "BackupConfigInvalid",
			"spec.backup.storage.s3 must be set")
		return nil
	}
	if fg.Spec.Backup.PITR {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "BackupPITRNotImplemented",
			"spec.backup.pitr is not yet implemented; binlog archiving will not run")
	}

	if err := r.reconcileBackupCronJob(ctx, fg); err != nil {
		return fmt.Errorf("reconcile backup cronjob: %w", err)
	}
	if err := r.reconcileBackupStatus(ctx, fg); err != nil {
		return fmt.Errorf("reconcile backup status: %w", err)
	}
	return nil
}

// cleanupBackupResources removes the backup CronJob when spec.backup is unset.
func (r *MysqlFailoverGroupReconciler) cleanupBackupResources(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupCronJobName(fg.Name),
			Namespace: fg.Namespace,
		},
	}
	if err := r.Delete(ctx, cj); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete backup cronjob: %w", err)
	}
	return nil
}

// replicaSite returns the site to target for backups. It prefers the
// non-active site (as reported in status). If status is not yet populated,
// it falls back to sites[1] so the first backup runs against a stable site.
func replicaSite(fg *v1alpha1.MysqlFailoverGroup) v1alpha1.SiteSpec {
	if fg.Status.ActiveSite != "" && fg.Status.ActiveSite == fg.Spec.Sites[0].Name {
		return fg.Spec.Sites[1]
	}
	if fg.Status.ActiveSite != "" && fg.Status.ActiveSite == fg.Spec.Sites[1].Name {
		return fg.Spec.Sites[0]
	}
	// No known active site yet — default to sites[1].
	return fg.Spec.Sites[1]
}

// renderBackupPrefix renders the S3 prefix Go template. An empty template
// yields "<namespace>/<fg>/<site>".
func renderBackupPrefix(tmpl string, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) (string, error) {
	if tmpl == "" {
		return fmt.Sprintf("%s/%s/%s", fg.Namespace, fg.Name, site.Name), nil
	}
	t, err := template.New("prefix").Option("missingkey=error").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parse prefix template: %w", err)
	}
	var buf bytes.Buffer
	data := struct {
		FailoverGroup string
		Site          string
		Namespace     string
	}{
		FailoverGroup: fg.Name,
		Site:          site.Name,
		Namespace:     fg.Namespace,
	}
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute prefix template: %w", err)
	}
	return buf.String(), nil
}

// backupImage resolves the container image for the backup Job.
func backupImage(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.Backup.Image != "" {
		return fg.Spec.Backup.Image
	}
	if fg.Spec.Backup.Method == "mysqldump" {
		// Fall back to the MySQL image which ships with mysqldump.
		image := fg.Spec.Image
		if image == "" {
			image = defaultMySQLImage
		}
		return image
	}
	return defaultXtrabackupImage
}

// backupMethod returns the configured method or the default (xtrabackup).
func backupMethod(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.Backup.Method == "" {
		return "xtrabackup"
	}
	return fg.Spec.Backup.Method
}

func (r *MysqlFailoverGroupReconciler) reconcileBackupCronJob(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	target := replicaSite(fg)
	s3 := fg.Spec.Backup.Storage.S3

	prefix, err := renderBackupPrefix(s3.Prefix, fg, target)
	if err != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "BackupPrefixInvalid",
			"prefix template error: %v", err)
		return err
	}

	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupCronJobName(fg.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cj, func() error {
		if err := controllerutil.SetControllerReference(fg, cj, r.Scheme); err != nil {
			return err
		}
		labels := commonLabels(fg.Name, target.Name)
		labels[labelComponent] = componentBackup
		cj.Labels = labels

		concurrency := batchv1.ForbidConcurrent
		successLimit := int32(3)
		failureLimit := int32(3)
		startingDeadline := int64(300)
		backoffLimit := int32(1)

		cj.Spec = batchv1.CronJobSpec{
			Schedule:                   fg.Spec.Backup.Schedule,
			ConcurrencyPolicy:          concurrency,
			SuccessfulJobsHistoryLimit: &successLimit,
			FailedJobsHistoryLimit:     &failureLimit,
			StartingDeadlineSeconds:    &startingDeadline,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: batchv1.JobSpec{
					BackoffLimit: &backoffLimit,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: labels,
						},
						Spec: backupPodSpec(fg, target, prefix),
					},
				},
			},
		}
		return nil
	})
	return err
}

// backupPodSpec builds the pod spec for a backup Job.
func backupPodSpec(fg *v1alpha1.MysqlFailoverGroup, target v1alpha1.SiteSpec, prefix string) corev1.PodSpec {
	s3 := fg.Spec.Backup.Storage.S3

	envFrom := []corev1.EnvFromSource{
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: s3.SecretName},
			},
		},
	}

	env := []corev1.EnvVar{
		{Name: "MYSQL_HOST", Value: fmt.Sprintf("%s.%s.svc.cluster.local",
			resourceName(fg.Name, target.Name), fg.Namespace)},
		{Name: "MYSQL_PORT", Value: fmt.Sprintf("%d", mysqlPort)},
		{Name: "MYSQL_DSN", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: fg.Spec.SecretName},
				Key:                  "dsn",
			},
		}},
		{Name: "BACKUP_METHOD", Value: backupMethod(fg)},
		{Name: "S3_BUCKET", Value: s3.Bucket},
		{Name: "S3_PREFIX", Value: prefix},
		{Name: "FAILOVER_GROUP", Value: fg.Name},
		{Name: "SITE", Value: target.Name},
	}
	if s3.Endpoint != "" {
		env = append(env, corev1.EnvVar{Name: "S3_ENDPOINT", Value: s3.Endpoint})
	}
	if s3.Region != "" {
		env = append(env, corev1.EnvVar{Name: "AWS_DEFAULT_REGION", Value: s3.Region})
	}
	if fg.Spec.Backup.Retention != nil {
		env = append(env,
			corev1.EnvVar{Name: "RETENTION_COUNT", Value: fmt.Sprintf("%d", fg.Spec.Backup.Retention.Count)},
			corev1.EnvVar{Name: "RETENTION_DAYS", Value: fmt.Sprintf("%d", fg.Spec.Backup.Retention.Days)},
		)
	}

	return corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{{
			Name:      backupContainerName,
			Image:     backupImage(fg),
			Command:   []string{"/bin/sh", "-c", backupScript},
			Env:       env,
			EnvFrom:   envFrom,
			Resources: fg.Spec.Backup.Resources,
		}},
	}
}

// reconcileBackupStatus reads the most recent Job created by the CronJob and
// updates fg.Status.Backup accordingly. It also emits Prometheus metrics for
// completed backups.
func (r *MysqlFailoverGroupReconciler) reconcileBackupStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	logger := log.FromContext(ctx)

	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{
			labelInstance:  fg.Name,
			labelComponent: componentBackup,
		},
	); err != nil {
		return fmt.Errorf("list backup jobs: %w", err)
	}
	if len(jobs.Items) == 0 {
		return nil
	}

	// Find the most recent Job by creation timestamp.
	sort.Slice(jobs.Items, func(i, j int) bool {
		return jobs.Items[i].CreationTimestamp.After(jobs.Items[j].CreationTimestamp.Time)
	})
	latest := jobs.Items[0]

	newStatus := backupStatusFromJob(&latest, fg)
	if newStatus == nil {
		return nil
	}

	if fg.Status.Backup != nil && equality.Semantic.DeepEqual(fg.Status.Backup, newStatus) {
		return nil
	}

	// Emit metrics (idempotent for completed Jobs — gauges and counters are
	// label-scoped; re-setting is safe and counter increments are guarded by
	// the semantic-equality check above).
	emitBackupMetrics(fg, &latest, newStatus)

	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &fresh); err != nil {
			return err
		}
		if fresh.Status.Backup != nil && equality.Semantic.DeepEqual(fresh.Status.Backup, newStatus) {
			return nil
		}
		fresh.Status.Backup = newStatus
		if err := r.Status().Update(ctx, &fresh); err != nil {
			logger.Error(err, "update backup status")
			return err
		}
		// Mirror into caller's copy so subsequent reconcile steps see the update.
		fg.Status.Backup = newStatus
		return nil
	})
}

// backupStatusFromJob derives a BackupStatus from a backup Job.
func backupStatusFromJob(job *batchv1.Job, fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.BackupStatus {
	status := &v1alpha1.BackupStatus{}
	if fg.Status.Backup != nil {
		// Preserve LastSuccessfulBackup across failures.
		status.LastSuccessfulBackup = fg.Status.Backup.LastSuccessfulBackup
	}

	// Find site from Job labels.
	status.LastBackupSite = job.Labels[labelSite]

	completed, failed := jobConditions(job)

	switch {
	case completed:
		end := job.Status.CompletionTime
		start := job.Status.StartTime
		if end != nil {
			status.LastBackupTime = end.DeepCopy()
			status.LastSuccessfulBackup = end.DeepCopy()
		}
		if end != nil && start != nil {
			dur := int64(end.Sub(start.Time).Seconds())
			status.LastBackupDurationSeconds = &dur
		}
		status.LastBackupResult = "Success"
		status.LastBackupMessage = fmt.Sprintf("backup job %s completed", job.Name)
	case failed:
		if job.Status.CompletionTime != nil {
			status.LastBackupTime = job.Status.CompletionTime.DeepCopy()
		} else {
			now := metav1.NewTime(time.Now())
			status.LastBackupTime = &now
		}
		status.LastBackupResult = "Failure"
		status.LastBackupMessage = jobFailureMessage(job)
	default:
		if job.Status.StartTime != nil {
			status.LastBackupTime = job.Status.StartTime.DeepCopy()
		}
		status.LastBackupResult = "InProgress"
		status.LastBackupMessage = fmt.Sprintf("backup job %s in progress", job.Name)
	}

	return status
}

// jobConditions returns whether the Job is Complete or Failed.
func jobConditions(job *batchv1.Job) (complete, failed bool) {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete, batchv1.JobSuccessCriteriaMet:
			complete = true
		case batchv1.JobFailed:
			failed = true
		}
	}
	return complete, failed
}

func jobFailureMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			if c.Message != "" {
				return c.Message
			}
			return c.Reason
		}
	}
	return "backup job failed"
}

// emitBackupMetrics updates Prometheus metrics based on the latest Job state.
func emitBackupMetrics(fg *v1alpha1.MysqlFailoverGroup, job *batchv1.Job, status *v1alpha1.BackupStatus) {
	labels := map[string]string{"failover_group": fg.Name}

	if status.LastBackupTime != nil {
		metrics.BackupLastAttemptTimestamp.With(labels).Set(float64(status.LastBackupTime.Unix()))
	}

	switch status.LastBackupResult {
	case "Success":
		metrics.BackupTotal.WithLabelValues(fg.Name, "success").Inc()
		if status.LastSuccessfulBackup != nil {
			metrics.BackupLastSuccessTimestamp.With(labels).Set(float64(status.LastSuccessfulBackup.Unix()))
		}
		if status.LastBackupDurationSeconds != nil {
			metrics.BackupDurationSeconds.WithLabelValues(fg.Name, backupMethod(fg)).
				Observe(float64(*status.LastBackupDurationSeconds))
		}
	case "Failure":
		metrics.BackupTotal.WithLabelValues(fg.Name, "failure").Inc()
	}
	_ = job
}
