package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// operatorImageFromEnv is overridden by the main reconciler constructor
// so the scheduled CronJobs can run the same operator image with the
// `bloodraven trigger-backup` subcommand. Left as a package variable so
// the ClusterRole/ServiceAccount already granted to the operator are
// reused.
var operatorImageFromEnv = ""

// operatorServiceAccountFromEnv is the ServiceAccount used by schedule
// CronJobs. Defaults to the operator's own ServiceAccount.
var operatorServiceAccountFromEnv = ""

// SetOperatorImageDefaults is called from cmd/bloodraven/main.go so the
// schedule reconciler knows what image and ServiceAccount to run inside
// the scheduled CronJob pods. It is a package-level setter to keep the
// reconciler struct stable.
func SetOperatorImageDefaults(image, serviceAccount string) {
	operatorImageFromEnv = image
	operatorServiceAccountFromEnv = serviceAccount
}

// reconcileBackupAssets provisions the shared scripts ConfigMap and any
// operator-managed backup PVCs referenced by PVC-backed profiles. It
// runs on every MysqlFailoverGroup reconcile and is a no-op when
// spec.backup is nil.
func (r *MysqlFailoverGroupReconciler) reconcileBackupAssets(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if fg.Spec.Backup == nil {
		return nil
	}

	if err := r.reconcileBackupScriptsConfigMap(ctx, fg); err != nil {
		return fmt.Errorf("reconcile backup scripts configmap: %w", err)
	}

	for _, profile := range fg.Spec.Backup.Profiles {
		if profile.Storage.Type != v1alpha1.BackupStoragePVC || profile.Storage.PVC == nil {
			continue
		}
		if profile.Storage.PVC.ClaimName != "" {
			// User-managed PVC; nothing to do.
			continue
		}
		if err := r.reconcileOwnedBackupPVC(ctx, fg, profile); err != nil {
			return fmt.Errorf("reconcile backup pvc for profile %s: %w", profile.Name, err)
		}
	}

	return nil
}

// reconcileBackupScriptsConfigMap owns a single per-group ConfigMap that
// holds the embedded dump.py / restore.py scripts. Backup and restore
// Jobs mount this ConfigMap read-only at /scripts.
func (r *MysqlFailoverGroupReconciler) reconcileBackupScriptsConfigMap(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupScriptsConfigMapName(fg.Name),
			Namespace: fg.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if err := controllerutil.SetControllerReference(fg, cm, r.Scheme); err != nil {
			return err
		}
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels[labelAppName] = "mysql-backup"
		cm.Labels[labelInstance] = fg.Name
		cm.Labels[labelFailoverGroup] = fg.Name
		cm.Labels[labelManagedBy] = managerName
		cm.Data = map[string]string{
			"dump.py":    BackupDumpScript(),
			"restore.py": BackupRestoreScript(),
		}
		return nil
	})
	return err
}

// reconcileOwnedBackupPVC provisions a PVC for a PVC-backed profile when
// the user did not supply claimName.
func (r *MysqlFailoverGroupReconciler) reconcileOwnedBackupPVC(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, profile v1alpha1.BackupProfile) error {
	if profile.Storage.PVC == nil {
		return nil
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ownedBackupPVCName(fg.Name, profile.Name),
			Namespace: fg.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		if err := controllerutil.SetControllerReference(fg, pvc, r.Scheme); err != nil {
			return err
		}
		if pvc.Labels == nil {
			pvc.Labels = map[string]string{}
		}
		pvc.Labels[labelAppName] = "mysql-backup"
		pvc.Labels[labelInstance] = fg.Name
		pvc.Labels[labelFailoverGroup] = fg.Name
		pvc.Labels[labelBackupProfile] = profile.Name
		pvc.Labels[labelManagedBy] = managerName

		// PVC spec is largely immutable — only set on creation.
		if pvc.CreationTimestamp.IsZero() {
			storageSize := profile.Storage.PVC.Size
			if storageSize.IsZero() {
				storageSize = resource.MustParse("10Gi")
			}
			pvc.Spec = corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storageSize,
					},
				},
			}
			if profile.Storage.PVC.StorageClassName != "" {
				sc := profile.Storage.PVC.StorageClassName
				pvc.Spec.StorageClassName = &sc
			}
		}
		return nil
	})
	return err
}

// reconcileBackupSchedules materializes one CronJob per entry in
// spec.backup.schedules[] and prunes orphaned CronJobs that belonged to
// schedules the user removed.
func (r *MysqlFailoverGroupReconciler) reconcileBackupSchedules(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	desired := map[string]struct{}{}

	if fg.Spec.Backup != nil {
		for _, sched := range fg.Spec.Backup.Schedules {
			name := scheduleCronJobName(fg.Name, sched.Name)
			desired[name] = struct{}{}
			if err := r.reconcileOneSchedule(ctx, fg, sched); err != nil {
				return fmt.Errorf("reconcile schedule %s: %w", sched.Name, err)
			}
		}
	}

	// Prune orphan CronJobs owned by this group but no longer desired.
	var existing batchv1.CronJobList
	if err := r.List(ctx, &existing,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{
			labelFailoverGroup: fg.Name,
			labelResourceKind:  "backup-schedule",
		},
	); err != nil {
		return fmt.Errorf("list cronjobs: %w", err)
	}
	for i := range existing.Items {
		cj := &existing.Items[i]
		if _, ok := desired[cj.Name]; ok {
			continue
		}
		if err := r.Delete(ctx, cj); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete orphan cronjob %s: %w", cj.Name, err)
		}
	}

	return nil
}

// reconcileOneSchedule creates or updates a single CronJob for a
// schedule entry. The CronJob's pod runs `bloodraven trigger-backup`
// against the operator's own ServiceAccount to create a MysqlBackup CR.
func (r *MysqlFailoverGroupReconciler) reconcileOneSchedule(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sched v1alpha1.BackupSchedule) error {
	// Validate the profile exists. If it doesn't, still create the
	// CronJob but emit a warning — the scheduled pod will fail fast
	// and make the misconfiguration visible via MysqlBackup phase.
	if findProfile(fg, sched.ProfileName) == nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "BackupScheduleInvalid",
			"schedule %q references unknown profile %q", sched.Name, sched.ProfileName)
	}

	image := operatorImageFromEnv
	if image == "" {
		// Falls back to the MySQL image so the build succeeds, but the
		// operator should always set this in main.go.
		image = "bloodraven:latest"
	}
	sa := operatorServiceAccountFromEnv

	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleCronJobName(fg.Name, sched.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cj, func() error {
		if err := controllerutil.SetControllerReference(fg, cj, r.Scheme); err != nil {
			return err
		}

		labels := map[string]string{
			labelAppName:        "mysql-backup",
			labelInstance:       fg.Name,
			labelFailoverGroup:  fg.Name,
			labelBackupProfile:  sched.ProfileName,
			labelBackupSchedule: sched.Name,
			labelResourceKind:   "backup-schedule",
			labelManagedBy:      managerName,
		}
		cj.Labels = labels

		concurrency := batchv1.ForbidConcurrent
		switch sched.ConcurrencyPolicy {
		case "Allow":
			concurrency = batchv1.AllowConcurrent
		case "Replace":
			concurrency = batchv1.ReplaceConcurrent
		case "Forbid", "":
			concurrency = batchv1.ForbidConcurrent
		}

		cj.Spec.Schedule = sched.Schedule
		cj.Spec.Suspend = boolPtr(sched.Suspend)
		cj.Spec.ConcurrencyPolicy = concurrency
		cj.Spec.StartingDeadlineSeconds = sched.StartingDeadlineSeconds
		cj.Spec.SuccessfulJobsHistoryLimit = sched.SuccessfulJobsHistoryLimit
		cj.Spec.FailedJobsHistoryLimit = sched.FailedJobsHistoryLimit

		backoff := int32(2)
		cj.Spec.JobTemplate = batchv1.JobTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: batchv1.JobSpec{
				BackoffLimit: &backoff,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						RestartPolicy:      corev1.RestartPolicyOnFailure,
						ServiceAccountName: sa,
						Containers: []corev1.Container{
							{
								Name:  "trigger",
								Image: image,
								Command: []string{
									"/bloodraven",
									"trigger-backup",
									"--group=" + fg.Name,
									"--profile=" + sched.ProfileName,
									"--schedule=" + sched.Name,
									"--namespace=" + fg.Namespace,
								},
							},
						},
					},
				},
			},
		}
		return nil
	})
	return err
}

// updateBackupStatus lists MysqlBackup CRs for this failover group and
// rolls the most recent activity per schedule into
// fg.status.backupSchedules[] and fg.status.lastBackupTime.
func (r *MysqlFailoverGroupReconciler) updateBackupStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if fg.Spec.Backup == nil || len(fg.Spec.Backup.Schedules) == 0 {
		// Still clear the rollup if nothing is scheduled.
		if fg.Status.BackupSchedules == nil {
			return nil
		}
	}

	var list v1alpha1.MysqlBackupList
	if err := r.List(ctx, &list,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{labelFailoverGroup: fg.Name},
	); err != nil {
		return fmt.Errorf("list backups: %w", err)
	}

	// Most-recent-per-schedule rollup (by start time).
	type rollup struct {
		latest    v1alpha1.MysqlBackup
		latestSet bool
		lastOK    *metav1.Time
	}
	bySched := map[string]*rollup{}

	for i := range list.Items {
		b := &list.Items[i]
		sched := b.Labels[labelBackupSchedule]
		if sched == "" {
			continue
		}
		ru, ok := bySched[sched]
		if !ok {
			ru = &rollup{}
			bySched[sched] = ru
		}
		if !ru.latestSet || startAfter(b, &ru.latest) {
			ru.latest = *b
			ru.latestSet = true
		}
		if b.Status.Phase == v1alpha1.BackupPhaseSucceeded && b.Status.CompletionTime != nil {
			if ru.lastOK == nil || b.Status.CompletionTime.After(ru.lastOK.Time) {
				ru.lastOK = b.Status.CompletionTime
			}
		}
	}

	var schedules []v1alpha1.BackupScheduleStatus
	var overallLastOK *metav1.Time
	if fg.Spec.Backup != nil {
		for _, s := range fg.Spec.Backup.Schedules {
			entry := v1alpha1.BackupScheduleStatus{
				Name:        s.Name,
				CronJobName: scheduleCronJobName(fg.Name, s.Name),
			}
			if ru, ok := bySched[s.Name]; ok && ru.latestSet {
				entry.LastScheduleTime = ru.latest.Status.StartTime
				entry.LastBackupName = ru.latest.Name
				entry.LastBackupPhase = string(ru.latest.Status.Phase)
				entry.LastSuccessfulTime = ru.lastOK
				if ru.lastOK != nil && (overallLastOK == nil || ru.lastOK.After(overallLastOK.Time)) {
					overallLastOK = ru.lastOK
				}
			}
			schedules = append(schedules, entry)
		}
	}

	// Also consider ad-hoc (unscheduled) backups for LastBackupTime.
	for i := range list.Items {
		b := &list.Items[i]
		if b.Status.Phase != v1alpha1.BackupPhaseSucceeded || b.Status.CompletionTime == nil {
			continue
		}
		if overallLastOK == nil || b.Status.CompletionTime.After(overallLastOK.Time) {
			overallLastOK = b.Status.CompletionTime
		}
	}

	// Deterministic ordering for diff stability.
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].Name < schedules[j].Name })

	patchBase := fg.DeepCopy()
	fg.Status.BackupSchedules = schedules
	fg.Status.LastBackupTime = overallLastOK
	if err := r.Status().Patch(ctx, fg, client.MergeFrom(patchBase)); err != nil {
		return fmt.Errorf("patch backup status rollup: %w", err)
	}
	return nil
}

// startAfter returns true if a's start time is after b's start time.
func startAfter(a, b *v1alpha1.MysqlBackup) bool {
	return timeOrZero(a.Status.StartTime).After(timeOrZero(b.Status.StartTime))
}

func boolPtr(v bool) *bool { return &v }

// Tick reserved for future: if the reconciler wants to requeue schedule
// rollup updates on a fixed cadence (not currently needed because the
// Watches(MysqlBackup) wiring in SetupWithManager already re-triggers
// reconciliation on every backup state change).
var _ = time.Second
