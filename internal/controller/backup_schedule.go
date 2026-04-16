package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// defaultScheduleTimeZone is the timezone used when a BackupSchedule
// does not set TimeZone. We default to UTC rather than the
// kube-controller-manager local timezone for reproducibility across
// clusters.
const defaultScheduleTimeZone = "Etc/UTC"

// SetOperatorImageDefaults is called from cmd/bloodraven/main.go so the
// schedule reconciler knows what image and ServiceAccount to run inside
// the scheduled CronJob pods. It is a package-level setter to keep the
// reconciler struct stable.
func SetOperatorImageDefaults(image, serviceAccount string) {
	operatorImageFromEnv = image
	operatorServiceAccountFromEnv = serviceAccount
}

// reconcileBackupAssets provisions the shared scripts ConfigMap and any
// operator-managed backup PVCs referenced by PVC-backed profiles. The
// scripts ConfigMap is reconciled whenever spec.backup OR
// spec.initFromBackup is set (both paths mount dump.py / restore.py);
// owned PVCs are only provisioned when spec.backup declares PVC-backed
// profiles.
//
// This function also emits the PITR reserved-not-implemented warning
// event when spec.backup.pitr is explicitly enabled — PITR is a future
// feature and the operator does not yet act on it.
func (r *MysqlFailoverGroupReconciler) reconcileBackupAssets(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if fg.Spec.Backup == nil && fg.Spec.InitFromBackup == nil {
		return nil
	}

	// PITR validation: when enabled, the referenced profile must exist.
	// The sidecar is responsible for the actual archival; the operator
	// only validates config, injects max_binlog_size, and wires env
	// vars into the pod spec.
	if fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.Enabled {
		pitr := fg.Spec.Backup.PITR
		if pitr.ProfileName == "" {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "BackupPITRInvalid",
				"spec.backup.pitr.enabled=true but profileName is empty; binlog archival disabled")
		} else if findProfile(fg, pitr.ProfileName) == nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "BackupPITRInvalid",
				"spec.backup.pitr.profileName=%q does not match any entry in spec.backup.profiles[]; "+
					"binlog archival disabled", pitr.ProfileName)
		}
	}

	// Both backup and restore Jobs mount this ConfigMap, so we reconcile
	// it for either path.
	if err := r.reconcileBackupScriptsConfigMap(ctx, fg); err != nil {
		return fmt.Errorf("reconcile backup scripts configmap: %w", err)
	}

	// Owned backup PVCs only come from spec.backup.profiles[]. The
	// initFromBackup PVC source is always user-managed (the restore
	// source must exist up-front; see buildRestoreJob).
	if fg.Spec.Backup != nil {
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
	}

	return nil
}

// reconcileBackupScriptsConfigMap owns a single per-group ConfigMap that
// holds the embedded dump.py / restore.py / cleanup.py scripts. Backup,
// restore, and cleanup Jobs mount this ConfigMap read-only at /scripts.
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
			"cleanup.py": BackupCleanupScript(),
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
		image = "bloodraven:latest"
	}
	sa := operatorServiceAccountFromEnv
	if sa == "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "BackupScheduleServiceAccountMissing",
			"operator ServiceAccount not configured (set BLOODRAVEN_OPERATOR_SA env var on the operator deployment); falling back to %q",
			"bloodraven")
		sa = "bloodraven"
	}

	tz := sched.TimeZone
	if tz == "" {
		tz = defaultScheduleTimeZone
	}

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
		// TimeZone is a *string on CronJobSpec; mint a local copy so
		// later mutations to the BackupSchedule struct don't alias
		// the pointer.
		tzCopy := tz
		cj.Spec.TimeZone = &tzCopy
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
// fg.status.backupSchedules[] and fg.status.lastBackupTime. Returns the
// minimum wake-up duration across all schedules (non-zero when a
// retry is pending), and any fatal error.
func (r *MysqlFailoverGroupReconciler) updateBackupStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	if fg.Spec.Backup == nil || len(fg.Spec.Backup.Schedules) == 0 {
		// Still clear the rollup if nothing is scheduled.
		if fg.Status.BackupSchedules == nil && fg.Status.LastBackupTime == nil {
			return 0, nil
		}
	}

	var list v1alpha1.MysqlBackupList
	if err := r.List(ctx, &list,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{labelFailoverGroup: fg.Name},
	); err != nil {
		return 0, fmt.Errorf("list backups: %w", err)
	}

	// Most-recent-per-schedule rollup (by start time).
	type rollup struct {
		latest     v1alpha1.MysqlBackup
		latestSet  bool
		lastOK     *metav1.Time
		lastOKName string
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
				ru.lastOKName = b.Name
			}
		}
	}

	var (
		schedules     []v1alpha1.BackupScheduleStatus
		overallLastOK *metav1.Time
		minRequeue    time.Duration
	)
	if fg.Spec.Backup != nil {
		for _, s := range fg.Spec.Backup.Schedules {
			entry := v1alpha1.BackupScheduleStatus{
				Name:        s.Name,
				CronJobName: scheduleCronJobName(fg.Name, s.Name),
			}
			if ru, ok := bySched[s.Name]; ok && ru.latestSet {
				created := ru.latest.CreationTimestamp
				entry.LastScheduleTime = &created
				entry.LastBackupName = ru.latest.Name
				entry.LastBackupPhase = string(ru.latest.Status.Phase)
				entry.LastSuccessfulTime = ru.lastOK
				entry.LastSuccessfulBackupName = ru.lastOKName
				entry.LastRetryAttempt = ru.latest.Status.Attempt
				if ru.lastOK != nil && (overallLastOK == nil || ru.lastOK.After(overallLastOK.Time)) {
					overallLastOK = ru.lastOK
				}
				// Operator-level retry: when the latest attempt landed
				// in Failed and the schedule has a Retry spec, consider
				// materializing a fresh CR.
				if ru.latest.Status.Phase == v1alpha1.BackupPhaseFailed {
					next, wait, err := r.maybeScheduleRetry(ctx, fg, s, &ru.latest)
					if err != nil {
						return 0, err
					}
					if next != nil {
						entry.NextRetryTime = next
					}
					if wait > 0 && (minRequeue == 0 || wait < minRequeue) {
						minRequeue = wait
					}
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

	// Semantic-equality short-circuit: skip the status patch when the
	// computed rollup is byte-identical to what's already on the CR.
	// This eliminates patch churn on idle reconciles and — crucially —
	// prevents every MysqlBackup watch event from producing a status
	// update storm on the failover group.
	if equality.Semantic.DeepEqual(schedules, fg.Status.BackupSchedules) &&
		equality.Semantic.DeepEqual(overallLastOK, fg.Status.LastBackupTime) {
		return minRequeue, nil
	}

	patchBase := fg.DeepCopy()
	fg.Status.BackupSchedules = schedules
	fg.Status.LastBackupTime = overallLastOK
	if err := r.Status().Patch(ctx, fg, client.MergeFrom(patchBase)); err != nil {
		return 0, fmt.Errorf("patch backup status rollup: %w", err)
	}
	return minRequeue, nil
}

// maybeScheduleRetry considers materializing a retry MysqlBackup for a
// Failed latest attempt on a scheduled CR. Returns (next, wait, err):
//
//	next  — populates entry.NextRetryTime when non-nil
//	wait  — contributes to the reconciler's RequeueAfter
//	err   — fatal error (list / create failures)
//
// A no-op result is (nil, 0, nil): no retry pending, no wake-up needed.
func (r *MysqlFailoverGroupReconciler) maybeScheduleRetry(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sched v1alpha1.BackupSchedule, failed *v1alpha1.MysqlBackup) (*metav1.Time, time.Duration, error) {
	if fg.Spec.Backup == nil || fg.Spec.Backup.Retry == nil {
		return nil, 0, nil
	}
	retry := fg.Spec.Backup.Retry
	if retry.MaxAttempts <= 1 {
		return nil, 0, nil
	}

	current := failed.Status.Attempt
	if current < 1 {
		current = 1
	}
	if current >= retry.MaxAttempts {
		return nil, 0, nil
	}

	// Check if a later attempt already exists (same schedule, higher
	// Attempt counter). If so, the retry has already been materialized.
	var siblings v1alpha1.MysqlBackupList
	if err := r.List(ctx, &siblings,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{
			labelFailoverGroup:  fg.Name,
			labelBackupSchedule: sched.Name,
		},
	); err != nil {
		return nil, 0, fmt.Errorf("list retry siblings: %w", err)
	}
	for i := range siblings.Items {
		if siblings.Items[i].Status.Attempt > current {
			return nil, 0, nil
		}
	}

	initial := retry.InitialBackoffSeconds
	if initial <= 0 {
		initial = 60
	}
	maxBackoff := retry.MaxBackoffSeconds
	if maxBackoff <= 0 {
		maxBackoff = 1800
	}

	// Exponential backoff starting at initial, doubling each step,
	// capped at maxBackoff. For the Nth retry (1-indexed), wait
	// initial * 2^(N-1) seconds.
	step := int32(1)
	for i := int32(1); i < current; i++ {
		step *= 2
	}
	backoffSeconds := initial * step
	if backoffSeconds > maxBackoff {
		backoffSeconds = maxBackoff
	}

	var base time.Time
	if failed.Status.CompletionTime != nil {
		base = failed.Status.CompletionTime.Time
	} else {
		base = time.Now()
	}
	when := base.Add(time.Duration(backoffSeconds) * time.Second)
	now := time.Now()
	if when.After(now) {
		wait := when.Sub(now)
		next := metav1.NewTime(when)
		return &next, wait, nil
	}

	// Backoff elapsed — create the retry CR.
	nextAttempt := current + 1
	retryCR := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s-r%d-", fg.Name, sched.ProfileName, nextAttempt),
			Namespace:    fg.Namespace,
			Labels: map[string]string{
				labelFailoverGroup:  fg.Name,
				labelBackupProfile:  sched.ProfileName,
				labelBackupSchedule: sched.Name,
				labelManagedBy:      managerName,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name},
			ProfileName:      sched.ProfileName,
			TriggeredBy:      fmt.Sprintf("retry/%s/attempt-%d", sched.Name, nextAttempt),
		},
	}
	if err := r.Create(ctx, retryCR); err != nil {
		return nil, 0, fmt.Errorf("create retry backup: %w", err)
	}
	// Best-effort status patch to record the attempt number. Missing
	// this doesn't break anything — the reconciler will default
	// Attempt=1 on the next pass — but it helps siblings loop detect
	// the retry on the very next reconcile of the parent CR.
	freshPatch := client.MergeFrom(retryCR.DeepCopy())
	retryCR.Status.Attempt = nextAttempt
	_ = r.Status().Patch(ctx, retryCR, freshPatch)

	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "BackupRetryScheduled",
		"scheduled retry #%d for failed backup %s (schedule=%s)",
		nextAttempt, failed.Name, sched.Name)

	return nil, 0, nil
}

// startAfter returns true if a was scheduled after b. Prefers the CR
// creation timestamp (always populated) and falls back to Job StartTime
// for deterministic ordering when two CRs share the same creation time.
func startAfter(a, b *v1alpha1.MysqlBackup) bool {
	ac := a.CreationTimestamp.Time
	bc := b.CreationTimestamp.Time
	if !ac.Equal(bc) {
		return ac.After(bc)
	}
	return timeOrZero(a.Status.StartTime).After(timeOrZero(b.Status.StartTime))
}

func boolPtr(v bool) *bool { return &v }
