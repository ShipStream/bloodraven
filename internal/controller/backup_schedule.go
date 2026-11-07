package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

// operatorImageFromEnv is overridden by the main reconciler constructor
// so the scheduled CronJobs can run the same operator image with the
// `bloodraven trigger-backup` subcommand. Left as a package variable so
// the ClusterRole/ServiceAccount already granted to the operator are
// reused.
var operatorImageFromEnv = ""

// defaultScheduleTimeZone is the timezone used when a BackupSchedule
// does not set TimeZone. We default to UTC rather than the
// kube-controller-manager local timezone for reproducibility across
// clusters.
const defaultScheduleTimeZone = "Etc/UTC"

// SetOperatorImageDefaults is called from cmd/bloodraven/main.go so the
// schedule reconciler knows what image to run inside the scheduled
// CronJob pods. It is a package-level setter to keep the reconciler
// struct stable. The second argument is ignored: schedule pods run
// under the group's schedule trigger ServiceAccount, not the operator's.
func SetOperatorImageDefaults(image, _ string) {
	operatorImageFromEnv = image
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
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventBackupPITRInvalid,
				"spec.backup.pitr.enabled=true but profileName is empty; binlog archival disabled")
		} else if findProfile(fg, pitr.ProfileName) == nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventBackupPITRInvalid,
				"spec.backup.pitr.profileName=%q does not match any entry in spec.backup.profiles[]; "+
					"binlog archival disabled", pitr.ProfileName)
		}
	}

	// Encryption validation: the passphrase Secret must exist and
	// contain the referenced key. We emit events (not failures) so a
	// user who intentionally plans to create the Secret *after* the
	// MysqlFailoverGroup isn't blocked — but every subsequent backup
	// that tries to mount a missing Secret will itself fail visibly,
	// which is the desired signal path.
	if fg.Spec.Backup != nil {
		for i := range fg.Spec.Backup.Profiles {
			p := &fg.Spec.Backup.Profiles[i]
			if !p.EncryptionEnabled() {
				continue
			}
			if err := r.validateEncryptionSecret(ctx, fg, p); err != nil {
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventBackupEncryptionInvalid,
					"profile %q: %s", p.Name, err.Error())
			}
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
			if profile.Storage.PVC.Provisioned == nil {
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

// validateEncryptionSecret confirms the referenced passphrase Secret
// exists and carries the expected key. Errors are returned so the
// caller can decide how to surface them — reconcileBackupAssets
// surfaces them as BackupEncryptionInvalid events rather than blocking
// reconciliation.
func (r *MysqlFailoverGroupReconciler) validateEncryptionSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, profile *v1alpha1.BackupProfile) error {
	ref := profile.Encryption.PassphraseSecretKeyRef
	key := ref.PassphraseSecretKeyOrDefault()
	var s corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: fg.Namespace, Name: ref.Name}, &s); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("encryption.passphraseSecretKeyRef Secret %q not found in namespace %q",
				ref.Name, fg.Namespace)
		}
		return fmt.Errorf("get encryption Secret: %w", err)
	}
	data, ok := s.Data[key]
	if !ok {
		return fmt.Errorf("Secret %q does not contain key %q", ref.Name, key)
	}
	// The one trim every consumer applies (Δ16, D-7): leading and
	// trailing whitespace, so the length checked is the key's.
	data = backupcrypto.TrimPassphrase(data)
	if len(data) == 0 {
		return fmt.Errorf("Secret %q key %q is empty", ref.Name, key)
	}
	// Warn-only: backups still run, so existing groups with a short
	// passphrase keep their backups and PITR archive.
	if len(data) < backupcrypto.MinPassphraseLength {
		return fmt.Errorf("Secret %q key %q holds a %d-byte passphrase; use at least %d random bytes, since the backup key is derived without key stretching",
			ref.Name, key, len(data), backupcrypto.MinPassphraseLength)
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
		cm.Labels[v1alpha1.FailoverGroupLabel] = fg.Name
		cm.Labels[labelManagedBy] = managerName
		cm.Data = map[string]string{
			"dump.py":    BackupDumpScript(),
			"restore.py": BackupRestoreScript(),
			"cleanup.py": BackupCleanupScript(),
			"verify.sh":  BackupVerifyScript(),
		}
		return nil
	})
	return err
}

// reconcileOwnedBackupPVC provisions the PVC of a profile with
// pvc.provisioned. An existing claim is never changed: the backup
// reconciler refuses a claim that does not match (ownedBackupPVCMismatch).
func (r *MysqlFailoverGroupReconciler) reconcileOwnedBackupPVC(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, profile v1alpha1.BackupProfile) error {
	if profile.Storage.PVC == nil || profile.Storage.PVC.Provisioned == nil {
		return nil
	}
	provisioned := profile.Storage.PVC.Provisioned
	size, err := provisioned.Size.Quantity()
	if err != nil {
		return fmt.Errorf("pvc.provisioned.size: %w", err)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ownedBackupPVCName(fg.Name, profile.Name),
			Namespace: fg.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		if err := controllerutil.SetControllerReference(fg, pvc, r.Scheme); err != nil {
			return err
		}
		if pvc.Labels == nil {
			pvc.Labels = map[string]string{}
		}
		pvc.Labels[labelAppName] = "mysql-backup"
		pvc.Labels[labelInstance] = fg.Name
		pvc.Labels[v1alpha1.FailoverGroupLabel] = fg.Name
		pvc.Labels[v1alpha1.BackupProfileLabel] = profile.Name
		pvc.Labels[labelManagedBy] = managerName

		// PVC spec is largely immutable — only set on creation.
		if pvc.CreationTimestamp.IsZero() {
			pvc.Spec = corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: size,
					},
				},
				StorageClassName: provisioned.StorageClassName,
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

	// Remembered refusals of schedules no longer in the spec (or of an
	// earlier incarnation of the group) are dropped (U27).
	current := map[string]bool{}
	if fg.Spec.Backup != nil {
		for _, sched := range fg.Spec.Backup.Schedules {
			current[sched.Name] = true
		}
	}
	r.forgetScheduleRefusals(client.ObjectKeyFromObject(fg), fg.UID, current)

	if fg.Spec.Backup != nil {
		for _, sched := range fg.Spec.Backup.Schedules {
			// A read-only profile (D-10, Δ23) is never backed up on a
			// schedule: its CronJob is not rendered (and an existing one
			// is pruned below), with one Warning per change.
			if readOnly, why := pitrProfileReadOnly(fg, sched.ProfileName); readOnly {
				r.noteScheduleRefused(fg, sched.Name, why)
				continue
			}
			r.noteScheduleRefused(fg, sched.Name, "")
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
			v1alpha1.FailoverGroupLabel: fg.Name,
			v1alpha1.ResourceLabel:      "backup-schedule",
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
// under the group's schedule trigger ServiceAccount (see
// reconcileScheduleTriggerRBAC) to create a MysqlBackup CR.
func (r *MysqlFailoverGroupReconciler) reconcileOneSchedule(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sched v1alpha1.BackupSchedule) error {
	// Validate the profile exists. If it doesn't, still create the
	// CronJob but emit a warning — the scheduled pod will fail fast
	// and make the misconfiguration visible via MysqlBackup phase.
	if findProfile(fg, sched.ProfileName) == nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventBackupScheduleInvalid,
			"schedule %q references unknown profile %q", sched.Name, sched.ProfileName)
	}

	image := operatorImageFromEnv
	if image == "" {
		image = "bloodraven:latest"
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
			labelAppName:                 "mysql-backup",
			labelInstance:                fg.Name,
			v1alpha1.FailoverGroupLabel:  fg.Name,
			v1alpha1.BackupProfileLabel:  sched.ProfileName,
			v1alpha1.BackupScheduleLabel: sched.Name,
			v1alpha1.ResourceLabel:       "backup-schedule",
			labelManagedBy:               managerName,
		}
		cj.Labels = labels

		concurrency := batchv1.ForbidConcurrent
		switch sched.ConcurrencyPolicy {
		case batchv1.AllowConcurrent:
			concurrency = batchv1.AllowConcurrent
		case batchv1.ReplaceConcurrent:
			concurrency = batchv1.ReplaceConcurrent
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
						ServiceAccountName: scheduleTriggerName(fg.Name),
						Tolerations:        groupReadOnlyTolerations(fg),
						Containers: []corev1.Container{
							{
								Name:  "trigger",
								Image: image,
								Command: operatorCommand(
									"trigger-backup",
									"--group="+fg.Name,
									"--profile="+sched.ProfileName,
									"--schedule="+sched.Name,
									"--namespace="+fg.Namespace,
								),
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
		client.MatchingLabels{v1alpha1.FailoverGroupLabel: fg.Name},
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
		sched := b.Labels[v1alpha1.BackupScheduleLabel]
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
				entry.LastBackupPhase = ru.latest.Status.Phase
				entry.LastSuccessfulTime = ru.lastOK
				entry.LastSuccessfulBackupName = ru.lastOKName
				entry.LastRetryAttempt = effectiveBackupAttempt(&ru.latest)
				if ru.lastOK != nil && (overallLastOK == nil || ru.lastOK.After(overallLastOK.Time)) {
					overallLastOK = ru.lastOK
				}
				// Operator-level retry: when the latest attempt landed
				// in Failed and the schedule has a Retry spec, consider
				// materializing a fresh CR.
				if ru.latest.Status.Phase == v1alpha1.BackupPhaseFailed {
					next, wait, conflict, err := r.maybeScheduleRetry(ctx, fg, s, &ru.latest)
					if err != nil {
						return 0, err
					}
					if next != nil {
						entry.NextRetryTime = next
					}
					entry.RetryConflict = conflict
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

	// Earlier reconciliation steps may already have written status. Refresh
	// before the optimistic patch, also avoiding duplicate warnings from cache lag.
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var latest v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, client.ObjectKeyFromObject(fg), &latest); err != nil {
		return 0, fmt.Errorf("refresh backup status rollup: %w", err)
	}
	if latest.UID != fg.UID || latest.Generation != fg.Generation {
		return 0, apierrors.NewConflict(v1alpha1.GroupVersion.WithResource("mysqlfailovergroups").GroupResource(), fg.Name, fmt.Errorf("group changed during backup rollup"))
	}
	if equality.Semantic.DeepEqual(schedules, latest.Status.BackupSchedules) && equality.Semantic.DeepEqual(overallLastOK, latest.Status.LastBackupTime) {
		fg.Status, fg.ResourceVersion = latest.Status, latest.ResourceVersion
		return minRequeue, nil
	}
	patchBase := latest.DeepCopy()
	latest.Status.BackupSchedules = schedules
	latest.Status.LastBackupTime = overallLastOK
	if err := r.Status().Patch(ctx, &latest, client.MergeFromWithOptions(patchBase, client.MergeFromWithOptimisticLock{})); err != nil {
		return 0, fmt.Errorf("patch backup status rollup: %w", err)
	}
	fg.Status, fg.ResourceVersion = latest.Status, latest.ResourceVersion
	previousConflicts := make(map[string]string)
	for _, entry := range patchBase.Status.BackupSchedules {
		previousConflicts[entry.Name] = entry.RetryConflict
	}
	for _, entry := range schedules {
		if entry.RetryConflict == "" || entry.RetryConflict == previousConflicts[entry.Name] {
			continue
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventBackupRetryConflict,
			"retry backup %q already exists with different lineage (failed backup=%s, schedule=%s); skipping this retry",
			entry.RetryConflict, entry.LastBackupName, entry.Name)
		log.FromContext(ctx).Info("backup retry lineage conflict",
			"group", fg.Name, "schedule", entry.Name, "backup", entry.LastBackupName, "retryBackup", entry.RetryConflict)
	}
	return minRequeue, nil
}

// Defaults of spec.backup.retry.{initialBackoff,maxBackoff} for objects
// built outside admission.
const (
	defaultBackupRetryInitialBackoff = time.Minute
	defaultBackupRetryMaxBackoff     = 30 * time.Minute
)

// backupRetryBackoffBounds returns the effective initial and maximum retry
// backoff. Admission bounds both (1s..1h, 1s..24h, max >= initial); only
// nil takes the default.
func backupRetryBackoffBounds(retry *v1alpha1.BackupRetrySpec) (initial, maxBackoff time.Duration) {
	initial, maxBackoff = defaultBackupRetryInitialBackoff, defaultBackupRetryMaxBackoff
	if retry.InitialBackoff != nil {
		initial = retry.InitialBackoff.Duration
	}
	if retry.MaxBackoff != nil {
		maxBackoff = retry.MaxBackoff.Duration
	}
	return initial, maxBackoff
}

// maybeScheduleRetry considers materializing a retry MysqlBackup for a
// Failed latest attempt on a scheduled CR. Returns (next, wait, conflict, err):
//
//	next  — populates entry.NextRetryTime when non-nil
//	wait  — contributes to the reconciler's RequeueAfter
//	conflict — name of a retry blocked by different lineage
//	err   — fatal error
//
// A no-op result is (nil, 0, "", nil): no retry pending, no wake-up needed.
func (r *MysqlFailoverGroupReconciler) maybeScheduleRetry(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sched v1alpha1.BackupSchedule, failed *v1alpha1.MysqlBackup) (*metav1.Time, time.Duration, string, error) {
	if fg.Spec.Backup == nil || fg.Spec.Backup.Retry == nil {
		return nil, 0, "", nil
	}
	retry := fg.Spec.Backup.Retry
	if retry.MaxAttempts <= 1 {
		return nil, 0, "", nil
	}

	current := effectiveBackupAttempt(failed)
	if current >= retry.MaxAttempts {
		return nil, 0, "", nil
	}

	initial, maxBackoff := backupRetryBackoffBounds(retry)

	// Exponential backoff starting at initial, doubling each step,
	// capped at maxBackoff. For the Nth retry (1-indexed), wait
	// initial * 2^(N-1).
	backoff := initial
	for i := int32(1); i < current && backoff < maxBackoff; i++ {
		backoff *= 2
	}
	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	var base time.Time
	if failed.Status.CompletionTime != nil {
		base = failed.Status.CompletionTime.Time
	} else {
		base = time.Now()
	}
	when := base.Add(backoff)
	now := time.Now()
	if when.After(now) {
		wait := when.Sub(now)
		next := metav1.NewTime(when)
		return &next, wait, "", nil
	}

	// Backoff elapsed — create the retry CR.
	if failed.UID == "" {
		return nil, 0, "", fmt.Errorf("cannot retry backup %q without its UID", failed.Name)
	}
	nextAttempt := current + 1
	rootUID, rootName := failed.Annotations[v1alpha1.BackupRetryRootUIDAnnotation], failed.Annotations[v1alpha1.BackupRetryRootAnnotation]
	if rootUID == "" {
		rootUID, rootName = string(failed.UID), failed.Name
	}
	retryCR := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      retryBackupName(failed, nextAttempt),
			Namespace: fg.Namespace,
			Annotations: map[string]string{
				v1alpha1.BackupRetryRootAnnotation:    rootName,
				v1alpha1.BackupRetryRootUIDAnnotation: rootUID,
				v1alpha1.BackupAttemptAnnotation:      strconv.FormatInt(int64(nextAttempt), 10),
				v1alpha1.TriggeredByAnnotation:        fmt.Sprintf("retry/%s/attempt-%d", sched.Name, nextAttempt),
			},
			Labels: map[string]string{
				v1alpha1.BackupRetryOfLabel:  string(failed.UID),
				v1alpha1.FailoverGroupLabel:  fg.Name,
				v1alpha1.BackupProfileLabel:  sched.ProfileName,
				v1alpha1.BackupScheduleLabel: sched.Name,
				labelManagedBy:               managerName,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fg.Name},
			ProfileName:      sched.ProfileName,
		},
	}
	if err := r.Create(ctx, retryCR); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, 0, "", fmt.Errorf("create retry backup: %w", err)
		}
		var existing v1alpha1.MysqlBackup
		if err := r.Get(ctx, client.ObjectKeyFromObject(retryCR), &existing); err != nil {
			if apierrors.IsNotFound(err) {
				// Create saw the object before the cache did. Keep the same
				// deterministic name and check again without blocking rollups.
				return nil, 5 * time.Second, "", nil
			}
			return nil, 0, "", fmt.Errorf("verify existing retry backup: %w", err)
		}
		if !retryBackupMatches(&existing, retryCR) {
			return nil, 0, retryCR.Name, nil
		}
		return nil, 0, "", nil
	}
	// The launch annotation is durable; this status patch only accelerates
	// observation by clients that display status.attempt.
	freshPatch := client.MergeFrom(retryCR.DeepCopy())
	retryCR.Status.Attempt = nextAttempt
	_ = r.Status().Patch(ctx, retryCR, freshPatch)

	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventBackupRetryScheduled,
		"scheduled retry #%d for failed backup %s (schedule=%s)",
		nextAttempt, failed.Name, sched.Name)

	return nil, 0, "", nil
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

// forgetScheduleRefusals drops what noteScheduleRefused remembers for the
// group nn, except the schedules in keep of incarnation uid (keep nil:
// everything, for a deleted group).
func (r *MysqlFailoverGroupReconciler) forgetScheduleRefusals(nn types.NamespacedName, uid types.UID, keep map[string]bool) {
	prefix := nn.Namespace + "/" + nn.Name + "/"
	r.scheduleRefusedMu.Lock()
	defer r.scheduleRefusedMu.Unlock()
	for key := range r.scheduleRefused {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		gotUID, schedule, _ := strings.Cut(rest, "/")
		if keep == nil || types.UID(gotUID) != uid || !keep[schedule] {
			delete(r.scheduleRefused, key)
		}
	}
}

// noteScheduleRefused emits BackupScheduleRefused when a schedule's
// read-only refusal starts or its reason changes, and forgets it when the
// schedule is no longer refused (why == "").
func (r *MysqlFailoverGroupReconciler) noteScheduleRefused(fg *v1alpha1.MysqlFailoverGroup, schedule, why string) {
	key := fg.Namespace + "/" + fg.Name + "/" + string(fg.UID) + "/" + schedule
	r.scheduleRefusedMu.Lock()
	defer r.scheduleRefusedMu.Unlock()
	if why == "" {
		delete(r.scheduleRefused, key)
		return
	}
	if r.scheduleRefused[key] == why {
		return
	}
	if r.scheduleRefused == nil {
		r.scheduleRefused = map[string]string{}
	}
	r.scheduleRefused[key] = why
	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventBackupScheduleRefused,
			"schedule %q is not run: %s", schedule, why)
	}
}

// ownedBackupPVCMismatch compares a profile's owned claim with its
// pvc.provisioned. A transition rule cannot see a profile that was removed
// and added back with other values, so the claim an earlier profile of the
// same name created may differ. It returns a refusal message when the claim
// has another explicit class or requests less than Size, and expanded when
// it requests more (an out-of-band expansion, which is accepted). A nil
// class matches any class: the DefaultStorageClass admission plugin writes
// the default's name into the claim. Size is compared with the request,
// not with status.capacity, which provisioners round up.
func ownedBackupPVCMismatch(provisioned *v1alpha1.ProvisionedVolumeSpec, pvc *corev1.PersistentVolumeClaim) (refusal string, expanded bool) {
	if provisioned.StorageClassName != nil {
		got := ""
		if pvc.Spec.StorageClassName != nil {
			got = *pvc.Spec.StorageClassName
		}
		if got != *provisioned.StorageClassName {
			return fmt.Sprintf("PVC %s has storageClassName %q, but pvc.provisioned.storageClassName is %q; delete the claim or restore the profile's values",
				pvc.Name, got, *provisioned.StorageClassName), false
		}
	}
	want, err := provisioned.Size.Quantity()
	if err != nil {
		return fmt.Sprintf("pvc.provisioned.size: %v", err), false
	}
	request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	switch request.Cmp(want) {
	case -1:
		return fmt.Sprintf("PVC %s requests %s, less than pvc.provisioned.size %s; delete the claim or restore the profile's values",
			pvc.Name, request.String(), provisioned.Size), false
	case 1:
		return "", true
	}
	return "", false
}
