package controller

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
)

// Condition types on MysqlBackup.status.conditions.
const (
	ConditionBackupJobCreated = "JobCreated"
	ConditionBackupReady      = "Ready"
)

// maxFailedRetention caps the number of Failed MysqlBackup CRs kept per
// (group, profile) pair. Prevents unbounded growth when backups repeatedly
// fail against a misconfigured profile.
const maxFailedRetention = 10

// MysqlBackupReconciler reconciles MysqlBackup resources.
type MysqlBackupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Clientset is optional; when non-nil it is used to tail backup Job
	// pod logs for the BLOODRAVEN_DUMP_COMPLETE sentinel so that
	// status.location / status.size can be populated. Tests using the
	// controller-runtime fake client leave it nil.
	Clientset kubernetes.Interface
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

func (r *MysqlBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("mysqlbackup", req.NamespacedName)

	var backup v1alpha1.MysqlBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion with finalizer. The rewritten finalize returns
	// (done, err):
	//   (true,  nil)  => we may remove the finalizer
	//   (false, nil)  => cleanup still in progress, requeue softly
	//   (false, err)  => hard error, back off on the next reconcile
	if !backup.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&backup, mysqlBackupFinalizer) {
			done, err := r.finalize(ctx, &backup)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("finalize mysqlbackup: %w", err)
			}
			if !done {
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
			controllerutil.RemoveFinalizer(&backup, mysqlBackupFinalizer)
			if err := r.Update(ctx, &backup); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&backup, mysqlBackupFinalizer) {
		controllerutil.AddFinalizer(&backup, mysqlBackupFinalizer)
		// Also stamp the canonical labels on first encounter so label
		// selectors work even for ad-hoc CRs created by `kubectl create`.
		_ = ensureBackupLabels(&backup)
		if err := r.Update(ctx, &backup); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Terminal states: retention pruning only, then stop reconciling.
	if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded || backup.Status.Phase == v1alpha1.BackupPhaseFailed {
		if err := r.pruneRetention(ctx, &backup); err != nil {
			logger.Error(err, "retention prune")
		}
		return ctrl.Result{}, nil
	}

	// Resolve the referenced failover group.
	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.FailoverGroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failBackup(ctx, &backup, "GroupNotFound",
				fmt.Sprintf("MysqlFailoverGroup %q not found", fgKey.Name))
		}
		return ctrl.Result{}, fmt.Errorf("get failover group: %w", err)
	}

	// Coalesce owner-ref set + label stamp into a single Update so we
	// don't round-trip the reconciler twice on creation.
	needsUpdate := false
	if len(backup.OwnerReferences) == 0 {
		if err := controllerutil.SetControllerReference(&fg, &backup, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set owner ref: %w", err)
		}
		needsUpdate = true
	}
	if ensureBackupLabels(&backup) {
		needsUpdate = true
	}
	if needsUpdate {
		if err := r.Update(ctx, &backup); err != nil {
			return ctrl.Result{}, fmt.Errorf("update owner ref / labels: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve the named profile.
	profile := findProfile(&fg, backup.Spec.ProfileName)
	if profile == nil {
		return r.failBackup(ctx, &backup, "ProfileNotFound",
			fmt.Sprintf("profile %q not found on MysqlFailoverGroup %q",
				backup.Spec.ProfileName, fg.Name))
	}

	// Pick the source site (replica-first, primary fallback).
	maxLag := int64(300)
	if fg.Spec.Backup != nil && fg.Spec.Backup.MaxLagSecondsForSource > 0 {
		maxLag = fg.Spec.Backup.MaxLagSecondsForSource
	}
	sourceSite, reason, err := selectSourceSite(&fg, backup.Spec.SourceSiteOverride, maxLag)
	if err != nil {
		return r.failBackup(ctx, &backup, "NoHealthySource", err.Error())
	}

	// Ensure derived creds Secret (parses the group's DSN into user/password).
	credsName := backupCredsSecretName(backup.Name)
	if err := r.ensureDerivedCredsSecret(ctx, &fg, &backup, credsName); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure creds secret: %w", err)
	}

	// Ensure the Job exists.
	var job batchv1.Job
	jobName := backupJobName(backup.Name)
	jobKey := types.NamespacedName{Namespace: backup.Namespace, Name: jobName}
	if err := r.Get(ctx, jobKey, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get job: %w", err)
		}
		// Build & create.
		built, err := BuildBackupJob(BackupJobInputs{
			FailoverGroup:        &fg,
			Profile:              *profile,
			Backup:               &backup,
			SourceSite:           sourceSite,
			CredsSecretName:      credsName,
			ScriptsConfigMapName: backupScriptsConfigMapName(fg.Name),
		})
		if err != nil {
			return r.failBackup(ctx, &backup, "BuildJobFailed", err.Error())
		}
		if err := controllerutil.SetControllerReference(&backup, built, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set job owner ref: %w", err)
		}
		if err := r.Create(ctx, built); err != nil {
			return ctrl.Result{}, fmt.Errorf("create job: %w", err)
		}
		r.Recorder.Eventf(&backup, corev1.EventTypeNormal, "BackupStarted",
			"created Job %s targeting site %s (reason=%s)", built.Name, sourceSite, reason)

		now := metav1.Now()
		patch := client.MergeFrom(backup.DeepCopy())
		backup.Status.Phase = v1alpha1.BackupPhaseRunning
		backup.Status.StartTime = &now
		backup.Status.JobName = built.Name
		backup.Status.SourceSite = sourceSite
		backup.Status.StorageType = profile.Storage.Type
		backup.Status.MysqlImage = mysqlImageFor(&fg)
		backup.Status.ActiveSiteAtStart = fg.Status.ActiveSite
		if profile.EncryptionEnabled() {
			// Stamp encryption attribution at Job-creation time so the
			// CR is self-describing even before the log-tail metadata
			// parse lands. The algorithm default drifts forward with
			// new releases via AlgorithmOrDefault(); keep both
			// populated for easy compliance auditing.
			backup.Status.Encrypted = true
			backup.Status.EncryptionAlgorithm = profile.Encryption.AlgorithmOrDefault()
		}
		if backup.Status.Attempt == 0 {
			backup.Status.Attempt = 1
		}
		backup.Status.Message = fmt.Sprintf("running (source=%s, reason=%s)", sourceSite, reason)
		backup.Status.ObservedGeneration = backup.Generation
		setCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               ConditionBackupJobCreated,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: now,
			Reason:             "Created",
			Message:            fmt.Sprintf("Job %s created", built.Name),
		})
		if err := r.Status().Patch(ctx, &backup, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch status after job create: %w", err)
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Job exists — observe its status.
	phase, message := jobPhase(&job, "backup")
	if phase == "" {
		// Still running. Emit a soft warning if the group's active
		// site drifted while the backup was in flight.
		r.maybeWarnInFlightFailover(&backup, &fg)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// --- Terminal state ----------------------------------------------
	//
	// Build a candidate next status on a deep-copied starting point,
	// then only patch + emit metrics/events if it's semantically
	// different from what's already on the object. This is the key
	// exactly-once guarantee for metric emission: stableJobCompletionTime
	// returns the same instant for every reconcile of the same terminal
	// Job, so DeepEqual short-circuits all subsequent attempts.
	next := *backup.Status.DeepCopy()
	stableTime := stableJobCompletionTime(&job)
	var completion metav1.Time
	if stableTime != nil {
		completion = *stableTime
	} else {
		completion = metav1.Now()
	}

	next.Phase = phase
	if next.StartTime == nil {
		if job.Status.StartTime != nil {
			next.StartTime = job.Status.StartTime
		} else {
			next.StartTime = &completion
		}
	}
	next.CompletionTime = &completion
	next.Message = message
	if next.SourceSite == "" {
		next.SourceSite = sourceSite
	}
	if next.StorageType == "" && profile != nil {
		next.StorageType = profile.Storage.Type
	}
	if next.MysqlImage == "" {
		next.MysqlImage = mysqlImageFor(&fg)
	}
	next.ObservedGeneration = backup.Generation

	if phase == v1alpha1.BackupPhaseSucceeded {
		if meta, ok := r.tailJobCompletion(ctx, &backup, &job); ok {
			if meta.Location != "" {
				next.Location = meta.Location
			}
			if meta.Size != "" {
				next.Size = meta.Size
			}
			if meta.SizeBytes > 0 {
				next.SizeBytes = meta.SizeBytes
			}
			if meta.GtidExecuted != "" {
				next.GtidExecuted = meta.GtidExecuted
			}
			if meta.BinlogFile != "" {
				next.BinlogFile = meta.BinlogFile
			}
			if meta.BinlogPos > 0 {
				next.BinlogPos = meta.BinlogPos
			}
			if meta.Encrypted {
				next.Encrypted = true
			}
			if meta.EncryptionAlgorithm != "" {
				next.EncryptionAlgorithm = meta.EncryptionAlgorithm
			}
		}
		// Backfill the encryption fields from the profile spec even when
		// the log tail couldn't be parsed — this covers fake-client
		// tests and any transient log-tail failure.
		if profile != nil && profile.EncryptionEnabled() && !next.Encrypted {
			next.Encrypted = true
			if next.EncryptionAlgorithm == "" {
				next.EncryptionAlgorithm = profile.Encryption.AlgorithmOrDefault()
			}
		}
		// If the dump didn't report bytes but did report a size string
		// (pure local case), derive a display string from the Go
		// helper so it's consistent across code paths.
		if next.Size == "" && next.SizeBytes > 0 {
			next.Size = humanBytes(next.SizeBytes)
		}
		setCondition(&next.Conditions, metav1.Condition{
			Type:               ConditionBackupReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: completion,
			Reason:             "Succeeded",
			Message:            message,
		})
	} else {
		setCondition(&next.Conditions, metav1.Condition{
			Type:               ConditionBackupReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: completion,
			Reason:             "Failed",
			Message:            message,
		})
	}

	// Exactly-once short-circuit.
	if backup.Status.Phase == phase && equality.Semantic.DeepEqual(&backup.Status, &next) {
		// Nothing to do — still run retention once though.
		if err := r.pruneRetention(ctx, &backup); err != nil {
			logger.Error(err, "retention prune after terminal state")
		}
		return ctrl.Result{}, nil
	}

	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status = next
	if err := r.Status().Patch(ctx, &backup, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch final status: %w", err)
	}

	r.emitTerminalMetrics(&backup, &job)

	if phase == v1alpha1.BackupPhaseSucceeded {
		r.Recorder.Eventf(&backup, corev1.EventTypeNormal, "BackupSucceeded",
			"backup %s completed: %s", backup.Name, message)
	} else {
		r.Recorder.Eventf(&backup, corev1.EventTypeWarning, "BackupFailed",
			"backup %s failed: %s", backup.Name, message)
	}

	if err := r.pruneRetention(ctx, &backup); err != nil {
		logger.Error(err, "retention prune after terminal state")
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler with the manager.
func (r *MysqlBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

// finalize deletes the dump artifact for a MysqlBackup before the CR is
// released. Returns (done, err) so the caller can distinguish
// "cleanup complete" from "cleanup in progress":
//
//	(true,  nil)  => may remove the finalizer
//	(false, nil)  => still working, requeue
//	(false, err)  => hard error, back off
//
// When the referenced group or profile is gone we emit an
// ArtifactCleanupSkipped warning event and release the finalizer
// immediately — there's nothing we can do without a bucket/claim.
func (r *MysqlBackupReconciler) finalize(ctx context.Context, backup *v1alpha1.MysqlBackup) (bool, error) {
	// No location recorded — nothing to clean up.
	if backup.Status.Location == "" {
		return true, nil
	}

	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.FailoverGroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Eventf(backup, corev1.EventTypeWarning, "ArtifactCleanupSkipped",
				"failover group %q gone; cannot clean up artifact at %q", fgKey.Name, backup.Status.Location)
			return true, nil
		}
		return false, fmt.Errorf("get failover group: %w", err)
	}

	profile := findProfile(&fg, backup.Spec.ProfileName)
	if profile == nil {
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, "ArtifactCleanupSkipped",
			"profile %q gone from group %q; cannot clean up artifact at %q",
			backup.Spec.ProfileName, fg.Name, backup.Status.Location)
		return true, nil
	}

	// Re-create the derived creds Secret in case it was GC'd between
	// backup completion and finalize.
	credsName := backupCredsSecretName(backup.Name)
	if err := r.ensureDerivedCredsSecret(ctx, &fg, backup, credsName); err != nil {
		return false, fmt.Errorf("ensure creds secret: %w", err)
	}

	// Observe the cleanup Job.
	var cleanupJob batchv1.Job
	cleanupKey := types.NamespacedName{Namespace: backup.Namespace, Name: cleanupJobName(backup.Name)}
	if err := r.Get(ctx, cleanupKey, &cleanupJob); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("get cleanup job: %w", err)
		}
		// Build and create.
		built, err := buildCleanupJob(cleanupJobInputs{
			FailoverGroup:        &fg,
			Profile:              profile,
			Backup:               backup,
			CredsSecretName:      credsName,
			ScriptsConfigMapName: backupScriptsConfigMapName(fg.Name),
		})
		if err != nil {
			return false, fmt.Errorf("build cleanup job: %w", err)
		}
		if err := controllerutil.SetControllerReference(backup, built, r.Scheme); err != nil {
			return false, fmt.Errorf("set cleanup job owner ref: %w", err)
		}
		if err := r.Create(ctx, built); err != nil {
			return false, fmt.Errorf("create cleanup job: %w", err)
		}
		r.Recorder.Eventf(backup, corev1.EventTypeNormal, "ArtifactCleanupStarted",
			"created cleanup Job %s for artifact at %q", built.Name, backup.Status.Location)
		return false, nil
	}

	phase, msg := jobPhase(&cleanupJob, "cleanup")
	switch phase {
	case v1alpha1.BackupPhaseSucceeded:
		r.Recorder.Eventf(backup, corev1.EventTypeNormal, "ArtifactCleanupSucceeded",
			"artifact cleanup succeeded for %q", backup.Status.Location)
		return true, nil
	case v1alpha1.BackupPhaseFailed:
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, "ArtifactCleanupFailed",
			"artifact cleanup Job %s failed: %s (remove finalizer %q to force-delete)",
			cleanupJob.Name, msg, mysqlBackupFinalizer)
		return false, fmt.Errorf("cleanup job %s failed: %s", cleanupJob.Name, msg)
	default:
		// Still running.
		return false, nil
	}
}

// failBackup transitions a MysqlBackup to Failed with the given reason.
func (r *MysqlBackupReconciler) failBackup(ctx context.Context, backup *v1alpha1.MysqlBackup, reason, message string) (ctrl.Result, error) {
	now := metav1.Now()
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status.Phase = v1alpha1.BackupPhaseFailed
	backup.Status.CompletionTime = &now
	backup.Status.Message = message
	backup.Status.ObservedGeneration = backup.Generation
	setCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               ConditionBackupReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: backup.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch failed status: %w", err)
	}
	r.Recorder.Eventf(backup, corev1.EventTypeWarning, reason, "%s", message)
	return ctrl.Result{}, nil
}

// ensureDerivedCredsSecret creates or updates the per-backup Secret
// carrying MYSQL_USER / MYSQL_PASSWORD. In credentials mode, reads
// directly from the effective backup secret. In legacy mode, parses
// the DSN secret. The Secret is owned by the MysqlBackup CR so it is
// garbage collected with the backup.
func (r *MysqlBackupReconciler) ensureDerivedCredsSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, backup *v1alpha1.MysqlBackup, secretName string) error {
	var user, password string

	backupSecretName := fg.Spec.EffectiveBackupSecretName()
	var srcSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: backupSecretName}, &srcSecret); err != nil {
		return fmt.Errorf("get backup credential secret %s: %w", backupSecretName, err)
	}

	if fg.Spec.UsesCredentials() {
		user = string(srcSecret.Data["username"])
		password = string(srcSecret.Data["password"])
		if user == "" {
			return fmt.Errorf("credential secret %s has empty 'username'", backupSecretName)
		}
	} else {
		dsnBytes, ok := srcSecret.Data["dsn"]
		if !ok {
			return fmt.Errorf("secret %s missing 'dsn' key", backupSecretName)
		}
		parsed, err := mysqldriver.ParseDSN(string(dsnBytes))
		if err != nil {
			return fmt.Errorf("parse dsn: %w", err)
		}
		if parsed.User == "" {
			return fmt.Errorf("dsn has empty user")
		}
		user = parsed.User
		password = parsed.Passwd
	}

	derived := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: backup.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, derived, func() error {
		if err := controllerutil.SetControllerReference(backup, derived, r.Scheme); err != nil {
			return err
		}
		if derived.Labels == nil {
			derived.Labels = map[string]string{}
		}
		derived.Labels[labelFailoverGroup] = fg.Name
		derived.Labels[labelMysqlBackup] = backup.Name
		derived.Labels[labelManagedBy] = managerName
		derived.Type = corev1.SecretTypeOpaque
		derived.Data = map[string][]byte{
			"MYSQL_USER":     []byte(user),
			"MYSQL_PASSWORD": []byte(password),
		}
		return nil
	})
	return err
}

// jobPhase summarizes a batch/v1 Job status into a BackupPhase, using
// `kind` ("backup", "restore", or "cleanup") to produce an accurate
// success message for either caller. Returns empty phase when the Job
// is still running.
//
// Both JobComplete and JobFailed conditions are consulted first; when a
// Job is terminal but the conditions have not yet been set by
// kube-controller-manager (e.g. during fast local test loops with a
// fake clientset), we fall back to the numeric Succeeded / Failed
// counters combined with the Job's activeDeadlineSeconds / backoffLimit
// to avoid reporting "still running" on a clearly-finished Job.
func jobPhase(job *batchv1.Job, kind string) (v1alpha1.BackupPhase, string) {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return v1alpha1.BackupPhaseSucceeded, kind + " completed successfully"
		}
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			msg := c.Message
			if msg == "" {
				msg = c.Reason
			}
			if msg == "" {
				msg = kind + " job failed"
			}
			return v1alpha1.BackupPhaseFailed, msg
		}
	}
	if job.Status.Succeeded > 0 {
		return v1alpha1.BackupPhaseSucceeded, kind + " completed successfully"
	}
	// Failed-counter fallback: a Job that has exceeded its backoffLimit
	// is terminally failed even before kube-controller-manager writes
	// the JobFailed condition.
	if job.Status.Failed > 0 {
		limit := int32(0)
		if job.Spec.BackoffLimit != nil {
			limit = *job.Spec.BackoffLimit
		}
		if job.Status.Failed > limit {
			return v1alpha1.BackupPhaseFailed, fmt.Sprintf("%s job failed (%d attempts, backoffLimit=%d)", kind, job.Status.Failed, limit)
		}
	}
	return "", ""
}

// stableJobCompletionTime returns a deterministic terminal time for a
// Job so that repeated reconciles of the same terminal CR produce
// identical status. Preference order:
//
//  1. First JobComplete / JobFailed condition's LastTransitionTime.
//  2. job.Status.CompletionTime (Succeeded path only).
//  3. nil — caller falls back to metav1.Now().
//
// This is the exactly-once anchor: the semantic-equality check in the
// terminal reconcile path only works if the computed "next" status is
// byte-identical across reconciles, which in turn requires a stable
// completion timestamp.
func stableJobCompletionTime(job *batchv1.Job) *metav1.Time {
	for i := range job.Status.Conditions {
		c := &job.Status.Conditions[i]
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			t := c.LastTransitionTime
			return &t
		}
	}
	if job.Status.CompletionTime != nil {
		t := *job.Status.CompletionTime
		return &t
	}
	return nil
}

// dumpCompletionMetadata is the parsed shape of the BLOODRAVEN_DUMP_COMPLETE
// sentinel line emitted by backup_script.py (unencrypted flow) or
// `bloodraven encrypt-upload` (encrypted flow) on success.
type dumpCompletionMetadata struct {
	Location            string
	Size                string
	SizeBytes           int64
	GtidExecuted        string
	BinlogFile          string
	BinlogPos           int64
	Encrypted           bool
	EncryptionAlgorithm string
}

// parseDumpCompleteLine parses a single `BLOODRAVEN_DUMP_COMPLETE k=v ...`
// sentinel line emitted by backup_script.py. Returns (meta, true) on a
// well-formed sentinel and (zero, false) on a prefix mismatch. Unknown
// keys are tolerated (forward-compatible). Malformed numerics leave
// the corresponding field at zero.
//
// Space-bearing values (e.g. "1.4 GiB") are round-tripped through an
// underscore escape so this whitespace-splitting parser can recover them.
func parseDumpCompleteLine(line string) (dumpCompletionMetadata, bool) {
	const prefix = "BLOODRAVEN_DUMP_COMPLETE"
	if !strings.HasPrefix(line, prefix) {
		return dumpCompletionMetadata{}, false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	fields := strings.Fields(rest)
	var meta dumpCompletionMetadata
	for _, f := range fields {
		eq := strings.IndexByte(f, '=')
		if eq <= 0 {
			continue
		}
		key := f[:eq]
		val := strings.ReplaceAll(f[eq+1:], "_", " ")
		switch key {
		case "location":
			meta.Location = val
		case "size":
			meta.Size = val
		case "sizeBytes":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				meta.SizeBytes = n
			}
		case "gtidExecuted":
			meta.GtidExecuted = strings.ReplaceAll(val, " ", "")
		case "binlogFile":
			meta.BinlogFile = val
		case "binlogPos":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				meta.BinlogPos = n
			}
		case "encrypted":
			meta.Encrypted = val == "true" || val == "1"
		case "algorithm":
			meta.EncryptionAlgorithm = val
		}
	}
	return meta, true
}

// tailJobCompletion tails the Job pod log and parses the trailing
// BLOODRAVEN_DUMP_COMPLETE sentinel into dumpCompletionMetadata.
// Returns (zero, false) on any failure so the caller can cleanly leave
// the existing status untouched. Requires a real clientset (nil in
// fake-client tests).
//
// Encrypted backup Jobs run the sentinel-emitting `bloodraven
// encrypt-upload` step in a main container named
// backupEncryptUploadContainerName; unencrypted Jobs emit it from the
// mysqlsh main container. We try both container names and fall back to
// the pod's first container so future container renames don't silently
// drop metadata.
func (r *MysqlBackupReconciler) tailJobCompletion(ctx context.Context, backup *v1alpha1.MysqlBackup, job *batchv1.Job) (dumpCompletionMetadata, bool) {
	if r.Clientset == nil {
		return dumpCompletionMetadata{}, false
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{labelMysqlBackup: backup.Name},
	); err != nil {
		return dumpCompletionMetadata{}, false
	}
	var pod *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded {
			pod = p
			break
		}
	}
	if pod == nil {
		return dumpCompletionMetadata{}, false
	}

	candidates := []string{backupJobContainerName, backupEncryptUploadContainerName}
	if len(pod.Spec.Containers) > 0 {
		// Include the pod's actual main container name as a fallback
		// for forward compatibility.
		name := pod.Spec.Containers[0].Name
		if name != "" && name != backupJobContainerName && name != backupEncryptUploadContainerName {
			candidates = append(candidates, name)
		}
	}

	for _, container := range candidates {
		meta, ok := r.tailOneContainer(ctx, job.Namespace, pod.Name, container)
		if ok {
			return meta, true
		}
	}
	return dumpCompletionMetadata{}, false
}

// tailOneContainer tails a single pod container log for the
// BLOODRAVEN_DUMP_COMPLETE sentinel. Separate helper so tailJobCompletion
// can try multiple container names in order.
func (r *MysqlBackupReconciler) tailOneContainer(ctx context.Context, namespace, podName, container string) (dumpCompletionMetadata, bool) {
	req := r.Clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: container,
		TailLines: ptr64(50),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return dumpCompletionMetadata{}, false
	}
	defer func() { _ = stream.Close() }()

	var (
		last dumpCompletionMetadata
		ok   bool
	)
	sc := bufio.NewScanner(io.LimitReader(stream, 64*1024))
	for sc.Scan() {
		if meta, hit := parseDumpCompleteLine(sc.Text()); hit {
			last = meta
			ok = true
		}
	}
	return last, ok
}

func ptr64(v int64) *int64 { return &v }

// emitTerminalMetrics updates the backup Prometheus metrics for a single
// terminal MysqlBackup observation. Called from the non-short-circuit
// branch of the reconcile loop, so it runs at most once per terminal CR.
func (r *MysqlBackupReconciler) emitTerminalMetrics(backup *v1alpha1.MysqlBackup, job *batchv1.Job) {
	group := backup.Spec.FailoverGroupRef.Name
	profile := backup.Spec.ProfileName
	result := "failure"
	if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded {
		result = "success"
	}

	bmetrics.BackupRunsTotal.WithLabelValues(group, profile, result).Inc()

	// Duration: prefer explicit StartTime/CompletionTime on the CR;
	// fall back to Job fields.
	var start, end time.Time
	if backup.Status.StartTime != nil {
		start = backup.Status.StartTime.Time
	} else if job.Status.StartTime != nil {
		start = job.Status.StartTime.Time
	}
	if backup.Status.CompletionTime != nil {
		end = backup.Status.CompletionTime.Time
	}
	if !start.IsZero() && !end.IsZero() && end.After(start) {
		bmetrics.BackupDurationSeconds.WithLabelValues(group, profile).
			Observe(end.Sub(start).Seconds())
	}

	if !end.IsZero() {
		bmetrics.BackupLastAttemptTimestamp.WithLabelValues(group, profile).
			Set(float64(end.Unix()))
	}
	if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded {
		if !end.IsZero() {
			bmetrics.BackupLastSuccessTimestamp.WithLabelValues(group, profile).
				Set(float64(end.Unix()))
		}
		if backup.Status.SizeBytes > 0 {
			bmetrics.BackupLastSizeBytes.WithLabelValues(group, profile).
				Set(float64(backup.Status.SizeBytes))
		}
	}
}

// maybeWarnInFlightFailover emits an InFlightFailover warning event
// when the failover group's active site drifted after this backup's
// Job was created. The artifact is still a valid point-in-time snapshot
// of the original source site — this is a soft signal to the operator,
// not a failure condition.
func (r *MysqlBackupReconciler) maybeWarnInFlightFailover(backup *v1alpha1.MysqlBackup, fg *v1alpha1.MysqlFailoverGroup) {
	if backup.Status.ActiveSiteAtStart == "" {
		return
	}
	if fg.Status.ActiveSite == "" {
		return
	}
	if fg.Status.ActiveSite == backup.Status.ActiveSiteAtStart {
		return
	}
	r.Recorder.Eventf(backup, corev1.EventTypeWarning, "InFlightFailover",
		"failover group active site drifted during backup (started=%s, current=%s); artifact remains a valid snapshot of the source site",
		backup.Status.ActiveSiteAtStart, fg.Status.ActiveSite)
}

// pruneRetention deletes old MysqlBackup CRs for the same (failover
// group, profile). List happens in-namespace rather than via a label
// selector so the sweep works for ad-hoc CRs that may not carry the
// canonical labels. Successes go through the combined count/age/
// min-keep policy; failures go through a simple count cap.
func (r *MysqlBackupReconciler) pruneRetention(ctx context.Context, trigger *v1alpha1.MysqlBackup) error {
	var list v1alpha1.MysqlBackupList
	if err := r.List(ctx, &list, client.InNamespace(trigger.Namespace)); err != nil {
		return err
	}

	filtered := list.Items[:0]
	for _, b := range list.Items {
		if b.Spec.FailoverGroupRef.Name != trigger.Spec.FailoverGroupRef.Name {
			continue
		}
		if b.Spec.ProfileName != trigger.Spec.ProfileName {
			continue
		}
		if !b.DeletionTimestamp.IsZero() {
			continue
		}
		filtered = append(filtered, b)
	}

	// Resolve the effective policy from the (possibly nil) profile.
	var profile *v1alpha1.BackupProfile
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: trigger.Namespace, Name: trigger.Spec.FailoverGroupRef.Name}, &fg); err == nil {
		profile = findProfile(&fg, trigger.Spec.ProfileName)
	}
	count, maxAge, minKeep, maxFailed := resolveRetention(profile)

	if err := r.pruneSuccessful(ctx, filtered, count, maxAge, minKeep); err != nil {
		return err
	}
	return r.pruneByPhase(ctx, filtered, v1alpha1.BackupPhaseFailed, int(maxFailed))
}

// resolveRetention dispatches on BackupProfile.RetentionPolicy vs. the
// shorthand Retention field and returns the effective policy knobs:
//
//	count   = max successful CRs to keep (0 disables count-based)
//	maxAge  = max age of a successful CR (0 disables age-based)
//	minKeep = safety floor: this many newest successes always kept
//	maxFailed = cap on the Failed bucket
//
// The shorthand path applies a MinKeep=1 safety floor so a retention
// sweep after a long outage can't wipe the last good backup.
func resolveRetention(profile *v1alpha1.BackupProfile) (count int32, maxAge time.Duration, minKeep int32, maxFailed int32) {
	if profile == nil {
		return 0, 0, 0, int32(maxFailedRetention)
	}
	if profile.RetentionPolicy != nil {
		p := profile.RetentionPolicy
		mk := p.MinKeep
		if mk < 0 {
			mk = 0
		}
		mf := p.MaxFailedKeep
		if mf == 0 {
			mf = int32(maxFailedRetention)
		}
		return p.Count, time.Duration(p.MaxAgeDays) * 24 * time.Hour, mk, mf
	}
	// Shorthand: single-count retention with a MinKeep=1 safety floor.
	return profile.Retention, 0, 1, int32(maxFailedRetention)
}

// pruneSuccessful applies the combined count/age/min-keep retention
// policy to the Succeeded bucket of a filtered MysqlBackup slice. A CR
// is kept when ANY of the following are true:
//
//   - its rank from newest is < MinKeep (safety floor),
//   - count > 0 and its rank from newest is < count, or
//   - maxAge > 0 and its completion time is within the age window.
//
// When both count and maxAge are zero the function is a no-op so users
// who set neither don't accidentally wipe their history.
func (r *MysqlBackupReconciler) pruneSuccessful(ctx context.Context, all []v1alpha1.MysqlBackup, count int32, maxAge time.Duration, minKeep int32) error {
	if count == 0 && maxAge == 0 {
		return nil
	}
	bucket := make([]v1alpha1.MysqlBackup, 0, len(all))
	for _, b := range all {
		if b.Status.Phase == v1alpha1.BackupPhaseSucceeded {
			bucket = append(bucket, b)
		}
	}
	if len(bucket) == 0 {
		return nil
	}
	sort.Slice(bucket, func(i, j int) bool {
		ti := timeOrZero(bucket[i].Status.CompletionTime)
		tj := timeOrZero(bucket[j].Status.CompletionTime)
		return ti.After(tj)
	})
	cutoff := time.Now().Add(-maxAge)
	for i, b := range bucket {
		// Safety floor.
		if int32(i) < minKeep {
			continue
		}
		// Within the count window.
		if count > 0 && int32(i) < count {
			continue
		}
		// Within the age window.
		if maxAge > 0 {
			t := timeOrZero(b.Status.CompletionTime)
			if !t.IsZero() && t.After(cutoff) {
				continue
			}
		}
		// Not kept by any policy check => delete.
		victim := b
		if err := r.Delete(ctx, &victim); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete old succeeded backup %s: %w", victim.Name, err)
		}
	}
	return nil
}

// pruneByPhase keeps the newest `keep` entries in the given phase and
// deletes the rest. Entries without a CompletionTime sort last.
func (r *MysqlBackupReconciler) pruneByPhase(ctx context.Context, all []v1alpha1.MysqlBackup, phase v1alpha1.BackupPhase, keep int) error {
	if keep <= 0 {
		return nil
	}
	bucket := make([]v1alpha1.MysqlBackup, 0, len(all))
	for _, b := range all {
		if b.Status.Phase == phase {
			bucket = append(bucket, b)
		}
	}
	if len(bucket) <= keep {
		return nil
	}
	sort.Slice(bucket, func(i, j int) bool {
		ti := timeOrZero(bucket[i].Status.CompletionTime)
		tj := timeOrZero(bucket[j].Status.CompletionTime)
		return ti.After(tj)
	})
	for i := keep; i < len(bucket); i++ {
		victim := bucket[i]
		if err := r.Delete(ctx, &victim); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete old %s backup %s: %w", phase, victim.Name, err)
		}
	}
	return nil
}

func timeOrZero(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}

// selectSourceSite picks the MySQL site to dump from. Replica first,
// primary fallback. Returns the chosen site name and a short reason
// string for events. `override` wins when set and the named site exists.
func selectSourceSite(fg *v1alpha1.MysqlFailoverGroup, override string, maxLagSeconds int64) (string, string, error) {
	if fg == nil || len(fg.Status.Sites) == 0 {
		return "", "", fmt.Errorf("failover group has no observed site status yet")
	}
	status := &fg.Status

	if override != "" {
		specSite := fg.Spec.SiteByName(override)
		if specSite == nil {
			return "", "", fmt.Errorf("sourceSiteOverride %q does not match any configured site", override)
		}
		if specSite.IsReadOnlyReader() {
			return "", "", fmt.Errorf("sourceSiteOverride %q names a read-only site, which cannot be a backup source", override)
		}
		for i := range status.Sites {
			if status.Sites[i].Name == override {
				return override, "override", nil
			}
		}
		return "", "", fmt.Errorf("sourceSiteOverride %q does not match any observed site", override)
	}

	if status.ActiveSite == "" {
		return "", "", fmt.Errorf("failover group has no active site yet")
	}

	var primary *v1alpha1.SiteStatus
	var replicas []*v1alpha1.SiteStatus
	for i := range status.Sites {
		s := &status.Sites[i]
		if s.Name == status.ActiveSite {
			primary = s
			continue
		}
		specSite := fg.Spec.SiteByName(s.Name)
		if specSite != nil && !specSite.IsReadOnlyReader() {
			replicas = append(replicas, s)
		}
	}

	for _, replica := range replicas {
		if replica.State == "read-only" && replica.Replicating {
			if replica.SecondsBehindSource == nil || *replica.SecondsBehindSource <= maxLagSeconds {
				return replica.Name, "replica-preferred", nil
			}
		}
	}

	primarySpec := fg.Spec.SiteByName(status.ActiveSite)
	if primary != nil && primary.State == "writable" && primarySpec != nil && primarySpec.IsPromotable() {
		return primary.Name, "primary-fallback", nil
	}

	return "", "", fmt.Errorf("no healthy source (primary=%s)", siteStateString(primary))
}

func siteStateString(s *v1alpha1.SiteStatus) string {
	if s == nil {
		return "nil"
	}
	return fmt.Sprintf("%s/%s", s.Name, s.State)
}
