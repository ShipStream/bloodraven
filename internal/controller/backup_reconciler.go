package controller

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
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
	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
)

// maxFailedRetention caps the number of Failed MysqlBackup CRs kept per
// (group, profile) pair. Prevents unbounded growth when backups repeatedly
// fail against a misconfigured profile.
const maxFailedRetention = 10

// maxBackupPendingDuration caps how long a MysqlBackup waits for a healthy
// source before it is failed terminally. A missing source is normally
// transient (failover / ordered update); if it persists, the CR must stop
// occupying the schedule and become eligible for failed-backup pruning.
const maxBackupPendingDuration = 30 * time.Minute

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

	// APIReader is an uncached reader for retention and cleanup decisions
	// that must see verification holds as they are now, not as the
	// informer cache last saw them. SetupWithManager defaults it to
	// mgr.GetAPIReader(); unit tests leave it nil and read the client.
	APIReader client.Reader

	// now is the clock for wait deadlines; nil means time.Now.
	now func() time.Time
}

func (r *MysqlBackupReconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch

func (r *MysqlBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("mysqlbackup", req.NamespacedName)

	var backup v1alpha1.MysqlBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion with finalizer. See finalize for the contract.
	if !backup.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&backup, v1alpha1.MysqlBackupFinalizer) {
			done, requeue, err := r.finalize(ctx, &backup)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("finalize mysqlbackup: %w", err)
			}
			if !done {
				if requeue <= 0 {
					requeue = 15 * time.Second
				}
				return ctrl.Result{RequeueAfter: requeue}, nil
			}
			controllerutil.RemoveFinalizer(&backup, v1alpha1.MysqlBackupFinalizer)
			if err := r.Update(ctx, &backup); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&backup, v1alpha1.MysqlBackupFinalizer) {
		controllerutil.AddFinalizer(&backup, v1alpha1.MysqlBackupFinalizer)
		// Also stamp the canonical labels on first encounter so label
		// selectors work even for ad-hoc CRs created by `kubectl create`.
		_ = ensureBackupLabels(&backup)
		if err := r.Update(ctx, &backup); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Retry launch metadata is persisted with Create, before any status write.
	if backup.Annotations[v1alpha1.BackupAttemptAnnotation] != "" && backup.Status.Attempt != effectiveBackupAttempt(&backup) {
		patch := client.MergeFrom(backup.DeepCopy())
		backup.Status.Attempt = effectiveBackupAttempt(&backup)
		if err := r.Status().Patch(ctx, &backup, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("restore retry attempt: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Recover completion metadata before retention pruning.
	if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded || backup.Status.Phase == v1alpha1.BackupPhaseFailed {
		var recovery ctrl.Result
		if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded {
			var err error
			recovery, err = r.repairTerminalAttribution(ctx, &backup)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := r.pruneRetention(ctx, &backup); err != nil {
			logger.Error(err, "retention prune")
		}
		return recovery, nil
	}

	// Resolve the referenced failover group.
	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.FailoverGroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failBackup(ctx, &backup, v1alpha1.ReasonGroupNotFound,
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

	// Look up the Job before resolving any mutable launch-time inputs. Once a
	// Job exists its source and storage configuration are pinned in its pod
	// template. Re-running source selection here used to make an unrelated
	// failover terminally fail an already-running backup whenever
	// status.activeSite was briefly empty.
	var job batchv1.Job
	jobName := backupJobName(backup.Name)
	jobKey := types.NamespacedName{Namespace: backup.Namespace, Name: jobName}
	jobErr := r.Get(ctx, jobKey, &job)
	if jobErr != nil && !apierrors.IsNotFound(jobErr) {
		return ctrl.Result{}, fmt.Errorf("get job: %w", jobErr)
	}
	if jobErr == nil && !metav1.IsControlledBy(&job, &backup) {
		// Never adopt another object's Job, e.g. a legacy cleanup Job
		// named "mysqlbackup-<other>-cleanup" for a backup "<other>-cleanup",
		// or a Job orphaned by an earlier backup of the same name. Wait in
		// Pending, which fails the backup after maxBackupPendingDuration.
		return r.pendBackup(ctx, &backup, v1alpha1.ReasonBackupJobConflict,
			fmt.Sprintf("Job %s exists but is not owned by this MysqlBackup; waiting for it to be removed", job.Name))
	}

	// The live profile is needed to launch a Job. An existing Job remains
	// observable even if the profile changes or is removed while it runs.
	profile := findProfile(&fg, backup.Spec.ProfileName)

	if apierrors.IsNotFound(jobErr) {
		if profile == nil {
			return r.failBackup(ctx, &backup, v1alpha1.ReasonProfileNotFound,
				fmt.Sprintf("profile %q not found on MysqlFailoverGroup %q",
					backup.Spec.ProfileName, fg.Name))
		}

		// Pick the source site once (replica-first, primary fallback). With no
		// explicit override, an unavailable topology is transient: keep the CR
		// Pending and retry instead of turning an ordinary failover window into
		// a terminal backup failure.
		maxLag := int64(300)
		if fg.Spec.Backup != nil && fg.Spec.Backup.MaxLagSecondsForSource != nil {
			maxLag = *fg.Spec.Backup.MaxLagSecondsForSource
		}
		sourceSite, reason, err := selectSourceSite(&fg, backup.Spec.SourceSite, maxLag)
		if err != nil {
			if backup.Spec.SourceSite != "" {
				return r.failBackup(ctx, &backup, v1alpha1.ReasonNoHealthySource, err.Error())
			}
			return r.pendBackup(ctx, &backup, v1alpha1.ReasonNoHealthySource, err.Error())
		}

		// A read-only profile (D-10, Δ23: the initFromBackup archive
		// before the restore succeeded, or an archive another group
		// created) is never written by a backup.
		if readOnly, why := pitrProfileReadOnly(&fg, profile.Name); readOnly {
			return r.failBackup(ctx, &backup, v1alpha1.ReasonProfileReadOnly, why)
		}

		// The stamp names the archive the group's writers render (K5),
		// never one nobody writes. While the site Deployments do not
		// agree on one, only a backup of the active PITR profile during
		// a roll waits (it becomes that archive's baseline once the roll
		// ends); every other backup runs without an archive stamp (F-B):
		// a dump-only profile's schedule must never stall on PITR, and a
		// group whose Deployments carry no render yet (the pre-#203
		// deferral) stays that way until the user acts.
		deploys, err := listPITRSiteDeployments(ctx, r.apiReader(), &fg)
		if err != nil {
			return ctrl.Result{}, err
		}
		archiveID, agree, why := renderedWriterArchiveID(&fg, deploys)
		if !agree {
			if pitrBackupWaitsForRollout(&fg, profile.Name, deploys) {
				return r.pendBackup(ctx, &backup, v1alpha1.ReasonPITRArchiveRolloutPending, why)
			}
			archiveID = ""
			r.Recorder.Eventf(&backup, corev1.EventTypeNormal, v1alpha1.EventPITRArchiveNotStamped,
				"the backup is not stamped with a PITR archive and is no PITR baseline: %s", why)
		}

		// An encrypted backup gets its own immutable dump-key snapshot
		// before its Job (I-17): the Job mounts the snapshot, so a later
		// edit of the profile's Secret never changes the key this
		// backup's artifacts are encrypted with or read back with.
		dumpKey := ""
		if profile.EncryptionEnabled() {
			name, reason, message, err := r.ensureDumpKeySnapshot(ctx, &fg, &backup, profile)
			if err != nil {
				return ctrl.Result{}, err
			}
			if reason != "" {
				return r.pendBackup(ctx, &backup, reason, message)
			}
			dumpKey = name
		}

		// Ensure derived creds Secret (parses the group's DSN into
		// user/password) before creating the Job that mounts it.
		credsName := backupCredsSecretName(backup.Name)
		if err := r.ensureDerivedCredsSecret(ctx, &fg, &backup, credsName); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure creds secret: %w", err)
		}

		// Build & create.
		built, err := BuildBackupJob(BackupJobInputs{
			FailoverGroup:        &fg,
			Profile:              *profile,
			Backup:               &backup,
			SourceSite:           sourceSite,
			CredsSecretName:      credsName,
			ScriptsConfigMapName: backupScriptsConfigMapName(fg.Name),
			DumpKeySecretName:    dumpKey,
			PITRArchiveID:        archiveID,
		})
		if err != nil {
			return r.failBackup(ctx, &backup, v1alpha1.ReasonBuildJobFailed, err.Error())
		}
		if err := controllerutil.SetControllerReference(&backup, built, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set job owner ref: %w", err)
		}
		if err := r.Create(ctx, built); err != nil {
			return ctrl.Result{}, fmt.Errorf("create job: %w", err)
		}
		r.Recorder.Eventf(&backup, corev1.EventTypeNormal, v1alpha1.EventBackupStarted,
			"created Job %s targeting site %s (reason=%s)", built.Name, sourceSite, reason)

		now := metav1.Now()
		patch := client.MergeFrom(backup.DeepCopy())
		backup.Status.Phase = v1alpha1.BackupPhaseRunning
		backup.Status.StartTime = &now
		backup.Status.JobName = built.Name
		backup.Status.SourceSite = sourceSite
		backup.Status.StorageType = profile.Storage.Type
		if storage, err := artifactStorageFromJob(built); err == nil {
			backup.Status.ArtifactStorage = storage
		}
		backup.Status.MysqlImage = mysqlImageFor(&fg)
		backup.Status.ActiveSiteAtStart = fg.Status.ActiveSite
		backup.Status.Encrypted, backup.Status.EncryptionAlgorithm = backupJobEncryption(built)
		backup.Status.FailoverGroupUID, backup.Status.PITRArchiveID = backupJobIdentity(built)
		backup.Status.Attempt = effectiveBackupAttempt(&backup)
		backup.Status.Message = fmt.Sprintf("running (source=%s, reason=%s)", sourceSite, reason)
		backup.Status.ObservedGeneration = backup.Generation
		setCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionJobCreated,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: now,
			Reason:             v1alpha1.ReasonCreated,
			Message:            fmt.Sprintf("Job %s created", built.Name),
		})
		if err := r.Status().Patch(ctx, &backup, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch status after job create: %w", err)
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// The Job owns the source decision from here on. New Jobs persist it in
	// v1alpha1.SiteLabel as a crash-safe fallback for the narrow create-before-status
	// window; status.sourceSite covers Jobs created by older operators.
	sourceSite := backup.Status.SourceSite
	if sourceSite == "" {
		sourceSite = job.Labels[v1alpha1.SiteLabel]
	}

	// Job exists — observe its status.
	phase, message := jobPhase(&job, "backup")
	if phase == "" {
		// The status patch after the Job's create can be lost (F21);
		// the identity the Job pinned is recovered from it.
		if err := r.recoverBackupIdentity(ctx, &backup, &job); err != nil {
			return ctrl.Result{}, err
		}
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
	if uid, archiveID := backupJobIdentity(&job); metav1.IsControlledBy(&job, &backup) && next.FailoverGroupUID == "" && uid != "" {
		next.FailoverGroupUID, next.PITRArchiveID = uid, archiveID
	}
	next.ObservedGeneration = backup.Generation

	var metadataResult ctrl.Result
	if phase == v1alpha1.BackupPhaseSucceeded {
		var err error
		metadataResult, err = r.collectBackupCompletion(ctx, &backup, &job, &next)
		if err != nil {
			next.Phase = backup.Status.Phase
			return ctrl.Result{}, r.persistBackupMetadataFailure(ctx, &backup, &next, err)
		}
		next.Encrypted, next.EncryptionAlgorithm = backupJobEncryption(&job)
		if next.ArtifactStorage == nil || next.ArtifactStorage.Location == "" {
			// Jobs created before status.artifactStorage existed.
			if storage, err := artifactStorageFromJob(&job); err == nil {
				next.ArtifactStorage = storage
			}
		}
		setCondition(&next.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: completion,
			Reason:             v1alpha1.ReasonSucceeded,
			Message:            message,
		})
	} else {
		setCondition(&next.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: completion,
			Reason:             v1alpha1.ReasonFailed,
			Message:            message,
		})
	}

	// Exactly-once short-circuit.
	if backup.Status.Phase == phase && equality.Semantic.DeepEqual(&backup.Status, &next) {
		// Nothing to do — still run retention once though.
		if err := r.pruneRetention(ctx, &backup); err != nil {
			logger.Error(err, "retention prune after terminal state")
		}
		return metadataResult, nil
	}

	previous := backup.Status.DeepCopy()
	correctedEncryption := backup.Status.Encrypted && !next.Encrypted
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status = next
	if err := r.Status().Patch(ctx, &backup, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch final status: %w", err)
	}
	if correctedEncryption {
		r.Recorder.Event(&backup, corev1.EventTypeWarning, v1alpha1.EventEncryptionAttributionCorrected,
			"corrected backup encryption attribution from its producing Job")
	}

	r.recordIncompleteBackupMetadata(&backup, previous)
	r.emitTerminalMetrics(&backup, &job)

	if phase == v1alpha1.BackupPhaseSucceeded {
		r.Recorder.Eventf(&backup, corev1.EventTypeNormal, v1alpha1.EventBackupSucceeded,
			"backup %s completed: %s", backup.Name, message)
	} else {
		r.Recorder.Eventf(&backup, corev1.EventTypeWarning, v1alpha1.EventBackupFailed,
			"backup %s failed: %s", backup.Name, message)
	}

	if err := r.pruneRetention(ctx, &backup); err != nil {
		logger.Error(err, "retention prune after terminal state")
	}

	return metadataResult, nil
}

// recoverBackupIdentity copies the source group UID and PITR archive ID a
// running Job pinned into a status that lost them. Only the backup's own
// Job is trusted, and recorded values are never replaced.
func (r *MysqlBackupReconciler) recoverBackupIdentity(ctx context.Context, backup *v1alpha1.MysqlBackup, job *batchv1.Job) error {
	uid, archiveID := backupJobIdentity(job)
	if uid == "" || backup.Status.FailoverGroupUID != "" || !metav1.IsControlledBy(job, backup) {
		return nil
	}
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status.FailoverGroupUID, backup.Status.PITRArchiveID = uid, archiveID
	if backup.Status.JobName == "" {
		backup.Status.JobName = job.Name
	}
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		return fmt.Errorf("patch backup identity recovered from job: %w", err)
	}
	return nil
}

// backupJobEncryption reads the launch-time encryption choice. The label also
// covers older Jobs; the uploader layout and its env pin the algorithm.
func backupJobEncryption(job *batchv1.Job) (bool, string) {
	encrypted := job.Labels[v1alpha1.BackupEncryptedLabel] == "true"
	algorithm := ""
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Name != backupEncryptUploadContainerName {
			continue
		}
		encrypted = true
		for _, env := range container.Env {
			if env.Name == "BLOODRAVEN_ENCRYPTION_ALGORITHM" {
				algorithm = env.Value
			}
		}
	}
	if !encrypted {
		return false, ""
	}
	return true, string((&v1alpha1.BackupEncryptionSpec{Algorithm: v1alpha1.EncryptionAlgorithm(algorithm)}).AlgorithmOrDefault())
}

func (r *MysqlBackupReconciler) repairTerminalAttribution(ctx context.Context, backup *v1alpha1.MysqlBackup) (ctrl.Result, error) {
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backupJobName(backup.Name)}, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&job, backup) {
		return ctrl.Result{}, nil
	}
	phase, _ := jobPhase(&job, "backup")
	if backup.Status.Phase != v1alpha1.BackupPhaseSucceeded && phase != v1alpha1.BackupPhaseSucceeded {
		return ctrl.Result{}, nil
	}
	next := *backup.Status.DeepCopy()
	next.Encrypted, next.EncryptionAlgorithm = backupJobEncryption(&job)
	_, expected, destinationErr := backupJobDestination(&job)
	_, raw, _ := backupJobRawDestination(&job)
	unexpanded := raw != "" && next.Location == raw && (destinationErr != nil || raw != expected) &&
		backupLocationFromJobFallback(&next)
	var recovery ctrl.Result
	if metadataLocationNeedsRepair(next.Location, expected) || unexpanded || r.Clientset != nil && completionMetadataRetryable(&next) {
		if next.CompletionTime == nil {
			next.CompletionTime = stableJobCompletionTime(&job)
		}
		var err error
		recovery, err = r.collectBackupCompletion(ctx, backup, &job, &next)
		if err != nil {
			if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded && errors.Is(err, errBackupDestinationUnavailable) {
				return ctrl.Result{}, r.persistBackupMetadataStatus(ctx, backup, &next, err)
			}
			return ctrl.Result{}, r.persistBackupMetadataFailure(ctx, backup, &next, err)
		}
		if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded && !apiMeta.IsStatusConditionTrue(next.Conditions, v1alpha1.ConditionReady) {
			setCondition(&next.Conditions, metav1.Condition{
				Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue,
				ObservedGeneration: backup.Generation, LastTransitionTime: metav1.Now(),
				Reason: v1alpha1.ReasonSucceeded, Message: "backup artifact destination available",
			})
		}
	}
	if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded && (next.ArtifactStorage == nil || next.ArtifactStorage.Location == "") {
		// Backups that succeeded before status.artifactStorage existed get
		// their destination now, while the producing Job certainly exists,
		// rather than at deletion, when a foreground cascade may already
		// have removed it.
		if storage, err := artifactStorageFromJob(&job); err == nil {
			next.ArtifactStorage = storage
		}
	}
	if uid, archiveID := backupJobIdentity(&job); next.FailoverGroupUID == "" && uid != "" {
		// A succeeded backup whose status lost its source identity (a
		// partial status edit, a rollback to a release without the
		// field) gets it back from the Job that pinned it, as a running
		// one does in recoverBackupIdentity. PITR refuses the backup
		// PITRBackupIdentityUnknown until then.
		next.FailoverGroupUID, next.PITRArchiveID = uid, archiveID
	}
	if equality.Semantic.DeepEqual(&backup.Status, &next) {
		return recovery, nil
	}
	previous := backup.Status.DeepCopy()
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status = next
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("repair backup completion metadata: %w", err)
	}
	if previous.Encrypted != next.Encrypted || previous.EncryptionAlgorithm != next.EncryptionAlgorithm {
		r.Recorder.Event(backup, corev1.EventTypeWarning, v1alpha1.EventEncryptionAttributionCorrected,
			"corrected backup encryption attribution from its producing Job")
	}
	updateRecoveredBackupSize(backup, previous)
	r.recordIncompleteBackupMetadata(backup, previous)
	return recovery, nil
}

// SetupWithManager registers the reconciler with the manager.
func (r *MysqlBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

// finalize deletes the dump artifact for a MysqlBackup before the CR is
// released:
//
//	(true,  _, nil) => may remove the finalizer
//	(false, d, nil) => cleanup running, retrying or blocked; requeue after d
//	(false, _, err) => API error, back off
//
// Two modes:
//
//   - Normal: the backup succeeded and status.location names its
//     artifact. A failed cleanup Job is a state, not an error: the
//     ArtifactCleanup condition names the cause and a fresh Job runs after
//     a fixed delay, indefinitely.
//   - Abandoned: the backup never succeeded, so status.location is empty,
//     but status.artifactStorage (recorded at launch, or recovered from
//     the producing Job) names the planned destination its Job wrote a
//     stamp and perhaps part of a dump to. The producer is stopped first;
//     cleanup is then best effort and never retried: a failed or
//     impossible cleanup releases the finalizer with an
//     ArtifactCleanupSkipped warning.
//
// When the referenced group or profile is gone we emit an
// ArtifactCleanupSkipped warning event and release the finalizer
// immediately (after an abandoned backup's producer has stopped) —
// there's nothing we can do without credentials.
func (r *MysqlBackupReconciler) finalize(ctx context.Context, backup *v1alpha1.MysqlBackup) (bool, time.Duration, error) {
	// Recover a succeeded backup's location before an empty-location
	// finalizer can release it. A backup whose Job failed keeps an empty
	// status.location; its planned destination is status.artifactStorage
	// (abandoned mode below).
	if _, err := r.repairTerminalAttribution(ctx, backup); err != nil && !errors.Is(err, errBackupDestinationUnavailable) {
		return false, 0, err
	}
	// A hold whose verification can no longer read (its finalizer was
	// removed by hand, say) would keep the PITR cutoff pinned for as long
	// as cleanup stays blocked. Drop it first.
	if stripped, err := r.stripDeadHolds(ctx, backup); err != nil {
		return false, 0, err
	} else if !stripped {
		return false, 2 * time.Second, nil
	}
	if backup.Status.Location == "" && backup.Status.Phase == v1alpha1.BackupPhaseSucceeded &&
		backup.Status.ArtifactStorage != nil && backup.Status.ArtifactStorage.Location != "" {
		// status.location was blanked after the destination was recorded.
		// The immutable snapshot still names the artifact; restore it
		// rather than release the finalizer and orphan the artifact.
		patch := client.MergeFrom(backup.DeepCopy())
		backup.Status.Location = backup.Status.ArtifactStorage.Location
		if err := r.Status().Patch(ctx, backup, patch); err != nil {
			return false, 0, fmt.Errorf("restore artifact location: %w", err)
		}
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactLocationRestored,
			"status.location was empty; restored %q from status.artifactStorage for cleanup", backup.Status.Location)
	}
	// A backup that never succeeded has no status.location, but its Job
	// may have written a backup stamp (and perhaps part of a dump) at the
	// planned destination recorded in status.artifactStorage, or, when the
	// status patch after the Job's create was lost, named by the Job
	// itself. Left behind, the stamp makes a re-created backup of the same
	// name refuse ErrBackupStampConflict (review B1), so it is cleaned up
	// in abandoned mode: only when the stamp there names this backup's
	// UID, and best effort, since the re-created backup's own stamp step
	// also replaces an abandoned stamp once no dump objects remain.
	//
	// Every abandoned-mode release, including the best-effort skips,
	// waits until the producing Job is stopped and none of its pods can
	// still write: until the finalizer is gone no backup of the same name
	// can be created, and that is what keeps a replacement's stamp step
	// from reusing a destination the old uploader still writes to.
	abandoned := false
	location := backup.Status.Location
	var producer *batchv1.Job
	if location == "" {
		condition := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionMetadataComplete)
		if backup.Status.Phase == v1alpha1.BackupPhaseSucceeded || condition != nil && condition.Reason == v1alpha1.ReasonDestinationUnavailable {
			r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupSkipped,
				"no recorded artifact destination (producing Job %q); skipping cleanup%s", backupJobName(backup.Name), dumpKeyKeptNote(backup))
			return true, 0, nil
		}
		if backup.UID == "" {
			return true, 0, nil
		}
		abandoned = true
		var err error
		if producer, err = r.ownedBackupJob(ctx, backup); err != nil {
			return false, 0, err
		}
		if backup.Status.ArtifactStorage == nil || backup.Status.ArtifactStorage.Location == "" {
			if producer == nil {
				// No Job ever ran, or it is gone with nothing recorded.
				// Its pods may outlive it; wait for them all the same.
				if wait, err := r.stopAbandonedProducer(ctx, backup, nil); err != nil || wait > 0 {
					return false, wait, err
				}
				// No recorded Job is no proof that none ran: a crash
				// between the Job's create and the status patch leaves
				// status empty, and that Job may have uploaded encrypted
				// artifacts before it went (foreground deletion removes
				// it). Without attribution nothing confirms the artifacts
				// are gone, so the dump-key snapshot, possibly their only
				// key, is kept and named (G2, Δ24).
				kept, err := r.dumpKeySnapshotKept(ctx, backup)
				if err != nil {
					return false, 0, err
				}
				if kept != "" {
					r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupSkipped,
						"no producing Job or artifact destination is recorded, so nothing is cleaned up; the dump-key snapshot Secret %q is kept because encrypted artifacts of this backup may exist (delete it once you have confirmed none remain)", kept)
				}
				return true, 0, nil
			}
			// The status patch after the Job's create was lost: the Job
			// (failed or still running) names what it wrote.
			recovered, err := artifactStorageFromJob(producer)
			if err != nil {
				if wait, err := r.stopAbandonedProducer(ctx, backup, producer); err != nil || wait > 0 {
					return false, wait, err
				}
				return r.skipAbandonedCleanup(backup, backupJobName(backup.Name),
					fmt.Sprintf("cannot recover the planned destination from Job %q: %v", producer.Name, err))
			}
			patch := client.MergeFrom(backup.DeepCopy())
			backup.Status.ArtifactStorage = recovered
			if err := r.Status().Patch(ctx, backup, patch); err != nil {
				return false, 0, fmt.Errorf("record recovered artifact storage: %w", err)
			}
		}
		location = backup.Status.ArtifactStorage.Location
		// The producer's destination check needs the Job, which stopping
		// deletes: run it now and keep a mismatch on the condition.
		if problem := artifactIdentityProblem(backup, location, backup.Status.ArtifactStorage, producer); problem != "" {
			if err := r.setArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonDestinationMismatch, problem); err != nil {
				return false, 0, err
			}
		}
		// The producing Job may still be writing (a Running backup being
		// deleted). Stop it, and wait for its pods to be gone, before
		// anything is released or deleted.
		if wait, err := r.stopAbandonedProducer(ctx, backup, producer); err != nil || wait > 0 {
			return false, wait, err
		}
		if previous := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup); previous != nil &&
			previous.Reason == v1alpha1.ReasonDestinationMismatch {
			return r.skipAbandonedCleanup(backup, location, previous.Message)
		}
	}
	// block reports a cleanup that cannot start. An abandoned artifact's
	// cleanup is best effort and never holds the deletion once its
	// producer has stopped.
	block := func(reason, message string, requeue time.Duration) (bool, time.Duration, error) {
		if abandoned {
			return r.skipAbandonedCleanup(backup, location, message)
		}
		return r.blockArtifactCleanup(ctx, backup, reason, message, requeue)
	}

	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.FailoverGroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupSkipped,
				"failover group %q gone; cannot clean up artifact at %q%s", fgKey.Name, location, dumpKeyKeptNote(backup))
			return true, 0, nil
		}
		return false, 0, fmt.Errorf("get failover group: %w", err)
	}

	profile := findProfile(&fg, backup.Spec.ProfileName)
	if profile == nil {
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupSkipped,
			"profile %q gone from group %q; cannot clean up artifact at %q%s",
			backup.Spec.ProfileName, fg.Name, location, dumpKeyKeptNote(backup))
		return true, 0, nil
	}

	// The destination comes from the producing Job, never from the live
	// profile: a bucket, endpoint, claim or prefix edited since the backup
	// ran must not redirect a recursive delete.
	if !abandoned {
		var err error
		if producer, err = r.ownedBackupJob(ctx, backup); err != nil {
			return false, 0, err
		}
	}
	storage := backup.Status.ArtifactStorage
	if storage == nil || storage.Location == "" {
		if producer == nil {
			return r.blockArtifactCleanup(ctx, backup, v1alpha1.ReasonDestinationUnverified,
				fmt.Sprintf("no recorded artifact destination and the producing Job %q is gone; inspect storage, delete the artifact at %q by hand, then remove finalizer %q",
					backupJobName(backup.Name), backup.Status.Location, v1alpha1.MysqlBackupFinalizer), 10*time.Minute)
		}
		recovered, err := artifactStorageFromJob(producer)
		if err != nil {
			return r.blockArtifactCleanup(ctx, backup, v1alpha1.ReasonDestinationUnverified,
				fmt.Sprintf("cannot recover the artifact destination from Job %q: %v", producer.Name, err), 10*time.Minute)
		}
		patch := client.MergeFrom(backup.DeepCopy())
		backup.Status.ArtifactStorage = recovered
		if err := r.Status().Patch(ctx, backup, patch); err != nil {
			return false, 0, fmt.Errorf("record recovered artifact storage: %w", err)
		}
		storage = recovered
	}
	if problem := artifactIdentityProblem(backup, location, storage, producer); problem != "" {
		return block(v1alpha1.ReasonDestinationMismatch, problem, 30*time.Minute)
	}

	cleanupJob, err := r.currentCleanupJob(ctx, backup)
	if err != nil {
		return false, 0, err
	}
	if cleanupJob == nil {
		// Foreground deletion makes the garbage collector delete every
		// dependent, including a cleanup Job created now. Wait until it
		// has finished that phase and dropped its finalizer.
		// Orphan deletion has the same window: a Job created before the
		// collector strips owner references would lose its owner and
		// block cleanup as somebody else's Job.
		switch {
		case controllerutil.ContainsFinalizer(backup, metav1.FinalizerDeleteDependents):
			if err := r.setArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonForegroundDeletion,
				"deleted with propagationPolicy=Foreground; cleanup starts after the garbage collector has removed this backup's dependents"); err != nil {
				return false, 0, err
			}
			return false, 10 * time.Second, nil
		case controllerutil.ContainsFinalizer(backup, metav1.FinalizerOrphanDependents):
			if err := r.setArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonOrphanDeletion,
				"deleted with propagationPolicy=Orphan; cleanup starts after the garbage collector has released this backup's dependents"); err != nil {
				return false, 0, err
			}
			return false, 10 * time.Second, nil
		}
		// A verification may still be reading the artifact (a manual
		// delete during a run, or a hold retention could not see).
		readers, err := artifactReaders(ctx, r.apiReader(), backup)
		if err != nil {
			return false, 0, err
		}
		if len(readers) > 0 {
			message := fmt.Sprintf("verification %s may still be reading the artifact", strings.Join(readers, ", "))
			previous := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
			if previous == nil || previous.Reason != v1alpha1.ReasonHeldByVerification || previous.Message != message {
				r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupDeferred, "cleanup waits: %s", message)
			}
			if err := r.setArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonHeldByVerification, message); err != nil {
				return false, 0, err
			}
			return false, 30 * time.Second, nil
		}
		// Never overlap executions: a retried Job's pods must be gone
		// before its replacement starts.
		if active, err := r.activeCleanupPods(ctx, backup); err != nil {
			return false, 0, err
		} else if active {
			return false, 5 * time.Second, nil
		}
		in := cleanupJobInputs{
			FailoverGroup: &fg,
			Profile:       profile,
			Backup:        backup,
			Storage:       storage,
		}
		if abandoned {
			in.Abandoned = true
			passphrase, missing, err := r.abandonedCleanupPassphrase(ctx, backup, profile, storage, producer)
			if err != nil {
				return false, 0, err
			}
			if missing != "" {
				// The recorded key is gone, but the artifacts it decrypts
				// may be there: the finalizer stays (no skip) until the
				// Secret is restored, so the leftovers are cleaned up then
				// (IMPL-203 §4.7, U11).
				return r.blockArtifactCleanup(ctx, backup, v1alpha1.ReasonPassphraseSecretMissing, missing, 5*time.Minute)
			}
			in.Passphrase = passphrase
		}
		built, err := buildCleanupJob(in)
		switch {
		case errors.Is(err, errOperatorImageUnset):
			return block(v1alpha1.ReasonOperatorImageUnset, err.Error(), 5*time.Minute)
		case errors.Is(err, errCleanupCredentialsMissing):
			return block(v1alpha1.ReasonCredentialsUnavailable,
				fmt.Sprintf("profile %q: %v", profile.Name, err), 5*time.Minute)
		case err != nil:
			// The recorded destination or storage type cannot be turned
			// into a Job (unknown type, snapshot without bucket or claim).
			return block(v1alpha1.ReasonCleanupJobInvalid, err.Error(), cleanupRetryAfterRejection)
		}
		if err := controllerutil.SetControllerReference(backup, built, r.Scheme); err != nil {
			return false, 0, fmt.Errorf("set cleanup job owner ref: %w", err)
		}
		if err := r.Create(ctx, built); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return false, 5 * time.Second, nil
			}
			return false, 0, fmt.Errorf("create cleanup job: %w", err)
		}
		r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupStarted,
			"created cleanup Job %s for artifact at %q", built.Name, location)
		return false, 15 * time.Second, nil
	}
	if !metav1.IsControlledBy(cleanupJob, backup) {
		return block(v1alpha1.ReasonCleanupJobConflict,
			fmt.Sprintf("Job %s is not owned by this MysqlBackup; waiting for it to be removed", cleanupJob.Name), 30*time.Second)
	}
	if cleanupJob.DeletionTimestamp != nil {
		return false, 5 * time.Second, nil
	}

	phase, _ := jobPhase(cleanupJob, "cleanup")
	switch phase {
	case v1alpha1.BackupPhaseSucceeded:
		pod, err := latestJobPod(ctx, r.Client, cleanupJob)
		if err != nil {
			return false, 0, fmt.Errorf("read cleanup verdict: %w", err)
		}
		_, message, _ := cleanupPodVerdict(pod)
		if reason, skipped := backupmeta.ParseCleanupSkipped(message); abandoned && skipped {
			// Nothing was deleted and the location may still hold this
			// backup's unproven artifacts: the dump-key snapshot stays
			// (K2, U05); the Event names it and the location.
			return r.skipAbandonedCleanup(backup, location, reason)
		}
		objects, counted := backupmeta.ParseCleanupComplete(message)
		switch {
		case abandoned:
			r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupSucceeded,
				"cleanup of the unfinished backup at %q removed %d objects (nothing is removed unless its backup stamp names this backup)", location, objects)
		case counted && objects == 0:
			r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupNothingFound,
				"no objects found at %q in the recorded destination; nothing was deleted", backup.Status.Location)
		case counted:
			r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupSucceeded,
				"artifact cleanup removed %d objects at %q", objects, backup.Status.Location)
		default:
			r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupSucceeded,
				"artifact cleanup succeeded for %q", backup.Status.Location)
		}
		// The artifacts are gone, so the key that decrypts them goes too
		// (Δ24), right before the finalizer is removed.
		if err := r.deleteDumpKeySnapshot(ctx, backup); err != nil {
			return false, 0, err
		}
		return true, 0, nil
	case v1alpha1.BackupPhaseFailed:
		if abandoned {
			pod, err := latestJobPod(ctx, r.Client, cleanupJob)
			if err != nil {
				return false, 0, fmt.Errorf("read cleanup verdict: %w", err)
			}
			_, message, _ := cleanupPodVerdict(pod)
			detail := strings.TrimSpace(strings.TrimPrefix(message, backupmeta.CleanupFailedPrefix))
			if detail == "" {
				_, detail = jobPhase(cleanupJob, "cleanup")
			}
			return r.skipAbandonedCleanup(backup, location, fmt.Sprintf("cleanup Job %s failed: %s", cleanupJob.Name, detail))
		}
		return r.retryFailedCleanup(ctx, backup, cleanupJob)
	default:
		// Running. Surface a container that cannot start (image pull,
		// configuration) instead of waiting out the deadline silently.
		pod, err := latestJobPod(ctx, r.Client, cleanupJob)
		if err != nil {
			return false, 0, fmt.Errorf("read cleanup pod: %w", err)
		}
		waiting := cleanupPodStuck(pod, time.Now())
		if created := cleanupJob.CreationTimestamp; pod == nil && !created.IsZero() && time.Since(created.Time) >= cleanupStartGrace {
			// The Job controller could not create a pod (ResourceQuota,
			// Pod Security admission): only the Job's events say why.
			waiting = fmt.Sprintf("no pod created since %s; check the Job's FailedCreate events (kubectl describe job %s)",
				created.UTC().Format(time.RFC3339), cleanupJob.Name)
		}
		if waiting != "" {
			// The text is stable for a given pod and cause, so this
			// writes only when the reason or the pod changes.
			if err := r.setArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonCleanupPending,
				fmt.Sprintf("cleanup Job %s: %s", cleanupJob.Name, waiting)); err != nil {
				return false, 0, err
			}
		} else if err := r.clearArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonCleanupPending); err != nil {
			return false, 0, err
		}
		return false, 15 * time.Second, nil
	}
}

// skipAbandonedCleanup gives up on an unfinished backup's leftovers and
// releases the finalizer: a backup that never succeeded did not hold a
// finalizer-protected artifact before backup stamps existed, and a
// re-created backup of the same name replaces an abandoned stamp itself
// once no dump objects remain at the location.
func (r *MysqlBackupReconciler) skipAbandonedCleanup(backup *v1alpha1.MysqlBackup, location, why string) (bool, time.Duration, error) {
	r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupSkipped,
		"cannot clean up what the unfinished backup left at %q (%s); a backup re-created with this name replaces its backup stamp once no dump objects remain there, otherwise delete %q and %q by hand%s",
		location, why, location, backupmeta.StampKey(location), dumpKeyKeptNote(backup))
	return true, 0, nil
}

// stopAbandonedProducer deletes the producing Job of a backup that never
// succeeded and returns a requeue until none of its pods is pending or
// running, so neither the abandoned cleanup nor a release (which lets a
// backup of the same name reuse the destination) races the dump.
//
// The wait is unbounded on purpose: a pod on an unreachable node stays
// Running or Terminating and may still be writing. Once it has been
// stopping for producerStopStuckAfter the ArtifactCleanup condition and a
// Warning Event name the pods and the remedy (force-delete them once the
// node is confirmed down; a force-deleted pod leaves the API and the wait
// ends).
func (r *MysqlBackupReconciler) stopAbandonedProducer(ctx context.Context, backup *v1alpha1.MysqlBackup, producer *batchv1.Job) (time.Duration, error) {
	if producer != nil && producer.DeletionTimestamp == nil {
		if phase, _ := jobPhase(producer, "backup"); phase == "" {
			background := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, producer, &client.DeleteOptions{PropagationPolicy: &background}); err != nil && !apierrors.IsNotFound(err) {
				return 0, fmt.Errorf("stop producing backup job: %w", err)
			}
			r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupDeferred,
				"stopping backup Job %s before cleaning up its unfinished artifact", producer.Name)
			return 5 * time.Second, nil
		}
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(backup.Namespace), client.MatchingLabels{
		v1alpha1.MysqlBackupLabel: backup.Name,
		v1alpha1.ResourceLabel:    "backup",
	}); err != nil {
		return 0, fmt.Errorf("list backup pods: %w", err)
	}
	if stuck := stuckProducerPods(pods.Items, backup.DeletionTimestamp, r.clock()); stuck != "" {
		message := fmt.Sprintf("backup pod %s has not stopped after %s; it may still write to the destination, so cleanup and release wait. If its node is unreachable and confirmed down, force-delete the pod (kubectl delete pod --force --grace-period=0)",
			stuck, producerStopStuckAfter)
		previous := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
		if previous == nil || previous.Reason != v1alpha1.ReasonProducerStopStuck || previous.Message != message {
			r.Recorder.Event(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupProducerStuck, message)
		}
		if err := r.setArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonProducerStopStuck, message); err != nil {
			return 0, err
		}
		return 30 * time.Second, nil
	}
	for i := range pods.Items {
		if phase := pods.Items[i].Status.Phase; phase != corev1.PodSucceeded && phase != corev1.PodFailed {
			return 5 * time.Second, nil
		}
	}
	if err := r.clearArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonProducerStopStuck); err != nil {
		return 0, err
	}
	return 0, nil
}

// producerStopStuckAfter is how long an abandoned backup's producer pods
// may take to stop before the wait is reported as stuck.
const producerStopStuckAfter = 10 * time.Minute

// stuckProducerPods returns the sorted names of the pods that are neither
// Succeeded nor Failed and have been stopping (since their own deletion,
// else since the backup's) for at least producerStopStuckAfter, or "".
func stuckProducerPods(pods []corev1.Pod, backupDeleted *metav1.Time, now time.Time) string {
	var stuck []string
	for i := range pods {
		pod := &pods[i]
		if phase := pod.Status.Phase; phase == corev1.PodSucceeded || phase == corev1.PodFailed {
			continue
		}
		since := backupDeleted
		if pod.DeletionTimestamp != nil && (since == nil || pod.DeletionTimestamp.Before(since)) {
			since = pod.DeletionTimestamp
		}
		if since != nil && now.Sub(since.Time) >= producerStopStuckAfter {
			stuck = append(stuck, pod.Name)
		}
	}
	sort.Strings(stuck)
	return strings.Join(stuck, ", ")
}

// abandonedCleanupPassphrase is the passphrase Secret an abandoned cleanup
// needs to read an encrypted backup's stamp (Δ17): the producing Job's own
// mount when the Job is still there (its dump-key snapshot), else the one
// recorded in status.artifactStorage when the Job launched (F025: a Job
// stopped by the deletion is gone by the time cleanup starts), which must
// exist: a missing one is reported as missing (the second return names
// it), never replaced by the profile's live Secret, whose bytes may differ
// (#203). Only a backup with no attribution at all falls back to the
// profile's Secret. nil for a plaintext backup.
func (r *MysqlBackupReconciler) abandonedCleanupPassphrase(ctx context.Context, backup *v1alpha1.MysqlBackup, profile *v1alpha1.BackupProfile, storage *v1alpha1.BackupArtifactStorage, producer *batchv1.Job) (*corev1.SecretVolumeSource, string, error) {
	encrypted := backup.Status.Encrypted
	recorded := (*v1alpha1.SecretKeyRef)(nil)
	if storage != nil {
		recorded = storage.PassphraseSecretKeyRef
	}
	if producer != nil {
		if e, _ := backupJobEncryption(producer); e {
			encrypted = true
		}
		for _, vol := range producer.Spec.Template.Spec.Volumes {
			if vol.Name == "backup-passphrase" && vol.Secret != nil {
				return vol.Secret.DeepCopy(), "", nil
			}
		}
	}
	if recorded != nil {
		var s corev1.Secret
		err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: recorded.Name}, &s)
		switch {
		case apierrors.IsNotFound(err):
			return nil, fmt.Sprintf("the recorded dump passphrase Secret %q is gone; restore it to clean up the encrypted artifact", recorded.Name), nil
		case err != nil:
			return nil, "", fmt.Errorf("get recorded passphrase secret %s: %w", recorded.Name, err)
		}
		return passphraseVolumeSource(recorded.Name, recorded.Key), "", nil
	}
	if !encrypted {
		return nil, "", nil
	}
	if profile == nil || profile.Encryption == nil || profile.Encryption.PassphraseSecretKeyRef.Name == "" {
		// buildCleanupJob refuses: the stamp cannot be read.
		return &corev1.SecretVolumeSource{}, "", nil
	}
	return passphraseVolumeSource(profile.Encryption.PassphraseSecretKeyRef.Name, profile.Encryption.PassphraseSecretKeyRef.PassphraseSecretKeyOrDefault()), "", nil
}

func passphraseVolumeSource(name, key string) *corev1.SecretVolumeSource {
	return &corev1.SecretVolumeSource{
		SecretName:  name,
		DefaultMode: ptr32(0o400),
		Items: []corev1.KeyToPath{{
			Key:  key,
			Path: backupPassphraseFileName,
		}},
	}
}

// currentCleanupJob returns the backup's cleanup Job under the current
// name, or a legacy-named one this backup controls, or nil. A legacy-named
// Job owned by anything else is another backup's Job and is ignored.
func (r *MysqlBackupReconciler) currentCleanupJob(ctx context.Context, backup *v1alpha1.MysqlBackup) (*batchv1.Job, error) {
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: cleanupJobName(backup.Name)}, &job)
	if err == nil {
		return &job, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get cleanup job: %w", err)
	}
	err = r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: legacyCleanupJobName(backup.Name)}, &job)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get legacy cleanup job: %w", err)
	}
	if !metav1.IsControlledBy(&job, backup) {
		return nil, nil
	}
	return &job, nil
}

// retryFailedCleanup records a failed cleanup Job once, then replaces it
// after cleanupRetryAfterFailure (transient or unknown failures, including
// deadline, eviction, OOM and crashes) or cleanupRetryAfterRejection
// (backupmeta.CleanupExitRejected).
func (r *MysqlBackupReconciler) retryFailedCleanup(ctx context.Context, backup *v1alpha1.MysqlBackup, job *batchv1.Job) (bool, time.Duration, error) {
	pod, err := latestJobPod(ctx, r.Client, job)
	if err != nil {
		return false, 0, fmt.Errorf("read cleanup verdict: %w", err)
	}
	code, message, terminated := cleanupPodVerdict(pod)
	reason, wait := v1alpha1.ReasonCleanupFailed, cleanupRetryAfterFailure
	if terminated && code == backupmeta.CleanupExitRejected {
		reason, wait = v1alpha1.ReasonCleanupRejected, cleanupRetryAfterRejection
	}
	detail := strings.TrimSpace(strings.TrimPrefix(message, backupmeta.CleanupFailedPrefix))
	if detail == "" {
		detail = cleanupPodStuck(pod, time.Now())
	}
	if detail == "" {
		_, detail = jobPhase(job, "cleanup")
	}
	if job.Annotations[v1alpha1.CleanupFailureRecordedAnnotation] == "" {
		// The optimistic lock makes the record exactly-once: a reconcile
		// working from a cached Job that predates the annotation conflicts
		// instead of counting the failure again.
		patch := client.MergeFromWithOptions(job.DeepCopy(), client.MergeFromWithOptimisticLock{})
		if job.Annotations == nil {
			job.Annotations = map[string]string{}
		}
		job.Annotations[v1alpha1.CleanupFailureRecordedAnnotation] = "true"
		if err := r.Patch(ctx, job, patch); err != nil {
			if apierrors.IsConflict(err) {
				return false, 2 * time.Second, nil
			}
			return false, 0, fmt.Errorf("mark cleanup job failure recorded: %w", err)
		}
		bmetrics.BackupArtifactCleanupFailuresTotal.WithLabelValues(backup.Spec.FailoverGroupRef.Name, backup.Spec.ProfileName, reason).Inc()
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupFailed,
			"artifact cleanup Job %s failed (%s): %s; a new Job runs %s after the failure",
			job.Name, reason, detail, wait)
	}
	if err := r.setArtifactCleanupCondition(ctx, backup, reason, fmt.Sprintf("cleanup Job %s failed: %s", job.Name, detail)); err != nil {
		return false, 0, err
	}
	failedAt := job.CreationTimestamp.Time
	if t := stableJobCompletionTime(job); t != nil {
		failedAt = t.Time
	}
	if remaining := time.Until(failedAt.Add(wait)); remaining > 0 {
		return false, remaining, nil
	}
	foreground := metav1.DeletePropagationForeground
	if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
		return false, 0, fmt.Errorf("delete failed cleanup job: %w", err)
	}
	r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupRetry,
		"replacing failed cleanup Job %s", job.Name)
	return false, 5 * time.Second, nil
}

// blockArtifactCleanup reports a cleanup that cannot start and requeues.
func (r *MysqlBackupReconciler) blockArtifactCleanup(ctx context.Context, backup *v1alpha1.MysqlBackup, reason, message string, requeue time.Duration) (bool, time.Duration, error) {
	previous := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	changed := previous == nil || previous.Reason != reason || previous.Message != message
	if err := r.setArtifactCleanupCondition(ctx, backup, reason, message); err != nil {
		return false, 0, err
	}
	if changed {
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupBlocked, "%s: %s", reason, message)
	}
	return false, requeue, nil
}

// clearArtifactCleanupCondition removes the ArtifactCleanup condition when it
// still carries reason, e.g. CleanupPending once the cleanup pod starts.
func (r *MysqlBackupReconciler) clearArtifactCleanupCondition(ctx context.Context, backup *v1alpha1.MysqlBackup, reason string) error {
	current := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if current == nil || current.Reason != reason {
		return nil
	}
	patch := client.MergeFrom(backup.DeepCopy())
	removeCondition(&backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if err := r.Status().Patch(ctx, backup, patch); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("clear %s condition: %w", v1alpha1.ConditionArtifactCleanup, err)
	}
	return nil
}

func (r *MysqlBackupReconciler) setArtifactCleanupCondition(ctx context.Context, backup *v1alpha1.MysqlBackup, reason, message string) error {
	previous := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if previous != nil && previous.Status == metav1.ConditionFalse && previous.Reason == reason && previous.Message == message {
		return nil
	}
	patch := client.MergeFrom(backup.DeepCopy())
	setCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionArtifactCleanup,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: backup.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
	if err := r.Status().Patch(ctx, backup, patch); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("patch %s condition: %w", v1alpha1.ConditionArtifactCleanup, err)
	}
	return nil
}

// ownedBackupJob returns the backup's own producing Job, or nil when it is
// gone or belongs to another incarnation of the same name.
func (r *MysqlBackupReconciler) ownedBackupJob(ctx context.Context, backup *v1alpha1.MysqlBackup) (*batchv1.Job, error) {
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backupJobName(backup.Name)}, &job); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get producing backup job: %w", err)
	}
	if !metav1.IsControlledBy(&job, backup) {
		return nil, nil
	}
	return &job, nil
}

// activeCleanupPods reports whether any cleanup pod of this backup is still
// pending or running.
func (r *MysqlBackupReconciler) activeCleanupPods(ctx context.Context, backup *v1alpha1.MysqlBackup) (bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(backup.Namespace), client.MatchingLabels{
		v1alpha1.MysqlBackupLabel: backup.Name,
		v1alpha1.ResourceLabel:    "backup-cleanup",
	}); err != nil {
		return false, fmt.Errorf("list cleanup pods: %w", err)
	}
	for i := range pods.Items {
		if phase := pods.Items[i].Status.Phase; phase != corev1.PodSucceeded && phase != corev1.PodFailed {
			return true, nil
		}
	}
	return false, nil
}

// failBackup transitions a MysqlBackup to Failed with the given reason.
func (r *MysqlBackupReconciler) failBackup(ctx context.Context, backup *v1alpha1.MysqlBackup, reason, message string) (ctrl.Result, error) {
	return r.failBackupOpts(ctx, backup, reason, message, false /*clearPreLaunchJob*/)
}

// failBackupOpts is failBackup with an option to drop stale Job attribution.
// clearPreLaunchJob is set on the no-Job path (pending deadline exceeded) so a
// terminal Failed status never advertises a Job that does not exist.
func (r *MysqlBackupReconciler) failBackupOpts(ctx context.Context, backup *v1alpha1.MysqlBackup, reason, message string, clearPreLaunchJob bool) (ctrl.Result, error) {
	now := metav1.Now()
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status.Phase = v1alpha1.BackupPhaseFailed
	backup.Status.CompletionTime = &now
	backup.Status.Message = message
	backup.Status.ObservedGeneration = backup.Generation
	if clearPreLaunchJob {
		clearPreLaunchJobStatus(backup, now)
	}
	setCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionReady,
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

// clearPreLaunchJobStatus drops Job attribution that is only valid while a
// backup Job exists. Used on the no-Job path (Pending retry or terminal fail
// before launch) so status never advertises a deleted/GC'd Job.
func clearPreLaunchJobStatus(backup *v1alpha1.MysqlBackup, now metav1.Time) {
	backup.Status.StartTime = nil
	backup.Status.JobName = ""
	backup.Status.SourceSite = ""
	backup.Status.ActiveSiteAtStart = ""
	setCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionJobCreated,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: backup.Generation,
		LastTransitionTime: now,
		Reason:             v1alpha1.ReasonNotCreated,
		Message:            "backup Job has not been created yet",
	})
}

// pendBackup records a retryable pre-launch dependency failure. A missing
// healthy source is expected during failover and ordered updates; it must not
// consume the one-shot MysqlBackup CR before a Job has even been created.
//
// If the wait exceeds maxBackupPendingDuration the CR is failed terminally so
// stalled backups do not accumulate while the schedule keeps creating new ones.
// Callers only reach this path when no Job exists yet; clear any stale
// Job-related status so Pending/Failed never advertises a Job that is gone.
func (r *MysqlBackupReconciler) pendBackup(ctx context.Context, backup *v1alpha1.MysqlBackup, reason, message string) (ctrl.Result, error) {
	now := metav1.Now()
	// A missing source is normally transient. If it persists, fail the CR so
	// it stops occupying the schedule and becomes eligible for pruning.
	if !backup.CreationTimestamp.IsZero() {
		if age := now.Sub(backup.CreationTimestamp.Time); age > maxBackupPendingDuration {
			return r.failBackupOpts(ctx, backup, reason,
				fmt.Sprintf("%s (pending for %s)", message, age.Truncate(time.Second)),
				true /*clearPreLaunchJob*/)
		}
	}

	wasPending := backup.Status.Phase == v1alpha1.BackupPhasePending
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status.Phase = v1alpha1.BackupPhasePending
	backup.Status.CompletionTime = nil
	// Pre-launch: no Job exists. Drop any leftover Job attribution left by a
	// deleted/GC'd Job so status matches the live cluster.
	clearPreLaunchJobStatus(backup, now)
	backup.Status.Message = message
	backup.Status.ObservedGeneration = backup.Generation
	setCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: backup.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch pending status: %w", err)
	}
	// Preserve the NoHealthySource signal operators used to get from failBackup,
	// but only on the transition into Pending — not on every 15s retry.
	if !wasPending {
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, reason, "%s", message)
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// ensureDerivedCredsSecret creates or updates the per-backup Secret
// carrying MYSQL_USER / MYSQL_PASSWORD, read from the effective backup
// secret. The Secret is owned by the MysqlBackup CR so it is garbage
// collected with the backup.
func (r *MysqlBackupReconciler) ensureDerivedCredsSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, backup *v1alpha1.MysqlBackup, secretName string) error {
	var user, password string

	backupSecretName := fg.Spec.EffectiveBackupSecretName()
	var srcSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: backupSecretName}, &srcSecret); err != nil {
		return fmt.Errorf("get backup credential secret %s: %w", backupSecretName, err)
	}

	user = string(srcSecret.Data["username"])
	password = string(srcSecret.Data["password"])
	if user == "" {
		return fmt.Errorf("credential secret %s has empty 'username'", backupSecretName)
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
		derived.Labels[v1alpha1.FailoverGroupLabel] = fg.Name
		derived.Labels[v1alpha1.MysqlBackupLabel] = backup.Name
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
type dumpCompletionMetadata = backupmeta.DumpComplete

func parseDumpCompleteLine(line string) (dumpCompletionMetadata, bool) {
	return backupmeta.ParseDumpComplete(line)
}

// tailJobCompletion distinguishes unavailable logs from a successfully read
// log without a usable sentinel. Only the producing main container of the
// newest Succeeded pod owned by job is read: the backup label is shared by the
// artifact-cleanup Job's pods and by a recreated same-name backup's pods.
func (r *MysqlBackupReconciler) tailJobCompletion(ctx context.Context, backup *v1alpha1.MysqlBackup, job *batchv1.Job) (dumpCompletionMetadata, bool, error) {
	if r.Clientset == nil {
		return dumpCompletionMetadata{}, false, fmt.Errorf("pod log client unavailable")
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{v1alpha1.MysqlBackupLabel: backup.Name},
	); err != nil {
		return dumpCompletionMetadata{}, false, fmt.Errorf("list backup pods: %w", err)
	}
	pod := newestSucceededJobPod(pods.Items, job)
	if pod == nil {
		return dumpCompletionMetadata{}, false, fmt.Errorf("completed backup pod not visible")
	}
	if container := backupJobMainContainer(job); container != nil {
		return r.tailOneContainer(ctx, job.Namespace, pod.Name, container.Name)
	}
	if len(pod.Spec.Containers) > 0 {
		return r.tailOneContainer(ctx, job.Namespace, pod.Name, pod.Spec.Containers[0].Name)
	}
	return dumpCompletionMetadata{}, false, fmt.Errorf("completed backup pod has no containers")
}

// newestSucceededJobPod returns the most recently created Succeeded pod that
// job owns, or nil.
func newestSucceededJobPod(pods []corev1.Pod, job *batchv1.Job) *corev1.Pod {
	var chosen *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase != corev1.PodSucceeded || !podBelongsToJob(p, job) {
			continue
		}
		if chosen == nil || p.CreationTimestamp.After(chosen.CreationTimestamp.Time) {
			chosen = p
		}
	}
	return chosen
}

func (r *MysqlBackupReconciler) tailOneContainer(ctx context.Context, namespace, podName, container string) (dumpCompletionMetadata, bool, error) {
	req := r.Clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: container,
		TailLines: ptr64(50),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return dumpCompletionMetadata{}, false, fmt.Errorf("read backup completion logs: %w", err)
	}
	defer func() { _ = stream.Close() }()
	var last dumpCompletionMetadata
	found := false
	bounded := &io.LimitedReader{R: stream, N: 64*1024 + 1}
	sc := bufio.NewScanner(bounded)
	sc.Buffer(make([]byte, 4096), 64*1024+1)
	for sc.Scan() {
		if meta, hit := parseDumpCompleteLine(sc.Text()); hit {
			last = meta
			found = true
		}
	}
	if err := sc.Err(); err != nil {
		return dumpCompletionMetadata{}, false, fmt.Errorf("scan backup completion logs: %w", err)
	}
	if bounded.N == 0 {
		return dumpCompletionMetadata{}, false, fmt.Errorf("backup completion logs exceeded read limit")
	}
	return last, found, nil
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
	r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventInFlightFailover,
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
	var checker *BackupReadChecker
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
		// Not kept by any policy check => delete, unless a verification
		// may still be reading the artifact.
		victim := b
		if checker == nil {
			checker = NewBackupReadChecker(r.apiReader())
		}
		if err := r.deleteUnreadBackup(ctx, &victim, checker); err != nil {
			return err
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
			return "", "", fmt.Errorf("sourceSite %q does not match any configured site", override)
		}
		if specSite.IsReadOnlyReader() {
			return "", "", fmt.Errorf("sourceSite %q names a read-only site, which cannot be a backup source", override)
		}
		for i := range status.Sites {
			if status.Sites[i].Name == override {
				return override, "override", nil
			}
		}
		return "", "", fmt.Errorf("sourceSite %q does not match any observed site", override)
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
		if replica.State == v1alpha1.SiteStateReadOnly && replica.Replicating {
			if replica.SecondsBehindSource == nil || *replica.SecondsBehindSource <= maxLagSeconds {
				return replica.Name, "replica-preferred", nil
			}
		}
	}

	primarySpec := fg.Spec.SiteByName(status.ActiveSite)
	if primary != nil && primary.State == v1alpha1.SiteStateWritable && primarySpec != nil && primarySpec.IsPromotable() {
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

// dumpKeySnapshotName is backup's dump-key snapshot (I-17):
// "mysqlbackup-<name>-dump-key-<uid[:8]>", DNS-truncated. The UID suffix
// keeps a same-name successor from ever meeting a kept predecessor's
// snapshot.
func dumpKeySnapshotName(backup *v1alpha1.MysqlBackup) string {
	uid := string(backup.UID)
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return truncateDNS1123("mysqlbackup-" + backup.Name + "-dump-key-" + uid)
}

// ensureDumpKeySnapshot returns backup's immutable dump-key snapshot,
// creating it from the profile's live passphrase (trimmed, D-7) when it
// does not exist (§4.7, K2). An existing snapshot labelled with this
// backup's UID is adopted as is: its bytes are never compared with the
// live Secret, so an edit between snapshot and Job creation changes
// nothing (row 50). It carries no owner reference (Δ24): the finalizer
// deletes it once the backup's artifacts are gone. A non-empty reason is a
// pend: the live Secret or key is missing or empty
// (PassphraseSecretMissing), or the name is held by another object
// (DumpKeySnapshotConflict).
func (r *MysqlBackupReconciler) ensureDumpKeySnapshot(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, backup *v1alpha1.MysqlBackup, profile *v1alpha1.BackupProfile) (name, reason, message string, err error) {
	name = dumpKeySnapshotName(backup)
	key := types.NamespacedName{Namespace: backup.Namespace, Name: name}
	adopt := func(s *corev1.Secret) (string, string, string, error) {
		if s.Labels[v1alpha1.MysqlBackupUIDLabel] != string(backup.UID) || s.Labels[labelAppName] != appNameDumpKeySnapshot {
			return "", v1alpha1.ReasonDumpKeySnapshotConflict,
				fmt.Sprintf("Secret %s exists but is not this backup's dump-key snapshot (label %s=%q); delete or rename it", name, v1alpha1.MysqlBackupUIDLabel, s.Labels[v1alpha1.MysqlBackupUIDLabel]), nil
		}
		return name, "", "", nil
	}
	var existing corev1.Secret
	switch err := r.apiReader().Get(ctx, key, &existing); {
	case err == nil:
		return adopt(&existing)
	case !apierrors.IsNotFound(err):
		return "", "", "", fmt.Errorf("get dump-key snapshot %s: %w", name, err)
	}

	ref := profile.Encryption.PassphraseSecretKeyRef
	secretKey := ref.PassphraseSecretKeyOrDefault()
	var live corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: ref.Name}, &live); err != nil {
		if apierrors.IsNotFound(err) {
			return "", v1alpha1.ReasonPassphraseSecretMissing,
				fmt.Sprintf("profile %q encryption.passphraseSecretKeyRef Secret %q not found", profile.Name, ref.Name), nil
		}
		return "", "", "", fmt.Errorf("get passphrase secret %s: %w", ref.Name, err)
	}
	passphrase := backupcrypto.TrimPassphrase(live.Data[secretKey])
	if len(passphrase) == 0 {
		return "", v1alpha1.ReasonPassphraseSecretMissing,
			fmt.Sprintf("profile %q encryption.passphraseSecretKeyRef Secret %q key %q is missing or empty", profile.Name, ref.Name, secretKey), nil
	}
	immutable := true
	snap := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: backup.Namespace,
			Labels: map[string]string{
				labelAppName:                   appNameDumpKeySnapshot,
				labelManagedBy:                 managerName,
				v1alpha1.FailoverGroupLabel:    fg.Name,
				v1alpha1.FailoverGroupUIDLabel: string(fg.UID),
				v1alpha1.MysqlBackupLabel:      backup.Name,
				v1alpha1.MysqlBackupUIDLabel:   string(backup.UID),
			},
		},
		Immutable: &immutable,
		Type:      corev1.SecretTypeOpaque,
		Data:      map[string][]byte{pitrKeySnapshotDataKey: append([]byte(nil), passphrase...)},
	}
	if err := r.Create(ctx, snap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", "", "", fmt.Errorf("create dump-key snapshot %s: %w", name, err)
		}
		if err := r.apiReader().Get(ctx, key, &existing); err != nil {
			return "", "", "", fmt.Errorf("get dump-key snapshot %s: %w", name, err)
		}
		return adopt(&existing)
	}
	return name, "", "", nil
}

// deleteDumpKeySnapshot deletes backup's dump-key snapshot once its
// artifacts are confirmed gone (Δ24). Only a Secret labelled with this
// backup's UID is deleted, with a UID precondition; NotFound is done.
func (r *MysqlBackupReconciler) deleteDumpKeySnapshot(ctx context.Context, backup *v1alpha1.MysqlBackup) error {
	if backup.UID == "" {
		return nil
	}
	var snap corev1.Secret
	err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: dumpKeySnapshotName(backup)}, &snap)
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return fmt.Errorf("get dump-key snapshot: %w", err)
	}
	if snap.Labels[v1alpha1.MysqlBackupUIDLabel] != string(backup.UID) || snap.Labels[labelAppName] != appNameDumpKeySnapshot {
		return nil
	}
	uid := snap.UID
	if err := r.Delete(ctx, &snap, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete dump-key snapshot %s: %w", snap.Name, err)
	}
	return nil
}

// dumpKeySnapshotKept returns the name of backup's dump-key snapshot when
// it exists (labelled with this backup's UID), else "".
func (r *MysqlBackupReconciler) dumpKeySnapshotKept(ctx context.Context, backup *v1alpha1.MysqlBackup) (string, error) {
	if backup.UID == "" {
		return "", nil
	}
	var snap corev1.Secret
	err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: dumpKeySnapshotName(backup)}, &snap)
	switch {
	case apierrors.IsNotFound(err):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("get dump-key snapshot: %w", err)
	}
	if snap.Labels[v1alpha1.MysqlBackupUIDLabel] != string(backup.UID) || snap.Labels[labelAppName] != appNameDumpKeySnapshot {
		return "", nil
	}
	return snap.Name, nil
}

// dumpKeyKeptNote is the clause a skipped cleanup's Event adds when the
// backup recorded a dump-key snapshot: the snapshot stays, because the
// artifacts it decrypts may still exist.
func dumpKeyKeptNote(backup *v1alpha1.MysqlBackup) string {
	if backup.Status.ArtifactStorage == nil || backup.Status.ArtifactStorage.PassphraseSecretKeyRef == nil {
		return ""
	}
	name := backup.Status.ArtifactStorage.PassphraseSecretKeyRef.Name
	if name != dumpKeySnapshotName(backup) {
		return ""
	}
	return fmt.Sprintf("; the dump-key snapshot Secret %q is kept (delete it once the artifacts are gone)", name)
}
