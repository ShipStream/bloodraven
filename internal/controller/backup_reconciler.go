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

// defaultBackupKeepLast and defaultBackupKeepFailed are the CRD defaults
// of BackupRetention.KeepLast and KeepFailed, applied when a profile built
// in Go (not read back through the API server) leaves them unset.
const (
	defaultBackupKeepLast   = int32(7)
	defaultBackupKeepFailed = int32(10)
)

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
	// status.artifactStorage.location / status.size can be populated. Tests using the
	// controller-runtime fake client leave it nil.
	Clientset kubernetes.Interface

	// APIReader is an uncached reader for retention and cleanup decisions
	// that must see verification holds as they are now, not as the
	// informer cache last saw them. SetupWithManager defaults it to
	// mgr.GetAPIReader(); unit tests leave it nil and read the client.
	APIReader client.Reader

	// now is the clock for wait deadlines; nil means time.Now.
	now func() time.Time

	// graceClock says when a Failed backup's terminalRetentionGrace starts.
	graceClock retentionGraceClock
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
			base := backup.DeepCopy()
			controllerutil.RemoveFinalizer(&backup, v1alpha1.MysqlBackupFinalizer)
			if err := r.Patch(ctx, &backup, metadataPatch(base)); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&backup, v1alpha1.MysqlBackupFinalizer) {
		base := backup.DeepCopy()
		controllerutil.AddFinalizer(&backup, v1alpha1.MysqlBackupFinalizer)
		// Also stamp the canonical labels on first encounter so label
		// selectors work even for ad-hoc CRs created by `kubectl create`.
		_ = ensureBackupLabels(&backup)
		if err := r.Patch(ctx, &backup, metadataPatch(base)); err != nil {
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
		// A terminal backup's spec can still change (deletionPolicy is
		// read at deletion): acknowledge the generation, or Argo CD health
		// reads Progressing for as long as the backup exists.
		if err := r.acknowledgeTerminalGeneration(ctx, &backup); err != nil {
			return ctrl.Result{}, err
		}
		requeue, err := r.pruneRetention(ctx, &backup)
		if err != nil {
			logger.Error(err, "retention prune")
		}
		recovery.RequeueAfter = shorterRequeue(recovery.RequeueAfter, requeue)
		return recovery, nil
	}

	// Ready is False from the first reconcile: generic Pending until a
	// launch attempt names a specific reason, Running once the Job runs.
	if backup.Status.Phase == "" && apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionReady) == nil {
		patch := client.MergeFrom(backup.DeepCopy())
		backup.Status.Phase = v1alpha1.BackupPhasePending
		backup.Status.ObservedGeneration = backup.Generation
		setCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: metav1.Now(),
			Reason:             v1alpha1.ReasonPending,
			Message:            "waiting for the operator to select a source and launch the backup Job",
		})
		if err := r.Status().Patch(ctx, &backup, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch initial pending status: %w", err)
		}
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
	base := backup.DeepCopy()
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
		if err := r.Patch(ctx, &backup, metadataPatch(base)); err != nil {
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
			return r.pendBackup(ctx, &backup, v1alpha1.ReasonWaitingForHealthySource, err.Error())
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
			r.Recorder.Eventf(&backup, corev1.EventTypeNormal, v1alpha1.EventPITRArchiveStampSkipped,
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
		// status.artifactStorage cannot change once recorded. A Job
		// re-created after its predecessor was deleted mid-run would, after
		// a profile edit, write somewhere the status can never name: the
		// backup fails instead of certifying an artifact it does not have.
		if recorded := backup.Status.ArtifactStorage; recorded != nil && recorded.Location != "" {
			if problem := producerStorageProblem(recorded, built); problem != "" {
				return r.failBackup(ctx, &backup, v1alpha1.ReasonBuildJobFailed, fmt.Sprintf(
					"the backup Job is gone and a replacement built from the current profile would not write to the destination recorded at launch: %s; create a new backup", problem))
			}
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
		if storage, err := artifactStorageFromJob(built); err == nil {
			backup.Status.ArtifactStorage = storage
		}
		backup.Status.MysqlImage = mysqlImageFor(&fg)
		backup.Status.ActiveSiteAtStart = fg.Status.ActiveSite
		backup.Status.Encryption = backupJobEncryption(built)
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
		setBackupRunningReady(&backup, built.Name, now)
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
	if next.MysqlImage == "" {
		next.MysqlImage = mysqlImageFor(&fg)
	}
	if uid, archiveID := backupJobIdentity(&job); metav1.IsControlledBy(&job, &backup) && next.FailoverGroupUID == "" && uid != "" {
		next.FailoverGroupUID, next.PITRArchiveID = uid, archiveID
	}
	next.ObservedGeneration = backup.Generation

	// The launch attribution (destination, encryption) comes from the
	// owned Job whatever its outcome: a lost launch-status write leaves
	// it absent, and a failed backup's planned destination is what its
	// abandoned cleanup and a same-name successor need.
	if next.ArtifactStorage == nil || next.ArtifactStorage.Location == "" {
		if storage, err := artifactStorageFromJob(&job); err == nil {
			next.ArtifactStorage = storage
		}
	}
	next.Encryption = backupJobEncryption(&job)

	var metadataResult ctrl.Result
	readyReason := v1alpha1.ReasonFailed
	if phase == v1alpha1.BackupPhaseSucceeded {
		var err error
		metadataResult, err = r.collectBackupCompletion(ctx, &backup, &job, &next)
		switch {
		case errors.Is(err, errBackupDestinationMismatch):
			// The Job succeeded, but not at the recorded destination:
			// the backup has no artifact there, so it fails.
			phase, message = v1alpha1.BackupPhaseFailed, err.Error()
			next.Phase, next.Message = phase, message
			readyReason = v1alpha1.ReasonDestinationMismatch
		case err != nil:
			next.Phase = backup.Status.Phase
			return ctrl.Result{}, r.persistBackupMetadataFailure(ctx, &backup, &next, err)
		}
	}
	if phase == v1alpha1.BackupPhaseSucceeded {
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
			Reason:             readyReason,
			Message:            message,
		})
	}

	// Exactly-once short-circuit.
	if backup.Status.Phase == phase && equality.Semantic.DeepEqual(&backup.Status, &next) {
		// Nothing to do — still run retention once though.
		requeue, err := r.pruneRetention(ctx, &backup)
		if err != nil {
			logger.Error(err, "retention prune after terminal state")
		}
		metadataResult.RequeueAfter = shorterRequeue(metadataResult.RequeueAfter, requeue)
		return metadataResult, nil
	}

	previous := backup.Status.DeepCopy()
	correctedEncryption := backupStatusEncrypted(&backup.Status) && !backupStatusEncrypted(&next)
	// The retention grace starts before the write: a Patch that commits
	// but whose response is lost must not let the retry's prune remove
	// the backup at once. A failed write only lengthens the grace.
	if previous.Phase != phase {
		r.graceClock.recordVerdict(&backup, time.Now())
	}
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

	requeue, err := r.pruneRetention(ctx, &backup)
	if err != nil {
		logger.Error(err, "retention prune after terminal state")
	}
	metadataResult.RequeueAfter = shorterRequeue(metadataResult.RequeueAfter, requeue)

	return metadataResult, nil
}

// recoverBackupIdentity restores, from the backup's own running Job, what
// the lost status patch after the Job's create would have written: the
// source group UID and PITR archive ID, the job name, the launch
// attribution (status.artifactStorage and status.encryption), and
// phase Running with Ready=False/Running. Only the backup's own Job is
// trusted, and a recorded identity or attribution is never replaced.
func (r *MysqlBackupReconciler) recoverBackupIdentity(ctx context.Context, backup *v1alpha1.MysqlBackup, job *batchv1.Job) error {
	if !metav1.IsControlledBy(job, backup) {
		return nil
	}
	uid, archiveID := backupJobIdentity(job)
	recoverIdentity := uid != "" && backup.Status.FailoverGroupUID == ""
	var storage *v1alpha1.BackupArtifactStorage
	if backup.Status.ArtifactStorage == nil || backup.Status.ArtifactStorage.Location == "" {
		// A Job whose destination cannot be read stays unattributed;
		// finalize and completion report that case.
		storage, _ = artifactStorageFromJob(job)
	}
	recoverEncryption := backup.Status.Encryption == nil
	ready := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionReady)
	recoverRunning := backup.Status.Phase != v1alpha1.BackupPhaseRunning || ready == nil || ready.Reason != v1alpha1.ReasonRunning
	if !recoverIdentity && storage == nil && !recoverEncryption && !recoverRunning {
		return nil
	}
	patch := client.MergeFrom(backup.DeepCopy())
	if recoverIdentity {
		backup.Status.FailoverGroupUID, backup.Status.PITRArchiveID = uid, archiveID
	}
	if storage != nil {
		backup.Status.ArtifactStorage = storage
	}
	if recoverEncryption {
		backup.Status.Encryption = backupJobEncryption(job)
	}
	if backup.Status.JobName == "" {
		backup.Status.JobName = job.Name
	}
	if recoverRunning {
		backup.Status.Phase = v1alpha1.BackupPhaseRunning
		setBackupRunningReady(backup, job.Name, metav1.Now())
	}
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		return fmt.Errorf("patch backup identity recovered from job: %w", err)
	}
	return nil
}

// acknowledgeTerminalGeneration records that a terminal backup's current
// generation was reconciled: status.observedGeneration and the Ready
// condition's observedGeneration move to it, while the phase, the Ready
// verdict and its transition time stay as they are.
func (r *MysqlBackupReconciler) acknowledgeTerminalGeneration(ctx context.Context, backup *v1alpha1.MysqlBackup) error {
	ready := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionReady)
	if backup.Status.ObservedGeneration == backup.Generation && (ready == nil || ready.ObservedGeneration == backup.Generation) {
		return nil
	}
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status.ObservedGeneration = backup.Generation
	if ready := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionReady); ready != nil {
		ready.ObservedGeneration = backup.Generation
	}
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		return fmt.Errorf("acknowledge terminal backup generation: %w", err)
	}
	return nil
}

// setBackupRunningReady sets Ready=False/Running for a backup whose Job
// runs.
func setBackupRunningReady(backup *v1alpha1.MysqlBackup, jobName string, now metav1.Time) {
	setCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: backup.Generation,
		LastTransitionTime: now,
		Reason:             v1alpha1.ReasonRunning,
		Message:            fmt.Sprintf("backup Job %s is running", jobName),
	})
}

// backupArtifactCompleted reports whether status records a completed
// artifact at its recorded destination: the backup succeeded, or its own
// Job did and the completion was collected (a status whose phase write was
// lost). Anything else left by its Job is an unfinished backup's leftovers.
func backupArtifactCompleted(status *v1alpha1.MysqlBackupStatus) bool {
	if status.ArtifactStorage == nil || status.ArtifactStorage.Location == "" {
		return false
	}
	if status.Phase == v1alpha1.BackupPhaseSucceeded {
		return true
	}
	c := apiMeta.FindStatusCondition(status.Conditions, v1alpha1.ConditionMetadataComplete)
	return c != nil && c.Reason != v1alpha1.ReasonDestinationUnavailable
}

// backupJobEncryption attributes the encryption of the artifact job writes
// from its launch-time choice: the uploader layout and its env pin the
// algorithm, and the label also marks the choice.
func backupJobEncryption(job *batchv1.Job) *v1alpha1.BackupEncryptionStatus {
	encrypted, algorithm := backupJobEncryptionChoice(job)
	if !encrypted {
		return &v1alpha1.BackupEncryptionStatus{Mode: v1alpha1.EncryptionModeNone}
	}
	return &v1alpha1.BackupEncryptionStatus{Mode: v1alpha1.EncryptionModePassphrase, Algorithm: algorithm}
}

// backupStatusEncrypted reports whether status attributes its artifact as
// encrypted.
func backupStatusEncrypted(status *v1alpha1.MysqlBackupStatus) bool {
	return status.Encryption != nil && status.Encryption.Mode == v1alpha1.EncryptionModePassphrase
}

func backupJobEncryptionChoice(job *batchv1.Job) (bool, v1alpha1.EncryptionAlgorithm) {
	encrypted := job.Labels[v1alpha1.BackupEncryptedLabel] == v1alpha1.LabelValueTrue
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
	return true, (&v1alpha1.BackupEncryptionSpec{Algorithm: v1alpha1.EncryptionAlgorithm(algorithm)}).AlgorithmOrDefault()
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
	next.Encryption = backupJobEncryption(&job)
	var recovery ctrl.Result
	// Completion is collected when the backup's own Job succeeded but the
	// status never recorded it (its terminal write was lost), when the
	// destination is not recorded, or while a log outage is retried.
	neverCollected := backup.Status.Phase != v1alpha1.BackupPhaseSucceeded &&
		apiMeta.FindStatusCondition(next.Conditions, v1alpha1.ConditionMetadataComplete) == nil
	if neverCollected || next.ArtifactStorage == nil || next.ArtifactStorage.Location == "" || r.Clientset != nil && completionMetadataRetryable(&next) {
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
	if !equality.Semantic.DeepEqual(previous.Encryption, next.Encryption) {
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
	r.graceClock.countFromFirstUse()
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
//   - Normal: the backup completed (backupArtifactCompleted) and
//     status.artifactStorage names its artifact. A failed cleanup Job is
//     a state, not an error: the
//     ArtifactCleanup condition names the cause and a fresh Job runs after
//     a fixed delay, indefinitely.
//   - Abandoned: the backup never completed, but status.artifactStorage
//     (recorded at launch, or recovered from the producing Job) names the
//     planned destination its Job wrote a
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
	// Record a completion the status lost before the mode is chosen: a
	// backup whose own Job succeeded is cleaned up as a completed artifact.
	// A backup whose Job failed is an unfinished one; its planned
	// destination is status.artifactStorage (abandoned mode below).
	if _, err := r.repairTerminalAttribution(ctx, backup); err != nil && !errors.Is(err, errBackupDestinationUnavailable) {
		return false, 0, err
	}
	if backup.Spec.DeletionPolicy == v1alpha1.DeletionPolicyRetain {
		// Retain applies only until the cleanup has started
		// (status.cleanupStartTime, recorded before the first cleanup Job
		// is created): a Job that ran may have deleted part of the
		// artifact, and one still running would be garbage-collected
		// mid-delete once the finalizer goes. The cleanup then runs to its
		// end, and its Events say that the late Retain was ignored.
		started, err := r.artifactCleanupStarted(ctx, backup)
		if err != nil {
			return false, 0, err
		}
		if !started {
			return r.retainArtifact(ctx, backup)
		}
	}
	// A hold whose verification can no longer read (its finalizer was
	// removed by hand, say) would keep the PITR cutoff pinned for as long
	// as cleanup stays blocked. Drop it first.
	if stripped, err := r.stripDeadHolds(ctx, backup); err != nil {
		return false, 0, err
	} else if !stripped {
		return false, 2 * time.Second, nil
	}
	// A backup that never succeeded has no completed artifact, but its Job
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
	location := ""
	var producer *batchv1.Job
	if backupArtifactCompleted(&backup.Status) {
		location = backup.Status.ArtifactStorage.Location
	} else {
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
		if problem := artifactIdentityProblem(backup, backup.Status.ArtifactStorage, producer); problem != "" {
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
	if problem := artifactIdentityProblem(backup, storage, producer); problem != "" {
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
		if wait, err := r.waitForArtifactReaders(ctx, backup, false); err != nil || wait > 0 {
			return false, wait, err
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
		if wait, err := r.recordCleanupStart(ctx, backup); err != nil || wait > 0 {
			return false, wait, err
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
				"cleanup of the unfinished backup at %q removed %d objects (nothing is removed unless its backup stamp names this backup)%s", location, objects, lateRetainNote(backup))
		case counted && objects == 0:
			r.Recorder.Eventf(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupNothingFound,
				"no objects found at %q in the recorded destination; nothing was deleted%s", location, lateRetainNote(backup))
		case counted:
			r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupSucceeded,
				"artifact cleanup removed %d objects at %q%s", objects, location, lateRetainNote(backup))
		default:
			r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupSucceeded,
				"artifact cleanup succeeded for %q%s", location, lateRetainNote(backup))
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

// retainArtifact releases a backup whose spec.deletionPolicy is Retain
// without touching storage (A-98). The artifact stays where it is, and so
// does the dump-key snapshot Secret that decrypts it: without it a
// retained encrypted artifact could not be read. The snapshot carries no
// owner reference, so nothing deletes it once the backup is gone.
//
// A backup that never succeeded may still have a producing Job writing to
// the destination. It is stopped, and its pods waited out, before the
// release, as the abandoned cleanup does: until the finalizer is gone no
// backup of the same name can be created, and a successor must not start
// while the old uploader may still write. A successor of the same name
// then finds the retained backup stamp, and its stamp step refuses the
// destination while dump objects remain there.
//
// It is also released only once no verification may still read it, as
// the Delete path's cleanup waits: the PITR binlog retention
// cutoff keeps a Terminating backup's coverage only while its object
// exists and is held or read, so releasing it under a running replay
// lets the archive prune binlogs the replay has yet to download.
func (r *MysqlBackupReconciler) retainArtifact(ctx context.Context, backup *v1alpha1.MysqlBackup) (bool, time.Duration, error) {
	if !backupArtifactCompleted(&backup.Status) && backup.UID != "" {
		producer, err := r.ownedBackupJob(ctx, backup)
		if err != nil {
			return false, 0, err
		}
		// Stopping deletes the Job, the only record of where it wrote when
		// the status patch after its create was lost: record it first, so
		// the ArtifactRetained Event and the retained status name it.
		if producer != nil && (backup.Status.ArtifactStorage == nil || backup.Status.ArtifactStorage.Location == "") {
			if recovered, err := artifactStorageFromJob(producer); err == nil {
				patch := client.MergeFrom(backup.DeepCopy())
				backup.Status.ArtifactStorage = recovered
				if err := r.Status().Patch(ctx, backup, patch); err != nil {
					return false, 0, fmt.Errorf("record recovered artifact storage: %w", err)
				}
			}
		}
		if wait, err := r.stopAbandonedProducer(ctx, backup, producer); err != nil || wait > 0 {
			return false, wait, err
		}
	}
	if stripped, err := r.stripDeadHolds(ctx, backup); err != nil {
		return false, 0, err
	} else if !stripped {
		return false, 2 * time.Second, nil
	}
	if wait, err := r.waitForArtifactReaders(ctx, backup, true); err != nil || wait > 0 {
		return false, wait, err
	}
	// The cleanup of this deletion never started (artifactCleanupStarted),
	// but a cleanup pod not attributable to it (an earlier backup of the
	// same name) may still be deleting at this destination. The artifact
	// is not reported as left in storage while one may still write; the
	// read is uncached so a pod the cache has not seen yet counts.
	if active, err := r.activeCleanupPodsFrom(ctx, r.apiReader(), backup); err != nil {
		return false, 0, err
	} else if active {
		log.FromContext(ctx).Info("deletionPolicy is Retain; waiting for a running cleanup pod of this backup name to stop before releasing it",
			"mysqlbackup", client.ObjectKeyFromObject(backup))
		return false, 5 * time.Second, nil
	}
	location := "no recorded destination"
	if storage := backup.Status.ArtifactStorage; storage != nil && storage.Location != "" {
		location = fmt.Sprintf("%q", storage.Location)
	}
	kept, err := r.dumpKeySnapshotKept(ctx, backup)
	if err != nil {
		return false, 0, err
	}
	key, what := "", "it"
	if kept != "" {
		key, what = fmt.Sprintf("; the dump-key snapshot Secret %q that decrypts it is kept", kept), "them"
	}
	r.Recorder.Eventf(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactRetained,
		"deletionPolicy is Retain: the artifact (%s) is left in storage%s; delete %s by hand once no longer needed",
		location, key, what)
	return true, 0, nil
}

// recordCleanupStart sets status.cleanupStartTime before the first
// cleanup Job is created, so the cutoff after which a late Retain is not
// applied survives the Job's removal (an Orphan delete of the Job, or the
// gap between a failed Job's deletion and its replacement, leaves its pod
// possibly still deleting with no Job to find). The write is conditioned on
// the resourceVersion this pass read: a spec edit (to Retain) since then
// conflicts, and the next pass decides from the new spec before any Job
// exists.
func (r *MysqlBackupReconciler) recordCleanupStart(ctx context.Context, backup *v1alpha1.MysqlBackup) (time.Duration, error) {
	if backup.Status.CleanupStartTime != nil {
		return 0, nil
	}
	patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
	now := metav1.NewTime(r.clock())
	backup.Status.CleanupStartTime = &now
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		if apierrors.IsConflict(err) {
			return 2 * time.Second, nil
		}
		return 0, fmt.Errorf("record cleanup start: %w", err)
	}
	return 0, nil
}

// artifactCleanupStarted reports whether the deletion has started cleaning
// up backup's artifact: status.cleanupStartTime is set, its own cleanup Job
// exists, or the ArtifactCleanup condition records one that failed or
// could not start.
func (r *MysqlBackupReconciler) artifactCleanupStarted(ctx context.Context, backup *v1alpha1.MysqlBackup) (bool, error) {
	if backup.Status.CleanupStartTime != nil {
		return true, nil
	}
	job, err := r.currentCleanupJob(ctx, backup)
	if err != nil {
		return false, err
	}
	if job != nil && metav1.IsControlledBy(job, backup) {
		return true, nil
	}
	if c := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup); c != nil {
		switch c.Reason {
		case v1alpha1.ReasonCleanupFailed, v1alpha1.ReasonCleanupRejected, v1alpha1.ReasonCleanupPending:
			return true, nil
		}
	}
	return false, nil
}

// lateRetainNote is appended to the cleanup Events of a backup whose
// deletionPolicy was set to Retain after its cleanup started.
func lateRetainNote(backup *v1alpha1.MysqlBackup) string {
	if backup.Spec.DeletionPolicy != v1alpha1.DeletionPolicyRetain {
		return ""
	}
	return "; deletionPolicy was changed to Retain after the cleanup started, so it was not applied"
}

// waitForArtifactReaders returns a requeue while a verification may still
// be reading backup's artifact, recording why on the ArtifactCleanup
// condition (HeldByVerification) and emitting ArtifactCleanupDeferred when
// the readers change. retain says the wait only holds the release of a
// Retain backup, whose artifact stays in storage, not a cleanup.
func (r *MysqlBackupReconciler) waitForArtifactReaders(ctx context.Context, backup *v1alpha1.MysqlBackup, retain bool) (time.Duration, error) {
	readers, err := artifactReaders(ctx, r.apiReader(), backup)
	if err != nil {
		return 0, err
	}
	if len(readers) == 0 {
		return 0, nil
	}
	message := fmt.Sprintf("verification %s may still be reading the artifact", strings.Join(readers, ", "))
	event := "cleanup waits: " + message
	if retain {
		message += "; deletionPolicy is Retain, so the artifact stays in storage and only the release of this backup waits"
		event = "release waits: " + message
	}
	previous := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup)
	if previous == nil || previous.Reason != v1alpha1.ReasonHeldByVerification || previous.Message != message {
		r.Recorder.Event(backup, corev1.EventTypeNormal, v1alpha1.EventArtifactCleanupDeferred, event)
	}
	if err := r.setArtifactCleanupCondition(ctx, backup, v1alpha1.ReasonHeldByVerification, message); err != nil {
		return 0, err
	}
	return 30 * time.Second, nil
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
			r.Recorder.Event(backup, corev1.EventTypeWarning, v1alpha1.EventArtifactCleanupStalledProducerStop, message)
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
	encrypted := backupStatusEncrypted(&backup.Status)
	recorded := (*v1alpha1.SecretKeyRef)(nil)
	if storage != nil {
		recorded = storage.PassphraseSecretKeyRef
	}
	if producer != nil {
		if e, _ := backupJobEncryptionChoice(producer); e {
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
			"artifact cleanup Job %s failed (%s): %s; a new Job runs %s after the failure%s",
			job.Name, reason, detail, wait, lateRetainNote(backup))
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
	return r.activeCleanupPodsFrom(ctx, r.Client, backup)
}

// activeCleanupPodsFrom is activeCleanupPods reading from reader.
func (r *MysqlBackupReconciler) activeCleanupPodsFrom(ctx context.Context, reader client.Reader, backup *v1alpha1.MysqlBackup) (bool, error) {
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(backup.Namespace), client.MatchingLabels{
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
// The failure never carries a pending reason (pendingTimeoutReason).
// Callers only reach this path when no Job exists yet; clear any stale
// Job-related status so Pending/Failed never advertises a Job that is gone.
func (r *MysqlBackupReconciler) pendBackup(ctx context.Context, backup *v1alpha1.MysqlBackup, reason, message string) (ctrl.Result, error) {
	now := metav1.Now()
	// A missing source is normally transient. If it persists, fail the CR so
	// it stops occupying the schedule and becomes eligible for pruning.
	if !backup.CreationTimestamp.IsZero() {
		if age := now.Sub(backup.CreationTimestamp.Time); age > maxBackupPendingDuration {
			failReason, failMessage := pendingTimeoutReason(reason, message, age)
			return r.failBackupOpts(ctx, backup, failReason, failMessage, true /*clearPreLaunchJob*/)
		}
	}

	// One Event per wait: on entering Pending or a different pending
	// reason, not on every retry.
	prev := apiMeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionReady)
	wasPending := backup.Status.Phase == v1alpha1.BackupPhasePending && prev != nil && prev.Reason == reason
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
	if !wasPending {
		r.Recorder.Eventf(backup, corev1.EventTypeWarning, reason, "%s", message)
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// pendingTimeoutReason maps a pending reason to the terminal reason of a
// backup that waited longer than maxBackupPendingDuration: a wait for a
// healthy source fails NoHealthySource, any other PendingTimedOut with the
// pending reason in the message.
func pendingTimeoutReason(reason, message string, age time.Duration) (string, string) {
	waited := age.Truncate(time.Second)
	if reason == v1alpha1.ReasonWaitingForHealthySource {
		return v1alpha1.ReasonNoHealthySource, fmt.Sprintf("%s (pending for %s)", message, waited)
	}
	return v1alpha1.ReasonPendingTimedOut, fmt.Sprintf("pending for %s (%s): %s", waited, reason, message)
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
// canonical labels. Successes go through the keep-if-any rules of the
// profile's retention; failures go through a count cap.
//
// It returns how soon to look again when a Failed backup was kept only for
// terminalRetentionGrace, or 0.
func (r *MysqlBackupReconciler) pruneRetention(ctx context.Context, trigger *v1alpha1.MysqlBackup) (time.Duration, error) {
	var list v1alpha1.MysqlBackupList
	if err := r.List(ctx, &list, client.InNamespace(trigger.Namespace)); err != nil {
		return 0, err
	}

	// Resolve the effective policy from the (possibly nil) profile.
	var profile *v1alpha1.BackupProfile
	var fg v1alpha1.MysqlFailoverGroup
	groupRead := false
	if err := r.Get(ctx, types.NamespacedName{Namespace: trigger.Namespace, Name: trigger.Spec.FailoverGroupRef.Name}, &fg); err == nil {
		groupRead = true
		profile = findProfile(&fg, trigger.Spec.ProfileName)
	}
	policy := resolveRetention(profile)

	// Computed before the in-place filter below reuses list.Items. Only a
	// schedule the group still names keeps its latest backup; when the
	// group cannot be read, every schedule label does.
	var schedules map[string]bool
	if groupRead {
		schedules = map[string]bool{}
		if fg.Spec.Backup != nil {
			for _, sched := range fg.Spec.Backup.Schedules {
				schedules[sched.Name] = true
			}
		}
	}
	scheduleLatest := latestScheduledBackups(list.Items, trigger.Spec.FailoverGroupRef.Name, schedules)

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

	if policy.pruneSucceeded {
		if err := r.pruneSuccessful(ctx, filtered, policy.keepLast, policy.keepWithin); err != nil {
			return 0, err
		}
	}
	return r.pruneFailed(ctx, filtered, int(policy.keepFailed), scheduleLatest, time.Now())
}

// latestScheduledBackups returns the names of the backups that are their
// schedule's latest (the rollup's status.backupSchedules[].lastBackupName,
// by the same order, startAfter) among items of group, ignoring those
// being deleted. A non-nil schedules limits it to the schedules it names:
// a removed or renamed schedule has no retry and no rollup entry, so its
// last backup is pruned like any other.
func latestScheduledBackups(items []v1alpha1.MysqlBackup, group string, schedules map[string]bool) map[string]bool {
	latest := map[string]*v1alpha1.MysqlBackup{}
	for i := range items {
		b := &items[i]
		sched := b.Labels[v1alpha1.BackupScheduleLabel]
		if sched == "" || b.Spec.FailoverGroupRef.Name != group || !b.DeletionTimestamp.IsZero() {
			continue
		}
		if schedules != nil && !schedules[sched] {
			continue
		}
		if cur, ok := latest[sched]; !ok || startAfter(b, cur) {
			latest[sched] = b
		}
	}
	names := make(map[string]bool, len(latest))
	for _, b := range latest {
		names[b.Name] = true
	}
	return names
}

// backupRetentionPolicy is a profile's effective retention.
type backupRetentionPolicy struct {
	// pruneSucceeded is false when the profile is gone: without its rules,
	// no successful backup is deleted.
	pruneSucceeded bool
	// keepLast is at least 1.
	keepLast int32
	// keepWithin is 0 when there is no age rule.
	keepWithin time.Duration
	// keepFailed is at least 0; 0 keeps no failed backup.
	keepFailed int32
}

// resolveRetention returns the retention the reconciler applies for
// profile. Admission defaults keepLast and keepFailed and refuses a
// keepLast below 1, so the defaults here only fill a profile built in Go.
// An explicit keepFailed of 0 is kept: it means keep none. A missing
// profile keeps every successful backup and caps failures at the default.
func resolveRetention(profile *v1alpha1.BackupProfile) backupRetentionPolicy {
	if profile == nil {
		return backupRetentionPolicy{keepFailed: defaultBackupKeepFailed}
	}
	rt := profile.Retention
	policy := backupRetentionPolicy{pruneSucceeded: true, keepLast: rt.KeepLast, keepFailed: defaultBackupKeepFailed}
	if policy.keepLast < 1 {
		policy.keepLast = defaultBackupKeepLast
	}
	if rt.KeepWithinDays != nil && *rt.KeepWithinDays > 0 {
		policy.keepWithin = time.Duration(*rt.KeepWithinDays) * 24 * time.Hour
	}
	if rt.KeepFailed != nil {
		policy.keepFailed = max(*rt.KeepFailed, 0)
	}
	return policy
}

// pruneSuccessful applies the keep-if-any retention rules to the
// Succeeded bucket of a filtered MysqlBackup slice. A CR is kept when
// either is true:
//
//   - its rank from newest is < keepLast (at least 1, so the newest
//     successful backup is never deleted), or
//   - keepWithin > 0 and its completion time is within that window.
//
// A CR no rule keeps is deleted unless a verification may still be
// reading its artifact (deleteUnreadBackup).
func (r *MysqlBackupReconciler) pruneSuccessful(ctx context.Context, all []v1alpha1.MysqlBackup, keepLast int32, keepWithin time.Duration) error {
	keepLast = max(keepLast, 1)
	bucket := make([]v1alpha1.MysqlBackup, 0, len(all))
	for _, b := range all {
		if b.Status.Phase == v1alpha1.BackupPhaseSucceeded {
			bucket = append(bucket, b)
		}
	}
	if len(bucket) <= int(keepLast) {
		return nil
	}
	sort.Slice(bucket, func(i, j int) bool {
		ti := timeOrZero(bucket[i].Status.CompletionTime)
		tj := timeOrZero(bucket[j].Status.CompletionTime)
		return ti.After(tj)
	})
	cutoff := time.Now().Add(-keepWithin)
	var checker *BackupReadChecker
	for i, b := range bucket {
		// Within the count window.
		if int32(i) < keepLast {
			continue
		}
		// Within the age window.
		if keepWithin > 0 {
			t := timeOrZero(b.Status.CompletionTime)
			if !t.IsZero() && t.After(cutoff) {
				continue
			}
		}
		// Not kept by any rule => delete, unless a verification may still
		// be reading the artifact.
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

// pruneFailed keeps the newest `keep` Failed backups and deletes the
// rest; 0 keeps none. Entries without a CompletionTime sort last.
//
// A Failed backup that is still its schedule's latest (scheduleLatest) is
// never deleted, whatever keep says, until a newer backup of the schedule
// exists. It is the only input of the operator retry (spec.backup.retry
// retries the schedule's latest backup when it failed), and the schedule
// status reports it (lastBackupPhase Failed); deleting it would cancel the
// retry and show the previous success instead.
//
// A Failed backup still inside its terminalRetentionGrace is kept too,
// and the returned duration says when the first such grace ends.
func (r *MysqlBackupReconciler) pruneFailed(ctx context.Context, all []v1alpha1.MysqlBackup, keep int, scheduleLatest map[string]bool, now time.Time) (time.Duration, error) {
	keep = max(keep, 0)
	bucket := make([]v1alpha1.MysqlBackup, 0, len(all))
	for _, b := range all {
		if b.Status.Phase == v1alpha1.BackupPhaseFailed {
			bucket = append(bucket, b)
		}
	}
	if len(bucket) <= keep {
		return 0, nil
	}
	sort.Slice(bucket, func(i, j int) bool {
		ti := timeOrZero(bucket[i].Status.CompletionTime)
		tj := timeOrZero(bucket[j].Status.CompletionTime)
		return ti.After(tj)
	})
	var requeue time.Duration
	for i := keep; i < len(bucket); i++ {
		victim := bucket[i]
		if scheduleLatest[victim.Name] {
			continue
		}
		if grace := r.graceClock.left(&victim, victim.Status.CompletionTime, now); grace > 0 {
			requeue = shorterRequeue(requeue, grace)
			continue
		}
		if err := r.Delete(ctx, &victim); err != nil && !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("delete old %s backup %s: %w", v1alpha1.BackupPhaseFailed, victim.Name, err)
		}
	}
	return requeue, nil
}

// terminalRetentionGrace is how long a Failed backup or a terminal
// verification is kept after its verdict, whatever the retention counts say (see
// retentionGraceClock for when it starts). With a
// keep-none rule (keepFailed: 0, verification keepLast: 0) the object would
// otherwise be deleted in its own terminal pass, before `kubectl bloodraven
// backup --wait` or `verify --wait` (which poll every 5 seconds) or anyone
// else reads the verdict.
const terminalRetentionGrace = 2 * time.Minute

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
		if replica.State == v1alpha1.SiteStateReadOnly && replica.Replication.Running() {
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
