package controller

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
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
	"github.com/shipstream/bloodraven/internal/backupmeta"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// defaultVerificationKeepLast and defaultVerificationKeepFailed are the
// CRD defaults of VerificationRetention, applied when a profile has no
// verification block or leaves a field unset. Failed runs are kept
// separately because they're what operators most want to investigate.
const (
	defaultVerificationKeepLast   = int32(30)
	defaultVerificationKeepFailed = int32(10)
)

// MysqlBackupVerificationReconciler reconciles MysqlBackupVerification CRs.
type MysqlBackupVerificationReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Clientset is optional; when non-nil the reconciler tails the
	// verification Job pod logs for the BLOODRAVEN_VERIFY_SANITY_COMPLETE
	// sentinel and populates status.sanityCheck. (status.replayedThroughBinlog
	// comes only from a PITR verification's termination verdict.)
	// Fake-client tests leave this nil; status will then only carry the
	// terminal Phase and Conditions.
	Clientset kubernetes.Interface

	// APIReader is an uncached reader for verification holds on
	// MysqlBackups. SetupWithManager defaults it to mgr.GetAPIReader();
	// unit tests leave it nil and read the client.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications/finalizers,verbs=update
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

func (r *MysqlBackupVerificationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("mysqlbackupverification", req.NamespacedName)

	var v v1alpha1.MysqlBackupVerification
	if err := r.Get(ctx, req.NamespacedName, &v); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: clean up artifacts, wait for the verification's
	// pods to stop reading the backup, release the hold, then drop the
	// finalizer. Releasing earlier would let retention or artifact
	// cleanup delete the backup under a still-running pod.
	if !v.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&v, v1alpha1.MysqlBackupVerificationFinalizer) {
			if _, err := r.cleanupEphemeral(ctx, &v, true); err != nil {
				return ctrl.Result{RequeueAfter: 15 * time.Second}, fmt.Errorf("cleanup on delete: %w", err)
			}
			released, err := r.releaseVerificationHold(ctx, &v)
			if err != nil {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, fmt.Errorf("release verification hold: %w", err)
			}
			if stopped, err := verificationExecutionStopped(ctx, r.apiReader(), v.Namespace, v.Name, true); err != nil {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			} else if !released || !stopped {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			controllerutil.RemoveFinalizer(&v, v1alpha1.MysqlBackupVerificationFinalizer)
			if err := r.Update(ctx, &v); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Stamp finalizer + labels on first encounter.
	if !controllerutil.ContainsFinalizer(&v, v1alpha1.MysqlBackupVerificationFinalizer) {
		controllerutil.AddFinalizer(&v, v1alpha1.MysqlBackupVerificationFinalizer)
		_ = ensureVerificationLabels(&v)
		if err := r.Update(ctx, &v); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Terminal: retention GC then stop reconciling (aside from TTL
	// cleanup of any lingering ephemeral resources on success).
	if v.Status.Phase == v1alpha1.VerificationPhaseSucceeded || v.Status.Phase == v1alpha1.VerificationPhaseFailed {
		requeue, err := r.maybeCleanupAfterTerminal(ctx, &v)
		if err != nil {
			logger.Error(err, "post-terminal cleanup")
		}
		requeue = shorterRequeue(requeue, r.maybeReleaseHold(ctx, &v))
		if err := r.pruneRetention(ctx, &v); err != nil {
			logger.Error(err, "retention prune")
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	var job batchv1.Job
	jobKey := types.NamespacedName{Namespace: v.Namespace, Name: verificationJobName(v.Name)}
	if err := r.Get(ctx, jobKey, &job); err != nil {
		if apierrors.IsNotFound(err) {
			return r.launchVerification(ctx, &v)
		}
		return ctrl.Result{}, fmt.Errorf("get verification job: %w", err)
	}
	if !metav1.IsControlledBy(&job, &v) {
		return ctrl.Result{}, fmt.Errorf("verification Job is not owned by this verification")
	}
	return r.observeVerificationJob(ctx, &v, &job)
}

func (r *MysqlBackupVerificationReconciler) launchVerification(ctx context.Context, v *v1alpha1.MysqlBackupVerification) (ctrl.Result, error) {
	// Resolve the referenced failover group.
	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: v.Namespace, Name: v.Spec.FailoverGroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failVerification(ctx, v, v1alpha1.ReasonGroupNotFound,
				fmt.Sprintf("MysqlFailoverGroup %q not found", fgKey.Name))
		}
		return ctrl.Result{}, fmt.Errorf("get failover group: %w", err)
	}

	// Coalesce owner-ref + label stamp.
	needsUpdate := false
	if len(v.OwnerReferences) == 0 {
		if err := controllerutil.SetControllerReference(&fg, v, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set owner ref: %w", err)
		}
		needsUpdate = true
	}
	if ensureVerificationLabels(v) {
		needsUpdate = true
	}
	if needsUpdate {
		if err := r.Update(ctx, v); err != nil {
			return ctrl.Result{}, fmt.Errorf("update owner ref / labels: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve the named profile.
	profile := findProfile(&fg, v.Spec.ProfileName)
	if profile == nil {
		return r.failVerification(ctx, v, v1alpha1.ReasonProfileNotFound,
			fmt.Sprintf("profile %q not found on MysqlFailoverGroup %q", v.Spec.ProfileName, fg.Name))
	}

	// Enforce per-profile single-flight: if another non-terminal
	// verification for the same (group, profile) exists and was
	// created earlier than this one, refuse to run concurrently.
	if blocked, err := r.findBlockingVerification(ctx, v); err != nil {
		return ctrl.Result{}, fmt.Errorf("check concurrent verifications: %w", err)
	} else if blocked != "" {
		return r.failVerification(ctx, v, v1alpha1.ReasonBlockedByActiveVerification,
			fmt.Sprintf("another verification %q for (group=%s, profile=%s) is still running",
				blocked, v.Spec.FailoverGroupRef.Name, v.Spec.ProfileName))
	}

	backup, err := r.resolvePinnedVerificationBackup(ctx, v)
	if err != nil {
		return r.failVerification(ctx, v, v1alpha1.ReasonBackupNotAvailable, err.Error())
	}
	if v.Status.BackupRef == nil {
		patch := client.MergeFrom(v.DeepCopy())
		v.Status.BackupRef = verificationBackupSnapshot(backup)
		setVerificationReady(&v.Status, v.Generation)
		if err := r.Status().Patch(ctx, v, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("persist pinned backup reference: %w", err)
		}
	}
	// Hold the pinned backup against retention and artifact cleanup
	// before anything reads it. The pin is persisted first, so a crash
	// here re-stamps the same backup on the next pass.
	if err := r.stampVerificationHold(ctx, v, false); err != nil {
		switch {
		case errors.Is(err, errHoldBackupGone), errors.Is(err, errHoldBackupReplaced), errors.Is(err, errHoldBackupTerminating):
			return r.failVerification(ctx, v, v1alpha1.ReasonBackupNotAvailable,
				fmt.Sprintf("pinned backup %q: %v", v.Status.BackupRef.Name, err))
		case apierrors.IsConflict(err):
			return ctrl.Result{RequeueAfter: time.Second}, nil
		default:
			return ctrl.Result{}, err
		}
	}

	// Ensure the derived creds Secret (S3 workloads consult it; the
	// ephemeral mysqld uses an in-pod synthetic root).
	credsName := verificationCredsSecretName(v.Name)
	if err := r.ensureVerificationCredsSecret(ctx, &fg, v, credsName); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure verification creds: %w", err)
	}

	// The ephemeral datadir is an emptyDir baked into the verification
	// Job pod spec (see buildVerificationJob), so there is no separate
	// PVC to provision here.

	// Phase transition: Pending → Provisioning on first pass.
	if v.Status.Phase == "" || v.Status.Phase == v1alpha1.VerificationPhasePending {
		now := metav1.Now()
		patch := client.MergeFrom(v.DeepCopy())
		v.Status.Phase = v1alpha1.VerificationPhaseProvisioning
		v.Status.StartTime = &now
		v.Status.Message = "ephemeral resources provisioned"
		v.Status.ObservedGeneration = v.Generation
		setVerificationReady(&v.Status, v.Generation)
		if err := r.Status().Patch(ctx, v, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch provisioning status: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	built, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        &fg,
		Profile:              *profile,
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      credsName,
		ScriptsConfigMapName: backupScriptsConfigMapName(fg.Name),
	})
	// The archive's read binding is not usable yet (Δ22): a fixable state
	// waits (requeue, message on status); an archive that can never verify,
	// or a binding the group has no room for, fails the verification.
	var notReady *errPITRBindingNotReady
	if errors.As(err, &notReady) {
		if notReady.terminal || notReady.reason == v1alpha1.ReasonBindingLimitReached {
			return r.failVerification(ctx, v, string(v1alpha1.PITRTokenArchiveBindingNotReady), notReady.Error())
		}
		return r.waitVerification(ctx, v, notReady.Error())
	}
	if err != nil {
		// A PITR refusal (PITRBackupIdentityUnknown, PITRTargetInvalid,
		// ...) is recorded by its catalog token, as the restore build
		// path and Job-time refusals are (F028); anything else is an
		// operator-side build failure.
		reason, message := v1alpha1.ReasonBuildJobFailed, err.Error()
		if tok := pitrFailureReason(message); tok != "" {
			reason, message = string(tok), tokenFirstMessage(string(tok), message)
		}
		return r.failVerification(ctx, v, reason, message)
	}
	// The dump key the Job mounts (required) must exist: a missing one
	// would leave the pod in ContainerCreating until the Job's deadline.
	// It fails now with the documented token (U13), as a restore does.
	if missing, err := r.verificationDumpKeyMissing(ctx, backup); err != nil {
		return ctrl.Result{}, err
	} else if missing != "" {
		return r.failVerification(ctx, v, string(pitrarchive.TokenBackupDumpKeyMissing), missing)
	}
	if err := controllerutil.SetControllerReference(v, built, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("set job owner ref: %w", err)
	}
	// The PITR archive key snapshot the Job mounts is held like the
	// backup (K9): a binding release refuses while the hold is live, and a
	// release that won the race leaves the stamp failing NotFound, so the
	// verification waits for the binding instead of mounting nothing.
	keySnapshot := built.Labels[v1alpha1.PITRKeySecretLabel]
	if keySnapshot != "" {
		if err := r.stampPITRKeySnapshotHold(ctx, v, &fg, keySnapshot); err != nil {
			switch {
			case errors.Is(err, errHoldKeySnapshotGone):
				return r.waitVerification(ctx, v, fmt.Sprintf("%s: archive key snapshot %q is gone; waiting for the archive binding",
					string(v1alpha1.PITRTokenArchiveBindingNotReady), keySnapshot))
			case apierrors.IsConflict(err):
				return ctrl.Result{RequeueAfter: time.Second}, nil
			default:
				return ctrl.Result{}, err
			}
		}
	}
	// Last uncached check: retention may have deleted the backup between
	// the stamp and now only if it won the resourceVersion race, which
	// leaves it Terminating or without this hold.
	if refusal, err := r.launchHoldConfirmed(ctx, v); err != nil {
		return ctrl.Result{}, err
	} else if refusal != "" {
		return r.failVerification(ctx, v, v1alpha1.ReasonBackupNotAvailable, refusal)
	}
	if keySnapshot != "" {
		if wait, err := r.pitrKeySnapshotHoldConfirmed(ctx, v, keySnapshot); err != nil {
			return ctrl.Result{}, err
		} else if wait != "" {
			return r.waitVerification(ctx, v, string(v1alpha1.PITRTokenArchiveBindingNotReady)+": "+wait)
		}
	}
	if err := r.Create(ctx, built); err != nil {
		return ctrl.Result{}, fmt.Errorf("create verification job: %w", err)
	}
	r.Recorder.Eventf(v, corev1.EventTypeNormal, v1alpha1.EventVerificationStarted,
		"created verification Job %s (backup=%s, profile=%s)", built.Name, backup.Name, profile.Name)

	patch := client.MergeFrom(v.DeepCopy())
	v.Status.Phase = v1alpha1.VerificationPhaseRestoring
	v.Status.JobName = built.Name
	v.Status.Message = "running restore Job against ephemeral mysqld"
	v.Status.ObservedGeneration = v.Generation
	setVerificationReady(&v.Status, v.Generation)
	if err := r.Status().Patch(ctx, v, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch running status: %w", err)
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func (r *MysqlBackupVerificationReconciler) observeVerificationJob(ctx context.Context, v *v1alpha1.MysqlBackupVerification, job *batchv1.Job) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	ref, err := verificationJobSource(v, job)
	if err != nil {
		return r.failVerification(ctx, v, v1alpha1.ReasonBackupIdentityMismatch, err.Error())
	}
	if err := r.pinVerificationJobSource(ctx, v, job, ref); err != nil {
		return ctrl.Result{}, err
	}
	if !verificationTerminal(v.Status.Phase) {
		// Idempotent; covers runs started by an operator that did not
		// stamp holds and a crash between Job creation and status. The
		// backup may already be Terminating: the hold then defers its
		// artifact cleanup until this run stops.
		if err := r.stampVerificationHold(ctx, v, true); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			if !errors.Is(err, errHoldBackupGone) && !errors.Is(err, errHoldBackupReplaced) {
				return ctrl.Result{}, err
			}
		}
	}
	// Observe the Job. Status translation reuses the backup helper.
	phase, message := jobPhase(job, "verification")
	jobFailed := phase == v1alpha1.BackupPhaseFailed
	if phase == "" {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Terminal — build next status and patch iff different, then emit
	// metrics exactly once.
	next := *v.Status.DeepCopy()
	stable := stableJobCompletionTime(job)
	var completion metav1.Time
	if stable != nil {
		completion = *stable
	} else {
		completion = metav1.Now()
	}

	// The verify.sh sanity sentinel adds best-effort detail. A PITR
	// verification is certified only by its termination verdict, which is
	// also the only source of replay detail.
	sentinels := r.tailVerificationSentinels(ctx, v, job)
	switch {
	case sentinels.Sanity != nil:
		next.SanityCheck = sentinels.Sanity
	case jobFailed && sentinels.LogsRead && v.Spec.SanityCheck != nil && v.Spec.SanityCheck.Query != "":
		// The whole log tail was read and the sanity phase never reported:
		// the Job failed before it (load or replay).
		next.SanityCheck = &v1alpha1.SanityCheckResult{Outcome: v1alpha1.SanityCheckOutcomeSkipped}
	}
	pitr := jobHasPITR(job)
	var pod *corev1.Pod
	if pitr {
		if pod, err = latestJobPod(ctx, r.Client, job); err != nil {
			return ctrl.Result{}, fmt.Errorf("list verification Job pods: %w", err)
		}
	}
	// A terminal observation always recomputes the certified result.
	next.PointInTime = nil
	if pitr && phase == v1alpha1.BackupPhaseSucceeded {
		result, err := pitrJobVerdict(pod, job)
		if err != nil {
			phase = v1alpha1.BackupPhaseFailed
			message = "PITRCoverageInvalid: " + err.Error()
		} else {
			next.PointInTime = result
		}
	}
	if pitr && jobFailed {
		if detail := pitrJobFailure(pod); detail != "" {
			message = detail
		}
	}
	reason := v1alpha1.ReasonSucceeded
	if phase == v1alpha1.BackupPhaseSucceeded {
		next.Phase = v1alpha1.VerificationPhaseSucceeded
	} else {
		next.Phase = v1alpha1.VerificationPhaseFailed
		reason = v1alpha1.ReasonFailed
		// Attribute the failure to the sanity-check phase when verify.sh
		// says that's where it stopped. Operators get a distinct reason they
		// can filter on (SanityCheckFailed vs the generic RestoreFailed)
		// without having to grep pod logs.
		switch {
		case pitrFailureReason(message) != "":
			reason = string(pitrFailureReason(message))
		case strings.HasPrefix(message, "SanityCheckTimeout"), sentinels.Sanity != nil && sentinels.Sanity.Outcome == v1alpha1.SanityCheckOutcomeTimedOut:
			reason = v1alpha1.ReasonSanityCheckTimeout
		case strings.HasPrefix(message, "SanityCheckFailed"), sentinels.Sanity != nil && sentinels.Sanity.Outcome == v1alpha1.SanityCheckOutcomeFailed:
			reason = v1alpha1.ReasonSanityCheckFailed
		}
	}
	conditionStatus := metav1.ConditionTrue
	if phase != v1alpha1.BackupPhaseSucceeded {
		conditionStatus = metav1.ConditionFalse
	}
	setCondition(&next.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionVerified,
		Status:             conditionStatus,
		ObservedGeneration: v.Generation,
		LastTransitionTime: completion,
		Reason:             reason,
		Message:            message,
	})
	next.CompletionTime = &completion
	if next.StartTime != nil {
		next.DurationSeconds = int64(completion.Sub(next.StartTime.Time).Seconds())
	}
	next.Message = message
	next.ObservedGeneration = v.Generation
	next.JobName = job.Name
	setVerificationReady(&next, v.Generation)

	if v.Status.Phase == next.Phase && equality.Semantic.DeepEqual(v.Status, &next) {
		// Idempotent — just run post-terminal cleanup + retention.
		requeue, err := r.maybeCleanupAfterTerminal(ctx, v)
		if err != nil {
			logger.Error(err, "post-terminal cleanup")
		}
		requeue = shorterRequeue(requeue, r.maybeReleaseHold(ctx, v))
		if err := r.pruneRetention(ctx, v); err != nil {
			logger.Error(err, "retention prune")
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	patch := client.MergeFrom(v.DeepCopy())
	v.Status = next
	if err := r.Status().Patch(ctx, v, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch final status: %w", err)
	}

	execution := v.DeepCopy()
	if group := job.Spec.Template.Labels[v1alpha1.FailoverGroupLabel]; group != "" {
		execution.Spec.FailoverGroupRef.Name = group
	}
	if profile := job.Spec.Template.Labels[v1alpha1.BackupProfileLabel]; profile != "" {
		execution.Spec.ProfileName = profile
	}
	r.emitTerminalMetrics(execution, verificationJobPITRMode(job))

	if next.Phase == v1alpha1.VerificationPhaseSucceeded {
		emitPointInTimeEvents(r.Recorder, v, next.PointInTime)
		r.Recorder.Eventf(v, corev1.EventTypeNormal, v1alpha1.EventVerificationSucceeded,
			"verification %s succeeded: %s", v.Name, message)
	} else {
		r.Recorder.Eventf(v, corev1.EventTypeWarning, v1alpha1.EventVerificationFailed,
			"verification %s failed: %s", v.Name, message)
	}

	// Cleanup: success → full cleanup; failure with KeepOnFailure=false
	// → full cleanup; failure with KeepOnFailure=true → keep PVC+Pod
	// for inspection, retention sweep reclaims later.
	requeue, err := r.maybeCleanupAfterTerminal(ctx, v)
	if err != nil {
		logger.Error(err, "post-terminal cleanup")
	}
	requeue = shorterRequeue(requeue, r.maybeReleaseHold(ctx, v))
	if err := r.pruneRetention(ctx, v); err != nil {
		logger.Error(err, "retention prune")
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// SetupWithManager registers the reconciler with the manager.
func (r *MysqlBackupVerificationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlBackupVerification{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}

// resolveBackup returns the MysqlBackup targeted by this verification.
// If spec.backupRef is set, looks it up; otherwise picks the latest
// Succeeded MysqlBackup for the (group, profile) pair.
func (r *MysqlBackupVerificationReconciler) resolveBackup(ctx context.Context, v *v1alpha1.MysqlBackupVerification) (*v1alpha1.MysqlBackup, error) {
	if v.Spec.BackupRef != nil && v.Spec.BackupRef.Name != "" {
		var backup v1alpha1.MysqlBackup
		if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: v.Spec.BackupRef.Name}, &backup); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("referenced MysqlBackup %q not found", v.Spec.BackupRef.Name)
			}
			return nil, fmt.Errorf("get referenced backup: %w", err)
		}
		if backup.Status.Phase != v1alpha1.BackupPhaseSucceeded {
			return nil, fmt.Errorf("referenced MysqlBackup %q is not Succeeded (phase=%q)", backup.Name, backup.Status.Phase)
		}
		if !backup.DeletionTimestamp.IsZero() {
			return nil, fmt.Errorf("referenced MysqlBackup %q is being deleted", backup.Name)
		}
		if backup.Spec.FailoverGroupRef.Name != v.Spec.FailoverGroupRef.Name ||
			backup.Spec.ProfileName != v.Spec.ProfileName {
			return nil, fmt.Errorf("referenced MysqlBackup %q does not match (group=%s, profile=%s)",
				backup.Name, v.Spec.FailoverGroupRef.Name, v.Spec.ProfileName)
		}
		return &backup, nil
	}

	// Latest Succeeded for (group, profile).
	var list v1alpha1.MysqlBackupList
	if err := r.List(ctx, &list,
		client.InNamespace(v.Namespace),
		client.MatchingLabels{
			v1alpha1.FailoverGroupLabel: v.Spec.FailoverGroupRef.Name,
			v1alpha1.BackupProfileLabel: v.Spec.ProfileName,
		},
	); err != nil {
		return nil, fmt.Errorf("list backups: %w", err)
	}
	var best *v1alpha1.MysqlBackup
	for i := range list.Items {
		b := &list.Items[i]
		if b.Status.Phase != v1alpha1.BackupPhaseSucceeded || !b.DeletionTimestamp.IsZero() {
			continue
		}
		if b.Status.CompletionTime == nil {
			continue
		}
		if best == nil || b.Status.CompletionTime.After(best.Status.CompletionTime.Time) {
			best = b
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no Succeeded MysqlBackup found for group=%s, profile=%s",
			v.Spec.FailoverGroupRef.Name, v.Spec.ProfileName)
	}
	return best, nil
}

// findBlockingVerification returns the name of another non-terminal
// MysqlBackupVerification for the same (group, profile) pair that was
// created strictly earlier than v. Empty result means nothing is
// blocking us.
func (r *MysqlBackupVerificationReconciler) findBlockingVerification(ctx context.Context, v *v1alpha1.MysqlBackupVerification) (string, error) {
	// List in-namespace and filter by spec rather than matching on
	// labels — ad-hoc CRs created via `kubectl create -f` without
	// labels, and the narrow window before a brand-new CR has been
	// label-stamped, would otherwise bypass single-flight entirely.
	var list v1alpha1.MysqlBackupVerificationList
	if err := r.List(ctx, &list, client.InNamespace(v.Namespace)); err != nil {
		return "", err
	}
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == v.Name {
			continue
		}
		if other.Spec.FailoverGroupRef.Name != v.Spec.FailoverGroupRef.Name ||
			other.Spec.ProfileName != v.Spec.ProfileName {
			continue
		}
		if !other.DeletionTimestamp.IsZero() {
			continue
		}
		if verificationTerminal(other.Status.Phase) {
			continue
		}
		if other.CreationTimestamp.Before(&v.CreationTimestamp) {
			return other.Name, nil
		}
	}
	return "", nil
}

// maybeReleaseHold releases a terminal verification's hold once its pods
// have stopped, and returns how soon to look again while they have not.
func (r *MysqlBackupVerificationReconciler) maybeReleaseHold(ctx context.Context, v *v1alpha1.MysqlBackupVerification) time.Duration {
	done, err := r.releaseVerificationHold(ctx, v)
	if err != nil {
		log.FromContext(ctx).Error(err, "release verification hold")
		return 5 * time.Second
	}
	if !done {
		return 30 * time.Second
	}
	return 0
}

// shorterRequeue returns the shorter non-zero delay.
func shorterRequeue(a, b time.Duration) time.Duration {
	if a == 0 || b != 0 && b < a {
		return b
	}
	return a
}

func verificationTerminal(p v1alpha1.VerificationPhase) bool {
	return p == v1alpha1.VerificationPhaseSucceeded || p == v1alpha1.VerificationPhaseFailed
}

// ensureVerificationCredsSecret creates the derived Secret used by the
// verification Job, pulled from the group's backup credential secret. The
// Secret is owned by the verification CR so it GCs with the CR.
//
// The verification Pod uses an in-pod synthetic root for the ephemeral
// mysqld, so this Secret's MYSQL_USER / MYSQL_PASSWORD are only needed
// for cloud-storage credential flow-through in later phases. We
// provision it here for parity with backup/restore so future phases
// don't reintroduce the plumbing.
func (r *MysqlBackupVerificationReconciler) ensureVerificationCredsSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, v *v1alpha1.MysqlBackupVerification, secretName string) error {
	var user, password string

	backupSecretName := fg.Spec.EffectiveBackupSecretName()
	var srcSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: backupSecretName}, &srcSecret); err != nil {
		return fmt.Errorf("get backup credential secret %s: %w", backupSecretName, err)
	}
	user = string(srcSecret.Data["username"])
	password = string(srcSecret.Data["password"])

	derived := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: v.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, derived, func() error {
		if err := controllerutil.SetControllerReference(v, derived, r.Scheme); err != nil {
			return err
		}
		if derived.Labels == nil {
			derived.Labels = map[string]string{}
		}
		derived.Labels[v1alpha1.FailoverGroupLabel] = fg.Name
		derived.Labels[v1alpha1.MysqlBackupVerificationLabel] = v.Name
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

// setVerificationReady sets Ready from Verified: a copy of it once the
// verification has a verdict, False/InProgress before.
func setVerificationReady(st *v1alpha1.MysqlBackupVerificationStatus, generation int64) {
	c := metav1.Condition{
		Type:               v1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
		Reason:             v1alpha1.ReasonInProgress,
		Message:            "the verification has not finished",
	}
	if verified := apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionVerified); verified != nil {
		c.Status, c.Reason, c.Message, c.LastTransitionTime = verified.Status, verified.Reason, verified.Message, verified.LastTransitionTime
	}
	setCondition(&st.Conditions, c)
}

// failVerification transitions a verification to Failed with the given
// reason. Mirrors MysqlBackupReconciler.failBackup.
func (r *MysqlBackupVerificationReconciler) failVerification(ctx context.Context, v *v1alpha1.MysqlBackupVerification, reason, message string) (ctrl.Result, error) {
	now := metav1.Now()
	patch := client.MergeFrom(v.DeepCopy())
	v.Status.Phase = v1alpha1.VerificationPhaseFailed
	v.Status.CompletionTime = &now
	if v.Status.StartTime != nil {
		v.Status.DurationSeconds = int64(now.Sub(v.Status.StartTime.Time).Seconds())
	}
	v.Status.Message = message
	v.Status.ObservedGeneration = v.Generation
	setCondition(&v.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionVerified,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: v.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
	setVerificationReady(&v.Status, v.Generation)
	if err := r.Status().Patch(ctx, v, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch failed status: %w", err)
	}
	r.Recorder.Eventf(v, corev1.EventTypeWarning, reason, "%s", message)
	r.emitTerminalMetrics(v, "")
	requeue, cerr := r.maybeCleanupAfterTerminal(ctx, v)
	if cerr != nil {
		log.FromContext(ctx).Error(cerr, "post-terminal cleanup")
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// maybeCleanupAfterTerminal implements the success/failure cleanup
// policy. Success deletes ephemeral resources once
// spec.ttlSecondsAfterFinished elapses (0: at once) and keeps them until
// retention deletes the verification when the TTL is unset; failure
// retains them iff spec.keepOnFailure is true (the default), and
// otherwise deletes them after the TTL, or at once when it is unset.
//
// Returns a non-zero requeue duration when the TTL gate is currently
// deferring deletion — the caller must propagate it via
// ctrl.Result{RequeueAfter}, otherwise a successful verification with
// a non-zero TTL would never fire cleanup after the controller cache
// quiesces.
func (r *MysqlBackupVerificationReconciler) maybeCleanupAfterTerminal(ctx context.Context, v *v1alpha1.MysqlBackupVerification) (time.Duration, error) {
	keepOnFailure := true
	if v.Spec.KeepOnFailure != nil {
		keepOnFailure = *v.Spec.KeepOnFailure
	}
	switch v.Status.Phase {
	case v1alpha1.VerificationPhaseSucceeded:
		if v.Spec.TTLSecondsAfterFinished == nil {
			return 0, nil
		}
		return r.cleanupEphemeral(ctx, v, false)
	case v1alpha1.VerificationPhaseFailed:
		if keepOnFailure {
			return 0, nil
		}
		return r.cleanupEphemeral(ctx, v, false)
	}
	return 0, nil
}

// cleanupEphemeral deletes the Job, PVC, and derived Secret owned by a
// verification. `force=true` is used on the deletion path and skips the
// TTL gate. Returns (remainingTTL, nil) when the TTL has not elapsed
// and cleanup is deferred; (0, nil) after a successful pass; and
// (0, err) on hard failure.
func (r *MysqlBackupVerificationReconciler) cleanupEphemeral(ctx context.Context, v *v1alpha1.MysqlBackupVerification, force bool) (time.Duration, error) {
	if !force {
		var ttl time.Duration
		if v.Spec.TTLSecondsAfterFinished != nil {
			ttl = time.Duration(*v.Spec.TTLSecondsAfterFinished) * time.Second
		}
		if ttl > 0 && v.Status.CompletionTime != nil {
			elapsed := time.Since(v.Status.CompletionTime.Time)
			if elapsed < ttl {
				return ttl - elapsed, nil
			}
		}
	}

	jobName := verificationJobName(v.Name)
	secretName := verificationCredsSecretName(v.Name)

	// Job (propagation=Foreground so pods go with it). The ephemeral
	// datadir is an emptyDir inside the Job pod, so deleting the Job
	// reclaims the datadir storage too — there is no separate PVC to
	// delete here.
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: jobName}, &job); err == nil {
		fg := metav1.DeletePropagationForeground
		if err := r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &fg}); err != nil && !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("delete verification job: %w", err)
		}
	} else if !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("get verification job for cleanup: %w", err)
	}

	// Derived creds Secret.
	var s corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: secretName}, &s); err == nil {
		if err := r.Delete(ctx, &s); err != nil && !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("delete verification creds secret: %w", err)
		}
	} else if !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("get verification creds for cleanup: %w", err)
	}

	// Best-effort status patch recording cleanup. Separate from the
	// main terminal status so an error here doesn't undo the
	// Succeeded/Failed transition.
	if !hasCondition(v.Status.Conditions, v1alpha1.ConditionResourcesCleanedUp) {
		now := metav1.Now()
		patch := client.MergeFrom(v.DeepCopy())
		setCondition(&v.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionResourcesCleanedUp,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: v.Generation,
			LastTransitionTime: now,
			Reason:             v1alpha1.ReasonCleanedUp,
			Message:            "ephemeral resources deleted",
		})
		if err := r.Status().Patch(ctx, v, patch); err != nil && !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("patch cleanup condition: %w", err)
		}
	}

	return 0, nil
}

func hasCondition(conds []metav1.Condition, t string) bool {
	for i := range conds {
		if conds[i].Type == t && conds[i].Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// emitTerminalMetrics publishes the verification Prometheus metrics for
// a single terminal observation. Called at most once per terminal CR
// via the semantic-equality short-circuit upstream.
// verificationJobPITRMode returns the PITR mode rendered into the Job, or ""
// when the Job replays no archive.
func verificationJobPITRMode(job *batchv1.Job) string {
	for _, init := range job.Spec.Template.Spec.InitContainers {
		if init.Name != restorePITRInitContainerName {
			continue
		}
		for _, env := range init.Env {
			if env.Name == "BLOODRAVEN_PITR_MODE" && env.Value != "" {
				return env.Value
			}
		}
		return "timestamp"
	}
	return ""
}

func (r *MysqlBackupVerificationReconciler) emitTerminalMetrics(v *v1alpha1.MysqlBackupVerification, pitrMode string) {
	group := v.Spec.FailoverGroupRef.Name
	profile := v.Spec.ProfileName
	result := "failure"
	if v.Status.Phase == v1alpha1.VerificationPhaseSucceeded {
		result = "success"
	}
	bmetrics.BackupVerificationRunsTotal.WithLabelValues(group, profile, result).Inc()

	var end time.Time
	if v.Status.CompletionTime != nil {
		end = v.Status.CompletionTime.Time
	}
	if !end.IsZero() {
		bmetrics.BackupVerificationLastAttemptTimestamp.WithLabelValues(group, profile).
			Set(float64(end.Unix()))
	}
	if v.Status.Phase == v1alpha1.VerificationPhaseSucceeded && !end.IsZero() {
		bmetrics.BackupVerifiedTimestamp.WithLabelValues(group, profile).
			Set(float64(end.Unix()))
	}
	if v.Status.StartTime != nil && !end.IsZero() && end.After(v.Status.StartTime.Time) {
		bmetrics.BackupVerificationDurationSeconds.WithLabelValues(group, profile).
			Observe(end.Sub(v.Status.StartTime.Time).Seconds())
	}
	// Replay lag measures archive freshness, so only a latest drill sets
	// it; a timestamp drill's newest event is its historical target.
	if pitrMode == "latest" && v.Status.Phase == v1alpha1.VerificationPhaseSucceeded &&
		v.Status.PointInTime != nil &&
		v.Status.PointInTime.NewestReplayedCommitTime != nil &&
		!end.IsZero() {
		lag := end.Sub(v.Status.PointInTime.NewestReplayedCommitTime.Time).Seconds()
		if lag < 0 {
			lag = 0
		}
		bmetrics.BackupVerificationReplayLagSeconds.WithLabelValues(group, profile).Set(lag)
	}
}

// verifySentinels bundles the parsed output of the verify.sh post-load
// sentinel. Sanity is nil when the phase did not run or the log is
// unreadable; it never decides the outcome. LogsRead is true only after
// the complete bounded log scan, so a nil Sanity with LogsRead means the
// sanity phase did not run.
type verifySentinels struct {
	Sanity   *v1alpha1.SanityCheckResult
	LogsRead bool
}

// tailVerificationSentinels reads the verification Job pod's logs and
// parses the trailing BLOODRAVEN_VERIFY_SANITY_COMPLETE sentinel into a
// structured status fragment. (verify.sh no longer prints
// BLOODRAVEN_VERIFY_REPLAY_COMPLETE; replay detail comes from the PITR
// termination verdict.) LogsRead is true only after the complete bounded log scan;
// a read failure cannot certify either present or absent completion metadata.
func (r *MysqlBackupVerificationReconciler) tailVerificationSentinels(ctx context.Context, v *v1alpha1.MysqlBackupVerification, job *batchv1.Job) verifySentinels {
	out := verifySentinels{}
	if r.Clientset == nil {
		return out
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{v1alpha1.MysqlBackupVerificationLabel: v.Name},
	); err != nil {
		return out
	}
	// Prefer the Succeeded pod; on Failed runs the pod phase is Failed
	// but we still want its tail. Take the most recent terminal pod.
	var chosen *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if job.UID != "" && !podBelongsToJob(p, job) {
			continue
		}
		if p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			continue
		}
		if chosen == nil || p.CreationTimestamp.After(chosen.CreationTimestamp.Time) {
			chosen = p
		}
	}
	if chosen == nil {
		return out
	}

	req := r.Clientset.CoreV1().Pods(job.Namespace).GetLogs(chosen.Name, &corev1.PodLogOptions{
		Container: backupJobContainerName,
		TailLines: ptr64(200),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return out
	}
	defer func() { _ = stream.Close() }()

	// TailLines bounds the read; long GTID lines need a larger buffer.
	sc := bufio.NewScanner(stream)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var sanity *v1alpha1.SanityCheckResult
	for sc.Scan() {
		if parsed, ok := parseSanitySentinel(sc.Text()); ok {
			sanity = parsed
		}
	}
	if err := sc.Err(); err != nil {
		log.FromContext(ctx).Error(err, "failed to scan verification sentinel log output",
			"pod", chosen.Name, "namespace", job.Namespace, "job", job.Name)
		return out
	}
	out.Sanity = sanity
	out.LogsRead = true
	return out
}

// parseSanitySentinel decodes version-2 sanity values and legacy errors.
// verify.sh prints the sentinel only after the query ran: error=timeout is
// a timeout, any other error a failed query or a result below minResult.
func parseSanitySentinel(line string) (*v1alpha1.SanityCheckResult, bool) {
	tokens, ok := backupmeta.ParseTokens(line, "BLOODRAVEN_VERIFY_SANITY_COMPLETE")
	if !ok {
		return nil, false
	}
	res := &v1alpha1.SanityCheckResult{Outcome: v1alpha1.SanityCheckOutcomePassed}
	if n, err := strconv.ParseInt(tokens["durationMs"], 10, 64); err == nil {
		res.DurationMilliseconds = &n
	}
	res.Result = tokens["resultRow"]
	res.Error = tokens["error"]
	if tokens["v"] == "" {
		// Older scripts encoded spaces in errors as underscores. Their
		// original scalars contain no whitespace and remain literal.
		res.Error = strings.ReplaceAll(res.Error, "_", " ")
	}
	switch {
	case strings.EqualFold(res.Error, "timeout"):
		res.Outcome = v1alpha1.SanityCheckOutcomeTimedOut
	case res.Error != "":
		res.Outcome = v1alpha1.SanityCheckOutcomeFailed
	}
	return res, true
}

// pruneRetention deletes old MysqlBackupVerification CRs for the same
// (group, profile). Succeeded and Failed buckets are capped
// independently; failed-keep defaults to 10 so operators can always
// find the last few failures to debug.
func (r *MysqlBackupVerificationReconciler) pruneRetention(ctx context.Context, trigger *v1alpha1.MysqlBackupVerification) error {
	var list v1alpha1.MysqlBackupVerificationList
	if err := r.List(ctx, &list, client.InNamespace(trigger.Namespace)); err != nil {
		return err
	}

	filtered := list.Items[:0]
	for _, it := range list.Items {
		if it.Spec.FailoverGroupRef.Name != trigger.Spec.FailoverGroupRef.Name {
			continue
		}
		if it.Spec.ProfileName != trigger.Spec.ProfileName {
			continue
		}
		if !it.DeletionTimestamp.IsZero() {
			continue
		}
		filtered = append(filtered, it)
	}

	var profile *v1alpha1.BackupProfile
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: trigger.Namespace, Name: trigger.Spec.FailoverGroupRef.Name}, &fg); err == nil {
		profile = findProfile(&fg, trigger.Spec.ProfileName)
	}
	keepLast, keepFailed := resolveVerificationRetention(profile)

	if err := r.pruneVerificationsByPhase(ctx, filtered, v1alpha1.VerificationPhaseSucceeded, int(keepLast)); err != nil {
		return err
	}
	return r.pruneVerificationsByPhase(ctx, filtered, v1alpha1.VerificationPhaseFailed, int(keepFailed))
}

// resolveVerificationRetention returns the effective (keepLast,
// keepFailed) counts for the verifications of profile: the profile's
// verification.retention, with the defaults for a missing profile,
// verification block or field. An explicit 0 is kept: it means keep none.
func resolveVerificationRetention(profile *v1alpha1.BackupProfile) (keepLast, keepFailed int32) {
	keepLast, keepFailed = defaultVerificationKeepLast, defaultVerificationKeepFailed
	if profile == nil || profile.Verification == nil {
		return keepLast, keepFailed
	}
	rt := profile.Verification.Retention
	if rt.KeepLast != nil {
		keepLast = max(*rt.KeepLast, 0)
	}
	if rt.KeepFailed != nil {
		keepFailed = max(*rt.KeepFailed, 0)
	}
	return keepLast, keepFailed
}

// pruneVerificationsByPhase keeps the newest `keep` entries in the
// given phase and deletes the rest; 0 keeps none. Only terminal phases
// are passed, so an in-flight CR is never swept; entries without a
// CompletionTime sort last.
func (r *MysqlBackupVerificationReconciler) pruneVerificationsByPhase(ctx context.Context, all []v1alpha1.MysqlBackupVerification, phase v1alpha1.VerificationPhase, keep int) error {
	keep = max(keep, 0)
	bucket := make([]v1alpha1.MysqlBackupVerification, 0, len(all))
	for _, it := range all {
		if it.Status.Phase == phase {
			bucket = append(bucket, it)
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
			return fmt.Errorf("delete old %s verification %s: %w", phase, victim.Name, err)
		}
	}
	return nil
}

// waitVerification records why a verification has not launched yet and
// requeues after pitrReadBindingRequeue (the read binding is not
// ready, Δ22). The status write is skipped when the message is unchanged.
func (r *MysqlBackupVerificationReconciler) waitVerification(ctx context.Context, v *v1alpha1.MysqlBackupVerification, message string) (ctrl.Result, error) {
	if v.Status.Message != message {
		patch := client.MergeFrom(v.DeepCopy())
		v.Status.Message = message
		v.Status.ObservedGeneration = v.Generation
		setVerificationReady(&v.Status, v.Generation)
		if err := r.Status().Patch(ctx, v, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch waiting status: %w", err)
		}
	}
	return ctrl.Result{RequeueAfter: pitrReadBindingRequeue}, nil
}

// verificationDumpKeyMissing explains why the dump-key Secret an encrypted
// backup recorded (verificationDumpKey) cannot be mounted, or "" (also for
// a plaintext backup). The read is uncached.
func (r *MysqlBackupVerificationReconciler) verificationDumpKeyMissing(ctx context.Context, backup *v1alpha1.MysqlBackup) (string, error) {
	ref, err := verificationDumpKey(backup)
	if err != nil || ref == nil {
		return "", nil // buildVerificationJob already refused a missing record
	}
	var s corev1.Secret
	err = r.apiReader().Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: ref.Name}, &s)
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Sprintf("%s: backup %q is encrypted and the Secret %q it recorded as its dump key is gone; restore that Secret",
			pitrarchive.TokenBackupDumpKeyMissing, backup.Name, ref.Name), nil
	case err != nil:
		return "", fmt.Errorf("get recorded dump key %s: %w", ref.Name, err)
	}
	key := ref.Key
	if key == "" {
		key = pitrKeySnapshotDataKey
	}
	if len(s.Data[key]) == 0 {
		return fmt.Sprintf("%s: backup %q is encrypted and the Secret %q it recorded as its dump key has no key %q",
			pitrarchive.TokenBackupDumpKeyMissing, backup.Name, ref.Name, key), nil
	}
	return "", nil
}
