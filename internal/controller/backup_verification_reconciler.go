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

// Condition types on MysqlBackupVerification.status.conditions.
const (
	ConditionVerified          = "Verified"
	ConditionResourcesCleanedUp = "ResourcesCleanedUp"
)

// defaultVerificationKeepSuccessful / KeepFailures are the retention
// caps applied when a profile's Verification.RetentionPolicy is not
// set. Failed runs are kept separately because they're what operators
// most want to investigate.
const (
	defaultVerificationKeepSuccessful = int32(30)
	defaultVerificationKeepFailures   = int32(10)
)

// MysqlBackupVerificationReconciler reconciles MysqlBackupVerification CRs.
type MysqlBackupVerificationReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Clientset is optional; when non-nil the reconciler tails the
	// verification Job pod logs for the BLOODRAVEN_VERIFY_REPLAY_COMPLETE
	// and BLOODRAVEN_VERIFY_SANITY_COMPLETE sentinels and populates
	// status.replayedThroughBinlog / status.sanityCheck accordingly.
	// Fake-client tests leave this nil; status will then only carry the
	// terminal Phase and Conditions.
	Clientset kubernetes.Interface
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications/finalizers,verbs=update
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups,verbs=get;list;watch
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

	// Deletion path: clean up artifacts then drop the finalizer.
	if !v.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&v, mysqlBackupVerificationFinalizer) {
			if _, err := r.cleanupEphemeral(ctx, &v, true); err != nil {
				return ctrl.Result{RequeueAfter: 15 * time.Second}, fmt.Errorf("cleanup on delete: %w", err)
			}
			controllerutil.RemoveFinalizer(&v, mysqlBackupVerificationFinalizer)
			if err := r.Update(ctx, &v); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Stamp finalizer + labels on first encounter.
	if !controllerutil.ContainsFinalizer(&v, mysqlBackupVerificationFinalizer) {
		controllerutil.AddFinalizer(&v, mysqlBackupVerificationFinalizer)
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
		if err := r.pruneRetention(ctx, &v); err != nil {
			logger.Error(err, "retention prune")
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	// Resolve the referenced failover group.
	var fg v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Namespace: v.Namespace, Name: v.Spec.FailoverGroupRef.Name}
	if err := r.Get(ctx, fgKey, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failVerification(ctx, &v, "GroupNotFound",
				fmt.Sprintf("MysqlFailoverGroup %q not found", fgKey.Name))
		}
		return ctrl.Result{}, fmt.Errorf("get failover group: %w", err)
	}

	// Coalesce owner-ref + label stamp.
	needsUpdate := false
	if len(v.OwnerReferences) == 0 {
		if err := controllerutil.SetControllerReference(&fg, &v, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set owner ref: %w", err)
		}
		needsUpdate = true
	}
	if ensureVerificationLabels(&v) {
		needsUpdate = true
	}
	if needsUpdate {
		if err := r.Update(ctx, &v); err != nil {
			return ctrl.Result{}, fmt.Errorf("update owner ref / labels: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve the named profile.
	profile := findProfile(&fg, v.Spec.ProfileName)
	if profile == nil {
		return r.failVerification(ctx, &v, "ProfileNotFound",
			fmt.Sprintf("profile %q not found on MysqlFailoverGroup %q", v.Spec.ProfileName, fg.Name))
	}

	// Enforce per-profile single-flight: if another non-terminal
	// verification for the same (group, profile) exists and was
	// created earlier than this one, refuse to run concurrently.
	if blocked, err := r.findBlockingVerification(ctx, &v); err != nil {
		return ctrl.Result{}, fmt.Errorf("check concurrent verifications: %w", err)
	} else if blocked != "" {
		return r.failVerification(ctx, &v, "BlockedByActiveVerification",
			fmt.Sprintf("another verification %q for (group=%s, profile=%s) is still running",
				blocked, v.Spec.FailoverGroupRef.Name, v.Spec.ProfileName))
	}

	// Resolve the target MysqlBackup (explicit ref, or the latest
	// Succeeded for this profile).
	backup, err := r.resolveBackup(ctx, &v)
	if err != nil {
		return r.failVerification(ctx, &v, "BackupNotAvailable", err.Error())
	}

	// Stamp the resolved ref onto status the moment we know it so a
	// later cleanup can still identify the source.
	if v.Status.BackupRef == nil || v.Status.BackupRef.Name != backup.Name {
		patch := client.MergeFrom(v.DeepCopy())
		v.Status.BackupRef = &v1alpha1.VerificationBackupRef{
			Name:        backup.Name,
			UID:         string(backup.UID),
			Location:    backup.Status.Location,
			StorageType: backup.Status.StorageType,
		}
		if err := r.Status().Patch(ctx, &v, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch backupRef: %w", err)
		}
	}

	// Ensure the derived creds Secret (S3 workloads consult it; the
	// ephemeral mysqld uses an in-pod synthetic root).
	credsName := verificationCredsSecretName(v.Name)
	if err := r.ensureVerificationCredsSecret(ctx, &fg, &v, credsName); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure verification creds: %w", err)
	}

	// Ensure ephemeral PVC.
	pvcName := verificationPVCName(v.Name)
	if err := r.ensureVerificationPVC(ctx, &v, *profile, backup); err != nil {
		return r.failVerification(ctx, &v, "PVCProvisioningFailed", err.Error())
	}

	// Phase transition: Pending → Provisioning on first pass.
	if v.Status.Phase == "" || v.Status.Phase == v1alpha1.VerificationPhasePending {
		now := metav1.Now()
		patch := client.MergeFrom(v.DeepCopy())
		v.Status.Phase = v1alpha1.VerificationPhaseProvisioning
		v.Status.StartTime = &now
		v.Status.PVCName = pvcName
		v.Status.Message = "ephemeral resources provisioned"
		v.Status.ObservedGeneration = v.Generation
		if err := r.Status().Patch(ctx, &v, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch provisioning status: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Ensure the Job exists.
	jobName := verificationJobName(v.Name)
	jobKey := types.NamespacedName{Namespace: v.Namespace, Name: jobName}
	var job batchv1.Job
	if err := r.Get(ctx, jobKey, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get verification job: %w", err)
		}
		built, err := buildVerificationJob(verificationJobInputs{
			FailoverGroup:        &fg,
			Profile:              *profile,
			Verification:         &v,
			Backup:               backup,
			CredsSecretName:      credsName,
			ScriptsConfigMapName: backupScriptsConfigMapName(fg.Name),
			PVCName:              pvcName,
		})
		if err != nil {
			return r.failVerification(ctx, &v, "BuildJobFailed", err.Error())
		}
		if err := controllerutil.SetControllerReference(&v, built, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set job owner ref: %w", err)
		}
		if err := r.Create(ctx, built); err != nil {
			return ctrl.Result{}, fmt.Errorf("create verification job: %w", err)
		}
		r.Recorder.Eventf(&v, corev1.EventTypeNormal, "VerificationStarted",
			"created verification Job %s (backup=%s, profile=%s)", built.Name, backup.Name, profile.Name)

		patch := client.MergeFrom(v.DeepCopy())
		v.Status.Phase = v1alpha1.VerificationPhaseRestoring
		v.Status.JobName = built.Name
		v.Status.Message = "running restore Job against ephemeral mysqld"
		v.Status.ObservedGeneration = v.Generation
		if err := r.Status().Patch(ctx, &v, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch running status: %w", err)
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Observe the Job. Status translation reuses the backup helper.
	phase, message := jobPhase(&job, "verification")
	if phase == "" {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Terminal — build next status and patch iff different, then emit
	// metrics exactly once.
	next := *v.Status.DeepCopy()
	stable := stableJobCompletionTime(&job)
	var completion metav1.Time
	if stable != nil {
		completion = *stable
	} else {
		completion = metav1.Now()
	}

	// Parse the verify.sh log sentinels for PITR replay + sanity check
	// details. Nil sentinels (missing Clientset, older Jobs) leave the
	// status fields at zero value and do not fail the reconcile.
	sentinels := r.tailVerificationSentinels(ctx, &v, &job)
	if sentinels.Replay != nil {
		next.ReplayedThroughBinlog = sentinels.Replay
	}
	if sentinels.Sanity != nil {
		next.SanityCheck = sentinels.Sanity
	}

	reason := "Succeeded"
	if phase == v1alpha1.BackupPhaseSucceeded {
		next.Phase = v1alpha1.VerificationPhaseSucceeded
	} else {
		next.Phase = v1alpha1.VerificationPhaseFailed
		reason = "Failed"
		// Attribute the failure to the sanity-check phase when the
		// sentinel tells us that's where we stopped. Operators get a
		// distinct reason they can filter on (SanityCheckFailed vs the
		// generic RestoreFailed) without having to grep pod logs.
		if sentinels.Sanity != nil && sentinels.Sanity.Error != "" {
			if strings.EqualFold(sentinels.Sanity.Error, "timeout") {
				reason = "SanityCheckTimeout"
			} else {
				reason = "SanityCheckFailed"
			}
		}
	}
	conditionStatus := metav1.ConditionTrue
	if phase != v1alpha1.BackupPhaseSucceeded {
		conditionStatus = metav1.ConditionFalse
	}
	setCondition(&next.Conditions, metav1.Condition{
		Type:               ConditionVerified,
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
	next.PVCName = pvcName

	if v.Status.Phase == next.Phase && equality.Semantic.DeepEqual(&v.Status, &next) {
		// Idempotent — just run post-terminal cleanup + retention.
		requeue, err := r.maybeCleanupAfterTerminal(ctx, &v)
		if err != nil {
			logger.Error(err, "post-terminal cleanup")
		}
		if err := r.pruneRetention(ctx, &v); err != nil {
			logger.Error(err, "retention prune")
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	patch := client.MergeFrom(v.DeepCopy())
	v.Status = next
	if err := r.Status().Patch(ctx, &v, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch final status: %w", err)
	}

	r.emitTerminalMetrics(&v)

	if next.Phase == v1alpha1.VerificationPhaseSucceeded {
		r.Recorder.Eventf(&v, corev1.EventTypeNormal, "VerificationSucceeded",
			"verification %s succeeded: %s", v.Name, message)
	} else {
		r.Recorder.Eventf(&v, corev1.EventTypeWarning, "VerificationFailed",
			"verification %s failed: %s", v.Name, message)
	}

	// Cleanup: success → full cleanup; failure with KeepOnFailure=false
	// → full cleanup; failure with KeepOnFailure=true → keep PVC+Pod
	// for inspection, retention sweep reclaims later.
	requeue, err := r.maybeCleanupAfterTerminal(ctx, &v)
	if err != nil {
		logger.Error(err, "post-terminal cleanup")
	}
	if err := r.pruneRetention(ctx, &v); err != nil {
		logger.Error(err, "retention prune")
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// SetupWithManager registers the reconciler with the manager.
func (r *MysqlBackupVerificationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlBackupVerification{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
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
			labelFailoverGroup: v.Spec.FailoverGroupRef.Name,
			labelBackupProfile: v.Spec.ProfileName,
		},
	); err != nil {
		return nil, fmt.Errorf("list backups: %w", err)
	}
	var best *v1alpha1.MysqlBackup
	for i := range list.Items {
		b := &list.Items[i]
		if b.Status.Phase != v1alpha1.BackupPhaseSucceeded {
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

func verificationTerminal(p v1alpha1.VerificationPhase) bool {
	return p == v1alpha1.VerificationPhaseSucceeded || p == v1alpha1.VerificationPhaseFailed
}

// ensureVerificationPVC provisions the ephemeral datadir PVC.
func (r *MysqlBackupVerificationReconciler) ensureVerificationPVC(ctx context.Context, v *v1alpha1.MysqlBackupVerification, profile v1alpha1.BackupProfile, backup *v1alpha1.MysqlBackup) error {
	pvc := buildVerificationPVC(verificationJobInputs{
		FailoverGroup: &v1alpha1.MysqlFailoverGroup{
			ObjectMeta: metav1.ObjectMeta{Name: v.Spec.FailoverGroupRef.Name, Namespace: v.Namespace},
		},
		Profile:      profile,
		Verification: v,
		Backup:       backup,
	})
	var existing corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: pvc.Name}, &existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get verification pvc: %w", err)
		}
		if err := controllerutil.SetControllerReference(v, pvc, r.Scheme); err != nil {
			return fmt.Errorf("set pvc owner ref: %w", err)
		}
		if err := r.Create(ctx, pvc); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return nil
			}
			return fmt.Errorf("create verification pvc: %w", err)
		}
		return nil
	}
	return nil
}

// ensureVerificationCredsSecret creates the derived Secret used by the
// verification Job. In credentials mode, pulls from the group's backup
// credential secret; in legacy mode, parses the DSN. The Secret is
// owned by the verification CR so it GCs with the CR.
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
	if fg.Spec.UsesCredentials() {
		user = string(srcSecret.Data["username"])
		password = string(srcSecret.Data["password"])
	} else {
		dsnBytes, ok := srcSecret.Data["dsn"]
		if !ok {
			return fmt.Errorf("secret %s missing 'dsn' key", backupSecretName)
		}
		parsed, err := mysqldriver.ParseDSN(string(dsnBytes))
		if err != nil {
			return fmt.Errorf("parse dsn: %w", err)
		}
		user = parsed.User
		password = parsed.Passwd
	}

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
		derived.Labels[labelFailoverGroup] = fg.Name
		derived.Labels[labelMysqlBackupVerification] = v.Name
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
		Type:               ConditionVerified,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: v.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
	if err := r.Status().Patch(ctx, v, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch failed status: %w", err)
	}
	r.Recorder.Eventf(v, corev1.EventTypeWarning, reason, "%s", message)
	r.emitTerminalMetrics(v)
	requeue, cerr := r.maybeCleanupAfterTerminal(ctx, v)
	if cerr != nil {
		log.FromContext(ctx).Error(cerr, "post-terminal cleanup")
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// maybeCleanupAfterTerminal implements the success/failure cleanup
// policy. Success always deletes ephemeral resources after the TTL
// elapses; failure retains them iff spec.keepOnFailure is true (the
// default).
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
		ttl := time.Duration(v.Spec.TTLSecondsAfterFinished) * time.Second
		if ttl > 0 && v.Status.CompletionTime != nil {
			elapsed := time.Since(v.Status.CompletionTime.Time)
			if elapsed < ttl {
				return ttl - elapsed, nil
			}
		}
	}

	jobName := verificationJobName(v.Name)
	pvcName := verificationPVCName(v.Name)
	secretName := verificationCredsSecretName(v.Name)

	// Job (propagation=Foreground so pods go with it).
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: jobName}, &job); err == nil {
		fg := metav1.DeletePropagationForeground
		if err := r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &fg}); err != nil && !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("delete verification job: %w", err)
		}
	} else if !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("get verification job for cleanup: %w", err)
	}

	// PVC.
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: pvcName}, &pvc); err == nil {
		if err := r.Delete(ctx, &pvc); err != nil && !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("delete verification pvc: %w", err)
		}
	} else if !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("get verification pvc for cleanup: %w", err)
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
	if !hasCondition(v.Status.Conditions, ConditionResourcesCleanedUp) {
		now := metav1.Now()
		patch := client.MergeFrom(v.DeepCopy())
		setCondition(&v.Status.Conditions, metav1.Condition{
			Type:               ConditionResourcesCleanedUp,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: v.Generation,
			LastTransitionTime: now,
			Reason:             "CleanedUp",
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
func (r *MysqlBackupVerificationReconciler) emitTerminalMetrics(v *v1alpha1.MysqlBackupVerification) {
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
	// Replay lag is only meaningful on success with PITR coordinates.
	if v.Status.Phase == v1alpha1.VerificationPhaseSucceeded &&
		v.Status.ReplayedThroughBinlog != nil &&
		v.Status.ReplayedThroughBinlog.Timestamp != nil &&
		!end.IsZero() {
		lag := end.Sub(v.Status.ReplayedThroughBinlog.Timestamp.Time).Seconds()
		if lag < 0 {
			lag = 0
		}
		bmetrics.BackupVerificationReplayLagSeconds.WithLabelValues(group, profile).Set(lag)
	}
}

// verifySentinels bundles the parsed output of the verify.sh post-load
// sentinels. Either field may be nil when the corresponding phase did
// not run (no pointInTime, no sanityCheck) or when log tailing is
// unavailable (no Clientset).
type verifySentinels struct {
	Replay *v1alpha1.BinlogReplayMark
	Sanity *v1alpha1.SanityCheckResult
}

// tailVerificationSentinels reads the verification Job pod's logs and
// parses the trailing BLOODRAVEN_VERIFY_REPLAY_COMPLETE and
// BLOODRAVEN_VERIFY_SANITY_COMPLETE sentinels into structured status
// fragments. Returns zero-value sentinels on any failure path so the
// caller cleanly leaves status at its existing values.
func (r *MysqlBackupVerificationReconciler) tailVerificationSentinels(ctx context.Context, v *v1alpha1.MysqlBackupVerification, job *batchv1.Job) verifySentinels {
	out := verifySentinels{}
	if r.Clientset == nil {
		return out
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{labelMysqlBackupVerification: v.Name},
	); err != nil {
		return out
	}
	// Prefer the Succeeded pod; on Failed runs the pod phase is Failed
	// but we still want its tail. Take the most recent terminal pod.
	var chosen *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
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

	sc := bufio.NewScanner(io.LimitReader(stream, 64*1024))
	for sc.Scan() {
		line := sc.Text()
		if mark, ok := parseReplaySentinel(line); ok {
			out.Replay = mark
			continue
		}
		if sanity, ok := parseSanitySentinel(line); ok {
			out.Sanity = sanity
		}
	}
	return out
}

// parseReplaySentinel parses a single BLOODRAVEN_VERIFY_REPLAY_COMPLETE
// line into a BinlogReplayMark. Returns (nil, false) on a prefix
// mismatch. Missing fields are tolerated (forward-compatible with
// future verify.sh extensions).
func parseReplaySentinel(line string) (*v1alpha1.BinlogReplayMark, bool) {
	const prefix = "BLOODRAVEN_VERIFY_REPLAY_COMPLETE"
	if !strings.HasPrefix(line, prefix) {
		return nil, false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	mark := &v1alpha1.BinlogReplayMark{}
	for _, f := range strings.Fields(rest) {
		eq := strings.IndexByte(f, '=')
		if eq <= 0 {
			continue
		}
		key := f[:eq]
		val := f[eq+1:]
		switch key {
		case "file":
			mark.File = val
		case "position":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				mark.Position = n
			}
		case "timestamp":
			if val == "" {
				continue
			}
			if t, err := time.Parse(time.RFC3339, val); err == nil {
				mt := metav1.NewTime(t)
				mark.Timestamp = &mt
			}
		}
	}
	return mark, true
}

// parseSanitySentinel parses a single BLOODRAVEN_VERIFY_SANITY_COMPLETE
// line into a SanityCheckResult. verify.sh escapes whitespace in
// string-valued fields (resultRow, error) as underscores so the
// whitespace-split parser round-trips cleanly.
func parseSanitySentinel(line string) (*v1alpha1.SanityCheckResult, bool) {
	const prefix = "BLOODRAVEN_VERIFY_SANITY_COMPLETE"
	if !strings.HasPrefix(line, prefix) {
		return nil, false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	res := &v1alpha1.SanityCheckResult{}
	for _, f := range strings.Fields(rest) {
		eq := strings.IndexByte(f, '=')
		if eq <= 0 {
			continue
		}
		key := f[:eq]
		val := strings.ReplaceAll(f[eq+1:], "_", " ")
		switch key {
		case "ran":
			res.Ran = val == "1" || strings.EqualFold(val, "true")
		case "durationMs":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				res.DurationMs = n
			}
		case "resultRow":
			res.ResultRow = val
		case "error":
			res.Error = val
		}
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

	keepSuccess, keepFailure := r.resolveVerificationRetention(ctx, trigger)

	if err := r.pruneVerificationsByPhase(ctx, filtered, v1alpha1.VerificationPhaseSucceeded, int(keepSuccess)); err != nil {
		return err
	}
	return r.pruneVerificationsByPhase(ctx, filtered, v1alpha1.VerificationPhaseFailed, int(keepFailure))
}

// resolveVerificationRetention returns the effective (keepSuccessful,
// keepFailures) counts for a trigger CR. The policy comes from the
// profile's Verification.RetentionPolicy when set; otherwise the
// package-level defaults apply.
func (r *MysqlBackupVerificationReconciler) resolveVerificationRetention(ctx context.Context, trigger *v1alpha1.MysqlBackupVerification) (int32, int32) {
	keepSuccess := defaultVerificationKeepSuccessful
	keepFailure := defaultVerificationKeepFailures
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: trigger.Namespace, Name: trigger.Spec.FailoverGroupRef.Name}, &fg); err == nil {
		if profile := findProfile(&fg, trigger.Spec.ProfileName); profile != nil && profile.Verification != nil && profile.Verification.RetentionPolicy != nil {
			if profile.Verification.RetentionPolicy.KeepSuccessful > 0 {
				keepSuccess = profile.Verification.RetentionPolicy.KeepSuccessful
			}
			if profile.Verification.RetentionPolicy.KeepFailures > 0 {
				keepFailure = profile.Verification.RetentionPolicy.KeepFailures
			}
		}
	}
	return keepSuccess, keepFailure
}

// pruneVerificationsByPhase keeps the newest `keep` entries in the
// given phase and deletes the rest. Entries without CompletionTime
// sort last so an in-flight CR never gets swept.
func (r *MysqlBackupVerificationReconciler) pruneVerificationsByPhase(ctx context.Context, all []v1alpha1.MysqlBackupVerification, phase v1alpha1.VerificationPhase, keep int) error {
	if keep <= 0 {
		return nil
	}
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
