package controller

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

	// Handle deletion with finalizer: delete the owned Job, ConfigMap, Secret.
	if !backup.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&backup, mysqlBackupFinalizer) {
			if err := r.finalize(ctx, &backup); err != nil {
				return ctrl.Result{}, fmt.Errorf("finalize mysqlbackup: %w", err)
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

	// Ensure owner reference so deleting the group cascades to in-flight
	// backups. Set once on creation; subsequent updates are no-ops.
	if len(backup.OwnerReferences) == 0 {
		if err := controllerutil.SetControllerReference(&fg, &backup, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set owner ref: %w", err)
		}
		if err := r.Update(ctx, &backup); err != nil {
			return ctrl.Result{}, fmt.Errorf("update owner ref: %w", err)
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
	sourceSite, reason, err := selectSourceSite(&fg.Status, backup.Spec.SourceSiteOverride, maxLag)
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
		// Still running.
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	patch := client.MergeFrom(backup.DeepCopy())
	now := metav1.Now()
	backup.Status.Phase = phase
	if backup.Status.StartTime == nil {
		if job.Status.StartTime != nil {
			backup.Status.StartTime = job.Status.StartTime
		} else {
			backup.Status.StartTime = &now
		}
	}
	backup.Status.CompletionTime = &now
	backup.Status.Message = message
	if backup.Status.SourceSite == "" {
		backup.Status.SourceSite = sourceSite
	}
	backup.Status.ObservedGeneration = backup.Generation

	if phase == v1alpha1.BackupPhaseSucceeded {
		if loc, size := r.tailJobCompletion(ctx, &backup, &job); loc != "" {
			backup.Status.Location = loc
			backup.Status.Size = size
		}
		setCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               ConditionBackupReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: now,
			Reason:             "Succeeded",
			Message:            message,
		})
		r.Recorder.Eventf(&backup, corev1.EventTypeNormal, "BackupSucceeded",
			"backup %s completed: %s", backup.Name, message)
	} else {
		setCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               ConditionBackupReady,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: backup.Generation,
			LastTransitionTime: now,
			Reason:             "Failed",
			Message:            message,
		})
		r.Recorder.Eventf(&backup, corev1.EventTypeWarning, "BackupFailed",
			"backup %s failed: %s", backup.Name, message)
	}

	if err := r.Status().Patch(ctx, &backup, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch final status: %w", err)
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

// finalize deletes dependent resources on MysqlBackup deletion. Job,
// creds Secret are owned by the CR so Kubernetes garbage collection will
// handle them; the finalizer exists so we can add S3 artifact cleanup
// later without a schema migration.
func (r *MysqlBackupReconciler) finalize(ctx context.Context, backup *v1alpha1.MysqlBackup) error {
	// For v1: nothing to do beyond letting owner-ref GC clean up. Keep
	// the finalizer attached so future versions that add S3 cleanup
	// can hook in here without a breaking change.
	return nil
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
// carrying MYSQL_USER / MYSQL_PASSWORD parsed from the failover group's
// DSN secret. The Secret is owned by the MysqlBackup CR so it is garbage
// collected with the backup.
func (r *MysqlBackupReconciler) ensureDerivedCredsSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, backup *v1alpha1.MysqlBackup, secretName string) error {
	var dsnSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.SecretName}, &dsnSecret); err != nil {
		return fmt.Errorf("get dsn secret %s: %w", fg.Spec.SecretName, err)
	}
	dsnBytes, ok := dsnSecret.Data["dsn"]
	if !ok {
		return fmt.Errorf("secret %s missing 'dsn' key", fg.Spec.SecretName)
	}
	parsed, err := mysqldriver.ParseDSN(string(dsnBytes))
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	if parsed.User == "" {
		return fmt.Errorf("dsn has empty user")
	}

	derived := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: backup.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, derived, func() error {
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
			"MYSQL_USER":     []byte(parsed.User),
			"MYSQL_PASSWORD": []byte(parsed.Passwd),
		}
		return nil
	})
	return err
}

// jobPhase summarizes a batch/v1 Job status into a BackupPhase, using
// `kind` ("backup" or "restore") to produce an accurate success message
// for either caller. Returns empty phase when the Job is still running.
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

// tailJobCompletion tails the Job pod log and parses the trailing
// BLOODRAVEN_DUMP_COMPLETE sentinel to populate status.location/size.
// Best-effort; returns empty strings on any failure. Requires a real
// clientset (nil in fake-client tests).
func (r *MysqlBackupReconciler) tailJobCompletion(ctx context.Context, backup *v1alpha1.MysqlBackup, job *batchv1.Job) (string, string) {
	if r.Clientset == nil {
		return "", ""
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{labelMysqlBackup: backup.Name},
	); err != nil {
		return "", ""
	}
	var podName string
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded {
			podName = p.Name
			break
		}
	}
	if podName == "" {
		return "", ""
	}

	req := r.Clientset.CoreV1().Pods(job.Namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: backupJobContainerName,
		TailLines: ptr64(50),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", ""
	}
	defer func() { _ = stream.Close() }()

	var lastLocation, lastSize string
	sc := bufio.NewScanner(io.LimitReader(stream, 64*1024))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "BLOODRAVEN_DUMP_COMPLETE") {
			// Format: BLOODRAVEN_DUMP_COMPLETE location=... size=...
			parts := strings.Fields(strings.TrimPrefix(line, "BLOODRAVEN_DUMP_COMPLETE "))
			for _, p := range parts {
				if strings.HasPrefix(p, "location=") {
					lastLocation = strings.TrimPrefix(p, "location=")
				}
				if strings.HasPrefix(p, "size=") {
					lastSize = strings.TrimPrefix(p, "size=")
				}
			}
		}
	}
	return lastLocation, lastSize
}

func ptr64(v int64) *int64 { return &v }

// pruneRetention deletes old MysqlBackup CRs for the same (failover
// group, profile). Keeps profile.retention newest successes plus a
// hard-coded cap of failed runs.
func (r *MysqlBackupReconciler) pruneRetention(ctx context.Context, trigger *v1alpha1.MysqlBackup) error {
	var list v1alpha1.MysqlBackupList
	if err := r.List(ctx, &list,
		client.InNamespace(trigger.Namespace),
		client.MatchingLabels{
			labelFailoverGroup: trigger.Spec.FailoverGroupRef.Name,
			labelBackupProfile: trigger.Spec.ProfileName,
		},
	); err != nil {
		return err
	}

	// If the trigger was not created with the standard labels, fall back
	// to filtering in-memory.
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

	// Determine keep count for successes.
	keepSuccess := int32(0)
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: trigger.Namespace, Name: trigger.Spec.FailoverGroupRef.Name}, &fg); err == nil {
		if profile := findProfile(&fg, trigger.Spec.ProfileName); profile != nil {
			keepSuccess = profile.Retention
		}
	}
	if keepSuccess == 0 {
		// Retention=0 means "keep all". Still trim failed runs.
		return r.pruneByPhase(ctx, filtered, v1alpha1.BackupPhaseFailed, maxFailedRetention)
	}

	if err := r.pruneByPhase(ctx, filtered, v1alpha1.BackupPhaseSucceeded, int(keepSuccess)); err != nil {
		return err
	}
	return r.pruneByPhase(ctx, filtered, v1alpha1.BackupPhaseFailed, maxFailedRetention)
}

// pruneByPhase keeps the newest `keep` entries in the given phase and
// deletes the rest. Entries without a CompletionTime sort last.
func (r *MysqlBackupReconciler) pruneByPhase(ctx context.Context, all []v1alpha1.MysqlBackup, phase v1alpha1.BackupPhase, keep int) error {
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
func selectSourceSite(status *v1alpha1.MysqlFailoverGroupStatus, override string, maxLagSeconds int64) (string, string, error) {
	if status == nil || len(status.Sites) == 0 {
		return "", "", fmt.Errorf("failover group has no observed site status yet")
	}

	if override != "" {
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

	var primary, replica *v1alpha1.SiteStatus
	for i := range status.Sites {
		s := &status.Sites[i]
		if s.Name == status.ActiveSite {
			primary = s
			continue
		}
		if replica == nil {
			replica = s
		}
	}

	if replica != nil && replica.State == "read-only" && replica.Replicating {
		if replica.SecondsBehindSource == nil {
			return replica.Name, "replica-preferred", nil
		}
		if *replica.SecondsBehindSource <= maxLagSeconds {
			return replica.Name, "replica-preferred", nil
		}
	}

	if primary != nil && primary.State == "writable" {
		return primary.Name, "primary-fallback", nil
	}

	return "", "", fmt.Errorf("no healthy source (primary=%s, replica=%s)",
		siteStateString(primary), siteStateString(replica))
}

func siteStateString(s *v1alpha1.SiteStatus) string {
	if s == nil {
		return "nil"
	}
	return fmt.Sprintf("%s/%s", s.Name, s.State)
}
