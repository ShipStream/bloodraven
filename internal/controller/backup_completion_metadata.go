package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ConditionBackupMetadataComplete = "MetadataComplete"
	maxCompletionMetadataWait       = 10 * time.Minute
)

var errBackupDestinationUnavailable = errors.New("backup completed without a recoverable destination")

// backupJobMainContainer excludes the encrypted Job's staging init container.
func backupJobMainContainer(job *batchv1.Job) *corev1.Container {
	for i := range job.Spec.Template.Spec.Containers {
		c := &job.Spec.Template.Spec.Containers[i]
		if c.Name == backupEncryptUploadContainerName {
			return c
		}
	}
	for i := range job.Spec.Template.Spec.Containers {
		c := &job.Spec.Template.Spec.Containers[i]
		if c.Name == backupJobContainerName {
			return c
		}
	}
	return nil
}

func completionMetadataRetryable(status *v1alpha1.MysqlBackupStatus) bool {
	c := apiMeta.FindStatusCondition(status.Conditions, ConditionBackupMetadataComplete)
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == "SentinelUnavailable" &&
		status.CompletionTime != nil && time.Now().Before(status.CompletionTime.Add(maxCompletionMetadataWait))
}

// Legacy records without this condition obtained their location from the
// producer. Only these fallback outcomes identify a Job-derived template.
func backupLocationFromJobFallback(status *v1alpha1.MysqlBackupStatus) bool {
	c := apiMeta.FindStatusCondition(status.Conditions, ConditionBackupMetadataComplete)
	if c == nil || c.Status != metav1.ConditionFalse {
		return false
	}
	return c.Reason == "SentinelUnavailable" || c.Reason == "SentinelMissing" || c.Reason == "LocationMismatch"
}

// collectBackupCompletion always obtains the required destination before
// success is persisted. Optional observations can be repaired independently.
func (r *MysqlBackupReconciler) collectBackupCompletion(ctx context.Context, backup *v1alpha1.MysqlBackup, job *batchv1.Job, next *v1alpha1.MysqlBackupStatus) (ctrl.Result, error) {
	storage, location, destinationErr := backupJobDestination(job)
	meta, found, logErr := r.tailJobCompletion(ctx, backup, job)
	if destinationErr == nil {
		next.Location = location
		next.StorageType = storage
	} else {
		_, raw, _ := backupJobRawDestination(job)
		if next.Location == raw && backupLocationFromJobFallback(next) {
			next.Location = ""
		}
		if found && meta.Location != "" {
			next.Location = meta.Location
			if storage == v1alpha1.BackupStorageS3 || storage == v1alpha1.BackupStoragePVC {
				next.StorageType = storage
			}
		}
	}
	if next.Location == "" {
		return ctrl.Result{}, fmt.Errorf("%w: %v", errBackupDestinationUnavailable, destinationErr)
	}
	if found {
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
	}
	if next.Size == "" && next.SizeBytes > 0 {
		next.Size = humanBytes(next.SizeBytes)
	}
	if destinationErr != nil && (!found || meta.Location == "") {
		// A retained producer location is not a Job fallback. Preserve its
		// provenance when neither the environment nor logs establish a path.
		return ctrl.Result{}, nil
	}
	condition := metav1.Condition{
		Type: ConditionBackupMetadataComplete, Status: metav1.ConditionTrue,
		ObservedGeneration: backup.Generation, LastTransitionTime: metav1.Now(),
		Reason: "Collected", Message: "completion metadata collected from Job logs",
	}
	switch {
	case logErr != nil:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "SentinelUnavailable"
		condition.Message = "completion logs unavailable; artifact destination recovered from Job inputs"
	case !found || meta.Location == "":
		condition.Status = metav1.ConditionFalse
		condition.Reason = "SentinelMissing"
		condition.Message = "completion sentinel missing or incomplete; artifact destination recovered from Job inputs"
	case destinationErr == nil && meta.Location != location:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "LocationMismatch"
		condition.Message = "completion location differs from producing Job; using the Job destination"
	}
	setCondition(&next.Conditions, condition)
	if r.Clientset != nil && completionMetadataRetryable(next) {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func metadataLocationNeedsRepair(current, expected string) bool {
	return current == "" || current != expected && expected != "" &&
		strings.ReplaceAll(current, " ", "_") == strings.ReplaceAll(expected, " ", "_")
}

func (r *MysqlBackupReconciler) persistBackupMetadataFailure(ctx context.Context, backup *v1alpha1.MysqlBackup, next *v1alpha1.MysqlBackupStatus, cause error) error {
	if err := r.persistBackupMetadataStatus(ctx, backup, next, cause); err != nil {
		return err
	}
	return cause
}

func (r *MysqlBackupReconciler) persistBackupMetadataStatus(ctx context.Context, backup *v1alpha1.MysqlBackup, next *v1alpha1.MysqlBackupStatus, cause error) error {
	for _, kind := range []string{ConditionBackupReady, ConditionBackupMetadataComplete} {
		setCondition(&next.Conditions, metav1.Condition{
			Type: kind, Status: metav1.ConditionFalse, ObservedGeneration: backup.Generation,
			LastTransitionTime: metav1.Now(), Reason: "DestinationUnavailable", Message: cause.Error(),
		})
	}
	if equality.Semantic.DeepEqual(&backup.Status, next) {
		return nil
	}
	previous := backup.Status.DeepCopy()
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Status = *next
	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		return fmt.Errorf("persist incomplete backup metadata: %w", err)
	}
	r.recordIncompleteBackupMetadata(backup, previous)
	return nil
}

func (r *MysqlBackupReconciler) recordIncompleteBackupMetadata(backup *v1alpha1.MysqlBackup, previous *v1alpha1.MysqlBackupStatus) {
	next := apiMeta.FindStatusCondition(backup.Status.Conditions, ConditionBackupMetadataComplete)
	if next == nil || next.Status != metav1.ConditionFalse {
		return
	}
	old := apiMeta.FindStatusCondition(previous.Conditions, ConditionBackupMetadataComplete)
	if old != nil && old.Status == next.Status && old.Reason == next.Reason && old.Message == next.Message {
		return
	}
	r.Recorder.Event(backup, corev1.EventTypeWarning, "BackupMetadataIncomplete", next.Message)
}

// Deferred size observations may refresh the size gauge, but must not move
// it behind a more recent success or repeat terminal counters/timestamps.
func updateRecoveredBackupSize(backup *v1alpha1.MysqlBackup, previous *v1alpha1.MysqlBackupStatus) {
	if previous.SizeBytes != 0 || backup.Status.SizeBytes <= 0 || backup.Status.Phase != v1alpha1.BackupPhaseSucceeded || backup.Status.CompletionTime == nil {
		return
	}
	group, profile := backup.Spec.FailoverGroupRef.Name, backup.Spec.ProfileName
	var latest dto.Metric
	if err := bmetrics.BackupLastSuccessTimestamp.WithLabelValues(group, profile).Write(&latest); err != nil {
		return
	}
	if float64(backup.Status.CompletionTime.Unix()) >= latest.GetGauge().GetValue() {
		bmetrics.BackupLastSizeBytes.WithLabelValues(group, profile).Set(float64(backup.Status.SizeBytes))
	}
}
