package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const annotationVerificationBackup = "shipstream.io/verification-backup"

func verificationBackupSnapshot(backup *v1alpha1.MysqlBackup) *v1alpha1.VerificationBackupRef {
	return &v1alpha1.VerificationBackupRef{
		Name: backup.Name, UID: string(backup.UID),
		Location: backup.Status.Location, StorageType: backup.Status.StorageType,
	}
}

func validateVerificationBackupRef(ref *v1alpha1.VerificationBackupRef) error {
	if ref == nil || ref.Name == "" || ref.UID == "" || ref.Location == "" {
		return fmt.Errorf("verification source requires a pinned name, UID and location")
	}
	if ref.StorageType != v1alpha1.BackupStorageS3 && ref.StorageType != v1alpha1.BackupStoragePVC {
		return fmt.Errorf("verification source has unknown storage type %q", ref.StorageType)
	}
	return nil
}

// resolvePinnedVerificationBackup selects once, then checks the original UID
// and uses its recorded destination when building a not-yet-created Job.
func (r *MysqlBackupVerificationReconciler) resolvePinnedVerificationBackup(ctx context.Context, v *v1alpha1.MysqlBackupVerification) (*v1alpha1.MysqlBackup, error) {
	if v.Status.BackupRef == nil {
		backup, err := r.resolveBackup(ctx, v)
		if err != nil {
			return nil, err
		}
		ref := verificationBackupSnapshot(backup)
		if err := validateVerificationBackupRef(ref); err != nil {
			return nil, err
		}
		return backup, nil
	}
	ref := v.Status.BackupRef
	if err := validateVerificationBackupRef(ref); err != nil {
		return nil, err
	}
	var backup v1alpha1.MysqlBackup
	if err := r.Get(ctx, client.ObjectKey{Namespace: v.Namespace, Name: ref.Name}, &backup); err != nil {
		return nil, fmt.Errorf("get pinned backup %q: %w", ref.Name, err)
	}
	if string(backup.UID) != ref.UID {
		return nil, fmt.Errorf("pinned backup %q UID changed: expected %q, found %q", ref.Name, ref.UID, backup.UID)
	}
	if backup.Status.Phase != v1alpha1.BackupPhaseSucceeded || backup.Spec.FailoverGroupRef.Name != v.Spec.FailoverGroupRef.Name || backup.Spec.ProfileName != v.Spec.ProfileName {
		return nil, fmt.Errorf("pinned backup %q is no longer a successful backup for this group/profile", ref.Name)
	}
	backup.Status.Location = ref.Location
	backup.Status.StorageType = ref.StorageType
	return &backup, nil
}

// Compare effective kubelet values with the literal source snapshot.
func verificationContainerEnv(container corev1.Container) map[string]string {
	env := make(map[string]string)
	previous := make(map[string]backupEnvironmentValue)
	for _, value := range container.Env {
		entry := backupEnvironmentValue{}
		if value.ValueFrom == nil {
			entry = expandBackupEnvironment(value.Value, previous)
		}
		previous[value.Name] = entry
		delete(env, value.Name)
		if entry.known {
			env[value.Name] = entry.value
		}
	}
	return env
}

// verificationJobReadsBackup checks the original source, not a decrypt staging
// path. This also permits safe observation of older Jobs without a snapshot.
func verificationJobReadsBackup(job *batchv1.Job, ref *v1alpha1.VerificationBackupRef) bool {
	for _, container := range job.Spec.Template.Spec.InitContainers {
		if container.Name != "decrypt-download" {
			continue
		}
		env := verificationContainerEnv(container)
		if env["BLOODRAVEN_STORAGE_TYPE"] != string(ref.StorageType) {
			return false
		}
		switch ref.StorageType {
		case v1alpha1.BackupStorageS3:
			bucket, prefix := splitS3Location(ref.Location)
			return env["BLOODRAVEN_SOURCE_PREFIX"] == prefix && (bucket == "" || env["BLOODRAVEN_S3_BUCKET"] == bucket)
		case v1alpha1.BackupStoragePVC:
			prefix := strings.TrimSuffix(strings.TrimPrefix(ref.Location, backupPVCMountPath+"/"), "/")
			return env["BLOODRAVEN_SOURCE_PREFIX"] == prefix
		}
	}
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Name != backupJobContainerName {
			continue
		}
		env := verificationContainerEnv(container)
		return env["BLOODRAVEN_INPUT_URL"] == mysqlShellDumpInput(ref.Location, ref.StorageType) &&
			(ref.StorageType != v1alpha1.BackupStorageS3 || env["BLOODRAVEN_S3_BUCKET"] != "")
	}
	return false
}

func verificationJobSource(v *v1alpha1.MysqlBackupVerification, job *batchv1.Job) (*v1alpha1.VerificationBackupRef, error) {
	if !metav1.IsControlledBy(job, v) {
		return nil, fmt.Errorf("verification Job is not owned by this verification")
	}
	ref := v.Status.BackupRef
	if encoded, exists := job.Spec.Template.Annotations[annotationVerificationBackup]; exists {
		var recorded v1alpha1.VerificationBackupRef
		if err := json.Unmarshal([]byte(encoded), &recorded); err != nil {
			return nil, fmt.Errorf("decode verification Job source snapshot: %w", err)
		}
		ref = &recorded
	}
	if err := validateVerificationBackupRef(ref); err != nil {
		return nil, err
	}
	if !verificationJobReadsBackup(job, ref) {
		return nil, fmt.Errorf("recorded backup %q does not match the verification Job's source inputs", ref.Name)
	}
	return ref, nil
}

func (r *MysqlBackupVerificationReconciler) pinVerificationJobSource(ctx context.Context, v *v1alpha1.MysqlBackupVerification, job *batchv1.Job, ref *v1alpha1.VerificationBackupRef) error {
	if equality.Semantic.DeepEqual(v.Status.BackupRef, ref) && v.Status.JobName == job.Name && v.Status.Phase != v1alpha1.VerificationPhaseProvisioning && v.Status.Phase != v1alpha1.VerificationPhasePending && v.Status.Phase != "" {
		return nil
	}
	patch := client.MergeFrom(v.DeepCopy())
	copy := *ref
	v.Status.BackupRef = &copy
	v.Status.JobName = job.Name
	if v.Status.Phase == "" || v.Status.Phase == v1alpha1.VerificationPhasePending || v.Status.Phase == v1alpha1.VerificationPhaseProvisioning {
		v.Status.Phase = v1alpha1.VerificationPhaseRestoring
	}
	if err := r.Status().Patch(ctx, v, patch); err != nil {
		return fmt.Errorf("persist verification Job source: %w", err)
	}
	return nil
}
