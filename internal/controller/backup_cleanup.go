package controller

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// cleanupJobInputs bundles the resolved inputs needed to render the
// artifact-cleanup Job attached to a MysqlBackup's finalizer.
type cleanupJobInputs struct {
	FailoverGroup        *v1alpha1.MysqlFailoverGroup
	Profile              *v1alpha1.BackupProfile
	Backup               *v1alpha1.MysqlBackup
	CredsSecretName      string
	ScriptsConfigMapName string
}

// buildCleanupJob renders the one-shot Job that runs cleanup.py against
// the backup's underlying storage, driven by BLOODRAVEN_STORAGE_TYPE.
// The shape mirrors BuildBackupJob (same creds-as-files mounts, same
// security-context defaults) because both Jobs run the same mysqlsh
// image and need the same hardening.
func buildCleanupJob(in cleanupJobInputs) (*batchv1.Job, error) {
	fg := in.FailoverGroup
	backup := in.Backup
	profile := in.Profile

	if backup == nil || backup.Status.Location == "" {
		return nil, fmt.Errorf("cleanup job: backup has no status.location")
	}
	if in.CredsSecretName == "" {
		return nil, fmt.Errorf("cleanup job: creds secret name is empty")
	}
	if in.ScriptsConfigMapName == "" {
		return nil, fmt.Errorf("cleanup job: scripts configmap name is empty")
	}

	// Resolve the effective storage type. Prefer the value recorded on
	// status (set at Job creation time) so the cleanup path works even
	// if the profile has been removed from spec.backup in the meantime.
	storageType := backup.Status.StorageType
	if storageType == "" && profile != nil {
		storageType = profile.Storage.Type
	}
	if storageType == "" {
		return nil, fmt.Errorf("cleanup job: cannot determine storage type for backup %s", backup.Name)
	}

	image := backupImage(fg)

	labels := map[string]string{
		labelAppName:       "mysql-backup",
		labelInstance:      fg.Name,
		labelFailoverGroup: fg.Name,
		labelMysqlBackup:   backup.Name,
		labelManagedBy:     managerName,
		labelResourceKind:  "backup-cleanup",
	}
	if profile != nil {
		labels[labelBackupProfile] = profile.Name
	}

	envVars := []corev1.EnvVar{
		{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(storageType)},
		{Name: "BLOODRAVEN_OUTPUT_URL", Value: backup.Status.Location},
		{Name: "BLOODRAVEN_BACKUP_NAME", Value: backup.Name},
		{Name: "BLOODRAVEN_MYSQL_CREDS_DIR", Value: backupCredsMountPath},
		{Name: "HOME", Value: mysqlshHomeMountPath},
	}

	var (
		volumes      []corev1.Volume
		volumeMounts []corev1.VolumeMount
	)

	// mysql-creds mount (unused by cleanup.py today, but we keep the
	// same layout as the backup Job so the security-context tests and
	// pod-spec auditing treat these two Jobs uniformly).
	volumes = append(volumes, corev1.Volume{
		Name: "mysql-creds",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  in.CredsSecretName,
				DefaultMode: ptr32(0o400),
			},
		},
	})
	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      "mysql-creds",
		MountPath: backupCredsMountPath,
		ReadOnly:  true,
	})

	switch storageType {
	case v1alpha1.BackupStorageS3:
		// For the S3 branch we need the bucket + endpoint + AWS creds.
		// Resolve from the live profile when it's still there, or fall
		// back to best-effort behavior on a missing profile (finalizer
		// caller already emitted a warning in that case).
		if profile != nil && profile.Storage.S3 != nil {
			s3 := profile.Storage.S3
			envVars = append(envVars,
				corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: s3.Bucket},
			)
			if s3.EndpointURL != "" {
				envVars = append(envVars, corev1.EnvVar{
					Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: s3.EndpointURL,
				})
			}
			if s3.Region != "" {
				envVars = append(envVars, corev1.EnvVar{Name: "AWS_REGION", Value: s3.Region})
			}
			volumes = append(volumes, corev1.Volume{
				Name: "aws-creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  s3.CredentialsSecret,
						DefaultMode: ptr32(0o400),
					},
				},
			})
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      "aws-creds",
				MountPath: backupAWSCredsMountPath,
				ReadOnly:  true,
			})
			envVars = append(envVars, corev1.EnvVar{
				Name:  "BLOODRAVEN_AWS_CREDS_DIR",
				Value: backupAWSCredsMountPath,
			})
		}

	case v1alpha1.BackupStoragePVC:
		// Mount the same PVC the backup used.
		var claimName string
		if profile != nil && profile.Storage.PVC != nil {
			claimName = profile.Storage.PVC.ClaimName
			if claimName == "" {
				claimName = ownedBackupPVCName(fg.Name, profile.Name)
			}
		}
		if claimName == "" {
			return nil, fmt.Errorf("cleanup job: PVC backup %s has no resolvable claim name", backup.Name)
		}
		vol := corev1.Volume{
			Name: "backups",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claimName,
				},
			},
		}
		// Mount the PVC without SubPath so the full path expressed in
		// backup.status.location (e.g. /backups/<subPath>/<name>) is
		// accessible inside the cleanup container.
		mount := corev1.VolumeMount{
			Name:      "backups",
			MountPath: backupPVCMountPath,
		}
		volumes = append(volumes, vol)
		volumeMounts = append(volumeMounts, mount)
		envVars = append(envVars,
			corev1.EnvVar{Name: "BLOODRAVEN_PVC_MOUNT_PATH", Value: backupPVCMountPath},
		)

	default:
		return nil, fmt.Errorf("cleanup job: unknown storage type %q", storageType)
	}

	// Scripts ConfigMap + writable home / tmp.
	volumes = append(volumes,
		corev1.Volume{
			Name: "scripts",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: in.ScriptsConfigMapName},
				},
			},
		},
		corev1.Volume{
			Name:         "mysqlsh-home",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		corev1.Volume{
			Name:         "tmp",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	)
	volumeMounts = append(volumeMounts,
		corev1.VolumeMount{Name: "scripts", MountPath: backupScriptsMountPath, ReadOnly: true},
		corev1.VolumeMount{Name: "mysqlsh-home", MountPath: mysqlshHomeMountPath},
		corev1.VolumeMount{Name: "tmp", MountPath: tmpMountPath},
	)

	var bspec *v1alpha1.BackupSpec
	if fg.Spec.Backup != nil {
		bspec = fg.Spec.Backup
	}
	var podSCSrc *corev1.PodSecurityContext
	var containerSCSrc *corev1.SecurityContext
	if bspec != nil {
		podSCSrc = bspec.PodSecurityContext
		containerSCSrc = bspec.ContainerSecurityContext
	}
	podSC, containerSC := mergeSecurityContexts(podSCSrc, containerSCSrc)

	activeDeadline := int64(600)
	backoff := int32(2)

	var pullSecrets []corev1.LocalObjectReference
	if bspec != nil {
		pullSecrets = bspec.ImagePullSecrets
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cleanupJobName(backup.Name),
			Namespace: fg.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &activeDeadline,
			BackoffLimit:          &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: pullSecrets,
					SecurityContext:  podSC,
					Containers: []corev1.Container{
						{
							Name:            backupJobContainerName,
							Image:           image,
							Command:         []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/cleanup.py"},
							Env:             envVars,
							VolumeMounts:    volumeMounts,
							SecurityContext: containerSC,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}
	return job, nil
}
