package controller

import (
	"fmt"
	"math"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// verificationDataMountPath is where the verification Job mounts
	// its ephemeral datadir PVC. The verify.sh script reads this path
	// from BLOODRAVEN_DATA_DIR.
	verificationDataMountPath = "/var/lib/mysql-verify"

	// verificationDefaultActiveDeadline is the fallback Job
	// activeDeadlineSeconds used when spec.backup.activeDeadlineSeconds
	// is not set. Verification loads a dump into a cold instance so the
	// ceiling matches the backup default.
	verificationDefaultActiveDeadline = int64(7200)

	// verificationMinPVCSize is the floor for the auto-sized ephemeral
	// datadir PVC. MySQL's initial datadir plus a small dump is already
	// a few hundred MB; 10Gi keeps us comfortable for tiny backups and
	// avoids churning StorageClass minimums.
	verificationMinPVCSize = int64(10 * 1024 * 1024 * 1024) // 10 GiB
)

// verificationJobInputs bundles the resolved inputs for building the
// verification Job. Mirrors BackupJobInputs in spirit.
type verificationJobInputs struct {
	FailoverGroup         *v1alpha1.MysqlFailoverGroup
	Profile               v1alpha1.BackupProfile
	Verification          *v1alpha1.MysqlBackupVerification
	Backup                *v1alpha1.MysqlBackup
	CredsSecretName       string
	ScriptsConfigMapName  string
	PVCName               string
}

// buildVerificationPVC constructs the ephemeral datadir PVC for a
// verification run. Size is taken from spec.storage.size when set,
// otherwise auto-sized from the referenced backup's sizeBytes.
func buildVerificationPVC(in verificationJobInputs) *corev1.PersistentVolumeClaim {
	v := in.Verification

	size := autoSizeVerificationPVC(v, in.Backup)

	labels := map[string]string{
		labelAppName:                 "mysql-verify",
		labelInstance:                in.FailoverGroup.Name,
		labelFailoverGroup:           in.FailoverGroup.Name,
		labelBackupProfile:           in.Profile.Name,
		labelMysqlBackupVerification: v.Name,
		labelManagedBy:               managerName,
		labelResourceKind:            verificationResourceKindCR,
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      verificationPVCName(v.Name),
			Namespace: v.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: size,
				},
			},
		},
	}
	if v.Spec.Storage != nil && v.Spec.Storage.StorageClassName != "" {
		sc := v.Spec.Storage.StorageClassName
		pvc.Spec.StorageClassName = &sc
	}
	return pvc
}

// autoSizeVerificationPVC returns the requested PVC capacity for a
// verification run. Explicit spec.storage.size wins; otherwise we use
// max(verificationMinPVCSize, ceil(1.5 * backup.status.sizeBytes / 10 GiB) * 10 GiB).
func autoSizeVerificationPVC(v *v1alpha1.MysqlBackupVerification, backup *v1alpha1.MysqlBackup) resource.Quantity {
	if v.Spec.Storage != nil && !v.Spec.Storage.Size.IsZero() {
		return v.Spec.Storage.Size
	}

	var src int64
	if backup != nil {
		src = backup.Status.SizeBytes
	}
	if src <= 0 {
		return *resource.NewQuantity(verificationMinPVCSize, resource.BinarySI)
	}
	need := int64(math.Ceil(float64(src) * 1.5))
	// Round up to the nearest 10 GiB.
	const step = int64(10 * 1024 * 1024 * 1024)
	blocks := (need + step - 1) / step
	sized := blocks * step
	if sized < verificationMinPVCSize {
		sized = verificationMinPVCSize
	}
	return *resource.NewQuantity(sized, resource.BinarySI)
}

// buildVerificationJob constructs the batchv1.Job that runs verify.sh
// inside the container image pinned on spec.backup.image. verify.sh
// boots an ephemeral mysqld on the mounted PVC, then delegates the
// actual load to the shared restore.py script.
func buildVerificationJob(in verificationJobInputs) (*batchv1.Job, error) {
	fg := in.FailoverGroup
	v := in.Verification
	backup := in.Backup

	if in.CredsSecretName == "" {
		return nil, fmt.Errorf("verification job: creds secret name is empty")
	}
	if in.ScriptsConfigMapName == "" {
		return nil, fmt.Errorf("verification job: scripts configmap name is empty")
	}
	if in.PVCName == "" {
		return nil, fmt.Errorf("verification job: pvc name is empty")
	}
	if backup == nil {
		return nil, fmt.Errorf("verification job: backup reference not resolved")
	}
	if backup.Status.Location == "" {
		return nil, fmt.Errorf("verification job: referenced backup %q has no status.location", backup.Name)
	}

	image := backupImage(fg)

	labels := map[string]string{
		labelAppName:                 "mysql-verify",
		labelInstance:                fg.Name,
		labelFailoverGroup:           fg.Name,
		labelBackupProfile:           in.Profile.Name,
		labelMysqlBackupVerification: v.Name,
		labelManagedBy:               managerName,
		labelResourceKind:            verificationResourceKindCR,
	}

	loadOptsJSON, err := marshalLoadOptions(verificationLoadOptions())
	if err != nil {
		return nil, err
	}

	inputURL := ensureTrailingSlash(backup.Status.Location)

	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_DATA_DIR", Value: verificationDataMountPath},
		{Name: "BLOODRAVEN_SCRIPTS_DIR", Value: backupScriptsMountPath},
		{Name: "BLOODRAVEN_INPUT_URL", Value: inputURL},
		{Name: "BLOODRAVEN_LOAD_OPTIONS", Value: loadOptsJSON},
		{Name: "HOME", Value: mysqlshHomeMountPath},
	}

	volumes := []corev1.Volume{
		{
			Name: "datadir",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: in.PVCName,
				},
			},
		},
		{
			Name: "scripts",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: in.ScriptsConfigMapName},
					DefaultMode:          ptr32(0o555),
				},
			},
		},
		{
			Name:         "mysqlsh-home",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name:         "tmp",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}
	mounts := []corev1.VolumeMount{
		{Name: "datadir", MountPath: verificationDataMountPath},
		{Name: "scripts", MountPath: backupScriptsMountPath, ReadOnly: true},
		{Name: "mysqlsh-home", MountPath: mysqlshHomeMountPath},
		{Name: "tmp", MountPath: tmpMountPath},
	}

	// S3-hosted backups need AWS creds mounted the same way the backup
	// Job mounts them. PVC-hosted backups need the backup PVC mounted
	// read-only so mysqlsh util.loadDump() can read the on-disk dump.
	awsCredsSecret := ""
	switch backup.Status.StorageType {
	case v1alpha1.BackupStorageS3:
		if in.Profile.Storage.S3 == nil {
			return nil, fmt.Errorf("verification job: backup %q storageType=S3 but profile %q has no s3 config",
				backup.Name, in.Profile.Name)
		}
		awsCredsSecret = in.Profile.Storage.S3.CredentialsSecret
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: in.Profile.Storage.S3.Bucket},
		)
		if in.Profile.Storage.S3.EndpointURL != "" {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: in.Profile.Storage.S3.EndpointURL})
		}
		if in.Profile.Storage.S3.Region != "" {
			env = append(env, corev1.EnvVar{Name: "AWS_REGION", Value: in.Profile.Storage.S3.Region})
		}
	case v1alpha1.BackupStoragePVC:
		if in.Profile.Storage.PVC == nil {
			return nil, fmt.Errorf("verification job: backup %q storageType=PVC but profile %q has no pvc config",
				backup.Name, in.Profile.Name)
		}
		claim := in.Profile.Storage.PVC.ClaimName
		if claim == "" {
			claim = ownedBackupPVCName(fg.Name, in.Profile.Name)
		}
		volumes = append(volumes, corev1.Volume{
			Name: "backup-src",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim,
					ReadOnly:  true,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "backup-src", MountPath: backupPVCMountPath, ReadOnly: true,
		})
	default:
		return nil, fmt.Errorf("verification job: backup %q has unknown or empty storageType %q",
			backup.Name, backup.Status.StorageType)
	}

	if awsCredsSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "aws-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  awsCredsSecret,
					DefaultMode: ptr32(0o400),
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "aws-creds", MountPath: backupAWSCredsMountPath, ReadOnly: true,
		})
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath})
	}

	activeDeadline := verificationDefaultActiveDeadline
	backoff := int32(0)
	var (
		pullSecrets []corev1.LocalObjectReference
		userPod     *corev1.PodSecurityContext
		userCont    *corev1.SecurityContext
	)
	if fg.Spec.Backup != nil {
		if fg.Spec.Backup.ActiveDeadlineSeconds > 0 {
			activeDeadline = fg.Spec.Backup.ActiveDeadlineSeconds
		}
		pullSecrets = fg.Spec.Backup.ImagePullSecrets
		userPod = fg.Spec.Backup.PodSecurityContext
		userCont = fg.Spec.Backup.ContainerSecurityContext
	}
	podSC, containerSC := mergeSecurityContexts(userPod, userCont)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      verificationJobName(v.Name),
			Namespace: v.Namespace,
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
							Name:  backupJobContainerName,
							Image: image,
							// verify.sh is mounted as executable via the
							// scripts ConfigMap default mode 0555.
							Command:         []string{"/bin/bash", backupScriptsMountPath + "/verify.sh"},
							Env:             env,
							Resources:       v.Spec.Resources,
							VolumeMounts:    mounts,
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

// verificationLoadOptions returns the util.loadDump() options used for
// a verification run. ResetProgress=true so retries of a failed
// verification start clean; SkipBinlog=true because the ephemeral
// mysqld runs with --skip-log-bin anyway; LoadIndexes=true so a
// "restore loads successfully" result is not weakened by deferred
// index builds that would only fail later.
func verificationLoadOptions() *v1alpha1.LoadOptions {
	t := true
	return &v1alpha1.LoadOptions{
		Threads:       4,
		ResetProgress: &t,
		SkipBinlog:    &t,
		LoadIndexes:   &t,
	}
}
