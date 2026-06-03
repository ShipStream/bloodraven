package controller

import (
	"fmt"
	"math"
	"strconv"
	"strings"

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
	FailoverGroup        *v1alpha1.MysqlFailoverGroup
	Profile              v1alpha1.BackupProfile
	Verification         *v1alpha1.MysqlBackupVerification
	Backup               *v1alpha1.MysqlBackup
	CredsSecretName      string
	ScriptsConfigMapName string
	PVCName              string
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

	inputURL := mysqlShellDumpInput(backup.Status.Location, backup.Status.StorageType)

	// PITR replay fragments (download init container + shared emptyDir
	// + main-container env/mounts). buildRestorePITRFragmentsFor returns
	// empty fragments when the translated spec is nil, so the append
	// paths are unconditional below.
	pitFromVerification, err := verificationPITRSpec(v.Spec.PointInTime)
	if err != nil {
		return nil, err
	}
	pitrFrags, err := buildRestorePITRFragmentsFor(fg, pitFromVerification)
	if err != nil {
		return nil, err
	}

	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_DATA_DIR", Value: verificationDataMountPath},
		{Name: "BLOODRAVEN_SCRIPTS_DIR", Value: backupScriptsMountPath},
		{Name: "BLOODRAVEN_INPUT_URL", Value: inputURL},
		{Name: "BLOODRAVEN_LOAD_OPTIONS", Value: loadOptsJSON},
		// mysqlsh's default progressFile lives next to the dump, but the
		// backup source is mounted read-only on verification Jobs, so
		// disable progress tracking to avoid a writable-backup
		// requirement.
		{Name: "BLOODRAVEN_LOAD_PROGRESS_FILE", Value: ""},
		{Name: "HOME", Value: mysqlshHomeMountPath},
	}

	// PITR replay env: tell verify.sh whether to run replay at all, and
	// forward the stop datetime + local dir picked up by mysqlbinlog.
	// pitrFrags.MainEnv already carries BLOODRAVEN_PITR_LOCAL_DIR and
	// BLOODRAVEN_PITR_STOP_DATETIME so we only need the mode toggle.
	if v.Spec.PointInTime != nil && v.Spec.PointInTime.Mode != "" && v.Spec.PointInTime.Mode != "none" {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_PITR_MODE", Value: v.Spec.PointInTime.Mode})
	}
	env = append(env, pitrFrags.MainEnv...)

	// Sanity-check env: verify.sh branches on BLOODRAVEN_VERIFY_SANITY_QUERY
	// being non-empty; min-rows floor and timeout are numeric scalars.
	if v.Spec.SanityCheck != nil && v.Spec.SanityCheck.Query != "" {
		query, err := validateSanityQuery(v.Spec.SanityCheck.Query)
		if err != nil {
			return nil, fmt.Errorf("verification job: %w", err)
		}
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_SANITY_QUERY", Value: query})
		maxSec := int32(60)
		var minRows int64
		if v.Spec.SanityCheck.Expect != nil {
			if v.Spec.SanityCheck.Expect.MaxDurationSeconds > 0 {
				maxSec = v.Spec.SanityCheck.Expect.MaxDurationSeconds
			}
			if v.Spec.SanityCheck.Expect.MinRows > 0 {
				minRows = v.Spec.SanityCheck.Expect.MinRows
			}
		}
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS", Value: strconv.FormatInt(int64(maxSec), 10)},
			corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_SANITY_MIN_ROWS", Value: strconv.FormatInt(minRows, 10)},
		)
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

	// Splice PITR fragments: shared emptyDir volume, pitr-aws-creds /
	// pitr-archive volume, and the read-only main-container mount.
	volumes = append(volumes, pitrFrags.PodVolumes...)
	mounts = append(mounts, pitrFrags.MainMounts...)

	var initContainers []corev1.Container
	if pitrFrags.InitContainer != nil {
		initContainers = append(initContainers, *pitrFrags.InitContainer)
	}

	// -------- Decryption wiring -------------------------------------
	//
	// When the source backup is encrypted we run the same
	// `bloodraven decrypt-download` init container the restore path
	// uses. The profile's encryption.passphraseSecret is the source
	// of truth — verification doesn't have its own decryption field
	// by design, because "verify what your profile produced" is
	// exactly the scope where the profile's passphrase is authoritative.
	if backup.Status.Encrypted {
		if !in.Profile.EncryptionEnabled() {
			return nil, fmt.Errorf(
				"verification job: backup %q is encrypted but profile %q has no encryption.passphraseSecret; "+
					"restore the encryption field to verify this backup",
				backup.Name, in.Profile.Name)
		}
		if operatorImageFromEnv == "" {
			return nil, fmt.Errorf("verification job: operator image is not configured (SetOperatorImageDefaults); required for encrypted verification")
		}

		const decryptDir = "/restore-decrypted"
		// Shared emptyDir: init container writes plaintext, main
		// container reads it through BLOODRAVEN_INPUT_URL.
		volumes = append(volumes, corev1.Volume{
			Name:         "restore-decrypted",
			VolumeSource: corev1.VolumeSource{EmptyDir: stagingEmptyDirSource(fg.Spec.Backup)},
		})
		passRef := in.Profile.Encryption.PassphraseSecret
		volumes = append(volumes, corev1.Volume{
			Name: "backup-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  passRef.Name,
					DefaultMode: ptr32(0o400),
					Items: []corev1.KeyToPath{
						{
							Key:  passRef.PassphraseSecretKeyOrDefault(),
							Path: backupPassphraseFileName,
						},
					},
				},
			},
		})

		decEnv := []corev1.EnvVar{
			{Name: "BLOODRAVEN_TARGET_DIR", Value: decryptDir},
			{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(backup.Status.StorageType)},
			{Name: "BLOODRAVEN_BACKUP_PASSPHRASE_FILE", Value: backupPassphraseMountPath + "/" + backupPassphraseFileName},
		}
		decMounts := []corev1.VolumeMount{
			{Name: "restore-decrypted", MountPath: decryptDir},
			{Name: "backup-passphrase", MountPath: backupPassphraseMountPath, ReadOnly: true},
			{Name: "tmp", MountPath: tmpMountPath},
		}

		switch backup.Status.StorageType {
		case v1alpha1.BackupStorageS3:
			prefix := strings.TrimSuffix(backup.Status.Location, "/")
			decEnv = append(decEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: prefix},
				corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: in.Profile.Storage.S3.Bucket},
			)
			if in.Profile.Storage.S3.EndpointURL != "" {
				decEnv = append(decEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: in.Profile.Storage.S3.EndpointURL,
				})
			}
			if in.Profile.Storage.S3.Region != "" {
				decEnv = append(decEnv, corev1.EnvVar{Name: "AWS_REGION", Value: in.Profile.Storage.S3.Region})
			}
			if in.Profile.Storage.S3.CredentialsSecret != "" {
				decMounts = append(decMounts, corev1.VolumeMount{
					Name: "aws-creds", MountPath: backupAWSCredsMountPath, ReadOnly: true,
				})
				decEnv = append(decEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath,
				})
			}
			// Drop the S3 env from the mysqlsh container: it reads the
			// decrypted directory now.
			filtered := env[:0]
			for _, e := range env {
				switch e.Name {
				case "BLOODRAVEN_S3_BUCKET",
					"BLOODRAVEN_S3_ENDPOINT_OVERRIDE",
					"AWS_REGION",
					"BLOODRAVEN_AWS_CREDS_DIR":
					continue
				case "BLOODRAVEN_INPUT_URL":
					continue
				}
				filtered = append(filtered, e)
			}
			env = append(filtered, corev1.EnvVar{
				Name: "BLOODRAVEN_INPUT_URL", Value: decryptDir,
			})

		case v1alpha1.BackupStoragePVC:
			const decPVCMount = "/restore-src-encrypted"
			// Swap the read-only backup-src mount to the decrypt init
			// container; drop from the main container's mounts since
			// mysqlsh no longer reads ciphertext directly.
			volumes = filterOutVolumeByName(volumes, "backup-src")
			mounts = filterOutMountByName(mounts, "backup-src")
			claim := in.Profile.Storage.PVC.ClaimName
			if claim == "" {
				claim = ownedBackupPVCName(fg.Name, in.Profile.Name)
			}
			volumes = append(volumes, corev1.Volume{
				Name: "decrypt-src",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: claim,
						ReadOnly:  true,
					},
				},
			})
			decMounts = append(decMounts, corev1.VolumeMount{
				Name: "decrypt-src", MountPath: decPVCMount, ReadOnly: true,
			})
			// Derive the relative prefix under the PVC root from
			// backup.Status.Location (which is the absolute pod-side
			// path the backup wrote to).
			prefix := strings.TrimPrefix(backup.Status.Location, backupPVCMountPath+"/")
			prefix = strings.TrimSuffix(prefix, "/")
			decEnv = append(decEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_PVC_MOUNT_PATH", Value: decPVCMount},
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: prefix},
			)
			// Point the mysqlsh INPUT_URL at the decrypted staging
			// dir instead of the in-pod backup path.
			for i, e := range env {
				if e.Name == "BLOODRAVEN_INPUT_URL" {
					env[i].Value = decryptDir
					break
				}
			}
		default:
			return nil, fmt.Errorf("verification job: backup %q has unknown storageType %q for decrypt wiring",
				backup.Name, backup.Status.StorageType)
		}

		// Add the read-only decrypt dir to the main container so
		// loadDump can read files from it.
		mounts = append(mounts, corev1.VolumeMount{
			Name: "restore-decrypted", MountPath: decryptDir, ReadOnly: true,
		})

		_, initSC := mergeSecurityContexts(nil, nil)
		// Use spec.backup.resources when set; otherwise the helper
		// returns the 100m/128Mi default so the init container clears
		// LimitRange admission.
		var initBackupSrc *backupResourcesSource
		if fg.Spec.Backup != nil {
			initBackupSrc = &backupResourcesSource{Resources: fg.Spec.Backup.Resources}
		}
		initResources := effectiveBackupResources(initBackupSrc, defaultInitContainerResources())
		initContainers = append(initContainers, corev1.Container{
			Name:            "decrypt-download",
			Image:           operatorImageFromEnv,
			Command:         []string{"bloodraven", "decrypt-download"},
			Env:             decEnv,
			Resources:       initResources,
			VolumeMounts:    decMounts,
			SecurityContext: initSC,
		})
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
					// verify.sh and the optional decrypt init container
					// both run against storage and never call the
					// Kubernetes API.
					AutomountServiceAccountToken: boolPtr(false),
					InitContainers:               initContainers,
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

// validateSanityQuery enforces the CRD contract that
// SanityCheckSpec.Query is a single SQL statement with no embedded
// newlines. Multi-statement input would break the client-side timeout
// budget (`timeout` wraps `mysql -e`, and multiple statements share the
// budget) and complicates the scalar-capture contract, so we reject
// any `;` other than an optional trailing one before handing the value
// to `mysql -e`. Returns the query with a trailing `;` stripped so
// verify.sh sees a single clean statement.
func validateSanityQuery(q string) (string, error) {
	trimmed := strings.TrimSpace(q)
	if trimmed == "" {
		return "", fmt.Errorf("sanityCheck.query must not be empty")
	}
	if strings.ContainsAny(trimmed, "\n\r") {
		return "", fmt.Errorf("sanityCheck.query must be a single line (contains a newline)")
	}
	// Strip a single trailing `;` — common SQL convention — before
	// scanning for any remaining separators.
	trimmed = strings.TrimRight(trimmed, " \t")
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimRight(trimmed, " \t")
	if strings.Contains(trimmed, ";") {
		return "", fmt.Errorf("sanityCheck.query must be a single statement (contains `;`)")
	}
	return trimmed, nil
}

// verificationPITRSpec translates a verification's
// PointInTimeVerificationSpec into the PITR shape that the shared
// restore-path init-container builder understands. Returns nil (no
// replay) for empty / "none" input. "latest" is encoded as a
// far-future stop datetime so the download + mysqlbinlog pipeline
// includes every archived event; "timestamp" requires a caller-supplied
// RFC3339 instant.
func verificationPITRSpec(in *v1alpha1.PointInTimeVerificationSpec) (*v1alpha1.PointInTimeSpec, error) {
	if in == nil {
		return nil, nil
	}
	switch in.Mode {
	case "", "none":
		return nil, nil
	case "latest":
		// A stop datetime far enough in the future that no archived
		// event could land after it. mysqlbinlog --stop-datetime is
		// inclusive of the target time, and the pitr-download init
		// container uses it as the ceiling when selecting manifest
		// entries — so a sentinel value effectively means "everything
		// available".
		return &v1alpha1.PointInTimeSpec{StopDatetime: "9999-12-31T23:59:59Z"}, nil
	case "timestamp":
		if in.Timestamp == "" {
			return nil, fmt.Errorf("pointInTime.timestamp is required when mode=timestamp")
		}
		return &v1alpha1.PointInTimeSpec{StopDatetime: in.Timestamp}, nil
	default:
		return nil, fmt.Errorf("pointInTime.mode %q is not one of none|latest|timestamp", in.Mode)
	}
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
