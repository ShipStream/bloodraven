package controller

import (
	"encoding/json"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	backupScriptsMountPath  = "/scripts"
	backupPVCMountPath      = "/backups"
	backupJobContainerName  = "mysqlsh"
	backupCredsMountPath    = "/run/bloodraven/mysql-creds"
	backupAWSCredsMountPath = "/run/bloodraven/aws-creds"
	mysqlshHomeMountPath    = "/home/mysqlsh"
	tmpMountPath            = "/tmp"

	// backupPassphraseMountPath is where the encryption passphrase
	// Secret is mounted in the backup / restore / verification Jobs
	// and in the MySQL sidecar (for PITR). The file name is always
	// "passphrase" regardless of the Secret key, because the operator
	// uses SecretVolumeSource.Items to remap the configurable key onto
	// this fixed file path.
	backupPassphraseMountPath = "/run/bloodraven/backup-passphrase"
	backupPassphraseFileName  = "passphrase"

	// backupStagingMountPath is the emptyDir where mysqlsh writes the
	// dump before the bloodraven container encrypts and uploads it.
	// Only used on the encrypted Job layout; the plain layout still
	// has mysqlsh write straight to the final destination.
	backupStagingMountPath = "/staging"

	// backupDumpInitContainerName is the init-container name that runs
	// mysqlsh when encryption is enabled. Kept distinct from the
	// main-container name so `kubectl logs -c mysqlsh-dump` targets
	// the dump phase unambiguously when debugging.
	backupDumpInitContainerName = "mysqlsh-dump"

	// backupEncryptUploadContainerName is the main-container name used
	// when encryption is enabled. When encryption is off, the main
	// container keeps the legacy "mysqlsh" name.
	backupEncryptUploadContainerName = "backup-encrypt-upload"
)

// BackupJobInputs bundles the resolved inputs for BuildBackupJob. Keeping
// everything in a struct avoids an exploding parameter list as new fields
// (TLS, extra labels, env overrides) get added.
type BackupJobInputs struct {
	FailoverGroup *v1alpha1.MysqlFailoverGroup
	Profile       v1alpha1.BackupProfile
	Backup        *v1alpha1.MysqlBackup
	SourceSite    string
	// CredsSecretName is the name of the derived Secret created by the
	// MysqlBackupReconciler that holds MYSQL_USER / MYSQL_PASSWORD.
	CredsSecretName string
	// ScriptsConfigMapName is the shared per-group ConfigMap holding the
	// embedded dump.py script.
	ScriptsConfigMapName string
}

// BuildBackupJob renders a batchv1.Job that executes mysqlsh
// util.dumpInstance() against the selected source site.
//
// Credentials for MySQL (and, when targeting S3, for AWS) are mounted as
// files under /run/bloodraven/{mysql,aws}-creds rather than injected as
// environment variables via envFrom. This keeps plaintext secrets out of
// the pod spec and out of process-level /proc/PID/environ inspection.
// The embedded Python scripts read the files via the
// BLOODRAVEN_MYSQL_CREDS_DIR / BLOODRAVEN_AWS_CREDS_DIR env vars.
//
// Pod- and container-level security contexts are hardened by default
// (RunAsNonRoot, ReadOnlyRootFilesystem, Capabilities.Drop=ALL, seccomp
// RuntimeDefault). Users can override individual fields via
// spec.backup.podSecurityContext / containerSecurityContext; those
// override values are merged on top of the defaults, never the other
// way around.
//
// Encryption (spec.backup.profiles[].encryption) flips the Job shape
// into a two-phase layout:
//
//	initContainers:
//	    mysqlsh-dump   - mysqlsh writes to /staging (emptyDir).
//	containers:
//	    backup-encrypt-upload - bloodraven reads /staging, encrypts
//	                            each artifact with AES-256-GCM, and
//	                            uploads the ciphertext to the final
//	                            destination; emits the usual
//	                            BLOODRAVEN_DUMP_COMPLETE sentinel so
//	                            the reconciler status-parse path works
//	                            identically either way.
//
// The Job is not owner-ref'd here; the caller must attach an owner
// reference to the MysqlBackup CR via controllerutil.SetControllerReference
// before creating it.
func BuildBackupJob(in BackupJobInputs) (*batchv1.Job, error) {
	fg := in.FailoverGroup
	backup := in.Backup

	if in.SourceSite == "" {
		return nil, fmt.Errorf("backup job: source site is empty")
	}
	if in.CredsSecretName == "" {
		return nil, fmt.Errorf("backup job: creds secret name is empty")
	}
	if in.ScriptsConfigMapName == "" {
		return nil, fmt.Errorf("backup job: scripts configmap name is empty")
	}

	bspec := fg.Spec.Backup
	mysqlshImage := backupImage(fg)
	encryption := in.Profile.Encryption
	encrypted := in.Profile.EncryptionEnabled()
	// Treat a non-nil Encryption with a missing passphraseSecret.name
	// as a hard misconfiguration. Falling back silently to the
	// unencrypted path would be an unsafe surprise — an operator who
	// set encryption.algorithm but forgot the Secret name would end up
	// with plaintext backups that their dashboard claims are encrypted.
	if encryption != nil && encryption.PassphraseSecret.Name == "" {
		return nil, fmt.Errorf("backup job: profile %q encryption requires passphraseSecret.name", in.Profile.Name)
	}

	// Job name derived from the backup CR name; capped at 63 chars.
	jobName := backupJobName(backup.Name)

	// Labels applied to both Job and pod so listings/queries work.
	labels := map[string]string{
		labelAppName:       "mysql-backup",
		labelInstance:      fg.Name,
		labelFailoverGroup: fg.Name,
		labelSite:          in.SourceSite,
		labelMysqlBackup:   backup.Name,
		labelBackupProfile: in.Profile.Name,
		labelManagedBy:     managerName,
		labelResourceKind:  "backup",
	}
	if encrypted {
		labels[labelBackupEncrypted] = "true"
	}

	// --- Shared base envVars / storage / credentials volumes -----------
	//
	// Start by building the full "dump → storage target" wiring the
	// way the plain path uses it. When encryption is on we redirect the
	// mysqlsh output to a local emptyDir and move the real storage
	// wiring onto the encrypt-upload container; but all of the
	// downstream AWS / PVC plumbing is identical either way, so we
	// compute it once up-front.

	baseEnv := []corev1.EnvVar{
		{Name: "BLOODRAVEN_MYSQL_HOST", Value: backupMySQLHost(fg, in.SourceSite)},
		{Name: "BLOODRAVEN_BACKUP_NAME", Value: backup.Name},
		{Name: "BLOODRAVEN_MYSQL_CREDS_DIR", Value: backupCredsMountPath},
		{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(in.Profile.Storage.Type)},
	}
	if fg.Spec.TLS != nil {
		baseEnv = append(baseEnv, corev1.EnvVar{Name: "BLOODRAVEN_TLS", Value: "1"})
	}

	// resolveBackupStorage returns the user-facing destination — S3
	// prefix / PVC path. In the encrypted flow this is the target the
	// uploader writes to, not the dump destination.
	outputURL, storageEnv, storageVolumes, storageMounts, err := resolveBackupStorage(fg, in.Profile, backup.Name)
	if err != nil {
		return nil, err
	}

	// mysql-creds volume is used by both the plain dump and the
	// encrypted dump init container (bloodraven doesn't need mysql
	// creds; the uploader only talks to storage, not to MySQL).
	mysqlCredsVolume := corev1.Volume{
		Name: "mysql-creds",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  in.CredsSecretName,
				DefaultMode: ptr32(0o400),
			},
		},
	}
	mysqlCredsMount := corev1.VolumeMount{
		Name:      "mysql-creds",
		MountPath: backupCredsMountPath,
		ReadOnly:  true,
	}

	var awsCredsVolume *corev1.Volume
	var awsCredsMount *corev1.VolumeMount
	if in.Profile.Storage.Type == v1alpha1.BackupStorageS3 && in.Profile.Storage.S3 != nil {
		awsCredsVolume = &corev1.Volume{
			Name: "aws-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  in.Profile.Storage.S3.CredentialsSecret,
					DefaultMode: ptr32(0o400),
				},
			},
		}
		awsCredsMount = &corev1.VolumeMount{
			Name:      "aws-creds",
			MountPath: backupAWSCredsMountPath,
			ReadOnly:  true,
		}
	}

	// Shared scripts volume (dump.py lives here).
	scriptsVolume := corev1.Volume{
		Name: "scripts",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: in.ScriptsConfigMapName},
			},
		},
	}
	scriptsMount := corev1.VolumeMount{
		Name:      "scripts",
		MountPath: backupScriptsMountPath,
		ReadOnly:  true,
	}

	// Writable home for mysqlsh state + /tmp (needed because the
	// container-level default sets ReadOnlyRootFilesystem=true).
	mysqlshHomeVolume := corev1.Volume{
		Name:         "mysqlsh-home",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
	tmpVolume := corev1.Volume{
		Name:         "tmp",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}

	dumpOptsJSON, err := marshalDumpOptions(in.Profile.Dump)
	if err != nil {
		return nil, err
	}

	activeDeadline := bspec.ActiveDeadlineSeconds
	if activeDeadline <= 0 {
		activeDeadline = 7200
	}
	backoffLimit := bspec.BackoffLimit
	if backoffLimit < 0 {
		backoffLimit = 0
	}

	podSC, containerSC := mergeSecurityContexts(bspec.PodSecurityContext, bspec.ContainerSecurityContext)

	// --- Plain / encrypted split ---------------------------------------

	var initContainers []corev1.Container
	var mainContainer corev1.Container
	var volumes []corev1.Volume

	if !encrypted {
		// Legacy single-container layout. mysqlsh dumps directly to
		// the final storage target; status.location reflects that
		// target verbatim.
		envVars := append([]corev1.EnvVar{}, baseEnv...)
		envVars = append(envVars, corev1.EnvVar{Name: "HOME", Value: mysqlshHomeMountPath})
		envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_OUTPUT_URL", Value: outputURL})
		envVars = append(envVars, storageEnv...)
		envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_DUMP_OPTIONS", Value: dumpOptsJSON})
		if awsCredsMount != nil {
			envVars = append(envVars, corev1.EnvVar{
				Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath,
			})
		}

		mounts := []corev1.VolumeMount{mysqlCredsMount}
		if awsCredsMount != nil {
			mounts = append(mounts, *awsCredsMount)
		}
		mounts = append(mounts, scriptsMount,
			corev1.VolumeMount{Name: "mysqlsh-home", MountPath: mysqlshHomeMountPath},
			corev1.VolumeMount{Name: "tmp", MountPath: tmpMountPath},
		)
		mounts = append(mounts, storageMounts...)

		volumes = append(volumes, mysqlCredsVolume)
		if awsCredsVolume != nil {
			volumes = append(volumes, *awsCredsVolume)
		}
		volumes = append(volumes, scriptsVolume, mysqlshHomeVolume, tmpVolume)
		volumes = append(volumes, storageVolumes...)

		if fg.Spec.TLS != nil {
			volumes = append(volumes, corev1.Volume{
				Name: "tls",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: fg.Spec.TLS.SecretName},
				},
			})
			mounts = append(mounts, corev1.VolumeMount{
				Name: "tls", MountPath: "/etc/mysql/tls", ReadOnly: true,
			})
		}

		mainContainer = corev1.Container{
			Name:            backupJobContainerName,
			Image:           mysqlshImage,
			Command:         []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/dump.py"},
			Env:             envVars,
			Resources:       bspec.Resources,
			VolumeMounts:    mounts,
			SecurityContext: containerSC,
		}
	} else {
		// Encrypted layout. The passphrase Secret is mounted on the
		// uploader only; the mysqlsh dump container never sees it
		// (defense-in-depth: a mysqlsh exploit that read files in its
		// pod wouldn't surface the passphrase).
		if encryption.PassphraseSecret.Name == "" {
			return nil, fmt.Errorf("backup job: profile %q encryption requires passphraseSecret.name", in.Profile.Name)
		}
		if operatorImageFromEnv == "" {
			return nil, fmt.Errorf("backup job: operator image is not configured (SetOperatorImageDefaults); required for encrypted profile %q", in.Profile.Name)
		}

		// -------- Init container: mysqlsh → /staging -------------------
		// Size cap comes from spec.backup.stagingVolumeSizeLimit when
		// set (AUDIT H6). Leaving it unset preserves the previous
		// "bounded only by node ephemeral-storage" behavior.
		stagingVolume := corev1.Volume{
			Name:         "staging",
			VolumeSource: corev1.VolumeSource{EmptyDir: stagingEmptyDirSource(fg.Spec.Backup)},
		}

		dumpEnv := append([]corev1.EnvVar{}, baseEnv...)
		dumpEnv = append(dumpEnv,
			corev1.EnvVar{Name: "HOME", Value: mysqlshHomeMountPath},
			corev1.EnvVar{Name: "BLOODRAVEN_DUMP_OPTIONS", Value: dumpOptsJSON},
			// Override to a local staging path; the uploader reads
			// from here and does the actual storage write.
			corev1.EnvVar{Name: "BLOODRAVEN_STORAGE_TYPE", Value: "PVC"},
			corev1.EnvVar{Name: "BLOODRAVEN_OUTPUT_URL", Value: backupStagingMountPath + "/" + backup.Name},
		)

		dumpMounts := []corev1.VolumeMount{
			mysqlCredsMount,
			scriptsMount,
			{Name: "mysqlsh-home", MountPath: mysqlshHomeMountPath},
			{Name: "tmp", MountPath: tmpMountPath},
			{Name: "staging", MountPath: backupStagingMountPath},
		}
		if fg.Spec.TLS != nil {
			dumpMounts = append(dumpMounts, corev1.VolumeMount{
				Name: "tls", MountPath: "/etc/mysql/tls", ReadOnly: true,
			})
		}

		initContainers = append(initContainers, corev1.Container{
			Name:            backupDumpInitContainerName,
			Image:           mysqlshImage,
			Command:         []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/dump.py"},
			Env:             dumpEnv,
			Resources:       bspec.Resources,
			VolumeMounts:    dumpMounts,
			SecurityContext: containerSC,
		})

		// -------- Main container: bloodraven encrypt-upload -----------
		passphraseVolume := corev1.Volume{
			Name: "backup-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  encryption.PassphraseSecret.Name,
					DefaultMode: ptr32(0o400),
					Items: []corev1.KeyToPath{
						{
							Key:  encryption.PassphraseSecret.PassphraseSecretKeyOrDefault(),
							Path: backupPassphraseFileName,
						},
					},
				},
			},
		}
		passphraseMount := corev1.VolumeMount{
			Name:      "backup-passphrase",
			MountPath: backupPassphraseMountPath,
			ReadOnly:  true,
		}

		uploadEnv := []corev1.EnvVar{
			{Name: "BLOODRAVEN_BACKUP_NAME", Value: backup.Name},
			{Name: "BLOODRAVEN_SOURCE_DIR", Value: backupStagingMountPath + "/" + backup.Name},
			{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(in.Profile.Storage.Type)},
			{Name: "BLOODRAVEN_OUTPUT_URL", Value: outputURL},
			{
				Name:  "BLOODRAVEN_ENCRYPTION_ALGORITHM",
				Value: encryption.AlgorithmOrDefault(),
			},
			{
				Name:  "BLOODRAVEN_BACKUP_PASSPHRASE_FILE",
				Value: backupPassphraseMountPath + "/" + backupPassphraseFileName,
			},
		}
		uploadEnv = append(uploadEnv, storageEnv...)
		if awsCredsMount != nil {
			uploadEnv = append(uploadEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath,
			})
		}

		uploadMounts := []corev1.VolumeMount{
			{Name: "staging", MountPath: backupStagingMountPath, ReadOnly: true},
			{Name: "tmp", MountPath: tmpMountPath},
			passphraseMount,
		}
		if awsCredsMount != nil {
			uploadMounts = append(uploadMounts, *awsCredsMount)
		}
		uploadMounts = append(uploadMounts, storageMounts...)

		volumes = append(volumes, mysqlCredsVolume)
		if awsCredsVolume != nil {
			volumes = append(volumes, *awsCredsVolume)
		}
		volumes = append(volumes, scriptsVolume, mysqlshHomeVolume, tmpVolume, stagingVolume, passphraseVolume)
		volumes = append(volumes, storageVolumes...)
		if fg.Spec.TLS != nil {
			volumes = append(volumes, corev1.Volume{
				Name: "tls",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: fg.Spec.TLS.SecretName},
				},
			})
		}

		mainContainer = corev1.Container{
			Name:            backupEncryptUploadContainerName,
			Image:           operatorImageFromEnv,
			Command:         []string{"bloodraven", "encrypt-upload"},
			Env:             uploadEnv,
			Resources:       bspec.Resources,
			VolumeMounts:    uploadMounts,
			SecurityContext: containerSC,
		}
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: fg.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &activeDeadline,
			BackoffLimit:          &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: bspec.ImagePullSecrets,
					SecurityContext:  podSC,
					// Backup execution Jobs run mysqlsh/bloodraven and
					// never call the Kubernetes API, so we drop the
					// auto-mounted ServiceAccount token to shrink the
					// blast radius of a container compromise.
					AutomountServiceAccountToken: boolPtr(false),
					InitContainers:               initContainers,
					Containers:                   []corev1.Container{mainContainer},
					Volumes:                      volumes,
				},
			},
		},
	}

	return job, nil
}

// mergeSecurityContexts returns the hardened pod- and container-level
// SecurityContexts used by backup, restore, and cleanup Jobs. User
// overrides from spec.backup.podSecurityContext /
// containerSecurityContext are merged on top of the defaults field by
// field: any unset user field stays at the default, any set user field
// wins.
//
// The defaults match the Restricted Pod Security Standard:
//
//	pod:
//	  RunAsNonRoot:   true
//	  RunAsUser:      27   (mysql)
//	  RunAsGroup:     27
//	  FSGroup:        27
//	  SeccompProfile: RuntimeDefault
//
//	container:
//	  AllowPrivilegeEscalation: false
//	  ReadOnlyRootFilesystem:   true
//	  RunAsNonRoot:             true
//	  Capabilities.Drop:        [ALL]
//	  SeccompProfile:           RuntimeDefault
//
// The `27` UID matches the stock MySQL image and keeps the dump Job
// compatible with PVC volumes written by the main MySQL Deployment.
func mergeSecurityContexts(userPod *corev1.PodSecurityContext, userContainer *corev1.SecurityContext) (*corev1.PodSecurityContext, *corev1.SecurityContext) {
	t := true
	f := false
	mysqlUID := int64(27)
	mysqlGID := int64(27)

	pod := &corev1.PodSecurityContext{
		RunAsNonRoot: &t,
		RunAsUser:    &mysqlUID,
		RunAsGroup:   &mysqlGID,
		FSGroup:      &mysqlGID,
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
	if userPod != nil {
		if userPod.RunAsNonRoot != nil {
			pod.RunAsNonRoot = userPod.RunAsNonRoot
		}
		if userPod.RunAsUser != nil {
			pod.RunAsUser = userPod.RunAsUser
		}
		if userPod.RunAsGroup != nil {
			pod.RunAsGroup = userPod.RunAsGroup
		}
		if userPod.FSGroup != nil {
			pod.FSGroup = userPod.FSGroup
		}
		if userPod.FSGroupChangePolicy != nil {
			pod.FSGroupChangePolicy = userPod.FSGroupChangePolicy
		}
		if userPod.SeccompProfile != nil {
			pod.SeccompProfile = userPod.SeccompProfile
		}
		if userPod.SELinuxOptions != nil {
			pod.SELinuxOptions = userPod.SELinuxOptions
		}
		if userPod.WindowsOptions != nil {
			pod.WindowsOptions = userPod.WindowsOptions
		}
		if len(userPod.SupplementalGroups) > 0 {
			pod.SupplementalGroups = userPod.SupplementalGroups
		}
		if len(userPod.Sysctls) > 0 {
			pod.Sysctls = userPod.Sysctls
		}
	}

	c := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &f,
		ReadOnlyRootFilesystem:   &t,
		RunAsNonRoot:             &t,
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
	if userContainer != nil {
		if userContainer.AllowPrivilegeEscalation != nil {
			c.AllowPrivilegeEscalation = userContainer.AllowPrivilegeEscalation
		}
		if userContainer.ReadOnlyRootFilesystem != nil {
			c.ReadOnlyRootFilesystem = userContainer.ReadOnlyRootFilesystem
		}
		if userContainer.RunAsNonRoot != nil {
			c.RunAsNonRoot = userContainer.RunAsNonRoot
		}
		if userContainer.RunAsUser != nil {
			c.RunAsUser = userContainer.RunAsUser
		}
		if userContainer.RunAsGroup != nil {
			c.RunAsGroup = userContainer.RunAsGroup
		}
		if userContainer.Privileged != nil {
			c.Privileged = userContainer.Privileged
		}
		if userContainer.ProcMount != nil {
			c.ProcMount = userContainer.ProcMount
		}
		if userContainer.Capabilities != nil {
			c.Capabilities = userContainer.Capabilities
		}
		if userContainer.SeccompProfile != nil {
			c.SeccompProfile = userContainer.SeccompProfile
		}
		if userContainer.SELinuxOptions != nil {
			c.SELinuxOptions = userContainer.SELinuxOptions
		}
		if userContainer.WindowsOptions != nil {
			c.WindowsOptions = userContainer.WindowsOptions
		}
	}

	return pod, c
}

// backupMySQLHost returns the in-cluster hostname of a site's MySQL service.
func backupMySQLHost(fg *v1alpha1.MysqlFailoverGroup, siteName string) string {
	return fmt.Sprintf("%s:%d", internalSiteServiceHost(fg.Name, siteName, fg.Namespace), mysqlPort)
}

// resolveBackupStorage returns (outputURL, extraEnv, volumes, mounts) for
// the selected profile storage. For S3 destinations it also encodes the
// bucket/endpoint as env vars so the static Python script can pick them up
// without templating.
func resolveBackupStorage(fg *v1alpha1.MysqlFailoverGroup, profile v1alpha1.BackupProfile, backupName string) (
	string, []corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error) {

	switch profile.Storage.Type {
	case v1alpha1.BackupStorageS3:
		s3 := profile.Storage.S3
		if s3 == nil {
			return "", nil, nil, nil, fmt.Errorf("profile %q storage.type=S3 but storage.s3 is nil", profile.Name)
		}
		prefix := strings.TrimRight(s3.Prefix, "/")
		if prefix != "" {
			prefix += "/"
		}
		// MySQL Shell's S3 dump APIs treat BLOODRAVEN_OUTPUT_URL as a prefix and
		// append their own separators while listing/writing dump objects. Passing a
		// trailing slash yields a double-slash prefix ("name//") that S3-compatible
		// stores such as RustFS reject with InvalidArgument.
		outputURL := fmt.Sprintf("%s%s", prefix, backupName)
		env := []corev1.EnvVar{
			{Name: "BLOODRAVEN_S3_BUCKET", Value: s3.Bucket},
		}
		if s3.EndpointURL != "" {
			env = append(env, corev1.EnvVar{
				Name:  "BLOODRAVEN_S3_ENDPOINT_OVERRIDE",
				Value: s3.EndpointURL,
			})
		}
		if s3.Region != "" {
			env = append(env, corev1.EnvVar{Name: "AWS_REGION", Value: s3.Region})
		}
		return outputURL, env, nil, nil, nil

	case v1alpha1.BackupStoragePVC:
		pvc := profile.Storage.PVC
		if pvc == nil {
			return "", nil, nil, nil, fmt.Errorf("profile %q storage.type=PVC but storage.pvc is nil", profile.Name)
		}
		claim := pvc.ClaimName
		if claim == "" {
			claim = ownedBackupPVCName(fg.Name, profile.Name)
		}
		sub := pvc.SubPath
		outputPath := fmt.Sprintf("%s/%s", backupPVCMountPath, backupName)
		if sub != "" {
			outputPath = fmt.Sprintf("%s/%s/%s", backupPVCMountPath, sub, backupName)
		}
		volumes := []corev1.Volume{{
			Name: "backups",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim,
				},
			},
		}}
		mounts := []corev1.VolumeMount{{
			Name:      "backups",
			MountPath: backupPVCMountPath,
		}}
		env := []corev1.EnvVar{
			{Name: "BLOODRAVEN_PVC_MOUNT_PATH", Value: backupPVCMountPath},
		}
		return outputPath, env, volumes, mounts, nil

	default:
		return "", nil, nil, nil, fmt.Errorf("profile %q has unknown storage type %q", profile.Name, profile.Storage.Type)
	}
}

// marshalDumpOptions serializes the DumpOptions into the shape expected by
// mysqlsh util.dumpInstance(). Nil defaults fall back to the script's own
// defaults (i.e. mysqlsh defaults).
func marshalDumpOptions(d *v1alpha1.DumpOptions) (string, error) {
	if d == nil {
		return "{}", nil
	}
	opts := map[string]any{}
	if d.Threads > 0 {
		opts["threads"] = d.Threads
	}
	if d.BytesPerChunk != "" {
		opts["bytesPerChunk"] = d.BytesPerChunk
	}
	if d.Compression != "" {
		opts["compression"] = d.Compression
	}
	if len(d.ExcludeSchemas) > 0 {
		opts["excludeSchemas"] = d.ExcludeSchemas
	}
	if len(d.IncludeSchemas) > 0 {
		opts["includeSchemas"] = d.IncludeSchemas
	}
	if d.Consistent != nil {
		opts["consistent"] = *d.Consistent
	}
	if d.Ocimds != nil {
		opts["ocimds"] = *d.Ocimds
	}
	b, err := json.Marshal(opts)
	if err != nil {
		return "", fmt.Errorf("marshal dump options: %w", err)
	}
	return string(b), nil
}

// marshalLoadOptions serializes LoadOptions into the shape expected by
// mysqlsh util.loadDump().
func marshalLoadOptions(l *v1alpha1.LoadOptions) (string, error) {
	if l == nil {
		return "{}", nil
	}
	opts := map[string]any{}
	if l.Threads > 0 {
		opts["threads"] = l.Threads
	}
	if l.ResetProgress != nil {
		opts["resetProgress"] = *l.ResetProgress
	}
	if l.SkipBinlog != nil {
		opts["skipBinlog"] = *l.SkipBinlog
	}
	if l.LoadIndexes != nil {
		opts["loadIndexes"] = *l.LoadIndexes
	}
	if len(l.IncludeSchemas) > 0 {
		opts["includeSchemas"] = l.IncludeSchemas
	}
	if len(l.ExcludeSchemas) > 0 {
		opts["excludeSchemas"] = l.ExcludeSchemas
	}
	b, err := json.Marshal(opts)
	if err != nil {
		return "", fmt.Errorf("marshal load options: %w", err)
	}
	return string(b), nil
}
