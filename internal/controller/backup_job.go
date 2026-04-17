package controller

import (
	"encoding/json"
	"fmt"

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
	image := backupImage(fg)

	// Job name derived from the backup CR name; capped at 63 chars.
	jobName := backupJobName(backup.Name)

	// Labels applied to both Job and pod so listings/queries work.
	labels := map[string]string{
		labelAppName:       "mysql-backup",
		labelInstance:      fg.Name,
		labelFailoverGroup: fg.Name,
		labelMysqlBackup:   backup.Name,
		labelBackupProfile: in.Profile.Name,
		labelManagedBy:     managerName,
		labelResourceKind:  "backup",
	}

	envVars := []corev1.EnvVar{
		{Name: "BLOODRAVEN_MYSQL_HOST", Value: backupMySQLHost(fg, in.SourceSite)},
		{Name: "BLOODRAVEN_BACKUP_NAME", Value: backup.Name},
		{Name: "BLOODRAVEN_MYSQL_CREDS_DIR", Value: backupCredsMountPath},
		{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(in.Profile.Storage.Type)},
		// mysqlsh needs a writable HOME because it creates state files
		// under ~/.mysqlsh. /home/mysqlsh is an emptyDir below.
		{Name: "HOME", Value: mysqlshHomeMountPath},
	}
	if fg.Spec.TLS != nil {
		envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_TLS", Value: "1"})
	}

	outputURL, extraEnv, volumes, volumeMounts, err := resolveBackupStorage(fg, in.Profile, backup.Name)
	if err != nil {
		return nil, err
	}
	envVars = append(envVars,
		corev1.EnvVar{Name: "BLOODRAVEN_OUTPUT_URL", Value: outputURL},
	)
	envVars = append(envVars, extraEnv...)

	// Dump options serialized once as JSON to keep the Python script static.
	dumpOptsJSON, err := marshalDumpOptions(in.Profile.Dump)
	if err != nil {
		return nil, err
	}
	envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_DUMP_OPTIONS", Value: dumpOptsJSON})

	// Mount the derived MySQL creds Secret as files (mode 0400) instead
	// of injecting via envFrom.
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

	// For S3 profiles, mount the AWS creds Secret as files the same way
	// and point the script at them.
	if in.Profile.Storage.Type == v1alpha1.BackupStorageS3 && in.Profile.Storage.S3 != nil {
		volumes = append(volumes, corev1.Volume{
			Name: "aws-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  in.Profile.Storage.S3.CredentialsSecret,
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

	// Shared scripts volume + dump.py mount.
	volumes = append(volumes, corev1.Volume{
		Name: "scripts",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: in.ScriptsConfigMapName},
			},
		},
	})
	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      "scripts",
		MountPath: backupScriptsMountPath,
		ReadOnly:  true,
	})

	// Writable home for mysqlsh state + /tmp (needed because the
	// container-level default sets ReadOnlyRootFilesystem=true).
	volumes = append(volumes,
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
		corev1.VolumeMount{Name: "mysqlsh-home", MountPath: mysqlshHomeMountPath},
		corev1.VolumeMount{Name: "tmp", MountPath: tmpMountPath},
	)

	// TLS volume, if configured.
	if fg.Spec.TLS != nil {
		volumes = append(volumes, corev1.Volume{
			Name: "tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: fg.Spec.TLS.SecretName},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "tls",
			MountPath: "/etc/mysql/tls",
			ReadOnly:  true,
		})
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
					Containers: []corev1.Container{
						{
							Name:            backupJobContainerName,
							Image:           image,
							Command:         []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/dump.py"},
							Env:             envVars,
							Resources:       bspec.Resources,
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
	return fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local:%d",
		fg.Name, siteName, fg.Namespace, mysqlPort)
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
		prefix := ""
		if s3.Prefix != "" {
			prefix = s3.Prefix + "/"
		}
		outputURL := fmt.Sprintf("%s%s/", prefix, backupName)
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
