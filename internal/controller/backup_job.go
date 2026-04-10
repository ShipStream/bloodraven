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
	backupScriptsMountPath = "/scripts"
	backupPVCMountPath     = "/backups"
	backupJobContainerName = "mysqlsh"
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
		labelAppName:        "mysql-backup",
		labelInstance:       fg.Name,
		labelFailoverGroup:  fg.Name,
		labelMysqlBackup:    backup.Name,
		labelBackupProfile:  in.Profile.Name,
		labelManagedBy:      managerName,
		labelResourceKind:   "backup",
	}

	// Resolve env vars + volumes for the selected storage backend.
	envVars := []corev1.EnvVar{
		{Name: "BLOODRAVEN_MYSQL_HOST", Value: backupMySQLHost(fg, in.SourceSite)},
		{Name: "BLOODRAVEN_BACKUP_NAME", Value: backup.Name},
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

	// envFrom: derived creds secret (user/password) + (for S3) the profile's
	// credentials secret so AWS_* env vars land in the container too.
	envFrom := []corev1.EnvFromSource{
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: in.CredsSecretName},
			},
		},
	}
	if in.Profile.Storage.Type == v1alpha1.BackupStorageS3 && in.Profile.Storage.S3 != nil {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: in.Profile.Storage.S3.CredentialsSecret,
				},
			},
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
					Containers: []corev1.Container{
						{
							Name:    backupJobContainerName,
							Image:   image,
							Command: []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/dump.py"},
							Env:     envVars,
							EnvFrom: envFrom,
							Resources: bspec.Resources,
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}

	return job, nil
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
		return outputPath, nil, volumes, mounts, nil

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
	b, err := json.Marshal(opts)
	if err != nil {
		return "", fmt.Errorf("marshal load options: %w", err)
	}
	return string(b), nil
}
