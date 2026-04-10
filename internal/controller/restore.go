package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// ensureTrailingSlash normalizes a dump prefix/location so it ends with
// a forward slash. mysqlsh util.loadDump() expects the directory-style
// prefix produced by util.dumpInstance().
func ensureTrailingSlash(s string) string {
	if s == "" {
		return s
	}
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

// isS3Location heuristically detects when a MysqlBackup.status.location
// points at an S3 bucket. util.dumpInstance() stores S3 outputs as a
// bare prefix (e.g. "lion/seed/") rather than an "s3://" URL, so we
// have to infer from the absence of a local filesystem path.
func isS3Location(loc string) bool {
	if loc == "" {
		return false
	}
	if strings.HasPrefix(loc, "s3://") {
		return true
	}
	if strings.HasPrefix(loc, "/") || strings.HasPrefix(loc, "pvc://") {
		return false
	}
	// Relative prefix with no leading slash => S3-style.
	return true
}

// restoreInFlight reports whether spec.initFromBackup is set and the
// one-shot restore has not yet reached Succeeded. Callers use it to gate
// side effects that would race the restore Job (notably the topology
// manager's fresh-deploy auto-clone).
func restoreInFlight(fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg.Spec.InitFromBackup == nil {
		return false
	}
	if fg.Status.Restore == nil {
		return true
	}
	return fg.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded
}

// reconcileRestoreJob creates and observes the one-shot restore Job when
// spec.initFromBackup is set. The returned duration is non-zero when the
// caller should requeue and NOT proceed to the pod-label sync / topology
// gate in the main reconcile loop.
func (r *MysqlFailoverGroupReconciler) reconcileRestoreJob(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	if fg.Spec.InitFromBackup == nil {
		// Clear stale restore status if the user removed the field.
		if fg.Status.Restore != nil {
			patch := client.MergeFrom(fg.DeepCopy())
			fg.Status.Restore = nil
			_ = r.Status().Patch(ctx, fg, patch)
		}
		return 0, nil
	}
	if fg.Status.Restore != nil && fg.Status.Restore.Phase == v1alpha1.BackupPhaseSucceeded {
		return 0, nil
	}

	if len(fg.Spec.Sites) == 0 {
		return 0, fmt.Errorf("initFromBackup set but no sites configured")
	}
	targetSite := fg.Spec.Sites[0].Name

	// Wait for the target site's Deployment to be Ready before creating
	// the restore Job; mysqlsh util.loadDump() needs a live server.
	deployName := resourceName(fg.Name, targetSite)
	var deploy appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: deployName}, &deploy); err != nil {
		if apierrors.IsNotFound(err) {
			return 10 * time.Second, nil
		}
		return 0, fmt.Errorf("get target deployment: %w", err)
	}
	if deploy.Status.ReadyReplicas < 1 {
		r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
			Phase:      v1alpha1.BackupPhasePending,
			TargetSite: targetSite,
			Message:    "waiting for primary MySQL to become ready",
		})
		return 10 * time.Second, nil
	}

	// Ensure derived creds Secret for the restore Job.
	credsName := restoreCredsSecretName(fg.Name)
	if err := r.ensureRestoreCredsSecret(ctx, fg, credsName); err != nil {
		return 0, fmt.Errorf("ensure restore creds secret: %w", err)
	}

	// Ensure the restore Job.
	var job batchv1.Job
	jobName := restoreJobName(fg.Name, targetSite)
	jobKey := types.NamespacedName{Namespace: fg.Namespace, Name: jobName}
	if err := r.Get(ctx, jobKey, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("get restore job: %w", err)
		}

		built, err := r.buildRestoreJob(ctx, fg, targetSite, credsName)
		if err != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreBuildFailed", "%s", err.Error())
			r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
				Phase:      v1alpha1.BackupPhaseFailed,
				TargetSite: targetSite,
				Message:    err.Error(),
			})
			return 0, nil
		}
		if err := controllerutil.SetControllerReference(fg, built, r.Scheme); err != nil {
			return 0, fmt.Errorf("set restore job owner ref: %w", err)
		}
		if err := r.Create(ctx, built); err != nil {
			return 0, fmt.Errorf("create restore job: %w", err)
		}
		now := metav1.Now()
		r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
			Phase:      v1alpha1.BackupPhaseRunning,
			JobName:    built.Name,
			TargetSite: targetSite,
			StartTime:  &now,
			Message:    "restore Job created",
		})
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreStarted",
			"created restore Job %s targeting site %s", built.Name, targetSite)
		return 15 * time.Second, nil
	}

	// Observe the Job.
	phase, message := jobPhase(&job, "restore")
	if phase == "" {
		return 15 * time.Second, nil
	}

	now := metav1.Now()
	rs := &v1alpha1.RestoreStatus{
		Phase:          phase,
		JobName:        job.Name,
		TargetSite:     targetSite,
		StartTime:      job.Status.StartTime,
		CompletionTime: &now,
		Message:        message,
	}
	if rs.StartTime == nil {
		if fg.Status.Restore != nil && fg.Status.Restore.StartTime != nil {
			rs.StartTime = fg.Status.Restore.StartTime
		} else {
			rs.StartTime = &now
		}
	}
	r.setRestoreStatus(ctx, fg, rs)

	if phase == v1alpha1.BackupPhaseSucceeded {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreSucceeded",
			"restore Job %s completed: %s", job.Name, message)
		return 0, nil
	}

	r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreFailed",
		"restore Job %s failed: %s", job.Name, message)
	// No automatic retry. Operator must delete the Job to re-trigger.
	return 0, nil
}

// setRestoreStatus patches fg.status.restore and swallows IsNotFound / no-op.
func (r *MysqlFailoverGroupReconciler) setRestoreStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, rs *v1alpha1.RestoreStatus) {
	patch := client.MergeFrom(fg.DeepCopy())
	fg.Status.Restore = rs
	if err := r.Status().Patch(ctx, fg, patch); err != nil && !apierrors.IsNotFound(err) {
		// Non-fatal; logged by caller via controller-runtime log.
	}
}

// ensureRestoreCredsSecret creates or updates the derived Secret used by
// the restore Job, parsing the group's DSN secret.
func (r *MysqlFailoverGroupReconciler) ensureRestoreCredsSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, secretName string) error {
	var dsnSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.SecretName}, &dsnSecret); err != nil {
		return fmt.Errorf("get dsn secret: %w", err)
	}
	dsnBytes, ok := dsnSecret.Data["dsn"]
	if !ok {
		return fmt.Errorf("secret %s missing 'dsn' key", fg.Spec.SecretName)
	}
	parsed, err := mysqldriver.ParseDSN(string(dsnBytes))
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}

	derived := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: fg.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, derived, func() error {
		if err := controllerutil.SetControllerReference(fg, derived, r.Scheme); err != nil {
			return err
		}
		if derived.Labels == nil {
			derived.Labels = map[string]string{}
		}
		derived.Labels[labelFailoverGroup] = fg.Name
		derived.Labels[labelManagedBy] = managerName
		derived.Labels[labelResourceKind] = "restore"
		derived.Type = corev1.SecretTypeOpaque
		derived.Data = map[string][]byte{
			"MYSQL_USER":     []byte(parsed.User),
			"MYSQL_PASSWORD": []byte(parsed.Passwd),
		}
		return nil
	})
	return err
}

// buildRestoreJob resolves the initFromBackup source and constructs the
// one-shot batchv1.Job. Returns an error when the source cannot be
// resolved (e.g. referenced MysqlBackup has no status.location yet).
func (r *MysqlFailoverGroupReconciler) buildRestoreJob(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, targetSite, credsName string) (*batchv1.Job, error) {
	src := fg.Spec.InitFromBackup.Source

	var (
		inputURL     string
		extraEnv     []corev1.EnvVar
		envFromExtra []corev1.EnvFromSource
		extraVolumes []corev1.Volume
		extraMounts  []corev1.VolumeMount
	)

	switch {
	case src.MysqlBackupRef != nil:
		var ref v1alpha1.MysqlBackup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: src.MysqlBackupRef.Name}, &ref); err != nil {
			return nil, fmt.Errorf("get referenced mysqlbackup: %w", err)
		}
		if ref.Status.Phase != v1alpha1.BackupPhaseSucceeded || ref.Status.Location == "" {
			return nil, fmt.Errorf("referenced mysqlbackup %s is not Succeeded or has no location", ref.Name)
		}
		inputURL = ensureTrailingSlash(ref.Status.Location)

		// If the referenced backup lived in S3, the restore Job needs
		// the bucket + credentials for that bucket. We walk back to the
		// profile on fg.spec.backup; if it was deleted, the raw
		// location is an S3-relative prefix that mysqlsh cannot
		// resolve without credentials, so fail fast with a clear
		// error instead of letting the Job hit a confusing runtime
		// failure.
		profile := findProfile(fg, ref.Spec.ProfileName)
		if isS3Location(ref.Status.Location) && (profile == nil || profile.Storage.Type != v1alpha1.BackupStorageS3 || profile.Storage.S3 == nil) {
			return nil, fmt.Errorf(
				"initFromBackup.source.mysqlBackupRef=%q resolves to an S3 location (%q) but profile %q is missing from spec.backup.profiles; "+
					"either restore the profile or set initFromBackup.source.s3 explicitly",
				ref.Name, ref.Status.Location, ref.Spec.ProfileName)
		}
		if profile != nil && profile.Storage.Type == v1alpha1.BackupStorageS3 && profile.Storage.S3 != nil {
			extraEnv = append(extraEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: profile.Storage.S3.Bucket},
			)
			if profile.Storage.S3.EndpointURL != "" {
				extraEnv = append(extraEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: profile.Storage.S3.EndpointURL,
				})
			}
			if profile.Storage.S3.Region != "" {
				extraEnv = append(extraEnv, corev1.EnvVar{Name: "AWS_REGION", Value: profile.Storage.S3.Region})
			}
			envFromExtra = append(envFromExtra, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: profile.Storage.S3.CredentialsSecret},
				},
			})
		}

	case src.S3 != nil:
		// mysqlsh util.loadDump() expects a directory-style prefix with
		// a trailing slash, matching what util.dumpInstance() writes.
		inputURL = ensureTrailingSlash(src.S3.Prefix)
		extraEnv = append(extraEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: src.S3.Bucket},
		)
		if src.S3.EndpointURL != "" {
			extraEnv = append(extraEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: src.S3.EndpointURL,
			})
		}
		if src.S3.Region != "" {
			extraEnv = append(extraEnv, corev1.EnvVar{Name: "AWS_REGION", Value: src.S3.Region})
		}
		envFromExtra = append(envFromExtra, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: src.S3.CredentialsSecret},
			},
		})

	case src.PVC != nil:
		// The restore PVC must already exist and contain the dump;
		// unlike spec.backup.profiles[].pvc the operator does not
		// provision a restore source. Reject the empty case with a
		// clear error so the misconfiguration is loud at reconcile
		// time, not at Job runtime.
		claim := src.PVC.ClaimName
		if claim == "" {
			return nil, fmt.Errorf(
				"initFromBackup.source.pvc.claimName is required; the restore source PVC must be created out of band and populated with the dump")
		}
		mountPath := "/restore"
		inputURL = mountPath
		if src.PVC.SubPath != "" {
			inputURL = mountPath + "/" + src.PVC.SubPath
		}
		extraVolumes = append(extraVolumes, corev1.Volume{
			Name: "restore-src",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim,
					ReadOnly:  true,
				},
			},
		})
		extraMounts = append(extraMounts, corev1.VolumeMount{
			Name: "restore-src", MountPath: mountPath, ReadOnly: true,
		})

	default:
		return nil, fmt.Errorf("initFromBackup.source must set mysqlBackupRef, s3, or pvc")
	}

	loadOptsJSON, err := marshalLoadOptions(fg.Spec.InitFromBackup.LoadOptions)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{
		labelAppName:       "mysql-restore",
		labelInstance:      fg.Name,
		labelFailoverGroup: fg.Name,
		labelSite:          targetSite,
		labelManagedBy:     managerName,
		labelResourceKind:  "restore",
	}

	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_MYSQL_HOST", Value: backupMySQLHost(fg, targetSite)},
		{Name: "BLOODRAVEN_INPUT_URL", Value: inputURL},
		{Name: "BLOODRAVEN_LOAD_OPTIONS", Value: loadOptsJSON},
	}
	if fg.Spec.TLS != nil {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_TLS", Value: "1"})
	}
	env = append(env, extraEnv...)

	envFrom := []corev1.EnvFromSource{
		{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: credsName}}},
	}
	envFrom = append(envFrom, envFromExtra...)

	// Shared scripts ConfigMap (same one created by reconcileBackupAssets).
	volumes := append([]corev1.Volume{
		{
			Name: "scripts",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: backupScriptsConfigMapName(fg.Name)},
				},
			},
		},
	}, extraVolumes...)

	mounts := append([]corev1.VolumeMount{
		{Name: "scripts", MountPath: backupScriptsMountPath, ReadOnly: true},
	}, extraMounts...)

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

	image := backupImage(fg)

	activeDeadline := int64(7200)
	backoff := int32(0)
	if fg.Spec.Backup != nil {
		if fg.Spec.Backup.ActiveDeadlineSeconds > 0 {
			activeDeadline = fg.Spec.Backup.ActiveDeadlineSeconds
		}
	}

	var pullSecrets []corev1.LocalObjectReference
	if fg.Spec.Backup != nil {
		pullSecrets = fg.Spec.Backup.ImagePullSecrets
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobName(fg.Name, targetSite),
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
					Containers: []corev1.Container{
						{
							Name:    backupJobContainerName,
							Image:   image,
							Command: []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/restore.py"},
							Env:     env,
							EnvFrom: envFrom,
							VolumeMounts: mounts,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}
	return job, nil
}

