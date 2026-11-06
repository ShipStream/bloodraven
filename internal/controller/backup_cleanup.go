package controller

import (
	"errors"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
)

// ConditionArtifactCleanup reports why a deleted MysqlBackup's artifact has
// not been removed yet. It is only written while the CR is Terminating.
const ConditionArtifactCleanup = "ArtifactCleanup"

// ArtifactCleanup condition reasons.
const (
	cleanupReasonFailed                = "CleanupFailed"
	cleanupReasonRejected              = "CleanupRejected"
	cleanupReasonDestinationUnverified = "DestinationUnverified"
	cleanupReasonDestinationMismatch   = "DestinationMismatch"
	cleanupReasonCredentialsMissing    = "CredentialsUnavailable"
	cleanupReasonOperatorImageUnset    = "OperatorImageUnset"
	cleanupReasonJobConflict           = "CleanupJobConflict"
	cleanupReasonHeldByVerification    = "HeldByVerification"
	cleanupReasonPending               = "CleanupPending"
	cleanupReasonForegroundDeletion    = "ForegroundDeletion"
	cleanupReasonOrphanDeletion        = "OrphanDeletion"
	cleanupReasonJobInvalid            = "CleanupJobInvalid"
)

const (
	// cleanupContainerName is the cleanup Job's only container.
	cleanupContainerName = "artifact-cleanup"
	// annotationCleanupFailureRecorded marks a failed cleanup Job whose
	// failure event and metric have been emitted, so each Job counts once.
	annotationCleanupFailureRecorded = "shipstream.io/cleanup-failure-recorded"
	// cleanupRetryAfterFailure and cleanupRetryAfterRejection delay a fresh
	// cleanup Job after a failed one, measured from the Job's failure time.
	// Retries never stop: credentials rotated inside the same Secret, IAM
	// fixes and transient outages all clear without operator action.
	cleanupRetryAfterFailure   = 5 * time.Minute
	cleanupRetryAfterRejection = 30 * time.Minute
)

var (
	errOperatorImageUnset          = errors.New("operator image is not configured (SetOperatorImageDefaults); artifact cleanup runs it")
	errCleanupCredentialsMissing   = errors.New("profile has no S3 credentialsSecret for the artifact's storage")
	errCleanupDestinationNotStored = errors.New("artifact has no recorded storage destination")
)

// cleanupJobInputs bundles the resolved inputs needed to render the
// artifact-cleanup Job attached to a MysqlBackup's finalizer.
type cleanupJobInputs struct {
	FailoverGroup *v1alpha1.MysqlFailoverGroup
	Profile       *v1alpha1.BackupProfile
	Backup        *v1alpha1.MysqlBackup
	// Storage is the immutable destination; nil falls back to
	// Backup.Status.ArtifactStorage. The live profile only supplies
	// credentials.
	Storage *v1alpha1.BackupArtifactStorage
	// Abandoned cleans up after a backup that never succeeded: the
	// location is the planned one (Storage.Location), and the Job deletes
	// only when the backup stamp there names Backup.UID.
	Abandoned bool
	// Passphrase mounts the encrypted backup's passphrase so an abandoned
	// cleanup can read the BRV1 stamp. An empty SecretName refuses.
	Passphrase *corev1.SecretVolumeSource
}

// buildCleanupJob renders the one-shot Job that runs `bloodraven
// artifact-cleanup` from the operator image against the backup's recorded
// destination. It mounts storage credentials only: cleanup never talks to
// MySQL, so the group's MySQL credentials are not a prerequisite.
func buildCleanupJob(in cleanupJobInputs) (*batchv1.Job, error) {
	fg := in.FailoverGroup
	backup := in.Backup
	profile := in.Profile

	if backup == nil {
		return nil, fmt.Errorf("cleanup job: no backup")
	}
	storage := in.Storage
	if storage == nil {
		storage = backup.Status.ArtifactStorage
	}
	location := backup.Status.Location
	if in.Abandoned {
		if storage == nil || storage.Location == "" {
			return nil, fmt.Errorf("cleanup job: %w: no planned location", errCleanupDestinationNotStored)
		}
		if backup.UID == "" {
			return nil, fmt.Errorf("cleanup job: backup %s has no UID", backup.Name)
		}
		if in.Passphrase != nil && in.Passphrase.SecretName == "" {
			return nil, fmt.Errorf("cleanup job: encrypted backup %s has no passphrase Secret to read its backup stamp", backup.Name)
		}
		location = storage.Location
	}
	if location == "" {
		return nil, fmt.Errorf("cleanup job: backup has no status.location")
	}
	if storage == nil {
		return nil, fmt.Errorf("cleanup job: %w", errCleanupDestinationNotStored)
	}
	if operatorImageFromEnv == "" {
		return nil, fmt.Errorf("cleanup job: %w", errOperatorImageUnset)
	}

	storageType := backup.Status.StorageType
	if storageType == "" && profile != nil {
		storageType = profile.Storage.Type
	}
	if storageType == "" {
		return nil, fmt.Errorf("cleanup job: cannot determine storage type for backup %s", backup.Name)
	}

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

	// Recorded values are already resolved paths; escape them so kubelet's
	// $(VAR) expansion cannot alter them.
	envVars := []corev1.EnvVar{
		{Name: "BLOODRAVEN_BACKUP_NAME", Value: backup.Name},
		{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(storageType)},
		{Name: "BLOODRAVEN_OUTPUT_URL", Value: literalArtifactEnvValue(location)},
	}
	var (
		volumes      []corev1.Volume
		volumeMounts []corev1.VolumeMount
	)
	if in.Abandoned {
		envVars = append(envVars,
			corev1.EnvVar{Name: "BLOODRAVEN_CLEANUP_MODE", Value: "abandoned"},
			corev1.EnvVar{Name: "BLOODRAVEN_BACKUP_UID", Value: literalArtifactEnvValue(string(backup.UID))},
		)
		if in.Passphrase != nil {
			envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_BACKUP_PASSPHRASE_FILE", Value: backupPassphraseMountPath + "/" + backupPassphraseFileName})
			volumes = append(volumes, corev1.Volume{Name: "backup-passphrase", VolumeSource: corev1.VolumeSource{Secret: in.Passphrase.DeepCopy()}})
			volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "backup-passphrase", MountPath: backupPassphraseMountPath, ReadOnly: true})
		}
	}

	switch storageType {
	case v1alpha1.BackupStorageS3:
		if storage.Bucket == "" {
			return nil, fmt.Errorf("cleanup job: %w: no bucket", errCleanupDestinationNotStored)
		}
		if profile == nil || profile.Storage.S3 == nil || profile.Storage.S3.CredentialsSecret == "" {
			return nil, fmt.Errorf("cleanup job: %w", errCleanupCredentialsMissing)
		}
		envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: literalArtifactEnvValue(storage.Bucket)})
		if storage.EndpointURL != "" {
			envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: literalArtifactEnvValue(storage.EndpointURL)})
		}
		if storage.Region != "" {
			envVars = append(envVars, corev1.EnvVar{Name: "AWS_REGION", Value: literalArtifactEnvValue(storage.Region)})
		}
		envVars = append(envVars, corev1.EnvVar{Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath})
		volumes = append(volumes, corev1.Volume{
			Name: "aws-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  profile.Storage.S3.CredentialsSecret,
					DefaultMode: ptr32(0o400),
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "aws-creds",
			MountPath: backupAWSCredsMountPath,
			ReadOnly:  true,
		})

	case v1alpha1.BackupStoragePVC:
		if storage.ClaimName == "" {
			return nil, fmt.Errorf("cleanup job: %w: no claim", errCleanupDestinationNotStored)
		}
		// Mount the whole claim (no SubPath) so the full recorded path
		// (e.g. /backups/<subPath>/<name>) resolves inside the container.
		volumes = append(volumes, corev1.Volume{
			Name: "backups",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: storage.ClaimName,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "backups",
			MountPath: backupPVCMountPath,
		})
		envVars = append(envVars,
			corev1.EnvVar{Name: "BLOODRAVEN_PVC_MOUNT_PATH", Value: backupPVCMountPath},
		)

	default:
		return nil, fmt.Errorf("cleanup job: unknown storage type %q", storageType)
	}

	volumes = append(volumes, corev1.Volume{
		Name:         "tmp",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "tmp", MountPath: tmpMountPath})

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
	containerName := cleanupContainerName

	var pullSecrets []corev1.LocalObjectReference
	if bspec != nil {
		pullSecrets = bspec.ImagePullSecrets
	}

	// Resources flow through from spec.backup.resources when set. Falls
	// back to defaultRestoreResources() (100m/128Mi) when the failover
	// group has no spec.backup block, so the Job is admitted on
	// LimitRange-gated clusters even on a fresh deploy.
	var cleanupResources corev1.ResourceRequirements
	if bspec != nil {
		cleanupResources = effectiveBackupResources(&backupResourcesSource{Resources: bspec.Resources}, defaultRestoreResources())
	} else {
		cleanupResources = defaultRestoreResources()
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
			// A refusal (CleanupExitRejected) will not succeed on an in-Job retry;
			// the finalizer retries it later with a fresh Job.
			PodFailurePolicy: &batchv1.PodFailurePolicy{Rules: []batchv1.PodFailurePolicyRule{{
				Action: batchv1.PodFailurePolicyActionFailJob,
				OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
					ContainerName: &containerName,
					Operator:      batchv1.PodFailurePolicyOnExitCodesOpIn,
					Values:        []int32{backupmeta.CleanupExitRejected},
				},
			}}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: pullSecrets,
					SecurityContext:  podSC,
					// Cleanup only talks to storage, never to the
					// Kubernetes API.
					AutomountServiceAccountToken: boolPtr(false),
					Tolerations:                  groupReadOnlyTolerations(fg.Name),
					Containers: []corev1.Container{
						{
							Name:                     cleanupContainerName,
							Image:                    operatorImageFromEnv,
							Command:                  operatorCommand("artifact-cleanup"),
							Env:                      envVars,
							Resources:                cleanupResources,
							VolumeMounts:             volumeMounts,
							SecurityContext:          containerSC,
							TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}
	return job, nil
}

// artifactStorageFromJob reads the storage destination the producing backup
// Job actually used, with kubelet's env expansion applied. It is the single
// source for status.artifactStorage, both at Job creation and when an
// older backup without a snapshot is deleted.
func artifactStorageFromJob(job *batchv1.Job) (*v1alpha1.BackupArtifactStorage, error) {
	c := backupJobMainContainer(job)
	if c == nil {
		return nil, fmt.Errorf("backup Job has no dump or uploader container")
	}
	env := make(map[string]backupEnvironmentValue)
	for _, v := range c.Env {
		entry := backupEnvironmentValue{known: false}
		if v.ValueFrom == nil {
			entry = expandBackupEnvironment(v.Value, env)
		}
		env[v.Name] = entry
	}
	value := func(name string, required bool) (string, error) {
		entry, ok := env[name]
		if !ok {
			if required {
				return "", fmt.Errorf("backup Job does not set %s", name)
			}
			return "", nil
		}
		if !entry.known {
			return "", fmt.Errorf("backup Job %s depends on unavailable runtime environment", name)
		}
		return entry.value, nil
	}
	_, location, err := backupJobDestination(job)
	if err != nil {
		return nil, fmt.Errorf("artifact location: %w", err)
	}
	storage := &v1alpha1.BackupArtifactStorage{Location: location}
	switch v1alpha1.BackupStorageType(env["BLOODRAVEN_STORAGE_TYPE"].value) {
	case v1alpha1.BackupStorageS3:
		var err error
		if storage.Bucket, err = value("BLOODRAVEN_S3_BUCKET", true); err != nil {
			return nil, err
		}
		if storage.Bucket == "" {
			return nil, fmt.Errorf("backup Job has an empty BLOODRAVEN_S3_BUCKET")
		}
		if storage.EndpointURL, err = value("BLOODRAVEN_S3_ENDPOINT_OVERRIDE", false); err != nil {
			return nil, err
		}
		if storage.Region, err = value("AWS_REGION", false); err != nil {
			return nil, err
		}
	case v1alpha1.BackupStoragePVC:
		for _, vol := range job.Spec.Template.Spec.Volumes {
			if vol.Name == "backups" && vol.PersistentVolumeClaim != nil {
				storage.ClaimName = vol.PersistentVolumeClaim.ClaimName
			}
		}
		if storage.ClaimName == "" {
			return nil, fmt.Errorf("backup Job has no backups PersistentVolumeClaim volume")
		}
	default:
		return nil, fmt.Errorf("backup Job has unknown storage type %q", env["BLOODRAVEN_STORAGE_TYPE"].value)
	}
	return storage, nil
}

// artifactIdentityProblem returns a non-empty reason when location (the
// backup's status.location, or for an abandoned cleanup its planned
// location) is not exactly the location the producing Job wrote, as recorded in
// status.artifactStorage. Name, bucket and mount checks are kept as a second
// line of defence. producer is the backup's own Job, or nil when it is gone.
func artifactIdentityProblem(backup *v1alpha1.MysqlBackup, location string, storage *v1alpha1.BackupArtifactStorage, producer *batchv1.Job) string {
	if storage.Location == "" {
		return "no recorded artifact location"
	}
	if location != storage.Location {
		return fmt.Sprintf("location %q differs from the recorded artifact location %q", location, storage.Location)
	}
	if name := backupmeta.CleanupArtifactName(location); name != backup.Name {
		return fmt.Sprintf("location %q does not end in the backup name %q", location, backup.Name)
	}
	switch backup.Status.StorageType {
	case v1alpha1.BackupStorageS3:
		if bucket, _, inBucket := backupmeta.SplitS3Location(location); inBucket && bucket != storage.Bucket {
			return fmt.Sprintf("location bucket %q differs from the recorded bucket %q", bucket, storage.Bucket)
		}
	case v1alpha1.BackupStoragePVC:
		if !strings.HasPrefix(location, backupPVCMountPath+"/") {
			return fmt.Sprintf("location %q is not under %s", location, backupPVCMountPath)
		}
	}
	if producer != nil {
		if _, expected, err := backupJobDestination(producer); err == nil && expected != location {
			return fmt.Sprintf("location %q differs from the producing Job's destination %q", location, expected)
		}
	}
	return ""
}

// cleanupPodWaiting returns why the cleanup container has not started, for
// reasons that need attention (image pull, configuration), or "".
func cleanupPodWaiting(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	for _, status := range pod.Status.ContainerStatuses {
		w := status.State.Waiting
		if w == nil || w.Reason == "" || w.Reason == "ContainerCreating" || w.Reason == "PodInitializing" {
			continue
		}
		if w.Message != "" {
			return fmt.Sprintf("container %s is waiting: %s: %s", status.Name, w.Reason, w.Message)
		}
		return fmt.Sprintf("container %s is waiting: %s", status.Name, w.Reason)
	}
	return ""
}

// cleanupStartGrace is how long a pod may sit unscheduled or creating its
// container (pulling the image, mounting volumes) before it is reported.
const cleanupStartGrace = time.Minute

// cleanupPodStuck explains a cleanup pod that cannot start: a waiting
// reason that needs attention, or a pod still unscheduled or creating its
// container after cleanupStartGrace (a missing Secret volume, an unbound
// claim, or a ReadWriteOnce claim attached on another node). "" otherwise.
func cleanupPodStuck(pod *corev1.Pod, now time.Time) string {
	if waiting := cleanupPodWaiting(pod); waiting != "" || pod == nil {
		return waiting
	}
	created := pod.CreationTimestamp
	if pod.Status.Phase != corev1.PodPending || created.IsZero() || now.Sub(created.Time) < cleanupStartGrace {
		return ""
	}
	// The text names the pod's creation time, not the elapsed time, so the
	// condition does not change (and is not rewritten) on every pass.
	since := created.UTC().Format(time.RFC3339)
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
			return fmt.Sprintf("pod %s not scheduled since %s: %s: %s", pod.Name, since, c.Reason, c.Message)
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if w := status.State.Waiting; w != nil {
			return fmt.Sprintf("container %s still %s since %s; check the pod's FailedMount/FailedAttachVolume events (kubectl describe pod %s)",
				status.Name, w.Reason, since, pod.Name)
		}
	}
	return fmt.Sprintf("pod %s Pending since %s; check its events (kubectl describe pod %s)", pod.Name, since, pod.Name)
}

// cleanupPodVerdict returns the exit code and termination message of the
// cleanup container. terminated is false when the pod has not recorded one.
func cleanupPodVerdict(pod *corev1.Pod) (exitCode int32, message string, terminated bool) {
	if pod == nil {
		return 0, "", false
	}
	for _, status := range pod.Status.ContainerStatuses {
		term := status.State.Terminated
		if term == nil {
			term = status.LastTerminationState.Terminated
		}
		if term == nil {
			continue
		}
		return term.ExitCode, strings.TrimSpace(term.Message), true
	}
	return 0, "", false
}
