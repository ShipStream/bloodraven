package controller

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// pitrDumpSource is where a PITR restore's or verification's dump lives and
// what the dump-meta init container must find in its stamp (§4.2).
type pitrDumpSource struct {
	// StorageType is S3 or PVC.
	StorageType v1alpha1.BackupStorageType
	// Prefix is the dump location relative to the store: the S3 key prefix
	// (no bucket, no trailing slash) or the path relative to the claim's
	// root. The stamp is backupmeta.StampKey(Prefix).
	Prefix string

	S3Bucket, S3EndpointURL, S3Region, AWSCredsSecret string
	PVCClaim                                          string

	// Passphrase is the dump key; nil for a plaintext dump.
	Passphrase *v1alpha1.PassphraseSecretRef

	// Expected identity, from the referenced MysqlBackup. All empty for a
	// direct S3 or PVC source, whose stamp is still required.
	ExpectedBackupUID    types.UID
	ExpectedSourceUID    types.UID
	ExpectedGtidExecuted string
}

// pitrDumpSourceForBackup describes a MysqlBackup's dump for dump-meta.
// profile must store to the backup's backend (callers check this first).
func pitrDumpSourceForBackup(fg *v1alpha1.MysqlFailoverGroup, backup *v1alpha1.MysqlBackup, profile *v1alpha1.BackupProfile, passphrase *v1alpha1.PassphraseSecretRef) (pitrDumpSource, error) {
	src := pitrDumpSource{
		StorageType:          backup.Status.StorageType,
		Passphrase:           passphrase,
		ExpectedBackupUID:    backup.UID,
		ExpectedSourceUID:    backup.Status.FailoverGroupUID,
		ExpectedGtidExecuted: backup.Status.GtidExecuted,
	}
	if src.StorageType == "" {
		src.StorageType = v1alpha1.BackupStoragePVC
		if isS3Location(backup.Status.Location) {
			src.StorageType = v1alpha1.BackupStorageS3
		}
	}
	switch src.StorageType {
	case v1alpha1.BackupStorageS3:
		if profile == nil || profile.Storage.S3 == nil {
			return src, fmt.Errorf("backup %q is on S3 but its profile has no s3 storage", backup.Name)
		}
		s3 := profile.Storage.S3
		bucket, prefix := splitS3Location(backup.Status.Location)
		if bucket == "" {
			bucket = s3.Bucket
		}
		src.S3Bucket, src.S3EndpointURL, src.S3Region, src.AWSCredsSecret = bucket, s3.EndpointURL, s3.Region, s3.CredentialsSecret
		src.Prefix = strings.Trim(prefix, "/")
	case v1alpha1.BackupStoragePVC:
		if profile == nil || profile.Storage.PVC == nil {
			return src, fmt.Errorf("backup %q is on a PVC but its profile has no pvc storage", backup.Name)
		}
		src.PVCClaim = profile.Storage.PVC.ClaimName
		if src.PVCClaim == "" {
			src.PVCClaim = ownedBackupPVCName(fg.Name, profile.Name)
		}
		// status.location is the in-pod path the backup Job wrote under
		// backupPVCMountPath; the stamp is relative to the claim's root.
		src.Prefix = strings.Trim(strings.TrimPrefix(backup.Status.Location, backupPVCMountPath), "/")
	default:
		return src, fmt.Errorf("backup %q has unknown storage type %q", backup.Name, src.StorageType)
	}
	if src.Prefix == "" {
		return src, fmt.Errorf("backup %q has no dump location", backup.Name)
	}
	return src, nil
}

// buildDumpMetaInitContainer renders the dump-meta init container and the
// volumes only it uses. It reads <prefix>/@.json, <prefix>/@.done.json and
// the stamp, checks them against the expected identity and writes
// /pitr-dump-meta/dump.json (the pitr-dump-meta volume that
// buildRestorePITRFragmentsFor adds). Its volumes are its own, so the
// caller's dump-loading wiring can change without affecting it.
func buildDumpMetaInitContainer(fg *v1alpha1.MysqlFailoverGroup, src pitrDumpSource) (corev1.Container, []corev1.Volume, error) {
	if src.Prefix == "" {
		return corev1.Container{}, nil, fmt.Errorf("point-in-time replay: the dump has no location")
	}
	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_DUMP_META_OUTPUT_DIR", Value: restorePITRDumpMetaDir},
		{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(src.StorageType)},
		{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: literalArtifactEnvValue(src.Prefix)},
	}
	mounts := []corev1.VolumeMount{
		{Name: "pitr-dump-meta", MountPath: restorePITRDumpMetaDir},
		{Name: "tmp", MountPath: tmpMountPath},
	}
	var volumes []corev1.Volume
	switch src.StorageType {
	case v1alpha1.BackupStorageS3:
		if src.S3Bucket == "" {
			return corev1.Container{}, nil, fmt.Errorf("point-in-time replay: the dump's S3 bucket is unknown")
		}
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: src.S3Bucket})
		if src.S3EndpointURL != "" {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: src.S3EndpointURL})
		}
		if src.S3Region != "" {
			env = append(env, corev1.EnvVar{Name: "AWS_REGION", Value: src.S3Region})
		}
		if src.AWSCredsSecret != "" {
			volumes = append(volumes, corev1.Volume{
				Name: "pitr-dump-aws-creds",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: src.AWSCredsSecret, DefaultMode: ptr32(0o400),
				}},
			})
			mounts = append(mounts, corev1.VolumeMount{Name: "pitr-dump-aws-creds", MountPath: backupAWSCredsMountPath, ReadOnly: true})
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath})
		}
	case v1alpha1.BackupStoragePVC:
		if src.PVCClaim == "" {
			return corev1.Container{}, nil, fmt.Errorf("point-in-time replay: the dump's claim is unknown")
		}
		volumes = append(volumes, corev1.Volume{
			Name: "pitr-dump-src",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: src.PVCClaim, ReadOnly: true,
			}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "pitr-dump-src", MountPath: restorePITRDumpSourceMountPath, ReadOnly: true})
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PVC_MOUNT_PATH", Value: restorePITRDumpSourceMountPath})
	default:
		return corev1.Container{}, nil, fmt.Errorf("point-in-time replay: unknown dump storage type %q", src.StorageType)
	}
	if src.Passphrase != nil {
		volumes = append(volumes, corev1.Volume{
			Name: "pitr-dump-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  src.Passphrase.Name,
				DefaultMode: ptr32(0o400),
				Items:       []corev1.KeyToPath{{Key: src.Passphrase.PassphraseSecretKeyOrDefault(), Path: backupPassphraseFileName}},
			}},
		})
		const keyDir = "/run/bloodraven/pitr-dump-key"
		mounts = append(mounts, corev1.VolumeMount{Name: "pitr-dump-key", MountPath: keyDir, ReadOnly: true})
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_BACKUP_PASSPHRASE_FILE", Value: keyDir + "/" + backupPassphraseFileName})
	}
	if src.ExpectedBackupUID != "" {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_DUMP_EXPECTED_BACKUP_UID", Value: string(src.ExpectedBackupUID)})
	}
	if src.ExpectedSourceUID != "" {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_DUMP_EXPECTED_SOURCE_UID", Value: string(src.ExpectedSourceUID)})
	}
	if src.ExpectedGtidExecuted != "" {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_DUMP_EXPECTED_GTID_EXECUTED", Value: src.ExpectedGtidExecuted})
	}

	_, sc := mergeSecurityContexts(nil, nil)
	var resourcesSrc *backupResourcesSource
	if fg.Spec.Backup != nil {
		resourcesSrc = &backupResourcesSource{Resources: fg.Spec.Backup.Resources}
	}
	return corev1.Container{
		Name:                     restorePITRDumpMetaContainerName,
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		Image:                    pitrOperatorImage(),
		Command:                  operatorCommand("dump-meta"),
		Env:                      env,
		Resources:                effectiveBackupResources(resourcesSrc, defaultInitContainerResources()),
		VolumeMounts:             mounts,
		SecurityContext:          sc,
	}, volumes, nil
}

// pitrLoadFilterRefusal refuses schema-filtered loads with point-in-time
// replay (D-45): the replay applies every archived transaction, including
// those on the skipped schemas, and R6 could not certify the result.
func pitrLoadFilterRefusal(fp string, opts *v1alpha1.LoadOptions) error {
	if opts == nil || (len(opts.IncludeSchemas) == 0 && len(opts.ExcludeSchemas) == 0) {
		return nil
	}
	return fmt.Errorf("%s: %s.loadOptions includeSchemas/excludeSchemas cannot be combined with %s.pointInTime: point-in-time replay needs the full dump loaded; remove the schema filters",
		pitrarchive.TokenBackupFiltered, fp, fp)
}

// pitrBackupBefore is the controller's early feedback for D-44 (dump-meta
// enforces the exact bound): a target before the backup finished can
// already be inside the dump. completionTime is a whole second (F27), so
// the bound is rounded up.
func pitrBackupBefore(req *pitrReplayRequest, backup v1alpha1.MysqlBackupStatus) error {
	if req == nil || req.Mode != "timestamp" {
		return nil
	}
	stop, err := backupmeta.ParseMicroTime(req.Stop)
	if err != nil {
		return fmt.Errorf("%s: %v", pitrarchive.TokenTargetInvalid, err)
	}
	if backup.CompletionTime == nil {
		return fmt.Errorf("%s: the backup has no completionTime, so it cannot be shown to finish before the target %s", pitrarchive.TokenBackupAfterTarget, req.Stop)
	}
	completed := backup.CompletionTime.UTC()
	if rounded := completed.Truncate(time.Second); !rounded.Equal(completed) {
		completed = rounded.Add(time.Second)
	}
	if stop.Before(completed) {
		return fmt.Errorf("%s: the backup finished at %s, after the target %s, so the dump may already contain later changes; use a backup that completed before the target",
			pitrarchive.TokenBackupAfterTarget, completed.Format("2006-01-02T15:04:05Z"), req.Stop)
	}
	return nil
}
