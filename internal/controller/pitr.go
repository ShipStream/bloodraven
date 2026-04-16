package controller

import (
	"fmt"
	"path"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// PITR sidecar/pod fragments
// --------------------------
//
// When spec.backup.pitr.enabled=true, the operator must:
//
//  1. Mount the MySQL data PVC read-only into the sidecar so the
//     archiver can inotify-watch mysql-bin.index and read sealed
//     binlog files.
//  2. Expose the backup profile's storage (S3 creds secret or the
//     backup PVC) to the sidecar so uploads can land.
//  3. Pass a consistent set of BLOODRAVEN_PITR_* env vars so the
//     sidecar's pitrConfigFromEnv() builds the right archiver config.
//
// The helpers below return env-var / volume-mount / pod-volume
// fragments that the reconciler splices into the Deployment spec.
// They return empty fragments when PITR is disabled or misconfigured;
// the caller always appends unconditionally.

const (
	pitrBinlogDir        = "/var/lib/mysql"
	pitrBinlogIndex      = "mysql-bin.index"
	pitrPVCMountPath     = "/run/bloodraven/pitr-archive"
	pitrAWSCredsMountDir = "/run/bloodraven/pitr-aws-creds"
	// pitrBinlogSubprefix is the logical subtree under the backup
	// profile's storage root that holds binlog objects and manifests.
	// Giving it its own prefix keeps full dumps and binlog archives
	// cleanly separated in the same bucket / PVC.
	pitrBinlogSubprefix = "binlogs"
)

// pitrSidecarFragments is the bundle of additions the PITR feature
// contributes to the MySQL pod spec. Zero-value means "PITR is off";
// callers append unconditionally.
type pitrSidecarFragments struct {
	SidecarEnv          []corev1.EnvVar
	SidecarVolumeMounts []corev1.VolumeMount
	PodVolumes          []corev1.Volume
}

// buildPITRSidecarFragments returns the env/mount/volume additions
// the MySQL Deployment needs when PITR is enabled for this group.
//
// The function is intentionally non-fatal on misconfiguration — the
// reconcilerPITR validation path in backup_schedule.go already emits a
// BackupPITRInvalid event, and returning empty fragments here means
// the sidecar's PITRConfig ends up nil (archiver goroutine disabled)
// rather than making the Deployment creation error out. This matches
// the general reconciler preference of "don't block the happy path on
// a mis-set knob when the user can fix it without a pod roll".
func buildPITRSidecarFragments(fg *v1alpha1.MysqlFailoverGroup) (pitrSidecarFragments, error) {
	out := pitrSidecarFragments{}

	if fg.Spec.Backup == nil || fg.Spec.Backup.PITR == nil || !fg.Spec.Backup.PITR.Enabled {
		return out, nil
	}
	pitr := fg.Spec.Backup.PITR
	if pitr.ProfileName == "" {
		return out, nil
	}
	profile := findProfile(fg, pitr.ProfileName)
	if profile == nil {
		return out, nil
	}

	// Always mount the MySQL data PVC read-only into the sidecar. The
	// read-only flag prevents a rogue archiver from accidentally
	// corrupting the live binlog stream if a bug ever tried.
	out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
		Name:      "data",
		MountPath: pitrBinlogDir,
		ReadOnly:  true,
	})

	// Core archiver env vars shared by both storage backends.
	out.SidecarEnv = append(out.SidecarEnv,
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_ENABLED", Value: "1"},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_BINLOG_DIR", Value: pitrBinlogDir},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_BINLOG_INDEX", Value: pitrBinlogIndex},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: string(profile.Storage.Type)},
		// Profile name is echoed so the sidecar's retention client
		// can query the operator for the per-profile cutoff.
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_PROFILE_NAME", Value: profile.Name},
	)
	if pitr.ArchivePollInterval != nil {
		out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
			Name:  "BLOODRAVEN_PITR_POLL_INTERVAL",
			Value: pitr.ArchivePollInterval.Duration.String(),
		})
	}

	switch profile.Storage.Type {
	case v1alpha1.BackupStorageS3:
		if profile.Storage.S3 == nil {
			return out, fmt.Errorf("profile %q type=S3 but storage.s3 nil", profile.Name)
		}
		s3 := profile.Storage.S3
		// Manifest prefix = <profile prefix>/binlogs (or just
		// "binlogs" if no profile prefix). Kept separate from dumps
		// which live under <prefix>/<backupName>/.
		manifestPrefix := pitrBinlogSubprefix
		if s3.Prefix != "" {
			manifestPrefix = path.Join(s3.Prefix, pitrBinlogSubprefix)
		}
		out.SidecarEnv = append(out.SidecarEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: manifestPrefix},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_BUCKET", Value: s3.Bucket},
		)
		if s3.EndpointURL != "" {
			out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_ENDPOINT_URL", Value: s3.EndpointURL,
			})
		}
		if s3.Region != "" {
			out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_REGION", Value: s3.Region,
			})
		}
		if s3.CredentialsSecret != "" {
			// Mount the AWS creds Secret as files (mode 0400), matching
			// the backup Job's mount layout so credential handling is
			// uniform across backup, restore, and archiver paths.
			out.PodVolumes = append(out.PodVolumes, corev1.Volume{
				Name: "pitr-aws-creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  s3.CredentialsSecret,
						DefaultMode: ptr32(0o400),
					},
				},
			})
			out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
				Name:      "pitr-aws-creds",
				MountPath: pitrAWSCredsMountDir,
				ReadOnly:  true,
			})
			out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_AWS_CREDS_DIR", Value: pitrAWSCredsMountDir,
			})
		}

	case v1alpha1.BackupStoragePVC:
		if profile.Storage.PVC == nil {
			return out, fmt.Errorf("profile %q type=PVC but storage.pvc nil", profile.Name)
		}
		pvc := profile.Storage.PVC
		claim := pvc.ClaimName
		if claim == "" {
			claim = ownedBackupPVCName(fg.Name, profile.Name)
		}
		// For PVC-backed archival the manifest prefix is a subdirectory
		// inside the mount path. The restore flow and archiver both
		// resolve paths relative to the mount point using this prefix.
		manifestPrefix := pitrBinlogSubprefix
		if pvc.SubPath != "" {
			manifestPrefix = path.Join(pvc.SubPath, pitrBinlogSubprefix)
		}

		out.PodVolumes = append(out.PodVolumes, corev1.Volume{
			Name: "pitr-archive",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim,
				},
			},
		})
		out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
			Name:      "pitr-archive",
			MountPath: pitrPVCMountPath,
		})
		out.SidecarEnv = append(out.SidecarEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: manifestPrefix},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_PVC_MOUNT_PATH", Value: pitrPVCMountPath},
		)

	default:
		return out, fmt.Errorf("profile %q has unknown storage type %q", profile.Name, profile.Storage.Type)
	}

	return out, nil
}

// restorePITRPVCMountPath is the mount path inside the restore Job for
// the backup profile's PVC when replaying archived binlogs. Separate
// from pitrPVCMountPath on the live MySQL pod so the two don't share a
// name; cross-reference in the sidecar config when debugging.
const restorePITRPVCMountPath = "/restore-pitr"

// buildRestorePITRExtras returns the env/volume/mount additions the
// restore Job needs when spec.initFromBackup.pointInTime is set.
// Returns empty slices when PITR is not requested.
//
// PITR replay requires:
//   - The MysqlBackup CR being restored from (so we can pull
//     gtidExecuted into the replay env — the restore script uses it
//     to set gtid_purged on the target before replaying).
//   - The backup profile that held the binlog archive (its storage
//     config tells us where to fetch binlogs + manifest from).
//
// Both are resolved off fg.Spec.Backup.PITR because PITR archival is
// keyed off the failover group's own profile set, not off any
// properties of the specific MysqlBackup being restored. In practice
// the two tend to be the same profile (you archive and dump to the
// same bucket) but we decouple them here so future users can separate
// the two if they want.
func buildRestorePITRExtras(fg *v1alpha1.MysqlFailoverGroup) (
	[]corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error,
) {
	if fg.Spec.InitFromBackup == nil || fg.Spec.InitFromBackup.PointInTime == nil {
		return nil, nil, nil, nil
	}
	pit := fg.Spec.InitFromBackup.PointInTime
	if pit.StopDatetime == "" {
		return nil, nil, nil, fmt.Errorf("initFromBackup.pointInTime.stopDatetime is required")
	}
	if fg.Spec.Backup == nil || fg.Spec.Backup.PITR == nil || !fg.Spec.Backup.PITR.Enabled {
		return nil, nil, nil, fmt.Errorf(
			"initFromBackup.pointInTime is set but spec.backup.pitr.enabled=false; " +
				"PITR restore requires the failover group to have continuous binlog " +
				"archival configured on the source")
	}
	pitr := fg.Spec.Backup.PITR
	profile := findProfile(fg, pitr.ProfileName)
	if profile == nil {
		return nil, nil, nil, fmt.Errorf("pitr profile %q not found in spec.backup.profiles", pitr.ProfileName)
	}

	var (
		env     []corev1.EnvVar
		volumes []corev1.Volume
		mounts  []corev1.VolumeMount
	)

	env = append(env,
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: pit.StopDatetime},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: string(profile.Storage.Type)},
	)
	if pit.ExcludeGtids != "" {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_EXCLUDE_GTIDS", Value: pit.ExcludeGtids})
	}

	switch profile.Storage.Type {
	case v1alpha1.BackupStorageS3:
		if profile.Storage.S3 == nil {
			return nil, nil, nil, fmt.Errorf("pitr profile %q type=S3 but storage.s3 nil", profile.Name)
		}
		s3 := profile.Storage.S3
		manifestPrefix := pitrBinlogSubprefix
		if s3.Prefix != "" {
			manifestPrefix = path.Join(s3.Prefix, pitrBinlogSubprefix)
		}
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: manifestPrefix},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_BUCKET", Value: s3.Bucket},
		)
		if s3.EndpointURL != "" {
			env = append(env, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_ENDPOINT_URL", Value: s3.EndpointURL,
			})
		}
		if s3.Region != "" {
			env = append(env, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_REGION", Value: s3.Region,
			})
		}
		// AWS creds are already mounted at backupAWSCredsMountPath by
		// the main restore-job path when source is S3. Re-point the
		// PITR helper at that same directory rather than double-mount.
		env = append(env, corev1.EnvVar{
			Name: "BLOODRAVEN_PITR_AWS_CREDS_DIR", Value: backupAWSCredsMountPath,
		})

	case v1alpha1.BackupStoragePVC:
		if profile.Storage.PVC == nil {
			return nil, nil, nil, fmt.Errorf("pitr profile %q type=PVC but storage.pvc nil", profile.Name)
		}
		pvc := profile.Storage.PVC
		claim := pvc.ClaimName
		if claim == "" {
			claim = ownedBackupPVCName(fg.Name, profile.Name)
		}
		manifestPrefix := pitrBinlogSubprefix
		if pvc.SubPath != "" {
			manifestPrefix = path.Join(pvc.SubPath, pitrBinlogSubprefix)
		}
		volumes = append(volumes, corev1.Volume{
			Name: "pitr-archive",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim,
					ReadOnly:  true,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "pitr-archive",
			MountPath: restorePITRPVCMountPath,
			ReadOnly:  true,
		})
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: manifestPrefix},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_PVC_MOUNT_PATH", Value: restorePITRPVCMountPath},
		)

	default:
		return nil, nil, nil, fmt.Errorf("pitr profile %q has unknown storage type %q",
			profile.Name, profile.Storage.Type)
	}

	return env, volumes, mounts, nil
}
