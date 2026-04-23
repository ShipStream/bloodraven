package controller

import (
	"fmt"
	"path"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Defaults applied when the corresponding PITRSpec fields are unset.
// Kept in one place so generateMyCnf, buildPITRSidecarFragments, and
// computeSpecHash all agree on the effective value — otherwise a
// change here would silently fail to roll pods.
const (
	defaultPITRMaxBinlogSize       = "100M"
	defaultPITRArchivePollInterval = 60 * time.Second
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

	// Encryption wiring. When the profile has encryption enabled we
	// mount the passphrase Secret onto the sidecar container and
	// point the archiver at the file; everything else (wrapper
	// selection, re-upload / prune semantics) falls out naturally
	// from the encryptedStore that sidecar.newArchiveStore chooses.
	if profile.EncryptionEnabled() {
		ref := profile.Encryption.PassphraseSecret
		out.PodVolumes = append(out.PodVolumes, corev1.Volume{
			Name: "pitr-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  ref.Name,
					DefaultMode: ptr32(0o400),
					Items: []corev1.KeyToPath{
						{
							Key:  ref.PassphraseSecretKeyOrDefault(),
							Path: backupPassphraseFileName,
						},
					},
				},
			},
		})
		out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
			Name:      "pitr-passphrase",
			MountPath: backupPassphraseMountPath,
			ReadOnly:  true,
		})
		out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
			Name:  "BLOODRAVEN_PITR_PASSPHRASE_FILE",
			Value: backupPassphraseMountPath + "/" + backupPassphraseFileName,
		})
	}

	return out, nil
}

// restorePITRPVCMountPath is the mount path inside the restore Job's
// init container for the backup profile's PVC when replaying archived
// binlogs. Separate from pitrPVCMountPath on the live MySQL pod so the
// two don't share a name; cross-reference in the sidecar config when
// debugging.
const restorePITRPVCMountPath = "/restore-pitr"

// restorePITRLocalDir is the shared emptyDir path where the init
// container drops downloaded binlog files and the mysqlsh container
// subsequently reads them. Path is shared across both containers;
// read-write in the init container, read-only in the main container.
const restorePITRLocalDir = "/pitr-binlogs"

// restorePITRInitContainerName is the name given to the
// bloodraven pitr-download init container. Named explicitly so it's
// easy to spot in kubectl describe output when diagnosing restore
// failures.
const restorePITRInitContainerName = "pitr-download"

// restorePITRFragments is the bundle of additions PITR contributes to
// a restore Job spec. Zero value means "PITR replay not requested";
// callers append unconditionally.
type restorePITRFragments struct {
	// MainEnv is the set of env vars the mysqlsh container needs to
	// know where to find the downloaded binlogs and what datetime to
	// stop at. Kept deliberately small — the init container does the
	// storage I/O, the main container just runs replay.
	MainEnv []corev1.EnvVar
	// MainMounts is the read-only mount that exposes the shared
	// emptyDir to the mysqlsh container.
	MainMounts []corev1.VolumeMount
	// InitContainer is the bloodraven pitr-download container that
	// runs before the mysqlsh container.
	InitContainer *corev1.Container
	// PodVolumes is the set of pod-level volumes that back the init
	// container and the read-only handoff to the main container.
	PodVolumes []corev1.Volume
}

// buildRestorePITRFragments wires up the restore-side PITR handoff:
//
//	initContainer: bloodraven pitr-download
//	   ├── downloads archived binlogs from S3 or PVC
//	   └── writes to emptyDir /pitr-binlogs
//	container: mysqlsh
//	   ├── util.loadDump()  (existing restore.py behavior)
//	   └── replays /pitr-binlogs/<site>/* via mysqlbinlog | mysql
//
// Splitting download + replay across two containers removes the aws
// CLI runtime dependency from the MySQL image and reuses the sidecar
// archiver's storage-backend abstraction.
//
// PITR replay is configured purely from the failover group's archive
// settings: the backup profile named by fg.Spec.Backup.PITR tells the
// init container where to fetch archived binlogs and manifests from.
// util.loadDump() sets @@GLOBAL.gtid_purged from the dump's recorded
// state, and mysqlbinlog's server-side GTID dedup then skips
// already-applied transactions during replay — no per-MysqlBackup
// metadata plumbing needed here.
func buildRestorePITRFragments(fg *v1alpha1.MysqlFailoverGroup) (restorePITRFragments, error) {
	if fg.Spec.InitFromBackup == nil {
		return restorePITRFragments{}, nil
	}
	return buildRestorePITRFragmentsFor(fg, fg.Spec.InitFromBackup.PointInTime)
}

// buildRestorePITRFragmentsFor is the parameterized variant that lets
// both the bootstrap restore (spec.initFromBackup.pointInTime) and the
// in-place restore (spec.restoreInPlace.pointInTime) share the PITR
// init-container wiring. A nil pit is a no-op: callers get an empty
// fragments struct and skip the init container entirely.
func buildRestorePITRFragmentsFor(fg *v1alpha1.MysqlFailoverGroup, pit *v1alpha1.PointInTimeSpec) (restorePITRFragments, error) {
	var out restorePITRFragments

	if pit == nil {
		return out, nil
	}
	if pit.StopDatetime == "" {
		return out, fmt.Errorf("pointInTime.stopDatetime is required")
	}
	if fg.Spec.Backup == nil || fg.Spec.Backup.PITR == nil || !fg.Spec.Backup.PITR.Enabled {
		return out, fmt.Errorf(
			"pointInTime is set but spec.backup.pitr.enabled=false; " +
				"PITR restore requires the failover group to have continuous binlog " +
				"archival configured on the source")
	}
	pitr := fg.Spec.Backup.PITR
	profile := findProfile(fg, pitr.ProfileName)
	if profile == nil {
		return out, fmt.Errorf("pitr profile %q not found in spec.backup.profiles", pitr.ProfileName)
	}

	// Shared emptyDir volume. Init container writes; main container
	// reads. sizeLimit is intentionally unset — the node's ephemeral
	// storage limit is the de-facto cap, matching how the existing
	// /tmp emptyDir is sized.
	emptyDirVol := corev1.Volume{
		Name:         "pitr-binlogs",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
	initMount := corev1.VolumeMount{Name: "pitr-binlogs", MountPath: restorePITRLocalDir}
	mainMount := corev1.VolumeMount{Name: "pitr-binlogs", MountPath: restorePITRLocalDir, ReadOnly: true}

	// Build a comma-separated site list from the spec so the init
	// container can skip a List call in the common case. The init
	// container falls back to listing the manifest prefix when this
	// env var is empty.
	var siteNames []string
	for _, s := range fg.Spec.Sites {
		siteNames = append(siteNames, s.Name)
	}

	initEnv := []corev1.EnvVar{
		{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: pit.StopDatetime},
		{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: string(profile.Storage.Type)},
		{Name: "BLOODRAVEN_PITR_OUTPUT_DIR", Value: restorePITRLocalDir},
		{Name: "BLOODRAVEN_PITR_SITES", Value: joinComma(siteNames)},
	}
	initMounts := []corev1.VolumeMount{initMount}
	volumes := []corev1.Volume{emptyDirVol}

	switch profile.Storage.Type {
	case v1alpha1.BackupStorageS3:
		if profile.Storage.S3 == nil {
			return out, fmt.Errorf("pitr profile %q type=S3 but storage.s3 nil", profile.Name)
		}
		s3 := profile.Storage.S3
		manifestPrefix := pitrBinlogSubprefix
		if s3.Prefix != "" {
			manifestPrefix = path.Join(s3.Prefix, pitrBinlogSubprefix)
		}
		initEnv = append(initEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: manifestPrefix},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_BUCKET", Value: s3.Bucket},
		)
		if s3.EndpointURL != "" {
			initEnv = append(initEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_ENDPOINT_URL", Value: s3.EndpointURL,
			})
		}
		if s3.Region != "" {
			initEnv = append(initEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_REGION", Value: s3.Region,
			})
		}
		if s3.CredentialsSecret != "" {
			// Mount profile S3 creds at a dedicated path inside the
			// init container. Kept distinct from the main
			// backupAWSCredsMountPath so the dump source and PITR
			// archive can use different credentials if operators
			// ever need that.
			volumes = append(volumes, corev1.Volume{
				Name: "pitr-aws-creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  s3.CredentialsSecret,
						DefaultMode: ptr32(0o400),
					},
				},
			})
			initMounts = append(initMounts, corev1.VolumeMount{
				Name: "pitr-aws-creds", MountPath: pitrAWSCredsMountDir, ReadOnly: true,
			})
			initEnv = append(initEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_AWS_CREDS_DIR", Value: pitrAWSCredsMountDir,
			})
		}

	case v1alpha1.BackupStoragePVC:
		if profile.Storage.PVC == nil {
			return out, fmt.Errorf("pitr profile %q type=PVC but storage.pvc nil", profile.Name)
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
		initMounts = append(initMounts, corev1.VolumeMount{
			Name: "pitr-archive", MountPath: restorePITRPVCMountPath, ReadOnly: true,
		})
		initEnv = append(initEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: manifestPrefix},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_PVC_MOUNT_PATH", Value: restorePITRPVCMountPath},
		)

	default:
		return out, fmt.Errorf("pitr profile %q has unknown storage type %q",
			profile.Name, profile.Storage.Type)
	}

	// Init container reuses the operator image — the `pitr-download`
	// subcommand is shipped in the same binary as the main
	// reconciler. operatorImageFromEnv is wired from main.go; if it's
	// empty (tests) we fall back to "bloodraven:latest" to keep the
	// Job spec shape stable.
	image := operatorImageFromEnv
	if image == "" {
		image = "bloodraven:latest"
	}

	// If the backup profile encrypts binlog archives too, mount the
	// passphrase Secret onto the download init container and point
	// the sidecar archive-store wrapper at the file. The operator
	// uses the same conventional path as backup / restore Jobs so
	// env var names stay consistent.
	if profile.EncryptionEnabled() {
		ref := profile.Encryption.PassphraseSecret
		volumes = append(volumes, corev1.Volume{
			Name: "pitr-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  ref.Name,
					DefaultMode: ptr32(0o400),
					Items: []corev1.KeyToPath{
						{
							Key:  ref.PassphraseSecretKeyOrDefault(),
							Path: backupPassphraseFileName,
						},
					},
				},
			},
		})
		initMounts = append(initMounts, corev1.VolumeMount{
			Name:      "pitr-passphrase",
			MountPath: backupPassphraseMountPath,
			ReadOnly:  true,
		})
		initEnv = append(initEnv, corev1.EnvVar{
			Name:  "BLOODRAVEN_PITR_PASSPHRASE_FILE",
			Value: backupPassphraseMountPath + "/" + backupPassphraseFileName,
		})
	}

	// Init container shares the main container's hardened security
	// context — same UID/GID so the emptyDir is readable, same
	// seccomp/capability profile.
	_, initSC := mergeSecurityContexts(nil, nil)
	init := corev1.Container{
		Name:            restorePITRInitContainerName,
		Image:           image,
		Command:         []string{"bloodraven", "pitr-download"},
		Env:             initEnv,
		VolumeMounts:    initMounts,
		SecurityContext: initSC,
	}

	mainEnv := []corev1.EnvVar{
		{Name: "BLOODRAVEN_PITR_LOCAL_DIR", Value: restorePITRLocalDir},
		{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: pit.StopDatetime},
	}
	if pit.ExcludeGtids != "" {
		mainEnv = append(mainEnv, corev1.EnvVar{
			Name: "BLOODRAVEN_PITR_EXCLUDE_GTIDS", Value: pit.ExcludeGtids,
		})
	}

	return restorePITRFragments{
		MainEnv:       mainEnv,
		MainMounts:    []corev1.VolumeMount{mainMount},
		InitContainer: &init,
		PodVolumes:    volumes,
	}, nil
}

// joinComma returns a comma-separated list of non-empty strings, used
// for BLOODRAVEN_PITR_SITES. stdlib strings.Join would include empty
// entries when the source slice has gaps; we filter them explicitly.
func joinComma(parts []string) string {
	out := make([]byte, 0, 32)
	first := true
	for _, p := range parts {
		if p == "" {
			continue
		}
		if !first {
			out = append(out, ',')
		}
		out = append(out, p...)
		first = false
	}
	return string(out)
}
