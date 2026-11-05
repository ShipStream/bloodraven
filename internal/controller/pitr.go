package controller

import (
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
)

// Defaults applied when the corresponding PITRSpec fields are unset.
// Kept in one place so generateMyCnf, buildPITRSidecarFragments, and
// computeSpecHash all agree on the effective value — otherwise a
// change here would silently fail to roll pods.
const (
	defaultPITRMaxBinlogSize       = "100M"
	defaultPITRArchivePollInterval = 60 * time.Second

	// Bump when PITR pod rendering changes without a corresponding CRD
	// field change. ComputeSpecHash includes this value so already-enabled
	// PITR pods roll forward to pick up render-only fixes such as Secret
	// volume modes.
	pitrPodRenderVersion = "pitr-pod-render-v5-archive-identity"

	// Official MySQL container images own /var/lib/mysql as uid/gid 999.
	// PITR sidecars must read mysql-bin.index and sealed binlog files from
	// that volume, which are not world-readable.
	mysqlDataUID = int64(999)
	mysqlDataGID = int64(999)
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

	// The archive identity the sidecar writes and verifies (§3.7): the
	// group's UID, the archive ID derived from it and the normalized
	// destination, and the profile's encryption mode. The manifest prefix
	// is the destination's key prefix, so the sidecar, the readers and
	// the descriptor agree on one spelling of it.
	ident, err := pitrArchiveIdentityFor(fg, profile)
	if err != nil {
		return out, err
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
		corev1.EnvVar{Name: "BLOODRAVEN_FAILOVER_GROUP_UID", Value: string(fg.UID)},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_ARCHIVE_ID", Value: ident.ArchiveID},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_DESTINATION", Value: ident.Destination.String()},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_ENCRYPTION_MODE", Value: string(ident.Encryption.Mode)},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: ident.Destination.KeyPrefix},
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
		out.SidecarEnv = append(out.SidecarEnv,
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
			// Mount the AWS creds Secret as files readable by the sidecar's
			// non-root process. Unlike backup/restore Jobs, the MySQL pod
			// does not get the backup Job's fsGroup defaults, so mode 0400
			// leaves root-owned Secret files unreadable and prevents the
			// archiver from starting.
			out.PodVolumes = append(out.PodVolumes, corev1.Volume{
				Name: "pitr-aws-creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  s3.CredentialsSecret,
						DefaultMode: ptr32(0o444),
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
		claim := ident.Destination.Claim

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
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_PVC_MOUNT_PATH", Value: pitrPVCMountPath},
		)

	default:
		return out, fmt.Errorf("profile %q has unknown storage type %q", profile.Name, profile.Storage.Type)
	}

	// Encryption wiring. When the profile has encryption enabled we
	// mount the passphrase Secret onto the sidecar container and
	// point the archiver at the file; everything else (wrapper
	// selection, re-upload / prune semantics) falls out naturally
	// from the encryptedStore that sidecar.NewArchiveStore chooses.
	if profile.EncryptionEnabled() {
		ref := profile.Encryption.PassphraseSecret
		// Match the S3 credentials projection: the non-root sidecar
		// must be able to read this file even without a pod fsGroup.
		out.PodVolumes = append(out.PodVolumes, corev1.Volume{
			Name: "pitr-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  ref.Name,
					DefaultMode: ptr32(0o444),
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

// pitrArchiveIdentity is the archive identity a group derives for one of
// its backup profiles: where the archive lives, the ID a writer creates
// there, and the encryption the descriptor must record.
type pitrArchiveIdentity struct {
	Destination pitrarchive.Destination
	ArchiveID   string
	Encryption  pitrarchive.EncryptionIdentity
}

// pitrArchiveIdentityFor derives profile's archive identity for fg. The
// archive ID is the creating derivation (ArchiveIDDerivations[0]) of fg's
// UID and the normalized destination.
func pitrArchiveIdentityFor(fg *v1alpha1.MysqlFailoverGroup, profile *v1alpha1.BackupProfile) (pitrArchiveIdentity, error) {
	dest, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return pitrArchiveIdentity{}, err
	}
	return pitrArchiveIdentity{
		Destination: dest,
		ArchiveID:   pitrarchive.CreateArchiveID(fg.UID, dest),
		Encryption:  pitrprofile.EncryptionIdentityFor(profile),
	}, nil
}

// activePITRArchiveIdentity is the identity of fg's active PITR archive,
// or false when PITR is disabled or its profile is missing or invalid.
func activePITRArchiveIdentity(fg *v1alpha1.MysqlFailoverGroup) (pitrArchiveIdentity, bool) {
	if !pitrEnabled(fg) || fg.Spec.Backup.PITR.ProfileName == "" {
		return pitrArchiveIdentity{}, false
	}
	profile := findProfile(fg, fg.Spec.Backup.PITR.ProfileName)
	if profile == nil {
		return pitrArchiveIdentity{}, false
	}
	ident, err := pitrArchiveIdentityFor(fg, profile)
	if err != nil {
		return pitrArchiveIdentity{}, false
	}
	return ident, true
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
// The downloaded binlogs keep their original GTIDs and include
// transactions the dump already contains. On a fresh bootstrap target the
// load installs the dump's gtidExecuted into @@GLOBAL.gtid_purged
// (updateGtidSet:"replace", see restoreLoadOptionsJSON), so MySQL skips
// those transactions and any GTID archived by more than one site — no
// per-MysqlBackup metadata plumbing needed here. That baseline cannot be
// installed on a live primary, whose own gtid_executed already holds the
// archived GTIDs, so in-place restores reject pointInTime
// (errInPlacePointInTimeUnsupported).
//
// The verification path reuses this same builder (buildRestorePITRFragmentsFor)
// against an EPHEMERAL verify mysqld, which is why that mysqld is started
// gtid_mode=ON (verify_script.sh) and loads with the same baseline
// (marshalVerificationLoadOptions), so raw replay of cross-site archived
// binlogs does not double-apply in-dump or duplicated GTIDs (#101).
func buildRestorePITRFragments(fg *v1alpha1.MysqlFailoverGroup) (restorePITRFragments, error) {
	if fg.Spec.InitFromBackup == nil {
		return restorePITRFragments{}, nil
	}
	if err := validateRestoreArchiveProfile(fg, fg.Spec.InitFromBackup.PointInTime); err != nil {
		return restorePITRFragments{}, err
	}
	return buildRestorePITRFragmentsFor(fg, fg.Spec.InitFromBackup.PointInTime)
}

// validateRestoreArchiveProfile enforces D-10 for initFromBackup: the
// restore names the profile whose archive it reads (CEL requires it at
// admission; this also covers objects admitted before the rule), and that
// profile may not resolve to the group's own active PITR destination, so
// the restored group never writes into the archive it restores from.
func validateRestoreArchiveProfile(fg *v1alpha1.MysqlFailoverGroup, pit *v1alpha1.PointInTimeSpec) error {
	if pit == nil {
		return nil
	}
	if pit.ArchiveProfileName == "" {
		return fmt.Errorf("pointInTime.archiveProfileName is required: name the backup profile whose PITR archive the restore reads")
	}
	profile := findProfile(fg, pit.ArchiveProfileName)
	if profile == nil {
		return fmt.Errorf("pitr profile %q not found in spec.backup.profiles", pit.ArchiveProfileName)
	}
	src, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return err
	}
	if own, ok := activePITRArchiveIdentity(fg); ok && own.Destination == src {
		return fmt.Errorf("%s: pointInTime.archiveProfileName %q resolves to %s, which is this group's own active PITR destination (spec.backup.pitr.profileName %q); the restored group would archive into the archive it restores from; archive into a different profile or prefix",
			pitrarchive.TokenArchiveDestinationShared, pit.ArchiveProfileName, src.String(), fg.Spec.Backup.PITR.ProfileName)
	}
	return nil
}

// buildRestorePITRFragmentsFor is the parameterized variant that lets
// the bootstrap restore (spec.initFromBackup.pointInTime) and backup
// verification share the PITR init-container wiring. A nil pit is a no-op: callers get an empty
// fragments struct and skip the init container entirely.
func buildRestorePITRFragmentsFor(fg *v1alpha1.MysqlFailoverGroup, pit *v1alpha1.PointInTimeSpec) (restorePITRFragments, error) {
	var out restorePITRFragments

	if pit == nil {
		return out, nil
	}
	if pit.StopDatetime == "" {
		return out, fmt.Errorf("pointInTime.stopDatetime is required")
	}
	// archiveProfileName reads a source archive that this group does not
	// archive into, so a restored group never writes into the source prefix.
	profileName := pit.ArchiveProfileName
	if profileName == "" {
		if fg.Spec.Backup == nil || fg.Spec.Backup.PITR == nil || !fg.Spec.Backup.PITR.Enabled {
			return out, fmt.Errorf(
				"pointInTime is set but spec.backup.pitr.enabled=false; " +
					"PITR restore requires the failover group to have continuous binlog " +
					"archival configured on the source, or pointInTime.archiveProfileName")
		}
		profileName = fg.Spec.Backup.PITR.ProfileName
	}
	profile := findProfile(fg, profileName)
	if profile == nil {
		return out, fmt.Errorf("pitr profile %q not found in spec.backup.profiles", profileName)
	}

	// Shared emptyDir volume. Init container writes; main container
	// reads. Size cap is opt-in via spec.backup.stagingVolumeSizeLimit
	// (AUDIT H6); leaving it unset preserves the prior behavior where
	// only the node's ephemeral-storage limit applies.
	emptyDirVol := corev1.Volume{
		Name:         "pitr-binlogs",
		VolumeSource: corev1.VolumeSource{EmptyDir: stagingEmptyDirSource(fg.Spec.Backup)},
	}
	initMount := corev1.VolumeMount{Name: "pitr-binlogs", MountPath: restorePITRLocalDir}
	mainMount := corev1.VolumeMount{Name: "pitr-binlogs", MountPath: restorePITRLocalDir, ReadOnly: true}

	// The reader opens the archive at the normalized destination and
	// checks the descriptor's encryption against the profile's. Replay
	// considers every site manifest under the prefix (D-9).
	ident, err := pitrArchiveIdentityFor(fg, profile)
	if err != nil {
		return out, err
	}
	initEnv := []corev1.EnvVar{
		{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: pit.StopDatetime},
		{Name: "BLOODRAVEN_PITR_ALLOW_PARTIAL", Value: strconv.FormatBool(pit.AllowPartial)},
		{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: string(profile.Storage.Type)},
		{Name: "BLOODRAVEN_PITR_OUTPUT_DIR", Value: restorePITRLocalDir},
		{Name: "BLOODRAVEN_PITR_DESTINATION", Value: ident.Destination.String()},
		{Name: "BLOODRAVEN_PITR_ENCRYPTION_MODE", Value: string(ident.Encryption.Mode)},
		{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: ident.Destination.KeyPrefix},
	}
	initMounts := []corev1.VolumeMount{initMount}
	volumes := []corev1.Volume{emptyDirVol}

	switch profile.Storage.Type {
	case v1alpha1.BackupStorageS3:
		if profile.Storage.S3 == nil {
			return out, fmt.Errorf("pitr profile %q type=S3 but storage.s3 nil", profile.Name)
		}
		s3 := profile.Storage.S3
		initEnv = append(initEnv,
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
		claim := ident.Destination.Claim
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
	// Resources flow through from spec.backup.resources when set;
	// otherwise default to 100m/128Mi so the init container is admitted
	// on LimitRange-gated clusters.
	var initBackupSrc *backupResourcesSource
	if fg.Spec.Backup != nil {
		initBackupSrc = &backupResourcesSource{Resources: fg.Spec.Backup.Resources}
	}
	initResources := effectiveBackupResources(initBackupSrc, defaultInitContainerResources())
	init := corev1.Container{
		Name:                     restorePITRInitContainerName,
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		Image:                    image,
		Command:                  operatorCommand("pitr-download"),
		Env:                      initEnv,
		Resources:                initResources,
		VolumeMounts:             initMounts,
		SecurityContext:          initSC,
	}

	mainEnv := []corev1.EnvVar{
		{Name: "BLOODRAVEN_PITR_LOCAL_DIR", Value: restorePITRLocalDir},
		{Name: "TZ", Value: "UTC"},
		{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: pit.StopDatetime},
		{Name: "BLOODRAVEN_PITR_ALLOW_PARTIAL", Value: strconv.FormatBool(pit.AllowPartial)},
	}
	if pit.ExcludeGtids != "" {
		// The planner must exempt exactly what mysqlbinlog --exclude-gtids skips.
		exclude := corev1.EnvVar{Name: "BLOODRAVEN_PITR_EXCLUDE_GTIDS", Value: pit.ExcludeGtids}
		mainEnv = append(mainEnv, exclude)
		init.Env = append(init.Env, exclude)
	}

	return restorePITRFragments{
		MainEnv:       mainEnv,
		MainMounts:    []corev1.VolumeMount{mainMount},
		InitContainer: &init,
		PodVolumes:    volumes,
	}, nil
}
