package controller

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
	"github.com/shipstream/bloodraven/internal/pitrplan"
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
	pitrPodRenderVersion = "pitr-pod-render-v6-key-snapshot"

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
	// pitrStateMountPath is the sidecar's pod-lifetime scratch emptyDir
	// (BLOODRAVEN_PITR_STATE_DIR). The archiver keeps a pending
	// history-integrity marker there until its first verified session
	// persists it, so a sidecar container restart does not drop it (A3).
	pitrStateMountPath  = "/run/bloodraven/pitr-state"
	pitrStateVolumeName = "pitr-sidecar-state"
)

// pitrSidecarFragments is the bundle of additions the PITR feature
// contributes to the MySQL pod spec. Zero-value means "PITR is off";
// callers append unconditionally.
type pitrSidecarFragments struct {
	SidecarEnv          []corev1.EnvVar
	SidecarVolumeMounts []corev1.VolumeMount
	PodVolumes          []corev1.Volume
}

// buildPITRSidecarFragments returns the env/mount/volume additions the
// MySQL Deployment needs for the resolved PITR render in (§4.6). A disabled
// render returns empty fragments. Everything comes from in, never from the
// live profile: the identity fields may be held (a Bound binding, or the
// group hold), so a profile or Secret edit changes nothing here unless the
// resolver accepted it.
//
// The archive key is the binding's immutable snapshot Secret (data key
// "passphrase"), mounted optional (Δ32, K12): a missing snapshot leaves
// mysqld running and the archiver failing closed at store init, instead of
// turning a PITR key problem into a MySQL outage at the next pod restart.
// BLOODRAVEN_PITR_KEY_REVISION echoes the snapshot name, or "plaintext"
// for an unencrypted archive (Δ2, K4), so a sidecar report proves which
// key a writer verified with. No profile name is rendered (K7).
func buildPITRSidecarFragments(fg *v1alpha1.MysqlFailoverGroup, in pitrRenderInputs) (pitrSidecarFragments, error) {
	out := pitrSidecarFragments{}
	if !in.Enabled {
		return out, nil
	}
	if err := in.validate(); err != nil {
		return out, fmt.Errorf("pitr render: %w", err)
	}
	dest, err := pitrarchive.ParseDestination(in.Destination)
	if err != nil {
		return out, fmt.Errorf("pitr render: %w", err)
	}

	// Always mount the MySQL data PVC read-only into the sidecar. The
	// read-only flag prevents a rogue archiver from accidentally
	// corrupting the live binlog stream if a bug ever tried.
	out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
		Name:      "data",
		MountPath: pitrBinlogDir,
		ReadOnly:  true,
	})

	// Pod-lifetime state the archiver must keep across a sidecar
	// container restart (the data PVC is mounted read-only).
	out.PodVolumes = append(out.PodVolumes, corev1.Volume{
		Name:         pitrStateVolumeName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
		Name:      pitrStateVolumeName,
		MountPath: pitrStateMountPath,
	})

	// Core archiver env vars shared by both storage backends. The archive
	// identity the sidecar writes and verifies (§3.7): the group's UID, the
	// archive ID, the normalized destination and the encryption mode. The
	// manifest prefix is the destination's key prefix, so the sidecar, the
	// readers and the descriptor agree on one spelling of it.
	out.SidecarEnv = append(out.SidecarEnv,
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_STATE_DIR", Value: pitrStateMountPath},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_ENABLED", Value: "1"},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_BINLOG_DIR", Value: pitrBinlogDir},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_BINLOG_INDEX", Value: pitrBinlogIndex},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: in.StorageType},
		corev1.EnvVar{Name: "BLOODRAVEN_FAILOVER_GROUP_UID", Value: string(fg.UID)},
		// Also the retention client's identity (/pitr-cutoff?archive=).
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_ARCHIVE_ID", Value: in.ArchiveID},
		// Literal: kubelet expands $(VAR) and $$ in EnvVar.Value, which
		// would change the destination the sidecar parses while the
		// archive ID above was derived from the unexpanded one (F029).
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_DESTINATION", Value: literalArtifactEnvValue(in.Destination)},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_ENCRYPTION_MODE", Value: in.EncryptionMode},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_KEY_REVISION", Value: in.keyRevision()},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: literalArtifactEnvValue(dest.KeyPrefix)},
	)
	if in.PollInterval != "" {
		out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
			Name:  "BLOODRAVEN_PITR_POLL_INTERVAL",
			Value: in.PollInterval,
		})
	}

	switch dest.Kind {
	case pitrarchive.DestinationS3:
		out.SidecarEnv = append(out.SidecarEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_BUCKET", Value: literalArtifactEnvValue(in.Bucket)},
		)
		if in.EndpointURL != "" {
			out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_ENDPOINT_URL", Value: literalArtifactEnvValue(in.EndpointURL),
			})
		}
		if in.Region != "" {
			out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_REGION", Value: literalArtifactEnvValue(in.Region),
			})
		}
		if in.CredentialsSecret != "" {
			// Mount the AWS creds Secret as files readable by the sidecar's
			// non-root process. Unlike backup/restore Jobs, the MySQL pod
			// does not get the backup Job's fsGroup defaults, so mode 0400
			// leaves root-owned Secret files unreadable and prevents the
			// archiver from starting.
			out.PodVolumes = append(out.PodVolumes, corev1.Volume{
				Name: "pitr-aws-creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  in.CredentialsSecret,
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

	case pitrarchive.DestinationPVC:
		out.PodVolumes = append(out.PodVolumes, corev1.Volume{
			Name: "pitr-archive",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: dest.Claim,
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
		return out, fmt.Errorf("pitr render: unknown destination kind %q", dest.Kind)
	}

	// Encryption wiring: the binding's key snapshot, never the user's
	// Secret (#203). The archiver reads the file at store init; the
	// encryptedStore wrapper does the rest.
	if in.KeySecretName != "" {
		optional := true
		// Match the S3 credentials projection: the non-root sidecar
		// must be able to read this file even without a pod fsGroup.
		out.PodVolumes = append(out.PodVolumes, corev1.Volume{
			Name: "pitr-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  in.KeySecretName,
					DefaultMode: ptr32(0o444),
					Optional:    &optional,
					Items: []corev1.KeyToPath{
						{
							Key:  pitrKeySnapshotDataKey,
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

// activePITRDestination is the normalized destination of fg's active PITR
// profile, or false when PITR is disabled or its profile is missing or
// invalid. It is only a location: the archive ID and encryption identity
// an archive has are the ones its binding accepted
// (status.pitrArchiveBindings), never derived from the live profile
// (U60).
func activePITRDestination(fg *v1alpha1.MysqlFailoverGroup) (pitrarchive.Destination, bool) {
	if !pitrEnabled(fg) || fg.Spec.Backup.PITR.ProfileName == "" {
		return pitrarchive.Destination{}, false
	}
	profile := findProfile(fg, fg.Spec.Backup.PITR.ProfileName)
	if profile == nil {
		return pitrarchive.Destination{}, false
	}
	dest, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return pitrarchive.Destination{}, false
	}
	return dest, true
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

// restorePITRDumpMetaDir is the shared emptyDir where the dump-meta init
// container writes dump.json for pitr-download (§4.2).
const restorePITRDumpMetaDir = "/pitr-dump-meta"

// restorePITRDumpMetaContainerName is the dump-meta init container, which
// runs first whenever a restore or verification replays archived binlogs.
const restorePITRDumpMetaContainerName = "dump-meta"

// restorePITRDumpSourceMountPath is where dump-meta mounts a PVC-hosted
// dump's claim, read-only.
const restorePITRDumpSourceMountPath = "/pitr-dump-src"

// pitrReplayRequest is one point-in-time replay as the Job renders it: the
// spec of a restore (PointInTimeSpec) or of a verification
// (PointInTimeVerificationSpec), normalized.
type pitrReplayRequest struct {
	// Mode is "timestamp" or "latest" (pitrplan.Mode).
	Mode string
	// Stop is the target in backupmeta.MicroTimeLayout; "" in latest mode.
	Stop         string
	AllowPartial bool
	ExcludeGtids string
	// ArchiveProfileName is the profile whose archive is read; "" means the
	// group's own active PITR profile (verifications).
	ArchiveProfileName string
	// Verification marks a verification Job, which never accepts Partial.
	Verification bool
}

// pitrTarget checks a point-in-time target. The CRD pattern admits only
// RFC 3339 with an explicit zone and whole seconds (A-39), and metav1.Time
// carries second precision, so an object read from the API always passes;
// the check guards objects built in memory, which must not reach a Job with
// a sub-second or missing target. The planner still compares µs commit times
// against it (commit < T).
func pitrTarget(field string, t *metav1.Time) (time.Time, error) {
	if t == nil || t.IsZero() {
		return time.Time{}, fmt.Errorf("%s: %s is required", pitrarchive.TokenTargetInvalid, field)
	}
	if t.Nanosecond() != 0 {
		return time.Time{}, fmt.Errorf("%s: %s %s has a fractional second; targets are whole seconds", pitrarchive.TokenTargetInvalid, field, t.UTC().Format(time.RFC3339Nano))
	}
	return t.UTC(), nil
}

// canonicalPITRTarget renders a parsed target the one way every PITR
// container reads it.
func canonicalPITRTarget(t time.Time) string {
	return backupmeta.NewMicroTime(t).String()
}

// pitrRequestFromSpec normalizes a restore's PointInTimeSpec.
func pitrRequestFromSpec(pit *v1alpha1.PointInTimeSpec) (*pitrReplayRequest, error) {
	if pit == nil {
		return nil, nil
	}
	stop, err := pitrTarget("pointInTime.stopTime", &pit.StopTime)
	if err != nil {
		return nil, err
	}
	exclude, err := canonicalExcludeGtids("pointInTime.excludeGtids", pit.ExcludeGtids)
	if err != nil {
		return nil, err
	}
	return &pitrReplayRequest{
		Mode:               string(pitrplan.ModeTimestamp),
		Stop:               canonicalPITRTarget(stop),
		AllowPartial:       pit.AllowPartial,
		ExcludeGtids:       exclude,
		ArchiveProfileName: pit.ArchiveProfileName,
	}, nil
}

// canonicalExcludeGtids parses excludeGtids and renders it the one way every
// PITR container reads it: the planner's canonical set (pitrplan.Set.String),
// which is also what pitr-download writes to coverage.json "excluded" and
// what restore.py compares BLOODRAVEN_PITR_EXCLUDE_GTIDS against. A user
// spelling (MySQL's multi-line output, unmerged or unsorted intervals, tags
// grouped under one UUID, uppercase UUIDs) would otherwise fail that
// comparison. A set that does not parse is refused at reconcile.
//
// Two bounds keep one admitted string from hurting anything (F030, F031):
// every source identifier must be a UUID before the parser expands tags
// grouped under it (an arbitrary long "UUID" times thousands of tags would
// otherwise allocate gigabytes in the operator), and the canonical set must
// fit maxExcludeGtidsEnvBytes, since it travels as one container env
// string and Linux refuses to exec with a string over 128 KiB (E2BIG),
// which no PITR refusal would ever report.
func canonicalExcludeGtids(field, raw string) (string, error) {
	if err := checkUserGTIDSetUUIDs(raw); err != nil {
		return "", fmt.Errorf("%s: %s: %v", pitrarchive.TokenTargetInvalid, field, err)
	}
	set, err := pitrplan.ParseUserSet(raw)
	if err != nil {
		return "", fmt.Errorf("%s: %s: %v", pitrarchive.TokenTargetInvalid, field, err)
	}
	canonical := set.String()
	if len(canonical) > maxExcludeGtidsEnvBytes {
		return "", fmt.Errorf("%s: %s is %d bytes in canonical form (tags grouped under one UUID are spelled out per tag), over the %d-byte limit for the replay containers; exclude fewer GTIDs",
			pitrarchive.TokenTargetInvalid, field, len(canonical), maxExcludeGtidsEnvBytes)
	}
	return canonical, nil
}

// maxExcludeGtidsEnvBytes bounds the canonical excludeGtids rendered into
// BLOODRAVEN_PITR_EXCLUDE_GTIDS: half of Linux's 128 KiB per-string exec
// limit (MAX_ARG_STRLEN), the same 64 KiB the planner allows a segment's
// GTID set.
const maxExcludeGtidsEnvBytes = 64 << 10

// gtidSourceUUID is a server UUID as MySQL prints it, either case.
var gtidSourceUUID = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)

// checkUserGTIDSetUUIDs requires every entry of a user GTID set, split the
// way internalmysql.ParseGTIDSet splits it, to start with a UUID.
func checkUserGTIDSetUUIDs(raw string) error {
	for _, part := range strings.Split(strings.ReplaceAll(raw, "\n", ","), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sid, _, _ := strings.Cut(part, ":")
		if !gtidSourceUUID.MatchString(sid) {
			if len(sid) > 64 {
				sid = sid[:64] + "..."
			}
			return fmt.Errorf("GTID entry source %q is not a server UUID", sid)
		}
	}
	return nil
}

// pitrDownloadBudgetEnv renders the limits pitr-download budgets against
// (contracts.md "Round-1 env additions"): the pitr-binlogs emptyDir's
// sizeLimit (spec.backup.stagingVolumeSizeLimit, the same quantity
// stagingEmptyDirSource sets), and the container's own memory limit. Each
// is rendered only when set; without them pitr-download falls back to
// statfs and the cgroup limit. The memory limit is the literal value, not
// a downward-API reference, because the downward API reports node
// allocatable memory for a container with no limit.
func pitrDownloadBudgetEnv(backup *v1alpha1.BackupSpec, res corev1.ResourceRequirements) []corev1.EnvVar {
	var env []corev1.EnvVar
	if backup != nil && backup.StagingVolumeSizeLimit != nil {
		if v := backup.StagingVolumeSizeLimit.Value(); v > 0 {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_STAGING_LIMIT_BYTES", Value: strconv.FormatInt(v, 10)})
		}
	}
	if q, ok := res.Limits[corev1.ResourceMemory]; ok {
		if v := q.Value(); v > 0 {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_MEMORY_LIMIT_BYTES", Value: strconv.FormatInt(v, 10)})
		}
	}
	return env
}

// pitrOperatorImage is the image of the PITR init containers (dump-meta,
// pitr-download), which ship in the operator binary. operatorImageFromEnv
// is wired from main.go; when it is empty (tests) the Job spec keeps a
// stable shape with "bloodraven:latest".
func pitrOperatorImage() string {
	if operatorImageFromEnv == "" {
		return "bloodraven:latest"
	}
	return operatorImageFromEnv
}

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
	// KeySecretName is the read binding's archive key snapshot the init
	// container mounts ("" for a plaintext archive). Callers label the Job
	// with it (v1alpha1.PITRKeySecretLabel, Δ18) and a verification holds it (K9).
	KeySecretName string
}

// buildRestorePITRFragments wires up the restore-side PITR handoff
// (design WS-D §4.2, §4.13):
//
//	initContainer: bloodraven dump-meta     (built by the Job builder, which
//	   └── writes /pitr-dump-meta/dump.json  knows where the dump lives)
//	initContainer: bloodraven pitr-download
//	   ├── opens and verifies the source archive, plans the replay
//	   └── stages the plan's files and coverage.json in /pitr-binlogs
//	initContainer: bloodraven decrypt-download (encrypted dumps only)
//	container: mysqlsh restore.py
//	   ├── util.loadDump()
//	   └── replays every segment into one mysql session and proves the result
//
// The bootstrap restore (spec.initFromBackup.pointInTime) and verification
// share buildRestorePITRFragmentsFor. Both load the dump with its GTID set
// installed (updateGtidSet "replace"), which restore.py proves equals the
// plan's baseline before replaying. In-place restores have no pointInTime:
// a live primary's gtid_executed cannot be the dump's.
func buildRestorePITRFragments(fg *v1alpha1.MysqlFailoverGroup) (restorePITRFragments, error) {
	if fg.Spec.InitFromBackup == nil {
		return restorePITRFragments{}, nil
	}
	if err := validateRestoreArchiveProfile(fg, fg.Spec.InitFromBackup.PointInTime); err != nil {
		return restorePITRFragments{}, err
	}
	req, err := pitrRequestFromSpec(fg.Spec.InitFromBackup.PointInTime)
	if err != nil {
		return restorePITRFragments{}, err
	}
	return buildRestorePITRFragmentsFor(fg, req)
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
		return fmt.Errorf("%s: pointInTime.archiveProfileName is required: name the backup profile whose PITR archive the restore reads", pitrarchive.TokenTargetInvalid)
	}
	profile := findProfile(fg, pit.ArchiveProfileName)
	if profile == nil {
		return fmt.Errorf("%s: pointInTime.archiveProfileName %q not found in spec.backup.profiles", pitrarchive.TokenTargetInvalid, pit.ArchiveProfileName)
	}
	src, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return err
	}
	if own, ok := activePITRDestination(fg); ok && own == src {
		return fmt.Errorf("%s: pointInTime.archiveProfileName %q resolves to %s, which is this group's own active PITR destination (spec.backup.pitr.profileName %q); the restored group would archive into the archive it restores from; archive into a different profile or prefix",
			pitrarchive.TokenArchiveDestinationShared, pit.ArchiveProfileName, src.String(), fg.Spec.Backup.PITR.ProfileName)
	}
	return nil
}

// buildRestorePITRFragmentsFor renders req's pitr-download init container
// and the main container's replay env. A nil req is a no-op: callers get an
// empty fragments struct and skip the init containers entirely.
// pitrReplayRequestEnv renders the replay request env that pitr-download
// and restore.py both read.
func pitrReplayRequestEnv(req *pitrReplayRequest) []corev1.EnvVar {
	env := []corev1.EnvVar{{Name: "BLOODRAVEN_PITR_MODE", Value: req.Mode}}
	if req.Mode == string(pitrplan.ModeTimestamp) {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: req.Stop})
	}
	env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_ALLOW_PARTIAL", Value: strconv.FormatBool(req.AllowPartial)})
	if req.ExcludeGtids != "" {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_EXCLUDE_GTIDS", Value: req.ExcludeGtids})
	}
	return env
}

// VerificationPITRRequestEnvForTest renders the replay request env a
// verification Job carries for spec (nil for no replay), so the
// cmd/bloodraven tests can feed it to the download planner. Not for
// production use.
func VerificationPITRRequestEnvForTest(spec *v1alpha1.PointInTimeVerificationSpec) (map[string]string, error) {
	req, err := verificationPITRRequest(spec)
	if err != nil || req == nil {
		return nil, err
	}
	out := map[string]string{}
	for _, e := range pitrReplayRequestEnv(req) {
		out[e.Name] = e.Value
	}
	return out, nil
}

func buildRestorePITRFragmentsFor(fg *v1alpha1.MysqlFailoverGroup, req *pitrReplayRequest) (restorePITRFragments, error) {
	var out restorePITRFragments

	if req == nil {
		return out, nil
	}
	switch {
	case req.Mode == "timestamp" && req.Stop == "":
		return out, fmt.Errorf("%s: a timestamp replay needs a target", pitrarchive.TokenTargetInvalid)
	case req.Mode == "latest" && req.Stop != "":
		return out, fmt.Errorf("internal error: latest replay rendered with a target")
	case req.Mode != "timestamp" && req.Mode != "latest":
		return out, fmt.Errorf("internal error: unknown PITR mode %q", req.Mode)
	case req.Verification && req.AllowPartial:
		return out, fmt.Errorf("internal error: verification rendered with allowPartial")
	}
	// archiveProfileName reads a source archive that this group does not
	// archive into, so a restored group never writes into the source prefix.
	profileName := req.ArchiveProfileName
	if profileName == "" {
		if fg.Spec.Backup == nil || fg.Spec.Backup.PITR == nil || !fg.Spec.Backup.PITR.Enabled {
			if req.Verification {
				// Without archiveProfileName a verification replays the
				// group's active PITR archive; with PITR disabled there is
				// none, but a retained archive can still be named (U69).
				return out, fmt.Errorf("%s: pointInTime is set but spec.backup.pitr.enabled=false, so group %q has no active PITR archive to replay; "+
					"set pointInTime.archiveProfileName to the profile of the archive to replay (a retained archive needs no writer), "+
					"enable spec.backup.pitr, or verify without pointInTime", pitrarchive.TokenTargetInvalid, fg.Name)
			}
			return out, fmt.Errorf("%s: pointInTime is set but spec.backup.pitr.enabled=false; "+
				"PITR replay reads the source group's binlog archive, so continuous archival "+
				"must be configured, or pointInTime.archiveProfileName must name the archive's profile", pitrarchive.TokenTargetInvalid)
		}
		profileName = fg.Spec.Backup.PITR.ProfileName
	}
	profile := findProfile(fg, profileName)
	if profile == nil {
		return out, fmt.Errorf("pitr profile %q not found in spec.backup.profiles", profileName)
	}
	// The archive is read with the encryption identity its read binding
	// accepted, never the profile's live Secret (#203, §4.7): a Pending or
	// refused binding returns an *errPITRBindingNotReady the caller waits
	// or fails on (Δ22).
	binding, err := pitrReadBinding(fg, profileName)
	if err != nil {
		return out, err
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
	// checks the descriptor's encryption against the binding's. Replay
	// considers every site manifest under the prefix (D-9).
	dest, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		return out, err
	}
	if dest.String() != binding.Destination {
		return out, fmt.Errorf("internal error: read binding %s names %q, profile %q resolves to %q", binding.ID, binding.Destination, profile.Name, dest.String())
	}
	// The replay request, rendered identically on pitr-download and the
	// main container (contracts: "WP-B2 assumptions").
	requestEnv := pitrReplayRequestEnv(req)
	initEnv := append(append([]corev1.EnvVar{}, requestEnv...),
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_DUMP_META", Value: restorePITRDumpMetaDir + "/dump.json"},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: string(profile.Storage.Type)},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_OUTPUT_DIR", Value: restorePITRLocalDir},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_DESTINATION", Value: literalArtifactEnvValue(dest.String())},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_ENCRYPTION_MODE", Value: string(binding.Encryption.Mode)},
		corev1.EnvVar{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: literalArtifactEnvValue(dest.KeyPrefix)},
	)
	// dump-meta writes dump.json here; pitr-download only reads it.
	dumpMetaVol := corev1.Volume{
		Name:         "pitr-dump-meta",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
	initMounts := []corev1.VolumeMount{initMount, {Name: "pitr-dump-meta", MountPath: restorePITRDumpMetaDir, ReadOnly: true}}
	volumes := []corev1.Volume{emptyDirVol, dumpMetaVol}

	switch profile.Storage.Type {
	case v1alpha1.BackupStorageS3:
		if profile.Storage.S3 == nil {
			return out, fmt.Errorf("pitr profile %q type=S3 but storage.s3 nil", profile.Name)
		}
		s3 := profile.Storage.S3
		initEnv = append(initEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_BUCKET", Value: literalArtifactEnvValue(s3.Bucket)},
		)
		if s3.EndpointURL != "" {
			initEnv = append(initEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_ENDPOINT_URL", Value: literalArtifactEnvValue(s3.EndpointURL),
			})
		}
		if s3.Region != "" {
			initEnv = append(initEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_PITR_S3_REGION", Value: literalArtifactEnvValue(s3.Region),
			})
		}
		if s3.CredentialsSecretName != "" {
			// Mount profile S3 creds at a dedicated path inside the
			// init container. Kept distinct from the main
			// backupAWSCredsMountPath so the dump source and PITR
			// archive can use different credentials if operators
			// ever need that.
			volumes = append(volumes, corev1.Volume{
				Name: "pitr-aws-creds",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  s3.CredentialsSecretName,
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
		claim := dest.Claim
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
	image := pitrOperatorImage()

	// An encrypted archive is opened with the read binding's key snapshot
	// (data key "passphrase"), mounted required: a Job must not start
	// without its key (the binding is Bound/Bound, so the snapshot was
	// verified this pass). The path is the conventional one backup and
	// restore Jobs use, so env var names stay consistent.
	keySecret := ""
	if binding.Encryption.Mode == v1alpha1.EncryptionModePassphrase {
		keySecret = binding.Encryption.KeySecretName
		if keySecret == "" {
			return out, fmt.Errorf("internal error: Bound passphrase binding %s has no key snapshot", binding.ID)
		}
		volumes = append(volumes, corev1.Volume{
			Name: "pitr-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  keySecret,
					DefaultMode: ptr32(0o400),
					Items: []corev1.KeyToPath{
						{
							Key:  pitrKeySnapshotDataKey,
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
	initEnv = append(initEnv, pitrDownloadBudgetEnv(fg.Spec.Backup, initResources)...)
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

	mainEnv := append([]corev1.EnvVar{
		{Name: "BLOODRAVEN_PITR_LOCAL_DIR", Value: restorePITRLocalDir},
		{Name: "TZ", Value: "UTC"},
	}, requestEnv...)

	return restorePITRFragments{
		MainEnv:       mainEnv,
		MainMounts:    []corev1.VolumeMount{mainMount},
		InitContainer: &init,
		PodVolumes:    volumes,
		KeySecretName: keySecret,
	}, nil
}
