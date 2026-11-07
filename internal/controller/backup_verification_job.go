package controller

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
)

const (
	// verificationDataMountPath is where the verification Job mounts
	// its ephemeral datadir emptyDir. The verify.sh script reads this
	// path from BLOODRAVEN_DATA_DIR.
	//
	// It MUST be /var/lib/mysql. On hosts with the stock mysqld AppArmor
	// profile loaded (e.g. GitHub-hosted Ubuntu runners), the kernel
	// attaches that profile to a containerized mysqld started from
	// /usr/sbin/mysqld — even though the pod is otherwise unconfined —
	// and the profile only permits the datadir under /var/lib/mysql/**.
	// A datadir anywhere else (the old /var/lib/mysql-verify) is denied
	// with errno 13 before mysqld can even create it. verify.sh runs a
	// copy of mysqld from the run volume when the keyring component is
	// available, which that profile does not match; the stock-binary
	// fallback still runs in place, so the path stays fixed.
	verificationDataMountPath = "/var/lib/mysql"

	// verificationRunMountPath is where the verification Job mounts an
	// emptyDir for mysqld's unix socket and pid file. The stock mysqld
	// AppArmor profile permits the socket only under /run/mysqld/, and the
	// image's /run/mysqld is not writable under readOnlyRootFilesystem, so
	// we mount a fresh emptyDir there. verify.sh also stages the private
	// mysqld copy and keyring component configuration here, and reads the
	// path from BLOODRAVEN_RUN_DIR.
	verificationRunMountPath = "/run/mysqld"

	// verificationDefaultActiveDeadline is the fallback Job
	// activeDeadlineSeconds used when spec.backup.activeDeadlineSeconds
	// is not set. Verification loads a dump into a cold instance so the
	// ceiling matches the backup default.
	verificationDefaultActiveDeadline = int64(7200)
)

// verificationJobInputs bundles the resolved inputs for building the
// verification Job. Mirrors BackupJobInputs in spirit.
type verificationJobInputs struct {
	FailoverGroup        *v1alpha1.MysqlFailoverGroup
	Profile              v1alpha1.BackupProfile
	Verification         *v1alpha1.MysqlBackupVerification
	Backup               *v1alpha1.MysqlBackup
	CredsSecretName      string
	ScriptsConfigMapName string
}

// buildVerificationJob constructs the batchv1.Job that runs verify.sh
// inside the container image pinned on spec.backup.image. verify.sh
// boots an ephemeral mysqld on the emptyDir datadir, then delegates the
// actual load to the shared restore.py script.
func buildVerificationJob(in verificationJobInputs) (*batchv1.Job, error) {
	fg := in.FailoverGroup
	v := in.Verification
	backup := in.Backup

	if in.CredsSecretName == "" {
		return nil, fmt.Errorf("verification job: creds secret name is empty")
	}
	if in.ScriptsConfigMapName == "" {
		return nil, fmt.Errorf("verification job: scripts configmap name is empty")
	}
	if backup == nil {
		return nil, fmt.Errorf("verification job: backup reference not resolved")
	}
	// The artifact is read at the destination the backup's producing Job
	// recorded; only the storage credentials come from the profile.
	storage, err := recordedArtifactStorage(backup)
	if err != nil {
		return nil, fmt.Errorf("verification job: %w", err)
	}

	image := backupImage(fg)

	labels := map[string]string{
		labelAppName:                          "mysql-verify",
		labelInstance:                         fg.Name,
		v1alpha1.FailoverGroupLabel:           fg.Name,
		v1alpha1.BackupProfileLabel:           in.Profile.Name,
		v1alpha1.MysqlBackupVerificationLabel: v.Name,
		labelManagedBy:                        managerName,
		v1alpha1.ResourceLabel:                verificationResourceKindCR,
	}

	loadOptsJSON, err := marshalVerificationLoadOptions(verificationLoadOptions())
	if err != nil {
		return nil, err
	}

	inputURL := mysqlShellDumpInput(storage.Location, storage.Type)

	// PITR replay fragments (download init container + shared emptyDir
	// + main-container env/mounts). buildRestorePITRFragmentsFor returns
	// empty fragments when the translated spec is nil, so the append
	// paths are unconditional below.
	pitrReq, err := verificationPITRRequest(v.Spec.PointInTime)
	if err != nil {
		return nil, err
	}
	// D-44 (a target before the dump ended) is enforced by dump-meta
	// against the dump's own serverClock.end. status.completionTime is
	// no bound: it includes the upload, which can end long after the
	// dump did, so it would refuse valid targets in between (F034).
	pitrFrags, err := buildRestorePITRFragmentsFor(fg, pitrReq)
	if err != nil {
		return nil, err
	}
	if pitrFrags.KeySecretName != "" {
		// Correlation label (Δ18); the verification also holds the
		// snapshot (K9) before this Job is created.
		labels[v1alpha1.PITRKeySecretLabel] = pitrFrags.KeySecretName
	}
	// An encrypted backup is decrypted only with the key it recorded (its
	// dump-key snapshot, status.artifactStorage.passphraseSecretKeyRef), never
	// the profile's live Secret (Δ17).
	dumpKey, err := verificationDumpKey(backup)
	if err != nil {
		return nil, err
	}
	// dump-meta reads the verified backup's identity first (§4.2). It
	// decrypts with the same key decrypt-download uses.
	var dumpMeta *corev1.Container
	var dumpMetaVolumes []corev1.Volume
	if pitrReq != nil {
		key := dumpKey
		profile := in.Profile
		src, err := pitrDumpSourceForBackup(backup, &profile, key)
		if err != nil {
			return nil, fmt.Errorf("verification job: %w", err)
		}
		container, volumes, err := buildDumpMetaInitContainer(fg, src)
		if err != nil {
			return nil, fmt.Errorf("verification job: %w", err)
		}
		dumpMeta, dumpMetaVolumes = &container, volumes
		if src.ExpectedSourceUID != "" {
			pitrFrags.MainEnv = append(pitrFrags.MainEnv, corev1.EnvVar{Name: "BLOODRAVEN_PITR_EXPECTED_SOURCE_UID", Value: string(src.ExpectedSourceUID)})
		}
	}
	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_DATA_DIR", Value: verificationDataMountPath},
		{Name: "BLOODRAVEN_RUN_DIR", Value: verificationRunMountPath},
		{Name: "BLOODRAVEN_SCRIPTS_DIR", Value: backupScriptsMountPath},
		{Name: "BLOODRAVEN_INPUT_URL", Value: literalArtifactEnvValue(inputURL)},
		{Name: "BLOODRAVEN_LOAD_OPTIONS", Value: loadOptsJSON},
		// mysqlsh's default progressFile lives next to the dump, but the
		// backup source is mounted read-only on verification Jobs, so
		// disable progress tracking to avoid a writable-backup
		// requirement.
		{Name: "BLOODRAVEN_LOAD_PROGRESS_FILE", Value: ""},
		{Name: "HOME", Value: mysqlshHomeMountPath},
	}

	// PITR replay env: tell verify.sh that this run replays (and so must
	// publish a verdict), and restore.py that it is a verification, which
	// never accepts a Partial outcome. pitrFrags.MainEnv carries the
	// replay request itself.
	if pitrReq != nil {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_PITR_MODE", Value: pitrReq.Mode})
	}
	env = append(env, pitrFrags.MainEnv...)

	// Sanity-check env: verify.sh branches on BLOODRAVEN_VERIFY_SANITY_QUERY
	// being non-empty; min-rows floor and timeout are numeric scalars.
	if v.Spec.SanityCheck != nil && v.Spec.SanityCheck.Query != "" {
		query, err := validateSanityQuery(v.Spec.SanityCheck.Query)
		if err != nil {
			return nil, fmt.Errorf("verification job: %w", err)
		}
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_SANITY_QUERY", Value: query})
		timeout := defaultSanityCheckTimeout
		if v.Spec.SanityCheck.Timeout != nil {
			timeout = v.Spec.SanityCheck.Timeout.Duration
		}
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS", Value: strconv.FormatInt(wholeSecondsCeil(timeout), 10)})
		// The floor is rendered only when set: an explicit minResult of 0
		// still rejects a negative result, an unset one compares nothing.
		if v.Spec.SanityCheck.MinResult != nil {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_VERIFY_SANITY_MIN_ROWS", Value: strconv.FormatInt(*v.Spec.SanityCheck.MinResult, 10)})
		}
	}

	// The ephemeral datadir, mysqld socket/pid dir, mysqlsh HOME, /tmp, and
	// any PITR / decrypt staging dirs live on emptyDir volumes rather than
	// PVCs (kind's local-path PVCs are left root-owned, so a non-root
	// mysqld can't use them; emptyDir honors the pod's fsGroup). The
	// datadir and socket paths are NOT arbitrary: where the stock mysqld
	// AppArmor profile is loaded on the node, the kernel confines a
	// containerized /usr/sbin/mysqld (the stock-binary fallback in
	// verify.sh) to /var/lib/mysql/** (datadir) and /run/mysqld/ (socket),
	// so those mounts must land exactly there.
	// /tmp must also be a writable emptyDir — InnoDB writes a temp file
	// there during --initialize, which fails with EROFS otherwise.
	//
	// Do NOT set EmptyDir.SizeLimit: a size-limited emptyDir is set up as a
	// separate mount that is not always fsGroup-chowned. Node disk is
	// bounded through spec.resources.limits.ephemeral-storage instead.
	volumes := []corev1.Volume{
		{
			Name:         "datadir",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name:         "run-mysqld",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name: "scripts",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: in.ScriptsConfigMapName},
					DefaultMode:          ptr32(0o555),
				},
			},
		},
		{
			Name:         "mysqlsh-home",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name:         "tmp",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}
	mounts := []corev1.VolumeMount{
		{Name: "datadir", MountPath: verificationDataMountPath},
		{Name: "run-mysqld", MountPath: verificationRunMountPath},
		{Name: "scripts", MountPath: backupScriptsMountPath, ReadOnly: true},
		{Name: "mysqlsh-home", MountPath: mysqlshHomeMountPath},
		{Name: "tmp", MountPath: tmpMountPath},
	}

	// S3-hosted backups need AWS creds mounted the same way the backup
	// Job mounts them. PVC-hosted backups need the backup PVC mounted
	// read-only so mysqlsh util.loadDump() can read the on-disk dump.
	awsCredsSecret := ""
	switch storage.Type {
	case v1alpha1.BackupStorageS3:
		if in.Profile.Storage.S3 == nil {
			return nil, fmt.Errorf("verification job: backup %q is on S3 but profile %q has no s3 config",
				backup.Name, in.Profile.Name)
		}
		awsCredsSecret = in.Profile.Storage.S3.CredentialsSecretName
		// The recorded destination is already expanded; escape it so
		// kubelet's $(VAR) expansion cannot change it again.
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: literalArtifactEnvValue(storage.Bucket)},
		)
		if storage.EndpointURL != "" {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: literalArtifactEnvValue(storage.EndpointURL)})
		}
		if storage.Region != "" {
			env = append(env, corev1.EnvVar{Name: "AWS_REGION", Value: literalArtifactEnvValue(storage.Region)})
		}
	case v1alpha1.BackupStoragePVC:
		volumes = append(volumes, corev1.Volume{
			Name: "backup-src",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: storage.ClaimName,
					ReadOnly:  true,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "backup-src", MountPath: backupPVCMountPath, ReadOnly: true,
		})
	default:
		return nil, fmt.Errorf("verification job: backup %q has unknown storage type %q",
			backup.Name, storage.Type)
	}

	if awsCredsSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "aws-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  awsCredsSecret,
					DefaultMode: ptr32(0o400),
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "aws-creds", MountPath: backupAWSCredsMountPath, ReadOnly: true,
		})
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath})
	}

	// Splice PITR fragments: shared emptyDir volume, pitr-aws-creds /
	// pitr-archive volume, and the read-only main-container mount.
	volumes = append(volumes, pitrFrags.PodVolumes...)
	volumes = append(volumes, dumpMetaVolumes...)
	mounts = append(mounts, pitrFrags.MainMounts...)

	// Init order (§6.3): dump-meta → pitr-download → decrypt-download.
	var initContainers []corev1.Container
	if dumpMeta != nil {
		initContainers = append(initContainers, *dumpMeta)
	}
	if pitrFrags.InitContainer != nil {
		initContainers = append(initContainers, *pitrFrags.InitContainer)
	}

	// -------- Decryption wiring -------------------------------------
	//
	// When the source backup is encrypted we run the same
	// `bloodraven decrypt-download` init container the restore path
	// uses, with the dump-key snapshot the backup recorded
	// (status.artifactStorage.passphraseSecretKeyRef, verificationDumpKey): the
	// immutable copy of the key its artifacts were encrypted with (Δ17).
	// Never fall back to the profile's live encryption.passphraseSecretKeyRef:
	// it may hold other bytes since the backup ran, and an encrypted
	// backup that recorded no key is refused (PITRBackupDumpKeyMissing).
	if dumpKey != nil {
		if operatorImageFromEnv == "" {
			return nil, fmt.Errorf("verification job: operator image is not configured (SetOperatorImageDefaults); required for encrypted verification")
		}

		const decryptDir = "/restore-decrypted"
		// Shared emptyDir: init container writes plaintext, main
		// container reads it through BLOODRAVEN_INPUT_URL.
		volumes = append(volumes, corev1.Volume{
			Name:         "restore-decrypted",
			VolumeSource: corev1.VolumeSource{EmptyDir: stagingEmptyDirSource(fg.Spec.Backup)},
		})
		passRef := *dumpKey
		volumes = append(volumes, corev1.Volume{
			Name: "backup-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  passRef.Name,
					DefaultMode: ptr32(0o400),
					Items: []corev1.KeyToPath{
						{
							Key:  passRef.PassphraseSecretKeyOrDefault(),
							Path: backupPassphraseFileName,
						},
					},
				},
			},
		})

		decEnv := []corev1.EnvVar{
			{Name: "BLOODRAVEN_TARGET_DIR", Value: decryptDir},
			{Name: "BLOODRAVEN_STORAGE_TYPE", Value: string(storage.Type)},
			{Name: "BLOODRAVEN_BACKUP_PASSPHRASE_FILE", Value: backupPassphraseMountPath + "/" + backupPassphraseFileName},
		}
		decMounts := []corev1.VolumeMount{
			{Name: "restore-decrypted", MountPath: decryptDir},
			{Name: "backup-passphrase", MountPath: backupPassphraseMountPath, ReadOnly: true},
			{Name: "tmp", MountPath: tmpMountPath},
		}

		switch storage.Type {
		case v1alpha1.BackupStorageS3:
			_, prefix := splitS3Location(storage.Location)
			decEnv = append(decEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: literalArtifactEnvValue(prefix)},
				corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: literalArtifactEnvValue(storage.Bucket)},
			)
			if storage.EndpointURL != "" {
				decEnv = append(decEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: literalArtifactEnvValue(storage.EndpointURL),
				})
			}
			if storage.Region != "" {
				decEnv = append(decEnv, corev1.EnvVar{Name: "AWS_REGION", Value: literalArtifactEnvValue(storage.Region)})
			}
			if in.Profile.Storage.S3.CredentialsSecretName != "" {
				decMounts = append(decMounts, corev1.VolumeMount{
					Name: "aws-creds", MountPath: backupAWSCredsMountPath, ReadOnly: true,
				})
				decEnv = append(decEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath,
				})
			}
			// Drop the S3 env from the mysqlsh container: it reads the
			// decrypted directory now.
			filtered := env[:0]
			for _, e := range env {
				switch e.Name {
				case "BLOODRAVEN_S3_BUCKET",
					"BLOODRAVEN_S3_ENDPOINT_OVERRIDE",
					"AWS_REGION",
					"BLOODRAVEN_AWS_CREDS_DIR":
					continue
				case "BLOODRAVEN_INPUT_URL":
					continue
				}
				filtered = append(filtered, e)
			}
			env = append(filtered, corev1.EnvVar{
				Name: "BLOODRAVEN_INPUT_URL", Value: decryptDir,
			})

		case v1alpha1.BackupStoragePVC:
			const decPVCMount = "/restore-src-encrypted"
			// Swap the read-only backup-src mount to the decrypt init
			// container; drop from the main container's mounts since
			// mysqlsh no longer reads ciphertext directly.
			volumes = filterOutVolumeByName(volumes, "backup-src")
			mounts = filterOutMountByName(mounts, "backup-src")
			volumes = append(volumes, corev1.Volume{
				Name: "decrypt-src",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: storage.ClaimName,
						ReadOnly:  true,
					},
				},
			})
			decMounts = append(decMounts, corev1.VolumeMount{
				Name: "decrypt-src", MountPath: decPVCMount, ReadOnly: true,
			})
			// Derive the relative prefix under the PVC root from the
			// recorded location (the absolute pod-side path the backup
			// wrote to).
			prefix := strings.TrimPrefix(storage.Location, backupPVCMountPath+"/")
			prefix = strings.TrimSuffix(prefix, "/")
			decEnv = append(decEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_PVC_MOUNT_PATH", Value: decPVCMount},
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: literalArtifactEnvValue(prefix)},
			)
			// Point the mysqlsh INPUT_URL at the decrypted staging
			// dir instead of the in-pod backup path.
			for i, e := range env {
				if e.Name == "BLOODRAVEN_INPUT_URL" {
					env[i].Value = decryptDir
					break
				}
			}
		default:
			return nil, fmt.Errorf("verification job: backup %q has unknown storage type %q for decrypt wiring",
				backup.Name, storage.Type)
		}

		// Add the read-only decrypt dir to the main container so
		// loadDump can read files from it.
		mounts = append(mounts, corev1.VolumeMount{
			Name: "restore-decrypted", MountPath: decryptDir, ReadOnly: true,
		})

		_, initSC := mergeSecurityContexts(nil, nil)
		// Use spec.backup.resources when set; otherwise the helper
		// returns the 100m/128Mi default so the init container clears
		// LimitRange admission.
		var initBackupSrc *backupResourcesSource
		if fg.Spec.Backup != nil {
			initBackupSrc = &backupResourcesSource{Resources: fg.Spec.Backup.Resources}
		}
		initResources := effectiveBackupResources(initBackupSrc, defaultInitContainerResources())
		initContainers = append(initContainers, corev1.Container{
			Name:                     "decrypt-download",
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			Image:                    operatorImageFromEnv,
			Command:                  operatorCommand("decrypt-download"),
			Env:                      decEnv,
			Resources:                initResources,
			VolumeMounts:             decMounts,
			SecurityContext:          initSC,
		})
	}

	activeDeadline := verificationDefaultActiveDeadline
	backoff := int32(0)
	var (
		pullSecrets []corev1.LocalObjectReference
		userPod     *corev1.PodSecurityContext
		userCont    *corev1.SecurityContext
	)
	if fg.Spec.Backup != nil {
		if fg.Spec.Backup.ActiveDeadlineSeconds > 0 {
			activeDeadline = fg.Spec.Backup.ActiveDeadlineSeconds
		}
		pullSecrets = fg.Spec.Backup.ImagePullSecrets
		userPod = fg.Spec.Backup.PodSecurityContext
		userCont = fg.Spec.Backup.ContainerSecurityContext
	}
	podSC, containerSC := mergeSecurityContexts(userPod, userCont)

	sourceSnapshot, err := encodeVerificationSource(verificationBackupSnapshot(backup))
	if err != nil {
		return nil, fmt.Errorf("encode verification source snapshot: %w", err)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      verificationJobName(v.Name),
			Namespace: v.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &activeDeadline,
			BackoffLimit:          &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{v1alpha1.VerificationBackupAnnotation: string(sourceSnapshot)}},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: pullSecrets,
					SecurityContext:  podSC,
					// verify.sh and the optional decrypt init container
					// both run against storage and never call the
					// Kubernetes API.
					AutomountServiceAccountToken: boolPtr(false),
					// Verification restores into a throwaway mysqld inside
					// this pod and never touches the group's MySQL, so the
					// read-only side is a fine place for it and a failover
					// must not evict a long-running run.
					Tolerations:    groupReadOnlyTolerations(fg),
					InitContainers: initContainers,
					Containers: []corev1.Container{
						{
							Name: backupJobContainerName,
							// verify.sh publishes restore.py's PITR verdict and its
							// own failures here; others fall back to the log tail.
							TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
							Image:                    image,
							// verify.sh is mounted as executable via the
							// scripts ConfigMap default mode 0555.
							Command:         []string{"/bin/bash", backupScriptsMountPath + "/verify.sh"},
							Env:             env,
							Resources:       v.Spec.Resources,
							VolumeMounts:    mounts,
							SecurityContext: containerSC,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}
	setTerminationMessageLimit(&job.Spec.Template.Spec)
	return job, nil
}

// validateSanityQuery enforces the CRD contract that
// SanityCheckSpec.Query is a single SQL statement with no embedded
// newlines. Multi-statement input would break the client-side timeout
// budget (`timeout` wraps `mysql -e`, and multiple statements share the
// budget) and complicates the scalar-capture contract, so we reject
// any `;` other than an optional trailing one before handing the value
// to `mysql -e`. Returns the query with a trailing `;` stripped so
// verify.sh sees a single clean statement.
func validateSanityQuery(q string) (string, error) {
	trimmed := strings.TrimSpace(q)
	if trimmed == "" {
		return "", fmt.Errorf("sanityCheck.query must not be empty")
	}
	if strings.ContainsAny(trimmed, "\n\r") {
		return "", fmt.Errorf("sanityCheck.query must be a single line (contains a newline)")
	}
	// Strip a single trailing `;` — common SQL convention — before
	// scanning for any remaining separators.
	trimmed = strings.TrimRight(trimmed, " \t")
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimRight(trimmed, " \t")
	if strings.Contains(trimmed, ";") {
		return "", fmt.Errorf("sanityCheck.query must be a single statement (contains `;`)")
	}
	// The query runs through `mysql -e`, whose client parses `\x`
	// commands anywhere and long-form commands (`system`, `source`, ...)
	// at the start of the input. `\!` / `system` run shell commands in a
	// pod that has the backup store credentials mounted, so reject every
	// backslash and require the statement to open as a query.
	if strings.Contains(trimmed, `\`) {
		return "", fmt.Errorf("sanityCheck.query must not contain a backslash (mysql client commands are not allowed)")
	}
	if !sanityQueryStartRE.MatchString(trimmed) {
		return "", fmt.Errorf("sanityCheck.query must start with SELECT, WITH, or `(`")
	}
	return trimmed, nil
}

var sanityQueryStartRE = regexp.MustCompile(`(?i)^(\(|(select|with)\b)`)

// defaultSanityCheckTimeout is spec.sanityCheck.timeout's API default.
const defaultSanityCheckTimeout = 60 * time.Second

// verificationPITRRequest normalizes a verification's
// PointInTimeVerificationSpec into the replay request the shared PITR
// builders render. It returns nil (no replay) when the block is absent.
// Verifications read the group's own active PITR archive and never accept
// a Partial outcome.
func verificationPITRRequest(in *v1alpha1.PointInTimeVerificationSpec) (*pitrReplayRequest, error) {
	if in == nil {
		return nil, nil
	}
	mode, err := pitrWireMode(in.Mode)
	if err != nil {
		return nil, err
	}
	// archiveProfileName reads a retained archive (after a prefix switch);
	// empty means the group's active PITR profile (Δ28).
	req := &pitrReplayRequest{Verification: true, ArchiveProfileName: in.ArchiveProfileName, Mode: string(mode)}
	switch {
	case mode == pitrplan.ModeTimestamp:
		stop, err := pitrTarget("pointInTime.stopTime (mode=Timestamp)", in.StopTime)
		if err != nil {
			return nil, err
		}
		req.Stop = canonicalPITRTarget(stop)
	case in.StopTime != nil:
		return nil, fmt.Errorf("%s: pointInTime.stopTime is forbidden when mode=Latest", pitrarchive.TokenTargetInvalid)
	}
	exclude, err := canonicalExcludeGtids("pointInTime.excludeGtids", in.ExcludeGtids)
	if err != nil {
		return nil, err
	}
	req.ExcludeGtids = exclude
	return req, nil
}

// verificationLoadOptions returns the util.loadDump() options used for
// a verification run. Verification Jobs disable load-progress tracking, so
// every run starts clean. SkipBinlog=true because the ephemeral mysqld runs
// with --skip-log-bin anyway; LoadIndexes=true so a
// "restore loads successfully" result is not weakened by deferred
// index builds that would only fail later.
func verificationLoadOptions() *v1alpha1.LoadOptions {
	t := true
	return &v1alpha1.LoadOptions{
		Threads:     4,
		SkipBinlog:  &t,
		LoadIndexes: &t,
	}
}

// marshalVerificationLoadOptions serializes the verification-run load
// options with the dump's GTID baseline installed (#101). The verify
// mysqld is started gtid_mode=ON (see verify_script.sh) so the raw
// `mysqlbinlog | mysql` PITR replay can rely on server-side GTID dedup,
// exactly like a bootstrap PITR restore into a fresh group; see
// marshalDumpGtidBaselineLoadOptions for why that needs
// updateGtidSet:"replace" with skipBinlog:true.
func marshalVerificationLoadOptions(l *v1alpha1.LoadOptions) (string, error) {
	return marshalDumpGtidBaselineLoadOptions(l)
}

// verificationDumpKey is the passphrase Secret key a verification decrypts
// backup's dump with: the one the backup recorded (Δ17). nil for a
// plaintext backup; an encrypted backup that recorded none is refused with
// PITRBackupDumpKeyMissing.
func verificationDumpKey(backup *v1alpha1.MysqlBackup) (*v1alpha1.PassphraseSecretKeyRef, error) {
	if !backupStatusEncrypted(&backup.Status) {
		return nil, nil
	}
	if backup.Status.ArtifactStorage == nil || backup.Status.ArtifactStorage.PassphraseSecretKeyRef == nil ||
		backup.Status.ArtifactStorage.PassphraseSecretKeyRef.Name == "" {
		return nil, fmt.Errorf("%s: verification job: backup %q is encrypted but recorded no dump key (status.artifactStorage.passphraseSecretKeyRef)",
			pitrarchive.TokenBackupDumpKeyMissing, backup.Name)
	}
	recorded := backup.Status.ArtifactStorage.PassphraseSecretKeyRef
	return &v1alpha1.PassphraseSecretKeyRef{Name: recorded.Name, Key: recorded.Key}, nil
}
