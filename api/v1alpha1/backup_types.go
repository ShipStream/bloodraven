package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultBackupImage is the default image that runs mysqlsh inside backup,
// restore, and cleanup Jobs. We deliberately pin a full version here
// (major.minor) rather than a floating tag.
//
// A common mistake is to reach for "container-registry.oracle.com/mysql/
// community-shell:8.0" — that repository does not exist in the Oracle
// registry. The shipped mysqlsh binary is bundled with the community-server
// image, which is what we default to here.
//
// Production deployments should always pin this explicitly via
// spec.backup.image; never rely on ":9" or ":latest" because cross-version
// mysqlsh dump/load compatibility is not guaranteed.
const DefaultBackupImage = "container-registry.oracle.com/mysql/community-server:9.7"

// BackupSpec is the top-level backup configuration embedded in
// MysqlFailoverGroupSpec as spec.backup. All fields are optional; omitting
// spec.backup entirely disables backups for this failover group.
// +kubebuilder:validation:XValidation:rule="!has(self.pitr) || !has(self.pitr.enabled) || !self.pitr.enabled || (has(self.pitr.profileName) && has(self.profiles) && self.profiles.exists(p, p.name == self.pitr.profileName))",message="spec.backup.pitr.profileName must name an entry of spec.backup.profiles when pitr.enabled is true"
// +kubebuilder:validation:XValidation:rule="!has(self.schedules) || self.schedules.all(s, has(self.profiles) && self.profiles.exists(p, p.name == s.profileName))",message="each spec.backup.schedules[].profileName must name an entry of spec.backup.profiles"
// +kubebuilder:validation:XValidation:rule="!has(self.profiles) || self.profiles.all(p, !has(p.verification) || !has(p.verification.pointInTime) || !has(p.verification.pointInTime.archiveProfileName) || self.profiles.exists(q, q.name == p.verification.pointInTime.archiveProfileName))",message="each spec.backup.profiles[].verification.pointInTime.archiveProfileName must name an entry of spec.backup.profiles"
type BackupSpec struct {
	// Image is the container image containing mysqlsh. It must include
	// util.dumpInstance() and util.loadDump() with native S3 support
	// (mysqlsh 8.0.32+). Defaults to DefaultBackupImage; production
	// deployments should always pin a full major.minor tag here and
	// avoid floating tags.
	// +kubebuilder:default="container-registry.oracle.com/mysql/community-server:9.7"
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullSecrets for the backup image. Keyed by name.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Profiles are reusable, named backup configurations. Schedules and
	// ad-hoc MysqlBackup CRs reference profiles by name. Names are unique
	// (the list is a map keyed by name).
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Profiles []BackupProfile `json:"profiles,omitempty"`

	// Schedules describe cron-driven recurring backups, each referencing
	// a profile by name. Each schedule becomes a Kubernetes CronJob owned
	// by this failover group. Names are unique (the list is a map keyed by
	// name).
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Schedules []BackupSchedule `json:"schedules,omitempty"`

	// MaxLagSecondsForSource is the threshold for replica-first source
	// selection. The backup reconciler prefers the replica site as the
	// dump source, but only if secondsBehindSource is at or below this
	// value. Above it, the reconciler falls back to the primary. 0 accepts
	// only a fully caught-up replica. Default: 300.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxLagSecondsForSource *int64 `json:"maxLagSecondsForSource,omitempty"`

	// Resources for the backup Job's mysqlsh container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// ActiveDeadlineSeconds caps the wall-clock duration of a single
	// backup Job. Default: 7200 (2h).
	// +kubebuilder:default=7200
	// +kubebuilder:validation:Minimum=60
	// +optional
	ActiveDeadlineSeconds int64 `json:"activeDeadlineSeconds,omitempty"`

	// BackoffLimit is the Job backoffLimit applied to backup Jobs. 0 runs
	// each Job's pod once. Default: 2.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// Retry configures operator-level retries for scheduled MysqlBackup
	// CRs that land in Failed. This is independent of Job-level
	// backoffLimit: Job backoff retries the container, whereas this
	// retries the whole CR, producing a new MysqlBackup with its own
	// Job and attempt counter. Omit to disable.
	// +optional
	Retry *BackupRetrySpec `json:"retry,omitempty"`

	// PITR configures point-in-time recovery via continuous binary-log
	// archival. When enabled, the per-pod sidecar watches the local
	// binlog index and uploads each sealed binlog file to the storage
	// backend of the referenced profile. Restore jobs can then replay
	// archived binlogs up to a target timestamp on top of a full dump.
	// Omit the field to disable PITR entirely.
	// +optional
	PITR *PITRSpec `json:"pitr,omitempty"`

	// PodSecurityContext overrides the default pod-level security context
	// used for backup, restore, and cleanup Jobs. User fields are merged
	// on top of the hardened defaults (RunAsNonRoot, RuntimeDefault
	// seccomp, etc.) — leave fields unset to keep the default.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// ContainerSecurityContext overrides the default container-level
	// security context. Same merge semantics as PodSecurityContext:
	// hardened defaults (ReadOnlyRootFilesystem, AllowPrivilegeEscalation
	// false, capability drop ALL) are applied first and user fields take
	// precedence.
	// +optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`

	// StagingVolumeSizeLimit caps the emptyDir used to stage plaintext
	// dump / restore / verification artifacts inside backup Jobs. When
	// the full dump spills past this limit the kubelet evicts the Job
	// pod; without the cap, a multi-GB dump can fill the node's
	// `/var/lib/kubelet` partition and trigger DiskPressure eviction of
	// unrelated pods. Empty value falls through to the
	// node's ephemeral-storage limit (preserving previous behavior).
	// The reconciler does not enforce this — it's a hint to the kubelet.
	// +optional
	StagingVolumeSizeLimit *resource.Quantity `json:"stagingVolumeSizeLimit,omitempty"`
}

// PITRSpec configures continuous binary-log archival for point-in-time
// recovery. The sidecar running on the primary MySQL pod watches the
// local binlog index via inotify and uploads each sealed binlog file
// (plus a per-site manifest) to the storage backend of the named
// backup profile. Restore Jobs later consume those artifacts to replay
// transactions up to a specified target timestamp.
type PITRSpec struct {
	// Enabled turns binlog archival on or off. Omitting spec.backup.pitr
	// entirely is equivalent to Enabled=false.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ProfileName selects the BackupProfile whose storage backend will
	// hold the archived binlog files. Binlog objects land under a
	// dedicated "binlogs/" subprefix (or subpath) inside the profile
	// location so they do not collide with full dumps. Required when
	// Enabled=true.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +optional
	ProfileName string `json:"profileName,omitempty"`

	// MaxBinlogSize is forwarded as the MySQL max_binlog_size variable,
	// controlling how frequently binlogs rotate. Smaller values reduce
	// the RPO gap (the tail of transactions on a crashed primary that
	// may not yet have been archived) at the cost of more objects in
	// storage. A byte count with an optional k, m or g suffix in either
	// case, as mysqld reads it (e.g. "100M", "512m", "1073741824").
	// Defaults to "100M" when PITR is enabled.
	// +kubebuilder:default="100M"
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[0-9]+[kKmMgG]?$`
	// +optional
	MaxBinlogSize string `json:"maxBinlogSize,omitempty"`

	// ArchivePollInterval is how often the archiver reconciles the list
	// of binlog files against storage as a belt-and-suspenders safety
	// net alongside inotify. Default: 60s. Bounds: 5s to 1h.
	// +kubebuilder:default="1m0s"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('5s') && duration(self) <= duration('1h')",message="archivePollInterval must be between 5s and 1h"
	// +optional
	ArchivePollInterval *metav1.Duration `json:"archivePollInterval,omitempty"`
}

// PITRStatus is the observed summary of continuous binlog archival for
// a failover group. Populated by the operator from periodic polls of
// each sidecar's /archiver/status endpoint, which reports aggregates of
// that site's manifest. While a sidecar does not answer, its last
// reported coverage is kept and Message says so.
type PITRStatus struct {
	// OldestArchivedTime is the earliest first-event timestamp across
	// all archived binlogs still present in storage. It bounds archive
	// retention; it does not prove that a target can be restored.
	// +optional
	OldestArchivedTime *metav1.Time `json:"oldestArchivedTime,omitempty"`

	// NewestArchivedTime is the latest last-event timestamp across all
	// archived binlogs. It measures archive freshness. Whether a target
	// is covered is decided per restore from verified archive bytes, a
	// single replaying site and a proven stop event.
	// +optional
	NewestArchivedTime *metav1.Time `json:"newestArchivedTime,omitempty"`

	// ArchivedFileCount is the number of binlog files currently held in
	// storage across all sites.
	// +optional
	ArchivedFileCount int32 `json:"archivedFileCount,omitempty"`

	// ArchivedBytes is the total size of archived binlog files.
	// +optional
	ArchivedBytes int64 `json:"archivedBytes,omitempty"`

	// LastArchivedTime is the wall clock at which the most recent
	// binlog file was uploaded (archivedAt on the manifest entry).
	// +optional
	LastArchivedTime *metav1.Time `json:"lastArchivedTime,omitempty"`

	// Message lists per-site problems as "site: error" joined with "; "
	// (for example an archive store that cannot be initialized, a
	// manifest read error, or an unreachable sidecar, in which case the
	// coverage fields are last-known). Empty on the happy path.
	// +optional
	Message string `json:"message,omitempty"`

	// Archive is the archive identity the sidecars observed at the active
	// PITR destination: the descriptor's archive ID and encryption mode,
	// and whether every reporting sidecar verified it.
	// +optional
	Archive *PITRArchiveObservation `json:"archive,omitempty"`
}

// PointInTimeSpec configures a restore that replays archived binlogs on
// top of a loaded full dump. It lives under InitFromBackupSpec; leaving
// it unset yields a plain dump-only restore. The binlogs come from the
// source archive that ArchiveProfileName names: the group that took the
// dump must have archived its binlogs there continuously from before the
// dump to the target (its spec.backup.pitr enabled with that profile's
// destination). The restored group's own spec.backup.pitr is not
// involved; it may be disabled.
type PointInTimeSpec struct {
	// ArchiveProfileName names the spec.backup.profiles entry whose binlog
	// archive the restore reads. Required under initFromBackup: the restore
	// reads the source group's archive through it, and it may not resolve
	// to this group's own active PITR destination, so the new group never
	// writes into the archive it restores from. Replay considers every
	// site manifest under that archive.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	ArchiveProfileName string `json:"archiveProfileName,omitempty"`

	// AllowPartial accepts an archive with no transaction at or after
	// StopTime: the restore then replays to the archive head (outcome
	// Partial). It relaxes nothing else; archive identity, lineage and
	// continuity checks still apply.
	// +optional
	// +kubebuilder:default=false
	AllowPartial bool `json:"allowPartial,omitempty"`

	// StopTime is the target instant: RFC 3339 with an explicit zone and
	// whole seconds ("2026-04-15T09:30:00Z", "2026-04-15T09:30:00+02:00").
	// Fractional seconds, a lowercase "t" or "z", and an offset outside
	// -23:59..+23:59 are refused. Replay restores the history of the first
	// archived transaction committed at or after it (commit times are
	// compared to the microsecond), cut just before that transaction.
	// +required
	// +kubebuilder:validation:Pattern=`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=date-time
	StopTime metav1.Time `json:"stopTime"`

	// ExcludeGtids are never replayed and must not intersect the backup.
	// Every archived transaction committed between the backup's newest
	// transaction and the target that is outside the target history (lost
	// at a failover, errant, or on another branch) must be listed; the
	// PITRArchiveDivergentHistory refusal prints that set. Excluding a
	// transaction on the target history (for example an accidental DROP
	// TABLE) skips its GTID, but later row images may still carry its
	// effects; such exclusions are reported as onHistoryExclusions.
	// +optional
	// +kubebuilder:validation:MaxLength=131072
	ExcludeGtids string `json:"excludeGtids,omitempty"`
}

// BackupRetrySpec configures operator-level retries for scheduled
// MysqlBackup CRs. MaxAttempts includes the original attempt.
// +kubebuilder:validation:XValidation:rule="!has(self.initialBackoff) || !has(self.maxBackoff) || duration(self.maxBackoff) >= duration(self.initialBackoff)",message="retry.maxBackoff must be at least retry.initialBackoff"
type BackupRetrySpec struct {
	// MaxAttempts is the total number of attempts (including the
	// original). 1 disables retries. Values are clamped to [1, 10].
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`

	// InitialBackoff is the delay between the first failure and the first
	// retry. Subsequent retries double the backoff until MaxBackoff.
	// Default: 1m. Bounds: 1s to 1h.
	// +kubebuilder:default="1m0s"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('1h')",message="retry.initialBackoff must be between 1s and 1h"
	// +optional
	InitialBackoff *metav1.Duration `json:"initialBackoff,omitempty"`

	// MaxBackoff caps the exponential backoff. Default: 30m. Bounds: 1s to
	// 24h, and at least InitialBackoff.
	// +kubebuilder:default="30m0s"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('24h')",message="retry.maxBackoff must be between 1s and 24h"
	// +optional
	MaxBackoff *metav1.Duration `json:"maxBackoff,omitempty"`
}

// BackupProfile is a named, reusable backup configuration.
type BackupProfile struct {
	// Name is the unique profile identifier within this failover group, a
	// DNS label of at most 63 characters (the bound every reference to a
	// profile, such as pointInTime.archiveProfileName, carries).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +required
	Name string `json:"name"`

	// Storage selects and configures the backup destination.
	// +required
	Storage BackupStorage `json:"storage"`

	// Dump customizes util.dumpInstance() options.
	// +optional
	Dump *DumpOptions `json:"dump,omitempty"`

	// Retention decides which of this profile's MysqlBackup objects are
	// deleted after each terminal backup. Deleting a backup removes its
	// artifacts. See BackupRetention.
	// +kubebuilder:default={}
	// +optional
	Retention BackupRetention `json:"retention,omitempty"`

	// Verification, when set and Enabled=true, schedules periodic
	// MysqlBackupVerification runs against the most recent Succeeded
	// backup produced from this profile. The MysqlFailoverGroup
	// reconciler renders a CronJob per enabled verification that POSTs
	// a MysqlBackupVerification CR, analogous to how
	// spec.backup.schedules[] renders backup CronJobs.
	// +optional
	Verification *VerificationSpec `json:"verification,omitempty"`

	// Encryption, when set, turns on client-side envelope encryption
	// of every artifact produced under this profile: full dump objects
	// written by backup Jobs AND sealed binlog files uploaded by the
	// sidecar archiver. Leaving the field unset falls back to whatever
	// encryption the underlying storage provides (bucket default SSE,
	// volume-level encryption, etc.) — which may be sufficient for
	// many workloads, but is not the same guarantee as keys held in a
	// Kubernetes Secret that the operator controls.
	//
	// The operator snapshots the passphrase into immutable Secrets it
	// manages: each MysqlBackup encrypts its dump with its own snapshot
	// (recorded in its status.artifactStorage.passphraseSecretKeyRef), and the
	// PITR archive with the snapshot its binding accepted
	// (status.pitrArchiveBindings). Changing the passphrase bytes or this
	// field on a profile in PITR archive use is refused: archive with a
	// new profile on a new storage prefix instead.
	//
	// Treat the Secret as part of the failover group's critical
	// recovery material and back it up out-of-band. See
	// https://bloodraven.dev/docs/backup-and-restore/backup-encryption for the wire format,
	// threat model, and rotation guidance.
	// +optional
	Encryption *BackupEncryptionSpec `json:"encryption,omitempty"`
}

// BackupEncryptionSpec turns on application-level envelope encryption
// for a backup profile. When set, dump objects and (when the profile is
// also used for PITR) archived binlog files are wrapped in a
// chunked AES-256-GCM stream using a data-encryption key derived from
// the referenced passphrase via HKDF-SHA256. The salt is random per
// file so two encryptions of the same plaintext never share a key.
//
// The operator enforces the invariant that the restore side resolves a
// passphrase identical to the encryption-time passphrase: for
// bootstrap restores the resolver consults
// spec.initFromBackup.decryption first, then the profile referenced
// by the source MysqlBackup. Missing or mismatched passphrases make
// the restore Job fail fast with a clear "decryption failed" event
// rather than silently reloading garbage into MySQL.
type BackupEncryptionSpec struct {
	// Algorithm selects the encryption mechanism. Only "AES-256-GCM" is
	// supported.
	// +kubebuilder:default="AES-256-GCM"
	// +optional
	Algorithm EncryptionAlgorithm `json:"algorithm,omitempty"`

	// PassphraseSecretKeyRef selects the Secret key, in the same namespace
	// as the MysqlFailoverGroup, holding the passphrase. The
	// passphrase must be non-empty; recommended length is >=32 bytes
	// of random material (use `head -c 48 /dev/urandom | base64`).
	// +required
	PassphraseSecretKeyRef PassphraseSecretKeyRef `json:"passphraseSecretKeyRef"`
}

// PassphraseSecretKeyRef points at a Secret key holding the passphrase
// used as input to the per-file HKDF-SHA256 key derivation. The
// same shape is reused by restore specs (spec.initFromBackup.decryption,
// spec.restoreInPlace.decryption) and the standby cluster's
// source.decryption, so one helper covers both sides. On a backup
// profile the operator does not mount this Secret into archive writers,
// backup Jobs or the cleanup Jobs of backups that recorded their dump key:
// it snapshots the passphrase (see
// BackupProfile.Encryption), and a byte change on a profile in PITR
// archive use is refused. One exception remains: deleting an encrypted
// backup that never succeeded and recorded no dump key (made before dump
// keys were recorded) runs its abandoned-artifact cleanup Job with this
// Secret, to read that backup's stamp.
type PassphraseSecretKeyRef struct {
	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	Name string `json:"name"`

	// Key within the Secret that holds the passphrase bytes. Leading
	// and trailing whitespace is stripped when the passphrase is
	// read.
	// +kubebuilder:default="passphrase"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	// +optional
	Key string `json:"key,omitempty"`
}

// BackupDecryptionSpec carries the passphrase a restore Job needs to
// recover an encrypted artifact: on spec.initFromBackup and
// spec.restoreInPlace, the source dump (archived binlogs use the archive
// binding's key snapshot); on a standby cluster, the source archive. It is
// intentionally a separate type from BackupEncryptionSpec: restore-side
// consumers never need to pick an Algorithm (the ciphertext header encodes
// it), so the surface is just "where is the passphrase".
type BackupDecryptionSpec struct {
	// PassphraseSecretKeyRef selects the Secret key holding the passphrase
	// that was used when the source artifact was originally encrypted.
	// Bumping the Secret contents to a new value does not rotate the
	// ciphertext — it will simply fail to decrypt.
	// +required
	PassphraseSecretKeyRef PassphraseSecretKeyRef `json:"passphraseSecretKeyRef"`
}

// BackupRetention is a profile's retention. A successful backup is kept
// when any rule keeps it: it is one of the newest KeepLast, or it
// completed within KeepWithinDays. So a successful backup is deleted only
// when it is outside KeepLast and older than KeepWithinDays. Failed
// backups are counted separately, against KeepFailed. A backup that a
// verification may be reading is kept until that run stops.
type BackupRetention struct {
	// KeepLast is how many of the newest successful backups are always
	// kept. At least one is, so retention never deletes the last good
	// backup.
	// +kubebuilder:default=7
	// +kubebuilder:validation:Minimum=1
	// +optional
	KeepLast int32 `json:"keepLast,omitempty"`

	// KeepWithinDays also keeps every successful backup that completed
	// within this many days, however many there are. Unset keeps by count
	// only.
	// +kubebuilder:validation:Minimum=1
	// +optional
	KeepWithinDays *int32 `json:"keepWithinDays,omitempty"`

	// KeepFailed is how many of the newest failed backups are kept; 0
	// keeps none. A failed backup is kept for two minutes after the
	// operator records its verdict, so a waiting client reads it. A
	// failed backup that is still the latest of a schedule in
	// spec.backup.schedules is kept until a newer backup of that schedule
	// exists, whatever this says: spec.backup.retry retries it, and
	// status.backupSchedules reports it.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	// +optional
	KeepFailed *int32 `json:"keepFailed,omitempty"`
}

// BackupStorageType enumerates the supported storage backends.
// +kubebuilder:validation:Enum=S3;PVC
type BackupStorageType string

const (
	BackupStorageS3  BackupStorageType = "S3"
	BackupStoragePVC BackupStorageType = "PVC"
)

// BackupStorage is a tagged union. Exactly one of S3 or PVC must be set
// and must match the Type discriminator.
// +kubebuilder:validation:XValidation:rule="(self.type == 'S3' && has(self.s3) && !has(self.pvc)) || (self.type == 'PVC' && has(self.pvc) && !has(self.s3))",message="exactly one of s3 or pvc must be set and must match type"
type BackupStorage struct {
	// Type is the storage backend discriminator.
	// +required
	Type BackupStorageType `json:"type"`

	// S3 configures an S3-compatible object store destination.
	// +optional
	S3 *S3Storage `json:"s3,omitempty"`

	// PVC configures a local PVC destination.
	// +optional
	PVC *PVCStorage `json:"pvc,omitempty"`
}

// S3Storage configures S3 or S3-compatible destinations for mysqlsh
// util.dumpInstance() and util.loadDump(). Fields map directly onto
// mysqlsh's s3BucketName / s3EndpointOverride / s3Profile options.
type S3Storage struct {
	// Bucket is the S3 bucket name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 63",message="bucket must be at most 63 bytes (UTF-8)"
	// +required
	Bucket string `json:"bucket"`

	// Prefix is the object-name prefix inside the bucket. Each backup
	// lands under "<prefix>/<mysqlbackup-name>/" and the PITR archive under
	// "<prefix>/binlogs/". A leading or trailing "/" is ignored. Empty
	// segments ("a//b") and "." or ".." segments are refused: the dump
	// writer keeps a prefix as written, while the archive and every reader
	// (standby discovery, dump-meta) resolve it as a path, so such a
	// prefix would name different keys for each.
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 512",message="prefix must be at most 512 bytes (UTF-8)"
	// +kubebuilder:validation:Pattern=`^/?(([^/.][^/]*|\.[^/.][^/]*|\.\.[^/]+)(/([^/.][^/]*|\.[^/.][^/]*|\.\.[^/]+))*)?/?$`
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// Region is the AWS region. Optional for non-AWS S3 stores.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Region string `json:"region,omitempty"`

	// EndpointURL overrides the S3 endpoint (e.g. MinIO, Ceph, Wasabi).
	// Passed to mysqlsh as s3EndpointOverride. Must be an absolute http://
	// or https:// URL without credentials, a query or a fragment.
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 512",message="endpointURL must be at most 512 bytes (UTF-8)"
	// +kubebuilder:validation:Pattern=`^https?://[^\s/?#@]+(/[^\s?#]*)?$`
	// +optional
	EndpointURL string `json:"endpointURL,omitempty"`

	// CredentialsSecretName names a Secret whose keys provide AWS
	// credentials to backup and restore Jobs. Expected keys:
	//   AWS_ACCESS_KEY_ID (required)
	//   AWS_SECRET_ACCESS_KEY (required)
	//   AWS_SESSION_TOKEN (optional)
	//   AWS_REGION (optional, overrides .region)
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	CredentialsSecretName string `json:"credentialsSecretName"`
}

// PVCStorage configures a local persistent volume as the backup destination:
// either an existing claim (ClaimName) or a claim the operator provisions
// and owns, named "mysql-<fg>-backup-<profile>" (Provisioned).
// +kubebuilder:validation:XValidation:rule="has(self.claimName) != has(self.provisioned)",message="exactly one of claimName or provisioned must be set"
// +kubebuilder:validation:XValidation:rule="has(self.provisioned) == has(oldSelf.provisioned) && (!has(self.provisioned) || (has(self.provisioned.storageClassName) == has(oldSelf.provisioned.storageClassName) && (!has(self.provisioned.storageClassName) || self.provisioned.storageClassName == oldSelf.provisioned.storageClassName) && quantity(self.provisioned.size).compareTo(quantity(oldSelf.provisioned.size)) == 0))",message="pvc.provisioned is immutable and cannot be added or removed: the operator applies it only when it creates the claim"
type PVCStorage struct {
	// ClaimName is the name of an existing PVC to use. Exactly one of
	// ClaimName and Provisioned is set.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 253",message="claimName must be at most 253 bytes (UTF-8)"
	// +optional
	ClaimName string `json:"claimName,omitempty"`

	// Provisioned makes the operator create and own the profile's claim,
	// "mysql-<fg>-backup-<profile>", with this class and size. It is
	// applied only when the claim is created, so it is immutable. Exactly
	// one of ClaimName and Provisioned is set.
	// +optional
	Provisioned *ProvisionedVolumeSpec `json:"provisioned,omitempty"`

	// SubPath inside the PVC to use. Each backup lands under
	// "<subPath>/<mysqlbackup-name>/".
	// A ".." segment is rejected so the path cannot leave the PVC.
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 512",message="subPath must be at most 512 bytes (UTF-8)"
	// +kubebuilder:validation:Pattern=`^/*((?:[^/.][^/]*|\.|\.[^/.][^/]*|\.\.[^/]+)/+)*(?:[^/.][^/]*|\.|\.[^/.][^/]*|\.\.[^/]+)?$`
	// +optional
	SubPath string `json:"subPath,omitempty"`
}

// ProvisionedVolumeSpec is a volume the operator provisions: a claim of
// Size, of class StorageClassName.
type ProvisionedVolumeSpec struct {
	// StorageClassName is the StorageClass of the claim. Unset uses the
	// cluster's default StorageClass.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// Size is the requested size of the claim, such as "50Gi".
	// +required
	Size StorageQuantity `json:"size"`
}

// DumpOptions maps onto the options argument of util.dumpInstance().
type DumpOptions struct {
	// Threads is the number of parallel dump threads.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	// +optional
	Threads int32 `json:"threads,omitempty"`

	// BytesPerChunk is the approximate chunk size: a byte count with an
	// optional k, K, M or G suffix, as mysqlsh reads it (e.g. "64M",
	// "512k"). mysqlsh refuses a lowercase m or g.
	// +kubebuilder:default="64M"
	// +kubebuilder:validation:MaxLength=20
	// +kubebuilder:validation:Pattern=`^[0-9]+[kKMG]?$`
	// +optional
	BytesPerChunk string `json:"bytesPerChunk,omitempty"`

	// Compression algorithm used by the dump utility.
	// +kubebuilder:default="zstd"
	// +optional
	Compression DumpCompression `json:"compression,omitempty"`

	// ExcludeSchemas skipped by the dump.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=64
	ExcludeSchemas []string `json:"excludeSchemas,omitempty"`

	// IncludeSchemas, if non-empty, restricts the dump to these schemas.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=64
	IncludeSchemas []string `json:"includeSchemas,omitempty"`

	// Consistent toggles consistent (locking) dumps. Requires BACKUP_ADMIN
	// on the MySQL user.
	// +kubebuilder:default=true
	// +optional
	Consistent *bool `json:"consistent,omitempty"`

	// Ocimds enables MySQL HeatWave Service compatibility checks.
	// +kubebuilder:default=false
	// +optional
	Ocimds *bool `json:"ocimds,omitempty"`
}

// DumpCompression is the util.dumpInstance() compression option. The
// values are mysqlsh's own and are passed to it verbatim.
// +kubebuilder:validation:Enum=none;gzip;zstd
type DumpCompression string

const (
	// DumpCompressionNone stores the dump chunks uncompressed.
	DumpCompressionNone DumpCompression = "none"
	// DumpCompressionGzip compresses the dump chunks with gzip.
	DumpCompressionGzip DumpCompression = "gzip"
	// DumpCompressionZstd compresses the dump chunks with zstd.
	DumpCompressionZstd DumpCompression = "zstd"
)

// BackupSchedule drives recurring backups via a Kubernetes CronJob owned
// by the operator. Each schedule references a named profile. The CronJob's
// pod creates a MysqlBackup CR via the "bloodraven trigger-backup"
// subcommand; the MysqlBackupReconciler then materializes the backup Job.
type BackupSchedule struct {
	// Name uniquely identifies this schedule within the failover group.
	// It is used as a label value, hence the 63-character bound.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// ProfileName references a BackupProfile by name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +required
	ProfileName string `json:"profileName"`

	// Schedule is a standard cron expression (5 fields). Evaluation uses
	// TimeZone below, not kube-controller-manager's local clock.
	// +kubebuilder:validation:MinLength=1
	// +required
	Schedule string `json:"schedule"`

	// TimeZone is an IANA timezone name ("America/Los_Angeles",
	// "Europe/Berlin", ...) used when interpreting the Schedule cron
	// expression. Defaults to "Etc/UTC".
	//
	// We set this explicitly because the kube-controller-manager local
	// timezone is environment-dependent and unreliable for backups: two
	// clusters running the same manifest can end up firing the same
	// cron at different wall-clock times. Per-schedule TimeZone makes
	// backups reproducible regardless of where the control plane runs.
	// +kubebuilder:default="Etc/UTC"
	// +optional
	TimeZone string `json:"timeZone,omitempty"`

	// Suspend pauses this schedule without deleting the CronJob.
	// +kubebuilder:default=false
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// StartingDeadlineSeconds is forwarded to the CronJob spec.
	// +kubebuilder:validation:Minimum=0
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// ConcurrencyPolicy is forwarded to the CronJob spec.
	// +kubebuilder:default=Forbid
	// +kubebuilder:validation:Enum=Allow;Forbid;Replace
	// +optional
	ConcurrencyPolicy batchv1.ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// SuccessfulJobsHistoryLimit is forwarded to the CronJob spec.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +optional
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// FailedJobsHistoryLimit is forwarded to the CronJob spec.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +optional
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

// InitFromBackupSpec configures a one-shot restore-on-first-boot. When set,
// the operator runs a restore Job against the initial primary site before
// normal bootstrap is considered complete. It is intended for recovering a
// brand-new failover group from an existing backup. Once the restore has
// completed successfully (fg.status.restore.phase == Succeeded), subsequent
// reconciles skip the restore even if this field is still set.
// +kubebuilder:validation:XValidation:rule="!has(self.pointInTime) || has(self.pointInTime.archiveProfileName)",message="pointInTime.archiveProfileName is required: name the backup profile whose PITR archive the restore reads"
type InitFromBackupSpec struct {
	// Source is a tagged reference to the artifact to restore.
	// +kubebuilder:validation:XValidation:rule="(has(self.backupRef) ? 1 : 0) + (has(self.s3) ? 1 : 0) + (has(self.pvc) ? 1 : 0) == 1",message="exactly one of backupRef, s3, or pvc must be set"
	// +required
	Source RestoreSource `json:"source"`

	// LoadOptions are forwarded to util.loadDump(). Omit for sane defaults.
	// +optional
	LoadOptions *LoadOptions `json:"loadOptions,omitempty"`

	// PointInTime, when set, enables post-load binlog replay to recover
	// the restored instance to a specific target timestamp. Requires
	// the source backup's failover group to have had spec.backup.pitr
	// enabled at the time of the full dump. Omit for a plain dump-only
	// restore.
	// +optional
	PointInTime *PointInTimeSpec `json:"pointInTime,omitempty"`

	// Decryption, when set, provides the passphrase that decrypts the
	// source dump. The dump key is resolved in this order: Decryption;
	// else, for a backupRef source, the dump-key snapshot that backup
	// recorded (status.artifactStorage.passphraseSecretKeyRef); else the restore
	// is refused (PITRBackupDumpKeyMissing). The profile's live
	// passphrase Secret is never used: it may hold other bytes since the
	// backup ran. For direct S3/PVC sources of encrypted dumps Decryption
	// is required. Archived binlogs a point-in-time restore replays are
	// decrypted with the key snapshot of the archive's read binding
	// (status.pitrArchiveBindings), never with Decryption.
	// +optional
	Decryption *BackupDecryptionSpec `json:"decryption,omitempty"`
}

// RestoreSource is the tagged-union selector for the restore source.
type RestoreSource struct {
	// BackupRef restores from a previously-completed MysqlBackup in
	// the same namespace. Its status.artifactStorage is resolved
	// automatically.
	// +optional
	BackupRef *LocalMysqlBackupRef `json:"backupRef,omitempty"`

	// S3 restores directly from an S3 URL. Requires credentialsSecretName.
	// +optional
	S3 *S3Storage `json:"s3,omitempty"`

	// PVC restores from an existing PVC. Restore only supports referencing
	// a pre-created claim by name; provisioning fields are not supported here.
	// +optional
	PVC *InitFromBackupPVCSource `json:"pvc,omitempty"`
}

// InitFromBackupPVCSource identifies an existing PVC to mount as the restore
// source. Unlike PVCStorage, this restore-specific type does not support
// operator provisioning and requires claimName to be set.
type InitFromBackupPVCSource struct {
	// ClaimName is the name of an existing PersistentVolumeClaim in the same
	// namespace to use as the restore source.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +required
	ClaimName string `json:"claimName"`

	// SubPath inside the PVC where the dump is located. Optional.
	// A ".." segment is rejected so the path cannot leave the PVC.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^/*((?:[^/.][^/]*|\.|\.[^/.][^/]*|\.\.[^/]+)/+)*(?:[^/.][^/]*|\.|\.[^/.][^/]*|\.\.[^/]+)?$`
	SubPath string `json:"subPath,omitempty"`
}

// LoadOptions maps onto the options argument of util.loadDump().
type LoadOptions struct {
	// Threads is the number of parallel load threads.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	// +optional
	Threads int32 `json:"threads,omitempty"`

	// SkipBinlog disables binary logging during the load to speed it up.
	// For in-place restores that must replicate to the peer site via
	// normal MySQL replication (per-schema in-place restore), callers
	// should set this to false so the load's DDL/DMLs flow through the
	// primary's binlog to the replica.
	// +kubebuilder:default=true
	// +optional
	SkipBinlog *bool `json:"skipBinlog,omitempty"`

	// LoadIndexes controls whether indexes are rebuilt during load.
	// +kubebuilder:default=true
	// +optional
	LoadIndexes *bool `json:"loadIndexes,omitempty"`

	// IncludeSchemas, if non-empty, restricts the load to these schemas.
	// Useful for carving a single tenant out of a full-instance dump
	// (e.g. migrating one client schema into its own failover group).
	// Forwarded to util.loadDump() as includeSchemas.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=64
	IncludeSchemas []string `json:"includeSchemas,omitempty"`

	// ExcludeSchemas, if non-empty, skips these schemas during load.
	// Forwarded to util.loadDump() as excludeSchemas.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=64
	ExcludeSchemas []string `json:"excludeSchemas,omitempty"`
}

// BackupScheduleStatus is the per-schedule rollup exposed on
// MysqlFailoverGroupStatus.
type BackupScheduleStatus struct {
	// Name of the schedule this entry describes.
	// +required
	Name string `json:"name"`

	// CronJobName is the name of the Kubernetes CronJob that materializes
	// this schedule.
	// +optional
	CronJobName string `json:"cronJobName,omitempty"`

	// LastScheduleTime is the most recent time this schedule created a
	// MysqlBackup CR.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// LastSuccessfulTime is the most recent time a MysqlBackup created by
	// this schedule reached the Succeeded phase.
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// LastBackupName is the name of the most recent MysqlBackup created
	// by this schedule.
	// +optional
	LastBackupName string `json:"lastBackupName,omitempty"`

	// LastBackupPhase is the phase of that most-recent MysqlBackup.
	// +optional
	LastBackupPhase BackupPhase `json:"lastBackupPhase,omitempty"`

	// LastSuccessfulBackupName is the name of the most recent Succeeded
	// MysqlBackup created by this schedule. Lets dashboards link to the
	// last known-good CR even after a subsequent failure.
	// +optional
	LastSuccessfulBackupName string `json:"lastSuccessfulBackupName,omitempty"`

	// LastRetryAttempt is the attempt counter of the latest CR (or the
	// latest retry created off it). 0 or 1 for the original attempt.
	// +optional
	LastRetryAttempt int32 `json:"lastRetryAttempt,omitempty"`

	// NextRetryTime, when set, is the earliest time the operator will
	// create a retry CR for a Failed latest attempt. Used by the
	// scheduler loop to wake up at exactly the right moment.
	// +optional
	NextRetryTime *metav1.Time `json:"nextRetryTime,omitempty"`

	// RetryConflict names the existing MysqlBackup whose lineage blocks
	// the next retry. Cleared when the conflict is no longer observed.
	// +optional
	RetryConflict string `json:"retryConflict,omitempty"`
}

// RestoreInPlaceSpec configures a re-runnable in-place restore that loads
// a previous dump into the currently-active primary WITHOUT a
// teardown/rename cycle. Unlike initFromBackup (which is one-shot and
// runs before normal bootstrap), an in-place restore operates against a
// live cluster: it fences writes, wipes (or drops) target schemas,
// loads the dump into the running mysqld, optionally replays binlogs
// to a target timestamp, and resumes.
//
// Two modes are supported, selected by loadOptions.includeSchemas:
//
//   - Full-instance restore (includeSchemas empty or unset): drops all
//     user schemas, re-loads from the dump, and re-clones the peers
//     via CLONE INSTANCE after the load completes. The primary Service
//     is fenced and the primary runs with offline_mode=ON for the
//     duration, so only CONNECTION_ADMIN/SUPER accounts (the operator,
//     the sidecar, the restore Job) can connect.
//
//   - Per-schema restore (exactly one entry in includeSchemas): drops
//     the named schema only, re-loads just that schema, and relies on
//     normal MySQL replication to propagate the DROP+load to the
//     replica. The primary Service remains up — other tenants keep
//     writing — and it is the caller's responsibility to put the
//     affected tenant into application-level maintenance mode.
//
// WARNING: PITR binlog replay via mysqlbinlog --database=<x> is not
// airtight for apps that issue cross-schema statements (e.g.
// `INSERT INTO a.t SELECT ... FROM b.t`) — the filter matches on the
// session's current default database at log time, not on the schemas
// actually referenced. For well-isolated multi-tenant schemas this is
// fine; for cross-schema apps, use a full-instance restore instead.
// +kubebuilder:validation:XValidation:rule="!has(self.loadOptions) || !has(self.loadOptions.includeSchemas) || size(self.loadOptions.includeSchemas) <= 1",message="restoreInPlace.loadOptions.includeSchemas supports at most one schema; use a full-instance restore instead"
type RestoreInPlaceSpec struct {
	// Confirm is a required anti-fat-finger token that gates the
	// destructive restore. Must be an RFC 3339 timestamp (e.g.
	// "2026-04-17T14:32:00Z"). The operator refuses to run an in-place
	// restore unless Confirm parses and is strictly greater than the
	// timestamp recorded in status.restoreInPlace.confirmTokenUsed.
	// This gives programmatic callers a simple "just send now()" idiom
	// while also protecting against replay (an older manifest applied
	// accidentally will not re-trigger a restore). Admission requires an
	// upper-case "T" and "Z", an offset within -23:59..+23:59 and a
	// digits-only fraction. The value is kept as written: the status
	// tokens compare it byte for byte.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Format=date-time
	// +kubebuilder:validation:Pattern=`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`
	// +required
	Confirm string `json:"confirm"`

	// Source is a tagged reference to the artifact to restore.
	// +kubebuilder:validation:XValidation:rule="(has(self.backupRef) ? 1 : 0) + (has(self.s3) ? 1 : 0) + (has(self.pvc) ? 1 : 0) == 1",message="exactly one of backupRef, s3, or pvc must be set"
	// +required
	Source RestoreSource `json:"source"`

	// LoadOptions are forwarded to util.loadDump(). In-place restores
	// require includeSchemas to be either empty (full instance) or
	// exactly one entry (single schema). For per-schema restores the
	// operator also forces skipBinlog to false so the DROP+load flows
	// through the primary's binlog to the replica.
	// +optional
	LoadOptions *LoadOptions `json:"loadOptions,omitempty"`

	// Decryption, when set, provides the passphrase that decrypts the
	// source dump, resolved as InitFromBackupSpec.Decryption is:
	// Decryption; else, for a backupRef source, the dump-key
	// snapshot that backup recorded
	// (status.artifactStorage.passphraseSecretKeyRef); else the restore is
	// refused (PITRBackupDumpKeyMissing). The profile's live passphrase
	// Secret is never used. For direct S3/PVC sources of encrypted dumps
	// Decryption is required. Archived binlogs are decrypted with the key
	// snapshot of the archive's read binding (status.pitrArchiveBindings).
	// +optional
	Decryption *BackupDecryptionSpec `json:"decryption,omitempty"`
}

// RestoreInPlacePhase enumerates the discrete states an in-place restore
// progresses through. The controller advances the state machine by one
// step per reconcile so that operator restarts land on a well-defined
// observable state.
//
// For full-instance restores the post-restore replica reclone is not a
// dedicated phase. When the restore is accepted the operator records
// every peer site in status.restoreReclones; after the load succeeds it
// reclones them one at a time from the restored primary through the
// reclone annotation (see internal/controller/topology.go:checkReclone).
// The in-place restore is considered Succeeded once the primary has the
// fresh data; the peers are recloned afterwards, and until then they are
// excluded from automatic promotion and the reader Services.
//
// A terminal status is kept, even after spec.restoreInPlace is removed, so
// its confirmTokenUsed keeps blocking replays.
// +kubebuilder:validation:Enum=Preflight;Fencing;Restoring;Resuming;Succeeded;Failed
type RestoreInPlacePhase string

const (
	// RestoreInPlacePreflight means the operator has accepted the
	// confirmation token and is validating preconditions (active site
	// writable, deployment rolled out, source artifact resolvable).
	RestoreInPlacePreflight RestoreInPlacePhase = "Preflight"

	// RestoreInPlaceFencing means the operator is applying the
	// pre-restore fence: freezing the topology manager and, for
	// full-instance restores, stripping the primary role label (the
	// -primary Service stops taking new connections) and setting
	// offline_mode=ON on the primary, which disconnects and refuses every
	// session without CONNECTION_ADMIN or SUPER. A full-instance restore
	// also stops the replication receiver (STOP REPLICA IO_THREAD) on every
	// peer in status.restoreReclones, so the restore's DROP and load never
	// reach them. The restore Job is created only once no client session
	// from before the fence remains and every peer's receiver is verified
	// stopped; a peer that cannot be stopped within two minutes ends the
	// run Failed without a Job.
	RestoreInPlaceFencing RestoreInPlacePhase = "Fencing"

	// RestoreInPlaceRestoring means the restore Job is running the
	// loadDump (and optional PITR replay) against the active primary.
	RestoreInPlaceRestoring RestoreInPlacePhase = "Restoring"

	// RestoreInPlaceResuming means the operator is lifting the fence:
	// restoring the primary role label, releasing the peer reclone queue
	// in status.restoreReclones (full-instance only), and unfreezing the
	// topology manager.
	RestoreInPlaceResuming RestoreInPlacePhase = "Resuming"

	// RestoreInPlaceSucceeded is the terminal success state.
	RestoreInPlaceSucceeded RestoreInPlacePhase = "Succeeded"

	// RestoreInPlaceFailed is the terminal failure state. The operator
	// will not retry automatically; bump spec.restoreInPlace.confirm to
	// a newer timestamp to re-arm.
	RestoreInPlaceFailed RestoreInPlacePhase = "Failed"
)

// RestoreReclonesPhase is the lifecycle of a restore-owned reclone queue.
// +kubebuilder:validation:Enum=Armed;Draining;Held
type RestoreReclonesPhase string

const (
	// RestoreReclonesArmed means the owning full-instance restore was
	// accepted and is still running. Nothing is recloned yet; the queued
	// peers are already excluded from promotion and the reader Services,
	// and their replication receivers are stopped before the restore Job
	// is created.
	RestoreReclonesArmed RestoreReclonesPhase = "Armed"

	// RestoreReclonesDraining means the owning restore succeeded and the
	// queued peers are being recloned from the restored primary, one at
	// a time. A queued peer's replication is started again only by its
	// own reclone.
	RestoreReclonesDraining RestoreReclonesPhase = "Draining"

	// RestoreReclonesHeld means the queue's sites still need recloning
	// but the primary is no trusted donor: a full-instance restore ran its
	// Job on it, failed, and the primary's gtid_executed moved past the
	// value recorded when the peers were stopped (or could not be
	// compared). The sites keep their replication receivers stopped, so
	// they keep the data they had before that restore. They stay excluded
	// from promotion and the reader Services and nothing is cloned or
	// replicated into them until an administrator sets the phase to
	// Draining (the primary's data is good again), a new full-instance
	// restore succeeds, or the queue is removed (which resumes their
	// replication from the current primary).
	RestoreReclonesHeld RestoreReclonesPhase = "Held"
)

// RestoreReclonesStatus is the queue of peer sites a full-instance
// in-place restore must reclone from the restored primary. The restore
// fixes the recipient set when it is accepted, before any destructive
// work. A site leaves Pending only after its reclone completes
// successfully; a failed reclone is retried. The queue is cleared once
// every peer is recloned. If the owning restore fails it is discarded
// when the restore never changed the primary (no Job, or gtid_executed
// still at FencedGtidExecuted), and put Held otherwise.
type RestoreReclonesStatus struct {
	// ConfirmToken is the spec.restoreInPlace.confirm value of the
	// restore that owns this queue.
	// +required
	ConfirmToken string `json:"confirmToken"`

	// Phase is Armed while the owning restore runs and Draining after it
	// succeeded. Held means a restore ran on the primary and failed, so
	// nothing is cloned from it.
	// +required
	Phase RestoreReclonesPhase `json:"phase"`

	// FencedGtidExecuted is the primary's @@GLOBAL.gtid_executed when the
	// owning restore stopped the peers' replication receivers, after the
	// offline_mode fence evicted application sessions. If the restore
	// fails and the primary still reports this set, the restore changed
	// nothing that replication could carry, so the queue is discarded and
	// the peers resume replicating instead of being held.
	// +optional
	FencedGtidExecuted string `json:"fencedGtidExecuted,omitempty"`

	// Pending lists the peer sites still to be recloned, in the order
	// they are recloned (PrimaryCandidate sites first). Pending sites
	// are never promoted automatically and are removed from the -replica
	// and read-only Services.
	// +optional
	// +listType=atomic
	Pending []string `json:"pending,omitempty"`

	// Requested is the pending site whose reclone the operator has
	// requested through the reclone annotation and not yet seen finish.
	// +optional
	Requested string `json:"requested,omitempty"`

	// RequestTime is when Requested was set. A request whose clone has
	// not started within a few minutes counts as a failed attempt, so one
	// site that cannot be cloned does not hold up the others.
	// +optional
	RequestTime *metav1.Time `json:"requestTime,omitempty"`

	// Failures holds the latest failed reclone attempt of each pending
	// site that has one. A site is retried after a short backoff; other
	// pending sites are recloned in the meantime. Keyed by site.
	// +optional
	// +listType=map
	// +listMapKey=site
	Failures []RestoreRecloneFailure `json:"failures,omitempty"`

	// Superseded is the queue of an earlier restore that this one
	// replaced when it was accepted. If this restore does not succeed,
	// the earlier queue is put back, because its sites still hold data
	// from before the earlier restore. It drains again only if this
	// restore's Job was never created; otherwise it is put back Held.
	// +optional
	Superseded *RestoreReclonesCarryover `json:"superseded,omitempty"`
}

// RestoreRecloneFailure is the latest failed reclone attempt of one site.
type RestoreRecloneFailure struct {
	// Site is the pending site whose reclone failed: the key of
	// failures.
	// +required
	// +kubebuilder:validation:MinLength=1
	Site string `json:"site"`

	// Message describes the failure.
	// +required
	Message string `json:"message"`

	// Time is when the failure was recorded.
	// +required
	Time metav1.Time `json:"time"`
}

// RestoreReclonesCarryover is the part of an earlier restore's reclone
// queue a later restore keeps until it succeeds.
type RestoreReclonesCarryover struct {
	// ConfirmToken is the earlier restore's confirm token.
	// +required
	ConfirmToken string `json:"confirmToken"`

	// Pending lists the earlier restore's sites not yet recloned.
	// +optional
	// +listType=atomic
	Pending []string `json:"pending,omitempty"`

	// Held is true when the earlier queue was already Held: the primary
	// stopped being a trusted donor for it before this restore.
	// +optional
	Held bool `json:"held,omitempty"`
}

// RestoreOfflineFenceStatus records the database fence a full-instance
// in-place restore holds on its target primary: @@GLOBAL.offline_mode=ON,
// which disconnects and refuses every session without CONNECTION_ADMIN or
// SUPER. The record is written before the operator changes offline_mode,
// so a restarted operator re-applies the fence while the restore runs and
// puts the prior value back once the restore has finished.
type RestoreOfflineFenceStatus struct {
	// Site is the primary the fence applies to.
	// +required
	Site string `json:"site"`

	// ConfirmToken is the spec.restoreInPlace.confirm value of the
	// restore that owns the fence.
	// +required
	ConfirmToken string `json:"confirmToken"`

	// RunID is status.restoreInPlace.runID of the run that owns the
	// fence. A request re-applied with the same confirm after a
	// cancellation is a new run, so it does not inherit a fence the
	// earlier run already released.
	// +optional
	RunID string `json:"runID,omitempty"`

	// PriorOfflineMode is @@GLOBAL.offline_mode on Site before the
	// operator enabled it; it is restored on release. Unset until read.
	// +optional
	PriorOfflineMode *bool `json:"priorOfflineMode,omitempty"`

	// SessionsEvicted is true once the operator verified that no client
	// session from before the fence remains on Site. Destructive work
	// starts only after that.
	// +optional
	SessionsEvicted bool `json:"sessionsEvicted,omitempty"`
}

// RestoreScopeType is what an in-place restore restores.
// +kubebuilder:validation:Enum=Full;Schema
type RestoreScopeType string

const (
	// RestoreScopeFull restores the whole instance.
	RestoreScopeFull RestoreScopeType = "Full"
	// RestoreScopeSchema restores one schema, named by RestoreScope.Schema.
	RestoreScopeSchema RestoreScopeType = "Schema"
)

// RestoreScope is the scope of an accepted in-place restore, inferred from
// spec.restoreInPlace.loadOptions.includeSchemas: one schema is a Schema
// restore, none is a Full restore.
// +kubebuilder:validation:XValidation:rule="self.type == 'Schema' ? has(self.schema) : !has(self.schema)",message="schema is required when type is Schema, and forbidden otherwise"
type RestoreScope struct {
	// Type is Full or Schema.
	// +required
	Type RestoreScopeType `json:"type"`

	// Schema is the restored schema when Type is Schema.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	Schema string `json:"schema,omitempty"`
}

// RestoreInPlaceStatus tracks an in-flight or completed in-place
// restore.
type RestoreInPlaceStatus struct {
	// Phase of the in-place restore.
	// +optional
	Phase RestoreInPlacePhase `json:"phase,omitempty"`

	// JobName is the name of the batchv1.Job performing the load.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// JobCreating is set, with RunConfirm bound, before the restore Job
	// is created, and the Job is created only once that write succeeded.
	// From then on the run may have executed even if JobName was never
	// recorded (a crash or a lost status write right after Create): a
	// Job that disappears counts as lost (its confirm is consumed), not
	// as a run that never started. It is kept on the terminal status.
	// +optional
	JobCreating bool `json:"jobCreating,omitempty"`

	// TargetSite is the site that received the load (always the active
	// primary at the moment of preflight).
	// +optional
	TargetSite string `json:"targetSite,omitempty"`

	// Scope is what the run restores: the whole instance, or one schema.
	// It is pinned when the run is accepted.
	// +optional
	Scope *RestoreScope `json:"scope,omitempty"`

	// ConfirmTokenUsed records the spec.restoreInPlace.confirm value of
	// the most recent executed restore. Subsequent reconciles ignore
	// any spec.confirm <= this value; a new restore requires a strictly
	// greater RFC 3339 timestamp. It carries over unchanged while a newer
	// request is running and when that request is rejected or cancelled,
	// and it is kept when spec.restoreInPlace is removed. A run whose Job
	// disappeared after spec.restoreInPlace was removed (JobLost) may
	// have executed, so it records that run's confirm (RunConfirm).
	// +optional
	ConfirmTokenUsed string `json:"confirmTokenUsed,omitempty"`

	// RunConfirm is the spec.restoreInPlace.confirm of the run in
	// flight, recorded when it enters Preflight. A confirm that advances
	// before the restore Job is created is bound into it first, so it is
	// always the confirm the Job runs with. It identifies the run after
	// spec.restoreInPlace is removed. Empty once the run is terminal;
	// ConfirmTokenUsed then records what executed.
	// +optional
	RunConfirm string `json:"runConfirm,omitempty"`

	// RunID identifies the run in flight, set with RunConfirm. A request
	// that is cancelled or rejected and then applied again with the same
	// confirm starts a new run with a new RunID. Empty once the run is
	// terminal.
	// +optional
	RunID string `json:"runID,omitempty"`

	// StartTime is when the restore entered Preflight.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the restore reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Provenance records what the restore loaded and what the target held
	// afterwards. An in-place restore never replays binary logs, so its
	// provenance.pointInTime stays unset.
	// +optional
	Provenance *RestoreProvenance `json:"provenance,omitempty"`

	// Message is a human-readable status line.
	// +optional
	Message string `json:"message,omitempty"`
}

// RestoreProvenance is the provenance of a restore: the backup it loaded,
// the target after the load, and the point-in-time replay's result.
type RestoreProvenance struct {
	// Source is what the referenced MysqlBackup recorded about its
	// artifact. Absent when the source was not a MysqlBackup.
	// +optional
	Source *RestoreSourceProvenance `json:"source,omitempty"`

	// Target is the target's state after the load and any replay.
	// +optional
	Target *RestoreTargetProvenance `json:"target,omitempty"`

	// PointInTime is the certified outcome of the point-in-time replay,
	// read from the replay's verdict. Unset when no replay ran.
	// +optional
	PointInTime *PointInTimeResult `json:"pointInTime,omitempty"`
}

// RestoreSourceProvenance is what the source MysqlBackup recorded about
// its artifact.
type RestoreSourceProvenance struct {
	// SizeBytes is the artifact size in bytes. Unset when unknown.
	// +optional
	// +kubebuilder:validation:Minimum=0
	SizeBytes *int64 `json:"sizeBytes,omitempty"`

	// GtidExecuted is the GTID set the backup recorded.
	// +optional
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// BinlogPosition is the binary-log position the backup recorded.
	// +optional
	BinlogPosition *BinlogCoordinate `json:"binlogPosition,omitempty"`
}

// RestoreTargetProvenance is the restore target's state after the load and
// any point-in-time replay.
type RestoreTargetProvenance struct {
	// GtidExecuted is @@global.gtid_executed on the target.
	// +optional
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// BinlogPosition is the target's binary-log position.
	// +optional
	BinlogPosition *BinlogCoordinate `json:"binlogPosition,omitempty"`
}

// BinlogCoordinate is a position in a server's binary log.
type BinlogCoordinate struct {
	// File is the binary-log file name, e.g. "mysql-bin.000412".
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	File string `json:"file"`

	// Position is the byte offset within File.
	// +required
	// +kubebuilder:validation:Minimum=0
	Position int64 `json:"position"`
}

// RestoreStatus tracks an in-flight or completed initFromBackup run.
type RestoreStatus struct {
	// JobUID identifies the producing Job across retries using the same name.
	// +optional
	JobUID string `json:"jobUID,omitempty"`
	// Reason is the failure token: the PITR catalog token found in the
	// failing step's termination message, for example
	// PITRArchiveDivergentHistory. Absent on success and for a failure
	// that carries no token.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`
	Reason PITRToken `json:"reason,omitempty"`

	// Phase of the restore Job.
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// JobName is the name of the batchv1.Job performing the restore.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// TargetSite is the site name whose MySQL instance is being populated.
	// +optional
	TargetSite string `json:"targetSite,omitempty"`

	// StartTime is when the restore Job was created.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the restore Job reached a terminal state.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Provenance records what the restore loaded, what the target held
	// afterwards and, with spec.initFromBackup.pointInTime, the certified
	// outcome of the point-in-time replay.
	// +optional
	Provenance *RestoreProvenance `json:"provenance,omitempty"`

	// Message is a human-readable status message.
	// +optional
	Message string `json:"message,omitempty"`
}
