package v1alpha1

import (
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
const DefaultBackupImage = "container-registry.oracle.com/mysql/community-server:9.6"

// BackupSpec is the top-level backup configuration embedded in
// MysqlFailoverGroupSpec as spec.backup. All fields are optional; omitting
// spec.backup entirely disables backups for this failover group.
type BackupSpec struct {
	// Image is the container image containing mysqlsh. It must include
	// util.dumpInstance() and util.loadDump() with native S3 support
	// (mysqlsh 8.0.32+). Defaults to DefaultBackupImage; production
	// deployments should always pin a full major.minor tag here and
	// avoid floating tags.
	// +kubebuilder:default="container-registry.oracle.com/mysql/community-server:9.6"
	Image string `json:"image,omitempty"`

	// ImagePullSecrets for the backup image.
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Profiles are reusable, named backup configurations. Schedules and
	// ad-hoc MysqlBackup CRs reference profiles by name.
	// +optional
	Profiles []BackupProfile `json:"profiles,omitempty"`

	// Schedules describe cron-driven recurring backups, each referencing
	// a profile by name. Each schedule becomes a Kubernetes CronJob owned
	// by this failover group.
	// +optional
	Schedules []BackupSchedule `json:"schedules,omitempty"`

	// MaxLagSecondsForSource is the threshold for replica-first source
	// selection. The backup reconciler prefers the replica site as the
	// dump source, but only if secondsBehindSource is at or below this
	// value. Above it, the reconciler falls back to the primary.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	MaxLagSecondsForSource int64 `json:"maxLagSecondsForSource,omitempty"`

	// Resources for the backup Job's mysqlsh container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// ActiveDeadlineSeconds caps the wall-clock duration of a single
	// backup Job. Default: 7200 (2h).
	// +kubebuilder:default=7200
	// +kubebuilder:validation:Minimum=60
	ActiveDeadlineSeconds int64 `json:"activeDeadlineSeconds,omitempty"`

	// BackoffLimit is the Job backoffLimit applied to backup Jobs.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0
	BackoffLimit int32 `json:"backoffLimit,omitempty"`

	// Retry configures operator-level retries for scheduled MysqlBackup
	// CRs that land in Failed. This is independent of Job-level
	// backoffLimit: Job backoff retries the container, whereas this
	// retries the whole CR, producing a new MysqlBackup with its own
	// Job and attempt counter. Omit to disable.
	// +optional
	Retry *BackupRetrySpec `json:"retry,omitempty"`

	// PITR, when true, is a reserved-not-implemented marker. The
	// reconciler emits a warning event on every sync when this is
	// enabled; no PITR logic is wired up today. Left here so the API
	// shape is stable for a future point-in-time recovery feature.
	// +optional
	PITR *bool `json:"pitr,omitempty"`

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
}

// BackupRetrySpec configures operator-level retries for scheduled
// MysqlBackup CRs. MaxAttempts includes the original attempt.
type BackupRetrySpec struct {
	// MaxAttempts is the total number of attempts (including the
	// original). 1 disables retries. Values are clamped to [1, 10].
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	MaxAttempts int32 `json:"maxAttempts,omitempty"`

	// InitialBackoffSeconds is the delay between the first failure and
	// the first retry. Subsequent retries double the backoff until
	// MaxBackoffSeconds.
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	InitialBackoffSeconds int32 `json:"initialBackoffSeconds,omitempty"`

	// MaxBackoffSeconds caps the exponential backoff. Default: 1800 (30m).
	// +kubebuilder:default=1800
	// +kubebuilder:validation:Minimum=1
	MaxBackoffSeconds int32 `json:"maxBackoffSeconds,omitempty"`
}

// BackupProfile is a named, reusable backup configuration.
type BackupProfile struct {
	// Name is the unique profile identifier within this failover group.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// Storage selects and configures the backup destination.
	Storage BackupStorage `json:"storage"`

	// Dump customizes util.dumpInstance() options.
	// +optional
	Dump *DumpOptions `json:"dump,omitempty"`

	// Retention is the legacy shorthand for count-based retention: the
	// maximum number of successful MysqlBackup CRs to keep for this
	// profile. 0 disables count-based pruning. If RetentionPolicy is
	// set it fully overrides this field.
	// +kubebuilder:default=7
	// +kubebuilder:validation:Minimum=0
	Retention int32 `json:"retention,omitempty"`

	// RetentionPolicy is the structured retention configuration. When
	// set it replaces the shorthand Retention field and enables
	// age-based pruning plus a min-keep safety floor.
	// +optional
	RetentionPolicy *RetentionPolicy `json:"retentionPolicy,omitempty"`
}

// RetentionPolicy is the structured retention configuration that replaces
// the legacy single-int Retention shorthand on BackupProfile. All fields
// are optional; a zero value disables the corresponding policy knob. The
// reconciler keeps a successful MysqlBackup iff ANY of the enabled checks
// say "keep":
//
//   - MinKeep: this many newest successes are always kept (safety floor).
//   - Count: keep the newest Count successful CRs.
//   - MaxAgeDays: keep successful CRs with completionTime newer than this.
//
// MinKeep is the critical knob: it prevents a retention sweep from wiping
// the last good backup after a long outage in which every recent attempt
// has failed.
type RetentionPolicy struct {
	// Count is the max number of successful CRs to keep. 0 disables
	// count-based pruning.
	// +kubebuilder:validation:Minimum=0
	Count int32 `json:"count,omitempty"`

	// MaxAgeDays is the maximum age (in days) of a successful CR before
	// it becomes eligible for pruning. 0 disables age-based pruning.
	// +kubebuilder:validation:Minimum=0
	MaxAgeDays int32 `json:"maxAgeDays,omitempty"`

	// MinKeep is a safety floor: this many newest successful CRs are
	// always kept, regardless of Count / MaxAgeDays. Default: 1.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	MinKeep int32 `json:"minKeep,omitempty"`

	// MaxFailedKeep caps the Failed bucket independently of the success
	// retention policy. Default: 10.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	MaxFailedKeep int32 `json:"maxFailedKeep,omitempty"`
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
	Bucket string `json:"bucket"`

	// Prefix is the object-name prefix inside the bucket. Each backup
	// lands under "<prefix>/<mysqlbackup-name>/".
	Prefix string `json:"prefix,omitempty"`

	// Region is the AWS region. Optional for non-AWS S3 stores.
	Region string `json:"region,omitempty"`

	// EndpointURL overrides the S3 endpoint (e.g. MinIO, Ceph, Wasabi).
	// Passed to mysqlsh as s3EndpointOverride.
	EndpointURL string `json:"endpointURL,omitempty"`

	// CredentialsSecret references a Secret whose keys provide AWS
	// credentials to backup and restore Jobs. Expected keys:
	//   AWS_ACCESS_KEY_ID (required)
	//   AWS_SECRET_ACCESS_KEY (required)
	//   AWS_SESSION_TOKEN (optional)
	//   AWS_REGION (optional, overrides .region)
	// +kubebuilder:validation:MinLength=1
	CredentialsSecret string `json:"credentialsSecret"`

	// StorageClass is the S3 storage class (e.g. STANDARD_IA,
	// DEEP_ARCHIVE). Informational; not enforced by the operator.
	StorageClass string `json:"storageClass,omitempty"`
}

// PVCStorage configures a local persistent volume as the backup destination.
// If ClaimName is empty the operator provisions and owns a PVC named
// "mysql-<fg>-backup-<profile>".
type PVCStorage struct {
	// ClaimName is the name of a pre-existing PVC to use. If empty the
	// operator provisions a PVC per profile.
	ClaimName string `json:"claimName,omitempty"`

	// StorageClassName is used when the operator provisions the PVC.
	StorageClassName string `json:"storageClassName,omitempty"`

	// Size is used when the operator provisions the PVC.
	Size resource.Quantity `json:"size,omitempty"`

	// SubPath inside the PVC to use. Each backup lands under
	// "<subPath>/<mysqlbackup-name>/".
	SubPath string `json:"subPath,omitempty"`
}

// DumpOptions maps onto the options argument of util.dumpInstance().
type DumpOptions struct {
	// Threads is the number of parallel dump threads.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	Threads int32 `json:"threads,omitempty"`

	// BytesPerChunk is the approximate chunk size (e.g. "64M").
	// +kubebuilder:default="64M"
	BytesPerChunk string `json:"bytesPerChunk,omitempty"`

	// Compression algorithm used by the dump utility.
	// +kubebuilder:default="zstd"
	// +kubebuilder:validation:Enum=none;gzip;zstd
	Compression string `json:"compression,omitempty"`

	// ExcludeSchemas skipped by the dump.
	ExcludeSchemas []string `json:"excludeSchemas,omitempty"`

	// IncludeSchemas, if non-empty, restricts the dump to these schemas.
	IncludeSchemas []string `json:"includeSchemas,omitempty"`

	// Consistent toggles consistent (locking) dumps. Requires BACKUP_ADMIN
	// on the MySQL user.
	// +kubebuilder:default=true
	Consistent *bool `json:"consistent,omitempty"`

	// Ocimds enables MySQL HeatWave Service compatibility checks.
	// +kubebuilder:default=false
	Ocimds *bool `json:"ocimds,omitempty"`
}

// BackupSchedule drives recurring backups via a Kubernetes CronJob owned
// by the operator. Each schedule references a named profile. The CronJob's
// pod creates a MysqlBackup CR via the "bloodraven trigger-backup"
// subcommand; the MysqlBackupReconciler then materializes the backup Job.
type BackupSchedule struct {
	// Name uniquely identifies this schedule within the failover group.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// ProfileName references a BackupProfile by name.
	// +kubebuilder:validation:MinLength=1
	ProfileName string `json:"profileName"`

	// Schedule is a standard cron expression (5 fields). Evaluation uses
	// TimeZone below, not kube-controller-manager's local clock.
	// +kubebuilder:validation:MinLength=1
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
	TimeZone string `json:"timeZone,omitempty"`

	// Suspend pauses this schedule without deleting the CronJob.
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`

	// StartingDeadlineSeconds is forwarded to the CronJob spec.
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// ConcurrencyPolicy is forwarded to the CronJob spec.
	// +kubebuilder:default=Forbid
	// +kubebuilder:validation:Enum=Allow;Forbid;Replace
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty"`

	// SuccessfulJobsHistoryLimit is forwarded to the CronJob spec.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// FailedJobsHistoryLimit is forwarded to the CronJob spec.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

// InitFromBackupSpec configures a one-shot restore-on-first-boot. When set,
// the operator runs a restore Job against the initial primary site before
// normal bootstrap is considered complete. It is intended for recovering a
// brand-new failover group from an existing backup. Once the restore has
// completed successfully (fg.status.restore.phase == Succeeded), subsequent
// reconciles skip the restore even if this field is still set.
type InitFromBackupSpec struct {
	// Source is a tagged reference to the artifact to restore.
	// +kubebuilder:validation:XValidation:rule="(has(self.mysqlBackupRef) ? 1 : 0) + (has(self.s3) ? 1 : 0) + (has(self.pvc) ? 1 : 0) == 1",message="exactly one of mysqlBackupRef, s3, or pvc must be set"
	Source InitFromBackupSource `json:"source"`

	// LoadOptions are forwarded to util.loadDump(). Omit for sane defaults.
	// +optional
	LoadOptions *LoadOptions `json:"loadOptions,omitempty"`
}

// InitFromBackupSource is the tagged-union selector for the restore source.
type InitFromBackupSource struct {
	// MysqlBackupRef restores from a previously-completed MysqlBackup in
	// the same namespace. Its status.location is resolved automatically.
	// +optional
	MysqlBackupRef *corev1.LocalObjectReference `json:"mysqlBackupRef,omitempty"`

	// S3 restores directly from an S3 URL. Requires credentialsSecret.
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
	ClaimName string `json:"claimName"`

	// SubPath inside the PVC where the dump is located. Optional.
	// +optional
	SubPath string `json:"subPath,omitempty"`
}

// LoadOptions maps onto the options argument of util.loadDump().
type LoadOptions struct {
	// Threads is the number of parallel load threads.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	Threads int32 `json:"threads,omitempty"`

	// ResetProgress clears previous loadDump progress tracking.
	// +kubebuilder:default=true
	ResetProgress *bool `json:"resetProgress,omitempty"`

	// SkipBinlog disables binary logging during the load to speed it up.
	// +kubebuilder:default=true
	SkipBinlog *bool `json:"skipBinlog,omitempty"`

	// LoadIndexes controls whether indexes are rebuilt during load.
	// +kubebuilder:default=true
	LoadIndexes *bool `json:"loadIndexes,omitempty"`
}

// BackupScheduleStatus is the per-schedule rollup exposed on
// MysqlFailoverGroupStatus.
type BackupScheduleStatus struct {
	// Name of the schedule this entry describes.
	Name string `json:"name"`

	// CronJobName is the name of the Kubernetes CronJob that materializes
	// this schedule.
	CronJobName string `json:"cronJobName,omitempty"`

	// LastScheduleTime is the most recent time this schedule created a
	// MysqlBackup CR.
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// LastSuccessfulTime is the most recent time a MysqlBackup created by
	// this schedule reached the Succeeded phase.
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// LastBackupName is the name of the most recent MysqlBackup created
	// by this schedule.
	LastBackupName string `json:"lastBackupName,omitempty"`

	// LastBackupPhase is the phase of that most-recent MysqlBackup.
	LastBackupPhase string `json:"lastBackupPhase,omitempty"`

	// LastSuccessfulBackupName is the name of the most recent Succeeded
	// MysqlBackup created by this schedule. Lets dashboards link to the
	// last known-good CR even after a subsequent failure.
	LastSuccessfulBackupName string `json:"lastSuccessfulBackupName,omitempty"`

	// LastRetryAttempt is the attempt counter of the latest CR (or the
	// latest retry created off it). 0 or 1 for the original attempt.
	LastRetryAttempt int32 `json:"lastRetryAttempt,omitempty"`

	// NextRetryTime, when set, is the earliest time the operator will
	// create a retry CR for a Failed latest attempt. Used by the
	// scheduler loop to wake up at exactly the right moment.
	NextRetryTime *metav1.Time `json:"nextRetryTime,omitempty"`
}

// RestoreStatus tracks an in-flight or completed initFromBackup run.
type RestoreStatus struct {
	// Phase of the restore Job.
	Phase BackupPhase `json:"phase,omitempty"`

	// JobName is the name of the batchv1.Job performing the restore.
	JobName string `json:"jobName,omitempty"`

	// TargetSite is the site name whose MySQL instance is being populated.
	TargetSite string `json:"targetSite,omitempty"`

	// StartTime is when the restore Job was created.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the restore Job reached a terminal state.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message is a human-readable status message.
	Message string `json:"message,omitempty"`
}
