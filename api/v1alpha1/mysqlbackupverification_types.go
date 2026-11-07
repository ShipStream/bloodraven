package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.failoverGroupRef.name`
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.profileName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.status.backupRef.name`,priority=1
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="Completed",type=date,JSONPath=`.status.completionTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MysqlBackupVerification is the Schema for the mysqlbackupverifications API.
// A verification restores a MysqlBackup artifact into an ephemeral,
// throwaway MySQL instance to prove the backup can actually be loaded.
// Unverified backups are schrödinger backups — the CR + its
// bloodraven_backup_verified_timestamp_seconds gauge give operators a
// concrete signal that recent backups are restorable.
//
// The ephemeral mysqld runs inside the verification Job's Pod on an
// emptyDir datadir, with a derived credentials Secret for the restore
// script. No Service or PersistentVolumeClaim is created; the Pod binds
// 127.0.0.1 only and is not reachable from outside its own network
// namespace. On success all artifacts are cleaned up; on failure
// KeepOnFailure (default true) leaves the Job and its Pod in place for
// inspection until the retention sweep reclaims them.
type MysqlBackupVerification struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec MysqlBackupVerificationSpec `json:"spec"`
	// +optional
	Status MysqlBackupVerificationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MysqlBackupVerificationList contains a list of MysqlBackupVerification.
type MysqlBackupVerificationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlBackupVerification `json:"items"`
}

// MysqlBackupVerificationSpec defines the desired state of a verification run.
type MysqlBackupVerificationSpec struct {
	// FailoverGroupRef identifies the MysqlFailoverGroup in the same
	// namespace whose backup profile should drive this verification.
	// +required
	FailoverGroupRef LocalGroupRef `json:"failoverGroupRef"`

	// ProfileName is the name of a spec.backup.profiles[] entry on the
	// referenced failover group.
	// +kubebuilder:validation:MinLength=1
	// +required
	ProfileName string `json:"profileName"`

	// BackupRef optionally pins the verification to a specific
	// MysqlBackup in the same namespace. When empty, the reconciler
	// resolves the latest Succeeded MysqlBackup for (group, profile).
	// +optional
	BackupRef *corev1.LocalObjectReference `json:"backupRef,omitempty"`

	// KeepOnFailure leaves the verification Job and its Pod in place when the
	// verification terminates in Failed, so operators can kubectl exec
	// into the verification instance to inspect why the load failed.
	// On success all artifacts are always cleaned up regardless of
	// this field.
	//
	// Defaults to true. Retention GC still reclaims KeepOnFailure
	// artifacts once the CR ages out of the failed-run retention bucket.
	// +kubebuilder:default=true
	// +optional
	KeepOnFailure *bool `json:"keepOnFailure,omitempty"`

	// TTLSecondsAfterFinished controls how long the verification Job and
	// its Pod persist after a successful verification before the
	// reconciler cleans them up. Default: 0 (clean up immediately).
	// Failed verifications with KeepOnFailure=true ignore this value.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// Resources sets requests and limits for the verification MySQL
	// container. Forwarded to the Job pod verbatim; when unset, no
	// verification-specific resource requests or limits are
	// configured (the container inherits cluster / namespace default
	// limits if any). The datadir is an emptyDir on the node's disk, so
	// bound it with limits.ephemeral-storage.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// PointInTime configures whether the verification replays archived
	// binlogs on top of the restored dump. Leave nil or set mode=none to
	// skip replay. mode=latest replays to the unique archive head;
	// mode=timestamp restores the history of the first archived
	// transaction at or after a caller-supplied instant, cut just before
	// it. Either way the replay may span every site's archive. Requires
	// spec.backup.pitr.enabled=true on the referenced failover group.
	// +optional
	PointInTime *PointInTimeVerificationSpec `json:"pointInTime,omitempty"`

	// SanityCheck runs a single scalar-returning SELECT against the
	// ephemeral mysqld after the dump load (and optional PITR replay)
	// succeeds. Failures (errors, timeouts, or minRows mismatch) flip
	// the verification to Failed with a SanityCheckFailed /
	// SanityCheckTimeout reason so an otherwise-loadable but obviously
	// corrupt dump does not quietly pass the freshness gauge.
	// +optional
	SanityCheck *SanityCheckSpec `json:"sanityCheck,omitempty"`
}

// PointInTimeVerificationSpec controls PITR binlog replay during a
// verification. When Mode is "none" (or this field is nil), the
// verification stops after util.loadDump() returns; no binlog replay
// happens. "latest" replays to the unique archive head; it exercises the
// archiver and replay path end to end. "timestamp" replays to a
// caller-supplied instant, which drills a specific known-good point or
// checks that archives covering a past incident still replay. A
// verification never accepts a Partial outcome.
type PointInTimeVerificationSpec struct {
	// Mode selects the replay behavior.
	// +kubebuilder:default=none
	// +kubebuilder:validation:Enum=none;latest;timestamp
	// +optional
	Mode string `json:"mode,omitempty"`

	// Timestamp is the target instant, a whole second in the same forms as
	// PointInTimeSpec.StopDatetime (fractional seconds are refused; a
	// stored fractional value must be corrected in the same update, as for
	// StopDatetime). Replay restores the history of the first archived
	// transaction committed at or after it, cut just before that
	// transaction. Required when Mode=timestamp; ignored otherwise.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Timestamp string `json:"timestamp,omitempty"`

	// ExcludeGtids are never replayed and must not intersect the backup,
	// with the same meaning as PointInTimeSpec.ExcludeGtids: every archived
	// transaction committed between the backup's newest transaction and the
	// target (or the archive head in latest mode) that is outside the
	// target history must be listed.
	// +optional
	// +kubebuilder:validation:MaxLength=131072
	ExcludeGtids string `json:"excludeGtids,omitempty"`

	// ArchiveProfileName names the backup profile whose PITR archive the
	// replay reads. Defaults to the group's spec.backup.pitr.profileName.
	// Use it to verify history in a retained archive after a prefix switch.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	ArchiveProfileName string `json:"archiveProfileName,omitempty"`
}

// SanityCheckSpec configures the post-load scalar sanity check. The
// verification runs the named Query via the `mysql` CLI (`mysql -B -N
// -e`, wrapped in `timeout`) against the ephemeral mysqld, expects a
// single row / single column result (empty result sets are treated as
// scalar 0), and fails the verification if the scalar is less than
// Expect.MinRows or if the query exceeds MaxDurationSeconds.
type SanityCheckSpec struct {
	// Query is a single SQL statement. Must return a scalar
	// (single-row, single-column) result. Multi-statement inputs are
	// rejected to keep the timeout budget predictable.
	// +kubebuilder:validation:MinLength=1
	// +required
	Query string `json:"query"`

	// Expect configures the pass/fail thresholds applied to the query
	// result. When nil, a non-error return with any scalar result
	// passes.
	// +optional
	Expect *SanityCheckExpectation `json:"expect,omitempty"`
}

// SanityCheckExpectation captures the pass/fail thresholds applied to
// the sanity query scalar result.
type SanityCheckExpectation struct {
	// MinRows is the floor applied to the scalar result. When >0 and
	// the scalar result is less than MinRows, the verification fails
	// with reason=SanityCheckFailed. Default: 0 (disabled).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinRows int64 `json:"minRows,omitempty"`

	// MaxDurationSeconds is the client-side timeout for the sanity
	// query. Exceeding it fails the verification with
	// reason=SanityCheckTimeout. Default: 60.
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxDurationSeconds int32 `json:"maxDurationSeconds,omitempty"`
}

// VerificationPhase is the lifecycle phase of a MysqlBackupVerification.
// +kubebuilder:validation:Enum=Pending;Provisioning;Restoring;Succeeded;Failed
type VerificationPhase string

const (
	// VerificationPhasePending indicates the CR has been accepted but
	// no ephemeral resources exist yet.
	VerificationPhasePending VerificationPhase = "Pending"
	// VerificationPhaseProvisioning indicates the reconciler is
	// resolving inputs and creating the verification Job for the run.
	VerificationPhaseProvisioning VerificationPhase = "Provisioning"
	// VerificationPhaseRestoring indicates the restore Job is loading
	// the backup artifact into the ephemeral MySQL instance.
	VerificationPhaseRestoring VerificationPhase = "Restoring"
	// VerificationPhaseSucceeded is the terminal success phase.
	VerificationPhaseSucceeded VerificationPhase = "Succeeded"
	// VerificationPhaseFailed is the terminal failure phase.
	VerificationPhaseFailed VerificationPhase = "Failed"
)

// MysqlBackupVerificationStatus tracks observed state of a verification.
type MysqlBackupVerificationStatus struct {
	// Phase is the lifecycle phase of this verification run.
	// +optional
	Phase VerificationPhase `json:"phase,omitempty"`

	// StartTime is when the verification entered Provisioning.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the verification reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// DurationSeconds is the wall-clock duration from StartTime to
	// CompletionTime, computed on terminal transition.
	// +optional
	DurationSeconds int64 `json:"durationSeconds,omitempty"`

	// BackupRef is the resolved reference to the MysqlBackup that was
	// verified. Populated once the reconciler resolves spec.backupRef
	// (or the latest-Succeeded default).
	// +optional
	BackupRef *VerificationBackupRef `json:"backupRef,omitempty"`

	// JobName is the name of the restore Job that loaded the dump into
	// the ephemeral MySQL instance.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// ReplayedThroughBinlog is populated on Succeeded verifications
	// whose spec.pointInTime.mode was not "none". Captures the last
	// binlog event that was applied so operators can correlate the
	// verified coverage window against their RPO target.
	// +optional
	ReplayedThroughBinlog *BinlogReplayMark `json:"replayedThroughBinlog,omitempty"`

	// PointInTime is the certified outcome of the point-in-time replay,
	// read from the replay's verdict. Set on Succeeded verifications whose
	// spec.pointInTime.mode is not none.
	// +optional
	PointInTime *PointInTimeResult `json:"pointInTime,omitempty"`

	// SanityCheck is populated when spec.sanityCheck is set. Records
	// whether the query ran, its duration, and the scalar result so
	// operators can look at `kubectl describe` and tell what the
	// verification actually asserted.
	// +optional
	SanityCheck *SanityCheckResult `json:"sanityCheck,omitempty"`

	// Message is a human-readable status message.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the .metadata.generation this status was
	// computed against.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of this
	// verification's state. Types include "Verified" and
	// "ResourcesCleanedUp".
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// BinlogReplayMark summarizes how far a PITR replay reached. Its fields are
// derived independently from the certified replay verdict, not read from
// one final event: File is the last binlog file the replay applied,
// Timestamp is the newest original commit time among the replayed
// transactions (which need not be an event in File), and Position is not
// reported.
type BinlogReplayMark struct {
	// File is the last binlog file the replay applied, e.g.
	// "mysql-bin.000412".
	// +optional
	File string `json:"file,omitempty"`

	// Position is not reported by current releases and stays unset.
	// +optional
	Position int64 `json:"position,omitempty"`

	// Timestamp is the newest original commit time among the replayed
	// transactions. Used to compute
	// bloodraven_backup_verification_replay_lag_seconds.
	// +optional
	Timestamp *metav1.Time `json:"timestamp,omitempty"`
}

// SanityCheckResult captures the observed outcome of
// spec.sanityCheck.query. ResultRow is the scalar column rendered as a
// string so the status subresource stays generic over MySQL type.
type SanityCheckResult struct {
	// Ran is true when the verification Job actually executed the
	// sanity query. Useful for distinguishing "skipped because upstream
	// phase failed" from "ran but returned 0 rows".
	// +optional
	Ran bool `json:"ran,omitempty"`

	// DurationMs is the client-side wall-clock duration of the query.
	// +optional
	DurationMs int64 `json:"durationMs,omitempty"`

	// ResultRow is the scalar result rendered as a string. Empty when
	// the query returned zero rows.
	// +optional
	ResultRow string `json:"resultRow,omitempty"`

	// Error is the error message from a failed sanity query. Set on
	// SanityCheckFailed or SanityCheckTimeout terminal transitions.
	// +optional
	Error string `json:"error,omitempty"`
}

// VerificationBackupRef records which MysqlBackup a verification run
// resolved to, including the backup's UID so a later deletion +
// GenerateName collision cannot confuse the audit trail.
type VerificationBackupRef struct {
	// Name of the verified MysqlBackup.
	// +required
	Name string `json:"name"`
	// UID of the verified MysqlBackup at resolution time.
	// +optional
	UID string `json:"uid,omitempty"`
	// Location copied from MysqlBackup.status.location.
	// +optional
	Location string `json:"location,omitempty"`
	// StorageType copied from MysqlBackup.status.storageType.
	// +optional
	StorageType BackupStorageType `json:"storageType,omitempty"`
}

// VerificationSpec is embedded on BackupProfile. When set the
// MysqlFailoverGroupReconciler renders a CronJob that creates
// MysqlBackupVerification CRs on schedule. See
// internal/controller/backup_verification_schedule.go.
type VerificationSpec struct {
	// Enabled turns the scheduled verification CronJob on or off.
	// Omitting spec.backup.profiles[].verification entirely is
	// equivalent to Enabled=false.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is a standard cron expression (5 fields). Evaluated in
	// the TimeZone below. Required when Enabled=true.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// TimeZone is an IANA timezone name used when interpreting
	// Schedule. Defaults to "Etc/UTC" for the same reproducibility
	// reasons as BackupSchedule.TimeZone.
	// +kubebuilder:default="Etc/UTC"
	// +optional
	TimeZone string `json:"timeZone,omitempty"`

	// Suspend pauses the scheduled CronJob without deleting it.
	// +kubebuilder:default=false
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// ConcurrencyPolicy is forwarded to the CronJob spec. Only Forbid
	// (default) and Replace are allowed; Allow would stack overlapping
	// verifications against the same profile, each restoring a full copy
	// of the backup onto node disk and racing for the newest backup.
	// +kubebuilder:default=Forbid
	// +kubebuilder:validation:Enum=Forbid;Replace
	// +optional
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty"`

	// StartingDeadlineSeconds is forwarded to the CronJob spec.
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

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

	// KeepOnFailure mirrors MysqlBackupVerificationSpec.KeepOnFailure
	// for CRs created by this schedule. Default: true.
	// +kubebuilder:default=true
	// +optional
	KeepOnFailure *bool `json:"keepOnFailure,omitempty"`

	// TTLSecondsAfterFinished mirrors the per-CR field for scheduled
	// CRs. Default: 0.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// RetentionPolicy caps how many verification CRs per profile are
	// retained after they reach a terminal phase.
	// +optional
	RetentionPolicy *VerificationRetentionPolicy `json:"retentionPolicy,omitempty"`

	// PointInTime is copied onto each scheduled verification CR by
	// trigger-verification. See PointInTimeVerificationSpec.
	// +optional
	PointInTime *PointInTimeVerificationSpec `json:"pointInTime,omitempty"`

	// SanityCheck is copied onto each scheduled verification CR by
	// trigger-verification. See SanityCheckSpec.
	// +optional
	SanityCheck *SanityCheckSpec `json:"sanityCheck,omitempty"`
}

// VerificationRetentionPolicy caps the number of MysqlBackupVerification
// CRs retained per (group, profile). The Succeeded bucket and the
// Failed bucket are capped independently so a flood of failures does
// not evict the record of last week's successful verification.
type VerificationRetentionPolicy struct {
	// KeepSuccessful is the max number of Succeeded CRs to keep.
	// Default: 30.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	// +optional
	KeepSuccessful int32 `json:"keepSuccessful,omitempty"`

	// KeepFailures is the max number of Failed CRs to keep. Failed
	// runs are what operators most want to investigate, so they are
	// retained independently of the Succeeded bucket. Default: 10.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	// +optional
	KeepFailures int32 `json:"keepFailures,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlBackupVerification{}, &MysqlBackupVerificationList{})
}
