package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mbv;mbvs,categories=bloodraven;mysql;backup
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.failoverGroupRef.name`
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.profileName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.status.backupRef.name`,priority=1
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="Completed",type=date,JSONPath=`.status.completionTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 63",message="metadata.name must be at most 63 characters: it is a label value on the objects the run creates"
// +kubebuilder:validation:XValidation:rule="self.spec.failoverGroupRef.name == oldSelf.spec.failoverGroupRef.name",message="spec.failoverGroupRef is immutable; create a new object instead"
// +kubebuilder:validation:XValidation:rule="self.spec.profileName == oldSelf.spec.profileName",message="spec.profileName is immutable; create a new object instead"
// +kubebuilder:validation:XValidation:rule="has(self.spec.backupRef) == has(oldSelf.spec.backupRef) && (!has(self.spec.backupRef) || self.spec.backupRef.name == oldSelf.spec.backupRef.name)",message="spec.backupRef is immutable; create a new object instead"
// +kubebuilder:validation:XValidation:rule="has(self.spec.pointInTime) == has(oldSelf.spec.pointInTime) && (!has(self.spec.pointInTime) || (self.spec.pointInTime.mode == oldSelf.spec.pointInTime.mode && has(self.spec.pointInTime.stopTime) == has(oldSelf.spec.pointInTime.stopTime) && (!has(self.spec.pointInTime.stopTime) || self.spec.pointInTime.stopTime == oldSelf.spec.pointInTime.stopTime) && (has(self.spec.pointInTime.excludeGtids) ? self.spec.pointInTime.excludeGtids : '') == (has(oldSelf.spec.pointInTime.excludeGtids) ? oldSelf.spec.pointInTime.excludeGtids : '') && (has(self.spec.pointInTime.archiveProfileName) ? self.spec.pointInTime.archiveProfileName : '') == (has(oldSelf.spec.pointInTime.archiveProfileName) ? oldSelf.spec.pointInTime.archiveProfileName : '')))",message="spec.pointInTime is immutable; create a new object instead"
// +kubebuilder:validation:XValidation:rule="has(self.spec.sanityCheck) == has(oldSelf.spec.sanityCheck) && (!has(self.spec.sanityCheck) || (self.spec.sanityCheck.query == oldSelf.spec.sanityCheck.query && has(self.spec.sanityCheck.minResult) == has(oldSelf.spec.sanityCheck.minResult) && (!has(self.spec.sanityCheck.minResult) || self.spec.sanityCheck.minResult == oldSelf.spec.sanityCheck.minResult) && has(self.spec.sanityCheck.timeout) == has(oldSelf.spec.sanityCheck.timeout) && (!has(self.spec.sanityCheck.timeout) || duration(self.spec.sanityCheck.timeout) == duration(oldSelf.spec.sanityCheck.timeout))))",message="spec.sanityCheck is immutable; create a new object instead"

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
// namespace. After a success the Job and its Pod are cleaned up as
// ttlSecondsAfterFinished says (unset keeps them until retention deletes
// the verification); after a failure KeepOnFailure (default true) leaves
// them in place for inspection until the retention sweep reclaims them.
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
	FailoverGroupRef LocalFailoverGroupRef `json:"failoverGroupRef"`

	// ProfileName is the name of a spec.backup.profiles[] entry on the
	// referenced failover group.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +required
	ProfileName string `json:"profileName"`

	// BackupRef optionally pins the verification to a specific
	// MysqlBackup in the same namespace. When omitted, the reconciler
	// resolves the latest Succeeded MysqlBackup for (group, profile).
	// +optional
	BackupRef *LocalMysqlBackupRef `json:"backupRef,omitempty"`

	// KeepOnFailure leaves the verification Job and its Pod in place when the
	// verification terminates in Failed, so operators can kubectl exec
	// into the verification instance to inspect why the load failed.
	// It does not apply to a success, whose Job and Pod are cleaned up as
	// ttlSecondsAfterFinished says.
	//
	// Defaults to true. Retention GC still reclaims KeepOnFailure
	// artifacts once the CR ages out of the failed-run retention bucket.
	// +kubebuilder:default=true
	// +optional
	KeepOnFailure *bool `json:"keepOnFailure,omitempty"`

	// TTLSecondsAfterFinished controls how long the verification Job and
	// its Pod persist after a successful verification before the
	// reconciler cleans them up. 0 cleans them up as soon as the
	// verification finishes; unset keeps them until retention deletes the
	// verification. Failed verifications with KeepOnFailure=true ignore
	// this value.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// Resources sets requests and limits for the verification MySQL
	// container. Forwarded to the Job pod verbatim; when unset, no
	// verification-specific resource requests or limits are
	// configured (the container inherits cluster / namespace default
	// limits if any). The datadir is an emptyDir on the node's disk, so
	// bound it with limits.ephemeral-storage.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// PointInTime configures whether the verification replays archived
	// binlogs on top of the restored dump. Leave it unset to skip replay.
	// mode=Latest replays to the unique archive head; mode=Timestamp
	// restores the history of the first archived
	// transaction at or after a caller-supplied instant, cut just before
	// it. Either way the replay may span every site's archive. Requires
	// spec.backup.pitr.enabled=true on the referenced failover group.
	// +optional
	PointInTime *PointInTimeVerificationSpec `json:"pointInTime,omitempty"`

	// SanityCheck runs a single scalar-returning SELECT against the
	// ephemeral mysqld after the dump load (and optional PITR replay)
	// succeeds. Failures (errors, timeouts, or a result below minResult) flip
	// the verification to Failed with a SanityCheckFailed /
	// SanityCheckTimeout reason so an otherwise-loadable but obviously
	// corrupt dump does not quietly pass the freshness gauge.
	// +optional
	SanityCheck *SanityCheckSpec `json:"sanityCheck,omitempty"`
}

// PointInTimeVerificationSpec controls PITR binlog replay during a
// verification. Without it the verification stops after util.loadDump()
// returns; no binlog replay happens. Latest replays to the unique archive
// head; it exercises the archiver and replay path end to end. Timestamp
// replays to a caller-supplied instant, which drills a specific known-good
// point or checks that archives covering a past incident still replay. A
// verification never accepts a Partial outcome.
// +kubebuilder:validation:XValidation:rule="self.mode == 'Timestamp' ? has(self.stopTime) : !has(self.stopTime)",message="pointInTime.stopTime is required when mode is Timestamp and forbidden when mode is Latest"
type PointInTimeVerificationSpec struct {
	// Mode selects the replay end point: Latest or Timestamp.
	// +required
	Mode PointInTimeMode `json:"mode"`

	// StopTime is the target instant, in the same form as
	// PointInTimeSpec.StopTime (RFC 3339, an explicit zone, whole seconds).
	// Replay restores the history of the first archived transaction
	// committed at or after it, cut just before that transaction. Required
	// when Mode=Timestamp; forbidden when Mode=Latest.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=date-time
	StopTime *metav1.Time `json:"stopTime,omitempty"`

	// ExcludeGtids are never replayed and must not intersect the backup,
	// with the same meaning as PointInTimeSpec.ExcludeGtids: every archived
	// transaction committed between the backup's newest transaction and the
	// target (or the archive head in Latest mode) that is outside the
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
// scalar 0), and fails the verification if an integer scalar is less
// than MinResult or if the query runs longer than Timeout.
type SanityCheckSpec struct {
	// Query is a single SQL statement. Must return a scalar
	// (single-row, single-column) result. Multi-statement inputs are
	// rejected to keep the timeout budget predictable.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	// +required
	Query string `json:"query"`

	// MinResult is the floor applied to an integer scalar result: a
	// smaller result fails the verification with reason=SanityCheckFailed.
	// A non-integer result is not compared. Unset applies no floor.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinResult *int64 `json:"minResult,omitempty"`

	// Timeout is the client-side timeout for the sanity query, applied in
	// whole seconds (rounded up). Exceeding it fails the verification with
	// reason=SanityCheckTimeout. Default: 60s. Bounds: 1s to 1h.
	// +kubebuilder:default="1m0s"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('1h')",message="sanityCheck.timeout must be between 1s and 1h"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
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

	// PointInTime is the certified outcome of the point-in-time replay,
	// read from the replay's verdict. Set on Succeeded verifications with
	// spec.pointInTime. Its newestReplayedCommitTime feeds
	// bloodraven_backup_verification_replay_lag_seconds.
	// +optional
	PointInTime *PointInTimeResult `json:"pointInTime,omitempty"`

	// SanityCheck is populated when spec.sanityCheck is set. Records
	// the check's outcome, its duration, and the scalar result so
	// operators can look at `kubectl describe` and tell what the
	// verification actually asserted. It stays unset when a failed run
	// leaves no evidence of whether the query ran (a pod stopped while
	// the query ran, a pod or log that cannot be read).
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
	// verification's state. Types include "Ready", "Verified" and
	// "ResourcesCleanedUp".
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SanityCheckOutcome is status.sanityCheck.outcome.
// +kubebuilder:validation:Enum=Passed;Failed;TimedOut;Skipped
type SanityCheckOutcome string

const (
	// SanityCheckOutcomePassed: the query ran and its result met MinResult.
	SanityCheckOutcomePassed SanityCheckOutcome = "Passed"
	// SanityCheckOutcomeFailed: the query errored or its result was below
	// MinResult.
	SanityCheckOutcomeFailed SanityCheckOutcome = "Failed"
	// SanityCheckOutcomeTimedOut: the query ran longer than Timeout.
	SanityCheckOutcomeTimedOut SanityCheckOutcome = "TimedOut"
	// SanityCheckOutcomeSkipped: the verification's pod shows it failed
	// before the sanity phase (the load or the replay failed), so the
	// query never ran.
	SanityCheckOutcomeSkipped SanityCheckOutcome = "Skipped"
)

// SanityCheckResult captures the observed outcome of
// spec.sanityCheck.query. Result is the scalar column rendered as a
// string so the status subresource stays generic over MySQL type.
type SanityCheckResult struct {
	// Outcome is the sanity check's result.
	// +required
	Outcome SanityCheckOutcome `json:"outcome"`

	// DurationMilliseconds is the client-side wall-clock duration of the
	// query. Unset when the query did not run.
	// +optional
	DurationMilliseconds *int64 `json:"durationMilliseconds,omitempty"`

	// Result is the scalar result rendered as a string ("0" when the
	// query returned zero rows). Empty when the query did not return.
	// +optional
	Result string `json:"result,omitempty"`

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
	// ArtifactStorage is the backup's status.artifactStorage at
	// resolution time: the destination the verification read the artifact
	// from, and the key it decrypted with.
	// +optional
	ArtifactStorage *BackupArtifactStorage `json:"artifactStorage,omitempty"`
}

// VerificationSpec is embedded on BackupProfile. When set the
// MysqlFailoverGroupReconciler renders a CronJob that creates
// MysqlBackupVerification CRs on schedule. See
// internal/controller/backup_verification_schedule.go.
// +kubebuilder:validation:XValidation:rule="!has(self.enabled) || !self.enabled || (has(self.schedule) && size(self.schedule) > 0)",message="verification.schedule is required when verification.enabled is true"
type VerificationSpec struct {
	// Enabled turns the scheduled verification CronJob on or off.
	// Omitting spec.backup.profiles[].verification entirely is
	// equivalent to Enabled=false.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is a standard cron expression (5 fields). Evaluated in
	// the TimeZone below. Required when Enabled=true.
	// +kubebuilder:validation:MaxLength=256
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
	ConcurrencyPolicy batchv1.ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// StartingDeadlineSeconds is forwarded to the CronJob spec.
	// +kubebuilder:validation:Minimum=0
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

	// TTLSecondsAfterFinished is copied onto each scheduled verification
	// CR; unset stays unset there. See
	// MysqlBackupVerificationSpec.TTLSecondsAfterFinished.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// Retention caps how many of this profile's MysqlBackupVerification
	// objects are kept once they finish. It applies to every verification
	// of the profile, scheduled or not. See VerificationRetention.
	// +kubebuilder:default={}
	// +optional
	Retention VerificationRetention `json:"retention,omitempty"`

	// PointInTime is copied onto each scheduled verification CR by
	// trigger-verification. See PointInTimeVerificationSpec.
	// +optional
	PointInTime *PointInTimeVerificationSpec `json:"pointInTime,omitempty"`

	// SanityCheck is copied onto each scheduled verification CR by
	// trigger-verification. See SanityCheckSpec.
	// +optional
	SanityCheck *SanityCheckSpec `json:"sanityCheck,omitempty"`
}

// VerificationRetention caps the MysqlBackupVerification objects kept per
// (group, profile). Succeeded and Failed runs are counted separately, so a
// run of failures does not evict the record of the last successful
// verification. A verification is kept for two minutes after the
// operator records its verdict, whatever the counts say, so a waiting
// client reads it.
type VerificationRetention struct {
	// KeepLast is how many of the newest Succeeded verifications are kept;
	// 0 keeps none.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	// +optional
	KeepLast *int32 `json:"keepLast,omitempty"`

	// KeepFailed is how many of the newest Failed verifications are kept;
	// 0 keeps none. Failed runs are what operators most want to
	// investigate, so they are not counted against KeepLast.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	// +optional
	KeepFailed *int32 `json:"keepFailed,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlBackupVerification{}, &MysqlBackupVerificationList{})
}
