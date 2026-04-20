package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
// Phase 1 implementation runs the ephemeral mysqld inside the
// verification Job's Pod on a dedicated PVC, with a derived
// credentials Secret for the restore script. No Service is created;
// the Pod binds 127.0.0.1 only and is not reachable from outside its
// own network namespace. On success all artifacts are cleaned up; on
// failure KeepOnFailure (default true) leaves the Job Pod and PVC in
// place for inspection until the retention sweep reclaims them.
type MysqlBackupVerification struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlBackupVerificationSpec   `json:"spec,omitempty"`
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
	FailoverGroupRef LocalGroupRef `json:"failoverGroupRef"`

	// ProfileName is the name of a spec.backup.profiles[] entry on the
	// referenced failover group.
	// +kubebuilder:validation:MinLength=1
	ProfileName string `json:"profileName"`

	// BackupRef optionally pins the verification to a specific
	// MysqlBackup in the same namespace. When empty, the reconciler
	// resolves the latest Succeeded MysqlBackup for (group, profile).
	// +optional
	BackupRef *corev1.LocalObjectReference `json:"backupRef,omitempty"`

	// Storage configures the ephemeral PVC the verification MySQL pod
	// mounts as its datadir. The PVC is always dedicated per
	// verification run and deleted during cleanup; its lifecycle is
	// NOT shared with the backup PVC.
	// +optional
	Storage *VerificationStorage `json:"storage,omitempty"`

	// KeepOnFailure leaves the ephemeral Pod and PVC in place when the
	// verification terminates in Failed, so operators can kubectl exec
	// into the verification instance to inspect why the load failed.
	// On success all artifacts are always cleaned up regardless of
	// this field.
	//
	// Defaults to true. Retention GC still reclaims KeepOnFailure
	// artifacts once the CR ages out of the failed-run retention bucket.
	// +kubebuilder:default=true
	KeepOnFailure *bool `json:"keepOnFailure,omitempty"`

	// TTLSecondsAfterFinished controls how long the verification Pod +
	// Service + PVC persist after a successful verification before the
	// reconciler cleans them up. Default: 0 (clean up immediately).
	// Failed verifications with KeepOnFailure=true ignore this value.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// Resources sets requests and limits for the verification MySQL
	// container. Forwarded to the Job pod verbatim; when unset, no
	// verification-specific resource requests or limits are
	// configured (the container inherits cluster / namespace default
	// limits if any).
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// TriggeredBy records what created this CR ("manual",
	// "schedule/<schedule-name>"). Informational only.
	// +optional
	TriggeredBy string `json:"triggeredBy,omitempty"`
}

// VerificationStorage sizes and parameterizes the ephemeral PVC used as
// MySQL datadir for a single verification run. All fields optional; the
// reconciler auto-sizes from the referenced backup's status.sizeBytes
// when Size is empty.
type VerificationStorage struct {
	// StorageClassName names the StorageClass used for the ephemeral
	// PVC. When empty the cluster default StorageClass is used.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// Size is the requested PVC capacity. When empty the reconciler
	// defaults to max(10Gi, 1.5 * backup.status.sizeBytes rounded up
	// to the nearest 10Gi).
	// +optional
	Size resource.Quantity `json:"size,omitempty"`
}

// VerificationPhase is the lifecycle phase of a MysqlBackupVerification.
// +kubebuilder:validation:Enum=Pending;Provisioning;Restoring;Checking;Cleaning;Succeeded;Failed
type VerificationPhase string

const (
	// VerificationPhasePending indicates the CR has been accepted but
	// no ephemeral resources exist yet.
	VerificationPhasePending VerificationPhase = "Pending"
	// VerificationPhaseProvisioning indicates the reconciler is
	// creating the ephemeral PVC / Service / Pod for the run.
	VerificationPhaseProvisioning VerificationPhase = "Provisioning"
	// VerificationPhaseRestoring indicates the restore Job is loading
	// the backup artifact into the ephemeral MySQL instance.
	VerificationPhaseRestoring VerificationPhase = "Restoring"
	// VerificationPhaseChecking indicates the restore completed and
	// any optional post-load sanity check is running. Phase 1 of the
	// verification feature treats load-success as the sanity check
	// and transitions directly to Cleaning.
	VerificationPhaseChecking VerificationPhase = "Checking"
	// VerificationPhaseCleaning indicates the reconciler is tearing
	// down the ephemeral resources. On failure with KeepOnFailure=true
	// this phase is skipped.
	VerificationPhaseCleaning VerificationPhase = "Cleaning"
	// VerificationPhaseSucceeded is the terminal success phase.
	VerificationPhaseSucceeded VerificationPhase = "Succeeded"
	// VerificationPhaseFailed is the terminal failure phase.
	VerificationPhaseFailed VerificationPhase = "Failed"
)

// MysqlBackupVerificationStatus tracks observed state of a verification.
type MysqlBackupVerificationStatus struct {
	// Phase is the lifecycle phase of this verification run.
	Phase VerificationPhase `json:"phase,omitempty"`

	// StartTime is when the verification entered Provisioning.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the verification reached a terminal phase.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// DurationSeconds is the wall-clock duration from StartTime to
	// CompletionTime, computed on terminal transition.
	DurationSeconds int64 `json:"durationSeconds,omitempty"`

	// BackupRef is the resolved reference to the MysqlBackup that was
	// verified. Populated once the reconciler resolves spec.backupRef
	// (or the latest-Succeeded default).
	BackupRef *VerificationBackupRef `json:"backupRef,omitempty"`

	// JobName is the name of the restore Job that loaded the dump into
	// the ephemeral MySQL instance.
	JobName string `json:"jobName,omitempty"`

	// PodName is the name of the ephemeral MySQL Pod.
	PodName string `json:"podName,omitempty"`

	// PVCName is the name of the ephemeral MySQL PVC.
	PVCName string `json:"pvcName,omitempty"`

	// ServiceName is the name of the headless Service routing the
	// restore Job to the ephemeral MySQL Pod.
	ServiceName string `json:"serviceName,omitempty"`

	// Message is a human-readable status message.
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the .metadata.generation this status was
	// computed against.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of this
	// verification's state. Types include "Verified" and
	// "ResourcesCleanedUp".
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// VerificationBackupRef records which MysqlBackup a verification run
// resolved to, including the backup's UID so a later deletion +
// GenerateName collision cannot confuse the audit trail.
type VerificationBackupRef struct {
	// Name of the verified MysqlBackup.
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
	Enabled bool `json:"enabled,omitempty"`

	// Schedule is a standard cron expression (5 fields). Evaluated in
	// the TimeZone below. Required when Enabled=true.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// TimeZone is an IANA timezone name used when interpreting
	// Schedule. Defaults to "Etc/UTC" for the same reproducibility
	// reasons as BackupSchedule.TimeZone.
	// +kubebuilder:default="Etc/UTC"
	TimeZone string `json:"timeZone,omitempty"`

	// Suspend pauses the scheduled CronJob without deleting it.
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`

	// ConcurrencyPolicy is forwarded to the CronJob spec. Only Forbid
	// (default) and Replace are allowed; Allow would stack overlapping
	// verifications against the same profile, which can't share a PVC.
	// +kubebuilder:default=Forbid
	// +kubebuilder:validation:Enum=Forbid;Replace
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty"`

	// StartingDeadlineSeconds is forwarded to the CronJob spec.
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// SuccessfulJobsHistoryLimit is forwarded to the CronJob spec.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// FailedJobsHistoryLimit is forwarded to the CronJob spec.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`

	// Storage, KeepOnFailure, and TTLSecondsAfterFinished seed the
	// per-CR spec fields of the same name. The reconciler copies these
	// into each MysqlBackupVerification created by the CronJob.
	// +optional
	Storage *VerificationStorage `json:"storage,omitempty"`

	// KeepOnFailure mirrors MysqlBackupVerificationSpec.KeepOnFailure
	// for CRs created by this schedule. Default: true.
	// +kubebuilder:default=true
	KeepOnFailure *bool `json:"keepOnFailure,omitempty"`

	// TTLSecondsAfterFinished mirrors the per-CR field for scheduled
	// CRs. Default: 0.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// RetentionPolicy caps how many verification CRs per profile are
	// retained after they reach a terminal phase.
	// +optional
	RetentionPolicy *VerificationRetentionPolicy `json:"retentionPolicy,omitempty"`
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
	KeepSuccessful int32 `json:"keepSuccessful,omitempty"`

	// KeepFailures is the max number of Failed CRs to keep. Failed
	// runs are what operators most want to investigate, so they are
	// retained independently of the Succeeded bucket. Default: 10.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	KeepFailures int32 `json:"keepFailures,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlBackupVerification{}, &MysqlBackupVerificationList{})
}
