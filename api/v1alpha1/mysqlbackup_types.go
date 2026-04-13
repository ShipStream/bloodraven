package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.failoverGroupRef.name`
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.profileName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="Completed",type=date,JSONPath=`.status.completionTime`
// +kubebuilder:printcolumn:name="Location",type=string,JSONPath=`.status.location`,priority=1
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.status.size`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MysqlBackup represents a single backup run against a MysqlFailoverGroup.
// It may be created directly by an operator (one-off backup) or indirectly
// by a CronJob materialized from spec.backup.schedules[].
type MysqlBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlBackupSpec   `json:"spec,omitempty"`
	Status MysqlBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MysqlBackupList contains a list of MysqlBackup.
type MysqlBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlBackup `json:"items"`
}

// MysqlBackupSpec defines the desired state of a MysqlBackup.
type MysqlBackupSpec struct {
	// FailoverGroupRef identifies the MysqlFailoverGroup in the same
	// namespace whose profile should drive this backup.
	FailoverGroupRef LocalGroupRef `json:"failoverGroupRef"`

	// ProfileName is the name of a spec.backup.profiles[] entry on the
	// referenced failover group.
	// +kubebuilder:validation:MinLength=1
	ProfileName string `json:"profileName"`

	// SourceSiteOverride, if set, forces a specific site to be the dump
	// source. Leave empty to let the reconciler choose (replica-first).
	// +optional
	SourceSiteOverride string `json:"sourceSiteOverride,omitempty"`

	// TriggeredBy is a free-form label recording what created this CR
	// (e.g. "schedule/nightly", "manual", "retry/nightly/attempt-2").
	// Informational only.
	TriggeredBy string `json:"triggeredBy,omitempty"`
}

// LocalGroupRef is a same-namespace reference to a MysqlFailoverGroup.
type LocalGroupRef struct {
	// Name of the MysqlFailoverGroup.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// BackupPhase is the lifecycle phase of a MysqlBackup.
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
type BackupPhase string

const (
	// BackupPhasePending indicates the CR has been accepted but no Job exists yet.
	BackupPhasePending BackupPhase = "Pending"
	// BackupPhaseRunning indicates the backup Job has been created and is executing.
	BackupPhaseRunning BackupPhase = "Running"
	// BackupPhaseSucceeded indicates the backup Job completed successfully.
	BackupPhaseSucceeded BackupPhase = "Succeeded"
	// BackupPhaseFailed indicates the backup Job failed terminally.
	BackupPhaseFailed BackupPhase = "Failed"
)

// MysqlBackupStatus tracks the observed state of a MysqlBackup.
type MysqlBackupStatus struct {
	// Phase is the lifecycle phase of this backup run.
	Phase BackupPhase `json:"phase,omitempty"`

	// StartTime is when the backup Job was created.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the backup Job reached a terminal state.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// JobName is the name of the batchv1.Job running the dump.
	JobName string `json:"jobName,omitempty"`

	// SourceSite is the site name whose MySQL instance was used as the
	// dump source.
	SourceSite string `json:"sourceSite,omitempty"`

	// Location is the resolved URL/path of the backup artifact, e.g.
	// "s3://bucket/prefix/backup-name/" or "/backups/subpath/backup-name".
	Location string `json:"location,omitempty"`

	// StorageType is the storage backend kind (S3 or PVC) that was used
	// for this backup. Populated at Job creation time so cleanup logic
	// can dispatch on it without resorting to a brittle location
	// heuristic after the referenced profile is gone.
	StorageType BackupStorageType `json:"storageType,omitempty"`

	// Size is the reported backup size as a human-readable string, best
	// effort. Empty when the Job log did not report a size.
	Size string `json:"size,omitempty"`

	// SizeBytes is the structured backup size emitted by the dump
	// sidecar. 0 when unknown (e.g. remote output where the dump
	// utility did not return a total).
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// GtidExecuted is the value of @@global.gtid_executed captured at
	// dump time. Empty on non-GTID instances.
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// BinlogFile is the binary log file name at dump time. Populated
	// alongside BinlogPos for pre-GTID point-in-time tooling.
	BinlogFile string `json:"binlogFile,omitempty"`

	// BinlogPos is the binary log position at dump time.
	BinlogPos int64 `json:"binlogPos,omitempty"`

	// ActiveSiteAtStart records MysqlFailoverGroup.status.activeSite as
	// observed when this backup's Job was created. The reconciler uses
	// this to emit an InFlightFailover warning when the group's active
	// site drifts while the backup is running.
	ActiveSiteAtStart string `json:"activeSiteAtStart,omitempty"`

	// Attempt is the retry attempt number for scheduled CRs: 1 for the
	// original attempt, 2 for the first retry, and so on. 0 is treated
	// as 1 for older CRs.
	Attempt int32 `json:"attempt,omitempty"`

	// Message is a human-readable status message.
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the .metadata.generation this status was
	// computed against.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of this
	// backup's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlBackup{}, &MysqlBackupList{})
}
