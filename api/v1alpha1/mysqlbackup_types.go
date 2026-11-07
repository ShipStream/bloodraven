package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.failoverGroupRef.name`
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.profileName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startTime`
// +kubebuilder:printcolumn:name="Completed",type=date,JSONPath=`.status.completionTime`
// +kubebuilder:printcolumn:name="Location",type=string,JSONPath=`.status.artifactStorage.location`,priority=1
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.status.size`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="!self.metadata.name.endsWith('.bloodraven-backup.json')",message="metadata.name must not end in .bloodraven-backup.json: that is the backup stamp's key suffix, and the dump location ends in the backup's name"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(oldSelf.status.artifactStorage) || !has(oldSelf.status.artifactStorage.location) || (has(self.status) && has(self.status.artifactStorage) && self.status.artifactStorage == oldSelf.status.artifactStorage)",message="status.artifactStorage cannot be changed or removed once its location is recorded"

// MysqlBackup represents a single backup run against a MysqlFailoverGroup.
// It may be created directly by an operator (one-off backup) or indirectly
// by a CronJob materialized from spec.backup.schedules[].
type MysqlBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec MysqlBackupSpec `json:"spec"`
	// +optional
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
	// +required
	FailoverGroupRef LocalFailoverGroupRef `json:"failoverGroupRef"`

	// ProfileName is the name of a spec.backup.profiles[] entry on the
	// referenced failover group.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +required
	ProfileName string `json:"profileName"`

	// SourceSite, if set, forces a specific site (a spec.sites[].name of
	// the referenced group) to be the dump source. Leave empty to let the
	// reconciler choose (replica-first).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9-]+$`
	// +optional
	SourceSite string `json:"sourceSite,omitempty"`

	// DeletionPolicy controls what happens to the backup's artifact when
	// this CR is deleted, by hand or by retention:
	//   - Delete (default) removes the artifact from storage, and then the
	//     backup's dump-key snapshot Secret, so retention frees storage.
	//   - Retain leaves the artifact in storage and keeps the dump-key
	//     snapshot Secret that decrypts it; an ArtifactRetained Event names
	//     both. Nothing tracks them afterwards: delete them by hand when
	//     they are no longer needed.
	// It is read when the deletion is processed, so it can be changed up to
	// then.
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
}

// LocalFailoverGroupRef is a same-namespace reference to a MysqlFailoverGroup.
type LocalFailoverGroupRef struct {
	// Name of the MysqlFailoverGroup.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	Name string `json:"name"`
}

// LocalMysqlBackupRef is a same-namespace reference to a MysqlBackup.
type LocalMysqlBackupRef struct {
	// Name of the MysqlBackup.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
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
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// StartTime is when the backup Job was created.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the backup Job reached a terminal state.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// JobName is the name of the batchv1.Job running the dump.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// SourceSite is the site name whose MySQL instance was used as the
	// dump source.
	// +optional
	SourceSite string `json:"sourceSite,omitempty"`

	// ArtifactStorage records the storage destination the producing Job
	// wrote to: the storage type and the exact artifact location, plus
	// bucket, endpoint and region for S3 or the claim for PVC. It is
	// captured from the Job when the Job is created and never follows later
	// profile edits, so restores, verifications and artifact cleanup cannot
	// be redirected to another bucket, claim or prefix. Once its location is
	// recorded it is immutable. Credentials are deliberately not recorded:
	// consumers read them from the profile's current credentialsSecretName.
	// Durable: after the producing Job is gone it is the only record of
	// where the artifact is and which key encrypted it.
	// +optional
	ArtifactStorage *BackupArtifactStorage `json:"artifactStorage,omitempty"`

	// Size is the reported backup size as a human-readable string, best
	// effort. Empty when the Job log did not report a size.
	// +optional
	Size string `json:"size,omitempty"`

	// SizeBytes is the structured backup size emitted by the dump
	// sidecar. 0 when unknown (e.g. remote output where the dump
	// utility did not return a total).
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// GtidExecuted is the value of @@global.gtid_executed captured at
	// dump time. Empty on non-GTID instances.
	// +optional
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// BinlogPosition is the binary-log position at dump time, for
	// pre-GTID point-in-time tooling. Unset when the dump did not report
	// one.
	// +optional
	BinlogPosition *BinlogCoordinate `json:"binlogPosition,omitempty"`

	// MysqlImage is the resolved MySQL server image (spec.image, or the
	// controller default) that was in use on the source failover group
	// when this backup was taken. Captured when the backup Job is
	// created and backfilled from the live spec on next reconcile if
	// empty on older CRs. Retained so future verifications can pin the
	// ephemeral mysqld to a version-compatible tag; the Phase-2
	// verification Job currently runs on the backup/restore image
	// (spec.backup.image) and does not yet consume this field.
	// +optional
	MysqlImage string `json:"mysqlImage,omitempty"`

	// ActiveSiteAtStart records MysqlFailoverGroup.status.activeSite as
	// observed when this backup's Job was created. The reconciler uses
	// this to emit an InFlightFailover warning when the group's active
	// site drifts while the backup is running.
	// +optional
	ActiveSiteAtStart string `json:"activeSiteAtStart,omitempty"`

	// Attempt is the retry attempt number for scheduled CRs: 1 for the
	// original attempt, 2 for the first retry, and so on. 0 is treated
	// as 1 for older CRs.
	// +optional
	Attempt int32 `json:"attempt,omitempty"`

	// Encryption attributes the artifact's encryption: Passphrase when the
	// producing Job encrypted it (artifactStorage.passphraseSecretKeyRef
	// names the key), None when it wrote plaintext. It is written from the
	// owned producing Job when the Job is created, and derived again from
	// that Job at completion. Absent when no owned producing Job attributes
	// it. Restores and verifications use it to decide whether to decrypt.
	// +optional
	Encryption *BackupEncryptionStatus `json:"encryption,omitempty"`

	// FailoverGroupUID is the metadata.uid of the source failover group,
	// pinned in the backup Job's template when the Job is created and
	// recovered from the Job. The backup's stamp records the same UID.
	// Durable: after the producing Job is gone it is the only record of
	// which group incarnation produced the artifact.
	// +optional
	FailoverGroupUID types.UID `json:"failoverGroupUID,omitempty"`

	// PITRArchiveID is the archive ID of the source group's active PITR
	// archive when the Job was created, pinned in the Job's template and
	// recovered from it. Empty when PITR was disabled.
	// Durable: PITR replay and binlog retention select the archive by it.
	// +optional
	// +kubebuilder:validation:Pattern=`^(ar1-[a-z2-7]{26})?$`
	PITRArchiveID string `json:"pitrArchiveID,omitempty"`

	// Message is a human-readable status message.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the .metadata.generation this status was
	// computed against.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of this
	// backup's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// BackupEncryptionStatus attributes a backup artifact's encryption.
type BackupEncryptionStatus struct {
	// Mode is Passphrase for an encrypted artifact and None for plaintext.
	// +required
	Mode EncryptionMode `json:"mode"`

	// Algorithm is the algorithm stamped onto the artifact header; empty
	// for None.
	// +optional
	Algorithm EncryptionAlgorithm `json:"algorithm,omitempty"`
}

// BackupArtifactStorage is the immutable storage destination of a backup
// artifact. Immutability is enforced from the MysqlBackup root, so removing
// the whole status does not bypass it.
type BackupArtifactStorage struct {
	// Type is the storage backend the producing Job wrote to.
	// +required
	Type BackupStorageType `json:"type"`

	// Location is the artifact location the producing Job wrote, e.g.
	// "s3://bucket/prefix/backup-name/" or "/backups/subpath/backup-name".
	// +optional
	Location string `json:"location,omitempty"`
	// Bucket is the S3 bucket the artifact was written to.
	// +optional
	Bucket string `json:"bucket,omitempty"`
	// EndpointURL is the S3 endpoint override in effect for the producing
	// Job, empty for the AWS default endpoint.
	// +optional
	EndpointURL string `json:"endpointURL,omitempty"`
	// Region is the S3 region in effect for the producing Job.
	// +optional
	Region string `json:"region,omitempty"`
	// ClaimName is the PersistentVolumeClaim the artifact was written to.
	// +optional
	ClaimName string `json:"claimName,omitempty"`
	// PassphraseSecretKeyRef is the passphrase Secret and key the producing Job
	// mounted, for an encrypted backup: the backup's own immutable dump-key
	// snapshot (mysqlbackup-<name>-dump-key-<uid8>, key passphrase), which
	// the operator made from the profile's passphrase before the Job and
	// deletes only after the backup's artifacts are deleted. Restores,
	// verifications and cleanup of a backup that never succeeded decrypt
	// with it, so a profile edited since the launch does not decide which
	// passphrase is used.
	// +optional
	PassphraseSecretKeyRef *SecretKeyRef `json:"passphraseSecretKeyRef,omitempty"`
}

// SecretKeyRef selects one data key of a Secret in the same namespace.
// Both fields are required; it is the shape a reference takes when there
// is no default key, such as the key a producing backup Job encrypted its
// artifact with.
type SecretKeyRef struct {
	// Name is the Secret name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	Name string `json:"name"`
	// Key is the data key within the Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	// +required
	Key string `json:"key"`
}

func init() {
	SchemeBuilder.Register(&MysqlBackup{}, &MysqlBackupList{})
}
