package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=msc;mscs,categories=bloodraven;mysql;dr
// +kubebuilder:printcolumn:name="Bucket",type=string,JSONPath=`.status.discovered.dumpLocation`
// +kubebuilder:printcolumn:name="BucketReadable",type=string,JSONPath=`.status.conditions[?(@.type=="BucketReadable")].status`
// +kubebuilder:printcolumn:name="SourceKnown",type=string,JSONPath=`.status.conditions[?(@.type=="SourceConfigKnown")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.activation.phase`,priority=1

// MysqlStandbyCluster is a passive observability descriptor for a DR
// relationship from this cluster to a source MysqlFailoverGroup's
// backup+PITR archive in a shared object store. The controller scans
// the source bucket on spec.freshness.discoveryInterval and publishes
// two conditions — BucketReadable and SourceConfigKnown — along with
// status.discovered (dump name, location, GTID set, binlog window).
// No MySQL contact, no restore jobs, no activation in Phase 1.
//
// Future phases (not yet implemented):
// Phase 2 adds continuous restore verification: a scheduled
// MysqlBackupVerification proves the latest dump+PITR window is
// restorable and gates a Restorable condition.
// Phase 3 adds one-command activation: a future admin-confirmed activate
// request materialises a new MysqlFailoverGroup in this namespace loaded
// from the source archive. The spec fields that drive Phase 2/3 are not
// part of v1alpha1 yet; they will be added (backward-compatibly) when
// that code lands.
//
// One MysqlStandbyCluster per DR relationship per namespace. Designed
// to be symmetric: failback uses a second MysqlStandbyCluster on the
// original-source cluster pointing at the new primary's prefix.
type MysqlStandbyCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlStandbyClusterSpec   `json:"spec,omitempty"`
	Status MysqlStandbyClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MysqlStandbyClusterList contains a list of MysqlStandbyCluster.
type MysqlStandbyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlStandbyCluster `json:"items"`
}

// MysqlStandbyClusterSpec defines the desired state of MysqlStandbyCluster.
// +kubebuilder:validation:XValidation:rule="self.transport != 'ObjectStore' || self.source.storage.type != 'S3' || (has(self.source.storage.s3.prefix) && size(self.source.storage.s3.prefix) > 0)",message="source.storage.s3.prefix must be non-empty when transport=ObjectStore and storage type=S3"
// +kubebuilder:validation:XValidation:rule="self.transport != 'ObjectStore' || self.source.storage.type == 'S3'",message="source.storage.type must be S3 when transport=ObjectStore; PVC-backed sources are not supported for cross-cluster DR (the operator pod does not mount the source cluster's backup PVC)"
type MysqlStandbyClusterSpec struct {
	// Transport selects the DR data path. Only ObjectStore is honoured in
	// v1alpha1; the Enum also lists Network, which is reserved for a future
	// cross-cluster MySQL replication mode and is NOT implemented — setting
	// it will produce a reconciler error (defense-in-depth check in
	// internal/controller). When unset, defaults to ObjectStore.
	// +kubebuilder:default="ObjectStore"
	// +kubebuilder:validation:Enum=ObjectStore;Network
	Transport StandbyTransport `json:"transport,omitempty"`

	// Source identifies the source MysqlFailoverGroup whose archive
	// this standby consumes. Cluster + Namespace are informational
	// (used in events + status); only the storage backend is load-bearing.
	Source StandbySource `json:"source"`

	// Template is the MysqlFailoverGroupSpec that the controller
	// materialises into a new MysqlFailoverGroup on Activate (Phase 3).
	// Required so the user declares the full activated shape at
	// standby-CR-creation time, not during an incident.
	Template StandbyFailoverGroupTemplate `json:"template"`

	// Freshness configures the Phase 1 bucket-discovery cadence via
	// discoveryInterval. The Phase 2 verification cadence and staleness
	// knobs are not part of v1alpha1 yet; they will be added
	// (backward-compatibly) when that code lands.
	// +optional
	Freshness *StandbyFreshnessSpec `json:"freshness,omitempty"`
}

// StandbyTransport selects the DR data path.
// +kubebuilder:validation:Enum=ObjectStore;Network
type StandbyTransport string

const (
	// StandbyTransportObjectStore is the only transport honoured in v1alpha1.
	StandbyTransportObjectStore StandbyTransport = "ObjectStore"
	// StandbyTransportNetwork is reserved for a future cross-cluster
	// MySQL replication mode and is not implemented.
	StandbyTransportNetwork StandbyTransport = "Network"
)

// StandbySource names the source archive the standby cluster consumes.
type StandbySource struct {
	// FailoverGroupName is the source MysqlFailoverGroup name.
	// Informational; used in events + status. Required so dashboards
	// can correlate a standby to its source.
	// +kubebuilder:validation:MinLength=1
	FailoverGroupName string `json:"failoverGroupName"`

	// Namespace of the source MysqlFailoverGroup in its own cluster.
	// Informational only; not used to make cross-cluster API calls.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Cluster is a free-form identifier for the source Kubernetes
	// cluster (e.g. "iad-prod"). Informational. Recommended convention
	// is the kubeconfig context name.
	// +optional
	Cluster string `json:"cluster,omitempty"`

	// Storage is the object-store backend the source operator writes
	// to. Reuses BackupStorage (api/v1alpha1/backup_types.go:388-444)
	// for consistency; the controller refuses to write to it
	// (except the dr-cursors/ subkey in Phase 2).
	// Only S3-compatible storage is usable: the standby reconciler
	// rejects PVC-backed sources with a ConfigError because the operator
	// pod does not mount the source cluster's backup PVC (see buildStoreCfg
	// in internal/controller/standbycluster_reconciler.go).
	Storage BackupStorage `json:"storage"`

	// ProfileName is the source backup profile under which dumps and
	// PITR binlogs are stored. The bucket prefix layout follows
	// <storage.prefix>/<mysqlbackup-name>/ for dumps and
	// <storage.prefix>/binlogs/ for the PITR archive
	// (docs/docs/backup-restore.mdx:570-577).
	// +kubebuilder:validation:MinLength=1
	ProfileName string `json:"profileName"`

	// Decryption, when set, provides the passphrase needed to decrypt
	// the source archive. Required when the source profile had
	// encryption enabled. Reuses BackupDecryptionSpec
	// (api/v1alpha1/backup_types.go:343-349). The Secret it references
	// must exist in this (DR) cluster's namespace — see
	// docs/docs/multi-cluster-dr.mdx for the cross-cluster passphrase
	// distribution runbook.
	// +optional
	Decryption *BackupDecryptionSpec `json:"decryption,omitempty"`
}

// StandbyFailoverGroupTemplate is a thin wrapper that lets the user
// declare the activated MysqlFailoverGroup's shape in advance.
// Behaves like a deferred MysqlFailoverGroupSpec: it is just YAML in
// the standby CR until activation (Phase 3), then it becomes the
// actual spec on the materialised group.
type StandbyFailoverGroupTemplate struct {
	// Name is the name of the MysqlFailoverGroup to create on
	// activate. Must be unique in this namespace and must not collide
	// with an existing MysqlFailoverGroup unless its ownerReferences
	// point back at this standby CR.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Spec is the embedded MysqlFailoverGroupSpec. Validated at
	// standby-CR-create-time but not actuated until activation (Phase 3).
	Spec MysqlFailoverGroupSpec `json:"spec"`
}

// StandbyFreshnessSpec configures the Phase 1 bucket-discovery cadence.
// Only discoveryInterval exists in v1alpha1. The Phase 2 verification
// cadence and staleness knobs (verifySchedule, verifyTimeZone,
// maxStaleness, suspend, retentionFloorRefresh) are not shipped yet; they
// will be added back (backward-compatibly) when the Phase 2 verification
// code that consumes them lands, rather than shipping inert fields that
// enforce nothing today.
type StandbyFreshnessSpec struct {
	// DiscoveryInterval controls how often the controller re-scans
	// the source bucket to refresh status.discovered.
	// Default: 5m. Minimum: 30s (shorter intervals produce no scheduled
	// poll; the reconciler also enforces this floor as defense-in-depth).
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('30s')",message="discoveryInterval must be at least 30s"
	DiscoveryInterval *metav1.Duration `json:"discoveryInterval,omitempty"`
}

// MysqlStandbyClusterStatus describes the observed state of a MysqlStandbyCluster.
type MysqlStandbyClusterStatus struct {
	// ObservedGeneration is the .metadata.generation this status was
	// computed against.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Discovered is the most recent successful bucket scan result.
	// Populated by the Phase 1 discovery loop.
	// +optional
	Discovered *StandbyDiscovered `json:"discovered,omitempty"`

	// LastVerified is the most recent terminal Succeeded
	// MysqlBackupVerification CR owned by this standby.
	// Populated by Phase 2+.
	// +optional
	LastVerified *StandbyLastVerified `json:"lastVerified,omitempty"`

	// Activation tracks the one-shot promote operation.
	// Populated by the Phase 3 activation state machine.
	// +optional
	Activation *StandbyActivationStatus `json:"activation,omitempty"`

	// MaterializedFailoverGroup is the name of the MysqlFailoverGroup
	// the controller created during activation. Empty until Activated.
	// +optional
	MaterializedFailoverGroup string `json:"materializedFailoverGroup,omitempty"`

	// Conditions represent the latest available observations of this
	// standby's state. Phase 1 conditions: BucketReadable,
	// SourceConfigKnown. Phase 2+: Restorable. Phase 3+:
	// ActivationInProgress, Active.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// StandbyDiscovered is the output of the Phase 1 bucket scan.
type StandbyDiscovered struct {
	// DumpLocation is the resolved location of the most recent
	// successful full dump (e.g.
	// "s3://bucket/prefix/orders-nightly-20260520/").
	// +optional
	DumpLocation string `json:"dumpLocation,omitempty"`

	// DumpName is the basename of the most recent successful dump.
	// +optional
	DumpName string `json:"dumpName,omitempty"`

	// DumpCompletionTime is the source-side completion time read from
	// the @.json metadata.
	// +optional
	DumpCompletionTime *metav1.Time `json:"dumpCompletionTime,omitempty"`

	// DumpGtidExecuted is the GTID set recorded in the dump metadata.
	// Required for activation PITR alignment (Phase 3).
	// +optional
	DumpGtidExecuted string `json:"dumpGtidExecuted,omitempty"`

	// DumpSizeBytes is the dump's reported size in bytes. 0 when unknown.
	// +optional
	DumpSizeBytes int64 `json:"dumpSizeBytes,omitempty"`

	// NOTE: Encrypted and EncryptionAlgorithm fields were intentionally
	// omitted here. BRV1 header detection (reading 32 bytes of each dump
	// to determine encryption status and algorithm) is a Phase 2 feature
	// that lands alongside the MysqlBackupVerification integration that
	// actually needs it. Adding unpopulated bool/string fields to the CRD
	// before that code exists creates dead-on-arrival schema.

	// OldestArchivedBinlogTime is the earliest first-event timestamp
	// across all manifest-<site>.json files in <prefix>/binlogs/.
	// +optional
	OldestArchivedBinlogTime *metav1.Time `json:"oldestArchivedBinlogTime,omitempty"`

	// NewestArchivedBinlogTime is the latest last-event timestamp
	// across all manifest-<site>.json files. Together with the newest
	// dump this defines the recoverable PITR window.
	// +optional
	NewestArchivedBinlogTime *metav1.Time `json:"newestArchivedBinlogTime,omitempty"`

	// ManifestCount is the number of per-site manifest files discovered
	// under <prefix>/binlogs/.
	// +optional
	ManifestCount int32 `json:"manifestCount,omitempty"`

	// ArchivedBinlogCount is the total count of archived binlog files
	// across all discovered manifests.
	// +optional
	ArchivedBinlogCount int32 `json:"archivedBinlogCount,omitempty"`

	// ArchivedBinlogBytes is the total size of archived binlog files
	// across all discovered manifests.
	// +optional
	ArchivedBinlogBytes int64 `json:"archivedBinlogBytes,omitempty"`

	// LastScanAt is the wall-clock time of the most recent successful
	// bucket scan.
	// +optional
	LastScanAt *metav1.Time `json:"lastScanAt,omitempty"`

	// Message is a human-readable status string from the discovery
	// loop. Empty on the happy path.
	// +optional
	Message string `json:"message,omitempty"`
}

// StandbyLastVerified summarises the most recent successful
// MysqlBackupVerification owned by this standby. Populated by Phase 2+.
type StandbyLastVerified struct {
	// VerificationName is the name of the MysqlBackupVerification CR.
	// +optional
	VerificationName string `json:"verificationName,omitempty"`

	// BackupName is the synthetic MysqlBackup that backed the
	// verification.
	// +optional
	BackupName string `json:"backupName,omitempty"`

	// StartTime is the verification's StartTime.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is the verification's CompletionTime.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// DurationSeconds is the wall-clock duration of the verification.
	// +optional
	DurationSeconds int64 `json:"durationSeconds,omitempty"`

	// ReplayedThroughBinlog is the PITR coordinate the verification
	// replayed up to (mirrors BinlogReplayMark,
	// api/v1alpha1/mysqlbackupverification_types.go:294-309).
	// +optional
	ReplayedThroughBinlog *BinlogReplayMark `json:"replayedThroughBinlog,omitempty"`
}

// StandbyActivationStatus tracks the one-shot promote operation (Phase 3).
type StandbyActivationStatus struct {
	// Phase is one of the values in StandbyActivationPhase.
	Phase StandbyActivationPhase `json:"phase,omitempty"`

	// ConfirmTokenUsed records the RFC3339 confirm token of the most
	// recent executed (or attempted) activation. A future activate
	// request (Phase 3) re-arms by supplying a strictly greater token;
	// subsequent reconciles ignore any token <= this value. (The spec
	// field that carries the request token is not part of v1alpha1 yet.)
	ConfirmTokenUsed string `json:"confirmTokenUsed,omitempty"`

	// StartTime is when the controller entered Validating.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the controller reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// DurationSeconds is CompletionTime - StartTime, rounded.
	// +optional
	DurationSeconds *int64 `json:"durationSeconds,omitempty"`

	// SourceDumpName is the dump that drove the activation (snapshot
	// of status.discovered.dumpName at Validating).
	// +optional
	SourceDumpName string `json:"sourceDumpName,omitempty"`

	// SourceDumpLocation is the resolved location of that dump.
	// +optional
	SourceDumpLocation string `json:"sourceDumpLocation,omitempty"`

	// SourceGtidExecuted is the dump-time GTID set.
	// +optional
	SourceGtidExecuted string `json:"sourceGtidExecuted,omitempty"`

	// TargetGtidExecuted is the GTID set on the materialised group
	// after restore + optional PITR replay. Populated on Activated.
	// +optional
	TargetGtidExecuted string `json:"targetGtidExecuted,omitempty"`

	// PitrStopDatetime is the normalised mysqlbinlog --stop-datetime
	// used for PITR replay (mirrors RestoreStatus.PitrStopDatetime,
	// api/v1alpha1/backup_types.go:915-916).
	// +optional
	PitrStopDatetime string `json:"pitrStopDatetime,omitempty"`

	// PitrReplayedBinlogCount is the number of archived binlogs
	// applied during activation.
	// +optional
	PitrReplayedBinlogCount int32 `json:"pitrReplayedBinlogCount,omitempty"`

	// ActiveSite is the materialised group's status.activeSite once
	// it becomes writable.
	// +optional
	ActiveSite string `json:"activeSite,omitempty"`

	// Reason is a machine-readable tag on Failed outcomes:
	// RestorableStale, RestoreFailed, ReplicationCatchupFailed,
	// TemplateInvalid, MaterializedGroupCollision, BucketUnreadable.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable line for kubectl describe.
	// +optional
	Message string `json:"message,omitempty"`
}

// StandbyActivationPhase is the discrete state machine for Phase 3
// activation.
// +kubebuilder:validation:Enum="";Validating;Restoring;Replaying;Provisioning;Activated;Failed
type StandbyActivationPhase string

const (
	// StandbyActivationPhaseNone is the zero value (no activation in progress).
	StandbyActivationPhaseNone StandbyActivationPhase = ""
	// StandbyActivationPhaseValidating means the controller is checking
	// preconditions and snapshotting the source dump reference.
	StandbyActivationPhaseValidating StandbyActivationPhase = "Validating"
	// StandbyActivationPhaseRestoring means the materialised
	// MysqlFailoverGroup has been created and initFromBackup is running.
	StandbyActivationPhaseRestoring StandbyActivationPhase = "Restoring"
	// StandbyActivationPhaseReplaying means the restore completed and the
	// controller is validating GTID coverage from PITR replay.
	StandbyActivationPhaseReplaying StandbyActivationPhase = "Replaying"
	// StandbyActivationPhaseProvisioning means GTID validation passed and
	// the controller is waiting for the materialised group to become Ready.
	StandbyActivationPhaseProvisioning StandbyActivationPhase = "Provisioning"
	// StandbyActivationPhaseActivated is the terminal success phase.
	StandbyActivationPhaseActivated StandbyActivationPhase = "Activated"
	// StandbyActivationPhaseFailed is the terminal failure phase.
	StandbyActivationPhaseFailed StandbyActivationPhase = "Failed"
)

// Condition type constants for MysqlStandbyCluster.
// Phase 1 uses BucketReadable and SourceConfigKnown.
// Phase 2+ adds Restorable.
// Phase 3+ adds ActivationInProgress and Active.
const (
	// StandbyConditionBucketReadable is True when the source
	// bucket+prefix was successfully listed within the last discovery
	// interval. Reason=ListSucceeded or ListFailed; Message carries
	// the underlying error on False.
	StandbyConditionBucketReadable = "BucketReadable"

	// StandbyConditionSourceConfigKnown is True when the controller has
	// resolved at least one full dump AND at least one binlog manifest
	// under the configured prefix. The default `kubectl get` printcolumn
	// surfaces this as the neutral `SourceKnown`.
	//
	// Reasons:
	//   - DumpFound (True): a dump and at least one binlog manifest exist.
	//   - NoDumpFound (False): no dump @.json under the prefix.
	//   - NoBinlogManifests (False): a dump WAS found, but no PITR binlog
	//     manifests exist under <prefix>/binlogs/ yet. This is NOT a
	//     misconfiguration — it is the expected state for a dump-only
	//     source (no continuous binlog archival), or a brand-new source
	//     whose first binlog manifest has not yet been uploaded. Recovery
	//     is limited to the dump (no point-in-time window).
	//   - MetadataUnreadable (False): the dump @.json could not be parsed.
	//   - ConfigError (False): propagated from a storage-backend error.
	StandbyConditionSourceConfigKnown = "SourceConfigKnown"

	// StandbyConditionRestorable is True when the most recent owned
	// MysqlBackupVerification is Succeeded and within a future staleness
	// threshold (Phase 2). Populated by Phase 2+.
	// Reason=NoVerification, Stale, or VerificationFailed on False.
	StandbyConditionRestorable = "Restorable"

	// StandbyConditionActivationInProgress is True while the activation
	// phase is non-terminal (Validating through Provisioning).
	// Populated by Phase 3+.
	StandbyConditionActivationInProgress = "ActivationInProgress"

	// StandbyConditionActive is True iff status.activation.phase=Activated.
	// Populated by Phase 3+.
	StandbyConditionActive = "Active"
)

func init() {
	SchemeBuilder.Register(&MysqlStandbyCluster{}, &MysqlStandbyClusterList{})
}
