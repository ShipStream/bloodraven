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
// Phase 3 adds one-command activation: an admin-confirmed activate
// request (spec.activate.confirm) materialises a new MysqlFailoverGroup
// in this namespace loaded from the source archive.
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

	// Freshness configures the continuous-restore verification cadence
	// and the staleness threshold at which Restorable=False is set.
	// Phase 1 only reads discoveryInterval; other fields are consumed
	// by Phase 2+.
	// +optional
	Freshness *StandbyFreshnessSpec `json:"freshness,omitempty"`

	// Activate is the one-shot promote contract (Phase 3). The
	// controller ignores this block until spec.activate.confirm is set
	// to an RFC3339 timestamp strictly greater than
	// status.activation.confirmTokenUsed. See RestoreInPlaceSpec
	// (api/v1alpha1/backup_types.go:723-732) for the prior art.
	// +optional
	Activate *StandbyActivateSpec `json:"activate,omitempty"`
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

// StandbyFreshnessSpec configures the verification cadence.
// Phase 1 reads discoveryInterval only; all other fields are reserved
// for Phase 2 (scheduled MysqlBackupVerification runs).
type StandbyFreshnessSpec struct {
	// DiscoveryInterval controls how often the controller re-scans
	// the source bucket to refresh status.discovered.
	// Default: 5m. Minimum: 30s (shorter intervals produce no scheduled
	// poll; the reconciler also enforces this floor as defense-in-depth).
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('30s')",message="discoveryInterval must be at least 30s"
	DiscoveryInterval *metav1.Duration `json:"discoveryInterval,omitempty"`

	// VerifySchedule is the cron expression for scheduled
	// MysqlBackupVerification runs against the most recently
	// discovered dump. Evaluated in VerifyTimeZone.
	// Default: "0 4 * * *" (daily at 04:00 UTC).
	// Phase 2+ only.
	// +kubebuilder:default="0 4 * * *"
	VerifySchedule string `json:"verifySchedule,omitempty"`

	// VerifyTimeZone is the IANA timezone name used to evaluate
	// VerifySchedule. Defaults to "Etc/UTC" for the same
	// reproducibility reasons as BackupSchedule.TimeZone
	// (api/v1alpha1/backup_types.go:516-526).
	// Phase 2+ only.
	// +kubebuilder:default="Etc/UTC"
	VerifyTimeZone string `json:"verifyTimeZone,omitempty"`

	// MaxStaleness is the threshold past which Restorable=False is
	// stamped because no recent Succeeded verification has occurred.
	// Default: 48h. Minimum: 5m (a shorter freshness window is not
	// meaningful for cross-cluster RPO purposes). Phase 2+ only.
	// +kubebuilder:default="48h"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('5m')",message="maxStaleness must be at least 5m"
	MaxStaleness *metav1.Duration `json:"maxStaleness,omitempty"`

	// Suspend pauses the verification CronJob without deleting the
	// standby CR. Useful when the source bucket is temporarily
	// unreadable and verification noise would obscure other signals.
	// Phase 2+ only.
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`

	// RetentionFloorRefresh is how often the controller refreshes
	// the dr-cursors/<name>.json bucket file that signals to the source
	// operator the oldest binlog timestamp the DR cluster still needs.
	// Default 5m; the file TTL is 60m (controller-side hard-coded).
	// Minimum: 30s. Phase 2+ only.
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('30s')",message="retentionFloorRefresh must be at least 30s"
	RetentionFloorRefresh *metav1.Duration `json:"retentionFloorRefresh,omitempty"`
}

// StandbyActivateSpec gates the one-shot promote action (Phase 3).
// Mirrors RestoreInPlaceSpec's confirm-token pattern
// (api/v1alpha1/backup_types.go:723-732).
type StandbyActivateSpec struct {
	// PHASE 3 IMPLEMENTER GUARD (kept as a separate comment group, out of the
	// doc comment below, so controller-gen does not fold it into the CRD field
	// description): this field is live API as of Phase 1, so a confirm token
	// can be persisted on a CR long before any activation code exists. In
	// Phase 1 status.activation is never written, leaving confirmTokenUsed
	// empty — so a pre-existing confirm would compare as strictly greater than
	// "" and read as already-armed. When Phase 3 lands, the first reconcile on
	// an activation-capable build MUST seed status.activation.confirmTokenUsed
	// from the current spec.activate.confirm (record-not-fire) and treat only
	// a subsequent bump as an activation request. Otherwise a months-old
	// confirm fires an unintended promote the moment the operator is upgraded.

	// Confirm is the required RFC3339 anti-fat-finger token. The
	// controller refuses to activate unless Confirm parses and is
	// strictly greater than status.activation.confirmTokenUsed. Bump
	// to a fresh now() to retry after a Failed activation.
	// Phase 3+ only.
	// +kubebuilder:validation:MinLength=1
	Confirm string `json:"confirm"`

	// PointInTime, when set, instructs the controller to replay
	// archived binlogs past the dump's GTID set up to the configured
	// stopDatetime. Reuses PointInTimeSpec
	// (api/v1alpha1/backup_types.go:191-210).
	// Phase 3+ only.
	// +optional
	PointInTime *PointInTimeSpec `json:"pointInTime,omitempty"`

	// AcceptUnverified, when true, lets the controller proceed even
	// if status.conditions[Restorable] is not True or is older than
	// freshness.maxStaleness. Use only when the world is on fire and
	// a stale-but-present archive is better than no archive.
	// Defaults to false.
	// Phase 3+ only.
	// +kubebuilder:default=false
	AcceptUnverified bool `json:"acceptUnverified,omitempty"`

	// RestoreTimeout caps the wall-clock duration the controller
	// waits for the materialised group's restore to reach Succeeded
	// before stamping ActivationFailed. Default 2h, matching the
	// existing backup ActiveDeadlineSeconds default
	// (api/v1alpha1/backup_types.go:62-65).
	// Phase 3+ only.
	// +kubebuilder:default="2h"
	RestoreTimeout *metav1.Duration `json:"restoreTimeout,omitempty"`
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

	// ConfirmTokenUsed is the spec.activate.confirm value of the most
	// recent executed (or attempted) activation. Subsequent reconciles
	// ignore any spec.confirm <= this value; bumping to a strictly
	// greater RFC3339 timestamp re-arms.
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

	// StandbyConditionSourceConfigKnown is True when the controller
	// has resolved at least one full dump and at least one binlog
	// manifest under the configured prefix. Reason=DumpFound,
	// NoDumpFound, or NoBinlogManifests.
	StandbyConditionSourceConfigKnown = "SourceConfigKnown"

	// StandbyConditionRestorable is True when the most recent owned
	// MysqlBackupVerification is Succeeded and within
	// freshness.maxStaleness. Populated by Phase 2+.
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
