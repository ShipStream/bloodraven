package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PITRArchiveEncryptionMode is the encryption mode recorded in a PITR
// archive's descriptor (<prefix>/binlogs/archive.json).
// +kubebuilder:validation:Enum=None;Passphrase
type PITRArchiveEncryptionMode string

const (
	// PITRArchiveEncryptionNone means the archive is stored in plaintext.
	PITRArchiveEncryptionNone PITRArchiveEncryptionMode = "None"
	// PITRArchiveEncryptionPassphrase means every archive object except the
	// descriptor is BRV1-encrypted with a passphrase-derived key.
	PITRArchiveEncryptionPassphrase PITRArchiveEncryptionMode = "Passphrase"
)

// EncryptionAlgorithm is a typed encryption algorithm identifier.
// +kubebuilder:validation:Enum=AES-256-GCM
type EncryptionAlgorithm string

const (
	// EncryptionAlgorithmAES256GCM is the only supported algorithm.
	EncryptionAlgorithmAES256GCM EncryptionAlgorithm = "AES-256-GCM"
)

// PITRArchiveSource identifies the failover group that created a PITR
// archive. The UID makes the identity survive a delete and re-create
// under the same name.
type PITRArchiveSource struct {
	// Namespace is the source group's namespace.
	// +required
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`
	// Name is the source group's name.
	// +required
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// UID is the source group's metadata.uid.
	// +required
	UID types.UID `json:"uid"`
}

// PITRArchiveIdentity is the identity read from a PITR archive's
// descriptor.
type PITRArchiveIdentity struct {
	// ArchiveID is the descriptor's archive ID, derived from the source
	// group's UID and the normalized destination.
	// +required
	// +kubebuilder:validation:Pattern=`^ar1-[a-z2-7]{26}$`
	ArchiveID string `json:"archiveID"`
	// Destination is the normalized archive destination the descriptor
	// was read from (s3://… or pvc://…).
	// +optional
	// +kubebuilder:validation:MaxLength=4096
	Destination string `json:"destination,omitempty"`
	// Source is the group that created the archive.
	// +required
	Source PITRArchiveSource `json:"source"`
	// EncryptionMode is the archive's encryption mode.
	// +required
	EncryptionMode PITRArchiveEncryptionMode `json:"encryptionMode"`
	// Algorithm is the encryption algorithm; empty for None.
	// +optional
	Algorithm EncryptionAlgorithm `json:"algorithm,omitempty"`
}

// PITRArchiveObservedState is the archive state the group's sidecars
// report for the active PITR destination.
// +kubebuilder:validation:Enum=AwaitingDescriptor;Verified;Refused
type PITRArchiveObservedState string

const (
	// PITRArchiveAwaitingDescriptor means no sidecar has verified a
	// descriptor yet (no writer has created it, or a replica is waiting for
	// the writer).
	PITRArchiveAwaitingDescriptor PITRArchiveObservedState = "AwaitingDescriptor"
	// PITRArchiveVerified means every reporting sidecar verified the
	// descriptor against this group's identity.
	PITRArchiveVerified PITRArchiveObservedState = "Verified"
	// PITRArchiveRefused means at least one sidecar refused the archive;
	// Reason says why.
	PITRArchiveRefused PITRArchiveObservedState = "Refused"
)

// PITRArchiveRefusalReason is why a sidecar refused a PITR archive. The
// same values are PITRArchiveReady=False reasons.
// +kubebuilder:validation:Enum=ArchiveForeign;ArchiveCopy;ArchiveLegacy;ArchiveDescriptorMissing;ArchiveEncryptionMismatch;ArchiveKeyMismatch;ArchiveVersionUnsupported;ArchiveStoreUnsupported;ArchiveUnsafeHistory
type PITRArchiveRefusalReason string

// PITRArchiveObservation is status.pitr.archive: the archive identity the
// group's sidecars observed at the active PITR destination.
type PITRArchiveObservation struct {
	// Destination is the normalized active PITR destination.
	// +required
	// +kubebuilder:validation:MaxLength=4096
	Destination string `json:"destination"`
	// ArchiveID is the archive ID the sidecars verified. Empty until a
	// descriptor has been verified.
	// +optional
	// +kubebuilder:validation:Pattern=`^(ar1-[a-z2-7]{26})?$`
	ArchiveID string `json:"archiveID,omitempty"`
	// EncryptionMode is the archive's encryption mode.
	// +optional
	EncryptionMode PITRArchiveEncryptionMode `json:"encryptionMode,omitempty"`
	// State summarizes the sidecars' reports.
	// +required
	State PITRArchiveObservedState `json:"state"`
	// Reason is set when State is Refused.
	// +optional
	Reason PITRArchiveRefusalReason `json:"reason,omitempty"`
	// KeyRevision is the snapshot name every counted sidecar echoed (#203);
	// empty for None.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	KeyRevision string `json:"keyRevision,omitempty"`
	// ObservedTime is when the operator last derived this observation from
	// sidecar reports.
	// +optional
	ObservedTime *metav1.MicroTime `json:"observedTime,omitempty"`
}

// PointInTimeMode is how a point-in-time replay chose its end point.
// +kubebuilder:validation:Enum=Timestamp;Latest
type PointInTimeMode string

const (
	// PointInTimeModeTimestamp replays the history of the first archived
	// transaction committed at or after the target, cut just before it.
	PointInTimeModeTimestamp PointInTimeMode = "Timestamp"
	// PointInTimeModeLatest replays to the unique archive head.
	PointInTimeModeLatest PointInTimeMode = "Latest"
)

// PointInTimeOutcome is the outcome of a point-in-time replay.
// +kubebuilder:validation:Enum=TargetCovered;Partial;ArchiveHead
type PointInTimeOutcome string

const (
	// PointInTimeTargetCovered means an archived transaction at or after
	// the target exists, and the restore is that transaction's history cut
	// just before it.
	PointInTimeTargetCovered PointInTimeOutcome = "TargetCovered"
	// PointInTimePartial means no archived transaction at or after the
	// target exists and allowPartial accepted the archive head instead.
	PointInTimePartial PointInTimeOutcome = "Partial"
	// PointInTimeArchiveHead means a latest-mode replay to the archive
	// head.
	PointInTimeArchiveHead PointInTimeOutcome = "ArchiveHead"
)

// ReplayedBinlogRef names one archived binlog by site and file name.
type ReplayedBinlogRef struct {
	// Site is the site whose archive holds the file.
	// +required
	// +kubebuilder:validation:Pattern=`^[a-z0-9-]+$`
	// +kubebuilder:validation:MaxLength=63
	Site string `json:"site"`
	// Name is the binlog file name, for example mysql-bin.000042.
	// +required
	// +kubebuilder:validation:MaxLength=255
	Name string `json:"name"`
}

// PointInTimeResult is the certified outcome of a point-in-time replay,
// read from the replay's verdict. Restores (status.restore.pointInTime and
// status.restoreInPlace.pointInTime), verifications (status.pointInTime)
// and standby activations (status.activation.pointInTime) share it.
type PointInTimeResult struct {
	// Mode is the replay mode.
	// +required
	Mode PointInTimeMode `json:"mode"`
	// StopTime is the requested target, exact to the microsecond. Unset in
	// Latest mode.
	// +optional
	StopTime *metav1.MicroTime `json:"stopTime,omitempty"`
	// Outcome is how the end point was reached.
	// +required
	Outcome PointInTimeOutcome `json:"outcome"`
	// Archive identifies the archive the replay read.
	// +optional
	Archive *PITRArchiveIdentity `json:"archive,omitempty"`
	// ReplaySites are the sites whose archived binlogs were staged, sorted.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9-]+$`
	// +kubebuilder:validation:items:MaxLength=63
	ReplaySites []string `json:"replaySites,omitempty"`
	// SegmentCount is the number of replay segments.
	// +optional
	SegmentCount int32 `json:"segmentCount,omitempty"`
	// ReplayedBinlogCount is the number of distinct archived binlogs the
	// segments read.
	// +optional
	ReplayedBinlogCount int32 `json:"replayedBinlogCount,omitempty"`
	// LastReplayedBinlog is the file of the last segment.
	// +optional
	LastReplayedBinlog *ReplayedBinlogRef `json:"lastReplayedBinlog,omitempty"`
	// ReplayedTransactionCount is the number of transactions replayed on
	// top of the backup.
	// +optional
	ReplayedTransactionCount int64 `json:"replayedTransactionCount,omitempty"`
	// NewestReplayedCommitTime is the newest original commit time among
	// the replayed transactions.
	// +optional
	NewestReplayedCommitTime *metav1.MicroTime `json:"newestReplayedCommitTime,omitempty"`
	// ExcludedDivergentTransactionCount is |D|: transactions committed between the backup's newest transaction and
	// the target, outside the target history, excluded by excludeGtids.
	// +optional
	ExcludedDivergentTransactionCount int64 `json:"excludedDivergentTransactionCount,omitempty"`
	// OnHistoryExclusions are excluded transactions on the target history that precede a replayed one (D-48).
	// Their GTIDs were skipped, but later row images may carry their effects; the result is not certified for them.
	// Empty when the set is too long to record; OnHistoryExclusionCount still counts it.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	OnHistoryExclusions string `json:"onHistoryExclusions,omitempty"`
	// OnHistoryExclusionCount is the number of GTIDs in OnHistoryExclusions.
	// +optional
	OnHistoryExclusionCount int64 `json:"onHistoryExclusionCount,omitempty"`
	// GtidExecuted is the target's @@global.gtid_executed after replay,
	// which the replay proved equal to the target history minus
	// excludeGtids. Empty when longer than 2048 characters; see
	// GtidExecutedSHA256.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	GtidExecuted string `json:"gtidExecuted,omitempty"`
	// GtidExecutedSHA256 is the hex SHA-256 of GtidExecuted as the server
	// printed it (newlines removed).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9a-f]{64})?$`
	GtidExecutedSHA256 string `json:"gtidExecutedSHA256,omitempty"`
}
