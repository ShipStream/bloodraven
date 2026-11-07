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
	// PITRArchiveAwaitingDescriptor means not every site has verified the
	// descriptor yet and none refused it: no writer has created it, a
	// replica is waiting for the writer, or a site has not reported the
	// active destination yet. Sites that did verify may already supply
	// ArchiveID.
	PITRArchiveAwaitingDescriptor PITRArchiveObservedState = "AwaitingDescriptor"
	// PITRArchiveVerified means every site of the group verified the
	// descriptor against this group's identity, all with one archive ID.
	PITRArchiveVerified PITRArchiveObservedState = "Verified"
	// PITRArchiveRefused means at least one site refused the archive
	// (Reason says why), or sites verified different archive IDs at one
	// destination (Reason and ArchiveID empty: nothing is certified).
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
	// EncryptionMode is the encryption mode the group's profile configures
	// for the archive; once a site has verified the descriptor it is the
	// mode that site reported, which equals the descriptor's. While the
	// archive is refused (ArchiveEncryptionMismatch, say) it is still the
	// configured mode, not the descriptor's.
	// +optional
	EncryptionMode PITRArchiveEncryptionMode `json:"encryptionMode,omitempty"`
	// State summarizes the sidecars' reports.
	// +required
	State PITRArchiveObservedState `json:"state"`
	// Reason is set when State is Refused by a site's refusal; empty when
	// sites verified conflicting archive IDs.
	// +optional
	Reason PITRArchiveRefusalReason `json:"reason,omitempty"`
	// KeyRevision is the snapshot name every counted sidecar echoed (#203);
	// empty for None.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	KeyRevision string `json:"keyRevision,omitempty"`
	// ObservedTime is when this observation last changed (the operator
	// keeps the previous time while sidecar reports derive the same
	// observation), so it is not a freshness heartbeat.
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
// status.restoreInPlace.pointInTime) and verifications
// (status.pointInTime) share it.
type PointInTimeResult struct {
	// Mode is the replay mode.
	// +required
	Mode PointInTimeMode `json:"mode"`
	// StopTime is the requested target (a whole second; commit times are
	// compared against it to the microsecond). Unset in Latest mode.
	// +optional
	StopTime *metav1.Time `json:"stopTime,omitempty"`
	// Outcome is how the end point was reached.
	// +required
	Outcome PointInTimeOutcome `json:"outcome"`
	// Archive identifies the archive the replay read.
	// +optional
	Archive *PITRArchiveIdentity `json:"archive,omitempty"`
	// ReplaySites are the sites whose archived binlogs were staged, sorted;
	// see ReplaySitesOmitted for the ones the verdict left out.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9-]+$`
	// +kubebuilder:validation:items:MaxLength=63
	ReplaySites []string `json:"replaySites,omitempty"`
	// ReplaySitesOmitted counts further staged sites the verdict did not
	// name: its site list is bounded, so ReplaySites is complete only when
	// this is zero.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ReplaySitesOmitted int32 `json:"replaySitesOmitted,omitempty"`
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

// PITRArchiveBindingState is whether a PITR archive binding's encryption
// identity is accepted.
// +kubebuilder:validation:Enum=Pending;Bound;Refused
type PITRArchiveBindingState string

const (
	// PITRArchiveBindingPending means the binding's candidate identity is
	// being authenticated (a check Job) or established (a writer creates
	// the descriptor and proves it).
	PITRArchiveBindingPending PITRArchiveBindingState = "Pending"
	// PITRArchiveBindingBound means the identity was proven against the
	// archive's descriptor and is the one every consumer uses.
	PITRArchiveBindingBound PITRArchiveBindingState = "Bound"
	// PITRArchiveBindingRefused means the last candidate was refused;
	// Reason says why.
	PITRArchiveBindingRefused PITRArchiveBindingState = "Refused"
)

// PITRArchiveBindingReason qualifies a binding's State.
// Pending: Authenticating, Establishing. Bound: Bound, KeyMaterialLost.
// Refused: the rest.
// +kubebuilder:validation:Enum=Authenticating;Establishing;Bound;KeyMaterialLost;KeyMismatch;ArchiveForeign;ArchiveCopy;ArchiveLegacy;ArchiveDescriptorMissing;ArchiveNotFound;ArchiveEncryptionMismatch;ArchiveVersionUnsupported;ArchiveListFailed
type PITRArchiveBindingReason string

// PITRArchiveBinding is the encryption identity the group accepted for one
// PITR archive destination it writes or reads (#203). Accepted encrypted
// bindings are reconstructible from their key snapshot's acceptance
// annotation; plaintext bindings are re-proven (the descriptor records the
// mode).
type PITRArchiveBinding struct {
	// ID identifies the binding: "pab-" and the first 16 hex digits of the
	// SHA-256 of "bloodraven-pitr-binding-v1\0" and Destination. It is the
	// release annotation's subject.
	// +required
	// +kubebuilder:validation:Pattern=`^pab-[0-9a-f]{16}$`
	ID string `json:"id"`
	// Destination is the normalized archive destination (s3://… or
	// pvc://…).
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	Destination string `json:"destination"`
	// ArchiveID is the descriptor's archive ID once a descriptor was
	// verified or created.
	// +optional
	// +kubebuilder:validation:Pattern=`^(ar1-[a-z2-7]{26})?$`
	ArchiveID string `json:"archiveID,omitempty"`
	// Source is the group that created the archive. Set from a verified
	// descriptor, and from a refusal that names the owner (ArchiveForeign).
	// +optional
	Source *PITRArchiveSource `json:"source,omitempty"`
	// Encryption is the accepted identity when State is Bound. Otherwise it
	// is the candidate under test (Pending) or the last candidate tried
	// (Refused); read it only when State is Bound.
	// +required
	Encryption PITRArchiveBindingEncryption `json:"encryption"`
	// State is whether Encryption is accepted.
	// +required
	State PITRArchiveBindingState `json:"state"`
	// Reason qualifies State.
	// +required
	Reason PITRArchiveBindingReason `json:"reason"`
	// Message is a human-readable explanation of Reason.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Message string `json:"message,omitempty"`
	// Check is the check Job authenticating Encryption, while one runs.
	// +optional
	Check *PITRArchiveBindingCheck `json:"check,omitempty"`
	// LastTransitionTime is when State or Reason last changed.
	// +optional
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

// PITRArchiveBindingEncryption is a binding's encryption identity: the mode
// and algorithm the archive's descriptor records and, for Passphrase, the
// operator-made key snapshot every consumer mounts.
type PITRArchiveBindingEncryption struct {
	// Mode is the archive's encryption mode.
	// +required
	Mode PITRArchiveEncryptionMode `json:"mode"`
	// Algorithm is empty for None.
	// +optional
	Algorithm EncryptionAlgorithm `json:"algorithm,omitempty"`
	// KeySecretName is the immutable, operator-made key snapshot
	// (mysql-<group>-pitr-key-<h10>); empty for None.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	KeySecretName string `json:"keySecretName,omitempty"`
}

// PITRArchiveBindingCheck names the check Job authenticating a binding's
// candidate identity.
type PITRArchiveBindingCheck struct {
	// JobName is the check Job's name.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	JobName string `json:"jobName"`
	// JobUID is the only Job whose result is accepted.
	// +optional
	JobUID types.UID `json:"jobUID,omitempty"`
}
