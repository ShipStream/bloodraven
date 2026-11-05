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
