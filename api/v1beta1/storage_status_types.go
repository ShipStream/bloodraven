package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// StorageStatus is status.storage: the observed state of the sites' data
// volumes. The group reconciler writes it; no other writer changes it.
type StorageStatus struct {
	// Sites holds one entry per site of spec.sites.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	Sites []SiteStorageStatus `json:"sites,omitempty"`
}

// SiteStorageStatus is the observed state of one site's data PVC.
type SiteStorageStatus struct {
	// Name is the site, the key of the list.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// ClaimName is the site's data PersistentVolumeClaim.
	// +optional
	ClaimName string `json:"claimName,omitempty"`

	// ClaimUID is the UID of the claim observed. A replacement claim has
	// another one.
	// +optional
	ClaimUID types.UID `json:"claimUID,omitempty"`

	// StorageClassName is the class the claim was created with, read from
	// the claim itself.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// DesiredSize is spec.sites[].storage.size as written in the spec this
	// observation was made against.
	// +optional
	DesiredSize string `json:"desiredSize,omitempty"`

	// RequestedSize is the claim's spec.resources.requests.storage.
	// +optional
	RequestedSize string `json:"requestedSize,omitempty"`

	// Capacity is the claim's status.capacity.storage: the size of the
	// bound volume. Empty while the claim is not bound.
	// +optional
	Capacity string `json:"capacity,omitempty"`

	// AbandonedSize is a desired size the operator no longer raises this
	// claim to: the claim's request was lowered below it after it had
	// requested at least that size, which is how an administrator recovers
	// from a failed expansion. It is kept while the claim and the desired
	// size stay the same; raising spec.sites[].storage.size above it
	// resumes growth.
	// +optional
	AbandonedSize string `json:"abandonedSize,omitempty"`

	// Phase summarizes the volume against the desired size:
	//   - Satisfied: the claim requests at least the desired size and the
	//     volume holds what it requests.
	//   - Pending: the claim is not bound yet (or does not exist); growth
	//     is not requested until it is.
	//   - Expanding: the claim requests the desired size and the storage
	//     driver is growing the volume.
	//   - FileSystemResizePending: the volume has grown and the file
	//     system on it grows when the node next mounts it; on a driver
	//     without online expansion that needs a restart of the site's pod.
	//   - Underspecified: the claim requests more than the desired size
	//     (it was expanded directly). The claim is kept, but a replacement
	//     would be created at the smaller desired size: raise
	//     spec.sites[].storage.size to at least the claim's request.
	//   - Unsupported: the desired size is larger than the claim and the
	//     claim's StorageClass does not allow volume expansion (or the
	//     claim has no class, or the class is gone).
	//   - Failed: the operator's request or its read of the claim's
	//     StorageClass was refused, the storage driver reports the expansion
	//     infeasible or reports a resize error on the claim, or the claim's
	//     request was lowered below the desired size after a failed
	//     expansion (AbandonedSize); Message says why.
	//   - Retrying: a transient error prevented the observation or the
	//     request; the operator retries.
	// +required
	Phase SiteStoragePhase `json:"phase"`

	// Message says what the phase means for this site and, when an
	// administrator has to act, what to do.
	// +optional
	Message string `json:"message,omitempty"`

	// LastTransitionTime is when Phase last changed.
	// +optional
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

// SiteStoragePhase is status.storage.sites[].phase.
// +kubebuilder:validation:Enum=Satisfied;Pending;Expanding;FileSystemResizePending;Underspecified;Unsupported;Failed;Retrying
type SiteStoragePhase string

const (
	SiteStorageSatisfied               SiteStoragePhase = "Satisfied"
	SiteStoragePending                 SiteStoragePhase = "Pending"
	SiteStorageExpanding               SiteStoragePhase = "Expanding"
	SiteStorageFileSystemResizePending SiteStoragePhase = "FileSystemResizePending"
	SiteStorageUnderspecified          SiteStoragePhase = "Underspecified"
	SiteStorageUnsupported             SiteStoragePhase = "Unsupported"
	SiteStorageFailed                  SiteStoragePhase = "Failed"
	SiteStorageRetrying                SiteStoragePhase = "Retrying"
)

// SiteStorage returns the storage observation of site, or nil.
func (s *MysqlFailoverGroupStatus) SiteStorage(site string) *SiteStorageStatus {
	if s == nil || s.Storage == nil {
		return nil
	}
	for i := range s.Storage.Sites {
		if s.Storage.Sites[i].Name == site {
			return &s.Storage.Sites[i]
		}
	}
	return nil
}
