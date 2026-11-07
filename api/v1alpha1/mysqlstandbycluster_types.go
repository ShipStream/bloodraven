package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=msc;mscs,categories=bloodraven;mysql;dr
// +kubebuilder:printcolumn:name="Dump",type=string,JSONPath=`.status.discovered.latestDump.name`
// +kubebuilder:printcolumn:name="NewestBinlog",type=date,JSONPath=`.status.discovered.binlogArchive.newestEventTime`
// +kubebuilder:printcolumn:name="BucketReadable",type=string,JSONPath=`.status.conditions[?(@.type=="BucketReadable")].status`
// +kubebuilder:printcolumn:name="SourceKnown",type=string,JSONPath=`.status.conditions[?(@.type=="SourceConfigKnown")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MysqlStandbyCluster is a passive observability descriptor for a DR
// relationship from this cluster to a source MysqlFailoverGroup's dumps and
// PITR archive in a shared object store. The controller scans the source
// prefix every spec.freshness.discoveryInterval and publishes the
// BucketReadable and SourceConfigKnown conditions with status.discovered
// (the newest dump and the verified binlog archive window). It never
// contacts MySQL, runs no restore Jobs and writes nothing to the bucket.
//
// One MysqlStandbyCluster per DR relationship per namespace. Failback uses a
// second MysqlStandbyCluster on the original source cluster pointing at the
// new primary's prefix.
type MysqlStandbyCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec MysqlStandbyClusterSpec `json:"spec"`
	// +optional
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
type MysqlStandbyClusterSpec struct {
	// Transport selects the DR data path. ObjectStore, the only transport,
	// reads the source group's dumps and PITR archive from its object store.
	// +kubebuilder:default="ObjectStore"
	// +optional
	Transport StandbyTransport `json:"transport,omitempty"`

	// Source is the source group's object store and, for an encrypted
	// source, the passphrase that decrypts it.
	// +required
	Source StandbySource `json:"source"`

	// Freshness configures the bucket-discovery cadence.
	// +optional
	Freshness *StandbyFreshnessSpec `json:"freshness,omitempty"`
}

// StandbyTransport selects the DR data path.
// +kubebuilder:validation:Enum=ObjectStore
type StandbyTransport string

const (
	// StandbyTransportObjectStore reads the source's dumps and PITR archive
	// from its object store.
	StandbyTransportObjectStore StandbyTransport = "ObjectStore"
)

// StandbySource names the object store the source group writes its dumps
// and PITR archive to. The standby only reads it.
type StandbySource struct {
	// S3 is the S3-compatible object store and prefix the source group's
	// backup profile writes to.
	// +required
	S3 StandbyS3Source `json:"s3"`

	// Decryption provides the passphrase that decrypts the source archive.
	// Required when the source profile encrypts. The Secret must exist in
	// this (DR) cluster's namespace; see
	// https://bloodraven.dev/docs/architecture/multi-cluster-dr for
	// distributing the passphrase across clusters.
	// +optional
	Decryption *BackupDecryptionSpec `json:"decryption,omitempty"`
}

// StandbyS3Source is the source group's S3 backup destination, read-only.
// The fields mean what they mean on the source profile's S3Storage, except
// that the prefix is required: the standby scans "<prefix>/<dump>/" for
// dumps and "<prefix>/binlogs/" for the PITR archive, never a bucket root.
type StandbyS3Source struct {
	// Bucket is the S3 bucket name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 63",message="bucket must be at most 63 bytes (UTF-8)"
	// +required
	Bucket string `json:"bucket"`

	// Prefix is the source profile's object-name prefix inside the bucket.
	// A leading or trailing "/" is ignored; at least one segment is
	// required. Empty segments ("a//b") and "." or ".." segments are
	// refused, as on S3Storage.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 512",message="prefix must be at most 512 bytes (UTF-8)"
	// +kubebuilder:validation:Pattern=`^/?([^/.][^/]*|\.[^/.][^/]*|\.\.[^/]+)(/([^/.][^/]*|\.[^/.][^/]*|\.\.[^/]+))*/?$`
	// +required
	Prefix string `json:"prefix"`

	// Region is the AWS region. Optional for non-AWS S3 stores.
	// +optional
	Region string `json:"region,omitempty"`

	// EndpointURL overrides the S3 endpoint (e.g. MinIO, Ceph, Wasabi).
	// Must be an absolute http:// or https:// URL without credentials, a
	// query or a fragment.
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 512",message="endpointURL must be at most 512 bytes (UTF-8)"
	// +kubebuilder:validation:Pattern=`^https?://[^\s/?#@]+(/[^\s?#]*)?$`
	// +optional
	EndpointURL string `json:"endpointURL,omitempty"`

	// CredentialsSecretName names a Secret in this namespace with read access
	// to the bucket. Expected keys: AWS_ACCESS_KEY_ID and
	// AWS_SECRET_ACCESS_KEY (required), AWS_SESSION_TOKEN and AWS_REGION
	// (optional; AWS_REGION overrides region).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	CredentialsSecretName string `json:"credentialsSecretName"`
}

// StandbyFreshnessSpec configures the bucket-discovery cadence.
type StandbyFreshnessSpec struct {
	// DiscoveryInterval controls how often the controller re-scans
	// the source bucket to refresh status.discovered.
	// Default: 5m. Bounds: 30s to 24h (shorter intervals produce no
	// scheduled poll; the reconciler also enforces the floor as
	// defense-in-depth).
	// +kubebuilder:default="5m0s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('30s') && duration(self) <= duration('24h')",message="discoveryInterval must be between 30s and 24h"
	// +optional
	DiscoveryInterval *metav1.Duration `json:"discoveryInterval,omitempty"`
}

// MysqlStandbyClusterStatus describes the observed state of a MysqlStandbyCluster.
type MysqlStandbyClusterStatus struct {
	// ObservedGeneration is the .metadata.generation this status was
	// computed against.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Discovered is the result of the most recent complete bucket scan. A
	// scan that fails or is cut short leaves it unchanged; the conditions
	// say why.
	// +optional
	Discovered *StandbyDiscovered `json:"discovered,omitempty"`

	// Conditions are BucketReadable and SourceConfigKnown.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// StandbyDiscovered is the result of a complete bucket scan.
type StandbyDiscovered struct {
	// LastScanTime is when the most recent complete scan finished.
	// +optional
	LastScanTime *metav1.Time `json:"lastScanTime,omitempty"`

	// LatestDump is the newest dump under the prefix. Absent when the
	// prefix holds no dump (SourceConfigKnown=False/NoDumpFound).
	// +optional
	LatestDump *StandbyDump `json:"latestDump,omitempty"`

	// BinlogArchive is the source PITR archive's verified identity and the
	// window its manifests cover, published together. Absent when the
	// prefix holds no archive, or while the archive's descriptor is missing
	// or refused (SourceConfigKnown says why); LatestDump is still
	// published then.
	// +optional
	BinlogArchive *StandbyBinlogArchive `json:"binlogArchive,omitempty"`
}

// StandbyDump is a dump found under the source prefix.
type StandbyDump struct {
	// Name is the dump directory's basename (the source MysqlBackup name).
	// +required
	Name string `json:"name"`

	// Location is the dump directory's object key under the bucket, with
	// a trailing "/" (e.g. "orders/west/orders-nightly-20260520/").
	// +required
	Location string `json:"location"`

	// CompletionTime is the source-side completion time recorded in the
	// dump's @.json metadata. Absent when the metadata does not record one.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// GtidExecuted is the GTID set recorded in the dump's @.json metadata.
	// +optional
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// SizeBytes is the dump's size from its @.json metadata. Absent when the
	// metadata does not record one.
	// +optional
	SizeBytes *int64 `json:"sizeBytes,omitempty"`
}

// StandbyBinlogArchive is the verified source PITR archive under
// "<prefix>/binlogs/" and the window its per-site manifests cover. Sites
// whose manifest carries a history-integrity marker are counted in
// ManifestCount but contribute no files to the window.
type StandbyBinlogArchive struct {
	// Identity is the identity read from the archive's descriptor and
	// verified against every manifest.
	// +required
	Identity PITRArchiveIdentity `json:"identity"`

	// ManifestCount is the number of per-site manifests.
	// +required
	ManifestCount int32 `json:"manifestCount"`

	// FileCount is the number of archived binlog files in the window.
	// +required
	FileCount int32 `json:"fileCount"`

	// SizeBytes is the total size of the archived binlog files in the
	// window.
	// +required
	SizeBytes int64 `json:"sizeBytes"`

	// OldestEventTime is the earliest first-event time of an archived file.
	// +optional
	OldestEventTime *metav1.Time `json:"oldestEventTime,omitempty"`

	// NewestEventTime is the latest last-event time of an archived file.
	// Together with LatestDump it bounds the recoverable point-in-time
	// window.
	// +optional
	NewestEventTime *metav1.Time `json:"newestEventTime,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlStandbyCluster{}, &MysqlStandbyClusterList{})
}
