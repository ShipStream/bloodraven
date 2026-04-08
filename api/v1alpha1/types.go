package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.primaryDC`
// +kubebuilder:printcolumn:name="DC1",type=string,JSONPath=`.status.dc1.state`
// +kubebuilder:printcolumn:name="DC2",type=string,JSONPath=`.status.dc2.state`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MysqlReplicaPair is the Schema for the mysqlreplicapairs API.
type MysqlReplicaPair struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlReplicaPairSpec   `json:"spec,omitempty"`
	Status MysqlReplicaPairStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MysqlReplicaPairList contains a list of MysqlReplicaPair.
type MysqlReplicaPairList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlReplicaPair `json:"items"`
}

// MysqlReplicaPairSpec defines the desired state of MysqlReplicaPair.
type MysqlReplicaPairSpec struct {
	// Image is the MySQL container image. Default: mysql:9.6
	Image string `json:"image,omitempty"`

	// SidecarImage is the image used for the sidecar/init container.
	SidecarImage string `json:"sidecarImage,omitempty"`

	// DC1 is the first datacenter instance configuration.
	DC1 DCInstanceSpec `json:"dc1"`

	// DC2 is the second datacenter instance configuration.
	DC2 DCInstanceSpec `json:"dc2"`

	// SecretName references the secret containing MySQL credentials.
	SecretName string `json:"secretName"`

	// TLS configures TLS for the MySQL instances.
	TLS *TLSSpec `json:"tls,omitempty"`

	// AZ is the availability zone identifier.
	AZ string `json:"az"`

	// Cloudflare contains the Cloudflare DNS configuration for failover.
	Cloudflare CloudflareSpec `json:"cloudflare"`

	// PollInterval is how often to poll MySQL instances. Default: 2s
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// FailureThreshold is how many consecutive failures before marking unreachable. Default: 3
	FailureThreshold int32 `json:"failureThreshold,omitempty"`

	// RecoveryThreshold is how many consecutive successes before marking recovered. Default: 2
	RecoveryThreshold int32 `json:"recoveryThreshold,omitempty"`

	// FailoverCooldown is the minimum time between failovers. Default: 60m
	FailoverCooldown *metav1.Duration `json:"failoverCooldown,omitempty"`

	// Sidecar configures sidecar behavior.
	Sidecar SidecarSpec `json:"sidecar,omitempty"`

	// MysqlConf allows overriding default my.cnf settings.
	MysqlConf map[string]string `json:"mysqlConf,omitempty"`
}

// DCInstanceSpec defines the configuration for a single DC instance.
type DCInstanceSpec struct {
	// Name is the datacenter name (e.g. "dc1", "dc2").
	Name string `json:"name"`

	// Zone is the Kubernetes topology zone for node selection.
	Zone string `json:"zone"`

	// LBIP is the load balancer IP for DNS failover.
	LBIP string `json:"lbIP"`

	// Storage configures the persistent volume for this DC.
	Storage StorageSpec `json:"storage"`

	// Resources defines the compute resources for the MySQL container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// StorageSpec defines PVC storage configuration.
type StorageSpec struct {
	// StorageClassName is the name of the StorageClass.
	StorageClassName string `json:"storageClassName"`

	// Size is the requested storage size.
	Size resource.Quantity `json:"size"`
}

// TLSSpec configures TLS for MySQL.
type TLSSpec struct {
	// IssuerRef references a cert-manager Issuer or ClusterIssuer.
	IssuerRef IssuerRef `json:"issuerRef"`

	// SecretName is the name of the Secret containing the TLS certificates.
	// cert-manager should be configured to create this secret from the IssuerRef.
	SecretName string `json:"secretName"`
}

// IssuerRef is a reference to a cert-manager issuer.
type IssuerRef struct {
	// Name of the issuer.
	Name string `json:"name"`

	// Kind of the issuer (Issuer or ClusterIssuer).
	Kind string `json:"kind"`
}

// CloudflareSpec configures Cloudflare DNS for failover.
type CloudflareSpec struct {
	// APITokenSecretRef references the secret containing the Cloudflare API token.
	APITokenSecretRef SecretKeyRef `json:"apiTokenSecretRef"`

	// ZoneID is the Cloudflare zone ID.
	ZoneID string `json:"zoneID"`
}

// SecretKeyRef references a key within a Kubernetes Secret.
type SecretKeyRef struct {
	// Name of the Secret.
	Name string `json:"name"`

	// Key within the Secret.
	Key string `json:"key"`
}

// SidecarSpec configures the sidecar container behavior.
type SidecarSpec struct {
	// LeaseTimeout is how long before a sidecar lease expires. Default: 20s
	LeaseTimeout *metav1.Duration `json:"leaseTimeout,omitempty"`

	// PeerCheckInterval is how often the sidecar checks its peer. Default: 5s
	PeerCheckInterval *metav1.Duration `json:"peerCheckInterval,omitempty"`

	// BloodravenAddress is the address of the Bloodraven operator health endpoint.
	// Default: bloodraven.<namespace>.svc.cluster.local:8082
	BloodravenAddress string `json:"bloodravenAddress,omitempty"`
}

// MysqlReplicaPairStatus defines the observed state of MysqlReplicaPair.
type MysqlReplicaPairStatus struct {
	// PrimaryDC is the name of the DC currently acting as primary.
	PrimaryDC string `json:"primaryDC,omitempty"`

	// DC1 is the observed state of DC1.
	DC1 DCInstanceStatus `json:"dc1,omitempty"`

	// DC2 is the observed state of DC2.
	DC2 DCInstanceStatus `json:"dc2,omitempty"`

	// Conditions represent the latest available observations.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastFailover is the timestamp of the last failover event.
	LastFailover *metav1.Time `json:"lastFailover,omitempty"`

	// LastFailoverTarget is the DC that was last promoted during failover.
	LastFailoverTarget string `json:"lastFailoverTarget,omitempty"`

	// WebsocketClients is the number of connected websocket clients.
	WebsocketClients int32 `json:"websocketClients,omitempty"`
}

// DCInstanceStatus describes the observed state of a single DC instance.
type DCInstanceStatus struct {
	// State is the current state: writable, read-only, unreachable, or unknown.
	State string `json:"state,omitempty"`

	// LastSeen is the last time this DC was successfully polled.
	LastSeen *metav1.Time `json:"lastSeen,omitempty"`

	// GtidExecuted is the GTID executed set.
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// Replicating indicates whether replication is running.
	Replicating bool `json:"replicating,omitempty"`

	// SecondsBehindSource is the replication lag in seconds.
	SecondsBehindSource *int64 `json:"secondsBehindSource,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlReplicaPair{}, &MysqlReplicaPairList{})
}
