package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Active",type=string,JSONPath=`.status.activeSite`
// +kubebuilder:printcolumn:name="Site-A",type=string,JSONPath=`.status.sites[0].state`
// +kubebuilder:printcolumn:name="Site-B",type=string,JSONPath=`.status.sites[1].state`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MysqlFailoverGroup is the Schema for the mysqlfailovergroups API.
type MysqlFailoverGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlFailoverGroupSpec   `json:"spec,omitempty"`
	Status MysqlFailoverGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MysqlFailoverGroupList contains a list of MysqlFailoverGroup.
type MysqlFailoverGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlFailoverGroup `json:"items"`
}

// MysqlFailoverGroupSpec defines the desired state of MysqlFailoverGroup.
type MysqlFailoverGroupSpec struct {
	// Image is the MySQL container image. Default: mysql:9.6
	// +kubebuilder:default="mysql:9.6"
	Image string `json:"image,omitempty"`

	// SidecarImage is the image used for the sidecar/init container.
	// +kubebuilder:default="ghcr.io/shipstream/bloodraven-sidecar:0.1.6"
	SidecarImage string `json:"sidecarImage,omitempty"`

	// Sites defines the two sites that form this failover group.
	// Exactly two sites must be specified; either can be active.
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=2
	Sites []SiteSpec `json:"sites"`

	// SecretName references the secret containing MySQL credentials.
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`

	// TLS configures TLS for the MySQL instances.
	TLS *TLSSpec `json:"tls,omitempty"`

	// DNS configures the external DNS record managed via the external-dns DNSEndpoint CRD.
	DNS DNSSpec `json:"dns"`

	// PollInterval is how often to poll MySQL instances. Default: 2s
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// FailureThreshold is how many consecutive failures before marking unreachable. Default: 3
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	FailureThreshold int32 `json:"failureThreshold,omitempty"`

	// RecoveryThreshold is how many consecutive successes before marking recovered. Default: 2
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	RecoveryThreshold int32 `json:"recoveryThreshold,omitempty"`

	// FailoverCooldown is the minimum time between failovers. Default: 5m
	FailoverCooldown *metav1.Duration `json:"failoverCooldown,omitempty"`

	// Sidecar configures sidecar behavior.
	Sidecar SidecarSpec `json:"sidecar,omitempty"`

	// CloneTimeout is the timeout in seconds for CLONE INSTANCE operations. It
	// controls the session variables net_read_timeout and net_write_timeout
	// (set per-session before cloning) and the global variable clone_ddl_timeout
	// (GLOBAL-only; set via SET GLOBAL before cloning and also written to
	// my.cnf). Default: 3600 (1 hour).
	// +kubebuilder:default=3600
	// +kubebuilder:validation:Minimum=60
	CloneTimeout int `json:"cloneTimeout,omitempty"`

	// MysqlConf allows overriding default my.cnf settings.
	MysqlConf map[string]string `json:"mysqlConf,omitempty"`

	// Replication configures replication health monitoring.
	Replication *ReplicationSpec `json:"replication,omitempty"`

	// UpdateStrategy controls how spec changes are rolled out. Default: OrderedUpdate
	// +kubebuilder:default="OrderedUpdate"
	// +kubebuilder:validation:Enum=OrderedUpdate;Recreate
	UpdateStrategy string `json:"updateStrategy,omitempty"`

	// SidecarResources defines the compute resources for the sidecar container.
	SidecarResources corev1.ResourceRequirements `json:"sidecarResources,omitempty"`

	// TerminationGracePeriodSeconds is the grace period for MySQL container shutdown. Default: 30
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=1
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// PodLabels are additional labels applied to every MySQL pod managed by this failover group.
	// These are merged with the operator's required labels; operator labels take precedence on conflict.
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// PodAnnotations are additional annotations applied to every MySQL pod managed by this failover group.
	// These are merged with the operator's required annotations; operator annotations take precedence on conflict.
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// ServiceTemplate customizes the Services created by the operator (site, primary, and replicas).
	ServiceTemplate *ServiceTemplate `json:"serviceTemplate,omitempty"`

	// ExtraContainers are additional containers injected into every MySQL pod.
	// Useful for exporters, log shippers, or other sidecars.
	ExtraContainers []corev1.Container `json:"extraContainers,omitempty"`

	// ExtraInitContainers are additional init containers injected into every MySQL pod.
	// These run after the operator's built-in init container.
	ExtraInitContainers []corev1.Container `json:"extraInitContainers,omitempty"`
}

// ReplicationSpec configures replication health monitoring.
type ReplicationSpec struct {
	// MaxLagSeconds is the maximum acceptable replication lag before marking degraded. Default: 300
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	MaxLagSeconds int64 `json:"maxLagSeconds,omitempty"`
}

// SiteSpec defines the configuration for a single site in the failover group.
type SiteSpec struct {
	// Name is the site identifier (e.g. "iad", "pdx").
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Zone is the Kubernetes topology zone for node selection.
	// +kubebuilder:validation:MinLength=1
	Zone string `json:"zone"`

	// LBIP is the load balancer IP for DNS failover.
	// +kubebuilder:validation:MinLength=1
	LBIP string `json:"lbIP"`

	// Storage configures the persistent volume for this site.
	Storage StorageSpec `json:"storage"`

	// Resources defines the compute resources for the MySQL container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// StorageSpec defines PVC storage configuration.
type StorageSpec struct {
	// StorageClassName is the name of the StorageClass.
	// +kubebuilder:validation:MinLength=1
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
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`
}

// IssuerRef is a reference to a cert-manager issuer.
type IssuerRef struct {
	// Name of the issuer.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Kind of the issuer (Issuer or ClusterIssuer).
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	Kind string `json:"kind"`
}

// DNSSpec configures the external DNS record managed by the operator.
// Bloodraven creates a DNSEndpoint CR (externaldns.k8s.io/v1alpha1) that
// external-dns watches and syncs to the configured DNS provider.
type DNSSpec struct {
	// Hostname is the fully-qualified DNS name to update on failover
	// (e.g. "lion.az.example.com"). Apps should CNAME to this hostname.
	// +kubebuilder:validation:MinLength=1
	Hostname string `json:"hostname"`

	// TTL is the DNS record TTL in seconds. Default: 60
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	TTL int64 `json:"ttl,omitempty"`
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

// ServiceTemplate customizes the Services created by the operator.
type ServiceTemplate struct {
	// Type is the Kubernetes Service type. Default: ClusterIP
	// +kubebuilder:validation:Enum=ClusterIP;LoadBalancer;NodePort
	Type corev1.ServiceType `json:"type,omitempty"`

	// Annotations are additional annotations applied to every Service managed by this failover group.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// MysqlFailoverGroupStatus defines the observed state of MysqlFailoverGroup.
type MysqlFailoverGroupStatus struct {
	// ActiveSite is the name of the site currently acting as primary (writable).
	ActiveSite string `json:"activeSite,omitempty"`

	// Sites is the observed state of each site, parallel to spec.sites.
	Sites []SiteStatus `json:"sites,omitempty"`

	// Conditions represent the latest available observations.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastFailover is the timestamp of the last failover event.
	LastFailover *metav1.Time `json:"lastFailover,omitempty"`

	// LastFailoverTarget is the site that was last promoted during failover.
	LastFailoverTarget string `json:"lastFailoverTarget,omitempty"`

	// UpdatePhase indicates the current phase of an ordered update, empty if not updating.
	UpdatePhase string `json:"updatePhase,omitempty"`
}

// SiteStatus describes the observed state of a single site.
type SiteStatus struct {
	// Name is the site identifier, mirrored from spec for convenience.
	Name string `json:"name,omitempty"`

	// State is the current state: writable, read-only, unreachable, or unknown.
	// +kubebuilder:validation:Enum=writable;read-only;unreachable;unknown
	State string `json:"state,omitempty"`

	// LastSeen is the last time this site was successfully polled.
	LastSeen *metav1.Time `json:"lastSeen,omitempty"`

	// GtidExecuted is the GTID executed set (populated when replication status is enriched).
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// Replicating indicates whether replication is running (populated when replication status is enriched).
	Replicating bool `json:"replicating,omitempty"`

	// SecondsBehindSource is the replication lag in seconds (populated when replication status is enriched).
	SecondsBehindSource *int64 `json:"secondsBehindSource,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlFailoverGroup{}, &MysqlFailoverGroupList{})
}
