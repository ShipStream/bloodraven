package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Active",type=string,JSONPath=`.status.activeSite`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
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
// +kubebuilder:validation:XValidation:rule="(has(self.secretName) && self.secretName != \"\" && !has(self.credentials)) || ((!has(self.secretName) || self.secretName == \"\") && has(self.credentials))",message="exactly one of secretName or credentials must be set"
// +kubebuilder:validation:XValidation:rule="self.sites.all(x, self.sites.filter(y, y.name == x.name).size() == 1)",message="spec.sites[].name must be unique"
// +kubebuilder:validation:XValidation:rule="self.sites.filter(s, s.role == 'primary-candidate').size() >= 2",message="spec.sites must contain at least two sites with role 'primary-candidate'"
// +kubebuilder:validation:XValidation:rule="!has(self.splitBrainPolicy) || !has(self.splitBrainPolicy.sitePriorities) || self.splitBrainPolicy.sitePriorities.all(p, self.sites.exists(s, s.name == p && s.role == 'primary-candidate'))",message="splitBrainPolicy.sitePriorities entries must match the names of sites with role 'primary-candidate'"
type MysqlFailoverGroupSpec struct {
	// Image is the MySQL container image. Default: mysql:9.6
	// +kubebuilder:default="mysql:9.6"
	Image string `json:"image,omitempty"`

	// SidecarImage is the image used for the sidecar/init container.
	// +kubebuilder:default="ghcr.io/shipstream/bloodraven-sidecar:0.1.6"
	// +kubebuilder:validation:MinLength=1
	SidecarImage string `json:"sidecarImage,omitempty"`

	// Sites defines the sites that form this failover group. The slice must
	// contain at least two sites with role "primary-candidate"; additional
	// sites with role "dr-only" may be appended for cross-region DR.
	// Failover is performed by picking the best primary-candidate replica
	// when the active site is lost.
	//
	// MaxItems is set to 16 so the Kubernetes CEL cost estimator can bound
	// the uniqueness-and-priority validation rules. Real deployments are
	// expected to use well under that; raise the cap if needed.
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=16
	Sites []SiteSpec `json:"sites"`

	// SecretName references the secret containing MySQL credentials (legacy).
	// The secret must contain a 'dsn' key with a MySQL DSN string.
	// Mutually exclusive with credentials.
	// +optional
	SecretName string `json:"secretName,omitempty"`

	// Credentials configures per-role MySQL credential management.
	// When set, the operator manages MySQL users and privileges for each
	// configured role. Mutually exclusive with secretName.
	// +optional
	Credentials *CredentialsSpec `json:"credentials,omitempty"`

	// TLS configures TLS for the MySQL instances.
	TLS *TLSSpec `json:"tls,omitempty"`

	// DNS configures the external DNS record managed via the external-dns DNSEndpoint CRD.
	DNS DNSSpec `json:"dns"`

	// PollInterval is how often to poll MySQL instances. Default: 2s
	// +kubebuilder:default="2s"
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
	// +kubebuilder:default="5m"
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

	// TerminationGracePeriodSeconds is the grace period for MySQL container shutdown. Default: 60
	// +kubebuilder:default=60
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

	// Backup configures scheduled and on-demand backups for this failover
	// group. Omit the field to disable backups entirely.
	// +optional
	Backup *BackupSpec `json:"backup,omitempty"`

	// InitFromBackup, when set, gates normal bootstrap on a one-shot
	// restore of the given backup into the initial primary site. The
	// restore runs once; after success it is skipped on subsequent
	// reconciles even if this field is still populated.
	// +optional
	InitFromBackup *InitFromBackupSpec `json:"initFromBackup,omitempty"`

	// SplitBrainPolicy configures automated resolution when more than
	// one site is simultaneously writable and there is no prior
	// failover history the operator can use to pick a winner (for
	// example, after a fresh deploy or an operator restart that lost
	// in-memory state). When omitted, or when SitePriorities is empty,
	// the operator takes no automated action and alerts only (manual
	// resolution required).
	// +optional
	SplitBrainPolicy *SplitBrainPolicySpec `json:"splitBrainPolicy,omitempty"`

	// RestoreInPlace, when set, runs a re-triggerable destructive
	// restore against the currently-active primary. Unlike
	// InitFromBackup (one-shot, greenfield), this field is meant to be
	// edited repeatedly: bumping spec.restoreInPlace.confirm to a newer
	// RFC 3339 timestamp triggers another restore. See RestoreInPlaceSpec
	// for the full-instance vs per-schema semantics and the fencing
	// choreography.
	// +optional
	RestoreInPlace *RestoreInPlaceSpec `json:"restoreInPlace,omitempty"`

	// PlannedFailover configures cluster-wide defaults for the graceful
	// planned-failover API. The admin triggers a planned failover by
	// annotating the CR with bloodraven.shipstream.io/planned-failover=<site>;
	// this block lets the knobs live on the CR rather than being spelled
	// into every kubectl annotate.
	// +optional
	PlannedFailover *PlannedFailoverSpec `json:"plannedFailover,omitempty"`

	// Dragonfly configures an optional per-site Dragonfly cluster
	// co-managed with MySQL. When omitted or disabled, no Dragonfly
	// resources are created.
	// +optional
	Dragonfly *DragonflySpec `json:"dragonfly,omitempty"`

	// PodSecurityContext optionally sets the pod-level security context for
	// the MySQL Deployment. When nil (default), no security context is
	// set on the pod; this preserves backward compatibility with existing
	// clusters whose PVCs were created without FSGroup. Setting this field
	// will apply the value as-is to the pod; the operator does not merge it
	// with hardened defaults. To enable Restricted PSS, set the standard
	// fields (RunAsNonRoot, RunAsUser, RunAsGroup, FSGroup, SeccompProfile).
	// See docs/docs/production-hardening.mdx for the upgrade procedure.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// ContainerSecurityContext optionally sets the container-level security
	// context for the MySQL Deployment's `mysql` and `sidecar` containers.
	// Same backward-compatibility semantics as PodSecurityContext.
	// +optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`
}

// SplitBrainPolicySpec configures automated split-brain resolution.
//
// SitePriorities declares an ordered list of authoritative sites that
// win unresolvable ties. When more than one site is writable and the
// operator cannot infer a winner from its own failover history, the
// operator walks the list in order and promotes the first entry that
// is currently writable; every other writable site is fenced.
//
// Entries must reference sites with role "primary-candidate" — a
// "dr-only" site is never auto-promoted, and naming one here is
// rejected by validation.
//
// This is a policy decision, not a safety feature: any writes accepted
// on losing sites that did not replicate to the winner will be isolated
// when those sites are fenced. The existing divergent-GTID detection
// will block auto-rejoin of any losing site whose GTID set contains
// transactions the winner never saw; those transactions are only
// recoverable via re-clone.
type SplitBrainPolicySpec struct {
	// SitePriorities is the ordered list of primary-candidate site
	// names that win unresolvable split-brain ties. The first entry
	// that is currently writable becomes the new primary; every other
	// writable site is fenced. If empty, the operator falls back to
	// manual resolution (alert only).
	//
	// MaxItems must stay equal to spec.sites' MaxItems so the CEL cost
	// estimator can bound the "every priority entry names a real
	// primary-candidate" rule.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=253
	SitePriorities []string `json:"sitePriorities,omitempty"`
}

// ReplicationSpec configures replication health monitoring.
type ReplicationSpec struct {
	// MaxLagSeconds is the maximum acceptable replication lag before marking degraded. Default: 300
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	MaxLagSeconds int64 `json:"maxLagSeconds,omitempty"`
}

// SiteRole describes the promotion eligibility of a site.
//
// "primary-candidate" sites are full participants: they can be promoted
// as the active primary during failover, and they count against the
// minimum-two-candidate quorum that the CRD requires.
//
// "dr-only" sites are passive replicas. They follow the active primary
// (typical use: cross-region DR) and are never considered as promotion
// targets — the operator will not auto-promote them on failover, and
// the split-brain priority picker treats them as ineligible winners.
//
// A writable "dr-only" site is an anomaly (the role implies read-only
// replication), and when the operator detects that anomaly during
// split-brain resolution it fences the site along with the other
// losers so divergent writes cannot continue. The role limits
// *promotion* and *policy eligibility*; it does not exempt the site
// from the same safety fencing that protects primary-candidate
// losers.
// +kubebuilder:validation:Enum=primary-candidate;dr-only
type SiteRole string

const (
	// SiteRolePrimaryCandidate is the default role: a site that
	// participates in failover elections.
	SiteRolePrimaryCandidate SiteRole = "primary-candidate"

	// SiteRoleDROnly is a passive replica role: a site that follows
	// the active primary but is never promoted automatically.
	SiteRoleDROnly SiteRole = "dr-only"
)

// SiteSpec defines the configuration for a single site in the failover group.
type SiteSpec struct {
	// Name is the site identifier (e.g. "iad", "pdx", "lhr").
	// Must be unique within spec.sites. MaxLength caps the CEL cost
	// of the spec-level uniqueness and priority-membership rules.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Role governs whether this site can be auto-promoted on failover.
	// Default: primary-candidate.
	// +kubebuilder:default="primary-candidate"
	// +optional
	Role SiteRole `json:"role,omitempty"`

	// Zone is the Kubernetes topology zone for node selection.
	// +kubebuilder:validation:MinLength=1
	Zone string `json:"zone"`

	// TaintNodeSelector identifies the Kubernetes nodes that receive this
	// failover group's db-readonly taint when this site is not writable. Use
	// group-scoped labels so one physical node can participate in multiple
	// failover groups at the same site.
	// +kubebuilder:validation:MinProperties=1
	TaintNodeSelector map[string]string `json:"taintNodeSelector"`

	// LBIP is the load balancer IP for DNS failover.
	// +kubebuilder:validation:MinLength=1
	LBIP string `json:"lbIP"`

	// Storage configures the persistent volume for this site.
	Storage StorageSpec `json:"storage"`

	// Resources defines the compute resources for the MySQL container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// EffectiveRole returns the site's role, defaulting to
// SiteRolePrimaryCandidate when unset. Callers should prefer this over
// reading Role directly so that the default is applied consistently even
// for objects that bypassed CRD validation (e.g., in-memory test fixtures).
func (s SiteSpec) EffectiveRole() SiteRole {
	if s.Role == "" {
		return SiteRolePrimaryCandidate
	}
	return s.Role
}

// IsPromotable reports whether this site can be auto-promoted to primary
// during failover.
func (s SiteSpec) IsPromotable() bool {
	return s.EffectiveRole() == SiteRolePrimaryCandidate
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
	// +kubebuilder:default="20s"
	LeaseTimeout *metav1.Duration `json:"leaseTimeout,omitempty"`

	// PeerCheckInterval is how often the sidecar checks its peer. Default: 5s
	// +kubebuilder:default="5s"
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

// CredentialsSpec configures per-role MySQL credential management.
// Each field references a Secret with 'username' and 'password' keys.
// The operator creates MySQL users with role-appropriate privileges
// and rotates passwords when the referenced Secrets change.
type CredentialsSpec struct {
	// OperatorSecret references the Secret for operator and sidecar connections.
	// Required keys: username, password, MYSQL_ROOT_PASSWORD.
	// Optional keys: MYSQL_REPLICATION_USER, MYSQL_REPLICATION_PASSWORD
	// (default to operator username/password if omitted).
	// Grants: ALL PRIVILEGES WITH GRANT OPTION.
	// +kubebuilder:validation:MinLength=1
	OperatorSecret string `json:"operatorSecret"`

	// AppSecret references the Secret for application read-write connections.
	// Required keys: username, password.
	// Grants: ALL PRIVILEGES (no GRANT OPTION, no SUPER).
	// +optional
	AppSecret string `json:"appSecret,omitempty"`

	// ReadOnlySecret references the Secret for application read-only connections.
	// Required keys: username, password.
	// Grants: SELECT, SHOW VIEW, SHOW DATABASES, PROCESS.
	// +optional
	ReadOnlySecret string `json:"readOnlySecret,omitempty"`

	// MonitorSecret references the Secret for Prometheus exporter connections.
	// Required keys: username, password.
	// Grants: PROCESS, REPLICATION CLIENT, SELECT on performance_schema.
	// +optional
	MonitorSecret string `json:"monitorSecret,omitempty"`

	// BackupSecret references the Secret for backup and restore connections.
	// Required keys: username, password.
	// Grants: SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER, RELOAD,
	// BACKUP_ADMIN, REPLICATION CLIENT.
	// +optional
	BackupSecret string `json:"backupSecret,omitempty"`
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

	// PromotionGtidExecuted is the GTID executed set recorded on the
	// candidate at the moment of the most recent promotion, before it
	// began accepting writes. Used for data-loss accounting.
	// +optional
	PromotionGtidExecuted string `json:"promotionGtidExecuted,omitempty"`

	// UpdatePhase indicates the current phase of an ordered update, empty if not updating.
	UpdatePhase string `json:"updatePhase,omitempty"`

	// Restore tracks an in-flight or completed initFromBackup operation.
	// +optional
	Restore *RestoreStatus `json:"restore,omitempty"`

	// RestoreInPlace tracks an in-flight or completed in-place restore
	// (spec.restoreInPlace). The consumed confirm token is stamped here
	// on success so subsequent reconciles can distinguish "restore
	// already done" from "new restore requested".
	// +optional
	RestoreInPlace *RestoreInPlaceStatus `json:"restoreInPlace,omitempty"`

	// BackupSchedules is a per-schedule rollup of the most recent backup
	// activity for each entry in spec.backup.schedules.
	// +optional
	BackupSchedules []BackupScheduleStatus `json:"backupSchedules,omitempty"`

	// LastBackupTime is the completion time of the most recent successful
	// MysqlBackup across all profiles, regardless of whether it was
	// scheduled or on-demand.
	// +optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// PITR is the API surface for surfacing a summary of the
	// continuous binary-log archive (oldest/newest archived events,
	// count, bytes). Reserved: the v1alpha1 controller does not yet
	// populate this field. Live archiver health is exposed via the
	// sidecar's /archiver/status endpoint in the meantime.
	// +optional
	PITR *PITRStatus `json:"pitr,omitempty"`

	// PlannedFailover tracks the most recent planned (admin-triggered)
	// failover attempt. Terminal status is retained until a newer
	// annotation replaces the block; the field is how kubectl describe
	// tells the story of a switchover after the fact.
	// +optional
	PlannedFailover *PlannedFailoverStatus `json:"plannedFailover,omitempty"`

	// Dragonfly is the observed state of the Dragonfly subsystem when
	// spec.dragonfly is configured.
	// +optional
	Dragonfly *DragonflyStatus `json:"dragonfly,omitempty"`
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

	// RecoveryState tracks old-primary recovery progress after failover.
	// Empty when no recovery is needed. RecoveryInProgress means the site is
	// being reconfigured, or is stabilizing, as a replica of the current primary.
	// RecoveryBlocked means divergent transactions were detected and the site
	// must be re-cloned.
	// +kubebuilder:validation:Enum="";RecoveryInProgress;RecoveryBlocked
	// +optional
	RecoveryState string `json:"recoveryState,omitempty"`

	// DivergentGtid is the GTID set of transactions on this site that
	// diverge from the current primary. Populated when RecoveryState is
	// RecoveryBlocked.
	// +optional
	DivergentGtid string `json:"divergentGtid,omitempty"`

	// DivergentTransactionCount is the number of divergent transactions.
	// +optional
	DivergentTransactionCount *int64 `json:"divergentTransactionCount,omitempty"`
}

func init() {
	SchemeBuilder.Register(&MysqlFailoverGroup{}, &MysqlFailoverGroupList{})
}
