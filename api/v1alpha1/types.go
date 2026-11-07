package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mfg;mfgs,categories=bloodraven;mysql
// +kubebuilder:printcolumn:name="Active",type=string,JSONPath=`.status.activeSite`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec.backup) || !has(self.spec.backup) || !has(oldSelf.spec.backup.pitr) || !has(self.spec.backup.pitr) || !(has(oldSelf.spec.backup.pitr.enabled) && oldSelf.spec.backup.pitr.enabled) || !(has(self.spec.backup.pitr.enabled) && self.spec.backup.pitr.enabled) || !has(oldSelf.spec.backup.pitr.profileName) || !has(self.spec.backup.pitr.profileName) || oldSelf.spec.backup.pitr.profileName != self.spec.backup.pitr.profileName || !has(oldSelf.spec.backup.profiles) || !has(self.spec.backup.profiles) || self.spec.backup.profiles.all(p, p.name != self.spec.backup.pitr.profileName || oldSelf.spec.backup.profiles.all(o, o.name != p.name || !(o.storage.type == p.storage.type && has(o.storage.s3) == has(p.storage.s3) && (!has(o.storage.s3) || (o.storage.s3.bucket == p.storage.s3.bucket && (has(o.storage.s3.prefix) ? o.storage.s3.prefix : '') == (has(p.storage.s3.prefix) ? p.storage.s3.prefix : '') && (has(o.storage.s3.endpointURL) ? o.storage.s3.endpointURL : '') == (has(p.storage.s3.endpointURL) ? p.storage.s3.endpointURL : ''))) && has(o.storage.pvc) == has(p.storage.pvc) && (!has(o.storage.pvc) || ((has(o.storage.pvc.claimName) ? o.storage.pvc.claimName : '') == (has(p.storage.pvc.claimName) ? p.storage.pvc.claimName : '') && (has(o.storage.pvc.subPath) ? o.storage.pvc.subPath : '') == (has(p.storage.pvc.subPath) ? p.storage.pvc.subPath : '')))) || (has(o.encryption) == has(p.encryption) && (!has(o.encryption) || ((has(o.encryption.algorithm) ? o.encryption.algorithm : 'AES-256-GCM') == (has(p.encryption.algorithm) ? p.encryption.algorithm : 'AES-256-GCM') && o.encryption.passphraseSecretKeyRef.name == p.encryption.passphraseSecretKeyRef.name && (has(o.encryption.passphraseSecretKeyRef.key) ? o.encryption.passphraseSecretKeyRef.key : 'passphrase') == (has(p.encryption.passphraseSecretKeyRef.key) ? p.encryption.passphraseSecretKeyRef.key : 'passphrase'))))))",message="spec.backup.profiles[pitr.profileName].encryption (presence, algorithm, passphraseSecretKeyRef) cannot change while that profile archives PITR binlogs to the same storage; create a new profile with a new storage prefix and its own passphrase, switch spec.backup.pitr.profileName to it, wait until its archive binding is Bound, then take a full backup (see Backup encryption > Rotation)"

// MysqlFailoverGroup is the Schema for the mysqlfailovergroups API.
type MysqlFailoverGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec MysqlFailoverGroupSpec `json:"spec"`
	// +optional
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
// +kubebuilder:validation:XValidation:rule="self.sites.all(x, self.sites.filter(y, y.serverID == x.serverID).size() == 1)",message="spec.sites[].serverID must be unique within the group"
// +kubebuilder:validation:XValidation:rule="self.sites.filter(s, s.role == 'PrimaryCandidate').size() >= 2",message="spec.sites must contain at least two sites with role PrimaryCandidate"
// +kubebuilder:validation:XValidation:rule="!has(self.splitBrainPolicy) || !has(self.splitBrainPolicy.sitePriorities) || self.splitBrainPolicy.sitePriorities.all(p, self.sites.exists(s, s.name == p && s.role == 'PrimaryCandidate'))",message="splitBrainPolicy.sitePriorities entries must match the names of sites with role PrimaryCandidate"
// +kubebuilder:validation:XValidation:rule="!has(self.serviceTemplate) || !has(self.serviceTemplate.externalTrafficPolicy) || (has(self.serviceTemplate.type) && self.serviceTemplate.type in ['NodePort', 'LoadBalancer'])",message="serviceTemplate.externalTrafficPolicy requires serviceTemplate.type NodePort or LoadBalancer"
// +kubebuilder:validation:XValidation:rule="self.sites.all(s, !has(s.serviceTemplate) || !has(s.serviceTemplate.externalTrafficPolicy) || ((has(s.serviceTemplate.type) ? s.serviceTemplate.type : (has(self.serviceTemplate) && has(self.serviceTemplate.type) ? self.serviceTemplate.type : 'ClusterIP')) in ['NodePort', 'LoadBalancer']))",message="site serviceTemplate.externalTrafficPolicy requires an effective type of NodePort or LoadBalancer"
// +kubebuilder:validation:XValidation:rule="self.sites.all(s, !has(s.serviceTemplate) || !has(s.serviceTemplate.nodePort) || ((has(s.serviceTemplate.type) ? s.serviceTemplate.type : (has(self.serviceTemplate) && has(self.serviceTemplate.type) ? self.serviceTemplate.type : 'ClusterIP')) in ['NodePort', 'LoadBalancer']))",message="site serviceTemplate.nodePort requires an effective type of NodePort or LoadBalancer"
// +kubebuilder:validation:XValidation:rule="self.sites.all(s, !has(self.serviceTemplate) || !has(self.serviceTemplate.externalTrafficPolicy) || ((has(s.serviceTemplate) && has(s.serviceTemplate.type) ? s.serviceTemplate.type : (has(self.serviceTemplate.type) ? self.serviceTemplate.type : 'ClusterIP')) in ['NodePort', 'LoadBalancer']))",message="inherited externalTrafficPolicy requires each effective site service type to be NodePort or LoadBalancer"
// +kubebuilder:validation:XValidation:rule="!has(self.encryptionAtRest) || !self.encryptionAtRest.enabled || has(self.tls)",message="spec.encryptionAtRest.enabled requires spec.tls: MySQL requires a secure connection to clone encrypted data"
// +kubebuilder:validation:XValidation:rule="!has(self.encryptionAtRest) || !self.encryptionAtRest.enabled || !has(self.encryptionAtRest.keyring) || !has(self.encryptionAtRest.keyring.dataFileDir) || (self.encryptionAtRest.keyring.dataFileDir != '/var/lib/mysql' && !self.encryptionAtRest.keyring.dataFileDir.startsWith('/var/lib/mysql/'))",message="spec.encryptionAtRest.keyring.dataFileDir must not be inside the MySQL data directory"
type MysqlFailoverGroupSpec struct {
	// Image is the MySQL container image. Default: mysql:9.7
	// +kubebuilder:default="mysql:9.7"
	// +optional
	Image string `json:"image,omitempty"`

	// SidecarImage is the image used for the sidecar/init container.
	// +kubebuilder:default="ghcr.io/shipstream/bloodraven-sidecar:1.3.2"
	// +kubebuilder:validation:MinLength=1
	// +optional
	SidecarImage string `json:"sidecarImage,omitempty"`

	// Sites defines the sites that form this failover group. The slice must
	// contain at least two sites with role PrimaryCandidate; additional
	// sites with role DisasterRecovery may be appended for cross-region DR.
	// Failover is performed by picking the best PrimaryCandidate replica
	// when the active site is lost.
	//
	// The list is a map keyed by name, so names are unique and server-side
	// apply merges per site. MaxItems is set to 16 so the Kubernetes CEL
	// cost estimator can bound the serverID-uniqueness and priority
	// validation rules. Real deployments are expected to use well under
	// that.
	// +required
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=16
	Sites []SiteSpec `json:"sites"`

	// Credentials configures per-role MySQL credential management. The
	// operator manages MySQL users and privileges for each configured role.
	// +required
	Credentials CredentialsSpec `json:"credentials"`

	// TLS configures TLS for the MySQL instances.
	// +optional
	TLS *TLSSpec `json:"tls,omitempty"`

	// DNS configures the external DNS record managed via the external-dns DNSEndpoint CRD.
	// +required
	DNS DNSSpec `json:"dns"`

	// PollInterval is the interval for polling MySQL instances. After a site
	// reaches FailureThreshold and continues failing, that site's probes back
	// off exponentially to a 30s cap; other sites keep this interval.
	// Default: 2s. Bounds: 1s to 5m.
	// +kubebuilder:default="2s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('5m')",message="pollInterval must be between 1s and 5m"
	// +optional
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// FailureThreshold is how many consecutive failures before marking unreachable. Default: 3
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	FailureThreshold int32 `json:"failureThreshold,omitempty"`

	// RecoveryThreshold is how many consecutive successes before marking recovered. Default: 2
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	RecoveryThreshold int32 `json:"recoveryThreshold,omitempty"`

	// FailoverCooldown is the minimum time between failovers, the
	// anti-flap guard; it cannot be disabled. Default: 5m. Bounds: 30s to
	// 24h.
	// +kubebuilder:default="5m0s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('30s') && duration(self) <= duration('24h')",message="failoverCooldown must be between 30s and 24h"
	// +optional
	FailoverCooldown *metav1.Duration `json:"failoverCooldown,omitempty"`

	// ConnectionDrainTimeout bounds the operator-side retry window for
	// evicting application sessions from a fenced former primary after an
	// emergency failover or autonomous self-fence. This drain runs only
	// after promotion has completed, so it cannot kill the operator's
	// promotion session. Default: 30s. Bounds: 1s to 10m.
	// +kubebuilder:default="30s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('10m')",message="connectionDrainTimeout must be between 1s and 10m"
	// +optional
	ConnectionDrainTimeout *metav1.Duration `json:"connectionDrainTimeout,omitempty"`

	// Sidecar configures sidecar behavior.
	// +optional
	Sidecar SidecarSpec `json:"sidecar,omitempty"`

	// CloneTimeout bounds CLONE INSTANCE operations. It sets the session
	// variables net_read_timeout and net_write_timeout (per session before
	// cloning) and the global variable clone_ddl_timeout (GLOBAL-only; set
	// via SET GLOBAL before cloning), all in whole seconds, rounded up.
	// Default: 1h. Bounds: 1m to 24h.
	// +kubebuilder:default="1h0m0s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1m') && duration(self) <= duration('24h')",message="cloneTimeout must be between 1m and 24h"
	// +optional
	CloneTimeout *metav1.Duration `json:"cloneTimeout,omitempty"`

	// MysqlConf allows overriding default my.cnf settings.
	// +optional
	MysqlConf map[string]string `json:"mysqlConf,omitempty"`

	// Replication configures replication health monitoring.
	// +optional
	Replication *ReplicationSpec `json:"replication,omitempty"`

	// UpdateStrategy controls how spec changes are rolled out. Default:
	// {type: OrderedUpdate}.
	// +kubebuilder:default={}
	// +optional
	UpdateStrategy UpdateStrategy `json:"updateStrategy,omitempty"`

	// SidecarResources defines the compute resources for the sidecar container.
	// +optional
	SidecarResources corev1.ResourceRequirements `json:"sidecarResources,omitempty"`

	// TerminationGracePeriodSeconds is the grace period for MySQL container shutdown. Default: 60
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// PodLabels are additional labels applied to every MySQL pod managed by this failover group.
	// These are merged with the operator's required labels; operator labels take precedence on conflict.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// PodAnnotations are additional annotations applied to every MySQL pod managed by this failover group.
	// These are merged with the operator's required annotations; operator annotations take precedence on conflict.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// ServiceTemplate customizes the Services created by the operator (site, primary, and replicas).
	// +optional
	ServiceTemplate *ServiceTemplate `json:"serviceTemplate,omitempty"`

	// ExtraContainers are additional containers injected into every MySQL pod.
	// Useful for exporters, log shippers, or other sidecars. Keyed by name.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	ExtraContainers []corev1.Container `json:"extraContainers,omitempty"`

	// ExtraInitContainers are additional init containers injected into every MySQL pod.
	// These run after the operator's built-in init container. Keyed by name.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
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
	// See https://bloodraven.dev/docs/configuration/production-hardening for the upgrade procedure.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// ContainerSecurityContext optionally sets the container-level security
	// context for the MySQL Deployment's `mysql` and `sidecar` containers.
	// Same backward-compatibility semantics as PodSecurityContext.
	// +optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`

	// EncryptionAtRest configures MySQL InnoDB data-at-rest encryption
	// using the GPL component_keyring_file keyring component, with the
	// live keyring projected from a per-site Kubernetes Secret so no key
	// material lands on the MySQL data PVC or a worker-node disk.
	//
	// Requires spec.tls: MySQL mandates a secure connection when cloning
	// encrypted data, and Bloodraven bootstraps replicas with CLONE
	// INSTANCE.
	// +optional
	EncryptionAtRest *EncryptionAtRestSpec `json:"encryptionAtRest,omitempty"`

	// License is an optional Ed25519-signed JWT recording this group's
	// Bloodraven license. It is a public assertion, not a credential —
	// leaking it does not grant access to anything. The operator verifies
	// it offline (no network) and exposes organization, edition, and
	// update-period end as logs and metrics. It never gates functionality
	// or changes failover behavior. When empty, the operator-level license
	// is used if set; otherwise the group is observed as Community.
	// +optional
	License string `json:"license,omitempty"`
}

// SplitBrainPolicySpec configures automated split-brain resolution.
//
// SitePriorities declares an ordered list of authoritative sites that
// win unresolvable ties. When more than one site is writable and the
// operator cannot infer a winner from its own failover history, the
// operator walks the list in order and promotes the first entry that
// is currently writable; every other writable site is fenced.
//
// Entries must reference sites with role PrimaryCandidate: a
// DisasterRecovery site is never auto-promoted, and naming one here is
// rejected by validation.
//
// This is a policy decision, not a safety feature: any writes accepted
// on losing sites that did not replicate to the winner will be isolated
// when those sites are fenced. The existing divergent-GTID detection
// will block auto-rejoin of any losing site whose GTID set contains
// transactions the winner never saw; those transactions are only
// recoverable via re-clone.
type SplitBrainPolicySpec struct {
	// SitePriorities is the ordered list of PrimaryCandidate site
	// names that win unresolvable split-brain ties. The first entry
	// that is currently writable becomes the new primary; every other
	// writable site is fenced. If empty, the operator falls back to
	// manual resolution (alert only).
	//
	// MaxItems must stay equal to spec.sites' MaxItems so the CEL cost
	// estimator can bound the "every priority entry names a real
	// PrimaryCandidate" rule.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.filter(y, y == x).size() == 1)",message="splitBrainPolicy.sitePriorities entries must be unique"
	SitePriorities []string `json:"sitePriorities,omitempty"`
}

// ReplicationSpec configures replication health monitoring.
type ReplicationSpec struct {
	// MaxLagSeconds is the maximum acceptable replication lag, in seconds,
	// before marking degraded. 0 requires the replica to be fully caught
	// up. Default: 300.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxLagSeconds *int64 `json:"maxLagSeconds,omitempty"`

	// ReadOnlyMaxLagSeconds is the maximum lag for a read-only site's
	// client-facing endpoint. Nil inherits MaxLagSeconds; explicit zero
	// requires the reader to be fully caught up.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ReadOnlyMaxLagSeconds *int64 `json:"readOnlyMaxLagSeconds,omitempty"`
}

// SiteRole describes the promotion eligibility of a site.
//
// PrimaryCandidate sites are full participants: they can be promoted
// as the active primary during failover, and they count against the
// minimum-two-candidate quorum that the CRD requires.
//
// DisasterRecovery sites are passive replicas. They follow the active
// primary (typical use: cross-region DR) and are never considered as
// promotion targets: the operator will not auto-promote them on
// failover, and the split-brain priority picker treats them as
// ineligible winners.
//
// A writable DisasterRecovery site is an anomaly (the role implies
// read-only replication), and when the operator detects that anomaly
// during split-brain resolution it fences the site along with the other
// losers so divergent writes cannot continue. The role limits
// *promotion* and *policy eligibility*; it does not exempt the site
// from the same safety fencing that protects PrimaryCandidate losers.
//
// ReadOnly sites are serving replicas that are never promoted and do not
// participate in core failover-group readiness.
// +kubebuilder:validation:Enum=PrimaryCandidate;DisasterRecovery;ReadOnly
type SiteRole string

const (
	// SiteRolePrimaryCandidate is the default role: a site that
	// participates in failover elections.
	SiteRolePrimaryCandidate SiteRole = "PrimaryCandidate"

	// SiteRoleDisasterRecovery is a passive replica role: a site that
	// follows the active primary but is never promoted automatically.
	SiteRoleDisasterRecovery SiteRole = "DisasterRecovery"

	// SiteRoleReadOnly is a serving replica that is never promoted and
	// does not participate in core failover-group readiness.
	SiteRoleReadOnly SiteRole = "ReadOnly"
)

// UpdateStrategyType is how spec changes are rolled out to the sites.
// +kubebuilder:validation:Enum=OrderedUpdate;Recreate
type UpdateStrategyType string

const (
	// UpdateStrategyOrderedUpdate rolls one site at a time: the followers
	// first, then the active site after a handoff to an updated standby.
	UpdateStrategyOrderedUpdate UpdateStrategyType = "OrderedUpdate"
	// UpdateStrategyRecreate patches every site's Deployment at once.
	UpdateStrategyRecreate UpdateStrategyType = "Recreate"
)

// UpdateStrategy controls how spec changes are rolled out.
type UpdateStrategy struct {
	// Type is the rollout strategy. Default: OrderedUpdate.
	// +kubebuilder:default=OrderedUpdate
	// +optional
	Type UpdateStrategyType `json:"type,omitempty"`
}

// EffectiveType returns the strategy type, defaulting to OrderedUpdate
// when unset (an object that bypassed CRD defaulting).
func (u UpdateStrategy) EffectiveType() UpdateStrategyType {
	if u.Type == "" {
		return UpdateStrategyOrderedUpdate
	}
	return u.Type
}

// SiteSpec defines the configuration for a single site in the failover group.
// +kubebuilder:validation:XValidation:rule="self.role == 'ReadOnly' || (has(self.taintNodeSelector) && has(self.lbIP))",message="taintNodeSelector and lbIP are required unless role is ReadOnly"
type SiteSpec struct {
	// Name is the site identifier (e.g. "iad", "pdx", "lhr"), the key of
	// spec.sites. MaxLength caps the CEL cost of the spec-level
	// priority-membership rule. The pattern matches what the per-site
	// Service names already require, and keeps the name safe to use in
	// object-storage keys.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9-]+$`
	Name string `json:"name"`

	// ServerID is the MySQL server_id of this site's server. It must be
	// unique within the group, and it cannot change: it identifies the
	// server in the binary logs, the PITR archive and backup stamps, so it
	// does not depend on the order of spec.sites. Use 101, 102, ... in
	// declaration order unless another scheme is needed.
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4294967295
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="serverID is immutable"
	ServerID int64 `json:"serverID"`

	// Role governs whether this site can be auto-promoted on failover.
	// Default: PrimaryCandidate.
	// +kubebuilder:default=PrimaryCandidate
	// +optional
	Role SiteRole `json:"role,omitempty"`

	// Zone is the Kubernetes topology zone for node selection.
	// +kubebuilder:validation:MinLength=1
	// +required
	Zone string `json:"zone"`

	// TaintNodeSelector identifies the Kubernetes nodes that receive this
	// failover group's db-readonly taint when this site is not writable. Use
	// group-scoped labels so one physical node can participate in multiple
	// failover groups at the same site.
	// +kubebuilder:validation:MinProperties=1
	// +optional
	TaintNodeSelector map[string]string `json:"taintNodeSelector,omitempty"`

	// LBIP is the load balancer IP for DNS failover.
	// +kubebuilder:validation:MinLength=1
	// +optional
	LBIP string `json:"lbIP,omitempty"`

	// Storage configures the persistent volume for this site.
	// +required
	Storage StorageSpec `json:"storage"`

	// Resources defines the compute resources for the MySQL container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// MysqlConf overrides group-level MySQL settings for this site.
	// +optional
	MysqlConf map[string]string `json:"mysqlConf,omitempty"`

	// ServiceTemplate overrides the group Service template for this site's
	// client-facing MySQL Service.
	// +optional
	ServiceTemplate *SiteServiceTemplate `json:"serviceTemplate,omitempty"`
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
	// +required
	StorageClassName string `json:"storageClassName"`

	// Size is the requested storage size.
	// +required
	Size resource.Quantity `json:"size"`
}

// TLSSpec configures TLS for MySQL.
type TLSSpec struct {
	// SecretName is the name of the Secret holding the server certificate
	// (tls.crt), its private key (tls.key) and the CA bundle (ca.crt). The
	// operator does not issue certificates; create the Secret yourself or
	// with a tool such as cert-manager.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	SecretName string `json:"secretName"`
}

// DNSSpec configures the external DNS record managed by the operator.
// Bloodraven creates a DNSEndpoint CR (externaldns.k8s.io/v1alpha1) that
// external-dns watches and syncs to the configured DNS provider.
type DNSSpec struct {
	// Hostname is the fully-qualified DNS name to update on failover
	// (e.g. "lion.az.example.com"). Apps should CNAME to this hostname.
	// +kubebuilder:validation:MinLength=1
	// +required
	Hostname string `json:"hostname"`

	// TTLSeconds is the DNS record TTL in seconds. Default: 60
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	// +optional
	TTLSeconds int64 `json:"ttlSeconds,omitempty"`
}

// SidecarSpec configures the sidecar container behavior.
// The XValidation messages below deliberately use the context-neutral
// "sidecar.*" field prefix rather than "spec.sidecar.*": SidecarSpec is
// embedded at spec.sidecar in MysqlFailoverGroup but at
// spec.template.spec.sidecar in MysqlStandbyCluster, and a struct-level
// marker emits the same message in both, so a fully-qualified path would
// be wrong in one of them.
// +kubebuilder:validation:XValidation:rule="duration(has(self.leaseTimeout) ? self.leaseTimeout : '20s') >= duration(has(self.peerCheckInterval) ? self.peerCheckInterval : '5s') + duration(has(self.peerCheckInterval) ? self.peerCheckInterval : '5s') + duration(has(self.peerCheckInterval) ? self.peerCheckInterval : '5s')",message="sidecar.leaseTimeout must be at least 3x sidecar.peerCheckInterval"
type SidecarSpec struct {
	// LeaseTimeout is how long before a sidecar lease expires. Default: 20s.
	// Bounds: 3s to 10m, and at least 3x PeerCheckInterval.
	// +kubebuilder:default="20s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('3s') && duration(self) <= duration('10m')",message="sidecar.leaseTimeout must be between 3s and 10m"
	// +optional
	LeaseTimeout *metav1.Duration `json:"leaseTimeout,omitempty"`

	// PeerCheckInterval is how often the sidecar checks its peer. Default: 5s.
	// Bounds: 1s to 1m.
	// +kubebuilder:default="5s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('1m')",message="sidecar.peerCheckInterval must be between 1s and 1m"
	// +optional
	PeerCheckInterval *metav1.Duration `json:"peerCheckInterval,omitempty"`

	// OperatorEndpoint is the host:port of the Bloodraven operator's
	// auxiliary endpoint, which the sidecar calls for its startup
	// active-site check and the PITR retention cutoff. When empty, the
	// operator uses its configured default auxiliary Service address.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9.-]+:[0-9]{1,5}$`
	// +optional
	OperatorEndpoint string `json:"operatorEndpoint,omitempty"`
}

// ServiceTemplate customizes the Services created by the operator.
type ServiceTemplate struct {
	// Type is the Kubernetes Service type. Default: ClusterIP
	// +kubebuilder:validation:Enum=ClusterIP;LoadBalancer;NodePort
	// +optional
	Type corev1.ServiceType `json:"type,omitempty"`

	// ExternalTrafficPolicy controls external traffic routing for Services
	// whose effective type is NodePort or LoadBalancer.
	// +optional
	// +kubebuilder:validation:Enum=Cluster;Local
	ExternalTrafficPolicy corev1.ServiceExternalTrafficPolicyType `json:"externalTrafficPolicy,omitempty"`

	// Annotations are additional annotations applied to every Service managed by this failover group.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// SiteServiceTemplate customizes one client-facing site Service.
type SiteServiceTemplate struct {
	// Type overrides the group Service type.
	// +optional
	// +kubebuilder:validation:Enum=ClusterIP;LoadBalancer;NodePort
	Type corev1.ServiceType `json:"type,omitempty"`

	// ExternalTrafficPolicy overrides the group policy.
	// +optional
	// +kubebuilder:validation:Enum=Cluster;Local
	ExternalTrafficPolicy corev1.ServiceExternalTrafficPolicyType `json:"externalTrafficPolicy,omitempty"`

	// NodePort requests a specific port for this site's MySQL endpoint.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	NodePort int32 `json:"nodePort,omitempty"`

	// Annotations are merged over group Service annotations for this site.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// CredentialsSpec configures per-role MySQL credential management.
// Each field references a Secret with 'username' and 'password' keys.
// The operator creates MySQL users with role-appropriate privileges
// and rotates passwords when the referenced Secrets change.
type CredentialsSpec struct {
	// OperatorSecretName names the Secret for operator and sidecar connections.
	// Required keys: username, password, MYSQL_ROOT_PASSWORD.
	// Optional keys: MYSQL_REPLICATION_USER, MYSQL_REPLICATION_PASSWORD
	// (default to operator username/password if omitted).
	// Grants: ALL PRIVILEGES WITH GRANT OPTION.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	OperatorSecretName string `json:"operatorSecretName"`

	// AppSecretName names the Secret for application read-write connections.
	// Required keys: username, password.
	// Grants on *.*: data and schema privileges (SELECT, INSERT, UPDATE,
	// DELETE, CREATE, DROP, ALTER, INDEX, REFERENCES, CREATE TEMPORARY
	// TABLES, LOCK TABLES, EXECUTE, CREATE/SHOW VIEW, CREATE/ALTER ROUTINE,
	// EVENT, TRIGGER, SHOW DATABASES). No GRANT OPTION, SUPER, CREATE USER,
	// or other administrative privileges.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +optional
	AppSecretName string `json:"appSecretName,omitempty"`

	// ReadOnlySecretName names the Secret for application read-only connections.
	// Required keys: username, password.
	// Grants: SELECT, SHOW VIEW, SHOW DATABASES, PROCESS.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +optional
	ReadOnlySecretName string `json:"readOnlySecretName,omitempty"`

	// MonitorSecretName names the Secret for Prometheus exporter connections.
	// Required keys: username, password.
	// Grants: PROCESS, REPLICATION CLIENT, SELECT on performance_schema.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +optional
	MonitorSecretName string `json:"monitorSecretName,omitempty"`

	// BackupSecretName names the Secret for backup connections. Restore
	// Jobs run as the operator account instead: they need DDL, SET GLOBAL
	// and CONNECTION_ADMIN, which this role does not have.
	// Required keys: username, password.
	// Grants: SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER, RELOAD,
	// BACKUP_ADMIN, REPLICATION CLIENT.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +optional
	BackupSecretName string `json:"backupSecretName,omitempty"`
}

// MysqlFailoverGroupStatus defines the observed state of MysqlFailoverGroup.
type MysqlFailoverGroupStatus struct {
	// ObservedGeneration is the .metadata.generation the topology manager
	// has applied: the generation its configuration was built from, or the
	// newest generation an unchanged configuration was resynchronised
	// against. The topology conditions (Ready, Degraded, Bootstrapping,
	// Updating, RecoveryPending, DivergentPromotion) carry the same value.
	// A condition another writer owns carries the generation that writer
	// evaluated, and status.encryptionAtRest.observedGeneration is the
	// encryption reconcile's own.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ActiveSite is the name of the site currently acting as primary (writable).
	// Durable: the topology manager restores it after a restart, before it
	// has observed the sites. Lost beside a failover record, it makes the
	// manager refuse automatic promotion until it observes a writable
	// primary.
	// +optional
	ActiveSite string `json:"activeSite,omitempty"`

	// TopologyGeneration increments whenever the authoritative active site changes
	// and persists across operator restarts to fence deployment leases.
	// Durable: it is restored after a restart and never moves backwards.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TopologyGeneration int64 `json:"topologyGeneration,omitempty"`

	// Sites is the observed state of each site, keyed by name.
	// +optional
	// +listType=map
	// +listMapKey=name
	Sites []SiteStatus `json:"sites,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastFailover is the record of the most recent failover: when it was
	// recorded, the site it promoted and that site's GTID set at promotion.
	// Durable: the topology manager restores the failover cooldown and the
	// promotion watermark from it, or from its annotation copies
	// (bloodraven.shipstream.io/last-failover, last-failover-target and
	// promotion-gtid-executed) when those are newer, after a restart.
	// +optional
	LastFailover *FailoverRecord `json:"lastFailover,omitempty"`

	// UpdatePhase is the current phase of an ordered update. Absent when
	// no ordered update is running.
	// +optional
	UpdatePhase UpdatePhase `json:"updatePhase,omitempty"`

	// Restore tracks an in-flight or completed initFromBackup operation.
	// Durable: until it records Succeeded the group is treated as still
	// restoring (no DNS, no application endpoints, no fresh-deploy clone).
	// +optional
	Restore *RestoreStatus `json:"restore,omitempty"`

	// RestoreInPlace tracks an in-flight or completed in-place restore
	// (spec.restoreInPlace). The consumed confirm token is stamped here
	// on success so subsequent reconciles can distinguish "restore
	// already done" from "new restore requested". Once a run is accepted
	// this status, not the spec, drives it: removing spec.restoreInPlace
	// while its Job runs keeps the fence until the Job finishes.
	// Durable: confirmTokenUsed is the only record that a confirm was
	// consumed, and the run record is what a restarted operator resumes.
	// +optional
	RestoreInPlace *RestoreInPlaceStatus `json:"restoreInPlace,omitempty"`

	// RestoreReclones is the queue of peer sites a full-instance in-place
	// restore still has to reclone from the restored primary. It lives
	// outside restoreInPlace because it outlives that restore: it drains
	// after the restore has succeeded, across later restores and spec
	// edits. Empty when nothing is queued.
	// Durable: pending sites are never promoted and do not replicate until
	// recloned; fencedGtidExecuted decides whether a failed restore's queue
	// is discarded or held.
	// +optional
	RestoreReclones *RestoreReclonesStatus `json:"restoreReclones,omitempty"`

	// RestoreOfflineFence is the offline_mode fence a full-instance
	// in-place restore holds on its target primary. It is cleared once the
	// restore has finished and the prior offline_mode is back.
	// Durable: it is written before offline_mode changes, and a restarted
	// operator re-applies the fence and restores the prior value from it.
	// +optional
	RestoreOfflineFence *RestoreOfflineFenceStatus `json:"restoreOfflineFence,omitempty"`

	// BackupSchedules is a per-schedule rollup of the most recent backup
	// activity for each entry in spec.backup.schedules.
	// +optional
	// +listType=map
	// +listMapKey=name
	BackupSchedules []BackupScheduleStatus `json:"backupSchedules,omitempty"`

	// LastBackupTime is the completion time of the most recent successful
	// MysqlBackup across all profiles, regardless of whether it was
	// scheduled or on-demand.
	// +optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// PITR summarizes the continuous binary-log archive (oldest/newest
	// archived events, count, bytes, per-site errors). The operator
	// populates it from its periodic polls of each sidecar's
	// /archiver/status endpoint while spec.backup.pitr.enabled is true.
	// Whether archiving is currently running is reported by the
	// PITRArchiveReady condition.
	// +optional
	PITR *PITRStatus `json:"pitr,omitempty"`

	// PlannedFailover tracks the most recent planned (admin-triggered)
	// failover attempt. Terminal status is retained until a newer
	// annotation replaces the block; the field is how kubectl describe
	// tells the story of a switchover after the fact.
	// Durable: its write-ahead fence and promotion records are what a
	// restarted operator resumes, rolls back or ends from.
	// +optional
	PlannedFailover *PlannedFailoverStatus `json:"plannedFailover,omitempty"`

	// PlannedFailoverHold is the automation hold left by a planned
	// failover that may have written to its target and ended
	// ManualRecoveryRequired. It outlives that run's plannedFailover block
	// (a refused request can replace the block) and stays until an
	// administrator acknowledges it with
	// bloodraven.shipstream.io/acknowledge-planned-failover-hold=<runID>.
	// The AutomaticFailoverHeld condition summarises it.
	// Durable: losing it would re-enable automatic failover beside a
	// possible split brain; the condition and the planned-failover-terminal
	// annotation are second copies that keep the hold until it is
	// acknowledged.
	// +optional
	PlannedFailoverHold *PlannedFailoverHold `json:"plannedFailoverHold,omitempty"`

	// Dragonfly is the observed state of the Dragonfly subsystem when
	// spec.dragonfly is configured.
	// +optional
	Dragonfly *DragonflyStatus `json:"dragonfly,omitempty"`

	// EncryptionAtRest is the observed state of the data-at-rest
	// encryption subsystem when spec.encryptionAtRest.enabled is true.
	// +optional
	EncryptionAtRest *EncryptionAtRestStatus `json:"encryptionAtRest,omitempty"`

	// PITRArchiveBindings are the encryption identities accepted for the
	// PITR archive destinations this group writes or reads (#203). Written
	// by the group reconciler.
	// Durable: a binding pins the key snapshot every writer and reader of
	// its archive mounts. A lost encrypted entry is rebuilt from the
	// acceptance its key snapshot records (snapshots outlive the group); a
	// lost plaintext entry stays Pending until a writer or a check Job
	// proves the archive again.
	// +optional
	// +listType=map
	// +listMapKey=id
	// +kubebuilder:validation:MaxItems=16
	PITRArchiveBindings []PITRArchiveBinding `json:"pitrArchiveBindings,omitempty"`
}

// SiteStatus describes the observed state of a single site.
type SiteStatus struct {
	// Name is the site identifier, mirrored from spec: the key of
	// status.sites.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// State is the current state: Writable, ReadOnly, Unreachable, or
	// Unknown.
	// +optional
	State SiteState `json:"state,omitempty"`

	// LastSeenTime is the last time this site was successfully polled.
	// +optional
	LastSeenTime *metav1.Time `json:"lastSeenTime,omitempty"`

	// GtidExecuted is the GTID executed set (populated when replication status is enriched).
	// +optional
	GtidExecuted string `json:"gtidExecuted,omitempty"`

	// Replication is the site's replication state: NotApplicable on the
	// writable primary; Healthy or Lagging once both replication threads
	// have run with a source set for consecutive polls (Lagging when
	// secondsBehindSource exceeds spec.replication.maxLagSeconds); Broken
	// when replica status was read and a thread is stopped or no source is
	// set; Unknown when replica status could not be read, the site has not
	// been probed, or the healthy streak is still building.
	// +optional
	Replication SiteReplicationState `json:"replication,omitempty"`

	// SecondsBehindSource is the replication lag in seconds (populated when replication status is enriched).
	// +optional
	SecondsBehindSource *int64 `json:"secondsBehindSource,omitempty"`

	// RecoveryState tracks old-primary recovery progress after failover.
	// Absent when no recovery is needed. RecoveryInProgress means the site is
	// being reconfigured, or is stabilizing, as a replica of the current primary.
	// RecoveryBlocked means the site holds transactions the primary lacks
	// (see DivergenceDetectedBy): it is not promoted by an ordered update or
	// a planned failover, and it must be recloned or the primary made to
	// contain the set. Replication may still be running.
	// +optional
	RecoveryState SiteRecoveryState `json:"recoveryState,omitempty"`

	// DivergentGtid is the GTID set of transactions on this site that
	// the primary named by DivergenceComparedTo lacks. Populated when
	// RecoveryState is RecoveryBlocked.
	// +optional
	DivergentGtid string `json:"divergentGtid,omitempty"`

	// DivergentTransactionCount is the number of divergent transactions.
	// +optional
	DivergentTransactionCount *int64 `json:"divergentTransactionCount,omitempty"`

	// DivergenceDetectedBy names the check that recorded DivergentGtid:
	// OldPrimaryRecovery (a returning former primary was compared with
	// the new primary) or ReplicaAudit (a replica, possibly replicating
	// normally, was found to hold transactions the primary lacks).
	// Populated when RecoveryState is RecoveryBlocked.
	// +optional
	DivergenceDetectedBy DivergenceDetector `json:"divergenceDetectedBy,omitempty"`

	// DivergenceComparedTo is the site whose GTID set DivergentGtid was
	// computed against: the primary at the time of the comparison.
	// Populated when RecoveryState is RecoveryBlocked.
	// +optional
	DivergenceComparedTo string `json:"divergenceComparedTo,omitempty"`

	// SourceHost is the replication source reported by MySQL.
	// +optional
	SourceHost string `json:"sourceHost,omitempty"`

	// SourceConvergenceState reports whether this follower directly follows
	// the confirmed active primary.
	// +optional
	SourceConvergenceState SourceConvergenceState `json:"sourceConvergenceState,omitempty"`

	// SourceConvergenceReason is a stable machine-readable explanation of
	// SourceConvergenceState.
	// +optional
	SourceConvergenceReason SourceConvergenceReason `json:"sourceConvergenceReason,omitempty"`
}

// SiteState is the observed state of a site's MySQL server.
// +kubebuilder:validation:Enum=Writable;ReadOnly;Unreachable;Unknown
type SiteState string

const (
	// SiteStateWritable means the server answered with read_only=OFF.
	SiteStateWritable SiteState = "Writable"
	// SiteStateReadOnly means the server answered with read_only=ON.
	SiteStateReadOnly SiteState = "ReadOnly"
	// SiteStateUnreachable means the operator could not query the server.
	SiteStateUnreachable SiteState = "Unreachable"
	// SiteStateUnknown means the site has not been classified since the
	// operator started.
	SiteStateUnknown SiteState = "Unknown"
)

// SiteReplicationState is status.sites[].replication.
// +kubebuilder:validation:Enum=Healthy;Lagging;Broken;Unknown;NotApplicable
type SiteReplicationState string

const (
	// SiteReplicationHealthy: both replication threads run with a source
	// set, over consecutive polls, within spec.replication.maxLagSeconds.
	SiteReplicationHealthy SiteReplicationState = "Healthy"
	// SiteReplicationLagging: as Healthy, but secondsBehindSource exceeds
	// spec.replication.maxLagSeconds (the Degraded ReplicationLagging
	// threshold).
	SiteReplicationLagging SiteReplicationState = "Lagging"
	// SiteReplicationBroken: replica status was read, and the IO or SQL
	// thread is stopped or no source is set.
	SiteReplicationBroken SiteReplicationState = "Broken"
	// SiteReplicationUnknown: replica status could not be read, the site
	// has not been probed, or the healthy streak is still building.
	SiteReplicationUnknown SiteReplicationState = "Unknown"
	// SiteReplicationNotApplicable: the site is the writable primary.
	SiteReplicationNotApplicable SiteReplicationState = "NotApplicable"
)

// Running reports whether replication runs: Healthy or Lagging. It is the
// predicate every reader that needs a replicating follower uses; a reader
// that also bounds lag applies its own threshold to secondsBehindSource.
func (s SiteReplicationState) Running() bool {
	return s == SiteReplicationHealthy || s == SiteReplicationLagging
}

// FailoverRecord is the durable record of the most recent failover. It
// embeds the {time, target} reference a planned run's claimBaseline
// records, and adds the promotion watermark.
type FailoverRecord struct {
	FailoverRecordRef `json:",inline"`

	// PromotionGtidExecuted is the GTID executed set recorded on the
	// target at the moment of its promotion, before it began accepting
	// writes. Used for data-loss accounting.
	// +optional
	PromotionGtidExecuted string `json:"promotionGtidExecuted,omitempty"`
}

// UpdatePhase is the phase of an in-flight ordered update. While the
// update runs it is also the reason of the Updating condition.
// +kubebuilder:validation:Enum=UpdateReplica;WaitReplica;Failover;UpdateOldPrimary;WaitOldPrimary
type UpdatePhase string

const (
	// UpdatePhaseUpdateReplica means a follower's Deployment is being
	// updated.
	UpdatePhaseUpdateReplica UpdatePhase = ReasonUpdateReplica
	// UpdatePhaseWaitReplica means the update waits for an updated
	// follower to be ready and caught up.
	UpdatePhaseWaitReplica UpdatePhase = ReasonWaitReplica
	// UpdatePhaseFailover means the active site is being handed off to an
	// updated standby.
	UpdatePhaseFailover UpdatePhase = ReasonFailover
	// UpdatePhaseUpdateOldPrimary means the former active site's
	// Deployment is being updated.
	UpdatePhaseUpdateOldPrimary UpdatePhase = ReasonUpdateOldPrimary
	// UpdatePhaseWaitOldPrimary means the update waits for the former
	// active site to replicate from the new primary.
	UpdatePhaseWaitOldPrimary UpdatePhase = ReasonWaitOldPrimary
)

// SourceConvergenceState describes direct-primary source convergence.
// +kubebuilder:validation:Enum=Converged;Pending;Blocked
type SourceConvergenceState string

const (
	SourceConvergenceConverged SourceConvergenceState = "Converged"
	SourceConvergencePending   SourceConvergenceState = "Pending"
	SourceConvergenceBlocked   SourceConvergenceState = "Blocked"
)

// SourceConvergenceReason explains a SourceConvergenceState.
// +kubebuilder:validation:Enum=DirectSource;SourceMismatch;ProbeFailed;MutationFailed;GTIDDiverged;RestorePeerHold
type SourceConvergenceReason string

const (
	// SourceReasonDirectSource: the follower replicates directly from the
	// confirmed active primary.
	SourceReasonDirectSource SourceConvergenceReason = "DirectSource"
	// SourceReasonSourceMismatch: the follower replicates from another
	// source, or its threads are not running.
	SourceReasonSourceMismatch SourceConvergenceReason = "SourceMismatch"
	// SourceReasonProbeFailed: the follower's replication status could
	// not be read.
	SourceReasonProbeFailed SourceConvergenceReason = "ProbeFailed"
	// SourceReasonMutationFailed: a source correction failed.
	SourceReasonMutationFailed SourceConvergenceReason = "MutationFailed"
	// SourceReasonGTIDDiverged: the follower holds transactions the
	// primary lacks, so it is not repointed.
	SourceReasonGTIDDiverged SourceConvergenceReason = "GTIDDiverged"
	// SourceReasonRestorePeerHold: the site awaits its post-restore
	// reclone (status.restoreReclones.pending) and its replication is held
	// stopped, so convergence does not touch it.
	SourceReasonRestorePeerHold SourceConvergenceReason = "RestorePeerHold"
)

// SiteRecoveryState is status.sites[].recoveryState. Absent means no
// recovery is needed.
// +kubebuilder:validation:Enum=RecoveryInProgress;RecoveryBlocked
type SiteRecoveryState string

const (
	// RecoveryStateInProgress means the site is being reconfigured, or is
	// stabilizing, as a replica of the current primary.
	RecoveryStateInProgress SiteRecoveryState = "RecoveryInProgress"
	// RecoveryStateBlocked means the site holds transactions the primary
	// lacks.
	RecoveryStateBlocked SiteRecoveryState = "RecoveryBlocked"
)

// DivergenceDetector names the check that recorded a divergence report.
// +kubebuilder:validation:Enum=OldPrimaryRecovery;ReplicaAudit
type DivergenceDetector string

const (
	// DivergenceDetectorOldPrimaryRecovery: a returning former primary was
	// compared with the new primary.
	DivergenceDetectorOldPrimaryRecovery DivergenceDetector = "OldPrimaryRecovery"
	// DivergenceDetectorReplicaAudit: a replica, possibly replicating
	// normally, was found to hold transactions the primary lacks.
	DivergenceDetectorReplicaAudit DivergenceDetector = "ReplicaAudit"
)

func init() {
	SchemeBuilder.Register(&MysqlFailoverGroup{}, &MysqlFailoverGroupList{})
}
