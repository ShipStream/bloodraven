package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DragonflySpec configures an optional per-site Dragonfly cluster co-managed
// with MySQL. When enabled, Bloodraven creates one Dragonfly StatefulSet per
// site, configures non-active sites as replicas of the active site, and
// participates in planned and emergency failover. Dragonfly is treated as
// non-durable cache/session state: emergency failover never blocks on it.
//
// +kubebuilder:validation:XValidation:rule="!self.enabled || (has(self.image) && self.image.size() > 0)",message="spec.dragonfly.image is required when spec.dragonfly.enabled is true"
// +kubebuilder:validation:XValidation:rule="!self.enabled || !self.image.endsWith(':latest')",message="spec.dragonfly.image must be pinned (no :latest)"
// +kubebuilder:validation:XValidation:rule="!has(self.auth) || (has(self.auth.secretName) && self.auth.secretName.size() > 0)",message="spec.dragonfly.auth.secretName is required when auth is set"
type DragonflySpec struct {
	// Enabled toggles Dragonfly co-management. When false (default), no
	// Dragonfly resources are created and existing MySQL-only behavior
	// is preserved.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Image is the pinned Dragonfly container image. Required when
	// Enabled is true. Tag ":latest" is rejected.
	// +optional
	Image string `json:"image,omitempty"`

	// Port is the client (Redis-compatible) port. Default: 6379.
	// +kubebuilder:default=6379
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// AdminPort is the admin port used for operator-side commands. Default: 9999.
	// +kubebuilder:default=9999
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	AdminPort int32 `json:"adminPort,omitempty"`

	// MaxMemoryMb caps Dragonfly's memory budget per pod. Translates to
	// the --maxmemory flag. 0 means leave unset (Dragonfly default).
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxMemoryMb int32 `json:"maxMemoryMb,omitempty"`

	// ProactorThreads sets Dragonfly's --proactor_threads flag. 0 means
	// leave unset (Dragonfly default: one per CPU).
	// +kubebuilder:validation:Minimum=0
	// +optional
	ProactorThreads int32 `json:"proactorThreads,omitempty"`

	// Args are additional command-line arguments appended to the
	// Dragonfly container command. Operator-managed flags (for example
	// --port, --admin_port, --maxmemory, --proactor_threads,
	// --requirepass, and --replicaof) are reserved for the operator and
	// cannot be overridden via spec.dragonfly.args.
	// +optional
	Args []string `json:"args,omitempty"`

	// Auth references a Secret containing the Dragonfly password. When
	// set, the operator wires --requirepass and authenticates its own
	// connections.
	// +optional
	Auth *DragonflyAuthSpec `json:"auth,omitempty"`

	// Resources defines the compute resources for the Dragonfly container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// PlannedFailover configures planned-switchover behavior.
	// +optional
	PlannedFailover *DragonflyPlannedFailoverSpec `json:"plannedFailover,omitempty"`

	// Snapshot configures Dragonfly snapshot persistence. When Dir is an
	// s3:// URI, Dragonfly writes snapshots to that bucket/prefix and
	// restores from it when a replacement pod starts. Bloodraven treats this
	// as a planned-maintenance continuity mechanism, not durable application
	// backup.
	// +optional
	Snapshot *DragonflySnapshotSpec `json:"snapshot,omitempty"`

	// PodSecurityContext optionally sets the pod-level security context for
	// the Dragonfly StatefulSet. When nil (default), no security context is
	// set on the pod; this preserves backward compatibility with existing
	// clusters. Setting this field will apply the value as-is to the pod;
	// the operator does not merge it with hardened defaults. To enable
	// Restricted PSS, set the standard fields (RunAsNonRoot, RunAsUser,
	// RunAsGroup, FSGroup, SeccompProfile). See
	// docs/docs/production-hardening.mdx for the upgrade procedure.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// ContainerSecurityContext optionally sets the container-level security
	// context for the Dragonfly StatefulSet's `dragonfly` container. Same
	// backward-compatibility semantics as PodSecurityContext.
	// +optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`
}

// DragonflySnapshotSpec configures Dragonfly's native snapshot location.
type DragonflySnapshotSpec struct {
	// Dir is passed to Dragonfly as --dir. Use s3://bucket[/prefix] for S3
	// snapshot/restore support. Empty leaves Dragonfly's default data dir.
	// +optional
	Dir string `json:"dir,omitempty"`

	// ServiceAccountName is assigned to Dragonfly pods so cloud IAM systems
	// such as EKS IRSA can grant access to the snapshot bucket without static
	// credentials. Empty uses the namespace default ServiceAccount.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// CredentialsSecretName is a Secret projected into Dragonfly pods as
	// environment variables for S3-compatible snapshot access. Typical keys
	// are AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION. Optional
	// when ServiceAccountName provides credentials through cloud IAM.
	// +optional
	CredentialsSecretName string `json:"credentialsSecretName,omitempty"`

	// S3Endpoint is passed to Dragonfly as --s3_endpoint for S3-compatible
	// services such as RustFS or MinIO. Leave empty for AWS S3.
	// +optional
	S3Endpoint string `json:"s3Endpoint,omitempty"`

	// S3UseHTTPS controls Dragonfly's --s3_use_https flag. Leave nil for
	// Dragonfly's default. Set false for in-cluster HTTP services like the
	// playground RustFS deployment.
	// +optional
	S3UseHTTPS *bool `json:"s3UseHTTPS,omitempty"`

	// S3SignPayload controls Dragonfly's --s3_sign_payload flag. Leave nil
	// for Dragonfly's default.
	// +optional
	S3SignPayload *bool `json:"s3SignPayload,omitempty"`
}

// DragonflyAuthSpec references a Secret containing the Dragonfly password.
type DragonflyAuthSpec struct {
	// SecretName is the name of the Secret containing the password.
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`

	// PasswordKey is the key inside the Secret that holds the password.
	// Default: "password".
	// +kubebuilder:default="password"
	// +optional
	PasswordKey string `json:"passwordKey,omitempty"`
}

// DragonflyPlannedFailoverSpec configures planned-switchover semantics for
// the Dragonfly subsystem.
type DragonflyPlannedFailoverSpec struct {
	// MaxSyncWait bounds the time the planned-failover state machine
	// waits in WaitingForDragonflySync for the target replica to catch
	// up to the source's replication offset. The same duration is used
	// as the REPLTAKEOVER timeout argument during PromotingDragonfly.
	// Default: 30s.
	// +kubebuilder:default="30s"
	// +optional
	MaxSyncWait *metav1.Duration `json:"maxSyncWait,omitempty"`

	// OnSyncTimeout governs behavior when the target replica fails to
	// reach the source's offset within MaxSyncWait, or when REPLTAKEOVER
	// fails:
	//   - "proceed" (default): continue with MySQL promotion, stamp
	//     sessionsPreserved=false. Cache/session continuity is best-effort.
	//   - "fail": abort planned failover before MySQL promotion, roll
	//     back the source MySQL fence, leave activeSite unchanged.
	// +kubebuilder:default="proceed"
	// +kubebuilder:validation:Enum=proceed;fail
	// +optional
	OnSyncTimeout string `json:"onSyncTimeout,omitempty"`
}

// DragonflyPhase describes the high-level state of the Dragonfly subsystem
// for a failover group.
// +kubebuilder:validation:Enum="";Disabled;Reconciling;ConfiguringReplication;Ready;Degraded;Promoting
type DragonflyPhase string

const (
	// DragonflyPhaseDisabled means spec.dragonfly is unset or enabled=false.
	DragonflyPhaseDisabled DragonflyPhase = "Disabled"

	// DragonflyPhaseReconciling means resources are being created or
	// updated and the topology is not yet stable.
	DragonflyPhaseReconciling DragonflyPhase = "Reconciling"

	// DragonflyPhaseConfiguringReplication means the master is up and
	// replicas are being linked.
	DragonflyPhaseConfiguringReplication DragonflyPhase = "ConfiguringReplication"

	// DragonflyPhaseReady means exactly one master is serving and all
	// replicas are linked and not syncing.
	DragonflyPhaseReady DragonflyPhase = "Ready"

	// DragonflyPhaseDegraded means at least one site is unhealthy or a
	// stale master is present, but the active write path is still
	// functional.
	DragonflyPhaseDegraded DragonflyPhase = "Degraded"

	// DragonflyPhasePromoting means a planned-failover Dragonfly
	// promotion is in flight.
	DragonflyPhasePromoting DragonflyPhase = "Promoting"
)

// DragonflyRole describes a Dragonfly site's observed role.
// +kubebuilder:validation:Enum=unknown;master;replica;stale-master;unconfigured;unreachable
type DragonflyRole string

const (
	DragonflyRoleUnknown      DragonflyRole = "unknown"
	DragonflyRoleMaster       DragonflyRole = "master"
	DragonflyRoleReplica      DragonflyRole = "replica"
	DragonflyRoleStaleMaster  DragonflyRole = "stale-master"
	DragonflyRoleUnconfigured DragonflyRole = "unconfigured"
	DragonflyRoleUnreachable  DragonflyRole = "unreachable"
)

// DragonflyStatus is the observed state of the Dragonfly subsystem for a
// failover group.
type DragonflyStatus struct {
	// Enabled mirrors spec.dragonfly.enabled at the time of the last
	// observation.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ActiveSite is the site currently acting as Dragonfly master.
	// Normally equal to status.activeSite (MySQL active); during a
	// planned-failover Dragonfly promotion the two may briefly diverge.
	// +optional
	ActiveSite string `json:"activeSite,omitempty"`

	// Phase is the high-level Dragonfly state.
	// +optional
	Phase DragonflyPhase `json:"phase,omitempty"`

	// Message is a human-readable status line.
	// +optional
	Message string `json:"message,omitempty"`

	// LastPromotionTime is the timestamp of the most recent successful
	// Dragonfly promotion (planned or emergency).
	// +optional
	LastPromotionTime *metav1.Time `json:"lastPromotionTime,omitempty"`

	// LastPromotionTarget is the site promoted in the most recent
	// successful Dragonfly promotion.
	// +optional
	LastPromotionTarget string `json:"lastPromotionTarget,omitempty"`

	// Sites is the per-site Dragonfly observation, parallel in spirit
	// to status.sites for MySQL.
	// +optional
	Sites []DragonflySiteStatus `json:"sites,omitempty"`

	// Upgrade tracks an explicit Dragonfly snapshot-restore upgrade
	// requested by annotation. It is separate from normal spec reconciliation
	// because this workflow intentionally creates a short cache outage while
	// preserving sessions through a pre-upgrade snapshot.
	// +optional
	Upgrade *DragonflyUpgradeStatus `json:"upgrade,omitempty"`
}

// DragonflyUpgradePhase describes the D6a snapshot-restore planned upgrade
// state machine.
// +kubebuilder:validation:Enum=Pending;SavingSnapshot;UpdatingActive;WaitingForActiveRestore;ReattachingReplicas;Succeeded;Failed
type DragonflyUpgradePhase string

const (
	DragonflyUpgradePhasePending                 DragonflyUpgradePhase = "Pending"
	DragonflyUpgradePhaseSavingSnapshot          DragonflyUpgradePhase = "SavingSnapshot"
	DragonflyUpgradePhaseUpdatingActive          DragonflyUpgradePhase = "UpdatingActive"
	DragonflyUpgradePhaseWaitingForActiveRestore DragonflyUpgradePhase = "WaitingForActiveRestore"
	DragonflyUpgradePhaseReattachingReplicas     DragonflyUpgradePhase = "ReattachingReplicas"
	DragonflyUpgradePhaseSucceeded               DragonflyUpgradePhase = "Succeeded"
	DragonflyUpgradePhaseFailed                  DragonflyUpgradePhase = "Failed"
)

// DragonflyUpgradeStatus is the observable audit trail for a one-shot
// snapshot-restore Dragonfly upgrade.
type DragonflyUpgradeStatus struct {
	// Phase is the current upgrade phase.
	// +optional
	Phase DragonflyUpgradePhase `json:"phase,omitempty"`

	// SourceImage is the image in use when the request was accepted.
	// +optional
	SourceImage string `json:"sourceImage,omitempty"`

	// TargetImage is the requested replacement image.
	// +optional
	TargetImage string `json:"targetImage,omitempty"`

	// ActiveSite is the site that was active when the request was accepted.
	// +optional
	ActiveSite string `json:"activeSite,omitempty"`

	// SnapshotDir is the Dragonfly --dir used for SAVE and startup restore.
	// +optional
	SnapshotDir string `json:"snapshotDir,omitempty"`

	// StartTime is when the upgrade request was accepted.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// SnapshotTime is when SAVE completed successfully.
	// +optional
	SnapshotTime *metav1.Time `json:"snapshotTime,omitempty"`

	// CompletionTime is set on Succeeded or Failed.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Reason is a machine-readable terminal or waiting reason.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable status line.
	// +optional
	Message string `json:"message,omitempty"`
}

// DragonflySiteStatus describes the observed state of one site's Dragonfly
// instance.
type DragonflySiteStatus struct {
	// Name is the site identifier, mirrored from spec for convenience.
	Name string `json:"name,omitempty"`

	// Role is the site's observed Dragonfly role.
	// +optional
	Role DragonflyRole `json:"role,omitempty"`

	// Reachable is true when the operator successfully connected and
	// completed an INFO replication call on the most recent poll.
	// +optional
	Reachable bool `json:"reachable,omitempty"`

	// ServiceName is the site-local Dragonfly Service name.
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// PodName is the Dragonfly pod name.
	// +optional
	PodName string `json:"podName,omitempty"`

	// ReplicationState is the parsed "role" field from INFO replication.
	// +optional
	ReplicationState string `json:"replicationState,omitempty"`

	// LinkStatus is the parsed master_link_status field from INFO
	// replication on a replica. Empty on a master.
	// +optional
	LinkStatus string `json:"linkStatus,omitempty"`

	// SyncInProgress is the parsed master_sync_in_progress field from
	// INFO replication on a replica.
	// +optional
	SyncInProgress bool `json:"syncInProgress,omitempty"`

	// LastIOSecondsAgo is the parsed master_last_io_seconds_ago field from
	// INFO replication on a replica. A value of -1 means the replica has
	// never received data from its master.
	// +optional
	LastIOSecondsAgo int `json:"lastIOSecondsAgo,omitempty"`

	// Ready reports operator-level readiness: a master must be reachable
	// and accepting writes; a replica must additionally have its link up,
	// have received at least one byte from the master, not be syncing, and
	// not be loading.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// Message is a human-readable status line for this site.
	// +optional
	Message string `json:"message,omitempty"`
}
