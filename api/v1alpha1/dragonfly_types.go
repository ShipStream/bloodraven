package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
// +kubebuilder:validation:XValidation:rule="!has(self.upgradeStrategy) || self.upgradeStrategy != 'SnapshotRestore' || (has(self.snapshot) && has(self.snapshot.dir) && self.snapshot.dir.size() > 0)",message="spec.dragonfly.snapshot.dir is required when spec.dragonfly.upgradeStrategy is SnapshotRestore"
type DragonflySpec struct {
	// Enabled toggles Dragonfly co-management. When false (default), no
	// Dragonfly resources are created and existing MySQL-only behavior
	// is preserved.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Image is the pinned Dragonfly container image. Required when
	// Enabled is true. Tag ":latest" is rejected. How a change reaches the
	// running sites is UpgradeStrategy's choice.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Image string `json:"image,omitempty"`

	// UpgradeStrategy decides how a change of Image reaches the running
	// Dragonfly sites:
	//   - Rolling (default): the StatefulSets roll like any other template
	//     change, replicas first, then the active site after a promotion.
	//     Cache contents survive only through replication.
	//   - SnapshotRestore: the operator saves a snapshot of the active
	//     site into snapshot.dir, restarts it on the new image restoring
	//     from that snapshot, then moves the other sites and re-attaches
	//     them as replicas. Cache and session traffic is shed while it
	//     runs; status.dragonfly.upgrade records the run. The ordinary
	//     render never moves an image under this strategy. A run that
	//     fails is not restarted for the same spec.dragonfly: change it
	//     (or set Image back to the running image and then forward again)
	//     to retry. Requires snapshot.dir.
	// Switching to Rolling while a change is pending rolls it the ordinary
	// way.
	// +kubebuilder:default=Rolling
	// +optional
	UpgradeStrategy DragonflyUpgradeStrategy `json:"upgradeStrategy,omitempty"`

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

	// MaxMemory caps Dragonfly's memory budget per pod (for example
	// "512Mi"), rendered as --maxmemory in bytes. Must be positive. Unset
	// leaves Dragonfly's default.
	// +kubebuilder:validation:XValidation:rule="quantity(string(self)).isGreaterThan(quantity('0'))",message="dragonfly.maxMemory must be greater than 0"
	// +optional
	MaxMemory *resource.Quantity `json:"maxMemory,omitempty"`

	// ProactorThreads sets Dragonfly's --proactor_threads flag. Unset
	// leaves Dragonfly's default (one per CPU).
	// +kubebuilder:validation:Minimum=1
	// +optional
	ProactorThreads *int32 `json:"proactorThreads,omitempty"`

	// Args are additional command-line arguments appended to the
	// Dragonfly container command. The flags the operator owns are refused,
	// in any spelling Dragonfly reads (-flag, --flag, --flag=value, and
	// --noflag): --port, --admin_port, --bind, --dir, --maxmemory,
	// --proactor_threads, --replicaof, --requirepass, --masterauth,
	// --break_replication_on_master_restart, --s3_endpoint, --s3_use_https
	// and --s3_sign_payload, and --flagfile, --fromenv and --tryfromenv,
	// which read flags from elsewhere. Use the typed fields for the values
	// the operator renders.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="self.all(a, !a.matches('^--?(no)?(port|admin_port|bind|dir|maxmemory|proactor_threads|replicaof|requirepass|masterauth|break_replication_on_master_restart|s3_endpoint|s3_use_https|s3_sign_payload|flagfile|fromenv|tryfromenv)(=|$)'))",message="spec.dragonfly.args must not set a flag the operator owns (port, admin_port, bind, dir, maxmemory, proactor_threads, replicaof, requirepass, masterauth, break_replication_on_master_restart, s3_endpoint, s3_use_https, s3_sign_payload) or read flags from elsewhere (flagfile, fromenv, tryfromenv)"
	Args []string `json:"args,omitempty"`

	// Auth references a Secret containing the Dragonfly password. When
	// set, the operator wires --requirepass and --masterauth (so replicas
	// can authenticate to a password-protected master) and authenticates
	// its own connections.
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
	// https://bloodraven.dev/docs/configuration/production-hardening for the upgrade procedure.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// ContainerSecurityContext optionally sets the container-level security
	// context for the Dragonfly StatefulSet's `dragonfly` container. Same
	// backward-compatibility semantics as PodSecurityContext.
	// +optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`
}

// DragonflyUpgradeStrategy is spec.dragonfly.upgradeStrategy.
// +kubebuilder:validation:Enum=Rolling;SnapshotRestore
type DragonflyUpgradeStrategy string

const (
	// DragonflyUpgradeStrategyRolling rolls an image change like any other
	// StatefulSet template change.
	DragonflyUpgradeStrategyRolling DragonflyUpgradeStrategy = "Rolling"
	// DragonflyUpgradeStrategySnapshotRestore moves an image change
	// through the snapshot-restore upgrade.
	DragonflyUpgradeStrategySnapshotRestore DragonflyUpgradeStrategy = "SnapshotRestore"
)

// DragonflySnapshotSpec configures Dragonfly's native snapshot location.
type DragonflySnapshotSpec struct {
	// Dir is passed to Dragonfly as --dir. Use s3://bucket[/prefix] for S3
	// snapshot/restore support. Empty leaves Dragonfly's default data dir.
	// +kubebuilder:validation:MaxLength=1024
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
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +optional
	CredentialsSecretName string `json:"credentialsSecretName,omitempty"`

	// S3EndpointURL overrides the S3 endpoint for S3-compatible services
	// such as RustFS or MinIO, in the URL form backup storage uses: an
	// absolute http:// or https:// URL with a host (a name, an IPv4
	// address, or an IPv6 address in brackets) and an optional port from 1
	// to 65535, and no path, credentials, query or fragment. The operator
	// passes the host and port to Dragonfly as --s3_endpoint and the
	// scheme as --s3_use_https (false for http://, true for https://).
	// Omit it, or leave it empty, for AWS S3 with Dragonfly's defaults.
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="bytes(self).size() <= 512",message="s3EndpointURL must be at most 512 bytes (UTF-8)"
	// +kubebuilder:validation:Pattern=`^(https?://(\[[0-9A-Fa-f:.]+\]|[^\s/?#@:\[\]]+)(:(6553[0-5]|655[0-2][0-9]|65[0-4][0-9]{2}|6[0-4][0-9]{3}|[1-5][0-9]{4}|[1-9][0-9]{0,3}))?/?)?$`
	// +optional
	S3EndpointURL string `json:"s3EndpointURL,omitempty"`

	// S3SignPayload controls Dragonfly's --s3_sign_payload flag. Leave nil
	// for Dragonfly's default.
	// +optional
	S3SignPayload *bool `json:"s3SignPayload,omitempty"`
}

// DragonflyAuthSpec references a Secret containing the Dragonfly password.
type DragonflyAuthSpec struct {
	// PasswordSecretKeyRef selects the Secret key, in the group's
	// namespace, that holds the Dragonfly password.
	// +required
	PasswordSecretKeyRef DragonflyPasswordSecretKeyRef `json:"passwordSecretKeyRef"`
}

// DragonflyPasswordSecretKeyRef selects the Secret key holding the
// Dragonfly password.
type DragonflyPasswordSecretKeyRef struct {
	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	Name string `json:"name"`

	// Key within the Secret that holds the password. Default: "password".
	// +kubebuilder:default="password"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	// +optional
	Key string `json:"key,omitempty"`
}

// DragonflyPlannedFailoverSpec configures planned-switchover semantics for
// the Dragonfly subsystem.
type DragonflyPlannedFailoverSpec struct {
	// MaxSyncWait bounds the time the planned-failover state machine
	// waits in WaitingForDragonflySync for the target replica to catch
	// up to the source's replication offset. The same duration is used
	// as the REPLTAKEOVER timeout argument during PromotingDragonfly;
	// Dragonfly takes that argument in whole seconds, so it is rounded up
	// to the next second.
	// Default: 30s. Bounds: 1s to 10m.
	// +kubebuilder:default="30s"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('10m')",message="dragonfly.plannedFailover.maxSyncWait must be between 1s and 10m"
	// +optional
	MaxSyncWait *metav1.Duration `json:"maxSyncWait,omitempty"`

	// OnSyncTimeout governs behavior when the target replica fails to
	// reach the source's offset within MaxSyncWait, or when REPLTAKEOVER
	// fails:
	//   - Proceed (default): continue with the MySQL promotion and record
	//     status.plannedFailover.dragonfly.sessions=Lost. Cache/session
	//     continuity is best-effort.
	//   - Fail: abort the planned failover before the MySQL promotion,
	//     roll back the source MySQL fence, leave activeSite unchanged.
	// +kubebuilder:default=Proceed
	// +optional
	OnSyncTimeout DragonflySyncTimeoutPolicy `json:"onSyncTimeout,omitempty"`
}

// DragonflySyncTimeoutPolicy is spec.dragonfly.plannedFailover.onSyncTimeout
// and the run's frozen settings.dragonfly.onSyncTimeout.
// +kubebuilder:validation:Enum=Proceed;Fail
type DragonflySyncTimeoutPolicy string

const (
	// DragonflySyncTimeoutProceed continues with the MySQL promotion.
	DragonflySyncTimeoutProceed DragonflySyncTimeoutPolicy = "Proceed"
	// DragonflySyncTimeoutFail rolls the planned failover back.
	DragonflySyncTimeoutFail DragonflySyncTimeoutPolicy = "Fail"
)

// DragonflyPhase describes the high-level state of the Dragonfly subsystem
// for a failover group.
// +kubebuilder:validation:Enum=Disabled;Reconciling;ConfiguringReplication;Ready;Degraded;Promoting
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
// +kubebuilder:validation:Enum=Unknown;Master;Replica;StaleMaster;Unconfigured;Unreachable
type DragonflyRole string

const (
	// DragonflyRoleUnknown: the site answered with a replication role the
	// operator does not recognise.
	DragonflyRoleUnknown DragonflyRole = "Unknown"
	// DragonflyRoleMaster: the site reports master and is the expected
	// active site.
	DragonflyRoleMaster DragonflyRole = "Master"
	// DragonflyRoleReplica: the site reports replica, whatever its link
	// health.
	DragonflyRoleReplica DragonflyRole = "Replica"
	// DragonflyRoleStaleMaster: the site reports master but is not the
	// expected active site.
	DragonflyRoleStaleMaster DragonflyRole = "StaleMaster"
	// DragonflyRoleUnconfigured: the site reports no replication role (a
	// fresh pod, not yet wired).
	DragonflyRoleUnconfigured DragonflyRole = "Unconfigured"
	// DragonflyRoleUnreachable: the operator could not reach the site.
	DragonflyRoleUnreachable DragonflyRole = "Unreachable"
)

// DragonflyStatus is the observed state of the Dragonfly subsystem for a
// failover group.
type DragonflyStatus struct {
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

	// Sites is the per-site Dragonfly observation, keyed by name like
	// status.sites for MySQL.
	// +optional
	// +listType=map
	// +listMapKey=name
	Sites []DragonflySiteStatus `json:"sites,omitempty"`

	// Image is the Dragonfly container image of the active Dragonfly
	// site's StatefulSet, as the operator last observed it. It is an
	// observation only: the snapshot-restore upgrade compares
	// spec.dragonfly.image with each site's live StatefulSet, never with
	// this field, and sites can run different images while an upgrade
	// runs or after one stopped part way (status.dragonfly.upgrade).
	// +optional
	Image string `json:"image,omitempty"`

	// Upgrade records the latest snapshot-restore upgrade run
	// (spec.dragonfly.upgradeStrategy SnapshotRestore). The run
	// intentionally creates a short cache outage while preserving sessions
	// through a pre-upgrade snapshot. A Failed record stays until
	// spec.dragonfly changes, or until no site runs an image other than
	// spec.dragonfly.image, when it is removed.
	// +optional
	Upgrade *DragonflyUpgradeStatus `json:"upgrade,omitempty"`

	// ReplTakeoverSupported is true when every successfully probed
	// reachable site advertised the REPLTAKEOVER command. False means at
	// least one probed site lacks it — emergency promotion will fall
	// back to REPLICAOF NO ONE. Nil means the operator has not yet
	// completed a probe (sites down, AUTH failing, or first tick
	// pending). This reports the running command table, not the image
	// tag; v1.38.0+ remains the supported pin.
	// +optional
	ReplTakeoverSupported *bool `json:"replTakeoverSupported,omitempty"`

	// ReplTakeoverProbeTime is when ReplTakeoverSupported or
	// ReplTakeoverProbeMessage last changed.
	// +optional
	ReplTakeoverProbeTime *metav1.Time `json:"replTakeoverProbeTime,omitempty"`

	// ReplTakeoverProbeMessage explains a false result (which sites
	// lacked the command). Empty when supported or not yet probed.
	// +optional
	ReplTakeoverProbeMessage string `json:"replTakeoverProbeMessage,omitempty"`
}

// DragonflyUpgradePhase describes the snapshot-restore upgrade state
// machine.
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

// DragonflyUpgradeStatus is the run record of one snapshot-restore
// Dragonfly upgrade.
type DragonflyUpgradeStatus struct {
	// Phase is the current upgrade phase.
	// +optional
	Phase DragonflyUpgradePhase `json:"phase,omitempty"`

	// SourceImage is the image the run moves from: the live image of the
	// active site's StatefulSet when the run started, or, for a run that
	// started after the active site had already moved, the live image of
	// the first site that had not.
	// +optional
	SourceImage string `json:"sourceImage,omitempty"`

	// TargetImage is the image the run moves to: spec.dragonfly.image when
	// the run started.
	// +optional
	TargetImage string `json:"targetImage,omitempty"`

	// SpecDigest is "sha256:<hex>" over spec.dragonfly as the run saw it
	// when it started. A Failed run is not restarted while spec.dragonfly
	// still matches it.
	// +optional
	SpecDigest string `json:"specDigest,omitempty"`

	// ActiveSite is the MySQL active site when the run started: the site
	// whose snapshot it saves and restores.
	// +optional
	ActiveSite string `json:"activeSite,omitempty"`

	// SnapshotDir is the Dragonfly --dir used for SAVE and startup restore.
	// +optional
	SnapshotDir string `json:"snapshotDir,omitempty"`

	// StartTime is when the run started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// SnapshotTime is when SAVE completed successfully.
	// +optional
	SnapshotTime *metav1.Time `json:"snapshotTime,omitempty"`

	// CompletionTime is set on Succeeded or Failed.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Reason is a machine-readable terminal reason.
	// +optional
	Reason DragonflyUpgradeReason `json:"reason,omitempty"`

	// Message is a human-readable status line.
	// +optional
	Message string `json:"message,omitempty"`
}

// DragonflyUpgradeReason is status.dragonfly.upgrade.reason.
// +kubebuilder:validation:MaxLength=1024
// +kubebuilder:validation:Pattern=`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`
type DragonflyUpgradeReason string

const (
	// DragonflyUpgradeReasonCompleted: the upgrade succeeded.
	DragonflyUpgradeReasonCompleted DragonflyUpgradeReason = "Completed"
	// DragonflyUpgradeReasonActiveSiteChanged: the MySQL active site moved
	// while the upgrade ran.
	DragonflyUpgradeReasonActiveSiteChanged DragonflyUpgradeReason = "ActiveSiteChanged"
	// DragonflyUpgradeReasonUnknownPhase: the recorded phase is not one
	// this operator knows.
	DragonflyUpgradeReasonUnknownPhase DragonflyUpgradeReason = "UnknownPhase"
	// DragonflyUpgradeReasonSnapshotDialFailed: the operator could not
	// connect to the active Dragonfly to save the snapshot.
	DragonflyUpgradeReasonSnapshotDialFailed DragonflyUpgradeReason = "DragonflySnapshotDialFailed"
	// DragonflyUpgradeReasonSnapshotSaveFailed: SAVE failed on the active
	// Dragonfly.
	DragonflyUpgradeReasonSnapshotSaveFailed DragonflyUpgradeReason = "DragonflySnapshotSaveFailed"
	// DragonflyUpgradeReasonWorkloadSelectorMismatch: a site's StatefulSet
	// was refused for its selector (WorkloadSelectorMismatch) while the
	// upgrade ran, so the upgrade stopped before touching it.
	DragonflyUpgradeReasonWorkloadSelectorMismatch DragonflyUpgradeReason = "WorkloadSelectorMismatch"
	// DragonflyUpgradeReasonActiveWorkloadDeleted: the active site's
	// StatefulSet was deleted before its snapshot was saved or after it
	// restored, so the data the upgrade moves was lost; the operator
	// re-creates the StatefulSet.
	DragonflyUpgradeReasonActiveWorkloadDeleted DragonflyUpgradeReason = "ActiveWorkloadDeleted"
)

// DragonflySiteStatus describes the observed state of one site's Dragonfly
// instance.
type DragonflySiteStatus struct {
	// Name is the site identifier, mirrored from spec: the key of
	// status.dragonfly.sites.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

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
	// INFO replication on a replica. Unset when Dragonfly did not report
	// it.
	// +optional
	SyncInProgress *bool `json:"syncInProgress,omitempty"`

	// LastIOSecondsAgo is the parsed master_last_io_seconds_ago field from
	// INFO replication on a replica. A value of -1 means the replica has
	// never received data from its master. Unset when Dragonfly did not
	// report it.
	// +optional
	LastIOSecondsAgo *int64 `json:"lastIOSecondsAgo,omitempty"`

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
