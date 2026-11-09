package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlannedFailoverMaxLagWaitMin and PlannedFailoverMaxLagWaitMax bound
// spec.plannedFailover.maxLagWait (its CEL rule) and the planned-failover
// annotation's maxLagWait= override, which the operator checks against the
// same pair.
const (
	PlannedFailoverMaxLagWaitMin = time.Second
	PlannedFailoverMaxLagWaitMax = time.Hour
)

// PlannedFailoverSpec configures admin-triggered graceful switchover.
// All fields are optional; an omitted block is equivalent to the defaults
// documented on each field.
type PlannedFailoverSpec struct {
	// MaxLagWait bounds the time spent waiting for the target's
	// GTID_EXECUTED to cover the fenced source's GTID_EXECUTED before
	// the state machine rolls back to the source. The source is fenced
	// while the run waits, so this also bounds the write outage.
	// Default: 5m. Bounds: 1s to 1h, the same bounds as the
	// planned-failover annotation's maxLagWait= override.
	// +kubebuilder:default="5m0s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('1h')",message="plannedFailover.maxLagWait must be between 1s and 1h"
	// +optional
	MaxLagWait *metav1.Duration `json:"maxLagWait,omitempty"`

	// DrainTimeout bounds the time the source primary has to shed
	// active application connections after super_read_only=ON is set.
	// During this window the reconciler repeatedly calls
	// KillAppConnections; if any connections remain after the timeout
	// the state machine proceeds anyway so a stuck client cannot block
	// a planned switchover indefinitely. Default: 30s. Bounds: 1s to 10m.
	// +kubebuilder:default="30s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('10m')",message="plannedFailover.drainTimeout must be between 1s and 10m"
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// OnCooldown controls how admission reacts when the anti-flap
	// cooldown is active:
	//   - Reject (default): the run ends Failed{NotStarted, CooldownActive}
	//     and the request annotation is removed. The admin re-annotates
	//     after the cooldown expires.
	//   - Defer: the run waits in Deferred{CooldownActive}, keeps the
	//     annotation in place, and re-tries admission at cooldown expiry.
	// +kubebuilder:default=Reject
	// +optional
	OnCooldown PlannedFailoverCooldownPolicy `json:"onCooldown,omitempty"`
}

// PlannedFailoverCooldownPolicy is spec.plannedFailover.onCooldown.
// +kubebuilder:validation:Enum=Reject;Defer
type PlannedFailoverCooldownPolicy string

const (
	// PlannedFailoverCooldownReject ends a request that meets an active
	// cooldown Failed{NotStarted, CooldownActive}.
	PlannedFailoverCooldownReject PlannedFailoverCooldownPolicy = "Reject"
	// PlannedFailoverCooldownDefer defers it until the cooldown expires.
	PlannedFailoverCooldownDefer PlannedFailoverCooldownPolicy = "Defer"
)

// PlannedFailoverPhase enumerates the discrete states a planned-failover
// run progresses through. Each transition is one durable, conditioned
// status write, so operator restarts always land on a well-defined record.
// +kubebuilder:validation:Enum=Admitting;Deferred;Fencing;Draining;WaitingForLag;WaitingForDragonflySync;PromotingDragonfly;Promoting;Publishing;RollingBack;Succeeded;Failed
type PlannedFailoverPhase string

const (
	// PlannedFailoverPhaseAdmitting means the request was accepted and
	// stamped with a runID, and the run is waiting for the topology
	// manager to grant its claim. The run owns nothing yet; it ends
	// Failed{NotStarted, AdmissionTimeout} if no grant arrives before
	// admission.deadlineTime.
	PlannedFailoverPhaseAdmitting PlannedFailoverPhase = "Admitting"

	// PlannedFailoverPhaseDeferred means admission is deferred: the
	// anti-flap cooldown is active with spec.plannedFailover.onCooldown
	// Defer, or a deployment failover-hold lease stands. The run owns
	// nothing; the annotation remains in place and admission is retried
	// at admission.retryTime.
	PlannedFailoverPhaseDeferred PlannedFailoverPhase = "Deferred"

	// PlannedFailoverPhaseFencing means the claim is granted and the
	// fence intent is durable: the run is setting super_read_only=ON on
	// the source and proving that the fence is its own.
	PlannedFailoverPhaseFencing PlannedFailoverPhase = "Fencing"

	// PlannedFailoverPhaseDraining means the source is fenced and the
	// run is killing application connections on it, bounded by
	// settings.drainTimeout.
	PlannedFailoverPhaseDraining PlannedFailoverPhase = "Draining"

	// PlannedFailoverPhaseWaitingForLag means the run is polling the
	// target until it has applied the fenced set, bounded by
	// settings.maxLagWait.
	PlannedFailoverPhaseWaitingForLag PlannedFailoverPhase = "WaitingForLag"

	// PlannedFailoverPhaseWaitingForDragonflySync means the run is
	// polling the target Dragonfly replica until its replication offset
	// reaches the source's, bounded by settings.dragonfly.maxSyncWait.
	// Skipped when Dragonfly was not enabled at acceptance.
	PlannedFailoverPhaseWaitingForDragonflySync PlannedFailoverPhase = "WaitingForDragonflySync"

	// PlannedFailoverPhasePromotingDragonfly means the run is promoting
	// the target Dragonfly (REPLTAKEOVER) before the MySQL promotion.
	PlannedFailoverPhasePromotingDragonfly PlannedFailoverPhase = "PromotingDragonfly"

	// PlannedFailoverPhasePromoting means the promotion intent is
	// durable and the run is promoting the target on a pinned session.
	PlannedFailoverPhasePromoting PlannedFailoverPhase = "Promoting"

	// PlannedFailoverPhasePublishing means the promotion is confirmed,
	// recorded in the topology manager and published to DNS, and the
	// claim is released. Only status bookkeeping remains.
	PlannedFailoverPhasePublishing PlannedFailoverPhase = "Publishing"

	// PlannedFailoverPhaseRollingBack means the run failed while nothing
	// was written to the target and is unfencing the source under the
	// rollback preconditions.
	PlannedFailoverPhaseRollingBack PlannedFailoverPhase = "RollingBack"

	// PlannedFailoverPhaseSucceeded is the terminal success state.
	PlannedFailoverPhaseSucceeded PlannedFailoverPhase = "Succeeded"

	// PlannedFailoverPhaseFailed is the terminal failure state;
	// result.outcome and result.reason say what happened.
	PlannedFailoverPhaseFailed PlannedFailoverPhase = "Failed"
)

// PlannedFailoverOutcome is the terminal outcome of a planned-failover run,
// recorded in result.outcome.
// +kubebuilder:validation:Enum=Succeeded;NotStarted;RolledBack;SourceLeftFenced;PromotedUnconfirmed;ManualRecoveryRequired
type PlannedFailoverOutcome string

const (
	// PlannedFailoverOutcomeSucceeded: the promotion completed and was
	// confirmed with positive evidence, recorded and published.
	PlannedFailoverOutcomeSucceeded PlannedFailoverOutcome = "Succeeded"
	// PlannedFailoverOutcomeNotStarted: no fence intent was ever durable,
	// so nothing was sent to any site.
	PlannedFailoverOutcomeNotStarted PlannedFailoverOutcome = "NotStarted"
	// PlannedFailoverOutcomeRolledBack: a fence intent was durable,
	// nothing was written to the target, and the source is writable again.
	PlannedFailoverOutcomeRolledBack PlannedFailoverOutcome = "RolledBack"
	// PlannedFailoverOutcomeSourceLeftFenced: nothing was written to the
	// target and the source stays read-only.
	PlannedFailoverOutcomeSourceLeftFenced PlannedFailoverOutcome = "SourceLeftFenced"
	// PlannedFailoverOutcomePromotedUnconfirmed: super_read_only=OFF may
	// have been sent to the target, and the target could not be confirmed
	// or was lost.
	PlannedFailoverOutcomePromotedUnconfirmed PlannedFailoverOutcome = "PromotedUnconfirmed"
	// PlannedFailoverOutcomeManualRecoveryRequired: the run may have
	// written to the target and the result cannot be explained. Automatic
	// cross-site actions stay held until the hold is acknowledged.
	PlannedFailoverOutcomeManualRecoveryRequired PlannedFailoverOutcome = "ManualRecoveryRequired"
)

// PlannedFailoverReason is a machine-readable reason for a run's outcome
// (result.reason) or for a rollback (rollback.reason).
// +kubebuilder:validation:Enum=InvalidAnnotation;UnknownSite;TargetNotPromotable;AlreadyActive;TargetUnhealthy;DivergentTarget;KeyringRotation;CooldownActive;ConcurrentOperation;InvalidFailoverState;Canceled;AdmissionTimeout;AuthorityChanged;LegacyRunRefused;FenceFailed;SourceLost;SourceUnfenced;TargetLost;ForeignWritable;PeerUnreadable;LagTimeout;InvalidGTID;DragonflySyncTimeout;DragonflyPromotionFailed;TargetChanged;PromotionAborted;PromotionUnconfirmed;ManagerUnavailable;StatusStalled;ManualRecoveryPending;ExecuteFailed;InternalError
type PlannedFailoverReason string

const (
	PlannedFailoverReasonInvalidAnnotation        PlannedFailoverReason = "InvalidAnnotation"
	PlannedFailoverReasonUnknownSite              PlannedFailoverReason = "UnknownSite"
	PlannedFailoverReasonTargetNotPromotable      PlannedFailoverReason = "TargetNotPromotable"
	PlannedFailoverReasonAlreadyActive            PlannedFailoverReason = "AlreadyActive"
	PlannedFailoverReasonTargetUnhealthy          PlannedFailoverReason = "TargetUnhealthy"
	PlannedFailoverReasonDivergentTarget          PlannedFailoverReason = "DivergentTarget"
	PlannedFailoverReasonKeyringRotation          PlannedFailoverReason = "KeyringRotation"
	PlannedFailoverReasonCooldownActive           PlannedFailoverReason = "CooldownActive"
	PlannedFailoverReasonConcurrentOperation      PlannedFailoverReason = "ConcurrentOperation"
	PlannedFailoverReasonInvalidFailoverState     PlannedFailoverReason = "InvalidFailoverState"
	PlannedFailoverReasonCanceled                 PlannedFailoverReason = "Canceled"
	PlannedFailoverReasonAdmissionTimeout         PlannedFailoverReason = "AdmissionTimeout"
	PlannedFailoverReasonAuthorityChanged         PlannedFailoverReason = "AuthorityChanged"
	PlannedFailoverReasonLegacyRunRefused         PlannedFailoverReason = "LegacyRunRefused"
	PlannedFailoverReasonFenceFailed              PlannedFailoverReason = "FenceFailed"
	PlannedFailoverReasonSourceLost               PlannedFailoverReason = "SourceLost"
	PlannedFailoverReasonSourceUnfenced           PlannedFailoverReason = "SourceUnfenced"
	PlannedFailoverReasonTargetLost               PlannedFailoverReason = "TargetLost"
	PlannedFailoverReasonForeignWritable          PlannedFailoverReason = "ForeignWritable"
	PlannedFailoverReasonPeerUnreadable           PlannedFailoverReason = "PeerUnreadable"
	PlannedFailoverReasonLagTimeout               PlannedFailoverReason = "LagTimeout"
	PlannedFailoverReasonInvalidGTID              PlannedFailoverReason = "InvalidGTID"
	PlannedFailoverReasonDragonflySyncTimeout     PlannedFailoverReason = "DragonflySyncTimeout"
	PlannedFailoverReasonDragonflyPromotionFailed PlannedFailoverReason = "DragonflyPromotionFailed"
	PlannedFailoverReasonTargetChanged            PlannedFailoverReason = "TargetChanged"
	PlannedFailoverReasonPromotionAborted         PlannedFailoverReason = "PromotionAborted"
	PlannedFailoverReasonPromotionUnconfirmed     PlannedFailoverReason = "PromotionUnconfirmed"
	PlannedFailoverReasonManagerUnavailable       PlannedFailoverReason = "ManagerUnavailable"
	PlannedFailoverReasonStatusStalled            PlannedFailoverReason = "StatusStalled"
	PlannedFailoverReasonManualRecoveryPending    PlannedFailoverReason = "ManualRecoveryPending"
	PlannedFailoverReasonExecuteFailed            PlannedFailoverReason = "ExecuteFailed"
	PlannedFailoverReasonInternalError            PlannedFailoverReason = "InternalError"
)

// PlannedFailoverDeferReason says why a run is Deferred (admission.deferReason).
// It is a separate type rather than a field-level subset of
// PlannedFailoverReason, which would make controller-gen emit conflicting
// allOf enums.
// +kubebuilder:validation:Enum=CooldownActive;DeploymentHold
type PlannedFailoverDeferReason string

const (
	PlannedFailoverDeferCooldownActive PlannedFailoverDeferReason = "CooldownActive"
	PlannedFailoverDeferDeploymentHold PlannedFailoverDeferReason = "DeploymentHold"
)

// PlannedFailoverWaitReason says why an Admitting run has no claim yet
// (admission.waitReason).
// +kubebuilder:validation:Enum=ManagerAbsent;ManagerNotReady;ManagerRetiring;PredecessorBusy;AuthorityUnsettled;PeerUnreadable;TargetNotReady;PromotionInFlight;UpdateInFlight;RecoveryInFlight;BootstrapInFlight;RestoreInFlight;KeyringRotation;DragonflyUpgradeInFlight;ClaimHeld;StatusUnsettled
type PlannedFailoverWaitReason string

const (
	PlannedFailoverWaitManagerAbsent            PlannedFailoverWaitReason = "ManagerAbsent"
	PlannedFailoverWaitManagerNotReady          PlannedFailoverWaitReason = "ManagerNotReady"
	PlannedFailoverWaitManagerRetiring          PlannedFailoverWaitReason = "ManagerRetiring"
	PlannedFailoverWaitPredecessorBusy          PlannedFailoverWaitReason = "PredecessorBusy"
	PlannedFailoverWaitAuthorityUnsettled       PlannedFailoverWaitReason = "AuthorityUnsettled"
	PlannedFailoverWaitPeerUnreadable           PlannedFailoverWaitReason = "PeerUnreadable"
	PlannedFailoverWaitTargetNotReady           PlannedFailoverWaitReason = "TargetNotReady"
	PlannedFailoverWaitPromotionInFlight        PlannedFailoverWaitReason = "PromotionInFlight"
	PlannedFailoverWaitUpdateInFlight           PlannedFailoverWaitReason = "UpdateInFlight"
	PlannedFailoverWaitRecoveryInFlight         PlannedFailoverWaitReason = "RecoveryInFlight"
	PlannedFailoverWaitBootstrapInFlight        PlannedFailoverWaitReason = "BootstrapInFlight"
	PlannedFailoverWaitRestoreInFlight          PlannedFailoverWaitReason = "RestoreInFlight"
	PlannedFailoverWaitKeyringRotation          PlannedFailoverWaitReason = "KeyringRotation"
	PlannedFailoverWaitDragonflyUpgradeInFlight PlannedFailoverWaitReason = "DragonflyUpgradeInFlight"
	PlannedFailoverWaitClaimHeld                PlannedFailoverWaitReason = "ClaimHeld"
	PlannedFailoverWaitStatusUnsettled          PlannedFailoverWaitReason = "StatusUnsettled"
)

// PlannedFailoverWarning qualifies a terminal outcome (result.warnings).
// +kubebuilder:validation:Enum=DNSUpdateFailed;DragonflySessionsLost;TargetReplicationNotRestored;UnreachablePeers;FenceNotAttributable;StatusStalled;SourceReplaced;TargetSuperReadOnlyNotRestored
type PlannedFailoverWarning string

const (
	PlannedFailoverWarningDNSUpdateFailed              PlannedFailoverWarning = "DNSUpdateFailed"
	PlannedFailoverWarningDragonflySessionsLost        PlannedFailoverWarning = "DragonflySessionsLost"
	PlannedFailoverWarningTargetReplicationNotRestored PlannedFailoverWarning = "TargetReplicationNotRestored"
	PlannedFailoverWarningUnreachablePeers             PlannedFailoverWarning = "UnreachablePeers"
	PlannedFailoverWarningFenceNotAttributable         PlannedFailoverWarning = "FenceNotAttributable"
	PlannedFailoverWarningStatusStalled                PlannedFailoverWarning = "StatusStalled"
	PlannedFailoverWarningSourceReplaced               PlannedFailoverWarning = "SourceReplaced"
	// TargetSuperReadOnlyNotRestored: the rollback ended with the source
	// writable but could not put the target back behind super_read_only
	// (it still reads read_only=ON, super_read_only=OFF and would accept
	// writes from SUPER or CONNECTION_ADMIN sessions); its replication was
	// not restored either.
	PlannedFailoverWarningTargetSuperReadOnlyNotRestored PlannedFailoverWarning = "TargetSuperReadOnlyNotRestored"
)

// PlannedFailoverFenceState is the state of the run's fence on the source.
// +kubebuilder:validation:Enum=Intended;Applied
type PlannedFailoverFenceState string

const (
	PlannedFailoverFenceIntended PlannedFailoverFenceState = "Intended"
	PlannedFailoverFenceApplied  PlannedFailoverFenceState = "Applied"
)

// PlannedFailoverPromotionState is the state of the target's promotion.
// +kubebuilder:validation:Enum=Intended;Confirmed
type PlannedFailoverPromotionState string

const (
	PlannedFailoverPromotionIntended  PlannedFailoverPromotionState = "Intended"
	PlannedFailoverPromotionConfirmed PlannedFailoverPromotionState = "Confirmed"
)

// PlannedFailoverDNSState is the result of publishing the promoted target
// to DNS.
// +kubebuilder:validation:Enum=Applied;Failed;Skipped
type PlannedFailoverDNSState string

const (
	PlannedFailoverDNSApplied PlannedFailoverDNSState = "Applied"
	PlannedFailoverDNSFailed  PlannedFailoverDNSState = "Failed"
	PlannedFailoverDNSSkipped PlannedFailoverDNSState = "Skipped"
)

// PlannedFailoverStatus is the record of the most recent planned-failover
// run. Every field belongs to the run named by runID. TargetSite and
// Settings are the run's parameters, copied from the consumed annotation and the
// spec at acceptance (the annotation is removed, so they are recorded here
// rather than re-read). The operator writes this block conditioned on the
// object's resourceVersion and refuses a non-terminal block without a
// runID (a block written by an older operator).
//
// RunID, Revision, Phase, TargetSite, Settings and StartTime are always
// written by this operator (every run block is minted whole), but the
// schema does not require them: an operator rolled back across this release
// (a Helm rollback leaves the CRD in place) decodes the block into its older
// struct, drops these fields and writes the whole status back; requiring
// them would fail every one of its status writes. The operator enforces
// presence instead: a non-terminal block without a runID is refused as a
// legacy run. The fields use omitempty/omitzero so that a block written
// by an older operator re-serializes byte for byte.
type PlannedFailoverStatus struct {
	// RunID identifies the run this block belongs to. It is minted when a
	// request is first stamped (Admitting or Deferred) and is the token
	// of the run's claim. Always set by this operator; absent only on a
	// block an older operator wrote.
	// +optional
	// +kubebuilder:validation:MinLength=1
	RunID string `json:"runID,omitempty"`

	// Revision increments on every write the run makes. Settlement of an
	// ambiguous write identifies a committed write by (runID, revision).
	// Always set with runID.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Revision int64 `json:"revision,omitempty"`

	// Phase of the run. Always set by this operator.
	// +optional
	Phase PlannedFailoverPhase `json:"phase,omitempty"`

	// TargetSite is the site the admin asked to promote. Always set with
	// runID.
	// +optional
	// +kubebuilder:validation:MinLength=1
	TargetSite string `json:"targetSite,omitempty"`

	// Settings are the run's effective timing settings, snapshotted at
	// acceptance from the annotation overrides and the spec. Always set
	// with runID.
	// +optional
	Settings PlannedFailoverSettings `json:"settings,omitzero"`

	// StartTime is when the request was first accepted. Always set by
	// this operator.
	// +optional
	StartTime metav1.Time `json:"startTime,omitzero"`

	// SourceSite is the site the topology manager named the unique writable
	// primary when it granted the claim. Empty until Fencing.
	// +optional
	SourceSite string `json:"sourceSite,omitempty"`

	// ClaimTime is when the claim was granted.
	// +optional
	ClaimTime *metav1.Time `json:"claimTime,omitempty"`

	// ClaimBaseline identifies the durable failover record at the grant
	// (the newer of status and the out-of-band record): its time, at
	// second precision, and its target. A re-proof compares those two by
	// value.
	// +optional
	ClaimBaseline *FailoverRecordRef `json:"claimBaseline,omitempty"`

	// Admission records why a run that owns nothing yet is waiting or
	// deferred, and its admission deadline.
	// +optional
	Admission *PlannedFailoverAdmission `json:"admission,omitempty"`

	// Fence is the write-ahead record of the run's fence on the source.
	// +optional
	Fence *PlannedFailoverFenceRecord `json:"fence,omitempty"`

	// Drain records the source connection drain.
	// +optional
	Drain *PlannedFailoverDrainRecord `json:"drain,omitempty"`

	// LagWait records the wait for the target to apply the fenced set.
	// +optional
	LagWait *PlannedFailoverLagWaitRecord `json:"lagWait,omitempty"`

	// Dragonfly tracks the Dragonfly side of the run. Populated only when
	// Dragonfly was enabled at acceptance.
	// +optional
	Dragonfly *PlannedFailoverDragonflyStatus `json:"dragonfly,omitempty"`

	// Promotion is the write-ahead record of the target's promotion.
	// +optional
	Promotion *PlannedFailoverPromotionRecord `json:"promotion,omitempty"`

	// DNS records publishing the promoted target.
	// +optional
	DNS *PlannedFailoverDNSRecord `json:"dns,omitempty"`

	// Rollback is the write-ahead record of a rollback.
	// +optional
	Rollback *PlannedFailoverRollbackRecord `json:"rollback,omitempty"`

	// Result is set when the run is terminal.
	// +optional
	Result *PlannedFailoverResult `json:"result,omitempty"`

	// Message is a human-readable status line suitable for kubectl
	// describe.
	// +optional
	Message string `json:"message,omitempty"`
}

// FailoverRecordRef identifies a failover record by {time, target}: when the
// last failover was recorded (at second precision, the precision of both
// durable copies) and the site it promoted. It is what a planned run's
// claimBaseline needs; status.lastFailover (FailoverRecord) embeds it and
// adds the promotion watermark, which is not part of the reference. Every
// field is optional because a group may have no failover history.
type FailoverRecordRef struct {
	// Time is when the failover was recorded.
	// +optional
	Time *metav1.Time `json:"time,omitempty"`
	// Target is the site the failover promoted.
	// +optional
	Target string `json:"target,omitempty"`
}

// PlannedFailoverSettings are a run's effective timing settings.
type PlannedFailoverSettings struct {
	// MaxLagWait bounds WaitingForLag.
	// +required
	MaxLagWait metav1.Duration `json:"maxLagWait"`
	// DrainTimeout bounds Draining.
	// +required
	DrainTimeout metav1.Duration `json:"drainTimeout"`
	// Dragonfly is set when spec.dragonfly was enabled at acceptance.
	// +optional
	Dragonfly *PlannedFailoverDragonflySettings `json:"dragonfly,omitempty"`
}

// PlannedFailoverDragonflySettings are a run's Dragonfly settings.
type PlannedFailoverDragonflySettings struct {
	// MaxSyncWait bounds WaitingForDragonflySync.
	// +required
	MaxSyncWait metav1.Duration `json:"maxSyncWait"`
	// OnSyncTimeout is spec.dragonfly.plannedFailover.onSyncTimeout at
	// acceptance.
	// +required
	OnSyncTimeout DragonflySyncTimeoutPolicy `json:"onSyncTimeout"`
}

// PlannedFailoverAdmission records an admission that owns nothing yet.
type PlannedFailoverAdmission struct {
	// DeadlineTime is when an Admitting run that still has no claim ends
	// Failed{NotStarted, AdmissionTimeout}.
	// +optional
	DeadlineTime *metav1.Time `json:"deadlineTime,omitempty"`
	// WaitReason is the latest reason the claim was not granted.
	// +optional
	WaitReason PlannedFailoverWaitReason `json:"waitReason,omitempty"`
	// DeferReason is why the run is Deferred.
	// +optional
	DeferReason PlannedFailoverDeferReason `json:"deferReason,omitempty"`
	// RetryTime is the earliest time a Deferred run re-tries admission.
	// +optional
	RetryTime *metav1.Time `json:"retryTime,omitempty"`
}

// PlannedFailoverFenceRecord is the write-ahead record of the run's fence.
type PlannedFailoverFenceRecord struct {
	// +required
	State PlannedFailoverFenceState `json:"state"`
	// +required
	IntentTime metav1.Time `json:"intentTime"`
	// SourceServerUUID is the source's @@server_uuid recorded by the grant's
	// evidence probe and written with the intent. The fence is sent only to a
	// server with this uuid, and a cross-process re-run attributes
	// super_read_only=ON to this run only when the uuid still matches.
	// +kubebuilder:validation:MinLength=1
	// +required
	SourceServerUUID string `json:"sourceServerUUID"`
	// ApplyTime is set iff state is Applied.
	// +optional
	ApplyTime *metav1.Time `json:"applyTime,omitempty"`
	// GtidSet is the fenced source set ("" is a valid empty set). It is also
	// the promotion watermark.
	// +optional
	GtidSet *string `json:"gtidSet,omitempty"`
}

// PlannedFailoverDrainRecord records the source connection drain.
type PlannedFailoverDrainRecord struct {
	// +required
	StartTime metav1.Time `json:"startTime"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// +optional
	RemainingConnections *int32 `json:"remainingConnections,omitempty"`
}

// PlannedFailoverLagWaitRecord records the wait for the target to apply
// the fenced set.
type PlannedFailoverLagWaitRecord struct {
	// +required
	StartTime metav1.Time `json:"startTime"`
	// +optional
	CatchUpTime *metav1.Time `json:"catchUpTime,omitempty"`
}

// PlannedFailoverPromotionRecord is the write-ahead record of the target's
// promotion.
type PlannedFailoverPromotionRecord struct {
	// +required
	State PlannedFailoverPromotionState `json:"state"`
	// +required
	IntentTime metav1.Time `json:"intentTime"`
	// TargetServerUUID is the target's @@server_uuid read on a pinned
	// session before the first promotion statement.
	// +kubebuilder:validation:MinLength=1
	// +required
	TargetServerUUID string `json:"targetServerUUID"`
	// +optional
	ConfirmationTime *metav1.Time `json:"confirmationTime,omitempty"`
	// Warnings are the warnings the run had collected when its promotion
	// was confirmed. The Publishing record carries them so that the
	// terminal record keeps them when another process writes it.
	// +optional
	// +listType=set
	Warnings []PlannedFailoverWarning `json:"warnings,omitempty"`
}

// PlannedFailoverDNSRecord records publishing the promoted target.
type PlannedFailoverDNSRecord struct {
	// +required
	State PlannedFailoverDNSState `json:"state"`
	// +required
	AttemptTime metav1.Time `json:"attemptTime"`
	// +optional
	LastError string `json:"lastError,omitempty"`
}

// PlannedFailoverRollbackRecord is the write-ahead record of a rollback.
type PlannedFailoverRollbackRecord struct {
	// +required
	Reason PlannedFailoverReason `json:"reason"`
	// +required
	IntentTime metav1.Time `json:"intentTime"`
}

// PlannedFailoverResult is a run's terminal result.
type PlannedFailoverResult struct {
	// +required
	Outcome PlannedFailoverOutcome `json:"outcome"`
	// +optional
	Reason PlannedFailoverReason `json:"reason,omitempty"`
	// +required
	CompletionTime metav1.Time `json:"completionTime"`
	// +optional
	// +listType=set
	Warnings []PlannedFailoverWarning `json:"warnings,omitempty"`
}

// PlannedFailoverHold is status.plannedFailoverHold: the automation hold of
// a planned-failover run that ended ManualRecoveryRequired. The operator
// writes it with that run's terminal record and removes it, with the
// AutomaticFailoverHeld condition set False, when the hold is acknowledged.
type PlannedFailoverHold struct {
	// RunID is the held run; the acknowledgement names it.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	RunID string `json:"runID"`

	// Reason is the run's result.reason.
	// +optional
	Reason PlannedFailoverReason `json:"reason,omitempty"`

	// SourceSite is the run's sourceSite (empty if the run never fenced).
	// +optional
	SourceSite string `json:"sourceSite,omitempty"`

	// TargetSite is the run's targetSite.
	// +optional
	TargetSite string `json:"targetSite,omitempty"`

	// AmbiguousSites are the sites that were writable, or whose
	// writability could not be read, when the run ended.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=16
	AmbiguousSites []string `json:"ambiguousSites,omitempty"`

	// HoldTime is when the hold was recorded.
	// +kubebuilder:validation:Required
	HoldTime metav1.Time `json:"holdTime"`
}

// DragonflyOffsetState says whether the source Dragonfly's replication
// offset was captured for the sync wait.
// +kubebuilder:validation:Enum=Captured;Unavailable
type DragonflyOffsetState string

const (
	DragonflyOffsetCaptured    DragonflyOffsetState = "Captured"
	DragonflyOffsetUnavailable DragonflyOffsetState = "Unavailable"
)

// DragonflyTakeoverState is the state of the target Dragonfly's takeover.
// +kubebuilder:validation:Enum=Intended;Succeeded;Failed
type DragonflyTakeoverState string

const (
	DragonflyTakeoverIntended  DragonflyTakeoverState = "Intended"
	DragonflyTakeoverSucceeded DragonflyTakeoverState = "Succeeded"
	DragonflyTakeoverFailed    DragonflyTakeoverState = "Failed"
)

// DragonflyTakeoverMethod is how the target Dragonfly was promoted. A
// planned run writes only ReplTakeover today; ReplicaOfNoOne (the
// emergency path's fallback) and Skipped are the rest of the closed set.
// +kubebuilder:validation:Enum=ReplTakeover;ReplicaOfNoOne;Skipped
type DragonflyTakeoverMethod string

const (
	// DragonflyTakeoverMethodReplTakeover: REPLTAKEOVER on the target.
	DragonflyTakeoverMethodReplTakeover DragonflyTakeoverMethod = "ReplTakeover"
	// DragonflyTakeoverMethodReplicaOfNoOne: REPLICAOF NO ONE on the target.
	DragonflyTakeoverMethodReplicaOfNoOne DragonflyTakeoverMethod = "ReplicaOfNoOne"
	// DragonflyTakeoverMethodSkipped: no takeover was attempted.
	DragonflyTakeoverMethodSkipped DragonflyTakeoverMethod = "Skipped"
)

// DragonflySessions says whether Dragonfly sessions survived the run.
// +kubebuilder:validation:Enum=Preserved;Lost
type DragonflySessions string

const (
	DragonflySessionsPreserved DragonflySessions = "Preserved"
	DragonflySessionsLost      DragonflySessions = "Lost"
)

// DragonflyPlannedReason is a machine-readable reason for a non-success
// Dragonfly outcome of a planned failover.
// +kubebuilder:validation:Enum=DragonflySyncTimeout;DragonflyPromotionFailed
type DragonflyPlannedReason string

const (
	DragonflyPlannedReasonSyncTimeout     DragonflyPlannedReason = "DragonflySyncTimeout"
	DragonflyPlannedReasonPromotionFailed DragonflyPlannedReason = "DragonflyPromotionFailed"
)

// DragonflyTakeoverRecord is the write-ahead record of the target
// Dragonfly's takeover.
type DragonflyTakeoverRecord struct {
	// +required
	State DragonflyTakeoverState `json:"state"`
	// +required
	IntentTime metav1.Time `json:"intentTime"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// +optional
	Method DragonflyTakeoverMethod `json:"method,omitempty"`
	// +optional
	TargetOffset *int64 `json:"targetOffset,omitempty"`
}

// PlannedFailoverDragonflyStatus records the Dragonfly side of a planned
// failover run.
type PlannedFailoverDragonflyStatus struct {
	// SyncWaitStartTime is when the run entered WaitingForDragonflySync.
	// The maxSyncWait budget is measured from it, independently of the
	// MySQL lag wait.
	// +optional
	SyncWaitStartTime *metav1.Time `json:"syncWaitStartTime,omitempty"`

	// SourceOffsetState says whether SourceOffset was captured.
	// +optional
	SourceOffsetState DragonflyOffsetState `json:"sourceOffsetState,omitempty"`

	// SourceOffset is the source Dragonfly's master replication offset,
	// the catch-up target of the sync wait. Set iff sourceOffsetState is
	// Captured.
	// +optional
	SourceOffset *int64 `json:"sourceOffset,omitempty"`

	// SyncWaitSeconds is the time spent in WaitingForDragonflySync.
	// +optional
	SyncWaitSeconds *int64 `json:"syncWaitSeconds,omitempty"`

	// Takeover is the write-ahead record of the target Dragonfly's
	// promotion.
	// +optional
	Takeover *DragonflyTakeoverRecord `json:"takeover,omitempty"`

	// Sessions says whether Dragonfly sessions survived the run.
	// +optional
	Sessions DragonflySessions `json:"sessions,omitempty"`

	// Reason explains a non-success Dragonfly outcome.
	// +optional
	Reason DragonflyPlannedReason `json:"reason,omitempty"`

	// Message is a human-readable line for kubectl describe.
	// +optional
	Message string `json:"message,omitempty"`
}
