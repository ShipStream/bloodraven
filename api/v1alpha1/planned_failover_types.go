package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlannedFailoverSpec configures admin-triggered graceful switchover.
// All fields are optional; an omitted block is equivalent to the defaults
// documented on each field.
type PlannedFailoverSpec struct {
	// MaxLagWait bounds the time spent waiting for the target's
	// GTID_EXECUTED to cover the fenced source's GTID_EXECUTED before
	// the state machine rolls back to the source. Must be expressible
	// as a Go time.Duration. Default: 5m.
	// +kubebuilder:default="5m"
	// +optional
	MaxLagWait *metav1.Duration `json:"maxLagWait,omitempty"`

	// DrainTimeout bounds the time the source primary has to shed
	// active application connections after super_read_only=ON is set.
	// During this window the reconciler repeatedly calls
	// KillAppConnections; if any connections remain after the timeout
	// the state machine proceeds anyway so a stuck client cannot block
	// a planned switchover indefinitely. Default: 30s.
	// +kubebuilder:default="30s"
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// OnCooldown controls how the state machine reacts when the
	// anti-flap cooldown rejects a request at Validating:
	//   - "reject" (default): stamp Failed{CooldownActive} and clear the
	//     annotation. The admin must re-annotate after the cooldown
	//     expires.
	//   - "defer": stamp Deferred and keep the annotation in place;
	//     the reconciler re-tries validation at cooldown expiry and
	//     transitions to Draining automatically.
	// +kubebuilder:default="reject"
	// +kubebuilder:validation:Enum=reject;defer
	// +optional
	OnCooldown string `json:"onCooldown,omitempty"`
}

// PlannedFailoverPhase enumerates the discrete states a planned-failover
// run progresses through. Each transition is one durable, conditioned
// status write, so operator restarts always land on a well-defined record.
// +kubebuilder:validation:Enum=Admitting;Deferred;Fencing;Draining;WaitingForLag;WaitingForDragonflySync;PromotingDragonfly;Promoting;Publishing;RollingBack;Succeeded;Failed
type PlannedFailoverPhase string

const (
	// PlannedFailoverPhaseAdmitting means the request was accepted and
	// stamped with a runId, and the run is waiting for the topology
	// manager to grant its claim. The run owns nothing yet; it ends
	// Failed{NotStarted, AdmissionTimeout} if no grant arrives before
	// admission.deadline.
	PlannedFailoverPhaseAdmitting PlannedFailoverPhase = "Admitting"

	// PlannedFailoverPhaseDeferred means admission is deferred: the
	// anti-flap cooldown is active with spec.plannedFailover.onCooldown
	// "defer", or a deployment failover-hold lease stands. The run owns
	// nothing; the annotation remains in place and admission is retried
	// at admission.retryAfter.
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
// +kubebuilder:validation:Enum=InvalidAnnotation;UnknownSite;TargetNotPromotable;AlreadyActive;TargetUnhealthy;DivergentTarget;KeyringRotation;CooldownActive;ConcurrentOperation;InvalidFailoverState;Cancelled;AdmissionTimeout;AuthorityChanged;LegacyRunRefused;FenceFailed;SourceLost;SourceUnfenced;TargetLost;ForeignWritable;PeerUnreadable;LagTimeout;InvalidGTID;DragonflySyncTimeout;DragonflyPromotionFailed;TargetChanged;PromotionAborted;PromotionUnconfirmed;ManagerUnavailable;StatusStalled;ManualRecoveryPending;ExecuteFailed;InternalError
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
	PlannedFailoverReasonCancelled                PlannedFailoverReason = "Cancelled"
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
// +kubebuilder:validation:Enum=DNSUpdateFailed;DragonflySessionsLost;TargetReplicationNotRestored;UnreachablePeers;FenceNotAttributable;StatusStalled;SourceReplaced
type PlannedFailoverWarning string

const (
	PlannedFailoverWarningDNSUpdateFailed              PlannedFailoverWarning = "DNSUpdateFailed"
	PlannedFailoverWarningDragonflySessionsLost        PlannedFailoverWarning = "DragonflySessionsLost"
	PlannedFailoverWarningTargetReplicationNotRestored PlannedFailoverWarning = "TargetReplicationNotRestored"
	PlannedFailoverWarningUnreachablePeers             PlannedFailoverWarning = "UnreachablePeers"
	PlannedFailoverWarningFenceNotAttributable         PlannedFailoverWarning = "FenceNotAttributable"
	PlannedFailoverWarningStatusStalled                PlannedFailoverWarning = "StatusStalled"
	PlannedFailoverWarningSourceReplaced               PlannedFailoverWarning = "SourceReplaced"
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
// run. Every field belongs to the run named by runId. Target and Settings
// are the run's parameters, copied from the consumed annotation and the
// spec at acceptance (the annotation is removed, so they are recorded here
// rather than re-read). The operator writes this block conditioned on the
// object's resourceVersion and refuses a non-terminal block without a
// runId (a block written by an older operator).
//
// The required fields use omitempty/omitzero so that an unchanged block
// written by an older operator re-serializes byte for byte and stays
// writable under CRD validation ratcheting until the operator refuses it.
type PlannedFailoverStatus struct {
	// RunID identifies the run this block belongs to. It is minted when a
	// request is first stamped (Admitting or Deferred) and is the token
	// of the run's claim.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	RunID string `json:"runId,omitempty"`

	// Revision increments on every write the run makes. Settlement of an
	// ambiguous write identifies a committed write by (runId, revision).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Revision int64 `json:"revision,omitempty"`

	// Phase of the run.
	// +kubebuilder:validation:Required
	Phase PlannedFailoverPhase `json:"phase,omitempty"`

	// Target is the site the admin asked to promote.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Target string `json:"target,omitempty"`

	// Settings are the run's effective timing settings, snapshotted at
	// acceptance from the annotation overrides and the spec.
	// +kubebuilder:validation:Required
	Settings PlannedFailoverSettings `json:"settings,omitzero"`

	// StartTime is when the request was first accepted.
	// +kubebuilder:validation:Required
	StartTime metav1.Time `json:"startTime,omitzero"`

	// Source is the site the topology manager named the unique writable
	// primary when it granted the claim. Empty until Fencing.
	// +optional
	Source string `json:"source,omitempty"`

	// ClaimedAt is when the claim was granted.
	// +optional
	ClaimedAt *metav1.Time `json:"claimedAt,omitempty"`

	// ClaimBaseline is the durable failover record {lastFailover,
	// lastFailoverTarget} at the grant (the newer of status and the
	// out-of-band record), at second precision. A re-proof compares it by
	// value.
	// +optional
	ClaimBaseline *PlannedFailoverRecordRef `json:"claimBaseline,omitempty"`

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

// PlannedFailoverRecordRef is a value copy of the failover record
// {lastFailover, lastFailoverTarget}, stored at second precision (the
// precision of both durable copies). Both are optional because a group may
// have no failover history.
type PlannedFailoverRecordRef struct {
	// +optional
	LastFailover *metav1.Time `json:"lastFailover,omitempty"`
	// +optional
	LastFailoverTarget string `json:"lastFailoverTarget,omitempty"`
}

// PlannedFailoverSettings are a run's effective timing settings.
type PlannedFailoverSettings struct {
	// MaxLagWait bounds WaitingForLag.
	MaxLagWait metav1.Duration `json:"maxLagWait"`
	// DrainTimeout bounds Draining.
	DrainTimeout metav1.Duration `json:"drainTimeout"`
	// Dragonfly is set when spec.dragonfly was enabled at acceptance.
	// +optional
	Dragonfly *PlannedFailoverDragonflySettings `json:"dragonfly,omitempty"`
}

// PlannedFailoverDragonflySettings are a run's Dragonfly settings.
type PlannedFailoverDragonflySettings struct {
	// MaxSyncWait bounds WaitingForDragonflySync.
	MaxSyncWait metav1.Duration `json:"maxSyncWait"`
	// OnSyncTimeout mirrors spec.dragonfly.plannedFailover.onSyncTimeout
	// (lower case like spec.plannedFailover.onCooldown).
	// +kubebuilder:validation:Enum=proceed;fail
	OnSyncTimeout string `json:"onSyncTimeout"`
}

// PlannedFailoverAdmission records an admission that owns nothing yet.
type PlannedFailoverAdmission struct {
	// Deadline is when an Admitting run that still has no claim ends
	// Failed{NotStarted, AdmissionTimeout}.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`
	// WaitReason is the latest reason the claim was not granted.
	// +optional
	WaitReason PlannedFailoverWaitReason `json:"waitReason,omitempty"`
	// DeferReason is why the run is Deferred.
	// +optional
	DeferReason PlannedFailoverDeferReason `json:"deferReason,omitempty"`
	// RetryAfter is the earliest time a Deferred run re-tries admission.
	// +optional
	RetryAfter *metav1.Time `json:"retryAfter,omitempty"`
}

// PlannedFailoverFenceRecord is the write-ahead record of the run's fence.
type PlannedFailoverFenceRecord struct {
	State      PlannedFailoverFenceState `json:"state"`
	IntendedAt metav1.Time               `json:"intendedAt"`
	// SourceServerUUID is the source's @@server_uuid recorded by the grant's
	// evidence probe and written with the intent. The fence is sent only to a
	// server with this uuid, and a cross-process re-run attributes
	// super_read_only=ON to this run only when the uuid still matches.
	// +kubebuilder:validation:MinLength=1
	SourceServerUUID string `json:"sourceServerUUID"`
	// AppliedAt is set iff state is Applied.
	// +optional
	AppliedAt *metav1.Time `json:"appliedAt,omitempty"`
	// GtidSet is the fenced source set ("" is a valid empty set). It is also
	// the promotion watermark.
	// +optional
	GtidSet *string `json:"gtidSet,omitempty"`
}

// PlannedFailoverDrainRecord records the source connection drain.
type PlannedFailoverDrainRecord struct {
	StartTime metav1.Time `json:"startTime"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// +optional
	RemainingConnections *int32 `json:"remainingConnections,omitempty"`
}

// PlannedFailoverLagWaitRecord records the wait for the target to apply
// the fenced set.
type PlannedFailoverLagWaitRecord struct {
	StartTime metav1.Time `json:"startTime"`
	// +optional
	CaughtUpAt *metav1.Time `json:"caughtUpAt,omitempty"`
}

// PlannedFailoverPromotionRecord is the write-ahead record of the target's
// promotion.
type PlannedFailoverPromotionRecord struct {
	State      PlannedFailoverPromotionState `json:"state"`
	IntendedAt metav1.Time                   `json:"intendedAt"`
	// TargetServerUUID is the target's @@server_uuid read on a pinned
	// session before the first promotion statement.
	// +kubebuilder:validation:MinLength=1
	TargetServerUUID string `json:"targetServerUUID"`
	// +optional
	ConfirmedAt *metav1.Time `json:"confirmedAt,omitempty"`
	// Warnings are the warnings the run had collected when its promotion
	// was confirmed. The Publishing record carries them so that the
	// terminal record keeps them when another process writes it.
	// +optional
	// +listType=set
	Warnings []PlannedFailoverWarning `json:"warnings,omitempty"`
}

// PlannedFailoverDNSRecord records publishing the promoted target.
type PlannedFailoverDNSRecord struct {
	State       PlannedFailoverDNSState `json:"state"`
	AttemptedAt metav1.Time             `json:"attemptedAt"`
	// +optional
	LastError string `json:"lastError,omitempty"`
}

// PlannedFailoverRollbackRecord is the write-ahead record of a rollback.
type PlannedFailoverRollbackRecord struct {
	Reason     PlannedFailoverReason `json:"reason"`
	IntendedAt metav1.Time           `json:"intendedAt"`
}

// PlannedFailoverResult is a run's terminal result.
type PlannedFailoverResult struct {
	Outcome PlannedFailoverOutcome `json:"outcome"`
	// +optional
	Reason         PlannedFailoverReason `json:"reason,omitempty"`
	CompletionTime metav1.Time           `json:"completionTime"`
	// +optional
	// +listType=set
	Warnings []PlannedFailoverWarning `json:"warnings,omitempty"`
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

// DragonflyTakeoverMethod is how the target Dragonfly was promoted.
// +kubebuilder:validation:Enum=REPLTAKEOVER;Skipped
type DragonflyTakeoverMethod string

const (
	DragonflyTakeoverMethodReplTakeover DragonflyTakeoverMethod = "REPLTAKEOVER"
	DragonflyTakeoverMethodSkipped      DragonflyTakeoverMethod = "Skipped"
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
	State      DragonflyTakeoverState `json:"state"`
	IntendedAt metav1.Time            `json:"intendedAt"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
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
