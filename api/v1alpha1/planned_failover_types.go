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

// PlannedFailoverPhase enumerates the discrete states a planned failover
// progresses through. The reconciler advances the state machine by one
// step per reconcile so that operator restarts land on a well-defined
// observable state.
//
// The enum carries both the legacy phases ("", Pending, Validating,
// Resuming) and the durable-claim phases (Admitting, Fencing, Publishing,
// RollingBack) while the claim lands; the legacy values are removed once
// nothing writes them.
// +kubebuilder:validation:Enum="";Pending;Admitting;Deferred;Validating;Fencing;Draining;WaitingForLag;WaitingForDragonflySync;PromotingDragonfly;Promoting;Publishing;Resuming;RollingBack;Succeeded;Failed
type PlannedFailoverPhase string

const (
	// PlannedFailoverPhaseNone is the zero value.
	PlannedFailoverPhaseNone PlannedFailoverPhase = ""

	// PlannedFailoverPhasePending means the annotation has been observed
	// and the reconciler is about to validate preconditions.
	PlannedFailoverPhasePending PlannedFailoverPhase = "Pending"

	// PlannedFailoverPhaseDeferred means validation rejected the request
	// because the anti-flap cooldown is still active AND
	// spec.plannedFailover.onCooldown="defer". The annotation remains
	// in place; the reconciler re-tries validation at cooldown expiry.
	PlannedFailoverPhaseDeferred PlannedFailoverPhase = "Deferred"

	// PlannedFailoverPhaseValidating means the reconciler is checking
	// preconditions (unknown site, role, cooldown, in-flight restore,
	// etc.).
	PlannedFailoverPhaseValidating PlannedFailoverPhase = "Validating"

	// PlannedFailoverPhaseDraining means super_read_only=ON has been set
	// on the source and the primary role label has been stripped.
	PlannedFailoverPhaseDraining PlannedFailoverPhase = "Draining"

	// PlannedFailoverPhaseWaitingForLag means the reconciler is polling
	// the target's GTID_EXECUTED until it covers the source's fenced
	// GTID_EXECUTED, bounded by spec.plannedFailover.maxLagWait.
	PlannedFailoverPhaseWaitingForLag PlannedFailoverPhase = "WaitingForLag"

	// PlannedFailoverPhaseWaitingForDragonflySync means the reconciler
	// is polling the target Dragonfly replica until its replication
	// offset reaches the source's offset captured at drain time, bounded
	// by spec.dragonfly.plannedFailover.maxSyncWait. Skipped when
	// spec.dragonfly is unset or disabled.
	PlannedFailoverPhaseWaitingForDragonflySync PlannedFailoverPhase = "WaitingForDragonflySync"

	// PlannedFailoverPhasePromotingDragonfly means the reconciler is
	// issuing REPLTAKEOVER against the target Dragonfly to promote it
	// before MySQL promotion runs. Skipped when spec.dragonfly is unset
	// or disabled.
	PlannedFailoverPhasePromotingDragonfly PlannedFailoverPhase = "PromotingDragonfly"

	// PlannedFailoverPhasePromoting means FailoverController.Execute is
	// running against the target.
	PlannedFailoverPhasePromoting PlannedFailoverPhase = "Promoting"

	// PlannedFailoverPhaseResuming means the reconciler is updating
	// status.activeSite and lifting the planned-failover guard.
	PlannedFailoverPhaseResuming PlannedFailoverPhase = "Resuming"

	// PlannedFailoverPhaseSucceeded is the terminal success state.
	PlannedFailoverPhaseSucceeded PlannedFailoverPhase = "Succeeded"

	// PlannedFailoverPhaseFailed is the terminal failure state. Leaves
	// status populated so kubectl describe explains the outcome.
	PlannedFailoverPhaseFailed PlannedFailoverPhase = "Failed"

	// PlannedFailoverPhaseAdmitting means the request was accepted and
	// stamped with a runId, and the run is waiting for the topology
	// manager to grant its claim. The run owns nothing yet; it ends
	// Failed{NotStarted, AdmissionTimeout} if no grant arrives before
	// admission.deadline.
	PlannedFailoverPhaseAdmitting PlannedFailoverPhase = "Admitting"

	// PlannedFailoverPhaseFencing means the claim is granted and the
	// fence intent is durable: the run is setting super_read_only=ON on
	// the source and proving that the fence is its own.
	PlannedFailoverPhaseFencing PlannedFailoverPhase = "Fencing"

	// PlannedFailoverPhasePublishing means the promotion is confirmed,
	// recorded in the topology manager and published to DNS, and the
	// claim is released. Only status bookkeeping remains.
	PlannedFailoverPhasePublishing PlannedFailoverPhase = "Publishing"

	// PlannedFailoverPhaseRollingBack means the run failed while nothing
	// was written to the target and is unfencing the source under the
	// rollback preconditions.
	PlannedFailoverPhaseRollingBack PlannedFailoverPhase = "RollingBack"
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

// PlannedFailoverStatus tracks an in-flight or completed planned
// failover. Only the most recent attempt is retained; re-annotating
// replaces the block.
//
// The durable-claim fields (runId through result) record one run: every
// field belongs to the run named by runId. They are written conditioned on
// the object's resourceVersion. Target and Settings are the run's
// parameters, copied from the consumed annotation and the spec at
// acceptance.
type PlannedFailoverStatus struct {
	// RunID identifies the run this block belongs to. It is minted when a
	// request is first stamped (Admitting or Deferred) and is the token
	// of the run's claim.
	// +kubebuilder:validation:MinLength=1
	// +optional
	RunID string `json:"runId,omitempty"`

	// Revision increments on every write the run makes. Settlement of an
	// ambiguous write identifies a committed write by (runId, revision).
	// +kubebuilder:validation:Minimum=1
	// +optional
	Revision int64 `json:"revision,omitempty"`

	// Settings are the run's effective timing settings, snapshotted at
	// acceptance from the annotation overrides and the spec.
	// +optional
	Settings *PlannedFailoverSettings `json:"settings,omitempty"`

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

	// Phase of the planned failover.
	Phase PlannedFailoverPhase `json:"phase,omitempty"`

	// Target is the site the admin asked to promote.
	Target string `json:"target,omitempty"`

	// SourcePrimary is the site being fenced (status.activeSite at the
	// moment the annotation was accepted).
	SourcePrimary string `json:"sourcePrimary,omitempty"`

	// SourceGtidAtFence is the source's GTID_EXECUTED recorded
	// immediately after super_read_only=ON took effect.
	SourceGtidAtFence string `json:"sourceGtidAtFence,omitempty"`

	// TargetGtidAtPromotion is the target's GTID_EXECUTED captured just
	// before the operator ran SET GLOBAL read_only=0 on it.
	TargetGtidAtPromotion string `json:"targetGtidAtPromotion,omitempty"`

	// StartTime is when the annotation was first accepted.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the state machine reached a terminal phase.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// DurationSeconds is CompletionTime - StartTime, rounded to seconds.
	// +optional
	DurationSeconds *int64 `json:"durationSeconds,omitempty"`

	// TransactionsLost is the count of GTIDs present on the fenced
	// source but not on the promoted target. By construction this is 0
	// on a successful planned switchover; the field is retained for
	// symmetry with the emergency-failover path's data-loss accounting.
	// +optional
	TransactionsLost *int64 `json:"transactionsLost,omitempty"`

	// Reason is a machine-readable tag on Failed outcomes. Examples:
	// "CooldownActive", "LagTimeout", "InvalidGTID", "SourceCrashed",
	// "ExecuteFailed", "InvalidAnnotation", "ConcurrentOperation",
	// "UnknownSite".
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable status line suitable for
	// kubectl describe.
	Message string `json:"message,omitempty"`

	// MaxLagWait is the effective maxLagWait used for this run
	// (after applying the spec default and the per-annotation override).
	// +optional
	MaxLagWait *metav1.Duration `json:"maxLagWait,omitempty"`

	// DrainStartTime is when the reconciler entered the Draining phase
	// (immediately after super_read_only=ON took effect on the source).
	// The DrainTimeout budget is measured from this timestamp.
	// +optional
	DrainStartTime *metav1.Time `json:"drainStartTime,omitempty"`

	// DrainTimeout is the effective drainTimeout used for this run.
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// LagWaitStartTime is when the reconciler entered the WaitingForLag
	// phase. The MaxLagWait budget and the lag-wait histogram are both
	// measured from this timestamp so Pending/Validating/Draining time
	// does not count against the lag budget.
	// +optional
	LagWaitStartTime *metav1.Time `json:"lagWaitStartTime,omitempty"`

	// RetryAfter is populated on the Deferred phase with the earliest
	// time the state machine will retry validation. Derived from
	// status.lastFailover + spec.failoverCooldown. Clears on transition
	// out of Deferred.
	// +optional
	RetryAfter *metav1.Time `json:"retryAfter,omitempty"`

	// Dragonfly tracks Dragonfly-specific outcome of this planned
	// failover attempt. Populated only when spec.dragonfly is enabled.
	// +optional
	Dragonfly *PlannedFailoverDragonflyStatus `json:"dragonfly,omitempty"`
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

// PlannedFailoverDragonflyStatus captures the Dragonfly-specific outcome of
// a planned-failover attempt. Subset of the full design (PLANS doc §42)
// needed to debug the first slice; additional fields can be added later
// without breaking compatibility.
type PlannedFailoverDragonflyStatus struct {
	// Enabled mirrors spec.dragonfly.enabled at the moment the
	// state machine entered the Dragonfly phases.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// SyncWaitStartTime is when the reconciler entered the
	// WaitingForDragonflySync phase. The maxSyncWait budget is measured
	// against this stamp so it is independent from the (often-expired)
	// MySQL lag-wait budget.
	// +optional
	SyncWaitStartTime *metav1.Time `json:"syncWaitStartTime,omitempty"`

	// SourceOffsetAtDrain is the source Dragonfly's master replication
	// offset captured immediately before WaitingForDragonflySync begins
	// polling. Used as the catch-up target for the replica. nil means
	// not yet captured (still within the capture step). Once a value is
	// stamped, OffsetCaptureFailed distinguishes a real captured offset
	// from an "unknown" fallback.
	// +optional
	SourceOffsetAtDrain *int64 `json:"sourceOffsetAtDrain,omitempty"`

	// OffsetCaptureFailed marks that the source Dragonfly was
	// unreachable or failed to respond to INFO replication when the
	// drain offset would normally be captured. The state machine
	// proceeds best-effort with sessions flagged unpreserved; the
	// per-target sync-readiness check is skipped because there is no
	// trustworthy comparator. SourceOffsetAtDrain may be set to a
	// sentinel zero alongside this flag — readers MUST consult this
	// field, not the offset value.
	// +optional
	OffsetCaptureFailed bool `json:"offsetCaptureFailed,omitempty"`

	// TargetOffsetAtPromotion is the target Dragonfly's slave/master
	// replication offset captured at the moment REPLTAKEOVER returned.
	// +optional
	TargetOffsetAtPromotion *int64 `json:"targetOffsetAtPromotion,omitempty"`

	// SyncWaitSeconds is the time spent in WaitingForDragonflySync,
	// from phase entry until either CandidateSyncReady passed or
	// MaxSyncWait expired.
	// +optional
	SyncWaitSeconds *int64 `json:"syncWaitSeconds,omitempty"`

	// SessionsPreserved is a tri-state: true if Dragonfly promotion
	// completed cleanly with replica caught up; false if sessions were
	// lost (timeout-with-proceed, REPLTAKEOVER failure, empty-master
	// fallback); nil when the field is unknown (e.g., Dragonfly disabled
	// for this attempt).
	// +optional
	SessionsPreserved *bool `json:"sessionsPreserved,omitempty"`

	// SourceOffsetState says whether SourceOffset was captured. It
	// replaces offsetCaptureFailed for durable-claim runs.
	// +optional
	SourceOffsetState DragonflyOffsetState `json:"sourceOffsetState,omitempty"`

	// SourceOffset is the source Dragonfly's master replication offset,
	// the catch-up target of the sync wait. Set iff sourceOffsetState is
	// Captured. It replaces sourceOffsetAtDrain for durable-claim runs.
	// +optional
	SourceOffset *int64 `json:"sourceOffset,omitempty"`

	// Takeover is the write-ahead record of the target Dragonfly's
	// promotion. It replaces targetOffsetAtPromotion and promotionMethod
	// for durable-claim runs.
	// +optional
	Takeover *DragonflyTakeoverRecord `json:"takeover,omitempty"`

	// Sessions says whether Dragonfly sessions survived the run. It
	// replaces sessionsPreserved for durable-claim runs.
	// +optional
	Sessions DragonflySessions `json:"sessions,omitempty"`

	// PromotionMethod records how the target was promoted. First slice
	// always uses "REPLTAKEOVER"; reserved values include
	// "REPLICAOF NO ONE" (fallback path) and "skipped" (no promotion
	// attempted, e.g., spec.dragonfly disabled at fail time).
	// +optional
	PromotionMethod string `json:"promotionMethod,omitempty"`

	// Reason is a machine-readable tag explaining a non-success outcome.
	// Examples: "DragonflySyncTimeout", "DragonflyPromotionFailed",
	// "DragonflyDisabled".
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable line for kubectl describe.
	// +optional
	Message string `json:"message,omitempty"`
}
