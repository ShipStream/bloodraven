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
// +kubebuilder:validation:Enum="";Pending;Deferred;Validating;Draining;WaitingForLag;Promoting;Resuming;Succeeded;Failed
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
)

// PlannedFailoverStatus tracks an in-flight or completed planned
// failover. Only the most recent attempt is retained; re-annotating
// replaces the block.
type PlannedFailoverStatus struct {
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
	// "CooldownActive", "LagTimeout", "SourceCrashed", "ExecuteFailed",
	// "InvalidAnnotation", "ConcurrentOperation", "UnknownSite".
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

	// RetryAfter is populated on the Deferred phase with the earliest
	// time the state machine will retry validation. Derived from
	// status.lastFailover + spec.failoverCooldown. Clears on transition
	// out of Deferred.
	// +optional
	RetryAfter *metav1.Time `json:"retryAfter,omitempty"`
}
