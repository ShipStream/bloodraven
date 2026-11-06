package v1alpha1

// MysqlFailoverGroup condition types and reasons owned by the planned-failover
// engine. The remaining condition types and reasons move here with A-69.

// ConditionAutomaticFailoverHeld summarises status.plannedFailoverHold. It is
// True while a planned failover that may have written to its target and
// ended ManualRecoveryRequired holds automatic cross-site actions (emergency
// failover, re-assert, split-brain resolution, ordered updates, rotations,
// follower mutations, bootstrap and DNS reconciliation) for the group, and
// False once an administrator has acknowledged the hold with the
// bloodraven.shipstream.io/acknowledge-planned-failover-hold=<runID>
// annotation. It is absent on a group that was never held.
const ConditionAutomaticFailoverHeld = "AutomaticFailoverHeld"

const (
	// ReasonPlannedFailoverManualRecoveryRequired is the reason of a True
	// AutomaticFailoverHeld condition. It is the same token as the
	// PlannedFailoverManualRecoveryRequired Event (EventPlannedFailoverManualRecoveryRequired).
	// The run's result outcome is the shorter ManualRecoveryRequired
	// (PlannedFailoverOutcomeManualRecoveryRequired).
	ReasonPlannedFailoverManualRecoveryRequired = "PlannedFailoverManualRecoveryRequired"

	// ReasonPlannedFailoverHoldAcknowledged is the reason of a False
	// AutomaticFailoverHeld condition after the hold was acknowledged. It is
	// the same token as the PlannedFailoverHoldAcknowledged Event.
	ReasonPlannedFailoverHoldAcknowledged = "PlannedFailoverHoldAcknowledged"
)
