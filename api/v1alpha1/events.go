package v1alpha1

// Event reasons the planned-failover engine emits on a MysqlFailoverGroup.
// They follow the <Subject><PastTenseVerb>[<Qualifier>] rule: Succeeded and
// Failed for the operation, Rejected for a request that is not admitted,
// Refused for an automatic action blocked for safety. The remaining Event
// reasons move here with A-76.
const (
	EventPlannedFailoverStarted                     = "PlannedFailoverStarted"
	EventPlannedFailoverSkipped                     = "PlannedFailoverSkipped"
	EventPlannedFailoverRejected                    = "PlannedFailoverRejected"
	EventPlannedFailoverDeferred                    = "PlannedFailoverDeferred"
	EventPlannedFailoverAdmissionDelayed            = "PlannedFailoverAdmissionDelayed"
	EventPlannedFailoverClaimGranted                = "PlannedFailoverClaimGranted"
	EventPlannedFailoverDrainStarted                = "PlannedFailoverDrainStarted"
	EventPlannedFailoverTargetCaughtUp              = "PlannedFailoverTargetCaughtUp"
	EventPlannedFailoverDNSUpdateFailed             = "PlannedFailoverDNSUpdateFailed"
	EventPlannedFailoverRollbackStarted             = "PlannedFailoverRollbackStarted"
	EventPlannedFailoverSucceeded                   = "PlannedFailoverSucceeded"
	EventPlannedFailoverFailed                      = "PlannedFailoverFailed"
	EventPlannedFailoverPromotionConfirmationFailed = "PlannedFailoverPromotionConfirmationFailed"
	EventPlannedFailoverStalledManagerAbsent        = "PlannedFailoverStalledManagerAbsent"
	EventPlannedFailoverStatusStalled               = "PlannedFailoverStatusStalled"
	EventPlannedFailoverLegacyRunRefused            = "PlannedFailoverLegacyRunRefused"

	// EventPlannedFailoverManualRecoveryRequired is the same token as the
	// True AutomaticFailoverHeld reason.
	EventPlannedFailoverManualRecoveryRequired = ReasonPlannedFailoverManualRecoveryRequired
	// EventPlannedFailoverHoldPersisted repeats every 10 minutes while a
	// hold stands.
	EventPlannedFailoverHoldPersisted = "PlannedFailoverHoldPersisted"
	// EventPlannedFailoverHoldAcknowledged is the same token as the False
	// AutomaticFailoverHeld reason.
	EventPlannedFailoverHoldAcknowledged           = ReasonPlannedFailoverHoldAcknowledged
	EventPlannedFailoverHoldAcknowledgementIgnored = "PlannedFailoverHoldAcknowledgementIgnored"
)
