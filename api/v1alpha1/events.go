package v1alpha1

// Event reasons, by the kind they are recorded on. Every reason an Event is
// recorded with is declared here (internal/apiregistry derives them from the
// code and fails on an undeclared one). A condition-mirror Event repeats the
// reason of the condition the same step sets, so it is declared as that
// condition-reason constant and does not follow the verb rule below.

// Event reasons the planned-failover engine emits on a MysqlFailoverGroup.
// They follow the <Subject><PastTenseVerb>[<Qualifier>] rule: Succeeded and
// Failed for the operation, Rejected for a request that is not admitted,
// Refused for an automatic action blocked for safety.
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

// Event reasons recorded on a MysqlFailoverGroup.
const (
	EventBackupEncryptionInvalid     = "BackupEncryptionInvalid"
	EventBackupPITRInvalid           = "BackupPITRInvalid"
	EventBackupRetryConflict         = "BackupRetryConflict"
	EventBackupRetryScheduled        = "BackupRetryScheduled"
	EventBackupScheduleInvalid       = "BackupScheduleInvalid"
	EventBackupScheduleRefused       = "BackupScheduleRefused"
	EventBootstrapRefusedRestoreHold = "BootstrapRefusedRestoreHold"
	EventCredentialReconcileFailed   = "CredentialReconcileFailed"
	EventDataLossDetected            = "DataLossDetected"
	EventDeploymentLeaseRevoked      = "DeploymentLeaseRevoked"
	EventDivergenceResolved          = "DivergenceResolved"
	EventDivergentReplicaDetected    = "DivergentReplicaDetected"
	// EventDragonflyOldSiteReconfigured fires when DragonflyManager
	// auto-attaches a stale-master pod as a replica of the active
	// master after verifying it provably never accepted writes
	// (connected_slaves=0 && master_repl_offset=0).
	EventDragonflyOldSiteReconfigured = "DragonflyOldSiteReconfigured"
	EventDragonflyPromotionCompleted  = "DragonflyPromotionCompleted"
	EventDragonflyPromotionFailed     = "DragonflyPromotionFailed"
	EventDragonflyPromotionStarted    = "DragonflyPromotionStarted"
	// EventDragonflyPromotionWaiting fires when a planned failover's
	// Dragonfly phase finds the group's Dragonfly gate held (usually an
	// emergency promotion that already sent its role change) and waits
	// for it while the MySQL source stays fenced.
	EventDragonflyPromotionWaiting = "DragonflyPromotionWaiting"
	// EventDragonflyReplTakeoverSupported fires on a false→true
	// capability transition (image upgrade restored the command).
	EventDragonflyReplTakeoverSupported = "DragonflyReplTakeoverSupported"
	// EventDragonflyReplTakeoverUnsupported fires when a capability
	// probe finds REPLTAKEOVER missing from the running command table.
	EventDragonflyReplTakeoverUnsupported = "DragonflyReplTakeoverUnsupported"
	// EventDragonflySessionsLost fires when emergency promotion
	// fell back to REPLICAOF NO ONE after REPLTAKEOVER failed.
	EventDragonflySessionsLost        = "DragonflySessionsLost"
	EventDragonflyStaleMasterDetected = "DragonflyStaleMasterDetected"
	EventDragonflySyncTimeout         = "DragonflySyncTimeout"
	EventDragonflyUpgradeCompleted    = "DragonflyUpgradeCompleted"
	// EventDragonflyUpgradeDeferred fires when a snapshot-upgrade request
	// is kept but not yet accepted because the Dragonfly master differs
	// from the MySQL active site.
	EventDragonflyUpgradeDeferred           = "DragonflyUpgradeDeferred"
	EventDragonflyUpgradeFailed             = "DragonflyUpgradeFailed"
	EventDragonflyUpgradeRejected           = "DragonflyUpgradeRejected"
	EventDragonflyUpgradeSnapshotCompleted  = "DragonflyUpgradeSnapshotCompleted"
	EventDragonflyUpgradeSnapshotStarted    = "DragonflyUpgradeSnapshotStarted"
	EventDragonflyUpgradeStarted            = "DragonflyUpgradeStarted"
	EventEncryptionAdoptionRefused          = "EncryptionAdoptionRefused"
	EventFailoverExecuted                   = "FailoverExecuted"
	EventGracefulShutdown                   = "GracefulShutdown"
	EventKeyringDigestMismatch              = "KeyringDigestMismatch"
	EventKeyringEscrowCorrupt               = "KeyringEscrowCorrupt"
	EventKeyringEscrowMissing               = "KeyringEscrowMissing"
	EventKeyringNotReadOnly                 = "KeyringNotReadOnly"
	EventKeyringPhase                       = "KeyringPhase"
	EventKeyringPromotionRefused            = "KeyringPromotionRefused"
	EventKeyringPromotionSkipped            = "KeyringPromotionSkipped"
	EventKeyringRecovered                   = "KeyringRecovered"
	EventKeyringRotationRefused             = "KeyringRotationRefused"
	EventKeyringUnsealed                    = "KeyringUnsealed"
	EventLicenseInvalid                     = "LicenseInvalid"
	EventLicenseVerified                    = "LicenseVerified"
	EventMysqlConfIgnored                   = "MysqlConfIgnored"
	EventNoPrimaryDetected                  = "NoPrimaryDetected"
	EventPITRArchiveBindingLimitReached     = "PITRArchiveBindingLimitReached"
	EventPITRArchiveBindingRefused          = "PITRArchiveBindingRefused"
	EventPITRArchiveBindingReleaseRefused   = "PITRArchiveBindingReleaseRefused"
	EventPITRArchiveBindingReleaseRejected  = "PITRArchiveBindingReleaseRejected"
	EventPITRArchiveBindingReleased         = "PITRArchiveBindingReleased"
	EventPITRArchiveBound                   = "PITRArchiveBound"
	EventPITRArchiveEncryptionChangeRefused = "PITRArchiveEncryptionChangeRefused"
	EventPITRArchiveKeyChangeRefused        = "PITRArchiveKeyChangeRefused"
	EventPITRArchiveKeyMaterialLost         = "PITRArchiveKeyMaterialLost"
	EventPITRArchiveKeySnapshotRecreated    = "PITRArchiveKeySnapshotRecreated"
	EventPITRArchiveKeySnapshotUnreadable   = "PITRArchiveKeySnapshotUnreadable"
	EventPITRArchiveKeySourceRemoved        = "PITRArchiveKeySourceRemoved"
	EventPITRArchiveNotReady                = "PITRArchiveNotReady"
	EventPITRArchiveReady                   = "PITRArchiveReady"
	// EventPITRArchiveReissueRiskDetected warns that a promoted site may reissue
	// GTIDs the archive already holds (D-46).
	EventPITRArchiveReissueRiskDetected = "PITRArchiveReissueRiskDetected"
	// EventPITRArchiveUnsafeHistoryDetected is the edge-triggered Warning for the
	// transition into PITRArchiveReady=False/ArchiveUnsafeHistory.
	EventPITRArchiveUnsafeHistoryDetected      = "PITRArchiveUnsafeHistoryDetected"
	EventPITRDivergentHistoryExcluded          = "PITRDivergentHistoryExcluded"
	EventPITRMysqlConfRefused                  = "PITRMysqlConfRefused"
	EventPITROnHistoryTransactionsExcluded     = "PITROnHistoryTransactionsExcluded"
	EventPointInTimePartial                    = "PointInTimePartial"
	EventPromotionRefusedPendingReclone        = "PromotionRefusedPendingReclone"
	EventRecloneRejected                       = "RecloneRejected"
	EventRecloneRequested                      = "RecloneRequested"
	EventRecoveryComplete                      = "RecoveryComplete"
	EventRestoreBuildFailed                    = "RestoreBuildFailed"
	EventRestoreFailed                         = "RestoreFailed"
	EventRestoreInPlaceBuildFailed             = "RestoreInPlaceBuildFailed"
	EventRestoreInPlaceCancelled               = "RestoreInPlaceCancelled"
	EventRestoreInPlaceConfirmDeferred         = "RestoreInPlaceConfirmDeferred"
	EventRestoreInPlaceConfirmRebound          = "RestoreInPlaceConfirmRebound"
	EventRestoreInPlaceCredsRefreshFailed      = "RestoreInPlaceCredsRefreshFailed"
	EventRestoreInPlaceFailed                  = "RestoreInPlaceFailed"
	EventRestoreInPlaceJobBusy                 = "RestoreInPlaceJobBusy"
	EventRestoreInPlaceJobCreated              = "RestoreInPlaceJobCreated"
	EventRestoreInPlaceJobLost                 = "RestoreInPlaceJobLost"
	EventRestoreInPlaceLoaded                  = "RestoreInPlaceLoaded"
	EventRestoreInPlacePeerStopFailed          = "RestoreInPlacePeerStopFailed"
	EventRestoreInPlaceRejected                = "RestoreInPlaceRejected"
	EventRestoreInPlaceSpecRemoved             = "RestoreInPlaceSpecRemoved"
	EventRestoreInPlaceStaleJobRemoved         = "RestoreInPlaceStaleJobRemoved"
	EventRestoreInPlaceStarted                 = "RestoreInPlaceStarted"
	EventRestoreInPlaceSucceeded               = "RestoreInPlaceSucceeded"
	EventRestoreInPlaceWaitingForPreviousQueue = "RestoreInPlaceWaitingForPreviousQueue"
	EventRestoreOfflineFenceApplied            = "RestoreOfflineFenceApplied"
	EventRestoreOfflineFenceReleaseFailed      = "RestoreOfflineFenceReleaseFailed"
	EventRestoreOfflineFenceReleased           = "RestoreOfflineFenceReleased"
	EventRestoreOfflineFenceSessionsKept       = "RestoreOfflineFenceSessionsKept"
	EventRestorePeerReplicationIncomplete      = "RestorePeerReplicationIncomplete"
	EventRestorePeerReplicationStopped         = "RestorePeerReplicationStopped"
	EventRestoreRecloneFailed                  = "RestoreRecloneFailed"
	EventRestoreRecloneRequested               = "RestoreRecloneRequested"
	EventRestoreRecloneSucceeded               = "RestoreRecloneSucceeded"
	EventRestoreRecloneWithdrawn               = "RestoreRecloneWithdrawn"
	EventRestoreReclonesAbandoned              = "RestoreReclonesAbandoned"
	EventRestoreReclonesComplete               = "RestoreReclonesComplete"
	EventRestoreReclonesDiscarded              = "RestoreReclonesDiscarded"
	EventRestoreReclonesHeld                   = "RestoreReclonesHeld"
	EventRestoreReclonesRestored               = "RestoreReclonesRestored"
	EventRestoreStarted                        = "RestoreStarted"
	EventRestoreSucceeded                      = "RestoreSucceeded"
	EventRestoreTargetUnavailable              = "RestoreTargetUnavailable"
	EventS3CredentialsIncomplete               = "S3CredentialsIncomplete"
	EventSecretMissingKey                      = "SecretMissingKey"
	EventSecretNotFound                        = "SecretNotFound"
	EventSidecarVersionSkew                    = "SidecarVersionSkew"
	EventSiteRecovered                         = "SiteRecovered"
	EventSplitBrainDetected                    = "SplitBrainDetected"
	// EventTopologyManagerStartFailed is the Warning Event reason for a
	// group left without a running topology manager because its start
	// keeps failing.
	EventTopologyManagerStartFailed = "TopologyManagerStartFailed"
	// EventTopologyManagerStarted is the Normal Event reason for a start
	// that succeeds after a TopologyManagerStartFailed Warning.
	EventTopologyManagerStarted      = "TopologyManagerStarted"
	EventTotalLossDetected           = "TotalLossDetected"
	EventVerificationScheduleInvalid = "VerificationScheduleInvalid"
)

// Event reasons recorded on a MysqlBackup.
const (
	EventArtifactCleanupBlocked         = "ArtifactCleanupBlocked"
	EventArtifactCleanupDeferred        = "ArtifactCleanupDeferred"
	EventArtifactCleanupFailed          = "ArtifactCleanupFailed"
	EventArtifactCleanupNothingFound    = "ArtifactCleanupNothingFound"
	EventArtifactCleanupProducerStuck   = "ArtifactCleanupProducerStuck"
	EventArtifactCleanupRetry           = "ArtifactCleanupRetry"
	EventArtifactCleanupSkipped         = "ArtifactCleanupSkipped"
	EventArtifactCleanupStarted         = "ArtifactCleanupStarted"
	EventArtifactCleanupSucceeded       = "ArtifactCleanupSucceeded"
	EventArtifactLocationRestored       = "ArtifactLocationRestored"
	EventBackupFailed                   = "BackupFailed"
	EventBackupMetadataIncomplete       = "BackupMetadataIncomplete"
	EventBackupStarted                  = "BackupStarted"
	EventBackupSucceeded                = "BackupSucceeded"
	EventEncryptionAttributionCorrected = "EncryptionAttributionCorrected"
	EventInFlightFailover               = "InFlightFailover"
	// EventPITRArchiveNotStamped (Normal, on a MysqlBackup): the
	// backup runs without a PITR archive stamp (not a PITR baseline)
	// because the site Deployments do not agree on a rendered archive and
	// waiting would not make it one (F-B).
	EventPITRArchiveNotStamped = "PITRArchiveNotStamped"
	EventRetentionDeferred     = "RetentionDeferred"
)

// Event reasons recorded on a MysqlBackupVerification.
const (
	EventVerificationFailed    = "VerificationFailed"
	EventVerificationStarted   = "VerificationStarted"
	EventVerificationSucceeded = "VerificationSucceeded"
)

// Event reasons recorded on a MysqlDatabase.
const (
	EventDatabaseCleanupSkipped     = "DatabaseCleanupSkipped"
	EventDatabaseDropDeferred       = "DatabaseDropDeferred"
	EventDatabaseDropSkipped        = "DatabaseDropSkipped"
	EventDatabaseDropped            = "DatabaseDropped"
	EventDatabaseRetained           = "DatabaseRetained"
	EventOwnerHostsRemoved          = "OwnerHostsRemoved"
	EventOwnerUserDropSkipped       = "OwnerUserDropSkipped"
	EventOwnerUserReservedSkipped   = "OwnerUserReservedSkipped"
	EventOwnerUserRotated           = "OwnerUserRotated"
	EventPartialRevokeLifted        = "PartialRevokeLifted"
	EventTenantPrivilegeRestriction = "TenantPrivilegeRestriction"
	EventUserDropSkipped            = "UserDropSkipped"
	EventUserHostsRemoved           = "UserHostsRemoved"
	EventUserRemoved                = "UserRemoved"
	EventUserReservedSkipped        = "UserReservedSkipped"
	EventUserRotated                = "UserRotated"
	EventUserTransferred            = "UserTransferred"
)

// Event reasons recorded on a MysqlStandbyCluster.
const (
	EventBucketScanned       = "BucketScanned"
	EventManifestParseFailed = "ManifestParseFailed"
	EventMetadataParseFailed = "MetadataParseFailed"
	EventPITRArchiveRefused  = "PITRArchiveRefused"
)

// Condition-mirror Event reasons recorded on a MysqlFailoverGroup: each repeats
// the reason of the condition the same step sets.
const (
	EventDivergentSitePromoted = ReasonDivergentSitePromoted
	EventOrderedUpdateStalled  = ReasonOrderedUpdateStalled
)

// Condition-mirror Event reasons recorded on a MysqlBackup: each repeats
// the reason of the condition the same step sets.
const (
	EventBackupJobConflict         = ReasonBackupJobConflict
	EventDumpKeySnapshotConflict   = ReasonDumpKeySnapshotConflict
	EventNoHealthySource           = ReasonNoHealthySource
	EventPITRArchiveRolloutPending = ReasonPITRArchiveRolloutPending
	EventPassphraseSecretMissing   = ReasonPassphraseSecretMissing
	EventProfileReadOnly           = ReasonProfileReadOnly
)

// Condition-mirror Event reasons recorded on a MysqlBackupVerification: each repeats
// the reason of the condition the same step sets.
const (
	EventPITRFailedSentinel            = string(PITRTokenPITRFailed)
	EventBackupIdentityMismatch        = ReasonBackupIdentityMismatch
	EventBackupNotAvailable            = ReasonBackupNotAvailable
	EventBlockedByActiveVerification   = ReasonBlockedByActiveVerification
	EventPITRArchiveBindingNotReady    = string(PITRTokenArchiveBindingNotReady)
	EventPITRArchiveCopy               = string(PITRTokenArchiveCopy)
	EventPITRArchiveDescriptorMissing  = string(PITRTokenArchiveDescriptorMissing)
	EventPITRArchiveDestinationShared  = string(PITRTokenArchiveDestinationShared)
	EventPITRArchiveDivergentHistory   = string(PITRTokenArchiveDivergentHistory)
	EventPITRArchiveEncryptionMismatch = string(PITRTokenArchiveEncryptionMismatch)
	EventPITRArchiveForeign            = string(PITRTokenArchiveForeign)
	EventPITRArchiveGap                = string(PITRTokenArchiveGap)
	EventPITRArchiveHistoryReset       = string(PITRTokenArchiveHistoryReset)
	EventPITRArchiveIdentityMismatch   = string(PITRTokenArchiveIdentityMismatch)
	EventPITRArchiveKeyMismatch        = string(PITRTokenArchiveKeyMismatch)
	EventPITRArchiveLegacy             = string(PITRTokenArchiveLegacy)
	EventPITRArchiveListFailed         = string(PITRTokenArchiveListFailed)
	EventPITRArchiveNamespaceReuse     = string(PITRTokenArchiveNamespaceReuse)
	EventPITRArchiveNotFound           = string(PITRTokenArchiveNotFound)
	EventPITRArchiveObjectInvalid      = string(PITRTokenArchiveObjectInvalid)
	EventPITRArchiveOrderConflict      = string(PITRTokenArchiveOrderConflict)
	EventPITRArchiveOrderInversion     = string(PITRTokenArchiveOrderInversion)
	EventPITRArchiveSplitBrain         = string(PITRTokenArchiveSplitBrain)
	EventPITRArchiveStoreUnsupported   = string(PITRTokenArchiveStoreUnsupported)
	EventPITRArchiveUnsafeHistory      = string(PITRTokenArchiveUnsafeHistory)
	EventPITRArchiveVersionUnsupported = string(PITRTokenArchiveVersionUnsupported)
	EventPITRBackupAfterTarget         = string(PITRTokenBackupAfterTarget)
	EventPITRBackupDumpKeyMissing      = string(PITRTokenBackupDumpKeyMissing)
	EventPITRBackupFiltered            = string(PITRTokenBackupFiltered)
	EventPITRBackupIdentityMismatch    = string(PITRTokenBackupIdentityMismatch)
	EventPITRBackupIdentityUnknown     = string(PITRTokenBackupIdentityUnknown)
	EventPITRBackupIncomplete          = string(PITRTokenBackupIncomplete)
	EventPITRBackupInconsistent        = string(PITRTokenBackupInconsistent)
	EventPITRBackupNotOnLineage        = string(PITRTokenBackupNotOnLineage)
	EventPITRCoverageInvalid           = string(PITRTokenCoverageInvalid)
	EventPITRExcludeIntersectsBackup   = string(PITRTokenExcludeIntersectsBackup)
	EventPITRNoArchivedReplay          = string(PITRTokenNoArchivedReplay)
	EventPITRPlanTooLarge              = string(PITRTokenPlanTooLarge)
	EventPITRReplayMismatch            = string(PITRTokenReplayMismatch)
	EventPITRReplayUnsupported         = string(PITRTokenReplayUnsupported)
	EventPITRStopAmbiguous             = string(PITRTokenStopAmbiguous)
	EventPITRTargetInvalid             = string(PITRTokenTargetInvalid)
	EventPITRTargetNotCovered          = string(PITRTokenTargetNotCovered)
)

// Condition-mirror Event reasons recorded on a MysqlDatabase: each repeats
// the reason of the condition the same step sets.
const (
	EventDatabaseNameConflict   = ReasonDatabaseNameConflict
	EventDatabasePreExists      = ReasonDatabasePreExists
	EventGrantUserMissing       = ReasonGrantUserMissing
	EventInvalidSpec            = ReasonInvalidSpec
	EventMySQLError             = ReasonMySQLError
	EventOwnerConflict          = ReasonOwnerConflict
	EventOwnerUserReserved      = ReasonOwnerUserReserved
	EventPartialRevokesOff      = ReasonPartialRevokesOff
	EventPeerOwnerSecretMissing = ReasonPeerOwnerSecretMissing
	EventPreExistingOwnerUser   = ReasonPreExistingOwnerUser
	EventPreExistingUser        = ReasonPreExistingUser
	EventUserClaimedBySibling   = ReasonUserClaimedBySibling
	EventUserReserved           = ReasonUserReserved
)

// Condition-mirror Event reasons recorded on a MysqlStandbyCluster: each repeats
// the reason of the condition the same step sets.
const (
	EventAuthFailed  = ReasonAuthFailed
	EventConfigError = ReasonConfigError
	EventListFailed  = ReasonListFailed
)

// Condition-mirror Event reasons recorded on more than one kind: each
// repeats the reason of the condition the same step sets.
const (
	EventBuildJobFailed  = ReasonBuildJobFailed  // MysqlBackup, MysqlBackupVerification
	EventGroupNotFound   = ReasonGroupNotFound   // MysqlBackup, MysqlBackupVerification
	EventProfileNotFound = ReasonProfileNotFound // MysqlBackup, MysqlBackupVerification
)
