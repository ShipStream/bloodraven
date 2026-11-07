package v1alpha1

// Condition types and reasons, per kind, and the deployment API's
// unstableReason values. The controllers write only the values declared
// here; internal/apiregistry derives every value they can write and fails
// on an undeclared one.

// Condition types. Ready is shared by the kinds that have it; the
// MysqlStandbyCluster types are the StandbyCondition constants.
const (
	// ConditionReady is the Ready condition type. On a MysqlDatabase,
	// together with status.observedGeneration, it is the contract a caller
	// polls instead of opening a MySQL connection, so treat both as API
	// surface rather than as diagnostics.
	ConditionReady = "Ready"
)

// MysqlFailoverGroup condition types.
const (
	// ConditionAutomaticFailoverHeld summarises status.plannedFailoverHold. It is
	// True while a planned failover that may have written to its target and
	// ended ManualRecoveryRequired holds automatic cross-site actions (emergency
	// failover, re-assert, split-brain resolution, ordered updates, rotations,
	// follower mutations, bootstrap and DNS reconciliation) for the group, and
	// False once an administrator has acknowledged the hold with the
	// bloodraven.shipstream.io/acknowledge-planned-failover-hold=<runID>
	// annotation. It is absent on a group that was never held.
	ConditionAutomaticFailoverHeld = "AutomaticFailoverHeld"
	ConditionBootstrapping         = "Bootstrapping"
	ConditionDegraded              = "Degraded"
	// ConditionDivergentPromotion records that a site holding transactions its
	// former primary lacked was promoted. Set by the operator, removed only by
	// an administrator.
	ConditionDivergentPromotion = "DivergentPromotion"
	// ConditionEncryptionAtRestReady summarises the encryption-at-rest
	// subsystem.
	ConditionEncryptionAtRestReady = "EncryptionAtRestReady"
	// ConditionPITRArchiveBindingReady reports the group's PITR archive
	// bindings.
	ConditionPITRArchiveBindingReady = "PITRArchiveBindingReady"
	// ConditionPITRArchiveReady reports whether every site's PITR archiver is
	// running. It is separate from Degraded, which replication reasons
	// overwrite last-writer-wins, so an archive failure can neither hide nor
	// be hidden by a replication problem.
	ConditionPITRArchiveReady = "PITRArchiveReady"
	// ConditionPITRMysqlConfAccepted reports whether spec.mysqlConf sets an
	// option that breaks PITR replay certification while
	// spec.backup.pitr.enabled is true. Such an option is refused: it is left
	// out of the rendered my.cnf, so the server keeps MySQL's safe default
	// (ROW, FULL, no filter). Present only while PITR is enabled.
	ConditionPITRMysqlConfAccepted = "PITRMysqlConfAccepted"
	ConditionRecoveryPending       = "RecoveryPending"
	// ConditionS3CredentialsReady reports whether every S3 credentialsSecretName the
	// group references holds the keys backup, restore and cleanup Jobs require.
	// Those Jobs fail rather than fall back to ambient (IRSA, Workload Identity,
	// instance-profile) credentials, so a Secret that relied on the fallback
	// would otherwise surface only when the next scheduled Job fails.
	ConditionS3CredentialsReady = "S3CredentialsReady"
	ConditionUpdating           = "Updating"
)

// MysqlBackup condition types.
const (
	// ConditionArtifactCleanup reports why a deleted MysqlBackup's artifact has
	// not been removed yet. It is only written while the CR is Terminating.
	ConditionArtifactCleanup  = "ArtifactCleanup"
	ConditionJobCreated       = "JobCreated"
	ConditionMetadataComplete = "MetadataComplete"
)

// MysqlBackupVerification condition types.
const (
	ConditionResourcesCleanedUp = "ResourcesCleanedUp"
	ConditionVerified           = "Verified"
)

// MysqlStandbyCluster condition types.
const (
	// StandbyConditionBucketReadable is True when the source
	// bucket+prefix was successfully listed within the last discovery
	// interval. Message carries the underlying error on False.
	//
	// Reasons:
	//   - ListSucceeded (True): the bucket prefix was listed.
	//   - ListFailed (False): the List call returned a genuine error.
	//   - ScanIncomplete (False): the List call did not complete before the
	//     scan deadline (a context deadline/cancel during List, distinct from a
	//     genuine ListFailed). The previous status.discovered is preserved.
	//   - AuthFailed (False): archive-store construction failed.
	//   - ConfigError (False): missing/invalid spec fields.
	StandbyConditionBucketReadable = "BucketReadable"

	// StandbyConditionSourceConfigKnown is True when the controller has
	// resolved at least one full dump AND at least one binlog manifest
	// under the configured prefix, and verified the source PITR archive's
	// identity (descriptor and every manifest). The default `kubectl get` printcolumn
	// surfaces this as the neutral `SourceKnown`.
	//
	// Reasons:
	//   - DumpFound (True): a dump and at least one binlog manifest exist,
	//     and the source archive's descriptor and manifests verified.
	//   - NoDumpFound (False): no dump @.json under the prefix.
	//   - NoBinlogManifests (False): a dump WAS found, but no PITR binlog
	//     manifests exist under <prefix>/binlogs/ yet. This is NOT a
	//     misconfiguration — it is the expected state for a dump-only
	//     source (no continuous binlog archival), or a brand-new source
	//     whose first binlog manifest has not yet been uploaded. Recovery
	//     is limited to the dump (no point-in-time window).
	//   - MetadataUnreadable (False): the dump @.json could not be parsed
	//     (genuinely malformed metadata, not a deadline).
	//   - ScanIncomplete (False): the scan was cut short by a context
	//     deadline/cancel before it finished reading every dump @.json (or the
	//     manifest loop). The selection would be partial, so it is NOT
	//     published; the previous status.discovered is preserved (last-known-
	//     good) and no BucketScanned event is emitted. Distinct from
	//     MetadataUnreadable (which is genuine corruption) and ListFailed.
	//   - ConfigError (False): propagated from a storage-backend error.
	//   - Archive* (False): the source PITR archive was refused:
	//     status.discovered.latestDump is still published, binlogArchive
	//     (identity and window) is not. The reason
	//     is the refusal token without its "PITR" prefix:
	//     ArchiveListFailed, ArchiveDescriptorMissing, ArchiveLegacy,
	//     ArchiveVersionUnsupported, ArchiveObjectInvalid,
	//     ArchiveEncryptionMismatch, ArchiveKeyMismatch,
	//     ArchiveIdentityMismatch (a manifest names another archive),
	//     ArchiveHistoryReset (a site's manifest records a reset epoch),
	//     ArchiveUnsafeHistory (every site carries a history-integrity
	//     marker), or ArchiveUnreadable (no other token). A marked site among
	//     unmarked ones is left out of the window instead.
	StandbyConditionSourceConfigKnown = "SourceConfigKnown"
)

// MysqlFailoverGroup AutomaticFailoverHeld reasons.
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

// MysqlFailoverGroup condition reasons.
const (
	ReasonAccepted                 = "Accepted"
	ReasonAdoptionRefused          = "AdoptionRefused"
	ReasonAlert                    = "Alert"
	ReasonAllSitesSealed           = "AllSitesSealed"
	ReasonArchiveStoreInitFailed   = "ArchiveStoreInitFailed"
	ReasonArchiverDisabled         = "ArchiverDisabled"
	ReasonArchiverError            = "ArchiverError"
	ReasonArchiving                = "Archiving"
	ReasonAwaitingFirstPoll        = "AwaitingFirstPoll"
	ReasonAwaitingRollout          = "AwaitingRollout"
	ReasonBindingLimitReached      = "BindingLimitReached"
	ReasonCloning                  = "Cloning"
	ReasonComplete                 = "Complete"
	ReasonCredentialsComplete      = "CredentialsComplete"
	ReasonCredentialsIncomplete    = "CredentialsIncomplete"
	ReasonDegraded                 = "Degraded"
	ReasonDivergentSitePromoted    = "DivergentSitePromoted"
	ReasonDivergentTransactions    = "DivergentTransactions"
	ReasonDone                     = "Done"
	ReasonEncryptionChangeRefused  = "EncryptionChangeRefused"
	ReasonFailover                 = "Failover"
	ReasonHealthy                  = "Healthy"
	ReasonInitFromBackupFailed     = "InitFromBackupFailed"
	ReasonInitFromBackupInProgress = "InitFromBackupInProgress"
	ReasonInitializing             = "Initializing"
	ReasonKeyChangeRefused         = "KeyChangeRefused"
	ReasonKeyringFailed            = "KeyringFailed"
	ReasonKeyringNotSealed         = "KeyringNotSealed"
	ReasonNoPrimary                = "NoPrimary"
	ReasonNoRecoveryPending        = "NoRecoveryPending"
	ReasonNotBootstrapping         = "NotBootstrapping"
	ReasonNotUpdating              = "NotUpdating"
	// ReasonOrderedUpdateStalled is the Updating condition reason, and the
	// Warning Event reason, for an ordered update stopped on a site whose pod
	// is not Ready.
	ReasonOrderedUpdateStalled      = "OrderedUpdateStalled"
	ReasonPITRUnsafeSettingRefused  = "PITRUnsafeSettingRefused"
	ReasonRecoveryInProgress        = "RecoveryInProgress"
	ReasonReplicationBroken         = "ReplicationBroken"
	ReasonReplicationError          = "ReplicationError"
	ReasonReplicationLagging        = "ReplicationLagging"
	ReasonReplicationSourceMismatch = "ReplicationSourceMismatch"
	ReasonRestoreReclonePending     = "RestoreReclonePending"
	ReasonSetupReplication          = "SetupReplication"
	ReasonSidecarOutdated           = "SidecarOutdated"
	ReasonSidecarUnreachable        = "SidecarUnreachable"
	ReasonSplitBrain                = "SplitBrain"
	ReasonTopologyPolled            = "TopologyPolled"
	ReasonTotalLoss                 = "TotalLoss"
	ReasonUpdateOldPrimary          = "UpdateOldPrimary"
	ReasonUpdateReplica             = "UpdateReplica"
	ReasonWaitOldPrimary            = "WaitOldPrimary"
	ReasonWaitReplica               = "WaitReplica"
	ReasonWaitingForRestart         = "WaitingForRestart"
)

// MysqlBackup condition reasons.
const (
	ReasonBackupJobConflict         = "BackupJobConflict"
	ReasonCleanupFailed             = "CleanupFailed"
	ReasonCleanupJobConflict        = "CleanupJobConflict"
	ReasonCleanupJobInvalid         = "CleanupJobInvalid"
	ReasonCleanupPending            = "CleanupPending"
	ReasonCleanupRejected           = "CleanupRejected"
	ReasonCollected                 = "Collected"
	ReasonCreated                   = "Created"
	ReasonCredentialsUnavailable    = "CredentialsUnavailable"
	ReasonDestinationMismatch       = "DestinationMismatch"
	ReasonDestinationUnavailable    = "DestinationUnavailable"
	ReasonDestinationUnverified     = "DestinationUnverified"
	ReasonDumpKeySnapshotConflict   = "DumpKeySnapshotConflict"
	ReasonForegroundDeletion        = "ForegroundDeletion"
	ReasonHeldByVerification        = "HeldByVerification"
	ReasonLocationMismatch          = "LocationMismatch"
	ReasonNoHealthySource           = "NoHealthySource"
	ReasonNotCreated                = "NotCreated"
	ReasonOperatorImageUnset        = "OperatorImageUnset"
	ReasonOrphanDeletion            = "OrphanDeletion"
	ReasonPITRArchiveRolloutPending = "PITRArchiveRolloutPending"
	ReasonPassphraseSecretMissing   = "PassphraseSecretMissing"
	ReasonProducerStopStuck         = "ProducerStopStuck"
	ReasonProfileReadOnly           = "ProfileReadOnly"
	ReasonSentinelMissing           = "SentinelMissing"
	ReasonSentinelUnavailable       = "SentinelUnavailable"
)

// MysqlBackupVerification condition reasons.
const (
	ReasonBackupNotAvailable          = "BackupNotAvailable"
	ReasonBlockedByActiveVerification = "BlockedByActiveVerification"
	ReasonCleanedUp                   = "CleanedUp"
	ReasonSanityCheckFailed           = "SanityCheckFailed"
	ReasonSanityCheckTimeout          = "SanityCheckTimeout"
)

// MysqlDatabase condition reasons.
const (
	ReasonApplying                  = "Applying"
	ReasonDatabaseNameConflict      = "DatabaseNameConflict"
	ReasonDatabasePreExists         = "DatabasePreExists"
	ReasonDatabaseReconciled        = "DatabaseReconciled"
	ReasonDeleting                  = "Deleting"
	ReasonGrantUserMissing          = "GrantUserMissing"
	ReasonGroupSecretMissing        = "GroupSecretMissing"
	ReasonGroupTerminating          = "GroupTerminating"
	ReasonInvalidSpec               = "InvalidSpec"
	ReasonMySQLError                = "MySQLError"
	ReasonNoActiveSite              = "NoActiveSite"
	ReasonOwnerConflict             = "OwnerConflict"
	ReasonOwnerSecretIncomplete     = "OwnerSecretIncomplete"
	ReasonOwnerSecretMissing        = "OwnerSecretMissing"
	ReasonOwnerUserReserved         = "OwnerUserReserved"
	ReasonPartialRevokesOff         = "PartialRevokesOff"
	ReasonPeerOwnerSecretMissing    = "PeerOwnerSecretMissing"
	ReasonPlannedFailoverInProgress = "PlannedFailoverInProgress"
	ReasonPreExistingOwnerUser      = "PreExistingOwnerUser"
	ReasonPreExistingUser           = "PreExistingUser"
	ReasonPrimaryUnavailable        = "PrimaryUnavailable"
	ReasonRestoreInProgress         = "RestoreInProgress"
	ReasonUserClaimedBySibling      = "UserClaimedBySibling"
	ReasonUserReserved              = "UserReserved"
	ReasonUserSecretIncomplete      = "UserSecretIncomplete"
	ReasonUserSecretMissing         = "UserSecretMissing"
)

// MysqlStandbyCluster condition reasons.
const (
	// SourceConfigKnown=False reasons for a refused source archive that are
	// not PITRArchiveRefusalReason values. ArchiveUnreadable is a refusal
	// without a recognised token.
	ReasonArchiveHistoryReset     = "ArchiveHistoryReset"
	ReasonArchiveIdentityMismatch = "ArchiveIdentityMismatch"
	ReasonArchiveListFailed       = "ArchiveListFailed"
	ReasonArchiveObjectInvalid    = "ArchiveObjectInvalid"
	ReasonArchiveUnreadable       = "ArchiveUnreadable"
	ReasonAuthFailed              = "AuthFailed"
	ReasonConfigError             = "ConfigError"
	ReasonDumpFound               = "DumpFound"
	ReasonListFailed              = "ListFailed"
	ReasonListSucceeded           = "ListSucceeded"
	ReasonMetadataUnreadable      = "MetadataUnreadable"
	ReasonNoBinlogManifests       = "NoBinlogManifests"
	ReasonNoDumpFound             = "NoDumpFound"
	ReasonScanIncomplete          = "ScanIncomplete"
)

// Condition reasons used by more than one kind.
const (
	ReasonBackupIdentityMismatch = "BackupIdentityMismatch" // MysqlBackupVerification, MysqlStandbyCluster
	ReasonBuildJobFailed         = "BuildJobFailed"         // MysqlBackup, MysqlBackupVerification
	ReasonFailed                 = "Failed"                 // MysqlFailoverGroup, MysqlBackup, MysqlBackupVerification
	ReasonGroupNotFound          = "GroupNotFound"          // MysqlBackup, MysqlBackupVerification, MysqlDatabase
	ReasonProfileNotFound        = "ProfileNotFound"        // MysqlBackup, MysqlBackupVerification
	ReasonSucceeded              = "Succeeded"              // MysqlBackup, MysqlBackupVerification
)

// PITRToken is a PITR refusal token: the first word of a PITR Job's
// termination message, of a sidecar error, or of a reconcile-time refusal.
// The same strings are the MysqlBackupVerification Verified=False reasons
// and the status.restore.reason values for a refused point-in-time restore.
// The sidecar, which does not import this package, keeps an identical
// catalog in internal/pitrarchive.
type PITRToken string

// PITRToken values.
const (
	PITRTokenArchiveListFailed         PITRToken = "PITRArchiveListFailed"
	PITRTokenArchiveNotFound           PITRToken = "PITRArchiveNotFound"
	PITRTokenArchiveLegacy             PITRToken = "PITRArchiveLegacy"
	PITRTokenArchiveDescriptorMissing  PITRToken = "PITRArchiveDescriptorMissing"
	PITRTokenArchiveVersionUnsupported PITRToken = "PITRArchiveVersionUnsupported"
	PITRTokenArchiveIdentityMismatch   PITRToken = "PITRArchiveIdentityMismatch"
	PITRTokenArchiveForeign            PITRToken = "PITRArchiveForeign"
	PITRTokenArchiveCopy               PITRToken = "PITRArchiveCopy"
	PITRTokenArchiveEncryptionMismatch PITRToken = "PITRArchiveEncryptionMismatch"
	PITRTokenArchiveKeyMismatch        PITRToken = "PITRArchiveKeyMismatch"
	PITRTokenArchiveStoreUnsupported   PITRToken = "PITRArchiveStoreUnsupported"
	PITRTokenArchiveUnsafeHistory      PITRToken = "PITRArchiveUnsafeHistory"
	PITRTokenArchiveHistoryReset       PITRToken = "PITRArchiveHistoryReset"
	PITRTokenArchiveObjectInvalid      PITRToken = "PITRArchiveObjectInvalid"
	PITRTokenArchiveNamespaceReuse     PITRToken = "PITRArchiveNamespaceReuse"
	PITRTokenArchiveSplitBrain         PITRToken = "PITRArchiveSplitBrain"
	PITRTokenArchiveDivergentHistory   PITRToken = "PITRArchiveDivergentHistory"
	PITRTokenArchiveGap                PITRToken = "PITRArchiveGap"
	PITRTokenArchiveOrderConflict      PITRToken = "PITRArchiveOrderConflict"
	PITRTokenArchiveOrderInversion     PITRToken = "PITRArchiveOrderInversion"
	PITRTokenArchiveDestinationShared  PITRToken = "PITRArchiveDestinationShared"
	PITRTokenArchiveBindingNotReady    PITRToken = "PITRArchiveBindingNotReady"
	PITRTokenBackupIdentityUnknown     PITRToken = "PITRBackupIdentityUnknown"
	PITRTokenBackupIdentityMismatch    PITRToken = "PITRBackupIdentityMismatch"
	PITRTokenBackupInconsistent        PITRToken = "PITRBackupInconsistent"
	PITRTokenBackupFiltered            PITRToken = "PITRBackupFiltered"
	PITRTokenBackupIncomplete          PITRToken = "PITRBackupIncomplete"
	PITRTokenBackupDumpKeyMissing      PITRToken = "PITRBackupDumpKeyMissing"
	PITRTokenBackupAfterTarget         PITRToken = "PITRBackupAfterTarget"
	PITRTokenBackupNotOnLineage        PITRToken = "PITRBackupNotOnLineage"
	PITRTokenExcludeIntersectsBackup   PITRToken = "PITRExcludeIntersectsBackup"
	PITRTokenStopAmbiguous             PITRToken = "PITRStopAmbiguous"
	PITRTokenTargetNotCovered          PITRToken = "PITRTargetNotCovered"
	PITRTokenTargetInvalid             PITRToken = "PITRTargetInvalid"
	PITRTokenNoArchivedReplay          PITRToken = "PITRNoArchivedReplay"
	PITRTokenReplayUnsupported         PITRToken = "PITRReplayUnsupported"
	PITRTokenPlanTooLarge              PITRToken = "PITRPlanTooLarge"
	PITRTokenCoverageInvalid           PITRToken = "PITRCoverageInvalid"
	PITRTokenReplayMismatch            PITRToken = "PITRReplayMismatch"
	PITRTokenPITRFailed                PITRToken = "BLOODRAVEN_PITR_FAILED"
)

// PITRArchiveBindingReason values: status.pitrArchiveBindings[].reason, and
// the PITRArchiveBindingReady reasons that name a binding. Pending:
// Authenticating, Establishing. Bound: Bound, KeyMaterialLost. Refused: the
// rest.
const (
	PITRArchiveBindingReasonAuthenticating            PITRArchiveBindingReason = "Authenticating"
	PITRArchiveBindingReasonEstablishing              PITRArchiveBindingReason = "Establishing"
	PITRArchiveBindingReasonBound                     PITRArchiveBindingReason = "Bound"
	PITRArchiveBindingReasonKeyMaterialLost           PITRArchiveBindingReason = "KeyMaterialLost"
	PITRArchiveBindingReasonKeyMismatch               PITRArchiveBindingReason = "KeyMismatch"
	PITRArchiveBindingReasonArchiveForeign            PITRArchiveBindingReason = "ArchiveForeign"
	PITRArchiveBindingReasonArchiveCopy               PITRArchiveBindingReason = "ArchiveCopy"
	PITRArchiveBindingReasonArchiveLegacy             PITRArchiveBindingReason = "ArchiveLegacy"
	PITRArchiveBindingReasonArchiveDescriptorMissing  PITRArchiveBindingReason = "ArchiveDescriptorMissing"
	PITRArchiveBindingReasonArchiveNotFound           PITRArchiveBindingReason = "ArchiveNotFound"
	PITRArchiveBindingReasonArchiveEncryptionMismatch PITRArchiveBindingReason = "ArchiveEncryptionMismatch"
	PITRArchiveBindingReasonArchiveVersionUnsupported PITRArchiveBindingReason = "ArchiveVersionUnsupported"
	PITRArchiveBindingReasonArchiveListFailed         PITRArchiveBindingReason = "ArchiveListFailed"
)

// PITRArchiveRefusalReason values: status.pitr.archive.reason, and the
// PITRArchiveReady=False reasons for a refused archive. Each is a
// PITRToken without its "PITR" prefix.
const (
	PITRArchiveRefusalReasonArchiveForeign            PITRArchiveRefusalReason = "ArchiveForeign"
	PITRArchiveRefusalReasonArchiveCopy               PITRArchiveRefusalReason = "ArchiveCopy"
	PITRArchiveRefusalReasonArchiveLegacy             PITRArchiveRefusalReason = "ArchiveLegacy"
	PITRArchiveRefusalReasonArchiveDescriptorMissing  PITRArchiveRefusalReason = "ArchiveDescriptorMissing"
	PITRArchiveRefusalReasonArchiveEncryptionMismatch PITRArchiveRefusalReason = "ArchiveEncryptionMismatch"
	PITRArchiveRefusalReasonArchiveKeyMismatch        PITRArchiveRefusalReason = "ArchiveKeyMismatch"
	PITRArchiveRefusalReasonArchiveVersionUnsupported PITRArchiveRefusalReason = "ArchiveVersionUnsupported"
	PITRArchiveRefusalReasonArchiveStoreUnsupported   PITRArchiveRefusalReason = "ArchiveStoreUnsupported"
	PITRArchiveRefusalReasonArchiveUnsafeHistory      PITRArchiveRefusalReason = "ArchiveUnsafeHistory"
)

// Deployment API unstableReason values: why GET /deploy/v1 reports a group
// unstable, and the reason of a 423 "unstable" lease refusal. Empty means
// stable. A True Degraded, RecoveryPending or Bootstrapping condition also
// gates: unstableReason is then that condition's reason, or its type when
// the reason is empty.
const (
	UnstableReasonDeploymentRevocationPending = "DeploymentRevocationPending"
	UnstableReasonInitializing                = ReasonInitializing
	UnstableReasonNoPrimary                   = ReasonNoPrimary
	UnstableReasonPlannedFailover             = "PlannedFailover"
	UnstableReasonOrderedUpdate               = "OrderedUpdate"
	UnstableReasonRestore                     = "Restore"
	UnstableReasonDragonflyUpgrade            = "DragonflyUpgrade"
	UnstableReasonRestoreInPlace              = "RestoreInPlace"
	UnstableReasonNotReady                    = "NotReady"
	UnstableReasonSiteNotObserved             = "SiteNotObserved"
	UnstableReasonSiteNotReady                = "SiteNotReady"
	UnstableReasonRestoreReclonePending       = ReasonRestoreReclonePending
	UnstableReasonReplicationNotRunning       = "ReplicationNotRunning"

	// The Dragonfly subsystem is in a phase other than Disabled or Ready:
	// "Dragonfly" followed by the DragonflyPhase.
	UnstableReasonDragonflyReconciling            = "Dragonfly" + string(DragonflyPhaseReconciling)
	UnstableReasonDragonflyConfiguringReplication = "Dragonfly" + string(DragonflyPhaseConfiguringReplication)
	UnstableReasonDragonflyDegraded               = "Dragonfly" + string(DragonflyPhaseDegraded)
	UnstableReasonDragonflyPromoting              = "Dragonfly" + string(DragonflyPhasePromoting)

	// A site's status.sites[].sourceConvergenceState is Pending or Blocked.
	UnstableReasonSourceConvergencePending = "SourceConvergence" + string(SourceConvergencePending)
	UnstableReasonSourceConvergenceBlocked = "SourceConvergence" + string(SourceConvergenceBlocked)

	// A site's status.sites[].recoveryState gates.
	UnstableReasonRecoveryInProgress = RecoveryStateInProgress
	UnstableReasonRecoveryBlocked    = RecoveryStateBlocked

	// Lease refusals only (423): another coordinated operation holds the
	// group, the topology has not been persisted or observed yet, or it
	// changed while a lease was being granted.
	UnstableReasonConcurrentOperation        = "ConcurrentOperation"
	UnstableReasonTopologyPersistencePending = "TopologyPersistencePending"
	UnstableReasonTopologyNotObserved        = "TopologyNotObserved"
	UnstableReasonTopologyObservationChanged = "TopologyObservationChanged"
)

// Deployment lease revocation reasons: the reason of a 409 "revoked" lease
// response, of a revoked lease record and of the DeploymentLeaseRevoked
// Event message.
const (
	DeploymentLeaseRevokedByOperator      = "operator_revoked"
	DeploymentLeaseRevokedTopologyChanged = "topology_changed"
)
