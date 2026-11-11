package v1alpha1

// Event reasons. Every reason an Event is recorded with is declared here,
// one const block per section of the generated Event reference in
// site/content/docs/8.observability/2.monitoring.md: internal/apiregistry
// derives every reason the code records, with its Event type and the kinds
// it is recorded on, checks them against this file and generates that page
// (make events-doc). A block's doc comment starts with
// "Events: <section>. On: <kinds>."; each constant's doc comment is its
// description.
//
// Names follow <Subject><PastTenseVerb>[<Qualifier>], the qualifier naming
// the cause: Succeeded and Failed for an operation, Rejected for a user
// request that is not admitted, Refused for an automatic action blocked
// for safety. US spelling, and PITR is the only acronym form. A
// condition-mirror Event repeats the reason of the condition the same
// step sets, so it is declared as that condition-reason constant and keeps
// the condition's noun; mirrors without a doc comment share one row,
// described by their block. Each reason is recorded with one Event type.

// Events: Topology and failover. On: MysqlFailoverGroup.
const (
	// A failover completed and a new primary was promoted.
	EventFailoverExecuted = "FailoverExecuted"
	// Divergent transactions found on old primary after emergency
	// failover.
	EventDataLossDetected = "DataLossDetected"
	// The replica GTID audit found a replica holding transactions the
	// primary lacks; emitted on first detection and when the count
	// changes.
	EventDivergentReplicaDetected = "DivergentReplicaDetected"
	// A replica's divergence report was cleared by a fresh containment
	// proof (after a reclone, or after the primary was made to contain the
	// set).
	EventDivergenceResolved = "DivergenceResolved"
	// A site carrying a divergence report became the primary (an emergency
	// failover with no clean candidate); its extra transactions are now
	// authoritative. Also recorded as the `DivergentPromotion` condition.
	EventDivergentSitePromoted = ReasonDivergentSitePromoted
	// The old primary recovered after a failover and is now replicating
	// from the new primary.
	EventOldPrimaryRecovered = "OldPrimaryRecovered"
	// Both sites are writable (split brain). Also emitted when a second
	// site answers writable before the debounce confirms it; the message
	// names the unconfirmed sites, and fencing waits for the confirmation.
	EventSplitBrainDetected = "SplitBrainDetected"
	// Both sites are read-only (no primary). Not emitted when the cause is
	// a mid-rotation or pending-reclone promotion refusal (that is
	// `KeyringPromotionRefused` or `PromotionRefusedReclonePending`), or
	// while the group is `Initializing` after an operator start.
	EventNoPrimaryDetected = "NoPrimaryDetected"
	// Both sites are unreachable.
	EventTotalLossDetected = "TotalLossDetected"
	// Degraded condition cleared, topology is healthy. Not emitted when
	// the group leaves `Initializing`, which is not a degraded state.
	EventSiteRecovered = "SiteRecovered"
	// Automatic promotion refused: the remaining candidates have not been
	// recloned since a full-instance in-place restore and still hold
	// pre-restore data (`status.restoreReclones.pending`). The group stays
	// without a writable primary until the old primary returns, or until
	// `status.restoreReclones` is removed to accept the stale copy (see
	// [Peer
	// reclones](/docs/backup-and-restore/backup-restore#peer-reclones-after-a-full-instance-restore)).
	EventPromotionRefusedReclonePending = "PromotionRefusedReclonePending"
	// The group has no running topology manager (no polling, status
	// updates or failover) because starting one keeps failing, usually
	// over an operator or TLS Secret that is missing, an operator Secret
	// without a `username` or `password` key, or TLS material that will
	// not load. The message carries the error; the runner retries every
	// 30s. Emitted on the first failure after a running manager was torn
	// down, otherwise once the failure outlives one retry; then at most
	// once per 5 minutes (a changed error) or every 10 minutes
	// (unchanged). The `failed to start topology manager` log line still
	// fires on every attempt.
	EventTopologyManagerStartFailed = "TopologyManagerStartFailed"
	// A topology manager started after a `TopologyManagerStartFailed`
	// Warning; the message gives the number of failed attempts. The
	// operator keeps this history in memory only: after an operator
	// restart a recovery is not announced, so neither Event proves the
	// current state — the `failed to start topology manager` log line,
	// repeated every 30s while the group has no manager, does.
	EventTopologyManagerStarted = "TopologyManagerStarted"
	// An ordered update stopped on a site whose pod is not `Ready`: one it
	// just rolled, or one already on the new pod template. Nothing else is
	// rolled or handed off until that pod has been `Ready` continuously
	// for 30s. The message (also the `Updating` condition's) names the
	// site, pod and cause. Emitted once per stall, not on every retry; a
	// crash loop reads the same (`crash looping, last exit code …`) in
	// every phase of the loop. See [Ordered
	// updates](/docs/operations/failover#ordered-updates).
	EventOrderedUpdateStalled = ReasonOrderedUpdateStalled
	// A valid `bloodraven.shipstream.io/reclone-site` annotation was
	// accepted (an administrator's, or the in-place restore's reclone
	// queue); CLONE INSTANCE will start on the next poll, or once a
	// planned failover, ordered update, promotion or in-place restore
	// holding the sites has finished.
	EventRecloneRequested = "RecloneRequested"
	// The reclone annotation failed the safety interlock, or named a site
	// whose post-restore reclone queue is `Armed` or `Held`. The interlock
	// refuses a value that does not parse as `<site>:confirm=<group>:uid=<group
	// UID>[:gtidPrefix=<prefix>]` (no site, an unknown key, a bare site
	// name), a `confirm=` or `uid=` that does not name this group, an
	// unknown site, and a divergent-GTID prefix that is missing, too short
	// or mismatched, or given for a site with no recorded divergence. The
	// message names the full value to set. The annotation is cleared so the
	// administrator can retry.
	EventRecloneRejected = "RecloneRejected"
	// A clone into a site was refused because an in-place restore holds it
	// (its reclone queue is `Armed` or `Held`, or the topology is frozen).
	// The site keeps its data and its stopped replication.
	EventBootstrapRefusedRestoreHold = "BootstrapRefusedRestoreHold"
	// A site was cloned (a reclone, the fresh-deploy bootstrap, the
	// auto-clone of an empty site, the post-restore reclone queue) and came
	// back with the `server_uuid` it had before. The mysql container's
	// launcher gives every clone recipient a new one by removing
	// `auto.cnf` on the restart that installs the clone; here it did not
	// run (a pod rendered before the launcher, which rolls with the
	// group's pending update, or a `spec.image` that starts mysqld
	// another way) or could not remove the file (its line in the mysql
	// container's log starts with `bloodraven-launcher:`). The message
	// names the site and the `server_uuid`. The site replicates and stays
	// a failover candidate; if it is promoted after losing transactions
	// as a primary it can reissue their GTIDs
	// (`PITRArchiveReissueRiskDetected`). Reclone the site once its pod
	// runs the launcher ([Server identity after a
	// clone](/docs/operations/operations#server-identity-after-a-clone)).
	EventCloneServerUUIDNotRotated = "CloneServerUUIDNotRotated"
	// The group's sidecar image (a `spec.sidecar.image` pin, or the
	// operator's default `BLOODRAVEN_SIDECAR_IMAGE` when it pins none) is
	// tagged with a different release than the running operator. The two share a rendering contract and are
	// supported only at matching releases; on a group with `spec.tls`, an
	// older sidecar cannot reach MySQL at all. Advisory only: the operator
	// never blocks on it, and it is not emitted for digest-pinned or
	// untagged images.
	EventSidecarVersionSkew = "SidecarVersionSkew"
	// Entries of the group or site `spec.mysqlConf` were left out of the
	// rendered my.cnf: a key outside `[A-Za-z0-9_.-]+` or a value with a
	// line break, or a key that would override an operator-owned setting,
	// including through a `loose-`, `skip-`, `disable-`, `enable-` or
	// `maximum-` spelling. The message names the site and the keys; remove
	// them from `spec.mysqlConf`. Admission refuses the malformed keys and
	// values, so only an object stored before that rule reaches the first
	// case. Also emitted, without a site, when
	// `spec.backup.pitr.maxBinlogSize` is not a byte count with an optional
	// `k`, `m` or `g` suffix (likewise refused at admission): my.cnf gets
	// the `100M` default instead.
	EventMysqlConfIgnored = "MysqlConfIgnored"
)

// Events: Refused workloads and metadata. On: MysqlFailoverGroup, MysqlBackupVerification.
//
// The operator refuses to act on something until a human fixes it, and
// does not retry in a loop. Each Event repeats the condition reason it
// sets and is recorded once per finding.
const (
	// A site's Deployment or Dragonfly StatefulSet has a selector other
	// than the one the operator renders (for example an object left from
	// an older release with other label keys). The selector is immutable,
	// so the operator does not update the object and reports
	// `Degraded=True/WorkloadSelectorMismatch`; the message names the
	// object. Delete it so the operator re-creates it (a MySQL site keeps
	// its PVC).
	EventWorkloadSelectorMismatch = ReasonWorkloadSelectorMismatch
	// An internal annotation payload carries a `schemaVersion` this
	// operator does not know (a newer operator wrote it, or it was
	// edited). It proves nothing: on a MysqlFailoverGroup (the
	// planned-failover terminal marker, a deployment lease record) it is
	// reported as `Degraded=True/MetadataVersionUnsupported` and the
	// payload is refused, except that a terminal marker naming a run with
	// outcome `ManualRecoveryRequired` still holds automatic failover. An
	// unreadable lease record refuses lease requests (the deployment API
	// answers `423 unstable` with this reason), the admission of planned
	// operations, and the reopening of admission after a topology change,
	// until the Lease is deleted or an operator that reads it runs; on
	// a MysqlBackupVerification (the verification Job's source record) the
	// run fails with `Verified=False/MetadataVersionUnsupported`.
	EventMetadataVersionUnsupported = ReasonMetadataVersionUnsupported
)

// Events: Planned failover. On: MysqlFailoverGroup.
//
// See [Planned failover](/docs/operations/planned-failover) for the phases,
// outcomes and the automation hold.
const (
	// A planned-failover request was accepted, or a deferred one resumed;
	// the message gives the source, the target, `maxLagWait` and
	// `drainTimeout`. The run enters `Admitting`.
	EventPlannedFailoverStarted = "PlannedFailoverStarted"
	// The planned-failover target is already the active primary, so
	// nothing is done and the request is removed. A deferred run that
	// resolves this way then ends `NotStarted` with `AlreadyActive`, which
	// also emits `PlannedFailoverRejected`.
	EventPlannedFailoverSkipped = "PlannedFailoverSkipped"
	// A planned-failover request was refused and the annotation removed:
	// its value is invalid, the run ended `NotStarted` (the message gives
	// the reason, for example `TargetUnhealthy`, `CooldownActive` or
	// `AdmissionTimeout`), another run is still running, an automation
	// hold stands, or an earlier run's fenced source has not healed.
	// Nothing was fenced; fix the cause and re-annotate. See [Planned
	// failover
	// outcomes](/docs/operations/planned-failover#outcomes-and-reasons).
	EventPlannedFailoverRejected = "PlannedFailoverRejected"
	// Planned switchover waits for cooldown or a deployment hold.
	// `Deferred/DeploymentHold` status names the blocking operation and
	// instance and gives an expiry-based retry time. No action is required
	// while an expected deployment is progressing; investigate an
	// unexpectedly long hold.
	EventPlannedFailoverDeferred = "PlannedFailoverDeferred"
	// A planned failover is waiting to be admitted; the message gives the
	// `admission.waitReason` (for example `PeerUnreadable`,
	// `TargetNotReady`, `UpdateInFlight` or `AuthorityUnsettled`) and its
	// detail. Emitted when the wait reason or its detail changes. Nothing
	// is fenced while it waits, and the run ends `NotStarted` with
	// `AdmissionTimeout` if it is not admitted within 2 minutes.
	EventPlannedFailoverAdmissionDelayed = "PlannedFailoverAdmissionDelayed"
	// The planned failover was granted the claim; the message names the
	// run, source and target. The topology manager defers emergency
	// failover, ordered updates and other topology changes while the run
	// holds it, and fencing the source starts next.
	EventPlannedFailoverClaimGranted = "PlannedFailoverClaimGranted"
	// The source is fenced (`super_read_only=ON`) and the planned failover
	// is closing application connections on it, for at most
	// `drainTimeout`. Writes are paused from here until the target is
	// promoted or the run rolls back.
	EventPlannedFailoverDrainStarted = "PlannedFailoverDrainStarted"
	// The target applied every transaction in the fenced set; the message
	// gives how long that took. The run promotes the target next, after
	// syncing Dragonfly when it is enabled.
	EventPlannedFailoverTargetCaughtUp = "PlannedFailoverTargetCaughtUp"
	// A planned failover promoted its target but the DNS update failed;
	// the message carries the error. The topology manager retries the
	// update, and the run records the `DNSUpdateFailed` warning on its
	// result. Check the DNS provider if clients still resolve the old
	// primary.
	EventPlannedFailoverDNSUpdateFailed = "PlannedFailoverDNSUpdateFailed"
	// A planned failover failed before anything was written to its target
	// and entered `RollingBack`; the message gives the reason (for example
	// `LagTimeout`, `FenceFailed`, `SourceLost`, `DivergentTarget`,
	// `DragonflySyncTimeout` or `DragonflyPromotionFailed`). The operator
	// unfences the source unless another site is writable; the run then
	// ends `Failed`, normally `RolledBack` or `SourceLeftFenced`, with
	// `PlannedFailoverFailed`.
	EventPlannedFailoverRollbackStarted = "PlannedFailoverRollbackStarted"
	// The planned failover completed: the target is the primary and
	// `status.lastFailover` records it. The message lists any warnings the
	// run recorded, such as `DNSUpdateFailed`, or `none`.
	EventPlannedFailoverSucceeded = "PlannedFailoverSucceeded"
	// A planned failover ended `Failed` with outcome `RolledBack`,
	// `SourceLeftFenced` or `PromotedUnconfirmed`; the message gives the
	// outcome, reason and status message. `NotStarted` runs emit
	// `PlannedFailoverRejected` instead. See [Planned failover
	// outcomes](/docs/operations/planned-failover#outcomes-and-reasons).
	EventPlannedFailoverFailed = "PlannedFailoverFailed"
	// `super_read_only=OFF` may have reached the target but the promotion
	// could not be confirmed. Check which site is writable.
	EventPlannedFailoverPromotionConfirmationFailed = "PlannedFailoverPromotionConfirmationFailed"
	// A planned failover that holds its claim has had no topology manager
	// for 30 seconds; it ends after 5 minutes.
	EventPlannedFailoverStalledManagerAbsent = "PlannedFailoverStalledManagerAbsent"
	// A planned failover's status writes failed for 2 minutes and the run
	// ended with the `planned-failover-terminal` annotation, or the
	// operator cannot write the group at all.
	EventPlannedFailoverStatusStalled = "PlannedFailoverStatusStalled"
	// A planned-failover block written by an older operator was refused
	// after an upgrade. A `SourceLeftFenced` refusal (the old run was
	// draining or waiting for lag) or a `PromotedUnconfirmed` refusal (it
	// was promoting or resuming, so the target may be writable) needs a
	// human.
	EventPlannedFailoverLegacyRunRefused = "PlannedFailoverLegacyRunRefused"
	// A planned failover may have written to its target and cannot explain
	// the result. Automation is held (`AutomaticFailoverHeld=True`) until
	// acknowledged. Page on it; see the [hold
	// runbook](/docs/operations/runbooks#planned-failover-automation-hold).
	EventPlannedFailoverManualRecoveryRequired = ReasonPlannedFailoverManualRecoveryRequired
	// Repeated every 10 minutes while an automation hold stands.
	EventPlannedFailoverHoldPersisted = "PlannedFailoverHoldPersisted"
	// The hold was acknowledged and cleared; automation resumes.
	EventPlannedFailoverHoldAcknowledged = ReasonPlannedFailoverHoldAcknowledged
	// The `acknowledge-planned-failover-hold` annotation resumed nothing:
	// it names a run other than the held one, no automation hold is
	// standing for the run it names, or the planned-failover claim is held
	// for another run. When no hold stands the annotation is removed
	// anyway.
	EventPlannedFailoverHoldAcknowledgmentIgnored = "PlannedFailoverHoldAcknowledgmentIgnored"
)

// Events: Deployment coordination. On: MysqlFailoverGroup.
const (
	// The group's deployment authority was revoked by a topology change
	// (`TopologyChanged`) or an explicit operator request
	// (`OperatorRevoked`). Stop the affected deployment clients; uncertain
	// post-DDL state needs manual recovery. See [deployment
	// coordination](/docs/operations/operations#deployment-coordination).
	EventDeploymentLeaseRevoked = "DeploymentLeaseRevoked"
)

// Events: Dragonfly. On: MysqlFailoverGroup.
const (
	// The operator is promoting a Dragonfly replica: in a planned
	// failover's `PromotingDragonfly` phase, because the MySQL active site
	// differs from the Dragonfly master, or because the Dragonfly master
	// is unreachable and exactly one healthy replica can take over.
	EventDragonflyPromotionStarted = "DragonflyPromotionStarted"
	// A Dragonfly target was promoted with `REPLTAKEOVER` and the active
	// Service now points at it. A promotion that fell back to `REPLICAOF
	// NO ONE` emits `DragonflySessionsLost` instead.
	EventDragonflyPromotionSucceeded = "DragonflyPromotionSucceeded"
	// A Dragonfly target was not promoted: it was unreachable,
	// `REPLTAKEOVER` failed or its reply was lost, it already reported
	// master before the takeover, the Dragonfly master was neither the
	// run's source nor its target, or `PromotingDragonfly` passed its
	// deadline. The source's traffic label is restored; a planned failover
	// then continues best-effort or rolls back, as `onSyncTimeout` says.
	// Also emitted when the old master could not be demoted after a
	// promotion: it is kept out of the active Service and needs manual
	// intervention.
	EventDragonflyPromotionFailed = "DragonflyPromotionFailed"
	// A planned failover reached `PromotingDragonfly` while another
	// Dragonfly promotion, usually an emergency one, was still running.
	// MySQL writes stay paused on the fenced source until it finishes, at
	// most about 40 seconds. Emitted once per wait;
	// `status.plannedFailover.dragonfly.message` says the same.
	EventDragonflyPromotionDeferred = "DragonflyPromotionDeferred"
	// A planned failover's target Dragonfly replica did not catch up
	// within `spec.dragonfly.plannedFailover.maxSyncWait`. With
	// `onSyncTimeout: Proceed` (the default) the takeover goes ahead and
	// `status.plannedFailover.dragonfly.sessions` records `Lost`; with
	// `Fail` the run rolls back before MySQL is promoted.
	EventDragonflySyncTimedOut = "DragonflySyncTimedOut"
	// An emergency Dragonfly promotion succeeded without `REPLTAKEOVER`:
	// it fell back to `REPLICAOF NO ONE` after the takeover failed or was
	// skipped because the old master was unreachable, or its reply was
	// lost and the target was then observed as master. The target is
	// writable, but cached data and sessions may be lost.
	EventDragonflySessionsLost = "DragonflySessionsLost"
	// A Dragonfly site other than the active one reports the master role.
	// The operator reattaches it as a replica, which discards what it
	// holds, only if no replica is attached to it (`connected_slaves=0`),
	// and not while it holds keys and the active site holds none;
	// otherwise it stays out of the active Service until you decide
	// whether its data can be discarded. Repeated at most every 5 minutes
	// per site.
	EventDragonflyStaleMasterDetected = "DragonflyStaleMasterDetected"
	// A stale Dragonfly master on a non-active site with no replica
	// attached (`connected_slaves=0`) was reattached as a replica of the
	// active master.
	EventDragonflyOldSiteReconfigured = "DragonflyOldSiteReconfigured"
	// Dragonfly advertises `REPLTAKEOVER` again after a
	// `DragonflyReplTakeoverUnsupported` Warning, typically after an image
	// change.
	EventDragonflyReplTakeoverSupported = "DragonflyReplTakeoverSupported"
	// A Dragonfly site does not advertise `REPLTAKEOVER`. Emergency
	// promotions fall back to `REPLICAOF NO ONE`, which does not preserve
	// session continuity. Emitted when the capability probe changes to
	// unsupported; use a Dragonfly image that supports the command.
	EventDragonflyReplTakeoverUnsupported = "DragonflyReplTakeoverUnsupported"
	// A snapshot-restore upgrade run started: `spec.dragonfly.upgradeStrategy`
	// is `SnapshotRestore` and a site's live StatefulSet runs an image other
	// than `spec.dragonfly.image`. `status.dragonfly.upgrade` records it;
	// the message gives the source and target images. Dragonfly promotions
	// and replication changes pause until the run ends.
	EventDragonflyUpgradeStarted = "DragonflyUpgradeStarted"
	// The active Dragonfly site was removed from the active Service and
	// the operator is saving a snapshot. Cache and session traffic is
	// unavailable until the upgrade completes or fails.
	EventDragonflyUpgradeSnapshotStarted = "DragonflyUpgradeSnapshotStarted"
	// `SAVE` completed on the active Dragonfly master into
	// `spec.dragonfly.snapshot.dir`. The active pod is restarted on the
	// target image next and restores from the snapshot.
	EventDragonflyUpgradeSnapshotCompleted = "DragonflyUpgradeSnapshotCompleted"
	// Every Dragonfly site runs the target image and the active site
	// serves traffic again with its replicas attached.
	EventDragonflyUpgradeSucceeded = "DragonflyUpgradeSucceeded"
	// A Dragonfly snapshot-restore upgrade failed, once per run; the
	// message gives the reason. On `DragonflySnapshotDialFailed`,
	// `DragonflySnapshotSaveFailed`, `UnknownPhase`,
	// `WorkloadSelectorMismatch`, `ActiveWorkloadDeleted`, `SpecChanged`,
	// `DragonflyDisabled` or `TimedOut` the operator restores the active
	// site's traffic label, except that an active the run restarted and
	// that is still loading its snapshot keeps it off until it answers as
	// loaded (the message says so); on `ActiveWorkloadDeleted` it re-creates the
	// active site's StatefulSet. On `ActiveSiteChanged` the upgrade was
	// abandoned because MySQL failed over or a promotion moved the
	// Dragonfly master. `SpecChanged`: `spec.dragonfly` changed during the
	// run, and the operator decides again for the new spec. `TimedOut`: the
	// run had not finished an hour after it started. The run is not
	// restarted while `spec.dragonfly` is unchanged: change it to retry.
	// When every site still runs one image, setting `spec.dragonfly.image`
	// back to it and then forward again also retries; when the sites run
	// different images, that starts a run back to the old image instead.
	EventDragonflyUpgradeFailed = "DragonflyUpgradeFailed"
	// A snapshot-restore upgrade run is needed but waits: `status.activeSite`
	// is empty, a site's StatefulSet is refused for its selector
	// (`WorkloadSelectorMismatch`), a planned failover run or automation
	// hold owns the sites, the Dragonfly master is not on the MySQL active
	// site, or the sites do not run the other `spec.dragonfly` settings
	// (snapshot flags, credentials, service account) yet. Emitted when the
	// cause changes; the run is retried every 10 seconds and starts once
	// the cause clears. No site's image moves meanwhile; the render rolls
	// out the other settings, keeping each site's image.
	EventDragonflyUpgradeDeferred = "DragonflyUpgradeDeferred"
)

// Events: Encryption at rest. On: MysqlFailoverGroup.
const (
	// `spec.encryptionAtRest.enabled` was turned on for a group that is
	// already serving, with `spec.encryptionAtRest.existingData: Refuse`
	// (the default). Existing tablespaces would stay plaintext, so
	// `EncryptionAtRestReady` is `False` with `AdoptionRefused`.
	// `enabled` cannot be turned back off. Restore a backup into a new
	// encrypted group and move the applications to it, or set
	// `existingData: LeavePlaintext` to accept partial coverage.
	EventEncryptionAdoptionRefused = "EncryptionAdoptionRefused"
	// A site's keyring moved to another phase (`Pending`, `Unsealed`,
	// `Escrowed`, `Sealed` or `Failed`); the message gives the old and new
	// phase and the site's status message. A move to `Failed` comes with
	// its own Warning, such as `KeyringEscrowTimedOut` or
	// `KeyringEscrowMissing`.
	EventKeyringPhaseChanged = "KeyringPhaseChanged"
	// A site was unsealed for a `CLONE INSTANCE`: its pod rolls onto a
	// writable keyring so the clone can rewrap the donor's tablespace
	// keys. The site is escrowed and sealed again after the clone.
	EventKeyringUnsealed = "KeyringUnsealed"
	// A site's keyring was not escrowed within `keyring.escrowTimeout`
	// (default `10m`) of being unsealed, and the site goes `Failed`. The
	// site stays unsealed and the operator keeps retrying; the message
	// carries the last reason. See [Keyring not
	// sealed](/docs/operations/runbooks#keyring-not-sealed).
	EventKeyringEscrowTimedOut = "KeyringEscrowTimedOut"
	// A sealed site's escrow Secret, its only copy of its keys, no longer
	// exists, so the site cannot restart. The site goes `Failed`. Do not
	// restart or delete the pod: recreate the Secret from it following
	// [Keyring escrow
	// lost](/docs/operations/runbooks#keyring-escrow-lost).
	EventKeyringEscrowMissing = "KeyringEscrowMissing"
	// A sealed site's escrow Secret, its only copy of its keys, no longer
	// matches the recorded digest. The site goes `Failed`. Do not restart
	// the pod; follow [Keyring escrow
	// lost](/docs/operations/runbooks#keyring-escrow-lost).
	EventKeyringEscrowCorrupt = "KeyringEscrowCorrupt"
	// A site's pod runs a keyring that does not match its escrow Secret,
	// either when it was about to be declared `Sealed` or later, when the
	// pod may be projecting a superseded version. The site goes `Failed`;
	// tablespaces rewrapped under a newer master key will not decrypt.
	// Check which escrow version the site's Deployment projects.
	EventKeyringDigestMismatch = "KeyringDigestMismatch"
	// MySQL reports a writable keyring component on a site the operator
	// considers `Sealed`, so the site is not protected as reported. The
	// site goes `Failed`; check the site's keyring rendering.
	EventKeyringNotReadOnly = "KeyringNotReadOnly"
	// A `Failed` site returned to `Sealed`: its escrow Secret is present
	// and matches the recorded digest, and MySQL confirms it runs that
	// keyring.
	EventKeyringRecovered = "KeyringRecovered"
	// A `rotate-keyring` request was refused. For the active primary the
	// annotation is removed: rotate the replicas, run a planned failover,
	// then rotate the former primary. Any other refusal (an ordered
	// update, planned failover or promotion in flight, an unsettled
	// topology, no running topology manager) keeps the annotation and
	// retries every 10 seconds, with one Event per reason.
	EventKeyringRotationRejected = "KeyringRotationRejected"
	// Emergency failover skipped a site whose `UnsealReason` is `Rotation`
	// and promoted another candidate.
	EventKeyringPromotionSkipped = "KeyringPromotionSkipped"
	// Emergency failover refused: every remaining candidate is
	// mid-keyring-rotation. The group stays without a writable primary
	// until the site is `Sealed`.
	EventKeyringPromotionRefused = "KeyringPromotionRefused"
)

// Events: Restore. On: MysqlFailoverGroup.
const (
	// Restore Job created.
	EventRestoreStarted = "RestoreStarted"
	// Restore completed (with or without PITR). Not emitted for a partial
	// outcome.
	EventRestoreSucceeded = "RestoreSucceeded"
	// Restore Job failed. Emitted once per Job; PITR failures carry a
	// stable reason in `status.restore.reason`. For `initFromBackup` the
	// message also states the recovery: the group stays out of service (no
	// DNS record, no Service endpoints) and nothing is cloned. If the load
	// had started, empty the target: delete and recreate the group, or
	// delete the group's MySQL pods and PVCs together, wait until the
	// operator has re-created every claim `Bound` and every pod `Running`,
	// then delete the restore Job. `Ready=False` with reason
	// `InitFromBackupFailed` says the same.
	EventRestoreFailed = "RestoreFailed"
	// The active site an `initFromBackup` restore would load into is
	// unreachable or in an unknown state; the restore waits in `Pending`.
	EventRestoreTargetUnavailable = "RestoreTargetUnavailable"
	// Failed to build restore Job spec, or the `backupRef` backup could
	// not be held for the restore (it is being deleted, or was replaced by
	// another backup of the same name after the restore pinned it; the
	// message says which). PITR render refusals
	// (`PITRArchiveDestinationShared` when
	// `pointInTime.archiveProfileName` resolves to the group's own active
	// PITR destination, `PITRTargetInvalid` for an unparsable or
	// fractional target, a missing or unknown `archiveProfileName`, or an
	// `excludeGtids` entry without a server UUID or longer than 64 KiB
	// canonical, `PITRBackupIdentityUnknown` for a backup with no recorded
	// source identity (no stamp) or a direct S3/PVC source at the root of
	// its store, `PITRBackupFiltered` for a filtered load) also set
	// `status.restore.reason`. `PITRBackupAfterTarget` is not raised here:
	// the restore Job checks the target against the dump itself. A cause
	// inside `spec.initFromBackup`, which is fixed at creation, names the
	// recreate remedy (delete the group and create it again with the
	// corrected block); a `restoreInPlace` cause names the edit.
	EventRestoreBuildFailed = "RestoreBuildFailed"
)

// Events: Point-in-time replay results. On: MysqlFailoverGroup, MysqlBackupVerification.
const (
	// `allowPartial` replayed to the archive head because nothing is
	// archived at or after the requested target (`pointInTime.outcome:
	// Partial`: `status.restore.provenance.pointInTime` on a group,
	// `status.pointInTime` on a verification). Names the requested target
	// and the newest replayed commit time.
	EventPITRReplayTruncated = "PITRReplayTruncated"
	// The replay excluded N archived transactions committed before the
	// target that are outside the target history
	// (`pointInTime.excludedDivergentTransactionCount`).
	EventPITRDivergentHistoryExcluded = "PITRDivergentHistoryExcluded"
	// An excluded transaction on the target history precedes a replayed
	// one: "excluded `<set>` precedes replayed transactions on the target
	// history; their GTIDs were skipped but later row images may carry
	// their effects; the result is not certified for them".
	EventPITROnHistoryTransactionsExcluded = "PITROnHistoryTransactionsExcluded"
)

// Events: In-place restore. On: MysqlFailoverGroup.
const (
	// A new in-place restore was accepted and entered `Preflight`; the
	// message gives the scope and confirm.
	EventRestoreInPlaceStarted = "RestoreInPlaceStarted"
	// The in-place restore Job was created against the active site; the
	// run is `Restoring`.
	EventRestoreInPlaceJobCreated = "RestoreInPlaceJobCreated"
	// The restore Job succeeded and the load is complete; the operator
	// lifts the fence (`Resuming`) and then marks the run `Succeeded`.
	EventRestoreInPlaceLoaded = "RestoreInPlaceLoaded"
	// The in-place restore finished `Succeeded`, and
	// `status.restoreInPlace.confirmTokenUsed` records the confirm it ran
	// with. After a full-instance restore the peers are recloned from the
	// restored primary next.
	EventRestoreInPlaceSucceeded = "RestoreInPlaceSucceeded"
	// The run's restore Job failed; the run ends `Failed` with the Job's
	// failure in the message. The confirm the Job ran with is used
	// (`confirmTokenUsed`), so bump `confirm` to retry. The fence and
	// topology freeze are released; for the peers of a full-instance
	// restore see `RestoreReclonesHeld` or `RestoreReclonesDiscarded`.
	EventRestoreInPlaceFailed = "RestoreInPlaceFailed"
	// The run ends `Failed` with nothing restored because the request was
	// refused: the spec is invalid (for example more than one
	// `loadOptions.includeSchemas` entry), the operator account lacks
	// privileges a full-instance restore needs (the message names them;
	// re-checked about once a minute), or the scope changed before the Job
	// was created. The confirm is not consumed, so a fixed spec runs with
	// the same `confirm`.
	EventRestoreInPlaceRejected = "RestoreInPlaceRejected"
	// The restore credentials Secret or the restore Job spec could not be
	// built (for example a `backupRef` whose profile is gone or moved to
	// another backend), or the `backupRef` backup could not be held for
	// the run (it is gone, not `Succeeded`, being deleted, or was replaced
	// by another backup of the same name; checked in `Preflight`, before
	// anything is fenced, and again before the Job is created). The run
	// ends `Failed` with nothing restored; the confirm counts as used, so
	// bump `confirm` to retry.
	EventRestoreInPlaceBuildFailed = "RestoreInPlaceBuildFailed"
	// `spec.restoreInPlace` was removed before the restore Job was
	// created; the run ends `Failed` and nothing was restored.
	EventRestoreInPlaceCanceled = "RestoreInPlaceCanceled"
	// `spec.restoreInPlace` was removed while its restore Job runs; the
	// fence and guards hold until the Job finishes.
	EventRestoreInPlaceSpecRemoved = "RestoreInPlaceSpecRemoved"
	// The run's restore Job disappeared before its outcome was seen; the
	// run ends `Failed` with the outcome unknown.
	EventRestoreInPlaceJobLost = "RestoreInPlaceJobLost"
	// A Job with the in-place restore name from an earlier request is
	// still running, so this run waits for it to finish instead of
	// deleting a destructive restore in flight. Repeated until it
	// finishes.
	EventRestoreInPlaceJobBusy = "RestoreInPlaceJobBusy"
	// A finished Job with the in-place restore name, left by an earlier
	// request, was deleted so this run can create its own.
	EventRestoreInPlaceStaleJobRemoved = "RestoreInPlaceStaleJobRemoved"
	// `spec.restoreInPlace.confirm` advanced before the restore Job was
	// created; the Job runs under the new confirm, recorded in
	// `status.restoreInPlace.runConfirm`.
	EventRestoreInPlaceConfirmRebound = "RestoreInPlaceConfirmRebound"
	// `spec.restoreInPlace.confirm` changed while the run's restore Job is
	// running; the Job is never interrupted. A newer confirm starts its
	// own run once this one is terminal, and one that does not advance
	// past it starts nothing. Repeated while the Job runs.
	EventRestoreInPlaceConfirmDeferred = "RestoreInPlaceConfirmDeferred"
	// After an in-place restore ended, `spec.restoreInPlace.confirm` was
	// changed to a value that is not an RFC 3339 timestamp, so no new run
	// starts and the previous terminal status stays. Set `confirm` to a
	// valid timestamp newer than `status.restoreInPlace.confirmTokenUsed`.
	// Admission refuses such a value, so only an object stored before that
	// rule reaches it.
	EventRestoreInPlaceConfirmIgnored = "RestoreInPlaceConfirmIgnored"
	// The restore credentials Secret could not be refreshed while the
	// run's restore Job exists. The Job is left running; the refresh is
	// retried on the next pass.
	EventRestoreInPlaceCredsRefreshFailed = "RestoreInPlaceCredsRefreshFailed"
	// A new in-place restore (bumped `confirm`) has not started because
	// the previous restore's reclone queue is not settled yet: its Job's
	// pods may still be running, or the primary is briefly unreadable (at
	// most ten minutes). `status.restoreInPlace` still shows the previous
	// run.
	EventRestoreInPlaceDeferredPreviousQueue = "RestoreInPlaceDeferredPreviousQueue"
	// A full-instance in-place restore set `offline_mode=ON` on the
	// primary and verified that no client session from before the fence
	// remains; the restore Job is created next.
	EventRestoreOfflineFenceApplied = "RestoreOfflineFenceApplied"
	// The operator account lacks `SYSTEM_USER`, so it could not kill the
	// named `SYSTEM_USER` accounts' sessions when the fence went up. They
	// stay connected during the restore.
	EventRestoreOfflineFenceSessionsKept = "RestoreOfflineFenceSessionsKept"
	// The restore finished and `offline_mode` on the primary is back to
	// its prior value.
	EventRestoreOfflineFenceReleased = "RestoreOfflineFenceReleased"
	// The operator could not put `offline_mode` back after the restore; it
	// retries. Applications without `CONNECTION_ADMIN` cannot connect
	// until it succeeds.
	EventRestoreOfflineFenceReleaseFailed = "RestoreOfflineFenceReleaseFailed"
	// A full-instance restore stopped the replication receiver of every
	// queued peer in Fencing, before creating its Job.
	EventRestorePeerReplicationStopped = "RestorePeerReplicationStopped"
	// A peer's receiver was stopped before it received every transaction
	// the primary committed before the fence (it was connecting, or did
	// not catch up within 30 s). The message names the missing GTIDs; the
	// peer keeps its pre-restore data without them.
	EventRestorePeerReplicationStoppedBehind = "RestorePeerReplicationStoppedBehind"
	// The receivers of the peers could not be verified stopped within two
	// minutes (unreachable peer, refused `STOP`). The run ends `Failed`
	// without a Job, nothing was restored, the confirm is consumed, and
	// the peers resume.
	EventRestoreInPlacePeerStopFailed = "RestoreInPlacePeerStopFailed"
)

// Events: Peer reclones after an in-place restore. On: MysqlFailoverGroup.
const (
	// The in-place restore's reclone queue requested the reclone of its
	// next peer.
	EventRestoreRecloneRequested = "RestoreRecloneRequested"
	// A newer full-instance restore was accepted; the old queue's reclone
	// request that had not started was withdrawn.
	EventRestoreRecloneWithdrawn = "RestoreRecloneWithdrawn"
	// A queued peer was recloned from the restored primary and left the
	// queue.
	EventRestoreRecloneSucceeded = "RestoreRecloneSucceeded"
	// A queued peer's reclone failed, or did not start within ten minutes.
	// The site stays queued; the other queued sites go first and it is
	// retried after a minute.
	EventRestoreRecloneFailed = "RestoreRecloneFailed"
	// Every peer of the in-place restore has been recloned;
	// `status.restoreReclones` is cleared.
	EventRestoreReclonesDrained = "RestoreReclonesDrained"
	// The in-place restore that armed the queue did not succeed and left
	// the primary unchanged: it never created its Job, or every pod of its
	// Job terminated and the primary's `gtid_executed` still equals
	// `status.restoreReclones.fencedGtidExecuted`. The peers resume
	// replicating and are eligible for promotion again.
	EventRestoreReclonesDiscarded = "RestoreReclonesDiscarded"
	// A restore that had replaced an earlier restore's reclone queue ended
	// without changing the primary. The earlier queue drains again.
	EventRestoreReclonesRestored = "RestoreReclonesRestored"
	// A full-instance restore ran its Job on the primary, failed, and the
	// primary changed (or could not be compared). Every peer, plus any
	// site of a replaced earlier queue, is held: replication receiver
	// stopped, pre-restore data kept, excluded from promotion and the
	// reader Services, nothing cloned. An administrator restores the
	// primary (for example from a backup of a held site), sets
	// `status.restoreReclones.phase` to `Draining`, or removes the queue
	// (which resumes their replication from the current primary).
	EventRestoreReclonesHeld = "RestoreReclonesHeld"
	// A queued site became the primary (a manual promotion; a planned
	// failover refuses a queued target) before it was recloned. It keeps
	// its pre-restore data and the rest of the queue is dropped.
	EventRestoreReclonesAbandoned = "RestoreReclonesAbandoned"
)

// Events: PITR archiving. On: MysqlFailoverGroup.
//
// The archive identity reasons are refusals, not transient errors: they do not
// clear by themselves. They are listed in [Archive
// identity](/docs/backup-and-restore/backup-restore#archive-identity).
//
// Divergent rows in a site's manifest (archived transactions its server no
// longer has, for example the tail an old primary archived before it was
// recloned) are evidence for restores, not an archiver error. They fire no
// Event and leave the condition `True`; its message lists them, for example
// `Every site reports a running archiver with a ready store and no error;
// iad: 1 divergent row (...)`. Only a server that could issue those GTIDs
// again is an error: its sidecar reports `reissuableGtids`, and the condition
// turns `False` with reason `ArchiverError` ([Reissue
// risk](/docs/backup-and-restore/pitr-history-repair#reissue-risk)).
//
// A `PITRArchiveNotReady` → `PITRArchiveRecovered` pair is expected, not an
// incident, when:
//
//   - A pod takes more than 60 s to come back during a rollout or node move
//     (`SidecarUnreachable`), for example while an RWO volume detaches and
//     reattaches.
//   - A new group's pods, or a bootstrapped site, take more than 5 minutes to
//     start answering (`SidecarUnreachable`).
//   - PITR is enabled with `updateStrategy.type: Recreate`, or a pod does not roll
//     onto the PITR configuration through the ordered update (`ArchiverDisabled`).
//     With the default ordered update, sites waiting for it report
//     `Unknown`/`AwaitingRollout` with no Event, for up to 30 minutes; a rollout
//     still pending after that is `ArchiverDisabled`, as is a PITR spec whose
//     `profileName` renders no archiver.
//   - MySQL takes more than about a minute to accept connections after a pod
//     start, for example during a long InnoDB crash recovery (`ArchiverError`,
//     `role check: ...`).
//
// Two failed archiver scans in a row (about 2 minutes at the default 60 s scan
// interval) also produce a pair if the third scan succeeds. Alert on a
// `PITRArchiveNotReady` without a following `PITRArchiveRecovered` within,
// say, 10 minutes, or on the condition itself.
const (
	// The `PITRArchiveReady` condition entered `False` (from absent,
	// `Unknown` or `True`). The message reads "PITRArchiveReady is False
	// (<reason>): <message>", with the reason (`ArchiveStoreInitFailed`,
	// an archive identity refusal such as `ArchiveLegacy`,
	// `ArchiveForeign`, `ArchiveCopy`, `ArchiveDescriptorMissing`,
	// `ArchiveEncryptionMismatch`, `ArchiveKeyMismatch`,
	// `ArchiveVersionUnsupported`, `ArchiveStoreUnsupported` or
	// `ArchiveUnsafeHistory`, then `ArchiverError`, `SidecarOutdated`,
	// `ArchiverDisabled`, `SidecarUnreachable`) and every affected site.
	// Not repeated while the condition stays `False`, even if the reason
	// changes, and not re-fired by an operator restart.
	EventPITRArchiveNotReady = "PITRArchiveNotReady"
	// The `PITRArchiveReady` condition returned from `False` to `True`
	// after 2 clean poll rounds: "PITRArchiveReady is True again: every
	// site reports a running archiver with a ready store and no error".
	EventPITRArchiveRecovered = "PITRArchiveRecovered"
	// Edge-triggered on entering `PITRArchiveReady=False` /
	// `ArchiveUnsafeHistory`: a site persisted its history-integrity
	// marker after an out-of-band change (`SET GLOBAL`, a hand-edited
	// my.cnf): such options in `spec.mysqlConf` are refused instead
	// (`PITRMysqlConfRefused`). Fires in addition to
	// `PITRArchiveNotReady`. The marker removes every restore target that
	// needs that site's bytes, including targets before the violation.
	// Remedy: undo the change, new prefix plus full backup ([Unsafe
	// history
	// marker](/docs/backup-and-restore/pitr-history-repair#unsafe-history-marker)).
	EventPITRArchiveUnsafeHistoryDetected = "PITRArchiveUnsafeHistoryDetected"
	// The active site changed to a site whose server could reissue GTIDs
	// its archive already holds: its manifest has divergent or quarantined
	// rows with transactions the server no longer has, issued under the
	// `server_uuid` it still has (a data directory restored or reset
	// without a clone, or a clone by a release that kept the recipient's
	// `server_uuid`). The message lists them; a sidecar that has not
	// reported that verdict warns on the row counts instead. Its new
	// writes reuse those GTIDs: start a new archive prefix and take a full
	// backup ([Reissue
	// risk](/docs/backup-and-restore/pitr-history-repair#reissue-risk)).
	EventPITRArchiveReissueRiskDetected = "PITRArchiveReissueRiskDetected"
	// A reclone started although the recipient's binary log tail was not
	// confirmed archived: before CLONE wipes a site's binary logs the
	// operator rotates them and waits up to 2 minutes for the site's
	// sidecar to archive the sealed tail. The message names the site and
	// why the wait ended (the sidecar did not answer, the rotation was
	// not acknowledged, the archiver was still uploading or could not
	// upload). A point-in-time restore whose window needs transactions
	// only that tail held refuses `PITRArchiveGap`. Not emitted when PITR
	// is not enabled.
	EventPITRArchiveTailNotArchived = "PITRArchiveTailNotArchived"
	// While PITR is enabled, group or site `spec.mysqlConf` sets an option
	// that breaks PITR replay (a replication or binlog filter,
	// `binlog_format≠ROW`, `binlog_row_image≠FULL`). The option is left
	// out of the rendered my.cnf and `PITRMysqlConfAccepted` is `False` /
	// `PITRUnsafeSettingRefused`; fires once per refused set. Remove the
	// option from `spec.mysqlConf`.
	EventPITRMysqlConfRefused = "PITRMysqlConfRefused"
)

// Events: PITR archive encryption bindings. On: MysqlFailoverGroup.
//
// Events for the group's [archive encryption
// bindings](/docs/backup-and-restore/backup-encryption#archive-encryption-bindings)
// (condition `PITRArchiveBindingReady`).
const (
	// A binding became `Bound` by sidecar proof or a check Job. Not
	// emitted when status-loss recovery rebuilds a binding; watch for the
	// `pitr archive binding changed` log line with `cause=recovery`
	// instead ([log schema](/docs/observability/log-schema)).
	EventPITRArchiveBound = "PITRArchiveBound"
	// A binding was released and its key snapshots deleted.
	EventPITRArchiveBindingReleased = "PITRArchiveBindingReleased"
	// A profile in archive use has encryption that differs from its
	// `Bound` binding. Pods are unchanged. Edge-triggered.
	EventPITRArchiveEncryptionChangeRefused = "PITRArchiveEncryptionChangeRefused"
	// A profile in archive use has passphrase bytes that differ from its
	// `Bound` binding's key snapshot (rotation, alias, rename while PITR
	// was off). Pods are unchanged. Edge-triggered.
	EventPITRArchiveKeyChangeRefused = "PITRArchiveKeyChangeRefused"
	// A binding was refused (check Job or correlated sidecar refusal), a
	// write target is not eligible (`ArchiveForeign`/`ArchiveCopy`), or
	// site Deployments without a decodable render annotation are deferred
	// until a check verifies (the message names only those: "Deployments
	// `<names>` are not updated until the archive accepts a key: ...").
	// Check refusals name the role: "... reported by the writer check of
	// ..." or "... reported by the reader check of ...".
	EventPITRArchiveBindingRefused = "PITRArchiveBindingRefused"
	// The profile's Secret or key is gone; the snapshot still serves.
	EventPITRArchiveKeySourceRemoved = "PITRArchiveKeySourceRemoved"
	// A deleted key snapshot was re-created under the same name from
	// matching live bytes; no roll. Not emitted when another object of
	// that name appeared first.
	EventPITRArchiveKeySnapshotRecreated = "PITRArchiveKeySnapshotRecreated"
	// A `Bound` binding's key snapshot is missing and cannot be reproduced
	// ("Secret `<s>` held the accepted key of archive `<id>`: restore it
	// from a backup of it, ..."), or is present but no longer passes the
	// check of the accepted key ("... do not delete it, it may hold the
	// only copy of that key; ..."). MySQL keeps running; the archiver
	// fails closed.
	EventPITRArchiveKeyMaterialLost = "PITRArchiveKeyMaterialLost"
	// A key snapshot's acceptance annotation is malformed; it is never
	// used for recovery and never deleted.
	EventPITRArchiveKeySnapshotReadFailed = "PITRArchiveKeySnapshotReadFailed"
	// A release annotation names a binding still in use; retried every
	// minute.
	EventPITRArchiveBindingReleaseRefused = "PITRArchiveBindingReleaseRefused"
	// A release annotation is malformed or its `confirm` is not the group
	// name; the annotation is removed.
	EventPITRArchiveBindingReleaseRejected = "PITRArchiveBindingReleaseRejected"
	// A destination in use gets no binding because 16 entries that stay
	// (`Bound`, or used by a target) exist, or status-loss recovery found
	// more accepted snapshots than free slots.
	EventPITRArchiveBindingLimitReached = "PITRArchiveBindingLimitReached"
)

// Events: Backup and verification scheduling. On: MysqlFailoverGroup.
const (
	// Schedule references an unknown backup profile. Admission refuses
	// such a schedule, so only an object stored before that rule reaches
	// it.
	EventBackupScheduleInvalid = "BackupScheduleInvalid"
	// The schedule's profile is
	// [read-only](/docs/backup-and-restore/backup-encryption#read-only-profiles)
	// (an unfinished `initFromBackup` archive profile, or a destination
	// whose binding is foreign or refused `ArchiveForeign`/`ArchiveCopy`,
	// or the archive profile of a succeeded `initFromBackup` restore whose
	// destination has no binding): no CronJob is rendered. Once per
	// change.
	EventBackupScheduleRefused = "BackupScheduleRefused"
	// Retry scheduled for a failed backup.
	EventBackupRetryScheduled = "BackupRetryScheduled"
	// Existing deterministic retry name has different lineage; emitted
	// when the schedule's `retryConflict` status marker changes. Only that
	// retry is skipped while other schedules and rollups continue.
	EventBackupRetryConflict = "BackupRetryConflict"
	// An encrypted backup profile's passphrase Secret is missing, lacks
	// the configured key (default `passphrase`), holds an empty value, or
	// holds a passphrase shorter than 32 bytes; the message names the
	// profile and the problem. While the Secret or key is missing, backups
	// of the profile wait `Pending` with `PassphraseSecretMissing`. A
	// short passphrase only warns: backups keep using it, so replace it
	// with a long random value.
	EventBackupEncryptionInvalid = "BackupEncryptionInvalid"
	// `spec.backup.pitr.enabled` is `true` but
	// `spec.backup.pitr.profileName` is empty or names no entry in
	// `spec.backup.profiles`. Binlog archiving stays disabled and
	// `PITRArchiveReady` reports `ArchiverDisabled`; set `profileName` to
	// an existing profile. Admission refuses both, so only an object
	// stored before that rule reaches it.
	EventBackupPITRInvalid = "BackupPITRInvalid"
	// A backup profile sets `verification.enabled: true` but leaves
	// `verification.schedule` empty, so no verification CronJob is created
	// for it. Set a schedule or disable verification. Admission refuses
	// it, so only an object stored before that rule reaches it.
	EventVerificationScheduleInvalid = "VerificationScheduleInvalid"
)

// Events: Credentials and secrets. On: MysqlFailoverGroup.
const (
	// Failed to reconcile MySQL users.
	EventCredentialReconcileFailed = "CredentialReconcileFailed"
	// Referenced Secret not found.
	EventSecretNotFound = "SecretNotFound"
	// A credential Secret is missing a required key, has an empty
	// `username` or `password`, sets only one of `MYSQL_REPLICATION_USER` and
	// `MYSQL_REPLICATION_PASSWORD`, or names the operator account in
	// `MYSQL_REPLICATION_USER` with a different `MYSQL_REPLICATION_PASSWORD`;
	// the message names the Secret and key.
	EventSecretMissingKey = "SecretMissingKey"
	// A `credentialsSecretName` the group references for S3 storage is
	// missing, or lacks a non-empty `AWS_ACCESS_KEY_ID` or
	// `AWS_SECRET_ACCESS_KEY`; the message names each Secret and key.
	// `S3CredentialsReady` is `False`, and backup, restore and cleanup
	// Jobs fail because they do not fall back to ambient cloud
	// credentials. Add both keys to the Secret. The
	// `initFromBackup.source.s3` Secret is checked only until the
	// bootstrap restore succeeds; after that it can be deleted.
	EventS3CredentialsIncomplete = "S3CredentialsIncomplete"
)

// Events: Operator lifecycle and license. On: MysqlFailoverGroup.
const (
	// Graceful shutdown started or completed.
	EventGracefulShutdown = "GracefulShutdown"
	// A presented token newly verified offline. Also used when a valid
	// token's update period has already ended (perpetual state).
	EventLicenseVerified = "LicenseVerified"
	// A presented token failed offline verification. Operator behavior is
	// unchanged.
	EventLicenseInvalid = "LicenseInvalid"
)

// Events: Backup lifecycle. On: MysqlBackup.
const (
	// Backup Job created.
	EventBackupStarted = "BackupStarted"
	// Backup completed successfully.
	EventBackupSucceeded = "BackupSucceeded"
	// Backup Job failed, or it succeeded at a destination other than the
	// recorded `status.artifactStorage` (`Ready=False/DestinationMismatch`).
	EventBackupFailed = "BackupFailed"
	// Completion logs are unavailable, missing, or disagree with the Job.
	// Check `MetadataComplete`; a recovered location remains usable, while
	// `DestinationUnavailable` keeps `Ready=False`.
	EventBackupMetadataIncomplete = "BackupMetadataIncomplete"
	// Backup encryption status repaired from its producing Job's launch
	// configuration.
	EventEncryptionAttributionCorrected = "EncryptionAttributionCorrected"
	// Active site changed while a backup was in progress.
	EventInFlightFailover = "InFlightFailover"
	// The backup was created with no PITR archive stamp
	// (`status.pitrArchiveID` empty), so it is no point-in-time baseline:
	// the site renders disagree or one is unreadable (a backup of a
	// profile other than the active PITR profile, which does not wait for
	// the roll), or the render is deferred (any site Deployment lacks a
	// decodable render while the write binding is not
	// `Bound`/`Establishing`: the pre-v1.7.0 deferral, which a backup of
	// the active profile does not wait for). The message says why; when
	// the renders disagree it also names Deployments of sites no longer in
	// `spec.sites` ("... belong to sites no longer in spec.sites and are
	// never re-rendered: scale them to zero or delete them").
	EventPITRArchiveStampSkipped = "PITRArchiveStampSkipped"
	// Retention kept a backup past its window because a verification, or
	// a restore (`initFromBackup` or `restoreInPlace` with `backupRef`),
	// is reading it. The message names the verifications and the restoring
	// groups.
	EventRetentionDeferred = "RetentionDeferred"
	// A Job named `mysqlbackup-<backup>` exists but is not owned by this
	// `MysqlBackup` (a Job orphaned by an earlier backup of the same name,
	// or an older operator's cleanup Job for a backup named `<backup>`
	// minus `-cleanup`). The backup waits in `Pending` with
	// `Ready=False/BackupJobConflict`, never adopts that Job, and fails
	// with `PendingTimedOut` after 30 minutes if it is still there.
	// Emitted once on entering the wait.
	EventBackupJobConflict = ReasonBackupJobConflict
	// No site qualifies as the backup's source (unreachable, not
	// replicating, or lagging beyond `maxLagSecondsForSource`). The backup
	// waits in `Pending` with `Ready=False/WaitingForHealthySource` and
	// retries. Emitted once on entering the wait.
	EventWaitingForHealthySource = ReasonWaitingForHealthySource
	// Terminal: no healthy source appeared within 30 minutes, or a pinned
	// `spec.sourceSite` cannot be used. The backup is `Failed` with
	// `Ready=False/NoHealthySource`.
	EventNoHealthySource = ReasonNoHealthySource
	// Terminal: the backup waited longer than 30 minutes for anything
	// other than a healthy source. The backup is `Failed` with
	// `Ready=False/PendingTimedOut`; the message reads `pending for
	// <duration> (<pending reason>): <message>`.
	EventPendingTimedOut = ReasonPendingTimedOut
	// The backup's profile is
	// [read-only](/docs/backup-and-restore/backup-encryption#read-only-profiles)
	// for this group (the `initFromBackup` archive profile before the
	// restore succeeds or without a binding, or a destination whose
	// binding names another group or was refused
	// `ArchiveForeign`/`ArchiveCopy`). The backup is `Failed` with
	// `Ready=False/ProfileReadOnly` and writes nothing.
	EventProfileReadOnly = ReasonProfileReadOnly
	// A backup of the active PITR profile waits while the site Deployments
	// roll onto a new archive render, so it can become that archive's
	// baseline. The backup waits in `Pending` with
	// `Ready=False/PITRArchiveRolloutPending` and fails with
	// `PendingTimedOut` after 30 minutes. Emitted once on entering the
	// wait.
	EventPITRArchiveRolloutPending = ReasonPITRArchiveRolloutPending
	// The Secret the encrypted backup would use as its dump-key snapshot
	// (`mysqlbackup-<backup>-dump-key-<uid>`) exists but is not labelled
	// as this backup's snapshot. The backup waits in `Pending` with
	// `Ready=False/DumpKeySnapshotConflict` and fails with
	// `PendingTimedOut` after 30 minutes; delete or rename that Secret.
	// Emitted once on entering the wait.
	EventDumpKeySnapshotConflict = ReasonDumpKeySnapshotConflict
	// The encrypted profile's `encryption.passphraseSecretKeyRef` Secret
	// is missing, or its key is missing or empty, so no dump-key snapshot
	// can be taken. The backup waits in `Pending` with
	// `Ready=False/PassphraseSecretMissing` and fails with
	// `PendingTimedOut` after 30 minutes. Emitted once on entering the
	// wait.
	EventPassphraseSecretMissing = ReasonPassphraseSecretMissing
	// Terminal: the profile's owned claim `mysql-<group>-backup-<profile>`
	// has another StorageClass than `pvc.provisioned.storageClassName`, or
	// requests less than `pvc.provisioned.size` (the claim was created for
	// an earlier profile of the same name). The backup is `Failed` with
	// `Ready=False/BackupStorageMismatch` and never writes to that claim;
	// delete the claim or restore the profile's values.
	EventBackupStorageMismatch = ReasonBackupStorageMismatch
	// The profile's owned claim requests more than `pvc.provisioned.size`,
	// for example after it was expanded out of band. The backup uses it.
	// Emitted when a backup's Job is created.
	EventBackupStorageExpansionAccepted = "BackupStorageExpansionAccepted"
)

// Events: Backup and verification setup. On: MysqlBackup, MysqlBackupVerification.
const (
	// The referenced `MysqlFailoverGroup` does not exist. The
	// `MysqlBackup` is `Failed` with `Ready=False/GroupNotFound`, or the
	// `MysqlBackupVerification` is `Failed` with
	// `Verified=False/GroupNotFound`.
	EventGroupNotFound = ReasonGroupNotFound
	// The named backup profile is not in the group's
	// `spec.backup.profiles`. A `MysqlBackup` without a Job yet is
	// `Failed` with `Ready=False/ProfileNotFound`; a
	// `MysqlBackupVerification` is `Failed` with
	// `Verified=False/ProfileNotFound`.
	EventProfileNotFound = ReasonProfileNotFound
	// The operator could not build the Job spec. On a `MysqlBackup` the
	// backup is `Failed` with `Ready=False/BuildJobFailed`, also when the
	// backup Job is gone mid-run and a replacement built from the current
	// profile would not write to the destination recorded at launch. On a
	// `MysqlBackupVerification` the run is `Failed` with
	// `Verified=False/BuildJobFailed`; a PITR refusal uses its refusal
	// token as the reason instead.
	EventBuildJobFailed = ReasonBuildJobFailed
)

// Events: Artifact cleanup. On: MysqlBackup.
const (
	// Cleanup Job created for backup artifact.
	EventArtifactCleanupStarted = "ArtifactCleanupStarted"
	// Artifact removed; the message gives the object count.
	EventArtifactCleanupSucceeded = "ArtifactCleanupSucceeded"
	// Cleanup found nothing at the recorded destination; the finalizer is
	// released.
	EventArtifactCleanupNothingFound = "ArtifactCleanupNothingFound"
	// A cleanup Job failed, once per Job. The `ArtifactCleanup` condition
	// names the cause; a new Job runs after 5 minutes (transient) or 30
	// minutes (refusal), and the finalizer blocks deletion until one
	// succeeds. Counted by
	// `bloodraven_backup_artifact_cleanup_failures_total`.
	EventArtifactCleanupFailed = "ArtifactCleanupFailed"
	// A failed cleanup Job is being replaced.
	EventArtifactCleanupRetry = "ArtifactCleanupRetry"
	// A `MysqlBackupVerification` (`ArtifactCleanup=False/HeldByVerification`)
	// or a restore (`ArtifactCleanup=False/HeldByRestore`) is still reading
	// the artifact; cleanup waits until its pods stop.
	EventArtifactCleanupDeferred = "ArtifactCleanupDeferred"
	// Deleting an abandoned backup (never succeeded, or its group or
	// profile is gone) waits for its producing backup Job's pods to stop,
	// and one has not stopped after 10 minutes
	// (`ArtifactCleanup=False/ProducerStopStuck`). The deletion stays
	// held; once the pod's node is confirmed down, force-delete the pod
	// (`kubectl delete pod --force --grace-period=0`).
	EventArtifactCleanupStalledProducerStop = "ArtifactCleanupStalledProducerStop"
	// Cleanup cannot start (`DestinationMismatch`,
	// `CredentialsUnavailable`, `OperatorImageUnset`,
	// `CleanupJobConflict`, `CleanupJobInvalid`); the message carries the
	// reason.
	EventArtifactCleanupBlocked = "ArtifactCleanupBlocked"
	// Referenced failover group or profile is gone, or no artifact
	// destination can be recovered; or the best-effort cleanup of a backup
	// that never succeeded (its stamp and any partial dump) could not run
	// or failed, or found no stamp but objects at the location
	// (`BLOODRAVEN_CLEANUP_SKIPPED`; nothing is deleted). Cleanup is
	// skipped and the finalizer is released; the message names what is
	// left (for the skipped verdict, the location, the stamp key and the
	// kept dump-key snapshot). An encrypted backup that recorded neither
	// its producing Job nor its artifact destination, or whose abandoned
	// cleanup was skipped, keeps its dump-key snapshot (`… is kept because
	// encrypted artifacts may exist; delete it once you have confirmed
	// none remain`).
	EventArtifactCleanupSkipped = "ArtifactCleanupSkipped"
	// The backup was deleted with `spec.deletionPolicy: Retain`: no
	// cleanup ran, and the artifact and its dump-key snapshot Secret (an
	// encrypted backup's key) are left in place; the message names both.
	// The finalizer is released at once, or for a backup that never
	// succeeded, once its producing Job's pods have stopped. Delete them
	// by hand once they are no longer needed.
	EventArtifactRetained = "ArtifactRetained"
)

// Events: Backup verification. On: MysqlBackupVerification.
const (
	// The verification Job was created and the run moved to `Restoring`;
	// the message names the Job, backup and profile.
	EventVerificationStarted = "VerificationStarted"
	// The verification run reached `Succeeded`.
	EventVerificationSucceeded = "VerificationSucceeded"
	// The verification run reached `Failed`. A PITR failure's `Verified`
	// condition reason is the refusal token, also when building the Job
	// was refused (for example `PITRTargetInvalid` for a verification
	// against a group with `spec.backup.pitr` disabled), not
	// `BuildJobFailed`.
	EventVerificationFailed = "VerificationFailed"
	// A verification's recorded source cannot be reconciled with its Job
	// inputs. The run fails without certifying a different backup.
	EventBackupIdentityMismatch = ReasonBackupIdentityMismatch
	// The verification has no usable backup: the `backupRef` backup is
	// missing, not `Succeeded`, being deleted or for another group or
	// profile, no `Succeeded` backup exists for the group and profile, or
	// the pinned backup was deleted, replaced by a same-name backup or
	// lost this verification's hold. The run is `Failed` with
	// `Verified=False/BackupNotAvailable`; create a new verification.
	EventBackupNotAvailable = ReasonBackupNotAvailable
	// An older verification for the same group and profile has not
	// finished, so this one is refused rather than run concurrently. The
	// run is `Failed` with `Verified=False/BlockedByActiveVerification`.
	EventBlockedByActiveVerification = ReasonBlockedByActiveVerification
)

// Events: PITR verification refusals. On: MysqlBackupVerification.
//
// A point-in-time verification was refused: the Event repeats the
// `Verified=False` reason, the refusal token. The tokens are listed in
// [Restore
// refusals](/docs/backup-and-restore/backup-restore#restore-refusals);
// `PITRReplayFailed` is the replay script's own failure.
const (
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
	EventPITRReplayFailed              = string(PITRTokenReplayFailed)
	EventPITRReplayMismatch            = string(PITRTokenReplayMismatch)
	EventPITRReplayUnsupported         = string(PITRTokenReplayUnsupported)
	EventPITRStopAmbiguous             = string(PITRTokenStopAmbiguous)
	EventPITRTargetInvalid             = string(PITRTokenTargetInvalid)
	EventPITRTargetNotCovered          = string(PITRTokenTargetNotCovered)
)

// Events: MysqlDatabase. On: MysqlDatabase.
//
// See [Tenant databases](/docs/configuration/tenant-databases) for the
// lifecycle these Events report.
const (
	// Under `deletionPolicy: Delete` the operator dropped the database and
	// the CR's accounts, only the database (owner left untouched), or only
	// the accounts (the database is still declared by another CR); the
	// message says which. The finalizer is released.
	EventDatabaseDropped = "DatabaseDropped"
	// Under `deletionPolicy: Delete` this CR never applied any DDL
	// (`status.databaseCreated` is unset), so there is nothing of its own
	// to drop. The finalizer is released and MySQL is untouched.
	EventDatabaseDropSkipped = "DatabaseDropSkipped"
	// Under `deletionPolicy: Delete` another live `MysqlDatabase` on the
	// group still declares the same `databaseName`, so the database is not
	// dropped. The CR's own accounts are still vetted and dropped
	// separately.
	EventDatabaseDropRefused = "DatabaseDropRefused"
	// Under `deletionPolicy: Delete` the DROP cannot run yet: the group
	// has no active site, its primary is fenced by a restore or planned
	// failover, a group credential Secret cannot be read, the primary is
	// unreachable, a transient MySQL error hit, or the active site moved
	// during the DROP. The finalizer stays and the drop is retried; set
	// `deletionPolicy: Retain` to release the CR without dropping.
	EventDatabaseDropDeferred = "DatabaseDropDeferred"
	// `deletionPolicy: Retain`: the finalizer is removed and the database,
	// the owner and the `users[]` principals are left untouched in MySQL.
	EventDatabaseRetained = "DatabaseRetained"
	// Under `deletionPolicy: Delete` the `MysqlFailoverGroup` is gone
	// (before the DROP, or it disappeared during it), so there is nothing
	// to connect to. The finalizer is released; the database and accounts
	// may remain in MySQL.
	EventDatabaseCleanupSkipped = "DatabaseCleanupSkipped"
	// The username in the owner Secret changed: the new owner user was
	// created and granted, then the previous one was revoked and dropped.
	// Also emitted when an abandoned rotation target is dropped before
	// rotating to a newer username.
	EventOwnerUserRotated = "OwnerUserRotated"
	// Hosts were removed from `spec.owner.hosts`; the owner user was
	// revoked and dropped on exactly those hosts.
	EventOwnerHostsRemoved = "OwnerHostsRemoved"
	// An owner account was not dropped because something else still claims
	// it: this CR's own `spec.grants[]`, or another live `MysqlDatabase`
	// (its owner Secret or user, its `spec.grants[]`, or its `users[]`
	// ledger). Emitted on deletion under `deletionPolicy: Delete`, and on
	// an owner rotation or host removal, where the account still loses its
	// rights on this database unless this CR's own `spec.grants[]`
	// declares it.
	EventOwnerUserDropSkipped = "OwnerUserDropSkipped"
	// An owner account this CR recorded has since become a group-level
	// principal, so it is never dropped. Emitted on deletion under
	// `deletionPolicy: Delete`, and on an owner rotation or host removal,
	// where its rights on this database are still revoked.
	EventOwnerUserReservedSkipped = "OwnerUserReservedSkipped"
	// The username in a `spec.users[]` Secret changed: the new principal
	// was created and granted, then the previous one was revoked and
	// dropped. Also emitted when an abandoned rotation target is dropped
	// before rotating to a newer username.
	EventUserRotated = "UserRotated"
	// Hosts were removed from a `spec.users[]` entry's `hosts`; that
	// principal was revoked and dropped on exactly those hosts.
	EventUserHostsRemoved = "UserHostsRemoved"
	// A `spec.users[]` entry was removed; its principal was revoked and
	// dropped on the hosts recorded for it.
	EventUserRemoved = "UserRemoved"
	// A `spec.users[]` entry was removed, but its principal is now
	// declared by another surface of this CR (another `users[]` entry, the
	// owner or `spec.grants[]`), so the account is kept and no
	// `UserRemoved` follows.
	EventUserTransferred = "UserTransferred"
	// A `users[]` principal was not dropped because this CR's own
	// `spec.grants[]` declares it or another live `MysqlDatabase` still
	// claims it. Emitted on deletion under `deletionPolicy: Delete`, and
	// on a rotation, entry removal or host removal, where the principal
	// still loses its rights on this database unless this CR's own
	// `spec.grants[]` declares it.
	EventUserDropSkipped = "UserDropSkipped"
	// A `users[]` principal this CR recorded has since become a
	// group-level principal, so it is never dropped. Emitted on deletion
	// under `deletionPolicy: Delete`, and on a rotation, entry removal or
	// host removal, where its rights on this database are still revoked.
	EventUserReservedSkipped = "UserReservedSkipped"
	// The operator removed a restriction on an impossible legacy escaped
	// spelling of the database name from an account this CR recorded; such
	// restrictions were left by older operator builds.
	EventPartialRevokeLifted = "PartialRevokeLifted"
	// The best-effort repair of legacy escaped-schema restrictions found
	// something it will not or could not fix: the operator account is
	// restricted on the database (repairs on that spelling are skipped for
	// every account), a recorded tenant account has a restriction on the
	// real schema that needs review by the privilege owner, or a repair
	// step failed. Normal tenant reconciliation continues; see the
	// privilege restriction recovery playbook.
	EventTenantPrivilegeRestriction = "TenantPrivilegeRestriction"
	// The spec failed validation at reconcile time (for example an
	// identifier or a username from a Secret that does not match the
	// allowed pattern, or a MySQL system schema as `databaseName`). The CR
	// is `Failed` with `Ready=False/InvalidSpec` and no SQL runs.
	EventInvalidSpec = ReasonInvalidSpec
	// MySQL refused a statement with a non-transient error while applying
	// the database, its accounts or grants, or while revoking or dropping
	// a retired account. The CR is `Failed` with `Ready=False/MySQLError`
	// and the message carries the error; retried every minute.
	EventMySQLError = ReasonMySQLError
	// An older `MysqlDatabase` on the same group already declares this
	// `databaseName`. This CR is `Failed` with
	// `Ready=False/DatabaseNameConflict` before any SQL runs, and
	// re-checked every minute.
	EventDatabaseNameConflict = ReasonDatabaseNameConflict
	// The schema already exists and `status.databaseCreated` does not say
	// this CR created it, so adoption is refused. The CR is `Failed` with
	// `Ready=False/DatabasePreExists`; re-checked every minute.
	EventDatabasePreExists = ReasonDatabasePreExists
	// An older `MysqlDatabase` on the same group shares this CR's owner
	// Secret or owner MySQL user. This CR is `Failed` with
	// `Ready=False/OwnerConflict` before any SQL runs; give each
	// `MysqlDatabase` a distinct owner.
	EventOwnerConflict = ReasonOwnerConflict
	// The owner Secret names a group-level principal (`root`,
	// `replicator`, a MySQL system account or a `spec.credentials` user).
	// The CR is `Failed` with `Ready=False/OwnerUserReserved` and no SQL
	// runs; use a tenant-specific username.
	EventOwnerUserReserved = ReasonOwnerUserReserved
	// An owner account `user@host` already exists and no record of this CR
	// names that user on that host, so adoption is refused. The CR is
	// `Failed` with `Ready=False/PreExistingOwnerUser`; re-checked every
	// minute.
	EventPreExistingOwnerUser = ReasonPreExistingOwnerUser
	// A `spec.users[]` account `user@host` already exists and no record of
	// this CR names it on that host, so adoption is refused. The CR is
	// `Failed` with `Ready=False/PreExistingUser`; re-checked every
	// minute.
	EventPreExistingUser = ReasonPreExistingUser
	// A `spec.users[]` username is already claimed by another
	// `MysqlDatabase` on the group (as its owner, in its `spec.grants[]`,
	// or in its `users[]` ledger). The CR is `Failed` with
	// `Ready=False/UserClaimedBySibling` before any SQL runs; share a
	// group-level principal through `spec.grants[]` instead.
	EventUserClaimedBySibling = ReasonUserClaimedBySibling
	// A `spec.users[]` Secret names a group-level principal (`root`,
	// `replicator`, a MySQL system account or a `spec.credentials` user).
	// The CR is `Failed` with `Ready=False/UserReserved` and no SQL runs.
	EventUserReserved = ReasonUserReserved
	// A `spec.grants[]` user does not exist at host `%`; the operator
	// never creates it. The CR is `Failed` with
	// `Ready=False/GrantUserMissing`; create the principal at the group
	// level and the CR goes `Ready` on its own (re-checked every minute).
	EventGrantUserMissing = ReasonGrantUserMissing
	// The operator cannot confirm or enable `partial_revokes=ON` on the
	// primary, so tenant changes are deferred. The CR is `Pending` with
	// `Ready=False/PartialRevokesOff` and retries; existing accounts and
	// grants are left intact.
	EventPartialRevokesOff = ReasonPartialRevokesOff
)

// Events: MysqlStandbyCluster. On: MysqlStandbyCluster.
const (
	// A discovery scan changed `status.discovered` (the newest dump, the
	// archive identity or its binlog window). The message gives the dump
	// and the manifest and binlog counts. Rescans that find nothing new
	// emit nothing.
	EventBucketScanned = "BucketScanned"
	// A binlog manifest under the source prefix could not be read or
	// parsed. The source archive is refused: `SourceConfigKnown=False`
	// with the archive reason (for example `ArchiveLegacy`,
	// `ArchiveVersionUnsupported` or `ArchiveUnreadable`), and
	// `latestDump` is published without `binlogArchive`.
	EventManifestParseFailed = "ManifestParseFailed"
	// The newest dump's `@.json` metadata is unreadable or malformed.
	// `SourceConfigKnown` is `False` with reason `MetadataUnreadable`; the
	// bucket itself stays readable.
	EventMetadataParseFailed = "MetadataParseFailed"
	// On a `MysqlStandbyCluster`: the source archive was refused
	// (`SourceConfigKnown=False` with an archive reason); emitted when the
	// reason changes.
	EventPITRArchiveRefused = "PITRArchiveRefused"
	// The archive store for the source bucket could not be constructed
	// (for example bad credentials or an unusable endpoint).
	// `BucketReadable` and `SourceConfigKnown` are `False` with reason
	// `AuthFailed`; discovery retries every discovery interval and the
	// Event repeats on each failed attempt.
	EventAuthFailed = ReasonAuthFailed
	// The standby's storage backend could not be resolved: an invalid
	// `spec.source.s3` bucket, prefix or endpoint (a prefix must name more
	// than the bucket root), an unreadable `credentialsSecretName` Secret,
	// or a `spec.source.decryption.passphraseSecretKeyRef` Secret or key
	// that is missing or empty. `BucketReadable` and `SourceConfigKnown`
	// are `False` with reason `ConfigError`; retried every discovery
	// interval.
	EventConfigError = ReasonConfigError
	// Listing the source bucket prefix failed. `BucketReadable` is `False`
	// with reason `ListFailed` and `status.discovered` keeps the last
	// successful scan; retried every discovery interval.
	EventListFailed = ReasonListFailed
)
