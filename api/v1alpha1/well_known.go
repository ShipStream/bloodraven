package v1alpha1

// Well-known metadata: the labels, annotations, finalizers and the node
// taint Bloodraven reads or writes. Every key is under
// bloodraven.shipstream.io: either the prefix itself
// (bloodraven.shipstream.io/<name>) or a subdomain of it for a key family
// whose name part is an id (<kind>-hold.bloodraven.shipstream.io/<id>,
// <namespace>.db-readonly.bloodraven.shipstream.io/<group>), so one prefix
// match finds every member. A boolean label value is "true" or "false".
//
// This file is the registry the generated page
// site/content/docs/4.configuration/12.well-known-metadata.md is built from
// (make well-known-doc). Each const block's doc comment starts with
// "Keys: <section title>." followed by optional paragraphs for the
// section. Each key's own doc comment describes it and ends with a
// paragraph of fields, each starting a line:
//
//	On: the objects that carry it.
//	Writer: who writes it, and who reads it when that matters.
//	Value: the value grammar.
//	Stability: Public, Operator-owned read-only or Internal (may change).
//	Key: the key pattern, for a prefix or a function (optional).
//
// The stability levels:
//
//   - Public: users and their tooling write or select on it. It changes
//     only with a release-noted break.
//   - Operator-owned read-only: the operator writes it and users may read
//     it, but must not write or remove it.
//   - Internal (may change): the operator's own bookkeeping.

// Values of the boolean labels (HealthyLabel, DragonflyTrafficLabel,
// BackupEncryptedLabel, the client labels).
const (
	LabelValueTrue  = "true"
	LabelValueFalse = "false"
)

// Keys: Action annotations.
//
// A user (or kubectl bloodraven) sets these on the MysqlFailoverGroup to
// request a one-shot action. They share one value grammar,
// `<subject>(:<key>=<value>)*`: the subject is a site name or an operation
// id, a pair splits at its first `=`, and a value matches
// `^[A-Za-z0-9._/+-]+$`, so it never contains `:` or `=`. An instant inside
// a value uses the ISO 8601 basic UTC form, for example `20261104T100000Z`.
// A destructive action on a site-name subject carries
// `confirm=<metadata.name>:uid=<metadata.uid>`; an operation id the
// operator minted and never reuses needs neither. Two keys are outside the
// grammar: RevokeDeploymentLeasesAnnotation is presence-only (its value is
// ignored), and RotateKeyringAnnotation takes a bare site name, since a
// rotation destroys nothing. A value that does not parse, or fails its
// interlock, is never acted on: each key's consumer reports why (a Warning
// Event, or the status of the request) and, for most keys, removes the
// annotation so the request can be set again; the entries below say which.
// Never commit an action annotation to a GitOps repository: a sync
// re-applies it.
const (
	// PlannedFailoverAnnotation triggers a graceful, cooldown-respecting
	// switchover to the named PrimaryCandidate site. The operator removes
	// it once the request is decided: accepted, rejected or skipped. A
	// request that is deferred (onCooldown=Defer, a deployment hold), or
	// that waits for admission, keeps it until then; removing it cancels a
	// deferred run.
	//
	// On: MysqlFailoverGroup.
	// Writer: user (kubectl bloodraven promote); the operator removes it.
	// Value: `<site>[:maxLagWait=<duration>]`, for example `pdx` or
	// `pdx:maxLagWait=30s`; maxLagWait is 1s to 1h.
	// Stability: Public.
	PlannedFailoverAnnotation = "bloodraven.shipstream.io/planned-failover"

	// AcknowledgePlannedFailoverHoldAnnotation acknowledges an automation
	// hold. An operator-minted run id cannot match another group, so the
	// value needs no confirm= or uid= pair. The operator removes it after
	// it clears the hold.
	//
	// On: MysqlFailoverGroup.
	// Writer: user (kubectl bloodraven ack-planned-failover-hold); the
	// operator removes it.
	// Value: `<runID>`, the held run's status.plannedFailoverHold.runID.
	// Stability: Public.
	AcknowledgePlannedFailoverHoldAnnotation = "bloodraven.shipstream.io/acknowledge-planned-failover-hold"

	// RecloneAnnotation asks the operator to reclone one site from the
	// current primary with CLONE INSTANCE, which wipes the site's datadir.
	// When the site has a recorded divergent GTID set
	// (status.sites[].divergentGtid), gtidPrefix= is required: at least 8
	// characters of the source UUID of the set's first entry. It proves
	// the requester read the divergence. The operator also writes it to
	// drain status.restoreReclones, and removes it when the clone is done
	// or the request is rejected.
	//
	// On: MysqlFailoverGroup.
	// Writer: user (kubectl bloodraven reclone); operator.
	// Value: `<site>:confirm=<metadata.name>:uid=<metadata.uid>`, plus
	// `:gtidPrefix=<uuid prefix>` when the site has diverged.
	// Stability: Public.
	RecloneAnnotation = "bloodraven.shipstream.io/reclone-site"

	// RevokeDeploymentLeasesAnnotation revokes every deployment lease the
	// group holds. Lease admission stays closed while it is set; the
	// operator removes it once every revocation has persisted, and a value
	// changed meanwhile is a new request it handles first.
	//
	// On: MysqlFailoverGroup.
	// Writer: user; the operator removes it.
	// Value: any; its presence is the request.
	// Stability: Public.
	RevokeDeploymentLeasesAnnotation = "bloodraven.shipstream.io/revoke-deployment-leases"

	// RotateKeyringAnnotation triggers an InnoDB master-key rotation on the
	// named site. The operator refuses to rotate the active primary: rotate
	// the replicas, run a planned failover, then rotate the former primary.
	//
	// On: MysqlFailoverGroup.
	// Writer: user; the operator removes it.
	// Value: `<site>`.
	// Stability: Public.
	RotateKeyringAnnotation = "bloodraven.shipstream.io/rotate-keyring"

	// ReleasePITRArchiveBindingAnnotation releases one PITR archive
	// binding and deletes its key snapshots. The binding id is derived from
	// the destination, so it recurs in a re-created group of the same
	// name: the uid pair keeps a stale request from releasing the new
	// group's binding.
	//
	// On: MysqlFailoverGroup.
	// Writer: user; the operator removes it.
	// Value: `<binding id>:confirm=<metadata.name>:uid=<metadata.uid>`.
	// Stability: Public.
	ReleasePITRArchiveBindingAnnotation = "bloodraven.shipstream.io/release-pitr-archive-binding"
)

// Keys: Failover record and planned-failover bookkeeping.
const (
	// LastFailoverAnnotation and LastFailoverTargetAnnotation copy the
	// anti-flap record (status.lastFailover) to object metadata, so a
	// status-write outage that overlaps an operator restart cannot lose
	// the cooldown. Both are written in one patch on every promotion.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: an RFC 3339 UTC instant at second precision; the target site
	// name.
	// Stability: Operator-owned read-only.
	LastFailoverAnnotation       = "bloodraven.shipstream.io/last-failover"
	LastFailoverTargetAnnotation = "bloodraven.shipstream.io/last-failover-target"

	// PromotionGtidExecutedAnnotation is the out-of-band copy of the
	// promotion watermark (status.lastFailover.promotionGtidExecuted),
	// written with LastFailoverAnnotation and LastFailoverTargetAnnotation.
	// Omitted above 64 KiB. Ignored unless
	// PromotionGtidExecutedForAnnotation names the event the other two keys
	// record.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: a GTID set.
	// Stability: Operator-owned read-only.
	PromotionGtidExecutedAnnotation = "bloodraven.shipstream.io/promotion-gtid-executed"

	// PromotionGtidExecutedForAnnotation binds the watermark to its
	// failover event, written and deleted in the same patch as
	// PromotionGtidExecutedAnnotation. A watermark whose binding does not
	// name the event the other two keys record is no watermark.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: `<last-failover> <last-failover-target>` (one space), for
	// example `2026-11-04T10:00:00Z iad`.
	// Stability: Operator-owned read-only.
	PromotionGtidExecutedForAnnotation = "bloodraven.shipstream.io/promotion-gtid-executed-for"

	// CloneServerUUIDAnnotation records, before CLONE INSTANCE is issued
	// into a site, the server_uuid the site had, so the check that every
	// clone gives its recipient a new one (CloneServerUUIDNotRotated) is
	// still made when the clone finishes after the operator that issued
	// it restarted. A site's entry is removed once its clone has an
	// outcome; the annotation is removed with the last entry. While an
	// entry exists, the site's keyring stays unsealed under encryption at
	// rest.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: `{"<site>":{"serverUUID":"<uuid>","previousCloneBegin":"<BEGIN_TIME>"}}`,
	// where previousCloneBegin is BEGIN_TIME of the site's
	// `performance_schema.clone_status` row before the clone (empty when
	// it had none, omitted when it could not be read).
	// Stability: Internal (may change).
	CloneServerUUIDAnnotation = "bloodraven.shipstream.io/clone-server-uuid"

	// ServerUUIDHistoryAnnotation records the server_uuid values the
	// operator has seen each site run under, newest last (at most 8 per
	// site). A site's replication receiver drops events that carry the
	// site's own server_id, whatever server_uuid issued them, so a site
	// whose volume came back from before a reclone lacks the transactions
	// it issued under its later server_uuid and cannot get them by
	// replication. The operator compares a returning site with the
	// primary under every recorded server_uuid (status.sites[].lostGtid).
	// Removing the annotation narrows that check to the site's current
	// server_uuid until the operator has seen the others again.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: `{"<site>":["<uuid>", ...]}`.
	// Stability: Internal (may change).
	ServerUUIDHistoryAnnotation = "bloodraven.shipstream.io/server-uuid-history"

	// PlannedFailoverStatusFenceAnnotation is bumped with a write
	// conditioned on the resourceVersion an ambiguous status write was
	// made against. Its success proves that write can never commit. Each
	// bump moves past the stored value.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: an ISO 8601 basic UTC instant with nanoseconds,
	// `20261104T100000.123456789Z`.
	// Stability: Internal (may change).
	PlannedFailoverStatusFenceAnnotation = "bloodraven.shipstream.io/planned-failover-status-fence"

	// PlannedFailoverTerminalAnnotation ends a run outside /status when its
	// status writes stall. Every planned-failover predicate treats the run
	// it names as terminal; a ManualRecoveryRequired marker is also a hold
	// record. A marker with a schemaVersion the operator does not know is
	// reported (Degraded=True/MetadataVersionUnsupported and a Warning
	// Event) and proves no outcome, but one naming a run with outcome
	// ManualRecoveryRequired still holds automation.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: `{"schemaVersion":1,"runID":"<runID>","outcome":"<outcome>","reason":"<reason>","warnings":["<warning>"]}`
	// (reason and warnings may be omitted).
	// Stability: Internal (may change).
	PlannedFailoverTerminalAnnotation = "bloodraven.shipstream.io/planned-failover-terminal"

	// PlannedFailoverStallSinceAnnotation is the durable start of the
	// status-stall deadline.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: `<runID>:since=<YYYYMMDDThhmmssZ>`.
	// Stability: Internal (may change).
	PlannedFailoverStallSinceAnnotation = "bloodraven.shipstream.io/planned-failover-stall-since"

	// PlannedFailoverManagerAbsentSinceAnnotation is the durable start of
	// the manager-absence deadline.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: `<runID>:since=<YYYYMMDDThhmmssZ>`.
	// Stability: Internal (may change).
	PlannedFailoverManagerAbsentSinceAnnotation = "bloodraven.shipstream.io/planned-failover-manager-absent-since"

	// PlannedFailoverConfirmedAnnotation records, before the run releases
	// its claim, that the run's promotion was confirmed, recorded and
	// published, so a restart before the final status write does not take
	// the released claim back. Removed when the run ends.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: `<runID>:since=<YYYYMMDDThhmmssZ>`, the confirmation time.
	// Stability: Internal (may change).
	PlannedFailoverConfirmedAnnotation = "bloodraven.shipstream.io/planned-failover-confirmed"

	// EncryptionStatusFenceAnnotation is bumped to advance the group's
	// resourceVersion, so a status write conditioned on an older version
	// (one whose response was lost) can no longer commit. The write is what
	// matters; nothing reads the value.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: an RFC 3339 timestamp.
	// Stability: Internal (may change).
	EncryptionStatusFenceAnnotation = "bloodraven.shipstream.io/encryption-status-fence"

	// RestoreInPlaceConfirmAnnotation is the spec.restoreInPlace.confirm
	// token the in-place restore Job was created for, so a Job left over
	// from an earlier request with the same name is not mistaken for the
	// current one.
	//
	// On: the in-place restore Job.
	// Writer: operator.
	// Value: the confirm token.
	// Stability: Internal (may change).
	RestoreInPlaceConfirmAnnotation = "bloodraven.shipstream.io/restore-confirm"

	// RestoreBackupUIDAnnotation records the MysqlBackup a restore Job was
	// built from (source.backupRef). The Job is created only when that is
	// the backup the restore holds (RestoreHoldAnnotationPrefix).
	//
	// On: restore Jobs (bootstrap and in-place) with a backupRef source.
	// Writer: operator.
	// Value: the MysqlBackup's metadata.uid.
	// Stability: Internal (may change).
	RestoreBackupUIDAnnotation = "bloodraven.shipstream.io/restore-backup-uid"
)

// Keys: Finalizers.
//
// Each finalizer is named after its kind. The operator adds and removes
// them; removing one by hand skips the cleanup it guards.
const (
	// FailoverGroupFinalizer runs the graceful-shutdown sequence (node
	// taints removed, DNS records deleted) before a MysqlFailoverGroup is
	// deleted.
	//
	// On: MysqlFailoverGroup.
	// Writer: operator.
	// Value: none (a finalizer).
	// Stability: Internal (may change).
	FailoverGroupFinalizer = "bloodraven.shipstream.io/failover-group"

	// MysqlDatabaseFinalizer guards the MySQL-side cleanup of a
	// MysqlDatabase.
	//
	// On: MysqlDatabase.
	// Writer: operator.
	// Value: none (a finalizer).
	// Stability: Internal (may change).
	MysqlDatabaseFinalizer = "bloodraven.shipstream.io/mysqldatabase"

	// MysqlBackupFinalizer guards the artifact cleanup of a MysqlBackup.
	//
	// On: MysqlBackup.
	// Writer: operator.
	// Value: none (a finalizer).
	// Stability: Internal (may change).
	MysqlBackupFinalizer = "bloodraven.shipstream.io/mysqlbackup"

	// MysqlBackupVerificationFinalizer guards the cleanup of a
	// MysqlBackupVerification's resources.
	//
	// On: MysqlBackupVerification.
	// Writer: operator.
	// Value: none (a finalizer).
	// Stability: Internal (may change).
	MysqlBackupVerificationFinalizer = "bloodraven.shipstream.io/mysqlbackup-verification"
)

// Keys: Group routing labels.
//
// The operator puts these on the objects it manages for a
// MysqlFailoverGroup (pods, Services, Deployments, StatefulSets, Secrets).
// Services, NetworkPolicies and monitors select on them. Operator labels
// win over spec.podLabels.
const (
	// FailoverGroupLabel names the owning MysqlFailoverGroup. It is also
	// on MysqlBackups and MysqlBackupVerifications (spec.failoverGroupRef),
	// and kubectl bloodraven sets it on the ones it creates. The one place
	// a user writes it is a MysqlBackup or MysqlBackupVerification created
	// by hand: set it to spec.failoverGroupRef, with BackupProfileLabel.
	// On every object the operator manages it is the operator's.
	//
	// On: managed objects and pods; MysqlBackup, MysqlBackupVerification.
	// Writer: operator; kubectl bloodraven backup and verify; user, on a
	// MysqlBackup or MysqlBackupVerification it creates.
	// Value: the group name.
	// Stability: Operator-owned read-only.
	FailoverGroupLabel = "bloodraven.shipstream.io/failover-group"

	// SiteLabel names the site. It is part of every Deployment and
	// StatefulSet selector, which is immutable: a workload whose selector
	// differs from the one the operator renders is refused
	// (Degraded=True/WorkloadSelectorMismatch).
	//
	// On: managed objects and pods.
	// Writer: operator.
	// Value: the site name.
	// Stability: Operator-owned read-only.
	SiteLabel = "bloodraven.shipstream.io/site"

	// RoleLabel is the site's role, set on the live pods each health poll.
	// The primary and replica Services select on it.
	//
	// On: MySQL pods.
	// Writer: operator.
	// Value: `primary` or `replica`.
	// Stability: Operator-owned read-only.
	RoleLabel = "bloodraven.shipstream.io/role"

	// HealthyLabel is the site's verdict from its last health poll. The
	// replica Service and a ReadOnly site's Service select on
	// "true".
	//
	// On: MySQL pods.
	// Writer: operator.
	// Value: `true` or `false`.
	// Stability: Operator-owned read-only.
	HealthyLabel = "bloodraven.shipstream.io/healthy"

	// DragonflyRoleLabel is a Dragonfly pod's operator-assigned role. The
	// active Service selects on it together with DragonflyTrafficLabel.
	//
	// On: Dragonfly pods.
	// Writer: operator.
	// Value: `master` or `replica`.
	// Stability: Operator-owned read-only.
	DragonflyRoleLabel = "bloodraven.shipstream.io/dragonfly-role"

	// DragonflyTrafficLabel gates whether a Dragonfly pod is eligible for
	// the active Service, which selects "true". A planned failover removes
	// it from the source pod before the takeover, so the Service sheds the
	// old endpoint first.
	//
	// On: Dragonfly pods.
	// Writer: operator.
	// Value: `true`, or absent.
	// Stability: Operator-owned read-only.
	DragonflyTrafficLabel = "bloodraven.shipstream.io/dragonfly-traffic"
)

// Keys: Managed-object bookkeeping.
const (
	// KeyringVersionLabel is the escrow version on a site's keyring escrow
	// Secret.
	//
	// On: keyring escrow Secrets.
	// Writer: operator; the keyring escrow recovery runbook reads it.
	// Value: an integer.
	// Stability: Internal (may change).
	KeyringVersionLabel = "bloodraven.shipstream.io/keyring-version"

	// KeyringDigestAnnotation records the digest of the escrowed keyring
	// bytes. It is informational: the operator always recomputes it.
	//
	// On: keyring escrow Secrets.
	// Writer: operator.
	// Value: `sha256:<hex>`.
	// Stability: Internal (may change).
	KeyringDigestAnnotation = "bloodraven.shipstream.io/keyring-digest"

	// SpecHashAnnotation is the hash of the rendered spec of a managed
	// object; a difference starts an ordered update.
	//
	// On: site Deployments and other managed objects.
	// Writer: operator.
	// Value: a hash.
	// Stability: Internal (may change).
	SpecHashAnnotation = "bloodraven.shipstream.io/spec-hash"

	// ManagedServiceAnnotationsAnnotation lists the Service annotations the
	// operator manages, so it can remove one that leaves the spec.
	//
	// On: Services.
	// Writer: operator.
	// Value: a JSON list of annotation keys.
	// Stability: Internal (may change).
	ManagedServiceAnnotationsAnnotation = "bloodraven.shipstream.io/managed-service-annotations"

	// CredentialHashAnnotation records the hash of the credentials last
	// applied.
	//
	// On: credential Secrets.
	// Writer: operator.
	// Value: a hash.
	// Stability: Internal (may change).
	CredentialHashAnnotation = "bloodraven.shipstream.io/credential-hash"

	// SystemSchemaRestrictionAnnotation records the credential hash for
	// which the application user's system-schema restriction last
	// succeeded.
	//
	// On: credential Secrets.
	// Writer: operator.
	// Value: a hash.
	// Stability: Internal (may change).
	SystemSchemaRestrictionAnnotation = "bloodraven.shipstream.io/system-schema-restriction"

	// DragonflyLoadHoldAnnotation marks a Dragonfly pod whose traffic the
	// operator holds off until it has loaded its snapshot: the active site
	// of a snapshot-restore upgrade that ended while it was still loading.
	// While it is set, the operator removes the pod's
	// DragonflyTrafficLabel again whoever restored it, and it removes the
	// annotation once the site answers as loaded.
	//
	// On: Dragonfly pods.
	// Writer: operator.
	// Value: an RFC 3339 time, when the hold started.
	// Stability: Internal (may change).
	DragonflyLoadHoldAnnotation = "bloodraven.shipstream.io/dragonfly-load-hold"
)

// Keys: Backup and verification labels.
const (
	// MysqlBackupLabel names the MysqlBackup a Job or a verification
	// belongs to.
	//
	// On: backup and cleanup Jobs, MysqlBackupVerification.
	// Writer: operator.
	// Value: the MysqlBackup name.
	// Stability: Operator-owned read-only.
	MysqlBackupLabel = "bloodraven.shipstream.io/mysqlbackup"

	// BackupProfileLabel names the backup profile (spec.profileName). Set
	// it with FailoverGroupLabel when you create a MysqlBackup by hand.
	//
	// On: MysqlBackup, MysqlBackupVerification, their Jobs and CronJobs.
	// Writer: operator; kubectl bloodraven backup and verify; user.
	// Value: the profile name.
	// Stability: Public.
	BackupProfileLabel = "bloodraven.shipstream.io/backup-profile"

	// BackupScheduleLabel names the schedule that created a MysqlBackup.
	// The schedule rollup reads it.
	//
	// On: MysqlBackup, backup CronJobs.
	// Writer: operator.
	// Value: the schedule name.
	// Stability: Public.
	BackupScheduleLabel = "bloodraven.shipstream.io/backup-schedule"

	// ResourceLabel classifies an operator-created Job, CronJob or Secret.
	//
	// On: operator-created Jobs, CronJobs and Secrets.
	// Writer: operator.
	// Value: `backup`, `backup-cleanup`, `backup-schedule`, `restore`,
	// `backup-verification`, `backup-verification-schedule` or
	// `pitr-archive-check`.
	// Stability: Operator-owned read-only.
	ResourceLabel = "bloodraven.shipstream.io/resource"

	// BackupEncryptedLabel marks a backup Job (and its pods) that writes
	// an encrypted artifact. The MysqlBackup itself does not carry it: read
	// status.encryption.mode there.
	//
	// On: backup Jobs and their pods.
	// Writer: operator.
	// Value: `true`, or absent.
	// Stability: Operator-owned read-only.
	BackupEncryptedLabel = "bloodraven.shipstream.io/backup-encrypted"

	// MysqlBackupVerificationLabel names the MysqlBackupVerification a Job
	// belongs to.
	//
	// On: verification Jobs.
	// Writer: operator.
	// Value: the MysqlBackupVerification name.
	// Stability: Operator-owned read-only.
	MysqlBackupVerificationLabel = "bloodraven.shipstream.io/mysqlbackup-verification"

	// VerificationScheduleLabel names the verification schedule that
	// created a MysqlBackupVerification. A verification created by hand or
	// by kubectl bloodraven verify does not carry it.
	//
	// On: scheduled MysqlBackupVerifications, verification CronJobs.
	// Writer: operator (the CronJob and the trigger command it runs).
	// Value: the backup profile name (a profile has one verification
	// schedule, named after it).
	// Stability: Public.
	VerificationScheduleLabel = "bloodraven.shipstream.io/verification-schedule"

	// TriggeredByAnnotation records what created a MysqlBackup or a
	// MysqlBackupVerification. Informational free text; nothing reads it.
	//
	// On: MysqlBackup, MysqlBackupVerification.
	// Writer: the schedules, the trigger commands, kubectl bloodraven
	// (--triggered-by, default `manual`), user.
	// Value: free text. The operator's writers use `schedule`,
	// `schedule/<name>` and `retry/<name>/attempt-<n>`; other writers may
	// record anything.
	// Stability: Public.
	TriggeredByAnnotation = "bloodraven.shipstream.io/triggered-by"
)

// Keys: Backup bookkeeping.
const (
	// BackupRetryOfLabel names the MysqlBackup a retry replaces.
	//
	// On: MysqlBackup.
	// Writer: operator.
	// Value: the replaced MysqlBackup's name.
	// Stability: Internal (may change).
	BackupRetryOfLabel = "bloodraven.shipstream.io/backup-retry-of"

	// BackupRetryRootAnnotation and BackupRetryRootUIDAnnotation name the
	// first backup of a retry chain.
	//
	// On: MysqlBackup.
	// Writer: operator.
	// Value: the root MysqlBackup's name; its uid.
	// Stability: Internal (may change).
	BackupRetryRootAnnotation    = "bloodraven.shipstream.io/backup-retry-root"
	BackupRetryRootUIDAnnotation = "bloodraven.shipstream.io/backup-retry-root-uid"

	// BackupAttemptAnnotation is the attempt number within a retry chain.
	//
	// On: MysqlBackup.
	// Writer: operator.
	// Value: an integer.
	// Stability: Internal (may change).
	BackupAttemptAnnotation = "bloodraven.shipstream.io/backup-attempt"

	// CleanupFailureRecordedAnnotation marks a failed cleanup Job whose
	// failure Event and metric were emitted, so each Job counts once.
	//
	// On: backup cleanup Jobs.
	// Writer: operator.
	// Value: `true`.
	// Stability: Internal (may change).
	CleanupFailureRecordedAnnotation = "bloodraven.shipstream.io/cleanup-failure-recorded"

	// BackupJobFailoverGroupUIDAnnotation pins the source group's uid on a
	// backup Job and its pod; status.failoverGroupUID is recovered from
	// it. The same key is FailoverGroupUIDLabel on key snapshots.
	//
	// On: backup Jobs and their pod template.
	// Writer: operator.
	// Value: the group's metadata.uid.
	// Stability: Internal (may change).
	BackupJobFailoverGroupUIDAnnotation = "bloodraven.shipstream.io/failover-group-uid"

	// BackupJobPITRArchiveIDAnnotation pins the source group's PITR archive
	// ID on a backup Job; status.pitrArchiveID is recovered from it.
	//
	// On: backup Jobs and their pod template.
	// Writer: operator.
	// Value: `ar1-…`, or empty while PITR is off.
	// Stability: Internal (may change).
	BackupJobPITRArchiveIDAnnotation = "bloodraven.shipstream.io/pitr-archive-id"

	// VerificationBackupAnnotation pins the MysqlBackup a verification Job
	// reads. A record with a schemaVersion the operator does not know
	// fails the verification (Verified=False/MetadataVersionUnsupported
	// and a Warning Event).
	//
	// On: verification Job pod templates.
	// Writer: operator.
	// Value: `{"schemaVersion":1,"name":…,"uid":…,"artifactStorage":{…}}`.
	// Stability: Internal (may change).
	VerificationBackupAnnotation = "bloodraven.shipstream.io/verification-backup"
)

// Keys: Holds.
//
// A hold keeps an object that a running operation reads. Holds form one
// family, `<kind>-hold.bloodraven.shipstream.io/<id>`, so every hold of a
// kind is found by one prefix match. Removing a hold by hand while its holder
// runs can delete data under it.
const (
	// VerificationHoldAnnotationPrefix marks a MysqlBackup, and a PITR
	// archive key snapshot, that a MysqlBackupVerification is reading. The
	// full key is the prefix followed by the verification's uid.
	// Retention, the artifact-cleanup finalizer and the PITR binlog cutoff
	// leave the object alone while a hold on it is live, also while the
	// verification is Terminating.
	//
	// Key: `verification-hold.bloodraven.shipstream.io/<verification uid>`.
	// On: MysqlBackup; PITR archive key snapshot Secrets.
	// Writer: operator.
	// Value: the verification's name.
	// Stability: Operator-owned read-only.
	VerificationHoldAnnotationPrefix = "verification-hold.bloodraven.shipstream.io/"

	// RestoreHoldAnnotationPrefix marks a MysqlBackup that a restore
	// (spec.initFromBackup or spec.restoreInPlace with source.backupRef)
	// of a MysqlFailoverGroup in the same namespace is reading. The full
	// key is the prefix followed by the hold ID the restore recorded in
	// status.restore.backupRef.holdID or
	// status.restoreInPlace.backupRef.holdID. Retention, the
	// artifact-cleanup finalizer and the PITR binlog cutoff leave the
	// backup alone while the hold is live: until the restore is Succeeded
	// or Failed and its Job's pods have stopped.
	//
	// Key: `restore-hold.bloodraven.shipstream.io/<hold ID>`.
	// On: MysqlBackup.
	// Writer: operator.
	// Value: the restoring group's name.
	// Stability: Operator-owned read-only.
	RestoreHoldAnnotationPrefix = "restore-hold.bloodraven.shipstream.io/"
)

// Keys: PITR archive binding metadata.
//
// On archive key snapshots (Secrets) and the Jobs that mount them.
const (
	// PITRRenderAnnotation is the canonical JSON of the PITR identity and
	// access fields a site Deployment renders.
	//
	// On: site Deployments.
	// Writer: operator.
	// Value: sorted JSON with `schemaVersion` 1; unknown keys are refused.
	// Stability: Internal (may change).
	PITRRenderAnnotation = "bloodraven.shipstream.io/pitr-render"

	// PITRDestinationAnnotation is the normalized archive destination.
	//
	// On: archive key snapshot Secrets.
	// Writer: operator.
	// Value: the normalized destination.
	// Stability: Internal (may change).
	PITRDestinationAnnotation = "bloodraven.shipstream.io/pitr-destination"

	// PITRAcceptedAnnotation is the acceptance record of an accepted
	// archive key snapshot. Never edit it.
	//
	// On: archive key snapshot Secrets.
	// Writer: operator.
	// Value: JSON with `schemaVersion` 1.
	// Stability: Operator-owned read-only.
	PITRAcceptedAnnotation = "bloodraven.shipstream.io/pitr-accepted"

	// PITRBindingIDLabel is the binding id. It is the release selector, so
	// a release also covers snapshots whose binding entry was dropped or
	// lost.
	//
	// On: archive key snapshot Secrets.
	// Writer: operator.
	// Value: `pab-<16 hex digits>`.
	// Stability: Internal (may change).
	PITRBindingIDLabel = "bloodraven.shipstream.io/pitr-binding-id"

	// FailoverGroupUIDLabel is the group's metadata.uid. Snapshots have no
	// owner reference, so it scopes every listing to one group
	// incarnation. Same key as BackupJobFailoverGroupUIDAnnotation.
	//
	// On: archive key and dump key snapshot Secrets.
	// Writer: operator; doctor support-bundle.sh selects on it.
	// Value: the group's metadata.uid.
	// Stability: Internal (may change).
	FailoverGroupUIDLabel = "bloodraven.shipstream.io/failover-group-uid"

	// MysqlBackupUIDLabel is the MysqlBackup's uid.
	//
	// On: dump key snapshot Secrets.
	// Writer: operator.
	// Value: the MysqlBackup's metadata.uid.
	// Stability: Internal (may change).
	MysqlBackupUIDLabel = "bloodraven.shipstream.io/mysqlbackup-uid"

	// PITRKeySecretLabel is the archive key snapshot a Job mounts. A
	// release is refused while such a Job has not stopped.
	//
	// On: check, restore, verification and in-place staging Jobs.
	// Writer: operator.
	// Value: the snapshot Secret's name.
	// Stability: Internal (may change).
	PITRKeySecretLabel = "bloodraven.shipstream.io/pitr-key-secret"

	// PITRKeySecretUIDAnnotation is the uid of the candidate snapshot a
	// check Job mounts, when the Job was created. A verdict is accepted
	// only for that object.
	//
	// On: archive check Jobs.
	// Writer: operator.
	// Value: the snapshot Secret's metadata.uid.
	// Stability: Internal (may change).
	PITRKeySecretUIDAnnotation = "bloodraven.shipstream.io/pitr-key-secret-uid"
)

// Keys: Deployment API leases.
//
// On the coordination.k8s.io Leases the operator creates for deployment
// leases.
const (
	// DeploymentLeaseAnnotation holds the lease record. A record with a
	// schemaVersion the operator does not know is refused: the lease
	// request fails, and the group reports
	// Degraded=True/MetadataVersionUnsupported with a Warning Event.
	//
	// On: deployment Leases.
	// Writer: operator.
	// Value: JSON with `schemaVersion` 1.
	// Stability: Internal (may change).
	DeploymentLeaseAnnotation = "bloodraven.shipstream.io/deployment-lease"

	// DeploymentKindLabel is the lease kind.
	//
	// On: deployment Leases.
	// Writer: operator.
	// Value: `migration` or `failover-hold`.
	// Stability: Internal (may change).
	DeploymentKindLabel = "bloodraven.shipstream.io/deployment-kind"

	// DeploymentStateLabel is the lease state.
	//
	// On: deployment Leases.
	// Writer: operator.
	// Value: `active`, `released`, `expired` or `revoked`.
	// Stability: Internal (may change).
	DeploymentStateLabel = "bloodraven.shipstream.io/deployment-state"
)

// Keys: Client labels.
//
// The chart's deployment API NetworkPolicy
// (auxiliary.deployAPI.networkPolicy.enabled) admits a client only when
// both labels match the same source.
const (
	// MysqlClientNamespaceLabel marks a namespace whose pods may reach the
	// operator's deployment API.
	//
	// On: Namespace.
	// Writer: user; the chart NetworkPolicy selects on it.
	// Value: `true`.
	// Stability: Public.
	MysqlClientNamespaceLabel = "bloodraven.shipstream.io/mysql-client"

	// DeployClientLabel marks a pod that may call the deployment API.
	//
	// On: Pod.
	// Writer: user; the chart NetworkPolicy selects on it.
	// Value: `true`.
	// Stability: Public.
	DeployClientLabel = "bloodraven.shipstream.io/deploy-client"
)

// readOnlyTaintKeySuffix ends the prefix of ReadOnlyTaintKey.
const readOnlyTaintKeySuffix = ".db-readonly.bloodraven.shipstream.io/"

// ReadOnlyTaintValue is the value of the read-only node taint; its effect is
// NoExecute.
const ReadOnlyTaintValue = LabelValueTrue

// ReadOnlyTaintKey returns the read-only taint key of the failover group
// name in namespace. The operator puts this taint, with effect NoExecute,
// on a site's nodes (spec.sites[].taintNodeSelector) while the site is read
// only, so pods that do not tolerate it are evicted; workloads that must
// keep running there tolerate it. The namespace is part of the key, so two
// groups of the same name in different namespaces never share a taint. The
// prefix is at most 100 characters (a 63-character namespace) and the name
// part is the group name.
//
// Key: `<namespace>.db-readonly.bloodraven.shipstream.io/<group>`.
// On: Node.
// Writer: operator; users tolerate it.
// Value: `true`, effect `NoExecute`.
// Stability: Public.
func ReadOnlyTaintKey(namespace, name string) string {
	return namespace + readOnlyTaintKeySuffix + name
}
