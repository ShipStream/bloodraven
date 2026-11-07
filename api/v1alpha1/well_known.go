package v1alpha1

// Well-known metadata keys: the labels, annotations, finalizers and the node
// taint Bloodraven reads or writes. Each key's comment ends with its
// stability:
//
//   - Public: users and their tooling write or select on it.
//   - Operator-owned read-only: the operator writes it and users may read
//     it, but must not write or remove it.
//   - Internal (may change): the operator's own bookkeeping.

// MysqlFailoverGroup annotations a user sets to request an action.
const (
	// PlannedFailoverAnnotation triggers a graceful, cooldown-respecting
	// switchover to the named PrimaryCandidate site. The operator removes
	// it once the request is decided: accepted, rejected or skipped. A
	// request that is deferred (onCooldown=Defer, a deployment hold), or
	// that waits for admission, keeps it until then; removing it cancels a
	// deferred run. The value is a site name, optionally followed by
	// key=value overrides separated by ':', for example "pdx" or
	// "pdx:maxLagWait=30s". Public.
	PlannedFailoverAnnotation = "bloodraven.shipstream.io/planned-failover"

	// AcknowledgePlannedFailoverHoldAnnotation acknowledges an automation
	// hold. Its value is the held run's runID
	// (status.plannedFailoverHold.runID). Public; wrapped by kubectl
	// bloodraven ack-planned-failover-hold.
	AcknowledgePlannedFailoverHoldAnnotation = "bloodraven.shipstream.io/acknowledge-planned-failover-hold"

	// RecloneAnnotation asks the operator to reclone one site from the
	// current primary with CLONE INSTANCE. The value is
	// "<site>:confirm=<group name>", or "<site>:<gtid prefix>" when the
	// site has a recorded divergent GTID set (the prefix must match it).
	// The operator also writes it to drain status.restoreReclones. Public.
	RecloneAnnotation = "bloodraven.shipstream.io/reclone-site"

	// RevokeDeploymentLeasesAnnotation revokes every deployment lease the
	// group holds. Public.
	RevokeDeploymentLeasesAnnotation = "bloodraven.shipstream.io/revoke-deployment-leases"

	// RotateKeyringAnnotation triggers an InnoDB master-key rotation on the
	// named site. The operator refuses to rotate the active primary: rotate
	// the replicas, run a planned failover, then rotate the former primary.
	// Public.
	RotateKeyringAnnotation = "bloodraven.shipstream.io/rotate-keyring"

	// ReleasePITRArchiveBindingAnnotation releases one PITR archive
	// binding: "<binding id>:confirm=<metadata.name>". Never commit it in
	// GitOps. Public.
	ReleasePITRArchiveBindingAnnotation = "bloodraven.shipstream.io/release-pitr-archive-binding"
)

// MysqlFailoverGroup annotations the operator writes.
const (
	// LastFailoverAnnotation and LastFailoverTargetAnnotation copy the
	// anti-flap record (status.lastFailover, status.lastFailoverTarget) to
	// object metadata, so a status-write outage that overlaps an operator
	// restart cannot lose the cooldown. LastFailoverAnnotation is an RFC
	// 3339 UTC instant at second precision; LastFailoverTargetAnnotation is
	// the target site name. Both are written in one patch on every
	// promotion. Operator-owned read-only.
	LastFailoverAnnotation       = "bloodraven.shipstream.io/last-failover"
	LastFailoverTargetAnnotation = "bloodraven.shipstream.io/last-failover-target"

	// PromotionGtidExecutedAnnotation is the out-of-band copy of the
	// promotion watermark, beside LastFailoverAnnotation and
	// LastFailoverTargetAnnotation. Operator-owned read-only.
	PromotionGtidExecutedAnnotation = "bloodraven.shipstream.io/promotion-gtid-executed"

	// PromotionGtidExecutedForAnnotation binds the watermark to its
	// failover event: "<last-failover> <last-failover-target>", written in
	// the same patch. A watermark whose binding does not name the event the
	// other two keys record is no watermark. Operator-owned read-only.
	PromotionGtidExecutedForAnnotation = "bloodraven.shipstream.io/promotion-gtid-executed-for"

	// PlannedFailoverStatusFenceAnnotation is bumped (an ISO 8601 basic UTC
	// instant with nanoseconds) with a write conditioned on the
	// resourceVersion an ambiguous status write was made against. Its
	// success proves that write can never commit. Internal (may change).
	PlannedFailoverStatusFenceAnnotation = "bloodraven.shipstream.io/planned-failover-status-fence"

	// PlannedFailoverTerminalAnnotation ends a run outside /status when its
	// status writes stall. Its value is a versioned JSON marker
	// ({"schemaVersion":1,"runID":…,"outcome":…,"reason":…}). Every
	// planned-failover predicate treats the run it names as terminal.
	// Internal (may change).
	PlannedFailoverTerminalAnnotation = "bloodraven.shipstream.io/planned-failover-terminal"

	// PlannedFailoverStallSinceAnnotation is the durable start of the
	// status-stall deadline: "<runID>:since=<YYYYMMDDThhmmssZ>". Internal
	// (may change).
	PlannedFailoverStallSinceAnnotation = "bloodraven.shipstream.io/planned-failover-stall-since"

	// PlannedFailoverManagerAbsentSinceAnnotation is the durable start of
	// the manager-absence deadline, in the same form as
	// PlannedFailoverStallSinceAnnotation. Internal (may change).
	PlannedFailoverManagerAbsentSinceAnnotation = "bloodraven.shipstream.io/planned-failover-manager-absent-since"

	// PlannedFailoverConfirmedAnnotation records, before the run releases
	// its claim, that the run's promotion was confirmed, recorded and
	// published: "<runID>:since=<confirmedAt>", in the same form as
	// PlannedFailoverStallSinceAnnotation. A restart before the final
	// status write then does not take the released claim back. Internal
	// (may change).
	PlannedFailoverConfirmedAnnotation = "bloodraven.shipstream.io/planned-failover-confirmed"

	// EncryptionStatusFenceAnnotation is bumped to advance the group's
	// resourceVersion, so a status write conditioned on an older version
	// (one whose response was lost) can no longer commit. Its value, an RFC
	// 3339 timestamp, carries no meaning. Internal (may change).
	EncryptionStatusFenceAnnotation = "shipstream.io/encryption-status-fence"

	// RestoreInPlaceConfirmAnnotation is on the in-place restore Job: the
	// spec.restoreInPlace.confirm token the Job was created for, so a Job
	// left over from an earlier request with the same name is not mistaken
	// for the current one. Internal (may change).
	RestoreInPlaceConfirmAnnotation = "bloodraven.shipstream.io/restore-confirm"
)

// Finalizers. Internal (may change).
const (
	// FailoverGroupFinalizer runs the graceful-shutdown sequence before a
	// MysqlFailoverGroup is deleted.
	FailoverGroupFinalizer = "shipstream.io/graceful-shutdown"
	// MysqlDatabaseFinalizer guards the MySQL-side cleanup of a
	// MysqlDatabase.
	MysqlDatabaseFinalizer = "shipstream.io/mysqldatabase"
	// MysqlBackupFinalizer guards the artifact cleanup of a MysqlBackup.
	MysqlBackupFinalizer = "shipstream.io/mysqlbackup"
	// MysqlBackupVerificationFinalizer guards the cleanup of a
	// MysqlBackupVerification's resources.
	MysqlBackupVerificationFinalizer = "shipstream.io/mysqlbackup-verification"
)

// Labels on the objects the operator manages for a MysqlFailoverGroup
// (pods, Services, Deployments, StatefulSets, Secrets).
const (
	// FailoverGroupLabel names the owning MysqlFailoverGroup. It is also
	// on MysqlBackups and MysqlBackupVerifications (spec.failoverGroupRef).
	// Operator-owned read-only.
	FailoverGroupLabel = "shipstream.io/failover-group"
	// SiteLabel names the site. Operator-owned read-only.
	SiteLabel = "shipstream.io/site"
	// RoleLabel is the site's role. Operator-owned read-only.
	RoleLabel = "shipstream.io/role"
	// HealthyLabel is "yes" or "no" by the site's last health poll.
	// Operator-owned read-only.
	HealthyLabel = "shipstream.io/healthy"
	// DragonflyRoleLabel is a Dragonfly pod's operator-assigned role:
	// "master" or "replica". The active Service selects on it together
	// with DragonflyTrafficLabel. Operator-owned read-only.
	DragonflyRoleLabel = "shipstream.io/dragonfly-role"
	// DragonflyTrafficLabel gates whether a Dragonfly pod is eligible for
	// the active Service: only "enabled" is selected. A planned failover
	// removes it from the source pod before the takeover, so the Service
	// sheds the old endpoint first. Operator-owned read-only.
	DragonflyTrafficLabel = "shipstream.io/dragonfly-traffic"
	// KeyringVersionLabel is the escrow version on a site's keyring
	// escrow Secret. Internal (may change).
	KeyringVersionLabel = "shipstream.io/keyring-version"
)

// Annotations on the objects the operator manages for a MysqlFailoverGroup.
// Internal (may change).
const (
	// KeyringDigestAnnotation records the digest of the escrowed keyring
	// bytes. It is informational: the operator always recomputes it.
	KeyringDigestAnnotation = "shipstream.io/keyring-digest"
	// SpecHashAnnotation is the hash of the rendered spec of a managed
	// object.
	SpecHashAnnotation = "shipstream.io/spec-hash"
	// ManagedServiceAnnotationsAnnotation lists the Service annotations the
	// operator manages, so it can remove one that leaves the spec.
	ManagedServiceAnnotationsAnnotation = "shipstream.io/managed-service-annotations"
	// CredentialHashAnnotation records the hash of the credentials last
	// applied.
	CredentialHashAnnotation = "shipstream.io/credential-hash"
	// SystemSchemaRestrictionAnnotation records the credential hash for
	// which the application user's system-schema restriction last
	// succeeded.
	SystemSchemaRestrictionAnnotation = "shipstream.io/system-schema-restriction"
)

// Labels on MysqlBackups, MysqlBackupVerifications and their Jobs.
const (
	// MysqlBackupLabel names the MysqlBackup a Job or a verification
	// belongs to. Operator-owned read-only.
	MysqlBackupLabel = "shipstream.io/mysqlbackup"
	// BackupProfileLabel names the backup profile (spec.profileName).
	// Users select on it, and set it with FailoverGroupLabel when they
	// create a MysqlBackup by hand. Public.
	BackupProfileLabel = "shipstream.io/backup-profile"
	// BackupScheduleLabel names the schedule that created a MysqlBackup.
	// Public.
	BackupScheduleLabel = "shipstream.io/backup-schedule"
	// ResourceLabel classifies an operator-created Job or Secret.
	// Operator-owned read-only.
	ResourceLabel = "shipstream.io/resource"
	// BackupEncryptedLabel marks a MysqlBackup whose artifact is encrypted.
	// Operator-owned read-only.
	BackupEncryptedLabel = "shipstream.io/backup-encrypted"
	// MysqlBackupVerificationLabel names the MysqlBackupVerification a Job
	// belongs to. Operator-owned read-only.
	MysqlBackupVerificationLabel = "shipstream.io/mysqlbackup-verification"
	// VerificationScheduleLabel names the verification schedule that
	// created a MysqlBackupVerification. Public.
	VerificationScheduleLabel = "shipstream.io/verification-schedule"
)

// TriggeredByAnnotation records what created a MysqlBackup or a
// MysqlBackupVerification: "manual", "schedule/<name>" or
// "retry/<name>/attempt-<n>". Set by the schedules, the trigger commands and
// kubectl bloodraven. Informational; nothing reads it. Public.
const TriggeredByAnnotation = "bloodraven.shipstream.io/triggered-by"

// Backup bookkeeping. Internal (may change).
const (
	// BackupRetryOfLabel names the MysqlBackup a retry replaces.
	BackupRetryOfLabel = "shipstream.io/backup-retry-of"
	// BackupRetryRootAnnotation and BackupRetryRootUIDAnnotation name the
	// first backup of a retry chain.
	BackupRetryRootAnnotation    = "shipstream.io/backup-retry-root"
	BackupRetryRootUIDAnnotation = "shipstream.io/backup-retry-root-uid"
	// BackupAttemptAnnotation is the attempt number within a retry chain.
	BackupAttemptAnnotation = "shipstream.io/backup-attempt"
	// CleanupFailureRecordedAnnotation marks a failed cleanup Job whose
	// failure Event and metric were emitted, so each Job counts once.
	CleanupFailureRecordedAnnotation = "shipstream.io/cleanup-failure-recorded"
	// BackupJobFailoverGroupUIDAnnotation pins the source group's UID on a
	// backup Job and its pod; status.failoverGroupUID is recovered from it.
	// The same key is the FailoverGroupUIDLabel of key snapshots.
	BackupJobFailoverGroupUIDAnnotation = "bloodraven.shipstream.io/failover-group-uid"
	// BackupJobPITRArchiveIDAnnotation pins the source group's PITR archive
	// ID ("" when PITR is off) on a backup Job; status.pitrArchiveID is
	// recovered from it.
	BackupJobPITRArchiveIDAnnotation = "bloodraven.shipstream.io/pitr-archive-id"
	// VerificationBackupAnnotation names the MysqlBackup a scheduled
	// verification was created for.
	VerificationBackupAnnotation = "shipstream.io/verification-backup"
)

// VerificationHoldAnnotationPrefix marks a MysqlBackup (and a PITR archive
// key snapshot) that a MysqlBackupVerification is reading. The full key is
// the prefix followed by the verification's UID; the value is its name.
// Retention and artifact cleanup leave the object alone while a hold on it
// is live, so removing one by hand while its verification runs can delete
// data under it. Operator-owned read-only.
const VerificationHoldAnnotationPrefix = "hold.verification.shipstream.io/"

// PITR archive binding metadata on key snapshots (Secrets) and the Jobs that
// mount them. Internal (may change), except where noted.
const (
	// PITRRenderAnnotation is on every site Deployment: the canonical JSON
	// of the PITR identity and access fields it renders.
	PITRRenderAnnotation = "bloodraven.shipstream.io/pitr-render"
	// PITRDestinationAnnotation is on every archive key snapshot: the
	// normalized archive destination.
	PITRDestinationAnnotation = "bloodraven.shipstream.io/pitr-destination"
	// PITRAcceptedAnnotation is on an accepted archive key snapshot: a
	// versioned JSON acceptance record. Operator-owned read-only; never
	// edit it.
	PITRAcceptedAnnotation = "bloodraven.shipstream.io/pitr-accepted"
	// PITRBindingIDLabel is on every archive key snapshot: the binding id.
	// It is the release selector, so a release also covers snapshots
	// whose binding entry was dropped or lost.
	PITRBindingIDLabel = "bloodraven.shipstream.io/pitr-binding-id"
	// FailoverGroupUIDLabel is on archive key and dump key snapshots: the
	// group's metadata.uid. Snapshots have no owner reference, so it scopes
	// every listing to one group incarnation. Same key as
	// BackupJobFailoverGroupUIDAnnotation.
	FailoverGroupUIDLabel = "bloodraven.shipstream.io/failover-group-uid"
	// MysqlBackupUIDLabel is on a dump key snapshot: the MysqlBackup's uid.
	MysqlBackupUIDLabel = "bloodraven.shipstream.io/mysqlbackup-uid"
	// PITRKeySecretLabel is on check, restore, verification and in-place
	// staging Jobs: the archive key snapshot the Job mounts. A release is
	// refused while such a Job has not stopped.
	PITRKeySecretLabel = "bloodraven.shipstream.io/pitr-key-secret"
	// PITRKeySecretUIDAnnotation is on a check Job that mounts a candidate
	// snapshot: the snapshot's UID when the Job was created. A verdict is
	// accepted only for that object.
	PITRKeySecretUIDAnnotation = "bloodraven.shipstream.io/pitr-key-secret-uid"
)

// Deployment API lease metadata, on the coordination.k8s.io Leases the
// operator creates for deployment leases. Internal (may change).
const (
	// DeploymentLeaseAnnotation holds the lease record as JSON.
	DeploymentLeaseAnnotation = "bloodraven.shipstream.io/deployment-lease"
	// DeploymentKindLabel is the lease kind.
	DeploymentKindLabel = "bloodraven.shipstream.io/deployment-kind"
	// DeploymentStateLabel is the lease state.
	DeploymentStateLabel = "bloodraven.shipstream.io/deployment-state"
)

// readOnlyTaintKeyPrefix is the prefix of ReadOnlyTaintKey.
const readOnlyTaintKeyPrefix = "shipstream.io/db-readonly-"

// ReadOnlyTaintValue is the value of the read-only node taint; its effect is
// NoExecute.
const ReadOnlyTaintValue = "true"

// ReadOnlyTaintKey returns the key of the NoExecute taint the operator puts
// on a site's nodes (spec.sites[].taintNodeSelector) while the site is read
// only, for the failover group named group. Workloads that must keep
// running there tolerate it. Public.
func ReadOnlyTaintKey(group string) string {
	return readOnlyTaintKeyPrefix + group
}
