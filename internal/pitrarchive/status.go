package pitrarchive

import "time"

// ObservedState is a consumer's view of its archive's descriptor. The API
// mirrors it as v1alpha1.PITRArchiveObservedState.
type ObservedState string

const (
	// StateAwaitingDescriptor: no descriptor yet (a replica before the
	// writer created it, or a writer before its first writable scan).
	StateAwaitingDescriptor ObservedState = "AwaitingDescriptor"
	// StateVerified: the descriptor passed VerifyDescriptor for this role.
	StateVerified ObservedState = "Verified"
	// StateRefused: a check failed; Reason says which.
	StateRefused ObservedState = "Refused"
)

// ArchiveStatus is the `archive` block of the sidecar's /archiver/status
// (§3.7). It is shared by internal/sidecar.Status and
// internal/mysql.ArchiverStatus so the two stay wire-identical.
type ArchiveStatus struct {
	// Destination is Destination.String() (BLOODRAVEN_PITR_DESTINATION).
	Destination string `json:"destination"`
	// ArchiveID is the verified descriptor's ID; "" until verified.
	ArchiveID      string         `json:"archiveID,omitempty"`
	EncryptionMode EncryptionMode `json:"encryptionMode,omitempty"`
	State          ObservedState  `json:"state"`
	// Reason is set when State is Refused (one of ArchiveReadyReasons).
	Reason RefusalReason `json:"reason,omitempty"`
	// KeyRevision is the mounted key snapshot's name (#203,
	// BLOODRAVEN_PITR_KEY_REVISION); "" for None and before #203.
	KeyRevision string `json:"keyRevision,omitempty"`
}

// MaxPromotionRecords is how many promotion records a manifest keeps
// (newest last). Dropping an older record only removes anchors and loss
// evidence, so the cap fails closed (D-43).
const MaxPromotionRecords = 256

// PromotionRecord is appended to a site's own manifest when its sidecar
// first observes its server writable in an episode, before any upload of
// that episode (D-43, §3.7). The planner uses records to anchor replica-copy
// witnesses and to prove losses in latest mode.
type PromotionRecord struct {
	ServerUUID   string `json:"serverUuid"`
	ServerID     uint32 `json:"serverId"`
	GTIDExecuted string `json:"gtidExecuted"`
	// ObservedAt is the sidecar's clock (the mysqld pod's node clock) when
	// the role probe that read GTIDExecuted returned, truncated to the
	// microsecond, UTC. The sidecar opens records only from probes that
	// ran for at most a second, so the snapshot is at most that much older.
	ObservedAt time.Time `json:"observedAt"`
	// ReadOnlyGTIDExecuted is the gtid_executed of the read-only
	// observation of the same server directly before this episode, when
	// the sidecar saw one ("" otherwise: a sidecar start while writable, a
	// failed probe in between, or a new server incarnation while writable).
	// Recorded for diagnosis; the planner does not use it.
	ReadOnlyGTIDExecuted string `json:"readOnlyGtidExecuted,omitempty"`
}

// HistoryViolation names a setting that makes a site's binlogs unable to
// certify history (D-34).
type HistoryViolation string

const (
	ViolationReplicationFilter HistoryViolation = "ReplicationFilter" // replication applier (or global) filters
	ViolationBinlogFilter      HistoryViolation = "BinlogFilter"      // Binlog_Do_DB / Binlog_Ignore_DB
	ViolationBinlogFormat      HistoryViolation = "BinlogFormat"      // binlog_format != ROW
	ViolationBinlogRowImage    HistoryViolation = "BinlogRowImage"    // binlog_row_image != FULL
	ViolationCommitOrder       HistoryViolation = "CommitOrder"       // replica_preserve_commit_order = OFF
)

// HistoryViolations is the closed set of violations.
var HistoryViolations = []HistoryViolation{
	ViolationReplicationFilter, ViolationBinlogFilter, ViolationBinlogFormat,
	ViolationBinlogRowImage, ViolationCommitOrder,
}

// HistoryIntegrity is the persisted per-site marker (D-34). A sidecar sets
// it in its own manifest on the first violation; no writer ever clears it,
// and clearing the setting does not clear it. A plan that uses a marked
// site's rows refuses PITRArchiveUnsafeHistory. The same shape is reported
// in /archiver/status.
type HistoryIntegrity struct {
	Violation HistoryViolation `json:"violation"`
	Detail    string           `json:"detail"`
	At        time.Time        `json:"at"`
}
