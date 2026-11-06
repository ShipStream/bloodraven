package pitrarchive

import (
	"encoding/json"
	"fmt"
	"time"
)

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

// MaxPromotionRecords is how many records (promotion and reset records
// together) a manifest keeps (newest last). Dropping an older record only
// removes anchors and loss evidence, so the cap fails closed (D-43).
const MaxPromotionRecords = 256

// RecordKind says what a site's manifest record reports.
type RecordKind string

const (
	// RecordPromotion ("" on the wire, the default) is a promotion
	// record: the first observation of a writable episode, written
	// before any upload of that episode (D-43).
	RecordPromotion RecordKind = ""
	// RecordReset is an incarnation-reset record: the sidecar observed
	// its server (in any role) with another server_uuid or server_id
	// than its previous observation or the site's latest record, or with
	// a gtid_executed that no longer contains theirs (a reclone, a
	// reset, a re-seed, a replaced datadir). The planner treats it only
	// as a fence: it is no anchor and no loss evidence.
	RecordReset RecordKind = "Reset"
)

// PromotionRecord is a record in a site's own manifest (D-43, §3.7): a
// promotion record, appended when the site's sidecar first observes its
// server writable in an episode, before any upload of that episode, or an
// incarnation-reset record (Kind RecordReset). The planner uses promotion
// records to anchor replica-copy witnesses and to prove losses in latest
// mode, and every record of a site as a fence: a record observed after an
// anchor and not provably after the witness means the witness may belong
// to a later episode or server incarnation.
type PromotionRecord struct {
	// Kind is RecordPromotion (omitted) or RecordReset. A reader treats
	// any other kind as a fence only.
	Kind         RecordKind `json:"kind,omitempty"`
	ServerUUID   string     `json:"serverUuid"`
	ServerID     uint32     `json:"serverId"`
	GTIDExecuted string     `json:"gtidExecuted"`
	// ObservedAt is the sidecar's clock (the mysqld pod's node clock) when
	// the role probe that read GTIDExecuted returned, truncated to the
	// microsecond, UTC. The sidecar opens promotion records only from
	// probes that ran for at most a second, so the snapshot is at most
	// that much older.
	ObservedAt time.Time `json:"observedAt"`
	// ProbeInterval bounds how long before ObservedAt the change this
	// record reports (a new episode or a new server incarnation) can
	// have happened: the time since the read of the sidecar's previous
	// observation that still showed the earlier state, and at least the
	// configured probe interval (PEER_CHECK_INTERVAL). For the first
	// observation after a sidecar start that found the server changed
	// against the site's latest record, it reaches back to that record.
	// Zero (absent) is unknown: the planner treats it as unbounded.
	ProbeInterval Duration `json:"probeInterval,omitempty"`
	// ReadOnlyGTIDExecuted is the gtid_executed of the read-only
	// observation of the same server directly before this episode, when
	// the sidecar saw one ("" otherwise: a sidecar start while writable, a
	// failed probe in between, or a new server incarnation while writable).
	// Recorded for diagnosis; the planner does not use it.
	ReadOnlyGTIDExecuted string `json:"readOnlyGtidExecuted,omitempty"`
}

// Duration is a time.Duration that JSON-encodes as a Go duration string
// ("5s", "1m2.000001s").
type Duration time.Duration

// MarshalJSON encodes d as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON decodes a non-negative Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	if v < 0 {
		return fmt.Errorf("duration %q is negative", s)
	}
	*d = Duration(v)
	return nil
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
