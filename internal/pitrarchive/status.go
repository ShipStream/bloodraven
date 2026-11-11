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

// ClockSkewMargin is δ (D-50): the fixed margin of every comparison that
// crosses clocks (a sidecar's record time against a server's commit time,
// two sites' records). The planner's default δ and the sidecar's
// confirmation delay both derive from it, so they cannot drift apart.
const ClockSkewMargin = 5 * time.Second

// PromotionConfirmDelay is how long after a promotion record's ObservedAt
// the read of the observation that confirms the record must start: 2δ. A
// witness the record's time cannot order (committed no later than
// ObservedAt + δ on the server's clock) was committed before ObservedAt +
// 2δ on the sidecar's, so the confirmation holds it (issue #269).
const PromotionConfirmDelay = 2 * ClockSkewMargin

// PromotionProbeSpread bounds how long a role probe may run for its
// observation to open a promotion record. A record's ObservedAt is when
// the probe returned, so the server read the record's GTIDExecuted at most
// this long before it. The sidecar enforces it and the planner relies on
// it (a transaction that only a record's confirmation holds was committed
// no earlier than this before ObservedAt).
const PromotionProbeSpread = time.Second

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
//
// A promotion record anchors a witness w in two ways (pitrplan/witness.go):
// by time, with GTIDExecuted, when w is not in it and it was observed more
// than δ before w committed; and by containment (issue #269), with
// PromotionGTIDExecuted, when w is outside that set and inside GTIDExecuted
// or the confirmation. The second needs no clock to order the record and
// w, so it covers the transactions an episode commits before and shortly
// after its record is observed. A record with a promotion set also dates
// its episode for the planner: the transactions its server committed
// outside that set were committed no earlier than ProbeInterval before
// ObservedAt, on the clock of ObservedAt (the sidecar probes the mysqld of
// its own pod), so an archived copy of one of them with an earlier commit
// time is another transaction under the same GTID.
//
// The fields added for issue #269 are optional. A record without them (an
// older sidecar's, or one whose evidence could not be established) loads
// and behaves as before; a writer that does not know them drops them when
// it rewrites the manifest, which only removes that evidence.
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
	// The planner does not read it: it is the first of the two sources of
	// PromotionGTIDExecuted, which is the field that carries the proof,
	// and is kept beside it for diagnosis. It is such a source only when
	// the observation that opened the episode still contained it; a record
	// whose server lost transactions in between carries it without a
	// PromotionGTIDExecuted.
	ReadOnlyGTIDExecuted string `json:"readOnlyGtidExecuted,omitempty"`
	// PromotionGTIDExecuted is the promotion set P of a promotion record
	// (issue #269): a set every member of which this server incarnation
	// executed before any transaction under its own server_uuid that
	// ReadOnlyGTIDExecuted does not hold, so before every transaction this
	// episode committed under the server's own server_uuid. The sidecar
	// derives it when it opens the record, as
	//
	//	P = ReadOnlyGTIDExecuted ∪ Previous_gtids(F*)
	//
	// F* being the newest local binlog file whose Previous_gtids hold no
	// transaction under the server's own server_uuid beyond
	// ReadOnlyGTIDExecuted and nothing GTIDExecuted lacks: a file the
	// server opened before the episode's first own transaction.
	// Previous_gtids are a subset of what the server had executed when it
	// opened the file (MySQL 9.7 leaves out GTIDs that are only in
	// mysql.gtid_executed); only that, and that they hold every transaction
	// the server had committed itself, is relied on. When the
	// operator's promotion-time FLUSH BINARY LOGS was acknowledged, F* is
	// the file it opened and P is the promotion GTID set; otherwise F* is
	// an older file and P is smaller. "" (absent) when the sidecar could
	// not establish it: no ReadOnlyGTIDExecuted, an unreadable binlog
	// header, a read that exceeded its bound, or a server that changed
	// while it was derived. Always a subset of GTIDExecuted; a reader
	// ignores one that is not.
	PromotionGTIDExecuted string `json:"promotionGtidExecuted,omitempty"`
	// ConfirmedAt and ConfirmedGTIDAdded are the record's confirmation
	// (issue #269), added once by the sidecar that wrote the record, and
	// only to a record that carries PromotionGTIDExecuted:
	// ConfirmedAt is its clock when a later role probe of the same
	// episode returned, a probe whose read started at least
	// PromotionConfirmDelay after ObservedAt, with the server writable in
	// every observation in between, of the same incarnation, and a
	// gtid_executed containing GTIDExecuted. ConfirmedGTIDAdded is what
	// that gtid_executed held beyond GTIDExecuted ("" when nothing): the
	// confirmed set is GTIDExecuted ∪ ConfirmedGTIDAdded. Only the
	// difference is stored: the confirmed set contains GTIDExecuted by
	// construction, and a full copy would add one more whole GTID set per
	// record to every manifest save. A zero ConfirmedAt means no
	// confirmation: the sidecar or the server stopped, or the episode
	// ended, before that probe.
	ConfirmedAt        time.Time `json:"confirmedAt,omitzero"`
	ConfirmedGTIDAdded string    `json:"confirmedGtidAdded,omitempty"`
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
