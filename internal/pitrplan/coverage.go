package pitrplan

import (
	"encoding/json"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// CoverageVersion is the coverage.json schema version (§4.12). restore.py's
// _read_pitr_coverage refuses any other version (PITRCoverageInvalid).
//
// The file never outlives its Job (an emptyDir between pitr-download and
// restore.py), so the version only guards a pod that mixes two releases.
// It changes when a reader of another release could misread the file. A
// field added for restore.py to require does not need a new version:
// segments[].startPosition (issue #271) is ignored by an older restore.py,
// which scans each file from its start with the same result, and a newer
// restore.py refuses a file without it by name.
const CoverageVersion = 2

// CoverageFileName is the file pitr-download writes into its output
// directory, atomically (write "<name>.part", then rename), including on
// refusal.
const CoverageFileName = "coverage.json"

// Mode is the replay mode as written in coverage.json.
type Mode string

const (
	// ModeTimestamp replays up to, and excluding, the witness of StopTime.
	ModeTimestamp Mode = "timestamp"
	// ModeLatest replays to the archive head.
	ModeLatest Mode = "latest"
)

// Outcome is the plan's outcome. The API mirrors it as
// v1alpha1.PointInTimeOutcome.
type Outcome string

const (
	// OutcomeTargetCovered: timestamp mode, a witness at or after T exists.
	OutcomeTargetCovered Outcome = "TargetCovered"
	// OutcomePartial: timestamp mode, no witness, allowPartial set (§4.8).
	OutcomePartial Outcome = "Partial"
	// OutcomeArchiveHead: latest mode.
	OutcomeArchiveHead Outcome = "ArchiveHead"
)

// GroundTruth says how the witness's predecessor set Λ was established
// (D-39).
type GroundTruth string

const (
	// GroundTruthOrigin: Λ is the predecessor set of the witness's origin
	// occurrences, which agree.
	GroundTruthOrigin GroundTruth = "Origin"
	// GroundTruthAnchored: Λ comes from a replica occurrence accepted
	// through an anchor (promotion records and/or an origin row).
	GroundTruthAnchored GroundTruth = "Anchored"
)

// Coverage is coverage.json version 2 (§4.12). GTID sets are canonical
// (GTIDSet.String()); "" is the empty set. Times are backupmeta.MicroTime
// (µs RFC3339 UTC, null when absent).
//
// On refusal the file is still written, with RefusalReason set (the
// termination message, which starts with its pitrarchive.Token) and no
// Files or Segments.
type Coverage struct {
	Version      int                  `json:"version"`
	Mode         Mode                 `json:"mode"`
	StopTime     backupmeta.MicroTime `json:"stopTime"` // null in latest mode
	AllowPartial bool                 `json:"allowPartial"`
	Outcome      Outcome              `json:"outcome"` // "" on refusal
	// RefusalReason is "" on success.
	RefusalReason string `json:"refusalReason"`

	Archive CoverageArchive `json:"archive"`
	Dump    CoverageDump    `json:"dump"`

	// Baseline is B, the dump's gtidExecuted.
	Baseline string `json:"baseline"`
	// BackupNewestCommitTime is maxOct(B): the newest commit time of a
	// member of B read from archived bytes; null when none was read (−∞).
	// It is proven only when every archived row holding members of B was
	// read; when one was pruned, lost, or left unread on its hint, it is a
	// lower bound and the plan passed the D-52 server-clock bound instead.
	BackupNewestCommitTime backupmeta.MicroTime `json:"backupNewestCommitTime"`
	// Excluded is X (excludeGtids).
	Excluded string `json:"excluded"`
	// Lineage is Λ.
	Lineage string `json:"lineage"`
	// Replay is P = Λ ∖ (B ∪ X); the union of the segments' GTIDs.
	Replay string `json:"replay"`
	// PostStop is Q = (∪ gtids of staged files) ∖ Λ ∖ X.
	PostStop string `json:"postStop"`
	// Divergent is D: in timestamp mode the archived transactions
	// committed in (maxOct(B), T) outside Λ ∪ B, excluded ones included.
	// A successful plan proved D ⊆ X. On a PITRArchiveDivergentHistory
	// refusal it is the complete set the message asks to exclude (in
	// latest mode, the unexcluded loss evidence and audit findings), which
	// is not ⊆ X.
	Divergent string `json:"divergent"`
	// OnHistoryExclusions is the D-48 set: X ∩ Λ members that precede a
	// replayed transaction.
	OnHistoryExclusions string `json:"onHistoryExclusions"`
	// Lost is latest mode's loss evidence (D-37): on success, the flagged
	// transactions X excludes; on a PITRArchiveDivergentHistory refusal,
	// every flagged transaction.
	Lost string `json:"lost"`

	// Witness is set in timestamp mode with outcome TargetCovered.
	Witness *CoverageWitness `json:"witness"`
	// Head is set in latest mode, and for Partial.
	Head *CoverageHead `json:"head"`

	NewestReplayedCommitTime backupmeta.MicroTime `json:"newestReplayedCommitTime"`
	ReplayedTransactionCount int64                `json:"replayedTransactionCount"`
	// Sites are the sites whose files are staged, sorted.
	Sites    []string          `json:"sites"`
	Files    []CoverageFile    `json:"files"`
	Segments []CoverageSegment `json:"segments"`
}

// CoverageArchive identifies the archive the plan read.
type CoverageArchive struct {
	ArchiveID      string                     `json:"archiveID"`
	Destination    string                     `json:"destination"`
	Source         pitrarchive.Source         `json:"source"`
	EncryptionMode pitrarchive.EncryptionMode `json:"encryptionMode"`
}

// CoverageDump identifies the dump the plan starts from (from dump.json and
// its stamp). restore.py checks Archive.Source.UID == Dump.SourceUID.
type CoverageDump struct {
	Location      string               `json:"location"`
	BackupUID     types.UID            `json:"backupUID"`
	SourceUID     types.UID            `json:"sourceUID"`
	PITRArchiveID string               `json:"pitrArchiveID"`
	Site          string               `json:"site"`
	ServerID      uint32               `json:"serverID"`
	End           backupmeta.MicroTime `json:"end"`
}

// CoverageWitness is the witness w (§4.4).
type CoverageWitness struct {
	GTID        string               `json:"gtid"`
	Site        string               `json:"site"`
	File        string               `json:"file"`
	ID          string               `json:"id"`
	CommitTime  backupmeta.MicroTime `json:"commitTime"`
	GroundTruth GroundTruth          `json:"groundTruth"`
	// Anchor is set when GroundTruth is Anchored.
	Anchor *CoverageAnchor `json:"anchor,omitempty"`
}

// CoverageAnchor records what anchored a replica-copy witness (D-39).
type CoverageAnchor struct {
	// Records name the promotion records used, as "<site>#<index>" into
	// that site's manifest Promotions.
	Records []string `json:"records"`
	// OriginRow is the origin row used, as "<site>/<name>/<id>", or "".
	OriginRow string `json:"originRow"`
}

// CoverageHead is the archive head h* (latest mode, §4.8).
type CoverageHead struct {
	Site string `json:"site"`
	File string `json:"file"`
	ID   string `json:"id"`
}

// CoverageFile is one staged file.
type CoverageFile struct {
	// Path is relative to the staging directory ("<site>/<name>.<id>").
	Path          string `json:"path"`
	Site          string `json:"site"`
	Name          string `json:"name"`
	ID            string `json:"id"`
	SHA256        string `json:"sha256"`
	PreviousGTIDs string `json:"previousGTIDs"`
	GTIDs         string `json:"gtids"`
}

// CoverageSegment is one replay segment: `mysqlbinlog
// --start-position=<StartPosition> --include-gtids=<GTIDs>` over
// Files[File], in order.
type CoverageSegment struct {
	// File indexes Coverage.Files.
	File int `json:"file"`
	// StartPosition is the offset in the file of the GTID event of the
	// first transaction the segment replays. restore.py requires it on
	// every segment and checks, before the load, that a GTID event starts
	// there.
	StartPosition int64  `json:"startPosition"`
	GTIDs         string `json:"gtids"`
	// StartPredecessors is pred(o) at the segment start; restore.py checks
	// StartPredecessors ∖ excluded == baseline ∪ earlier segments (R4).
	StartPredecessors string               `json:"startPredecessors"`
	Transactions      int64                `json:"transactions"`
	FirstCommitTime   backupmeta.MicroTime `json:"firstCommitTime"`
	LastCommitTime    backupmeta.MicroTime `json:"lastCommitTime"`
}

// Marshal returns the coverage.json bytes: Version forced to
// CoverageVersion, nil lists written as [] (restore.py iterates them), and
// two-space indentation.
func (c Coverage) Marshal() ([]byte, error) {
	c.Version = CoverageVersion
	if c.Sites == nil {
		c.Sites = []string{}
	}
	if c.Files == nil {
		c.Files = []CoverageFile{}
	}
	if c.Segments == nil {
		c.Segments = []CoverageSegment{}
	}
	if c.Witness != nil && c.Witness.Anchor != nil && c.Witness.Anchor.Records == nil {
		w, a := *c.Witness, *c.Witness.Anchor
		a.Records = []string{}
		w.Anchor = &a
		c.Witness = &w
	}
	return json.MarshalIndent(c, "", "  ")
}

// Coverage renders the plan as coverage.json version 2. archive and dump
// come from the glue (descriptor and dump.json); refusal is the
// termination message ("" on success).
func (p *Plan) Coverage(archive CoverageArchive, dump CoverageDump, refusal string) Coverage {
	c := Coverage{
		Version: CoverageVersion, Mode: p.Mode, AllowPartial: p.AllowPartial, Outcome: p.Outcome, RefusalReason: refusal,
		Archive: archive, Dump: dump,
		Baseline: p.Baseline.String(), Excluded: p.Excluded.String(), Lineage: p.Lineage.String(), Replay: p.Replay.String(),
		PostStop: p.PostStop.String(), Divergent: p.Divergent.String(), OnHistoryExclusions: p.OnHistoryExclusions.String(), Lost: p.Lost.String(),
		Sites: p.Sites,
	}
	if refusal != "" {
		c.Outcome = ""
	}
	if p.Mode == ModeTimestamp && !p.Stop.IsZero() {
		c.StopTime = backupmeta.NewMicroTime(p.Stop)
	}
	if p.BackupNewestCommit != 0 {
		c.BackupNewestCommitTime = microTime(p.BackupNewestCommit)
	}
	if w := p.Witness; w != nil {
		c.Witness = &CoverageWitness{GTID: w.GTID.String(), Site: w.Row.Site, File: w.Row.Name, ID: w.Row.ID,
			CommitTime: microTime(w.CommitTime), GroundTruth: w.GroundTruth}
		if w.GroundTruth == GroundTruthAnchored {
			c.Witness.Anchor = &CoverageAnchor{Records: w.AnchorRecords, OriginRow: w.AnchorOriginRow}
		}
	}
	if h := p.Head; h != nil {
		c.Head = &CoverageHead{Site: h.Site, File: h.Name, ID: h.ID}
	}
	if refusal != "" {
		return c
	}
	c.NewestReplayedCommitTime = microTime(p.newestReplayed)
	c.ReplayedTransactionCount = p.replayedCount
	for _, f := range p.Files {
		c.Files = append(c.Files, CoverageFile{Path: f.StagedPath(), Site: f.Row.Site, Name: f.Row.Name, ID: f.Row.ID,
			SHA256: f.Digest, PreviousGTIDs: f.Previous.String(), GTIDs: f.GTIDs.String()})
	}
	for _, s := range p.Segments {
		c.Segments = append(c.Segments, CoverageSegment{File: s.File, StartPosition: s.StartPosition, GTIDs: s.GTIDs.String(), StartPredecessors: s.StartPredecessors.String(),
			Transactions: s.Transactions, FirstCommitTime: microTime(s.FirstCommit), LastCommitTime: microTime(s.LastCommit)})
	}
	return c
}

func microTime(us int64) backupmeta.MicroTime {
	if us == 0 {
		return backupmeta.MicroTime{}
	}
	return backupmeta.NewMicroTime(time.UnixMicro(us))
}
