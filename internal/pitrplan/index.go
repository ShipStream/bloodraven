package pitrplan

import (
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// TxnFlags classify a transaction (§4.1).
type TxnFlags uint8

const (
	// TxnEmpty: no data or statement event, by the sidecar's
	// binlogDataEvent rule (the emptyGtids hint uses the same rule).
	TxnEmpty TxnFlags = 1 << iota
	// TxnXA: an XA transaction (XA_PREPARE, or an XA statement). Replaying
	// one is unsupported (L5).
	TxnXA
	// TxnUnsupported: an event the planner cannot certify mysqlbinlog
	// replays faithfully (LOAD DATA blocks, incidents, unknown types).
	// Treated as L5.
	TxnUnsupported
)

// Txn is one indexed transaction of a file. Its GTID is
// (FileIndex.SIDs[SID], GNO).
type Txn struct {
	GNO int64
	// OCT is the original commit timestamp, µs since the Unix epoch. The
	// index pass refuses 0 (PITRReplayUnsupported).
	OCT int64
	// Srv is the originating server_id (the GTID event header's).
	Srv   uint32
	SID   uint32
	Flags TxnFlags
}

// Empty reports the TxnEmpty flag.
func (t Txn) Empty() bool { return t.Flags&TxnEmpty != 0 }

// FileIndex is the per-transaction index of one downloaded, digest-
// authenticated binlog file (§4.10), built by
// internal/sidecar.InspectTransactions.
type FileIndex struct {
	// Digest is the plaintext SHA-256 (hex).
	Digest string
	Size   int64
	// WriterServerID is fsrv(f), the FORMAT_DESCRIPTION event's server_id.
	WriterServerID uint32
	// Previous is the file's own PREVIOUS_GTIDS. The planner adds the
	// manifest row's baseGtids to form prev(f).
	Previous Set
	SIDs     []string
	Txns     []Txn
	// GTIDs, EmptyGTIDs, CommitMin and CommitMax are the content values
	// the manifest hints must equal.
	GTIDs      Set
	EmptyGTIDs Set
	CommitMin  int64
	CommitMax  int64
}

// GTID returns transaction i's GTID.
func (x *FileIndex) GTID(i int) GTID {
	return GTID{SID: x.SIDs[x.Txns[i].SID], GNO: x.Txns[i].GNO}
}

// IndexBuilder accumulates a FileIndex event by event; the sidecar's index
// pass drives it. It charges every transaction to the budget before
// storing it and enforces the per-file identity rules (§4.9): no GTID
// repeats, and gtids ∩ prev = ∅.
type IndexBuilder struct {
	label  string
	budget *Budget
	idx    FileIndex
	sids   map[string]uint32
	open   bool
	prev   bool
}

// NewIndexBuilder starts a file; label ("<site>/<name>") names it in
// refusals.
func NewIndexBuilder(label string, budget *Budget) *IndexBuilder {
	return &IndexBuilder{label: label, budget: budget, sids: map[string]uint32{},
		idx: FileIndex{GTIDs: Set{}, EmptyGTIDs: Set{}, Previous: Set{}}}
}

// SetWriter records the FORMAT_DESCRIPTION server_id.
func (b *IndexBuilder) SetWriter(serverID uint32) { b.idx.WriterServerID = serverID }

// SetPrevious records PREVIOUS_GTIDS; a second one is an invalid file.
func (b *IndexBuilder) SetPrevious(s Set) error {
	if b.prev {
		return refuse(pitrarchive.TokenArchiveObjectInvalid, "%s holds two PREVIOUS_GTIDS events", b.label)
	}
	b.prev = true
	if err := b.budget.chargeSet(s, "previous GTIDs of "+b.label); err != nil {
		return err
	}
	b.idx.Previous = s
	return nil
}

// HasPrevious reports whether PREVIOUS_GTIDS was seen.
func (b *IndexBuilder) HasPrevious() bool { return b.prev }

// BeginTxn opens the transaction g. oct is µs; srv the event's server_id.
func (b *IndexBuilder) BeginTxn(g GTID, oct int64, srv uint32) error {
	b.closeTxn()
	if g.GNO <= 0 {
		return refuse(pitrarchive.TokenReplayUnsupported, "%s holds a GTID with GNO %d", b.label, g.GNO)
	}
	if oct <= 0 {
		return refuse(pitrarchive.TokenReplayUnsupported, "%s: transaction %s has no original commit timestamp; the planner orders history by it", b.label, g)
	}
	if b.idx.Previous.Has(g) {
		return refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s: %s is both in the file's Previous_gtids and among its transactions; the GTID was reissued", b.label, g)
	}
	if err := b.budget.Charge(TxnBytes, "transaction index of "+b.label); err != nil {
		return err
	}
	if !b.idx.GTIDs.add(g) {
		return refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s: %s occurs twice in one file; the GTID was reissued", b.label, g)
	}
	sid, ok := b.sids[g.SID]
	if !ok {
		sid = uint32(len(b.idx.SIDs))
		b.sids[g.SID] = sid
		b.idx.SIDs = append(b.idx.SIDs, g.SID)
	}
	b.idx.Txns = append(b.idx.Txns, Txn{GNO: g.GNO, OCT: oct, Srv: srv, SID: sid, Flags: TxnEmpty})
	b.budget.indexed++
	if b.idx.CommitMin == 0 || oct < b.idx.CommitMin {
		b.idx.CommitMin = oct
	}
	b.idx.CommitMax = max(b.idx.CommitMax, oct)
	b.open = true
	return nil
}

// InTxn reports whether a transaction is open.
func (b *IndexBuilder) InTxn() bool { return b.open }

// Mark sets flags on the open transaction; MarkData clears TxnEmpty.
func (b *IndexBuilder) Mark(f TxnFlags) {
	if b.open {
		b.idx.Txns[len(b.idx.Txns)-1].Flags |= f
	}
}

// MarkData records a data or statement event in the open transaction.
func (b *IndexBuilder) MarkData() {
	if b.open {
		b.idx.Txns[len(b.idx.Txns)-1].Flags &^= TxnEmpty
	}
}

func (b *IndexBuilder) closeTxn() {
	if !b.open {
		return
	}
	b.open = false
	t := b.idx.Txns[len(b.idx.Txns)-1]
	if t.Empty() {
		b.idx.EmptyGTIDs.add(GTID{b.idx.SIDs[t.SID], t.GNO})
	}
}

// Finish closes the file.
func (b *IndexBuilder) Finish(digest string, size int64) (*FileIndex, error) {
	b.closeTxn()
	if !b.prev {
		return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "%s has no PREVIOUS_GTIDS event", b.label)
	}
	b.idx.Digest, b.idx.Size = digest, size
	out := b.idx
	return &out, nil
}
