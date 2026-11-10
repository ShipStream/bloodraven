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

// EffectDigest identifies a transaction's row changes independently of the
// server that logged them (issue #268): 128 bits of a SHA-256 over the
// transaction's rows events in a canonical form that an origin copy and a
// faithful replica copy share (internal/sidecar's effect digester defines
// the form and what it leaves out). The zero value means "not comparable":
// the transaction has no rows events, or holds something whose re-logging
// by a replica is not byte-stable. Two copies of a GTID are compared only
// when both digests are non-zero.
type EffectDigest [16]byte

// IsZero reports whether the transaction's row changes are not comparable.
func (d EffectDigest) IsZero() bool { return d == EffectDigest{} }

// Txn is one indexed transaction of a file. Its GTID is
// (FileIndex.SIDs[SID], GNO). It is a fixed-size record without pointers,
// so an index is spilled to disk as is (issue #266).
type Txn struct {
	GNO int64
	// OCT is the original commit timestamp, µs since the Unix epoch. The
	// index pass refuses 0 (PITRReplayUnsupported).
	OCT int64
	// Pos is the file offset of the transaction's GTID event: where
	// `mysqlbinlog --start-position` starts a segment whose first replayed
	// transaction this is.
	Pos int64
	// Effect is the digest of the transaction's row changes, or zero.
	Effect EffectDigest
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
	// Txns are the file's transactions in file order. The array is
	// read-only once the index is built: with a spilling budget it may be
	// file-backed memory of the budget's arena (issue #266), valid until
	// the arena is closed.
	Txns []Txn
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
//
// The transactions accumulate in an array that doubles (charged, or
// spilled, at its capacity) and is cut to its length by Finish, so a
// finished index holds exactly TxnBytes per transaction.
type IndexBuilder struct {
	label  string
	budget *Budget
	idx    FileIndex
	txns   vec[Txn]
	sids   map[string]uint32
	open   bool
	prev   bool
}

// NewIndexBuilder starts a file; label ("<site>/<name>") names it in
// refusals.
func NewIndexBuilder(label string, budget *Budget) *IndexBuilder {
	return &IndexBuilder{label: label, budget: budget, sids: map[string]uint32{}, txns: newVec[Txn](budget),
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

// BeginTxn opens the transaction g. oct is µs; srv the event's server_id;
// pos the file offset of its GTID event, which lies after the 4-byte magic
// and after every earlier transaction's.
func (b *IndexBuilder) BeginTxn(g GTID, oct int64, srv uint32, pos int64) error {
	if err := b.closeTxn(); err != nil {
		return err
	}
	if last := b.txns.n - 1; pos < 4 || (last >= 0 && pos <= b.txns.items()[last].Pos) {
		return refuse(pitrarchive.TokenArchiveObjectInvalid, "%s: transaction %s at offset %d does not follow the previous transaction", b.label, g, pos)
	}
	if g.GNO <= 0 || g.GNO > MaxGNO {
		return refuse(pitrarchive.TokenReplayUnsupported, "%s holds a GTID with GNO %d", b.label, g.GNO)
	}
	if oct <= 0 {
		return refuse(pitrarchive.TokenReplayUnsupported, "%s: transaction %s has no original commit timestamp; the planner orders history by it", b.label, g)
	}
	if b.idx.Previous.Has(g) {
		return refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s: %s is both in the file's Previous_gtids and among its transactions; the GTID was reissued", b.label, g)
	}
	// (Review F047) a new interval of the GTID set, and a new source
	// identifier's map slots and spelling. The transaction record itself
	// is charged, or spilled, by the array it is appended to.
	var charge int64
	sid, ok := b.sids[g.SID]
	if !ok {
		charge += indexSourceBytes + sidBytes(g.SID)
	}
	if !b.idx.GTIDs.Has(g) && !b.idx.GTIDs.adjacent(g) {
		charge += intervalBytes
	}
	if err := b.budget.Charge(charge, "transaction index of "+b.label); err != nil {
		return err
	}
	if !b.idx.GTIDs.add(g) {
		return refuse(pitrarchive.TokenArchiveNamespaceReuse, "%s: %s occurs twice in one file; the GTID was reissued", b.label, g)
	}
	if !ok {
		sid = uint32(len(b.idx.SIDs))
		b.sids[g.SID] = sid
		b.idx.SIDs = append(b.idx.SIDs, g.SID)
	}
	if err := b.txns.push(Txn{GNO: g.GNO, OCT: oct, Pos: pos, Srv: srv, SID: sid, Flags: TxnEmpty}, "transaction index of "+b.label); err != nil {
		return err
	}
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

// last is the open transaction; setLast stores it back.
func (b *IndexBuilder) last() Txn { return b.txns.items()[b.txns.n-1] }

func (b *IndexBuilder) setLast(t Txn) { b.txns.set(b.txns.n-1, t) }

// Mark sets flags on the open transaction; MarkData clears TxnEmpty.
func (b *IndexBuilder) Mark(f TxnFlags) {
	if b.open {
		if t := b.last(); t.Flags|f != t.Flags {
			t.Flags |= f
			b.setLast(t)
		}
	}
}

// MarkData records a data or statement event in the open transaction.
func (b *IndexBuilder) MarkData() {
	if b.open {
		if t := b.last(); t.Flags&TxnEmpty != 0 {
			t.Flags &^= TxnEmpty
			b.setLast(t)
		}
	}
}

// SetEffect records the open transaction's effect digest (zero: not
// comparable).
func (b *IndexBuilder) SetEffect(d EffectDigest) {
	if b.open {
		if t := b.last(); t.Effect != d {
			t.Effect = d
			b.setLast(t)
		}
	}
}

func (b *IndexBuilder) closeTxn() error {
	if !b.open {
		return nil
	}
	b.open = false
	t := b.last()
	if !t.Empty() {
		return nil
	}
	g := GTID{b.idx.SIDs[t.SID], t.GNO}
	var charge int64
	if _, ok := b.idx.EmptyGTIDs[g.SID]; !ok {
		charge += sourceBytes
	}
	if !b.idx.EmptyGTIDs.adjacent(g) {
		charge += intervalBytes
	}
	if err := b.budget.Charge(charge, "empty GTIDs of "+b.label); err != nil {
		return err
	}
	b.idx.EmptyGTIDs.add(g)
	return nil
}

// Finish closes the file.
func (b *IndexBuilder) Finish(digest string, size int64) (*FileIndex, error) {
	if err := b.closeTxn(); err != nil {
		return nil, err
	}
	if !b.prev {
		return nil, refuse(pitrarchive.TokenArchiveObjectInvalid, "%s has no PREVIOUS_GTIDS event", b.label)
	}
	// The array keeps exactly its transactions.
	if err := b.txns.trim("transaction index of " + b.label); err != nil {
		return nil, err
	}
	b.idx.Digest, b.idx.Size = digest, size
	out := b.idx
	out.Txns = b.txns.items()
	return &out, nil
}
