package pitrplan

import (
	"encoding/binary"
	"fmt"

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

// EffectDigest describes a transaction's row changes independently of the
// server that logged them (issue #268), so that two archived copies of one
// GTID can be compared. internal/sidecar's effect digester computes it and
// defines the canonical form; it has three parts:
//
//   - tables (32 bits): which tables the transaction changes, how (write,
//     update, delete) and in which order;
//   - layout (64 bits): the tables part plus how each table's row images
//     are encoded (the columns, their types and which of them are logged);
//   - rows (32 bits): the row bytes of each table's changes as a multiset,
//     so that it does not depend on the order of the rows.
//
// The zero value means "not comparable": the transaction has no rows
// events, or holds something a replica does not re-log in a stable form.
// Compare defines what two digests say about two copies.
//
// The split follows from what a hash collision costs. Colliding tables or
// rows parts make two copies that differ look equal: a missed detection,
// with probability 2⁻³² for each copy that really differs, and such copies
// are rare. Colliding layout parts make two copies with different layouts
// look comparable, and their row bytes then differ: a refusal of a healthy
// archive. Copies with different layouts are not rare (every transaction of
// a table during a rolling schema change), so the layout part has 64 bits.
type EffectDigest [16]byte

// NewEffectDigest assembles a digest from its parts. A digest whose parts
// are all zero would read as "not comparable"; the digester never passes
// one (it sets a bit).
func NewEffectDigest(tables uint32, layout uint64, rows uint32) EffectDigest {
	var d EffectDigest
	binary.BigEndian.PutUint32(d[0:4], tables)
	binary.BigEndian.PutUint64(d[4:12], layout)
	binary.BigEndian.PutUint32(d[12:16], rows)
	return d
}

// IsZero reports whether the transaction's row changes are not comparable.
func (d EffectDigest) IsZero() bool { return d == EffectDigest{} }

// String prints the parts: tables-layout-rows.
func (d EffectDigest) String() string {
	return fmt.Sprintf("%x-%x-%x", d[0:4], d[4:12], d[12:16])
}

// EffectRelation is what two copies' effect digests say about them.
type EffectRelation uint8

const (
	// EffectNotComparable: one copy has no digest, or both change the same
	// tables with different layouts (the sites' table definitions or row
	// images differ: binlog_row_image, a column added or widened on one
	// site, a generated invisible primary key on one site). Nothing is
	// concluded.
	EffectNotComparable EffectRelation = iota
	// EffectEqual: the same tables, layouts and row bytes.
	EffectEqual
	// EffectTablesDiffer: the copies change different tables, or the same
	// tables in other ways or another order.
	EffectTablesDiffer
	// EffectRowsDiffer: the same tables and layouts, other row bytes.
	EffectRowsDiffer
)

// Compare relates two copies of one GTID. The relation is symmetric but
// not transitive: a may be not comparable with b and with c while b and c
// differ, so a set of copies is consistent only when every pair is (the
// planner's occurrence registry keeps what that takes, occ.go).
func (d EffectDigest) Compare(o EffectDigest) EffectRelation {
	switch {
	case d.IsZero() || o.IsZero():
		return EffectNotComparable
	case [4]byte(d[0:4]) != [4]byte(o[0:4]):
		return EffectTablesDiffer
	case [8]byte(d[4:12]) != [8]byte(o[4:12]):
		return EffectNotComparable
	case [4]byte(d[12:16]) != [4]byte(o[12:16]):
		return EffectRowsDiffer
	}
	return EffectEqual
}

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
	// Effect describes the transaction's row changes, or is zero.
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
