package sidecar

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"unsafe"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/shipstream/bloodraven/internal/pitrplan"
)

// The effect digest (issue #268) lets the planner compare the row changes of
// two archived copies of one GTID. A copy injected under another server's
// GTID (a manual repair with SET GTID_NEXT), or applied by a server whose
// data had drifted, can agree with the original on commit time, origin
// server and emptiness and still change different rows.
//
// What is digested. A replica re-logs the row changes it applies
// (log_replica_updates) in its own events, so the digest covers only what an
// origin copy and a faithful replica copy share byte for byte. Per
// transaction, in file order, for every WRITE_ROWS, UPDATE_ROWS and
// DELETE_ROWS event (version 2):
//
//   - a group header: the event class, the database and table names (from
//     the statement's TABLE_MAP event for the event's table id), the column
//     count and the columns-present bitmap or bitmaps, with the unused high
//     bits of their last byte cleared;
//   - the row images: every byte after the bitmaps.
//
// A group is a run of consecutive rows events with the same header. A server
// cuts one statement's rows into events at binlog_row_event_max_size and at
// partition changes, each on a row boundary, so a group's concatenated row
// images do not depend on where the events were cut. The transaction's
// digest is SHA-256(domain ‖ SHA-256(header digest ‖ row images) of every
// group), cut to 128 bits. The stream is unambiguous: a header's fields
// have fixed sizes (the bitmaps' follows from the column count, the names
// are digested with their lengths), and each group enters the
// transaction's hash as one fixed-size digest.
//
// What is left out, because it legitimately differs between copies: table
// ids, event boundaries, transaction compression (the events inside a
// TRANSACTION_PAYLOAD are digested like outer ones), every event header
// (timestamp, server_id, log_pos, flags, checksum), the rows events' flags
// and extra-data block, the TABLE_MAP column metadata and optional metadata,
// the GTID event, BEGIN, COMMIT and XID, and ROWS_QUERY.
//
// What is not comparable (a zero digest, which the planner never compares):
//
//   - a transaction without rows events (DDL and other statements): a
//     replica or a newer server may re-log different statement text;
//   - a transaction that holds any statement besides BEGIN and COMMIT
//     (SAVEPOINT, ROLLBACK, XA, a statement-format change beside row
//     changes, LOAD DATA, user-variable events): a replica writing ROW logs
//     a statement it received as rows;
//   - PARTIAL_UPDATE_ROWS events (binlog_row_value_options=PARTIAL_JSON):
//     the origin logs a JSON diff, a replica without the option the whole
//     document;
//   - a table with a JSON column: row images carry the stored binary
//     document, and one document has more than one binary form (an
//     in-place partial update leaves slack that a logically reloaded copy
//     does not have; measured on MySQL 9.7.4, testdata/native/README.md);
//   - rows events of versions 0 and 1 (no MySQL 8 or 9 server writes them),
//     a file whose FORMAT_DESCRIPTION declares other post-header lengths
//     than the ones framed here, and anything that does not frame exactly:
//     a rows event without its TABLE_MAP, a column count that disagrees
//     with it, a truncated post-header or bitmap, more than
//     effectMaxTables table maps in one statement.
//
// The digester never refuses a file: what it cannot frame is not comparable,
// so it cannot turn a restore that plans today into a refusal on its own.
// Only two comparable copies that disagree refuse, in the planner
// (PITRArchiveNamespaceReuse). A missed difference is preferred to a false
// one, which would refuse a healthy archive. The native captures under
// testdata/native/effect hold origin and replica copies of the same
// transactions and pin both directions (TestEffectDigestNativeFixtures).
//
// The table maps and rows events are framed here, with every length checked
// against the event, and not by go-mysql's decoders (review F078 found they
// index their input without bounds checks). No row image is decoded.

const (
	// effectDomain versions the canonical form.
	effectDomain = "bloodraven pitr effect digest v1\x00"
	// effectMaxTables bounds the table maps of one statement.
	effectMaxTables = 64
	// effectMaxColumns is MySQL's column limit per table.
	effectMaxColumns = 4096

	tableMapPostHeader = 8  // table id 6, flags 2
	rowsV2PostHeader   = 10 // table id 6, flags 2, extra-data length 2
	rowsStmtEndFlag    = 0x0001
	mysqlTypeJSON      = 245
)

// effectTable is one TABLE_MAP of the open statement.
type effectTable struct {
	id uint64
	// name is SHA-256 over the length-prefixed database and table names.
	name    [sha256.Size]byte
	columns uint64
	// stable is false for a table whose row images are not byte-stable
	// across copies (a JSON column).
	stable bool
}

// effectStateBytes is charged once per index pass for the digester: its
// table list and three SHA-256 states.
const effectStateBytes = int64(unsafe.Sizeof(effectDigester{})) + 3*256

// effectDigester computes one transaction's pitrplan.EffectDigest
// incrementally: nothing of a transaction is kept but hash states and the
// open statement's table maps.
type effectDigester struct {
	// enabled: the file's FORMAT_DESCRIPTION declares the post-header
	// lengths this parser frames.
	enabled bool
	tx      hash.Hash // the transaction: domain, then one digest per group
	group   hash.Hash // the open group: header digest, then row images
	scratch hash.Hash // a group header
	// header is the open group's header digest; sum and next are scratch
	// space for digests, so that hashing allocates nothing per event.
	header [sha256.Size]byte
	sum    [sha256.Size]byte
	next   [sha256.Size]byte
	// fixed holds a header's fixed fields, and single bytes, on their way
	// into a hash.
	fixed  [1 + 8]byte
	open   bool // a group is open
	rows   bool // the transaction has a digested rows event
	off    bool // the transaction is not comparable
	tables [effectMaxTables]effectTable
	ntable int
}

// init reads the post-header lengths of the events the digester frames from
// the FORMAT_DESCRIPTION event (lengths[i] is event type i+1's).
func (d *effectDigester) init(lengths []byte) {
	at := func(kind replication.EventType) int {
		if i := int(kind) - 1; i < len(lengths) {
			return int(lengths[i])
		}
		return -1
	}
	d.enabled = at(replication.TABLE_MAP_EVENT) == tableMapPostHeader &&
		at(replication.WRITE_ROWS_EVENTv2) == rowsV2PostHeader &&
		at(replication.UPDATE_ROWS_EVENTv2) == rowsV2PostHeader &&
		at(replication.DELETE_ROWS_EVENTv2) == rowsV2PostHeader
	d.tx, d.group, d.scratch = sha256.New(), sha256.New(), sha256.New()
}

// begin starts a transaction.
func (d *effectDigester) begin() {
	d.open, d.rows, d.off, d.ntable = false, false, !d.enabled, 0
	if d.enabled {
		d.tx.Reset()
		d.tx.Write([]byte(effectDomain))
	}
}

// finish returns the transaction's digest: zero when it is not comparable.
func (d *effectDigester) finish() pitrplan.EffectDigest {
	var out pitrplan.EffectDigest
	if d.off || !d.rows {
		return out
	}
	d.closeGroup()
	copy(out[:], d.tx.Sum(d.sum[:0]))
	return out
}

func (d *effectDigester) closeGroup() {
	if d.open {
		d.tx.Write(d.group.Sum(d.sum[:0]))
		d.open = false
	}
}

// event digests one in-transaction event (outer, or inside a payload); body
// is its post-header and payload without the checksum, query the upper-case
// statement of a QUERY_EVENT.
func (d *effectDigester) event(kind replication.EventType, body []byte, query string) {
	if d.off {
		return
	}
	switch kind {
	case replication.QUERY_EVENT:
		if query != "BEGIN" && query != "COMMIT" {
			d.off = true
		}
	case replication.XID_EVENT, replication.ROWS_QUERY_EVENT:
	case replication.TABLE_MAP_EVENT:
		d.off = !d.tableMap(body)
	case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
		d.off = !d.rowsEvent(kind, body)
	default:
		// Rows events of other versions, PARTIAL_UPDATE_ROWS, statement
		// companions (INTVAR, RAND, USER_VAR), XA_PREPARE, LOAD DATA and
		// unknown events.
		d.off = true
	}
}

// tableMap records a TABLE_MAP event: table id, flags, the database name
// (one length byte, the name, NUL), the table name (the same), the column
// count (a length-encoded integer) and one type byte per column. The column
// metadata, null bitmap and optional metadata after them are not read.
func (d *effectDigester) tableMap(body []byte) bool {
	if len(body) < tableMapPostHeader {
		return false
	}
	t := effectTable{id: tableID(body), stable: true}
	pos := tableMapPostHeader
	d.scratch.Reset()
	for range 2 {
		if pos >= len(body) {
			return false
		}
		n := int(body[pos])
		pos++
		// The name and its NUL terminator.
		if len(body)-pos < n+1 || body[pos+n] != 0 {
			return false
		}
		d.fixed[0] = byte(n)
		d.scratch.Write(d.fixed[:1])
		d.scratch.Write(body[pos : pos+n])
		pos += n + 1
	}
	d.scratch.Sum(d.next[:0])
	t.name = d.next
	columns, n, err := payloadLengthEncodedInt(body[pos:])
	if err != nil || columns == 0 || columns > effectMaxColumns {
		return false
	}
	pos += n
	if uint64(len(body)-pos) < columns {
		return false
	}
	t.columns = columns
	for _, typ := range body[pos : pos+int(columns)] {
		if typ == mysqlTypeJSON {
			t.stable = false
		}
	}
	// A statement maps each table once; a repeated id replaces its entry.
	for i := range d.tables[:d.ntable] {
		if d.tables[i].id == t.id {
			d.tables[i] = t
			return true
		}
	}
	if d.ntable == effectMaxTables {
		return false
	}
	d.tables[d.ntable] = t
	d.ntable++
	return true
}

// rowsEvent digests a version-2 rows event: table id, flags, the extra-data
// length (which counts its own two bytes) and the extra data, the column
// count, the columns-present bitmap (two for an update: before and after
// image), then the row images to the end of the event.
func (d *effectDigester) rowsEvent(kind replication.EventType, body []byte) bool {
	if len(body) < rowsV2PostHeader {
		return false
	}
	id := tableID(body)
	flags := binary.LittleEndian.Uint16(body[6:8])
	extra := int(binary.LittleEndian.Uint16(body[8:10]))
	if extra < 2 || len(body)-8 < extra {
		return false
	}
	pos := 8 + extra
	columns, n, err := payloadLengthEncodedInt(body[pos:])
	if err != nil || columns == 0 || columns > effectMaxColumns {
		return false
	}
	pos += n
	bitmaps := 1
	class := byte('W')
	switch kind {
	case replication.UPDATE_ROWS_EVENTv2:
		bitmaps, class = 2, 'U'
	case replication.DELETE_ROWS_EVENTv2:
		class = 'D'
	}
	width := int(columns+7) / 8
	if len(body)-pos < bitmaps*width {
		return false
	}
	var table *effectTable
	for i := range d.tables[:d.ntable] {
		if d.tables[i].id == id {
			table = &d.tables[i]
		}
	}
	if table == nil || !table.stable || table.columns != columns {
		return false
	}
	d.fixed[0] = class
	binary.LittleEndian.PutUint64(d.fixed[1:], columns)
	d.scratch.Reset()
	d.scratch.Write(d.fixed[:])
	d.scratch.Write(table.name[:])
	// The bits past the last column are not part of the bitmap.
	mask := byte(0xff)
	if r := columns % 8; r != 0 {
		mask = byte(1)<<r - 1
	}
	for range bitmaps {
		d.scratch.Write(body[pos : pos+width-1])
		d.fixed[0] = body[pos+width-1] & mask
		d.scratch.Write(d.fixed[:1])
		pos += width
	}
	d.scratch.Sum(d.next[:0])
	if !d.open || d.next != d.header {
		d.closeGroup()
		d.header, d.open = d.next, true
		d.group.Reset()
		d.group.Write(d.header[:])
	}
	d.group.Write(body[pos:])
	d.rows = true
	if flags&rowsStmtEndFlag != 0 {
		// The statement's table maps end with it.
		d.ntable = 0
	}
	return true
}

// tableID reads the 6-byte table id that starts a TABLE_MAP or rows event.
func tableID(body []byte) uint64 {
	var id [8]byte
	copy(id[:], body[:6])
	return binary.LittleEndian.Uint64(id[:])
}
