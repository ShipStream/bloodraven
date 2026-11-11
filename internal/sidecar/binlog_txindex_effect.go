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
// What a faithful replica shares with the origin. A replica re-logs the row
// changes it applies (log_replica_updates) in its own events. Measured on
// MySQL 9.7.4 and 8.4 (testdata/native/README.md), its copy has:
//
//   - the same tables, changed the same way, in the same order;
//   - the same row images, but not in the same order: a table without a
//     primary key (or with only a nullable unique key) is applied by a
//     scan, and the replica logs the rows of an UPDATE or DELETE in the
//     order its scan finds them;
//   - another encoding of the row images when the two sites' table
//     definitions or row images differ: a session's binlog_row_image, a
//     column added or widened on one site first (a rolling schema change),
//     a generated invisible primary key on the replica only. The data is
//     the same and the bytes are not.
//
// So the digest has three parts (pitrplan.EffectDigest), each a truncated
// SHA-256 over the transaction's WRITE_ROWS, UPDATE_ROWS and DELETE_ROWS
// events (version 2) in file order. A group is a run of consecutive rows
// events of one table and one class (write, update, delete); a server cuts
// a statement's rows into events at binlog_row_event_max_size and at
// partition changes, so where a group is cut into events means nothing.
//
//   - tables: per group, the class and the database and table names (from
//     the statement's TABLE_MAP for the event's table id; the names are
//     digested with their lengths).
//   - layout: the same, plus, per group, each distinct consecutive encoding
//     of its events: the column count, the columns-present bitmap (two for
//     an update) with the unused high bits of the last byte cleared, and
//     the TABLE_MAP's column type array and column metadata block (the
//     part of a TABLE_MAP that says how a row image's bytes are laid out).
//   - rows: per group, the class and names, the total length of the row
//     images (every byte after the bitmaps) and how often each byte value
//     occurs in them. It is a multiset of bytes: the same for the same rows
//     in any order and cut into events anywhere.
//
// Every record that enters a hash has a fixed size or carries its length,
// so no bytes can move between a name, a layout and row data.
//
// What the planner concludes (pitrplan.EffectDigest.Compare): other tables
// refuse; the same tables with another layout say nothing; the same tables
// and layout with other row bytes refuse.
//
// What the rows part cannot see: a change that only permutes the bytes of a
// group's row images (two rows' values exchanged, the digits of a value
// reordered) has the same multiset. Telling those apart needs the row
// boundaries, which means decoding every column type's length; review F078
// found go-mysql's row decoder indexes its input without bounds checks, and
// a decoder of our own for every type is a larger risk than the gap.
//
// What is left out, because it legitimately differs between copies: table
// ids, event boundaries, transaction compression (the events inside a
// TRANSACTION_PAYLOAD are digested like outer ones), every event header
// (timestamp, server_id, log_pos, flags, checksum), the rows events' flags
// and extra-data block, the TABLE_MAP flags, null bitmap and optional
// metadata (binlog_row_metadata), the GTID event, BEGIN, COMMIT and XID,
// and ROWS_QUERY.
//
// What is not comparable at all (a zero digest):
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
//     with it, a truncated post-header, bitmap or table map, more than
//     effectMaxTables table maps in one statement.
//
// The digester never refuses a file: what it cannot frame is not comparable,
// so it cannot turn a restore that plans today into a refusal on its own.
// Only two comparable copies that disagree refuse, in the planner
// (PITRArchiveNamespaceReuse). A missed difference is preferred to a false
// one, which would refuse a healthy archive during an incident. The native
// captures under testdata/native/effect hold origin and replica copies of
// the same transactions and pin every direction
// (TestEffectDigestNativeFixtures).
//
// The table maps and rows events are framed here, with every length checked
// against the event, and not by go-mysql's decoders. No row image is
// decoded.

const (
	// The domains version the canonical form of each part.
	effectTablesDomain = "bloodraven pitr effect digest v2 tables\x00"
	effectLayoutDomain = "bloodraven pitr effect digest v2 layout\x00"
	effectRowsDomain   = "bloodraven pitr effect digest v2 rows\x00"
	// effectMaxTables bounds the table maps of one statement.
	effectMaxTables = 64
	// effectMaxColumns is MySQL's column limit per table.
	effectMaxColumns = 4096

	tableMapPostHeader = 8  // table id 6, flags 2
	rowsV2PostHeader   = 10 // table id 6, flags 2, extra-data length 2
	rowsStmtEndFlag    = 0x0001
	mysqlTypeJSON      = 245

	// effectGroupKey is a group's class byte and table name digest.
	effectGroupKey = 1 + sha256.Size
	// effectLanesMin is the row-image length from which count uses its
	// lanes: below it, adding them up costs more than they save.
	effectLanesMin = 2048

	// effectRowsRecord is the largest record of a group's row bytes: the
	// group key, the total length, the number of byte values that occur,
	// and each one with its count.
	effectRowsRecord = effectGroupKey + 8 + 2 + 256*(1+8)
)

// effectTable is one TABLE_MAP of the open statement.
type effectTable struct {
	id uint64
	// name is SHA-256 over the length-prefixed database and table names.
	name [sha256.Size]byte
	// layout is SHA-256 over the column count, the column type array and
	// the column metadata block with its length.
	layout  [sha256.Size]byte
	columns uint64
	// stable is false for a table whose row images are not byte-stable
	// across copies (a JSON column).
	stable bool
}

// effectStateBytes is charged once per index pass for the digester: its
// table list, byte counts and four SHA-256 states.
const effectStateBytes = int64(unsafe.Sizeof(effectDigester{})) + 4*256

// effectDigester computes one transaction's pitrplan.EffectDigest
// incrementally: nothing of a transaction is kept but hash states, the open
// group's byte counts and the open statement's table maps.
type effectDigester struct {
	// enabled: the file's FORMAT_DESCRIPTION declares the post-header
	// lengths this parser frames.
	enabled bool
	// One hash per part of the digest, over the transaction.
	tableSum, layoutSum, rowSum hash.Hash
	scratch                     hash.Hash // a table map's names and layout, an event's layout
	// key is the open group's class and table name digest; layout the
	// encoding of its latest event.
	key    [effectGroupKey]byte
	layout [sha256.Size]byte
	// length and counts are the open group's row bytes: how many, and how
	// often each value occurs.
	length uint64
	counts [256]uint64
	// lanes are count's scratch tables; they are zero between events.
	lanes [4][256]uint32
	// sum and next are scratch space for digests, fixed for a record's
	// fixed fields and record for a group's row-bytes record, so that
	// hashing allocates nothing per event.
	sum    [sha256.Size]byte
	next   [sha256.Size]byte
	fixed  [1 + 8]byte
	record [effectRowsRecord]byte
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
	d.tableSum, d.layoutSum, d.rowSum, d.scratch = sha256.New(), sha256.New(), sha256.New(), sha256.New()
}

// begin starts a transaction.
func (d *effectDigester) begin() {
	if d.open {
		// A transaction that was set aside left its group open.
		d.length, d.counts = 0, [256]uint64{}
	}
	d.open, d.rows, d.off, d.ntable = false, false, !d.enabled, 0
	if d.enabled {
		d.tableSum.Reset()
		d.tableSum.Write([]byte(effectTablesDomain))
		d.layoutSum.Reset()
		d.layoutSum.Write([]byte(effectLayoutDomain))
		d.rowSum.Reset()
		d.rowSum.Write([]byte(effectRowsDomain))
	}
}

// finish returns the transaction's digest: zero when it is not comparable.
func (d *effectDigester) finish() pitrplan.EffectDigest {
	if d.off || !d.rows {
		return pitrplan.EffectDigest{}
	}
	d.closeGroup()
	tables := binary.BigEndian.Uint32(d.tableSum.Sum(d.sum[:0]))
	layout := binary.BigEndian.Uint64(d.layoutSum.Sum(d.sum[:0]))
	rows := binary.BigEndian.Uint32(d.rowSum.Sum(d.sum[:0]))
	if tables == 0 && layout == 0 && rows == 0 {
		// Zero means "not comparable".
		rows = 1
	}
	return pitrplan.NewEffectDigest(tables, layout, rows)
}

// closeGroup adds the open group's row bytes to the rows part: its key, the
// total length, and the byte values that occur with their counts, in
// ascending order.
func (d *effectDigester) closeGroup() {
	if !d.open {
		return
	}
	rec := append(d.record[:0], d.key[:]...)
	rec = binary.LittleEndian.AppendUint64(rec, d.length)
	at := len(rec)
	rec = append(rec, 0, 0)
	values := uint16(0)
	for v := range d.counts {
		if n := d.counts[v]; n != 0 {
			rec = append(rec, byte(v))
			rec = binary.LittleEndian.AppendUint64(rec, n)
			values++
			d.counts[v] = 0
		}
	}
	binary.LittleEndian.PutUint16(rec[at:], values)
	d.rowSum.Write(rec)
	d.length, d.open = 0, false
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
// count (a length-encoded integer), one type byte per column, the length of
// the column metadata (a length-encoded integer) and the metadata, and the
// null bitmap. The optional metadata after it is not read.
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
	types := body[pos : pos+int(columns)]
	for _, typ := range types {
		if typ == mysqlTypeJSON {
			t.stable = false
		}
	}
	pos += int(columns)
	metadata, n, err := payloadLengthEncodedInt(body[pos:])
	if err != nil {
		return false
	}
	pos += n
	// The metadata, then one null bit per column.
	if rest := uint64(len(body) - pos); rest < metadata || rest-metadata < (columns+7)/8 {
		return false
	}
	d.scratch.Reset()
	binary.LittleEndian.PutUint64(d.fixed[:8], columns)
	d.scratch.Write(d.fixed[:8])
	d.scratch.Write(types)
	binary.LittleEndian.PutUint64(d.fixed[:8], metadata)
	d.scratch.Write(d.fixed[:8])
	d.scratch.Write(body[pos : pos+int(metadata)])
	d.scratch.Sum(d.next[:0])
	t.layout = d.next
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
	// The event's encoding: the table's, and which columns are logged.
	d.scratch.Reset()
	d.scratch.Write(table.layout[:])
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
	if !d.open || d.key[0] != class || [sha256.Size]byte(d.key[1:]) != table.name {
		d.closeGroup()
		d.key[0] = class
		copy(d.key[1:], table.name[:])
		d.open = true
		d.tableSum.Write(d.key[:])
		d.fixed[0] = 'G'
		d.layoutSum.Write(d.fixed[:1])
		d.layoutSum.Write(d.key[:])
		d.layout = [sha256.Size]byte{}
		d.startLayout()
	} else if d.next != d.layout {
		d.startLayout()
	}
	d.count(body[pos:])
	d.rows = true
	if flags&rowsStmtEndFlag != 0 {
		// The statement's table maps end with it.
		d.ntable = 0
	}
	return true
}

// count adds an event's row images to the open group's byte counts.
func (d *effectDigester) count(rows []byte) {
	d.length += uint64(len(rows))
	if len(rows) < effectLanesMin {
		for _, b := range rows {
			d.counts[b]++
		}
		return
	}
	// Four tables, one per position modulo 4: a run of one byte value
	// (zeros are common in row images) would otherwise make every
	// increment wait for the one before it. An event is shorter than
	// 4 GiB, so a lane's 32-bit counts cannot overflow before they are
	// added up.
	lanes := &d.lanes
	for len(rows) >= 4 {
		lanes[0][rows[0]]++
		lanes[1][rows[1]]++
		lanes[2][rows[2]]++
		lanes[3][rows[3]]++
		rows = rows[4:]
	}
	for _, b := range rows {
		d.counts[b]++
	}
	for v := range d.counts {
		d.counts[v] += uint64(lanes[0][v]) + uint64(lanes[1][v]) + uint64(lanes[2][v]) + uint64(lanes[3][v])
	}
	*lanes = [4][256]uint32{}
}

// startLayout records the encoding in d.next as the open group's next one.
func (d *effectDigester) startLayout() {
	d.layout = d.next
	d.fixed[0] = 'L'
	d.layoutSum.Write(d.fixed[:1])
	d.layoutSum.Write(d.layout[:])
}

// tableID reads the 6-byte table id that starts a TABLE_MAP or rows event.
func tableID(body []byte) uint64 {
	var id [8]byte
	copy(id[:], body[:6])
	return binary.LittleEndian.Uint64(id[:])
}
