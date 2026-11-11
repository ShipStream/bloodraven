package sidecar

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// The effect digest (issue #268). The crafted cases complete the fixture's
// first transaction (A:11), whose FORMAT_DESCRIPTION declares MySQL 9.7's
// post-header lengths for table maps and version-2 rows events.

const (
	fxTable = 108
	// MYSQL_TYPE_VARCHAR and MYSQL_TYPE_JSON.
	fxVarchar = 15
	fxJSON    = 245
)

var (
	fxBegin  = txEvent(replication.QUERY_EVENT, queryEventBody("BEGIN"))
	fxXID    = txEvent(replication.XID_EVENT, xidBody())
	fxRow1   = []byte("\x00\x05first")
	fxRow2   = []byte("\x00\x06second")
	fxBitmap = [][]byte{{0x01}}
)

func fxMap(id uint64, db, table string, types ...byte) []byte {
	return txEvent(replication.TABLE_MAP_EVENT, binlogtest.TableMapBody(id, db, table, types...))
}

// fxRows is a rows event of one column over table id; end sets STMT_END_F.
func fxRows(kind replication.EventType, id uint64, end bool, rows ...[]byte) []byte {
	bitmaps := fxBitmap
	if kind == replication.UPDATE_ROWS_EVENTv2 {
		bitmaps = [][]byte{{0x01}, {0x01}}
	}
	return txEvent(kind, binlogtest.RowsBody(id, end, 1, bitmaps, bytes.Join(rows, nil)))
}

// fxBase is the transaction every case is compared with: one statement
// writing two rows into orders.markers.
func fxBase() [][]byte {
	return [][]byte{fxBegin, fxMap(fxTable, "orders", "markers", fxVarchar),
		fxRows(replication.WRITE_ROWS_EVENTv2, fxTable, true, fxRow1, fxRow2), fxXID}
}

// effectOf indexes a file holding the fixture's first GTID and events, and
// returns that transaction's digest. The digester never refuses.
func effectOf(t *testing.T, events ...[]byte) pitrplan.EffectDigest {
	t.Helper()
	idx, err := InspectTransactions(bytes.NewReader(txFile(events...)), TxIndexOptions{Label: "f"})
	if err != nil {
		t.Fatalf("the index pass refused: %v", err)
	}
	return idx.Txns[0].Effect
}

// rawRows is a rows event body with every field explicit.
func rawRows(id uint64, flags uint16, extra []byte, columns []byte, bitmaps, rows []byte) []byte {
	body := make([]byte, 6, 32)
	binary.LittleEndian.PutUint32(body, uint32(id))
	body = binary.LittleEndian.AppendUint16(body, flags)
	body = binary.LittleEndian.AppendUint16(body, uint16(2+len(extra)))
	body = append(body, extra...)
	body = append(body, columns...)
	body = append(body, bitmaps...)
	return append(body, rows...)
}

// reheader rewrites an event's timestamp, server_id, log_pos and flags.
func reheader(ev []byte) []byte {
	out := append([]byte(nil), ev[:len(ev)-4]...)
	binary.LittleEndian.PutUint32(out[0:], 1)
	binary.LittleEndian.PutUint32(out[5:], 4242)
	binary.LittleEndian.PutUint32(out[13:], 987654)
	binary.LittleEndian.PutUint16(out[17:], 0x80)
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

// inner strips an outer test event to an event inside a payload (no CRC).
func inner(events ...[]byte) []byte {
	var out []byte
	for _, ev := range events {
		e := append([]byte(nil), ev[:len(ev)-4]...)
		binary.LittleEndian.PutUint32(e[9:], uint32(len(e)))
		out = append(out, e...)
	}
	return out
}

// fxMapMeta is fxMap with the column metadata block given.
func fxMapMeta(id uint64, db, table string, types, metadata []byte) []byte {
	return txEvent(replication.TABLE_MAP_EVENT, binlogtest.TableMapBodyWithMetadata(id, db, table, types, metadata))
}

// What a replica may change when it re-logs a transaction leaves the digest
// alone: where the events are cut, the order of a group's rows, and
// everything outside the names, the layout and the row images.
func TestEffectDigestIgnoresRelogging(t *testing.T) {
	base := effectOf(t, fxBase()...)
	if base.IsZero() {
		t.Fatal("a row transaction has no digest")
	}
	w := replication.WRITE_ROWS_EVENTv2
	varchar := fxMap(fxTable, "orders", "markers", fxVarchar)
	payload := inner(fxBase()...)
	for name, events := range map[string][][]byte{
		"another table id":         {fxBegin, fxMap(7, "orders", "markers", fxVarchar), fxRows(w, 7, true, fxRow1, fxRow2), fxXID},
		"rows cut into two events": {fxBegin, varchar, fxRows(w, fxTable, false, fxRow1), fxRows(w, fxTable, true, fxRow2), fxXID},
		"two statements over the same table": {fxBegin, varchar, fxRows(w, fxTable, true, fxRow1),
			fxMap(9, "orders", "markers", fxVarchar), fxRows(w, 9, true, fxRow2), fxXID},
		// A table without a primary key: the replica logs the rows in the
		// order its scan finds them (review R3 finding 1).
		"rows in another order":                    {fxBegin, varchar, fxRows(w, fxTable, true, fxRow2, fxRow1), fxXID},
		"rows in another order, cut into events":   {fxBegin, varchar, fxRows(w, fxTable, false, fxRow2), fxRows(w, fxTable, true, fxRow1), fxXID},
		"rows in another order, in two statements": {fxBegin, varchar, fxRows(w, fxTable, true, fxRow2), fxMap(9, "orders", "markers", fxVarchar), fxRows(w, 9, true, fxRow1), fxXID},
		"other rows-event flags": {fxBegin, varchar,
			txEvent(w, rawRows(fxTable, 0x0007, nil, []byte{1}, []byte{0x01}, append(append([]byte(nil), fxRow1...), fxRow2...))), fxXID},
		"an extra-data block": {fxBegin, varchar,
			txEvent(w, rawRows(fxTable, 1, []byte{0, 3, 0, 0x2a, 0}, []byte{1}, []byte{0x01}, append(append([]byte(nil), fxRow1...), fxRow2...))), fxXID},
		"unused bitmap bits set": {fxBegin, varchar,
			txEvent(w, rawRows(fxTable, 1, nil, []byte{1}, []byte{0xff}, append(append([]byte(nil), fxRow1...), fxRow2...))), fxXID},
		"optional metadata (binlog_row_metadata)": {fxBegin,
			txEvent(replication.TABLE_MAP_EVENT, append(binlogtest.TableMapBody(fxTable, "orders", "markers", fxVarchar), "optional metadata"...)),
			fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID},
		"other table-map flags and null bitmap": {fxBegin, func() []byte {
			body := binlogtest.TableMapBody(fxTable, "orders", "markers", fxVarchar)
			body[6], body[len(body)-1] = 0xff, 0x01
			return txEvent(replication.TABLE_MAP_EVENT, body)
		}(), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID},
		"a ROWS_QUERY event":       {fxBegin, txEvent(replication.ROWS_QUERY_EVENT, []byte("\x06INSERT")), varchar, fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID},
		"COMMIT instead of an XID": {fxBegin, varchar, fxRows(w, fxTable, true, fxRow1, fxRow2), txEvent(replication.QUERY_EVENT, queryEventBody("COMMIT"))},
		"other event headers":      {reheader(fxBegin), reheader(varchar), reheader(fxRows(w, fxTable, true, fxRow1, fxRow2)), reheader(fxXID)},
		"transaction compression":  {payloadEvent(uint64(len(payload)), zstdFrame(t, payload))},
		"compressed, and cut differently": {func() []byte {
			p := inner(fxBegin, varchar, fxRows(w, fxTable, false, fxRow1), fxRows(w, fxTable, true, fxRow2), fxXID)
			return payloadEvent(uint64(len(p)), zstdFrame(t, p))
		}()},
	} {
		if got := effectOf(t, events...); got != base {
			t.Errorf("%s: digest %s, want the base %s", name, got, base)
		}
	}
}

// How a transaction with other row changes compares with the base
// (pitrplan.EffectDigest.Compare): other tables, classes or their order
// differ in the tables part; another encoding of the same tables is not
// comparable; other row bytes in the same encoding differ in the rows part.
func TestEffectDigestSeesRowChanges(t *testing.T) {
	base := effectOf(t, fxBase()...)
	w, u, d := replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2
	varchar := fxMap(fxTable, "orders", "markers", fxVarchar)
	other := fxMap(9, "orders", "other", fxVarchar)
	both := append(append([]byte(nil), fxRow1...), fxRow2...)
	two := fxMap(fxTable, "orders", "markers", fxVarchar, fxVarchar)
	const (
		tables = pitrplan.EffectTablesDiffer
		layout = pitrplan.EffectNotComparable
		rows   = pitrplan.EffectRowsDiffer
		equal  = pitrplan.EffectEqual
	)
	seen := map[pitrplan.EffectDigest]string{base: "the base"}
	for _, tc := range []struct {
		name   string
		want   pitrplan.EffectRelation
		events [][]byte
	}{
		{"another row value", rows, [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1, []byte("\x00\x06SECOND")), fxXID}},
		{"one letter of a value changed", rows, [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1, []byte("\x00\x06secone")), fxXID}},
		{"one row fewer", rows, [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1), fxXID}},
		{"one row more", rows, [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1, fxRow2, fxRow2), fxXID}},
		{"one row twice instead of two rows", rows, [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1, fxRow1), fxXID}},
		{"another table", tables, [][]byte{fxBegin, fxMap(fxTable, "orders", "other", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"another database", tables, [][]byte{fxBegin, fxMap(fxTable, "billing", "markers", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		// "ab"."c" and "a"."bc": the names are length-prefixed.
		{"names split a.bc", tables, [][]byte{fxBegin, fxMap(fxTable, "a", "bc", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"names split ab.c", tables, [][]byte{fxBegin, fxMap(fxTable, "ab", "c", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"a delete of the same images", tables, [][]byte{fxBegin, varchar, fxRows(d, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"an update of the same images", tables, [][]byte{fxBegin, varchar, fxRows(u, fxTable, true, fxRow1, fxRow2), fxXID}},
		// The same bytes, split between two tables at different places.
		{"the second row in another table", tables, [][]byte{fxBegin, varchar, other, fxRows(w, fxTable, false, fxRow1), fxRows(w, 9, true, fxRow2), fxXID}},
		{"the first row in another table", tables, [][]byte{fxBegin, varchar, other, fxRows(w, 9, false, fxRow1), fxRows(w, fxTable, true, fxRow2), fxXID}},
		{"both rows, then another table's", tables, [][]byte{fxBegin, varchar, other, fxRows(w, fxTable, false, fxRow1, fxRow2), fxRows(w, 9, true, fxRow2), fxXID}},
		{"another table's row between them", tables, [][]byte{fxBegin, varchar, other, fxRows(w, fxTable, false, fxRow1), fxRows(w, 9, false, fxRow2), fxRows(w, fxTable, true, fxRow2), fxXID}},
		{"a write, then a delete", tables, [][]byte{fxBegin, varchar, fxRows(w, fxTable, false, fxRow1), fxRows(d, fxTable, true, fxRow2), fxXID}},
		// The same table and class, encoded differently.
		{"two columns", layout, [][]byte{fxBegin, two, txEvent(w, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03}, both)), fxXID}},
		{"two columns, one present", layout, [][]byte{fxBegin, two, txEvent(w, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x01}, both)), fxXID}},
		{"two columns, the other present", layout, [][]byte{fxBegin, two, txEvent(w, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x02}, both)), fxXID}},
		{"another column type", layout, [][]byte{fxBegin, fxMap(fxTable, "orders", "markers", 253), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"other column metadata (a wider column)", layout, [][]byte{fxBegin, fxMapMeta(fxTable, "orders", "markers", []byte{fxVarchar}, []byte{0x90, 0x01}), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"a shorter column metadata block", layout, [][]byte{fxBegin, fxMapMeta(fxTable, "orders", "markers", []byte{fxVarchar}, []byte{0}), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"the second statement with other column metadata", layout, [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1),
			fxMapMeta(9, "orders", "markers", []byte{fxVarchar}, []byte{0x90, 0x01}), fxRows(w, 9, true, fxRow2), fxXID}},
	} {
		got := effectOf(t, tc.events...)
		if got.IsZero() {
			t.Errorf("%s: not comparable", tc.name)
			continue
		}
		if rel := base.Compare(got); rel != tc.want {
			t.Errorf("%s: %s compares %d with the base %s, want %d", tc.name, got, rel, base, tc.want)
		}
		if prev, ok := seen[got]; ok {
			t.Errorf("%s: the same digest as %s", tc.name, prev)
		}
		seen[got] = tc.name
	}

	// Updates: the images' bitmaps are part of the layout, each on its own.
	update := func(bitmaps []byte, rows []byte) pitrplan.EffectDigest {
		return effectOf(t, fxBegin, two, txEvent(u, rawRows(fxTable, 1, nil, []byte{2}, bitmaps, rows)), fxXID)
	}
	full, minimal, before := update([]byte{0x03, 0x03}, both), update([]byte{0x03, 0x01}, both), update([]byte{0x01, 0x03}, both)
	for name, rel := range map[string]pitrplan.EffectRelation{
		"another after-image bitmap (binlog_row_image=MINIMAL)": full.Compare(minimal),
		"another before-image bitmap":                           full.Compare(before),
		"the two bitmaps exchanged":                             minimal.Compare(before),
	} {
		if rel != layout {
			t.Errorf("an update with %s: relation %d, want not comparable", name, rel)
		}
	}
	if got := update([]byte{0x03, 0x03}, fxRow1); full.Compare(got) != rows {
		t.Errorf("an update with other row bytes: %s against %s", got, full)
	}

	// Two statements of one table with different encodings (a session that
	// changes binlog_row_image, or MINIMAL images of statements that set
	// other columns) are one group with two layouts, in their order.
	one := txEvent(u, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03, 0x01}, fxRow1))
	oneMore := txEvent(u, rawRows(fxTable, 0, nil, []byte{2}, []byte{0x03, 0x01}, fxRow2))
	second := txEvent(u, rawRows(9, 1, nil, []byte{2}, []byte{0x03, 0x02}, fxRow2))
	two9 := fxMap(9, "orders", "markers", fxVarchar, fxVarchar)
	mixed := effectOf(t, fxBegin, two, one, two9, second, fxXID)
	if got := effectOf(t, fxBegin, two, oneMore, one, two9, second, fxXID); mixed.Compare(got) != rows {
		t.Errorf("a group with two layouts and one more row: %s against %s", got, mixed)
	}
	if got := effectOf(t, fxBegin, two9, second, two, one, fxXID); mixed.Compare(got) != layout {
		t.Errorf("a group's two layouts in the other order: %s against %s", got, mixed)
	}
	if got := effectOf(t, fxBegin, two, one, two9, second, two, txEvent(u, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03, 0x01}, nil)), fxXID); mixed.Compare(got) != layout {
		t.Errorf("a group that returns to its first layout: %s against %s", got, mixed)
	}

	// What the rows part cannot see (the documented limit): a change that
	// only permutes the bytes of a group's row images.
	for name, events := range map[string][][]byte{
		"two rows' values exchanged":  {fxBegin, varchar, fxRows(w, fxTable, true, []byte("\x00\x06second"), []byte("\x00\x05first")), fxXID},
		"a value's letters reordered": {fxBegin, varchar, fxRows(w, fxTable, true, []byte("\x00\x05tsrif"), fxRow2), fxXID},
		"a letter moved between rows": {fxBegin, varchar, fxRows(w, fxTable, true, []byte("\x00\x06firsts"), []byte("\x00\x05econd")), fxXID},
	} {
		if got := effectOf(t, events...); got != base {
			t.Errorf("%s: digest %s, want the base's %s (the rows part is a multiset of bytes)", name, got, base)
		}
	}
}

// Row images are counted the same in events of any size: large events go
// through count's four lanes, small ones do not, and a group's bytes may be
// cut anywhere.
func TestEffectDigestCountsLargeEvents(t *testing.T) {
	w := replication.WRITE_ROWS_EVENTv2
	varchar := fxMap(fxTable, "orders", "markers", fxVarchar)
	// Runs of one value, every value, and a tail that is not a multiple
	// of four.
	rows := make([]byte, 3*effectLanesMin+3)
	for i := range rows {
		rows[i] = byte(i / 7)
		if i%1000 < 300 {
			rows[i] = 0
		}
	}
	digest := func(rows []byte, cut int) pitrplan.EffectDigest {
		events := [][]byte{fxBegin, varchar}
		for len(rows) > cut {
			events = append(events, fxRows(w, fxTable, false, rows[:cut]))
			rows = rows[cut:]
		}
		return effectOf(t, append(events, fxRows(w, fxTable, true, rows), fxXID)...)
	}
	whole := digest(rows, len(rows))
	for _, cut := range []int{1, 3, 100, effectLanesMin - 1, effectLanesMin, effectLanesMin + 1, 2*effectLanesMin + 2} {
		if got := digest(rows, cut); got != whole {
			t.Errorf("cut into events of %d bytes: %s, want %s", cut, got, whole)
		}
	}
	// The same bytes backwards are the same multiset; one other byte, at
	// any position modulo four, is not.
	backwards := append([]byte(nil), rows...)
	slices.Reverse(backwards)
	if got := digest(backwards, len(rows)); got != whole {
		t.Errorf("the bytes in reverse: %s, want %s", got, whole)
	}
	for _, at := range []int{0, 1, 2, 3, len(rows) - 1, len(rows) - 2} {
		changed := append([]byte(nil), rows...)
		changed[at] ^= 0x55
		if got := digest(changed, len(rows)); whole.Compare(got) != pitrplan.EffectRowsDiffer {
			t.Errorf("byte %d changed: %s against %s", at, got, whole)
		}
	}
}

// What cannot be framed with certainty, and what a replica does not re-log
// byte for byte, is not comparable: a zero digest, never a refusal.
func TestEffectDigestNotComparable(t *testing.T) {
	w := replication.WRITE_ROWS_EVENTv2
	varchar := fxMap(fxTable, "orders", "markers", fxVarchar)
	rows := fxRows(w, fxTable, true, fxRow1, fxRow2)
	both := append(append([]byte(nil), fxRow1...), fxRow2...)
	query := func(q string) []byte { return txEvent(replication.QUERY_EVENT, queryEventBody(q)) }
	// tableMapOf is a TABLE_MAP of the rows' table id with the given bytes
	// after its post-header.
	tableMapOf := func(tail ...byte) []byte {
		body := make([]byte, 8)
		body[0] = fxTable
		return txEvent(replication.TABLE_MAP_EVENT, append(body, tail...))
	}
	var manyMaps [][]byte
	manyMaps = append(manyMaps, fxBegin)
	for id := uint64(1); id <= effectMaxTables+1; id++ {
		manyMaps = append(manyMaps, fxMap(id, "orders", "markers", fxVarchar))
	}
	manyMaps = append(manyMaps, fxRows(w, 1, true, fxRow1), fxXID)
	for name, events := range map[string][][]byte{
		"no rows events (DDL)":                   {query("CREATE TABLE t (id INT)")},
		"no rows events (empty)":                 {fxBegin, query("COMMIT")},
		"a table map without rows":               {fxBegin, varchar, fxXID},
		"a SAVEPOINT beside the rows":            {fxBegin, varchar, rows, query("SAVEPOINT a"), fxXID},
		"closed by ROLLBACK":                     {fxBegin, varchar, rows, query("ROLLBACK")},
		"a statement before its rows (CTAS)":     {query("CREATE TABLE t2 SELECT 1"), varchar, rows, fxXID},
		"a statement-format change in the BEGIN": {fxBegin, query("UPDATE t SET a = 1"), varchar, rows, fxXID},
		"a user-variable event":                  {fxBegin, txEvent(replication.USER_VAR_EVENT, make([]byte, 8)), varchar, rows, fxXID},
		"a PARTIAL_UPDATE_ROWS event":            {fxBegin, varchar, rows, txEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, make([]byte, 16)), fxXID},
		"a version-1 rows event":                 {fxBegin, varchar, txEvent(replication.WRITE_ROWS_EVENTv1, make([]byte, 16)), fxXID},
		"a version-0 rows event":                 {fxBegin, varchar, txEvent(replication.WRITE_ROWS_EVENTv0, make([]byte, 16)), fxXID},
		"a JSON column":                          {fxBegin, fxMap(fxTable, "orders", "markers", fxJSON), rows, fxXID},
		"a JSON column beside others": {fxBegin, fxMap(fxTable, "orders", "markers", fxVarchar, fxJSON),
			txEvent(w, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03}, both)), fxXID},
		"rows without their table map":              {fxBegin, rows, fxXID},
		"rows of another table id":                  {fxBegin, varchar, fxRows(w, 9, true, fxRow1), fxXID},
		"rows after the statement's end":            {fxBegin, varchar, rows, fxRows(w, fxTable, true, fxRow1), fxXID},
		"a column count unlike the table map's":     {fxBegin, varchar, txEvent(w, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03}, both)), fxXID},
		"a rows event shorter than its post-header": {fxBegin, varchar, txEvent(w, make([]byte, 9)), fxXID},
		"an extra-data length below its own size":   {fxBegin, varchar, txEvent(w, append(append(make([]byte, 8), 1, 0), 1, 0x01)), fxXID},
		"an extra-data block past the event":        {fxBegin, varchar, txEvent(w, append(make([]byte, 8), 0xff, 0x7f, 1, 0x01)), fxXID},
		"no column count":                           {fxBegin, varchar, txEvent(w, rawRows(fxTable, 1, nil, nil, nil, nil)), fxXID},
		"a NULL column count":                       {fxBegin, varchar, txEvent(w, rawRows(fxTable, 1, nil, []byte{0xfb}, []byte{0x01}, both)), fxXID},
		"a truncated column count":                  {fxBegin, varchar, txEvent(w, rawRows(fxTable, 1, nil, []byte{0xfc, 1}, nil, nil)), fxXID},
		"zero columns":                              {fxBegin, varchar, txEvent(w, rawRows(fxTable, 1, nil, []byte{0}, nil, both)), fxXID},
		"more columns than MySQL allows":            {fxBegin, varchar, txEvent(w, rawRows(fxTable, 1, nil, []byte{0xfc, 0x01, 0x10}, make([]byte, 513), both)), fxXID},
		"a truncated bitmap":                        {fxBegin, varchar, txEvent(w, rawRows(fxTable, 1, nil, []byte{1}, nil, nil)), fxXID},
		"an update with one bitmap": {fxBegin, varchar,
			txEvent(replication.UPDATE_ROWS_EVENTv2, rawRows(fxTable, 1, nil, []byte{1}, []byte{0x01}, nil)), fxXID},
		"a table map shorter than its post-header": {fxBegin, txEvent(replication.TABLE_MAP_EVENT, make([]byte, 7)), rows, fxXID},
		"a table map without names":                {fxBegin, txEvent(replication.TABLE_MAP_EVENT, make([]byte, 8)), rows, fxXID},
		"a database name past the event":           {fxBegin, txEvent(replication.TABLE_MAP_EVENT, append(make([]byte, 8), 200, 'a', 0)), rows, fxXID},
		"a database name without its NUL":          {fxBegin, txEvent(replication.TABLE_MAP_EVENT, append(make([]byte, 8), 1, 'a', 'b', 1, 'c', 0, 1, fxVarchar)), rows, fxXID},
		"a table name without its NUL":             {fxBegin, txEvent(replication.TABLE_MAP_EVENT, append(make([]byte, 8), 1, 'a', 0, 1, 'c', 'd', 1, fxVarchar)), rows, fxXID},
		"a table map without a column count":       {fxBegin, txEvent(replication.TABLE_MAP_EVENT, append(make([]byte, 8), 1, 'a', 0, 1, 'c', 0)), rows, fxXID},
		"a table map with zero columns":            {fxBegin, txEvent(replication.TABLE_MAP_EVENT, append(make([]byte, 8), 1, 'a', 0, 1, 'c', 0, 0)), rows, fxXID},
		"a table map with fewer types than columns": {fxBegin, txEvent(replication.TABLE_MAP_EVENT, append(make([]byte, 8), 1, 'a', 0, 1, 'c', 0, 3, fxVarchar)),
			rows, fxXID},
		"a table map without its metadata length":  {fxBegin, tableMapOf(1, 'a', 0, 1, 'c', 0, 1, fxVarchar), rows, fxXID},
		"a truncated metadata length":              {fxBegin, tableMapOf(1, 'a', 0, 1, 'c', 0, 1, fxVarchar, 0xfc, 2), rows, fxXID},
		"column metadata past the event":           {fxBegin, tableMapOf(1, 'a', 0, 1, 'c', 0, 1, fxVarchar, 3, 0, 0), rows, fxXID},
		"a table map without its null bitmap":      {fxBegin, tableMapOf(1, 'a', 0, 1, 'c', 0, 1, fxVarchar, 2, 0, 0), rows, fxXID},
		"more table maps than one statement holds": manyMaps,
	} {
		if got := effectOf(t, events...); !got.IsZero() {
			t.Errorf("%s: digest %x, want not comparable", name, got)
		}
	}
	// The shortest table map that frames: one column, two bytes of metadata
	// and the null bitmap.
	if effectOf(t, fxBegin, tableMapOf(1, 'a', 0, 1, 'c', 0, 1, fxVarchar, 2, 0, 0, 0), rows, fxXID).IsZero() {
		t.Error("a complete table map: not comparable")
	}
	// As many table maps as one statement may hold are comparable.
	atLimit := append([][]byte{}, manyMaps[:1+effectMaxTables]...)
	atLimit = append(atLimit, fxRows(w, 1, true, fxRow1), fxXID)
	if effectOf(t, atLimit...).IsZero() {
		t.Errorf("%d table maps in one statement: not comparable", effectMaxTables)
	}
}

// The framing never reads past an event: random bodies, and every prefix of
// valid ones, are digested or set aside without a panic.
func TestEffectDigestFramingBounds(t *testing.T) {
	var d effectDigester
	lengths := make([]byte, 41)
	lengths[18], lengths[29], lengths[30], lengths[31] = tableMapPostHeader, rowsV2PostHeader, rowsV2PostHeader, rowsV2PostHeader
	d.init(lengths)
	if !d.enabled {
		t.Fatal("the digester does not accept MySQL 9.7's post-header lengths")
	}
	kinds := []replication.EventType{replication.TABLE_MAP_EVENT, replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2}
	valid := [][]byte{
		binlogtest.TableMapBody(fxTable, "orders", "markers", fxVarchar, fxVarchar),
		rawRows(fxTable, 1, []byte{0, 3, 0, 0x2a, 0}, []byte{2}, []byte{0x03}, fxRow1),
		rawRows(fxTable, 0, nil, []byte{2}, []byte{0x03, 0x01}, fxRow1),
		rawRows(fxTable, 0, nil, []byte{0xfc, 0x00, 0x10}, make([]byte, 512), fxRow1),
	}
	run := func(kind replication.EventType, body []byte) {
		d.begin()
		// A table map the rows events can resolve, so their own framing
		// is what is exercised.
		d.event(replication.TABLE_MAP_EVENT, binlogtest.TableMapBody(fxTable, "orders", "markers", fxVarchar, fxVarchar), "")
		d.event(kind, body, "")
		d.finish()
	}
	for i, body := range valid {
		for n := 0; n <= len(body); n++ {
			run(kinds[i], body[:n])
		}
	}
	// A fixed linear congruential sequence: the test is deterministic.
	state := uint32(0x2f1f78ac)
	next := func() uint32 { state = state*1664525 + 1013904223; return state >> 8 }
	for i := 0; i < 20000; i++ {
		body := append([]byte(nil), valid[i%len(valid)]...)
		for flips := 1 + next()%4; flips > 0; flips-- {
			body[next()%uint32(len(body))] = byte(next())
		}
		body = body[:next()%uint32(len(body)+1)]
		run(kinds[i%len(kinds)], body)
	}
}

// The digester's state belongs to one transaction: a transaction that is
// not comparable does not affect the next one, and a file whose
// FORMAT_DESCRIPTION declares other post-header lengths than the ones framed
// here has no digests at all.
func TestEffectDigestPerTransactionAndFile(t *testing.T) {
	base := effectOf(t, fxBase()...)
	gtid := splitEvents(txFixture())[4] // B:1's GTID event
	query := txEvent(replication.QUERY_EVENT, queryEventBody("SAVEPOINT a"))
	events := append([][]byte{fxBegin, fxMap(fxTable, "orders", "markers", fxVarchar), query,
		fxRows(replication.WRITE_ROWS_EVENTv2, fxTable, false, fxRow2), fxXID, gtid}, fxBase()...)
	idx, err := InspectTransactions(bytes.NewReader(txFile(events...)), TxIndexOptions{Label: "f"})
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Txns[0].Effect.IsZero() || idx.Txns[1].Effect != base {
		t.Fatalf("digests %x, %x; want not comparable, then the base %x", idx.Txns[0].Effect, idx.Txns[1].Effect, base)
	}

	// Another post-header length for table maps (a 4-byte table id, as
	// MySQL 5.1.4 wrote): nothing in the file is framed.
	all := splitEvents(txFile(fxBase()...))
	fde := append([]byte(nil), all[0][:len(all[0])-4]...)
	const lengths = replication.EventHeaderSize + 2 + 50 + 4 + 1
	if fde[lengths+int(replication.TABLE_MAP_EVENT)-1] != tableMapPostHeader {
		t.Fatalf("the fixture's FORMAT_DESCRIPTION declares a %d-byte table map post-header", fde[lengths+int(replication.TABLE_MAP_EVENT)-1])
	}
	fde[lengths+int(replication.TABLE_MAP_EVENT)-1] = 6
	all[0] = binary.LittleEndian.AppendUint32(fde, crc32.ChecksumIEEE(fde))
	idx, err = InspectTransactions(bytes.NewReader(joinEvents(all)), TxIndexOptions{Label: "f"})
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Txns[0].Effect.IsZero() {
		t.Fatalf("digest %x from a file with unknown post-header lengths", idx.Txns[0].Effect)
	}
}

// The digester's fixed state is charged to the budget for the pass (D-42)
// and returned with it.
func TestEffectDigestStateIsCharged(t *testing.T) {
	if effectStateBytes < 8<<10 || effectStateBytes > 24<<10 {
		t.Fatalf("effectStateBytes = %d", effectStateBytes)
	}
	data := txFile(fxBase()...)
	budget := pitrplan.NewBudget(effectStateBytes - 1)
	_, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "f", Budget: budget})
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, "effect digest state") {
		t.Fatalf("a budget below the digester's state: %v", err)
	}
	budget = pitrplan.NewBudget(1 << 20)
	if _, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "f", Budget: budget}); err != nil {
		t.Fatal(err)
	}
	if budget.Peak() < effectStateBytes || budget.Used() >= effectStateBytes {
		t.Fatalf("peak %d, used %d after the pass; the state is %d bytes", budget.Peak(), budget.Used(), effectStateBytes)
	}
}

// nativeCopy is one occurrence of a GTID in a native fixture file.
type nativeCopy struct {
	file string
	data []byte
	tx   pitrplan.Txn
	// end is the offset after the transaction's last event.
	end int64
}

// nativeCopies indexes every native fixture and groups the transactions by
// GTID.
func nativeCopies(t *testing.T) map[string][]nativeCopy {
	t.Helper()
	copies := map[string][]nativeCopy{}
	files := 0
	err := filepath.WalkDir("testdata/native", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".bin") {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		idx, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: p})
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		files++
		for i, tx := range idx.Txns {
			end := idx.Size
			if i+1 < len(idx.Txns) {
				end = idx.Txns[i+1].Pos
			}
			g := idx.GTID(i).String()
			copies[g] = append(copies[g], nativeCopy{file: p, data: data, tx: tx, end: end})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 20 {
		t.Fatalf("indexed %d native fixtures", files)
	}
	return copies
}

// events counts the transaction's outer events of kind.
func (c nativeCopy) events(kind replication.EventType) int {
	n := 0
	for pos := c.tx.Pos; pos < c.end; pos += int64(binary.LittleEndian.Uint32(c.data[pos+9:])) {
		if replication.EventType(c.data[pos+4]) == kind {
			n++
		}
	}
	return n
}

// rowImages concatenates the row images (every byte after the bitmaps) of
// the transaction's outer rows events, in file order.
func (c nativeCopy) rowImages(t *testing.T) []byte {
	t.Helper()
	var out []byte
	for pos := c.tx.Pos; pos < c.end; pos += int64(binary.LittleEndian.Uint32(c.data[pos+9:])) {
		kind := replication.EventType(c.data[pos+4])
		body := c.data[pos+replication.EventHeaderSize : pos+int64(binary.LittleEndian.Uint32(c.data[pos+9:]))-4]
		bitmaps := 1
		switch kind {
		case replication.TRANSACTION_PAYLOAD_EVENT:
			t.Fatalf("%s: the transaction at %d is compressed", c.file, c.tx.Pos)
		case replication.UPDATE_ROWS_EVENTv2:
			bitmaps = 2
		case replication.WRITE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
		default:
			continue
		}
		at := 8 + int(binary.LittleEndian.Uint16(body[8:]))
		columns := int(body[at])
		if columns >= 251 {
			t.Fatalf("%s: a rows event of %d or more columns", c.file, columns)
		}
		out = append(out, body[at+1+bitmaps*((columns+7)/8):]...)
	}
	return out
}

// Every GTID that occurs in more than one native fixture file is archived
// as an origin copy and as a replica copy. cases.tsv (testdata/native/effect:
// a MySQL 9.7 source and its replica; effect-mixed: an 8.4 source and a 9.7
// replica; see the README) says how each labelled pair's digests must
// compare, so a rule that made everything "not comparable", or everything
// equal, fails here:
//
//	equal         the same tables, layout and row bytes
//	layout        the same tables, encoded differently: not comparable
//	tables        other tables: the planner refuses
//	rows          the same tables and layout, other row bytes: refused
//	uncomparable  a copy has no digest
//
// Outside the labelled pairs no two copies may differ.
func TestEffectDigestNativeFixtures(t *testing.T) {
	copies := nativeCopies(t)
	expect := map[string]string{} // GTID → the expectation
	label := map[string]string{}
	bySet := map[string]pitrplan.Set{}
	for _, dir := range []string{"effect", "effect-mixed"} {
		raw, err := os.ReadFile(filepath.Join("testdata/native", dir, "cases.tsv"))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			f := strings.Split(line, "\t")
			if len(f) != 3 {
				t.Fatalf("%s/cases.tsv line %q", dir, line)
			}
			set, err := pitrplan.ParseSet(f[2])
			if err != nil || set.IsEmpty() {
				t.Fatalf("%s/cases.tsv %s: %q: %v", dir, f[0], f[2], err)
			}
			if _, dup := bySet[f[0]]; dup {
				t.Fatalf("case %s is listed twice", f[0])
			}
			bySet[f[0]] = set
			for g := range copies {
				if one, err := pitrplan.ParseSet(g); err == nil && set.Contains(one) {
					expect[g], label[g] = f[1], f[0]
				}
			}
		}
	}
	gtids := make([]string, 0, len(copies))
	for g := range copies {
		gtids = append(gtids, g)
	}
	sort.Strings(gtids)
	count := map[string]int{}
	cases := map[string]map[string]bool{} // expectation → labels seen
	for _, g := range gtids {
		cs := copies[g]
		if len(cs) < 2 {
			if expect[g] != "" {
				t.Errorf("%s (%s) occurs in %s only", g, label[g], cs[0].file)
			}
			continue
		}
		if len(cs) != 2 {
			t.Fatalf("%s occurs in %d fixture files", g, len(cs))
		}
		a, b := cs[0], cs[1]
		got := "uncomparable"
		if !a.tx.Effect.IsZero() && !b.tx.Effect.IsZero() {
			got = map[pitrplan.EffectRelation]string{pitrplan.EffectEqual: "equal", pitrplan.EffectNotComparable: "layout",
				pitrplan.EffectTablesDiffer: "tables", pitrplan.EffectRowsDiffer: "rows"}[a.tx.Effect.Compare(b.tx.Effect)]
		}
		want := expect[g]
		switch {
		case want == "" && (got == "tables" || got == "rows"):
			t.Errorf("%s: digests %s (%s) and %s (%s) differ in their %s", g, a.tx.Effect, a.file, b.tx.Effect, b.file, got)
		case want != "" && got != want:
			t.Errorf("%s (%s): the copies in %s and %s compare as %q (%s, %s), want %q", g, label[g], a.file, b.file, got, a.tx.Effect, b.tx.Effect, want)
		}
		count[got]++
		if want != "" {
			if cases[want] == nil {
				cases[want] = map[string]bool{}
			}
			cases[want][label[g]] = true
		}
		// The faithful and the drifted copies agree on everything the
		// planner compared before the digest; the injected ones do not.
		if same := a.tx.OCT == b.tx.OCT && a.tx.Srv == b.tx.Srv && a.tx.Empty() == b.tx.Empty(); same == strings.HasPrefix(label[g], "injected") {
			t.Errorf("%s (%s): commit time, origin server and emptiness agree: %v", g, label[g], same)
		}
	}
	if count["equal"] < 100 || count["layout"] < 10 || count["tables"] != 1 || count["rows"] != 2 || count["uncomparable"] < 30 {
		t.Fatalf("pairs: %v; want at least 100 equal, 10 with another layout and 30 without a digest, 1 with other tables (injected) and 2 with other rows (drift, injected-values)", count)
	}
	// Every case the capture scripts label is present, and the cases that
	// review R3 found refused by an order-sensitive, layout-blind digest
	// are among them.
	for want, names := range map[string][]string{
		"equal": {"nopk-update-key-order", "nopk-update-desc", "nopk-delete-desc", "nopk-update-index-range", "nopk-delete-asc", "uniqnull-update-desc",
			"pk-update-desc", "pk-delete-desc", "nopk-duplicates", "no-primary-key", "load-data", "fk-update-cascade", "fk-delete-cascade", "fk-set-null", "fk-multi",
			"cascade", "trigger", "partitioned", "source-only-partitioned", "replica-indexes", "replica-functional-index", "replica-default", "wide-table",
			"multi-table", "multi-table-update", "multi-table-delete", "insert-select", "big-blob", "big-insert", "big-update", "big-delete", "types", "generated",
			"tagged", "source-compressed", "only-source-compressed", "uncompressed-cuts", "rows-query", "on-update-timestamp", "invisible-column", "characters",
			"mixed-plain", "mixed-fk-update-cascade", "mixed-fk-delete-cascade", "mixed-fk-set-null", "mixed-trigger", "mixed-partitioned", "mixed-nopk-update-key-order"},
		"layout": {"row-image-minimal-update", "row-image-minimal-insert", "row-image-minimal-delete", "row-image-noblob-update", "replica-column",
			"replica-wider-column", "replica-invisible-primary-key"},
		"tables":       {"injected"},
		"rows":         {"drift", "injected-values"},
		"uncomparable": {"schema", "savepoint", "json", "json-partial", "json-neighbour", "mixed-format", "statement-format", "create-select", "mixed-schema"},
	} {
		for _, name := range names {
			if !cases[want][name] {
				t.Errorf("no pair of case %q compares as %q", name, want)
			}
		}
	}

	// The capture is what it claims.
	pair := func(name string) (s, r nativeCopy) {
		t.Helper()
		set := bySet[name]
		if set.Count() != 1 {
			t.Fatalf("case %s holds %s", name, set)
		}
		cs := copies[set.String()]
		if len(cs) != 2 {
			t.Fatalf("case %s: %d copies", name, len(cs))
		}
		for _, c := range cs {
			if strings.HasPrefix(filepath.Base(c.file), "s-") {
				s = c
			} else {
				r = c
			}
		}
		return s, r
	}
	// s and r cut one statement's rows at different sizes: with neither
	// side compressing, r holds more WRITE_ROWS events than s for the same
	// rows.
	s, r := pair("uncompressed-cuts")
	if ns, nr := s.events(replication.WRITE_ROWS_EVENTv2), r.events(replication.WRITE_ROWS_EVENTv2); ns < 2 || nr <= ns {
		t.Errorf("uncompressed-cuts: s cut the rows into %d events and r into %d; want 2 or more, and more on r", ns, nr)
	}
	// Compression on one side only, each way.
	s, r = pair("big-insert")
	if s.events(replication.TRANSACTION_PAYLOAD_EVENT) != 0 || r.events(replication.TRANSACTION_PAYLOAD_EVENT) != 1 || s.events(replication.WRITE_ROWS_EVENTv2) < 2 {
		t.Errorf("big-insert: want s uncompressed in 2 or more rows events and r compressed")
	}
	s, r = pair("only-source-compressed")
	if s.events(replication.TRANSACTION_PAYLOAD_EVENT) != 1 || r.events(replication.TRANSACTION_PAYLOAD_EVENT) != 0 {
		t.Errorf("only-source-compressed: want s compressed and r uncompressed")
	}
	// A tagged GTID, and a partial-JSON update the origin logged as such.
	if set := bySet["tagged"]; !strings.Contains(set.String(), ":fx:") {
		t.Errorf("case tagged holds %s", set)
	}
	s, r = pair("json-partial")
	if s.events(replication.PARTIAL_UPDATE_ROWS_EVENT) != 1 || r.events(replication.PARTIAL_UPDATE_ROWS_EVENT) != 0 {
		t.Errorf("json-partial: want a PARTIAL_UPDATE_ROWS event on s only")
	}
	// Tables without a primary key (or with only a nullable unique key):
	// the replica logged the same row images in another order, from MySQL
	// 9.7 and from 8.4 alike. With a primary key it kept the order.
	sorted := func(b []byte) string {
		c := append([]byte(nil), b...)
		sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
		return string(c)
	}
	for name, reordered := range map[string]bool{"nopk-update-key-order": true, "nopk-update-desc": true, "nopk-delete-desc": true, "uniqnull-update-desc": true,
		"mixed-nopk-update-key-order": true, "pk-update-desc": false, "pk-delete-desc": false} {
		s, r = pair(name)
		si, ri := s.rowImages(t), r.rowImages(t)
		if len(si) == 0 || sorted(si) != sorted(ri) {
			t.Errorf("%s: the two copies' row images are not the same bytes (%d and %d)", name, len(si), len(ri))
		}
		if bytes.Equal(si, ri) == reordered {
			t.Errorf("%s: the copies' row images are in the same order: %v, want %v", name, !reordered, !reordered)
		}
	}
	// The same tables with another encoding: the row images are not the
	// same bytes, although the data is.
	for _, name := range []string{"row-image-minimal-update", "row-image-noblob-update"} {
		s, r = pair(name)
		if si, ri := s.rowImages(t), r.rowImages(t); len(si) == 0 || len(si) >= len(ri) {
			t.Errorf("%s: s logged %d bytes of row images and r %d; want fewer on s", name, len(si), len(ri))
		}
	}
}

// Every transaction's Pos is the offset of its GTID event, which is where
// restore.py starts mysqlbinlog; restore.py checks that event's framing
// there (type, size, and end position = offset + size), so the fixtures
// must frame every GTID event that way.
func TestInspectTransactionsOffsets(t *testing.T) {
	check := func(name string, data []byte) int {
		t.Helper()
		idx, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: name})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var want []int64
		for pos := int64(4); pos < int64(len(data)); pos += int64(binary.LittleEndian.Uint32(data[pos+9:])) {
			switch replication.EventType(data[pos+4]) {
			case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT:
				want = append(want, pos)
				size := int64(binary.LittleEndian.Uint32(data[pos+9:]))
				if end := int64(binary.LittleEndian.Uint32(data[pos+13:])); end != pos+size {
					t.Errorf("%s: the GTID event at %d ends at %d, its header says %d", name, pos, pos+size, end)
				}
			}
		}
		if len(want) != len(idx.Txns) {
			t.Fatalf("%s: %d GTID events, %d transactions", name, len(want), len(idx.Txns))
		}
		for i, tx := range idx.Txns {
			if tx.Pos != want[i] {
				t.Errorf("%s: transaction %s at offset %d, its GTID event is at %d", name, idx.GTID(i), tx.Pos, want[i])
			}
		}
		return len(want)
	}
	if check("synthetic", txFixture()) != 3 {
		t.Fatal("the synthetic fixture holds three transactions")
	}
	total := 0
	for _, cs := range nativeCopies(t) {
		for _, c := range cs {
			if c.tx.Pos < 4 || c.end <= c.tx.Pos {
				t.Fatalf("%s: transaction at %d ends at %d", c.file, c.tx.Pos, c.end)
			}
			total++
		}
	}
	seen := map[string]bool{}
	for _, cs := range nativeCopies(t) {
		for _, c := range cs {
			if !seen[c.file] {
				seen[c.file] = true
				check(c.file, c.data)
			}
		}
	}
	if total < 150 {
		t.Fatalf("checked %d native transactions", total)
	}
}
