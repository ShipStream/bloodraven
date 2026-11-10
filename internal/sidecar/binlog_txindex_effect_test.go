package sidecar

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
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

// What a replica may change when it re-logs a transaction leaves the digest
// alone.
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
		"other rows-event flags": {fxBegin, varchar,
			txEvent(w, rawRows(fxTable, 0x0007, nil, []byte{1}, []byte{0x01}, append(append([]byte(nil), fxRow1...), fxRow2...))), fxXID},
		"an extra-data block": {fxBegin, varchar,
			txEvent(w, rawRows(fxTable, 1, []byte{0, 3, 0, 0x2a, 0}, []byte{1}, []byte{0x01}, append(append([]byte(nil), fxRow1...), fxRow2...))), fxXID},
		"unused bitmap bits set": {fxBegin, varchar,
			txEvent(w, rawRows(fxTable, 1, nil, []byte{1}, []byte{0xff}, append(append([]byte(nil), fxRow1...), fxRow2...))), fxXID},
		"other column metadata and optional metadata": {fxBegin,
			txEvent(replication.TABLE_MAP_EVENT, append(binlogtest.TableMapBody(fxTable, "orders", "markers", fxVarchar), "optional metadata"...)),
			fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID},
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
			t.Errorf("%s: digest %x, want the base %x", name, got, base)
		}
	}
}

// Other row changes give another digest.
func TestEffectDigestSeesRowChanges(t *testing.T) {
	base := effectOf(t, fxBase()...)
	w, u, d := replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2
	varchar := fxMap(fxTable, "orders", "markers", fxVarchar)
	other := fxMap(9, "orders", "other", fxVarchar)
	both := append(append([]byte(nil), fxRow1...), fxRow2...)
	seen := map[pitrplan.EffectDigest]string{base: "the base"}
	for _, tc := range []struct {
		name   string
		events [][]byte
	}{
		{"another row value", [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1, []byte("\x00\x06SECOND")), fxXID}},
		{"one row fewer", [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow1), fxXID}},
		{"rows in another order", [][]byte{fxBegin, varchar, fxRows(w, fxTable, true, fxRow2, fxRow1), fxXID}},
		{"another table", [][]byte{fxBegin, fxMap(fxTable, "orders", "other", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"another database", [][]byte{fxBegin, fxMap(fxTable, "billing", "markers", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		// "ab"."c" and "a"."bc": the names are length-prefixed.
		{"names split a.bc", [][]byte{fxBegin, fxMap(fxTable, "a", "bc", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"names split ab.c", [][]byte{fxBegin, fxMap(fxTable, "ab", "c", fxVarchar), fxRows(w, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"a delete of the same images", [][]byte{fxBegin, varchar, fxRows(d, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"an update of the same images", [][]byte{fxBegin, varchar, fxRows(u, fxTable, true, fxRow1, fxRow2), fxXID}},
		{"an update with another after-image bitmap", [][]byte{fxBegin, fxMap(fxTable, "orders", "markers", fxVarchar, fxVarchar),
			txEvent(u, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03, 0x01}, both)), fxXID}},
		{"an update with both columns in the after image", [][]byte{fxBegin, fxMap(fxTable, "orders", "markers", fxVarchar, fxVarchar),
			txEvent(u, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03, 0x03}, both)), fxXID}},
		{"two columns", [][]byte{fxBegin, fxMap(fxTable, "orders", "markers", fxVarchar, fxVarchar),
			txEvent(w, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x03}, both)), fxXID}},
		{"two columns, one present", [][]byte{fxBegin, fxMap(fxTable, "orders", "markers", fxVarchar, fxVarchar),
			txEvent(w, rawRows(fxTable, 1, nil, []byte{2}, []byte{0x01}, both)), fxXID}},
		// The same bytes, split between two tables at different places.
		{"the second row in another table", [][]byte{fxBegin, varchar, other, fxRows(w, fxTable, false, fxRow1), fxRows(w, 9, true, fxRow2), fxXID}},
		{"the first row in another table", [][]byte{fxBegin, varchar, other, fxRows(w, 9, false, fxRow1), fxRows(w, fxTable, true, fxRow2), fxXID}},
		{"both rows, then another table's", [][]byte{fxBegin, varchar, other, fxRows(w, fxTable, false, fxRow1, fxRow2), fxRows(w, 9, true, fxRow2), fxXID}},
		{"another table's row between them", [][]byte{fxBegin, varchar, other, fxRows(w, fxTable, false, fxRow1), fxRows(w, 9, false, fxRow2), fxRows(w, fxTable, true, fxRow2), fxXID}},
		{"a write, then a delete", [][]byte{fxBegin, varchar, fxRows(w, fxTable, false, fxRow1), fxRows(d, fxTable, true, fxRow2), fxXID}},
	} {
		got := effectOf(t, tc.events...)
		if got.IsZero() {
			t.Errorf("%s: not comparable", tc.name)
			continue
		}
		if prev, ok := seen[got]; ok {
			t.Errorf("%s: the same digest as %s", tc.name, prev)
		}
		seen[got] = tc.name
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
		"more table maps than one statement holds": manyMaps,
	} {
		if got := effectOf(t, events...); !got.IsZero() {
			t.Errorf("%s: digest %x, want not comparable", name, got)
		}
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
	if effectStateBytes < 2<<10 || effectStateBytes > 8<<10 {
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

// Every GTID that occurs in more than one native fixture file has the same
// digest in each copy that is comparable, except the two copies the effect
// capture made differ on purpose (testdata/native/effect: a source and its
// replica, cut at different event sizes, compressed on one side only; see
// the README). cases.tsv says what each labelled pair must be, so a rule
// that made everything "not comparable" fails here.
func TestEffectDigestNativeFixtures(t *testing.T) {
	copies := nativeCopies(t)
	raw, err := os.ReadFile("testdata/native/effect/cases.tsv")
	if err != nil {
		t.Fatal(err)
	}
	expect := map[string]string{} // GTID → equal | differ | uncomparable
	label := map[string]string{}
	bySet := map[string]pitrplan.Set{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 3 {
			t.Fatalf("cases.tsv line %q", line)
		}
		set, err := pitrplan.ParseSet(f[2])
		if err != nil || set.IsEmpty() {
			t.Fatalf("cases.tsv %s: %q: %v", f[0], f[2], err)
		}
		bySet[f[0]] = set
		for g := range copies {
			if one, err := pitrplan.ParseSet(g); err == nil && set.Contains(one) {
				expect[g], label[g] = f[1], f[0]
			}
		}
	}
	gtids := make([]string, 0, len(copies))
	for g := range copies {
		gtids = append(gtids, g)
	}
	sort.Strings(gtids)
	count := map[string]int{}
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
		got := "equal"
		switch {
		case a.tx.Effect.IsZero() || b.tx.Effect.IsZero():
			got = "uncomparable"
		case a.tx.Effect != b.tx.Effect:
			got = "differ"
		}
		want := expect[g]
		switch {
		case want == "" && got == "differ":
			t.Errorf("%s: digests %x (%s) and %x (%s) differ", g, a.tx.Effect, a.file, b.tx.Effect, b.file)
		case want != "" && got != want:
			t.Errorf("%s (%s): the copies in %s and %s are %s (%x, %x), want %s", g, label[g], a.file, b.file, got, a.tx.Effect, b.tx.Effect, want)
		}
		count[got]++
		// The faithful and the drifted copies agree on everything the
		// planner compared before the digest; the injected one does not.
		if same := a.tx.OCT == b.tx.OCT && a.tx.Srv == b.tx.Srv && a.tx.Empty() == b.tx.Empty(); same == (label[g] == "injected") {
			t.Errorf("%s (%s): commit time, origin server and emptiness agree: %v", g, label[g], same)
		}
	}
	if count["equal"] < 30 || count["differ"] != 2 || count["uncomparable"] < 10 {
		t.Fatalf("pairs: %v; want at least 30 equal, exactly 2 that differ (drift, injected) and at least 10 not comparable", count)
	}

	// The capture is what it claims. s and r cut one statement's rows at
	// different sizes: with neither side compressing, r holds more
	// WRITE_ROWS events than s for the same rows.
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
