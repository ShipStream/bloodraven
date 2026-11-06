package sidecar

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

const (
	txA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	txB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)

var txT0 = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

func txFixture() []byte {
	return binlogtest.File{ServerID: 102, Previous: txA + ":1-10", Txns: []binlogtest.Txn{
		{SID: txA, GNO: 11, Commit: txT0.Add(time.Second), ServerID: 101},
		{SID: txB, GNO: 1, Commit: txT0.Add(2 * time.Second), ServerID: 102, Empty: true},
		{SID: txB, GNO: 2, Commit: txT0.Add(3*time.Second + 123456*time.Microsecond), ServerID: 102},
	}}.MustBytes()
}

// indexMatchesCoverage checks that the index pass and the sidecar's
// inspection agree on every manifest hint, so a correctly archived row is
// never refused as ObjectInvalid.
func indexMatchesCoverage(t *testing.T, data []byte, idx *pitrplan.FileIndex) {
	t.Helper()
	ev, err := InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	c := ev.Metadata.GTIDCoverage
	if !c.Complete {
		return
	}
	if idx.GTIDs.String() != pitrplan.MustParseSet(c.GTIDs).String() || idx.EmptyGTIDs.String() != pitrplan.MustParseSet(c.EmptyGTIDs).String() ||
		idx.CommitMin != c.OriginalCommitMin || idx.CommitMax != c.OriginalCommitMax ||
		idx.Previous.String() != pitrplan.MustParseSet(ev.Metadata.PreviousGTIDs).String() || idx.Digest != ev.Digest {
		t.Fatalf("index %+v disagrees with coverage %+v (prev %q)", idx, c, ev.Metadata.PreviousGTIDs)
	}
}

func TestInspectTransactions(t *testing.T) {
	data := txFixture()
	idx, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "b/f"})
	if err != nil {
		t.Fatal(err)
	}
	if idx.WriterServerID != 102 || len(idx.Txns) != 3 || idx.Size != int64(len(data)) {
		t.Fatalf("index %+v", idx)
	}
	want := []struct {
		g     string
		srv   uint32
		empty bool
		oct   int64
	}{
		{txA + ":11", 101, false, txT0.Add(time.Second).UnixMicro()},
		{txB + ":1", 102, true, txT0.Add(2 * time.Second).UnixMicro()},
		{txB + ":2", 102, false, txT0.Add(3*time.Second + 123456*time.Microsecond).UnixMicro()},
	}
	for i, w := range want {
		tx := idx.Txns[i]
		if idx.GTID(i).String() != w.g || tx.Srv != w.srv || tx.Empty() != w.empty || tx.OCT != w.oct {
			t.Errorf("txn %d = %s %+v, want %+v", i, idx.GTID(i), tx, w)
		}
	}
	indexMatchesCoverage(t, data, idx)
}

func TestInspectTransactionsNativeFixtures(t *testing.T) {
	for _, tc := range []struct {
		file  string
		flags pitrplan.TxnFlags
	}{
		{"testdata/native/compressed-session.bin", 0},
		{"testdata/native/initial.bin", 0},
		{"testdata/native/xa-prepare.bin", pitrplan.TxnXA},
		{"testdata/native/xa.bin", pitrplan.TxnXA},
		{"testdata/native/load-data.bin", pitrplan.TxnUnsupported},
	} {
		data, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		idx, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: tc.file})
		if err != nil {
			t.Fatalf("%s: %v", tc.file, err)
		}
		for i, tx := range idx.Txns {
			if tx.Flags&(pitrplan.TxnXA|pitrplan.TxnUnsupported) != tc.flags || tx.Srv != idx.WriterServerID {
				t.Errorf("%s txn %d %s flags %b", tc.file, i, idx.GTID(i), tx.Flags)
			}
		}
		indexMatchesCoverage(t, data, idx)
	}
	// The MySQL 9.7 failback capture: tagged GTIDs, compressed payloads,
	// and replica copies (srv ≠ fsrv) in one mixed file.
	data, err := os.ReadFile("testdata/native/failback/a-mysql-bin.000003.bin")
	if err != nil {
		t.Fatal(err)
	}
	idx, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "a/mysql-bin.000003"})
	if err != nil {
		t.Fatal(err)
	}
	tagged, origin, replica := 0, 0, 0
	for i, tx := range idx.Txns {
		if strings.HasSuffix(idx.GTID(i).SID, ":fixture") {
			tagged++
		}
		if tx.Srv == idx.WriterServerID {
			origin++
		} else {
			replica++
		}
	}
	if tagged == 0 || origin == 0 || replica == 0 {
		t.Fatalf("mixed file: %d tagged, %d origin, %d replica transactions", tagged, origin, replica)
	}
	indexMatchesCoverage(t, data, idx)
}

func TestInspectTransactionsRefusals(t *testing.T) {
	data := txFixture()
	noTimes := binlogtest.File{ServerID: 101, Txns: []binlogtest.Txn{{SID: txA, GNO: 1, ServerID: 101}}}.MustBytes()
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-30] ^= 0xff
	for name, tc := range map[string]struct {
		data []byte
		want pitrarchive.Token
	}{
		"no original commit timestamp": {noTimes, pitrarchive.TokenReplayUnsupported},
		"truncated":                    {data[:len(data)-7], pitrarchive.TokenArchiveObjectInvalid},
		"bad CRC":                      {corrupt, pitrarchive.TokenArchiveObjectInvalid},
		"bad magic":                    {append([]byte("xbin"), data[4:]...), pitrarchive.TokenArchiveObjectInvalid},
		// F208: a payload where FORMAT_DESCRIPTION must be is a broken
		// file, not an unsupported transaction.
		"payload before FORMAT_DESCRIPTION": {append([]byte{0xfe, 'b', 'i', 'n'}, payloadEvent(10, zstdFrame(t, make([]byte, 10)))...), pitrarchive.TokenArchiveObjectInvalid},
	} {
		_, err := InspectTransactions(bytes.NewReader(tc.data), TxIndexOptions{Label: "f"})
		if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != tc.want {
			t.Errorf("%s: %v, want %s", name, err, tc.want)
		}
	}
	// A read error is not a refusal.
	_, err := InspectTransactions(errReader{}, TxIndexOptions{Label: "f"})
	if _, ok := pitrplan.RefusalOf(err); ok || err == nil {
		t.Errorf("read error became %v", err)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

// splitEvents splits a binlog after its magic into raw events.
func splitEvents(data []byte) [][]byte {
	var out [][]byte
	for pos := 4; pos < len(data); {
		n := int(binary.LittleEndian.Uint32(data[pos+9:]))
		out = append(out, data[pos:pos+n])
		pos += n
	}
	return out
}

func joinEvents(events [][]byte) []byte {
	out := []byte{0xfe, 'b', 'i', 'n'}
	for _, e := range events {
		out = append(out, e...)
	}
	return out
}

// payloadEvent crafts a TRANSACTION_PAYLOAD_EVENT whose TLV header claims
// uncompressedSize, with a valid CRC.
func payloadEvent(uncompressedSize uint64, payload []byte) []byte {
	lenenc := func(v uint64) []byte {
		switch {
		case v < 251:
			return []byte{byte(v)}
		case v < 1<<16:
			return []byte{0xfc, byte(v), byte(v >> 8)}
		case v < 1<<24:
			return []byte{0xfd, byte(v), byte(v >> 8), byte(v >> 16)}
		}
		b := []byte{0xfe}
		return binary.LittleEndian.AppendUint64(b, v)
	}
	field := func(kind byte, v uint64) []byte {
		val := lenenc(v)
		return append(append([]byte{kind}, lenenc(uint64(len(val)))...), val...)
	}
	var body []byte
	body = append(body, field(1, uint64(len(payload)))...)
	body = append(body, field(2, 0)...)
	body = append(body, field(3, uncompressedSize)...)
	body = append(body, 0)
	body = append(body, payload...)
	size := 19 + len(body) + 4
	h := make([]byte, 19)
	binary.LittleEndian.PutUint32(h[0:], uint32(txT0.Unix()))
	h[4] = byte(replication.TRANSACTION_PAYLOAD_EVENT)
	binary.LittleEndian.PutUint32(h[5:], 101)
	binary.LittleEndian.PutUint32(h[9:], uint32(size))
	ev := append(h, body...)
	return binary.LittleEndian.AppendUint32(ev, crc32.ChecksumIEEE(ev))
}

// D-42: the decoder bounds refuse before Parse or zstd runs.
func TestInspectTransactionsDecoderBounds(t *testing.T) {
	budget := pitrplan.NewBudget(8 << 20) // MaxEventBytes = 1 MiB
	limit := budget.MaxEventBytes()
	var parsed []int
	decompressed := 0
	oldParse, oldDecompress := txIndexParse, txIndexDecompress
	defer func() { txIndexParse, txIndexDecompress = oldParse, oldDecompress }()
	txIndexParse = func(p *replication.BinlogParser, raw []byte) (*replication.BinlogEvent, error) {
		parsed = append(parsed, len(raw))
		return oldParse(p, raw)
	}
	txIndexDecompress = func(src []byte, n int64) ([]byte, error) {
		decompressed++
		return oldDecompress(src, n)
	}
	events := splitEvents(txFixture())

	// An event_size above the bound: only its header is read.
	huge := make([]byte, 19)
	copy(huge, events[3])
	binary.LittleEndian.PutUint32(huge[9:], uint32(limit+1))
	data := joinEvents(append(append([][]byte{}, events[:3]...), huge))
	_, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "f", Budget: budget})
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("oversized event: %v", err)
	}
	for _, n := range parsed {
		if int64(n) > limit {
			t.Fatalf("Parse ran on a %d-byte event", n)
		}
	}

	// A payload whose uncompressed_size is above the bound: zstd never runs.
	parsed, decompressed = nil, 0
	ev := payloadEvent(uint64(limit)+1, []byte{0x28, 0xb5, 0x2f, 0xfd})
	data = joinEvents(append(append([][]byte{}, events[:3]...), ev))
	_, err = InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "f", Budget: budget})
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("oversized payload: %v", err)
	}
	if decompressed != 0 {
		t.Fatal("zstd ran on an oversized payload")
	}
	for _, n := range parsed {
		if n == len(ev) {
			t.Fatal("Parse ran on the payload event")
		}
	}
	// Only the indexed structures stay charged; event and payload
	// charges are transient. Per run the index keeps 448 bytes: the
	// Previous_gtids set, the transaction, its new source identifier and
	// its first interval (review F047).
	if budget.Used() > 3*448 {
		t.Errorf("transient charges leaked: %d bytes", budget.Used())
	}
}

// zstdFrame compresses src into one single-segment frame with its content
// size, as MySQL writes payloads.
func zstdFrame(t *testing.T, src []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithSingleSegment(true))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	return enc.EncodeAll(src, nil)
}

// zstdStream compresses src as a streaming frame, which declares no content
// size; window sets the frame's declared window.
func zstdStream(t *testing.T, src []byte, window int) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(window), zstd.WithSingleSegment(false))
	if err != nil {
		t.Fatal(err)
	}
	// Flushing before Close writes the frame header before the size is
	// known.
	if _, err := enc.Write(src); err != nil {
		t.Fatal(err)
	}
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	var h zstd.Header
	if err := h.Decode(buf.Bytes()); err != nil || h.HasFCS {
		t.Fatalf("streaming frame header %+v: %v", h, err)
	}
	return buf.Bytes()
}

// txEvent is an outer event of kind with a valid CRC32.
func txEvent(kind replication.EventType, body []byte) []byte {
	h := make([]byte, 19)
	binary.LittleEndian.PutUint32(h[0:], uint32(txT0.Unix()))
	h[4] = byte(kind)
	binary.LittleEndian.PutUint32(h[5:], 101)
	binary.LittleEndian.PutUint32(h[9:], uint32(19+len(body)+4))
	ev := append(h, body...)
	return binary.LittleEndian.AppendUint32(ev, crc32.ChecksumIEEE(ev))
}

// innerEvent is an event inside a payload (no checksum).
func innerEvent(kind replication.EventType, body []byte) []byte {
	h := make([]byte, 19)
	h[4] = byte(kind)
	binary.LittleEndian.PutUint32(h[9:], uint32(19+len(body)))
	return append(h, body...)
}

// queryEventBody is a QUERY_EVENT post-header (no schema, no status
// variables), the schema's NUL and the statement.
func queryEventBody(q string) []byte {
	return append(append(make([]byte, 13), 0), q...)
}

func xidBody() []byte { return make([]byte, 8) }

// prefix is the fixture's FORMAT_DESCRIPTION, PREVIOUS_GTIDS and its first
// GTID event (A:11), whose transaction the cases below complete.
func txPrefix() [][]byte { return splitEvents(txFixture())[:3] }

func txFile(events ...[]byte) []byte {
	return joinEvents(append(txPrefix(), events...))
}

func refusalToken(t *testing.T, data []byte) (pitrarchive.Token, string) {
	t.Helper()
	_, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "f"})
	r, ok := pitrplan.RefusalOf(err)
	if !ok {
		return "", fmt.Sprint(err)
	}
	return r.Token, r.Message
}

// F080: every transaction ends with its terminator before the next GTID or
// the end of the file, and nothing but bookkeeping follows a terminator.
func TestInspectTransactionsTermination(t *testing.T) {
	gtid := splitEvents(txFixture())[4] // B:1's GTID event
	begin := txEvent(replication.QUERY_EVENT, queryEventBody("BEGIN"))
	insert := txEvent(replication.QUERY_EVENT, queryEventBody("INSERT INTO t VALUES (1)"))
	tableMap := txEvent(replication.TABLE_MAP_EVENT, make([]byte, 16))
	rows := txEvent(replication.WRITE_ROWS_EVENTv2, make([]byte, 16))
	xid := txEvent(replication.XID_EVENT, xidBody())
	incident := txEvent(replication.INCIDENT_EVENT, make([]byte, 4))
	stop := txEvent(replication.STOP_EVENT, nil)
	for _, tc := range []struct {
		name   string
		events [][]byte
		want   pitrarchive.Token
		detail string
	}{
		{name: "row transaction", events: [][]byte{begin, tableMap, rows, xid, stop}},
		{name: "statement outside BEGIN", events: [][]byte{insert, gtid, begin, txEvent(replication.QUERY_EVENT, queryEventBody("COMMIT"))}},
		{name: "rows of a statement, closed by their XID", events: [][]byte{txEvent(replication.QUERY_EVENT, queryEventBody("CREATE TABLE t2 SELECT 1")), tableMap, rows, xid}},
		{name: "incident after the XID", events: [][]byte{begin, rows, xid, incident}, want: pitrarchive.TokenReplayUnsupported, detail: "outside any GTID transaction"},
		{name: "incident after a DDL", events: [][]byte{txEvent(replication.QUERY_EVENT, queryEventBody("CREATE TABLE t (id INT)")), incident}, want: pitrarchive.TokenReplayUnsupported, detail: "outside any GTID transaction"},
		{name: "XID after the XID", events: [][]byte{begin, rows, xid, xid}, want: pitrarchive.TokenReplayUnsupported, detail: "outside any GTID transaction"},
		{name: "file ends before the XID", events: [][]byte{begin, tableMap, rows}, want: pitrarchive.TokenArchiveObjectInvalid, detail: "ends inside transaction " + txA + ":11"},
		{name: "file ends after the GTID", events: nil, want: pitrarchive.TokenArchiveObjectInvalid, detail: "ends inside transaction"},
		{name: "statement rows without their XID", events: [][]byte{txEvent(replication.QUERY_EVENT, queryEventBody("CREATE TABLE t2 SELECT 1")), tableMap, rows}, want: pitrarchive.TokenArchiveObjectInvalid, detail: "ends inside"},
		{name: "next GTID before the XID", events: [][]byte{begin, rows, gtid}, want: pitrarchive.TokenArchiveObjectInvalid, detail: "without a terminator before " + txB + ":1"},
		{name: "XA PREPARE without XA START", events: [][]byte{txEvent(replication.XA_PREPARE_LOG_EVENT, make([]byte, 16))}, want: pitrarchive.TokenArchiveObjectInvalid, detail: "without XA START"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tok, msg := refusalToken(t, txFile(tc.events...))
			if tok != tc.want || !strings.Contains(msg, tc.detail) {
				t.Fatalf("got %s %q, want %s %q", tok, msg, tc.want, tc.detail)
			}
		})
	}
}

// The MySQL 9.7 shapes the terminator rule must accept: DDL, BEGIN … XID,
// the atomic CTAS (BEGIN, CREATE … START TRANSACTION, rows, XID), MyISAM
// (BEGIN … COMMIT), XA PREPARE / XA COMMIT, compressed transactions and an
// empty tagged transaction (F080); and a Previous_gtids whose tagged-only
// UUID go-mysql attributes to another server (F079).
func TestInspectTransactionsNativeStructures(t *testing.T) {
	data, err := os.ReadFile("testdata/native/txindex/structures.bin")
	if err != nil {
		t.Fatal(err)
	}
	// F207: go-mysql decodes only FORMAT_DESCRIPTION and GTID events; no
	// table map or row event reaches it, so it keeps no table state.
	parsed := map[replication.EventType]int{}
	old := txIndexParse
	defer func() { txIndexParse = old }()
	txIndexParse = func(p *replication.BinlogParser, raw []byte) (*replication.BinlogEvent, error) {
		parsed[replication.EventType(raw[4])]++
		return old(p, raw)
	}
	idx, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "structures"})
	if err != nil {
		t.Fatal(err)
	}
	for kind := range parsed {
		if kind != replication.FORMAT_DESCRIPTION_EVENT && kind != replication.GTID_EVENT && kind != replication.GTID_TAGGED_LOG_EVENT {
			t.Errorf("go-mysql decoded a %s", kind)
		}
	}
	const server = "2f1f78ac-d998-11f1-87d6-5aba2ed1f094"
	const tagged = "cccccccc-cccc-cccc-cccc-cccccccccccc:tg"
	if got := idx.GTIDs.String(); got != server+":1-11,"+tagged+":1-2" {
		t.Fatalf("gtids %s", got)
	}
	xa := 0
	for i, tx := range idx.Txns {
		if tx.Flags&pitrplan.TxnUnsupported != 0 {
			t.Errorf("txn %s flagged unsupported", idx.GTID(i))
		}
		if tx.Flags&pitrplan.TxnXA != 0 {
			xa++
		}
	}
	if xa != 2 || idx.EmptyGTIDs.String() != tagged+":1" {
		t.Fatalf("%d XA transactions, empty %s", xa, idx.EmptyGTIDs)
	}
	indexMatchesCoverage(t, data, idx)

	xaData, err := os.ReadFile("testdata/native/txindex/xa-shapes.bin")
	if err != nil {
		t.Fatal(err)
	}
	xaIdx, err := InspectTransactions(bytes.NewReader(xaData), TxIndexOptions{Label: "xa-shapes"})
	if err != nil {
		t.Fatal(err)
	}
	if got := xaIdx.GTIDs.String(); got != server+":16-21" {
		t.Fatalf("xa-shapes gtids %s", got)
	}
	for i, tx := range xaIdx.Txns {
		if wantXA := i < 3; (tx.Flags&pitrplan.TxnXA != 0) != wantXA || tx.Flags&pitrplan.TxnUnsupported != 0 {
			t.Errorf("xa-shapes txn %s flags %b", xaIdx.GTID(i), tx.Flags)
		}
	}
	indexMatchesCoverage(t, xaData, xaIdx)

	next, err := os.ReadFile("testdata/native/txindex/tagged-only-previous.bin")
	if err != nil {
		t.Fatal(err)
	}
	idx, err = InspectTransactions(bytes.NewReader(next), TxIndexOptions{Label: "tagged-only-previous"})
	if err != nil {
		t.Fatal(err)
	}
	if got := idx.Previous.String(); got != server+":1-11,"+tagged+":1-2" {
		t.Fatalf("previous %s", got)
	}
	if got := idx.GTIDs.String(); got != server+":12" {
		t.Fatalf("gtids %s", got)
	}
}

// F079: the PREVIOUS_GTIDS decoder.
func TestDecodePreviousGTIDs(t *testing.T) {
	uuid := func(b byte) []byte { return bytes.Repeat([]byte{b}, 16) }
	ivs := func(pairs ...int64) []byte {
		out := binary.LittleEndian.AppendUint64(nil, uint64(len(pairs)/2))
		for _, v := range pairs {
			out = binary.LittleEndian.AppendUint64(out, uint64(v))
		}
		return out
	}
	classic := func(sources ...[]byte) []byte {
		out := binary.LittleEndian.AppendUint64(nil, uint64(len(sources)))
		for _, s := range sources {
			out = append(out, s...)
		}
		return out
	}
	taggedHeader := func(n int) []byte { return []byte{1, byte(n), 0, 0, 0, 0, 0, 1} }
	src := func(u []byte, tag string, iv []byte) []byte {
		out := append([]byte(nil), u...)
		if tag != "-" {
			out = append(out, byte(len(tag)<<1))
			out = append(out, tag...)
		}
		return append(out, iv...)
	}
	a, b := strings.Repeat("aa", 4)+"-aaaa-aaaa-aaaa-"+strings.Repeat("aa", 6), strings.Repeat("bb", 4)+"-bbbb-bbbb-bbbb-"+strings.Repeat("bb", 6)
	for _, tc := range []struct {
		name string
		data []byte
		want string
		err  string
	}{
		{name: "empty", data: classic(), want: ""},
		{name: "classic", data: classic(src(uuid(0xaa), "-", ivs(1, 11, 20, 21)), src(uuid(0xbb), "-", ivs(3, 5))), want: a + ":1-10:20," + b + ":3-4"},
		{name: "tagged after the same UUID", data: append(taggedHeader(2), append(src(uuid(0xaa), "", ivs(1, 3)), src(uuid(0xaa), "t1", ivs(1, 2))...)...), want: a + ":1-2," + a + ":t1:1"},
		{name: "tagged-only UUID", data: append(taggedHeader(2), append(src(uuid(0xaa), "", ivs(1, 3)), src(uuid(0xbb), "TG", ivs(1, 3))...)...), want: a + ":1-2," + b + ":tg:1-2"},
		{name: "truncated interval", data: classic(src(uuid(0xaa), "-", ivs(1, 11)))[:40], err: "intervals"},
		{name: "trailing bytes", data: append(classic(src(uuid(0xaa), "-", ivs(1, 11))), 0), err: "after the set"},
		{name: "empty interval", data: classic(src(uuid(0xaa), "-", ivs(5, 5))), err: "bad interval"},
		{name: "too short", data: []byte{1, 2}, err: "too short"},
		{name: "bad tag length", data: append(taggedHeader(1), append(uuid(0xaa), 0x81)...), err: "tag length"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodePreviousGTIDs(tc.data)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %q, %v; want error %q", got, err, tc.err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
			if _, err := pitrplan.ParseSet(got); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// F078: go-mysql's decoders index their input without bounds checks; a
// short event, or any panic in them, is ArchiveObjectInvalid, never a crash.
func TestInspectTransactionsDecoderPanics(t *testing.T) {
	events := splitEvents(txFixture())
	shortFDE := txEvent(replication.FORMAT_DESCRIPTION_EVENT, []byte{4, 0})
	if tok, msg := refusalToken(t, joinEvents([][]byte{shortFDE})); tok != pitrarchive.TokenArchiveObjectInvalid {
		t.Fatalf("short FORMAT_DESCRIPTION: %s %s", tok, msg)
	}
	shortGTID := txEvent(replication.GTID_EVENT, []byte{1})
	if tok, msg := refusalToken(t, joinEvents(append(append([][]byte{}, events[:2]...), shortGTID))); tok != pitrarchive.TokenArchiveObjectInvalid {
		t.Fatalf("short GTID: %s %s", tok, msg)
	}
	tagged := txEvent(replication.GTID_TAGGED_LOG_EVENT, []byte{0xff})
	if tok, msg := refusalToken(t, joinEvents(append(append([][]byte{}, events[:2]...), tagged))); tok != pitrarchive.TokenArchiveObjectInvalid {
		t.Fatalf("garbage tagged GTID: %s %s", tok, msg)
	}
	old := txIndexParse
	defer func() { txIndexParse = old }()
	txIndexParse = func(*replication.BinlogParser, []byte) (*replication.BinlogEvent, error) { panic("index out of range") }
	if tok, msg := refusalToken(t, txFixture()); tok != pitrarchive.TokenArchiveObjectInvalid || !strings.Contains(msg, "index out of range") {
		t.Fatalf("decoder panic: %s %s", tok, msg)
	}
}

// F081: the decoder bound's remedy says whether more memory raises it.
func TestInspectTransactionsBoundRemedy(t *testing.T) {
	events := splitEvents(txFixture())
	for _, tc := range []struct {
		budget int64
		want   string
	}{
		{budget: 8 << 20, want: "raise the Job's memory limit"},
		{budget: 1 << 30, want: "fixed 64 MiB bound, which more memory does not raise"},
	} {
		budget := pitrplan.NewBudget(tc.budget)
		huge := make([]byte, 19)
		copy(huge, events[3])
		binary.LittleEndian.PutUint32(huge[9:], uint32(budget.MaxEventBytes()+1))
		_, err := InspectTransactions(bytes.NewReader(joinEvents(append(append([][]byte{}, events[:3]...), huge))), TxIndexOptions{Label: "f", Budget: budget})
		if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, tc.want) {
			t.Errorf("budget %d: %v, want %q", tc.budget, err, tc.want)
		}
		payload := payloadEvent(uint64(budget.MaxEventBytes())+1, zstdFrame(t, []byte("x")))
		_, err = InspectTransactions(bytes.NewReader(joinEvents(append(append([][]byte{}, events[:3]...), payload))), TxIndexOptions{Label: "f", Budget: budget})
		if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, tc.want) {
			t.Errorf("budget %d payload: %v, want %q", tc.budget, err, tc.want)
		}
	}
}

// F084: the payload parser's integrity and framing guards.
func TestInspectTransactionsPayloadGuards(t *testing.T) {
	begin := innerEvent(replication.QUERY_EVENT, queryEventBody("BEGIN"))
	insert := innerEvent(replication.QUERY_EVENT, queryEventBody("INSERT INTO t VALUES (1)"))
	xid := innerEvent(replication.XID_EVENT, xidBody())
	inner := func(events ...[]byte) []byte { return bytes.Join(events, nil) }
	good := inner(begin, insert, xid)
	// rawPayload builds a payload event from a TLV header and its data.
	rawPayload := func(header []byte, data []byte) []byte {
		return txEvent(replication.TRANSACTION_PAYLOAD_EVENT, append(append([]byte(nil), header...), data...))
	}
	frame := zstdFrame(t, good)
	tlv := func(fields ...[3]uint64) []byte { // {field, length, value}
		var out []byte
		for _, f := range fields {
			out = append(out, byte(f[0]), byte(f[1]))
			if f[1] == 1 {
				out = append(out, byte(f[2]))
			} else {
				out = append(out, 0xfc, byte(f[2]), byte(f[2]>>8))
			}
		}
		return append(out, 0)
	}
	size := func(n int) [3]uint64 {
		if n < 251 {
			return [3]uint64{1, 1, uint64(n)}
		}
		return [3]uint64{1, 3, uint64(n)}
	}
	usize := func(n int) [3]uint64 {
		if n < 251 {
			return [3]uint64{3, 1, uint64(n)}
		}
		return [3]uint64{3, 3, uint64(n)}
	}
	badCRC := payloadEvent(uint64(len(good)), frame)
	badCRC[len(badCRC)-8] ^= 0xff
	for _, tc := range []struct {
		name   string
		event  []byte
		want   pitrarchive.Token
		detail string
	}{
		{name: "valid", event: payloadEvent(uint64(len(good)), frame)},
		{name: "bad CRC", event: badCRC, want: pitrarchive.TokenArchiveObjectInvalid, detail: "fails its CRC32"},
		{name: "unknown field", event: rawPayload(tlv(size(len(frame)), [3]uint64{2, 1, 0}, [3]uint64{7, 1, 0}), frame), want: pitrarchive.TokenArchiveObjectInvalid, detail: "unknown or duplicate field 7"},
		{name: "duplicate field", event: rawPayload(tlv(size(len(frame)), size(len(frame))), frame), want: pitrarchive.TokenArchiveObjectInvalid, detail: "unknown or duplicate field 1"},
		{name: "incomplete header", event: rawPayload(tlv(size(len(frame)), [3]uint64{2, 1, 0}), frame), want: pitrarchive.TokenArchiveObjectInvalid, detail: "incomplete"},
		{name: "truncated header", event: rawPayload([]byte{1, 9, 1}, nil), want: pitrarchive.TokenArchiveObjectInvalid, detail: "truncated"},
		{name: "size disagrees", event: rawPayload(tlv(size(len(frame)+1), [3]uint64{2, 1, 0}, usize(len(good))), frame), want: pitrarchive.TokenArchiveObjectInvalid, detail: "unsupported compression"},
		{name: "nested GTID", event: payloadEvent(uint64(len(good)+19+25), zstdFrame(t, inner(begin, innerEvent(replication.GTID_EVENT, make([]byte, 25)), insert, xid))), want: pitrarchive.TokenArchiveObjectInvalid, detail: "holds a GTIDEvent"},
		{name: "nested payload", event: payloadEvent(uint64(len(good)+19+4), zstdFrame(t, inner(begin, innerEvent(replication.TRANSACTION_PAYLOAD_EVENT, make([]byte, 4)), insert, xid))), want: pitrarchive.TokenArchiveObjectInvalid, detail: "holds a"},
		{name: "truncated inner query", event: payloadEvent(uint64(19+5), zstdFrame(t, innerEvent(replication.QUERY_EVENT, make([]byte, 5)))), want: pitrarchive.TokenArchiveObjectInvalid, detail: "short query event"},
		{name: "inner event overruns", event: payloadEvent(uint64(len(good)-1), zstdFrame(t, good[:len(good)-1])), want: pitrarchive.TokenArchiveObjectInvalid, detail: "inner event of"},
		{name: "inner header truncated", event: payloadEvent(uint64(len(good)+5), zstdFrame(t, append(append([]byte(nil), good...), 1, 2, 3, 4, 5))), want: pitrarchive.TokenArchiveObjectInvalid, detail: "truncated inner event"},
		{name: "unterminated", event: payloadEvent(uint64(len(begin)+len(insert)), zstdFrame(t, inner(begin, insert))), want: pitrarchive.TokenArchiveObjectInvalid, detail: "ends inside transaction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tok, msg := refusalToken(t, txFile(tc.event))
			if tok != tc.want || !strings.Contains(msg, tc.detail) {
				t.Fatalf("got %s %q, want %s %q", tok, msg, tc.want, tc.detail)
			}
		})
	}
	// A payload outside a transaction, or inside an uncompressed one.
	ev := payloadEvent(uint64(len(good)), frame)
	if tok, _ := refusalToken(t, joinEvents(append(splitEvents(txFixture())[:2], ev))); tok != pitrarchive.TokenReplayUnsupported {
		t.Fatalf("payload outside a transaction: %s", tok)
	}
	if tok, msg := refusalToken(t, txFile(txEvent(replication.QUERY_EVENT, queryEventBody("BEGIN")), ev)); tok != pitrarchive.TokenArchiveObjectInvalid || !strings.Contains(msg, "inside the uncompressed") {
		t.Fatalf("payload inside a transaction: %s %s", tok, msg)
	}
}

// F085: decompression is bounded by the declared size, with valid frames
// whose output is larger or smaller than declared.
func TestDecompressPayloadBounds(t *testing.T) {
	data := bytes.Repeat([]byte("binlog payload "), 20) // 300 bytes
	framed, stream := zstdFrame(t, data), zstdStream(t, data, 1<<20)
	for _, tc := range []struct {
		name  string
		src   []byte
		limit int64
		err   string
	}{
		{name: "exact", src: framed, limit: int64(len(data))},
		{name: "exact streaming", src: stream, limit: int64(len(data))},
		{name: "content size disagrees", src: framed, limit: int64(len(data)) - 1, err: "zstd frame declares 300 bytes, the payload header 299"},
		{name: "streaming past the declared size", src: stream, limit: int64(len(data)) - 1, err: "decompresses past its declared 299 bytes"},
		{name: "streaming short of the declared size", src: stream, limit: int64(len(data)) + 1, err: "decompresses to 300 bytes, header says 301"},
		{name: "window above the cap", src: zstdStream(t, data, 16<<20), limit: int64(len(data)), err: "window"},
		{name: "not zstd", src: []byte("not zstd"), limit: 8, err: "zstd frame header"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := decompressPayload(tc.src, tc.limit)
			if tc.err == "" {
				if err != nil || !bytes.Equal(out, data) {
					t.Fatalf("%v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("got %v, want %q", err, tc.err)
			}
		})
	}
	// F083: MySQL writes single-segment frames (window = content size),
	// even at compression level 22, so a frame declaring a large window
	// is never a MySQL payload. The captured level-22 transaction in
	// tagged-only-previous.bin decodes (TestInspectTransactionsNativeStructures).
	big := zstdFrame(t, bytes.Repeat([]byte{7}, 9<<20))
	var h zstd.Header
	if err := h.Decode(big); err != nil || !h.SingleSegment || h.FrameContentSize != 9<<20 {
		t.Fatalf("single-segment frame header %+v: %v", h, err)
	}
	if out, err := decompressPayload(big, 9<<20); err != nil || len(out) != 9<<20 {
		t.Fatalf("9 MiB single-segment frame: %v", err)
	}
}

// F210: payloadDecodeBytes bounds what decompression allocates (an upper
// bound of its live heap): the output, the decoder's window and its state.
func TestDecompressPayloadHeapBound(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, n := range []int{1 << 10, 256 << 10, 4 << 20, 16 << 20, 64 << 20} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(rng.Intn(16)) // compressible, not trivially
		}
		frame := zstdFrame(t, data)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		out, err := decompressPayload(frame, int64(n))
		runtime.ReadMemStats(&after)
		if err != nil || len(out) != n {
			t.Fatal(err)
		}
		if alloc := int64(after.TotalAlloc - before.TotalAlloc); alloc > payloadDecodeBytes(int64(n)) {
			t.Errorf("%d-byte payload allocated %d bytes, charged %d", n, alloc, payloadDecodeBytes(int64(n)))

		}
	}
}

// txCutStore serves objects whose body ends early with a transport
// error after n bytes.
type txCutStore struct {
	*memStore
	n int
}

func (s *txCutStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.memStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{io.MultiReader(io.LimitReader(r, int64(s.n)), txErrReader{fmt.Errorf("s3 body: %w", io.ErrUnexpectedEOF)}), r}, nil
}

type txErrReader struct{ err error }

func (e txErrReader) Read([]byte) (int, error) { return 0, e.err }

// F082, F209: a body cut short by the transport is not an integrity
// failure even though decryption reports it as truncated ciphertext; bytes
// appended after an aligned final chunk, read in full, are.
func TestArchiveIntegrityErrorClassification(t *testing.T) {
	pass := []byte("archive passphrase")
	plain := bytes.Repeat([]byte{0x5a}, 2<<16) // two full 64 KiB chunks
	var sealed bytes.Buffer
	if _, err := backupcrypto.Encrypt(&sealed, bytes.NewReader(plain), pass); err != nil {
		t.Fatal(err)
	}
	read := func(store ArchiveStore, key string) error {
		r, err := store.Open(context.Background(), key)
		if err != nil {
			return err
		}
		defer r.Close()
		_, err = io.Copy(io.Discard, r)
		return err
	}
	inner := newMemStore()
	_ = inner.Put(context.Background(), "whole", bytes.NewReader(sealed.Bytes()), 0)
	_ = inner.Put(context.Background(), "appended", bytes.NewReader(append(append([]byte(nil), sealed.Bytes()...), 0)), 0)
	tampered := append([]byte(nil), sealed.Bytes()...)
	tampered[len(tampered)/2] ^= 1
	_ = inner.Put(context.Background(), "tampered", bytes.NewReader(tampered), 0)
	store := WrapWithEncryption(inner, pass)
	if err := read(store, "whole"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"appended", "tampered"} {
		if err := read(store, key); err == nil || !IsArchiveIntegrityError(err) {
			t.Errorf("%s: %v is not an integrity error", key, err)
		}
	}
	for _, cut := range []int{10, sealed.Len() / 2, sealed.Len() - 1} {
		err := read(WrapWithEncryption(&txCutStore{memStore: inner, n: cut}, pass), "whole")
		if err == nil || IsArchiveIntegrityError(err) {
			t.Errorf("transport cut at %d: %v classified as integrity", cut, err)
		}
		// The caller sees the transport's cause, not the decrypt symptom.
		if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, backupcrypto.ErrTruncated) {
			t.Errorf("transport cut at %d: %v does not carry the store's error", cut, err)
		}
	}
	if IsArchiveIntegrityError(backupcrypto.ErrTruncated) || !IsArchiveIntegrityError(ErrTamperedOrDowngrade) {
		t.Error("bare crypto errors come only from a failed transport")
	}
}

// F211, F212: PlanSite charges what it parses, and trusts only coverage
// bound to the row's content ID.
func TestPlanSiteChargedAndBound(t *testing.T) {
	row := func(i int, bound bool) ManifestEntry {
		id := fmt.Sprintf("%064x", i)
		cov := &BinlogGTIDCoverage{Complete: true, GTIDs: fmt.Sprintf("%s:%d", txA, i+2)}
		if bound {
			cov.ContentID = id
		}
		return ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i+1), ID: id, PreviousGTIDs: fmt.Sprintf("%s:1-%d", txA, i+1), GTIDCoverage: cov}
	}
	m := &Manifest{Version: ManifestVersion, Site: "a", Files: []ManifestEntry{row(1, true), row(2, false)}}
	site, err := PlanSiteCharged(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !site.Rows[0].Hints.Known || site.Rows[1].Hints.Known {
		t.Fatalf("hints known: bound %t, unbound %t", site.Rows[0].Hints.Known, site.Rows[1].Hints.Known)
	}
	many := &Manifest{Version: ManifestVersion, Site: "a"}
	for i := 0; i < 5000; i++ {
		many.Files = append(many.Files, row(i, true))
	}
	budget := pitrplan.NewBudget(1 << 20)
	if _, err := PlanSiteCharged(many, budget); err == nil {
		t.Fatal("5000 rows fit a 1 MiB budget")
	} else if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("many rows: %v", err)
	}
	budget = pitrplan.NewBudget(64 << 20)
	if _, err := PlanSiteCharged(many, budget); err != nil || budget.Used() < 5000*planSiteRowBytes {
		t.Fatalf("charged %d for 5000 rows: %v", budget.Used(), err)
	}
}

// F079: the manifest writer paths (InspectBinlog for uploads, the scan's
// header read) decode Previous_gtids with the same decoder as the index
// pass, so a tagged source whose UUID has no untagged GTIDs keeps its UUID.
func TestManifestPreviousGTIDsMatchIndexPass(t *testing.T) {
	const path = "testdata/native/txindex/tagged-only-previous.bin"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := InspectTransactions(bytes.NewReader(data), TxIndexOptions{Label: "tagged-only-previous"})
	if err != nil {
		t.Fatal(err)
	}
	want := idx.Previous.String()
	if !strings.Contains(want, "cccccccc-cccc-cccc-cccc-cccccccccccc:tg:1-2") {
		t.Fatalf("index pass previous %s lacks the tagged-only source", want)
	}
	ev, err := InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	header, err := localBinlogHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"upload metadata": ev.Metadata.PreviousGTIDs, "scan header": header.previous} {
		got, err := pitrplan.ParseSet(text)
		if err != nil {
			t.Fatalf("%s previous %q: %v", name, text, err)
		}
		if got.String() != want {
			t.Errorf("%s previous %s, index pass %s", name, got.String(), want)
		}
	}
}
