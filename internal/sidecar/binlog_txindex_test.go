package sidecar

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

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
	// A payload that decompresses past its declared size is refused too.
	_, err = InspectTransactions(bytes.NewReader(joinEvents(append(append([][]byte{}, events[:3]...), payloadEvent(10, []byte("not zstd"))))), TxIndexOptions{Label: "f", Budget: budget})
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenArchiveObjectInvalid {
		t.Fatalf("bad payload: %v", err)
	}
	// Only the indexed transactions stay charged; event and payload
	// charges are transient.
	if budget.Used() > 3*(pitrplan.TxnBytes+64) { // + each Previous_gtids set
		t.Errorf("transient charges leaked: %d bytes", budget.Used())
	}
}
