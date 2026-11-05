package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/klauspost/compress/zstd"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// Binlog and archive fixture builders kept from the per-site planner's
// tests (#206 series 1 deleted that planner; series 2's planner tests reuse
// these). They build real binlog bytes, so they do not depend on the planner.

func inspectBinlogFixture(t *testing.T, raw []byte) sidecar.BinlogEvidence {
	t.Helper()
	evidence, err := sidecar.InspectBinlog(bytes.NewReader(raw), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func idleRotationAfter(t *testing.T, raw []byte, at time.Time) []byte {
	t.Helper()
	evidence := inspectBinlogFixture(t, raw)
	previous, err := internalmysql.ParseGTIDSet(evidence.Metadata.PreviousGTIDs)
	if err != nil {
		t.Fatal(err)
	}
	executed := sidecar.UnionBinlogGTIDs(previous, evidence.GTIDs)
	if len(executed) != 1 {
		t.Fatal("idle fixture expects one source UUID")
	}
	body := binary.LittleEndian.AppendUint64(nil, 1)
	for sid, intervals := range executed {
		uuid, err := hex.DecodeString(strings.ReplaceAll(sid, "-", ""))
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, uuid...)
		body = binary.LittleEndian.AppendUint64(body, uint64(len(intervals)))
		for _, interval := range intervals {
			body = binary.LittleEndian.AppendUint64(body, uint64(interval.Start))
			body = binary.LittleEndian.AppendUint64(body, uint64(interval.End)+1)
		}
	}
	events := splitBinlogEvents(coverageStream(t, 0, at, true, false))

	server := splitBinlogEvents(raw)[0][5:9]
	for i, event := range events {
		if event[4] == 35 {
			event = append(bytes.Clone(event[:19]), body...)
			events[i] = append(event, make([]byte, 4)...)
		}
		copy(events[i][5:9], server)
	}
	return reframeBinlogEvents(events)
}

func retagGTIDFixture(t *testing.T, raw []byte, number uint64) []byte {
	t.Helper()
	raw = bytes.Clone(raw)
	sid, _ := hex.DecodeString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		if raw[off+4] == 33 {
			copy(raw[off+20:off+36], sid)
			binary.LittleEndian.PutUint64(raw[off+36:], number)
			binary.LittleEndian.PutUint32(raw[off+size-4:off+size], crc32.ChecksumIEEE(raw[off:off+size-4]))
		}
		off += size
	}
	return raw
}

func joinBinlogFixtures(t *testing.T, files ...[]byte) []byte {
	t.Helper()
	out := []byte("\xfebin")
	for i, raw := range files {
		for off := 4; off < len(raw); {
			size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
			event := bytes.Clone(raw[off : off+size])
			off += size
			kind := event[4]
			if (i > 0 && (kind == 15 || kind == 35)) || (i < len(files)-1 && kind == 3) {
				continue
			}
			binary.LittleEndian.PutUint32(event[13:17], uint32(len(out)+size))
			binary.LittleEndian.PutUint32(event[size-4:], crc32.ChecksumIEEE(event[:size-4]))
			out = append(out, event...)
		}
	}
	return out
}

func coverageTime(hour, minute, second int) time.Time {
	return time.Date(2026, 4, 15, hour, minute, second, 0, time.UTC)
}

// Preserve native framing and CRCs while varying history, clocks, and idle files.
func coverageStream(t *testing.T, previous uint64, at time.Time, idle, foreign bool) []byte {
	t.Helper()
	raw := pitrFixture(t, "incarnation-a.bin")
	result := []byte("\xfebin")
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		event := bytes.Clone(raw[off : off+size])
		kind := event[4]
		off += size
		if idle && (kind == 33 || kind == 2) {
			continue
		}
		ts := at
		if kind == 33 || kind == 2 {
			ts = ts.Add(time.Second)
		}
		if kind == 3 {
			ts = ts.Add(2 * time.Second)
		}
		if kind != 35 {
			binary.LittleEndian.PutUint32(event[:4], uint32(ts.Unix()))
		}
		if kind == 35 {
			binary.LittleEndian.PutUint64(event[19+40:], previous+1)
		}
		if kind == 33 {
			binary.LittleEndian.PutUint64(event[19+17:], previous+1)
		}
		if foreign && (kind == 35 || kind == 33) {
			sid, _ := hex.DecodeString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			offset := 19 + 8
			if kind == 33 {
				offset = 19 + 1
			}
			copy(event[offset:offset+16], sid)
		}
		binary.LittleEndian.PutUint32(event[13:17], uint32(len(result)+size))
		binary.LittleEndian.PutUint32(event[size-4:], crc32.ChecksumIEEE(event[:size-4]))
		result = append(result, event...)
	}
	return result
}

func patchEventTimestamp(t *testing.T, raw []byte, kind byte, ts uint32) []byte {
	t.Helper()
	raw = bytes.Clone(raw)
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		if raw[off+4] == kind {
			binary.LittleEndian.PutUint32(raw[off:off+4], ts)
			binary.LittleEndian.PutUint32(raw[off+size-4:off+size], crc32.ChecksumIEEE(raw[off:off+size-4]))
		}
		off += size
	}
	return raw
}

func coverageArchive(t *testing.T, streams map[string][][]byte) sidecar.ArchiveStore {
	t.Helper()
	store, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir(), MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	for site, files := range streams {
		m := sidecar.Manifest{Site: site}
		for i, data := range files {
			e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			row := sidecar.ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i+1), ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs}
			key, err := sidecar.ArchiveEntryKey("binlogs", site, row)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatal(err)
			}
			m.Files = append(m.Files, row)
		}
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err = store.Put(context.Background(), sidecar.ManifestKey("binlogs", site), bytes.NewReader(raw), int64(len(raw))); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func coveragePutSite(t *testing.T, store sidecar.ArchiveStore, site string, data []byte) sidecar.BinlogEvidence {
	t.Helper()
	e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	row := sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs}
	key, err := sidecar.ArchiveEntryKey("binlogs", site, row)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(sidecar.Manifest{Site: site, Files: []sidecar.ManifestEntry{row}})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Put(context.Background(), sidecar.ManifestKey("binlogs", site), bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
	return e
}

type coverageListErrorStore struct {
	sidecar.ArchiveStore
	opened bool
}

func (s *coverageListErrorStore) List(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("access denied")
}

func (s *coverageListErrorStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.opened = true
	return s.ArchiveStore.Open(ctx, key)
}

func pitrFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("../../internal/sidecar/testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func pitrFixtureID(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// Rebuild native event positions and CRCs after inserting or replacing queries.
func reframeBinlogEvents(events [][]byte) []byte {
	out := []byte("\xfebin")
	for _, event := range events {
		event = bytes.Clone(event)
		binary.LittleEndian.PutUint32(event[9:13], uint32(len(event)))
		binary.LittleEndian.PutUint32(event[13:17], uint32(len(out)+len(event)))
		binary.LittleEndian.PutUint32(event[len(event)-4:], crc32.ChecksumIEEE(event[:len(event)-4]))
		out = append(out, event...)
	}
	return out
}

func splitBinlogEvents(raw []byte) [][]byte {
	var events [][]byte
	for off := 4; off < len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
		events = append(events, bytes.Clone(raw[off:off+size]))
		off += size
	}
	return events
}

func queryEventSQL(event []byte) string {
	body := event[19 : len(event)-4]
	start := 13 + int(binary.LittleEndian.Uint16(body[11:13])) + int(body[8]) + 1
	return string(body[start:])
}

func setQueryEventSQL(event []byte, query string) []byte {
	old := queryEventSQL(event)
	out := bytes.Clone(event[:len(event)-4-len(old)])
	out = append(out, query...)
	return append(out, make([]byte, 4)...)
}

func insertSavepointFixture(t *testing.T, raw []byte) []byte {
	t.Helper()
	var events [][]byte
	inserted, xid := false, false
	for _, event := range splitBinlogEvents(raw) {
		events = append(events, event)
		if event[4] == 2 && queryEventSQL(event) == "BEGIN" {
			events = append(events, setQueryEventSQL(event, "SAVEPOINT `s1`"))
			inserted = true
		}
		xid = xid || event[4] == 16
	}
	if !inserted || !xid {
		t.Fatal("native fixture must contain BEGIN ... XID")
	}
	return reframeBinlogEvents(events)
}

func moveBinlogClock(raw []byte, at time.Time) []byte {
	events := splitBinlogEvents(raw)
	for _, event := range events {
		if binary.LittleEndian.Uint32(event[:4]) != 0 {
			binary.LittleEndian.PutUint32(event[:4], uint32(at.Unix()))
		}
	}
	return reframeBinlogEvents(events)
}

func compressNativeTransaction(t *testing.T, raw []byte) []byte {
	t.Helper()
	events := splitBinlogEvents(raw)
	begin, end := -1, -1
	for i, event := range events {
		if event[4] == 2 && queryEventSQL(event) == "BEGIN" {
			begin = i
		}
		if begin >= 0 && event[4] == 16 {
			end = i
			break
		}
	}
	if begin < 0 || end < 0 {
		t.Fatal("native transaction body absent")
	}
	var inner []byte
	for _, event := range events[begin : end+1] {
		event = bytes.Clone(event[:len(event)-4])
		binary.LittleEndian.PutUint32(event[9:13], uint32(len(event)))
		binary.LittleEndian.PutUint32(event[13:17], 0)
		inner = append(inner, event...)
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	compressed := encoder.EncodeAll(inner, nil)
	encoder.Close()
	body := []byte{}
	for _, field := range []struct {
		key   byte
		value uint64
	}{{1, uint64(len(compressed))}, {2, 0}, {3, uint64(len(inner))}} {
		value := mysql.PutLengthEncodedInt(field.value)
		body = append(body, field.key, byte(len(value)))
		body = append(body, value...)
	}
	body = append(body, 0)
	body = append(body, compressed...)
	payload := bytes.Clone(events[begin][:19])
	payload[4] = 40
	payload = append(payload, body...)
	payload = append(payload, make([]byte, 4)...)
	result := append([][]byte{}, events[:begin]...)
	result = append(result, payload)
	result = append(result, events[end+1:]...)
	return reframeBinlogEvents(result)
}

const lostTailSIDA = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

const lostTailSIDB = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"

type lostTailInterval struct{ start, end uint64 }

type lostTailTx struct {
	sid string
	gno uint64
	at  time.Time
}

func lostTailSIDBytes(t testing.TB, s string) []byte {
	raw, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(raw) != 16 {
		t.Fatalf("bad sid %q", s)
	}
	return raw
}

// lostTailBinlog mirrors testdata/generate_binlogs.py: FDE, Previous_gtids, one
// GTID+Query pair per transaction, Stop. Previous_gtids may name several sources.
func lostTailBinlog(t testing.TB, previous map[string][]lostTailInterval, txs []lostTailTx) []byte {
	t.Helper()
	out := []byte{0xfe, 'b', 'i', 'n'}
	event := func(kind byte, ts uint32, body []byte) {
		size := 19 + len(body) + 4
		h := make([]byte, 19)
		binary.LittleEndian.PutUint32(h[0:], ts)
		h[4] = kind
		binary.LittleEndian.PutUint32(h[5:], 1)
		binary.LittleEndian.PutUint32(h[9:], uint32(size))
		binary.LittleEndian.PutUint32(h[13:], uint32(len(out)+size))
		data := append(h, body...)
		out = append(out, data...)
		out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(data))
	}
	first := uint32(txs[0].at.Unix())
	fde := binary.LittleEndian.AppendUint16(nil, 4)
	ver := make([]byte, 50)
	copy(ver, "8.4.0-log")
	fde = append(fde, ver...)
	fde = binary.LittleEndian.AppendUint32(fde, first-3600)
	fde = append(fde, 19)
	lengths := make([]byte, 41)
	lengths[1] = 13
	fde = append(fde, lengths...)
	fde = append(fde, 1)
	event(15, first-3600, fde)
	var sids []string
	for sid := range previous {
		sids = append(sids, sid)
	}
	sort.Strings(sids)
	prev := binary.LittleEndian.AppendUint64(nil, uint64(len(sids)))
	for _, sid := range sids {
		prev = append(prev, lostTailSIDBytes(t, sid)...)
		prev = binary.LittleEndian.AppendUint64(prev, uint64(len(previous[sid])))
		for _, iv := range previous[sid] {
			prev = binary.LittleEndian.AppendUint64(prev, iv.start)
			prev = binary.LittleEndian.AppendUint64(prev, iv.end+1)
		}
	}
	event(35, 0, prev)
	var last uint32
	for _, tx := range txs {
		ts := uint32(tx.at.Unix())
		g := append([]byte{1}, lostTailSIDBytes(t, tx.sid)...)
		g = binary.LittleEndian.AppendUint64(g, tx.gno)
		g = append(g, 2)
		g = binary.LittleEndian.AppendUint64(g, 0)
		g = binary.LittleEndian.AppendUint64(g, 1)
		event(33, ts, g)
		q := binary.LittleEndian.AppendUint32(nil, 1)
		q = binary.LittleEndian.AppendUint32(q, 0)
		q = append(q, 6)
		q = binary.LittleEndian.AppendUint16(q, 0)
		q = binary.LittleEndian.AppendUint16(q, 0)
		q = append(q, []byte("orders\x00")...)
		q = append(q, []byte("INSERT INTO markers VALUES ('"+tx.sid[:1]+time.Unix(int64(ts), 0).UTC().Format("150405")+"')")...)
		event(2, ts, q)
		last = ts
	}
	event(3, last+1, nil)
	if _, err := sidecar.InspectBinlog(bytes.NewReader(out), time.Time{}); err != nil {
		t.Fatalf("generated binlog invalid: %v", err)
	}
	return out
}

func lostTailTxs(sid string, from, to uint64, at time.Time, step time.Duration) []lostTailTx {
	var txs []lostTailTx
	for g := from; g <= to; g++ {
		txs = append(txs, lostTailTx{sid: sid, gno: g, at: at})
		at = at.Add(step)
	}
	return txs
}

func putLostTailBinlog(t *testing.T, store sidecar.ArchiveStore, name string, data []byte) sidecar.ManifestEntry {
	t.Helper()
	e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	cov := e.Metadata.GTIDCoverage
	cov.ContentID = e.Digest
	row := sidecar.ManifestEntry{Name: name, ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs, FirstEventTime: e.Metadata.FirstEventTime, LastEventTime: e.Metadata.LastEventTime, Size: int64(len(data)), GTIDCoverage: cov}
	key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	return row
}

type plannerMemoryStore struct {
	sidecar.ArchiveStore
	manifest, payload []byte
}

func (s *plannerMemoryStore) Get(context.Context, string) ([]byte, bool, error) {
	return s.manifest, true, nil
}

func (s *plannerMemoryStore) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.payload)), nil
}

func (s *plannerMemoryStore) BinlogContentID(d string) string { return d }

type plannerChainStore struct {
	sidecar.ArchiveStore
	manifest []byte
	files    map[string][]byte
}

func (s *plannerChainStore) Get(context.Context, string) ([]byte, bool, error) {
	return s.manifest, true, nil
}

func (s *plannerChainStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.files[key])), nil
}

func (s *plannerChainStore) BinlogContentID(d string) string { return d }

func buildPlannerChain(t testing.TB, n int) *plannerChainStore {
	t.Helper()
	raw, err := os.ReadFile("../../internal/sidecar/testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	store := &plannerChainStore{files: make(map[string][]byte)}
	m := sidecar.Manifest{Site: "iad"}
	for i := 0; i < n; i++ {
		data := bytes.Clone(raw)
		for off := 4; off < len(data); {
			size := int(binary.LittleEndian.Uint32(data[off+9 : off+13]))
			if data[off+4] == 35 {
				binary.LittleEndian.PutUint64(data[off+19+8+16+8+8:], uint64(11+i))
			}
			if data[off+4] == 33 {
				binary.LittleEndian.PutUint64(data[off+19+17:], uint64(11+i))
			}
			binary.LittleEndian.PutUint32(data[off+size-4:off+size], crc32.ChecksumIEEE(data[off:off+size-4]))
			off += size
		}
		ev, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		row := sidecar.ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i+1), ID: ev.Digest, PreviousGTIDs: ev.Metadata.PreviousGTIDs}
		key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		store.files[key] = data
		m.Files = append(m.Files, row)
	}
	store.manifest, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func archiveBinlogFixtures(t *testing.T, payloads ...[]byte) (sidecar.ArchiveStore, []sidecar.ManifestEntry) {
	t.Helper()
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir(), MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	var rows []sidecar.ManifestEntry
	for i, data := range payloads {
		e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		row := sidecar.ManifestEntry{Name: []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"}[i], ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs, FirstEventTime: e.Metadata.FirstEventTime, Size: int64(len(data))}
		key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	saveArchiveManifest(t, store, sidecar.Manifest{Site: "iad", Files: rows})
	return store, rows
}

func saveArchiveManifest(t *testing.T, store sidecar.ArchiveStore, m sidecar.Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
}

type nativePITRCase struct {
	Rollback bool   `json:"rollback,omitempty"`
	Stop     string `json:"stop"`
	Exclude  string `json:"exclude,omitempty"`
	Prefix   int    `json:"prefix"`
	SHA256   string `json:"sha256"`
}

// Move one captured file's outer event clock forward, preserving event bodies
// and CRCs. Its successors now model a backward clock without changing GTIDs.
func nativeCasePayloads(t *testing.T, rows []sidecar.ManifestEntry, payloads [][]byte, rollback bool) ([]sidecar.ManifestEntry, [][]byte) {
	t.Helper()
	rows = append([]sidecar.ManifestEntry(nil), rows...)
	payloads = append([][]byte(nil), payloads...)
	if !rollback {
		return rows, payloads
	}
	data := bytes.Clone(payloads[1])
	for offset := 4; offset < len(data); {
		size := int(binary.LittleEndian.Uint32(data[offset+9 : offset+13]))
		stamp := binary.LittleEndian.Uint32(data[offset : offset+4])
		if stamp > 0 {
			binary.LittleEndian.PutUint32(data[offset:offset+4], stamp+60)
		}
		binary.LittleEndian.PutUint32(data[offset+size-4:offset+size], crc32.ChecksumIEEE(data[offset:offset+size-4]))
		offset += size
	}
	e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	rows[1].ID, rows[1].FirstEventTime = e.Digest, e.Metadata.FirstEventTime
	payloads[1] = data
	return rows, payloads
}

func nativePITRRows(t *testing.T) ([]sidecar.ManifestEntry, [][]byte, []string) {
	t.Helper()
	catalog, err := os.ReadFile("../../internal/sidecar/testdata/native/catalog.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var rows []sidecar.ManifestEntry
	var data [][]byte
	var labels []string
	for _, line := range strings.Split(strings.TrimSpace(string(catalog)), "\n") {
		fields := strings.Split(line, "\t")
		payload, err := os.ReadFile("../../internal/sidecar/testdata/native/" + fields[0] + ".bin")
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := sidecar.InspectBinlog(bytes.NewReader(payload), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, sidecar.ManifestEntry{Name: fields[1], ID: evidence.Digest, Size: evidence.Metadata.Size, PreviousGTIDs: evidence.Metadata.PreviousGTIDs, BaseGTIDs: func() string {
			if len(fields) > 2 {
				return strings.ReplaceAll(fields[2], "\\n", ",")
			}
			return ""
		}(), FirstEventTime: evidence.Metadata.FirstEventTime})
		data = append(data, payload)
		labels = append(labels, fields[0])
	}
	return rows, data, labels
}
