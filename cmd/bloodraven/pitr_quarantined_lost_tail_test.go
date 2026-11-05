package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

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

// Site A archived A:1-11, then a lost tail L1/L2 (A:12-31, never replicated).
// After failover to B, A was recloned and later uploaded R1, whose P is the
// donor set A:1-11,B:1-2. The operator follows the repair guide: quarantine
// both abandoned rows behind the canonical post-failover backup A:1-11,B:1-2.
// A restore from exactly that backup to a time inside R1 does not need the
// quarantined rows at all.
func TestPITRQuarantinedLostTailDoesNotBlockCanonicalWindow(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir(), MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC)
	f1 := putLostTailBinlog(t, store, "mysql-bin.000001", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 10}}}, lostTailTxs(lostTailSIDA, 11, 11, day, time.Second)))
	l1 := putLostTailBinlog(t, store, "mysql-bin.000002", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 11}}}, lostTailTxs(lostTailSIDA, 12, 21, day.Add(10*time.Minute), time.Second)))
	l2 := putLostTailBinlog(t, store, "mysql-bin.000003", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 21}}}, lostTailTxs(lostTailSIDA, 22, 31, day.Add(20*time.Minute), time.Second)))
	r1 := putLostTailBinlog(t, store, "mysql-bin.000001", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 11}}, lostTailSIDB: {{1, 2}}}, lostTailTxs(lostTailSIDB, 3, 4, day.Add(time.Hour), 10*time.Minute)))
	saveArchiveManifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{f1, l1, l2, r1}})
	canonical := lostTailSIDA + ":1-11," + lostTailSIDB + ":1-2"
	for _, row := range []sidecar.ManifestEntry{l1, l2} {
		if err := sidecar.QuarantineArchive(ctx, store, "binlogs", "iad", row.Name, row.ID, "abandoned lost tail after failover", canonical, false); err != nil {
			t.Fatal(err)
		}
	}
	stop := day.Add(time.Hour + 5*time.Minute) // between B:3 and B:4 in R1
	n, err := downloadPITRWindow(ctx, store, "binlogs", "iad", stop, t.TempDir(), canonical, slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("restore from the canonical quarantine bound to a time inside R1 failed: n=%d err=%v", n, err)
	}
}

// Same history restored to "latest": no stop barrier, so the excluded rows are
// skipped before they are compared with any barrier.
func TestPITRQuarantinedLostTailLatestWindow(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir(), MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC)
	f1 := putLostTailBinlog(t, store, "mysql-bin.000001", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 10}}}, lostTailTxs(lostTailSIDA, 11, 11, day, time.Second)))
	l1 := putLostTailBinlog(t, store, "mysql-bin.000002", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 11}}}, lostTailTxs(lostTailSIDA, 12, 21, day.Add(10*time.Minute), time.Second)))
	l2 := putLostTailBinlog(t, store, "mysql-bin.000003", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 21}}}, lostTailTxs(lostTailSIDA, 22, 31, day.Add(20*time.Minute), time.Second)))
	r1 := putLostTailBinlog(t, store, "mysql-bin.000001", lostTailBinlog(t, map[string][]lostTailInterval{lostTailSIDA: {{1, 11}}, lostTailSIDB: {{1, 2}}}, lostTailTxs(lostTailSIDB, 3, 4, day.Add(time.Hour), 10*time.Minute)))
	saveArchiveManifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{f1, l1, l2, r1}})
	canonical := lostTailSIDA + ":1-11," + lostTailSIDB + ":1-2"
	for _, row := range []sidecar.ManifestEntry{l1, l2} {
		if err := sidecar.QuarantineArchive(ctx, store, "binlogs", "iad", row.Name, row.ID, "abandoned lost tail after failover", canonical, false); err != nil {
			t.Fatal(err)
		}
	}
	n, err := downloadPITRWindow(ctx, store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), t.TempDir(), canonical, slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("latest restore: n=%d err=%v", n, err)
	}
}

// Mixed-version shape at scale: every ID-aware row also has an unverified
// legacy alias (no contentId), and each late alias discovery calls
// resetPrefix(), which re-queues the whole validated prefix. Planning must
// stay roughly linear.
func TestPITRLateAliasDiscoveryPlannerCost(t *testing.T) {
	run := func(n int) time.Duration {
		store := buildPlannerChain(t, n)
		var m sidecar.Manifest
		if err := json.Unmarshal(store.manifest, &m); err != nil {
			t.Fatal(err)
		}
		ids := append([]sidecar.ManifestEntry(nil), m.Files...)
		for _, row := range ids {
			key, _ := sidecar.ArchiveEntryKey("binlogs", "iad", row)
			legacy := sidecar.ManifestEntry{Name: row.Name, PreviousGTIDs: row.PreviousGTIDs}
			lkey, _ := sidecar.ArchiveEntryKey("binlogs", "iad", legacy)
			store.files[lkey] = store.files[key]
			m.Files = append(m.Files, legacy)
		}
		store.manifest, _ = json.Marshal(m)
		at := time.Now()
		got, err := downloadPITRWindow(context.Background(), store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), t.TempDir(), "", slog.New(slog.NewTextHandler(io.Discard, nil)))
		elapsed := time.Since(at)
		if err != nil || got != n {
			t.Fatalf("n=%d got=%d err=%v", n, got, err)
		}
		t.Logf("n=%d (+%d legacy aliases) planning+staging=%v", n, n, elapsed)
		return elapsed
	}
	small := run(250)
	large := run(1000)
	big := run(4000)
	if big > max(4*time.Second, small*16*6) {
		t.Fatalf("late alias discovery made planning superlinear: 250=%v 1000=%v 4000=%v", small, large, big)
	}
}
