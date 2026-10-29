package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

type rereview3MemoryStore struct {
	sidecar.ArchiveStore
	manifest, payload []byte
}

func (s *rereview3MemoryStore) Get(context.Context, string) ([]byte, bool, error) {
	return s.manifest, true, nil
}
func (s *rereview3MemoryStore) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.payload)), nil
}
func (s *rereview3MemoryStore) BinlogContentID(d string) string { return d }
func BenchmarkRereview3WindowPlanning(b *testing.B) {
	data, err := os.ReadFile("../../internal/sidecar/testdata/empty-sealed.bin")
	if err != nil {
		b.Fatal(err)
	}
	ev, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		b.Fatal(err)
	}
	for _, n := range []int{100, 500, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			m := sidecar.Manifest{Site: "iad"}
			for i := 0; i < n; i++ {
				m.Files = append(m.Files, sidecar.ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i+1), ID: ev.Digest, PreviousGTIDs: ev.Metadata.PreviousGTIDs})
			}
			encoded, _ := json.Marshal(m)
			store := &rereview3MemoryStore{manifest: encoded, payload: data}
			root := b.TempDir()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := downloadPITRWindow(context.Background(), store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), root, "", logger)
				if err != nil || got != n {
					b.Fatalf("%d %v", got, err)
				}
			}
		})
	}
}

// A later empty rotation on the same predecessor frontier must not suppress
// the only transaction-bearing file when its clock is behind that rotation.
func TestRereview3EqualFrontierEmptyStopDoesNotDropEarlierTransaction(t *testing.T) {
	empty := pitrFixture(t, "empty-sealed.bin")
	tx := pitrFixture(t, "incarnation-b.bin")
	stop := time.Date(2026, 4, 15, 10, 30, 0, 0, time.UTC)
	// Model the old transaction file followed by a rebuilt empty file at the
	// same GTID frontier (the old tail was not on the canonical donor).
	for off := 4; off < len(empty); {
		size := int(binary.LittleEndian.Uint32(empty[off+9 : off+13]))
		binary.LittleEndian.PutUint32(empty[off:off+4], uint32(stop.Add(time.Hour).Unix()))
		if empty[off+4] == 35 {
			binary.LittleEndian.PutUint64(empty[off+19+8+16+8+8:], 21)
		}
		binary.LittleEndian.PutUint32(empty[off+size-4:off+size], crc32.ChecksumIEEE(empty[off:off+size-4]))
		off += size
	}
	store, rows := round2Archive(t, pitrFixture(t, "incarnation-a.bin"), tx, empty)
	// Both observed P sets are 1-20; transaction 21 exists only in the older file.
	t.Setenv("BLOODRAVEN_PITR_START_GTIDS", retentionTestSID+":1-10")
	out := t.TempDir()
	count, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", stop, out, slog.Default())
	if err != nil {
		if !strings.Contains(err.Error(), "ambiguous empty stop frontier") {
			t.Fatalf("unexpected refusal: %v", err)
		}
		t.Logf("safe refusal: %v", err)
		return
	}
	files, _ := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
	found := false
	for _, f := range files {
		data, _ := os.ReadFile(f)
		if bytes.Equal(data, tx) {
			found = true
		}
	}
	if !found {
		t.Fatalf("silently omitted archived transaction at %s: got %d files, older row=%+v", stop, count, rows[1])
	}
}

type rereview3ChainStore struct {
	sidecar.ArchiveStore
	manifest []byte
	files    map[string][]byte
}

func (s *rereview3ChainStore) Get(context.Context, string) ([]byte, bool, error) {
	return s.manifest, true, nil
}
func (s *rereview3ChainStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.files[key])), nil
}
func (s *rereview3ChainStore) BinlogContentID(d string) string { return d }
func BenchmarkRereview3ChainPlanning(b *testing.B) {
	raw, err := os.ReadFile("../../internal/sidecar/testdata/incarnation-a.bin")
	if err != nil {
		b.Fatal(err)
	}
	for _, n := range []int{100, 500, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			m := sidecar.Manifest{Site: "iad"}
			store := &rereview3ChainStore{files: make(map[string][]byte)}
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
					b.Fatal(err)
				}
				row := sidecar.ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i+1), ID: ev.Digest, PreviousGTIDs: ev.Metadata.PreviousGTIDs}
				key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
				if err != nil {
					b.Fatal(err)
				}
				store.files[key] = data
				m.Files = append(m.Files, row)
			}
			encoded, _ := json.Marshal(m)
			store.manifest = encoded
			root := b.TempDir()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := downloadPITRWindow(context.Background(), store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), root, "", logger)
				if err != nil || got != n {
					b.Fatalf("%d %v", got, err)
				}
			}
		})
	}
}

func rereview3BuildChain(t testing.TB, n int) *rereview3ChainStore {
	t.Helper()
	raw, err := os.ReadFile("../../internal/sidecar/testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	store := &rereview3ChainStore{files: make(map[string][]byte)}
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
func TestRereview3PlannerScalingIncludingLowerCut(t *testing.T) {
	run := func(n int, lower bool) time.Duration {
		store := rereview3BuildChain(t, n)
		root := t.TempDir()
		start := ""
		want := n
		if lower {
			start = fmt.Sprintf("%s:1-%d", retentionTestSID, 10+n/2)
			want = n / 2
		}
		at := time.Now()
		got, err := downloadPITRWindow(context.Background(), store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), root, start, slog.New(slog.NewTextHandler(io.Discard, nil)))
		elapsed := time.Since(at)
		if err != nil || got != want {
			t.Fatalf("n=%d lower=%v: %d %v", n, lower, got, err)
		}
		t.Logf("n=%d lower=%v planning+staging=%v", n, lower, elapsed)
		return elapsed
	}
	small := run(100, false)
	large := run(1000, false)
	if large > max(5*time.Second, small*40) {
		t.Fatalf("planner still scales cubically: 100=%v 1000=%v", small, large)
	}
	for _, lower := range []bool{false, true} {
		bigger := run(4000, lower)
		if bigger > max(8*time.Second, large*12) {
			t.Fatalf("4000-row planner scaling regressed: 1000=%v 4000=%v", large, bigger)
		}
	}
}

func TestRereview3AuditedDivergentLastRowMustNotReplay(t *testing.T) {
	store, rows := round2Archive(t, pitrFixture(t, "incarnation-b.bin"))
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: rows, Divergent: []sidecar.ArchiveDivergence{{Name: rows[0].Name, ID: rows[0].ID, MissingGTIDs: retentionTestSID + ":21"}}})
	n, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", time.Now(), t.TempDir(), slog.Default())
	if err == nil || n != 0 || !strings.Contains(err.Error(), "audited divergent") {
		t.Fatalf("silently replayed audited divergent tail: %d %v", n, err)
	}
}
func TestRereview3AcknowledgedHistoryPreservesOlderBackupWindows(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", root)
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "binlogs")
	t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "")
	cfg, err := pitrDownloadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	store, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	data := pitrFixture(t, "incarnation-a.bin")
	row := sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: pitrFixtureID(data)}
	key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{row}, Epoch: &sidecar.ArchiveEpoch{ResetReason: "obsolete false guard"}})
	args := []string{"--site=iad", "--acknowledge-history", "--reason=canonical backup verified", "--resume-gtids=" + retentionTestSID + ":1-11", "--confirm"}
	if err := pitrQuarantine(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if n, err := downloadPITRWindow(ctx, store, "binlogs", "iad", time.Now(), t.TempDir(), retentionTestSID+":1-10", slog.Default()); err != nil || n != 1 {
		t.Fatalf("verified acknowledgement blocked an older valid window: %d %v", n, err)
	}
	if n, err := downloadPITRWindow(ctx, store, "binlogs", "iad", time.Now(), t.TempDir(), retentionTestSID+":1-11", slog.Default()); err != nil || n != 0 {
		t.Fatalf("canonical recovery failed: %d %v", n, err)
	}
}
