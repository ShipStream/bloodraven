package sidecar

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type retentionReviewStore struct {
	ArchiveStore
	reads int
}

func (s *retentionReviewStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.reads++
	return s.ArchiveStore.Open(ctx, key)
}

func retentionReviewArchiver(t *testing.T, store ArchiveStore, logger *slog.Logger, response map[string]any) *BinlogArchiver {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(response) }))
	t.Cleanup(server.Close)
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, logger)
	a.SetRetentionClient(strings.TrimPrefix(server.URL, "http://"), "ns", "fg", "p", time.Hour)
	return a
}
func retentionReviewProof() map[string]any {
	return map[string]any{"coverage": PITRGTIDCoverageProtocol, "retainedGTIDs": []string{coverageTestSID + ":1-11"}, "cutoffTime": "2026-04-17T00:00:00Z"}
}
func TestReviewRetentionBindsExistingProofToArchivedBytes(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, layout := range []string{"ID", "legacy", "verified legacy"} {
			t.Run(fmt.Sprintf("encrypted=%v/%s", encrypted, layout), func(t *testing.T) {
				ctx := context.Background()
				pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				var backend ArchiveStore = pvc
				if encrypted {
					backend = WrapWithEncryption(backend, []byte("random archive identity key"))
				}
				store := &retentionReviewStore{ArchiveStore: backend}
				original, err := fileContentID("testdata/incarnation-a.bin")
				if err != nil {
					t.Fatal(err)
				}
				original = store.BinlogContentID(original)
				data, err := os.ReadFile("testdata/incarnation-b.bin")
				if err != nil {
					t.Fatal(err)
				}
				row := ManifestEntry{Name: "mysql-bin.000001", LastEventTime: time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC), GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: coverageTestSID + ":11"}}
				if layout == "ID" {
					row.ID = original
				}
				if layout == "verified legacy" {
					row.ContentID = original
				}
				key, err := ArchiveEntryKey("binlogs", "iad", row)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
					t.Fatal(err)
				}
				if err := appendManifestEntry(ctx, store, "binlogs", "iad", row); err != nil {
					t.Fatal(err)
				}
				a := retentionReviewArchiver(t, store, slog.Default(), retentionReviewProof())
				a.maybeRunRetention(ctx)
				if _, exists, err := store.Get(ctx, key); err != nil || !exists {
					t.Fatalf("transaction 21 deleted using stale transaction-11 proof: %t, %v", exists, err)
				}
				m, err := loadManifest(ctx, store, "binlogs", "iad")
				if err != nil || len(m.Files) != 1 {
					t.Fatalf("unsafe retirement: %+v, %v", m, err)
				}
				if store.reads != 1 {
					t.Fatalf("proof must be rebound using one inspection, got %d", store.reads)
				}
				if layout == "legacy" {
					if m.Files[0].GTIDCoverage == nil || m.Files[0].GTIDCoverage.GTIDs != coverageTestSID+":21" || m.Files[0].ContentID == "" || m.Files[0].PreviousGTIDs != coverageTestSID+":1-20" {
						t.Fatalf("proof was not rebuilt from the archived bytes: %+v", m.Files[0])
					}
				}
			})
		}
	}
}
func TestReviewRetentionBackfillIsBoundedAndPersistent(t *testing.T) {
	ctx := context.Background()
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	store := &retentionReviewStore{ArchiveStore: pvc}
	payload, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Site: "iad"}
	for i := 0; i < 16; i++ {
		entry := ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i), Size: int64(len(payload)), LastEventTime: time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)}
		key, err := ArchiveEntryKey("binlogs", "iad", entry)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, entry)
	}
	if err := saveManifest(ctx, store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	create := func() *BinlogArchiver {
		return retentionReviewArchiver(t, store, slog.Default(), retentionReviewProof())
	}
	create().maybeRunRetention(ctx)
	if store.reads == 0 || store.reads > 8 {
		t.Fatalf("one sweep read %d objects, want a bounded batch <=8", store.reads)
	}
	persisted, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	verified := 0
	for _, entry := range persisted.Files {
		if entry.ContentID != "" {
			verified++
		}
	}
	if verified != store.reads {
		t.Fatalf("coverage and content IDs not persisted together: verified=%d reads=%d", verified, store.reads)
	}
	for batches := 0; batches < 16 && store.reads < 16; batches++ {
		create().maybeRunRetention(ctx)
	}
	if store.reads != 16 {
		t.Fatalf("restart repeated work or failed to finish bounded batches: reads=%d", store.reads)
	}
	create().maybeRunRetention(ctx)
	if store.reads != 16 {
		t.Fatalf("verified proofs were inspected again: reads=%d", store.reads)
	}
}
func TestReviewRetentionLogsDeferralReasonAndLevel(t *testing.T) {
	for _, tc := range []struct{ reason, level string }{{"noRetainedBackups", "INFO"}, {"backupInFlight", "INFO"}, {"backupMetadataUnknown", "WARN"}} {
		t.Run(tc.reason, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			store := newMemStore()
			a := retentionReviewArchiver(t, store, logger, map[string]any{"coverage": PITRGTIDCoverageProtocol, "retentionDeferredReason": tc.reason})
			a.maybeRunRetention(context.Background())
			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &record); err != nil {
				t.Fatal(err)
			}
			if record["msg"] != "retention deferred" || record["reason"] != tc.reason || record["level"] != tc.level {
				t.Fatalf("wrong deferral log: %s", logs.Bytes())
			}
		})
	}
}
func TestReviewEmptyAndCompressedBinlogsHaveCompleteCoverage(t *testing.T) {
	for _, tc := range []struct{ file, gtids string }{{"empty-sealed.bin", ""}, {"compressed-transaction.bin", coverageTestSID + ":11"}} {
		t.Run(tc.file, func(t *testing.T) {
			meta, err := parseBinlogMetadata(filepath.Join("testdata", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			if meta.GTIDCoverage == nil || !meta.GTIDCoverage.Complete || meta.GTIDCoverage.GTIDs != tc.gtids {
				t.Fatalf("valid sealed file cannot retire: %+v", meta.GTIDCoverage)
			}
			proof := retentionReviewProof()
			a := retentionReviewArchiver(t, newMemStore(), slog.Default(), proof)
			parsed, err := a.fetchRetentionCutoff(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !fileCoveredByBackups(meta.GTIDCoverage, parsed.backups) {
				t.Fatal("complete coverage is not recognized")
			}
			ctx := context.Background()
			file := filepath.Join("testdata", tc.file)
			id, err := a.localBinlogID(file)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.archiveOne(ctx, file, "mysql-bin.000001", id); err != nil {
				t.Fatal(err)
			}
			a.maybeRunRetention(ctx)
			m, err := loadManifest(ctx, a.store, "binlogs", "iad")
			if err != nil || len(m.Files) != 0 {
				t.Fatalf("complete empty/compressed file was not retired: %+v, %v", m, err)
			}
		})
	}
}

func TestReviewRetentionRetriesThrottledLookupSoon(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTooManyRequests) }))
	defer server.Close()
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, newMemStore(), slog.Default())
	a.SetRetentionClient(strings.TrimPrefix(server.URL, "http://"), "ns", "fg", "p", time.Hour)
	a.maybeRunRetention(context.Background())
	next := a.lastRetentionRun.Add(a.retentionCfg.Interval)
	if remaining := time.Until(next); remaining > 30*time.Second || remaining <= 0 {
		t.Fatalf("throttling postpones the next site for an hour: %v", remaining)
	}
}

func TestReviewCompressedPayloadRequiresImmediateGTID(t *testing.T) {
	original, err := os.ReadFile("testdata/compressed-transaction.bin")
	if err != nil {
		t.Fatal(err)
	}
	data := append([]byte{}, original[:4]...)
	for offset := 4; offset < len(original); {
		size := int(binary.LittleEndian.Uint32(original[offset+9 : offset+13]))
		event := bytes.Clone(original[offset : offset+size])
		offset += size
		if event[4] == 33 {
			continue
		}
		binary.LittleEndian.PutUint32(event[13:17], uint32(len(data)+size))
		binary.LittleEndian.PutUint32(event[size-4:], crc32.ChecksumIEEE(event[:size-4]))
		data = append(data, event...)
	}
	file := filepath.Join(t.TempDir(), "mysql-bin.000001")
	if err := os.WriteFile(file, data, 0600); err != nil {
		t.Fatal(err)
	}
	meta, err := parseBinlogMetadata(file)
	if err != nil {
		t.Fatal(err)
	}
	if meta.GTIDCoverage == nil || meta.GTIDCoverage.Complete {
		t.Fatalf("orphan payload received complete coverage: %+v", meta.GTIDCoverage)
	}
}
