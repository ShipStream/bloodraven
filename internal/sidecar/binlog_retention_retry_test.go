package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type retentionFaultStore struct {
	ArchiveStore
	deletes      map[string]int
	failDelete   map[string]int
	manifestPuts int
	failSaveAt   int
}

func (s *retentionFaultStore) Delete(ctx context.Context, key string) error {
	s.deletes[key]++
	if s.failDelete[key] > 0 {
		s.failDelete[key]--
		return errors.New("transient delete failure")
	}
	return s.ArchiveStore.Delete(ctx, key)
}
func (s *retentionFaultStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if key == ManifestKey("binlogs", "iad") {
		s.manifestPuts++
		if s.manifestPuts == s.failSaveAt {
			return errors.New("manifest write failed")
		}
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

type retentionPendingRow struct {
	Name           string    `json:"name"`
	ID             string    `json:"id"`
	ContentID      string    `json:"contentId"`
	Key            string    `json:"key"`
	Deleted        bool      `json:"deleted"`
	PreviousGTIDs  string    `json:"previousGtids"`
	FirstEventTime time.Time `json:"firstEventTime"`
}

func readRetentionPendingRows(t *testing.T, store ArchiveStore) []retentionPendingRow {
	t.Helper()
	data, ok, err := store.Get(context.Background(), ManifestKey("binlogs", "iad"))
	if err != nil || !ok {
		t.Fatalf("read manifest: %t, %v", ok, err)
	}
	var body struct {
		Pruned []retentionPendingRow `json:"pruned"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	return body.Pruned
}

func newRetentionRetryFixture(t *testing.T, legacy bool) (*retentionFaultStore, func() *BinlogArchiver, *atomic.Int32, []ManifestEntry, []string) {
	t.Helper()
	ctx := context.Background()
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	store := &retentionFaultStore{ArchiveStore: pvc, deletes: map[string]int{}, failDelete: map[string]int{}}
	var entries []ManifestEntry
	var keys []string
	for i, fixture := range []string{"incarnation-a.bin", "incarnation-b.bin"} {
		file := filepath.Join("testdata", fixture)
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := parseBinlogMetadata(file)
		if err != nil {
			t.Fatal(err)
		}
		id, err := fileContentID(file)
		if err != nil {
			t.Fatal(err)
		}
		meta.GTIDCoverage.ContentID = id
		if legacy && i == 0 {
			id = ""
		}
		entry := ManifestEntry{Name: []string{"mysql-bin.000001", "mysql-bin.000002"}[i], ID: id, Size: meta.Size, FirstEventTime: meta.FirstEventTime, LastEventTime: meta.LastEventTime, PreviousGTIDs: meta.PreviousGTIDs, GTIDCoverage: meta.GTIDCoverage}
		if entry.ID == "" {
			entry.ContentID = meta.GTIDCoverage.ContentID
		}
		key, err := ArchiveEntryKey("binlogs", "iad", entry)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
		keys = append(keys, key)
	}
	if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Files: entries}); err != nil {
		t.Fatal(err)
	}
	store.manifestPuts = 0
	status := &atomic.Int32{}
	status.Store(http.StatusOK)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
		_ = json.NewEncoder(w).Encode(pitrCutoffResponse{Coverage: PITRGTIDCoverageProtocol, CutoffTime: "2026-04-17T00:00:00Z", RetainedGTIDs: []string{coverageTestSID + ":1-11"}})
	}))
	t.Cleanup(srv.Close)
	cfg := &PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}
	factory := func() *BinlogArchiver {
		a := NewBinlogArchiver(cfg, "iad", &stubRoleChecker{}, store, slog.Default())
		a.SetRetentionClient(strings.TrimPrefix(srv.URL, "http://"), "ns", "fg", "p", time.Hour)
		return a
	}
	return store, factory, status, entries, keys
}

func TestRetentionRetriesFailedDeleteAfterRestart(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		label := "ID-aware"
		if legacy {
			label = "legacy"
		}
		t.Run(label, func(t *testing.T) {
			store, factory, status, entries, keys := newRetentionRetryFixture(t, legacy)
			store.failDelete[keys[0]] = 1
			factory().maybeRunRetention(context.Background())
			rows := readRetentionPendingRows(t, store)
			if len(rows) != 1 || rows[0].Deleted {
				t.Errorf("failed delete lost its durable pending record: %+v", rows)
			} else {
				wantID, err := fileContentID("testdata/incarnation-a.bin")
				if err != nil {
					t.Fatal(err)
				}
				actualID := rows[0].ID
				if actualID == "" {
					actualID = rows[0].ContentID
				}
				if actualID != wantID || rows[0].Key != keys[0] || rows[0].PreviousGTIDs != entries[0].PreviousGTIDs || !rows[0].FirstEventTime.Equal(entries[0].FirstEventTime) {
					t.Errorf("retired identity/header metadata missing: %+v", rows[0])
				}
			}
			status.Store(http.StatusServiceUnavailable)
			factory().maybeRunRetention(context.Background())
			_, exists, err := store.Get(context.Background(), keys[0])
			if err != nil || exists {
				t.Errorf("failed delete was never retried after restart: exists=%t, %v", exists, err)
			}
			if store.deletes[keys[0]] != 2 {
				t.Errorf("delete attempts=%d, want 2", store.deletes[keys[0]])
			}
			if store.deletes[keys[1]] != 0 {
				t.Fatal("protected object was deleted")
			}
			rows = readRetentionPendingRows(t, store)
			if len(rows) != 1 || !rows[0].Deleted {
				t.Errorf("successful deletion was not acknowledged: %+v", rows)
			}
			m, err := loadManifest(context.Background(), store, "binlogs", "iad")
			if err != nil || len(m.Files) != 1 || m.Files[0].Name != "mysql-bin.000002" {
				t.Fatalf("live manifest did not preserve the protected row: %+v, %v", m, err)
			}
		})
	}
}

func TestRetentionRetriesAfterDeleteAcknowledgementFails(t *testing.T) {
	store, factory, status, _, keys := newRetentionRetryFixture(t, false)
	store.failSaveAt = 2 // Retire save succeeds; deletion acknowledgement fails.
	factory().maybeRunRetention(context.Background())
	_, exists, err := store.Get(context.Background(), keys[0])
	if err != nil || exists {
		t.Fatalf("first delete did not succeed: %t, %v", exists, err)
	}
	rows := readRetentionPendingRows(t, store)
	if len(rows) != 1 || rows[0].Deleted {
		t.Errorf("unacknowledged delete was not durable: %+v", rows)
	}
	status.Store(http.StatusServiceUnavailable)
	factory().maybeRunRetention(context.Background())
	if store.deletes[keys[0]] != 2 {
		t.Errorf("missing-object delete was not retried: %d calls", store.deletes[keys[0]])
	}
	rows = readRetentionPendingRows(t, store)
	if len(rows) != 1 || !rows[0].Deleted {
		t.Errorf("retry was not acknowledged: %+v", rows)
	}
}

func TestRetentionResumesPersistedDeleteBeforeContactingOperator(t *testing.T) {
	store, factory, status, entries, keys := newRetentionRetryFixture(t, false)
	body := map[string]any{"version": 1, "site": "iad", "files": entries[1:], "pruned": []map[string]any{{"name": entries[0].Name, "id": entries[0].ID, "key": "unrelated/victim", "deleted": false, "previousGtids": entries[0].PreviousGTIDs, "firstEventTime": entries[0].FirstEventTime}}}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), "unrelated/victim", strings.NewReader("protected"), 9); err != nil {
		t.Fatal(err)
	}
	status.Store(http.StatusServiceUnavailable)
	factory().maybeRunRetention(context.Background())
	_, exists, err := store.Get(context.Background(), keys[0])
	if err != nil || exists {
		t.Errorf("persisted intent was not resumed: %t, %v", exists, err)
	}
	if store.deletes["unrelated/victim"] != 0 {
		t.Fatal("stored tombstone key steered deletion outside the archive")
	}
	rows := readRetentionPendingRows(t, store)
	if len(rows) != 1 || !rows[0].Deleted {
		t.Errorf("resumed delete not acknowledged: %+v", rows)
	}
}

func TestRetentionDoesNotDeleteBeforeRetirementSave(t *testing.T) {
	store, factory, _, _, keys := newRetentionRetryFixture(t, false)
	store.failSaveAt = 1
	factory().maybeRunRetention(context.Background())
	if store.deletes[keys[0]] != 0 {
		t.Fatal("object deleted before retirement was durable")
	}
	m, err := loadManifest(context.Background(), store, "binlogs", "iad")
	if err != nil || len(m.Files) != 2 || len(readRetentionPendingRows(t, store)) != 0 {
		t.Fatalf("failed save changed the durable manifest: %+v, %v", m, err)
	}
	_, exists, err := store.Get(context.Background(), keys[0])
	if err != nil || !exists {
		t.Fatalf("failed retirement lost object: %t, %v", exists, err)
	}
	factory().maybeRunRetention(context.Background())
	_, exists, err = store.Get(context.Background(), keys[0])
	if err != nil || exists {
		t.Fatalf("retry after save failure did not clean up: %t, %v", exists, err)
	}
}

func TestRetentionDoesNotRetryDeleteOfLiveReference(t *testing.T) {
	store, factory, status, entries, keys := newRetentionRetryFixture(t, false)
	body := map[string]any{"version": 1, "site": "iad", "files": entries, "pruned": []map[string]any{{"name": entries[0].Name, "id": entries[0].ID, "key": keys[0], "deleted": false}}}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	status.Store(http.StatusServiceUnavailable)
	factory().maybeRunRetention(context.Background())
	_, exists, err := store.Get(context.Background(), keys[0])
	if err != nil || !exists {
		t.Fatalf("pending retry deleted an object advertised for restore: %t, %v", exists, err)
	}
	if store.deletes[keys[0]] != 0 {
		t.Fatal("live reference was passed to Delete")
	}
	status.Store(http.StatusOK)
	factory().maybeRunRetention(context.Background())
	_, exists, err = store.Get(context.Background(), keys[0])
	if err != nil || exists {
		t.Fatalf("revalidated retirement did not finish: %t, %v", exists, err)
	}
}

func TestRetentionAcknowledgesOnlySuccessfulDeletes(t *testing.T) {
	store, factory, status, entries, keys := newRetentionRetryFixture(t, false)
	extra := entries[0]
	extra.Name = "mysql-bin.000003"
	extraKey, err := ArchiveEntryKey("binlogs", "iad", extra)
	if err != nil {
		t.Fatal(err)
	}
	data, _, err := store.Get(context.Background(), keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), extraKey, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if err := appendManifestEntry(context.Background(), store, "binlogs", "iad", extra); err != nil {
		t.Fatal(err)
	}
	store.failDelete[keys[0]] = 1
	factory().maybeRunRetention(context.Background())
	rows := readRetentionPendingRows(t, store)
	if len(rows) != 2 {
		t.Fatalf("want two retirement records: %+v", rows)
	}
	for _, row := range rows {
		if row.Deleted != (row.Name == extra.Name) {
			t.Errorf("wrong per-object acknowledgement: %+v", row)
		}
	}
	status.Store(http.StatusServiceUnavailable)
	factory().maybeRunRetention(context.Background())
	if store.deletes[keys[0]] != 2 || store.deletes[extraKey] != 1 {
		t.Fatalf("retry ignored acknowledgements: %v", store.deletes)
	}
}
