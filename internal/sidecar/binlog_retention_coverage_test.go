package sidecar

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

const coverageTestSID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

func TestRetentionRequiresGTIDCoverageInEveryBackup(t *testing.T) {
	base := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		response    map[string]any
		wantDeleted bool
	}{
		{"covered and lagging", map[string]any{"coverage": "gtid-v1", "retainedGTIDs": []string{coverageTestSID + ":1-20", coverageTestSID + ":1-11"}}, true},
		{"legacy time-only response", map[string]any{}, false},
		{"unknown backup GTIDs", map[string]any{"coverage": "gtid-v1", "retainedGTIDs": []string{coverageTestSID + ":1-20", ""}}, false},
		{"malformed backup GTIDs", map[string]any{"coverage": "gtid-v1", "retainedGTIDs": []string{"not-a-gtid"}}, false},
		{"missing backup proof", map[string]any{"coverage": "gtid-v1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.response["cutoffTime"] = base.Add(4 * time.Hour).Format(time.RFC3339)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(tc.response) }))
			defer srv.Close()
			store := newMemStore()
			ctx := context.Background()
			var rows []map[string]any
			for i, fixture := range []string{"incarnation-a.bin", "incarnation-b.bin", "tagged-transaction.bin", "incarnation-a.bin"} {
				name := []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003", "mysql-bin.000004"}[i]
				file := filepath.Join("testdata", fixture)
				payload, err := os.ReadFile(file)
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
				meta.GTIDCoverage.Complete = i != 3
				rows = append(rows, map[string]any{"name": name, "contentId": id, "lastEventTime": base.Add(time.Hour), "gtidCoverage": meta.GTIDCoverage})
				if err := store.Put(ctx, "binlogs/iad/"+name, bytes.NewReader(payload), int64(len(payload))); err != nil {
					t.Fatal(err)
				}
			}
			data, err := json.Marshal(map[string]any{"version": 1, "site": "iad", "files": rows})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatal(err)
			}
			a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
			a.SetRetentionClient(strings.TrimPrefix(srv.URL, "http://"), "ns", "fg", "p", time.Hour)
			a.maybeRunRetention(ctx)
			for i, row := range rows {
				_, exists, err := store.Get(ctx, "binlogs/iad/"+row["name"].(string))
				if err != nil {
					t.Fatal(err)
				}
				if want := !(tc.wantDeleted && i == 0); exists != want {
					t.Errorf("file %s exists=%t, want %t; retain every transaction absent from any backup", row["name"], exists, want)
				}
			}
		})
	}
}

func TestBinlogMetadataIncludesCompleteTransactionCoverage(t *testing.T) {
	data, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	anonymous := bytes.Clone(data)
	for offset := 4; offset < len(anonymous); {
		size := int(binary.LittleEndian.Uint32(anonymous[offset+9 : offset+13]))
		if anonymous[offset+4] == 33 {
			anonymous[offset+4] = 34
			binary.LittleEndian.PutUint32(anonymous[offset+size-4:offset+size], crc32.ChecksumIEEE(anonymous[offset:offset+size-4]))
			break
		}
		offset += size
	}
	badFormatChecksum := bytes.Clone(data)
	badFormatChecksum[4] ^= 1
	badChecksum := bytes.Clone(data)
	badChecksum[len(badChecksum)-1] ^= 1
	for _, tc := range []struct {
		name  string
		bytes []byte
		known bool
	}{
		{"complete", data, true},
		{"partial final header", append(append([]byte{}, data...), 1), false},
		{"partial final event", data[:len(data)-1], false},
		{"anonymous transaction", anonymous, false},
		{"bad checksum", badChecksum, false},
		{"bad format checksum", badFormatChecksum, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "mysql-bin.000001")
			if err := os.WriteFile(file, tc.bytes, 0o600); err != nil {
				t.Fatal(err)
			}
			meta, err := parseBinlogMetadata(file)
			if err != nil && tc.known {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			coverage, ok := fields["gtidCoverage"].(map[string]any)
			if tc.known {
				if !ok || coverage["complete"] != true || coverage["gtids"] != coverageTestSID+":11" {
					t.Fatalf("complete file lacks transaction proof: %s", encoded)
				}
			} else if ok && coverage["complete"] == true {
				t.Fatalf("partial file incorrectly proved coverage: %s", encoded)
			}
		})
	}
}

func TestRetentionBackfillsCoverageWithoutAliasingIncarnations(t *testing.T) {
	ctx := context.Background()
	store, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var rows []ManifestEntry
	for _, fixture := range []string{"incarnation-a.bin", "incarnation-b.bin"} {
		file := filepath.Join("testdata", fixture)
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := parseBinlogMetadata(file)
		if err != nil {
			t.Fatal(err)
		}
		entry := ManifestEntry{Name: "mysql-bin.000001", FirstEventTime: meta.FirstEventTime, LastEventTime: meta.LastEventTime, PreviousGTIDs: meta.PreviousGTIDs}
		if len(rows) > 0 {
			entry.ID, err = fileContentID(file)
			if err != nil {
				t.Fatal(err)
			}
		}
		key, err := ArchiveEntryKey("binlogs", "iad", entry)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, entry)
	}
	if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Files: rows}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("coverage") != PITRGTIDCoverageProtocol {
			t.Error("client omitted coverage capability")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"coverage": PITRGTIDCoverageProtocol, "cutoffTime": "2026-04-17T00:00:00Z", "retainedGTIDs": []string{coverageTestSID + ":1-11"}})
	}))
	defer srv.Close()
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
	a.SetRetentionClient(strings.TrimPrefix(srv.URL, "http://"), "ns", "fg", "p", time.Hour)
	a.maybeRunRetention(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 || m.Files[0].ID != rows[1].ID {
		t.Fatalf("retention confused reused names: %+v", m.Files)
	}
	if m.Files[0].GTIDCoverage == nil || !m.Files[0].GTIDCoverage.Complete || m.Files[0].GTIDCoverage.GTIDs != coverageTestSID+":21" {
		t.Fatalf("legacy coverage was not persisted: %+v", m.Files[0])
	}
	for i, row := range rows {
		key, err := ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		_, exists, err := store.Get(ctx, key)
		if err != nil || exists != (i == 1) {
			t.Errorf("identity %d exists=%t: %v", i, exists, err)
		}
	}
}

func TestTaggedBinlogCoverageStaysSeparateFromUntaggedTransactions(t *testing.T) {
	meta, err := parseBinlogMetadata("testdata/tagged-transaction.bin")
	if err != nil {
		t.Fatal(err)
	}
	if meta.GTIDCoverage == nil || !meta.GTIDCoverage.Complete || meta.GTIDCoverage.GTIDs != coverageTestSID+":restore_tag:11" {
		t.Fatalf("tagged transaction coverage: %+v", meta.GTIDCoverage)
	}
	untagged, err := internalmysql.ParseGTIDSet(coverageTestSID + ":1-100")
	if err != nil {
		t.Fatal(err)
	}
	tagged, err := internalmysql.ParseGTIDSet(coverageTestSID + ":restore_tag:1-11")
	if err != nil {
		t.Fatal(err)
	}
	if fileCoveredByBackups(meta.GTIDCoverage, []internalmysql.GTIDSet{untagged}) {
		t.Fatal("untagged backup authorized deleting tagged data")
	}
	if !fileCoveredByBackups(meta.GTIDCoverage, []internalmysql.GTIDSet{tagged}) {
		t.Fatal("tagged backup should cover the tagged transaction")
	}
}

func TestRetentionKeepsUnverifiableArchivedCoverage(t *testing.T) {
	data, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mismatched content ID", "truncated file"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			content := bytes.Clone(data)
			entry := ManifestEntry{Name: "mysql-bin.000001", LastEventTime: time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC), ID: strings.Repeat("a", 64)}
			if name == "truncated file" {
				content = content[:len(content)-1]
				source := filepath.Join(t.TempDir(), "source")
				if err := os.WriteFile(source, content, 0o600); err != nil {
					t.Fatal(err)
				}
				entry.ID, err = fileContentID(source)
				if err != nil {
					t.Fatal(err)
				}
			}
			key, err := ArchiveEntryKey("binlogs", "iad", entry)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content))); err != nil {
				t.Fatal(err)
			}
			if err := appendManifestEntry(ctx, store, "binlogs", "iad", entry); err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(pitrCutoffResponse{Coverage: PITRGTIDCoverageProtocol, CutoffTime: "2026-04-17T00:00:00Z", RetainedGTIDs: []string{coverageTestSID + ":1-100"}})
			}))
			defer srv.Close()
			a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
			a.SetRetentionClient(strings.TrimPrefix(srv.URL, "http://"), "ns", "fg", "p", time.Hour)
			a.maybeRunRetention(ctx)
			_, exists, err := store.Get(ctx, key)
			if err != nil || !exists {
				t.Fatalf("unknown file was deleted: %v", err)
			}
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil || len(m.Files) != 1 {
				t.Fatalf("unknown row was removed: %+v, %v", m, err)
			}
			if m.Files[0].GTIDCoverage != nil && m.Files[0].GTIDCoverage.Complete {
				t.Fatal("unknown file received a complete proof")
			}
		})
	}
}
