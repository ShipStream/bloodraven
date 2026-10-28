package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestRound2OrderingSupportsNativeXAAndChecksumOff(t *testing.T) {
	for _, name := range []string{"fresh-empty", "initial", "flush-empty", "xa-prepare", "xa", "load-data", "compressed", "checksum-off", "restart-empty", "recloned"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata/native", name+".bin"))
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := InspectBinlog(bytes.NewReader(data), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if evidence.Digest == "" || evidence.Metadata.Size != int64(len(data)) {
				t.Fatal("stream was not fully verified")
			}
			if (name == "xa-prepare" || name == "checksum-off") && evidence.Metadata.GTIDCoverage.Complete {
				t.Fatal("ordering weakened retention completeness")
			}
			if (name == "xa-prepare" || name == "load-data" || name == "checksum-off") && evidence.GTIDs.IsEmpty() {
				t.Fatal("ordering lost observed transactions")
			}
		})
	}
}
func TestRound2NoScratchAcrossEncryptionAndInspection(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plaintext", true: "encrypted"}[encrypted], func(t *testing.T) {
			ctx := context.Background()
			root, local := t.TempDir(), t.TempDir()
			destination := filepath.Join(t.TempDir(), "download")
			base, err := newPVCStore(&PITRPVCConfig{MountPath: root})
			if err != nil {
				t.Fatal(err)
			}
			var store ArchiveStore = base
			if encrypted {
				store = WrapWithEncryption(store, []byte("high entropy fixture passphrase"))
			}
			a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: local}, "iad", &stubRoleChecker{}, store, slog.Default())
			source := "testdata/incarnation-a.bin"
			data, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			id, err := a.localBinlogID(source)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("TMPDIR", filepath.Join(root, "missing-readonly-tmp"))
			if err := a.archiveOne(ctx, source, "mysql-bin.000001", id); err != nil {
				t.Fatal(err)
			}
			legacy := ManifestEntry{Name: "mysql-bin.000099", LastEventTime: time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)}
			key, err := ArchiveEntryKey("binlogs", "iad", legacy)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatal(err)
			}
			if err := appendManifestEntry(ctx, store, "binlogs", "iad", legacy); err != nil {
				t.Fatal(err)
			}
			got := ""
			err = a.inspectArchivedBinlog(ctx, legacy, func(evidence BinlogEvidence) error {
				got = store.BinlogContentID(evidence.Digest)
				return nil
			})
			if err != nil || got != id {
				t.Fatalf("legacy verification: %s, %v", got, err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(pitrCutoffResponse{Coverage: PITRGTIDCoverageProtocol, CutoffTime: "2026-04-17T00:00:00Z", RetainedGTIDs: []string{coverageTestSID + ":1-10"}})
			}))
			defer server.Close()
			a.SetRetentionClient(strings.TrimPrefix(server.URL, "http://"), "ns", "fg", "p", time.Hour)
			a.maybeRunRetention(ctx)
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, entry := range m.Files {
				if entry.Name == legacy.Name {
					found = entry.GTIDCoverage != nil && entry.ContentID == id
				}
			}
			if !found {
				t.Fatal("backfill did not persist verified metadata")
			}
			if err := store.GetFile(ctx, key, destination); err != nil {
				t.Fatal(err)
			}
			reader, err := os.Open(destination)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			gotData, err := io.ReadAll(reader)
			if err != nil || !bytes.Equal(gotData, data) {
				t.Fatal("download differs")
			}
		})
	}
}

type nativeCloneBaseStub struct {
	stubRoleChecker
	base string
}

func (s *nativeCloneBaseStub) BinlogBase(context.Context) (string, error) { return s.base, nil }
func TestRound2ArchiveCapturesNativeClonePurgedBase(t *testing.T) {
	catalog, err := os.ReadFile("testdata/native/catalog.tsv")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(catalog)), "\n")
	fields := strings.Split(lines[len(lines)-1], "\t")
	base := strings.ReplaceAll(fields[2], "\\n", ",")
	if base == "" {
		t.Fatal("native clone no longer exercises the missing-header baseline")
	}
	store := newMemStore()
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &nativeCloneBaseStub{base: base}, store, slog.Default())
	file := "testdata/native/recloned.bin"
	id, err := a.localBinlogID(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(context.Background(), file, fields[1], id); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(context.Background(), store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 || m.Files[0].PreviousGTIDs != "" || m.Files[0].BaseGTIDs != base {
		t.Fatalf("clone bridge was lost: %+v", m.Files)
	}
}
