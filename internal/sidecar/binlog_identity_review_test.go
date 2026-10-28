package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type reviewLegacyReads struct {
	ArchiveStore
	gets       int
	beforeRead func()
}

type reviewLegacyReader struct {
	io.ReadCloser
	beforeRead func()
}

func (r *reviewLegacyReader) Read(p []byte) (int, error) {
	if r.beforeRead != nil {
		r.beforeRead()
		r.beforeRead = nil
	}
	return r.ReadCloser.Read(p)
}

func (s *reviewLegacyReads) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.ArchiveStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	return &reviewLegacyReader{ReadCloser: r, beforeRead: func() {
		s.gets++
		if s.beforeRead != nil {
			s.beforeRead()
		}
	}}, nil
}

func TestReviewLegacyIdentityRecoveryAndPriority(t *testing.T) {
	for _, mode := range []string{"legacy", "stripped", "missing", "transient"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			local := t.TempDir()
			cfg := &PITRConfig{BinlogDir: local, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}
			pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			store := &reviewLegacyReads{ArchiveStore: pvc}
			payload, err := os.ReadFile("testdata/incarnation-a.bin")
			if err != nil {
				t.Fatal(err)
			}
			oldName, newName := "mysql-bin.000001", "mysql-bin.000002"
			for _, name := range []string{oldName, newName} {
				if err := os.WriteFile(filepath.Join(local, name), payload, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(local, cfg.BinlogIndex), []byte(oldName+"\n"+newName+"\nmysql-bin.000003\n"), 0600); err != nil {
				t.Fatal(err)
			}
			id, err := fileContentID(filepath.Join(local, oldName))
			if err != nil {
				t.Fatal(err)
			}
			oldKey := "binlogs/iad/" + oldName
			if mode == "stripped" {
				oldKey += "." + id
			}
			if mode != "missing" {
				if err := pvc.Put(ctx, oldKey, bytes.NewReader(payload), int64(len(payload))); err != nil {
					t.Fatal(err)
				}
			}
			meta, err := parseBinlogMetadata("testdata/incarnation-a.bin")
			if err != nil {
				t.Fatal(err)
			}
			row := ManifestEntry{Name: oldName, RemotePath: oldKey, Size: meta.Size, PreviousGTIDs: meta.PreviousGTIDs}
			if mode == "missing" {
				row.ContentID = id
			}
			if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Files: []ManifestEntry{row}}); err != nil {
				t.Fatal(err)
			}
			if mode == "transient" {
				if err := os.Remove(filepath.Join(pvc.root, oldKey)); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(pvc.root, oldKey), 0700); err != nil {
					t.Fatal(err)
				}
			}
			store.beforeRead = func() {
				keys, err := pvc.List(ctx, "binlogs/iad/"+newName+".")
				if err != nil || len(keys) != 1 {
					t.Errorf("legacy verification preceded fresh upload: %v, %v", keys, err)
				}
			}
			create := func() *BinlogArchiver {
				return NewBinlogArchiver(cfg, "iad", &stubRoleChecker{}, store, slog.Default())
			}
			a := create()
			a.scanAndArchive(ctx)
			if mode == "transient" {
				a.scanAndArchive(ctx)
				if a.Snapshot(ctx).LastError == "" {
					t.Fatal("transient read failure ignored")
				}
				return
			}
			if a.Snapshot(ctx).LastError != "" {
				t.Fatalf("archive failed: %s", a.Snapshot(ctx).LastError)
			}
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "stripped" {
				if m.Files[0].ID != id {
					t.Fatalf("ID-stripped row not normalized: %+v", m.Files[0])
				}
			}
			if mode == "missing" {
				found := false
				count := 0
				for _, r := range m.Files {
					found = found || r.Name == oldName && r.ID == id
					if r.Name == oldName {
						count++
					}
				}
				if !found || count != 1 {
					t.Fatal("missing legacy object not repaired")
				}
				return
			}
			reads := store.gets
			create().scanAndArchive(ctx)
			if store.gets != reads {
				t.Fatalf("legacy identity downloaded again after restart: %d -> %d", reads, store.gets)
			}
		})
	}
}

func TestReviewStrippedIdentityRetainsObjectAddress(t *testing.T) {
	ctx := context.Background()
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id, err := fileContentID("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	row := ManifestEntry{Name: "mysql-bin.000001", RemotePath: "binlogs/iad/mysql-bin.000001." + id}
	if err := pvc.Put(ctx, row.RemotePath, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, pvc, slog.Default())
	// Retention uses this same inspection path to backfill legacy metadata.
	err = a.inspectArchivedBinlog(ctx, row, func(evidence BinlogEvidence) error {
		if evidence.Digest != id {
			t.Fatal("inspection read different bytes")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stripped row cannot be inspected by retention: %v", err)
	}
}

func TestReviewEncryptedIdentityIsKeyed(t *testing.T) {
	plain := newMemStore()
	file := "testdata/incarnation-a.bin"
	sha, err := fileContentID(file)
	if err != nil {
		t.Fatal(err)
	}
	var previous, first string
	for _, pass := range []string{"high entropy key A", "high entropy key B", "high entropy key A"} {
		store := WrapWithEncryption(plain, []byte(pass))
		a := NewBinlogArchiver(&PITRConfig{}, "iad", &stubRoleChecker{}, store, slog.Default())
		id, err := a.localBinlogID(file)
		if err != nil {
			t.Fatal(err)
		}
		if id == sha || len(id) != 64 {
			t.Fatalf("encrypted identity exposes plaintext digest: %s", id)
		}
		if previous != "" && previous == id {
			t.Fatal("different keys yield same identity")
		}
		if first == "" {
			first = id
		} else if pass == "high entropy key A" && first != id {
			t.Fatal("same key did not produce stable identity")
		}
		previous = id
	}
}

func TestReviewStrippedIdentityConfinement(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	id := strings.Repeat("a", 64)
	for _, key := range []string{"other/iad/mysql-bin.000001." + id, "binlogs/other/mysql-bin.000001." + id, "binlogs/iad/../iad/mysql-bin.000001." + id, "binlogs/iad/elsewhere." + id} {
		data, _ := json.Marshal(Manifest{Files: []ManifestEntry{{Name: "mysql-bin.000001", RemotePath: key}}})
		if err := store.Put(ctx, ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		m, err := loadManifest(ctx, store, "binlogs", "iad")
		if err != nil {
			t.Fatal(err)
		}
		if m.Files[0].ID != "" {
			t.Fatalf("adopted unconfined key %s", key)
		}
	}
}
