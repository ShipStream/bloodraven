package sidecar

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRetiredLocalBinlogIsNotReuploaded(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, pending := range []bool{false, true} {
			label := "ID-aware"
			if legacy {
				label = "legacy"
			}
			if pending {
				label += "/pending"
			} else {
				label += "/acknowledged"
			}
			t.Run(label, func(t *testing.T) {
				ctx := context.Background()
				store, factory, status, _, keys := newRetentionRetryFixture(t, legacy)
				a := factory()
				initialUploads := 0
				if !legacy {
					m, err := loadManifest(ctx, store, "binlogs", "iad")
					if err != nil {
						t.Fatal(err)
					}
					m.Files = m.Files[1:]
					if err := saveManifest(ctx, store, "binlogs", m); err != nil {
						t.Fatal(err)
					}
					if err := store.ArchiveStore.Delete(ctx, keys[0]); err != nil {
						t.Fatal(err)
					}
					initialUploads = 1
				}
				oldBytes, err := os.ReadFile("testdata/incarnation-a.bin")
				if err != nil {
					t.Fatal(err)
				}
				localFile := filepath.Join(a.cfg.BinlogDir, "mysql-bin.000001")
				if err := os.WriteFile(localFile, oldBytes, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex), []byte("mysql-bin.000001\nmysql-bin.000003\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				counted := &identityCountingStore{ArchiveStore: store}
				a.store = counted
				if pending {
					store.failDelete[keys[0]] = 1
				}
				a.scanAndArchive(ctx)
				if rows := readRetentionPendingRows(t, store); len(rows) != 1 {
					t.Fatalf("initial sweep did not retire the file: %+v", rows)
				}
				if counted.puts != initialUploads {
					t.Fatalf("initial archive uploads=%d, want %d", counted.puts, initialUploads)
				}
				status.Store(http.StatusServiceUnavailable)
				a.scanAndArchive(ctx)
				restarted := factory()
				restarted.store = counted
				restarted.scanAndArchive(ctx)
				if counted.puts != initialUploads {
					t.Errorf("retired local bytes were re-uploaded %d times", counted.puts-initialUploads)
				}
				m, err := loadManifest(ctx, store, "binlogs", "iad")
				if err != nil {
					t.Fatal(err)
				}
				if len(m.Files) != 1 || m.Files[0].Name != "mysql-bin.000002" {
					t.Errorf("retired live row returned: %+v", m.Files)
				}
				if snap := restarted.Snapshot(ctx); snap.BacklogFiles != 0 || snap.ManifestFileCount != 1 || snap.ManifestBytes != 360 || snap.LastError != "" {
					t.Errorf("retirement counted as active coverage/backlog: %+v", snap)
				}
				_, exists, err := store.Get(ctx, keys[0])
				if err != nil || exists {
					t.Errorf("retired object remains after resumed cleanup: %t, %v", exists, err)
				}
				// Identical headers are insufficient to suppress a new incarnation.
				replacement, err := os.ReadFile("testdata/same-header.bin")
				if err != nil {
					t.Fatal(err)
				}
				tmp := localFile + ".replacement"
				if err := os.WriteFile(tmp, replacement, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(tmp, localFile); err != nil {
					t.Fatal(err)
				}
				restarted.retentionCfg = nil // Isolate upload identity from the fixtures' old dates.
				counted.puts = 0
				restarted.scanAndArchive(ctx)
				if counted.puts != 1 {
					t.Errorf("new content reusing a retired name was not archived: %d uploads", counted.puts)
				}
			})
		}
	}
}

func TestSiteRetentionDoesNotDependOnUploadRoleOrSealedIndex(t *testing.T) {
	for _, name := range []string{"read-only", "no sealed logs", "role unavailable"} {
		t.Run(name, func(t *testing.T) {
			store, factory, _, _, keys := newRetentionRetryFixture(t, false)
			a := factory()
			switch name {
			case "read-only":
				a.mysql = &stubRoleChecker{readOnly: true}
			case "no sealed logs":
				if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex), []byte("mysql-bin.000003\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "role unavailable":
				a.mysql = &stubRoleChecker{isReadOnlyErr: errors.New("mysql unavailable")}
			}
			counted := &identityCountingStore{ArchiveStore: store}
			a.store = counted
			a.scanAndArchive(context.Background())
			_, exists, err := store.Get(context.Background(), keys[0])
			if err != nil || exists {
				t.Errorf("site skipped retention: exists=%t, %v", exists, err)
			}
			_, protected, err := store.Get(context.Background(), keys[1])
			if err != nil || !protected {
				t.Fatalf("needed object was removed: %t, %v", protected, err)
			}
			if counted.puts != 0 {
				t.Fatal("retention-only site uploaded binlogs")
			}
			if a.Snapshot(context.Background()).ManifestFileCount != 1 {
				t.Error("active manifest counts were not refreshed after retention")
			}
		})
	}
}

func TestRetirementCompactionRequiresAcknowledgementAndLocalAbsence(t *testing.T) {
	for _, mode := range []string{"readable index", "empty index", "missing index", "unreadable index"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			local := t.TempDir()
			indexPath := filepath.Join(local, "mysql-bin.index")
			switch mode {
			case "readable index":
				if err := os.WriteFile(indexPath, []byte("mysql-bin.000003\nmysql-bin.000009\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "empty index":
				if err := os.WriteFile(indexPath, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "unreadable index":
				if err := os.Mkdir(indexPath, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(local, "mysql-bin.000004"), []byte("unindexed local bytes"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("missing-target", filepath.Join(local, "mysql-bin.000005")); err != nil {
				t.Fatal(err)
			}
			var records []PrunedEntry
			for i, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003", "mysql-bin.000004", "mysql-bin.000005"} {
				records = append(records, PrunedEntry{Name: name, ID: strings.Repeat("a", 64), Deleted: i != 1})
			}
			if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Pruned: records}); err != nil {
				t.Fatal(err)
			}
			a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: local, BinlogIndex: "mysql-bin.index"}, "iad", &stubRoleChecker{readOnly: true}, store, slog.Default())
			a.scanAndArchive(ctx)
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, p := range m.Pruned {
				got = append(got, p.Name)
			}
			want := []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003", "mysql-bin.000004", "mysql-bin.000005"}
			if mode == "readable index" {
				want = want[1:]
			}
			if mode == "empty index" {
				want = []string{"mysql-bin.000002", "mysql-bin.000004", "mysql-bin.000005"}
			}
			if !slices.Equal(got, want) {
				t.Errorf("compacted retirement records = %v, want %v", got, want)
			}
		})
	}
}

func TestFailedRetirementCompactionPreservesDurableSuppression(t *testing.T) {
	ctx := context.Background()
	store, factory, _, entries, keys := newRetentionRetryFixture(t, false)
	a := factory()
	a.retentionCfg = nil
	a.mysql = &stubRoleChecker{readOnly: true}
	record := PrunedEntry{Name: entries[0].Name, ID: entries[0].ID, Key: keys[0], Deleted: true}
	if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Pruned: []PrunedEntry{record}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex), []byte("mysql-bin.000009\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.manifestPuts = 0
	store.failSaveAt = 1
	a.scanAndArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Pruned) != 1 {
		t.Fatalf("failed compaction lost durable identity: %+v, %v", m, err)
	}
	data, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, record.Name), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex), []byte("mysql-bin.000001\nmysql-bin.000009\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	counted := &identityCountingStore{ArchiveStore: store}
	restarted := factory()
	restarted.retentionCfg = nil
	restarted.store = counted
	restarted.scanAndArchive(ctx)
	if counted.puts != 0 {
		t.Fatal("failed compaction caused retired bytes to be uploaded")
	}
}

func TestMalformedRetirementIdentityDoesNotSuppressNewData(t *testing.T) {
	ctx := context.Background()
	store, factory, _, entries, _ := newRetentionRetryFixture(t, false)
	a := factory()
	a.retentionCfg = nil
	// Matching names and header metadata cannot substitute for a valid content ID.
	records := []PrunedEntry{
		{Name: entries[0].Name, Deleted: true, PreviousGTIDs: entries[0].PreviousGTIDs, FirstEventTime: entries[0].FirstEventTime},
		{Name: entries[0].Name, ID: "../../outside", Deleted: true},
	}
	if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Pruned: records}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, entries[0].Name), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex), []byte("mysql-bin.000001\nmysql-bin.000009\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	counted := &identityCountingStore{ArchiveStore: store}
	a.store = counted
	a.scanAndArchive(ctx)
	if counted.puts != 1 {
		t.Fatalf("malformed retirement suppressed an upload: %d", counted.puts)
	}
}
