package sidecar

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

type identityCountingStore struct {
	ArchiveStore
	puts     int
	failNext bool
}

func (s *identityCountingStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if !strings.Contains(key, "/manifest-") {
		s.puts++
		if s.failNext {
			s.failNext = false
			return errors.New("injected upload failure")
		}
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

func (s *identityCountingStore) PutFile(ctx context.Context, key, src string) error {
	s.puts++
	if s.failNext {
		s.failNext = false
		return errors.New("injected upload failure")
	}
	return s.ArchiveStore.PutFile(ctx, key, src)
}

func TestBinlogIdentityFixturesParseCompletely(t *testing.T) {
	for _, name := range []string{"incarnation-a.bin", "incarnation-b.bin", "same-header.bin"} {
		parser := replication.NewBinlogParser()
		parser.SetVerifyChecksum(true)
		var events []replication.EventType
		err := parser.ParseFile(filepath.Join("testdata", name), 0, func(e *replication.BinlogEvent) error { events = append(events, e.Header.EventType); return nil })
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 5 || events[4] != replication.STOP_EVENT {
			t.Fatalf("incomplete fixture parse: %v", events)
		}
		meta, err := parseBinlogMetadata(filepath.Join("testdata", name))
		if err != nil || meta.FirstEventTime.IsZero() || meta.PreviousGTIDs == "" {
			t.Fatalf("fixture metadata: %+v, %v", meta, err)
		}
	}
}

func TestArchiverPreservesReusedBinlogNames(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, replacement := range []string{"incarnation-b.bin", "same-header.bin"} {
			name := replacement
			if legacy {
				name += "/legacy"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				local := t.TempDir()
				cfg := &PITRConfig{StorageType: "PVC", BinlogDir: local, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour}
				pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				store := &identityCountingStore{ArchiveStore: pvc}
				oldBytes, err := os.ReadFile("testdata/incarnation-a.bin")
				if err != nil {
					t.Fatal(err)
				}
				newBytes, err := os.ReadFile(filepath.Join("testdata", replacement))
				if err != nil {
					t.Fatal(err)
				}
				if len(oldBytes) != len(newBytes) {
					t.Fatal("regression requires equal-size files")
				}
				localFile := filepath.Join(local, "mysql-bin.000001")
				if err := os.WriteFile(localFile, oldBytes, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(local, cfg.BinlogIndex), []byte("mysql-bin.000001\nmysql-bin.000002\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				newArchiver := func() *BinlogArchiver {
					return NewBinlogArchiver(cfg, "iad", &stubRoleChecker{}, store, slog.Default())
				}
				a := newArchiver()
				if legacy {
					key := binlogObjectKey("binlogs", "iad", "mysql-bin.000001")
					if err := store.Put(ctx, key, bytes.NewReader(oldBytes), int64(len(oldBytes))); err != nil {
						t.Fatal(err)
					}
					meta, err := parseBinlogMetadata(localFile)
					if err != nil {
						t.Fatal(err)
					}
					if err := appendManifestEntry(ctx, store, "binlogs", "iad", ManifestEntry{Name: meta.Name, RemotePath: key, Size: meta.Size, PreviousGTIDs: meta.PreviousGTIDs, FirstEventTime: meta.FirstEventTime, LastEventTime: meta.LastEventTime}); err != nil {
						t.Fatal(err)
					}
				} else {
					a.scanAndArchive(ctx)
				}
				m, err := loadManifest(ctx, store, "binlogs", "iad")
				if err != nil || len(m.Files) != 1 {
					t.Fatalf("initial archive: %+v, %v", m, err)
				}
				originalKey := m.Files[0].RemotePath
				initialPuts := store.puts
				a.scanAndArchive(ctx)
				if store.puts != initialPuts {
					t.Fatal("unchanged original was re-uploaded")
				}
				originalInfo, err := os.Stat(localFile)
				if err != nil {
					t.Fatal(err)
				}
				replacementPath := localFile + ".replacement"
				if err := os.WriteFile(replacementPath, newBytes, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(replacementPath, originalInfo.ModTime(), originalInfo.ModTime()); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(replacementPath, localFile); err != nil {
					t.Fatal(err)
				}
				store.failNext = true
				a.scanAndArchive(ctx)
				if snap := a.Snapshot(ctx); snap.BacklogFiles != 1 || snap.LastError == "" {
					t.Fatalf("new incarnation must count as pending after failed upload: %+v", snap)
				}
				a.scanAndArchive(ctx)
				m, err = loadManifest(ctx, store, "binlogs", "iad")
				if err != nil || len(m.Files) != 2 {
					t.Fatalf("want both incarnations: %+v, %v", m, err)
				}
				if m.Files[0].RemotePath == m.Files[1].RemotePath {
					t.Fatal("incarnations share an object key")
				}
				got, ok, err := store.Get(ctx, originalKey)
				if err != nil || !ok || !bytes.Equal(got, oldBytes) {
					t.Fatal("original archive bytes were overwritten")
				}
				var foundNew bool
				for _, row := range m.Files {
					got, ok, err := store.Get(ctx, row.RemotePath)
					if err == nil && ok && bytes.Equal(got, newBytes) {
						foundNew = true
					}
				}
				if !foundNew {
					t.Fatal("replacement binlog bytes were not preserved")
				}
				before := store.puts
				a.scanAndArchive(ctx)
				newArchiver().scanAndArchive(ctx)
				if store.puts != before {
					t.Fatal("unchanged content was re-uploaded after scan/restart")
				}
			})
		}
	}
}

func TestManifestPrunesOnlySelectedBinlogIdentity(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	first := ManifestEntry{Name: "mysql-bin.000001", ID: strings.Repeat("a", 64), RemotePath: "unrelated/secret", Size: 100}
	second := first
	second.ID = strings.Repeat("b", 64)
	legacy := first
	legacy.ID = ""
	for _, entry := range []ManifestEntry{first, second, legacy} {
		if err := appendManifestEntry(ctx, store, "binlogs", "iad", entry); err != nil {
			t.Fatal(err)
		}
	}
	first.Size = 150
	if err := appendManifestEntry(ctx, store, "binlogs", "iad", first); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 3 {
		t.Fatalf("identity upsert: %+v, %v", m, err)
	}
	removed, err := pruneManifest(ctx, store, "binlogs", "iad", map[binlogIdentity]string{first.identity(): first.ID})
	if err != nil || len(removed) != 1 || removed[0] != "binlogs/iad/"+first.Name+"."+first.ID {
		t.Fatalf("confined delete keys: %v, %v", removed, err)
	}
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 2 {
		t.Fatalf("pruning removed another incarnation: %+v, %v", m, err)
	}
	for _, row := range m.Files {
		if row.ID == first.ID {
			t.Fatal("selected identity was not pruned")
		}
	}
	bad := ManifestEntry{Name: first.Name, ID: "../../victim", RemotePath: "unrelated/secret"}
	if err := appendManifestEntry(ctx, store, "binlogs", "iad", bad); err != nil {
		t.Fatal(err)
	}
	removed, err = pruneManifest(ctx, store, "binlogs", "iad", map[binlogIdentity]string{bad.identity(): ""})
	if err != nil || len(removed) != 0 {
		t.Fatalf("invalid identity generated delete keys: %v, %v", removed, err)
	}
}

func TestArchiveEntryKeyRejectsInvalidIdentity(t *testing.T) {
	for _, entry := range []ManifestEntry{
		{Name: "../../victim"}, {Name: "mysql-bin.000001", ID: "../../victim"},
		{Name: "mysql-bin.000001", ID: strings.Repeat("A", 64)},
		{Name: "mysql-bin.000001", ID: strings.Repeat("a", 63)},
	} {
		if key, err := ArchiveEntryKey("binlogs", "iad", entry); err == nil {
			t.Errorf("accepted unsafe identity: %q", key)
		}
	}
}
