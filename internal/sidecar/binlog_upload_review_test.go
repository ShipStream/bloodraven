package sidecar

import (
	"bytes"
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type reviewReplaceBeforeUploadStore struct {
	ArchiveStore
	replacement []byte
	replaced    bool
	target      string
}

func (s *reviewReplaceBeforeUploadStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if !strings.Contains(key, "/manifest-") && !s.replaced {
		s.replaced = true
		if s.target != "" {
			next := s.target + ".replacement"
			if err := os.WriteFile(next, s.replacement, 0600); err != nil {
				return err
			}
			if err := os.Rename(next, s.target); err != nil {
				return err
			}
		} else {
			return s.ArchiveStore.Put(ctx, key, bytes.NewReader(s.replacement), int64(len(s.replacement)))
		}
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

func TestReviewUploadedBytesStayBoundToIdentityAndCoverage(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, replaceSource := range []bool{false, true} {
			t.Run(fmt.Sprintf("encrypted=%v/source=%v", encrypted, replaceSource), func(t *testing.T) {
				ctx := context.Background()
				pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				var backend ArchiveStore = pvc
				if encrypted {
					backend = WrapWithEncryption(backend, []byte("random high entropy archive key"))
				}
				local := t.TempDir()
				file := filepath.Join(local, "mysql-bin.000099")
				initial, err := os.ReadFile("testdata/incarnation-a.bin")
				if err != nil {
					t.Fatal(err)
				}
				replacement, err := os.ReadFile("testdata/incarnation-b.bin")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(file, initial, 0600); err != nil {
					t.Fatal(err)
				}
				store := &reviewReplaceBeforeUploadStore{ArchiveStore: backend, replacement: replacement}
				if replaceSource {
					store.target = file
				}
				a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: local}, "iad", &stubRoleChecker{}, store, slog.Default())
				id, err := a.localBinlogID(file)
				if err != nil {
					t.Fatal(err)
				}
				err = a.archiveOne(ctx, file, "mysql-bin.000099", id)
				m, loadErr := loadManifest(ctx, backend, "binlogs", "iad")
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if err != nil {
					if len(m.Files) != 0 {
						t.Fatal("failed upload published metadata that could authorize retirement")
					}
					if replaceSource {
						t.Fatalf("pinned inode should survive replacement of the original source: %v", err)
					}
					return
				}
				if len(m.Files) != 1 {
					t.Fatalf("manifest: %+v", m)
				}
				key, err := ArchiveEntryKey("binlogs", "iad", m.Files[0])
				if err != nil {
					t.Fatal(err)
				}
				got, ok, err := backend.Get(ctx, key)
				if err != nil || !ok || !bytes.Equal(got, initial) {
					t.Fatalf("uploaded GTID 21 under GTID 11 identity/metadata: %t, %v", ok, err)
				}
			})
		}
	}
}

func TestReviewOrderingFixturesHaveValidPredecessors(t *testing.T) {
	for _, name := range []string{"review-order-a.bin", "review-order-b.bin", "review-clock-b.bin"} {
		parser := replication.NewBinlogParser()
		parser.SetVerifyChecksum(true)
		if err := parser.ParseFile(filepath.Join("testdata", name), 0, func(*replication.BinlogEvent) error { return nil }); err != nil {
			t.Fatal(err)
		}
		meta, err := parseBinlogMetadata(filepath.Join("testdata", name))
		if err != nil || meta.PreviousGTIDs == "" {
			t.Fatalf("fixture %s invalid: %+v, %v", name, meta, err)
		}
		t.Logf("%s first=%v previous=%s", name, meta.FirstEventTime, meta.PreviousGTIDs)
	}
}
