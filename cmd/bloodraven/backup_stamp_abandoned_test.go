package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// stampOf is startedStamp for another incarnation (UID) of the backup.
func stampOf(uid string, start time.Time) backupmeta.Stamp {
	s := startedStamp(start)
	s.Backup.UID = types.UID("uid-" + uid)
	return s
}

// Review B1: a failed backup's stamp must not wedge a re-created backup of
// the same name. The start step replaces an abandoned stamp (same
// namespace and name, another UID, no serverClock.end) when no dump object
// remains under the location, and refuses otherwise.
func TestWriteBackupStampReplacesAbandonedStamp(t *testing.T) {
	ctx := context.Background()
	recreated := stampOf("new", stampStart.Add(time.Hour))
	tests := []struct {
		name     string
		store    func(*testing.T) sidecar.ArchiveStore
		existing backupmeta.Stamp
		dumpObj  bool
		wantErr  error
	}{
		{name: "abandoned stamp, empty location", store: stampTestStore, existing: stampOf("old", stampStart)},
		{name: "abandoned stamp on a store without atomic create", store: func(t *testing.T) sidecar.ArchiveStore { return noAtomicCreateStore{stampTestStore(t)} },
			existing: stampOf("old", stampStart)},
		{name: "abandoned encrypted stamp", store: func(t *testing.T) sidecar.ArchiveStore {
			return sidecar.WrapWithEncryptionOptions(stampTestStore(t), []byte("dump key"), false)
		}, existing: stampOf("old", stampStart)},
		{name: "abandoned stamp with a dump object left", store: stampTestStore, existing: stampOf("old", stampStart), dumpObj: true, wantErr: ErrBackupStampConflict},
		{name: "a finished dump of the earlier backup", store: stampTestStore, existing: func() backupmeta.Stamp {
			s := stampOf("old", stampStart)
			s.ServerClock.End = backupmeta.NewMicroTime(stampStart.Add(time.Minute))
			return s
		}(), wantErr: ErrBackupStampConflict},
		{name: "same name in another namespace", store: stampTestStore, existing: func() backupmeta.Stamp {
			s := stampOf("old", stampStart)
			s.Backup.Namespace = "other-ns"
			return s
		}(), wantErr: ErrBackupStampConflict},
		{name: "another name", store: stampTestStore, existing: func() backupmeta.Stamp {
			s := stampOf("old", stampStart)
			s.Backup.Name = "nightly-2"
			return s
		}(), wantErr: ErrBackupStampConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.store(t)
			b, err := tt.existing.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, stampKey, bytes.NewReader(b), int64(len(b))); err != nil {
				t.Fatal(err)
			}
			if tt.dumpObj {
				if err := store.Put(ctx, "lion/nightly-1/@.json", strings.NewReader("{}"), 2); err != nil {
					t.Fatal(err)
				}
			}
			got, reused, err := writeBackupStamp(ctx, store, stampKey, recreated)
			stored, parseErr := backupmeta.ParseStamp(stampBytes(t, store, stampKey))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if !sameStampBytes(stored, tt.existing) {
					t.Fatalf("a refused write changed the stamp: %+v", stored)
				}
				return
			}
			if err != nil || reused || !sameStampBytes(got, recreated) || !sameStampBytes(stored, recreated) {
				t.Fatalf("got %+v reused=%t err=%v; stored %+v", got, reused, err, stored)
			}
			// The re-created backup finishes its own stamp normally.
			tmpl := recreated
			tmpl.ServerClock, tmpl.CreatedAt = backupmeta.ServerClock{}, backupmeta.MicroTime{}
			if _, err := finishBackupStamp(ctx, store, stampKey, tmpl, backupmeta.NewMicroTime(stampStart.Add(2*time.Hour))); err != nil {
				t.Fatalf("finish the re-created stamp: %v", err)
			}
		})
	}
}

// abandonedCleanupStore is a cleanup store that also serves the stamp's
// bytes, as the S3 store does.
type abandonedCleanupStore struct {
	*cleanupMemoryStore
	data map[string][]byte
}

func (s *abandonedCleanupStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.keys[key] {
		return nil, false, nil
	}
	return s.data[key], true, nil
}

func encryptForTest(t *testing.T, plain, passphrase []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	if _, err := backupcrypto.Encrypt(&out, bytes.NewReader(plain), passphrase); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// Review B1: artifact-cleanup in abandoned mode deletes what a backup that
// never succeeded left at its planned destination only when the stamp
// there names its UID; another backup's stamp or no stamp deletes nothing,
// and a stamp it cannot read refuses.
func TestArtifactCleanupAbandonedMode(t *testing.T) {
	ctx := context.Background()
	passphraseFile := filepath.Join(t.TempDir(), "passphrase")
	if err := os.WriteFile(passphraseFile, []byte("dump key"), 0o600); err != nil {
		t.Fatal(err)
	}
	stampFor := func(uid string) []byte {
		s := stampOf(uid, stampStart)
		s.Backup.Name = "backup-1"
		b, err := s.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	const key = "p/backup-1" + backupmeta.StampSuffix
	tests := []struct {
		name        string
		stamp       []byte // nil: no stamp
		passphrase  string
		uid         string
		wantDeleted int
		wantRemoved bool
		wantErr     string
	}{
		{name: "own stamp", stamp: stampFor("mine"), uid: "uid-mine", wantDeleted: 2, wantRemoved: true},
		{name: "another backup's stamp", stamp: stampFor("theirs"), uid: "uid-mine"},
		{name: "no stamp", uid: "uid-mine"},
		{name: "own encrypted stamp", stamp: encryptForTest(t, stampFor("mine"), []byte("dump key")), passphrase: passphraseFile, uid: "uid-mine", wantDeleted: 2, wantRemoved: true},
		{name: "plaintext stamp of an encrypted backup", stamp: stampFor("mine"), passphrase: passphraseFile, uid: "uid-mine", wantErr: "not BRV1"},
		{name: "wrong passphrase", stamp: encryptForTest(t, stampFor("mine"), []byte("other key")), passphrase: passphraseFile, uid: "uid-mine", wantErr: "decrypt backup stamp"},
		{name: "no uid", stamp: stampFor("mine"), wantErr: "BLOODRAVEN_BACKUP_UID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &abandonedCleanupStore{cleanupMemoryStore: &cleanupMemoryStore{keys: map[string]bool{
				"p/backup-1/@.json": true, "p/backup-1/chunk": true, "p/backup-10/@.json": true,
				"p/backup-10" + backupmeta.StampSuffix: true,
			}}, data: map[string][]byte{}}
			if tt.stamp != nil {
				store.keys[key], store.data[key] = true, tt.stamp
			}
			req := artifactCleanupRequest{Name: "backup-1", StorageType: "S3", Location: "p/backup-1", Bucket: "b",
				Mode: cleanupModeAbandoned, UID: tt.uid, PassphraseFile: tt.passphrase}
			n, cerr := runCleanupRequest(ctx, req, func(context.Context, *sidecar.PITRConfig) (cleanupStore, error) { return store, nil })
			if tt.wantErr != "" {
				if cerr == nil || cerr.code != backupmeta.CleanupExitRejected || !strings.Contains(cerr.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want a refusal containing %q", cerr, tt.wantErr)
				}
				if !store.keys["p/backup-1/@.json"] {
					t.Fatal("a refused cleanup deleted the dump")
				}
				return
			}
			if cerr != nil || n != tt.wantDeleted {
				t.Fatalf("deleted %d, err %v; want %d", n, cerr, tt.wantDeleted)
			}
			if store.keys["p/backup-1/@.json"] == tt.wantRemoved || store.keys[key] == tt.wantRemoved && tt.stamp != nil {
				t.Fatalf("removed=%t wanted, keys %v", tt.wantRemoved, store.keys)
			}
			if !store.keys["p/backup-10/@.json"] || !store.keys["p/backup-10"+backupmeta.StampSuffix] {
				t.Fatalf("a sibling backup was touched: %v", store.keys)
			}
		})
	}

	t.Run("PVC", func(t *testing.T) {
		for _, own := range []bool{true, false} {
			mount := t.TempDir()
			mustWrite(t, filepath.Join(mount, "sub", "backup-1", "@.json"))
			uid := "uid-theirs"
			if own {
				uid = "uid-mine"
			}
			if err := os.WriteFile(filepath.Join(mount, "sub", "backup-1"+backupmeta.StampSuffix), stampFor(strings.TrimPrefix(uid, "uid-")), 0o644); err != nil {
				t.Fatal(err)
			}
			req := artifactCleanupRequest{Name: "backup-1", StorageType: "PVC", Location: filepath.Join(mount, "sub", "backup-1"),
				MountPath: mount, Mode: cleanupModeAbandoned, UID: "uid-mine"}
			n, cerr := runCleanupRequest(ctx, req, nil)
			if cerr != nil {
				t.Fatal(cerr)
			}
			_, statErr := os.Stat(filepath.Join(mount, "sub", "backup-1", "@.json"))
			_, stampErr := os.Stat(filepath.Join(mount, "sub", "backup-1"+backupmeta.StampSuffix))
			if own != (n == 1 && os.IsNotExist(statErr) && os.IsNotExist(stampErr)) || !own && (statErr != nil || stampErr != nil) {
				t.Fatalf("own=%t: deleted %d, dump %v, stamp %v", own, n, statErr, stampErr)
			}
		}
	})

	t.Run("unknown mode", func(t *testing.T) {
		_, cerr := runCleanupRequest(ctx, artifactCleanupRequest{Name: "backup-1", StorageType: "S3", Mode: "everything"}, nil)
		if cerr == nil || cerr.code != backupmeta.CleanupExitRejected {
			t.Fatalf("err = %v, want a refusal", cerr)
		}
	})
}
