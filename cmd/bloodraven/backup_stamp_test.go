package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// stampTestStore is a PVC store on a temp dir; it creates atomically
// (link(2)).
func stampTestStore(t *testing.T) sidecar.ArchiveStore {
	t.Helper()
	store, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC",
		PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir(), Writable: true, MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// noAtomicCreateStore hides the inner store's PutIfAbsent, like a store
// without conditional writes.
type noAtomicCreateStore struct{ sidecar.ArchiveStore }

func stampTemplate() backupmeta.Stamp {
	return backupmeta.Stamp{
		Kind: backupmeta.StampKind, Version: backupmeta.StampVersion,
		Backup:        backupmeta.ObjectRef{Namespace: "ns", Name: "nightly-1", UID: "backup-uid"},
		Source:        backupmeta.ObjectRef{Namespace: "ns", Name: "lion", UID: "fg-uid"},
		PITRArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa",
		Site:          "pdx", ServerID: 102,
		DumpFilters: backupmeta.DumpFilters{ExcludeSchemas: []string{"scratch"}},
	}
}

func startedStamp(start time.Time) backupmeta.Stamp {
	s := stampTemplate()
	s.ServerClock.Start = backupmeta.NewMicroTime(start)
	s.CreatedAt = backupmeta.NewMicroTime(start.Add(time.Second))
	return s
}

func stampBytes(t *testing.T, store sidecar.ArchiveStore, key string) []byte {
	t.Helper()
	b, ok, err := store.Get(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("stamp %s: ok=%t err=%v", key, ok, err)
	}
	return b
}

const stampKey = "lion/nightly-1" + backupmeta.StampSuffix

var stampStart = time.Date(2026, 11, 4, 12, 0, 0, 123456000, time.UTC)

// D-8: the start step creates the stamp once, accepts its own retry, keeps a
// retried pod's earlier stamp, refuses another backup's stamp or a finished
// one, and falls back to Put plus read-back on a store without atomic create.
func TestWriteBackupStamp(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name       string
		store      func(*testing.T) sidecar.ArchiveStore
		existing   *backupmeta.Stamp
		desired    backupmeta.Stamp
		wantErr    error
		wantReused bool
		wantStart  time.Time
	}{
		{name: "atomic create", store: stampTestStore, desired: startedStamp(stampStart), wantStart: stampStart},
		{name: "no atomic create falls back to Put", store: func(t *testing.T) sidecar.ArchiveStore { return noAtomicCreateStore{stampTestStore(t)} }, desired: startedStamp(stampStart), wantStart: stampStart},
		{name: "identical retry", store: stampTestStore, existing: ptrStamp(startedStamp(stampStart)), desired: startedStamp(stampStart), wantReused: true, wantStart: stampStart},
		{name: "retried pod keeps the earlier start", store: stampTestStore, existing: ptrStamp(startedStamp(stampStart)), desired: startedStamp(stampStart.Add(time.Minute)), wantReused: true, wantStart: stampStart},
		// Another namespace's backup sharing the prefix may be live. (An
		// abandoned stamp of a same-name backup in this namespace is
		// replaced: TestWriteBackupStampReplacesAbandonedStamp.)
		{name: "another backup's stamp", store: stampTestStore, existing: func() *backupmeta.Stamp {
			s := startedStamp(stampStart)
			s.Backup.Namespace, s.Backup.UID = "other-ns", "other-uid"
			return &s
		}(), desired: startedStamp(stampStart), wantErr: ErrBackupStampConflict},
		{name: "another server", store: stampTestStore, existing: func() *backupmeta.Stamp {
			s := startedStamp(stampStart)
			s.ServerID = 101
			return &s
		}(), desired: startedStamp(stampStart), wantErr: ErrBackupStampConflict},
		{name: "a finished dump is never stamped again", store: stampTestStore, existing: func() *backupmeta.Stamp {
			s := startedStamp(stampStart)
			s.ServerClock.End = backupmeta.NewMicroTime(stampStart.Add(time.Hour))
			return &s
		}(), desired: startedStamp(stampStart), wantErr: ErrBackupStampConflict},
		{name: "invalid template", store: stampTestStore, desired: func() backupmeta.Stamp {
			s := startedStamp(stampStart)
			s.Source.UID = ""
			return s
		}(), wantErr: backupmeta.ErrStampInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.store(t)
			if tt.existing != nil {
				b, err := tt.existing.Canonical()
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Put(ctx, stampKey, bytes.NewReader(b), int64(len(b))); err != nil {
					t.Fatal(err)
				}
			}
			got, reused, err := writeBackupStamp(ctx, store, stampKey, tt.desired)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if reused != tt.wantReused || !got.ServerClock.Start.Equal(tt.wantStart) || got.Complete() {
				t.Fatalf("got %+v reused=%t", got, reused)
			}
			stored, err := backupmeta.ParseStamp(stampBytes(t, store, stampKey))
			if err != nil || !sameStampBytes(stored, got) {
				t.Fatalf("stored %+v (%v), returned %+v", stored, err, got)
			}
		})
	}
}

func ptrStamp(s backupmeta.Stamp) *backupmeta.Stamp { return &s }

// D-8/D-52: the end step adds serverClock.end and changes nothing else; a
// retry with the same end is a no-op, a different end, a missing stamp or
// another backup's stamp refuses. Until it runs the stamp has no end, which
// is how a failed dump stays incomplete.
func TestFinishBackupStamp(t *testing.T) {
	ctx := context.Background()
	end := backupmeta.NewMicroTime(stampStart.Add(42 * time.Minute))
	store := stampTestStore(t)
	if _, _, err := writeBackupStamp(ctx, store, stampKey, startedStamp(stampStart)); err != nil {
		t.Fatal(err)
	}
	before := stampBytes(t, store, stampKey)
	if s, _ := backupmeta.ParseStamp(before); s.Complete() {
		t.Fatal("a stamp before the end step must not be complete")
	}
	got, err := finishBackupStamp(ctx, store, stampKey, stampTemplate(), end)
	if err != nil {
		t.Fatal(err)
	}
	after := stampBytes(t, store, stampKey)
	want := startedStamp(stampStart)
	want.ServerClock.End = end
	if wb, _ := want.Canonical(); !bytes.Equal(after, wb) || !sameStampBytes(got, want) {
		t.Fatalf("finished stamp\n got %s\nwant %s", after, wb)
	}
	// Every field but the end is byte-identical.
	if !strings.Contains(string(after), `"end":"2026-11-04T12:42:00.123456Z"`) ||
		strings.Replace(string(after), `"end":"2026-11-04T12:42:00.123456Z"`, `"end":null`, 1) != string(before) {
		t.Fatalf("the rewrite changed more than serverClock.end:\n%s\n%s", before, after)
	}
	if _, err := finishBackupStamp(ctx, store, stampKey, stampTemplate(), end); err != nil {
		t.Fatalf("identical retry refused: %v", err)
	}
	if _, err := finishBackupStamp(ctx, store, stampKey, stampTemplate(), backupmeta.NewMicroTime(end.Add(time.Second))); !errors.Is(err, backupmeta.ErrStampRewriteConflict) {
		t.Fatalf("a different end: %v", err)
	}
	other := stampTemplate()
	other.Backup.UID = "other"
	if _, err := finishBackupStamp(ctx, store, stampKey, other, end); !errors.Is(err, ErrBackupStampConflict) {
		t.Fatalf("another backup's template: %v", err)
	}
	if _, err := finishBackupStamp(ctx, stampTestStore(t), stampKey, stampTemplate(), end); !errors.Is(err, ErrBackupStampConflict) {
		t.Fatalf("missing stamp: %v", err)
	}
	early := stampTestStore(t)
	if _, _, err := writeBackupStamp(ctx, early, stampKey, startedStamp(stampStart)); err != nil {
		t.Fatal(err)
	}
	if _, err := finishBackupStamp(ctx, early, stampKey, stampTemplate(), backupmeta.NewMicroTime(stampStart.Add(-time.Second))); !errors.Is(err, backupmeta.ErrStampInvalid) {
		t.Fatalf("an end before the start: %v", err)
	}
}

// The stamp of an encrypted dump is BRV1 ciphertext at rest and reads back
// through the dump key.
func TestBackupStampEncrypted(t *testing.T) {
	ctx := context.Background()
	raw := stampTestStore(t)
	store := sidecar.WrapWithEncryptionOptions(raw, []byte("dump key"), false)
	if _, _, err := writeBackupStamp(ctx, store, stampKey, startedStamp(stampStart)); err != nil {
		t.Fatal(err)
	}
	if _, err := finishBackupStamp(ctx, store, stampKey, stampTemplate(), backupmeta.NewMicroTime(stampStart.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if b := stampBytes(t, raw, stampKey); !bytes.HasPrefix(b, []byte("BRV1")) || bytes.Contains(b, []byte("BloodravenBackupStamp")) {
		t.Fatalf("stamp is not encrypted at rest: %q", b[:min(len(b), 40)])
	}
	if s, err := backupmeta.ParseStamp(stampBytes(t, store, stampKey)); err != nil || !s.Complete() {
		t.Fatalf("decrypted stamp %+v, %v", s, err)
	}
}

// The stamp key is the dump location plus the suffix, for both storage
// types, so cleanup and dump-meta find it beside the dump directory.
func TestBackupStampStoreConfig(t *testing.T) {
	t.Setenv("BLOODRAVEN_S3_BUCKET", "b")
	t.Setenv("BLOODRAVEN_PVC_MOUNT_PATH", "/backups")
	tests := []struct {
		storageType, outputURL, wantKey, wantWritePrefix string
	}{
		{"S3", "lion/nightly-1", "lion/nightly-1" + backupmeta.StampSuffix, ""},
		{"S3", "nightly-1/", "nightly-1" + backupmeta.StampSuffix, ""},
		{"PVC", "/backups/sub/nightly-1", "sub/nightly-1" + backupmeta.StampSuffix, "sub"},
		{"PVC", "/backups/nightly-1", "nightly-1" + backupmeta.StampSuffix, ""},
	}
	for _, tt := range tests {
		cfg, key, err := backupStampStoreConfig(tt.storageType, tt.outputURL)
		if err != nil || key != tt.wantKey {
			t.Fatalf("%s %s: key %q err %v, want %q", tt.storageType, tt.outputURL, key, err, tt.wantKey)
		}
		if cfg.PVC != nil && (!cfg.PVC.Writable || cfg.PVC.WritePrefix != tt.wantWritePrefix) {
			t.Fatalf("%s: PVC config %+v", tt.outputURL, cfg.PVC)
		}
	}
	if _, _, err := backupStampStoreConfig("PVC", "/elsewhere/nightly-1"); err == nil {
		t.Fatal("a location outside the mount was accepted")
	}
}

func TestBackupStampTemplateFromEnv(t *testing.T) {
	t.Setenv(backupStampTemplateEnv, `{"kind":"BloodravenBackupStamp","unknown":1}`)
	if _, err := backupStampTemplateFromEnv(); err == nil {
		t.Fatal("unknown template field accepted")
	}
	t.Setenv(backupStampTemplateEnv, "")
	if _, err := backupStampTemplateFromEnv(); err == nil {
		t.Fatal("missing template accepted")
	}
}
