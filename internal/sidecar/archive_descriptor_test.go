package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// faultStore injects archive-session failures around a real store.
type faultStore struct {
	ArchiveStore
	putIfAbsentErr error
	listErr        error
	dropManifests  bool
}

func (s *faultStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.putIfAbsentErr != nil {
		return s.putIfAbsentErr
	}
	return PutIfAbsent(ctx, s.ArchiveStore, key, r, size)
}

func (s *faultStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.ArchiveStore.List(ctx, prefix)
}

func (s *faultStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.dropManifests && strings.Contains(key, "/manifest-") {
		_, err := io.Copy(io.Discard, r)
		return err
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

type forbiddenError struct{}

func (forbiddenError) Error() string       { return "api error AccessDenied: Access Denied" }
func (forbiddenError) HTTPStatusCode() int { return 403 }

func putDescriptor(t *testing.T, store ArchiveStore, source pitrarchive.Source, dest pitrarchive.Destination, enc pitrarchive.EncryptionIdentity, passphrase []byte) *pitrarchive.Descriptor {
	t.Helper()
	d, err := pitrarchive.NewDescriptor(source, dest, enc, passphrase, "other", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := d.Canonical()
	raw, _ := splitArchiveStore(store)
	if err := raw.Put(context.Background(), pitrarchive.DescriptorKey("binlogs"), bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
	return d
}

func passphraseIdentity() pitrarchive.EncryptionIdentity {
	return pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}
}

func TestArchiveSessionCreatesDescriptorOnEmptyPrefix(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			ctx := context.Background()
			inner := newMemStore()
			var store ArchiveStore = inner
			if encrypted {
				store = WrapWithEncryption(inner, []byte(" archive key\n"))
			}
			s, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", nil)
			if !errors.Is(err, ErrDescriptorAbsent) {
				t.Fatalf("open on an empty prefix = %v, want ErrDescriptorAbsent", err)
			}
			if st := s.Status(); st.State != pitrarchive.StateAwaitingDescriptor {
				t.Fatalf("status before create = %+v", st)
			}
			if err := s.EnsureCreated(ctx); err != nil {
				t.Fatal(err)
			}
			raw, ok, _ := inner.Get(ctx, "binlogs/archive.json")
			if !ok {
				t.Fatal("no descriptor written")
			}
			d, err := pitrarchive.ParseDescriptor(raw) // plaintext even on an encrypted store
			if err != nil {
				t.Fatalf("descriptor is not plaintext JSON: %v", err)
			}
			if d.ArchiveID != testArchiveID || d.Source != testArchiveSource || d.CreatedBy != "iad" {
				t.Fatalf("descriptor = %+v", d)
			}
			if encrypted && !backupcrypto.VerifyArchiveKeyCheck([]byte("archive key"), d.KeyCheckFields(), d.Encryption.KeyCheck) {
				t.Fatal("keyCheck does not verify the trimmed passphrase")
			}
			if st := s.Status(); st.State != pitrarchive.StateVerified || st.ArchiveID != testArchiveID || st.Destination != testArchiveDestination.String() {
				t.Fatalf("status after create = %+v", st)
			}
			for key := range inner.objects {
				if strings.Contains(key, pitrarchive.CASProbeKeyPrefix) {
					t.Fatalf("probe key %s left behind", key)
				}
			}
			// A second session verifies the same descriptor without writing.
			again, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "pdx", nil)
			if err != nil || again.ArchiveID() != testArchiveID {
				t.Fatalf("re-open = %v, %q", err, again.ArchiveID())
			}
		})
	}
}

func TestArchiveSessionRefusals(t *testing.T) {
	other := pitrarchive.Source{Namespace: "ns", Name: "other", UID: "00000000-0000-4000-8000-0000000000ff"}
	otherDest, err := pitrarchive.NewPVCDestination("ns", "copy", "")
	if err != nil {
		t.Fatal(err)
	}
	none := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}
	tests := []struct {
		name      string
		encrypted bool
		seed      func(t *testing.T, store ArchiveStore)
		want      pitrarchive.Token
	}{
		{name: "v1 manifest is legacy", seed: func(t *testing.T, store ArchiveStore) {
			body := `{"version":1,"site":"iad","files":[]}`
			_ = store.Put(context.Background(), "binlogs/manifest-iad.json", strings.NewReader(body), int64(len(body)))
		}, want: pitrarchive.TokenArchiveLegacy},
		{name: "stray binlog object is legacy", seed: func(t *testing.T, store ArchiveStore) {
			_ = store.Put(context.Background(), "binlogs/iad/mysql-bin.000001", strings.NewReader("x"), 1)
		}, want: pitrarchive.TokenArchiveLegacy},
		{name: "v2 manifest without descriptor", seed: func(t *testing.T, store ArchiveStore) {
			if err := saveManifest(context.Background(), store, "binlogs", &Manifest{Site: "iad", ArchiveID: testArchiveID}); err != nil {
				t.Fatal(err)
			}
		}, want: pitrarchive.TokenArchiveDescriptorMissing},
		{name: "encrypted v2 manifest without descriptor", encrypted: true, seed: func(t *testing.T, store ArchiveStore) {
			if err := saveManifest(context.Background(), store, "binlogs", &Manifest{Site: "iad", ArchiveID: testArchiveID}); err != nil {
				t.Fatal(err)
			}
		}, want: pitrarchive.TokenArchiveDescriptorMissing},
		{name: "foreign group", seed: func(t *testing.T, store ArchiveStore) {
			putDescriptor(t, store, other, testArchiveDestination, none, nil)
		}, want: pitrarchive.TokenArchiveForeign},
		{name: "copy from another destination", seed: func(t *testing.T, store ArchiveStore) {
			putDescriptor(t, store, testArchiveSource, otherDest, none, nil)
		}, want: pitrarchive.TokenArchiveCopy},
		{name: "plaintext archive, encrypted profile", encrypted: true, seed: func(t *testing.T, store ArchiveStore) {
			putDescriptor(t, store, testArchiveSource, testArchiveDestination, none, nil)
		}, want: pitrarchive.TokenArchiveEncryptionMismatch},
		{name: "encrypted archive, plaintext profile", seed: func(t *testing.T, store ArchiveStore) {
			putDescriptor(t, store, testArchiveSource, testArchiveDestination, passphraseIdentity(), []byte("k"))
		}, want: pitrarchive.TokenArchiveEncryptionMismatch},
		{name: "other passphrase", encrypted: true, seed: func(t *testing.T, store ArchiveStore) {
			putDescriptor(t, store, testArchiveSource, testArchiveDestination, passphraseIdentity(), []byte("a different key"))
		}, want: pitrarchive.TokenArchiveKeyMismatch},
		{name: "unparsable descriptor", seed: func(t *testing.T, store ArchiveStore) {
			_ = store.Put(context.Background(), "binlogs/archive.json", strings.NewReader("{"), 1)
		}, want: pitrarchive.TokenArchiveVersionUnsupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var store ArchiveStore = newMemStore()
			if tt.encrypted {
				store = WrapWithEncryption(store, []byte("archive key"))
			}
			tt.seed(t, store)
			before, _ := store.List(ctx, "")
			s, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", nil)
			if s == nil {
				t.Fatalf("no session: %v", err)
			}
			err = s.EnsureCreated(ctx)
			if token, ok := pitrarchive.TokenOf(err); !ok || token != tt.want {
				t.Fatalf("EnsureCreated = %v, want %s", err, tt.want)
			}
			if !strings.HasPrefix(err.Error(), string(tt.want)+": ") {
				t.Fatalf("message %q does not lead with its token", err)
			}
			wantReason, _ := tt.want.Reason()
			if st := s.Status(); st.State != pitrarchive.StateRefused || st.Reason != wantReason || st.ArchiveID != "" {
				t.Fatalf("status = %+v, want Refused/%s", st, wantReason)
			}
			after, _ := store.List(ctx, "")
			if len(after) != len(before) {
				t.Fatalf("a refused session wrote objects: %v -> %v", before, after)
			}
			if _, err := s.LoadManifest(ctx, "iad"); !errors.Is(err, ErrArchiveNotVerified) {
				t.Fatalf("LoadManifest on a refused session = %v", err)
			}
		})
	}
}

func TestArchiveSessionStoreFailures(t *testing.T) {
	tests := []struct {
		name          string
		store         *faultStore
		wantRefusal   pitrarchive.Token
		wantTransient string
	}{
		{name: "store without atomic create", store: &faultStore{ArchiveStore: plainOnlyStore{newMemStore()}}, wantRefusal: pitrarchive.TokenArchiveStoreUnsupported},
		{name: "store ignores conditional create", store: &faultStore{ArchiveStore: newMemStore(), putIfAbsentErr: ErrConditionalWriteUnsupported}, wantRefusal: pitrarchive.TokenArchiveStoreUnsupported},
		{name: "inconclusive probe", store: &faultStore{ArchiveStore: newMemStore(), putIfAbsentErr: fmt.Errorf("wrapped: %w", ErrConditionalConflict)}, wantTransient: "conditional write kept conflicting"},
		{name: "list denied", store: &faultStore{ArchiveStore: newMemStore(), listErr: forbiddenError{}}, wantTransient: "s3:ListBucket"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s, _ := OpenArchiveForWrite(ctx, tt.store, "binlogs", testArchiveIdentity(tt.store), "iad", nil)
			err := s.EnsureCreated(ctx)
			if tt.wantRefusal != "" {
				if token, ok := pitrarchive.TokenOf(err); !ok || token != tt.wantRefusal {
					t.Fatalf("EnsureCreated = %v, want %s", err, tt.wantRefusal)
				}
				return
			}
			if _, refused := pitrarchive.TokenOf(err); refused || err == nil || !strings.Contains(err.Error(), tt.wantTransient) {
				t.Fatalf("EnsureCreated = %v, want a transient error mentioning %q", err, tt.wantTransient)
			}
			if !s.transientFailure() || s.Status().State != pitrarchive.StateAwaitingDescriptor {
				t.Fatalf("transient=%t status=%+v", s.transientFailure(), s.Status())
			}
		})
	}
}

// Concurrent first writers produce one descriptor; the loser accepts it.
func TestArchiveSessionSiblingRace(t *testing.T) {
	ctx := context.Background()
	store, err := newPVCStore(testPVCConfig(t.TempDir()), "")
	if err != nil {
		t.Fatal(err)
	}
	const writers = 8
	errs := make([]error, writers)
	ids := make([]string, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := newArchiveSession(store, "binlogs", testArchiveIdentity(store), fmt.Sprintf("site%d", i), nil)
			if err != nil {
				errs[i] = err
				return
			}
			errs[i] = s.EnsureCreated(ctx)
			ids[i] = s.ArchiveID()
		}()
	}
	wg.Wait()
	for i := range writers {
		if errs[i] != nil || ids[i] != testArchiveID {
			t.Fatalf("writer %d: %v (%q)", i, errs[i], ids[i])
		}
	}
	keys, _ := store.List(ctx, "binlogs/")
	if len(keys) != 1 || keys[0] != "binlogs/archive.json" {
		t.Fatalf("keys after the race = %v", keys)
	}
}

func TestArchiveSessionReverifyGuardsSavesAndDeletes(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	s, err := testWriterSession(ctx, store, "binlogs")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "binlogs/iad/old", strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	m, err := s.LoadManifest(ctx, "iad")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.ArchiveID != testArchiveID {
		t.Fatalf("save did not stamp the archive ID: %q", m.ArchiveID)
	}

	// Another group's descriptor replaces ours: saves and deletes refuse.
	other := pitrarchive.Source{Namespace: "ns", Name: "other", UID: "00000000-0000-4000-8000-0000000000ff"}
	putDescriptor(t, store, other, testArchiveDestination, pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}, nil)
	if err := s.SaveManifest(ctx, m); err == nil || !strings.Contains(err.Error(), string(pitrarchive.TokenArchiveForeign)) {
		t.Fatalf("save after the descriptor changed = %v, want %s", err, pitrarchive.TokenArchiveForeign)
	}
	if err := s.DeleteObject(ctx, "binlogs/iad/old"); err == nil {
		t.Fatal("delete after the descriptor changed succeeded")
	}
	if _, ok, _ := store.Get(ctx, "binlogs/iad/old"); !ok {
		t.Fatal("refused delete removed the object")
	}
	if st := s.Status(); st.State != pitrarchive.StateRefused || st.Reason != pitrarchive.ReasonArchiveForeign {
		t.Fatalf("status = %+v", st)
	}

	// The descriptor disappears: DescriptorMissing.
	s2, err := testWriterSession(ctx, newMemStore(), "binlogs")
	if err != nil {
		t.Fatal(err)
	}
	_ = s2.store.Delete(ctx, "binlogs/archive.json")
	if err := s2.DeleteObject(ctx, "binlogs/iad/x"); err == nil || !strings.Contains(err.Error(), string(pitrarchive.TokenArchiveDescriptorMissing)) {
		t.Fatalf("delete after the descriptor vanished = %v", err)
	}
}

func TestArchiveSessionManifestChecks(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	s, err := testWriterSession(ctx, store, "binlogs")
	if err != nil {
		t.Fatal(err)
	}
	// A manifest stamped for another archive refuses.
	if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "pdx", ArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadManifest(ctx, "pdx"); !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("LoadManifest of another archive's manifest = %v", err)
	}

	// A stale copy never drops a known marker or promotion record.
	m, err := s.LoadManifest(ctx, "iad")
	if err != nil {
		t.Fatal(err)
	}
	stale := *m
	at := time.Date(2026, 11, 4, 1, 2, 3, 4000, time.UTC)
	m.AppendPromotionRecord(PromotionRecord{ServerUUID: "u", ServerID: 101, GTIDExecuted: "u:1-3", ObservedAt: at})
	m.HistoryIntegrity = &HistoryIntegrity{Violation: pitrarchive.ViolationBinlogFormat, Detail: "binlog_format=MIXED", At: at}
	if err := s.SaveManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	stale.Files = append(stale.Files, ManifestEntry{Name: "mysql-bin.000001"})
	if err := s.SaveManifest(ctx, &stale); err != nil {
		t.Fatal(err)
	}
	got, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if got.HistoryIntegrity == nil || len(got.Promotions) != 1 || len(got.Files) != 1 {
		t.Fatalf("stale save lost evidence: %+v", got)
	}

	// A save whose bytes do not read back fails.
	dropping := &faultStore{ArchiveStore: newMemStore(), dropManifests: true}
	ds, err := testWriterSession(ctx, dropping, "binlogs")
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.SaveManifest(ctx, &Manifest{Site: "iad"}); err == nil || !strings.Contains(err.Error(), "read back") {
		t.Fatalf("save without read-back = %v", err)
	}
}

// A replica never creates the descriptor; it re-opens every cycle and
// verifies once the writer has created it.
func TestReplicaArchiverAwaitsDescriptor(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	cfg := withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, store)
	replica := NewBinlogArchiver(cfg, "pdx", &stubRoleChecker{readOnly: true}, store, slog.Default())
	replica.scanAndArchive(ctx)
	st := replica.Snapshot(ctx)
	if st.Archive == nil || st.Archive.State != pitrarchive.StateAwaitingDescriptor || st.LastError != "" || st.StoreReady != nil {
		t.Fatalf("replica before the writer = %+v (archive %+v)", st, st.Archive)
	}
	if _, ok, _ := store.Get(ctx, "binlogs/archive.json"); ok {
		t.Fatal("a replica created the descriptor")
	}
	primary := NewBinlogArchiver(withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, store), "iad", &stubRoleChecker{}, store, slog.Default())
	primary.scanAndArchive(ctx)
	if st := primary.Snapshot(ctx); st.Archive.State != pitrarchive.StateVerified || st.LastError != "" {
		t.Fatalf("primary = %+v (archive %+v)", st, st.Archive)
	}
	replica.scanAndArchive(ctx)
	if st := replica.Snapshot(ctx); st.Archive.State != pitrarchive.StateVerified || st.Archive.ArchiveID != testArchiveID {
		t.Fatalf("replica after the writer = %+v", st.Archive)
	}
}

// A refused or unusable identity surfaces in status and blocks every write.
func TestArchiverReportsRefusedArchive(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	body := `{"version":1,"site":"iad","files":[]}`
	_ = store.Put(ctx, "binlogs/manifest-iad.json", strings.NewReader(body), int64(len(body)))
	a := NewBinlogArchiver(withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, store), "iad", &stubRoleChecker{}, store, slog.Default())
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if !strings.HasPrefix(st.LastError, string(pitrarchive.TokenArchiveLegacy)+": ") || st.Archive.State != pitrarchive.StateRefused ||
		st.Archive.Reason != pitrarchive.ReasonArchiveLegacy || st.StoreReady != nil {
		t.Fatalf("status = %+v (archive %+v)", st, st.Archive)
	}
	if _, ok, _ := store.Get(ctx, "binlogs/archive.json"); ok {
		t.Fatal("descriptor created over a legacy prefix")
	}

	bad := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, newMemStore(), slog.Default())
	bad.scanAndArchive(ctx)
	if st := bad.Snapshot(ctx); st.StoreReady == nil || *st.StoreReady || !strings.Contains(st.LastError, "archive identity") {
		t.Fatalf("archiver without identity = %+v", st)
	}

	denied := &faultStore{ArchiveStore: newMemStore(), listErr: forbiddenError{}}
	d := NewBinlogArchiver(withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, denied), "iad", &stubRoleChecker{}, denied, slog.Default())
	d.scanAndArchive(ctx)
	if st := d.Snapshot(ctx); st.StoreReady == nil || *st.StoreReady || !strings.HasPrefix(st.LastError, "archive store init: ") || !strings.Contains(st.LastError, "s3:ListBucket") {
		t.Fatalf("archiver with list denied = %+v", st)
	}
}

func TestOpenArchiveForRead(t *testing.T) {
	ctx := context.Background()
	reader := func(uid, id string) pitrarchive.Expectation {
		return pitrarchive.Expectation{Role: pitrarchive.RoleReader, SourceUID: types.UID(uid),
			ArchiveID: id, Destination: testArchiveDestination, Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}
	}
	tests := []struct {
		name   string
		seed   func(store ArchiveStore)
		expect pitrarchive.Expectation
		want   pitrarchive.Token // "" for success
	}{
		{name: "empty", seed: func(ArchiveStore) {}, expect: reader("", ""), want: pitrarchive.TokenArchiveNotFound},
		{name: "objects but no manifests", seed: func(s ArchiveStore) { _ = s.Put(ctx, "binlogs/iad/x", strings.NewReader("x"), 1) }, expect: reader("", ""), want: pitrarchive.TokenArchiveNotFound},
		{name: "v2 manifests", seed: func(s ArchiveStore) {
			_ = saveManifest(ctx, s, "binlogs", &Manifest{Site: "iad", ArchiveID: testArchiveID})
		}, expect: reader("", ""), want: pitrarchive.TokenArchiveDescriptorMissing},
		{name: "v1 manifests", seed: func(s ArchiveStore) {
			_ = s.Put(ctx, "binlogs/manifest-iad.json", strings.NewReader(`{"version":1}`), 13)
		}, expect: reader("", ""), want: pitrarchive.TokenArchiveLegacy},
		{name: "verified", seed: func(s ArchiveStore) { _ = seedTestDescriptor(ctx, s, "binlogs") }, expect: reader(string(testArchiveSource.UID), testArchiveID)},
		{name: "any reader accepts a copy", seed: func(s ArchiveStore) { _ = seedTestDescriptor(ctx, s, "binlogs") }, expect: reader("", "")},
		{name: "stamp names another archive", seed: func(s ArchiveStore) { _ = seedTestDescriptor(ctx, s, "binlogs") }, expect: reader("", "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa"), want: pitrarchive.TokenArchiveIdentityMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMemStore()
			tt.seed(store)
			d, err := OpenArchiveForRead(ctx, store, "binlogs", tt.expect)
			if tt.want == "" {
				if err != nil || d.ArchiveID != testArchiveID {
					t.Fatalf("OpenArchiveForRead = %+v, %v", d, err)
				}
				return
			}
			if token, ok := pitrarchive.TokenOf(err); !ok || token != tt.want {
				t.Fatalf("OpenArchiveForRead = %v, want %s", err, tt.want)
			}
		})
	}
	if _, err := OpenArchiveForRead(ctx, &faultStore{ArchiveStore: newMemStore(), listErr: forbiddenError{}}, "binlogs", reader("", "")); err == nil {
		t.Fatal("list failure was not refused")
	} else if token, _ := pitrarchive.TokenOf(err); token != pitrarchive.TokenArchiveListFailed {
		t.Fatalf("list failure = %v", err)
	}
}

func TestSnapshotCountsDivergentAndQuarantinedRows(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	m := &Manifest{Site: "pdx",
		Divergent:   []ArchiveDivergence{{Name: "a", MissingGTIDs: "u:1"}, {Name: "b", MissingGTIDs: "u:2"}},
		Quarantined: []ArchiveQuarantine{{Entry: ManifestEntry{Name: "c"}, Reason: "lost", ResumeGTIDs: "u:1-3"}},
	}
	if err := seedManifest(ctx, store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	a := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, "pdx", &stubRoleChecker{readOnly: true}, store, slog.Default())
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if st.DivergentRows != 2 || st.QuarantinedRows != 1 || st.Archive == nil || st.Archive.State != pitrarchive.StateVerified {
		t.Fatalf("status = %+v (archive %+v)", st, st.Archive)
	}
}
