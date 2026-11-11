package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// A manifest row without a content identity is refused at every way a
// manifest enters this release (#271): nothing recovers, verifies or
// repairs such a row any more, so no code path may read around it.

// identitylessManifests are version-2 manifests, carrying the archive's
// ID, that hold one row with no id: each kind of row, and the two shapes
// earlier releases repaired (an ID recoverable from the object key, and a
// recorded content digest).
func identitylessManifests(archiveID string) map[string]string {
	id := strings.Repeat("a", 64)
	doc := func(body string) string {
		return fmt.Sprintf(`{"version":2,"archiveId":%q,"site":"iad",%s}`, archiveID, body)
	}
	return map[string]string{
		"active row":                doc(`"files":[{"name":"mysql-bin.000001","remotePath":"binlogs/iad/mysql-bin.000001","size":1}]`),
		"row with a stripped id":    doc(`"files":[{"name":"mysql-bin.000001","remotePath":"binlogs/iad/mysql-bin.000001.` + id + `","size":1}]`),
		"row with a content digest": doc(`"files":[{"name":"mysql-bin.000001","contentId":"` + id + `","remotePath":"binlogs/iad/mysql-bin.000001","size":1}]`),
		"one row among identified":  doc(`"files":[{"name":"mysql-bin.000001","id":"` + id + `"},{"name":"mysql-bin.000002"}]`),
		"quarantined row":           doc(`"files":[],"quarantined":[{"entry":{"name":"mysql-bin.000001"},"reason":"lost","resumeGtids":"u:1-3"}]`),
		"retirement record":         doc(`"files":[],"pruned":[{"name":"mysql-bin.000001","contentId":"` + id + `","key":"binlogs/iad/mysql-bin.000001","deleted":true}]`),
	}
}

func putRawManifest(t *testing.T, store ArchiveStore, body string) {
	t.Helper()
	if err := store.Put(context.Background(), manifestKey("binlogs", "iad"), strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
}

func assertIdentitylessRefusal(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("%s: %v, want ErrManifestInvalid", what, err)
	}
	if tok, ok := pitrarchive.ParseToken(err.Error()); !ok || tok != pitrarchive.TokenArchiveObjectInvalid {
		t.Fatalf("%s: %q does not start with %s", what, err, pitrarchive.TokenArchiveObjectInvalid)
	}
	if !strings.Contains(err.Error(), "has no id") {
		t.Fatalf("%s: %q does not name the missing id", what, err)
	}
}

// Readers (pitr-download, the standby reader) load through LoadManifest;
// writers (the archiver, pitr-quarantine, retention) through the session.
func TestIdentitylessRowRefusedAtLoad(t *testing.T) {
	ctx := context.Background()
	for name, body := range identitylessManifests(testArchiveID) {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			putRawManifest(t, store, body)
			_, err := LoadManifest(ctx, store, "binlogs", "iad")
			assertIdentitylessRefusal(t, "LoadManifest", err)

			session, err := testWriterSession(ctx, store, "binlogs")
			if err != nil {
				t.Fatal(err)
			}
			_, err = session.LoadManifest(ctx, "iad")
			assertIdentitylessRefusal(t, "session.LoadManifest", err)
			err = QuarantineArchive(ctx, session, "iad", "mysql-bin.000001", strings.Repeat("a", 64), "abandoned", "u:1-3", false)
			assertIdentitylessRefusal(t, "QuarantineArchive", err)
			err = AcknowledgeArchiveHistory(ctx, session, "iad", "canonical backup", "u:1-3")
			assertIdentitylessRefusal(t, "AcknowledgeArchiveHistory", err)
			_, err = pruneManifest(ctx, session, "iad", map[binlogIdentity]string{{Name: "mysql-bin.000001"}: ""})
			assertIdentitylessRefusal(t, "pruneManifest", err)
			if got, _, _ := store.Get(ctx, manifestKey("binlogs", "iad")); string(got) != body {
				t.Fatalf("a refused manifest was rewritten:\n%s", got)
			}
		})
	}
}

// The archiver reports the refusal and writes nothing: no upload, no
// manifest save, no repair of the row.
func TestIdentitylessRowStopsTheArchiver(t *testing.T) {
	ctx := context.Background()
	payload, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range identitylessManifests(testArchiveID) {
		for _, readOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/readOnly=%v", name, readOnly), func(t *testing.T) {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), payload, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000001\nmysql-bin.000002\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				store := &identityCountingStore{ArchiveStore: newMemStore()}
				if err := seedTestDescriptor(ctx, store, "binlogs"); err != nil {
					t.Fatal(err)
				}
				// The legacy layout's object, which earlier releases read.
				if err := store.ArchiveStore.Put(ctx, "binlogs/iad/mysql-bin.000001", bytes.NewReader(payload), int64(len(payload))); err != nil {
					t.Fatal(err)
				}
				putRawManifest(t, store.ArchiveStore, body)
				state := &historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: readOnly}, uuid: coverageTestSID, executed: coverageTestSID + ":1-11"}
				cfg := &PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}
				a := newTestArchiver(cfg, "iad", state, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
				store.puts = 0
				for i := 0; i < 3; i++ {
					a.scanAndArchive(ctx)
				}
				st := a.Snapshot(ctx)
				if tok, ok := pitrarchive.ParseToken(st.LastError); !ok || tok != pitrarchive.TokenArchiveObjectInvalid || !strings.Contains(st.LastError, "has no id") {
					t.Fatalf("lastError %q, want a leading %s naming the row", st.LastError, pitrarchive.TokenArchiveObjectInvalid)
				}
				if st.UploadFailures == 0 || st.FilesArchived != 0 || store.puts != 0 {
					t.Fatalf("uploadFailures %d, filesArchived %d, puts %d", st.UploadFailures, st.FilesArchived, store.puts)
				}
				if got, _, _ := store.Get(ctx, manifestKey("binlogs", "iad")); string(got) != body {
					t.Fatalf("the archiver rewrote a refused manifest:\n%s", got)
				}
			})
		}
	}
}

// A manifest this release would refuse to load is never saved either.
func TestIdentitylessRowRefusedAtSave(t *testing.T) {
	ctx := context.Background()
	id := strings.Repeat("a", 64)
	for name, m := range map[string]*Manifest{
		"active row":        {Site: "iad", Files: []ManifestEntry{{Name: "mysql-bin.000001"}}},
		"quarantined row":   {Site: "iad", Quarantined: []ArchiveQuarantine{{Entry: ManifestEntry{Name: "mysql-bin.000001"}, Reason: "lost", ResumeGTIDs: "u:1"}}},
		"retirement record": {Site: "iad", Pruned: []PrunedEntry{{Name: "mysql-bin.000001", Key: "binlogs/iad/mysql-bin.000001", Deleted: true}}},
	} {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			session, err := testWriterSession(ctx, store, "binlogs")
			if err != nil {
				t.Fatal(err)
			}
			good := &Manifest{Site: "iad", Files: []ManifestEntry{{Name: "mysql-bin.000009", ID: id}}}
			if err := session.SaveManifest(ctx, good); err != nil {
				t.Fatal(err)
			}
			before, _, _ := store.Get(ctx, manifestKey("binlogs", "iad"))
			for what, err := range map[string]error{
				"session.SaveManifest": session.SaveManifest(ctx, m),
				"saveManifest":         saveManifest(ctx, store, "binlogs", m),
			} {
				if !errors.Is(err, ErrManifestInvalid) || !strings.Contains(err.Error(), "refusing to save") || !strings.Contains(err.Error(), "has no id") {
					t.Fatalf("%s: %v", what, err)
				}
			}
			if after, _, _ := store.Get(ctx, manifestKey("binlogs", "iad")); !bytes.Equal(before, after) {
				t.Fatalf("a refused save changed the manifest:\n%s", after)
			}
		})
	}
}

// With no descriptor, such a manifest is not a version-2 manifest this
// release reads: the prefix is legacy content for writers, readers and the
// one-shot probe, never an archive whose descriptor is merely missing.
func TestIdentitylessManifestWithoutDescriptorIsLegacy(t *testing.T) {
	ctx := context.Background()
	none := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}
	reader := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: testArchiveDestination, Encryption: none}
	writer := pitrarchive.Expectation{Role: pitrarchive.RoleWriter, SourceUID: testArchiveSource.UID, Destination: testArchiveDestination, Encryption: none}
	for name, body := range identitylessManifests(testArchiveID) {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			putRawManifest(t, store, body)
			token := func(err error) pitrarchive.Token {
				tok, _ := pitrarchive.TokenOf(err)
				return tok
			}
			_, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", nil)
			if token(err) != pitrarchive.TokenArchiveLegacy {
				t.Fatalf("writer: %v, want %s", err, pitrarchive.TokenArchiveLegacy)
			}
			if _, err := OpenArchiveForRead(ctx, store, "binlogs", reader); token(err) != pitrarchive.TokenArchiveLegacy {
				t.Fatalf("reader: %v, want %s", err, pitrarchive.TokenArchiveLegacy)
			}
			for _, expect := range []pitrarchive.Expectation{reader, writer} {
				probe, err := ProbeArchive(ctx, store, "binlogs", expect)
				if err != nil || probe.State != ArchiveProbeRefused || probe.Token != pitrarchive.TokenArchiveLegacy {
					t.Fatalf("probe as %s: %+v, %v, want %s", expect.Role, probe, err, pitrarchive.TokenArchiveLegacy)
				}
			}
			// A writable sidecar does not establish an archive over it.
			a := NewBinlogArchiver(withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, store), "iad", &stubRoleChecker{}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
			a.scanAndArchive(ctx)
			if st := a.Snapshot(ctx); st.Archive == nil || st.Archive.State != pitrarchive.StateRefused || st.Archive.Reason != pitrarchive.ReasonArchiveLegacy {
				t.Fatalf("archiver status: %+v (archive %+v)", st, st.Archive)
			}
			if _, ok, _ := store.Get(ctx, pitrarchive.DescriptorKey("binlogs")); ok {
				t.Fatal("a descriptor was created over legacy content")
			}
		})
	}
}

// The planner's view refuses the same rows, should a Manifest reach it
// without passing a loader.
func TestIdentitylessRowRefusedByPlanSite(t *testing.T) {
	for name, m := range map[string]*Manifest{
		"active row":      {Site: "iad", Files: []ManifestEntry{{Name: "mysql-bin.000001"}}},
		"quarantined row": {Site: "iad", Quarantined: []ArchiveQuarantine{{Entry: ManifestEntry{Name: "mysql-bin.000001"}}}},
	} {
		if _, err := PlanSite(m); err == nil || !strings.HasPrefix(err.Error(), string(pitrarchive.TokenArchiveObjectInvalid)) || !strings.Contains(err.Error(), "has no id") {
			t.Errorf("%s: PlanSite = %v", name, err)
		}
	}
}

// The fixtures above are what a decoder sees: a contentId on a row is not
// an identity.
func TestIdentitylessFixturesDecode(t *testing.T) {
	for name, body := range identitylessManifests(testArchiveID) {
		var m Manifest
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := checkManifestRows("k", &m); err == nil {
			t.Errorf("%s: fixture holds no identity-less row", name)
		}
	}
}
