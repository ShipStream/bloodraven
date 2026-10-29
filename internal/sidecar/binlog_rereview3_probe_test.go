package sidecar

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// rr3FailOnceStore fails the first Put whose key starts with failPrefix.
type rr3FailOnceStore struct {
	ArchiveStore
	failPrefix string
	failed     bool
}

func (s *rr3FailOnceStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if !s.failed && strings.HasPrefix(key, s.failPrefix) {
		s.failed = true
		_, _ = io.Copy(io.Discard, r)
		return fmt.Errorf("injected transient S3 503 SlowDown")
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

func rr3Copy(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// A transient failure of an older sealed file, followed by successful upload of
// the next one, makes the retry look like a "numbering restart behind archived
// execution" and persists a permanent epoch.resetReason on an ordinary history.
func TestRR3ProbeTransientUploadFailureSetsResetGuard(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rr3Copy(t, "testdata/review-order-a.bin", filepath.Join(dir, "mysql-bin.000010")) // P=1-10 T=11
	rr3Copy(t, "testdata/review-order-b.bin", filepath.Join(dir, "mysql-bin.000011")) // P=1-11 T=12
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000010\nmysql-bin.000011\nmysql-bin.000012\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &rr3FailOnceStore{ArchiveStore: newMemStore(), failPrefix: "binlogs/iad/mysql-bin.000010."}
	state := &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-12"}
	a := NewBinlogArchiver(&PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	a.scanAndArchive(ctx) // 000010 fails, 000011 succeeds
	a.scanAndArchive(ctx) // 000010 retried
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 2 {
		t.Fatalf("expected both files archived, got %+v", m.Files)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		t.Fatalf("ordinary retry persisted a permanent reset guard: %s (lastError=%q)", m.Epoch.ResetReason, a.Snapshot(ctx).LastError)
	}
}

// The R13 repair path (missing legacy object replaced from surviving local
// bytes) for any row that is not the newest name trips the same guard.
func TestRR3ProbeLegacyRepairSetsResetGuard(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rr3Copy(t, "testdata/review-order-a.bin", filepath.Join(dir, "mysql-bin.000010"))
	rr3Copy(t, "testdata/review-order-b.bin", filepath.Join(dir, "mysql-bin.000011"))
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000010\nmysql-bin.000011\nmysql-bin.000012\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store := newMemStore()
	idA, err := fileContentID(filepath.Join(dir, "mysql-bin.000010"))
	if err != nil {
		t.Fatal(err)
	}
	metaA, err := parseBinlogMetadata(filepath.Join(dir, "mysql-bin.000010"))
	if err != nil {
		t.Fatal(err)
	}
	state := &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-12"}
	a := NewBinlogArchiver(&PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	// Legacy row for 000010 with a recorded content ID whose object is gone.
	if err := appendManifestEntry(ctx, store, "binlogs", "iad", ManifestEntry{Name: "mysql-bin.000010", ContentID: idA, Size: metaA.Size, PreviousGTIDs: metaA.PreviousGTIDs, FirstEventTime: metaA.FirstEventTime, LastEventTime: metaA.LastEventTime}); err != nil {
		t.Fatal(err)
	}
	// 000011 already archived normally (ID-aware) through the real path.
	idB, err := fileContentID(filepath.Join(dir, "mysql-bin.000011"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, filepath.Join(dir, "mysql-bin.000011"), "mysql-bin.000011", idB); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		a.scanAndArchive(ctx)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("files=%d", len(m.Files))
	for _, f := range m.Files {
		t.Logf("row %s id=%q contentId=%q", f.Name, f.ID, f.ContentID)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		t.Fatalf("R13 repair persisted a permanent reset guard: %s", m.Epoch.ResetReason)
	}
}

// Clean reclone (same server_uuid, donor had every archived transaction), a
// replica period, then promotion: new names upload first, then the historical
// cursor publishes the new incarnation's reused low names. The guard fires.
func TestRR3ProbeRecloneThenPromotionSetsResetGuard(t *testing.T) {
	ctx := context.Background()
	a := coverageTestSID // the recloned site's own server_uuid
	d := "dddddddd-bbbb-cccc-dddd-eeeeeeeeeeee"
	store := newMemStore()
	old := []ManifestEntry{
		{Name: "mysql-bin.000001", ID: strings.Repeat("1", 64), PreviousGTIDs: "", GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: a + ":1-50"}},
		{Name: "mysql-bin.000002", ID: strings.Repeat("2", 64), PreviousGTIDs: a + ":1-50", GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: a + ":51-100"}},
	}
	for _, row := range old {
		if err := appendManifestEntry(ctx, store, "binlogs", "iad", row); err != nil {
			t.Fatal(err)
		}
	}
	state := &historyStateStub{uuid: a, executed: a + ":1-110," + d + ":1-60"}
	arch := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	donor := a + ":1-100," + d + ":1-10" // gtid_purged after CLONE (donor had a:1-100)
	// New incarnation: 000001 (replica, d:11-30), 000002 (replica, d:31-60),
	// 000003 (promoted, a:101-110). 000003 is a new name -> foreground first.
	n3 := ManifestEntry{Name: "mysql-bin.000003", ID: strings.Repeat("6", 64), PreviousGTIDs: d + ":11-60", BaseGTIDs: donor, GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: a + ":101-110"}}
	if err := arch.publishBinlog(ctx, n3, false); err != nil {
		t.Fatal(err)
	}
	// Historical cursor later publishes the reused name 000002 of the new incarnation.
	n2 := ManifestEntry{Name: "mysql-bin.000002", ID: strings.Repeat("5", 64), PreviousGTIDs: d + ":11-30", BaseGTIDs: donor, GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: d + ":31-60"}}
	if err := arch.publishBinlog(ctx, n2, false); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		t.Fatalf("clean reclone + promotion persisted a permanent reset guard: %s", m.Epoch.ResetReason)
	}
}

type rr3OpenFailOnce struct {
	ArchiveStore
	key    string
	failed bool
}

func (s *rr3OpenFailOnce) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == s.key && !s.failed {
		s.failed = true
		return nil, fmt.Errorf("injected transient S3 GET 500")
	}
	return s.ArchiveStore.Open(ctx, key)
}

// Post-upgrade legacy verification: one transient GET error on an older legacy
// row (object intact) re-uploads the local bytes as a sibling row and trips the
// permanent reset guard.
func TestRR3ProbeLegacyVerificationTransientGetSetsResetGuard(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rr3Copy(t, "testdata/review-order-a.bin", filepath.Join(dir, "mysql-bin.000010"))
	rr3Copy(t, "testdata/review-order-b.bin", filepath.Join(dir, "mysql-bin.000011"))
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000010\nmysql-bin.000011\nmysql-bin.000012\n"), 0600); err != nil {
		t.Fatal(err)
	}
	base := newMemStore()
	store := &rr3OpenFailOnce{ArchiveStore: base, key: "binlogs/iad/mysql-bin.000010"}
	state := &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-12"}
	for _, name := range []string{"mysql-bin.000010", "mysql-bin.000011"} {
		path := filepath.Join(dir, name)
		data, _ := os.ReadFile(path)
		meta, err := parseBinlogMetadata(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := base.Put(ctx, "binlogs/iad/"+name, strings.NewReader(string(data)), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		if err := appendManifestEntry(ctx, base, "binlogs", "iad", ManifestEntry{Name: name, RemotePath: "binlogs/iad/" + name, Size: meta.Size, PreviousGTIDs: meta.PreviousGTIDs, FirstEventTime: meta.FirstEventTime, LastEventTime: meta.LastEventTime}); err != nil {
			t.Fatal(err)
		}
	}
	a := NewBinlogArchiver(&PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	for i := 0; i < 4; i++ {
		a.scanAndArchive(ctx)
	}
	m, err := loadManifest(ctx, base, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 2 {
		t.Fatalf("transient GET manufactured a sibling upload: %+v", m.Files)
	}
	for _, f := range m.Files {
		t.Logf("row %s id=%.12s contentId=%.12s", f.Name, f.ID, f.ContentID)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		t.Fatalf("transient GET during legacy verification persisted a permanent reset guard: %s", m.Epoch.ResetReason)
	}
}
