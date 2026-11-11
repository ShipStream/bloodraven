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

// failOnceStore fails the first Put whose key starts with failPrefix.
type failOnceStore struct {
	ArchiveStore
	failPrefix string
	failed     bool
}

func (s *failOnceStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if !s.failed && strings.HasPrefix(key, s.failPrefix) {
		s.failed = true
		_, _ = io.Copy(io.Discard, r)
		return fmt.Errorf("injected transient S3 503 SlowDown")
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

func copyBinlogFixture(t *testing.T, src, dst string) {
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
// the next one, must not make the retry look like a "numbering restart behind
// archived execution" and persist a permanent epoch.resetReason on an ordinary
// history.
func TestTransientUploadFailureDoesNotSetResetGuard(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	copyBinlogFixture(t, "testdata/review-order-a.bin", filepath.Join(dir, "mysql-bin.000010")) // P=1-10 T=11
	copyBinlogFixture(t, "testdata/review-order-b.bin", filepath.Join(dir, "mysql-bin.000011")) // P=1-11 T=12
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000010\nmysql-bin.000011\nmysql-bin.000012\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &failOnceStore{ArchiveStore: newMemStore(), failPrefix: "binlogs/iad/mysql-bin.000010."}
	state := &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-12"}
	a := newTestArchiver(&PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
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

// Clean reclone (same server_uuid, donor had every archived transaction), a
// replica period, then promotion: new names upload first, then the historical
// cursor publishes the new incarnation's reused low names. The guard fires.
func TestRecloneThenPromotionDoesNotSetResetGuard(t *testing.T) {
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
	arch := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	donor := a + ":1-100," + d + ":1-10" // gtid_purged after CLONE (donor had a:1-100)
	// New incarnation: 000001 (replica, d:11-30), 000002 (replica, d:31-60),
	// 000003 (promoted, a:101-110). 000003 is a new name -> foreground first.
	n3 := ManifestEntry{Name: "mysql-bin.000003", ID: strings.Repeat("6", 64), PreviousGTIDs: d + ":11-60", BaseGTIDs: donor, GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: a + ":101-110"}}
	if err := arch.publishBinlog(ctx, n3); err != nil {
		t.Fatal(err)
	}
	// Historical cursor later publishes the reused name 000002 of the new incarnation.
	n2 := ManifestEntry{Name: "mysql-bin.000002", ID: strings.Repeat("5", 64), PreviousGTIDs: d + ":11-30", BaseGTIDs: donor, GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: d + ":31-60"}}
	if err := arch.publishBinlog(ctx, n2); err != nil {
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
