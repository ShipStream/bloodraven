package sidecar

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// A quarantine moves metadata only, so reissuing a quarantined row's GTIDs
// with different content is still reuse (D-36, O1).
func TestReuseDetectionComparesQuarantinedRows(t *testing.T) {
	for _, objectLost := range []bool{false, true} {
		name := map[bool]string{false: "quarantined object kept", true: "quarantined object lost"}[objectLost]
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()
			a := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
			first := filepath.Join("testdata", "incarnation-a.bin")
			firstID, err := a.localBinlogID(first)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.archiveOne(ctx, first, "mysql-bin.000001", firstID); err != nil {
				t.Fatal(err)
			}
			if err := testQuarantine(ctx, store, "binlogs", "iad", "mysql-bin.000001", firstID, "lost at failover", coverageTestSID+":1-10", false); err != nil {
				t.Fatal(err)
			}
			if objectLost {
				key, err := ArchiveEntryKey("binlogs", "iad", ManifestEntry{Name: "mysql-bin.000001", ID: firstID})
				if err != nil {
					t.Fatal(err)
				}
				_ = store.Delete(ctx, key)
			}
			// The same GTID 11, issued again with different statement bytes.
			second := filepath.Join("testdata", "same-header.bin")
			secondID, err := a.localBinlogID(second)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.archiveOne(ctx, second, "mysql-bin.000002", secondID); err != nil {
				t.Fatal(err)
			}
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			if objectLost {
				if m.Epoch != nil && m.Epoch.ResetReason != "" {
					t.Fatalf("reuse recorded without the quarantined bytes: %+v", m.Epoch)
				}
				return
			}
			if m.Epoch == nil || len(m.Epoch.Reuse) != 1 || !strings.Contains(m.Epoch.ResetReason, "verified GTID namespace reuse") {
				t.Fatalf("reissue of a quarantined row's GTIDs was not detected: %+v", m.Epoch)
			}
			if m.Epoch.Reuse[0].First.ID != firstID || m.Epoch.Reuse[0].Second.ID != secondID {
				t.Fatalf("evidence = %+v", m.Epoch.Reuse[0])
			}
		})
	}
}
