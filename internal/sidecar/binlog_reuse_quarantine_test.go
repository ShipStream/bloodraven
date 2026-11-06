package sidecar

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
)

// openErrStore answers Open of one key with a fixed error, as an S3 or PVC
// backend would for a lost or unreadable object.
type openErrStore struct {
	*memStore
	key string
	err error
}

func (s *openErrStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == s.key {
		return nil, s.err
	}
	return s.memStore.Open(ctx, key)
}

// A quarantine moves metadata only, so reissuing a quarantined row's GTIDs
// with different content is still reuse (D-36, O1). A quarantined row whose
// object is gone (PVC ENOENT or S3 NoSuchKey) has no bytes to prove reuse
// with and is skipped; any other read failure fails the archive closed.
func TestReuseDetectionComparesQuarantinedRows(t *testing.T) {
	tests := []struct {
		name      string
		openErr   error // nil keeps the quarantined object readable
		wantReuse bool
		wantErr   bool
	}{
		{name: "quarantined object kept", wantReuse: true},
		{name: "quarantined object lost (pvc)", openErr: os.ErrNotExist},
		{name: "quarantined object lost (s3)", openErr: &smithy.GenericAPIError{Code: "NoSuchKey", Message: "The specified key does not exist."}},
		{name: "quarantined object unreadable", openErr: &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mem := newMemStore()
			store := &openErrStore{memStore: mem}
			a := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
			first := filepath.Join("testdata", "incarnation-a.bin")
			firstID, err := a.localBinlogID(first)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.archiveOne(ctx, first, "mysql-bin.000001", firstID); err != nil {
				t.Fatal(err)
			}
			if err := testQuarantine(ctx, mem, "binlogs", "iad", "mysql-bin.000001", firstID, "lost at failover", coverageTestSID+":1-10", false); err != nil {
				t.Fatal(err)
			}
			if tt.openErr != nil {
				key, err := ArchiveEntryKey("binlogs", "iad", ManifestEntry{Name: "mysql-bin.000001", ID: firstID})
				if err != nil {
					t.Fatal(err)
				}
				store.key, store.err = key, tt.openErr
			}
			// The same GTID 11, issued again with different statement bytes.
			second := filepath.Join("testdata", "same-header.bin")
			secondID, err := a.localBinlogID(second)
			if err != nil {
				t.Fatal(err)
			}
			err = a.archiveOne(ctx, second, "mysql-bin.000002", secondID)
			if tt.wantErr {
				if err == nil {
					t.Fatal("an unreadable quarantined object was skipped; want the archive to fail closed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			m, err := loadManifest(ctx, mem, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			if !tt.wantReuse {
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
