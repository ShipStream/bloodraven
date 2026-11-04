package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// The operator debounces ArchiverError on archiver scans, so the
// sidecar must report how many scans in a row ended with an error.
func TestScanCountsConsecutiveFailedScans(t *testing.T) {
	checker := &stubRoleChecker{isReadOnlyErr: errors.New("mysql down")}
	cfg := &PITRConfig{
		StorageType:    "PVC",
		BinlogDir:      t.TempDir(),
		BinlogIndex:    "mysql-bin.index",
		ManifestPrefix: "binlogs",
		PollInterval:   time.Hour,
	}
	a := NewBinlogArchiver(cfg, "dc1", checker, newMemStore(), testLogger())
	ctx := context.Background()

	for want := int64(1); want <= 2; want++ {
		a.scanAndArchive(ctx)
		if got := a.Snapshot(ctx).ConsecutiveFailedScans; got != want {
			t.Fatalf("after %d failed scans: consecutiveFailedScans = %d", want, got)
		}
	}

	checker.mu.Lock()
	checker.isReadOnlyErr, checker.readOnly = nil, true
	checker.mu.Unlock()
	a.scanAndArchive(ctx)
	snap := a.Snapshot(ctx)
	if snap.ConsecutiveFailedScans != 0 || snap.LastError != "" {
		t.Fatalf("after a clean scan: consecutiveFailedScans = %d, lastError = %q", snap.ConsecutiveFailedScans, snap.LastError)
	}

	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["consecutiveFailedScans"]; !ok {
		t.Fatalf("consecutiveFailedScans omitted at 0; the operator reads absent as an older sidecar: %s", raw)
	}
}
