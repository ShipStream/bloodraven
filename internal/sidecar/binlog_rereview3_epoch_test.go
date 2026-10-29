package sidecar

import (
	"context"
	"log/slog"
	"testing"
)

func TestRereview3OlderUploadRetryIsNotReset(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	state := &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-12"}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	// The older upload failed transiently; its successor was archived successfully.
	for _, f := range []struct{ path, name string }{
		{"testdata/review-order-b.bin", "mysql-bin.000002"},
		{"testdata/review-order-a.bin", "mysql-bin.000001"},
	} {
		id, err := a.localBinlogID(f.path)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.archiveOne(ctx, f.path, f.name, id); err != nil {
			t.Fatal(err)
		}
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		t.Fatalf("ordinary out-of-order retry permanently disables PITR: %s", m.Epoch.ResetReason)
	}
}
