package main

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"log/slog"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func TestRereview4QuarantineOnlyRestrictsAffectedWindow(t *testing.T) {
	stop := time.Date(2026, 4, 15, 9, 30, 0, 0, time.UTC)
	first := pitrFixture(t, "incarnation-a.bin")
	for off := 4; off < len(first); {
		size := int(binary.LittleEndian.Uint32(first[off+9 : off+13]))
		if first[off+4] == 3 {
			binary.LittleEndian.PutUint32(first[off:off+4], uint32(stop.Add(time.Second).Unix()))
			binary.LittleEndian.PutUint32(first[off+size-4:off+size], crc32.ChecksumIEEE(first[off:off+size-4]))
		}
		off += size
	}
	store, rows := round2Archive(t, first, pitrFixture(t, "incarnation-b.bin"))
	if err := sidecar.QuarantineArchive(context.Background(), store, "binlogs", "iad", rows[1].Name, rows[1].ID, "lost future row", retentionTestSID+":1-30", false); err != nil {
		t.Fatal(err)
	}
	key, err := sidecar.ArchiveEntryKey("binlogs", "iad", rows[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	n, err := downloadPITRWindow(context.Background(), store, "binlogs", "iad", stop, t.TempDir(), retentionTestSID+":1-10", slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("unrelated quarantine blocked the older replay window: %d %v", n, err)
	}
	if _, err := downloadPITRWindow(context.Background(), store, "binlogs", "iad", time.Now(), t.TempDir(), retentionTestSID+":1-10", slog.Default()); err == nil {
		t.Fatal("needed quarantined row was silently omitted")
	}
}
