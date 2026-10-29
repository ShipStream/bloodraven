package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func TestRereview4LegacyAliasMustKeepDurableDivergence(t *testing.T) {
	ctx := context.Background()
	data := pitrFixture(t, "incarnation-b.bin")
	store, rows := round2Archive(t, data)
	legacy := rows[0]
	legacy.ID = ""
	legacy.ContentID = ""
	key, err := sidecar.ArchiveEntryKey("binlogs", "iad", legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{legacy, rows[0]}, Divergent: []sidecar.ArchiveDivergence{{Name: legacy.Name, MissingGTIDs: retentionTestSID + ":21"}}})
	n, err := downloadPITRWindow(ctx, store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), t.TempDir(), "", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "divergent") {
		t.Fatalf("audited divergent legacy bytes replayed through ID-aware alias: n=%d err=%v", n, err)
	}
}

func TestRereview4UnrecordedLegacyReuseCanOmitNewTransaction(t *testing.T) {
	// These are the same two streams used by the real archiver-side probe:
	// incarnation-a and same-header issue different queries as GTID 11.
	store, rows := round2Archive(t, pitrFixture(t, "incarnation-a.bin"), pitrFixture(t, "same-header.bin"))
	raw := pitrFixture(t, "incarnation-a.bin")
	rows[0].ID = ""
	key, err := sidecar.ArchiveEntryKey("binlogs", "iad", rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: rows})
	n, err := downloadPITRWindow(context.Background(), store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), t.TempDir(), retentionTestSID+":1-11", slog.Default())
	if err == nil {
		t.Fatalf("pre-reuse backup silently accepted: n=%d err=%v; replacement GTID 11 query omitted", n, err)
	}
}

func TestRereview4AliasSafetyInEveryResolutionOrder(t *testing.T) {
	for _, known := range []bool{false, true} {
		for _, legacyFirst := range []bool{false, true} {
			for _, late := range []bool{false, true} {
				t.Run(fmt.Sprintf("known=%t/legacyFirst=%t/late=%t", known, legacyFirst, late), func(t *testing.T) {
					ctx := context.Background()
					data := pitrFixture(t, "incarnation-b.bin")
					store, rows := round2Archive(t, data)
					legacy := rows[0]
					legacy.ID = ""
					if !known {
						legacy.ContentID = ""
					} else {
						legacy.ContentID = rows[0].ID
					}
					if late {
						legacy.PreviousGTIDs = retentionTestSID + ":1-100"
					}
					key, err := sidecar.ArchiveEntryKey("binlogs", "iad", legacy)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
						t.Fatal(err)
					}
					entries := []sidecar.ManifestEntry{rows[0], legacy}
					if legacyFirst {
						entries[0], entries[1] = entries[1], entries[0]
					}
					round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: entries, Divergent: []sidecar.ArchiveDivergence{{Name: legacy.Name, MissingGTIDs: retentionTestSID + ":21"}}})
					_, err = downloadPITRWindow(ctx, store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), t.TempDir(), "", slog.Default())
					if err == nil || !strings.Contains(err.Error(), "divergent") {
						t.Fatalf("alias lost safety: %v", err)
					}
				})
			}
		}
	}
}
