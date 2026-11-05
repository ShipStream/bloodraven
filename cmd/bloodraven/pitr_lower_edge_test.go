package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

// P1: single-site archive whose first file starts after the backup bound
// (S:11 never archived). The restore cannot replay S:11.
func TestPITRLowerEdgeStartGapSingleSite(t *testing.T) {
	f := patchEventTimestamp(t, coverageStream(t, 11, coverageTime(9, 10, 0), false, false), 3, uint32(coverageTime(9, 31, 0).Unix()))
	store := coverageArchive(t, map[string][][]byte{"iad": {f}})
	requireLowerGapRefused(t, store, []string{"iad"})
}

// P2: two sites; pdx's first file names S:12 in its predecessor (clone base or
// purged index) but nobody archived S:12's bytes.
func TestPITRLowerEdgeStartGapMultiSite(t *testing.T) {
	a := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	p := joinBinlogFixtures(t, retagGTIDFixture(t, coverageStream(t, 12, coverageTime(9, 10, 0), false, false), 1), retagGTIDFixture(t, coverageStream(t, 12, coverageTime(9, 40, 0), false, false), 2))
	store := coverageArchive(t, map[string][][]byte{"iad": {a}, "pdx": {p}})
	requireLowerGapRefused(t, store, []string{"iad", "pdx"})
}

// P5: ordinary single failover; target BEFORE the failover; both the old
// primary and the promoted replica's replica-era archive reach the target.
func TestPITRLowerEdgeTargetBeforeFailoverBothCover(t *testing.T) {
	a := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	b := patchEventTimestamp(t, coverageStream(t, 11, coverageTime(9, 20, 0), false, false), 3, uint32(coverageTime(9, 35, 0).Unix()))
	r2 := patchEventTimestamp(t, coverageStream(t, 11, coverageTime(9, 20, 0), false, false), 3, uint32(coverageTime(9, 41, 0).Unix()))
	p := retagGTIDFixture(t, coverageStream(t, 12, coverageTime(9, 50, 0), false, false), 1)
	store := coverageArchive(t, map[string][][]byte{"iad": {a, b}, "pdx": {a, r2, p}})
	requireCrossSiteOrderGuard(t, store)
}

func TestPITRLowerEdgeFailbackIntermediateEra(t *testing.T) {
	// iad X: prev S:1-10, S:11 08:50 (own), then replicated bbbb:1 09:10, bbbb:2 09:40, rotate 10:00.
	x := joinBinlogFixtures(t, coverageStream(t, 10, coverageTime(8, 50, 0), false, false),
		retagGTIDFixture(t, coverageStream(t, 11, coverageTime(9, 10, 0), false, false), 1),
		retagGTIDFixture(t, coverageStream(t, 11, coverageTime(9, 40, 0), false, false), 2))
	x = patchEventTimestamp(t, x, 3, uint32(coverageTime(10, 0, 0).Unix()))
	// Replica-era S:11 followed by B:1, sealed after the target.
	r := patchEventTimestamp(t, coverageStream(t, 10, coverageTime(8, 50, 0), false, false), 3, uint32(coverageTime(9, 0, 0).Unix()))
	p1 := patchEventTimestamp(t, retagGTIDFixture(t, coverageStream(t, 11, coverageTime(9, 10, 0), false, false), 1), 3, uint32(coverageTime(9, 41, 0).Unix()))
	store := coverageArchive(t, map[string][][]byte{"iad": {x}, "pdx": {r, p1}})
	requireCrossSiteOrderGuard(t, store)
	requireCrossSiteOrderGuardBound(t, store, retentionTestSID+":1-11")
}

func TestPITRLaggingReplicaLocalStopRefused(t *testing.T) {
	a := coverageStream(t, 10, coverageTime(9, 0, 0), false, false) // S:11 09:00:01, rotate 09:00:02
	r0 := a                                                         // pdx replica-era copy, sealed 09:00:02
	// pdx R1: FDE 09:00:02 (local), no transactions applied yet (lagging),
	// sealed by a restart at 09:35 local.
	r1 := patchEventTimestamp(t, coverageStream(t, 11, coverageTime(9, 0, 2), true, false), 3, uint32(coverageTime(9, 35, 0).Unix()))
	// pdx R2: FDE 09:35 local, replicated S:12 with original time 09:20, rotate at promotion 09:41.
	r2 := coverageStream(t, 11, coverageTime(9, 20, 0), false, false)
	r2 = patchEventTimestamp(t, r2, 15, uint32(coverageTime(9, 35, 0).Unix()))
	r2 = patchEventTimestamp(t, r2, 3, uint32(coverageTime(9, 41, 0).Unix()))
	// pdx primary era.
	p := joinBinlogFixtures(t, retagGTIDFixture(t, coverageStream(t, 12, coverageTime(9, 42, 0), false, false), 1), retagGTIDFixture(t, coverageStream(t, 12, coverageTime(9, 50, 0), false, false), 2))
	store := coverageArchive(t, map[string][][]byte{"iad": {a}, "pdx": {r0, r1, r2, p}})
	// pdx's rotation at 09:35 ends replay before S:12 (09:20), so no override
	// may certify or replay this window.
	for _, partial := range []bool{false, true} {
		for _, unverified := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, unverified, "timestamp", slog.Default())
			if err == nil || !strings.Contains(err.Error(), "replay would not apply it") || result.TargetCovered {
				t.Fatalf("lagging replica stop accepted: %+v %v", result, err)
			}
		}
	}
}
func requireCrossSiteOrderGuard(t *testing.T, store sidecar.ArchiveStore) {
	t.Helper()
	requireCrossSiteOrderGuardBound(t, store, retentionTestSID+":1-10")
}
func requireCrossSiteOrderGuardBound(t *testing.T, store sidecar.ArchiveStore, bound string) {
	t.Helper()
	for _, partial := range []bool{false, true} {
		for _, unverified := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), bound, partial, unverified, "timestamp", slog.Default())
			if !unverified {
				if err == nil || !strings.Contains(err.Error(), "PITRCrossSiteOrder") {
					t.Fatalf("ambiguous streams accepted by default: %+v %v", result, err)
				}
			} else {
				if err != nil || !result.CrossSiteUnverified {
					t.Fatalf("explicit unverified replay refused or mislabeled: %+v %v", result, err)
				}
				for _, site := range result.Sites {
					if len(site.Staged) == 0 {
						t.Fatal("replay site silently removed")
					}
				}
			}
		}
	}
}

func requireLowerGapRefused(t *testing.T, store sidecar.ArchiveStore, sites []string) {
	t.Helper()
	for _, partial := range []bool{false, true} {
		for _, unverified := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", sites, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, unverified, "timestamp", slog.Default())
			if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || result.TargetCovered || !strings.Contains(err.Error(), "archive does not contain transactions between the backup bound and ") {
				t.Fatalf("missing lower history accepted: %+v %v", result, err)
			}
		}
	}
}

func TestPITRLowerGapCannotUseSkippedSite(t *testing.T) {
	first := patchEventTimestamp(t, coverageStream(t, 10, coverageTime(9, 10, 0), false, false), 3, uint32(coverageTime(9, 31, 0).Unix()))
	missing := coverageStream(t, 11, coverageTime(9, 20, 0), false, false)
	later := patchEventTimestamp(t, coverageStream(t, 12, coverageTime(9, 25, 0), false, false), 3, uint32(coverageTime(9, 35, 0).Unix()))
	store := coverageArchive(t, map[string][][]byte{"iad": {first}, "pdx": {missing, later}})
	for _, partial := range []bool{false, true} {
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, true, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || !strings.Contains(err.Error(), "archive does not contain transactions between the backup bound and") {
			t.Fatalf("skipped-site bytes supplied lower proof: %+v %v", result, err)
		}
	}
}

func TestPITRInterruptedBodyCannotCertifyOtherSite(t *testing.T) {
	old := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	interrupted := patchEventTimestamp(t, old, 2, uint32(coverageTime(9, 30, 0).Unix()))
	interrupted = patchEventTimestamp(t, interrupted, 3, uint32(coverageTime(9, 31, 0).Unix()))
	store := coverageArchive(t, map[string][][]byte{"iad": {interrupted}, "pdx": {old}})
	for _, partial := range []bool{false, true} {
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, true, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || result.TargetCovered {
			t.Fatalf("announcement certified a missing body: %+v %v", result, err)
		}
	}
}

// A foreign writer's archive never reached the backup bound, so replaying it
// would apply another lineage on top of the dump.
func TestPITRForeignWriterCannotUsePartialForMissingLowerHistory(t *testing.T) {
	file := patchEventTimestamp(t, coverageStream(t, 10, coverageTime(9, 10, 0), false, true), 3, uint32(coverageTime(9, 31, 0).Unix()))
	store := coverageArchive(t, map[string][][]byte{"iad": {file}})
	for _, partial := range []bool{false, true} {
		for _, mode := range []string{"timestamp", "latest"} {
			if mode == "latest" && partial {
				continue
			}
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, false, mode, slog.Default())
			if err == nil || !strings.Contains(err.Error(), "replayed history does not continue the backup bound") || result.TargetCovered {
				t.Fatalf("foreign lineage replayed: %+v %v", result, err)
			}
		}
	}
}

func TestPITROverridesRemainIndependent(t *testing.T) {
	file := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	store := coverageArchive(t, map[string][][]byte{"iad": {file}, "pdx": {file}})
	for _, partial := range []bool{false, true} {
		for _, cross := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, cross, "timestamp", slog.Default())
			if partial && cross {
				if err != nil || !result.Partial || !result.CrossSiteUnverified || result.TargetCovered {
					t.Fatalf("combined explicit outcome lost: %+v %v", result, err)
				}
			} else {
				want := "PITRCrossSiteOrder"
				if cross {
					want = "PITRTargetNotCovered"
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("one override bypassed another rule: %+v %v", result, err)
				}
			}
		}
	}
}
