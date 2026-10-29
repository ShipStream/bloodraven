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
func TestRereview3StartGapSingleSite(t *testing.T) {
	f := round1PatchTimestamp(t, round1Stream(t, 11, round1Time(9, 10, 0), false, false), 3, uint32(round1Time(9, 31, 0).Unix()))
	store := round1Archive(t, map[string][][]byte{"iad": {f}})
	round3CheckLowerGap(t, store, []string{"iad"})
}

// P2: two sites; pdx's first file names S:12 in its predecessor (clone base or
// purged index) but nobody archived S:12's bytes.
func TestRereview3StartGapMultiSite(t *testing.T) {
	a := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	p := round2Join(t, round2Retag(t, round1Stream(t, 12, round1Time(9, 10, 0), false, false), 1), round2Retag(t, round1Stream(t, 12, round1Time(9, 40, 0), false, false), 2))
	store := round1Archive(t, map[string][][]byte{"iad": {a}, "pdx": {p}})
	round3CheckLowerGap(t, store, []string{"iad", "pdx"})
}

// P5: ordinary single failover; target BEFORE the failover; both the old
// primary and the promoted replica's replica-era archive reach the target.
func TestRereview3TargetBeforeFailoverBothCover(t *testing.T) {
	a := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	b := round1PatchTimestamp(t, round1Stream(t, 11, round1Time(9, 20, 0), false, false), 3, uint32(round1Time(9, 35, 0).Unix()))
	r2 := round1PatchTimestamp(t, round1Stream(t, 11, round1Time(9, 20, 0), false, false), 3, uint32(round1Time(9, 41, 0).Unix()))
	p := round2Retag(t, round1Stream(t, 12, round1Time(9, 50, 0), false, false), 1)
	store := round1Archive(t, map[string][][]byte{"iad": {a, b}, "pdx": {a, r2, p}})
	round3CheckCrossSite(t, store)
}

func TestRereview3FailbackIntermediateEra(t *testing.T) {
	// iad X: prev S:1-10, S:11 08:50 (own), then replicated bbbb:1 09:10, bbbb:2 09:40, rotate 10:00.
	x := round2Join(t, round1Stream(t, 10, round1Time(8, 50, 0), false, false),
		round2Retag(t, round1Stream(t, 11, round1Time(9, 10, 0), false, false), 1),
		round2Retag(t, round1Stream(t, 11, round1Time(9, 40, 0), false, false), 2))
	x = round1PatchTimestamp(t, x, 3, uint32(round1Time(10, 0, 0).Unix()))
	// Replica-era S:11 followed by B:1, sealed after the target.
	r := round1PatchTimestamp(t, round1Stream(t, 10, round1Time(8, 50, 0), false, false), 3, uint32(round1Time(9, 0, 0).Unix()))
	p1 := round1PatchTimestamp(t, round2Retag(t, round1Stream(t, 11, round1Time(9, 10, 0), false, false), 1), 3, uint32(round1Time(9, 41, 0).Unix()))
	store := round1Archive(t, map[string][][]byte{"iad": {x}, "pdx": {r, p1}})
	round3CheckCrossSite(t, store)
	round3CheckCrossSiteBound(t, store, retentionTestSID+":1-11")
}

func TestRereview3LaggingReplicaLocalStop(t *testing.T) {
	a := round1Stream(t, 10, round1Time(9, 0, 0), false, false) // S:11 09:00:01, rotate 09:00:02
	r0 := a                                                     // pdx replica-era copy, sealed 09:00:02
	// pdx R1: FDE 09:00:02 (local), no transactions applied yet (lagging),
	// sealed by a restart at 09:35 local.
	r1 := round1PatchTimestamp(t, round1Stream(t, 11, round1Time(9, 0, 2), true, false), 3, uint32(round1Time(9, 35, 0).Unix()))
	// pdx R2: FDE 09:35 local, replicated S:12 with original time 09:20, rotate at promotion 09:41.
	r2 := round1Stream(t, 11, round1Time(9, 20, 0), false, false)
	r2 = round1PatchTimestamp(t, r2, 15, uint32(round1Time(9, 35, 0).Unix()))
	r2 = round1PatchTimestamp(t, r2, 3, uint32(round1Time(9, 41, 0).Unix()))
	// pdx primary era.
	p := round2Join(t, round2Retag(t, round1Stream(t, 12, round1Time(9, 42, 0), false, false), 1), round2Retag(t, round1Stream(t, 12, round1Time(9, 50, 0), false, false), 2))
	store := round1Archive(t, map[string][][]byte{"iad": {a}, "pdx": {r0, r1, r2, p}})
	round3CheckCrossSite(t, store)
}
func round3CheckCrossSite(t *testing.T, store sidecar.ArchiveStore) {
	t.Helper()
	round3CheckCrossSiteBound(t, store, retentionTestSID+":1-10")
}
func round3CheckCrossSiteBound(t *testing.T, store sidecar.ArchiveStore, bound string) {
	t.Helper()
	for _, partial := range []bool{false, true} {
		for _, unverified := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), t.TempDir(), bound, partial, unverified, "timestamp", slog.Default())
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

func round3CheckLowerGap(t *testing.T, store sidecar.ArchiveStore, sites []string) {
	t.Helper()
	for _, partial := range []bool{false, true} {
		for _, unverified := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", sites, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, unverified, "timestamp", slog.Default())
			if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || result.TargetCovered || !strings.Contains(err.Error(), "archive does not contain transactions between the backup bound and ") {
				t.Fatalf("missing lower history accepted: %+v %v", result, err)
			}
		}
	}
}

func TestRound3LowerGapCannotUseSkippedSite(t *testing.T) {
	first := round1PatchTimestamp(t, round1Stream(t, 10, round1Time(9, 10, 0), false, false), 3, uint32(round1Time(9, 31, 0).Unix()))
	missing := round1Stream(t, 11, round1Time(9, 20, 0), false, false)
	later := round1PatchTimestamp(t, round1Stream(t, 12, round1Time(9, 25, 0), false, false), 3, uint32(round1Time(9, 35, 0).Unix()))
	store := round1Archive(t, map[string][][]byte{"iad": {first}, "pdx": {missing, later}})
	for _, partial := range []bool{false, true} {
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, true, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || !strings.Contains(err.Error(), "archive does not contain transactions between the backup bound and") {
			t.Fatalf("skipped-site bytes supplied lower proof: %+v %v", result, err)
		}
	}
}

func TestRound3InterruptedBodyCannotCertifyOtherSite(t *testing.T) {
	old := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	interrupted := round1PatchTimestamp(t, old, 2, uint32(round1Time(9, 30, 0).Unix()))
	interrupted = round1PatchTimestamp(t, interrupted, 3, uint32(round1Time(9, 31, 0).Unix()))
	store := round1Archive(t, map[string][][]byte{"iad": {interrupted}, "pdx": {old}})
	for _, partial := range []bool{false, true} {
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, true, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || result.TargetCovered {
			t.Fatalf("announcement certified a missing body: %+v %v", result, err)
		}
	}
}

func TestRound3ForeignWriterCannotUsePartialForMissingLowerHistory(t *testing.T) {
	file := round1PatchTimestamp(t, round1Stream(t, 10, round1Time(9, 10, 0), false, true), 3, uint32(round1Time(9, 31, 0).Unix()))
	store := round1Archive(t, map[string][][]byte{"iad": {file}})
	round3CheckLowerGap(t, store, []string{"iad"})
}

func TestRound3OverridesRemainIndependent(t *testing.T) {
	file := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	store := round1Archive(t, map[string][][]byte{"iad": {file}, "pdx": {file}})
	for _, partial := range []bool{false, true} {
		for _, cross := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, cross, "timestamp", slog.Default())
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
