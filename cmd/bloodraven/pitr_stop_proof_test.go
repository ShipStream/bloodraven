package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// A lagging replica's archive alone: a server-local rotation reaches the
// target before the replicated S:12 that committed at 09:20. mysqlbinlog
// would end at the rotation, so no override may certify or replay it.
func TestPITRSingleSiteLaggingReplicaStopRefused(t *testing.T) {
	r0 := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	r1 := round1PatchTimestamp(t, round1Stream(t, 11, round1Time(9, 0, 2), true, false), 3, uint32(round1Time(9, 35, 0).Unix()))
	r2 := round1Stream(t, 11, round1Time(9, 20, 0), false, false)
	r2 = round1PatchTimestamp(t, r2, 15, uint32(round1Time(9, 35, 0).Unix()))
	r2 = round1PatchTimestamp(t, r2, 3, uint32(round1Time(9, 41, 0).Unix()))
	// r1 latches on its rotation and r2 holds S:12; without r1, r2's own
	// FORMAT_DESCRIPTION is the stop event and S:12 follows it in the file.
	for _, files := range [][][]byte{{r0, r1, r2}, {r0, r2}} {
		store := round1Archive(t, map[string][][]byte{"pdx": files})
		for _, partial := range []bool{false, true} {
			result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, false, "timestamp", slog.Default())
			if err == nil || !strings.Contains(err.Error(), retentionTestSID+":12 committed 2026-04-15T09:20:01Z") || result.TargetCovered {
				t.Fatalf("lagging replica stop certified: %+v %v", result, err)
			}
		}
	}
}

// An anchored dump-covered stop is the frontier: later post-target files are
// read for proof but never staged, so the restore stays a covered idle window.
func TestPITRIdleBarrierDoesNotStageLaterFiles(t *testing.T) {
	old := round1PatchTimestamp(t, pitrFixture(t, "incarnation-a.bin"), 3, uint32(round1Time(9, 31, 0).Unix()))
	later := round1Stream(t, 11, round1Time(9, 40, 0), false, false)
	store := round1Archive(t, map[string][][]byte{"iad": {old, later}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-11", false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered || len(result.Sites[0].Staged) != 0 {
		t.Fatalf("idle barrier staged later files: %+v %v", result, err)
	}
	// A replica-era copy of that later file at another site still stages
	// nothing before the target, so it cannot make the window cross-site.
	store = round1Archive(t, map[string][][]byte{"iad": {old, later}, "pdx": {later}})
	result, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-11", false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered || result.CrossSiteUnverified {
		t.Fatalf("post-target copy made an idle window cross-site: %+v %v", result, err)
	}
}

// An old site's dump-covered file that only rotates after the target is not
// anchored and replays nothing, so it cannot veto the replaying site.
func TestPITRDumpCoveredUnanchoredSiteDoesNotVeto(t *testing.T) {
	stale := round1PatchTimestamp(t, round1Stream(t, 9, round1Time(8, 0, 0), false, false), 3, uint32(round1Time(9, 35, 0).Unix()))
	writer := round2Join(t, round2Retag(t, round1Stream(t, 10, round1Time(9, 10, 0), false, false), 1), round2Retag(t, round1Stream(t, 10, round1Time(9, 20, 0), false, false), 2))
	writer = round1PatchTimestamp(t, writer, 3, uint32(round1Time(9, 31, 0).Unix()))
	// The old site's history lacks bbbb:1, so its file is not anchored.
	bound := retentionTestSID + ":1-10,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1"
	store := round1Archive(t, map[string][][]byte{"aaa": {stale}, "pdx": {writer}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"aaa", "pdx"}, round1Time(9, 30, 0), t.TempDir(), bound, false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered || result.ProofSite != "pdx" || result.Sites[0].StopReached {
		t.Fatalf("stale dump-covered site vetoed coverage: %+v %v", result, err)
	}
}

// A later site whose first file starts after the target names, in its
// Previous_gtids, history the replaying site committed after the target.
// That history is explained by the archive, so it is not a lower-edge gap.
func TestPITRFutureOnlySiteDoesNotFailLowerEdge(t *testing.T) {
	iad := round2Join(t, round1Stream(t, 10, round1Time(9, 20, 0), false, false), round1Stream(t, 11, round1Time(9, 40, 0), false, false))
	pdx := round1Stream(t, 12, round1Time(9, 50, 0), false, false)
	store := round1Archive(t, map[string][][]byte{"iad": {iad}, "pdx": {pdx}})
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", false, false, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered || result.ProofSite != "iad" || len(result.Sites[1].Staged) != 0 {
		t.Fatalf("future-only site refused a covered window: %+v %v", result, err)
	}
	// Without the replaying site's post-target evidence the same gap is a
	// pre-target hole, and it still refuses.
	store = round1Archive(t, map[string][][]byte{"iad": {round1Stream(t, 10, round1Time(9, 20, 0), false, false)}, "pdx": {pdx}})
	_, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", true, false, "timestamp", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "archive does not contain transactions between the backup bound and") {
		t.Fatalf("unexplained predecessor gap certified: %v", err)
	}
}

// A transaction-bearing successor cannot anchor an unanchored barrier; the
// chain stays unanchored and coverage policy, not an integrity error, decides.
func TestPITRTransactionSuccessorLeavesBarrierUnanchored(t *testing.T) {
	file := round1Stream(t, 10, round1Time(9, 40, 0), false, false)
	successor := round1Stream(t, 11, round1Time(9, 50, 0), false, false)
	store := round1Archive(t, map[string][][]byte{"iad": {file, successor}})
	// The bound names a source this archive never carried.
	bound := retentionTestSID + ":1-10,cccccccc-bbbb-cccc-dddd-eeeeeeeeeeee:1"
	_, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), bound, false, false, "timestamp", slog.Default())
	if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || strings.Contains(err.Error(), "ambiguous transaction-bearing successor") {
		t.Fatalf("unanchored chain did not reach coverage policy: %v", err)
	}
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), bound, true, false, "timestamp", slog.Default())
	if err != nil || !result.Partial || result.TargetCovered {
		t.Fatalf("allowPartial could not complete an unanchored chain: %+v %v", result, err)
	}
}

// Explicit excludeGtids reach the planner exactly as they reach mysqlbinlog.
func TestPITRExplicitExclusionSkipsInnerCutVeto(t *testing.T) {
	raw := pitrFixture(t, "native/compressed-inner-cut.bin")
	idle := round5IdleAfter(t, raw, round1Time(9, 35, 0))
	sid := "b5e6ac37-d371-11f1-8b67-65731305df52"
	store := round1Archive(t, map[string][][]byte{"iad": {raw, idle}})
	request := pitrRequest{prefix: "binlogs", sites: []string{"iad"}, stop: round1Time(9, 30, 0), outDir: t.TempDir(), start: sid + ":1-8"}
	if _, err := planPITRArchive(context.Background(), store, request, slog.Default()); err == nil || !strings.Contains(err.Error(), sid+":9") {
		t.Fatalf("inner cut not refused without exclusion: %v", err)
	}
	request.outDir, request.exclude = t.TempDir(), sid+":9"
	result, err := planPITRArchive(context.Background(), store, request, slog.Default())
	if err != nil || !result.TargetCovered {
		t.Fatalf("excluded inner cut still refused: %+v %v", result, err)
	}
}

// Excluded transactions are not reported as replayed.
func TestPITRNewestReplayTimeSkipsExcludedTransactions(t *testing.T) {
	raw := round2Join(t, round1Stream(t, 10, round1Time(9, 0, 0), false, false), round1Stream(t, 11, round1Time(9, 10, 0), false, false))
	excluded, err := internalmysql.ParseGTIDSet(retentionTestSID + ":12")
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := sidecar.InspectBinlogForReplay(bytes.NewReader(raw), round1Time(9, 30, 0), excluded)
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.NewestReplayEventTime.Equal(round1Time(9, 0, 1)) {
		t.Fatalf("newest replay time counted an excluded transaction: %s", evidence.NewestReplayEventTime)
	}
}

// An earlier inner-cut refusal must not mask a later corrupt event.
func TestPITRPayloadRefusalDoesNotMaskCorruption(t *testing.T) {
	raw := pitrFixture(t, "native/compressed-inner-cut.bin")
	corrupt := append(bytes.Clone(raw), round4Events(raw)[len(round4Events(raw))-1][:10]...)
	store := round1Archive(t, map[string][][]byte{"iad": {raw}})
	m, err := sidecar.LoadManifest(context.Background(), store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	key, err := sidecar.ArchiveEntryKey("binlogs", "iad", m.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), key, bytes.NewReader(corrupt), int64(len(corrupt))); err != nil {
		t.Fatal(err)
	}
	sid := "b5e6ac37-d371-11f1-8b67-65731305df52"
	_, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad"}, round1Time(9, 30, 0), t.TempDir(), sid+":1-8", true, true, "timestamp", slog.Default())
	if err == nil || strings.Contains(err.Error(), "PITRTargetNotCovered") || !strings.Contains(err.Error(), "preserve objects") {
		t.Fatalf("payload refusal masked corruption: %v", err)
	}
}
