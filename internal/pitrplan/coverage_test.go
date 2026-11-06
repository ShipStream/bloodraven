package pitrplan

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// TestCoverageJSONShape pins the §4.12 key set that restore.py reads.
func TestCoverageJSONShape(t *testing.T) {
	at := backupmeta.NewMicroTime(time.Date(2026, 11, 4, 20, 1, 2, 123456000, time.UTC))
	c := Coverage{
		Mode: ModeTimestamp, StopTime: at, Outcome: OutcomeTargetCovered,
		Archive: CoverageArchive{ArchiveID: "ar1-abcdefghijklmnopqrstuvwxyz", Destination: "s3://b/binlogs?endpoint=aws",
			Source: pitrarchive.Source{Namespace: "db", Name: "orders", UID: "u"}, EncryptionMode: pitrarchive.EncryptionNone},
		Dump:    CoverageDump{Location: "l", BackupUID: "b", SourceUID: "u", Site: "b", ServerID: 102, End: at},
		Witness: &CoverageWitness{GTID: "u:11", GroundTruth: GroundTruthAnchored, Anchor: &CoverageAnchor{}},
		Head:    &CoverageHead{Site: "a"},
	}
	b, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for k := range top {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	want := []string{"allowPartial", "archive", "backupNewestCommitTime", "baseline", "divergent", "dump", "excluded",
		"files", "head", "lineage", "lost", "mode", "newestReplayedCommitTime", "onHistoryExclusions", "outcome",
		"postStop", "refusalReason", "replay", "replayedTransactionCount", "segments", "sites", "stopTime", "version", "witness"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("coverage keys\n got %v\nwant %v", keys, want)
	}
	for k, v := range map[string]string{"version": "2", "files": "[]", "segments": "[]", "sites": "[]",
		"backupNewestCommitTime": "null", "stopTime": `"2026-11-04T20:01:02.123456Z"`, "mode": `"timestamp"`} {
		if string(top[k]) != v {
			t.Errorf("%s = %s, want %s", k, top[k], v)
		}
	}
	if !strings.Contains(string(b), `"records": []`) {
		t.Errorf("anchor records not normalized to []: %s", b)
	}
	var back Coverage
	if err := json.Unmarshal(b, &back); err != nil || back.Witness.GroundTruth != GroundTruthAnchored || !back.StopTime.Equal(at.Time) {
		t.Fatalf("round trip = %+v, %v", back, err)
	}
}
