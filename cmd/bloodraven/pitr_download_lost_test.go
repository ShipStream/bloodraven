package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// lostTailArchive: a commits A:1-95, then A:96-98 and, 300 s later, A:99 in
// one file, then A:100-101; b was promoted without A:96-99 between A:98 and
// A:99. With A:96-99 excluded, a's second file holds nothing to replay and
// its manifest row cannot date its transactions against b's record: the
// loss report (coverage.json's lost) reads it.
func lostTailArchive(t *testing.T) (a *dlArchive, straddling string) {
	t.Helper()
	a = &dlArchive{store: stampTestStore(t), dest: downloadDest(t), manifests: map[string]*sidecar.Manifest{}}
	a.desc = putDescriptor(t, a.store, a.dest)
	for _, s := range []string{"a", "b"} {
		a.manifests[s] = &sidecar.Manifest{Version: 2, ArchiveID: a.desc.ArchiveID, Site: s}
	}
	at := func(s int) time.Time { return dlEpoch.Add(time.Duration(s) * time.Second) }
	a.manifests["a"].AppendPromotionRecord(pitrarchive.PromotionRecord{ServerUUID: sidA, ServerID: 101, ObservedAt: dlEpoch.Add(-time.Minute)})
	a.manifests["b"].AppendPromotionRecord(pitrarchive.PromotionRecord{ServerUUID: sidB, ServerID: 102, GTIDExecuted: sidA + ":1-95", ObservedAt: at(200)})
	a.put(t, "a", "mysql-bin.000001", 101, "", "", dlTxns(sidA, 1, 95, at(1), 101))
	e := a.put(t, "a", "mysql-bin.000002", 101, sidA+":1-95", "", dlTxns(sidA, 96, 98, at(96), 101), dlTxns(sidA, 99, 99, at(400), 101))
	a.put(t, "a", "mysql-bin.000003", 101, sidA+":1-99", "", dlTxns(sidA, 100, 101, at(401), 101))
	a.save(t)
	key, err := sidecar.ArchiveEntryKey(downloadPrefix, "a", e)
	if err != nil {
		t.Fatal(err)
	}
	return a, key
}

// A read for the loss report that fails never fails the restore: the plan
// is staged as it is without the read, coverage.json's lost lacks the
// row's transactions, and the log names the row. The failures are the real
// fetcher's: a store error that outlasts its retries, and a missing object.
func TestPlanAndStageLossReportReadFailureKeepsThePlan(t *testing.T) {
	old := pitrFetchBackoff
	pitrFetchBackoff = []time.Duration{0}
	defer func() { pitrFetchBackoff = old }()
	for _, tc := range []struct {
		name     string
		break_   func(s *flakyStore, key string)
		lost     string
		warn     bool
		errorHas string
	}{
		{"readable", func(*flakyStore, string) {}, sidA + ":96-98", false, ""},
		{"store error", func(s *flakyStore, key string) { s.open[key] = -1 }, "", true, "connection reset by peer"},
		{"missing object", func(s *flakyStore, key string) { s.missing[key] = true }, "", true, "mysql-bin.000002"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, key := lostTailArchive(t)
			store := newFlakyStore(a.store)
			tc.break_(store, key)
			out, metaDir := t.TempDir(), t.TempDir()
			req := dlRequest(a, out, writeDumpJSON(t, metaDir, sidA+":1-10", dlEpoch.Add(11*time.Second), a), time.Time{})
			req.mode = pitrplan.ModeLatest
			req.exclude = pitrplan.MustParseSet(sidA + ":96-99")
			var logged bytes.Buffer
			res, err := planAndStage(context.Background(), store, req, slog.New(slog.NewJSONHandler(&logged, nil)))
			if err != nil {
				t.Fatalf("a failed report read failed the restore: %v", err)
			}
			c := readCoverage(t, out)
			if c.Outcome != pitrplan.OutcomeArchiveHead || c.Lost != tc.lost || c.Replay != sidA+":11-95:100-101" || len(stagedFiles(t, out)) != 2 {
				t.Fatalf("coverage: outcome %q lost %q replay %q, staged %v", c.Outcome, c.Lost, c.Replay, stagedFiles(t, out))
			}
			if (len(res.plan.LostUnread) == 1) != tc.warn {
				t.Fatalf("unread rows %+v", res.plan.LostUnread)
			}
			var warns []map[string]any
			sc := bufio.NewScanner(&logged)
			for sc.Scan() {
				var line map[string]any
				if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
					t.Fatalf("log line %q: %v", sc.Text(), err)
				}
				if line["msg"] == "pitr loss report incomplete" {
					warns = append(warns, line)
				}
			}
			if !tc.warn {
				if len(warns) != 0 {
					t.Fatalf("logged %v", warns)
				}
				return
			}
			if len(warns) != 1 || warns[0]["level"] != "WARN" || warns[0]["rows"] != "a/mysql-bin.000002" || warns[0]["count"] != float64(1) ||
				!strings.Contains(warns[0]["error"].(string), tc.errorHas) {
				t.Fatalf("logged %v", warns)
			}
		})
	}
}
