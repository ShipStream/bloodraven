package scenarios

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func mustMicro(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(pitrMicroLayout, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestPITRFenceTargetIsWholeSecondAfterNow pins the fence's T: a whole
// second (stopDatetime admits no fraction, A-39) strictly after the clock
// read under the lock, and at most one second after it.
func TestPITRFenceTargetIsWholeSecondAfterNow(t *testing.T) {
	for _, now := range []string{
		"2026-11-05T10:00:00.000000Z",
		"2026-11-05T10:00:00.000001Z",
		"2026-11-05T10:00:00.999999Z",
		"2026-11-05T23:59:59.500000Z",
	} {
		n := mustMicro(t, now)
		got := pitrFenceTarget(n)
		if !got.After(n) || got.Sub(n) > time.Second || got.Truncate(time.Second) != got {
			t.Errorf("pitrFenceTarget(%s)=%s: want the next whole second strictly after now", now, got.Format(pitrMicroLayout))
		}
		if s := got.Format(pitrTargetLayout); strings.Contains(s, ".") || !strings.HasSuffix(s, "Z") {
			t.Errorf("target %q must be whole-second RFC 3339 UTC", s)
		}
	}
}

// TestPITRBackupClearTime pins C10: a target must be at least a second past
// both the server clock seen after the backup succeeded (an upper bound on
// serverClock.end) and the dump end rounded up to the second.
func TestPITRBackupClearTime(t *testing.T) {
	cases := []struct {
		name, seen, end, want string
	}{
		{"server clock dominates", "2026-11-05T10:00:05.250000Z", "2026-11-05T10:00:03.100000Z", "2026-11-05T10:00:06.250000Z"},
		{"dump end rounds up", "2026-11-05T10:00:03.200000Z", "2026-11-05T10:00:03.100000Z", "2026-11-05T10:00:05.000000Z"},
		{"whole-second dump end", "2026-11-05T10:00:02.000000Z", "2026-11-05T10:00:03.000000Z", "2026-11-05T10:00:04.000000Z"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := pitrBackupClearTime(mustMicro(t, c.seen), mustMicro(t, c.end))
			if !got.Equal(mustMicro(t, c.want)) {
				t.Fatalf("clear=%s, want %s", got.Format(pitrMicroLayout), c.want)
			}
		})
	}
}

func TestPITRParseDumpEnd(t *testing.T) {
	for in, want := range map[string]string{
		"2026-11-05 10:00:03":            "2026-11-05T10:00:03.000000Z",
		"2026-11-05 10:00:03.123456":     "2026-11-05T10:00:03.123456Z",
		"2026-11-05T10:00:03.5Z":         "2026-11-05T10:00:03.500000Z",
		" 2026-11-05T10:00:03.250000 \n": "2026-11-05T10:00:03.250000Z",
	} {
		got, err := pitrParseDumpEnd(in)
		if err != nil {
			t.Fatalf("pitrParseDumpEnd(%q): %v", in, err)
		}
		if got.Format(pitrMicroLayout) != want {
			t.Errorf("pitrParseDumpEnd(%q)=%s, want %s", in, got.Format(pitrMicroLayout), want)
		}
	}
	if _, err := pitrParseDumpEnd("yesterday"); err == nil {
		t.Error("a non-timestamp end must be an error")
	}
}

func TestPITRCheckTargetAfterBackup(t *testing.T) {
	env := &runner.Env{}
	stashSet(env, "backupClear:b2", "2026-11-05T10:00:06.250000Z")
	cases := []struct {
		name, mode, backup, target string
		wantErr                    string
	}{
		{"after clear", "timestamp", "b2", "2026-11-05T10:00:07Z", ""},
		{"before clear", "timestamp", "b2", "2026-11-05T10:00:06Z", "before backup b2's clear time"},
		{"latest needs no target", "latest", "b2", "", ""},
		{"untracked backup", "timestamp", "b9", "2026-11-05T10:00:07Z", "no recorded clear time"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := pitrCheckTargetAfterBackup(env, pitrExpectation{Backup: c.backup, Mode: c.mode, Fence: pitrFence{T: c.target}})
			if c.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err=%v, want %q", err, c.wantErr)
			}
		})
	}
}

// TestPITRPromotionReadyToleratesOnlyExpectedDivergence pins G5: s70's
// failback must not wait for a clean LastError while the recloned site's
// audit keeps reporting its retained lost rows, yet must still require the
// right primary, archive and a fresh promotion record.
func TestPITRPromotionReadyToleratesOnlyExpectedDivergence(t *testing.T) {
	notBefore := mustMicro(t, "2026-11-05T10:00:10.000000Z")
	good := func() *pgsidecar.ArchiverStatusResponse {
		return &pgsidecar.ArchiverStatusResponse{
			Enabled: true, Primary: true, ManifestPrefix: "e2e/s70/run/binlogs/", LastScanAt: notBefore,
			Archive:             &pitrarchive.ArchiveStatus{ArchiveID: "arch-1", State: pitrarchive.StateVerified},
			PromotionRecordedAt: notBefore.Add(2 * time.Second),
		}
	}
	cases := []struct {
		name     string
		mutate   func(*pgsidecar.ArchiverStatusResponse)
		tolerate bool
		want     bool
	}{
		{"clean", func(*pgsidecar.ArchiverStatusResponse) {}, false, true},
		{"divergence error tolerated", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.DivergentRows = "site iad: archived rows missing from the history", 1
		}, true, true},
		{"quarantined rows count as expected divergence", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.QuarantinedRows = "audit", 2
		}, true, true},
		{"divergence error without tolerance", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.DivergentRows = "audit", 1
		}, false, false},
		{"error without divergent rows", func(s *pgsidecar.ArchiverStatusResponse) { s.LastError = "upload failed" }, true, false},
		{"not primary", func(s *pgsidecar.ArchiverStatusResponse) { s.Primary = false }, true, false},
		{"other prefix", func(s *pgsidecar.ArchiverStatusResponse) { s.ManifestPrefix = "e2e/s70/run-fresh/binlogs" }, true, false},
		{"other archive", func(s *pgsidecar.ArchiverStatusResponse) { s.Archive.ArchiveID = "arch-2" }, true, false},
		{"archive refused", func(s *pgsidecar.ArchiverStatusResponse) { s.Archive.State = pitrarchive.StateRefused }, true, false},
		{"no archive status", func(s *pgsidecar.ArchiverStatusResponse) { s.Archive = nil }, true, false},
		{"stale promotion record", func(s *pgsidecar.ArchiverStatusResponse) {
			s.PromotionRecordedAt = notBefore.Add(-5 * time.Second)
		}, true, false},
		{"record within the µs slack", func(s *pgsidecar.ArchiverStatusResponse) {
			s.PromotionRecordedAt = notBefore.Add(-500 * time.Millisecond)
		}, false, true},
		{"no record yet", func(s *pgsidecar.ArchiverStatusResponse) { s.PromotionRecordedAt = time.Time{} }, true, false},
		{"never scanned", func(s *pgsidecar.ArchiverStatusResponse) { s.LastScanAt = time.Time{} }, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := good()
			c.mutate(st)
			got, summary := pitrPromotionReady(st, "e2e/s70/run/binlogs", "arch-1", notBefore, c.tolerate)
			if got != c.want {
				t.Fatalf("ready=%v, want %v (%s)", got, c.want, summary)
			}
		})
	}
}

// TestRefusalNamesExactly pins C11: the refusal must print L itself, not a
// superset, a subset or merely a set of the same source.
func TestRefusalNamesExactly(t *testing.T) {
	const a = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const b = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
	lost := a + ":41-60"
	latest := "PITRArchiveDivergentHistory: the newest archived state includes " + a + ":41-60, committed before pdx was promoted at 2026-11-05T10:00:00.123456Z without them; they were lost at that failover."
	window := "PITRArchiveDivergentHistory: 20 archived transactions committed between the backup's newest transaction (2026-11-05T09:00:00.000000Z) and 2026-11-05T10:05:00.000000Z are not in the history of " + b + ":9 (site pdx, committed 2026-11-05T10:04:00.000000Z): " + a + ":41-60 (sites iad, 2026-11-05T10:00:00.000000Z…2026-11-05T10:00:01.000000Z). To restore " + b + ":9's history, add them to excludeGtids"
	cases := []struct {
		name, msg, want string
		ok              bool
	}{
		{"latest message", latest, lost, true},
		{"D-window message among other GTIDs", window, lost, true},
		{"equal as sets, other spelling", latest, a + ":41-50:51-60", true},
		{"multi-source set", "includes " + a + ":41-60," + b + ":3-4, committed", b + ":3-4," + a + ":41-60", true},
		{"over-reported by one", strings.Replace(latest, ":41-60", ":40-60", 1), lost, false},
		{"under-reported", strings.Replace(latest, ":41-60", ":41-59", 1), lost, false},
		{"only the uuid", "lost transactions of " + a + " were found", lost, false},
		{"tagged source", "includes " + a + ":app:1-2, committed", a + ":app:1-2", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := refusalNamesExactly(c.msg, c.want)
			if (err == nil) != c.ok {
				t.Fatalf("refusalNamesExactly ok=%v, want %v: %v", err == nil, c.ok, err)
			}
		})
	}
}

// TestPITRScenariosInPITRE2EJob pins C8: the label-gated e2e `pitr` job
// runs every #206 scenario (which the release profile also includes, see
// TestScenariosPITR206RegisteredAndProfiled), through run-all so their
// resets happen (C12), and every listed ID is registered.
func TestPITRScenariosInPITRE2EJob(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "e2e.yml"))
	if err != nil {
		t.Fatal(err)
	}
	_, job, ok := strings.Cut(string(raw), "\n  pitr:\n")
	if !ok {
		t.Fatal("e2e.yml has no pitr job")
	}
	var list string
	for _, line := range strings.Split(job, "\n") {
		if v, found := strings.CutPrefix(strings.TrimSpace(line), "scenario:"); found {
			list = strings.TrimSpace(v)
			break
		}
	}
	got, err := runner.SelectByIDs(runner.DefaultRegistry.List(), list)
	if err != nil {
		t.Fatalf("pitr job scenario list %q: %v", list, err)
	}
	listed := map[string]bool{}
	for _, s := range got {
		listed[s.ID] = true
	}
	for _, id := range scenariosPITR206 {
		if !listed[id] {
			t.Errorf("the e2e pitr job does not run %s (list %q)", id, list)
		}
	}
	reusable, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "_e2e.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(reusable), `run-all --scenarios="$E2E_SCENARIOS"`) {
		t.Error("_e2e.yml must run scenario lists through `run-all --scenarios` so ResetBeforeRunAll applies")
	}
}
