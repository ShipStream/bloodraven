package scenarios

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
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
	const auditErr = "site iad name mysql-bin.000004 id ab12 P=3e11fa47-71ca-11e1-9e33-c80aa9429562:1-40 divergent transactions=3e11fa47-71ca-11e1-9e33-c80aa9429562:41-60; preserve objects and use pitr-quarantine with a canonical full-backup bound"
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
			s.LastError, s.DivergentRows = auditErr, 1
		}, true, true},
		{"divergence error behind an unsafe-history prefix", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.DivergentRows = "PITRArchiveUnsafeHistory: site iad ...; also: "+auditErr, 1
		}, true, true},
		{"quarantined rows count as expected divergence", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.QuarantinedRows = auditErr, 2
		}, true, true},
		{"divergence error without tolerance", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.DivergentRows = auditErr, 1
		}, false, false},
		{"error without divergent rows", func(s *pgsidecar.ArchiverStatusResponse) { s.LastError = "upload failed" }, true, false},
		{"audit error without divergent rows", func(s *pgsidecar.ArchiverStatusResponse) { s.LastError = auditErr }, true, false},
		{"unrelated error while rows are suspect (F181)", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.DivergentRows = "archive mysql-bin.000007: put object: 503 Service Unavailable", 1
		}, true, false},
		{"unreadable-object audit error while rows are suspect", func(s *pgsidecar.ArchiverStatusResponse) {
			s.LastError, s.DivergentRows = "site iad name mysql-bin.000004 id x P=a:1-3 object missing or unreadable, digest unknown: 404; preserve objects and use pitr-quarantine with a canonical full-backup bound", 1
		}, true, false},
		{"not primary", func(s *pgsidecar.ArchiverStatusResponse) { s.Primary = false }, true, false},
		{"other prefix", func(s *pgsidecar.ArchiverStatusResponse) { s.ManifestPrefix = "e2e/s70/run-fresh/binlogs" }, true, false},
		{"other archive", func(s *pgsidecar.ArchiverStatusResponse) { s.Archive.ArchiveID = "arch-2" }, true, false},
		{"archive refused", func(s *pgsidecar.ArchiverStatusResponse) { s.Archive.State = pitrarchive.StateRefused }, true, false},
		{"no archive status", func(s *pgsidecar.ArchiverStatusResponse) { s.Archive = nil }, true, false},
		{"stale promotion record", func(s *pgsidecar.ArchiverStatusResponse) {
			s.PromotionRecordedAt = notBefore.Add(-5 * time.Second)
		}, true, false},
		{"record half a second before the request is stale (F182: no cross-host slack)", func(s *pgsidecar.ArchiverStatusResponse) {
			s.PromotionRecordedAt = notBefore.Add(-500 * time.Millisecond)
		}, false, false},
		{"record a microsecond before the request is stale", func(s *pgsidecar.ArchiverStatusResponse) {
			s.PromotionRecordedAt = notBefore.Add(-time.Microsecond)
		}, false, false},
		{"record at the request instant", func(s *pgsidecar.ArchiverStatusResponse) {
			s.PromotionRecordedAt = notBefore
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
	for _, id := range append([]string{scenarioPITR203}, scenariosPITR206...) {
		if !listed[id] {
			t.Errorf("the e2e pitr job does not run %s (list %q)", id, list)
		}
	}
	// Δ19: 69 runs second, after the single-site baseline 31, and the job
	// takes the hosted runner's full 360-minute cap.
	if len(got) < 2 || got[0].ID != "31-pitr-verification-rustfs" || got[1].ID != scenarioPITR203 {
		t.Errorf("the e2e pitr job must run 31 then %s first (list %q)", scenarioPITR203, list)
	}
	jobBody, _, _ := strings.Cut(job, "\n  # ")
	if !strings.Contains(jobBody, "timeout-minutes: 360") {
		t.Error("the e2e pitr job must set timeout-minutes: 360 (IMPL-203 Δ19)")
	}
	var budget time.Duration
	for _, s := range got {
		budget += s.Timeout
	}
	if budget > 300*time.Minute {
		t.Errorf("the e2e pitr job's scenario ceilings sum to %s; with resets and setup that leaves too little of the 360m cap: split the job", budget)
	}
	reusable, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "_e2e.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(reusable), `run-all --scenarios="$E2E_SCENARIOS"`) {
		t.Error("_e2e.yml must run scenario lists through `run-all --scenarios` so ResetBeforeRunAll applies")
	}
	// F107: scenario-list jobs write and upload their JUnit report too.
	if !strings.Contains(string(reusable), `--junit-out="$E2E_JUNIT_OUT"`) {
		t.Error("_e2e.yml scenario-list run must pass --junit-out")
	}
	_, upload, ok := strings.Cut(string(reusable), "- name: Upload JUnit results\n")
	if !ok {
		t.Fatal("_e2e.yml has no JUnit upload step")
	}
	upload, _, _ = strings.Cut(upload, "- name:")
	if strings.Contains(upload, "inputs.scenario == ''") || !strings.Contains(upload, "e2e-scenarios-junit.xml") && !strings.Contains(upload, "'scenarios'") {
		t.Errorf("the JUnit upload must cover scenario-list jobs:\n%s", upload)
	}
}

// TestPITRSealedFileArchived pins F185: a flushing fence is done only when
// the file it sealed has a new manifest row; an older file's upload, or a
// row of the same name that predates the flush, does not count.
func TestPITRSealedFileArchived(t *testing.T) {
	before := []pitrManifestRow{{Name: "mysql-bin.000003", ID: "a"}, {Name: "mysql-bin.000004", ID: "old"}}
	cases := []struct {
		name  string
		after []pitrManifestRow
		want  bool
	}{
		{"nothing new", before, false},
		{"an older pending file uploaded", append(append([]pitrManifestRow{}, before...), pitrManifestRow{Name: "mysql-bin.000002", ID: "b"}), false},
		{"the sealed file uploaded", append(append([]pitrManifestRow{}, before...), pitrManifestRow{Name: "mysql-bin.000005", ID: "c"}), true},
		{"a same-name row from before", []pitrManifestRow{{Name: "mysql-bin.000005", ID: "c"}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pitrSealedFileArchived(before, c.after, "mysql-bin.000005"); got != c.want {
				t.Fatalf("archived=%v, want %v", got, c.want)
			}
		})
	}
	if pitrSealedFileArchived(before, before, "mysql-bin.000004") {
		t.Error("a row of the sealed name that existed before the flush must not count")
	}
	if got := pitrManifestKey("/e2e/s37/run/binlogs/", "iad"); got != "e2e/s37/run/binlogs/manifest-iad.json" {
		t.Errorf("manifest key %q", got)
	}
}

// TestPITRRecloneRequest pins F067: the confirmation is built only once the
// site's divergence report matches what the caller expects.
func TestPITRRecloneRequest(t *testing.T) {
	const div = "3e11fa47-71ca-11e1-9e33-c80aa9429562:41-60"
	cases := []struct {
		name      string
		site      v1alpha1.SiteStatus
		divergent bool
		ready     bool
		value     string
	}{
		{"divergent site not yet blocked", v1alpha1.SiteStatus{Name: "iad", State: "read-only"}, true, false, ""},
		{"divergent gtid but not RecoveryBlocked", v1alpha1.SiteStatus{Name: "iad", State: "read-only", DivergentGtid: div}, true, false, ""},
		{"blocked with gtid", v1alpha1.SiteStatus{Name: "iad", State: "read-only", RecoveryState: "RecoveryBlocked", DivergentGtid: div}, true, true, "iad:3e11fa47"},
		{"unreachable", v1alpha1.SiteStatus{Name: "iad", State: "unreachable", RecoveryState: "RecoveryBlocked", DivergentGtid: div}, true, false, ""},
		{"healthy replica, cold reclone", v1alpha1.SiteStatus{Name: "iad", State: "read-only", Replicating: true}, false, true, "iad:confirm=mfg"},
		{"cold reclone refused while divergence is recorded", v1alpha1.SiteStatus{Name: "iad", State: "read-only", DivergentGtid: div}, false, false, ""},
		{"cold reclone refused while blocked", v1alpha1.SiteStatus{Name: "iad", State: "read-only", RecoveryState: "RecoveryBlocked"}, false, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ready, value, msg := pitrRecloneRequest([]v1alpha1.SiteStatus{{Name: "pdx", State: "writable"}, c.site}, "iad", "mfg", c.divergent)
			if ready != c.ready || value != c.value {
				t.Fatalf("ready=%v value=%q, want %v %q (%s)", ready, value, c.ready, c.value, msg)
			}
		})
	}
	if ready, _, _ := pitrRecloneRequest(nil, "iad", "mfg", false); ready {
		t.Error("an absent site is not ready")
	}
}

// TestPITRRecloneWatch pins F188: an attempt that fails again while the
// earlier failure's LastTransitionTime is kept is still reported, and an
// earlier attempt's outcome is never taken for this one's.
func TestPITRRecloneWatch(t *testing.T) {
	t0 := metav1.NewTime(mustMicro(t, "2026-11-05T10:00:00.000000Z").Truncate(time.Second))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	cond := func(status metav1.ConditionStatus, reason, msg string, ltt metav1.Time) *metav1.Condition {
		return &metav1.Condition{Type: "Bootstrapping", Status: status, Reason: reason, Message: msg, LastTransitionTime: ltt}
	}
	oldFail := cond(metav1.ConditionTrue, "Failed", "Reclone failed: donor gone", t0)
	t.Run("old failure is not this attempt's", func(t *testing.T) {
		w := newPitrRecloneWatch(oldFail)
		if done, _, err := w.observe(oldFail, false); done || err != nil {
			t.Fatalf("done=%v err=%v", done, err)
		}
	})
	t.Run("failure after an observed in-progress phase keeps the old LTT", func(t *testing.T) {
		w := newPitrRecloneWatch(oldFail)
		if _, _, err := w.observe(cond(metav1.ConditionTrue, "Cloning", "Reclone in progress: Cloning", t0), false); err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.observe(oldFail, false); err == nil {
			t.Fatal("this attempt's failure must be reported even with the old LastTransitionTime and message")
		}
	})
	t.Run("failure with a new message", func(t *testing.T) {
		w := newPitrRecloneWatch(oldFail)
		if _, _, err := w.observe(cond(metav1.ConditionTrue, "Failed", "Reclone failed: clone plugin error", t0), false); err == nil {
			t.Fatal("a changed failure message is this attempt's failure")
		}
	})
	t.Run("old Done is not this attempt's", func(t *testing.T) {
		oldDone := cond(metav1.ConditionFalse, "Done", "Reclone completed successfully", t0)
		w := newPitrRecloneWatch(oldDone)
		if done, _, _ := w.observe(oldDone, true); done {
			t.Fatal("the previous reclone's Done must not complete this one")
		}
		if done, _, _ := w.observe(cond(metav1.ConditionFalse, "Done", "Reclone completed successfully", t1), true); !done {
			t.Fatal("a Done with a new transition time completes the attempt")
		}
	})
	t.Run("done needs replication", func(t *testing.T) {
		w := newPitrRecloneWatch(nil)
		if done, _, _ := w.observe(cond(metav1.ConditionFalse, "Done", "", t1), false); done {
			t.Fatal("done before the site replicates")
		}
	})
}

// TestPITRNewRecloneRejection pins F067's rejection detection: a new (or
// recounted) RecloneRejected Event naming the site fails the wait; old
// ones and other sites' do not.
func TestPITRNewRecloneRejection(t *testing.T) {
	ev := func(uid, msg string, count int32) corev1.Event {
		return corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{UID: types.UID(uid)},
			InvolvedObject: corev1.ObjectReference{Kind: "MysqlFailoverGroup", Name: "mfg"},
			Reason:         "RecloneRejected", Message: msg, Count: count,
		}
	}
	before := map[string]int32{"u1": 1}
	coldMsg := `reclone of "iad" rejected: cold reclone wipes the datadir and must be confirmed`
	if _, ok := pitrNewRecloneRejection(before, []corev1.Event{ev("u1", coldMsg, 1)}, "mfg", "iad"); ok {
		t.Error("an Event seen before the request must not count")
	}
	if _, ok := pitrNewRecloneRejection(before, []corev1.Event{ev("u1", coldMsg, 2)}, "mfg", "iad"); !ok {
		t.Error("a recounted Event is a new rejection")
	}
	if _, ok := pitrNewRecloneRejection(before, []corev1.Event{ev("u2", coldMsg, 1)}, "mfg", "iad"); !ok {
		t.Error("a new Event is a rejection")
	}
	if _, ok := pitrNewRecloneRejection(before, []corev1.Event{ev("u2", "reclone of iad refused: it awaits its post-restore reclone", 1)}, "mfg", "iad"); !ok {
		t.Error("the restore-hold refusal names the site unquoted")
	}
	if _, ok := pitrNewRecloneRejection(before, []corev1.Event{ev("u3", `reclone of "pdx" rejected`, 1)}, "mfg", "iad"); ok {
		t.Error("another site's rejection must not count")
	}
}

// TestPITRWriterAwaitFirst pins F189: a window starts after the writer's
// first commit, and a writer that stopped without one fails it.
func TestPITRWriterAwaitFirst(t *testing.T) {
	w := &pitrWriter{stop: make(chan struct{}), done: make(chan struct{}), first: make(chan struct{})}
	close(w.first)
	if err := w.awaitFirst(context.Background(), "iad"); err != nil {
		t.Fatal(err)
	}
	failed := &pitrWriter{stop: make(chan struct{}), done: make(chan struct{}), first: make(chan struct{}), err: errors.New("read-only")}
	close(failed.done)
	if err := failed.awaitFirst(context.Background(), "iad"); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pending := &pitrWriter{stop: make(chan struct{}), done: make(chan struct{}), first: make(chan struct{})}
	if err := pending.awaitFirst(ctx, "iad"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

// TestPITRScenarioTargetsAreAdmissible pins F068: every target the PITR
// scenarios generate (pitrFenceTarget, used by s31 and every fence) passes
// the CRDs' admission patterns for verification timestamps and
// stopDatetime, which refuse fractional seconds.
func TestPITRScenarioTargetsAreAdmissible(t *testing.T) {
	root := filepath.Join("..", "..", "..", "config", "crd", "bases")
	var patterns []*regexp.Regexp
	for _, f := range []struct{ file, field string }{
		{"shipstream.io_mysqlbackupverifications.yaml", "timestamp"},
		{"shipstream.io_mysqlfailovergroups.yaml", "stopDatetime"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, f.file))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(raw), "\n")
		found := 0
		for i, l := range lines {
			if strings.TrimSpace(l) != f.field+":" {
				continue
			}
			for j := i + 1; j < len(lines) && j < i+30; j++ {
				if v, ok := strings.CutPrefix(strings.TrimSpace(lines[j]), "pattern: "); ok {
					patterns = append(patterns, regexp.MustCompile(strings.Trim(v, `'"`)))
					found++
					break
				}
			}
		}
		if found == 0 {
			t.Fatalf("%s: no pattern found for %s", f.file, f.field)
		}
	}
	for _, now := range []string{"2026-11-05T10:00:00.000000Z", "2026-11-05T10:00:00.123456Z", "2026-12-31T23:59:59.999999Z"} {
		target := pitrFenceTarget(mustMicro(t, now)).Format(pitrTargetLayout)
		for _, p := range patterns {
			if !p.MatchString(target) {
				t.Errorf("target %q is refused by admission pattern %s", target, p)
			}
		}
	}
	for _, p := range patterns {
		if p.MatchString("2026-11-05T10:00:00.250000Z") {
			t.Errorf("pattern %s admits a fractional target; this test no longer pins the A-39 rule", p)
		}
	}
}

// TestPITRAssertResultReplayCount pins F187: with exclusions the replay
// count is (fenced ∖ exclusions) ∖ baseline, subtracted once.
func TestPITRAssertResultReplayCount(t *testing.T) {
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	env := &runner.Env{Capture: &runner.Capture{}}
	stashSet(env, "archiveID", "ar1-abc")
	stashSet(env, "baseline:b1", u+":1-10")
	res := &v1alpha1.PointInTimeResult{
		Mode: v1alpha1.PointInTimeModeLatest, Outcome: v1alpha1.PointInTimeArchiveHead,
		Archive:      &v1alpha1.PITRArchiveIdentity{ArchiveID: "ar1-abc"},
		GtidExecuted: u + ":1-15:18-20", ReplayedTransactionCount: 8,
	}
	e := pitrExpectation{Backup: "b1", Mode: "latest", Fence: pitrFence{GTID: u + ":1-20"}, Exclude: u + ":16-17", Outcome: v1alpha1.PointInTimeArchiveHead}
	if err := assertPITRResult(env, "t", res, e); err != nil {
		t.Fatal(err)
	}
	res.ReplayedTransactionCount = 6
	if err := assertPITRResult(env, "t", res, e); err == nil {
		t.Fatal("a count that subtracts the exclusions twice must fail")
	}
}

// TestPITRScenarioCleanupBudgets pins F071: the PITR scenarios do not run
// their composed cleanup on the 4-minute default.
func TestPITRScenarioCleanupBudgets(t *testing.T) {
	want := map[string]time.Duration{
		s69ID: pitrCleanupBaseBudget,
		s37ID: pitrCleanupBaseBudget + pitrDeleteGroupTimeout,
		s68ID: pitrCleanupBaseBudget,
		s70ID: pitrCleanupBaseBudget + s70RepairTimeout,
	}
	for _, sc := range runner.DefaultRegistry.List() {
		w, ok := want[sc.ID]
		if !ok {
			continue
		}
		delete(want, sc.ID)
		if sc.CleanupTimeout < w {
			t.Errorf("%s: CleanupTimeout=%s, want at least %s", sc.ID, sc.CleanupTimeout, w)
		}
	}
	for id := range want {
		t.Errorf("%s is not registered", id)
	}
	// The base must cover at least the healthy-baseline wait plus one group
	// deletion, one backup deletion with its artifact check, and the
	// release of two archive bindings (#203).
	if pitrCleanupBaseBudget < 6*time.Minute+runner.DefaultCleanupTimeout+5*time.Minute+2*pitrReleaseBindingWaitTime {
		t.Errorf("pitrCleanupBaseBudget=%s is below its composed parts", pitrCleanupBaseBudget)
	}
}

// TestPITRCleanupOwnership pins F190 and F191's guards: only this run's
// group is deleted, and only prefixes inside e2e/<scenario>/<run>.
func TestPITRCleanupOwnership(t *testing.T) {
	mine := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{pitrRunAnnotation: "s37-20261105T100000Z"}}}
	if !pitrOwnedByRun(mine, "s37-20261105T100000Z") {
		t.Error("this run's group must be deletable")
	}
	if pitrOwnedByRun(mine, "s37-20261104T090000Z") || pitrOwnedByRun(&v1alpha1.MysqlFailoverGroup{}, "s37-20261105T100000Z") || pitrOwnedByRun(mine, "") {
		t.Error("another run's (or an unannotated) group must be left in place")
	}
	for prefix, ok := range map[string]bool{
		"e2e/37-pitr-planned-failback-no-reclone/s37-20261105T100000Z": true,
		"e2e/70-pitr-lossy-failover/s70-20261105T100000Z-fresh/":       true,
		"e2e/":             false,
		"e2e/37-pitr":      false,
		"backups/prod":     false,
		"e2e/../prod/x":    false,
		"":                 false,
		"e2e/37-pitr//run": false,
	} {
		if got := pitrArchivePrefixDeletable(prefix); got != ok {
			t.Errorf("pitrArchivePrefixDeletable(%q)=%v, want %v", prefix, got, ok)
		}
	}
}

// TestS70ShouldClearDelay pins F193: cleanup resets SOURCE_DELAY from b's
// live state, never on a promoted b.
func TestS70ShouldClearDelay(t *testing.T) {
	for _, c := range []struct {
		name                 string
		readOnly, hasChannel bool
		delay                int64
		want                 bool
	}{
		{"delayed replica", true, true, 300, true},
		{"partly applied (START failed)", true, true, 300, true},
		{"replica without a delay", true, true, 0, false},
		{"promoted b", false, false, 0, false},
		{"writable with a leftover channel", false, true, 300, false},
	} {
		if got := s70ShouldClearDelay(c.readOnly, c.hasChannel, c.delay); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestS70OperatorPollSync pins F070's synchronization: the wait spans two
// operator poll intervals (mirrored from the controller's constant), and a
// failed poll of the site is detected from status.pitr.message.
func TestS70OperatorPollSync(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "controller", "archiver_poller.go"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`const archiverPollInterval = (\d+) \* time\.Second`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("archiverPollInterval not found in internal/controller/archiver_poller.go")
	}
	if got := string(m[1]) + "s"; got != pitrOperatorArchiverPollInterval.String() {
		t.Fatalf("pitrOperatorArchiverPollInterval=%s, controller archiverPollInterval=%s", pitrOperatorArchiverPollInterval, got)
	}
	if s70OperatorPollWait < 2*pitrOperatorArchiverPollInterval {
		t.Errorf("s70OperatorPollWait=%s is under two poll intervals", s70OperatorPollWait)
	}
	if !s70OperatorPollFailing(&v1alpha1.PITRStatus{Message: "iad: archiver status unavailable: dial tcp: refused"}, "iad") {
		t.Error("a failed poll of iad must be detected")
	}
	if s70OperatorPollFailing(&v1alpha1.PITRStatus{Message: "pdx: archiver status unavailable: x; iad: site iad name ..."}, "iad") {
		t.Error("another site's failed poll, or iad's own lastError, is not iad's failed poll")
	}
}

func TestPITRDecryptObjectReadsEncryptedManifest(t *testing.T) {
	pass := []byte("s37-test-passphrase-0123456789abcdef")
	var enc bytes.Buffer
	if _, err := backupcrypto.Encrypt(&enc, strings.NewReader(`{"files":[{"name":"mysql-bin.000001"}]}`), pass); err != nil {
		t.Fatal(err)
	}
	if !backupcrypto.LooksEncrypted(enc.Bytes()) {
		t.Fatal("encrypted manifest not recognised as BRV1")
	}
	plain, err := pitrDecryptObject(enc.Bytes(), pass)
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Files []pitrManifestRow `json:"files"`
	}
	if err := json.Unmarshal(plain, &m); err != nil || len(m.Files) != 1 || m.Files[0].Name != "mysql-bin.000001" {
		t.Fatalf("decoded %+v err=%v", m, err)
	}
	if _, err := pitrDecryptObject(enc.Bytes(), []byte("wrong-passphrase-0123456789abcdef")); err == nil {
		t.Fatal("wrong passphrase decrypted")
	}
}

func TestPITRSiteReplicating(t *testing.T) {
	mfg := &v1alpha1.MysqlFailoverGroup{}
	mfg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "iad", State: "writable"},
		{Name: "pdx", State: "read-only"},
	}
	if ok, _, _ := pitrSiteReplicating(mfg, "pdx"); ok {
		t.Fatal("a read-only site that is not replicating counted as replicating")
	}
	mfg.Status.Sites[1].Replicating = true
	if ok, _, _ := pitrSiteReplicating(mfg, "pdx"); !ok {
		t.Fatal("a replicating read-only site was not accepted")
	}
	mfg.Status.Sites[1].RecoveryState = "RecoveryBlocked"
	if ok, _, _ := pitrSiteReplicating(mfg, "pdx"); ok {
		t.Fatal("a RecoveryBlocked replica counted as a planned-failover target")
	}
	mfg.Status.Sites[1].RecoveryState = "RecoveryInProgress"
	if ok, _, _ := pitrSiteReplicating(mfg, "pdx"); !ok {
		t.Fatal("a replica in RecoveryInProgress (not refused by validation) was not accepted")
	}
	if ok, _, _ := pitrSiteReplicating(mfg, "iad"); ok {
		t.Fatal("the writable site counted as a replicating target")
	}
	if ok, _, _ := pitrSiteReplicating(mfg, "sfo"); ok {
		t.Fatal("a site missing from status counted as replicating")
	}
}

// TestPITRObservedArchiveIDMatchesPrefix pins that only a Verified
// observation of the configured prefix's destination yields an archive ID:
// after s70's prefix switch the status may still describe the old archive.
func TestPITRObservedArchiveIDMatchesPrefix(t *testing.T) {
	dest := func(prefix string) string {
		d, err := pitrarchive.NewS3Destination(backupE2EBucket, prefix, backupE2EEndpoint)
		if err != nil {
			t.Fatal(err)
		}
		return d.String()
	}
	const (
		oldPrefix = "e2e/70-pitr-lossy-failover/s70-run"
		newPrefix = "e2e/70-pitr-lossy-failover/s70-run-fresh"
		id        = "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	cases := []struct {
		name   string
		obs    *v1alpha1.PITRArchiveObservation
		prefix string
		want   string
	}{
		{"no observation", nil, newPrefix, ""},
		{"verified at the prefix", &v1alpha1.PITRArchiveObservation{Destination: dest(newPrefix), ArchiveID: id, State: v1alpha1.PITRArchiveVerified}, newPrefix, id},
		{"slash-trimmed prefix", &v1alpha1.PITRArchiveObservation{Destination: dest(newPrefix), ArchiveID: id, State: v1alpha1.PITRArchiveVerified}, "/" + newPrefix + "/", id},
		{"stale observation of the previous prefix", &v1alpha1.PITRArchiveObservation{Destination: dest(oldPrefix), ArchiveID: id, State: v1alpha1.PITRArchiveVerified}, newPrefix, ""},
		{"awaiting descriptor", &v1alpha1.PITRArchiveObservation{Destination: dest(newPrefix), ArchiveID: id, State: v1alpha1.PITRArchiveAwaitingDescriptor}, newPrefix, ""},
		{"unparseable destination", &v1alpha1.PITRArchiveObservation{Destination: "bogus", ArchiveID: id, State: v1alpha1.PITRArchiveVerified}, newPrefix, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mfg := &v1alpha1.MysqlFailoverGroup{}
			if c.obs != nil {
				mfg.Status.PITR = &v1alpha1.PITRStatus{Enabled: true, Archive: c.obs}
			}
			if got := pitrObservedArchiveID(mfg, c.prefix); got != c.want {
				t.Errorf("pitrObservedArchiveID=%q, want %q", got, c.want)
			}
		})
	}
}

func TestGTIDOverlap(t *testing.T) {
	const a = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const b = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
	cases := []struct{ x, y, want string }{
		{a + ":1-10", a + ":5-20", a + ":5-10"},
		{a + ":1-4", a + ":5-20", ""},
		{a + ":3-4", a + ":1-9," + b + ":1-2", a + ":3-4"},
		{a + ":3-4", b + ":1-9", ""},
	}
	for _, c := range cases {
		got, err := gtidOverlap(c.x, c.y)
		if err != nil {
			t.Fatal(err)
		}
		if c.want == "" {
			if got != "" {
				t.Errorf("gtidOverlap(%s, %s)=%q, want none", c.x, c.y, got)
			}
			continue
		}
		if eq, err := gtidSetsEqual(got, c.want); err != nil || !eq {
			t.Errorf("gtidOverlap(%s, %s)=%q, want %q", c.x, c.y, got, c.want)
		}
	}
}

func TestPITRReaderSites(t *testing.T) {
	mfg := &v1alpha1.MysqlFailoverGroup{}
	mfg.Spec.Sites = []v1alpha1.SiteSpec{
		{Name: "iad", Role: v1alpha1.SiteRolePrimaryCandidate},
		{Name: "pdx", Role: v1alpha1.SiteRolePrimaryCandidate},
		{Name: "reader", Role: v1alpha1.SiteRoleReadOnly},
	}
	if got := pitrReaderSites(mfg); len(got) != 1 || got[0] != "reader" {
		t.Fatalf("pitrReaderSites=%v, want [reader]", got)
	}
}

func TestPITROnlyTargetLocalExtra(t *testing.T) {
	const a, b, local = "808ed4dd-d9b6-11f1-9bac-4a9f5150b22b", "80925e8f-d9b6-11f1-ad00-766dbd96b6d1", "685eca8a-d9b7-11f1-a3cb-8a03027c3685"
	want := a + ":1-12,\n" + b + ":1-12"
	for _, tc := range []struct {
		name string
		got  string
		ok   bool
	}{
		{"exact", want, true},
		{"target-local extra", local + ":1-9,\n" + a + ":1-12,\n" + b + ":1-12", true},
		{"extra history transaction", a + ":1-13,\n" + b + ":1-12", false},
		{"missing history transaction", local + ":1-9,\n" + a + ":1-11,\n" + b + ":1-12", false},
	} {
		ok, err := pitrOnlyTargetLocalExtra(tc.got, want)
		if err != nil || ok != tc.ok {
			t.Errorf("%s: ok=%t err=%v, want %t", tc.name, ok, err, tc.ok)
		}
	}
}

// TestPITRBindingIDsUnderPrefixes pins the cleanup's selection of bindings
// to release (IMPL-203 Δ20): every entry and every snapshot under a run's
// prefix, in any endpoint spelling, and nothing else.
func TestPITRBindingIDsUnderPrefixes(t *testing.T) {
	p1 := "e2e/69-pitr-encryption-binding-refusal/s69-20261106T100000Z-p1"
	dest := func(bucket, prefix, endpoint string) string {
		d, err := pitrarchive.NewS3Destination(bucket, prefix, endpoint)
		if err != nil {
			t.Fatal(err)
		}
		return d.String()
	}
	mfg := &v1alpha1.MysqlFailoverGroup{}
	mfg.Status.PITRArchiveBindings = []v1alpha1.PITRArchiveBinding{
		{ID: "pab-0000000000000001", Destination: dest(backupE2EBucket, p1, backupE2EEndpoint)},
		{ID: "pab-0000000000000002", Destination: dest(backupE2EBucket, p1, "http://rustfs.bloodraven-playground.svc:9000")},
		{ID: "pab-0000000000000003", Destination: dest(backupE2EBucket, p1+"-p2", backupE2EEndpoint)},
		{ID: "pab-0000000000000004", Destination: dest("other-bucket", p1, backupE2EEndpoint)},
		{ID: "pab-0000000000000005", Destination: "pvc://ns/claim/" + p1 + "/binlogs"},
	}
	snap := func(id, destination string) corev1.Secret {
		return corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{pitrBindingIDLabel: id},
			Annotations: map[string]string{pitrDestinationAnnotation: destination},
		}}
	}
	snaps := []corev1.Secret{
		snap("pab-0000000000000006", dest(backupE2EBucket, p1, backupE2EEndpoint)), // entry dropped
		snap("pab-0000000000000001", dest(backupE2EBucket, p1, backupE2EEndpoint)),
		snap("pab-0000000000000007", dest(backupE2EBucket, "e2e/other/run", backupE2EEndpoint)),
		snap("", dest(backupE2EBucket, p1, backupE2EEndpoint)),
	}
	got := pitrBindingIDsUnderPrefixes(mfg, snaps, []string{p1})
	want := []string{"pab-0000000000000001", "pab-0000000000000002", "pab-0000000000000006"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("ids under P1 = %v, want %v", got, want)
	}
	if got := pitrBindingIDsUnderPrefixes(mfg, nil, nil); len(got) != 0 {
		t.Errorf("no prefix must select nothing, got %v", got)
	}
	if pitrReleaseValue("pab-0000000000000001", "playground") != "pab-0000000000000001:confirm=playground" {
		t.Error("the release value must be <id>:confirm=<group>")
	}
	if !strings.Contains(pitrGroupSnapshotSelector("u-1"), "bloodraven.shipstream.io/failover-group-uid=u-1") ||
		!strings.Contains(pitrGroupSnapshotSelector("u-1"), "mysql-pitr-key") || !strings.Contains(pitrGroupSnapshotSelector("u-1"), "mysql-dump-key") {
		t.Errorf("group snapshot selector %q must cover both snapshot kinds by the group UID", pitrGroupSnapshotSelector("u-1"))
	}
}

// TestPITRDecryptArchiveObjectUsesStashedBytes pins Δ20: archive objects
// are decrypted with passphrases stashed before any rotation, trying each,
// and plaintext passes through.
func TestPITRDecryptArchiveObjectUsesStashedBytes(t *testing.T) {
	env := &runner.Env{}
	t.Cleanup(func() {
		stashMu.Lock()
		delete(stashes, env)
		stashMu.Unlock()
	})
	plain := []byte(`{"files":[]}`)
	if got, err := pitrDecryptArchiveObject(env, plain); err != nil || string(got) != string(plain) {
		t.Fatalf("plaintext object: %q, %v", got, err)
	}
	p2 := []byte("second-archive-passphrase-0123456789")
	var enc bytes.Buffer
	if _, err := backupcrypto.Encrypt(&enc, strings.NewReader(string(plain)), p2); err != nil {
		t.Fatal(err)
	}
	if _, err := pitrDecryptArchiveObject(env, enc.Bytes()); err == nil {
		t.Fatal("an encrypted object with no stashed passphrase must fail")
	}
	pitrStashArchivePassphrase(env, []byte("run-passphrase-0123456789abcdef\n"))
	pitrStashArchivePassphrase(env, []byte("run-passphrase-0123456789abcdef"))
	if n := len(pitrArchivePassphrases(env)); n != 1 {
		t.Fatalf("a trimmed duplicate must be stashed once, got %d", n)
	}
	pitrStashArchivePassphrase(env, p2)
	got, err := pitrDecryptArchiveObject(env, enc.Bytes())
	if err != nil || string(got) != string(plain) {
		t.Fatalf("decrypt with the second stashed passphrase: %q, %v", got, err)
	}
	if string(pitrArchivePassphrases(env)[0]) != "run-passphrase-0123456789abcdef" {
		t.Error("the run passphrase must stay first")
	}
}
