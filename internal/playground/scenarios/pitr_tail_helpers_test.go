package scenarios

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brmysql "github.com/shipstream/bloodraven/internal/mysql"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

// TestPITRTailArchived pins the tail wait's predicate (#264): Drained
// counts only from a scan that saw the rotation, and a tail that held the
// site's own transactions also needs its manifest row.
func TestPITRTailArchived(t *testing.T) {
	const sealed, next = "mysql-bin.000007", "mysql-bin.000008"
	tail := func(state, active string) *pgsidecar.ArchiverStatusResponse {
		return &pgsidecar.ArchiverStatusResponse{Enabled: true, Site: "pdx",
			Tail: &pgsidecar.ArchiverTailResponse{State: state, ActiveFile: active, NewestSealedFile: sealed}}
	}
	cases := []struct {
		name        string
		st          *pgsidecar.ArchiverStatusResponse
		wantRow     bool
		rowArchived bool
		want        bool
		msg         string
	}{
		{name: "drained after the rotation, row uploaded", st: tail(brmysql.ArchiverTailDrained, next), wantRow: true, rowArchived: true, want: true},
		{name: "drained after the rotation, nothing to upload", st: tail(brmysql.ArchiverTailDrained, next), want: true},
		{name: "drained after the rotation, row uploaded though not required", st: tail(brmysql.ArchiverTailDrained, next), rowArchived: true, want: true},
		{name: "drained, but from a scan before the rotation", st: tail(brmysql.ArchiverTailDrained, sealed), wantRow: true, rowArchived: true, msg: "did not see " + sealed + " sealed"},
		{name: "drained after the rotation but the era's row is missing", st: tail(brmysql.ArchiverTailDrained, next), wantRow: true, msg: "holds no new row for " + sealed},
		{name: "pending", st: tail(brmysql.ArchiverTailPending, next), wantRow: true, msg: "the tail is Pending"},
		{name: "pending although the row is there (an older file is not)", st: tail(brmysql.ArchiverTailPending, next), wantRow: true, rowArchived: true, msg: "the tail is Pending"},
		{name: "blocked", st: func() *pgsidecar.ArchiverStatusResponse {
			s := tail(brmysql.ArchiverTailBlocked, next)
			s.Tail.Reason = "this site's manifest holds no promotion record"
			return s
		}(), msg: "this site's manifest holds no promotion record"},
		{name: "no active file reported", st: tail(brmysql.ArchiverTailDrained, ""), msg: "did not see"},
		{name: "no tail block (older sidecar, or no scan yet)", st: &pgsidecar.ArchiverStatusResponse{Enabled: true}, rowArchived: true, msg: "no tail block"},
		{name: "no status", st: nil, msg: "no archiver status"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, msg := pitrTailArchived(c.st, sealed, c.wantRow, c.rowArchived)
			if got != c.want {
				t.Fatalf("archived=%v, want %v (%s)", got, c.want, msg)
			}
			if !strings.Contains(msg, c.msg) {
				t.Errorf("message %q lacks %q", msg, c.msg)
			}
		})
	}
}

func TestArchiverTailSummary(t *testing.T) {
	if got := archiverTailSummary(nil); got != "<none>" {
		t.Errorf("nil tail summary %q", got)
	}
	got := archiverSummary(&pgsidecar.ArchiverStatusResponse{Site: "iad", Tail: &pgsidecar.ArchiverTailResponse{
		State: brmysql.ArchiverTailBlocked, ActiveFile: "mysql-bin.000003", NewestSealedFile: "mysql-bin.000002", PendingFiles: 2, Reason: "no record"}})
	if want := `tail={Blocked active=mysql-bin.000003 newestSealed=mysql-bin.000002 pending=2 reason="no record"}`; !strings.Contains(got, want) {
		t.Errorf("summary %q lacks %q", got, want)
	}
	if got := archiverSummary(&pgsidecar.ArchiverStatusResponse{Site: "iad"}); !strings.Contains(got, "tail=<none>") {
		t.Errorf("summary without a tail: %q", got)
	}
}

// TestPITRTailWatchStash: a watch survives the stash between the step that
// starts it (before a primary is lost) and the one that waits (after it
// came back).
func TestPITRTailWatchStash(t *testing.T) {
	env := &runner.Env{Capture: &runner.Capture{}}
	w := &pitrTailWatch{Site: "iad", Key: "e2e/s70/run/binlogs/manifest-iad.json", File: "mysql-bin.000009",
		Before: []pitrManifestRow{{Name: "mysql-bin.000008", ID: "a1"}, {Name: "mysql-bin.000009", ID: "old-incarnation"}}}
	if err := pitrStashTailWatch(context.Background(), env, "a", w); err != nil {
		t.Fatal(err)
	}
	got, err := pitrFetchTailWatch(env, "a")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, w) {
		t.Errorf("fetched %+v, want %+v", got, w)
	}
	// The fetched rows still tell an earlier incarnation's same-named file
	// from the upload of the sealed one (F185).
	if pitrSealedFileArchived(got.Before, got.Before, got.File) {
		t.Error("a row that predates the watch must not count as the tail's upload")
	}
	if !pitrSealedFileArchived(got.Before, append(append([]pitrManifestRow{}, got.Before...), pitrManifestRow{Name: got.File, ID: "new"}), got.File) {
		t.Error("a new row of the sealed file is the tail's upload")
	}
	if _, err := pitrFetchTailWatch(env, "b"); err == nil {
		t.Error("a watch that was never stashed must be an error")
	}
}

// TestPITRRotationFailureLine: the tail wait's timeout quotes the
// operator's WARN line for the outgoing primary, and only that.
func TestPITRRotationFailureLine(t *testing.T) {
	warn := `{"level":"WARN","msg":"outgoing primary binlog rotation failed","site":"iad","durationMs":1000,"runId":"pf-1","timedOut":true,"error":"no answer within 1s; continuing without it"}`
	lines := logLines(
		`{"level":"INFO","msg":"outgoing primary binlog rotated","site":"iad","durationMs":9,"runId":"pf-0"}`,
		`{"level":"WARN","msg":"promotion binlog rotation failed","site":"iad","durationMs":1000,"timedOut":true,"error":"x"}`,
		`{"level":"WARN","msg":"outgoing primary binlog rotation failed","site":"pdx","durationMs":1000,"timedOut":true,"error":"x"}`,
		warn,
	)
	if got := pitrRotationFailureLine(lines, "iad", logOutgoingBinlogRotated, logOutgoingBinlogRotationFailed); got != warn {
		t.Errorf("failure line %q, want %q", got, warn)
	}
	if got := pitrRotationFailureLine(lines[:3], "iad", logOutgoingBinlogRotated, logOutgoingBinlogRotationFailed); got != "" {
		t.Errorf("no failed outgoing rotation of iad, got %q", got)
	}
	text := `time=now level=WARN msg="outgoing primary binlog rotation failed" site=iad durationMs=4 candidate=pdx timedOut=false error="Error 1227"`
	if got := pitrRotationFailureLine(logLines(text), "iad", logOutgoingBinlogRotated, logOutgoingBinlogRotationFailed); got != text {
		t.Errorf("text-format failure line %q", got)
	}
}

// TestPITRPreRecloneTailOutcome pins the reading of the operator's two
// lines about a reclone's archive-tail wait, in JSON and slog text.
func TestPITRPreRecloneTailOutcome(t *testing.T) {
	const (
		archivedJSON    = `{"time":"2026-11-10T10:00:04Z","level":"INFO","msg":"pre-reclone archive tail archived","site":"iad","waitedMs":4012}`
		archivedText    = `time=2026-11-10T10:00:04Z level=INFO msg="pre-reclone archive tail archived" site=iad waitedMs=4012`
		notArchivedJSON = `{"level":"WARN","msg":"pre-reclone archive tail not archived","site":"iad","waitedMs":120004,"reason":"the site's archiver cannot archive the tail: this site's manifest holds no promotion record"}`
		notArchivedText = `level=WARN msg="pre-reclone archive tail not archived" site=iad waitedMs=120004 reason="FLUSH BINARY LOGS on the site was not acknowledged"`
		started         = `{"level":"INFO","msg":"pre-reclone archive tail wait started","site":"iad","timeout":"2m0s"}`
		rotated         = `{"level":"INFO","msg":"pre-reclone binlog rotated","site":"iad","durationMs":8}`
		otherSite       = `{"level":"INFO","msg":"pre-reclone archive tail archived","site":"reader","waitedMs":2001}`
		otherSiteWarn   = `{"level":"WARN","msg":"pre-reclone archive tail not archived","site":"reader","waitedMs":120001,"reason":"x"}`
	)
	cases := []struct {
		name        string
		lines       []string
		archived    bool
		waited      time.Duration
		notArchived string
	}{
		{name: "archived (JSON)", lines: []string{started, rotated, archivedJSON}, archived: true, waited: 4012 * time.Millisecond},
		{name: "archived (text)", lines: []string{archivedText}, archived: true, waited: 4012 * time.Millisecond},
		{name: "not archived (JSON)", lines: []string{started, notArchivedJSON}, notArchived: notArchivedJSON},
		{name: "not archived (text)", lines: []string{notArchivedText}, notArchived: notArchivedText},
		{name: "wait started, no outcome yet", lines: []string{started, rotated}},
		{name: "another site's outcome", lines: []string{otherSite, otherSiteWarn}},
		{name: "another site's failure next to this site's success", lines: []string{otherSiteWarn, archivedJSON}, archived: true, waited: 4012 * time.Millisecond},
		{name: "nothing", lines: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := pitrPreRecloneTailOutcome(logLines(c.lines...), "iad")
			if got.Archived != c.archived || got.Waited != c.waited || got.NotArchived != c.notArchived {
				t.Errorf("outcome %+v, want archived=%v waited=%s notArchived=%q", got, c.archived, c.waited, c.notArchived)
			}
		})
	}
}

func tailNotArchivedEvent(uid, site string, count int32) corev1.Event {
	return corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "ev-" + uid, Namespace: "bloodraven-playground", UID: types.UID(uid)},
		InvolvedObject: corev1.ObjectReference{Kind: "MysqlFailoverGroup", Name: "playground"},
		Type:           corev1.EventTypeWarning,
		Reason:         v1alpha1.EventPITRArchiveTailNotArchived,
		Message: "site " + site + " is being recloned although its binary log tail was not confirmed archived within 2m0s: the site's archiver still had 1 sealed binary log(s) to archive. " +
			"CLONE wipes the site's binary logs; a point-in-time restore whose window needs transactions only they held refuses PITRArchiveGap.",
		Count: count,
	}
}

// TestPITRNewTailNotArchived pins the Event-absence check of a reclone: a
// new (or recounted) PITRArchiveTailNotArchived Event naming the site
// fails it; older ones, other sites' and other reasons do not.
func TestPITRNewTailNotArchived(t *testing.T) {
	before := map[string]int32{"u1": 1}
	if _, ok := pitrNewTailNotArchived(before, []corev1.Event{tailNotArchivedEvent("u1", "iad", 1)}, "playground", "iad"); ok {
		t.Error("an Event seen before the request must not count")
	}
	if _, ok := pitrNewTailNotArchived(before, []corev1.Event{tailNotArchivedEvent("u1", "iad", 2)}, "playground", "iad"); !ok {
		t.Error("a recounted Event is a new one")
	}
	if ev, ok := pitrNewTailNotArchived(before, []corev1.Event{tailNotArchivedEvent("u2", "iad", 1)}, "playground", "iad"); !ok || ev.UID != "u2" {
		t.Error("a new Event for the site must be found")
	}
	if _, ok := pitrNewTailNotArchived(before, []corev1.Event{tailNotArchivedEvent("u2", "pdx", 1)}, "playground", "iad"); ok {
		t.Error("another site's Event must not count")
	}
	// "iad" must not match a site whose name merely starts with it.
	if _, ok := pitrNewTailNotArchived(before, []corev1.Event{tailNotArchivedEvent("u2", "iad2", 1)}, "playground", "iad"); ok {
		t.Error("site iad2's Event must not count for iad")
	}
	if _, ok := pitrNewTailNotArchived(before, []corev1.Event{tailNotArchivedEvent("u2", "iad", 1)}, "s37-restored", "iad"); ok {
		t.Error("another group's Event must not count")
	}
	rejected := tailNotArchivedEvent("u3", "iad", 1)
	rejected.Reason = v1alpha1.EventRecloneRejected
	if _, ok := pitrNewTailNotArchived(before, []corev1.Event{rejected}, "playground", "iad"); ok {
		t.Error("an Event of another reason must not count")
	}
	pod := tailNotArchivedEvent("u4", "iad", 1)
	pod.InvolvedObject.Kind = "Pod"
	if _, ok := pitrNewTailNotArchived(before, []corev1.Event{pod}, "playground", "iad"); ok {
		t.Error("an Event on another kind must not count")
	}
}

// TestPITRRecloneTailCheck pins what fails a reclone while it runs: the
// Event or the operator's WARN line, each printed; the INFO line alone is
// the confirmation.
func TestPITRRecloneTailCheck(t *testing.T) {
	const (
		archived    = `{"level":"INFO","msg":"pre-reclone archive tail archived","site":"iad","waitedMs":4012}`
		notArchived = `{"level":"WARN","msg":"pre-reclone archive tail not archived","site":"iad","waitedMs":120004,"reason":"FLUSH BINARY LOGS on the site was not acknowledged"}`
	)
	out, err := pitrRecloneTailCheck(nil, nil, logLines(archived), "playground", "iad")
	if err != nil || !out.Archived {
		t.Fatalf("confirmed tail: out=%+v err=%v", out, err)
	}
	out, err = pitrRecloneTailCheck(nil, nil, nil, "playground", "iad")
	if err != nil || out.Archived {
		t.Fatalf("nothing logged yet must be neither a failure nor a confirmation: out=%+v err=%v", out, err)
	}
	if _, err = pitrRecloneTailCheck(nil, nil, logLines(notArchived), "playground", "iad"); err == nil || !strings.Contains(err.Error(), notArchived) {
		t.Fatalf("the WARN line must fail the reclone and be printed: %v", err)
	}
	ev := tailNotArchivedEvent("u1", "iad", 1)
	_, err = pitrRecloneTailCheck(map[string]int32{}, []corev1.Event{ev}, logLines(archived), "playground", "iad")
	if err == nil || !strings.Contains(err.Error(), v1alpha1.EventPITRArchiveTailNotArchived) || !strings.Contains(err.Error(), "still had 1 sealed binary log") {
		t.Fatalf("the Event must fail the reclone and its message be printed: %v", err)
	}
	// An Event from an earlier reclone of the same site (s68 reclones a
	// twice) is in the snapshot taken before this one.
	if _, err = pitrRecloneTailCheck(map[string]int32{"u1": 1}, []corev1.Event{ev}, logLines(archived), "playground", "iad"); err != nil {
		t.Fatalf("an Event that predates the reclone must not fail it: %v", err)
	}
}

// TestPITRAssertRecloneTailArchived runs the post-reclone assertion against
// a fake API server: the operator's INFO line passes it, a new Event or
// the WARN line fails it, and a missing INFO line fails it after the wait.
func TestPITRAssertRecloneTailArchived(t *testing.T) {
	const archived = `{"level":"INFO","msg":"pre-reclone archive tail archived","site":"iad","waitedMs":4012}`
	newEnv := func(t *testing.T, events ...corev1.Event) *runner.Env {
		env := testScenarioEnv(t, testBaselineMFG(false))
		env.Capture = &runner.Capture{}
		for i := range events {
			if _, err := env.Kube.Kubernetes.CoreV1().Events(env.Namespace).Create(context.Background(), &events[i], metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		return env
	}
	lines := func(l ...string) logSource { return func() []pglogs.Match { return logLines(l...) } }

	t.Run("confirmed, no Event", func(t *testing.T) {
		env := newEnv(t)
		before, _, err := pitrRecloneEventSnapshot(context.Background(), env)
		if err != nil {
			t.Fatal(err)
		}
		if err := pitrAssertRecloneTailArchived(context.Background(), env, lines(archived), before, "iad", time.Second); err != nil {
			t.Fatalf("a confirmed tail must pass: %v", err)
		}
	})
	t.Run("an Event from before the reclone is not this reclone's", func(t *testing.T) {
		env := newEnv(t, tailNotArchivedEvent("u1", "iad", 1))
		before, _, err := pitrRecloneEventSnapshot(context.Background(), env)
		if err != nil {
			t.Fatal(err)
		}
		if before["u1"] != 1 {
			t.Fatalf("the snapshot must record the tail Event: %v", before)
		}
		if err := pitrAssertRecloneTailArchived(context.Background(), env, lines(archived), before, "iad", time.Second); err != nil {
			t.Fatalf("an older Event must not fail the reclone: %v", err)
		}
	})
	t.Run("a new Event fails although the INFO line is there", func(t *testing.T) {
		env := newEnv(t)
		before, _, err := pitrRecloneEventSnapshot(context.Background(), env)
		if err != nil {
			t.Fatal(err)
		}
		ev := tailNotArchivedEvent("u2", "iad", 1)
		if _, err := env.Kube.Kubernetes.CoreV1().Events(env.Namespace).Create(context.Background(), &ev, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		err = pitrAssertRecloneTailArchived(context.Background(), env, lines(archived), before, "iad", time.Second)
		if err == nil || !strings.Contains(err.Error(), v1alpha1.EventPITRArchiveTailNotArchived) {
			t.Fatalf("a new PITRArchiveTailNotArchived Event must fail the reclone: %v", err)
		}
	})
	t.Run("no INFO line", func(t *testing.T) {
		env := newEnv(t)
		start := time.Now()
		err := pitrAssertRecloneTailArchived(context.Background(), env, lines(`{"level":"INFO","msg":"pre-reclone archive tail wait started","site":"iad","timeout":"2m0s"}`), map[string]int32{}, "iad", 50*time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), logPreRecloneTailArchived) || !strings.Contains(err.Error(), "iad") {
			t.Fatalf("a reclone without the operator's confirmation must fail: %v", err)
		}
		if time.Since(start) > 5*time.Second {
			t.Errorf("the wait overran: %s", time.Since(start))
		}
	})
}

// TestPITRRecloneEventSnapshotCountsBothReasons: the snapshot a reclone
// takes before its request covers the two Events it later looks for.
func TestPITRRecloneEventSnapshotCountsBothReasons(t *testing.T) {
	env := testScenarioEnv(t, testBaselineMFG(false))
	rejected := tailNotArchivedEvent("r1", "iad", 3)
	rejected.Reason = v1alpha1.EventRecloneRejected
	other := tailNotArchivedEvent("o1", "iad", 1)
	other.Reason = "FailoverCompleted"
	for _, ev := range []corev1.Event{tailNotArchivedEvent("t1", "iad", 2), rejected, other} {
		if _, err := env.Kube.Kubernetes.CoreV1().Events(env.Namespace).Create(context.Background(), &ev, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	seen, events, err := pitrRecloneEventSnapshot(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]int32{"t1": 2, "r1": 3}; !reflect.DeepEqual(seen, want) {
		t.Errorf("snapshot %v, want %v", seen, want)
	}
	if len(events) != 3 {
		t.Errorf("events=%d, want all 3 returned", len(events))
	}
}

// pitrScenarioSteps returns a registered scenario's step names.
func pitrScenarioSteps(t *testing.T, id string) (runner.Scenario, []string) {
	t.Helper()
	s, ok := runner.DefaultRegistry.Get(id)
	if !ok {
		t.Fatalf("scenario %s is not registered", id)
	}
	names := make([]string, 0, len(s.Steps))
	for _, step := range s.Steps {
		names = append(names, step.Name)
	}
	return s, names
}

func stepIndex(t *testing.T, names []string, fragment string) int {
	t.Helper()
	for i, n := range names {
		if strings.Contains(n, fragment) {
			return i
		}
	}
	t.Fatalf("no step named like %q (steps %q)", fragment, names)
	return -1
}

// TestPITRScenariosAssertArchivedTail pins the #264 inventory of the
// cross-site PITR scenarios: 37 and 68 verify inside the tail a planned
// failback archives (68 no longer expects PITRArchiveGap), and 70 brings
// the old primary back, with its unflushed lost tail archived, before it
// reclones it.
func TestPITRScenariosAssertArchivedTail(t *testing.T) {
	s37, n37 := pitrScenarioSteps(t, s37ID)
	failback := stepIndex(t, n37, "planned B→A")
	tail := stepIndex(t, n37, "right after the failback")
	eraA2 := stepIndex(t, n37, "era A': marker M3")
	if !(failback < tail && tail < eraA2) {
		t.Errorf("s37 steps %q: the tail verification must run after the failback and before a's next flush", n37)
	}
	if !strings.Contains(s37.Hypothesis, "outgoing primary's tail") {
		t.Errorf("s37 hypothesis must state that the failback archives b's era: %s", s37.Hypothesis)
	}

	s68, n68 := pitrScenarioSteps(t, s68ID)
	stepIndex(t, n68, "inside b's unflushed era (T4) and after it (T5)")
	for _, text := range append([]string{s68.Title, s68.Hypothesis}, n68...) {
		if strings.Contains(text, "refuses PITRArchiveGap") {
			t.Errorf("s68 still expects a PITRArchiveGap refusal: %q", text)
		}
	}
	if !strings.Contains(s68.Hypothesis, v1alpha1.EventPITRArchiveTailNotArchived) {
		t.Errorf("s68 hypothesis must state the absent Event: %s", s68.Hypothesis)
	}

	s70, n70 := pitrScenarioSteps(t, s70ID)
	back := stepIndex(t, n70, "a comes back")
	reclone := stepIndex(t, n70, "reclone a")
	afterLoss := stepIndex(t, n70, "a backup taken after the loss")
	if !(afterLoss < back && back < reclone) {
		t.Errorf("s70 steps %q: a must come back (and its tail be archived and verified) before it is recloned", n70)
	}
	if !strings.Contains(s70.Hypothesis, "L ∪ L'") {
		t.Errorf("s70 hypothesis must state the unflushed lost tail: %s", s70.Hypothesis)
	}
	// A reclone with PITR enabled waits up to pitrPreRecloneTailWait before
	// the clone; the scenario budgets must still hold their reclones.
	for _, s := range []runner.Scenario{s68, s70} {
		if s.Timeout < 60*time.Minute {
			t.Errorf("%s Timeout=%s was lowered below the budget its reclones were sized for", s.ID, s.Timeout)
		}
	}
	if s70RepairTimeout < 8*time.Minute+pitrPreRecloneTailWait {
		t.Errorf("s70RepairTimeout=%s does not cover a cleanup reclone (8m) plus the pre-reclone tail wait %s", s70RepairTimeout, pitrPreRecloneTailWait)
	}
}
