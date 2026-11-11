package scenarios

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	brcontroller "github.com/shipstream/bloodraven/internal/controller"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func logLines(lines ...string) []pglogs.Match {
	out := make([]pglogs.Match, len(lines))
	for i, l := range lines {
		out[i] = pglogs.Match{Time: time.Unix(1_800_000_000+int64(i), 0), Line: l}
	}
	return out
}

// TestParseBinlogRotation pins the reading of the operator's rotation lines
// in both formats it may be deployed with: the JSON its logger writes and
// slog text.
func TestParseBinlogRotation(t *testing.T) {
	const done, failed = logPromotionBinlogRotated, logPromotionBinlogRotationFailed
	cases := []struct {
		name string
		line string
		ok   bool
		want binlogRotationLog
	}{
		{
			name: "JSON INFO",
			line: `{"time":"2026-11-10T10:00:07.5Z","level":"INFO","msg":"promotion binlog rotated","site":"pdx","durationMs":12}`,
			ok:   true,
			want: binlogRotationLog{Site: "pdx", Duration: 12 * time.Millisecond, DurationKnown: true},
		},
		{
			name: "text INFO",
			line: `time=2026-11-10T10:00:07.5Z level=INFO msg="promotion binlog rotated" site=pdx durationMs=12`,
			ok:   true,
			want: binlogRotationLog{Site: "pdx", Duration: 12 * time.Millisecond, DurationKnown: true},
		},
		{
			name: "JSON INFO of zero milliseconds",
			line: `{"level":"INFO","msg":"promotion binlog rotated","site":"iad","durationMs":0}`,
			ok:   true,
			want: binlogRotationLog{Site: "iad", DurationKnown: true},
		},
		{
			name: "JSON WARN timed out",
			line: `{"level":"WARN","msg":"promotion binlog rotation failed","site":"pdx","durationMs":1000,"timedOut":true,"error":"no answer within 1s; continuing without it"}`,
			ok:   true,
			want: binlogRotationLog{Site: "pdx", Duration: time.Second, DurationKnown: true, Failed: true, TimedOut: true, Error: "no answer within 1s; continuing without it"},
		},
		{
			name: "text WARN with an error that is not a timeout",
			line: `level=WARN msg="promotion binlog rotation failed" site=pdx durationMs=3 timedOut=false error="Error 1227 (42000): Access denied; you need the RELOAD privilege"`,
			ok:   true,
			want: binlogRotationLog{Site: "pdx", Duration: 3 * time.Millisecond, DurationKnown: true, Failed: true, Error: "Error 1227 (42000): Access denied; you need the RELOAD privilege"},
		},
		{
			name: "INFO without durationMs",
			line: `{"level":"INFO","msg":"promotion binlog rotated","site":"pdx"}`,
			ok:   true,
			want: binlogRotationLog{Site: "pdx"},
		},
		{
			name: "INFO with a durationMs that is not a whole number",
			line: `{"level":"INFO","msg":"promotion binlog rotated","site":"pdx","durationMs":"12ms"}`,
			ok:   true,
			want: binlogRotationLog{Site: "pdx"},
		},
		{
			name: "a negative durationMs is not a duration",
			line: `{"level":"INFO","msg":"promotion binlog rotated","site":"pdx","durationMs":-5}`,
			ok:   true,
			want: binlogRotationLog{Site: "pdx"},
		},
		{name: "another rotation's line", line: `{"level":"INFO","msg":"outgoing primary binlog rotated","site":"pdx","durationMs":12}`},
		{name: "the message only as a substring of another msg", line: `{"level":"INFO","msg":"not a promotion binlog rotated line","site":"pdx","durationMs":12}`},
		{name: "the message in another field", line: `{"level":"INFO","msg":"something else","note":"promotion binlog rotated","site":"pdx"}`},
		{name: "unstructured text that quotes the message", line: `promotion binlog rotated on pdx in 12ms`},
		{name: "unrelated line", line: `{"level":"INFO","msg":"failover complete","target":"pdx"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseBinlogRotation(c.line, done, failed)
			if ok != c.ok {
				t.Fatalf("ok=%v, want %v (%+v)", ok, c.ok, got)
			}
			if !ok {
				return
			}
			c.want.Line = c.line
			if got != c.want {
				t.Errorf("got %+v\nwant %+v", got, c.want)
			}
		})
	}
}

// TestAssessBinlogRotation pins how scenario 01 reads the promotion's FLUSH
// BINARY LOGS from the operator's log: the first rotation line for the
// promoted site, acknowledged or not. The rotation is best effort, so a
// WARN line, or an acknowledgement at or over the bound, is a measurement
// with a concern to report, never an error; the only errors are a missing
// line (wait more) and an INFO line that breaks the log schema.
func TestAssessBinlogRotation(t *testing.T) {
	const done, failed = logPromotionBinlogRotated, logPromotionBinlogRotationFailed
	info := func(site string, ms int) string {
		return `{"level":"INFO","msg":"promotion binlog rotated","site":"` + site + `","durationMs":` + strconv.Itoa(ms) + `}`
	}
	warn := func(site string) string {
		return `{"level":"WARN","msg":"promotion binlog rotation failed","site":"` + site + `","durationMs":1000,"timedOut":true,"error":"no answer within 1s; continuing without it"}`
	}
	bound := brcontroller.BinlogRotationTimeout
	cases := []struct {
		name        string
		lines       []string
		wantMs      int64
		wantAcked   bool
		wantConcern string // "" for an acknowledged rotation under the bound
		wantErr     string
		waitMore    bool
	}{
		{name: "acknowledged", lines: []string{`{"msg":"failover started"}`, info("pdx", 12)}, wantMs: 12, wantAcked: true},
		{name: "acknowledged in text format", lines: []string{`level=INFO msg="promotion binlog rotated" site=pdx durationMs=40`}, wantMs: 40, wantAcked: true},
		{name: "nothing logged yet", lines: []string{`{"msg":"failover started"}`}, waitMore: true},
		{name: "only another site's rotation", lines: []string{info("iad", 12)}, waitMore: true},
		{name: "another site's failure does not count against pdx", lines: []string{warn("iad"), info("pdx", 9)}, wantMs: 9, wantAcked: true},
		{name: "not acknowledged is a concern, not an error", lines: []string{warn("pdx")}, wantMs: 1000, wantConcern: "was not acknowledged (timedOut=true"},
		{name: "the first line for the site is the one reported", lines: []string{info("pdx", 12), warn("pdx")}, wantMs: 12, wantAcked: true},
		{name: "a failure first is the one reported", lines: []string{warn("pdx"), info("pdx", 12)}, wantMs: 1000, wantConcern: "the operator logged: "},
		{name: "at the bound is a concern, not an error", lines: []string{info("pdx", 1000)}, wantMs: 1000, wantAcked: true, wantConcern: "took 1s, not under its 1s bound"},
		{name: "over the bound is a concern, not an error", lines: []string{info("pdx", 1500)}, wantMs: 1500, wantAcked: true, wantConcern: "took 1.5s, not under its 1s bound"},
		{name: "just under the bound", lines: []string{info("pdx", 999)}, wantMs: 999, wantAcked: true},
		{name: "no durationMs", lines: []string{`{"level":"INFO","msg":"promotion binlog rotated","site":"pdx"}`}, wantErr: "carries no usable durationMs"},
		{name: "the first acknowledged rotation is the one reported", lines: []string{info("pdx", 12), info("pdx", 700)}, wantMs: 12, wantAcked: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := assessBinlogRotation(logLines(c.lines...), "pdx", done, failed)
			switch {
			case c.waitMore:
				if !errors.Is(err, errNoRotationLine) {
					t.Fatalf("err=%v, want errNoRotationLine", err)
				}
			case c.wantErr != "":
				if err == nil || errors.Is(err, errNoRotationLine) || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err=%v, want one containing %q", err, c.wantErr)
				}
			default:
				if err != nil {
					t.Fatalf("a best-effort rotation's outcome must not be an error: %v", err)
				}
				if got.Duration.Milliseconds() != c.wantMs || got.Site != "pdx" || got.acknowledged() != c.wantAcked {
					t.Errorf("rotation %+v, want %dms on pdx, acknowledged=%v", got, c.wantMs, c.wantAcked)
				}
				concern := got.concern(bound)
				if (concern == "") != (c.wantConcern == "") || !strings.Contains(concern, c.wantConcern) {
					t.Errorf("concern %q, want one containing %q", concern, c.wantConcern)
				}
				if concern != "" && !strings.Contains(concern, "best effort") {
					t.Errorf("concern %q must say the rotation is best effort", concern)
				}
			}
		})
	}
	// The concern prints the operator's WARN line itself.
	rec, err := assessBinlogRotation(logLines(warn("pdx")), "pdx", done, failed)
	if err != nil || !strings.Contains(rec.concern(bound), warn("pdx")) {
		t.Errorf("the concern must print the WARN line: %q (err %v)", rec.concern(bound), err)
	}
	// No line at all is not an acknowledged rotation.
	if (binlogRotationLog{}).acknowledged() {
		t.Error("a rotation with no log line counts as acknowledged")
	}
}

// TestWaitBinlogRotation pins the wait: it returns as soon as the line is
// there, fails at once on a WARN line, and times out naming both messages.
func TestWaitBinlogRotation(t *testing.T) {
	const done, failed = logPromotionBinlogRotated, logPromotionBinlogRotationFailed
	var mu sync.Mutex
	var lines []pglogs.Match
	src := func() []pglogs.Match {
		mu.Lock()
		defer mu.Unlock()
		return append([]pglogs.Match(nil), lines...)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		lines = logLines(`{"level":"INFO","msg":"promotion binlog rotated","site":"pdx","durationMs":7}`)
		mu.Unlock()
	}()
	rec, err := waitBinlogRotation(context.Background(), src, "pdx", done, failed, 10*time.Second)
	if err != nil || rec.Duration != 7*time.Millisecond || !rec.acknowledged() {
		t.Fatalf("rotation %+v err %v, want 7ms, acknowledged", rec, err)
	}

	warn := logLines(`{"level":"WARN","msg":"promotion binlog rotation failed","site":"pdx","durationMs":1000,"timedOut":true,"error":"x"}`)
	begin := time.Now()
	rec, err = waitBinlogRotation(context.Background(), func() []pglogs.Match { return warn }, "pdx", done, failed, 10*time.Second)
	if err != nil || rec.acknowledged() || !rec.TimedOut || time.Since(begin) > 5*time.Second {
		t.Fatalf("a WARN line must end the wait at once as an unacknowledged rotation, without an error: %+v, %v after %s", rec, err, time.Since(begin))
	}

	start := time.Now()
	_, err = waitBinlogRotation(context.Background(), func() []pglogs.Match { return nil }, "pdx", done, failed, 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), done) || !strings.Contains(err.Error(), failed) || !strings.Contains(err.Error(), "pdx") {
		t.Fatalf("timeout error %v must name both messages and the site", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("the wait overran its timeout: %s", time.Since(start))
	}
}

func TestLinesSince(t *testing.T) {
	lines := logLines("a", "b", "c")
	if got := linesSince(lines, lines[1].Time); len(got) != 2 || got[0].Line != "b" {
		t.Errorf("since the second line: %v", got)
	}
	if got := linesSince(lines, time.Time{}); len(got) != 3 {
		t.Errorf("a zero since keeps every line: %v", got)
	}
	if got := linesSince(lines, lines[2].Time.Add(time.Nanosecond)); len(got) != 0 {
		t.Errorf("since after the last line: %v", got)
	}
}

// fakeProbeTarget is a site whose role and write outcome the test sets.
type fakeProbeTarget struct {
	mu       sync.Mutex
	readOnly bool
	roErr    error
	writeErr error
	reads    int
	writes   int
}

func (f *fakeProbeTarget) ReadOnly(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	return f.readOnly, f.roErr
}

func (f *fakeProbeTarget) Write(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	return f.writeErr
}

func (f *fakeProbeTarget) set(fn func(*fakeProbeTarget)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeProbeTarget) counts() (reads, writes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads, f.writes
}

func awaitProbe(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestWriteProbeNeverWritesWhileReadOnly pins the probe's safety rule (its
// account is root: a write on a replica would be an errant transaction) and
// its measurement: the first write is sent only once the site reports
// itself writable, its acknowledgement is recorded once, and the probe
// stops writing after it.
func TestWriteProbeNeverWritesWhileReadOnly(t *testing.T) {
	target := &fakeProbeTarget{readOnly: true}
	closed := 0
	p := runWriteProbe("pdx", target, time.Millisecond, func() { closed++ })
	awaitProbe(t, func() bool { r, _ := target.counts(); return r >= 5 }, "five read-only rounds")
	if _, w := target.counts(); w != 0 {
		t.Fatalf("the probe wrote %d times to a read-only site", w)
	}
	if _, ok := p.firstAckAt(); ok {
		t.Fatal("a first ack was recorded while the site was read-only")
	}
	before := time.Now()
	target.set(func(f *fakeProbeTarget) { f.readOnly = false })
	awaitProbe(t, func() bool { _, ok := p.firstAckAt(); return ok }, "the first acknowledged write")
	at, _ := p.firstAckAt()
	if at.Before(before) || at.After(time.Now()) {
		t.Errorf("first ack at %s, want between %s and now", at, before)
	}
	// One write is the measurement; the probe does not keep writing.
	time.Sleep(20 * time.Millisecond)
	if _, w := target.counts(); w != 1 {
		t.Errorf("the probe wrote %d times, want exactly 1", w)
	}
	if again, _ := p.firstAckAt(); !again.Equal(at) {
		t.Errorf("the first ack moved from %s to %s", at, again)
	}
	p.stop()
	p.stop()
	if closed != 1 {
		t.Errorf("the connection was closed %d times, want 1", closed)
	}
	if s := p.summary(); !strings.Contains(s, "pdx") || !strings.Contains(s, "acked=true") {
		t.Errorf("summary %q", s)
	}
}

// TestWriteProbeRetriesFailures: a failed flag read or a refused write is
// counted and retried, and is never taken for the first write.
func TestWriteProbeRetriesFailures(t *testing.T) {
	target := &fakeProbeTarget{roErr: errors.New("connection refused")}
	p := runWriteProbe("pdx", target, time.Millisecond, nil)
	defer p.stop()
	awaitProbe(t, func() bool { r, _ := target.counts(); return r >= 3 }, "three failed reads")
	if _, w := target.counts(); w != 0 {
		t.Fatalf("the probe wrote although it could not read the role (%d writes)", w)
	}
	// Writable, but the write itself is refused (the fence came back).
	target.set(func(f *fakeProbeTarget) { f.roErr, f.writeErr = nil, errors.New("Error 1290: --super-read-only") })
	awaitProbe(t, func() bool { _, w := target.counts(); return w >= 3 }, "three refused writes")
	if _, ok := p.firstAckAt(); ok {
		t.Fatal("a refused write was recorded as acknowledged")
	}
	if s := p.summary(); !strings.Contains(s, "1290") || !strings.Contains(s, "acked=false") {
		t.Errorf("summary %q must carry the last error", s)
	}
	target.set(func(f *fakeProbeTarget) { f.writeErr = nil })
	awaitProbe(t, func() bool { _, ok := p.firstAckAt(); return ok }, "the first acknowledged write")
}

func TestWriteProbeStopBeforeFirstAck(t *testing.T) {
	target := &fakeProbeTarget{readOnly: true}
	p := runWriteProbe("pdx", target, time.Millisecond, nil)
	p.stop()
	r, _ := target.counts()
	time.Sleep(10 * time.Millisecond)
	if again, _ := target.counts(); again != r {
		t.Errorf("the probe kept running after stop (%d -> %d reads)", r, again)
	}
	var none *writeProbe
	none.stop() // a probe that was never started
}

func TestWriteProbeTableDDL(t *testing.T) {
	got := writeProbeTableDDL("chaos_s01", "rto_probe")
	for _, frag := range []string{"CREATE DATABASE IF NOT EXISTS `chaos_s01`", "CREATE TABLE IF NOT EXISTS `chaos_s01`.`rto_probe`", "AUTO_INCREMENT PRIMARY KEY"} {
		if !strings.Contains(got, frag) {
			t.Errorf("DDL %q lacks %q", got, frag)
		}
	}
}

// TestFailoverRTO pins what scenario 01 reports and asserts: the RTO is
// fault to first acknowledged write on the new primary, it must be inside
// the failover budget, and the report names both durations and the flush.
func TestFailoverRTO(t *testing.T) {
	fault := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)
	good := func() failoverRTO {
		return failoverRTO{
			Old: "iad", New: "pdx",
			FaultAt: fault, LastOldAck: fault.Add(350 * time.Millisecond), FirstNewAck: fault.Add(8200 * time.Millisecond),
			LoadAcked:     61,
			Rotation:      binlogRotationLog{Site: "pdx", Duration: 12 * time.Millisecond, DurationKnown: true},
			RotationBound: brcontroller.BinlogRotationTimeout,
		}
	}
	m := good()
	if m.rto() != 8200*time.Millisecond || m.outage() != 7850*time.Millisecond {
		t.Fatalf("rto=%s outage=%s, want 8.2s and 7.85s", m.rto(), m.outage())
	}
	if err := m.check(failoverObserveBudget); err != nil {
		t.Fatalf("a failover inside the budget failed: %v", err)
	}
	want := "failover under load iad->pdx: rto=8.2s (fault to first acknowledged write on pdx), write outage=7.85s (last ack on iad to first ack on pdx), " +
		"promotion FLUSH BINARY LOGS=12ms (bound 1s, 0.15% of rto), load acked=61 before the outage"
	if got := m.String(); got != want {
		t.Errorf("report\n%s\nwant\n%s", got, want)
	}

	// A rotation that failed, or was never logged, is reported as such
	// rather than as a duration, and neither it nor a slow one fails the
	// failover's check: the rotation is best effort, the budget is not.
	failedRot := good()
	failedRot.Rotation = binlogRotationLog{Site: "pdx", Duration: time.Second, DurationKnown: true, Failed: true, TimedOut: true, Line: "x"}
	if got := failedRot.String(); !strings.Contains(got, "promotion FLUSH BINARY LOGS=not acknowledged after 1000ms (timedOut=true, bound 1s)") || !strings.Contains(got, "rto=8.2s") {
		t.Errorf("report of a failed rotation: %s", got)
	}
	if err := failedRot.check(failoverObserveBudget); err != nil {
		t.Errorf("an unacknowledged rotation failed a failover inside its budget: %v", err)
	}
	slowRot := good()
	slowRot.Rotation = binlogRotationLog{Site: "pdx", Duration: 1200 * time.Millisecond, DurationKnown: true, Line: "x"}
	if got := slowRot.String(); !strings.Contains(got, "promotion FLUSH BINARY LOGS=acknowledged after 1200ms, not under its bound 1s") {
		t.Errorf("report of a slow rotation: %s", got)
	}
	if err := slowRot.check(failoverObserveBudget); err != nil {
		t.Errorf("a slow rotation failed a failover inside its budget: %v", err)
	}
	// The budget still fails, whatever the rotation did.
	late := failedRot
	late.FirstNewAck = fault.Add(failoverObserveBudget + time.Second)
	if err := late.check(failoverObserveBudget); err == nil || !strings.Contains(err.Error(), "failover budget") {
		t.Errorf("a failover over its budget passed: %v", err)
	}
	unlogged := good()
	unlogged.Rotation = binlogRotationLog{}
	if got := unlogged.String(); !strings.Contains(got, "promotion FLUSH BINARY LOGS=not measured (bound 1s)") {
		t.Errorf("report without a rotation line: %s", got)
	}

	cases := []struct {
		name    string
		mutate  func(*failoverRTO)
		budget  time.Duration
		wantErr string
	}{
		{"exactly at the budget", func(m *failoverRTO) { m.FirstNewAck = fault.Add(failoverObserveBudget) }, failoverObserveBudget, ""},
		{"over the budget", func(m *failoverRTO) { m.FirstNewAck = fault.Add(failoverObserveBudget + time.Millisecond) }, failoverObserveBudget, "over the 1m30s failover budget"},
		{"no write on the new primary", func(m *failoverRTO) { m.FirstNewAck = time.Time{} }, failoverObserveBudget, "acknowledged no write"},
		{"no load before the fault", func(m *failoverRTO) { m.LoadAcked, m.LastOldAck = 0, time.Time{} }, failoverObserveBudget, "not under load"},
		{"a write on the target before the fault", func(m *failoverRTO) { m.FirstNewAck = fault.Add(-time.Second) }, failoverObserveBudget, "not a read-only replica"},
		{"the old primary acknowledging after the fault is fine", func(m *failoverRTO) { m.LastOldAck = fault.Add(2 * time.Second) }, failoverObserveBudget, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := good()
			c.mutate(&m)
			err := m.check(c.budget)
			if (err == nil) != (c.wantErr == "") || (err != nil && !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("err=%v, want %q", err, c.wantErr)
			}
		})
	}
}

// TestScenario01MeasuresFailoverUnderLoad pins where the #229 RTO
// measurement lives: scenario 01, which the smoke profile (the PR-labelled
// E2E job and the release gate) and the release profile (nightly and
// post-release) both run, with the load started before the fault and the
// measurement after the existing failover assertions.
func TestScenario01MeasuresFailoverUnderLoad(t *testing.T) {
	s, ok := runner.DefaultRegistry.Get(s01ID)
	if !ok {
		t.Fatalf("scenario %s is not registered", s01ID)
	}
	if s.Quarantine != "" {
		t.Fatalf("scenario %s must not be quarantined: %q", s01ID, s.Quarantine)
	}
	all := runner.DefaultRegistry.List()
	for _, p := range []runner.Profile{runner.ProfileSmoke, runner.ProfileRelease, runner.ProfileFull} {
		if !inventoryContains(runner.SelectForProfile(all, p), s01ID) {
			t.Errorf("scenario %s must be in the %s profile", s01ID, p)
		}
	}
	var names []string
	for _, step := range s.Steps {
		names = append(names, step.Name)
	}
	index := func(fragment string) int {
		for i, n := range names {
			if strings.Contains(n, fragment) {
				return i
			}
		}
		t.Fatalf("scenario %s has no step named like %q (steps %q)", s01ID, fragment, names)
		return -1
	}
	load, fault, flip, measure := index("start a write load"), index("scale active primary to 0"), index("wait for activeSite to flip"), index("promotion FLUSH BINARY LOGS measured")
	if !(load < fault && fault < flip && flip < measure) || measure != len(names)-1 {
		t.Errorf("steps %q: want the load before the fault and the measurement last", names)
	}
	// The rotation is best effort: no step may be named as asserting it.
	for _, n := range names {
		if strings.Contains(n, "FLUSH BINARY LOGS acknowledged") {
			t.Errorf("step %q asserts the promotion's rotation; it is a reported measurement", n)
		}
	}
	// The existing waits keep their budgets: the observe wait, the metric
	// and log waits (30s each), plus the measurement's two waits, the
	// promotion binlog file check and the load's start.
	if min := failoverObserveBudget + 2*30*time.Second + 2*s01LogWait + s01PromotionFileTimeout + s01LoadWarmup; s.Timeout < min {
		t.Errorf("Timeout=%s is below the sum of the scenario's waits %s", s.Timeout, min)
	}
	if s.CleanupTimeout < s01RepairTimeout+time.Minute {
		t.Errorf("CleanupTimeout=%s leaves no room after the %s repair wait", s.CleanupTimeout, s01RepairTimeout)
	}
	if s.Cleanup == nil {
		t.Error("the scenario must stop its load and probe in Cleanup")
	}
	for _, frag := range []string{brcontroller.BinlogRotationTimeout.String(), failoverObserveBudget.String(), "FLUSH BINARY LOGS"} {
		if !strings.Contains(s.Hypothesis, frag) {
			t.Errorf("the hypothesis must state the bound %q: %s", frag, s.Hypothesis)
		}
	}
}

// TestOperatorLogMirrors pins the playground's copies of operator strings
// and bounds the scenarios read from its log and Events: each message is a
// literal of the controller package (they are in the log schema, so a
// rename is a contract change that must reach the scenarios too), the
// pre-reclone bound equals the operator's, and the Event message still
// starts the way pitrNewTailNotArchived matches it.
func TestOperatorLogMirrors(t *testing.T) {
	dir := filepath.Join("..", "..", "controller")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var src strings.Builder
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src.Write(raw)
	}
	code := src.String()
	for _, msg := range []string{
		logPromotionBinlogRotated, logPromotionBinlogRotationFailed,
		logOutgoingBinlogRotated, logOutgoingBinlogRotationFailed,
		logPreRecloneTailArchived, logPreRecloneTailNotArchived,
	} {
		if !strings.Contains(code, strconv.Quote(msg)) {
			t.Errorf("internal/controller has no string literal %q; update the playground's mirror", msg)
		}
	}
	// The fields the scenarios read from those lines.
	for _, field := range []string{`"durationMs"`, `"timedOut"`, `"waitedMs"`} {
		if !strings.Contains(code, field) {
			t.Errorf("internal/controller logs no field %s any more", field)
		}
	}
	m := regexp.MustCompile(`preRecloneArchiveTailTimeout\s*=\s*(\d+)\s*\*\s*time\.(Minute|Second)`).FindStringSubmatch(code)
	if m == nil {
		t.Fatal("internal/controller declares no preRecloneArchiveTailTimeout = <n> * time.<unit>; update the mirror and this test")
	}
	n, _ := strconv.Atoi(m[1])
	unit := time.Minute
	if m[2] == "Second" {
		unit = time.Second
	}
	if got := time.Duration(n) * unit; got != pitrPreRecloneTailWait {
		t.Errorf("pitrPreRecloneTailWait=%s, operator preRecloneArchiveTailTimeout=%s", pitrPreRecloneTailWait, got)
	}
	if !strings.Contains(code, `"site %s is being recloned although`) {
		t.Error("the PITRArchiveTailNotArchived Event message no longer starts \"site <name> is being recloned although\"; update pitrNewTailNotArchived")
	}
}

// fakeBinlogHeaders is a binlogHeaderReader over a fixed index.
type fakeBinlogHeaders struct {
	files    []string
	prev     map[string]string
	listErr  error
	readErr  map[string]error
	readLog  []string
	listings int
}

func (f *fakeBinlogHeaders) BinlogFiles(context.Context) ([]string, error) {
	f.listings++
	return f.files, f.listErr
}

func (f *fakeBinlogHeaders) PreviousGtids(_ context.Context, file string) (string, error) {
	f.readLog = append(f.readLog, file)
	if err := f.readErr[file]; err != nil {
		return "", err
	}
	return f.prev[file], nil
}

// TestCheckPromotionBinlogFile pins scenario 01's check of the real file:
// a file that starts at the recorded promotion GTID set verifies it,
// wherever it sits in the index; only an index that proves no file starts
// there (one starts before it, the next past it) is a mismatch; anything
// that cannot be compared is "not checked", never a failure.
func TestCheckPromotionBinlogFile(t *testing.T) {
	const a, b = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	recorded := a + ":1-100"
	cases := []struct {
		name     string
		recorded string
		h        *fakeBinlogHeaders
		verdict  promotionFileVerdict
		file     string
		detail   string
		reads    []string
	}{
		{
			name: "the newest file starts at the recorded set", recorded: recorded,
			h:       &fakeBinlogHeaders{files: []string{"binlog.000006", "binlog.000007"}, prev: map[string]string{"binlog.000006": a + ":1-40", "binlog.000007": a + ":1-100"}},
			verdict: promotionFileVerified, file: "binlog.000007", detail: "binlog.000007 starts at the recorded promotion GTID set", reads: []string{"binlog.000007"},
		},
		{
			name: "the new primary rotated again since", recorded: recorded,
			h: &fakeBinlogHeaders{files: []string{"binlog.000006", "binlog.000007", "binlog.000008"},
				prev: map[string]string{"binlog.000006": a + ":1-40", "binlog.000007": a + ":1-100", "binlog.000008": a + ":1-100," + b + ":1-9"}},
			verdict: promotionFileVerified, file: "binlog.000007", reads: []string{"binlog.000008", "binlog.000007"},
		},
		{
			name: "the same set written differently", recorded: a + ":1-50:51-100",
			h:       &fakeBinlogHeaders{files: []string{"binlog.000007"}, prev: map[string]string{"binlog.000007": a + ":1-100"}},
			verdict: promotionFileVerified, file: "binlog.000007",
		},
		{
			name: "a multi-line recorded set", recorded: a + ":1-100,\n" + b + ":1-3",
			h:       &fakeBinlogHeaders{files: []string{"binlog.000007"}, prev: map[string]string{"binlog.000007": a + ":1-100," + b + ":1-3"}},
			verdict: promotionFileVerified, file: "binlog.000007",
		},
		{
			name: "the rotation's file starts past the recorded set", recorded: recorded,
			h:       &fakeBinlogHeaders{files: []string{"binlog.000006", "binlog.000007"}, prev: map[string]string{"binlog.000006": a + ":1-40", "binlog.000007": a + ":1-100," + b + ":1"}},
			verdict: promotionFileMismatch, file: "binlog.000006",
			detail: "no binary log file starts at the recorded promotion GTID set: binlog.000006 starts 60 transaction(s) before it (" + a + ":41-100) and binlog.000007 starts past it",
		},
		{
			name: "no file was opened at the promotion", recorded: recorded,
			h:       &fakeBinlogHeaders{files: []string{"binlog.000006"}, prev: map[string]string{"binlog.000006": a + ":1-40"}},
			verdict: promotionFileMismatch, file: "binlog.000006", detail: "and no later file exists",
		},
		{
			name: "an empty record", recorded: " ",
			h:       &fakeBinlogHeaders{files: []string{"binlog.000007"}, prev: map[string]string{"binlog.000007": a + ":1-100"}},
			verdict: promotionFileNotChecked, detail: "promotionGtidExecuted is empty", reads: nil,
		},
		{
			name: "an unparsable record", recorded: "not a gtid set",
			h:       &fakeBinlogHeaders{files: []string{"binlog.000007"}},
			verdict: promotionFileNotChecked, detail: "does not parse",
		},
		{
			name: "the index cannot be listed", recorded: recorded,
			h:       &fakeBinlogHeaders{listErr: errors.New("connection refused")},
			verdict: promotionFileNotChecked, detail: "list the binary logs: connection refused",
		},
		{
			name: "an empty index", recorded: recorded,
			h:       &fakeBinlogHeaders{},
			verdict: promotionFileNotChecked, detail: "lists no binary log",
		},
		{
			name: "a header cannot be read", recorded: recorded,
			h: &fakeBinlogHeaders{files: []string{"binlog.000006", "binlog.000007"}, prev: map[string]string{"binlog.000006": a + ":1-100"},
				readErr: map[string]error{"binlog.000007": errors.New("i/o timeout")}},
			verdict: promotionFileNotChecked, detail: "read the Previous_gtids of binlog.000007: i/o timeout",
		},
		{
			name: "a header that does not parse", recorded: recorded,
			h:       &fakeBinlogHeaders{files: []string{"binlog.000007"}, prev: map[string]string{"binlog.000007": "garbage"}},
			verdict: promotionFileNotChecked, detail: "do not parse",
		},
		{
			name: "sets that do not nest", recorded: recorded,
			h:       &fakeBinlogHeaders{files: []string{"binlog.000007"}, prev: map[string]string{"binlog.000007": a + ":1-90," + b + ":1"}},
			verdict: promotionFileNotChecked, detail: "do not nest",
		},
		{
			name: "the file was purged", recorded: recorded,
			h:       &fakeBinlogHeaders{files: []string{"binlog.000008", "binlog.000009"}, prev: map[string]string{"binlog.000008": a + ":1-100," + b + ":1-5", "binlog.000009": a + ":1-100," + b + ":1-9"}},
			verdict: promotionFileNotChecked, detail: "no longer on the server", reads: []string{"binlog.000009", "binlog.000008"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := checkPromotionBinlogFile(context.Background(), c.h, c.recorded)
			if got.Verdict != c.verdict || got.File != c.file || !strings.Contains(got.Detail, c.detail) {
				t.Fatalf("got %+v\nwant verdict %d file %q detail containing %q", got, c.verdict, c.file, c.detail)
			}
			if c.reads != nil && strings.Join(c.h.readLog, ",") != strings.Join(c.reads, ",") {
				t.Errorf("headers read %v, want %v (newest first, stopping at the answer)", c.h.readLog, c.reads)
			}
			if c.name == "an empty record" && (c.h.listings != 0 || len(c.h.readLog) != 0) {
				t.Errorf("an empty record still read the server (%d listings, %v)", c.h.listings, c.h.readLog)
			}
		})
	}
}

// TestSQLBinlogHeaders_RefusesAnOddFileName: a name that is not a plain
// binary log file name never reaches a statement.
func TestSQLBinlogHeaders_RefusesAnOddFileName(t *testing.T) {
	for _, name := range []string{"", "binlog.000001' OR '1", "a b", "x;y"} {
		if _, err := (sqlBinlogHeaders{}).PreviousGtids(context.Background(), name); err == nil || !strings.Contains(err.Error(), "unexpected binary log file name") {
			t.Errorf("PreviousGtids(%q) = %v, want the name refused", name, err)
		}
	}
}

// TestRealMySQL_PromotionBinlogFile runs the check's SQL against a real
// server with binary logging and GTIDs on, through the statements a
// promotion sends. It needs BLOODRAVEN_TEST_MYSQL_ADMIN_DSN:
//
//	docker run -d --name br-s01 -e MYSQL_ROOT_PASSWORD=root -p 33309:3306 mysql:9.7 --gtid-mode=ON --enforce-gtid-consistency=ON --server-id=7
//	BLOODRAVEN_TEST_MYSQL_ADMIN_DSN='root:root@tcp(127.0.0.1:33309)/' go test ./internal/playground/scenarios -run RealMySQL_PromotionBinlogFile -v
//
// It proves that SHOW BINARY LOGS and SHOW BINLOG EVENTS are read as the
// check assumes: the file a FLUSH BINARY LOGS opens on a read-only server
// is found by its Previous_gtids, also after writes and a later rotation,
// and a rotation that follows a write is reported as a mismatch.
func TestRealMySQL_PromotionBinlogFile(t *testing.T) {
	dsn := os.Getenv("BLOODRAVEN_TEST_MYSQL_ADMIN_DSN")
	if dsn == "" {
		t.Skip("BLOODRAVEN_TEST_MYSQL_ADMIN_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	gtid := func() string {
		t.Helper()
		var g string
		if err := db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&g); err != nil {
			t.Fatal(err)
		}
		return g
	}
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, q := range []string{"SET GLOBAL read_only = OFF", "DROP DATABASE IF EXISTS br_s01_file_test"} {
			_, _ = db.ExecContext(cleanCtx, q)
		}
	})
	exec("SET GLOBAL read_only = OFF")
	exec("CREATE DATABASE IF NOT EXISTS br_s01_file_test")
	exec("CREATE TABLE IF NOT EXISTS br_s01_file_test.t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
	exec("INSERT INTO br_s01_file_test.t (v) VALUES (1)")
	h := sqlBinlogHeaders{db: db}

	// A promotion: read the set on the read-only server, rotate, clear.
	exec("SET GLOBAL super_read_only = ON")
	recorded := gtid()
	exec("FLUSH BINARY LOGS")
	exec("SET GLOBAL read_only = OFF")
	files, err := h.BinlogFiles(ctx)
	if err != nil || len(files) < 2 {
		t.Fatalf("BinlogFiles = %v, %v; want at least two files after a rotation", files, err)
	}
	got := checkPromotionBinlogFile(ctx, h, recorded)
	if got.Verdict != promotionFileVerified || got.File != files[len(files)-1] {
		t.Fatalf("after a promotion-shaped rotation: %+v; want %s verified", got, files[len(files)-1])
	}
	t.Logf("verified: %s", got.Detail)
	// The new primary writes and rotates again: the file is still found.
	exec("INSERT INTO br_s01_file_test.t (v) VALUES (2)")
	exec("FLUSH BINARY LOGS")
	exec("INSERT INTO br_s01_file_test.t (v) VALUES (3)")
	again := checkPromotionBinlogFile(ctx, h, recorded)
	if again.Verdict != promotionFileVerified || again.File != got.File {
		t.Fatalf("after a later rotation: %+v; want %s verified", again, got.File)
	}
	// A rotation that lands after a write does not start at the set read
	// before that write.
	stale := gtid()
	exec("INSERT INTO br_s01_file_test.t (v) VALUES (4)")
	exec("FLUSH BINARY LOGS")
	late := checkPromotionBinlogFile(ctx, h, stale)
	if late.Verdict != promotionFileMismatch {
		t.Fatalf("a rotation after a write: %+v; want a mismatch", late)
	}
	t.Logf("mismatch: %s", late.Detail)
}
