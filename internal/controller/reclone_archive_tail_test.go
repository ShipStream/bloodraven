package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// tailSourceScript is a scripted ArchiveTailSource: it answers the
// recipient's status from a function of the call number, and records the
// calls. block, when set, holds every status request until its context
// ends (a sidecar that never answers).
type tailSourceScript struct {
	mu      sync.Mutex
	enabled bool
	answer  func(call int) (*mysql.ArchiverStatus, error)
	block   bool
	calls   int
	sites   []string
}

func (s *tailSourceScript) ArchivingEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enabled
}

func (s *tailSourceScript) TailStatus(ctx context.Context, site string) (*mysql.ArchiverStatus, error) {
	s.mu.Lock()
	s.calls++
	call, block, answer := s.calls, s.block, s.answer
	s.sites = append(s.sites, site)
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return answer(call)
}

func (s *tailSourceScript) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func tailStatus(state, active string, pending int64, reason string) *mysql.ArchiverStatus {
	return &mysql.ArchiverStatus{Enabled: true, Tail: &mysql.ArchiverTailStatus{
		State: state, ActiveFile: active, NewestSealedFile: "binlog.000001", PendingFiles: pending, Reason: reason}}
}

// stepQueue is a manual runner for the wait's steps: it holds each step
// until the test runs it, so a test decides when a step "returns".
type stepQueue struct{ steps []func() }

func (q *stepQueue) run(step func()) { q.steps = append(q.steps, step) }

// runNext runs the oldest held step.
func (q *stepQueue) runNext(t *testing.T) {
	t.Helper()
	if len(q.steps) == 0 {
		t.Fatal("no step is held")
	}
	step := q.steps[0]
	q.steps = q.steps[1:]
	step()
}

// recloneTailFixture is a two-site manager on a fake clock with dc2 the
// reclone recipient, a scripted status source and a recorder of the
// not-archived callback. The recipient writes binlog.000007, so its first
// acknowledged rotation opens binlog.000008. Steps run inline (the wait
// then behaves the same on every run); tests of what happens while a step
// is in flight install a stepQueue or the production goroutine.
type recloneTailFixture struct {
	tm        *TopologyManager
	clk       *clock.FakeClock
	primary   *mockMySQL
	recipient *mockMySQL
	probe     *tailSourceScript
	logs      *syncBuffer
	reported  []string
}

func newRecloneTailFixture(t *testing.T, answer func(call int) (*mysql.ArchiverStatus, error)) *recloneTailFixture {
	t.Helper()
	f := &recloneTailFixture{
		primary:   &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"},
		recipient: &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10", binlogSeq: 7},
		probe:     &tailSourceScript{enabled: true, answer: answer},
	}
	f.tm, f.clk, f.logs = newRecloneTailManager(f.primary, f.recipient)
	f.tm.ArchiveTail = f.probe
	f.tm.ArchiveTailNotArchivedCallback = func(site, reason string) { f.reported = append(f.reported, site+": "+reason) }
	return f
}

func newRecloneTailManager(primary, recipient *mockMySQL) (*TopologyManager, *clock.FakeClock, *syncBuffer) {
	clk := clock.NewFakeClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrap(primary, recipient)
	tm.clock = clk
	fc, logs := rotationController(clk)
	tm.failover = fc
	tm.logger = fc.logger
	tm.autoBootstrapSuppressed = true
	tm.recloneTailRun = func(step func()) { step() }
	return tm, clk, logs
}

// step runs one poll's worth of the wait and advances the clock a poll.
func (f *recloneTailFixture) step() bool {
	settled := f.tm.recloneArchiveTailSettled(context.Background(), f.tm.getSite("dc2"))
	if !settled {
		f.clk.Advance(2 * time.Second)
	}
	return settled
}

// runToEnd polls until the wait ends and returns how long that took on the
// fake clock; nothing may be announced before the end.
func (f *recloneTailFixture) runToEnd(t *testing.T) time.Duration {
	t.Helper()
	start := f.clk.Now()
	for polls := 0; !f.step(); polls++ {
		if polls > 400 {
			t.Fatal("the wait never ended")
		}
		if len(f.reported) != 0 {
			t.Fatalf("the gap was announced before the wait ended: %v", f.reported)
		}
	}
	return f.clk.Since(start)
}

func (f *recloneTailFixture) flushes() int {
	f.recipient.mu.Lock()
	defer f.recipient.mu.Unlock()
	return f.recipient.flushBinaryLogsCalls
}

// waitWorkers fails the test if a step goroutine is still running after
// two seconds.
func waitRecloneTailWorkers(t *testing.T, tm *TopologyManager) {
	t.Helper()
	done := make(chan struct{})
	go func() { tm.waitRecloneTailWorkers(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a step goroutine of the pre-reclone wait was left behind")
	}
}

// TestRecloneTail_SkippedWithoutPITR: with no status source wired, or with
// PITR not enabled for the group, the reclone sends no rotation, reads no
// sidecar status, waits for nothing and reports nothing.
func TestRecloneTail_SkippedWithoutPITR(t *testing.T) {
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) { return nil, errors.New("must not matter") })
	f.probe.enabled = false
	if !f.step() {
		t.Fatal("a group without PITR waited before its reclone")
	}
	f.tm.ArchiveTail = nil
	if !f.step() {
		t.Fatal("a manager without a status source waited before its reclone")
	}
	if f.flushes() != 0 || len(f.reported) != 0 || f.probe.callCount() != 0 {
		t.Fatalf("without PITR the reclone rotated %d times, read the sidecar %d times and reported %v", f.flushes(), f.probe.callCount(), f.reported)
	}
	// PITR switched off mid-wait: the wait is dropped, nothing announced.
	f.tm.ArchiveTail = f.probe
	f.probe.enabled = true
	f.probe.answer = func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
	}
	if f.step() {
		t.Fatal("a wait with an unarchived tail ended at once")
	}
	f.probe.mu.Lock()
	f.probe.enabled = false
	f.probe.mu.Unlock()
	if !f.step() || len(f.reported) != 0 || f.tm.recloneTail != nil {
		t.Fatalf("PITR switched off mid-wait: reported %v, wait %v; want the wait dropped silently", f.reported, f.tm.recloneTail)
	}
}

// TestRecloneTail_WaitsForADrainedScanOfTheRotation: the wait takes the
// baseline, rotates once, reads the file the rotation opened, and ends only
// on a status that is Drained and names that file (or a later one) as the
// active one. A Drained from a scan before the rotation, or a scan still
// uploading, does not end it.
func TestRecloneTail_WaitsForADrainedScanOfTheRotation(t *testing.T) {
	f := newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		switch call {
		case 1, 2: // the baseline; then the sidecar has not scanned since the rotation
			return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
		case 3: // it saw the rotation and is uploading the tail
			return tailStatus(mysql.ArchiverTailPending, "binlog.000008", 1, ""), nil
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000008", 0, ""), nil
	})
	for poll := 1; poll <= 3; poll++ {
		if f.step() {
			t.Fatalf("poll %d: the wait ended before a Drained scan of the rotated binary log", poll)
		}
		if got := f.flushes(); got != 1 {
			t.Fatalf("poll %d: %d rotations, want exactly one (after the baseline)", poll, got)
		}
	}
	if !f.step() {
		t.Fatal("the wait did not end on a Drained scan of the rotated binary log")
	}
	if len(f.reported) != 0 {
		t.Fatalf("an archived tail was reported not archived: %v", f.reported)
	}
	logLine(t, f.logs.String(), "pre-reclone archive tail wait started")
	logLine(t, f.logs.String(), "pre-reclone binlog rotated")
	line := logLine(t, f.logs.String(), "pre-reclone archive tail archived")
	for _, want := range []string{"site=dc2", "waitedMs=6000", "rotatedFile=binlog.000008"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line %q lacks %q", line, want)
		}
	}
	// Settled: a clone deferred after it does not rotate or wait again.
	if !f.step() || f.flushes() != 1 || f.probe.callCount() != 4 {
		t.Fatalf("a settled wait ran again: rotations=%d status reads=%d", f.flushes(), f.probe.callCount())
	}
	f.tm.clearRecloneArchiveTail()
	if f.step() {
		t.Fatal("a new reclone of the site did not wait")
	}
}

// TestRecloneTail_ALaterActiveFileSettlesToo: the server may rotate again
// after the operator's rotation; a Drained scan that names a later file
// has seen the operator's rotation as well.
func TestRecloneTail_ALaterActiveFileSettlesToo(t *testing.T) {
	f := newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		if call == 1 {
			return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000011", 0, ""), nil
	})
	if f.step() {
		t.Fatal("the wait ended on the baseline")
	}
	if !f.step() || len(f.reported) != 0 {
		t.Fatalf("a Drained scan of a later file did not end the wait cleanly (reported %v)", f.reported)
	}
}

// TestRecloneTail_AScanFromBeforeTheRotationNeverSettles (r2 F2): the
// sidecar's last published scan can be one rotation behind the server. The
// server already wrote binlog.000003 when the wait began (a scan that saw
// binlog.000002 as active is still published), the operator's rotation
// opens binlog.000004, and a scan that read the index before that rotation
// then reports "Drained, active binlog.000003". That scan never saw the
// file the operator sealed: the wait must not end on it, and at the bound
// the gap is announced. Only a Drained scan that names binlog.000004 ends
// the wait as archived.
func TestRecloneTail_AScanFromBeforeTheRotationNeverSettles(t *testing.T) {
	staleDrained := func(call int) (*mysql.ArchiverStatus, error) {
		if call == 1 {
			return tailStatus(mysql.ArchiverTailDrained, "binlog.000002", 0, ""), nil
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000003", 0, ""), nil
	}
	f := newRecloneTailFixture(t, staleDrained)
	f.recipient.binlogSeq = 3
	if waited := f.runToEnd(t); waited != preRecloneArchiveTailTimeout {
		t.Fatalf("the wait ended after %s on a scan from before the rotation, want the %s bound", waited, preRecloneArchiveTailTimeout)
	}
	if strings.Contains(f.logs.String(), `msg="pre-reclone archive tail archived"`) {
		t.Fatal("a scan that read the index before the operator's rotation was logged as archived")
	}
	want := "its last scan saw binlog.000003 as the active file; the rotation opened binlog.000004"
	if len(f.reported) != 1 || !strings.Contains(f.reported[0], want) {
		t.Fatalf("reported %q, want one report with %q", f.reported, want)
	}

	// The same wait ends as archived once a scan names the rotated file.
	f = newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		if call < 4 {
			return staleDrained(call)
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000004", 0, ""), nil
	})
	f.recipient.binlogSeq = 3
	if waited := f.runToEnd(t); waited != 6*time.Second || len(f.reported) != 0 {
		t.Fatalf("waited %s, reported %v; want the wait to end on the fourth status, unannounced", waited, f.reported)
	}
}

// TestRecloneTail_UnreadableOrIncomparableFileNameIsUnconfirmed: when the
// name of the file the rotation opened cannot be read, cannot be compared
// with the sidecar's, or the sidecar's numbering is behind it (a reset
// started it again), the tail is unconfirmed: the wait runs to its bound
// and announces the gap, whatever the sidecar says is Drained.
func TestRecloneTail_UnreadableOrIncomparableFileNameIsUnconfirmed(t *testing.T) {
	// drained answers the baseline (binlog.000007, before the rotation)
	// and then a Drained scan with active as the active file.
	drained := func(active string) func(int) (*mysql.ArchiverStatus, error) {
		return func(call int) (*mysql.ArchiverStatus, error) {
			if call == 1 {
				return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
			}
			return tailStatus(mysql.ArchiverTailDrained, active, 0, ""), nil
		}
	}
	always := func(active string) func(int) (*mysql.ArchiverStatus, error) {
		return func(int) (*mysql.ArchiverStatus, error) {
			return tailStatus(mysql.ArchiverTailDrained, active, 0, ""), nil
		}
	}
	everyStep := func(steps int) int { return steps } // an uncounted rotation is sent again by every step
	once := func(int) int { return 1 }
	cases := []struct {
		name    string
		answer  func(int) (*mysql.ArchiverStatus, error)
		arm     func(f *recloneTailFixture)
		reason  string
		flushes func(steps int) int
	}{
		{
			name:    "the file name cannot be read",
			answer:  drained("binlog.000099"),
			arm:     func(f *recloneTailFixture) { f.recipient.binlogFileErr = errors.New("Error 1227: access denied") },
			reason:  "the name of the file it opened could not be read",
			flushes: everyStep,
		},
		{
			name:   "the server's file name has no sequence number",
			answer: drained("binlog.000099"),
			arm: func(f *recloneTailFixture) {
				f.recipient.binlogFileName = "binlog"
			},
			reason:  "the name of the file it opened could not be read",
			flushes: everyStep,
		},
		{
			// The sidecar's last scan is of a numbering the server has
			// since started again (it names binlog.000999; the rotations
			// open binlog.000008 and up): no rotation counts against it.
			name:    "the baseline is a scan of an earlier numbering",
			answer:  always("binlog.000999"),
			reason:  "binlog.000999 as the active binary log, but the rotation opened binlog.0000",
			flushes: everyStep,
		},
		{
			name:    "the baseline has another base name",
			answer:  always("mysql-bin.000001"),
			reason:  "mysql-bin.000001 as the active binary log, but the rotation opened binlog.0000",
			flushes: everyStep,
		},
		{
			name:    "the numbering started again after the rotation",
			answer:  drained("binlog.000002"),
			reason:  "its last scan saw binlog.000002 as the active file; the rotation opened binlog.000008",
			flushes: once,
		},
		{
			name:    "another base name after the rotation",
			answer:  drained("mysql-bin.000009"),
			reason:  "mysql-bin.000009 as the active binary log, which cannot be compared with binlog.000008",
			flushes: once,
		},
		{
			name:    "a name without a sequence number after the rotation",
			answer:  drained("binlog"),
			reason:  "binlog as the active binary log, which cannot be compared with binlog.000008",
			flushes: once,
		},
		{
			name:    "no active file reported after the rotation",
			answer:  drained(""),
			reason:  "has not reported a scan of the rotated binary log",
			flushes: once,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecloneTailFixture(t, tc.answer)
			if tc.arm != nil {
				tc.arm(f)
			}
			if waited := f.runToEnd(t); waited != preRecloneArchiveTailTimeout {
				t.Fatalf("the wait ended after %s, want the %s bound", waited, preRecloneArchiveTailTimeout)
			}
			if strings.Contains(f.logs.String(), `msg="pre-reclone archive tail archived"`) {
				t.Fatal("an unconfirmed tail was logged as archived")
			}
			if len(f.reported) != 1 || !strings.Contains(f.reported[0], tc.reason) {
				t.Fatalf("reported %q, want one report with %q", f.reported, tc.reason)
			}
			if got, want := f.flushes(), tc.flushes(f.probe.callCount()); got != want {
				t.Fatalf("rotations = %d, want %d", got, want)
			}
		})
	}
}

// TestRecloneTail_ABaselineWithoutNamesCounts: a scan that could not read
// the binary log index reports a tail block with no file names. Such a
// baseline names no file the rotation could be confused with, so the
// rotation counts; the wait still ends only on a scan that names the
// rotated file.
func TestRecloneTail_ABaselineWithoutNamesCounts(t *testing.T) {
	f := newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		switch call {
		case 1:
			return tailStatus(mysql.ArchiverTailBlocked, "", 0, "read the binary log index: permission denied"), nil
		case 2:
			return tailStatus(mysql.ArchiverTailDrained, "", 0, ""), nil
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000008", 0, ""), nil
	})
	if waited := f.runToEnd(t); waited != 4*time.Second {
		t.Fatalf("the wait ended after %s, want it to end on the third status, the first to name the rotated file", waited)
	}
	if len(f.reported) != 0 || f.flushes() != 1 {
		t.Fatalf("reported %v after %d rotations; want an archived tail after one rotation", f.reported, f.flushes())
	}
}

// TestBaselinePrecedesRotation: a rotation counts only against a baseline
// that names an earlier file of the same numbering, or no file.
func TestBaselinePrecedesRotation(t *testing.T) {
	for _, tc := range []struct {
		baseline, rotated string
		want              bool
	}{
		{"binlog.000007", "binlog.000008", true},
		{"binlog.000002", "binlog.000011", true},
		{"binlog.999999", "binlog.1000000", true}, // the suffix grows past six digits
		{"", "binlog.000001", true},
		{"binlog.000008", "binlog.000008", false},
		{"binlog.000011", "binlog.000002", false},
		{"mysql-bin.000001", "binlog.000008", false},
		{"binlog", "binlog.000008", false},
		{"binlog.000007", "binlog", false},
		{"binlog.00000x", "binlog.000008", false},
	} {
		if got := baselinePrecedesRotation(tc.baseline, tc.rotated); got != tc.want {
			t.Errorf("baselinePrecedesRotation(%q, %q) = %v, want %v", tc.baseline, tc.rotated, got, tc.want)
		}
	}
}

// TestRecloneTail_ABaselineFromBeforeAResetIsReplaced: the sidecar's last
// published scan names binlog.000011, from before the server's binary logs
// were reset; the server now writes binlog.000001. The first rotation
// (opening binlog.000002) does not count against that baseline. Once the
// sidecar has scanned the new numbering, the next step's rotation does,
// and the wait ends on a Drained scan of the file that rotation opened.
func TestRecloneTail_ABaselineFromBeforeAResetIsReplaced(t *testing.T) {
	f := newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		switch call {
		case 1:
			return tailStatus(mysql.ArchiverTailDrained, "binlog.000011", 0, ""), nil
		case 2: // the sidecar scanned the first rotation
			return tailStatus(mysql.ArchiverTailPending, "binlog.000002", 1, ""), nil
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000003", 0, ""), nil
	})
	f.recipient.binlogSeq = 1
	if waited := f.runToEnd(t); waited != 4*time.Second {
		t.Fatalf("the wait ended after %s, want it to end on the third status", waited)
	}
	if len(f.reported) != 0 || f.flushes() != 2 {
		t.Fatalf("reported %v after %d rotations; want an archived tail after two rotations", f.reported, f.flushes())
	}
	line := logLine(t, f.logs.String(), "pre-reclone archive tail archived")
	if !strings.Contains(line, "rotatedFile=binlog.000003") {
		t.Fatalf("log line %q; want the file the counted rotation opened", line)
	}
}

// TestRecloneTail_TimesOutAndAnnouncesTheGap: each way the tail stays
// unconfirmed ends the wait at the bound, not before, with the callback
// naming the site and the cause; the reclone then proceeds.
func TestRecloneTail_TimesOutAndAnnouncesTheGap(t *testing.T) {
	cases := []struct {
		name     string
		answer   func(call int) (*mysql.ArchiverStatus, error)
		flushErr error
		reason   string
		flushes  func(steps int) int
	}{
		{
			name:    "sidecar unreachable",
			answer:  func(int) (*mysql.ArchiverStatus, error) { return nil, errors.New("connection refused") },
			reason:  "did not answer: connection refused",
			flushes: func(int) int { return 0 }, // no baseline, so no rotation
		},
		{
			name: "rotation never acknowledged",
			answer: func(int) (*mysql.ArchiverStatus, error) {
				return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
			},
			flushErr: errors.New("connection refused"),
			reason:   "FLUSH BINARY LOGS on the site was not acknowledged",
			flushes:  func(steps int) int { return steps }, // retried by every step
		},
		{
			name: "sidecar never scans the rotation",
			answer: func(int) (*mysql.ArchiverStatus, error) {
				return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
			},
			reason:  "has not reported a scan of the rotated binary log",
			flushes: func(int) int { return 1 },
		},
		{
			name: "upload still pending",
			answer: func(call int) (*mysql.ArchiverStatus, error) {
				if call == 1 {
					return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
				}
				return tailStatus(mysql.ArchiverTailPending, "binlog.000008", 2, ""), nil
			},
			reason:  "still had 2 sealed binary log(s) to archive",
			flushes: func(int) int { return 1 },
		},
		{
			name: "archiver blocked",
			answer: func(call int) (*mysql.ArchiverStatus, error) {
				if call == 1 {
					return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
				}
				return tailStatus(mysql.ArchiverTailBlocked, "binlog.000008", 1, "the site's latest record is an incarnation reset"), nil
			},
			reason:  "cannot archive the tail: the site's latest record is an incarnation reset",
			flushes: func(int) int { return 1 },
		},
		{
			name: "an unknown tail state",
			answer: func(call int) (*mysql.ArchiverStatus, error) {
				if call == 1 {
					return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
				}
				return tailStatus("Archived", "binlog.000008", 0, ""), nil
			},
			reason:  `an archive tail state this operator does not know ("Archived")`,
			flushes: func(int) int { return 1 },
		},
		{
			name:    "no tail block",
			answer:  func(int) (*mysql.ArchiverStatus, error) { return &mysql.ArchiverStatus{Enabled: true}, nil },
			reason:  "reports no archive tail",
			flushes: func(int) int { return 0 },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecloneTailFixture(t, tc.answer)
			f.recipient.flushBinaryLogsErr = tc.flushErr
			if waited := f.runToEnd(t); waited != preRecloneArchiveTailTimeout {
				t.Fatalf("the wait ended after %s, want exactly the %s bound", waited, preRecloneArchiveTailTimeout)
			}
			if len(f.reported) != 1 || !strings.HasPrefix(f.reported[0], "dc2: ") || !strings.Contains(f.reported[0], tc.reason) {
				t.Fatalf("reported %q, want one report naming dc2 with %q", f.reported, tc.reason)
			}
			if got, want := f.flushes(), tc.flushes(f.probe.callCount()); got != want {
				t.Fatalf("rotations = %d, want %d", got, want)
			}
			line := logLine(t, f.logs.String(), "pre-reclone archive tail not archived")
			if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "site=dc2") || !strings.Contains(line, "waitedMs=120000") {
				t.Fatalf("log line %q; want WARN with the site and the bound", line)
			}
		})
	}
}

// TestRecloneTail_TheBoundRunsFromTheAcknowledgedRotation (r1 L2): the two
// minutes are the sidecar's time to archive the file the rotation sealed,
// so they count from the acknowledged rotation, not from the wait's start.
// A rotation that is first acknowledged late leaves the sidecar the whole
// span, capped at twice the span after the wait began.
func TestRecloneTail_TheBoundRunsFromTheAcknowledgedRotation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ackedAt time.Duration // the first step whose rotation is acknowledged starts here
		want    time.Duration
	}{
		{"acknowledged at once", 0, preRecloneArchiveTailTimeout},
		{"acknowledged after one minute", time.Minute, time.Minute + preRecloneArchiveTailTimeout},
		{"acknowledged two seconds before the first span ends", 118 * time.Second, 118*time.Second + preRecloneArchiveTailTimeout},
		{"acknowledged by the last attempt", preRecloneArchiveTailTimeout, 2 * preRecloneArchiveTailTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) {
				return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
			})
			start := f.clk.Now()
			f.recipient.flushBinaryLogsFn = func(context.Context) error {
				if f.clk.Since(start) < tc.ackedAt {
					return errors.New("connection refused")
				}
				return nil
			}
			if waited := f.runToEnd(t); waited != tc.want {
				t.Fatalf("the wait ended after %s, want %s", waited, tc.want)
			}
			if len(f.reported) != 1 || !strings.Contains(f.reported[0], "has not reported a scan of the rotated binary log yet") {
				t.Fatalf("reported %q, want the scan the sidecar still owes", f.reported)
			}
		})
	}
}

// TestRecloneTail_TheWaitIsCappedAtTwiceTheBound: the last attempt to
// rotate starts when the first span ends; when its step takes a while (a
// slow sidecar answer before the rotation) the rotation is acknowledged
// after that, and the deadline is still twice the span after the wait
// began, not a span after the late acknowledgement.
func TestRecloneTail_TheWaitIsCappedAtTwiceTheBound(t *testing.T) {
	var f *recloneTailFixture
	var start time.Time
	slowed := false
	f = newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) {
		if !slowed && f.clk.Since(start) >= preRecloneArchiveTailTimeout {
			slowed = true
			f.clk.Advance(8 * time.Second) // the sidecar takes eight seconds to answer
		}
		return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
	})
	start = f.clk.Now()
	f.recipient.flushBinaryLogsFn = func(context.Context) error {
		if f.clk.Since(start) < preRecloneArchiveTailTimeout {
			return errors.New("connection refused")
		}
		return nil
	}
	if waited := f.runToEnd(t); waited != 2*preRecloneArchiveTailTimeout {
		t.Fatalf("the wait ended after %s, want the cap of %s", waited, 2*preRecloneArchiveTailTimeout)
	}
	if !slowed || len(f.reported) != 1 {
		t.Fatalf("slowed=%v reported=%q; want the late acknowledgement and one report", slowed, f.reported)
	}
}

// TestRecloneTail_AWaitThatWasNotDrivenJudgesAFreshStatus (r1 L2): while
// the reclone is deferred (a planned failover holds the sites, another
// bootstrap runs) nobody reads the sidecar. When the wait is driven again
// after its deadline, it must not end on what the sidecar said before the
// deferral: it reads the status again and ends on that.
func TestRecloneTail_AWaitThatWasNotDrivenJudgesAFreshStatus(t *testing.T) {
	archived := false
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) {
		if archived {
			return tailStatus(mysql.ArchiverTailDrained, "binlog.000008", 0, ""), nil
		}
		return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
	})
	for poll := 1; poll <= 2; poll++ {
		if f.step() {
			t.Fatalf("poll %d: the wait ended while the tail was still uploading", poll)
		}
	}
	// Five minutes without a poll reaching the wait; the sidecar finishes
	// meanwhile.
	f.clk.Advance(5 * time.Minute)
	archived = true
	reads := f.probe.callCount()
	if !f.step() {
		t.Fatal("the wait did not end on the first poll after its deadline")
	}
	if f.probe.callCount() != reads+1 {
		t.Fatalf("status reads after the deferral = %d, want one", f.probe.callCount()-reads)
	}
	if len(f.reported) != 0 {
		t.Fatalf("a tail archived during the deferral was announced as not archived: %v", f.reported)
	}
	logLine(t, f.logs.String(), "pre-reclone archive tail archived")
}

// TestRecloneTail_NoArchiverEndsAtOnce: PITR is enabled but the recipient's
// sidecar runs no archiver, so nothing can archive the tail: the gap is
// announced on the first status and the reclone is not delayed. No rotation
// is sent.
func TestRecloneTail_NoArchiverEndsAtOnce(t *testing.T) {
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) { return &mysql.ArchiverStatus{Enabled: false}, nil })
	if !f.step() {
		t.Fatal("the reclone waited on a sidecar that runs no archiver")
	}
	if len(f.reported) != 1 || !strings.Contains(f.reported[0], "runs no binlog archiver") || f.flushes() != 0 {
		t.Fatalf("reported %v with %d rotations; want the gap announced and nothing rotated", f.reported, f.flushes())
	}
}

// TestRecloneTail_RestartStartsOver: the wait lives in memory. A new
// manager (an operator restart mid-wait; the reclone annotation is still
// there) rotates again and waits its own full bound.
func TestRecloneTail_RestartStartsOver(t *testing.T) {
	stuck := func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
	}
	f := newRecloneTailFixture(t, stuck)
	for i := 0; i < 30; i++ { // 60s into the first process's wait
		if f.step() {
			t.Fatal("the wait ended early")
		}
	}
	// The replacement process: same servers, a later clock.
	tm2, clk2, _ := newRecloneTailManager(f.primary, f.recipient)
	clk2.Set(f.clk.Now())
	probe2 := &tailSourceScript{enabled: true, answer: stuck}
	var reported []string
	tm2.ArchiveTail = probe2
	tm2.ArchiveTailNotArchivedCallback = func(site, reason string) { reported = append(reported, site) }
	start := clk2.Now()
	for !tm2.recloneArchiveTailSettled(context.Background(), tm2.getSite("dc2")) {
		clk2.Advance(2 * time.Second)
	}
	if waited := clk2.Since(start); waited != preRecloneArchiveTailTimeout {
		t.Fatalf("the restarted wait ended after %s, want a full %s", waited, preRecloneArchiveTailTimeout)
	}
	if got := f.flushes(); got != 2 {
		t.Fatalf("rotations across the restart = %d, want 2 (one per process)", got)
	}
	if len(reported) != 1 || len(f.reported) != 0 {
		t.Fatalf("reports: replacement %v, first process %v; want one, from the process that reached the bound", reported, f.reported)
	}
}

// TestRecloneTail_APollNeverWaitsOnAStepInFlight (r1 M1): the status
// request and the rotation run as a step off the poll goroutine. While a
// step has not returned, a poll starts no second one and decides nothing;
// it picks the result up once the step has returned.
func TestRecloneTail_APollNeverWaitsOnAStepInFlight(t *testing.T) {
	f := newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		if call == 1 {
			return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000008", 0, ""), nil
	})
	q := &stepQueue{}
	f.tm.recloneTailRun = q.run
	for poll := 1; poll <= 3; poll++ {
		if f.step() {
			t.Fatalf("poll %d: the wait ended with its first step still in flight", poll)
		}
		if len(q.steps) != 1 || f.probe.callCount() != 0 || f.flushes() != 0 {
			t.Fatalf("poll %d: %d steps held, %d status reads, %d rotations; want one step, not yet run", poll, len(q.steps), f.probe.callCount(), f.flushes())
		}
	}
	q.runNext(t) // the step returns: baseline read, rotation acknowledged
	if f.step() {
		t.Fatal("the wait ended on the baseline")
	}
	if len(q.steps) != 1 || f.flushes() != 1 {
		t.Fatalf("after the first step: %d steps held, %d rotations; want the second step started and one rotation", len(q.steps), f.flushes())
	}
	q.runNext(t)
	if !f.step() || len(f.reported) != 0 {
		t.Fatalf("the wait did not end cleanly on the second step's Drained scan (reported %v)", f.reported)
	}
	waitRecloneTailWorkers(t, f.tm)
}

// TestRecloneTail_AStepThatDoesNotReturnIsAbandonedOnTheClock: a step that
// has not returned archiveTailStepTimeout later, by the manager's clock, is
// cancelled and replaced; its late result is discarded and it sends no
// rotation once cancelled. A wait whose steps never return ends at its
// deadline with that cause.
func TestRecloneTail_AStepThatDoesNotReturnIsAbandonedOnTheClock(t *testing.T) {
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
	})
	q := &stepQueue{}
	f.tm.recloneTailRun = q.run
	if f.step() {
		t.Fatal("ended at once")
	}
	first := f.tm.recloneTail.step
	for f.clk.Since(first.startedAt) < archiveTailStepTimeout {
		if f.tm.recloneTail.step != first {
			t.Fatalf("the step was replaced %s after it started, before the %s bound", f.clk.Since(first.startedAt), archiveTailStepTimeout)
		}
		if f.step() {
			t.Fatal("the wait ended with a step in flight")
		}
	}
	if f.step() { // the poll at the bound
		t.Fatal("the wait ended when its first step was abandoned")
	}
	if f.tm.recloneTail.step == first || len(q.steps) != 2 {
		t.Fatalf("the step was not replaced at the %s bound (%d steps held)", archiveTailStepTimeout, len(q.steps))
	}
	// The abandoned step returns late: cancelled, it sends no rotation and
	// its result does not reach the wait.
	q.runNext(t)
	if f.flushes() != 0 || strings.Contains(f.logs.String(), "pre-reclone binlog rotat") {
		t.Fatalf("an abandoned step sent %d rotations; log:\n%s", f.flushes(), f.logs.String())
	}
	if f.step() {
		t.Fatal("the wait ended on an abandoned step's result")
	}
	if f.tm.recloneTail.rotated {
		t.Fatal("an abandoned step's result reached the wait")
	}
	// Nothing ever returns: the wait ends at its deadline, announced.
	start := f.tm.recloneTail.startedAt
	for !f.step() {
		if f.clk.Since(start) > 2*preRecloneArchiveTailTimeout {
			t.Fatal("a wait whose steps never return did not end")
		}
	}
	if waited := f.clk.Since(start); waited < preRecloneArchiveTailTimeout || waited > preRecloneArchiveTailTimeout+2*archiveTailStepTimeout {
		t.Fatalf("the wait ended after %s, want the %s bound plus at most two abandoned steps", waited, preRecloneArchiveTailTimeout)
	}
	if len(f.reported) != 1 || !strings.Contains(f.reported[0], "did not return within 10s") {
		t.Fatalf("reported %q, want the step that did not return", f.reported)
	}
	for len(q.steps) > 0 {
		q.runNext(t)
	}
	waitRecloneTailWorkers(t, f.tm)
}

// TestRecloneTail_RotationBoundIsTheInjectedClock (r1 M4, the pre-reclone
// caller): the rotation's bound is the manager's clock, not the wall
// clock. With a flush that never answers and a fake clock that is not
// advanced, the step is still in flight well past BinlogRotationTimeout of
// wall time; advancing the clock by the bound ends the step, unacknowledged,
// while the flush is still blocked.
func TestRecloneTail_RotationBoundIsTheInjectedClock(t *testing.T) {
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
	})
	f.tm.recloneTailRun = nil // the production goroutine
	entered, release := make(chan struct{}), make(chan struct{})
	var flushReturned atomic.Bool
	var flushCalls atomic.Int32
	flushDone := make(chan struct{})
	f.recipient.flushBinaryLogsFn = func(context.Context) error {
		if flushCalls.Add(1) > 1 {
			return errors.New("a second rotation was sent")
		}
		close(entered)
		<-release // ignores its context, like a read parked on a blackholed socket
		flushReturned.Store(true)
		close(flushDone)
		return errors.New("released by the test")
	}
	if f.tm.recloneArchiveTailSettled(context.Background(), f.tm.getSite("dc2")) {
		t.Fatal("ended at once")
	}
	step := f.tm.recloneTail.step
	<-entered
	stepDone := func() bool {
		f.tm.recloneTailMu.Lock()
		defer f.tm.recloneTailMu.Unlock()
		return step.done
	}
	time.Sleep(BinlogRotationTimeout + 400*time.Millisecond)
	if stepDone() {
		t.Fatal("the rotation was abandoned on the wall clock: the fake clock has not moved")
	}
	f.clk.Advance(BinlogRotationTimeout)
	waitRecloneTailWorkers(t, f.tm)
	if !stepDone() {
		t.Fatal("advancing the clock by the rotation bound did not end the step")
	}
	if flushReturned.Load() {
		t.Fatal("the flush returned; the step was meant to end while it is still blocked")
	}
	// The next poll takes the result: no rotation acknowledged. Its own
	// step is held, so the blocked flush is the only one ever sent.
	q := &stepQueue{}
	f.tm.recloneTailRun = q.run
	f.clk.Advance(time.Second)
	if f.tm.recloneArchiveTailSettled(context.Background(), f.tm.getSite("dc2")) {
		t.Fatal("the wait ended on an unacknowledged rotation")
	}
	if f.tm.recloneTail.rotated {
		t.Fatal("an abandoned rotation counts as acknowledged")
	}
	line := logLine(t, f.logs.String(), "pre-reclone binlog rotation failed")
	if !strings.Contains(line, "timedOut=true") {
		t.Fatalf("log line %q; want timedOut=true", line)
	}
	// The abandoned flush goroutine ends once the server answers.
	close(release)
	select {
	case <-flushDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the abandoned flush did not return when released")
	}
	f.tm.pauseRecloneArchiveTail()
	q.runNext(t) // cancelled: it sends nothing
	if got := f.flushes(); got != 1 {
		t.Fatalf("rotations = %d, want the one that hung", got)
	}
	waitRecloneTailWorkers(t, f.tm)
}

// TestRecloneTail_NoStepIsLeftBehind: a step in flight ends when the
// manager's context is cancelled (it stops, or leadership is lost), when
// the reclone request goes away, and when the request is deferred, even if
// the sidecar never answers.
func TestRecloneTail_NoStepIsLeftBehind(t *testing.T) {
	hung := func(t *testing.T) (*recloneTailFixture, context.CancelFunc) {
		f := newRecloneTailFixture(t, nil)
		f.probe.block = true
		f.tm.recloneTailRun = nil // the production goroutine
		pollN(f.tm, 2)
		f.tm.SetRecloneSite("dc2")
		ctx, cancel := context.WithCancel(context.Background())
		if f.tm.checkReclone(ctx) {
			t.Fatal("the reclone started with its sidecar hung")
		}
		if f.tm.recloneTail == nil || f.tm.recloneTail.step == nil {
			t.Fatal("no step in flight")
		}
		return f, cancel
	}
	t.Run("the manager stops", func(t *testing.T) {
		f, cancel := hung(t)
		cancel()
		waitRecloneTailWorkers(t, f.tm)
	})
	t.Run("the request is withdrawn", func(t *testing.T) {
		f, cancel := hung(t)
		defer cancel()
		f.tm.cancelPendingReclone("dc2")
		f.tm.checkReclone(context.Background())
		waitRecloneTailWorkers(t, f.tm)
		if f.tm.recloneTail != nil || f.tm.recloneTailSettled != "" {
			t.Fatal("a withdrawn reclone request left its wait behind")
		}
	})
	t.Run("the request names a site the manager does not have", func(t *testing.T) {
		f, cancel := hung(t)
		defer cancel()
		f.tm.mu.Lock()
		f.tm.reclonePendingSite = "dc9"
		f.tm.mu.Unlock()
		f.tm.checkReclone(context.Background()) // drops the request
		waitRecloneTailWorkers(t, f.tm)
		f.tm.checkReclone(context.Background())
		if f.tm.recloneTail != nil {
			t.Fatal("the wait outlived the request it was for")
		}
	})
	t.Run("the request is deferred", func(t *testing.T) {
		f, cancel := hung(t)
		defer cancel()
		f.tm.mu.Lock()
		f.tm.topologyFrozen = true
		f.tm.mu.Unlock()
		f.tm.checkReclone(context.Background())
		waitRecloneTailWorkers(t, f.tm)
		if f.tm.recloneTail == nil || f.tm.recloneTail.step != nil {
			t.Fatal("a deferred request must keep its wait and drop the step in flight")
		}
	})
}

// TestCheckReclone_WaitsForTheArchiveTailBeforeCloning: with PITR enabled a
// reclone request does not reach CLONE until the recipient's tail is
// archived (or the bound has passed), and the pending request survives the
// wait.
func TestCheckReclone_WaitsForTheArchiveTailBeforeCloning(t *testing.T) {
	drained := false
	var mu sync.Mutex
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) {
		mu.Lock()
		defer mu.Unlock()
		if !drained {
			return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
		}
		return tailStatus(mysql.ArchiverTailDrained, "binlog.000008", 0, ""), nil
	})
	pollN(f.tm, 2)
	f.tm.SetRecloneSite("dc2")
	for i := 0; i < 3; i++ {
		if f.tm.checkReclone(context.Background()) {
			t.Fatal("the reclone started before the recipient's tail was archived")
		}
		if phase := f.tm.BootstrapPhase(); phase != BootstrapPhaseNone {
			t.Fatalf("bootstrap phase %q during the wait", phase)
		}
		f.tm.mu.RLock()
		pending := f.tm.reclonePendingSite
		f.tm.mu.RUnlock()
		if pending != "dc2" {
			t.Fatalf("the pending reclone was dropped during the wait (%q)", pending)
		}
		f.clk.Advance(2 * time.Second)
	}
	if got := f.flushes(); got != 1 {
		t.Fatalf("rotations during the wait = %d, want 1", got)
	}
	mu.Lock()
	drained = true
	mu.Unlock()
	if !f.tm.checkReclone(context.Background()) {
		t.Fatal("the reclone did not start once the tail was archived")
	}
	if len(f.reported) != 0 {
		t.Fatalf("an archived tail was reported not archived: %v", f.reported)
	}
	if f.tm.recloneTail != nil || f.tm.recloneTailSettled != "" {
		t.Fatal("the wait's state outlived the reclone it was for")
	}
}

// TestCheckReclone_DroppedRequestForgetsTheWait: a request that goes away
// mid-wait leaves no state behind for a later request of the same site.
func TestCheckReclone_DroppedRequestForgetsTheWait(t *testing.T) {
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
	})
	pollN(f.tm, 2)
	f.tm.SetRecloneSite("dc2")
	if f.tm.checkReclone(context.Background()) {
		t.Fatal("the reclone started during the wait")
	}
	if f.tm.recloneTail == nil {
		t.Fatal("no wait in progress")
	}
	f.tm.mu.Lock()
	f.tm.reclonePendingSite = ""
	f.tm.mu.Unlock()
	f.tm.checkReclone(context.Background())
	if f.tm.recloneTail != nil || f.tm.recloneTailSettled != "" {
		t.Fatal("a dropped reclone request left its wait behind")
	}
}

// TestArchiverPollerTailStatus: the pre-reclone status source reads the
// named site's sidecar live, reports archiving enabled only when the
// group's PITR spec renders an archiver (from memory, with no request), and
// records nothing in the poller's snapshots.
func TestArchiverPollerTailStatus(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/archiver/status" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"enabled":true,"primary":false,"tail":{"state":"Drained","activeFile":"binlog.000008","newestSealedFile":"binlog.000007","pendingFiles":0}}`))
	}))
	defer srv.Close()
	p := newArchiverPoller("ns", "orders", []string{"dc1", "dc2"},
		[]*mysql.SidecarClient{nil, mysql.NewSidecarClient(srv.URL)}, nil, testLogger())
	var _ ArchiveTailSource = p

	if p.ArchivingEnabled() || requests != 0 {
		t.Fatalf("PITR not rendered: ArchivingEnabled = true after %d requests; want disabled and no request", requests)
	}
	p.rolloutRenders = true
	if !p.ArchivingEnabled() || requests != 0 {
		t.Fatalf("PITR rendered: ArchivingEnabled = %v after %d requests; want enabled and no request", p.ArchivingEnabled(), requests)
	}
	st, err := p.TailStatus(context.Background(), "dc2")
	if err != nil || st == nil || st.Tail == nil {
		t.Fatalf("TailStatus = %+v, %v", st, err)
	}
	if *st.Tail != (mysql.ArchiverTailStatus{State: mysql.ArchiverTailDrained, ActiveFile: "binlog.000008", NewestSealedFile: "binlog.000007"}) {
		t.Fatalf("tail = %+v", *st.Tail)
	}
	if _, err := p.TailStatus(context.Background(), "dc1"); err == nil {
		t.Fatal("a site with no sidecar client returned no error")
	}
	if _, err := p.TailStatus(context.Background(), "dc9"); err == nil {
		t.Fatal("an unknown site returned no error")
	}
	if len(p.Snapshots()) != 0 {
		t.Fatalf("the status read recorded snapshots: %v", p.Snapshots())
	}
}

// TestEmitPITRArchiveTailNotArchived: the runner records one Warning Event
// on the group that names the site and the cause.
func TestEmitPITRArchiveTailNotArchived(t *testing.T) {
	fg := pitrEnabledFG("default", "orders")
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(4)
	r := &TopologyManagerRunner{
		logger:   testLogger(),
		client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(fg).Build(),
		recorder: rec,
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	r.emitPITRArchiveTailNotArchived(context.Background(), nn, "dc2", "the site's sidecar did not answer: connection refused")
	select {
	case ev := <-rec.Events:
		for _, want := range []string{"Warning " + v1alpha1.EventPITRArchiveTailNotArchived, "site dc2", "did not answer: connection refused", "PITRArchiveGap"} {
			if !strings.Contains(ev, want) {
				t.Fatalf("event %q lacks %q", ev, want)
			}
		}
	default:
		t.Fatal("no Event recorded")
	}
	// A group that is gone records nothing and does not panic.
	r.emitPITRArchiveTailNotArchived(context.Background(), types.NamespacedName{Namespace: "default", Name: "gone"}, "dc2", "x")
	select {
	case ev := <-rec.Events:
		t.Fatalf("an Event was recorded for a missing group: %s", ev)
	default:
	}
}

// recloneUnderClaimFixture is an opsFixture with a reclone of dc2 pending
// and PITR archiving on, the wait's steps run by run.
func recloneUnderClaimFixture(t *testing.T, src *tailSourceScript, run func(step func())) *opsFixture {
	t.Helper()
	f := newOpsFixture(t, "")
	f.tm.bootstrap = NewBootstrapController(testLogger())
	f.tm.autoBootstrapSuppressed = true
	f.tm.ArchiveTail = src
	f.tm.recloneTailRun = run
	f.tm.SetRecloneSite("dc2")
	return f
}

// TestCheckReclone_NoRotationUnderAPlannedClaim: while a planned failover
// holds the sites the clone cannot start, so the pre-reclone step sends
// nothing: no status read and no FLUSH BINARY LOGS on a site the run owns.
// It starts once the claim is released. (The DST planned sweep flags the
// rotation as ForeignMutationUnderClaim without this.)
func TestCheckReclone_NoRotationUnderAPlannedClaim(t *testing.T) {
	probe := &tailSourceScript{enabled: true, answer: func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailPending, "binlog.000001", 1, ""), nil
	}}
	f := recloneUnderClaimFixture(t, probe, func(step func()) { step() })
	f.hold(false)
	for i := 0; i < 3; i++ {
		if f.tm.checkReclone(context.Background()) {
			t.Fatal("a reclone started under a planned failover's claim")
		}
	}
	if got := len(f.tgt.flushLog()); got != 0 || probe.callCount() != 0 {
		t.Fatalf("under the claim the pre-reclone step sent %d rotations and %d status reads; want none", got, probe.callCount())
	}
	if !f.reg.slot.release(f.ref.RunID, ReleaseTerminal) {
		t.Fatal("fixture: the claim was not held")
	}
	if f.tm.checkReclone(context.Background()) {
		t.Fatal("the reclone started before its recipient's tail was archived")
	}
	if got := len(f.tgt.flushLog()); got != 1 || probe.callCount() == 0 {
		t.Fatalf("after the claim was released: %d rotations, %d status reads; want the wait to start", got, probe.callCount())
	}
}

// TestCheckReclone_ClaimTakenDuringTheStatusRequestSendsNoRotation (r1 L1):
// the poll's claim check and the rotation are not one step. A planned
// failover that claims the sites while the wait's status request is in
// flight is seen by the check the step makes right before the statement:
// nothing is sent to the site the run now owns, and the rotation is sent
// once the claim is released.
func TestCheckReclone_ClaimTakenDuringTheStatusRequestSendsNoRotation(t *testing.T) {
	var f *opsFixture
	probe := &tailSourceScript{enabled: true}
	probe.answer = func(call int) (*mysql.ArchiverStatus, error) {
		if call == 1 {
			f.hold(false) // the run claims the sites while the request is in flight
		}
		return tailStatus(mysql.ArchiverTailPending, "binlog.000001", 1, ""), nil
	}
	f = recloneUnderClaimFixture(t, probe, func(step func()) { step() })
	if f.tm.checkReclone(context.Background()) {
		t.Fatal("a reclone started under a planned failover's claim")
	}
	if !f.topo.PlannedClaimHeld(f.ref.Group) {
		t.Fatal("fixture: the claim was not taken during the status request")
	}
	if got := len(f.tgt.flushLog()); got != 0 {
		t.Fatalf("%d rotations were sent to dc2 under a claim taken during the status request; want none", got)
	}
	f.clk.Advance(2 * time.Second)
	if f.tm.checkReclone(context.Background()) || probe.callCount() != 1 || len(f.tgt.flushLog()) != 0 {
		t.Fatalf("under the claim: %d status reads, %d rotations; want the wait paused", probe.callCount(), len(f.tgt.flushLog()))
	}
	if !f.reg.slot.release(f.ref.RunID, ReleaseTerminal) {
		t.Fatal("fixture: the claim was not held")
	}
	f.clk.Advance(2 * time.Second)
	if f.tm.checkReclone(context.Background()) {
		t.Fatal("the reclone started before its recipient's tail was archived")
	}
	if got := len(f.tgt.flushLog()); got != 1 {
		t.Fatalf("after the claim was released: %d rotations, want 1", got)
	}
}

// TestCheckReclone_ClaimTakenWhileAStepIsInFlightCancelsIt (r1 L1): a step
// that is still in flight when another actor claims the sites is
// cancelled by the next poll, and a cancelled step sends no rotation.
func TestCheckReclone_ClaimTakenWhileAStepIsInFlightCancelsIt(t *testing.T) {
	probe := &tailSourceScript{enabled: true, answer: func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailPending, "binlog.000001", 1, ""), nil
	}}
	q := &stepQueue{}
	f := recloneUnderClaimFixture(t, probe, q.run)
	if f.tm.checkReclone(context.Background()) || len(q.steps) != 1 {
		t.Fatalf("the wait did not start a step (%d held)", len(q.steps))
	}
	f.hold(false)
	// Before the next poll the step's own check stops it ...
	q.runNext(t)
	if got := len(f.tgt.flushLog()); got != 0 {
		t.Fatalf("%d rotations were sent under the claim; want none", got)
	}
	// ... and a step started before the claim and still in flight at the
	// next poll is cancelled by it.
	if !f.reg.slot.release(f.ref.RunID, ReleaseTerminal) {
		t.Fatal("fixture: the claim was not held")
	}
	f.clk.Advance(2 * time.Second)
	if f.tm.checkReclone(context.Background()) || len(q.steps) != 1 {
		t.Fatalf("the wait did not start its next step (%d held)", len(q.steps))
	}
	step := f.tm.recloneTail.step
	f.hold(false)
	f.tm.checkReclone(context.Background())
	if f.tm.recloneTail == nil || f.tm.recloneTail.step != nil {
		t.Fatal("the poll under the claim left the step in flight")
	}
	if !f.reg.slot.release(f.ref.RunID, ReleaseTerminal) {
		t.Fatal("fixture: the claim was not held")
	}
	q.runNext(t) // the cancelled step returns after the claim is gone
	if got := len(f.tgt.flushLog()); got != 0 {
		t.Fatalf("a cancelled step sent %d rotations", got)
	}
	f.tm.recloneTailMu.Lock()
	done := step.done
	f.tm.recloneTailMu.Unlock()
	if !done {
		t.Fatal("fixture: the cancelled step did not run")
	}
	waitRecloneTailWorkers(t, f.tm)
}
