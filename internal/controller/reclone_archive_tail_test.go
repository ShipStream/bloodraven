package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// tailProbeScript is a scripted ArchiveTailProbe: it answers the recipient's
// status from a function of the call number, and records the calls.
type tailProbeScript struct {
	mu      sync.Mutex
	enabled bool
	answer  func(call int) (*mysql.ArchiverStatus, error)
	calls   int
	sites   []string
}

func (s *tailProbeScript) probe(_ context.Context, site string) (*mysql.ArchiverStatus, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.sites = append(s.sites, site)
	if !s.enabled {
		return nil, false, nil
	}
	st, err := s.answer(s.calls)
	return st, true, err
}

func (s *tailProbeScript) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func tailStatus(state, active string, pending int64, reason string) *mysql.ArchiverStatus {
	return &mysql.ArchiverStatus{Enabled: true, Tail: &mysql.ArchiverTailStatus{
		State: state, ActiveFile: active, NewestSealedFile: "binlog.000001", PendingFiles: pending, Reason: reason}}
}

// recloneTailFixture is a two-site manager on a fake clock with dc2 the
// reclone recipient, a scripted sidecar probe and a recorder of the
// not-archived callback.
type recloneTailFixture struct {
	tm        *TopologyManager
	clk       *clock.FakeClock
	primary   *mockMySQL
	recipient *mockMySQL
	probe     *tailProbeScript
	logs      *syncBuffer
	reported  []string
}

func newRecloneTailFixture(t *testing.T, answer func(call int) (*mysql.ArchiverStatus, error)) *recloneTailFixture {
	t.Helper()
	f := &recloneTailFixture{
		primary:   &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"},
		recipient: &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"},
		probe:     &tailProbeScript{enabled: true, answer: answer},
	}
	f.tm, f.clk, f.logs = newRecloneTailManager(f.primary, f.recipient)
	f.tm.ArchiveTailProbe = f.probe.probe
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

func (f *recloneTailFixture) flushes() int {
	f.recipient.mu.Lock()
	defer f.recipient.mu.Unlock()
	return f.recipient.flushBinaryLogsCalls
}

// TestRecloneTail_SkippedWithoutPITR: with no status source wired, or with
// PITR not enabled for the group, the reclone sends no rotation, waits for
// nothing and reports nothing.
func TestRecloneTail_SkippedWithoutPITR(t *testing.T) {
	f := newRecloneTailFixture(t, func(int) (*mysql.ArchiverStatus, error) { return nil, errors.New("must not matter") })
	f.probe.enabled = false
	if !f.step() {
		t.Fatal("a group without PITR waited before its reclone")
	}
	f.tm.ArchiveTailProbe = nil
	if !f.step() {
		t.Fatal("a manager without a status source waited before its reclone")
	}
	if f.flushes() != 0 || len(f.reported) != 0 {
		t.Fatalf("without PITR the reclone rotated %d times and reported %v", f.flushes(), f.reported)
	}
}

// TestRecloneTail_WaitsForADrainedScanOfTheRotation: the wait takes the
// baseline, rotates once, and ends only on a status that is Drained and
// names another active file than the baseline, so a Drained from before the
// rotation, or a scan still uploading, does not end it.
func TestRecloneTail_WaitsForADrainedScanOfTheRotation(t *testing.T) {
	f := newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		switch call {
		case 1: // baseline: the server writes 000007
			return tailStatus(mysql.ArchiverTailDrained, "binlog.000007", 0, ""), nil
		case 2: // the sidecar has not scanned since the rotation
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
	if !strings.Contains(line, "site=dc2") || !strings.Contains(line, "waitedMs=6000") {
		t.Fatalf("log line %q; want the site and the 6s it waited", line)
	}
	// Settled: a clone deferred after it does not rotate or wait again.
	if !f.step() || f.flushes() != 1 || f.probe.callCount() != 4 {
		t.Fatalf("a settled wait ran again: rotations=%d probes=%d", f.flushes(), f.probe.callCount())
	}
	f.tm.clearRecloneArchiveTail()
	if f.step() {
		t.Fatal("a new reclone of the site did not wait")
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
		flushes  func(polls int) int
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
			flushes:  func(polls int) int { return polls }, // retried every poll
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
			start := f.clk.Now()
			polls := 0
			for !f.step() {
				polls++
				if polls > 200 {
					t.Fatal("the wait never ended")
				}
				if len(f.reported) != 0 {
					t.Fatalf("the gap was announced before the wait ended: %v", f.reported)
				}
			}
			if waited := f.clk.Since(start); waited != preRecloneArchiveTailTimeout {
				t.Fatalf("the wait ended after %s, want exactly the %s bound", waited, preRecloneArchiveTailTimeout)
			}
			if len(f.reported) != 1 || !strings.HasPrefix(f.reported[0], "dc2: ") || !strings.Contains(f.reported[0], tc.reason) {
				t.Fatalf("reported %q, want one report naming dc2 with %q", f.reported, tc.reason)
			}
			if got, want := f.flushes(), tc.flushes(polls+1); got != want {
				t.Fatalf("rotations = %d, want %d", got, want)
			}
			line := logLine(t, f.logs.String(), "pre-reclone archive tail not archived")
			if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "site=dc2") || !strings.Contains(line, "waitedMs=120000") {
				t.Fatalf("log line %q; want WARN with the site and the bound", line)
			}
		})
	}
}

// TestRecloneTail_NoArchiverEndsAtOnce: PITR is enabled but the recipient's
// sidecar runs no archiver, so nothing can archive the tail: the gap is
// announced on the first poll and the reclone is not delayed.
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
// there) takes a new baseline, rotates again and waits its own full bound.
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
	probe2 := &tailProbeScript{enabled: true, answer: stuck}
	var reported []string
	tm2.ArchiveTailProbe = probe2.probe
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

// TestCheckReclone_WaitsForTheArchiveTailBeforeCloning: with PITR enabled a
// reclone request does not reach CLONE until the recipient's tail is
// archived (or the bound has passed), and the pending request survives the
// wait.
func TestCheckReclone_WaitsForTheArchiveTailBeforeCloning(t *testing.T) {
	drained := false
	var mu sync.Mutex
	f := newRecloneTailFixture(t, func(call int) (*mysql.ArchiverStatus, error) {
		mu.Lock()
		defer mu.Unlock()
		if call == 1 || !drained {
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

// TestArchiverPollerTailStatus: the pre-reclone probe reads the named
// site's sidecar live, and only when the group's PITR spec renders an
// archiver; it records nothing in the poller's snapshots.
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

	if st, enabled, err := p.tailStatus(context.Background(), "dc2"); st != nil || enabled || err != nil || requests != 0 {
		t.Fatalf("PITR not rendered: tailStatus = %+v, %v, %v after %d requests; want disabled and no request", st, enabled, err, requests)
	}
	p.rolloutRenders = true
	st, enabled, err := p.tailStatus(context.Background(), "dc2")
	if err != nil || !enabled || st == nil || st.Tail == nil {
		t.Fatalf("tailStatus = %+v, %v, %v", st, enabled, err)
	}
	if *st.Tail != (mysql.ArchiverTailStatus{State: mysql.ArchiverTailDrained, ActiveFile: "binlog.000008", NewestSealedFile: "binlog.000007"}) {
		t.Fatalf("tail = %+v", *st.Tail)
	}
	if _, enabled, err := p.tailStatus(context.Background(), "dc1"); err == nil || !enabled {
		t.Fatalf("a site with no sidecar client: enabled=%v err=%v; want an error", enabled, err)
	}
	if _, _, err := p.tailStatus(context.Background(), "dc9"); err == nil {
		t.Fatal("an unknown site returned no error")
	}
	if len(p.Snapshots()) != 0 {
		t.Fatalf("the probe recorded snapshots: %v", p.Snapshots())
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

// TestCheckReclone_NoRotationUnderAPlannedClaim: while a planned failover
// holds the sites the clone cannot start, so the pre-reclone step sends
// nothing: no status read and no FLUSH BINARY LOGS on a site the run owns.
// It starts once the claim is released. (The DST planned sweep flags the
// rotation as ForeignMutationUnderClaim without this.)
func TestCheckReclone_NoRotationUnderAPlannedClaim(t *testing.T) {
	f := newOpsFixture(t, "")
	f.tm.bootstrap = NewBootstrapController(testLogger())
	f.tm.autoBootstrapSuppressed = true
	probe := &tailProbeScript{enabled: true, answer: func(int) (*mysql.ArchiverStatus, error) {
		return tailStatus(mysql.ArchiverTailPending, "binlog.000007", 1, ""), nil
	}}
	f.tm.ArchiveTailProbe = probe.probe
	f.hold(false)
	f.tm.SetRecloneSite("dc2")
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
