package component

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// The best-effort binary log rotations on the failover path (#229, #264):
// the promotion candidate's before its writes are enabled, the fenced
// source's at a handoff, and a reclone recipient's before CLONE. These
// tests run the production FailoverController and TopologyManager, and for
// the pool tests the production mysql.Checker over a fake server, with the
// primary pool at its production size.

const rotationUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// pooledChecker is a real mysql.Checker over r whose primary pool has the
// production pool's two connections.
func pooledChecker(t *testing.T, r *testutil.FakeReplica) mysql.Checker {
	t.Helper()
	db := sql.OpenDB(r)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	c := mysql.NewCheckerFromPools(db, sql.OpenDB(r), r)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// slowFlush makes r's FLUSH BINARY LOGS block until its statement is
// abandoned, and returns a channel that is closed when one is in flight.
func slowFlush(r *testutil.FakeReplica) <-chan struct{} {
	entered := make(chan struct{})
	var once sync.Once
	r.OnFlush(func(ctx context.Context) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	})
	return entered
}

// probeLikeAPoll runs the reads a topology poll issues against one site,
// each under the poll's own probe timeout, and returns the slowest.
func probeLikeAPoll(c mysql.Checker) (time.Duration, error) {
	var slowest time.Duration
	for _, probe := range []func(ctx context.Context) error{
		func(ctx context.Context) error { _, err := c.CheckReadOnly(ctx); return err },
		func(ctx context.Context) error { _, err := c.ShowReplicaStatus(ctx); return err },
		func(ctx context.Context) error { _, err := c.GetGtidExecuted(ctx); return err },
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		err := probe(ctx)
		cancel()
		if err != nil {
			return 0, err
		}
		slowest = max(slowest, time.Since(start))
	}
	return slowest, nil
}

// TestHandoffPoolContention_SlowRotationStarvesNoProbe is #229's pool
// contention test: a maintenance handoff holds one of the candidate's two
// pooled connections for its pinned promotion session, and the promotion's
// FLUSH BINARY LOGS is slow (it never answers). Health probes of the
// candidate, issued concurrently as the topology poll and other actors
// issue them, must all be served by the remaining pooled connection, and
// the handoff must complete, on the pinned session, as soon as the injected
// clock reaches the rotation's bound.
func TestHandoffPoolContention_SlowRotationStarvesNoProbe(t *testing.T) {
	set := rotationUUID + ":1-100"
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: set}
	candidate := testutil.NewFakeReplica(testutil.FakeReplicaState{
		Configured: true, IORunning: true, SQLRunning: true, SourceHost: "primary-internal",
		RetrievedGtidSet: set, ExecutedGtidSet: set, ReadOnly: true, SuperReadOnly: true,
	})
	checker := pooledChecker(t, candidate)
	entered := slowFlush(candidate)

	clk := clock.NewFakeClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
	fc := controller.NewFailoverControllerWithClock(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), clk)
	type result struct {
		gtid string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		gtid, err := fc.ExecuteHandoff(context.Background(), checker, src, "standby", controller.HandoffOptions{SourceSite: "active"})
		done <- result{gtid, err}
	}()

	select {
	case <-entered:
	case r := <-done:
		t.Fatalf("the handoff ended (%q, %v) before the candidate's rotation was sent", r.gtid, r.err)
	case <-time.After(10 * time.Second):
		t.Fatal("the candidate's rotation was never sent")
	}
	// The promotion is parked on its rotation: the pinned session holds a
	// pooled connection and the candidate is still read-only.
	if st := candidate.State(); !st.ReadOnly || st.Configured {
		t.Fatalf("candidate state while the rotation is in flight: %+v; want reset and still read-only", st)
	}

	// Health probes from four concurrent actors. A rotation that used a
	// pooled connection would leave them none, and every probe would time
	// out here.
	const actors, rounds = 4, 25
	var slowest atomic.Int64
	errs := make(chan error, actors)
	var wg sync.WaitGroup
	for a := 0; a < actors; a++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				took, err := probeLikeAPoll(checker)
				if err != nil {
					errs <- err
					return
				}
				for {
					cur := slowest.Load()
					if int64(took) <= cur || slowest.CompareAndSwap(cur, int64(took)) {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("a health probe failed while the rotation was in flight: %v", err)
	}
	if got := time.Duration(slowest.Load()); got > time.Second {
		t.Fatalf("the slowest health probe took %s while the rotation was in flight; probes are being starved", got)
	}
	select {
	case r := <-done:
		t.Fatalf("the handoff ended (%q, %v) before the clock reached the rotation's bound", r.gtid, r.err)
	default:
	}

	// The bound passes on the injected clock: the rotation is abandoned and
	// the promotion continues on its pinned session.
	clk.Advance(controller.BinlogRotationTimeout)
	select {
	case r := <-done:
		if r.err != nil || r.gtid == "" {
			t.Fatalf("ExecuteHandoff = %q, %v; a slow rotation must not fail the handoff", r.gtid, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handoff did not complete after the rotation's bound")
	}
	st := candidate.State()
	if st.ReadOnly || st.SuperReadOnly {
		t.Fatalf("candidate not writable after the handoff: %+v", st)
	}
	if len(st.BinlogPreviousGtids) != 0 {
		t.Fatalf("the abandoned rotation rotated the fake: %v", st.BinlogPreviousGtids)
	}
	stmts := candidate.Statements()
	flush, clear := slices.Index(stmts, "FLUSH BINARY LOGS"), slices.Index(stmts, "SET GLOBAL super_read_only = OFF")
	if flush < 0 || clear < flush {
		t.Fatalf("candidate statements %v; want the rotation before the clears", stmts)
	}
	if took, err := probeLikeAPoll(checker); err != nil || took > time.Second {
		t.Fatalf("health probe after the handoff: %s, %v", took, err)
	}
}

// TestHandoff_RotationFailuresChangeNothing: with both rotations failing
// (the source's and the candidate's), and with both slow, the handoff's
// statements and outcome are those of a handoff whose rotations succeeded.
func TestHandoff_RotationFailuresChangeNothing(t *testing.T) {
	set := rotationUUID + ":1-100"
	run := func(t *testing.T, arm func(src *testutil.FakeMySQL, candidate *testutil.FakeReplica, clk *clock.FakeClock)) (srcCalls, candStmts []string) {
		t.Helper()
		src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: set}
		candidate := testutil.NewFakeReplica(testutil.FakeReplicaState{
			Configured: true, IORunning: true, SQLRunning: true, SourceHost: "primary-internal",
			RetrievedGtidSet: set, ExecutedGtidSet: set, ReadOnly: true, SuperReadOnly: true,
		})
		clk := clock.NewFakeClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
		if arm != nil {
			arm(src, candidate, clk)
		}
		fc := controller.NewFailoverControllerWithClock(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), clk)
		gtid, err := fc.ExecuteHandoff(context.Background(), pooledChecker(t, candidate), src, "standby", controller.HandoffOptions{SourceSite: "active"})
		if err != nil || gtid == "" {
			t.Fatalf("ExecuteHandoff = %q, %v", gtid, err)
		}
		if st := candidate.State(); st.ReadOnly || st.SuperReadOnly {
			t.Fatalf("candidate not writable: %+v", st)
		}
		if !src.SuperReadOnlySet {
			t.Fatal("the source is not fenced after a completed handoff")
		}
		return src.GetCalls(), candidate.Statements()
	}
	wantSrc, wantCand := run(t, nil)
	if !slices.Contains(wantSrc, "FlushBinaryLogs") || !slices.Contains(wantCand, "FLUSH BINARY LOGS") {
		t.Fatalf("a clean handoff sent no rotation: source %v, candidate %v", wantSrc, wantCand)
	}
	cases := map[string]func(src *testutil.FakeMySQL, candidate *testutil.FakeReplica, clk *clock.FakeClock){
		"failing": func(src *testutil.FakeMySQL, candidate *testutil.FakeReplica, _ *clock.FakeClock) {
			src.FlushBinaryLogsErr = errors.New("connection refused")
			candidate.OnFlush(func(context.Context) error { return errors.New("connection refused") })
		},
		"slow": func(src *testutil.FakeMySQL, candidate *testutil.FakeReplica, clk *clock.FakeClock) {
			// Each rotation takes its whole bound of injected time and
			// answers only when abandoned.
			slow := func(ctx context.Context) error {
				clk.Advance(controller.BinlogRotationTimeout)
				<-ctx.Done()
				return ctx.Err()
			}
			src.FlushBinaryLogsFn = slow
			candidate.OnFlush(slow)
		},
	}
	for name, arm := range cases {
		t.Run(name, func(t *testing.T) {
			gotSrc, gotCand := run(t, arm)
			if !slices.Equal(gotSrc, wantSrc) {
				t.Fatalf("source calls = %v, want %v as with acknowledged rotations", gotSrc, wantSrc)
			}
			if !slices.Equal(gotCand, wantCand) {
				t.Fatalf("candidate statements = %v, want %v as with acknowledged rotations", gotCand, wantCand)
			}
		})
	}
}

// TestFailover_RotationStartsTheNewFileAtThePromotionGTID: an emergency
// failover through the topology manager rotates the candidate's binary log
// before enabling its writes, and the file that rotation opens starts at
// the set the operator records as lastFailover.promotionGtidExecuted. A
// later lane (#269) relies on that equality.
func TestFailover_RotationStartsTheNewFileAtThePromotionGTID(t *testing.T) {
	gtid := rotationUUID + ":1-42"
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: gtid}
	h := newTestHarnessWithMySQL(t, &mockMySQL{readOnly: false, gtidExecuted: gtid}, dc2)
	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	h.pollN(3)
	if dc2.isReadOnly() {
		t.Fatal("dc2 was not promoted")
	}
	flushed := dc2.flushedAt()
	if len(flushed) != 1 || flushed[0] != gtid {
		t.Fatalf("dc2 rotations found the sets %v, want exactly one at %q", flushed, gtid)
	}
	if got := h.tm.Status().PromotionGtidExecuted(); got != flushed[0] {
		t.Fatalf("recorded promotion GTID %q, the rotated file starts at %q", got, flushed[0])
	}
	if got := dc2.callCount("FlushBinaryLogs"); got != 1 {
		t.Fatalf("dc2 received %d rotations, want 1", got)
	}
}

// TestFailover_FailingRotationStillPromotes: the emergency failover does
// not depend on the rotation.
func TestFailover_FailingRotationStillPromotes(t *testing.T) {
	gtid := rotationUUID + ":1-42"
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: gtid}
	dc2.failNextCalls("FlushBinaryLogs", 1)
	h := newTestHarnessWithMySQL(t, &mockMySQL{readOnly: false, gtidExecuted: gtid}, dc2)
	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	h.pollN(3)
	if dc2.isReadOnly() {
		t.Fatal("dc2 was not promoted although only its rotation failed")
	}
	if got := h.tm.Status().PromotionGtidExecuted(); got != gtid {
		t.Fatalf("recorded promotion GTID %q, want %q", got, gtid)
	}
	if got := dc2.flushedAt(); len(got) != 0 {
		t.Fatalf("a failed rotation rotated: %v", got)
	}
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Fatalf("DNS not flipped after the promotion: %s", h.dns.getLastIP())
	}
}

// --- the pre-reclone archive-tail wait (#264) ------------------------------

// sidecarTail is the recipient sidecar's /archiver/status as the
// pre-reclone wait reads it (controller.ArchiveTailSource). The recipient
// mock writes binlog.000001 until its first rotation, which opens
// binlog.000002.
type sidecarTail struct {
	mu          sync.Mutex
	pitrOff     bool
	unreachable bool
	// hang holds every status request until its context ends: a sidecar
	// that never answers.
	hang   bool
	active string
	state  string
	reason string
	// scanOnFlush makes the fake behave like a sidecar that scans on the
	// rotation: once the recipient's binary log was rotated it reports a
	// scan with scanned as the active file, ended scannedState (Drained
	// when empty).
	scanOnFlush  func() bool
	scanned      string
	scannedState string
	probes       int
}

func (s *sidecarTail) ArchivingEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.pitrOff
}

func (s *sidecarTail) TailStatus(ctx context.Context, _ string) (*mysql.ArchiverStatus, error) {
	s.mu.Lock()
	s.probes++
	hang := s.hang
	s.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreachable {
		return nil, errors.New("dial tcp: connection refused")
	}
	active, state := s.active, s.state
	if s.scanOnFlush != nil && s.scanOnFlush() {
		active, state = s.scanned, mysql.ArchiverTailDrained
		if s.scannedState != "" {
			state = s.scannedState
		}
	}
	return &mysql.ArchiverStatus{Enabled: true, Tail: &mysql.ArchiverTailStatus{State: state, ActiveFile: active, Reason: s.reason}}, nil
}

func (s *sidecarTail) probeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probes
}

// recloneTailHarness is a bootstrap harness whose failover controller runs
// on the harness clock, with dc2 the reclone recipient. The wait's steps
// run inline, so a poll's outcome does not depend on goroutine timing;
// TestReclone_HungRecipientDoesNotSlowThePollLoop runs them as production
// does.
func recloneTailHarness(t *testing.T, tail *sidecarTail, announced *[]string) *testHarness {
	t.Helper()
	gtid := rotationUUID + ":1-42"
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: gtid,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	h := newRecloneHarness(t, dc1, dc2)
	h.tm.SetAutoBootstrapSuppressed(true)
	h.tm.ArchiveTail = tail
	h.tm.SetRecloneTailStepRunnerForSim(func(step func()) { step() })
	h.tm.ArchiveTailNotArchivedCallback = func(site, reason string) { *announced = append(*announced, site+": "+reason) }
	h.pollN(2)
	return h
}

// newRecloneHarness is newBootstrapHarness with the failover controller on
// the harness clock, so the rotation's bound is the fake clock's.
func newRecloneHarness(t *testing.T, dc1, dc2 *mockMySQL) *testHarness {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	cfg := controller.TopologyConfig{
		Name: "lion", Sites: defaultTwoSiteConfig(), PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: int64(5 * time.Minute),
	}
	bootCfg := controller.BootstrapConfig{ReplUser: "replicator", ReplPassword: "secret", CloneTimeout: 30 * time.Second}
	tainter, hub, dns := newMockTainter(), platform.NewHub(logger), &mockDNS{}
	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2},
		controller.NewFailoverControllerWithClock(logger, clk), nil, controller.NewBootstrapController(logger), bootCfg,
		tainter, hub, dns, logger, clk)
	return &testHarness{tm: tm, dc1MySQL: dc1, dc2MySQL: dc2, tainter: tainter, dns: dns, hub: hub, logger: logger, clock: clk}
}

// pollUntilCloned polls (2s of fake time per poll) until dc2's CLONE was
// issued, and returns the fake time that took.
func pollUntilCloned(t *testing.T, h *testHarness, maxPolls int) time.Duration {
	t.Helper()
	start := h.clock.Now()
	for i := 0; i < maxPolls; i++ {
		h.pollN(1)
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) && h.tm.BootstrapPhase() != controller.BootstrapPhaseNone && !h.dc2MySQL.cloned() {
			time.Sleep(2 * time.Millisecond) // the clone runs in the manager's goroutine
		}
		if h.dc2MySQL.cloned() {
			return h.clock.Since(start)
		}
		h.clock.Advance(2 * time.Second)
	}
	t.Fatalf("dc2 was not cloned within %d polls", maxPolls)
	return 0
}

// TestReclone_WaitsForTheSidecarToArchiveTheTail: with PITR enabled a
// reclone rotates the recipient's binary log and starts CLONE only once the
// recipient's sidecar reports a Drained scan of the rotated log; nothing is
// announced.
func TestReclone_WaitsForTheSidecarToArchiveTheTail(t *testing.T) {
	var announced []string
	tail := &sidecarTail{active: "binlog.000001", state: mysql.ArchiverTailPending}
	h := recloneTailHarness(t, tail, &announced)
	h.tm.SetRecloneSite("dc2")
	for i := 0; i < 5; i++ {
		h.pollN(1)
		h.clock.Advance(2 * time.Second)
		if h.dc2MySQL.cloned() {
			t.Fatalf("poll %d: CLONE was issued before the sidecar archived the recipient's tail", i+1)
		}
	}
	if got := h.dc2MySQL.callCount("FlushBinaryLogs"); got != 1 {
		t.Fatalf("the recipient received %d rotations during the wait, want 1", got)
	}
	// A scan that read the binary log index before the rotation ends
	// Drained: it never saw the file the rotation sealed (r2 F2).
	tail.mu.Lock()
	tail.state = mysql.ArchiverTailDrained
	tail.mu.Unlock()
	for i := 0; i < 3; i++ {
		h.pollN(1)
		h.clock.Advance(2 * time.Second)
		if h.dc2MySQL.cloned() {
			t.Fatal("CLONE was issued on a Drained scan from before the rotation")
		}
	}
	// The sidecar finishes: it reports the rotated log, Drained.
	tail.mu.Lock()
	tail.active = "binlog.000002"
	tail.mu.Unlock()
	pollUntilCloned(t, h, 3)
	if len(announced) != 0 {
		t.Fatalf("an archived tail was announced as not archived: %v", announced)
	}
	if got := h.dc1MySQL.callCount("FlushBinaryLogs"); got != 0 {
		t.Fatalf("the donor's binary log was rotated %d times", got)
	}
}

// TestReclone_UnconfirmedTailIsAnnouncedAndCloneProceeds: when the rotation
// fails, or the sidecar never reports the tail archived, or it cannot be
// reached, the reclone still starts, at the wait's bound and not before,
// and the gap is announced with the site and the cause.
func TestReclone_UnconfirmedTailIsAnnouncedAndCloneProceeds(t *testing.T) {
	cases := []struct {
		name   string
		arm    func(h *testHarness, tail *sidecarTail)
		reason string
	}{
		{"rotation fails", func(h *testHarness, _ *sidecarTail) { h.dc2MySQL.failNextCalls("FlushBinaryLogs", 1000) },
			"FLUSH BINARY LOGS on the site was not acknowledged"},
		{"sidecar never drains", func(h *testHarness, tail *sidecarTail) {
			// It scans the rotated log and cannot upload the tail.
			tail.scanOnFlush = func() bool { return h.dc2MySQL.callCount("FlushBinaryLogs") > 0 }
			tail.scanned, tail.scannedState, tail.reason = "binlog.000002", mysql.ArchiverTailBlocked, "archive store unreachable"
		}, "cannot archive the tail: archive store unreachable"},
		{"sidecar unreachable", func(_ *testHarness, tail *sidecarTail) { tail.unreachable = true }, "did not answer"},
		{"the rotated file's name cannot be read", func(h *testHarness, tail *sidecarTail) {
			h.dc2MySQL.binlogFileErr = errors.New("Error 1227: access denied")
			tail.state = mysql.ArchiverTailDrained // whatever the sidecar says
		}, "the name of the file it opened could not be read"},
		{"a scan from before the rotation", func(_ *testHarness, tail *sidecarTail) {
			tail.state = mysql.ArchiverTailDrained
		}, "its last scan saw binlog.000001 as the active file; the rotation opened binlog.000002"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var announced []string
			tail := &sidecarTail{active: "binlog.000001", state: mysql.ArchiverTailPending}
			h := recloneTailHarness(t, tail, &announced)
			tc.arm(h, tail)
			h.tm.SetRecloneSite("dc2")
			waited := pollUntilCloned(t, h, 100)
			if waited != 2*time.Minute {
				t.Fatalf("CLONE was issued after %s of waiting, want exactly the 2m bound", waited)
			}
			if len(announced) != 1 || !strings.HasPrefix(announced[0], "dc2: ") || !strings.Contains(announced[0], tc.reason) {
				t.Fatalf("announced %q, want one announcement naming dc2 with %q", announced, tc.reason)
			}
		})
	}
}

// TestReclone_OperatorRestartMidWaitStartsOver: the wait is in memory. A
// new manager (the operator restarted; the reclone annotation is still on
// the group, so the request is applied again) rotates again and waits
// again, and clones once the sidecar reports the tail archived.
func TestReclone_OperatorRestartMidWaitStartsOver(t *testing.T) {
	var announced []string
	tail := &sidecarTail{active: "binlog.000001", state: mysql.ArchiverTailPending}
	h := recloneTailHarness(t, tail, &announced)
	h.tm.SetRecloneSite("dc2")
	for i := 0; i < 10; i++ {
		h.pollN(1)
		h.clock.Advance(2 * time.Second)
	}
	if h.dc2MySQL.cloned() {
		t.Fatal("CLONE was issued during the first process's wait")
	}

	// The replacement process over the same servers.
	h2 := newRecloneHarness(t, h.dc1MySQL, h.dc2MySQL)
	h2.clock.Set(h.clock.Now())
	h2.tm.SetAutoBootstrapSuppressed(true)
	var announced2 []string
	// The first process's rotation opened binlog.000002, which the sidecar
	// has seen; the new process's rotation opens binlog.000003.
	tail2 := &sidecarTail{active: "binlog.000002", state: mysql.ArchiverTailPending, scanned: "binlog.000003"}
	rotations := h.dc2MySQL.callCount("FlushBinaryLogs")
	tail2.scanOnFlush = func() bool { return h.dc2MySQL.callCount("FlushBinaryLogs") > rotations }
	h2.tm.ArchiveTail = tail2
	h2.tm.SetRecloneTailStepRunnerForSim(func(step func()) { step() })
	h2.tm.ArchiveTailNotArchivedCallback = func(site, reason string) { announced2 = append(announced2, site) }
	h2.pollN(2)
	h2.tm.SetRecloneSite("dc2")
	waited := pollUntilCloned(t, h2, 10)
	if waited > 10*time.Second {
		t.Fatalf("the restarted wait took %s although the sidecar archived the tail at once", waited)
	}
	if got := h.dc2MySQL.callCount("FlushBinaryLogs"); got != rotations+1 {
		t.Fatalf("rotations after the restart = %d, want %d (one more, from the new process)", got, rotations+1)
	}
	if len(announced)+len(announced2) != 0 {
		t.Fatalf("announcements: %v %v; the tail was archived", announced, announced2)
	}
}

// TestReclone_WithoutPITRClonesAtOnce: a group without PITR archiving
// neither rotates nor waits nor announces.
func TestReclone_WithoutPITRClonesAtOnce(t *testing.T) {
	gtid := rotationUUID + ":1-42"
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: gtid,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	h := newRecloneHarness(t, dc1, dc2)
	h.tm.SetAutoBootstrapSuppressed(true)
	tail := &sidecarTail{pitrOff: true} // PITR not enabled for the group
	h.tm.ArchiveTail = tail
	h.pollN(2)
	h.tm.SetRecloneSite("dc2")
	if waited := pollUntilCloned(t, h, 2); waited != 0 {
		t.Fatalf("a reclone without PITR waited %s", waited)
	}
	if got := dc2.callCount("FlushBinaryLogs"); got != 0 {
		t.Fatalf("a reclone without PITR rotated the recipient %d times", got)
	}
	if got := tail.probeCount(); got != 0 {
		t.Fatalf("a reclone without PITR read the recipient's sidecar %d times", got)
	}
}

// TestReclone_HungRecipientDoesNotSlowThePollLoop (r1 M1): the pre-reclone
// wait's status request and rotation run off the poll goroutine, as in
// production (no inline runner here). With a recipient whose sidecar never
// answers, or whose rotation never answers, every poll still returns at
// once, for the whole wait, and the loss of the primary is detected after
// the same number of polls as with no reclone pending. When the request is
// dropped (the recipient became the primary) the step in flight ends; no
// goroutine is left behind.
func TestReclone_HungRecipientDoesNotSlowThePollLoop(t *testing.T) {
	// A poll with nothing to wait for takes well under a millisecond of
	// wall time here; one that waits on the recipient would take the
	// sidecar client's timeout (seconds) or, on this fake clock, never
	// return.
	const pollBudget = 2 * time.Second
	poll := func(t *testing.T, h *testHarness) {
		t.Helper()
		done := make(chan struct{})
		go func() { h.pollN(1); close(done) }()
		select {
		case <-done:
		case <-time.After(pollBudget):
			t.Fatalf("a poll did not return within %s: it waits on the reclone recipient", pollBudget)
		}
	}
	// pollsToFailover takes the primary away and counts the polls until
	// dc2 is promoted.
	pollsToFailover := func(t *testing.T, h *testHarness) int {
		t.Helper()
		h.dc1MySQL.setError(errDown)
		for n := 1; n <= 20; n++ {
			poll(t, h)
			h.clock.Advance(2 * time.Second)
			if !h.dc2MySQL.isReadOnly() {
				return n
			}
		}
		t.Fatal("dc2 was not promoted within 20 polls of the primary's loss")
		return 0
	}
	newHarness := func(t *testing.T) *testHarness {
		gtid := rotationUUID + ":1-42"
		dc1 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
		dc2 := &mockMySQL{readOnly: true, gtidExecuted: gtid,
			replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
		h := newRecloneHarness(t, dc1, dc2)
		h.tm.SetAutoBootstrapSuppressed(true)
		h.pollN(2)
		return h
	}
	control := pollsToFailover(t, newHarness(t))

	for _, tc := range []struct {
		name string
		arm  func(h *testHarness, tail *sidecarTail)
		// sent is what the wait must have sent to the recipient while it
		// hung.
		sent func(t *testing.T, h *testHarness, tail *sidecarTail)
	}{
		{
			name: "the sidecar never answers",
			arm:  func(_ *testHarness, tail *sidecarTail) { tail.hang = true },
			sent: func(t *testing.T, h *testHarness, tail *sidecarTail) {
				if tail.probeCount() == 0 || h.dc2MySQL.callCount("RotateBinaryLog") != 0 {
					t.Fatalf("status requests = %d, rotations = %d; want the wait stuck on its status request", tail.probeCount(), h.dc2MySQL.callCount("RotateBinaryLog"))
				}
			},
		},
		{
			name: "the rotation never answers",
			arm: func(h *testHarness, _ *sidecarTail) {
				h.dc2MySQL.rotateBinaryLogFn = func(ctx context.Context) (string, error) {
					<-ctx.Done()
					return "", ctx.Err()
				}
			},
			sent: func(t *testing.T, h *testHarness, _ *sidecarTail) {
				if h.dc2MySQL.callCount("RotateBinaryLog") == 0 {
					t.Fatal("the wait never sent its rotation")
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var announced []string
			tail := &sidecarTail{active: "binlog.000001", state: mysql.ArchiverTailPending}
			h := newHarness(t)
			h.tm.ArchiveTail = tail
			h.tm.ArchiveTailNotArchivedCallback = func(site, reason string) { announced = append(announced, site+": "+reason) }
			tc.arm(h, tail)
			h.tm.SetRecloneSite("dc2")
			// A minute of the wait: thirty polls, each returning at once.
			for i := 0; i < 30; i++ {
				poll(t, h)
				h.clock.Advance(2 * time.Second)
			}
			if h.dc2MySQL.cloned() || len(announced) != 0 {
				t.Fatalf("the wait ended early: cloned=%v announced=%v", h.dc2MySQL.cloned(), announced)
			}
			tc.sent(t, h, tail)
			if got := pollsToFailover(t, h); got != control {
				t.Fatalf("the primary's loss was detected and failed over after %d polls with a hung reclone recipient, %d without a reclone", got, control)
			}
			// dc2 is the primary now: the request is dropped and the step
			// in flight is cancelled.
			poll(t, h)
			poll(t, h)
			idle := make(chan struct{})
			go func() { h.tm.WaitRecloneTailStepsForSim(); close(idle) }()
			select {
			case <-idle:
			case <-time.After(5 * time.Second):
				t.Fatal("a step of the pre-reclone wait was left running after its request was dropped")
			}
			if h.dc2MySQL.cloned() {
				t.Fatal("the new primary was cloned")
			}
		})
	}
}

// cloned reports whether CLONE INSTANCE was issued on the mock.
func (m *mockMySQL) cloned() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cloneInstanceCalled
}
