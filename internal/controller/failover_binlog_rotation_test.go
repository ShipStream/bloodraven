package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// rotationFlusher is a binlogFlusher whose FlushBinaryLogs runs fn.
type rotationFlusher struct {
	fn func(ctx context.Context) error
}

func (r rotationFlusher) FlushBinaryLogs(ctx context.Context) error { return r.fn(ctx) }

// syncBuffer is a log sink a test reads while the logger may still write.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func rotationController(clk clock.Clock) (*FailoverController, *syncBuffer) {
	logs := &syncBuffer{}
	return NewFailoverControllerWithClock(slog.New(slog.NewTextHandler(logs, nil)), clk), logs
}

// logLine returns the first log line holding msg.
func logLine(t *testing.T, logs, msg string) string {
	t.Helper()
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, `msg="`+msg+`"`) {
			return line
		}
	}
	t.Fatalf("no log line with msg %q in:\n%s", msg, logs)
	return ""
}

// TestRotateBinlog_SuccessLogsTheDuration: an acknowledged rotation reports
// true and logs the documented INFO line with the site and durationMs.
func TestRotateBinlog_SuccessLogsTheDuration(t *testing.T) {
	clk := clock.NewFakeClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
	fc, logs := rotationController(clk)
	ok := fc.rotateBinlog(context.Background(), rotationFlusher{fn: func(context.Context) error {
		clk.Advance(12 * time.Millisecond)
		return nil
	}}, "dc2", promotionRotation)
	if !ok {
		t.Fatal("rotateBinlog = false for an acknowledged flush")
	}
	line := logLine(t, logs.String(), "promotion binlog rotated")
	for _, want := range []string{"level=INFO", "site=dc2", "durationMs=12"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line %q lacks %q", line, want)
		}
	}
	if strings.Contains(logs.String(), "promotion binlog rotation failed") {
		t.Fatalf("a successful rotation logged the failure line:\n%s", logs.String())
	}
}

// TestRotateBinlog_ErrorIsLoggedNotReturned: a failing flush logs the
// documented WARN line and reports false; the caller carries on.
func TestRotateBinlog_ErrorIsLoggedNotReturned(t *testing.T) {
	fc, logs := rotationController(clock.NewFakeClock(time.Unix(0, 0)))
	ok := fc.rotateBinlog(context.Background(), rotationFlusher{fn: func(context.Context) error {
		return errors.New("Error 1227: access denied; you need the RELOAD privilege")
	}}, "dc2", promotionRotation)
	if ok {
		t.Fatal("rotateBinlog = true for a failed flush")
	}
	line := logLine(t, logs.String(), "promotion binlog rotation failed")
	for _, want := range []string{"level=WARN", "site=dc2", "timedOut=false", "RELOAD", "durationMs="} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line %q lacks %q", line, want)
		}
	}
}

// TestRotateBinlog_AbandonedAtTheClockBound: the bound is the injected
// clock's, not wall time. A flush that blocks is abandoned exactly when the
// clock has advanced BinlogRotationTimeout, its context is cancelled, and
// the caller is released even though the flush never answered.
func TestRotateBinlog_AbandonedAtTheClockBound(t *testing.T) {
	clk := clock.NewFakeClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
	fc, logs := rotationController(clk)
	entered := make(chan struct{})
	flushErr := make(chan error, 1)
	returned := make(chan bool, 1)
	go func() {
		returned <- fc.rotateBinlog(context.Background(), rotationFlusher{fn: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			flushErr <- ctx.Err()
			return ctx.Err()
		}}, "dc2", promotionRotation)
	}()
	<-entered
	clk.Advance(BinlogRotationTimeout - time.Millisecond)
	select {
	case <-returned:
		t.Fatal("rotateBinlog returned before the clock reached its bound")
	case <-time.After(50 * time.Millisecond):
	}
	clk.Advance(time.Millisecond)
	select {
	case ok := <-returned:
		if ok {
			t.Fatal("rotateBinlog = true for an abandoned flush")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rotateBinlog still blocked after the clock passed its bound")
	}
	select {
	case err := <-flushErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the abandoned flush saw %v, want its context cancelled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the abandoned flush was never cancelled")
	}
	line := logLine(t, logs.String(), "promotion binlog rotation failed")
	for _, want := range []string{"level=WARN", "site=dc2", "timedOut=true", "durationMs=1000"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log line %q lacks %q", line, want)
		}
	}
}

// TestRotateBinlog_ReleasesTheCallerWhenTheFlushIgnoresCancellation: the
// caller continues at the bound even when the flush does not honour its
// context (a read parked on a blackholed socket).
func TestRotateBinlog_ReleasesTheCallerWhenTheFlushIgnoresCancellation(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	fc, _ := rotationController(clk)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	returned := make(chan bool, 1)
	go func() {
		returned <- fc.rotateBinlog(context.Background(), rotationFlusher{fn: func(context.Context) error {
			close(entered)
			<-release // ignores ctx
			return nil
		}}, "dc2", promotionRotation)
	}()
	<-entered
	clk.Advance(BinlogRotationTimeout)
	select {
	case ok := <-returned:
		if ok {
			t.Fatal("rotateBinlog = true although the flush never answered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a flush that ignores cancellation held the caller past the bound")
	}
}

// TestRotateBinlog_CancelledCaller: the caller's own cancellation ends the
// wait at once.
func TestRotateBinlog_CancelledCaller(t *testing.T) {
	fc, logs := rotationController(clock.NewFakeClock(time.Unix(0, 0)))
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	go func() {
		<-entered
		cancel()
	}()
	ok := fc.rotateBinlog(ctx, rotationFlusher{fn: func(c context.Context) error {
		close(entered)
		<-c.Done()
		return c.Err()
	}}, "dc2", promotionRotation)
	if ok {
		t.Fatal("rotateBinlog = true after its caller was cancelled")
	}
	logLine(t, logs.String(), "promotion binlog rotation failed")
}

// TestPromote_RotationNeverStopsTheEmergencyPromotion: a failing flush and
// one that blocks until the clock bound both leave the emergency promotion
// with its full statement sequence, the rotation before the clears.
func TestPromote_RotationNeverStopsTheEmergencyPromotion(t *testing.T) {
	want := []string{"WaitForRelayLogDrain", "StopReplica", "ResetReplicaAll", "GetGtidExecuted", "FlushBinaryLogs", "SetSuperReadOnly(OFF)", "SetReadOnly(OFF)"}
	t.Run("failing", func(t *testing.T) {
		candidate := &trackingMock{readOnly: true, gtidExecuted: "uuid:1-9",
			flushFn: func(context.Context) error { return errors.New("connection refused") }}
		fc, logs := rotationController(clock.NewFakeClock(time.Unix(0, 0)))
		gtid, err := fc.Execute(context.Background(), candidate, nil, "dc2")
		if err != nil || gtid != "uuid:1-9" {
			t.Fatalf("Execute = %q, %v; a failed rotation must not fail the promotion", gtid, err)
		}
		if got := candidate.getCalls(); !slices.Equal(got, want) {
			t.Fatalf("candidate calls = %v, want %v", got, want)
		}
		logLine(t, logs.String(), "promotion binlog rotation failed")
		logLine(t, logs.String(), "failover complete")
	})
	t.Run("slow", func(t *testing.T) {
		clk := clock.NewFakeClock(time.Unix(0, 0))
		candidate := &trackingMock{readOnly: true, gtidExecuted: "uuid:1-9"}
		candidate.flushFn = func(ctx context.Context) error {
			// The flush outlives the bound: time passes, and it answers
			// only once it is abandoned.
			clk.Advance(BinlogRotationTimeout)
			<-ctx.Done()
			return ctx.Err()
		}
		fc, logs := rotationController(clk)
		done := make(chan error, 1)
		go func() {
			_, err := fc.Execute(context.Background(), candidate, nil, "dc2")
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Execute = %v; a slow rotation must not fail the promotion", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a slow rotation blocked the emergency promotion")
		}
		if got := candidate.getCalls(); !slices.Equal(got, want) {
			t.Fatalf("candidate calls = %v, want %v", got, want)
		}
		line := logLine(t, logs.String(), "promotion binlog rotation failed")
		if !strings.Contains(line, "timedOut=true") {
			t.Fatalf("log line %q does not report the timeout", line)
		}
	})
}

// TestPromoteVerifiedFrom_RotatesAtThePromotionGTID: on every stage that
// has not been writable, the pinned promotion rotates the candidate's
// binary log once, after the replication reset and before the gate and the
// clears, while the candidate is read-only and holds exactly the set the
// promotion returns. That is the property #269 relies on: the file the
// rotation opens has a Previous_gtids header equal to the recorded
// promotion GTID. From WasWritable nothing is rotated.
func TestPromoteVerifiedFrom_RotatesAtThePromotionGTID(t *testing.T) {
	F := opsSet(t, opsF)
	cases := []struct {
		from   PromotionStage
		repl   *mysql.ReplicaStatus
		gtid   string
		before []string // statements the candidate received before the flush
	}{
		{PromotionStageNotStarted, &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}, opsF, []string{"stop", "reset"}},
		{PromotionStageReplicaStopped, &mysql.ReplicaStatus{}, opsF, []string{"reset"}},
		{PromotionStageReplicaReset, nil, opsF, nil},
		{PromotionStageWasWritable, nil, opsF + "," + opsUUIDB + ":1", nil},
	}
	for _, tc := range cases {
		t.Run(string(tc.from), func(t *testing.T) {
			m := &opsMySQL{uuid: opsUUIDB, ro: true, sro: true, repl: tc.repl, gtid: tc.gtid}
			gated := 0
			hooks := promoteHooks{gate: func(context.Context) error {
				gated = len(m.flushLog())
				return nil
			}}
			gtid, _, err := NewFailoverController(testLogger()).promoteVerifiedFrom(context.Background(), mysql.CheckerSession(m), m, "dc2", F, tc.from, hooks)
			if err != nil {
				t.Fatal(err)
			}
			flushes := m.flushLog()
			if tc.from == PromotionStageWasWritable {
				if len(flushes) != 0 {
					t.Fatalf("a promotion resumed from WasWritable rotated the binary log: %+v", flushes)
				}
				return
			}
			if len(flushes) != 1 {
				t.Fatalf("flushes = %+v, want exactly one", flushes)
			}
			fl := flushes[0]
			if !slices.Equal(fl.after, tc.before) {
				t.Fatalf("the rotation followed %v, want %v (after the reset, before the clears)", fl.after, tc.before)
			}
			if !fl.ro {
				t.Fatal("the rotation reached a writable candidate")
			}
			if gated != 1 {
				t.Fatalf("the gate ran with %d rotations sent; the rotation must precede the gate", gated)
			}
			got, _ := mysql.ParseGTIDSet(gtid)
			at, _ := mysql.ParseGTIDSet(fl.gtid)
			if !got.Contains(at) || !at.Contains(got) {
				t.Fatalf("the rotation found the set %q, the promotion recorded %q; they must be equal", fl.gtid, gtid)
			}
		})
	}
}

// TestPromoteVerifiedFrom_RotationOutcomeChangesNothing: a failing
// rotation leaves the pinned promotion's statements and result unchanged.
func TestPromoteVerifiedFrom_RotationOutcomeChangesNothing(t *testing.T) {
	F := opsSet(t, opsF)
	m := &opsMySQL{uuid: opsUUIDB, ro: true, sro: true, gtid: opsF,
		repl:     &mysql.ReplicaStatus{IORunning: true, SQLRunning: true},
		failStmt: map[string]error{"flush": errors.New("connection refused")}}
	gtid, stage, err := NewFailoverController(testLogger()).promoteVerified(context.Background(), m, "dc2", F)
	if err != nil || stage != promoteStageWriteAttempted || gtid == "" {
		t.Fatalf("promoteVerified = %q, %v, %v; want the promotion to complete", gtid, stage, err)
	}
	if got, want := m.statements(), []string{"stop", "reset", "sro=OFF", "ro=OFF"}; !slices.Equal(got, want) {
		t.Fatalf("statements = %v, want %v", got, want)
	}
	if fl := m.flushLog(); len(fl) != 1 || fl[0].err == nil {
		t.Fatalf("flushes = %+v, want one failed attempt", fl)
	}
}

// TestPromoteVerifiedFrom_GuardStopsTheRotation: the rotation is guarded
// like a statement, so a pass that lost its claim after the reset sends
// neither the rotation nor anything after it.
func TestPromoteVerifiedFrom_GuardStopsTheRotation(t *testing.T) {
	F := opsSet(t, opsF)
	m := &opsMySQL{uuid: opsUUIDB, ro: true, sro: true, gtid: opsF, repl: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}}
	guards := 0
	hooks := promoteHooks{guard: func() error {
		guards++
		if guards > 2 { // STOP and RESET pass; the claim is lost afterwards
			return errClaimLost
		}
		return nil
	}}
	_, stage, err := NewFailoverController(testLogger()).promoteVerifiedFrom(context.Background(), mysql.CheckerSession(m), m, "dc2", F, PromotionStageNotStarted, hooks)
	if !errors.Is(err, errClaimLost) || stage != promoteStageReplicaReset {
		t.Fatalf("promoteVerifiedFrom = %v, %v; want the guard's error at ReplicaReset", stage, err)
	}
	if fl := m.flushLog(); len(fl) != 0 {
		t.Fatalf("a pass that lost its claim rotated the binary log: %+v", fl)
	}
	if got, want := m.statements(), []string{"stop", "reset"}; !slices.Equal(got, want) {
		t.Fatalf("statements = %v, want %v", got, want)
	}
}

// TestPlannedPromote_RotatesOncePerRun: the planned promotion rotates the
// target once per run and process. A promotion whose gate keeps it at
// ReplicaReset over several passes does not add a file per pass, and a
// re-run of the confirmed promotion rotates nothing.
func TestPlannedPromote_RotatesOncePerRun(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	if _, err := f.topo.ReadTargetServerUUID(context.Background(), f.ref); err != nil {
		t.Fatal(err)
	}
	// The source does not answer at the pre-write gate: the pass ends
	// Unconfirmed after STOP, RESET and the rotation, and is retried.
	f.src.mu.Lock()
	f.src.down = true
	f.src.mu.Unlock()
	for pass := 1; pass <= 2; pass++ {
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteUnconfirmed {
			t.Fatalf("pass %d: Promote = %+v, %v; want Unconfirmed while the source is unreadable", pass, out, err)
		}
		if fl := f.tgt.flushLog(); len(fl) != 1 {
			t.Fatalf("pass %d: target rotations = %d, want 1 for the whole run", pass, len(fl))
		}
	}
	f.src.mu.Lock()
	f.src.down = false
	f.src.mu.Unlock()
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteConfirmed {
		t.Fatalf("Promote = %+v, %v; want Confirmed", out, err)
	}
	fl := f.tgt.flushLog()
	if len(fl) != 1 {
		t.Fatalf("target rotations = %d, want 1", len(fl))
	}
	if !slices.Equal(fl[0].after, []string{"stop", "reset"}) || !fl[0].ro || fl[0].gtid != opsF {
		t.Fatalf("the rotation = %+v; want it after the reset, on a read-only target holding F", fl[0])
	}
	if got, want := f.tgt.statements(), []string{"stop", "reset", "sro=OFF", "ro=OFF"}; !slices.Equal(got, want) {
		t.Fatalf("target statements = %v, want %v", got, want)
	}
	// Confirmed: a re-run finds the target writable and sends nothing.
	if _, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting)); err != nil {
		t.Fatal(err)
	}
	if got := len(f.tgt.flushLog()); got != 1 {
		t.Fatalf("a re-run of the confirmed promotion rotated the target again (%d rotations)", got)
	}
	if got := len(f.src.flushLog()); got != 0 {
		t.Fatalf("Promote rotated the source's binary log %d times; it sends nothing to another site", got)
	}
}

// TestPlannedPromote_ClaimLostBeforeTheRotation: a pass whose claim is
// gone sends nothing, the rotation included.
func TestPlannedPromote_ClaimLostBeforeTheRotation(t *testing.T) {
	f := newOpsFixture(t, "")
	f.fenced()
	// No claim installed: Promote refuses before any statement.
	if _, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting)); err == nil {
		t.Fatal("Promote without the claim returned no error")
	}
	if fl := f.tgt.flushLog(); len(fl) != 0 {
		t.Fatalf("a pass without the claim rotated the target: %+v", fl)
	}
}

// TestPlannedFence_SealsTheSourceOnce: the pass that sends the fence and
// reads it back rotates the fenced source's binary log, once, after the
// fence; a re-proof of the same fence does not rotate again, and a failing
// rotation leaves the fence proof as it was.
func TestPlannedFence_SealsTheSourceOnce(t *testing.T) {
	for _, failing := range []bool{false, true} {
		name := "acknowledged"
		if failing {
			name = "failing"
		}
		t.Run(name, func(t *testing.T) {
			f := newOpsFixture(t, "")
			f.hold(false)
			if failing {
				f.src.failStmt = map[string]error{"flush": errors.New("connection refused")}
			}
			proof, err := f.topo.Fence(context.Background(), f.ref, opsUUIDA)
			if err != nil || proof.Kind != FenceApplied || proof.GtidSet != opsF || proof.Attribution != FenceByMemo {
				t.Fatalf("Fence = %+v, %v; want Applied by memo with F whatever the rotation did", proof, err)
			}
			fl := f.src.flushLog()
			if len(fl) != 1 {
				t.Fatalf("source rotations = %d, want 1", len(fl))
			}
			if !slices.Equal(fl[0].after, []string{"sro=ON"}) || !fl[0].ro || fl[0].gtid != opsF {
				t.Fatalf("the rotation = %+v; want it after the fence, on the read-only source holding F", fl[0])
			}
			if (fl[0].err != nil) != failing {
				t.Fatalf("rotation error = %v, failing = %v", fl[0].err, failing)
			}
			// The same process proves the fence again (a lost Draining
			// write): no second rotation.
			fence := v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceIntended, SourceServerUUID: opsUUIDA}
			if proof, err := f.topo.ProveFence(context.Background(), f.ref, fence); err != nil || proof.Kind != FenceApplied {
				t.Fatalf("ProveFence = %+v, %v", proof, err)
			}
			if proof, err := f.topo.Fence(context.Background(), f.ref, opsUUIDA); err != nil || proof.Kind != FenceApplied {
				t.Fatalf("second Fence = %+v, %v", proof, err)
			}
			if got := len(f.src.flushLog()); got != 1 {
				t.Fatalf("source rotations after re-proofs = %d, want 1", got)
			}
			if got := len(f.tgt.flushLog()); got != 0 {
				t.Fatalf("the fence rotated the target %d times", got)
			}
		})
	}
}

// TestExecuteHandoff_RotatesSourceThenCandidate: a maintenance handoff
// rotates the fenced source's binary log (its writable-era tail) and the
// candidate's (its replica-era history) and completes the same whether the
// rotations are acknowledged or fail.
func TestExecuteHandoff_RotatesSourceThenCandidate(t *testing.T) {
	for _, failing := range []bool{false, true} {
		name := "acknowledged"
		if failing {
			name = "failing"
		}
		t.Run(name, func(t *testing.T) {
			set := handoffUUID + ":1-100"
			src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: set}
			candidate := handoffCandidate(set)
			if failing {
				src.FlushBinaryLogsErr = errors.New("connection refused")
				candidate.OnFlush(func(context.Context) error { return errors.New("connection refused") })
			}
			logs := &syncBuffer{}
			fc := NewFailoverController(slog.New(slog.NewTextHandler(logs, nil)))
			gtid, err := fc.ExecuteHandoff(context.Background(), candidate.Checker(), src, "standby", HandoffOptions{SourceSite: "active"})
			if err != nil || gtid == "" {
				t.Fatalf("ExecuteHandoff = %q, %v; want the handoff to complete", gtid, err)
			}
			if st := candidate.State(); st.ReadOnly || st.SuperReadOnly {
				t.Fatalf("candidate not writable after the handoff: %+v", st)
			}
			// The source: fence, kill, read the fenced set, then rotate.
			calls := src.GetCalls()
			fence, flush := slices.Index(calls, "SetSuperReadOnly(ON)"), slices.Index(calls, "FlushBinaryLogs")
			if fence < 0 || flush < fence || slices.Contains(calls[:flush], "SetSuperReadOnly(OFF)") {
				t.Fatalf("source calls = %v; want one rotation after the fence", calls)
			}
			// The candidate: rotated after the reset, before the clears.
			stmts := candidate.Statements()
			reset, cflush, clear := slices.Index(stmts, "RESET REPLICA ALL"), slices.Index(stmts, "FLUSH BINARY LOGS"), slices.Index(stmts, "SET GLOBAL super_read_only = OFF")
			if reset < 0 || cflush < reset || clear < cflush {
				t.Fatalf("candidate statements = %v; want FLUSH BINARY LOGS between the reset and the clears", stmts)
			}
			if failing {
				line := logLine(t, logs.String(), "outgoing primary binlog rotation failed")
				if !strings.Contains(line, "site=active") {
					t.Fatalf("log line %q does not name the source site", line)
				}
				logLine(t, logs.String(), "promotion binlog rotation failed")
				if got := candidate.State().BinlogPreviousGtids; len(got) != 0 {
					t.Fatalf("a failed rotation rotated the candidate: %v", got)
				}
				return
			}
			logLine(t, logs.String(), "outgoing primary binlog rotated")
			logLine(t, logs.String(), "promotion binlog rotated")
			// The file the candidate's rotation opened starts at the
			// promotion GTID set.
			prev := candidate.State().BinlogPreviousGtids
			if len(prev) != 1 {
				t.Fatalf("candidate rotations = %v, want 1", prev)
			}
			got, _ := mysql.ParseGTIDSet(prev[0])
			want, _ := mysql.ParseGTIDSet(gtid)
			if !got.Contains(want) || !want.Contains(got) {
				t.Fatalf("the rotated file starts at %q, the handoff recorded %q", prev[0], gtid)
			}
		})
	}
}
