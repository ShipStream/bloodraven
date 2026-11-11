package controller

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// hungFlush is a FLUSH BINARY LOGS that never answers and does not honour
// cancellation (a read parked on a blackholed socket): it blocks until the
// test releases it. A second call on the same fake answers at once.
type hungFlush struct {
	once    sync.Once
	entered chan struct{} // closed when the flush starts
	ctxDone chan struct{} // closed when the flush's context ends
	release chan struct{} // closed by the test to let the flush return
	exited  chan struct{} // closed when the flush has returned
}

func newHungFlush() *hungFlush {
	return &hungFlush{entered: make(chan struct{}), ctxDone: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
}

func (h *hungFlush) flush(ctx context.Context) error {
	first := false
	h.once.Do(func() { first = true })
	if !first {
		return nil
	}
	close(h.entered)
	select {
	case <-ctx.Done():
		close(h.ctxDone)
	case <-h.release:
	}
	<-h.release
	close(h.exited)
	return ctx.Err()
}

// rotationWallHold is how long the clock tests leave a caller parked on a
// hung flush with the fake clock unadvanced: well past
// BinlogRotationTimeout of wall time, so a bound taken from the wall clock
// would have released it.
const rotationWallHold = BinlogRotationTimeout + 400*time.Millisecond

// requireBoundOnInjectedClock is the assertion every caller of rotateBinlog
// shares. With the flush hung and the fake clock unadvanced, the caller
// (whose return closes done) stays parked for rotationWallHold of wall time
// and the flush is not cancelled: the bound is not the wall clock's.
// Advancing the fake clock by BinlogRotationTimeout then releases the
// caller while the flush is still blocked; the flush's context is cancelled
// as the caller leaves, and the flush goroutine ends once the fake lets it.
func requireBoundOnInjectedClock(t *testing.T, clk *clock.FakeClock, h *hungFlush, done <-chan struct{}) {
	t.Helper()
	select {
	case <-h.entered:
	case <-done:
		t.Fatal("the caller returned without sending the rotation")
	case <-time.After(10 * time.Second):
		t.Fatal("the rotation was never sent")
	}
	select {
	case <-done:
		t.Fatalf("the caller returned with the fake clock unadvanced: the rotation's bound is not on the injected clock")
	case <-h.ctxDone:
		t.Fatalf("the flush was cancelled with the fake clock unadvanced: the rotation's bound is not on the injected clock")
	case <-time.After(rotationWallHold):
	}
	clk.Advance(BinlogRotationTimeout - time.Millisecond)
	select {
	case <-done:
		t.Fatal("the caller returned before the injected clock reached BinlogRotationTimeout")
	case <-time.After(50 * time.Millisecond):
	}
	clk.Advance(time.Millisecond)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the caller is still blocked after the injected clock passed BinlogRotationTimeout")
	}
	select {
	case <-h.exited:
		t.Fatal("the flush had answered: the caller was not released while it was still blocked")
	default:
	}
	select {
	case <-h.ctxDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the abandoned flush was never cancelled")
	}
	close(h.release)
	select {
	case <-h.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("the flush goroutine did not end after the flush returned")
	}
}

// TestBinlogRotation_BoundIsTheInjectedClock: every caller of rotateBinlog
// (the helper itself, the emergency promotion, the maintenance handoff's
// source and candidate rotations, the planned failover's source seal and
// target rotation) is bounded by the controller's clock and by nothing
// else. Each case hangs the flush, leaves the fake clock alone for longer
// than BinlogRotationTimeout of wall time, and requires the caller to stay
// parked; then one advance of the fake clock releases it and the operation
// completes as if the rotation had failed. A bound taken from the wall
// clock (clock.RealClock, time.After, a context deadline) fails the first
// half; a caller that does not pass the controller's clock fails the second.
//
// The cases run in parallel: each costs rotationWallHold of wall time.
func TestBinlogRotation_BoundIsTheInjectedClock(t *testing.T) {
	newClock := func() *clock.FakeClock { return clock.NewFakeClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)) }

	t.Run("rotateBinlog", func(t *testing.T) {
		t.Parallel()
		clk, h := newClock(), newHungFlush()
		fc, logs := rotationController(clk)
		done := make(chan struct{})
		var ok bool
		go func() {
			defer close(done)
			ok = fc.rotateBinlog(context.Background(), rotationFlusher{fn: h.flush}, "dc2", promotionRotation)
		}()
		requireBoundOnInjectedClock(t, clk, h, done)
		if ok {
			t.Fatal("rotateBinlog = true for a flush that never answered")
		}
		logLine(t, logs.String(), "promotion binlog rotation failed")
	})

	t.Run("emergency promotion", func(t *testing.T) {
		t.Parallel()
		clk, h := newClock(), newHungFlush()
		candidate := &trackingMock{readOnly: true, gtidExecuted: "uuid:1-9", flushFn: h.flush}
		fc, logs := rotationController(clk)
		done := make(chan struct{})
		var gtid string
		var err error
		go func() {
			defer close(done)
			gtid, err = fc.Execute(context.Background(), candidate, nil, "dc2")
		}()
		requireBoundOnInjectedClock(t, clk, h, done)
		if err != nil || gtid != "uuid:1-9" {
			t.Fatalf("Execute = %q, %v; an abandoned rotation must not change the promotion", gtid, err)
		}
		line := logLine(t, logs.String(), "promotion binlog rotation failed")
		if !containsAll(line, "site=dc2", "timedOut=true") {
			t.Fatalf("log line %q; want the timeout on dc2", line)
		}
		logLine(t, logs.String(), "failover complete")
	})

	handoff := func(t *testing.T, hangSource bool) {
		clk, h := newClock(), newHungFlush()
		set := handoffUUID + ":1-100"
		src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: set}
		candidate := handoffCandidate(set)
		if hangSource {
			src.FlushBinaryLogsFn = h.flush
		} else {
			candidate.OnFlush(h.flush)
		}
		fc, logs := rotationController(clk)
		done := make(chan struct{})
		var gtid string
		var err error
		go func() {
			defer close(done)
			gtid, err = fc.ExecuteHandoff(context.Background(), candidate.Checker(), src, "standby", HandoffOptions{SourceSite: "active"})
		}()
		requireBoundOnInjectedClock(t, clk, h, done)
		if err != nil || gtid == "" {
			t.Fatalf("ExecuteHandoff = %q, %v; an abandoned rotation must not change the handoff", gtid, err)
		}
		if st := candidate.State(); st.ReadOnly || st.SuperReadOnly {
			t.Fatalf("candidate not writable after the handoff: %+v", st)
		}
		msg, site := "promotion binlog rotation failed", "site=standby"
		if hangSource {
			msg, site = "outgoing primary binlog rotation failed", "site=active"
		}
		if line := logLine(t, logs.String(), msg); !containsAll(line, site, "timedOut=true") {
			t.Fatalf("log line %q; want the timeout on %s", line, site)
		}
	}
	t.Run("handoff source", func(t *testing.T) {
		t.Parallel()
		handoff(t, true)
	})
	t.Run("handoff candidate", func(t *testing.T) {
		t.Parallel()
		handoff(t, false)
	})

	t.Run("planned source seal", func(t *testing.T) {
		t.Parallel()
		f, h := newOpsFixture(t, ""), newHungFlush()
		fc, logs := rotationController(f.clk)
		f.tm.failover = fc
		f.hold(false)
		f.src.flushFn = h.flush
		done := make(chan struct{})
		var proof FenceProof
		var err error
		go func() {
			defer close(done)
			proof, err = f.topo.Fence(context.Background(), f.ref, opsUUIDA)
		}()
		requireBoundOnInjectedClock(t, f.clk, h, done)
		if err != nil || proof.Kind != FenceApplied || proof.GtidSet != opsF {
			t.Fatalf("Fence = %+v, %v; an abandoned rotation must not change the fence proof", proof, err)
		}
		if line := logLine(t, logs.String(), "outgoing primary binlog rotation failed"); !containsAll(line, "site=dc1", "runId=run-1", "timedOut=true") {
			t.Fatalf("log line %q; want the timeout on dc1 for run-1", line)
		}
	})

	t.Run("planned target", func(t *testing.T) {
		t.Parallel()
		f, h := newOpsFixture(t, ""), newHungFlush()
		fc, logs := rotationController(f.clk)
		f.tm.failover = fc
		f.hold(false)
		f.fenced()
		if _, err := f.topo.ReadTargetServerUUID(context.Background(), f.ref); err != nil {
			t.Fatal(err)
		}
		f.tgt.flushFn = h.flush
		done := make(chan struct{})
		var out PromoteOutcome
		var err error
		go func() {
			defer close(done)
			out, err = f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		}()
		requireBoundOnInjectedClock(t, f.clk, h, done)
		if err != nil || out.Kind != PromoteConfirmed {
			t.Fatalf("Promote = %+v, %v; an abandoned rotation must not change the promotion", out, err)
		}
		if line := logLine(t, logs.String(), "promotion binlog rotation failed"); !containsAll(line, "site=dc2", "timedOut=true") {
			t.Fatalf("log line %q; want the timeout on dc2", line)
		}
	})
}

// containsAll reports whether s holds every one of subs.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

// TestRotateBinlog_CancelledCallerWhoseFlushIgnoresCancellation: the
// caller's own cancellation ends the wait even when the flush does not
// honour its context. The fake clock is never advanced, so only the
// caller's context can have released it. The abandoned flush goroutine is
// left parked in the driver, not in rotateBinlog: once the flush returns it
// delivers its result to the buffered channel and ends.
func TestRotateBinlog_CancelledCallerWhoseFlushIgnoresCancellation(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	fc, logs := rotationController(clk)
	h := newHungFlush()
	// The flush runs on a goroutine rotateBinlog started; its header line
	// ("goroutine 123 [") identifies it in a later stack dump.
	flushGoroutine := make(chan string, 1)
	flusher := rotationFlusher{fn: func(ctx context.Context) error {
		buf := make([]byte, 64)
		buf = buf[:runtime.Stack(buf, false)]
		if i := bytes.IndexByte(buf, '['); i > 0 {
			flushGoroutine <- string(buf[:i+1])
		} else {
			flushGoroutine <- ""
		}
		return h.flush(ctx)
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan bool, 1)
	go func() { returned <- fc.rotateBinlog(ctx, flusher, "dc2", promotionRotation) }()
	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the rotation was never sent")
	}
	select {
	case <-returned:
		t.Fatal("rotateBinlog returned before its caller was cancelled or the clock moved")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case ok := <-returned:
		if ok {
			t.Fatal("rotateBinlog = true after its caller was cancelled")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled caller stayed blocked on a flush that ignores cancellation")
	}
	select {
	case <-h.exited:
		t.Fatal("the flush had answered: the caller was not released by its own cancellation")
	default:
	}
	line := logLine(t, logs.String(), "promotion binlog rotation failed")
	if !containsAll(line, "level=WARN", "site=dc2", "timedOut=false", "context canceled") {
		t.Fatalf("log line %q; want the caller's cancellation, not a timeout", line)
	}
	// The flush returns at last. Its goroutine must end then: the result
	// channel is buffered, so the send nobody reads cannot park it.
	header := <-flushGoroutine
	if !strings.HasPrefix(header, "goroutine ") {
		t.Fatalf("could not identify the flush goroutine (%q)", header)
	}
	alive := func() bool {
		buf := make([]byte, 1<<20)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				return bytes.Contains(buf[:n], []byte(header))
			}
			buf = make([]byte, 2*len(buf))
		}
	}
	if !alive() {
		t.Fatal("the flush goroutine is gone while the flush is still blocked; the check below would prove nothing")
	}
	close(h.release)
	deadline := time.Now().Add(5 * time.Second)
	for alive() {
		if time.Now().After(deadline) {
			t.Fatal("the flush goroutine outlived the flush: it is parked on a result nobody reads")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
