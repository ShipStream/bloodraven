package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
	"github.com/shipstream/bloodraven/internal/testutil"
)

const (
	catchUpTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	catchUpFenced   = catchUpTestUUID + ":1-100"
	catchUpBehind   = catchUpTestUUID + ":1-90"
)

// catchUpOpts keeps the gate's budgets short; the tests run on the wall
// clock through the real mysql.Checker.
func catchUpOpts() FailoverOptions {
	return FailoverOptions{
		OldPrimarySite:       "dc1",
		CatchUp:              150 * time.Millisecond,
		PollInterval:         time.Millisecond,
		OldPrimaryLossWindow: 20 * time.Millisecond,
	}
}

// catchUpOldPrimary is a primary holding catchUpFenced, writable unless
// fenced.
func catchUpOldPrimary(fenced bool) *testutil.FakeReplica {
	return testutil.NewFakeReplica(testutil.FakeReplicaState{
		ExecutedGtidSet: catchUpFenced, ReadOnly: fenced, SuperReadOnly: fenced,
	})
}

// catchUpCandidate is a read-only replica of the old primary that has
// executed executed and keeps replicating.
func catchUpCandidate(executed string) *testutil.FakeReplica {
	return testutil.NewFakeReplica(testutil.FakeReplicaState{
		Configured: true, IORunning: true, SQLRunning: true,
		SecondsBehindSource: int64Ptr(0), SourceHost: "dc1",
		RetrievedGtidSet: executed, ExecutedGtidSet: executed,
		ReadOnly: true, SuperReadOnly: true,
	})
}

func assertCandidateUntouched(t *testing.T, c *testutil.FakeReplica) {
	t.Helper()
	for _, stmt := range c.Statements() {
		if strings.HasPrefix(stmt, "STOP REPLICA") || stmt == "RESET REPLICA ALL" || strings.HasPrefix(stmt, "SET GLOBAL") {
			t.Fatalf("refused failover sent %q to the candidate; statements: %v", stmt, c.Statements())
		}
	}
	if st := c.State(); !st.ReadOnly || !st.SuperReadOnly {
		t.Fatalf("candidate writable after a refusal: read_only=%v super_read_only=%v", st.ReadOnly, st.SuperReadOnly)
	}
}

func runCatchUp(t *testing.T, oldPrimary, candidate mysql.Checker, opts FailoverOptions) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return NewFailoverController(testutil.TestLogger()).ExecuteWithOptions(ctx, candidate, oldPrimary, "dc2", opts)
}

func requireRefusal(t *testing.T, err error, outcome string) *CandidateNotCaughtUpError {
	t.Helper()
	var refusal *CandidateNotCaughtUpError
	if !errors.As(err, &refusal) {
		t.Fatalf("error = %v, want *CandidateNotCaughtUpError", err)
	}
	if refusal.OldPrimary != outcome {
		t.Fatalf("old primary outcome = %q, want %q (error: %v)", refusal.OldPrimary, outcome, err)
	}
	return refusal
}

func TestEmergencyCatchUp_BehindCandidateRefusedAndOldPrimaryUnfenced(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpBehind)

	_, err := runCatchUp(t, old.Checker(), cand.Checker(), catchUpOpts())

	refusal := requireRefusal(t, err, OldPrimaryUnfenced)
	if refusal.MissingTransactions != 10 || !strings.Contains(refusal.Reason, "timed out") {
		t.Fatalf("refusal = %+v, want 10 missing after a timeout", refusal)
	}
	assertCandidateUntouched(t, cand)
	if st := old.State(); st.ReadOnly || st.SuperReadOnly {
		t.Fatalf("old primary still fenced: read_only=%v super_read_only=%v", st.ReadOnly, st.SuperReadOnly)
	}
	if !slices.Contains(old.Statements(), "SET GLOBAL super_read_only = ON") {
		t.Fatal("old primary was never fenced")
	}
}

func TestEmergencyCatchUp_CandidateCatchesUpAndIsPromoted(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpBehind)
	cand.OnPoll(func(poll int, s *testutil.FakeReplicaState) {
		if poll >= 3 {
			s.RetrievedGtidSet, s.ExecutedGtidSet = catchUpFenced, catchUpFenced
		}
	})
	opts := catchUpOpts()
	opts.CatchUp = 5 * time.Second

	gtid, err := runCatchUp(t, old.Checker(), cand.Checker(), opts)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gtid != catchUpFenced {
		t.Fatalf("promotion GTID = %q, want %q", gtid, catchUpFenced)
	}
	if st := cand.State(); st.ReadOnly || st.SuperReadOnly {
		t.Fatal("candidate not promoted")
	}
	if st := old.State(); !st.SuperReadOnly {
		t.Fatal("old primary not fenced at promotion")
	}
}

func TestEmergencyCatchUp_SelfFencedOldPrimaryStaysFenced(t *testing.T) {
	old := catchUpOldPrimary(true)
	cand := catchUpCandidate(catchUpBehind)

	_, err := runCatchUp(t, old.Checker(), cand.Checker(), catchUpOpts())

	requireRefusal(t, err, OldPrimaryLeftFenced)
	assertCandidateUntouched(t, cand)
	if st := old.State(); !st.ReadOnly || !st.SuperReadOnly {
		t.Fatalf("self-fenced old primary made writable: read_only=%v super_read_only=%v", st.ReadOnly, st.SuperReadOnly)
	}
	for _, stmt := range old.Statements() {
		if stmt == "SET GLOBAL super_read_only = OFF" || stmt == "SET GLOBAL read_only = OFF" {
			t.Fatalf("refusal sent %q to a self-fenced old primary", stmt)
		}
	}
}

func TestEmergencyCatchUp_UnreachableOldPrimaryIsNotWaitedFor(t *testing.T) {
	down := errors.New("dial tcp: connection refused")
	old := &trackingMock{fenceErr: down, checkErr: down}
	cand := catchUpCandidate(catchUpBehind)
	opts := catchUpOpts()
	opts.CatchUp = time.Hour

	start := time.Now()
	gtid, err := runCatchUp(t, old, cand.Checker(), opts)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gtid != catchUpBehind {
		t.Fatalf("promotion GTID = %q, want what the candidate had (%q)", gtid, catchUpBehind)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("emergency failover waited for an unreachable old primary")
	}
	if got := old.getCalls(); slices.Contains(got, "KillAppConnections") || slices.Contains(got, "GetGtidExecuted") {
		t.Fatalf("old primary calls %v: an unfenced old primary gets neither the kill nor the catch-up", got)
	}
}

func TestEmergencyCatchUp_SplitBrainWinnerIsNotGated(t *testing.T) {
	old := catchUpOldPrimary(false)
	// The winner took writes of its own; it can never contain the loser.
	winner := testutil.NewFakeReplica(testutil.FakeReplicaState{
		ExecutedGtidSet: catchUpBehind + ",bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-3",
	})
	opts := catchUpOpts()
	opts.CatchUp = time.Hour
	opts.AllowDivergentOldPrimary = true

	if _, err := runCatchUp(t, old.Checker(), winner.Checker(), opts); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if st := old.State(); !st.SuperReadOnly {
		t.Fatal("split-brain loser not fenced")
	}
	if st := winner.State(); st.ReadOnly {
		t.Fatal("split-brain winner not writable")
	}
}

func TestEmergencyCatchUp_ApplierErrorEndsTheWaitEarly(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpBehind)
	cand.Mutate(func(s *testutil.FakeReplicaState) {
		s.SQLRunning, s.LastSQLErrno, s.LastSQLError = false, 1062, "Duplicate entry '7' for key 'PRIMARY'"
	})
	opts := catchUpOpts()
	opts.CatchUp = time.Hour

	start := time.Now()
	_, err := runCatchUp(t, old.Checker(), cand.Checker(), opts)
	refusal := requireRefusal(t, err, OldPrimaryUnfenced)
	if !strings.Contains(refusal.Reason, "applier stopped") || !strings.Contains(refusal.Reason, "1062") {
		t.Fatalf("reason = %q, want the applier error", refusal.Reason)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("an applier error did not end the wait early")
	}
	assertCandidateUntouched(t, cand)
}

func TestEmergencyCatchUp_StoppedReceiverShortOfTheSetEndsTheWaitEarly(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpBehind)
	cand.Mutate(func(s *testutil.FakeReplicaState) {
		s.IORunning, s.LastIOErrno = false, 13114
		s.LastIOError = "Got fatal error 1236 from source when reading data from binary log: 'Cannot replicate because the source purged required binary logs.'"
	})
	opts := catchUpOpts()
	opts.CatchUp = time.Hour

	start := time.Now()
	_, err := runCatchUp(t, old.Checker(), cand.Checker(), opts)
	refusal := requireRefusal(t, err, OldPrimaryUnfenced)
	if !strings.Contains(refusal.Reason, "receiver is stopped") {
		t.Fatalf("reason = %q, want the stopped receiver", refusal.Reason)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("a stopped receiver did not end the wait early")
	}
}

// A stopped receiver that already received the whole set is not a reason
// to give up: the applier is still working through the relay log.
func TestEmergencyCatchUp_StoppedReceiverWithTheSetReceivedKeepsWaiting(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpBehind)
	cand.Mutate(func(s *testutil.FakeReplicaState) {
		s.IORunning, s.RetrievedGtidSet = false, catchUpFenced
	})
	cand.OnPoll(func(poll int, s *testutil.FakeReplicaState) {
		if poll >= 3 {
			s.ExecutedGtidSet = catchUpFenced
		}
	})
	opts := catchUpOpts()
	opts.CatchUp = 5 * time.Second

	if _, err := runCatchUp(t, old.Checker(), cand.Checker(), opts); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

// diesAfterFence is an old primary that answers the fence and the read of
// its fenced set, then stops answering.
type diesAfterFence struct {
	mysql.Checker
	dead atomic.Bool
}

func (d *diesAfterFence) GetGtidExecuted(ctx context.Context) (string, error) {
	g, err := d.Checker.GetGtidExecuted(ctx)
	d.dead.Store(true)
	return g, err
}

func (d *diesAfterFence) CheckReadOnly(ctx context.Context) (bool, error) {
	if d.dead.Load() {
		return false, errors.New("dial tcp: i/o timeout")
	}
	return d.Checker.CheckReadOnly(ctx)
}

// An old primary that dies during the wait can send nothing more: the
// failover goes ahead with what the candidate has, as when the fence fails.
func TestEmergencyCatchUp_OldPrimaryLostDuringTheWaitPromotes(t *testing.T) {
	old := &diesAfterFence{Checker: catchUpOldPrimary(false).Checker()}
	cand := catchUpCandidate(catchUpBehind)
	opts := catchUpOpts()
	opts.CatchUp = time.Hour

	gtid, err := runCatchUp(t, old, cand.Checker(), opts)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gtid != catchUpBehind {
		t.Fatalf("promotion GTID = %q, want %q", gtid, catchUpBehind)
	}
}

// failingFence answers probes but rejects SET GLOBAL super_read_only = ON.
type failingFence struct {
	mysql.Checker
}

func (f failingFence) SetSuperReadOnly(ctx context.Context, on bool) error {
	if on {
		return errors.New("Error 1205: Lock wait timeout exceeded")
	}
	return f.Checker.SetSuperReadOnly(ctx, on)
}

// A fence that fails on an old primary that still answers writable: it is
// alive and serving, so promoting the candidate would make two primaries.
func TestEmergencyCatchUp_FailedFenceOnWritableOldPrimaryRefuses(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpFenced)

	_, err := runCatchUp(t, failingFence{old.Checker()}, cand.Checker(), catchUpOpts())
	var notFenced *OldPrimaryNotFencedError
	if !errors.As(err, &notFenced) {
		t.Fatalf("error = %v, want *OldPrimaryNotFencedError", err)
	}
	assertCandidateUntouched(t, cand)
}

// A fence that returned an error on an old primary that now answers
// read-only (the statement applied, or it was already fenced) is a fence:
// the candidate must catch up with it.
func TestEmergencyCatchUp_FailedFenceOnReadOnlyOldPrimaryIsGated(t *testing.T) {
	old := catchUpOldPrimary(true)
	cand := catchUpCandidate(catchUpBehind)

	_, err := runCatchUp(t, failingFence{old.Checker()}, cand.Checker(), catchUpOpts())
	requireRefusal(t, err, OldPrimaryLeftFenced)
	assertCandidateUntouched(t, cand)
}

// An old primary that restarted during the wait is read-only by its own
// start: the refusal does not lift that.
func TestEmergencyCatchUp_OldPrimaryRestartedDuringTheWaitIsNotUnfenced(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpBehind)
	cand.OnPoll(func(poll int, _ *testutil.FakeReplicaState) {
		if poll == 2 {
			old.Mutate(func(s *testutil.FakeReplicaState) { s.ReadOnly, s.SuperReadOnly = true, false })
		}
	})

	_, err := runCatchUp(t, old.Checker(), cand.Checker(), catchUpOpts())
	requireRefusal(t, err, OldPrimaryNotReleased)
	if st := old.State(); !st.ReadOnly {
		t.Fatal("restarted old primary made writable")
	}
}

func TestEmergencyCatchUp_CancelledWaitUnfencesTheOldPrimary(t *testing.T) {
	old := catchUpOldPrimary(false)
	cand := catchUpCandidate(catchUpBehind)
	ctx, cancel := context.WithCancel(context.Background())
	cand.OnPoll(func(poll int, _ *testutil.FakeReplicaState) {
		if poll == 2 {
			cancel()
		}
	})
	opts := catchUpOpts()
	opts.CatchUp = time.Hour

	_, err := NewFailoverController(testutil.TestLogger()).ExecuteWithOptions(ctx, cand.Checker(), old.Checker(), "dc2", opts)
	refusal := requireRefusal(t, err, OldPrimaryUnfenced)
	if !strings.Contains(refusal.Reason, "cancelled") {
		t.Fatalf("reason = %q, want cancelled", refusal.Reason)
	}
	if st := old.State(); st.ReadOnly || st.SuperReadOnly {
		t.Fatal("old primary left fenced after a cancelled wait")
	}
}

// Tagged GTID sets (MySQL 8.4): the comparison is per uuid and tag.
func TestEmergencyCatchUp_TaggedGTIDSets(t *testing.T) {
	old := testutil.NewFakeReplica(testutil.FakeReplicaState{
		ExecutedGtidSet: catchUpTestUUID + ":1-90:app:1-5",
	})
	cand := catchUpCandidate(catchUpTestUUID + ":1-90")

	_, err := runCatchUp(t, old.Checker(), cand.Checker(), catchUpOpts())
	refusal := requireRefusal(t, err, OldPrimaryUnfenced)
	if refusal.MissingTransactions != 5 {
		t.Fatalf("missing = %d, want the 5 tagged transactions", refusal.MissingTransactions)
	}
}

func TestCatchUpRefusalBackoff(t *testing.T) {
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 2 * time.Minute, 2 * time.Minute}
	for i, w := range want {
		if got := catchUpRefusalBackoff(i + 1); got != w {
			t.Errorf("backoff after refusal %d = %s, want %s", i+1, got, w)
		}
	}
}

// A group that has never failed over has no failover target: the site the
// operator last saw as its primary is the old primary to fence (and to
// catch up with).
func TestPreviousPrimaryFallsBackToLastActiveSite(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	tm.mu.Lock()
	tm.sites[0].state = state.StateUnreachable
	tm.lastActiveSite = "dc1"
	tm.mu.Unlock()
	if got := tm.previousPrimary("dc2"); got != "dc1" {
		t.Fatalf("previousPrimary = %q, want dc1 (lastActiveSite)", got)
	}
	if got := tm.previousPrimary("dc1"); got != "" {
		t.Fatalf("previousPrimary(dc1) = %q, want none: the candidate is not its own old primary", got)
	}
}

// Consecutive refusals of one pair back off further; a refusal long after
// the last one, or of another pair, starts again at 10s. Each refusal is a
// new record for the runner's Event.
func TestRecordCatchUpRefusal_BacksOffAndRecords(t *testing.T) {
	clk := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(&mockMySQL{}, &mockMySQL{readOnly: true}, clk)
	refusal := &CandidateNotCaughtUpError{Candidate: "dc2", OldPrimarySite: "dc1", MissingTransactions: 10,
		Waited: 30 * time.Second, Reason: "timed out", OldPrimary: OldPrimaryUnfenced, Err: errors.New("timed out")}

	retryAfter := func() time.Duration {
		tm.mu.RLock()
		defer tm.mu.RUnlock()
		return tm.catchUpRetryAt.Sub(clk.Now())
	}
	tm.recordCatchUpRefusal(refusal)
	if got := retryAfter(); got != 10*time.Second {
		t.Fatalf("first backoff = %s, want 10s", got)
	}
	tm.recordCatchUpRefusal(refusal)
	if got := retryAfter(); got != 20*time.Second {
		t.Fatalf("second backoff = %s, want 20s", got)
	}
	snap := tm.buildSnapshot(nil)
	if r := snap.PromotionCatchUpRefusal; r == nil || r.Seq != 2 || r.Candidate != "dc2" || r.OldPrimary != "dc1" ||
		r.MissingTransactions != 10 || r.OldPrimaryOutcome != OldPrimaryUnfenced {
		t.Fatalf("snapshot refusal = %+v, want the second refusal of dc2 over dc1", r)
	}

	clk.Advance(catchUpRefusalResetAfter)
	tm.recordCatchUpRefusal(refusal)
	if got := retryAfter(); got != 10*time.Second {
		t.Fatalf("backoff after a quiet period = %s, want 10s again", got)
	}
	other := *refusal
	other.OldPrimarySite = "dc3"
	tm.recordCatchUpRefusal(&other)
	if got := retryAfter(); got != 10*time.Second {
		t.Fatalf("backoff for another pair = %s, want 10s", got)
	}

	tm.resetCatchUpRefusals()
	if tm.deferAfterCatchUpRefusal(context.Background(), "dc2", "dc3", &mockMySQL{}, false) {
		t.Fatal("deferred after the backoff was reset by a promotion")
	}
}

// Within the backoff, a decision is deferred while the old primary answers
// and goes ahead once it does not; a split-brain winner is never deferred.
func TestDeferAfterCatchUpRefusal(t *testing.T) {
	clk := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(&mockMySQL{}, &mockMySQL{readOnly: true}, clk)
	tm.recordCatchUpRefusal(&CandidateNotCaughtUpError{Candidate: "dc2", OldPrimarySite: "dc1", Err: errors.New("timed out")})
	ctx := context.Background()

	answering := &mockMySQL{}
	if !tm.deferAfterCatchUpRefusal(ctx, "dc2", "dc1", answering, false) {
		t.Fatal("not deferred while the old primary answers within the backoff")
	}
	if tm.deferAfterCatchUpRefusal(ctx, "dc2", "dc1", answering, true) {
		t.Fatal("split-brain winner deferred")
	}
	if tm.deferAfterCatchUpRefusal(ctx, "dc2", "dc1", &mockMySQL{err: errors.New("connection refused")}, false) {
		t.Fatal("deferred although the old primary stopped answering")
	}
	if tm.deferAfterCatchUpRefusal(ctx, "dc1", "dc2", answering, false) {
		t.Fatal("deferred another pair")
	}
	clk.Advance(failoverRetryBackoff)
	if tm.deferAfterCatchUpRefusal(ctx, "dc2", "dc1", answering, false) {
		t.Fatal("deferred after the backoff ended")
	}
}

// The runner emits one PromotionRefusedCandidateBehind Event per refusal.
func TestRunnerEmitsCatchUpRefusalEventOncePerRefusal(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	fg := newTestFG()
	recorder := record.NewFakeRecorder(10)
	runner := &TopologyManagerRunner{logger: testLogger(), recorder: recorder, managers: make(map[types.NamespacedName]*managedTopology)}
	runner.SetManagerForTest(types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, tm)

	snap := TopologySnapshot{PromotionCatchUpRefusal: &PromotionCatchUpRefusal{
		Seq: 1, Candidate: "dc2", OldPrimary: "dc1", MissingTransactions: 10, Waited: 30 * time.Second,
		Reason: "timed out after 30s with 10 transactions missing", OldPrimaryOutcome: OldPrimaryUnfenced,
		RetryAt: time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC),
	}}
	runner.emitKeyringPromotionEvents(fg, snap)
	runner.emitKeyringPromotionEvents(fg, snap)
	events := drainEvents(recorder)
	if len(events) != 1 || !strings.Contains(events[0], "Warning PromotionRefusedCandidateBehind emergency failover to dc2 refused") ||
		!strings.Contains(events[0], "10 transactions missing") || !strings.Contains(events[0], "unfenced and serving writes again") {
		t.Fatalf("events = %q, want one PromotionRefusedCandidateBehind", events)
	}
	next := *snap.PromotionCatchUpRefusal
	next.Seq = 2
	runner.emitKeyringPromotionEvents(fg, TopologySnapshot{PromotionCatchUpRefusal: &next})
	if events := drainEvents(recorder); len(events) != 1 {
		t.Fatalf("events after the next refusal = %q, want one more", events)
	}
}
