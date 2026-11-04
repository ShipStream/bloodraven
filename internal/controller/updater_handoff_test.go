package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

const handoffUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// fencedGTIDSource is a healthy active primary whose gtid_executed is
// beforeFence until super_read_only=ON is set and afterFence from then on:
// transactions committed between the operator's first look and the fence.
type fencedGTIDSource struct {
	*testutil.FakeMySQL
	beforeFence, afterFence string
	// afterFenceErr fails the post-fence GTID read.
	afterFenceErr error
}

func (s *fencedGTIDSource) fenced() bool {
	return slices.Contains(s.GetCalls(), "SetSuperReadOnly(ON)")
}

func (s *fencedGTIDSource) GetGtidExecuted(ctx context.Context) (string, error) {
	fenced := s.fenced()
	if _, err := s.FakeMySQL.GetGtidExecuted(ctx); err != nil {
		return "", err
	}
	if fenced {
		return s.afterFence, s.afterFenceErr
	}
	return s.beforeFence, nil
}

// handoffCandidate is a direct, healthy, read-only replica of
// primary-internal served through the real mysql.Checker.
func handoffCandidate(executed string) *testutil.FakeReplica {
	return testutil.NewFakeReplica(testutil.FakeReplicaState{
		Configured: true, IORunning: true, SQLRunning: true,
		SecondsBehindSource: int64Ptr(0),
		SourceHost:          "primary-internal",
		RetrievedGtidSet:    executed, ExecutedGtidSet: executed,
		ReadOnly: true, SuperReadOnly: true,
	})
}

type handoffRun struct {
	processed []string
	err       error
	promoted  string
	gtid      string
	applied   []string
}

func runHandoff(t *testing.T, source mysql.Checker, candidate mysql.Checker) handoffRun {
	t.Helper()
	return runHandoffWith(t, source, candidate, HandoffOptions{
		PreFenceCatchUp: 100 * time.Millisecond,
		CatchUp:         100 * time.Millisecond,
		PollInterval:    time.Millisecond,
	}, defaultActivePrimaryLossWindow)
}

// runHandoffWith is runHandoff with explicit handoff budgets and the
// UpdateController's primary-loss window, which the handoff uses to watch
// the source.
func runHandoffWith(t *testing.T, source mysql.Checker, candidate mysql.Checker, opts HandoffOptions, lossWindow time.Duration) handoffRun {
	t.Helper()
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = time.Millisecond
	uc.primaryLossWindow = lossWindow
	uc.handoff = opts
	// Bounds the old-active wait that follows a (wrong) promotion so a
	// regression fails instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var run handoffRun
	run.processed, run.err = uc.ExecuteTargets(ctx,
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: source, Drifted: true},
		[]UpdateTarget{{
			Name: "standby", Host: "standby-internal", Checker: candidate,
			Promotable: true, ExpectedSource: "primary-internal",
		}},
		func(_ context.Context, name string) error {
			run.applied = append(run.applied, name)
			return nil
		},
		func(site, gtid string) {
			run.promoted, run.gtid = site, gtid
		})
	return run
}

func candidatePromotionStatements(statements []string) []string {
	var out []string
	for _, s := range statements {
		if s == "RESET REPLICA ALL" || s == "STOP REPLICA" || s == "SET GLOBAL read_only = OFF" || s == "SET GLOBAL super_read_only = OFF" {
			out = append(out, s)
		}
	}
	return out
}

// roleCalls keeps the read-only and replication-role calls from a FakeMySQL
// call log, dropping the probes.
func roleCalls(calls []string) []string {
	var out []string
	for _, c := range calls {
		if strings.HasPrefix(c, "SetSuperReadOnly") || strings.HasPrefix(c, "SetReadOnly") ||
			c == "StopReplica" || c == "ResetReplicaAll" || c == "ChangeReplicationSource" || c == "StartReplica" {
			out = append(out, c)
		}
	}
	return out
}

// assertHandoffAborted checks that nothing moved: the candidate is still a
// read-only replica, no authority callback ran, the active site keeps its
// drift and its Deployment, and the source was handed back its writes.
func assertHandoffAborted(t *testing.T, run handoffRun, source *testutil.FakeMySQL, candidate *testutil.FakeReplica, wantFenced bool) {
	t.Helper()
	if run.err == nil {
		t.Fatal("ExecuteTargets() = nil error, want the handoff refused")
	}
	if got := candidatePromotionStatements(candidate.Statements()); len(got) != 0 {
		t.Errorf("candidate received promotion statements %v before the catch-up gate passed", got)
	}
	if st := candidate.State(); !st.ReadOnly || !st.Configured {
		t.Errorf("candidate state read_only=%v configured=%v, want a read-only configured replica", st.ReadOnly, st.Configured)
	}
	if run.promoted != "" {
		t.Errorf("authority callback ran for %q", run.promoted)
	}
	if slices.Contains(run.applied, "primary") {
		t.Errorf("active Deployment was updated: applied=%v", run.applied)
	}
	if slices.Contains(run.processed, "primary") {
		t.Errorf("active drift was cleared: processed=%v", run.processed)
	}
	calls := roleCalls(source.GetCalls())
	fenced := slices.Contains(calls, "SetSuperReadOnly(ON)")
	if fenced != wantFenced {
		t.Errorf("source fenced = %v, want %v (role calls %v)", fenced, wantFenced, calls)
	}
	if fenced {
		if !slices.Contains(calls, "SetSuperReadOnly(OFF)") || !slices.Contains(calls, "SetReadOnly(OFF)") {
			t.Errorf("source not fully unfenced after abort: role calls %v", calls)
		}
		if source.IsReadOnlyVal() {
			t.Error("source left read-only after abort")
		}
	}
}

// The receiver has applied everything it fetched (u:1-90), but the source
// committed u:91-100 before the fence. The drain-based emergency path calls
// that drained and promotes without 91-100; the maintenance handoff must not.
func TestExecuteTargets_HandoffWaitsForFencedSourceGTIDs(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-90", afterFence: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-90")

	run := runHandoff(t, source, candidate.Checker())

	assertHandoffAborted(t, run, src, candidate, true)
	if run.err != nil && !strings.Contains(run.err.Error(), "did not reach the fenced source GTID set") {
		t.Errorf("error = %v, want the fenced catch-up timeout", run.err)
	}
}

// Once the missing transactions arrive, the handoff promotes, and the
// promotion GTID contains everything the source committed.
func TestExecuteTargets_HandoffPromotesAfterFencedCatchUp(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-90", afterFence: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-90")
	// The receiver fetches 91-100 a poll after the fence: the first look
	// after fencing still shows everything received as applied.
	fencedPolls := 0
	candidate.OnPoll(func(_ int, s *testutil.FakeReplicaState) {
		if !source.fenced() || !s.Configured {
			return
		}
		fencedPolls++
		if fencedPolls >= 2 {
			s.RetrievedGtidSet = handoffUUID + ":1-100"
			s.ExecutedGtidSet = handoffUUID + ":1-100"
		}
	})
	src.ReplicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "standby-internal"}

	run := runHandoff(t, source, candidate.Checker())

	if run.err != nil {
		t.Fatalf("ExecuteTargets() error = %v", run.err)
	}
	if run.promoted != "standby" {
		t.Fatalf("authority callback site = %q, want standby", run.promoted)
	}
	got, err := mysql.ParseGTIDSet(run.gtid)
	if err != nil {
		t.Fatalf("parse promotion GTID %q: %v", run.gtid, err)
	}
	want, _ := mysql.ParseGTIDSet(handoffUUID + ":1-100")
	if !got.Contains(want) {
		t.Fatalf("promotion GTID %q is missing source transactions %s", run.gtid, want.Subtract(got))
	}
	if st := candidate.State(); st.ReadOnly || st.Discarded != "" {
		t.Errorf("candidate read_only=%v discarded=%q, want writable with nothing discarded", st.ReadOnly, st.Discarded)
	}
	if got := strings.Join(run.processed, ","); got != "primary" {
		t.Errorf("processed = %q, want primary", got)
	}
}

// A candidate that cannot reach the source's current set while the source is
// still writable is refused before the source is fenced: no write pause.
func TestExecuteTargets_HandoffRefusesLaggingCandidateWithoutFencing(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-90")

	run := runHandoff(t, src, candidate.Checker())

	assertHandoffAborted(t, run, src, candidate, false)
}

// The source must be fenced before anything is promoted; a failed fence
// (the source may still accept writes) refuses the handoff.
func TestExecuteTargets_HandoffRefusesWhenSourceFenceFails(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100",
		FenceErr: errors.New("Error 2013: Lost connection to MySQL server during query")}
	candidate := handoffCandidate(handoffUUID + ":1-100")

	run := runHandoff(t, src, candidate.Checker())

	if run.err == nil || !strings.Contains(run.err.Error(), "fence source") {
		t.Fatalf("ExecuteTargets() error = %v, want the fence failure", run.err)
	}
	if got := candidatePromotionStatements(candidate.Statements()); len(got) != 0 {
		t.Errorf("candidate received promotion statements %v after a failed fence", got)
	}
	if run.promoted != "" || slices.Contains(run.applied, "primary") {
		t.Errorf("handoff proceeded after a failed fence: promoted=%q applied=%v", run.promoted, run.applied)
	}
	// The fence may have applied before the connection dropped, so the
	// rollback still tries to clear both flags.
	if calls := roleCalls(src.GetCalls()); !slices.Contains(calls, "SetReadOnly(OFF)") {
		t.Errorf("source rollback did not clear read_only: role calls %v", calls)
	}
}

// Without the fenced GTID set there is nothing to prove catch-up against.
func TestExecuteTargets_HandoffRefusesWhenFencedGTIDReadFails(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-100", afterFenceErr: errors.New("Error 2013: Lost connection")}
	candidate := handoffCandidate(handoffUUID + ":1-100")

	run := runHandoff(t, source, candidate.Checker())

	assertHandoffAborted(t, run, src, candidate, true)
}

// A malformed fenced GTID set cannot prove containment either.
func TestExecuteTargets_HandoffRefusesUnparseableFencedGTID(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-100", afterFence: handoffUUID + ":garbage-"}
	candidate := handoffCandidate(handoffUUID + ":1-100")

	run := runHandoff(t, source, candidate.Checker())

	assertHandoffAborted(t, run, src, candidate, true)
}

// An applier error means the candidate will not catch up; the handoff stops
// on it instead of waiting out the budget.
func TestExecuteTargets_HandoffAbortsOnCandidateApplierError(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-90", afterFence: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-90")
	candidate.OnPoll(func(_ int, s *testutil.FakeReplicaState) {
		if source.fenced() {
			s.SQLRunning = false
			s.LastSQLErrno, s.LastSQLError = 1062, "Error 'Duplicate entry' on query"
		}
	})

	run := runHandoff(t, source, candidate.Checker())

	assertHandoffAborted(t, run, src, candidate, true)
	if run.err != nil && !strings.Contains(run.err.Error(), "1062") {
		t.Errorf("error = %v, want the applier error", run.err)
	}
}

// --- review round 1 ------------------------------------------------------

func assertCandidateNotWritable(t *testing.T, candidate *testutil.FakeReplica) {
	t.Helper()
	for _, s := range candidate.Statements() {
		if s == "SET GLOBAL read_only = OFF" {
			t.Fatal("candidate was made writable")
		}
	}
	if candidate.State().ReadOnly == false {
		t.Fatal("candidate is writable")
	}
}

func assertSourceUnfenced(t *testing.T, source *testutil.FakeMySQL) {
	t.Helper()
	calls := roleCalls(source.GetCalls())
	if !slices.Contains(calls, "SetSuperReadOnly(OFF)") || !slices.Contains(calls, "SetReadOnly(OFF)") || source.IsReadOnlyVal() {
		t.Fatalf("source not unfenced: role calls %v", calls)
	}
}

// The promotion runs on one pinned connection. A candidate pod replaced
// right after STOP REPLICA breaks that connection: the handoff refuses
// before RESET REPLICA ALL instead of resetting or promoting whichever
// server the address now reaches.
func TestExecuteTargets_HandoffRefusesCandidateReplacedAfterStop(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	original := handoffCandidate(handoffUUID + ":1-100")
	replacement := handoffCandidate(handoffUUID + ":1-90")
	endpoint := testutil.NewFakeEndpoint(original)
	endpoint.OnStatement(func(q string) {
		if q == "STOP REPLICA" {
			endpoint.Switch(replacement)
		}
	})

	run := runHandoff(t, src, endpoint.Checker())

	if run.err == nil || !strings.Contains(run.err.Error(), "after STOP REPLICA") {
		t.Fatalf("ExecuteTargets() error = %v, want the refusal after STOP REPLICA", run.err)
	}
	for name, r := range map[string]*testutil.FakeReplica{"original": original, "replacement": replacement} {
		if slices.Contains(r.Statements(), "RESET REPLICA ALL") {
			t.Errorf("%s: RESET REPLICA ALL ran after the pinned connection broke", name)
		}
		assertCandidateNotWritable(t, r)
	}
	assertSourceUnfenced(t, src)
	if run.promoted != "" {
		t.Errorf("authority callback ran for %q", run.promoted)
	}
}

// Adapted from the round-2 review reproduction: a replacement that
// appears after the final GTID proof, just before the writability
// statements, must never be the server made writable. The pinned
// connection to the proven server breaks, so the handoff refuses.
func TestExecuteTargets_HandoffRefusesReplacementAfterFinalProof(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	original := handoffCandidate(handoffUUID + ":1-100")
	replacement := handoffCandidate(handoffUUID + ":1-90")
	endpoint := testutil.NewFakeEndpoint(original)
	resetSeen := false
	endpoint.OnStatement(func(q string) {
		switch {
		case q == "RESET REPLICA ALL":
			resetSeen = true
		case resetSeen && q == "SELECT @@global.gtid_executed":
			// The post-RESET proof passed on the original; the
			// Service now reaches the replacement.
			endpoint.Switch(replacement)
		}
	})

	run := runHandoff(t, src, endpoint.Checker())

	if run.err == nil {
		t.Fatalf("handoff succeeded with GTID %q; replacement %s writable=%v",
			run.gtid, replacement.State().ExecutedGtidSet, !replacement.State().ReadOnly)
	}
	assertCandidateNotWritable(t, replacement)
	assertCandidateNotWritable(t, original)
	assertSourceUnfenced(t, src)
	if run.promoted != "" {
		t.Errorf("authority callback ran for %q", run.promoted)
	}
}

// The round-2 review's wrapper form of the same race: a Checker whose
// writability calls would go to a replacement. Promotion uses the pinned
// session of the proven server, so only that server can become writable.
type lateReplacementChecker struct {
	mysql.Checker
	replacement *testutil.FakeReplica
}

func (c *lateReplacementChecker) SetSuperReadOnly(ctx context.Context, on bool) error {
	if !on {
		c.Checker = c.replacement.Checker()
	}
	return c.Checker.SetSuperReadOnly(ctx, on)
}

func TestExecuteHandoff_PromotesOnlyTheProvenServer(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	before := handoffCandidate(handoffUUID + ":1-100")
	after := handoffCandidate(handoffUUID + ":1-90")
	candidate := &lateReplacementChecker{Checker: before.Checker(), replacement: after}

	gtid, err := NewFailoverController(testutil.TestLogger()).ExecuteHandoff(context.Background(), candidate, src, "standby", HandoffOptions{})

	assertCandidateNotWritable(t, after)
	if err == nil {
		got, perr := mysql.ParseGTIDSet(gtid)
		want, _ := mysql.ParseGTIDSet(handoffUUID + ":1-100")
		if perr != nil || !got.Contains(want) || before.State().ReadOnly {
			t.Fatalf("handoff succeeded with GTID %q but the proven server writable=%v", gtid, !before.State().ReadOnly)
		}
	}
}

// A connection blip right after RESET REPLICA ALL stops the promotion on
// the same server. The abort re-creates the standby's replication channel
// to the source, so it keeps replicating instead of drifting behind.
func TestExecuteTargets_HandoffAbortAfterResetRestoresReplication(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	replica := handoffCandidate(handoffUUID + ":1-100")
	endpoint := testutil.NewFakeEndpoint(replica)
	broken := false
	endpoint.OnStatement(func(q string) {
		if q == "RESET REPLICA ALL" && !broken {
			broken = true
			endpoint.Break()
		}
	})

	run := runHandoff(t, src, endpoint.Checker())

	if run.err == nil || !strings.Contains(run.err.Error(), "after RESET REPLICA ALL") {
		t.Fatalf("ExecuteTargets() error = %v, want the refusal after RESET REPLICA ALL", run.err)
	}
	st := replica.State()
	if !st.Configured || st.SourceHost != "primary-internal" || !st.IORunning || !st.SQLRunning {
		t.Errorf("standby replication not restored: configured=%v source=%q io=%v sql=%v",
			st.Configured, st.SourceHost, st.IORunning, st.SQLRunning)
	}
	assertCandidateNotWritable(t, replica)
	assertSourceUnfenced(t, src)
}

// A RESET REPLICA ALL that errors may still have removed the channel, so
// the abort re-creates it before restarting replication.
func TestExecuteTargets_HandoffResetErrorRestoresChannel(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := &testutil.FakeMySQL{ReadOnlyVal: true, GtidExecutedVal: handoffUUID + ":1-100",
		ResetReplicaErr:  errors.New("i/o timeout"),
		ReplicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "primary-internal"}}

	run := runHandoff(t, src, candidate)

	if run.err == nil {
		t.Fatal("ExecuteTargets() = nil error, want the RESET REPLICA ALL failure")
	}
	calls := roleCalls(candidate.GetCalls())
	if i := slices.Index(calls, "ChangeReplicationSource"); i < 0 || !slices.Contains(calls[i:], "StartReplica") {
		t.Errorf("channel not re-created and restarted after a failed RESET: role calls %v", calls)
	}
	if got := candidate.ChangeReplicationOpts; got.Host != "primary-internal" {
		t.Errorf("channel re-created towards %q, want primary-internal", got.Host)
	}
	assertSourceUnfenced(t, src)
}

// A proof that only arrives after the budget expired is reported as such,
// not as "0 transactions missing".
func TestExecuteHandoff_LateProofIsReportedPlainly(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := lateGTIDCandidate{&testutil.FakeMySQL{ReadOnlyVal: true, GtidExecutedVal: handoffUUID + ":1-100",
		ReplicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "primary-internal"}}}

	_, err := NewFailoverController(testutil.TestLogger()).ExecuteHandoff(context.Background(), candidate, src, "standby",
		HandoffOptions{PreFenceCatchUp: 20 * time.Millisecond, CatchUp: 20 * time.Millisecond, PollInterval: time.Millisecond})

	if err == nil || !strings.Contains(err.Error(), "catch-up was confirmed only after the budget expired") {
		t.Fatalf("ExecuteHandoff() error = %v, want the late-confirmation message", err)
	}
}

// lateGTIDCandidate answers gtid_executed correctly, but only once its
// context has ended.
type lateGTIDCandidate struct {
	*testutil.FakeMySQL
}

func (c lateGTIDCandidate) GetGtidExecuted(ctx context.Context) (string, error) {
	<-ctx.Done()
	return c.FakeMySQL.GetGtidExecuted(context.Background())
}

// A STOP REPLICA that errors may still have stopped the threads; the
// handoff restarts them so the standby keeps replicating after the abort.
func TestExecuteTargets_HandoffRestartsReplicationWhenStopReplicaFails(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := &testutil.FakeMySQL{ReadOnlyVal: true, GtidExecutedVal: handoffUUID + ":1-100",
		StopReplicaErr:   errors.New("i/o timeout"),
		ReplicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "primary-internal"}}

	run := runHandoff(t, src, candidate)

	if run.err == nil {
		t.Fatal("ExecuteTargets() = nil error, want the STOP REPLICA failure")
	}
	if calls := roleCalls(candidate.GetCalls()); !slices.Contains(calls, "StartReplica") {
		t.Errorf("replication not restarted after a failed STOP REPLICA: role calls %v", calls)
	}
	assertSourceUnfenced(t, src)
}

// unansweringSource is an active primary that stops answering probes after
// its first CheckReadOnly, or once it is fenced.
type unansweringSource struct {
	*testutil.FakeMySQL
	mu           sync.Mutex
	probes       int
	failOnFenced bool
}

func (s *unansweringSource) CheckReadOnly(ctx context.Context) (bool, error) {
	ro, err := s.FakeMySQL.CheckReadOnly(ctx)
	s.mu.Lock()
	s.probes++
	n := s.probes
	s.mu.Unlock()
	lost := n > 1
	if s.failOnFenced {
		lost = slices.Contains(s.GetCalls(), "SetSuperReadOnly(ON)")
	}
	if lost {
		return false, errors.New("dial tcp: connection refused")
	}
	return ro, err
}

// While a handoff waits, cross-site failover is held off. A source that
// stops answering must end the wait after the loss window, not after the
// catch-up budget.
func TestExecuteTargets_HandoffEndsWhenSourceLost(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failOnFenced bool
		beforeFence  string
		wantFenced   bool
	}{
		{name: "before the fence", beforeFence: handoffUUID + ":1-100"},
		{name: "after the fence", failOnFenced: true, beforeFence: handoffUUID + ":1-90", wantFenced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &testutil.FakeMySQL{ReadOnlyVal: false}
			source := &unansweringSource{FakeMySQL: src, failOnFenced: tc.failOnFenced}
			gtids := &fencedGTIDSource{FakeMySQL: src, beforeFence: tc.beforeFence, afterFence: handoffUUID + ":1-100"}
			candidate := handoffCandidate(handoffUUID + ":1-90")
			opts := HandoffOptions{PreFenceCatchUp: 3 * time.Second, CatchUp: 3 * time.Second, PollInterval: time.Millisecond}

			start := time.Now()
			run := runHandoffWith(t, struct {
				*unansweringSource
				gtidReader
			}{source, gtids}, candidate.Checker(), opts, 20*time.Millisecond)
			elapsed := time.Since(start)

			if !errors.Is(run.err, errActivePrimaryLost) {
				t.Fatalf("ExecuteTargets() error = %v, want errActivePrimaryLost", run.err)
			}
			if elapsed >= 2*time.Second {
				t.Errorf("handoff held the update for %s, want it to end within the loss window", elapsed)
			}
			if fenced := slices.Contains(src.GetCalls(), "SetSuperReadOnly(ON)"); fenced != tc.wantFenced {
				t.Errorf("source fenced = %v, want %v", fenced, tc.wantFenced)
			}
			if run.promoted != "" {
				t.Errorf("authority callback ran for %q", run.promoted)
			}
		})
	}
}

// gtidReader lets a test combine one wrapper's GetGtidExecuted with
// another's CheckReadOnly.
type gtidReader interface {
	GetGtidExecuted(ctx context.Context) (string, error)
}

// blockingKillSource never returns from KillAppConnections until its
// context ends.
type blockingKillSource struct {
	*testutil.FakeMySQL
}

func (s blockingKillSource) KillAppConnections(ctx context.Context) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// slowGTIDCandidate answers gtid_executed only when its context ends.
type slowGTIDCandidate struct {
	*testutil.FakeMySQL
}

func (c slowGTIDCandidate) GetGtidExecuted(ctx context.Context) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

// The fenced phase has one budget that starts at the fence: a slow
// eviction or a slow probe cannot stretch the write pause past it.
func TestExecuteTargets_HandoffFencedBudgetBoundsSlowSteps(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100")
	opts := HandoffOptions{PreFenceCatchUp: time.Second, CatchUp: 100 * time.Millisecond, PollInterval: time.Millisecond}

	start := time.Now()
	run := runHandoffWith(t, blockingKillSource{src}, candidate.Checker(), opts, defaultActivePrimaryLossWindow)
	elapsed := time.Since(start)

	if run.err == nil {
		t.Fatal("ExecuteTargets() = nil error, want the fenced budget to expire")
	}
	if elapsed >= 2*time.Second {
		t.Errorf("fenced phase took %s with a 100ms budget, want it bounded by the budget (plus rollback)", elapsed)
	}
	assertSourceUnfenced(t, src)
}

// A slow candidate probe cannot stretch a catch-up wait past its budget.
func TestExecuteTargets_HandoffWaitBoundsSlowProbes(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := slowGTIDCandidate{&testutil.FakeMySQL{ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "primary-internal"}}}
	opts := HandoffOptions{PreFenceCatchUp: 50 * time.Millisecond, CatchUp: 50 * time.Millisecond, PollInterval: time.Millisecond}

	start := time.Now()
	run := runHandoffWith(t, src, candidate, opts, defaultActivePrimaryLossWindow)
	elapsed := time.Since(start)

	if run.err == nil {
		t.Fatal("ExecuteTargets() = nil error, want the pre-fence wait to time out")
	}
	if elapsed >= 2*time.Second {
		t.Errorf("pre-fence wait took %s with a 50ms budget, want slow probes bounded by it", elapsed)
	}
	if slices.Contains(src.GetCalls(), "SetSuperReadOnly(ON)") {
		t.Error("source was fenced although the candidate never proved catch-up")
	}
}

// A refusal before the fence is the common case (a lagging standby) and
// must leave the documented log line, with the reason, for operators.
func TestExecuteHandoff_PreFenceRefusalIsLogged(t *testing.T) {
	var logs bytes.Buffer
	fc := NewFailoverController(slog.New(slog.NewTextHandler(&logs, nil)))
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-90")

	_, err := fc.ExecuteHandoff(context.Background(), candidate.Checker(), src, "standby",
		HandoffOptions{PreFenceCatchUp: 20 * time.Millisecond, CatchUp: 20 * time.Millisecond, PollInterval: time.Millisecond})

	if err == nil {
		t.Fatal("ExecuteHandoff() = nil error, want the lagging standby refused")
	}
	out := logs.String()
	if !strings.Contains(out, `msg="maintenance handoff refused; source not fenced"`) ||
		!strings.Contains(out, "did not reach the source GTID set before fencing") {
		t.Fatalf("refusal not logged with its reason; logs:\n%s", out)
	}
}

// --- #202: candidate extras ----------------------------------------------

// errantUUID is a server the source never replicated from: transactions
// under it on the candidate are history the source lacks.
const errantUUID = "d1240b6c-71ca-11f0-9e33-c80aa9429563"

// Equal sets are the normal handoff: the candidate holds exactly what the
// fenced source committed.
func TestExecuteTargets_HandoffPromotesCandidateWithEqualSet(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100")
	src.ReplicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "standby-internal"}

	run := runHandoff(t, src, candidate.Checker())

	if run.err != nil {
		t.Fatalf("ExecuteTargets() error = %v", run.err)
	}
	if run.promoted != "standby" || candidate.State().ReadOnly {
		t.Fatalf("promoted=%q candidate read_only=%v, want standby promoted", run.promoted, candidate.State().ReadOnly)
	}
}

// A caught-up replica that also holds transactions the source never
// committed (issue #202's demoted site) is a strict superset of the source.
// Containment alone would promote it and make its errant history
// authoritative. The handoff refuses before fencing, so no write pauses and
// the next attempt costs nothing.
func TestExecuteTargets_HandoffRefusesSupersetCandidateBeforeFencing(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100," + errantUUID + ":1-221")

	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = time.Millisecond
	uc.handoff = HandoffOptions{PreFenceCatchUp: 100 * time.Millisecond, CatchUp: 100 * time.Millisecond, PollInterval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var run handoffRun
	run.processed, run.err = uc.ExecuteTargets(ctx,
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: src, Drifted: true},
		[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: candidate.Checker(), Promotable: true, ExpectedSource: "primary-internal"}},
		func(_ context.Context, name string) error { run.applied = append(run.applied, name); return nil },
		func(site, gtid string) { run.promoted, run.gtid = site, gtid })

	assertHandoffAborted(t, run, src, candidate, false)
	var divergent *DivergentCandidateError
	if !errors.As(run.err, &divergent) {
		t.Fatalf("error = %v, want a DivergentCandidateError", run.err)
	}
	if divergent.Candidate != "standby" || divergent.Transactions != 221 || divergent.Gtid != errantUUID+":1-221" {
		t.Errorf("DivergentCandidateError = %+v, want standby with 221 transactions %s:1-221", divergent, errantUUID)
	}
	out := logs.String()
	if !strings.Contains(out, `msg="maintenance handoff refused; candidate has transactions the source lacks"`) ||
		!strings.Contains(out, "extraTransactions=221") || !strings.Contains(out, "extraGtid="+errantUUID+":1-221") {
		t.Errorf("refusal not logged with the extra set; logs:\n%s", out)
	}
}

// The pre-fence check can miss extras that appear later (it reads a live
// replica). The pinned proof after STOP REPLICA requires equality with the
// fenced source: on extras the handoff aborts, restarts the candidate's
// replication, gives the source its writes back, and never enables writes.
func TestExecuteTargets_HandoffAbortsOnExtrasAfterStopReplica(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	replica := handoffCandidate(handoffUUID + ":1-100")
	endpoint := testutil.NewFakeEndpoint(replica)
	endpoint.OnStatement(func(q string) {
		if q == "STOP REPLICA" {
			replica.Mutate(func(s *testutil.FakeReplicaState) {
				s.ExecutedGtidSet = handoffUUID + ":1-100," + errantUUID + ":1-3"
			})
		}
	})

	run := runHandoff(t, src, endpoint.Checker())

	var divergent *DivergentCandidateError
	if !errors.As(run.err, &divergent) || divergent.Transactions != 3 {
		t.Fatalf("ExecuteTargets() error = %v, want a DivergentCandidateError naming 3 transactions", run.err)
	}
	if !strings.Contains(run.err.Error(), "after STOP REPLICA") {
		t.Errorf("error = %v, want the post-STOP proof named", run.err)
	}
	if slices.Contains(replica.Statements(), "RESET REPLICA ALL") {
		t.Error("RESET REPLICA ALL ran after the post-STOP proof failed")
	}
	if st := replica.State(); !st.IORunning || !st.SQLRunning || !st.Configured {
		t.Errorf("candidate replication not restarted: io=%v sql=%v configured=%v", st.IORunning, st.SQLRunning, st.Configured)
	}
	assertCandidateNotWritable(t, replica)
	assertSourceUnfenced(t, src)
	if run.promoted != "" {
		t.Errorf("authority callback ran for %q", run.promoted)
	}
}

// Extras seen only after RESET REPLICA ALL: the channel is gone, so the
// abort re-creates it towards the source before restarting replication.
func TestExecuteTargets_HandoffAbortsOnExtrasAfterReset(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	replica := handoffCandidate(handoffUUID + ":1-100")
	endpoint := testutil.NewFakeEndpoint(replica)
	endpoint.OnStatement(func(q string) {
		if q == "RESET REPLICA ALL" {
			replica.Mutate(func(s *testutil.FakeReplicaState) {
				s.ExecutedGtidSet = handoffUUID + ":1-100," + errantUUID + ":7"
			})
		}
	})

	run := runHandoff(t, src, endpoint.Checker())

	var divergent *DivergentCandidateError
	if !errors.As(run.err, &divergent) || divergent.Transactions != 1 {
		t.Fatalf("ExecuteTargets() error = %v, want a DivergentCandidateError naming 1 transaction", run.err)
	}
	if !strings.Contains(run.err.Error(), "after RESET REPLICA ALL") {
		t.Errorf("error = %v, want the post-RESET proof named", run.err)
	}
	st := replica.State()
	if !st.Configured || st.SourceHost != "primary-internal" || !st.IORunning || !st.SQLRunning {
		t.Errorf("channel not re-created: configured=%v source=%q io=%v sql=%v", st.Configured, st.SourceHost, st.IORunning, st.SQLRunning)
	}
	assertCandidateNotWritable(t, replica)
	assertSourceUnfenced(t, src)
}

// A standby with recorded divergence is not a handoff target: the active
// update stalls with a message that names the set, and nothing is fenced.
func TestExecuteTargets_SkipsDivergentStandbyAsHandoffTarget(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100," + errantUUID + ":1-221")
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = time.Millisecond
	uc.handoff = HandoffOptions{PreFenceCatchUp: 100 * time.Millisecond, CatchUp: 100 * time.Millisecond, PollInterval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	processed, err := uc.ExecuteTargets(ctx,
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: src, Drifted: true},
		[]UpdateTarget{{
			Name: "standby", Host: "standby-internal", Checker: candidate.Checker(),
			Promotable: true, ExpectedSource: "primary-internal",
			DivergenceBlocked: true, DivergentTransactions: 221, DivergentGtid: errantUUID + ":1-221",
		}},
		func(context.Context, string) error { return nil }, nil)

	if err == nil {
		t.Fatal("ExecuteTargets() = nil error, want the divergent standby refused")
	}
	want := "standby is divergent (221 transactions the primary lacks); reclone or reconcile before the active site can be updated"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error = %q, want it to contain %q", err, want)
	}
	if slices.Contains(processed, "primary") {
		t.Errorf("active drift cleared: processed=%v", processed)
	}
	if slices.Contains(src.GetCalls(), "SetSuperReadOnly(ON)") {
		t.Error("source fenced for a divergent standby")
	}
	assertCandidateNotWritable(t, candidate)
}

// slowFirstReadCandidate answers its first gtid_executed read after
// delay, and reports a lagging set throughout.
type slowFirstReadCandidate struct {
	*testutil.FakeMySQL
	delay time.Duration
	reads atomic.Int32
}

func (c *slowFirstReadCandidate) GetGtidExecuted(ctx context.Context) (string, error) {
	if c.reads.Add(1) == 1 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return c.FakeMySQL.GetGtidExecuted(ctx)
}

// Everything before the fence shares one PreFenceCatchUp budget: a slow
// candidate read followed by a lagging wait cannot hold the update (and
// the emergency failover it holds off) for twice that.
func TestExecuteHandoff_PreFencePhaseSharesOneBudget(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	const budget = 300 * time.Millisecond
	candidate := &slowFirstReadCandidate{delay: budget - 50*time.Millisecond, FakeMySQL: &testutil.FakeMySQL{ReadOnlyVal: true, GtidExecutedVal: handoffUUID + ":1-90",
		ReplicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "primary-internal"}}}

	start := time.Now()
	_, err := NewFailoverController(testutil.TestLogger()).ExecuteHandoff(context.Background(), candidate, src, "standby",
		HandoffOptions{PreFenceCatchUp: budget, CatchUp: budget, PollInterval: time.Millisecond})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("ExecuteHandoff() = nil error, want the pre-fence phase to time out")
	}
	if elapsed > budget+budget/2 {
		t.Fatalf("pre-fence phase took %s, want it bounded by one %s budget", elapsed, budget)
	}
	if slices.Contains(src.GetCalls(), "SetSuperReadOnly(ON)") {
		t.Error("source fenced")
	}
}
