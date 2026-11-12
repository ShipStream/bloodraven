package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// regressedSource is a fencedGTIDSource that answers the fenced read with
// afterFence and every later read with afterRestart: it restarted on an
// older data directory while the handoff waited for the candidate.
type regressedSource struct {
	*fencedGTIDSource
	afterRestart string
	fencedReads  atomic.Int32
}

func (s *regressedSource) GetGtidExecuted(ctx context.Context) (string, error) {
	got, err := s.fencedGTIDSource.GetGtidExecuted(ctx)
	if err == nil && s.fenced() && s.fencedReads.Add(1) > 1 {
		return s.afterRestart, nil
	}
	return got, err
}

// An abort gives the source its writes back only if it still holds what
// the handoff read from it. A source that restarted on older data during
// the fenced wait is read-only by its own boot; unfencing it would have it
// issue the lost GTIDs again (the ordered update's abort unfence the DST
// sweep found, first seed 8805).
func TestExecuteTargets_HandoffAbortLeavesARegressedSourceReadOnly(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &regressedSource{
		fencedGTIDSource: &fencedGTIDSource{FakeMySQL: src, beforeFence: handoffUUID + ":1-90", afterFence: handoffUUID + ":1-100"},
		afterRestart:     handoffUUID + ":1-95",
	}
	// The candidate holds the pre-fence set and never reaches the fenced
	// one, so the fenced wait runs out and the handoff aborts.
	candidate := handoffCandidate(handoffUUID + ":1-90")

	run := runHandoff(t, source, candidate.Checker())

	if run.err == nil || !strings.Contains(run.err.Error(), "source left read-only: it lacks 5 transactions it held before the handoff") {
		t.Fatalf("ExecuteTargets() error = %v, want the source left read-only for the 5 transactions it lost", run.err)
	}
	if calls := roleCalls(src.GetCalls()); slices.Contains(calls, "SetReadOnly(OFF)") || slices.Contains(calls, "SetSuperReadOnly(OFF)") {
		t.Fatalf("the regressed source was unfenced: role calls %v", calls)
	}
	if run.promoted != "" {
		t.Fatalf("handoff promoted %q", run.promoted)
	}

	// The control: the same wait running out on a source that kept its
	// set is unfenced.
	src = &testutil.FakeMySQL{ReadOnlyVal: false}
	whole := &fencedGTIDSource{FakeMySQL: src, beforeFence: handoffUUID + ":1-90", afterFence: handoffUUID + ":1-100"}
	run = runHandoff(t, whole, handoffCandidate(handoffUUID+":1-90").Checker())
	if run.err == nil {
		t.Fatal("ExecuteTargets() = nil error, want the catch-up wait to run out")
	}
	if calls := roleCalls(src.GetCalls()); !slices.Contains(calls, "SetReadOnly(OFF)") || !slices.Contains(calls, "SetSuperReadOnly(OFF)") {
		t.Fatalf("a source that kept its set was not unfenced: role calls %v", calls)
	}
}

// After a handoff the old active, whose pod the update just replaced, is
// configured as a replica of the new primary only through the replication
// start gate and after the rejoin check: one that came back without
// transactions it issued is left fenced without a channel.
func TestExecuteTargets_OldActiveIsRejoinedThroughTheGateAndTheCheck(t *testing.T) {
	run := func(check func(context.Context, string, mysql.Checker, mysql.Checker) error, gate func(string, func() error) error) (error, *testutil.FakeMySQL, []string) {
		src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
		src.ReplicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "standby-internal"}
		candidate := handoffCandidate(handoffUUID + ":1-100")
		logger := testutil.TestLogger()
		uc := NewUpdateController(NewFailoverController(logger), logger)
		uc.tickInterval = time.Millisecond
		uc.handoff = HandoffOptions{PreFenceCatchUp: 100 * time.Millisecond, CatchUp: 100 * time.Millisecond, PollInterval: time.Millisecond}
		var asked []string
		if check != nil {
			uc.SetReplicaRejoinCheck(func(ctx context.Context, site string, replica, source mysql.Checker) error {
				asked = append(asked, site)
				return check(ctx, site, replica, source)
			})
		}
		if gate != nil {
			uc.SetReplicaStartGate(gate)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := uc.ExecuteTargets(ctx,
			UpdateTarget{Name: "primary", Host: "primary-internal", Checker: src, Drifted: true},
			[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: candidate.Checker(), Promotable: true, ExpectedSource: "primary-internal"}},
			func(context.Context, string) error { return nil }, func(string, string) {})
		return err, src, asked
	}

	// Nothing in the way: the old active is repointed, and the check was
	// asked about it.
	err, src, asked := run(func(context.Context, string, mysql.Checker, mysql.Checker) error { return nil }, nil)
	if err != nil || !slices.Contains(roleCalls(src.GetCalls()), "ChangeReplicationSource") {
		t.Fatalf("err=%v role calls %v, want the old active repointed", err, roleCalls(src.GetCalls()))
	}
	if !slices.Contains(asked, "primary") {
		t.Fatalf("rejoin check asked about %v, want the old active", asked)
	}

	// The check finds the old active without its own transactions.
	err, src, _ = run(func(context.Context, string, mysql.Checker, mysql.Checker) error { return errOwnTransactionsLost }, nil)
	if !errors.Is(err, errOwnTransactionsLost) {
		t.Fatalf("ExecuteTargets() error = %v, want errOwnTransactionsLost", err)
	}
	if calls := roleCalls(src.GetCalls()); slices.Contains(calls, "ChangeReplicationSource") || slices.Contains(calls, "StartReplica") {
		t.Fatalf("replication was configured on an old active that lacks its own transactions: %v", calls)
	}

	// The gate refuses (a report recorded earlier).
	err, src, _ = run(nil, func(site string, _ func() error) error {
		if site == "primary" {
			return errOwnTransactionsLost
		}
		return nil
	})
	if !errors.Is(err, errOwnTransactionsLost) {
		t.Fatalf("ExecuteTargets() error = %v, want the gate's refusal", err)
	}
	if calls := roleCalls(src.GetCalls()); slices.Contains(calls, "ChangeReplicationSource") || slices.Contains(calls, "StartReplica") {
		t.Fatalf("replication was configured past the gate: %v", calls)
	}
}

// The original two-site Execute restarts a standby's stopped channel
// through the same gate (it used to call StartReplica directly).
func TestWaitForReplicaReady_StartsTheChannelThroughTheGate(t *testing.T) {
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = time.Millisecond
	uc.SetReplicaStartGate(func(string, func() error) error { return errOwnTransactionsLost })
	standby := &testutil.FakeMySQL{ReadOnlyVal: true,
		ReplicaStatusVal: &mysql.ReplicaStatus{SourceHost: "primary-internal", IORunning: false, SQLRunning: false}}
	err := uc.waitForReplicaReady(context.Background(), UpdateTarget{Name: "standby", Checker: standby}, nil, 30*time.Millisecond)
	if err == nil {
		t.Fatal("waitForReplicaReady() = nil, want a timeout: the channel must not be started")
	}
	if slices.Contains(standby.GetCalls(), "StartReplica") {
		t.Fatalf("StartReplica was called past the gate: %v", standby.GetCalls())
	}
}

// The fence can only stop the source's set from growing. A fenced set
// that lacks transactions the source held before the fence means the
// source restarted on older data between the two reads: the handoff
// aborts and does not unfence it.
func TestExecuteTargets_HandoffAbortsOnASourceThatRegressedBeforeTheFencedRead(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src, beforeFence: handoffUUID + ":1-100", afterFence: handoffUUID + ":1-95"}
	candidate := handoffCandidate(handoffUUID + ":1-100")

	run := runHandoff(t, source, candidate.Checker())

	if run.err == nil || !strings.Contains(run.err.Error(), "the fenced source no longer holds every transaction it held before the fence") ||
		!strings.Contains(run.err.Error(), "source left read-only") {
		t.Fatalf("ExecuteTargets() error = %v, want the regressed source left read-only", run.err)
	}
	if calls := roleCalls(src.GetCalls()); slices.Contains(calls, "SetReadOnly(OFF)") || slices.Contains(calls, "SetSuperReadOnly(OFF)") {
		t.Fatalf("the regressed source was unfenced: role calls %v", calls)
	}
	if got := candidatePromotionStatements(candidate.Statements()); len(got) != 0 || run.promoted != "" {
		t.Fatalf("the candidate was promoted over a source that lost transactions: %v promoted=%q", got, run.promoted)
	}
}

// restartingSource is a fencedGTIDSource that restarts once its fenced
// set has been read: mysqld comes back with read_only=ON and
// super_read_only=OFF, holding the same set.
type restartingSource struct {
	*fencedGTIDSource
	reads atomic.Int32
}

func (s *restartingSource) GetGtidExecuted(ctx context.Context) (string, error) {
	if s.fenced() {
		s.reads.Add(1)
	}
	return s.fencedGTIDSource.GetGtidExecuted(ctx)
}

func (s *restartingSource) CheckSuperReadOnly(context.Context) (bool, error) {
	return s.fenced() && s.reads.Load() == 0, nil
}

// An abort lifts the handoff's own fence only. A source that restarted
// while the handoff waited is read-only by its start (read_only without
// super_read_only), and stays so for the re-assert to decide, even when
// its set shows nothing lost.
func TestExecuteTargets_HandoffAbortLeavesARestartedSourceReadOnly(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &restartingSource{fencedGTIDSource: &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-90", afterFence: handoffUUID + ":1-100"}}
	candidate := handoffCandidate(handoffUUID + ":1-90")

	run := runHandoff(t, source, candidate.Checker())

	if run.err == nil || !strings.Contains(run.err.Error(), "source left read-only: it restarted during the handoff") {
		t.Fatalf("ExecuteTargets() error = %v, want the restarted source left read-only", run.err)
	}
	if calls := roleCalls(src.GetCalls()); slices.Contains(calls, "SetReadOnly(OFF)") || slices.Contains(calls, "SetSuperReadOnly(OFF)") {
		t.Fatalf("the restarted source was unfenced: role calls %v", calls)
	}
}

// writableRegressedSource is a regressedSource that came back writable:
// after the fenced read it answers read_only=OFF (a pod that boots
// writable, or something that lifted its read_only).
type writableRegressedSource struct {
	*regressedSource
}

func (s *writableRegressedSource) CheckReadOnly(ctx context.Context) (bool, error) {
	if s.fenced() && s.fencedReads.Load() >= 1 {
		return false, nil
	}
	return s.regressedSource.CheckReadOnly(ctx)
}

// The abort says what it found. A source that restarted on older data and
// is writable is not "left read-only": the handoff sends it nothing, and
// the error says it is writable without the transactions. (The follower
// that holds them is then reported by the replica GTID audit, and the log
// names the primary as the site that lost them:
// TestOwnLoss_AWritablePrimaryThatLacksItsOwnTransactionsIsNamed.)
func TestExecuteTargets_HandoffAbortSaysAWritableRegressedSourceIsWritable(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &writableRegressedSource{&regressedSource{
		fencedGTIDSource: &fencedGTIDSource{FakeMySQL: src, beforeFence: handoffUUID + ":1-90", afterFence: handoffUUID + ":1-100"},
		afterRestart:     handoffUUID + ":1-95",
	}}
	run := runHandoff(t, source, handoffCandidate(handoffUUID+":1-90").Checker())
	if run.err == nil || !strings.Contains(run.err.Error(), "source is writable without 5 transactions it held before the handoff") {
		t.Fatalf("ExecuteTargets() error = %v, want it to say the source is writable without the 5 transactions", run.err)
	}
	if strings.Contains(run.err.Error(), "left read-only") {
		t.Fatalf("ExecuteTargets() error = %v: it says the source was left read-only, and it is writable", run.err)
	}
	if calls := roleCalls(src.GetCalls()); slices.Contains(calls, "SetReadOnly(OFF)") || slices.Contains(calls, "SetSuperReadOnly(OFF)") {
		t.Fatalf("the handoff sent the regressed source an unfence: role calls %v", calls)
	}
}
