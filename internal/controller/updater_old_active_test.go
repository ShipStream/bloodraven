package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// runRollout runs a full ordered update of a drifted standby and the active
// site, with afterActiveUpdate run when the active site's Deployment is
// updated (after the handoff, before it is repointed at the new primary).
func runRollout(t *testing.T, source *fencedGTIDSource, candidate *testutil.FakeReplica, standbyDrifted bool,
	afterStandbyUpdate, afterActiveUpdate func()) handoffRun {
	t.Helper()
	logger := testutil.TestLogger()
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = time.Millisecond
	uc.handoff = HandoffOptions{PreFenceCatchUp: 2 * time.Second, CatchUp: 2 * time.Second, PollInterval: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var run handoffRun
	run.processed, run.err = uc.ExecuteTargets(ctx,
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: source, Drifted: true},
		[]UpdateTarget{{
			Name: "standby", Host: "standby-internal", Checker: candidate.Checker(),
			Promotable: true, Drifted: standbyDrifted, ExpectedSource: "primary-internal",
		}},
		func(_ context.Context, name string) error {
			run.applied = append(run.applied, name)
			switch {
			case name == "standby" && afterStandbyUpdate != nil:
				afterStandbyUpdate()
			case name == "primary" && afterActiveUpdate != nil:
				afterActiveUpdate()
			}
			return nil
		},
		func(site, gtid string) { run.promoted, run.gtid = site, gtid })
	return run
}

// Issue #202, end to end: the rollout restarts the standby, whose receiver
// reconnects with its threads running but a backlog still to fetch, while
// the active site keeps committing until it is fenced. The standby is
// promoted only once it holds everything the fenced source committed, and
// the old active is then repointed at it.
func TestExecuteTargets_RestartedStandbyIsPromotedOnlyAfterCatchingUp(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-100", afterFence: handoffUUID + ":1-105"}
	candidate := handoffCandidate(handoffUUID + ":1-90")
	restarted := false
	sinceRestart := 0
	candidate.OnPoll(func(_ int, s *testutil.FakeReplicaState) {
		if !restarted || !s.Configured {
			return
		}
		// The receiver fetches the backlog a few polls after the restart,
		// and what was committed up to the fence a few polls after that.
		sinceRestart++
		switch {
		case source.fenced() && sinceRestart >= 6:
			s.RetrievedGtidSet, s.ExecutedGtidSet = handoffUUID+":1-105", handoffUUID+":1-105"
		case sinceRestart >= 3:
			s.RetrievedGtidSet, s.ExecutedGtidSet = handoffUUID+":1-100", handoffUUID+":1-100"
		}
	})

	run := runRollout(t, source, candidate, true, func() {
		// The pod is back and replicating; nothing of 91-100 has arrived.
		restarted = true
	}, func() {
		src.ReplicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "standby-internal"}
	})

	if run.err != nil {
		t.Fatalf("ExecuteTargets() error = %v", run.err)
	}
	if run.promoted != "standby" {
		t.Fatalf("promoted = %q, want standby", run.promoted)
	}
	got, err := mysql.ParseGTIDSet(run.gtid)
	if err != nil {
		t.Fatalf("parse promotion GTID %q: %v", run.gtid, err)
	}
	want, _ := mysql.ParseGTIDSet(source.afterFence)
	if missing := want.Subtract(got); !missing.IsEmpty() {
		t.Fatalf("DATA LOSS: standby promoted at %q without %d transactions the fenced source committed (%s)",
			run.gtid, missing.TransactionCount(), missing)
	}
	if got := strings.Join(run.processed, ","); got != "standby,primary" {
		t.Fatalf("processed = %q, want standby,primary", got)
	}
	if !slices.Contains(src.GetCalls(), "ChangeReplicationSource") {
		t.Fatal("old active holding nothing the new primary lacks was not repointed at it")
	}
}

// The handoff proves the new primary holds the fenced source's set, so this
// cannot happen through ExecuteHandoff. If the old active nonetheless holds
// transactions the new primary lacks when it is about to be repointed, the
// rollout must not repoint it: that would leave them as errant GTIDs on a
// replica that looks healthy, as in #202. Left fenced and unconfigured, it
// goes to old-primary recovery, which reports the divergence.
func TestExecuteTargets_DoesNotRepointOldActiveHoldingTransactionsNewPrimaryLacks(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-100", afterFence: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100")

	run := runRollout(t, source, candidate, false, nil, func() {
		source.afterFence = handoffUUID + ":1-100,eeeeeeee-0000-0000-0000-000000000001:1-3"
		src.ReplicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "standby-internal"}
	})

	if run.promoted != "standby" {
		t.Fatalf("promoted = %q, want the handoff to have happened", run.promoted)
	}
	if run.err == nil || !strings.Contains(run.err.Error(), "3 transactions") {
		t.Fatalf("ExecuteTargets() error = %v, want a refusal naming the 3 divergent transactions", run.err)
	}
	assertOldActiveNotRepointed(t, run, src)
}

func TestExecuteTargets_DoesNotRepointOldActiveWithUnreadableGTIDSet(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false}
	source := &fencedGTIDSource{FakeMySQL: src,
		beforeFence: handoffUUID + ":1-100", afterFence: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100")

	run := runRollout(t, source, candidate, false, nil, func() {
		source.afterFenceErr = errors.New("read tcp: i/o timeout")
	})

	if run.promoted != "standby" {
		t.Fatalf("promoted = %q, want the handoff to have happened", run.promoted)
	}
	if run.err == nil || !strings.Contains(run.err.Error(), "before repointing") {
		t.Fatalf("ExecuteTargets() error = %v, want a refusal to repoint without a GTID comparison", run.err)
	}
	assertOldActiveNotRepointed(t, run, src)
}

func assertOldActiveNotRepointed(t *testing.T, run handoffRun, src *testutil.FakeMySQL) {
	t.Helper()
	for _, c := range src.GetCalls() {
		if c == "ChangeReplicationSource" || c == "StartReplica" || c == "ResetReplicaAll" {
			t.Fatalf("old active was reconfigured (%s) without proof the new primary holds its transactions; calls %v", c, src.GetCalls())
		}
	}
	if !src.IsReadOnlyVal() {
		t.Fatal("old active is writable after the handoff")
	}
	if slices.Contains(run.processed, "primary") {
		t.Fatalf("old active drift was cleared: processed=%v", run.processed)
	}
}
