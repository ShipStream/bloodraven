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

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// oldActive is the active site of an ordered update. Like a promoted-away
// primary, it has no replication channel until the rollout configures and
// starts one: promotion runs RESET REPLICA ALL and the handoff only fences
// the source. restart models the rollout replacing its pod: mysqld boots
// with read_only only, so super_read_only (never persisted) is off until
// something sets it again. With afterPrivilegedWrite set, a SUPER session
// commits one transaction as soon as it can: right after the first GTID
// read that finds the restarted site unfenced. From then on the site's set
// is afterPrivilegedWrite.
type oldActive struct {
	*fencedGTIDSource
	afterPrivilegedWrite string

	mu        sync.Mutex
	restarted bool
	refenced  bool
	wrote     bool
}

func newOldActive(beforeFence, afterFence string) *oldActive {
	return &oldActive{fencedGTIDSource: &fencedGTIDSource{
		FakeMySQL:   &testutil.FakeMySQL{ReadOnlyVal: false},
		beforeFence: beforeFence, afterFence: afterFence,
	}}
}

func (s *oldActive) restart() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.restarted, s.refenced = true, false
}

// refencedSinceRestart reports whether super_read_only is set on the
// restarted site.
func (s *oldActive) refencedSinceRestart() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restarted && s.refenced
}

func (s *oldActive) privilegedWriteLanded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wrote
}

func (s *oldActive) SetSuperReadOnly(ctx context.Context, on bool) error {
	err := s.fencedGTIDSource.SetSuperReadOnly(ctx, on)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil && s.restarted {
		s.refenced = on
	}
	return err
}

func (s *oldActive) GetGtidExecuted(ctx context.Context) (string, error) {
	raw, err := s.fencedGTIDSource.GetGtidExecuted(ctx)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wrote {
		return s.afterPrivilegedWrite, nil
	}
	if s.restarted && !s.refenced && s.afterPrivilegedWrite != "" {
		s.wrote = true
	}
	return raw, nil
}

func (s *oldActive) ShowReplicaStatus(ctx context.Context) (*mysql.ReplicaStatus, error) {
	if _, err := s.FakeMySQL.ShowReplicaStatus(ctx); err != nil {
		return nil, err
	}
	if !slices.Contains(s.GetCalls(), "StartReplica") {
		return nil, nil
	}
	return &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "standby-internal"}, nil
}

// newPrimary is the handoff candidate with its GTID reads replaced once
// the rollout has moved on to the old active.
type newPrimary struct {
	mysql.Checker

	mu   sync.Mutex
	gtid func() (string, error)
}

func (p *newPrimary) replaceGTIDReads(fn func() (string, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gtid = fn
}

func (p *newPrimary) GetGtidExecuted(ctx context.Context) (string, error) {
	p.mu.Lock()
	fn := p.gtid
	p.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return p.Checker.GetGtidExecuted(ctx)
}

// rollout is a full ordered update of a standby and the active site.
// afterActiveUpdate runs when the active site's Deployment is updated:
// after the handoff, before the site is repointed at the new primary.
type rollout struct {
	source, candidate                     mysql.Checker
	standbyDrifted                        bool
	afterStandbyUpdate, afterActiveUpdate func()
	logs                                  *bytes.Buffer
}

func (r rollout) run(t *testing.T) handoffRun {
	t.Helper()
	logger := testutil.TestLogger()
	if r.logs != nil {
		logger = slog.New(slog.NewTextHandler(r.logs, nil))
	}
	uc := NewUpdateController(NewFailoverController(logger), logger)
	uc.tickInterval = time.Millisecond
	uc.handoff = HandoffOptions{PreFenceCatchUp: 2 * time.Second, CatchUp: 2 * time.Second, PollInterval: time.Millisecond}
	// Bounds the old-active wait that follows a (wrong) repoint so a
	// regression fails instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var run handoffRun
	run.processed, run.err = uc.ExecuteTargets(ctx,
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: r.source, Drifted: true},
		[]UpdateTarget{{
			Name: "standby", Host: "standby-internal", Checker: r.candidate,
			Promotable: true, Drifted: r.standbyDrifted, ExpectedSource: "primary-internal",
		}},
		func(_ context.Context, name string) error {
			run.applied = append(run.applied, name)
			switch {
			case name == "standby" && r.afterStandbyUpdate != nil:
				r.afterStandbyUpdate()
			case name == "primary" && r.afterActiveUpdate != nil:
				r.afterActiveUpdate()
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
	source := newOldActive(handoffUUID+":1-100", handoffUUID+":1-105")
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

	run := rollout{
		source: source, candidate: candidate.Checker(), standbyDrifted: true,
		// The pod is back and replicating; nothing of 91-100 has arrived.
		afterStandbyUpdate: func() { restarted = true },
		afterActiveUpdate:  source.restart,
	}.run(t)

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
	if !slices.Contains(source.GetCalls(), "ChangeReplicationSource") {
		t.Fatal("old active holding nothing the new primary lacks was not repointed at it")
	}
}

// The rollout replaced the old active's pod, so it is back with read_only
// only. If it compared GTID sets before fencing, a privileged write landing
// after the read would pass the comparison and be repointed into an errant
// GTID. Fenced first, the write cannot land, and the clean site is
// repointed.
func TestExecuteTargets_FencesRestartedOldActiveBeforeComparingGTIDs(t *testing.T) {
	source := newOldActive(handoffUUID+":1-100", handoffUUID+":1-100")
	source.afterPrivilegedWrite = handoffUUID + ":1-101"
	candidate := handoffCandidate(handoffUUID + ":1-100")

	run := rollout{source: source, candidate: candidate.Checker(), afterActiveUpdate: source.restart}.run(t)

	if source.privilegedWriteLanded() {
		t.Fatalf("a write landed on the restarted old active after its GTID set was compared; repointing buried it as an errant GTID (err=%v, calls %v)",
			run.err, roleCalls(source.GetCalls()))
	}
	if run.err != nil {
		t.Fatalf("ExecuteTargets() error = %v", run.err)
	}
	if got := strings.Join(run.processed, ","); got != "primary" {
		t.Fatalf("processed = %q, want primary", got)
	}
}

// The handoff proves the new primary holds the fenced source's set, so this
// cannot happen through ExecuteHandoff. If the old active nonetheless holds
// transactions the new primary lacks when it is about to be repointed, the
// rollout must not repoint it: that would leave them as errant GTIDs on a
// replica that looks healthy, as in #202. It stays fenced with no
// replication channel, for old-primary recovery to report (see
// TestOrderedUpdate_OldActiveWithTransactionsNewPrimaryLacksIsReportedByRecovery).
func TestExecuteTargets_DoesNotRepointOldActiveHoldingTransactionsNewPrimaryLacks(t *testing.T) {
	const divergent = "eeeeeeee-0000-0000-0000-000000000001:1-3"
	source := newOldActive(handoffUUID+":1-100", handoffUUID+":1-100")
	candidate := handoffCandidate(handoffUUID + ":1-100")
	var logs bytes.Buffer

	run := rollout{source: source, candidate: candidate.Checker(), logs: &logs, afterActiveUpdate: func() {
		source.restart()
		source.afterFence = handoffUUID + ":1-100," + divergent
	}}.run(t)

	if run.promoted != "standby" {
		t.Fatalf("promoted = %q, want the handoff to have happened", run.promoted)
	}
	if run.err == nil || !strings.Contains(run.err.Error(), "3 transactions") {
		t.Fatalf("ExecuteTargets() error = %v, want a refusal naming the 3 divergent transactions", run.err)
	}
	assertOldActiveNotRepointed(t, run, source, true)

	var record string
	for line := range strings.Lines(logs.String()) {
		if strings.Contains(line, `msg="ordered update: old active has transactions the new primary lacks; not repointing it"`) {
			record = line
		}
	}
	if record == "" {
		t.Fatalf("refusal not logged; logs:\n%s", logs.String())
	}
	for _, want := range []string{
		"level=ERROR", "site=primary", "newPrimary=standby", "divergentTransactions=3",
		"divergentGtid=" + divergent,
		"oldPrimaryGtid=" + handoffUUID + ":1-100," + divergent,
		"newPrimaryGtid=" + handoffUUID + ":1-100",
		`error="old active primary has 3 transactions`,
	} {
		if !strings.Contains(record, want) {
			t.Errorf("refusal record lacks %s:\n%s", want, record)
		}
	}
}

// Every way the comparison can fail to happen refuses the repoint: a fence
// that may not have applied, a GTID read that failed, or a set that cannot
// be parsed, on either side.
func TestExecuteTargets_DoesNotRepointOldActiveWithoutAGTIDComparison(t *testing.T) {
	lost := errors.New("Error 2013: Lost connection to MySQL server during query")
	for _, tc := range []struct {
		name        string
		fail        func(oa *oldActive, np *newPrimary)
		wantErr     string
		unparseable bool
		// wantFenced is false when the fence itself failed.
		wantFenced bool
	}{
		{
			name:    "old active fence fails",
			fail:    func(oa *oldActive, _ *newPrimary) { oa.FenceErr = lost },
			wantErr: "fence old active primary before repointing",
		},
		{
			name:       "old active GTID read fails",
			fail:       func(oa *oldActive, _ *newPrimary) { oa.afterFenceErr = lost },
			wantErr:    "read old active primary GTID set before repointing",
			wantFenced: true,
		},
		{
			name:        "old active GTID set unparseable",
			fail:        func(oa *oldActive, _ *newPrimary) { oa.afterFence = handoffUUID + ":garbage-" },
			wantErr:     "read old active primary GTID set before repointing",
			unparseable: true,
			wantFenced:  true,
		},
		{
			name: "new primary GTID read fails",
			fail: func(_ *oldActive, np *newPrimary) {
				np.replaceGTIDReads(func() (string, error) { return "", lost })
			},
			wantErr:    "read new primary standby GTID set before repointing",
			wantFenced: true,
		},
		{
			name: "new primary GTID set unparseable",
			fail: func(_ *oldActive, np *newPrimary) {
				np.replaceGTIDReads(func() (string, error) { return handoffUUID + ":garbage-", nil })
			},
			wantErr:     "read new primary standby GTID set before repointing",
			unparseable: true,
			wantFenced:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := newOldActive(handoffUUID+":1-100", handoffUUID+":1-100")
			candidate := &newPrimary{Checker: handoffCandidate(handoffUUID + ":1-100").Checker()}

			run := rollout{source: source, candidate: candidate, afterActiveUpdate: func() {
				source.restart()
				tc.fail(source, candidate)
			}}.run(t)

			if run.promoted != "standby" {
				t.Fatalf("promoted = %q, want the handoff to have happened", run.promoted)
			}
			if run.err == nil || !strings.Contains(run.err.Error(), tc.wantErr) {
				t.Fatalf("ExecuteTargets() error = %v, want %q", run.err, tc.wantErr)
			}
			if tc.unparseable && !errors.Is(run.err, errUnparseableGTID) {
				t.Errorf("error = %v, want it to wrap errUnparseableGTID", run.err)
			}
			assertOldActiveNotRepointed(t, run, source, tc.wantFenced)
		})
	}
}

func assertOldActiveNotRepointed(t *testing.T, run handoffRun, source *oldActive, wantFenced bool) {
	t.Helper()
	for _, c := range source.GetCalls() {
		if c == "StopReplica" || c == "ResetReplicaAll" || c == "ChangeReplicationSource" || c == "StartReplica" {
			t.Fatalf("old active was reconfigured (%s) without proof the new primary holds its transactions; calls %v", c, source.GetCalls())
		}
	}
	if !source.IsReadOnlyVal() {
		t.Fatal("old active is writable after the handoff")
	}
	if wantFenced && !source.refencedSinceRestart() {
		t.Errorf("refused old active was left without super_read_only after its restart; role calls %v", roleCalls(source.GetCalls()))
	}
	if slices.Contains(run.processed, "primary") {
		t.Fatalf("old active was reported as processed: processed=%v", run.processed)
	}
}
