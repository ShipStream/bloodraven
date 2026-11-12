package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// A hole or only lag (own_transactions_lost.go, "A hole, or only lag?").
//
// Measured on MySQL 9.7 (two servers, mysqlbinlog 8.4): transactions B
// issued, decoded from B's binary log and applied on A, keep B's
// server_uuid and are logged on A with A's server_id; B, replicating from
// A, receives all of them. While the replay ran, B lacked transactions
// under its own server_uuid that A held at every comparison made in the
// audit's read order. With a real hole (transactions A received from B by
// replication, which carry B's server_id) B's Exec_Source_Log_Pos reaches
// A's position while the transactions stay missing; with a delayed or
// stopped applier it stays behind until they are applied.
//
// posServer models the part of that the audit reads: a primary that
// reports its binary log coordinates with its executed set.
type posServer struct {
	*lossServer
	pmu    sync.Mutex
	file   string
	pos    uint64
	posErr error
}

func (p *posServer) ReadBinlogPosition(context.Context) (mysql.BinlogPosition, error) {
	p.pmu.Lock()
	file, pos, err := p.file, p.pos, p.posErr
	p.pmu.Unlock()
	if err != nil {
		return mysql.BinlogPosition{}, err
	}
	p.mockMySQL.mu.Lock()
	defer p.mockMySQL.mu.Unlock()
	return mysql.BinlogPosition{File: file, Pos: pos, GtidExecuted: p.mockMySQL.gtidExecuted}, nil
}

// commit makes the primary hold set at binary log position pos.
func (p *posServer) commit(set string, pos uint64) {
	p.pmu.Lock()
	p.pos = pos
	p.pmu.Unlock()
	p.mockMySQL.mu.Lock()
	p.mockMySQL.gtidExecuted = set
	p.mockMySQL.mu.Unlock()
}

type lagFixture struct {
	tm      *TopologyManager
	clk     *clock.FakeClock
	primary *posServer
	site    *lossServer
	logs    *bytes.Buffer
}

const (
	lagFile = "mysql-bin.000007"
	// The primary holds the site's ten transactions and twenty of its own.
	lagPrimarySet = lossUUIDNew + ":1-20," + lossUUIDOld + ":1-100"
	// The site lacks its last ten.
	lagSiteSet = lossUUIDNew + ":1-20," + lossUUIDOld + ":1-90"
)

// newLagFixture is dc1, the primary at lagFile:5000, and dc2, a read-only
// site whose channel from dc1 runs and whose applier stands at execPos.
func newLagFixture(t *testing.T, primarySet, siteSet string, execPos uint64) *lagFixture {
	t.Helper()
	f := &lagFixture{logs: &bytes.Buffer{}}
	f.primary = &posServer{lossServer: &lossServer{mockMySQL: &mockMySQL{readOnly: false, gtidExecuted: primarySet}, uuid: lossUUIDNew},
		file: lagFile, pos: 5000}
	f.site = &lossServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: siteSet}, uuid: lossUUIDOld,
		donor: f.primary.lossServer, cloneUUID: lossUUIDRe}
	f.setApplier(execPos, true)
	f.clk = clock.NewFakeClock(time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC))
	f.tm = NewTopologyManagerWithClock(testTopologyConfig(), []mysql.Checker{f.primary, f.site}, NewFailoverController(testLogger()), nil,
		NewBootstrapController(testLogger()), BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger(), f.clk)
	f.tm.failoverCooldown = 0
	f.tm.logger = slog.New(slog.NewJSONHandler(f.logs, nil))
	t.Cleanup(func() {
		metrics.OwnTransactionsLost.WithLabelValues("dc2").Set(0)
		metrics.DivergentTransactions.WithLabelValues("dc2").Set(0)
	})
	return f
}

// setApplier sets the site's channel: its applier's position in the
// primary's log, and whether both threads run.
func (f *lagFixture) setApplier(execPos uint64, running bool) {
	f.site.mockMySQL.mu.Lock()
	f.site.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1", SourceUUID: lossUUIDNew,
		IORunning: running, SQLRunning: running, ExecSourceLogFile: lagFile, ExecSourceLogPos: execPos}
	f.site.mockMySQL.mu.Unlock()
}

func (f *lagFixture) setSiteSet(set string) {
	f.site.mockMySQL.mu.Lock()
	f.site.mockMySQL.gtidExecuted = set
	f.site.mockMySQL.mu.Unlock()
}

// poll polls for d of the fixture's clock, once a second.
func (f *lagFixture) poll(d time.Duration) {
	for end := f.clk.Now().Add(d); f.clk.Now().Before(end); f.clk.Advance(time.Second) {
		f.tm.Poll(context.Background())
	}
}

func (f *lagFixture) logged(msgPrefix string) int {
	n := 0
	for _, line := range strings.Split(f.logs.String(), "\n") {
		if strings.Contains(line, `"msg":"`+msgPrefix) {
			n++
		}
	}
	return n
}

const lagNotReportedMsg = "replica GTID audit: the follower lacks transactions under its own server_uuid that the primary holds, and its applier has not executed"

// A replay onto the primary of transactions the follower issued: the
// follower is behind, its applier has not reached the primary's position,
// and what it lacks is on its way. Nothing is reported, nothing is cloned,
// and when it has applied them there was never a report.
func TestOwnLossLag_AReplayOnThePrimaryIsNotAHole(t *testing.T) {
	f := newLagFixture(t, lagPrimarySet, lagSiteSet, 4000)
	f.poll(90 * time.Second)
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("own-loss record = %+v: a follower that is only behind was reported", rec)
	}
	if _, _, clones, _ := f.site.counts(); clones != 0 {
		t.Fatal("a follower that is only behind was cloned")
	}
	if got := f.logged(lagNotReportedMsg); got != 1 {
		t.Fatalf("the unproven deficit was logged %d times, want once", got)
	}
	// Audited again well before the regular interval while it is unproven.
	f.tm.mu.RLock()
	next := f.tm.gtidAudit["dc2"].nextAt.Sub(f.clk.Now())
	f.tm.mu.RUnlock()
	if next > ownLossProofRetry {
		t.Fatalf("next audit in %s, want within %s while a deficit is unproven", next, ownLossProofRetry)
	}

	// The follower applies them.
	f.setSiteSet(lagPrimarySet)
	f.setApplier(5000, true)
	f.poll(40 * time.Second)
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("own-loss record = %+v after the follower caught up", rec)
	}
	f.tm.mu.RLock()
	proof := f.tm.ownLossProof["dc2"]
	f.tm.mu.RUnlock()
	if proof != nil {
		t.Fatalf("the primary's snapshot %+v is still kept for a follower that lacks nothing", proof)
	}
}

// The replay goes on: the follower passes the first snapshot holding all
// of it, and lacks what the primary took since. Still lag.
func TestOwnLossLag_AContinuingReplayIsJudgedAgainstANewSnapshot(t *testing.T) {
	f := newLagFixture(t, lossUUIDNew+":1-20,"+lossUUIDOld+":1-95", lagSiteSet, 4000)
	f.poll(10 * time.Second)
	// The primary takes five more; the follower has applied the first five.
	f.primary.commit(lagPrimarySet, 6000)
	f.setSiteSet(lossUUIDNew + ":1-20," + lossUUIDOld + ":1-95")
	f.setApplier(5000, true)
	f.poll(20 * time.Second)
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("own-loss record = %+v: the follower holds everything of the snapshot it passed", rec)
	}
	f.tm.mu.RLock()
	proof := f.tm.ownLossProof["dc2"]
	f.tm.mu.RUnlock()
	if proof == nil || proof.pos.Pos != 6000 {
		t.Fatalf("snapshot = %+v, want a new one at the primary's position 6000", proof)
	}
}

// A hole: the follower's applier has executed the primary's log past the
// position at which the primary held the transactions, and they are still
// missing. Its receiver dropped them.
func TestOwnLossLag_AHoleIsReportedOnceTheApplierHasPassed(t *testing.T) {
	t.Run("caught up: reported by the first audit", func(t *testing.T) {
		f := newLagFixture(t, lagPrimarySet, lagSiteSet, 5000)
		f.tm.SetKeyringGate(&testKeyringGate{}) // holds the clone, so the report can be read
		f.poll(3 * time.Second)
		rec := ownLossOf(f.tm, "dc2")
		if rec == nil || rec.witness || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc1" {
			t.Fatalf("own-loss record = %+v, want a hole of %s against dc1", rec, lossLostGTID)
		}
		if got := f.logged(lagNotReportedMsg); got != 0 {
			t.Fatalf("a caught-up follower's hole was first called lag (%d lines)", got)
		}
	})
	t.Run("behind, then past the snapshot without them", func(t *testing.T) {
		f := newLagFixture(t, lagPrimarySet, lagSiteSet, 4000)
		f.tm.SetKeyringGate(&testKeyringGate{})
		f.poll(20 * time.Second)
		if rec := ownLossOf(f.tm, "dc2"); rec != nil {
			t.Fatalf("own-loss record = %+v before the applier passed the snapshot", rec)
		}
		// The primary commits one more of its own; the follower applies it
		// and its position passes the snapshot's.
		f.primary.commit(lossUUIDNew+":1-21,"+lossUUIDOld+":1-100", 5300)
		f.setSiteSet(lossUUIDNew + ":1-21," + lossUUIDOld + ":1-90")
		f.setApplier(5300, true)
		f.poll(10 * time.Second)
		rec := ownLossOf(f.tm, "dc2")
		if rec == nil || rec.witness || rec.lostGtid != lossLostGTID {
			t.Fatalf("own-loss record = %+v, want the hole %s", rec, lossLostGTID)
		}
	})
	t.Run("the reclone runs on the proof", func(t *testing.T) {
		f := newLagFixture(t, lagPrimarySet, lagSiteSet, 5000)
		for i := 0; i < 40; i++ {
			f.tm.Poll(context.Background())
			f.clk.Advance(time.Second)
			if _, _, clones, _ := f.site.counts(); clones > 0 {
				return
			}
			time.Sleep(5 * time.Millisecond) // the bootstrap goroutine
		}
		t.Fatalf("the site with a proven hole was not cloned (report %+v)", ownLossOf(f.tm, "dc2"))
	})
}

// The check before the clone asks the same question: a recipient whose
// channel runs and whose applier is behind the primary's snapshot is not
// cloned over, whatever the report says.
func TestOwnLossLag_TheRecloneWaitsForTheProof(t *testing.T) {
	f := newLagFixture(t, lagPrimarySet, lagSiteSet, 4000)
	f.poll(3 * time.Second)
	f.tm.mu.Lock()
	f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1",
		detectedBy: divergenceDetectedByAudit}}
	f.tm.mu.Unlock()
	if f.tm.verifyOwnLossReclone(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2")) {
		t.Fatal("the clone was allowed although the recipient's applier has not passed the primary's snapshot")
	}
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || !strings.Contains(rec.recloneWaiting, "may be replication lag") {
		t.Fatalf("own-loss record = %+v, want the refusal to say why", rec)
	}
	// Past the snapshot and still without them: the clone may run.
	f.setApplier(5000, true)
	if !f.tm.verifyOwnLossReclone(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2")) {
		t.Fatalf("the clone was refused on a proven hole (%+v)", ownLossOf(f.tm, "dc2"))
	}
	// Past the snapshot holding everything: the report ends, no clone.
	f.setSiteSet(lagPrimarySet)
	if f.tm.verifyOwnLossReclone(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2")) {
		t.Fatal("the clone was allowed for a site that holds everything")
	}
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("own-loss record = %+v, want it cleared", rec)
	}
}

// Where the proof by position does not apply, the single comparison
// stands, as for a site without a channel.
func TestOwnLossLag_WithoutARunningChannelTheComparisonStands(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(f *lagFixture)
		warn    string
	}{
		{"the channel's threads are stopped", func(f *lagFixture) { f.setApplier(4000, false) }, ""},
		{"the channel points at another host", func(f *lagFixture) {
			f.site.mockMySQL.mu.Lock()
			f.site.mockMySQL.replicaStatusVal.SourceHost = "mysql-elsewhere"
			f.site.mockMySQL.mu.Unlock()
		}, ""},
		{"the primary's position cannot be read", func(f *lagFixture) {
			f.primary.pmu.Lock()
			f.primary.posErr = errors.New("Access denied; you need the REPLICATION CLIENT privilege")
			f.primary.pmu.Unlock()
		}, "own transactions lost: the primary's binary log position could not be read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLagFixture(t, lagPrimarySet, lagSiteSet, 4000)
			tc.arrange(f)
			lost := mustGTIDSet(t, lossLostGTID)
			hole, proven := f.tm.proveOwnLossHole(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2"), lossUUIDNew, lossUUIDOld,
				[]string{lossUUIDOld}, lost)
			if !proven || hole.String() != lossLostGTID {
				t.Fatalf("hole = %q proven = %v, want the comparison's own result", hole.String(), proven)
			}
			if tc.warn != "" && f.logged(tc.warn) != 1 {
				t.Fatalf("log %q missing", tc.warn)
			}
		})
	}
}

// What is not a proof: a receiver that still names the primary's
// predecessor, a site that answers as another server, a status that
// cannot be read.
func TestOwnLossLag_Unproven(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(f *lagFixture)
	}{
		{"the receiver names another server under the primary's host", func(f *lagFixture) {
			f.site.mockMySQL.mu.Lock()
			f.site.mockMySQL.replicaStatusVal.SourceUUID = lossUUIDThird
			f.site.mockMySQL.mu.Unlock()
		}},
		{"the site answers as another server", func(f *lagFixture) {
			f.site.lmu.Lock()
			f.site.uuid = lossUUIDRe
			f.site.lmu.Unlock()
		}},
		{"the site's replica status cannot be read", func(f *lagFixture) {
			f.site.mockMySQL.mu.Lock()
			f.site.mockMySQL.replicaStatusErr = errors.New("connection refused")
			f.site.mockMySQL.mu.Unlock()
		}},
		{"the applier reports a position in another log", func(f *lagFixture) {
			f.site.mockMySQL.mu.Lock()
			f.site.mockMySQL.replicaStatusVal.ExecSourceLogFile = "binlog.000900"
			f.site.mockMySQL.mu.Unlock()
		}},
		{"the applier reports no position", func(f *lagFixture) {
			f.site.mockMySQL.mu.Lock()
			f.site.mockMySQL.replicaStatusVal.ExecSourceLogFile = ""
			f.site.mockMySQL.mu.Unlock()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The applier's position is past the snapshot: only the
			// arranged fault stands between the deficit and a hole.
			f := newLagFixture(t, lagPrimarySet, lagSiteSet, 9000)
			tc.arrange(f)
			hole, proven := f.tm.proveOwnLossHole(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2"), lossUUIDNew, lossUUIDOld,
				[]string{lossUUIDOld}, mustGTIDSet(t, lossLostGTID))
			if proven {
				t.Fatalf("proven with hole %q", hole.String())
			}
		})
	}
}

// A snapshot beyond the primary's log as it is now (the log was reset)
// can never be passed: it is dropped and the next look takes another.
func TestOwnLossLag_ASnapshotBeyondTheLogIsDropped(t *testing.T) {
	f := newLagFixture(t, lagPrimarySet, lagSiteSet, 4000)
	prove := func() (mysql.GTIDSet, bool) {
		return f.tm.proveOwnLossHole(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2"), lossUUIDNew, lossUUIDOld,
			[]string{lossUUIDOld}, mustGTIDSet(t, lossLostGTID))
	}
	if _, proven := prove(); proven {
		t.Fatal("proven while the applier is behind")
	}
	// The primary's log starts again.
	f.primary.pmu.Lock()
	f.primary.file, f.primary.pos = "mysql-bin.000001", 900
	f.primary.pmu.Unlock()
	f.site.mockMySQL.mu.Lock()
	f.site.mockMySQL.replicaStatusVal.ExecSourceLogFile, f.site.mockMySQL.replicaStatusVal.ExecSourceLogPos = "mysql-bin.000001", 900
	f.site.mockMySQL.mu.Unlock()
	if _, proven := prove(); proven {
		t.Fatal("proven against a snapshot the applier never passed")
	}
	hole, proven := prove()
	if !proven || hole.String() != lossLostGTID {
		t.Fatalf("hole = %q proven = %v, want the hole proven against a snapshot of the new log", hole.String(), proven)
	}
}

func mustGTIDSet(t *testing.T, raw string) mysql.GTIDSet {
	t.Helper()
	set, err := mysql.ParseGTIDSet(raw)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestReplicaStatus_ExecutedPast(t *testing.T) {
	at := mysql.BinlogPosition{File: "mysql-bin.000007", Pos: 5000}
	for _, tc := range []struct {
		name string
		file string
		pos  uint64
		want bool
	}{
		{"behind in the same file", "mysql-bin.000007", 4999, false},
		{"at the position", "mysql-bin.000007", 5000, true},
		{"beyond it", "mysql-bin.000007", 5001, true},
		{"a later file", "mysql-bin.000008", 4, true},
		{"an earlier file", "mysql-bin.000006", 999999, false},
		{"another log", "binlog.000009", 999999, false},
		{"no position", "", 0, false},
		{"a name without a number", "mysql-bin", 999999, false},
	} {
		rs := &mysql.ReplicaStatus{ExecSourceLogFile: tc.file, ExecSourceLogPos: tc.pos}
		if got := rs.ExecutedPast(at); got != tc.want {
			t.Errorf("%s: ExecutedPast = %v, want %v", tc.name, got, tc.want)
		}
	}
	if (*mysql.ReplicaStatus)(nil).ExecutedPast(at) {
		t.Error("a nil status has executed past a position")
	}
	if (&mysql.ReplicaStatus{ExecSourceLogFile: "mysql-bin.000007", ExecSourceLogPos: 1}).ExecutedPast(mysql.BinlogPosition{}) {
		t.Error("executed past an unknown position")
	}
}

// The witness case with real servers. dc3 holds Old:91-100, which dc2
// issued and lost and the primary dc1 never had: dc3 is divergent, dc2
// carries a witness report. An administrator keeps the transactions: they
// are decoded from dc3 and replayed onto dc1. Measured on MySQL 9.7: dc1
// logs them with its own server_id, so dc2, whose channel runs, receives
// them like any other transaction. While dc2 is behind it is not reported
// against the primary and not cloned; once it has applied them no site
// holds anything it lacks, and its report ends.
func TestOwnLoss_WitnessReportEndsWhenTheTransactionsAreReplayedOntoThePrimary(t *testing.T) {
	replica := func(uuid, set string) *lossServer {
		return &lossServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: set,
			replicaStatusVal: &mysql.ReplicaStatus{SourceHost: "mysql-dc1", SourceUUID: lossUUIDNew, IORunning: true, SQLRunning: true,
				ExecSourceLogFile: lagFile, ExecSourceLogPos: 5000}}, uuid: uuid}
	}
	dc1 := &posServer{lossServer: &lossServer{mockMySQL: &mockMySQL{readOnly: false, gtidExecuted: witnessPrimarySet}, uuid: lossUUIDNew},
		file: lagFile, pos: 5000}
	dc2, dc3 := replica(lossUUIDOld, witnessPrimarySet), replica(lossUUIDThird, witnessHolderSet)
	dc2.donor, dc2.cloneUUID = dc1.lossServer, lossUUIDRe
	clk := clock.NewFakeClock(time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC))
	cfg := testTopologyConfig()
	cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3",
		Role: state.SiteRoleDROnly, TaintSelector: taintSelector("dc3"), Host: "mysql-dc3"})
	logs := &bytes.Buffer{}
	tm := NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2, dc3}, NewFailoverController(testLogger()), nil,
		NewBootstrapController(testLogger()), BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger(), clk)
	tm.failoverCooldown = 0
	tm.logger = slog.New(slog.NewJSONHandler(logs, nil))
	t.Cleanup(func() {
		for _, site := range []string{"dc1", "dc2", "dc3"} {
			metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
			metrics.DivergentTransactions.WithLabelValues(site).Set(0)
		}
	})
	poll := func(d time.Duration) {
		t.Helper()
		for end := clk.Now().Add(d); clk.Now().Before(end); clk.Advance(time.Second) {
			tm.Poll(context.Background())
			if _, _, clones, _ := dc2.counts(); clones > 0 {
				t.Fatal("dc2 was cloned")
			}
		}
	}

	poll(6 * time.Second)
	if rec := ownLossOf(tm, "dc2"); rec == nil || !rec.witness || rec.comparedTo != "dc3" {
		t.Fatalf("dc2 own-loss record = %+v, want a witness report against dc3", rec)
	}

	// The replay lands on the primary. dc2 has not applied it yet.
	dc1.commit(witnessHolderSet, 6000)
	poll(2 * gtidAuditInterval)
	rec := ownLossOf(tm, "dc2")
	if rec == nil || !rec.witness {
		t.Fatalf("dc2 own-loss record = %+v while it is behind: want the witness report kept, and no report against the primary", rec)
	}
	if err := tm.withReplicaStartGate("dc2", func() error { return nil }); err != nil {
		t.Fatalf("replication start gate = %v: a site that is only behind was refused", err)
	}
	if got := recoveryOf(tm, "dc3").state; got != "" {
		t.Fatalf("dc3 recovery state = %q, want its divergence cleared: the primary holds the transactions now", got)
	}

	// dc2 applies them.
	setGTID(dc2, witnessHolderSet)
	dc2.mockMySQL.mu.Lock()
	dc2.mockMySQL.replicaStatusVal.ExecSourceLogPos = 6000
	dc2.mockMySQL.mu.Unlock()
	poll(2 * gtidAuditInterval)
	if rec := ownLossOf(tm, "dc2"); rec != nil {
		t.Fatalf("dc2 own-loss record = %+v after it applied the replayed transactions, want none", rec)
	}
	if err := tm.liveHandoffRefusal("dc2")(); err != nil {
		t.Fatalf("handoff refusal after the report ended = %v", err)
	}
}
