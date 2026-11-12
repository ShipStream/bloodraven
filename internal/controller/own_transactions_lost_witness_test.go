package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// A report that rests on a witness: a site lacks transactions it issued
// that another site holds and the primary does not.

type witnessFixture struct {
	tm            *TopologyManager
	clk           *clock.FakeClock
	dc1, dc2, dc3 *lossServer
	logs          *bytes.Buffer
}

// newWitnessFixture is dc1 (the primary, lossUUIDNew), dc2 (a replicating
// primary-candidate, lossUUIDOld) and dc3 (a replicating disaster-recovery
// site), each with the given executed set.
func newWitnessFixture(t *testing.T, dc1Set, dc2Set, dc3Set string, priorities ...string) *witnessFixture {
	t.Helper()
	replica := func(uuid, set string) *lossServer {
		return &lossServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: set,
			replicaStatusVal: &mysql.ReplicaStatus{SourceHost: "mysql-dc1", IORunning: true, SQLRunning: true}}, uuid: uuid}
	}
	f := &witnessFixture{logs: &bytes.Buffer{}}
	f.dc1 = &lossServer{mockMySQL: &mockMySQL{readOnly: false, gtidExecuted: dc1Set}, uuid: lossUUIDNew}
	f.dc2 = replica(lossUUIDOld, dc2Set)
	f.dc3 = replica(lossUUIDThird, dc3Set)
	f.dc2.donor, f.dc2.cloneUUID = f.dc1, lossUUIDRe
	f.dc3.donor, f.dc3.cloneUUID = f.dc1, "66666666-0000-11f1-8000-0000000000f6"
	f.clk = clock.NewFakeClock(time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC))
	cfg := testTopologyConfig()
	cfg.SitePriorities = priorities
	cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3",
		Role: state.SiteRoleDROnly, TaintSelector: taintSelector("dc3"), Host: "mysql-dc3"})
	f.tm = NewTopologyManagerWithClock(cfg, []mysql.Checker{f.dc1, f.dc2, f.dc3}, NewFailoverController(testLogger()), nil,
		NewBootstrapController(testLogger()), BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger(), f.clk)
	f.tm.failoverCooldown = 0
	f.tm.logger = slog.New(slog.NewJSONHandler(f.logs, nil))
	t.Cleanup(func() {
		for _, site := range []string{"dc1", "dc2", "dc3"} {
			metrics.OwnTransactionsLost.WithLabelValues(site).Set(0)
			metrics.DivergentTransactions.WithLabelValues(site).Set(0)
		}
	})
	return f
}

// poll polls n times, a second apart, and fails if dc2 is cloned.
func (f *witnessFixture) poll(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		f.tm.Poll(context.Background())
		f.clk.Advance(time.Second)
		if _, _, clones, _ := f.dc2.counts(); clones > 0 {
			t.Fatalf("poll %d: dc2 was cloned", i)
		}
	}
}

func (f *witnessFixture) logged(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(f.logs.String()), "\n") {
		var rec map[string]any
		if line == "" || json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

func setGTID(s *lossServer, gtid string) {
	s.mockMySQL.mu.Lock()
	s.mockMySQL.gtidExecuted = gtid
	s.mockMySQL.mu.Unlock()
}

const (
	witnessPrimarySet = lossUUIDOld + ":1-90," + lossUUIDNew + ":1-20"
	witnessHolderSet  = lossUUIDOld + ":1-100," + lossUUIDNew + ":1-20"
)

// dc2 was the primary, issued Old:91-100, and only dc3 received them. dc1
// was promoted without them, and dc2 came back without them too (its
// volume came back older), so against the primary it lacks nothing and is
// rejoined. But made writable again it would issue Old:91-100 a second
// time, while dc3 holds the first ones. The audit that finds dc3 divergent
// finds that too: dc2 gets a report that rests on dc3. It keeps
// replicating and is not recloned, and nothing promotes it.
func TestOwnLoss_AuditFindsASiteThatLacksWhatADivergentFollowerHolds(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.poll(t, 6)

	if got := recoveryOf(f.tm, "dc3"); got.state != v1alpha1.RecoveryStateBlocked || got.divergentGtid != lossLostGTID {
		t.Fatalf("dc3 recovery = %+v, want RecoveryBlocked for %s", got, lossLostGTID)
	}
	rec := ownLossOf(f.tm, "dc2")
	if rec == nil || !rec.witness || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc3" || rec.detectedBy != divergenceDetectedByAudit {
		t.Fatalf("dc2 own-loss record = %+v, want a witness report of %s against dc3 by the audit", rec, lossLostGTID)
	}
	warns := f.logged(t, "own transactions lost")
	if len(warns) != 1 || warns[0]["site"] != "dc2" || warns[0]["comparedTo"] != "dc3" ||
		!strings.Contains(warns[0]["remedy"].(string), "reclone dc3 to discard them, or replay them onto the primary (this site then receives them by replication)") {
		t.Fatalf("own transactions lost log = %v, want one for dc2 against dc3 with the witness remedy", warns)
	}

	// Not promoted by anything that asks.
	if err := f.tm.liveHandoffRefusal("dc2")(); err == nil || !strings.Contains(err.Error(), "lacks transactions it issued itself") {
		t.Fatalf("handoff refusal = %v, want dc2 refused", err)
	}
	snap := f.tm.buildSnapshot(nil)
	if s := snap.Sites[1]; s.RecoveryState != v1alpha1.RecoveryStateRecloneRequired || s.LostGtid != lossLostGTID || !s.LostToWitness || s.LostComparedTo != "dc3" {
		t.Fatalf("snapshot site = %+v, want RecloneRequired, lost to the witness dc3", s)
	}
	// Still a replica: its replication may be started, and no reclone is
	// asked for.
	if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); err != nil {
		t.Fatalf("replication start gate = %v, want dc2 admitted: it holds everything the primary does", err)
	}
	f.tm.mu.RLock()
	pending := f.tm.reclonePendingSite
	f.tm.mu.RUnlock()
	if pending != "" || !strings.Contains(rec.recloneWaiting, "held by dc3, not by the primary") {
		t.Fatalf("pending reclone %q, waiting %q: want none, waiting for dc3", pending, rec.recloneWaiting)
	}
	if f.tm.verifyOwnLossReclone(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2")) {
		t.Fatal("the check before the clone let a witness report clone the site")
	}

	// A clean comparison with the primary (the audit of dc2 itself) does
	// not clear it: dc3 still holds the transactions.
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 3)
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || !rec.witness {
		t.Fatalf("dc2 own-loss record after its audit = %+v, want it kept", rec)
	}

	// It ends when the witness no longer holds them (dc3 was recloned).
	setGTID(f.dc3, witnessPrimarySet)
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 3)
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("dc2 own-loss record = %+v after dc3 lost the transactions, want none", rec)
	}
	if got := f.logged(t, "own transactions lost cleared: no site holds transactions the site lacks"); len(got) != 1 {
		t.Fatalf("witness clear logged %d times, want once", len(got))
	}
	if err := f.tm.liveHandoffRefusal("dc2")(); err != nil {
		t.Fatalf("handoff refusal after the report ended = %v", err)
	}
}

// The transactions a witness holds reach the primary, and nothing can tell
// whether replication will deliver them to the site: the checkers of this
// fixture read no binary log coordinates, as for a primary whose position
// cannot be read. The single comparison then stands, on the safe side: the
// site lacks transactions the primary holds, which is the stronger report.
// Its replication is refused and it is recloned. What a real server does
// when an administrator replays them onto the primary, and a site whose
// channel runs receives them, is pinned by
// TestOwnLoss_WitnessReportEndsWhenTheTransactionsAreReplayedOntoThePrimary.
func TestOwnLoss_WitnessReportBecomesAHoleWithoutAProofOfLag(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	// The clone the hole leads to is held at its keyring unseal, so the
	// report can be looked at.
	f.tm.SetKeyringGate(&testKeyringGate{})
	f.poll(t, 6)
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || !rec.witness {
		t.Fatalf("dc2 own-loss record = %+v, want a witness report", rec)
	}
	setGTID(f.dc1, witnessHolderSet)
	f.clk.Advance(gtidAuditInterval)
	f.tm.Poll(context.Background())
	rec := ownLossOf(f.tm, "dc2")
	if rec == nil || rec.witness || rec.comparedTo != "dc1" || rec.lostGtid != lossLostGTID {
		t.Fatalf("dc2 own-loss record = %+v, want a report against the primary dc1", rec)
	}
	if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); !errors.Is(err, errOwnTransactionsLost) {
		t.Fatalf("replication start gate = %v, want errOwnTransactionsLost", err)
	}
	// A witness that holds nothing more does not end it.
	f.tm.mu.Lock()
	changed, _ := f.tm.settleOwnLossLocked("dc2", mysql.GTIDSet{}, "dc3", divergenceDetectedByAudit, nil, true)
	f.tm.mu.Unlock()
	if changed || ownLossOf(f.tm, "dc2") == nil {
		t.Fatal("a clean comparison with a witness cleared a report against the primary")
	}
}

// What ends a witness report without the witness's set, and what does
// not.
func TestOwnLoss_WitnessReportReview(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(f *witnessFixture)
		kept    bool
	}{
		{"the witness does not answer: kept", func(f *witnessFixture) {
			f.dc3.mockMySQL.mu.Lock()
			f.dc3.mockMySQL.gtidExecutedErr = errors.New("connection refused")
			f.dc3.mockMySQL.mu.Unlock()
		}, true},
		{"the site does not answer: kept", func(f *witnessFixture) {
			f.dc2.lmu.Lock()
			f.dc2.snapErr = errors.New("connection refused")
			f.dc2.lmu.Unlock()
		}, true},
		{"an in-place restore queued the witness: ended", func(f *witnessFixture) { f.tm.SetRestoreReclonePending([]string{"dc3"}) }, false},
		{"the site was cloned: ended", func(f *witnessFixture) { f.tm.clearOwnLossAfterClone("dc2") }, false},
		{"nothing changed: kept", func(*witnessFixture) {}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
			f.poll(t, 6)
			if rec := ownLossOf(f.tm, "dc2"); rec == nil || !rec.witness {
				t.Fatalf("dc2 own-loss record = %+v, want a witness report", rec)
			}
			tc.arrange(f)
			f.clk.Advance(gtidAuditInterval)
			f.tm.reviewOwnLossReports(context.Background())
			if rec := ownLossOf(f.tm, "dc2"); (rec != nil) != tc.kept {
				t.Fatalf("dc2 own-loss record = %+v, kept=%v wanted", rec, tc.kept)
			}
		})
	}
}

// A divergence that consists of the divergent site's own transactions, or
// of an identity a clone superseded, costs no read and makes no report.
func TestOwnLoss_CrossCheckOnlyForLiveIdentities(t *testing.T) {
	// dc3 holds transactions of its own the primary lacks.
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet+","+lossUUIDThird+":1-5")
	f.poll(t, 6)
	if got := recoveryOf(f.tm, "dc3").state; got != v1alpha1.RecoveryStateBlocked {
		t.Fatalf("dc3 recovery state = %q, want RecoveryBlocked", got)
	}
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("dc2 own-loss record = %+v, want none: dc3's extras are its own", rec)
	}

	// dc2 was recloned since it issued Old:91-100 (it runs under Re now):
	// it gave them up at the clone.
	g := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	g.dc2.lmu.Lock()
	g.dc2.uuid = lossUUIDRe
	g.dc2.lmu.Unlock()
	g.tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
	g.tm.markSiteCloned(context.Background(), "dc2")
	g.poll(t, 6)
	if got := recoveryOf(g.tm, "dc3").state; got != v1alpha1.RecoveryStateBlocked {
		t.Fatalf("dc3 recovery state = %q, want RecoveryBlocked", got)
	}
	if rec := ownLossOf(g.tm, "dc2"); rec != nil {
		t.Fatalf("dc2 own-loss record = %+v, want none: a clone superseded the identity", rec)
	}
}

// A site with an own-loss report that turns up writable beside the
// primary is fenced, whatever the split-brain policy prefers: it never
// wins, and it is not left issuing transactions. With every writable site
// reported nothing is fenced.
func TestOwnLoss_WritableReportedSiteIsFencedInASplitBrain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		priorities []string
	}{
		{"the policy prefers the reported site", []string{"dc2", "dc1"}},
		{"the policy prefers the other site", []string{"dc1", "dc2"}},
		{"no policy", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet, tc.priorities...)
			f.poll(t, 3)
			// dc2 carries a report and is made writable by someone else.
			f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
			f.dc2.mockMySQL.mu.Lock()
			f.dc2.mockMySQL.readOnly, f.dc2.mockMySQL.superReadOnly = false, false
			f.dc2.mockMySQL.mu.Unlock()
			f.poll(t, 6)
			if siteWritable(f.dc2) {
				t.Fatal("dc2 was left writable beside dc1 although it carries an own-loss report")
			}
			if !siteWritable(f.dc1) {
				t.Fatal("dc1 was fenced: a site with an own-loss report won the split brain")
			}
			if got := f.logged(t, "split-brain: fencing a writable site that lacks transactions it issued itself"); len(got) == 0 || got[0]["site"] != "dc2" {
				t.Fatalf("fence log = %v, want one for dc2", got)
			}
		})
	}

	// Every writable site carries a report: nothing to prefer.
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet, "dc2", "dc1")
	f.poll(t, 3)
	f.dc2.mockMySQL.mu.Lock()
	f.dc2.mockMySQL.readOnly, f.dc2.mockMySQL.superReadOnly = false, false
	f.dc2.mockMySQL.mu.Unlock()
	f.tm.mu.Lock()
	f.tm.sites[1].state = state.StateWritable
	f.tm.ownLoss = map[string]*ownLossRecord{
		"dc1": {lostGtid: lossUUIDNew + ":21-22", lostCount: 2, witness: true, comparedTo: "dc3", detectedBy: divergenceDetectedByAudit},
		"dc2": {lostGtid: lossLostGTID, lostCount: 10, witness: true, comparedTo: "dc3", detectedBy: divergenceDetectedByAudit},
	}
	f.tm.mu.Unlock()
	if sound, attempted := f.tm.fenceWritableOwnLossSites(context.Background(), false); attempted || len(sound) != 0 {
		t.Fatalf("fenceWritableOwnLossSites = %v, %v: want nothing fenced when every writable site carries a report", sound, attempted)
	}
	if !siteWritable(f.dc1) || !siteWritable(f.dc2) {
		t.Fatal("a site was fenced although no writable site is without a report")
	}
}
