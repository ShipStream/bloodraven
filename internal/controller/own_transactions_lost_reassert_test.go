package controller

import (
	"context"
	"testing"
	"time"
)

// reassertGroup is dc1 and dc2, both read-only behind a fence, with dc2
// the recorded failover target: the state the primary re-assert heals.
func reassertGroup(t *testing.T, dc1GTID, dc2GTID string) *lossFixture {
	t.Helper()
	f := newLossFixture(t, dc1GTID, dc2GTID, false)
	f.primary.mockMySQL.readOnly, f.primary.mockMySQL.superReadOnly = true, true
	f.site.mockMySQL.superReadOnly = true
	pollN(f.tm, 3)
	f.tm.mu.Lock()
	f.tm.lastFailoverTarget = "dc2"
	f.tm.mu.Unlock()
	return f
}

// pollFor polls once a second for d of the fixture's clock.
func (f *lossFixture) pollFor(d time.Duration) {
	for end := f.clk.Now().Add(d); f.clk.Now().Before(end); f.clk.Advance(time.Second) {
		f.tm.Poll(context.Background())
	}
}

// TestOwnLoss_ReassertComparesAStandingReportAgain: with no writable site
// nothing but the primary re-assert compares the recorded primary with its
// peers, and the re-assert reads every one of them. A report that does not
// say what it rests on (one restored from status after an operator
// restart) is therefore settled by that comparison, like a witness's
// report is by its review: it stands while a peer holds transactions the
// target issued and lacks, and ends when none does. Left standing, it kept
// a complete primary read-only for good (the wedge of DST seed 39918,
// with an operator restart between the report and the repair).
func TestOwnLoss_ReassertComparesAStandingReportAgain(t *testing.T) {
	t.Run("a restored report on a target that is complete again", func(t *testing.T) {
		// dc2 came back without Old:91-100, was reported against dc1, and
		// then got them back (its newer volume was attached again). The
		// operator restarted in between.
		f := reassertGroup(t, lossUUIDOld+":1-100", lossUUIDOld+":1-100")
		f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
		f.pollFor(2 * time.Minute)
		if !siteWritable(f.site) {
			t.Fatalf("dc2 stayed read-only: it holds everything dc1 holds, and its report (%+v) was never compared again", f.loss())
		}
		if rec := f.loss(); rec != nil {
			t.Fatalf("own-loss record = %+v, want it cleared by the comparison", rec)
		}
		if got := len(f.records(t, "own transactions lost cleared: no site holds transactions the site lacks")); got != 1 {
			t.Fatalf("the clearing was logged %d times, want once", got)
		}
	})
	t.Run("a restored report on a target that still lacks them", func(t *testing.T) {
		f := reassertGroup(t, lossUUIDOld+":1-100", lossReturnGTID)
		f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
		f.pollFor(2 * time.Minute)
		if siteWritable(f.site) {
			t.Fatalf("dc2 was made writable although dc1 holds %s, which dc2 would issue again", lossLostGTID)
		}
		rec := f.loss()
		if rec == nil || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc1" || !rec.witness {
			t.Fatalf("own-loss record = %+v, want %s against the witness dc1", rec, lossLostGTID)
		}
	})
	t.Run("a report against the primary is not settled by the re-assert", func(t *testing.T) {
		// A comparison of this manager with the confirmed primary found the
		// hole. It ends by a clean comparison with a primary or a reclone,
		// not here: it was made under every server_uuid the site has had,
		// which no peer's set is evidence for once there is no primary.
		f := reassertGroup(t, lossUUIDOld+":1-100", lossUUIDOld+":1-100")
		f.tm.mu.Lock()
		f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1",
			detectedBy: divergenceDetectedByRecovery}}
		f.tm.mu.Unlock()
		f.pollFor(2 * time.Minute)
		if siteWritable(f.site) {
			t.Fatal("dc2 was made writable over a report made against the confirmed primary")
		}
		if f.loss() == nil {
			t.Fatal("the report against the primary was cleared without a comparison with a primary")
		}
	})
}
