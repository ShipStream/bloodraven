package controller

import (
	"context"
	"testing"
	"time"
)

// The primary re-assert carries two rules that were written apart: a
// source a planned failover left fenced is lifted from that fence only
// while it is the server the run's fence intent names
// (plannedNamedSourceIsFenceServer), and no target is made writable while
// a peer holds transactions it issued, under a server_uuid no clone has
// superseded, and lacks (own_transactions_lost.go). These tests pin how the
// two meet in checkPrimaryReassert: the first decides whether a fence may
// be lifted at all, the second is asked of every target that passes it.

// plannedNamedGroup is dc1 and dc2 read-only, with dc2 the source a
// planned run left fenced and named for the re-assert, bound to boundUUID.
// dc2 runs under lossUUIDOld unless a test replaces it.
func plannedNamedGroup(t *testing.T, dc1GTID, dc2GTID, boundUUID string) *lossFixture {
	t.Helper()
	f := newLossFixture(t, dc1GTID, dc2GTID, false)
	f.primary.mockMySQL.readOnly, f.primary.mockMySQL.superReadOnly = true, true
	f.site.mockMySQL.superReadOnly = true // the run's fence
	pollN(f.tm, 3)
	f.tm.mu.Lock()
	if !f.tm.nameFencedSourceLocked("run-1", "dc2", boundUUID, "") {
		t.Fatal("setup: the source was not named")
	}
	f.tm.mu.Unlock()
	return f
}

func TestReassert_PlannedNamedSourceAndOwnTransactions(t *testing.T) {
	ctx := context.Background()
	whole := lossUUIDOld + ":1-100"

	t.Run("the run's server, complete: its fence is lifted", func(t *testing.T) {
		f := plannedNamedGroup(t, whole, whole, lossUUIDOld)
		if !f.tm.checkPrimaryReassert(ctx) || !siteWritable(f.site) {
			t.Fatalf("re-assert did not run (writable=%v)", siteWritable(f.site))
		}
	})
	t.Run("the run's server on older data: a peer holds what it issued", func(t *testing.T) {
		f := plannedNamedGroup(t, whole, lossReturnGTID, lossUUIDOld)
		if f.tm.checkPrimaryReassert(ctx) || siteWritable(f.site) {
			t.Fatalf("the fence was lifted from a source that lacks %s, which dc1 holds", lossLostGTID)
		}
		rec := f.loss()
		if rec == nil || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc1" || !rec.witness {
			t.Fatalf("own-loss record = %+v, want %s against the witness dc1", rec, lossLostGTID)
		}
	})
	t.Run("the same with the peer's divergence recorded as that set", func(t *testing.T) {
		f := plannedNamedGroup(t, whole, lossReturnGTID, lossUUIDOld)
		f.tm.SetRecoveryBlockedFrom("dc1", lossLostGTID, 10, divergenceDetectedByAudit, "dc2")
		if f.tm.checkPrimaryReassert(ctx) || siteWritable(f.site) {
			t.Fatal("the peer's recorded divergence, which is the source's own lost tail, was set aside")
		}
	})
	t.Run("a standing report on the run's server is settled by the comparison with every peer", func(t *testing.T) {
		// Complete again: no peer holds what the report names, so it ends
		// and the fence is lifted.
		f := plannedNamedGroup(t, whole, whole, lossUUIDOld)
		f.tm.mu.Lock()
		f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1",
			detectedBy: divergenceDetectedByRecovery}}
		f.tm.mu.Unlock()
		if !f.tm.checkPrimaryReassert(ctx) || !siteWritable(f.site) || f.loss() != nil {
			t.Fatalf("writable=%v report=%+v, want the report ended and the fence lifted", siteWritable(f.site), f.loss())
		}
		// Still short: the report stands and refuses.
		g := plannedNamedGroup(t, whole, lossReturnGTID, lossUUIDOld)
		g.tm.mu.Lock()
		g.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1",
			detectedBy: divergenceDetectedByRecovery}}
		g.tm.mu.Unlock()
		if g.tm.checkPrimaryReassert(ctx) || siteWritable(g.site) || g.loss() == nil {
			t.Fatal("the fence was lifted from a source that lacks transactions it issued, which dc1 holds")
		}
	})

	// Another server answers in the source's place.
	t.Run("a replaced server behind a fence is left alone", func(t *testing.T) {
		f := plannedNamedGroup(t, whole, whole, lossUUIDRe) // the intent names another server
		if f.tm.checkPrimaryReassert(ctx) || siteWritable(f.site) {
			t.Fatal("a fence that is not the run's was lifted")
		}
		if f.loss() != nil {
			t.Fatalf("own-loss record = %+v, want none: the server is complete", f.loss())
		}
	})
	t.Run("a replaced server at mysqld's boot read_only, complete, is lifted like an observed primary", func(t *testing.T) {
		f := plannedNamedGroup(t, whole, whole, lossUUIDRe)
		f.site.mockMySQL.mu.Lock()
		f.site.mockMySQL.superReadOnly = false
		f.site.mockMySQL.mu.Unlock()
		if !f.tm.checkPrimaryReassert(ctx) || !siteWritable(f.site) {
			t.Fatalf("re-assert did not run (writable=%v): boot read_only is no fence", siteWritable(f.site))
		}
	})
	t.Run("a replaced server at boot read_only on older data is refused for its earlier identity", func(t *testing.T) {
		// The site ran under lossUUIDOld; its data directory was replaced
		// (no clone), so it answers under another server_uuid without the
		// tail it issued under the first. No clone superseded that identity.
		f := plannedNamedGroup(t, whole, lossReturnGTID, lossUUIDOld)
		f.tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
		f.site.lmu.Lock()
		f.site.uuid = lossUUIDRe
		f.site.lmu.Unlock()
		f.site.mockMySQL.mu.Lock()
		f.site.mockMySQL.superReadOnly = false
		f.site.mockMySQL.mu.Unlock()
		if f.tm.checkPrimaryReassert(ctx) || siteWritable(f.site) {
			t.Fatalf("a server without %s, which it issued under its earlier server_uuid and dc1 holds, was made writable", lossLostGTID)
		}
		if rec := f.loss(); rec == nil || rec.lostGtid != lossLostGTID || !rec.witness {
			t.Fatalf("own-loss record = %+v, want %s against a witness", rec, lossLostGTID)
		}
	})
	t.Run("the same after a clone: the peer is divergent, the site is not short, and a human decides", func(t *testing.T) {
		f := plannedNamedGroup(t, whole, lossReturnGTID, lossUUIDOld)
		f.tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
		f.tm.markSiteCloned(ctx, "dc2")
		f.site.lmu.Lock()
		f.site.uuid = lossUUIDRe
		f.site.lmu.Unlock()
		f.site.mockMySQL.mu.Lock()
		f.site.mockMySQL.superReadOnly = false
		f.site.mockMySQL.mu.Unlock()
		if f.tm.checkPrimaryReassert(ctx) || siteWritable(f.site) {
			t.Fatal("a target was made writable although a peer holds transactions it lacks")
		}
		if rec := f.loss(); rec != nil {
			t.Fatalf("own-loss record = %+v, want none: the site gave %s up at its clone", rec, lossLostGTID)
		}
		if got := len(f.records(t, "primary re-assert refused: peer has transactions the target lacks — divergence needs human review")); got != 1 {
			t.Fatalf("the divergence refusal was logged %d times, want once", got)
		}
	})
}

// A source a planned run named is made writable again although it no
// longer holds part of the set it was fenced with, when no site holds that
// part: every peer was compared and the source contains each. The
// transactions are lost, and the re-assert says so, once.
func TestReassert_PlannedNamedSourceWithoutPartOfItsFencedSet(t *testing.T) {
	ctx := context.Background()
	whole := lossUUIDOld + ":1-100"
	arrange := func(t *testing.T, dc1, dc2 string) (*lossFixture, *[]string) {
		f := newLossFixture(t, dc1, dc2, false)
		f.primary.mockMySQL.readOnly, f.primary.mockMySQL.superReadOnly = true, true
		f.site.mockMySQL.superReadOnly = true
		pollN(f.tm, 3)
		var reported []string
		f.tm.ReassertDataLossCallback = func(site, runID, lostGtid string, lostCount int64) {
			reported = append(reported, site+"/"+runID+"/"+lostGtid)
			if lostCount != 10 {
				t.Errorf("lost count = %d, want 10", lostCount)
			}
		}
		f.tm.mu.Lock()
		if !f.tm.nameFencedSourceLocked("run-1", "dc2", lossUUIDOld, whole) {
			t.Fatal("setup: the source was not named")
		}
		f.tm.mu.Unlock()
		return f, &reported
	}
	const msg = "primary re-assert: the fenced source no longer holds transactions it held under the planned failover's fence, and no site holds them; they are lost"

	t.Run("no site holds the lost part: re-asserted, and reported as lost", func(t *testing.T) {
		f, reported := arrange(t, lossReturnGTID, lossReturnGTID)
		if !f.tm.checkPrimaryReassert(ctx) || !siteWritable(f.site) {
			t.Fatalf("re-assert did not run (writable=%v)", siteWritable(f.site))
		}
		if got := *reported; len(got) != 1 || got[0] != "dc2/run-1/"+lossLostGTID {
			t.Fatalf("reported = %v, want the lost set once", got)
		}
		if got := f.records(t, msg); len(got) != 1 || got[0]["level"] != "ERROR" || got[0]["lostGtid"] != lossLostGTID {
			t.Fatalf("log = %v, want one ERROR line with the lost set", got)
		}
	})
	t.Run("a peer holds it: refused, nothing is called lost", func(t *testing.T) {
		f, reported := arrange(t, whole, lossReturnGTID)
		if f.tm.checkPrimaryReassert(ctx) || siteWritable(f.site) {
			t.Fatal("the fence was lifted although dc1 holds what the source lost")
		}
		if len(*reported) != 0 || len(f.records(t, msg)) != 0 {
			t.Fatalf("a loss was reported (%v) although a peer holds the transactions", *reported)
		}
	})
	t.Run("the source holds its fenced set: nothing is reported", func(t *testing.T) {
		f, reported := arrange(t, whole, whole)
		if !f.tm.checkPrimaryReassert(ctx) || !siteWritable(f.site) {
			t.Fatal("re-assert did not run")
		}
		if len(*reported) != 0 {
			t.Fatalf("reported = %v, want nothing", *reported)
		}
	})
}

// The re-assert reads the named source's server_uuid twice: when it
// decides whether the fence is the run's, and with the set it compares. A
// pod replaced between the two reads is not lifted from a fence on the
// first read's word.
func TestReassert_PlannedNamedSourceIsCheckedOnTheSnapshotItDecidesOn(t *testing.T) {
	whole := lossUUIDOld + ":1-100"
	f := plannedNamedGroup(t, whole, whole, lossUUIDOld)
	set := mustGTIDSet(t, whole)
	if !f.tm.reassertNamedSourceCheck("dc2", lossUUIDOld, true, set, false) {
		t.Fatal("the run's own server was refused")
	}
	if f.tm.reassertNamedSourceCheck("dc2", lossUUIDRe, true, set, false) {
		t.Fatal("another server than the fence names passed the second check")
	}
	if f.tm.reassertNamedSourceCheck("dc2", "", false, set, false) {
		t.Fatal("a server whose identity was not read passed the second check")
	}
	if got := f.records(t, "primary re-assert deferred: the fenced source answered as another server than the one its fence was checked on"); len(got) != 2 {
		t.Fatalf("the deferral was logged %d times, want twice", len(got))
	}
	// A site no run named has no binding to check.
	if !f.tm.reassertNamedSourceCheck("dc1", lossUUIDNew, true, set, false) {
		t.Fatal("a site without a binding was refused")
	}

	// In the re-assert itself: the server answers the first read as the
	// run's and the snapshot as another.
	g := plannedNamedGroup(t, whole, whole, lossUUIDOld)
	g.tm.mu.Lock()
	g.tm.plannedFencedSource.serverUUID = lossUUIDRe // the fence names a server the snapshot does not show
	g.tm.mu.Unlock()
	if g.tm.checkPrimaryReassert(context.Background()) || siteWritable(g.site) {
		t.Fatal("a fence that is not the run's was lifted")
	}
}

// A name given by anything but the run ends the run's binding: left
// standing, it would make the re-assert of a site a promotion named treat
// the site's server as "not the run's".
func TestPlannedFencedSource_BindingEndsWhenTheSiteIsNamedByAnotherPath(t *testing.T) {
	bound := func(f *lossFixture) plannedFencedSource {
		f.tm.mu.RLock()
		defer f.tm.mu.RUnlock()
		return f.tm.plannedFencedSource
	}
	for _, tc := range []struct {
		name string
		act  func(f *lossFixture)
	}{
		{"another site is observed as the only writable one", func(f *lossFixture) {
			f.tm.mu.Lock()
			f.tm.nameActiveSiteLocked("dc1", false)
			f.tm.mu.Unlock()
		}},
		{"a planned promotion is recorded", func(f *lossFixture) {
			f.tm.mu.Lock()
			f.tm.applyPlannedPromotionLocked(f.clk.Now().Add(time.Hour), "dc1", lossUUIDOld+":1-100")
			f.tm.mu.Unlock()
		}},
		{"a retired manager's failover record is carried", func(f *lossFixture) {
			f.tm.carryFailoverRecord(FailoverRecord{LastFailover: f.clk.Now().Add(time.Hour), LastFailoverTarget: "dc2"})
		}},
		{"a promotion of the same site is recorded", func(f *lossFixture) {
			f.tm.recordFailover(context.Background(), f.clk.Now(), "dc2", lossUUIDOld+":1-100")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			whole := lossUUIDOld + ":1-100"
			f := plannedNamedGroup(t, whole, whole, lossUUIDRe)
			if bound(f).site != "dc2" {
				t.Fatal("setup: no binding")
			}
			tc.act(f)
			if b := bound(f); b != (plannedFencedSource{}) {
				t.Fatalf("binding = %+v, want none after the site was named by another path", b)
			}
		})
	}
}
