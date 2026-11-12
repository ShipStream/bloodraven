package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// Reports restored from status after an operator restart, in a group with
// a third site, and the guards of the review and the lag proof that only
// such a group exercises.

// restoredHoleFixture is a group in which dc2 has a real hole against the
// primary dc1 (it lacks Old:91-100, which dc1 holds) and dc3 is a healthy
// replica, which therefore holds them too. The report is restored from
// status, as after an operator restart: it names dc1. channel says whether
// dc2 has a replication channel.
func restoredHoleFixture(t *testing.T, channel bool) *witnessFixture {
	t.Helper()
	f := newWitnessFixture(t, witnessHolderSet, witnessPrimarySet, witnessHolderSet)
	if !channel {
		f.dc2.mockMySQL.mu.Lock()
		f.dc2.mockMySQL.replicaStatusVal = nil
		f.dc2.mockMySQL.mu.Unlock()
	}
	f.tm.SetOwnTransactionsLostFrom("dc2", lossLostGTID, 10, "dc1")
	return f
}

// In the first poll after a restart the replicas are classified before the
// primary is seen writable, so the review runs with no active site and the
// primary counts as a site that has not answered. dc3, a healthy replica,
// holds everything the primary holds. The review must not take dc3 for the
// report's holder: the report would then say that the primary does not
// hold the transactions, dc2's replication could be started over the hole,
// and no reclone would be requested. The report stays restored until the
// primary is compared, becomes a hole, and dc2 is recloned.
func TestOwnLoss_RestoredHoleBesideAHealthyReplicaIsRecloned(t *testing.T) {
	for _, channel := range []bool{false, true} {
		name := "no channel"
		if channel {
			name = "a running channel"
		}
		t.Run(name, func(t *testing.T) {
			f := restoredHoleFixture(t, channel)
			for i := 0; i < 45; i++ {
				f.tm.Poll(context.Background())
				f.clk.Advance(time.Second)
				if rec := ownLossOf(f.tm, "dc2"); rec != nil && (rec.witness || rec.comparedTo != "dc1") {
					t.Fatalf("poll %d: record = %+v, want the report to keep naming the primary dc1 as a hole", i, rec)
				}
			}
			if _, _, clones, _ := f.dc2.counts(); clones != 1 {
				t.Fatalf("dc2 was cloned %d times, want once: it has a hole against the primary", clones)
			}
			if rec := ownLossOf(f.tm, "dc2"); rec != nil {
				t.Fatalf("record = %+v, want it ended by the reclone", rec)
			}
		})
	}
}

// The same while an ordered update (which is what follows an operator
// upgrade) holds recovery and the audit down: nothing compares dc2 with
// the primary for the length of the hold, so the report must stay what
// status said, a report against dc1 that keeps dc2's replication from
// being started. Both halves of the review's rule are needed: in the first
// poll the primary is not classified yet (a restored report gets no holder
// while a site is silent), and from the second on the primary answers and
// holds the transactions (the review leaves that to the audit).
func TestOwnLoss_RestoredHoleBesideAHealthyReplicaDuringAHold(t *testing.T) {
	for _, channel := range []bool{false, true} {
		name := "no channel"
		if channel {
			name = "a running channel"
		}
		t.Run(name, func(t *testing.T) {
			f := restoredHoleFixture(t, channel)
			f.tm.mu.Lock()
			f.tm.deploymentUpdateRunning = true
			f.tm.mu.Unlock()
			for i := 0; i < 120; i++ {
				f.tm.Poll(context.Background())
				f.clk.Advance(time.Second)
			}
			rec := ownLossOf(f.tm, "dc2")
			if rec == nil || rec.witness || rec.comparedTo != "dc1" {
				t.Fatalf("record = %+v, want the restored report against dc1 unchanged", rec)
			}
			if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); !errors.Is(err, errOwnTransactionsLost) {
				t.Fatalf("replication start gate = %v, want it refused: dc2 has a hole against the primary", err)
			}
			if err := f.tm.liveHandoffRefusal("dc2")(); err == nil || strings.Contains(err.Error(), "reclone dc3") {
				t.Fatalf("handoff refusal = %v, want dc2 refused without naming the healthy replica dc3", err)
			}
			if _, _, clones, _ := f.dc2.counts(); clones != 0 {
				t.Fatalf("dc2 was cloned %d times during the hold", clones)
			}
		})
	}
}

// A primary that holds transactions the site lacks is never replaced as
// the report's holder by another site that holds them too, also for a
// witness report made by this manager.
func TestOwnLoss_ReviewLeavesAReportThePrimaryHolds(t *testing.T) {
	f := newWitnessFixture(t, witnessHolderSet, witnessPrimarySet, witnessHolderSet)
	f.tm.mu.Lock()
	f.tm.sites[0].state, f.tm.sites[1].state, f.tm.sites[2].state = state.StateWritable, state.StateReadOnly, state.StateReadOnly
	f.tm.lastFailoverTarget = "dc1"
	f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "",
		witness: true, detectedBy: divergenceDetectedByAudit}}
	active := f.tm.activeSiteLocked()
	f.tm.mu.Unlock()
	if active != "dc1" {
		t.Fatalf("active site = %q, want dc1", active)
	}
	if f.tm.reviewOwnLossReports(context.Background()) {
		t.Fatal("the review changed a report whose transactions the primary holds")
	}
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || rec.comparedTo != "" {
		t.Fatalf("record = %+v, want it unchanged", rec)
	}
}

// A report restored from status names the primary, and the site is
// complete: the hole closed while the operator was down (the reclone
// finished during the restart), or the status write that ended the report
// was lost. A third site is down, so the report cannot end yet. The clean
// comparison with the primary makes it a report of the third kind, and the
// primary, known not to hold the transactions, is no longer its holder:
// the site may replicate, and nothing says that the primary holds them.
// The report stays (the rule that ends it needs every site) until dc3
// answers.
func TestOwnLoss_RestoredReportNamingThePrimaryComparedClean(t *testing.T) {
	t.Run("another site is silent: kept, with no holder, and replication may start", func(t *testing.T) {
		f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
		f.tm.SetOwnTransactionsLostFrom("dc2", lossLostGTID, 10, "dc1")
		silence(f.dc3, true)
		for i := 0; i < 3; i++ {
			f.poll(t, 6)
			f.clk.Advance(gtidAuditInterval)
		}
		rec := ownLossOf(f.tm, "dc2")
		if rec == nil || !rec.witness || rec.comparedTo != "" || rec.lostGtid != lossLostGTID {
			t.Fatalf("record = %+v, want the report kept as one that names no holder", rec)
		}
		if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); err != nil {
			t.Fatalf("replication start gate = %v, want it open: dc2 was compared clean with the primary", err)
		}
		if f.tm.ownLossHoleSite("dc2") {
			t.Fatal("the report still holds like a hole against the primary")
		}
		refusal := f.tm.liveHandoffRefusal("dc2")()
		if refusal == nil || strings.Contains(refusal.Error(), "dc1 holds them") || strings.Contains(refusal.Error(), "reclone dc1") {
			t.Fatalf("handoff refusal = %v, want dc2 refused without naming the primary as the holder", refusal)
		}
		for _, s := range f.tm.buildSnapshot(nil).Sites {
			if s.Name != "dc2" {
				continue
			}
			if !s.LostToWitness || s.LostComparedTo != "" || strings.Contains(s.RecloneWaiting, "as the primary yet") {
				t.Fatalf("snapshot = lostToWitness %v, comparedTo %q, waiting %q; want a witness report with no holder",
					s.LostToWitness, s.LostComparedTo, s.RecloneWaiting)
			}
		}
		if got := len(f.logged(t, "own transactions lost kept: the primary does not hold the transactions, the report stands until every other site has been compared")); got != 1 {
			t.Fatalf("the change of kind was logged %d times, want once", got)
		}

		// dc3 answers without the transactions: the report ends.
		silence(f.dc3, false)
		f.clk.Advance(gtidAuditInterval)
		f.poll(t, 6)
		if rec := ownLossOf(f.tm, "dc2"); rec != nil {
			t.Fatalf("record = %+v, want it ended once every site answered", rec)
		}
	})
	t.Run("every site answers: ended", func(t *testing.T) {
		f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
		f.tm.SetOwnTransactionsLostFrom("dc2", lossLostGTID, 10, "dc1")
		f.poll(t, 6)
		if rec := ownLossOf(f.tm, "dc2"); rec != nil {
			t.Fatalf("record = %+v, want none", rec)
		}
	})
}

// A witness report made while no site was writable names the site that is
// the primary now, so it holds like a hole until the primary is compared.
// A clean comparison settles it the same way: the primary is no longer the
// holder, and the report waits for every other site as one of the third
// kind. A witness report that names another site is not touched by a
// comparison with the primary.
func TestOwnLoss_WitnessReportNamingThePrimaryComparedClean(t *testing.T) {
	for _, tc := range []struct {
		name, holder string
		wantHolder   string
		wantChanged  bool
	}{
		{"it names the primary", "dc1", "", true},
		{"it names another site", "dc3", "dc3", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
			f.poll(t, 3)
			f.tm.mu.Lock()
			f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: tc.holder,
				witness: true, detectedBy: divergenceDetectedByRecovery}}
			changed, _ := f.tm.settleOwnLossLocked("dc2", mysql.GTIDSet{}, "dc1", divergenceDetectedByAudit, []string{lossUUIDOld}, false)
			hole := f.tm.ownLossHoleLocked("dc2")
			rec := *f.tm.ownLoss["dc2"]
			f.tm.mu.Unlock()
			if changed != tc.wantChanged || rec.comparedTo != tc.wantHolder || !rec.witness || rec.detectedBy != divergenceDetectedByRecovery {
				t.Fatalf("changed = %v, record = %+v; want changed %v and holder %q", changed, rec, tc.wantChanged, tc.wantHolder)
			}
			if hole {
				t.Fatal("the report holds like a hole after a clean comparison with the primary")
			}
		})
	}
}

// Only a site the manager has seen read-only is reviewed. The live flag
// alone is not enough: a site whose tracked state is not read-only (not
// classified yet, or unreachable a moment ago) is one the poll loop has
// drawn no conclusion about.
func TestOwnLoss_ReviewSkipsASiteNotTrackedReadOnly(t *testing.T) {
	arrange := func(t *testing.T, tracked state.SiteState) *witnessFixture {
		f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
		f.tm.mu.Lock()
		f.tm.sites[0].state, f.tm.sites[1].state, f.tm.sites[2].state = state.StateWritable, tracked, state.StateReadOnly
		f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc3",
			witness: true, detectedBy: divergenceDetectedByAudit}}
		f.tm.mu.Unlock()
		return f
	}
	for _, tracked := range []state.SiteState{state.StateUnknown, state.StateUnreachable, state.StateWritable} {
		f := arrange(t, tracked)
		if f.tm.reviewOwnLossReports(context.Background()) || ownLossOf(f.tm, "dc2") == nil {
			t.Fatalf("tracked state %v: the report of a site that is not tracked read-only was reviewed and ended", tracked)
		}
	}
	// The control: tracked read-only, the same report ends.
	f := arrange(t, state.StateReadOnly)
	if !f.tm.reviewOwnLossReports(context.Background()) || ownLossOf(f.tm, "dc2") != nil {
		t.Fatalf("record = %+v, want the report of a read-only site ended", ownLossOf(f.tm, "dc2"))
	}
}

// The review compares the other sites under the server_uuids of the site
// that no clone has superseded, not under every recorded one. dc2 was
// recloned and runs under a new server_uuid; dc3 still holds transactions
// of dc2's earlier identity that dc2 lacks. dc2 gave those up at the clone
// (its donor did not have them), so dc3 is divergent and dc2 is not short:
// the report ends. Under every recorded server_uuid dc3 would be its
// holder for good.
func TestOwnLoss_ReviewComparesPeersUnderLiveServerUUIDs(t *testing.T) {
	arrange := func(t *testing.T, timeline []string) *witnessFixture {
		f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
		f.dc2.lmu.Lock()
		f.dc2.uuid = lossUUIDRe
		f.dc2.lmu.Unlock()
		f.tm.mu.Lock()
		f.tm.sites[0].state, f.tm.sites[1].state, f.tm.sites[2].state = state.StateWritable, state.StateReadOnly, state.StateReadOnly
		f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc3",
			witness: true, detectedBy: divergenceDetectedByAudit}}
		f.tm.mu.Unlock()
		f.tm.siteIdentity.mu.Lock()
		f.tm.siteIdentity.timeline = map[string][]string{"dc2": timeline}
		f.tm.siteIdentity.mu.Unlock()
		return f
	}
	t.Run("the earlier identity was given up at a clone: ended", func(t *testing.T) {
		f := arrange(t, []string{lossUUIDOld, serverUUIDCloneMarker, lossUUIDRe})
		if !f.tm.reviewOwnLossReports(context.Background()) {
			t.Fatal("the review changed nothing")
		}
		if rec := ownLossOf(f.tm, "dc2"); rec != nil {
			t.Fatalf("record = %+v, want it ended: dc3 holds only transactions of an identity dc2 gave up at a clone", rec)
		}
	})
	t.Run("the server_uuid changed without a clone: kept", func(t *testing.T) {
		f := arrange(t, []string{lossUUIDOld, lossUUIDRe})
		f.tm.reviewOwnLossReports(context.Background())
		if rec := ownLossOf(f.tm, "dc2"); rec == nil || rec.comparedTo != "dc3" {
			t.Fatalf("record = %+v, want the report kept against dc3", rec)
		}
	})
}

// A stored lag proof is a snapshot of one server's binary log. Every site
// names its log files alike (mysql-bin.NNNNNN), so a snapshot taken on
// another primary, or on a server that ran under the primary's name with
// another server_uuid, compares against this primary's positions without
// any error: an applier that is past the old coordinates would turn lag
// into a proven hole. Such a proof is replaced by a snapshot of the
// primary of now.
func TestOwnLossLag_AProofOfAnotherPrimaryIsNotUsed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		proof ownLossProof
	}{
		{"another site was the primary", ownLossProof{primary: "dc3", primaryUUID: lossUUIDNew}},
		{"another server ran under the primary's name", ownLossProof{primary: "dc1", primaryUUID: lossUUIDThird}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The primary stands at 5000 and the site's applier at 4000:
			// behind a snapshot taken now, past the stale one at 3000.
			f := newLagFixture(t, lagPrimarySet, lagSiteSet, 4000)
			stale := tc.proof
			stale.pos = mysql.BinlogPosition{File: lagFile, Pos: 3000, GtidExecuted: lagPrimarySet}
			stale.set = mustGTIDSet(t, lagPrimarySet)
			f.tm.mu.Lock()
			f.tm.ownLossProof = map[string]*ownLossProof{"dc2": &stale}
			f.tm.mu.Unlock()
			hole, proven := f.tm.proveOwnLossHole(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2"), lossUUIDNew, lossUUIDOld,
				[]string{lossUUIDOld}, mustGTIDSet(t, lossLostGTID))
			if proven {
				t.Fatalf("hole = %q proven on a snapshot of another server; want nothing concluded until the applier passes a snapshot of this primary", hole.String())
			}
			f.tm.mu.RLock()
			got := f.tm.ownLossProof["dc2"]
			f.tm.mu.RUnlock()
			if got == nil || got.primary != "dc1" || got.primaryUUID != lossUUIDNew || got.pos.Pos != 5000 {
				t.Fatalf("stored proof = %+v, want a snapshot of dc1 at 5000", got)
			}
		})
	}
}

// A transient error on the read of the primary's binary log position (a
// timeout, a lost connection) concludes nothing: taken for "the primary
// reports no position" it would make the single comparison stand, and a
// follower that is only behind during a replay would be reported. Only an
// error that says the primary will never report one makes the comparison
// stand.
func TestOwnLossLag_ATransientPositionErrorConcludesNothing(t *testing.T) {
	f := newLagFixture(t, lagPrimarySet, lagSiteSet, 4000)
	f.primary.pmu.Lock()
	f.primary.posErr = errors.New("show binary log status: context deadline exceeded")
	f.primary.pmu.Unlock()
	lost := mustGTIDSet(t, lossLostGTID)
	prove := func() (mysql.GTIDSet, bool) {
		return f.tm.proveOwnLossHole(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2"), lossUUIDNew, lossUUIDOld,
			[]string{lossUUIDOld}, lost)
	}
	if hole, proven := prove(); proven {
		t.Fatalf("hole = %q proven on a position read that failed transiently", hole.String())
	}
	if n := f.logged("own transactions lost: the primary's binary log position could not be read"); n != 0 {
		t.Fatalf("the fallback was logged %d times for a transient error", n)
	}
	// The whole path: the audit reports nothing and no reclone is asked for.
	f.poll(2 * time.Minute)
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("record = %+v, want no report while the position cannot be read for a transient reason", rec)
	}
	// The read works again, and the applier is still behind: still lag.
	f.primary.pmu.Lock()
	f.primary.posErr = nil
	f.primary.pmu.Unlock()
	if hole, proven := prove(); proven {
		t.Fatalf("hole = %q proven while the applier is behind the snapshot", hole.String())
	}
}

// The primary re-assert ends its target's report by comparing the target
// with every peer under the target's live server_uuids. A target whose
// server_uuid cannot be read cannot be compared, so its report stands and
// it is not made writable: without that guard the comparison is skipped
// and the report is cleared as if every peer had been compared.
func TestOwnLoss_ReassertKeepsAReportWhenTheTargetIdentityIsUnknown(t *testing.T) {
	f := reassertGroup(t, lossUUIDOld+":1-100", lossUUIDOld+":1-100")
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.site.lmu.Lock()
	f.site.uuid = ""
	f.site.lmu.Unlock()
	f.pollFor(2 * time.Minute)
	if siteWritable(f.site) {
		t.Fatal("dc2 was made writable although it could not be compared with its peers under its server_uuid")
	}
	if f.loss() == nil {
		t.Fatal("the report was cleared without a comparison")
	}
	// Its server_uuid can be read again: compared, cleared, re-asserted.
	f.site.lmu.Lock()
	f.site.uuid = lossUUIDOld
	f.site.lmu.Unlock()
	f.pollFor(2 * time.Minute)
	if !siteWritable(f.site) || f.loss() != nil {
		t.Fatalf("dc2 writable = %v, report = %+v; want it re-asserted once it could be compared", siteWritable(f.site), f.loss())
	}
}
