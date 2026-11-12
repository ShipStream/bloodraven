package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// What a report is, what ends it, and what it decides in a split brain
// (ownLossRecord).

func silence(s *lossServer, down bool) {
	var err error
	if down {
		err = errors.New("connection refused")
	}
	s.mockMySQL.mu.Lock()
	s.mockMySQL.err, s.mockMySQL.gtidExecutedErr = err, err
	s.mockMySQL.mu.Unlock()
	s.lmu.Lock()
	s.snapErr = err
	s.lmu.Unlock()
}

// The holder of a report is stored and restored. A restored report does
// not end by a clean comparison with the primary: it becomes a witness
// report that names its holder, and ends when every other site has
// answered and none holds the transactions.
func TestOwnLoss_RestoredReportKeepsItsHolder(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.tm.SetOwnTransactionsLostFrom("dc2", lossLostGTID, 10, "dc3")
	f.tm.SetRecoveryBlockedFrom("dc3", lossLostGTID, 10, divergenceDetectedByAudit, "dc1")
	silence(f.dc3, true)

	if rec := ownLossOf(f.tm, "dc2"); rec == nil || rec.comparedTo != "dc3" || rec.witness || rec.detectedBy != "" {
		t.Fatalf("restored record = %+v, want the holder dc3 and no kind yet", rec)
	}
	if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); !errors.Is(err, errOwnTransactionsLost) {
		t.Fatalf("replication start gate = %v, want a restored report to hold like a hole until it is compared", err)
	}
	for _, s := range f.tm.Status().Sites {
		if s.Name == "dc2" && (s.LostComparedTo != "dc3" || s.LostGtid != lossLostGTID) {
			t.Fatalf("status of dc2 = %+v, want lostComparedTo dc3", s)
		}
	}

	f.poll(t, 6)
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 6)
	rec := ownLossOf(f.tm, "dc2")
	if rec == nil || !rec.witness || rec.comparedTo != "dc3" || rec.lostGtid != lossLostGTID {
		t.Fatalf("record after the audit against the primary = %+v, want a witness report that still names dc3", rec)
	}
	if got := f.logged(t, "own transactions lost kept: the primary does not hold the transactions, the report stands until every other site has been compared"); len(got) != 1 {
		t.Fatalf("the kept report was logged %d times, want once", len(got))
	}
	if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); err != nil {
		t.Fatalf("replication start gate = %v, want dc2 admitted once the primary is known not to hold the transactions", err)
	}
	if err := f.tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Fatal("handoff to dc2 is not refused while dc3 does not answer")
	}

	// dc3 answers again and still holds them: kept.
	silence(f.dc3, false)
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 3)
	if ownLossOf(f.tm, "dc2") == nil {
		t.Fatal("the report ended although dc3 holds the transactions")
	}
	// dc3 is recloned.
	setGTID(f.dc3, witnessPrimarySet)
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 3)
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("record = %+v after every site answered without the transactions, want none", rec)
	}
}

// A manager replaced in the same process (a spec or credential change)
// takes over its predecessor's reports with their kind, also one the
// status it was started from does not hold yet; and it drops one that
// status still holds and the predecessor has ended.
func TestOwnLoss_ManagerReplacementCarriesReports(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.poll(t, 6)
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || !rec.witness || rec.comparedTo != "dc3" {
		t.Fatalf("dc2 own-loss record = %+v, want a witness report against dc3", rec)
	}

	// The replacement starts from a status without the report, and dc3
	// does not answer it.
	g := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	silence(g.dc3, true)
	g.tm.SetOwnTransactionsLost("dc3", lossUUIDThird+":1-2", 2) // stale in status: the predecessor has no such report
	g.tm.carryOwnLossReports(f.tm)
	rec := ownLossOf(g.tm, "dc2")
	if rec == nil || !rec.witness || rec.comparedTo != "dc3" || rec.lostGtid != lossLostGTID || rec.detectedBy == "" {
		t.Fatalf("carried record = %+v, want the predecessor's witness report against dc3", rec)
	}
	if ownLossOf(g.tm, "dc3") != nil {
		t.Fatal("a report the predecessor does not hold was kept from status")
	}
	g.poll(t, 6)
	g.clk.Advance(gtidAuditInterval)
	g.poll(t, 6)
	if rec := ownLossOf(g.tm, "dc2"); rec == nil || !rec.witness {
		t.Fatalf("record after the replacement's audits = %+v, want the witness report kept while dc3 is silent", rec)
	}
	if err := g.tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Fatal("handoff to dc2 is not refused after the manager was replaced")
	}
}

// Two sites hold the transactions a third lost. The report names one; when
// that one is repaired the other still holds them, and the report moves to
// it.
func TestOwnLoss_TwoWitnesses(t *testing.T) {
	// No site is writable: dc1 and dc3 are both witnesses of dc2.
	f := newWitnessFixture(t, witnessHolderSet, witnessPrimarySet, witnessHolderSet)
	f.dc1.mockMySQL.mu.Lock()
	f.dc1.mockMySQL.readOnly, f.dc1.mockMySQL.superReadOnly = true, true
	f.dc1.mockMySQL.mu.Unlock()
	f.poll(t, 4)
	f.tm.mu.Lock()
	f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc3", witness: true,
		detectedBy: divergenceDetectedByAudit}}
	f.tm.mu.Unlock()

	setGTID(f.dc3, witnessPrimarySet) // dc3 is repaired
	f.tm.reviewOwnLossReports(context.Background())
	rec := ownLossOf(f.tm, "dc2")
	if rec == nil || !rec.witness || rec.comparedTo != "dc1" {
		t.Fatalf("record = %+v, want the report kept and moved to dc1, which holds the transactions too", rec)
	}

	setGTID(f.dc1, witnessPrimarySet)
	f.clk.Advance(gtidAuditInterval)
	f.tm.reviewOwnLossReports(context.Background())
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("record = %+v after neither site holds them, want none", rec)
	}
}

// The review concludes nothing from a site that is not seen read-only, and
// reviews only a site that is read-only itself.
func TestOwnLoss_ReviewNeedsEverySiteReadOnly(t *testing.T) {
	arrange := func(t *testing.T) *witnessFixture {
		f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
		f.poll(t, 4)
		f.tm.mu.Lock()
		f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc3", witness: true,
			detectedBy: divergenceDetectedByAudit}}
		f.tm.mu.Unlock()
		return f
	}
	t.Run("control: every site answers without the transactions", func(t *testing.T) {
		f := arrange(t)
		f.tm.reviewOwnLossReports(context.Background())
		if ownLossOf(f.tm, "dc2") != nil {
			t.Fatal("the report was kept although no site holds the transactions")
		}
	})
	t.Run("a peer is not classified read-only", func(t *testing.T) {
		f := arrange(t)
		f.tm.mu.Lock()
		f.tm.sites[2].state = state.StateUnreachable
		f.tm.mu.Unlock()
		f.tm.reviewOwnLossReports(context.Background())
		if ownLossOf(f.tm, "dc2") == nil {
			t.Fatal("the report ended although dc3 is not seen read-only")
		}
	})
	t.Run("the site itself answers writable", func(t *testing.T) {
		f := arrange(t)
		f.dc2.mockMySQL.mu.Lock()
		f.dc2.mockMySQL.readOnly, f.dc2.mockMySQL.superReadOnly = false, false
		f.dc2.mockMySQL.mu.Unlock()
		f.tm.reviewOwnLossReports(context.Background())
		if ownLossOf(f.tm, "dc2") == nil {
			t.Fatal("the report of a writable site was ended by a review: it is what makes the split-brain fence choose it")
		}
	})
}

// The reviewer's split brain with a report that rests on a real holder:
// the failover record names dc2, dc2 lost transactions dc3 holds, an
// administrator made dc1 the primary, and dc2 turns up writable. dc2 is
// fenced and keeps its report; dc1 keeps its writes.
func TestOwnLoss_ReportedFailoverTargetIsFencedNotKept(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.tm.SetLastFailoverTarget("dc2")
	f.poll(t, 6)
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || !rec.witness {
		t.Fatalf("dc2 own-loss record = %+v, want a witness report", rec)
	}
	f.dc2.mockMySQL.mu.Lock()
	f.dc2.mockMySQL.readOnly, f.dc2.mockMySQL.superReadOnly = false, false
	f.dc2.mockMySQL.mu.Unlock()
	f.poll(t, 8)
	if siteWritable(f.dc2) || !siteWritable(f.dc1) {
		t.Fatalf("dc1 writable=%v dc2 writable=%v, want the reported site fenced and the other kept", siteWritable(f.dc1), siteWritable(f.dc2))
	}
	if ownLossOf(f.tm, "dc2") == nil {
		t.Fatal("the report was dropped from a site the operator fenced")
	}
	if got := f.logged(t, "site that lacks transactions it issued itself is now the primary; unless its server_uuid was changed it issues their GTIDs again"); len(got) != 0 {
		t.Fatalf("dc2 was recorded as promoted: %v", got)
	}
	if got := f.logged(t, "split-brain: fencing a writable site that lacks transactions it issued itself"); len(got) == 0 {
		t.Fatal("the fence was not logged")
	}

	// The control, without a report: the failover record decides, as
	// before.
	g := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
	g.tm.SetLastFailoverTarget("dc2")
	g.poll(t, 3)
	g.dc2.mockMySQL.mu.Lock()
	g.dc2.mockMySQL.readOnly, g.dc2.mockMySQL.superReadOnly = false, false
	g.dc2.mockMySQL.mu.Unlock()
	g.poll(t, 8)
	if !siteWritable(g.dc2) || siteWritable(g.dc1) {
		t.Fatalf("dc1 writable=%v dc2 writable=%v, want the recorded failover target kept when it carries no report", siteWritable(g.dc1), siteWritable(g.dc2))
	}
}

// Every writable site carries a report, with failover history: nobody is
// provably the one to keep. Nothing is fenced, and the refusal is
// recorded for the Event.
func TestOwnLoss_SplitBrainWithEveryWritableSiteReported(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
	f.tm.SetLastFailoverTarget("dc2")
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
	if keep, reported := f.tm.splitBrainKeepSite("dc2"); keep != "" || !reported {
		t.Fatalf("splitBrainKeepSite = %q, %v: a reported failover target was kept", keep, reported)
	}
	f.tm.fenceReturningOldPrimary(context.Background(), state.CrossSiteAction{SplitBrain: true})
	if !siteWritable(f.dc1) || !siteWritable(f.dc2) {
		t.Fatalf("dc1 writable=%v dc2 writable=%v, want nothing fenced when every writable site carries a report", siteWritable(f.dc1), siteWritable(f.dc2))
	}
	f.tm.mu.RLock()
	refused := append([]string(nil), f.tm.ownLossPromotionRefused...)
	f.tm.mu.RUnlock()
	if !stringSlicesEqual(refused, []string{"dc1", "dc2"}) {
		t.Fatalf("recorded refusal = %v, want both sites, for the PromotionRefusedOwnTransactionsLost Event", refused)
	}
}

// A reported site that is the only writable one is recorded as promoted,
// and its report dropped, only when every other site is seen read-only.
// While one is not, that one may be the primary an administrator
// promoted, and the report must be there to fence this site when both
// show up writable.
func TestOwnLoss_ReportOfASoleWritableSiteWaitsForTheOthers(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.poll(t, 6)
	if rec := ownLossOf(f.tm, "dc2"); rec == nil {
		t.Fatal("no report on dc2")
	}
	// dc1 stops answering; someone makes dc2 writable.
	silence(f.dc1, true)
	f.dc2.mockMySQL.mu.Lock()
	f.dc2.mockMySQL.readOnly, f.dc2.mockMySQL.superReadOnly = false, false
	f.dc2.mockMySQL.mu.Unlock()
	f.poll(t, 8)
	rec := ownLossOf(f.tm, "dc2")
	if rec == nil {
		t.Fatal("the report was dropped while dc1 was not seen read-only")
	}
	if !strings.Contains(rec.recloneWaiting, "made writable without a reclone") {
		t.Fatalf("recloneWaiting = %q, want it to say why the report stays", rec.recloneWaiting)
	}
	// dc1 is back, writable: dc2 is the one fenced.
	silence(f.dc1, false)
	f.poll(t, 8)
	if siteWritable(f.dc2) || !siteWritable(f.dc1) {
		t.Fatalf("dc1 writable=%v dc2 writable=%v, want dc2 fenced", siteWritable(f.dc1), siteWritable(f.dc2))
	}

	// The control: every other site read-only. The report is dropped and
	// the promotion recorded.
	g := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	g.poll(t, 6)
	g.dc1.mockMySQL.mu.Lock()
	g.dc1.mockMySQL.readOnly, g.dc1.mockMySQL.superReadOnly = true, true
	g.dc1.mockMySQL.mu.Unlock()
	g.dc2.mockMySQL.mu.Lock()
	g.dc2.mockMySQL.readOnly, g.dc2.mockMySQL.superReadOnly = false, false
	g.dc2.mockMySQL.mu.Unlock()
	g.poll(t, 8)
	if ownLossOf(g.tm, "dc2") != nil {
		t.Fatal("the report stayed on a site that is the only writable one beside read-only sites")
	}
	if got := g.logged(t, "site that lacks transactions it issued itself is now the primary; unless its server_uuid was changed it issues their GTIDs again"); len(got) != 1 {
		t.Fatalf("the promotion was logged %d times, want once", len(got))
	}
}

// What a refusal tells the administrator: a hole is repaired by the
// site's reclone; a witness report by the site it names, and no reclone
// is coming.
func TestOwnLoss_RefusalSaysWhatEndsTheReport(t *testing.T) {
	for _, tc := range []struct {
		name string
		rec  ownLossRecord
		want []string
		not  string
	}{
		{"a hole against the primary", ownLossRecord{comparedTo: "dc1", detectedBy: divergenceDetectedByAudit},
			[]string{"wait for its reclone"}, ""},
		{"a witness report", ownLossRecord{comparedTo: "dc3", witness: true, detectedBy: divergenceDetectedByAudit},
			[]string{"dc3 holds them and the primary does not", "no reclone of dc2 is coming", "reclone dc3", "kubectl bloodraven reclone lion dc2 --cold"}, "wait for its reclone"},
		{"a witness report whose holder status did not say", ownLossRecord{witness: true, detectedBy: divergenceDetectedByAudit},
			[]string{"no reclone of dc2 is coming", "once every other site answers and none holds them"}, "wait for its reclone"},
		{"restored, not compared yet", ownLossRecord{comparedTo: "dc3"},
			[]string{"restored from status and is being compared again"}, "wait for its reclone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
			rec := tc.rec
			rec.lostGtid, rec.lostCount = lossLostGTID, 10
			f.tm.mu.Lock()
			f.tm.sites[0].state, f.tm.sites[1].state, f.tm.sites[2].state = state.StateWritable, state.StateReadOnly, state.StateReadOnly
			f.tm.ready = true
			f.tm.ownLoss = map[string]*ownLossRecord{"dc2": &rec}
			f.tm.mu.Unlock()
			err := f.tm.liveHandoffRefusal("dc2")()
			if err == nil {
				t.Fatal("no refusal")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("handoff refusal %q lacks %q", err.Error(), want)
				}
			}
			if tc.not != "" && strings.Contains(err.Error(), tc.not) {
				t.Errorf("handoff refusal %q says %q", err.Error(), tc.not)
			}
			f.tm.mu.Lock()
			decision, _, decided := f.tm.plannedClaimRefusalLocked(PlannedClaimRequest{RunID: "run-1", Target: "dc2"})
			f.tm.mu.Unlock()
			if !decided || decision.Kind != ClaimReject {
				t.Fatalf("planned claim = %+v decided=%v, want it rejected", decision, decided)
			}
			for _, want := range tc.want {
				if !strings.Contains(decision.Message, want) {
					t.Errorf("planned claim rejection %q lacks %q", decision.Message, want)
				}
			}
		})
	}
}

// The active primary lacks transactions it issued, which a follower
// holds: the follower is reported divergent, and the log says which of
// the two lost data. This is what is left of an ordered update's handoff
// whose abort found the source writable on older data.
func TestOwnLoss_AWritablePrimaryThatLacksItsOwnTransactionsIsNamed(t *testing.T) {
	// dc1 is the primary under lossUUIDNew and lacks New:19-20, which dc2
	// holds.
	f := newWitnessFixture(t, lossUUIDOld+":1-90,"+lossUUIDNew+":1-18", lossUUIDOld+":1-90,"+lossUUIDNew+":1-20", lossUUIDOld+":1-90,"+lossUUIDNew+":1-18")
	f.poll(t, 6)
	if got := recoveryOf(f.tm, "dc2"); got.state != v1alpha1.RecoveryStateBlocked || got.divergentGtid != lossUUIDNew+":19-20" {
		t.Fatalf("dc2 recovery = %+v, want it reported RecoveryBlocked for the transactions the primary lost", got)
	}
	const msg = "the primary lacks transactions it issued itself, which another site holds; it restarted on older data and was made writable outside the operator, and reuses their GTIDs for what it commits now"
	got := f.logged(t, msg)
	if len(got) != 1 || got[0]["site"] != "dc1" || got[0]["heldBy"] != "dc2" || got[0]["lostGtid"] != lossUUIDNew+":19-20" {
		t.Fatalf("log = %v, want one line naming dc1, dc2 and the set", got)
	}
	f.clk.Advance(2 * gtidAuditInterval)
	f.poll(t, 6)
	if got := f.logged(t, msg); len(got) != 1 {
		t.Fatalf("logged %d times, want once per finding", len(got))
	}
	if ownLossOf(f.tm, "dc1") != nil {
		t.Fatal("the writable primary carries an own-loss report: it is not a subject of one")
	}
}

// crossCheckOwnLoss counts only what the divergent site holds and the
// primary lacks. What the primary holds too is compared with the primary,
// where lag can be told from a hole.
func TestOwnLoss_CrossCheckCountsOnlyWhatThePrimaryLacks(t *testing.T) {
	// dc3 is divergent by Old:101, which dc2 issued and still holds and
	// the primary lacks. dc3 also holds Old:91-100, which the primary
	// holds too and dc2 has not applied yet (a replay onto the primary).
	f := newWitnessFixture(t, witnessHolderSet, witnessPrimarySet+","+lossUUIDOld+":101", witnessHolderSet+","+lossUUIDOld+":101")
	site := f.tm.getSite("dc3")
	f.tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
	f.tm.mu.Lock()
	f.tm.sites[0].state, f.tm.sites[1].state, f.tm.sites[2].state = state.StateWritable, state.StateReadOnly, state.StateReadOnly
	f.tm.mu.Unlock()
	if f.tm.crossCheckOwnLoss(context.Background(), site, mustGTIDSet(t, lossUUIDOld+":101"), "dc1") {
		t.Fatal("a report changed")
	}
	if rec := ownLossOf(f.tm, "dc2"); rec != nil {
		t.Fatalf("dc2 own-loss record = %+v: it was reported against dc3 for transactions the primary holds", rec)
	}
}

// The status the topology writer stores carries the report's holder, and
// drops it with the report.
func TestMergeTopologySnapshotStatus_LostComparedTo(t *testing.T) {
	snap := TopologySnapshot{ActiveSite: "dc1", Sites: []SiteSnapshot{
		{Name: "dc1", State: state.StateWritable},
		{Name: "dc2", State: state.StateReadOnly, RecoveryState: v1alpha1.RecoveryStateRecloneRequired,
			LostGtid: lossLostGTID, LostTxnCount: 10, LostComparedTo: "dc3", LostToWitness: true},
	}}
	var st v1alpha1.MysqlFailoverGroupStatus
	if !MergeTopologySnapshotStatus(&st, snap) {
		t.Fatal("snapshot not merged")
	}
	if s := st.Sites[1]; s.LostGtid != lossLostGTID || s.LostComparedTo != "dc3" {
		t.Fatalf("dc2 = %+v, want the lost set with lostComparedTo dc3", s)
	}
	snap.Sites[1].LostGtid, snap.Sites[1].LostTxnCount, snap.Sites[1].RecoveryState = "", 0, ""
	if !MergeTopologySnapshotStatus(&st, snap) {
		t.Fatal("snapshot not merged")
	}
	if s := st.Sites[1]; s.LostGtid != "" || s.LostComparedTo != "" {
		t.Fatalf("dc2 = %+v, want the holder dropped with the report", s)
	}
}

// crossCheckOwnLoss reads a site whose only recorded server_uuid stands
// before a clone marker: which identities are live depends on the
// server_uuid the site runs under, which only the read tells. A filter on
// the live identities of the timeline alone would skip it.
func TestOwnLoss_CrossCheckReadsASiteRecordedBeforeACloneMarker(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.tm.mu.Lock()
	f.tm.sites[0].state, f.tm.sites[1].state, f.tm.sites[2].state = state.StateWritable, state.StateReadOnly, state.StateReadOnly
	f.tm.mu.Unlock()
	// The timeline says dc2 was cloned after it ran under Old, and nothing
	// has read dc2 since. It still runs under Old.
	f.tm.siteIdentity.mu.Lock()
	f.tm.siteIdentity.timeline = map[string][]string{"dc2": {lossUUIDOld, serverUUIDCloneMarker}}
	f.tm.siteIdentity.mu.Unlock()
	if !f.tm.crossCheckOwnLoss(context.Background(), f.tm.getSite("dc3"), mustGTIDSet(t, lossLostGTID), "dc1") {
		t.Fatal("no report: dc2 was not read")
	}
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || !rec.witness || rec.comparedTo != "dc3" || rec.lostGtid != lossLostGTID {
		t.Fatalf("dc2 own-loss record = %+v, want a witness report against dc3", rec)
	}
	// The read recorded the identity again, after the marker.
	if got := f.tm.liveSiteServerUUIDs("dc2", ""); !stringSlicesEqual(got, []string{lossUUIDOld}) {
		t.Fatalf("live server_uuids of dc2 = %v, want the one it runs under", got)
	}
}

// A witness report that names the site which is the primary now (the
// picker or the re-assert made it while no site was writable) says that
// the primary holds the transactions. It holds like a hole: the site's
// replication is not started on it, and its reclone is not withheld as
// for a report that rests on another site.
func TestOwnLoss_WitnessReportThatNamesThePrimaryHoldsLikeAHole(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
	f.poll(t, 3)
	f.tm.mu.Lock()
	f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1", witness: true,
		detectedBy: divergenceDetectedByRecovery}}
	hole := f.tm.ownLossHoleLocked("dc2")
	f.tm.ownLoss["dc2"].comparedTo = "dc3"
	witness := f.tm.ownLossHoleLocked("dc2")
	f.tm.mu.Unlock()
	if !hole {
		t.Fatal("a witness report that names the primary does not hold the site out of replication")
	}
	if witness {
		t.Fatal("a witness report that names another site holds the site out of replication")
	}
}

// A report restored from status on a site that is not the recorded
// primary, with no writable site: nothing compares the site with a
// primary, but the review reads every other site. It ends the report
// when all of them answer and none holds the transactions, and keeps it
// while one is silent.
func TestOwnLoss_RestoredReportWithoutAPrimary(t *testing.T) {
	arrange := func(t *testing.T) *witnessFixture {
		f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
		f.dc1.mockMySQL.mu.Lock()
		f.dc1.mockMySQL.readOnly, f.dc1.mockMySQL.superReadOnly = true, true
		f.dc1.mockMySQL.mu.Unlock()
		f.poll(t, 4)
		f.tm.SetOwnTransactionsLostFrom("dc2", lossLostGTID, 10, "dc3")
		return f
	}
	t.Run("every site answers without them: ended", func(t *testing.T) {
		f := arrange(t)
		f.tm.reviewOwnLossReports(context.Background())
		if rec := ownLossOf(f.tm, "dc2"); rec != nil {
			t.Fatalf("record = %+v, want the restored report ended", rec)
		}
	})
	t.Run("the site it names is silent: kept", func(t *testing.T) {
		f := arrange(t)
		silence(f.dc3, true)
		f.tm.reviewOwnLossReports(context.Background())
		if ownLossOf(f.tm, "dc2") == nil {
			t.Fatal("the restored report ended although dc3 did not answer")
		}
	})
	t.Run("a site holds them: kept, as a witness report", func(t *testing.T) {
		f := arrange(t)
		setGTID(f.dc3, witnessHolderSet)
		f.tm.reviewOwnLossReports(context.Background())
		if rec := ownLossOf(f.tm, "dc2"); rec == nil || !rec.witness || rec.comparedTo != "dc3" || rec.detectedBy == "" {
			t.Fatalf("record = %+v, want a confirmed witness report against dc3", rec)
		}
	})
}

// The restart on a status an older CRD stored, with a silent witness: the
// report restored without its set does not end by the clean comparison
// with the primary, and holds until the witness answers.
func TestOwnLoss_ReportRestoredWithoutItsSetSurvivesASilentWitness(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.tm.SetOwnTransactionsLostUnknown("dc2")
	f.tm.SetRecoveryBlockedFrom("dc2", "", 0, "", "") // what the same status restores beside it
	silence(f.dc3, true)
	f.poll(t, 6)
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 6)
	rec := ownLossOf(f.tm, "dc2")
	if rec == nil || !rec.witness {
		t.Fatalf("record = %+v, want the report kept as a witness report while dc3 is silent", rec)
	}
	if err := f.tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Fatal("handoff to dc2 is not refused")
	}
	msg := ownLossMessage(f.tm.buildSnapshot(nil), "lion")
	if !strings.Contains(msg, "dc2 was reported as lacking transactions it issued itself before the operator restarted") {
		t.Fatalf("condition message = %q, want it to say the report was restored without its set", msg)
	}
	// dc3 answers: it holds them, and the report gets its set and holder.
	silence(f.dc3, false)
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 3)
	if rec := ownLossOf(f.tm, "dc2"); rec == nil || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc3" {
		t.Fatalf("record = %+v, want %s against dc3", rec, lossLostGTID)
	}
}
