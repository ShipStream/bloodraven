package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// Whose GTID set is evidence that a site lacks transactions it issued, and
// what the automatic reclone rests on (own_transactions_lost.go, "Whose
// set is evidence").

const lossUUIDThird = "55555555-0000-11f1-8000-0000000000e5" // dc3

// evidenceGroup is dc1 (down), dc2 (the candidate) and dc3, with dc3 a
// primary-candidate or a disaster-recovery site.
func evidenceGroup(t *testing.T, dc2, dc3 *lossServer, dc3Role state.SiteRole) *TopologyManager {
	t.Helper()
	dc1 := &lossServer{mockMySQL: &mockMySQL{err: errors.New("connection refused")}, uuid: lossUUIDNew}
	cfg := testTopologyConfig()
	cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3",
		Role: dc3Role, TaintSelector: taintSelector("dc3"), Host: "mysql-dc3"})
	tm := NewTopologyManager(cfg, []mysql.Checker{dc1, dc2, dc3}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	tm.failoverCooldown = 0
	t.Cleanup(func() {
		metrics.OwnTransactionsLost.WithLabelValues("dc2").Set(0)
		metrics.OwnTransactionsLost.WithLabelValues("dc1").Set(0)
	})
	return tm
}

func fencedLossServer(uuid, gtid string) *lossServer {
	return &lossServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: gtid}, uuid: uuid}
}

func siteWritable(s *lossServer) bool {
	s.mockMySQL.mu.Lock()
	defer s.mockMySQL.mu.Unlock()
	return !s.mockMySQL.readOnly
}

func ownLossOf(tm *TopologyManager, site string) *ownLossRecord {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if rec := tm.ownLoss[site]; rec != nil {
		cp := *rec
		return &cp
	}
	return nil
}

// After a full-instance in-place restore of dc1, dc2 was recloned from it
// and left the queue; dc3 still awaits its post-restore reclone and holds
// history dc2 issued under the server_uuid it had before its reclone. dc1
// fails. dc2 is the one promotable copy of the restored data and must be
// promoted: what a queued site holds says nothing about it.
func TestOwnLoss_RestoreQueuedPeerIsNoWitnessAgainstTheReclonedCandidate(t *testing.T) {
	restored := lossUUIDOld + ":1-50," + lossUUIDNew + ":1-15"
	dc2 := fencedLossServer(lossUUIDRe, restored)
	dc3 := fencedLossServer(lossUUIDThird, lossUUIDOld+":1-80,"+lossUUIDNew+":1-20")
	tm := evidenceGroup(t, dc2, dc3, state.SiteRolePrimaryCandidate)
	// dc2's history as its clone recorded it.
	tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
	tm.SetRestoreReclonePending([]string{"dc3"})
	pollN(tm, 8)
	if rec := ownLossOf(tm, "dc2"); !siteWritable(dc2) || rec != nil {
		t.Fatalf("dc2 was not promoted (active=%q writable=%v): own-loss record %+v rests on dc3, a site queued for its post-restore reclone",
			tm.Status().ActiveSite, siteWritable(dc2), rec)
	}
}

// The same queue, holding transactions under the server_uuid the candidate
// runs under now (a restore that rewound the GTID set, or a clone that did
// not rotate the uuid). The queued site is still no witness: it is never
// promoted and never replicated from, and its reclone discards what it
// holds. The control without the queue is refused.
func TestOwnLoss_RestoreQueuedPeerIsNoWitnessUnderTheCurrentUUID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(*TopologyManager)
		promote bool
	}{
		{"pending", func(tm *TopologyManager) { tm.SetRestoreReclonePending([]string{"dc3"}) }, true},
		{"held", func(tm *TopologyManager) {
			tm.mu.Lock()
			tm.restorePeerHeld = []string{"dc3"}
			tm.mu.Unlock()
		}, true},
		{"blocked", func(tm *TopologyManager) { tm.SetRestoreRecloneBlocked([]string{"dc3"}) }, true},
		{"not queued", func(*TopologyManager) {}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dc2 := fencedLossServer(lossUUIDRe, lossUUIDRe+":1-5")
			dc3 := fencedLossServer(lossUUIDThird, lossUUIDRe+":1-9")
			tm := evidenceGroup(t, dc2, dc3, state.SiteRoleDROnly)
			tc.arrange(tm)
			got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"})
			if tc.promote && (got != "dc2" || ownLossOf(tm, "dc2") != nil) {
				t.Fatalf("picked %q with record %+v, want dc2 and no report: dc3 awaits its post-restore reclone", got, ownLossOf(tm, "dc2"))
			}
			if !tc.promote && (got != "" || ownLossOf(tm, "dc2") == nil) {
				t.Fatalf("picked %q with record %+v, want a refusal: dc3 holds %s:6-9, which dc2 would issue again", got, ownLossOf(tm, "dc2"), lossUUIDRe)
			}
		})
	}
}

// After a full-instance in-place restore the restored primary restarts
// (it boots read-only) before its peer is recloned. Whatever the queued
// peer holds, the restored primary is not reported as a site that lost
// its own transactions.
func TestOwnLoss_RestoredPrimaryIsNotReportedOnAQueuedPeer(t *testing.T) {
	dc1 := fencedLossServer(lossUUIDNew, lossUUIDNew+":1-15")
	dc2 := fencedLossServer(lossUUIDOld, lossUUIDNew+":1-20")
	tm := NewTopologyManager(testTopologyConfig(), []mysql.Checker{dc1, dc2}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	tm.failoverCooldown = 0
	t.Cleanup(func() { metrics.OwnTransactionsLost.WithLabelValues("dc1").Set(0) })
	tm.SetRestoreReclonePending([]string{"dc2"})
	pollN(tm, 3)
	tm.mu.Lock()
	tm.lastFailoverTarget = "dc1"
	tm.mu.Unlock()
	pollN(tm, 3)
	if st := tm.Status(); st.Sites[0].LostGtid != "" || st.Sites[0].RecoveryState == v1alpha1.RecoveryStateRecloneRequired {
		t.Fatalf("the restored primary is reported %q (lostGtid %s) on the evidence of a peer that awaits its post-restore reclone",
			st.Sites[0].RecoveryState, st.Sites[0].LostGtid)
	}

	// The control: the same peer outside the queue is evidence, and the
	// target is reported instead of being made writable.
	tm.SetRestoreReclonePending(nil)
	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert was attempted although dc2 holds transactions dc1 issued and lacks")
	}
	if rec := ownLossOf(tm, "dc1"); rec == nil || rec.lostGtid != lossUUIDNew+":16-20" || rec.comparedTo != "dc2" {
		t.Fatalf("own-loss record = %+v, want %s:16-20 against dc2", rec, lossUUIDNew)
	}
}

// dc2 was recloned by an administrator who chose to discard transactions
// it had issued and its donor lacked (Old:51-80). dc3, a disaster-recovery
// site, still holds them: it is divergent, and reported or not, it is no
// witness against dc2. Since its clone dc2 holds of its earlier identity
// exactly what its donor held, so what dc3 has beyond that dc2 gave up at
// the clone.
func TestOwnLoss_WitnessOfDiscardedTransactionsDoesNotRefuseTheReclonedSite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		blocked bool
	}{
		{"the witness carries a RecoveryBlocked report", true},
		{"the witness came back unreported", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dc2 := fencedLossServer(lossUUIDRe, lossUUIDOld+":1-50,"+lossUUIDNew+":1-15")
			dc3 := fencedLossServer(lossUUIDThird, lossUUIDOld+":1-80,"+lossUUIDNew+":1-15")
			tm := evidenceGroup(t, dc2, dc3, state.SiteRoleDROnly)
			// dc2's timeline as its clone recorded it.
			tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
			tm.markSiteCloned(context.Background(), "dc2")
			if tc.blocked {
				tm.SetRecoveryBlockedFrom("dc3", lossUUIDOld+":51-80", 30, divergenceDetectedByAudit, "dc1")
			}
			if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "dc2" {
				t.Fatalf("picked %q, want dc2: dc3 holds only transactions dc2 gave up at its reclone", got)
			}
			if rec := ownLossOf(tm, "dc2"); rec != nil {
				t.Fatalf("own-loss record = %+v, want none", rec)
			}
		})
	}

	// The control: the same sets, but dc2's server_uuid changed without a
	// clone (its data directory was replaced). Nothing shows that it gave
	// those transactions up, so the witness counts and dc2 is refused.
	dc2 := fencedLossServer(lossUUIDRe, lossUUIDOld+":1-50,"+lossUUIDNew+":1-15")
	dc3 := fencedLossServer(lossUUIDThird, lossUUIDOld+":1-80,"+lossUUIDNew+":1-15")
	tm := evidenceGroup(t, dc2, dc3, state.SiteRoleDROnly)
	tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
	if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "" {
		t.Fatalf("picked %q: dc2 lacks %s:51-80 of an identity no clone superseded", got, lossUUIDOld)
	}
	if rec := ownLossOf(tm, "dc2"); rec == nil || rec.lostGtid != lossUUIDOld+":51-80" {
		t.Fatalf("own-loss record = %+v, want %s:51-80", rec, lossUUIDOld)
	}
}

func TestServerUUIDTimeline(t *testing.T) {
	const a, b, c, m = "aaaaaaaa-0000-11f1-8000-000000000001", "bbbbbbbb-0000-11f1-8000-000000000002", "cccccccc-0000-11f1-8000-000000000003", serverUUIDCloneMarker
	var timeline []string
	for _, step := range []struct {
		entry   string
		changed bool
		want    []string
	}{
		{m, false, nil},                      // a marker with nothing before it
		{a, true, []string{a}},               //
		{a, false, []string{a}},              // a uuid is recorded once
		{m, true, []string{a, m}},            //
		{m, false, []string{a, m}},           // a second clone under the same uuid adds nothing
		{b, true, []string{a, m, b}},         //
		{a, false, []string{a, m, b}},        // back under an earlier uuid: nothing new
		{m, true, []string{a, m, b, m}},      //
		{c, true, []string{a, m, b, m, c}},   //
		{"", false, []string{a, m, b, m, c}}, //
	} {
		var changed bool
		timeline, changed = appendServerUUIDEntry(timeline, step.entry)
		if changed != step.changed || !stringSlicesEqual(timeline, step.want) {
			t.Fatalf("append %q: timeline %v changed=%v, want %v changed=%v", step.entry, timeline, changed, step.want, step.changed)
		}
	}
	if got := recordedServerUUIDs(timeline); !stringSlicesEqual(got, []string{a, b, c}) {
		t.Fatalf("recordedServerUUIDs = %v, want every uuid without the markers", got)
	}
	for _, tc := range []struct {
		name     string
		timeline []string
		current  string
		want     []string
	}{
		{"never cloned: every uuid", []string{a, b}, b, []string{a, b}},
		{"cloned: what came after the clone", []string{a, m, b}, b, []string{b}},
		{"cloned twice", []string{a, m, b, m, c}, c, []string{c}},
		{"the clone has not installed: the uuid it runs under", []string{a, m}, a, []string{a}},
		{"a volume from before the clone: the uuid it runs under counts", []string{a, m, b}, a, []string{b, a}},
		{"a uuid changed without a clone stays live", []string{a, m, b, c}, c, []string{b, c}},
		{"an unknown current uuid adds nothing", []string{a, m, b}, "", []string{b}},
		{"no record: the current uuid", nil, a, []string{a}},
	} {
		if got := liveServerUUIDs(tc.timeline, tc.current); !stringSlicesEqual(got, tc.want) {
			t.Errorf("%s: liveServerUUIDs(%v, %q) = %v, want %v", tc.name, tc.timeline, tc.current, got, tc.want)
		}
	}
}

// The other side of the same rule: a clone that did not give the recipient
// a new server_uuid (CloneServerUUIDNotRotated) leaves it able to issue the
// discarded GTIDs again, and a witness that holds them, blocked or not,
// refuses it.
func TestOwnLoss_WitnessRefusesACandidateUnderTheUUIDItRunsUnder(t *testing.T) {
	dc2 := fencedLossServer(lossUUIDOld, lossUUIDOld+":1-50,"+lossUUIDNew+":1-15")
	dc3 := fencedLossServer(lossUUIDThird, lossUUIDOld+":1-80,"+lossUUIDNew+":1-15")
	tm := evidenceGroup(t, dc2, dc3, state.SiteRoleDROnly)
	tm.SetRecoveryBlockedFrom("dc3", lossUUIDOld+":51-80", 30, divergenceDetectedByAudit, "dc1")
	if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "" {
		t.Fatalf("picked %q: dc2 runs under %s and would issue 51-80 again, which dc3 holds", got, lossUUIDOld)
	}
	if rec := ownLossOf(tm, "dc2"); rec == nil || rec.lostGtid != lossUUIDOld+":51-80" || rec.comparedTo != "dc3" {
		t.Fatalf("own-loss record = %+v, want %s:51-80 against dc3", rec, lossUUIDOld)
	}
}

// A primary is evidence under every server_uuid the site has had, also for
// transactions the site once gave up: if the site that kept them is the
// primary now, the site's receiver drops them and only a clone brings them.
func TestOwnLoss_PrimaryIsEvidenceUnderAnEarlierUUID(t *testing.T) {
	f := newLossFixture(t, lossUUIDOld+":1-80,"+lossUUIDNew+":1-15", lossUUIDOld+":1-50,"+lossUUIDNew+":1-15", false)
	f.site.lmu.Lock()
	f.site.uuid = lossUUIDRe
	f.site.lmu.Unlock()
	f.tm.knownSiteServerUUIDs("dc2", lossUUIDOld)
	pollN(f.tm, 6)
	rec := f.loss()
	if rec == nil || rec.lostGtid != lossUUIDOld+":51-80" || rec.comparedTo != "dc1" {
		t.Fatalf("own-loss record = %+v, want %s:51-80 against the primary dc1", rec, lossUUIDOld)
	}
	if changes, starts, _, _ := f.site.counts(); changes != 0 || starts != 0 {
		t.Fatalf("replication was configured (CHANGE=%d START=%d): the receiver drops those transactions", changes, starts)
	}
}

// The recorded primary came back read-only, and its peer carries a
// divergence report whose set is the primary's own lost tail (the peer was
// audited against it after the loss). Re-assert used to set a peer's
// recorded divergence aside before comparing, made the target writable,
// and the target then issued those GTIDs again, which also cleared the
// peer's report by itself (DST seed 337457).
func TestOwnLoss_ReassertCountsAPeersRecordedDivergenceUnderTheTargetsUUID(t *testing.T) {
	f := newLossFixture(t, lossUUIDOld+":1-100", lossReturnGTID, false)
	f.primary.mockMySQL.readOnly, f.primary.mockMySQL.superReadOnly = true, true
	pollN(f.tm, 3)
	f.tm.mu.Lock()
	f.tm.lastFailoverTarget = "dc2"
	f.tm.mu.Unlock()
	f.tm.SetRecoveryBlockedFrom("dc1", lossLostGTID, 10, divergenceDetectedByAudit, "dc2")
	if f.tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert was attempted")
	}
	if siteWritable(f.site) {
		t.Fatalf("dc2 was made writable although dc1 holds %s, which dc2 would issue again", lossLostGTID)
	}
	rec := f.loss()
	if rec == nil || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc1" {
		t.Fatalf("own-loss record = %+v, want %s against dc1", rec, lossLostGTID)
	}
	if got := len(f.records(t, "primary re-assert refused: a peer holds transactions the target issued itself and lacks")); got != 1 {
		t.Fatalf("refusal logged %d times, want once", got)
	}

	// The control: a recorded divergence under another server's uuid is
	// still set aside, and the target is re-asserted.
	g := newLossFixture(t, lossUUIDOld+":1-90,"+lossUUIDNew+":1-5", lossReturnGTID, false)
	g.primary.mockMySQL.readOnly, g.primary.mockMySQL.superReadOnly = true, true
	pollN(g.tm, 3)
	g.tm.mu.Lock()
	g.tm.lastFailoverTarget = "dc2"
	g.tm.mu.Unlock()
	g.tm.SetRecoveryBlockedFrom("dc1", lossUUIDNew+":1-5", 5, divergenceDetectedByAudit, "dc2")
	if !g.tm.checkPrimaryReassert(context.Background()) || !siteWritable(g.site) {
		t.Fatalf("re-assert did not run (writable=%v): dc1's recorded divergence is its own history, not dc2's", siteWritable(g.site))
	}
}

// --- the automatic reclone ---

// pollNoClone polls n times, a second apart, waiting for the bootstrap a
// poll started, and fails when the site is cloned.
func (f *lossFixture) pollNoClone(t *testing.T, n int, why string) {
	t.Helper()
	for i := 0; i < n; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
		f.clk.Advance(time.Second)
		if _, _, clones, _ := f.site.counts(); clones > 0 {
			t.Fatalf("poll %d: dc2 was cloned over %s (recovery=%q loss=%+v)", i, why, recoveryOf(f.tm, "dc2").state, f.loss())
		}
	}
}

func (f *lossFixture) setHold(on bool) {
	f.tm.mu.Lock()
	f.tm.deploymentUpdateRunning = on
	f.tm.mu.Unlock()
}

func (f *lossFixture) pendingReclone() (site, owner string) {
	f.tm.mu.RLock()
	defer f.tm.mu.RUnlock()
	return f.tm.reclonePendingSite, f.tm.reclonePendingOwner
}

// A report restored from status on a site that also holds transactions the
// primary lacks, while an ordered update holds the sites: recovery and the
// audit stand down, so nothing records the divergence. No request is made
// during the hold, and none runs when it ends.
func TestOwnLoss_HeldRequestDoesNotCloneOverADivergentSite(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-90:101-105", true)
	f.site.mockMySQL.superReadOnly = true
	f.setHold(true)
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.pollNoClone(t, 6, "during the hold")
	if site, owner := f.pendingReclone(); site != "" {
		t.Fatalf("a reclone was requested during the hold (site=%q owner=%q)", site, owner)
	}
	if rec := f.loss(); rec == nil || !strings.Contains(rec.recloneWaiting, "another topology change holds the sites") {
		t.Fatalf("own-loss record = %+v, want it to wait for the hold", rec)
	}
	f.setHold(false)
	f.pollNoClone(t, 40, "although it holds "+lossUUIDOld+":101-105, which the primary lacks")
	if got := recoveryOf(f.tm, "dc2").state; got != v1alpha1.RecoveryStateBlocked {
		t.Fatalf("recovery state = %q, want RecoveryBlocked", got)
	}
}

// A report that does not hold against the current primary (restored from
// status) on a complete, replicating site. Nothing is requested on it
// before a comparison, and the comparison clears it.
func TestOwnLoss_UnconfirmedReportIsNotCloned(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossPrimaryGTID, true)
	f.site.mockMySQL.superReadOnly = true
	f.site.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1", IORunning: true, SQLRunning: true}
	pollN(f.tm, 4)
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.setHold(true)
	f.pollNoClone(t, 3, "during the hold")
	if site, _ := f.pendingReclone(); site != "" {
		t.Fatalf("a reclone of %q was requested on a report restored from status", site)
	}
	f.setHold(false)
	f.pollNoClone(t, 40, "although it holds every transaction it issued")
	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record = %+v, want it cleared by the audit", rec)
	}
}

// A report restored from status for a site whose channel points at the
// primary but is stopped, and which holds transactions the primary lacks.
func TestOwnLoss_RestoredReportOnADivergentFollowerIsNotCloned(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-90:101-105", true)
	f.site.mockMySQL.superReadOnly = true
	f.site.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1", IORunning: false, SQLRunning: false}
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.pollNoClone(t, 12, "although it holds "+lossUUIDOld+":101-105, which the primary lacks")
}

// A request never outlives its poll: one that stood when the report
// cleared, when a divergence block was recorded or when a hold began is
// gone on the next poll, before checkReclone runs.
func TestOwnLoss_RequestIsWithdrawnEveryPoll(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(f *lossFixture)
	}{
		{"the report cleared", func(f *lossFixture) { delete(f.tm.ownLoss, "dc2") }},
		{"a divergence block was recorded", func(f *lossFixture) {
			f.tm.recovery["dc2"] = &siteRecovery{state: v1alpha1.RecoveryStateBlocked, divergentGtid: lossUUIDOld + ":101-105"}
		}},
		{"an ordered update began", func(f *lossFixture) { f.tm.deploymentUpdateRunning = true }},
		{"the report is not confirmed", func(f *lossFixture) { f.tm.ownLoss["dc2"].detectedBy = "" }},
		{"the report was made against another site", func(f *lossFixture) { f.tm.ownLoss["dc2"].comparedTo = "dc3" }},
		{"an initFromBackup restore is in flight", func(f *lossFixture) { f.tm.autoBootstrapSuppressed = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", true)
			pollN(f.tm, 2)
			f.tm.mu.Lock()
			f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1", detectedBy: divergenceDetectedByRecovery}}
			f.tm.mu.Unlock()
			f.tm.requestOwnLossReclone()
			if site, owner := f.pendingReclone(); site != "dc2" || owner != recloneOwnerOwnLoss {
				t.Fatalf("pending = %q/%q, want the operator's request for dc2", site, owner)
			}
			f.tm.mu.Lock()
			tc.arrange(f)
			f.tm.mu.Unlock()
			f.tm.requestOwnLossReclone()
			if site, owner := f.pendingReclone(); site != "" {
				t.Fatalf("pending = %q/%q after %s, want the request withdrawn", site, owner, tc.name)
			}
		})
	}

	// An administrator's request for the same site is not the operator's
	// to withdraw.
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", true)
	pollN(f.tm, 2)
	f.tm.SetRecloneSite("dc2")
	f.tm.requestOwnLossReclone()
	if site, owner := f.pendingReclone(); site != "dc2" || owner != "" {
		t.Fatalf("pending = %q/%q, want the administrator's request kept", site, owner)
	}
}

// unsealGate is a keyring gate that answers not-ready until told, and can
// run a step when the unseal is asked for (after checkReclone's check,
// before the bootstrap goroutine's).
type unsealGate struct {
	testKeyringGate
	onRequest func()
}

func (g *unsealGate) RequestKeyringUnseal(ctx context.Context, nn types.NamespacedName, site string) (bool, error) {
	if g.onRequest != nil {
		g.onRequest()
	}
	return g.testKeyringGate.RequestKeyringUnseal(ctx, nn, site)
}

// gtidHookServer runs hook before it answers a GTID read.
type gtidHookServer struct {
	*lossServer
	hook func()
}

func (h gtidHookServer) GetGtidExecuted(ctx context.Context) (string, error) {
	h.hook()
	return h.lossServer.GetGtidExecuted(ctx)
}

func (f *lossFixture) setSiteGTID(gtid string) {
	f.site.mockMySQL.mu.Lock()
	f.site.mockMySQL.gtidExecuted = gtid
	f.site.mockMySQL.mu.Unlock()
}

// The clone rests on what is read when it is about to start. A confirmed
// report on a site that has since taken transactions the primary lacks
// (it was made writable for a while) starts no clone, whatever the report
// says, and the divergence is recorded.
func TestOwnLoss_RecloneIsRefusedForASiteThatDivergedAfterTheReport(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	gate := &unsealGate{}
	f.tm.SetKeyringGate(gate)
	// The keyring is not unsealed yet: the report is made and confirmed,
	// the request stands, and no clone starts.
	f.pollNoClone(t, 6, "before its keyring was unsealed")
	if rec := f.loss(); rec == nil || rec.detectedBy == "" || rec.comparedTo != "dc1" {
		t.Fatalf("own-loss record = %+v, want a confirmed report against dc1", rec)
	}
	if site, owner := f.pendingReclone(); site != "dc2" || owner != recloneOwnerOwnLoss {
		t.Fatalf("pending = %q/%q, want the operator's request for dc2", site, owner)
	}
	// The site diverges, and then the clone could start.
	f.setSiteGTID(lossUUIDOld + ":1-90:101-105")
	gate.ready = true
	f.pollNoClone(t, 40, "although it holds "+lossUUIDOld+":101-105, which the primary lacks")
	if got := recoveryOf(f.tm, "dc2").state; got != v1alpha1.RecoveryStateBlocked {
		t.Fatalf("recovery state = %q, want RecoveryBlocked", got)
	}
	refusals := f.records(t, "automatic reclone not started: the check before the clone does not allow it")
	if len(refusals) != 1 || refusals[0]["site"] != "dc2" || refusals[0]["donor"] != "dc1" || refusals[0]["extraGtid"] != lossUUIDOld+":101-105" {
		t.Fatalf("refusal log = %v, want one naming the extra transactions", refusals)
	}
	if _, _, _, fenced := f.site.counts(); !fenced {
		t.Fatal("the site was not fenced by the check")
	}
}

// The same, with the divergence appearing after checkReclone's check and
// before the bootstrap goroutine issues the clone (the keyring unseal can
// take a pod roll): the goroutine's own check stops it, and nothing is
// counted as a failed clone.
func TestOwnLoss_RecloneIsCheckedAgainByTheBootstrapGoroutine(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	gate := &unsealGate{}
	gate.ready = true
	gate.onRequest = func() { f.setSiteGTID(lossUUIDOld + ":1-90:101-105") }
	f.tm.SetKeyringGate(gate)
	f.pollNoClone(t, 12, "although it took "+lossUUIDOld+":101-105 after the first check")
	if gate.calls.Load() == 0 {
		t.Fatal("the bootstrap was never admitted: the goroutine's check was not reached")
	}
	if phase := f.tm.BootstrapPhase(); phase == BootstrapPhaseFailed {
		t.Fatalf("bootstrap phase = %q: a withdrawn clone is not a failed one", phase)
	}
	if rec := f.loss(); rec != nil && rec.recloneFailures != 0 {
		t.Fatalf("recloneFailures = %d, want 0", rec.recloneFailures)
	}
}

// A site that turns out complete when the clone is about to start (it was
// repaired by hand since the report) is not cloned, and its report is
// cleared by that comparison.
func TestOwnLoss_RecloneIsWithdrawnForASiteFoundComplete(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	gate := &unsealGate{}
	f.tm.SetKeyringGate(gate)
	f.pollNoClone(t, 6, "before its keyring was unsealed")
	f.setSiteGTID(lossPrimaryGTID)
	gate.ready = true
	f.tm.requestOwnLossReclone()
	if f.tm.checkReclone(context.Background()) {
		t.Fatal("a reclone started for a site that holds every transaction it issued")
	}
	if rec := f.loss(); rec != nil {
		t.Fatalf("own-loss record = %+v, want it cleared", rec)
	}
	if site, _ := f.pendingReclone(); site != "" {
		t.Fatalf("pending reclone = %q, want none", site)
	}
}

// What the check before the clone refuses on, each with the site left
// uncloned and the request dropped.
func TestOwnLoss_VerifyRecloneRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(f *lossFixture)
		reason  string
	}{
		{"the site cannot be fenced", func(f *lossFixture) { f.site.mockMySQL.superReadOnlyErr = errors.New("fence failed") }, "could not be fenced"},
		{"the site's set cannot be read", func(f *lossFixture) {
			f.site.lmu.Lock()
			f.site.snapErr = errors.New("read failed")
			f.site.lmu.Unlock()
		}, "GTID set could not be read"},
		{"the primary's set cannot be read", func(f *lossFixture) { f.primary.mockMySQL.gtidExecutedErr = errors.New("read failed") }, "primary's GTID set could not be read"},
		{"the site is recorded divergent", func(f *lossFixture) {
			f.tm.mu.Lock()
			f.tm.recovery["dc2"] = &siteRecovery{state: v1alpha1.RecoveryStateBlocked, divergentGtid: lossUUIDOld + ":101"}
			f.tm.mu.Unlock()
		}, "also holds transactions the primary lacks"},
		{"the primary changed", func(f *lossFixture) {
			f.tm.mu.Lock()
			f.tm.sites[0].mysql = gtidHookServer{f.primary, func() {
				f.tm.mu.Lock()
				f.tm.authorityEpoch++
				f.tm.mu.Unlock()
			}}
			f.tm.mu.Unlock()
		}, "primary changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", true)
			pollN(f.tm, 2)
			f.setSiteGTID(lossReturnGTID)
			f.tm.mu.Lock()
			f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1", detectedBy: divergenceDetectedByRecovery}}
			f.tm.reclonePendingSite, f.tm.reclonePendingOwner = "dc2", recloneOwnerOwnLoss
			f.tm.mu.Unlock()
			tc.arrange(f)
			if f.tm.verifyOwnLossReclone(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2")) {
				t.Fatal("the check let the clone start")
			}
			if site, _ := f.pendingReclone(); site != "" {
				t.Fatalf("pending reclone = %q, want the request dropped", site)
			}
			if rec := f.loss(); rec == nil || !strings.Contains(rec.recloneWaiting, tc.reason) {
				t.Fatalf("own-loss record = %+v, want recloneWaiting to mention %q", rec, tc.reason)
			}
		})
	}

	// The control: the same fixture with nothing in the way passes.
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", true)
	pollN(f.tm, 2)
	f.setSiteGTID(lossReturnGTID)
	f.tm.mu.Lock()
	f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1", detectedBy: divergenceDetectedByRecovery}}
	f.tm.reclonePendingSite, f.tm.reclonePendingOwner = "dc2", recloneOwnerOwnLoss
	f.tm.mu.Unlock()
	if !f.tm.verifyOwnLossReclone(context.Background(), f.tm.getSite("dc1"), f.tm.getSite("dc2")) {
		t.Fatalf("the check refused a lossy, contained, fenced site: %+v", f.loss())
	}
}

// dc1 (the primary) dies. dc2 lacks transactions it issued (Old:91-100)
// but holds New:41-50, which dc3 lacks; dc3 holds Old:1-100. The picker
// refuses dc2 on dc3's evidence and promotes dc3. dc2, the only copy of
// New:41-50, is then divergent as well as lossy and is never cloned over.
func TestOwnLoss_LossyAndAheadCandidateIsNotClonedOver(t *testing.T) {
	for _, withChannel := range []bool{false, true} {
		name := "no channel"
		if withChannel {
			name = "stopped channel to the dead primary"
		}
		t.Run(name, func(t *testing.T) {
			f := newLossFixture(t, "", lossUUIDOld+":1-90,"+lossUUIDNew+":1-50", true)
			f.primary.mockMySQL.err = errors.New("connection refused")
			f.site.mockMySQL.superReadOnly = true
			dc3 := &lossServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: lossUUIDOld + ":1-100," + lossUUIDNew + ":1-40"}, uuid: lossUUIDThird}
			f.site.donor = dc3
			if withChannel {
				f.site.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1", IORunning: false, SQLRunning: true}
				dc3.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1", IORunning: false, SQLRunning: true}
			}
			cfg := testTopologyConfig()
			cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3",
				Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("dc3"), Host: "mysql-dc3"})
			f.tm = NewTopologyManagerWithClock(cfg, []mysql.Checker{f.primary, f.site, dc3}, NewFailoverController(testLogger()), nil, NewBootstrapController(testLogger()),
				BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass", CloneTimeout: 10 * time.Second},
				newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger(), f.clk)
			f.tm.failoverCooldown = 0
			f.pollNoClone(t, 40, "although it held "+lossUUIDNew+":41-50, which no reachable site has")
			if siteWritable(f.site) || !siteWritable(dc3) {
				t.Fatalf("writable: dc2=%v dc3=%v, want dc3 promoted and dc2 refused", siteWritable(f.site), siteWritable(dc3))
			}
		})
	}
}

// The wait between failed automatic reclones doubles up to its cap, and
// the count of consecutive failures is published for the Event.
func TestOwnLoss_FailedRecloneBacksOff(t *testing.T) {
	for failures, want := range map[int]time.Duration{
		1: 5 * time.Minute, 2: 10 * time.Minute, 3: 20 * time.Minute, 4: 40 * time.Minute,
		5: 80 * time.Minute, 6: 2 * time.Hour, 7: 2 * time.Hour, 40: 2 * time.Hour,
	} {
		if got := ownLossRecloneBackoff(failures); got != want {
			t.Errorf("ownLossRecloneBackoff(%d) = %s, want %s", failures, got, want)
		}
	}

	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	f.site.cloneFails = errors.New("clone instance: Error 3862 (HY000): Clone Donor Error")
	attempt := func(wantClones int, wait time.Duration) {
		t.Helper()
		for i := 0; i < 10; i++ {
			f.tm.Poll(context.Background())
			f.waitBootstrapEnd(t)
		}
		if _, _, clones, _ := f.site.counts(); clones != wantClones {
			t.Fatalf("clone attempts = %d, want %d", clones, wantClones)
		}
		rec := f.loss()
		if rec == nil || rec.recloneFailures != wantClones {
			t.Fatalf("own-loss record = %+v, want %d failures", rec, wantClones)
		}
		if got := rec.recloneRetryAfter.Sub(f.clk.Now()); got != wait {
			t.Fatalf("next attempt in %s after %d failures, want %s", got, wantClones, wait)
		}
		if got := f.tm.buildSnapshot(nil).Sites[1].RecloneFailures; got != wantClones {
			t.Fatalf("snapshot RecloneFailures = %d, want %d", got, wantClones)
		}
		// One second short of the wait nothing is asked for.
		f.clk.Advance(wait - time.Second)
		for i := 0; i < 3; i++ {
			f.tm.Poll(context.Background())
			f.waitBootstrapEnd(t)
		}
		if _, _, clones, _ := f.site.counts(); clones != wantClones {
			t.Fatalf("clone attempts = %d before the wait ended, want %d", clones, wantClones)
		}
		f.clk.Advance(2 * time.Second)
	}
	attempt(1, 5*time.Minute)
	attempt(2, 10*time.Minute)
	attempt(3, 20*time.Minute)
	if rec := f.loss(); !strings.Contains(rec.recloneWaiting, "the last 3 automatic reclones did not complete") {
		t.Fatalf("recloneWaiting = %q, want the count of failures", rec.recloneWaiting)
	}

	// A manager that replaces this one keeps the count and the wait.
	g := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	g.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	g.tm.carryOwnLossRecloneBackoff(f.tm)
	if rec, prev := g.loss(), f.loss(); rec.recloneFailures != 3 || !rec.recloneRetryAfter.Equal(prev.recloneRetryAfter) {
		t.Fatalf("carried backoff = %d failures until %s, want 3 until %s", rec.recloneFailures, rec.recloneRetryAfter, prev.recloneRetryAfter)
	}
}

// failingSiteIdentityStore cannot be written: the server_uuid history
// reaches the group only through another write.
type failingSiteIdentityStore struct{}

func (failingSiteIdentityStore) Load(context.Context) (map[string][]string, error) {
	return nil, nil
}

func (failingSiteIdentityStore) Add(context.Context, string, string) error {
	return errors.New("the API server is away")
}

// The server_uuid a clone recipient gives up is stored by the write the
// clone waits for, not by a write of its own that may fail: an operator
// that restarted between the two would never have recorded it, and a
// volume that later came back from before the clone would not be
// recognised.
func TestCloneIdentityRecord_StoresTheServerUUIDHistoryInTheSameWrite(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg).Build()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	site := fg.Spec.Sites[1].Name
	history := func() (map[string][]string, string) {
		t.Helper()
		var cur v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &cur); err != nil {
			t.Fatal(err)
		}
		return parseServerUUIDHistory(cur.Annotations[v1alpha1.ServerUUIDHistoryAnnotation], nil, nn), cur.ResourceVersion
	}

	store := &groupCloneIdentityStore{client: c, nn: nn, logger: testLogger()}
	_, before := history()
	if err := store.Record(ctx, site, CloneIdentity{ServerUUID: strings.ToUpper(lossUUIDOld)}); err != nil {
		t.Fatal(err)
	}
	got, after := history()
	if !stringSlicesEqual(got[site], []string{lossUUIDOld, serverUUIDCloneMarker}) {
		t.Fatalf("server_uuid timeline after Record = %v, want [%s clone]", got, lossUUIDOld)
	}
	if recs, err := store.Load(ctx); err != nil || recs[site].ServerUUID == "" {
		t.Fatalf("clone identity record = %v, %v, want it stored", recs, err)
	}
	var cur v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	if before == after || cur.Annotations[v1alpha1.CloneServerUUIDAnnotation] == "" {
		t.Fatalf("resourceVersion %s -> %s: the record was not written", before, after)
	}
	// Removing the record keeps the history.
	if err := store.Resolve(ctx, site, CloneIdentity{ServerUUID: strings.ToUpper(lossUUIDOld)}); err != nil {
		t.Fatal(err)
	}
	if got, _ := history(); len(got[site]) != 2 {
		t.Fatalf("server_uuid timeline after Resolve = %v, want it kept", got)
	}

	// Through the manager: the history's own store fails, the clone
	// identity record's write carries the uuid.
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	f.tm.SetCloneIdentityStore(store)
	f.tm.SetSiteIdentityStore(failingSiteIdentityStore{})
	f.site.lmu.Lock()
	f.site.uuid = lossUUIDThird
	f.site.lmu.Unlock()
	uuidBefore := f.tm.cloneServerUUIDBefore(ctx, f.site, site)
	f.tm.knownSiteServerUUIDs(site, uuidBefore)
	if err := f.tm.recordCloneIdentity(ctx, site, uuidBefore, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := history(); !stringSlicesEqual(got[site], []string{lossUUIDOld, serverUUIDCloneMarker, lossUUIDThird, serverUUIDCloneMarker}) {
		t.Fatalf("server_uuid timeline = %v, want %s and the clone added by the clone identity record's write", got, lossUUIDThird)
	}
	f.tm.siteIdentity.mu.Lock()
	unsaved := len(f.tm.siteIdentity.unsaved[site])
	f.tm.siteIdentity.mu.Unlock()
	if unsaved != 0 {
		t.Fatalf("%d entries of %s still wait to be stored, want none", unsaved, site)
	}
	if live := f.tm.liveSiteServerUUIDs(site, lossUUIDRe); !stringSlicesEqual(live, []string{lossUUIDRe}) {
		t.Fatalf("live server_uuids after the clone = %v, want only the new one", live)
	}
}

// The ordered update's rejoin check: a site it restarted and is about to
// start replicating is compared with its source first.
func TestOwnLoss_UpdaterRejoinCheck(t *testing.T) {
	ctx := context.Background()
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, false)
	pollN(f.tm, 1)
	if err := f.tm.refuseRejoinWithoutOwnTransactions(ctx, "dc2", f.site, f.primary); !errors.Is(err, errOwnTransactionsLost) {
		t.Fatalf("rejoin check = %v, want errOwnTransactionsLost", err)
	}
	if rec := f.loss(); rec == nil || rec.lostGtid != lossLostGTID || rec.comparedTo != "dc1" {
		t.Fatalf("own-loss record = %+v, want %s against dc1", rec, lossLostGTID)
	}
	// The gate refuses from then on.
	if err := f.tm.withReplicaStartGate("dc2", func() error { return nil }); !errors.Is(err, errOwnTransactionsLost) {
		t.Fatalf("gate = %v, want errOwnTransactionsLost", err)
	}

	g := newLossFixture(t, lossPrimaryGTID, lossUUIDOld+":1-100", false)
	if err := g.tm.refuseRejoinWithoutOwnTransactions(ctx, "dc2", g.site, g.primary); err != nil || g.loss() != nil {
		t.Fatalf("rejoin check of a complete site = %v (record %+v), want it admitted", err, g.loss())
	}
	// A set that cannot be read refuses this start and records nothing.
	g.site.lmu.Lock()
	g.site.snapErr = errors.New("read failed")
	g.site.lmu.Unlock()
	if err := g.tm.refuseRejoinWithoutOwnTransactions(ctx, "dc2", g.site, g.primary); err == nil || errors.Is(err, errOwnTransactionsLost) || g.loss() != nil {
		t.Fatalf("rejoin check with an unreadable site = %v (record %+v), want a plain refusal", err, g.loss())
	}

	// The topology manager installs the check and the gate on its updater.
	uc := NewUpdateController(NewFailoverController(testLogger()), testLogger())
	NewTopologyManager(testTopologyConfig(), []mysql.Checker{g.primary, g.site}, NewFailoverController(testLogger()), uc, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	if uc.rejoinCheck == nil || uc.replicaStartGate == nil {
		t.Fatal("the updater has no rejoin check or no replication start gate")
	}
}

// A manager that starts into an outage chooses a promotion target without
// waiting on the API server, so it must have the sites' recorded
// server_uuids by then: they are loaded when the manager starts, not by
// the first comparison that happens to store one (DST seed 9458: a
// replaced manager promoted a site whose earlier identity it had not
// loaded).
func TestOwnLoss_ManagerStartedIntoAnOutageKnowsTheEarlierUUIDs(t *testing.T) {
	build := func() (*TopologyManager, *memSiteIdentityStore) {
		// dc2's data directory was replaced under a new server_uuid, on
		// older data; dc3 holds what dc2 issued under the earlier one.
		dc2 := fencedLossServer(lossUUIDRe, lossUUIDOld+":1-50,"+lossUUIDNew+":1-15")
		dc3 := fencedLossServer(lossUUIDThird, lossUUIDOld+":1-80,"+lossUUIDNew+":1-15")
		tm := evidenceGroup(t, dc2, dc3, state.SiteRoleDROnly)
		store := &memSiteIdentityStore{uuids: map[string][]string{"dc2": {lossUUIDOld}}}
		tm.SetSiteIdentityStore(store)
		return tm, store
	}

	tm, _ := build()
	if err := tm.LoadSiteIdentities(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "" {
		t.Fatalf("picked %q: dc2 lacks %s:51-80, issued under a server_uuid the group has on record for it", got, lossUUIDOld)
	}

	// The control shows what the load is for: without it the picker,
	// which does not read the store, knows only the uuid dc2 runs under.
	tm, _ = build()
	if got := tm.pickFreshestCandidate(context.Background(), []string{"dc2"}); got != "dc2" {
		t.Fatalf("picked %q without the history loaded; the control no longer shows the difference", got)
	}

	// A load that fails is retried off the poll loop.
	tm, store := build()
	store.loadErr = errors.New("the API server is away")
	if err := tm.LoadSiteIdentities(context.Background()); err == nil {
		t.Fatal("LoadSiteIdentities = nil with a store that cannot be read")
	}
	store.mu.Lock()
	store.loadErr = nil
	store.mu.Unlock()
	tm.siteIdentity.mu.Lock()
	tm.siteIdentity.nextTry = time.Time{}
	tm.siteIdentity.mu.Unlock()
	tm.loadSiteIdentitiesInBackground(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		tm.siteIdentity.mu.Lock()
		loaded := tm.siteIdentity.loaded
		tm.siteIdentity.mu.Unlock()
		if loaded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the background load did not load the history")
		}
		time.Sleep(time.Millisecond)
	}
}

// Source convergence looks at a recorded report before anything else. The
// replication start gate would refuse the site too, but only convergence's
// own look gives the site the right status while a hold stands: without it
// a site that lacks its own transactions reads Pending/SourceMismatch, as
// if its channel were about to be repointed when the hold ends.
func TestOwnLoss_ConvergenceReportsARecordedSiteDuringAHold(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossUUIDNew+":1-5,"+lossUUIDOld+":1-90", false)
	f.site.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1"}
	pollN(f.tm, 2)
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.setHold(true)
	pollN(f.tm, 3)
	st := f.tm.Status()
	if st.Sites[1].SourceConvergenceState != sourceConvergenceBlocked || st.Sites[1].SourceConvergenceReason != v1alpha1.SourceReasonOwnTransactionsLost {
		t.Fatalf("source convergence during the hold = %s/%s, want Blocked/OwnTransactionsLost", st.Sites[1].SourceConvergenceState, st.Sites[1].SourceConvergenceReason)
	}
	if changes, starts, _, _ := f.site.counts(); starts != 0 || changes != 0 {
		t.Fatalf("the channel was touched during the hold (START=%d CHANGE=%d)", starts, changes)
	}

	// A site whose report rests on a witness is not held back: against
	// the primary it lacks nothing, and its stopped channel is restarted,
	// also once the report stands.
	g := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	g.dc2.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1"}
	g.poll(t, 6)
	if rec := ownLossOf(g.tm, "dc2"); rec == nil || !rec.witness {
		t.Fatalf("dc2 own-loss record = %+v, want a witness report", rec)
	}
	// Its channel stops again (a restart with --skip-replica-start).
	g.dc2.mockMySQL.mu.Lock()
	g.dc2.mockMySQL.replicaStatusVal = &mysql.ReplicaStatus{SourceHost: "mysql-dc1"}
	g.dc2.mockMySQL.mu.Unlock()
	_, before, _, _ := g.dc2.counts()
	g.poll(t, 3)
	if _, after, _, _ := g.dc2.counts(); after <= before {
		t.Fatalf("START REPLICA calls %d -> %d: the stopped channel of a site whose report rests on a witness is no longer restarted", before, after)
	}
}
