package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// R5-A: dc2 once had a clone issued that never installed (failed/killed): its
// timeline is [Old, clone] and it still runs under Old. Same history otherwise
// as TestOwnLoss_AuditFindsASiteThatLacksWhatADivergentFollowerHolds.
func TestR5_AuditCrossCheckAfterAFailedClone(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	f.tm.noteSiteServerUUID(context.Background(), "dc2", lossUUIDOld)
	f.tm.markSiteCloned(context.Background(), "dc2")
	f.poll(t, 6)
	if got := recoveryOf(f.tm, "dc3"); got.state != v1alpha1.RecoveryStateBlocked {
		t.Fatalf("dc3 recovery = %+v", got)
	}
	rec := ownLossOf(f.tm, "dc2")
	if rec == nil || !rec.witness {
		t.Errorf("dc2 own-loss record = %+v, want a witness report (it runs under Old and would reissue Old:91-100)", rec)
	}
	if err := f.tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Errorf("handoff to dc2 is not refused")
	}
}

// R5-C: a witness report across an operator restart (or a manager replaced
// by a spec change). The runner restores status.sites[].lostGtid with
// SetOwnTransactionsLost; the report's kind (witness) is not stored. The
// witness dc3 does not answer after the restart.
func TestR5_WitnessReportAcrossRestartWithSilentWitness(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessHolderSet)
	// The restart: what startManager restores from status.
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.tm.SetRecoveryBlockedFrom("dc3", lossLostGTID, 10, divergenceDetectedByAudit, "dc1")
	// dc3 is down.
	f.dc3.mockMySQL.mu.Lock()
	f.dc3.mockMySQL.err = errors.New("dc3 down")
	f.dc3.mockMySQL.gtidExecutedErr = errors.New("dc3 down")
	f.dc3.mockMySQL.mu.Unlock()
	f.dc3.lmu.Lock()
	f.dc3.snapErr = errors.New("dc3 down")
	f.dc3.lmu.Unlock()
	if ownLossOf(f.tm, "dc2") == nil {
		t.Fatal("no restored report")
	}
	f.poll(t, 6)
	f.clk.Advance(gtidAuditInterval)
	f.poll(t, 6)
	rec := ownLossOf(f.tm, "dc2")
	t.Logf("dc2 record after restart: %+v", rec)
	for _, m := range f.logged(t, "own transactions lost cleared: the site holds every transaction it issued") {
		t.Logf("cleared: %v", m)
	}
	if rec == nil {
		t.Errorf("dc2's witness report did not survive the restart although dc3 never answered")
	}
	if err := f.tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Errorf("handoff to dc2 is not refused after the restart")
	}
}

// R5-D: the same split brain as TestOwnLoss_WritableReportedSiteIsFencedInASplitBrain,
// in a group with failover history that names the reported site (it was the
// last failover target; dc1 became writable without a recorded failover).
func TestR5_WritableReportedSiteWithFailoverHistory(t *testing.T) {
	f := newWitnessFixture(t, witnessPrimarySet, witnessPrimarySet, witnessPrimarySet)
	f.tm.SetLastFailoverTarget("dc2")
	f.poll(t, 3)
	f.tm.SetOwnTransactionsLost("dc2", lossLostGTID, 10)
	f.dc2.mockMySQL.mu.Lock()
	f.dc2.mockMySQL.readOnly, f.dc2.mockMySQL.superReadOnly = false, false
	f.dc2.mockMySQL.mu.Unlock()
	f.poll(t, 8)
	t.Logf("dc1 writable=%v dc2 writable=%v dc2 report=%+v", siteWritable(f.dc1), siteWritable(f.dc2), ownLossOf(f.tm, "dc2"))
	if siteWritable(f.dc2) && !siteWritable(f.dc1) {
		t.Errorf("the reported site dc2 was kept writable and the sound site dc1 was fenced")
	}
}

// R5-B: an operator restart on the status an older CRD stores for a
// RecloneRequired site: recoveryState RecoveryBlocked, no divergentGtid, no
// lostGtid (degradeStatusForOlderCRD).
func TestR5_RestartOnDegradedStatus(t *testing.T) {
	f := newLossFixture(t, lossPrimaryGTID, lossReturnGTID, true)
	f.tm.SetRecoveryBlockedFrom("dc2", "", 0, "", "")
	for i := 0; i < 12; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
		f.clk.Advance(time.Second)
	}
	_, _, clones, _ := f.site.counts()
	t.Logf("clones=%d loss=%+v recovery=%+v", clones, f.loss(), recoveryOf(f.tm, "dc2"))
	if clones != 1 {
		t.Errorf("clones = %d, want the automatic reclone to run", clones)
	}
	f.clk.Advance(2 * gtidAuditInterval)
	for i := 0; i < 12; i++ {
		f.tm.Poll(context.Background())
		f.waitBootstrapEnd(t)
		f.clk.Advance(time.Second)
	}
	_, _, clones, _ = f.site.counts()
	t.Logf("later: clones=%d loss=%+v recovery=%+v", clones, f.loss(), recoveryOf(f.tm, "dc2"))
}
