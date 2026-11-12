package controller

import (
	"context"
	"testing"
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
	if !f.tm.nameFencedSourceLocked("run-1", "dc2", boundUUID) {
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
	t.Run("a standing report on the run's server refuses before anything is read", func(t *testing.T) {
		f := plannedNamedGroup(t, whole, whole, lossUUIDOld)
		f.tm.mu.Lock()
		f.tm.ownLoss = map[string]*ownLossRecord{"dc2": {lostGtid: lossLostGTID, lostCount: 10, comparedTo: "dc1",
			detectedBy: divergenceDetectedByRecovery}}
		f.tm.mu.Unlock()
		if f.tm.checkPrimaryReassert(ctx) || siteWritable(f.site) {
			t.Fatal("the fence was lifted from a source that carries a report against the primary")
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
