package component

import (
	"errors"
	"slices"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// A DST finding of the sweeps of the merged tree, pinned by its shape like
// the ones in dst_regression_test.go.

// TestDSTPF_RollbackLeavesASourceFencedThatLostWhatAnotherSiteHolds pins
// PromotedWithoutOwnTransactions by a planned failover's rollback. The
// shrunk trial: the run is accepted; before its fence is read, dc1 restarts
// on an older data directory under the same server_uuid and comes up
// fenced, so the fenced set the run records is already the short one; the
// target dc2 is cut off from the operator, the lag wait times out and the
// run rolls back. The rollback compared the source with the fenced set and
// with the target only. The fenced set is short and the target does not
// answer, so it unfenced dc1, although dc3, a disaster-recovery site that
// answers, holds the transactions dc1 issued and lost: dc1 would issue
// those GTIDs again.
//
// The rollback now compares the source with every site that answers, by
// the rules of every other promotion decision. dc1 stays fenced, and the
// primary re-assert, which the run names it for, refuses it for the same
// reason once every site answers again and reports what it lacks.
func TestDSTPF_RollbackLeavesASourceFencedThatLostWhatAnotherSiteHolds(t *testing.T) {
	short := pfUUIDA + ":1-97"
	lost := pfUUIDA + ":98-100"
	down := errors.New("dial tcp: i/o timeout")
	unfenced := func(h *pfHarness) bool {
		got := h.src.statements()
		return slices.Contains(got, "SetSuperReadOnly:off") || slices.Contains(got, "SetReadOnly:off")
	}
	// rolledBack runs the trial up to the terminal record.
	rolledBack := func(t *testing.T, witnessHolds bool) (*pfHarness, *v1alpha1.PlannedFailoverStatus) {
		h := newPFHarness(t, pfOpts{third: state.SiteRoleDROnly})
		h.request("dc2")
		h.runUntil(20, inPhase(v1alpha1.PlannedFailoverPhaseFencing))
		h.src.setGtidExecuted(short)
		h.src.respawn(true)
		h.src.setRO(true, true) // its sidecar fences it at boot
		h.tgt.setError(down)
		if !witnessHolds {
			h.third.setGtidExecuted(short) // it never received them
		}
		pf := h.runUntil(120, terminal)
		if pf.Fence == nil || pf.Fence.GtidSet == nil || *pf.Fence.GtidSet != short {
			t.Fatalf("setup: fence %+v, want the fenced set read after the restart (%s); store log:\n%s", pf.Fence, short,
				strings.Join(h.store.LogStrings(), "\n"))
		}
		return h, pf
	}
	t.Run("a disaster-recovery site holds them", func(t *testing.T) {
		h, pf := rolledBack(t, true)
		if pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
			t.Fatalf("outcome %s (%s), want SourceLeftFenced; store log:\n%s", pf.Result.Outcome, pf.Result.Reason,
				strings.Join(h.store.LogStrings(), "\n"))
		}
		if unfenced(h) || !h.src.isReadOnly() {
			t.Fatalf("source statements %v: the rollback unfenced a source that lacks %s, which dc3 holds", h.src.statements(), lost)
		}
		if h.holdCondition() {
			t.Fatal("a hold over a source that stayed fenced")
		}
		// Every site answers again and is read-only: the state the primary
		// re-assert heals, and the run named dc1 for it. dc1 is not lifted.
		h.tgt.setError(nil)
		for range 30 {
			h.tick()
			h.pass()
			if !h.src.isReadOnly() {
				t.Fatalf("the re-assert made dc1 writable without %s; dc1 statements %v", lost, h.src.statements())
			}
		}
		reported := ""
		for _, s := range h.tm.Status().Sites {
			if s.Name == "dc1" {
				reported = s.LostGtid
			}
		}
		if reported != lost {
			t.Fatalf("dc1 reported lost %q, want %s", reported, lost)
		}
	})
	t.Run("no site that answers holds them", func(t *testing.T) {
		// The documented residual: the only holder of the lost
		// transactions is the target, which is out of reach. The operator
		// has no evidence, and the rollback unfences the source as before.
		h, pf := rolledBack(t, false)
		if pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack || !unfenced(h) {
			t.Fatalf("outcome %s (%s), source statements %v; want the source unfenced: nothing that answers shows a loss",
				pf.Result.Outcome, pf.Result.Reason, h.src.statements())
		}
	})
}
