package controller

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The planned engine's Succeeded terminal write is one of the two writers
// that persist status.activeSite, so it must report the committed move to
// the D-46 PITR archive reissue warning, once, against the persisted
// pre-write value.
func TestPlannedFailoverEngine_ReportsPersistedActiveSiteMove(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePublishing)
	// The manager has promoted the target: its authority names pdx at a
	// newer topology generation than the persisted status.
	h.topo.authority = &PlannedAuthority{ActiveSite: "pdx", TopologyGeneration: h.fg().Status.TopologyGeneration + 1}
	type call struct{ from, to string }
	var calls []call
	h.e.activeSitePersisted = func(committed *v1alpha1.MysqlFailoverGroup, from string) {
		calls = append(calls, call{from: from, to: committed.Status.ActiveSite})
	}
	h.step()
	if pf := h.pf(); pf == nil || pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("block = %+v, want Succeeded", pf)
	}
	var moves []call
	for _, c := range calls {
		if c.from != c.to {
			moves = append(moves, c)
		}
	}
	if len(moves) != 1 || moves[0] != (call{from: "iad", to: "pdx"}) {
		t.Fatalf("activeSite moves reported = %+v (all calls %+v), want exactly iad -> pdx", moves, calls)
	}
	if got := h.fg().Status.ActiveSite; got != "pdx" {
		t.Fatalf("persisted activeSite = %q, want pdx", got)
	}
}
