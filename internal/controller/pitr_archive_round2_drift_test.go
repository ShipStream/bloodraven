package controller

import (
	"context"
	"testing"
	"time"
)

// Re-review N4 (Opus): a new topology manager has not computed its
// spec-drift set until the runner's next sync. Until then a disabled
// sidecar is treated as awaiting rollout, not as ArchiverDisabled.
func TestPITRArchiveDriftNotYetComputedIsAwaitingRollout(t *testing.T) {
	tm := &TopologyManager{}
	if _, known := tm.SpecDriftView(); known {
		t.Fatal("a new topology manager reports its drift set as computed")
	}
	tm.SetSpecDriftSites(nil)
	if _, known := tm.SpecDriftView(); !known {
		t.Fatal("drift set still unknown after SetSpecDriftSites")
	}

	fg := pitrArchiveFG(nil)
	nn := FailoverGroupNamespacedName(fg)
	_, c, rec, p := pitrArchiveRunner(t, fg, pitrRolloutServers(t))
	known := false
	p.rolloutKnown = func() bool { return known }
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		at := t0.Add(time.Duration(i) * 30 * time.Second)
		p.now = func() time.Time { return at }
		p.round(context.Background())
	}
	cond, _ := pitrCondition(t, c, nn)
	if cond == nil || cond.Reason != reasonPITRAwaitingRollout {
		t.Fatalf("condition = %+v, want Unknown/%s before the drift set is known", cond, reasonPITRAwaitingRollout)
	}
	if w, _ := countPITREvents(drainEventsPITR(rec)); w != 0 {
		t.Fatalf("%d Warnings before the drift set is known", w)
	}
	known = true
	for i := 3; i < 5; i++ {
		at := t0.Add(time.Duration(i) * 30 * time.Second)
		p.now = func() time.Time { return at }
		p.round(context.Background())
	}
	if cond, _ = pitrCondition(t, c, nn); cond.Reason != reasonPITRArchiverDisabled {
		t.Fatalf("condition = %+v, want False/%s once the drift set is known and empty", cond, reasonPITRArchiverDisabled)
	}
}
