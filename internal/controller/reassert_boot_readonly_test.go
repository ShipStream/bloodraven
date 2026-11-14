package controller

import (
	"context"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
)

// After a total loss of a group that never failed over, the primary is
// re-asserted on the strength of its boot-only read_only (no failover
// record names it). Its sidecar's startup safety net fenced it
// (super_read_only=ON) a second after the re-assert, because /active-site
// named no site yet; every later re-assert then refused the fenced,
// unnamed site and the group stayed with no writable site (scenario 11,
// #236). The re-assert now names its target, so the next one lifts a
// fence that lands anyway.
func TestPrimaryReassert_BootReadOnlyTargetSurvivesSafetyNetFence(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	site0 := &mockMySQL{readOnly: true, gtidExecuted: gtid}
	site1 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: gtid}
	clk := clock.NewFakeClock(time.Date(2026, 11, 14, 0, 10, 0, 0, time.UTC))
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(site0, site1, clk)
	tm.failoverCooldown = 30 * time.Second
	setWedgedTopology(tm, "")
	tm.mu.Lock()
	tm.lastActiveSite, tm.lastActiveSiteNamed = "dc1", false // restored from status only
	tm.mu.Unlock()
	ctx := context.Background()

	if !tm.checkPrimaryReassert(ctx) {
		t.Fatal("the boot read-only primary was not re-asserted")
	}

	// The safety net fences dc1 before a poll reads it writable.
	site0.mu.Lock()
	site0.superReadOnly, site0.readOnly = true, true
	site0.mu.Unlock()
	tm.mu.Lock()
	tm.deploymentAwaitingObservation = false // a poll observed the re-assert
	tm.mu.Unlock()
	clk.Advance(tm.failoverCooldown + time.Second)

	if !tm.checkPrimaryReassert(ctx) {
		t.Fatal("the re-asserted primary, fenced afterwards, was not re-asserted again")
	}
	site0.mu.Lock()
	ro := site0.readOnly
	site0.mu.Unlock()
	if ro {
		t.Fatal("dc1 is still read-only after the second re-assert")
	}
}
