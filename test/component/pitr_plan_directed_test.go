package component

import (
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// TestPITRPlanPropertyLostReturnsInUnreadFiles is a directed history for
// the property sweep's oracle (pitr_plan_property_test.go), which the
// random generator does not produce: the transactions b's era lost return
// with b, and sit in files the plan does not read. b commits B:1, which a
// receives, and B:2-4, one per binlog file; the failover to a loses B:2-4,
// which b's sealed files hold. b is promoted again without a reclone (the
// operator reclones such a site first), so the newest archived state holds
// B:2-4. With them excluded, the three files hold nothing to replay and
// are not downloaded, and coverage.json's lost list must still name all of
// B:2-4: a planner that reports only the lost transactions of the files it
// downloaded reports none of them.
func TestPITRPlanPropertyLostReturnsInUnreadFiles(t *testing.T) {
	p := newPropTrial(t, propConfirmDelay)
	for range 3 {
		p.commit("a")
	}
	p.takeBackup("b")
	p.commit("a")
	p.failover("a", "b", 0, false, propPromotion{rotate: true})
	p.commit("b")
	p.drain("a", 0)
	for range 3 {
		p.srv["b"].rotate()
		p.commit("b")
	}
	p.failover("b", "a", 3, true, propPromotion{rotate: true})
	// Past the probe interval, so that a's record precedes its commits.
	time.Sleep(20 * time.Millisecond)
	for range 2 {
		txn := p.newCommit("a")
		p.srv["a"].apply(txn)
		p.srv["b"].apply(txn)
		p.copied("b", []binlogtest.Txn{txn})
	}
	p.srv["a"].rotate()
	p.failover("a", "b", 0, false, propPromotion{rotate: true})
	for range 2 {
		p.commit("b")
	}
	p.srv["b"].rotate()
	p.waitUploaded("b")
	p.targets = nil
	lost := pitrplan.MustParseSet(writerUUIDB + ":2-4")
	if !p.dead.Equal(lost) {
		t.Fatalf("the history lost %s, want %s", p.dead, lost)
	}
	o := newPropOracle(t, p, time.Microsecond)
	o.checkRecords("directed")
	checked := false
	for _, r := range o.run("directed") {
		if r.mode != pitrplan.ModeLatest {
			continue
		}
		if r.X.IsEmpty() {
			ref, ok := pitrplan.RefusalOf(r.err)
			if !ok || ref.Token != pitrarchive.TokenArchiveDivergentHistory || !r.plan.Divergent.Equal(lost) {
				t.Fatalf("latest without exclusions: %+v %v, want PITRArchiveDivergentHistory naming %s", r.plan, r.err, lost)
			}
			continue
		}
		checked = true
		if r.err != nil || !r.plan.Lost.Equal(lost) || !r.plan.Lineage.Contains(lost) {
			t.Fatalf("latest excluding %s: %+v %v, want a plan that reports them all lost", r.X, r.plan, r.err)
		}
		// The files that hold only B:2-4 are not all downloaded.
		if r.plan.Downloaded >= len(o.rows) {
			t.Fatalf("latest excluding %s downloaded %d of %d rows; the fixture means files of lost transactions to stay unread", r.X, r.plan.Downloaded, len(o.rows))
		}
	}
	if !checked {
		t.Fatal("no latest plan with the lost transactions excluded")
	}
}
