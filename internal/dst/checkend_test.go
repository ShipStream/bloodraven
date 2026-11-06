package dst

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestCheckEnd_HoldKeepsTransactionAccounting: a standing automation hold
// exempts the end state from convergence, not from transaction accounting
// (K059). An acknowledged transaction on some site is the hold's to report;
// one that exists on no site was destroyed while automation was held.
func TestCheckEnd_HoldKeepsTransactionAccounting(t *testing.T) {
	setup := func() *trialRunner {
		tr := twoSiteHandTrial(15, true, 20)
		tr.Actors = Actors{Enabled: true}
		r := &trialRunner{trial: tr, logger: discardLogger(), operatorDeadUntil: -1,
			kindsSeen: map[string]struct{}{}, reasonsSeen: map[string]struct{}{}}
		r.setup(false)
		for range 3 {
			r.cluster.Tick() // acked application writes on alpha
		}
		s := r.pl.store
		s.mu.Lock()
		st := s.obj.Status.DeepCopy()
		setHoldRecord(st, "r1", "alpha", "beta")
		s.commitStatusLocked("test", *st)
		s.mu.Unlock()
		if !r.plannedHoldStands() {
			t.Fatal("hold not standing")
		}
		return r
	}
	has := func(vs []Violation, inv string) bool {
		for _, v := range vs {
			if v.Invariant == inv {
				return true
			}
		}
		return false
	}

	if vs := setup().checkEnd(); has(vs, "SilentDataLoss") {
		t.Fatalf("held, every acked transaction on a site: %v", vs)
	}

	r := setup()
	r.cluster.mu.Lock()
	for _, s := range r.cluster.sites {
		s.executed = gtidVec{} // the acked history is gone from every site
	}
	r.cluster.mu.Unlock()
	if vs := r.checkEnd(); !has(vs, "SilentDataLoss") {
		t.Fatalf("held, acked transactions on no site: violations %v, want SilentDataLoss", vs)
	}
}

// TestCheckEnd_UnrecordedPlannedPromotionStampsNoCooldown: a planned run
// that never recorded its promotion (ManualRecoveryRequired here) stamps no
// cooldown (§10.1), so automation acting after an acknowledged hold is not
// CooldownViolated; a recorded (Succeeded) run's promotion still is.
func TestCheckEnd_UnrecordedPlannedPromotionStampsNoCooldown(t *testing.T) {
	for _, tc := range []struct {
		outcome v1alpha1.PlannedFailoverOutcome
		want    bool
	}{
		{v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, false},
		{v1alpha1.PlannedFailoverOutcomeSucceeded, true},
	} {
		tr := twoSiteHandTrial(15, true, 20)
		tr.Actors = Actors{Enabled: true}
		r := &trialRunner{trial: tr, logger: discardLogger(), operatorDeadUntil: -1,
			kindsSeen: map[string]struct{}{}, reasonsSeen: map[string]struct{}{}}
		r.setup(false)
		r.pl.mon.runs["r1"] = &runObs{id: "r1", terminal: true, outcome: tc.outcome}
		r.promotionPolls = []int{13, 16} // 3 polls apart, cooldown 10
		r.promotionActors = []Actor{ActorPF, ActorTM}
		r.promotionRuns = []string{"r1", ""}
		got := false
		for _, v := range r.checkEnd() {
			if v.Invariant == "CooldownViolated" {
				got = true
			}
		}
		if got != tc.want {
			t.Errorf("planned run %s, then a manager promotion inside the cooldown: CooldownViolated=%v, want %v", tc.outcome, got, tc.want)
		}
	}
}
