package dst

import (
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/state"
)

// The anti-flap tests below use hand-built trials rather than seeds.
//
// The conjunction they need — a status-write outage that is live at the
// moment of a promotion AND spans an operator restart — occurs in roughly
// 0.2% of generated trials, so leaving it to random search would make the
// regression gate a lottery. The campaign still finds these; these fix them
// in place.

// antiFlapTrial builds a two-site trial that promotes beta while alpha is
// down, restarts the operator inside the cooldown, and then takes beta down
// too so the restarted process has a reason to promote again.
//
// The restarted process must refuse: the cooldown from beta's promotion is
// still running, and the only question is whether it knows that.
func antiFlapTrial(extraOps ...Op) Trial {
	const (
		warmup   = 8
		cooldown = 60
		healAt   = 40
	)
	ops := []Op{
		{At: 10, Kind: OpCrash, Site: "alpha", Fenced: true},   // beta gets promoted
		{At: 22, Kind: OpRecover, Site: "alpha", Fenced: true}, // alpha returns as a replica
		{At: 26, Kind: OpRestartOperator},                      // fresh process, inside the cooldown
		{At: 28, Kind: OpCrash, Site: "beta", Fenced: true},    // now it wants alpha
		{At: 34, Kind: OpRecover, Site: "beta", Fenced: true},
	}
	ops = append(ops, extraOps...)
	// Ops must be applied in schedule order; the harness relies on it.
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0 && ops[j].At < ops[j-1].At; j-- {
			ops[j], ops[j-1] = ops[j-1], ops[j]
		}
	}
	return Trial{
		SiteNames:   []string{"alpha", "beta"},
		Roles:       []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate},
		CooldownSec: cooldown,
		WarmupPolls: warmup,
		HealAt:      healAt,
		Polls:       healAt + cooldown + 55,
		// A long lease keeps the sidecars from adding their own fencing
		// noise to a test about operator bookkeeping; they still run, and
		// still fence a stale primary via the topology rule.
		SidecarLeasePolls: 30,
		SidecarTopology:   true,
		Ops:               ops,
	}
}

func violationsNamed(vs []Violation, name string) []Violation {
	var out []Violation
	for _, v := range vs {
		if v.Invariant == name {
			out = append(out, v)
		}
	}
	return out
}

// TestAntiFlapSurvivesStatusOutageAcrossRestart is the regression gate for
// the durable out-of-band anti-flap record.
//
// With status writes denied for the whole fault window, the CR status copy
// of lastFailover never lands. If that were the only durable copy, the
// process that starts at poll 26 would rehydrate nothing, treat the cooldown
// as expired, and promote alpha the moment beta dies at poll 28 — the
// CooldownViolated(restart) class. The out-of-band store is what makes it
// refuse.
func TestAntiFlapSurvivesStatusOutageAcrossRestart(t *testing.T) {
	trial := antiFlapTrial(Op{At: 9, Kind: OpStatusOutage})
	r := RunTrial(trial, true, nil)

	if len(r.PromotionPolls) == 0 {
		t.Fatalf("trial never promoted, so it proves nothing; trial:\n%s", trial)
	}
	if got := violationsNamed(r.Violations, "AntiFlapStateLost"); len(got) > 0 {
		t.Errorf("the restart lost the anti-flap record while the out-of-band store was healthy: %v", got)
	}
	if got := violationsNamed(r.Violations, "CooldownViolated"); len(got) > 0 {
		t.Errorf("cooldown not enforced across the restart: %v", got)
	}
	if len(r.LostStatePolls) > 0 {
		t.Errorf("restart at polls %v came up without the durable record", r.LostStatePolls)
	}
	if r.FinalFailoverRecord == nil || r.FinalFailoverRecord.LastFailoverTarget == "" {
		t.Errorf("out-of-band store holds no record after %d promotions", len(r.PromotionPolls))
	}
	for _, v := range r.Violations {
		t.Logf("violation: %s", v)
	}
}

// TestAntiFlapLostWhenEveryDurablePathIsDenied pins the residue: with BOTH
// durable paths rejecting writes, nothing can carry the cooldown across a
// restart, and the harness must classify that as inherent rather than as a
// regression. If this ever starts reporting AntiFlapStateLost, the
// classification has drifted and the expected class would start masking real
// findings.
func TestAntiFlapLostWhenEveryDurablePathIsDenied(t *testing.T) {
	trial := antiFlapTrial(
		Op{At: 9, Kind: OpStatusOutage},
		Op{At: 9, Kind: OpStateOutage},
	)
	r := RunTrial(trial, true, nil)

	if len(r.PromotionPolls) == 0 {
		t.Fatalf("trial never promoted; trial:\n%s", trial)
	}
	if len(r.LostStatePolls) == 0 {
		t.Fatalf("both durable paths denied, yet the restart still rehydrated the record — the outage is not reaching the store")
	}
	if got := violationsNamed(r.Violations, "AntiFlapStateLost"); len(got) > 0 {
		t.Errorf("losing the record with every durable path denied is inherent, not a violation: %v", got)
	}
	// With no durable record surviving the restart, the second promotion is
	// inherent to the schedule — requiring it keeps this test from passing
	// vacuously when the trial stops promoting at all.
	if len(r.PromotionPolls) < 2 {
		t.Fatalf("expected a second promotion after the restart, got %v", r.PromotionPolls)
	}
	cv := violationsNamed(r.Violations, "CooldownViolated")
	if len(cv) == 0 {
		t.Fatal("expected the inherent cooldown violation; the trial is no longer exercising the lost-state class")
	}
	if fp := fingerprint(cv); fp != "CooldownViolated(restart+stateLost)" {
		t.Errorf("cooldown violation fingerprinted as %q, want the inherent class; detail: %v", fp, cv)
	}
}

// TestAntiFlapStateLostInvariantIsArmed guards the guard. AntiFlapStateLost
// only ever fires on a stale rehydrate, so a harness change that quietly
// stopped tracking promotions would make it vacuous and every other
// anti-flap assertion here would keep passing. Feeding it a stale record
// directly proves it still bites.
func TestAntiFlapStateLostInvariantIsArmed(t *testing.T) {
	// Every case feeds the same stale rehydrate — a promotion the harness
	// saw and a process that came up knowing nothing about it — and varies
	// only which durable path could have preserved the record.
	cases := []struct {
		name string
		// stateDenied is an unhealed rejection on the out-of-band store;
		// statusDropped is the promoting process landing a status write
		// that still predates its own promotion.
		stateDenied    bool
		statusDropped  bool
		wantViolations int
		wantDetail     string
	}{
		{
			name:           "store healthy is a regression",
			wantViolations: 1,
			wantDetail:     "accepting writes",
		},
		{
			// Losing the record with the store denied is inherent, so the
			// poll is still recorded as lost but nothing is flagged.
			name:           "store denied is inherent",
			stateDenied:    true,
			wantViolations: 0,
		},
		{
			// The status path could have preserved it, so the loss is a
			// regression again even with the store denied.
			name:           "store denied but status dropped the record",
			stateDenied:    true,
			statusDropped:  true,
			wantViolations: 1,
			wantDetail:     "dropped the record",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &trialRunner{
				trial:                     antiFlapTrial(),
				lastPromotionAt:           mustFakeNow(),
				stateDeniedSincePromotion: tc.stateDenied,
				statusDroppedPromotion:    tc.statusDropped,
			}

			r.checkRehydratedAntiFlap(42)

			// Recorded as lost in every case: that bookkeeping is what
			// separates an inherent reset from a regression, so it must
			// not depend on which path was down.
			if len(r.lostStatePolls) != 1 || r.lostStatePolls[0] != 42 {
				t.Errorf("stale rehydrate not recorded: %v", r.lostStatePolls)
			}
			got := violationsNamed(r.violations, "AntiFlapStateLost")
			if len(got) != tc.wantViolations {
				t.Fatalf("want %d AntiFlapStateLost, got %v", tc.wantViolations, r.violations)
			}
			if tc.wantDetail != "" && !strings.Contains(got[0].Detail, tc.wantDetail) {
				t.Errorf("detail should explain why this is a regression (%q), got %q", tc.wantDetail, got[0].Detail)
			}
		})
	}
}

// mustFakeNow is the trial clock's epoch; any instant strictly after the
// zero record is enough to make a rehydrate stale.
func mustFakeNow() time.Time {
	return time.Date(2030, 1, 1, 0, 1, 0, 0, time.UTC)
}
