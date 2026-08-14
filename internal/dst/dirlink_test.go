package dst

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/state"
)

// Two-site cluster in the same steady state NewCluster uses for trials:
// alpha writable, beta a caught-up replica of alpha.
func twoSiteCluster() *Cluster {
	return NewCluster([]SiteSpec{
		{Name: "alpha", Host: "alpha.mysql.sim", LBIP: "10.0.0.1", UUID: "00000000-0000-0000-0000-000000000001"},
		{Name: "beta", Host: "beta.mysql.sim", LBIP: "10.0.0.2", UUID: "00000000-0000-0000-0000-000000000002"},
	}, 100)
}

func truthNamed(truth []SiteTruth, name string) SiteTruth {
	for _, t := range truth {
		if t.Name == name {
			return t
		}
	}
	return SiteTruth{}
}

func eventsOfKind(events []Event, kind EventKind) []Event {
	var out []Event
	for _, e := range events {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func fencePolls(events []Event, site string) []int {
	var out []int
	for _, e := range events {
		if e.Kind == EvSidecarFence && e.Site == site {
			out = append(out, e.Poll)
		}
	}
	return out
}

func TestDirKeyIsOrdered(t *testing.T) {
	if dirKey("alpha", "beta") == dirKey("beta", "alpha") {
		t.Fatal("dirKey sorts its arguments; one-way reachability is unrepresentable")
	}
	if dirKey("alpha", "beta") != "alpha>beta" {
		t.Fatalf("dirKey(alpha,beta)=%q", dirKey("alpha", "beta"))
	}
}

func TestSetDirLink_BlocksReplicaFetchOnlyThatDirection(t *testing.T) {
	c := twoSiteCluster()
	c.SetCapture(true)
	c.BeginPoll(1)

	// Replica-initiated IO: beta → alpha down stops beta fetching.
	c.SetDirLink("beta", "alpha", true)
	if !c.linkDown("beta", "alpha") {
		t.Fatal("beta→alpha should be down")
	}
	if c.linkDown("alpha", "beta") {
		t.Fatal("alpha→beta should still be up")
	}
	beta := truthNamed(c.Truth(), "beta")
	if beta.IORunning {
		t.Fatal("beta IO should be down when beta cannot reach alpha")
	}
	if !beta.SQLRunning {
		t.Fatal("SQL thread is local; a directed cut must not stop it")
	}

	// Reverse cut alone does not stop replica-initiated fetch.
	c.SetDirLink("beta", "alpha", false)
	c.SetDirLink("alpha", "beta", true)
	beta = truthNamed(c.Truth(), "beta")
	if !beta.IORunning {
		t.Fatal("beta IO should stay up when only alpha→beta is cut")
	}

	// A tick with beta→alpha down must not advance beta's retrieved set.
	c.SetDirLink("alpha", "beta", false)
	c.SetDirLink("beta", "alpha", true)
	before := truthNamed(c.Truth(), "beta").Retrieved.clone()
	c.Tick() // alpha writes one txn
	after := truthNamed(c.Truth(), "beta")
	if after.Retrieved.String() != before.String() {
		t.Fatalf("beta retrieved moved under a beta→alpha cut: before=%s after=%s", before, after.Retrieved)
	}
	alpha := truthNamed(c.Truth(), "alpha")
	if after.Retrieved.contains(alpha.Executed) {
		t.Fatal("beta fetched alpha's new write across a down replica→source link")
	}

	// Same tick with the reverse cut must fetch.
	c.SetDirLink("beta", "alpha", false)
	c.SetDirLink("alpha", "beta", true)
	c.Tick()
	beta = truthNamed(c.Truth(), "beta")
	alpha = truthNamed(c.Truth(), "alpha")
	if !beta.Retrieved.contains(alpha.Executed) {
		t.Fatalf("beta should fetch across an alpha→beta cut: retrieved=%s executed=%s", beta.Retrieved, alpha.Executed)
	}
}

func TestSetPairLink_IsTwoOneWayCuts(t *testing.T) {
	c := twoSiteCluster()
	c.SetCapture(true)
	c.BeginPoll(1)
	c.SetPairLink("alpha", "beta", true)
	if !c.linkDown("alpha", "beta") || !c.linkDown("beta", "alpha") {
		t.Fatal("SetPairLink must cut both directions")
	}
	if n := len(eventsOfKind(c.Events(), EvPartPair)); n != 1 {
		t.Fatalf("pair cut should emit one EvPartPair, got %d", n)
	}
	if n := len(eventsOfKind(c.Events(), EvPartOneWay)); n != 0 {
		t.Fatalf("pair sugar must not emit one-way events (got %d); those mean OpPartOneWay", n)
	}

	c.SetPairLink("alpha", "beta", false)
	if c.linkDown("alpha", "beta") || c.linkDown("beta", "alpha") {
		t.Fatal("SetPairLink(false) must heal both directions")
	}
}

func TestSetDirLink_HealIsDirectional(t *testing.T) {
	c := twoSiteCluster()
	c.SetDirLink("alpha", "beta", true)
	c.SetDirLink("beta", "alpha", true)
	c.SetDirLink("beta", "alpha", false)
	if !c.linkDown("alpha", "beta") {
		t.Fatal("healing beta→alpha must leave alpha→beta down")
	}
	if c.linkDown("beta", "alpha") {
		t.Fatal("beta→alpha should be up after its own heal")
	}
}

func TestPartOneWayWeightSitsAfterPair(t *testing.T) {
	// crash 16 + op-site 13 = 29, then pair 5 (29–33), one-way 4 (34–37),
	// restart 8 starting at 38. This layout is what keeps later kinds on
	// the same pickFaultKind rolls after the 9→5+4 split.
	if got := pickFaultKind(28); got != OpPartOpSite {
		t.Fatalf("roll 28: got %s, want %s", got, OpPartOpSite)
	}
	if got := pickFaultKind(29); got != OpPartPair {
		t.Fatalf("roll 29: got %s, want %s", got, OpPartPair)
	}
	if got := pickFaultKind(33); got != OpPartPair {
		t.Fatalf("roll 33: got %s, want %s", got, OpPartPair)
	}
	if got := pickFaultKind(34); got != OpPartOneWay {
		t.Fatalf("roll 34: got %s, want %s", got, OpPartOneWay)
	}
	if got := pickFaultKind(37); got != OpPartOneWay {
		t.Fatalf("roll 37: got %s, want %s", got, OpPartOneWay)
	}
	if got := pickFaultKind(38); got != OpRestartOperator {
		t.Fatalf("roll 38: got %s, want %s", got, OpRestartOperator)
	}
}

func TestOneWayOpIsGenerated(t *testing.T) {
	const seeds = 600
	n, fromFirst, fromSecond := 0, 0, 0
	for seed := uint64(1); seed <= seeds; seed++ {
		for _, op := range GenerateTrial(seed).Ops {
			if op.Kind != OpPartOneWay {
				continue
			}
			n++
			switch op.Site {
			case "alpha":
				fromFirst++
			default:
				fromSecond++
			}
		}
	}
	if n < 10 {
		t.Fatalf("OpPartOneWay appeared in only %d ops across %d seeds", n, seeds)
	}
	if fromFirst == 0 || fromSecond == 0 {
		t.Fatalf("OpPartOneWay only generated one orientation: from-alpha=%d other=%d", fromFirst, fromSecond)
	}
}

func TestSkipMasksOneWayIndependentlyOfPair(t *testing.T) {
	base := Trial{
		SiteNames:         []string{"alpha", "beta"},
		Roles:             []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate},
		CooldownSec:       10,
		WarmupPolls:       8,
		HealAt:            45,
		Polls:             120,
		SidecarLeasePolls: 60,
		SidecarTopology:   true,
		Ops: []Op{
			{At: 10, Kind: OpPartOneWay, Site: "beta", Peer: "alpha"},
			{At: 12, Kind: OpPartPair, Site: "alpha", Peer: "beta"},
		},
	}

	oneWayOnly := base
	oneWayOnly.Skip = []bool{false, true}
	r := RunTrial(oneWayOnly, true, nil)
	if n := len(eventsOfKind(r.Events, EvPartOneWay)); n != 1 {
		t.Fatalf("unmasked one-way should emit EvPartOneWay once, got %d\n%s", n, formatEvents(r.Events))
	}
	if n := len(eventsOfKind(r.Events, EvPartPair)); n != 0 {
		t.Fatalf("masked pair should emit no EvPartPair, got %d", n)
	}

	pairOnly := base
	pairOnly.Skip = []bool{true, false}
	r = RunTrial(pairOnly, true, nil)
	if n := len(eventsOfKind(r.Events, EvPartPair)); n != 1 {
		t.Fatalf("unmasked pair should emit EvPartPair once, got %d\n%s", n, formatEvents(r.Events))
	}
	if n := len(eventsOfKind(r.Events, EvPartOneWay)); n != 0 {
		t.Fatalf("masked one-way should emit no EvPartOneWay, got %d", n)
	}
}

func twoSiteHandTrial(lease int, topology bool, healAt int, ops ...Op) Trial {
	return Trial{
		SiteNames:         []string{"alpha", "beta"},
		Roles:             []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate},
		CooldownSec:       10,
		WarmupPolls:       8,
		HealAt:            healAt,
		Polls:             healAt + 10 + 55,
		SidecarLeasePolls: lease,
		SidecarTopology:   topology,
		Ops:               ops,
	}
}

// TestShapeD_NoFailoverNoSelfFence is the documented shape D: operator
// reaches both sites, beta cannot reach alpha. No promotion, primary does
// not self-fence. Replica IO during the cut is asserted in the model tests
// above — RunTrial finals are post-heal.
func TestShapeD_NoFailoverNoSelfFence(t *testing.T) {
	trial := twoSiteHandTrial(60, true, 45,
		Op{At: 10, Kind: OpPartOneWay, Site: "beta", Peer: "alpha"},
	)
	r := RunTrial(trial, true, nil)
	if len(r.PromotionPolls) != 0 {
		t.Errorf("shape D promoted: %v", r.PromotionPolls)
	}
	if r.FinalStatus.ActiveSite != "alpha" {
		t.Errorf("active site %q, want alpha", r.FinalStatus.ActiveSite)
	}
	if polls := fencePolls(r.Events, "alpha"); len(polls) != 0 {
		t.Errorf("alpha self-fenced at polls %v; operator and peer were reachable", polls)
	}
	if n := len(eventsOfKind(r.Events, EvPartOneWay)); n == 0 {
		t.Fatal("one-way cut never applied")
	}
	if len(r.Violations) > 0 {
		t.Errorf("violations: %v", r.Violations)
	}
	t.Logf("shape D (operator reaches both, beta↛alpha): promotions=%v active=%s alphaFences=%v violations=%d",
		r.PromotionPolls, r.FinalStatus.ActiveSite, fencePolls(r.Events, "alpha"), len(r.Violations))
}

// TestShapeD_LeaseRule_ReachablePeerSuppressesFence isolates rule #2:
// operator unreachable from both sites, but the writable primary can still
// ping its peer. One reachable peer must keep the lease quiet.
func TestShapeD_LeaseRule_ReachablePeerSuppressesFence(t *testing.T) {
	const (
		cutAt  = 10
		lease  = 8
		healAt = 40
	)
	trial := twoSiteHandTrial(lease, false, healAt,
		Op{At: cutAt, Kind: OpPartOpSite, Site: "alpha"},
		Op{At: cutAt, Kind: OpPartOpSite, Site: "beta"},
		// beta↛alpha: replica cannot reach primary. Primary can still ping replica.
		Op{At: cutAt, Kind: OpPartOneWay, Site: "beta", Peer: "alpha"},
	)
	r := RunTrial(trial, true, nil)
	var during []int
	for _, p := range fencePolls(r.Events, "alpha") {
		if p > cutAt && p < healAt {
			t.Fatalf("alpha self-fenced at poll %d while it could still reach beta (lease rule must stay quiet)", p)
		}
		during = append(during, p)
	}
	// A promotion at HealAt is the operator returning from total isolation
	// (same without the one-way cut). It is not a shape-D finding.
	t.Logf("shape D lease quiet (both op-site down, beta↛alpha): alphaFencesDuringCut=none allAlphaFences=%v promotions=%v", during, r.PromotionPolls)
}

// TestShapeD_LeaseRule_UnreachablePeerFences is the positive control: the
// same operator isolation, but the primary cannot ping the replica, so
// rule #2 must fire after the lease and before heal.
func TestShapeD_LeaseRule_UnreachablePeerFences(t *testing.T) {
	const (
		cutAt  = 10
		lease  = 8
		healAt = 40
	)
	trial := twoSiteHandTrial(lease, false, healAt,
		Op{At: cutAt, Kind: OpPartOpSite, Site: "alpha"},
		Op{At: cutAt, Kind: OpPartOpSite, Site: "beta"},
		// alpha↛beta: primary cannot ping replica. Replica can still fetch.
		Op{At: cutAt, Kind: OpPartOneWay, Site: "alpha", Peer: "beta"},
	)
	r := RunTrial(trial, true, nil)
	var hit []int
	for _, p := range fencePolls(r.Events, "alpha") {
		if p > cutAt && p < healAt {
			hit = append(hit, p)
		}
	}
	if len(hit) == 0 {
		t.Fatalf("alpha did not self-fence after lease while operator and peer were unreachable\nevents:\n%s", formatEvents(r.Events))
	}
	if hit[0] < cutAt+lease {
		t.Errorf("alpha fenced at poll %d; lease is %d polls starting at %d", hit[0], lease, cutAt)
	}
	t.Logf("shape D lease fires (both op-site down, alpha↛beta): alphaFencedAt=%v promotions=%v", hit, r.PromotionPolls)
}
