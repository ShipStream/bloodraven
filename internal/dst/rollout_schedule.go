package dst

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
)

// Paced-rollout trials (proposals/23-paced-rollouts.md, README "Paced
// rollouts"). A rollout trial is a fleet of failover groups under one
// operator process, an operator release that makes every group's workloads
// release-pending, and a schedule of operator restarts, pods that do not
// become Ready and administrator annotations. It is a trial family of its
// own (Trial.Rollout), run by RunTrial like any other: the fault schedule
// of the failover trials is one group's, and what is judged here is the
// fleet.

// Rollout-stream ops. They are generated only into Trial.Rollout.Ops.
const (
	// OpRelease is an operator release: every workload the running
	// renderer wrote, and every rollout record written for it, becomes
	// another renderer's, and the operator restarts (N polls later). The
	// first release of a trial starts the rollout; a later one lands in
	// the middle of it (the second upgrade of R-5).
	OpRelease OpKind = "release"
	// OpCrashOperatorWrite kills the operator process at an API write: N is
	// the countdown in the operator's writes (status.rollout, a Deployment,
	// a ConfigMap, the group's status), PreApply chooses whether the fatal
	// write is committed, Down is how many steps pass before the
	// replacement starts.
	OpCrashOperatorWrite OpKind = "crashOperatorAtWrite"
	// OpPodNotReady arms the pod of Group/Site: after its next roll it is
	// not Ready for N steps, or with N == 0 until an OpPodReady or the
	// heal.
	OpPodNotReady OpKind = "podNotReadyAfterRoll"
	// OpPodReady makes the pod of Group/Site Ready (the fix of a bad
	// build, as far as this pod goes).
	OpPodReady OpKind = "podReady"
	// OpBadBuild arms every pod of the fleet: none becomes Ready after its
	// next roll until an OpPodReady or the heal. The release cannot run in
	// these pods.
	OpBadBuild OpKind = "badBuild"
	// OpSiteDown stops the MySQL of a read-only site of Group (a site that
	// is writable when the op lands is left alone), so the group is not
	// Ready; OpSiteUp starts it again.
	OpSiteDown OpKind = "siteDown"
	OpSiteUp   OpKind = "siteUp"
	// OpRolloutAdmit sets the rollout-admit annotation on Group for N
	// steps; OpRolloutReleaseSlot the rollout-release-slot annotation.
	OpRolloutAdmit       OpKind = "rolloutAdmit"
	OpRolloutReleaseSlot OpKind = "rolloutReleaseSlot"
)

// RolloutGroup is one failover group of a rollout trial.
type RolloutGroup struct {
	Name  string
	Sites []string
	// Recreate is spec.updateStrategy.type=Recreate: the group reconciler
	// writes every site's Deployment in one pass, and no ordered update
	// runs.
	Recreate bool
}

// RolloutPlan is the fleet and the schedule of a rollout trial. The zero
// value (GenerateTrial's) is no rollout trial.
type RolloutPlan struct {
	Enabled bool
	Groups  []RolloutGroup
	// The pacer's durations, in steps (one step is rolloutStep of fake
	// time). SoakSteps 0 is no soak.
	SoakSteps, StartGraceSteps, StallAfterSteps int
	// PacerAfterGroups runs the pacer's pass after the groups' passes in
	// each step instead of before them.
	PacerAfterGroups bool

	Ops []Op
	// Skip masks Ops for shrinking; nil means all ops are active.
	Skip []bool
}

// ActiveOps returns the ops not masked out by Skip.
func (p RolloutPlan) ActiveOps() []Op {
	if p.Skip == nil {
		return p.Ops
	}
	out := make([]Op, 0, len(p.Ops))
	for i, op := range p.Ops {
		if !p.Skip[i] {
			out = append(out, op)
		}
	}
	return out
}

func (p RolloutPlan) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "  rollout: soak=%d startGrace=%d stallAfter=%d (steps) pacerAfterGroups=%v\n",
		p.SoakSteps, p.StartGraceSteps, p.StallAfterSteps, p.PacerAfterGroups)
	for _, g := range p.Groups {
		strategy := "OrderedUpdate"
		if g.Recreate {
			strategy = "Recreate"
		}
		fmt.Fprintf(&b, "  group %s sites=%v %s\n", g.Name, g.Sites, strategy)
	}
	for i, op := range p.Ops {
		masked := ""
		if p.Skip != nil && p.Skip[i] {
			masked = " [masked]"
		}
		fmt.Fprintf(&b, "  r%02d %s%s\n", i, op, masked)
	}
	return b.String()
}

// rolloutStream salts the PRNG stream of the rollout trials.
const rolloutStream = 0x7061636564726f6c

var (
	rolloutGroupNames = []string{"ant", "bee", "cat", "dog"}
	rolloutSiteNames  = []string{"east", "west", "north"}
)

// rolloutWarmup is the steps before the first release: the groups'
// managers observe their sites and write a Ready status.
const rolloutWarmup = 4

// GenerateRolloutTrial derives a rollout trial from a seed. Everything is
// in Trial.Rollout; the failover fields of the Trial are unused.
func GenerateRolloutTrial(seed uint64) Trial {
	rng := rand.New(rand.NewPCG(seed, rolloutStream))

	plan := RolloutPlan{Enabled: true}
	// Two groups are a fleet; a third makes a queue behind the slot's
	// holder; the cost of a trial grows with each.
	numGroups := []int{2, 2, 2, 3, 3, 3, 3, 4}[rng.IntN(8)]
	for i := 0; i < numGroups; i++ {
		g := RolloutGroup{Name: rolloutGroupNames[i], Sites: rolloutSiteNames[:2]}
		if rng.IntN(100) < 20 {
			g.Sites = rolloutSiteNames[:3]
		}
		g.Recreate = rng.IntN(100) < 25
		plan.Groups = append(plan.Groups, g)
	}
	plan.SoakSteps = []int{0, 0, 2, 5}[rng.IntN(4)]
	plan.StartGraceSteps = []int{4, 8, 14}[rng.IntN(3)]
	plan.StallAfterSteps = []int{6, 12, 24}[rng.IntN(3)]
	plan.PacerAfterGroups = rng.IntN(2) == 0

	window := 18 + rng.IntN(22)
	healAt := rolloutWarmup + window
	at := func() int { return rolloutWarmup + rng.IntN(window) }
	pickGroup := func() RolloutGroup { return plan.Groups[rng.IntN(numGroups)] }
	pickSite := func(g RolloutGroup) string { return g.Sites[rng.IntN(len(g.Sites))] }

	ops := []Op{{At: rolloutWarmup, Kind: OpRelease, N: rng.IntN(2)}}

	// The build. One trial in four ships a release whose pods never become
	// Ready; otherwise single pods are slow or broken.
	if rng.IntN(100) < 25 {
		ops = append(ops, Op{At: rolloutWarmup, Kind: OpBadBuild})
		if rng.IntN(100) < 50 {
			// The fix reaches the first group's pods before the heal.
			g := plan.Groups[0]
			h := at()
			for _, site := range g.Sites {
				ops = append(ops, Op{At: h, Kind: OpPodReady, Group: g.Name, Site: site})
			}
		}
	} else {
		for n := rng.IntN(4); n > 0; n-- {
			g := pickGroup()
			if rng.IntN(100) < 50 {
				g = plan.Groups[0] // the first group in the queue is the one a bad pod halts the fleet on
			}
			op := Op{At: rolloutWarmup + rng.IntN(window) - 1, Kind: OpPodNotReady, Group: g.Name, Site: pickSite(g)}
			if rng.IntN(100) < 50 {
				op.N = 1 + rng.IntN(12)
			} else if rng.IntN(100) < 60 {
				if h := op.At + 2 + rng.IntN(window); h < healAt {
					ops = append(ops, Op{At: h, Kind: OpPodReady, Group: g.Name, Site: op.Site})
				}
			}
			ops = append(ops, op)
		}
	}

	// Operator restarts: between passes, and in the middle of one.
	for n := rng.IntN(4); n > 0; n-- {
		ops = append(ops, Op{At: at(), Kind: OpRestartOperator})
	}
	for n := rng.IntN(4); n > 0; n-- {
		ops = append(ops, Op{At: at(), Kind: OpCrashOperatorWrite, N: rng.IntN(8), PreApply: rng.IntN(2) == 0, Down: rng.IntN(3)})
	}

	// A second upgrade mid-rollout, sometimes a third.
	if rng.IntN(100) < 40 {
		ops = append(ops, Op{At: rolloutWarmup + 1 + rng.IntN(window-1), Kind: OpRelease, N: rng.IntN(2)})
		if rng.IntN(100) < 20 {
			ops = append(ops, Op{At: rolloutWarmup + 1 + rng.IntN(window-1), Kind: OpRelease, N: rng.IntN(2)})
		}
	}

	// A group that is not stable: a read-only site's MySQL is down.
	if rng.IntN(100) < 30 {
		g := pickGroup()
		op := Op{At: at() - 2, Kind: OpSiteDown, Group: g.Name, Site: g.Sites[1+rng.IntN(len(g.Sites)-1)]}
		ops = append(ops, op)
		if rng.IntN(100) < 60 {
			if h := op.At + 2 + rng.IntN(window); h < healAt {
				ops = append(ops, Op{At: h, Kind: OpSiteUp, Group: g.Name, Site: op.Site})
			}
		}
	}

	// The administrator's annotations.
	if rng.IntN(100) < 12 {
		ops = append(ops, Op{At: at(), Kind: OpRolloutAdmit, Group: pickGroup().Name, N: 2 + rng.IntN(10)})
	}
	if rng.IntN(100) < 12 {
		ops = append(ops, Op{At: at(), Kind: OpRolloutReleaseSlot, Group: pickGroup().Name, N: 2 + rng.IntN(10)})
	}

	sort.SliceStable(ops, func(i, j int) bool { return ops[i].At < ops[j].At })
	plan.Ops = ops

	// The settle window: every group may still have to be rolled after the
	// heal, one after another, each after the soak, a stall back-off and a
	// deferral of an admission that could not start.
	settle := 30 + numGroups*(12+plan.SoakSteps) + plan.StartGraceSteps
	return Trial{
		Seed:        seed,
		WarmupPolls: rolloutWarmup,
		HealAt:      healAt,
		Polls:       healAt + settle,
		Rollout:     plan,
	}
}
