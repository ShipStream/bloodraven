package dst

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"

	"github.com/shipstream/bloodraven/internal/state"
)

// actorStreamTag keys the actor schedule's PRNG stream (§15.1: a separate
// stream, rand.NewPCG(seed, 0x9f1a2b3c)), so enabling the actors leaves
// every existing seed's fault schedule byte-identical (D18).
const actorStreamTag = 0x9f1a2b3c

// SlowStatement marks one (site, statement, actor) slow: it yields between
// issue and apply and between apply and acknowledgement, so the actor can be
// blocked inside it while others act. Only the planned and update actors'
// statements can be slow; the topology manager issues probes from per-site
// goroutines, and a yield there would break the one-goroutine scheduler
// (there are no TM-internal yields, F2).
type SlowStatement struct {
	Site  string
	Kind  EventKind
	Actor Actor
}

func (s SlowStatement) String() string {
	return fmt.Sprintf("%s %s by=%s", s.Site, s.Kind, s.Actor)
}

// Actors is a trial's planned-failover actor schedule.
type Actors struct {
	Enabled bool
	Ops     []Op
	// Skip masks Ops for shrinking; nil means all ops are active.
	Skip []bool
	Slow []SlowStatement
	// RogueAtYield makes rogue writability (a human's read_only=0 on a
	// random site) an eligible scheduler step at yields.
	RogueAtYield bool
	// SchedSeed seeds the yield scheduler's own stream.
	SchedSeed uint64
}

// ActiveOps returns the actor ops not masked out by Skip.
func (a Actors) ActiveOps() []Op {
	if a.Skip == nil {
		return a.Ops
	}
	out := make([]Op, 0, len(a.Ops))
	for i, op := range a.Ops {
		if !a.Skip[i] {
			out = append(out, op)
		}
	}
	return out
}

func (a Actors) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "  actors: rogueAtYield=%v schedSeed=%d\n", a.RogueAtYield, a.SchedSeed)
	for _, s := range a.Slow {
		fmt.Fprintf(&b, "    slow %s\n", s)
	}
	for i, op := range a.Ops {
		masked := ""
		if a.Skip != nil && a.Skip[i] {
			masked = " [masked]"
		}
		fmt.Fprintf(&b, "    %s%s\n", op, masked)
	}
	return b.String()
}

// actorFaultWeights is the actor stream's fault-kind distribution; like
// faultWeights it must sum to 100.
var actorFaultWeights = []faultWeight{
	{OpPFWriteLost, 14},
	{OpPFWriteAmbiguous, 14},
	{OpPFWriteLate, 12},
	{OpPFReadOutage, 10},
	{OpPFStatusDenied, 12},
	{OpOOBLateWrite, 8},
	{OpRestartPromotedTarget, 10},
	{OpReplacePod, 12},
	{OpManagerAbsent, 8},
}

func init() {
	sum := 0
	for _, w := range actorFaultWeights {
		sum += w.weight
	}
	if sum != 100 {
		panic(fmt.Sprintf("dst: actorFaultWeights sum to %d, want 100", sum))
	}
}

func pickActorFaultKind(roll int) OpKind {
	acc := 0
	for _, w := range actorFaultWeights {
		acc += w.weight
		if roll < acc {
			return w.kind
		}
	}
	return actorFaultWeights[len(actorFaultWeights)-1].kind
}

// slowKinds are the statements a trial may mark slow.
var slowKinds = []EventKind{EvSetSuperRO, EvStopReplica, EvResetReplica, EvSetRO, EvStartReplica, EvChangeSource}

// unknownPlannedTarget is a request for a site the group does not have.
const unknownPlannedTarget = "nowhere"

// GenerateActors derives t's actor schedule from t.Seed on the actor
// stream. It reads only t's site layout and windows, never the fault
// stream, so the fault schedule is unchanged whatever it draws.
func GenerateActors(t Trial) Actors {
	rng := rand.New(rand.NewPCG(t.Seed, actorStreamTag))
	names := t.SiteNames
	window := max(t.HealAt-t.WarmupPolls, 1)
	at := func() int { return t.WarmupPolls + rng.IntN(window) }
	pickSite := func() string { return names[rng.IntN(len(names))] }

	// Promotable targets other than the baseline primary.
	var candidates []string
	for i, n := range names {
		if i > 0 && t.Roles[i] == state.SiteRolePrimaryCandidate {
			candidates = append(candidates, n)
		}
	}

	var ops []Op
	for range 1 + rng.IntN(3) {
		roll := rng.IntN(100)
		target := unknownPlannedTarget
		switch {
		case roll < 70 && len(candidates) > 0:
			target = candidates[rng.IntN(len(candidates))]
		case roll < 85:
			target = names[0]
		}
		ops = append(ops, Op{At: at(), Kind: OpPlannedFailover, Site: target})
	}
	for range rng.IntN(3) {
		ops = append(ops, Op{At: at(), Kind: OpOrderedUpdate})
	}
	for range rng.IntN(3) {
		ops = append(ops, Op{At: at(), Kind: OpReplaceManager, N: rng.IntN(4)})
	}
	for range rng.IntN(5) {
		kind := pickActorFaultKind(rng.IntN(100))
		start := at()
		switch kind {
		case OpPFWriteLost, OpPFWriteAmbiguous:
			ops = append(ops, Op{At: start, Kind: kind, N: 1 + rng.IntN(3)})
		case OpPFWriteLate:
			ops = append(ops, Op{At: start, Kind: kind, N: 1 + rng.IntN(2), K: 1 + rng.IntN(4)})
		case OpPFReadOutage:
			ops = append(ops, Op{At: start, Kind: kind, N: 1 + rng.IntN(4)})
		case OpPFStatusDenied:
			// Repeated restarts inside the outage: the durable stall-since
			// timestamp must survive them (§6.4, D36).
			n := 5 + rng.IntN(40)
			ops = append(ops, Op{At: start, Kind: kind, N: n})
			for range rng.IntN(3) {
				ops = append(ops, Op{At: start + 1 + rng.IntN(n), Kind: OpRestartOperator})
			}
		case OpOOBLateWrite:
			ops = append(ops, Op{At: start, Kind: kind})
		case OpRestartPromotedTarget:
			ops = append(ops, Op{At: start, Kind: kind, K: rng.IntN(6)})
		case OpReplacePod:
			back := 0
			if rng.IntN(2) == 1 {
				back = 1 + rng.IntN(5)
			}
			ops = append(ops, Op{At: start, Kind: kind, Site: pickSite(), K: back})
		case OpManagerAbsent:
			ops = append(ops, Op{At: start, Kind: kind, N: 1 + rng.IntN(30)})
		default:
			panic(fmt.Sprintf("dst: actorFaultWeights kind %q has no case in GenerateActors", kind))
		}
	}
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].At < ops[j].At })

	var slow []SlowStatement
	for range rng.IntN(3) {
		actor := ActorPF
		if rng.IntN(100) >= 80 {
			actor = ActorUpdate
		}
		slow = append(slow, SlowStatement{Site: pickSite(), Kind: slowKinds[rng.IntN(len(slowKinds))], Actor: actor})
	}

	return Actors{
		Enabled:      true,
		Ops:          ops,
		Slow:         slow,
		RogueAtYield: rng.IntN(100) < 20,
		SchedSeed:    rng.Uint64(),
	}
}

// GeneratePlannedTrial is GenerateTrial(seed) with the actors enabled: the
// same fault schedule, plus the actor schedule.
func GeneratePlannedTrial(seed uint64) Trial {
	t := GenerateTrial(seed)
	t.Actors = GenerateActors(t)
	// A request made just before HealAt may wait out its admission
	// deadline: the settle window must let every run end.
	t.Polls = t.HealAt + max(t.Polls-t.HealAt, plannedSettlePolls)
	return t
}

// isSlow reports whether the trial marks (site, kind, actor) slow.
func (a Actors) isSlow(site string, kind EventKind, actor Actor) bool {
	for _, s := range a.Slow {
		if s.Site == site && s.Kind == kind && s.Actor == actor {
			return true
		}
	}
	return false
}
