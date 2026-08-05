package dst

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"

	"github.com/shipstream/bloodraven/internal/state"
)

// OpKind identifies one fault-schedule operation.
type OpKind string

const (
	OpCrash           OpKind = "crash"
	OpRecover         OpKind = "recover"
	OpPartOpSite      OpKind = "partitionOperatorSite"
	OpHealOpSite      OpKind = "healOperatorSite"
	OpPartPair        OpKind = "partitionPair"
	OpHealPair        OpKind = "healPair"
	OpRestartOperator OpKind = "restartOperator"
	OpAmbiguousMuts   OpKind = "ambiguousMutations"
	OpFailMuts        OpKind = "failMutations"
	OpStallApply      OpKind = "stallApply"
	OpHealApply       OpKind = "healApply"
	OpStallFetch      OpKind = "stallFetch"
	OpHealFetch       OpKind = "healFetch"
	OpDrainStall      OpKind = "drainStall"
	OpHealDrain       OpKind = "healDrain"
	OpRogueFence      OpKind = "rogueFence"
	OpRogueWritable   OpKind = "rogueWritable"
	OpStatusOutage    OpKind = "statusWriteOutage"
	OpStatusHeal      OpKind = "statusWriteHeal"
	OpStateOutage     OpKind = "failoverStateOutage"
	OpStateHeal       OpKind = "failoverStateHeal"
	OpDNSOutage       OpKind = "dnsOutage"
	OpDNSHeal         OpKind = "dnsHeal"
	// OpCrashOperatorMid kills the operator process partway through a
	// sequence of MySQL mutations rather than between polls. N is the
	// countdown in operator mutations; PreApply chooses whether the fatal
	// statement reaches the server; Down is how many polls pass before the
	// replacement process starts.
	OpCrashOperatorMid OpKind = "crashOperatorMid"
)

// Op is one scheduled fault operation, applied at the start of poll At.
type Op struct {
	At     int
	Kind   OpKind
	Site   string
	Peer   string // partner for pair partitions
	N      int    // count for ambiguous/failing mutations, or crash countdown
	Fenced bool   // crash boot mode: true = sidecar re-fences on boot

	// PreApply (OpCrashOperatorMid) kills the operator BEFORE the fatal
	// statement reaches the server rather than after it applied.
	PreApply bool
	// Down (OpCrashOperatorMid) is how many polls the operator stays dead
	// before its replacement starts.
	Down int
}

func (o Op) String() string {
	s := fmt.Sprintf("@p%03d %s", o.At, o.Kind)
	if o.Site != "" {
		s += " site=" + o.Site
	}
	if o.Peer != "" {
		s += " peer=" + o.Peer
	}
	if o.N != 0 {
		s += fmt.Sprintf(" n=%d", o.N)
	}
	if o.Kind == OpCrash {
		s += fmt.Sprintf(" bootFenced=%v", o.Fenced)
	}
	if o.Kind == OpCrashOperatorMid {
		s += fmt.Sprintf(" preApply=%v down=%d", o.PreApply, o.Down)
	}
	return s
}

// Trial is a fully-specified simulation run. Everything is derived from Seed
// by GenerateTrial; execution consumes no further randomness.
type Trial struct {
	Seed        uint64
	SiteNames   []string
	Roles       []state.SiteRole
	Priorities  []string
	CooldownSec int
	SeedHistory bool // rehydrate a prior failover (target = first site) at start
	WarmupPolls int
	HealAt      int // poll at which every outstanding fault is healed
	Polls       int

	// Sidecar actor configuration. Every site runs a real
	// sidecar.FencingMonitor (see sidecar.go); these are the deployment
	// knobs a real chart exposes.
	//
	//   SidecarLeasePolls — --fencing-lease-timeout, in polls (1 poll = 1s).
	//   SidecarTopology   — topology-aware fencing (rule #1) wired or not;
	//                       the monitor documents lease-only as a supported
	//                       degraded mode, so both are worth covering.
	//   SidecarAfterPoll  — whether the monitor's tick lands after the
	//                       operator's poll or before it. The sidecar ticks
	//                       on its own schedule in production, so both
	//                       interleavings are real; fixing one per trial
	//                       keeps replay exact.
	SidecarLeasePolls int
	SidecarTopology   bool
	SidecarAfterPoll  bool

	Ops []Op
	// Skip masks Ops for shrinking; nil means all ops are active.
	Skip []bool
}

// ActiveOps returns the ops not masked out by Skip.
func (t Trial) ActiveOps() []Op {
	if t.Skip == nil {
		return t.Ops
	}
	out := make([]Op, 0, len(t.Ops))
	for i, op := range t.Ops {
		if !t.Skip[i] {
			out = append(out, op)
		}
	}
	return out
}

func (t Trial) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d sites=%d roles=%v priorities=%v cooldown=%ds history=%v warmup=%d healAt=%d polls=%d\n",
		t.Seed, len(t.SiteNames), t.Roles, t.Priorities, t.CooldownSec, t.SeedHistory, t.WarmupPolls, t.HealAt, t.Polls)
	fmt.Fprintf(&b, "  sidecar: leasePolls=%d topologyAware=%v tickAfterPoll=%v\n",
		t.SidecarLeasePolls, t.SidecarTopology, t.SidecarAfterPoll)
	for i, op := range t.Ops {
		masked := ""
		if t.Skip != nil && t.Skip[i] {
			masked = " [masked]"
		}
		fmt.Fprintf(&b, "  %s%s\n", op, masked)
	}
	return b.String()
}

var trialSiteNames = []string{"alpha", "beta", "gamma"}

// faultWeight is one entry in the fault-kind distribution. Weights are
// percentages of a single rng.IntN(100) draw and must sum to exactly 100 —
// faultWeightSum enforces that at init so a mis-edited table fails loudly
// instead of silently starving the last kind.
//
// A table rather than a case list: adding a fault kind then costs one line
// and a rebalance you can audit at a glance, and every kind still consumes
// exactly one draw, so replay stays exact.
type faultWeight struct {
	kind   OpKind
	weight int
}

var faultWeights = []faultWeight{
	{OpCrash, 16},
	{OpPartOpSite, 13},
	{OpPartPair, 9},
	{OpRestartOperator, 8},
	{OpCrashOperatorMid, 6},
	{OpAmbiguousMuts, 9},
	{OpFailMuts, 7},
	{OpStallApply, 5},
	{OpStallFetch, 4},
	{OpDrainStall, 3},
	{OpRogueFence, 6},
	{OpRogueWritable, 3},
	{OpStatusOutage, 4},
	{OpStateOutage, 4},
	{OpDNSOutage, 3},
}

func init() {
	sum := 0
	for _, w := range faultWeights {
		sum += w.weight
	}
	if sum != 100 {
		panic(fmt.Sprintf("dst: faultWeights sum to %d, want 100", sum))
	}
}

// pickFaultKind maps one uniform draw in [0,100) onto the weight table.
func pickFaultKind(roll int) OpKind {
	acc := 0
	for _, w := range faultWeights {
		acc += w.weight
		if roll < acc {
			return w.kind
		}
	}
	return faultWeights[len(faultWeights)-1].kind
}

// GenerateTrial derives a complete trial from a seed.
func GenerateTrial(seed uint64) Trial {
	rng := rand.New(rand.NewPCG(seed, seed*0x9E3779B97F4A7C15+0xBF58476D1CE4E5B9))

	numSites := 2
	if rng.IntN(100) < 30 {
		numSites = 3
	}
	names := trialSiteNames[:numSites]
	roles := make([]state.SiteRole, numSites)
	for i := range roles {
		roles[i] = state.SiteRolePrimaryCandidate
	}
	if numSites == 3 {
		switch rng.IntN(10) {
		case 0, 1, 2, 3:
			roles[2] = state.SiteRoleDROnly
		case 4, 5:
			roles[2] = state.SiteRoleReadOnly
		}
	}

	var priorities []string
	switch rng.IntN(3) {
	case 1:
		priorities = []string{names[0]}
	case 2:
		priorities = append([]string{}, names[:min(2, numSites)]...)
	}

	cooldown := []int{10, 30, 60}[rng.IntN(3)]
	seedHistory := rng.IntN(2) == 0

	// Sidecar deployment shape. The lease values bracket the operator's own
	// debounce (FailureThreshold 3 + RecoveryThreshold 2): a lease shorter
	// than the operator's reaction time means the sidecar fences first, a
	// longer one means the operator usually gets there first, and both
	// orderings happen in production depending on how the chart is tuned.
	sidecarLease := []int{8, 15, 30}[rng.IntN(3)]
	sidecarTopology := rng.IntN(100) < 80
	sidecarAfterPoll := rng.IntN(2) == 0

	const warmup = 8
	faultWindow := 25 + rng.IntN(30)
	healAt := warmup + faultWindow
	// Settle must outlast the anti-flap cooldown, the 30s recovery
	// stabilization delay, and debounce thresholds, so a healed cluster that
	// still fails to converge is a genuine wedge rather than a pending timer.
	settle := cooldown + 55
	polls := healAt + settle

	numOps := 1 + rng.IntN(6)
	var ops []Op
	pickSite := func() string { return names[rng.IntN(numSites)] }
	at := func() int { return warmup + rng.IntN(faultWindow) }
	pairedHeal := func(start int, healKind OpKind, site, peer string) {
		if rng.IntN(100) < 60 {
			h := start + 1 + rng.IntN(faultWindow)
			if h < healAt {
				ops = append(ops, Op{At: h, Kind: healKind, Site: site, Peer: peer})
			}
		}
	}

	for i := 0; i < numOps; i++ {
		switch pickFaultKind(rng.IntN(100)) {
		case OpCrash:
			site := pickSite()
			start := at()
			op := Op{At: start, Kind: OpCrash, Site: site, Fenced: rng.IntN(100) < 45}
			ops = append(ops, op)
			if rng.IntN(100) < 70 {
				h := start + 2 + rng.IntN(faultWindow)
				if h < healAt {
					ops = append(ops, Op{At: h, Kind: OpRecover, Site: site, Fenced: op.Fenced})
				}
			}
		case OpPartOpSite:
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpPartOpSite, Site: site})
			pairedHeal(start, OpHealOpSite, site, "")
		case OpPartPair:
			a := pickSite()
			b := pickSite()
			for b == a {
				b = pickSite()
			}
			start := at()
			ops = append(ops, Op{At: start, Kind: OpPartPair, Site: a, Peer: b})
			pairedHeal(start, OpHealPair, a, b)
		case OpRestartOperator:
			ops = append(ops, Op{At: at(), Kind: OpRestartOperator})
		case OpCrashOperatorMid:
			// The countdown is in operator mutations, not polls, so the
			// process dies at an arbitrary point inside a fence/promote
			// sequence rather than cleanly between polls. A zero countdown
			// kills on the very next mutation.
			ops = append(ops, Op{
				At:       at(),
				Kind:     OpCrashOperatorMid,
				N:        rng.IntN(6),
				PreApply: rng.IntN(2) == 0,
				Down:     rng.IntN(4),
			})
		case OpAmbiguousMuts:
			ops = append(ops, Op{At: at(), Kind: OpAmbiguousMuts, Site: pickSite(), N: 1 + rng.IntN(4)})
		case OpFailMuts:
			ops = append(ops, Op{At: at(), Kind: OpFailMuts, Site: pickSite(), N: 1 + rng.IntN(4)})
		case OpStallApply: // replication apply stall (relay backlog builds)
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpStallApply, Site: site})
			pairedHeal(start, OpHealApply, site, "")
		case OpStallFetch:
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpStallFetch, Site: site})
			pairedHeal(start, OpHealFetch, site, "")
		case OpDrainStall:
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpDrainStall, Site: site})
			pairedHeal(start, OpHealDrain, site, "")
		case OpRogueFence:
			// A human or an out-of-band actor setting super_read_only=ON.
			// The sidecar's own fencing is no longer represented here — it
			// runs as a real actor (sidecar.go) — but "someone with SUPER
			// fenced this site" is still its own fault mode.
			ops = append(ops, Op{At: at(), Kind: OpRogueFence, Site: pickSite()})
		case OpRogueWritable: // human error on a replica
			ops = append(ops, Op{At: at(), Kind: OpRogueWritable, Site: pickSite()})
		case OpStatusOutage: // CR /status subresource write outage
			start := at()
			ops = append(ops, Op{At: start, Kind: OpStatusOutage})
			pairedHeal(start, OpStatusHeal, "", "")
		case OpStateOutage: // out-of-band anti-flap store outage
			start := at()
			ops = append(ops, Op{At: start, Kind: OpStateOutage})
			pairedHeal(start, OpStateHeal, "", "")
		default: // DNS outage
			start := at()
			ops = append(ops, Op{At: start, Kind: OpDNSOutage})
			pairedHeal(start, OpDNSHeal, "", "")
		}
	}

	sort.SliceStable(ops, func(i, j int) bool { return ops[i].At < ops[j].At })

	return Trial{
		Seed:              seed,
		SiteNames:         append([]string{}, names...),
		Roles:             roles,
		Priorities:        priorities,
		CooldownSec:       cooldown,
		SeedHistory:       seedHistory,
		WarmupPolls:       warmup,
		HealAt:            healAt,
		Polls:             polls,
		SidecarLeasePolls: sidecarLease,
		SidecarTopology:   sidecarTopology,
		SidecarAfterPoll:  sidecarAfterPoll,
		Ops:               ops,
	}
}
