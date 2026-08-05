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
	OpDNSOutage       OpKind = "dnsOutage"
	OpDNSHeal         OpKind = "dnsHeal"
)

// Op is one scheduled fault operation, applied at the start of poll At.
type Op struct {
	At     int
	Kind   OpKind
	Site   string
	Peer   string // partner for pair partitions
	N      int    // count for ambiguous/failing mutations
	Fenced bool   // crash boot mode: true = sidecar re-fences on boot
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
	Ops         []Op
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
		switch rng.IntN(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17: // 18%: crash
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
		case 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31: // 14%: operator<->site partition
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpPartOpSite, Site: site})
			pairedHeal(start, OpHealOpSite, site, "")
		case 32, 33, 34, 35, 36, 37, 38, 39, 40, 41: // 10%: site<->site partition
			a := pickSite()
			b := pickSite()
			for b == a {
				b = pickSite()
			}
			start := at()
			ops = append(ops, Op{At: start, Kind: OpPartPair, Site: a, Peer: b})
			pairedHeal(start, OpHealPair, a, b)
		case 42, 43, 44, 45, 46, 47, 48, 49, 50, 51: // 10%: operator restart
			ops = append(ops, Op{At: at(), Kind: OpRestartOperator})
		case 52, 53, 54, 55, 56, 57, 58, 59, 60, 61: // 10%: ambiguous mutations
			ops = append(ops, Op{At: at(), Kind: OpAmbiguousMuts, Site: pickSite(), N: 1 + rng.IntN(4)})
		case 62, 63, 64, 65, 66, 67, 68, 69: // 8%: failing mutations
			ops = append(ops, Op{At: at(), Kind: OpFailMuts, Site: pickSite(), N: 1 + rng.IntN(4)})
		case 70, 71, 72, 73, 74, 75: // 6%: replication apply stall (relay backlog builds)
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpStallApply, Site: site})
			pairedHeal(start, OpHealApply, site, "")
		case 76, 77, 78, 79: // 4%: replication fetch stall
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpStallFetch, Site: site})
			pairedHeal(start, OpHealFetch, site, "")
		case 80, 81, 82: // 3%: relay drain stall
			site := pickSite()
			start := at()
			ops = append(ops, Op{At: start, Kind: OpDrainStall, Site: site})
			pairedHeal(start, OpHealDrain, site, "")
		case 83, 84, 85, 86, 87, 88: // 6%: rogue fence (stale sidecar lease)
			ops = append(ops, Op{At: at(), Kind: OpRogueFence, Site: pickSite()})
		case 89, 90, 91: // 3%: rogue writable (human error on a replica)
			ops = append(ops, Op{At: at(), Kind: OpRogueWritable, Site: pickSite()})
		case 92, 93, 94, 95: // 4%: CR status write outage
			start := at()
			ops = append(ops, Op{At: start, Kind: OpStatusOutage})
			pairedHeal(start, OpStatusHeal, "", "")
		default: // 4%: DNS outage
			start := at()
			ops = append(ops, Op{At: start, Kind: OpDNSOutage})
			pairedHeal(start, OpDNSHeal, "", "")
		}
	}

	sort.SliceStable(ops, func(i, j int) bool { return ops[i].At < ops[j].At })

	return Trial{
		Seed:        seed,
		SiteNames:   append([]string{}, names...),
		Roles:       roles,
		Priorities:  priorities,
		CooldownSec: cooldown,
		SeedHistory: seedHistory,
		WarmupPolls: warmup,
		HealAt:      healAt,
		Polls:       polls,
		Ops:         ops,
	}
}
