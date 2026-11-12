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
	OpPartOneWay      OpKind = "partitionOneWay"
	OpHealOneWay      OpKind = "healOneWay"
	OpRestartOperator OpKind = "restartOperator"
	OpAmbiguousMuts   OpKind = "ambiguousMutations"
	OpFailMuts        OpKind = "failMutations"
	// OpFailFlush and OpSlowFlush are the binary log rotation faults: the
	// next N FLUSH BINARY LOGS on Site fail without rotating, or block
	// until the operator abandons them at its bound (K=1: the server still
	// rotates afterwards). They are generated from a PRNG stream of their
	// own (flushFaults), so they add to a seed's schedule without changing
	// the ops it had before they existed.
	OpFailFlush OpKind = "failFlush"
	OpSlowFlush OpKind = "slowFlush"
	// OpRecloneWait requests a reclone of Site with PITR archiving on, so
	// the manager rotates the site's binary log and waits for its sidecar
	// to archive the tail, and withdraws the request N polls later (or at
	// heal-all), before the wait's bound: CLONE itself is not modelled. K
	// is what the site's sidecar answers meanwhile: 0 nothing (it is
	// unreachable), 1 still uploading, 2 blocked, 3 a scan from before the
	// rotation. The request is applied again to every manager that
	// replaces the one it was given to, as the annotation would be. Drawn
	// from the rotation faults' stream.
	OpRecloneWait   OpKind = "recloneWait"
	OpStallApply    OpKind = "stallApply"
	OpHealApply     OpKind = "healApply"
	OpStallFetch    OpKind = "stallFetch"
	OpHealFetch     OpKind = "healFetch"
	OpDrainStall    OpKind = "drainStall"
	OpHealDrain     OpKind = "healDrain"
	OpRogueFence    OpKind = "rogueFence"
	OpRogueWritable OpKind = "rogueWritable"
	OpStatusOutage  OpKind = "statusWriteOutage"
	OpStatusHeal    OpKind = "statusWriteHeal"
	OpStateOutage   OpKind = "failoverStateOutage"
	OpStateHeal     OpKind = "failoverStateHeal"
	OpDNSOutage     OpKind = "dnsOutage"
	OpDNSHeal       OpKind = "dnsHeal"
	// OpCrashOperatorMid kills the operator process partway through a
	// sequence of MySQL mutations rather than between polls. N is the
	// countdown in operator mutations; PreApply chooses whether the fatal
	// statement reaches the server; Down is how many polls pass before the
	// replacement process starts.
	OpCrashOperatorMid OpKind = "crashOperatorMid"

	// Actor-stream ops (§15.1). They are generated only into Trial.Actors,
	// from a PRNG stream of their own, so they never appear in (or shift)
	// the fault schedule above.
	//
	// OpPlannedFailover sets the planned-failover request annotation (Site
	// is the requested target, possibly the active site or an unknown one).
	OpPlannedFailover OpKind = "plannedFailover"
	// OpOrderedUpdate starts the update actor on the current manager.
	OpOrderedUpdate OpKind = "orderedUpdate"
	// OpReplaceManager retires the current manager (which keeps polling for
	// N more polls) and registers a new one in the same process.
	OpReplaceManager OpKind = "replaceManager"
	// OpManagerAbsent leaves the group with no registered manager for N
	// polls.
	OpManagerAbsent OpKind = "managerAbsent"
	// OpManagerAbsentLong is OpManagerAbsent past the manager-absent
	// deadline; it survives heal-all (Trial.settleStart).
	OpManagerAbsentLong OpKind = "managerAbsentLong"
	// OpPFWriteLost: the next N engine status writes time out uncommitted.
	OpPFWriteLost OpKind = "pfWriteLost"
	// OpPFWriteAmbiguous: the next N engine status writes commit and time
	// out.
	OpPFWriteAmbiguous OpKind = "pfWriteAmbiguous"
	// OpPFWriteLate: the next N writes time out and commit K polls later
	// iff their resourceVersion is still current.
	OpPFWriteLate OpKind = "pfWriteLate"
	// OpPFReadOutage: uncached reads fail for N polls.
	OpPFReadOutage OpKind = "pfReadOutage"
	// OpPFStatusDenied: status writes are forbidden for N polls while
	// metadata writes work.
	OpPFStatusDenied OpKind = "pfStatusDenied"
	// OpPFStatusDeniedLong is OpPFStatusDenied past the stall deadline; it
	// survives heal-all (Trial.settleStart).
	OpPFStatusDeniedLong OpKind = "pfStatusDeniedLong"
	// OpOOBLateWrite replays an older out-of-band failover record late.
	OpOOBLateWrite OpKind = "oobLateWrite"
	// OpRestartPromotedTarget restarts a run's target K yields after the
	// run's read_only clear on it (data kept, boots read_only=ON).
	OpRestartPromotedTarget OpKind = "restartPromotedTarget"
	// OpReplacePod replaces Site's pod; K > 0 regresses its data directory
	// K ticks (#218).
	OpReplacePod OpKind = "replacePod"

	// OpPFMetaFault: the next N engine metadata writes (terminal marker,
	// request consumption, deadline annotations, acknowledgement cleanup)
	// fail with class K: 0 timed out uncommitted, 1 committed with the
	// response lost, 2 forbidden (K184).
	OpPFMetaFault OpKind = "pfMetaFault"
	// OpPFAckHold arms a human acknowledgement: K polls after an automation
	// hold first stands, the acknowledgement annotation is set to the held
	// run's ID (N=1: padded with whitespace, which the operator trims).
	// Only inside the fault window (K174).
	OpPFAckHold OpKind = "pfAckHold"
	// opPFSettleOutage fails the next N settlement reads only: a settlement
	// that ends Unknown (an ambiguous write while the API server stops
	// answering). Generated only as the companion of an OpPFWriteLate.
	opPFSettleOutage OpKind = "pfSettleOutage"
)

// Op is one scheduled fault operation, applied at the start of poll At.
type Op struct {
	At     int
	Kind   OpKind
	Site   string
	Peer   string // destination for one-way cuts; partner for pair partitions
	N      int    // count for ambiguous/failing mutations, or crash countdown
	Fenced bool   // crash boot mode: true = sidecar re-fences on boot
	// BootWritable (crash boot mode, with Fenced false) boots read_only=OFF:
	// a pod not yet rolled to the read_only=ON mysqld flag, mid-upgrade.
	// Otherwise an unfenced boot is read_only=ON with super_read_only OFF.
	BootWritable bool

	// PreApply (OpCrashOperatorMid) kills the operator BEFORE the fatal
	// statement reaches the server rather than after it applied.
	PreApply bool
	// Down (OpCrashOperatorMid) is how many polls the operator stays dead
	// before its replacement starts.
	Down int
	// K is the second count of an actor-stream op: the late-commit delay
	// (OpPFWriteLate), the yield countdown (OpRestartPromotedTarget), or
	// the regress depth (OpReplacePod).
	K int
}

func (o Op) String() string {
	s := fmt.Sprintf("@p%03d %s", o.At, o.Kind)
	if o.Site != "" {
		s += " site=" + o.Site
	}
	if o.Peer != "" {
		s += " peer=" + o.Peer
	}
	if o.N != 0 || o.Kind == OpCrashOperatorMid {
		s += fmt.Sprintf(" n=%d", o.N)
	}
	if o.Kind == OpRegressBoot {
		s += fmt.Sprintf(" bootFenced=%v", o.Fenced)
	}
	if o.Kind == OpCrash {
		s += fmt.Sprintf(" bootFenced=%v", o.Fenced)
		if o.BootWritable {
			s += " bootWritable=true"
		}
	}
	if o.Kind == OpCrashOperatorMid {
		s += fmt.Sprintf(" preApply=%v down=%d", o.PreApply, o.Down)
	}
	if o.K != 0 {
		s += fmt.Sprintf(" k=%d", o.K)
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

	// Actors is the planned-failover actor schedule (§15.1). The zero value
	// (GenerateTrial's) disables it, and the trial runs exactly as before
	// the actors existed.
	Actors Actors
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
	seed := fmt.Sprintf("%d", t.Seed)
	if t.Seed == 0 {
		seed = "hand-built"
	}
	fmt.Fprintf(&b, "seed=%s sites=%d roles=%v priorities=%v cooldown=%ds history=%v warmup=%d healAt=%d polls=%d\n",
		seed, len(t.SiteNames), t.Roles, t.Priorities, t.CooldownSec, t.SeedHistory, t.WarmupPolls, t.HealAt, t.Polls)
	fmt.Fprintf(&b, "  sidecar: leasePolls=%d topologyAware=%v tickAfterPoll=%v\n",
		t.SidecarLeasePolls, t.SidecarTopology, t.SidecarAfterPoll)
	for i, op := range t.Ops {
		masked := ""
		if t.Skip != nil && t.Skip[i] {
			masked = " [masked]"
		}
		fmt.Fprintf(&b, "  %s%s\n", op, masked)
	}
	if t.Actors.Enabled {
		b.WriteString(t.Actors.String())
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
	{OpPartPair, 5},
	{OpPartOneWay, 4},
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

// flushFaultStream salts the PRNG stream of the binary log rotation faults.
const flushFaultStream = 0x4f1bb1e5f1a5c0de

// flushFaults draws a trial's binary log rotation faults (OpFailFlush,
// OpSlowFlush): in two trials out of five, one or two ops inside the fault
// window, each on one site and good for the next one to three flushes; and
// its pre-reclone wait (OpRecloneWait). A stream of its own keeps the rest
// of every seed's schedule as it was.
func flushFaults(seed uint64, names []string, warmup, faultWindow int) []Op {
	rng := rand.New(rand.NewPCG(seed, flushFaultStream))
	ops := make([]Op, 0, 3)
	if rng.IntN(100) < 40 {
		for n := 1 + rng.IntN(2); n > 0; n-- {
			op := Op{At: warmup + rng.IntN(faultWindow), Kind: OpFailFlush, Site: names[rng.IntN(len(names))], N: 1 + rng.IntN(3)}
			if rng.IntN(2) == 0 {
				op.Kind = OpSlowFlush
				op.K = rng.IntN(2)
			}
			ops = append(ops, op)
		}
	}
	// A pre-reclone archive-tail wait, in three trials out of ten. Drawn
	// after the rotation faults so their draws are unchanged.
	if rng.IntN(100) < 30 {
		ops = append(ops, Op{At: warmup + rng.IntN(faultWindow), Kind: OpRecloneWait, Site: names[rng.IntN(len(names))],
			N: 5 + rng.IntN(30), K: rng.IntN(4)})
	}
	return ops
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
		kind := pickFaultKind(rng.IntN(100))
		switch kind {
		case OpCrash:
			site := pickSite()
			start := at()
			// One draw, as before the read-only boot mode existed, so the
			// rest of every schedule is unchanged: 45% fenced, 45% the
			// read_only=ON boot, 10% a legacy writable boot.
			boot := rng.IntN(100)
			op := Op{At: start, Kind: OpCrash, Site: site, Fenced: boot < 45, BootWritable: boot >= 90}
			ops = append(ops, op)
			if rng.IntN(100) < 70 {
				h := start + 2 + rng.IntN(faultWindow)
				if h < healAt {
					ops = append(ops, Op{At: h, Kind: OpRecover, Site: site, Fenced: op.Fenced, BootWritable: op.BootWritable})
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
		case OpPartOneWay:
			// Identical draw shape to OpPartPair: Site=from, Peer=to, no
			// extra direction coin. Inserted immediately after the pair
			// bucket so only rolls that land in this 4-point subrange
			// change behavior; later kinds keep their pickFaultKind rolls.
			a := pickSite()
			b := pickSite()
			for b == a {
				b = pickSite()
			}
			start := at()
			ops = append(ops, Op{At: start, Kind: OpPartOneWay, Site: a, Peer: b})
			pairedHeal(start, OpHealOneWay, a, b)
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
		case OpDNSOutage:
			start := at()
			ops = append(ops, Op{At: start, Kind: OpDNSOutage})
			pairedHeal(start, OpDNSHeal, "", "")
		default:
			panic(fmt.Sprintf("dst: faultWeights kind %q has no case in GenerateTrial", kind))
		}
	}

	ops = append(ops, flushFaults(seed, names, warmup, faultWindow)...)
	ops = append(ops, ownLossFaults(seed, names, warmup, faultWindow, healAt)...)
	ops = append(ops, ownLossEvidenceFaults(seed, names, warmup, faultWindow, healAt)...)
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
