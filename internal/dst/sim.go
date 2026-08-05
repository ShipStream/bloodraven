// Package dst is a deterministic simulation testing (DST) engine for the
// Bloodraven failover control loop.
//
// It runs the real TopologyManager — the production Poll cycle, failover
// controller, recovery, source convergence, DNS reconcile, and primary
// re-assert logic — against an in-memory model of a multi-site MySQL
// cluster. Every trial is driven by a seeded PRNG: the fault schedule
// (crashes, partitions, ambiguous writes, operator restarts, rogue
// fencing) is generated up front from the seed, execution consumes no
// randomness, and the fake clock advances one poll interval per cycle.
// A failing trial therefore replays exactly from its seed, and its fault
// schedule can be shrunk to a minimal reproduction.
//
// The model is deliberately small but semantically faithful where the
// operator's correctness depends on it: GTID sets advance as vectors of
// contiguous per-UUID transaction ranges, replication is a two-stage
// fetch/apply pump with relay-log backlog, super_read_only implies
// read_only (and read_only=OFF clears super_read_only), RESET REPLICA
// ALL requires stopped threads and purges relay logs, and errant
// transactions break the IO thread the way error 1236 does. Faults the
// model cannot express (e.g. CLONE semantics) are excluded and the
// corresponding operator features are disabled in trials.
package dst

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ---------------------------------------------------------------------------
// GTID vectors
// ---------------------------------------------------------------------------

// gtidVec is a GTID set restricted to contiguous [1..n] ranges per server
// UUID. The simulated workload only ever appends to a site's own stream and
// replication applies in order, so every set that arises in the model is
// exactly representable. Rendered strings are parsed by the operator with the
// production mysql.ParseGTIDSet.
type gtidVec map[string]int64

func (v gtidVec) clone() gtidVec {
	out := make(gtidVec, len(v))
	for u, n := range v {
		out[u] = n
	}
	return out
}

// contains reports whether v covers every transaction in o.
func (v gtidVec) contains(o gtidVec) bool {
	for u, n := range o {
		if n > 0 && v[u] < n {
			return false
		}
	}
	return true
}

// deficit returns the total number of transactions in o missing from v.
func (v gtidVec) deficit(o gtidVec) int64 {
	var d int64
	for u, n := range o {
		if n > v[u] {
			d += n - v[u]
		}
	}
	return d
}

func (v gtidVec) String() string {
	uuids := make([]string, 0, len(v))
	for u, n := range v {
		if n > 0 {
			uuids = append(uuids, u)
		}
	}
	sort.Strings(uuids)
	parts := make([]string, 0, len(uuids))
	for _, u := range uuids {
		if v[u] == 1 {
			parts = append(parts, u+":1")
		} else {
			parts = append(parts, fmt.Sprintf("%s:1-%d", u, v[u]))
		}
	}
	return strings.Join(parts, ",")
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

// EventKind labels a model-level occurrence. Only main-thread occurrences
// (operator mutations, DNS writes, harness fault ops) are evented so event
// order is deterministic; concurrent read probes are not.
type EventKind string

const (
	EvSetSuperRO   EventKind = "setSuperReadOnly"
	EvSetRO        EventKind = "setReadOnly"
	EvPromote      EventKind = "promote"
	EvStopReplica  EventKind = "stopReplica"
	EvResetReplica EventKind = "resetReplicaAll"
	EvChangeSource EventKind = "changeSource"
	EvStartReplica EventKind = "startReplica"
	EvStartSQL     EventKind = "startReplicaSQL"
	EvDrain        EventKind = "waitRelayDrain"
	EvKillConns    EventKind = "killAppConns"
	EvDNSSet       EventKind = "dnsSet"
	EvStateWrite   EventKind = "failoverStateWrite"

	// Sidecar FencingMonitor actions. Distinct from the operator's own
	// EvSetSuperRO so invariants that ask "did the OPERATOR fence this
	// site" cannot be satisfied by a sidecar that happened to fence it.
	EvSidecarFence EventKind = "sidecarSelfFence"
	EvSidecarKill  EventKind = "sidecarKillConns"

	// Harness-injected fault events.
	EvCrash           EventKind = "fault.crash"
	EvRecoverSite     EventKind = "fault.recover"
	EvPartOpSite      EventKind = "fault.partitionOperatorSite"
	EvHealOpSite      EventKind = "fault.healOperatorSite"
	EvPartPair        EventKind = "fault.partitionPair"
	EvHealPair        EventKind = "fault.healPair"
	EvOperatorRestart EventKind = "fault.operatorRestart"
	EvOperatorArm     EventKind = "fault.armOperatorCrash"
	EvOperatorDie     EventKind = "fault.operatorDied"
	EvAmbiguous       EventKind = "fault.ambiguousMutations"
	EvFailMutations   EventKind = "fault.failMutations"
	EvStallApply      EventKind = "fault.stallApply"
	EvHealApply       EventKind = "fault.healApply"
	EvStallFetch      EventKind = "fault.stallFetch"
	EvHealFetch       EventKind = "fault.healFetch"
	EvDrainStall      EventKind = "fault.drainStall"
	EvHealDrain       EventKind = "fault.healDrain"
	EvWedgeFence      EventKind = "fault.rogueFence"
	EvRogueWritable   EventKind = "fault.rogueWritable"
	EvStatusOutage    EventKind = "fault.statusWriteOutage"
	EvStatusHeal      EventKind = "fault.statusWriteHeal"
	EvStateOutage     EventKind = "fault.failoverStateOutage"
	EvStateHeal       EventKind = "fault.failoverStateHeal"
	EvDNSOutage       EventKind = "fault.dnsOutage"
	EvDNSHeal         EventKind = "fault.dnsHeal"
)

// Event is one model-level occurrence during a trial.
type Event struct {
	Poll   int
	Seq    int
	Site   string
	Kind   EventKind
	Detail string
	// Outcome is "" (applied cleanly), "failed" (rejected without applying),
	// or "ambiguous" (applied but an error was returned to the operator).
	Outcome string
}

func (e Event) String() string {
	s := fmt.Sprintf("p%03d %s %s", e.Poll, e.Site, e.Kind)
	if e.Detail != "" {
		s += " " + e.Detail
	}
	if e.Outcome != "" {
		s += " [" + e.Outcome + "]"
	}
	return s
}

// Violation is an invariant breach detected during or after a trial.
type Violation struct {
	Invariant string
	Poll      int
	Detail    string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s @p%03d: %s", v.Invariant, v.Poll, v.Detail)
}

// ---------------------------------------------------------------------------
// Site + cluster model
// ---------------------------------------------------------------------------

// siteData is the full state of one simulated MySQL server. All access goes
// through Cluster.mu.
type siteData struct {
	name string
	host string
	lbIP string
	uuid string

	crashed       bool
	readOnly      bool
	superReadOnly bool

	// Replication channel state. ioWant/sqlWant are the thread run flags
	// (START/STOP REPLICA); the *effective* running state additionally
	// requires reachability of the source and no sticky thread error.
	replConfigured bool
	sourceHost     string
	ioWant         bool
	sqlWant        bool
	ioErr          string
	sqlErr         string

	executed  gtidVec // applied transactions (durable)
	retrieved gtidVec // fetched into relay log (durable, purged by RESET REPLICA ALL)

	hasData bool
}

// Cluster is the shared virtual MySQL cluster plus fault state. The operator
// interacts with it only through simChecker/simDNS; the harness mutates fault
// state and pumps workload+replication between polls.
type Cluster struct {
	mu     sync.Mutex
	sites  []*siteData
	byName map[string]*siteData
	byHost map[string]*siteData
	byLBIP map[string]string // lb ip -> site name

	// Fault state.
	opLinkDown    map[string]bool
	pairLinkDown  map[string]bool // key: "a|b" with a < b
	ambiguousMuts map[string]int  // site -> next N mutations apply-but-error
	failMuts      map[string]int  // site -> next N mutations error-without-apply
	fetchStalled  map[string]bool
	applyStalled  map[string]bool
	drainStalled  map[string]bool
	dnsDenied     bool
	statusDenied  bool
	// stateDenied rejects writes to the out-of-band anti-flap store. It is
	// a SEPARATE knob from statusDenied on purpose: the two durable paths
	// have different RBAC rules and different admission chains in
	// production, so a trial in which only one is denied is the realistic
	// case, and the one the durable store exists to survive.
	stateDenied bool

	// acked accumulates every transaction acknowledged to the simulated
	// application, across all primaries the trial ever had.
	acked gtidVec

	// Event capture. pollEvents always holds the current poll's events (for
	// per-poll invariants); the full log is kept only in capture mode.
	capture    bool
	events     []Event
	pollEvents []Event
	seq        int
	poll       int

	// Mid-Execute operator death. crashCountdown counts operator mutations
	// down to the fatal one; -1 means unarmed. operatorDead gates every
	// operator-side interaction with the model afterwards.
	crashCountdown int
	crashPreApply  bool
	operatorDead   bool

	// Model-detected violations (checked at mutation time).
	violations []Violation
}

// SiteSpec configures one simulated site.
type SiteSpec struct {
	Name string
	Host string
	LBIP string
	UUID string
}

// NewCluster builds a cluster in a converged steady state: the first site is
// the writable primary holding baselineTxns transactions, every other site is
// a fenced read-only replica of it, fully caught up.
func NewCluster(specs []SiteSpec, baselineTxns int64) *Cluster {
	c := &Cluster{
		byName:        make(map[string]*siteData),
		byHost:        make(map[string]*siteData),
		byLBIP:        make(map[string]string),
		opLinkDown:    make(map[string]bool),
		pairLinkDown:  make(map[string]bool),
		ambiguousMuts: make(map[string]int),
		failMuts:      make(map[string]int),
		fetchStalled:  make(map[string]bool),
		applyStalled:  make(map[string]bool),
		drainStalled:  make(map[string]bool),
		acked:         make(gtidVec),

		crashCountdown: -1,
	}
	primaryUUID := specs[0].UUID
	base := gtidVec{primaryUUID: baselineTxns}
	for i, sp := range specs {
		s := &siteData{
			name:     sp.Name,
			host:     sp.Host,
			lbIP:     sp.LBIP,
			uuid:     sp.UUID,
			executed: base.clone(),
			hasData:  baselineTxns > 0,
		}
		if i == 0 {
			s.readOnly = false
			s.superReadOnly = false
			s.retrieved = make(gtidVec)
		} else {
			s.readOnly = true
			s.superReadOnly = true
			s.replConfigured = true
			s.sourceHost = specs[0].Host
			s.ioWant = true
			s.sqlWant = true
			s.retrieved = base.clone()
		}
		c.sites = append(c.sites, s)
		c.byName[sp.Name] = s
		c.byHost[canonicalHost(sp.Host)] = s
		c.byLBIP[sp.LBIP] = sp.Name
	}
	c.acked = base.clone()
	return c
}

func canonicalHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	h = strings.TrimSuffix(h, ":3306")
	return strings.TrimSuffix(h, ".")
}

func pairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// SetCapture enables full event logging (used when reproducing a failure).
func (c *Cluster) SetCapture(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capture = on
}

// BeginPoll stamps the poll index and resets the per-poll event window.
func (c *Cluster) BeginPoll(p int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.poll = p
	c.pollEvents = c.pollEvents[:0]
}

// PollEvents returns a copy of the events recorded since BeginPoll.
func (c *Cluster) PollEvents() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.pollEvents))
	copy(out, c.pollEvents)
	return out
}

// Events returns the full captured event log (capture mode only).
func (c *Cluster) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.events))
	copy(out, c.events)
	return out
}

// Violations returns model-detected violations so far.
func (c *Cluster) Violations() []Violation {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Violation, len(c.violations))
	copy(out, c.violations)
	return out
}

// event must be called with c.mu held.
func (c *Cluster) event(site string, kind EventKind, detail, outcome string) {
	e := Event{Poll: c.poll, Seq: c.seq, Site: site, Kind: kind, Detail: detail, Outcome: outcome}
	c.seq++
	c.pollEvents = append(c.pollEvents, e)
	if c.capture {
		c.events = append(c.events, e)
	}
}

// violate must be called with c.mu held.
func (c *Cluster) violate(invariant, detail string) {
	c.violations = append(c.violations, Violation{Invariant: invariant, Poll: c.poll, Detail: detail})
}

// ---------------------------------------------------------------------------
// Truth accessors (harness/invariant side)
// ---------------------------------------------------------------------------

// SiteTruth is a read-only snapshot of one site's model state.
type SiteTruth struct {
	Name          string
	Crashed       bool
	ReadOnly      bool
	SuperReadOnly bool
	OpLinkDown    bool
	ReplConfig    bool
	SourceHost    string
	IORunning     bool
	SQLRunning    bool
	LastError     string
	Executed      gtidVec
	Retrieved     gtidVec
}

// Writable reports whether the site would accept an application write.
func (t SiteTruth) Writable() bool {
	return !t.Crashed && !t.ReadOnly && !t.SuperReadOnly
}

// Reachable reports whether the operator can reach the site.
func (t SiteTruth) Reachable() bool {
	return !t.Crashed && !t.OpLinkDown
}

// Truth returns a snapshot of every site's model state, in site order.
func (c *Cluster) Truth() []SiteTruth {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]SiteTruth, 0, len(c.sites))
	for _, s := range c.sites {
		io, sql := c.effectiveThreadsLocked(s)
		out = append(out, SiteTruth{
			Name:          s.name,
			Crashed:       s.crashed,
			ReadOnly:      s.readOnly,
			SuperReadOnly: s.superReadOnly,
			OpLinkDown:    c.opLinkDown[s.name],
			ReplConfig:    s.replConfigured,
			SourceHost:    s.sourceHost,
			IORunning:     io,
			SQLRunning:    sql,
			LastError:     firstNonEmpty(s.sqlErr, s.ioErr),
			Executed:      s.executed.clone(),
			Retrieved:     s.retrieved.clone(),
		})
	}
	return out
}

// SiteCrashed reports whether the named site's process is down.
func (c *Cluster) SiteCrashed(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byName[name].crashed
}

// Acked returns the ledger of all application-acknowledged transactions.
func (c *Cluster) Acked() gtidVec {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acked.clone()
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// effectiveThreadsLocked computes the observable replica thread states.
func (c *Cluster) effectiveThreadsLocked(s *siteData) (io, sql bool) {
	if !s.replConfigured || s.crashed {
		return false, false
	}
	io = s.ioWant && s.ioErr == ""
	if io {
		src := c.byHost[canonicalHost(s.sourceHost)]
		if src == nil || src.crashed || c.pairLinkDown[pairKey(s.name, src.name)] || src == s {
			io = false // "Connecting": link or source down
		}
	}
	sql = s.sqlWant && s.sqlErr == ""
	return io, sql
}

// ---------------------------------------------------------------------------
// Fault mutators (harness thread, between polls)
// ---------------------------------------------------------------------------

func (c *Cluster) Crash(site string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.byName[site]
	if s.crashed {
		return
	}
	s.crashed = true
	c.event(site, EvCrash, "", "")
}

// Recover restarts a crashed site. fenced=true models the sidecar winning the
// boot race (comes back super_read_only); fenced=false models the documented
// "old primary respawned writable" case (read_only=0 in server config, no
// sidecar fence yet). Replica threads auto-start when channel metadata exists
// (MySQL default, no skip_replica_start).
func (c *Cluster) Recover(site string, fenced bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.byName[site]
	if !s.crashed {
		return
	}
	s.crashed = false
	if fenced {
		s.readOnly = true
		s.superReadOnly = true
	} else {
		s.readOnly = false
		s.superReadOnly = false
	}
	if s.replConfigured {
		s.ioWant = true
		s.sqlWant = true
		s.ioErr = ""
		s.sqlErr = ""
	}
	c.event(site, EvRecoverSite, fmt.Sprintf("fenced=%v", fenced), "")
}

func (c *Cluster) SetOperatorLink(site string, down bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.opLinkDown[site] == down {
		return
	}
	c.opLinkDown[site] = down
	if down {
		c.event(site, EvPartOpSite, "", "")
	} else {
		c.event(site, EvHealOpSite, "", "")
	}
}

func (c *Cluster) SetPairLink(a, b string, down bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := pairKey(a, b)
	if c.pairLinkDown[k] == down {
		return
	}
	c.pairLinkDown[k] = down
	if down {
		c.event(a, EvPartPair, "peer="+b, "")
	} else {
		c.event(a, EvHealPair, "peer="+b, "")
	}
}

func (c *Cluster) AddAmbiguousMutations(site string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ambiguousMuts[site] += n
	c.event(site, EvAmbiguous, fmt.Sprintf("n=%d", n), "")
}

func (c *Cluster) AddFailMutations(site string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failMuts[site] += n
	c.event(site, EvFailMutations, fmt.Sprintf("n=%d", n), "")
}

func (c *Cluster) SetApplyStalled(site string, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyStalled[site] == on {
		return
	}
	c.applyStalled[site] = on
	if on {
		c.event(site, EvStallApply, "", "")
	} else {
		c.event(site, EvHealApply, "", "")
	}
}

func (c *Cluster) SetFetchStalled(site string, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fetchStalled[site] == on {
		return
	}
	c.fetchStalled[site] = on
	if on {
		c.event(site, EvStallFetch, "", "")
	} else {
		c.event(site, EvHealFetch, "", "")
	}
}

func (c *Cluster) SetDrainStalled(site string, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.drainStalled[site] == on {
		return
	}
	c.drainStalled[site] = on
	if on {
		c.event(site, EvDrainStall, "", "")
	} else {
		c.event(site, EvHealDrain, "", "")
	}
}

// RogueFence models a sidecar or human setting super_read_only=ON outside the
// operator (e.g. a stale fencing lease firing after promotion).
func (c *Cluster) RogueFence(site string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.byName[site]
	if s.crashed {
		return
	}
	s.superReadOnly = true
	s.readOnly = true
	c.event(site, EvWedgeFence, "", "")
}

// RogueWritable models a human running SET GLOBAL read_only=OFF on a replica.
func (c *Cluster) RogueWritable(site string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.byName[site]
	if s.crashed {
		return
	}
	s.readOnly = false
	s.superReadOnly = false
	c.event(site, EvRogueWritable, "", "")
}

func (c *Cluster) SetDNSDenied(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dnsDenied == on {
		return
	}
	c.dnsDenied = on
	if on {
		c.event("", EvDNSOutage, "", "")
	} else {
		c.event("", EvDNSHeal, "", "")
	}
}

func (c *Cluster) SetStatusDenied(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.statusDenied == on {
		return
	}
	c.statusDenied = on
	if on {
		c.event("", EvStatusOutage, "", "")
	} else {
		c.event("", EvStatusHeal, "", "")
	}
}

// StatusDenied reports whether simulated CR status writes are being rejected.
func (c *Cluster) StatusDenied() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statusDenied
}

// SetStateDenied toggles the out-of-band anti-flap store outage.
func (c *Cluster) SetStateDenied(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stateDenied == on {
		return
	}
	c.stateDenied = on
	if on {
		c.event("", EvStateOutage, "", "")
	} else {
		c.event("", EvStateHeal, "", "")
	}
}

// StateDenied reports whether out-of-band anti-flap writes are being
// rejected.
func (c *Cluster) StateDenied() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stateDenied
}

// NoteFailoverStateWrite records an out-of-band anti-flap write attempt.
func (c *Cluster) NoteFailoverStateWrite(target string, denied bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	outcome := ""
	if denied {
		outcome = "denied"
	}
	c.event("", EvStateWrite, "target="+target, outcome)
}

// ClearInjectedCallFaults drops un-consumed ambiguous/failing mutation
// counters. Used by heal-all so the settle window is fault-free.
func (c *Cluster) ClearInjectedCallFaults() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.ambiguousMuts)
	clear(c.failMuts)
}

// NoteOperatorRestart records the restart in the event stream.
func (c *Cluster) NoteOperatorRestart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.event("", EvOperatorRestart, "", "")
}

// ---------------------------------------------------------------------------
// Mid-Execute operator death
// ---------------------------------------------------------------------------
//
// A plain operator restart lands between polls, so every promotion sequence
// it interrupts is interrupted at a boundary the operator chose. Real
// processes die in the middle: after RESET REPLICA ALL and before the
// read_only grant, after fencing one peer and before fencing the next,
// after the promotion and before the durable write.
//
// Death is modeled as "the process stops interacting" rather than as a
// panic: the fatal statement lands (or does not, per PreApply), the flag
// goes up, and from then until the harness builds a replacement, every
// operator-side call — reads, mutations, DNS, status, the anti-flap store —
// fails or is dropped. Unwinding the real stack with a panic would be no
// more faithful (the manager is discarded either way) and would risk
// unwinding through one of Poll's per-site goroutines, where a recover on
// the harness goroutine cannot catch it.

// errOperatorDead is what a dead process's in-flight calls see. It never
// reaches production code paths that inspect error text; it exists so the
// remainder of the dying Poll cannot change the model.
var errOperatorDead = errors.New("dst: operator process died mid-execute")

// ArmOperatorCrash schedules the operator's death on the nth subsequent
// mutation (n == 0 kills on the very next one). preApply kills before the
// statement reaches the server rather than after it applied.
func (c *Cluster) ArmOperatorCrash(n int, preApply bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.crashCountdown = n
	c.crashPreApply = preApply
	c.event("", EvOperatorArm, fmt.Sprintf("afterMutations=%d preApply=%v", n, preApply), "")
}

// ReviveOperator clears the death flag; the harness calls it when it builds
// the replacement process.
func (c *Cluster) ReviveOperator() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operatorDead = false
	c.crashCountdown = -1
}

// DisarmOperatorCrash cancels a countdown that has not fired yet, without
// reviving an operator that already died. Used by heal-all so the settle
// window is fault-free.
func (c *Cluster) DisarmOperatorCrash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.crashCountdown = -1
}

// OperatorDead reports whether the operator process is currently dead.
func (c *Cluster) OperatorDead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.operatorDead
}

// crashNowLocked reports whether this mutation is the fatal one, consuming
// the countdown. Must be called with c.mu held.
func (c *Cluster) crashNowLocked() bool {
	if c.crashCountdown < 0 {
		return false
	}
	if c.crashCountdown > 0 {
		c.crashCountdown--
		return false
	}
	c.crashCountdown = -1
	return true
}

// killOperatorLocked marks the process dead. Must be called with c.mu held.
func (c *Cluster) killOperatorLocked(site string, kind EventKind, applied bool) {
	c.operatorDead = true
	c.event(site, EvOperatorDie, fmt.Sprintf("during=%s applied=%v", kind, applied), "")
}

// ---------------------------------------------------------------------------
// Workload + replication pump (harness thread, between polls)
// ---------------------------------------------------------------------------

// Tick advances the data plane by one step: the application writes one
// transaction to every currently-writable site (adversarial: some client
// somewhere still holds a connection to anything writable), then replication
// fetches and applies subject to links, stalls, and errant-GTID breakage.
func (c *Cluster) Tick() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Application writes.
	for _, s := range c.sites {
		if s.crashed || s.readOnly || s.superReadOnly {
			continue
		}
		s.executed[s.uuid]++
		s.hasData = true
		if c.acked[s.uuid] < s.executed[s.uuid] {
			c.acked[s.uuid] = s.executed[s.uuid]
		}
	}

	// Replication pump.
	for _, s := range c.sites {
		if s.crashed || !s.replConfigured {
			continue
		}
		src := c.byHost[canonicalHost(s.sourceHost)]

		// IO thread: fetch from source into the relay log.
		if s.ioWant && s.ioErr == "" && src != nil && src != s && !src.crashed &&
			!c.pairLinkDown[pairKey(s.name, src.name)] {
			// Errant-transaction check (error 1236): with AUTO_POSITION the
			// source refuses a replica that has transactions it lacks.
			if !src.executed.contains(s.executed) {
				s.ioErr = "Got fatal error 1236 from source: replica has transactions the source is missing"
				s.ioWant = false
			} else if !c.fetchStalled[s.name] {
				for u, n := range src.executed {
					from := s.retrieved[u]
					if s.executed[u] > from {
						from = s.executed[u]
					}
					if n > from {
						s.retrieved[u] = n
					} else if s.retrieved[u] < from {
						s.retrieved[u] = from
					}
				}
			}
		}

		// SQL thread: apply the relay backlog.
		if s.sqlWant && s.sqlErr == "" && !c.applyStalled[s.name] {
			for u, n := range s.retrieved {
				if n > s.executed[u] {
					s.executed[u] = n
				}
			}
		}
	}
}
