package dst

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// claimView is what the planned invariants need to see of the process's
// PlannedClaimSlot. The slot's fields are unexported; the implementation
// is an accessor hook the claim's …ForSim seam provides. Without one
// (nil), the claim-introspecting invariants judge the durable block
// instead, or are skipped where only the slot can answer.
type claimView interface {
	// Held returns the slot's claim; ok=false when the slot is empty.
	Held() (runID string, hold bool, ok bool)
	// Releases returns the release notes recorded since the last call.
	Releases() []claimRelease
}

// claimRelease is one slot release (the slot's claimReleaseNote) and the
// actor that was running when it happened ("" when not attributed).
type claimRelease struct {
	RunID string
	Why   controller.ClaimReleaseReason
	By    string
}

// engineReleaseReasons is every reason the engine may release with (§3.6).
var engineReleaseReasons = []controller.ClaimReleaseReason{
	controller.ReleaseNotCommitted, controller.ReleaseAdmissionTimeout, controller.ReleasePublishing,
	controller.ReleaseTerminal, controller.ReleaseToHold, controller.ReleaseAck, controller.ReleaseZombie,
	controller.ReleaseManagerAbsent, controller.ReleaseStatusStalled,
}

// Markers that tag a violation's detail with the finding it belongs to
// (README, Finding classes: the fixed ones and any open one); fingerprint
// turns each into a class of its own, so a sweep reports which finding a
// trial hit.
const (
	// writableTargetWithChannelMarker: the hold was set over a target that
	// is the only writable site and still has its replication channel.
	writableTargetWithChannelMarker = "(the target is the only writable site and still has its replication channel)"
	// replacedTargetSameProcessMarker: the hold was set by the process
	// that wrote Promoting, over a target replaced under a new server_uuid.
	replacedTargetSameProcessMarker = "(the target was replaced under a new server_uuid and the process that wrote Promoting decided)"
	// unreadablePeerMarker: every other writable site was unreadable by the
	// operator when the run confirmed.
	unreadablePeerMarker = "(every other writable site was unreadable by the operator at the confirmation)"

	// regressedTargetMarker: the hold was set over the intent's server on
	// an older copy of its data, with the evidence for a rollback complete.
	regressedTargetMarker = "(the target answers on an older copy of its data)"

	// The outcome findings are recognised by the text of their detail.
	// targetLacksFencedSetMarker: a Succeeded run whose target lacked part
	// of the fenced set when the run confirmed.
	targetLacksFencedSetMarker = "the target lacks fenced transactions"
	// rolledBackAfterClearMarker: a RolledBack run that sent read_only=OFF
	// to its target, which was still writable by that clear, or held
	// transactions outside the fenced set, when the rollback was decided.
	rolledBackAfterClearMarker = "ended RolledBack but it sent read_only=OFF to the target"
	// rolledBackFencedSourceMarker: a RolledBack run whose source is not
	// writable and that never read it writable.
	rolledBackFencedSourceMarker = "ended RolledBack but the source "
)

// unconfirmedGrace is debounce (FailureThreshold 3 + RecoveryThreshold 2)
// plus 10s, in polls: how long after PromotedUnconfirmed with an
// unreachable target the group may go without a writable primary
// (UnconfirmedBlocksEmergency).
const unconfirmedGrace = 5 + 10

// zombiePasses is how many engine passes a Run-mode claim may outlive its
// run (ZombieClaim).
const zombiePasses = 2

// runObs is everything the monitor learned about one planned run.
type runObs struct {
	id, source, target string

	// fenceGen is the process generation in which a pf fence landed on
	// the source (the fence memo lives in that process); -1 if none.
	fenceGen int
	// fenceInc is the source's incarnation when the run's fence landed; a
	// later incarnation means the fence is no longer the run's. healed is
	// set once the source was writable again after the run ended.
	fenceInc int
	// fenceUUID is the source's server_uuid when the run's fence landed:
	// the production memo attributes a fence only to that server (K078).
	fenceUUID string
	healed    bool
	// intentUUID is the durable Fencing intent's sourceServerUUID; fenceSet
	// is fence.gtidSet (F).
	intentUUID string
	fenceSet   string

	targetWritable bool // the run made its target writable
	targetInc      int  // the target's incarnation when the run made it writable
	resets         int  // RESET REPLICA ALL on the target
	roClearTried   bool // a read_only=OFF was sent to the target
	writeTried     bool // a super_read_only=OFF or read_only=OFF was sent to the target
	promotionStmt  bool // any promotion statement was sent to the target
	pfMutations    int  // every pf mutation attempt for the run

	marked bool
	// holdJudged: HoldWithoutDataAtRisk was judged for the run's hold (at
	// its condition flip or its ManualRecoveryRequired marker, whichever
	// landed first).
	holdJudged bool
	terminal   bool
	outcome    v1alpha1.PlannedFailoverOutcome
	reason     v1alpha1.PlannedFailoverReason

	// watermark tracking (WatermarkRegressed).
	published      bool
	confirmedAt    time.Time
	watermarkSeen  bool
	watermarkFlags bool

	// gateWritable is the truth-writable non-target set at the last
	// pre-write gate probe of the current engine pass, nil when the pass
	// has none (K180: a target unfence needs one); gateJudged marks that
	// the gate's first unfence was judged against it (the SRO/RO pair
	// shares one gate); gateOpen marks the gate-to-SET window (for
	// RogueRaceResidual). All three close at the end of the pass and when
	// the run ends (K179).
	gateWritable []string
	gateJudged   bool
	gateOpen     bool
	// probeSeq is the cluster sequence at the run's last facade peer probe
	// (-1: none): only the run's own probe makes a later rogue the R2
	// residual (K183).
	probeSeq int
	// confirmWritable and publishWritable are the truth-writable sites at
	// the confirm probe and at the Publishing record.
	confirmWritable []string
	publishWritable []string
	confirmSeen     bool
	// confirmUnreadable are the sites of confirmWritable the operator could
	// not read at the confirm probe.
	confirmUnreadable []string
	// peerObs is, per site other than the target, what the last pre-write
	// gate or confirm probe that reached it found, and the process that
	// probed. confirmResidual are the sites of confirmUnreadable that are
	// the documented residual of the confirmation (§7.3): the confirming
	// process last read them read-only at such a probe, and an external
	// writer made them writable after it.
	peerObs         map[string]peerProbeObs
	confirmResidual []string

	// unconfirmedSince is the poll the UnconfirmedBlocksEmergency window
	// started at: the run ended PromotedUnconfirmed, and since that poll
	// every precondition of the emergency promotion held (the target
	// unreachable among them). -1: not judged (the outcome is another, or a
	// primary existed since).
	unconfirmedSince int
	// unconfirmedGen is the process generation unconfirmedSince was counted
	// in; unconfirmedFault marks an injected fault on a promotion statement
	// the topology manager sent to the run's source since the last poll
	// boundary (emergencyPromotionStatement). Both restart the window.
	unconfirmedGen   int
	unconfirmedFault bool
	// rollbackSawSourceWritable: the source was truth-writable at a yield
	// or an own unfence while the run's block read RollingBack, so a
	// RolledBack that says "the source is writable" was true when the run
	// read it (another actor may fence it again before the record commits).
	rollbackSawSourceWritable bool
	// fencingSawSourceWritable: the source was truth-writable at a peer
	// probe of the current engine pass while the run's block read Fencing,
	// and the run has not fenced it since. A RolledBack decided in that
	// pass without a rollback ("authority moved before the fence was sent")
	// read the source writable; its sidecar may fence it before the record
	// commits. It closes with the pass (closeGate).
	fencingSawSourceWritable bool
	// sourceLost: at a peer probe of the rollback the source was
	// truth-writable without part of the fenced set, or without
	// transactions of its own stream that a site the operator can read
	// holds (the target, or any other). Two histories exist from then on,
	// which is what a hold is for (sourceLacksHistory).
	sourceLost bool
	// rollbackTarget is the server that stood in for the target when the
	// run's RollingBack record committed: a RolledBack is judged against
	// the target the rollback was decided over, not the one that answers
	// when the terminal record lands.
	rollbackTarget *rollbackTargetObs
	// roClearSeq is the cluster sequence of the last read_only=OFF the run
	// sent that reached, or may have reached, its target (-1: none).
	roClearSeq int
	// confirmTarget and publishTarget are whether the target held the
	// fenced set at the confirm probe and at the Publishing record.
	confirmTarget, publishTarget *bool
	// superseded: after the run ended SourceLeftFenced, the topology
	// manager took authority away from the fenced source: it counted
	// another site as the group's primary (managerActive), or it fenced
	// the source or repointed it at another site itself. The re-assert no
	// longer names the source then, and what becomes of the other primary
	// is not the run's fence (FencedSourceNeverHealed). A site an external
	// writer made writable that the manager never adopted (a read-only
	// reader, which it fences; a site it never observed) does not count.
	superseded bool
	startSeq   int // the cluster sequence when the monitor first saw the run
	// promotingGen is the process generation that committed the run's
	// Promoting record (-1: none). A terminal record from a later process
	// was decided without the promotion memo (§7.1: a cross-process re-run
	// cannot prove nothing was sent).
	promotingGen int
	targetUUID   string // promotion.targetServerUUID
	zombieFor    int    // engine passes the slot has held this terminal run
	// tailReleased: the confirmed tail released the claim (why=publishing)
	// while the block still read Promoting (the DST-4 ruling); the block
	// owns the sites durably without a live claim until Publishing commits.
	// Only in the process that released it: a restart hydrates the claim
	// again (tailReleasedGen is that process's generation).
	tailReleased    bool
	tailReleasedGen int
}

// rollbackTargetObs is the target as it was when a run's RollingBack
// record committed.
type rollbackTargetObs struct {
	serverUUID string
	writable   bool
	// outsideFence: it held transactions outside the fenced set (always
	// false while the fenced set is unknown).
	outsideFence bool
	// seq is the cluster sequence then.
	seq int
}

// peerProbeObs is one pre-write gate or confirm probe's finding about a
// site: the process generation that probed, the cluster sequence then, and
// whether the site read read-only.
type peerProbeObs struct {
	gen, seq int
	readOnly bool
}

// plannedStats are statistics, not violations.
type plannedStats struct {
	// RogueRaceResidual counts rogue writability landing inside an open
	// gate-to-SET window (§18 R2).
	RogueRaceResidual int
	// FirstFaultPhase is the run phase when the first store fault op hit.
	FirstFaultPhase string
	// ReplacedWhileHeld counts manager replacements while a run held the
	// claim.
	ReplacedWhileHeld int
	// Deferrals counts "planned failover in progress" cross-site deferrals.
	Deferrals int
	// ReplacedTargetHold counts holds set by a cross-process re-run that
	// found the target replaced (§7.1 row 1).
	ReplacedTargetHold int
	// SucceededTargetRestarted counts Succeeded runs whose target restarted
	// (booting read-only) between the run's read_only=OFF and the record.
	SucceededTargetRestarted int
	// UnfenceBesideUnreadable counts rollback unfences beside a writable
	// site the operator could not read (§9 step 1: allowed by rule).
	UnfenceBesideUnreadable int
	// UnreadableReaderWritable counts fence, gate, confirm and record
	// observations beside a writable read-only reader the operator could
	// not read (§4.1: an unreadable reader is allowed, with a warning).
	UnreadableReaderWritable int
	// UnreadableTargetHold counts holds set while the run's target could
	// not be read and another site was writable (§4.4: the target may hold
	// the run's writes).
	UnreadableTargetHold int
	// UnreadablePeerResidual counts Succeeded runs confirmed beside a
	// writable promotable site the operator could not read then, which had
	// answered read-only at the confirming process's pre-write gate or an
	// earlier confirm probe and was made writable by an external writer
	// after it (§7.3: allowed, with the warning UnreachablePeers).
	UnreadablePeerResidual int
	// SourceLostHold counts holds set by a rollback that found the source
	// writable without transactions it held (two histories; §9).
	SourceLostHold int
	// StalledBeforeFence counts runs ended RolledBack by a status stall
	// before any fence of theirs landed, over a source that is not
	// writable (§6.4: the stall end reads nothing and says only that the
	// run sent no fence; the source's state is someone else's doing).
	StalledBeforeFence int
}

// plannedMonitor checks the planned-failover invariants of §15.1. The
// harness feeds it the cluster's events and landed mutations (onEvent and
// onMutation, called with Cluster.mu held), the store's commits, yields,
// and the poll boundaries. Every check counts rogue writability: truth is
// the model, not the operator's view.
type plannedMonitor struct {
	c     *Cluster
	roles map[string]state.SiteRole
	sites []string

	cur   *v1alpha1.MysqlFailoverGroup // the object after the last commit
	runs  map[string]*runObs
	order []string

	procGen int // bumped by every operator restart
	claim   claimView

	poll       int
	violations []Violation
	flagged    map[string]bool
	stats      plannedStats

	// rogueSeq is the event sequence of the last rogue read_only=0 per
	// site. A foreign writable site whose rogue writability landed after
	// the run's last facade peer probe (runObs.probeSeq) is the §18 R2
	// residual, not an operator fault.
	rogueSeq map[string]int
	// onRecorded is called when a run's Publishing record commits, with its
	// target and confirmedAt: the durable point of a planned promotion.
	onRecorded func(target string, confirmedAt time.Time)
	// noManager reports whether the group has no registered topology
	// manager now (nil: it always has one).
	noManager func() bool
	// activeSite reports the site the registered topology manager counts
	// as the group's primary after its last poll: its Status().ActiveSite,
	// the one writable promotable site it observed ("" when it observed
	// none or several, or no manager runs). nil derives the same from the
	// model's truth, as a manager that had just polled every site would
	// (managerActive).
	activeSite func() string
}

func newPlannedMonitor(c *Cluster, sites []string, roles []state.SiteRole) *plannedMonitor {
	m := &plannedMonitor{
		c:        c,
		roles:    map[string]state.SiteRole{},
		sites:    append([]string(nil), sites...),
		runs:     map[string]*runObs{},
		flagged:  map[string]bool{},
		rogueSeq: map[string]int{},
	}
	for i, s := range sites {
		m.roles[s] = roles[i]
	}
	return m
}

// violate records one violation per (invariant, key).
func (m *plannedMonitor) violate(invariant, key, detail string) {
	k := invariant + "|" + key
	if m.flagged[k] {
		return
	}
	m.flagged[k] = true
	m.violations = append(m.violations, Violation{Invariant: invariant, Poll: m.poll, Detail: detail})
}

// managerActive is the site the topology manager counts as the group's
// primary now: activeSite when the harness wired the manager's own view,
// otherwise the one truth-writable site, if it is promotable and the
// operator can reach it (TopologyManager.activeSiteLocked on the truth).
func (m *plannedMonitor) managerActive(truth []SiteTruth) string {
	if m.activeSite != nil {
		return m.activeSite()
	}
	active := ""
	for _, t := range truth {
		if !t.Writable() {
			continue
		}
		if active != "" || !t.Reachable() {
			return ""
		}
		active = t.Name
	}
	if active != "" && m.roles[active] != state.SiteRolePrimaryCandidate {
		return ""
	}
	return active
}

// emergencyPromotionStatement reports whether e is a statement the
// emergency promotion of its site depends on (FailoverController.Execute on
// the candidate: the relay drain, STOP REPLICA, RESET REPLICA ALL and the
// two read-only clears). The best-effort FLUSH BINARY LOGS is not one: a
// rotation that fails or is abandoned never stops a promotion.
func emergencyPromotionStatement(e Event) bool {
	switch e.Kind {
	case EvDrain, EvStopReplica, EvResetReplica, EvPromote:
		return true
	case EvSetSuperRO, EvSetRO:
		return e.Detail == "on=false"
	}
	return false
}

// activeRun is the run the durable block names, while it is not terminal.
func (m *plannedMonitor) activeRun() *runObs {
	if m.cur == nil || m.cur.Status.PlannedFailover == nil {
		return nil
	}
	r := m.runs[m.cur.Status.PlannedFailover.RunID]
	if r == nil || r.terminal {
		return nil
	}
	return r
}

// ownsSites is the controller's plannedFailoverOwnsSites (through its sim
// seam, so the two cannot drift, K181), restricted to blocks that name a
// run: a block without a runID is malformed, and no claim can name it.
func ownsSites(obj *v1alpha1.MysqlFailoverGroup) bool {
	if obj == nil || obj.Status.PlannedFailover == nil || obj.Status.PlannedFailover.RunID == "" {
		return false
	}
	return controller.PlannedFailoverOwnsSitesForSim(obj)
}

// claimHeld reports whether a run holds the claim: the slot when a view
// exists, the durable block otherwise.
func (m *plannedMonitor) claimHeld() bool {
	if m.claim != nil {
		_, hold, ok := m.claim.Held()
		return ok && !hold
	}
	return ownsSites(m.cur)
}

// crossSite reports whether e is a cross-site action the claim and the hold
// exclude: anything but fencing a non-promotable (dr-only or reader) site.
func (m *plannedMonitor) crossSite(e Event) bool {
	switch e.Kind {
	case EvPromote, EvResetReplica, EvStopReplica, EvStartReplica, EvChangeSource, EvSetRO, EvDrain, EvKillConns, EvFlushBinlog:
		return true
	case EvSetSuperRO:
		return e.Detail != "on=true" || m.roles[e.Site] == state.SiteRolePrimaryCandidate
	}
	return false
}

// autoMutation reports whether e changes a promotable site's writability
// (HoldNotHonoured).
func (m *plannedMonitor) autoMutation(e Event) bool {
	if m.roles[e.Site] != state.SiteRolePrimaryCandidate && e.Kind == EvSetSuperRO && e.Detail == "on=true" {
		return false // fencing a non-promotable site is not an automatic promotion action
	}
	if m.roles[e.Site] == state.SiteRoleReadOnly {
		return false
	}
	switch e.Kind {
	case EvPromote, EvResetReplica, EvSetSuperRO:
		return true
	case EvSetRO:
		return e.Detail == "on=false"
	}
	return false
}

func isUnfence(e Event) bool {
	return (e.Kind == EvSetSuperRO || e.Kind == EvSetRO) && e.Detail == "on=false"
}

// onEvent sees every recorded event (c.mu held).
func (m *plannedMonitor) onEvent(e Event) {
	if e.Kind == EvRecoverSite && strings.Contains(e.Detail, "bootWritable=true") {
		// A pod booted writable (not yet rolled to read_only=ON) is an
		// external writer, like a human's read_only=0.
		m.rogueSeq[e.Site] = e.Seq
		return
	}
	if e.Kind == EvRogueWritable {
		m.rogueSeq[e.Site] = e.Seq
		for _, id := range m.order {
			if m.runs[id].gateOpen {
				m.stats.RogueRaceResidual++
			}
		}
		return
	}
	if effectiveActor(e.Actor) == ActorTM && e.Outcome != "" && emergencyPromotionStatement(e) {
		// An injected fault on a promotion statement the topology manager
		// sent to the source of an unconfirmed run: its emergency promotion
		// was attempted and retries after a backoff
		// (UnconfirmedBlocksEmergency). A failing or abandoned rotation is
		// not such a fault: the promotion goes on without it.
		for _, id := range m.order {
			if r := m.runs[id]; r.unconfirmedSince >= 0 && e.Site == r.source {
				r.unconfirmedFault = true
			}
		}
	}
	if effectiveActor(e.Actor) == ActorTM && observedOutcome(e.Outcome) &&
		(e.Kind == EvChangeSource || (e.Kind == EvSetSuperRO && e.Detail == "on=true")) {
		// The topology manager fenced the source of a run that ended
		// SourceLeftFenced, or made it a replica of another site: the
		// source is the manager's from here on (FencedSourceNeverHealed).
		for _, id := range m.order {
			if r := m.runs[id]; r.terminal && r.outcome == v1alpha1.PlannedFailoverOutcomeSourceLeftFenced && !r.healed && e.Site == r.source {
				r.superseded = true
			}
		}
	}
	if effectiveActor(e.Actor) != ActorPF || strings.HasPrefix(string(e.Kind), "fault.") || e.Kind == EvPinSession {
		return
	}
	r := m.activeRun()
	if r == nil {
		return
	}
	r.pfMutations++
	if e.Site != r.target {
		return
	}
	switch {
	case e.Kind == EvSetRO && e.Detail == "on=false":
		// A statement the process died before sending ("operatorDied":
		// not applied) never reached the target: a re-run that then finds
		// the target read-only inside F may roll back honestly. Every
		// other outcome reached the server or may have.
		if e.Outcome != "operatorDied" {
			r.roClearTried = true
			r.roClearSeq = e.Seq
		}
		r.writeTried = true
		r.promotionStmt = true
	case isUnfence(e):
		r.writeTried = true
		r.promotionStmt = true
	case e.Kind == EvStopReplica, e.Kind == EvResetReplica:
		r.promotionStmt = true
	}
}

// writableExceptLocked lists the truth-writable sites other than site.
// c.mu is held by the caller.
func (m *plannedMonitor) writableExceptLocked(site string) []string {
	var out []string
	for _, w := range m.c.writableSitesLocked() {
		if w != site {
			out = append(out, w)
		}
	}
	return out
}

// operatorReadableLocked reports whether the operator can read site now
// (not crashed, operator link up). c.mu is held by the caller.
func (m *plannedMonitor) operatorReadableLocked(site string) bool {
	s := m.c.byName[site]
	return s != nil && !s.crashed && !m.c.opLinkDown[site]
}

// readableOrPromotableLocked drops from sites the read-only readers the
// operator cannot read now, counting them. A planned failover is strict
// about its promotable peers only (§4.1): a primary-candidate or dr-only
// site must answer read-only at the grant, the fence and the gate, while an
// unreadable reader is allowed with the UnreachablePeers warning, so a
// reader that is writable where the operator cannot see it is the documented
// residual, not a decision the run got wrong. A writable reader the operator
// can read, and every writable promotable site, still count. c.mu is held
// by the caller.
func (m *plannedMonitor) readableOrPromotableLocked(sites []string) []string {
	var out []string
	for _, w := range sites {
		if m.roles[w] == state.SiteRoleReadOnly && !m.operatorReadableLocked(w) {
			m.stats.UnreadableReaderWritable++
			continue
		}
		out = append(out, w)
	}
	return out
}

// readableOrPromotable is readableOrPromotableLocked for callers that do
// not hold c.mu.
func (m *plannedMonitor) readableOrPromotable(sites []string) []string {
	truth := map[string]SiteTruth{}
	for _, t := range m.c.Truth() {
		truth[t.Name] = t
	}
	var out []string
	for _, w := range sites {
		if m.roles[w] == state.SiteRoleReadOnly && !truth[w].Reachable() {
			m.stats.UnreadableReaderWritable++
			continue
		}
		out = append(out, w)
	}
	return out
}

// externallyWritable reports whether a rogue or a writable boot made site
// writable at or after event sequence since.
func (m *plannedMonitor) externallyWritable(site string, since int) bool {
	seq, ok := m.rogueSeq[site]
	return ok && seq >= since
}

// raceResidual splits foreign writable sites into those made writable by a
// rogue after run r's last peer probe (the R2 residual, counted) and the
// rest. A run that never probed has no residual.
func (m *plannedMonitor) raceResidual(r *runObs, sites []string) []string {
	if r.probeSeq < 0 {
		return sites
	}
	var rest []string
	for _, w := range sites {
		if m.externallyWritable(w, r.probeSeq) {
			m.stats.RogueRaceResidual++
			continue
		}
		rest = append(rest, w)
	}
	return rest
}

// onMutation sees every landed mutation (c.mu held).
func (m *plannedMonitor) onMutation(e Event, before, after SiteTruth) {
	actor := effectiveActor(e.Actor)
	switch actor {
	case ActorPF:
		m.onPFMutation(e, before, after)
	case ActorTM, ActorUpdate:
		if m.crossSite(e) && m.claimHeld() {
			m.violate("ForeignMutationUnderClaim", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"%s %s %s by %s landed while a planned run holds the claim", e.Site, e.Kind, e.Detail, actor))
		}
		// An ordered update is automation too: admission must refuse it
		// under a standing hold (K060).
		if m.autoMutation(e) && holdStands(m.cur) {
			m.violate("HoldNotHonoured", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"%s %s %s by %s while the automation hold stands", e.Site, e.Kind, e.Detail, actor))
		}
	}
}

// blockRun is the run the durable block names, terminal or not.
func (m *plannedMonitor) blockRun() *runObs {
	if m.cur == nil || m.cur.Status.PlannedFailover == nil {
		return nil
	}
	return m.runs[m.cur.Status.PlannedFailover.RunID]
}

func (m *plannedMonitor) onPFMutation(e Event, before, after SiteTruth) {
	// Every landed pf mutation belongs to a live run (K040): a stale pass
	// that unfences or resets after its run's end, or before any run is
	// recorded, is the planned path acting without a run.
	r := m.blockRun()
	switch {
	case r == nil:
		m.violate("PlannedMutationWithoutRun", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
			"the planned path sent %s %s to %s with no run recorded", e.Kind, e.Detail, e.Site))
		return
	case r.marked || markerRunID(m.cur.GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation]) == r.id:
		m.violate("MarkedRunResumed", r.id, fmt.Sprintf("run %s sent %s %s to %s after its terminal marker",
			r.id, e.Kind, e.Detail, e.Site))
		return
	case r.terminal:
		m.violate("PlannedMutationWithoutRun", r.id+"|terminal", fmt.Sprintf(
			"run %s sent %s %s to %s after its terminal record (outcome %s)", r.id, e.Kind, e.Detail, e.Site, r.outcome))
		return
	}
	// A run never acts under another run's standing hold: admission refuses
	// new runs until the hold is acknowledged (K060).
	if h := controller.PlannedHoldFrom(m.cur); h != nil && h.RunID != r.id {
		m.violate("HoldNotHonoured", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
			"run %s sent %s %s to %s while run %q's automation hold stands", r.id, e.Kind, e.Detail, e.Site, h.RunID))
	}

	// PlannedFenceWithoutAuthority: just before the fence, S must have been
	// the unique writable site.
	if e.Kind == EvSetSuperRO && e.Detail == "on=true" && e.Site == r.source {
		others := m.raceResidual(r, m.readableOrPromotableLocked(m.writableExceptLocked(e.Site)))
		if !before.Writable() || len(others) > 0 {
			m.violate("PlannedFenceWithoutAuthority", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"run %s fenced %s (writable before: %v; other writable sites: %v)", r.id, e.Site, before.Writable(), others))
		}
		r.fenceGen = m.procGen
		r.fenceInc = after.Incarnation
		r.fenceUUID = after.ServerUUID
		// What the pass read before its own fence no longer describes the
		// source.
		r.fencingSawSourceWritable = false
	}

	if isUnfence(e) && e.Site == r.source {
		// A writable peer the operator cannot read does not block the
		// rollback's unfence (§9 step 1, #234's rule): that is the
		// documented residual, counted, not a violation.
		var readable []string
		for _, w := range m.writableExceptLocked(e.Site) {
			if m.operatorReadableLocked(w) {
				readable = append(readable, w)
			} else {
				m.stats.UnfenceBesideUnreadable++
			}
		}
		if others := m.raceResidual(r, readable); len(others) > 0 {
			m.violate("UnfenceBesideWritable", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"run %s unfenced source %s while %v writable", r.id, e.Site, others))
		}
		if after.Writable() {
			r.rollbackSawSourceWritable = true
		}
		byMemo := r.fenceGen == m.procGen && strings.EqualFold(r.fenceUUID, after.ServerUUID)
		byIntent := r.intentUUID != "" && r.intentUUID == after.ServerUUID
		if !byMemo && !byIntent {
			m.violate("UnfenceUnattributed", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"run %s unfenced source %s (server_uuid %s) with no fence memo and no matching durable intent (intent uuid %q)",
				r.id, e.Site, after.ServerUUID, r.intentUUID))
		}
	}

	if e.Site != r.target {
		return
	}
	if isUnfence(e) {
		switch {
		case r.gateWritable == nil:
			m.violate("PlannedPromoteWithoutGate", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"run %s sent %s %s to its target %s with no pre-write gate probe in the pass", r.id, e.Kind, e.Detail, e.Site))
		case !r.gateJudged && len(r.gateWritable) > 0:
			m.violate("PlannedPromoteBesideWritable", r.id, fmt.Sprintf(
				"run %s cleared read-only on %s although %v were writable at the pre-write gate's probe",
				r.id, e.Site, r.gateWritable))
		}
		r.gateJudged = true
		r.gateOpen = false
	}
	switch e.Kind {
	case EvStopReplica, EvKillConns:
		if r.targetWritable {
			m.violate("RepeatedPromotion", r.id, fmt.Sprintf("run %s sent %s to %s after making it writable", r.id, e.Kind, e.Site))
		}
	case EvResetReplica:
		r.resets++
		if r.targetWritable || r.resets > 1 {
			m.violate("RepeatedPromotion", r.id, fmt.Sprintf(
				"run %s sent RESET REPLICA ALL to %s (resets=%d, target already writable=%v)", r.id, e.Site, r.resets, r.targetWritable))
		}
	case EvFlushBinlog:
		// The promotion's rotation precedes the clears: a target the run
		// made writable gets none while it is still writable (the Writable
		// stage) or holds its own transactions outside F (WasWritable). A
		// target that restarted read-only after the run's clears and
		// before any write landed holds exactly F with no channel. That is
		// the ReplicaReset stage again, as the engine classifies it, and a
		// new process, which has no rotation memo, rotates it once more
		// before the clears: one more empty file. A target an external
		// writer made writable after the pass classified it is not the
		// run's doing either: the gate that follows the rotation refuses.
		own, err := ownExtras(before, r.fenceSet)
		if r.targetWritable && (before.Writable() || err != nil || !own.IsEmpty()) {
			m.violate("RepeatedPromotion", r.id, fmt.Sprintf(
				"run %s sent FLUSH BINARY LOGS to %s after making it writable, while it was still writable or held its own transactions outside F",
				r.id, e.Site))
		}
	case EvSetRO:
		if e.Detail == "on=false" && after.Writable() {
			r.targetWritable = true
			r.targetInc = after.Incarnation
		}
	}
}

// onYield observes yield points: the gate and confirm probes record truth.
func (m *plannedMonitor) onYield(point string) {
	class := yieldClass(point)
	if r := m.activeRun(); r != nil {
		var src SiteTruth
		truth := m.c.Truth()
		for _, t := range truth {
			if t.Name == r.source {
				src = t
			}
		}
		switch m.cur.Status.PlannedFailover.Phase {
		case v1alpha1.PlannedFailoverPhaseRollingBack:
			if src.Name != "" && src.Writable() {
				r.rollbackSawSourceWritable = true
				if class == YieldPeerProbe && sourceLacksHistory(src, truth, r.fenceSet) {
					r.sourceLost = true
				}
			}
		case v1alpha1.PlannedFailoverPhaseFencing:
			if class == YieldPeerProbe && src.Name != "" && src.Writable() {
				r.fencingSawSourceWritable = true
			}
		}
	}
	if class == YieldPeerProbe {
		if r := m.activeRun(); r != nil {
			r.probeSeq = m.c.Seq()
		}
		return
	}
	if class != YieldGateProbe && class != YieldConfirmProbe {
		return
	}
	r := m.activeRun()
	if r == nil {
		return
	}
	var others []string
	for _, t := range m.c.Truth() {
		if t.Writable() && t.Name != r.target {
			others = append(others, t.Name)
		}
	}
	others = m.readableOrPromotable(others)
	if others == nil {
		others = []string{}
	}
	// What the probe read from every site it reached: the engine falls back
	// on these answers at a confirmation that cannot read a promotable site.
	seq := m.c.Seq()
	truth := m.c.Truth()
	if r.peerObs == nil {
		r.peerObs = map[string]peerProbeObs{}
	}
	for _, t := range truth {
		if t.Name != r.target && t.Reachable() {
			r.peerObs[t.Name] = peerProbeObs{gen: m.procGen, seq: seq, readOnly: !t.Writable()}
		}
	}
	if class == YieldGateProbe {
		r.gateWritable = others
		r.gateJudged = false
		r.gateOpen = true
		return
	}
	r.confirmWritable = m.readableOrPromotable(m.c.writableSites())
	r.confirmUnreadable, r.confirmResidual = nil, nil
	for _, t := range truth {
		if t.Name == r.target && r.fenceSet != "" {
			has := containsSet(t, r.fenceSet)
			r.confirmTarget = &has
		}
		if !t.Writable() || t.Reachable() {
			continue
		}
		r.confirmUnreadable = append(r.confirmUnreadable, t.Name)
		// The residual: this process read the site read-only at its gate or
		// an earlier confirm probe, and what made it writable since is an
		// external writer. A site the process never read, or last read
		// writable, or one a restart forgot, is not.
		if o, ok := r.peerObs[t.Name]; ok && t.Name != r.target && o.gen == m.procGen && o.readOnly && m.externallyWritable(t.Name, o.seq) {
			r.confirmResidual = append(r.confirmResidual, t.Name)
		}
	}
	r.confirmSeen = true
}

// onCommit observes every committed object change.
func (m *plannedMonitor) onCommit(kind string, before, after *v1alpha1.MysqlFailoverGroup) {
	m.cur = after
	m.onMarker(before, after)
	// The status failover record is timestamp-monotonic: no writer moves it
	// to an older failover, the runner's two-step snapshot write included
	// (K042).
	if b := before.Status.LastFailoverTime(); b != nil {
		if a := after.Status.LastFailoverTime(); a == nil || a.Before(b) {
			m.violate("StatusRecordRegressed", fmt.Sprintf("%s|%d", kind, m.c.Seq()), fmt.Sprintf(
				"a %s write moved status.lastFailover from %s to %v", kind, b.UTC().Format(time.RFC3339), a))
		}
	}
	pf := after.Status.PlannedFailover
	if pf == nil || pf.RunID == "" {
		m.checkWatermarks(after)
		return
	}
	r := m.runs[pf.RunID]
	if r == nil {
		r = &runObs{id: pf.RunID, fenceGen: -1, unconfirmedSince: -1, startSeq: m.c.Seq(), promotingGen: -1, probeSeq: -1, roClearSeq: -1}
		m.runs[pf.RunID] = r
		m.order = append(m.order, pf.RunID)
	}
	r.target = pf.TargetSite
	if pf.SourceSite != "" {
		r.source = pf.SourceSite
	}
	if pf.Fence != nil {
		r.intentUUID = pf.Fence.SourceServerUUID
		if pf.Fence.GtidSet != nil {
			r.fenceSet = *pf.Fence.GtidSet
		}
	}
	if markerRunID(after.GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation]) == r.id {
		r.marked = true
	}
	if pf.Phase == v1alpha1.PlannedFailoverPhasePromoting && r.promotingGen < 0 {
		r.promotingGen = m.procGen
	}
	if pf.Promotion != nil {
		r.targetUUID = pf.Promotion.TargetServerUUID
	}
	if pf.Phase == v1alpha1.PlannedFailoverPhaseRollingBack && r.rollbackTarget == nil {
		// The rollback was decided over the server that answers for the
		// target now.
		for _, t := range m.c.Truth() {
			if t.Name != r.target {
				continue
			}
			obs := &rollbackTargetObs{serverUUID: t.ServerUUID, writable: t.Writable(), seq: m.c.Seq()}
			if f, err := mysql.ParseGTIDSet(r.fenceSet); err == nil && r.fenceSet != "" {
				exec, err := mysql.ParseGTIDSet(t.Executed.String())
				obs.outsideFence = err != nil || !exec.Subtract(f).IsEmpty()
			}
			r.rollbackTarget = obs
		}
	}
	if (pf.Phase == v1alpha1.PlannedFailoverPhasePublishing || pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded) &&
		!r.published && r.fenceSet != "" {
		r.published = true
		r.publishWritable = m.readableOrPromotable(m.c.writableSites())
		for _, t := range m.c.Truth() {
			if t.Name == r.target {
				has := containsSet(t, r.fenceSet)
				r.publishTarget = &has
			}
		}
		if pf.Promotion != nil && pf.Promotion.ConfirmationTime != nil {
			r.confirmedAt = pf.Promotion.ConfirmationTime.UTC().Truncate(time.Second)
		}
		if m.onRecorded != nil {
			m.onRecorded(r.target, r.confirmedAt)
		}
	}
	if pf.Result != nil && !r.terminal {
		r.terminal = true
		r.outcome = pf.Result.Outcome
		r.reason = pf.Result.Reason
		m.checkTerminal(r)
	}
	if !holdConditionTrue(before) && holdConditionTrue(after) {
		m.judgeHold(r)
	}
	m.checkWatermarks(after)
}

// onMarker judges a run ended by its terminal marker when the marker
// commits (K039): the marker is the run's end, and the status result that
// copies it lands only after status writes heal, against later truth. A
// ManualRecoveryRequired marker is a hold on its own (PlannedHoldFrom), so
// its justification is judged then too (K077).
func (m *plannedMonitor) onMarker(before, after *v1alpha1.MysqlFailoverGroup) {
	key := v1alpha1.PlannedFailoverTerminalAnnotation
	v := after.GetAnnotations()[key]
	if v == "" || v == before.GetAnnotations()[key] {
		return
	}
	mk, ok := controller.ParseTerminalMarker(v)
	if !ok {
		return
	}
	r := m.runs[mk.RunID]
	if r == nil {
		return
	}
	r.marked = true
	if !r.terminal {
		r.terminal = true
		r.outcome = mk.Outcome
		r.reason = mk.Reason
		m.checkTerminal(r)
	}
	if mk.Outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		m.judgeHold(r)
	}
}

// judgeHold runs checkHoldAtRisk once per run.
func (m *plannedMonitor) judgeHold(r *runObs) {
	if r.holdJudged {
		return
	}
	r.holdJudged = true
	m.checkHoldAtRisk(r)
}

func holdConditionTrue(obj *v1alpha1.MysqlFailoverGroup) bool {
	if obj == nil {
		return false
	}
	for _, c := range obj.Status.Conditions {
		if c.Type == v1alpha1.ConditionAutomaticFailoverHeld && c.Status == "True" {
			return true
		}
	}
	return false
}

// ownExtras returns the target's transactions in its own server_uuid stream
// that are not in F.
func ownExtras(t SiteTruth, fence string) (mysql.GTIDSet, error) {
	f, err := mysql.ParseGTIDSet(fence)
	if err != nil {
		return nil, err
	}
	exec, err := mysql.ParseGTIDSet(t.Executed.String())
	if err != nil {
		return nil, err
	}
	extra := exec.Subtract(f)
	out := mysql.GTIDSet{}
	for u, ivs := range extra {
		if strings.EqualFold(u, t.ServerUUID) {
			out[u] = ivs
		}
	}
	return out, nil
}

// containsSet reports whether t's executed set contains the GTID set s.
func containsSet(t SiteTruth, s string) bool {
	want, err := mysql.ParseGTIDSet(s)
	if err != nil {
		return false
	}
	have, err := mysql.ParseGTIDSet(t.Executed.String())
	if err != nil {
		return false
	}
	return want.Subtract(have).IsEmpty()
}

// checkTerminal is OutcomeDishonest and SucceededBesideWritable, judged
// against truth at the terminal write.
func (m *plannedMonitor) checkTerminal(r *runObs) {
	// What the deciding pass read at its peer probe, before the pass's
	// observations close with the run.
	fencingSawSourceWritable := r.fencingSawSourceWritable
	r.closeGate()
	truth := map[string]SiteTruth{}
	var writable []string
	for _, t := range m.c.Truth() {
		truth[t.Name] = t
		if t.Writable() {
			writable = append(writable, t.Name)
		}
	}
	dishonest := func(why string) {
		m.violate("OutcomeDishonest", r.id, fmt.Sprintf("run %s ended %s but %s", r.id, r.outcome, why))
	}
	src, tgt := truth[r.source], truth[r.target]
	switch r.outcome {
	case v1alpha1.PlannedFailoverOutcomeNotStarted:
		if r.pfMutations > 0 {
			dishonest(fmt.Sprintf("it sent %d mutations", r.pfMutations))
		}
	case v1alpha1.PlannedFailoverOutcomeRolledBack:
		// RolledBack says nothing was written to the target. After a
		// read_only=OFF reached it (or may have), that is judged by what
		// the server held when the rollback was decided, not by the
		// statement: the server that stood in for the target when the
		// RollingBack record committed must have read read-only with
		// nothing outside the fenced set. That is the re-proof's evidence
		// for the server the intent names (the same data directory holds
		// exactly what it held before the clears, so no transaction
		// committed while it was writable), and the DST-3 ruling's for a
		// server that replaced it. A run that ended RolledBack without a
		// RollingBack record is judged against the target as it is now.
		if r.roClearTried {
			at := r.rollbackTarget
			if at == nil {
				at = &rollbackTargetObs{serverUUID: tgt.ServerUUID, writable: tgt.Writable(), seq: m.c.Seq()}
				if f, err := mysql.ParseGTIDSet(r.fenceSet); err == nil && r.fenceSet != "" {
					exec, err := mysql.ParseGTIDSet(tgt.Executed.String())
					at.outsideFence = err != nil || !exec.Subtract(f).IsEmpty()
				}
			}
			// A target an external writer made writable after the run's
			// last clear is not writable by the run's doing.
			stillWritable := at.writable && !m.externallyWritable(r.target, r.roClearSeq)
			if stillWritable || (at.outsideFence && !m.externallyWritable(r.target, r.startSeq)) {
				dishonest(fmt.Sprintf("it sent read_only=OFF to the target, which was writable=%v and held transactions outside the fenced set=%v when the rollback was decided",
					at.writable, at.outsideFence))
			}
		}
		// Judged with what the run read: a source it found (or made)
		// writable while rolling back may be fenced again by another actor
		// (its sidecar's lease rule, a rogue) before the record commits. So
		// may a source the pass that ended the run from Fencing read
		// writable at its peer probe and did not fence.
		//
		// A run ended by a status stall is finished in memory, with no
		// MySQL access (§6.4). Its RolledBack is the process that granted
		// the claim saying that it never sent a fence: "the source is
		// untouched", not "the source is writable". That is judged by what
		// it says: no fence of the run landed on the source. A source that
		// restarted read-only, or that its sidecar fenced, in the two
		// minutes of the stall is not the run's doing.
		switch {
		case r.source == "" || src.Writable() || r.rollbackSawSourceWritable || fencingSawSourceWritable:
		case r.reason == v1alpha1.PlannedFailoverReasonStatusStalled && r.fenceGen < 0:
			m.stats.StalledBeforeFence++
		default:
			dishonest("the source " + r.source + " is not writable")
		}
		if r.fenceSet != "" {
			// Own extras an external writer put on the target during the
			// run are not the run's writes.
			if extra, err := ownExtras(tgt, r.fenceSet); err == nil && !extra.IsEmpty() && !m.externallyWritable(r.target, r.startSeq) {
				dishonest(fmt.Sprintf("the target holds own-UUID extras %s", extra))
			}
		}
	case v1alpha1.PlannedFailoverOutcomeSourceLeftFenced:
		// SourceLost and ManagerUnavailable end the run without reaching the
		// source (§6.2): whether it is fenced is what the run cannot know,
		// and the sidecar lease rule or the emergency path decides it.
		// So does a run ended while no manager is registered, whatever
		// reason its block already carried: a RollingBack run keeps its
		// rollback reason through the 5m manager-absence end.
		unreachable := r.reason == v1alpha1.PlannedFailoverReasonSourceLost ||
			r.reason == v1alpha1.PlannedFailoverReasonManagerUnavailable ||
			(m.noManager != nil && m.noManager())
		if r.source != "" && src.Writable() && !unreachable && !m.externallyWritable(r.source, r.startSeq) {
			dishonest("the source " + r.source + " is writable")
		}
	case v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed:
		// ManagerUnavailable ends a Promoting run without MySQL access, as
		// PromotedUnconfirmed by rule (§6.3): like SourceLeftFenced above, the
		// run cannot read what reached the target, so no attempt is required.
		if !r.writeTried && !r.crossProcess(m.procGen) && r.reason != v1alpha1.PlannedFailoverReasonManagerUnavailable {
			dishonest("it never sent super_read_only=OFF or read_only=OFF")
		}
		// The UnconfirmedBlocksEmergency window opens here whether or not
		// the target answers at this moment: a poll in which it answers
		// only starts the window again (endPoll).
		r.unconfirmedSince, r.unconfirmedGen = m.poll, m.procGen
	case v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired:
		// A hold needs a possible write to the target, or a source the
		// rollback found writable without transactions it held (two
		// histories whatever the run sent).
		if !r.promotionStmt && !r.crossProcess(m.procGen) && !r.sourceLost {
			dishonest("it sent no promotion statement")
		}
	case v1alpha1.PlannedFailoverOutcomeSucceeded:
		// The claim was released at the Publishing record; what happens to
		// the sites after it (a target restart, a pod booting writable) is
		// the topology manager's to handle. The run is judged at its
		// confirm probe, or at the Publishing record when the facade
		// confirmed without a probe yield (§7.3).
		at := r.confirmWritable
		if !r.confirmSeen {
			at = r.publishWritable
		}
		if at == nil {
			at = m.readableOrPromotable(writable)
		}
		if r.confirmSeen && len(r.confirmResidual) > 0 {
			// The confirmation takes a promotable site that does not answer
			// as a warning when the confirming process last read it
			// read-only, at its gate or since (§7.3): what an external
			// writer did to it afterwards is the documented residual.
			var judged []string
			for _, w := range at {
				if slices.Contains(r.confirmResidual, w) {
					m.stats.UnreadablePeerResidual++
					continue
				}
				judged = append(judged, w)
			}
			at = judged
		}
		var others []string
		for _, w := range at {
			if w != r.target {
				others = append(others, w)
			}
		}
		// A writable peer the operator could not read at the confirmation,
		// and that is not that residual, is tagged: the confirming process
		// never read it for the run, or last read it writable.
		unreadable := ""
		if r.confirmSeen && len(others) > 0 && !slices.ContainsFunc(others, func(w string) bool {
			return !slices.Contains(r.confirmUnreadable, w)
		}) {
			unreadable = " " + unreadablePeerMarker
		}
		if len(at) != 1 || at[0] != r.target {
			dishonest(fmt.Sprintf("the writable sites were %v when it confirmed%s", at, unreadable))
		}
		if len(writable) == 0 && r.targetWritable && tgt.Incarnation != r.targetInc {
			m.stats.SucceededTargetRestarted++
		}
		// The same moment judges what the target held: a target that holds
		// the fenced set when the run confirms and loses part of it
		// afterwards (it restarts on an older data directory) is not a
		// promotion the run got wrong.
		hasFence := r.confirmTarget
		if !r.confirmSeen || hasFence == nil {
			hasFence = r.publishTarget
		}
		if hasFence == nil && r.fenceSet != "" {
			has := containsSet(tgt, r.fenceSet)
			hasFence = &has
		}
		if hasFence != nil && !*hasFence {
			dishonest("the target lacks fenced transactions " + r.fenceSet)
		}
		if len(others) > 0 {
			m.violate("SucceededBesideWritable", r.id, fmt.Sprintf("run %s Succeeded while %v were writable%s", r.id, others, unreadable))
		}
	}
}

// closeGate ends the run's gate observation (pass end, run end).
func (r *runObs) closeGate() {
	r.gateWritable, r.gateJudged, r.gateOpen = nil, false, false
	r.fencingSawSourceWritable = false
}

// sourceLacksHistory reports whether src lacks part of the fenced set, or
// transactions of one of its own streams that one of others holds: the
// model's form of the rollback's plannedSourceLost. As there, only a site
// the operator can read is evidence (the run's target or any other site),
// and a stream a clone into src superseded is not judged: what another site
// holds of it beyond src's copy src gave up at the clone. An unknown fenced
// set proves nothing about the first.
func sourceLacksHistory(src SiteTruth, others []SiteTruth, fence string) bool {
	if fence != "" && !containsSet(src, fence) {
		return true
	}
	for _, o := range others {
		if o.Name == src.Name || !o.Reachable() {
			continue
		}
		for u, n := range o.Executed {
			if src.ownsLiveStream(u) && n > src.Executed[u] {
				return true
			}
		}
	}
	return false
}

// runRecorded reports whether run id recorded its promotion: its
// Publishing record committed, or it ended Succeeded.
func (m *plannedMonitor) runRecorded(id string) bool {
	r := m.runs[id]
	return r != nil && (r.published || (r.terminal && r.outcome == v1alpha1.PlannedFailoverOutcomeSucceeded))
}

// crossProcess reports whether a process other than the one that wrote the
// Promoting record is deciding the run now.
func (r *runObs) crossProcess(gen int) bool { return r.promotingGen >= 0 && r.promotingGen != gen }

// checkHoldAtRisk is HoldWithoutDataAtRisk: a hold needs two writable
// sites, or a site holding transactions outside F.
//
// One spec-mandated exception (§7.1 row 1): a process that finds the
// target's server_uuid changed and cannot prove that no clear reached the
// replaced server (another process than the one that wrote Promoting, or
// that process once it sent super_read_only=OFF or read_only=OFF) cannot
// rule out writes to the replaced pod, whose data is gone from view, and
// holds by design, except where the DST-3 ruling says to roll back: the
// run's own failover record is not durable (it never reached
// RecordPlannedPromotion), the source still holds all of F under the run's
// fence, and no site is writable. A hold in that state is
// HoldWithoutDataAtRisk. The process that wrote Promoting and sent no clear
// knows nothing was written: it gets no exception.
func (m *plannedMonitor) checkHoldAtRisk(r *runObs) {
	if r.sourceLost {
		// The rollback read the source writable without transactions it
		// held: it issues their GTIDs again for other transactions, so two
		// histories exist although no GTID set shows it for long.
		m.stats.SourceLostHold++
		return
	}
	if r.fenceSet == "" {
		return // F unknown: the rule cannot be judged
	}
	f, err := mysql.ParseGTIDSet(r.fenceSet)
	if err != nil {
		return
	}
	if (r.crossProcess(m.procGen) || r.writeTried) && r.targetUUID != "" {
		truth := m.c.Truth()
		for _, t := range truth {
			// Replaced: another server answers for the target, or the
			// intent's server on an older copy of its data (it lacks part of
			// F, which it held when the run entered Promoting). Either way
			// the directory a clear may have reached is gone from view.
			newServer := !strings.EqualFold(t.ServerUUID, r.targetUUID)
			if t.Name == r.target && (newServer || !containsSet(t, r.fenceSet)) {
				if m.replacedTargetRollbackDue(r, truth, f) {
					detail := fmt.Sprintf(
						"run %s held after finding target %s replaced, but its record is not durable, the source holds F under its fence and no site is writable (DST-3: roll back)",
						r.id, r.target)
					switch {
					case !newServer:
						detail += " " + regressedTargetMarker
					case !r.crossProcess(m.procGen):
						detail += " " + replacedTargetSameProcessMarker
					}
					m.violate("HoldWithoutDataAtRisk", r.id, detail)
					return
				}
				m.stats.ReplacedTargetHold++
				return
			}
		}
	}
	writable, otherWritable := 0, false
	var tgt SiteTruth
	for _, t := range m.c.Truth() {
		if t.Writable() {
			writable++
			if t.Name != r.target {
				otherWritable = true
			}
		}
		if t.Name == r.target {
			tgt = t
		}
		if exec, err := mysql.ParseGTIDSet(t.Executed.String()); err == nil && !exec.Subtract(f).IsEmpty() {
			return
		}
	}
	if r.promotingGen >= 0 && otherWritable && !tgt.Reachable() {
		// A promotion intent exists, the pass that decides cannot read the
		// target, and another site is writable: the target may hold the
		// run's writes beside that writer, which is what the hold is for
		// (§4.4, the re-proof after hydration). The truth may be that
		// nothing was written; the run cannot know it.
		m.stats.UnreadableTargetHold++
		return
	}
	if writable <= 1 {
		replaced := r.targetUUID != "" && !strings.EqualFold(tgt.ServerUUID, r.targetUUID)
		detail := fmt.Sprintf(
			"run %s set the automation hold with %d writable site(s) and no site holding GTIDs outside F (target %s: writable=%v channel=%v server_uuid changed=%v; decided by another process than the one that wrote Promoting=%v)",
			r.id, writable, r.target, tgt.Writable(), tgt.ReplConfig, replaced, r.crossProcess(m.procGen))
		switch {
		case tgt.Writable() && tgt.ReplConfig:
			detail += " " + writableTargetWithChannelMarker
		case replaced && !r.crossProcess(m.procGen):
			detail += " " + replacedTargetSameProcessMarker
		}
		m.violate("HoldWithoutDataAtRisk", r.id, detail)
	}
}

// replacedTargetRollbackDue is the DST-3 ruling's precondition, judged on
// the truth and the durable object when the hold lands.
func (m *plannedMonitor) replacedTargetRollbackDue(r *runObs, truth []SiteTruth, f mysql.GTIDSet) bool {
	if m.cur == nil || m.cur.Status.PlannedFailover == nil {
		return false
	}
	pf := m.cur.Status.PlannedFailover
	// The judge has no clock: a far-future now only disables the record
	// reader's clock-skew rejection, which the simulated clock never needs.
	farFuture := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	if pf.RunID != r.id || controller.PlannedOwnRecordDurableForSim(m.cur, pf, farFuture) {
		return false
	}
	for _, t := range truth {
		// Production rolls back only on positive evidence from every
		// promotable peer (K178): an unreachable primary-candidate or
		// dr-only site's writability is unknown, and the source must be
		// reachable to be unfenced. A read-only-role reader that does not
		// answer is allowed there as everywhere (§4.1), so it does not
		// excuse a hold; neither does what it is doing out of sight.
		if !t.Reachable() {
			if m.roles[t.Name] == state.SiteRoleReadOnly && t.Name != r.source && t.Name != r.target {
				continue
			}
			return false
		}
		if t.Writable() {
			return false
		}
		exec, err := mysql.ParseGTIDSet(t.Executed.String())
		if t.Name == r.target && (err != nil || !exec.Subtract(f).IsEmpty()) {
			// The server under the new server_uuid holds transactions outside
			// F: it kept the data of the one that may have been written to
			// (the same data directory under a new auto.cnf, say).
			return false
		}
		if t.Name != r.source {
			continue
		}
		if !t.SuperReadOnly || r.intentUUID == "" || !strings.EqualFold(t.ServerUUID, r.intentUUID) || err != nil || !exec.Contains(f) {
			return false
		}
	}
	return true
}

// checkWatermarks is WatermarkRegressed: once a run's Publishing record
// committed and status carried its watermark, status must keep containing
// it until a later promotion record.
func (m *plannedMonitor) checkWatermarks(obj *v1alpha1.MysqlFailoverGroup) {
	for _, id := range m.order {
		r := m.runs[id]
		if !r.published || r.watermarkFlags {
			continue
		}
		if lf := obj.Status.LastFailoverTime(); lf != nil && !r.confirmedAt.IsZero() &&
			lf.UTC().Truncate(time.Second).After(r.confirmedAt) {
			r.watermarkFlags = true // a later promotion record supersedes it
			continue
		}
		have := obj.Status.PromotionGtidExecuted()
		w, err := mysql.ParseGTIDSet(r.fenceSet)
		if err != nil {
			continue
		}
		h, err := mysql.ParseGTIDSet(have)
		contains := err == nil && w.Subtract(h).IsEmpty()
		// The status record carrying the run's own promotion (lastFailover
		// at its confirmation) must carry its watermark from the first
		// write on (K038), not only keep one it once showed.
		ownRecord := !r.confirmedAt.IsZero() && obj.Status.LastFailoverTime() != nil &&
			obj.Status.LastFailoverTime().UTC().Truncate(time.Second).Equal(r.confirmedAt)
		switch {
		case contains:
			r.watermarkSeen = true
		case r.watermarkSeen:
			r.watermarkFlags = true
			m.violate("WatermarkRegressed", r.id, fmt.Sprintf(
				"status promotionGtidExecuted %q no longer contains run %s's watermark %s", have, r.id, r.fenceSet))
		case ownRecord:
			r.watermarkFlags = true
			m.violate("WatermarkRegressed", r.id, fmt.Sprintf(
				"status records run %s's promotion (lastFailover %s) with promotionGtidExecuted %q, which lacks its watermark %s",
				r.id, r.confirmedAt.Format(time.RFC3339), have, r.fenceSet))
		}
	}
}

// noteDeferral records a "planned failover in progress" cross-site
// deferral (AutomationHeldByPendingRequest).
func (m *plannedMonitor) noteDeferral() {
	m.stats.Deferrals++
	if m.claim == nil {
		return // only the slot can say a deferral had nothing behind it
	}
	if _, _, ok := m.claim.Held(); !ok && !holdStands(m.cur) {
		m.violate("AutomationHeldByPendingRequest", fmt.Sprintf("p%d", m.poll),
			"a cross-site action was deferred for a planned failover while the claim slot was empty")
	}
}

// noteRestart is an operator restart: a new process, with no fence memo.
func (m *plannedMonitor) noteRestart() { m.procGen++ }

// noteReplace is a manager replacement.
func (m *plannedMonitor) noteReplace() {
	if m.claimHeld() {
		m.stats.ReplacedWhileHeld++
	}
}

// noteFault records the first store fault's phase.
func (m *plannedMonitor) noteFault() {
	if m.stats.FirstFaultPhase != "" {
		return
	}
	phase := "none"
	if m.cur != nil && m.cur.Status.PlannedFailover != nil {
		phase = string(m.cur.Status.PlannedFailover.Phase)
	}
	m.stats.FirstFaultPhase = phase
}

// noteEnginePass closes the pass's gate windows and ages Run-mode claims
// whose run is over (ZombieClaim).
func (m *plannedMonitor) noteEnginePass() {
	for _, id := range m.order {
		m.runs[id].closeGate()
	}
	if m.claim == nil {
		return
	}
	id, hold, ok := m.claim.Held()
	if !ok || hold {
		return
	}
	r := m.runs[id]
	over := r == nil || r.terminal || r.marked ||
		m.cur == nil || m.cur.Status.PlannedFailover == nil || m.cur.Status.PlannedFailover.RunID != id
	if !over {
		return
	}
	if r == nil {
		r = &runObs{id: id, fenceGen: -1, unconfirmedSince: -1, probeSeq: -1, roClearSeq: -1}
		m.runs[id] = r
		m.order = append(m.order, id)
	}
	r.zombieFor++
	if r.zombieFor > zombiePasses {
		m.violate("ZombieClaim", id, fmt.Sprintf("the slot still holds run %s %d engine passes after its run ended", id, r.zombieFor))
	}
}

// endPoll runs the per-poll checks. managerReady is whether a manager is
// registered and has been built since the last restart.
func (m *plannedMonitor) endPoll(p int, managerReady bool) {
	m.poll = p
	if m.claim != nil {
		for _, rel := range m.claim.Releases() {
			if !slices.Contains(engineReleaseReasons, rel.Why) {
				m.violate("ClaimRevoked", rel.RunID+"|"+string(rel.Why), fmt.Sprintf(
					"run %s's claim was released with %q, which is not an engine release", rel.RunID, rel.Why))
			}
			// Only the engine releases (K177): an approved reason from any
			// other actor is a revocation all the same.
			if rel.By != "" && rel.By != string(ActorPF) {
				m.violate("ClaimRevoked", rel.RunID+"|by|"+rel.By, fmt.Sprintf(
					"run %s's claim was released (%q) while %s was running, not the planned-failover engine", rel.RunID, rel.Why, rel.By))
			}
			if r := m.runs[rel.RunID]; r != nil && rel.Why == controller.ReleasePublishing {
				r.tailReleased, r.tailReleasedGen = true, m.procGen
			}
		}
		// A Promoting block whose own failover record is durable released
		// its claim for good (O-m1): it is never hydrated back.
		farFuture := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
		if managerReady && ownsSites(m.cur) && controller.PlannedRunNeedsClaim(m.cur, farFuture) {
			id := m.cur.Status.PlannedFailover.RunID
			tail := m.cur.Status.PlannedFailover.Phase == v1alpha1.PlannedFailoverPhasePromoting && m.runs[id] != nil && m.runs[id].tailReleased &&
				m.runs[id].tailReleasedGen == m.procGen
			if held, _, ok := m.claim.Held(); !tail && (!ok || held != id) {
				m.violate("ClaimRevoked", id+"|slot", fmt.Sprintf(
					"run %s durably owns the sites but the live slot holds %q", id, held))
			}
		}
	}
	truth := m.c.Truth()
	active := ""
	if managerReady && !m.c.OperatorDead() {
		active = m.managerActive(truth)
	}
	for _, id := range m.order {
		r := m.runs[id]
		if r.terminal && r.outcome == v1alpha1.PlannedFailoverOutcomeSourceLeftFenced && !r.healed {
			for _, t := range truth {
				if t.Name == r.source && t.Writable() {
					r.healed = true
				}
			}
			// Authority moves on only when the manager adopts another
			// site. A site that is merely writable for a while (a reader
			// an external writer opened, which the manager fences and
			// never adopts) leaves the source the group's only named
			// primary, and the run's fence still has to be healed.
			if active != "" && active != r.source {
				r.superseded = true
			}
		}
		if r.unconfirmedSince < 0 {
			continue
		}
		writable := false
		var src, tgt SiteTruth
		for _, t := range truth {
			if t.Writable() {
				writable = true
			}
			switch t.Name {
			case r.source:
				src = t
			case r.target:
				tgt = t
			}
		}
		fault := r.unconfirmedFault
		r.unconfirmedFault = false
		// The rule: while the unconfirmed target stays unreachable, a live
		// operator promotes the reachable, complete source within the
		// grace. It is judged over a window in which those preconditions
		// held at every poll.
		switch {
		case src.Writable() || active != "":
			// The source was promoted, or the manager counts a site as the
			// group's primary: there is no emergency left for the
			// unconfirmed run to block. What becomes of that primary is
			// the other invariants' to judge.
			r.unconfirmedSince = -1
		case writable, tgt.Reachable():
			// A site is writable that the manager has not adopted (a
			// reader an external writer opened; the manager fences it and
			// promotes nothing beside it), or the target answers: no
			// emergency promotion is due at this poll. Neither ends the
			// judgment: once that site is fenced again, or the target is
			// gone again, the operator owes the promotion within a new
			// grace.
			r.unconfirmedSince, r.unconfirmedGen = p, m.procGen
		case m.c.OperatorDead() || !managerReady || r.unconfirmedGen != m.procGen || fault ||
			src.Name == "" || !src.Reachable() || (r.fenceSet != "" && !containsSet(src, r.fenceSet)):
			// No operator ran this poll, a new process starts its debounce
			// over, an injected fault failed the promotion (it retries
			// after a backoff), or the source cannot be promoted: the
			// window starts again.
			r.unconfirmedSince, r.unconfirmedGen = p, m.procGen
		case p-r.unconfirmedSince > unconfirmedGrace:
			m.violate("UnconfirmedBlocksEmergency", id, fmt.Sprintf(
				"run %s ended PromotedUnconfirmed with its target unreachable, and %d polls later no site is writable although source %s is reachable and complete",
				id, p-r.unconfirmedSince, r.source))
		}
	}
}

// checkEnd runs the end-of-trial invariants (after heal and settle).
func (m *plannedMonitor) checkEnd(endPoll int) []Violation {
	m.poll = endPoll
	truth := m.c.Truth()
	allReachable := true
	anyWritable := false
	byName := map[string]SiteTruth{}
	for _, t := range truth {
		byName[t.Name] = t
		if !t.Reachable() {
			allReachable = false
		}
		if t.Writable() {
			anyWritable = true
		}
	}
	lastTarget := ""
	if m.cur != nil {
		lastTarget = m.cur.Status.LastFailoverTarget()
	}
	ids := append([]string(nil), m.order...)
	sort.Strings(ids)
	for _, id := range ids {
		r := m.runs[id]
		if r.outcome == v1alpha1.PlannedFailoverOutcomeSourceLeftFenced && !r.healed {
			// Judged only while the source still carries the run's own
			// fence: a restart since (a new incarnation) replaced it, and a
			// fence that was healed once and came back is not the run's.
			src := byName[r.source]
			// The monitor sees every process's pf fences, so "the run's own
			// fence" is one that landed, in the source's current
			// incarnation, and is still set (not mysqld's read_only boot).
			attributable := r.fenceGen >= 0 && src.Incarnation == r.fenceInc && src.SuperReadOnly
			// The re-assert's own preconditions (as WedgedNoPrimary): every
			// peer GTID-contained in the source; a divergence needs a human.
			contained := true
			for _, t := range truth {
				if t.Name != r.source && !src.Executed.contains(t.Executed) {
					contained = false
				}
			}
			// Once the topology manager adopted another site after the run
			// ended (or fenced or repointed the source itself), its normal
			// paths own the source (it demotes it to a replica of that site
			// under a fence of its own): what became of that primary is not
			// this run's fence wedging the group. A site that was only
			// writable for a while without being adopted does not supersede
			// the run (runObs.superseded).
			if attributable && !r.superseded && contained && allReachable && (lastTarget == "" || lastTarget == r.source) && !anyWritable {
				m.violate("FencedSourceNeverHealed", id, fmt.Sprintf(
					"run %s left its own fence on %s and after settle no site is writable", id, r.source))
			}
		}
		if !r.terminal && !r.marked {
			m.violate("PlannedRunStuck", id, fmt.Sprintf("run %s is not terminal after heal and settle", id))
		}
	}
	// A request annotation nobody consumed after heal and settle never
	// produced a run, so no run entry can report it (K182).
	if m.cur != nil {
		if v, ok := m.cur.GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
			m.violate("PlannedRunStuck", "request", fmt.Sprintf(
				"the planned-failover request %q is still unconsumed after heal and settle", v))
		}
	}
	if m.claim != nil {
		if id, hold, ok := m.claim.Held(); ok && (!hold || !holdStands(m.cur)) {
			m.violate("PlannedRunStuck", "slot", fmt.Sprintf(
				"the slot still holds run %s (hold=%v) after heal and settle without a standing hold record", id, hold))
		}
	}
	return m.violations
}

// signature is the monitor's coarse signature dimensions.
func (m *plannedMonitor) signature() string {
	outcomes := map[string]bool{}
	for _, r := range m.runs {
		o := string(r.outcome)
		if !r.terminal {
			o = "open"
			if r.marked {
				o = "marked"
			}
		}
		outcomes[o] = true
	}
	keys := make([]string, 0, len(outcomes))
	for k := range outcomes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprintf("pf=%s;firstFault=%s;replacedHeld=%v;",
		strings.Join(keys, ","), m.stats.FirstFaultPhase, m.stats.ReplacedWhileHeld > 0)
}
