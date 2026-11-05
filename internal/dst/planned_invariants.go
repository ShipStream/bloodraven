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

// claimRelease is one slot release (the slot's claimReleaseNote).
type claimRelease struct {
	RunID string
	Why   controller.ClaimReleaseReason
}

// engineReleaseReasons is every reason the engine may release with (§3.6).
var engineReleaseReasons = []controller.ClaimReleaseReason{
	controller.ReleaseNotCommitted, controller.ReleaseAdmissionTimeout, controller.ReleasePublishing,
	controller.ReleaseTerminal, controller.ReleaseToHold, controller.ReleaseAck, controller.ReleaseZombie,
	controller.ReleaseManagerAbsent, controller.ReleaseStatusStalled,
}

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
	// intentUUID is the durable Fencing intent's sourceServerUUID; fenceSet
	// is fence.gtidSet (F).
	intentUUID string
	fenceSet   string

	targetWritable bool // the run made its target writable
	resets         int  // RESET REPLICA ALL on the target
	roClearTried   bool // a read_only=OFF was sent to the target
	promotionStmt  bool // any promotion statement was sent to the target
	pfMutations    int  // every pf mutation attempt for the run

	marked   bool
	terminal bool
	outcome  v1alpha1.PlannedFailoverOutcome

	// watermark tracking (WatermarkRegressed).
	published      bool
	confirmedAt    time.Time
	watermarkSeen  bool
	watermarkFlags bool

	// gateWritable is the truth-writable non-target set at the last
	// pre-write gate probe, nil when no gate probe is open; gateOpen marks
	// the open window (for RogueRaceResidual).
	gateWritable []string
	gateOpen     bool
	confirmSet   []string
	confirmSeen  bool

	unconfirmedSince int // poll of a PromotedUnconfirmed with an unreachable target; -1 none
	zombieFor        int // engine passes the slot has held this terminal run
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
}

func newPlannedMonitor(c *Cluster, sites []string, roles []state.SiteRole) *plannedMonitor {
	m := &plannedMonitor{
		c:       c,
		roles:   map[string]state.SiteRole{},
		sites:   append([]string(nil), sites...),
		runs:    map[string]*runObs{},
		flagged: map[string]bool{},
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

// ownsSites mirrors the controller's plannedFailoverOwnsSites (unexported).
func ownsSites(obj *v1alpha1.MysqlFailoverGroup) bool {
	if obj == nil || obj.Status.PlannedFailover == nil {
		return false
	}
	pf := obj.Status.PlannedFailover
	if pf.RunID == "" || markerRunID(obj.GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]) == pf.RunID {
		return false
	}
	switch pf.Phase {
	case v1alpha1.PlannedFailoverPhaseFencing, v1alpha1.PlannedFailoverPhaseDraining,
		v1alpha1.PlannedFailoverPhaseWaitingForLag, v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly, v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhaseRollingBack:
		return true
	}
	return false
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
// exclude: anything but fencing a non-promotable reader.
func (m *plannedMonitor) crossSite(e Event) bool {
	switch e.Kind {
	case EvPromote, EvResetReplica, EvStopReplica, EvStartReplica, EvChangeSource, EvSetRO, EvDrain, EvKillConns:
		return true
	case EvSetSuperRO:
		return e.Detail != "on=true" || m.roles[e.Site] != state.SiteRoleReadOnly
	}
	return false
}

// autoMutation reports whether e changes a promotable site's writability
// (HoldNotHonoured).
func (m *plannedMonitor) autoMutation(e Event) bool {
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
	if e.Kind == EvRogueWritable {
		for _, r := range m.runs {
			if r.gateOpen {
				m.stats.RogueRaceResidual++
			}
		}
		return
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
		r.roClearTried = true
		r.promotionStmt = true
	case e.Kind == EvStopReplica, e.Kind == EvResetReplica, isUnfence(e):
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
		if actor == ActorTM && m.autoMutation(e) && holdStands(m.cur) {
			m.violate("HoldNotHonoured", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"%s %s %s by the topology manager while the automation hold stands", e.Site, e.Kind, e.Detail))
		}
	}
}

func (m *plannedMonitor) onPFMutation(e Event, before, after SiteTruth) {
	r := m.activeRun()
	if r == nil {
		return
	}
	if r.marked || markerRunID(m.cur.GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]) == r.id {
		m.violate("MarkedRunResumed", r.id, fmt.Sprintf("run %s sent %s %s to %s after its terminal marker",
			r.id, e.Kind, e.Detail, e.Site))
	}

	// PlannedFenceWithoutAuthority: just before the fence, S must have been
	// the unique writable site.
	if e.Kind == EvSetSuperRO && e.Detail == "on=true" {
		others := m.writableExceptLocked(e.Site)
		if !before.Writable() || len(others) > 0 {
			m.violate("PlannedFenceWithoutAuthority", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"run %s fenced %s (writable before: %v; other writable sites: %v)", r.id, e.Site, before.Writable(), others))
		}
		if e.Site == r.source {
			r.fenceGen = m.procGen
		}
	}

	if isUnfence(e) && e.Site == r.source {
		if others := m.writableExceptLocked(e.Site); len(others) > 0 {
			m.violate("UnfenceBesideWritable", fmt.Sprintf("%d", e.Seq), fmt.Sprintf(
				"run %s unfenced source %s while %v writable", r.id, e.Site, others))
		}
		byMemo := r.fenceGen == m.procGen
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
	if isUnfence(e) && r.gateWritable != nil {
		if len(r.gateWritable) > 0 {
			m.violate("PlannedPromoteBesideWritable", r.id, fmt.Sprintf(
				"run %s cleared read-only on %s although %v were writable at the pre-write gate's probe",
				r.id, e.Site, r.gateWritable))
		}
		r.gateWritable = nil
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
	case EvSetRO:
		if e.Detail == "on=false" && after.Writable() {
			r.targetWritable = true
		}
	}
}

// onYield observes yield points: the gate and confirm probes record truth.
func (m *plannedMonitor) onYield(point string) {
	class := yieldClass(point)
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
	if others == nil {
		others = []string{}
	}
	if class == YieldGateProbe {
		r.gateWritable = others
		r.gateOpen = true
		return
	}
	r.confirmSet = others
	r.confirmSeen = true
}

// onCommit observes every committed object change.
func (m *plannedMonitor) onCommit(_ string, before, after *v1alpha1.MysqlFailoverGroup) {
	m.cur = after
	pf := after.Status.PlannedFailover
	if pf == nil || pf.RunID == "" {
		m.checkWatermarks(after)
		return
	}
	r := m.runs[pf.RunID]
	if r == nil {
		r = &runObs{id: pf.RunID, fenceGen: -1, unconfirmedSince: -1}
		m.runs[pf.RunID] = r
		m.order = append(m.order, pf.RunID)
	}
	r.target = pf.Target
	if pf.Source != "" {
		r.source = pf.Source
	}
	if pf.Fence != nil {
		r.intentUUID = pf.Fence.SourceServerUUID
		if pf.Fence.GtidSet != nil {
			r.fenceSet = *pf.Fence.GtidSet
		}
	}
	if markerRunID(after.GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]) == r.id {
		r.marked = true
	}
	if (pf.Phase == v1alpha1.PlannedFailoverPhasePublishing || pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded) &&
		!r.published && r.fenceSet != "" {
		r.published = true
		if pf.Promotion != nil && pf.Promotion.ConfirmedAt != nil {
			r.confirmedAt = pf.Promotion.ConfirmedAt.UTC().Truncate(time.Second)
		}
	}
	if pf.Result != nil && !r.terminal {
		r.terminal = true
		r.outcome = pf.Result.Outcome
		m.checkTerminal(r)
	}
	if !holdConditionTrue(before) && holdConditionTrue(after) {
		m.checkHoldAtRisk(r)
	}
	m.checkWatermarks(after)
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
		if r.roClearTried {
			dishonest("it sent read_only=OFF to the target")
		}
		if r.source != "" && !src.Writable() {
			dishonest("the source " + r.source + " is not writable")
		}
		if r.fenceSet != "" {
			if extra, err := ownExtras(tgt, r.fenceSet); err == nil && !extra.IsEmpty() {
				dishonest(fmt.Sprintf("the target holds own-UUID extras %s", extra))
			}
		}
	case v1alpha1.PlannedFailoverOutcomeSourceLeftFenced:
		if r.source != "" && src.Writable() {
			dishonest("the source " + r.source + " is writable")
		}
	case v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed:
		if !r.roClearTried {
			dishonest("it never sent read_only=OFF")
		}
		if !tgt.Reachable() {
			r.unconfirmedSince = m.poll
		}
	case v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired:
		if !r.promotionStmt {
			dishonest("it sent no promotion statement")
		}
	case v1alpha1.PlannedFailoverOutcomeSucceeded:
		if len(writable) != 1 || writable[0] != r.target {
			dishonest(fmt.Sprintf("the writable sites are %v", writable))
		}
		if r.fenceSet != "" && !containsSet(tgt, r.fenceSet) {
			dishonest("the target lacks fenced transactions " + r.fenceSet)
		}
		others := r.confirmSet
		if !r.confirmSeen {
			others = nil
			for _, w := range writable {
				if w != r.target {
					others = append(others, w)
				}
			}
		}
		if len(others) > 0 {
			m.violate("SucceededBesideWritable", r.id, fmt.Sprintf("run %s Succeeded while %v were writable", r.id, others))
		}
	}
}

// checkHoldAtRisk is HoldWithoutDataAtRisk: a hold needs two writable
// sites, or a site holding transactions outside F.
func (m *plannedMonitor) checkHoldAtRisk(r *runObs) {
	if r.fenceSet == "" {
		return // F unknown: the rule cannot be judged
	}
	f, err := mysql.ParseGTIDSet(r.fenceSet)
	if err != nil {
		return
	}
	writable := 0
	for _, t := range m.c.Truth() {
		if t.Writable() {
			writable++
		}
		if exec, err := mysql.ParseGTIDSet(t.Executed.String()); err == nil && !exec.Subtract(f).IsEmpty() {
			return
		}
	}
	if writable <= 1 {
		m.violate("HoldWithoutDataAtRisk", r.id, fmt.Sprintf(
			"run %s set the automation hold with %d writable site(s) and no site holding GTIDs outside F", r.id, writable))
	}
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
		if lf := obj.Status.LastFailover; lf != nil && !r.confirmedAt.IsZero() &&
			lf.UTC().Truncate(time.Second).After(r.confirmedAt) {
			r.watermarkFlags = true // a later promotion record supersedes it
			continue
		}
		have := obj.Status.PromotionGtidExecuted
		w, err := mysql.ParseGTIDSet(r.fenceSet)
		if err != nil {
			continue
		}
		h, err := mysql.ParseGTIDSet(have)
		contains := err == nil && w.Subtract(h).IsEmpty()
		switch {
		case contains:
			r.watermarkSeen = true
		case r.watermarkSeen:
			r.watermarkFlags = true
			m.violate("WatermarkRegressed", r.id, fmt.Sprintf(
				"status promotionGtidExecuted %q no longer contains run %s's watermark %s", have, r.id, r.fenceSet))
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

// noteEnginePass ages Run-mode claims whose run is over (ZombieClaim).
func (m *plannedMonitor) noteEnginePass() {
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
		r = &runObs{id: id, fenceGen: -1, unconfirmedSince: -1}
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
		}
		if managerReady && ownsSites(m.cur) {
			id := m.cur.Status.PlannedFailover.RunID
			if held, _, ok := m.claim.Held(); !ok || held != id {
				m.violate("ClaimRevoked", id+"|slot", fmt.Sprintf(
					"run %s durably owns the sites but the live slot holds %q", id, held))
			}
		}
	}
	truth := m.c.Truth()
	for _, id := range m.order {
		r := m.runs[id]
		if r.unconfirmedSince < 0 || p-r.unconfirmedSince <= unconfirmedGrace {
			continue
		}
		writable := false
		var src SiteTruth
		for _, t := range truth {
			if t.Writable() {
				writable = true
			}
			if t.Name == r.source {
				src = t
			}
		}
		switch {
		case writable:
			r.unconfirmedSince = -1
		case src.Name != "" && src.Reachable() && (r.fenceSet == "" || containsSet(src, r.fenceSet)):
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
		lastTarget = m.cur.Status.LastFailoverTarget
	}
	ids := append([]string(nil), m.order...)
	sort.Strings(ids)
	for _, id := range ids {
		r := m.runs[id]
		if r.outcome == v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
			attributable := r.fenceGen >= 0 || (r.intentUUID != "" && byName[r.source].ServerUUID == r.intentUUID)
			if attributable && allReachable && (lastTarget == "" || lastTarget == r.source) && !anyWritable {
				m.violate("FencedSourceNeverHealed", id, fmt.Sprintf(
					"run %s left its own fence on %s and after settle no site is writable", id, r.source))
			}
		}
		if !r.terminal && !r.marked {
			m.violate("PlannedRunStuck", id, fmt.Sprintf("run %s is not terminal after heal and settle", id))
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
