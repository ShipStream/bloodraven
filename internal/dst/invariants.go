package dst

import (
	"fmt"
	"sort"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// dualWritableGrace is how many consecutive polls a fully-observable
// dual-writable condition may persist before it counts as unresolved.
// Covers debounce (FailureThreshold + RecoveryThreshold) plus slack for a
// fence retry and confirmation.
const dualWritableGrace = 13

// effectiveOutcome reports whether an event's mutation took effect on the
// model ("" = clean apply, "ambiguous" = applied but the operator saw an
// error).
func effectiveOutcome(o string) bool { return o == "" || o == "ambiguous" }

// observedOutcome includes a statement that reached MySQL before the
// operator died. It is suitable for physical writable/fencing invariants,
// but not for anti-flap bookkeeping: the dead Execute path could not reach
// recordFailover after receiving this outcome.
func observedOutcome(o string) bool {
	return effectiveOutcome(o) || o == "appliedThenOperatorDied"
}

// promotionTarget returns the site a failover-style promotion landed on this
// poll, if any. The signature is RESET REPLICA ALL plus SET read_only=OFF
// taking effect on the same site in the same poll — that is
// FailoverController.Execute's fingerprint, and distinguishes a promotion
// from a primary re-assert (which never resets replication metadata).
func promotionTarget(events []Event, outcome func(string) bool) (string, int) {
	reset := map[string]bool{}
	for _, e := range events {
		if e.Kind == EvResetReplica && outcome(e.Outcome) {
			reset[e.Site] = true
		}
	}
	for _, e := range events {
		if e.Kind == EvSetRO && e.Detail == "on=false" && outcome(e.Outcome) && reset[e.Site] {
			return e.Site, e.Seq
		}
	}
	return "", 0
}

// lastKnownTarget is the operator's current in-memory failover target: set by
// an observed promotion, then rehydrated from the newer durable copy across a
// restart just like the production runner.
func (r *trialRunner) lastKnownTarget() string {
	if r.currentTarget != "" {
		return r.currentTarget
	}
	statusRecord := controller.FailoverRecord{}
	if r.persisted != nil {
		statusRecord = controller.FailoverRecord{
			LastFailover:       r.persisted.LastFailover,
			LastFailoverTarget: r.persisted.LastFailoverTarget,
		}
	}
	oobRecord := controller.FailoverRecord{}
	if r.persistedFailover != nil {
		oobRecord = *r.persistedFailover
	}
	return controller.NewerFailoverRecord(statusRecord, oobRecord).LastFailoverTarget
}

// splitBrainResolvable reports whether the operator has an automatic path out
// of the CURRENT dual-writable state, mirroring its real capability rather
// than mere configuration presence:
//
//   - history path: the failover target must itself be a live writable
//     authority (the operator refuses to fence everything else around a
//     non-writable target);
//   - priorities path: state.ResolveSplitBrain resolves only when a priority
//     entry names a currently-writable primary-candidate — priorities that
//     name only fenced/crashed sites are, by documented design, alert-only.
//
// Anything else is the documented manual-resolution state, not a violation.
func (r *trialRunner) splitBrainResolvable(truth []SiteTruth) bool {
	if r.plannedHoldStands() {
		// The automation hold (§10.5): a human resolves this split brain.
		return false
	}
	writableByName := map[string]bool{}
	var writableObs []state.SiteObservation
	for i, t := range truth {
		if !t.Writable() {
			continue
		}
		writableByName[t.Name] = true
		writableObs = append(writableObs, state.SiteObservation{
			Name: t.Name, Role: r.trial.Roles[i], State: state.StateWritable,
		})
	}
	if target := r.lastKnownTarget(); target != "" && writableByName[target] {
		return true
	}
	if len(r.trial.Priorities) > 0 {
		if winner, _ := state.ResolveSplitBrain(writableObs, r.trial.Priorities); winner != "" {
			return true
		}
	}
	return false
}

// checkPoll runs the per-poll invariants.
func (r *trialRunner) checkPoll(p int, st controller.StatusResponse, events []Event) {
	for _, e := range events {
		// Fault-injection events are INPUT shape, not observed behavior:
		// folding them into the signature multiplies the signature space by
		// the schedule space and the saturation rule can never go dry (they
		// re-entered the poll window when event stamping was fixed to open
		// the window before fault application).
		if strings.HasPrefix(string(e.Kind), "fault.") {
			continue
		}
		key := string(e.Kind)
		if e.Outcome != "" {
			key += ":" + e.Outcome
		}
		if e.Kind == EvFlushBinlog {
			// A rotation the server completes later is the model's doing,
			// like a fault; and why a rotation did not happen (failed,
			// abandoned, unreachable) is repro detail. One extra dimension
			// instead of four keeps the signature space coarse.
			if e.Outcome == "late" {
				continue
			}
			if e.Outcome != "" {
				key = string(e.Kind) + ":notRotated"
			}
		}
		// Which statement the operator died on is repro detail, not a
		// distinct behavior class. Left per-statement, the two death
		// outcomes would add one signature dimension per mutation kind and
		// multiply the space by 2^(2*kinds) — enough that the saturation
		// rule stops firing within any sane wall-clock budget. The event
		// log keeps the full detail; only the signature collapses.
		if e.Outcome == "operatorDied" || e.Outcome == "appliedThenOperatorDied" {
			key = "operatorDiedMidStatement:" + e.Outcome
		}
		r.kindsSeen[key] = struct{}{}
	}

	// Restart bookkeeping first: a restart at this poll means the operator's
	// in-memory target knowledge reverted to the CR before any decision ran.
	for _, rp := range r.restartPolls {
		if rp == p {
			r.currentTarget = ""
			r.dualStreak = 0
		}
	}

	// A promotion for anti-flap bookkeeping must have returned far enough for
	// Execute to reach recordFailover. A writable grant that applied just
	// before process death is still visible to the physical ordering
	// invariants below, but deliberately does not arm durable-state checks.
	physical := events
	if r.pl != nil {
		// With actors on, a planned run's or an ordered update's promotion
		// spans polls and is recorded later than it lands (the confirmed
		// tail; the update's record step): the pf and upd promotions are
		// detected across polls and armed for anti-flap bookkeeping when
		// recorded (armPromotion). The single-cycle physical checks below
		// are Execute's (the topology manager's) properties; the planned
		// run's have their own invariants (planned_invariants.go).
		for _, pr := range r.pl.promotions(events) {
			r.notePromotionPoll(p)
			r.promotionActors = append(r.promotionActors, pr.actor)
			r.promotionRuns = append(r.promotionRuns, pr.runID)
		}
		physical = tmEvents(events)
		if target, _ := promotionTarget(physical, effectiveOutcome); target != "" {
			r.notePromotionPoll(p)
			r.promotionActors = append(r.promotionActors, ActorTM)
			r.promotionRuns = append(r.promotionRuns, "")
			r.armPromotion(target, r.pollStartAt)
		}
	} else if target, _ := promotionTarget(events, effectiveOutcome); target != "" {
		r.notePromotionPoll(p)
		// The operator stamped the promotion during this poll, so at or
		// after the poll's start (a slow binary log rotation may have
		// advanced the clock inside the poll); every earlier promotion was
		// stamped before it. A restart that rehydrates anything older lost
		// the record.
		r.armPromotion(target, r.pollStartAt)
	}

	// Promotion detection for physical writable/fencing invariants includes
	// a grant that applied immediately before the operator died.
	if target, grantSeq := promotionTarget(physical, observedOutcome); target != "" {
		// I: the operator must never promote while it observes another
		// writable site, unless it fenced that site in the same cycle.
		// "Fenced" deliberately counts an ATTEMPT with any outcome: fencing
		// is best-effort by design (the old primary may be unreachable, and
		// the priorities path promotes its winner even when a loser fence
		// errors), so requiring an effective fence would flag by-design
		// behavior. A failed fence that leaves two writable sites is still
		// caught — by DualWritableUnresolved when it persists.
		for _, s := range st.Sites {
			if s.Name == target || s.State != v1alpha1.SiteStateWritable {
				continue
			}
			fenced := false
			for _, e := range events {
				if e.Site == s.Name && e.Kind == EvSetSuperRO && e.Detail == "on=true" {
					fenced = true
					break
				}
			}
			if !fenced {
				r.violations = append(r.violations, Violation{
					Invariant: "PromoteWhileObservedWritable",
					Poll:      p,
					Detail:    fmt.Sprintf("promoted %s while %s observed writable with no fence attempt", target, s.Name),
				})
			}
		}

		// I: fencing of other sites happens before the writable grant. A
		// re-fence of a site whose fence was already sent before the grant
		// (the split-brain path's "fencing returning old primary") is not
		// out of order. As above, the earlier fence counts as an attempt
		// with any outcome: the promotion fences best effort (an unreachable
		// old primary fails it) and the same poll retries after the grant.
		// Whether two sites then stay writable is DualWritableUnresolved's.
		fencedBefore := map[string]bool{}
		// A site an external writer made writable after the grant (a rogue
		// read_only=0, a pod booting writable) was read-only when the
		// promotion ran: fencing it afterwards is the operator reacting to
		// a new writer, not a fence the promotion owed before its grant.
		madeWritableAfter := map[string]int{}
		for _, e := range events {
			if e.Kind == EvSetSuperRO && e.Detail == "on=true" && e.Seq < grantSeq {
				fencedBefore[e.Site] = true
			}
			if e.Seq > grantSeq && (e.Kind == EvRogueWritable ||
				(e.Kind == EvRecoverSite && strings.Contains(e.Detail, "bootWritable=true"))) {
				if _, ok := madeWritableAfter[e.Site]; !ok {
					madeWritableAfter[e.Site] = e.Seq
				}
			}
		}
		for _, e := range events {
			if seq, ok := madeWritableAfter[e.Site]; ok && seq < e.Seq {
				continue
			}
			if e.Site != target && e.Kind == EvSetSuperRO && e.Detail == "on=true" && e.Seq > grantSeq && !fencedBefore[e.Site] {
				r.violations = append(r.violations, Violation{
					Invariant: "FenceAfterPromote",
					Poll:      p,
					Detail:    fmt.Sprintf("fence of %s (seq %d) after writable grant on %s (seq %d)", e.Site, e.Seq, target, grantSeq),
				})
			}
		}
	}

	// I: the operator rotates a site's binary log only while the site is
	// read-only by its own hand: a promotion candidate before its writes
	// are enabled, a source it has fenced. A rotation an actor sends to a
	// site after that actor enabled its writes is out of order (flushGrants
	// says how long a grant stands).
	r.violations = append(r.violations, r.flushGrants.observe(p, events)...)

	// I: a dual-writable state that is fully observable to the operator must
	// be resolved within the grace window (when a resolution policy exists).
	truth := r.cluster.Truth()
	r.flushGrants.settle(truth)
	writable := 0
	allReachable := true
	for _, t := range truth {
		if t.Writable() {
			writable++
			if !t.Reachable() {
				allReachable = false
			}
		}
	}
	// No registered manager (managerAbsent) is no operator, like a dead one.
	managed := r.pl == nil || r.pl.reg.Current() != nil
	// A run whose status writes are stalled keeps its claim, and with it
	// the manager's split-brain fence deferred, until the stall marker ends
	// it (§6.4): the documented bound on that deferral is the stall
	// deadline, so the streak counts only once the run outlives it.
	// So does a run that is rolling back: a rollback that cannot verify its
	// target back at super_read_only=ON retries with the claim until the
	// rollback deadline (§9), the documented bound of that deferral.
	stalled := r.pl != nil && (r.pl.stalledClaimWithinDeadline() || r.pl.rollbackClaimWithinDeadline())
	if !r.cluster.OperatorDead() && managed && !stalled && writable >= 2 && allReachable && r.splitBrainResolvable(truth) {
		r.dualStreak++
		if r.dualStreak > dualWritableGrace && !r.dualFlagged {
			r.dualFlagged = true
			names := []string{}
			for _, t := range truth {
				if t.Writable() {
					names = append(names, t.Name)
				}
			}
			detail := fmt.Sprintf("%v writable and operator-reachable for >%d polls without being fenced", names, dualWritableGrace)
			if r.pl != nil && r.pl.confirmedClaimHeld() {
				// The claim of a run that already made its target writable
				// is held until its Publishing record commits (§2.3), and
				// blocks the manager's split-brain fence meanwhile (DST-4).
				detail += " " + confirmedClaimMarker
			}
			r.violations = append(r.violations, Violation{
				Invariant: "DualWritableUnresolved",
				Poll:      p,
				Detail:    detail,
			})
		}
	} else {
		r.dualStreak = 0
	}
}

// checkEnd runs the end-of-trial invariants after the settle window.
func (r *trialRunner) checkEnd() []Violation {
	var out []Violation
	if r.cluster == nil || r.tm == nil {
		return out
	}
	truth := r.cluster.Truth()
	st := r.tm.Status()
	final := r.persisted

	byName := map[string]SiteTruth{}
	var writable []SiteTruth
	for _, t := range truth {
		byName[t.Name] = t
		if t.Writable() {
			writable = append(writable, t)
		}
	}

	// Cooldown spacing between observed promotions.
	//
	// The detail carries both the restart count and how many of those
	// restarts lost the anti-flap record, because the two combinations mean
	// very different things and the campaign fingerprints them apart:
	// losing the record while every durable path was up is a regression,
	// while losing it during an out-of-band store outage is the inherent
	// residue (both durable copies were unwritable, so nothing could have
	// carried the cooldown across the restart).
	cooldownPolls := r.trial.CooldownSec // 1s per poll
	cooldown := time.Duration(r.trial.CooldownSec) * time.Second
	for i := 1; i < len(r.promotionPolls); i++ {
		if i < len(r.promotionActors) && r.promotionActors[i] == ActorUpdate {
			// An ordered update is a deliberate maintenance handoff: it
			// stamps the cooldown for what follows but is not held by it.
			continue
		}
		if i-1 < len(r.promotionRuns) && r.promotionRuns[i-1] != "" && r.pl != nil && !r.pl.mon.runRecorded(r.promotionRuns[i-1]) {
			// A planned run that never recorded its promotion
			// (PromotedUnconfirmed, ManualRecoveryRequired, a rollback)
			// stamps no cooldown by rule (§10.1): emergency failover may
			// follow it, and an acknowledged hold hands the decision to
			// automation again.
			continue
		}
		gap := r.promotionPolls[i] - r.promotionPolls[i-1]
		// The operator's cooldown runs on the clock, from the stamp of one
		// promotion to the decision of the next. Both lie inside their
		// polls' windows, so the two are provably too close only when the
		// later window ends less than a cooldown after the earlier one
		// began. With no time taken inside a poll (no slow rotation) that
		// is the poll count.
		tooClose := gap < cooldownPolls
		if i < len(r.promotionSpans) {
			// (A runner built by hand with poll numbers only is judged on
			// those: one second per poll.)
			tooClose = r.promotionSpans[i].end.Sub(r.promotionSpans[i-1].start) < cooldown
		}
		if tooClose {
			restarts, lost := 0, 0
			for _, rp := range r.restartPolls {
				if rp > r.promotionPolls[i-1] && rp <= r.promotionPolls[i] {
					restarts++
				}
			}
			for _, lp := range r.lostStatePolls {
				if lp > r.promotionPolls[i-1] && lp <= r.promotionPolls[i] {
					lost++
				}
			}
			detail := fmt.Sprintf("promotions %d polls apart (cooldown %d polls, operator restarts between: %d, of which lost the durable anti-flap record: %d)",
				gap, cooldownPolls, restarts, lost)
			if r.retiredRecordPolls[r.promotionPolls[i-1]] {
				detail += " " + replacedBeforeRecordMarker
			}
			out = append(out, Violation{
				Invariant: "CooldownViolated",
				Poll:      r.promotionPolls[i],
				Detail:    detail,
			})
		}
	}

	if r.plannedHoldStands() {
		// A standing automation hold is the documented manual-recovery
		// state (§10.5): nothing automatic converges the group until a
		// human acknowledges it, and the planned invariants judge whether
		// the hold was warranted (HoldWithoutDataAtRisk). Convergence is
		// exempt; transaction accounting is not (K059).
		return append(out, r.checkHeldNoLoss(truth)...)
	}
	switch {
	case len(writable) >= 2:
		if r.splitBrainResolvable(truth) {
			names := []string{}
			for _, t := range writable {
				names = append(names, t.Name)
			}
			out = append(out, Violation{
				Invariant: "EndSplitBrain",
				Poll:      r.trial.Polls - 1,
				Detail:    fmt.Sprintf("trial ended with %v writable", names),
			})
		}

	case len(writable) == 1:
		p := writable[0]
		out = append(out, r.checkConverged(p, byName, st, final)...)
		out = append(out, r.checkNoSilentLoss(p, truth, final)...)

	case len(writable) == 0:
		out = append(out, r.checkWedge(byName)...)
	}
	return out
}

// checkConverged verifies every reachable non-primary site ended as a healthy
// replica of the final primary — or is explicitly reported as blocked.
func (r *trialRunner) checkConverged(p SiteTruth, byName map[string]SiteTruth, st controller.StatusResponse, final *controller.TopologySnapshot) []Violation {
	var out []Violation
	endPoll := r.trial.Polls - 1

	if st.ActiveSite != p.Name {
		out = append(out, Violation{
			Invariant: "StatusClaimMismatch",
			Poll:      endPoll,
			Detail:    fmt.Sprintf("truth primary %s but operator claims activeSite=%q", p.Name, st.ActiveSite),
		})
	}
	if _, site, ok := r.dns.Record(); !ok || site != p.Name {
		out = append(out, Violation{
			Invariant: "DNSStale",
			Poll:      endPoll,
			Detail:    fmt.Sprintf("truth primary %s but DNS points at %q", p.Name, site),
		})
	}

	// Iterate in declared site order: violation ordering must be
	// deterministic (map order is not).
	for _, name := range r.trial.SiteNames {
		t, ok := byName[name]
		if !ok || name == p.Name || !t.Reachable() {
			continue
		}
		healthy := !t.Writable() && t.ReplConfig &&
			canonicalHost(t.SourceHost) == canonicalHost(p.Name+".mysql.sim") &&
			t.IORunning && t.SQLRunning && p.Executed.contains(t.Executed) && t.Executed.contains(p.Executed)
		if healthy {
			continue
		}
		if r.reportedBlocked(name, t, p, final) {
			continue
		}
		out = append(out, Violation{
			Invariant: "NonConvergence",
			Poll:      endPoll,
			Detail: fmt.Sprintf("site %s not a healthy replica of %s at end and not reported blocked (ro=%v repl=%v src=%q io=%v sql=%v lastErr=%q exec=%s primary=%s)",
				name, p.Name, t.ReadOnly, t.ReplConfig, t.SourceHost, t.IORunning, t.SQLRunning, t.LastError, t.Executed, p.Executed),
		})
	}
	return out
}

// reportedBlocked reports whether the operator's final persisted status
// explains site's non-convergence: recovery blocked on divergence, or source
// convergence blocked on divergence.
func (r *trialRunner) reportedBlocked(name string, t SiteTruth, p SiteTruth, final *controller.TopologySnapshot) bool {
	if final == nil {
		return false
	}
	diverged := !p.Executed.contains(t.Executed)
	if !diverged {
		return false
	}
	for _, s := range final.Sites {
		if s.Name != name {
			continue
		}
		if s.RecoveryState == "RecoveryBlocked" || s.SourceConvergenceState == "Blocked" {
			return true
		}
	}
	return false
}

// checkNoSilentLoss verifies every acknowledged transaction is either on the
// final primary or explicitly reported (recovery divergence or blocked source
// convergence). Data on sites that never came back is inherent async-DR RPO
// and exempt.
func (r *trialRunner) checkNoSilentLoss(p SiteTruth, truth []SiteTruth, final *controller.TopologySnapshot) []Violation {
	var out []Violation
	endPoll := r.trial.Polls - 1

	acked, err := mysql.ParseGTIDSet(r.cluster.Acked().String())
	if err != nil {
		return []Violation{{Invariant: "ModelHole", Poll: endPoll, Detail: "acked set unparseable: " + err.Error()}}
	}
	primarySet, err := mysql.ParseGTIDSet(p.Executed.String())
	if err != nil {
		return []Violation{{Invariant: "ModelHole", Poll: endPoll, Detail: "primary set unparseable: " + err.Error()}}
	}

	reported := mysql.GTIDSet{}
	if final != nil {
		for _, snap := range final.Sites {
			if snap.RecoveryState == "RecoveryBlocked" && snap.DivergentGtid != "" {
				if g, err := mysql.ParseGTIDSet(snap.DivergentGtid); err == nil {
					for uuid, ivs := range g {
						reported[uuid] = append(reported[uuid], ivs...)
					}
				}
			}
			if snap.SourceConvergenceState != "Blocked" {
				continue
			}
			for _, t := range truth {
				if t.Name != snap.Name {
					continue
				}
				if extra, err := mysql.ParseGTIDSet(t.Executed.String()); err == nil {
					for uuid, ivs := range extra.Subtract(primarySet) {
						reported[uuid] = append(reported[uuid], ivs...)
					}
				}
			}
		}
	}

	missing := acked.Subtract(primarySet).Subtract(reported)
	if missing.IsEmpty() {
		return nil
	}

	// For each missing range, find who still holds it. Sorted UUID order
	// keeps violation ordering deterministic.
	uuids := make([]string, 0, len(missing))
	for uuid := range missing {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)
	for _, uuid := range uuids {
		for _, iv := range missing[uuid] {
			holderUp := false
			holderExists := false
			for _, t := range truth {
				if t.Executed[uuid] >= iv.End {
					holderExists = true
					if t.Reachable() {
						holderUp = true
					}
				}
			}
			switch {
			case holderUp:
				out = append(out, Violation{
					Invariant: "SilentDataLoss",
					Poll:      endPoll,
					Detail: fmt.Sprintf("acked txns %s:%d-%d absent from primary %s and unreported, but held by a reachable site",
						uuid, iv.Start, iv.End, p.Name),
				})
			case holderExists:
				// All holders down: async-DR RPO, acceptable.
			default:
				out = append(out, Violation{
					Invariant: "ModelHole",
					Poll:      endPoll,
					Detail:    fmt.Sprintf("acked txns %s:%d-%d exist nowhere in the model", uuid, iv.Start, iv.End),
				})
			}
		}
	}
	return out
}

// checkHeldNoLoss is checkNoSilentLoss under a standing automation hold.
// The hold leaves the histories it was set over for a human to reconcile:
// an acknowledged transaction on any site, reachable or not, is reported by
// the hold and not lost, whichever site is writable. One that exists on no
// site was destroyed while automation was held: the hold failed to
// preserve it.
func (r *trialRunner) checkHeldNoLoss(truth []SiteTruth) []Violation {
	endPoll := r.trial.Polls - 1
	acked, err := mysql.ParseGTIDSet(r.cluster.Acked().String())
	if err != nil {
		return []Violation{{Invariant: "ModelHole", Poll: endPoll, Detail: "acked set unparseable: " + err.Error()}}
	}
	missing := acked
	for _, t := range truth {
		have, err := mysql.ParseGTIDSet(t.Executed.String())
		if err != nil {
			return []Violation{{Invariant: "ModelHole", Poll: endPoll, Detail: "site set unparseable: " + err.Error()}}
		}
		missing = missing.Subtract(have)
	}
	if missing.IsEmpty() {
		return nil
	}
	return []Violation{{
		Invariant: "SilentDataLoss",
		Poll:      endPoll,
		Detail:    fmt.Sprintf("acked txns %s exist on no site at the end, under a standing automation hold", missing),
	}}
}

// checkWedge decides whether a zero-writable end state is a by-design manual
// intervention point or a wedge the operator was equipped to heal.
func (r *trialRunner) checkWedge(byName map[string]SiteTruth) []Violation {
	target := r.lastKnownTarget()
	if target == "" {
		return nil // no failover history: all-read-only needs a human, by design
	}
	t, ok := byName[target]
	if !ok || !t.Reachable() || t.Writable() {
		return nil
	}
	var role state.SiteRole
	for i, n := range r.trial.SiteNames {
		if n == target {
			role = r.trial.Roles[i]
		}
	}
	if role != state.SiteRolePrimaryCandidate {
		return nil
	}
	// Re-assert preconditions: every peer reachable, read-only, and GTID-
	// contained in the target. If they hold and the group still has no
	// primary, the operator failed to heal a wedge it is designed to heal.
	for name, peer := range byName {
		if name == target {
			continue
		}
		if r.coreSite(name) && (!peer.Reachable() || peer.Writable()) {
			return nil
		}
		if !t.Executed.contains(peer.Executed) {
			return nil // divergence: human review required, by design
		}
	}
	detail := fmt.Sprintf("no writable site at end; target %s reachable, read-only, GTID-complete; re-assert should have restored it",
		target)
	if r.replacedBeforeRecord {
		// The manager never learned the promotion that made target the
		// target (DST-1): it judges the wedge from an older record.
		detail += " " + replacedBeforeRecordMarker
	}
	return []Violation{{
		Invariant: "WedgedNoPrimary",
		Poll:      r.trial.Polls - 1,
		Detail:    detail,
	}}
}

// notePromotionPoll records a promotion detected in poll p with the poll's
// fake-clock window.
func (r *trialRunner) notePromotionPoll(p int) {
	r.promotionPolls = append(r.promotionPolls, p)
	r.promotionSpans = append(r.promotionSpans, clockSpan{start: r.pollStartAt, end: r.clk.Now()})
}

// armPromotion records a promotion that reached the operator's failover
// record at instant at: the in-memory target, and the anti-flap
// bookkeeping a later restart is judged against.
func (r *trialRunner) armPromotion(target string, at time.Time) {
	r.promotionArms++
	r.currentTarget = target
	r.lastPromotionAt = at
	r.statusDroppedPromotion = false
	r.replacedBeforeRecord = false
	r.promotionRestarts = len(r.restartPolls)
}

// armRecordedPromotion arms a planned run's promotion when its Publishing
// record commits, with the run's confirmation time. The record can commit
// long after the promotion (a status outage), and after a later promotion
// by another actor was armed. The operator's failover record is
// timestamp-monotonic (the run's terminal write and the snapshot merge both
// refuse to move it to an older failover), so the late record of an older
// promotion changes neither the operator's target nor what a restart must
// rehydrate, and it is not armed over the newer one.
func (r *trialRunner) armRecordedPromotion(target string, confirmedAt time.Time) {
	if confirmedAt.Before(r.lastPromotionAt) {
		return
	}
	r.armPromotion(target, confirmedAt)
}

// tmEvents is the events not issued by the planned or update actors.
func tmEvents(events []Event) []Event {
	out := make([]Event, 0, len(events))
	for _, e := range events {
		if a := effectiveActor(e.Actor); a != ActorPF && a != ActorUpdate {
			out = append(out, e)
		}
	}
	return out
}

// plannedHoldStands reports whether a planned-failover automation hold stands
// on the simulated object (trials with actors only).
func (r *trialRunner) plannedHoldStands() bool {
	return r.pl != nil && holdStands(r.pl.store.Object())
}

// confirmedClaimMarker tags a DualWritableUnresolved during which a planned
// run that had already made its target writable held the claim
// (fingerprint DualWritableUnresolved(confirmedClaim): the fixed DST-4, now a
// regression; README, Finding classes).
const confirmedClaimMarker = "(while a planned run that had made its target writable held the claim)"

// flushGrant is one actor's standing grant of writes to a site: the event
// that enabled them and the poll it landed in.
type flushGrant struct {
	seq, poll int
}

// flushGrants is the state of FlushAfterWriteGrant: for every actor and
// site, the first statement by which that actor enabled the site's writes
// (Promote, or a clear of read_only or super_read_only that reached the
// server) and that still stands. A grant stands until the site is fenced
// again by anyone (an operator actor, its sidecar, an external writer) or
// its server restarts or is replaced. The zero value is ready to use.
//
// Whether a grant outlives its poll depends on the actor (settle):
//
//   - A planned failover's and an ordered update's grants do, while the
//     site stays writable. Their handoffs span polls and engine passes, and
//     every rotation they send is to a site they hold under the claim and
//     have just fenced or are about to make writable. A rotation of a site
//     they made writable in an earlier poll, not fenced since, is out of
//     order.
//   - The topology manager's do not. Its promotion (FailoverController.
//     Execute) is one call inside one poll, which the per-poll rule judges
//     in full, and both rotations it sends to a site it made writable in an
//     earlier poll are by design: the split-brain priorities path fences
//     the losers and runs the whole promotion on the policy winner, which
//     is writable already (often by the manager's own re-assert), and the
//     pre-reclone rotation's recipient can be a former primary an operator
//     partition has kept writable. Carried across polls, the rule flagged
//     the priorities path and nothing else in the planned sweeps. On that
//     path the rotation lands on a writable site, so the file it opens
//     need not start at the recorded promotion GTID; nothing in a trial
//     writes inside a poll, so the model cannot show the difference.
type flushGrants struct {
	granted map[string]flushGrant // actor|site
}

// observe applies one poll's events in order and returns a
// FlushAfterWriteGrant violation for every FLUSH BINARY LOGS an actor sent
// to a site while its own grant stood. A rotation that failed or was
// abandoned counts like one that rotated: the statement was sent. A
// rotation the server completes after the operator abandoned it ("late") is
// the model's doing, not a statement the operator ordered there.
func (g *flushGrants) observe(p int, events []Event) []Violation {
	if g.granted == nil {
		g.granted = map[string]flushGrant{}
	}
	var out []Violation
	for _, e := range events {
		actor := effectiveActor(e.Actor)
		key := string(actor) + "|" + e.Site
		switch {
		case e.Kind == EvPromote && observedOutcome(e.Outcome),
			(e.Kind == EvSetRO || e.Kind == EvSetSuperRO) && e.Detail == "on=false" && observedOutcome(e.Outcome):
			if _, ok := g.granted[key]; !ok {
				g.granted[key] = flushGrant{seq: e.Seq, poll: p}
			}
		case (e.Kind == EvSetRO || e.Kind == EvSetSuperRO) && e.Detail == "on=true" && observedOutcome(e.Outcome),
			e.Kind == EvSidecarFence && observedOutcome(e.Outcome),
			e.Kind == EvWedgeFence, e.Kind == EvCrash, e.Kind == EvRecoverSite, e.Kind == EvReplacePod, e.Kind == EvRestartSite:
			g.revoke("", e.Site)
		case e.Kind == EvFlushBinlog && e.Outcome != "late":
			grant, ok := g.granted[key]
			if !ok {
				continue
			}
			detail := fmt.Sprintf("%s sent FLUSH BINARY LOGS to %s (seq %d) after enabling its writes (seq %d)", actor, e.Site, e.Seq, grant.seq)
			if grant.poll != p {
				detail += fmt.Sprintf(" at poll %d, and the site was not fenced or restarted since", grant.poll)
			}
			out = append(out, Violation{Invariant: "FlushAfterWriteGrant", Poll: p, Detail: detail})
		}
	}
	return out
}

// revoke ends the grants on site ("" for every site) of actor ("" for every
// actor).
func (g *flushGrants) revoke(actor Actor, site string) {
	for key := range g.granted {
		a, s, _ := strings.Cut(key, "|")
		if (actor == "" || a == string(actor)) && (site == "" || s == site) {
			delete(g.granted, key)
		}
	}
}

// settle ends, at a poll's end, the grants that do not outlive it: the
// topology manager's, and every grant on a site that is not writable (a
// clear of super_read_only that left read_only on, a fence the model
// applied without an event). A new process that finds such a target
// read-only may rotate it again before it clears again.
func (g *flushGrants) settle(truth []SiteTruth) {
	g.revoke(ActorTM, "")
	for _, t := range truth {
		if !t.Writable() {
			g.revoke("", t.Name)
		}
	}
}
