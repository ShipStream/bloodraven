package dst

import (
	"fmt"
	"sort"
	"strings"

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

// promotionTarget returns the site a failover-style promotion landed on this
// poll, if any. The signature is RESET REPLICA ALL plus SET read_only=OFF
// taking effect on the same site in the same poll — that is
// FailoverController.Execute's fingerprint, and distinguishes a promotion
// from a primary re-assert (which never resets replication metadata).
func promotionTarget(events []Event) (string, int) {
	reset := map[string]bool{}
	for _, e := range events {
		if e.Kind == EvResetReplica && effectiveOutcome(e.Outcome) {
			reset[e.Site] = true
		}
	}
	for _, e := range events {
		if e.Kind == EvSetRO && e.Detail == "on=false" && effectiveOutcome(e.Outcome) && reset[e.Site] {
			return e.Site, e.Seq
		}
	}
	return "", 0
}

// lastKnownTarget is the operator's current in-memory failover target: set by
// an observed promotion, reset to the persisted CR value across a restart.
func (r *trialRunner) lastKnownTarget() string {
	if r.currentTarget != "" {
		return r.currentTarget
	}
	if r.persisted != nil {
		return r.persisted.LastFailoverTarget
	}
	return ""
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

	// Promotion detection + associated invariants.
	if target, grantSeq := promotionTarget(events); target != "" {
		r.promotionPolls = append(r.promotionPolls, p)
		r.currentTarget = target
		// The clock has not advanced for this poll yet, so Now() is the
		// instant the operator itself stamped on the promotion. A restart
		// that rehydrates anything older lost the record.
		r.lastPromotionAt = r.clk.Now()
		r.statusDroppedPromotion = false
		r.promotionRestarts = len(r.restartPolls)

		// I: the operator must never promote while it observes another
		// writable site, unless it fenced that site in the same cycle.
		// "Fenced" deliberately counts an ATTEMPT with any outcome: fencing
		// is best-effort by design (the old primary may be unreachable, and
		// the priorities path promotes its winner even when a loser fence
		// errors), so requiring an effective fence would flag by-design
		// behavior. A failed fence that leaves two writable sites is still
		// caught — by DualWritableUnresolved when it persists.
		for _, s := range st.Sites {
			if s.Name == target || s.State != "writable" {
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

		// I: fencing of other sites happens before the writable grant.
		for _, e := range events {
			if e.Site != target && e.Kind == EvSetSuperRO && e.Detail == "on=true" && e.Seq > grantSeq {
				r.violations = append(r.violations, Violation{
					Invariant: "FenceAfterPromote",
					Poll:      p,
					Detail:    fmt.Sprintf("fence of %s (seq %d) after writable grant on %s (seq %d)", e.Site, e.Seq, target, grantSeq),
				})
			}
		}
	}

	// I: a dual-writable state that is fully observable to the operator must
	// be resolved within the grace window (when a resolution policy exists).
	truth := r.cluster.Truth()
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
	if writable >= 2 && allReachable && r.splitBrainResolvable(truth) {
		r.dualStreak++
		if r.dualStreak > dualWritableGrace && !r.dualFlagged {
			r.dualFlagged = true
			names := []string{}
			for _, t := range truth {
				if t.Writable() {
					names = append(names, t.Name)
				}
			}
			r.violations = append(r.violations, Violation{
				Invariant: "DualWritableUnresolved",
				Poll:      p,
				Detail:    fmt.Sprintf("%v writable and operator-reachable for >%d polls without being fenced", names, dualWritableGrace),
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
	for i := 1; i < len(r.promotionPolls); i++ {
		gap := r.promotionPolls[i] - r.promotionPolls[i-1]
		if gap < cooldownPolls {
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
			out = append(out, Violation{
				Invariant: "CooldownViolated",
				Poll:      r.promotionPolls[i],
				Detail: fmt.Sprintf("promotions %d polls apart (cooldown %d polls, operator restarts between: %d, of which lost the durable anti-flap record: %d)",
					gap, cooldownPolls, restarts, lost),
			})
		}
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
	return []Violation{{
		Invariant: "WedgedNoPrimary",
		Poll:      r.trial.Polls - 1,
		Detail: fmt.Sprintf("no writable site at end; target %s reachable, read-only, GTID-complete; re-assert should have restored it",
			target),
	}}
}
