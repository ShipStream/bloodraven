package state

import (
	"fmt"
	"strings"
)

// SiteRole mirrors api/v1alpha1.SiteRole as a string. The pure state
// package avoids importing v1alpha1 to stay decoupled from CRD types.
type SiteRole string

const (
	// SiteRolePrimaryCandidate is a site that can be auto-promoted.
	SiteRolePrimaryCandidate SiteRole = "primary-candidate"
	// SiteRoleDROnly is a site that only ever follows the active primary.
	SiteRoleDROnly SiteRole = "dr-only"
	// SiteRoleReadOnly is a non-promotable serving reader whose health does
	// not contribute to failover-group readiness.
	SiteRoleReadOnly SiteRole = "read-only"
)

// SiteObservation is a snapshot of one site at a single poll cycle.
type SiteObservation struct {
	Name  string
	Role  SiteRole
	State SiteState
	// PromotionBlocked excludes a primary-candidate from auto-promotion
	// without changing its role. Used for mid-keyring-rotation sites and
	// for sites an in-place restore has not recloned yet: they stay in the
	// topology tallies but must not become the sole authoritative copy.
	// Default false keeps existing fixtures valid.
	PromotionBlocked bool
	// PromotionBlockedReason says why PromotionBlocked is set; it only
	// selects the alert wording. Empty means PromotionBlockedRotation.
	PromotionBlockedReason string
	// PendingWritable marks a StateUnknown site (not yet classified since the
	// operator started) whose latest successful probe answered writable. The
	// recovery debounce has not confirmed it, so it is never treated as the
	// primary, but no other site is promoted over it either. Ignored for any
	// other State.
	PendingWritable bool
	// ClassificationOverdue marks a StateUnknown site that has been probed
	// often enough to be classified had its probes answered or failed
	// consistently: its answers alternate with errors. It no longer turns
	// NoPrimary into Initializing, so a flapping site cannot hold the group
	// in Initializing indefinitely. PendingWritable still applies to it.
	// Ignored for any other State.
	ClassificationOverdue bool
}

// Reasons for SiteObservation.PromotionBlocked.
const (
	PromotionBlockedRotation       = "Rotation"
	PromotionBlockedPendingReclone = "PendingReclone"
	// PromotionBlockedOwnTransactionsLost: the site lacks transactions it
	// issued itself and would issue their GTIDs again.
	PromotionBlockedOwnTransactionsLost = "OwnTransactionsLost"
)

// CrossSiteAction describes the cross-site action implied by a poll
// cycle of observations.
type CrossSiteAction struct {
	// FenceSites lists writable non-promotable sites. These are anomalies,
	// never active primaries or split-brain winners.
	FenceSites []string
	// PromotionCandidates is the ordered list of primary-candidate
	// sites that are eligible to be promoted this cycle. The caller
	// must rank by GTID freshness as the primary selector (most-
	// caught-up replica wins) and use this order only as the
	// tiebreaker. The list is ordered by spec.splitBrainPolicy
	// .sitePriorities first, then by declared site order. Empty when
	// no promotion is warranted.
	PromotionCandidates []string
	// SplitBrain is true iff more than one site is currently writable.
	// Callers that want auto-resolution should consult ResolveSplitBrain.
	// Reason can be "SplitBrain" while this is false: a second site's
	// writable answer is reported before the debounce confirms it, but
	// nothing may act on it until then.
	SplitBrain bool
	// Alert, when non-empty, is a human-readable summary of a cross-site
	// condition (TotalLoss / SplitBrain / NoPrimary / SiteUnhealthy).
	Alert string
	// Reason is a short machine-readable reason tag suitable for metric
	// labels or CR condition reasons: "Healthy", "SiteUnhealthy",
	// "SplitBrain", "NoPrimary", "TotalLoss", or "Initializing" (no site is
	// confirmed writable and a primary-candidate is still unclassified;
	// never an alert).
	Reason string
}

// CrossSiteAction.Reason values. The operator serves them as the
// MysqlFailoverGroup Ready and Degraded condition reasons, so each must also
// be declared in api/v1alpha1 (internal/apiregistry pins that).
const (
	ReasonHealthy       = "Healthy"
	ReasonSiteUnhealthy = "SiteUnhealthy"
	ReasonSplitBrain    = "SplitBrain"
	ReasonNoPrimary     = "NoPrimary"
	ReasonTotalLoss     = "TotalLoss"
	ReasonInitializing  = "Initializing"
)

// EvalCrossSite evaluates an N-site topology and returns the cross-site
// action implied by the current observations.
//
// sitePriorities is an ordered list of primary-candidate site names
// that break promotion ties. It never overrides GTID freshness (that is
// the caller's job with fresh MySQL data); it only orders the candidate
// list so that the caller knows which site to prefer when two replicas
// have equivalent GTID sets.
//
// The function is pure: it never considers history or policy beyond the
// supplied priorities. Split-brain auto-resolution (which needs
// knowledge of prior failovers) is layered on top by the caller via
// ResolveSplitBrain.
//
// A primary-candidate in StateUnknown has not been classified since the
// operator started. While no site is confirmed writable, such a site turns
// NoPrimary into Initializing: NoPrimary claims every candidate was seen
// read-only, which is not known yet. Once it is ClassificationOverdue it no
// longer does, so a flapping site cannot hide NoPrimary indefinitely. A
// PendingWritable one, overdue or not, also holds back the failover row, so
// no site is promoted over a site that answered writable, and it counts
// toward an (unconfirmed) split brain.
func EvalCrossSite(observations []SiteObservation, sitePriorities []string) CrossSiteAction {
	var action CrossSiteAction

	var writable, readOnly, unreachable, unclassified, overdue, pending []SiteObservation
	coreCount := 0
	for _, obs := range observations {
		// Count configured core sites by role before state classification so an
		// Unknown observation remains part of the topology instead of making an
		// all-unknown startup look like an empty, healthy group.
		if obs.Role != SiteRoleReadOnly {
			coreCount++
		}
		if obs.State == StateWritable && obs.Role != SiteRolePrimaryCandidate {
			action.FenceSites = append(action.FenceSites, obs.Name)
			continue
		}
		if obs.Role == SiteRoleReadOnly {
			continue
		}
		switch obs.State {
		case StateWritable:
			writable = append(writable, obs)
		case StateReadOnly:
			readOnly = append(readOnly, obs)
		case StateUnreachable:
			unreachable = append(unreachable, obs)
		case StateUnknown:
			if obs.Role == SiteRolePrimaryCandidate {
				if obs.ClassificationOverdue {
					overdue = append(overdue, obs)
				} else {
					unclassified = append(unclassified, obs)
				}
				if obs.PendingWritable {
					pending = append(pending, obs)
				}
			}
		}
	}

	if coreCount == 0 {
		if len(action.FenceSites) > 0 {
			action.Alert = fmt.Sprintf("writable non-promotable site requires fencing (%s)", strings.Join(action.FenceSites, ", "))
			action.Reason = ReasonSiteUnhealthy
		} else {
			action.Reason = ReasonHealthy
		}
		return action
	}

	// Fencing is best-effort and must be confirmed by a later poll before any
	// promotion. Otherwise a failed fence and a successful promotion could
	// leave two sites accepting writes.
	if len(action.FenceSites) > 0 {
		action.Alert = fmt.Sprintf("writable non-promotable site requires fencing (%s)", strings.Join(action.FenceSites, ", "))
		action.Reason = ReasonSiteUnhealthy
		return action
	}

	// Total loss: every site is unreachable.
	if len(unreachable) == coreCount {
		action.Alert = "TOTAL LOSS: all sites are unreachable"
		action.Reason = ReasonTotalLoss
		return action
	}

	// Split-brain: more than one site is writable.
	if len(writable) > 1 {
		action.SplitBrain = true
		action.Alert = fmt.Sprintf("SPLIT BRAIN: %d sites are writable (%s)",
			len(writable), strings.Join(siteNames(writable), ", "))
		action.Reason = ReasonSplitBrain
		return action
	}

	// A writable answer the debounce has not confirmed is reported with the
	// confirmed ones, so a second writable site is never hidden behind the
	// debounce. SplitBrain stays false: fencing and split-brain resolution
	// wait for the confirmation.
	if len(writable)+len(pending) > 1 {
		answered := append(siteNames(writable), siteNames(pending)...)
		action.Alert = fmt.Sprintf("SPLIT BRAIN: %d sites are writable (%s); %s not yet confirmed",
			len(answered), strings.Join(answered, ", "), strings.Join(siteNames(pending), ", "))
		action.Reason = ReasonSplitBrain
		return action
	}

	// No writable site. Emit promotion candidates only when at least
	// one site is unreachable — i.e. the primary has clearly failed —
	// and no site has answered writable without confirmation yet.
	// Without any unreachable peer we refuse to auto-elect a primary
	// (all-read-only is a startup or recovery state that needs human
	// input).
	if len(writable) == 0 {
		if len(pending) == 0 && len(unreachable) > 0 && len(readOnly) > 0 {
			candidates := RankPromotionCandidates(readOnly, sitePriorities)
			if len(candidates) > 0 {
				action.PromotionCandidates = candidates
				action.Reason = ReasonSiteUnhealthy
				return action
			}
			rotating, pendingReclone, ownLost := blockedCandidateNames(readOnly)
			var refusals []string
			if len(rotating) > 0 {
				refusals = append(refusals, fmt.Sprintf(
					"refusing to promote %s (UnsealReason=Rotation); finish the rotation before this site can be promoted",
					strings.Join(rotating, ", ")))
			}
			if len(pendingReclone) > 0 {
				refusals = append(refusals, fmt.Sprintf(
					"refusing to promote %s (pending post-restore reclone; holds pre-restore data); "+
						"bring the primary back, or remove status.restoreReclones (kubectl patch --subresource=status) to accept that data",
					strings.Join(pendingReclone, ", ")))
			}
			if len(ownLost) > 0 {
				refusals = append(refusals, fmt.Sprintf(
					"refusing to promote %s (lacks transactions it issued itself; it would issue their GTIDs again); "+
						"bring the primary back so the site can be recloned from it",
					strings.Join(ownLost, ", ")))
			}
			if len(refusals) > 0 {
				action.Alert = "NO PRIMARY: " + strings.Join(refusals, "; ")
				action.Reason = ReasonNoPrimary
				return action
			}
		}
		if len(unclassified) > 0 {
			action.Reason = ReasonInitializing
			return action
		}
		// No eligible promotion target: alert only. Use a message that
		// matches the two-site convention when exactly two read-only
		// sites are observed, for test/UX familiarity.
		if len(overdue) > 0 {
			action.Alert = fmt.Sprintf("NO PRIMARY: no writable site available; %s not classified (probes alternate between answers and errors)",
				strings.Join(siteNames(overdue), ", "))
		} else if len(readOnly) == 2 && len(unreachable) == 0 {
			action.Alert = "NO PRIMARY: both sites are read-only"
		} else {
			action.Alert = "NO PRIMARY: no writable site available"
		}
		action.Reason = ReasonNoPrimary
		return action
	}

	// Exactly one writable. Healthy, unless some sites are unreachable.
	if len(unreachable) > 0 {
		action.Alert = fmt.Sprintf("%s unreachable while %s is primary",
			strings.Join(siteNames(unreachable), ", "), writable[0].Name)
		action.Reason = ReasonSiteUnhealthy
		return action
	}
	action.Reason = ReasonHealthy
	return action
}

// RankPromotionCandidates returns every primary-candidate site from
// obs, ordered first by appearance in sitePriorities and then by
// declared observation order. Non-primary-candidate sites are omitted.
// Used by callers that rank further by GTID freshness.
func RankPromotionCandidates(obs []SiteObservation, sitePriorities []string) []string {
	seen := make(map[string]struct{}, len(obs))
	out := make([]string, 0, len(obs))
	for _, name := range sitePriorities {
		for _, c := range obs {
			if c.Name == name && c.Role == SiteRolePrimaryCandidate && !c.PromotionBlocked {
				if _, dup := seen[name]; !dup {
					out = append(out, name)
					seen[name] = struct{}{}
				}
				break
			}
		}
	}
	for _, c := range obs {
		if c.Role != SiteRolePrimaryCandidate || c.PromotionBlocked {
			continue
		}
		if _, dup := seen[c.Name]; dup {
			continue
		}
		out = append(out, c.Name)
		seen[c.Name] = struct{}{}
	}
	return out
}

// blockedCandidateNames splits the promotion-blocked primary-candidates
// by reason.
func blockedCandidateNames(obs []SiteObservation) (rotating, pendingReclone, ownLost []string) {
	for _, c := range obs {
		if c.Role != SiteRolePrimaryCandidate || !c.PromotionBlocked {
			continue
		}
		switch c.PromotionBlockedReason {
		case PromotionBlockedPendingReclone:
			pendingReclone = append(pendingReclone, c.Name)
		case PromotionBlockedOwnTransactionsLost:
			ownLost = append(ownLost, c.Name)
		default:
			rotating = append(rotating, c.Name)
		}
	}
	return rotating, pendingReclone, ownLost
}

// ResolveSplitBrain picks the winner and losers from a writable set
// during split-brain auto-resolution. It never falls back to declared
// order: an empty sitePriorities list, or one whose entries name no
// currently-writable primary-candidate, returns ("", nil) so the
// operator alerts only instead of picking a winner by chance.
//
// When the list does name a match the winner is the earliest entry
// that is currently writable and primary-candidate; every other
// writable site becomes a loser that the caller should fence.
func ResolveSplitBrain(writable []SiteObservation, sitePriorities []string) (winner string, losers []string) {
	if len(sitePriorities) == 0 {
		return "", nil
	}
	for _, name := range sitePriorities {
		for _, c := range writable {
			if c.Name == name && c.Role == SiteRolePrimaryCandidate {
				winner = name
				break
			}
		}
		if winner != "" {
			break
		}
	}
	if winner == "" {
		return "", nil
	}
	for _, obs := range writable {
		if obs.Name != winner {
			losers = append(losers, obs.Name)
		}
	}
	return winner, losers
}

func siteNames(s []SiteObservation) []string {
	out := make([]string, len(s))
	for i, obs := range s {
		out[i] = obs.Name
	}
	return out
}
