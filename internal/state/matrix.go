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
	// without changing its role. Used for mid-keyring-rotation sites:
	// they stay in the topology tallies but must not become the sole
	// authoritative copy. Default false keeps existing fixtures valid.
	PromotionBlocked bool
}

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
	SplitBrain bool
	// Alert, when non-empty, is a human-readable summary of a cross-site
	// condition (TotalLoss / SplitBrain / NoPrimary / Degraded).
	Alert string
	// Reason is a short machine-readable reason tag suitable for metric
	// labels or CR condition reasons: "Healthy", "Degraded",
	// "SplitBrain", "NoPrimary", "TotalLoss".
	Reason string
}

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
func EvalCrossSite(observations []SiteObservation, sitePriorities []string) CrossSiteAction {
	var action CrossSiteAction

	var writable, readOnly, unreachable []SiteObservation
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
		}
	}

	if coreCount == 0 {
		if len(action.FenceSites) > 0 {
			action.Alert = fmt.Sprintf("writable non-promotable site requires fencing (%s)", strings.Join(action.FenceSites, ", "))
			action.Reason = "Degraded"
		} else {
			action.Reason = "Healthy"
		}
		return action
	}

	// Fencing is best-effort and must be confirmed by a later poll before any
	// promotion. Otherwise a failed fence and a successful promotion could
	// leave two sites accepting writes.
	if len(action.FenceSites) > 0 {
		action.Alert = fmt.Sprintf("writable non-promotable site requires fencing (%s)", strings.Join(action.FenceSites, ", "))
		action.Reason = "Degraded"
		return action
	}

	// Total loss: every site is unreachable.
	if len(unreachable) == coreCount {
		action.Alert = "TOTAL LOSS: all sites are unreachable"
		action.Reason = "TotalLoss"
		return action
	}

	// Split-brain: more than one site is writable.
	if len(writable) > 1 {
		action.SplitBrain = true
		action.Alert = fmt.Sprintf("SPLIT BRAIN: %d sites are writable (%s)",
			len(writable), strings.Join(siteNames(writable), ", "))
		action.Reason = "SplitBrain"
		return action
	}

	// No writable site. Emit promotion candidates only when at least
	// one site is unreachable — i.e. the primary has clearly failed.
	// Without any unreachable peer we refuse to auto-elect a primary
	// (all-read-only is a startup or recovery state that needs human
	// input).
	if len(writable) == 0 {
		if len(unreachable) > 0 && len(readOnly) > 0 {
			candidates := RankPromotionCandidates(readOnly, sitePriorities)
			if len(candidates) > 0 {
				action.PromotionCandidates = candidates
				action.Reason = "Degraded"
				return action
			}
			if blocked := rotationBlockedNames(readOnly); len(blocked) > 0 {
				action.Alert = fmt.Sprintf(
					"NO PRIMARY: refusing to promote %s (UnsealReason=Rotation); finish the rotation before this site can be promoted",
					strings.Join(blocked, ", "))
				action.Reason = "NoPrimary"
				return action
			}
		}
		// No eligible promotion target: alert only. Use a message that
		// matches the two-site convention when exactly two read-only
		// sites are observed, for test/UX familiarity.
		if len(readOnly) == 2 && len(unreachable) == 0 {
			action.Alert = "NO PRIMARY: both sites are read-only"
		} else {
			action.Alert = "NO PRIMARY: no writable site available"
		}
		action.Reason = "NoPrimary"
		return action
	}

	// Exactly one writable. Healthy, unless some sites are unreachable.
	if len(unreachable) > 0 {
		action.Alert = fmt.Sprintf("%s unreachable while %s is primary",
			strings.Join(siteNames(unreachable), ", "), writable[0].Name)
		action.Reason = "Degraded"
		return action
	}
	action.Reason = "Healthy"
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

func rotationBlockedNames(obs []SiteObservation) []string {
	var out []string
	for _, c := range obs {
		if c.Role == SiteRolePrimaryCandidate && c.PromotionBlocked {
			out = append(out, c.Name)
		}
	}
	return out
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
