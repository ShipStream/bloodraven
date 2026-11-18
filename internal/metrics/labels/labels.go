// Package labels produces the values of the operator's enum-valued metric
// labels. A label value is the lowercase kebab-case form of the API enum
// value (PrimaryCandidate is primary-candidate, ReadOnly is read-only),
// which is the Prometheus convention and keeps the documented queries
// stable. Every enum-valued label goes through the function for its enum
// here; no producer lowercases a value by hand, because strings.ToLower
// and the kebab-case form differ on every two-word value.
//
// It is a separate package from internal/metrics because it imports the
// API package, which the sidecar (an internal/metrics importer) does not.
package labels

import (
	"strings"
	"unicode"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
)

// kebab returns the lowercase kebab-case form of a CamelCase enum value. A
// run of capitals is one word (GTIDDiverged is gtid-diverged).
func kebab(v string) string {
	r := []rune(v)
	var b strings.Builder
	for i, c := range r {
		if unicode.IsUpper(c) && i > 0 {
			prev := r[i-1]
			nextLower := i+1 < len(r) && unicode.IsLower(r[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				b.WriteByte('-')
			}
		}
		b.WriteRune(unicode.ToLower(c))
	}
	return b.String()
}

// Role is the role label of the site-scoped gauges
// (bloodraven_site_state, bloodraven_replication_lag_seconds,
// bloodraven_replication_running): spec.sites[].role.
func Role(r v1beta1.SiteRole) string { return kebab(string(r)) }

// SiteState is the state label of bloodraven_site_state and the from and
// to labels of bloodraven_state_transitions_total.
func SiteState(s v1beta1.SiteState) string { return kebab(string(s)) }

// SourceState is the state label of bloodraven_replication_source_state.
func SourceState(s v1beta1.SourceConvergenceState) string { return kebab(string(s)) }

// KeyringPhase is the phase label of bloodraven_keyring_phase.
func KeyringPhase(p v1beta1.SiteKeyringPhase) string { return kebab(string(p)) }

// RestoreReclonePhase is the phase label of
// bloodraven_restore_reclone_pending.
func RestoreReclonePhase(p v1beta1.RestoreReclonesPhase) string { return kebab(string(p)) }

// PlannedFailoverResult is the result label of
// bloodraven_planned_failovers_total: the run's outcome.
func PlannedFailoverResult(o v1beta1.PlannedFailoverOutcome) string { return kebab(string(o)) }

// SiteStates are the bloodraven_site_state label values, in the order the
// state-set is emitted.
var SiteStates = labelsOf(SiteState,
	v1beta1.SiteStateWritable, v1beta1.SiteStateReadOnly, v1beta1.SiteStateUnreachable, v1beta1.SiteStateUnknown)

// SourceStates are the bloodraven_replication_source_state label values.
var SourceStates = labelsOf(SourceState,
	v1beta1.SourceConvergenceConverged, v1beta1.SourceConvergencePending, v1beta1.SourceConvergenceBlocked)

// KeyringPhases are the bloodraven_keyring_phase label values.
var KeyringPhases = labelsOf(KeyringPhase,
	v1beta1.KeyringPhasePending, v1beta1.KeyringPhaseUnsealed, v1beta1.KeyringPhaseEscrowed,
	v1beta1.KeyringPhaseSealed, v1beta1.KeyringPhaseFailed)

func labelsOf[T ~string](f func(T) string, vs ...T) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = f(v)
	}
	return out
}
