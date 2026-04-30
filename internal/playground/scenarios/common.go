// Package scenarios contains the playground chaos scenario
// definitions. Each scenario file's init() registers itself with
// runner.DefaultRegistry, so the cmd/playground-chaos binary picks
// them up via a blank import of this package.
package scenarios

import (
	"context"
	"fmt"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// AssertHealthyBaseline is the precheck shared by every scenario.
// Bails with a human-readable error suggesting `reset-mysql.sh` if
// the cluster is in a recovery-blocked or otherwise un-clean state.
func AssertHealthyBaseline(ctx context.Context, env *runner.Env) error {
	mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
	if err != nil {
		return fmt.Errorf("baseline: get MFG: %w", err)
	}
	if pgkube.ReadyCondition(mfg) != "True" {
		return fmt.Errorf("baseline unhealthy: Ready condition is %q (run ./playground/setup.sh)", pgkube.ReadyCondition(mfg))
	}
	if mfg.Status.ActiveSite == "" {
		return fmt.Errorf("baseline unhealthy: no active site (run ./playground/setup.sh)")
	}
	for _, s := range mfg.Status.Sites {
		if s.RecoveryState == "RecoveryBlocked" {
			return fmt.Errorf("baseline unhealthy: site %s has RecoveryBlocked — run ./playground/reset-mysql.sh and retry", s.Name)
		}
		if s.State == "" || s.State == "unknown" || s.State == "unreachable" {
			return fmt.Errorf("baseline unhealthy: site %s state=%q — wait for recovery or run ./playground/reset-mysql.sh", s.Name, s.State)
		}
	}
	if mfg.Status.PlannedFailover != nil &&
		mfg.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseNone &&
		!plannedTerminal(mfg.Status.PlannedFailover.Phase) {
		return fmt.Errorf("baseline unhealthy: planned failover in non-terminal phase %q", mfg.Status.PlannedFailover.Phase)
	}
	return nil
}

func plannedTerminal(p v1alpha1.PlannedFailoverPhase) bool {
	switch p {
	case v1alpha1.PlannedFailoverPhaseSucceeded, v1alpha1.PlannedFailoverPhaseFailed:
		return true
	default:
		return false
	}
}

// PrimaryCandidates returns the ordered list of primary-candidate
// site names from spec.sites.
func PrimaryCandidates(mfg *v1alpha1.MysqlFailoverGroup) []string {
	var out []string
	for _, s := range mfg.Spec.Sites {
		if s.IsPromotable() {
			out = append(out, s.Name)
		}
	}
	return out
}

// PeerOf returns the first primary-candidate site name that is not
// the active site. For the playground's 2-site topology this is the
// failover target.
func PeerOf(mfg *v1alpha1.MysqlFailoverGroup, site string) (string, error) {
	for _, c := range PrimaryCandidates(mfg) {
		if c != site {
			return c, nil
		}
	}
	return "", fmt.Errorf("no peer site found for %q", site)
}
