// Package scenarios contains the playground chaos scenario
// definitions. Each scenario file's init() registers itself with
// runner.DefaultRegistry, so the cmd/playground-chaos binary picks
// them up via a blank import of this package.
package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
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
	if err := assertDragonflyBaselineHealthy(mfg); err != nil {
		return err
	}
	return nil
}

// assertDragonflyBaselineHealthy is a no-op when spec.dragonfly is
// disabled. When enabled it requires that status.dragonfly.phase is
// Ready, that exactly one site is the master, and that every other
// site is a replica with master_link_status="up". This folds the D1
// (replica attachment / LOADING bug) and D9 (silent key loss) baseline
// checks into the standard precheck rather than separate scenarios —
// every Dragonfly chaos scenario inherits them.
func assertDragonflyBaselineHealthy(mfg *v1alpha1.MysqlFailoverGroup) error {
	if mfg.Spec.Dragonfly == nil || !mfg.Spec.Dragonfly.Enabled {
		return nil
	}
	dfStat := mfg.Status.Dragonfly
	if dfStat == nil {
		return fmt.Errorf("baseline unhealthy: spec.dragonfly enabled but status.dragonfly not yet populated (waiting for first reconcile)")
	}
	if dfStat.Phase != v1alpha1.DragonflyPhaseReady {
		return fmt.Errorf("baseline unhealthy: status.dragonfly.phase=%q (want Ready)", dfStat.Phase)
	}
	if dfStat.ActiveSite == "" {
		return fmt.Errorf("baseline unhealthy: status.dragonfly.activeSite is empty")
	}
	masters := 0
	for _, s := range dfStat.Sites {
		switch s.Role {
		case v1alpha1.DragonflyRoleMaster:
			masters++
			if s.Name != dfStat.ActiveSite {
				return fmt.Errorf("baseline unhealthy: dragonfly site %q reports role=master but activeSite=%q (split-brain or stale-master)",
					s.Name, dfStat.ActiveSite)
			}
		case v1alpha1.DragonflyRoleReplica:
			if s.LinkStatus != "up" {
				return fmt.Errorf("baseline unhealthy: dragonfly replica site %q has linkStatus=%q (want up)", s.Name, s.LinkStatus)
			}
			if s.SyncInProgress {
				return fmt.Errorf("baseline unhealthy: dragonfly replica site %q has syncInProgress=true", s.Name)
			}
		case v1alpha1.DragonflyRoleUnreachable:
			return fmt.Errorf("baseline unhealthy: dragonfly site %q is unreachable", s.Name)
		}
	}
	if masters != 1 {
		return fmt.Errorf("baseline unhealthy: expected exactly 1 dragonfly master, got %d", masters)
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

// firstMatchSince scans a tailer's ring buffer once for the first line
// at or after `since` that satisfies pred. Used by negative-assertion
// scenarios that want to confirm a forbidden log line did NOT appear
// inside the observation window — Wait.UntilLog is the wrong tool
// because it blocks until match-or-deadline, but here we want a single
// snapshot scan of "anything that already happened".
func firstMatchSince(t *pglogs.Tailer, since time.Time, pred pglogs.Predicate) (bool, string) {
	for _, m := range t.Snapshot() {
		if !since.IsZero() && m.Time.Before(since) {
			continue
		}
		if pred(m.Line) {
			return true, m.Line
		}
	}
	return false, ""
}
