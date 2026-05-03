// Package scenarios contains the playground chaos scenario
// definitions. Each scenario file's init() registers itself with
// runner.DefaultRegistry, so the cmd/playground-chaos binary picks
// them up via a blank import of this package.
package scenarios

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// AssertHealthyBaseline is the precheck shared by every scenario.
// Bails with a human-readable error suggesting `reset-mysql.sh` if
// the cluster is in a recovery-blocked or otherwise un-clean state.
//
// Each error includes the exact remediation command — the goal is that
// the next chaos run either passes or fails with a single specific
// error, never with a 4-min hang on `ready=false`.
func AssertHealthyBaseline(ctx context.Context, env *runner.Env) error {
	return CheckBaseline(ctx, env.Kube, env.Namespace, env.FG)
}

// CheckBaseline is the runner-Env-free form of AssertHealthyBaseline.
// Used by the chaos CLI's `check` subcommand to run structural checks
// without a full Env. Scenarios should keep using AssertHealthyBaseline.
func CheckBaseline(ctx context.Context, k *pgkube.Client, namespace, fg string) error {
	mfg, err := k.GetMFGNamed(ctx, namespace, fg)
	if err != nil {
		return fmt.Errorf("baseline: get MFG: %w", err)
	}

	// Structural symptom checks first — these surface specific known
	// failure modes that would otherwise hide behind a generic
	// "ready=false" or "no active site" error.

	// Stuck scale-to-0 from a prior chaos run that did not clean up.
	// We check Deployment.spec.replicas rather than running pods because
	// "scaled to 0" is the sticky state — a deployment held at 0 replicas
	// will never go Ready until it is scaled back up.
	for _, s := range mfg.Spec.Sites {
		dep, err := k.GetDeployment(ctx, namespace, pgkube.MysqlDeploymentName(fg, s.Name))
		if err != nil {
			// If the deployment is missing, fall through — the
			// generic ready=false / state=unknown checks below will
			// catch it with a more useful message.
			continue
		}
		if dep.Spec.Replicas != nil && *dep.Spec.Replicas == 0 {
			return fmt.Errorf("baseline unhealthy: site %s scaled to 0 from prior chaos — kubectl -n %s scale deployment %s --replicas=1",
				s.Name, namespace, pgkube.MysqlDeploymentName(fg, s.Name))
		}
	}

	// lastFailoverTarget sanity: if set, must name a known site.
	if t := mfg.Status.LastFailoverTarget; t != "" {
		if !siteInSpec(mfg, t) {
			return fmt.Errorf("baseline unhealthy: status.lastFailoverTarget=%q is not in spec.sites — run ./playground/reset-mysql.sh", t)
		}
	}

	// Anti-flap cooldown still ticking from a prior scenario. The
	// operator's emergency-failover gate is `time.Since(lastFailover) <
	// failoverCooldown`. If we start an emergency-failover scenario
	// while inside that window, it hangs until the timeout. Surface it
	// up front.
	if cd := failoverCooldown(mfg); cd > 0 && mfg.Status.LastFailover != nil {
		elapsed := time.Since(mfg.Status.LastFailover.Time)
		if elapsed < cd {
			remaining := (cd - elapsed).Round(time.Second)
			return fmt.Errorf("baseline unhealthy: anti-flap cooldown active for another %s — wait or run ./playground/reset-mysql.sh", remaining)
		}
	}

	if pgkube.ReadyCondition(mfg) != "True" {
		// Disambiguate the matrix.go "both read-only / NoPrimary" case
		// — without this hint the user sees "Ready=False" and starts
		// digging through operator logs for a real fault when the
		// fix is just to wipe stale status.
		if bothReadOnly(mfg) {
			return fmt.Errorf("baseline unhealthy: operator sees both sites read-only and refuses to auto-promote (matrix.go startup-state guard) — run ./playground/reset-mysql.sh")
		}
		return fmt.Errorf("baseline unhealthy: Ready condition is %q (run ./playground/setup.sh)", pgkube.ReadyCondition(mfg))
	}
	if mfg.Status.ActiveSite == "" {
		if bothReadOnly(mfg) {
			return fmt.Errorf("baseline unhealthy: operator sees both sites read-only and refuses to auto-promote (matrix.go startup-state guard) — run ./playground/reset-mysql.sh")
		}
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

	// Replication off on a non-active site. The active site is the
	// primary so it does not replicate; every other primary-candidate
	// site should be replicating. We tolerate sites that are not
	// promotable (witness-only / non-DB sites) — only flag candidates.
	for _, s := range mfg.Status.Sites {
		if s.Name == mfg.Status.ActiveSite {
			continue
		}
		if !isPromotableSite(mfg, s.Name) {
			continue
		}
		if !s.Replicating {
			return fmt.Errorf("baseline unhealthy: site %s is not replicating from %s — run ./playground/reset-mysql.sh and retry",
				s.Name, mfg.Status.ActiveSite)
		}
	}

	if mfg.Status.PlannedFailover != nil &&
		mfg.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseNone &&
		!plannedTerminal(mfg.Status.PlannedFailover.Phase) {
		return fmt.Errorf("baseline unhealthy: planned failover in non-terminal phase %q", mfg.Status.PlannedFailover.Phase)
	}
	return nil
}

// bothReadOnly reports whether the observed status matches the
// matrix.go "both read-only / NoPrimary" symptom: every site is in
// state "read-only" with no writable site. This is the operator's
// startup-state guard refusing to auto-elect — the cluster cannot
// recover on its own, so the user has to intervene.
func bothReadOnly(mfg *v1alpha1.MysqlFailoverGroup) bool {
	if len(mfg.Status.Sites) == 0 {
		return false
	}
	for _, s := range mfg.Status.Sites {
		if s.State != "read-only" {
			return false
		}
	}
	return true
}

func siteInSpec(mfg *v1alpha1.MysqlFailoverGroup, name string) bool {
	for _, s := range mfg.Spec.Sites {
		if s.Name == name {
			return true
		}
	}
	return false
}

func isPromotableSite(mfg *v1alpha1.MysqlFailoverGroup, name string) bool {
	for _, s := range mfg.Spec.Sites {
		if s.Name == name {
			return s.IsPromotable()
		}
	}
	return false
}

// failoverCooldown returns the configured cooldown, or 0 if none.
func failoverCooldown(mfg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if mfg.Spec.FailoverCooldown != nil {
		return mfg.Spec.FailoverCooldown.Duration
	}
	return 0
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

func stashMetricCounter(ctx context.Context, env *runner.Env, stashKey, name string, labels map[string]string) error {
	v, err := metricCounter(ctx, env, name, labels)
	if err != nil {
		return err
	}
	return ctxStash(ctx, env, stashKey, strconv.FormatFloat(v, 'g', -1, 64))
}

func fetchStashedFloat(env *runner.Env, key string) (float64, error) {
	raw := ctxFetch(env, key)
	if raw == "" {
		return 0, fmt.Errorf("stash %s not set", key)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parse stash %s=%q: %w", key, raw, err)
	}
	return v, nil
}

func metricCounter(ctx context.Context, env *runner.Env, name string, labels map[string]string) (float64, error) {
	snap, err := env.Metrics.Scrape(ctx)
	if err != nil {
		return 0, fmt.Errorf("scrape metrics: %w", err)
	}
	v, _ := snap.Counter(name, labels)
	return v, nil
}
