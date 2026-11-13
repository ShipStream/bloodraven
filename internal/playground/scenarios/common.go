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
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// AssertDragonflyHealthyBaseline is the precheck for Dragonfly-specific
// scenarios. The shared baseline intentionally treats omitted
// spec.dragonfly as a valid MySQL-only playground, but Dragonfly
// scenarios need the subsystem enabled or their assertions can attach
// to leftover pods and wait forever for status the operator will never
// populate.
func AssertDragonflyHealthyBaseline(ctx context.Context, env *runner.Env) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("dragonfly baseline: get MFG: %w", err)
	}
	if mfg.Spec.Dragonfly == nil || !mfg.Spec.Dragonfly.Enabled {
		return fmt.Errorf("dragonfly baseline unhealthy: spec.dragonfly.enabled is not true — run ./playground/setup.sh or apply playground/manifests/failovergroup.yaml")
	}
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
	failoverRecord, _, failoverErr := brcontroller.EffectiveFailoverRecord(mfg, time.Now())
	if failoverErr != nil {
		return fmt.Errorf("baseline unhealthy: durable anti-flap state is invalid (%v) — run ./playground/reset-mysql.sh", failoverErr)
	}

	// A standing planned-failover automation hold keeps emergency
	// failover, bootstrap and DNS reconcile held, so every scenario that
	// needs them would time out with no hint of the cause.
	if hold := brcontroller.PlannedHoldFrom(mfg); hold != nil {
		if hold.RunID == "" {
			return fmt.Errorf("baseline unhealthy: AutomaticFailoverHeld=True names no planned-failover run (status.plannedFailoverHold is absent) — resolve the split brain, then remove the condition by hand (see the hold runbook) or run ./playground/reset-mysql.sh")
		}
		return fmt.Errorf("baseline unhealthy: planned-failover automation hold stands (run %s, reason %s) — resolve the split brain, then kubectl bloodraven ack-planned-failover-hold %s -n %s, or run ./playground/reset-mysql.sh",
			hold.RunID, emptyAsNone(string(hold.Reason)), fg, namespace)
	}

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
	if t := failoverRecord.LastFailoverTarget; t != "" {
		if !siteInSpec(mfg, t) {
			return fmt.Errorf("baseline unhealthy: effective lastFailoverTarget=%q is not in spec.sites — run ./playground/reset-mysql.sh", t)
		}
	}

	// Anti-flap cooldown still ticking from a prior scenario. The
	// operator's emergency-failover gate is `time.Since(lastFailover) <
	// failoverCooldown`. If we start an emergency-failover scenario
	// while inside that window, it hangs until the timeout. Surface it
	// up front.
	if cd := failoverCooldown(mfg); cd > 0 && !failoverRecord.LastFailover.IsZero() {
		elapsed := time.Since(failoverRecord.LastFailover)
		if elapsed < cd {
			remaining := (cd - elapsed).Round(time.Second)
			return fmt.Errorf("baseline unhealthy: anti-flap cooldown active for another %s — wait or run ./playground/reset-mysql.sh", remaining)
		}
	}
	if mfg.Status.UpdatePhase != "" {
		return fmt.Errorf("baseline unhealthy: ordered update still in phase %q — wait for rollout to finish", mfg.Status.UpdatePhase)
	}

	if pgkube.ReadyCondition(mfg) != "True" {
		// Disambiguate the matrix.go "both read-only / NoPrimary" case
		// — without this hint the user sees "Ready=False" and starts
		// digging through operator logs for a real fault when the
		// fix is just to wipe stale status.
		if bothReadOnly(mfg) {
			return fmt.Errorf("baseline unhealthy: operator sees all sites read-only and refuses to auto-promote (matrix.go startup-state guard) — run ./playground/reset-mysql.sh")
		}
		return fmt.Errorf("baseline unhealthy: Ready condition is %q (run ./playground/setup.sh)", pgkube.ReadyCondition(mfg))
	}
	if mfg.Status.ActiveSite == "" {
		if bothReadOnly(mfg) {
			return fmt.Errorf("baseline unhealthy: operator sees all sites read-only and refuses to auto-promote (matrix.go startup-state guard) — run ./playground/reset-mysql.sh")
		}
		return fmt.Errorf("baseline unhealthy: no active site (run ./playground/setup.sh)")
	}
	// Every site of the spec, the non-active and the read-only ones
	// included, must have been observed. A site the operator has not
	// reported yet (a reader still coming up after setup.sh) has no
	// status entry, and iterating status.sites alone would pass it (#262).
	for _, s := range mfg.Spec.Sites {
		if !siteInStatus(mfg, s.Name) {
			return fmt.Errorf("baseline unhealthy: site %s state=%q (no status reported yet) — wait for recovery or run ./playground/reset-mysql.sh", s.Name, "")
		}
	}
	for _, s := range mfg.Status.Sites {
		if s.RecoveryState == v1alpha1.RecoveryStateBlocked {
			return fmt.Errorf("baseline unhealthy: site %s has RecoveryBlocked — run ./playground/reset-mysql.sh and retry", s.Name)
		}
		if s.State == "" || s.State == v1alpha1.SiteStateUnknown || s.State == v1alpha1.SiteStateUnreachable {
			return fmt.Errorf("baseline unhealthy: site %s state=%q — wait for recovery or run ./playground/reset-mysql.sh", s.Name, s.State)
		}
	}

	// A paced rollout that waits, rolls or is stalled restarts pods (or
	// will) under whatever the scenario does next.
	if r := mfg.Status.Rollout; r != nil && r.Phase != "" && r.Phase != v1alpha1.RolloutPhaseComplete {
		return fmt.Errorf("baseline unhealthy: paced rollout in phase %q (%s) — wait for it to complete; kubectl -n %s get mysqlfailovergroup %s -o jsonpath='{.status.rollout}'",
			r.Phase, emptyAsNone(r.Reason), namespace, fg)
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
		if !s.Replication.Running() {
			return fmt.Errorf("baseline unhealthy: site %s is not replicating from %s — run ./playground/reset-mysql.sh and retry",
				s.Name, mfg.Status.ActiveSite)
		}
	}

	if mfg.Status.PlannedFailover != nil &&
		mfg.Status.PlannedFailover.Phase != "" &&
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
// site is a replica with master_link_status="up" and observed master IO.
// This folds the D1
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
	if mfg.Status.ActiveSite != "" && dfStat.ActiveSite != mfg.Status.ActiveSite {
		return fmt.Errorf("baseline unhealthy: status.dragonfly.activeSite=%q does not match status.activeSite=%q",
			dfStat.ActiveSite, mfg.Status.ActiveSite)
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
			if s.LastIOSecondsAgo == nil || *s.LastIOSecondsAgo < 0 {
				return fmt.Errorf("baseline unhealthy: dragonfly replica site %q has lastIOSecondsAgo=%s (never synced)", s.Name, lastIOString(s.LastIOSecondsAgo))
			}
			if dragonflySyncing(s.SyncInProgress) {
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
		if s.State != v1alpha1.SiteStateReadOnly {
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

func siteInStatus(mfg *v1alpha1.MysqlFailoverGroup, name string) bool {
	for _, s := range mfg.Status.Sites {
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

// failoverCooldown returns the configured cooldown or the operator default.
func failoverCooldown(mfg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if mfg.Spec.FailoverCooldown != nil && mfg.Spec.FailoverCooldown.Duration > 0 {
		return mfg.Spec.FailoverCooldown.Duration
	}
	return 5 * time.Minute
}

// plannedReason is the planned-failover run's reason: its result reason
// once terminal, else its admission wait or defer reason.
func plannedReason(pf *v1alpha1.PlannedFailoverStatus) string {
	switch {
	case pf == nil:
		return ""
	case pf.Result != nil:
		return string(pf.Result.Reason)
	case pf.Admission != nil && pf.Admission.WaitReason != "":
		return string(pf.Admission.WaitReason)
	case pf.Admission != nil:
		return string(pf.Admission.DeferReason)
	}
	return ""
}

// plannedOutcome is the terminal run's outcome, or "".
func plannedOutcome(pf *v1alpha1.PlannedFailoverStatus) v1alpha1.PlannedFailoverOutcome {
	if pf == nil || pf.Result == nil {
		return ""
	}
	return pf.Result.Outcome
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

func emptyAsNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// pitrSpecProfile is spec.backup.pitr.profileName while PITR is enabled,
// else "". Status no longer echoes either.
func pitrSpecProfile(mfg *v1alpha1.MysqlFailoverGroup) string {
	if mfg.Spec.Backup == nil || mfg.Spec.Backup.PITR == nil || !mfg.Spec.Backup.PITR.Enabled {
		return ""
	}
	return mfg.Spec.Backup.PITR.ProfileName
}

// encryptionAllSitesSealed reports EncryptionAtRestReady=True with reason
// AllSitesSealed: every site's keyring is sealed.
func encryptionAllSitesSealed(mfg *v1alpha1.MysqlFailoverGroup) bool {
	c := apimeta.FindStatusCondition(mfg.Status.Conditions, v1alpha1.ConditionEncryptionAtRestReady)
	return c != nil && c.Status == metav1.ConditionTrue && c.Reason == v1alpha1.ReasonAllSitesSealed
}

// restorePointInTime is status.restore.provenance.pointInTime, nil when
// the restore recorded none.
func restorePointInTime(rs *v1alpha1.RestoreStatus) *v1alpha1.PointInTimeResult {
	if rs == nil || rs.Provenance == nil {
		return nil
	}
	return rs.Provenance.PointInTime
}

// dragonflySyncing reports a syncInProgress Dragonfly reported as true; an
// unreported value is not a sync in progress.
func dragonflySyncing(v *bool) bool {
	return v != nil && *v
}

// optionalBoolString formats an optional observation.
func optionalBoolString(v *bool) string {
	if v == nil {
		return "unset"
	}
	return fmt.Sprint(*v)
}

// lastIOString renders status.dragonfly.sites[].lastIOSecondsAgo; "unset"
// when Dragonfly did not report it.
func lastIOString(v *int64) string {
	if v == nil {
		return "unset"
	}
	return fmt.Sprint(*v)
}
