package scenarios

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/sidecar"
)

// operatorServiceAccount is the operator's ServiceAccount name in the
// playground. Its namespace is env.Namespace (bloodraven-playground). The
// RBAC-denial scenarios (32, 38) resolve the ClusterRole bound to this SA and
// strip selected verbs, then restore them in cleanup.
const operatorServiceAccount = "bloodraven"

// siteLBIP returns spec.sites[<site>].lbIP, or "" if the site is unknown.
// The DNSEndpoint failover scenario (38) compares this against the live
// DNSEndpoint targets.
func siteLBIP(mfg *v1alpha1.MysqlFailoverGroup, site string) string {
	for _, s := range mfg.Spec.Sites {
		if s.Name == site {
			return s.LBIP
		}
	}
	return ""
}

// stringsContain reports whether want is in list.
func stringsContain(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// probeSiteWritable opens (or reuses) a direct MySQL connection to a site and
// reports whether it is writable at the MySQL layer: read_only=OFF AND
// super_read_only=OFF. Used by the RBAC-denial scenarios to assert promotion
// at the data layer even while the operator's status write is denied and the
// CR is therefore stale.
func probeSiteWritable(ctx context.Context, env *runner.Env, site string) (bool, error) {
	c, err := env.MySQL(site)
	if err != nil {
		return false, err
	}
	ro, err := c.ReadOnly(ctx)
	if err != nil {
		return false, err
	}
	sro, err := c.SuperReadOnly(ctx)
	if err != nil {
		return false, err
	}
	return !ro && !sro, nil
}

// waitSiteWritable polls a site's direct MySQL connection until it is writable
// at the MySQL layer, or the deadline passes. Used by the RBAC-denial
// scenarios to detect promotion at the data layer without trusting the CR
// status (which is intentionally frozen while status writes are denied).
func waitSiteWritable(ctx context.Context, env *runner.Env, site string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for {
		w, err := probeSiteWritable(ctx, env, site)
		if err == nil && w {
			return nil
		}
		if err != nil {
			last = err.Error()
		} else {
			last = "site still read-only"
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("site %s did not become writable within %s (last: %s)", site, timeout, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// --- SOURCE_DELAY helpers (scenario 35) ------------------------------------

// setSourceDelay applies CHANGE REPLICATION SOURCE TO SOURCE_DELAY=<seconds>
// on a replica while keeping both IO and SQL threads running, so the
// operator's replicaStatusHealthy check stays true (STOP REPLICA SQL_THREAD
// would trip auto-recovery). Idempotent.
func setSourceDelay(ctx context.Context, c *pgmysql.SiteClient, seconds int) error {
	stmts := []string{
		"STOP REPLICA",
		fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_DELAY = %d", seconds),
		"START REPLICA",
	}
	for _, stmt := range stmts {
		if _, err := c.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("apply source_delay stmt %q: %w", stmt, err)
		}
	}
	return nil
}

// clearSourceDelay resets SOURCE_DELAY to 0 and restarts replication.
func clearSourceDelay(ctx context.Context, c *pgmysql.SiteClient) error {
	return setSourceDelay(ctx, c, 0)
}

// s35RollbackConverged classifies an observed status snapshot while polling
// for a planned-failover LagTimeout rollback to settle. Unfencing the source
// is eventual — the topology manager only reports a site as status.activeSite
// once it polls the site as writable, and unfence clears super_read_only
// before read_only — so status.activeSite transiently reads "" between the
// terminal Failed phase and the source resuming as sole active site. done is
// true only once the source is confirmed active again; err fires immediately
// (terminating the poll) if the rollback actually left the target active or
// advanced the failover history to the target, since those are genuine
// contract violations rather than eventual-consistency noise.
func s35RollbackConverged(activeSite, lastFailoverTarget, source, target string) (done bool, msg string, err error) {
	msg = fmt.Sprintf("activeSite=%q lastFailoverTarget=%q", activeSite, lastFailoverTarget)
	if activeSite == target {
		return false, msg, fmt.Errorf("status.activeSite advanced to the rolled-back target %q instead of staying on source %q", target, source)
	}
	if lastFailoverTarget == target {
		return false, msg, fmt.Errorf("lastFailoverTarget advanced to the rolled-back target %q", target)
	}
	return activeSite == source, msg, nil
}

// --- restore-in-place helpers (scenario 36) --------------------------------

// newConfirmToken returns an RFC3339 confirm token for spec.restoreInPlace,
// offset from base. The operator requires the token to be a valid RFC3339
// timestamp strictly greater than status.restoreInPlace.confirmTokenUsed.
func newConfirmToken(base time.Time, offset time.Duration) string {
	return base.Add(offset).UTC().Format(time.RFC3339)
}

// confirmTokenAdvances mirrors the operator's confirmAdvances gate: a spec
// confirm advances when it parses as RFC3339 and is strictly after the
// last-used token (an empty or unparseable last-used token always advances).
// Kept as a pure helper so scenario 36 can construct a deliberately-stale
// token and unit tests can lock the monotonicity contract.
func confirmTokenAdvances(specConfirm, lastUsed string) (bool, error) {
	specT, err := time.Parse(time.RFC3339, specConfirm)
	if err != nil {
		return false, fmt.Errorf("confirm %q is not RFC3339: %w", specConfirm, err)
	}
	if lastUsed == "" {
		return true, nil
	}
	lastT, err := time.Parse(time.RFC3339, lastUsed)
	if err != nil {
		return true, nil
	}
	return specT.After(lastT), nil
}

// patchRestoreInPlace sets spec.restoreInPlace on the MFG (add or replace).
// Removal is handled in the scenario Cleanup via removeRestoreInPlaceSpec so
// a failed restore leaves the spec observable for forensics until cleanup.
//
// Returns the status.restoreInPlace observed immediately before the patch
// (nil if none was set yet). The reconciler does not clear a previous
// terminal status the instant a new spec is written — it only replaces
// status.restoreInPlace once it actually reconciles the new spec — so a
// caller that immediately waits on status.restoreInPlace can otherwise
// observe this stale pre-patch snapshot and mistake it for the outcome of
// the request it just made. Callers that need to tell the fresh request's
// status apart from a leftover terminal status (e.g. waitRestoreInPlaceActive)
// should pass this snapshot through to restoreInPlaceStatusChanged.
func patchRestoreInPlace(ctx context.Context, env *runner.Env, spec v1alpha1.RestoreInPlaceSpec) (*v1alpha1.RestoreInPlaceStatus, error) {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return nil, err
	}
	op := "add"
	if mfg.Spec.RestoreInPlace != nil {
		op = "replace"
	}
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{
		Op:    op,
		Path:  "/spec/restoreInPlace",
		Value: spec,
	}}); err != nil {
		return nil, err
	}
	return mfg.Status.RestoreInPlace, nil
}

// removeRestoreInPlaceSpec removes spec.restoreInPlace if present. Idempotent.
func removeRestoreInPlaceSpec(ctx context.Context, env *runner.Env) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	if mfg.Spec.RestoreInPlace == nil {
		return nil
	}
	return env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{
		Op:   "remove",
		Path: "/spec/restoreInPlace",
	}})
}

// waitRestoreInPlacePhase polls status.restoreInPlace.phase until it equals
// want or reaches a different terminal phase (Succeeded/Failed). Returns the
// observed status. An unexpected terminal phase is returned as an error.
func waitRestoreInPlacePhase(ctx context.Context, env *runner.Env, want v1alpha1.RestoreInPlacePhase) (*v1alpha1.RestoreInPlaceStatus, error) {
	var result *v1alpha1.RestoreInPlaceStatus
	_, err := env.Wait.UntilCR(ctx, env.Namespace, fmt.Sprintf("restoreInPlace.phase==%s", want),
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			rip := mfg.Status.RestoreInPlace
			if rip == nil {
				return false, "no status.restoreInPlace yet", nil
			}
			result = rip
			msg := fmt.Sprintf("phase=%q target=%q job=%q confirmUsed=%q message=%q",
				rip.Phase, rip.TargetSite, rip.JobName, rip.ConfirmTokenUsed, rip.Message)
			if rip.Phase == want {
				return true, msg, nil
			}
			if restoreInPlaceTerminal(rip.Phase) {
				return false, msg, fmt.Errorf("restoreInPlace reached terminal %q before %q: %s", rip.Phase, want, rip.Message)
			}
			return false, msg, nil
		})
	return result, err
}

// waitRestoreInPlaceActive waits until the restore Job exists — phase
// Restoring with a non-empty jobName — so a scenario can pull storage out from
// under an actually-running restore.
//
// stale is the status.restoreInPlace snapshot observed just before the spec
// was patched (patchRestoreInPlace's return value), or nil if none existed.
// reconcileInPlaceRestore does not clear a previous terminal status the
// instant the new spec is written; it only replaces status.restoreInPlace
// once it actually reconciles the new spec. Until an observed status is
// distinguishable from stale, it cannot be attributed to the fresh request,
// so it is neither a terminal failure nor progress — the wait keeps polling
// instead of surfacing it as a hard error.
func waitRestoreInPlaceActive(ctx context.Context, env *runner.Env, stale *v1alpha1.RestoreInPlaceStatus) (*v1alpha1.RestoreInPlaceStatus, error) {
	var result *v1alpha1.RestoreInPlaceStatus
	_, err := env.Wait.UntilCR(ctx, env.Namespace, "restoreInPlace Job active (phase=Restoring, jobName set)",
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			rip := mfg.Status.RestoreInPlace
			if rip == nil {
				return false, "no status.restoreInPlace yet", nil
			}
			msg := fmt.Sprintf("phase=%q job=%q message=%q", rip.Phase, rip.JobName, rip.Message)
			if !restoreInPlaceStatusChanged(stale, rip) {
				return false, "stale status from prior request: " + msg, nil
			}
			result = rip
			if rip.Phase == v1alpha1.RestoreInPlaceFailed || rip.Phase == v1alpha1.RestoreInPlaceSucceeded {
				return false, msg, fmt.Errorf("restoreInPlace terminal %q before the Job became observable: %s", rip.Phase, rip.Message)
			}
			return rip.Phase == v1alpha1.RestoreInPlaceRestoring && rip.JobName != "", msg, nil
		})
	return result, err
}

// restoreInPlaceStatusChanged reports whether observed is distinguishable
// from stale, a snapshot captured before a fresh spec.restoreInPlace was
// patched in. It compares every field the reconciler refreshes on each
// state-machine transition (see reconcileInPlaceRestore and its per-phase
// helpers in internal/controller/restore_inplace.go): phase, jobName,
// targetSite, message, and startTime — startTime in particular is
// re-stamped with a fresh metav1.Now() every time a terminal status
// advances into a new attempt (the cur==nil branch of
// reconcileInPlaceRestore), so it alone would almost always prove a
// transition, but comparing every field avoids relying on clock resolution.
// A nil stale snapshot means no restoreInPlace status existed before the
// patch, so any non-nil observed status is by definition already changed.
func restoreInPlaceStatusChanged(stale, observed *v1alpha1.RestoreInPlaceStatus) bool {
	if stale == nil || observed == nil {
		return stale != observed
	}
	if stale.Phase != observed.Phase ||
		stale.JobName != observed.JobName ||
		stale.TargetSite != observed.TargetSite ||
		stale.Message != observed.Message {
		return true
	}
	return !stale.StartTime.Equal(observed.StartTime)
}

func restoreInPlaceTerminal(p v1alpha1.RestoreInPlacePhase) bool {
	return p == v1alpha1.RestoreInPlaceSucceeded || p == v1alpha1.RestoreInPlaceFailed
}

// deleteJobIfPresent best-effort deletes a batch Job by name (foreground
// propagation so its pods go too) and is a no-op when the Job is absent.
func deleteJobIfPresent(ctx context.Context, env *runner.Env, name string) error {
	if name == "" {
		return nil
	}
	policy := metav1.DeletePropagationForeground
	err := env.Kube.Kubernetes.BatchV1().Jobs(env.Namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// captureJobPodLogs writes the logs of every pod belonging to a batch Job to
// the scenario capture dir, one file per container. Best-effort — records
// failures as capture notes rather than returning them. Scenario 36 uses it to
// capture the restore Job's S3/RustFS read failure as evidence.
func captureJobPodLogs(ctx context.Context, env *runner.Env, jobName, filePrefix string) {
	if jobName == "" {
		return
	}
	pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + jobName})
	if err != nil {
		env.Capture.Note("list job pods failed: " + err.Error())
		return
	}
	for _, pod := range pods.Items {
		containers := make([]string, 0)
		for _, c := range pod.Spec.InitContainers {
			containers = append(containers, c.Name)
		}
		for _, c := range pod.Spec.Containers {
			containers = append(containers, c.Name)
		}
		for _, cname := range containers {
			body, err := env.Kube.PodLogTailLines(ctx, env.Namespace, pod.Name, cname, 400)
			if err != nil {
				env.Capture.Note(fmt.Sprintf("read job pod log %s/%s failed: %v", pod.Name, cname, err))
				continue
			}
			_ = env.Capture.WriteFile(fmt.Sprintf("%s-%s-%s.log", filePrefix, pod.Name, cname), body)
		}
	}
}

// inPlaceRestoreJobName mirrors the controller's fixed in-place restore Job
// name (inPlaceRestoreJobName in internal/controller/restore_inplace.go:
// "mysql-<fg>-<site>-inplace-restore"). The controller truncates to the
// 63-char DNS-1123 label limit, but the playground fg/site names are far
// under it, so a plain format is faithful here.
func inPlaceRestoreJobName(fg, site string) string {
	return fmt.Sprintf("mysql-%s-%s-inplace-restore", fg, site)
}

// clearRetainedInPlaceRestoreJobs captures pod logs (best-effort, when
// capturePrefix is non-empty) for, then deletes, any retained fixed-name
// in-place restore Job on each given site. The Job name is fixed, so a
// terminal Job from a prior attempt survives across runs (and across a
// playground reset). The controller now clears such a Job itself before
// reusing the name, but a scenario that aborts before stashing the Job
// name still needs to sweep the leftover so the next attempt starts clean,
// and its logs are the evidence of why a prior restore failed.
func clearRetainedInPlaceRestoreJobs(ctx context.Context, env *runner.Env, capturePrefix string, sites ...string) error {
	seen := map[string]bool{}
	var errs []error
	for _, site := range sites {
		if site == "" || seen[site] {
			continue
		}
		seen[site] = true
		name := inPlaceRestoreJobName(env.FG, site)
		if capturePrefix != "" {
			captureJobPodLogs(ctx, env, name, capturePrefix+"-"+site)
		}
		if err := deleteJobIfPresent(ctx, env, name); err != nil {
			errs = append(errs, fmt.Errorf("delete retained restore job %s: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("clear retained in-place restore jobs: %w", errors.Join(errs...))
	}
	return nil
}

// --- PITR manifest helpers (scenario 37) -----------------------------------

// readSiteBinlogManifest reads a per-site binlog manifest straight from RustFS
// using the sidecar's own key convention (<prefix>/binlogs/manifest-<site>.json).
// Returns found=false when the site has archived nothing yet.
func readSiteBinlogManifest(ctx context.Context, env *runner.Env, bucket, binlogPrefix, site string) (*pgsidecar.Manifest, bool, error) {
	key := pgsidecar.ManifestKey(binlogPrefix, site)
	data, found, err := env.Chaos.ReadRustFSObject(ctx, bucket, key)
	if err != nil {
		return nil, false, fmt.Errorf("read manifest %s: %w", key, err)
	}
	if !found {
		return nil, false, nil
	}
	var m pgsidecar.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, true, fmt.Errorf("decode manifest %s: %w", key, err)
	}
	return &m, true, nil
}

// pitrHandoffSanityQuery builds the scalar sanity query for the PITR
// archive-handoff verification: it returns 1 iff the marker table replayed to
// the PITR target contains exactly the baseline row plus markers A and B (both
// inserted before the target) and NOT marker C (inserted after the target),
// scoped to this run's run_id. Kept separate so it can be unit-tested.
func pitrHandoffSanityQuery(db, runStem string) string {
	return "SELECT IF(" +
		"SUM(phase='baseline') = 1 AND SUM(phase='A') = 1 AND SUM(phase='B') = 1 AND SUM(phase='C') = 0" +
		", 1, 0) FROM " + quoteSQLIdentifier(db) + ".marker WHERE run_id=" + quoteSQLString(runStem)
}

func quoteSQLIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

// manifestNewestLastEvent returns the latest LastEventTime across a manifest's
// files (zero time if empty).
func manifestNewestLastEvent(m *pgsidecar.Manifest) time.Time {
	var newest time.Time
	if m == nil {
		return newest
	}
	for _, f := range m.Files {
		if f.LastEventTime.After(newest) {
			newest = f.LastEventTime
		}
	}
	return newest
}
