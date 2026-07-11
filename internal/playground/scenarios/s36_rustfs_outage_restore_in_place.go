package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario36RustFSOutageRestoreInPlace())
}

const (
	s36ID          = "36-rustfs-outage-during-restore-in-place"
	s36RestoreDB   = "chaos_s36_restore"
	s36SafeDB      = "chaos_s36_safe"
	s36RecloneAnno = "bloodraven.shipstream.io/reclone-site"
)

// scenario36RustFSOutageRestoreInPlace exercises a per-schema in-place restore
// that fails because object storage disappears mid-restore. It isolates its
// marker schemas, takes a valid backup, checks the confirmation-token gate,
// waits for the restore Job to be actively running, then scales RustFS to 0.
// The restore must terminate Failed with the topology frozen (no emergency
// failover, no reclone) and the untouched safe schema must survive on the
// active primary. The terminal Failed must also HOLD on the same confirm: a
// genuine execution failure records confirmTokenUsed (per the
// RestoreInPlaceFailed/ConfirmTokenUsed contract), so the operator does not
// silently re-arm and re-run the destructive restore — retrying requires a new
// monotonic confirm. Destructive, so it runs ResetBeforeRunAll.
func scenario36RustFSOutageRestoreInPlace() runner.Scenario {
	return runner.Scenario{
		ID:    s36ID,
		Title: "RustFS outage during in-place restore fails safely, canary survives",
		Hypothesis: "A per-schema in-place restore whose RustFS storage is scaled to 0 mid-run terminates Failed " +
			"with targetSite=active, status.activeSite unchanged, no reclone annotation, and the untouched safe " +
			"schema still readable. An invalid confirm is rejected without a run (confirmTokenUsed stays empty). A " +
			"genuine execution failure holds terminal Failed on the same confirm (confirmTokenUsed records the " +
			"executed confirm); the operator does not auto-re-arm the destructive restore, so retrying needs a new " +
			"monotonic confirm.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#36-rustfs-outage-during-restoreinplace",
		Timeout:           24 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          s36Precheck,
		Steps: []runner.Step{
			s36ConfigureProfile(),
			s36SeedSchemas(),
			s36CreateBackup(),
			s36MutateAfterBackup(),
			s36RejectInvalidConfirm(),
			s36StartRestoreThenOutage(),
			s36ObserveFailedSafely(),
			s36VerifySafety(),
		},
		Cleanup: s36Cleanup,
	}
}

func s36Precheck(ctx context.Context, env *runner.Env) error {
	if err := assertReplicationRunningPrecheck(ctx, env); err != nil {
		return err
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	if mfg.Spec.RestoreInPlace != nil {
		return fmt.Errorf("precheck: spec.restoreInPlace already set; run ./playground/reset-mysql.sh")
	}
	return nil
}

func s36ConfigureProfile() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "ensure RustFS bucket and configure isolated backup profile",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket); err != nil {
				return err
			}
			runStem := "s36-" + backupRunStamp(env)
			prefix := "e2e/" + s36ID + "/" + runStem
			if err := ctxStash(ctx, env, "backupRunStem", runStem); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "backupPrefix", prefix); err != nil {
				return err
			}
			if err := patchBackupSpec(ctx, env, backupProfileSpec(prefix, false)); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			return waitForBackupProfile(waitCtx, env, prefix, false)
		},
	}
}

func s36SeedSchemas() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create restore + safe schemas with baseline marker rows",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "activeAtSeed", active); err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			stmts := []string{
				"CREATE DATABASE IF NOT EXISTS " + s36RestoreDB,
				"CREATE TABLE IF NOT EXISTS " + s36RestoreDB + ".marker (id INT PRIMARY KEY, note VARCHAR(64))",
				fmt.Sprintf("INSERT INTO %s.marker (id, note) VALUES (1, 'in-backup') ON DUPLICATE KEY UPDATE note=VALUES(note)", s36RestoreDB),
				"CREATE DATABASE IF NOT EXISTS " + s36SafeDB,
				"CREATE TABLE IF NOT EXISTS " + s36SafeDB + ".canary (id INT PRIMARY KEY, note VARCHAR(64))",
				fmt.Sprintf("INSERT INTO %s.canary (id, note) VALUES (1, 'must-survive') ON DUPLICATE KEY UPDATE note=VALUES(note)", s36SafeDB),
			}
			for _, q := range stmts {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("seed stmt %q: %w", q, err)
				}
			}
			if err := waitForReplicaGTID(ctx, env, active, replica); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("seeded %s.marker (in-backup) and %s.canary (must-survive) on active=%s", s36RestoreDB, s36SafeDB, active))
			return nil
		},
	}
}

func s36CreateBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create MysqlBackup and wait Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := "s36-backup-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "backupName", name); err != nil {
				return err
			}
			if err := createMysqlBackup(ctx, env, name, s36ID); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			b, err := waitForBackupPhase(waitCtx, env, name, v1alpha1.BackupPhaseSucceeded)
			if err != nil {
				return err
			}
			env.Capture.Note("backup succeeded: " + backupStatusSummary(b))
			return nil
		},
	}
}

func s36MutateAfterBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "mutate restore schema after backup (so a restore would revert it)",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, _, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			// Change chaos_s36_restore only; leave chaos_s36_safe untouched.
			if _, err := primary.Exec(ctx, fmt.Sprintf("UPDATE %s.marker SET note='post-backup-mutation' WHERE id=1", s36RestoreDB)); err != nil {
				return fmt.Errorf("mutate restore marker: %w", err)
			}
			env.Capture.Note("mutated chaos_s36_restore after the backup; chaos_s36_safe left untouched")
			return nil
		},
	}
}

func s36RejectInvalidConfirm() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "confirm-token gate: an invalid confirm is rejected without a run",
		Do: func(ctx context.Context, env *runner.Env) error {
			backupName := ctxFetch(env, "backupName")
			spec := v1alpha1.RestoreInPlaceSpec{
				Confirm:     "not-a-timestamp",
				Source:      v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: backupName}},
				LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{s36RestoreDB}},
			}
			if _, err := patchRestoreInPlace(ctx, env, spec); err != nil {
				return fmt.Errorf("patch invalid-confirm restoreInPlace: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "invalid confirm rejected (phase=Failed, no Job)",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					rip := mfg.Status.RestoreInPlace
					if rip == nil {
						return false, "no restoreInPlace status yet", nil
					}
					msg := fmt.Sprintf("phase=%q job=%q confirmUsed=%q message=%q", rip.Phase, rip.JobName, rip.ConfirmTokenUsed, rip.Message)
					if rip.Phase == v1alpha1.RestoreInPlaceFailed {
						if rip.JobName != "" {
							return false, msg, fmt.Errorf("invalid confirm should be rejected before a Job is created, got jobName=%q", rip.JobName)
						}
						if rip.ConfirmTokenUsed != "" {
							return false, msg, fmt.Errorf("invalid confirm must not populate confirmTokenUsed, got %q", rip.ConfirmTokenUsed)
						}
						return true, msg, nil
					}
					return false, msg, nil
				})
			if err != nil {
				return fmt.Errorf("invalid confirm was not cleanly rejected: %w", err)
			}
			env.Capture.Note("invalid confirm rejected (phase=Failed, no Job, confirmTokenUsed empty)")
			return nil
		},
	}
}

func s36StartRestoreThenOutage() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "apply fresh confirm, wait restore Job active, then scale RustFS to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "activeAtRestore", active); err != nil {
				return err
			}
			// Belt-and-suspenders: sweep any retained fixed-name in-place
			// restore Job left by a prior run before arming a fresh
			// request, capturing its logs first for forensics. The
			// operator now clears a stale Job itself (it no longer
			// inherits a leftover terminal Job's phase), but a fresh
			// confirmed request must not race a leftover; a failed sweep
			// is non-fatal — the operator remains the authoritative guard.
			if err := clearRetainedInPlaceRestoreJobs(ctx, env, "s36-retained-restore-job", active, replica); err != nil {
				env.Capture.Note("pre-attempt retained restore-job sweep: " + err.Error())
			}
			// Baseline the failover counter so we can prove no emergency
			// failover fired while the topology was frozen for the restore.
			// The series to watch is the one a failover would increment — the
			// PEER, i.e. the site that would be promoted away from the active
			// primary — so baseline and comparison are the same series.
			if err := stashMetricCounter(ctx, env, "s36FailoversToPeerBefore", "bloodraven_failovers_total", map[string]string{"target_site": replica}); err != nil {
				return err
			}

			backupName := ctxFetch(env, "backupName")
			confirm := newConfirmToken(env.StartTime, 5*time.Minute) // strictly-fresh, monotonic
			if err := ctxStash(ctx, env, "s36Confirm", confirm); err != nil {
				return err
			}
			env.Capture.Note("applying fresh confirm=" + confirm)
			spec := v1alpha1.RestoreInPlaceSpec{
				Confirm:     confirm,
				Source:      v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: backupName}},
				LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{s36RestoreDB}},
			}
			stale, err := patchRestoreInPlace(ctx, env, spec)
			if err != nil {
				return fmt.Errorf("patch fresh restoreInPlace: %w", err)
			}

			// Wait until the restore Job is actually running before pulling
			// storage — otherwise the failure could be attributed to backup
			// creation or validation rather than the RustFS outage. The
			// invalid-confirm step above already drove status.restoreInPlace to
			// a terminal Failed; the reconciler does not clear that status the
			// instant this patch lands, so the wait must be able to tell it
			// apart from genuine progress on this fresh request (stale).
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
			rip, err := waitRestoreInPlaceActive(waitCtx, env, stale)
			if err != nil {
				return fmt.Errorf("restore Job did not become active: %w", err)
			}
			if err := ctxStash(ctx, env, "restoreJobName", rip.JobName); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "restoreTargetSite", rip.TargetSite); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("restore Job active: job=%s targetSite=%s; scaling RustFS to 0", rip.JobName, rip.TargetSite))

			pod, err := env.Chaos.ScaleRustFSToZero(ctx)
			if err != nil {
				return err
			}
			env.Capture.Note("scaled RustFS to 0 (pod was " + pod + ")")
			return nil
		},
	}
}

func s36ObserveFailedSafely() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "restore terminates Failed and holds terminal on the same confirm; topology frozen",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			rip, err := waitRestoreInPlacePhase(waitCtx, env, v1alpha1.RestoreInPlaceFailed)
			if err != nil {
				return fmt.Errorf("restore did not reach terminal Failed: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("restoreInPlace terminal: phase=%s targetSite=%s confirmUsed=%q message=%q",
				rip.Phase, rip.TargetSite, rip.ConfirmTokenUsed, rip.Message))
			// Capture the restore Job pod logs as evidence of the S3/RustFS read failure.
			captureJobPodLogs(ctx, env, ctxFetch(env, "restoreJobName"), "s36-restore-job")
			if err := s36RequireStorageFailureEvidence(ctx, env, rip.Message, ctxFetch(env, "restoreJobName")); err != nil {
				return err
			}

			// Regression guard for the live re-arm defect: a genuine
			// execution failure records confirmTokenUsed, so the terminal
			// Failed must HOLD on the same confirm. The prior operator bug
			// left confirmTokenUsed empty on failure, so confirmAdvances()
			// treated the unchanged confirm as fresh and re-armed the
			// destructive restore (Failed→Preflight→Fencing→Restoring),
			// which the verify step then observed as Preflight. Sample the
			// phase across a window several reconciler requeues wide and
			// require it to stay Failed on the executed confirm.
			if err := s36RequireStableFailed(ctx, env, ctxFetch(env, "s36Confirm"), 20*time.Second); err != nil {
				return err
			}
			return nil
		},
	}
}

func s36RequireStorageFailureEvidence(ctx context.Context, env *runner.Env, statusMessage, jobName string) error {
	indicators := []string{"s3", "rustfs", "connection refused", "connection reset", "connection timed out", "no such host", "endpoint", "getobject", "object storage"}
	containsIndicator := func(value string) bool {
		value = strings.ToLower(value)
		for _, indicator := range indicators {
			if strings.Contains(value, indicator) {
				return true
			}
		}
		return false
	}
	if containsIndicator(statusMessage) {
		return nil
	}
	pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + jobName})
	if err != nil {
		return fmt.Errorf("list restore Job pods for storage-failure evidence: %w", err)
	}
	for _, pod := range pods.Items {
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			body, err := env.Kube.PodLogTailLines(ctx, env.Namespace, pod.Name, container.Name, 400)
			if err == nil && containsIndicator(string(body)) {
				return nil
			}
		}
	}
	return fmt.Errorf("restore failed without RustFS/S3 read or connectivity evidence (status=%q job=%q)", statusMessage, jobName)
}

// s36RequireStableFailed samples status.restoreInPlace across window and fails
// if the phase ever leaves terminal Failed (the operator auto-re-armed) or if
// confirmTokenUsed does not record the executed confirm. It proves the operator
// does not silently retry the destructive in-place restore on the same confirm
// — a retry must require a new monotonic confirm.
func s36RequireStableFailed(ctx context.Context, env *runner.Env, wantConfirm string, window time.Duration) error {
	deadline := time.Now().Add(window)
	for {
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return err
		}
		rip := mfg.Status.RestoreInPlace
		if rip == nil {
			return fmt.Errorf("status.restoreInPlace cleared while it must hold terminal Failed")
		}
		if rip.Phase != v1alpha1.RestoreInPlaceFailed {
			return fmt.Errorf("restoreInPlace re-armed off terminal Failed on the same confirm: phase=%q confirmUsed=%q message=%q "+
				"(a failed-but-executed restore must hold; retrying requires a new monotonic confirm)",
				rip.Phase, rip.ConfirmTokenUsed, rip.Message)
		}
		if wantConfirm != "" && rip.ConfirmTokenUsed != wantConfirm {
			return fmt.Errorf("terminal Failed confirmTokenUsed=%q, want the executed confirm %q "+
				"(the executed confirm must be recorded so the Failed holds)", rip.ConfirmTokenUsed, wantConfirm)
		}
		if time.Now().After(deadline) {
			env.Capture.Note(fmt.Sprintf("restoreInPlace held terminal Failed (confirmUsed=%q) across %s — no auto-re-arm", wantConfirm, window))
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func s36VerifySafety() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "targetSite correct, active unchanged, no reclone, safe canary survives",
		Do: func(ctx context.Context, env *runner.Env) error {
			activeAtRestore := ctxFetch(env, "activeAtRestore")
			wantTarget := ctxFetch(env, "restoreTargetSite")

			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			rip := mfg.Status.RestoreInPlace
			if rip == nil || rip.Phase != v1alpha1.RestoreInPlaceFailed {
				return fmt.Errorf("restoreInPlace not terminal Failed at verify: %v", rip)
			}
			if wantTarget != "" && rip.TargetSite != wantTarget {
				return fmt.Errorf("restore targetSite=%q, want %q (active at preflight)", rip.TargetSite, wantTarget)
			}
			// A genuine execution failure records the confirm it consumed
			// (RestoreInPlaceFailed/ConfirmTokenUsed contract): recording it
			// is exactly what makes the terminal Failed stable and forces a
			// new monotonic confirm to retry. It must equal the confirm this
			// run applied — not empty (which would silently re-arm) and not a
			// stale prior value.
			wantConfirm := ctxFetch(env, "s36Confirm")
			if wantConfirm != "" && rip.ConfirmTokenUsed != wantConfirm {
				return fmt.Errorf("confirmTokenUsed=%q on the failed restore, want the executed confirm %q "+
					"(a failed-but-executed restore must record its confirm so it holds terminal)", rip.ConfirmTokenUsed, wantConfirm)
			}
			if mfg.Status.ActiveSite != activeAtRestore {
				return fmt.Errorf("status.activeSite changed %q -> %q during a frozen in-place restore", activeAtRestore, mfg.Status.ActiveSite)
			}
			if _, ok := mfg.Annotations[s36RecloneAnno]; ok {
				return fmt.Errorf("unexpected reclone annotation set for a per-schema restore: %q", mfg.Annotations[s36RecloneAnno])
			}
			writable := 0
			for _, s := range mfg.Status.Sites {
				if s.State == "writable" {
					writable++
				}
				if s.RecoveryState == "RecoveryBlocked" {
					return fmt.Errorf("site %s RecoveryBlocked after failed restore", s.Name)
				}
			}
			if writable != 1 {
				return fmt.Errorf("expected exactly one writable site after failed restore, got %d", writable)
			}
			// No emergency failover fired during the restore: the same series
			// that was baselined at injection (promotions TO the peer) must not
			// have advanced. A scrape failure is inconclusive, not a failure —
			// activeSite-unchanged above already proves the topology held.
			peer, perr := PeerOf(mfg, activeAtRestore)
			before, berr := fetchStashedFloat(env, "s36FailoversToPeerBefore")
			switch {
			case perr != nil || berr != nil:
				env.Capture.Note(fmt.Sprintf("failover-counter check skipped (peer=%v baseline=%v)", perr, berr))
			default:
				cur, cerr := metricCounter(ctx, env, "bloodraven_failovers_total", map[string]string{"target_site": peer})
				if cerr != nil {
					env.Capture.Note("failover-counter check skipped (scrape failed): " + cerr.Error())
				} else if cur > before {
					return fmt.Errorf("emergency failover fired during the in-place restore: failovers_total{target_site=%s} %g -> %g (topology must stay frozen)",
						peer, before, cur)
				} else {
					env.Capture.Note(fmt.Sprintf("no emergency failover during the restore: failovers_total{target_site=%s} held at %g", peer, cur))
				}
			}

			// The untouched safe schema must still be readable on the active primary.
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, mfg.Status.ActiveSite, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", mfg.Status.ActiveSite, err)
			}
			defer primary.Close()
			note, err := primary.ScalarString(ctx, fmt.Sprintf("SELECT note FROM %s.canary WHERE id=1", s36SafeDB))
			if err != nil {
				return fmt.Errorf("read safe canary (must survive the restore of another schema): %w", err)
			}
			if note != "must-survive" {
				return fmt.Errorf("safe canary corrupted: note=%q want must-survive", note)
			}
			env.Capture.Note("safe canary survived; failed restore was RustFS-caused, topology stayed frozen")
			return nil
		},
	}
}

func s36Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	// Chaos.Revert already ran before this Cleanup and scaled RustFS back to 1.
	// Remove the restore spec so no reconcile re-arms, delete the Job, and clean
	// up backup CRs + schemas.
	if err := removeRestoreInPlaceSpec(ctx, env); err != nil {
		errs = append(errs, fmt.Errorf("remove restoreInPlace spec: %w", err))
	}
	if err := deleteJobIfPresent(ctx, env, ctxFetch(env, "restoreJobName")); err != nil {
		errs = append(errs, fmt.Errorf("delete restore job: %w", err))
	}
	// The stashed name is only populated once the Job became observable; a
	// run that aborted earlier leaves the fixed-name Job behind. Sweep it
	// on every site (capturing logs first) so nothing leaks into the next
	// run's fresh confirmed request.
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		errs = append(errs, fmt.Errorf("get MFG for retained-job and schema cleanup: %w", err))
	} else {
		var sites []string
		for i := range mfg.Spec.Sites {
			sites = append(sites, mfg.Spec.Sites[i].Name)
		}
		if serr := clearRetainedInPlaceRestoreJobs(ctx, env, "s36-cleanup-restore-job", sites...); serr != nil {
			errs = append(errs, serr)
		}
	}
	if err := deleteBackupCRs(ctx, env, ctxFetch(env, "backupName"), ""); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	// Drop scenario schemas from the writable site.
	if err == nil && mfg.Status.ActiveSite != "" {
		if primary, oerr := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, mfg.Status.ActiveSite, env.Creds); oerr == nil {
			for _, db := range []string{s36RestoreDB, s36SafeDB} {
				if _, derr := primary.Exec(ctx, "DROP DATABASE IF EXISTS "+db); derr != nil {
					env.Capture.Note(fmt.Sprintf("cleanup: drop %s: %v", db, derr))
				}
			}
			_ = primary.Close()
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("s36 cleanup: %w", errors.Join(errs...))
	}
	return nil
}
