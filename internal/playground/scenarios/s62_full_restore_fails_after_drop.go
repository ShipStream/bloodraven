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
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario62FullRestoreFailsAfterDrop())
}

const (
	s62ID       = "62-full-restore-fails-after-drop-keeps-peers"
	s62MarkerDB = "chaos_s62_marker"
	// s62DropDoneMarker is the restore Job's log line printed once every
	// user schema on the primary has been dropped.
	s62DropDoneMarker = "BLOODRAVEN_DROP_ALL_USER_SCHEMAS_DONE"
)

// scenario62FullRestoreFailsAfterDrop covers #212. A full-instance in-place
// restore drops every user schema on the primary with binlogged DROP
// DATABASE statements. Before the fix the peers replicated them (the
// operator account they replicate as by default is exempt from
// offline_mode; a dedicated replication user reconnected once the fence was
// released), so a restore
// that failed after its DROP left the peers as damaged as the primary.
//
// The scenario seeds a marker schema (one row in the backup, one written
// after it), starts a full-instance restore, takes RustFS away while the
// restore is fencing so the Job drops the schemas and then fails to load,
// and requires: the restore Failed after the DROP step, the reclone queue is
// Held, every peer's replication receiver is stopped (and stays stopped
// across a two-minute window with the topology unfrozen), and every peer
// still holds both marker rows.
func scenario62FullRestoreFailsAfterDrop() runner.Scenario {
	return runner.Scenario{
		ID:    s62ID,
		Title: "Full-instance restore failing after its DROP leaves the peers intact and held",
		Hypothesis: "A full-instance in-place restore stops every peer's replication receiver before its Job runs. When the " +
			"Job drops the user schemas and then fails, the queue is Held, the peers' receivers stay stopped across a " +
			"two-minute window, and each peer still holds the marker rows written before the restore.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#62-full-instance-restore-failing-after-its-drop",
		Timeout:           30 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          s62Precheck,
		Steps: []runner.Step{
			s62ConfigureProfile(),
			s62SeedMarker(),
			s62CreateBackup(),
			s62WriteAfterBackup(),
			s62StartRestoreAndCutStorage(),
			s62ObserveFailedAfterDrop(),
			s62VerifyPeersIntactAndHeld(),
		},
		Cleanup:        s62Cleanup,
		CleanupTimeout: 12 * time.Minute,
	}
}

func s62Precheck(ctx context.Context, env *runner.Env) error {
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
	if mfg.Status.RestoreReclones != nil {
		return fmt.Errorf("precheck: status.restoreReclones is set (%s); run ./playground/reset-mysql.sh", mfg.Status.RestoreReclones.Phase)
	}
	return nil
}

func s62ConfigureProfile() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "ensure RustFS bucket and configure isolated backup profile",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket); err != nil {
				return err
			}
			runStem := "s62-" + backupRunStamp(env)
			prefix := "e2e/" + s62ID + "/" + runStem
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
			return waitForBackupProfile(waitCtx, env, prefix)
		},
	}
}

func s62SeedMarker() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create marker schema with the in-backup row",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			for _, q := range []string{
				"CREATE DATABASE IF NOT EXISTS " + s62MarkerDB,
				"CREATE TABLE IF NOT EXISTS " + s62MarkerDB + ".marker (id INT PRIMARY KEY, note VARCHAR(64))",
				fmt.Sprintf("INSERT INTO %s.marker (id, note) VALUES (1, 'in-backup') ON DUPLICATE KEY UPDATE note=VALUES(note)", s62MarkerDB),
			} {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("seed stmt %q: %w", q, err)
				}
			}
			return waitForReplicaGTID(ctx, env, active, replica)
		},
	}
}

func s62CreateBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create MysqlBackup and wait Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := "s62-backup-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "backupName", name); err != nil {
				return err
			}
			if err := createMysqlBackup(ctx, env, name, s62ID); err != nil {
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

func s62WriteAfterBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "write a post-backup marker row and wait for every peer",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			if _, err := primary.Exec(ctx, fmt.Sprintf("INSERT INTO %s.marker (id, note) VALUES (2, 'after-backup')", s62MarkerDB)); err != nil {
				return fmt.Errorf("write post-backup marker: %w", err)
			}
			for _, s := range mfg.Spec.Sites {
				if s.Name == active {
					continue
				}
				if err := waitForReplicaGTID(ctx, env, active, s.Name); err != nil {
					return fmt.Errorf("peer %s did not receive the post-backup row: %w", s.Name, err)
				}
			}
			return ctxStash(ctx, env, "activeAtRestore", active)
		},
	}
}

func s62StartRestoreAndCutStorage() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "start a full-instance restore and scale RustFS to 0 while it fences",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeAtRestore")
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			var sites []string
			for _, s := range mfg.Spec.Sites {
				sites = append(sites, s.Name)
			}
			if err := clearRetainedInPlaceRestoreJobs(ctx, env, "s62-retained-restore-job", sites...); err != nil {
				env.Capture.Note("pre-attempt retained restore-job sweep: " + err.Error())
			}
			confirm := newConfirmToken(env.StartTime, 5*time.Minute)
			if err := ctxStash(ctx, env, "s62Confirm", confirm); err != nil {
				return err
			}
			spec := v1alpha1.RestoreInPlaceSpec{
				Confirm: confirm,
				Source:  v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: ctxFetch(env, "backupName")}},
			}
			if _, err := patchRestoreInPlace(ctx, env, spec); err != nil {
				return fmt.Errorf("patch full-instance restoreInPlace: %w", err)
			}
			// Cut storage before the Job exists: the Job then passes its
			// offline_mode check, drops every user schema, and fails reading
			// the dump. Waiting for the Job instead races a small dump's load.
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
			if _, err := waitRestoreInPlacePhase(waitCtx, env, v1alpha1.RestoreInPlaceFencing); err != nil {
				return fmt.Errorf("restore did not reach Fencing: %w", err)
			}
			pod, err := env.Chaos.ScaleRustFSToZero(ctx)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("restore of %s fencing; scaled RustFS to 0 (pod was %s)", active, pod))
			return nil
		},
	}
}

func s62ObserveFailedAfterDrop() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "restore Fails after its DROP step; the reclone queue is Held",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
			defer cancel()
			rip, err := waitRestoreInPlacePhase(waitCtx, env, v1alpha1.RestoreInPlaceFailed)
			if err != nil {
				return fmt.Errorf("restore did not reach terminal Failed: %w", err)
			}
			jobName := rip.JobName
			if jobName == "" {
				return fmt.Errorf("restore failed before its Job ran (%q); the scenario needs a failure after the DROP step", rip.Message)
			}
			_ = ctxStash(ctx, env, "restoreJobName", jobName)
			captureJobPodLogs(ctx, env, jobName, "s62-restore-job")
			dropped, err := s62JobLogContains(ctx, env, jobName, s62DropDoneMarker)
			if err != nil {
				return err
			}
			if !dropped {
				return fmt.Errorf("restore Job %s failed without logging %s; the failure must come after the DROP for this scenario", jobName, s62DropDoneMarker)
			}
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace, "restoreReclones Held", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				q := mfg.Status.RestoreReclones
				if q == nil {
					return false, "status.restoreReclones cleared", fmt.Errorf("the reclone queue was discarded after a restore that dropped the primary's schemas")
				}
				return q.Phase == v1alpha1.RestoreReclonesHeld, fmt.Sprintf("phase=%s pending=%v fenced=%q", q.Phase, q.Pending, q.FencedGtidExecuted), nil
			})
			return err
		},
	}
}

// s62JobLogContains reports whether any container of the Job's pods logged
// needle.
func s62JobLogContains(ctx context.Context, env *runner.Env, jobName, needle string) (bool, error) {
	pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + jobName})
	if err != nil {
		return false, fmt.Errorf("list restore Job pods: %w", err)
	}
	for _, pod := range pods.Items {
		for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			body, err := env.Kube.PodLogTailLines(ctx, env.Namespace, pod.Name, c.Name, 2000)
			if err == nil && strings.Contains(string(body), needle) {
				return true, nil
			}
		}
	}
	return false, nil
}

func s62VerifyPeersIntactAndHeld() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "every peer keeps its receiver stopped and both marker rows for two minutes",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := ctxFetch(env, "activeAtRestore")
			if mfg.Status.ActiveSite != active {
				return fmt.Errorf("status.activeSite changed %q -> %q", active, mfg.Status.ActiveSite)
			}
			var peers []string
			for _, s := range mfg.Spec.Sites {
				if s.Name != active {
					peers = append(peers, s.Name)
				}
			}
			deadline := time.Now().Add(2 * time.Minute)
			for {
				for _, peer := range peers {
					if err := s62CheckPeer(ctx, env, peer); err != nil {
						return err
					}
				}
				if time.Now().After(deadline) {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Second):
				}
			}
			env.Capture.Note(fmt.Sprintf("peers %v kept their receivers stopped and both marker rows across two minutes", peers))
			return nil
		},
	}
}

func s62CheckPeer(ctx context.Context, env *runner.Env, peer string) error {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, peer, env.Creds)
	if err != nil {
		return fmt.Errorf("open peer %s: %w", peer, err)
	}
	defer c.Close()
	// performance_schema reports the receiver as ON, CONNECTING or OFF; a
	// connecting receiver would stream the DROPs as soon as it connects.
	svc, err := c.ScalarString(ctx, "SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1")
	if err != nil {
		return fmt.Errorf("peer %s receiver state: %w", peer, err)
	}
	if svc != "OFF" {
		return fmt.Errorf("peer %s replication receiver is %s, want OFF while the queue is Held", peer, svc)
	}
	rows, err := c.ScalarInt(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s.marker WHERE id IN (1, 2)", s62MarkerDB))
	if err != nil {
		return fmt.Errorf("peer %s lost the marker schema (the restore's DROP reached it): %w", peer, err)
	}
	if rows != 2 {
		return fmt.Errorf("peer %s has %d of the 2 marker rows", peer, rows)
	}
	return nil
}

func s62Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	if err := removeRestoreInPlaceSpec(ctx, env); err != nil {
		errs = append(errs, fmt.Errorf("remove restoreInPlace spec: %w", err))
	}
	if err := deleteJobIfPresent(ctx, env, ctxFetch(env, "restoreJobName")); err != nil {
		errs = append(errs, fmt.Errorf("delete restore job: %w", err))
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		errs = append(errs, err)
	} else {
		var sites []string
		for _, s := range mfg.Spec.Sites {
			sites = append(sites, s.Name)
		}
		if serr := clearRetainedInPlaceRestoreJobs(ctx, env, "s62-cleanup-restore-job", sites...); serr != nil {
			errs = append(errs, serr)
		}
		// The primary is damaged and the peers are held; the next run-all
		// scenario starts from a reset (ResetBeforeRunAll). Release the hold
		// so a single run leaves a replicating group behind.
		if mfg.Status.RestoreReclones != nil {
			if perr := env.Kube.PatchMFGStatusNamed(ctx, env.Namespace, env.FG,
				[]pgkube.JSONPatchOp{{Op: "remove", Path: "/status/restoreReclones"}}); perr != nil {
				errs = append(errs, fmt.Errorf("remove status.restoreReclones: %w", perr))
			}
		}
	}
	if err := deleteBackupCRs(ctx, env, ctxFetch(env, "backupName"), ""); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("s62 cleanup: %w", errors.Join(errs...))
	}
	return nil
}
