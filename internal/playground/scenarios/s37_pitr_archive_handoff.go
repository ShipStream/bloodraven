package scenarios

import (
	"context"
	"fmt"
	"path"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario37PITRArchiveHandoff())
}

const (
	s37ID     = "37-pitr-archive-handoff-across-failover"
	s37DBName = "chaos_s37_pitr"
)

// scenario37PITRArchiveHandoff proves PITR binlog archiving survives an
// emergency failover. It archives marker A on the old active, fails over,
// archives marker B on the new active (each under its own per-site manifest in
// the same prefix), then verifies a timestamp PITR replay that must include the
// baseline plus A and B and exclude a post-target marker C. Destructive
// (failover + backup/PITR state), so it runs ResetBeforeRunAll.
func scenario37PITRArchiveHandoff() runner.Scenario {
	return runner.Scenario{
		ID:    s37ID,
		Title: "PITR archive handoff across failover replays markers from both primaries",
		Hypothesis: "With PITR enabled, marker A archived on the old active and marker B archived on the new active " +
			"(each in its own per-site manifest under the same prefix) are both restorable to a timestamp after B and " +
			"before C: a pinned timestamp verification includes baseline+A+B and excludes C, proving archive " +
			"continuity across an emergency failover.",
		Risk:              "medium",
		DocLink:           "playground/chaos-scenarios.md#37-pitr-archive-handoff-across-failover",
		Timeout:           30 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s37ConfigurePITR(),
			s37WaitArchiverReady(),
			s37SeedAndBackup(),
			s37MarkerABeforeFailover(),
			s37Failover(),
			s37MarkerBAfterFailover(),
			s37TargetAndMarkerC(),
			s37VerifyPerSiteManifests(),
			s37VerifyTimestampReplay(),
		},
		Cleanup: s37Cleanup,
	}
}

func s37ConfigurePITR() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "ensure bucket, configure RustFS PITR profile",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket); err != nil {
				return err
			}
			runStem := "s37-" + backupRunStamp(env)
			prefix := "e2e/" + s37ID + "/" + runStem
			if err := ctxStash(ctx, env, "backupRunStem", runStem); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "backupPrefix", prefix); err != nil {
				return err
			}
			if err := patchBackupSpec(ctx, env, backupProfileSpec(prefix, true)); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			return waitForBackupProfile(waitCtx, env, prefix, true)
		},
	}
}

func s37WaitArchiverReady() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait PITR rollout and active archiver readiness",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "PITR spec rollout", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				ready := conditionTrueForGeneration(mfg.Status.Conditions, "Ready", mfg.Generation)
				pitrSpec := mfg.Spec.Backup != nil && mfg.Spec.Backup.PITR != nil && mfg.Spec.Backup.PITR.Enabled && mfg.Spec.Backup.PITR.ProfileName == backupE2EProfile
				return pitrSpec && ready && mfg.Status.UpdatePhase == "" && mfg.Status.ActiveSite != "",
					fmt.Sprintf("pitrSpec=%v ready=%v active=%q", pitrSpec, ready, mfg.Status.ActiveSite), nil
			})
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			wantPrefix := path.Join(ctxFetch(env, "backupPrefix"), "binlogs")
			st, err := waitForArchiverReady(waitCtx, env, active, wantPrefix)
			if err != nil {
				return err
			}
			env.Capture.Note("active archiver ready: " + archiverSummary(st))
			return nil
		},
	}
}

func s37SeedAndBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed baseline marker and take a full backup",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			stmts := []string{
				"CREATE DATABASE IF NOT EXISTS " + s37DBName,
				"DROP TABLE IF EXISTS " + s37DBName + ".marker",
				"CREATE TABLE " + s37DBName + ".marker (id INT PRIMARY KEY, run_id VARCHAR(64), phase VARCHAR(16), site VARCHAR(16), created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))",
			}
			for _, q := range stmts {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("marker schema %q: %w", q, err)
				}
			}
			if _, err := primary.Exec(ctx, "INSERT INTO "+s37DBName+".marker (id, run_id, phase, site) VALUES (?,?,?,?)", 1, runStem, "baseline", active); err != nil {
				return fmt.Errorf("insert baseline: %w", err)
			}
			if err := waitForReplicaGTID(ctx, env, active, replica); err != nil {
				return err
			}
			name := "s37-backup-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "backupName", name); err != nil {
				return err
			}
			if err := createMysqlBackup(ctx, env, name, s37ID); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			b, err := waitForBackupPhase(waitCtx, env, name, v1alpha1.BackupPhaseSucceeded)
			if err != nil {
				return err
			}
			_ = ctxStash(ctx, env, "backupUID", string(b.UID))
			env.Capture.Note("baseline seeded and backup succeeded on active=" + active)
			return nil
		},
	}
}

func s37MarkerABeforeFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "insert marker A on old active and archive it",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			active, _, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "oldActive", active); err != nil {
				return err
			}
			if err := s37InsertMarker(ctx, env, active, 2, runStem, "A", active); err != nil {
				return err
			}
			if err := s37FlushAndArchive(ctx, env, active); err != nil {
				return fmt.Errorf("archive marker A on %s: %w", active, err)
			}
			env.Capture.Note("marker A archived on old active " + active)
			return nil
		},
	}
}

func s37Failover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "emergency failover: scale old active to 0, wait new active archiver primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			oldActive := ctxFetch(env, "oldActive")
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			peer, err := PeerOf(mfg, oldActive)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "newActive", peer); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("scaling old active %s to 0 to fail over to %s", oldActive, peer))
			if err := env.Chaos.ScaleSiteToZero(ctx, oldActive); err != nil {
				return err
			}
			// Wait for the peer to become the active site.
			foCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			if _, err := env.Wait.UntilCR(foCtx, env.Namespace, fmt.Sprintf("status.activeSite==%s after failover", peer),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					return mfg.Status.ActiveSite == peer, "activeSite=" + mfg.Status.ActiveSite, nil
				}); err != nil {
				return fmt.Errorf("failover to %s did not complete: %w", peer, err)
			}
			// Wait until the new active's sidecar archiver reports primary=true
			// using the SAME manifest prefix — archive continuity.
			wantPrefix := path.Join(ctxFetch(env, "backupPrefix"), "binlogs")
			archCtx, archCancel := context.WithTimeout(ctx, 4*time.Minute)
			defer archCancel()
			st, err := waitForArchiverReady(archCtx, env, peer, wantPrefix)
			if err != nil {
				return fmt.Errorf("new active %s archiver did not take over on the same prefix: %w", peer, err)
			}
			env.Capture.Note("new active archiver primary: " + archiverSummary(st))
			return nil
		},
	}
}

func s37MarkerBAfterFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "insert marker B on new active and archive it",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			newActive := ctxFetch(env, "newActive")
			if err := s37InsertMarker(ctx, env, newActive, 3, runStem, "B", newActive); err != nil {
				return err
			}
			if err := s37FlushAndArchive(ctx, env, newActive); err != nil {
				return fmt.Errorf("archive marker B on %s: %w", newActive, err)
			}
			env.Capture.Note("marker B archived on new active " + newActive)
			return nil
		},
	}
}

func s37TargetAndMarkerC() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "capture PITR target after B, insert + archive marker C after target",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			newActive := ctxFetch(env, "newActive")
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, newActive, env.Creds)
			if err != nil {
				return fmt.Errorf("open new active %s: %w", newActive, err)
			}
			defer primary.Close()

			// A short pause so the target timestamp sits strictly after B and
			// strictly before C.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			target, err := primary.ScalarString(ctx, "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')")
			if err != nil {
				return fmt.Errorf("capture PITR target: %w", err)
			}
			if err := ctxStash(ctx, env, "pitrTarget", target); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			if err := s37InsertMarker(ctx, env, newActive, 4, runStem, "C", newActive); err != nil {
				return err
			}
			if err := s37FlushAndArchive(ctx, env, newActive); err != nil {
				return fmt.Errorf("archive marker C on %s: %w", newActive, err)
			}
			env.Capture.Note("PITR target=" + target + "; marker C archived after target on " + newActive)
			return nil
		},
	}
}

func s37VerifyPerSiteManifests() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "both per-site binlog manifests exist under the same prefix (archive continuity)",
		Do: func(ctx context.Context, env *runner.Env) error {
			binlogPrefix := path.Join(ctxFetch(env, "backupPrefix"), "binlogs")
			oldActive := ctxFetch(env, "oldActive")
			newActive := ctxFetch(env, "newActive")
			for _, site := range []string{oldActive, newActive} {
				m, found, err := readSiteBinlogManifest(ctx, env, backupE2EBucket, binlogPrefix, site)
				if err != nil {
					return err
				}
				if !found || m == nil || len(m.Files) == 0 {
					return fmt.Errorf("manifest for site %s missing or empty under %s (archive continuity broken)", site, binlogPrefix)
				}
				env.Capture.Note(fmt.Sprintf("manifest-%s.json: %d files, newestLastEvent=%s", site, len(m.Files), manifestNewestLastEvent(m).Format(time.RFC3339Nano)))
			}
			env.Capture.Note(fmt.Sprintf("both per-site manifests present under %s: manifest-%s.json and manifest-%s.json", binlogPrefix, oldActive, newActive))
			return nil
		},
	}
}

func s37VerifyTimestampReplay() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "timestamp PITR verification includes baseline+A+B, excludes C",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			name := "s37-verify-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "verificationName", name); err != nil {
				return err
			}
			query := pitrHandoffSanityQuery(s37DBName, runStem)
			pit := &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: ctxFetch(env, "pitrTarget")}
			if err := createMysqlBackupVerification(ctx, env, name, ctxFetch(env, "backupName"), s37ID, pit, query, 1); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			v, err := waitForVerificationPhase(waitCtx, env, name, v1alpha1.VerificationPhaseSucceeded)
			if err != nil {
				return err
			}
			if v.Status.SanityCheck == nil || !v.Status.SanityCheck.Ran || v.Status.SanityCheck.ResultRow != "1" {
				return fmt.Errorf("sanity check did not pass baseline+A+B present / C absent: %v", v.Status.SanityCheck)
			}
			if v.Status.ReplayedThroughBinlog == nil {
				return fmt.Errorf("verification did not replay binlogs (ReplayedThroughBinlog nil) — PITR handoff not exercised")
			}
			env.Capture.Note("PITR handoff verification succeeded: " + verificationStatusSummary(v))
			return nil
		},
	}
}

// s37InsertMarker inserts a phase marker row on the given site.
func s37InsertMarker(ctx context.Context, env *runner.Env, site string, id int, runStem, phase, siteTag string) error {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s for marker %s: %w", site, phase, err)
	}
	defer c.Close()
	if _, err := c.Exec(ctx, "INSERT INTO "+s37DBName+".marker (id, run_id, phase, site) VALUES (?,?,?,?)", id, runStem, phase, siteTag); err != nil {
		return fmt.Errorf("insert marker %s on %s: %w", phase, site, err)
	}
	return nil
}

// s37FlushAndArchive flushes binary logs on a site and waits for the sidecar
// archiver to advance (new files uploaded, backlog drained, no error).
func s37FlushAndArchive(ctx context.Context, env *runner.Env, site string) error {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return fmt.Errorf("open sidecar %s: %w", site, err)
	}
	defer probe.Close()
	before, err := probe.ArchiverStatus(ctx)
	if err != nil {
		return fmt.Errorf("read pre-flush archiver status: %w", err)
	}
	primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open mysql %s: %w", site, err)
	}
	defer primary.Close()
	if _, err := primary.Exec(ctx, "FLUSH BINARY LOGS"); err != nil {
		return fmt.Errorf("flush binary logs: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		st, err := probe.ArchiverStatus(waitCtx)
		if err != nil {
			last = err.Error()
		} else {
			last = archiverSummary(st)
			advanced := st.FilesArchived > before.FilesArchived || st.ManifestFileCount > before.ManifestFileCount
			if advanced && st.BacklogFiles == 0 && st.LastError == "" {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("archiver did not advance after flush on %s: %w (last: %s)", site, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

func s37Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	if err := deleteBackupCRs(ctx, env, ctxFetch(env, "backupName"), ctxFetch(env, "verificationName")); err != nil {
		errs = append(errs, err)
	}
	// Old active is scaled back to 1 by the ScaleSiteToZero reverter (runs
	// before this Cleanup), so replication can drain the schema drop.
	if err := dropMarkerSchemaAndReplicate(ctx, env, s37DBName); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	// Wait for a healthy baseline, absorbing any anti-flap cooldown from the
	// emergency failover and confirming PITR spec was restored.
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "s37 cleanup healthy baseline",
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			ready := conditionTrue(mfg.Status.Conditions, "Ready")
			wantPITR, wantProfile, err := originalPITRExpected(env)
			if err != nil {
				return false, "", err
			}
			pitrRestored := mfg.Status.PITR == nil || (!mfg.Status.PITR.Enabled && !wantPITR)
			if wantPITR {
				pitrRestored = mfg.Status.PITR != nil && mfg.Status.PITR.Enabled && mfg.Status.PITR.ProfileName == wantProfile
			}
			var cooldown string
			if mfg.Spec.FailoverCooldown != nil && mfg.Status.LastFailover != nil {
				if rem := mfg.Spec.FailoverCooldown.Duration - time.Since(mfg.Status.LastFailover.Time); rem > 0 {
					cooldown = rem.Round(time.Second).String()
				}
			}
			done := ready && mfg.Status.ActiveSite != "" && mfg.Status.UpdatePhase == "" && pitrRestored && cooldown == ""
			return done, fmt.Sprintf("ready=%v active=%q pitrRestored=%v cooldown=%q", ready, mfg.Status.ActiveSite, pitrRestored, cooldown), nil
		}); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("s37 cleanup: %v", errs)
	}
	return nil
}
