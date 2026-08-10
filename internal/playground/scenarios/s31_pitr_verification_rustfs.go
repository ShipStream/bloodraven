package scenarios

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario31PITRVerificationRustFS())
}

const (
	s31ID     = "31-pitr-verification-rustfs"
	s31DBName = "chaos_s31_pitr"
)

func scenario31PITRVerificationRustFS() runner.Scenario {
	return runner.Scenario{
		ID:    s31ID,
		Title: "PITR verification replays RustFS binlogs to timestamp",
		Hypothesis: "With PITR enabled against RustFS, a pinned MysqlBackupVerification can restore a full dump, " +
			"replay archived binlogs to a timestamp, include pre-target marker rows, and exclude post-target rows.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#31-pitr-verification-against-rustfs",
		Timeout:  24 * time.Minute,
		Precheck: assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s31EnsureBucket(),
			s31ConfigureProfile(),
			s31WaitArchiverReady(),
			s31SeedBaseline(),
			s31CreateBackup(),
			s31WaitBackupSucceeded(),
			s31InsertPITRMarkers(),
			s31ForceBinlogArchive(),
			s31CreateVerification(),
			s31WaitVerificationSucceeded(),
		},
		Cleanup: s31Cleanup,
	}
}

func s31EnsureBucket() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "ensure RustFS backup bucket exists",
		Do: func(ctx context.Context, env *runner.Env) error {
			env.Capture.Note(fmt.Sprintf("RustFS endpoint=%s bucket=%s credentialsSecret=%s", backupE2EEndpoint, backupE2EBucket, backupE2ECredsSecret))
			return env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket)
		},
	}
}

func s31ConfigureProfile() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure RustFS backup profile with PITR",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := "s31-" + backupRunStamp(env)
			prefix := "e2e/" + s31ID + "/" + runStem
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

func s31WaitArchiverReady() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait PITR rollout and archiver readiness",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "PITR spec rollout", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				ready := conditionTrueForGeneration(mfg.Status.Conditions, "Ready", mfg.Generation)
				pitrSpec := mfg.Spec.Backup != nil && mfg.Spec.Backup.PITR != nil && mfg.Spec.Backup.PITR.Enabled && mfg.Spec.Backup.PITR.ProfileName == backupE2EProfile
				return pitrSpec && ready && mfg.Status.UpdatePhase == "" && mfg.Status.ActiveSite != "", fmt.Sprintf("pitrSpec=%v ready=%v updatePhase=%q active=%q", pitrSpec, ready, mfg.Status.UpdatePhase, mfg.Status.ActiveSite), nil
			})
			if err != nil {
				return err
			}
			if _, err := env.Logs("operator"); err != nil {
				env.Capture.Note("open operator logs failed: " + err.Error())
			}
			wantPrefix := path.Join(ctxFetch(env, "backupPrefix"), "binlogs")
			active, st, err := waitForActiveArchiverReady(waitCtx, env, wantPrefix)
			if err != nil {
				return err
			}
			if _, err := env.Logs("sidecar:" + active); err != nil {
				env.Capture.Note("open sidecar logs failed: " + err.Error())
			}
			env.Capture.Note("active archiver ready: " + archiverSummary(st))
			if _, replica, err := activeAndReplica(ctx, env); err == nil {
				if passive, err := env.Sidecar(replica); err == nil {
					if pst, err := passive.ArchiverStatus(ctx); err == nil {
						env.Capture.Note("passive archiver snapshot: " + archiverSummary(pst))
					}
				}
			}
			return ctxStash(ctx, env, "activeSite", active)
		},
	}
}

func waitForActiveArchiverReady(ctx context.Context, env *runner.Env, wantPrefix string) (string, *pgsidecar.ArchiverStatusResponse, error) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			last = err.Error()
		} else if mfg.Status.ActiveSite == "" {
			last = "active site is empty"
		} else {
			active := mfg.Status.ActiveSite
			probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, active)
			if err != nil {
				last = fmt.Sprintf("active=%s: %v", active, err)
			} else {
				st, statusErr := probe.ArchiverStatus(ctx)
				probe.Close()
				if statusErr != nil {
					last = fmt.Sprintf("active=%s: %v", active, statusErr)
				} else {
					last = fmt.Sprintf("active=%s %s", active, archiverSummary(st))
					if st.Enabled && st.Primary && st.StorageType == string(v1alpha1.BackupStorageS3) && strings.Trim(st.ManifestPrefix, "/") == strings.Trim(wantPrefix, "/") && st.LastError == "" && !st.LastScanAt.IsZero() {
						return active, st, nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return "", nil, fmt.Errorf("wait for active archiver readiness: %w (last: %s)", ctx.Err(), last)
		case <-tick.C:
		}
	}
}

func waitForArchiverReady(ctx context.Context, env *runner.Env, site, wantPrefix string) (*pgsidecar.ArchiverStatusResponse, error) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
		if err != nil {
			last = err.Error()
		} else {
			st, err := probe.ArchiverStatus(ctx)
			probe.Close()
			if err != nil {
				last = err.Error()
			} else {
				last = archiverSummary(st)
				if st.Enabled && st.Primary && st.StorageType == string(v1alpha1.BackupStorageS3) && strings.Trim(st.ManifestPrefix, "/") == strings.Trim(wantPrefix, "/") && st.LastError == "" && !st.LastScanAt.IsZero() {
					return st, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for archiver readiness: %w (last: %s)", ctx.Err(), last)
		case <-tick.C:
		}
	}
}

func s31SeedBaseline() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed baseline row before full backup",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active mysql %s: %w", active, err)
			}
			defer primary.Close()
			stmts := []string{
				"CREATE DATABASE IF NOT EXISTS " + s31DBName,
				"DROP TABLE IF EXISTS " + s31DBName + ".marker",
				"CREATE TABLE " + s31DBName + ".marker (id INT PRIMARY KEY, run_id VARCHAR(64), phase VARCHAR(32), payload VARCHAR(128), created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))",
			}
			for _, q := range stmts {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("marker schema %q: %w", q, err)
				}
			}
			if _, err := primary.Exec(ctx, "INSERT INTO "+s31DBName+".marker (id, run_id, phase, payload) VALUES (?, ?, ?, ?)", 1, runStem, "baseline", "in-full-backup"); err != nil {
				return fmt.Errorf("insert baseline row: %w", err)
			}
			if err := waitForReplicaGTID(ctx, env, active, replica); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("seeded %s.marker baseline on active=%s replica=%s runStem=%s", s31DBName, active, replica, runStem))
			return nil
		},
	}
}

func s31CreateBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create MysqlBackup",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := "s31-backup-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "backupName", name); err != nil {
				return err
			}
			return createMysqlBackup(ctx, env, name, s31ID)
		},
	}
}

func s31WaitBackupSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait MysqlBackup Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := ctxFetch(env, "backupName")
			prefix := ctxFetch(env, "backupPrefix")
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			b, err := waitForBackupPhase(waitCtx, env, name, v1alpha1.BackupPhaseSucceeded)
			if err != nil {
				return err
			}
			wantLocation := prefix + "/" + name
			if b.Status.Location != wantLocation {
				return fmt.Errorf("backup location %q, want %q", b.Status.Location, wantLocation)
			}
			if b.Status.StorageType != v1alpha1.BackupStorageS3 {
				return fmt.Errorf("backup storageType=%q, want S3", b.Status.StorageType)
			}
			if b.Status.JobName == "" {
				return fmt.Errorf("backup status.jobName is empty")
			}
			if err := ctxStash(ctx, env, "backupUID", string(b.UID)); err != nil {
				return err
			}
			env.Capture.Note("backup succeeded: " + backupStatusSummary(b))
			return nil
		},
	}
}

func s31InsertPITRMarkers() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "insert PITR marker rows around target timestamp",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active mysql %s: %w", active, err)
			}
			defer primary.Close()
			insert := "INSERT INTO " + s31DBName + ".marker (id, run_id, phase, payload) VALUES (?, ?, ?, ?)"
			if _, err := primary.Exec(ctx, insert, 2, runStem, "before-target", "should-replay"); err != nil {
				return fmt.Errorf("insert before-target row: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			target, err := primary.ScalarString(ctx, "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')")
			if err != nil {
				return fmt.Errorf("capture PITR target timestamp: %w", err)
			}
			if err := ctxStash(ctx, env, "pitrTargetTimestamp", target); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			if _, err := primary.Exec(ctx, insert, 3, runStem, "after-target", "must-not-replay"); err != nil {
				return fmt.Errorf("insert after-target row: %w", err)
			}
			if err := waitForReplicaGTID(ctx, env, active, replica); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("PITR markers inserted target=%s active=%s replica=%s", target, active, replica))
			return nil
		},
	}
}

func s31ForceBinlogArchive() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "force binlog archival",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, _, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, active)
			if err != nil {
				return fmt.Errorf("open sidecar %s: %w", active, err)
			}
			defer probe.Close()
			before, err := probe.ArchiverStatus(ctx)
			if err != nil {
				return fmt.Errorf("read pre-flush archiver status: %w", err)
			}
			env.Capture.Note("pre-flush archiver: " + archiverSummary(before))
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active mysql %s: %w", active, err)
			}
			defer primary.Close()
			if _, err := primary.Exec(ctx, "FLUSH BINARY LOGS"); err != nil {
				return fmt.Errorf("flush binary logs: %w", err)
			}
			target, err := time.Parse(time.RFC3339Nano, ctxFetch(env, "pitrTargetTimestamp"))
			if err != nil {
				return fmt.Errorf("parse PITR target timestamp: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			st, err := waitForArchiveCoverage(waitCtx, env, probe, before, target)
			if err != nil {
				return err
			}
			env.Capture.Note("post-flush archiver: " + archiverSummary(st))
			statusCtx, statusCancel := context.WithTimeout(ctx, 20*time.Second)
			defer statusCancel()
			mfg, err := env.Wait.UntilCR(statusCtx, env.Namespace, "PITR status archive coverage", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				if mfg.Status.PITR == nil {
					return false, "status.pitr is nil", nil
				}
				p := mfg.Status.PITR
				covered := p.NewestArchivedTime != nil && !p.NewestArchivedTime.Time.Before(target.Add(-time.Second))
				ok := p.Enabled && p.ProfileName == backupE2EProfile && p.ArchivedFileCount > 0 && covered && p.Message == ""
				return ok, fmt.Sprintf("enabled=%v profile=%s files=%d newest=%v covered=%v message=%q", p.Enabled, p.ProfileName, p.ArchivedFileCount, p.NewestArchivedTime, covered, p.Message), nil
			})
			if err != nil {
				env.Capture.Note("MFG PITR status did not catch up after sidecar coverage; continuing with verification proof: " + err.Error())
				return nil
			}
			if mfg.Status.PITR != nil {
				env.Capture.Note(fmt.Sprintf("MFG PITR status: enabled=%v profile=%s files=%d newest=%v message=%q", mfg.Status.PITR.Enabled, mfg.Status.PITR.ProfileName, mfg.Status.PITR.ArchivedFileCount, mfg.Status.PITR.NewestArchivedTime, mfg.Status.PITR.Message))
			}
			return nil
		},
	}
}

func waitForArchiveCoverage(ctx context.Context, env *runner.Env, probe *pgsidecar.Probe, before *pgsidecar.ArchiverStatusResponse, target time.Time) (*pgsidecar.ArchiverStatusResponse, error) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		st, err := probe.ArchiverStatus(ctx)
		if err != nil {
			last = err.Error()
		} else {
			last = archiverSummary(st)
			advanced := st.FilesArchived > before.FilesArchived || st.ManifestFileCount > before.ManifestFileCount
			covered := !st.NewestArchivedTime.IsZero() && !st.NewestArchivedTime.Before(target.Add(-time.Second))
			if advanced && st.BacklogFiles == 0 && st.LastError == "" && covered {
				return st, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for binlog archive coverage: %w (target=%s last=%s)", ctx.Err(), target.Format(time.RFC3339Nano), last)
		case <-tick.C:
		}
	}
}

func s31CreateVerification() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create pinned PITR MysqlBackupVerification",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := "s31-verify-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "verificationName", name); err != nil {
				return err
			}
			runStem := ctxFetch(env, "backupRunStem")
			query := "SELECT IF(SUM(phase='baseline') = 1 AND SUM(phase='before-target') = 1 AND SUM(phase='after-target') = 0, 1, 0) FROM " + s31DBName + ".marker WHERE run_id=" + quoteSQLString(runStem)
			pit := &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: ctxFetch(env, "pitrTargetTimestamp")}
			return createMysqlBackupVerification(ctx, env, name, ctxFetch(env, "backupName"), s31ID, pit, query, 1)
		},
	}
}

func s31WaitVerificationSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "wait PITR MysqlBackupVerification Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := ctxFetch(env, "verificationName")
			backupName := ctxFetch(env, "backupName")
			backupUID := ctxFetch(env, "backupUID")
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			v, err := waitForVerificationPhase(waitCtx, env, name, v1alpha1.VerificationPhaseSucceeded)
			if err != nil {
				return err
			}
			if v.Status.BackupRef == nil || v.Status.BackupRef.Name != backupName {
				return fmt.Errorf("verification backupRef=%v, want name=%s", v.Status.BackupRef, backupName)
			}
			if backupUID != "" && v.Status.BackupRef.UID != "" && v.Status.BackupRef.UID != backupUID {
				return fmt.Errorf("verification backupRef UID=%q, want %q", v.Status.BackupRef.UID, backupUID)
			}
			if v.Status.SanityCheck == nil || !v.Status.SanityCheck.Ran || v.Status.SanityCheck.ResultRow != "1" {
				return fmt.Errorf("verification sanity=%v, want ran=true resultRow=1", v.Status.SanityCheck)
			}
			if v.Status.ReplayedThroughBinlog == nil {
				return fmt.Errorf("verification replay mark is nil for PITR run")
			}
			if !conditionTrue(v.Status.Conditions, "Verified") {
				return fmt.Errorf("verification missing Verified=True condition: %s", conditionsSummary(v.Status.Conditions))
			}
			env.Capture.Note("PITR verification succeeded: " + verificationStatusSummary(v))
			return nil
		},
	}
}

func s31Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	if err := deleteBackupCRs(ctx, env, ctxFetch(env, "backupName"), ctxFetch(env, "verificationName")); err != nil {
		errs = append(errs, err)
	}
	if err := dropMarkerSchemaAndReplicate(ctx, env, s31DBName); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	if _, err := env.Wait.UntilCR(ctx, env.Namespace, "s31 cleanup healthy baseline", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ready := false
		for _, c := range mfg.Status.Conditions {
			if c.Type == "Ready" {
				ready = c.Status == metav1.ConditionTrue
			}
		}
		wantPITR, wantProfile, err := originalPITRExpected(env)
		if err != nil {
			return false, "", err
		}
		pitrRestored := mfg.Status.PITR == nil || (!mfg.Status.PITR.Enabled && !wantPITR)
		if wantPITR {
			pitrRestored = mfg.Status.PITR != nil && mfg.Status.PITR.Enabled && mfg.Status.PITR.ProfileName == wantProfile
		}
		return ready && mfg.Status.ActiveSite != "" && mfg.Status.UpdatePhase == "" && pitrRestored, fmt.Sprintf("ready=%v active=%q updatePhase=%q pitrRestored=%v wantPITR=%v wantProfile=%q pitr=%v", ready, mfg.Status.ActiveSite, mfg.Status.UpdatePhase, pitrRestored, wantPITR, wantProfile, mfg.Status.PITR), nil
	}); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("s31 cleanup: %v", errs)
	}
	return nil
}
