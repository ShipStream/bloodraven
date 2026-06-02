package scenarios

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario30BackupVerificationRustFS())
}

const (
	s30ID     = "30-backup-verification-rustfs"
	s30DBName = "chaos_s30_backup"
)

func scenario30BackupVerificationRustFS() runner.Scenario {
	return runner.Scenario{
		ID:    s30ID,
		Title: "Backup verification restores RustFS backup",
		Hypothesis: "A real MysqlBackup written to the playground RustFS bucket can be restored by a pinned " +
			"MysqlBackupVerification and the restored MySQL contains marker rows from the dump.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#30-backup-verification-against-rustfs",
		Timeout:  18 * time.Minute,
		Precheck: assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s30EnsureBucket(),
			s30ConfigureProfile(),
			s30SeedMarkers(),
			s30CreateBackup(),
			s30WaitBackupSucceeded(),
			s30CreateVerification(),
			s30WaitVerificationSucceeded(),
		},
		Cleanup: s30Cleanup,
	}
}

func s30EnsureBucket() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "ensure RustFS backup bucket exists",
		Do: func(ctx context.Context, env *runner.Env) error {
			env.Capture.Note(fmt.Sprintf("RustFS endpoint=%s bucket=%s credentialsSecret=%s", backupE2EEndpoint, backupE2EBucket, backupE2ECredsSecret))
			return env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket)
		},
	}
}

func s30ConfigureProfile() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure RustFS backup profile",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := "s30-" + backupRunStamp(env)
			prefix := "e2e/" + s30ID + "/" + runStem
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

func s30SeedMarkers() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed backup marker rows",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := ctxFetch(env, "backupRunStem")
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			primary, err := env.MySQL(active)
			if err != nil {
				return fmt.Errorf("open active mysql %s: %w", active, err)
			}
			stmts := []string{
				"CREATE DATABASE IF NOT EXISTS " + s30DBName,
				"DROP TABLE IF EXISTS " + s30DBName + ".marker",
				"CREATE TABLE " + s30DBName + ".marker (id INT PRIMARY KEY, run_id VARCHAR(64), phase VARCHAR(32), payload VARCHAR(128), created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))",
			}
			for _, q := range stmts {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("marker schema %q: %w", q, err)
				}
			}
			insert := "INSERT INTO " + s30DBName + ".marker (id, run_id, phase, payload) VALUES (?, ?, ?, ?)"
			if _, err := primary.Exec(ctx, insert, 1, runStem, "baseline", "present-in-full-backup"); err != nil {
				return fmt.Errorf("insert baseline row 1: %w", err)
			}
			if _, err := primary.Exec(ctx, insert, 2, runStem, "baseline", "second-row"); err != nil {
				return fmt.Errorf("insert baseline row 2: %w", err)
			}
			if err := waitForReplicaGTID(ctx, env, active, replica); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("seeded %s.marker on active=%s replica=%s runStem=%s", s30DBName, active, replica, runStem))
			return nil
		},
	}
}

func s30CreateBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create MysqlBackup",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := "s30-backup-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "backupName", name); err != nil {
				return err
			}
			return createMysqlBackup(ctx, env, name, s30ID)
		},
	}
}

func s30WaitBackupSucceeded() runner.Step {
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

func s30CreateVerification() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create pinned MysqlBackupVerification",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := "s30-verify-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "verificationName", name); err != nil {
				return err
			}
			runStem := ctxFetch(env, "backupRunStem")
			query := "SELECT COUNT(*) FROM " + s30DBName + ".marker WHERE run_id=" + quoteSQLString(runStem) + " AND phase='baseline'"
			return createMysqlBackupVerification(ctx, env, name, ctxFetch(env, "backupName"), s30ID, nil, query, 2)
		},
	}
}

func s30WaitVerificationSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "wait MysqlBackupVerification Succeeded",
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
			if v.Status.SanityCheck == nil || !v.Status.SanityCheck.Ran || v.Status.SanityCheck.ResultRow != "2" {
				return fmt.Errorf("verification sanity=%v, want ran=true resultRow=2", v.Status.SanityCheck)
			}
			if !conditionTrue(v.Status.Conditions, "Verified") {
				return fmt.Errorf("verification missing Verified=True condition: %s", conditionsSummary(v.Status.Conditions))
			}
			env.Capture.Note("verification succeeded: " + verificationStatusSummary(v))
			return nil
		},
	}
}

func s30Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	if err := deleteBackupCRs(ctx, env, ctxFetch(env, "backupName"), ctxFetch(env, "verificationName")); err != nil {
		errs = append(errs, err)
	}
	if err := dropMarkerSchemaAndReplicate(ctx, env, s30DBName); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	if _, err := env.Wait.UntilCR(ctx, env.Namespace, "s30 cleanup healthy baseline", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ready := false
		for _, c := range mfg.Status.Conditions {
			if c.Type == "Ready" {
				ready = c.Status == metav1.ConditionTrue
			}
		}
		return ready && mfg.Status.ActiveSite != "" && mfg.Status.UpdatePhase == "", fmt.Sprintf("ready=%v active=%q updatePhase=%q", ready, mfg.Status.ActiveSite, mfg.Status.UpdatePhase), nil
	}); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("s30 cleanup: %v", errs)
	}
	return nil
}
