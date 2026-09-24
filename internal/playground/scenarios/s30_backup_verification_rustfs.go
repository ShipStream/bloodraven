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
	runner.Register(scenario30BackupVerificationRustFS(s30Plain))
	runner.Register(scenario30BackupVerificationRustFS(s30Encrypted))
}

// s30Variant parameterizes scenario 30. The encrypted variant runs the
// same dump -> verify round trip through the operator-image
// `encrypt-upload` / `decrypt-download` containers, which the plain
// variant never renders.
type s30Variant struct {
	id        string
	short     string // CR name / run-stem prefix
	dbName    string
	title     string
	docLink   string
	encrypted bool
}

var (
	s30Plain = s30Variant{
		id:      "30-backup-verification-rustfs",
		short:   "s30",
		dbName:  "chaos_s30_backup",
		title:   "Backup verification restores RustFS backup",
		docLink: "playground/chaos-scenarios.md#30-backup-verification-against-rustfs",
	}
	s30Encrypted = s30Variant{
		id:        "30-encrypted-backup-verification-rustfs",
		short:     "s30e",
		dbName:    "chaos_s30_encrypted_backup",
		title:     "Encrypted backup verification restores RustFS backup",
		docLink:   "playground/chaos-scenarios.md#30-backup-verification-against-rustfs",
		encrypted: true,
	}
)

func scenario30BackupVerificationRustFS(v s30Variant) runner.Scenario {
	hypothesis := "A real MysqlBackup written to the playground RustFS bucket can be restored by a pinned " +
		"MysqlBackupVerification and the restored MySQL contains marker rows from the dump."
	if v.encrypted {
		hypothesis = "An AES-256-GCM encrypted MysqlBackup (operator-image encrypt-upload container) written to the " +
			"playground RustFS bucket can be decrypted (decrypt-download init container) and restored by a pinned " +
			"MysqlBackupVerification, and the restored MySQL contains marker rows from the dump."
	}
	return runner.Scenario{
		ID:         v.id,
		Title:      v.title,
		Hypothesis: hypothesis,
		Risk:       "medium",
		DocLink:    v.docLink,
		Timeout:    18 * time.Minute,
		Precheck:   assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s30EnsureBucket(v),
			s30ConfigureProfile(v),
			s30SeedMarkers(v),
			s30CreateBackup(v),
			s30WaitBackupSucceeded(v),
			s30CreateVerification(v),
			s30WaitVerificationSucceeded(v),
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error { return s30Cleanup(ctx, env, v) },
	}
}

func s30EnsureBucket(v s30Variant) runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "ensure RustFS backup bucket exists",
		Do: func(ctx context.Context, env *runner.Env) error {
			env.Capture.Note(fmt.Sprintf("RustFS endpoint=%s bucket=%s credentialsSecret=%s", backupE2EEndpoint, backupE2EBucket, backupE2ECredsSecret))
			if err := env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket); err != nil {
				return err
			}
			if v.encrypted {
				return ensureBackupPassphraseSecret(ctx, env)
			}
			return nil
		},
	}
}

func s30ConfigureProfile(v s30Variant) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure RustFS backup profile",
		Do: func(ctx context.Context, env *runner.Env) error {
			runStem := v.short + "-" + backupRunStamp(env)
			prefix := "e2e/" + v.id + "/" + runStem
			if err := ctxStash(ctx, env, "backupRunStem", runStem); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "backupPrefix", prefix); err != nil {
				return err
			}
			spec := backupProfileSpec(prefix, false)
			if v.encrypted {
				spec.Profiles[0].Encryption = backupE2EEncryption()
			}
			if err := patchBackupSpec(ctx, env, spec); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			return waitForBackupProfile(waitCtx, env, prefix, false)
		},
	}
}

func s30SeedMarkers(v s30Variant) runner.Step {
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
				"CREATE DATABASE IF NOT EXISTS " + v.dbName,
				"DROP TABLE IF EXISTS " + v.dbName + ".marker",
				"CREATE TABLE " + v.dbName + ".marker (id INT PRIMARY KEY, run_id VARCHAR(64), phase VARCHAR(32), payload VARCHAR(128), created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))",
			}
			for _, q := range stmts {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("marker schema %q: %w", q, err)
				}
			}
			insert := "INSERT INTO " + v.dbName + ".marker (id, run_id, phase, payload) VALUES (?, ?, ?, ?)"
			if _, err := primary.Exec(ctx, insert, 1, runStem, "baseline", "present-in-full-backup"); err != nil {
				return fmt.Errorf("insert baseline row 1: %w", err)
			}
			if _, err := primary.Exec(ctx, insert, 2, runStem, "baseline", "second-row"); err != nil {
				return fmt.Errorf("insert baseline row 2: %w", err)
			}
			if err := waitForReplicaGTID(ctx, env, active, replica); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("seeded %s.marker on active=%s replica=%s runStem=%s", v.dbName, active, replica, runStem))
			return nil
		},
	}
}

func s30CreateBackup(v s30Variant) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create MysqlBackup",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := v.short + "-backup-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "backupName", name); err != nil {
				return err
			}
			return createMysqlBackup(ctx, env, name, v.id)
		},
	}
}

func s30WaitBackupSucceeded(v s30Variant) runner.Step {
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
			if v.encrypted {
				// encrypt-upload reports the ciphertext prefix as a
				// full s3:// URL (cmd/bloodraven storageConfigFromEnv).
				wantLocation = "s3://" + backupE2EBucket + "/" + prefix + "/" + name + "/"
			}
			if b.Status.Location != wantLocation {
				return fmt.Errorf("backup location %q, want %q", b.Status.Location, wantLocation)
			}
			if b.Status.StorageType != v1alpha1.BackupStorageS3 {
				return fmt.Errorf("backup storageType=%q, want S3", b.Status.StorageType)
			}
			if b.Status.JobName == "" {
				return fmt.Errorf("backup status.jobName is empty")
			}
			if b.Status.Encrypted != v.encrypted {
				return fmt.Errorf("backup status.encrypted=%v, want %v", b.Status.Encrypted, v.encrypted)
			}
			if err := ctxStash(ctx, env, "backupUID", string(b.UID)); err != nil {
				return err
			}
			env.Capture.Note("backup succeeded: " + backupStatusSummary(b))
			return nil
		},
	}
}

func s30CreateVerification(v s30Variant) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create pinned MysqlBackupVerification",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := v.short + "-verify-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "verificationName", name); err != nil {
				return err
			}
			runStem := ctxFetch(env, "backupRunStem")
			query := "SELECT COUNT(*) FROM " + v.dbName + ".marker WHERE run_id=" + quoteSQLString(runStem) + " AND phase='baseline'"
			return createMysqlBackupVerification(ctx, env, name, ctxFetch(env, "backupName"), v.id, nil, query, 2)
		},
	}
}

func s30WaitVerificationSucceeded(v s30Variant) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "wait MysqlBackupVerification Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := ctxFetch(env, "verificationName")
			backupName := ctxFetch(env, "backupName")
			backupUID := ctxFetch(env, "backupUID")
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			vr, err := waitForVerificationPhase(waitCtx, env, name, v1alpha1.VerificationPhaseSucceeded)
			if err != nil {
				return err
			}
			if vr.Status.BackupRef == nil || vr.Status.BackupRef.Name != backupName {
				return fmt.Errorf("verification backupRef=%v, want name=%s", vr.Status.BackupRef, backupName)
			}
			if backupUID != "" && vr.Status.BackupRef.UID != "" && vr.Status.BackupRef.UID != backupUID {
				return fmt.Errorf("verification backupRef UID=%q, want %q", vr.Status.BackupRef.UID, backupUID)
			}
			if vr.Status.SanityCheck == nil || !vr.Status.SanityCheck.Ran || vr.Status.SanityCheck.ResultRow != "2" {
				return fmt.Errorf("verification sanity=%v, want ran=true resultRow=2", vr.Status.SanityCheck)
			}
			if !conditionTrue(vr.Status.Conditions, "Verified") {
				return fmt.Errorf("verification missing Verified=True condition: %s", conditionsSummary(vr.Status.Conditions))
			}
			env.Capture.Note("verification succeeded: " + verificationStatusSummary(vr))
			return nil
		},
	}
}

func s30Cleanup(ctx context.Context, env *runner.Env, v s30Variant) error {
	var errs []error
	if err := deleteBackupCRs(ctx, env, ctxFetch(env, "backupName"), ctxFetch(env, "verificationName")); err != nil {
		errs = append(errs, err)
	}
	if err := dropMarkerSchemaAndReplicate(ctx, env, v.dbName); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	if v.encrypted {
		if err := deleteBackupPassphraseSecret(ctx, env); err != nil {
			errs = append(errs, err)
		}
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
