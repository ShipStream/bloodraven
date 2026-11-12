package scenarios

import (
	"context"
	"fmt"
	"time"

	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario72PITREncryptedBinlog())
}

const s72ID = "72-pitr-encrypted-binlog"

// scenario72PITREncryptedBinlog is scenario 31 on a group whose binary log
// MySQL encrypts: the archiver has to decrypt every file it archives
// (internal/sidecar/local_binlog.go), and the point-in-time verification
// then restores a dump and replays those files with mysqlbinlog, which
// reads plaintext only. Before the archiver could decrypt, such a group
// archived nothing and the scenario stopped at the archiver's readiness.
func scenario72PITREncryptedBinlog() runner.Scenario {
	return runner.Scenario{
		ID:    s72ID,
		Title: "PITR archives and replays binary logs that MySQL encrypts (encryption at rest)",
		Hypothesis: "With spec.encryptionAtRest (binlog_encryption=ON) and PITR against RustFS, the sidecar archives the " +
			"plaintext of the encrypted binary logs without an archiver error, and a pinned MysqlBackupVerification " +
			"restores a full dump, replays the archived binlogs to a timestamp, includes the pre-target marker rows " +
			"and excludes the post-target ones.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#72-pitr-encrypted-binlog",
		Timeout: 24 * time.Minute,
		// As scenario 31: the cleanup restores spec.backup, which rolls
		// every site, and releases the run's archive binding.
		CleanupTimeout: pitrCleanupBudget(0),
		Quarantine: "requires the dedicated TLS + spec.encryptionAtRest baseline; " +
			"CI and local encryption jobs run this scenario explicitly",
		Precheck: assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s72AssertBinlogEncrypted(),
			s31EnsureBucket(),
			s31ConfigureProfile(),
			s31WaitArchiverReady(),
			s31SeedBaseline(),
			s31CreateBackup(),
			s31WaitBackupSucceeded(),
			s31InsertPITRMarkers(),
			s31ForceBinlogArchive(),
			s72AssertBinlogEncrypted(),
			s31CreateVerification(),
			s31WaitVerificationSucceeded(),
		},
		Cleanup: s31Cleanup,
	}
}

// s72AssertBinlogEncrypted fails unless both primary-candidate sites run
// with binlog_encryption=ON: without it the scenario is scenario 31 and
// proves nothing about decryption. It runs again after the archive was
// forced, because enabling PITR rolls every site.
func s72AssertBinlogEncrypted() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "binlog_encryption is ON on the active site and the replica",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, replica, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			for _, site := range []string{active, replica} {
				db, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
				if err != nil {
					return fmt.Errorf("open mysql on %s: %w", site, err)
				}
				on, err := db.ScalarInt(ctx, "SELECT @@global.binlog_encryption")
				_ = db.Close()
				if err != nil {
					return fmt.Errorf("read binlog_encryption on %s: %w", site, err)
				}
				if on != 1 {
					return fmt.Errorf("binlog_encryption is OFF on %s: this scenario needs the encryption-at-rest baseline "+
						"(./playground/enable-encryption.sh) with coverage.binaryLog on", site)
				}
			}
			env.Capture.Note(fmt.Sprintf("binlog_encryption=ON on active=%s replica=%s", active, replica))
			return nil
		},
	}
}
