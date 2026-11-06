package scenarios

// Shared helpers for the chronological cross-site PITR scenarios (#206):
// 37-pitr-planned-failback-no-reclone, 68-pitr-failback-with-reclone and
// 70-pitr-lossy-failover (DESIGN §9.4).
//
// Every verification and restore asserts the replay's certified outcome
// (status.pointInTime): the outcome, gtidExecuted EQUAL (as GTID sets) to
// the expected target history minus excludeGtids, replayedTransactionCount,
// the archive ID, and the marker rows. Nothing is asserted on segment counts
// or sites: those are planner choices, not the contract.
//
// Expected sets come from fences. A fence takes FLUSH TABLES WITH READ LOCK
// on the active primary, which blocks every commit, then reads
// @@gtid_executed and UTC_TIMESTAMP(6) (the target T) under the lock. Every
// transaction committed before the lock is in the set and has an original
// commit time before T; every transaction committed after UNLOCK TABLES has
// one after T. So the expected history at T is exact even while the counter
// app writes throughout. A flushing fence also runs FLUSH BINARY LOGS under
// the lock, so the sealed file's exec set (prev ∪ gtids) is exactly the
// fenced set: the latest-mode expectation.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
	brmysql "github.com/shipstream/bloodraven/internal/mysql"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

// pitrMicroLayout is the µs UTC form every PITR target is written in.
const pitrMicroLayout = "2006-01-02T15:04:05.000000Z"

// pitrScenarioMaxBinlogSize keeps the counter app from rotating a binlog by
// size during a scenario: the scenarios decide exactly which files are
// sealed (and so archived) with explicit FLUSH BINARY LOGS, and an unplanned
// rotation of a short era would archive it early.
const pitrScenarioMaxBinlogSize = "64M"

// pitrBackupSpec is backupProfileSpec with PITR on, the scenario's binlog
// size, and optional encryption.
func pitrBackupSpec(prefix, passphraseSecret string) v1alpha1.BackupSpec {
	spec := backupProfileSpec(prefix, true)
	spec.PITR.MaxBinlogSize = pitrScenarioMaxBinlogSize
	if passphraseSecret != "" {
		spec.Profiles[0].Encryption = backupE2EEncryption(passphraseSecret)
	}
	return spec
}

// pitrConfigureArchive patches spec.backup with a fresh PITR prefix under
// e2e/<scenario>/<runStem>[-<suffix>] and waits for the profile, the PITR
// rollout and a ready archiver on the active site. It stashes
// backupRunStem (first call only) and backupPrefix.
func pitrConfigureArchive(ctx context.Context, env *runner.Env, scenarioID, short, suffix string, encrypted bool) error {
	if err := env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket); err != nil {
		return err
	}
	runStem := ctxFetch(env, "backupRunStem")
	if runStem == "" {
		runStem = short + "-" + backupRunStamp(env)
		if err := ctxStash(ctx, env, "backupRunStem", runStem); err != nil {
			return err
		}
	}
	prefix := "e2e/" + scenarioID + "/" + runStem
	if suffix != "" {
		prefix += "-" + suffix
	}
	if err := ctxStash(ctx, env, "backupPrefix", prefix); err != nil {
		return err
	}
	secret := ctxFetch(env, backupPassphraseSecretKey)
	if encrypted && secret == "" {
		var err error
		if secret, err = ensureBackupPassphraseSecret(ctx, env); err != nil {
			return err
		}
	}
	if !encrypted {
		secret = ""
	}
	// patchBackupSpec records the spec it replaces for cleanup; a second
	// call (a new prefix) must keep the group's original spec, not this
	// scenario's first one.
	origHad, origSpec := ctxFetch(env, backupOriginalHadKey), ctxFetch(env, backupOriginalSpecKey)
	if err := patchBackupSpec(ctx, env, pitrBackupSpec(prefix, secret)); err != nil {
		return err
	}
	if origHad != "" {
		if err := ctxStash(ctx, env, backupOriginalHadKey, origHad); err != nil {
			return err
		}
		if err := ctxStash(ctx, env, backupOriginalSpecKey, origSpec); err != nil {
			return err
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := waitForBackupProfile(waitCtx, env, prefix, true); err != nil {
		return err
	}
	rolloutCtx, rolloutCancel := context.WithTimeout(ctx, 8*time.Minute)
	defer rolloutCancel()
	mfg, err := env.Wait.UntilCR(rolloutCtx, env.Namespace, "PITR rollout and archive verified", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ready := conditionTrueForGeneration(mfg.Status.Conditions, "Ready", mfg.Generation)
		archiveID := pitrObservedArchiveID(mfg)
		return ready && mfg.Status.UpdatePhase == "" && mfg.Status.ActiveSite != "" && archiveID != "",
			fmt.Sprintf("ready=%v updatePhase=%q active=%q archive=%s", ready, mfg.Status.UpdatePhase, mfg.Status.ActiveSite, pitrArchiveSummary(mfg)), nil
	})
	if err != nil {
		return err
	}
	st, err := waitForArchiverReady(rolloutCtx, env, mfg.Status.ActiveSite, prefix+"/binlogs")
	if err != nil {
		return err
	}
	env.Capture.Note(fmt.Sprintf("PITR archive %s verified at prefix %s (encrypted=%v); active archiver: %s", pitrObservedArchiveID(mfg), prefix, encrypted, archiverSummary(st)))
	return ctxStash(ctx, env, "archiveID", pitrObservedArchiveID(mfg))
}

func pitrObservedArchiveID(mfg *v1alpha1.MysqlFailoverGroup) string {
	if mfg.Status.PITR == nil || mfg.Status.PITR.Archive == nil || mfg.Status.PITR.Archive.State != v1alpha1.PITRArchiveVerified {
		return ""
	}
	return mfg.Status.PITR.Archive.ArchiveID
}

func pitrArchiveSummary(mfg *v1alpha1.MysqlFailoverGroup) string {
	if mfg.Status.PITR == nil || mfg.Status.PITR.Archive == nil {
		return "<none>"
	}
	a := mfg.Status.PITR.Archive
	return fmt.Sprintf("{id=%q state=%s reason=%s mode=%s}", a.ArchiveID, a.State, a.Reason, a.EncryptionMode)
}

// pitrCreateMarkerTable creates <db>.marker on the active site.
func pitrCreateMarkerTable(ctx context.Context, env *runner.Env, site, db string) error {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s: %w", site, err)
	}
	defer c.Close()
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + db,
		"DROP TABLE IF EXISTS " + db + ".marker",
		"CREATE TABLE " + db + ".marker (id INT PRIMARY KEY, run_id VARCHAR(64), phase VARCHAR(32), site VARCHAR(32), created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))",
	} {
		if _, err := c.Exec(ctx, q); err != nil {
			return fmt.Errorf("marker schema %q: %w", q, err)
		}
	}
	return nil
}

// pitrInsertMarker inserts one marker row on site, whose writer must be
// that site's server.
func pitrInsertMarker(ctx context.Context, env *runner.Env, site, db string, id int, phase string) error {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s for marker %s: %w", site, phase, err)
	}
	defer c.Close()
	if _, err := c.Exec(ctx, "INSERT INTO "+db+".marker (id, run_id, phase, site) VALUES (?,?,?,?)", id, ctxFetch(env, "backupRunStem"), phase, site); err != nil {
		return fmt.Errorf("insert marker %s on %s: %w", phase, site, err)
	}
	return nil
}

// pitrMarkerQuery is a verification sanity query that returns 1 iff this
// run's marker rows are exactly phases, in id order.
func pitrMarkerQuery(db, runStem string, phases []string) string {
	return "SELECT IF(IFNULL(GROUP_CONCAT(phase ORDER BY id SEPARATOR ','), '') = " + quoteSQLString(strings.Join(phases, ",")) +
		", 1, 0) FROM " + quoteSQLIdentifier(db) + ".marker WHERE run_id=" + quoteSQLString(runStem)
}

// pitrFence is one fenced observation of a primary: T (µs UTC) and the
// exact executed set at T.
type pitrFence struct {
	Site string
	T    string
	GTID string
}

// pitrTakeFence fences site's primary (see the file comment). With flush it
// also seals the active binlog under the lock and waits until the archiver
// uploaded it, so fence.GTID is the sealed file's exec set.
func pitrTakeFence(ctx context.Context, env *runner.Env, site string, flush bool) (pitrFence, error) {
	var before *pgsidecar.ArchiverStatusResponse
	var probe *pgsidecar.Probe
	if flush {
		var err error
		probe, err = pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
		if err != nil {
			return pitrFence{}, fmt.Errorf("open sidecar %s: %w", site, err)
		}
		defer probe.Close()
		if before, err = probe.ArchiverStatus(ctx); err != nil {
			return pitrFence{}, fmt.Errorf("read pre-flush archiver status on %s: %w", site, err)
		}
	}
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return pitrFence{}, fmt.Errorf("open %s for fence: %w", site, err)
	}
	defer c.Close()
	fenceCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	conn, err := c.DB.Conn(fenceCtx)
	if err != nil {
		return pitrFence{}, fmt.Errorf("pin connection on %s: %w", site, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(fenceCtx, "SET SESSION lock_wait_timeout = 60"); err != nil {
		return pitrFence{}, err
	}
	if _, err := conn.ExecContext(fenceCtx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		return pitrFence{}, fmt.Errorf("FLUSH TABLES WITH READ LOCK on %s: %w", site, err)
	}
	unlocked := false
	defer func() {
		if !unlocked {
			_, _ = conn.ExecContext(context.Background(), "UNLOCK TABLES")
		}
	}()
	var gtid, readOnly string
	if err := conn.QueryRowContext(fenceCtx, "SELECT @@global.gtid_executed, @@global.read_only").Scan(&gtid, &readOnly); err != nil {
		return pitrFence{}, fmt.Errorf("read gtid_executed under lock on %s: %w", site, err)
	}
	if readOnly != "0" {
		return pitrFence{}, fmt.Errorf("fence on %s: the site is read-only; fences must run on the active primary", site)
	}
	// Keep T strictly after every fenced commit and strictly before every
	// later one, whatever the clock granularity.
	var t string
	if err := conn.QueryRowContext(fenceCtx, "SELECT SLEEP(0.002), DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')").Scan(new(int), &t); err != nil {
		return pitrFence{}, fmt.Errorf("read target time under lock on %s: %w", site, err)
	}
	if flush {
		if _, err := conn.ExecContext(fenceCtx, "FLUSH BINARY LOGS"); err != nil {
			return pitrFence{}, fmt.Errorf("FLUSH BINARY LOGS under lock on %s: %w", site, err)
		}
	}
	if _, err := conn.ExecContext(fenceCtx, "DO SLEEP(0.002)"); err != nil {
		return pitrFence{}, err
	}
	if _, err := conn.ExecContext(fenceCtx, "UNLOCK TABLES"); err != nil {
		return pitrFence{}, fmt.Errorf("UNLOCK TABLES on %s: %w", site, err)
	}
	unlocked = true
	fence := pitrFence{Site: site, T: t, GTID: strings.ReplaceAll(strings.TrimSpace(gtid), "\n", "")}
	if flush {
		if err := pitrWaitArchiverAdvanced(ctx, env, probe, site, before, true); err != nil {
			return pitrFence{}, err
		}
	}
	env.Capture.Note(fmt.Sprintf("fence on %s: T=%s flush=%v gtid=%s", site, fence.T, flush, fence.GTID))
	return fence, nil
}

// pitrWaitArchiverAdvanced waits until the archiver uploaded a new file
// after before. With clean it also requires no backlog and no error.
func pitrWaitArchiverAdvanced(ctx context.Context, env *runner.Env, probe *pgsidecar.Probe, site string, before *pgsidecar.ArchiverStatusResponse, clean bool) error {
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
			if advanced && (!clean || (st.BacklogFiles == 0 && st.LastError == "")) {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("archiver on %s did not upload the flushed binlog: %w (last: %s)", site, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

// pitrFlush seals the active binlog on site and waits for the upload,
// without fencing.
func pitrFlush(ctx context.Context, env *runner.Env, site string, clean bool) error {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return fmt.Errorf("open sidecar %s: %w", site, err)
	}
	defer probe.Close()
	before, err := probe.ArchiverStatus(ctx)
	if err != nil {
		return fmt.Errorf("read pre-flush archiver status on %s: %w", site, err)
	}
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.Exec(ctx, "FLUSH BINARY LOGS"); err != nil {
		return fmt.Errorf("flush binary logs on %s: %w", site, err)
	}
	return pitrWaitArchiverAdvanced(ctx, env, probe, site, before, clean)
}

// pitrStashFence and pitrFetchFence keep fences across steps.
func pitrStashFence(ctx context.Context, env *runner.Env, key string, f pitrFence) error {
	body, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return ctxStash(ctx, env, "fence:"+key, string(body))
}

func pitrFetchFence(env *runner.Env, key string) (pitrFence, error) {
	raw := ctxFetch(env, "fence:"+key)
	if raw == "" {
		return pitrFence{}, fmt.Errorf("fence %q was not recorded", key)
	}
	var f pitrFence
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		return pitrFence{}, fmt.Errorf("decode fence %q: %w", key, err)
	}
	return f, nil
}

// pitrSiteGTID reads @@gtid_executed on any site.
func pitrSiteGTID(ctx context.Context, env *runner.Env, site string) (string, error) {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return "", err
	}
	defer c.Close()
	g, err := c.GtidExecuted(ctx)
	return strings.ReplaceAll(g, "\n", ""), err
}

func pitrServerUUID(ctx context.Context, env *runner.Env, site string) (string, error) {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return "", err
	}
	defer c.Close()
	return c.ScalarString(ctx, "SELECT @@server_uuid")
}

// gtidSetsEqual compares two GTID sets as sets of transactions.
func gtidSetsEqual(a, b string) (bool, error) {
	as, err := brmysql.ParseGTIDSet(a)
	if err != nil {
		return false, fmt.Errorf("parse %q: %w", a, err)
	}
	bs, err := brmysql.ParseGTIDSet(b)
	if err != nil {
		return false, fmt.Errorf("parse %q: %w", b, err)
	}
	return as.Contains(bs) && bs.Contains(as), nil
}

// gtidSubtract returns a ∖ b, canonical.
func gtidSubtract(a, b string) (string, error) {
	as, err := brmysql.ParseGTIDSet(a)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", a, err)
	}
	bs, err := brmysql.ParseGTIDSet(b)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", b, err)
	}
	return as.Subtract(bs).String(), nil
}

func gtidCount(s string) (int64, error) {
	set, err := brmysql.ParseGTIDSet(s)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", s, err)
	}
	return set.TransactionCount(), nil
}

// pitrDumpBaseline reads B, the dump's own @.json gtidExecuted, straight
// from RustFS (decrypting with this run's passphrase when the dump is
// encrypted). MysqlBackup.status.gtidExecuted is read before the dump and
// can be smaller, so replay counts are computed against @.json.
func pitrDumpBaseline(ctx context.Context, env *runner.Env, backupName string) (string, error) {
	b, err := getMysqlBackup(ctx, env, backupName)
	if err != nil {
		return "", err
	}
	bucket, prefix, ok, err := backupArtifactPrefix(b)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("MysqlBackup %s has no S3 artifact", backupName)
	}
	key := prefix + "@.json"
	data, found, err := env.Chaos.ReadRustFSObject(ctx, bucket, key)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", key, err)
	}
	if !found {
		return "", fmt.Errorf("dump metadata s3://%s/%s not found", bucket, key)
	}
	if b.Status.Encrypted {
		pass, err := pitrRunPassphrase(ctx, env)
		if err != nil {
			return "", err
		}
		var plain bytes.Buffer
		if _, err := backupcrypto.Decrypt(&plain, bytes.NewReader(data), pass); err != nil {
			return "", fmt.Errorf("decrypt %s: %w", key, err)
		}
		data = plain.Bytes()
	}
	var meta struct {
		GTIDExecuted string `json:"gtidExecuted"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return "", fmt.Errorf("decode %s: %w", key, err)
	}
	return strings.ReplaceAll(strings.TrimSpace(meta.GTIDExecuted), "\n", ""), nil
}

func pitrRunPassphrase(ctx context.Context, env *runner.Env) ([]byte, error) {
	name := ctxFetch(env, backupPassphraseSecretKey)
	if name == "" {
		return nil, fmt.Errorf("the dump is encrypted but this run created no passphrase Secret")
	}
	sec, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get passphrase secret %s: %w", name, err)
	}
	raw := sec.Data[backupE2EEncryption(name).PassphraseSecret.PassphraseSecretKeyOrDefault()]
	return backupcrypto.TrimPassphrase(raw), nil
}

// pitrBackup creates a MysqlBackup, waits for it, and records B.
func pitrBackup(ctx context.Context, env *runner.Env, scenarioID, key string) error {
	name := pitrShortName(scenarioID, "backup-"+key) + "-" + backupRunStamp(env)
	if err := ctxStash(ctx, env, "backup:"+key, name); err != nil {
		return err
	}
	pitrTrackCR(ctx, env, "MysqlBackup", name)
	if err := createMysqlBackup(ctx, env, name, scenarioID); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	b, err := waitForBackupPhase(waitCtx, env, name, v1alpha1.BackupPhaseSucceeded)
	if err != nil {
		return err
	}
	baseline, err := pitrDumpBaseline(ctx, env, name)
	if err != nil {
		return err
	}
	if err := ctxStash(ctx, env, "baseline:"+key, baseline); err != nil {
		return err
	}
	env.Capture.Note(fmt.Sprintf("backup %s succeeded on site %s (completion=%v); dump gtidExecuted=%s", name, b.Status.SourceSite, b.Status.CompletionTime, baseline))
	return nil
}

// pitrTrackCR records a CR this run created, for cleanup.
func pitrTrackCR(ctx context.Context, env *runner.Env, kind, name string) {
	key := "created:" + kind
	existing := ctxFetch(env, key)
	if existing != "" {
		existing += ","
	}
	_ = ctxStash(ctx, env, key, existing+name)
}

func pitrTrackedCRs(env *runner.Env, kind string) []string {
	raw := ctxFetch(env, "created:"+kind)
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}

// pitrExpectation is what one verification or restore must certify.
type pitrExpectation struct {
	// Backup is the backup key passed to pitrBackup.
	Backup string
	// Mode is timestamp or latest.
	Mode string
	// Fence supplies T (timestamp mode) and the expected executed set.
	Fence pitrFence
	// Exclude is excludeGtids.
	Exclude string
	// Outcome is the expected status.pointInTime.outcome.
	Outcome v1alpha1.PointInTimeOutcome
	// Markers are this run's marker phases the result must hold, in order.
	Markers []string
}

// pitrExpectedSet is the fenced set minus excludeGtids.
func (e pitrExpectation) expectedSet() (string, error) {
	if e.Exclude == "" {
		return e.Fence.GTID, nil
	}
	return gtidSubtract(e.Fence.GTID, e.Exclude)
}

// assertPITRResult checks a certified point-in-time result against e.
func assertPITRResult(env *runner.Env, what string, res *v1alpha1.PointInTimeResult, e pitrExpectation) error {
	if res == nil {
		return fmt.Errorf("%s: status.pointInTime is not set", what)
	}
	wantMode := v1alpha1.PointInTimeModeTimestamp
	if e.Mode == "latest" {
		wantMode = v1alpha1.PointInTimeModeLatest
	}
	if res.Mode != wantMode {
		return fmt.Errorf("%s: pointInTime.mode=%s, want %s", what, res.Mode, wantMode)
	}
	if res.Outcome != e.Outcome {
		return fmt.Errorf("%s: pointInTime.outcome=%s, want %s", what, res.Outcome, e.Outcome)
	}
	if wantMode == v1alpha1.PointInTimeModeTimestamp {
		if res.StopTime == nil || res.StopTime.UTC().Format(pitrMicroLayout) != e.Fence.T {
			return fmt.Errorf("%s: pointInTime.stopTime=%v, want %s", what, res.StopTime, e.Fence.T)
		}
	}
	wantArchive := ctxFetch(env, "archiveID")
	if res.Archive == nil || res.Archive.ArchiveID != wantArchive {
		return fmt.Errorf("%s: pointInTime.archive=%+v, want archiveID %s (the source group's status.pitr.archive)", what, res.Archive, wantArchive)
	}
	want, err := e.expectedSet()
	if err != nil {
		return err
	}
	if res.GtidExecuted == "" {
		return fmt.Errorf("%s: pointInTime.gtidExecuted is empty (sha256=%s); expected %s", what, res.GtidExecutedSHA256, want)
	}
	equal, err := gtidSetsEqual(res.GtidExecuted, want)
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if !equal {
		extra, _ := gtidSubtract(res.GtidExecuted, want)
		missing, _ := gtidSubtract(want, res.GtidExecuted)
		return fmt.Errorf("%s: pointInTime.gtidExecuted=%s, want %s (extra %q, missing %q)", what, res.GtidExecuted, want, extra, missing)
	}
	baseline := ctxFetch(env, "baseline:"+e.Backup)
	missingBase, err := gtidSubtract(baseline, want)
	if err != nil {
		return err
	}
	if missingBase != "" {
		return fmt.Errorf("%s: the expected history %s does not contain the backup's %s (missing %s); the scenario's expectation is wrong", what, want, baseline, missingBase)
	}
	replay, err := gtidSubtract(want, baseline)
	if err != nil {
		return err
	}
	if e.Exclude != "" {
		if replay, err = gtidSubtract(replay, e.Exclude); err != nil {
			return err
		}
	}
	n, err := gtidCount(replay)
	if err != nil {
		return err
	}
	if res.ReplayedTransactionCount != n {
		return fmt.Errorf("%s: pointInTime.replayedTransactionCount=%d, want %d (|%s|)", what, res.ReplayedTransactionCount, n, replay)
	}
	env.Capture.Note(fmt.Sprintf("%s certified: outcome=%s gtidExecuted=%s replayed=%d archive=%s", what, res.Outcome, res.GtidExecuted, res.ReplayedTransactionCount, res.Archive.ArchiveID))
	return nil
}

// pitrVerifySpec builds the verification's pointInTime block.
func pitrVerifySpec(e pitrExpectation) *v1alpha1.PointInTimeVerificationSpec {
	pit := &v1alpha1.PointInTimeVerificationSpec{Mode: e.Mode, ExcludeGtids: e.Exclude}
	if e.Mode == "timestamp" {
		pit.Timestamp = e.Fence.T
	}
	return pit
}

// pitrVerify runs one MysqlBackupVerification of e.Backup and asserts the
// certified result and the marker rows.
func pitrVerify(ctx context.Context, env *runner.Env, scenarioID, db, label string, e pitrExpectation) error {
	name := fmt.Sprintf("%s-%s", pitrShortName(scenarioID, label), backupRunStamp(env))
	pitrTrackCR(ctx, env, "MysqlBackupVerification", name)
	query := pitrMarkerQuery(db, ctxFetch(env, "backupRunStem"), e.Markers)
	if err := createMysqlBackupVerification(ctx, env, name, ctxFetch(env, "backup:"+e.Backup), scenarioID, pitrVerifySpec(e), query, 1); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	v, err := waitForVerificationPhase(waitCtx, env, name, v1alpha1.VerificationPhaseSucceeded)
	if err != nil {
		return err
	}
	if v.Status.SanityCheck == nil || !v.Status.SanityCheck.Ran || v.Status.SanityCheck.ResultRow != "1" {
		return fmt.Errorf("verification %s: marker rows are not exactly %v (sanity=%+v)", name, e.Markers, v.Status.SanityCheck)
	}
	if !conditionTrue(v.Status.Conditions, "Verified") {
		return fmt.Errorf("verification %s missing Verified=True: %s", name, conditionsSummary(v.Status.Conditions))
	}
	return assertPITRResult(env, "verification "+name, v.Status.PointInTime, e)
}

// pitrVerifyRefused runs one verification and asserts it fails with token
// as the Verified=False reason, and that the message contains every snippet.
func pitrVerifyRefused(ctx context.Context, env *runner.Env, scenarioID, db, label string, e pitrExpectation, token string, snippets ...string) (*v1alpha1.MysqlBackupVerification, error) {
	name := fmt.Sprintf("%s-%s", pitrShortName(scenarioID, label), backupRunStamp(env))
	pitrTrackCR(ctx, env, "MysqlBackupVerification", name)
	query := pitrMarkerQuery(db, ctxFetch(env, "backupRunStem"), e.Markers)
	if err := createMysqlBackupVerification(ctx, env, name, ctxFetch(env, "backup:"+e.Backup), scenarioID, pitrVerifySpec(e), query, 1); err != nil {
		return nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	v, err := waitForVerificationPhase(waitCtx, env, name, v1alpha1.VerificationPhaseFailed)
	if v == nil {
		return nil, err
	}
	if v.Status.Phase != v1alpha1.VerificationPhaseFailed {
		return v, err
	}
	cond := findCondition(v.Status.Conditions, "Verified")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != token {
		return v, fmt.Errorf("verification %s: want Verified=False/%s, got %s (message %q)", name, token, conditionsSummary(v.Status.Conditions), v.Status.Message)
	}
	for _, s := range snippets {
		if !strings.Contains(cond.Message, s) {
			return v, fmt.Errorf("verification %s refused %s but its message lacks %q: %s", name, token, s, cond.Message)
		}
	}
	if v.Status.PointInTime != nil {
		return v, fmt.Errorf("verification %s refused but status.pointInTime is set: %+v", name, v.Status.PointInTime)
	}
	env.Capture.Note(fmt.Sprintf("verification %s refused as expected: %s", name, cond.Message))
	return v, nil
}

// pitrShortName keeps verification names short: "s37-<label>".
func pitrShortName(scenarioID, label string) string {
	return "s" + strings.SplitN(scenarioID, "-", 2)[0] + "-" + label
}

// pitrWaitCooldown waits until the anti-flap cooldown from the last
// failover has elapsed, so a planned failover is not rejected.
func pitrWaitCooldown(ctx context.Context, env *runner.Env) error {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "anti-flap cooldown elapsed", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		rec, _, err := brcontroller.EffectiveFailoverRecord(mfg, time.Now())
		if err != nil {
			return false, "", err
		}
		if rec.LastFailover.IsZero() {
			return true, "no failover recorded", nil
		}
		// One extra second for whole-second status timestamps.
		rem := failoverCooldown(mfg) + time.Second - time.Since(rec.LastFailover)
		return rem <= 0, fmt.Sprintf("cooldown remaining %s", rem.Round(time.Second)), nil
	})
	return err
}

// pitrPlannedFailover runs a planned failover to target and waits for it
// to succeed, for target's archiver to be primary on the active prefix,
// and for target's promotion record to be confirmed after the request.
// It returns the confirmed promotionRecordedAt.
func pitrPlannedFailover(ctx context.Context, env *runner.Env, target string) (time.Time, error) {
	if err := pitrWaitCooldown(ctx, env); err != nil {
		return time.Time{}, err
	}
	requested := time.Now()
	if err := env.Chaos.AnnotatePlannedFailover(ctx, target); err != nil {
		return time.Time{}, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "planned failover to "+target, func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		pf := mfg.Status.PlannedFailover
		if pf == nil || pf.StartTime == nil || pf.StartTime.Time.Before(requested.Add(-2*time.Second)) {
			return false, "no planned failover for this request yet", nil
		}
		msg := fmt.Sprintf("phase=%s target=%s reason=%q active=%s", pf.Phase, pf.Target, pf.Reason, mfg.Status.ActiveSite)
		if pf.Phase == v1alpha1.PlannedFailoverPhaseFailed {
			return false, msg, fmt.Errorf("planned failover to %s failed: %s (%s)", target, pf.Reason, pf.Message)
		}
		return pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded && pf.Target == target && mfg.Status.ActiveSite == target, msg, nil
	}); err != nil {
		return time.Time{}, err
	}
	recorded, err := pitrWaitPromotionRecorded(ctx, env, target, requested)
	if err != nil {
		return time.Time{}, err
	}
	env.Capture.Note(fmt.Sprintf("planned failover to %s succeeded; promotion recorded at %s", target, recorded.Format(time.RFC3339Nano)))
	return recorded, nil
}

// pitrWaitPromotionRecorded waits until site's archiver is the primary on
// the active prefix and reports a promotion record observed after
// notBefore.
func pitrWaitPromotionRecorded(ctx context.Context, env *runner.Env, site string, notBefore time.Time) (time.Time, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	prefix := ctxFetch(env, "backupPrefix") + "/binlogs"
	if _, err := waitForArchiverReady(waitCtx, env, site, prefix); err != nil {
		return time.Time{}, err
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		probe, err := pgsidecar.Open(waitCtx, env.Kube, env.Namespace, env.FG, site)
		if err != nil {
			last = err.Error()
		} else {
			st, err := probe.ArchiverStatus(waitCtx)
			probe.Close()
			if err != nil {
				last = err.Error()
			} else {
				last = fmt.Sprintf("promotionRecordedAt=%s %s", st.PromotionRecordedAt.Format(time.RFC3339Nano), archiverSummary(st))
				// observedAt is truncated to µs; allow a second of slack.
				if !st.PromotionRecordedAt.IsZero() && !st.PromotionRecordedAt.Before(notBefore.Add(-time.Second)) {
					return st.PromotionRecordedAt, nil
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return time.Time{}, fmt.Errorf("site %s confirmed no promotion record after %s: %w (last: %s)", site, notBefore.Format(time.RFC3339Nano), waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

// pitrRecloneSite reclones site (a replica, or a RecoveryBlocked former
// primary) from the active site and waits for the clone to finish.
func pitrRecloneSite(ctx context.Context, env *runner.Env, site string) error {
	readyCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	mfg, err := env.Wait.UntilCR(readyCtx, env.Namespace, site+" reachable before reclone", func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		for _, s := range m.Status.Sites {
			if s.Name == site {
				return s.State == "read-only", fmt.Sprintf("state=%s recovery=%s", s.State, s.RecoveryState), nil
			}
		}
		return false, "site absent", nil
	})
	if err != nil {
		return err
	}
	value := site + ":confirm=" + env.FG
	for _, s := range mfg.Status.Sites {
		if s.Name == site && len(strings.TrimSpace(s.DivergentGtid)) >= 8 {
			value = site + ":" + strings.TrimSpace(s.DivergentGtid)[:8]
		}
	}
	requested := time.Now()
	if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, "bloodraven.shipstream.io/reclone-site", value); err != nil {
		return err
	}
	env.Capture.Note("requested reclone-site=" + value)
	cloneCtx, cloneCancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cloneCancel()
	if _, err := env.Wait.UntilCR(cloneCtx, env.Namespace, "reclone of "+site+" completes", func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		boot := findCondition(m.Status.Conditions, "Bootstrapping")
		if boot == nil {
			return false, "bootstrap not observed", nil
		}
		if boot.Reason == "Failed" && boot.LastTransitionTime.Time.After(requested) {
			return false, boot.Message, fmt.Errorf("reclone of %s failed: %s", site, boot.Message)
		}
		replicating := false
		for _, s := range m.Status.Sites {
			if s.Name == site {
				replicating = s.Replicating && s.State == "read-only"
			}
		}
		return boot.Reason == "Done" && boot.LastTransitionTime.Time.After(requested) && replicating, fmt.Sprintf("bootstrap=%s replicating=%v", boot.Reason, replicating), nil
	}); err != nil {
		return err
	}
	env.Capture.Note("reclone of " + site + " completed")
	return nil
}

// pitrSourceSpecForRestore derives a minimal group spec that restores from
// the source group's backup and archive: the source's two primary-candidate
// sites (same zones, so pods land on the same nodes) without taints, load
// balancer IPs, readers, Dragonfly or encryption at rest, the source's
// backup profiles with PITR off and no schedules, and the restore. The seed
// (first priority) is the source's current active site, whose nodes carry
// no read-only taint.
func pitrSourceSpecForRestore(src *v1alpha1.MysqlFailoverGroup, name, backupName, stop string) (*v1alpha1.MysqlFailoverGroup, error) {
	spec := src.Spec.DeepCopy()
	var sites []v1alpha1.SiteSpec
	var priorities []string
	for _, s := range spec.Sites {
		if !s.IsPromotable() {
			continue
		}
		s.TaintNodeSelector = nil
		s.LBIP = ""
		s.ServiceTemplate = nil
		sites = append(sites, s)
		if s.Name == src.Status.ActiveSite {
			priorities = append([]string{s.Name}, priorities...)
		} else {
			priorities = append(priorities, s.Name)
		}
	}
	if len(sites) < 2 {
		return nil, fmt.Errorf("source group has %d primary-candidate sites, want 2", len(sites))
	}
	spec.Sites = sites
	spec.SplitBrainPolicy = &v1alpha1.SplitBrainPolicySpec{SitePriorities: priorities}
	spec.DNS = v1alpha1.DNSSpec{Hostname: name + "-db.example.local", TTL: src.Spec.DNS.TTL}
	spec.Dragonfly = nil
	spec.EncryptionAtRest = nil
	spec.RestoreInPlace = nil
	spec.ServiceTemplate = &v1alpha1.ServiceTemplate{Type: corev1.ServiceTypeClusterIP}
	if spec.Backup == nil {
		return nil, fmt.Errorf("source group has no spec.backup")
	}
	spec.Backup.PITR = nil
	spec.Backup.Schedules = nil
	spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: backupName}},
		PointInTime: &v1alpha1.PointInTimeSpec{
			StopDatetime:       stop,
			ArchiveProfileName: backupE2EProfile,
		},
	}
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: src.Namespace,
			Labels:    map[string]string{"chaos.playground.bloodraven.io/created-by-e2e": "true"},
		},
		Spec: *spec,
	}, nil
}
