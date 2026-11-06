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
// @@gtid_executed and the server clock under the lock, and holds the lock
// until the server clock has passed the next whole second, which is the
// target T (stopDatetime admits no fraction, A-39). Every transaction
// committed before the lock is in the set and has an original commit time
// before T; every transaction committed after UNLOCK TABLES has one after T.
// So the expected history at T is exact even while a writer commits
// concurrently. A flushing fence also runs FLUSH BINARY LOGS under the lock,
// so the sealed file's exec set (prev ∪ gtids) is exactly the fenced set:
// the latest-mode expectation.
//
// The playground counter app only writes on request, so the scenarios drive
// their own concurrent load: pitrWithWriter runs a background writer on the
// era's primary across each era's marker, fence and witness marker. It is
// stopped (its last insert committed) before every flush, failover, reclone,
// scale-down and backup, so the archive boundaries the scenarios assert stay
// exact: in particular nothing commits on b between s68 cycle 1's flush of b
// and the reclone of a, which would otherwise be an unarchived tail.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
	brmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

// pitrMicroLayout is the µs UTC form the server clock is read in.
const pitrMicroLayout = "2006-01-02T15:04:05.000000Z"

// pitrTargetLayout is the whole-second UTC RFC 3339 form every PITR target
// is written in: stopDatetime and the verification timestamp admit no
// fraction (A-39).
const pitrTargetLayout = time.RFC3339

// pitrFenceTarget is the target T for a fence whose server clock read now
// under the lock: the next whole second, strictly after now, so every
// commit before the lock is before T.
func pitrFenceTarget(now time.Time) time.Time {
	return now.UTC().Truncate(time.Second).Add(time.Second)
}

// pitrServerNow reads a server's UTC clock at µs precision.
func pitrServerNow(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}) (time.Time, error) {
	var raw string
	if err := q.QueryRowContext(ctx, "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')").Scan(&raw); err != nil {
		return time.Time{}, err
	}
	return time.Parse(pitrMicroLayout, raw)
}

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
// e2e/<scenario>/<runStem>[-<suffix>] and follows #203's prefix switch to
// its end (pitrWaitArchiveBound): the new binding Bound, the rollout done,
// the archive verified, and the active archiver writing there. Only then
// may a scenario take the full backup the switch requires. It stashes
// backupRunStem (first call only), backupPrefix and archiveID.
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
	// pitrCleanup deletes every prefix this run archived to, once the
	// original spec.backup is back and no archiver writes there (F191).
	pitrTrackCR(ctx, env, pitrArchivePrefixKind, prefix)
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
	mfg, binding, err := pitrWaitArchiveBound(ctx, env, prefix)
	if err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, pitrArchiveSwitchWait)
	defer cancel()
	st, err := waitForArchiverReady(waitCtx, env, mfg.Status.ActiveSite, prefix+"/binlogs")
	if err != nil {
		return err
	}
	env.Capture.Note(fmt.Sprintf("PITR archive %s bound (binding %s) and verified at prefix %s (encrypted=%v); active archiver: %s", binding.ArchiveID, binding.ID, prefix, encrypted, archiverSummary(st)))
	return ctxStash(ctx, env, "archiveID", binding.ArchiveID)
}

// pitrArchiveSwitchWait bounds a PITR enable or prefix switch (#203): the
// archive check Job (up to its 120 s deadline, plus a retry), the render,
// an ordered update of every site (each pod restart is ~45 s on CI, and
// the update fails over to move the writer), and the new writer's first
// archiver pass, which creates the descriptor that binds the prefix. CI
// run 6f3fc4b took 2.5 minutes from the patch to Bound; this leaves room
// for a slow pod start or a check retry.
const pitrArchiveSwitchWait = 12 * time.Minute

// pitrArchiveDestination is the normalized binding destination of the e2e
// bucket's archive at prefix (under <prefix>/binlogs).
func pitrArchiveDestination(prefix string) (string, error) {
	d, err := pitrarchive.NewS3Destination(backupE2EBucket, prefix, backupE2EEndpoint)
	if err != nil {
		return "", err
	}
	return d.String(), nil
}

// pitrWaitArchiveBound follows #203's procedure for a PITR enable or a new
// archive prefix: the operator checks the prefix (Pending/Authenticating,
// then Establishing for an empty one), renders it, rolls every site
// (writers move to the new prefix only once the ordered update has failed
// over), and the writer's descriptor binds it (Bound). It returns once the
// prefix's binding is Bound with an archive ID, status.pitr.archive
// verifies the same archive, and the group is Ready at its generation with
// no update in progress. Waiting only for the spec or for Ready returns
// while the binding is still Establishing (s70's remedy, CI 6f3fc4b).
func pitrWaitArchiveBound(ctx context.Context, env *runner.Env, prefix string) (*v1alpha1.MysqlFailoverGroup, *v1alpha1.PITRArchiveBinding, error) {
	dest, err := pitrArchiveDestination(prefix)
	if err != nil {
		return nil, nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, pitrArchiveSwitchWait)
	defer cancel()
	mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "PITR archive binding Bound, rollout done and archive verified at "+prefix, func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ok, msg := pitrArchiveBoundReady(mfg, prefix, dest)
		return ok, msg, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return mfg, pitrFindBindingByDestination(mfg, dest), nil
}

// pitrArchiveBoundReady is pitrWaitArchiveBound's predicate, with a summary
// of what it still waits on.
func pitrArchiveBoundReady(mfg *v1alpha1.MysqlFailoverGroup, prefix, dest string) (bool, string) {
	spec := backupProfileMatches(backupE2EProfileOf(mfg), prefix) && mfg.Spec.Backup.PITR != nil &&
		mfg.Spec.Backup.PITR.Enabled && mfg.Spec.Backup.PITR.ProfileName == backupE2EProfile
	b := pitrFindBindingByDestination(mfg, dest)
	bound := b != nil && b.State == v1alpha1.PITRArchiveBindingBound && b.ArchiveID != ""
	verified := bound && pitrObservedArchiveID(mfg, prefix) == b.ArchiveID
	ready := conditionTrueForGeneration(mfg.Status.Conditions, "Ready", mfg.Generation)
	settled := ready && backupProfileRolloutReady(mfg)
	binding := "<none>"
	if b != nil {
		binding = fmt.Sprintf("{id=%s %s/%s archive=%q msg=%q}", b.ID, b.State, b.Reason, b.ArchiveID, b.Message)
	}
	return spec && bound && verified && settled, fmt.Sprintf("spec=%v binding=%s archiveVerified=%v ready=%v updatePhase=%q active=%q archive=%s",
		spec, binding, verified, ready, mfg.Status.UpdatePhase, mfg.Status.ActiveSite, pitrArchiveSummary(mfg))
}

// pitrFindBindingByDestination returns the binding entry for a normalized
// destination, or nil.
func pitrFindBindingByDestination(mfg *v1alpha1.MysqlFailoverGroup, dest string) *v1alpha1.PITRArchiveBinding {
	for i := range mfg.Status.PITRArchiveBindings {
		if mfg.Status.PITRArchiveBindings[i].Destination == dest {
			return &mfg.Status.PITRArchiveBindings[i]
		}
	}
	return nil
}

// pitrObservedArchiveID is the verified archive ID status.pitr.archive
// reports for the profile prefix (whose archive lives under
// <prefix>/binlogs, pitrarchive.NormalizeKeyPrefix), or "". The observation names its
// destination: right after a prefix switch (s70's remedy) it can still
// describe the previous prefix, whose ID must not be taken for the new
// archive's.
func pitrObservedArchiveID(mfg *v1alpha1.MysqlFailoverGroup, prefix string) string {
	if mfg.Status.PITR == nil || mfg.Status.PITR.Archive == nil || mfg.Status.PITR.Archive.State != v1alpha1.PITRArchiveVerified {
		return ""
	}
	a := mfg.Status.PITR.Archive
	dest, err := pitrarchive.ParseDestination(a.Destination)
	if err != nil || dest.KeyPrefix != pitrarchive.NormalizeKeyPrefix(prefix) {
		return ""
	}
	return a.ArchiveID
}

func pitrArchiveSummary(mfg *v1alpha1.MysqlFailoverGroup) string {
	if mfg.Status.PITR == nil || mfg.Status.PITR.Archive == nil {
		return "<none>"
	}
	a := mfg.Status.PITR.Archive
	return fmt.Sprintf("{dest=%s id=%q state=%s reason=%s mode=%s}", a.Destination, a.ArchiveID, a.State, a.Reason, a.EncryptionMode)
}

// pitrCreateMarkerTable creates <db>.marker and the background writer's
// <db>.load on the active site.
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
		"DROP TABLE IF EXISTS " + db + ".load",
		"CREATE TABLE " + db + ".load (id BIGINT AUTO_INCREMENT PRIMARY KEY, site VARCHAR(32), created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))",
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

// pitrFence is one fenced observation of a primary: T, the whole-second
// RFC 3339 UTC target (pitrTargetLayout), and the exact executed set at T.
type pitrFence struct {
	Site string
	T    string
	GTID string
	// Manifest and File are set by a flushing fence: the site manifest's
	// bucket key and the binlog file the flush sealed, which the archiver
	// uploaded before the fence returned.
	Manifest string `json:",omitempty"`
	File     string `json:",omitempty"`
}

// pitrTakeFence fences site's primary (see the file comment). With flush it
// also seals the active binlog under the lock and waits until the archiver
// uploaded it, so fence.GTID is the sealed file's exec set.
func pitrTakeFence(ctx context.Context, env *runner.Env, site string, flush bool) (pitrFence, error) {
	var seal *pitrSealWatch
	if flush {
		var err error
		if seal, err = pitrStartSealWatch(ctx, env, site); err != nil {
			return pitrFence{}, err
		}
		defer seal.Close()
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
	// T is the next whole second after the server clock under the lock,
	// and the lock is held until the clock has passed it: T is strictly
	// after every fenced commit and strictly before every later one.
	now, err := pitrServerNow(fenceCtx, conn)
	if err != nil {
		return pitrFence{}, fmt.Errorf("read the server clock under lock on %s: %w", site, err)
	}
	target := pitrFenceTarget(now)
	wait := target.Sub(now) + 5*time.Millisecond
	if _, err := conn.ExecContext(fenceCtx, fmt.Sprintf("DO SLEEP(%.6f)", wait.Seconds())); err != nil {
		return pitrFence{}, err
	}
	if after, err := pitrServerNow(fenceCtx, conn); err != nil {
		return pitrFence{}, fmt.Errorf("re-read the server clock under lock on %s: %w", site, err)
	} else if !after.After(target) {
		return pitrFence{}, fmt.Errorf("fence on %s: the server clock %s has not passed the target %s after the sleep", site, after.Format(pitrMicroLayout), target.Format(pitrTargetLayout))
	}
	t := target.Format(pitrTargetLayout)
	if flush {
		if err := seal.flush(fenceCtx, conn); err != nil {
			return pitrFence{}, err
		}
	}
	if _, err := conn.ExecContext(fenceCtx, "UNLOCK TABLES"); err != nil {
		return pitrFence{}, fmt.Errorf("UNLOCK TABLES on %s: %w", site, err)
	}
	unlocked = true
	fence := pitrFence{Site: site, T: t, GTID: strings.ReplaceAll(strings.TrimSpace(gtid), "\n", "")}
	if flush {
		if err := seal.wait(ctx, env, true); err != nil {
			return pitrFence{}, err
		}
		fence.Manifest, fence.File = seal.key, seal.file
	}
	env.Capture.Note(fmt.Sprintf("fence on %s: T=%s flush=%v gtid=%s", site, fence.T, flush, fence.GTID))
	return fence, nil
}

// pitrQueryer is what the binlog-status read needs: a *sql.DB or a pinned
// *sql.Conn.
type pitrQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// pitrCurrentBinlog returns the binlog file the server is writing, which is
// the file the next FLUSH BINARY LOGS seals.
func pitrCurrentBinlog(ctx context.Context, q pitrQueryer) (string, error) {
	rows, err := q.QueryContext(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		return "", fmt.Errorf("SHOW BINARY LOG STATUS: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("SHOW BINARY LOG STATUS returned no row (binary logging off?)")
	}
	vals := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return "", err
	}
	for i, c := range cols {
		if strings.EqualFold(c, "File") && len(vals[i]) > 0 {
			return string(vals[i]), nil
		}
	}
	return "", fmt.Errorf("SHOW BINARY LOG STATUS has no File column (columns %v)", cols)
}

// pitrManifestRow identifies one archived row of a site manifest.
type pitrManifestRow struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

// pitrSealedFileArchived reports whether after holds a row for file that
// before did not: the upload of the file a flush sealed. A row of the same
// name already present before (an earlier incarnation's file, say) does
// not count (F185).
func pitrSealedFileArchived(before, after []pitrManifestRow, file string) bool {
	seen := make(map[pitrManifestRow]bool, len(before))
	for _, r := range before {
		seen[r] = true
	}
	for _, r := range after {
		if r.Name == file && !seen[r] {
			return true
		}
	}
	return false
}

// pitrManifestKey is the bucket key of site's manifest under the archiver's
// manifest prefix (the sidecar's manifestKey layout).
func pitrManifestKey(manifestPrefix, site string) string {
	return path.Join(strings.Trim(manifestPrefix, "/"), "manifest-"+site+".json")
}

// pitrManifestFile is one archived file of a site manifest, with the
// bucket key of its object.
type pitrManifestFile struct {
	Name       string `json:"name"`
	ID         string `json:"id"`
	RemotePath string `json:"remotePath"`
}

// pitrReadManifestRows reads site's manifest rows straight from RustFS; a
// manifest not written yet has none.
func pitrReadManifestRows(ctx context.Context, env *runner.Env, key string) ([]pitrManifestRow, error) {
	files, err := pitrReadManifest(ctx, env, key)
	if err != nil {
		return nil, err
	}
	rows := make([]pitrManifestRow, 0, len(files))
	for _, f := range files {
		rows = append(rows, pitrManifestRow{Name: f.Name, ID: f.ID})
	}
	return rows, nil
}

// pitrReadManifest reads and, for an encrypted archive, decrypts a site
// manifest (pitrDecryptArchiveObject).
func pitrReadManifest(ctx context.Context, env *runner.Env, key string) ([]pitrManifestFile, error) {
	data, found, err := env.Chaos.ReadRustFSObject(ctx, backupE2EBucket, key)
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", key, err)
	}
	if !found {
		return nil, nil
	}
	if data, err = pitrDecryptArchiveObject(env, data); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", key, err)
	}
	var m struct {
		Files []pitrManifestFile `json:"files"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", key, err)
	}
	return m.Files, nil
}

// pitrDecryptArchiveObject returns a plaintext archive object as is and
// decrypts a BRV1 one with the first archive passphrase this run stashed
// that opens it (pitrArchivePassphrases). It never reads a live Secret: a
// scenario that rotates the profile's Secret (s69) must still read the
// archive written under the bytes stashed before the rotation.
func pitrDecryptArchiveObject(env *runner.Env, data []byte) ([]byte, error) {
	if !backupcrypto.LooksEncrypted(data) {
		return data, nil
	}
	passes := pitrArchivePassphrases(env)
	if len(passes) == 0 {
		return nil, errors.New("the object is encrypted but this run stashed no archive passphrase")
	}
	var last error
	for _, pass := range passes {
		plain, err := pitrDecryptObject(data, pass)
		if err == nil {
			return plain, nil
		}
		last = err
	}
	return nil, fmt.Errorf("no stashed archive passphrase (%d tried) decrypts it: %w", len(passes), last)
}

// pitrSealWatch proves that the file one FLUSH BINARY LOGS seals reaches
// the site's manifest: it records the manifest rows before the flush and
// the file the flush seals, then waits for a new row of that file.
type pitrSealWatch struct {
	site   string
	probe  *pgsidecar.Probe
	key    string
	before []pitrManifestRow
	file   string
}

func pitrStartSealWatch(ctx context.Context, env *runner.Env, site string) (*pitrSealWatch, error) {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return nil, fmt.Errorf("open sidecar %s: %w", site, err)
	}
	st, err := probe.ArchiverStatus(ctx)
	if err != nil {
		probe.Close()
		return nil, fmt.Errorf("read pre-flush archiver status on %s: %w", site, err)
	}
	if st.ManifestPrefix == "" {
		probe.Close()
		return nil, fmt.Errorf("archiver on %s reports no manifest prefix: %s", site, archiverSummary(st))
	}
	manifestSite := st.Site
	if manifestSite == "" {
		manifestSite = site
	}
	w := &pitrSealWatch{site: site, probe: probe, key: pitrManifestKey(st.ManifestPrefix, manifestSite)}
	if w.before, err = pitrReadManifestRows(ctx, env, w.key); err != nil {
		probe.Close()
		return nil, err
	}
	return w, nil
}

func (w *pitrSealWatch) Close() { w.probe.Close() }

// flush records the file being written and seals it, on one connection so
// nothing rotates in between.
func (w *pitrSealWatch) flush(ctx context.Context, q pitrQueryer) error {
	file, err := pitrCurrentBinlog(ctx, q)
	if err != nil {
		return fmt.Errorf("read the active binlog on %s: %w", w.site, err)
	}
	if _, err := q.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
		return fmt.Errorf("FLUSH BINARY LOGS on %s: %w", w.site, err)
	}
	w.file = file
	return nil
}

// wait waits until the sealed file has a new row in the site's manifest.
// With clean it also requires no backlog and no archiver error.
func (w *pitrSealWatch) wait(ctx context.Context, env *runner.Env, clean bool) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		archived := false
		after, err := pitrReadManifestRows(waitCtx, env, w.key)
		if err != nil {
			last = err.Error()
		} else {
			archived = pitrSealedFileArchived(w.before, after, w.file)
			last = fmt.Sprintf("%s archived=%v (%d rows)", w.file, archived, len(after))
		}
		if archived {
			if !clean {
				return nil
			}
			st, err := w.probe.ArchiverStatus(waitCtx)
			if err != nil {
				last = err.Error()
			} else {
				last += "; " + archiverSummary(st)
				if st.BacklogFiles == 0 && st.LastError == "" {
					return nil
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("archiver on %s did not upload the sealed binlog %s to %s: %w (last: %s)", w.site, w.file, w.key, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

// pitrFlush seals the active binlog on site and waits until that file is in
// the site's manifest, without fencing.
func pitrFlush(ctx context.Context, env *runner.Env, site string, clean bool) error {
	seal, err := pitrStartSealWatch(ctx, env, site)
	if err != nil {
		return err
	}
	defer seal.Close()
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return err
	}
	defer c.Close()
	conn, err := c.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pin connection on %s: %w", site, err)
	}
	err = seal.flush(ctx, conn)
	_ = conn.Close()
	if err != nil {
		return err
	}
	return seal.wait(ctx, env, clean)
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

// pitrOnlyTargetLocalExtra reports whether got holds every GTID of want and
// its extra GTIDs all belong to server UUIDs absent from want: the restore
// target's own first-boot transactions, never a transaction of the restored
// history.
func pitrOnlyTargetLocalExtra(got, want string) (bool, error) {
	missing, err := gtidSubtract(want, got)
	if err != nil || missing != "" {
		return false, err
	}
	extra, err := gtidSubtract(got, want)
	if err != nil {
		return false, err
	}
	history := map[string]bool{}
	for _, u := range gtidSetUUIDs(want) {
		history[u] = true
	}
	for _, u := range gtidSetUUIDs(extra) {
		if history[u] {
			return false, nil
		}
	}
	return true, nil
}

// gtidSetUUIDs lists the server UUIDs of a canonical GTID set string
// ("uuid[:tag]:intervals,…").
func gtidSetUUIDs(set string) []string {
	var out []string
	for _, part := range strings.Split(strings.ReplaceAll(set, "\n", ""), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if i := strings.Index(part, ":"); i > 0 {
			out = append(out, strings.ToLower(part[:i]))
		}
	}
	return out
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

// gtidOverlap returns a ∩ b, canonical ("" when they share nothing).
func gtidOverlap(a, b string) (string, error) {
	as, err := brmysql.ParseGTIDSet(a)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", a, err)
	}
	bs, err := brmysql.ParseGTIDSet(b)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", b, err)
	}
	return as.Subtract(as.Subtract(bs)).String(), nil
}

// pitrReaderSites returns the group's read-only (reader) sites, in spec
// order. They replicate straight from the active site.
func pitrReaderSites(mfg *v1alpha1.MysqlFailoverGroup) []string {
	var out []string
	for _, s := range mfg.Spec.Sites {
		if s.IsReadOnlyReader() {
			out = append(out, s.Name)
		}
	}
	return out
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
	data, err := pitrReadDumpObject(ctx, env, b, "@.json")
	if err != nil {
		return "", err
	}
	var meta struct {
		GTIDExecuted string `json:"gtidExecuted"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return "", fmt.Errorf("decode @.json of %s: %w", backupName, err)
	}
	return strings.ReplaceAll(strings.TrimSpace(meta.GTIDExecuted), "\n", ""), nil
}

// pitrArchivePassphrasesKey stashes, as a JSON list of strings, every
// passphrase this run gave an archive profile, the run passphrase first.
// They are stashed when the Secret is created, before any rotation, so the
// helpers that decrypt archive objects never depend on a live Secret.
const pitrArchivePassphrasesKey = "pitrArchivePassphrases"

// pitrStashArchivePassphrase records a passphrase (trimmed, as the operator
// uses it) for pitrDecryptArchiveObject.
func pitrStashArchivePassphrase(env *runner.Env, pass []byte) {
	var list []string
	if raw := ctxFetch(env, pitrArchivePassphrasesKey); raw != "" {
		_ = json.Unmarshal([]byte(raw), &list)
	}
	p := string(backupcrypto.TrimPassphrase(pass))
	for _, have := range list {
		if have == p {
			return
		}
	}
	body, _ := json.Marshal(append(list, p))
	stashSet(env, pitrArchivePassphrasesKey, string(body))
}

// pitrArchivePassphrases lists the stashed archive passphrases, the run
// passphrase first.
func pitrArchivePassphrases(env *runner.Env) [][]byte {
	var list []string
	if raw := ctxFetch(env, pitrArchivePassphrasesKey); raw != "" {
		_ = json.Unmarshal([]byte(raw), &list)
	}
	out := make([][]byte, 0, len(list))
	for _, p := range list {
		out = append(out, []byte(p))
	}
	return out
}

// pitrBackupDumpPassphrase is the passphrase a backup's dump is encrypted
// with: the dump-key snapshot its status.artifactStorage.passphraseSecret
// records (#203: an immutable per-backup copy, so a later edit of the
// profile's Secret does not change it).
func pitrBackupDumpPassphrase(ctx context.Context, env *runner.Env, b *v1alpha1.MysqlBackup) ([]byte, error) {
	if b.Status.ArtifactStorage == nil || b.Status.ArtifactStorage.PassphraseSecret == nil || b.Status.ArtifactStorage.PassphraseSecret.Name == "" {
		return nil, fmt.Errorf("encrypted MysqlBackup %s records no status.artifactStorage.passphraseSecret", b.Name)
	}
	ref := b.Status.ArtifactStorage.PassphraseSecret
	sec, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get dump key %s of MysqlBackup %s: %w", ref.Name, b.Name, err)
	}
	key := ref.Key
	if key == "" {
		key = "passphrase"
	}
	pass := backupcrypto.TrimPassphrase(sec.Data[key])
	if len(pass) == 0 {
		return nil, fmt.Errorf("dump key %s of MysqlBackup %s has no %q", ref.Name, b.Name, key)
	}
	return pass, nil
}

// pitrBackup creates a MysqlBackup, waits for it, records B, and waits
// until targets are clear of it (pitrWaitPastBackup).
func pitrBackup(ctx context.Context, env *runner.Env, scenarioID, key string) error {
	return pitrBackupOfProfile(ctx, env, scenarioID, key, backupE2EProfile)
}

// pitrBackupOfProfile is pitrBackup of another profile.
func pitrBackupOfProfile(ctx context.Context, env *runner.Env, scenarioID, key, profile string) error {
	name := pitrShortName(scenarioID, "backup-"+key) + "-" + backupRunStamp(env)
	if err := ctxStash(ctx, env, "backup:"+key, name); err != nil {
		return err
	}
	pitrTrackCR(ctx, env, "MysqlBackup", name)
	if err := createMysqlBackupForProfile(ctx, env, name, scenarioID, profile); err != nil {
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
	return pitrWaitPastBackup(ctx, env, key, b)
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
	// ArchiveProfile is the verification's pointInTime.archiveProfileName
	// ("" = the group's active PITR profile).
	ArchiveProfile string
	// TargetLocal accepts, on a restore, the target server's own
	// transactions (users and grants its first boot wrote), which restore.py
	// keeps and certifies: gtidExecuted may hold extra GTIDs only under
	// server UUIDs that appear nowhere in the expected history.
	TargetLocal bool
}

// expectedSet is the fenced set minus excludeGtids.
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
		wantStop, err := time.Parse(pitrTargetLayout, e.Fence.T)
		if err != nil {
			return fmt.Errorf("%s: fence target %q: %w", what, e.Fence.T, err)
		}
		if res.StopTime == nil || !res.StopTime.UTC().Equal(wantStop) {
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
	if !equal && e.TargetLocal {
		if ok, err := pitrOnlyTargetLocalExtra(res.GtidExecuted, want); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		} else if ok {
			equal = true
		}
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
	// want is already the fenced set minus excludeGtids, so the replay is
	// (fenced ∖ exclusions) ∖ baseline.
	replay, err := gtidSubtract(want, baseline)
	if err != nil {
		return err
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
	pit := &v1alpha1.PointInTimeVerificationSpec{Mode: e.Mode, ExcludeGtids: e.Exclude, ArchiveProfileName: e.ArchiveProfile}
	if e.Mode == "timestamp" {
		pit.Timestamp = e.Fence.T
	}
	return pit
}

// pitrVerify runs one MysqlBackupVerification of e.Backup and asserts the
// certified result and the marker rows.
func pitrVerify(ctx context.Context, env *runner.Env, scenarioID, db, label string, e pitrExpectation) error {
	name, err := pitrStartVerify(ctx, env, scenarioID, db, label, e)
	if err != nil {
		return err
	}
	return pitrFinishVerify(ctx, env, name, e)
}

// pitrStartVerify creates the verification pitrVerify runs and returns
// its name without waiting for it.
func pitrStartVerify(ctx context.Context, env *runner.Env, scenarioID, db, label string, e pitrExpectation) (string, error) {
	if err := pitrCheckTargetAfterBackup(env, e); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s-%s", pitrShortName(scenarioID, label), backupRunStamp(env))
	pitrTrackCR(ctx, env, "MysqlBackupVerification", name)
	query := pitrMarkerQuery(db, ctxFetch(env, "backupRunStem"), e.Markers)
	if err := createMysqlBackupVerification(ctx, env, name, ctxFetch(env, "backup:"+e.Backup), scenarioID, pitrVerifySpec(e), query, 1); err != nil {
		return "", err
	}
	return name, nil
}

// pitrFinishVerify waits for a verification pitrStartVerify created and
// asserts the certified result and the marker rows.
func pitrFinishVerify(ctx context.Context, env *runner.Env, name string, e pitrExpectation) error {
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
	if err := pitrCheckTargetAfterBackup(env, e); err != nil {
		return nil, err
	}
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

// pitrSiteNow reads site's server clock. The sidecar shares the MySQL
// pod's (and so the node's) clock, so a promotion record's observedAt is
// comparable with it; the runner host's clock is not (F182).
func pitrSiteNow(ctx context.Context, env *runner.Env, site string) (time.Time, error) {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return time.Time{}, fmt.Errorf("open %s for its clock: %w", site, err)
	}
	defer c.Close()
	now, err := pitrServerNow(ctx, c.DB)
	if err != nil {
		return time.Time{}, fmt.Errorf("read the server clock on %s: %w", site, err)
	}
	return now, nil
}

// pitrWaitServerClockPast waits until site's server clock is strictly past t.
func pitrWaitServerClockPast(ctx context.Context, env *runner.Env, site string, t time.Time) error {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s for its clock: %w", site, err)
	}
	defer c.Close()
	return pitrWaitClockPast(ctx, c.DB, site, t)
}

func pitrWaitClockPast(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, site string, t time.Time) error {
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		now, err := pitrServerNow(waitCtx, q)
		if err != nil {
			return fmt.Errorf("read the server clock on %s: %w", site, err)
		}
		if now.After(t) {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("server clock on %s did not pass %s: %w", site, t.Format(pitrMicroLayout), waitCtx.Err())
		case <-time.After(t.Sub(now) + 10*time.Millisecond):
		}
	}
}

// pitrPlannedFailover runs a planned failover to target and waits for it
// to succeed, for target's archiver to be primary on the active prefix and
// archive, and for target's promotion record to be confirmed after the
// request (pitrWaitPromotionRecorded; tolerateDivergence as there). It
// returns the confirmed promotionRecordedAt. The request time is read on
// target's server clock, the clock its sidecar records with.
func pitrPlannedFailover(ctx context.Context, env *runner.Env, target string, tolerateDivergence bool) (time.Time, error) {
	if err := pitrWaitCooldown(ctx, env); err != nil {
		return time.Time{}, err
	}
	// The operator refuses a target that is not replicating
	// (TargetUnhealthy). After the previous failover the old primary
	// resumes replication asynchronously, so wait for status to say so.
	if err := pitrWaitSiteReplicating(ctx, env, target); err != nil {
		return time.Time{}, err
	}
	requested, err := pitrSiteNow(ctx, env, target)
	if err != nil {
		return time.Time{}, err
	}
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
	recorded, err := pitrWaitPromotionRecorded(ctx, env, target, requested, tolerateDivergence)
	if err != nil {
		return time.Time{}, err
	}
	env.Capture.Note(fmt.Sprintf("planned failover to %s succeeded; promotion recorded at %s", target, recorded.Format(time.RFC3339Nano)))
	return recorded, nil
}

// pitrWaitSiteReplicating waits until status reports site as a read-only
// replica that is replicating and not RecoveryBlocked, which a planned
// failover requires of its target (TargetUnhealthy, DivergentTarget).
func pitrWaitSiteReplicating(ctx context.Context, env *runner.Env, site string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	_, err := env.Wait.UntilCR(waitCtx, env.Namespace, site+" replicating", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		return pitrSiteReplicating(mfg, site)
	})
	return err
}

// pitrSiteReplicating reports whether status shows site replicating as a
// read-only replica with no divergence block. A recloned former primary
// (s70) replicates before the operator's replica GTID audit clears its
// RecoveryBlocked report, and validatePlannedFailoverRequest refuses a
// blocked target (DivergentTarget) whatever its threads say.
func pitrSiteReplicating(mfg *v1alpha1.MysqlFailoverGroup, site string) (bool, string, error) {
	for _, st := range mfg.Status.Sites {
		if st.Name != site {
			continue
		}
		msg := fmt.Sprintf("state=%s replicating=%t recovery=%q lastSeen=%v", st.State, st.Replicating, st.RecoveryState, st.LastSeen)
		return st.State == "read-only" && st.Replicating && st.RecoveryState != "RecoveryBlocked", msg, nil
	}
	return false, "site not in status", nil
}

// pitrWaitPromotionRecorded waits until site's archiver is the primary on
// the active prefix and this run's archive, and reports a promotion record
// observed after notBefore (pitrPromotionReady). It does not wait for a
// clean LastError when tolerateDivergence is set: after a lossy failover the
// recloned old primary's audit reports its deliberately retained rows for
// good, so waitForArchiverReady would never return.
func pitrWaitPromotionRecorded(ctx context.Context, env *runner.Env, site string, notBefore time.Time, tolerateDivergence bool) (time.Time, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	prefix := ctxFetch(env, "backupPrefix") + "/binlogs"
	archiveID := ctxFetch(env, "archiveID")
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
				var ready bool
				ready, last = pitrPromotionReady(st, prefix, archiveID, notBefore, tolerateDivergence)
				if ready {
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

// pitrRecloneRequest decides whether site can be recloned now and the
// reclone-site annotation value for it. With expectDivergent the site must
// be RecoveryBlocked with a recorded divergent GTID, which the value
// confirms by prefix; without it the site must be a read-only replica with
// no divergence recorded, confirmed by the group name (a cold reclone).
// Building the value before the divergence report settles would pick the
// wrong confirmation, which the operator rejects (F067).
func pitrRecloneRequest(sites []v1alpha1.SiteStatus, site, group string, expectDivergent bool) (ready bool, value, msg string) {
	for _, s := range sites {
		if s.Name != site {
			continue
		}
		div := strings.TrimSpace(s.DivergentGtid)
		msg = fmt.Sprintf("state=%s recovery=%s divergentGtid=%q", s.State, s.RecoveryState, div)
		if s.State != "read-only" {
			return false, "", msg
		}
		if expectDivergent {
			if s.RecoveryState != "RecoveryBlocked" || len(div) < 8 {
				return false, "", msg + " (waiting for RecoveryBlocked with a divergent GTID)"
			}
			return true, site + ":" + div[:8], msg
		}
		if div != "" || s.RecoveryState == "RecoveryBlocked" {
			return false, "", msg + " (a cold reclone needs a site with no recorded divergence)"
		}
		return true, site + ":confirm=" + group, msg
	}
	return false, "", "site absent"
}

// pitrRecloneWatch tells a reclone attempt's outcome apart from an earlier
// one's. setCondition keeps LastTransitionTime while Status is unchanged,
// so a Failed → Cloning → Failed attempt keeps the old time (Failed and
// every in-progress phase are Status=True): the attempt is identified by
// observing one of its in-progress phases, or by any change of the
// condition's reason, transition time or message from the pre-request
// snapshot (F188).
type pitrRecloneWatch struct {
	pre        *metav1.Condition
	sawAttempt bool
}

func newPitrRecloneWatch(pre *metav1.Condition) *pitrRecloneWatch {
	w := &pitrRecloneWatch{}
	if pre != nil {
		c := *pre
		w.pre = &c
	}
	return w
}

// observe classifies one Bootstrapping observation: done once this attempt
// finished and the site replicates, err once this attempt failed.
func (w *pitrRecloneWatch) observe(boot *metav1.Condition, replicating bool) (bool, string, error) {
	if boot == nil {
		return false, "bootstrap not observed", nil
	}
	msg := fmt.Sprintf("bootstrap=%s replicating=%v", boot.Reason, replicating)
	if boot.Status == metav1.ConditionTrue && boot.Reason != "Failed" {
		w.sawAttempt = true
		return false, msg, nil
	}
	changed := w.pre == nil || w.pre.Reason != boot.Reason || w.pre.Status != boot.Status ||
		!w.pre.LastTransitionTime.Equal(&boot.LastTransitionTime) || w.pre.Message != boot.Message
	if !w.sawAttempt && !changed {
		return false, msg + " (the condition still shows the state before the request)", nil
	}
	switch boot.Reason {
	case "Failed":
		return false, boot.Message, fmt.Errorf("reclone failed: %s", boot.Message)
	case "Done":
		return replicating, msg, nil
	}
	return false, msg, nil
}

// pitrNewRecloneRejection finds a RecloneRejected Event for site that is not
// in before (keyed by Event UID, with its count): a rejected annotation is
// cleared, so without this the completion wait would only time out (F067).
func pitrNewRecloneRejection(before map[string]int32, events []corev1.Event, group, site string) (corev1.Event, bool) {
	for _, ev := range events {
		if ev.InvolvedObject.Kind != "MysqlFailoverGroup" || ev.InvolvedObject.Name != group || ev.Reason != "RecloneRejected" {
			continue
		}
		if !strings.Contains(ev.Message, `"`+site+`"`) && !strings.Contains(ev.Message, "reclone of "+site+" ") {
			continue
		}
		if n, seen := before[string(ev.UID)]; seen && n >= ev.Count {
			continue
		}
		return ev, true
	}
	return corev1.Event{}, false
}

func pitrRecloneRejectedSnapshot(ctx context.Context, env *runner.Env) (map[string]int32, []corev1.Event, error) {
	events, err := env.Kube.RecentEvents(ctx, env.Namespace, 200)
	if err != nil {
		return nil, nil, err
	}
	seen := map[string]int32{}
	for _, ev := range events {
		if ev.Reason == "RecloneRejected" {
			seen[string(ev.UID)] = ev.Count
		}
	}
	return seen, events, nil
}

// pitrRecloneSite reclones site from the active site and waits for the
// clone to finish. expectDivergent selects the confirmation
// (pitrRecloneRequest): true for a RecoveryBlocked former primary, false
// for a healthy replica.
func pitrRecloneSite(ctx context.Context, env *runner.Env, site string, expectDivergent bool) error {
	readyCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	var value string
	mfg, err := env.Wait.UntilCR(readyCtx, env.Namespace, site+" ready for reclone", func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ready, v, msg := pitrRecloneRequest(m.Status.Sites, site, env.FG, expectDivergent)
		value = v
		return ready, msg, nil
	})
	if err != nil {
		return err
	}
	rejectedBefore, _, err := pitrRecloneRejectedSnapshot(ctx, env)
	if err != nil {
		return fmt.Errorf("list events before the reclone of %s: %w", site, err)
	}
	watch := newPitrRecloneWatch(findCondition(mfg.Status.Conditions, "Bootstrapping"))
	if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, "bloodraven.shipstream.io/reclone-site", value); err != nil {
		return err
	}
	env.Capture.Note("requested reclone-site=" + value)
	cloneCtx, cloneCancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cloneCancel()
	if _, err := env.Wait.UntilCR(cloneCtx, env.Namespace, "reclone of "+site+" completes", func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		if _, events, err := pitrRecloneRejectedSnapshot(cloneCtx, env); err == nil {
			if ev, rejected := pitrNewRecloneRejection(rejectedBefore, events, env.FG, site); rejected {
				return false, ev.Message, fmt.Errorf("reclone-site=%s was rejected: %s", value, ev.Message)
			}
		}
		replicating := false
		for _, s := range m.Status.Sites {
			if s.Name == site {
				replicating = s.Replicating && s.State == "read-only"
			}
		}
		done, msg, err := watch.observe(findCondition(m.Status.Conditions, "Bootstrapping"), replicating)
		if err != nil {
			return false, msg, fmt.Errorf("reclone of %s: %w", site, err)
		}
		return done, msg, nil
	}); err != nil {
		return err
	}
	env.Capture.Note("reclone of " + site + " completed")
	return nil
}

// pitrSourceSpecForRestore derives a minimal group spec that restores from
// the source group's backup and archive: the source's two primary-candidate
// sites without load balancers, readers, Dragonfly or encryption at rest,
// the source's backup profiles with PITR off and no schedules, and the
// restore. The seed (first priority) is the source's current active site,
// in its zone, whose node carries no read-only taint.
//
// The other site must be schedulable too: the operator seeds a new group
// (lifts the seed's fence and publishes it active, so the restore Job can
// load into it) only when every site is reachable, and the source's
// standby node carries the source's NoExecute read-only taint, which only
// the source's own pods tolerate. It goes in the zone of the source's
// read-only site, which the source never taints, when there is one.
//
// lbIP and taintNodeSelector are required for a primary candidate. The
// lbIPs are documentation addresses (RFC 5737; the Services are ClusterIP,
// so only the restored group's own DNS name points at them), and the
// selector is a group-scoped label no node carries: the restored group's
// taint must never land on a node where the source's MySQL runs, since the
// source does not tolerate it.
func pitrSourceSpecForRestore(src *v1alpha1.MysqlFailoverGroup, name, backupName, stop string) (*v1alpha1.MysqlFailoverGroup, error) {
	spec := src.Spec.DeepCopy()
	untaintedZone := ""
	for _, s := range spec.Sites {
		if s.Role == v1alpha1.SiteRoleReadOnly {
			untaintedZone = s.Zone
			break
		}
	}
	var sites []v1alpha1.SiteSpec
	var priorities []string
	for _, s := range spec.Sites {
		if !s.IsPromotable() {
			continue
		}
		s.TaintNodeSelector = map[string]string{"shipstream.io/failover-group." + name: "true"}
		s.LBIP = fmt.Sprintf("192.0.2.%d", len(sites)+1)
		s.ServiceTemplate = nil
		if s.Name == src.Status.ActiveSite {
			priorities = append([]string{s.Name}, priorities...)
		} else {
			if untaintedZone != "" {
				s.Zone = untaintedZone
			}
			priorities = append(priorities, s.Name)
		}
		sites = append(sites, s)
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

// pitrWriterInterval is the background writer's pace: one committed insert
// every 50 ms (about 20 transactions per second) on the era's primary.
const pitrWriterInterval = 50 * time.Millisecond

// pitrWriter commits rows into <db>.load on one site at pitrWriterInterval
// until stopped. It is what makes the fences' exactness claim observable:
// commits arrive before, during (blocked on the read lock) and after each
// fence.
type pitrWriter struct {
	stop    chan struct{}
	done    chan struct{}
	first   chan struct{}
	once    sync.Once
	commits atomic.Int64
	err     error
}

// pitrStartWriter starts a writer on site. The connection is opened before
// it returns, so a site that cannot be written fails the step here.
func pitrStartWriter(ctx context.Context, env *runner.Env, site, db string) (*pitrWriter, error) {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return nil, fmt.Errorf("open %s for the background writer: %w", site, err)
	}
	w := &pitrWriter{stop: make(chan struct{}), done: make(chan struct{}), first: make(chan struct{})}
	query := "INSERT INTO " + quoteSQLIdentifier(db) + ".load (site) VALUES (?)"
	go func() {
		defer close(w.done)
		defer c.Close()
		tick := time.NewTicker(pitrWriterInterval)
		defer tick.Stop()
		for {
			// The first insert runs at once, the rest on the tick. The
			// statement runs on the scenario context, not on stop: a stop
			// never cancels an insert in flight, so after Stop returns no
			// commit of this writer can still land.
			if _, err := c.Exec(ctx, query, site); err != nil {
				w.err = fmt.Errorf("background writer on %s after %d commits: %w", site, w.commits.Load(), err)
				return
			}
			w.commits.Add(1)
			w.once.Do(func() { close(w.first) })
			select {
			case <-w.stop:
				return
			case <-ctx.Done():
				w.err = ctx.Err()
				return
			case <-tick.C:
			}
		}
	}()
	return w, nil
}

// pitrWriterFirstCommitTimeout bounds the wait for a writer's first commit.
const pitrWriterFirstCommitTimeout = 30 * time.Second

// awaitFirst waits until the writer committed once, or failed, or the
// bound or ctx ran out.
func (w *pitrWriter) awaitFirst(ctx context.Context, site string) error {
	select {
	case <-w.first:
		return nil
	case <-w.done:
		select {
		case <-w.first:
			return nil
		default:
		}
		return fmt.Errorf("the background writer on %s stopped before its first commit: %v", site, w.err)
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(pitrWriterFirstCommitTimeout):
		return fmt.Errorf("the background writer on %s committed nothing within %s", site, pitrWriterFirstCommitTimeout)
	}
}

// Stop waits for the writer's last insert to finish and returns how many
// it committed, or its first error.
func (w *pitrWriter) Stop() (int64, error) {
	close(w.stop)
	<-w.done
	return w.commits.Load(), w.err
}

// pitrWithWriter runs fn while a background writer commits on site, then
// stops the writer. fn starts only after the writer's first commit, so a
// fast fn cannot end the window before the writer's first insert (F189). A
// writer that failed, or committed nothing, fails the step: the window was
// meant to have concurrent writes.
func pitrWithWriter(ctx context.Context, env *runner.Env, site, db string, fn func() error) error {
	w, err := pitrStartWriter(ctx, env, site, db)
	if err != nil {
		return err
	}
	if err := w.awaitFirst(ctx, site); err != nil {
		_, werr := w.Stop()
		return errors.Join(err, werr)
	}
	fnErr := fn()
	n, werr := w.Stop()
	if fnErr != nil {
		return errors.Join(fnErr, werr)
	}
	if werr != nil {
		return werr
	}
	if n == 0 {
		return fmt.Errorf("the background writer on %s committed nothing during the window", site)
	}
	env.Capture.Note(fmt.Sprintf("background writer on %s committed %d transactions during the window", site, n))
	return nil
}

// pitrReadDumpObject reads one object of a backup's dump from RustFS,
// decrypting it with the backup's recorded dump key when the dump is
// encrypted.
func pitrReadDumpObject(ctx context.Context, env *runner.Env, b *v1alpha1.MysqlBackup, name string) ([]byte, error) {
	bucket, prefix, ok, err := backupArtifactPrefix(b)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("MysqlBackup %s has no S3 artifact", b.Name)
	}
	key := prefix + name
	data, found, err := env.Chaos.ReadRustFSObject(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	if !found {
		return nil, fmt.Errorf("dump object s3://%s/%s not found", bucket, key)
	}
	if b.Status.Encrypted {
		pass, err := pitrBackupDumpPassphrase(ctx, env, b)
		if err != nil {
			return nil, err
		}
		if data, err = pitrDecryptObject(data, pass); err != nil {
			return nil, fmt.Errorf("decrypt %s: %w", key, err)
		}
	}
	return data, nil
}

// pitrDecryptObject decrypts one BRV1 object read straight from the store.
func pitrDecryptObject(data, passphrase []byte) ([]byte, error) {
	var plain bytes.Buffer
	if _, err := backupcrypto.Decrypt(&plain, bytes.NewReader(data), passphrase); err != nil {
		return nil, err
	}
	return plain.Bytes(), nil
}

// pitrParseDumpEnd parses @.done.json's end, which the dump container
// writes in UTC ("YYYY-MM-DD HH:MM:SS[.ffffff]"), as dump-meta does.
func pitrParseDumpEnd(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", time.RFC3339Nano, "2006-01-02T15:04:05.999999999"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("dump end %q is not a timestamp", s)
}

// pitrBackupClearTime is the earliest instant after which a target is
// clear of a backup (D-52): a whole second past both the dumped server's
// clock when the dump finished and the dump's own end, rounded up to the
// whole second. serverClock.end is read by the backup Job before it exits,
// so serverSeen, a server clock reading taken after the backup reported
// Succeeded, bounds it from above without reading the stamp.
func pitrBackupClearTime(serverSeen, dumpEnd time.Time) time.Time {
	end := dumpEnd.UTC()
	if end.Truncate(time.Second) != end {
		end = end.Truncate(time.Second).Add(time.Second)
	}
	clear := serverSeen.UTC()
	if end.After(clear) {
		clear = end
	}
	return clear.Add(time.Second)
}

// pitrWaitPastBackup waits until the server clock is past
// pitrBackupClearTime for backup b, so every later fence's T satisfies
// T ≥ serverClock.end + 1 s and T > ceil_s(@.done.json end), and stashes the
// bound for pitrCheckTargetAfterBackup.
func pitrWaitPastBackup(ctx context.Context, env *runner.Env, key string, b *v1alpha1.MysqlBackup) error {
	raw, err := pitrReadDumpObject(ctx, env, b, "@.done.json")
	if err != nil {
		return err
	}
	var done struct {
		End string `json:"end"`
	}
	if err := json.Unmarshal(raw, &done); err != nil {
		return fmt.Errorf("decode @.done.json of %s: %w", b.Name, err)
	}
	dumpEnd, err := pitrParseDumpEnd(done.End)
	if err != nil {
		return fmt.Errorf("backup %s: %w", b.Name, err)
	}
	site := b.Status.SourceSite
	if site == "" {
		if site, _, err = activeAndReplica(ctx, env); err != nil {
			return err
		}
	}
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s for the backup clock: %w", site, err)
	}
	defer c.Close()
	seen, err := pitrServerNow(ctx, c.DB)
	if err != nil {
		return fmt.Errorf("read the server clock on %s: %w", site, err)
	}
	clear := pitrBackupClearTime(seen, dumpEnd)
	if err := pitrWaitClockPast(ctx, c.DB, site, clear); err != nil {
		return err
	}
	env.Capture.Note(fmt.Sprintf("backup %s: dump end %s, server clock after Succeeded %s; targets are clear after %s",
		b.Name, dumpEnd.Format(pitrMicroLayout), seen.Format(pitrMicroLayout), clear.Format(pitrMicroLayout)))
	return ctxStash(ctx, env, "backupClear:"+key, clear.Format(pitrMicroLayout))
}

// pitrCheckTargetAfterBackup fails a timestamp expectation whose T is not
// past its backup's clear time: such a target would refuse
// PITRBackupAfterTarget for a reason the scenario did not mean to test.
func pitrCheckTargetAfterBackup(env *runner.Env, e pitrExpectation) error {
	if e.Mode != "timestamp" {
		return nil
	}
	raw := ctxFetch(env, "backupClear:"+e.Backup)
	if raw == "" {
		return fmt.Errorf("backup %q has no recorded clear time; take it with pitrBackup", e.Backup)
	}
	clear, err := time.Parse(pitrMicroLayout, raw)
	if err != nil {
		return err
	}
	t, err := time.Parse(pitrTargetLayout, e.Fence.T)
	if err != nil {
		return fmt.Errorf("target %q: %w", e.Fence.T, err)
	}
	if t.Before(clear) {
		return fmt.Errorf("target %s is before backup %s's clear time %s (D-52); the scenario's fence came too early", e.Fence.T, e.Backup, raw)
	}
	return nil
}

// pitrDivergenceAuditError matches the sidecar's history-audit LastError for
// archived rows the server no longer has (internal/sidecar
// binlog_history.go auditArchive), possibly behind an unsafe-history prefix.
var pitrDivergenceAuditError = regexp.MustCompile(`site \S+ name \S+ id \S* P=\S* divergent transactions=\S+; preserve objects and use pitr-quarantine`)

// pitrPromotionReady reports whether an archiver status shows site's
// promotion confirmed: primary on the active prefix and the expected
// archive, scanned, with a promotion record observed no earlier than
// notBefore. notBefore must be read on the site's server clock
// (pitrSiteNow), which the sidecar shares, so no cross-host slack is
// needed (F182). With tolerateDivergence the divergence audit's LastError
// is accepted while the site's manifest holds divergent or quarantined
// rows: the audit keeps reporting rows a lossy failover deliberately left
// behind (s70), and that error never clears. Any other error, or the audit
// error with no such rows, is not ready (F181).
func pitrPromotionReady(st *pgsidecar.ArchiverStatusResponse, prefix, archiveID string, notBefore time.Time, tolerateDivergence bool) (bool, string) {
	if st == nil {
		return false, "no status"
	}
	summary := fmt.Sprintf("promotionRecordedAt=%s divergentRows=%d quarantinedRows=%d %s", st.PromotionRecordedAt.Format(time.RFC3339Nano), st.DivergentRows, st.QuarantinedRows, archiverSummary(st))
	if !st.Enabled || !st.Primary || st.LastScanAt.IsZero() || strings.Trim(st.ManifestPrefix, "/") != strings.Trim(prefix, "/") {
		return false, summary
	}
	if st.Archive == nil || st.Archive.State != pitrarchive.StateVerified || (archiveID != "" && st.Archive.ArchiveID != archiveID) {
		return false, summary + fmt.Sprintf(" archive=%+v want %s", st.Archive, archiveID)
	}
	if st.LastError != "" && !(tolerateDivergence && st.DivergentRows+st.QuarantinedRows > 0 && pitrDivergenceAuditError.MatchString(st.LastError)) {
		return false, summary
	}
	if st.PromotionRecordedAt.IsZero() || st.PromotionRecordedAt.Before(notBefore) {
		return false, summary
	}
	return true, summary
}

// gtidSetPattern matches one canonical GTID set in a message: comma-joined
// <uuid>[:<tag>]:<n>[-<m>]... elements, as pitrplan's Set.String() prints.
var gtidSetPattern = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?::[a-z_][a-z0-9_]{0,31})?(?::[0-9]+(?:-[0-9]+)?)+(?:,[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?::[a-z_][a-z0-9_]{0,31})?(?::[0-9]+(?:-[0-9]+)?)+)*`)

// refusalNamesExactly checks that msg prints a GTID set equal to want (as
// sets): a refusal that names a superset, a subset or another interval of
// the same source fails.
func refusalNamesExactly(msg, want string) error {
	found := gtidSetPattern.FindAllString(msg, -1)
	for _, set := range found {
		if eq, err := gtidSetsEqual(set, want); err == nil && eq {
			return nil
		}
	}
	return fmt.Errorf("the refusal names the sets %q, none equal to %s: %s", found, want, msg)
}

// Metadata of the #203 archive bindings (IMPL-203 §3.5). The operator's
// constants are unexported (internal/controller/pitr_binding_contract.go);
// these mirror them, and TestPITRPlaygroundContractMirrors pins each mirror
// to the controller's declaration, so a drifted value fails a unit test
// rather than turning a scenario assertion vacuous.
const (
	pitrSnapshotAppLabel       = "app.kubernetes.io/name"
	pitrKeySnapshotApp         = "mysql-pitr-key"
	pitrDumpKeySnapshotApp     = "mysql-dump-key"
	pitrGroupUIDLabel          = "bloodraven.shipstream.io/failover-group-uid"
	pitrBindingIDLabel         = "bloodraven.shipstream.io/pitr-binding-id"
	pitrDestinationAnnotation  = "bloodraven.shipstream.io/pitr-destination"
	pitrAcceptedAnnotation     = "bloodraven.shipstream.io/pitr-accepted"
	pitrRenderAnnotation       = "bloodraven.shipstream.io/pitr-render"
	pitrCheckJobSelector       = "app.kubernetes.io/name=mysql-backup,shipstream.io/resource=pitr-archive-check"
	pitrBindingReadyCondition  = "PITRArchiveBindingReady"
	pitrSnapshotPassphraseKey  = "passphrase"
	pitrReleaseBindingWaitTime = 2 * time.Minute
	// pitrSnapshotUnmountWaitTime bounds the wait, before the cleanup's
	// releases, for the pods that still mount the run's key snapshots
	// (old ReplicaSets, deleted verification and restore pods) to finish
	// terminating.
	pitrSnapshotUnmountWaitTime = 3 * time.Minute
)

// pitrKeySnapshotSelector lists a group's archive key snapshots. They have
// no owner reference (they outlive the group), so the group-UID label is
// what scopes them.
func pitrKeySnapshotSelector(groupUID string) string {
	return pitrSnapshotAppLabel + "=" + pitrKeySnapshotApp + "," + pitrGroupUIDLabel + "=" + groupUID
}

// pitrGroupSnapshotSelector lists every key snapshot (archive and dump) a
// group's operator made.
func pitrGroupSnapshotSelector(groupUID string) string {
	return pitrSnapshotAppLabel + " in (" + pitrKeySnapshotApp + "," + pitrDumpKeySnapshotApp + ")," + pitrGroupUIDLabel + "=" + groupUID
}

// pitrDestinationUnderPrefixes reports whether a normalized destination is
// an S3 archive in the e2e bucket under one of the profile prefixes, in any
// endpoint spelling (s69's read alias reaches P1 through another one).
func pitrDestinationUnderPrefixes(destination string, prefixes []string) bool {
	d, err := pitrarchive.ParseDestination(destination)
	if err != nil || d.Kind != pitrarchive.DestinationS3 || d.Bucket != backupE2EBucket {
		return false
	}
	for _, p := range prefixes {
		if d.KeyPrefix == pitrarchive.NormalizeKeyPrefix(p) {
			return true
		}
	}
	return false
}

// pitrBindingIDsUnderPrefixes is every binding id of the group that covers
// one of the prefixes: its status entries, and the id label of every
// archive key snapshot whose destination is there (an entry that was
// dropped or lost still has releasable snapshots). Sorted, unique.
func pitrBindingIDsUnderPrefixes(mfg *v1alpha1.MysqlFailoverGroup, snapshots []corev1.Secret, prefixes []string) []string {
	seen := map[string]bool{}
	for _, b := range mfg.Status.PITRArchiveBindings {
		if pitrDestinationUnderPrefixes(b.Destination, prefixes) {
			seen[b.ID] = true
		}
	}
	for i := range snapshots {
		s := &snapshots[i]
		if id := s.Labels[pitrBindingIDLabel]; id != "" && pitrDestinationUnderPrefixes(s.Annotations[pitrDestinationAnnotation], prefixes) {
			seen[id] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// pitrFindBindingByID returns the group's binding entry with id, or nil.
func pitrFindBindingByID(mfg *v1alpha1.MysqlFailoverGroup, id string) *v1alpha1.PITRArchiveBinding {
	for i := range mfg.Status.PITRArchiveBindings {
		if mfg.Status.PITRArchiveBindings[i].ID == id {
			return &mfg.Status.PITRArchiveBindings[i]
		}
	}
	return nil
}

// pitrListKeySnapshots lists the group's archive key snapshots, optionally
// only those of one binding id.
func pitrListKeySnapshots(ctx context.Context, env *runner.Env, groupUID, id string) ([]corev1.Secret, error) {
	sel := pitrKeySnapshotSelector(groupUID)
	if id != "" {
		sel += "," + pitrBindingIDLabel + "=" + id
	}
	list, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, fmt.Errorf("list archive key snapshots (%s): %w", sel, err)
	}
	return list.Items, nil
}

// pitrReleaseValue is the release annotation's value for one binding id
// (`<id>:confirm=<group>`, one id per annotation).
func pitrReleaseValue(id, group string) string {
	return id + ":confirm=" + group
}

// pitrReleaseBinding asks the operator to release one binding of the
// scenario's group and waits up to pitrReleaseBindingWaitTime until the
// annotation is consumed, the entry is gone and no snapshot of the id is
// left. On a timeout (or the caller's deadline) it takes its own annotation
// back (pitrWithdrawRelease), so a refused release does not fire later.
func pitrReleaseBinding(ctx context.Context, env *runner.Env, id string) error {
	value := pitrReleaseValue(id, env.FG)
	if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, brcontroller.ReleasePITRArchiveBindingAnnotation, value); err != nil {
		return fmt.Errorf("annotate the release of %s: %w", id, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, pitrReleaseBindingWaitTime)
	defer cancel()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var last string
	for {
		mfg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, env.FG)
		if err != nil {
			last = err.Error()
		} else {
			pending := mfg.Annotations[brcontroller.ReleasePITRArchiveBindingAnnotation] == value
			entry := pitrFindBindingByID(mfg, id)
			snaps, err := pitrListKeySnapshots(waitCtx, env, string(mfg.UID), id)
			switch {
			case err != nil:
				last = err.Error()
			case !pending && entry == nil && len(snaps) == 0:
				env.Capture.Note(fmt.Sprintf("released PITR archive binding %s", id))
				return nil
			case !pending && entry != nil:
				return fmt.Errorf("the operator removed the release annotation for %s but kept the entry (%s/%s: %s): the release was rejected", id, entry.State, entry.Reason, entry.Message)
			default:
				last = fmt.Sprintf("annotation pending=%v entry=%v snapshots=%d", pending, entry != nil, len(snaps))
			}
		}
		select {
		case <-waitCtx.Done():
			err := fmt.Errorf("release of PITR archive binding %s did not finish in %s (last: %s)", id, pitrReleaseBindingWaitTime, last)
			if werr := pitrWithdrawRelease(ctx, env, value); werr != nil {
				return errors.Join(err, werr)
			}
			return err
		case <-tick.C:
		}
	}
}

// pitrReleaseWithdrawTimeout bounds pitrWithdrawRelease. It runs on a
// context detached from the caller's: the withdrawal matters most when the
// cleanup's own deadline is what ended the wait.
const pitrReleaseWithdrawTimeout = 30 * time.Second

// pitrWithdrawRelease removes the release annotation only while it still
// holds this request's value (a JSON Patch test guards the removal, so a
// newer request is never taken back). An annotation the operator already
// consumed, or that now holds another value, needs no withdrawal.
func pitrWithdrawRelease(ctx context.Context, env *runner.Env, value string) error {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pitrReleaseWithdrawTimeout)
	defer cancel()
	err := env.Kube.PatchMFGNamed(wctx, env.Namespace, env.FG, pitrReleaseWithdrawPatch(value))
	if err == nil {
		env.Capture.Note("withdrew the pending release request " + value)
		return nil
	}
	mfg, gerr := env.Kube.GetMFGNamed(wctx, env.Namespace, env.FG)
	if gerr == nil && mfg.Annotations[brcontroller.ReleasePITRArchiveBindingAnnotation] != value {
		return nil
	}
	return fmt.Errorf("withdraw the release request %s: %w; remove the %s annotation by hand or the operator keeps retrying it", value, errors.Join(err, gerr), brcontroller.ReleasePITRArchiveBindingAnnotation)
}

// pitrReleaseWithdrawPatch removes the release annotation if, and only if,
// it still equals value.
func pitrReleaseWithdrawPatch(value string) []pgkube.JSONPatchOp {
	p := "/metadata/annotations/" + strings.NewReplacer("~", "~0", "/", "~1").Replace(brcontroller.ReleasePITRArchiveBindingAnnotation)
	return []pgkube.JSONPatchOp{{Op: "test", Path: p, Value: value}, {Op: "remove", Path: p}}
}

// pitrReleaseBindings is the PITR cleanups' binding hygiene (IMPL-203
// Δ20): archive key snapshots outlive the group and are never pruned, and
// a retained Bound entry counts against the 16-binding limit, so every
// binding this run created under its archive prefixes is released, one
// annotation per id (an id whose entry was dropped is still released
// through the snapshots that carry its label). It runs after the original
// spec.backup is back and the group is healthy. Snapshots are removed only
// through the operator's release, which refuses while a pod mounts one, an
// unfinished Job labels one or a live verification holds one: a snapshot
// left after the releases is reported, never deleted here, since deleting
// it would bypass exactly that protection. Failures are cleanup warnings:
// they are noted, never returned.
func pitrReleaseBindings(ctx context.Context, env *runner.Env, prefixes []string) {
	if len(prefixes) == 0 {
		return
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		env.Capture.Note("cleanup warning: read the group to release PITR bindings: " + err.Error())
		return
	}
	uid := string(mfg.UID)
	snaps, err := pitrListKeySnapshots(ctx, env, uid, "")
	if err != nil {
		env.Capture.Note("cleanup warning: " + err.Error())
		return
	}
	// The operator refuses to release a snapshot a non-terminal pod still
	// mounts, and the healthy baseline does not wait for the pods of an
	// old ReplicaSet or a deleted verification to finish terminating.
	names := map[string]bool{}
	for _, n := range pitrSnapshotsUnderPrefixes(snaps, prefixes) {
		names[n] = true
	}
	if err := pitrWaitSnapshotsUnmounted(ctx, env, names); err != nil {
		env.Capture.Note("cleanup warning: " + err.Error())
	}
	for _, id := range pitrBindingIDsUnderPrefixes(mfg, snaps, prefixes) {
		if err := pitrReleaseBinding(ctx, env, id); err != nil {
			env.Capture.Note("cleanup warning: " + err.Error())
		}
	}
	if snaps, err = pitrListKeySnapshots(ctx, env, uid, ""); err != nil {
		env.Capture.Note("cleanup warning: " + err.Error())
		return
	}
	if left := pitrSnapshotsUnderPrefixes(snaps, prefixes); len(left) > 0 {
		env.Capture.Note(fmt.Sprintf("cleanup warning: archive key snapshots %v under this run's prefixes outlived the operator's release and are retained; release them with the %s annotation once nothing uses them", left, brcontroller.ReleasePITRArchiveBindingAnnotation))
	}
}

// pitrSnapshotsUnderPrefixes names the snapshots whose destination is under
// one of the prefixes, sorted.
func pitrSnapshotsUnderPrefixes(snaps []corev1.Secret, prefixes []string) []string {
	var out []string
	for i := range snaps {
		if pitrDestinationUnderPrefixes(snaps[i].Annotations[pitrDestinationAnnotation], prefixes) {
			out = append(out, snaps[i].Name)
		}
	}
	sort.Strings(out)
	return out
}

// pitrPodsMountingSecrets names the pods that mount one of the Secrets
// through a secret volume and are not terminal, which is what the
// operator's release in-use scan counts (a terminating pod is still
// Running).
func pitrPodsMountingSecrets(pods []corev1.Pod, names map[string]bool) []string {
	var out []string
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, v := range p.Spec.Volumes {
			if v.Secret != nil && names[v.Secret.SecretName] {
				out = append(out, p.Name+" mounts "+v.Secret.SecretName)
				break
			}
		}
	}
	return out
}

// pitrWaitSnapshotsUnmounted waits up to pitrSnapshotUnmountWaitTime until
// no non-terminal pod in the namespace mounts one of the named snapshots.
func pitrWaitSnapshotsUnmounted(ctx context.Context, env *runner.Env, names map[string]bool) error {
	if len(names) == 0 {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, pitrSnapshotUnmountWaitTime)
	defer cancel()
	var last string
	for {
		pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(waitCtx, metav1.ListOptions{})
		if err != nil {
			last = err.Error()
		} else if users := pitrPodsMountingSecrets(pods.Items, names); len(users) == 0 {
			return nil
		} else {
			last = strings.Join(users, "; ")
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("key snapshots still mounted after %s: %s", pitrSnapshotUnmountWaitTime, last)
		case <-time.After(2 * time.Second):
		}
	}
}

// pitrDeleteGroupKeySnapshots deletes every archive and dump key snapshot
// of a group this run deleted (by its UID label: they have no owner, so
// group deletion leaves them).
func pitrDeleteGroupKeySnapshots(ctx context.Context, env *runner.Env, groupUID string) error {
	if groupUID == "" {
		return nil
	}
	list, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: pitrGroupSnapshotSelector(groupUID)})
	if err != nil {
		return fmt.Errorf("list key snapshots of group %s: %w", groupUID, err)
	}
	var errs []error
	for i := range list.Items {
		if err := pitrDeleteSecretExact(ctx, env, &list.Items[i]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// pitrDeleteSecretExact deletes exactly the Secret object read (UID
// precondition).
func pitrDeleteSecretExact(ctx context.Context, env *runner.Env, s *corev1.Secret) error {
	uid := s.UID
	err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Delete(ctx, s.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete key snapshot %s: %w", s.Name, err)
	}
	env.Capture.Note("deleted key snapshot " + s.Name)
	return nil
}
