package sidecar

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// BinlogArchiver uploads sealed binlog files from the local MySQL data
// directory to remote storage, and keeps a per-site manifest describing
// what was archived. It runs inside the sidecar on every MySQL pod but
// uploads every sealed binlog only while the local MySQL is the primary.
// That keeps archive ownership unambiguous across failovers without
// coordinating between sites: whichever side owns the write role also
// owns its binlog archival. A site whose server turned read-only still
// archives the sealed tail of its last writable era, and no replica
// copy (binlog_tail.go).
//
// The write path is event-driven via inotify on the binlog index file,
// with a periodic full scan as a safety net. On each trigger the
// archiver:
//
//  1. Reads the index file, which MySQL maintains as a list of active
//     + historical binlog basenames, one per line.
//  2. Subtracts the last line (the currently-open file) — that one
//     isn't sealed yet.
//  3. Subtracts content identities already archived or durably retired.
//  4. For each remaining file: parse metadata, upload to storage,
//     append to manifest.
//
// The archiver deduplicates by basename and sealed plaintext content,
// preserving older files when a rebuilt site's binlog numbering restarts.
type BinlogArchiver struct {
	// Identity work and cursors belong to the scan goroutine, not the status mutex.
	retentionNextInspection     int
	retentionInspectionFailures map[binlogIdentity]time.Time
	historyTask                 *binlogHistoryTask
	historyNext                 int
	historyAuditError           string
	historyScanBusy             bool
	reuseWork                   *archiveReuseWork
	reuseNext                   int
	reuseFailed                 map[archiveReuseKey]time.Time
	reuseErrors                 map[archiveReuseKey]error
	reuseCompared               map[archiveReuseKey]bool
	sealedSnapshot              map[string]os.FileInfo
	localIDs                    map[string]cachedBinlogIdentity
	// The read-only tail drain (binlog_tail.go): its memory, its running
	// upload pass, and what the last scanFiles pass uploaded and reconciled.
	tail         tailDrain
	tailUpload   *tailUpload
	scanUploaded []string
	scanSettled  map[string]bool
	// uploadFailure is the last pass of uploads that failed, a writable
	// scan's or a tail drain's, while no later scan has settled it
	// (binlog_tail.go, tailOutcome).
	uploadFailure *uploadFailure
	cfg           *PITRConfig
	// retentionCfg holds the fields the archiver needs to call the
	// operator's /pitr-cutoff endpoint for retention decisions. Nil
	// when those fields weren't passed via env (tests, or a
	// standalone dev sidecar running without an operator).
	retentionCfg *retentionClientConfig
	site         string
	mysql        roleChecker
	store        ArchiveStore
	logger       *slog.Logger
	now          func() time.Time

	// Archive identity (§3.5-§3.7), owned by the scan goroutine. session
	// is nil only when the configured identity is unusable (identityErr).
	session     *ArchiveSession
	identityErr error
	episode     promotionEpisode
	// promotionSets reads local binlog headers for a promotion record's
	// promotion set (promotion_record.go).
	promotionSets promotionSetReader
	// startChecked: this process's first identified role observation was
	// checked against the site's latest record (recordResets).
	startChecked bool
	// roles is fed by the role sampler goroutine and the scan; it has
	// its own lock.
	roles           roleTimeline
	persistedMarker *HistoryIntegrity
	pendingMarker   *HistoryIntegrity
	// pendingMarkerErr is an unreadable pending-marker file; it blocks
	// uploads like a marker, since it may hold one.
	pendingMarkerErr error
	// pendingMarkerSaved: the pending marker's file is durable (or there
	// is no state directory); a failed write is retried every scan.
	pendingMarkerSaved bool
	unsafeLogged       bool

	// mu guards lastError, lastScanAt, filesArchiv, lastRetentionRun,
	// uploadFailures, lastUploadAt, backlogFiles, tailStatus,
	// reissuableGTIDs, and the manifest aggregates
	// (manifestFileCount/Bytes/oldest/newest).
	mu               sync.Mutex
	lastError        string
	lastScanAt       time.Time
	filesArchiv      int64
	lastRetentionRun time.Time
	// uploadFailures is the cumulative count of failed archive attempts
	// since sidecar start. Increments once per binlog whose upload or
	// manifest append errors out, including role-check / index-read
	// failures that prevent the scan from progressing.
	uploadFailures int64
	// lastUploadAt is the wall-clock time of the most recent successful
	// archive (ManifestEntry appended). Zero until the first success.
	lastUploadAt time.Time
	// failedScans is the number of consecutive scan cycles (scan plus
	// its maintenance pass) that ended with lastError set. Reset by a
	// cycle that ends clean.
	failedScans int64
	// backlogFiles is the count of sealed index entries that were
	// missing from the manifest at the end of the most recent scan.
	// Reset every cycle. >0 means the archiver hasn't caught up yet.
	backlogFiles int64
	// tailStatus is the tail block of the most recent scan (publishTail);
	// nil until a scan has read the binlog index.
	tailStatus *internalmysql.ArchiverTailStatus
	// manifestFileCount/Bytes and oldest/newestArchivedTime are
	// aggregates computed from the manifest after each scan.
	// They power the operator's status.pitr fields.
	manifestFileCount  int64
	manifestBytes      int64
	oldestArchivedTime time.Time
	newestArchivedTime time.Time
	// divergentRows/quarantinedRows count this site's manifest rows;
	// reissuableGTIDs is the last maintenance pass's verdict on what this
	// server could reissue (nil: not evaluated); promotionRecordedAt is the
	// confirmed record of the current writable episode; historyIntegrity is
	// the site's marker (persisted or pending).
	divergentRows       int64
	quarantinedRows     int64
	reissuableGTIDs     *string
	promotionRecordedAt time.Time
	historyIntegrity    *HistoryIntegrity
}

// retentionClientConfig is what the archiver needs to query the
// operator for the current retention cutoff. All of it comes from the
// sidecar env; set by the caller via SetRetentionClient.
type retentionClientConfig struct {
	BloodravenAddress string
	Namespace         string
	FailoverGroup     string
	// ArchiveID is the archive this pod writes (BLOODRAVEN_PITR_ARCHIVE_ID).
	// The operator only answers for an archive the group itself creates.
	ArchiveID string
	// Interval between retention sweeps. Defaults to 1h to avoid
	// putting pressure on the operator aux endpoint + repeatedly
	// listing S3.
	Interval time.Duration
}

// roleChecker is the narrow subset of mysqlQuerier that the archiver
// needs. Using a minimal interface lets tests substitute a stub without
// pulling in the SQL driver.
//
// The optional extensions the archiver asserts are roleProber (promotion
// records), historyIntegrityProber (D-34), binlogStateReader (the
// divergence audit), binlogBootstrapReader and binlogBaseReader. All
// methods are exported so tests outside the package can supply a server.
type roleChecker interface {
	IsReadOnly(ctx context.Context) (bool, error)
}

// NewBinlogArchiver builds an archiver instance. It does NOT start any
// goroutines — call Run for that. site identifies the MySQL site this
// archiver belongs to and is written into every manifest entry; the
// restore Job groups manifests by site.
//
// cfg.Archive is the archive identity the archiver writes under. The
// session it opens verifies the prefix's descriptor (creating it on an
// empty prefix when this site is writable); an unusable identity leaves the
// archiver refusing every write, with the reason in LastError.
func NewBinlogArchiver(cfg *PITRConfig, site string, mysql roleChecker, store ArchiveStore, logger *slog.Logger) *BinlogArchiver {
	a := &BinlogArchiver{
		cfg:    cfg,
		site:   site,
		mysql:  mysql,
		store:  store,
		logger: logger.With("component", "binlog-archiver", "site", site),
		now:    time.Now,
	}
	a.roles.confirmDelay = promotionConfirmDelay(cfg)
	a.session, a.identityErr = newArchiveSession(store, cfg.ManifestPrefix, cfg.Archive, site, a.logger)
	if a.identityErr != nil {
		a.identityErr = fmt.Errorf("archive identity: %w", a.identityErr)
	}
	a.loadPendingMarker()
	return a
}

// ensureSession opens the archive session when it is not verified: a
// writable site establishes an empty prefix (EnsureCreated), a read-only
// site only opens it and waits for the writer. It returns nil when the
// session is verified or (read-only) still awaiting the descriptor, and
// otherwise the refusal or store error, which blocks this cycle's writes.
func (a *BinlogArchiver) ensureSession(ctx context.Context, writable bool) error {
	if a.session == nil {
		return a.identityErr
	}
	if a.session.Verified() {
		return nil
	}
	var err error
	if writable {
		err = a.session.EnsureCreated(ctx)
	} else {
		err = a.session.Open(ctx)
	}
	if errors.Is(err, ErrDescriptorAbsent) {
		return nil
	}
	if err != nil && a.session.transientFailure() {
		return fmt.Errorf("archive store init: %w", err)
	}
	return err
}

// openSession opens an unverified session without establishing the
// archive; a verified session is left alone (saves Reverify it).
func (a *BinlogArchiver) openSession(ctx context.Context) error {
	if a.session == nil {
		return a.identityErr
	}
	if a.session.Verified() {
		return nil
	}
	return a.session.Open(ctx)
}

// loadManifest reads this site's manifest through the archive session and
// refreshes the counters and marker that /archiver/status reports.
func (a *BinlogArchiver) loadManifest(ctx context.Context) (*Manifest, error) {
	if err := a.openSession(ctx); err != nil {
		return nil, err
	}
	m, err := a.session.LoadManifest(ctx, a.site)
	if err != nil {
		return nil, err
	}
	a.observeManifest(m)
	return m, nil
}

// saveManifest is the archiver's only manifest write path. A recorded
// episode that the role timeline has since ended writes nothing (D-43).
func (a *BinlogArchiver) saveManifest(ctx context.Context, m *Manifest) error {
	if err := a.episodeCurrent(); err != nil {
		return err
	}
	if err := a.openSession(ctx); err != nil {
		return err
	}
	m.Site = a.site
	if err := a.session.SaveManifest(ctx, m); err != nil {
		return err
	}
	a.observeManifest(m)
	return nil
}

func (a *BinlogArchiver) observeManifest(m *Manifest) {
	if m.HistoryIntegrity != nil && a.persistedMarker == nil {
		marker := *m.HistoryIntegrity
		a.persistedMarker = &marker
		a.pendingMarker = nil
		a.pendingMarkerErr = nil
		a.clearPendingMarker()
	}
	a.mu.Lock()
	a.divergentRows = int64(len(m.Divergent))
	a.quarantinedRows = int64(len(m.Quarantined))
	if a.persistedMarker != nil {
		a.historyIntegrity = a.persistedMarker
	}
	a.mu.Unlock()
}

// SetRetentionClient wires the retention-sweep dependencies onto the
// archiver. When unset, the archiver still uploads but never prunes
// (useful for tests and single-profile deployments where retention
// isn't wanted).
func (a *BinlogArchiver) SetRetentionClient(bloodravenAddr, namespace, group, archiveID string, interval time.Duration) {
	if interval <= 0 {
		interval = time.Hour
	}
	a.retentionCfg = &retentionClientConfig{
		BloodravenAddress: bloodravenAddr,
		Namespace:         namespace,
		FailoverGroup:     group,
		ArchiveID:         archiveID,
		Interval:          interval,
	}
}

// Run drives the archiver. It blocks until ctx is cancelled. Errors
// from individual scan iterations are logged but never returned — the
// archiver is expected to self-heal on subsequent triggers (e.g. a
// transient S3 upload failure retries on the next inotify event or
// poll tick).
func (a *BinlogArchiver) Run(ctx context.Context) {
	defer func() {
		if a.reuseWork != nil {
			a.reuseWork.close()
			a.reuseWork = nil
		}
		if a.historyTask != nil {
			a.historyTask.close()
			a.historyTask = nil
		}
	}()
	// The role sampler runs apart from the archive I/O below, so a long
	// scan neither delays the observation a promotion record holds nor
	// hides a demotion (D-43).
	samplerCtx, stopSampler := context.WithCancel(ctx)
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		a.runRoleSampler(samplerCtx)
	}()
	defer func() {
		stopSampler()
		<-samplerDone
	}()
	a.logger.Info("binlog archiver starting",
		"storageType", a.cfg.StorageType,
		"binlogDir", a.cfg.BinlogDir,
		"binlogIndex", a.cfg.BinlogIndex,
		"pollInterval", a.cfg.PollInterval,
	)

	// Best-effort initial scan: covers the case where binlogs exist
	// from a prior run that the archiver missed while offline.
	a.scanAndArchive(ctx)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		a.logger.Error("create fsnotify watcher; falling back to poll-only", "error", err)
		a.runPollOnly(ctx)
		return
	}
	defer watcher.Close()

	// We watch the directory, not the .index file itself: MySQL
	// rewrites the index atomically (write to .index.tmp, rename), so a
	// file-level watch would lose the watch after the first rotate.
	if err := watcher.Add(a.cfg.BinlogDir); err != nil {
		a.logger.Error("add watch; falling back to poll-only", "dir", a.cfg.BinlogDir, "error", err)
		a.runPollOnly(ctx)
		return
	}

	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()
	roleTicker := time.NewTicker(roleProbeInterval(a.cfg))
	defer roleTicker.Stop()

	indexPath := filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex)

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("binlog archiver stopping")
			return

		case ev, ok := <-watcher.Events:
			if !ok {
				return
			}
			// Only trigger on events touching the index file. MySQL
			// updates it on every rotation; the actual binlog files
			// themselves are appended to continuously so watching them
			// would be noisy.
			if filepath.Clean(ev.Name) == filepath.Clean(indexPath) &&
				(ev.Has(fsnotify.Write) || ev.Has(fsnotify.Create) || ev.Has(fsnotify.Rename)) {
				a.logger.Debug("binlog index changed", "event", ev.Op.String())
				a.scanAndArchive(ctx)
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			a.logger.Warn("fsnotify error", "error", err)

		case <-a.reuseReady():
			a.reuseWork.notified = true
			a.scanAndArchive(ctx)
		case <-roleTicker.C:
			a.recordTick(ctx)
		case <-ticker.C:
			a.scanAndArchive(ctx)
		}
	}
}

// runPollOnly is the fallback path when inotify is unavailable
// (unlikely on Linux but worth handling: certain FUSE-mounted volumes
// don't support inotify). Rotation is detected with worse latency but
// never missed.
func (a *BinlogArchiver) runPollOnly(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()
	roleTicker := time.NewTicker(roleProbeInterval(a.cfg))
	defer roleTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.reuseReady():
			a.reuseWork.notified = true
			a.scanAndArchive(ctx)
		case <-roleTicker.C:
			a.recordTick(ctx)
		case <-ticker.C:
			a.scanAndArchive(ctx)
		}
	}
}

// scanAndArchive performs one archiver cycle: check role, read index,
// diff against manifest, upload deltas. Called from the inotify hot
// path AND the periodic ticker; we use a lightweight mutex only for
// observability state (lastError/lastScanAt). The archival work itself
// is serialized by the single goroutine that owns this archiver.
func (a *BinlogArchiver) scanAndArchive(ctx context.Context) {
	a.setScan(time.Now())
	// Registered first so it runs last, after maintainArchive.
	defer a.finishScan()
	a.historyScanBusy = false
	defer a.maintainArchive(ctx)
	// The tail block is published when the scan ends, before its
	// maintenance pass: every return below sets it.
	var tail tailScan
	defer func() { a.publishTail(tail) }()

	// The primary uploads every sealed binlog; a read-only site only the
	// tail of its last writable era (drainTail). Every site maintains its
	// own archive. On failover the former replica starts uploading on its
	// next scan, once its promotion record is confirmed (D-43).
	_, _, err := a.sampleRole(ctx)
	// The episode follows the timeline before anything below can return:
	// a read-only or failed observation ends it here.
	snap := a.syncEpisode()
	if err == nil && snap.state == roleUnknown {
		err = errors.New("a concurrent role probe failed")
	}
	if err != nil {
		a.incUploadFailures()
		a.setError(fmt.Sprintf("role check: %v", err))
		a.logger.Debug("role check failed; skipping scan", "error", err)
		tail = a.tailBlockedEarly(fmt.Sprintf("role check: %v", err))
		return
	}
	writable := snap.state == roleWritable
	// D-34 runs before the session, so a violation is seen and held while
	// the session awaits the descriptor, is refused or is failing.
	verifiedBefore := a.session != nil && a.session.Verified()
	integrityErr := a.checkHistoryIntegrity(ctx)
	if err := a.ensureSession(ctx, writable); err != nil {
		a.incUploadFailures()
		if integrityErr != nil {
			err = joinKeepToken(integrityErr, err)
		}
		a.setError(err.Error())
		a.setBacklog(0)
		a.logger.Debug("archive session not ready; skipping scan", "error", err)
		tail = a.tailBlockedEarly(err.Error())
		return
	}
	if verified := a.session != nil && a.session.Verified(); verified && (!verifiedBefore || a.pendingMarker != nil) {
		// The session just became verified: read the manifest's marker
		// and persist a pending one.
		integrityErr = a.checkHistoryIntegrity(ctx)
	}
	// Incarnation-reset records are written in every role, before the
	// episode's promotion record and any upload (N3 revised).
	resetErr := a.recordResets(ctx, snap)
	recordErr := a.observeRole(ctx, snap)
	if recordErr == nil {
		a.confirmEpisode(ctx, snap)
	}
	if integrityErr != nil {
		a.incUploadFailures()
		a.setError(integrityErr.Error())
		a.setBacklog(0)
		tail = a.tailBlockedEarly(integrityErr.Error())
		return
	}
	if !writable {
		// A standby uploads only its writable-era tail, and nothing when
		// it has none: that clears stale errors, so a former primary
		// that failed over cleanly stops reporting noise. A reset record
		// that cannot be written is reported (a standby awaiting the
		// writer's descriptor cannot run the check yet).
		if resetErr != nil && !errors.Is(resetErr, errStartCheckPending) {
			a.setBacklog(0)
			a.incUploadFailures()
			a.setError(resetErr.Error())
			tail = a.tailBlockedEarly(resetErr.Error())
			return
		}
		tail = a.drainTail(ctx)
		return
	}
	switch {
	case resetErr != nil && recordErr != nil && !errors.Is(recordErr, errStartCheckPending):
		recordErr = joinKeepToken(resetErr, recordErr)
	case resetErr != nil:
		recordErr = resetErr
	}
	if recordErr != nil {
		a.incUploadFailures()
		a.setError(recordErr.Error())
		a.logger.Warn("uploads wait for the promotion record", "error", recordErr)
		tail = a.tailBlockedEarly(recordErr.Error())
		return
	}

	indexPath := filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex)
	entries, err := readBinlogIndex(indexPath)
	if err != nil {
		a.incUploadFailures()
		a.setError(fmt.Sprintf("read index: %v", err))
		a.logger.Warn("read binlog index", "path", indexPath, "error", err)
		tail = tailScan{state: internalmysql.ArchiverTailBlocked, reason: fmt.Sprintf("read index: %v", err)}
		return
	}
	if len(entries) < 2 {
		// < 2 means either no binlogs yet (len 0) or only the active
		// one exists (len 1). Nothing sealed to archive.
		a.uploadFailure = nil
		a.clearScanError()
		a.setBacklog(0)
		tail = tailDrained(entries)
		return
	}
	// Drop the last entry: it is the active binlog (tail of the index
	// file is what MySQL is currently writing to).
	sealed := entries[:len(entries)-1]

	manifest, err := a.loadManifest(ctx)
	if err != nil {
		a.incUploadFailures()
		a.setError(wrapKeepToken("load manifest", err).Error())
		a.logger.Warn("load manifest", "error", err)
		tail = tailBlocked(entries, 0, a.noteUploadFailure())
		return
	}
	retired := a.retiredBinlogIdentities(manifest)
	archivedThisCycle, backlog, hadFileError := a.scanFiles(ctx, manifest, sealed, retired)
	a.setBacklog(backlog)

	if archivedThisCycle > 0 {
		now := time.Now().UTC()
		a.mu.Lock()
		a.filesArchiv += int64(archivedThisCycle)
		a.lastUploadAt = now
		a.mu.Unlock()
		a.logger.Info("archived sealed binlogs", "count", archivedThisCycle)
	}
	// Preserve the most recent per-file error when at least one archive
	// failed this cycle — otherwise /archiver/status would look green
	// after a partial failure and the operator's status.pitr Message
	// would flap clear on every scan.
	if !hadFileError {
		a.uploadFailure = nil
		a.clearScanError()
	}
	// A writable site has to archive every sealed file.
	switch {
	case hadFileError:
		tail = tailBlocked(entries, backlog, a.noteUploadFailure())
	case backlog > 0:
		tail = tailPending(entries, backlog)
	default:
		tail = tailDrained(entries)
	}
}

// maybeRunRetention resumes durable pending deletes, then queries /pitr-cutoff and,
// if a cutoff is returned, prunes manifest entries (and their remote
// objects) whose transactions are covered by every retained backup and
// whose LastEventTime is strictly before the bounded cutoff. Errors
// are logged but not surfaced via setError so a transient 503 from the
// operator doesn't leak into the archiver status.
func (a *BinlogArchiver) maybeRunRetention(ctx context.Context) {
	if a.retentionCfg == nil {
		return
	}
	a.mu.Lock()
	last := a.lastRetentionRun
	a.mu.Unlock()
	if !last.IsZero() && time.Since(last) < a.retentionCfg.Interval {
		return
	}

	a.mu.Lock()
	a.lastRetentionRun = time.Now()
	a.mu.Unlock()
	deleted, pruned := 0, 0
	cutoffText := ""
	defer func() {
		if deleted > 0 {
			a.logger.Info("retention sweep complete", "cutoff", cutoffText, "deleted", deleted, "manifest_pruned", pruned)
		}
	}()
	manifest, err := a.loadManifest(ctx)
	if err != nil {
		a.logger.Warn("retention: load manifest", "error", err)
		return
	}
	count, err := a.retryPrunedDeletes(ctx, manifest)
	deleted += count
	if err != nil {
		a.logger.Warn("retention: save delete acknowledgements", "error", err)
		return
	}
	if manifest.Epoch != nil && manifest.Epoch.ResetReason != "" {
		return
	}
	proof, err := a.fetchRetentionCutoff(ctx)
	if err != nil {
		a.logger.Warn("fetch retention cutoff", "error", err)
		if errors.Is(err, errRetentionRateLimited) {
			a.retryRetentionSoon()
		}
		return
	}
	if proof.cutoff.IsZero() {
		if proof.reason == "noRetainedBackups" || proof.reason == "backupInFlight" {
			a.logger.Info("retention deferred", "reason", proof.reason)
		} else {
			a.logger.Warn("retention deferred", "reason", proof.reason)
		}
		return
	}
	cutoff, ok := boundRetentionCutoff(proof.cutoff, time.Now())
	if !ok {
		a.logger.Warn("retention: ignoring cutoff in the future", "cutoff", cutoff.Format(time.RFC3339))
		return
	}

	cutoffText = cutoff.Format(time.RFC3339)
	backfill, err := a.backfillRetentionCoverage(ctx, manifest, cutoff)
	if err != nil {
		if backfill.progress {
			a.retryRetentionSoon()
		}
		a.logger.Warn("retention: save coverage", "error", err)
		return
	}
	unknownCoverage := false
	for _, f := range manifest.Files {
		if !f.LastEventTime.IsZero() && f.LastEventTime.Before(cutoff) && (!coverageBoundToEntry(f) || !f.GTIDCoverage.Complete) {
			unknownCoverage = true
		}
	}
	if backfill.pending {
		a.retryRetentionSoon()
	}
	if unknownCoverage {
		if backfill.pending {
			a.logger.Info("retention deferred", "reason", "binlogGTIDCoverageUnknown")
		} else {
			a.logger.Warn("retention deferred", "reason", "binlogGTIDCoverageUnknown")
		}
	}
	if manifest.Epoch != nil && manifest.Epoch.ResetReason != "" {
		return
	}
	if a.archiveReusePending(manifest) {
		if err := a.archiveReuseError(); err != nil {
			a.logger.Warn("retention deferred", "reason", "archiveIssuanceUnverified", "error", err)
		} else {
			a.logger.Info("retention deferred", "reason", "archiveIssuanceUnverified")
		}
		return
	}
	drop := map[binlogIdentity]string{}
	for _, f := range manifest.Files {
		if !f.LastEventTime.IsZero() && f.LastEventTime.Before(cutoff) &&
			coverageBoundToEntry(f) && fileCoveredByBackups(f.GTIDCoverage, proof.backups) {
			drop[f.identity()] = f.GTIDCoverage.ContentID
		}
	}
	if len(drop) == 0 {
		return
	}
	removed, err := pruneManifest(ctx, a.session, a.site, drop)
	if err != nil {
		a.logger.Warn("retention: prune manifest", "error", err)
		return
	}
	pruned = len(removed)
	manifest, err = a.loadManifest(ctx)
	if err != nil {
		a.logger.Warn("retention: load manifest", "error", err)
		return
	}
	count, err = a.retryPrunedDeletes(ctx, manifest)
	deleted += count
	if err != nil {
		a.logger.Warn("retention: save delete acknowledgements", "error", err)
	}
}

// minRetainedBinlogAge is the newest data a retention sweep may delete:
// binlogs whose last event is within this window are always kept. The
// cutoff comes over the operator's unauthenticated aux endpoint, so this
// bounds what a spoofed reply can destroy. A legitimate cutoff is the
// oldest retained backup's start time; GTID containment is also required.
const minRetainedBinlogAge = 24 * time.Hour

var errRetentionRateLimited = errors.New("PITR cutoff lookup rate limited")

// Retry bounded backfill and throttled lookups without waiting another hour.
func (a *BinlogArchiver) retryRetentionSoon() {
	delay := min(30*time.Second, a.retentionCfg.Interval)
	a.mu.Lock()
	a.lastRetentionRun = time.Now().Add(-a.retentionCfg.Interval + delay)
	a.mu.Unlock()
}

// boundRetentionCutoff clamps cutoff so a sweep never deletes binlogs
// newer than minRetainedBinlogAge. It returns false for a cutoff in the
// future, which no retained backup can produce.
func boundRetentionCutoff(cutoff, now time.Time) (time.Time, bool) {
	if cutoff.After(now) {
		return cutoff, false
	}
	if floor := now.Add(-minRetainedBinlogAge); cutoff.After(floor) {
		return floor, true
	}
	return cutoff, true
}

type pitrCutoffResponse struct {
	CutoffTime              string   `json:"cutoffTime,omitempty"`
	Coverage                string   `json:"coverage,omitempty"`
	RetainedGTIDs           []string `json:"retainedGTIDs,omitempty"`
	RetentionDeferredReason string   `json:"retentionDeferredReason,omitempty"`
}

type retentionCoverageProof struct {
	cutoff  time.Time
	backups []internalmysql.GTIDSet
	reason  string
}

func fileCoveredByBackups(coverage *BinlogGTIDCoverage, backups []internalmysql.GTIDSet) bool {
	if coverage == nil || !coverage.Complete || len(backups) == 0 {
		return false
	}
	transactions, err := internalmysql.ParseGTIDSet(coverage.GTIDs)
	if err != nil {
		return false
	}
	for _, backup := range backups {
		if backup.IsEmpty() || !backup.Contains(transactions) {
			return false
		}
	}
	return true
}

// retentionCutoffURL is the operator's /pitr-cutoff query for this pod's
// group and archive.
func (a *BinlogArchiver) retentionCutoffURL() string {
	q := url.Values{}
	q.Set("namespace", a.retentionCfg.Namespace)
	q.Set("group", a.retentionCfg.FailoverGroup)
	q.Set("archive", a.retentionCfg.ArchiveID)
	q.Set("coverage", PITRGTIDCoverageProtocol)
	u := url.URL{Scheme: "http", Host: a.retentionCfg.BloodravenAddress, Path: "/pitr-cutoff", RawQuery: q.Encode()}
	return u.String()
}

// fetchRetentionCutoff calls the operator's aux HTTP endpoint. Returns
// an empty proof when the peer lacks the coverage protocol or backup bounds.
// Transport and malformed-metadata errors defer the entire sweep.
func (a *BinlogArchiver) fetchRetentionCutoff(ctx context.Context) (retentionCoverageProof, error) {
	if a.retentionCfg.BloodravenAddress == "" {
		return retentionCoverageProof{}, fmt.Errorf("no bloodraven address configured")
	}
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, a.retentionCutoffURL(), nil)
	if err != nil {
		return retentionCoverageProof{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return retentionCoverageProof{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return retentionCoverageProof{}, errRetentionRateLimited
	}
	if resp.StatusCode != http.StatusOK {
		return retentionCoverageProof{}, fmt.Errorf("operator status %d", resp.StatusCode)
	}
	var body pitrCutoffResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return retentionCoverageProof{}, err
	}
	if body.Coverage != PITRGTIDCoverageProtocol || len(body.RetainedGTIDs) == 0 {
		reason := body.RetentionDeferredReason
		switch reason {
		case "noRetainedBackups", "backupInFlight", "backupMetadataUnknown":
		default:
			reason = "backupGTIDCoverageUnknown"
		}
		return retentionCoverageProof{reason: reason}, nil
	}
	if body.RetentionDeferredReason != "" {
		return retentionCoverageProof{reason: "backupMetadataUnknown"}, nil
	}
	var backups []internalmysql.GTIDSet
	for _, raw := range body.RetainedGTIDs {
		set, err := internalmysql.ParseGTIDSet(raw)
		if err != nil || set.IsEmpty() {
			return retentionCoverageProof{}, fmt.Errorf("retention backup GTID coverage unknown")
		}
		backups = append(backups, set)
	}
	if body.CutoffTime == "" {
		return retentionCoverageProof{reason: "backupGTIDCoverageUnknown"}, nil
	}
	t, err := time.Parse(time.RFC3339, body.CutoffTime)
	if err != nil {
		return retentionCoverageProof{}, err
	}
	return retentionCoverageProof{cutoff: t, backups: backups}, nil
}

// Status is a snapshot of the archiver's state for the sidecar HTTP
// surface. Exposed via /archiver/status so kubectl describe / operator
// polling / humans can tell at a glance whether archival is running.
type Status struct {
	Enabled bool `json:"enabled"`
	// StoreReady is false while archival is configured but cannot reach
	// a verified archive session: the store cannot be constructed, the
	// configured archive identity is unusable, or establishing or opening
	// the archive (its descriptor) is failing on a transient store error.
	// LastError says why. A refusal of a reachable archive (foreign, copy,
	// legacy, ...) keeps it true and reports the refusal in Archive.
	// Absent when PITR is not configured, and from sidecars that predate
	// the field, which exit instead of serving while the store is down;
	// readers must treat absent as "not reported", not as false.
	StoreReady     *bool     `json:"storeReady,omitempty"`
	Primary        bool      `json:"primary"`
	LastScanAt     time.Time `json:"lastScanAt"`
	FilesArchived  int64     `json:"filesArchived"`
	LastError      string    `json:"lastError,omitempty"`
	StorageType    string    `json:"storageType"`
	ManifestPrefix string    `json:"manifestPrefix"`
	Site           string    `json:"site"`
	// UploadFailures is the cumulative count of archive-attempt errors
	// since sidecar start (archive store construction, role check, index
	// read, manifest I/O, or per-file upload). Monotonic except across
	// restarts.
	UploadFailures int64 `json:"uploadFailures"`
	// ConsecutiveFailedScans is how many archiver scans in a row ended
	// with LastError set; 0 after a clean scan. LastError survives until
	// the next scan, so the operator debounces on this rather than on
	// how many of its polls saw the error. Always present, so absent
	// means a sidecar that predates the field.
	ConsecutiveFailedScans int64 `json:"consecutiveFailedScans"`
	// LastUploadAt is the time of the most recent successful archive.
	// Omitted until the archiver has appended at least one manifest
	// entry in this process lifetime.
	LastUploadAt time.Time `json:"lastUploadAt,omitempty"`
	// BacklogFiles is the number of sealed binlogs present in the
	// index but still missing from the manifest at the end of the
	// last scan. 0 on the happy path.
	BacklogFiles int64 `json:"backlogFiles"`
	// ManifestFileCount / ManifestBytes / OldestArchivedTime /
	// NewestArchivedTime are aggregates computed from the site's
	// manifest. The operator mirrors these into status.pitr so users
	// can see PITR coverage from kubectl describe without needing
	// storage access. Zero values mean "no archived files yet".
	ManifestFileCount  int64     `json:"manifestFileCount"`
	ManifestBytes      int64     `json:"manifestBytes"`
	OldestArchivedTime time.Time `json:"oldestArchivedTime,omitempty"`
	NewestArchivedTime time.Time `json:"newestArchivedTime,omitempty"`
	// Archive is the archive identity this sidecar observes (§3.7): its
	// destination, verified descriptor ID, encryption mode, state, refusal
	// reason and key revision. nil when PITR is not configured, and when
	// the configured identity is unusable (StoreReady=false, LastError
	// names the identity error).
	Archive *pitrarchive.ArchiveStatus `json:"archive,omitempty"`
	// DivergentRows and QuarantinedRows count the Divergent and
	// Quarantined entries in this site's own manifest. A divergent row is
	// evidence, not a failure: the row holds (or builds on) transactions
	// this site's server no longer has, and a restore across them refuses
	// PITRArchiveDivergentHistory until excludeGtids lists them.
	DivergentRows   int64 `json:"divergentRows"`
	QuarantinedRows int64 `json:"quarantinedRows"`
	// ReissuableGTIDs is the part of those transactions (of the active and
	// the quarantined rows) that this server could issue again: the ones
	// under its own current server_uuid. Non-empty is an archiver error
	// (LastError), and the runner warns PITRArchiveReissueRiskDetected when
	// such a site is promoted. The empty string means the last history
	// audit found none. nil means the last maintenance pass did not
	// evaluate it: no audit has run in this process, the audit could not
	// read the manifest or the server's state, the manifest carries a
	// recorded GTID reuse (the audit stops there), or the server has no
	// history yet (empty gtid_executed, a clone in progress, a read-only
	// server with no replication channel). A verdict never outlives the
	// pass that made it.
	ReissuableGTIDs *string `json:"reissuableGtids,omitempty"`
	// PromotionRecordedAt is the ObservedAt of the promotion record that
	// gates the current writable episode's uploads; zero while no record
	// is confirmed (D-43).
	PromotionRecordedAt time.Time `json:"promotionRecordedAt,omitzero"`
	// HistoryIntegrity is the site's history-integrity marker (D-34):
	// the one persisted in its manifest, else a violation seen but not
	// yet persisted (no verified session yet); nil when unmarked.
	HistoryIntegrity *pitrarchive.HistoryIntegrity `json:"historyIntegrity,omitempty"`
	// Tail is what the last scan decided about the sealed binlogs this
	// site still has to archive: every sealed file while writable, the
	// writable-era tail while read-only (binlog_tail.go). State and the
	// file names come from one read of the binlog index, so the state is
	// about the index in which ActiveFile was the active file, not about
	// rotations after it (publishTail). nil until a scan has read the
	// binlog index.
	Tail *internalmysql.ArchiverTailStatus `json:"tail,omitempty"`
}

// Snapshot returns a copy of the archiver's observable state. The
// role query is done OUTSIDE the mutex: IsReadOnly runs a SQL query
// that can block for up to the handler's context timeout, and holding
// a.mu across that call would stall scanAndArchive's own lastScanAt /
// lastError updates under load.
func (a *BinlogArchiver) Snapshot(ctx context.Context) Status {
	primary := false
	if ro, err := a.mysql.IsReadOnly(ctx); err == nil {
		primary = !ro
	}
	var archive *pitrarchive.ArchiveStatus
	var storeReady *bool
	if a.session != nil {
		st := a.session.Status()
		archive = &st
		// An identity that cannot create the descriptor is unusable like
		// one rejected at construction (StoreReady=false, F194).
		if st.State != pitrarchive.StateVerified && (a.session.transientFailure() || errors.Is(a.session.Err(), errArchiveIdentityConfig)) {
			storeReady = new(bool)
		}
	} else {
		storeReady = new(bool)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var marker *HistoryIntegrity
	if a.historyIntegrity != nil {
		copied := *a.historyIntegrity
		marker = &copied
	}
	var tail *internalmysql.ArchiverTailStatus
	if a.tailStatus != nil {
		copied := *a.tailStatus
		tail = &copied
	}
	var reissuable *string
	if a.reissuableGTIDs != nil {
		copied := *a.reissuableGTIDs
		reissuable = &copied
	}
	return Status{
		Tail:                   tail,
		StoreReady:             storeReady,
		Archive:                archive,
		DivergentRows:          a.divergentRows,
		QuarantinedRows:        a.quarantinedRows,
		ReissuableGTIDs:        reissuable,
		PromotionRecordedAt:    a.promotionRecordedAt,
		HistoryIntegrity:       marker,
		Enabled:                true,
		Primary:                primary,
		LastScanAt:             a.lastScanAt,
		FilesArchived:          a.filesArchiv,
		LastError:              a.lastError,
		StorageType:            a.cfg.StorageType,
		ManifestPrefix:         a.cfg.ManifestPrefix,
		Site:                   a.site,
		UploadFailures:         a.uploadFailures,
		ConsecutiveFailedScans: a.failedScans,
		LastUploadAt:           a.lastUploadAt,
		BacklogFiles:           a.backlogFiles,
		ManifestFileCount:      a.manifestFileCount,
		ManifestBytes:          a.manifestBytes,
		OldestArchivedTime:     a.oldestArchivedTime,
		NewestArchivedTime:     a.newestArchivedTime,
	}
}

func (a *BinlogArchiver) setScan(t time.Time) {
	a.mu.Lock()
	a.lastScanAt = t
	a.mu.Unlock()
}

// finishScan counts the cycle that just ended as failed or clean.
func (a *BinlogArchiver) finishScan() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastError != "" {
		a.failedScans++
	} else {
		a.failedScans = 0
	}
}

func (a *BinlogArchiver) setError(s string) {
	a.mu.Lock()
	a.lastError = s
	a.mu.Unlock()
}

// clearScanError ends the errors of the scan itself. A standing
// history-audit error stays: the maintenance pass that follows this scan
// confirms or clears it, and a status read in between must not show a
// clean archiver for one that is not (the operator would count that poll
// as a recovery).
func (a *BinlogArchiver) clearScanError() {
	if a.historyAuditError != "" {
		a.setMaintenanceError(a.historyAuditError)
		return
	}
	a.setError("")
}

func (a *BinlogArchiver) incUploadFailures() {
	a.mu.Lock()
	a.uploadFailures++
	a.mu.Unlock()
}

func (a *BinlogArchiver) setBacklog(n int64) {
	if n < 0 {
		n = 0
	}
	a.mu.Lock()
	a.backlogFiles = n
	a.mu.Unlock()
}

// updateManifestAggregates recomputes file count, total bytes, and the
// oldest/newest event-time bounds from the current manifest. Called at
// the end of each scan so the values surfaced via Snapshot track what's
// actually in storage.
func (a *BinlogArchiver) updateManifestAggregates(files []ManifestEntry) {
	var (
		count  int64
		bytes  int64
		oldest time.Time
		newest time.Time
	)
	for _, f := range files {
		count++
		bytes += f.Size
		if !f.FirstEventTime.IsZero() && (oldest.IsZero() || f.FirstEventTime.Before(oldest)) {
			oldest = f.FirstEventTime
		}
		if !f.LastEventTime.IsZero() && f.LastEventTime.After(newest) {
			newest = f.LastEventTime
		}
	}
	a.mu.Lock()
	a.manifestFileCount = count
	a.manifestBytes = bytes
	a.oldestArchivedTime = oldest
	a.newestArchivedTime = newest
	a.mu.Unlock()
}

// readBinlogIndex reads and parses the MySQL binary log index file.
// The file contains one basename per line (the name of each binlog
// file MySQL currently knows about, in rotation order). Blank lines
// and comments are ignored defensively; MySQL itself never writes
// them but this guards against hand-edits.
func readBinlogIndex(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// No binlog index yet — MySQL hasn't started / rotated.
			return nil, nil
		}
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	var names []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		names = append(names, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	// Sort by name as a stability safety net. MySQL's index file is
	// already in rotation order, but we don't rely on that — the
	// caller only needs "last entry is active, rest are sealed" and
	// basenames are zero-padded decimals so lexical and numeric order
	// coincide (mysql-bin.000001 < mysql-bin.000010 < ...).
	sort.Strings(names)
	return names, nil
}

// wrapKeepToken adds context to err while keeping a leading PITR refusal
// token first, so a LastError still starts with the token the controller
// reports (pitrarchive.ParseToken). errors.Is/As still see err.
func wrapKeepToken(context string, err error) error {
	msg := err.Error()
	tok, ok := pitrarchive.ParseToken(msg)
	if !ok {
		return fmt.Errorf("%s: %w", context, err)
	}
	rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(msg), string(tok)))
	rest = strings.TrimSpace(strings.TrimPrefix(rest, ":"))
	if rest == "" {
		return &tokenContextError{msg: fmt.Sprintf("%s: %s", tok, context), err: err}
	}
	return &tokenContextError{msg: fmt.Sprintf("%s: %s: %s", tok, context, rest), err: err}
}

// joinKeepToken joins two errors into one message, the one that starts
// with a PITR refusal token first (first when both or neither do).
func joinKeepToken(first, second error) error {
	if _, ok := pitrarchive.ParseToken(first.Error()); !ok {
		if _, ok := pitrarchive.ParseToken(second.Error()); ok {
			first, second = second, first
		}
	}
	return &tokenContextError{msg: first.Error() + "; " + second.Error(), err: errors.Join(first, second)}
}

type tokenContextError struct {
	msg string
	err error
}

func (e *tokenContextError) Error() string { return e.msg }
func (e *tokenContextError) Unwrap() error { return e.err }
