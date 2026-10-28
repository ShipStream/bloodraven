package sidecar

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// BinlogArchiver uploads sealed binlog files from the local MySQL data
// directory to remote storage, and keeps a per-site manifest describing
// what was archived. It runs inside the sidecar on every MySQL pod but
// is "active" (actually uploading) only when the local MySQL is the
// primary. That keeps archive ownership unambiguous across failovers
// without coordinating between sites: whichever side owns the write
// role also owns its binlog archival.
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
	retentionInspectionFailures map[retentionInspectionIdentity]time.Time
	historyTask                 *binlogHistoryTask
	historyNext                 int
	historyNextAudit            int
	historyAuditError           string
	historyScanBusy             bool
	reuseWork                   *archiveReuseWork
	reuseNext                   int
	reuseFailed                 map[archiveReuseKey]time.Time
	reuseErrors                 map[archiveReuseKey]error
	reuseCompared               map[archiveReuseKey]bool
	sealedSnapshot              map[string]os.FileInfo
	legacyChecked               map[string]time.Time
	localIDs                    map[string]cachedBinlogIdentity
	cfg                         *PITRConfig
	// retentionCfg holds the fields the archiver needs to call the
	// operator's /pitr-cutoff endpoint for retention decisions. Nil
	// when those fields weren't passed via env (tests, or a
	// standalone dev sidecar running without an operator).
	retentionCfg *retentionClientConfig
	site         string
	mysql        roleChecker
	store        ArchiveStore
	logger       *slog.Logger

	// mu guards lastError, lastScanAt, filesArchiv, lastRetentionRun,
	// uploadFailures, lastUploadAt, backlogFiles, and the manifest
	// aggregates (manifestFileCount/Bytes/oldest/newest).
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
	// backlogFiles is the count of sealed index entries that were
	// missing from the manifest at the end of the most recent scan.
	// Reset every cycle. >0 means the archiver hasn't caught up yet.
	backlogFiles int64
	// manifestFileCount/Bytes and oldest/newestArchivedTime are
	// aggregates computed from the manifest after each scan.
	// They power the operator's status.pitr fields.
	manifestFileCount  int64
	manifestBytes      int64
	oldestArchivedTime time.Time
	newestArchivedTime time.Time
}

// retentionClientConfig is what the archiver needs to query the
// operator for the current retention cutoff. All of it comes from the
// sidecar env; set by the caller via SetRetentionClient.
type retentionClientConfig struct {
	BloodravenAddress string
	Namespace         string
	FailoverGroup     string
	ProfileName       string
	// Interval between retention sweeps. Defaults to 1h to avoid
	// putting pressure on the operator aux endpoint + repeatedly
	// listing S3.
	Interval time.Duration
}

// roleChecker is the narrow subset of mysqlQuerier that the archiver
// needs. Using a minimal interface lets tests substitute a stub without
// pulling in the SQL driver.
type roleChecker interface {
	IsReadOnly(ctx context.Context) (bool, error)
	isConnectable(ctx context.Context) bool
}

// NewBinlogArchiver builds an archiver instance. It does NOT start any
// goroutines — call Run for that. site identifies the MySQL site this
// archiver belongs to and is written into every manifest entry; the
// restore Job groups manifests by site.
func NewBinlogArchiver(cfg *PITRConfig, site string, mysql roleChecker, store ArchiveStore, logger *slog.Logger) *BinlogArchiver {
	return &BinlogArchiver{
		cfg:    cfg,
		site:   site,
		mysql:  mysql,
		store:  store,
		logger: logger.With("component", "binlog-archiver", "site", site),
	}
}

// SetRetentionClient wires the retention-sweep dependencies onto the
// archiver. When unset, the archiver still uploads but never prunes
// (useful for tests and single-profile deployments where retention
// isn't wanted).
func (a *BinlogArchiver) SetRetentionClient(bloodravenAddr, namespace, group, profile string, interval time.Duration) {
	if interval <= 0 {
		interval = time.Hour
	}
	a.retentionCfg = &retentionClientConfig{
		BloodravenAddress: bloodravenAddr,
		Namespace:         namespace,
		FailoverGroup:     group,
		ProfileName:       profile,
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.reuseReady():
			a.reuseWork.notified = true
			a.scanAndArchive(ctx)
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
	a.historyScanBusy = false
	defer a.maintainArchive(ctx)

	// Only the primary uploads; every site maintains its own archive.
	// On failover the former replica starts uploading on its next scan.
	// A brief re-check race after failover is fine — at worst the new
	// primary's first archive cycle fires a couple of seconds later.
	ro, err := a.mysql.IsReadOnly(ctx)
	if err != nil {
		a.incUploadFailures()
		a.setError(fmt.Sprintf("role check: %v", err))
		a.logger.Debug("role check failed; skipping scan", "error", err)
		return
	}
	if ro {
		// No uploads on a standby. Clear stale errors so a former
		// primary that failed over cleanly stops reporting noise.
		a.setError("")
		a.setBacklog(0)
		return
	}

	indexPath := filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex)
	entries, err := readBinlogIndex(indexPath)
	if err != nil {
		a.incUploadFailures()
		a.setError(fmt.Sprintf("read index: %v", err))
		a.logger.Warn("read binlog index", "path", indexPath, "error", err)
		return
	}
	if len(entries) < 2 {
		// < 2 means either no binlogs yet (len 0) or only the active
		// one exists (len 1). Nothing sealed to archive.
		a.setError("")
		a.setBacklog(0)
		return
	}
	// Drop the last entry: it is the active binlog (tail of the index
	// file is what MySQL is currently writing to).
	sealed := entries[:len(entries)-1]

	manifest, err := loadManifest(ctx, a.store, a.cfg.ManifestPrefix, a.site)
	if err != nil {
		a.incUploadFailures()
		a.setError(fmt.Sprintf("load manifest: %v", err))
		a.logger.Warn("load manifest", "error", err)
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
		a.setError("")
	}
	a.maybeRunRetention(ctx)
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
	manifest, err := loadManifest(ctx, a.store, a.cfg.ManifestPrefix, a.site)
	if err != nil {
		a.logger.Warn("retention: load manifest", "error", err)
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
	removed, err := pruneManifest(ctx, a.store, a.cfg.ManifestPrefix, a.site, drop)
	if err != nil {
		a.logger.Warn("retention: prune manifest", "error", err)
		return
	}
	pruned = len(removed)
	for _, key := range removed {
		if err := a.store.Delete(ctx, key); err != nil {
			a.logger.Warn("retention: delete object", "key", key, "error", err)
			continue
		}
		deleted++
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

// fetchRetentionCutoff calls the operator's aux HTTP endpoint. Returns
// an empty proof when the peer lacks the coverage protocol or backup bounds.
// Transport and malformed-metadata errors defer the entire sweep.
func (a *BinlogArchiver) fetchRetentionCutoff(ctx context.Context) (retentionCoverageProof, error) {
	if a.retentionCfg.BloodravenAddress == "" {
		return retentionCoverageProof{}, fmt.Errorf("no bloodraven address configured")
	}
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	url := fmt.Sprintf("http://%s/pitr-cutoff?namespace=%s&group=%s&profile=%s&coverage=%s",
		a.retentionCfg.BloodravenAddress,
		a.retentionCfg.Namespace,
		a.retentionCfg.FailoverGroup,
		a.retentionCfg.ProfileName, PITRGTIDCoverageProtocol)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
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
	Enabled        bool      `json:"enabled"`
	Primary        bool      `json:"primary"`
	LastScanAt     time.Time `json:"lastScanAt"`
	FilesArchived  int64     `json:"filesArchived"`
	LastError      string    `json:"lastError,omitempty"`
	StorageType    string    `json:"storageType"`
	ManifestPrefix string    `json:"manifestPrefix"`
	Site           string    `json:"site"`
	// UploadFailures is the cumulative count of archive-attempt errors
	// since sidecar start (role check, index read, manifest I/O, or
	// per-file upload). Monotonic except across restarts.
	UploadFailures int64 `json:"uploadFailures"`
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
	a.mu.Lock()
	defer a.mu.Unlock()
	return Status{
		Enabled:            true,
		Primary:            primary,
		LastScanAt:         a.lastScanAt,
		FilesArchived:      a.filesArchiv,
		LastError:          a.lastError,
		StorageType:        a.cfg.StorageType,
		ManifestPrefix:     a.cfg.ManifestPrefix,
		Site:               a.site,
		UploadFailures:     a.uploadFailures,
		LastUploadAt:       a.lastUploadAt,
		BacklogFiles:       a.backlogFiles,
		ManifestFileCount:  a.manifestFileCount,
		ManifestBytes:      a.manifestBytes,
		OldestArchivedTime: a.oldestArchivedTime,
		NewestArchivedTime: a.newestArchivedTime,
	}
}

func (a *BinlogArchiver) setScan(t time.Time) {
	a.mu.Lock()
	a.lastScanAt = t
	a.mu.Unlock()
}

func (a *BinlogArchiver) setError(s string) {
	a.mu.Lock()
	a.lastError = s
	a.mu.Unlock()
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
