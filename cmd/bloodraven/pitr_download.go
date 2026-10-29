package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

// runPITRDownload is the entry point for `bloodraven pitr-download`.
// It runs as an init container on the restore Job, downloads every
// archived binlog that could intersect the PITR replay window, and
// writes them into a shared emptyDir for the mysqlsh container to
// replay afterward.
//
// Moving the download out of the Python restore script accomplishes
// three things:
//
//   - We don't depend on the `aws` CLI existing in the Oracle MySQL
//     community-server image (it doesn't, reliably). The init
//     container ships the bloodraven image we already require for
//     scheduled backups, and uses the same AWS SDK v2 code path as
//     the sidecar archiver.
//   - Pagination and retries are handled correctly by the SDK's
//     paginators — the old shell-out version would silently truncate
//     archives larger than one page.
//   - The storage-backend abstraction (sidecar.ArchiveStore) is
//     reused directly, so S3 and PVC paths stay in lockstep and
//     additional backends plug in one place.
//
// All configuration is via env vars, mirroring the rest of the
// bloodraven Job surface. The relevant set is documented inline; see
// internal/controller/restore.go for where each gets set.
func runPITRDownload(args []string) {
	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo)

	stopDT := os.Getenv("BLOODRAVEN_PITR_STOP_DATETIME")
	if stopDT == "" {
		logger.Error("missing required env var", "error", "BLOODRAVEN_PITR_STOP_DATETIME is required")
		os.Exit(2)
	}
	outDir := os.Getenv("BLOODRAVEN_PITR_OUTPUT_DIR")
	if outDir == "" {
		logger.Error("missing required env var", "error", "BLOODRAVEN_PITR_OUTPUT_DIR is required")
		os.Exit(2)
	}
	prefix := os.Getenv("BLOODRAVEN_PITR_MANIFEST_PREFIX")
	if prefix == "" {
		logger.Error("missing required env var", "error", "BLOODRAVEN_PITR_MANIFEST_PREFIX is required")
		os.Exit(2)
	}

	var sites []string
	if v := os.Getenv("BLOODRAVEN_PITR_SITES"); v != "" {
		for _, s := range strings.Split(v, ",") {
			if s = strings.TrimSpace(s); s != "" {
				sites = append(sites, s)
			}
		}
	}

	cfg, err := pitrDownloadConfigFromEnv()
	if err != nil {
		logger.Error("invalid archive config", "error", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		logger.Info("received signal, cancelling")
		cancel()
	}()

	store, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		logger.Error("init archive store", "error", err)
		os.Exit(2)
	}

	stopTime, err := backupmeta.ParsePITRStopTime(stopDT)
	if err != nil {
		logger.Error("parse stop-datetime", "value", stopDT, "error", err)
		os.Exit(2)
	}

	result, err := downloadPITRArchive(ctx, store, prefix, sites, stopTime, outDir, os.Getenv("BLOODRAVEN_PITR_START_GTIDS"), os.Getenv("BLOODRAVEN_PITR_ALLOW_PARTIAL") == "true", os.Getenv("BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE") == "true", os.Getenv("BLOODRAVEN_PITR_MODE"), logger)
	if err != nil {
		// Kubernetes retains this reason even when the main container never starts.
		_ = os.WriteFile("/dev/termination-log", []byte(err.Error()), 0644)
		logger.Error("download site binlogs", "error", err)
		os.Exit(2)
	}
	totalDownloaded := 0
	for _, site := range result.Sites {
		totalDownloaded += len(site.Staged)
	}
	logger.Info("pitr-download complete", "files", totalDownloaded, "sites", len(sites))
}

// pitrDownloadConfigFromEnv builds a sidecar.PITRConfig from the same
// env var surface the sidecar archiver uses. We reuse the config
// struct (and its NewArchiveStore constructor) so one set of rules
// governs how S3/PVC credentials + paths are resolved everywhere.
func pitrDownloadConfigFromEnv() (*sidecar.PITRConfig, error) {
	storageType := os.Getenv("BLOODRAVEN_PITR_STORAGE_TYPE")
	prefix := os.Getenv("BLOODRAVEN_PITR_MANIFEST_PREFIX")

	cfg := &sidecar.PITRConfig{
		StorageType:    storageType,
		ManifestPrefix: prefix,
	}
	switch storageType {
	case "S3":
		bucket := os.Getenv("BLOODRAVEN_PITR_S3_BUCKET")
		if bucket == "" {
			return nil, fmt.Errorf("BLOODRAVEN_PITR_S3_BUCKET is required for S3")
		}
		cfg.S3 = &sidecar.PITRS3Config{
			Bucket:      bucket,
			Region:      os.Getenv("BLOODRAVEN_PITR_S3_REGION"),
			EndpointURL: os.Getenv("BLOODRAVEN_PITR_S3_ENDPOINT_URL"),
			AWSCredsDir: os.Getenv("BLOODRAVEN_PITR_AWS_CREDS_DIR"),
		}
	case "PVC":
		mount := os.Getenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH")
		if mount == "" {
			return nil, fmt.Errorf("BLOODRAVEN_PITR_PVC_MOUNT_PATH is required for PVC")
		}
		cfg.PVC = &sidecar.PITRPVCConfig{MountPath: mount}
	default:
		return nil, fmt.Errorf("BLOODRAVEN_PITR_STORAGE_TYPE=%q; must be S3 or PVC", storageType)
	}

	// Encryption: when the profile enables archive encryption the
	// operator sets BLOODRAVEN_PITR_PASSPHRASE_FILE on this init
	// container. sidecar.NewArchiveStore transparently wraps the
	// concrete backend in an encryptedStore when PassphraseFile is
	// non-empty, so the download path decrypts without any further
	// plumbing here.
	cfg.PassphraseFile = os.Getenv("BLOODRAVEN_PITR_PASSPHRASE_FILE")
	return cfg, nil
}

// isSafeBasename rejects names with path separators, parent references,
// or NUL / control bytes. Manifest entries should always be plain
// basenames (the archiver writes `mysql-bin.000123`-style names); a
// payload that contains "/" or ".." is a tampered manifest.
func isSafeBasename(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, "/\\") {
		return false
	}
	for _, r := range name {
		if r < 0x20 {
			return false
		}
	}
	return true
}

// Version 1 is shared with restore_script.py. Only authenticated stream evidence
// enters this file; manifest timestamps are never coverage evidence.
type pitrStagedFile struct {
	Path                      string    `json:"path"`
	Digest                    string    `json:"sha256"`
	FirstEventTime            time.Time `json:"firstEventTime"`
	LastEventTime             time.Time `json:"lastEventTime"`
	FirstTransactionEventTime time.Time `json:"firstTransactionEventTime"`
	LastTransactionEventTime  time.Time `json:"lastTransactionEventTime"`
	TimestampsComplete        bool      `json:"timestampsComplete"`
}
type pitrSiteCoverage struct {
	replayPrefix          internalmysql.GTIDSet
	lowerGTIDs            internalmysql.GTIDSet
	unverifiableGTIDs     internalmysql.GTIDSet
	replayRefusal         string
	AnchorReason          string `json:"anchorReason,omitempty"`
	CheckpointGTIDs       string `json:"checkpointGTIDs"`
	InWindowGTIDs         string `json:"inWindowGTIDs"`
	checkpoint            internalmysql.GTIDSet
	window                internalmysql.GTIDSet
	Site                  string           `json:"site"`
	StopReached           bool             `json:"stopReached"`
	Anchored              bool             `json:"anchored"`
	StagedStopReached     bool             `json:"stagedStopReached"`
	NewestEventTime       time.Time        `json:"newestEventTime"`
	NewestReplayEventTime time.Time        `json:"newestReplayEventTime"`
	HasReplayTransactions bool             `json:"hasReplayTransactions"`
	Staged                []pitrStagedFile `json:"staged"`
}
type pitrCoverage struct {
	AllowUnverifiedCrossSite bool               `json:"allowUnverifiedCrossSite"`
	CrossSiteUnverified      bool               `json:"crossSiteUnverified"`
	Version                  int                `json:"version"`
	RefusalReason            string             `json:"refusalReason,omitempty"`
	Mode                     string             `json:"mode"`
	StopDatetime             string             `json:"stopDatetime"`
	AllowPartial             bool               `json:"allowPartial"`
	TargetCovered            bool               `json:"targetCovered"`
	Partial                  bool               `json:"partial"`
	Sites                    []pitrSiteCoverage `json:"sites"`
}

func downloadPITRArchive(ctx context.Context, store sidecar.ArchiveStore, prefix string, sites []string, stop time.Time, outDir, start string, allowPartial, allowUnverifiedCrossSite bool, mode string, logger *slog.Logger) (pitrCoverage, error) {
	if mode == "" {
		mode = "timestamp"
	}
	if mode != "timestamp" && mode != "latest" {
		return pitrCoverage{}, fmt.Errorf("invalid PITR mode %q", mode)
	}
	if mode == "latest" && (allowPartial || allowUnverifiedCrossSite) {
		return pitrCoverage{}, fmt.Errorf("allowPartial and allowUnverifiedCrossSite are not supported for latest verification")
	}
	result := pitrCoverage{Version: 1, Mode: mode, AllowUnverifiedCrossSite: allowUnverifiedCrossSite, StopDatetime: stop.UTC().Format(time.RFC3339Nano), AllowPartial: allowPartial, Sites: []pitrSiteCoverage{}}
	// Always preflight the entire prefix, even with an explicit replay set.
	keys, err := store.List(ctx, sidecar.ManifestKeyPrefix(prefix))
	if err != nil {
		return result, fmt.Errorf("PITRArchiveListFailed: list site manifests under %q: %w; restore needs list permission on the prefix (S3 ListObjectsV2)", prefix, err)
	}
	selected := make(map[string]bool)
	for _, site := range sites {
		selected[site] = true
	}
	var omitted []string
	for _, key := range keys {
		if site := sidecar.SiteFromManifestKey(key); site != "" && !selected[site] {
			omitted = append(omitted, key)
		}
	}
	sort.Strings(omitted)
	if len(omitted) > 0 {
		return result, fmt.Errorf("PITRArchiveSiteOmitted: omitted manifest keys %v; replay set %v; add the source site to pointInTime.sites for replay validation, or move a foreign archive to its own prefix", omitted, sites)
	}
	if strings.TrimSpace(start) == "" && !allowPartial {
		return result, fmt.Errorf("PITRBackupBoundUnknown: pointInTime requires a non-empty trusted source GTID bound; use a mysqlBackupRef whose backup has status.gtidExecuted, or set pointInTime.allowPartial for a restore")
	}
	planStop := stop
	if mode == "latest" {
		planStop = time.Time{}
	}
	hasTransactions := false
	var newest []string
	replaySites := append([]string(nil), sites...)
	sort.Strings(replaySites)
	for _, site := range replaySites {
		coverage, err := planPITRWindow(ctx, store, prefix, site, planStop, outDir, start, logger)
		if err != nil {
			if coverage.AnchorReason != "" {
				result.Sites = append(result.Sites, coverage)
			}
			return result, err
		}
		result.Sites = append(result.Sites, coverage)
		result.TargetCovered = result.TargetCovered || (coverage.StopReached && coverage.Anchored)
		hasTransactions = hasTransactions || coverage.HasReplayTransactions
		ts := "none"
		if !coverage.NewestEventTime.IsZero() {
			ts = coverage.NewestEventTime.Format(time.RFC3339)
		}
		newest = append(newest, site+"="+ts)
	}
	baseline, err := internalmysql.ParseGTIDSet(start)
	if err != nil {
		return result, err
	}
	refusal := validatePITRPayloadReplay(&result)
	if refusal == nil {
		refusal = validatePITRLowerEdge(&result, baseline, false)
	}
	orderErr := validatePITRCrossSiteOrder(result.Sites)
	if refusal == nil && orderErr != nil {
		if allowUnverifiedCrossSite {
			refusal = validatePITRLowerEdge(&result, baseline, true)
			if refusal == nil {
				result.CrossSiteUnverified = true
			}
		} else {
			refusal = orderErr
		}
	}
	if refusal == nil && orderErr == nil {
		refusal = validatePITRLowerEdge(&result, baseline, true)
	}
	if coverageErr := validatePITRCoveringHistory(&result, baseline); refusal == nil {
		refusal = coverageErr
	}
	result.TargetCovered = false
	for _, site := range result.Sites {
		result.TargetCovered = result.TargetCovered || (site.StopReached && site.Anchored)
	}
	if refusal == nil && mode == "latest" && !hasTransactions {
		refusal = fmt.Errorf("PITRNoArchivedReplay: no archived binlog after the backup to replay; sites searched %v; rotate the active binlog on the source (FLUSH BINARY LOGS)", sites)
	}
	if mode != "latest" && !result.TargetCovered {
		if allowPartial && refusal == nil {
			result.Partial = true
		} else if refusal == nil {
			refusal = fmt.Errorf("PITRTargetNotCovered: requested %s; newest verified archived event per site [%s]; sites searched %v; no verified stop barrier on a backup-anchored chain covering the replay history; the active binlog is archived only after rotation (FLUSH BINARY LOGS on the source), or allowPartial restores to the newest archived point", result.StopDatetime, strings.Join(newest, ", "), sites)
		}
	}
	if refusal != nil {
		result.RefusalReason = strings.SplitN(refusal.Error(), ":", 2)[0]
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return result, err
	}
	data, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	tmp := filepath.Join(outDir, "coverage.json.part")
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return result, err
	}
	if err := os.Rename(tmp, filepath.Join(outDir, "coverage.json")); err != nil {
		return result, err
	}
	return result, refusal
}

// Disjoint all-event ranges prove that the single mysqlbinlog invocation cannot
// encounter a barrier before an earlier event from a later replay site.
func validatePITRCrossSiteOrder(sites []pitrSiteCoverage) error {
	type segment struct {
		site                         string
		first, last, txFirst, txLast time.Time
		complete                     bool
	}
	var segments []segment
	for _, site := range sites {
		if len(site.Staged) == 0 {
			continue
		}
		seg := segment{site: site.Site, complete: true}
		for _, file := range site.Staged {
			if !file.TimestampsComplete || file.FirstEventTime.IsZero() || file.LastEventTime.IsZero() || file.LastEventTime.Before(file.FirstEventTime) {
				seg.complete = false
			}
			if seg.first.IsZero() || file.FirstEventTime.Before(seg.first) {
				seg.first = file.FirstEventTime
			}
			if file.LastEventTime.After(seg.last) {
				seg.last = file.LastEventTime
			}
			if !file.FirstTransactionEventTime.IsZero() && (seg.txFirst.IsZero() || file.FirstTransactionEventTime.Before(seg.txFirst)) {
				seg.txFirst = file.FirstTransactionEventTime
			}
			if file.LastTransactionEventTime.After(seg.txLast) {
				seg.txLast = file.LastTransactionEventTime
			}
		}
		segments = append(segments, seg)
	}
	if len(segments) < 2 {
		return nil
	}
	var details []string
	valid := true
	stamp := func(t time.Time) string {
		if t.IsZero() {
			return "none"
		}
		return t.Format(time.RFC3339)
	}
	for i, seg := range segments {
		details = append(details, fmt.Sprintf("%s events=[%s,%s] transaction events=[%s,%s] timestampsComplete=%t", seg.site, stamp(seg.first), stamp(seg.last), stamp(seg.txFirst), stamp(seg.txLast), seg.complete))
		if !seg.complete || (i > 0 && !segments[i-1].last.Before(seg.first)) {
			valid = false
		}
	}
	if !valid {
		return fmt.Errorf("PITRCrossSiteOrder: staged site streams overlap, lack complete event-time evidence, or are not chronological in replay order: %s; for targets after the failover, use a full backup taken after the failover and before the target for a single-site window; for targets before the failover, the only restore option is pointInTime.allowUnverifiedCrossSite (correct when the first-sorted site holds the complete pre-target history); cross-site completeness is not verified with that opt-in, and verification cannot cover these pre-failover windows until #206; clock skew can cause conservative refusals", strings.Join(details, "; "))
	}
	return nil
}

// Lower-edge history must exist in the dump or in complete staged prefixes.
// A GTID announcement inside a stop file alone cannot supply missing bytes.
func validatePITRLowerEdge(result *pitrCoverage, baseline internalmysql.GTIDSet, globalStop bool) error {
	if baseline.IsEmpty() {
		return nil
	}
	available := sidecar.UnionBinlogGTIDs(baseline)
	unverifiable := make(internalmysql.GTIDSet)
	for _, site := range result.Sites {
		unverifiable = sidecar.UnionBinlogGTIDs(unverifiable, site.unverifiableGTIDs)
		available = sidecar.UnionBinlogGTIDs(available, site.lowerGTIDs)
		if globalStop && site.StagedStopReached {
			break
		}
	}
	var refusal error
	for i := range result.Sites {
		site := &result.Sites[i]
		if !site.Anchored && !site.HasReplayTransactions {
			continue
		}
		if !available.Contains(site.checkpoint) {
			site.Anchored = false
			site.AnchorReason = "archive does not contain transactions between the backup bound and " + site.checkpoint.String()
			if unverifiable.Contains(site.checkpoint.Subtract(available)) {
				site.AnchorReason = pitrUnverifiableAnchorReason
			}
			if refusal == nil {
				refusal = fmt.Errorf("PITRTargetNotCovered: site %s: %s", site.Site, site.AnchorReason)
			}
		}
	}
	return refusal
}

// With every replay site retained, a stop may certify only history that includes
// all staged writers. This is containment evidence, not cross-site replay proof.
func validatePITRCoveringHistory(result *pitrCoverage, baseline internalmysql.GTIDSet) error {
	var refusal error
	for i := range result.Sites {
		site := &result.Sites[i]
		if !site.StopReached {
			continue
		}
		history := sidecar.UnionBinlogGTIDs(site.checkpoint, site.replayPrefix)
		for j, other := range result.Sites {
			if i == j {
				continue
			}
			if !history.Contains(other.window.Subtract(baseline)) {
				site.Anchored = false
				if site.AnchorReason == "" {
					site.AnchorReason = "stop history does not contain other sites' in-window GTIDs"
					if site.unverifiableGTIDs.Contains(other.window.Subtract(baseline).Subtract(history)) {
						site.AnchorReason = pitrUnverifiableAnchorReason
					}
				}
				if refusal == nil {
					refusal = fmt.Errorf("PITRTargetNotCovered: site %s: %s; use a full backup that covers the required history and precedes the target; allowUnverifiedCrossSite does not bypass missing or unverifiable coverage", site.Site, site.AnchorReason)
				}
			}
		}
	}
	return refusal
}

const pitrUnverifiableAnchorReason = "archived file lacks verifiable transaction evidence (XA/checksum-off/unsupported encoding)"

func validatePITRPayloadReplay(result *pitrCoverage) error {
	var refusal error
	for i := range result.Sites {
		site := &result.Sites[i]
		if site.replayRefusal != "" {
			site.Anchored = false
			site.AnchorReason = site.replayRefusal
			if refusal == nil {
				refusal = fmt.Errorf("PITRTargetNotCovered: site %s: %s", site.Site, site.AnchorReason)
			}
		}
		if site.StagedStopReached {
			break
		}
	}
	return refusal
}
