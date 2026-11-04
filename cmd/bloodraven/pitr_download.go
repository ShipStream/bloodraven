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
// It runs as an init container on the restore Job, plans the PITR
// replay window from verified archive bytes, stages the replaying
// site's files and coverage.json into a shared emptyDir for the mysqlsh
// container, and refuses windows it cannot certify.
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

	cfg, err := pitrDownloadStoreConfig()
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

	store, err := newArchiveStore(ctx, cfg)
	if err != nil {
		logger.Error("init archive store", "error", err)
		os.Exit(2)
	}

	stopTime, err := backupmeta.ParsePITRStopTime(stopDT)
	if err != nil {
		logger.Error("parse stop-datetime", "value", stopDT, "error", err)
		os.Exit(2)
	}

	result, err := planPITRArchive(ctx, store, pitrRequest{
		prefix:                   prefix,
		sites:                    sites,
		stop:                     stopTime,
		outDir:                   outDir,
		start:                    os.Getenv("BLOODRAVEN_PITR_START_GTIDS"),
		exclude:                  os.Getenv("BLOODRAVEN_PITR_EXCLUDE_GTIDS"),
		allowPartial:             os.Getenv("BLOODRAVEN_PITR_ALLOW_PARTIAL") == "true",
		allowUnverifiedCrossSite: os.Getenv("BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE") == "true",
		mode:                     os.Getenv("BLOODRAVEN_PITR_MODE"),
	}, logger)
	if err != nil {
		// Kubernetes keeps only the last 4096 bytes, so the stable reason
		// comes first and the message is bounded; the full error is logged.
		_ = os.WriteFile("/dev/termination-log", []byte(compactTerminationMessage(err.Error())), 0644)
		logger.Error("download site binlogs", "error", err)
		os.Exit(2)
	}
	totalDownloaded := 0
	for _, site := range result.Sites {
		totalDownloaded += len(site.Staged)
	}
	logger.Info("pitr-download complete", "files", totalDownloaded, "sites", len(sites))
}

// newArchiveStore builds every subcommand's archive store. Tests that
// cannot pass a PVC mount verifier through the environment (re-executed
// subcommands) wrap it; production never replaces it.
var newArchiveStore = sidecar.NewArchiveStore

// pitrDownloadStoreConfig is the archive config for pitr-download. It
// only reads (the restore mounts the PVC read-only), so the PVC store
// verifies the mount but does not probe write access.
func pitrDownloadStoreConfig() (*sidecar.PITRConfig, error) {
	return pitrDownloadConfigFromEnv()
}

// pitrQuarantineStoreConfig is the archive config for pitr-quarantine,
// which rewrites manifests under BLOODRAVEN_PITR_MANIFEST_PREFIX.
func pitrQuarantineStoreConfig() (*sidecar.PITRConfig, error) {
	cfg, err := pitrDownloadConfigFromEnv()
	if err != nil {
		return nil, err
	}
	if cfg.PVC != nil {
		cfg.PVC.Writable = true
	}
	return cfg, nil
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

// compactTerminationMessage keeps the stable reason prefix of a refusal within
// the kubelet's 4096-byte termination message limit.
func compactTerminationMessage(message string) string {
	const limit = 3072
	if len(message) <= limit {
		return message
	}
	return message[:limit] + " ... (truncated; see pitr-download log)"
}

// Version 1 is shared with restore_script.py. Only authenticated stream evidence
// enters this file; manifest timestamps are never coverage evidence. Event-time
// ranges are diagnostics only: no policy gates on them.
type pitrStagedFile struct {
	Path                      string    `json:"path"`
	Digest                    string    `json:"sha256"`
	PreviousGTIDs             string    `json:"previousGTIDs"`
	GTIDs                     string    `json:"gtids"`
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
	postStopGTIDs         internalmysql.GTIDSet
	replayRefusal         string
	AnchorReason          string `json:"anchorReason,omitempty"`
	CheckpointGTIDs       string `json:"checkpointGTIDs"`
	InWindowGTIDs         string `json:"inWindowGTIDs"`
	LowerGTIDs            string `json:"lowerGTIDs"`
	PostStopGTIDs         string `json:"postStopGTIDs"`
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
	AllowUnverifiedCrossSite bool   `json:"allowUnverifiedCrossSite"`
	CrossSiteUnverified      bool   `json:"crossSiteUnverified"`
	Version                  int    `json:"version"`
	RefusalReason            string `json:"refusalReason,omitempty"`
	Mode                     string `json:"mode"`
	StopDatetime             string `json:"stopDatetime"`
	AllowPartial             bool   `json:"allowPartial"`
	TargetCovered            bool   `json:"targetCovered"`
	Partial                  bool   `json:"partial"`
	// ProofSite names the site whose verified stop certifies coverage.
	ProofSite string             `json:"proofSite,omitempty"`
	Sites     []pitrSiteCoverage `json:"sites"`
}

type pitrRequest struct {
	prefix                   string
	sites                    []string
	stop                     time.Time
	outDir                   string
	start                    string
	exclude                  string
	allowPartial             bool
	allowUnverifiedCrossSite bool
	mode                     string
}

func planPITRArchive(ctx context.Context, store sidecar.ArchiveStore, req pitrRequest, logger *slog.Logger) (pitrCoverage, error) {
	mode, sites, stop, outDir := req.mode, req.sites, req.stop, req.outDir
	allowPartial, allowUnverifiedCrossSite := req.allowPartial, req.allowUnverifiedCrossSite
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
	keys, err := store.List(ctx, sidecar.ManifestKeyPrefix(req.prefix))
	if err != nil {
		return result, fmt.Errorf("PITRArchiveListFailed: list site manifests under %q: %w; restore needs list permission on the prefix (S3 ListObjectsV2)", req.prefix, err)
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
		return result, fmt.Errorf("PITRArchiveSiteOmitted: omitted manifest keys %s; replay set %s; add the source site to pointInTime.sites for replay validation, or move a foreign archive to its own prefix", boundedList(omitted), boundedList(sites))
	}
	if strings.TrimSpace(req.start) == "" && !allowPartial {
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
		coverage, err := planPITRWindow(ctx, store, req.prefix, site, planStop, outDir, req.start, req.exclude, logger)
		if err != nil {
			if coverage.AnchorReason != "" {
				result.Sites = append(result.Sites, coverage)
			}
			return result, err
		}
		result.Sites = append(result.Sites, coverage)
		hasTransactions = hasTransactions || coverage.HasReplayTransactions
		ts := "none"
		if !coverage.NewestEventTime.IsZero() {
			ts = coverage.NewestEventTime.Format(time.RFC3339)
		}
		newest = append(newest, site+"="+ts)
	}
	baseline, err := internalmysql.ParseGTIDSet(req.start)
	if err != nil {
		return result, err
	}

	// Only one site's files may replay. A site whose staged files apply no
	// transaction would only end the single mysqlbinlog run early, so it is
	// left out when another site also stages files.
	var replaying []string
	stagedSites := 0
	for _, site := range result.Sites {
		if site.HasReplayTransactions {
			replaying = append(replaying, site.Site)
		}
		if len(site.Staged) > 0 {
			stagedSites++
		}
	}
	if stagedSites > 1 {
		for i := range result.Sites {
			site := &result.Sites[i]
			if site.HasReplayTransactions || len(site.Staged) == 0 {
				continue
			}
			if err := unstagePITRSite(outDir, site); err != nil {
				return result, err
			}
		}
	}
	crossSite := len(replaying) > 1
	refusal := validatePITRPayloadReplay(&result)
	if refusal == nil {
		refusal = validatePITRLowerEdge(&result, baseline, crossSite)
	}
	if refusal == nil && crossSite {
		if allowUnverifiedCrossSite {
			result.CrossSiteUnverified = true
		} else {
			refusal = pitrCrossSiteRefusal(replaying)
		}
	}
	// An unauthorized cross-site window certifies nothing.
	if !crossSite || result.CrossSiteUnverified {
		result.ProofSite = pitrProofSite(result.Sites, len(replaying))
	}
	result.TargetCovered = result.ProofSite != ""
	if refusal == nil && mode == "latest" && !hasTransactions {
		refusal = fmt.Errorf("PITRNoArchivedReplay: no archived binlog after the backup to replay; sites searched %s; rotate the active binlog on the source (FLUSH BINARY LOGS)", boundedList(sites))
	}
	if mode != "latest" && !result.TargetCovered {
		if allowPartial && refusal == nil {
			result.Partial = true
		} else if refusal == nil {
			var reasons []string
			for _, site := range result.Sites {
				if site.StopReached && site.AnchorReason != "" {
					reasons = append(reasons, site.Site+": "+site.AnchorReason)
				}
			}
			remedy := "the active binlog is archived only after rotation (FLUSH BINARY LOGS on the source), or allowPartial restores to the newest archived point"
			if len(reasons) > 0 {
				// A stop exists but cannot certify the backup's history;
				// rotating again does not change that.
				remedy = strings.Join(boundedSlice(reasons), "; ") + "; for an unanchored chain use a backup taken on this archive's lineage, for an unproven stop rotate again after a write on the source, or allowPartial restores to the newest archived point"
			}
			refusal = fmt.Errorf("PITRTargetNotCovered: requested %s; newest verified archived event per site [%s]; sites searched %s; no verified stop barrier on a backup-anchored chain covering the replay history; %s", result.StopDatetime, strings.Join(boundedSlice(newest), ", "), boundedList(sites), remedy)
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
	// The main container may run as a different UID; the emptyDir is pod-private.
	tmp := filepath.Join(outDir, "coverage.json.part")
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return result, err
	}
	if err := os.Chmod(tmp, 0644); err != nil {
		return result, err
	}
	if err := os.Rename(tmp, filepath.Join(outDir, "coverage.json")); err != nil {
		return result, err
	}
	return result, refusal
}

// pitrProofSite returns the site whose own verified stop certifies coverage.
// With one replaying site only that site can; with none, any anchored stop
// shows an idle window; with several (unverified), the first in replay order
// whose staged files reach the stop is where mysqlbinlog ends.
func pitrProofSite(sites []pitrSiteCoverage, replaying int) string {
	for _, site := range sites {
		switch {
		case replaying == 1 && !site.HasReplayTransactions:
			continue
		case replaying > 1 && !site.StagedStopReached:
			continue
		}
		if site.StopReached && site.Anchored {
			return site.Site
		}
		if replaying > 0 {
			return ""
		}
	}
	return ""
}

func unstagePITRSite(outDir string, site *pitrSiteCoverage) error {
	for _, file := range site.Staged {
		path, err := safeJoinUnderRoot(outDir, filepath.FromSlash(file.Path))
		if err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	site.Staged = []pitrStagedFile{}
	site.StagedStopReached = false
	site.lowerGTIDs = nil
	site.LowerGTIDs = ""
	site.window = nil
	site.InWindowGTIDs = ""
	site.replayRefusal = ""
	site.NewestReplayEventTime = time.Time{}
	return nil
}

func pitrCrossSiteRefusal(replaying []string) error {
	return fmt.Errorf("PITRCrossSiteOrder: archived files from %d sites %s hold transactions to replay; windows that span sites are not certified until #206; for a target after the failover, use a full backup taken after the failover and before the target, so one site replays; for a target before the failover, a restore can set pointInTime.allowUnverifiedCrossSite, which replays sites in name order without proving cross-site completeness; verification cannot cover these windows", len(replaying), boundedList(replaying))
}

const pitrListBound = 8

func boundedSlice(items []string) []string {
	if len(items) <= pitrListBound {
		return items
	}
	out := append([]string(nil), items[:pitrListBound]...)
	return append(out, fmt.Sprintf("... %d more", len(items)-pitrListBound))
}

// boundedList keeps refusal messages within the termination message limit.
func boundedList(items []string) string {
	return "[" + strings.Join(boundedSlice(items), " ") + "]"
}

// Lower-edge history must exist in the dump or in complete staged prefixes.
// A GTID announcement inside a stop file alone cannot supply missing bytes.
// Transactions another site's archive shows committed after the target
// explain a later site's predecessor history without being replayed.
func validatePITRLowerEdge(result *pitrCoverage, baseline internalmysql.GTIDSet, crossSite bool) error {
	if baseline.IsEmpty() {
		return nil
	}
	available := sidecar.UnionBinlogGTIDs(baseline)
	unverifiable := make(internalmysql.GTIDSet)
	reached := false
	for _, site := range result.Sites {
		available = sidecar.UnionBinlogGTIDs(available, site.postStopGTIDs)
		if reached {
			continue
		}
		unverifiable = sidecar.UnionBinlogGTIDs(unverifiable, site.unverifiableGTIDs)
		available = sidecar.UnionBinlogGTIDs(available, site.lowerGTIDs)
		if crossSite && site.StagedStopReached {
			reached = true
		}
	}
	var refusal error
	for i := range result.Sites {
		site := &result.Sites[i]
		if !site.HasReplayTransactions && !(site.StopReached && site.Anchored) {
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

const pitrUnverifiableAnchorReason = "archived file lacks verifiable transaction evidence (XA/checksum-off/unsupported encoding)"

func validatePITRPayloadReplay(result *pitrCoverage) error {
	var refusal error
	for i := range result.Sites {
		site := &result.Sites[i]
		if len(site.Staged) == 0 {
			continue
		}
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
