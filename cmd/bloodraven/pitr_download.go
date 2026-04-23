package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
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
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	stopDT := os.Getenv("BLOODRAVEN_PITR_STOP_DATETIME")
	if stopDT == "" {
		logger.Error("BLOODRAVEN_PITR_STOP_DATETIME is required")
		os.Exit(2)
	}
	outDir := os.Getenv("BLOODRAVEN_PITR_OUTPUT_DIR")
	if outDir == "" {
		logger.Error("BLOODRAVEN_PITR_OUTPUT_DIR is required")
		os.Exit(2)
	}
	prefix := os.Getenv("BLOODRAVEN_PITR_MANIFEST_PREFIX")
	if prefix == "" {
		logger.Error("BLOODRAVEN_PITR_MANIFEST_PREFIX is required")
		os.Exit(2)
	}

	// Sites list comes from the operator (fg.Spec.Sites). Used as a
	// fast path to skip the list call for small clusters; when empty
	// we fall back to listing the manifest prefix and harvesting site
	// names from filenames (which also catches decommissioned sites
	// that still have binlogs in the archive).
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

	stopTime, err := parsePITRStopTime(stopDT)
	if err != nil {
		logger.Error("parse stop-datetime", "value", stopDT, "error", err)
		os.Exit(2)
	}

	// Discover sites when the operator didn't pass them explicitly.
	if len(sites) == 0 {
		discovered, err := discoverSites(ctx, store, prefix)
		if err != nil {
			logger.Error("list site manifests", "prefix", prefix, "error", err)
			os.Exit(2)
		}
		sites = discovered
	}
	if len(sites) == 0 {
		logger.Warn("no site manifests found under prefix; nothing to replay",
			"prefix", prefix)
		os.Exit(0)
	}

	totalDownloaded := 0
	for _, site := range sites {
		n, err := downloadSiteBinlogs(ctx, store, prefix, site, stopTime, outDir, logger)
		if err != nil {
			logger.Error("download site binlogs", "site", site, "error", err)
			os.Exit(2)
		}
		totalDownloaded += n
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

// parsePITRStopTime normalizes the user-provided stop datetime. We
// accept both RFC 3339 ("2026-04-15T09:30:00Z", "…+00:00") and the
// MySQL-native form ("2026-04-15 09:30:00") so operators can paste
// either without mental conversion. The returned time is UTC; all
// manifest timestamps are also UTC, so comparisons are direct.
func parsePITRStopTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized datetime: %q", s)
}

// discoverSites lists the manifest prefix and returns the set of site
// names corresponding to manifest-<site>.json objects. Used when the
// operator didn't pass an explicit site list — this makes the
// download robust against decommissioned sites whose binlogs are
// still in the archive.
func discoverSites(ctx context.Context, store sidecar.ArchiveStore, prefix string) ([]string, error) {
	keys, err := store.List(ctx, sidecar.ManifestKeyPrefix(prefix))
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var sites []string
	for _, k := range keys {
		site := sidecar.SiteFromManifestKey(k)
		if site == "" {
			continue
		}
		if _, dup := seen[site]; dup {
			continue
		}
		seen[site] = struct{}{}
		sites = append(sites, site)
	}
	return sites, nil
}

// downloadSiteBinlogs reads one site's manifest, filters entries whose
// FirstEventTime is strictly after stopTime (they can't contribute
// transactions inside the replay window), and downloads each surviving
// file into <outDir>/<site>/<name>. Returns the number of files
// downloaded.
//
// Entries with a zero FirstEventTime are conservatively kept: the
// parser couldn't find a usable first timestamp (truncated file,
// malformed header), and the cheapest safe default is to replay it
// and let GTID dedup on the server filter applied transactions.
func downloadSiteBinlogs(
	ctx context.Context,
	store sidecar.ArchiveStore,
	prefix, site string,
	stopTime time.Time,
	outDir string,
	logger *slog.Logger,
) (int, error) {
	m, err := sidecar.LoadManifest(ctx, store, prefix, site)
	if err != nil {
		return 0, err
	}
	// Reject manifest entries whose Name would escape the site's
	// staging directory or whose stored RemotePath points outside the
	// trusted manifest prefix. A compromised bucket could otherwise
	// have a crafted manifest yank arbitrary objects (e.g. from
	// another tenant's prefix) or land files in /tmp / /var/run/secrets
	// via basename traversal (AUDIT H1).
	siteDir := filepath.Join(outDir, site)
	if err := os.MkdirAll(siteDir, 0o755); err != nil {
		return 0, fmt.Errorf("mkdir %s: %w", siteDir, err)
	}
	absSiteDir, err := filepath.Abs(siteDir)
	if err != nil {
		return 0, fmt.Errorf("abs %s: %w", siteDir, err)
	}
	// Reconstruct the expected RemotePath from trusted inputs
	// (profile prefix + site + basename) rather than trusting the
	// stored value — the manifest attacker has no way to influence
	// the prefix + site pair we assemble here.
	allowedRemotePrefix := path.Join(prefix, site) + "/"
	downloaded := 0
	for _, e := range m.Files {
		if !e.FirstEventTime.IsZero() && e.FirstEventTime.After(stopTime) {
			continue
		}
		if !isSafeBasename(e.Name) {
			return downloaded, fmt.Errorf("reject manifest entry with unsafe name %q", e.Name)
		}
		dst, err := safeJoinUnderRoot(absSiteDir, e.Name)
		if err != nil {
			return downloaded, fmt.Errorf("reject unsafe manifest name %q: %w", e.Name, err)
		}
		// Accept the stored RemotePath only when it lies under the
		// expected prefix+site; otherwise rebuild it from trusted
		// fields.
		remotePath := e.RemotePath
		if remotePath == "" || !strings.HasPrefix(remotePath, allowedRemotePrefix) {
			remotePath = path.Join(prefix, site, e.Name)
		}
		if err := store.GetFile(ctx, remotePath, dst); err != nil {
			return downloaded, fmt.Errorf("download %s: %w", remotePath, err)
		}
		logger.Info("archived binlog downloaded",
			"site", site,
			"name", e.Name,
			"size", e.Size,
			"first_event", e.FirstEventTime.Format(time.RFC3339))
		downloaded++
	}
	return downloaded, nil
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
