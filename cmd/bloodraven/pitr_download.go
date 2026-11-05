package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

// runPITRDownload is the entry point for `bloodraven pitr-download`, the
// restore and verification Jobs' PITR init container.
//
// #206 series 1: it opens the source archive as a reader and checks its
// identity (the descriptor, its encryption and keyCheck, every site
// manifest as version 2 of the descriptor's archive, no verified reuse),
// then refuses PITRReplayUnsupported. The per-site planner that replayed
// version-1 archives is gone, and the chronological planner (series 2,
// internal/pitrplan) is not in this build, so no archive can be replayed;
// a restore without pointInTime is unaffected (D-40).
//
// Env (see internal/controller/pitr.go for where each is set):
//
//	BLOODRAVEN_PITR_STOP_DATETIME     the target (validated, not yet used)
//	BLOODRAVEN_PITR_OUTPUT_DIR        the staging emptyDir
//	BLOODRAVEN_PITR_DESTINATION       the normalized archive destination
//	BLOODRAVEN_PITR_MANIFEST_PREFIX   its key prefix (must agree)
//	BLOODRAVEN_PITR_ENCRYPTION_MODE   None or Passphrase, from the archive profile
//	BLOODRAVEN_PITR_PASSPHRASE_FILE   the archive key when Passphrase
//	plus the S3/PVC storage env pitrDownloadConfigFromEnv reads.
func runPITRDownload(args []string) {
	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo)

	for _, name := range []string{"BLOODRAVEN_PITR_STOP_DATETIME", "BLOODRAVEN_PITR_OUTPUT_DIR", "BLOODRAVEN_PITR_MANIFEST_PREFIX", "BLOODRAVEN_PITR_DESTINATION", "BLOODRAVEN_PITR_ENCRYPTION_MODE"} {
		if os.Getenv(name) == "" {
			logger.Error("missing required env var", "error", name+" is required")
			os.Exit(2)
		}
	}
	stopDT := os.Getenv("BLOODRAVEN_PITR_STOP_DATETIME")
	if _, err := backupmeta.ParsePITRStopTime(stopDT); err != nil {
		logger.Error("parse stop-datetime", "value", stopDT, "error", err)
		os.Exit(2)
	}
	req, err := pitrArchiveRequestFromEnv(os.Getenv)
	if err != nil {
		logger.Error("invalid archive config", "error", err)
		os.Exit(2)
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

	// openPITRArchive always refuses in series 1: an identity failure, or
	// PITRReplayUnsupported for an archive that passed every check.
	// Kubernetes keeps only the last 4096 bytes, so the stable reason comes
	// first and the message is bounded; the full error is logged.
	err = openPITRArchive(ctx, store, req)
	_ = os.WriteFile("/dev/termination-log", []byte(compactTerminationMessage(err.Error())), 0644)
	logger.Error("download site binlogs", "error", err)
	os.Exit(2)
}

// pitrArchiveRequest is what pitr-download expects of the archive it reads.
type pitrArchiveRequest struct {
	prefix     string
	expect     pitrarchive.Expectation
	passphrase bool
}

// pitrArchiveRequestFromEnv reads the reader's expectation. The rendered
// manifest prefix must be the destination's key prefix: the two are
// rendered from one normalized destination, so a disagreement is a stale
// or hand-edited Job.
func pitrArchiveRequestFromEnv(getenv func(string) string) (pitrArchiveRequest, error) {
	dest, err := pitrarchive.ParseDestination(getenv("BLOODRAVEN_PITR_DESTINATION"))
	if err != nil {
		return pitrArchiveRequest{}, fmt.Errorf("BLOODRAVEN_PITR_DESTINATION: %w", err)
	}
	prefix := getenv("BLOODRAVEN_PITR_MANIFEST_PREFIX")
	if prefix != dest.KeyPrefix {
		return pitrArchiveRequest{}, fmt.Errorf("BLOODRAVEN_PITR_MANIFEST_PREFIX %q is not the key prefix %q of %s", prefix, dest.KeyPrefix, dest)
	}
	enc := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionMode(getenv("BLOODRAVEN_PITR_ENCRYPTION_MODE"))}
	if enc.Mode == pitrarchive.EncryptionPassphrase {
		enc.Algorithm = pitrarchive.AlgorithmAES256GCM
	}
	if err := enc.Validate(); err != nil {
		return pitrArchiveRequest{}, fmt.Errorf("BLOODRAVEN_PITR_ENCRYPTION_MODE: %w", err)
	}
	hasKey := getenv("BLOODRAVEN_PITR_PASSPHRASE_FILE") != ""
	if hasKey != (enc.Mode == pitrarchive.EncryptionPassphrase) {
		return pitrArchiveRequest{}, fmt.Errorf("BLOODRAVEN_PITR_ENCRYPTION_MODE=%s with passphrase file set=%t", enc.Mode, hasKey)
	}
	return pitrArchiveRequest{
		prefix:     prefix,
		passphrase: hasKey,
		expect:     pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: dest, Encryption: enc},
	}, nil
}

// openPITRArchive opens the archive as a reader (§3.3, §4.2): the
// descriptor, then every site manifest, which must be version 2 and name
// the descriptor's archive, then verified reuse. It returns the first
// refusal, or PITRReplayUnsupported for an archive that passed every check
// (series 1 has no planner).
func openPITRArchive(ctx context.Context, store sidecar.ArchiveStore, req pitrArchiveRequest) error {
	desc, err := sidecar.OpenArchiveForRead(ctx, store, req.prefix, req.expect)
	if err != nil {
		if _, ok := pitrarchive.TokenOf(err); ok {
			return err
		}
		return fmt.Errorf("%s: open archive at %s: %w", pitrarchive.TokenArchiveListFailed, req.expect.Destination, err)
	}
	dest := req.expect.Destination.String()
	keys, err := store.List(ctx, sidecar.ManifestKeyPrefix(req.prefix))
	if err != nil {
		return fmt.Errorf("%s: list site manifests under %q: %w; restore needs list permission on the prefix (S3 ListObjectsV2)", pitrarchive.TokenArchiveListFailed, req.prefix, err)
	}
	sort.Strings(keys)
	var sites []string
	for _, key := range keys {
		site := sidecar.SiteFromManifestKey(key)
		if site == "" {
			continue
		}
		if !isSafeBasename(site) {
			return fmt.Errorf("%s: archive %s at %s: manifest key %q names an unsafe site", pitrarchive.TokenArchiveObjectInvalid, desc.ArchiveID, dest, key)
		}
		m, err := sidecar.LoadManifest(ctx, store, req.prefix, site)
		if err != nil {
			if _, ok := pitrarchive.ParseToken(err.Error()); ok {
				return err
			}
			return fmt.Errorf("%s: archive %s at %s: manifest %s: %w", pitrarchive.TokenArchiveObjectInvalid, desc.ArchiveID, dest, key, err)
		}
		if m.ArchiveID != desc.ArchiveID {
			return fmt.Errorf("%s: archive %s at %s: manifest %s names archive %q; the prefix mixes two archives", pitrarchive.TokenArchiveIdentityMismatch, desc.ArchiveID, dest, key, m.ArchiveID)
		}
		if m.Epoch != nil && m.Epoch.ResetReason != "" {
			return fmt.Errorf("%s: archive %s at %s: site %s: %s; start a new archive prefix and take a full backup", pitrarchive.TokenArchiveHistoryReset, desc.ArchiveID, dest, site, m.Epoch.ResetReason)
		}
		sites = append(sites, site)
	}
	return fmt.Errorf("%s: archive %s at %s (source %s/%s, %d site manifests %s) passed its identity checks, but this build cannot replay a version-2 archive; restore without pointInTime, or use a release with the chronological planner",
		pitrarchive.TokenReplayUnsupported, desc.ArchiveID, dest, desc.Source.Namespace, desc.Source.Name, len(sites), boundedList(sites))
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
