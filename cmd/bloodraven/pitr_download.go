package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/aws/smithy-go"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

// runPITRDownload is the entry point for `bloodraven pitr-download`, the
// restore and verification Jobs' PITR init container (design §4). It is
// the planner's glue:
//
//  1. read /pitr-dump-meta/dump.json (written by dump-meta);
//  2. open the archive as a reader, expecting the dump stamp's source UID
//     and archive ID (§3.3), and load every site manifest as version 2 of
//     that archive, refusing any verified reuse (§4.2);
//  3. run internal/pitrplan, which downloads and indexes rows on demand;
//  4. stage the plan's files under BLOODRAVEN_PITR_OUTPUT_DIR as
//     <site>/<name>.<id>, and write coverage.json version 2 atomically,
//     also on refusal (with refusalReason set and no files).
//
// Env (rendered by internal/controller's buildRestorePITRFragmentsFor in
// pitr.go):
//
//	BLOODRAVEN_PITR_MODE               timestamp | latest
//	BLOODRAVEN_PITR_STOP_DATETIME      T in backupmeta.MicroTimeLayout (timestamp mode only)
//	BLOODRAVEN_PITR_ALLOW_PARTIAL      true | false
//	BLOODRAVEN_PITR_EXCLUDE_GTIDS      X (optional)
//	BLOODRAVEN_PITR_DUMP_META          /pitr-dump-meta/dump.json
//	BLOODRAVEN_PITR_OUTPUT_DIR         the staging emptyDir
//	BLOODRAVEN_PITR_DESTINATION        the normalized archive destination
//	BLOODRAVEN_PITR_MANIFEST_PREFIX    its key prefix (must agree)
//	BLOODRAVEN_PITR_ENCRYPTION_MODE    None or Passphrase, from the archive profile
//	BLOODRAVEN_PITR_PASSPHRASE_FILE    the archive key when Passphrase
//	BLOODRAVEN_PITR_MEMORY_LIMIT_BYTES the container's memory limit (optional; else the cgroup's)
//	BLOODRAVEN_PITR_PLAN_MEMORY_BYTES  the planner budget (default 60% of the memory limit, or 512 MiB; D-42)
//	BLOODRAVEN_PITR_STAGING_LIMIT_BYTES the staging emptyDir's sizeLimit (optional)
//	plus the S3/PVC storage env pitrDownloadConfigFromEnv reads.
//
// With a known memory limit, the Go soft memory limit is set to 90% of it
// (unless GOMEMLIMIT is set), so the heap stays near the live data the
// budget charges and planning refuses PITRPlanTooLarge instead of being
// OOM-killed (review round 1 P3).
//
// The planner's arrays that grow with the number of transactions (the
// index, the occurrence registry, the plan search, the order check) spill
// to unlinked files under <output dir>/.pitr-spill once they pass about
// half of the planner budget (issue #266, pitr_download_spill.go), so a
// large window costs staging disk instead of refusing on memory. The
// directory is gone before the plan's files are staged.
func runPITRDownload(args []string) {
	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo)
	fail := func(code int, msg string, err error) {
		_ = os.WriteFile("/dev/termination-log", []byte(compactTerminationMessage(err.Error())), 0644)
		logger.Error(msg, "error", err)
		os.Exit(code)
	}
	req, err := pitrDownloadRequestFromEnv(os.Getenv)
	if err != nil {
		fail(2, "invalid pitr-download config", err)
	}
	if soft := pitrSoftMemoryLimit(req.memoryLimit, os.Getenv("GOMEMLIMIT")); soft > 0 {
		debug.SetMemoryLimit(soft)
	}
	cfg, err := pitrDownloadStoreConfig()
	if err != nil {
		fail(2, "invalid archive config", err)
	}
	if err := checkStoreDestination(cfg, req.archive.expect.Destination); err != nil {
		fail(2, "invalid archive config", err)
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
		fail(2, "init archive store", err)
	}
	if _, err := planAndStage(ctx, store, req, logger); err != nil {
		// Kubernetes keeps only the last 4096 bytes, so the stable token
		// comes first and the message is bounded; the full error is logged.
		fail(2, "download site binlogs", err)
	}
}

// pitrDownloadRequest is everything pitr-download needs besides the store.
type pitrDownloadRequest struct {
	archive      pitrArchiveRequest
	mode         pitrplan.Mode
	stop         time.Time
	allowPartial bool
	exclude      pitrplan.Set
	excludeRaw   string
	dumpMetaPath string
	outDir       string
	delta        time.Duration
	memory       int64
	// memoryLimit is the container's memory limit, 0 when unknown.
	memoryLimit int64
	// stagingLimit is the staging emptyDir's sizeLimit, 0 when unset.
	stagingLimit int64
}

func pitrDownloadRequestFromEnv(getenv func(string) string) (pitrDownloadRequest, error) {
	var req pitrDownloadRequest
	for _, name := range []string{"BLOODRAVEN_PITR_MODE", "BLOODRAVEN_PITR_OUTPUT_DIR", "BLOODRAVEN_PITR_DUMP_META", "BLOODRAVEN_PITR_MANIFEST_PREFIX", "BLOODRAVEN_PITR_DESTINATION", "BLOODRAVEN_PITR_ENCRYPTION_MODE"} {
		if getenv(name) == "" {
			return req, fmt.Errorf("%s is required", name)
		}
	}
	req.mode = pitrplan.Mode(getenv("BLOODRAVEN_PITR_MODE"))
	stop := getenv("BLOODRAVEN_PITR_STOP_DATETIME")
	switch req.mode {
	case pitrplan.ModeTimestamp:
		t, err := backupmeta.ParseMicroTime(stop)
		if err != nil || t.String() != stop {
			return req, fmt.Errorf("%s: BLOODRAVEN_PITR_STOP_DATETIME %q is not in the canonical µs UTC form %s", pitrarchive.TokenTargetInvalid, stop, backupmeta.MicroTimeLayout)
		}
		req.stop = t.Time
	case pitrplan.ModeLatest:
		if stop != "" {
			return req, fmt.Errorf("%s: latest mode takes no BLOODRAVEN_PITR_STOP_DATETIME", pitrarchive.TokenTargetInvalid)
		}
	default:
		return req, fmt.Errorf("BLOODRAVEN_PITR_MODE=%q; must be timestamp or latest", req.mode)
	}
	switch v := getenv("BLOODRAVEN_PITR_ALLOW_PARTIAL"); v {
	case "", "false":
	case "true":
		req.allowPartial = true
	default:
		return req, fmt.Errorf("BLOODRAVEN_PITR_ALLOW_PARTIAL=%q; must be true or false", v)
	}
	req.excludeRaw = getenv("BLOODRAVEN_PITR_EXCLUDE_GTIDS")
	// X is user input; MySQL reads its UUIDs case-insensitively.
	x, err := pitrplan.ParseUserSet(req.excludeRaw)
	if err != nil {
		return req, fmt.Errorf("%s: excludeGtids %q: %v", pitrarchive.TokenTargetInvalid, req.excludeRaw, err)
	}
	req.exclude = x
	req.dumpMetaPath = getenv("BLOODRAVEN_PITR_DUMP_META")
	req.outDir = getenv("BLOODRAVEN_PITR_OUTPUT_DIR")
	req.delta = pitrplan.DefaultClockSkewMargin
	limit, err := pitrMemoryLimit(getenv, pitrCgroupMemoryFiles)
	if err != nil {
		return req, err
	}
	req.memoryLimit = limit
	if v := getenv("BLOODRAVEN_PITR_PLAN_MEMORY_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return req, fmt.Errorf("BLOODRAVEN_PITR_PLAN_MEMORY_BYTES=%q: a positive byte count", v)
		}
		req.memory = n
	} else {
		req.memory = defaultPlanMemory(limit)
	}
	if v := getenv("BLOODRAVEN_PITR_STAGING_LIMIT_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return req, fmt.Errorf("BLOODRAVEN_PITR_STAGING_LIMIT_BYTES=%q: a positive byte count", v)
		}
		req.stagingLimit = n
	}
	req.archive, err = pitrArchiveRequestFromEnv(getenv)
	return req, err
}

// pitrCgroupMemoryFiles are the container's memory limit under cgroup v2
// and v1; tests replace them.
var pitrCgroupMemoryFiles = []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"}

// pitrMemoryLimit is the container's memory limit in bytes, or 0 when none
// is known: BLOODRAVEN_PITR_MEMORY_LIMIT_BYTES (the controller renders the
// container's limits.memory), else the first readable cgroup file. cgroup
// v2's "max" and v1's page-rounded 2^63 mean unlimited.
func pitrMemoryLimit(getenv func(string) string, cgroupFiles []string) (int64, error) {
	if v := getenv("BLOODRAVEN_PITR_MEMORY_LIMIT_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("BLOODRAVEN_PITR_MEMORY_LIMIT_BYTES=%q: a positive byte count", v)
		}
		return n, nil
	}
	for _, path := range cgroupFiles {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil || n <= 0 || n >= 1<<62 {
			return 0, nil // "max", or v1's unlimited
		}
		return n, nil
	}
	return 0, nil
}

// defaultPlanMemory is 60% of the memory limit, or 512 MiB when there is
// none (D-42).
func defaultPlanMemory(limit int64) int64 {
	if limit <= 0 {
		return pitrplan.DefaultPlanMemoryBytes
	}
	return limit / 10 * 6
}

// pitrSoftMemoryLimit is the Go soft memory limit pitr-download sets: 90%
// of the container's limit, or -1 (leave it) when the limit is unknown or
// GOMEMLIMIT is set. With the budget at 60% of the limit and the planner's
// live heap within 1.4× of its charges (pitrplan's TestBudgetBoundsLiveHeap),
// the live heap stays below the soft limit, which makes the GC collect
// instead of letting garbage grow the heap toward the kernel's limit.
func pitrSoftMemoryLimit(limit int64, gomemlimit string) int64 {
	if limit <= 0 || gomemlimit != "" {
		return -1
	}
	return limit / 10 * 9
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

// readDumpMetaFile reads dump.json strictly.
func readDumpMetaFile(path string) (backupmeta.DumpMeta, error) {
	var d backupmeta.DumpMeta
	b, err := os.ReadFile(path)
	if err != nil {
		return d, fmt.Errorf("%s: read %s: %w (dump-meta must run first)", pitrarchive.TokenBackupIdentityUnknown, path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return d, fmt.Errorf("%s: %s: %v", pitrarchive.TokenBackupIdentityUnknown, path, err)
	}
	if d.Version != backupmeta.DumpMetaVersion {
		return d, fmt.Errorf("%s: %s has version %d, want %d", pitrarchive.TokenBackupIdentityUnknown, path, d.Version, backupmeta.DumpMetaVersion)
	}
	if err := d.Stamp.Validate(); err != nil {
		return d, fmt.Errorf("%s: %s: %v", pitrarchive.TokenBackupIdentityUnknown, path, err)
	}
	return d, nil
}

// openedArchive is a verified archive: its descriptor and every manifest.
type openedArchive struct {
	desc      *pitrarchive.Descriptor
	manifests []*sidecar.Manifest
}

// pitrSiteName is the CRD's spec.sites[].name pattern. A manifest naming
// any other site is not one this archive's writers produced, and could
// collide with the staging directory's own entries (.pitr-cache,
// coverage.json; review F125).
var pitrSiteName = regexp.MustCompile(`^[a-z0-9-]{1,253}$`)

// retryMetadata runs one archive-open read with the in-process retry the
// row fetches use (P4): a transient store error is retried with backoff,
// and one that persists ends as a token-less *pitrplan.TransientError, so
// an outage is never recorded as a refusal (review F020). Cancellation
// returns the context's error.
func retryMetadata(ctx context.Context, backoff []time.Duration, what string, transient func(error) bool, read func() error) error {
	for attempt := 0; ; attempt++ {
		err := read()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !transient(err) {
			return err
		}
		if attempt == len(backoff) {
			return &pitrplan.TransientError{Err: fmt.Errorf("%s after %d attempts: %w", what, attempt+1, err)}
		}
		if werr := sleepCtx(ctx, backoff[attempt]); werr != nil {
			return werr
		}
	}
}

// listPermissionDenied reports a listing refused for lack of permission
// (an S3 403 or AccessDenied, a filesystem EACCES): retrying cannot help.
func listPermissionDenied(err error) bool {
	if errors.Is(err, fs.ErrPermission) {
		return true
	}
	var withStatus interface{ HTTPStatusCode() int }
	if errors.As(err, &withStatus) && withStatus.HTTPStatusCode() == http.StatusForbidden {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied"
}

// manifestContentError reports a manifest load failure about the stored
// bytes (a refusal token, undecodable JSON, bytes that fail
// authentication) rather than the store failing to deliver them.
func manifestContentError(err error) bool {
	if _, ok := pitrarchive.ParseToken(err.Error()); ok {
		return true
	}
	var syntax *json.SyntaxError
	var typ *json.UnmarshalTypeError
	return sidecar.IsArchiveIntegrityError(err) || errors.As(err, &syntax) || errors.As(err, &typ)
}

// openPITRArchive opens the archive as a reader (§3.3, §4.2): the
// descriptor, then every site manifest, which must be version 2 and name
// the descriptor's archive, then verified reuse (D-35). Every manifest's
// version and identity is checked before any reset marker, so the
// precedence is the documented one (review F126). Transient read failures
// are retried and end token-less (F020).
func openPITRArchive(ctx context.Context, store sidecar.ArchiveStore, req pitrArchiveRequest, backoff []time.Duration) (*openedArchive, error) {
	var desc *pitrarchive.Descriptor
	err := retryMetadata(ctx, backoff, "read the archive descriptor", func(err error) bool {
		// Refusals carry their token; any other failure is a read of the
		// descriptor or of a manifest while classifying the prefix.
		if _, ok := pitrarchive.TokenOf(err); ok {
			return false
		}
		if _, ok := pitrarchive.ParseToken(err.Error()); ok {
			return false
		}
		return storeErrorTransient(err)
	}, func() error {
		var err error
		desc, err = sidecar.OpenArchiveForRead(ctx, store, req.prefix, req.expect)
		return err
	})
	if err != nil {
		if _, ok := pitrarchive.TokenOf(err); ok {
			return nil, err
		}
		if _, ok := pitrarchive.ParseToken(err.Error()); ok || pitrplan.IsTransient(err) || ctx.Err() != nil {
			return nil, err
		}
		// A missing object or bytes that fail authentication while the
		// descriptor was read.
		return nil, fmt.Errorf("%s: open archive at %s: %w", pitrarchive.TokenArchiveObjectInvalid, req.expect.Destination, err)
	}
	dest := req.expect.Destination.String()
	var keys []string
	err = retryMetadata(ctx, backoff, "list site manifests", func(err error) bool { return !listPermissionDenied(err) }, func() error {
		var err error
		keys, err = store.List(ctx, sidecar.ManifestKeyPrefix(req.prefix))
		return err
	})
	if err != nil {
		if pitrplan.IsTransient(err) || ctx.Err() != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%s: list site manifests under %q: %w; restore needs list permission on the prefix (S3 ListObjectsV2, s3:ListBucket) or a readable archive volume", pitrarchive.TokenArchiveListFailed, req.prefix, err)
	}
	sort.Strings(keys)
	var sites []string
	seen := map[string]bool{}
	for _, key := range keys {
		site := sidecar.SiteFromManifestKey(key)
		// Only "<prefix>/manifest-<site>.json" itself names a site: a
		// nested lookalike (manifest-x/manifest-a.json) is another object
		// (review F124).
		if site == "" || key != sidecar.ManifestKey(req.prefix, site) || seen[site] {
			continue
		}
		if !pitrSiteName.MatchString(site) {
			return nil, fmt.Errorf("%s: archive %s at %s: manifest key %q names an invalid site; site names match %s", pitrarchive.TokenArchiveObjectInvalid, desc.ArchiveID, dest, key, pitrSiteName)
		}
		seen[site] = true
		sites = append(sites, site)
	}
	out := &openedArchive{desc: desc}
	for _, site := range sites {
		key := sidecar.ManifestKey(req.prefix, site)
		var m *sidecar.Manifest
		err := retryMetadata(ctx, backoff, "read manifest "+key, func(err error) bool { return !manifestContentError(err) && storeErrorTransient(err) }, func() error {
			var err error
			m, err = sidecar.LoadManifest(ctx, store, req.prefix, site)
			return err
		})
		if err != nil {
			if _, ok := pitrarchive.ParseToken(err.Error()); ok || pitrplan.IsTransient(err) || ctx.Err() != nil {
				return nil, err
			}
			return nil, fmt.Errorf("%s: archive %s at %s: manifest %s: %w", pitrarchive.TokenArchiveObjectInvalid, desc.ArchiveID, dest, key, err)
		}
		// LoadManifest returns an empty manifest without an archive ID
		// for a missing object (a stored version-2 manifest always has
		// one): it was listed and then removed.
		if m.ArchiveID == "" {
			return nil, &pitrplan.TransientError{Err: fmt.Errorf("manifest %s was listed but is gone; the archive changed while it was read; retry", key)}
		}
		// LoadManifest sets m.Site from the key; the stored "site" field
		// is never trusted (review F127).
		if m.ArchiveID != desc.ArchiveID {
			return nil, fmt.Errorf("%s: archive %s at %s: manifest %s names archive %q; the prefix mixes two archives", pitrarchive.TokenArchiveIdentityMismatch, desc.ArchiveID, dest, key, m.ArchiveID)
		}
		out.manifests = append(out.manifests, m)
	}
	for _, m := range out.manifests {
		if m.Epoch != nil && m.Epoch.ResetReason != "" {
			return nil, fmt.Errorf("%s: archive %s at %s: site %s: %s; start a new archive prefix and take a full backup", pitrarchive.TokenArchiveHistoryReset, desc.ArchiveID, dest, m.Site, m.Epoch.ResetReason)
		}
	}
	return out, nil
}

// checkStoreDestination refuses a store configuration that does not
// address the archive destination the Job names: both are rendered from
// one normalized destination, so a disagreement is a stale or hand-edited
// Job, which could otherwise read one bucket and report another in
// coverage.json (review F123).
func checkStoreDestination(cfg *sidecar.PITRConfig, dest pitrarchive.Destination) error {
	switch dest.Kind {
	case pitrarchive.DestinationS3:
		if cfg.S3 == nil {
			return fmt.Errorf("BLOODRAVEN_PITR_STORAGE_TYPE=%s, but BLOODRAVEN_PITR_DESTINATION %s is S3", cfg.StorageType, dest)
		}
		if cfg.S3.Bucket != dest.Bucket {
			return fmt.Errorf("BLOODRAVEN_PITR_S3_BUCKET %q is not the bucket of BLOODRAVEN_PITR_DESTINATION %s", cfg.S3.Bucket, dest)
		}
		ep, err := pitrarchive.NormalizeEndpoint(cfg.S3.EndpointURL)
		if err != nil {
			return fmt.Errorf("BLOODRAVEN_PITR_S3_ENDPOINT_URL: %w", err)
		}
		if ep != dest.Endpoint {
			return fmt.Errorf("BLOODRAVEN_PITR_S3_ENDPOINT_URL %q is not the endpoint of BLOODRAVEN_PITR_DESTINATION %s", cfg.S3.EndpointURL, dest)
		}
	case pitrarchive.DestinationPVC:
		// The claim is mounted by the Job; the mount path cannot name it.
		if cfg.PVC == nil {
			return fmt.Errorf("BLOODRAVEN_PITR_STORAGE_TYPE=%s, but BLOODRAVEN_PITR_DESTINATION %s is a PVC", cfg.StorageType, dest)
		}
	default:
		return fmt.Errorf("BLOODRAVEN_PITR_DESTINATION %s has an unknown kind", dest)
	}
	if cfg.ManifestPrefix != dest.KeyPrefix {
		return fmt.Errorf("BLOODRAVEN_PITR_MANIFEST_PREFIX %q is not the key prefix %q of %s", cfg.ManifestPrefix, dest.KeyPrefix, dest)
	}
	return nil
}

// cachedRow is a tee-cached download: its file and its authenticated size.
type cachedRow struct {
	path string
	size int64
}

// rowFetcher downloads, tees, authenticates and indexes rows for the
// planner. A row is cached in cacheDir only when the planner says it may
// become a plan file (P2), the node filesystem keeps max(10%, 256 MiB)
// free after it, and the staging emptyDir's sizeLimit (when set) still
// has room for it beside the planner's spill. The copy's allowance is the
// row's size as its manifest row claims it, taken when the download
// starts; the copy is dropped as soon as the bytes received pass it (such
// a row is refused anyway). An uncached plan file is re-downloaded with a
// digest check when staged. The spill takes precedence over the cache:
// the copy being written and the cached rows are dropped to make room for
// it (stagingSpill.reserve), and what the open copy has yet to write
// counts as taken (review R3 finding 3: a row's index spills while the row
// downloads, so both grow at once). Before staging, when the spill is
// gone, cached rows the plan does not stage are deleted, and the staged
// set must fit under the sizeLimit and above the node's floor.
type rowFetcher struct {
	store    sidecar.ArchiveStore
	prefix   string
	entries  map[pitrplan.RowRef]sidecar.ManifestEntry
	budget   *pitrplan.Budget
	cacheDir string
	cached   map[pitrplan.RowRef]cachedRow
	parts    int
	// limit is the staging emptyDir's sizeLimit (0: none); cachedBytes is
	// the cache's usage as the kubelet measures it (whole blocks).
	limit       int64
	cachedBytes int64
	// tee is the copy of the row being downloaded, or nil: a part file
	// that is not yet a cached row.
	tee *cacheTee
	// spill is the planner's spill arena on the same volume, or nil.
	spill *stagingSpill
	// freeAfter reports the free bytes left on dir's filesystem after
	// writing n more, and the floor to keep; tests replace it.
	freeAfter func(dir string, n int64) (free, threshold int64)
	// backoff is the delay before each retry of a transient store error.
	backoff []time.Duration
}

// pitrFetchBackoff bounds the in-process retry of transient store errors
// (P4): 5 retries over 31 s. Restore and verification Jobs have
// backoffLimit 0, so a network blip must not end the Job.
var pitrFetchBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// stagingBlock is the allocation unit the emptyDir's usage is counted in;
// stagingReserve is headroom for directories and coverage.json's
// temporary copy.
const (
	stagingBlock   = 4096
	stagingReserve = 1 << 20
)

func blocks(n int64) int64 { return (n + stagingBlock - 1) / stagingBlock * stagingBlock }

// stagingDir is the staging emptyDir: the cache's parent, where staged
// files land.
func (rf *rowFetcher) stagingDir() string { return filepath.Dir(rf.cacheDir) }

// Fetch implements pitrplan.Fetcher; the planner calls FetchForPlan.
func (rf *rowFetcher) Fetch(ctx context.Context, ref pitrplan.RowRef) (*pitrplan.FileIndex, error) {
	return rf.FetchForPlan(ctx, ref, true)
}

// FetchForPlan implements pitrplan.StagingFetcher. A missing object of a
// row the site's live manifest still lists, or an integrity failure, is
// returned as an ordinary error (the planner's unreadable row); a missing
// object of a row retention has since retired, any other store error after
// its retries, and cancellation end planning without a refusal
// (*pitrplan.TransientError).
func (rf *rowFetcher) FetchForPlan(ctx context.Context, ref pitrplan.RowRef, mayStage bool) (*pitrplan.FileIndex, error) {
	entry, ok := rf.entries[ref]
	if !ok {
		return nil, fmt.Errorf("pitr-download: unknown row %s/%s", ref.Site, ref.Name)
	}
	key, err := sidecar.ArchiveEntryKey(rf.prefix, ref.Site, entry)
	if err != nil {
		return nil, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: fmt.Sprintf("%s/%s: %v", ref.Site, ref.Name, err)}
	}
	for attempt := 0; ; attempt++ {
		partial := rf.budget.Checkpoint()
		idx, transient, err := rf.fetchOnce(ctx, ref, entry, key, mayStage)
		if err == nil {
			partial.Keep()
			return idx, nil
		}
		// A failed attempt's partial index is garbage: its memory charges
		// and its spilled arrays are returned.
		partial.Undo()
		if ctx.Err() != nil {
			// Cancellation is neither damage nor a refusal.
			return nil, &pitrplan.TransientError{Err: fmt.Errorf("read %s: %w", key, ctx.Err())}
		}
		if !transient {
			if sidecar.IsArchiveObjectMissing(err) {
				return nil, rf.missingObject(ctx, ref, key, err)
			}
			return nil, err
		}
		if attempt == len(rf.backoff) {
			return nil, &pitrplan.TransientError{Err: fmt.Errorf("read %s after %d attempts: %w", key, attempt+1, err)}
		}
		if werr := sleepCtx(ctx, rf.backoff[attempt]); werr != nil {
			return nil, &pitrplan.TransientError{Err: werr}
		}
	}
}

// missingObject tells a lost object from a retired row (review F058): the
// planner works from the manifests read when the Job started, and
// retention may have retired a row and deleted its object since. It
// re-reads the site's live manifest; a row it no longer lists (or lists
// as pruned) ends planning as a transient error, to be retried with a
// fresh snapshot, so the planner neither reports the row as damaged nor
// advises quarantining it. A row still listed is the planner's unreadable
// row.
func (rf *rowFetcher) missingObject(ctx context.Context, ref pitrplan.RowRef, key string, err error) error {
	var live *sidecar.Manifest
	rerr := retryMetadata(ctx, rf.backoff, "re-read manifest of site "+ref.Site, func(err error) bool { return !manifestContentError(err) && storeErrorTransient(err) }, func() error {
		var err error
		live, err = sidecar.LoadManifest(ctx, rf.store, rf.prefix, ref.Site)
		return err
	})
	if rerr != nil {
		return &pitrplan.TransientError{Err: fmt.Errorf("%s is missing and the site's live manifest cannot be read to tell whether retention retired it (%v); retry", key, rerr)}
	}
	listed := false
	rows := live.Files
	if ref.State == pitrplan.RowQuarantined {
		rows = quarantinedEntries(live)
	}
	for _, e := range rows {
		if e.Name == ref.Name && e.ID == ref.ID {
			listed = true
			break
		}
	}
	if !listed {
		return &pitrplan.TransientError{Err: fmt.Errorf("%s/%s (id %s) is gone and its site's live manifest no longer lists it: the archive changed during planning (row retired); retry", ref.Site, ref.Name, ref.ID)}
	}
	return err
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// storeErrorTransient classifies a store error: a missing object or bad
// bytes are about the row; everything else (timeouts, throttling, 5xx,
// resets) is transient.
func storeErrorTransient(err error) bool {
	return !sidecar.IsArchiveObjectMissing(err) && !sidecar.IsArchiveIntegrityError(err)
}

// recordingReader remembers the first error, other than EOF, of the
// store's reader, so a transport failure the index pass reports as a
// truncated binlog is still seen as transient.
type recordingReader struct {
	r   io.Reader
	err error
}

func (rr *recordingReader) Read(p []byte) (int, error) {
	n, err := rr.r.Read(p)
	if err != nil && err != io.EOF && rr.err == nil {
		rr.err = err
	}
	return n, err
}

// cacheTee copies what it reads into the cache file until a write fails
// or the bytes pass its allowance; then it drops the copy (a full disk
// only costs a re-download).
type cacheTee struct {
	r    io.Reader
	out  *os.File
	part string
	// allow is the staging disk the copy may take, in whole blocks. It is
	// set aside for the copy while it is open (reserved, unwritten).
	allow   int64
	written int64
}

// reserved is the staging disk set aside for the open copy: its whole
// allowance, written or not.
func (t *cacheTee) reserved() int64 {
	if t == nil || t.out == nil {
		return 0
	}
	return t.allow
}

// unwritten is the part of the open copy's allowance it has yet to write:
// what the filesystem's free space does not show yet.
func (t *cacheTee) unwritten() int64 {
	if t == nil || t.out == nil {
		return 0
	}
	return max(t.allow-blocks(t.written), 0)
}

func (t *cacheTee) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 && t.out != nil {
		t.written += int64(n)
		if blocks(t.written) > t.allow {
			t.drop()
		} else if _, werr := t.out.Write(p[:n]); werr != nil {
			t.drop()
		}
	}
	return n, err
}

func (t *cacheTee) drop() {
	if t.out != nil {
		_ = t.out.Close()
		_ = os.Remove(t.part)
		t.out = nil
	}
}

// fetchOnce is one download attempt; transient reports whether a retry
// may succeed.
func (rf *rowFetcher) fetchOnce(ctx context.Context, ref pitrplan.RowRef, entry sidecar.ManifestEntry, key string, mayStage bool) (*pitrplan.FileIndex, bool, error) {
	r, err := rf.store.Open(ctx, key)
	if err != nil {
		return nil, storeErrorTransient(err) && ctx.Err() == nil, fmt.Errorf("open %s: %w", key, err)
	}
	defer r.Close()
	rec := &recordingReader{r: r}
	// The digest of every byte, also when the index pass stops early.
	h := sha256.New()
	tee := &cacheTee{r: io.TeeReader(rec, h)}
	if mayStage {
		if allow := rf.cacheAllowance(entry.Size); allow > 0 {
			rf.parts++
			tee.part = filepath.Join(rf.cacheDir, fmt.Sprintf("%d.part", rf.parts))
			tee.allow = allow
			if tee.out, err = os.OpenFile(tee.part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600); err != nil {
				tee.out = nil
			}
		}
	}
	// The open copy is known to the spill's disk budget until it is a
	// cached row or gone.
	rf.tee = tee
	defer func() {
		rf.tee = nil
		tee.drop()
	}()
	label := ref.Site + "/" + ref.Name
	idx, err := sidecar.InspectTransactions(tee, sidecar.TxIndexOptions{Label: label, Budget: rf.budget})
	refusal, refused := pitrplan.RefusalOf(err)
	if err == nil || refused {
		// Read to EOF so the object's integrity check completes, and so a
		// refusal is reported only for authentic bytes (review F021).
		if _, cerr := io.Copy(io.Discard, tee); err == nil {
			err = cerr
		}
	}
	if rec.err != nil {
		// The store's stream failed; whatever the index pass made of it.
		return nil, storeErrorTransient(rec.err) && ctx.Err() == nil, fmt.Errorf("read %s: %w", key, rec.err)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if (err == nil || refused) && !sidecar.MatchesBinlogDigest(rf.store, entry.ID, digest) {
		return nil, false, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: fmt.Sprintf("%s (id %s): the bytes' digest %s does not match the row's content ID", label, entry.ID, digest)}
	}
	if refused {
		return nil, false, refusal
	}
	if err != nil {
		return nil, false, err
	}
	if idx.Size != entry.Size {
		return nil, false, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: fmt.Sprintf("%s (id %s): the manifest row says %d bytes, its authenticated bytes are %d", label, entry.ID, entry.Size, idx.Size)}
	}
	if tee.out != nil {
		out := tee.out
		tee.out = nil
		if cerr := out.Close(); cerr != nil {
			_ = os.Remove(tee.part)
		} else {
			rf.cached[ref] = cachedRow{path: tee.part, size: idx.Size}
			rf.cachedBytes += blocks(idx.Size)
		}
	}
	return idx, false, nil
}

// cacheRoom is the staging disk a new copy may take: what the emptyDir's
// sizeLimit leaves beside the cache, the spill and the reserve, and what
// the node's filesystem has above its floor.
func (rf *rowFetcher) cacheRoom() int64 {
	room := int64(math.MaxInt64)
	if rf.limit > 0 {
		room = rf.limit - rf.cachedBytes - rf.spill.diskBytes() - stagingReserve
	}
	free, threshold := rf.freeAfter(rf.cacheDir, 0)
	return min(room, free-threshold)
}

// cacheAllowance is the tee rule: the bytes a row may cache, or 0 when
// size (the manifest's claim) does not fit the room. The allowance is the
// claimed size in whole blocks and bounds what the tee writes: the copy
// takes no more of the volume than was set aside for it when its download
// started, however long the download and whatever spills meanwhile.
func (rf *rowFetcher) cacheAllowance(size int64) int64 {
	if need := blocks(size); need <= rf.cacheRoom() {
		return need
	}
	return 0
}

// prepareStaging deletes cached rows the plan does not stage, then checks
// that the staged set (plus coverage.json of coverageBytes) fits under the
// emptyDir's sizeLimit and keeps the node filesystem above its floor,
// refusing PITRPlanTooLarge rather than letting the kubelet evict the pod
// (review F022).
func (rf *rowFetcher) prepareStaging(files []pitrplan.PlanFile, coverageBytes int64) error {
	keep := map[pitrplan.RowRef]bool{}
	for _, f := range files {
		keep[f.Row] = true
	}
	for ref, c := range rf.cached {
		if !keep[ref] {
			if err := os.Remove(c.path); err != nil {
				return err
			}
			rf.cachedBytes -= blocks(c.size)
			delete(rf.cached, ref)
		}
	}
	var staged, download int64
	for _, f := range files {
		staged += blocks(f.Size)
		if _, ok := rf.cached[f.Row]; !ok {
			download += blocks(f.Size)
		}
	}
	need := staged + blocks(coverageBytes) + stagingReserve
	if rf.limit > 0 && need > rf.limit {
		return &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
			"the plan stages %d binlog files of %s, which with coverage.json needs %s, more than the staging volume's sizeLimit of %s; raise spec.backup.stagingVolumeSizeLimit, or choose a backup closer to the target",
			len(files), util.HumanBytes(staged), util.HumanBytes(need), util.HumanBytes(rf.limit))}
	}
	if free, threshold := rf.freeAfter(rf.stagingDir(), download+blocks(coverageBytes)+stagingReserve); free < threshold {
		return rf.nodeFloorRefusal(download+blocks(coverageBytes)+stagingReserve, threshold)
	}
	return nil
}

func (rf *rowFetcher) nodeFloorRefusal(n, threshold int64) error {
	return &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
		"staging the plan needs %s more on the node's filesystem, which would leave less than its floor of %s free (max(10%%, 256 MiB), kept clear of the kubelet's eviction threshold); give the Job's node more ephemeral storage, or choose a backup closer to the target",
		util.HumanBytes(n), util.HumanBytes(threshold))}
}

// pitrDiskFreeAfter is the tee rule's free-space check; tests replace it.
var pitrDiskFreeAfter = diskFreeAfter

// diskFreeAfter reports free bytes after writing n, and the threshold.
func diskFreeAfter(dir string, n int64) (int64, int64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 1
	}
	total := int64(st.Blocks) * st.Bsize  //nolint:gosec // block counts fit
	free := int64(st.Bavail)*st.Bsize - n //nolint:gosec
	return free, max(total/10, 256<<20)
}

// stage places one plan file at its staged path: from the tee cache, or
// re-downloaded with a digest check (transient store errors retried),
// after rechecking the node's floor for it.
func (rf *rowFetcher) stage(ctx context.Context, outDir string, f pitrplan.PlanFile) error {
	rel := f.StagedPath()
	dst, err := safeJoinUnderRoot(outDir, filepath.FromSlash(rel))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if c, ok := rf.cached[f.Row]; ok {
		if err := os.Rename(c.path, dst); err != nil {
			return err
		}
		delete(rf.cached, f.Row)
		rf.cachedBytes -= blocks(c.size)
		return os.Chmod(dst, 0o644)
	}
	if free, threshold := rf.freeAfter(outDir, blocks(f.Size)); free < threshold {
		return rf.nodeFloorRefusal(blocks(f.Size), threshold)
	}
	entry := rf.entries[f.Row]
	key, err := sidecar.ArchiveEntryKey(rf.prefix, f.Row.Site, entry)
	if err != nil {
		return fmt.Errorf("%s: %s/%s: %v", pitrarchive.TokenArchiveObjectInvalid, f.Row.Site, f.Row.Name, err)
	}
	for attempt := 0; ; attempt++ {
		transient, err := rf.stageOnce(ctx, key, dst, f)
		if err == nil || !transient {
			return err
		}
		if attempt == len(rf.backoff) {
			return &pitrplan.TransientError{Err: fmt.Errorf("stage %s/%s after %d attempts: %w", f.Row.Site, f.Row.Name, attempt+1, err)}
		}
		if werr := sleepCtx(ctx, rf.backoff[attempt]); werr != nil {
			return werr
		}
	}
}

// stageOnce is one staging download. Cancellation returns the context's
// error, never a refusal (review F023).
func (rf *rowFetcher) stageOnce(ctx context.Context, key, dst string, f pitrplan.PlanFile) (bool, error) {
	r, err := rf.store.Open(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if storeErrorTransient(err) {
			return true, fmt.Errorf("open %s: %w", key, err)
		}
		return false, fmt.Errorf("%s: %s/%s became unreadable while staging: %v", pitrarchive.TokenArchiveObjectInvalid, f.Row.Site, f.Row.Name, err)
	}
	defer r.Close()
	tmp := dst + ".part"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	rec := &recordingReader{r: r}
	h := sha256.New()
	_, cerr := io.Copy(io.MultiWriter(out, h), rec)
	if err := out.Close(); cerr == nil {
		cerr = err
	}
	if rec.err != nil {
		_ = os.Remove(tmp)
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if storeErrorTransient(rec.err) {
			return true, fmt.Errorf("read %s: %w", key, rec.err)
		}
		return false, fmt.Errorf("%s: %s/%s became unreadable during staging (%v)", pitrarchive.TokenArchiveObjectInvalid, f.Row.Site, f.Row.Name, rec.err)
	}
	if cerr != nil {
		_ = os.Remove(tmp)
		return false, cerr // a local write failure
	}
	if hex.EncodeToString(h.Sum(nil)) != f.Digest {
		_ = os.Remove(tmp)
		return false, fmt.Errorf("%s: %s/%s changed during staging; the archive changed during planning", pitrarchive.TokenArchiveObjectInvalid, f.Row.Site, f.Row.Name)
	}
	return false, os.Rename(tmp, dst)
}

// planResult is what planAndStage returns.
type planResult struct {
	plan     *pitrplan.Plan
	coverage pitrplan.Coverage
}

// manifestRowBytes is the budget charge for a manifest row pitr-download
// holds besides PlanSite's conversion: the decoded entry, its copy in the
// fetcher's entries map with the map's slot, and its strings (review F211).
var manifestRowBytes = 2*int64(unsafe.Sizeof(sidecar.ManifestEntry{})) + int64(unsafe.Sizeof(pitrplan.RowRef{})) + 64

func manifestEntryBytes(e sidecar.ManifestEntry) int64 {
	n := manifestRowBytes + int64(len(e.Name)+len(e.ID)+len(e.BaseGTIDs)+len(e.PreviousGTIDs))
	if c := e.GTIDCoverage; c != nil {
		n += int64(unsafe.Sizeof(*c)) + int64(len(c.GTIDs)+len(c.EmptyGTIDs)+len(c.ContentID))
	}
	return n
}

// planAndStage runs the planner and leaves the staging directory holding
// exactly coverage.json and, on success, the plan's files.
func planAndStage(ctx context.Context, store sidecar.ArchiveStore, req pitrDownloadRequest, logger *slog.Logger) (*planResult, error) {
	if err := os.MkdirAll(req.outDir, 0o755); err != nil {
		return nil, err
	}
	if err := clearStaging(req.outDir); err != nil {
		return nil, err
	}
	res := &planResult{}
	dump, err := readDumpMetaFile(req.dumpMetaPath)
	cov := pitrplan.Coverage{Mode: req.mode, AllowPartial: req.allowPartial, Excluded: req.exclude.String()}
	if req.mode == pitrplan.ModeTimestamp {
		cov.StopTime = backupmeta.NewMicroTime(req.stop)
	}
	if err != nil {
		return res, writeRefusal(req.outDir, cov, err)
	}
	cov.Dump = coverageDump(dump)
	cov.Baseline = dump.GTIDExecuted
	expect := req.archive
	expect.expect.SourceUID = dump.Stamp.Source.UID
	expect.expect.ArchiveID = dump.Stamp.PITRArchiveID
	opened, err := openPITRArchive(ctx, store, expect, pitrFetchBackoff)
	if err != nil {
		return res, writeRefusal(req.outDir, cov, err)
	}
	cov.Archive = pitrplan.CoverageArchive{ArchiveID: opened.desc.ArchiveID, Destination: req.archive.expect.Destination.String(),
		Source: opened.desc.Source, EncryptionMode: opened.desc.Encryption.Mode}

	// The budget exists before the manifests are converted: their rows
	// and parsed sets are charged as they are built (D-42, review F211).
	budget := pitrplan.NewBudget(req.memory)
	rf := &rowFetcher{store: store, prefix: req.archive.prefix, budget: budget, cached: map[pitrplan.RowRef]cachedRow{},
		cacheDir: filepath.Join(req.outDir, ".pitr-cache"), freeAfter: pitrDiskFreeAfter, limit: req.stagingLimit, backoff: pitrFetchBackoff}
	// The spill is closed on every way out: its files are unlinked from
	// the start, and nothing of it is left under the staging directory.
	rf.spill = openStagingSpill(req, rf, budget)
	defer rf.spill.close()
	var archive pitrplan.Archive
	entries := map[pitrplan.RowRef]sidecar.ManifestEntry{}
	for _, m := range opened.manifests {
		for _, e := range append(append([]sidecar.ManifestEntry(nil), m.Files...), quarantinedEntries(m)...) {
			if !isSafeBasename(e.Name) {
				return res, writeRefusal(req.outDir, cov, fmt.Errorf("%s: site %s lists an unsafe binlog name %q", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name))
			}
			if err := budget.Charge(manifestEntryBytes(e), "manifest rows of site "+m.Site); err != nil {
				return res, writeRefusal(req.outDir, cov, err)
			}
		}
		site, err := sidecar.PlanSiteCharged(m, budget)
		if err != nil {
			return res, writeRefusal(req.outDir, cov, err)
		}
		for i, row := range site.Rows {
			var e sidecar.ManifestEntry
			if i < len(m.Files) {
				e = m.Files[i]
			} else {
				e = m.Quarantined[i-len(m.Files)].Entry
			}
			entries[pitrplan.RowRef{Site: m.Site, Name: row.Name, ID: row.ID, State: row.State}] = e
		}
		archive.Sites = append(archive.Sites, site)
	}
	cacheDir := rf.cacheDir
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return res, err
	}
	defer os.RemoveAll(cacheDir)
	rf.entries = entries
	var plan *pitrplan.Plan
	err = rf.spill.guard(func() error {
		var err error
		plan, err = pitrplan.Run(ctx, pitrplan.Request{Mode: req.mode, Stop: req.stop, AllowPartial: req.allowPartial, Exclude: req.exclude,
			Delta: req.delta, Dump: dump, Budget: budget}, archive, rf)
		return err
	})
	// The plan holds no spilled memory. A plan, or a refusal, computed
	// over spilled data the volume lost is not trusted: it ends the Job
	// as an error, to be retried.
	spilled := rf.spill.stats()
	if serr := rf.spill.finish(); serr != nil && ctx.Err() == nil {
		if _, refused := pitrplan.RefusalOf(err); err == nil || refused {
			err = serr
		}
	}
	res.plan = plan
	if err != nil {
		if _, ok := pitrplan.RefusalOf(err); !ok && ctx.Err() == nil {
			// A read error that is not a refusal (a transient store
			// error after its retries): keep it retryable and say what
			// failed.
			err = fmt.Errorf("pitr plan: %w", err)
		}
		if r, ok := pitrplan.RefusalOf(err); ok && r.Token == pitrarchive.TokenArchiveDivergentHistory && plan != nil && !plan.Divergent.IsEmpty() {
			fmt.Printf("BLOODRAVEN_PITR_DIVERGENT_SET gtids=%s\n", boundedText(plan.Divergent.Subtract(plan.Excluded).String(), 64<<10))
		}
		c := cov
		if plan != nil {
			c = plan.Coverage(cov.Archive, cov.Dump, err.Error())
			// A refusal before the planner reads the dump (target or
			// request checks) leaves its baseline empty; the dump's is
			// known (review F131).
			if c.Baseline == "" {
				c.Baseline = cov.Baseline
			}
		}
		_ = os.RemoveAll(cacheDir)
		return res, writeRefusal(req.outDir, c, err)
	}
	res.coverage = plan.Coverage(cov.Archive, cov.Dump, "")
	covBytes, err := res.coverage.Marshal()
	if err != nil {
		return res, err
	}
	if err := rf.prepareStaging(plan.Files, int64(len(covBytes))); err != nil {
		_ = os.RemoveAll(cacheDir)
		return res, writeRefusal(req.outDir, plan.Coverage(cov.Archive, cov.Dump, err.Error()), err)
	}
	for _, f := range plan.Files {
		if err := rf.stage(ctx, req.outDir, f); err != nil {
			_ = os.RemoveAll(cacheDir)
			_ = clearStaging(req.outDir)
			return res, writeRefusal(req.outDir, plan.Coverage(cov.Archive, cov.Dump, err.Error()), err)
		}
	}
	if err := os.RemoveAll(cacheDir); err != nil {
		return res, err
	}
	if err := writeCoverage(req.outDir, res.coverage); err != nil {
		return res, err
	}
	witness, groundTruth := "", ""
	if plan.Witness != nil {
		witness, groundTruth = plan.Witness.GTID.String(), string(plan.Witness.GroundTruth)
	}
	logger.Info("pitr plan", "archiveID", opened.desc.ArchiveID, "mode", string(plan.Mode), "outcome", string(plan.Outcome),
		"witness", witness, "groundTruth", groundTruth, "segments", len(plan.Segments), "files", len(plan.Files),
		"sites", strings.Join(plan.Sites, ","), "replayed", plan.ReplayedTransactions(), "divergent", plan.Divergent.Count(),
		"onHistoryExcluded", plan.OnHistoryExclusions.Count(), "downloaded", plan.Downloaded, "indexedTransactions", plan.IndexedTransactions,
		"memoryPeakBytes", budget.Peak(), "spilledPeakBytes", budget.SpillPeak(), "spillDiskPeakBytes", spilled.PeakBytes)
	if !plan.Divergent.IsEmpty() {
		fmt.Printf("BLOODRAVEN_PITR_DIVERGENT_SET gtids=%s\n", boundedText(plan.Divergent.String(), 64<<10))
	}
	return res, nil
}

func quarantinedEntries(m *sidecar.Manifest) []sidecar.ManifestEntry {
	out := make([]sidecar.ManifestEntry, 0, len(m.Quarantined))
	for _, q := range m.Quarantined {
		out = append(out, q.Entry)
	}
	return out
}

func coverageDump(d backupmeta.DumpMeta) pitrplan.CoverageDump {
	return pitrplan.CoverageDump{Location: d.Location, BackupUID: d.Stamp.Backup.UID, SourceUID: d.Stamp.Source.UID,
		PITRArchiveID: d.Stamp.PITRArchiveID, Site: d.Stamp.Site, ServerID: d.Stamp.ServerID, End: d.End}
}

func boundedText(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}

// clearStaging removes everything under the staging directory.
func clearStaging(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// writeRefusal clears the staging directory, records the refusal in
// coverage.json and returns err.
func writeRefusal(dir string, c pitrplan.Coverage, err error) error {
	_ = clearStaging(dir)
	c.RefusalReason = err.Error()
	c.Outcome, c.Files, c.Segments = "", nil, nil
	if werr := writeCoverage(dir, c); werr != nil {
		return errors.Join(err, werr)
	}
	return err
}

// writeCoverage writes coverage.json atomically. The main container may run
// as another UID; the emptyDir is pod-private.
func writeCoverage(dir string, c pitrplan.Coverage) error {
	data, err := c.Marshal()
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, pitrplan.CoverageFileName+".part")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, pitrplan.CoverageFileName))
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
// the kubelet's 4096-byte termination message limit. pitr-download,
// dump-meta, backup-stamp and backup-encrypt-upload share it, so the pointer
// to the full error names no one container.
func compactTerminationMessage(message string) string {
	const limit = 3072
	if len(message) <= limit {
		return message
	}
	return message[:limit] + " ... (truncated; see the container log)"
}
