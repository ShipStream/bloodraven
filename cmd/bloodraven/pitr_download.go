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
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

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
// Env (rendered by internal/controller; see contracts.md "WP-B2
// assumptions"):
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

// openPITRArchive opens the archive as a reader (§3.3, §4.2): the
// descriptor, then every site manifest, which must be version 2 and name
// the descriptor's archive, then verified reuse (D-35).
func openPITRArchive(ctx context.Context, store sidecar.ArchiveStore, req pitrArchiveRequest) (*openedArchive, error) {
	desc, err := sidecar.OpenArchiveForRead(ctx, store, req.prefix, req.expect)
	if err != nil {
		if _, ok := pitrarchive.TokenOf(err); ok {
			return nil, err
		}
		if _, ok := pitrarchive.ParseToken(err.Error()); ok {
			return nil, err
		}
		return nil, fmt.Errorf("%s: open archive at %s: %w", pitrarchive.TokenArchiveListFailed, req.expect.Destination, err)
	}
	dest := req.expect.Destination.String()
	keys, err := store.List(ctx, sidecar.ManifestKeyPrefix(req.prefix))
	if err != nil {
		return nil, fmt.Errorf("%s: list site manifests under %q: %w; restore needs list permission on the prefix (S3 ListObjectsV2)", pitrarchive.TokenArchiveListFailed, req.prefix, err)
	}
	sort.Strings(keys)
	out := &openedArchive{desc: desc}
	for _, key := range keys {
		site := sidecar.SiteFromManifestKey(key)
		if site == "" {
			continue
		}
		if !isSafeBasename(site) {
			return nil, fmt.Errorf("%s: archive %s at %s: manifest key %q names an unsafe site", pitrarchive.TokenArchiveObjectInvalid, desc.ArchiveID, dest, key)
		}
		m, err := sidecar.LoadManifest(ctx, store, req.prefix, site)
		if err != nil {
			if _, ok := pitrarchive.ParseToken(err.Error()); ok {
				return nil, err
			}
			return nil, fmt.Errorf("%s: archive %s at %s: manifest %s: %w", pitrarchive.TokenArchiveObjectInvalid, desc.ArchiveID, dest, key, err)
		}
		if m.ArchiveID != desc.ArchiveID {
			return nil, fmt.Errorf("%s: archive %s at %s: manifest %s names archive %q; the prefix mixes two archives", pitrarchive.TokenArchiveIdentityMismatch, desc.ArchiveID, dest, key, m.ArchiveID)
		}
		if m.Epoch != nil && m.Epoch.ResetReason != "" {
			return nil, fmt.Errorf("%s: archive %s at %s: site %s: %s; start a new archive prefix and take a full backup", pitrarchive.TokenArchiveHistoryReset, desc.ArchiveID, dest, site, m.Epoch.ResetReason)
		}
		if m.Site == "" {
			m.Site = site
		}
		if m.Site != site {
			return nil, fmt.Errorf("%s: archive %s at %s: manifest %s is for site %q", pitrarchive.TokenArchiveObjectInvalid, desc.ArchiveID, dest, key, m.Site)
		}
		out.manifests = append(out.manifests, m)
	}
	return out, nil
}

// rowFetcher downloads, tees, authenticates and indexes rows for the
// planner. A row is cached in cacheDir only when the planner says it may
// become a plan file (P2), the node filesystem keeps max(10%, 256 MiB)
// free after it, and the staging emptyDir's sizeLimit (when set) still
// has room for it; an uncached plan file is re-downloaded with a digest
// check when staged. Before staging, cached rows the plan does not stage
// are deleted, and the staged set must fit.
type rowFetcher struct {
	store    sidecar.ArchiveStore
	prefix   string
	entries  map[pitrplan.RowRef]sidecar.ManifestEntry
	budget   *pitrplan.Budget
	cacheDir string
	cached   map[pitrplan.RowRef]string
	digests  map[pitrplan.RowRef]string
	parts    int
	// limit is the staging emptyDir's sizeLimit (0: none); cachedBytes is
	// the cache's usage as the kubelet measures it (whole blocks).
	limit       int64
	cachedBytes int64
	// freeAfter reports the free bytes left after writing n more; tests
	// replace it.
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

// Fetch implements pitrplan.Fetcher; the planner calls FetchForPlan.
func (rf *rowFetcher) Fetch(ctx context.Context, ref pitrplan.RowRef) (*pitrplan.FileIndex, error) {
	return rf.FetchForPlan(ctx, ref, true)
}

// FetchForPlan implements pitrplan.StagingFetcher. A missing object or an
// integrity failure is returned as an ordinary error (the planner's
// unreadable row); any other store error is retried with backoff and then
// returned as a *pitrplan.TransientError, which ends planning without a
// refusal.
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
		used := rf.budget.Used()
		idx, transient, err := rf.fetchOnce(ctx, ref, entry, key, mayStage)
		if err == nil {
			return idx, nil
		}
		// A failed attempt's partial index is garbage.
		rf.budget.Release(rf.budget.Used() - used)
		if !transient {
			return nil, err
		}
		if attempt == len(rf.backoff) {
			return nil, &pitrplan.TransientError{Err: fmt.Errorf("read %s after %d attempts: %w", key, attempt+1, err)}
		}
		if werr := sleepCtx(ctx, rf.backoff[attempt]); werr != nil {
			return nil, werr
		}
	}
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

// cacheTee copies what it reads into the cache file until a write fails;
// then it drops the copy (a full disk only costs a re-download).
type cacheTee struct {
	r    io.Reader
	out  *os.File
	part string
}

func (t *cacheTee) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 && t.out != nil {
		if _, werr := t.out.Write(p[:n]); werr != nil {
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
	tee := &cacheTee{r: rec}
	if mayStage && rf.cacheFits(entry.Size) {
		rf.parts++
		tee.part = filepath.Join(rf.cacheDir, fmt.Sprintf("%d.part", rf.parts))
		if tee.out, err = os.OpenFile(tee.part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600); err != nil {
			tee.out = nil
		}
	}
	label := ref.Site + "/" + ref.Name
	idx, err := sidecar.InspectTransactions(tee, sidecar.TxIndexOptions{Label: label, Budget: rf.budget})
	if err == nil {
		// Read to EOF so the object's integrity check completes.
		_, err = io.Copy(io.Discard, tee)
	}
	if rec.err != nil {
		// The store's stream failed; whatever the index pass made of it.
		tee.drop()
		return nil, storeErrorTransient(rec.err) && ctx.Err() == nil, fmt.Errorf("read %s: %w", key, rec.err)
	}
	if err != nil {
		tee.drop()
		return nil, false, err
	}
	if !sidecar.MatchesBinlogDigest(rf.store, entry.ID, idx.Digest) {
		tee.drop()
		return nil, false, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: fmt.Sprintf("%s (id %s): the bytes' digest %s does not match the row's content ID", label, entry.ID, idx.Digest)}
	}
	rf.digests[ref] = idx.Digest
	if tee.out != nil {
		if cerr := tee.out.Close(); cerr != nil {
			_ = os.Remove(tee.part)
		} else {
			rf.cached[ref] = tee.part
			rf.cachedBytes += blocks(entry.Size)
		}
	}
	return idx, false, nil
}

// cacheFits is the tee rule: the node keeps its floor, and the emptyDir's
// sizeLimit has room for the cache plus this row and the reserve.
func (rf *rowFetcher) cacheFits(size int64) bool {
	if rf.limit > 0 && rf.cachedBytes+blocks(size)+stagingReserve > rf.limit {
		return false
	}
	free, threshold := rf.freeAfter(rf.cacheDir, size)
	return free >= threshold
}

// prepareStaging deletes cached rows the plan does not stage, then checks
// that the staged set (plus coverage.json of coverageBytes) fits under the
// emptyDir's sizeLimit and on the filesystem, refusing PITRPlanTooLarge
// rather than letting the kubelet evict the pod.
func (rf *rowFetcher) prepareStaging(files []pitrplan.PlanFile, coverageBytes int64) error {
	keep := map[pitrplan.RowRef]bool{}
	for _, f := range files {
		keep[f.Row] = true
	}
	for ref, part := range rf.cached {
		if !keep[ref] {
			if err := os.Remove(part); err != nil {
				return err
			}
			rf.cachedBytes -= blocks(rf.entries[ref].Size)
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
	if free, _ := rf.freeAfter(rf.cacheDir, download+blocks(coverageBytes)); free < 0 {
		return &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
			"staging the plan needs %s more on the node's filesystem than it has free; give the Job's node more ephemeral storage, or choose a backup closer to the target",
			util.HumanBytes(download+blocks(coverageBytes)))}
	}
	return nil
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
// re-downloaded with a digest check (transient store errors retried).
func (rf *rowFetcher) stage(ctx context.Context, outDir string, f pitrplan.PlanFile) error {
	rel := f.StagedPath()
	dst, err := safeJoinUnderRoot(outDir, filepath.FromSlash(rel))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if part, ok := rf.cached[f.Row]; ok {
		if err := os.Rename(part, dst); err != nil {
			return err
		}
		delete(rf.cached, f.Row)
		rf.cachedBytes -= blocks(f.Size)
		return os.Chmod(dst, 0o644)
	}
	entry := rf.entries[f.Row]
	key, err := sidecar.ArchiveEntryKey(rf.prefix, f.Row.Site, entry)
	if err != nil {
		return err
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

func (rf *rowFetcher) stageOnce(ctx context.Context, key, dst string, f pitrplan.PlanFile) (bool, error) {
	r, err := rf.store.Open(ctx, key)
	if err != nil {
		if storeErrorTransient(err) && ctx.Err() == nil {
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
		if storeErrorTransient(rec.err) && ctx.Err() == nil {
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
	opened, err := openPITRArchive(ctx, store, expect)
	if err != nil {
		return res, writeRefusal(req.outDir, cov, err)
	}
	cov.Archive = pitrplan.CoverageArchive{ArchiveID: opened.desc.ArchiveID, Destination: req.archive.expect.Destination.String(),
		Source: opened.desc.Source, EncryptionMode: opened.desc.Encryption.Mode}

	var archive pitrplan.Archive
	entries := map[pitrplan.RowRef]sidecar.ManifestEntry{}
	for _, m := range opened.manifests {
		for _, e := range append(append([]sidecar.ManifestEntry(nil), m.Files...), quarantinedEntries(m)...) {
			if !isSafeBasename(e.Name) {
				return res, writeRefusal(req.outDir, cov, fmt.Errorf("%s: site %s lists an unsafe binlog name %q", pitrarchive.TokenArchiveObjectInvalid, m.Site, e.Name))
			}
		}
		site, err := sidecar.PlanSite(m)
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
	budget := pitrplan.NewBudget(req.memory)
	cacheDir := filepath.Join(req.outDir, ".pitr-cache")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return res, err
	}
	defer os.RemoveAll(cacheDir)
	rf := &rowFetcher{store: store, prefix: req.archive.prefix, entries: entries, budget: budget, cacheDir: cacheDir,
		cached: map[pitrplan.RowRef]string{}, digests: map[pitrplan.RowRef]string{}, freeAfter: pitrDiskFreeAfter,
		limit: req.stagingLimit, backoff: pitrFetchBackoff}
	plan, err := pitrplan.Run(ctx, pitrplan.Request{Mode: req.mode, Stop: req.stop, AllowPartial: req.allowPartial, Exclude: req.exclude,
		Delta: req.delta, Dump: dump, Budget: budget}, archive, rf)
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
		c := plan.Coverage(cov.Archive, cov.Dump, err.Error())
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
		"onHistoryExcluded", plan.OnHistoryExclusions.Count(), "downloaded", plan.Downloaded, "indexedTransactions", plan.IndexedTransactions)
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
