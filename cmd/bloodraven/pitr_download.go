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
//	BLOODRAVEN_PITR_CLOCK_SKEW_MARGIN  δ (Go duration, default 5s; D-50)
//	BLOODRAVEN_PITR_PLAN_MEMORY_BYTES  the planner budget (default 60% of the cgroup's memory.max, or 512 MiB; D-42)
//	plus the S3/PVC storage env pitrDownloadConfigFromEnv reads.
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
	if v := getenv("BLOODRAVEN_PITR_CLOCK_SKEW_MARGIN"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return req, fmt.Errorf("BLOODRAVEN_PITR_CLOCK_SKEW_MARGIN=%q: a non-negative duration", v)
		}
		if d == 0 {
			d = time.Microsecond // 0 would mean "default" to the planner
		}
		req.delta = d
	}
	if v := getenv("BLOODRAVEN_PITR_PLAN_MEMORY_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return req, fmt.Errorf("BLOODRAVEN_PITR_PLAN_MEMORY_BYTES=%q: a positive byte count", v)
		}
		req.memory = n
	} else {
		req.memory = defaultPlanMemory("/sys/fs/cgroup/memory.max")
	}
	req.archive, err = pitrArchiveRequestFromEnv(getenv)
	return req, err
}

// defaultPlanMemory is 60% of the cgroup's memory.max, or 512 MiB when the
// cgroup is unlimited or unreadable (D-42).
func defaultPlanMemory(path string) int64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return pitrplan.DefaultPlanMemoryBytes
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || n <= 0 {
		return pitrplan.DefaultPlanMemoryBytes
	}
	return n / 10 * 6
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
// planner. A row is cached in cacheDir iff the free space afterwards
// stays ≥ max(10%, 256 MiB); an uncached file is re-downloaded with a
// digest check when staged.
type rowFetcher struct {
	store    sidecar.ArchiveStore
	prefix   string
	entries  map[pitrplan.RowRef]sidecar.ManifestEntry
	budget   *pitrplan.Budget
	cacheDir string
	cached   map[pitrplan.RowRef]string
	digests  map[pitrplan.RowRef]string
	parts    int
	// freeAfter reports the free bytes left after writing n more; tests
	// replace it.
	freeAfter func(dir string, n int64) (free, threshold int64)
}

func (rf *rowFetcher) Fetch(ctx context.Context, ref pitrplan.RowRef) (*pitrplan.FileIndex, error) {
	entry, ok := rf.entries[ref]
	if !ok {
		return nil, fmt.Errorf("pitr-download: unknown row %s/%s", ref.Site, ref.Name)
	}
	key, err := sidecar.ArchiveEntryKey(rf.prefix, ref.Site, entry)
	if err != nil {
		return nil, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: fmt.Sprintf("%s/%s: %v", ref.Site, ref.Name, err)}
	}
	r, err := rf.store.Open(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", key, err)
	}
	defer r.Close()
	var src io.Reader = r
	var out *os.File
	var part string
	if free, threshold := rf.freeAfter(rf.cacheDir, entry.Size); free >= threshold {
		rf.parts++
		part = filepath.Join(rf.cacheDir, fmt.Sprintf("%d.part", rf.parts))
		if out, err = os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600); err != nil {
			return nil, err
		}
		src = io.TeeReader(r, out)
	}
	label := ref.Site + "/" + ref.Name
	idx, err := sidecar.InspectTransactions(src, sidecar.TxIndexOptions{Label: label, Budget: rf.budget})
	if err == nil {
		// Read to EOF so the object's integrity check completes.
		_, err = io.Copy(io.Discard, src)
	}
	if out != nil {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		if part != "" {
			_ = os.Remove(part)
		}
		return nil, err
	}
	if !sidecar.MatchesBinlogDigest(rf.store, entry.ID, idx.Digest) {
		if part != "" {
			_ = os.Remove(part)
		}
		return nil, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: fmt.Sprintf("%s (id %s): the bytes' digest %s does not match the row's content ID", label, entry.ID, idx.Digest)}
	}
	rf.digests[ref] = idx.Digest
	if part != "" {
		rf.cached[ref] = part
	}
	return idx, nil
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
// re-downloaded with a digest check.
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
		return os.Chmod(dst, 0o644)
	}
	entry := rf.entries[f.Row]
	key, err := sidecar.ArchiveEntryKey(rf.prefix, f.Row.Site, entry)
	if err != nil {
		return err
	}
	r, err := rf.store.Open(ctx, key)
	if err != nil {
		return fmt.Errorf("%s: %s/%s became unreadable while staging: %v", pitrarchive.TokenArchiveObjectInvalid, f.Row.Site, f.Row.Name, err)
	}
	defer r.Close()
	tmp := dst + ".part"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, cerr := io.Copy(io.MultiWriter(out, h), r)
	if err := out.Close(); cerr == nil {
		cerr = err
	}
	if cerr != nil || hex.EncodeToString(h.Sum(nil)) != f.Digest {
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: %s/%s changed or became unreadable during staging (%v); the archive changed during planning", pitrarchive.TokenArchiveObjectInvalid, f.Row.Site, f.Row.Name, cerr)
	}
	return os.Rename(tmp, dst)
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
		cached: map[pitrplan.RowRef]string{}, digests: map[pitrplan.RowRef]string{}, freeAfter: pitrDiskFreeAfter}
	plan, err := pitrplan.Run(ctx, pitrplan.Request{Mode: req.mode, Stop: req.stop, AllowPartial: req.allowPartial, Exclude: req.exclude,
		Delta: req.delta, Dump: dump, Budget: budget}, archive, rf)
	res.plan = plan
	if err != nil {
		if _, ok := pitrplan.RefusalOf(err); !ok && ctx.Err() == nil {
			// A read error that is not a refusal: keep it retryable and
			// say what failed.
			err = fmt.Errorf("pitr plan: %w", err)
		}
		if r, ok := pitrplan.RefusalOf(err); ok && r.Token == pitrarchive.TokenArchiveDivergentHistory && plan != nil && !plan.Divergent.IsEmpty() {
			fmt.Printf("BLOODRAVEN_PITR_DIVERGENT_SET gtids=%s\n", boundedText(plan.Divergent.Subtract(plan.Excluded).String(), 64<<10))
		}
		c := plan.Coverage(cov.Archive, cov.Dump, err.Error())
		_ = os.RemoveAll(cacheDir)
		return res, writeRefusal(req.outDir, c, err)
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
	res.coverage = plan.Coverage(cov.Archive, cov.Dump, "")
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
