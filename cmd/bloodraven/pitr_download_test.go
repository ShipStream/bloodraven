package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	ktypes "k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

const downloadPrefix = "orders/binlogs"

func downloadDest(t *testing.T) pitrarchive.Destination {
	t.Helper()
	d, err := pitrarchive.NewPVCDestination("ns", "backups", "orders")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func putObject(t *testing.T, store sidecar.ArchiveStore, key string, b []byte) {
	t.Helper()
	if err := store.Put(context.Background(), key, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
}

func putDescriptor(t *testing.T, store sidecar.ArchiveStore, dest pitrarchive.Destination) *pitrarchive.Descriptor {
	t.Helper()
	d, err := pitrarchive.NewDescriptor(pitrarchive.Source{Namespace: "ns", Name: "orders", UID: "orders-uid"}, dest,
		pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}, nil, "dc1", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	putObject(t, store, pitrarchive.DescriptorKey(downloadPrefix), b)
	return d
}

func putManifest(t *testing.T, store sidecar.ArchiveStore, m sidecar.Manifest) {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	putObject(t, store, sidecar.ManifestKey(downloadPrefix, m.Site), b)
}

// pitr-download opens the archive as a reader and checks its identity;
// every identity failure refuses with its own token first (§4.11 step 1–2).
func TestOpenPITRArchive(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, store sidecar.ArchiveStore, dest pitrarchive.Destination)
		enc   pitrarchive.EncryptionIdentity
		uid   string
		want  pitrarchive.Token
	}{
		{name: "verified v2 archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			id := putDescriptor(t, s, d).ArchiveID
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: id, Site: "dc1"})
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: id, Site: "dc2"})
		}},
		{name: "empty prefix", setup: func(*testing.T, sidecar.ArchiveStore, pitrarchive.Destination) {}, want: pitrarchive.TokenArchiveNotFound},
		{name: "legacy v1 manifests", setup: func(t *testing.T, s sidecar.ArchiveStore, _ pitrarchive.Destination) {
			putObject(t, s, sidecar.ManifestKey(downloadPrefix, "dc1"), []byte(`{"version":1,"site":"dc1","files":[]}`))
		}, want: pitrarchive.TokenArchiveLegacy},
		{name: "descriptor missing", setup: func(t *testing.T, s sidecar.ArchiveStore, _ pitrarchive.Destination) {
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa", Site: "dc1"})
		}, want: pitrarchive.TokenArchiveDescriptorMissing},
		{name: "encryption mismatch", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			putDescriptor(t, s, d)
		}, enc: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}, want: pitrarchive.TokenArchiveEncryptionMismatch},
		{name: "dump stamp from another group", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			putDescriptor(t, s, d)
		}, uid: "other-uid", want: pitrarchive.TokenArchiveIdentityMismatch},
		{name: "manifest of another archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			putDescriptor(t, s, d)
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: "ar1-bbbbbbbbbbbbbbbbbbbbbbbbbb", Site: "dc1"})
		}, want: pitrarchive.TokenArchiveIdentityMismatch},
		{name: "legacy manifest in a v2 archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			putDescriptor(t, s, d)
			putObject(t, s, sidecar.ManifestKey(downloadPrefix, "dc1"), []byte(`{"version":1,"site":"dc1","files":[]}`))
		}, want: pitrarchive.TokenArchiveLegacy},
		{name: "verified reuse refuses the whole archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			id := putDescriptor(t, s, d).ArchiveID
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: id, Site: "dc1", Epoch: &sidecar.ArchiveEpoch{ResetReason: "GTID reuse verified"}})
		}, want: pitrarchive.TokenArchiveHistoryReset},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := stampTestStore(t)
			dest := downloadDest(t)
			tt.setup(t, store, dest)
			enc := tt.enc
			if enc.Mode == "" {
				enc.Mode = pitrarchive.EncryptionNone
			}
			opened, err := openPITRArchive(context.Background(), store, pitrArchiveRequest{
				prefix: downloadPrefix,
				expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: dest, Encryption: enc, SourceUID: ktypes.UID(tt.uid)},
			})
			if tt.want == "" {
				if err != nil || len(opened.manifests) != 2 {
					t.Fatalf("open = %+v, %v", opened, err)
				}
				return
			}
			if tok, ok := pitrarchive.ParseToken(errString(err)); !ok || tok != tt.want {
				t.Fatalf("err = %v, want token %s", err, tt.want)
			}
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestPITRArchiveRequestFromEnv(t *testing.T) {
	dest, err := pitrarchive.NewS3Destination("b", "orders", "")
	if err != nil {
		t.Fatal(err)
	}
	env := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	base := map[string]string{
		"BLOODRAVEN_PITR_DESTINATION":     dest.String(),
		"BLOODRAVEN_PITR_MANIFEST_PREFIX": dest.KeyPrefix,
		"BLOODRAVEN_PITR_ENCRYPTION_MODE": "None",
	}
	req, err := pitrArchiveRequestFromEnv(env(base))
	if err != nil || req.prefix != "orders/binlogs" || req.expect.Destination != dest || req.expect.Role != pitrarchive.RoleReader {
		t.Fatalf("request %+v, err %v", req, err)
	}
	for name, mutate := range map[string]func(map[string]string){
		"prefix disagrees with the destination": func(m map[string]string) { m["BLOODRAVEN_PITR_MANIFEST_PREFIX"] = "/orders/binlogs" },
		"non-canonical destination":             func(m map[string]string) { m["BLOODRAVEN_PITR_DESTINATION"] = "s3://b/orders/binlogs" },
		"unknown mode":                          func(m map[string]string) { m["BLOODRAVEN_PITR_ENCRYPTION_MODE"] = "Kms" },
		"passphrase mode without a key":         func(m map[string]string) { m["BLOODRAVEN_PITR_ENCRYPTION_MODE"] = "Passphrase" },
		"key without passphrase mode":           func(m map[string]string) { m["BLOODRAVEN_PITR_PASSPHRASE_FILE"] = "/k" },
	} {
		kv := map[string]string{}
		for k, v := range base {
			kv[k] = v
		}
		mutate(kv)
		if _, err := pitrArchiveRequestFromEnv(env(kv)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	base["BLOODRAVEN_PITR_ENCRYPTION_MODE"], base["BLOODRAVEN_PITR_PASSPHRASE_FILE"] = "Passphrase", "/k"
	if req, err := pitrArchiveRequestFromEnv(env(base)); err != nil || req.expect.Encryption.Algorithm != pitrarchive.AlgorithmAES256GCM {
		t.Fatalf("passphrase request %+v, %v", req, err)
	}
}

func TestPITRDownloadRequestFromEnv(t *testing.T) {
	dest := downloadDest(t)
	base := map[string]string{
		"BLOODRAVEN_PITR_MODE": "timestamp", "BLOODRAVEN_PITR_STOP_DATETIME": "2026-11-04T20:01:02.123456Z",
		"BLOODRAVEN_PITR_ALLOW_PARTIAL": "false", "BLOODRAVEN_PITR_DUMP_META": "/pitr-dump-meta/dump.json", "BLOODRAVEN_PITR_OUTPUT_DIR": "/pitr",
		"BLOODRAVEN_PITR_DESTINATION": dest.String(), "BLOODRAVEN_PITR_MANIFEST_PREFIX": dest.KeyPrefix, "BLOODRAVEN_PITR_ENCRYPTION_MODE": "None",
		"BLOODRAVEN_PITR_PLAN_MEMORY_BYTES": "1048576",
	}
	env := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	req, err := pitrDownloadRequestFromEnv(env(base))
	if err != nil || req.mode != pitrplan.ModeTimestamp || req.stop.UnixMicro()%1e6 != 123456 || req.memory != 1<<20 || req.delta != 5*time.Second {
		t.Fatalf("request %+v, %v", req, err)
	}
	// X is user input: MySQL reads its UUIDs case-insensitively.
	upper := map[string]string{"BLOODRAVEN_PITR_EXCLUDE_GTIDS": "3E11FA47-71CA-11E1-9E33-C80AA9429562:2,3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}
	for k, v := range base {
		if _, ok := upper[k]; !ok {
			upper[k] = v
		}
	}
	if req, err := pitrDownloadRequestFromEnv(env(upper)); err != nil || req.exclude.String() != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-2" {
		t.Fatalf("upper-case excludeGtids = %q, %v", req.exclude.String(), err)
	}
	for name, kv := range map[string]map[string]string{
		"non-canonical stop":     {"BLOODRAVEN_PITR_STOP_DATETIME": "2026-11-04 20:01:02"},
		"seven digits":           {"BLOODRAVEN_PITR_STOP_DATETIME": "2026-11-04T20:01:02.1234567Z"},
		"latest with a stop":     {"BLOODRAVEN_PITR_MODE": "latest"},
		"unknown mode":           {"BLOODRAVEN_PITR_MODE": "head"},
		"bad excludeGtids":       {"BLOODRAVEN_PITR_EXCLUDE_GTIDS": "nope"},
		"bad allowPartial":       {"BLOODRAVEN_PITR_ALLOW_PARTIAL": "yes"},
		"bad clock margin":       {"BLOODRAVEN_PITR_CLOCK_SKEW_MARGIN": "-1s"},
		"missing dump meta path": {"BLOODRAVEN_PITR_DUMP_META": ""},
	} {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range kv {
			m[k] = v
		}
		if _, err := pitrDownloadRequestFromEnv(env(m)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("1000000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := defaultPlanMemory(filepath.Join(dir, "memory.max")); got != 600000000 {
		t.Errorf("60%% of memory.max = %d", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "unlimited"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := defaultPlanMemory(filepath.Join(dir, "unlimited")); got != pitrplan.DefaultPlanMemoryBytes {
		t.Errorf("unlimited cgroup budget = %d", got)
	}
}

// ---- end-to-end: the mixed-file failback layout (§4.15 Example 3) ----

const (
	sidA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	sidB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)

var dlEpoch = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

// dlTxns builds sid:from-to, one second apart from start, with origin srv.
func dlTxns(sid string, from, to int64, start time.Time, srv uint32) []binlogtest.Txn {
	var out []binlogtest.Txn
	for n := from; n <= to; n++ {
		out = append(out, binlogtest.Txn{SID: sid, GNO: n, Commit: start.Add(time.Duration(n-from) * time.Second), ServerID: srv})
	}
	return out
}

type dlArchive struct {
	store     sidecar.ArchiveStore
	desc      *pitrarchive.Descriptor
	manifests map[string]*sidecar.Manifest
	dest      pitrarchive.Destination
	tA, tB    func(n int64) time.Time
}

func (a *dlArchive) put(t *testing.T, site, name string, fsrv uint32, prev, base string, txns ...[]binlogtest.Txn) sidecar.ManifestEntry {
	t.Helper()
	var all []binlogtest.Txn
	for _, tx := range txns {
		all = append(all, tx...)
	}
	data := binlogtest.File{ServerID: fsrv, Previous: prev, Txns: all}.MustBytes()
	ev, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	e := sidecar.ManifestEntry{Name: name, ID: hex.EncodeToString(sum[:]), BaseGTIDs: base, PreviousGTIDs: ev.Metadata.PreviousGTIDs,
		GTIDCoverage: ev.Metadata.GTIDCoverage, Size: int64(len(data))}
	key, err := sidecar.ArchiveEntryKey(downloadPrefix, site, e)
	if err != nil {
		t.Fatal(err)
	}
	putObject(t, a.store, key, data)
	m := a.manifests[site]
	m.Files = append(m.Files, e)
	return e
}

func (a *dlArchive) save(t *testing.T) {
	t.Helper()
	for _, m := range a.manifests {
		putManifest(t, a.store, *m)
	}
}

// failbackArchive is Example 3: a2 = [A:71-100 origin, Bu:1-50 replica,
// A:101-110 origin] after A→B→A without reclone; b's era file is never
// archived.
func failbackArchive(t *testing.T) *dlArchive {
	t.Helper()
	a := &dlArchive{store: stampTestStore(t), dest: downloadDest(t), manifests: map[string]*sidecar.Manifest{}}
	a.desc = putDescriptor(t, a.store, a.dest)
	for _, s := range []string{"a", "b"} {
		a.manifests[s] = &sidecar.Manifest{Version: 2, ArchiveID: a.desc.ArchiveID, Site: s}
	}
	a.tA = func(n int64) time.Time {
		if n <= 100 {
			return dlEpoch.Add(time.Duration(n) * time.Second)
		}
		return dlEpoch.Add(time.Duration(200+n) * time.Second)
	}
	a.tB = func(n int64) time.Time { return dlEpoch.Add(time.Duration(110+n) * time.Second) }
	a.manifests["a"].AppendPromotionRecord(pitrarchive.PromotionRecord{ServerUUID: sidA, ServerID: 101, ObservedAt: dlEpoch})
	a.manifests["b"].AppendPromotionRecord(pitrarchive.PromotionRecord{ServerUUID: sidB, ServerID: 102, GTIDExecuted: sidA + ":1-100", ObservedAt: a.tA(100).Add(2 * time.Second)})
	a.manifests["a"].AppendPromotionRecord(pitrarchive.PromotionRecord{ServerUUID: sidA, ServerID: 101, GTIDExecuted: sidA + ":1-100," + sidB + ":1-50", ObservedAt: a.tB(50).Add(2 * time.Second)})
	a.put(t, "a", "mysql-bin.000001", 101, "", "", dlTxns(sidA, 1, 30, a.tA(1), 101))
	a.put(t, "a", "mysql-bin.000002", 101, sidA+":1-30", "", dlTxns(sidA, 31, 70, a.tA(31), 101))
	a.put(t, "b", "mysql-bin.000001", 102, "", sidA+":1-10", dlTxns(sidA, 11, 60, a.tA(11), 101))
	a.put(t, "a", "mysql-bin.000003", 101, sidA+":1-70", "", dlTxns(sidA, 71, 100, a.tA(71), 101), dlTxns(sidB, 1, 50, a.tB(1), 102), dlTxns(sidA, 101, 110, a.tA(101), 101))
	a.save(t)
	return a
}

func writeDumpJSON(t *testing.T, dir string, b string, end time.Time, a *dlArchive) string {
	t.Helper()
	created := backupmeta.NewMicroTime(end.Add(-time.Second))
	meta := backupmeta.DumpMeta{Version: 1, Location: "dumps/nightly", GTIDExecuted: b, End: backupmeta.NewMicroTime(end),
		ServerClock: backupmeta.ServerClock{Start: created, End: backupmeta.NewMicroTime(end)},
		Stamp: backupmeta.Stamp{Kind: backupmeta.StampKind, Version: backupmeta.StampVersion,
			Backup: backupmeta.ObjectRef{Namespace: "ns", Name: "nightly", UID: "backup-uid"}, Source: backupmeta.ObjectRef{Namespace: "ns", Name: "orders", UID: a.desc.Source.UID},
			PITRArchiveID: a.desc.ArchiveID, Site: "b", ServerID: 102, ServerClock: backupmeta.ServerClock{Start: created, End: backupmeta.NewMicroTime(end)}, CreatedAt: created}}
	path := filepath.Join(dir, "dump.json")
	if err := writeDumpMeta(dir, meta); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(filepath.Dir(path), dumpMetaJSONName)
}

func stagedFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if rel != pitrplan.CoverageFileName {
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func readCoverage(t *testing.T, dir string) pitrplan.Coverage {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, pitrplan.CoverageFileName))
	if err != nil {
		t.Fatal(err)
	}
	var c pitrplan.Coverage
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPlanAndStageMixedFileFailback(t *testing.T) {
	for _, cache := range []bool{true, false} {
		name := map[bool]string{true: "tee cached", false: "re-downloaded"}[cache]
		t.Run(name, func(t *testing.T) {
			a := failbackArchive(t)
			old := pitrDiskFreeAfter
			defer func() { pitrDiskFreeAfter = old }()
			pitrDiskFreeAfter = func(string, int64) (int64, int64) {
				if cache {
					return 1, 0
				}
				return 0, 1
			}
			out, metaDir := t.TempDir(), t.TempDir()
			// Leftovers from an earlier attempt are cleared.
			if err := os.WriteFile(filepath.Join(out, "stale.part"), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			stop := a.tB(36).Add(-500 * time.Millisecond)
			req := pitrDownloadRequest{
				archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
					Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
				mode: pitrplan.ModeTimestamp, stop: stop, exclude: pitrplan.Set{}, outDir: out, delta: 5 * time.Second, memory: 64 << 20,
				dumpMetaPath: writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a),
			}
			res, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			c := readCoverage(t, out)
			if c.Version != 2 || c.Outcome != pitrplan.OutcomeTargetCovered || c.RefusalReason != "" || c.Witness == nil || c.Witness.GroundTruth != pitrplan.GroundTruthAnchored ||
				c.Witness.GTID != sidB+":36" || c.Lineage != sidA+":1-100,"+sidB+":1-35" || c.Replay != sidA+":41-100,"+sidB+":1-35" || c.Archive.ArchiveID != a.desc.ArchiveID ||
				c.Dump.SourceUID != a.desc.Source.UID || c.Dump.ServerID != 102 || c.StopTime.String() != backupmeta.NewMicroTime(stop).String() {
				t.Fatalf("coverage = %+v", c)
			}
			if len(c.Segments) != 2 || c.Files[c.Segments[0].File].Name != "mysql-bin.000002" || c.Segments[0].GTIDs != sidA+":41-70" ||
				c.Files[c.Segments[1].File].Name != "mysql-bin.000003" || c.Segments[1].GTIDs != sidA+":71-100,"+sidB+":1-35" ||
				c.Segments[1].StartPredecessors != sidA+":1-70" || c.Segments[1].Transactions != 65 || c.ReplayedTransactionCount != 95 {
				t.Fatalf("segments = %+v files = %+v", c.Segments, c.Files)
			}
			// The staged files are exactly coverage.json's files, with
			// matching digests; no cache or .part file is left.
			var want []string
			for _, f := range c.Files {
				want = append(want, f.Path)
				b, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(f.Path)))
				if err != nil {
					t.Fatal(err)
				}
				if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != f.SHA256 {
					t.Errorf("%s digest mismatch", f.Path)
				}
			}
			sort.Strings(want)
			if got := stagedFiles(t, out); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("staged %v, coverage lists %v", got, want)
			}
			// Download set: the frontier (a's second file, then b's
			// era-1 file, whose hmax is above maxOct(B)) and the witness
			// row; the closure needs nothing else, and a's first file
			// (A:1-30) overlaps no used file.
			if res.plan.Downloaded != 3 {
				t.Errorf("downloaded %d rows, want 3", res.plan.Downloaded)
			}
		})
	}
}

func TestPlanAndStageRefusalWritesCoverage(t *testing.T) {
	a := failbackArchive(t)
	out, metaDir := t.TempDir(), t.TempDir()
	req := pitrDownloadRequest{
		archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
		mode: pitrplan.ModeTimestamp, stop: a.tA(120), exclude: pitrplan.Set{}, outDir: out, delta: 5 * time.Second, memory: 64 << 20,
		dumpMetaPath: writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a),
	}
	_, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if tok, ok := pitrarchive.ParseToken(errString(err)); !ok || tok != pitrarchive.TokenTargetNotCovered {
		t.Fatalf("err = %v, want PITRTargetNotCovered", err)
	}
	c := readCoverage(t, out)
	if !strings.HasPrefix(c.RefusalReason, "PITRTargetNotCovered: ") || c.Outcome != "" || len(c.Files) != 0 || len(c.Segments) != 0 || c.Baseline != sidA+":1-40" {
		t.Fatalf("refusal coverage = %+v", c)
	}
	if got := stagedFiles(t, out); len(got) != 0 {
		t.Fatalf("refusal left files %v", got)
	}
	// A dump stamped for another archive is an identity mismatch.
	a.desc.ArchiveID = "ar1-bbbbbbbbbbbbbbbbbbbbbbbbbb"
	req.dumpMetaPath = writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a)
	_, err = planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenArchiveIdentityMismatch {
		t.Fatalf("err = %v, want PITRArchiveIdentityMismatch", err)
	}
	// A tampered object refuses ObjectInvalid.
	a = failbackArchive(t)
	e := a.manifests["a"].Files[2]
	key, _ := sidecar.ArchiveEntryKey(downloadPrefix, "a", e)
	data := binlogtest.File{ServerID: 101, Previous: sidA + ":1-70", Txns: dlTxns(sidA, 71, 72, a.tA(71), 101)}.MustBytes()
	putObject(t, a.store, key, data)
	req.dumpMetaPath = writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a)
	req.stop = a.tA(80)
	_, err = planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenArchiveObjectInvalid {
		t.Fatalf("err = %v, want PITRArchiveObjectInvalid", err)
	}
}
