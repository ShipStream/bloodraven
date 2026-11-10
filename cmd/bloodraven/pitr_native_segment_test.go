package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// Native mixed-file failback fixtures (internal/sidecar/testdata/native/
// failback, captured from two MySQL 9.7 servers by `capture.sh failback`):
// A→B and B→A planned role changes without reclone and without rotation at
// either change, binlog_transaction_compression=ON, tagged GTIDs. a's
// mysql-bin.000003 holds [a's era-1 tail, b's era 2 as replica copies, a's
// era 3]; b's own file is never archived (Example 3's layout).
//
// TestPITRNativeFailbackPlans checks every plan against the captured
// primaries' gtid_executed at each target without Docker, and that every
// segment's start offset is a GTID event of its file (issue #271).
// TestPITRNativeSegmentReplay (BLOODRAVEN_PITR_NATIVE_TEST=1) replays each
// plan with the mysqlsh image's mysqlbinlog (--start-position and
// --include-gtids per segment, as restore.py runs it) into mysql:9.7 and
// compares the rows and gtid_executed with what the source servers held.
//
// The effect-digest capture (internal/sidecar/testdata/native/effect, a
// source and its replica) is planned by TestPITRNativeEffectPlans, and
// captured afresh and checked again by TestPITRNativeEffectCapture
// (BLOODRAVEN_PITR_NATIVE_TEST=1), issue #268.

const nativeFailbackDir = "../../internal/sidecar/testdata/native/failback"

type nativeTarget struct {
	label, executed, rows, rowsSkip string
	stop                            time.Time
}

type nativeFailback struct {
	targets map[string]nativeTarget
	skip    pitrplan.Set
	records []struct {
		site string
		rec  pitrarchive.PromotionRecord
	}
	files map[string][]byte // a's binlogs by name
}

func readTSV(t *testing.T, name string) [][]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(nativeFailbackDir, name))
	if err != nil {
		t.Fatal(err)
	}
	var out [][]string
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		out = append(out, strings.Split(line, "\t"))
	}
	return out
}

func loadNativeFailback(t *testing.T) *nativeFailback {
	t.Helper()
	nf := &nativeFailback{targets: map[string]nativeTarget{}, files: map[string][]byte{}}
	for _, f := range readTSV(t, "targets.tsv") {
		nt := nativeTarget{label: f[0], executed: f[1], rows: f[2], rowsSkip: f[3]}
		if f[4] != "-" {
			mt, err := backupmeta.ParseMicroTime(f[4])
			if err != nil {
				t.Fatal(err)
			}
			nt.stop = mt.Time
		}
		nf.targets[f[0]] = nt
	}
	skip := readTSV(t, "skip.tsv")[0]
	nf.skip = pitrplan.MustParseSet(skip[2]).Subtract(pitrplan.MustParseSet(skip[1]))
	for _, f := range readTSV(t, "records.tsv") {
		at, err := backupmeta.ParseMicroTime(f[4])
		if err != nil {
			t.Fatal(err)
		}
		var id uint32
		fmt.Sscan(f[2], &id)
		nf.records = append(nf.records, struct {
			site string
			rec  pitrarchive.PromotionRecord
		}{f[0], pitrarchive.PromotionRecord{ServerUUID: f[1], ServerID: id, GTIDExecuted: f[3], ObservedAt: at.Time}})
	}
	for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"} {
		b, err := os.ReadFile(filepath.Join(nativeFailbackDir, "a-"+name+".bin"))
		if err != nil {
			t.Fatal(err)
		}
		nf.files[name] = b
	}
	return nf
}

// archive stores a's three files and both sites' promotion records; the
// dump is mysql-bin.000001's history (B), taken on a. With through > 0 the
// dump was taken later: B also holds the transactions of mysql-bin.000002
// up to a's GNO through, so the plan starts inside that file.
func (nf *nativeFailback) archive(t *testing.T, through int64) (*dlArchive, string, time.Time) {
	t.Helper()
	a := &dlArchive{store: stampTestStore(t), dest: downloadDest(t), manifests: map[string]*sidecar.Manifest{}}
	a.desc = putDescriptor(t, a.store, a.dest)
	for _, s := range []string{"a", "b"} {
		a.manifests[s] = &sidecar.Manifest{Version: 2, ArchiveID: a.desc.ArchiveID, Site: s}
	}
	for _, r := range nf.records {
		a.manifests[r.site].AppendPromotionRecord(r.rec)
	}
	var b string
	var end time.Time
	for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"} {
		data := nf.files[name]
		ev, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if !ev.Metadata.GTIDCoverage.Complete {
			t.Fatalf("%s: coverage incomplete", name)
		}
		// Coverage bound to the row's content ID, as the archiver
		// uploads it (PlanSite trusts nothing else).
		cov := *ev.Metadata.GTIDCoverage
		cov.ContentID = ev.Digest
		e := sidecar.ManifestEntry{Name: name, ID: ev.Digest, PreviousGTIDs: ev.Metadata.PreviousGTIDs, GTIDCoverage: &cov, Size: int64(len(data))}
		key, err := sidecar.ArchiveEntryKey(downloadPrefix, "a", e)
		if err != nil {
			t.Fatal(err)
		}
		putObject(t, a.store, key, data)
		a.manifests["a"].Files = append(a.manifests["a"].Files, e)
		if name == "mysql-bin.000001" {
			b = pitrplan.MustParseSet(e.PreviousGTIDs).Union(pitrplan.MustParseSet(ev.Metadata.GTIDCoverage.GTIDs)).String()
			end = time.UnixMicro(ev.Metadata.GTIDCoverage.OriginalCommitMax + 1000).UTC()
		}
		if name == "mysql-bin.000002" && through > 0 {
			idx := indexNative(t, name, data)
			set := pitrplan.MustParseSet(b)
			for i, tx := range idx.Txns {
				if g := idx.GTID(i); !strings.Contains(g.SID, ":") && g.GNO <= through {
					set = set.Union(pitrplan.SetOf(g))
					end = time.UnixMicro(tx.OCT + 1000).UTC()
				}
			}
			b = set.String()
		}
	}
	a.save(t)
	return a, b, end
}

func indexNative(t *testing.T, label string, data []byte) *pitrplan.FileIndex {
	t.Helper()
	idx, err := sidecar.InspectTransactions(bytes.NewReader(data), sidecar.TxIndexOptions{Label: label})
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	return idx
}

type nativeCase struct {
	name    string
	target  string
	exclude bool
	latest  bool
	truth   pitrplan.GroundTruth
	// through extends the backup into mysql-bin.000002 (archive).
	through int64
}

var nativeCases = []nativeCase{
	{name: "era 1 (origin)", target: "t1", truth: pitrplan.GroundTruthOrigin},
	{name: "b's era (anchored by b's promotion record)", target: "t2", truth: pitrplan.GroundTruthAnchored},
	{name: "b's era, X inside a segment", target: "t2", exclude: true, truth: pitrplan.GroundTruthAnchored},
	{name: "era 3 after the failback", target: "t3", truth: pitrplan.GroundTruthOrigin},
	{name: "era 3, X inside a segment", target: "t3", exclude: true, truth: pitrplan.GroundTruthOrigin},
	{name: "latest", target: "latest", latest: true},
	// The dump holds mysql-bin.000002's first three transactions (a:5-7),
	// so the first segment starts inside that file, at the tagged
	// transaction a:fixture:1.
	{name: "a segment that starts mid-file at a tagged GTID", target: "t3", through: 7, truth: pitrplan.GroundTruthOrigin},
}

func (nf *nativeFailback) plan(t *testing.T, c nativeCase, memory int64) (*planResult, string, error) {
	t.Helper()
	a, b, end := nf.archive(t, c.through)
	tgt := nf.targets[c.target]
	out, metaDir := t.TempDir(), t.TempDir()
	meta := backupmeta.DumpMeta{Version: 1, Location: "dumps/native", GTIDExecuted: b, End: backupmeta.NewMicroTime(end),
		ServerClock: backupmeta.ServerClock{Start: backupmeta.NewMicroTime(end), End: backupmeta.NewMicroTime(end)},
		Stamp: backupmeta.Stamp{Kind: backupmeta.StampKind, Version: backupmeta.StampVersion,
			Backup: backupmeta.ObjectRef{Namespace: "ns", Name: "native", UID: "backup-uid"}, Source: backupmeta.ObjectRef{Namespace: "ns", Name: "orders", UID: a.desc.Source.UID},
			PITRArchiveID: a.desc.ArchiveID, Site: "a", ServerID: 101, ServerClock: backupmeta.ServerClock{Start: backupmeta.NewMicroTime(end), End: backupmeta.NewMicroTime(end)},
			CreatedAt: backupmeta.NewMicroTime(end)}}
	if err := writeDumpMeta(metaDir, meta); err != nil {
		t.Fatal(err)
	}
	req := pitrDownloadRequest{
		archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
		mode: pitrplan.ModeTimestamp, stop: tgt.stop, exclude: pitrplan.Set{}, outDir: out, delta: 5 * time.Second, memory: memory,
		dumpMetaPath: filepath.Join(metaDir, dumpMetaJSONName),
	}
	if c.latest {
		req.mode, req.stop = pitrplan.ModeLatest, time.Time{}
	}
	if c.exclude {
		req.exclude = nf.skip
	}
	res, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return res, out, err
}

func TestPITRNativeFailbackPlans(t *testing.T) {
	nf := loadNativeFailback(t)
	// The fixtures carry what the test claims: tagged GTIDs and compressed
	// transaction payloads in the mixed file.
	idx, err := sidecar.InspectTransactions(bytes.NewReader(nf.files["mysql-bin.000003"]), sidecar.TxIndexOptions{Label: "a/mysql-bin.000003"})
	if err != nil {
		t.Fatal(err)
	}
	tagged, replica := false, false
	for i, tx := range idx.Txns {
		tagged = tagged || strings.HasSuffix(idx.GTID(i).SID, ":fixture")
		replica = replica || tx.Srv != idx.WriterServerID
	}
	if !tagged || !replica || countEvents(nf.files["mysql-bin.000003"], 40) < len(idx.Txns) {
		t.Fatalf("fixture lacks tagged GTIDs (%v), replica copies (%v) or compressed payloads", tagged, replica)
	}
	// What the segments' start offsets cover across the cases: a start
	// inside a file, one at a compressed transaction and one at a tagged
	// GTID.
	midFile, compressed, taggedStart := 0, 0, 0
	for _, c := range nativeCases {
		t.Run(c.name, func(t *testing.T) {
			res, _, err := nf.plan(t, c, 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			for k, seg := range res.coverage.Segments {
				f := res.coverage.Files[seg.File]
				data := nf.files[f.Name]
				idx := indexNative(t, f.Name, data)
				include := pitrplan.MustParseSet(seg.GTIDs)
				want := int64(-1)
				for i, tx := range idx.Txns {
					if include.Has(idx.GTID(i)) {
						want = tx.Pos
						break
					}
				}
				pos := seg.StartPosition
				if pos != want || pos != res.plan.Segments[k].StartPosition {
					t.Fatalf("segment %d of %s starts at offset %d; its first replayed transaction is at %d", k, f.Name, pos, want)
				}
				kind := data[pos+4]
				if kind != 33 && kind != 42 {
					t.Fatalf("segment %d of %s: event type %d at offset %d is no GTID event", k, f.Name, kind, pos)
				}
				if pos > idx.Txns[0].Pos {
					midFile++
					// The event after the GTID event.
					if next := pos + int64(binary.LittleEndian.Uint32(data[pos+9:])); data[next+4] == 40 {
						compressed++
					}
					if kind == 42 {
						taggedStart++
					}
				}
			}
			want := pitrplan.MustParseSet(nf.targets[c.target].executed)
			if c.exclude {
				want = want.Subtract(nf.skip)
			}
			got := res.plan.Lineage.Subtract(res.plan.Excluded)
			if !got.Equal(want) {
				t.Fatalf("Λ ∖ X = %s, the source held %s", got, want)
			}
			if !c.latest && res.plan.Witness.GroundTruth != c.truth {
				t.Errorf("ground truth %s, want %s", res.plan.Witness.GroundTruth, c.truth)
			}
			if c.exclude && res.plan.OnHistoryExclusions.IsEmpty() {
				t.Errorf("an excluded transaction inside a segment is an on-history exclusion")
			}
		})
	}
	if midFile == 0 || compressed == 0 || taggedStart == 0 {
		t.Fatalf("segments starting mid-file: %d, at a compressed transaction: %d, at a tagged GTID: %d; want each", midFile, compressed, taggedStart)
	}
	// The decoder bound: a budget whose MaxEventBytes (32 KiB) is below
	// the big payload's uncompressed size (60211 bytes) refuses before
	// zstd runs; the small payloads before it, whose decode workspace is
	// charged too, fit.
	_, _, err = nf.plan(t, nativeCases[3], 256<<10)
	if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenPlanTooLarge || !strings.Contains(errString(err), "compressed transaction payload") {
		t.Fatalf("small budget: %v, want PITRPlanTooLarge", err)
	}
}

func countEvents(data []byte, kind byte) int {
	n := 0
	for pos := 4; pos+19 <= len(data); {
		if data[pos+4] == kind {
			n++
		}
		pos += int(binary.LittleEndian.Uint32(data[pos+9:]))
	}
	return n
}

// replayScript runs in mysql:9.7: load the backup (mysql-bin.000001
// replayed whole, and the part of mysql-bin.000002 a later dump holds),
// check gtid_executed = B, then replay every segment into one mysql session
// as restore.py does (--start-position at the segment's first transaction,
// --include-gtids), and print the result.
const replayScript = `set -euo pipefail
mysqld --no-defaults --user=root --initialize-insecure --datadir=/tmp/d >/tmp/init.log 2>&1
mysqld --no-defaults --user=root --datadir=/tmp/d --socket=/tmp/s.sock --skip-networking --mysqlx=OFF --server-id=900 --log-bin=/tmp/d/bin --gtid-mode=ON --enforce-gtid-consistency=ON --log-error=/tmp/err &
for i in $(seq 1 90); do mysqladmin --socket=/tmp/s.sock -uroot ping --silent 2>/dev/null && break; sleep 1; done
q() { mysql --socket=/tmp/s.sock -uroot --batch --skip-column-names -e "$1"; }
/usr/libexec/mysqlsh/mysqlbinlog --no-defaults /work/baseline.bin | mysql --socket=/tmp/s.sock -uroot --binary-mode
if [ -s /work/baseline-extra.txt ]; then /usr/libexec/mysqlsh/mysqlbinlog --no-defaults --include-gtids="$(cat /work/baseline-extra.txt)" /work/baseline2.bin | mysql --socket=/tmp/s.sock -uroot --binary-mode; fi
echo "BASELINE $(q "SELECT REPLACE(@@global.gtid_executed, CHAR(10), '')")"
( while IFS=$'\t' read -r gtids start path; do /usr/libexec/mysqlsh/mysqlbinlog --no-defaults --include-gtids="$gtids" --start-position="$start" "/stage/$path"; done </work/segments.tsv ) | mysql --socket=/tmp/s.sock -uroot --binary-mode
echo "EXECUTED $(q "SELECT REPLACE(@@global.gtid_executed, CHAR(10), '')")"
echo "ROWS $(q "SELECT CONCAT(COUNT(*), '/', COALESCE(SUM(CRC32(CONCAT_WS('|', id, tag, site, SHA2(payload, 256)))), 0)) FROM replay.markers")"
`

func TestPITRNativeSegmentReplay(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("set BLOODRAVEN_PITR_NATIVE_TEST=1 (needs docker and the mysql:9.7 image)")
	}
	nf := loadNativeFailback(t)
	for _, c := range nativeCases {
		t.Run(c.name, func(t *testing.T) {
			res, stage, err := nf.plan(t, c, 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			work := t.TempDir()
			if err := os.WriteFile(filepath.Join(work, "baseline.bin"), nf.files["mysql-bin.000001"], 0o644); err != nil {
				t.Fatal(err)
			}
			// The part of mysql-bin.000002 a later dump already holds.
			extra := res.plan.Baseline.Intersect(indexNative(t, "mysql-bin.000002", nf.files["mysql-bin.000002"]).GTIDs)
			if (c.through > 0) == extra.IsEmpty() {
				t.Fatalf("the baseline holds %s of mysql-bin.000002", extra)
			}
			for name, data := range map[string][]byte{"baseline2.bin": nf.files["mysql-bin.000002"], "baseline-extra.txt": []byte(extra.String())} {
				if err := os.WriteFile(filepath.Join(work, name), data, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			var segs strings.Builder
			for _, s := range res.coverage.Segments {
				fmt.Fprintf(&segs, "%s\t%d\t%s\n", s.GTIDs, s.StartPosition, res.coverage.Files[s.File].Path)
			}
			if err := os.WriteFile(filepath.Join(work, "segments.tsv"), []byte(segs.String()), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(work, "replay.sh"), []byte(replayScript), 0o644); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "none", "-e", "TZ=UTC",
				"-v", work+":/work:ro", "-v", stage+":/stage:ro", "--entrypoint", "bash", "mysql:9.7", "/work/replay.sh")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("replay: %v\n%s", err, out)
			}
			fields := map[string]string{}
			for _, line := range strings.Split(string(out), "\n") {
				if k, v, ok := strings.Cut(line, " "); ok && (k == "BASELINE" || k == "EXECUTED" || k == "ROWS") {
					fields[k] = v
				}
			}
			if !pitrplan.MustParseSet(fields["BASELINE"]).Equal(res.plan.Baseline) {
				t.Fatalf("loaded baseline %q, want B = %s", fields["BASELINE"], res.plan.Baseline)
			}
			tgt := nf.targets[c.target]
			want, rows := pitrplan.MustParseSet(tgt.executed), tgt.rows
			if c.exclude {
				want, rows = want.Subtract(nf.skip), tgt.rowsSkip
			}
			if got := pitrplan.MustParseSet(fields["EXECUTED"]); !got.Equal(want) {
				t.Errorf("gtid_executed %s, want %s", got, want)
			}
			if fields["ROWS"] != rows {
				t.Errorf("rows %q, the source held %q\n%s", fields["ROWS"], rows, out)
			}
		})
	}
}

// The effect-digest capture (issue #268): a source s and its replica r
// (log_replica_updates) hold every transaction as an origin copy and as a
// replica copy, cut into rows events at different sizes and compressed on
// one side only (internal/sidecar/testdata/native/effect, README "Effect-
// digest pairs"). mysql-bin.000001 is the backup's history, 000002 the
// faithful workload, 000003 a transaction r applied over drifted data, and
// 000004 a repair transaction injected on r under s's GTID.

const nativeEffectDir = "../../internal/sidecar/testdata/native/effect"

type effectCapture struct {
	// cases are cases.tsv: label → expectation and GTIDs.
	cases map[string]struct {
		expect string
		gtids  pitrplan.Set
	}
	// phases are phases.tsv: label → s's gtid_executed and clock at the
	// rotation that ended the phase.
	phases map[string]struct {
		executed string
		at       time.Time
	}
	files map[string][]byte // "<site>/<name>"
}

func loadEffectCapture(t *testing.T, dir string) *effectCapture {
	t.Helper()
	c := &effectCapture{files: map[string][]byte{},
		cases: map[string]struct {
			expect string
			gtids  pitrplan.Set
		}{},
		phases: map[string]struct {
			executed string
			at       time.Time
		}{}}
	tsv := func(name string, fields int) [][]string {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		var out [][]string
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			f := strings.Split(line, "\t")
			if len(f) != fields {
				t.Fatalf("%s: line %q", name, line)
			}
			out = append(out, f)
		}
		return out
	}
	for _, f := range tsv("cases.tsv", 3) {
		c.cases[f[0]] = struct {
			expect string
			gtids  pitrplan.Set
		}{f[1], pitrplan.MustParseSet(f[2])}
	}
	for _, f := range tsv("phases.tsv", 3) {
		at, err := backupmeta.ParseMicroTime(f[2])
		if err != nil {
			t.Fatal(err)
		}
		c.phases[f[0]] = struct {
			executed string
			at       time.Time
		}{f[1], at.Time}
	}
	for _, site := range []string{"s", "r"} {
		for n := 1; n <= 4; n++ {
			name := fmt.Sprintf("mysql-bin.%06d", n)
			b, err := os.ReadFile(filepath.Join(dir, site+"-"+name+".bin"))
			if err != nil {
				t.Fatal(err)
			}
			c.files[site+"/"+name] = b
		}
	}
	return c
}

// checkDigests compares the two copies of every transaction with what
// cases.tsv says they must be.
func (c *effectCapture) checkDigests(t *testing.T) {
	t.Helper()
	type copies struct{ s, r *pitrplan.Txn }
	byGTID := map[pitrplan.GTID]*copies{}
	for label, data := range c.files {
		idx := indexNative(t, label, data)
		for i := range idx.Txns {
			g := idx.GTID(i)
			if byGTID[g] == nil {
				byGTID[g] = &copies{}
			}
			if strings.HasPrefix(label, "s/") {
				byGTID[g].s = &idx.Txns[i]
			} else {
				byGTID[g].r = &idx.Txns[i]
			}
		}
	}
	count := map[string]int{}
	for g, cp := range byGTID {
		if cp.s == nil || cp.r == nil {
			t.Fatalf("%s is not in both servers' binlogs", g)
		}
		got := "equal"
		switch {
		case cp.s.Effect.IsZero() || cp.r.Effect.IsZero():
			got = "uncomparable"
		case cp.s.Effect != cp.r.Effect:
			got = "differ"
		}
		count[got]++
		label, want := "", ""
		for name, cs := range c.cases {
			if cs.gtids.Has(g) {
				label, want = name, cs.expect
			}
		}
		// Outside the labelled cases (the replication user's DDL) nothing
		// may differ.
		if (want == "" && got == "differ") || (want != "" && got != want) {
			t.Errorf("%s (%s): the source's and the replica's copies are %s (%x, %x), want %s", g, label, got, cp.s.Effect, cp.r.Effect, want)
		}
		// The drifted copy agrees with the original on everything the
		// planner compared before the digest; the injected one does not.
		same := cp.s.OCT == cp.r.OCT && cp.s.Srv == cp.r.Srv && cp.s.Empty() == cp.r.Empty()
		if same == (label == "injected") {
			t.Errorf("%s (%s): commit time, origin server and emptiness agree: %v", g, label, same)
		}
	}
	if count["equal"] < 30 || count["differ"] != 2 || count["uncomparable"] < 10 {
		t.Fatalf("pairs: %v; want at least 30 equal, 2 that differ (drift, injected) and at least 10 not comparable", count)
	}
	for _, name := range []string{"big-insert", "big-update", "big-delete", "multi-table", "types", "generated", "partitioned", "no-primary-key",
		"cascade", "trigger", "tagged", "source-compressed", "only-source-compressed", "uncompressed-cuts", "drift", "injected", "json", "json-partial", "savepoint"} {
		if _, ok := c.cases[name]; !ok {
			t.Errorf("the capture has no case %q", name)
		}
	}
}

// archive stores the named files of both sites ("s/mysql-bin.000002", …)
// and returns the archive.
func (c *effectCapture) archive(t *testing.T, names ...string) *dlArchive {
	t.Helper()
	a := &dlArchive{store: stampTestStore(t), dest: downloadDest(t), manifests: map[string]*sidecar.Manifest{}}
	a.desc = putDescriptor(t, a.store, a.dest)
	for _, s := range []string{"s", "r"} {
		a.manifests[s] = &sidecar.Manifest{Version: 2, ArchiveID: a.desc.ArchiveID, Site: s}
	}
	for _, label := range names {
		site, name, _ := strings.Cut(label, "/")
		data := c.files[label]
		ev, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if !ev.Metadata.GTIDCoverage.Complete {
			t.Fatalf("%s: coverage incomplete", label)
		}
		cov := *ev.Metadata.GTIDCoverage
		cov.ContentID = ev.Digest
		e := sidecar.ManifestEntry{Name: name, ID: ev.Digest, PreviousGTIDs: ev.Metadata.PreviousGTIDs, GTIDCoverage: &cov, Size: int64(len(data))}
		key, err := sidecar.ArchiveEntryKey(downloadPrefix, site, e)
		if err != nil {
			t.Fatal(err)
		}
		putObject(t, a.store, key, data)
		a.manifests[site].Files = append(a.manifests[site].Files, e)
	}
	a.save(t)
	return a
}

// plan replays to the archive head from a dump s took at the end of phase.
func (c *effectCapture) plan(t *testing.T, phase string, names ...string) (*planResult, error) {
	t.Helper()
	a := c.archive(t, names...)
	p, ok := c.phases[phase]
	if !ok {
		t.Fatalf("no phase %q", phase)
	}
	end := backupmeta.NewMicroTime(p.at)
	out, metaDir := t.TempDir(), t.TempDir()
	meta := backupmeta.DumpMeta{Version: 1, Location: "dumps/effect", GTIDExecuted: p.executed, End: end,
		ServerClock: backupmeta.ServerClock{Start: end, End: end},
		Stamp: backupmeta.Stamp{Kind: backupmeta.StampKind, Version: backupmeta.StampVersion,
			Backup: backupmeta.ObjectRef{Namespace: "ns", Name: "effect", UID: "backup-uid"}, Source: backupmeta.ObjectRef{Namespace: "ns", Name: "orders", UID: a.desc.Source.UID},
			PITRArchiveID: a.desc.ArchiveID, Site: "s", ServerID: 101, ServerClock: backupmeta.ServerClock{Start: end, End: end}, CreatedAt: end}}
	if err := writeDumpMeta(metaDir, meta); err != nil {
		t.Fatal(err)
	}
	req := pitrDownloadRequest{
		archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
		mode: pitrplan.ModeLatest, exclude: pitrplan.Set{}, outDir: out, delta: 5 * time.Second, memory: 64 << 20,
		dumpMetaPath: filepath.Join(metaDir, dumpMetaJSONName),
	}
	return planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// checkPlans runs the planner over the capture: faithful copies plan, and
// a copy with other row changes refuses.
func (c *effectCapture) checkPlans(t *testing.T) {
	t.Helper()
	one := func(name string) string {
		set := c.cases[name].gtids
		if set.Count() != 1 {
			t.Fatalf("case %s holds %s", name, set)
		}
		return set.String()
	}
	// Both sites' copies of the faithful workload are downloaded (one is
	// replayed, the closure compares the other) and the plan is certified:
	// no comparable pair differs.
	res, err := c.plan(t, "setup", "s/mysql-bin.000001", "r/mysql-bin.000001", "s/mysql-bin.000002", "r/mysql-bin.000002")
	if err != nil {
		t.Fatalf("faithful copies: %v", err)
	}
	if want := pitrplan.MustParseSet(c.phases["faithful"].executed); !res.plan.Lineage.Equal(want) || res.plan.Downloaded < 3 {
		t.Fatalf("faithful copies: Λ = %s after %d downloads, want %s with both copies of mysql-bin.000002 read", res.plan.Lineage, res.plan.Downloaded, want)
	}
	// r applied the drift transaction over other data: the two copies agree
	// on GTID, commit time, origin server and emptiness, and only their row
	// changes differ.
	_, err = c.plan(t, "setup", "s/mysql-bin.000001", "r/mysql-bin.000001", "s/mysql-bin.000002", "r/mysql-bin.000002", "s/mysql-bin.000003", "r/mysql-bin.000003")
	msg := errString(err)
	if tok, _ := pitrarchive.ParseToken(msg); tok != pitrarchive.TokenArchiveNamespaceReuse ||
		!strings.Contains(msg, one("drift")+" differs between ") || !strings.Contains(msg, "/mysql-bin.000003 (row changes differ: effect digest ") ||
		!strings.Contains(msg, "injected under this GTID") || strings.Contains(msg, "commit time") || strings.Contains(msg, "origin server") {
		t.Fatalf("drifted copy: %v, want PITRArchiveNamespaceReuse on row changes alone", err)
	}
	// The repair injected on r under s's GTID differs in its GTID event
	// too. The dump was taken after the drift and r's drifted file is gone,
	// so this is the only pair that disagrees.
	_, err = c.plan(t, "drift", "s/mysql-bin.000001", "r/mysql-bin.000001", "s/mysql-bin.000002", "r/mysql-bin.000002", "s/mysql-bin.000003",
		"s/mysql-bin.000004", "r/mysql-bin.000004")
	msg = errString(err)
	if tok, _ := pitrarchive.ParseToken(msg); tok != pitrarchive.TokenArchiveNamespaceReuse ||
		!strings.Contains(msg, one("injected")+" differs between ") || !strings.Contains(msg, "commit time") || !(strings.Contains(msg, "origin server 101 vs 102") || strings.Contains(msg, "origin server 102 vs 101")) ||
		!strings.Contains(msg, "row changes differ: effect digest ") {
		t.Fatalf("injected repair: %v, want PITRArchiveNamespaceReuse naming commit time, origin server and row changes", err)
	}
}

func TestPITRNativeEffectPlans(t *testing.T) {
	c := loadEffectCapture(t, nativeEffectDir)
	c.checkDigests(t)
	c.checkPlans(t)
}

// TestPITRNativeEffectCapture captures a source and replica pair afresh
// with the fixture's own script (two MySQL 9.7 servers in one network-less
// container) and checks the new capture like the committed one, so the
// canonical form is tested against the server image CI pulls, not only
// against the bytes captured once.
func TestPITRNativeEffectCapture(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("set BLOODRAVEN_PITR_NATIVE_TEST=1 (needs docker and the mysql:9.7 image)")
	}
	script, err := os.Open(filepath.Join(nativeEffectDir, "capture-inner.sh"))
	if err != nil {
		t.Fatal(err)
	}
	defer script.Close()
	// The container's root writes the capture; it makes the files readable.
	out := t.TempDir()
	if err := os.Chmod(out, 0o777); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i", "--network", "none", "-v", out+":/fixtures", "--entrypoint", "bash", "mysql:9.7", "-s")
	cmd.Stdin = script
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("capture: %v\n%s", err, b)
	}
	c := loadEffectCapture(t, out)
	c.checkDigests(t)
	c.checkPlans(t)
}
