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
// primaries' gtid_executed at each target without Docker.
// TestPITRNativeSegmentReplay (BLOODRAVEN_PITR_NATIVE_TEST=1) replays each
// plan with the mysqlsh image's mysqlbinlog into mysql:9.7 and compares the
// rows and gtid_executed with what the source servers held.

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
// dump is mysql-bin.000001's history (B), taken on a.
func (nf *nativeFailback) archive(t *testing.T) (*dlArchive, string, time.Time) {
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
	}
	a.save(t)
	return a, b, end
}

type nativeCase struct {
	name    string
	target  string
	exclude bool
	latest  bool
	truth   pitrplan.GroundTruth
}

var nativeCases = []nativeCase{
	{name: "era 1 (origin)", target: "t1", truth: pitrplan.GroundTruthOrigin},
	{name: "b's era (anchored by b's promotion record)", target: "t2", truth: pitrplan.GroundTruthAnchored},
	{name: "b's era, X inside a segment", target: "t2", exclude: true, truth: pitrplan.GroundTruthAnchored},
	{name: "era 3 after the failback", target: "t3", truth: pitrplan.GroundTruthOrigin},
	{name: "era 3, X inside a segment", target: "t3", exclude: true, truth: pitrplan.GroundTruthOrigin},
	{name: "latest", target: "latest", latest: true},
}

func (nf *nativeFailback) plan(t *testing.T, c nativeCase, memory int64) (*planResult, string, error) {
	t.Helper()
	a, b, end := nf.archive(t)
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
	for _, c := range nativeCases {
		t.Run(c.name, func(t *testing.T) {
			res, _, err := nf.plan(t, c, 64<<20)
			if err != nil {
				t.Fatal(err)
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
// replayed whole), check gtid_executed = B, then replay every segment into
// one mysql session as restore.py does, and print the result.
const replayScript = `set -euo pipefail
mysqld --no-defaults --user=root --initialize-insecure --datadir=/tmp/d >/tmp/init.log 2>&1
mysqld --no-defaults --user=root --datadir=/tmp/d --socket=/tmp/s.sock --skip-networking --mysqlx=OFF --server-id=900 --log-bin=/tmp/d/bin --gtid-mode=ON --enforce-gtid-consistency=ON --log-error=/tmp/err &
for i in $(seq 1 90); do mysqladmin --socket=/tmp/s.sock -uroot ping --silent 2>/dev/null && break; sleep 1; done
q() { mysql --socket=/tmp/s.sock -uroot --batch --skip-column-names -e "$1"; }
/usr/libexec/mysqlsh/mysqlbinlog --no-defaults /work/baseline.bin | mysql --socket=/tmp/s.sock -uroot --binary-mode
echo "BASELINE $(q "SELECT REPLACE(@@global.gtid_executed, CHAR(10), '')")"
( while IFS=$'\t' read -r gtids path; do /usr/libexec/mysqlsh/mysqlbinlog --no-defaults --include-gtids="$gtids" "/stage/$path"; done </work/segments.tsv ) | mysql --socket=/tmp/s.sock -uroot --binary-mode
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
			var segs strings.Builder
			for _, s := range res.coverage.Segments {
				fmt.Fprintf(&segs, "%s\t%s\n", s.GTIDs, res.coverage.Files[s.File].Path)
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
