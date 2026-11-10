package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// The planner ↔ restore.py seam, end to end: a binlogtest-built two-site
// failback archive (Example 3), the dump's metadata read by dump-meta's
// loadDumpMeta, pitr-download's real glue (env parsing, planAndStage), and
// the embedded restore.py main() over the staged directory with the env
// names the controller renders on the main container. restore.py runs
// under python3 with a fake mysqlsh session whose GTID functions are an
// exact set model, and with mysqlbinlog/mysql PATH shims that record
// their arguments. The server's gtid_executed after the replay is the
// baseline plus every --include-gtids set restore.py actually passed, so
// R6 checks the planner's segments against what was replayed.

// seamPythonPrelude is the GTID model and the fake mysqlsh wiring.
const seamPythonPrelude = `import os, re, types

def gparse(s):
    out = {}
    for member in [m for m in ''.join((s or '').split()).split(',') if m]:
        parts = member.split(':')
        uuid, tag = parts[0].lower(), ''
        for p in parts[1:]:
            m = re.match(r'^(\d+)(?:-(\d+))?$', p)
            if not m:
                tag = p
                continue
            lo, hi = int(m.group(1)), int(m.group(2) or m.group(1))
            out.setdefault((uuid, tag), set()).update(range(lo, hi + 1))
    return {k: v for k, v in out.items() if v}

def gfmt(d):
    out = []
    for (uuid, tag) in sorted(k for k in d if d[k]):
        nums, runs = sorted(d[(uuid, tag)]), []
        for n in nums:
            if runs and runs[-1][1] == n - 1:
                runs[-1][1] = n
            else:
                runs.append([n, n])
        ivs = ':'.join(str(a) if a == b else '%d-%d' % (a, b) for a, b in runs)
        out.append(uuid + (':' + tag if tag else '') + ':' + ivs)
    return ','.join(out)

def gsub(a, b):
    pa, pb = gparse(a), gparse(b)
    return gfmt({k: v - pb.get(k, set()) for k, v in pa.items()})

def gunion(*sets):
    out = {}
    for s in sets:
        for k, v in gparse(s).items():
            out.setdefault(k, set()).update(v)
    return gfmt(out)

class Result:
    def __init__(self, value): self.value = value
    def fetch_one(self): return (self.value,)

class Session:
    executed = ''
    def run_sql(self, sql, args=None):
        args = list(args or [])
        if sql == 'SELECT GTID_SUBTRACT(?, ?)':
            return Result(gsub(args[0], args[1]))
        if sql == 'SELECT GTID_SUBSET(?, ?)':
            return Result(1 if gsub(args[0], args[1]) == '' else 0)
        if sql == 'SELECT @@global.gtid_executed':
            return Result(self.executed)
        if sql == 'SELECT @@server_uuid':
            return Result('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee')
        raise AssertionError('unexpected SQL ' + sql)
    def close(self): pass

session = Session()
class Shell:
    def connect(self, conn): pass
    def open_session(self, conn): return session
class Util:
    def load_dump(self, url, opts):
        session.executed = os.environ['SEAM_DUMP_GTIDS']
import mysqlsh
mysqlsh.globals = types.SimpleNamespace(shell=Shell(), session=session, util=Util())
g = job['main'].__globals__
g['_prepare_local_infile'] = lambda s, st: None
g['_restore_local_infile'] = lambda s, p: None
g['_target_coordinates'] = lambda s: (session.executed, 'mysql-bin.000001', 4)
orig_run = g['_run_pitr_segments']
def run(*a):
    orig_run(*a)
    applied = []
    if os.path.exists(os.environ['SEAM_BINLOG_LOG']):
        for line in open(os.environ['SEAM_BINLOG_LOG']).read().splitlines():
            applied.append(line.split('\t')[0][len('--include-gtids='):])
    session.executed = gunion(session.executed, *applied)
g['_run_pitr_segments'] = run
job['main']()
print('SEAM_MAIN_OK')
`

// seamGTIDNext prints, for every GTID of its --include-gtids=<set>
// argument, what mysqlbinlog prints for a GTID event: the "#… server id …
// end_log_pos … GTID" header and its comment lines, then the GTID_NEXT
// assignment (restore.py counts only an assignment in that position, F156).
const seamGTIDNext = `import re, sys
spec = sys.argv[1].split('=', 1)[1]
for member in [m for m in spec.split(',') if m]:
    parts = member.split(':')
    sid = parts[0]
    for part in parts[1:]:
        r = re.match(r'^(\d+)(?:-(\d+))?$', part)
        if not r:
            sid = parts[0] + ':' + part
            continue
        for n in range(int(r.group(1)), int(r.group(2) or r.group(1)) + 1):
            print("#261101 10:00:00 server id 101  end_log_pos 235 CRC32 0x883f9886 \tGTID\tlast_committed=0\tsequence_number=1")
            print("/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;")
            print("# original_commit_timestamp=1 (2026-11-01 10:00:00.000001 UTC)")
            print("SET @@SESSION.GTID_NEXT= '%s:%d'/*!*/;" % (sid, n))
`

func TestPITRDownloadRestoreScriptSeam(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("python3 is required to run restore.py in CI")
		}
		t.Skip("python3 is unavailable")
	}
	const location = "dumps/nightly"
	baseline := sidA + ":1-40"
	for _, tc := range []struct {
		name         string
		mode         pitrplan.Mode
		stop         func(a *dlArchive) time.Time
		exclude      string
		wantOutcome  string
		wantIncludes []string // per segment: "<file name>\t<--include-gtids set>"
		wantVerdict  []string
	}{
		{
			name: "timestamp across the failback",
			mode: pitrplan.ModeTimestamp,
			stop: func(a *dlArchive) time.Time { return a.tB(36).Add(-500 * time.Millisecond) },
			wantIncludes: []string{
				"mysql-bin.000002\t" + sidA + ":41-70",
				"mysql-bin.000003\t" + sidA + ":71-100," + sidB + ":1-35",
			},
			wantVerdict: []string{"outcome=TargetCovered", "segments=2", "files=2", "transactions=95", "lastFile=a%2Fmysql-bin.000003", "divergent=0", "onHistoryExcluded=0"},
		},
		{
			name: "latest to the archive head",
			mode: pitrplan.ModeLatest,
			wantIncludes: []string{
				"mysql-bin.000002\t" + sidA + ":41-70",
				"mysql-bin.000003\t" + sidA + ":71-110," + sidB + ":1-50",
			},
			wantVerdict: []string{"outcome=ArchiveHead", "stop= ", "segments=2", "files=2", "transactions=120"},
		},
		{
			name:    "timestamp with an on-history exclusion",
			mode:    pitrplan.ModeTimestamp,
			stop:    func(a *dlArchive) time.Time { return a.tB(36).Add(-500 * time.Millisecond) },
			exclude: sidA + ":50",
			wantIncludes: []string{
				"mysql-bin.000002\t" + sidA + ":41-49:51-70",
				"mysql-bin.000003\t" + sidA + ":71-100," + sidB + ":1-35",
			},
			wantVerdict: []string{"outcome=TargetCovered", "segments=2", "transactions=94", "onHistoryExcluded=1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			a := failbackArchive(t)

			// dump-meta: the dump b took at A:1-40, read through the real
			// checks into dump.json.
			end := a.tA(42)
			created := backupmeta.NewMicroTime(end.Add(-time.Second))
			stamp := backupmeta.Stamp{Kind: backupmeta.StampKind, Version: backupmeta.StampVersion,
				Backup:        backupmeta.ObjectRef{Namespace: "ns", Name: "nightly", UID: "backup-uid"},
				Source:        backupmeta.ObjectRef{Namespace: "ns", Name: "orders", UID: a.desc.Source.UID},
				PITRArchiveID: a.desc.ArchiveID, Site: "b", ServerID: 102,
				ServerClock: backupmeta.ServerClock{Start: created, End: backupmeta.NewMicroTime(end)}, CreatedAt: created}
			raw, err := stamp.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			putObject(t, a.store, backupmeta.StampKey(location), raw)
			putObject(t, a.store, path.Join(location, "@.json"), []byte(`{"gtidExecuted":"`+baseline+`","gtidExecutedInconsistent":false}`))
			putObject(t, a.store, path.Join(location, "@.done.json"), []byte(`{"end":"`+end.Format("2006-01-02 15:04:05")+`"}`))
			meta, err := loadDumpMeta(ctx, a.store, location, location, dumpExpectation{backupUID: "backup-uid", sourceUID: a.desc.Source.UID, gtidExecuted: baseline})
			if err != nil {
				t.Fatalf("dump-meta: %v", err)
			}
			metaDir, staging := t.TempDir(), t.TempDir()
			if err := writeDumpMeta(metaDir, meta); err != nil {
				t.Fatal(err)
			}

			// The replay request as the controller renders it on both
			// pitr-download and the main container.
			request := map[string]string{
				"BLOODRAVEN_PITR_MODE":            string(tc.mode),
				"BLOODRAVEN_PITR_ALLOW_PARTIAL":   "false",
				"BLOODRAVEN_PITR_DUMP_META":       filepath.Join(metaDir, dumpMetaJSONName),
				"BLOODRAVEN_PITR_OUTPUT_DIR":      staging,
				"BLOODRAVEN_PITR_DESTINATION":     a.dest.String(),
				"BLOODRAVEN_PITR_ENCRYPTION_MODE": "None",
				"BLOODRAVEN_PITR_MANIFEST_PREFIX": a.dest.KeyPrefix,
			}
			if tc.stop != nil {
				request["BLOODRAVEN_PITR_STOP_DATETIME"] = backupmeta.NewMicroTime(tc.stop(a)).String()
			}
			if tc.exclude != "" {
				request["BLOODRAVEN_PITR_EXCLUDE_GTIDS"] = tc.exclude
			}
			req, err := pitrDownloadRequestFromEnv(func(k string) string { return request[k] })
			if err != nil {
				t.Fatalf("pitr-download env: %v", err)
			}
			if _, err := planAndStage(ctx, a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
				t.Fatalf("pitr-download: %v", err)
			}
			cov := readCoverage(t, staging)

			// restore.py main() over the staged directory.
			dir, shims := t.TempDir(), t.TempDir()
			binlogLog, mysqlLog := filepath.Join(dir, "mysqlbinlog.log"), filepath.Join(dir, "mysql.log")
			// The mysqlbinlog shim emits one GTID_NEXT assignment per GTID
			// of its --include-gtids set, as the real one does; restore.py
			// checks them against the segment (P6).
			for name, body := range map[string]string{
				"mysqlbinlog":  "#!/bin/sh\nprintf '%s\\t%s\\t%s\\n' \"$1\" \"$2\" \"$3\" >>\"$SEAM_BINLOG_LOG\"\necho \"-- replay $1 $2 $3\"\nexec " + python + " " + filepath.Join(shims, "gtid_next.py") + " \"$1\"\n",
				"gtid_next.py": seamGTIDNext,
				"mysql":        "#!/bin/sh\necho \"$@\" >>\"$SEAM_MYSQL_LOG\"\ncat >>\"$SEAM_MYSQL_LOG\"\n",
			} {
				if err := os.WriteFile(filepath.Join(shims, name), []byte(body), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			for name, contents := range map[string]string{"job.py": controller.BackupRestoreScript(), "mysqlsh.py": "# stub\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			verdictPath, termPath := filepath.Join(dir, "verdict"), filepath.Join(dir, "termination-log")
			cmd := exec.Command(python, "-c", "import runpy\njob = runpy.run_path('job.py')\n"+seamPythonPrelude)
			cmd.Dir = dir
			cmd.Env = []string{"PYTHONPATH=" + dir, "PYTHONDONTWRITEBYTECODE=1", "PATH=" + shims + ":/usr/bin:/bin",
				"SEAM_BINLOG_LOG=" + binlogLog, "SEAM_MYSQL_LOG=" + mysqlLog, "SEAM_DUMP_GTIDS=" + baseline,
				"BLOODRAVEN_MYSQL_HOST=db:3306", "BLOODRAVEN_INPUT_URL=" + location, "MYSQL_USER=root", "MYSQL_PASSWORD=pw",
				"BLOODRAVEN_LOAD_PROGRESS_FILE=", "BLOODRAVEN_PITR_LOCAL_DIR=" + staging, "TZ=UTC",
				"BLOODRAVEN_PITR_EXPECTED_SOURCE_UID=" + string(a.desc.Source.UID),
				"BLOODRAVEN_PITR_VERDICT_PATH=" + verdictPath, "BLOODRAVEN_TERMINATION_LOG=" + termPath}
			for _, k := range []string{"BLOODRAVEN_PITR_MODE", "BLOODRAVEN_PITR_STOP_DATETIME", "BLOODRAVEN_PITR_ALLOW_PARTIAL", "BLOODRAVEN_PITR_EXCLUDE_GTIDS"} {
				if v, ok := request[k]; ok {
					cmd.Env = append(cmd.Env, k+"="+v)
				}
			}
			out, err := cmd.CombinedOutput()
			term, _ := os.ReadFile(termPath)
			if err != nil || !strings.Contains(string(out), "SEAM_MAIN_OK") {
				t.Fatalf("restore.py refused pitr-download's coverage: %v\ntermination: %s\n%s", err, term, out)
			}

			// One mysqlbinlog --include-gtids --start-position per segment,
			// in order, over the staged files. The offset is where the
			// staged bytes hold the GTID event of the segment's first
			// replayed transaction (restore.py checked the event's framing
			// before the load).
			root, err := filepath.EvalSymlinks(staging)
			if err != nil {
				t.Fatal(err)
			}
			logged, err := os.ReadFile(binlogLog)
			if err != nil {
				t.Fatal(err)
			}
			got := strings.Split(strings.TrimSpace(string(logged)), "\n")
			if len(got) != len(tc.wantIncludes) || len(cov.Segments) != len(tc.wantIncludes) {
				t.Fatalf("mysqlbinlog ran %q for %d segments, want %d", got, len(cov.Segments), len(tc.wantIncludes))
			}
			for k, want := range tc.wantIncludes {
				name, gtids, _ := strings.Cut(want, "\t")
				f := cov.Files[cov.Segments[k].File]
				if f.Name != name || f.Site != "a" {
					t.Errorf("segment %d reads %s/%s, want a/%s", k, f.Site, f.Name, name)
				}
				staged, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f.Path)))
				if err != nil {
					t.Fatal(err)
				}
				idx, err := sidecar.InspectTransactions(bytes.NewReader(staged), sidecar.TxIndexOptions{Label: f.Path})
				if err != nil {
					t.Fatal(err)
				}
				include := pitrplan.MustParseSet(gtids)
				start, first := int64(-1), int64(-1)
				for i, tx := range idx.Txns {
					if first < 0 {
						first = tx.Pos
					}
					if include.Has(idx.GTID(i)) {
						start = tx.Pos
						break
					}
				}
				if line := fmt.Sprintf("--include-gtids=%s\t--start-position=%d\t%s", gtids, start, filepath.Join(root, filepath.FromSlash(f.Path))); got[k] != line || cov.Segments[k].StartPosition != start {
					t.Errorf("mysqlbinlog call %d = %q (coverage startPosition %d), want %q", k, got[k], cov.Segments[k].StartPosition, line)
				}
				// The first segment starts inside its file: the dump holds
				// the file's first transactions.
				if k == 0 && start <= first {
					t.Errorf("segment 0 starts at offset %d, the file's first transaction (%d); want a mid-file start", start, first)
				}
			}
			stream, err := os.ReadFile(mysqlLog)
			if err != nil || strings.Count(string(stream), "--binary-mode") != 1 {
				t.Errorf("want one mysql session, got %q (%v)", stream, err)
			}

			verdict, err := os.ReadFile(verdictPath)
			if err != nil {
				t.Fatal(err)
			}
			line := strings.SplitN(string(verdict), "\n", 2)[0] + " "
			for _, want := range append([]string{"BLOODRAVEN_PITR_VERDICT v=2 mode=" + string(tc.mode) + " ", "archive=" + a.desc.ArchiveID + " ",
				"encryptionMode=None ", "sourceUID=" + string(a.desc.Source.UID) + " ", "sites=a "}, tc.wantVerdict...) {
				if !strings.Contains(line, want+map[bool]string{true: "", false: " "}[strings.HasSuffix(want, " ")]) {
					t.Errorf("verdict %q lacks %q", line, want)
				}
			}
			if !strings.Contains(string(verdict), "\nBLOODRAVEN_PITR_RESULT v=1 executed=") {
				t.Errorf("verdict has no result line: %q", verdict)
			}
		})
	}
}
