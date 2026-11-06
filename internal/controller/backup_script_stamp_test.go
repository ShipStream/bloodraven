package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dumpScriptPrelude fakes the mysqlsh globals dump.py's main() uses: a
// session answering the GTID capture, SHOW BINARY LOG STATUS and the
// serverClock.end query, and a dump_instance that writes @.json into a
// local output directory (or fails when TEST_DUMP_FAIL is set).
const dumpScriptPrelude = `import json, os, types
import mysqlsh
CLOCK = '2026-11-04T13:00:00.654321Z'
class Col:
    def __init__(self, label): self.label = label
    def get_column_label(self): return self.label
class Res:
    def __init__(self, row, cols=()): self.row, self.cols = row, cols
    def fetch_one(self): return self.row
    def get_columns(self): return [Col(c) for c in self.cols]
QUERIES = []
DUMPED = {'done': False}
class Session:
    def __init__(self, uuid, fresh=False): self.uuid, self.fresh, self.closed = uuid, fresh, False
    def run_sql(self, sql, args=None):
        if self.closed: raise Exception('session closed')
        # The session opened before the dump idles through it; with
        # TEST_IDLE_KILLED the server dropped it (wait_timeout, 4031).
        if not self.fresh and DUMPED['done'] and os.environ.get('TEST_IDLE_KILLED'):
            raise Exception('MySQL Error (4031): The client was disconnected by the server because of inactivity.')
        QUERIES.append(sql)
        if sql == 'SELECT @@global.gtid_executed': return Res(('aaaa:1-5',))
        if sql == 'SHOW BINARY LOG STATUS': return Res(('mysql-bin.000003', 157), ('File', 'Position'))
        if sql == 'SELECT @@global.server_uuid': return Res((self.uuid,))
        if sql == job['_SERVER_CLOCK_SQL']: return Res((CLOCK,))
        raise AssertionError('unexpected SQL ' + sql)
    def close(self): self.closed = True
class Util:
    def dump_instance(self, out, opts):
        if os.environ.get('TEST_DUMP_FAIL'):
            raise Exception('dump exploded')
        if not opts.get('s3BucketName'):
            os.makedirs(out, exist_ok=True)
            with open(os.path.join(out, '@.json'), 'w') as f:
                f.write('{}')
        DUMPED['done'] = True
UUID = '3E11FA47-71CA-11E1-9E33-C80AA9429562'
class Shell:
    def connect(self, conn): pass
    def open_session(self, conn):
        return Session(os.environ.get('TEST_FRESH_UUID') or UUID.lower(), fresh=True)
session = Session(UUID)
mysqlsh.globals = types.SimpleNamespace(shell=Shell(), session=session, util=Util())
try:
    job['main']()
    code = 0
except SystemExit as e:
    code = e.code
print('EXIT', code)
print('CLOCK_QUERIED', job['_SERVER_CLOCK_SQL'] in QUERIES)
`

// Review B7: dump.py's end path, which every backup's stamp depends on.
// A plaintext Job finishes the stamp through the bloodraven binary the
// backup-stamp init container installed (BLOODRAVEN_STAMP_TOOL) with
// `backup-stamp --end <serverClock.end>`; an encrypted Job's staged dump
// carries the end to encrypt-upload in BLOODRAVEN_DUMP_META.json
// (serverClockEnd, the field encrypt-upload reads). With neither, or when
// the tool fails, the backup fails rather than leave a stamp without an end
// (every PITR would refuse PITRBackupIncomplete).
func TestDumpScriptFinishesBackupStamp(t *testing.T) {
	for _, tc := range []struct {
		name string
		// s3: dump to an S3 prefix; otherwise to a local directory.
		s3, tool, dumpFails bool
		idleKilled          bool
		freshUUID           string
		toolExit            string
		wantExit            string
		wantArgv            string // "" = the tool must not run
		wantMetaEnd         string // "-" = no meta file
		wantErr             string
	}{
		{name: "plain S3 finishes through the tool", s3: true, tool: true, wantExit: "EXIT 0",
			wantArgv: "backup-stamp --end 2026-11-04T13:00:00.654321Z", wantMetaEnd: "-"},
		{name: "plain PVC finishes through the tool", tool: true, wantExit: "EXIT 0",
			wantArgv: "backup-stamp --end 2026-11-04T13:00:00.654321Z", wantMetaEnd: ""},
		{name: "encrypted staging hands the end to encrypt-upload", wantExit: "EXIT 0", wantMetaEnd: "2026-11-04T13:00:00.654321Z"},
		{name: "S3 without the tool fails", s3: true, wantExit: "EXIT 2", wantMetaEnd: "-",
			wantErr: "BLOODRAVEN_DUMP_FAILED: backup stamp: BLOODRAVEN_STAMP_TOOL is unset"},
		{name: "a failing tool fails the backup", s3: true, tool: true, toolExit: "3", wantExit: "EXIT 2",
			wantArgv: "backup-stamp --end 2026-11-04T13:00:00.654321Z", wantMetaEnd: "-", wantErr: "backup-stamp --end exited 3"},
		{name: "a failed dump never finishes the stamp", s3: true, tool: true, dumpFails: true, wantExit: "EXIT 2", wantMetaEnd: "-",
			wantErr: "BLOODRAVEN_DUMP_FAILED: dump exploded"},
		// F027: the pre-dump session idles through the dump and the server
		// drops it after wait_timeout; serverClock.end comes from a fresh
		// session to the same server.
		{name: "a dump that outlasts wait_timeout still finishes", s3: true, tool: true, idleKilled: true, wantExit: "EXIT 0",
			wantArgv: "backup-stamp --end 2026-11-04T13:00:00.654321Z", wantMetaEnd: "-"},
		{name: "the fresh session must reach the dumped server", s3: true, tool: true, freshUUID: "4f22ab58-82db-22f2-af44-d91bba530673", wantExit: "EXIT 2",
			wantMetaEnd: "-", wantErr: "not the dumped server 3e11fa47-71ca-11e1-9e33-c80aa9429562"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			argvLog := filepath.Join(dir, "argv")
			output := filepath.Join(dir, "staging", "nightly-1")
			env := map[string]string{
				"BLOODRAVEN_MYSQL_HOST": "lion-pdx:3306", "MYSQL_USER": "backup",
				"BLOODRAVEN_OUTPUT_URL": output, "TEST_ARGV_LOG": argvLog, "TEST_TOOL_EXIT": tc.toolExit,
			}
			if tc.s3 {
				env["BLOODRAVEN_OUTPUT_URL"] = "prod/nightly-1"
				env["BLOODRAVEN_S3_BUCKET"] = "bucket"
			}
			if tc.tool {
				tool := filepath.Join(dir, "bloodraven")
				script := "#!/bin/sh\necho \"$@\" > \"$TEST_ARGV_LOG\"\nexit ${TEST_TOOL_EXIT:-0}\n"
				if err := os.WriteFile(tool, []byte(script), 0o755); err != nil {
					t.Fatal(err)
				}
				env["BLOODRAVEN_STAMP_TOOL"] = tool
			}
			if tc.dumpFails {
				env["TEST_DUMP_FAIL"] = "1"
			}
			if tc.idleKilled {
				env["TEST_IDLE_KILLED"] = "1"
			}
			if tc.freshUUID != "" {
				env["TEST_FRESH_UUID"] = tc.freshUUID
			}
			out, _ := runEmbeddedBackupPython(t, dumpScript, dumpScriptPrelude, env)
			text := string(out)
			if !strings.Contains(text, tc.wantExit) {
				t.Fatalf("want %s:\n%s", tc.wantExit, text)
			}
			if tc.wantErr != "" && !strings.Contains(text, tc.wantErr) {
				t.Fatalf("want %q:\n%s", tc.wantErr, text)
			}
			if complete := strings.Contains(text, "BLOODRAVEN_DUMP_COMPLETE"); complete != (tc.wantExit == "EXIT 0") {
				t.Fatalf("DUMP_COMPLETE printed=%t:\n%s", complete, text)
			}
			if clock := strings.Contains(text, "CLOCK_QUERIED True"); clock != (!tc.dumpFails && tc.freshUUID == "") {
				t.Fatalf("serverClock.end queried=%t after a dump that failed=%t", clock, tc.dumpFails)
			}
			argv, err := os.ReadFile(argvLog)
			if tc.wantArgv == "" {
				if err == nil {
					t.Fatalf("the stamp tool ran: %q", argv)
				}
			} else if strings.TrimSpace(string(argv)) != tc.wantArgv {
				t.Fatalf("tool argv %q (%v), want %q", argv, err, tc.wantArgv)
			}
			raw, err := os.ReadFile(filepath.Join(output, "BLOODRAVEN_DUMP_META.json"))
			if tc.wantMetaEnd == "-" {
				if err == nil {
					t.Fatalf("unexpected dump meta %s", raw)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var meta map[string]any
			if err := json.Unmarshal(raw, &meta); err != nil {
				t.Fatal(err)
			}
			end, has := meta["serverClockEnd"]
			if tc.wantMetaEnd == "" && has || tc.wantMetaEnd != "" && end != tc.wantMetaEnd {
				t.Fatalf("serverClockEnd = %v (present %t), want %q", end, has, tc.wantMetaEnd)
			}
			if meta["gtidExecuted"] != "aaaa:1-5" || meta["binlogFile"] != "mysql-bin.000003" {
				t.Fatalf("dump meta %v", meta)
			}
		})
	}
}
