package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The restore.py contract tests (design WS-D §4.13, §9.1). They run the
// embedded script under python3 with a fake mysqlsh session whose
// GTID_SUBTRACT, GTID_SUBSET and @@global.gtid_executed are an exact
// GTID-set model, so every R1-R6 rule is checked with the same SQL the
// script sends to a real server.

// pitrScriptPrelude defines the GTID model, the fake session and a valid
// two-segment cross-site plan staged in a temporary directory:
//
//	baseline B = A:1-10 (the dump), lineage Λ = A:1-20,B:1-5
//	segment 0: a/mysql-bin.000001 replays A:11-20 from A:1-10
//	segment 1: b/mysql-bin.000004 replays B:1-5   from A:1-20
//	postStop  = B:6-7 (staged after the witness B:6)
const pitrScriptPrelude = `import copy, hashlib, io, json, os, re, signal, tempfile, types
A = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
B = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
STOP = '2030-01-01T00:00:00.000000Z'
ARCHIVE = 'ar1-' + 'a' * 26

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

def gsubset(a, b):
    return gsub(a, b) == ''

class Result:
    def __init__(self, value): self.value = value
    def fetch_one(self): return (self.value,)

TARGET = 'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee'

class Session:
    def __init__(self, executed='', uuid=TARGET):
        self.executed = executed
        self.uuid = uuid
        self.queries = []
        self.max_arg = 0
        self.closed = False
    def close(self):
        self.closed = True
    def run_sql(self, sql, args=None):
        self.queries.append(sql)
        args = list(args or [])
        self.max_arg = max([self.max_arg] + [len(a) for a in args])
        if sql == 'SELECT GTID_SUBTRACT(?, ?)':
            return Result(gsub(args[0], args[1]))
        if sql == 'SELECT GTID_SUBSET(?, ?)':
            return Result(1 if gsubset(args[0], args[1]) else 0)
        if sql == 'SELECT @@global.gtid_executed':
            return Result(self.executed)
        if sql == 'SELECT @@server_uuid':
            return Result(self.uuid)
        raise AssertionError('unexpected SQL ' + sql)

def stage(d, path, data):
    full = os.path.join(d, path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, 'wb') as f:
        f.write(data)
    return hashlib.sha256(data).hexdigest()

def write_coverage(d, cov):
    with open(os.path.join(d, 'coverage.json'), 'w') as f:
        json.dump(cov, f)

def base_plan():
    d = tempfile.mkdtemp()
    s1 = stage(d, 'a/mysql-bin.000001.r1', b'site a bytes')
    s2 = stage(d, 'b/mysql-bin.000004.r2', b'site b bytes')
    cov = {
        'version': 2, 'mode': 'timestamp', 'stopTime': STOP, 'allowPartial': False,
        'outcome': 'TargetCovered', 'refusalReason': '',
        'archive': {'archiveID': ARCHIVE, 'destination': 's3://bucket/src/binlogs?endpoint=aws',
                    'source': {'namespace': 'ns', 'name': 'src', 'uid': 'src-uid'}, 'encryptionMode': 'None'},
        'dump': {'location': 'src/nightly', 'backupUID': 'backup-uid', 'sourceUID': 'src-uid', 'pitrArchiveID': ARCHIVE,
                 'site': 'a', 'serverID': 101, 'end': '2029-12-31T23:00:00.000000Z'},
        'baseline': A + ':1-10', 'backupNewestCommitTime': '2029-12-31T22:59:00.000000Z', 'excluded': '',
        'lineage': A + ':1-20,' + B + ':1-5', 'replay': A + ':11-20,' + B + ':1-5', 'postStop': B + ':6-7',
        'divergent': '', 'onHistoryExclusions': '', 'lost': '',
        'witness': {'gtid': B + ':6', 'site': 'b', 'file': 'mysql-bin.000004', 'id': 'r2',
                    'commitTime': STOP, 'groundTruth': 'Origin'},
        'head': None, 'newestReplayedCommitTime': '2029-12-31T23:59:59.999999Z', 'replayedTransactionCount': 15,
        'sites': ['a', 'b'],
        'files': [
            {'path': 'a/mysql-bin.000001.r1', 'site': 'a', 'name': 'mysql-bin.000001', 'id': 'r1', 'sha256': s1,
             'previousGTIDs': A + ':1-5', 'gtids': A + ':6-20'},
            {'path': 'b/mysql-bin.000004.r2', 'site': 'b', 'name': 'mysql-bin.000004', 'id': 'r2', 'sha256': s2,
             'previousGTIDs': A + ':1-20', 'gtids': B + ':1-7'},
        ],
        'segments': [
            {'file': 0, 'gtids': A + ':11-20', 'startPredecessors': A + ':1-10', 'transactions': 10,
             'firstCommitTime': '2029-12-31T23:10:00.000000Z', 'lastCommitTime': '2029-12-31T23:20:00.000000Z'},
            {'file': 1, 'gtids': B + ':1-5', 'startPredecessors': A + ':1-20', 'transactions': 5,
             'firstCommitTime': '2029-12-31T23:30:00.000000Z', 'lastCommitTime': '2029-12-31T23:59:59.999999Z'},
        ],
    }
    write_coverage(d, cov)
    os.environ.update({'BLOODRAVEN_PITR_MODE': 'timestamp', 'BLOODRAVEN_PITR_STOP_DATETIME': STOP,
                       'BLOODRAVEN_PITR_ALLOW_PARTIAL': 'false', 'BLOODRAVEN_PITR_LOCAL_DIR': d})
    return d, cov

def read(d):
    return job['_read_pitr_coverage'](job['_pitr_request']())
`

// runPITRScript runs body after the prelude and returns its output and the
// termination message it recorded.
func runPITRScript(t *testing.T, body string, env map[string]string) (string, string, error) {
	t.Helper()
	term := filepath.Join(t.TempDir(), "termination-log")
	all := map[string]string{"BLOODRAVEN_TERMINATION_LOG": term}
	for k, v := range env {
		all[k] = v
	}
	out, err := runEmbeddedBackupPython(t, restoreScript, pitrScriptPrelude+body, all)
	recorded, _ := os.ReadFile(term)
	return string(out), string(recorded), err
}

// _read_pitr_coverage's refusals (§4.13 checks 1-6), each recorded for the
// controller with its token first: one case per guard (F151).
func TestPITRScriptCoverageRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, mutate, token, detail string
		env                         map[string]string
	}{
		{name: "valid plan", mutate: ``},
		{name: "missing coverage", mutate: `os.remove(os.path.join(d,'coverage.json'))`, token: "PITRCoverageInvalid"},
		{name: "version 1", mutate: `cov['version']=1`, token: "PITRCoverageInvalid", detail: "version"},
		{name: "recorded refusal", mutate: `cov['refusalReason']='PITRArchiveGap: x'`, token: "PITRCoverageInvalid", detail: "refusal"},
		{name: "mode differs", mutate: `cov['mode']='latest'`, token: "PITRCoverageInvalid", detail: "mode"},
		{name: "stop differs", mutate: `cov['stopTime']='2030-01-01T00:00:01.000000Z'`, token: "PITRCoverageInvalid", detail: "stop time"},
		{name: "partial policy differs", mutate: `cov['allowPartial']=True`, token: "PITRCoverageInvalid", detail: "partial policy"},
		{name: "unauthorized partial", mutate: `cov['outcome']='Partial'`, token: "PITRCoverageInvalid", detail: "not authorized"},
		{name: "verification partial", mutate: `cov['outcome']='Partial'; cov['allowPartial']=True`, token: "PITRCoverageInvalid", detail: "verification",
			env: map[string]string{"BLOODRAVEN_PITR_ALLOW_PARTIAL_OVERRIDE": "true", "BLOODRAVEN_VERIFY_PITR_MODE": "timestamp"}},
		{name: "archive head in timestamp mode", mutate: `cov['outcome']='ArchiveHead'`, token: "PITRCoverageInvalid", detail: "not valid"},
		{name: "covered without witness", mutate: `cov['witness']=None`, token: "PITRCoverageInvalid", detail: "witness"},
		{name: "exclusions differ", mutate: `cov['excluded']=B+':9'`, token: "PITRCoverageInvalid", detail: "exclusions"},
		{name: "archive and dump disagree", mutate: `cov['dump']['sourceUID']='other-uid'`, token: "PITRArchiveIdentityMismatch", detail: "other-uid"},
		{name: "unexpected source", mutate: ``, token: "PITRArchiveIdentityMismatch", detail: "restored backup's source group",
			env: map[string]string{"BLOODRAVEN_PITR_EXPECTED_SOURCE_UID": "expected-uid"}},
		{name: "dump stamped with another archive", mutate: `cov['dump']['pitrArchiveID']='ar1-'+'b'*26`, token: "PITRArchiveIdentityMismatch", detail: "stamped"},
		{name: "malformed archive id", mutate: `cov['archive']['archiveID']='ar1-short'`, token: "PITRCoverageInvalid", detail: "archiveID"},
		{name: "path escapes", mutate: `cov['files'][0]['path']='../escape'`, token: "PITRCoverageInvalid", detail: "escapes"},
		{name: "absolute path", mutate: `cov['files'][0]['path']='/etc/passwd'`, token: "PITRCoverageInvalid", detail: "path is invalid"},
		{name: "digest differs", mutate: `stage(d,'a/mysql-bin.000001.r1',b'tampered')`, token: "PITRCoverageInvalid", detail: "differs from the verified"},
		{name: "unlisted staged file", mutate: `stage(d,'a/mysql-bin.000002.r9',b'x')`, token: "PITRCoverageInvalid", detail: "unlisted"},
		{name: "leftover part file", mutate: `stage(d,'b/mysql-bin.000005.part',b'x')`, token: "PITRCoverageInvalid", detail: "unlisted"},
		{name: "listed file missing", mutate: `os.remove(os.path.join(d,'b/mysql-bin.000004.r2'))`, token: "PITRCoverageInvalid", detail: "missing"},
		{name: "empty segment", mutate: `cov['segments'][1]['gtids']=''`, token: "PITRCoverageInvalid", detail: "includes no GTID"},
		{name: "segment file out of range", mutate: `cov['segments'][1]['file']=2`, token: "PITRCoverageInvalid", detail: "out of range"},
		{name: "transaction count differs", mutate: `cov['replayedTransactionCount']=16`, token: "PITRCoverageInvalid", detail: "replayedTransactionCount"},
		{name: "not a GTID set", mutate: `cov['lineage']='a; DROP'`, token: "PITRCoverageInvalid", detail: "not a GTID set"},
		{name: "not an object", mutate: `cov = []`, token: "PITRCoverageInvalid", detail: "must be an object"},
		{name: "path listed twice", mutate: `cov['files'][1]['path']=cov['files'][0]['path']`, token: "PITRCoverageInvalid", detail: "listed twice"},
		{name: "staged symlink", mutate: `os.symlink(os.path.join(d,'a/mysql-bin.000001.r1'), os.path.join(d,'a/link'))`, token: "PITRCoverageInvalid", detail: "not a regular file"},
		{name: "listed path is a symlink out of the directory", mutate: `os.symlink('/etc', os.path.join(d,'esc')); cov['files'][0]['path']='esc/passwd'`, token: "PITRCoverageInvalid", detail: "escapes"},
		{name: "bad digest syntax", mutate: `cov['files'][0]['sha256']='XYZ'`, token: "PITRCoverageInvalid", detail: "sha256 is invalid"},
		{name: "empty site", mutate: `cov['files'][0]['site']=''`, token: "PITRCoverageInvalid", detail: "site is empty"},
		{name: "empty name", mutate: `cov['files'][0]['name']=''`, token: "PITRCoverageInvalid", detail: "name is empty"},
		{name: "file previous not a set", mutate: `cov['files'][0]['previousGTIDs']=5`, token: "PITRCoverageInvalid", detail: "previousGTIDs must be a GTID set string"},
		{name: "files not a list", mutate: `cov['files']={}`, token: "PITRCoverageInvalid", detail: "files must be a list"},
		{name: "segments not a list", mutate: `cov['segments']={}`, token: "PITRCoverageInvalid", detail: "segments must be a list"},
		{name: "segment not an object", mutate: `cov['segments'][0]=[]`, token: "PITRCoverageInvalid", detail: "segments[0] must be an object"},
		{name: "segment start not a set", mutate: `cov['segments'][0]['startPredecessors']='x;y'`, token: "PITRCoverageInvalid", detail: "startPredecessors is not a GTID set"},
		{name: "zero transactions", mutate: `cov['segments'][0]['transactions']=0`, token: "PITRCoverageInvalid", detail: "transactions is invalid"},
		// F154: a segment's count is checked against its GTIDs before the load.
		{name: "segment count differs from its GTIDs", mutate: `cov['segments'][0]['transactions']=11; cov['replayedTransactionCount']=16`, token: "PITRCoverageInvalid", detail: "segments[0] counts 11 transactions but includes 10 GTIDs"},
		{name: "sites malformed", mutate: `cov['sites']=['a','']`, token: "PITRCoverageInvalid", detail: "sites must be a list"},
		{name: "archive not an object", mutate: `cov['archive']=None`, token: "PITRCoverageInvalid", detail: "archive and dump must be objects"},
		{name: "encryption mode invalid", mutate: `cov['archive']['encryptionMode']='Rot13'`, token: "PITRCoverageInvalid", detail: "encryptionMode is invalid"},
		{name: "source incomplete", mutate: `cov['archive']['source']['uid']=''`, token: "PITRCoverageInvalid", detail: "source is incomplete"},
		{name: "unknown outcome", mutate: `cov['outcome']='Maybe'`, token: "PITRCoverageInvalid", detail: "not valid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range tc.env {
				env[k] = v
			}
			body := `d, cov = base_plan()
` + tc.mutate + `
if os.path.exists(os.path.join(d,'coverage.json')): write_coverage(d, cov)
if os.environ.get('BLOODRAVEN_PITR_ALLOW_PARTIAL_OVERRIDE'): os.environ['BLOODRAVEN_PITR_ALLOW_PARTIAL'] = 'true'
read(d)
print('COVERAGE_OK')
`
			out, term, err := runPITRScript(t, body, env)
			if tc.token == "" {
				if err != nil || !strings.Contains(out, "COVERAGE_OK") {
					t.Fatalf("valid plan refused: %v %s", err, out)
				}
				return
			}
			if err == nil || !strings.HasPrefix(term, tc.token+": ") || !strings.Contains(term, tc.detail) {
				t.Fatalf("want %s (%q), got err=%v term=%q out=%s", tc.token, tc.detail, err, term, out)
			}
		})
	}
}

// R1 and R3-R5 run with the server's GTID functions before the load; R2
// after it.
func TestPITRScriptPlanRules(t *testing.T) {
	for _, tc := range []struct {
		name, mutate, token, detail string
		baseline                    string // the loaded server's gtid_executed for R2; default the plan's
	}{
		{name: "valid plan"},
		{name: "R1 overlapping segments", mutate: `cov['segments'][1]['gtids']=A+':20,'+B+':1-5'; cov['segments'][1]['transactions']=6; cov['replayedTransactionCount']=16`, token: "PITRCoverageInvalid", detail: "R1: segment 1 repeats"},
		{name: "R1 union differs from replay", mutate: `cov['replay']=A+':11-20,'+B+':1-6'`, token: "PITRCoverageInvalid", detail: "R1: the segments replay"},
		{name: "R1 replay is not lineage minus baseline", mutate: `cov['lineage']=A+':1-20,'+B+':1-6'`, token: "PITRCoverageInvalid", detail: "R1: replay"},
		{name: "R3 baseline outside lineage", mutate: `cov['baseline']=A+':1-10,'+B+':99'`, token: "PITRCoverageInvalid", detail: "R3: the baseline holds bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:99"},
		{name: "R3 exclusion intersects backup", mutate: `cov['excluded']=A+':3'`, token: "PITRExcludeIntersectsBackup", detail: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:3"},
		{name: "R3 divergent not excluded", mutate: `cov['divergent']=B+':9'`, token: "PITRCoverageInvalid", detail: "R3: divergent"},
		{name: "R3 lost not excluded", mutate: `cov['lost']=B+':9'`, token: "PITRCoverageInvalid", detail: "R3: lost"},
		{name: "R3 on-history not excluded", mutate: `cov['onHistoryExclusions']=A+':12'`, token: "PITRCoverageInvalid", detail: "R3: onHistoryExclusions"},
		{name: "R4 segment starts elsewhere", mutate: `cov['segments'][1]['startPredecessors']=A+':1-19'`, token: "PITRCoverageInvalid", detail: "R4: segment 1"},
		{name: "R4 order reversed", mutate: `cov['segments'].reverse()`, token: "PITRCoverageInvalid", detail: "R4: segment 0"},
		{name: "R4 segment names the wrong file", mutate: `cov['segments'][1]['file']=0`, token: "PITRCoverageInvalid", detail: "R4: segment 1 replays bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-5, which its file a/mysql-bin.000001.r1 does not hold"},
		{name: "R4 start outside its file", mutate: `cov['files'][0]['previousGTIDs']=A+':1-12'; cov['files'][0]['gtids']=A+':13-20'`, token: "PITRCoverageInvalid", detail: "R4: segment 0 replays aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:11-12, which its file"},
		{name: "R4 start before its file", mutate: `cov['files'][0]['previousGTIDs']=A+':1-10,'+B+':9'`, token: "PITRCoverageInvalid", detail: "R4: segment 0 starts after aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10, which is no position of its file"},
		{name: "R2 loaded dump differs", baseline: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-11", token: "PITRCoverageInvalid", detail: "R2"},
		// F159: a postStop the history already replays is refused before
		// the load, not after the replay.
		{name: "R6 postStop inside the history", mutate: `cov['postStop']=B+':5-7'`, token: "PITRCoverageInvalid", detail: "R6: postStop holds bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loaded := "cov['baseline']"
			if tc.baseline != "" {
				loaded = "'" + tc.baseline + "'"
			}
			body := `d, cov = base_plan()
` + tc.mutate + `
os.environ['BLOODRAVEN_PITR_EXCLUDE_GTIDS'] = cov['excluded']
write_coverage(d, cov)
c = read(d)
s = Session()
job['_verify_pitr_plan'](s, c)
s.executed = ` + loaded + `
job['_verify_pitr_baseline'](s, c)
assert all(q.startswith('SELECT GTID_') or q == 'SELECT @@global.gtid_executed' for q in s.queries), s.queries
print('PLAN_OK')
`
			out, term, err := runPITRScript(t, body, nil)
			if tc.token == "" {
				if err != nil || !strings.Contains(out, "PLAN_OK") {
					t.Fatalf("valid plan refused: %v %s %s", err, term, out)
				}
				return
			}
			if err == nil || !strings.HasPrefix(term, tc.token+": ") || !strings.Contains(term, tc.detail) {
				t.Fatalf("want %s (%q), got err=%v term=%q out=%s", tc.token, tc.detail, err, term, out)
			}
		})
	}
}

// R5: latest and Partial need something to replay.
func TestPITRScriptEmptyReplay(t *testing.T) {
	for _, tc := range []struct {
		name, setup string
		token       string
	}{
		{name: "covered with an empty plan is fine", setup: `cov['lineage']=cov['baseline']; cov['replay']=''; cov['segments']=[]; cov['replayedTransactionCount']=0`},
		{name: "latest without replay", setup: `cov['mode']='latest'; cov['stopTime']=None; cov['outcome']='ArchiveHead'; cov['witness']=None; cov['head']={'site':'a','file':'mysql-bin.000001','id':'r1'}
cov['lineage']=cov['baseline']; cov['replay']=''; cov['segments']=[]; cov['replayedTransactionCount']=0
os.environ['BLOODRAVEN_PITR_MODE']='latest'; del os.environ['BLOODRAVEN_PITR_STOP_DATETIME']`, token: "PITRNoArchivedReplay"},
		{name: "partial without replay", setup: `cov['outcome']='Partial'; cov['allowPartial']=True; cov['witness']=None
cov['lineage']=cov['baseline']; cov['replay']=''; cov['segments']=[]; cov['replayedTransactionCount']=0
os.environ['BLOODRAVEN_PITR_ALLOW_PARTIAL']='true'`, token: "PITRNoArchivedReplay"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `d, cov = base_plan()
` + tc.setup + `
write_coverage(d, cov)
job['_verify_pitr_plan'](Session(), read(d))
print('PLAN_OK')
`
			out, term, err := runPITRScript(t, body, nil)
			if tc.token == "" {
				if err != nil || !strings.Contains(out, "PLAN_OK") {
					t.Fatalf("%v %s %s", err, term, out)
				}
				return
			}
			if err == nil || !strings.HasPrefix(term, tc.token+": ") {
				t.Fatalf("want %s, got err=%v term=%q out=%s", tc.token, err, term, out)
			}
		})
	}
}

// R6: the replayed server holds exactly Λ ∖ X, and nothing after the stop
// or outside the history.
func TestPITRScriptResultRule(t *testing.T) {
	for _, tc := range []struct {
		name, mutate, executed, token, detail string
	}{
		{name: "exact", executed: "A+':1-20,'+B+':1-5'"},
		{name: "exact with an excluded on-history transaction", mutate: `cov['excluded']=A+':12'; cov['onHistoryExclusions']=A+':12'`, executed: "A+':1-11:13-20,'+B+':1-5'"},
		{name: "missing a transaction", executed: "A+':1-20,'+B+':1-4'", token: "PITRReplayMismatch", detail: "missing bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:5"},
		{name: "applied the witness", executed: "A+':1-20,'+B+':1-6'", token: "PITRReplayMismatch", detail: "extra bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:6"},
		{name: "applied an excluded transaction", mutate: `cov['excluded']=A+':12'; cov['onHistoryExclusions']=A+':12'`, executed: "A+':1-20,'+B+':1-5'", token: "PITRReplayMismatch", detail: "R6"},
		{name: "post-stop leak inside the history", mutate: `cov['postStop']=B+':5-7'`, executed: "A+':1-20,'+B+':1-5'", token: "PITRReplayMismatch", detail: "after the stop"},
		// F038: a server that cannot be read is an operational failure,
		// never a replay mismatch.
		{name: "unreadable server", mutate: `Session.run_sql = lambda self, sql, args=None: (_ for _ in ()).throw(RuntimeError('MySQL Error 2013: Lost connection'))`, executed: "''", token: "BLOODRAVEN_PITR_FAILED", detail: "Lost connection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `d, cov = base_plan()
` + tc.mutate + `
s = Session(` + tc.executed + `)
print('S=' + job['_verify_pitr_result'](s, cov))
`
			out, term, err := runPITRScript(t, body, nil)
			if tc.token == "" {
				if err != nil || !strings.Contains(out, "S=") {
					t.Fatalf("%v %s %s", err, term, out)
				}
				return
			}
			if err == nil || !strings.HasPrefix(term, tc.token+": ") || !strings.Contains(term, tc.detail) {
				t.Fatalf("want %s (%q), got err=%v term=%q out=%s", tc.token, tc.detail, err, term, out)
			}
		})
	}
}

// pythonFakeReplay replaces subprocess.Popen: mysqlbinlog processes emit a
// marker and one GTID_NEXT assignment per GTID of their --include-gtids set,
// as the real one does (or fail for the segment index in FAIL_SEGMENT, or
// emit an extra, a missing or a repeated GTID for FAKE_EMIT, or fail to start
// for EXEC_FAIL); the one mysql process records its stdin and any signal it
// gets.
const pythonFakeReplay = `
procs = []
class FakeStdin(io.BytesIO):
    writes = 0
    def write(self, b):
        FakeStdin.writes += 1
        if os.environ.get('WRITE_FAIL') and FakeStdin.writes > int(os.environ['WRITE_FAIL']):
            raise BrokenPipeError(32, 'Broken pipe')
        return super().write(b)
    def close(self):
        self.final = self.getvalue()
        super().close()
def gtid_header():
    return b"#261106  4:11:55 server id 1  end_log_pos 235 CRC32 0x883f9886 \tGTID\tlast_committed=0\tsequence_number=1\n" \
        b"/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;\n# original_commit_timestamp=1 (2026-11-06 04:11:55.000001 UTC)\n" \
        b"/*!80001 SET @@session.original_commit_timestamp=1*//*!*/;\n"
def gtid_next(g):
    return gtid_header() + ("SET @@SESSION.GTID_NEXT= '%s'/*!*/;\n" % g).encode()
class FailingStdout(io.BytesIO):
    def __iter__(self):
        lines = self.getvalue().splitlines(True)
        yield lines[0]
        raise OSError(5, 'Input/output error')
class FakeProc:
    def __init__(self, args, stdin=None, stdout=None, env=None, **kw):
        self.args, self.signals, self.env, self.waited = args, [], env, False
        self.is_mysql = args[0] == 'mysql'
        if os.environ.get('EXEC_FAIL') == ('mysql' if self.is_mysql else 'mysqlbinlog'):
            raise OSError(7, 'Argument list too long')
        procs.append(self)
        if self.is_mysql:
            self.stdin = FakeStdin()
            self.rc = int(os.environ.get('MYSQL_RC', '0'))
            # A mysql still waiting for input never exits on its own.
            self.hang = bool(os.environ.get('READ_FAIL'))
            if os.environ.get('WRITE_FAIL'):
                self.rc = 1
        else:
            self.hang = False
            seg = len([p for p in procs if not p.is_mysql]) - 1
            self.rc = 1 if seg == int(os.environ.get('FAIL_SEGMENT', '-1')) else 0
            gtids = []
            for (uuid, tag), nums in sorted(gparse(args[1].split('=', 1)[1]).items()):
                gtids += ['%s%s:%d' % (uuid, ':' + tag if tag else '', n) for n in sorted(nums)]
            mode = os.environ.get('FAKE_EMIT', '')
            if mode == 'extra' and seg == 1:
                gtids.insert(1, B + ':6')
            if mode == 'missing' and seg == 1:
                gtids = gtids[:-1]
            if mode == 'repeat' and seg == 1:
                gtids = gtids[:-1] + [gtids[0]]
            body = ('-- segment %d %s\n' % (seg, args[1])).encode()
            for g in gtids:
                body += gtid_next(g) + ('INSERT /* %s */;\n' % g).encode()
                if mode == 'lookalike':
                    # A statement whose text holds a line that reads like a
                    # GTID_NEXT assignment (F156).
                    body += b"CREATE PROCEDURE p() SELECT 'x\nSET @@SESSION.GTID_NEXT= ''" + (B + ":99").encode() + b"''/*!*/;\ny'/*!*/;\n"
            body += b"SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;\n"
            self.stdout = FailingStdout(body) if os.environ.get('READ_FAIL') else io.BytesIO(body)
    def poll(self):
        return None if self.hang and signal.SIGKILL not in self.signals else self.rc
    def wait(self, timeout=None):
        if self.hang and signal.SIGKILL not in self.signals:
            if timeout is None:
                raise AssertionError('waited forever on a mysql that never exits')
            raise subprocess.TimeoutExpired(self.args, timeout)
        self.waited = True
        self.returncode = -9 if signal.SIGKILL in self.signals else self.rc
        return self.returncode
    def send_signal(self, sig):
        self.signals.append(sig)
g = job['_run_pitr_segments'].__globals__
import subprocess
g['subprocess'].Popen = FakeProc
`

// Every segment goes through its own mysqlbinlog --include-gtids into ONE
// mysql session; a mysqlbinlog failure SIGKILLs mysql (D-32).
func TestPITRScriptSegmentReplay(t *testing.T) {
	body := `d, cov = base_plan()
c = read(d)
` + pythonFakeReplay + `
job['_run_pitr_segments']('db', 3306, 'root', 'secret', c, '/usr/bin/mysqlbinlog')
mysql = [p for p in procs if p.is_mysql]
binlogs = [p for p in procs if not p.is_mysql]
assert len(mysql) == 1 and mysql[0].args[:2] == ['mysql', '--binary-mode'], [p.args for p in procs]
assert mysql[0].env['MYSQL_PWD'] == 'secret' and 'secret' not in ' '.join(mysql[0].args)
assert [p.args for p in binlogs] == [
    ['/usr/bin/mysqlbinlog', '--include-gtids=' + A + ':11-20', os.path.join(os.path.realpath(d), 'a/mysql-bin.000001.r1')],
    ['/usr/bin/mysqlbinlog', '--include-gtids=' + B + ':1-5', os.path.join(os.path.realpath(d), 'b/mysql-bin.000004.r2')],
], [p.args for p in binlogs]
stream = mysql[0].stdin.final.decode()
assert stream.index('segment 0') < stream.index('segment 1'), stream
assert not mysql[0].signals
print('REPLAY_OK')
`
	out, term, err := runPITRScript(t, body, nil)
	if err != nil || !strings.Contains(out, "REPLAY_OK") || !strings.Contains(out, "BLOODRAVEN_PITR_SEGMENT k=1 file=b%2Fmysql-bin.000004 gtids=5 transactions=5") {
		t.Fatalf("%v %s %s", err, term, out)
	}

	fail := `d, cov = base_plan()
c = read(d)
` + pythonFakeReplay + `
try:
    job['_run_pitr_segments']('db', 3306, 'root', 'secret', c, '/usr/bin/mysqlbinlog')
finally:
    mysql = [p for p in procs if p.is_mysql][0]
    print('SIGNALS=%s' % mysql.signals)
    print('BINLOGS=%d' % len([p for p in procs if not p.is_mysql]))
`
	out, term, err = runPITRScript(t, fail, map[string]string{"FAIL_SEGMENT": "0"})
	if err == nil || term != "BLOODRAVEN_PITR_FAILED: mysqlbinlog exit=1 segment=0" || !strings.Contains(out, "SIGNALS=[<Signals.SIGKILL: 9>]") || !strings.Contains(out, "BINLOGS=1") {
		t.Fatalf("mysqlbinlog failure: err=%v term=%q out=%s", err, term, out)
	}
}

// P6: restore.py checks what mysqlbinlog emits: a transaction outside the
// segment never reaches mysql, and the emitted set must be exactly the
// segment's. P7: a mysqlbinlog that cannot start is a recorded failure and
// mysql is killed.
func TestPITRScriptSegmentEmittedSet(t *testing.T) {
	body := `d, cov = base_plan()
c = read(d)
` + pythonFakeReplay + `
try:
    job['_run_pitr_segments']('db', 3306, 'root', 'secret', c, '/usr/bin/mysqlbinlog')
finally:
    mysql = [p for p in procs if p.is_mysql][0]
    print('SIGNALS=%s' % mysql.signals)
    print('FORWARDED_EXTRA=%s' % (B + ':6' in mysql.stdin.getvalue().decode() if not mysql.stdin.closed else 'closed'))
`
	for _, tc := range []struct{ emit, exec, term string }{
		{emit: "extra", term: "PITRReplayMismatch: segment 1 (b/mysql-bin.000004): mysqlbinlog emitted bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:6, which the segment does not replay"},
		{emit: "missing", term: "PITRReplayMismatch: segment 1 (b/mysql-bin.000004): mysqlbinlog emitted 4 transactions (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-4) but the segment replays 5 (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-5)"},
		{emit: "repeat", term: "PITRReplayMismatch: segment 1 (b/mysql-bin.000004): mysqlbinlog emitted 5 transactions (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-4, some twice)"},
		{exec: "mysqlbinlog", term: "BLOODRAVEN_PITR_FAILED: cannot start mysqlbinlog for segment=0: [Errno 7] Argument list too long"},
	} {
		t.Run(tc.emit+tc.exec, func(t *testing.T) {
			out, term, err := runPITRScript(t, body, map[string]string{"FAKE_EMIT": tc.emit, "EXEC_FAIL": tc.exec})
			if err == nil || !strings.HasPrefix(term, tc.term) || !strings.Contains(out, "SIGNALS=[<Signals.SIGKILL: 9>]") || strings.Contains(out, "FORWARDED_EXTRA=True") {
				t.Fatalf("err=%v term=%q out=%s", err, term, out)
			}
		})
	}
}

// D-32: mysqlbinlog comes from PATH, else mysqlsh's bundled copy, else the
// replay is unsupported.
func TestPITRScriptResolvesMysqlbinlog(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "mysqlbinlog"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	body := `g = job['_resolve_mysqlbinlog'].__globals__
g['_MYSQLSH_MYSQLBINLOG'] = os.environ['BUNDLED']
print('BIN=' + job['_resolve_mysqlbinlog']())
`
	bundled := filepath.Join(t.TempDir(), "mysqlbinlog")
	if err := os.WriteFile(bundled, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	empty := t.TempDir()
	for _, tc := range []struct {
		path, bundled, want, token string
	}{
		{path: bin, bundled: bundled, want: "BIN=" + filepath.Join(bin, "mysqlbinlog")},
		{path: empty, bundled: bundled, want: "BIN=" + bundled},
		{path: empty, bundled: filepath.Join(empty, "absent"), token: "PITRReplayUnsupported"},
	} {
		out, term, err := runPITRScript(t, body, map[string]string{"PATH": tc.path, "BUNDLED": tc.bundled})
		if tc.token != "" {
			if err == nil || !strings.HasPrefix(term, tc.token+": ") {
				t.Fatalf("want %s: %v %q %s", tc.token, err, term, out)
			}
			continue
		}
		if err != nil || !strings.Contains(out, tc.want) {
			t.Fatalf("want %s: %v %s", tc.want, err, out)
		}
	}
}

// main() orders the replay: the plan's set algebra before the load, R2
// after it, every segment into one session, R6, then the verdict, which the
// controller accepts.
func TestPITRScriptMainCertifiesReplay(t *testing.T) {
	for _, tc := range []struct {
		name      string
		setup     string
		longS     bool
		wantSites []string
	}{
		{name: "covered cross-site", wantSites: []string{"a", "b"}},
		{name: "on-history exclusion", setup: `cov['excluded']=A+':12'; cov['onHistoryExclusions']=A+':12'; cov['divergent']=''
cov['replay']=A+':11:13-20,'+B+':1-5'; cov['segments'][0]['gtids']=A+':11:13-20'; cov['segments'][0]['transactions']=9; cov['replayedTransactionCount']=14
os.environ['BLOODRAVEN_PITR_EXCLUDE_GTIDS']=A+':12'`, wantSites: []string{"a", "b"}},
		{name: "long executed set", longS: true, wantSites: []string{"a", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdictPath := filepath.Join(t.TempDir(), "verdict")
			long := "False"
			if tc.longS {
				long = "True"
			}
			body := `d, cov = base_plan()
` + tc.setup + `
LONG = ` + long + `
if LONG:
    extra = ','.join('%08x-0000-0000-0000-000000000000:1' % i for i in range(60))
    cov['baseline'] = cov['baseline'] + ',' + extra
    cov['lineage'] = cov['lineage'] + ',' + extra
    for seg in cov['segments']:
        seg['startPredecessors'] = seg['startPredecessors'] + ',' + extra
    for f in cov['files']:
        f['previousGTIDs'] = f['previousGTIDs'] + ',' + extra
write_coverage(d, cov)
` + pythonFakeReplay + `
order = []
session = Session()
class Shell:
    def connect(self, conn): order.append('connect')
    def open_session(self, conn):
        order.append('open')
        return session
class Util:
    def load_dump(self, url, opts):
        order.append('load')
        assert opts.get('progressFile') == ''
        session.executed = cov['baseline']
import mysqlsh
mysqlsh.globals = types.SimpleNamespace(shell=Shell(), session=session, util=Util())
orig_plan, orig_base = g['_verify_pitr_plan'], g['_verify_pitr_baseline']
g['_verify_pitr_plan'] = lambda s, c: (order.append('plan'), orig_plan(s, c))
g['_verify_pitr_baseline'] = lambda s, c, l='': (order.append('R2'), orig_base(s, c, l))
g['_resolve_mysqlbinlog'] = lambda: '/usr/bin/mysqlbinlog'
g['_prepare_local_infile'] = lambda s, st: None
g['_restore_local_infile'] = lambda s, p: None
g['_target_coordinates'] = lambda s: (session.executed, 'mysql-bin.000001', 4)
orig_run = g['_run_pitr_segments']
def run(*a):
    order.append('replay')
    orig_run(*a)
    session.executed = gsub(cov['lineage'], cov['excluded'])
g['_run_pitr_segments'] = run
os.environ.update({'BLOODRAVEN_MYSQL_HOST': 'db:3306', 'BLOODRAVEN_INPUT_URL': 'src/nightly', 'MYSQL_USER': 'root', 'MYSQL_PASSWORD': 'pw', 'BLOODRAVEN_LOAD_PROGRESS_FILE': ''})
job['main']()
assert order == ['connect', 'plan', 'load', 'open', 'R2', 'replay', 'open'], order
print('MAIN_OK')
`
			out, term, err := runPITRScript(t, body, map[string]string{"BLOODRAVEN_PITR_VERDICT_PATH": verdictPath})
			if err != nil || !strings.Contains(out, "MAIN_OK") {
				t.Fatalf("%v %q %s", err, term, out)
			}
			raw, err := os.ReadFile(verdictPath)
			if err != nil {
				t.Fatal(err)
			}
			if len(raw) > 4096 || len(strings.SplitN(string(raw), "\n", 2)[0]) > 1024 {
				t.Fatalf("verdict exceeds its bounds: %d bytes", len(raw))
			}
			res, err := parsePITRVerdict(string(raw), pitrRenderedRequest{Mode: "timestamp", Stop: "2030-01-01T00:00:00.000000Z", EncryptionMode: "None"})
			if err != nil {
				t.Fatalf("controller refused restore.py's verdict %q: %v", raw, err)
			}
			if res.Archive.ArchiveID != "ar1-"+strings.Repeat("a", 26) || res.SegmentCount != 2 || res.ReplayedBinlogCount != 2 ||
				res.LastReplayedBinlog == nil || res.LastReplayedBinlog.Site != "b" || res.LastReplayedBinlog.Name != "mysql-bin.000004" ||
				strings.Join(res.ReplaySites, ",") != strings.Join(tc.wantSites, ",") || res.StopTime == nil ||
				!res.StopTime.Time.Equal(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)) {
				t.Fatalf("result: %+v", res)
			}
			if tc.longS != (res.GtidExecuted == "") || res.GtidExecutedSHA256 == "" {
				t.Fatalf("executed set: %q sha=%q", res.GtidExecuted, res.GtidExecutedSHA256)
			}
			if tc.name == "on-history exclusion" && (res.OnHistoryExclusionCount != 1 || res.OnHistoryExclusions != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:12" || res.ReplayedTransactionCount != 14) {
				t.Fatalf("on-history result: %+v", res)
			}
		})
	}
}

// A refusal before the load never touches the target.
func TestPITRScriptRefusesBeforeLoad(t *testing.T) {
	body := `d, cov = base_plan()
cov['segments'][1]['startPredecessors'] = A + ':1-19'
write_coverage(d, cov)
loaded = []
session = Session()
class Shell:
    def connect(self, conn): pass
class Util:
    def load_dump(self, url, opts): loaded.append(url)
import mysqlsh
mysqlsh.globals = types.SimpleNamespace(shell=Shell(), session=session, util=Util())
g = job['main'].__globals__
g['_resolve_mysqlbinlog'] = lambda: '/usr/bin/mysqlbinlog'
os.environ.update({'BLOODRAVEN_MYSQL_HOST': 'db:3306', 'BLOODRAVEN_INPUT_URL': 'src/nightly', 'MYSQL_USER': 'root'})
try:
    job['main']()
finally:
    print('LOADED=%d' % len(loaded))
`
	out, term, err := runPITRScript(t, body, nil)
	if err == nil || !strings.HasPrefix(term, "PITRCoverageInvalid: R4") || !strings.Contains(out, "LOADED=0") {
		t.Fatalf("%v %q %s", err, term, out)
	}
}

// verify.sh publishes restore.py's verdict only once verification passes,
// and records its own failures.
func TestPITRVerifyScriptPublishesVerdictLast(t *testing.T) {
	start := strings.Index(verifyScript, "TERMINATION_LOG=")
	fail := verifyScript[start : strings.Index(verifyScript[start:], "\n}\n")+start+3]
	dir := t.TempDir()
	term := filepath.Join(dir, "termination-log")
	script := fail + `log() { :; }
fail 3 "SanityCheckFailed: scalar 0 < minRows 1"
`
	cmd := execBash(t, script, "BLOODRAVEN_TERMINATION_LOG="+term)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("fail did not exit: %s", out)
	}
	if recorded, _ := os.ReadFile(term); string(recorded) != "SanityCheckFailed: scalar 0 < minRows 1" {
		t.Fatalf("failure not recorded: %q", recorded)
	}
	if !strings.Contains(verifyScript, `cat "$PITR_VERDICT" >"$TERMINATION_LOG"`) || strings.Index(verifyScript, `cat "$PITR_VERDICT" >"$TERMINATION_LOG"`) < strings.Index(verifyScript, "# ---- Sanity check") {
		t.Fatal("verdict must be published after the sanity check")
	}
}

func execBash(t *testing.T, script string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH")}, env...)
	return cmd
}

// TestPITRNativeSegmentReplay is opt-in (BLOODRAVEN_PITR_NATIVE_TEST=1,
// docker; CI job test-pitr-native). It runs restore.py's own
// _run_pitr_segments against a real mysql:9.7 server, with mysqlbinlog and
// mysql invoked inside the container through PATH shims, over the captured
// binlog_transaction_compression fixture (GTIDs :3 and :4 of one file on
// top of :1-2). It proves F7 for the shapes the planner emits: two segments
// of the same file in one session, and an excluded transaction inside a
// segment, ending at exactly the planned gtid_executed and rows.
func TestPITRNativeSegmentReplay(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("opt-in native replay (BLOODRAVEN_PITR_NATIVE_TEST=1, docker)")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("docker is required")
	}
	fixtures, err := filepath.Abs("../sidecar/testdata/native")
	if err != nil {
		t.Fatal(err)
	}
	const uuid = "94ae26b8-d373-11f1-8ee3-b7be919e6317"
	image := os.Getenv("BLOODRAVEN_PITR_NATIVE_MYSQL_IMAGE")
	if image == "" {
		image = "mysql:9.7"
	}
	for _, tc := range []struct {
		name     string
		segments [][2]string // include set, start predecessors
		executed string
		rows     string
	}{
		{name: "two segments of one file", segments: [][2]string{{uuid + ":3", uuid + ":1-2"}, {uuid + ":4", uuid + ":1-3"}}, executed: uuid + ":1-4", rows: "1,2,4"},
		{name: "excluded transaction inside a segment", segments: [][2]string{{uuid + ":4", uuid + ":1-2"}}, executed: uuid + ":1-2:4", rows: "2,4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := fmt.Sprintf("bloodraven-pitr-native-%d", time.Now().UnixNano())
			run := exec.Command(docker, "run", "-d", "--rm", "--name", name, "--network", "none", "-e", "MYSQL_ALLOW_EMPTY_PASSWORD=1", "-e", "TZ=UTC",
				"-v", fixtures+":"+fixtures+":ro", image, "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
			if out, err := run.CombinedOutput(); err != nil {
				t.Fatalf("start %s: %v %s", image, err, out)
			}
			t.Cleanup(func() { _ = exec.Command(docker, "rm", "-f", name).Run() })
			sql := func(q string) string {
				out, err := exec.Command(docker, "exec", name, "mysql", "-uroot", "-N", "-B", "-e", q).CombinedOutput()
				if err != nil {
					t.Fatalf("%s: %v %s", q, err, out)
				}
				return strings.TrimSpace(string(out))
			}
			deadline := time.Now().Add(3 * time.Minute)
			for {
				if out, err := exec.Command(docker, "exec", name, "mysql", "-uroot", "-h127.0.0.1", "-e", "SELECT 1").CombinedOutput(); err == nil {
					break
				} else if time.Now().After(deadline) {
					t.Fatalf("mysql did not start: %s", out)
				}
				time.Sleep(2 * time.Second)
			}
			// E0: the schema the fixture's first file created, as a load
			// with updateGtidSet "replace" leaves it.
			sql("RESET BINARY LOGS AND GTIDS; SET sql_log_bin=0; CREATE DATABASE replay; CREATE TABLE replay.markers(id INT PRIMARY KEY, value BLOB); SET sql_log_bin=1; SET GLOBAL gtid_purged='" + uuid + ":1-2'")

			// The shims forward restore.py's own argv unchanged. The server
			// listens on 127.0.0.1:3306 inside the container, the fixtures
			// are mounted at their host path, and MYSQL_PWD is forwarded the
			// way restore.py hands it to mysql. mysqlbinlog is the image's
			// bundled copy at _MYSQLSH_MYSQLBINLOG: the mysql:9.7 image
			// ships no mysqlbinlog in /usr/bin, and only mysqlsh (which
			// runs restore.py in the Job) puts its libexec on PATH.
			shims := t.TempDir()
			for bin, script := range map[string]string{
				"mysqlbinlog": "#!/bin/sh\nexec " + docker + " exec " + name + " /usr/libexec/mysqlsh/mysqlbinlog \"$@\"\n",
				"mysql":       "#!/bin/sh\nexec " + docker + " exec -i -e MYSQL_PWD " + name + " mysql \"$@\"\n",
			} {
				if err := os.WriteFile(filepath.Join(shims, bin), []byte(script), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			segments := []map[string]any{}
			for _, s := range tc.segments {
				segments = append(segments, map[string]any{"file": 0, "gtids": s[0], "startPredecessors": s[1], "transactions": 1})
			}
			cov := map[string]any{"segments": segments, "files": []map[string]any{{"site": "a", "name": "mysql-bin.000002", "_path": filepath.Join(fixtures, "compressed-session.bin")}},
				"mode": "timestamp", "stopTime": "2030-01-01T00:00:00.000000Z", "replayedTransactionCount": len(segments)}
			raw, _ := json.Marshal(cov)
			body := `import json, os
c = json.loads(os.environ['COVERAGE'])
job['_run_pitr_segments']('127.0.0.1', 3306, 'root', '', c, job['_resolve_mysqlbinlog']())
print('NATIVE_OK')
`
			out, err := runEmbeddedBackupPython(t, restoreScript, body, map[string]string{"PATH": shims + ":" + os.Getenv("PATH"), "COVERAGE": string(raw), "BLOODRAVEN_TERMINATION_LOG": filepath.Join(t.TempDir(), "t")})
			if err != nil || !strings.Contains(string(out), "NATIVE_OK") {
				t.Fatalf("native replay: %v %s", err, out)
			}
			if got := strings.ReplaceAll(sql("SELECT @@global.gtid_executed"), "\n", ""); got != tc.executed {
				t.Fatalf("gtid_executed = %s, want %s", got, tc.executed)
			}
			if got := sql("SELECT GROUP_CONCAT(id ORDER BY id) FROM replay.markers"); got != tc.rows {
				t.Fatalf("rows = %s, want %s", got, tc.rows)
			}
		})
	}
}

// P8: verify.sh's mysqld comment describes the v2 replay (staged segment
// files, each GTID once), not v1's download-everything-and-dedup model.
func TestVerifyScriptDescribesSegmentReplay(t *testing.T) {
	for _, stale := range []string{"downloads EVERY site", "dedup the same", "double-applying"} {
		if strings.Contains(verifyScript, stale) {
			t.Errorf("verify_script.sh still says %q", stale)
		}
	}
	if !strings.Contains(verifyScript, "--include-gtids=<segment>") || !strings.Contains(verifyScript, "--gtid-mode=ON") {
		t.Error("verify_script.sh must explain why the segment replay needs GTID mode")
	}
}

// F035, F157, F158: mysql's own failures. A mysql that cannot start, dies
// mid-stream, keeps waiting while mysqlbinlog's output fails, or exits
// nonzero at the end is a recorded failure naming where it was detected,
// and every child process is reaped (a waiting mysql is killed first).
func TestPITRScriptReplayFailures(t *testing.T) {
	body := `d, cov = base_plan()
c = read(d)
` + pythonFakeReplay + `
try:
    job['_run_pitr_segments']('db', 3306, 'root', 'secret', c, '/usr/bin/mysqlbinlog')
finally:
    mysql = [p for p in procs if p.is_mysql]
    print('MYSQL_SIGNALS=%s' % (mysql[0].signals if mysql else 'none'))
    print('UNREAPED=%d' % len([p for p in procs if not p.waited]))
`
	for _, tc := range []struct {
		name, term string
		env        map[string]string
		killed     bool
	}{
		{name: "mysql cannot start", env: map[string]string{"EXEC_FAIL": "mysql"}, term: "BLOODRAVEN_PITR_FAILED: cannot start mysql: [Errno 7]"},
		{name: "mysql dies mid-stream", env: map[string]string{"WRITE_FAIL": "3"},
			term: "BLOODRAVEN_PITR_FAILED: mysql exit=1 segment=0 (segment is where the failure was detected; the failing statement may be from an earlier segment; [Errno 32] Broken pipe)"},
		{name: "mysqlbinlog output fails while mysql waits", env: map[string]string{"READ_FAIL": "1"}, killed: true,
			term: "BLOODRAVEN_PITR_FAILED: mysql exit=-9 segment=0 (segment is where the failure was detected; the failing statement may be from an earlier segment; [Errno 5] Input/output error)"},
		{name: "mysql exits nonzero at the end", env: map[string]string{"MYSQL_RC": "1"},
			term: "BLOODRAVEN_PITR_FAILED: mysql exit=1 segment=1 (segment is where the failure was detected; the failing statement may be from an earlier segment)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, term, err := runPITRScript(t, body, tc.env)
			if err == nil || !strings.HasPrefix(term, tc.term) {
				t.Fatalf("err=%v term=%q out=%s", err, term, out)
			}
			if !strings.Contains(out, "UNREAPED=0") {
				t.Fatalf("a child process was not reaped: %s", out)
			}
			if tc.killed != strings.Contains(out, "MYSQL_SIGNALS=[<Signals.SIGKILL: 9>]") {
				t.Fatalf("mysql killed=%t expected, got %s", tc.killed, out)
			}
		})
	}
}

// F156: a statement whose text holds a line reading like a GTID_NEXT
// assignment is replayed, not taken for a transaction boundary.
func TestPITRScriptGTIDNextLookalike(t *testing.T) {
	body := `d, cov = base_plan()
c = read(d)
` + pythonFakeReplay + `
job['_run_pitr_segments']('db', 3306, 'root', 'secret', c, '/usr/bin/mysqlbinlog')
mysql = [p for p in procs if p.is_mysql][0]
assert (B + ':99').encode() in mysql.stdin.final, 'the statement was not forwarded'
print('REPLAY_OK')
`
	out, term, err := runPITRScript(t, body, map[string]string{"FAKE_EMIT": "lookalike"})
	if err != nil || !strings.Contains(out, "REPLAY_OK") {
		t.Fatalf("%v %q %s", err, term, out)
	}
}

// F155: the emitted-set check keeps merged intervals, whatever the order,
// and still finds repeats and strays.
func TestPITRScriptSegmentCheckIntervals(t *testing.T) {
	body := `
C = job['_SegmentCheck']
c = C(A + ':1-100000')
for n in range(1, 100001):
    assert c.admit('%s:%d' % (A, n))
emitted, repeated = c.emitted()
assert emitted == {A: [(1, 100000)]} and not repeated and c.count == 100000, (emitted, repeated)
assert len(c.seen[A][0]) == 1
c = C(A + ':1-10:20-30,' + B + ':tag:1-3')
for g in [A + ':30', A + ':5', A + ':6', A + ':4', B.upper() + ':TAG:2', A + ':20']:
    assert c.admit(g), g
assert not c.admit(A + ':15') and not c.admit(B + ':2')
emitted, repeated = c.emitted()
assert emitted == {A: [(4, 6), (20, 20), (30, 30)], B + ':tag': [(2, 2)]} and not repeated, emitted
assert c.admit(A + ':5')
assert c.emitted()[1] and c.count == 7
print('CHECK_OK')
`
	out, term, err := runPITRScript(t, body, nil)
	if err != nil || !strings.Contains(out, "CHECK_OK") {
		t.Fatalf("%v %q %s", err, term, out)
	}
}

// F039: the plan checks accumulate canonical sets, so many consecutive
// segments of one source never send a growing list to the server.
func TestPITRScriptPlanChecksStayCompact(t *testing.T) {
	body := `d, cov = base_plan()
N = 3000
cov['files'] = cov['files'][:1]
cov['files'][0]['gtids'] = A + ':6-%d' % (10 + N)
cov['segments'] = [{'file': 0, 'gtids': A + ':%d' % n, 'startPredecessors': A + ':1-%d' % (n - 1), 'transactions': 1,
                    'firstCommitTime': STOP, 'lastCommitTime': STOP} for n in range(11, 11 + N)]
cov['lineage'] = A + ':1-%d' % (10 + N)
cov['replay'] = A + ':11-%d' % (10 + N)
cov['postStop'] = ''
cov['replayedTransactionCount'] = N
os.remove(os.path.join(d, 'b/mysql-bin.000004.r2'))
write_coverage(d, cov)
s = Session()
job['_verify_pitr_plan'](s, read(d))
assert s.max_arg < 200, s.max_arg
print('COMPACT_OK')
`
	out, term, err := runPITRScript(t, body, nil)
	if err != nil || !strings.Contains(out, "COMPACT_OK") {
		t.Fatalf("%v %q %s", err, term, out)
	}
}

// F004: a fresh target's own transactions from before the load (image
// initialization, the operator's credential reconcile) are part of R2's
// and R6's exact sets; anything else on the target refuses.
func TestPITRScriptTargetLocalHistory(t *testing.T) {
	for _, tc := range []struct {
		name, setup, token, detail string
	}{
		{name: "own transactions kept", setup: `s = Session(TARGET + ':1-3')`},
		{name: "fresh target", setup: `s = Session('')`},
		{name: "another server's transactions", setup: `s = Session(TARGET + ':1-3,' + 'ffffffff-ffff-ffff-ffff-ffffffffffff:1')`, token: "PITRCoverageInvalid", detail: "transactions of other servers (ffffffff-ffff-ffff-ffff-ffffffffffff)"},
		{name: "target uuid in the plan", setup: `s = Session(A + ':1-3', uuid=A.upper())`, token: "PITRCoverageInvalid", detail: "is a server of the restored history"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `d, cov = base_plan()
c = read(d)
` + tc.setup + `
local = job['_target_local_gtids'](s, c)
s.executed = job['_gtid_union'](c['baseline'], local)
job['_verify_pitr_baseline'](s, c, local)
s.executed = job['_gtid_union'](gsub(c['lineage'], c['excluded']), local)
S = job['_verify_pitr_result'](s, c, local)
assert (TARGET in S) == bool(local), S
print('LOCAL_OK')
`
			out, term, err := runPITRScript(t, body, nil)
			if tc.token == "" {
				if err != nil || !strings.Contains(out, "LOCAL_OK") {
					t.Fatalf("%v %q %s", err, term, out)
				}
				return
			}
			if err == nil || !strings.HasPrefix(term, tc.token+": ") || !strings.Contains(term, tc.detail) {
				t.Fatalf("want %s (%q), got err=%v term=%q out=%s", tc.token, tc.detail, err, term, out)
			}
		})
	}
	// R2 and R6 stay exact: target transactions beyond those seen before
	// the load (a write during the replay) still refuse.
	body := `d, cov = base_plan()
c = read(d)
s = Session(TARGET + ':1-3')
local = job['_target_local_gtids'](s, c)
s.executed = job['_gtid_union'](gsub(c['lineage'], c['excluded']), TARGET + ':1-4')
job['_verify_pitr_result'](s, c, local)
`
	_, term, err := runPITRScript(t, body, nil)
	if err == nil || !strings.HasPrefix(term, "PITRReplayMismatch: R6") || !strings.Contains(term, "extra "+"eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee:4") {
		t.Fatalf("a target write during the replay: err=%v term=%q", err, term)
	}
}

// F038: a proof session that cannot be opened is an operational failure.
func TestPITRScriptCheckSessionFailure(t *testing.T) {
	body := `
class BadShell:
    def open_session(self, conn): raise RuntimeError('MySQL Error 2003: Can not connect')
import mysqlsh
mysqlsh.globals = types.SimpleNamespace(shell=BadShell())
job['_open_check_session']({}, 'prove the replay (R6)')
`
	_, term, err := runPITRScript(t, body, nil)
	if err == nil || term != "BLOODRAVEN_PITR_FAILED: cannot open a session to prove the replay (R6): MySQL Error 2003: Can not connect" {
		t.Fatalf("err=%v term=%q", err, term)
	}
}

// F040: the verdict fits this container's share of the kubelet's
// termination messages (12 KiB over four containers in an encrypted PITR
// pod): the on-history line goes first, then S gives way to its digest,
// and no line is cut.
func TestPITRScriptVerdictBudget(t *testing.T) {
	body := `d, cov = base_plan()
cov['onHistoryExclusions'] = ','.join('%08x-0000-0000-0000-000000000000:1' % i for i in range(18))
cov['excluded'] = cov['onHistoryExclusions']
executed = ','.join('%08x-1111-1111-1111-111111111111:1-99' % i for i in range(48))
assert 1900 < len(executed) <= 2048 and len(cov['onHistoryExclusions']) <= 768
print('VERDICT=' + json.dumps(job['_pitr_verdict_message'](cov, executed)))
os.environ['BLOODRAVEN_TERMINATION_MESSAGE_LIMIT'] = '4096'
print('ROOMY=' + json.dumps(job['_pitr_verdict_message'](cov, executed)))
os.environ['BLOODRAVEN_TERMINATION_MESSAGE_LIMIT'] = '1500'
print('TIGHT=' + json.dumps(job['_pitr_verdict_message'](cov, executed)))
`
	out, term, err := runPITRScript(t, body, nil)
	if err != nil {
		t.Fatalf("%v %q %s", err, term, out)
	}
	read := func(prefix string) string {
		for _, line := range strings.Split(out, "\n") {
			if v, ok := strings.CutPrefix(line, prefix); ok {
				var msg string
				if err := json.Unmarshal([]byte(v), &msg); err != nil {
					t.Fatal(err)
				}
				return msg
			}
		}
		t.Fatalf("no %s in %s", prefix, out)
		return ""
	}
	req := pitrRenderedRequest{Mode: "timestamp", Stop: "2030-01-01T00:00:00.000000Z", EncryptionMode: "None"}
	for _, tc := range []struct {
		prefix                    string
		limit                     int
		wantOnHistory, wantInline bool
	}{
		{prefix: "VERDICT=", limit: 3072, wantInline: true},
		{prefix: "ROOMY=", limit: 4096, wantOnHistory: true, wantInline: true},
		{prefix: "TIGHT=", limit: 1500},
	} {
		msg := read(tc.prefix)
		if len(msg) > tc.limit {
			t.Fatalf("%s %d bytes over %d", tc.prefix, len(msg), tc.limit)
		}
		res, err := parsePITRVerdict(msg, req)
		if err != nil {
			t.Fatalf("%s refused: %v", tc.prefix, err)
		}
		if (res.OnHistoryExclusions != "") != tc.wantOnHistory || (res.GtidExecuted != "") != tc.wantInline || res.OnHistoryExclusionCount != 18 {
			t.Fatalf("%s: on-history %q, executed inline %t", tc.prefix, res.OnHistoryExclusions, res.GtidExecuted != "")
		}
	}
}

// F160: a cut GTID set in the log says so.
func TestPITRScriptFindingsMarkTruncation(t *testing.T) {
	body := `d, cov = base_plan()
cov['divergent'] = ','.join('%08x-0000-0000-0000-000000000000:1' % i for i in range(2000))
job['_print_pitr_findings'](cov)
`
	out, term, err := runPITRScript(t, body, nil)
	if err != nil {
		t.Fatalf("%v %q", err, term)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "BLOODRAVEN_PITR_DIVERGENT_SET gtids=") {
			if len(line) != len("BLOODRAVEN_PITR_DIVERGENT_SET gtids=")+65536+3 || !strings.HasSuffix(line, "...") {
				t.Fatalf("divergent set line of %d bytes without its marker", len(line))
			}
			return
		}
	}
	t.Fatalf("no divergent set line: %s", out)
}

// F040: verify.sh publishes restore.py's verdict whole or not at all.
func TestPITRVerifyScriptVerdictBudget(t *testing.T) {
	start := strings.Index(verifyScript, "TERMINATION_LOG=")
	fail := verifyScript[start : strings.Index(verifyScript[start:], "\n}\n")+start+3]
	publish := verifyScript[strings.Index(verifyScript, "# Publishing the verdict is the last action."):]
	for _, tc := range []struct {
		size  int
		limit string
		ok    bool
	}{
		{size: 3000, ok: true},
		{size: 3100},
		{size: 3100, limit: "4096", ok: true},
		{size: 4100, limit: "9000"},
	} {
		dir := t.TempDir()
		term, verdict := filepath.Join(dir, "termination-log"), filepath.Join(dir, "verdict")
		if err := os.WriteFile(verdict, []byte(strings.Repeat("v", tc.size)), 0o644); err != nil {
			t.Fatal(err)
		}
		env := []string{"BLOODRAVEN_TERMINATION_LOG=" + term, "PITR_VERDICT=" + verdict}
		if tc.limit != "" {
			env = append(env, "BLOODRAVEN_TERMINATION_MESSAGE_LIMIT="+tc.limit)
		}
		out, err := execBash(t, fail+"log() { :; }\n"+publish, env...).CombinedOutput()
		recorded, _ := os.ReadFile(term)
		if tc.ok {
			if err != nil || len(recorded) != tc.size {
				t.Fatalf("%d bytes (limit %q): %v %s, recorded %d", tc.size, tc.limit, err, out, len(recorded))
			}
			continue
		}
		if err == nil || !strings.HasPrefix(string(recorded), "PITRCoverageInvalid: restore.py's verdict is") {
			t.Fatalf("%d bytes (limit %q): err=%v recorded %q", tc.size, tc.limit, err, recorded)
		}
	}
}
