package controller

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// pythonFakeProcess replaces subprocess.Popen and records argv.
const pythonFakeProcess = `import io
calls=[]
class Process:
    def __init__(self,args,**kwargs):
        calls.append(args)
        self.stdout=io.BytesIO()
    def wait(self): return 0
g=job['_run_pitr'].__globals__
g['subprocess'].Popen=Process
`

func TestPITRScriptRequiresCoverage(t *testing.T) {
	term := filepath.Join(t.TempDir(), "termination-log")
	out, err := runEmbeddedBackupPython(t, restoreScript, `import tempfile
job['_run_pitr']('db', 3306, 'root', 'secret', '2030-01-01 00:00:00', '', tempfile.mkdtemp(), '')
`, map[string]string{"BLOODRAVEN_TERMINATION_LOG": term})
	if err == nil || !strings.Contains(string(out), "PITRCoverageInvalid") {
		t.Fatalf("missing coverage accepted: %s %v", out, err)
	}
	if recorded, _ := os.ReadFile(term); !strings.HasPrefix(string(recorded), "PITRCoverageInvalid: ") {
		t.Fatalf("refusal not recorded for the controller: %q", recorded)
	}
}

// The verdict restore.py writes must be the one the controller accepts.
func TestPITRScriptVerdictMatchesController(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		allow, partial, covered, files bool
		wantError, noop                bool
	}{
		{name: "strict uncovered", wantError: true},
		{name: "partial not authorized", partial: true, wantError: true},
		{name: "partial noop", allow: true, partial: true, noop: true},
		{name: "covered zero files", covered: true},
		{name: "partial replay", allow: true, partial: true, files: true},
		{name: "covered replay", covered: true, files: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]bool{"allow": tc.allow, "partial": tc.partial, "covered": tc.covered, "files": tc.files})
			verdictPath := filepath.Join(t.TempDir(), "verdict")
			body := pythonFakeProcess + `import os, json, tempfile, hashlib
cfg=json.loads(os.environ['TEST_POLICY'])
d=tempfile.mkdtemp()
sites=[]
if cfg['files']:
    os.mkdir(os.path.join(d,'iad'))
    with open(os.path.join(d,'iad','mysql-bin.000001'),'wb') as f: f.write(b'verified bytes')
    sites=[{'site':'iad','stopReached':cfg['covered'],'anchored':cfg['covered'],'stagedStopReached':cfg['covered'],'newestReplayEventTime':'2029-12-31T23:59:50Z','hasReplayTransactions':True,'staged':[{'path':'iad/mysql-bin.000001','sha256':hashlib.sha256(b'verified bytes').hexdigest()}]}]
if cfg['covered'] and not sites:
    sites=[{'site':'iad','stopReached':True,'anchored':True,'stagedStopReached':False,'newestReplayEventTime':'0001-01-01T00:00:00Z','hasReplayTransactions':False,'staged':[]}]
coverage={'version':1,'mode':'timestamp','stopDatetime':'2030-01-01T00:00:00Z','allowPartial':cfg['allow'],'targetCovered':cfg['covered'],'partial':cfg['partial'],'proofSite':'iad' if cfg['covered'] else '','sites':sites}
with open(os.path.join(d,'coverage.json'),'w') as f: json.dump(coverage,f)
certificate=job['_read_pitr_coverage'](d,'2030-01-01 00:00:00')
last,count=job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','',d,'',certificate)
job['_write_pitr_verdict'](certificate,'2030-01-01 00:00:00',last,count)
`
			out, err := runEmbeddedBackupPython(t, restoreScript, body, map[string]string{"TEST_POLICY": string(raw), "BLOODRAVEN_PITR_ALLOW_PARTIAL": strconv.FormatBool(tc.allow), "BLOODRAVEN_PITR_VERDICT_PATH": verdictPath, "BLOODRAVEN_TERMINATION_LOG": filepath.Join(t.TempDir(), "t")})
			if (err != nil) != tc.wantError {
				t.Fatalf("%v: %s", err, out)
			}
			if tc.wantError {
				return
			}
			if strings.Contains(string(out), "BLOODRAVEN_PITR_NOOP") != tc.noop || strings.Contains(string(out), "BLOODRAVEN_PITR_PARTIAL") != tc.partial {
				t.Fatalf("policy output: %s", out)
			}
			verdict, err := os.ReadFile(verdictPath)
			if err != nil {
				t.Fatal(err)
			}
			job := &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{{Name: restorePITRInitContainerName, Env: []corev1.EnvVar{{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: "2030-01-01T00:00:00Z"}, {Name: "BLOODRAVEN_PITR_ALLOW_PARTIAL", Value: strconv.FormatBool(tc.allow)}}}},
				Containers:     []corev1.Container{{Name: backupJobContainerName}},
			}}}}
			pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{terminated(backupJobContainerName, 0, string(verdict))}}}
			parsed, err := pitrJobVerdict(pod, job)
			if err != nil || parsed.Partial != tc.partial || parsed.TargetCovered != tc.covered {
				t.Fatalf("controller rejected restore.py's verdict %q: %+v %v", verdict, parsed, err)
			}
			if tc.files && (parsed.NewestEventTime != "2029-12-31T23:59:50Z" || parsed.Files != 1 || parsed.LastFile != "mysql-bin.000001") {
				t.Fatalf("verdict replay fields: %+v", parsed)
			}
			if len(verdict) > 4096 {
				t.Fatal("verdict exceeds the termination message limit")
			}
		})
	}
}

// mysqlbinlog emits no transaction at all for a year-9999 --stop-datetime,
// so latest replay passes no stop.
func TestPITRScriptLatestOmitsStopDatetime(t *testing.T) {
	for _, mode := range []string{"latest", "timestamp"} {
		body := pythonFakeProcess + `g['_collect_binlogs']=lambda _: ['/pitr/iad/mysql-bin.000001']
coverage={'mode':'` + mode + `','partial':False,'crossSiteUnverified':False}
job['_run_pitr']('db',3306,'root','secret','9999-12-31 23:59:59','','/pitr','',coverage)
has=any(a.startswith('--stop-datetime') for a in calls[0])
assert has == (coverage['mode'] != 'latest'), calls
`
		if out, err := runEmbeddedBackupPython(t, restoreScript, body, nil); err != nil {
			t.Fatalf("%s: %v %s", mode, err, out)
		}
	}
}

func TestPITRScriptReplayExcludesBackupAndExplicitGTIDs(t *testing.T) {
	body := pythonFakeProcess + `g['_collect_binlogs']=lambda _: ['/pitr/iad/mysql-bin.000001']
coverage={'mode':'timestamp','partial':False,'crossSiteUnverified':False}
job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','other:9','/pitr','',coverage)
assert '--exclude-gtids=source:1-3,other:9' in calls[0], calls
`
	out, err := runEmbeddedBackupPython(t, restoreScript, body, map[string]string{"BLOODRAVEN_SOURCE_GTID_EXECUTED": "source:1-3"})
	if err != nil || !strings.Contains(string(out), "BLOODRAVEN_PITR_COMPLETE") {
		t.Fatalf("trusted dump bound not excluded: %v %s", err, out)
	}
}

// The script parses stops without fromisoformat, so every Python version
// agrees with the Go planner, and both reject sub-nanosecond fractions.
func TestPITRScriptStopParsingMatchesPlanner(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, restoreScript, `p=job['_parse_pitr_target']
for value in ['2030-01-01T00:00:00.5Z','2030-01-01T00:00:00.000000001Z','2030-01-01 00:00:00.5','2030-01-01T02:00:00.1+02:00','2030-01-01T00:00:01Z']:
    assert p(value) == '2030-01-01 00:00:01', (value, p(value))
assert p('2030-01-01T00:00:00.000Z') == '2030-01-01 00:00:00'
assert p('2029-12-31T19:00:00-05:00') == '2030-01-01 00:00:00'
for bad in ['2030-01-01T00:00:00.0000000001Z','yesterday']:
    try:
        p(bad)
    except ValueError:
        continue
    raise AssertionError(bad)
`, nil)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for value, want := range map[string]string{"2030-01-01T00:00:00.5Z": "2030-01-01 00:00:01", "2030-01-01T00:00:00.000000001Z": "2030-01-01 00:00:01", "2030-01-01T02:00:00.1+02:00": "2030-01-01 00:00:01"} {
		job := &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: restorePITRInitContainerName, Env: []corev1.EnvVar{{Name: "BLOODRAVEN_PITR_STOP_DATETIME", Value: value}}}}, Containers: []corev1.Container{{Name: backupJobContainerName}}}}}}
		pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{terminated(backupJobContainerName, 0, testVerdict(verdictFields{stop: want, covered: true}))}}}
		if _, err := pitrJobVerdict(pod, job); err != nil {
			t.Fatalf("%s: controller stop normalization differs: %v", value, err)
		}
	}
}

func TestPITRScriptRejectsStagingCorruptionUnderPartial(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, restoreScript, `import tempfile, os, json
local=tempfile.mkdtemp()
os.mkdir(os.path.join(local,'iad'))
with open(os.path.join(local,'iad','mysql-bin.000001'),'wb') as f: f.write(b'changed after download')
coverage={'version':1,'mode':'timestamp','stopDatetime':'2030-01-01T00:00:00Z','allowPartial':True,'partial':True,'targetCovered':False,'sites':[{'site':'iad','stopReached':False,'anchored':False,'stagedStopReached':False,'hasReplayTransactions':True,'newestReplayEventTime':'2029-12-31T23:59:50Z','staged':[{'path':'iad/mysql-bin.000001','sha256':'wrong'}]}]}
with open(os.path.join(local,'coverage.json'),'w') as f: json.dump(coverage,f)
job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','',local,'')
`, map[string]string{"BLOODRAVEN_PITR_ALLOW_PARTIAL": "true", "BLOODRAVEN_TERMINATION_LOG": filepath.Join(t.TempDir(), "t")})
	if err == nil || !strings.Contains(string(out), "staged identity differs") {
		t.Fatalf("staging corruption accepted: %v %s", err, out)
	}
}

// Every coverage refusal is known before the destructive load, so the
// script refuses before it even connects.
func TestPITRScriptValidatesCoverageBeforeLoad(t *testing.T) {
	body := `import types, tempfile, sys
events=[]
class Shell:
    def connect(self, conn): events.append('connect')
class Util:
    def load_dump(self, *a): events.append('load')
job['mysqlsh'].globals=types.SimpleNamespace(shell=Shell(), session=None, util=Util())
try:
    job['main']()
except SystemExit as e:
    assert e.code == 2, e.code
assert events == [], events
`
	out, err := runEmbeddedBackupPython(t, restoreScript, body, map[string]string{
		"BLOODRAVEN_MYSQL_HOST": "db:3306", "BLOODRAVEN_INPUT_URL": "/dump", "MYSQL_USER": "root", "MYSQL_PASSWORD": "x",
		"BLOODRAVEN_PITR_STOP_DATETIME": "2030-01-01T00:00:00Z", "BLOODRAVEN_PITR_LOCAL_DIR": t.TempDir(),
		"BLOODRAVEN_TERMINATION_LOG": filepath.Join(t.TempDir(), "t"),
	})
	if err != nil || !strings.Contains(string(out), "PITRCoverageInvalid") {
		t.Fatalf("coverage was not refused before the load: %v %s", err, out)
	}
}

// pythonGTIDSession answers the GTID functions _verify_loaded_dump uses with
// a small single-purpose GTID set implementation.
const pythonGTIDSession = `def parse(s):
    out={}
    for part in [p for p in (s or '').split(',') if p]:
        sid,*ranges=part.split(':')
        for r in ranges:
            a,_,b=r.partition('-')
            out.setdefault(sid,set()).update(range(int(a),int(b or a)+1))
    return out
def render(m):
    parts=[]
    for sid in sorted(m):
        if m[sid]:
            parts.append(sid+':'+':'.join(str(n) for n in sorted(m[sid])))
    return ','.join(parts)
def subtract(a,b):
    a,b=parse(a),parse(b)
    return render({sid:v-b.get(sid,set()) for sid,v in a.items()})
class Result:
    def __init__(self,v): self.v=v
    def fetch_one(self): return (self.v,)
class Session:
    def __init__(self,executed): self.executed=executed
    def run_sql(self,sql,args=None):
        if sql=='SELECT @@global.gtid_executed': return Result(self.executed)
        if sql=='SELECT GTID_SUBTRACT(?, ?)': return Result(subtract(*args))
        if sql=='SELECT GTID_SUBTRACT(?, GTID_SUBTRACT(?, ?))': return Result(subtract(args[0],subtract(args[1],args[2])))
        raise AssertionError(sql)
`

// The bound is read before mysqlsh's snapshot and may be unknown, so the
// staged replay is checked against the loaded dump before it is applied.
func TestPITRScriptChecksReplayAgainstLoadedDump(t *testing.T) {
	for _, tc := range []struct {
		name, executed, coverage, want string
		unknown                        bool
	}{
		{name: "latest replay already in dump", executed: "a:1-11", want: "PITRNoArchivedReplay",
			coverage: `{'mode':'latest','sites':[{'site':'iad','inWindowGTIDs':'a:11','staged':[{'previousGTIDs':'a:1-10','gtids':'a:11'}]}]}`},
		{name: "latest replay", executed: "a:1-10",
			coverage: `{'mode':'latest','sites':[{'site':'iad','inWindowGTIDs':'a:11','staged':[{'previousGTIDs':'a:1-10','gtids':'a:11'}]}]}`},
		{name: "dump holds post-target commit", executed: "a:1-12", want: "the backup already contains transactions committed at or after the target",
			coverage: `{'mode':'timestamp','sites':[{'site':'iad','postStopGTIDs':'a:12','inWindowGTIDs':'a:11','staged':[]}]}`},
		{name: "unknown bound gap", executed: "a:1-5", unknown: true, want: "archive does not contain transactions between the loaded backup and a:6:7:8:9:10",
			coverage: `{'mode':'timestamp','sites':[{'site':'iad','inWindowGTIDs':'a:11','staged':[{'previousGTIDs':'a:1-10','gtids':'a:11'}]}]}`},
		{name: "unknown bound branch", executed: "a:1-10", unknown: true, want: "does not continue the loaded backup",
			coverage: `{'mode':'timestamp','sites':[{'site':'iad','inWindowGTIDs':'b:1','staged':[{'previousGTIDs':'a:1-5','gtids':'b:1'}]}]}`},
		{name: "unknown bound continues", executed: "a:1-10", unknown: true,
			coverage: `{'mode':'timestamp','sites':[{'site':'iad','inWindowGTIDs':'a:11-12','staged':[{'previousGTIDs':'a:1-5','gtids':'a:6-11'},{'previousGTIDs':'a:1-11','gtids':'a:12'}]}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"BLOODRAVEN_SOURCE_GTID_EXECUTED": "a:1-10", "BLOODRAVEN_TERMINATION_LOG": filepath.Join(t.TempDir(), "t")}
			if tc.unknown {
				delete(env, "BLOODRAVEN_SOURCE_GTID_EXECUTED")
			}
			out, err := runEmbeddedBackupPython(t, restoreScript, pythonGTIDSession+`job['_verify_loaded_dump'](Session('`+tc.executed+`'), `+tc.coverage+`, '')
print('checked')
`, env)
			if tc.want == "" {
				if err != nil || !strings.Contains(string(out), "checked") {
					t.Fatalf("valid replay refused: %v %s", err, out)
				}
				return
			}
			if err == nil || !strings.Contains(string(out), tc.want) {
				t.Fatalf("want %q: %v %s", tc.want, err, out)
			}
		})
	}
}

func TestPITRScriptCoveredIdleNeedsProofSite(t *testing.T) {
	for _, proof := range []string{"iad", ""} {
		out, err := runEmbeddedBackupPython(t, restoreScript, `import tempfile,os,json
local=tempfile.mkdtemp()
coverage={'version':1,'mode':'timestamp','stopDatetime':'2030-01-01T00:00:00Z','allowPartial':False,'partial':False,'targetCovered':True,'proofSite':'`+proof+`','sites':[{'site':'iad','anchored':True,'stopReached':True,'stagedStopReached':False,'hasReplayTransactions':False,'newestReplayEventTime':'0001-01-01T00:00:00Z','staged':[]}]}
with open(os.path.join(local,'coverage.json'),'w') as f:json.dump(coverage,f)
assert job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','',local,'') == ('',0)
`, map[string]string{"BLOODRAVEN_TERMINATION_LOG": filepath.Join(t.TempDir(), "t")})
		if proof != "" && (err != nil || strings.Contains(string(out), "BLOODRAVEN_PITR_NOOP")) {
			t.Fatalf("covered idle rejected or mislabeled: %v %s", err, out)
		}
		if proof == "" && (err == nil || !strings.Contains(string(out), "inconsistent proof site")) {
			t.Fatalf("coverage without a proof site accepted: %v %s", err, out)
		}
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

// Opt-in: BLOODRAVEN_PITR_NATIVE_TEST=1 with the bloodraven-pitr-mysqlbinlog:8.4
// image (internal/sidecar/testdata/native/Dockerfile). The mysqlbinlog command
// line restore.py builds for latest replay must emit the archived transactions.
func TestPITRScriptLatestCommandLineReplaysNatively(t *testing.T) {
	if os.Getenv("BLOODRAVEN_PITR_NATIVE_TEST") != "1" {
		t.Skip("opt-in MySQL 8.4 native oracle")
	}
	fixtures, err := filepath.Abs("../sidecar/testdata/native")
	if err != nil {
		t.Fatal(err)
	}
	body := pythonFakeProcess + `import json
g['_collect_binlogs']=lambda _: ['/fixtures/compressed-session.bin']
for mode in ('latest','timestamp'):
    calls.clear()
    job['_run_pitr']('db',3306,'root','secret','9999-12-31 23:59:59','','/fixtures','',{'mode':mode,'partial':False,'crossSiteUnverified':False})
    print('ARGV '+mode+' '+json.dumps(calls[0][1:]))
`
	out, err := runEmbeddedBackupPython(t, restoreScript, body, nil)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		mode, encoded, ok := strings.Cut(strings.TrimPrefix(line, "ARGV "), " ")
		if !ok || !strings.HasPrefix(line, "ARGV ") {
			continue
		}
		var args []string
		if err := json.Unmarshal([]byte(encoded), &args); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("docker", append([]string{"run", "--rm", "--network", "none", "-e", "TZ=UTC", "-v", fixtures + ":/fixtures:ro", "bloodraven-pitr-mysqlbinlog:8.4", "--no-defaults"}, args...)...)
		sql, err := cmd.Output()
		if err != nil {
			t.Fatalf("native mysqlbinlog %v: %v", args, err)
		}
		transactions := strings.Count(string(sql), "SET @@SESSION.GTID_NEXT= '")
		// The year-9999 stop makes mysqlbinlog emit no transaction at all.
		if (mode == "latest") != (transactions >= 2) {
			t.Fatalf("%s replay emitted %d transactions with %v", mode, transactions, args)
		}
	}
}
