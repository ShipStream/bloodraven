package main

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRound2StagedNonwriterAfterWriter(t *testing.T) {
	writer := round2Join(t, round2Retag(t, round1Stream(t, 10, round1Time(9, 0, 0), false, false), 1), round2Retag(t, round1Stream(t, 10, round1Time(9, 10, 0), false, false), 2))
	nonwriter := round1Stream(t, 10, round1Time(10, 0, 0), false, false)
	store := round1Archive(t, map[string][][]byte{"pdx": {writer}, "zzz": {nonwriter}})
	for _, partial := range []bool{false, true} {
		result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"pdx", "zzz"}, round1Time(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", partial, false, "timestamp", slog.Default())
		if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || result.TargetCovered {
			t.Fatalf("later nonwriter's staged barrier certified missing writer history: %+v %v", result, err)
		}
	}
}

func TestRound3UnverifiedCoverageScriptContract(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required for the restore coverage contract")
	}
	old := round1Stream(t, 10, round1Time(9, 0, 0), false, false)
	primary := round2Join(t, round2Retag(t, round1Stream(t, 11, round1Time(9, 10, 0), false, false), 1), round2Retag(t, round1Stream(t, 11, round1Time(9, 40, 0), false, false), 2))
	store := round1Archive(t, map[string][][]byte{"iad": {old}, "pdx": {old, primary}})
	out := t.TempDir()
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, round1Time(9, 30, 0), out, retentionTestSID+":1-10", false, true, "timestamp", slog.Default())
	if err != nil || !result.TargetCovered {
		t.Fatalf("unverified replay planning: %+v %v", result, err)
	}
	raw, err := os.ReadFile(filepath.Join(out, "coverage.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"crossSiteUnverified":true`) || strings.Contains(string(raw), `"pruned"`) {
		t.Fatalf("missing unverified outcome: %s", raw)
	}

	cmd := exec.Command(python, "-c", `import runpy,sys,types,io
sys.modules['mysqlsh']=types.ModuleType('mysqlsh')
job=runpy.run_path(sys.argv[1])
coverage=job['_read_pitr_coverage'](sys.argv[2],'2026-04-15 09:30:00')
assert coverage['targetCovered'] and not coverage['partial']
assert coverage['newestReplayedEventTime']
assert coverage['crossSiteUnverified']
assert len(job['_collect_binlogs'](sys.argv[2])) == 3
calls=[]
class Process:
    def __init__(self,args,**kwargs):
        calls.append(args)
        self.stdout=io.BytesIO()
    def wait(self): return 0
job['_run_pitr'].__globals__['subprocess'].Popen=Process
last,count=job['_run_pitr']('db',3306,'root','secret','2026-04-15 09:30:00','',sys.argv[2],'',coverage)
assert count==3 and len(calls)==2
assert calls[0][0]=='mysqlbinlog' and calls[0][-3:]==job['_collect_binlogs'](sys.argv[2])
assert calls[1][0]=='mysql'
job['_print_restore_complete'].__globals__['_target_coordinates']=lambda _: ('target:1','target-binlog',42)
job['_print_restore_complete'](None,'2026-04-15 09:30:00',last,count,coverage)
print('all replay sites retained with an unverified outcome')
`, "../../internal/controller/restore_script.py", out)
	cmd.Env = append(os.Environ(), "BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE=true")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("actual restore script rejected planner output: %v %s", err, output)
	}
	if !strings.Contains(string(output), "pitrCrossSiteUnverified=true pitrReplaySites=iad%2Cpdx") || !strings.Contains(string(output), "cross-site completeness was not verified") {
		t.Fatalf("unverified script outcome lost: %s", output)
	}
}
