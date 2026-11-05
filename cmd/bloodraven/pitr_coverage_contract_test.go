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

// Only the replaying site can certify coverage. Another site's stop barrier,
// which applies nothing before the target, is left out of the replay.
func TestPITROnlyReplayingSiteCertifiesCoverage(t *testing.T) {
	writer := joinBinlogFixtures(t, retagGTIDFixture(t, coverageStream(t, 10, coverageTime(9, 0, 0), false, false), 1), retagGTIDFixture(t, coverageStream(t, 10, coverageTime(9, 10, 0), false, false), 2))
	for _, shape := range []string{"staged barrier", "idle barrier"} {
		for _, nonwriter := range []string{"iad", "zzz"} {
			t.Run(shape+" "+nonwriter, func(t *testing.T) {
				other := coverageStream(t, 10, coverageTime(10, 0, 0), shape == "idle barrier", false)
				store := coverageArchive(t, map[string][][]byte{"pdx": {writer}, nonwriter: {other}})
				result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"pdx", nonwriter}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", false, false, "timestamp", slog.Default())
				if err == nil || !strings.Contains(err.Error(), "PITRTargetNotCovered") || result.TargetCovered {
					t.Fatalf("another site's barrier certified the writer's missing tail: %+v %v", result, err)
				}
				result, err = downloadPITRArchive(context.Background(), store, "binlogs", []string{"pdx", nonwriter}, coverageTime(9, 30, 0), t.TempDir(), retentionTestSID+":1-10", true, false, "timestamp", slog.Default())
				if err != nil || result.TargetCovered || !result.Partial || result.CrossSiteUnverified {
					t.Fatalf("partial single-site replay refused: %+v %v", result, err)
				}
				for _, site := range result.Sites {
					if (site.Site == "pdx") != (len(site.Staged) > 0) {
						t.Fatalf("only the writer may be replayed: %+v", result.Sites)
					}
				}
			})
		}
	}
}

func TestPITRUnverifiedCoverageScriptContract(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required for the restore coverage contract")
	}
	old := coverageStream(t, 10, coverageTime(9, 0, 0), false, false)
	primary := joinBinlogFixtures(t, retagGTIDFixture(t, coverageStream(t, 11, coverageTime(9, 10, 0), false, false), 1), retagGTIDFixture(t, coverageStream(t, 11, coverageTime(9, 40, 0), false, false), 2))
	store := coverageArchive(t, map[string][][]byte{"iad": {old}, "pdx": {old, primary}})
	out := t.TempDir()
	result, err := downloadPITRArchive(context.Background(), store, "binlogs", []string{"iad", "pdx"}, coverageTime(9, 30, 0), out, retentionTestSID+":1-10", false, true, "timestamp", slog.Default())
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
