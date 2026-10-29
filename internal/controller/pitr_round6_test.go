package controller

import (
	"strings"
	"testing"
)

func TestRound6ReplayExcludesBackupAndExplicitGTIDs(t *testing.T) {
	body := `import io
calls=[]
class Process:
    def __init__(self,args,**kwargs):
        calls.append(args)
        self.stdout=io.BytesIO()
    def wait(self): return 0
g=job['_run_pitr'].__globals__
g['_collect_binlogs']=lambda _: ['/pitr/iad/mysql-bin.000001']
g['subprocess'].Popen=Process
coverage={'partial':False,'crossSiteUnverified':False}
job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','other:9','/pitr','',coverage)
assert '--exclude-gtids=source:1-3,other:9' in calls[0], calls
`
	out, err := runEmbeddedBackupPython(t, restoreScript, body, map[string]string{"BLOODRAVEN_SOURCE_GTID_EXECUTED": "source:1-3"})
	if err != nil || !strings.Contains(string(out), "BLOODRAVEN_PITR_COMPLETE") {
		t.Fatalf("trusted dump bound not excluded: %v %s", err, out)
	}
}
