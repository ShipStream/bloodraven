package doctorscripts

import (
	"os/exec"
	"strings"
	"testing"
)

// sidecar-probe.sh keyring summarizes the /keyring/status coverage flags.
// A flag the sidecar omitted (MySQL did not report it) is "not-reported",
// never false; a reported false stays false.
func TestSidecarProbeKeyringCoverageAbsentIsNotReported(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not on PATH")
	}
	for _, tc := range []struct {
		name, body, want string
	}{
		{
			name: "partial report",
			body: `{"enabled":true,"present":true,"coverage":{"systemTablespaceEncrypted":true,"unencryptedTablespaces":2,"redoLogEncrypted":false}}`,
			want: "coverage: systemTablespaceEncrypted=true redoLogEncrypted=false undoLogEncrypted=not-reported binlogEncrypted=not-reported unencryptedTablespaces=2",
		},
		{
			name: "no coverage block",
			body: `{"enabled":true,"present":false}`,
			want: "coverage: not reported (no coverage block)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newRuleEnv(t)
			e.rule(t, "get pods -n orders -l app.kubernetes.io/instance=lion -o jsonpath=*", "mysql-lion-dc1-0", false)
			e.rule(t, "get pod mysql-lion-dc1-0 -n orders -o jsonpath=*", "sidecar", false)
			e.rule(t, "exec -n orders mysql-lion-dc1-0 -c sidecar -- wget *", tc.body, false)
			out, err := e.run(t, scriptPath("sidecar-probe.sh"), "-n", "orders", "lion", "keyring")
			if err != nil {
				t.Fatalf("sidecar-probe.sh failed: %v\n%s", err, out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("output lacks %q:\n%s", tc.want, out)
			}
			if strings.Contains(out, "undoLogEncrypted=false") {
				t.Errorf("an omitted flag was reported as false:\n%s", out)
			}
		})
	}
}

// sidecar-probe.sh archiver prints the raw /archiver/status and summarizes
// its tail block (#264). A status without the block (no scan has read the
// binlog index yet, or an older sidecar) is "not reported", never Drained.
func TestSidecarProbeArchiverTail(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not on PATH")
	}
	for _, tc := range []struct {
		name, body, want string
	}{
		{
			name: "blocked read-only site",
			body: `{"enabled":true,"primary":false,"backlogFiles":0,"tail":{"state":"Blocked","activeFile":"mysql-bin.000012","newestSealedFile":"mysql-bin.000011","pendingFiles":2,"reason":"this site's manifest holds no promotion record"}}`,
			want: "tail: state=Blocked activeFile=mysql-bin.000012 newestSealedFile=mysql-bin.000011 pendingFiles=2 reason=this site's manifest holds no promotion record\n",
		},
		{
			name: "drained with a single binlog",
			body: `{"enabled":true,"primary":true,"tail":{"state":"Drained","activeFile":"mysql-bin.000001","pendingFiles":0}}`,
			want: "tail: state=Drained activeFile=mysql-bin.000001 newestSealedFile=none pendingFiles=0\n",
		},
		{
			name: "no tail block",
			body: `{"enabled":true,"primary":false,"backlogFiles":0}`,
			want: "tail: not reported (no scan has read the binlog index yet, or the sidecar predates the tail block)",
		},
		{
			name: "archiver disabled",
			body: `{"enabled":false}`,
			want: "tail: not reported (no archiver runs on this pod)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newRuleEnv(t)
			e.rule(t, "get pods -n orders -l app.kubernetes.io/instance=lion -o jsonpath=*", "mysql-lion-dc1-0", false)
			e.rule(t, "get pod mysql-lion-dc1-0 -n orders -o jsonpath=*", "sidecar", false)
			e.rule(t, "exec -n orders mysql-lion-dc1-0 -c sidecar -- wget *", tc.body, false)
			out, err := e.run(t, scriptPath("sidecar-probe.sh"), "-n", "orders", "lion", "archiver")
			if err != nil {
				t.Fatalf("sidecar-probe.sh failed: %v\n%s", err, out)
			}
			if !strings.Contains(out, tc.body) {
				t.Errorf("output lacks the raw status:\n%s", out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("output lacks %q:\n%s", tc.want, out)
			}
			if tc.name != "drained with a single binlog" && strings.Contains(out, "state=Drained") {
				t.Errorf("a status without a drained tail was summarized as Drained:\n%s", out)
			}
		})
	}
}
