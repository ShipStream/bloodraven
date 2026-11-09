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
