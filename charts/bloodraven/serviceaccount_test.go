package bloodraven_test

import (
	"os/exec"
	"strings"
	"testing"
)

func TestServiceAccountChart(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is required for chart render tests")
	}
	for _, tt := range []struct {
		name, values, wantError, wantSubject string
	}{
		{name: "created by default", wantSubject: "name: test-bloodraven"},
		{name: "existing account by name", values: "serviceAccount:\n  create: false\n  name: bloodraven-operator\n", wantSubject: "name: bloodraven-operator"},
		{name: "existing account needs a name", values: "serviceAccount:\n  create: false\n", wantError: "serviceAccount.name is required when serviceAccount.create=false"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("helm", "template", "test", ".", "--namespace", "operator-system", "--values", "-")
			cmd.Stdin = strings.NewReader(tt.values)
			out, err := cmd.CombinedOutput()
			if tt.wantError != "" {
				if err == nil || !strings.Contains(string(out), tt.wantError) {
					t.Fatalf("expected %q error, got %v: %s", tt.wantError, err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			if !strings.Contains(string(out), "- kind: ServiceAccount\n    "+tt.wantSubject+"\n") {
				t.Fatalf("ClusterRoleBinding subject %q not rendered:\n%s", tt.wantSubject, out)
			}
		})
	}
}
