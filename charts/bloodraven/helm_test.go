package bloodraven_test

import (
	"os"
	"os/exec"
	"testing"
)

// requireHelm skips a chart render test when helm is missing, except in CI,
// where the job installs helm and a skip would hide a broken chart.
func requireHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("helm is required for chart render tests in CI: %v", err)
		}
		t.Skip("helm is required for chart render tests")
	}
}
