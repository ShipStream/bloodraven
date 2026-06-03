package runner_test

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/playground/runner"
	_ "github.com/shipstream/bloodraven/internal/playground/scenarios"
)

func TestProfilesSelectRegisteredScenarios(t *testing.T) {
	all := runner.DefaultRegistry.List()
	if len(all) == 0 {
		t.Fatal("no scenarios registered")
	}

	smoke := runner.SelectForProfile(all, runner.ProfileSmoke)
	if len(smoke) != 3 {
		t.Fatalf("smoke profile selected %d scenarios, want 3", len(smoke))
	}

	release := runner.SelectForProfile(all, runner.ProfileRelease)
	if len(release) != 12 {
		t.Fatalf("release profile selected %d scenarios, want 12", len(release))
	}

	full := runner.SelectForProfile(all, runner.ProfileFull)
	if len(full) != len(all) {
		t.Fatalf("full profile selected %d scenarios, want all %d", len(full), len(all))
	}
}
