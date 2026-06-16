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

	// 09-network-partition-self-fence is quarantined (issue #93), so it is
	// excluded from the release profile (12 members - 1 quarantined = 11).
	release := runner.SelectForProfile(all, runner.ProfileRelease)
	if len(release) != 11 {
		t.Fatalf("release profile selected %d scenarios, want 11 (09 quarantined)", len(release))
	}

	// Quarantined scenarios are excluded from full as well.
	quarantined := 0
	for _, s := range all {
		if s.Quarantine != "" {
			quarantined++
		}
	}
	if quarantined == 0 {
		t.Fatal("expected at least one quarantined scenario (09-network-partition-self-fence)")
	}
	full := runner.SelectForProfile(all, runner.ProfileFull)
	if len(full) != len(all)-quarantined {
		t.Fatalf("full profile selected %d scenarios, want %d (all %d minus %d quarantined)",
			len(full), len(all)-quarantined, len(all), quarantined)
	}

	// No quarantined scenario may appear in any batch profile.
	for _, batch := range [][]runner.Scenario{smoke, release, full} {
		for _, s := range batch {
			if s.Quarantine != "" {
				t.Errorf("quarantined scenario %q must not appear in a batch profile", s.ID)
			}
		}
	}
}
