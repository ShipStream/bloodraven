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
	if len(smoke) != 4 {
		t.Fatalf("smoke profile selected %d scenarios, want 4", len(smoke))
	}
	if !containsScenarioID(smoke, "42-reader-stall-no-group-degradation") {
		t.Error("smoke profile must include 42-reader-stall-no-group-degradation (issue #115 R4)")
	}

	// The release subset lists 17 members, all of which are now selected:
	// 31-pitr-verification-rustfs is no longer quarantined (#101 fixed — the
	// verify mysqld runs gtid_mode=ON and server-side dedup handles the PITR
	// replay), and the issue #115 reader scenarios (40-44) are release-graded.
	release := runner.SelectForProfile(all, runner.ProfileRelease)
	if len(release) != 17 {
		t.Fatalf("release profile selected %d scenarios, want 17", len(release))
	}
	if !containsScenarioID(release, "09-network-partition-self-fence") {
		t.Error("release profile must include 09-network-partition-self-fence (no longer quarantined)")
	}
	if !containsScenarioID(release, "31-pitr-verification-rustfs") {
		t.Error("31-pitr-verification-rustfs is no longer quarantined (#101) and must be in the release profile")
	}
	for _, id := range []string{
		"40-reader-data-loss-reclone",
		"41-reader-availability-during-failover",
		"42-reader-stall-no-group-degradation",
		"43-writable-reader-fence",
		"44-reader-source-convergence-invariant",
	} {
		if !containsScenarioID(release, id) {
			t.Errorf("%s must be in the release profile", id)
		}
	}

	// full = every registered scenario minus any that are quarantined. Computed
	// from the live quarantine state so this stays correct regardless of how
	// many scenarios are quarantined at any given time.
	quarantined := 0
	for _, s := range all {
		if s.Quarantine != "" {
			quarantined++
		}
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

// TestSelectForProfileExcludesQuarantined exercises the quarantine mechanism
// directly with synthetic scenarios, so coverage of the exclusion does not
// depend on any real scenario currently being quarantined.
func TestSelectForProfileExcludesQuarantined(t *testing.T) {
	scenarios := []runner.Scenario{
		{ID: "healthy"},
		{ID: "broken", Quarantine: "tracked in #999"},
	}
	full := runner.SelectForProfile(scenarios, runner.ProfileFull)
	if containsScenarioID(full, "broken") {
		t.Error("quarantined scenario must be excluded from the full profile")
	}
	if !containsScenarioID(full, "healthy") {
		t.Error("non-quarantined scenario must be included in the full profile")
	}
}

func containsScenarioID(scenarios []runner.Scenario, id string) bool {
	for _, s := range scenarios {
		if s.ID == id {
			return true
		}
	}
	return false
}
