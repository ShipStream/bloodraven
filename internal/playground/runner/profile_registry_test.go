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

	// The release subset lists 20 members, all of which are now selected:
	// 31-pitr-verification-rustfs is no longer quarantined (#101 fixed — the
	// verify mysqld runs gtid_mode=ON and server-side dedup handles the PITR
	// replay), and the issue #115 reader scenarios (40-44) are release-graded.
	// The four chaos-round 2026-10-15 regressions bring it to 24, and the
	// #235 planned-failover claim restart scenario (63) to 25, and the three
	// #206 cross-site PITR histories (37, 68, 70) to 28, and the paced
	// rollout halt (73, #244) to 29.
	release := runner.SelectForProfile(all, runner.ProfileRelease)
	if len(release) != 29 {
		t.Fatalf("release profile selected %d scenarios, want 29", len(release))
	}
	if !containsScenarioID(release, "73-paced-rollout-bad-build-halt") {
		t.Error("release profile must include 73-paced-rollout-bad-build-halt (#244)")
	}
	if containsScenarioID(smoke, "73-paced-rollout-bad-build-halt") {
		t.Error("73-paced-rollout-bad-build-halt must not be in the smoke profile")
	}
	// The upgrade E2E needs a cluster installed at the previous release; the
	// e2e upgrade job names it, and no profile selects it.
	if containsScenarioID(runner.SelectForProfile(all, runner.ProfileFull), "74-upgrade-paced-rollout") {
		t.Error("74-upgrade-paced-rollout must not be selected by any profile")
	}
	for _, id := range []string{
		"37-pitr-planned-failback-no-reclone",
		"68-pitr-failback-with-reclone",
		"70-pitr-lossy-failover",
	} {
		if !containsScenarioID(release, id) {
			t.Errorf("release profile must include %s (#206)", id)
		}
		if containsScenarioID(smoke, id) {
			t.Errorf("%s must not be in the smoke profile", id)
		}
	}
	// #203's archive encryption binding scenario runs in the e2e pitr job
	// and the full profile only (IMPL-203 Δ19).
	if containsScenarioID(release, "69-pitr-encryption-binding-refusal") || containsScenarioID(smoke, "69-pitr-encryption-binding-refusal") {
		t.Error("69-pitr-encryption-binding-refusal must not be in the smoke or release profile")
	}
	if !containsScenarioID(runner.SelectForProfile(all, runner.ProfileFull), "69-pitr-encryption-binding-refusal") {
		t.Error("69-pitr-encryption-binding-refusal must be in the full profile")
	}
	for _, id := range []string{
		"09-anti-flap-cooldown",
		"57-hung-primary-detected-and-fenced",
		"58-hung-reader-does-not-slow-failover",
		"59-killed-clone-never-promoted",
	} {
		if !containsScenarioID(release, id) {
			t.Errorf("release profile must include %s", id)
		}
	}
	for _, id := range []string{
		"60-operator-only-partition-no-acked-loss",
		"61-reader-ahead-of-new-primary-recovers",
	} {
		if containsScenarioID(release, id) || containsScenarioID(runner.SelectForProfile(all, runner.ProfileFull), id) {
			t.Errorf("%s is quarantined and must not be selected by any profile", id)
		}
	}
	if !containsScenarioID(release, "09-network-partition-self-fence") {
		t.Error("release profile must include 09-network-partition-self-fence (no longer quarantined)")
	}
	if !containsScenarioID(release, "31-pitr-verification-rustfs") {
		t.Error("31-pitr-verification-rustfs is no longer quarantined (#101) and must be in the release profile")
	}
	for _, id := range []string{
		"30-encrypted-backup-verification-rustfs",
		"40-reader-data-loss-reclone",
		"41-reader-availability-during-failover",
		"42-reader-stall-no-group-degradation",
		"43-writable-reader-fence",
		"44-reader-source-convergence-invariant",
		"53-deployment-hold-expiry",
		"55-deployment-emergency-revocation",
		"63-planned-failover-operator-restart-mid-lag",
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
