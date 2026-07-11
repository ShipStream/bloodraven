package scenarios

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// scenarios3239 is the exhaustive list of the scenarios added in this work
// package. Kept in one place so the inventory test, the docs, and the profile
// choices stay in lock-step.
var scenarios3239 = []string{
	"32-mfg-status-write-denial-emergency-promotion",
	"33-scoped-dns-outage",
	"34-operator-kill-during-wait-replica",
	"35-planned-switchover-lag-timeout-rollback",
	"36-rustfs-outage-during-restore-in-place",
	"37-pitr-archive-handoff-across-failover",
	"38-dnsendpoint-write-denial-during-failover",
	"39-dragonfly-master-partition",
}

// resetBeforeRunAll3239 records which of the new scenarios must start from a
// pristine playground in run-all (destructive backup/restore/PITR state).
var resetBeforeRunAll3239 = map[string]bool{
	"36-rustfs-outage-during-restore-in-place": true,
	"37-pitr-archive-handoff-across-failover":  true,
}

func TestScenarios3239RegisteredAndProfiled(t *testing.T) {
	all := runner.DefaultRegistry.List()
	full := runner.SelectForProfile(all, runner.ProfileFull)
	smoke := runner.SelectForProfile(all, runner.ProfileSmoke)
	release := runner.SelectForProfile(all, runner.ProfileRelease)

	for _, id := range scenarios3239 {
		s, ok := runner.DefaultRegistry.Get(id)
		if !ok {
			t.Errorf("scenario %s is not registered", id)
			continue
		}
		if s.Quarantine != "" {
			t.Errorf("scenario %s must not be quarantined: %q", id, s.Quarantine)
		}
		if s.Timeout <= 0 {
			t.Errorf("scenario %s must set a positive Timeout", id)
		}
		if !inventoryContains(full, id) {
			t.Errorf("scenario %s must appear in the full profile", id)
		}
		// Full-profile-only by allowlist omission: never in smoke or release.
		if inventoryContains(smoke, id) {
			t.Errorf("scenario %s must NOT be in the smoke profile (full-only)", id)
		}
		if inventoryContains(release, id) {
			t.Errorf("scenario %s must NOT be in the release profile (full-only)", id)
		}
		if s.ResetBeforeRunAll != resetBeforeRunAll3239[id] {
			t.Errorf("scenario %s ResetBeforeRunAll=%v, want %v", id, s.ResetBeforeRunAll, resetBeforeRunAll3239[id])
		}
	}
}

func inventoryContains(scens []runner.Scenario, id string) bool {
	for _, s := range scens {
		if s.ID == id {
			return true
		}
	}
	return false
}
