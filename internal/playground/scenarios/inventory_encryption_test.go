package scenarios

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// encryptionScenarioIDs are the scenarios that require the dedicated
// TLS + spec.encryptionAtRest playground produced by
// ./playground/enable-encryption.sh. They are deliberately kept out of
// every batch profile: run-all against the shared baseline would hit
// their Precheck, and --auto-reset would then escalate to a destructive
// MySQL wipe of a cluster they were never meant to touch.
var encryptionScenarioIDs = []string{
	"48-keyring-seal-and-rotation",
	"50-encrypted-pod-replace",
	"51-encrypted-reclone",
	"52-rotation-promotion-refused",
}

func TestEncryptionScenariosRegisteredAndQuarantined(t *testing.T) {
	for _, id := range encryptionScenarioIDs {
		t.Run(id, func(t *testing.T) {
			scenario, ok := runner.DefaultRegistry.Get(id)
			if !ok {
				t.Fatalf("scenario %s is not registered", id)
			}
			if scenario.Quarantine == "" {
				t.Fatalf("scenario %s must stay quarantined — it needs the encryption baseline, "+
					"and letting run-all reach it invites an --auto-reset data wipe", id)
			}
			if scenario.Timeout <= 0 || scenario.Precheck == nil || scenario.Cleanup == nil || len(scenario.Steps) == 0 {
				t.Fatalf("scenario %s is incomplete: timeout=%s precheck=%v cleanup=%v steps=%d",
					id, scenario.Timeout, scenario.Precheck != nil, scenario.Cleanup != nil, len(scenario.Steps))
			}
			if scenario.Hypothesis == "" || scenario.DocLink == "" {
				t.Errorf("scenario %s is missing a hypothesis or doc link", id)
			}
			for _, profile := range runner.Profiles() {
				if inventoryContains(runner.SelectForProfile(runner.DefaultRegistry.List(), profile), id) {
					t.Errorf("quarantined scenario %s is selected by the %s profile", id, profile)
				}
			}
		})
	}
}

// TestEncryptionScenariosCoverTheChaosPlanMinimum pins the mapping from
// .tmp/chaos-plan-v1.0.md's "minimum CI experiment set to close #120"
// onto real scenario IDs. Deleting or renaming one of these without
// replacing its coverage should fail here rather than quietly shrinking
// what the encryption CI job proves.
func TestEncryptionScenariosCoverTheChaosPlanMinimum(t *testing.T) {
	required := map[string]string{
		"48-keyring-seal-and-rotation":  "EXP-15: replica rotation happy path + primary rotation refused",
		"50-encrypted-pod-replace":      "EXP-12/EXP-04: sealed pod replace re-projects the escrow and the data decrypts",
		"51-encrypted-reclone":          "EXP-06: clone recipient unseals before CLONE INSTANCE, then re-seals on an escrow no older than the pre-clone version",
		"52-rotation-promotion-refused": "EXP-01/EXP-16 inverse: planned and emergency promotion of UnsealReason=Rotation is refused",
	}
	for id, why := range required {
		if _, ok := runner.DefaultRegistry.Get(id); !ok {
			t.Errorf("scenario %s is missing; it is the only coverage for %s", id, why)
		}
	}
}
