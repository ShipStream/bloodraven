package scenarios

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func TestScenario49RegisteredAndFullProfiled(t *testing.T) {
	const id = "49-tenant-database-failover"

	scenario, ok := runner.DefaultRegistry.Get(id)
	if !ok {
		t.Fatalf("scenario %s is not registered", id)
	}
	if scenario.Quarantine != "" {
		t.Fatalf("scenario %s must not be quarantined: %q", id, scenario.Quarantine)
	}
	if scenario.Timeout <= 0 || scenario.Precheck == nil || scenario.Cleanup == nil || len(scenario.Steps) == 0 {
		t.Fatalf("scenario %s is incomplete: timeout=%s precheck=%v cleanup=%v steps=%d",
			id, scenario.Timeout, scenario.Precheck != nil, scenario.Cleanup != nil, len(scenario.Steps))
	}
	// The scenario creates and drops a real tenant database, so it must have
	// a Cleanup that runs even on failure — asserted above — and it is
	// deliberately full-profile only until it has run green in CI a few
	// times. Promoting it to release is a separate, deliberate change.
	if !inventoryContains(runner.SelectForProfile(runner.DefaultRegistry.List(), runner.ProfileFull), id) {
		t.Fatalf("scenario %s is not selected by the full profile", id)
	}
	if inventoryContains(runner.SelectForProfile(runner.DefaultRegistry.List(), runner.ProfileSmoke), id) {
		t.Fatalf("scenario %s must not be in the smoke profile", id)
	}
}
