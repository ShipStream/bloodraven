package scenarios

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// Scenario 62 (#212) is destructive (it fails a full-instance restore after
// its DROP) and slow, so it runs in the full profile only, from a reset.
func TestScenario62RegisteredAndProfiled(t *testing.T) {
	s, ok := runner.DefaultRegistry.Get(s62ID)
	if !ok {
		t.Fatalf("scenario %s is not registered", s62ID)
	}
	if s.Quarantine != "" || s.Timeout <= 0 || !s.ResetBeforeRunAll {
		t.Fatalf("scenario %s: quarantine=%q timeout=%s reset=%v", s62ID, s.Quarantine, s.Timeout, s.ResetBeforeRunAll)
	}
	all := runner.DefaultRegistry.List()
	if !inventoryContains(runner.SelectForProfile(all, runner.ProfileFull), s62ID) {
		t.Errorf("scenario %s must appear in the full profile", s62ID)
	}
	for _, p := range []runner.Profile{runner.ProfileSmoke, runner.ProfileRelease} {
		if inventoryContains(runner.SelectForProfile(all, p), s62ID) {
			t.Errorf("scenario %s must not be in the %s profile", s62ID, p)
		}
	}
}
