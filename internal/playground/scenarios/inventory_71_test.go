package scenarios

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// Scenario 71 is registered, not quarantined, resets the cluster before a
// run-all, and stays in the full profile only until it has run on a real
// cluster once.
func TestScenario71RegisteredAndProfiled(t *testing.T) {
	s, ok := runner.DefaultRegistry.Get(s71ID)
	if !ok {
		t.Fatalf("scenario %s is not registered", s71ID)
	}
	if s.Quarantine != "" || s.Timeout <= 0 || !s.ResetBeforeRunAll || s.Cleanup == nil {
		t.Fatalf("scenario %s: quarantine=%q timeout=%s reset=%v cleanup=%v", s71ID, s.Quarantine, s.Timeout, s.ResetBeforeRunAll, s.Cleanup != nil)
	}
	all := runner.DefaultRegistry.List()
	if !inventoryContains(runner.SelectForProfile(all, runner.ProfileFull), s71ID) {
		t.Errorf("scenario %s must appear in the full profile", s71ID)
	}
	for _, p := range []runner.Profile{runner.ProfileSmoke, runner.ProfileRelease} {
		if inventoryContains(runner.SelectForProfile(all, p), s71ID) {
			t.Errorf("scenario %s must not be in the %s profile before its first run on a cluster", s71ID, p)
		}
	}
}

func TestS71NewestOwn(t *testing.T) {
	const a, b = "aaaaaaaa-0000-11f1-8000-000000000001", "bbbbbbbb-0000-11f1-8000-000000000002"
	tests := []struct {
		name    string
		full    string
		uuid    string
		n       int64
		want    string
		wantErr bool
	}{
		{"the tail of the only interval", a + ":1-20", a, 3, a + ":18-20", false},
		{"other servers are ignored", a + ":1-20," + b + ":1-900", a, 3, a + ":18-20", false},
		{"the tail of the last interval", a + ":1-5:9-20", a, 3, a + ":18-20", false},
		{"an upper-case uuid", a + ":1-20", "AAAAAAAA-0000-11F1-8000-000000000001", 2, a + ":19-20", false},
		{"too few in a row", a + ":1-20:25-26", a, 3, "", true},
		{"exactly as many as it would lose", a + ":1-3", a, 3, "", true},
		{"a server that issued nothing", b + ":1-20", a, 3, "", true},
		{"an unparsable set", "not a set", a, 3, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s71NewestOwn(tt.full, tt.uuid, tt.n)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("s71NewestOwn = %q, %v; want %q, error %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}
