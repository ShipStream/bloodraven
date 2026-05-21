package runner

import (
	"testing"
)

func TestProfileIsValid(t *testing.T) {
	for _, p := range []Profile{ProfileSmoke, ProfileRelease, ProfileFull} {
		if !p.IsValid() {
			t.Errorf("expected %q to be valid", p)
		}
	}
	for _, p := range []Profile{"unknown", "", "partial"} {
		if p.IsValid() {
			t.Errorf("expected %q to be invalid", p)
		}
	}
}

func TestProfilesReturnsAll(t *testing.T) {
	got := Profiles()
	if len(got) != 3 {
		t.Fatalf("Profiles() returned %d entries, want 3", len(got))
	}
	want := map[Profile]bool{ProfileSmoke: true, ProfileRelease: true, ProfileFull: true}
	for _, p := range got {
		if !want[p] {
			t.Errorf("unexpected profile %q", p)
		}
	}
}

func TestSelectForProfileFull(t *testing.T) {
	all := []Scenario{
		{ID: "01-clean-primary-kill"},
		{ID: "02-planned-switchover"},
		{ID: "06-self-fence-isolated-primary"},
	}
	got := SelectForProfile(all, ProfileFull)
	if len(got) != 3 {
		t.Fatalf("ProfileFull: got %d scenarios, want 3", len(got))
	}
	// Empty string defaults to full
	got2 := SelectForProfile(all, "")
	if len(got2) != 3 {
		t.Fatalf("empty profile: got %d scenarios, want 3", len(got2))
	}
}

func TestSelectForProfileSmoke(t *testing.T) {
	all := []Scenario{
		{ID: "01-clean-primary-kill"},
		{ID: "02-planned-switchover"},
		{ID: "02-operator-kill-restart"},
		{ID: "06-self-fence-isolated-primary"},
		{ID: "10-full-bootstrap-after-data-wipe"},
	}
	got := SelectForProfile(all, ProfileSmoke)
	if len(got) != 3 {
		t.Fatalf("ProfileSmoke: got %d scenarios, want 3; got %v", len(got), ids(got))
	}
	for _, s := range got {
		if !smokeScenarios[s.ID] {
			t.Errorf("ProfileSmoke returned unexpected scenario %q", s.ID)
		}
	}
}

func TestSelectForProfileRelease(t *testing.T) {
	all := []Scenario{
		{ID: "01-clean-primary-kill"},
		{ID: "02-planned-switchover"},
		{ID: "02-operator-kill-restart"},
		{ID: "04-data-integrity-on-failover"},
		{ID: "05-operator-kill-during-failover"},
		{ID: "06-self-fence-isolated-primary"},
		{ID: "09-network-partition-self-fence"},
		{ID: "10-full-bootstrap-after-data-wipe"},
		{ID: "12-old-primary-recovery-no-divergence"},
		{ID: "23-failover-state-durability"},
		{ID: "05-split-brain-auto-resolve"}, // not in release
	}
	got := SelectForProfile(all, ProfileRelease)
	if len(got) != 10 {
		t.Fatalf("ProfileRelease: got %d scenarios, want 10; got %v", len(got), ids(got))
	}
	for _, s := range got {
		if !releaseScenarios[s.ID] {
			t.Errorf("ProfileRelease returned unexpected scenario %q", s.ID)
		}
	}
}

func TestSelectForProfileReleaseIncludesSmoke(t *testing.T) {
	// Every smoke scenario must also be in the release profile.
	for id := range smokeScenarios {
		if !releaseScenarios[id] {
			t.Errorf("smoke scenario %q is not in release profile", id)
		}
	}
}

func TestSelectForProfileUnknownAllowlistIgnoresMissing(t *testing.T) {
	// If the allowlist references an ID that doesn't exist in the
	// scenario list, SelectForProfile silently skips it (no panic,
	// no error). This makes adding new scenarios to a profile safe
	// even before the scenario is registered.
	all := []Scenario{{ID: "01-clean-primary-kill"}}
	got := SelectForProfile(all, ProfileSmoke)
	if len(got) != 1 || got[0].ID != "01-clean-primary-kill" {
		t.Fatalf("ProfileSmoke with missing IDs: got %v", ids(got))
	}
}

func TestSelectForProfileUnknownProfileReturnsAll(t *testing.T) {
	all := []Scenario{{ID: "a"}, {ID: "b"}}
	got := SelectForProfile(all, Profile("unknown"))
	if len(got) != 2 {
		t.Fatalf("unknown profile: got %d scenarios, want 2", len(got))
	}
}

func TestSelectForProfileEmptyInput(t *testing.T) {
	got := SelectForProfile(nil, ProfileSmoke)
	if len(got) != 0 {
		t.Fatalf("empty input: got %d scenarios, want 0", len(got))
	}
}

// ids is a test helper that extracts scenario IDs.
func ids(scens []Scenario) []string {
	out := make([]string, len(scens))
	for i, s := range scens {
		out[i] = s.ID
	}
	return out
}
