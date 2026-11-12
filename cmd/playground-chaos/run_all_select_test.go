package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// TestSelectRunAllScenariosListKeepsResets pins C12: a CI scenario list
// runs through run-all, so each listed scenario that sets ResetBeforeRunAll
// is reset before it runs, in the listed order.
func TestSelectRunAllScenariosListKeepsResets(t *testing.T) {
	registered := runner.DefaultRegistry.List()
	list := "31-pitr-verification-rustfs,37-pitr-planned-failback-no-reclone,68-pitr-failback-with-reclone,70-pitr-lossy-failover"
	got, err := selectRunAllScenarios(registered, runner.ProfileRelease, list, runner.Shard{})
	if err != nil {
		t.Fatal(err)
	}
	wantReset := map[string]bool{
		"31-pitr-verification-rustfs":         false,
		"37-pitr-planned-failback-no-reclone": true,
		"68-pitr-failback-with-reclone":       true,
		"70-pitr-lossy-failover":              true,
	}
	ids := strings.Split(list, ",")
	if len(got) != len(ids) {
		t.Fatalf("selected %d scenarios, want %d", len(got), len(ids))
	}
	for i, s := range got {
		if s.ID != ids[i] {
			t.Errorf("position %d: %s, want %s (listed order)", i, s.ID, ids[i])
		}
		if s.ResetBeforeRunAll != wantReset[s.ID] {
			t.Errorf("%s ResetBeforeRunAll=%v, want %v", s.ID, s.ResetBeforeRunAll, wantReset[s.ID])
		}
	}
}

func TestSelectRunAllScenarios(t *testing.T) {
	registered := []runner.Scenario{{ID: "01-clean-primary-kill"}, {ID: "02-x"}, {ID: "03-q", Quarantine: "#1"}}
	cases := []struct {
		name    string
		profile runner.Profile
		list    string
		want    string
		wantErr bool
		only    []runner.Scenario // replaces registered when set
	}{
		{name: "profile when no list", profile: runner.ProfileFull, want: "01-clean-primary-kill,02-x"},
		{name: "list overrides profile", profile: runner.ProfileSmoke, list: "02-x,03-q", want: "02-x,03-q"},
		{name: "blank list means profile", profile: runner.ProfileSmoke, list: "  ", want: "01-clean-primary-kill"},
		{name: "unknown listed id", profile: runner.ProfileFull, list: "02-x,nope", wantErr: true},
		{name: "empty profile selection", profile: runner.ProfileRelease, list: "", wantErr: true, only: []runner.Scenario{{ID: "02-x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := registered
			if tc.only != nil {
				reg = tc.only
			}
			got, err := selectRunAllScenarios(reg, tc.profile, tc.list, runner.Shard{})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want an error, got %d scenarios", len(got))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for _, s := range got {
				ids = append(ids, s.ID)
			}
			if strings.Join(ids, ",") != tc.want {
				t.Fatalf("got %v, want %s", ids, tc.want)
			}
		})
	}
}

// TestSelectRunAllScenariosShard pins what CI's full-profile jobs rely on
// (#236): `run-all --profile=full --shard=<i>/<n>` over i=1..n runs every
// scenario of the profile exactly once, and --shard is refused with an
// explicit --scenarios list.
func TestSelectRunAllScenariosShard(t *testing.T) {
	registered := runner.DefaultRegistry.List()
	whole, err := selectRunAllScenarios(registered, runner.ProfileFull, "", runner.Shard{})
	if err != nil {
		t.Fatal(err)
	}
	const total = 4
	ran := map[string]int{}
	for i := 1; i <= total; i++ {
		part, err := selectRunAllScenarios(registered, runner.ProfileFull, "", runner.Shard{Index: i, Total: total})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range part {
			ran[s.ID]++
		}
	}
	if len(ran) != len(whole) {
		t.Fatalf("%d shards run %d distinct scenarios, the full profile has %d", total, len(ran), len(whole))
	}
	for _, s := range whole {
		if ran[s.ID] != 1 {
			t.Errorf("%s runs in %d shards, want exactly 1", s.ID, ran[s.ID])
		}
	}

	if _, err := selectRunAllScenarios(registered, runner.ProfileFull, "01-clean-primary-kill", runner.Shard{Index: 1, Total: 2}); err == nil {
		t.Error("--shard with --scenarios must be refused")
	}
	// More shards than scenarios: an empty shard is not an error, the
	// other shards cover the profile.
	two := []runner.Scenario{{ID: "01-clean-primary-kill"}, {ID: "02-x"}}
	part, err := selectRunAllScenarios(two, runner.ProfileFull, "", runner.Shard{Index: 3, Total: 3})
	if err != nil || len(part) != 0 {
		t.Errorf("shard 3/3 of two scenarios: got %d scenarios, err %v; want none and no error", len(part), err)
	}
}

// TestListScenariosShard pins `list --profile --shard`, which the E2E
// workflow uses to record what a shard set out to run: one line per
// scenario starting with its ID, quarantined scenarios left out, and the
// plain `list` unchanged (everything registered, quarantined included).
func TestListScenariosShard(t *testing.T) {
	registered := []runner.Scenario{
		{ID: "01-clean-primary-kill", Title: "a", Hypothesis: "h"},
		{ID: "02-x", Title: "b"},
		{ID: "03-q", Title: "c", Quarantine: "#1"},
		{ID: "04-y", Title: "d"},
	}
	ids := func(out string) string {
		var got []string
		for _, line := range strings.Split(out, "\n") {
			if line != "" && !strings.HasPrefix(line, " ") {
				got = append(got, strings.Fields(line)[0])
			}
		}
		return strings.Join(got, ",")
	}

	var plain strings.Builder
	if code := listScenarios(&plain, registered, false, runner.DefaultProfile, ""); code != exitOK {
		t.Fatalf("list: exit %d", code)
	}
	if got := ids(plain.String()); got != "01-clean-primary-kill,02-x,03-q,04-y" {
		t.Errorf("plain list = %s, want every registered scenario", got)
	}

	var union []string
	for i := 1; i <= 2; i++ {
		var out strings.Builder
		if code := listScenarios(&out, registered, false, runner.ProfileFull, fmt.Sprintf("%d/2", i)); code != exitOK {
			t.Fatalf("list --shard=%d/2: exit %d", i, code)
		}
		if strings.Contains(out.String(), "03-q") {
			t.Errorf("list --shard=%d/2 names the quarantined scenario, which run-all would not run", i)
		}
		union = append(union, ids(out.String()))
	}
	if got := strings.Join(union, "|"); got != "01-clean-primary-kill,04-y|02-x" {
		t.Errorf("shards = %s, want 01-clean-primary-kill,04-y|02-x", got)
	}

	var smoke strings.Builder
	if code := listScenarios(&smoke, registered, true, runner.ProfileSmoke, ""); code != exitOK {
		t.Fatalf("list --profile=smoke: exit %d", code)
	}
	if got := ids(smoke.String()); got != "01-clean-primary-kill" {
		t.Errorf("list --profile=smoke = %s, want 01-clean-primary-kill", got)
	}

	if code := listScenarios(&strings.Builder{}, registered, false, runner.ProfileFull, "3/2"); code != exitFlagParse {
		t.Errorf("list --shard=3/2: exit %d, want %d", code, exitFlagParse)
	}
}
