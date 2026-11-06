package main

import (
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
	got, err := selectRunAllScenarios(registered, runner.ProfileRelease, list)
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
			got, err := selectRunAllScenarios(reg, tc.profile, tc.list)
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
