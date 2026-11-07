package labels

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// apiEnumValues returns the +kubebuilder:validation:Enum values declared on
// the api/v1alpha1 type typeName.
func apiEnumValues(t *testing.T, typeName string) []string {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob(filepath.Join("..", "..", "..", "api", "v1alpha1", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE || gd.Doc == nil {
				continue
			}
			for _, spec := range gd.Specs {
				if spec.(*ast.TypeSpec).Name.Name != typeName {
					continue
				}
				for _, c := range gd.Doc.List {
					if v, ok := strings.CutPrefix(c.Text, "// +kubebuilder:validation:Enum="); ok {
						return strings.Split(v, ";")
					}
				}
				t.Fatalf("api/v1alpha1.%s has no Enum marker", typeName)
			}
		}
	}
	t.Fatalf("api/v1alpha1.%s not found", typeName)
	return nil
}

// TestEnumLabelValuesAreDocumented takes every value of every enum-valued
// metric label through its mapping and compares it with the value the
// monitoring docs, alerts and dashboards select on. The API enum is read
// from its CRD marker, so a value added later fails here until it is
// documented.
func TestEnumLabelValuesAreDocumented(t *testing.T) {
	for _, tc := range []struct {
		label    string
		apiType  string
		fn       func(string) string
		want     map[string]string
		stateSet []string
	}{
		{"role (site gauges)", "SiteRole", func(v string) string { return Role(v1alpha1.SiteRole(v)) },
			map[string]string{"PrimaryCandidate": "primary-candidate", "DisasterRecovery": "disaster-recovery", "ReadOnly": "read-only"}, nil},
		{"bloodraven_site_state{state}, bloodraven_state_transitions_total{from,to}", "SiteState", func(v string) string { return SiteState(v1alpha1.SiteState(v)) },
			map[string]string{"Writable": "writable", "ReadOnly": "read-only", "Unreachable": "unreachable", "Unknown": "unknown"}, SiteStates},
		{"bloodraven_replication_source_state{state}", "SourceConvergenceState", func(v string) string { return SourceState(v1alpha1.SourceConvergenceState(v)) },
			map[string]string{"Converged": "converged", "Pending": "pending", "Blocked": "blocked"}, SourceStates},
		{"bloodraven_keyring_phase{phase}", "SiteKeyringPhase", func(v string) string { return KeyringPhase(v1alpha1.SiteKeyringPhase(v)) },
			map[string]string{"Pending": "pending", "Unsealed": "unsealed", "Escrowed": "escrowed", "Sealed": "sealed", "Failed": "failed"}, KeyringPhases},
		{"bloodraven_restore_reclone_pending{phase}", "RestoreReclonesPhase", func(v string) string { return RestoreReclonePhase(v1alpha1.RestoreReclonesPhase(v)) },
			map[string]string{"Armed": "armed", "Draining": "draining", "Held": "held"}, nil},
		{"bloodraven_planned_failovers_total{result}", "PlannedFailoverOutcome", func(v string) string { return PlannedFailoverResult(v1alpha1.PlannedFailoverOutcome(v)) },
			map[string]string{"Succeeded": "succeeded", "NotStarted": "not-started", "RolledBack": "rolled-back", "SourceLeftFenced": "source-left-fenced",
				"PromotedUnconfirmed": "promoted-unconfirmed", "ManualRecoveryRequired": "manual-recovery-required"}, nil},
	} {
		enum := apiEnumValues(t, tc.apiType)
		var mapped []string
		for _, v := range enum {
			want, ok := tc.want[v]
			if !ok {
				t.Errorf("%s: API value %s has no documented label value", tc.label, v)
				continue
			}
			got := tc.fn(v)
			if got != want {
				t.Errorf("%s: %s -> %q, want %q", tc.label, v, got, want)
			}
			mapped = append(mapped, got)
		}
		for v := range tc.want {
			if !slices.Contains(enum, v) {
				t.Errorf("%s: documented value for %s, which is not in the API enum %v", tc.label, v, enum)
			}
		}
		if tc.stateSet != nil {
			a, b := slices.Clone(tc.stateSet), slices.Clone(mapped)
			slices.Sort(a)
			slices.Sort(b)
			if !slices.Equal(a, b) {
				t.Errorf("%s: emitted state-set %v, want every mapped value %v", tc.label, tc.stateSet, mapped)
			}
		}
	}
}

// kebab agrees with the documented label form on the shapes the enums use,
// including an acronym run, which strings.ToLower gets wrong.
func TestKebab(t *testing.T) {
	for in, want := range map[string]string{
		"Writable": "writable", "ReadOnly": "read-only", "GTIDDiverged": "gtid-diverged",
		"ManualRecoveryRequired": "manual-recovery-required", "AES256": "aes256", "": "",
	} {
		if got := kebab(in); got != want {
			t.Errorf("kebab(%q) = %q, want %q", in, got, want)
		}
	}
}
