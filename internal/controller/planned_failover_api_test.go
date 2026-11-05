package controller

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// pfRemovedStatusFields are the status.plannedFailover fields the durable
// claim removed (§11 Removed); pfRemovedDragonflyFields the removed
// status.plannedFailover.dragonfly fields; pfRemovedPhases the removed
// phases.
var (
	pfRemovedStatusFields = []string{"sourcePrimary", "sourceGtidAtFence", "targetGtidAtPromotion", "completionTime",
		"durationSeconds", "transactionsLost", "reason", "maxLagWait", "drainTimeout", "drainStartTime", "lagWaitStartTime", "retryAfter"}
	pfRemovedDragonflyFields = []string{"enabled", "sourceOffsetAtDrain", "offsetCaptureFailed", "targetOffsetAtPromotion",
		"sessionsPreserved", "promotionMethod"}
	pfRemovedPhases = []string{"", "Pending", "Validating", "Resuming"}
)

func pfJSONNames(t reflect.Type) []string {
	var out []string
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		out = append(out, name)
	}
	return out
}

// TestPlannedFailoverOldFieldsGone: the removed fields and phases are gone
// from the Go types, the generated CRD (and its chart copy), and every
// non-test Go source outside the API package.
func TestPlannedFailoverOldFieldsGone(t *testing.T) {
	status := pfJSONNames(reflect.TypeFor[v1alpha1.PlannedFailoverStatus]())
	dragonfly := pfJSONNames(reflect.TypeFor[v1alpha1.PlannedFailoverDragonflyStatus]())
	for _, f := range pfRemovedStatusFields {
		if slices.Contains(status, f) {
			t.Errorf("PlannedFailoverStatus still has %q", f)
		}
	}
	for _, f := range pfRemovedDragonflyFields {
		if slices.Contains(dragonfly, f) {
			t.Errorf("PlannedFailoverDragonflyStatus still has %q", f)
		}
	}

	for _, path := range []string{"../../config/crd/bases/shipstream.io_mysqlfailovergroups.yaml",
		"../../charts/bloodraven/crds/shipstream.io_mysqlfailovergroups.yaml"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var crd struct {
			Spec struct {
				Versions []struct {
					Schema struct {
						OpenAPIV3Schema struct {
							Properties map[string]struct {
								Properties map[string]pfSchema `json:"properties"`
							} `json:"properties"`
						} `json:"openAPIV3Schema"`
					} `json:"schema"`
				} `json:"versions"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(b, &crd); err != nil {
			t.Fatal(err)
		}
		for _, v := range crd.Spec.Versions {
			pf, ok := v.Schema.OpenAPIV3Schema.Properties["status"].Properties["plannedFailover"]
			if !ok {
				t.Fatalf("%s: no status.plannedFailover schema", path)
			}
			for _, f := range pfRemovedStatusFields {
				if _, ok := pf.Properties[f]; ok {
					t.Errorf("%s: status.plannedFailover.%s still in the schema", path, f)
				}
			}
			for _, f := range pfRemovedDragonflyFields {
				if _, ok := pf.Properties["dragonfly"].Properties[f]; ok {
					t.Errorf("%s: status.plannedFailover.dragonfly.%s still in the schema", path, f)
				}
			}
			for _, p := range pfRemovedPhases {
				if slices.Contains(pf.Properties["phase"].Enum, p) {
					t.Errorf("%s: phase enum still has %q", path, p)
				}
			}
			for _, req := range []string{"runId", "revision", "phase", "target", "settings", "startTime"} {
				if !slices.Contains(pf.Required, req) {
					t.Errorf("%s: status.plannedFailover.%s is not required", path, req)
				}
			}
		}
	}

	// No Go code outside the API package names a removed field or phase.
	idents := regexp.MustCompile(`\.(SourcePrimary|SourceGtidAtFence|TargetGtidAtPromotion|TransactionsLost|DrainStartTime|LagWaitStartTime|` +
		`SourceOffsetAtDrain|OffsetCaptureFailed|TargetOffsetAtPromotion|SessionsPreserved|PromotionMethod)\b|` +
		`PlannedFailoverPhase(None|Pending|Validating|Resuming)\b`)
	for _, root := range []string{"../../internal", "../../cmd", "../../test"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "planned_failover_api_test.go") {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for i, line := range strings.Split(string(b), "\n") {
				code, _, _ := strings.Cut(line, "//")
				if idents.MatchString(code) {
					t.Errorf("%s:%d names a removed planned-failover field or phase: %s", path, i+1, strings.TrimSpace(line))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

type pfSchema struct {
	Properties map[string]pfSchema `json:"properties"`
	Enum       []string            `json:"enum"`
	Required   []string            `json:"required"`
}
