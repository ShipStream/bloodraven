package controller

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// apiCRDSchema is the subset of a CRD property the timer tests read.
type apiCRDSchema struct {
	Type        string                  `json:"type"`
	Pattern     string                  `json:"pattern"`
	Format      string                  `json:"format"`
	Default     any                     `json:"default"`
	Properties  map[string]apiCRDSchema `json:"properties"`
	Items       *apiCRDSchema           `json:"items"`
	Validations []struct {
		Rule string `json:"rule"`
	} `json:"x-kubernetes-validations"`
}

// apiCRDRoot loads the v1alpha1 openAPIV3Schema of a generated CRD file and
// of its chart copy.
func apiCRDRoots(t *testing.T, file string) map[string]apiCRDSchema {
	t.Helper()
	out := map[string]apiCRDSchema{}
	for _, dir := range []string{"../../config/crd/bases", "../../charts/bloodraven/crds"} {
		path := filepath.Join(dir, file)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var crd struct {
			Spec struct {
				Versions []struct {
					Name   string `json:"name"`
					Schema struct {
						OpenAPIV3Schema apiCRDSchema `json:"openAPIV3Schema"`
					} `json:"schema"`
				} `json:"versions"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(b, &crd); err != nil {
			t.Fatal(err)
		}
		for _, v := range crd.Spec.Versions {
			if v.Name == "v1alpha1" {
				out[path] = v.Schema.OpenAPIV3Schema
			}
		}
		if _, ok := out[path]; !ok {
			t.Fatalf("%s: no v1alpha1 schema", path)
		}
	}
	return out
}

func (s apiCRDSchema) at(path []string) (apiCRDSchema, bool) {
	for _, p := range path {
		if p == "[]" {
			if s.Items == nil {
				return apiCRDSchema{}, false
			}
			s = *s.Items
			continue
		}
		next, ok := s.Properties[p]
		if !ok {
			return apiCRDSchema{}, false
		}
		s = next
	}
	return s, true
}

var durationType = reflect.TypeFor[metav1.Duration]()

// apiDurationPaths walks t (a v1alpha1 type) and returns the JSON path of
// every metav1.Duration or *metav1.Duration field, "[]" marking a list item.
func apiDurationPaths(t reflect.Type, prefix []string, out *[][]string) {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if t.Kind() != reflect.Pointer {
			prefix = append(append([]string{}, prefix...), "[]")
		}
		t = t.Elem()
	}
	if t == durationType {
		*out = append(*out, prefix)
		return
	}
	if t.Kind() != reflect.Struct || t.PkgPath() != reflect.TypeFor[v1alpha1.MysqlFailoverGroup]().PkgPath() {
		return
	}
	for i := range t.NumField() {
		f := t.Field(i)
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		path := prefix
		if !(f.Anonymous && strings.Contains(opts, "inline")) && name != "" {
			path = append(append([]string{}, prefix...), name)
		}
		apiDurationPaths(f.Type, path, out)
	}
}

// apiDurationExempt are the Duration properties with no CEL bounds, each
// with its reason. WE-Q3: the frozen status.plannedFailover.settings are
// operator-written copies of already bounded inputs, and a refused status
// write while the source is fenced would wedge the planned failover.
var apiDurationExempt = map[string]string{
	"status.plannedFailover.settings.maxLagWait":            "WE-Q3: frozen run settings",
	"status.plannedFailover.settings.drainTimeout":          "WE-Q3: frozen run settings",
	"status.plannedFailover.settings.dragonfly.maxSyncWait": "WE-Q3: frozen run settings",
}

var (
	durationLowerRE = regexp.MustCompile(`duration\(self\) >= duration\('[0-9a-z]+'\)`)
	durationUpperRE = regexp.MustCompile(`duration\(self\) <= duration\('[0-9a-z]+'\)`)
)

// TestDurationFieldsCarryCELBounds (A-38, OPUS-10): metav1.Duration
// generates a plain string with no format, so scanning the CRD for a format
// would pass vacuously. Reflect over the Go types instead and require every
// Duration property to carry both a lower and an upper duration() bound,
// except the named WE-Q3 status snapshots. A default must be spelled as
// metav1.Duration marshals it.
func TestDurationFieldsCarryCELBounds(t *testing.T) {
	kinds := map[string]reflect.Type{
		"shipstream.io_mysqlfailovergroups.yaml":      reflect.TypeFor[v1alpha1.MysqlFailoverGroup](),
		"shipstream.io_mysqlbackups.yaml":             reflect.TypeFor[v1alpha1.MysqlBackup](),
		"shipstream.io_mysqlbackupverifications.yaml": reflect.TypeFor[v1alpha1.MysqlBackupVerification](),
		"shipstream.io_mysqldatabases.yaml":           reflect.TypeFor[v1alpha1.MysqlDatabase](),
		"shipstream.io_mysqlstandbyclusters.yaml":     reflect.TypeFor[v1alpha1.MysqlStandbyCluster](),
	}
	exemptSeen := map[string]bool{}
	total := 0
	for file, kind := range kinds {
		var paths [][]string
		for _, root := range []string{"Spec", "Status"} {
			f, _ := kind.FieldByName(root)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			apiDurationPaths(f.Type, []string{name}, &paths)
		}
		for path, schema := range apiCRDRoots(t, file) {
			for _, p := range paths {
				key := strings.Join(p, ".")
				if file == "shipstream.io_mysqlfailovergroups.yaml" {
					if _, ok := apiDurationExempt[key]; ok {
						exemptSeen[key] = true
						continue
					}
				}
				total++
				prop, ok := schema.at(p)
				if !ok {
					t.Errorf("%s: Go Duration field %s has no CRD property", path, key)
					continue
				}
				if prop.Type != "string" || prop.Format != "" {
					t.Errorf("%s: %s is type %q format %q, want a plain string (no Format=duration)", path, key, prop.Type, prop.Format)
				}
				// metav1.Duration marshals as time.Duration.String(), so a
				// default spelled any other way ("5m") is rewritten ("5m0s")
				// by the operator's first typed Update. That changes spec and
				// costs the object CRD validation ratcheting on rules at spec.
				if prop.Default != nil {
					raw, _ := prop.Default.(string)
					d, err := time.ParseDuration(raw)
					if err != nil || d.String() != raw {
						t.Errorf("%s: %s default %v is not the canonical Duration spelling (want %q)", path, key, prop.Default, d.String())
					}
				}
				lower, upper := false, false
				for _, v := range prop.Validations {
					lower = lower || durationLowerRE.MatchString(v.Rule)
					upper = upper || durationUpperRE.MatchString(v.Rule)
				}
				if !lower || !upper {
					t.Errorf("%s: %s has no CEL lower and upper duration() bound (lower %v, upper %v)", path, key, lower, upper)
				}
			}
		}
	}
	for key := range apiDurationExempt {
		if !exemptSeen[key] {
			t.Errorf("exemption %s names no Duration field; remove it", key)
		}
	}
	if total < 30 {
		t.Fatalf("found only %d Duration properties; the walk is broken", total)
	}
}

// TestPlannedMaxLagWaitBoundsShared (WE-Q3): the CRD rule on
// spec.plannedFailover.maxLagWait and the annotation parser use the same
// constant pair.
func TestPlannedMaxLagWaitBoundsShared(t *testing.T) {
	want := fmt.Sprintf("duration(self) >= duration('%s') && duration(self) <= duration('%s')",
		compactDuration(v1alpha1.PlannedFailoverMaxLagWaitMin), compactDuration(v1alpha1.PlannedFailoverMaxLagWaitMax))
	for path, schema := range apiCRDRoots(t, "shipstream.io_mysqlfailovergroups.yaml") {
		prop, _ := schema.at([]string{"spec", "plannedFailover", "maxLagWait"})
		if len(prop.Validations) != 1 || prop.Validations[0].Rule != want {
			t.Errorf("%s: maxLagWait rules %+v, want %q", path, prop.Validations, want)
		}
	}
	for _, tc := range []struct {
		d  time.Duration
		ok bool
	}{
		{d: v1alpha1.PlannedFailoverMaxLagWaitMin, ok: true},
		{d: v1alpha1.PlannedFailoverMaxLagWaitMax, ok: true},
		{d: v1alpha1.PlannedFailoverMaxLagWaitMin - time.Millisecond},
		{d: v1alpha1.PlannedFailoverMaxLagWaitMax + time.Second},
		{d: 24 * time.Hour},
	} {
		_, err := parsePlannedFailoverAnnotation("dc2:maxLagWait=" + tc.d.String())
		if (err == nil) != tc.ok {
			t.Errorf("annotation maxLagWait=%s: err %v, want ok=%v", tc.d, err, tc.ok)
		}
	}
	for _, bad := range []string{"0s", "-5s"} {
		if _, err := parsePlannedFailoverAnnotation("dc2:maxLagWait=" + bad); err == nil {
			t.Errorf("annotation maxLagWait=%s accepted", bad)
		}
	}
}

// apiStopTimePattern is A-39's pattern; both stopTime markers spell it.
const apiStopTimePattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`

// TestStopTimeCarriesRFC3339Pattern (A-39, OPUS-R3-03): both user-written
// stopTime properties carry the pattern, and the pattern admits only strings
// that metav1.Time decodes at whole-second precision.
func TestStopTimeCarriesRFC3339Pattern(t *testing.T) {
	for file, path := range map[string][]string{
		"shipstream.io_mysqlfailovergroups.yaml":      {"spec", "initFromBackup", "pointInTime", "stopTime"},
		"shipstream.io_mysqlbackupverifications.yaml": {"spec", "pointInTime", "stopTime"},
	} {
		for crd, schema := range apiCRDRoots(t, file) {
			prop, ok := schema.at(path)
			if !ok || prop.Pattern != apiStopTimePattern || prop.Type != "string" || prop.Format != "date-time" {
				t.Errorf("%s %v: type %q format %q pattern %q", crd, path, prop.Type, prop.Format, prop.Pattern)
			}
		}
	}
	re := regexp.MustCompile(apiStopTimePattern)
	for _, s := range []string{
		"2030-01-01T00:00:00Z", "2030-01-01T02:00:00+02:00", "2030-01-01T02:00:00-23:59", "2030-01-01T00:00:00",
		"2030-01-01 00:00:00Z", "2030-01-01t00:00:00Z", "2030-01-01T00:00:00z", "2030-01-01T00:00:00.5Z",
		"2030-01-01T00:00:00+25:00", "2030-01-01T00:00:00+99:00", "2030-01-01T00:00:00+05:99", "2030-01-01T00:00:00+24:00",
		"2030-01-01T00:00:00+0200", "2030-13-01T00:00:00Z", "2030-02-30T00:00:00Z", "2030-01-01T24:00:00Z",
	} {
		if !re.MatchString(s) {
			continue
		}
		b, _ := json.Marshal(s)
		var mt metav1.Time
		if err := mt.UnmarshalJSON(b); err != nil {
			// The apiserver's date-time format check refuses impossible
			// dates and clock values the pattern admits.
			if _, perr := time.Parse(time.RFC3339, s); perr == nil {
				t.Errorf("pattern admits %q, which metav1.Time refuses: %v", s, err)
			}
			continue
		}
		if mt.Nanosecond() != 0 {
			t.Errorf("pattern admits a fraction: %q", s)
		}
	}
}

// TestAPIHasNoPlatformSizedInt (A-36): no field in api/ is an int, uint or
// uintptr, whose CRD integer has no format and whose size depends on the
// build.
func TestAPIHasNoPlatformSizedInt(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("../../api/v1alpha1/*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob api: %v", err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") || strings.HasSuffix(file, "zz_generated.deepcopy.go") {
			continue
		}
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			st, ok := n.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				ast.Inspect(field.Type, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && (id.Name == "int" || id.Name == "uint" || id.Name == "uintptr") {
						t.Errorf("%s: field of platform-sized type %s; use int32 or int64", fset.Position(field.Pos()), id.Name)
					}
					return true
				})
			}
			return true
		})
	}
}
