package controller

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// mustTopologyConfig is CRConfigToTopologyConfig for fixtures whose roles
// are valid.
func mustTopologyConfig(fg *v1alpha1.MysqlFailoverGroup) TopologyConfig {
	cfg, err := CRConfigToTopologyConfig(fg)
	if err != nil {
		panic(err)
	}
	return cfg
}

// apiModeOf is the API point-in-time mode whose wire value is wire.
func apiModeOf(t *testing.T, wire string) v1alpha1.PointInTimeMode {
	t.Helper()
	for _, m := range []v1alpha1.PointInTimeMode{v1alpha1.PointInTimeModeLatest, v1alpha1.PointInTimeModeTimestamp} {
		if w, err := pitrWireMode(m); err == nil && string(w) == wire {
			return m
		}
	}
	t.Fatalf("no API point-in-time mode has the wire value %q", wire)
	return ""
}

// wireModeOf is pitrWireMode for a valid mode.
func wireModeOf(t *testing.T, m v1alpha1.PointInTimeMode) string {
	t.Helper()
	w, err := pitrWireMode(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(w)
}

// apiEnumValues returns the +kubebuilder:validation:Enum values declared on
// the api/v1alpha1 type typeName: the set the apiserver admits, so a test
// that ranges over it covers every value a future edit adds.
func apiEnumValues(t *testing.T, typeName string) []string {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob(filepath.Join("..", "..", "api", "v1alpha1", "*.go"))
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
				if ts := spec.(*ast.TypeSpec); ts.Name.Name == typeName {
					for _, c := range gd.Doc.List {
						if v, ok := strings.CutPrefix(c.Text, "// +kubebuilder:validation:Enum="); ok {
							return strings.Split(v, ";")
						}
					}
					t.Fatalf("api/v1alpha1.%s has no Enum marker", typeName)
				}
			}
		}
	}
	t.Fatalf("api/v1alpha1.%s not found", typeName)
	return nil
}

var internalSiteStates = []state.SiteState{state.StateUnknown, state.StateWritable, state.StateReadOnly, state.StateUnreachable}

// Every API role maps to an internal role and back to itself; an unset
// role is the API default; anything else is refused.
func TestTopologySiteRoleRoundTrip(t *testing.T) {
	seen := map[state.SiteRole]v1alpha1.SiteRole{}
	for _, v := range apiEnumValues(t, "SiteRole") {
		r := v1alpha1.SiteRole(v)
		internal, err := topologySiteRole(r)
		if err != nil {
			t.Fatalf("%s: %v", r, err)
		}
		if prev, dup := seen[internal]; dup {
			t.Fatalf("%s and %s both map to %q", prev, r, internal)
		}
		seen[internal] = r
		if back := APISiteRole(internal); back != r {
			t.Errorf("%s -> %q -> %s", r, internal, back)
		}
	}
	if got, err := topologySiteRole(""); err != nil || got != state.SiteRolePrimaryCandidate {
		t.Errorf("unset role -> %q, %v; want the PrimaryCandidate default", got, err)
	}
	for _, bad := range []v1alpha1.SiteRole{"primary-candidate", "dr-only", "read-only", "primarycandidate", "Primary"} {
		if got, err := topologySiteRole(bad); err == nil {
			t.Errorf("%q -> %q; want refused", bad, got)
		}
	}
	if got := APISiteRole("bogus"); got != "" {
		t.Errorf("APISiteRole(bogus) = %q", got)
	}
}

// A role the operator cannot interpret configures no topology manager.
func TestCRConfigToTopologyConfigRefusesUnknownRole(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[1].Role = "dr-only"
	if _, err := CRConfigToTopologyConfig(fg); err == nil || !strings.Contains(err.Error(), `"dc2"`) {
		t.Fatalf("err = %v, want a refusal naming dc2", err)
	}
}

// Each API role, taken through the topology configuration and the topology
// manager's observations into EvalCrossSite: a writable non-promotable site
// is fenced, and a writable PrimaryCandidate (the only writer) is not.
// A cast of the API value into state.SiteRole would fence the
// PrimaryCandidate, because the matrix compares the internal spelling.
func TestAPIRolesThroughTopologyIntoEvalCrossSite(t *testing.T) {
	for _, v := range apiEnumValues(t, "SiteRole") {
		role := v1alpha1.SiteRole(v)
		t.Run(v, func(t *testing.T) {
			fg := newTestFG()
			third := fg.Spec.Sites[1]
			third.Name, third.ServerID, third.Role = "x", 103, role
			fg.Spec.Sites = append(fg.Spec.Sites, third)
			cfg, err := CRConfigToTopologyConfig(fg)
			if err != nil {
				t.Fatal(err)
			}
			tm := NewTopologyManager(cfg, []mysql.Checker{&mockMySQL{}, &mockMySQL{}, &mockMySQL{}}, NewFailoverController(testLogger()),
				nil, nil, BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
			tm.sites[0].state, tm.sites[1].state, tm.sites[2].state = state.StateReadOnly, state.StateReadOnly, state.StateWritable

			action := state.EvalCrossSite(tm.observations(), cfg.SitePriorities)
			fenced := slices.Contains(action.FenceSites, "x")
			if role == v1alpha1.SiteRolePrimaryCandidate {
				if fenced || action.SplitBrain {
					t.Fatalf("writable PrimaryCandidate: action = %+v, want it unfenced", action)
				}
				if got := tm.effectiveActiveSiteLocked(); got != "x" {
					t.Errorf("active site = %q, want x", got)
				}
			} else if !fenced {
				t.Fatalf("writable %s: action = %+v, want it fenced", role, action)
			}
			if got := tm.Status().Sites[2].Role; got != role {
				t.Errorf("/status role = %q, want %q", got, role)
			}
		})
	}
}

// /status and /ws/status serialize every role and state as the CR value.
func TestStatusEndpointsServeCRVocabulary(t *testing.T) {
	states := apiEnumValues(t, "SiteState")
	for _, v := range apiEnumValues(t, "SiteRole") {
		role := v1alpha1.SiteRole(v)
		for _, st := range internalSiteStates {
			fg := newTestFG()
			fg.Spec.Sites[1].Role = role
			if role == v1alpha1.SiteRolePrimaryCandidate {
				fg.Spec.Sites[0].Role = role
			}
			tm := NewTopologyManager(mustTopologyConfig(fg), []mysql.Checker{&mockMySQL{}, &mockMySQL{}}, NewFailoverController(testLogger()),
				nil, nil, BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
			tm.sites[1].state = st

			raw, err := json.Marshal(tm.Status())
			if err != nil {
				t.Fatal(err)
			}
			var status struct {
				Sites []struct{ Role, State string }
			}
			if err := json.Unmarshal(raw, &status); err != nil {
				t.Fatal(err)
			}
			tm.mu.RLock()
			msg := tm.topologyMessageLocked(nil, "")
			tm.mu.RUnlock()
			raw, err = json.Marshal(msg)
			if err != nil {
				t.Fatal(err)
			}
			var ws struct {
				Sites []struct{ Role, State string }
			}
			if err := json.Unmarshal(raw, &ws); err != nil {
				t.Fatal(err)
			}
			for name, got := range map[string]struct{ Role, State string }{"/status": status.Sites[1], "/ws/status": ws.Sites[1]} {
				if got.Role != string(role) || got.State != string(apiSiteState(st)) || !slices.Contains(states, got.State) {
					t.Errorf("%s for role %s, state %v: role=%q state=%q", name, role, st, got.Role, got.State)
				}
			}
		}
	}
}

// Every internal site state maps to a distinct API SiteState the CRD
// admits, and every API value is reached.
func TestAPISiteStateCoversEveryInternalState(t *testing.T) {
	enum := apiEnumValues(t, "SiteState")
	got := map[v1alpha1.SiteState]state.SiteState{}
	for _, s := range internalSiteStates {
		v := apiSiteState(s)
		if !slices.Contains(enum, string(v)) {
			t.Errorf("%v -> %q, not in the API enum %v", s, v, enum)
		}
		if prev, dup := got[v]; dup {
			t.Errorf("%v and %v both map to %q", prev, s, v)
		}
		got[v] = s
	}
	if len(got) != len(enum) {
		t.Errorf("internal states reach %d of the %d API values %v", len(got), len(enum), enum)
	}
}

// Every Dragonfly classifier value maps to a distinct DragonflyRole the
// CRD admits, and every API value is reached. The classifier's own values
// stay lowercase: they are also the dragonfly-role pod label.
func TestAPIDragonflyRoleCoversClassifier(t *testing.T) {
	enum := apiEnumValues(t, "DragonflyRole")
	classifier := []dragonfly.SiteRole{dragonfly.RoleUnknown, dragonfly.RoleMaster, dragonfly.RoleReplica,
		dragonfly.RoleStaleMaster, dragonfly.RoleUnconfigured, dragonfly.RoleUnreachable}
	// Each classifier value is reachable: the list above is exhaustive.
	reached := map[dragonfly.SiteRole]bool{
		dragonfly.ClassifySiteRole(dragonfly.ReplicationInfo{}, false, "a", "a"):                true,
		dragonfly.ClassifySiteRole(dragonfly.ReplicationInfo{Role: "master"}, true, "a", "a"):   true,
		dragonfly.ClassifySiteRole(dragonfly.ReplicationInfo{Role: "master"}, true, "a", "b"):   true,
		dragonfly.ClassifySiteRole(dragonfly.ReplicationInfo{Role: "slave"}, true, "a", "b"):    true,
		dragonfly.ClassifySiteRole(dragonfly.ReplicationInfo{}, true, "a", "b"):                 true,
		dragonfly.ClassifySiteRole(dragonfly.ReplicationInfo{Role: "sentinel"}, true, "a", "b"): true,
	}
	for r := range reached {
		if !slices.Contains(classifier, r) {
			t.Errorf("classifier value %q is not in the mapped list", r)
		}
	}
	got := map[v1alpha1.DragonflyRole]dragonfly.SiteRole{}
	for _, r := range classifier {
		v := apiDragonflyRole(r)
		if !slices.Contains(enum, string(v)) {
			t.Errorf("%q -> %q, not in the API enum %v", r, v, enum)
		}
		if prev, dup := got[v]; dup {
			t.Errorf("%q and %q both map to %q", prev, r, v)
		}
		got[v] = r
	}
	if len(got) != len(enum) {
		t.Errorf("classifier values reach %d of the %d API values %v", len(got), len(enum), enum)
	}
}

// Every API point-in-time mode has a wire value the replay consumers read
// (pitrplan's modes), and no other value has one.
func TestPITRWireModeIsExhaustive(t *testing.T) {
	want := map[string]pitrplan.Mode{"Latest": pitrplan.ModeLatest, "Timestamp": pitrplan.ModeTimestamp}
	for _, v := range apiEnumValues(t, "PointInTimeMode") {
		w, err := pitrWireMode(v1alpha1.PointInTimeMode(v))
		if err != nil {
			t.Fatalf("%s: %v", v, err)
		}
		if w != want[v] {
			t.Errorf("%s -> %q, want %q", v, w, want[v])
		}
	}
	for _, bad := range []v1alpha1.PointInTimeMode{"", "none", "latest", "timestamp"} {
		if w, err := pitrWireMode(bad); err == nil {
			t.Errorf("%q -> %q; want refused", bad, w)
		}
	}
}
