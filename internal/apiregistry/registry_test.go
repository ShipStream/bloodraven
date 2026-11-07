// Package apiregistry holds the tests that keep the operator's served tokens
// (Event reasons, condition types and reasons, the deployment API's
// unstableReason) declared in api/v1alpha1. It has no non-test code.
package apiregistry

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"sort"
	"strings"
	"sync"
	"testing"

	"golang.org/x/tools/go/packages"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

func tokenStrings[T ~string](ts []T) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = string(t)
	}
	return out
}

// trustedSources are the functions that parse a reason out of a message or
// an error. Their results are catalog values, and
// TestPITRTokenCatalogMatchesAPI pins the catalog to api/v1alpha1.
var trustedSources = []trustedSource{
	{name: modulePath + "/internal/controller.pitrFailureReason", values: tokenStrings(pitrarchive.ServedTokens)},
	{name: modulePath + "/internal/pitrarchive.ParseToken", values: tokenStrings(pitrarchive.AllTokens)},
	{name: "(" + modulePath + "/internal/pitrarchive.Token).Reason", values: tokenStrings(pitrarchive.ArchiveReadyReasons)},
}

// catalogPackages may spell a served value with their own constants: they
// do not import api/v1alpha1 (the sidecar's PITR catalog, the pure topology
// matrix), and each value they produce is checked against the API like any
// other.
var catalogPackages = []string{
	modulePath + "/internal/pitrarchive.",
	modulePath + "/internal/state.",
}

var (
	loadOnce sync.Once
	loaded   *analysis
	sinkList []sink
)

// analysed loads the operator's code once per test binary.
func analysed(t *testing.T) (*analysis, []sink) {
	t.Helper()
	loadOnce.Do(func() {
		loaded = loadAnalysis(t, trustedSources, "./api/...", "./internal/...", "./cmd/...")
		sinkList = loaded.sinks()
	})
	if loaded == nil {
		t.Fatal("the analysis did not load; see the first test's failure")
	}
	return loaded, sinkList
}

// registry is the set of values each kind of sink may carry.
type registry struct {
	events, conditionTypes, conditionReasons, unstable map[string]string            // value -> constant
	byType                                             map[string]map[string]string // API type -> value -> constant
}

func newRegistry(t *testing.T, a *analysis) registry {
	t.Helper()
	consts := a.apiConsts(t)
	r := registry{
		events:           map[string]string{},
		conditionTypes:   map[string]string{},
		conditionReasons: map[string]string{},
		unstable:         map[string]string{},
	}
	if len(consts["events.go"]) == 0 || len(consts["conditions.go"]) == 0 {
		t.Fatalf("api/v1alpha1 declares no constants in events.go or conditions.go")
	}
	for name, v := range consts["events.go"] {
		r.events[v] = name
	}
	r.byType = map[string]map[string]string{}
	scope := a.apiPackage(t).Types.Scope()
	for _, name := range scope.Names() {
		c, ok := scope.Lookup(name).(*types.Const)
		if !ok || c.Val().Kind() != constant.String {
			continue
		}
		if n, ok := c.Type().(*types.Named); ok {
			tn := n.Obj().Name()
			if r.byType[tn] == nil {
				r.byType[tn] = map[string]string{}
			}
			r.byType[tn][constant.StringVal(c.Val())] = name
		}
	}
	for name, v := range consts["conditions.go"] {
		switch {
		case strings.HasPrefix(name, "Condition"), strings.HasPrefix(name, "StandbyCondition"):
			r.conditionTypes[v] = name
		case strings.HasPrefix(name, "UnstableReason"), strings.HasPrefix(name, "DeploymentLease"):
			r.unstable[v] = name
		default:
			r.conditionReasons[v] = name
		}
	}
	return r
}

func (r registry) forKind(kind string) (map[string]string, string) {
	if typ, ok := strings.CutPrefix(kind, sinkStatusReasonPrefix); ok {
		return r.byType[typ], "an api/v1alpha1." + typ + " constant"
	}
	switch kind {
	case sinkEvent:
		return r.events, "an Event reason in events.go"
	case sinkConditionType:
		return r.conditionTypes, "a condition type (Condition*) in conditions.go"
	case sinkConditionReason:
		return r.conditionReasons, "a condition reason in conditions.go"
	case sinkUnstableReason:
		// A gating condition's type or reason is also an unstableReason.
		all := map[string]string{}
		for _, m := range []map[string]string{r.unstable, r.conditionTypes, r.conditionReasons} {
			for v, n := range m {
				all[v] = n
			}
		}
		return all, "an UnstableReason*, condition type or condition reason in conditions.go"
	case sinkMetricReason:
		// A-114: a metric's reason label carries a reason constant
		// verbatim: a condition reason, a lease revocation or refusal
		// reason, or a value of a typed reason.
		all := map[string]string{}
		for _, m := range []map[string]string{r.unstable, r.conditionReasons} {
			for v, n := range m {
				all[v] = n
			}
		}
		for typ, m := range r.byType {
			if strings.HasSuffix(typ, "Reason") {
				for v, n := range m {
					all[v] = n
				}
			}
		}
		return all, "a reason constant in api/v1alpha1"
	}
	return nil, ""
}

// TestServedValuesAreDeclared follows every value that reaches an Event, a
// condition (written or compared), a deployment API unstableReason or a
// lease refusal reason back to where it is spelled. Each value must be
// declared in api/v1alpha1, and spelled with that constant (or with a
// catalog package's own constant), not with a literal or a constant
// declared anywhere else.
func TestServedValuesAreDeclared(t *testing.T) {
	a, sinks := analysed(t)
	reg := newRegistry(t, a)
	seen := map[string]bool{}
	report := func(format string, args ...any) {
		msg := strings.ReplaceAll(fmt.Sprintf(format, args...), a.root, "")
		if !seen[msg] {
			seen[msg] = true
			t.Error(msg)
		}
	}
	for _, s := range sinks {
		for _, p := range s.flow.problems {
			report("%s sink at %s: the value cannot be followed: %s", s.kind, s.pos, p)
		}
		if s.flow.conditionReason && s.kind == sinkEvent {
			report("Event at %s: its reason is copied from a condition; spell the Event constant", s.pos)
		}
		allowed, what := reg.forKind(s.kind)
		for _, o := range s.flow.values {
			if o.value == "" {
				continue
			}
			if _, ok := allowed[o.value]; !ok {
				report("%s %q (spelled at %s, used at %s) is not %s", s.kind, o.value, o.pos, s.pos, what)
				continue
			}
			if o.trusted != "" || spelledByAPI(o.constant) {
				continue
			}
			how := "a literal"
			if o.constant != "" {
				how = "the constant " + o.constant
			}
			report("%s %q at %s is spelled as %s; use api/v1alpha1.%s", s.kind, o.value, o.pos, how, allowed[o.value])
		}
	}
}

func spelledByAPI(constant string) bool {
	if strings.HasPrefix(constant, apiPath+".") {
		return true
	}
	for _, p := range catalogPackages {
		if strings.HasPrefix(constant, p) {
			return true
		}
	}
	return false
}

// TestUnstableReasonsAreReachable keeps the unstableReason set closed in the
// other direction: every UnstableReason* and DeploymentLeaseRevoked*
// constant is a value the deployment API can serve.
func TestUnstableReasonsAreReachable(t *testing.T) {
	a, sinks := analysed(t)
	reg := newRegistry(t, a)
	served := map[string]bool{}
	for _, s := range sinks {
		if s.kind != sinkUnstableReason {
			continue
		}
		for _, o := range s.flow.values {
			served[o.value] = true
		}
	}
	for _, v := range sortedKeys(reg.unstable) {
		if !served[v] {
			t.Errorf("api/v1alpha1.%s = %q is never served by the deployment API", reg.unstable[v], v)
		}
	}
}

// TestConditionMirrorEventsReferenceTheirReason pins the condition-mirror
// classification: an Event reason that repeats a condition reason is
// declared as that condition-reason constant, so the two cannot drift
// apart, and every other Event reason is not a condition reason.
func TestConditionMirrorEventsReferenceTheirReason(t *testing.T) {
	a, _ := analysed(t)
	reg := newRegistry(t, a)
	api := a.apiPackage(t)
	for _, f := range api.Syntax {
		if !strings.HasSuffix(a.pos(f.Pos()).Filename, "/events.go") {
			continue
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, n := range vs.Names {
					c := api.TypesInfo.Defs[n].(*types.Const)
					v := constant.StringVal(c.Val())
					reason, mirrored := reg.conditionReasons[v]
					if !mirrored {
						continue
					}
					if i >= len(vs.Values) || !referencesConst(api, vs.Values[i], reason) {
						t.Errorf("events.go: %s = %q repeats the condition reason %s; declare it as %s", n.Name, v, reason, reason)
					}
				}
			}
		}
	}
}

// referencesConst reports whether e names the constant name, directly or
// converted (string(c)).
func referencesConst(p *packages.Package, e ast.Expr, name string) bool {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident:
		return x.Name == name
	case *ast.CallExpr:
		if tv, ok := p.TypesInfo.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 {
			return referencesConst(p, x.Args[0], name)
		}
	}
	return false
}

// TestPITRTokenCatalogMatchesAPI pins api/v1alpha1's PITRToken and
// PITRArchiveRefusalReason constants to internal/pitrarchive's catalog,
// which the sidecar uses because it does not import the API package: the
// same names (PITR + the catalog name) and the same values, no more, no
// fewer.
func TestPITRTokenCatalogMatchesAPI(t *testing.T) {
	a, _ := analysed(t)
	api := a.apiPackage(t)
	byType := map[string]map[string]string{} // type -> name -> value
	scope := api.Types.Scope()
	for _, name := range scope.Names() {
		c, ok := scope.Lookup(name).(*types.Const)
		if !ok {
			continue
		}
		n, ok := c.Type().(*types.Named)
		if !ok || c.Val().Kind() != constant.String {
			continue
		}
		tn := n.Obj().Name()
		if byType[tn] == nil {
			byType[tn] = map[string]string{}
		}
		byType[tn][name] = constant.StringVal(c.Val())
	}

	// The API declares what the operator serves: the replay script's
	// sentinel is served as PITRReplayFailed.
	want := map[string]string{}
	for _, tok := range pitrarchive.ServedTokens {
		want[string(tok)] = ""
	}
	got := map[string]string{}
	for name, v := range byType["PITRToken"] {
		got[v] = name
	}
	compareSets(t, "PITRToken", got, want)

	want = map[string]string{}
	for _, r := range pitrarchive.ArchiveReadyReasons {
		want[string(r)] = ""
	}
	got = map[string]string{}
	for name, v := range byType["PITRArchiveRefusalReason"] {
		got[v] = name
	}
	compareSets(t, "PITRArchiveRefusalReason", got, want)

	// Names follow the catalog's: TokenArchiveGap is PITRTokenArchiveGap.
	tokenNames := pitrTokenNames(t, a)
	for v, apiName := range invert(byType["PITRToken"]) {
		if want := "PITR" + tokenNames[v]; apiName != want {
			t.Errorf("api/v1alpha1.%s = %q; the catalog names it %s, so the API constant is %s", apiName, v, tokenNames[v], want)
		}
	}
}

func invert(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[v] = k
	}
	return out
}

func compareSets(t *testing.T, what string, got, want map[string]string) {
	t.Helper()
	for v := range want {
		if _, ok := got[v]; !ok {
			t.Errorf("api/v1alpha1 has no %s constant for the catalog value %q", what, v)
		}
	}
	for v, name := range got {
		if _, ok := want[v]; !ok {
			t.Errorf("api/v1alpha1.%s = %q is not in internal/pitrarchive's catalog", name, v)
		}
	}
}

// pitrTokenNames maps each catalog value to its internal/pitrarchive
// constant name.
func pitrTokenNames(t *testing.T, a *analysis) map[string]string {
	t.Helper()
	var pa *packages.Package
	packages.Visit(a.pkgs, nil, func(p *packages.Package) {
		if p.PkgPath == modulePath+"/internal/pitrarchive" && p.Types != nil {
			pa = p
		}
	})
	if pa == nil {
		t.Fatal("internal/pitrarchive was not loaded")
	}
	out := map[string]string{}
	scope := pa.Types.Scope()
	for _, name := range scope.Names() {
		c, ok := scope.Lookup(name).(*types.Const)
		if !ok {
			continue
		}
		if n, ok := c.Type().(*types.Named); ok && n.Obj().Name() == "Token" {
			out[constant.StringVal(c.Val())] = name
		}
	}
	return out
}

// TestRegistryCatchesUndeclaredValues runs the analysis on a fixture that
// emits an undeclared Event reason through a helper, a literal condition
// reason and a dynamic reason, so the registry test cannot pass vacuously.
func TestRegistryCatchesUndeclaredValues(t *testing.T) {
	a := loadAnalysis(t, nil, "./internal/apiregistry/testdata/emitter")
	reg := newRegistry(t, mustLoadAPI(t))
	var undeclared, literal, dynamic []string
	for _, s := range a.sinks() {
		allowed, _ := reg.forKind(s.kind)
		if len(s.flow.problems) > 0 {
			dynamic = append(dynamic, s.kind)
		}
		for _, o := range s.flow.values {
			if _, ok := allowed[o.value]; !ok {
				undeclared = append(undeclared, s.kind+":"+o.value)
			} else if !spelledByAPI(o.constant) {
				literal = append(literal, s.kind+":"+o.value)
			}
		}
	}
	sort.Strings(undeclared)
	sort.Strings(literal)
	if got, want := strings.Join(undeclared, ","), "event:FixtureUndeclaredEvent,metricReason:FixtureUndeclaredMetricReason"; got != want {
		t.Errorf("undeclared values = %s, want %s", got, want)
	}
	if got, want := strings.Join(literal, ","), "conditionReason:Bound"; got != want {
		t.Errorf("literally spelled values = %s, want %s", got, want)
	}
	if got, want := strings.Join(dynamic, ","), "event"; got != want {
		t.Errorf("unfollowable sinks = %s, want %s", got, want)
	}
}

// TestEventPairsFollowEachCall: a helper that forwards both the Event type
// and the reason is paired per call, so a reason recorded with two types is
// caught (A-75) and a reason is not charged with a type another call
// passes.
func TestEventPairsFollowEachCall(t *testing.T) {
	a := loadAnalysis(t, nil, "./internal/apiregistry/testdata/emitter")
	types := map[string][]string{}
	for _, s := range a.sinks() {
		if !s.recorded {
			continue
		}
		for _, p := range s.pairProblems {
			t.Errorf("pairing problem at %s: %s", s.pos, p)
		}
		for _, p := range s.pairs {
			types[p.reason] = append(types[p.reason], p.eventType)
		}
	}
	for reason, want := range map[string]string{
		"PlannedFailoverSkipped":  "Normal,Warning",
		"PlannedFailoverRejected": "Warning",
		"PlannedFailoverStarted":  "Warning",
	} {
		got := types[reason]
		sort.Strings(got)
		if strings.Join(got, ",") != want {
			t.Errorf("%s recorded as %v, want %s", reason, got, want)
		}
	}
}

var (
	apiOnce sync.Once
	apiOnly *analysis
)

func mustLoadAPI(t *testing.T) *analysis {
	t.Helper()
	apiOnce.Do(func() { apiOnly = loadAnalysis(t, nil, "./api/...") })
	return apiOnly
}
