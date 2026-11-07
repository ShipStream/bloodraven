package apiregistry

import (
	"go/ast"
	"go/constant"
	"go/types"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	deployAPIPkg = modulePath + "/cmd/bloodraven"
	// deployAPIDoc documents every /deploy/v1 token field with its closed
	// value set, in its "Token values" table.
	deployAPIDoc = "../../site/content/docs/4.configuration/11.deployment-api.md"
)

// deployResponseFuncs build the GET /deploy/v1/groups/{group} body: the
// function, and the JSON path its map sits at.
var deployResponseFuncs = map[string]string{
	deployAPIPkg + ".deployGroupResponse":      "",
	deployAPIPkg + ".plannedFailoverOperation": "operations.plannedFailover.",
}

// deployIdentityFields are the string fields of the body that carry names,
// not tokens.
var deployIdentityFields = map[string]bool{"group": true, "namespace": true, "activeSite": true}

// deployTokenFields are the token fields of the body.
var deployTokenFields = []string{
	"health.degradedReason",
	"operations.dragonflyUpgrade",
	"operations.plannedFailover.outcome",
	"operations.plannedFailover.phase",
	"operations.plannedFailover.reason",
	"operations.restoreInPlace",
	"operations.updatePhase",
	"unstableReason",
}

// tokenSet is the closed value set of one token field.
type tokenSet struct {
	values map[string]bool
	// conditionReason: the field also passes a condition's reason
	// through (checked where each condition is written).
	conditionReason bool
}

// deployTokenSets follows every string field of the group response to the
// values it can carry.
func deployTokenSets(t *testing.T, a *analysis, reg registry) map[string]*tokenSet {
	t.Helper()
	sets := map[string]*tokenSet{}
	found := map[string]bool{}
	for _, p := range a.pkgs {
		if p.PkgPath != deployAPIPkg {
			continue
		}
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok {
					continue
				}
				fn, _ := p.TypesInfo.Defs[fd.Name].(*types.Func)
				prefix, ok := deployResponseFuncs[fn.FullName()]
				if !ok {
					continue
				}
				found[fn.FullName()] = true
				deployFields(t, a, reg, p, fd.Body, prefix, sets)
			}
		}
	}
	for name := range deployResponseFuncs {
		if !found[name] {
			t.Fatalf("%s not found; update deployResponseFuncs", name)
		}
	}
	return sets
}

// deployFields records the string fields of every map literal (and every
// m["key"] = v assignment) in body.
func deployFields(t *testing.T, a *analysis, reg registry, p *packages.Package, body *ast.BlockStmt, prefix string, sets map[string]*tokenSet) {
	t.Helper()
	field := func(key string, e ast.Expr) {
		tv := p.TypesInfo.Types[e]
		if b, ok := tv.Type.Underlying().(*types.Basic); !ok || b.Info()&types.IsString == 0 {
			return // booleans, numbers, nested maps, lease views
		}
		path := prefix + key
		if deployIdentityFields[path] {
			return
		}
		s := sets[path]
		if s == nil {
			s = &tokenSet{values: map[string]bool{}}
			sets[path] = s
		}
		deployFieldValues(t, a, reg, p, e, path, s)
	}
	var walk func(lit *ast.CompositeLit, prefix string)
	walk = func(lit *ast.CompositeLit, at string) {
		if _, ok := p.TypesInfo.Types[lit].Type.Underlying().(*types.Map); !ok {
			return
		}
		for _, elt := range lit.Elts {
			kv := elt.(*ast.KeyValueExpr)
			key := stringConst(p, kv.Key)
			if nested, ok := ast.Unparen(kv.Value).(*ast.CompositeLit); ok {
				walk(nested, at+key+".")
				continue
			}
			field(strings.TrimPrefix(at+key, prefix), kv.Value)
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CompositeLit:
			walk(x, prefix)
			return false
		case *ast.AssignStmt:
			for i, l := range x.Lhs {
				ix, ok := ast.Unparen(l).(*ast.IndexExpr)
				if !ok || i >= len(x.Rhs) {
					continue
				}
				field(stringConst(p, ix.Index), x.Rhs[i])
			}
		}
		return true
	})
}

func stringConst(p *packages.Package, e ast.Expr) string {
	tv := p.TypesInfo.Types[e]
	if tv.Value == nil || tv.Value.Kind() != constant.String {
		return "<non-constant key>"
	}
	return constant.StringVal(tv.Value)
}

// deployFieldValues adds the values e can carry to s. A value of an
// api/v1alpha1 enum type carries that type's constants; anything else is
// followed to its constants like every other served value.
func deployFieldValues(t *testing.T, a *analysis, reg registry, p *packages.Package, e ast.Expr, path string, s *tokenSet) {
	t.Helper()
	inner := ast.Unparen(e)
	if call, ok := inner.(*ast.CallExpr); ok {
		if tv, ok := p.TypesInfo.Types[call.Fun]; ok && tv.IsType() && len(call.Args) == 1 {
			inner = ast.Unparen(call.Args[0])
		}
	}
	if tv := p.TypesInfo.Types[inner]; tv.Value == nil {
		if typ := strings.TrimPrefix(namedPath(tv.Type), apiPath+"."); typ != namedPath(tv.Type) {
			consts := reg.byType[typ]
			if len(consts) == 0 {
				t.Errorf("%s: %s is of api/v1alpha1.%s, which declares no constants", a.pos(e.Pos()), path, typ)
			}
			for v := range consts {
				s.values[v] = true
			}
			return
		}
	}
	fl := a.resolve(p, e)
	for _, prob := range fl.problems {
		t.Errorf("%s: the value cannot be followed: %s", path, strings.ReplaceAll(prob, a.root, ""))
	}
	s.conditionReason = s.conditionReason || fl.conditionReason
	allowed, _ := reg.forKind(sinkUnstableReason)
	for _, o := range fl.values {
		if o.value == "" {
			continue
		}
		if _, ok := allowed[o.value]; !ok && reg.byTypeValue(o.value) == "" {
			t.Errorf("%s %q (spelled at %s) is not declared in api/v1alpha1", path, o.value, strings.ReplaceAll(o.pos.String(), a.root, ""))
		}
		s.values[o.value] = true
	}
}

// conditionReasonsOf returns the reasons the operator writes on a
// condition of type typ with status (every metav1.Condition literal whose
// Type is the constant typ and whose Status is status or not a constant).
func conditionReasonsOf(t *testing.T, a *analysis, typ string, status metav1.ConditionStatus) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, p := range a.pkgs {
		if !strings.HasPrefix(p.PkgPath, modulePath+"/internal/") {
			continue
		}
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok || namedPath(p.TypesInfo.Types[lit].Type) != metaV1Path+".Condition" {
					return true
				}
				fields := map[string]ast.Expr{}
				for _, elt := range lit.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok {
						if id, ok := kv.Key.(*ast.Ident); ok {
							fields[id.Name] = kv.Value
						}
					}
				}
				if fields["Type"] == nil || stringConst(p, fields["Type"]) != typ || fields["Reason"] == nil {
					return true
				}
				if st := fields["Status"]; st != nil {
					if tv := p.TypesInfo.Types[st]; tv.Value != nil && constant.StringVal(tv.Value) != string(status) {
						return true
					}
				}
				fl := a.resolve(p, fields["Reason"])
				for _, prob := range fl.problems {
					t.Errorf("%s reason: %s", typ, strings.ReplaceAll(prob, a.root, ""))
				}
				for _, o := range fl.values {
					if o.value != "" && o.value != "Healthy" && o.value != "Initializing" {
						out[o.value] = true
					}
				}
				return true
			})
		}
	}
	return out
}

// byTypeValue returns the API type that declares v as a constant, or "".
func (r registry) byTypeValue(v string) string {
	for typ, consts := range r.byType {
		if _, ok := consts[v]; ok {
			return typ
		}
	}
	return ""
}

// TestDeployAPITokenFieldsAreDeclared pins A-114 and F-32 for /deploy/v1:
// every string field of the group response is either a name or a token
// field, and every value a token field can carry is an api/v1alpha1
// constant, the unstableReason included.
func TestDeployAPITokenFieldsAreDeclared(t *testing.T) {
	a, _ := analysed(t)
	reg := newRegistry(t, a)
	sets := deployTokenSets(t, a, reg)
	got := sortedKeys(sets)
	want := append([]string(nil), deployTokenFields...)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("token fields of the group response = %v, want %v; classify a new string field as a name (deployIdentityFields) or a token (deployTokenFields) and document it", got, want)
	}
	for _, f := range deployTokenFields {
		if len(sets[f].values) == 0 && !sets[f].conditionReason {
			t.Errorf("%s carries no value", f)
		}
	}
}

// docTokenRow matches a "Token values" table row: | `field` | values |.
var docTokenRow = regexp.MustCompile("^\\| `([a-zA-Z.]+)` \\| (.*) \\|$")

var backticked = regexp.MustCompile("`([^`]+)`")

// TestDeployAPIDocTokenSets pins the "Token values" table of
// 11.deployment-api.md to the constants: each row lists exactly the values
// its field can carry. A field that also passes a condition's reason
// through lists the values it carries itself; the doc names the condition.
func TestDeployAPIDocTokenSets(t *testing.T) {
	a, _ := analysed(t)
	reg := newRegistry(t, a)
	sets := deployTokenSets(t, a, reg)
	doc := docTable(t, "Token values")
	for _, f := range deployTokenFields {
		want := sortedKeys(sets[f].values)
		if f == "health.degradedReason" {
			// The reason of a True Degraded condition.
			want = sortedKeys(conditionReasonsOf(t, a, "Degraded", metav1.ConditionTrue))
		}
		documented, ok := doc[f]
		if !ok {
			t.Errorf("%s: no row in the Token values table of %s; want %s", f, deployAPIDoc, backtickList(want))
			continue
		}
		if strings.Join(documented, ",") != strings.Join(want, ",") {
			t.Errorf("%s: documented %v, want %s", f, documented, backtickList(want))
		}
	}
	for f := range doc {
		if _, ok := sets[f]; !ok {
			t.Errorf("the Token values table documents %s, which the group response does not serve", f)
		}
	}

	// operations.plannedFailover.reason belongs to its phase
	// (plannedFailoverOperation): Admitting reports admission.waitReason,
	// Deferred admission.deferReason, a terminal run result.reason.
	byPhase := docTable(t, "Planned failover reason by phase")
	for phase, typ := range map[string]string{
		"Admitting": "PlannedFailoverWaitReason",
		"Deferred":  "PlannedFailoverDeferReason",
		"Succeeded": "PlannedFailoverReason",
		"Failed":    "PlannedFailoverReason",
	} {
		want := sortedKeys(reg.byType[typ])
		if got := byPhase[phase]; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("planned failover reason for phase %s: documented %v, want %s (api/v1alpha1.%s)", phase, got, backtickList(want), typ)
		}
	}
}

// docTable parses the table under the heading named heading in
// 11.deployment-api.md: each row | `key` | `v1`, `v2` | maps key to its
// sorted backticked values.
func docTable(t *testing.T, heading string) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(deployAPIDoc)
	if err != nil {
		t.Fatal(err)
	}
	doc := map[string][]string{}
	in := false
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "#") {
			in = strings.TrimSpace(strings.TrimLeft(line, "#")) == heading
			continue
		}
		if !in {
			continue
		}
		m := docTokenRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var vals []string
		for _, v := range backticked.FindAllStringSubmatch(m[2], -1) {
			vals = append(vals, v[1])
		}
		sort.Strings(vals)
		doc[m[1]] = vals
	}
	if len(doc) == 0 {
		t.Fatalf("%s has no %q table", deployAPIDoc, heading)
	}
	return doc
}

func backtickList(vs []string) string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = "`" + v + "`"
	}
	return strings.Join(out, ", ")
}
