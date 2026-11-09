package apiregistry

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/types/typeutil"
)

// This file is a small whole-program value-flow analysis over the operator's
// production code. It finds every place a reason reaches a served surface (an
// Event, a status condition, the deployment API's unstableReason) and derives
// the set of string values that can arrive there, following the value back
// through constants, conversions, concatenations, local variables, struct
// fields, function parameters (to every call site) and function results (to
// every return statement). An expression it cannot follow is reported as a
// problem rather than silently accepted, so a new dynamic reason fails the
// registry test until it is either made constant or listed as a trusted
// source.

const (
	modulePath = "github.com/shipstream/bloodraven"
	apiPath    = modulePath + "/api/v1alpha1"
	metaV1Path = "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMetaPkg = "k8s.io/apimachinery/pkg/api/meta"
)

// origin is one value that reaches a sink.
type origin struct {
	value string
	// pos is where the value was spelled: the constant reference or literal.
	pos token.Position
	// constant is the constant that defines the value, as
	// "<package path>.<name>": the API constant it was spelled with, or,
	// for a constant declared elsewhere, the constant its initialiser
	// chain ends at. It is "" for a literal, a concatenation or a trusted
	// function result.
	constant string
	// spelled is the constant written at pos, when it differs from
	// constant (a local alias).
	spelled string
	// trusted names the trusted function that produced the value, if any.
	trusted string
}

// flow is the result of following one expression.
type flow struct {
	values []origin
	// conditionReason is set when the expression copies the Reason of an
	// existing metav1.Condition. Every condition reason is itself checked at
	// the point it is written, so the copy carries a declared value.
	conditionReason bool
	problems        []string
}

func (f *flow) add(o flow) {
	f.values = append(f.values, o.values...)
	f.conditionReason = f.conditionReason || o.conditionReason
	f.problems = append(f.problems, o.problems...)
}

// trustedSource is a function whose result the analysis cannot follow
// (it parses a value out of a Job message, say) but whose result set is
// known and pinned by a separate exhaustive test.
type trustedSource struct {
	// name is the function's types.Func.FullName().
	name   string
	values []string
}

type pkgFile struct {
	pkg  *packages.Package
	file *ast.File
}

type funcDecl struct {
	pkg  *packages.Package
	decl *ast.FuncDecl
}

type callRef struct {
	pkg  *packages.Package
	call *ast.CallExpr
}

type valueRef struct {
	pkg  *packages.Package
	expr ast.Expr
	// resultIdx selects a result when expr is a multi-value call.
	resultIdx int
	// rangeElem is set when the variable is the value of a range over
	// expr.
	rangeElem bool
	// rangeKey is set when the variable is the key of a range over expr.
	rangeKey bool
	// elem is set when expr is stored as an element of the container
	// variable: c[k] = expr.
	elem bool
}

type paramRef struct {
	fn  *types.Func  // the declared function, or nil
	lit *ast.FuncLit // the function literal, when fn is nil
	pkg *packages.Package
	idx int
}

type analysis struct {
	// root is the module directory with a trailing slash; messages print
	// positions relative to it.
	root     string
	fset     *token.FileSet
	pkgs     []*packages.Package
	funcs    map[*types.Func]funcDecl
	params   map[*types.Var]paramRef
	calls    map[*types.Func][]callRef
	varCalls map[types.Object][]callRef // calls through a func-typed variable or field
	litVars  map[*ast.FuncLit][]types.Object
	invoked  map[*ast.FuncLit]bool // function literals called where they are written
	iface    map[string][]callRef  // interface method calls by method name
	escapes  map[*types.Func][]token.Position
	writes   map[types.Object][]valueRef
	trusted  map[string]trustedSource
	memo     map[any]*flow
	visiting map[any]int // key -> depth while it is being computed
	cutDepth int         // the shallowest key a cycle cut reached
	// reasonMetrics caches metricReasonLabels.
	reasonMetrics map[*types.Var]int
}

func loadAnalysis(t *testing.T, trusted []trustedSource, patterns ...string) *analysis {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Dir:   "../..",
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		t.Fatalf("load packages: %v", err)
	}
	var errs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			errs = append(errs, e.Error())
		}
	})
	if len(errs) > 0 {
		t.Fatalf("load packages:\n%s", strings.Join(errs, "\n"))
	}
	root, err := filepath.Abs(cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	a := &analysis{
		root:     root + string(filepath.Separator),
		fset:     pkgs[0].Fset,
		pkgs:     pkgs,
		funcs:    map[*types.Func]funcDecl{},
		params:   map[*types.Var]paramRef{},
		calls:    map[*types.Func][]callRef{},
		varCalls: map[types.Object][]callRef{},
		litVars:  map[*ast.FuncLit][]types.Object{},
		invoked:  map[*ast.FuncLit]bool{},
		iface:    map[string][]callRef{},
		escapes:  map[*types.Func][]token.Position{},
		writes:   map[types.Object][]valueRef{},
		trusted:  map[string]trustedSource{},
		memo:     map[any]*flow{},
		visiting: map[any]int{},
		cutDepth: 1 << 30,
	}
	for _, s := range trusted {
		a.trusted[s.name] = s
	}
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			a.index(p, f)
		}
	}
	return a
}

func (a *analysis) pos(p token.Pos) token.Position { return a.fset.Position(p) }

func (a *analysis) objOf(p *packages.Package, id *ast.Ident) types.Object {
	if o := p.TypesInfo.Uses[id]; o != nil {
		return o
	}
	return p.TypesInfo.Defs[id]
}

// index records declarations, call sites, function-value escapes and every
// write to a variable or field.
func (a *analysis) index(p *packages.Package, f *ast.File) {
	info := p.TypesInfo
	callFuns := map[ast.Expr]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			fn, _ := info.Defs[x.Name].(*types.Func)
			if fn == nil {
				return true
			}
			a.funcs[fn] = funcDecl{pkg: p, decl: x}
			sig := fn.Type().(*types.Signature)
			for i := 0; i < sig.Params().Len(); i++ {
				a.params[sig.Params().At(i)] = paramRef{fn: fn, idx: i}
			}
		case *ast.FuncLit:
			if sig, ok := info.Types[x].Type.(*types.Signature); ok {
				for i := 0; i < sig.Params().Len(); i++ {
					a.params[sig.Params().At(i)] = paramRef{lit: x, pkg: p, idx: i}
				}
			}
		case *ast.CallExpr:
			callFuns[ast.Unparen(x.Fun)] = true
			if lit, ok := ast.Unparen(x.Fun).(*ast.FuncLit); ok {
				a.invoked[lit] = true
			}
			if fn := typeutil.StaticCallee(info, x); fn != nil {
				a.calls[fn] = append(a.calls[fn], callRef{pkg: p, call: x})
			} else if v := a.lhsObj(p, x.Fun); v != nil {
				a.varCalls[v] = append(a.varCalls[v], callRef{pkg: p, call: x})
			} else if sel, ok := ast.Unparen(x.Fun).(*ast.SelectorExpr); ok {
				if fn, ok := info.Uses[sel.Sel].(*types.Func); ok && isInterfaceMethod(fn) {
					a.iface[fn.Name()] = append(a.iface[fn.Name()], callRef{pkg: p, call: x})
				}
			}
		case *ast.AssignStmt:
			if x.Tok != token.ASSIGN && x.Tok != token.DEFINE {
				for _, l := range x.Lhs {
					if o := a.lhsObj(p, l); o != nil {
						// x += y and friends: record the whole statement as
						// an unfollowable write.
						a.writes[o] = append(a.writes[o], valueRef{pkg: p, expr: &ast.BadExpr{From: x.Pos(), To: x.End()}})
					}
				}
				return true
			}
			if len(x.Lhs) == len(x.Rhs) {
				for i, l := range x.Lhs {
					if ix, ok := ast.Unparen(l).(*ast.IndexExpr); ok {
						if o := a.lhsObj(p, ix.X); o != nil {
							a.writes[o] = append(a.writes[o], valueRef{pkg: p, expr: x.Rhs[i], elem: true})
						}
						continue
					}
					if o := a.lhsObj(p, l); o != nil {
						a.write(o, valueRef{pkg: p, expr: x.Rhs[i]})
					}
				}
			} else if len(x.Rhs) == 1 {
				for i, l := range x.Lhs {
					if o := a.lhsObj(p, l); o != nil {
						a.writes[o] = append(a.writes[o], valueRef{pkg: p, expr: x.Rhs[0], resultIdx: i})
					}
				}
			}
		case *ast.RangeStmt:
			if x.Key != nil {
				if o := a.lhsObj(p, x.Key); o != nil {
					a.writes[o] = append(a.writes[o], valueRef{pkg: p, expr: x.X, rangeKey: true})
				}
			}
			if x.Value != nil {
				if o := a.lhsObj(p, x.Value); o != nil {
					a.writes[o] = append(a.writes[o], valueRef{pkg: p, expr: x.X, rangeElem: true})
				}
			}
		case *ast.ValueSpec:
			if len(x.Values) == len(x.Names) {
				for i, n := range x.Names {
					if o := info.Defs[n]; o != nil {
						a.write(o, valueRef{pkg: p, expr: x.Values[i]})
					}
				}
			} else if len(x.Values) == 1 {
				for i, n := range x.Names {
					if o := info.Defs[n]; o != nil {
						a.writes[o] = append(a.writes[o], valueRef{pkg: p, expr: x.Values[0], resultIdx: i})
					}
				}
			}
		case *ast.CompositeLit:
			st, ok := underlyingStruct(info.Types[x].Type)
			if !ok {
				return true
			}
			for i, elt := range x.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					if id, ok := kv.Key.(*ast.Ident); ok {
						if o, ok := info.Uses[id].(*types.Var); ok && o.IsField() {
							a.write(o, valueRef{pkg: p, expr: kv.Value})
						}
					}
				} else if i < st.NumFields() {
					a.writes[st.Field(i)] = append(a.writes[st.Field(i)], valueRef{pkg: p, expr: elt})
				}
			}
		case *ast.UnaryExpr:
			if x.Op == token.AND {
				if o := a.lhsObj(p, x.X); o != nil {
					// &v lets anything write v.
					a.writes[o] = append(a.writes[o], valueRef{pkg: p, expr: &ast.BadExpr{From: x.Pos(), To: x.End()}})
				}
			}
		}
		return true
	})
	// Second pass: a function referenced anywhere but in call position
	// escapes; its parameters can receive values the analysis cannot see.
	selectorSels := map[*ast.Ident]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		if s, ok := n.(*ast.SelectorExpr); ok {
			selectorSels[s.Sel] = true
		}
		return true
	})
	ast.Inspect(f, func(n ast.Node) bool {
		var id *ast.Ident
		var whole ast.Expr
		switch x := n.(type) {
		case *ast.SelectorExpr:
			id, whole = x.Sel, x
		case *ast.Ident:
			if selectorSels[x] {
				return true
			}
			id, whole = x, x
		default:
			return true
		}
		fn, ok := info.Uses[id].(*types.Func)
		if !ok || callFuns[whole] {
			return true
		}
		a.escapes[fn] = append(a.escapes[fn], a.pos(id.Pos()))
		return true
	})
}

// write records a write to o, and the variable a function literal is
// stored in.
func (a *analysis) write(o types.Object, w valueRef) {
	a.writes[o] = append(a.writes[o], w)
	if lit, ok := ast.Unparen(w.expr).(*ast.FuncLit); ok {
		a.litVars[lit] = append(a.litVars[lit], o)
	}
}

func isInterfaceMethod(fn *types.Func) bool {
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		return false
	}
	_, ok := sig.Recv().Type().Underlying().(*types.Interface)
	return ok
}

func underlyingStruct(t types.Type) (*types.Struct, bool) {
	if t == nil {
		return nil, false
	}
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	st, ok := t.Underlying().(*types.Struct)
	return st, ok
}

// lhsObj returns the variable or field an assignment target writes.
func (a *analysis) lhsObj(p *packages.Package, e ast.Expr) types.Object {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident:
		if x.Name == "_" {
			return nil
		}
		if v, ok := a.objOf(p, x).(*types.Var); ok {
			return v
		}
	case *ast.SelectorExpr:
		if sel := p.TypesInfo.Selections[x]; sel != nil && sel.Kind() == types.FieldVal {
			return sel.Obj()
		}
		if v, ok := p.TypesInfo.Uses[x.Sel].(*types.Var); ok {
			return v
		}
	case *ast.StarExpr:
		return a.lhsObj(p, x.X)
	}
	return nil
}

func (a *analysis) problem(p token.Pos, format string, args ...any) flow {
	return flow{problems: []string{fmt.Sprintf("%s: %s", a.pos(p), fmt.Sprintf(format, args...))}}
}

// isConditionField reports whether v is a field of metav1.Condition.
func isConditionField(v *types.Var, name string) bool {
	if !v.IsField() || v.Name() != name || v.Pkg() == nil || v.Pkg().Path() != metaV1Path {
		return false
	}
	return true
}

// resolve follows e to the string values it can take.
func (a *analysis) resolve(p *packages.Package, e ast.Expr) flow {
	e = ast.Unparen(e)
	info := p.TypesInfo
	if tv, ok := info.Types[e]; ok && tv.Value != nil {
		if tv.Value.Kind() != constant.String {
			return a.problem(e.Pos(), "non-string constant %s", tv.Value)
		}
		def, spelled := a.constOf(p, e)
		if spelled == def {
			spelled = ""
		}
		return flow{values: []origin{{value: constant.StringVal(tv.Value), pos: a.pos(e.Pos()), constant: def, spelled: spelled}}}
	}
	switch x := e.(type) {
	case *ast.BadExpr:
		return a.problem(x.Pos(), "written through a compound assignment or a pointer")
	case *ast.Ident:
		return a.resolveObj(a.objOf(p, x), x.Pos())
	case *ast.SelectorExpr:
		if sel := info.Selections[x]; sel != nil {
			if sel.Kind() == types.FieldVal {
				v := sel.Obj().(*types.Var)
				if isConditionField(v, "Reason") {
					return flow{conditionReason: true}
				}
				return a.resolveObj(v, x.Pos())
			}
			return a.problem(x.Pos(), "method value %s", x.Sel.Name)
		}
		return a.resolveObj(info.Uses[x.Sel], x.Pos())
	case *ast.CallExpr:
		if tv, ok := info.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 {
			return a.resolve(p, x.Args[0])
		}
		return a.resolveCall(p, x, 0)
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return a.problem(x.Pos(), "operator %s", x.Op)
		}
		l, r := a.resolve(p, x.X), a.resolve(p, x.Y)
		out := flow{problems: append(l.problems, r.problems...)}
		if l.conditionReason || r.conditionReason {
			out.problems = append(out.problems, fmt.Sprintf("%s: concatenates a condition reason", a.pos(x.Pos())))
		}
		for _, lv := range l.values {
			for _, rv := range r.values {
				out.values = append(out.values, origin{value: lv.value + rv.value, pos: lv.pos})
			}
		}
		return out
	case *ast.IndexExpr:
		return a.resolveElems(p, x.X, false)
	}
	return a.problem(e.Pos(), "cannot follow %T", e)
}

// constOf returns the constant that defines e's value (following
// constants declared outside the API package to their initialisers) and the
// constant e spells, both as "<package path>.<name>", or "" for a literal.
func (a *analysis) constOf(p *packages.Package, e ast.Expr) (def, spelled string) {
	e = ast.Unparen(e)
	var id *ast.Ident
	switch x := e.(type) {
	case *ast.Ident:
		id = x
	case *ast.SelectorExpr:
		id = x.Sel
	case *ast.CallExpr:
		// A conversion of a constant: T(c).
		if tv, ok := p.TypesInfo.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 {
			return a.constOf(p, x.Args[0])
		}
		return "", ""
	default:
		return "", ""
	}
	c, ok := p.TypesInfo.Uses[id].(*types.Const)
	if !ok || c.Pkg() == nil {
		return "", ""
	}
	spelled = c.Pkg().Path() + "." + c.Name()
	if c.Pkg().Path() == apiPath {
		return spelled, spelled
	}
	for _, w := range a.writes[c] {
		if d, _ := a.constOf(w.pkg, w.expr); d != "" {
			return d, spelled
		}
	}
	return spelled, spelled
}

func (a *analysis) resolveObj(obj types.Object, at token.Pos) flow {
	switch o := obj.(type) {
	case *types.Const:
		return flow{values: []origin{{value: constant.StringVal(o.Val()), pos: a.pos(at)}}}
	case *types.Var:
		return a.memoized(o, func() flow { return a.resolveVar(o, at) })
	case nil:
		return a.problem(at, "unresolved identifier")
	}
	return a.problem(at, "cannot follow %T %s", obj, obj.Name())
}

// memoized computes f for key once. A key reached again while it is being
// computed (a cycle: a variable assigned from itself, mutually recursive
// helpers) contributes nothing on the inner visit; the outer computation
// collects every value. A result that depended on such a cut through an
// ancestor still being computed is incomplete, so it is not cached; it is
// recomputed when next asked for.
func (a *analysis) memoized(key any, f func() flow) flow {
	if r, ok := a.memo[key]; ok {
		return *r
	}
	if d, ok := a.visiting[key]; ok {
		a.cutDepth = min(a.cutDepth, d)
		return flow{}
	}
	depth := len(a.visiting)
	a.visiting[key] = depth
	outer := a.cutDepth
	a.cutDepth = depth + 1 // no cut yet below this key
	r := f()
	delete(a.visiting, key)
	if a.cutDepth >= depth {
		a.memo[key] = &r
	}
	a.cutDepth = min(outer, a.cutDepth)
	return r
}

func (a *analysis) resolveVar(v *types.Var, at token.Pos) flow {
	var out flow
	if ref, ok := a.params[v]; ok {
		// A parameter carries its callers' arguments and whatever the body
		// assigns to it.
		out.add(a.resolveParam(v, ref, at))
	} else if v.Pkg() != nil && !strings.HasPrefix(v.Pkg().Path(), modulePath) {
		return a.problem(at, "reads %s.%s from another module", v.Pkg().Path(), v.Name())
	}
	for _, w := range a.writes[v] {
		switch {
		case w.elem:
			out.add(a.problem(w.expr.Pos(), "%s is a container read as a value", v.Name()))
		case w.rangeElem:
			out.add(a.resolveElems(w.pkg, w.expr, false))
		case w.rangeKey:
			out.add(a.resolveElems(w.pkg, w.expr, true))
		case w.resultIdx > 0 || isMultiValueCall(w.pkg, w.expr):
			call, ok := ast.Unparen(w.expr).(*ast.CallExpr)
			if !ok {
				// v, ok := m[k] or x.(T) or <-ch.
				if w.resultIdx == 0 {
					out.add(a.resolveCommaOK(w.pkg, w.expr))
				} else {
					out.add(a.problem(w.expr.Pos(), "second value of a comma-ok expression"))
				}
				continue
			}
			out.add(a.resolveCall(w.pkg, call, w.resultIdx))
		default:
			out.add(a.resolve(w.pkg, w.expr))
		}
	}
	return out
}

func isMultiValueCall(p *packages.Package, e ast.Expr) bool {
	tv, ok := p.TypesInfo.Types[ast.Unparen(e)]
	if !ok {
		return false
	}
	_, tuple := tv.Type.(*types.Tuple)
	return tuple
}

func (a *analysis) resolveCommaOK(p *packages.Package, e ast.Expr) flow {
	switch x := ast.Unparen(e).(type) {
	case *ast.IndexExpr:
		return a.resolveElems(p, x.X, false)
	case *ast.TypeAssertExpr:
		return a.resolve(p, x.X)
	}
	return a.problem(e.Pos(), "cannot follow %T", e)
}

// resolveElems follows the elements (or keys) of a slice, array or map.
func (a *analysis) resolveElems(p *packages.Package, e ast.Expr, keys bool) flow {
	e = ast.Unparen(e)
	switch x := e.(type) {
	case *ast.CompositeLit:
		var out flow
		for _, elt := range x.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if keys {
					out.add(a.resolve(p, kv.Key))
				} else {
					out.add(a.resolve(p, kv.Value))
				}
				continue
			}
			if keys {
				continue
			}
			out.add(a.resolve(p, elt))
		}
		return out
	case *ast.Ident, *ast.SelectorExpr:
		var obj types.Object
		if id, ok := x.(*ast.Ident); ok {
			obj = a.objOf(p, id)
		} else if sel := p.TypesInfo.Selections[x.(*ast.SelectorExpr)]; sel != nil && sel.Kind() == types.FieldVal {
			obj = sel.Obj()
		} else {
			obj = p.TypesInfo.Uses[x.(*ast.SelectorExpr).Sel]
		}
		v, ok := obj.(*types.Var)
		if !ok {
			return a.problem(e.Pos(), "cannot follow the elements of %T", obj)
		}
		type elemKey struct {
			v    *types.Var
			keys bool
		}
		return a.memoized(elemKey{v, keys}, func() flow {
			var out flow
			for _, w := range a.writes[v] {
				if w.elem {
					if !keys {
						out.add(a.resolve(w.pkg, w.expr))
					}
					continue
				}
				if w.rangeElem || w.rangeKey || w.resultIdx > 0 {
					out.add(a.problem(w.expr.Pos(), "container %s written from a range or a multi-value call", v.Name()))
					continue
				}
				out.add(a.resolveElems(w.pkg, w.expr, keys))
			}
			return out
		})
	case *ast.CallExpr:
		if id, ok := ast.Unparen(x.Fun).(*ast.Ident); ok && id.Name == "make" {
			if _, builtin := p.TypesInfo.Uses[id].(*types.Builtin); builtin {
				return flow{}
			}
		}
		if id, ok := ast.Unparen(x.Fun).(*ast.Ident); ok && id.Name == "append" {
			if _, builtin := p.TypesInfo.Uses[id].(*types.Builtin); builtin {
				out := a.resolveElems(p, x.Args[0], keys)
				for _, arg := range x.Args[1:] {
					if x.Ellipsis.IsValid() {
						out.add(a.resolveElems(p, arg, keys))
					} else {
						out.add(a.resolve(p, arg))
					}
				}
				return out
			}
		}
		if lit, ok := ast.Unparen(x.Fun).(*ast.FuncLit); ok {
			var out flow
			for _, r := range bodyReturns(lit.Body) {
				if len(r.Results) == 1 {
					out.add(a.resolveElems(p, r.Results[0], keys))
				}
			}
			return out
		}
		if fn := typeutil.StaticCallee(p.TypesInfo, x); fn != nil {
			if d, ok := a.funcs[fn]; ok {
				var out flow
				for _, r := range returns(d.decl) {
					if len(r.Results) == 1 {
						out.add(a.resolveElems(d.pkg, r.Results[0], keys))
					}
				}
				return out
			}
		}
	}
	return a.problem(e.Pos(), "cannot follow the elements of %T", e)
}

func (a *analysis) resolveParam(v *types.Var, ref paramRef, at token.Pos) flow {
	if ref.fn == nil {
		return a.resolveLitParam(v, ref, at)
	}
	var out flow
	if esc := a.escapes[ref.fn]; len(esc) > 0 {
		out.add(a.problem(at, "parameter %s of %s, which is used as a function value at %s", v.Name(), ref.fn.Name(), esc[0]))
	}
	sig := ref.fn.Type().(*types.Signature)
	for _, c := range a.funcSites(ref.fn) {
		if sig.Variadic() && ref.idx == sig.Params().Len()-1 {
			out.add(a.problem(c.call.Pos(), "variadic parameter %s", v.Name()))
			continue
		}
		if (c.call.Ellipsis.IsValid() && ref.idx >= len(c.call.Args)-1) || ref.idx >= len(c.call.Args) {
			out.add(a.problem(c.call.Pos(), "call to %s spreads its arguments", ref.fn.Name()))
			continue
		}
		out.add(a.resolve(c.pkg, c.call.Args[ref.idx]))
	}
	return out
}

// funcSites lists the calls of fn: static calls, and for a method, the
// calls through every interface it implements.
func (a *analysis) funcSites(fn *types.Func) []callRef {
	sig := fn.Type().(*types.Signature)
	sites := append([]callRef(nil), a.calls[fn]...)
	if sig.Recv() != nil {
		for _, c := range a.iface[fn.Name()] {
			im := c.pkg.TypesInfo.Uses[ast.Unparen(c.call.Fun).(*ast.SelectorExpr).Sel].(*types.Func)
			it := im.Type().(*types.Signature).Recv().Type().Underlying().(*types.Interface)
			rt := sig.Recv().Type()
			if types.Implements(rt, it) || types.Implements(types.NewPointer(rt), it) {
				sites = append(sites, c)
			}
		}
	}
	return sites
}

// paramSites lists the calls that bind ref's parameters.
func (a *analysis) paramSites(ref paramRef) []callRef {
	if ref.fn != nil {
		return a.funcSites(ref.fn)
	}
	var out []callRef
	for _, o := range a.litVars[ref.lit] {
		out = append(out, a.varCalls[o]...)
	}
	return out
}

// resolveLitParam follows a function literal's parameter to the calls made
// through every variable or field the literal is stored in.
func (a *analysis) resolveLitParam(v *types.Var, ref paramRef, at token.Pos) flow {
	vars := a.litVars[ref.lit]
	if len(vars) == 0 {
		return a.problem(at, "parameter %s of a function literal that is passed on, not stored", v.Name())
	}
	var out flow
	for _, o := range vars {
		for _, c := range a.varCalls[o] {
			if (c.call.Ellipsis.IsValid() && ref.idx >= len(c.call.Args)-1) || ref.idx >= len(c.call.Args) {
				out.add(a.problem(c.call.Pos(), "call spreads its arguments"))
				continue
			}
			out.add(a.resolve(c.pkg, c.call.Args[ref.idx]))
		}
	}
	return out
}

func returns(d *ast.FuncDecl) []*ast.ReturnStmt {
	if d.Body == nil {
		return nil
	}
	return bodyReturns(d.Body)
}

// bodyReturns lists the return statements of a function body, not those of
// the function literals inside it.
func bodyReturns(body *ast.BlockStmt) []*ast.ReturnStmt {
	var out []*ast.ReturnStmt
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			out = append(out, x)
		}
		return true
	})
	return out
}

// resolveCall follows result idx of call to every return statement of its
// callee.
func (a *analysis) resolveCall(p *packages.Package, call *ast.CallExpr, idx int) flow {
	fn := typeutil.StaticCallee(p.TypesInfo, call)
	if fn == nil {
		return a.resolveDynamicCall(p, call, idx)
	}
	if fn.Pkg() != nil && fn.Pkg().Path() == "strings" && (fn.Name() == "TrimPrefix" || fn.Name() == "TrimSuffix") {
		affix := p.TypesInfo.Types[call.Args[1]]
		if affix.Value == nil {
			return a.problem(call.Pos(), "strings.%s with a non-constant affix", fn.Name())
		}
		in := a.resolve(p, call.Args[0])
		out := flow{problems: in.problems, conditionReason: in.conditionReason}
		cut := constant.StringVal(affix.Value)
		for _, v := range in.values {
			if fn.Name() == "TrimPrefix" {
				v.value = strings.TrimPrefix(v.value, cut)
			} else {
				v.value = strings.TrimSuffix(v.value, cut)
			}
			v.constant, v.spelled = "", ""
			out.values = append(out.values, v)
		}
		return out
	}
	if s, ok := a.trusted[fn.FullName()]; ok {
		var out flow
		for _, v := range s.values {
			out.values = append(out.values, origin{value: v, pos: a.pos(call.Pos()), trusted: s.name})
		}
		return out
	}
	d, ok := a.funcs[fn]
	if !ok {
		return a.problem(call.Pos(), "calls %s, which is outside the analysed code", fn.FullName())
	}
	type callKey struct {
		fn  *types.Func
		idx int
	}
	return a.memoized(callKey{fn, idx}, func() flow {
		var out flow
		sig := fn.Type().(*types.Signature)
		for _, r := range returns(d.decl) {
			switch {
			case len(r.Results) == 0:
				out.add(a.resolveVar(sig.Results().At(idx), r.Pos()))
			case len(r.Results) == sig.Results().Len():
				out.add(a.resolve(d.pkg, r.Results[idx]))
			case len(r.Results) == 1:
				inner, ok := ast.Unparen(r.Results[0]).(*ast.CallExpr)
				if !ok {
					out.add(a.problem(r.Pos(), "unexpected return shape"))
					continue
				}
				out.add(a.resolveCall(d.pkg, inner, idx))
			}
		}
		return out
	})
}

// resolveDynamicCall follows a call through a function literal: one called
// where it is written, or one stored in the variable or field called.
func (a *analysis) resolveDynamicCall(p *packages.Package, call *ast.CallExpr, idx int) flow {
	var lits []valueRef
	if lit, ok := ast.Unparen(call.Fun).(*ast.FuncLit); ok {
		lits = append(lits, valueRef{pkg: p, expr: lit})
	} else if v := a.lhsObj(p, call.Fun); v != nil {
		for _, w := range a.writes[v] {
			if _, ok := ast.Unparen(w.expr).(*ast.FuncLit); !ok {
				return a.problem(call.Pos(), "call through %s, which is assigned something other than a function literal", v.Name())
			}
			lits = append(lits, w)
		}
	}
	if len(lits) == 0 {
		return a.problem(call.Pos(), "dynamic call")
	}
	var out flow
	for _, w := range lits {
		lit := ast.Unparen(w.expr).(*ast.FuncLit)
		sig := w.pkg.TypesInfo.Types[lit].Type.(*types.Signature)
		for _, r := range bodyReturns(lit.Body) {
			switch {
			case len(r.Results) == sig.Results().Len():
				out.add(a.resolve(w.pkg, r.Results[idx]))
			case len(r.Results) == 0:
				out.add(a.resolveVar(sig.Results().At(idx), r.Pos()))
			default:
				out.add(a.problem(r.Pos(), "unexpected return shape"))
			}
		}
	}
	return out
}

// sink is one place a reason reaches a served surface.
type sink struct {
	kind string // "event", "conditionReason", "conditionType", "unstableReason"
	pos  token.Position
	flow flow
	// object is the API kind an Event is recorded on, when the recorder's
	// object argument has a static API type.
	object string
	// objects are the API kinds a recorder call records its Event on:
	// the object argument's static type, or, for a helper's parameter,
	// the static types its callers pass.
	objects []string
	// recorded is set for an Event recorder call (not an Event read back).
	recorded bool
	// pairs are the (reason, Event type) pairs a recorder call records.
	pairs []eventPair
	// pairProblems are the reasons pairs is incomplete.
	pairProblems []string
}

// eventPair is one reason an Event call records, with its Event type.
type eventPair struct {
	reason, eventType string
	pos               token.Position
}

const (
	sinkEvent           = "event"
	sinkConditionReason = "conditionReason"
	sinkConditionType   = "conditionType"
	sinkUnstableReason  = "unstableReason"
	// sinkMetricReason is the value of a metric's "reason" label.
	sinkMetricReason = "metricReason"
	// sinkStatusReasonPrefix + "<type>" is a typed status reason field that
	// is not a condition: its values are the api/v1alpha1 constants of
	// that type.
	sinkStatusReasonPrefix = "statusReason:"
)

// eventMethods are the recorder methods (client-go's EventRecorder and the
// operator's own Event sinks) whose "reason" parameter is an Event reason.
var eventMethods = map[string]bool{"Event": true, "Eventf": true, "AnnotatedEventf": true}

// unstableFuncs are the functions whose results are the deployment API's
// unstableReason.
var unstableFuncs = map[string]bool{
	modulePath + "/internal/controller.DeploymentUnstableReason":                      true,
	"(*" + modulePath + "/internal/controller.DeploymentLeaseManager).UnstableReason": true,
}

// fieldSinks are the struct fields whose value is served: a write to one is
// a sink, and so is a constant compared with a read of one.
var fieldSinks = map[string]string{
	metaV1Path + ".Condition.Reason": sinkConditionReason,
	metaV1Path + ".Condition.Type":   sinkConditionType,
	// An Event read back: the playground and the plugin match Events by
	// reason.
	"k8s.io/api/core/v1.Event.Reason": sinkEvent,
	// The reason of a deployment API lease refusal (423 and 409 bodies).
	modulePath + "/internal/controller.DeploymentLeaseError.Reason": sinkUnstableReason,
	// Typed status reasons that are not conditions (A-33). The planned
	// failover's rollback, result and hold reasons are not listed: the
	// engine passes them through pointers this analysis does not follow,
	// and their Go type already closes the set.
	apiPath + ".RestoreStatus.Reason":                  sinkStatusReasonPrefix + "PITRToken",
	apiPath + ".DragonflyUpgradeStatus.Reason":         sinkStatusReasonPrefix + "DragonflyUpgradeReason",
	apiPath + ".SiteStatus.SourceConvergenceReason":    sinkStatusReasonPrefix + "SourceConvergenceReason",
	apiPath + ".PlannedFailoverDragonflyStatus.Reason": sinkStatusReasonPrefix + "DragonflyPlannedReason",
	apiPath + ".PlannedFailoverAdmission.WaitReason":   sinkStatusReasonPrefix + "PlannedFailoverWaitReason",
	apiPath + ".PlannedFailoverAdmission.DeferReason":  sinkStatusReasonPrefix + "PlannedFailoverDeferReason",
	apiPath + ".PITRArchiveObservation.Reason":         sinkStatusReasonPrefix + "PITRArchiveRefusalReason",
	apiPath + ".PITRArchiveBinding.Reason":             sinkStatusReasonPrefix + "PITRArchiveBindingReason",
	apiPath + ".SiteEncryptionStatus.UnsealReason":     sinkStatusReasonPrefix + "KeyringUnsealReason",
}

// eventTypeIndex is the index of a recorder method's Event type parameter.
func eventTypeIndex(fn *types.Func) int {
	sig := fn.Type().(*types.Signature)
	for i := 0; i < sig.Params().Len(); i++ {
		if strings.EqualFold(sig.Params().At(i).Name(), "eventtype") {
			return i
		}
	}
	return -1
}

func paramIndex(fn *types.Func, name string) int {
	sig := fn.Type().(*types.Signature)
	for i := 0; i < sig.Params().Len(); i++ {
		if sig.Params().At(i).Name() == name {
			return i
		}
	}
	return -1
}

// namedPath returns "<package path>.<type name>" for t or the type it
// points to.
func namedPath(t types.Type) string {
	if t == nil {
		return ""
	}
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	if !ok || n.Obj().Pkg() == nil {
		return ""
	}
	return n.Obj().Pkg().Path() + "." + n.Obj().Name()
}

// fieldSinkOf returns the sink kind of a selector that reads or writes a
// served field, or "".
func fieldSinkOf(info *types.Info, e ast.Expr) string {
	sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	s := info.Selections[sel]
	if s == nil || s.Kind() != types.FieldVal {
		return ""
	}
	recv := s.Recv()
	// An embedded field's selection names the outer type; the field's own
	// struct is the last step of the path, which only matters for promoted
	// fields, and no sink field is promoted.
	return fieldSinks[namedPath(recv)+"."+sel.Sel.Name]
}

// sinks finds every sink in the loaded code and follows its value.
func (a *analysis) sinks() []sink {
	var out []sink
	add := func(kind string, p *packages.Package, e ast.Expr) {
		out = append(out, sink{kind: kind, pos: a.pos(e.Pos()), flow: a.resolve(p, e)})
	}
	// compared adds a sink for a value compared with a served field: a
	// constant, or whatever a variable compared with it can hold.
	compared := func(kind string, p *packages.Package, e ast.Expr) {
		if fieldSinkOf(p.TypesInfo, e) != "" {
			return
		}
		add(kind, p, e)
	}
	for _, p := range a.pkgs {
		info := p.TypesInfo
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					fn, _ := typeutil.Callee(info, x).(*types.Func)
					if fn == nil {
						return true
					}
					if eventMethods[fn.Name()] {
						if i := paramIndex(fn, "reason"); i >= 0 && i < len(x.Args) {
							add(sinkEvent, p, x.Args[i])
							s := &out[len(out)-1]
							s.recorded = true
							if len(x.Args) > 0 {
								s.object = apiKindOf(info.Types[x.Args[0]].Type)
								s.objects = a.objectKinds(p, x.Args[0], 0)
							}
							if j := eventTypeIndex(fn); j >= 0 && j < len(x.Args) {
								s.pairs, s.pairProblems = a.eventPairs(p, x.Args[i], x.Args[j], 0)
							} else {
								s.pairProblems = []string{fmt.Sprintf("%s: %s has no eventtype parameter", a.pos(x.Pos()), fn.Name())}
							}
						}
					}
					if idx, ok := a.metricReasonIndex(p, x); ok {
						if idx < 0 {
							out = append(out, sink{kind: sinkMetricReason, pos: a.pos(x.Pos()),
								flow: a.problem(x.Pos(), "labels of a metric with a reason label set by name; use WithLabelValues")})
						} else if idx < len(x.Args) {
							add(sinkMetricReason, p, x.Args[idx])
						}
					}
					if fn.Pkg() != nil && fn.Pkg().Path() == apiMetaPkg {
						if i := paramIndex(fn, "conditionType"); i >= 0 && i < len(x.Args) {
							add(sinkConditionType, p, x.Args[i])
						}
					}
				case *ast.CompositeLit:
					tp := namedPath(info.Types[x].Type)
					for _, elt := range x.Elts {
						kv, ok := elt.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						key, ok := kv.Key.(*ast.Ident)
						if !ok {
							continue
						}
						if kind := fieldSinks[tp+"."+key.Name]; kind != "" {
							add(kind, p, kv.Value)
						}
					}
				case *ast.AssignStmt:
					for i, l := range x.Lhs {
						if i >= len(x.Rhs) {
							continue
						}
						if kind := fieldSinkOf(info, l); kind != "" {
							add(kind, p, x.Rhs[i])
						}
					}
				case *ast.BinaryExpr:
					if x.Op != token.EQL && x.Op != token.NEQ {
						return true
					}
					if kind := fieldSinkOf(info, x.X); kind != "" {
						compared(kind, p, x.Y)
					}
					if kind := fieldSinkOf(info, x.Y); kind != "" {
						compared(kind, p, x.X)
					}
				case *ast.SwitchStmt:
					kind := ""
					if x.Tag != nil {
						kind = fieldSinkOf(info, x.Tag)
					}
					if kind == "" {
						return true
					}
					for _, c := range x.Body.List {
						for _, e := range c.(*ast.CaseClause).List {
							compared(kind, p, e)
						}
					}
				case *ast.FuncDecl:
					fn, _ := info.Defs[x.Name].(*types.Func)
					if fn == nil || !unstableFuncs[fn.FullName()] {
						return true
					}
					for _, r := range returns(x) {
						if len(r.Results) == 1 {
							add(sinkUnstableReason, p, r.Results[0])
						}
					}
				}
				return true
			})
		}
	}
	return out
}

// apiPackage returns the loaded api/v1alpha1 package.
func (a *analysis) apiPackage(t *testing.T) *packages.Package {
	t.Helper()
	var api *packages.Package
	packages.Visit(a.pkgs, nil, func(p *packages.Package) {
		if p.PkgPath == apiPath && p.TypesInfo != nil {
			api = p
		}
	})
	if api == nil {
		t.Fatalf("%s was not loaded", apiPath)
	}
	return api
}

// apiConsts returns the string constants api/v1alpha1 declares, by file
// base name.
func (a *analysis) apiConsts(t *testing.T) map[string]map[string]string {
	t.Helper()
	api := a.apiPackage(t)
	out := map[string]map[string]string{}
	for _, f := range api.Syntax {
		base := a.pos(f.Pos()).Filename
		base = base[strings.LastIndex(base, "/")+1:]
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, s := range gd.Specs {
				for _, n := range s.(*ast.ValueSpec).Names {
					c, ok := api.TypesInfo.Defs[n].(*types.Const)
					if !ok || c.Val().Kind() != constant.String {
						continue
					}
					if out[base] == nil {
						out[base] = map[string]string{}
					}
					out[base][c.Name()] = constant.StringVal(c.Val())
				}
			}
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// apiKindOf names the api/v1alpha1 type t is, or points to.
func apiKindOf(t types.Type) string {
	if p := namedPath(t); strings.HasPrefix(p, apiPath+".") {
		return strings.TrimPrefix(p, apiPath+".")
	}
	return ""
}

// eventPairs pairs the reasons an Event call can record with its Event
// types. When both take several values and both are parameters of the
// function the call is in, each call of that function is paired on its
// own (a helper that forwards both); several values on both sides that
// cannot be split that way are a problem, because the analysis could not
// tell which reason is recorded with which type.
func (a *analysis) eventPairs(p *packages.Package, reason, typ ast.Expr, depth int) ([]eventPair, []string) {
	rf, tf := a.resolve(p, reason), a.resolve(p, typ)
	problems := append([]string(nil), tf.problems...)
	if tf.conditionReason {
		problems = append(problems, fmt.Sprintf("%s: the Event type is a condition reason", a.pos(typ.Pos())))
	}
	var reasons []origin
	for _, o := range rf.values {
		if o.value != "" {
			reasons = append(reasons, o)
		}
	}
	typeSet := map[string]bool{}
	for _, o := range tf.values {
		typeSet[o.value] = true
	}
	reasonSet := map[string]bool{}
	for _, o := range reasons {
		reasonSet[o.value] = true
	}
	if len(typeSet) > 1 && len(reasonSet) > 1 {
		ri, ti, sites, ok := a.sharedParams(p, reason, typ)
		if !ok || depth > 16 {
			return nil, append(problems, fmt.Sprintf("%s: reasons %v and Event types %v cannot be paired",
				a.pos(reason.Pos()), sortedKeys(reasonSet), sortedKeys(typeSet)))
		}
		var out []eventPair
		for _, c := range sites {
			if c.call.Ellipsis.IsValid() || ri >= len(c.call.Args) || ti >= len(c.call.Args) {
				problems = append(problems, fmt.Sprintf("%s: call spreads its arguments", a.pos(c.call.Pos())))
				continue
			}
			pp, pr := a.eventPairs(c.pkg, c.call.Args[ri], c.call.Args[ti], depth+1)
			out = append(out, pp...)
			problems = append(problems, pr...)
		}
		return out, problems
	}
	var out []eventPair
	for _, r := range reasons {
		for t := range typeSet {
			out = append(out, eventPair{reason: r.value, eventType: t, pos: r.pos})
		}
	}
	return out, problems
}

// sharedParams reports whether reason and typ are both parameters of the
// same function, never assigned in its body, and returns their indexes and
// the calls that bind them.
func (a *analysis) sharedParams(p *packages.Package, reason, typ ast.Expr) (int, int, []callRef, bool) {
	param := func(e ast.Expr) (*types.Var, paramRef, bool) {
		id, ok := ast.Unparen(e).(*ast.Ident)
		if !ok {
			return nil, paramRef{}, false
		}
		v, ok := a.objOf(p, id).(*types.Var)
		if !ok {
			return nil, paramRef{}, false
		}
		ref, ok := a.params[v]
		return v, ref, ok && len(a.writes[v]) == 0
	}
	rv, rref, ok1 := param(reason)
	tv, tref, ok2 := param(typ)
	if !ok1 || !ok2 || rv == tv || rref.fn != tref.fn || rref.lit != tref.lit {
		return 0, 0, nil, false
	}
	if rref.fn != nil && len(a.escapes[rref.fn]) > 0 {
		return 0, 0, nil, false
	}
	return rref.idx, tref.idx, a.paramSites(rref), true
}

// metricVecPkg is the client_golang package whose *Vec constructors take
// the label names.
const metricVecPkg = "github.com/prometheus/client_golang/prometheus"

// metricReasonIndex reports whether call sets the labels of a metric
// vector declared with a "reason" label, and the index of that label in
// its WithLabelValues arguments (-1 for a call that sets labels by name).
func (a *analysis) metricReasonIndex(p *packages.Package, call *ast.CallExpr) (int, bool) {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return 0, false
	}
	var v *types.Var
	switch x := ast.Unparen(sel.X).(type) {
	case *ast.Ident:
		v, _ = a.objOf(p, x).(*types.Var)
	case *ast.SelectorExpr:
		v, _ = p.TypesInfo.Uses[x.Sel].(*types.Var)
	}
	if v == nil {
		return 0, false
	}
	idx, ok := a.metricReasonLabels()[v]
	if !ok {
		return 0, false
	}
	switch sel.Sel.Name {
	case "WithLabelValues", "GetMetricWithLabelValues", "DeleteLabelValues":
		return idx, true
	case "With", "GetMetricWith", "Delete", "CurryWith", "MustCurryWith", "DeletePartialMatch":
		return -1, true
	}
	return 0, false
}

// metricReasonLabels maps each package-level metric vector declared with a
// "reason" label to that label's index.
func (a *analysis) metricReasonLabels() map[*types.Var]int {
	if a.reasonMetrics != nil {
		return a.reasonMetrics
	}
	a.reasonMetrics = map[*types.Var]int{}
	for _, p := range a.pkgs {
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				gd, ok := d.(*ast.GenDecl)
				if !ok || gd.Tok != token.VAR {
					continue
				}
				for _, spec := range gd.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, n := range vs.Names {
						if i >= len(vs.Values) {
							continue
						}
						call, ok := ast.Unparen(vs.Values[i]).(*ast.CallExpr)
						if !ok || len(call.Args) == 0 {
							continue
						}
						fn, _ := typeutil.Callee(p.TypesInfo, call).(*types.Func)
						if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != metricVecPkg || !strings.HasSuffix(fn.Name(), "Vec") {
							continue
						}
						labels, ok := ast.Unparen(call.Args[len(call.Args)-1]).(*ast.CompositeLit)
						if !ok {
							continue
						}
						for j, l := range labels.Elts {
							if tv := p.TypesInfo.Types[l]; tv.Value != nil && constant.StringVal(tv.Value) == "reason" {
								a.reasonMetrics[p.TypesInfo.Defs[n].(*types.Var)] = j
							}
						}
					}
				}
			}
		}
	}
	return a.reasonMetrics
}

// objectKinds returns the API kinds e can be: its static type, or, for a
// parameter (of an interface type, say), the static types of the
// arguments its callers pass, followed transitively.
func (a *analysis) objectKinds(p *packages.Package, e ast.Expr, depth int) []string {
	if k := apiKindOf(p.TypesInfo.Types[e].Type); k != "" {
		return []string{k}
	}
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok || depth > 16 {
		return nil
	}
	v, ok := a.objOf(p, id).(*types.Var)
	if !ok {
		return nil
	}
	ref, ok := a.params[v]
	if !ok || len(a.writes[v]) > 0 {
		return nil
	}
	seen := map[string]bool{}
	for _, c := range a.paramSites(ref) {
		if ref.idx < len(c.call.Args) {
			for _, k := range a.objectKinds(c.pkg, c.call.Args[ref.idx], depth+1) {
				seen[k] = true
			}
		}
	}
	return sortedKeys(seen)
}
