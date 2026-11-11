package pitrplan

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// The bound on a spill's unwritten pages (store.go) holds only if every
// write to memory that may be in the arena is counted, which means it goes
// through arr.set, fill, copyIn, sortFunc or vec.push. That was a
// convention; TestSpilledWritesAreAllCounted observes it at run time, but
// only for the pages a test run happens to dirty (a mutation that wrote
// Tarjan's low array directly slipped past it: the page was already
// counted). spillWriteCheck enforces it on the source instead (review R3
// finding 5): it type-checks the package and reports every place outside
// the accessors that writes, or could write, memory that may be spilled.
//
// What may be spilled: the s field of an arr, what vec.items returns, a
// FileIndex's Txns, a dirtyState's mem, what view and the Arena return,
// every slice of those, and every variable, parameter, field or function
// result they are assigned to, passed as or returned from. What counts as a
// write: an assignment or ++/-- to an element (or a field of one), taking
// an element's address, a method with a pointer receiver called on one,
// copy, append or clear with such a slice as the destination, handing one
// to a function outside the checked packages that is not known to only
// read it, and giving an arr the memory of another one. The accessors are
// the methods of arr and vec and the functions alloc and view, all in
// store.go; a method of arr or vec declared anywhere else is reported too.

const spillPkgPath = "github.com/shipstream/bloodraven/internal/pitrplan"

// spillReadOnly are the functions outside the checked packages that may be
// handed spilled memory: they do not write through it.
var spillReadOnly = map[string]bool{
	"unsafe.SliceData":        true,
	"unsafe.Slice":            true,
	"unsafe.Pointer":          true,
	"sort.Search":             true,
	"sort.SearchInts":         true,
	"slices.Index":            true,
	"slices.Contains":         true,
	"slices.Equal":            true,
	"slices.BinarySearchFunc": true,
}

type spillWriteCheck struct {
	fset *token.FileSet
	pkgs []*packages.Package
	// tainted are the variables, parameters, fields and functions (by their
	// first result) that may hold spilled memory, beside the roots.
	tainted map[types.Object]bool
	changed bool
	out     []string
}

// loadSpillPackages type-checks the planner and the index pass (the one
// other package that fills a FileIndex), with overlay replacing or adding
// source files.
func loadSpillPackages(t *testing.T, overlay map[string][]byte) *spillWriteCheck {
	t.Helper()
	cfg := &packages.Config{
		Mode:    packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Overlay: overlay,
	}
	pkgs, err := packages.Load(cfg, spillPkgPath, "github.com/shipstream/bloodraven/internal/sidecar")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(pkgs) != 2 {
		t.Fatalf("loaded %d packages", len(pkgs))
	}
	c := &spillWriteCheck{pkgs: pkgs, tainted: map[types.Object]bool{}}
	for _, p := range pkgs {
		for _, e := range p.Errors {
			t.Fatalf("%s does not type-check: %v", p.PkgPath, e)
		}
		c.fset = p.Fset
	}
	return c
}

func origin(obj types.Object) types.Object {
	switch o := obj.(type) {
	case *types.Var:
		return o.Origin()
	case *types.Func:
		return o.Origin()
	}
	return obj
}

// named returns the name of t's named type when it is declared in the
// planner package (through pointers and instantiations), or "".
func spillNamed(t types.Type) string {
	for {
		t = types.Unalias(t)
		p, ok := t.(*types.Pointer)
		if !ok {
			break
		}
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	if !ok || n.Obj().Pkg() == nil || n.Obj().Pkg().Path() != spillPkgPath {
		return ""
	}
	return n.Origin().Obj().Name()
}

// field resolves a selector to a struct field: its owner's name and its own.
func (c *spillWriteCheck) field(p *packages.Package, sel *ast.SelectorExpr) (owner, name string, obj types.Object) {
	s := p.TypesInfo.Selections[sel]
	if s == nil || s.Kind() != types.FieldVal {
		return "", "", nil
	}
	// The field's own struct: the receiver, unless the field is promoted.
	recv := s.Recv()
	if idx := s.Index(); len(idx) > 1 {
		t := recv
		for _, i := range idx[:len(idx)-1] {
			for {
				t = types.Unalias(t)
				ptr, ok := t.(*types.Pointer)
				if !ok {
					break
				}
				t = ptr.Elem()
			}
			t = t.Underlying().(*types.Struct).Field(i).Type()
		}
		recv = t
	}
	return spillNamed(recv), s.Obj().Name(), origin(s.Obj())
}

// callee resolves a call's function: its object (nil for a builtin, a
// conversion or a function value), and a name for the allow-list.
func (c *spillWriteCheck) callee(p *packages.Package, call *ast.CallExpr) (obj types.Object, name string) {
	fun := ast.Unparen(call.Fun)
	// Explicit instantiation: view[T](…).
	switch f := fun.(type) {
	case *ast.IndexExpr:
		fun = f.X
	case *ast.IndexListExpr:
		fun = f.X
	}
	switch f := fun.(type) {
	case *ast.Ident:
		obj = p.TypesInfo.Uses[f]
	case *ast.SelectorExpr:
		if s := p.TypesInfo.Selections[f]; s != nil {
			obj = s.Obj()
		} else {
			obj = p.TypesInfo.Uses[f.Sel]
		}
	}
	switch o := obj.(type) {
	case *types.Builtin:
		return nil, o.Name()
	case *types.Func:
		name = o.Name()
		if o.Pkg() != nil {
			name = o.Pkg().Path() + "." + o.Name()
		}
		return origin(o), name
	}
	return nil, ""
}

func (c *spillWriteCheck) checked(obj types.Object) bool {
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	for _, p := range c.pkgs {
		if obj.Pkg().Path() == p.PkgPath {
			return true
		}
	}
	return false
}

// spilled reports whether e may be memory of the arena.
func (c *spillWriteCheck) spilled(p *packages.Package, e ast.Expr) bool {
	switch e := ast.Unparen(e).(type) {
	case *ast.Ident:
		if obj := p.TypesInfo.Uses[e]; obj != nil {
			return c.tainted[origin(obj)]
		}
		if obj := p.TypesInfo.Defs[e]; obj != nil {
			return c.tainted[origin(obj)]
		}
	case *ast.SliceExpr:
		return c.spilled(p, e.X)
	case *ast.SelectorExpr:
		owner, name, obj := c.field(p, e)
		switch {
		case owner == "arr" && name == "s", owner == "FileIndex" && name == "Txns", owner == "dirtyState" && name == "mem":
			return true
		case obj != nil:
			return c.tainted[obj]
		}
	case *ast.CallExpr:
		obj, name := c.callee(p, e)
		if f, ok := obj.(*types.Func); ok {
			if recv := f.Type().(*types.Signature).Recv(); recv != nil {
				switch owner := spillNamed(recv.Type()); {
				case owner == "vec" && f.Name() == "items":
					return true
				case owner == "Arena" && (f.Name() == "Alloc" || f.Name() == "Resize"):
					return true
				}
			}
			if name == spillPkgPath+".view" {
				return true
			}
			return c.tainted[obj]
		}
	}
	return false
}

func (c *spillWriteCheck) taint(obj types.Object) {
	if obj == nil {
		return
	}
	if obj = origin(obj); !c.tainted[obj] {
		c.tainted[obj] = true
		c.changed = true
	}
}

// taintTarget marks what an assignment's left side names.
func (c *spillWriteCheck) taintTarget(p *packages.Package, lhs ast.Expr) {
	switch l := ast.Unparen(lhs).(type) {
	case *ast.Ident:
		if obj := p.TypesInfo.Defs[l]; obj != nil {
			c.taint(obj)
		} else {
			c.taint(p.TypesInfo.Uses[l])
		}
	case *ast.SelectorExpr:
		if _, _, obj := c.field(p, l); obj != nil {
			c.taint(obj)
		}
	}
}

// propagate follows spilled memory through one file: assignments,
// composite literals, arguments of checked functions and returns.
func (c *spillWriteCheck) propagate(p *packages.Package, file *ast.File) {
	var funcs []types.Object // the enclosing declared functions
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncDecl:
			funcs = append(funcs[:0], p.TypesInfo.Defs[n.Name])
		case *ast.AssignStmt:
			if len(n.Lhs) == len(n.Rhs) {
				for i, rhs := range n.Rhs {
					if c.spilled(p, rhs) {
						c.taintTarget(p, n.Lhs[i])
					}
				}
			} else if len(n.Rhs) == 1 && c.spilled(p, n.Rhs[0]) {
				// mem, err := arena.Alloc(n)
				c.taintTarget(p, n.Lhs[0])
			}
		case *ast.ValueSpec:
			for i, v := range n.Values {
				if i < len(n.Names) && c.spilled(p, v) {
					c.taint(p.TypesInfo.Defs[n.Names[i]])
				}
			}
		case *ast.CompositeLit:
			st, ok := types.Unalias(p.TypesInfo.TypeOf(n)).Underlying().(*types.Struct)
			if !ok {
				break
			}
			for i, el := range n.Elts {
				if kv, ok := el.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok && c.spilled(p, kv.Value) {
						c.taint(p.TypesInfo.Uses[key])
					}
				} else if i < st.NumFields() && c.spilled(p, el) {
					c.taint(st.Field(i))
				}
			}
		case *ast.CallExpr:
			obj, _ := c.callee(p, n)
			f, ok := obj.(*types.Func)
			if !ok || !c.checked(f) {
				break
			}
			params := f.Type().(*types.Signature).Params()
			for i, arg := range n.Args {
				if params.Len() > 0 && c.spilled(p, arg) {
					c.taint(params.At(min(i, params.Len()-1)))
				}
			}
		case *ast.ReturnStmt:
			if len(n.Results) > 0 && len(funcs) > 0 && c.spilled(p, n.Results[0]) {
				c.taint(funcs[0])
			}
		}
		return true
	})
}

// element reports whether e is an element of spilled memory, or a field of
// one: something whose assignment writes the arena.
func (c *spillWriteCheck) element(p *packages.Package, e ast.Expr) bool {
	for {
		switch x := ast.Unparen(e).(type) {
		case *ast.IndexExpr:
			if c.spilled(p, x.X) {
				return true
			}
			e = x.X
		case *ast.SelectorExpr:
			if s := p.TypesInfo.Selections[x]; s == nil || s.Kind() != types.FieldVal {
				return false
			}
			e = x.X
		case *ast.StarExpr:
			e = x.X
		default:
			return false
		}
	}
}

func (c *spillWriteCheck) report(pos token.Pos, format string, args ...any) {
	at := c.fset.Position(pos)
	c.out = append(c.out, fmt.Sprintf("%s:%d: %s", filepath.Base(at.Filename), at.Line, fmt.Sprintf(format, args...)))
}

// accessor reports whether fn is one of the functions that may write
// spilled memory, and reports a method of arr or vec outside store.go.
func (c *spillWriteCheck) accessor(p *packages.Package, fn *ast.FuncDecl) bool {
	if p.PkgPath != spillPkgPath {
		return false
	}
	inStore := filepath.Base(c.fset.Position(fn.Pos()).Filename) == "store.go"
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return inStore && (fn.Name.Name == "alloc" || fn.Name.Name == "view")
	}
	owner := spillNamed(p.TypesInfo.TypeOf(fn.Recv.List[0].Type))
	if owner != "arr" && owner != "vec" {
		return false
	}
	if !inStore {
		c.report(fn.Pos(), "%s.%s is declared outside store.go: the methods of arr and vec are the audited accessors of spilled memory", owner, fn.Name.Name)
		return false
	}
	return true
}

// violations lists every write to spilled memory outside the accessors.
func (c *spillWriteCheck) violations() []string {
	for c.changed = true; c.changed; {
		c.changed = false
		for _, p := range c.pkgs {
			for _, f := range p.Syntax {
				c.propagate(p, f)
			}
		}
	}
	for _, p := range c.pkgs {
		for _, f := range p.Syntax {
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if ok && c.accessor(p, fn) {
					continue
				}
				ast.Inspect(decl, func(n ast.Node) bool { c.inspect(p, n); return true })
			}
		}
	}
	sort.Strings(c.out)
	return c.out
}

func (c *spillWriteCheck) inspect(p *packages.Package, n ast.Node) {
	switch n := n.(type) {
	case *ast.AssignStmt:
		for i, lhs := range n.Lhs {
			if c.element(p, lhs) {
				c.report(lhs.Pos(), "assignment to an element of a spilled array; use set")
			}
			// An arr that takes another's memory loses its page accounting.
			if sel, ok := ast.Unparen(lhs).(*ast.SelectorExpr); ok && len(n.Lhs) == len(n.Rhs) {
				if owner, name, _ := c.field(p, sel); owner == "arr" && name == "s" && c.spilled(p, n.Rhs[i]) {
					c.report(lhs.Pos(), "an arr is given spilled memory; allocate it with alloc or resize")
				}
			}
		}
	case *ast.IncDecStmt:
		if c.element(p, n.X) {
			c.report(n.X.Pos(), "%s of an element of a spilled array; use set", n.Tok)
		}
	case *ast.RangeStmt:
		for _, e := range []ast.Expr{n.Key, n.Value} {
			if e != nil && c.element(p, e) {
				c.report(e.Pos(), "a range assigns to an element of a spilled array; use set")
			}
		}
	case *ast.UnaryExpr:
		if n.Op == token.AND && c.element(p, n.X) {
			c.report(n.Pos(), "the address of an element of a spilled array is taken; writes through it are not counted")
		}
	case *ast.CompositeLit:
		if spillNamed(p.TypesInfo.TypeOf(n)) != "arr" {
			break
		}
		for i, el := range n.Elts {
			value := el
			if kv, ok := el.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); !ok || key.Name != "s" {
					continue
				}
				value = kv.Value
			} else if i != 0 {
				continue
			}
			if c.spilled(p, value) {
				c.report(el.Pos(), "an arr is given spilled memory; allocate it with alloc or resize")
			}
		}
	case *ast.CallExpr:
		obj, name := c.callee(p, n)
		if obj == nil {
			// A builtin (or a conversion, or a function value).
			if (name == "copy" || name == "append" || name == "clear") && len(n.Args) > 0 && c.spilled(p, n.Args[0]) {
				c.report(n.Pos(), "%s into a spilled array; use copyIn, push or fill", name)
			}
			return
		}
		f := obj.(*types.Func)
		sig := f.Type().(*types.Signature)
		// A method with a pointer receiver on an element.
		if sel, ok := ast.Unparen(n.Fun).(*ast.SelectorExpr); ok && sig.Recv() != nil {
			if _, ptr := types.Unalias(sig.Recv().Type()).(*types.Pointer); ptr && c.element(p, sel.X) {
				c.report(n.Pos(), "%s has a pointer receiver and is called on an element of a spilled array", f.Name())
			}
		}
		if c.checked(f) || spillReadOnly[name] {
			return
		}
		if recv := sig.Recv(); recv != nil && spillNamed(recv.Type()) == "Arena" {
			return // the arena's own memory
		}
		for _, arg := range n.Args {
			if c.spilled(p, arg) {
				c.report(arg.Pos(), "a spilled array is passed to %s, which is not known to only read it", name)
			}
		}
	}
}

// Every write to memory that may be spilled goes through the accessors.
func TestSpilledArraysAreWrittenThroughAccessors(t *testing.T) {
	c := loadSpillPackages(t, nil)
	for _, v := range c.violations() {
		t.Error(v)
	}
	// The check sees the package's spilled arrays at all.
	if len(c.tainted) < 10 {
		t.Fatalf("the check follows only %d variables, fields and functions that hold spilled memory", len(c.tainted))
	}
}

// The check catches the bypasses it exists for: the one a mutation of the
// original change slipped past the run-time test (a direct write to
// Tarjan's low array), and every other shape of an uncounted write.
func TestSpillWriteCheckCatchesBypasses(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// 1. Tarjan's low, written directly instead of through set.
	const counted, direct = "low.set(int(v), min(low.s[v], index.s[w]))", "low.s[v] = min(low.s[v], index.s[w])"
	identity := filepath.Join(dir, "identity.go")
	src, err := os.ReadFile(identity)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(src), counted) != 1 {
		t.Fatalf("identity.go no longer holds %q once; point this test at Tarjan's low again", counted)
	}
	line := 1 + strings.Count(string(src[:strings.Index(string(src), counted)]), "\n")
	got := loadSpillPackages(t, map[string][]byte{identity: []byte(strings.Replace(string(src), counted, direct, 1))}).violations()
	want := fmt.Sprintf("identity.go:%d: assignment to an element of a spilled array; use set", line)
	if len(got) != 1 || got[0] != want {
		t.Fatalf("a direct write to Tarjan's low:\n got %q\nwant %q", got, want)
	}

	// 2. Every shape, in a file added to the package. Each line marked BAD
	// must be reported, and no other.
	const bypass = `package pitrplan

import "slices"

type bypassRec struct{ a, b int32 }

func (r *bypassRec) bump() { r.a++ }

type bypassHolder struct{ recs []bypassRec }

func bypassFill(dst []bypassRec) {
	dst[0] = bypassRec{} // BAD: a parameter that receives spilled memory
}

func bypassShapes(a *arr[bypassRec], v *vec[bypassRec], x *FileIndex, d *dirtyState) int32 {
	a.s[0] = bypassRec{}        // BAD
	a.s[1].a = 1                // BAD
	a.s[2].b++                  // BAD
	a.s[3].a += 2               // BAD
	(*a).s[4] = bypassRec{}     // BAD
	v.items()[0] = bypassRec{}  // BAD
	v.a.s[1] = bypassRec{}      // BAD
	x.Txns[0].Flags = TxnEmpty  // BAD
	d.mem[0] = 1                // BAD
	alias := a.s
	alias[5] = bypassRec{}      // BAD
	part := alias[2:4]
	part[0].a = 3               // BAD
	var late []bypassRec
	late = v.items()
	late[1] = bypassRec{}       // BAD
	h := bypassHolder{recs: a.s}
	h.recs[0] = bypassRec{}     // BAD
	p := &a.s[6]                // BAD
	p.a = 4
	a.s[7].bump()               // BAD
	copy(a.s, late)             // BAD
	copy(a.s[1:], late)         // BAD
	_ = append(a.s[:1], late...) // BAD
	clear(a.s)                  // BAD
	slices.Reverse(a.s)         // BAD
	slices.SortFunc(alias, func(x, y bypassRec) int { return int(x.a - y.a) }) // BAD
	for i := range a.s {
		a.s[i].a = int32(i)     // BAD
	}
	for a.s[0].a = range 3 {    // BAD
	}
	func() { a.s[8] = bypassRec{} }() // BAD
	bypassFill(a.s)
	other := arr[bypassRec]{s: a.s, b: a.b} // BAD
	other.s = v.items()         // BAD

	// Reads, copies of records, rebinding and the accessors are fine.
	rec := a.s[0]
	rec.a = 9
	sum := rec.a + a.s[1].b + v.items()[0].a + int32(len(alias)) + int32(x.Txns[0].Flags) + int32(d.mem[0])
	for _, r := range a.s {
		r.a++
		sum += r.a
	}
	heap := append([]bypassRec(nil), a.s...)
	heap[0] = bypassRec{}
	copy(heap, a.s)
	alias = heap
	a.set(0, rec)
	a.fill(0, 1, rec)
	a.copyIn(0, heap)
	v.set(0, rec)
	_ = slices.Index(a.s, rec)
	return sum + other.s[0].a
}
`
	added := filepath.Join(dir, "zz_bypass.go")
	got = loadSpillPackages(t, map[string][]byte{added: []byte(bypass)}).violations()
	reported := map[int]string{}
	for _, v := range got {
		var n int
		if _, err := fmt.Sscanf(v, "zz_bypass.go:%d:", &n); err != nil {
			t.Errorf("a violation outside the added file: %s", v)
			continue
		}
		reported[n] = v
	}
	bad := 0
	for i, text := range strings.Split(bypass, "\n") {
		n := i + 1
		marked := strings.Contains(text, "// BAD")
		if marked {
			bad++
		}
		if _, ok := reported[n]; ok != marked {
			t.Errorf("zz_bypass.go:%d reported %v, marked BAD %v: %s (%s)", n, ok, marked, strings.TrimSpace(text), reported[n])
		}
	}
	if bad < 25 {
		t.Fatalf("%d bypass shapes", bad)
	}
}
