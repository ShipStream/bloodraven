package scenarios

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// controllerStringConsts reads the untyped string constants declared in the
// controller package's non-test files. The playground cannot import the
// operator's unexported contract constants, so it reads their declarations.
func controllerStringConsts(t *testing.T) map[string]string {
	t.Helper()
	dir := filepath.Join("..", "..", "controller")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					v, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatalf("%s: unquote %s: %v", f, name.Name, err)
					}
					out[name.Name] = v
				}
			}
		}
	}
	return out
}

// TestPITRPlaygroundContractMirrors pins U65: every playground mirror of an
// operator #203 contract string that api/v1alpha1 does not export equals the
// operator's declaration, so a
// renamed label, annotation or selector fails here instead of silently
// matching nothing in s69's assertions (for example "no check Job ran").
func TestPITRPlaygroundContractMirrors(t *testing.T) {
	c := controllerStringConsts(t)
	get := func(name string) string {
		v, ok := c[name]
		if !ok {
			t.Fatalf("internal/controller declares no string constant %s; update the mirror and this test", name)
		}
		return v
	}
	cases := map[string]struct{ mirror, want string }{
		"app label":                {pitrSnapshotAppLabel, get("labelAppName")},
		"archive key snapshot app": {pitrKeySnapshotApp, get("appNamePITRKeySnapshot")},
		"dump key snapshot app":    {pitrDumpKeySnapshotApp, get("appNameDumpKeySnapshot")},
		"check Job selector":       {pitrCheckJobSelector, get("labelAppName") + "=" + get("appNameBackupJob") + "," + v1alpha1.ResourceLabel + "=" + get("resourcePITRArchiveCheck")},
		"snapshot passphrase key":  {pitrSnapshotPassphraseKey, get("pitrKeySnapshotDataKey")},
	}
	for name, tc := range cases {
		if tc.mirror != tc.want {
			t.Errorf("%s: playground mirror %q, operator %q", name, tc.mirror, tc.want)
		}
	}
}
