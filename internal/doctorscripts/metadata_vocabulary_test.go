package doctorscripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/api/v1alpha1"
)

// oldPrefixJSONPath matches a kubectl JSONPath that reads a label or
// annotation under the bare shipstream.io/ prefix, which no key the
// operator writes uses any more (every key is under bloodraven.shipstream.io/).
var oldPrefixJSONPath = regexp.MustCompile(`\.(annotations|labels)\.shipstream\\\.io/`)

// jsonPathKey is how the doctor scripts spell a metadata key inside a
// kubectl custom-columns JSONPath: dots escaped.
func jsonPathKey(kind, key string) string {
	return "." + kind + "." + strings.ReplaceAll(key, ".", `\.`)
}

func readScript(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(scriptPath(name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestDoctorScriptsReadKeysAtBloodravenPrefix pins the custom-columns paths
// the triage and support-bundle scripts read the backup retry chain and the
// pod-to-backup mapping from. A path at the old prefix prints <none> on every
// cluster without any error, so only a test notices it.
func TestDoctorScriptsReadKeysAtBloodravenPrefix(t *testing.T) {
	scripts, err := filepath.Glob(scriptPath("*.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if len(scripts) == 0 {
		t.Fatal("no doctor scripts found")
	}
	for _, path := range scripts {
		name := filepath.Base(path)
		for i, line := range strings.Split(readScript(t, name), "\n") {
			if m := oldPrefixJSONPath.FindString(line); m != "" {
				t.Errorf("%s:%d reads a key at the old shipstream.io/ prefix (%q); the operator writes it under bloodraven.shipstream.io/", name, i+1, m)
			}
		}
	}

	want := []string{
		jsonPathKey("annotations", v1alpha1.BackupAttemptAnnotation),
		jsonPathKey("labels", v1alpha1.BackupRetryOfLabel),
		jsonPathKey("annotations", v1alpha1.BackupRetryRootAnnotation),
		jsonPathKey("annotations", v1alpha1.BackupRetryRootUIDAnnotation),
		jsonPathKey("labels", v1alpha1.MysqlBackupLabel),
		jsonPathKey("annotations", v1alpha1.VerificationBackupAnnotation),
	}
	for _, name := range []string{"triage.sh", "support-bundle.sh"} {
		body := readScript(t, name)
		for _, p := range want {
			if !strings.Contains(body, p) {
				t.Errorf("%s does not read %s", name, p)
			}
		}
	}
}

// pitrEventFilter is the grep -E alternation triage.sh filters the PITR
// warnings section with.
var pitrEventFilter = regexp.MustCompile(`(?m)^PITR_EVENTS=\$\(kubectl get events[^\n]*\n[^\n]*\n\s*\| grep -E '([^']+)'`)

// expandAlternation expands a pattern made of literals and non-nested
// groups, such as `A(b|c)|D`, into every name it matches.
func expandAlternation(t *testing.T, pattern string) []string {
	t.Helper()
	var top []string
	depth, start := 0, 0
	for i, c := range pattern {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case '|':
			if depth == 0 {
				top = append(top, pattern[start:i])
				start = i + 1
			}
		}
	}
	top = append(top, pattern[start:])

	var names []string
	for _, branch := range top {
		parts := []string{""}
		for branch != "" {
			open := strings.IndexByte(branch, '(')
			if open < 0 {
				for i := range parts {
					parts[i] += branch
				}
				break
			}
			closing := strings.IndexByte(branch[open:], ')')
			if closing < 0 || strings.ContainsAny(branch[open+1:open+closing], "()") {
				t.Fatalf("unsupported pattern branch %q", branch)
			}
			prefix, alts := branch[:open], strings.Split(branch[open+1:open+closing], "|")
			var next []string
			for _, p := range parts {
				for _, a := range alts {
					next = append(next, p+prefix+a)
				}
			}
			parts, branch = next, branch[open+closing+1:]
		}
		names = append(names, parts...)
	}
	for _, n := range names {
		if !regexp.MustCompile(`^[A-Z][A-Za-z0-9]+$`).MatchString(n) {
			t.Fatalf("pattern %q expands to %q, which is not a plain Event reason; extend the expander or simplify the pattern", pattern, n)
		}
	}
	return names
}

// apiEventReasons returns the values of every Event* string constant in
// api/v1alpha1, resolving constants defined as another constant.
func apiEventReasons(t *testing.T) map[string]bool {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "..", "api", "v1alpha1")
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	aliases := map[string]string{}
	fset := token.NewFileSet()
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					switch v := vs.Values[i].(type) {
					case *ast.BasicLit:
						if v.Kind == token.STRING {
							s, err := strconv.Unquote(v.Value)
							if err != nil {
								t.Fatal(err)
							}
							values[n.Name] = s
						}
					case *ast.Ident:
						aliases[n.Name] = v.Name
					}
				}
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for name, target := range aliases {
			if v, ok := values[target]; ok {
				values[name] = v
				delete(aliases, name)
				changed = true
			}
		}
	}
	reasons := map[string]bool{}
	for name, v := range values {
		if strings.HasPrefix(name, "Event") {
			reasons[v] = true
		}
	}
	if len(reasons) < 50 {
		t.Fatalf("found only %d Event reason constants in %s; the scan is broken", len(reasons), dir)
	}
	return reasons
}

// TestTriagePITREventFilterNamesRegisteredReasons keeps the triage PITR
// warnings filter on Event reasons the operator emits: a renamed reason
// left in the filter drops its Warning from the section silently.
func TestTriagePITREventFilterNamesRegisteredReasons(t *testing.T) {
	m := pitrEventFilter.FindStringSubmatch(readScript(t, "triage.sh"))
	if m == nil {
		t.Fatal("triage.sh: PITR_EVENTS grep -E filter not found; update the test with the script")
	}
	names := expandAlternation(t, m[1])
	if len(names) < 10 {
		t.Fatalf("filter %q expands to only %d names", m[1], len(names))
	}
	reasons := apiEventReasons(t)
	for _, n := range names {
		if !reasons[n] {
			t.Errorf("triage.sh PITR filter names %q, which is not an Event reason in api/v1alpha1", n)
		}
	}
}
