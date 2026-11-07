package controller

import (
	"regexp"
	"sort"
	"strings"
	"testing"
)

// celInput is one schema node a CEL rule reads: the node a field reference
// reaches, and whether the rule only iterates or measures it (a list or map
// receiver of a macro, size() or `in`), so that the container bound is what
// the cost estimator needs.
type celInput struct {
	path      string
	schema    apiCRDSchema
	container bool
}

var (
	celStringLiteral = regexp.MustCompile(`'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"`)
	celChain         = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*`)
	celMacro         = regexp.MustCompile(`\.(all|exists|exists_one|filter|map)\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,`)
)

// celMatchingParen returns the index of the ')' closing the '(' at open.
func celMatchingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return len(s)
}

// celNormalize blanks string literals (they hold regexes and paths, not
// field references), reads oldSelf.value() as oldSelf, and replaces every
// has(...) with true: has() tests presence and costs nothing per byte.
func celNormalize(rule string) string {
	rule = celStringLiteral.ReplaceAllString(rule, "''")
	rule = strings.ReplaceAll(rule, "oldSelf.value()", "oldSelf")
	for {
		i := -1
		for j := strings.Index(rule, "has("); j >= 0; {
			if j == 0 || !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.", rune(rule[j-1])) {
				i = j
				break
			}
			k := strings.Index(rule[j+1:], "has(")
			if k < 0 {
				break
			}
			j += 1 + k
		}
		if i < 0 {
			return rule
		}
		end := celMatchingParen(rule, i+3)
		rule = rule[:i] + "true" + rule[min(end+1, len(rule)):]
	}
}

type celBinding struct {
	name       string
	varAt      int
	start, end int
	schema     apiCRDSchema
	path       string
	mapKey     bool
	resolved   bool
}

// celRuleInputs resolves every field reference in rule, evaluated at node
// (path nodePath), to the schema node it reaches.
func celRuleInputs(t *testing.T, nodePath string, node apiCRDSchema, rule string) []celInput {
	t.Helper()
	r := celNormalize(rule)
	var bindings []celBinding

	resolveHead := func(head string, at int) (apiCRDSchema, string, bool, bool) {
		if head == "self" || head == "oldSelf" {
			return node, nodePath, false, true
		}
		var best *celBinding
		for i := range bindings {
			b := &bindings[i]
			if b.name == head && b.start <= at && at <= b.end && (best == nil || b.start > best.start) {
				best = b
			}
		}
		if best == nil {
			return apiCRDSchema{}, "", false, false
		}
		if !best.resolved {
			t.Errorf("%s: rule %q: cannot resolve the receiver of %s", nodePath, rule, head)
			return apiCRDSchema{}, "", false, false
		}
		return best.schema, best.path, best.mapKey, true
	}
	// walk resolves chain (at offset at) as far as properties go and returns
	// the node reached and the unresolved remainder.
	walk := func(chain string, at int) (apiCRDSchema, string, []string, bool) {
		segs := strings.Split(chain, ".")
		s, path, mapKey, ok := resolveHead(segs[0], at)
		if !ok || mapKey {
			return apiCRDSchema{}, "", nil, false
		}
		rest := segs[1:]
		for len(rest) > 0 {
			next, ok := s.Properties[rest[0]]
			if !ok {
				break
			}
			s, path, rest = next, path+"."+rest[0], rest[1:]
		}
		return s, path, rest, true
	}

	// Macro bindings, outermost first so a receiver can use an enclosing
	// binding.
	for _, m := range celMacro.FindAllStringSubmatchIndex(r, -1) {
		dot := m[0]
		begin := dot
		for begin > 0 && strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.", rune(r[begin-1])) {
			begin--
		}
		b := celBinding{name: r[m[4]:m[5]], varAt: m[4], start: dot, end: celMatchingParen(r, m[3])}
		if s, path, rest, ok := walk(r[begin:dot], begin); ok && len(rest) == 0 {
			switch {
			case s.Items != nil:
				b.schema, b.path, b.resolved = *s.Items, path+"[]", true
			case s.AdditionalProperties != nil:
				b.mapKey, b.resolved = true, true
			}
		}
		bindings = append(bindings, b)
	}

	declared := map[int]bool{}
	for _, b := range bindings {
		declared[b.varAt] = true
	}
	var out []celInput
	for _, loc := range celChain.FindAllStringIndex(r, -1) {
		if loc[0] > 0 && r[loc[0]-1] == '.' || declared[loc[0]] {
			continue
		}
		chain := r[loc[0]:loc[1]]
		head, _, _ := strings.Cut(chain, ".")
		if _, _, mapKey, ok := resolveHead(head, loc[0]); !ok || mapKey {
			continue
		}
		s, path, rest, ok := walk(chain, loc[0])
		if !ok {
			continue
		}
		after := strings.TrimLeft(r[loc[1]:], " ")
		in := celInput{path: path, schema: s}
		switch {
		case len(rest) > 0:
			// A method or macro on the node: iterating or measuring a
			// list or map needs the container bound; a string method
			// reads the whole string; on an object (metadata, an
			// optional) nothing is traversed.
			if s.Type == "object" && s.AdditionalProperties == nil {
				continue
			}
			in.container = true
		case strings.HasPrefix(after, "["):
			if s.AdditionalProperties != nil {
				in = celInput{path: path + "{}", schema: *s.AdditionalProperties}
			} else if s.Items != nil {
				in = celInput{path: path + "[]", schema: *s.Items}
			}
		}
		out = append(out, in)
	}
	return out
}

// celInputUnbounded lists what is unbounded about one input.
func celInputUnbounded(in celInput) []string {
	s := in.schema
	var out []string
	switch {
	case s.IntOrString:
		if s.MaxLength == nil {
			out = append(out, in.path+" (int-or-string)")
		}
	case s.Type == "string":
		if s.MaxLength == nil && len(s.Enum) == 0 {
			out = append(out, in.path)
		}
	case s.Type == "array":
		if s.MaxItems == nil {
			out = append(out, in.path+" (list)")
		}
		if s.Items != nil && s.Items.Type != "object" {
			out = append(out, celInputUnbounded(celInput{path: in.path + "[]", schema: *s.Items})...)
		}
	case s.AdditionalProperties != nil:
		if s.MaxProperties == nil {
			out = append(out, in.path+" (map)")
		}
	case s.Type == "object" && !in.container:
		// Compared whole: every leaf is read.
		unboundedLeaves(s, in.path, &out)
	}
	return out
}

// celRules collects every x-kubernetes-validations rule under s.
func celRules(s apiCRDSchema, path string, visit func(path string, node apiCRDSchema, rule string)) {
	for _, v := range s.Validations {
		visit(path, s, v.Rule)
	}
	for name, p := range s.Properties {
		celRules(p, path+"."+name, visit)
	}
	if s.Items != nil {
		celRules(*s.Items, path+"[]", visit)
	}
	if s.AdditionalProperties != nil {
		celRules(*s.AdditionalProperties, path+"{}", visit)
	}
}

// celUnboundedExempt are rule inputs left without a bound, with why.
var celUnboundedExempt = map[string]string{
	// controller-gen refuses MaxLength on a resource.Quantity, and the
	// rule is outside every list, so its worst case is one request-sized
	// parse, within budget on 1.31 and 1.37. Making it a string type like
	// StorageQuantity would force quoting a byte count for no cost gain.
	"MysqlFailoverGroup.spec.dragonfly.maxMemory (int-or-string)": "Quantity",
	// Operator-written status, compared whole by the write-once rule; the
	// values come from bounded spec fields. A bound on status would refuse
	// a status write mid-run (no ratcheting on 1.31) rather than protect
	// anything; the rule sits at the root, outside every list.
	"MysqlBackup.status.artifactStorage.bucket":      "status",
	"MysqlBackup.status.artifactStorage.claimName":   "status",
	"MysqlBackup.status.artifactStorage.endpointURL": "status",
	"MysqlBackup.status.artifactStorage.location":    "status",
	"MysqlBackup.status.artifactStorage.region":      "status",
}

// TestCELRuleInputsAreBounded (A-92): every string, list and map a CEL rule
// reads has a maxLength, maxItems or maxProperties (or an enum), so each
// rule's estimated cost does not depend on the request size limit. The
// envtest suite installing the CRDs on 1.31 and 1.37 is the cost check
// itself; this names the field when a new rule reads an unbounded one.
func TestCELRuleInputsAreBounded(t *testing.T) {
	kinds := map[string]string{
		"shipstream.io_mysqlfailovergroups.yaml":      "MysqlFailoverGroup",
		"shipstream.io_mysqlbackups.yaml":             "MysqlBackup",
		"shipstream.io_mysqlbackupverifications.yaml": "MysqlBackupVerification",
		"shipstream.io_mysqldatabases.yaml":           "MysqlDatabase",
		"shipstream.io_mysqlstandbyclusters.yaml":     "MysqlStandbyCluster",
	}
	exemptSeen := map[string]bool{}
	rules := 0
	for file, kind := range kinds {
		for crdPath, root := range apiCRDRoots(t, file) {
			unbounded := map[string]bool{}
			celRules(root, kind, func(path string, node apiCRDSchema, rule string) {
				rules++
				for _, in := range celRuleInputs(t, path, node, rule) {
					for _, u := range celInputUnbounded(in) {
						if _, ok := celUnboundedExempt[u]; ok {
							exemptSeen[u] = true
							continue
						}
						unbounded[u] = true
					}
				}
			})
			var list []string
			for u := range unbounded {
				list = append(list, u)
			}
			sort.Strings(list)
			for _, u := range list {
				t.Errorf("%s: a CEL rule reads %s, which has no maxLength, maxItems or maxProperties", crdPath, u)
			}
		}
	}
	if rules < 50 {
		t.Fatalf("found %d CEL rules; the walk is broken", rules)
	}
	for u := range celUnboundedExempt {
		if !exemptSeen[u] {
			t.Errorf("exemption %s matched no rule input; remove it", u)
		}
	}
}

// TestCELRuleInputsResolve pins the reference walk on rules of each shape,
// so a broken walk does not pass TestCELRuleInputsAreBounded vacuously.
func TestCELRuleInputsResolve(t *testing.T) {
	str := func(n int64) apiCRDSchema { return apiCRDSchema{Type: "string", MaxLength: &n} }
	free := apiCRDSchema{Type: "string"}
	node := apiCRDSchema{Type: "object", Properties: map[string]apiCRDSchema{
		"name":  free,
		"label": str(10),
		"items": {Type: "array", Items: &apiCRDSchema{Type: "object", Properties: map[string]apiCRDSchema{
			"name": free,
		}}},
		"conf": {Type: "object", AdditionalProperties: &free},
		"ref":  {Type: "object", Properties: map[string]apiCRDSchema{"name": free}},
	}}
	for _, tc := range []struct {
		rule string
		want []string
	}{
		{"self.name == 'x'", []string{"N.name"}},
		{"has(self.name) && self.label.matches('^a$')", nil},
		{"self.items.all(i, i.name != self.label)", []string{"N.items (list)", "N.items[].name"}},
		{"self.conf.all(k, k.matches('^a$'))", []string{"N.conf (map)"}},
		{"self.ref == oldSelf.ref", []string{"N.ref.name"}},
		{"oldSelf.hasValue() || size(self.name) > 0", []string{"N.name"}},
		{"self.items.exists(i, i.name == 'a') && self.items.all(i, has(i.name))", []string{"N.items (list)", "N.items[].name"}},
	} {
		got := map[string]bool{}
		for _, in := range celRuleInputs(t, "N", node, tc.rule) {
			for _, u := range celInputUnbounded(in) {
				got[u] = true
			}
		}
		var gotList []string
		for u := range got {
			gotList = append(gotList, u)
		}
		sort.Strings(gotList)
		if strings.Join(gotList, ",") != strings.Join(tc.want, ",") {
			t.Errorf("rule %q: unbounded %v, want %v", tc.rule, gotList, tc.want)
		}
	}
}
