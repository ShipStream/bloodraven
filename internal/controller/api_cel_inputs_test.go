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
	celOptionalHas   = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*(?:\.\??[A-Za-z_][A-Za-z0-9_]*)*\.\?[A-Za-z_][A-Za-z0-9_]*(?:\.\??[A-Za-z_][A-Za-z0-9_]*)*\.hasValue\(\)`)
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
// has(...) and x.?f.hasValue() with true: both test presence and cost
// nothing per byte.
func celNormalize(rule string) string {
	rule = celStringLiteral.ReplaceAllString(rule, "''")
	rule = strings.ReplaceAll(rule, "oldSelf.value()", "oldSelf")
	rule = celOptionalHas.ReplaceAllString(rule, "true")
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
		{"oldSelf.?name.hasValue() || size(self.name) > 0", []string{"N.name"}},
		{"oldSelf.?ref.name.hasValue() || self.label == 'a'", nil},
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

// celLeaves lists the leaf paths under s: scalars, a list's items as
// path[] and a map's values as path{}.
func celLeaves(s apiCRDSchema, path string, out map[string]bool) {
	switch {
	case s.Items != nil:
		celLeaves(*s.Items, path+"[]", out)
	case s.AdditionalProperties != nil:
		celLeaves(*s.AdditionalProperties, path+"{}", out)
	case len(s.Properties) > 0:
		for name, p := range s.Properties {
			celLeaves(p, path+"."+name, out)
		}
	default:
		out[path] = true
	}
}

// celFrozenLeafExempt are leaves under a fixed-at-creation block that no
// root transition rule reads, with why each may change. Empty today: every
// leaf of the three blocks is frozen.
var celFrozenLeafExempt = map[string]string{}

// frozenLeavesNotCompared returns the leaves under each block (a spec path
// of root) that no root rule using oldSelf reads, minus the exemptions.
func frozenLeavesNotCompared(t *testing.T, kind string, root apiCRDSchema, blocks []string) []string {
	t.Helper()
	read := map[string]bool{}
	rootRules := 0
	for _, v := range root.Validations {
		if !strings.Contains(v.Rule, "oldSelf") {
			continue
		}
		rootRules++
		for _, in := range celRuleInputs(t, kind, root, v.Rule) {
			if in.container {
				continue
			}
			celLeaves(in.schema, in.path, read)
		}
	}
	if rootRules == 0 {
		t.Fatalf("%s: no root transition rule; the walk is broken", kind)
	}
	var missing []string
	for _, block := range blocks {
		s := root
		for _, seg := range strings.Split(block, ".") {
			var ok bool
			if s, ok = s.Properties[seg]; !ok {
				t.Fatalf("%s: no %s in the schema", kind, block)
			}
		}
		leaves := map[string]bool{}
		celLeaves(s, kind+"."+block, leaves)
		for leaf := range leaves {
			if _, exempt := celFrozenLeafExempt[leaf]; !read[leaf] && !exempt {
				missing = append(missing, leaf)
			}
		}
	}
	sort.Strings(missing)
	return missing
}

// TestFrozenBlocksCompareEveryLeaf (A-22, A-23): spec.initFromBackup on a
// MysqlFailoverGroup and spec.pointInTime and spec.sanityCheck on a
// MysqlBackupVerification are fixed at creation by root transition rules
// that compare them field by field. A field added to one of those types
// later is not frozen until a rule reads it, so every leaf must be read by
// a root rule that uses oldSelf, or be listed in celFrozenLeafExempt with
// the reason it may change. A leaf compared only through its parent object
// (self.x == oldSelf.x) counts as read.
func TestFrozenBlocksCompareEveryLeaf(t *testing.T) {
	for _, tc := range []struct {
		file, kind string
		blocks     []string
	}{
		{"shipstream.io_mysqlfailovergroups.yaml", "MysqlFailoverGroup", []string{"spec.initFromBackup"}},
		{"shipstream.io_mysqlbackupverifications.yaml", "MysqlBackupVerification", []string{"spec.pointInTime", "spec.sanityCheck"}},
	} {
		for crdPath, root := range apiCRDRoots(t, tc.file) {
			for _, leaf := range frozenLeavesNotCompared(t, tc.kind, root, tc.blocks) {
				t.Errorf("%s: %s is in a fixed-at-creation block but no root transition rule compares it; add it to the rule or exempt it with the reason", crdPath, leaf)
			}
		}
	}

	// The check itself: a leaf the rule leaves out is reported, and one
	// compared through its parent object is not.
	str := apiCRDSchema{Type: "string"}
	synthetic := func(rule string) apiCRDSchema {
		root := apiCRDSchema{Type: "object", Properties: map[string]apiCRDSchema{"spec": {Type: "object", Properties: map[string]apiCRDSchema{
			"blk": {Type: "object", Properties: map[string]apiCRDSchema{
				"a":   str,
				"b":   str,
				"ref": {Type: "object", Properties: map[string]apiCRDSchema{"name": str}},
			}},
		}}}}
		root.Validations = append(root.Validations, struct {
			Rule string `json:"rule"`
		}{rule})
		return root
	}
	if got := frozenLeavesNotCompared(t, "K", synthetic("self.spec.blk.a == oldSelf.spec.blk.a && self.spec.blk.ref == oldSelf.spec.blk.ref"), []string{"spec.blk"}); strings.Join(got, ",") != "K.spec.blk.b" {
		t.Errorf("synthetic rule missing b: reported %v, want [K.spec.blk.b]", got)
	}
	if got := frozenLeavesNotCompared(t, "K", synthetic("has(self.spec.blk.b) == has(oldSelf.spec.blk.b) && self.spec.blk.a == oldSelf.spec.blk.a && self.spec.blk.ref.name == oldSelf.spec.blk.ref.name"), []string{"spec.blk"}); strings.Join(got, ",") != "K.spec.blk.b" {
		t.Errorf("presence-only check of b: reported %v, want [K.spec.blk.b]", got)
	}
}
