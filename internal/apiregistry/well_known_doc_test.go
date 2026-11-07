package apiregistry

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The well-known metadata registry (A-79). api/v1alpha1/well_known.go
// declares every label, annotation and finalizer key, and the read-only
// taint, in const blocks whose doc comment starts with
//
//	Keys: <section title>.
//
// followed by optional paragraphs for the section. Each key's doc comment
// describes it and ends with a paragraph of fields (On, Writer, Value,
// Stability, and Key for a prefix or the taint function). The page
// 4.configuration/12.well-known-metadata.md is generated from it: run
// `make well-known-doc` (this test with -update) after changing
// well_known.go.

const (
	wellKnownSource = "../../api/v1alpha1/well_known.go"
	wellKnownDoc    = "../../site/content/docs/4.configuration/12.well-known-metadata.md"
)

var (
	wellKnownBlockHeader = regexp.MustCompile(`^Keys: (.+)\.$`)
	wellKnownField       = regexp.MustCompile(`^(On|Writer|Value|Stability|Key): `)
	// wellKnownKey is A-77's key shape: bloodraven.shipstream.io/<name>, or
	// a subdomain of it for a family whose name part is an id.
	wellKnownKey      = regexp.MustCompile(`^([a-z0-9-]+\.)*bloodraven\.shipstream\.io/[a-z0-9-]*$`)
	wellKnownKeySuffx = []string{"Label", "Annotation", "AnnotationPrefix", "Finalizer"}
)

var wellKnownStabilities = []string{"Public", "Operator-owned read-only", "Internal (may change)"}

type wellKnownSection struct {
	title string
	intro []string
	keys  []wellKnownEntry
}

type wellKnownEntry struct {
	names, keys []string
	kind        string
	desc        string
	fields      map[string]string
}

// wellKnownSections parses well_known.go into its sections. Problems with
// the declarations are test errors.
func wellKnownSections(t *testing.T) []wellKnownSection {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, wellKnownSource, nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var out []wellKnownSection
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.GenDecl:
			if d.Tok != token.CONST {
				continue
			}
			lines := commentLines(d.Doc)
			header := ""
			if len(lines) > 0 {
				if m := wellKnownBlockHeader.FindStringSubmatch(strings.TrimSpace(lines[0])); m != nil {
					header = m[1]
				}
			}
			if header == "" {
				for _, spec := range d.Specs {
					for _, n := range spec.(*ast.ValueSpec).Names {
						if isKeyName(n.Name) {
							t.Errorf("well_known.go:%d: %s is outside a \"Keys: <title>.\" block", fset.Position(n.Pos()).Line, n.Name)
						}
					}
				}
				continue
			}
			sec := wellKnownSection{title: header, intro: markdownBlocks(lines[1:])}
			for _, spec := range d.Specs {
				vs := spec.(*ast.ValueSpec)
				e := wellKnownEntry{}
				for i, n := range vs.Names {
					if !isKeyName(n.Name) {
						t.Errorf("well_known.go:%d: %s in a Keys block is not named <Name>{Label,Annotation,AnnotationPrefix,Finalizer}", fset.Position(n.Pos()).Line, n.Name)
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Errorf("well_known.go: %s is not a string literal", n.Name)
						continue
					}
					v, _ := strconv.Unquote(lit.Value)
					e.names = append(e.names, n.Name)
					e.keys = append(e.keys, v)
					kind := keyKind(n.Name)
					if e.kind != "" && e.kind != kind {
						t.Errorf("well_known.go: %s shares a declaration with a key of another kind", n.Name)
					}
					e.kind = kind
				}
				if len(e.names) == 0 {
					continue
				}
				if vs.Doc == nil && len(sec.keys) > 0 {
					// A key declared on the line after another, without
					// its own doc comment, shares the previous one.
					prev := &sec.keys[len(sec.keys)-1]
					if prev.kind != e.kind {
						t.Errorf("well_known.go: %s shares a doc comment with a key of another kind", e.names[0])
					}
					prev.names = append(prev.names, e.names...)
					prev.keys = append(prev.keys, e.keys...)
					continue
				}
				e.desc, e.fields = keyDoc(t, e.names[0], commentLines(vs.Doc))
				sec.keys = append(sec.keys, e)
			}
			out = append(out, sec)
		case *ast.FuncDecl:
			if d.Name.Name != "ReadOnlyTaintKey" {
				continue
			}
			e := wellKnownEntry{names: []string{d.Name.Name}, kind: "taint"}
			e.desc, e.fields = keyDoc(t, d.Name.Name, commentLines(d.Doc))
			out = append(out, wellKnownSection{title: "Node taint", keys: []wellKnownEntry{e}})
		}
	}
	return out
}

func isKeyName(name string) bool {
	if !ast.IsExported(name) {
		return false
	}
	for _, s := range wellKnownKeySuffx {
		if strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}

func keyKind(name string) string {
	switch {
	case strings.HasSuffix(name, "Label"):
		return "label"
	case strings.HasSuffix(name, "Finalizer"):
		return "finalizer"
	default:
		return "annotation"
	}
}

// keyDoc splits a key's doc comment into its description and its fields.
func keyDoc(t *testing.T, name string, lines []string) (string, map[string]string) {
	t.Helper()
	fields := map[string]string{}
	var desc []string
	cur := ""
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if m := wellKnownField.FindStringSubmatch(l); m != nil {
			cur = m[1]
			if _, dup := fields[cur]; dup {
				t.Errorf("well_known.go: %s has two %s: fields", name, cur)
			}
			fields[cur] = strings.TrimPrefix(l, m[0])
			continue
		}
		if cur != "" {
			if l == "" {
				t.Errorf("well_known.go: %s: the fields must be the last paragraph", name)
				cur = ""
				continue
			}
			fields[cur] += " " + l
			continue
		}
		desc = append(desc, l)
	}
	for _, f := range []string{"On", "Writer", "Value", "Stability"} {
		if fields[f] == "" {
			t.Errorf("well_known.go: %s has no %s: field", name, f)
		}
	}
	if s := strings.TrimSuffix(fields["Stability"], "."); !slices.Contains(wellKnownStabilities, s) {
		t.Errorf("well_known.go: %s: Stability %q is not one of %q", name, fields["Stability"], wellKnownStabilities)
	}
	return strings.Join(markdownBlocks(desc), " "), fields
}

// TestWellKnownKeys pins A-77: every key is under bloodraven.shipstream.io,
// a prefix ends in '/', and no key is declared twice with one kind.
func TestWellKnownKeys(t *testing.T) {
	seen := map[string]string{}
	for _, sec := range wellKnownSections(t) {
		for _, e := range sec.keys {
			for i, k := range e.keys {
				name := e.names[i]
				if !wellKnownKey.MatchString(k) {
					t.Errorf("%s = %q is not bloodraven.shipstream.io/<name> or <family>.bloodraven.shipstream.io/<id>", name, k)
				}
				prefix := strings.HasSuffix(name, "AnnotationPrefix")
				if prefix != strings.HasSuffix(k, "/") {
					t.Errorf("%s = %q: a prefix, and only a prefix, ends in '/'", name, k)
				}
				if prefix && e.fields["Key"] == "" {
					t.Errorf("%s: a prefix needs a Key: field with the full key pattern", name)
				}
				id := e.kind + " " + k
				if other, dup := seen[id]; dup {
					t.Errorf("%s and %s declare the same %s", name, other, id)
				}
				seen[id] = name
			}
		}
	}
}

// TestWellKnownMetadataDoc checks that the generated page is current.
func TestWellKnownMetadataDoc(t *testing.T) {
	want := renderWellKnownDoc(wellKnownSections(t))
	if *updateEventsDoc {
		if err := os.WriteFile(wellKnownDoc, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	// Nuxt Content reads a bare <name> as an HTML tag: every angle
	// bracket on the page is inside a code span.
	for i, line := range strings.Split(want, "\n") {
		if strings.HasPrefix(line, "<!--") {
			continue
		}
		if outside := codeSpan.ReplaceAllString(line, ""); strings.ContainsAny(outside, "<>") {
			t.Errorf("generated line %d has '<' or '>' outside a code span; quote it in well_known.go: %s", i+1, line)
		}
	}
	got, err := os.ReadFile(wellKnownDoc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("%s is stale; run `make well-known-doc`.\n%s", wellKnownDoc, firstDiff(string(got), want))
	}
}

// docDescription turns a key's Go doc description into page prose: the
// leading identifiers become "This key" or "These keys" (a function's first,
// Go-facing sentence is dropped), and a key constant named in the text
// becomes its key.
func docDescription(e wellKnownEntry, keyOf map[string]string) string {
	d := e.desc
	if e.kind == "taint" {
		if _, rest, ok := strings.Cut(d, ". "); ok {
			d = rest
		}
	} else {
		lead := strings.Join(e.names, " and ") + " "
		if strings.HasPrefix(d, lead) {
			subject := "This key "
			if len(e.names) > 1 {
				subject = "These keys "
			}
			d = subject + strings.TrimPrefix(d, lead)
		}
	}
	return identifierRE.ReplaceAllStringFunc(d, func(w string) string {
		if k, ok := keyOf[w]; ok {
			return "`" + k + "`"
		}
		return w
	})
}

var codeSpan = regexp.MustCompile("`[^`]*`")

var identifierRE = regexp.MustCompile(`\b[A-Z][A-Za-z0-9]+\b`)

func renderWellKnownDoc(sections []wellKnownSection) string {
	keyOf := map[string]string{}
	for _, sec := range sections {
		for _, e := range sec.keys {
			for i, n := range e.names {
				if i < len(e.keys) {
					keyOf[n] = e.keys[i]
				}
			}
		}
	}
	var b strings.Builder
	b.WriteString(`---
title: Well-known metadata
description: Every label, annotation, finalizer and node taint Bloodraven reads or writes, with its writer, value grammar and stability level.
---

<!-- Generated from api/v1alpha1/well_known.go by ` + "`make well-known-doc`" + `. Do not edit. -->

This page lists every label, annotation and finalizer key Bloodraven reads or writes, and its node taint, with the objects that carry each one, who writes it, the grammar of its value, and how stable it is:

- **Public**: you may set or select on it, and it changes only with a release-noted break.
- **Operator-owned read-only**: the operator writes it; you may read it, but do not edit or remove it.
- **Internal (may change)**: operator bookkeeping. Do not read or edit it; its key and value can change in any release.

Every key is under ` + "`bloodraven.shipstream.io`" + `: the prefix itself (` + "`bloodraven.shipstream.io/<name>`" + `), or a subdomain of it for a family whose name part is an id (` + "`<kind>-hold.bloodraven.shipstream.io/<id>`" + `, ` + "`<namespace>.db-readonly.bloodraven.shipstream.io/<group>`" + `), so one prefix match finds every member. The ` + "`app.kubernetes.io/*`" + ` labels the operator also sets are the Kubernetes recommended labels and are not listed. A boolean label value is ` + "`true`" + ` or ` + "`false`" + `.

A structured internal value is a JSON object with a ` + "`schemaVersion`" + ` member. A payload with a version the operator does not know proves nothing: the operator refuses it and reports it with a condition and a Warning Event (` + "`MetadataVersionUnsupported`" + `), as each key below says.

`)
	for _, sec := range sections {
		fmt.Fprintf(&b, "## %s\n\n", sec.title)
		for _, p := range sec.intro {
			b.WriteString(p + "\n\n")
		}
		b.WriteString("| Key | On | Writer | Value and meaning | Stability |\n|---|---|---|---|---|\n")
		for _, e := range sec.keys {
			var keys []string
			if k := e.fields["Key"]; k != "" {
				keys = append(keys, strings.TrimSuffix(k, "."))
			} else {
				for _, k := range e.keys {
					keys = append(keys, "`"+k+"`")
				}
			}
			value := strings.TrimSuffix(e.fields["Value"], ".")
			meaning := value + "."
			if d := docDescription(e, keyOf); d != "" {
				meaning += " " + d
			}
			fmt.Fprintf(&b, "| %s (%s) | %s | %s | %s | %s |\n",
				tableCell(strings.Join(keys, ", ")), e.kind,
				tableCell(strings.TrimSuffix(e.fields["On"], ".")),
				tableCell(strings.TrimSuffix(e.fields["Writer"], ".")),
				tableCell(meaning),
				strings.TrimSuffix(e.fields["Stability"], "."))
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n") + "\n"
}
