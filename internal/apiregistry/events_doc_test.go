package apiregistry

import (
	"flag"
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The Event reason registry (A-76). api/v1alpha1/events.go declares every
// Event reason in const blocks, one block per documentation section. A
// block's doc comment starts with the line
//
//	Events: <section title>. On: <Kind>[, <Kind>].
//
// followed by optional paragraphs rendered under the section heading. Each
// constant's own doc comment is its description. Condition-mirror
// constants (declared as a condition reason, A-74) without a doc comment
// share one row, described by the block's paragraphs. The Event type
// (Normal or Warning) comes from the code that records the Event, not from
// the declaration.
//
// The "Kubernetes Events" section of 2.monitoring.md is generated from
// the registry: run `make events-doc` (this test with -update) after
// changing events.go or an Event's type.

var updateEventsDoc = flag.Bool("update", false, "rewrite the generated Event reason section of "+eventsDoc)

const (
	eventsDoc = "../../site/content/docs/8.observability/2.monitoring.md"
	// eventsDocStart and eventsDocEnd bound the generated part of the
	// page: it starts at the first "### " heading after eventsDocStart's
	// introduction and ends before eventsDocEnd.
	eventsDocStart = "## Kubernetes Events"
	eventsDocEnd   = "### Forwarding events with Kubewatch"
)

// eventDocRow matches a generated table row: | <reasons> | <type> |.
var eventDocRow = regexp.MustCompile(`(?m)^\| (.+?) \| (?:Normal|Warning) \|`)

var eventBlockHeader = regexp.MustCompile(`^Events: (.+)\. On: ([A-Za-z]+(?:, [A-Za-z]+)*)\.$`)

// eventKinds are the kinds an Event can be recorded on.
var eventKinds = map[string]bool{
	"MysqlFailoverGroup": true, "MysqlBackup": true, "MysqlBackupVerification": true,
	"MysqlDatabase": true, "MysqlStandbyCluster": true,
}

type eventBlock struct {
	title   string
	kinds   []string
	intro   []string // Markdown blocks: paragraphs and list items
	members []eventMember
	pos     token.Position
}

type eventMember struct {
	name, value, desc string
	mirror            bool
	pos               token.Position
}

// eventBlocks parses events.go into its sections. Problems with the
// declarations themselves are test errors.
func eventBlocks(t *testing.T, a *analysis, reg registry) []eventBlock {
	t.Helper()
	api := a.apiPackage(t)
	var out []eventBlock
	for _, f := range api.Syntax {
		if !strings.HasSuffix(a.pos(f.Pos()).Filename, "/events.go") {
			continue
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			b := eventBlock{pos: a.pos(gd.Pos())}
			lines := commentLines(gd.Doc)
			if len(lines) == 0 {
				t.Errorf("events.go:%d: const block without an \"Events: <title>. On: <kinds>.\" doc comment", b.pos.Line)
				continue
			}
			m := eventBlockHeader.FindStringSubmatch(strings.TrimSpace(lines[0]))
			if m == nil {
				t.Errorf("events.go:%d: block doc comment starts %q, want \"Events: <title>. On: <Kind>[, <Kind>].\"", b.pos.Line, lines[0])
				continue
			}
			b.title = m[1]
			b.kinds = strings.Split(m[2], ", ")
			for _, k := range b.kinds {
				if !eventKinds[k] {
					t.Errorf("events.go:%d: %q is not a kind Events are recorded on", b.pos.Line, k)
				}
			}
			b.intro = markdownBlocks(lines[1:])
			if gd.Lparen == token.NoPos {
				t.Errorf("events.go:%d: declare Event reasons in a parenthesised const block", b.pos.Line)
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for _, n := range vs.Names {
					c := api.TypesInfo.Defs[n].(*types.Const)
					if c.Val().Kind() != constant.String {
						t.Errorf("events.go: %s is not a string constant", n.Name)
						continue
					}
					v := constant.StringVal(c.Val())
					_, mirror := reg.conditionReasons[v]
					desc := strings.Join(markdownBlocks(commentLines(vs.Doc)), " ")
					if desc == "" && !mirror {
						t.Errorf("events.go: %s has no doc comment; describe the Event for the generated docs table", n.Name)
					}
					b.members = append(b.members, eventMember{name: n.Name, value: v, desc: desc, mirror: mirror, pos: a.pos(n.Pos())})
				}
			}
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		t.Fatal("events.go declares no Event reason blocks")
	}
	return out
}

// commentLines returns a doc comment's lines without the comment markers.
func commentLines(g *ast.CommentGroup) []string {
	if g == nil {
		return nil
	}
	var out []string
	for _, c := range g.List {
		l := strings.TrimPrefix(c.Text, "//")
		l = strings.TrimPrefix(l, " ")
		out = append(out, l)
	}
	return out
}

// markdownBlocks joins comment lines into Markdown blocks: paragraphs (lines
// joined by spaces) and "- " list items (gofmt indents them; a deeper
// indented line continues the item).
func markdownBlocks(lines []string) []string {
	var out []string
	cur := ""
	inItem := false
	flush := func() {
		if cur != "" {
			out = append(out, cur)
		}
		cur, inItem = "", false
	}
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		switch {
		case trimmed == "":
			flush()
		case strings.HasPrefix(trimmed, "- "):
			flush()
			cur, inItem = trimmed, true
		case inItem && strings.HasPrefix(l, "  "):
			cur += " " + trimmed
		default:
			if inItem {
				flush()
			}
			if cur == "" {
				cur = trimmed
			} else {
				cur += " " + trimmed
			}
		}
	}
	flush()
	return out
}

// eventFacts is what the code records for one Event reason.
type eventFacts struct {
	types   map[string]bool
	objects map[string]bool
	sites   []token.Position
}

func recordedEvents(t *testing.T, sinks []sink) map[string]*eventFacts {
	t.Helper()
	out := map[string]*eventFacts{}
	for _, s := range sinks {
		if !s.recorded {
			continue
		}
		for _, p := range s.pairProblems {
			t.Errorf("Event at %s: %s", s.pos, p)
		}
		for _, p := range s.pairs {
			f := out[p.reason]
			if f == nil {
				f = &eventFacts{types: map[string]bool{}, objects: map[string]bool{}}
				out[p.reason] = f
			}
			f.types[p.eventType] = true
			for _, o := range s.objects {
				f.objects[o] = true
			}
			f.sites = append(f.sites, s.pos)
		}
	}
	return out
}

// TestEventReasonsHaveOneType (A-75): every Event reason is recorded with
// one Event type, Normal or Warning, wherever it is recorded.
func TestEventReasonsHaveOneType(t *testing.T) {
	_, sinks := analysed(t)
	facts := recordedEvents(t, sinks)
	for _, v := range sortedKeys(facts) {
		f := facts[v]
		types := sortedKeys(f.types)
		for _, ty := range types {
			if ty != "Normal" && ty != "Warning" {
				t.Errorf("Event %s is recorded with type %q", v, ty)
			}
		}
		if len(types) > 1 {
			t.Errorf("Event %s is recorded as %s (at %v); one reason has one type: give the other case its own reason", v, strings.Join(types, " and "), f.sites)
		}
	}
}

// TestEventDeclarations keeps the registry and the code in step: every
// declared reason is recorded somewhere, on the kinds its section
// declares, and each reason is declared once.
func TestEventDeclarations(t *testing.T) {
	a, sinks := analysed(t)
	reg := newRegistry(t, a)
	facts := recordedEvents(t, sinks)
	seen := map[string]string{}
	for _, b := range eventBlocks(t, a, reg) {
		kinds := map[string]bool{}
		for _, k := range b.kinds {
			kinds[k] = true
		}
		for _, m := range b.members {
			if prev, dup := seen[m.value]; dup {
				t.Errorf("events.go: %s and %s both declare %q", prev, m.name, m.value)
			}
			seen[m.value] = m.name
			f := facts[m.value]
			if f == nil {
				t.Errorf("events.go: %s = %q is never recorded; remove it or record it", m.name, m.value)
				continue
			}
			for _, o := range sortedKeys(f.objects) {
				if !kinds[o] {
					t.Errorf("events.go: %s is recorded on %s (at %v), but its section %q declares %s", m.name, o, f.sites, b.title, strings.Join(b.kinds, ", "))
				}
			}
		}
	}
}

// TestEventReasonDocTable: the Event reason section of 2.monitoring.md is
// generated from the registry, so every declared reason is documented
// with the type the code records it with, and nothing else is.
func TestEventReasonDocTable(t *testing.T) {
	a, sinks := analysed(t)
	reg := newRegistry(t, a)
	facts := recordedEvents(t, sinks)
	want := renderEventsDoc(t, eventBlocks(t, a, reg), facts)

	raw, err := os.ReadFile(eventsDoc)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	start, end := eventsDocRegion(t, doc)
	if *updateEventsDoc {
		if err := os.WriteFile(eventsDoc, []byte(doc[:start]+want+doc[end:]), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if got := doc[start:end]; got != want {
		t.Errorf("the Event reason section of %s is stale; run `make events-doc`.\n%s", eventsDoc, firstDiff(got, want))
	}

	// Every declared reason appears in the generated table.
	documented := map[string]bool{}
	for _, row := range eventDocRow.FindAllStringSubmatch(want, -1) {
		for _, v := range backticked.FindAllStringSubmatch(row[1], -1) {
			documented[v[1]] = true
		}
	}
	for v, name := range reg.events {
		if !documented[v] {
			t.Errorf("events.go: %s = %q has no row in the generated table", name, v)
		}
	}
	for v := range documented {
		if _, ok := reg.events[v]; !ok {
			t.Errorf("the generated table documents %q, which events.go does not declare", v)
		}
	}
}

func eventsDocRegion(t *testing.T, doc string) (int, int) {
	t.Helper()
	i := strings.Index(doc, "\n"+eventsDocStart+"\n")
	if i < 0 {
		t.Fatalf("%s has no %q heading", eventsDoc, eventsDocStart)
	}
	j := strings.Index(doc[i+1:], "\n### ")
	if j < 0 {
		t.Fatalf("%s: no \"### \" heading after %q", eventsDoc, eventsDocStart)
	}
	start := i + 1 + j + 1
	k := strings.Index(doc[start:], "\n"+eventsDocEnd+"\n")
	if k < 0 {
		t.Fatalf("%s has no %q heading after %q", eventsDoc, eventsDocEnd, eventsDocStart)
	}
	return start, start + k + 1
}

func renderEventsDoc(t *testing.T, blocks []eventBlock, facts map[string]*eventFacts) string {
	t.Helper()
	typeOf := func(m eventMember) string {
		f := facts[m.value]
		if f == nil {
			return "?"
		}
		ts := sortedKeys(f.types)
		if len(ts) != 1 {
			return strings.Join(ts, " / ")
		}
		return ts[0]
	}
	var sb strings.Builder
	for _, b := range blocks {
		fmt.Fprintf(&sb, "### %s\n\n", b.title)
		kinds := make([]string, len(b.kinds))
		for i, k := range b.kinds {
			kinds[i] = "`" + k + "`"
		}
		fmt.Fprintf(&sb, "Recorded on %s.", joinAnd(kinds))
		var grouped []eventMember
		for _, m := range b.members {
			if m.desc == "" {
				grouped = append(grouped, m)
			}
		}
		if len(grouped) == 0 {
			for i, p := range b.intro {
				sep := "\n\n"
				if i > 0 && strings.HasPrefix(p, "- ") && strings.HasPrefix(b.intro[i-1], "- ") {
					sep = "\n" // the next item of a list
				}
				sb.WriteString(sep + p)
			}
		} else if len(b.intro) == 0 {
			t.Errorf("events.go:%d: section %q has undocumented condition mirrors but no paragraph to describe them", b.pos.Line, b.title)
		}
		sb.WriteString("\n\n| Reason | Type | Description |\n|---|---|---|\n")
		for _, m := range b.members {
			if m.desc == "" {
				continue
			}
			desc := m.desc
			if m.mirror {
				desc += " Repeats the condition reason."
			}
			fmt.Fprintf(&sb, "| `%s` | %s | %s |\n", m.value, typeOf(m), tableCell(desc))
		}
		if len(grouped) > 0 {
			names := make([]string, len(grouped))
			types := map[string]bool{}
			for i, m := range grouped {
				names[i] = "`" + m.value + "`"
				types[typeOf(m)] = true
			}
			if len(types) > 1 {
				t.Errorf("events.go:%d: the condition mirrors of section %q share a row but are recorded as %v; describe them separately", b.pos.Line, b.title, sortedKeys(types))
			}
			fmt.Fprintf(&sb, "| %s | %s | %s |\n", strings.Join(names, ", "), strings.Join(sortedKeys(types), " / "),
				tableCell(strings.Join(b.intro, " ")+" Each repeats the condition reason."))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func tableCell(s string) string { return strings.ReplaceAll(s, "|", `\|`) }

func joinAnd(xs []string) string {
	switch len(xs) {
	case 0:
		return ""
	case 1:
		return xs[0]
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}

// firstDiff shows where two texts first differ.
func firstDiff(got, want string) string {
	g, w := strings.Split(got, "\n"), strings.Split(want, "\n")
	for i := 0; i < len(g) || i < len(w); i++ {
		var gl, wl string
		if i < len(g) {
			gl = g[i]
		}
		if i < len(w) {
			wl = w[i]
		}
		if gl != wl {
			return fmt.Sprintf("first difference at line %d of the section:\n got: %s\nwant: %s", i+1, gl, wl)
		}
	}
	return ""
}
