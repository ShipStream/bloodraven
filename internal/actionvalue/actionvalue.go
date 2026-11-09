// Package actionvalue parses and formats the values of Bloodraven's
// one-shot action annotations, and of the operator's own annotation values
// that use the same shape (the planned-failover deadline starts).
//
// The grammar is
//
//	<subject>(:<key>=<value>)*
//
// The subject is a site name or an operation id; an action that accepts a
// list takes a comma-separated list of ids. A pair splits at its first '='.
// A subject id and a value match ^[A-Za-z0-9._/+-]+$, so neither contains
// ':', '=' or ',', and every split is unambiguous. A key is a camelCase
// word. An instant inside a value is ISO 8601 basic UTC
// (20261104T100000Z), compared as an instant: RFC 3339's colons would
// break the split.
//
// Destructive actions on a site-name subject carry
// confirm=<metadata.name> and uid=<metadata.uid> (Confirm). An
// operation-id subject the operator minted and recorded, and never reuses,
// cannot match another group or a re-created one, so it needs neither.
package actionvalue

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// ConfirmKey and UIDKey are the pairs a destructive action on a site-name
// subject carries: confirm=<metadata.name>:uid=<metadata.uid>.
const (
	ConfirmKey = "confirm"
	UIDKey     = "uid"
)

// InstantLayout is the ISO 8601 basic UTC form of an instant inside a
// value, at second precision.
const InstantLayout = "20060102T150405Z"

var (
	tokenRE = regexp.MustCompile(`^[A-Za-z0-9._/+-]+$`)
	keyRE   = regexp.MustCompile(`^[a-z][A-Za-z0-9]*$`)
)

// ValidToken reports whether s may be a subject id or a value.
func ValidToken(s string) bool { return tokenRE.MatchString(s) }

// Grammar is what one annotation accepts.
type Grammar struct {
	// Keys are the keys the value may carry. Any other key is an error.
	Keys []string
	// Required are the keys the value must carry (a subset of Keys).
	Required []string
	// SubjectList accepts a comma-separated list of ids as the subject.
	SubjectList bool
}

// Pair is one key=value pair.
type Pair struct {
	Key, Value string
}

// Value is a parsed annotation value.
type Value struct {
	// Subjects holds the subject: one element, or the ids of a list.
	Subjects []string
	// Pairs are the key=value pairs in their written order.
	Pairs []Pair
}

// Subject returns the single subject (the first id of a list).
func (v Value) Subject() string {
	if len(v.Subjects) == 0 {
		return ""
	}
	return v.Subjects[0]
}

// Get returns the value of key and whether the value carries it.
func (v Value) Get(key string) (string, bool) {
	for _, p := range v.Pairs {
		if p.Key == key {
			return p.Value, true
		}
	}
	return "", false
}

// Error is a value that does not match its grammar.
type Error struct {
	// Value is the raw value, trimmed.
	Value string
	// Problem says what is wrong, without the value.
	Problem string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%q: %s", e.Value, e.Problem)
}

// Parse parses raw against g. Whitespace around the whole value is
// trimmed (a shell heredoc's trailing newline). It returns an *Error for an
// empty value or subject, a subject or value outside the charset, a list
// where g takes one subject, a pair that is not key=value, an unknown or
// repeated key, or a missing required key.
func Parse(raw string, g Grammar) (Value, error) {
	v := strings.TrimSpace(raw)
	fail := func(format string, args ...any) (Value, error) {
		return Value{}, &Error{Value: v, Problem: fmt.Sprintf(format, args...)}
	}
	if v == "" {
		return fail("the value is empty")
	}
	parts := strings.Split(v, ":")
	var out Value
	if parts[0] == "" {
		return fail("the value names no subject")
	}
	subjects := []string{parts[0]}
	if strings.Contains(parts[0], ",") {
		if !g.SubjectList {
			return fail("the subject %q is a list; this annotation takes one subject", parts[0])
		}
		subjects = strings.Split(parts[0], ",")
	}
	for _, s := range subjects {
		if !ValidToken(s) {
			return fail("the subject %q contains characters outside [A-Za-z0-9._/+-]", s)
		}
		if slices.Contains(out.Subjects, s) {
			return fail("the subject list names %q more than once", s)
		}
		out.Subjects = append(out.Subjects, s)
	}
	for _, kv := range parts[1:] {
		key, val, ok := strings.Cut(kv, "=")
		if !ok || key == "" {
			return fail("%q must be key=value", kv)
		}
		if !keyRE.MatchString(key) {
			return fail("the key %q is not a camelCase word", key)
		}
		if !slices.Contains(g.Keys, key) {
			if len(g.Keys) == 0 {
				return fail("unknown key %q (this annotation takes no key=value pairs)", key)
			}
			return fail("unknown key %q (supported: %s)", key, strings.Join(g.Keys, ", "))
		}
		if _, dup := out.Get(key); dup {
			return fail("the key %q appears more than once", key)
		}
		if !ValidToken(val) {
			return fail("%s=%q: the value must match [A-Za-z0-9._/+-]+", key, val)
		}
		out.Pairs = append(out.Pairs, Pair{Key: key, Value: val})
	}
	for _, key := range g.Required {
		if _, ok := out.Get(key); !ok {
			return fail("the %s= pair is missing", key)
		}
	}
	return out, nil
}

// Format renders subject and pairs in the grammar. It does not validate:
// callers format values they built from validated parts.
func Format(subject string, pairs ...Pair) string {
	var b strings.Builder
	b.WriteString(subject)
	for _, p := range pairs {
		b.WriteString(":")
		b.WriteString(p.Key)
		b.WriteString("=")
		b.WriteString(p.Value)
	}
	return b.String()
}

// ConfirmPairs are the confirm= and uid= pairs naming the object name and
// uid, in that order.
func ConfirmPairs(name, uid string) []Pair {
	return []Pair{{Key: ConfirmKey, Value: name}, {Key: UIDKey, Value: uid}}
}

// Confirm checks the confirm= and uid= pairs of v against the object's
// name and uid. A uid that differs means the request was written for an
// earlier object of the same name.
func Confirm(v Value, name, uid string) error {
	confirm, ok := v.Get(ConfirmKey)
	if !ok {
		return fmt.Errorf("the %s= pair is missing", ConfirmKey)
	}
	if confirm != name {
		return fmt.Errorf("%s=%s does not name this object (%s)", ConfirmKey, confirm, name)
	}
	got, ok := v.Get(UIDKey)
	if !ok {
		return fmt.Errorf("the %s= pair is missing", UIDKey)
	}
	if got != uid {
		return fmt.Errorf("%s=%s is not this object's uid (%s): the request was written for an earlier object of the same name", UIDKey, got, uid)
	}
	return nil
}

// FormatInstant renders t in InstantLayout, truncated to the second.
func FormatInstant(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(InstantLayout)
}

// ParseInstant reads an InstantLayout value.
func ParseInstant(s string) (time.Time, error) {
	t, err := time.Parse(InstantLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("instant %q is not ISO 8601 basic UTC (YYYYMMDDThhmmssZ)", s)
	}
	return t, nil
}
