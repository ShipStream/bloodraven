package actionvalue

import (
	"strings"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	destructive := Grammar{Keys: []string{ConfirmKey, UIDKey, "gtidPrefix"}, Required: []string{ConfirmKey, UIDKey}}
	operation := Grammar{}
	list := Grammar{SubjectList: true}
	deadline := Grammar{Keys: []string{"since"}, Required: []string{"since"}}
	cases := []struct {
		name     string
		g        Grammar
		in       string
		subjects []string
		pairs    []Pair
		wantErr  string
	}{
		{name: "site subject with confirm and uid", g: destructive, in: "iad:confirm=orders:uid=6f1c2d3e-0000-4000-8000-000000000001",
			subjects: []string{"iad"}, pairs: []Pair{{ConfirmKey, "orders"}, {UIDKey, "6f1c2d3e-0000-4000-8000-000000000001"}}},
		{name: "gtidPrefix pair", g: destructive, in: "iad:confirm=orders:uid=u:gtidPrefix=3e11fa47",
			subjects: []string{"iad"}, pairs: []Pair{{ConfirmKey, "orders"}, {UIDKey, "u"}, {"gtidPrefix", "3e11fa47"}}},
		{name: "whitespace around the value is trimmed", g: destructive, in: "  iad:confirm=orders:uid=u\n",
			subjects: []string{"iad"}, pairs: []Pair{{ConfirmKey, "orders"}, {UIDKey, "u"}}},
		{name: "operation-id subject needs no confirm", g: operation, in: "6f1c2d3e-0000-4000-8000-000000000001",
			subjects: []string{"6f1c2d3e-0000-4000-8000-000000000001"}},
		{name: "comma-separated id list", g: list, in: "run-a,run-b,run-c", subjects: []string{"run-a", "run-b", "run-c"}},
		{name: "basic-form instant value", g: deadline, in: "run-a:since=20261104T100000Z",
			subjects: []string{"run-a"}, pairs: []Pair{{"since", "20261104T100000Z"}}},
		{name: "value charset", g: Grammar{Keys: []string{"k"}}, in: "s:k=A-z_0.9/+", subjects: []string{"s"}, pairs: []Pair{{"k", "A-z_0.9/+"}}},

		{name: "empty", g: operation, in: "  ", wantErr: "empty"},
		{name: "no subject", g: destructive, in: ":confirm=orders:uid=u", wantErr: "names no subject"},
		{name: "missing confirm", g: destructive, in: "iad:uid=u", wantErr: "confirm= pair is missing"},
		{name: "missing uid", g: destructive, in: "iad:confirm=orders", wantErr: "uid= pair is missing"},
		{name: "a ':' in a value", g: destructive, in: "iad:confirm=orders:uid=u:gtidPrefix=3e11fa47:1-5", wantErr: "must be key=value"},
		{name: "RFC 3339 instant", g: deadline, in: "run-a:since=2026-11-04T10:00:00Z", wantErr: "must be key=value"},
		{name: "an '=' in a value", g: destructive, in: "iad:confirm=a=b:uid=u", wantErr: "the value must match"},
		{name: "empty value", g: destructive, in: "iad:confirm=:uid=u", wantErr: "the value must match"},
		{name: "empty pair", g: destructive, in: "iad::confirm=orders:uid=u", wantErr: "must be key=value"},
		{name: "pair without '='", g: destructive, in: "iad:a1b2c3d4", wantErr: "must be key=value"},
		{name: "unknown key", g: destructive, in: "iad:confirm=orders:uid=u:force=1", wantErr: "unknown key"},
		{name: "repeated key", g: destructive, in: "iad:confirm=orders:confirm=orders:uid=u", wantErr: "more than once"},
		{name: "key that is not camelCase", g: destructive, in: "iad:Confirm=orders:uid=u", wantErr: "camelCase"},
		{name: "pair on an action that takes none", g: operation, in: "run-a:confirm=orders", wantErr: "takes no key=value pairs"},
		{name: "list where one subject is taken", g: operation, in: "run-a,run-b", wantErr: "takes one subject"},
		{name: "empty list element", g: list, in: "run-a,,run-b", wantErr: "characters outside"},
		{name: "repeated list element", g: list, in: "run-a,run-a", wantErr: "more than once"},
		{name: "subject charset", g: operation, in: "pdx east", wantErr: "characters outside"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := Parse(tc.in, tc.g)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Parse(%q) err = %v, want %q", tc.in, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.in, err)
			}
			if strings.Join(v.Subjects, ",") != strings.Join(tc.subjects, ",") || len(v.Pairs) != len(tc.pairs) {
				t.Fatalf("Parse(%q) = %+v", tc.in, v)
			}
			for i, p := range tc.pairs {
				if v.Pairs[i] != p {
					t.Fatalf("Parse(%q) pair %d = %+v, want %+v", tc.in, i, v.Pairs[i], p)
				}
			}
		})
	}
}

func TestFormatRoundTrips(t *testing.T) {
	g := Grammar{Keys: []string{ConfirmKey, UIDKey, "gtidPrefix"}, Required: []string{ConfirmKey, UIDKey}}
	pairs := append(ConfirmPairs("orders", "6f1c2d3e-0000-4000-8000-000000000001"), Pair{Key: "gtidPrefix", Value: "3e11fa47"})
	s := Format("iad", pairs...)
	if s != "iad:confirm=orders:uid=6f1c2d3e-0000-4000-8000-000000000001:gtidPrefix=3e11fa47" {
		t.Fatalf("Format = %q", s)
	}
	v, err := Parse(s, g)
	if err != nil || v.Subject() != "iad" {
		t.Fatalf("Parse(Format) = %+v, %v", v, err)
	}
	if err := Confirm(v, "orders", "6f1c2d3e-0000-4000-8000-000000000001"); err != nil {
		t.Fatalf("Confirm: %v", err)
	}
}

func TestConfirm(t *testing.T) {
	v := Value{Subjects: []string{"iad"}, Pairs: ConfirmPairs("orders", "uid-1")}
	if err := Confirm(v, "orders", "uid-1"); err != nil {
		t.Fatal(err)
	}
	if err := Confirm(v, "other", "uid-1"); err == nil || !strings.Contains(err.Error(), "does not name this object") {
		t.Fatalf("another name: %v", err)
	}
	if err := Confirm(v, "orders", "uid-2"); err == nil || !strings.Contains(err.Error(), "earlier object of the same name") {
		t.Fatalf("a re-created object: %v", err)
	}
	if err := Confirm(Value{Subjects: []string{"iad"}}, "orders", "uid-1"); err == nil {
		t.Fatal("no pairs accepted")
	}
}

func TestInstant(t *testing.T) {
	at := time.Date(2026, 11, 4, 10, 0, 0, 999_000_000, time.FixedZone("x", 3600))
	s := FormatInstant(at)
	if s != "20261104T090000Z" {
		t.Fatalf("FormatInstant = %q", s)
	}
	if !ValidToken(s) {
		t.Fatalf("%q is not a valid value", s)
	}
	got, err := ParseInstant(s)
	if err != nil || !got.Equal(at.Truncate(time.Second)) {
		t.Fatalf("ParseInstant(%q) = %v, %v", s, got, err)
	}
	// Compared as an instant: the same moment in another offset is equal.
	if !got.Equal(time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)) {
		t.Fatal("not the same instant")
	}
	for _, bad := range []string{"2026-11-04T10:00:00Z", "20261104T100000", "20261104T100000+0100", ""} {
		if _, err := ParseInstant(bad); err == nil {
			t.Errorf("ParseInstant(%q) accepted", bad)
		}
	}
}
