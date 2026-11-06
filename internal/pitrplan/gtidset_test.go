package pitrplan

import "testing"

// ParseUserSet is the one canonical spelling the controller renders for
// excludeGtids, pitr-download writes to coverage.json and restore.py
// compares (BLOODRAVEN_PITR_EXCLUDE_GTIDS against "excluded").
func TestParseUserSetCanonical(t *testing.T) {
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const v = "4f22ab58-82db-22f2-af44-d91bba530673"
	for _, tc := range []struct {
		name, in, want string
	}{
		{"empty", "", ""},
		{"whitespace", "  \n ", ""},
		{"merged intervals", u + ":5-7:1-4:9", u + ":1-7:9"},
		{"single-element interval", u + ":3-3", u + ":3"},
		{"MySQL multi-line output, sorted", v + ":1-2,\n" + u + ":1-3", u + ":1-3," + v + ":1-2"},
		{"repeated UUID", u + ":1-2," + u + ":3", u + ":1-3"},
		{"upper-case UUID is the same source", "3E11FA47-71CA-11E1-9E33-C80AA9429562:4," + u + ":1-3", u + ":1-4"},
		{"tags grouped under one UUID", u + ":1-5:Blue:1-3", u + ":1-5," + u + ":blue:1-3"},
		{"upper-case UUID with a tag", "3E11FA47-71CA-11E1-9E33-C80AA9429562:tag:2," + u + ":tag:1", u + ":tag:1-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ParseUserSet(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got := s.String(); got != tc.want {
				t.Fatalf("ParseUserSet(%q).String() = %q, want %q", tc.in, got, tc.want)
			}
			again, err := ParseUserSet(s.String())
			if err != nil || again.String() != tc.want {
				t.Fatalf("canonical form does not round-trip: %q, %v", again.String(), err)
			}
		})
	}
	if _, err := ParseUserSet(u + ":5-3"); err == nil {
		t.Fatal("a reversed interval parsed")
	}
}
