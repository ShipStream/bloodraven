package mysql

import "testing"

func TestParseGTIDSet_SingleUUID(t *testing.T) {
	gs, err := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gs) != 1 {
		t.Fatalf("expected 1 UUID, got %d", len(gs))
	}
	intervals := gs["3e11fa47-71ca-11e1-9e33-c80aa9429562"]
	if len(intervals) != 1 {
		t.Fatalf("expected 1 interval, got %d", len(intervals))
	}
	if intervals[0].Start != 1 || intervals[0].End != 5 {
		t.Errorf("expected 1-5, got %d-%d", intervals[0].Start, intervals[0].End)
	}
}

func TestParseGTIDSet_MultipleUUIDs(t *testing.T) {
	gs, err := ParseGTIDSet("uuid1:1-5,uuid2:1-3:7-9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gs) != 2 {
		t.Fatalf("expected 2 UUIDs, got %d", len(gs))
	}

	if len(gs["uuid1"]) != 1 {
		t.Fatalf("uuid1: expected 1 interval, got %d", len(gs["uuid1"]))
	}
	if gs["uuid1"][0].Start != 1 || gs["uuid1"][0].End != 5 {
		t.Errorf("uuid1: expected 1-5, got %d-%d", gs["uuid1"][0].Start, gs["uuid1"][0].End)
	}

	if len(gs["uuid2"]) != 2 {
		t.Fatalf("uuid2: expected 2 intervals, got %d", len(gs["uuid2"]))
	}
	if gs["uuid2"][0].Start != 1 || gs["uuid2"][0].End != 3 {
		t.Errorf("uuid2 interval 0: expected 1-3, got %d-%d", gs["uuid2"][0].Start, gs["uuid2"][0].End)
	}
	if gs["uuid2"][1].Start != 7 || gs["uuid2"][1].End != 9 {
		t.Errorf("uuid2 interval 1: expected 7-9, got %d-%d", gs["uuid2"][1].Start, gs["uuid2"][1].End)
	}
}

func TestParseGTIDSet_Empty(t *testing.T) {
	gs, err := ParseGTIDSet("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gs) != 0 {
		t.Errorf("expected empty set, got %d entries", len(gs))
	}
}

func TestParseGTIDSet_SingleTransaction(t *testing.T) {
	gs, err := ParseGTIDSet("uuid1:42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	intervals := gs["uuid1"]
	if len(intervals) != 1 {
		t.Fatalf("expected 1 interval, got %d", len(intervals))
	}
	if intervals[0].Start != 42 || intervals[0].End != 42 {
		t.Errorf("expected 42-42, got %d-%d", intervals[0].Start, intervals[0].End)
	}
}

func TestParseGTIDSet_Invalid(t *testing.T) {
	tests := []string{
		"no-colon",
		"uuid:",
		"uuid:abc",
		"uuid:5-3", // start > end
	}
	for _, input := range tests {
		_, err := ParseGTIDSet(input)
		if err == nil {
			t.Errorf("expected error for %q", input)
		}
	}
}

func TestGTIDSet_Contains(t *testing.T) {
	superset, _ := ParseGTIDSet("uuid1:1-10,uuid2:1-5")
	subset, _ := ParseGTIDSet("uuid1:3-7")
	empty := GTIDSet{}

	if !superset.Contains(subset) {
		t.Error("superset should contain subset")
	}
	if !superset.Contains(empty) {
		t.Error("any set should contain empty set")
	}
	if subset.Contains(superset) {
		t.Error("subset should not contain superset")
	}

	// Different UUID not in superset.
	other, _ := ParseGTIDSet("uuid3:1-5")
	if superset.Contains(other) {
		t.Error("superset should not contain set with unknown UUID")
	}
}

func TestGTIDSet_Contains_MultipleIntervals(t *testing.T) {
	// The set has gaps: 1-3 and 7-9 but not 4-6.
	set, _ := ParseGTIDSet("uuid1:1-3:7-9")
	within, _ := ParseGTIDSet("uuid1:1-3")
	across, _ := ParseGTIDSet("uuid1:1-5") // includes 4-5 which is not in set

	if !set.Contains(within) {
		t.Error("set should contain interval within first range")
	}
	if set.Contains(across) {
		t.Error("set should not contain interval spanning gap")
	}
}

func TestGTIDSet_TransactionCount(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"", 0},
		{"uuid1:1-5", 5},
		{"uuid1:1-5,uuid2:1-3:7-9", 5 + 3 + 3},
		{"uuid1:42", 1},
	}
	for _, tt := range tests {
		gs, err := ParseGTIDSet(tt.input)
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		got := gs.TransactionCount()
		if got != tt.want {
			t.Errorf("TransactionCount(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestGTIDSet_String(t *testing.T) {
	input := "uuid1:1-5"
	gs, _ := ParseGTIDSet(input)
	if gs.String() != input {
		t.Errorf("String() = %q, want %q", gs.String(), input)
	}

	// Single transaction should render without range.
	gs2, _ := ParseGTIDSet("uuid1:42")
	if gs2.String() != "uuid1:42" {
		t.Errorf("String() = %q, want %q", gs2.String(), "uuid1:42")
	}

	// Empty set.
	empty := GTIDSet{}
	if empty.String() != "" {
		t.Errorf("String() = %q, want empty", empty.String())
	}
}

func TestGTIDSet_String_Sorted(t *testing.T) {
	// Verify UUIDs are sorted in output.
	gs, _ := ParseGTIDSet("bbb:1-5,aaa:1-3")
	want := "aaa:1-3,bbb:1-5"
	if gs.String() != want {
		t.Errorf("String() = %q, want %q", gs.String(), want)
	}
}

func TestParseGTIDSet_TaggedGTID(t *testing.T) {
	gs, err := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:mytag:1-5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gs) != 1 {
		t.Fatalf("expected 1 UUID, got %d", len(gs))
	}
	intervals := gs["3e11fa47-71ca-11e1-9e33-c80aa9429562:mytag"]
	if len(intervals) != 1 {
		t.Fatalf("expected 1 interval, got %d", len(intervals))
	}
	if intervals[0].Start != 1 || intervals[0].End != 5 {
		t.Errorf("expected 1-5, got %d-%d", intervals[0].Start, intervals[0].End)
	}
}

func TestParseGTIDSet_TaggedGTID_SingleTransaction(t *testing.T) {
	gs, err := ParseGTIDSet("uuid1:admin:42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	intervals := gs["uuid1:admin"]
	if len(intervals) != 1 {
		t.Fatalf("expected 1 interval, got %d", len(intervals))
	}
	if intervals[0].Start != 42 || intervals[0].End != 42 {
		t.Errorf("expected 42-42, got %d-%d", intervals[0].Start, intervals[0].End)
	}
}

func TestParseGTIDSet_TaggedGTID_MultipleIntervals(t *testing.T) {
	gs, err := ParseGTIDSet("uuid1:mytag:1-3:7-9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	intervals := gs["uuid1:mytag"]
	if len(intervals) != 2 {
		t.Fatalf("expected 2 intervals, got %d", len(intervals))
	}
	if intervals[0].Start != 1 || intervals[0].End != 3 {
		t.Errorf("interval 0: expected 1-3, got %d-%d", intervals[0].Start, intervals[0].End)
	}
	if intervals[1].Start != 7 || intervals[1].End != 9 {
		t.Errorf("interval 1: expected 7-9, got %d-%d", intervals[1].Start, intervals[1].End)
	}
}

func TestParseGTIDSet_TaggedGTID_Mixed(t *testing.T) {
	gs, err := ParseGTIDSet("uuid1:1-5,uuid2:data:1-3:7-9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gs) != 2 {
		t.Fatalf("expected 2 UUIDs, got %d", len(gs))
	}

	if gs["uuid1"][0].Start != 1 || gs["uuid1"][0].End != 5 {
		t.Errorf("uuid1: expected 1-5, got %d-%d", gs["uuid1"][0].Start, gs["uuid1"][0].End)
	}

	if len(gs["uuid2:data"]) != 2 {
		t.Fatalf("uuid2:data: expected 2 intervals, got %d", len(gs["uuid2:data"]))
	}
	if gs["uuid2:data"][0].Start != 1 || gs["uuid2:data"][0].End != 3 {
		t.Errorf("uuid2:data interval 0: expected 1-3, got %d-%d", gs["uuid2:data"][0].Start, gs["uuid2:data"][0].End)
	}
	if gs["uuid2:data"][1].Start != 7 || gs["uuid2:data"][1].End != 9 {
		t.Errorf("uuid2:data interval 1: expected 7-9, got %d-%d", gs["uuid2:data"][1].Start, gs["uuid2:data"][1].End)
	}
}

func TestParseGTIDSet_TaggedSourcesRemainDistinct(t *testing.T) {
	gs, err := ParseGTIDSet("uuid1:1-5,uuid1:blue:1-5,uuid1:green:1-5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, sid := range []string{"uuid1", "uuid1:blue", "uuid1:green"} {
		if got := gs[sid]; len(got) != 1 || got[0] != (Interval{Start: 1, End: 5}) {
			t.Errorf("source %q intervals = %+v, want 1-5", sid, got)
		}
	}
	if got, want := gs.String(), "uuid1:1-5,uuid1:blue:1-5,uuid1:green:1-5"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestGTIDSet_TaggedSourceArithmetic(t *testing.T) {
	blue, err := ParseGTIDSet("uuid1:blue:1-5")
	if err != nil {
		t.Fatalf("parse blue: %v", err)
	}
	green, err := ParseGTIDSet("uuid1:green:1-5")
	if err != nil {
		t.Fatalf("parse green: %v", err)
	}
	untagged, err := ParseGTIDSet("uuid1:1-5")
	if err != nil {
		t.Fatalf("parse untagged: %v", err)
	}

	for name, other := range map[string]GTIDSet{"different tag": green, "untagged": untagged} {
		t.Run(name, func(t *testing.T) {
			if blue.Contains(other) {
				t.Fatal("tagged set unexpectedly contains a distinct source identifier")
			}
			if blue.HasCommonUUIDs(other) {
				t.Fatal("distinct tagged source identifiers unexpectedly overlap")
			}
			if got := blue.Subtract(other).String(); got != "uuid1:blue:1-5" {
				t.Fatalf("Subtract() = %q, want tagged transactions preserved", got)
			}
		})
	}
}

func TestParseGTIDSet_TaggedGTID_BackwardCompatible(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"uuid1:1-5", 5},
		{"uuid1:1-5,uuid2:1-3:7-9", 5 + 3 + 3},
		{"uuid1:42", 1},
		{"uuid1:tag:1-5", 5},
		{"uuid1:admin:42", 1},
		{"uuid1:data:1-3:7-9", 3 + 3},
	}
	for _, tt := range tests {
		gs, err := ParseGTIDSet(tt.input)
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		got := gs.TransactionCount()
		if got != tt.want {
			t.Errorf("TransactionCount(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestGTIDSet_Subtract(t *testing.T) {
	tests := []struct {
		name      string
		a, b      string
		want      string
		wantCount int64
	}{
		{
			name: "empty minus empty",
			a:    "", b: "",
			want: "", wantCount: 0,
		},
		{
			name: "something minus empty",
			a:    "uuid1:1-5", b: "",
			want: "uuid1:1-5", wantCount: 5,
		},
		{
			name: "empty minus something",
			a:    "", b: "uuid1:1-5",
			want: "", wantCount: 0,
		},
		{
			name: "identical sets",
			a:    "uuid1:1-10", b: "uuid1:1-10",
			want: "", wantCount: 0,
		},
		{
			name: "superset minus subset",
			a:    "uuid1:1-10", b: "uuid1:3-7",
			want: "uuid1:1-2:8-10", wantCount: 5,
		},
		{
			name: "subset minus superset",
			a:    "uuid1:3-7", b: "uuid1:1-10",
			want: "", wantCount: 0,
		},
		{
			name: "disjoint UUIDs",
			a:    "uuid1:1-5", b: "uuid2:1-5",
			want: "uuid1:1-5", wantCount: 5,
		},
		{
			name: "partial overlap from left",
			a:    "uuid1:1-10", b: "uuid1:1-5",
			want: "uuid1:6-10", wantCount: 5,
		},
		{
			name: "partial overlap from right",
			a:    "uuid1:1-10", b: "uuid1:6-15",
			want: "uuid1:1-5", wantCount: 5,
		},
		{
			name: "multi UUID mixed",
			a:    "uuid1:1-10,uuid2:1-5",
			b:    "uuid1:1-10",
			want: "uuid2:1-5", wantCount: 5,
		},
		{
			name: "multi interval subtraction",
			a:    "uuid1:1-20",
			b:    "uuid1:3-5:10-15",
			want: "uuid1:1-2:6-9:16-20", wantCount: 11,
		},
		{
			name: "single transaction divergence",
			a:    "uuid1:1-10",
			b:    "uuid1:1-9",
			want: "uuid1:10", wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := ParseGTIDSet(tt.a)
			if err != nil {
				t.Fatalf("parse a: %v", err)
			}
			b, err := ParseGTIDSet(tt.b)
			if err != nil {
				t.Fatalf("parse b: %v", err)
			}
			got := a.Subtract(b)
			if got.String() != tt.want {
				t.Errorf("Subtract() = %q, want %q", got.String(), tt.want)
			}
			if got.TransactionCount() != tt.wantCount {
				t.Errorf("TransactionCount() = %d, want %d", got.TransactionCount(), tt.wantCount)
			}
		})
	}
}

func TestGTIDSet_IsEmpty(t *testing.T) {
	empty, err := ParseGTIDSet("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !empty.IsEmpty() {
		t.Error("expected empty GTID set to be empty")
	}

	nonEmpty, err := ParseGTIDSet("uuid1:1-5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nonEmpty.IsEmpty() {
		t.Error("expected non-empty GTID set to not be empty")
	}
}

func TestGTIDSet_HasCommonUUIDs(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{name: "shared uuid", a: "uuid1:1-5", b: "uuid1:1-10", want: true},
		{name: "disjoint uuids", a: "uuid1:1-5", b: "uuid2:1-5", want: false},
		{name: "one shared among many", a: "uuid1:1-2,uuid2:1-3", b: "uuid2:5-9,uuid3:1-1", want: true},
		{name: "empty a", a: "", b: "uuid1:1-5", want: false},
		{name: "empty b", a: "uuid1:1-5", b: "", want: false},
		{name: "both empty", a: "", b: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := ParseGTIDSet(tt.a)
			if err != nil {
				t.Fatalf("parse a: %v", err)
			}
			b, err := ParseGTIDSet(tt.b)
			if err != nil {
				t.Fatalf("parse b: %v", err)
			}
			if got := a.HasCommonUUIDs(b); got != tt.want {
				t.Errorf("HasCommonUUIDs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseGTIDSet_MalformedIntervalNotTreatedAsTag(t *testing.T) {
	// These inputs must return errors, not be silently misparsed as tagged GTID entries.
	tests := []string{
		"uuid:5-3:7-9",   // start > end in first interval; must error, not skip as tag
		"uuid: 1-5:7-9",  // leading whitespace in first interval; must error, not skip as tag
		"uuid:1-2-3:7-9", // malformed interval (extra hyphen); must error, not skip as tag
	}
	for _, input := range tests {
		_, err := ParseGTIDSet(input)
		if err == nil {
			t.Errorf("expected error for %q, got nil", input)
		}
	}
}
