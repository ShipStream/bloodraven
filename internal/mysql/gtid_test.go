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
