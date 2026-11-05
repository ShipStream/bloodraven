package backupmeta

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMicroTimeJSON(t *testing.T) {
	ts := time.Date(2026, 11, 4, 20, 1, 2, 123456789, time.FixedZone("x", -3600))
	m := NewMicroTime(ts)
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"2026-11-04T21:01:02.123456Z"` {
		t.Fatalf("marshal = %s", b)
	}
	whole, _ := json.Marshal(NewMicroTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
	if string(whole) != `"2026-01-02T03:04:05.000000Z"` {
		t.Fatalf("whole-second marshal = %s (want six fixed digits)", whole)
	}
	if b, _ := json.Marshal(MicroTime{}); string(b) != "null" {
		t.Fatalf("zero marshal = %s", b)
	}
	var back MicroTime
	if err := json.Unmarshal([]byte(`"2026-11-04T22:01:02.123456+01:00"`), &back); err != nil || !back.Equal(m.Time) || back.Location() != time.UTC {
		t.Fatalf("unmarshal = %v, %v", back, err)
	}
	if err := json.Unmarshal([]byte(`null`), &back); err != nil || !back.IsZero() {
		t.Fatalf("null unmarshal = %v, %v", back, err)
	}
	for _, bad := range []string{`"2026-11-04T21:01:02.1234567Z"`, `"yesterday"`, `12`, `"0001-01-01T00:00:00Z"`} {
		if err := json.Unmarshal([]byte(bad), &back); err == nil {
			t.Errorf("unmarshal %s accepted", bad)
		}
	}
}

func testStamp() Stamp {
	return Stamp{
		Kind:          StampKind,
		Version:       StampVersion,
		Backup:        ObjectRef{Namespace: "db", Name: "nightly-1", UID: "uid-backup"},
		Source:        ObjectRef{Namespace: "db", Name: "orders", UID: "uid-group"},
		PITRArchiveID: "ar1-abcdefghijklmnopqrstuvwxyz",
		Site:          "b",
		ServerID:      102,
		ServerClock:   ServerClock{Start: NewMicroTime(time.Date(2026, 11, 4, 20, 0, 0, 5000, time.UTC))},
		CreatedAt:     NewMicroTime(time.Date(2026, 11, 4, 19, 59, 59, 0, time.UTC)),
	}
}

func TestStampCanonicalGolden(t *testing.T) {
	b, err := testStamp().Canonical()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"kind":"BloodravenBackupStamp","version":1,` +
		`"backup":{"namespace":"db","name":"nightly-1","uid":"uid-backup"},` +
		`"source":{"namespace":"db","name":"orders","uid":"uid-group"},` +
		`"pitrArchiveID":"ar1-abcdefghijklmnopqrstuvwxyz","site":"b","serverID":102,` +
		`"dumpFilters":{"includeSchemas":[],"excludeSchemas":[]},` +
		`"serverClock":{"start":"2026-11-04T20:00:00.000005Z","end":null},` +
		`"createdAt":"2026-11-04T19:59:59.000000Z"}`
	if string(b) != want {
		t.Fatalf("canonical stamp\n got %s\nwant %s", b, want)
	}
	back, err := ParseStamp(b)
	if err != nil {
		t.Fatal(err)
	}
	b2, _ := back.Canonical()
	if string(b2) != string(b) {
		t.Fatalf("round trip changed bytes:\n%s\n%s", b, b2)
	}
	if back.Complete() || !back.DumpFilters.Empty() {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestStampValidate(t *testing.T) {
	tests := map[string]func(*Stamp){
		"kind":        func(s *Stamp) { s.Kind = "x" },
		"version":     func(s *Stamp) { s.Version = 2 },
		"backup uid":  func(s *Stamp) { s.Backup.UID = "" },
		"source name": func(s *Stamp) { s.Source.Name = "" },
		"archive id":  func(s *Stamp) { s.PITRArchiveID = "ar1-x" },
		"site":        func(s *Stamp) { s.Site = "" },
		"server id":   func(s *Stamp) { s.ServerID = 0 },
		"start":       func(s *Stamp) { s.ServerClock.Start = MicroTime{} },
		"end<start":   func(s *Stamp) { s.ServerClock.End = NewMicroTime(s.ServerClock.Start.Add(-time.Second)) },
		"createdAt":   func(s *Stamp) { s.CreatedAt = MicroTime{} },
	}
	for name, mut := range tests {
		s := testStamp()
		mut(&s)
		if err := s.Validate(); !errors.Is(err, ErrStampInvalid) {
			t.Errorf("%s: Validate = %v, want ErrStampInvalid", name, err)
		}
	}
	ok := testStamp()
	ok.PITRArchiveID = "" // PITR disabled on the source
	if err := ok.Validate(); err != nil {
		t.Fatalf("stamp without archive ID: %v", err)
	}
	if _, err := ParseStamp([]byte(`{"kind":"BloodravenBackupStamp","extra":1}`)); !errors.Is(err, ErrStampInvalid) {
		t.Fatalf("unknown field accepted: %v", err)
	}
}

func TestStampFilters(t *testing.T) {
	s := testStamp()
	s.DumpFilters.ExcludeSchemas = []string{"tmp"}
	if s.DumpFilters.Empty() {
		t.Fatal("exclude filter reported empty")
	}
	b, _ := s.Canonical()
	if !strings.Contains(string(b), `"dumpFilters":{"includeSchemas":[],"excludeSchemas":["tmp"]}`) {
		t.Fatalf("filters = %s", b)
	}
}

func TestCheckStampEndRewrite(t *testing.T) {
	prev := testStamp()
	end := NewMicroTime(prev.ServerClock.Start.Add(90 * time.Second))
	next := prev
	next.ServerClock.End = end
	if err := CheckStampEndRewrite(prev, next); err != nil {
		t.Fatalf("valid rewrite: %v", err)
	}
	// An identical retry after the rewrite landed is accepted.
	if err := CheckStampEndRewrite(next, next); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	// nil vs empty filters are the same stamp.
	nilFilters := next
	nilFilters.DumpFilters = DumpFilters{IncludeSchemas: []string{}, ExcludeSchemas: nil}
	if err := CheckStampEndRewrite(prev, nilFilters); err != nil {
		t.Fatalf("nil/empty filters: %v", err)
	}
	refuse := map[string]Stamp{
		"no end": prev,
	}
	other := next
	other.ServerClock.End = NewMicroTime(end.Add(time.Second))
	refuse["end changed"] = other
	for name, mut := range map[string]func(*Stamp){
		"site":       func(s *Stamp) { s.Site = "a" },
		"server id":  func(s *Stamp) { s.ServerID = 101 },
		"start":      func(s *Stamp) { s.ServerClock.Start = NewMicroTime(s.ServerClock.Start.Add(time.Microsecond)) },
		"archive id": func(s *Stamp) { s.PITRArchiveID = "" },
		"filters":    func(s *Stamp) { s.DumpFilters.IncludeSchemas = []string{"a"} },
		"createdAt":  func(s *Stamp) { s.CreatedAt = NewMicroTime(s.CreatedAt.Add(time.Second)) },
	} {
		c := next
		mut(&c)
		refuse[name] = c
	}
	for name, n := range refuse {
		base := prev
		if name == "end changed" {
			base = next
		}
		if err := CheckStampEndRewrite(base, n); !errors.Is(err, ErrStampRewriteConflict) {
			t.Errorf("%s: CheckStampEndRewrite = %v, want ErrStampRewriteConflict", name, err)
		}
	}
}

func TestStampKey(t *testing.T) {
	for in, want := range map[string]string{
		"backups/nightly-1":   "backups/nightly-1.bloodraven-backup.json",
		"backups/nightly-1/":  "backups/nightly-1.bloodraven-backup.json",
		"/mnt/pvc/nightly-1/": "/mnt/pvc/nightly-1.bloodraven-backup.json",
	} {
		if got := StampKey(in); got != want {
			t.Errorf("StampKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDumpMetaJSON(t *testing.T) {
	s := testStamp()
	s.ServerClock.End = NewMicroTime(s.ServerClock.Start.Add(time.Minute))
	m := DumpMeta{
		Version: DumpMetaVersion, Location: "backups/nightly-1", GTIDExecuted: "u:1-10",
		End: NewMicroTime(s.ServerClock.End.Time), ServerClock: s.ServerClock, Stamp: s,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"version":1`, `"location":`, `"gtidExecuted":"u:1-10"`, `"gtidExecutedInconsistent":false`, `"end":"2026-11-04T20:01:00.000005Z"`, `"serverClock":{"start":`, `"stamp":{"kind":"BloodravenBackupStamp"`} {
		if !strings.Contains(string(b), key) {
			t.Errorf("dump.json lacks %s: %s", key, b)
		}
	}
	var back DumpMeta
	if err := json.Unmarshal(b, &back); err != nil || !back.ServerClock.End.Equal(m.ServerClock.End.Time) || back.Stamp.ServerID != 102 {
		t.Fatalf("round trip = %+v, %v", back, err)
	}
}
