package runner

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestCaptureMeasure pins that a measurement is kept in order, apart from
// the notes, and also lands in the notes (so a failure's scenario.log has
// it).
func TestCaptureMeasure(t *testing.T) {
	c := &Capture{}
	if got := c.Measurements(); len(got) != 0 {
		t.Fatalf("a new capture has measurements %v", got)
	}
	c.Note("a note")
	c.Measure("rto=8.2s")
	c.Measure("promotion FLUSH BINARY LOGS=12ms")
	got := c.Measurements()
	if want := []string{"rto=8.2s", "promotion FLUSH BINARY LOGS=12ms"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("measurements %v, want %v", got, want)
	}
	// The returned slice is a copy.
	got[0] = "changed"
	if c.Measurements()[0] != "rto=8.2s" {
		t.Error("Measurements must return a copy")
	}
	if want := []string{"a note", "measured: rto=8.2s", "measured: promotion FLUSH BINARY LOGS=12ms"}; !reflect.DeepEqual(c.notes, want) {
		t.Errorf("notes %v, want %v", c.notes, want)
	}
}

// TestWriteJUnitCarriesMeasurements pins where CI shows a scenario's
// measurements: the test case's system-out, for a passing scenario and a
// failing one, and no element at all for a scenario that recorded none.
func TestWriteJUnitCarriesMeasurements(t *testing.T) {
	results := []Result{
		{ID: "01-clean-primary-kill", Passed: true, Duration: 40 * time.Second,
			Measurements: []string{"failover under load iad->pdx: rto=8.2s", "acknowledged writes missing on pdx after the failover: 0 of 61 []"}},
		{ID: "02-planned-switchover", Passed: true, Duration: time.Second},
		{ID: "70-pitr-lossy-failover", Passed: false, Phase: PhaseVerify, StepName: "s", Failure: "boom", Duration: time.Second,
			Measurements: []string{"a <measurement> & its \"quotes\""}},
	}
	path := filepath.Join(t.TempDir(), "junit.xml")
	if err := WriteJUnit(path, results); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got JUnitTestSuite
	if err := xml.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if len(got.Cases) != 3 {
		t.Fatalf("cases=%d, want 3", len(got.Cases))
	}
	if want := "failover under load iad->pdx: rto=8.2s\nacknowledged writes missing on pdx after the failover: 0 of 61 []"; got.Cases[0].SystemOut != want {
		t.Errorf("passing case system-out %q, want %q", got.Cases[0].SystemOut, want)
	}
	if got.Cases[1].SystemOut != "" {
		t.Errorf("a case without measurements has system-out %q", got.Cases[1].SystemOut)
	}
	if got.Cases[2].Failure == nil || got.Cases[2].SystemOut != `a <measurement> & its "quotes"` {
		t.Errorf("failing case: failure=%v system-out %q", got.Cases[2].Failure, got.Cases[2].SystemOut)
	}
	if n := strings.Count(string(body), "<system-out>"); n != 2 {
		t.Errorf("%d system-out elements, want 2 (none for a case without measurements):\n%s", n, body)
	}
	// JUnit readers expect failure before system-out inside a testcase.
	if f, o := strings.Index(string(body), "<failure"), strings.LastIndex(string(body), "<system-out>"); f < 0 || o < f {
		t.Errorf("the failing case's system-out must follow its failure element:\n%s", body)
	}
}
