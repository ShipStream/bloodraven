package runner

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestWriteJUnitRoundTrip writes a fixture run-all result set to a
// temp file and reads it back through encoding/xml. The runner's
// JUnit output is expected to be ingested by GitHub Actions and
// junit-reporter consumers, so we treat round-trip parsability as
// the contract — any field rename that breaks `xml.Unmarshal` should
// fail this test.
func TestWriteJUnitRoundTrip(t *testing.T) {
	results := []Result{
		{
			ID:        "01-clean-primary-kill",
			Title:     "Clean primary kill",
			Passed:    true,
			Duration:  12*time.Second + 150*time.Millisecond,
			StartTime: time.Unix(1714400000, 0),
		},
		{
			ID:          "02-planned-switchover",
			Title:       "Planned switchover",
			Passed:      false,
			Phase:       PhaseObserve,
			StepName:    "PlannedFailoverStatus reaches Succeeded",
			Failure:     "planned failover entered Failed: TargetUnhealthy",
			Duration:    1*time.Second + 55*time.Millisecond,
			StartTime:   time.Unix(1714400020, 0),
			CapturePath: "playground/chaos-results/20260429T193327Z/02-planned-switchover",
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "results.xml")

	if err := WriteJUnit(path, results); err != nil {
		t.Fatalf("WriteJUnit returned error: %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.HasPrefix(string(body), xml.Header) {
		t.Fatalf("output missing xml header; first 80 bytes: %q", body[:min(len(body), 80)])
	}

	var got JUnitTestSuite
	if err := xml.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v\n--- payload ---\n%s", err, body)
	}

	if got.Name != "playground-chaos" {
		t.Errorf("Name = %q, want playground-chaos", got.Name)
	}
	if got.Tests != 2 {
		t.Errorf("Tests = %d, want 2", got.Tests)
	}
	if got.Failures != 1 {
		t.Errorf("Failures = %d, want 1", got.Failures)
	}
	if got.Errors != 0 {
		t.Errorf("Errors = %d, want 0", got.Errors)
	}
	wantTime := results[0].Duration.Seconds() + results[1].Duration.Seconds()
	if got.Time != wantTime {
		t.Errorf("Time = %v, want %v", got.Time, wantTime)
	}
	if len(got.Cases) != 2 {
		t.Fatalf("Cases len = %d, want 2", len(got.Cases))
	}

	c1 := got.Cases[0]
	if c1.Name != "01-clean-primary-kill" {
		t.Errorf("Cases[0].Name = %q", c1.Name)
	}
	if c1.Failure != nil {
		t.Errorf("Cases[0] should have no Failure, got %+v", c1.Failure)
	}

	c2 := got.Cases[1]
	if c2.Name != "02-planned-switchover" {
		t.Errorf("Cases[1].Name = %q", c2.Name)
	}
	if c2.Failure == nil {
		t.Fatalf("Cases[1] should have a Failure block")
	}
	if !strings.Contains(c2.Failure.Type, string(PhaseObserve)) {
		t.Errorf("Failure.Type = %q, want it to include phase %q", c2.Failure.Type, PhaseObserve)
	}
	if !strings.Contains(c2.Failure.Type, "PlannedFailoverStatus reaches Succeeded") {
		t.Errorf("Failure.Type = %q, want step name", c2.Failure.Type)
	}
	if c2.Failure.Message != "planned failover entered Failed: TargetUnhealthy" {
		t.Errorf("Failure.Message = %q", c2.Failure.Message)
	}
	if !strings.Contains(c2.Failure.Body, "Forensics:") {
		t.Errorf("Failure.Body should reference Forensics path; got %q", c2.Failure.Body)
	}
	if !strings.Contains(c2.Failure.Body, "20260429T193327Z/02-planned-switchover") {
		t.Errorf("Failure.Body should include capture path; got %q", c2.Failure.Body)
	}
}

// TestWriteJUnitEmptyPathIsNoop documents the contract that callers
// can pass "" to disable JUnit output without an error and without
// creating any file.
func TestWriteJUnitEmptyPathIsNoop(t *testing.T) {
	dir := t.TempDir()
	if err := WriteJUnit("", []Result{{ID: "x", Passed: true}}); err != nil {
		t.Fatalf("WriteJUnit(\"\") = %v, want nil", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read tempdir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("WriteJUnit(\"\") created %d entries in tempdir; should create none", len(entries))
	}
}

// TestWriteJUnitAllPassed verifies the suite-level counts when every
// scenario passes (no failures element should be emitted on the
// testcases).
func TestWriteJUnitAllPassed(t *testing.T) {
	results := []Result{
		{ID: "01", Passed: true, Duration: time.Second},
		{ID: "02", Passed: true, Duration: 2 * time.Second},
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "ok.xml")
	if err := WriteJUnit(path, results); err != nil {
		t.Fatalf("WriteJUnit: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got JUnitTestSuite
	if err := xml.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Failures != 0 {
		t.Errorf("Failures = %d, want 0", got.Failures)
	}
	for i, c := range got.Cases {
		if c.Failure != nil {
			t.Errorf("Cases[%d] (%s) should have nil Failure, got %+v", i, c.Name, c.Failure)
		}
	}
}

