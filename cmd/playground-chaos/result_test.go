package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// TestWriteResultPrintsMeasurements pins that a scenario's measurements are
// printed under its PASS/FAIL line: a passing run's CI log shows them, and a
// failing run's shows them before the failure.
func TestWriteResultPrintsMeasurements(t *testing.T) {
	measured := []string{"failover under load iad->pdx: rto=8.2s", "acknowledged writes missing on pdx after the failover: 0 of 61 []"}
	var out bytes.Buffer
	writeResult(&out, runner.Result{ID: "01-clean-primary-kill", Title: "t", Passed: true, Duration: 41 * time.Second, Measurements: measured})
	want := "PASS  01-clean-primary-kill  (t)  duration=41s\n" +
		"       measured: failover under load iad->pdx: rto=8.2s\n" +
		"       measured: acknowledged writes missing on pdx after the failover: 0 of 61 []\n"
	if out.String() != want {
		t.Errorf("pass output:\n%s\nwant:\n%s", out.String(), want)
	}

	out.Reset()
	writeResult(&out, runner.Result{ID: "01-clean-primary-kill", Title: "t", Phase: runner.PhaseVerify, StepName: "measure",
		Failure: "over budget", CapturePath: "/tmp/x", CleanupErr: errors.New("cleanup broke"), Measurements: measured[:1]})
	got := out.String()
	for _, frag := range []string{"FAIL  01-clean-primary-kill", "measured: failover under load iad->pdx: rto=8.2s", `phase=verify step="measure"`, "over budget", "forensics: /tmp/x", "cleanup: cleanup broke"} {
		if !strings.Contains(got, frag) {
			t.Errorf("fail output lacks %q:\n%s", frag, got)
		}
	}
	if strings.Index(got, "measured:") > strings.Index(got, "phase=verify") {
		t.Errorf("measurements must precede the failure details:\n%s", got)
	}

	out.Reset()
	writeResult(&out, runner.Result{ID: "02-x", Title: "t", Passed: true})
	if strings.Contains(out.String(), "measured") {
		t.Errorf("a scenario without measurements prints none:\n%s", out.String())
	}
}
