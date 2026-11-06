package controller

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// C14: δ is a fixed 5 s margin. Restore and verification pods are built
// entirely by the operator and nothing renders a clock-skew env into them,
// so the docs must not present BLOODRAVEN_PITR_CLOCK_SKEW_MARGIN as a knob.
func TestDocsDoNotOfferClockSkewKnob(t *testing.T) {
	const knob = "BLOODRAVEN_PITR_CLOCK_SKEW_MARGIN"
	root := filepath.Join("..", "..", "site", "content", "docs")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".md") {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), knob) {
			t.Errorf("%s documents %s, which no Job renders", path, knob)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// C15: status.pitr.archive.observedTime moves only when the observation
// changes; the CRD description (kubectl explain) must say so rather than
// suggest a freshness heartbeat.
func TestPITRArchiveObservedTimeDescription(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "shipstream.io_mysqlfailovergroups.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	crd := string(b)
	if strings.Contains(crd, "last derived this observation") || !strings.Contains(crd, "ObservedTime is when this observation last changed") {
		t.Fatal("observedTime's CRD description does not say it is the last-change time")
	}
}
