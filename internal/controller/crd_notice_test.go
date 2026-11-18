package controller

import (
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"
)

// The newer-CRD notice is recorded once per group for each operator start,
// and not at all when the CRDs are the build's own.
func TestNoteNewerCRDs(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	r := &MysqlFailoverGroupReconciler{Recorder: rec}
	fg := newTestFG()
	r.noteNewerCRDs(fg)
	if events := drainRunnerEvents(rec); len(events) != 0 {
		t.Fatalf("no newer CRD, but %v", events)
	}

	r.NewerCRDs = "mysqlfailovergroups.shipstream.io: the installed CustomResourceDefinition is newer than this operator: spec.futureField is not known to this build"
	other := newTestFG()
	other.Name = "tiger"
	r.noteNewerCRDs(fg)
	r.noteNewerCRDs(fg)
	r.noteNewerCRDs(other)
	events := drainRunnerEvents(rec)
	if len(events) != 2 {
		t.Fatalf("events = %v, want one per group", events)
	}
	for _, e := range events {
		if !strings.Contains(e, "Warning NewerCRDDetected") || !strings.Contains(e, "spec.futureField") {
			t.Fatalf("event %q does not name the reason and the difference", e)
		}
	}
}
