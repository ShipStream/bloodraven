package controller

import (
	"context"
	"log/slog"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// holdUltraEngine runs the production engine and store over a fake client
// holding fg, against the real topology facade of f.
func holdUltraEngine(t *testing.T, f *opsFixture, fg *v1alpha1.MysqlFailoverGroup) (*PlannedFailoverEngine, func() *v1alpha1.MysqlFailoverGroup, func(func(*v1alpha1.MysqlFailoverGroup))) {
	t.Helper()
	c := newPFFakeClient(&pfFaults{}, fg)
	e := NewPlannedFailoverEngine(newAPIPlannedFailoverStore(c, c, f.clk), f.topo, &pfFakeSink{}, f.clk, SimHooks{})
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	get := func() *v1alpha1.MysqlFailoverGroup {
		var cur v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), nn, &cur); err != nil {
			t.Fatal(err)
		}
		return &cur
	}
	edit := func(fn func(*v1alpha1.MysqlFailoverGroup)) {
		cur := get()
		fn(cur)
		if err := c.Status().Update(context.Background(), cur); err != nil {
			t.Fatal(err)
		}
	}
	return e, get, edit
}

func holdUltraStep(t *testing.T, e *PlannedFailoverEngine, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	if _, err := e.Step(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}); err != nil {
		t.Fatalf("Step: %v", err)
	}
}

func heldConditionOnly() []metav1.Condition {
	return []metav1.Condition{{Type: v1alpha1.ConditionAutomaticFailoverHeld, Status: metav1.ConditionTrue,
		Reason: v1alpha1.ReasonPlannedFailoverManualRecoveryRequired, Message: "set by hand", LastTransitionTime: metav1.Now()}}
}

// K001: a hold rebuilt from a True AutomaticFailoverHeld condition that
// names no run is released once the condition is removed (the documented
// remedy), and a running engine installs the same hold startManager would.
func TestUltraK001_ConditionOnlyHoldInstalledAndReleasedWithTheCondition(t *testing.T) {
	f := newOpsFixture(t, "")
	fg := newTestFG()
	fg.Status.Conditions = heldConditionOnly()
	e, _, edit := holdUltraEngine(t, f, fg)

	holdUltraStep(t, e, fg)
	if v, ok := f.reg.slot.View(); !ok || v.Mode != "Hold" || v.RunID != "" {
		t.Fatalf("slot after a pass over a condition-only hold = %+v, %v; want a run-less Hold", v, ok)
	}

	edit(func(cur *v1alpha1.MysqlFailoverGroup) {
		apimeta.RemoveStatusCondition(&cur.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld)
	})
	holdUltraStep(t, e, fg)
	if f.reg.slot.Held() {
		t.Fatal("the run-less hold outlived its condition: automation stays held with nothing to acknowledge")
	}
	notes := f.reg.slot.ReleaseNotes()
	if len(notes) == 0 || notes[len(notes)-1] != (ClaimReleaseNote{RunID: "", Why: ReleaseOrphanHold}) {
		t.Fatalf("release notes = %+v, want the last one {\"\", orphanHold}", notes)
	}
}

// K001 at registration: startManager's hydration of a condition-only hold
// is released by the next engine pass once the condition is gone.
func TestUltraK001_HydratedConditionOnlyHoldReleasedAfterRemoval(t *testing.T) {
	f := newOpsFixture(t, "")
	view := newTestFG()
	view.Status.Conditions = heldConditionOnly()
	WireRegisteredManager(f.tm, ManagerWiring{Slot: f.reg.slot, HoldFrom: PlannedHoldFrom(view)})
	if v, ok := f.reg.slot.View(); !ok || v.Mode != "Hold" {
		t.Fatalf("registration did not hydrate the condition-only hold: %+v", v)
	}
	fg := newTestFG() // the condition has since been removed by hand
	e, _, _ := holdUltraEngine(t, f, fg)
	holdUltraStep(t, e, fg)
	if f.reg.slot.Held() {
		t.Fatal("the hydrated condition-only hold was never released")
	}
}

// K002/K004 (engine half): a named Hold claim that no durable record backs
// (inherited by a recreated group, or hydrated from a read an
// acknowledgement then cleared) is released by the next pass; one that a
// record backs is kept.
func TestUltraK004_OrphanNamedHoldReleasedByThePass(t *testing.T) {
	f := newOpsFixture(t, "")
	f.reg.slot.hydrate(plannedClaim{mode: claimModeHold, runID: "run-9"})

	backed := newTestFG()
	setPlannedHold(&backed.Status, v1alpha1.PlannedFailoverHold{RunID: "run-9",
		Reason: v1alpha1.PlannedFailoverReasonForeignWritable, HoldTime: metav1.Now()}, 1)
	e, _, edit := holdUltraEngine(t, f, backed)
	holdUltraStep(t, e, backed)
	if v, ok := f.reg.slot.View(); !ok || v.Mode != "Hold" || v.RunID != "run-9" {
		t.Fatalf("a durably backed hold was not kept: %+v, %v", v, ok)
	}

	// The record goes (an acknowledgement already ran, or the object was
	// recreated without it); no ack annotation remains.
	edit(func(cur *v1alpha1.MysqlFailoverGroup) {
		cur.Status.PlannedFailoverHold = nil
		cur.Status.Conditions = nil
	})
	holdUltraStep(t, e, backed)
	if f.reg.slot.Held() {
		t.Fatal("an orphaned hold kept automation held")
	}
}

// K004: a registration whose uncached read predates the acknowledgement's
// release does not hydrate the released hold back; a release of another
// run does not suppress hydration (fail closed).
func TestUltraK004_StaleViewDoesNotRehydrateAcknowledgedHold(t *testing.T) {
	view := newTestFG()
	setPlannedHold(&view.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1",
		Reason: v1alpha1.PlannedFailoverReasonForeignWritable, HoldTime: metav1.Now()}, 1)
	nn := types.NamespacedName{Namespace: view.Namespace, Name: view.Name}

	for _, tc := range []struct {
		name     string
		released string
		wantHeld bool
	}{
		{name: "the hold's own ack ran after the read", released: "run-1", wantHeld: false},
		{name: "another run released after the read", released: "run-7", wantHeld: true},
		{name: "nothing released after the read", wantHeld: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &TopologyManagerRunner{logger: slog.Default(), managers: map[types.NamespacedName]*managedTopology{}}
			slot := r.Slot(nn)
			if tc.released != "" {
				mode := claimModeHold
				if tc.released != "run-1" {
					mode = claimModeRun
				}
				slot.hydrate(plannedClaim{mode: mode, runID: tc.released})
			}
			gen := slot.generation() // startManager stamps, then reads
			if tc.released != "" {
				why := ReleaseAck
				if tc.released != "run-1" {
					why = ReleaseTerminal
				}
				if !slot.release(tc.released, why) {
					t.Fatal("setup release failed")
				}
			}
			f := newOpsFixture(t, "")
			r.registerManagerWith(nn, view, &managedTopology{tm: f.tm, viewSlot: slot, viewSlotGen: gen})
			if got := slot.Held(); got != tc.wantHeld {
				t.Fatalf("slot held = %v, want %v", got, tc.wantHeld)
			}
		})
	}
}

// K002: a manager for a recreated CR (same name, another UID) gets an empty
// claim slot and neither the stashed nor the predecessor's failover record;
// a replacement of the same incarnation still gets both.
func TestUltraK002_RecreatedGroupInheritsNoSlotOrRecord(t *testing.T) {
	old := newOpsFixture(t, "")
	at := old.clk.Now().Add(-time.Minute)
	old.tm.recordFailover(context.Background(), at, "dc2", opsF)
	nn := old.nn
	r := &TopologyManagerRunner{logger: slog.Default(), managers: map[types.NamespacedName]*managedTopology{}}
	oldMT := &managedTopology{tm: old.tm, uid: "uid-1"}
	r.registerManagerWith(nn, nil, oldMT)
	oldSlot := r.Slot(nn)
	oldSlot.toHold("run-1") // a standing ManualRecoveryRequired hold

	// Same incarnation: the replacement shares the slot and gets the record.
	r.mu.Lock()
	r.retireManagerLocked(nn, oldMT)
	r.mu.Unlock()
	same := newOpsFixture(t, "")
	sameMT := &managedTopology{tm: same.tm, uid: "uid-1"}
	r.registerManagerWith(nn, nil, sameMT)
	if r.Slot(nn) != oldSlot || !oldSlot.Held() {
		t.Fatal("a same-incarnation replacement did not share the group's slot")
	}
	same.tm.mu.RLock()
	gotTarget := same.tm.lastFailoverTarget
	same.tm.mu.RUnlock()
	if gotTarget != "dc2" {
		t.Fatalf("same-incarnation replacement record target = %q, want dc2", gotTarget)
	}

	// The CR is deleted and recreated before any sync prunes the name.
	r.mu.Lock()
	r.retireManagerLocked(nn, sameMT)
	r.mu.Unlock()
	fresh := newOpsFixture(t, "")
	r.registerManagerWith(nn, nil, &managedTopology{tm: fresh.tm, uid: "uid-2"})
	if slot := r.Slot(nn); slot == oldSlot || slot.Held() {
		t.Fatal("the recreated group inherited the old incarnation's claim slot (and its hold)")
	}
	if !oldSlot.Held() {
		t.Fatal("the old incarnation's slot was mutated; its retiring managers rely on it")
	}
	if fresh.tm.plannedClaimHeld() {
		t.Fatal("the recreated group's manager is held")
	}
	// Let the retired managers' goroutines finish their carries.
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.RLock()
		n := len(r.retiring[nn])
		r.mu.RUnlock()
		if n == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	fresh.tm.mu.RLock()
	defer fresh.tm.mu.RUnlock()
	if !fresh.tm.lastFailover.IsZero() || fresh.tm.lastFailoverTarget != "" {
		t.Fatalf("the recreated group carried the old incarnation's failover record: %s %q",
			fresh.tm.lastFailover, fresh.tm.lastFailoverTarget)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if st, ok := r.retiredFailover[nn]; ok && st.uid != "uid-2" {
		t.Fatalf("the stash holds the old incarnation's record: %+v", st)
	}
}

// K085: an acknowledgement can delete a hold marker that only the lenient
// hold parse accepts.
func TestUltraK085_TerminalMarkerNamesLenientHoldMarker(t *testing.T) {
	lenient := `{"schemaVersion":1,"runID":"run-1","outcome":"ManualRecoveryRequired","writer":"v2"}`
	if !TerminalMarkerNames(lenient, "run-1") || TerminalMarkerNames(lenient, "run-2") {
		t.Fatal("TerminalMarkerNames does not match a lenient hold marker by run")
	}
	other := `{"schemaVersion":1,"runID":"run-1","outcome":"RolledBack","writer":"v2"}`
	if TerminalMarkerNames(other, "run-1") {
		t.Fatal("a lenient non-hold marker matched")
	}
	strict := TerminalMarker{RunID: "run-1", Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack}.String()
	if !TerminalMarkerNames(strict, "run-1") {
		t.Fatal("a strict marker did not match its run")
	}

	// End to end: a hold carried only by a lenient marker refuses a request
	// and holds the slot.
	f := newOpsFixture(t, "")
	fg := newTestFG()
	fg.Annotations = map[string]string{PlannedFailoverTerminalAnnotation: lenient}
	e, _, _ := holdUltraEngine(t, f, fg)
	holdUltraStep(t, e, fg)
	if v, ok := f.reg.slot.View(); !ok || v.Mode != "Hold" || v.RunID != "run-1" {
		t.Fatalf("a lenient hold marker did not hold automation: %+v, %v", v, ok)
	}
}

// K081: release notes are bounded.
func TestUltraK081_ReleaseNotesBounded(t *testing.T) {
	s := &PlannedClaimSlot{}
	for i := 0; i < 3*maxClaimReleaseNotes; i++ {
		id := "run-" + time.Duration(i).String()
		s.hydrate(plannedClaim{mode: claimModeRun, runID: id})
		if !s.release(id, ReleaseTerminal) {
			t.Fatal("release failed")
		}
	}
	notes := s.ReleaseNotes()
	if len(notes) != maxClaimReleaseNotes {
		t.Fatalf("len(notes) = %d, want %d", len(notes), maxClaimReleaseNotes)
	}
	if last := notes[len(notes)-1].RunID; last != "run-"+time.Duration(3*maxClaimReleaseNotes-1).String() {
		t.Fatalf("newest note = %q", last)
	}
}
