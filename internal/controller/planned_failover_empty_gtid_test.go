package controller

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// plannedDrainRunner wires a real TopologyManager over the iad/pdx sites of
// plannedFailoverFG into a TopologyManagerRunner, so the Draining phase runs
// the production FenceSite and KillSiteAppConnections paths.
func plannedDrainRunner(nn types.NamespacedName, iad, pdx *mockMySQL) *TopologyManagerRunner {
	cfg := TopologyConfig{
		Name: "lion",
		Sites: []SiteTopologyConfig{
			{Name: "iad", Zone: "lion-iad", LBIP: "1.1.1.1", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("iad"), Host: "mysql-iad"},
			{Name: "pdx", Zone: "lion-pdx", LBIP: "2.2.2.2", Role: state.SiteRolePrimaryCandidate, TaintSelector: taintSelector("pdx"), Host: "mysql-pdx"},
		},
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
	tm := NewTopologyManager(cfg, []mysql.Checker{iad, pdx},
		NewFailoverController(testLogger()), nil, nil, BootstrapConfig{},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	runner := &TopologyManagerRunner{
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		managers: make(map[types.NamespacedName]*managedTopology),
	}
	runner.SetManagerForTest(nn, tm)
	return runner
}

func superReadOnlyCallCount(m *mockMySQL) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.superReadOnlyCalls
}

// A source with an empty gtid_executed (no transaction ever binlogged, or
// after RESET BINARY LOGS AND GTIDS) is a valid fence snapshot. Draining must
// fence once, keep its drain start time, and advance; an empty
// SourceGtidAtFence must not be read as "not fenced yet".
func TestPlannedFailoverDraining_EmptySourceGTIDFencesOnceAndAdvances(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseDraining,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
		DrainTimeout:  &metav1.Duration{Duration: 30 * time.Second},
		MaxLagWait:    &metav1.Duration{Duration: 30 * time.Second},
	}
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: false, gtidExecuted: ""}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: ""}

	r, _ := newReconciler(fg)
	r.Runner = plannedDrainRunner(nn, iad, pdx)

	// Pass 1: fence the source and record the (empty) snapshot.
	if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseDraining || pf.DrainStartTime == nil {
		t.Fatalf("pass 1: want Draining with DrainStartTime stamped, got %+v", pf)
	}
	if pf.SourceGtidAtFence != "" {
		t.Fatalf("pass 1: SourceGtidAtFence = %q, want the empty snapshot", pf.SourceGtidAtFence)
	}
	if got := superReadOnlyCallCount(iad); got != 1 {
		t.Fatalf("pass 1: SetSuperReadOnly calls on iad = %d, want 1", got)
	}
	drainStart := pf.DrainStartTime.DeepCopy()

	// Pass 2, through a fresh reconciler as after an operator restart: no
	// connections remain, so the drain finishes without re-fencing.
	r2 := &MysqlFailoverGroupReconciler{Client: r.Client, Scheme: r.Scheme, Recorder: r.Recorder, Runner: r.Runner}
	if _, err := r2.reconcilePlannedFailover(context.Background(), fetchFG(t, r2, nn)); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	pf = fetchFG(t, r2, nn).Status.PlannedFailover
	if got := superReadOnlyCallCount(iad); got != 1 {
		t.Errorf("pass 2: source re-fenced: SetSuperReadOnly calls on iad = %d, want 1", got)
	}
	if pf.DrainStartTime == nil || !pf.DrainStartTime.Equal(drainStart) {
		t.Errorf("pass 2: DrainStartTime changed from %v to %v", drainStart, pf.DrainStartTime)
	}
	if pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForLag {
		t.Fatalf("pass 2: phase = %q (%s), want WaitingForLag", pf.Phase, pf.Message)
	}

	// Pass 3: an empty target set contains the empty source snapshot.
	if _, err := r2.reconcilePlannedFailover(context.Background(), fetchFG(t, r2, nn)); err != nil {
		t.Fatalf("pass 3: %v", err)
	}
	pf = fetchFG(t, r2, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Fatalf("pass 3: phase = %q (%s), want Promoting", pf.Phase, pf.Message)
	}
}

// With the drain budget already spent, a persisted empty-snapshot Draining
// status must proceed despite remaining connections instead of re-fencing
// and restarting the budget.
func TestPlannedFailoverDraining_EmptySourceGTIDHonoursDrainBudget(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	drainStart := metav1.NewTime(time.Now().Add(-45 * time.Second).Truncate(time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:          v1alpha1.PlannedFailoverPhaseDraining,
		Target:         "pdx",
		SourcePrimary:  "iad",
		StartTime:      &start,
		DrainStartTime: &drainStart,
		DrainTimeout:   &metav1.Duration{Duration: 10 * time.Second},
	}
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: "", killConnectionResults: []int{3}}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: ""}

	r, _ := newReconciler(fg)
	r.Runner = plannedDrainRunner(nn, iad, pdx)

	if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if got := superReadOnlyCallCount(iad); got != 0 {
		t.Errorf("source re-fenced: SetSuperReadOnly calls on iad = %d, want 0", got)
	}
	if pf.DrainStartTime == nil || !pf.DrainStartTime.Equal(&drainStart) {
		t.Errorf("DrainStartTime reset from %v to %v", drainStart, pf.DrainStartTime)
	}
	if pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForLag || !strings.Contains(pf.Message, "drain budget exhausted") {
		t.Fatalf("phase = %q (%s), want WaitingForLag after the drain budget", pf.Phase, pf.Message)
	}
}
