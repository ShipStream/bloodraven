package controller

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

func TestSetCondition_PreservesLastTransitionTime(t *testing.T) {
	oldTime := metav1.NewTime(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	newTime := metav1.NewTime(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC))

	conditions := []metav1.Condition{
		{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: oldTime,
			Reason:             "TopologyPolled",
			Message:            "At least one site is writable",
		},
	}

	// Same status -> LastTransitionTime should be preserved.
	setCondition(&conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: newTime,
		Reason:             "TopologyPolled",
		Message:            "At least one site is writable",
	})

	if !conditions[0].LastTransitionTime.Equal(&oldTime) {
		t.Errorf("expected LastTransitionTime to be preserved as %v, got %v",
			oldTime, conditions[0].LastTransitionTime)
	}

	// Different status -> LastTransitionTime should change.
	setCondition(&conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: newTime,
		Reason:             "TopologyPolled",
		Message:            "No site is writable",
	})

	if !conditions[0].LastTransitionTime.Equal(&newTime) {
		t.Errorf("expected LastTransitionTime to be updated to %v, got %v",
			newTime, conditions[0].LastTransitionTime)
	}
}

func TestSetCondition_AddsNewCondition(t *testing.T) {
	var conditions []metav1.Condition

	now := metav1.Now()
	setCondition(&conditions, metav1.Condition{
		Type:               "Degraded",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: now,
		Reason:             "Healthy",
		Message:            "No cross-site alerts",
	})

	if len(conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}
	if conditions[0].Type != "Degraded" {
		t.Errorf("expected type Degraded, got %s", conditions[0].Type)
	}
}

func TestStatusDeepEqual_IdenticalStatuses(t *testing.T) {
	now := metav1.Now()
	lastSeen := metav1.NewTime(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))

	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Sites: []v1alpha1.SiteStatus{
			{
				Name:     "dc1",
				State:    "writable",
				LastSeen: &lastSeen,
			},
			{
				Name:     "dc2",
				State:    "read-only",
				LastSeen: &lastSeen,
			},
		},
		Conditions: []metav1.Condition{
			{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "TopologyPolled",
				Message:            "At least one site is writable",
			},
		},
		LastFailoverTarget: "dc1",
	}

	existing := status.DeepCopy()

	if !equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected identical statuses to be equal")
	}
}

func TestStatusDeepEqual_DifferentActiveSite(t *testing.T) {
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", State: "writable"},
			{Name: "dc2", State: "read-only"},
		},
	}

	existing := status.DeepCopy()
	status.ActiveSite = "dc2"

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different ActiveSite to be unequal")
	}
}

func TestStatusDeepEqual_DifferentSiteState(t *testing.T) {
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", State: "writable"},
			{Name: "dc2", State: "read-only"},
		},
	}

	existing := status.DeepCopy()
	status.Sites[0].State = "unreachable"

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different Sites[0] state to be unequal")
	}
}

func TestStatusDeepEqual_DifferentConditionStatus(t *testing.T) {
	now := metav1.Now()
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
		Conditions: []metav1.Condition{
			{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "TopologyPolled",
			},
		},
	}

	existing := status.DeepCopy()
	status.Conditions[0].Status = metav1.ConditionFalse

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected different condition status to be unequal")
	}
}

func TestStatusDeepEqual_LastFailoverChange(t *testing.T) {
	status := v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "dc1",
	}

	existing := status.DeepCopy()
	failoverTime := metav1.NewTime(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))
	status.LastFailover = &failoverTime

	if equality.Semantic.DeepEqual(existing, &status) {
		t.Error("expected status with new LastFailover to be unequal")
	}
}

func TestUpdateCRStatus_IsNotFound_NoPanic(t *testing.T) {
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "gone-fg", Namespace: "default"}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateReadOnly},
		},
	}

	runner.updateCRStatus(context.Background(), nn, snap)
}

func TestUpdateCRStatus_ExistingCR(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateReadOnly},
		},
		ActiveSite: "dc1",
	}

	runner.updateCRStatus(context.Background(), nn, snap)

	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatalf("failed to get fg: %v", err)
	}
	if updated.Status.ActiveSite != "dc1" {
		t.Errorf("expected ActiveSite=dc1, got %q", updated.Status.ActiveSite)
	}
}

func TestUpdateCRStatus_DeletedMidUpdate(t *testing.T) {
	// CR exists when Get is called but is deleted before Status().Update.
	// Uses SubResourceUpdate interceptor to simulate mid-update deletion.
	fg := newTestFG()
	scheme := testScheme()

	deleteCalled := false
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, underlying client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if !deleteCalled {
					deleteCalled = true
					_ = underlying.Delete(ctx, fg)
				}
				return underlying.Status().Update(ctx, obj, opts...)
			},
		}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateReadOnly},
		},
	}

	// Must not panic even though CR is deleted mid-update.
	runner.updateCRStatus(context.Background(), nn, snap)

	if !deleteCalled {
		t.Error("expected interceptor to be called for SubResourceUpdate")
	}
}

func TestStopManager(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runner := &TopologyManagerRunner{
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "test-pair", Namespace: "default"}
	_, cancel := context.WithCancel(context.Background())
	runner.managers[nn] = &managedTopology{
		cancel: cancel,
	}

	if len(runner.managers) != 1 {
		t.Fatalf("expected 1 manager, got %d", len(runner.managers))
	}

	runner.StopManager(nn)

	if len(runner.managers) != 0 {
		t.Errorf("expected 0 managers after StopManager, got %d", len(runner.managers))
	}

	runner.StopManager(nn)
}

func TestStopManagedTopologyWaitTimesOut(t *testing.T) {
	done := make(chan struct{})
	start := time.Now()
	stopManagedTopologyWait(&managedTopology{done: done}, 20*time.Millisecond)
	elapsed := time.Since(start)
	if elapsed < 20*time.Millisecond {
		t.Fatalf("returned too fast: %v", elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("wait was not bounded: elapsed %v", elapsed)
	}
}

func TestStopManagedTopologyWaitReturnsOnDone(t *testing.T) {
	done := make(chan struct{})
	close(done)
	start := time.Now()
	stopManagedTopologyWait(&managedTopology{done: done}, time.Minute)
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("should return immediately when done is closed, elapsed %v", time.Since(start))
	}
}

func TestStopManagedTopologyHaltsMetrics(t *testing.T) {
	tm := &TopologyManager{}
	stopManagedTopologyWait(&managedTopology{tm: tm}, time.Millisecond)
	if !tm.siteMetricsHalted() {
		t.Fatal("expected HaltSiteMetrics to be set before the wait")
	}
}

func TestStopManager_NotFound(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runner := &TopologyManagerRunner{
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "nonexistent", Namespace: "default"}
	runner.StopManager(nn)
}

func TestSync_RemovesDeletedCRManagers(t *testing.T) {
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: "stale-pair", Namespace: "default"}
	_, cancel := context.WithCancel(context.Background())
	runner.managers[nn] = &managedTopology{
		cancel: cancel,
		cfg:    TopologyConfig{},
	}

	if err := runner.sync(context.Background()); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	if len(runner.managers) != 0 {
		t.Errorf("expected 0 managers after sync, got %d", len(runner.managers))
	}
}

func TestUpdateCRStatus_SetsConditions(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateReadOnly, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged},
		},
		ActiveSite: "dc1",
	}

	runner.updateCRStatus(context.Background(), nn, snap)

	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatalf("failed to get fg: %v", err)
	}

	foundReady := false
	for _, cond := range updated.Status.Conditions {
		if cond.Type == "Ready" {
			foundReady = true
			if cond.Status != metav1.ConditionTrue {
				t.Errorf("expected Ready=True, got %s", cond.Status)
			}
		}
	}
	if !foundReady {
		t.Error("Ready condition not found in status")
	}
}

func TestUpdateCRStatus_WritableNonPromotableIsDegraded(t *testing.T) {
	for _, role := range []state.SiteRole{state.SiteRoleReadOnly, state.SiteRoleDROnly} {
		t.Run(string(role), func(t *testing.T) {
			fg := newTestFG()
			fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: "anomaly", Role: v1alpha1.SiteRole(role)})
			scheme := testScheme()
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(fg).Build()
			runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
			nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
			runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
				Sites: []SiteSnapshot{
					{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
					{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged},
					{Name: "anomaly", Role: role, State: state.StateWritable, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged},
				},
				DegradedReason: "Degraded",
				Alert:          "writable non-promotable site requires fencing (anomaly)",
			})
			var updated v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), nn, &updated); err != nil {
				t.Fatal(err)
			}
			var ready, degraded *metav1.Condition
			for i := range updated.Status.Conditions {
				condition := &updated.Status.Conditions[i]
				switch condition.Type {
				case "Ready":
					ready = condition
				case "Degraded":
					degraded = condition
				}
			}
			if ready == nil || ready.Status != metav1.ConditionFalse {
				t.Fatalf("Ready condition = %+v, want False", ready)
			}
			if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != "Degraded" {
				t.Fatalf("Degraded condition = %+v, want True/Degraded", degraded)
			}
		})
	}
}

func TestUpdateCRStatus_ConvergencePendingWithNilReplicationIsDegraded(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}

	// A failed replica-status probe leaves Replication nil while convergence
	// is marked Pending/ProbeFailed. The nil guard must not suppress the
	// ReplicationSourceMismatch Degraded condition for the read-only
	// follower, and the writable primary must not be flagged.
	runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly,
				Replication: nil, SourceConvergenceState: sourceConvergencePending, SourceConvergenceReason: sourceReasonProbeFailed},
		},
		ActiveSite:     "dc1",
		DegradedReason: "Healthy",
	})

	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatal(err)
	}
	for _, condition := range updated.Status.Conditions {
		if condition.Type == "Degraded" && condition.Reason == "ReplicationSourceMismatch" {
			if condition.Status != metav1.ConditionTrue {
				t.Fatalf("ReplicationSourceMismatch condition = %+v, want True", condition)
			}
			return
		}
	}
	t.Fatal("ReplicationSourceMismatch Degraded condition not found for probe-failed follower")
}

func TestUpdateOnlyStatusCallbackPreservesNoPrimaryCondition(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: true}, &mockMySQL{readOnly: true})
	tm.sites[0].state = state.StateReadOnly
	tm.sites[1].state = state.StateReadOnly
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	tm.StatusCallback = func(snapshot TopologySnapshot) { runner.updateCRStatus(context.Background(), nn, snapshot) }

	tm.emitStatusSnapshot()
	tm.emitStatusSnapshot() // update-completion-style callback without a transition
	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatal(err)
	}
	for _, condition := range updated.Status.Conditions {
		if condition.Type == "Degraded" {
			if condition.Status != metav1.ConditionTrue || condition.Reason != "NoPrimary" {
				t.Fatalf("persistent topology degradation was cleared: %+v", condition)
			}
			return
		}
	}
	t.Fatal("Degraded condition not found")
}

func drainRunnerEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestEmitFailoverEvents_FailoverExecuted(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		LastFailoverTarget: "",
	}
	snap := TopologySnapshot{
		LastFailoverTarget: "dc2",
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "FailoverExecuted") {
		t.Errorf("want FailoverExecuted event, got %s", events[0])
	}
	if !strings.Contains(events[0], "dc2") {
		t.Errorf("want event to mention dc2, got %s", events[0])
	}
}

func TestEmitFailoverEvents_NoEventOnSameTarget(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		LastFailoverTarget: "dc2",
	}
	snap := TopologySnapshot{
		LastFailoverTarget: "dc2",
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events when target unchanged, got %v", events)
	}
}

func TestEmitFailoverEvents_DataLossDetected(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", RecoveryState: ""},
			{Name: "dc2", RecoveryState: ""},
		},
	}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", RecoveryState: "RecoveryBlocked", DivergentTxnCount: 5},
			{Name: "dc2"},
		},
		RecoveryState:     "RecoveryBlocked",
		RecoverySite:      "dc1",
		DivergentTxnCount: 5,
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "DataLossDetected") {
		t.Errorf("want DataLossDetected event, got %s", events[0])
	}
	if !strings.Contains(events[0], "5 divergent") {
		t.Errorf("want event to mention 5 divergent transactions, got %s", events[0])
	}
}

func TestEmitFailoverEvents_NoDataLossEventWhenAlreadyBlocked(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", RecoveryState: "RecoveryBlocked"},
			{Name: "dc2", RecoveryState: ""},
		},
	}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", RecoveryState: "RecoveryBlocked", DivergentTxnCount: 5},
			{Name: "dc2"},
		},
		RecoveryState:     "RecoveryBlocked",
		RecoverySite:      "dc1",
		DivergentTxnCount: 5,
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events when already blocked, got %v", events)
	}
}

// A blocked site can diverge FURTHER (it respawns writable, takes writes, and
// is re-fenced). The periodic re-verification rewrites the count, and the
// Event stream must follow it — otherwise an admin who extracted only the
// first-reported set silently loses the rest.
func TestEmitFailoverEvents_DataLossEventWhenDivergenceGrows(t *testing.T) {
	tests := []struct {
		name string
		// prevCount is the count already persisted in status; nil means a
		// status that never recorded one.
		prevCount *int64
		newCount  int64
		want      []string // substrings the single expected event must contain
	}{
		{name: "unchanged count is not re-reported", prevCount: ptr64(5), newCount: 5},
		{name: "grown count is re-reported", prevCount: ptr64(5), newCount: 9, want: []string{"DataLossDetected", "9 divergent"}},
		{name: "shrunk count is re-reported", prevCount: ptr64(9), newCount: 5, want: []string{"DataLossDetected", "5 divergent"}},
		{name: "absent prior count is treated as unchanged", prevCount: nil, newCount: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := record.NewFakeRecorder(10)
			runner := &TopologyManagerRunner{recorder: rec}
			existing := &v1alpha1.MysqlFailoverGroupStatus{
				Sites: []v1alpha1.SiteStatus{
					{Name: "dc1", RecoveryState: "RecoveryBlocked", DivergentTransactionCount: tt.prevCount},
				},
			}
			// The summary fields carry deliberately stale values: event
			// emission reads Sites[] only, so a regression back to the
			// summary would fail this test rather than pass it by accident.
			snap := TopologySnapshot{
				Sites:             []SiteSnapshot{{Name: "dc1", RecoveryState: "RecoveryBlocked", DivergentTxnCount: tt.newCount}},
				RecoveryState:     "",
				RecoverySite:      "stale-site",
				DivergentTxnCount: 999,
			}

			runner.emitFailoverEvents(newTestFG(), existing, snap)
			events := drainRunnerEvents(rec)

			if len(tt.want) == 0 {
				if len(events) != 0 {
					t.Errorf("want no events, got %v", events)
				}
				return
			}
			if len(events) != 1 {
				t.Fatalf("want exactly one event, got %v", events)
			}
			for _, sub := range tt.want {
				if !strings.Contains(events[0], sub) {
					t.Errorf("event %q missing %q", events[0], sub)
				}
			}
		})
	}
}

func TestEmitFailoverEvents_RecoveryComplete(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: rec}

	existing := &v1alpha1.MysqlFailoverGroupStatus{
		Sites: []v1alpha1.SiteStatus{
			{Name: "dc1", RecoveryState: "RecoveryBlocked"},
			{Name: "dc2", RecoveryState: ""},
		},
	}
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1"},
			{Name: "dc2"},
		},
	}

	runner.emitFailoverEvents(fg, existing, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "RecoveryComplete") {
		t.Errorf("want RecoveryComplete event, got %s", events[0])
	}
	if !strings.Contains(events[0], "dc1") {
		t.Errorf("want event to mention dc1, got %s", events[0])
	}
}

func newDegradedTestRunner(rec *record.FakeRecorder, fg *v1alpha1.MysqlFailoverGroup, lastReason string) (*TopologyManagerRunner, types.NamespacedName) {
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	runner := &TopologyManagerRunner{
		recorder: rec,
		managers: map[types.NamespacedName]*managedTopology{
			nn: {lastTopologyDegradedReason: lastReason},
		},
	}
	return runner, nn
}

func TestEmitDegradedTransitionEvents_SplitBrain(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "Healthy")

	snap := TopologySnapshot{
		Alert: "SPLIT BRAIN: both sites are writable",
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateWritable},
		},
	}

	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "SplitBrainDetected") {
		t.Errorf("want SplitBrainDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_NoPrimary(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "Healthy")

	snap := TopologySnapshot{
		Alert: "NO PRIMARY: both sites are read-only",
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateReadOnly},

			{Name: "dc2", State: state.StateReadOnly},
		},
	}

	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "NoPrimaryDetected") {
		t.Errorf("want NoPrimaryDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_TotalLoss(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "Healthy")

	snap := TopologySnapshot{
		Alert: "TOTAL LOSS: both sites are unreachable",
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateUnreachable},

			{Name: "dc2", State: state.StateUnreachable},
		},
	}

	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "TotalLossDetected") {
		t.Errorf("want TotalLossDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_SiteRecovered(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "SplitBrain")

	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateReadOnly},
		},
	}

	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "SiteRecovered") {
		t.Errorf("want SiteRecovered event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_NoEventOnSameReason(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "SplitBrain")

	snap := TopologySnapshot{
		Alert: "SPLIT BRAIN: both sites are writable",
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateWritable},
		},
	}

	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events when reason unchanged, got %v", events)
	}
}

func TestEmitDegradedTransitionEvents_TransitionBetweenAlerts(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "SplitBrain")

	snap := TopologySnapshot{
		Alert: "TOTAL LOSS: both sites are unreachable",
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateUnreachable},

			{Name: "dc2", State: state.StateUnreachable},
		},
	}

	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d: %v", len(events), events)
	}
	if !strings.Contains(events[0], "TotalLossDetected") {
		t.Errorf("want TotalLossDetected event, got %s", events[0])
	}
}

func TestEmitDegradedTransitionEvents_NoRecoveryEventOnFreshManager(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "")

	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateReadOnly},
		},
	}

	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)

	if len(events) != 0 {
		t.Errorf("want no events on fresh manager becoming healthy, got %v", events)
	}
}

func TestEmitDegradedTransitionEvents_ReplicationDoesNotCauseFalseRecovery(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "SplitBrain")

	// Topology recovers — this should emit SiteRecovered.
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateWritable},

			{Name: "dc2", State: state.StateReadOnly},
		},
	}
	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events := drainRunnerEvents(rec)
	if len(events) != 1 || !strings.Contains(events[0], "SiteRecovered") {
		t.Fatalf("want SiteRecovered, got %v", events)
	}

	// Topology stays healthy on next cycle — no event even if the persisted
	// Degraded condition was overwritten by ReplicationBroken.
	runner.emitDegradedTransitionEvents(fg, nn, snap)
	events = drainRunnerEvents(rec)
	if len(events) != 0 {
		t.Errorf("want no events on repeated healthy cycle, got %v", events)
	}
}

func TestEmitFailoverEvents_NilRecorder(t *testing.T) {
	fg := newTestFG()
	runner := &TopologyManagerRunner{recorder: nil}

	existing := &v1alpha1.MysqlFailoverGroupStatus{}
	snap := TopologySnapshot{LastFailoverTarget: "dc2"}

	// Must not panic.
	runner.emitFailoverEvents(fg, existing, snap)
}

func TestUpdateCRStatus_EmitsFailoverEvent(t *testing.T) {
	fg := newTestFG()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	rec := record.NewFakeRecorder(10)

	runner := &TopologyManagerRunner{
		client:   c,
		recorder: rec,
		logger:   logger,
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	failoverTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{

			{Name: "dc1", State: state.StateUnreachable},

			{Name: "dc2", State: state.StateWritable},
		},
		ActiveSite:         "dc2",
		LastFailover:       failoverTime,
		LastFailoverTarget: "dc2",
	}

	runner.updateCRStatus(context.Background(), nn, snap)
	events := drainRunnerEvents(rec)

	found := false
	for _, e := range events {
		if strings.Contains(e, "FailoverExecuted") {
			found = true
		}
	}
	if !found {
		t.Errorf("want FailoverExecuted event from updateCRStatus, got %v", events)
	}
}
