package controller

import (
	"context"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// topologyConditionTypes are the conditions updateCRStatus owns; each is
// stamped with the generation the topology manager applied (A-68).
var topologyConditionTypes = []string{
	v1alpha1.ConditionReady,
	v1alpha1.ConditionDegraded,
	v1alpha1.ConditionBootstrapping,
	v1alpha1.ConditionUpdating,
	v1alpha1.ConditionRecoveryPending,
}

// TestUpdateCRStatus_StampsAppliedGeneration pins A-68: the topology
// conditions and status.observedGeneration carry the generation the
// manager applied, not the fetched object's newer one, and
// status.observedGeneration never moves backwards.
func TestUpdateCRStatus_StampsAppliedGeneration(t *testing.T) {
	ctx := context.Background()
	runner, c, fg, _ := lifecycleRunner(t)
	nn := client.ObjectKeyFromObject(fg)
	var cur v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	cur.Generation = 7
	if err := c.Update(ctx, &cur); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	snap := TopologySnapshot{
		ActiveSite: "dc1",
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true, ReplicaStatusRead: true,
				Replication: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SecondsBehindSource: new(int64(0))}, SourceConvergenceState: sourceConvergenceConverged},
		},
		DegradedReason:    v1alpha1.ReasonHealthy,
		AppliedGeneration: cur.Generation - 2,
	}
	write := func(s TopologySnapshot) v1alpha1.MysqlFailoverGroup {
		t.Helper()
		if err := runner.updateCRStatus(ctx, nn, s); err != nil {
			t.Fatal(err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	got := write(snap)
	if got.Status.ObservedGeneration != snap.AppliedGeneration {
		t.Fatalf("status.observedGeneration = %d, want the applied %d (metadata.generation %d)", got.Status.ObservedGeneration, snap.AppliedGeneration, got.Generation)
	}
	for _, typ := range topologyConditionTypes {
		cond := apimeta.FindStatusCondition(got.Status.Conditions, typ)
		if cond == nil {
			t.Fatalf("no %s condition", typ)
		}
		if cond.ObservedGeneration != snap.AppliedGeneration {
			t.Errorf("%s.observedGeneration = %d, want the applied %d, not metadata.generation %d", typ, cond.ObservedGeneration, snap.AppliedGeneration, got.Generation)
		}
	}
	if ready := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady); ready.Status != metav1.ConditionTrue || ready.Reason != v1alpha1.ReasonHealthy {
		t.Fatalf("Ready = %+v, want True/Healthy", ready)
	}

	// An older snapshot (a retry built before the generation advanced)
	// does not move status.observedGeneration back.
	snap.AppliedGeneration = cur.Generation
	write(snap)
	snap.AppliedGeneration = cur.Generation - 1
	if got = write(snap); got.Status.ObservedGeneration != cur.Generation {
		t.Fatalf("status.observedGeneration moved back to %d, want %d", got.Status.ObservedGeneration, cur.Generation)
	}
}

// TestSync_EqualConfigEditAdvancesObservedGeneration pins F-28: an edit
// outside the topology configuration (a backup schedule) keeps the running
// manager, and the equal-config resync still advances
// status.observedGeneration and Ready.observedGeneration to
// metadata.generation without any topology change.
func TestSync_EqualConfigEditAdvancesObservedGeneration(t *testing.T) {
	ctx := context.Background()
	runner, c, fg, _ := lifecycleRunner(t)
	nn := client.ObjectKeyFromObject(fg)
	var cur v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	cur.Generation = 3
	if err := c.Update(ctx, &cur); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	cfg := mustTopologyConfig(&cur)
	hash, err := connectionMaterialHash(ctx, c, &cur)
	if err != nil {
		t.Fatal(err)
	}
	cfg.CredentialHash = hash
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	tm.StatusCallback = func(snap TopologySnapshot) {
		if err := runner.updateCRStatus(ctx, nn, snap); err != nil {
			t.Errorf("updateCRStatus: %v", err)
		}
	}
	runner.mu.Lock()
	runner.managers[nn] = &managedTopology{tm: tm, cfg: cfg}
	runner.mu.Unlock()

	observed := func() (int64, int64, int64) {
		t.Helper()
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		ready := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady)
		if ready == nil {
			t.Fatal("no Ready condition")
		}
		return got.Generation, got.Status.ObservedGeneration, ready.ObservedGeneration
	}
	settle := func() {
		t.Helper()
		if err := runner.sync(ctx); err != nil {
			t.Fatal(err)
		}
		if registeredManager(runner, nn).tm != tm {
			t.Fatal("sync replaced the live manager; the test needs the equal-config path")
		}
		for range 4 {
			tm.Poll(ctx)
		}
	}
	settle()
	if gen, obs, ready := observed(); obs != gen || ready != gen {
		t.Fatalf("settled: observedGeneration %d, Ready.observedGeneration %d, want metadata.generation %d", obs, ready, gen)
	}

	// A backup-schedule-only edit: the topology configuration is unchanged.
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	cur.Spec.Backup = &v1alpha1.BackupSpec{Schedules: []v1alpha1.BackupSchedule{{Name: "nightly", ProfileName: "s3", Schedule: "0 2 * * *"}}}
	cur.Generation++
	if err := c.Update(ctx, &cur); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	edited := mustTopologyConfig(&cur)
	edited.CredentialHash = hash
	if !edited.Equal(cfg) {
		t.Fatal("a backup schedule changed the topology configuration; the test needs an edit outside it")
	}

	// Polling alone does not claim the edit was applied.
	tm.Poll(ctx)
	if gen, obs, _ := observed(); obs >= gen {
		t.Fatalf("observedGeneration %d reached metadata.generation %d before the runner resynchronised", obs, gen)
	}

	settle()
	if gen, obs, ready := observed(); obs != gen || ready != gen {
		t.Fatalf("after the equal-config resync: observedGeneration %d, Ready.observedGeneration %d, want metadata.generation %d", obs, ready, gen)
	}
}

// K017: a callback whose status write conflicts with a newer one at the
// same topology generation (a later callback that published a newer
// applied generation first) must not move observedGeneration, or the
// topology conditions' observedGeneration, back on its retry.
func TestUpdateCRStatus_OlderCallbackRetryDoesNotMoveGenerationBack(t *testing.T) {
	ctx := context.Background()
	runner, c, fg, _ := lifecycleRunner(t)
	nn := client.ObjectKeyFromObject(fg)
	var cur v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &cur); err != nil {
		t.Fatal(err)
	}
	cur.Generation = 5
	if err := c.Update(ctx, &cur); err != nil {
		t.Fatal(err)
	}
	snap := TopologySnapshot{
		ActiveSite: "dc1",
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicationHealthy: true, ReplicaStatusRead: true,
				Replication: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SecondsBehindSource: new(int64(0))}, SourceConvergenceState: sourceConvergenceConverged},
		},
		DegradedReason:    v1alpha1.ReasonHealthy,
		AppliedGeneration: 4,
	}
	// The newer callback's write lands between the older one's read and
	// its first update attempt, which then conflicts.
	raced := false
	runner.client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if !raced {
				raced = true
				var newer v1alpha1.MysqlFailoverGroup
				if err := cl.Get(ctx, nn, &newer); err != nil {
					return err
				}
				newer.Status.ObservedGeneration = 5
				newer.Status.TopologyGeneration = snap.TopologyGeneration
				newer.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue,
					Reason: v1alpha1.ReasonHealthy, ObservedGeneration: 5, LastTransitionTime: metav1.Now()}}
				if err := cl.Status().Update(ctx, &newer); err != nil {
					return err
				}
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	if err := runner.updateCRStatus(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	if !raced {
		t.Fatal("the race was not staged")
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ObservedGeneration != 5 {
		t.Fatalf("status.observedGeneration = %d, want the newer write's 5", got.Status.ObservedGeneration)
	}
	if ready := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady); ready == nil || ready.ObservedGeneration != 5 {
		t.Fatalf("Ready = %+v, want the newer write's generation 5", ready)
	}
}
