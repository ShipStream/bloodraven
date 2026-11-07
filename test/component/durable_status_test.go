package component

import (
	"context"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A-89 (R11): the status fields marked Durable carry state the operator
// cannot reconstruct from the sites. These cases pin what a lost status
// does and does not cause. The PITR archive bindings' case is
// TestPITRBindingStatusWipeAndIntegrity.

func durableTestGroup(annotations map[string]string) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: "default", UID: "lion-uid", Annotations: annotations},
		Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{
			{Name: "dc1", ServerID: 101, Role: v1alpha1.SiteRolePrimaryCandidate},
			{Name: "dc2", ServerID: 102, Role: v1alpha1.SiteRolePrimaryCandidate},
		}},
	}
}

func noopRecorder() controller.FailoverStateRecorder {
	return recorderFunc(func(context.Context, controller.FailoverRecord) error { return nil })
}

// runningGroupSites are two sites that hold data: a writable primary and a
// replica with replication metadata.
func runningGroupSites() (*mockMySQL, *mockMySQL) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"}
	dc2 := &mockMySQL{
		readOnly: true, gtidExecuted: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-lion-dc1.default.svc.cluster.local"},
	}
	return dc1, dc2
}

// Status emptied on a running group whose sites hold data, with the
// out-of-band annotation copy of the failover record present: the
// operator rehydrates the record from the annotations, does not take the
// group for a fresh deploy (no bootstrap clone), and fences deployment
// leases while status names no active site.
func TestDurableStatus_LostStatusOnRunningGroup(t *testing.T) {
	dc1, dc2 := runningGroupSites()
	h := newBootstrapHarness(t, dc1, dc2)
	failedAt := h.clock.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	ann, err := controller.FailoverRecordAnnotations(controller.FailoverRecord{
		LastFailover: failedAt, LastFailoverTarget: "dc1", PromotionGtidExecuted: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-90",
	})
	if err != nil {
		t.Fatal(err)
	}
	fg := durableTestGroup(ann) // status is empty: lost
	if reason := controller.DeploymentUnstableReason(fg); reason == "" {
		t.Fatal("deployment leases are not fenced while status names no active site")
	}

	h.tm.RestoreFailoverStateForSim(fg, noopRecorder())
	rec, ok := h.tm.FailoverRecordForSim()
	if !ok || rec.LastFailoverTarget != "dc1" || !rec.LastFailover.Equal(failedAt) ||
		rec.PromotionGtidExecuted != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-90" {
		t.Fatalf("failover record not rehydrated from the annotations: %+v (ok=%t)", rec, ok)
	}

	h.pollN(6)
	time.Sleep(50 * time.Millisecond) // a wrongly started bootstrap runs in a goroutine
	for name, m := range map[string]*mockMySQL{"dc1": dc1, "dc2": dc2} {
		m.mu.Lock()
		cloned := m.cloneInstanceCalled
		m.mu.Unlock()
		if cloned {
			t.Errorf("%s was recloned: a lost status made a running group look like a fresh deploy", name)
		}
	}
	if phase := h.tm.BootstrapPhase(); phase != controller.BootstrapPhaseNone {
		t.Errorf("bootstrap phase %q, want none", phase)
	}
	if dc1.isReadOnly() || !dc2.isReadOnly() {
		t.Errorf("roles changed: dc1 read-only=%t dc2 read-only=%t", dc1.isReadOnly(), dc2.isReadOnly())
	}
}

// Status lost while the primary is down: the operator has not observed a
// unique writable primary since it started, so it must not promote the
// replica on the strength of the lost status.
func TestDurableStatus_LostStatusRefusesPromotionBeforeAPrimaryIsObserved(t *testing.T) {
	dc1, dc2 := runningGroupSites()
	dc1.setError(errDown)
	h := newBootstrapHarness(t, dc1, dc2)
	ann, err := controller.FailoverRecordAnnotations(controller.FailoverRecord{
		LastFailover: h.clock.Now().Add(-time.Hour).UTC().Truncate(time.Second), LastFailoverTarget: "dc1",
	})
	if err != nil {
		t.Fatal(err)
	}
	h.tm.RestoreFailoverStateForSim(durableTestGroup(ann), noopRecorder())
	h.pollN(10)
	if !dc2.isReadOnly() {
		t.Fatal("promoted dc2 without having observed a unique writable primary since the status was lost")
	}
	dc2.mu.Lock()
	cloned := dc2.cloneInstanceCalled
	dc2.mu.Unlock()
	if cloned {
		t.Fatal("dc2 was recloned with the primary down")
	}

	// The primary comes back: once a unique writable primary is observed
	// the wait ends, and a later loss of that primary fails over.
	dc1.setError(nil)
	h.pollN(4)
	if dc1.isReadOnly() || !dc2.isReadOnly() {
		t.Fatalf("roles after dc1 returned: dc1 read-only=%t dc2 read-only=%t", dc1.isReadOnly(), dc2.isReadOnly())
	}
	dc1.setError(errDown)
	h.pollN(10)
	if dc2.isReadOnly() {
		t.Fatal("no failover after a writable primary was observed and lost: the wait did not end")
	}

	// Control: the same outage with the status intact (it names the
	// active site) fails over, so the refusal above is the lost status's.
	dc1c, dc2c := runningGroupSites()
	dc1c.setError(errDown)
	hc := newBootstrapHarness(t, dc1c, dc2c)
	intact := durableTestGroup(ann)
	intact.Status.ActiveSite, intact.Status.TopologyGeneration = "dc1", 3
	intact.Status.SetLastFailover(&metav1.Time{Time: hc.clock.Now().Add(-time.Hour).UTC().Truncate(time.Second)}, "dc1", "")
	hc.tm.RestoreFailoverStateForSim(intact, noopRecorder())
	hc.pollN(10)
	if dc2c.isReadOnly() {
		t.Fatal("control: an intact status did not fail over either, so the refusal proves nothing")
	}
}

// A true fresh deploy: no status, no annotations and no replication
// metadata. Bootstrap proceeds as it always has.
func TestDurableStatus_FreshDeployBootstraps(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: false}
	h := newBootstrapHarness(t, dc1, dc2)
	fg := durableTestGroup(nil)
	h.tm.RestoreFailoverStateForSim(fg, noopRecorder())
	if _, ok := h.tm.FailoverRecordForSim(); ok {
		t.Fatal("a fresh deploy restored a failover record")
	}
	h.pollN(3)
	waitForPhase(t, h.tm, controller.BootstrapPhaseDone, 2*time.Second)
	dc2.mu.Lock()
	cloned := dc2.cloneInstanceCalled
	dc2.mu.Unlock()
	if !cloned {
		t.Fatal("a fresh deploy was not bootstrapped")
	}
}

// status.plannedFailoverHold and the AutomaticFailoverHeld condition lost
// together: the planned-failover-terminal marker, the hold's out-of-band
// copy, keeps automation held and deployment leases fenced until the hold
// is acknowledged. With every copy gone nothing holds: that is why the
// hold is Durable.
func TestDurableStatus_LostHoldKeptByTerminalMarker(t *testing.T) {
	marker := controller.TerminalMarker{
		RunID:   "6c1f3e8a-0d7b-4c41-9d25-6a1c2f6e9b10",
		Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
		Reason:  v1alpha1.PlannedFailoverReasonPromotionUnconfirmed,
	}
	fg := durableTestGroup(map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: marker.String()})
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "dc1", State: v1alpha1.SiteStateWritable, Replication: v1alpha1.SiteReplicationNotApplicable},
		{Name: "dc2", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy},
	}
	fg.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonHealthy}}

	hold := controller.PlannedHoldFrom(fg)
	if hold == nil || !hold.FromMarker || hold.RunID != marker.RunID {
		t.Fatalf("the terminal marker did not keep the hold: %+v", hold)
	}
	leases := controller.NewDeploymentLeaseManager(nil, nil, nil)
	if reason := leases.UnstableReason(fg); reason != v1alpha1.UnstableReasonAutomaticFailoverHeld {
		t.Fatalf("deployment leases are not fenced by a hold the marker keeps: unstable reason %q", reason)
	}

	delete(fg.Annotations, v1alpha1.PlannedFailoverTerminalAnnotation)
	if hold := controller.PlannedHoldFrom(fg); hold != nil {
		t.Fatalf("a hold survived losing every copy: %+v", hold)
	}
	if reason := leases.UnstableReason(fg); reason != "" {
		t.Fatalf("control: the group without any hold copy is unstable (%q), so the assertion above proves nothing", reason)
	}
}
