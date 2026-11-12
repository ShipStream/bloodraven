package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// pfSyncFixture is a run in the sync wait with a long maxSyncWait and the
// source's position already captured by this process.
func pfSyncFixture(t *testing.T, policy string, source, target DragonflyConnection) (*MysqlFailoverGroupReconciler, types.NamespacedName) {
	t.Helper()
	fg := plannedFailoverFGWithDragonfly(policy)
	fg.Status.PlannedFailover.Settings.Dragonfly.MaxSyncWait = metav1.Duration{Duration: time.Hour}
	r, _, _ := pfDFReconciler(t, fg)
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFIad: source, pfDFPdx: target})
	nn := fgNN(fg)
	pfDFPass(t, r, nn)
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Dragonfly.SourceOffsetState != v1alpha1.DragonflyOffsetCaptured {
		t.Fatalf("offset capture = %+v", pf.Dragonfly)
	}
	return r, nn
}

// A target that refuses the position command, or reports its position in
// another form than the source, cannot be compared however long the run
// waits. onSyncTimeout is applied at once; MySQL is not held fenced for
// maxSyncWait (an hour here, 30s by default).
func TestPlannedFailoverDragonflySync_UnprovableTargetDoesNotWait(t *testing.T) {
	linked := dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up"}
	refusals := []struct {
		name   string
		target func() *fakeDragonflyConn
	}{
		{"ACL without DEBUG", func() *fakeDragonflyConn {
			return &fakeDragonflyConn{info: linked,
				positionErr: fmt.Errorf("target: %w", &dragonfly.ServerError{Message: "NOPERM ops has no ACL permissions"})}
		}},
		{"no such subcommand", func() *fakeDragonflyConn {
			return &fakeDragonflyConn{info: linked,
				positionErr: &dragonfly.ServerError{Message: "ERR Unknown subcommand or wrong number of arguments for 'REPLICA'. Try DEBUG HELP."}}
		}},
		{"command renamed away", func() *fakeDragonflyConn {
			return &fakeDragonflyConn{info: linked, positionErr: &dragonfly.ServerError{Message: "ERR unknown command `DEBUG`"}}
		}},
		{"a single offset against a per-flow source", func() *fakeDragonflyConn {
			c := syncProofReplica("a", 52)
			c.position.Scalar = true
			return c
		}},
	}
	for _, tc := range refusals {
		t.Run(tc.name+"/Proceed", func(t *testing.T) {
			r, nn := pfSyncFixture(t, "Proceed", syncProofMaster("a", 24, 28), tc.target())
			pfDFPass(t, r, nn)
			pf := fetchFG(t, r, nn).Status.PlannedFailover
			if pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly || pf.Dragonfly.Sessions != v1alpha1.DragonflySessionsLost ||
				pf.Dragonfly.Reason != v1alpha1.DragonflyPlannedReasonSyncTimeout || !strings.Contains(pf.Dragonfly.Message, "cannot be proven") {
				t.Fatalf("phase = %s dragonfly = %+v, want the takeover at once with sessions Lost and the reason", pf.Phase, pf.Dragonfly)
			}
			if pf.Dragonfly.SyncWaitSeconds == nil || *pf.Dragonfly.SyncWaitSeconds > 1 {
				t.Fatalf("syncWaitSeconds = %v, want no wait", pf.Dragonfly.SyncWaitSeconds)
			}
		})
		t.Run(tc.name+"/Fail", func(t *testing.T) {
			r, nn := pfSyncFixture(t, "Fail", syncProofMaster("a", 24, 28), tc.target())
			pfDFPass(t, r, nn)
			if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack ||
				pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonDragonflySyncTimeout {
				t.Fatalf("phase = %s rollback = %+v, want RollingBack at once", pf.Phase, pf.Rollback)
			}
		})
	}
	// What may pass is waited for, as before.
	waits := []struct {
		name   string
		target func() *fakeDragonflyConn
	}{
		{"transport error", func() *fakeDragonflyConn {
			return &fakeDragonflyConn{info: linked, positionErr: errors.New("i/o timeout")}
		}},
		{"role changed between the commands", func() *fakeDragonflyConn {
			return &fakeDragonflyConn{info: linked, positionErr: &dragonfly.ServerError{Message: "ERR I am master"}}
		}},
		{"not yet in stable sync", func() *fakeDragonflyConn {
			c := syncProofReplica("a", 1, 1)
			c.info.MasterSyncInProgress = true
			return c
		}},
		{"behind", func() *fakeDragonflyConn { return syncProofReplica("a", 1, 1) }},
	}
	for _, tc := range waits {
		t.Run(tc.name+" waits", func(t *testing.T) {
			r, nn := pfSyncFixture(t, "Fail", syncProofMaster("a", 24, 28), tc.target())
			for i := 0; i < 3; i++ {
				pfDFPass(t, r, nn)
			}
			if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync {
				t.Fatalf("phase = %s, want the wait to continue", pf.Phase)
			}
		})
	}
}

// pfCountingSource counts the position reads of a source.
type pfCountingSource struct {
	*fakeDragonflyConn
	reads int
}

func (c *pfCountingSource) ReplicationPosition(ctx context.Context) (dragonfly.Position, error) {
	c.reads++
	return c.fakeDragonflyConn.ReplicationPosition(ctx)
}

// A source whose process was replaced during the wait has a new
// replication id; a target that synced from the new instance follows that
// id and can never reach a position read from the old one. The source is
// read again, once.
func TestPlannedFailoverDragonflySync_SourceRestartRereadsThePositionOnce(t *testing.T) {
	source := &pfCountingSource{fakeDragonflyConn: syncProofMaster("a", 24, 28)}
	target := syncProofReplica("a", 1, 1)
	r, nn := pfSyncFixture(t, "Fail", source, target)
	if source.reads != 1 {
		t.Fatalf("source read %d times at the capture, want once", source.reads)
	}

	// The source restarted (id b) and the target synced from it.
	source.info.MasterReplID, source.position = "b", &dragonfly.Position{Role: "master", Flows: []int64{3, 4}, ReplID: "b"}
	target.info.MasterReplID, target.position = "b", &dragonfly.Position{Role: "slave", Flows: []int64{3, 4}, ReplID: "b"}
	pfDFPass(t, r, nn)
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly ||
		pf.Dragonfly.Sessions == v1alpha1.DragonflySessionsLost {
		t.Fatalf("phase = %s dragonfly = %+v, want PromotingDragonfly: the target reached the restarted source", pf.Phase, pf.Dragonfly)
	}
	if source.reads != 2 {
		t.Fatalf("source read %d times, want one more read after the replication id changed", source.reads)
	}
}

// The re-read happens once per run: a target that keeps following
// something else does not keep the wait reading the source, and the
// wait's deadline does not move.
func TestPlannedFailoverDragonflySync_SourceRereadIsBounded(t *testing.T) {
	source := &pfCountingSource{fakeDragonflyConn: syncProofMaster("a", 24, 28)}
	target := syncProofReplica("z", 24, 28)
	r, nn := pfSyncFixture(t, "Fail", source, target)
	for i := 0; i < 5; i++ {
		pfDFPass(t, r, nn)
	}
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync {
		t.Fatalf("phase = %s, want the wait to continue to its deadline", pf.Phase)
	}
	if source.reads != 2 {
		t.Fatalf("source read %d times over 5 passes, want 2: the capture and one re-read", source.reads)
	}
	r.plannedClock.(*clock.FakeClock).Advance(2 * time.Hour)
	pfDFPass(t, r, nn)
	if pf := fetchFG(t, r, nn).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack {
		t.Fatalf("phase = %s, want RollingBack at maxSyncWait", pf.Phase)
	}
}

// pfTakeoverFixture is a run in PromotingDragonfly with the takeover
// Intended by this process.
func pfTakeoverFixture(t *testing.T, policy string, source, target DragonflyConnection) (*MysqlFailoverGroupReconciler, client.Client, types.NamespacedName) {
	t.Helper()
	fg := plannedFailoverFGWithDragonfly(policy)
	now := metav1.Now()
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly.SourceOffsetState = v1alpha1.DragonflyOffsetCaptured
	fg.Status.PlannedFailover.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntentTime: now}
	r, c, _ := pfDFReconciler(t, fg, makeDragonflyPod(fg.Name, "iad", "master", true), makeDragonflyPod(fg.Name, "pdx", "replica", true))
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFIad: source, pfDFPdx: target})
	return r, c, fgNN(fg)
}

// takeover.targetOffset is the promoted target's position as a master. A
// target that still answers as a replica after a successful REPLTAKEOVER
// reply gives no such position, and none is recorded.
func TestPlannedFailoverDragonfly_TargetOffsetOnlyFromAMaster(t *testing.T) {
	t.Run("answers as a master", func(t *testing.T) {
		target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
			Role: "slave", MasterLinkStatus: "up", MasterReplOffset: 77, SlaveReplOffset: 70}}}
		r, _, nn := pfTakeoverFixture(t, "Proceed", &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}, target)
		pfDFPass(t, r, nn)
		tk := fetchFG(t, r, nn).Status.PlannedFailover.Dragonfly.Takeover
		if tk.State != v1alpha1.DragonflyTakeoverSucceeded || tk.TargetOffset == nil || *tk.TargetOffset != 77 {
			t.Fatalf("takeover = %+v, want Succeeded with targetOffset 77", tk)
		}
	})
	t.Run("still answers as a replica", func(t *testing.T) {
		// REPLTAKEOVER succeeds; the fake keeps reporting role slave.
		target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 70}}
		r, _, nn := pfTakeoverFixture(t, "Proceed", &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}, target)
		pfDFPass(t, r, nn)
		tk := fetchFG(t, r, nn).Status.PlannedFailover.Dragonfly.Takeover
		if tk.State != v1alpha1.DragonflyTakeoverSucceeded || tk.TargetOffset != nil {
			t.Fatalf("takeover = %+v, want Succeeded without a targetOffset: a replica's position is not the promoted master's", tk)
		}
	})
}

// A REPLTAKEOVER that ends with "Operation canceled" has an unknown
// outcome, like a lost reply: on v1.38 the source can still complete it
// and exit. The run records it unconfirmed, with what the two sites report,
// and does not claim that no role changed. A refusal before the source was
// asked is a plain failure.
func TestPlannedFailoverDragonfly_CanceledTakeoverIsUnconfirmed(t *testing.T) {
	for _, tc := range []struct {
		name            string
		err             error
		wantUnconfirmed bool
	}{
		{"operation canceled", &dragonfly.ServerError{Message: "ERR Couldn't execute takeover: Operation canceled"}, true},
		{"reply lost", errors.New("read tcp: i/o timeout"), true},
		{"full sync not done", &dragonfly.ServerError{Message: "ERR Full sync not done"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{
				info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "down"}, replTakeoverErr: tc.err}}
			source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
			r, c, nn := pfTakeoverFixture(t, "Proceed", source, target)
			pfDFPass(t, r, nn)
			pf := fetchFG(t, r, nn).Status.PlannedFailover
			if pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed {
				t.Fatalf("takeover = %+v, want Failed", pf.Dragonfly.Takeover)
			}
			msg := pf.Dragonfly.Message
			if got := strings.Contains(msg, "unconfirmed"); got != tc.wantUnconfirmed {
				t.Fatalf("message %q: unconfirmed=%v, want %v", msg, got, tc.wantUnconfirmed)
			}
			if strings.Contains(msg, "no Dragonfly role was changed") {
				t.Fatalf("message %q claims that no role changed", msg)
			}
			if tc.wantUnconfirmed && !strings.Contains(msg, "target=slave, source=master") {
				t.Fatalf("message %q does not say what the sites report after the attempt", msg)
			}
			// The source keeps its traffic label either way.
			var pod corev1.Pod
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: nn.Namespace, Name: makeDragonflyPod(nn.Name, "iad", "", false).Name}, &pod); err != nil {
				t.Fatal(err)
			}
			if pod.Labels[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled || pod.Labels[v1alpha1.DragonflyRoleLabel] != "master" {
				t.Fatalf("source labels = %v, want master with traffic", pod.Labels)
			}
		})
	}
}
