package controller

import (
	"context"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestInitRestoreReadyStale: the runner asks for a status write only when
// the Ready condition disagrees with the restore verdict updateCRStatus
// would derive, so a settled group is not rewritten on every sync.
func TestInitRestoreReadyStale(t *testing.T) {
	withRestore := func(phase v1alpha1.BackupPhase, ready *metav1.Condition) *v1alpha1.MysqlFailoverGroup {
		fg := newTestFG()
		fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{}
		fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: phase, JobName: "restore-lion-dc1", TargetSite: "dc1", Message: "load failed"}
		if ready != nil {
			fg.Status.Conditions = []metav1.Condition{*ready}
		}
		return fg
	}
	verdict := func(phase v1alpha1.BackupPhase) *metav1.Condition {
		reason, message := initRestoreReadyCondition(withRestore(phase, nil))
		return &metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: message}
	}
	topologyReady := &metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy", Message: "ok"}
	for _, tc := range []struct {
		name string
		fg   *v1alpha1.MysqlFailoverGroup
		want bool
	}{
		{"no Ready condition yet", withRestore(v1alpha1.BackupPhaseFailed, nil), false},
		{"Initializing keeps its Unknown", withRestore(v1alpha1.BackupPhaseFailed,
			&metav1.Condition{Type: "Ready", Status: metav1.ConditionUnknown, Reason: "Initializing"}), false},
		{"in progress matches", withRestore(v1alpha1.BackupPhaseRunning, verdict(v1alpha1.BackupPhaseRunning)), false},
		{"failed matches", withRestore(v1alpha1.BackupPhaseFailed, verdict(v1alpha1.BackupPhaseFailed)), false},
		{"running to failed", withRestore(v1alpha1.BackupPhaseFailed, verdict(v1alpha1.BackupPhaseRunning)), true},
		{"pending to running (message)", withRestore(v1alpha1.BackupPhaseRunning, verdict(v1alpha1.BackupPhasePending)), true},
		{"topology verdict while unfinished", withRestore(v1alpha1.BackupPhaseRunning, topologyReady), true},
		{"failed to succeeded", withRestore(v1alpha1.BackupPhaseSucceeded, verdict(v1alpha1.BackupPhaseFailed)), true},
		{"succeeded and settled", withRestore(v1alpha1.BackupPhaseSucceeded, topologyReady), false},
		{"initFromBackup removed after failure", func() *v1alpha1.MysqlFailoverGroup {
			fg := withRestore(v1alpha1.BackupPhaseFailed, verdict(v1alpha1.BackupPhaseFailed))
			fg.Spec.InitFromBackup = nil
			return fg
		}(), true},
		{"no restore at all", func() *v1alpha1.MysqlFailoverGroup {
			fg := newTestFG()
			fg.Status.Conditions = []metav1.Condition{*topologyReady}
			return fg
		}(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := initRestoreReadyStale(tc.fg); got != tc.want {
				t.Fatalf("initRestoreReadyStale = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestInitRestoreReady_FollowsRestorePhaseWithoutTopologyEvent: once the
// topology has settled, the restore reconcile changes only status.restore
// and the topology manager has no state change to report. The Ready
// condition must still follow the restore from in progress to failed, and
// from failed to succeeded, through the runner's sync and through the
// restore path's direct request.
func TestInitRestoreReady_FollowsRestorePhaseWithoutTopologyEvent(t *testing.T) {
	for _, path := range []string{"sync", "restore reconcile"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			runner, c, fg, _ := lifecycleRunner(t)
			nn := client.ObjectKeyFromObject(fg)

			// The group as the restore leaves it running: spec.initFromBackup
			// set and the load under way into the seed dc1.
			if err := c.Get(ctx, nn, fg); err != nil {
				t.Fatal(err)
			}
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{}
			if err := c.Update(ctx, fg); err != nil {
				t.Fatal(err)
			}
			setRestorePhase := func(phase v1alpha1.BackupPhase) {
				t.Helper()
				var cur v1alpha1.MysqlFailoverGroup
				if err := c.Get(ctx, nn, &cur); err != nil {
					t.Fatal(err)
				}
				cur.Status.Restore = &v1alpha1.RestoreStatus{Phase: phase, JobName: "restore-lion-dc1", TargetSite: "dc1", Message: "BackoffLimitExceeded"}
				if err := c.Status().Update(ctx, &cur); err != nil {
					t.Fatal(err)
				}
			}
			setRestorePhase(v1alpha1.BackupPhaseRunning)

			// A manager registered as live with the group's own config, so
			// sync takes its update-in-place path rather than restarting it.
			var cur v1alpha1.MysqlFailoverGroup
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

			ready := func() *metav1.Condition {
				t.Helper()
				var got v1alpha1.MysqlFailoverGroup
				if err := c.Get(ctx, nn, &got); err != nil {
					t.Fatal(err)
				}
				return apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
			}
			// observe is what each path does when status.restore moved.
			observe := func() {
				t.Helper()
				switch path {
				case "sync":
					if err := runner.sync(ctx); err != nil {
						t.Fatal(err)
					}
				default:
					var got v1alpha1.MysqlFailoverGroup
					if err := c.Get(ctx, nn, &got); err != nil {
						t.Fatal(err)
					}
					runner.RefreshInitRestoreReadiness(nn, &got)
				}
			}
			if err := runner.sync(ctx); err != nil {
				t.Fatal(err)
			}
			if registeredManager(runner, nn).tm != tm {
				t.Fatal("sync replaced the live manager; the test needs the update-in-place path")
			}
			for i := 0; i < 6; i++ {
				tm.Poll(ctx)
			}
			if r := ready(); r == nil || r.Reason != v1alpha1.ReasonInitFromBackupInProgress {
				t.Fatalf("settled Ready = %+v, want %s", r, v1alpha1.ReasonInitFromBackupInProgress)
			}

			// The load fails. The topology does not move, so polling alone
			// writes nothing.
			setRestorePhase(v1alpha1.BackupPhaseFailed)
			tm.Poll(ctx)
			if r := ready(); r.Reason != v1alpha1.ReasonInitFromBackupInProgress {
				t.Fatalf("Ready moved to %s without a refresh; the test no longer isolates the restore-only change", r.Reason)
			}
			observe()
			tm.Poll(ctx)
			if r := ready(); r.Status != metav1.ConditionFalse || r.Reason != v1alpha1.ReasonInitFromBackupFailed {
				t.Fatalf("after the restore failed Ready = %+v, want False/%s", r, v1alpha1.ReasonInitFromBackupFailed)
			}

			// Retried and succeeded: Ready returns to the topology's verdict.
			setRestorePhase(v1alpha1.BackupPhaseSucceeded)
			observe()
			tm.Poll(ctx)
			if r := ready(); r.Reason == v1alpha1.ReasonInitFromBackupFailed || r.Reason == v1alpha1.ReasonInitFromBackupInProgress {
				t.Fatalf("after the restore succeeded Ready = %+v, want the topology's verdict", r)
			}

			// Settled: no further refresh is requested.
			observe()
			tm.mu.RLock()
			pending := tm.statusRefreshRequested
			tm.mu.RUnlock()
			if pending {
				t.Fatal("a settled Ready condition still requests a status refresh")
			}
		})
	}
}
