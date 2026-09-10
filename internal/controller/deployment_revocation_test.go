package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
	coordinationv1 "k8s.io/api/coordination/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func deploymentRevocationReconciler(m *DeploymentLeaseManager) *MysqlFailoverGroupReconciler {
	runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
	runner.deploymentLeases = m
	return &MysqlFailoverGroupReconciler{Client: m.client, APIReader: m.reader, Scheme: m.client.Scheme(), Recorder: record.NewFakeRecorder(100), Runner: runner}
}

func TestDeploymentLeaseRevocationReconcilesBeforeEarlyReturn(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	ctx := context.Background()
	migration := grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	hold := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 120)
	if err := m.Release(ctx, fg, "migration", "op1", "tenant", "tenant-ns", "deployer", migration.Token); err != nil {
		t.Fatal(err)
	}
	migration = grantDeploymentLease(t, m, fg, "migration", "op2", 120)
	hold2 := grantDeploymentLease(t, m, fg, "failover-hold", "op2", 120)
	fg.Annotations = map[string]string{RevokeDeploymentLeasesAnnotation: "request1", "unrelated": "keep"}
	if err := m.client.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	// Missing credential Secrets force an early reconcile return, but must not
	// prevent an administrator from releasing a deployment hold.
	r := deploymentRevocationReconciler(m)
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)})
	if err != nil || result.RequeueAfter == 0 {
		t.Fatalf("reconcile = %+v, %v", result, err)
	}
	fresh, err := m.fresh(ctx, fg)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := fresh.Annotations[RevokeDeploymentLeasesAnnotation]; pending || fresh.Annotations["unrelated"] != "keep" {
		t.Fatalf("annotations = %v", fresh.Annotations)
	}
	for _, lease := range []DeploymentLeaseResult{migration, hold, hold2} {
		_, err := m.Renew(ctx, fg, lease.Kind, lease.OperationID, "tenant", "tenant-ns", "deployer", lease.Token, 30)
		if e := requireDeploymentError(t, err, 409, "revoked"); e.Reason != "operator_revoked" {
			t.Fatal(e)
		}
	}
	if hold, err := m.Hold(ctx, fg); err != nil || hold != nil {
		t.Fatalf("hold = %+v, %v", hold, err)
	}
	grantDeploymentLease(t, m, fg, "migration", "op3", 30)
}

func TestDeploymentLeaseRevocationPartialFailureClosesAdmission(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	ctx := context.Background()
	migration := grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	hold := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 120)
	fg.Annotations = map[string]string{RevokeDeploymentLeasesAnnotation: "request1"}
	if err := m.client.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fail := true
	m.client = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if _, ok := obj.(*coordinationv1.Lease); ok && fail && obj.GetName() == deploymentLeaseName(fg.Name, "migration", "") {
			return errors.New("migration write unavailable")
		}
		return c.Update(ctx, obj, opts...)
	}})
	r := deploymentRevocationReconciler(m)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}); err == nil {
		t.Fatal("expected partial revocation failure")
	}
	fresh, err := m.fresh(ctx, fg)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Annotations[RevokeDeploymentLeasesAnnotation] != "request1" {
		t.Fatal("request lost on partial revocation")
	}
	_, err = m.Renew(ctx, fg, "failover-hold", "op1", "tenant", "tenant-ns", "deployer", hold.Token, 30)
	if e := requireDeploymentError(t, err, 409, "revoked"); e.Reason != "operator_revoked" {
		t.Fatal(e)
	}
	_, err = m.Renew(ctx, fg, "migration", "op1", "tenant", "tenant-ns", "deployer", migration.Token, 30)
	if err == nil {
		t.Fatal("renewed a lease whose revocation could not persist")
	}
	_, err = m.Grant(ctx, fg, "migration", "op2", "tenant", "tenant-ns", "deployer", 30)
	requireDeploymentError(t, err, 423, "unstable")
	_, err = m.BeginPlanned(ctx, fg, "OrderedUpdate")
	requireDeploymentError(t, err, 423, "unstable")
	fail = false
	// A heartbeat can durably revoke its own lease before the controller retry.
	_, err = m.Renew(ctx, fg, "migration", "op1", "tenant", "tenant-ns", "deployer", migration.Token, 30)
	if e := requireDeploymentError(t, err, 409, "revoked"); e.Reason != "operator_revoked" {
		t.Fatal(e)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}); err != nil {
		t.Fatal(err)
	}
	grantDeploymentLease(t, m, fg, "migration", "op2", 30)
}

func TestDeploymentLeaseRevocationCleanupRetryDoesNotRevokeNewLeases(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup rejected", true: "cleanup response lost"}[committed], func(t *testing.T) {
			m, fg, now := deploymentLeaseFixture(t)
			ctx := context.Background()
			grantDeploymentLease(t, m, fg, "migration", "op1", 120)
			fg.Annotations = map[string]string{RevokeDeploymentLeasesAnnotation: "request1"}
			if err := m.client.Update(ctx, fg); err != nil {
				t.Fatal(err)
			}
			stale := fg.DeepCopy()
			fail := true
			m.client = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if fail {
					if committed {
						if err := c.Patch(ctx, obj, patch, opts...); err != nil {
							return err
						}
					}
					return errors.New("cleanup response unavailable")
				}
				return c.Patch(ctx, obj, patch, opts...)
			}})
			r := deploymentRevocationReconciler(m)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}
			if _, err := r.Reconcile(ctx, req); err == nil {
				t.Fatal("expected cleanup error")
			}
			fail = false
			if !committed {
				_, err := m.Grant(ctx, fg, "migration", "op2", "tenant", "tenant-ns", "deployer", 30)
				requireDeploymentError(t, err, 423, "unstable")
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatal(err)
				}
			}
			newLease := grantDeploymentLease(t, m, fg, "migration", "op2", 30)
			// Restart with an informer still returning the old annotation. Only
			// the uncached marker may authorize another revocation pass.
			m = NewDeploymentLeaseManager(m.client, m.reader, nil)
			m.SetClock(func() time.Time { return *now })
			r = deploymentRevocationReconciler(m)
			r.Client = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if group, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
					*group = *stale.DeepCopy()
					return nil
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
			if _, err := m.Renew(ctx, fg, "migration", "op2", "tenant", "tenant-ns", "deployer", newLease.Token, 30); err != nil {
				t.Fatalf("completed request revoked a later grant: %v", err)
			}
		})
	}
}

func TestDeploymentLeaseRevocationDuringWrite(t *testing.T) {
	for _, renew := range []bool{false, true} {
		t.Run(map[bool]string{false: "grant", true: "renew"}[renew], func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			var token string
			if renew {
				token = grantDeploymentLease(t, m, fg, "migration", "op1", 30).Token
			}
			request := func(ctx context.Context, c client.Client) error {
				fresh, err := m.fresh(ctx, fg)
				if err != nil {
					return err
				}
				fresh.Annotations = map[string]string{RevokeDeploymentLeasesAnnotation: "request1"}
				return c.Update(ctx, fresh)
			}
			requested := false
			m.client = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if err := c.Create(ctx, obj, opts...); err != nil {
						return err
					}
					if !requested {
						requested = true
						return request(ctx, c)
					}
					return nil
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if err := c.Update(ctx, obj, opts...); err != nil {
						return err
					}
					if !requested {
						requested = true
						return request(ctx, c)
					}
					return nil
				},
			})
			var err error
			if renew {
				_, err = m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", token, 30)
			} else {
				_, err = m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
			}
			if e := requireDeploymentError(t, err, 409, "revoked"); e.Reason != "operator_revoked" {
				t.Fatal(e)
			}
		})
	}
}

func TestDeploymentLeaseLagAloneDoesNotBlockAdmission(t *testing.T) {
	for _, mode := range []string{"lag only", "lag and replication error", "earlier site error"} {
		t.Run(mode, func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			broken := mode != "lag only"
			lag := fg.Spec.EffectiveMaxLagSeconds() + 100
			repl := &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "iad", SecondsBehindSource: &lag}
			if broken {
				repl.LastError = "replication failure"
			}
			snap := TopologySnapshot{TopologyGeneration: 7, ActiveSite: "iad", Sites: []SiteSnapshot{
				{Name: "iad", State: state.StateWritable},
				{Name: "pdx", State: state.StateReadOnly, Replication: repl, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged},
			}}
			if mode == "earlier site error" {
				fg.Spec.Sites = append(fg.Spec.Sites, fg.Spec.Sites[1])
				fg.Spec.Sites[2].Name = "fra"
				if err := m.client.Update(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
				snap.Sites = append(snap.Sites, SiteSnapshot{Name: "fra", State: state.StateReadOnly, Replication: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "iad", SecondsBehindSource: &lag}, ReplicationHealthy: true, SourceConvergenceState: sourceConvergenceConverged})
			}
			runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
			if err := runner.updateCRStatus(context.Background(), client.ObjectKeyFromObject(fg), snap); err != nil {
				t.Fatal(err)
			}
			fresh, _, err := m.Snapshot(context.Background(), fg)
			if err != nil {
				t.Fatal(err)
			}
			degraded := apimeta.FindStatusCondition(fresh.Status.Conditions, "Degraded")
			if degraded == nil || degraded.Status != metav1.ConditionTrue || fresh.Status.Sites[1].SecondsBehindSource == nil || *fresh.Status.Sites[1].SecondsBehindSource != lag {
				t.Fatalf("lost degraded/lag reporting: %+v", fresh.Status)
			}
			if broken {
				if degraded.Reason == "ReplicationLagging" {
					t.Fatal("lag masked a non-lag degradation")
				}
				_, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
				requireDeploymentError(t, err, 423, "unstable")
			} else {
				if degraded.Reason != "ReplicationLagging" || DeploymentUnstableReason(fresh) != "" {
					t.Fatalf("lag-only stability = %q, degraded = %+v", DeploymentUnstableReason(fresh), degraded)
				}
				grantDeploymentLease(t, m, fg, "migration", "op1", 30)
				grantDeploymentLease(t, m, fg, "failover-hold", "op1", 30)
			}
		})
	}
}

func TestDeploymentLeaseRevocationCleanupPreservesChangedRequest(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	ctx := context.Background()
	grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	fg.Annotations = map[string]string{RevokeDeploymentLeasesAnnotation: "request1"}
	if err := m.client.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	replace := true
	m.client = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		if replace {
			replace = false
			fresh, err := m.fresh(ctx, fg)
			if err != nil {
				return err
			}
			fresh.Annotations[RevokeDeploymentLeasesAnnotation] = "request2"
			if err := c.Update(ctx, fresh); err != nil {
				return err
			}
		}
		return c.Patch(ctx, obj, patch, opts...)
	}})
	r := deploymentRevocationReconciler(m)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("expected cleanup conflict")
	}
	fresh, err := m.fresh(ctx, fg)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Annotations[RevokeDeploymentLeasesAnnotation] != "request2" {
		t.Fatal("cleanup removed a replacement request")
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	grantDeploymentLease(t, m, fg, "migration", "op2", 30)
}

func TestDeploymentLeaseCompletionRetryDoesNotRestoreUpdatePhase(t *testing.T) {
	for _, failedCompletion := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion persisted", true: "completion rejected"}[failedCompletion], func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			ctx := context.Background()
			fg.Spec.Sites[0].Name, fg.Spec.Sites[1].Name = "dc1", "dc2"
			if err := m.client.Update(ctx, fg); err != nil {
				t.Fatal(err)
			}
			repl := &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}
			tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true, replicaStatusVal: repl})
			tm.cfg.Name, tm.cfg.Namespace = fg.Name, fg.Namespace
			tm.topologyGeneration, tm.deploymentActiveSite = 7, "dc1"
			for i := 0; i < 8; i++ {
				tm.Poll(ctx)
			}
			tm.deploymentLeases = m
			tm.updater = NewUpdateController(tm.failover, testLogger())
			runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
			runner.deploymentLeases = m
			var last TopologySnapshot
			tm.StatusCallback = func(snap TopologySnapshot) {
				last = snap
				err := runner.updateCRStatus(ctx, client.ObjectKeyFromObject(fg), snap)
				if err != nil {
					t.Fatal(err)
				}
				tm.mu.Lock()
				tm.statusWriteFailed = tm.deploymentEpoch != snap.DeploymentEpoch
				tm.mu.Unlock()
			}
			tm.updater.setPhase(UpdatePhaseWaitReplica)
			if !tm.beginDeploymentTopologyChange() {
				t.Fatal("mutation window unavailable")
			}
			tm.Poll(ctx)
			if tm.statusRetrySnapshot == nil || tm.statusRetrySnapshot.UpdatePhase != string(UpdatePhaseWaitReplica) {
				t.Fatal("did not cache in-flight phase")
			}
			tm.updater.setPhase(UpdatePhaseNone)
			if failedCompletion {
				tm.MarkStatusWriteResult(errors.New("completion write rejected"))
			} else {
				tm.emitStatusSnapshot()
			}
			tm.finishDeploymentTopologyChange()
			tm.Poll(ctx)
			if last.UpdatePhase != "" || last.DeploymentEpoch != tm.deploymentEpoch {
				t.Fatalf("replayed stale completion: %+v", last)
			}
			if err := m.TopologyPersisted(ctx, fg, last.DeploymentEpoch); err != nil {
				t.Fatal(err)
			}
			fresh, err := m.fresh(ctx, fg)
			if err != nil {
				t.Fatal(err)
			}
			if fresh.Status.UpdatePhase != "" {
				t.Fatalf("durable phase = %q", fresh.Status.UpdatePhase)
			}
			grantDeploymentLease(t, m, fg, "migration", "op1", 30)
			tm.Poll(ctx)
			if tm.statusWriteFailed {
				t.Fatal("completion retry never cleared")
			}
		})
	}
}
