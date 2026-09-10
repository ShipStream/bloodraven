package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func deploymentLeaseFixture(t *testing.T) (*DeploymentLeaseManager, *v1alpha1.MysqlFailoverGroup, *time.Time) {
	t.Helper()
	fg := plannedFailoverFG("")
	fg.UID = "lease-group-uid"
	fg.Status.TopologyGeneration = 7
	fg.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy", LastTransitionTime: metav1.Now()}}
	scheme := testScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fg).WithObjects(fg).Build()
	m := NewDeploymentLeaseManager(c, c, record.NewFakeRecorder(100))
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	m.SetClock(func() time.Time { return now })
	return m, fg, &now
}

func grantDeploymentLease(t *testing.T, m *DeploymentLeaseManager, fg *v1alpha1.MysqlFailoverGroup, kind, operation string, ttl int) DeploymentLeaseResult {
	t.Helper()
	r, err := m.Grant(context.Background(), fg, kind, operation, "tenant", "tenant-ns", "deployer", ttl)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func requireDeploymentError(t *testing.T, err error, status int, code string) *DeploymentLeaseError {
	t.Helper()
	var e *DeploymentLeaseError
	if !errors.As(err, &e) || e.Status != status || e.Code != code {
		t.Fatalf("error = %v, want %d %s", err, status, code)
	}
	return e
}

func TestDeploymentLeaseGrantRotationAndHashOnly(t *testing.T) {
	m, fg, now := deploymentLeaseFixture(t)
	first := grantDeploymentLease(t, m, fg, "migration", "op1", 1)
	if first.Status != 201 || !first.ExpiresAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("grant = %+v", first)
	}
	second := grantDeploymentLease(t, m, fg, "migration", "op1", 999)
	if second.Status != 200 || second.Token == first.Token || len(second.Token) != 64 || !second.ExpiresAt.Equal(now.Add(120*time.Second)) {
		t.Fatalf("regrant = %+v", second)
	}
	_, err := m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", first.Token, 30)
	requireDeploymentError(t, err, 403, "token_mismatch")
	var leases coordinationv1.LeaseList
	if err := m.reader.List(context.Background(), &leases); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(leases)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), first.Token) || strings.Contains(string(b), second.Token) || !strings.Contains(string(b), leaseTokenHash(second.Token)) {
		t.Fatal("durable Lease must contain hashes, never bearer tokens")
	}
	views, err := m.List(context.Background(), fg)
	if err != nil || len(views) != 1 {
		t.Fatalf("list = %+v, %v", views, err)
	}
	_, err = m.Grant(context.Background(), fg, "migration", "op2", "other", "tenant-ns", "deployer", 30)
	e := requireDeploymentError(t, err, 409, "held")
	if e.Holder == nil || e.Holder.OperationID != "op1" {
		t.Fatal("missing holder")
	}
}

func TestDeploymentLeaseExpiryBoundaryAndRestart(t *testing.T) {
	m, fg, now := deploymentLeaseFixture(t)
	r := grantDeploymentLease(t, m, fg, "migration", "op1", 5)
	m = NewDeploymentLeaseManager(m.client, m.reader, nil)
	m.SetClock(func() time.Time { return *now })
	*now = r.ExpiresAt
	views, err := m.List(context.Background(), fg)
	if err != nil || len(views) != 1 {
		t.Fatalf("lease expired at equality: %v %v", views, err)
	}
	renewed, err := m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", r.Token, 5)
	if err != nil {
		t.Fatal(err)
	}
	*now = renewed.ExpiresAt.Add(time.Nanosecond)
	before := testutil.ToFloat64(metrics.DeployLeaseExpirationsTotal.WithLabelValues(fg.Name, "migration"))
	_, err = m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", r.Token, 5)
	requireDeploymentError(t, err, 404, "not_found")
	_, err = m.List(context.Background(), fg)
	if err != nil {
		t.Fatal(err)
	}
	if delta := testutil.ToFloat64(metrics.DeployLeaseExpirationsTotal.WithLabelValues(fg.Name, "migration")) - before; delta != 1 {
		t.Fatalf("expiration count = %v", delta)
	}
	if err := m.Release(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", r.Token); err != nil {
		t.Fatal(err)
	}
}

func TestDeploymentLeaseHoldIdentity(t *testing.T) {
	for _, mutate := range []string{"missing", "operation", "instance", "namespace", "serviceAccount"} {
		t.Run(mutate, func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			if mutate != "missing" {
				grantDeploymentLease(t, m, fg, "migration", "op1", 30)
			}
			op, instance, ns, sa := "op1", "tenant", "tenant-ns", "deployer"
			switch mutate {
			case "operation":
				op = "op2"
			case "instance":
				instance = "other"
			case "namespace":
				ns = "other"
			case "serviceAccount":
				sa = "other"
			}
			_, err := m.Grant(context.Background(), fg, "failover-hold", op, instance, ns, sa, 30)
			requireDeploymentError(t, err, 403, "forbidden")
		})
	}
}

func TestDeploymentLeaseRevokedTombstoneSurvivesReplacementAndRelease(t *testing.T) {
	for _, reason := range []string{"topology_changed", "operator_revoked"} {
		t.Run(reason, func(t *testing.T) {
			m, fg, now := deploymentLeaseFixture(t)
			old := grantDeploymentLease(t, m, fg, "migration", "old-operation", 120)
			grantDeploymentLease(t, m, fg, "failover-hold", "old-operation", 120)
			before := testutil.ToFloat64(metrics.DeployLeaseRevocationsTotal.WithLabelValues(fg.Name, "migration", reason))
			if reason == "topology_changed" {
				fg.Status.TopologyGeneration++
				if err := m.client.Status().Update(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			} else if err := m.Revoke(context.Background(), fg, reason); err != nil {
				t.Fatal(err)
			}
			_, err := m.Renew(context.Background(), fg, "migration", "old-operation", "tenant", "tenant-ns", "deployer", old.Token, 30)
			if e := requireDeploymentError(t, err, 409, "revoked"); e.Reason != reason {
				t.Fatal(e)
			}
			if err := m.Release(context.Background(), fg, "migration", "old-operation", "tenant", "tenant-ns", "deployer", old.Token); err != nil {
				t.Fatal(err)
			}
			grantDeploymentLease(t, m, fg, "migration", "new-operation", 30)
			m = NewDeploymentLeaseManager(m.client, m.reader, nil)
			m.SetClock(func() time.Time { return *now })
			*now = now.Add(24 * time.Hour)
			_, err = m.Renew(context.Background(), fg, "migration", "old-operation", "tenant", "tenant-ns", "deployer", old.Token, 30)
			if e := requireDeploymentError(t, err, 409, "revoked"); e.Reason != reason {
				t.Fatal(e)
			}
			if delta := testutil.ToFloat64(metrics.DeployLeaseRevocationsTotal.WithLabelValues(fg.Name, "migration", reason)) - before; delta != 1 {
				t.Fatalf("revocation count = %v", delta)
			}
		})
	}
}

func TestDeploymentLeaseAdmissionRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		m, fg, _ := deploymentLeaseFixture(t)
		grantDeploymentLease(t, m, fg, "migration", "op1", 30)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var grantErr, beginErr error
		var hold *DeploymentLeaseView
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, grantErr = m.Grant(context.Background(), fg, "failover-hold", "op1", "tenant", "tenant-ns", "deployer", 30)
		}()
		go func() {
			defer wg.Done()
			<-start
			hold, beginErr = m.BeginPlanned(context.Background(), fg, "OrderedUpdate")
		}()
		close(start)
		wg.Wait()
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if grantErr == nil && hold == nil {
			t.Fatal("hold and planned operation both admitted")
		}
		if grantErr != nil {
			requireDeploymentError(t, grantErr, 423, "unstable")
		}
	}
}

func TestDeploymentLeaseEmergencyNeverWaitsForAdmission(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	grantDeploymentLease(t, m, fg, "failover-hold", "op1", 120)
	old := &mockMySQL{readOnly: true}
	target := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(old, target)
	tm.cfg.Name, tm.cfg.Namespace = fg.Name, fg.Namespace
	tm.deploymentLeases = m
	tm.topologyGeneration = fg.Status.TopologyGeneration
	tm.deploymentActiveSite = "dc1"
	tm.sites[0].state, tm.sites[1].state = state.StateUnreachable, state.StateReadOnly
	// Simulate an arbitrarily slow lease API-server call holding admission.
	m.mu.Lock()
	done := make(chan struct{})
	go func() {
		tm.applyCrossSiteAction(context.Background(), state.CrossSiteAction{PromotionCandidates: []string{"dc2"}})
		close(done)
	}()
	select {
	case <-done:
		m.mu.Unlock()
	case <-time.After(2 * time.Second):
		m.mu.Unlock()
		t.Fatal("emergency promotion waited for deployment admission")
	}
	if target.readOnly || tm.topologyGeneration != fg.Status.TopologyGeneration+1 {
		t.Fatalf("emergency not promoted: ro=%v generation=%d", target.readOnly, tm.topologyGeneration)
	}
	_, err := m.Grant(context.Background(), fg, "migration", "op2", "tenant", "tenant-ns", "deployer", 30)
	requireDeploymentError(t, err, 423, "unstable")
}

func TestDeploymentLeaseTopologyPersistenceFailureClosesAPI(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	r := grantDeploymentLease(t, m, fg, "migration", "op1", 30)
	failing := true
	c := interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if failing {
			return errors.New("status unavailable")
		}
		return c.SubResource(sub).Update(ctx, obj, opts...)
	}})
	runner := NewTopologyManagerRunner(c, nil, nil, nil, testLogger())
	runner.SetDeploymentLeases(m)
	epoch := m.TopologyPending(client.ObjectKeyFromObject(fg))
	snap := TopologySnapshot{TopologyGeneration: 8, DeploymentEpoch: epoch, ActiveSite: "pdx", Sites: []SiteSnapshot{{Name: "iad", State: state.StateReadOnly}, {Name: "pdx", State: state.StateWritable}}}
	if err := runner.updateCRStatus(context.Background(), client.ObjectKeyFromObject(fg), snap); err == nil {
		t.Fatal("expected persistence error")
	}
	_, err := m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", r.Token, 30)
	requireDeploymentError(t, err, 423, "unstable")
	failing = false
	if err := runner.updateCRStatus(context.Background(), client.ObjectKeyFromObject(fg), snap); err != nil {
		t.Fatal(err)
	}
	if err := m.TopologyPersisted(context.Background(), fg, epoch); err != nil {
		t.Fatal(err)
	}
	_, err = m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", r.Token, 30)
	requireDeploymentError(t, err, 409, "revoked")
	if _, pending := m.pending.Load(client.ObjectKeyFromObject(fg)); pending {
		t.Fatal("gate did not reopen")
	}
}

func TestDeploymentLeaseNames(t *testing.T) {
	if got := deploymentLeaseName("main", "migration", ""); got != "bloodraven-deploy-main-migration" {
		t.Fatal(got)
	}
	a := deploymentLeaseName(strings.Repeat("a", 253), "migration", "op1")
	b := deploymentLeaseName(strings.Repeat("a", 253), "migration", "op2")
	if len(a) > 63 || a == b {
		t.Fatalf("invalid hashed names %q %q", a, b)
	}
}

func TestDeploymentLeasePlannedDeferral(t *testing.T) {
	m, fg, now := deploymentLeaseFixture(t)
	grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	hold := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 30)
	fg.Annotations = map[string]string{PlannedFailoverAnnotation: "pdx"}
	if err := m.client.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	r := &MysqlFailoverGroupReconciler{Client: m.client, Recorder: record.NewFakeRecorder(100), Runner: NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())}
	r.Runner.SetDeploymentLeases(m)
	epoch := m.TopologyPending(client.ObjectKeyFromObject(fg))
	if err := m.TopologyPersisted(context.Background(), fg, epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fresh := fetchFG(t, r, client.ObjectKeyFromObject(fg))
	pf := fresh.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Reason != "DeploymentHold" || !pf.RetryAfter.Time.Equal(hold.ExpiresAt) || !strings.Contains(pf.Message, "op1") || !strings.Contains(pf.Message, "tenant") {
		t.Fatalf("deferral = %+v", pf)
	}
	if _, err := m.Renew(context.Background(), fresh, "failover-hold", "op1", "tenant", "tenant-ns", "deployer", hold.Token, 30); err != nil {
		t.Fatalf("deferred operation prevented renewal: %v", err)
	}
	*now = hold.ExpiresAt.Add(time.Nanosecond)
	if _, err := r.reconcilePlannedFailover(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	fresh = fetchFG(t, r, client.ObjectKeyFromObject(fg))
	if fresh.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePending {
		t.Fatalf("did not resume: %+v", fresh.Status.PlannedFailover)
	}
}

func TestDeploymentLeaseTopologyGenerationHydrationAndBootstrap(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
	runner.SetDeploymentLeases(m)
	runner.restoreFailoverState(tm, fg, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
	if tm.topologyGeneration != 7 {
		t.Fatal("generation not hydrated")
	}
	// An observed first authoritative site is a bootstrap generation change.
	tm.deploymentActiveSite = ""
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly
	snap := tm.buildSnapshot(nil)
	if snap.TopologyGeneration != 8 {
		t.Fatalf("bootstrap generation = %d", snap.TopologyGeneration)
	}
	if next := tm.buildSnapshot(nil); next.TopologyGeneration != 8 {
		t.Fatal("unchanged authority bumped generation")
	}
}

func TestDeploymentLeaseRestoreWaitAndSameSiteInvalidation(t *testing.T) {
	m, fg, now := deploymentLeaseFixture(t)
	migration := grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	hold := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 5)
	fg.Spec.RestoreInPlace = fgInPlaceRestore("").Spec.RestoreInPlace
	if err := m.client.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full-instance"}
	if err := m.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
	runner.SetDeploymentLeases(m)
	r := &MysqlFailoverGroupReconciler{Client: m.client, Recorder: record.NewFakeRecorder(100), Runner: runner}
	if _, err := r.inPlacePreflight(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlacePreflight || !strings.Contains(fg.Status.RestoreInPlace.Message, "DeploymentHold") || inPlaceRestoreFreezesTopology(fg) {
		t.Fatalf("restore did not wait safely: %+v", fg.Status.RestoreInPlace)
	}
	*now = hold.ExpiresAt.Add(time.Nanosecond)
	if _, err := r.inPlacePreflight(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing || fg.Status.TopologyGeneration != 8 || fg.Status.ActiveSite != "iad" {
		t.Fatalf("same-site invalidation missing: %+v", fg.Status)
	}
	if !r.setInPlaceRestoreStatus(context.Background(), fg, fg.Status.RestoreInPlace.DeepCopy()) || fg.Status.TopologyGeneration != 8 {
		t.Fatal("same phase retry bumped generation")
	}
	_, err := m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", migration.Token, 30)
	requireDeploymentError(t, err, 409, "revoked")
}

func TestDeploymentLeaseUpdaterDefersAndReserves(t *testing.T) {
	m, fg, now := deploymentLeaseFixture(t)
	grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	hold := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 5)
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	tm.cfg.Namespace, tm.cfg.Name = fg.Namespace, fg.Name
	tm.deploymentLeases = m
	if _, allowed := tm.beginDeploymentUpdate(context.Background()); allowed {
		t.Fatal("updater ignored deployment hold")
	}
	*now = hold.ExpiresAt.Add(time.Nanosecond)
	end, allowed := tm.beginDeploymentUpdate(context.Background())
	if !allowed {
		t.Fatal("updater did not resume after expiry")
	}
	defer end()
	_, _, err := m.Snapshot(context.Background(), fg)
	requireDeploymentError(t, err, 423, "unstable")
	_, err = m.Grant(context.Background(), fg, "failover-hold", "op1", "tenant", "tenant-ns", "deployer", 5)
	requireDeploymentError(t, err, 423, "unstable")
}

func TestDeploymentLeaseStatusRetryPreservesNewerOperations(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	var persisted *v1alpha1.MysqlFailoverGroup
	reads := 0
	c := interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
			reads++
			if reads == 2 {
				fresh := &v1alpha1.MysqlFailoverGroup{}
				if err := c.Get(ctx, key, fresh); err != nil {
					return err
				}
				fresh.Status.TopologyGeneration = 9
				fresh.Status.ActiveSite = "pdx"
				fresh.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseResuming}
				fresh.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFencing}
				if err := c.Status().Update(ctx, fresh); err != nil {
					return err
				}
				persisted = fresh.DeepCopy()
			}
		}
		return c.Get(ctx, key, obj, opts...)
	}})
	runner := NewTopologyManagerRunner(c, nil, nil, nil, testLogger())
	snap := TopologySnapshot{TopologyGeneration: 8, ActiveSite: "iad", Sites: []SiteSnapshot{{Name: "iad", State: state.StateWritable}, {Name: "pdx", State: state.StateReadOnly}}}
	if err := runner.updateCRStatus(context.Background(), client.ObjectKeyFromObject(fg), snap); err != nil {
		t.Fatal(err)
	}
	fresh := &v1alpha1.MysqlFailoverGroup{}
	if err := m.reader.Get(context.Background(), client.ObjectKeyFromObject(fg), fresh); err != nil {
		t.Fatal(err)
	}
	if persisted == nil || fresh.Status.TopologyGeneration != 9 || fresh.Status.ActiveSite != "pdx" || fresh.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseResuming || fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Fatalf("retry clobbered newer state: %+v", fresh.Status)
	}
}

func TestDeploymentLeasePendingEpochCannotBeClearedByStaleConfirmation(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	first := m.TopologyPending(client.ObjectKeyFromObject(fg))
	second := m.TopologyPending(client.ObjectKeyFromObject(fg))
	if err := m.TopologyPersisted(context.Background(), fg, first); err != nil {
		t.Fatal(err)
	}
	_, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
	requireDeploymentError(t, err, 423, "unstable")
	if err := m.TopologyPersisted(context.Background(), fg, second); err != nil {
		t.Fatal(err)
	}
	grantDeploymentLease(t, m, fg, "migration", "op1", 30)
}

func TestDeploymentLeaseHoldsSurviveMigrationHandoff(t *testing.T) {
	m, fg, now := deploymentLeaseFixture(t)
	ctx := context.Background()
	first := grantDeploymentLease(t, m, fg, "migration", "op1", 30)
	hold1 := grantDeploymentLease(t, m, fg, "failover-hold", "op1", 60)
	if err := m.Release(ctx, fg, "migration", "op1", "tenant", "tenant-ns", "deployer", first.Token); err != nil {
		t.Fatal(err)
	}
	grantDeploymentLease(t, m, fg, "migration", "op2", 30)
	hold2 := grantDeploymentLease(t, m, fg, "failover-hold", "op2", 30)
	if active := testutil.ToFloat64(metrics.DeployLeasesActive.WithLabelValues(fg.Name, "failover-hold")); active != 2 {
		t.Fatalf("active holds = %v", active)
	}
	m = NewDeploymentLeaseManager(m.client, m.reader, nil)
	m.SetClock(func() time.Time { return *now })
	// Only grants require current migration ownership, not existing renewals.
	if _, err := m.Renew(ctx, fg, "failover-hold", "op1", "tenant", "tenant-ns", "deployer", hold1.Token, 60); err != nil {
		t.Fatal(err)
	}
	_, err := m.Grant(ctx, fg, "failover-hold", "op1", "tenant", "tenant-ns", "deployer", 30)
	requireDeploymentError(t, err, 403, "forbidden")
	hold, err := m.BeginPlanned(ctx, fg, "PlannedFailover")
	if err != nil || hold == nil || hold.OperationID != "op2" || !hold.ExpiresAt.Equal(hold2.ExpiresAt) {
		t.Fatalf("earliest hold = %+v, %v", hold, err)
	}
	if err := m.Release(ctx, fg, "failover-hold", "op2", "tenant", "tenant-ns", "deployer", hold2.Token); err != nil {
		t.Fatal(err)
	}
	hold, err = m.Hold(ctx, fg)
	if err != nil || hold == nil || hold.OperationID != "op1" {
		t.Fatalf("remaining hold = %+v, %v", hold, err)
	}
	if err := m.Revoke(ctx, fg, "operator_revoked"); err != nil {
		t.Fatal(err)
	}
	_, err = m.Renew(ctx, fg, "failover-hold", "op1", "tenant", "tenant-ns", "deployer", hold1.Token, 30)
	requireDeploymentError(t, err, 409, "revoked")
}

func TestDeploymentLeaseStability(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1alpha1.MysqlFailoverGroup)
	}{
		{"missing site", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.Sites = fg.Status.Sites[:1] }},
		{"primary read-only", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.Sites[0].State = "read-only" }},
		{"recovery in progress", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.Sites[1].RecoveryState = "RecoveryInProgress" }},
		{"recovery blocked", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.Sites[1].RecoveryState = "RecoveryBlocked" }},
		{"source convergence", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Sites[1].SourceConvergenceState = v1alpha1.SourceConvergencePending
		}},
		{"restore", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning}
		}},
		{"dragonfly reconciling", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Phase: v1alpha1.DragonflyPhaseReconciling}
		}},
		{"dragonfly promoting", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Phase: v1alpha1.DragonflyPhasePromoting}
		}},
		{"dragonfly degraded", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Phase: v1alpha1.DragonflyPhaseDegraded}
		}},
		{"empty degraded reason", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Conditions = append(fg.Status.Conditions, metav1.Condition{Type: "Degraded", Status: metav1.ConditionTrue})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			tc.change(fg)
			if err := m.client.Status().Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			if DeploymentUnstableReason(fg) == "" {
				t.Fatal("unsafe topology reported stable")
			}
			_, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
			requireDeploymentError(t, err, 423, "unstable")
		})
	}
}

func TestDeploymentLeaseGenerationChangesDuringWrite(t *testing.T) {
	for _, renewal := range []bool{false, true} {
		t.Run(map[bool]string{false: "grant", true: "renew"}[renewal], func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			var token string
			if renewal {
				token = grantDeploymentLease(t, m, fg, "migration", "op1", 30).Token
			}
			change := func(ctx context.Context, c client.Client) error {
				fresh := &v1alpha1.MysqlFailoverGroup{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(fg), fresh); err != nil {
					return err
				}
				fresh.Status.TopologyGeneration++
				return c.Status().Update(ctx, fresh)
			}
			changed := false
			m.client = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if err := c.Create(ctx, obj, opts...); err != nil {
						return err
					}
					if !changed {
						changed = true
						return change(ctx, c)
					}
					return nil
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if err := c.Update(ctx, obj, opts...); err != nil {
						return err
					}
					if !changed {
						changed = true
						return change(ctx, c)
					}
					return nil
				},
			})
			var err error
			if renewal {
				_, err = m.Renew(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", token, 30)
			} else {
				_, err = m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
			}
			if e := requireDeploymentError(t, err, 409, "revoked"); e.TopologyGeneration != 8 {
				t.Fatal(e)
			}
		})
	}
}

func TestDeploymentLeaseSnapshotRejectsTornObservation(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "generation", true: "pending"}[pending], func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			grantDeploymentLease(t, m, fg, "migration", "op1", 30)
			changed := false
			m.reader = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*coordinationv1.Lease); ok && !changed {
					changed = true
					if pending {
						m.TopologyPending(client.ObjectKeyFromObject(fg))
					} else {
						fresh := &v1alpha1.MysqlFailoverGroup{}
						if err := c.Get(ctx, client.ObjectKeyFromObject(fg), fresh); err != nil {
							return err
						}
						fresh.Status.TopologyGeneration++
						fresh.Status.ActiveSite = "pdx"
						if err := c.Status().Update(ctx, fresh); err != nil {
							return err
						}
					}
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			fresh, leases, err := m.Snapshot(context.Background(), fg)
			if pending {
				requireDeploymentError(t, err, 423, "unstable")
				return
			}
			if err != nil || fresh.Status.TopologyGeneration != 8 || fresh.Status.ActiveSite != "pdx" || len(leases) != 0 {
				t.Fatalf("snapshot = %+v, %+v, %v", fresh, leases, err)
			}
		})
	}
}

func TestDeploymentLeaseUnconfirmedPromotionFencesGeneration(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	old := &mockMySQL{readOnly: false}
	target := &mockMySQL{readOnly: true, err: errors.New("probe unavailable")}
	tm, _, _ := newTestTopologyManager(old, target)
	tm.cfg.Namespace, tm.cfg.Name = fg.Namespace, fg.Name
	tm.deploymentLeases = m
	tm.topologyGeneration, tm.deploymentActiveSite = 7, "dc1"
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
	if _, err := tm.PlannedPromote(context.Background(), "dc2", "dc1"); err == nil {
		t.Fatal("expected failed confirmation")
	}
	snap := tm.buildSnapshot(nil)
	if target.readOnly || snap.TopologyGeneration != 8 || snap.DeploymentEpoch != 0 || snap.DeploymentActiveSite != "dc2" {
		t.Fatalf("unconfirmed promotion reopened stale authority: %+v", snap)
	}
	target.setError(nil)
	tm.Poll(context.Background())
	snap = tm.buildSnapshot(nil)
	if snap.TopologyGeneration != 8 || snap.DeploymentEpoch == 0 {
		t.Fatalf("fresh observation = %+v", snap)
	}
}

func TestDeploymentLeaseUpdateDiscardsPlanAfterAdmissionWait(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "dc1-host"}})
	tm.cfg.Name, tm.cfg.Namespace = fg.Name, fg.Namespace
	tm.deploymentLeases = m
	tm.topologyGeneration, tm.deploymentActiveSite = 7, "dc1"
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.specDriftSites = []string{"dc2"}
	applied := make(chan struct{}, 1)
	tm.ApplyUpdate = func(context.Context, string) error { applied <- struct{}{}; return errors.New("unexpected update") }
	entered, resume := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	m.reader = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if blocked.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
		return c.Get(ctx, key, obj, opts...)
	}})
	if !tm.checkUpdate(context.Background()) {
		t.Fatal("update not queued")
	}
	<-entered
	// Advance authority while the asynchronous admission read is stalled.
	tm.recordFailover(context.Background(), tm.clock.Now(), "dc2", "")
	fg.Status.TopologyGeneration = 8
	if err := m.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if err := m.TopologyPersisted(context.Background(), fg, tm.deploymentEpoch); err != nil {
		t.Fatal(err)
	}
	close(resume)
	deadline := time.Now().Add(2 * time.Second)
	for {
		tm.mu.RLock()
		waiting := tm.deploymentUpdatePending
		tm.mu.RUnlock()
		if !waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("update admission did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-applied:
		t.Fatal("stale update plan ran after authority changed")
	default:
	}
}

func TestDeploymentLeaseGenerationRestoredFromUnpersistedFailover(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	ctx := context.Background()
	lease := grantDeploymentLease(t, m, fg, "migration", "op1", 120)
	// The last status write still describes iad, but a later failover also
	// ended at iad. Comparing just the active-site name misses this restart.
	failoverTime := time.Now().Add(-time.Minute).Truncate(time.Second)
	annotations, err := FailoverRecordAnnotations(FailoverRecord{LastFailover: failoverTime, LastFailoverTarget: "iad"})
	if err != nil {
		t.Fatal(err)
	}
	fg.Annotations = annotations
	if err := m.client.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	tm.cfg.Name, tm.cfg.Namespace = fg.Name, fg.Namespace
	runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
	runner.SetDeploymentLeases(m)
	runner.restoreFailoverState(tm, fg, client.ObjectKeyFromObject(fg))
	if tm.topologyGeneration != 8 {
		t.Fatalf("generation = %d", tm.topologyGeneration)
	}
	snap := TopologySnapshot{TopologyGeneration: tm.topologyGeneration, DeploymentEpoch: tm.deploymentEpoch, ActiveSite: "iad", LastFailover: failoverTime, LastFailoverTarget: "iad", Sites: []SiteSnapshot{{Name: "iad", State: state.StateWritable}, {Name: "pdx", State: state.StateReadOnly}}}
	if err := runner.updateCRStatus(ctx, client.ObjectKeyFromObject(fg), snap); err != nil {
		t.Fatal(err)
	}
	if err := m.TopologyPersisted(ctx, fg, tm.deploymentEpoch); err != nil {
		t.Fatal(err)
	}
	_, err = m.Renew(ctx, fg, "migration", "op1", "tenant", "tenant-ns", "deployer", lease.Token, 30)
	requireDeploymentError(t, err, 409, "revoked")
	fresh, err := m.fresh(ctx, fg)
	if err != nil {
		t.Fatal(err)
	}
	restarted, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	runner.restoreFailoverState(restarted, fresh, client.ObjectKeyFromObject(fg))
	if restarted.topologyGeneration != 8 {
		t.Fatalf("ordinary restart bumped generation to %d", restarted.topologyGeneration)
	}
}

func TestDeploymentLeaseReassertNeverWaitsForAdmission(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	primary := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(primary, &mockMySQL{readOnly: true})
	tm.cfg.Name, tm.cfg.Namespace = fg.Name, fg.Namespace
	tm.deploymentLeases = m
	tm.topologyGeneration, tm.deploymentActiveSite = 7, "dc1"
	setWedgedTopology(tm, "dc1")
	m.mu.Lock()
	done := make(chan bool, 1)
	go func() { done <- tm.checkPrimaryReassert(context.Background()) }()
	select {
	case attempted := <-done:
		m.mu.Unlock()
		if !attempted || primary.readOnly {
			t.Fatal("reassert did not restore writability")
		}
	case <-time.After(2 * time.Second):
		m.mu.Unlock()
		t.Fatal("reassert waited for lease admission")
	}
	if tm.topologyGeneration != 7 {
		t.Fatalf("same-authority reassert bumped generation to %d", tm.topologyGeneration)
	}
	if snap := tm.buildSnapshot(nil); snap.DeploymentEpoch != 0 {
		t.Fatal("reassert reopened admission before a fresh observation")
	}
}

// A refused topology change touches no MySQL state, so it must not consume the
// re-assert cooldown and block the next poll for failoverCooldown.
func TestDeploymentLeaseReassertRefusalDoesNotConsumeCooldown(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	primary := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(primary, &mockMySQL{readOnly: true})
	tm.cfg.Name, tm.cfg.Namespace = fg.Name, fg.Namespace
	tm.deploymentLeases = m
	tm.topologyGeneration, tm.deploymentActiveSite = 7, "dc1"
	tm.failoverCooldown = 30 * time.Second
	setWedgedTopology(tm, "dc1")

	// Another topology change is already in flight, so admission is refused.
	tm.mu.Lock()
	tm.deploymentChanging = 1
	tm.mu.Unlock()
	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("reassert reported an attempt while a topology change was already active")
	}
	tm.mu.RLock()
	stamped, stillReadOnly := tm.lastReassert, primary.readOnly
	tm.mu.RUnlock()
	if !stamped.IsZero() {
		t.Fatal("admission refusal consumed the re-assert cooldown")
	}
	if !stillReadOnly {
		t.Fatal("refused reassert mutated MySQL")
	}

	tm.mu.Lock()
	tm.deploymentChanging = 0
	tm.mu.Unlock()
	if !tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("reassert stayed blocked by the cooldown after admission was released")
	}
	if primary.readOnly {
		t.Fatal("reassert did not restore writability")
	}
}

// Only revocations need to outlive the slot. Archiving released or expired
// records would leak one Lease per historical operation ID with nothing gating
// on it, since the tombstone check rejects "revoked" only.
func TestDeploymentLeaseSupersededNonRevokedRecordsAreNotArchived(t *testing.T) {
	for _, tc := range []struct{ name, mode string }{{"released", "release"}, {"expired", "expire"}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m, fg, now := deploymentLeaseFixture(t)
			old := grantDeploymentLease(t, m, fg, "migration", "old-operation", 30)
			if tc.mode == "release" {
				if err := m.Release(ctx, fg, "migration", "old-operation", "tenant", "tenant-ns", "deployer", old.Token); err != nil {
					t.Fatal(err)
				}
			} else {
				*now = now.Add(time.Hour)
			}
			next := grantDeploymentLease(t, m, fg, "migration", "new-operation", 30)

			archived := deploymentLeaseName(fg.Name, "migration", "old-operation")
			var leases coordinationv1.LeaseList
			if err := m.reader.List(ctx, &leases); err != nil {
				t.Fatal(err)
			}
			for i := range leases.Items {
				if leases.Items[i].Name == archived {
					t.Fatalf("superseded %s record was archived as %s", tc.name, archived)
				}
			}
			// The operation ID stays reusable: only revocation fences it.
			if err := m.Release(ctx, fg, "migration", "new-operation", "tenant", "tenant-ns", "deployer", next.Token); err != nil {
				t.Fatal(err)
			}
			if r := grantDeploymentLease(t, m, fg, "migration", "old-operation", 30); r.Status != 201 {
				t.Fatalf("regrant status = %d, want 201", r.Status)
			}
		})
	}
}

func TestDeploymentLeaseGrantRechecksStabilityAfterWrite(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	m.client = interceptor.NewClient(m.client.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if err := c.Create(ctx, obj, opts...); err != nil {
			return err
		}
		fresh := &v1alpha1.MysqlFailoverGroup{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(fg), fresh); err != nil {
			return err
		}
		fresh.Status.Dragonfly = &v1alpha1.DragonflyStatus{Phase: v1alpha1.DragonflyPhasePromoting}
		return c.Status().Update(ctx, fresh)
	}})
	_, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
	if e := requireDeploymentError(t, err, 423, "unstable"); e.Reason != "DragonflyPromoting" {
		t.Fatal(e)
	}
}
