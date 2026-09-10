package component

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func deploymentCoordinationFixture(t *testing.T) (*controller.MysqlFailoverGroupReconciler, *controller.DeploymentLeaseManager, *v1alpha1.MysqlFailoverGroup, *clock.FakeClock) {
	t.Helper()
	ctx := context.Background()
	clk := clock.NewFakeClock(time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC))
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: "default", UID: "deployment-coordination-uid"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			SecretName: "mysql-credentials",
			Sites: []v1alpha1.SiteSpec{
				{Name: "dc1", Zone: "lion-dc1", Storage: v1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}},
				{Name: "dc2", Zone: "lion-dc2", Storage: v1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}},
			},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "dc1", TopologyGeneration: 7,
			Sites: []v1alpha1.SiteStatus{
				{Name: "dc1", State: "writable"},
				{Name: "dc2", State: "read-only", Replicating: true},
			},
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy", LastTransitionTime: metav1.NewTime(clk.Now())}},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: fg.Spec.SecretName, Namespace: fg.Namespace},
		Data:       map[string][]byte{"dsn": []byte("root:password@tcp(localhost:3306)/mysql")},
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(fg).WithObjects(fg, secret).Build()
	recorder := record.NewFakeRecorder(100)
	leases := controller.NewDeploymentLeaseManager(c, c, recorder)
	leases.SetClock(clk.Now)
	runner := controller.NewTopologyManagerRunner(c, nil, nil, recorder, slog.New(slog.NewTextHandler(io.Discard, nil)))
	runner.SetDeploymentLeases(leases)
	// Seed the already-persisted healthy topology without starting a live runner.
	epoch := leases.TopologyPending(client.ObjectKeyFromObject(fg))
	if err := leases.TopologyPersisted(ctx, fg, epoch); err != nil {
		t.Fatal(err)
	}
	r := &controller.MysqlFailoverGroupReconciler{Client: c, APIReader: c, Scheme: scheme, Recorder: recorder, Runner: runner}
	return r, leases, fg, clk
}

func grantCoordinationHold(t *testing.T, leases *controller.DeploymentLeaseManager, fg *v1alpha1.MysqlFailoverGroup, operation, instance string, ttl int) controller.DeploymentLeaseResult {
	t.Helper()
	ctx := context.Background()
	migration, err := leases.Grant(ctx, fg, "migration", operation, instance, "tenant-ns", "deployer", 120)
	if err != nil {
		t.Fatalf("grant migration: %v", err)
	}
	hold, err := leases.Grant(ctx, fg, "failover-hold", operation, instance, "tenant-ns", "deployer", ttl)
	if err != nil {
		t.Fatalf("grant hold: %v", err)
	}
	// Migration serialization can end while its independently expiring hold remains.
	if err := leases.Release(ctx, fg, "migration", operation, instance, "tenant-ns", "deployer", migration.Token); err != nil {
		t.Fatalf("release migration: %v", err)
	}
	return hold
}

func TestDeploymentCoordination_PlannedFailoverHoldExpiry(t *testing.T) {
	for _, unhealthy := range []bool{false, true} {
		name := "healthy_target_resumes_pending"
		if unhealthy {
			name = "changed_target_revalidated_and_rejected"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r, leases, fg, clk := deploymentCoordinationFixture(t)
			later := grantCoordinationHold(t, leases, fg, "operation-long", "instance-long", 90)
			earlier := grantCoordinationHold(t, leases, fg, "operation-short", "instance-short", 30)
			fg.Annotations = map[string]string{controller.PlannedFailoverAnnotation: "dc2"}
			if err := r.Update(ctx, fg); err != nil {
				t.Fatal(err)
			}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}
			reconcile := func() ctrl.Result {
				t.Helper()
				result, err := r.Reconcile(ctx, request)
				if err != nil {
					t.Fatalf("Reconcile: %v", err)
				}
				if err := r.Get(ctx, request.NamespacedName, fg); err != nil {
					t.Fatal(err)
				}
				return result
			}
			assertHeld := func(hold controller.DeploymentLeaseResult) {
				t.Helper()
				result := reconcile()
				pf := fg.Status.PlannedFailover
				if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Reason != "DeploymentHold" {
					t.Fatalf("planned failover = %+v, want Deferred/DeploymentHold", pf)
				}
				if pf.RetryAfter == nil || !pf.RetryAfter.Time.Equal(hold.ExpiresAt) {
					t.Fatalf("retryAfter = %v, want earliest hold expiry %v", pf.RetryAfter, hold.ExpiresAt)
				}
				if !strings.Contains(pf.Message, hold.OperationID) || !strings.Contains(pf.Message, hold.Instance) {
					t.Fatalf("hold message lacks operation/instance: %q", pf.Message)
				}
				if pf.Target != "dc2" || pf.SourcePrimary != "dc1" || fg.Status.ActiveSite != "dc1" {
					t.Fatalf("hold changed switchover identity: status=%+v", fg.Status)
				}
				if fg.Annotations[controller.PlannedFailoverAnnotation] != "dc2" {
					t.Fatal("hold consumed annotation needed for automatic retry")
				}
				if want := max(time.Second, hold.ExpiresAt.Sub(clk.Now())+time.Nanosecond); result.RequeueAfter != want {
					t.Fatalf("requeueAfter = %v, want %v", result.RequeueAfter, want)
				}
			}

			assertHeld(earlier)
			start := fg.Status.PlannedFailover.StartTime.DeepCopy()
			clk.Advance(earlier.ExpiresAt.Sub(clk.Now()))
			assertHeld(earlier) // Equality is still active; expiry is strictly after the deadline.
			clk.Advance(time.Nanosecond)
			assertHeld(later)
			if unhealthy {
				fg.Status.Sites[1].Replicating = false
				if err := r.Status().Update(ctx, fg); err != nil {
					t.Fatal(err)
				}
			}
			clk.Advance(later.ExpiresAt.Sub(clk.Now()) + time.Nanosecond)
			result := reconcile()
			pf := fg.Status.PlannedFailover
			if pf == nil {
				t.Fatal("expiry cleared planned-failover status instead of revalidating")
			}
			if unhealthy {
				if pf.Phase != v1alpha1.PlannedFailoverPhaseFailed || pf.Reason != "TargetUnhealthy" {
					t.Fatalf("changed target was not revalidated: %+v", pf)
				}
			} else if pf.Phase != v1alpha1.PlannedFailoverPhasePending || pf.Reason != "" || result.RequeueAfter != time.Second {
				t.Fatalf("expired holds did not resume Pending: status=%+v result=%+v", pf, result)
			}
			if pf.RetryAfter != nil || !pf.StartTime.Equal(start) {
				t.Fatalf("expiry left retryAfter or reset startTime: %+v", pf)
			}
			if _, present := fg.Annotations[controller.PlannedFailoverAnnotation]; present {
				t.Fatal("revalidated request retained annotation")
			}
			if hold, err := leases.Hold(ctx, fg); err != nil || hold != nil {
				t.Fatalf("expired holds remain: %+v, %v", hold, err)
			}
		})
	}
}

func TestDeploymentCoordination_DeferredHoldAllowsEmergencyPoll(t *testing.T) {
	ctx := context.Background()
	r, leases, fg, clk := deploymentCoordinationFixture(t)
	hold := grantCoordinationHold(t, leases, fg, "operation-emergency", "instance-emergency", 120)
	h := newTestHarness(t)
	h.pollN(2)
	r.Runner.SetManagerForTest(client.ObjectKeyFromObject(fg), h.tm)
	fg.Annotations = map[string]string{controller.PlannedFailoverAnnotation: "dc2"}
	if err := r.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}
	// Exercise both initial deferral and the status-driven guard sync on retry.
	for range 2 {
		if _, err := r.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Get(ctx, request.NamespacedName, fg); err != nil {
		t.Fatal(err)
	}
	if pf := fg.Status.PlannedFailover; pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Reason != "DeploymentHold" {
		t.Fatalf("planned failover not held before emergency: %+v", pf)
	}
	if !h.dc2MySQL.isReadOnly() {
		t.Fatal("target became writable before primary failure")
	}
	if active, err := leases.Hold(ctx, fg); err != nil || active == nil || active.OperationID != hold.OperationID || !clk.Now().Before(active.ExpiresAt) {
		t.Fatalf("hold not active at emergency: %+v, %v", active, err)
	}

	// This covers the reconciler-to-runner guard, not asynchronous lease revocation.
	h.dc1MySQL.setError(errDown)
	h.pollN(3)
	if h.dc2MySQL.isReadOnly() || h.dc2MySQL.writableGrants() == 0 {
		t.Fatal("deployment deferral blocked emergency promotion through Poll")
	}
	if !h.tainter.isTainted(taintSelector("dc1")) || h.dns.getLastIP() != "2.2.2.2" {
		t.Fatal("emergency promotion did not fence the failed site and redirect DNS")
	}
	h.pollN(2)
	if status := h.tm.Status(); status.Sites[0].State != "unreachable" || status.Sites[1].State != "writable" {
		t.Fatalf("emergency topology did not converge: %+v", status)
	}
}
