//go:build envtest

package envtest

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestDeploymentLeaseTerminalPersistence(t *testing.T) {
	for _, tc := range []struct{ state, reason string }{
		{"expired", ""},
		{"released", ""},
		{"revoked", "operator_revoked"},
		{"revoked", "topology_changed"},
	} {
		t.Run(tc.state+"/"+tc.reason, func(t *testing.T) {
			ns := createNamespace(t, "deploy-terminal")
			fg := newTestFG(ns)
			if err := k8sClient.Create(ctx, fg); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(ctx, fg) })
			fg.Status.TopologyGeneration = 7
			fg.Status.ActiveSite = "dc1"
			fg.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy", LastTransitionTime: metav1.Now()}}
			fg.Status.Sites = []v1alpha1.SiteStatus{
				{Name: "dc1", State: "writable"},
				{Name: "dc2", State: "read-only", Replicating: true},
			}
			if err := k8sClient.Status().Update(ctx, fg); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
			m := controller.NewDeploymentLeaseManager(k8sClient, k8sClient, nil)
			m.SetClock(func() time.Time { return now })
			grants := make(map[string]controller.DeploymentLeaseResult)
			for _, kind := range []string{"migration", "failover-hold"} {
				grant, err := m.Grant(ctx, fg, kind, "old-operation", "tenant", ns, "deployer", 5)
				if err != nil {
					t.Fatal(err)
				}
				grants[kind] = grant
			}
			expiresAt := grants["migration"].ExpiresAt
			now = expiresAt.Add(-500 * time.Millisecond)
			if hold, err := m.BeginPlanned(ctx, fg, "PlannedFailover"); err != nil || hold == nil {
				t.Fatalf("expected blocking hold before expiry: %v, %v", hold, err)
			}
			switch tc.state {
			case "expired":
				// Equality still holds; the one-second spec floor must not add grace.
				now = expiresAt
				if hold, err := m.BeginPlanned(ctx, fg, "PlannedFailover"); err != nil || hold == nil {
					t.Fatalf("expected blocking hold at equality: %v, %v", hold, err)
				}
				now = expiresAt.Add(time.Nanosecond)
				// Exercise the housekeeping path that failed against the live API.
				if err := m.TopologyPersisted(ctx, fg, 0); err != nil {
					t.Fatal(err)
				}
			case "released":
				for kind, grant := range grants {
					if err := m.Release(ctx, fg, kind, "old-operation", "tenant", ns, "deployer", grant.Token); err != nil {
						t.Fatal(err)
					}
				}
			case "revoked":
				if tc.reason == "topology_changed" {
					fg.Status.TopologyGeneration++
					if err := k8sClient.Status().Update(ctx, fg); err != nil {
						t.Fatal(err)
					}
					if err := m.TopologyPersisted(ctx, fg, 0); err != nil {
						t.Fatal(err)
					}
				} else if err := m.Revoke(ctx, fg, tc.reason); err != nil {
					t.Fatal(err)
				}
			}
			var leases coordinationv1.LeaseList
			if err := k8sClient.List(ctx, &leases, client.InNamespace(ns)); err != nil {
				t.Fatal(err)
			}
			if len(leases.Items) != 2 {
				t.Fatalf("stored leases = %d, want 2", len(leases.Items))
			}
			for _, lease := range leases.Items {
				var record struct {
					controller.DeploymentLeaseView
					State  string `json:"state"`
					Reason string `json:"reason"`
				}
				if err := json.Unmarshal([]byte(lease.Annotations["bloodraven.shipstream.io/deployment-lease"]), &record); err != nil {
					t.Fatal(err)
				}
				if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 1 {
					t.Fatalf("%s: terminal duration must be 1", lease.Name)
				}
				if record.State != tc.state || record.Reason != tc.reason || !record.ExpiresAt.Equal(expiresAt) || record.TopologyGeneration != 7 || lease.Labels["bloodraven.shipstream.io/deployment-state"] != tc.state {
					t.Fatalf("%s: terminal record = %+v", lease.Name, record)
				}
			}
			// A new manager must observe durable terminal states without waiting
			// for the projected Kubernetes duration or the original expiry.
			m = controller.NewDeploymentLeaseManager(k8sClient, k8sClient, nil)
			m.SetClock(func() time.Time { return now })
			if views, err := m.List(ctx, fg); err != nil || len(views) != 0 {
				t.Fatalf("terminal leases listed: %v, %v", views, err)
			}
			if hold, err := m.BeginPlanned(ctx, fg, "PlannedFailover"); err != nil || hold != nil {
				t.Fatalf("terminal hold blocked planned admission: %v, %v", hold, err)
			}
			m.EndPlanned(fg)
			// Archiving a revoked migration after its original expiry must also
			// be API-valid and retain the old operation's fencing tombstone.
			now = expiresAt.Add(24 * time.Hour)
			if _, err := m.Grant(ctx, fg, "migration", "new-operation", "tenant", ns, "deployer", 30); err != nil {
				t.Fatal(err)
			}
			m = controller.NewDeploymentLeaseManager(k8sClient, k8sClient, nil)
			m.SetClock(func() time.Time { return now })
			for kind, grant := range grants {
				_, err := m.Renew(ctx, fg, kind, "old-operation", "tenant", ns, "deployer", grant.Token, 30)
				var leaseErr *controller.DeploymentLeaseError
				if !errors.As(err, &leaseErr) {
					t.Fatalf("%s: expected ownership loss, got %v", kind, err)
				}
				if tc.state == "revoked" {
					if leaseErr.Status != 409 || leaseErr.Code != "revoked" || leaseErr.Reason != tc.reason || leaseErr.TopologyGeneration != fg.Status.TopologyGeneration {
						t.Fatalf("%s: lost revocation fencing: %v", kind, leaseErr)
					}
				} else if leaseErr.Status != 404 || leaseErr.Code != "not_found" {
					t.Fatalf("%s: expected not_found, got %v", kind, leaseErr)
				}
				if err := m.Release(ctx, fg, kind, "old-operation", "tenant", ns, "deployer", grant.Token); err != nil {
					t.Fatalf("%s: idempotent release: %v", kind, err)
				}
			}
			if tc.state == "revoked" {
				now = now.Add(time.Minute)
				_, err := m.Grant(ctx, fg, "migration", "old-operation", "tenant", ns, "deployer", 30)
				var leaseErr *controller.DeploymentLeaseError
				if !errors.As(err, &leaseErr) || leaseErr.Status != 409 || leaseErr.Code != "revoked" || leaseErr.Reason != tc.reason {
					t.Fatalf("revoked operation re-grant = %v", err)
				}
			}
		})
	}
}
