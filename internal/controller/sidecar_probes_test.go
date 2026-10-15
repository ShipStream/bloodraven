package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// The sidecar is the component that fences MySQL when MySQL misbehaves, so
// its liveness must not depend on MySQL: a liveness probe on the
// MySQL-backed /health restarts the fencing agent during exactly the stalls
// it exists for, and a restart loop leaves the node with no fencing at all.
// Readiness keeps reporting MySQL connectivity, with a probe timeout long
// enough for the handler's own bound so a slow MySQL reads as 503, not as a
// probe timeout.
func TestReconcile_SidecarLivenessIsIndependentOfMySQL(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	for _, site := range fg.Spec.Sites {
		var d appsv1.Deployment
		if err := c.Get(context.Background(), types.NamespacedName{
			Namespace: fg.Namespace, Name: resourceName(fg.Name, site.Name),
		}, &d); err != nil {
			t.Fatalf("get deployment %s: %v", site.Name, err)
		}
		sc := containerByName(d.Spec.Template.Spec.Containers, "sidecar")
		if sc == nil {
			t.Fatalf("site %s: no sidecar container", site.Name)
		}

		live := httpProbePath(t, site.Name, "liveness", sc.LivenessProbe)
		if live == "/health" {
			t.Errorf("site %s: sidecar liveness probes /health, which fails whenever MySQL stalls and restarts the fencing agent", site.Name)
		}
		if live != "/peer/ping" {
			t.Errorf("site %s: sidecar liveness path = %q, want /peer/ping", site.Name, live)
		}

		if ready := httpProbePath(t, site.Name, "readiness", sc.ReadinessProbe); ready != "/health" {
			t.Errorf("site %s: sidecar readiness path = %q, want /health", site.Name, ready)
		}
		// /health bounds its MySQL check at 2s; a 1s probe timeout would
		// report every slow-but-alive MySQL as a probe timeout.
		if sc.ReadinessProbe.TimeoutSeconds < 3 {
			t.Errorf("site %s: sidecar readiness timeoutSeconds = %d, want >= 3 to fit /health's 2s MySQL bound",
				site.Name, sc.ReadinessProbe.TimeoutSeconds)
		}
	}
}

func httpProbePath(t *testing.T, site, kind string, p *corev1.Probe) string {
	t.Helper()
	if p == nil || p.HTTPGet == nil {
		t.Fatalf("site %s: sidecar %s probe is not an HTTP GET: %+v", site, kind, p)
	}
	if p.HTTPGet.Port.IntValue() != sidecarPort {
		t.Errorf("site %s: sidecar %s probe port = %s, want %d", site, kind, p.HTTPGet.Port.String(), sidecarPort)
	}
	return p.HTTPGet.Path
}
