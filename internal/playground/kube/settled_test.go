package kube

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestSiteDeploymentProblem(t *testing.T) {
	mem := func(v string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(v)}}
	}
	site := &v1alpha1.SiteSpec{Name: "iad", Resources: mem("384Mi")}
	deployment := func(res corev1.ResourceRequirements, gen, observed int64, updated, replicas, available int32) *appsv1.Deployment {
		d := &appsv1.Deployment{}
		d.Generation = gen
		d.Spec.Template.Spec.Containers = []corev1.Container{{Name: "sidecar"}, {Name: "mysql", Resources: res}}
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: observed, UpdatedReplicas: updated, Replicas: replicas, AvailableReplicas: available}
		return d
	}
	cases := []struct {
		name string
		dep  *appsv1.Deployment
		want string
	}{
		{"settled", deployment(mem("384Mi"), 3, 3, 1, 1, 1), ""},
		{"same quantity written differently", deployment(mem("402653184"), 3, 3, 1, 1, 1), ""},
		{"spec change not written by the operator yet", deployment(mem("276Mi"), 3, 3, 1, 1, 1), "resources-not-applied"},
		{"template written, not observed", deployment(mem("384Mi"), 4, 3, 1, 1, 1), "rollout-not-observed"},
		{"old and new pod", deployment(mem("384Mi"), 4, 4, 1, 2, 1), "rolling(updated=1 replicas=2 want=1)"},
		{"new pod not available", deployment(mem("384Mi"), 4, 4, 1, 1, 0), "unavailable(available=0 want=1)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SiteDeploymentProblem(site, tc.dep); got != tc.want {
				t.Fatalf("problem = %q, want %q", got, tc.want)
			}
		})
	}
}
