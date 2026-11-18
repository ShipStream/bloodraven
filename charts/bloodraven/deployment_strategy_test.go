package bloodraven_test

import (
	"os/exec"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"
)

// TestOperatorDeploymentStrategy pins how the operator's pod is replaced
// (#299). With leader election the previous pod must stay until the new one
// is Ready: a pod that refuses to start (cmd/bloodraven/crd_check.go) never
// is, so a chart-only upgrade leaves the previous operator running. Without
// leader election the pods must never overlap.
func TestOperatorDeploymentStrategy(t *testing.T) {
	requireHelm(t)
	surge, unavailable := intstr.FromInt32(1), intstr.FromInt32(0)
	for _, tt := range []struct {
		name string
		args []string
		want appsv1.DeploymentStrategy
	}{
		{
			name: "default",
			want: appsv1.DeploymentStrategy{
				Type:          appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &surge, MaxUnavailable: &unavailable},
			},
		},
		{
			name: "two replicas",
			args: []string{"--set", "replicaCount=2"},
			want: appsv1.DeploymentStrategy{
				Type:          appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &surge, MaxUnavailable: &unavailable},
			},
		},
		{
			name: "leader election off",
			args: []string{"--set", "leaderElection.enabled=false"},
			want: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"template", "test", ".", "--namespace", "operator-system", "--show-only", "templates/deployment.yaml"}, tt.args...)
			out, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			var deployment appsv1.Deployment
			if err := yaml.UnmarshalStrict(out, &deployment); err != nil {
				t.Fatalf("decode the Deployment: %v\n%s", err, out)
			}
			got := deployment.Spec.Strategy
			if got.Type != tt.want.Type {
				t.Fatalf("strategy type = %q, want %q", got.Type, tt.want.Type)
			}
			if (got.RollingUpdate == nil) != (tt.want.RollingUpdate == nil) {
				t.Fatalf("rollingUpdate = %+v, want %+v", got.RollingUpdate, tt.want.RollingUpdate)
			}
			if got.RollingUpdate == nil {
				return
			}
			if *got.RollingUpdate.MaxSurge != surge || *got.RollingUpdate.MaxUnavailable != unavailable {
				t.Fatalf("rollingUpdate = surge %s, unavailable %s; want 1 and 0",
					got.RollingUpdate.MaxSurge.String(), got.RollingUpdate.MaxUnavailable.String())
			}
		})
	}
}
