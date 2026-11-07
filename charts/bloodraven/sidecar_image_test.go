package bloodraven_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	sigsyaml "sigs.k8s.io/yaml"
)

// TestSidecarImageChart pins the env the controller reads for the default
// sidecar image of groups that leave spec.sidecar.image empty
// (SetSidecarImageDefault in internal/controller) and that it renders from
// sidecar.image.repository and sidecar.image.tag, the tag defaulting to the
// chart's appVersion.
func TestSidecarImageChart(t *testing.T) {
	requireHelm(t)
	raw, err := os.ReadFile("Chart.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var chart struct {
		AppVersion string `json:"appVersion"`
	}
	if err := sigsyaml.Unmarshal(raw, &chart); err != nil {
		t.Fatal(err)
	}
	if chart.AppVersion == "" {
		t.Fatal("Chart.yaml has no appVersion")
	}
	for _, tt := range []struct {
		name, values, want string
	}{
		{name: "default", want: "ghcr.io/shipstream/bloodraven-sidecar:" + chart.AppVersion},
		{name: "mirror repository", values: "sidecar:\n  image:\n    repository: registry.example/mirror/bloodraven-sidecar\n",
			want: "registry.example/mirror/bloodraven-sidecar:" + chart.AppVersion},
		{name: "repository and tag", values: "sidecar:\n  image:\n    repository: bloodraven-sidecar\n    tag: playground\n",
			want: "bloodraven-sidecar:playground"},
		{name: "operator image does not move it", values: "image:\n  repository: registry.example/bloodraven\n  tag: dev\n",
			want: "ghcr.io/shipstream/bloodraven-sidecar:" + chart.AppVersion},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("helm", "template", "test", ".", "--namespace", "operator-system", "--values", "-")
			cmd.Stdin = strings.NewReader(tt.values)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			if got := operatorEnv(t, out, "BLOODRAVEN_SIDECAR_IMAGE"); got != tt.want {
				t.Fatalf("BLOODRAVEN_SIDECAR_IMAGE = %q, want %q", got, tt.want)
			}
		})
	}
}
