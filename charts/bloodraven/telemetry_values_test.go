package bloodraven_test

import (
	"bytes"
	"os/exec"
	"testing"
)

// TestTelemetryValuesChart pins the env the operator reads for the usage
// report (internal/telemetry) and that the report stays on when the
// telemetry key is missing, which is what `helm upgrade --reuse-values` from
// a release before it renders with.
func TestTelemetryValuesChart(t *testing.T) {
	requireHelm(t)
	for _, tt := range []struct {
		name         string
		args         []string
		enabled      string
		wantEndpoint bool
	}{
		{name: "defaults", enabled: "true"},
		{name: "off", args: []string{"--set", "telemetry.enabled=false"}, enabled: "false"},
		{name: "endpoint", args: []string{"--set", "telemetry.endpoint=https://proxy.example/r"}, enabled: "true", wantEndpoint: true},
		{name: "values of a release before telemetry", args: []string{"--set", "telemetry=null"}, enabled: "true"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"template", "test", ".", "--namespace", "operator-system"}, tt.args...)
			out, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			if got := operatorEnv(t, out, "BLOODRAVEN_TELEMETRY"); got != tt.enabled {
				t.Errorf("BLOODRAVEN_TELEMETRY = %q, want %q", got, tt.enabled)
			}
			// POD_NAMESPACE comes from the downward API: the install ID
			// ConfigMap lives in the operator's namespace.
			if !bytes.Contains(out, []byte("name: POD_NAMESPACE")) {
				t.Error("POD_NAMESPACE is not rendered")
			}
			if got := bytes.Contains(out, []byte("BLOODRAVEN_TELEMETRY_ENDPOINT")); got != tt.wantEndpoint {
				t.Errorf("BLOODRAVEN_TELEMETRY_ENDPOINT rendered = %v, want %v", got, tt.wantEndpoint)
			}
			if tt.wantEndpoint && operatorEnv(t, out, "BLOODRAVEN_TELEMETRY_ENDPOINT") != "https://proxy.example/r" {
				t.Error("endpoint override not rendered")
			}
		})
	}
}
