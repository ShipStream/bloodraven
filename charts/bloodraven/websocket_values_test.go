package bloodraven_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// requireHelm skips a chart render test when helm is missing, except in CI,
// where the job installs helm and a skip would hide a broken chart.
func requireHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("helm is required for chart render tests in CI: %v", err)
		}
		t.Skip("helm is required for chart render tests")
	}
}

func TestWSMaxClientsChart(t *testing.T) {
	requireHelm(t)
	const wantError = "auxiliary.service.wsMaxClients must be a whole number from 0 (unlimited) to 2147483647"
	const key = "auxiliary.service.wsMaxClients"
	for _, tt := range []struct {
		name, values, want string
		// args are extra helm flags; --set parses integers as int64 rather
		// than the float64 a values file yields.
		args      []string
		wantError bool
	}{
		{name: "default", want: "100"},
		{name: "zero is unlimited", values: "0", want: "0"},
		{name: "null keeps default", values: "null", want: "100"},
		{name: "empty string keeps default", values: `""`, want: "100"},
		{name: "blank string keeps default", values: `"  "`, want: "100"},
		{name: "large value renders as an integer", values: "1000000", want: "1000000"},
		{name: "whole float", values: "1.0", want: "1"},
		{name: "numeric string", values: `"7"`, want: "7"},
		{name: "max int32", values: "2147483647", want: "2147483647"},
		{name: "negative", values: "-1", wantError: true},
		{name: "negative string", values: `"-1"`, wantError: true},
		{name: "fraction", values: "0.5", wantError: true},
		{name: "non-numeric string", values: `"abc"`, wantError: true},
		{name: "boolean", values: "false", wantError: true},
		{name: "overflow", values: "2147483648", wantError: true},
		{name: "overflow string", values: `"99999999999"`, wantError: true},
		{name: "--set zero is unlimited", args: []string{"--set", key + "=0"}, want: "0"},
		{name: "--set integer", args: []string{"--set", key + "=250"}, want: "250"},
		{name: "--set negative", args: []string{"--set", key + "=-1"}, wantError: true},
		{name: "--set overflow", args: []string{"--set", key + "=2147483648"}, wantError: true},
		{name: "--set boolean", args: []string{"--set", key + "=false"}, wantError: true},
		{name: "--set-string numeric", args: []string{"--set-string", key + "=5"}, want: "5"},
		{name: "--set-string empty keeps default", args: []string{"--set-string", key + "="}, want: "100"},
		{name: "--set-string non-numeric", args: []string{"--set-string", key + "=abc"}, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := ""
			if tt.values != "" {
				values = "auxiliary:\n  service:\n    wsMaxClients: " + tt.values + "\n"
			}
			args := append([]string{"template", "test", ".", "--namespace", "operator-system", "--values", "-"}, tt.args...)
			cmd := exec.Command("helm", args...)
			cmd.Stdin = strings.NewReader(values)
			out, err := cmd.CombinedOutput()
			if tt.wantError {
				if err == nil || !strings.Contains(string(out), wantError) {
					t.Fatalf("expected %q error, got %v: %s", wantError, err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			if got := wsMaxClientsEnv(t, out); got != tt.want {
				t.Fatalf("BLOODRAVEN_WS_MAX_CLIENTS = %q, want %q", got, tt.want)
			}
		})
	}
}

// wsMaxClientsEnv returns the operator container's BLOODRAVEN_WS_MAX_CLIENTS
// from rendered chart output.
func wsMaxClientsEnv(t *testing.T, rendered []byte) string {
	t.Helper()
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered), 4096)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		var meta struct{ Kind string }
		if err := json.Unmarshal(raw, &meta); err != nil {
			t.Fatal(err)
		}
		if meta.Kind != "Deployment" {
			continue
		}
		var deployment appsv1.Deployment
		if err := json.Unmarshal(raw, &deployment); err != nil {
			t.Fatal(err)
		}
		for _, c := range deployment.Spec.Template.Spec.Containers {
			for _, env := range c.Env {
				if env.Name == "BLOODRAVEN_WS_MAX_CLIENTS" {
					return env.Value
				}
			}
		}
	}
	t.Fatal("BLOODRAVEN_WS_MAX_CLIENTS not found in the rendered Deployment")
	return ""
}
