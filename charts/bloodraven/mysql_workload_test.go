package bloodraven_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestMySQLWorkloadDashboardDistribution(t *testing.T) {
	requireHelm(t)
	want, err := os.ReadFile("dashboards/mysql-workload.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []string{"false", "true"} {
		t.Run(enabled, func(t *testing.T) {
			cmd := exec.Command("helm", "template", "test", ".", "--namespace", "operator-system",
				"--set", "grafanaDashboards.enabled="+enabled, "--set", "grafanaDashboards.namespace=monitoring",
				"--show-only", "templates/grafana-dashboards.yaml")
			out, err := cmd.CombinedOutput()
			if enabled == "false" {
				// Helm reports that the template cannot be found if it emits no objects.
				if err == nil || !bytes.Contains(out, []byte("could not find template templates/grafana-dashboards.yaml")) {
					t.Fatalf("expected no dashboard templates, got %v: %s", err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			dashboards := map[string]string{}
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
			for {
				var cm corev1.ConfigMap
				if err := decoder.Decode(&cm); err != nil {
					if err == io.EOF {
						break
					}
					t.Fatal(err)
				}
				for name, data := range cm.Data {
					if cm.Namespace != "monitoring" || cm.Labels["grafana_dashboard"] != "1" || cm.Annotations["grafana_folder"] != "Bloodraven" {
						t.Fatalf("unexpected Grafana discovery metadata for %s", name)
					}
					dashboards[name] = data
				}
			}
			if got := dashboards["mysql-workload.json"]; got != strings.TrimSpace(string(want)) {
				t.Fatal("shipped workload dashboard differs from the source")
			}
			assertDashboardLink(t, dashboards["mysql-workload.json"], "/d/bloodraven-overview")
			assertDashboardLink(t, dashboards["overview.json"], "/d/bloodraven-mysql-workload")
		})
	}
}

func assertDashboardLink(t *testing.T, raw, url string) {
	t.Helper()
	var dashboard struct {
		Links []struct {
			URL         string
			KeepTime    bool
			IncludeVars bool
		}
	}
	if err := json.Unmarshal([]byte(raw), &dashboard); err != nil {
		t.Fatal(err)
	}
	for _, link := range dashboard.Links {
		if link.URL == url && link.KeepTime && link.IncludeVars {
			return
		}
	}
	t.Errorf("missing time- and filter-preserving dashboard link to %s", url)
}
