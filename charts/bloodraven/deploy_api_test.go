package bloodraven_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestDeploymentDashboard(t *testing.T) {
	raw, err := os.ReadFile("dashboards/failover.json")
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		Panels []struct {
			ID      int
			Title   string
			GridPos struct{ X, Y, W, H int }
			Targets []struct{ Expr string }
		}
	}
	if err := json.Unmarshal(raw, &dashboard); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Active deployment leases":              "bloodraven_deploy_leases_active",
		"Deployment lease revocations per hour": "bloodraven_deploy_lease_revocations_total",
		"Planned failover deferrals per hour":   "bloodraven_planned_failovers_deferred_total",
	}
	ids := map[int]bool{}
	for i, panel := range dashboard.Panels {
		if ids[panel.ID] {
			t.Fatalf("duplicate panel ID %d", panel.ID)
		}
		ids[panel.ID] = true
		p := panel.GridPos
		if p.X < 0 || p.Y < 0 || p.W <= 0 || p.H <= 0 || p.X+p.W > 24 {
			t.Fatalf("invalid grid for %s", panel.Title)
		}
		for _, other := range dashboard.Panels[:i] {
			o := other.GridPos
			if p.X < o.X+o.W && o.X < p.X+p.W && p.Y < o.Y+o.H && o.Y < p.Y+p.H {
				t.Fatalf("overlapping panels: %s and %s", panel.Title, other.Title)
			}
		}
		if metric := want[panel.Title]; metric != "" {
			if len(panel.Targets) != 1 || !strings.Contains(panel.Targets[0].Expr, metric+`{group=~"$group"}`) || strings.Contains(panel.Targets[0].Expr, "namespace=") {
				t.Fatalf("incorrect group-scoped query for %s", panel.Title)
			}
			delete(want, panel.Title)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing deployment panels: %v", want)
	}
}

func TestDeployAPIChart(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is required for chart render tests")
	}
	for _, tt := range []struct {
		name, values, wantError string
		enabled, policy         bool
		port                    int32
		audience                string
	}{
		{name: "disabled defaults"},
		{name: "TLS required", values: "auxiliary:\n  deployAPI:\n    enabled: true\n", wantError: "requires auxiliary.escrowTLS.enabled=true"},
		{name: "leader election required", values: "leaderElection:\n  enabled: false\nauxiliary:\n  deployAPI:\n    enabled: true\n  escrowTLS:\n    enabled: true\n    existingSecret: test-tls\n", wantError: "requires leaderElection.enabled=true"},
		{name: "secret required", values: "auxiliary:\n  deployAPI:\n    enabled: true\n  escrowTLS:\n    enabled: true\n", wantError: "auxiliary.escrowTLS.existingSecret is required"},
		{name: "policy requires API", values: "auxiliary:\n  deployAPI:\n    networkPolicy:\n      enabled: true\n", wantError: "requires auxiliary.deployAPI.enabled=true"},
		{name: "empty audience refused", values: "auxiliary:\n  deployAPI:\n    enabled: true\n    audience: ''\n  escrowTLS:\n    enabled: true\n    existingSecret: test-tls\n", wantError: "audience must not be empty"},
		{name: "enabled without policy", values: "auxiliary:\n  deployAPI:\n    enabled: true\n  escrowTLS:\n    enabled: true\n    existingSecret: test-tls\n", enabled: true, port: 8443, audience: "bloodraven-deploy"},
		{name: "enabled with policy", values: "auxiliary:\n  deployAPI:\n    enabled: true\n    networkPolicy:\n      enabled: true\n  escrowTLS:\n    enabled: true\n    existingSecret: test-tls\n", enabled: true, policy: true, port: 8443, audience: "bloodraven-deploy"},
		{name: "custom port audience and peer", values: "auxiliary:\n  deployAPI:\n    enabled: true\n    audience: custom-deploy\n    networkPolicy:\n      enabled: true\n      additionalIngress:\n        - from:\n            - ipBlock:\n                cidr: 192.0.2.0/24\n          ports:\n            - port: 9443\n              protocol: TCP\n  escrowTLS:\n    enabled: true\n    port: 9443\n    existingSecret: test-tls\n", enabled: true, policy: true, port: 9443, audience: "custom-deploy"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("helm", "template", "test", ".", "--namespace", "operator-system", "--values", "-")
			cmd.Stdin = strings.NewReader(tt.values)
			out, err := cmd.CombinedOutput()
			if tt.wantError != "" {
				if err == nil || !strings.Contains(string(out), tt.wantError) {
					t.Fatalf("expected %q error, got %v: %s", tt.wantError, err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			var deployment appsv1.Deployment
			var policy networkingv1.NetworkPolicy
			var role rbacv1.ClusterRole
			var auxiliary corev1.Service
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
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
				var target any
				switch meta.Kind {
				case "Deployment":
					target = &deployment
				case "NetworkPolicy":
					target = &policy
				case "ClusterRole":
					target = &role
				case "Service":
					var service corev1.Service
					if err := json.Unmarshal(raw, &service); err != nil {
						t.Fatal(err)
					}
					if service.Name == "bloodraven" {
						auxiliary = service
					}
				}
				if target != nil {
					if err := json.Unmarshal(raw, target); err != nil {
						t.Fatal(err)
					}
				}
			}
			if deployment.Name == "" || role.Name == "" {
				t.Fatal("missing operator Deployment or ClusterRole")
			}
			env := map[string]string{}
			for _, variable := range deployment.Spec.Template.Spec.Containers[0].Env {
				env[variable.Name] = variable.Value
			}
			if (env["BLOODRAVEN_DEPLOY_API_ENABLED"] == "true") != tt.enabled || env["BLOODRAVEN_DEPLOY_API_AUDIENCE"] != tt.audience {
				t.Fatalf("incorrect deployment API env: %v", env)
			}
			if (policy.Name != "") != tt.policy {
				t.Fatalf("policy rendered = %t, want %t", policy.Name != "", tt.policy)
			}
			for _, want := range []rbacv1.PolicyRule{
				{APIGroups: []string{"authentication.k8s.io"}, Resources: []string{"tokenreviews"}, Verbs: []string{"create"}},
				{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
			} {
				found := false
				for _, rule := range role.Rules {
					found = found || reflect.DeepEqual(rule, want)
				}
				if !found {
					t.Fatalf("missing RBAC rule: %+v", want)
				}
			}
			if tt.enabled {
				found := false
				for _, port := range auxiliary.Spec.Ports {
					found = found || (port.Port == tt.port && port.TargetPort.StrVal == "escrow-tls")
				}
				if !found {
					t.Fatalf("missing shared TLS Service port %d", tt.port)
				}
			}
			if !tt.policy {
				return
			}
			if policy.Namespace != deployment.Namespace || !reflect.DeepEqual(policy.Spec.PodSelector, *deployment.Spec.Selector) || !reflect.DeepEqual(policy.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) {
				t.Fatal("policy must select this operator's pods and only isolate ingress")
			}
			open := policy.Spec.Ingress[0]
			if len(open.From) != 0 || len(open.Ports) != 3 {
				t.Fatal("must preserve existing metrics, health, and auxiliary ingress")
			}
			for i, port := range []int{8080, 8081, 8082} {
				if *open.Ports[i].Port != intstr.FromInt(port) || *open.Ports[i].Protocol != corev1.ProtocolTCP {
					t.Fatalf("missing preserved port %d", port)
				}
			}
			tls := policy.Spec.Ingress[1]
			if len(tls.Ports) != 1 || tls.Ports[0].Port.IntVal != tt.port || len(tls.From) != 2 {
				t.Fatalf("incorrect TLS ingress: %+v", tls)
			}
			deploy, escrow := tls.From[0], tls.From[1]
			if deploy.NamespaceSelector == nil || deploy.PodSelector == nil || deploy.NamespaceSelector.MatchLabels["shipstream.io/mysql-client"] != "true" || deploy.PodSelector.MatchLabels["bloodraven.shipstream.io/deploy-client"] != "true" {
				t.Fatal("deployment namespace AND pod selectors must be in the same peer")
			}
			if escrow.NamespaceSelector == nil || len(escrow.NamespaceSelector.MatchLabels) != 0 || escrow.PodSelector == nil || escrow.PodSelector.MatchLabels["app.kubernetes.io/name"] != "mysql" || escrow.PodSelector.MatchLabels["app.kubernetes.io/managed-by"] != "bloodraven" {
				t.Fatal("must preserve managed MySQL escrow ingress across namespaces")
			}
			if tt.port == 9443 {
				if len(policy.Spec.Ingress) != 3 || policy.Spec.Ingress[2].From[0].IPBlock.CIDR != "192.0.2.0/24" || policy.Spec.Ingress[2].Ports[0].Port.IntVal != tt.port {
					t.Fatal("additional ingress rule not preserved")
				}
			} else if len(policy.Spec.Ingress) != 2 {
				t.Fatal("unexpected additional ingress")
			}
		})
	}
}
