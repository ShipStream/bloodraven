package bloodraven_test

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// TestCRDCheckChart pins the --crd-check argument the chart passes
// (cmd/bloodraven/crd_check.go), also for values that have no crdCheck key:
// `helm upgrade --reuse-values` from a release before v1.8.0.
func TestCRDCheckChart(t *testing.T) {
	requireHelm(t)
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{name: "default", want: "--crd-check=enforce"},
		{name: "break-glass", args: []string{"--set", "crdCheck=warn"}, want: "--crd-check=warn"},
		{name: "values of a release before v1.8.0", args: []string{"--set", "crdCheck=null"}, want: "--crd-check=enforce"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"template", "test", ".", "--namespace", "operator-system"}, tt.args...)
			out, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			if !bytes.Contains(out, []byte("- "+tt.want+"\n")) {
				t.Fatalf("the operator's args do not carry %s:\n%s", tt.want, out)
			}
		})
	}
}

// renderedClusterRole renders the chart and returns the operator's
// ClusterRole.
func renderedClusterRole(t *testing.T) rbacv1.ClusterRole {
	t.Helper()
	out, err := exec.Command("helm", "template", "test", ".", "--namespace", "operator-system").CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v: %s", err, out)
	}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	for {
		var role rbacv1.ClusterRole
		if err := dec.Decode(&role); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode the rendered chart: %v", err)
		}
		if role.Kind == "ClusterRole" {
			return role
		}
	}
	t.Fatal("the chart renders no ClusterRole")
	return rbacv1.ClusterRole{}
}

// grants flattens rules into "group/resource[/name] verb" permissions.
func grants(rules []rbacv1.PolicyRule) map[string]bool {
	out := map[string]bool{}
	for _, rule := range rules {
		names := rule.ResourceNames
		if len(names) == 0 {
			names = []string{""}
		}
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				for _, name := range names {
					for _, verb := range rule.Verbs {
						key := group + "/" + resource
						if name != "" {
							key += "/" + name
						}
						out[key+" "+verb] = true
					}
				}
			}
		}
	}
	return out
}

// D5 and C-5b: the operator holds no write permission on
// CustomResourceDefinitions. Applying CRDs and trimming
// status.storedVersions is the administrator's job, with the
// bloodraven-storage-migrator role.
func TestOperatorClusterRoleDoesNotWriteCRDs(t *testing.T) {
	requireHelm(t)
	role := renderedClusterRole(t)
	found := false
	for _, rule := range role.Rules {
		touches := false
		for _, resource := range rule.Resources {
			touches = touches || resource == "*" || strings.HasPrefix(resource, "customresourcedefinitions")
		}
		crdGroup := false
		for _, group := range rule.APIGroups {
			crdGroup = crdGroup || group == "*" || group == "apiextensions.k8s.io"
		}
		if !touches || !crdGroup {
			continue
		}
		found = true
		if len(rule.Verbs) != 1 || rule.Verbs[0] != "get" || len(rule.ResourceNames) != 5 ||
			len(rule.Resources) != 1 || rule.Resources[0] != "customresourcedefinitions" {
			t.Errorf("the operator's rule on CustomResourceDefinitions is %+v, want get on the five Bloodraven CRDs by name and nothing else", rule)
		}
	}
	if !found {
		t.Error("the operator cannot read its CRDs: the check at start needs get on the five by name")
	}
}

// C-5: the chart's ClusterRole is written by hand and config/rbac/role.yaml
// is generated from the kubebuilder markers. The chart must grant everything
// the markers ask for, or a reconciler fails with Forbidden on a Helm
// install only.
func TestChartClusterRoleCoversGeneratedRole(t *testing.T) {
	requireHelm(t)
	raw, err := os.ReadFile("../../config/rbac/role.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var generated rbacv1.ClusterRole
	if err := yaml.Unmarshal(raw, &generated); err != nil {
		t.Fatal(err)
	}
	chart := grants(renderedClusterRole(t).Rules)
	var missing []string
	for grant := range grants(generated.Rules) {
		if !chart[grant] {
			missing = append(missing, grant)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("charts/bloodraven/templates/clusterrole.yaml does not grant what config/rbac/role.yaml (the kubebuilder markers) asks for:\n  %s",
			strings.Join(missing, "\n  "))
	}
}
