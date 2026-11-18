package storagemigration

import (
	"os"
	"sort"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// The example ClusterRole grants what the command checks for, and nothing
// else: a permission the command gains must be added to the example, and
// the example must not hand out more than the command uses.
func TestExampleClusterRoleMatchesRequiredPermissions(t *testing.T) {
	raw, err := os.ReadFile("../../examples/storage-migrator-clusterrole.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.UnmarshalStrict(raw, &role); err != nil {
		t.Fatal(err)
	}
	if role.Name != "bloodraven-storage-migrator" {
		t.Fatalf("the example role is named %q", role.Name)
	}
	granted := map[string]bool{}
	for _, rule := range role.Rules {
		names := rule.ResourceNames
		if len(names) == 0 {
			names = []string{""}
		}
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				for _, name := range names {
					for _, verb := range rule.Verbs {
						p := Permission{Group: group, Resource: resource, Name: name, Verb: verb}
						if res, sub, ok := strings.Cut(resource, "/"); ok {
							p.Resource, p.Subresource = res, sub
						}
						granted[p.String()] = true
					}
				}
			}
		}
	}
	required := map[string]bool{}
	for _, p := range RequiredPermissions() {
		required[p.String()] = true
	}
	var missing, extra []string
	for p := range required {
		if !granted[p] {
			missing = append(missing, p)
		}
	}
	for p := range granted {
		if !required[p] {
			extra = append(extra, p)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing)+len(extra) > 0 {
		t.Fatalf("examples/storage-migrator-clusterrole.yaml does not match RequiredPermissions:\n  missing: %v\n  not needed: %v", missing, extra)
	}
}
