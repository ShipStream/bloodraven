package kube

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetClusterRole fetches a ClusterRole by name.
func (c *Client) GetClusterRole(ctx context.Context, name string) (*rbacv1.ClusterRole, error) {
	return c.Kubernetes.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
}

// UpdateClusterRole writes a full ClusterRole update. Chaos scenarios use
// this to remove and later restore selected verbs on the operator's bound
// role; a JSON-Patch on the rules array is fragile because rule ordering is
// not stable, so a whole-object update against a captured original is the
// safe restore path.
func (c *Client) UpdateClusterRole(ctx context.Context, cr *rbacv1.ClusterRole) error {
	_, err := c.Kubernetes.RbacV1().ClusterRoles().Update(ctx, cr, metav1.UpdateOptions{})
	return err
}

// ListClusterRoleBindings returns every ClusterRoleBinding in the cluster.
func (c *Client) ListClusterRoleBindings(ctx context.Context) (*rbacv1.ClusterRoleBindingList, error) {
	return c.Kubernetes.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
}

// ResolveBoundClusterRole finds the ClusterRole a ServiceAccount is bound to
// via a ClusterRoleBinding whose roleRef is a ClusterRole. Returns the role
// name and the binding name (for capture/logging). Errors if no such binding
// exists — the RBAC-denial scenarios refuse to run rather than mutate a role
// the operator is not actually using.
//
// The live playground binds ServiceAccount bloodraven-playground/bloodraven
// to ClusterRole bloodraven via ClusterRoleBinding bloodraven; this resolves
// it dynamically so a renamed Helm release still works.
func (c *Client) ResolveBoundClusterRole(ctx context.Context, saNamespace, saName string) (roleName, bindingName string, err error) {
	crbs, err := c.ListClusterRoleBindings(ctx)
	if err != nil {
		return "", "", fmt.Errorf("list clusterrolebindings: %w", err)
	}
	for i := range crbs.Items {
		crb := &crbs.Items[i]
		if crb.RoleRef.Kind != "ClusterRole" {
			continue
		}
		for _, s := range crb.Subjects {
			if s.Kind == "ServiceAccount" && s.Name == saName && s.Namespace == saNamespace {
				return crb.RoleRef.Name, crb.Name, nil
			}
		}
	}
	return "", "", fmt.Errorf("no ClusterRoleBinding binds ServiceAccount %s/%s to a ClusterRole", saNamespace, saName)
}

// CloneClusterRoleRules deep-copies a slice of PolicyRules so a captured
// "original" cannot be mutated by a later in-place edit of the live object.
// Chaos RBAC injection saves the result and restores it verbatim in cleanup.
func CloneClusterRoleRules(rules []rbacv1.PolicyRule) []rbacv1.PolicyRule {
	if rules == nil {
		return nil
	}
	out := make([]rbacv1.PolicyRule, len(rules))
	for i := range rules {
		out[i] = *rules[i].DeepCopy()
	}
	return out
}

// RemoveVerbsForResource returns a new rule set with the given verbs removed
// only from the (apiGroup, resource) pair. Rules that grant the resource
// alongside OTHER resources are split so those other resources keep their
// original verbs — this is what lets scenario 32 strip patch/update from
// shipstream.io/mysqlfailovergroups/status while preserving the same verbs on
// mysqlbackups/status, and scenario 38 strip write verbs from
// externaldns.k8s.io/dnsendpoints without touching any other rule.
//
// Rules that match no pair are returned unchanged (deep-copied). If removing
// the verbs empties a matched rule fragment's verb list, that fragment is
// dropped entirely — the API server rejects an empty-verb rule. A pair that
// matches nothing is a silent no-op; callers assert on the returned diff.
func RemoveVerbsForResource(rules []rbacv1.PolicyRule, apiGroup, resource string, verbs []string) []rbacv1.PolicyRule {
	drop := make(map[string]bool, len(verbs))
	for _, v := range verbs {
		drop[v] = true
	}
	var out []rbacv1.PolicyRule
	for i := range rules {
		r := rules[i]
		if !rbacRuleMatches(r, apiGroup, resource) {
			out = append(out, *r.DeepCopy())
			continue
		}
		if len(r.Resources) == 1 {
			// Single-resource rule: reduce its verbs in place.
			reduced := rbacFilterVerbs(r.Verbs, drop)
			if len(reduced) == 0 {
				continue
			}
			nr := *r.DeepCopy()
			nr.Verbs = reduced
			out = append(out, nr)
			continue
		}
		// Multi-resource rule: split into an "others keep original verbs"
		// fragment and a "target resource with reduced verbs" fragment.
		others := make([]string, 0, len(r.Resources)-1)
		for _, res := range r.Resources {
			if res != resource {
				others = append(others, res)
			}
		}
		if len(others) > 0 {
			keep := *r.DeepCopy()
			keep.Resources = others
			out = append(out, keep)
		}
		reduced := rbacFilterVerbs(r.Verbs, drop)
		if len(reduced) > 0 {
			frag := *r.DeepCopy()
			frag.Resources = []string{resource}
			frag.Verbs = reduced
			out = append(out, frag)
		}
	}
	return out
}

func rbacRuleMatches(r rbacv1.PolicyRule, apiGroup, resource string) bool {
	return rbacContains(r.APIGroups, apiGroup) && rbacContains(r.Resources, resource)
}

func rbacContains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func rbacFilterVerbs(verbs []string, drop map[string]bool) []string {
	var out []string
	for _, v := range verbs {
		if !drop[v] {
			out = append(out, v)
		}
	}
	return out
}
