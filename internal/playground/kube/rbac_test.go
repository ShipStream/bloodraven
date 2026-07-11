package kube

import (
	"reflect"
	"sort"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

// verbsFor returns the (sorted) verbs granted for exactly the (apiGroup,
// resource) pair across a rule set, so a test can assert an effective grant
// regardless of how the rules were split.
func verbsFor(rules []rbacv1.PolicyRule, apiGroup, resource string) []string {
	set := map[string]bool{}
	for _, r := range rules {
		if rbacContains(r.APIGroups, apiGroup) && rbacContains(r.Resources, resource) {
			for _, v := range r.Verbs {
				set[v] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func TestRemoveVerbsForResource_DedicatedRule(t *testing.T) {
	// The live chart role has a dedicated single-resource rule for
	// mysqlfailovergroups/status.
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{"shipstream.io"}, Resources: []string{"mysqlfailovergroups/status"}, Verbs: []string{"get", "patch", "update"}},
		{APIGroups: []string{"externaldns.k8s.io"}, Resources: []string{"dnsendpoints"}, Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
	}
	got := RemoveVerbsForResource(rules, "shipstream.io", "mysqlfailovergroups/status", []string{"patch", "update"})

	if v := verbsFor(got, "shipstream.io", "mysqlfailovergroups/status"); !reflect.DeepEqual(v, []string{"get"}) {
		t.Errorf("mysqlfailovergroups/status verbs = %v, want [get]", v)
	}
	// The unrelated dnsendpoints rule must be untouched.
	if v := verbsFor(got, "externaldns.k8s.io", "dnsendpoints"); !reflect.DeepEqual(v, []string{"create", "delete", "get", "list", "patch", "update", "watch"}) {
		t.Errorf("dnsendpoints verbs changed: %v", v)
	}
}

func TestRemoveVerbsForResource_GroupedRuleSplits(t *testing.T) {
	// The generated role folds all /status subresources into one rule.
	// Removing patch/update from ONLY mysqlfailovergroups/status must
	// preserve them for the sibling resources.
	rules := []rbacv1.PolicyRule{
		{
			APIGroups: []string{"shipstream.io"},
			Resources: []string{"mysqlbackups/status", "mysqlbackupverifications/status", "mysqlfailovergroups/status", "mysqlstandbyclusters/status"},
			Verbs:     []string{"get", "patch", "update"},
		},
	}
	got := RemoveVerbsForResource(rules, "shipstream.io", "mysqlfailovergroups/status", []string{"patch", "update"})

	if v := verbsFor(got, "shipstream.io", "mysqlfailovergroups/status"); !reflect.DeepEqual(v, []string{"get"}) {
		t.Errorf("mysqlfailovergroups/status verbs = %v, want [get]", v)
	}
	for _, sibling := range []string{"mysqlbackups/status", "mysqlbackupverifications/status", "mysqlstandbyclusters/status"} {
		if v := verbsFor(got, "shipstream.io", sibling); !reflect.DeepEqual(v, []string{"get", "patch", "update"}) {
			t.Errorf("sibling %s verbs = %v, want [get patch update]", sibling, v)
		}
	}
}

func TestRemoveVerbsForResource_DNSEndpointsKeepsReadVerbs(t *testing.T) {
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{"externaldns.k8s.io"}, Resources: []string{"dnsendpoints"}, Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
	}
	got := RemoveVerbsForResource(rules, "externaldns.k8s.io", "dnsendpoints", []string{"create", "delete", "patch", "update"})
	if v := verbsFor(got, "externaldns.k8s.io", "dnsendpoints"); !reflect.DeepEqual(v, []string{"get", "list", "watch"}) {
		t.Errorf("dnsendpoints verbs = %v, want [get list watch]", v)
	}
}

func TestRemoveVerbsForResource_UnknownResourceIsNoOp(t *testing.T) {
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{"shipstream.io"}, Resources: []string{"mysqlfailovergroups/status"}, Verbs: []string{"get", "patch", "update"}},
	}
	got := RemoveVerbsForResource(rules, "shipstream.io", "does-not-exist/status", []string{"patch"})
	if !reflect.DeepEqual(got, rules) {
		t.Errorf("unknown resource must be a no-op; got %#v", got)
	}
	// Also a matching resource in the wrong group is a no-op.
	got2 := RemoveVerbsForResource(rules, "other.io", "mysqlfailovergroups/status", []string{"patch"})
	if !reflect.DeepEqual(got2, rules) {
		t.Errorf("wrong apiGroup must be a no-op; got %#v", got2)
	}
}

func TestRemoveVerbsForResource_EmptyingDropsFragment(t *testing.T) {
	// Removing every verb from a single-resource rule drops the rule
	// (an empty-verb rule is rejected by the API server).
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{"externaldns.k8s.io"}, Resources: []string{"dnsendpoints"}, Verbs: []string{"patch", "update"}},
	}
	got := RemoveVerbsForResource(rules, "externaldns.k8s.io", "dnsendpoints", []string{"patch", "update"})
	if len(got) != 0 {
		t.Errorf("fully-stripped single-resource rule should be dropped, got %#v", got)
	}
}

func TestCloneClusterRoleRules_IsDeepCopy(t *testing.T) {
	orig := []rbacv1.PolicyRule{
		{APIGroups: []string{"shipstream.io"}, Resources: []string{"mysqlfailovergroups/status"}, Verbs: []string{"get", "patch", "update"}},
	}
	clone := CloneClusterRoleRules(orig)
	// Mutating the clone must not touch the original.
	clone[0].Verbs[0] = "MUTATED"
	clone[0].Resources = append(clone[0].Resources, "extra")
	if orig[0].Verbs[0] != "get" {
		t.Errorf("original verb mutated via clone: %v", orig[0].Verbs)
	}
	if len(orig[0].Resources) != 1 {
		t.Errorf("original resources mutated via clone: %v", orig[0].Resources)
	}
	if CloneClusterRoleRules(nil) != nil {
		t.Error("CloneClusterRoleRules(nil) should be nil")
	}
}
