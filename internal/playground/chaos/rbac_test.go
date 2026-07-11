package chaos

import (
	"reflect"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

func TestRemovedRBACGrantsRestoresOnlyDeniedPair(t *testing.T) {
	original := []rbacv1.PolicyRule{{
		APIGroups: []string{"shipstream.io"},
		Resources: []string{"mysqlfailovergroups/status", "mysqlbackups/status"},
		Verbs:     []string{"get", "patch", "update"},
	}}
	got := removedRBACGrants(original, []VerbDenial{{
		APIGroup: "shipstream.io", Resource: "mysqlfailovergroups/status", Verbs: []string{"patch", "update"},
	}})
	want := []rbacv1.PolicyRule{{
		APIGroups: []string{"shipstream.io"},
		Resources: []string{"mysqlfailovergroups/status"},
		Verbs:     []string{"patch", "update"},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("removed grants = %#v, want %#v", got, want)
	}
}
