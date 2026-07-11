package chaos

import (
	"reflect"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

func TestRemovedRBACGrantsRestoresOnlyDeniedPair(t *testing.T) {
	statusDenial := VerbDenial{APIGroup: "shipstream.io", Resource: "mysqlfailovergroups/status", Verbs: []string{"patch", "update"}}
	tests := []struct {
		name     string
		original []rbacv1.PolicyRule
		denials  []VerbDenial
		want     []rbacv1.PolicyRule
	}{
		{
			name: "single rule narrowed to denied pair",
			original: []rbacv1.PolicyRule{{
				APIGroups: []string{"shipstream.io"},
				Resources: []string{"mysqlfailovergroups/status", "mysqlbackups/status"},
				Verbs:     []string{"get", "patch", "update"},
			}},
			denials: []VerbDenial{statusDenial},
			want: []rbacv1.PolicyRule{{
				APIGroups: []string{"shipstream.io"},
				Resources: []string{"mysqlfailovergroups/status"},
				Verbs:     []string{"patch", "update"},
			}},
		},
		{
			name: "multiple matching rules and denials",
			original: []rbacv1.PolicyRule{
				{APIGroups: []string{"shipstream.io"}, Resources: []string{"mysqlfailovergroups/status"}, Verbs: []string{"patch"}},
				{APIGroups: []string{"shipstream.io"}, Resources: []string{"mysqlfailovergroups/status", "dnsendpoints"}, Verbs: []string{"update"}},
			},
			denials: []VerbDenial{statusDenial, {APIGroup: "shipstream.io", Resource: "dnsendpoints", Verbs: []string{"update"}}},
			want: []rbacv1.PolicyRule{
				{APIGroups: []string{"shipstream.io"}, Resources: []string{"mysqlfailovergroups/status"}, Verbs: []string{"patch"}},
				{APIGroups: []string{"shipstream.io"}, Resources: []string{"mysqlfailovergroups/status"}, Verbs: []string{"update"}},
				{APIGroups: []string{"shipstream.io"}, Resources: []string{"dnsendpoints"}, Verbs: []string{"update"}},
			},
		},
		{
			name: "narrows API groups and preserves resource names",
			original: []rbacv1.PolicyRule{{
				APIGroups:     []string{"shipstream.io", "apps"},
				Resources:     []string{"mysqlfailovergroups/status"},
				Verbs:         []string{"get", "patch", "update"},
				ResourceNames: []string{"playground"},
			}},
			denials: []VerbDenial{statusDenial},
			want: []rbacv1.PolicyRule{{
				APIGroups:     []string{"shipstream.io"},
				Resources:     []string{"mysqlfailovergroups/status"},
				Verbs:         []string{"patch", "update"},
				ResourceNames: []string{"playground"},
			}},
		},
		{
			name:     "no matching rule",
			original: []rbacv1.PolicyRule{{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"update"}}},
			denials:  []VerbDenial{statusDenial},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := removedRBACGrants(tc.original, tc.denials)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("removed grants = %#v, want %#v", got, tc.want)
			}
			if len(got) > 0 && len(got[0].ResourceNames) > 0 {
				got[0].ResourceNames[0] = "mutated"
				if tc.original[0].ResourceNames[0] == "mutated" {
					t.Fatal("removed grant aliases original ResourceNames")
				}
			}
		})
	}
}
