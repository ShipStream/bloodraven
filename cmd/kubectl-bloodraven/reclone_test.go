package main

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestAutoPullDivergentGtidPrefix(t *testing.T) {
	tests := []struct {
		name string
		site string
		fg   *v1alpha1.MysqlFailoverGroup
		want string
	}{
		{
			name: "site with divergent GTID longer than 12 chars is truncated",
			site: "iad",
			fg: &v1alpha1.MysqlFailoverGroup{
				Status: v1alpha1.MysqlFailoverGroupStatus{
					Sites: []v1alpha1.SiteStatus{
						{Name: "iad", DivergentGtid: "abcdef0123456789aaaa:1-7"},
					},
				},
			},
			want: "abcdef012345",
		},
		{
			name: "GTID shorter than minRecloneGtidPrefix returns empty so the caller can refuse",
			site: "iad",
			fg: &v1alpha1.MysqlFailoverGroup{
				Status: v1alpha1.MysqlFailoverGroupStatus{
					Sites: []v1alpha1.SiteStatus{
						{Name: "iad", DivergentGtid: "ab12"},
					},
				},
			},
			want: "",
		},
		{
			name: "GTID exactly minRecloneGtidPrefix-long is preserved verbatim",
			site: "iad",
			fg: &v1alpha1.MysqlFailoverGroup{
				Status: v1alpha1.MysqlFailoverGroupStatus{
					Sites: []v1alpha1.SiteStatus{
						{Name: "iad", DivergentGtid: "abcd1234"},
					},
				},
			},
			want: "abcd1234",
		},
		{
			name: "no divergent GTID returns empty so cold reclone is allowed",
			site: "pdx",
			fg: &v1alpha1.MysqlFailoverGroup{
				Status: v1alpha1.MysqlFailoverGroupStatus{
					Sites: []v1alpha1.SiteStatus{
						{Name: "pdx"},
					},
				},
			},
			want: "",
		},
		{
			name: "missing site returns empty",
			site: "lhr",
			fg: &v1alpha1.MysqlFailoverGroup{
				Status: v1alpha1.MysqlFailoverGroupStatus{
					Sites: []v1alpha1.SiteStatus{{Name: "iad", DivergentGtid: "abcdef0123456789"}},
				},
			},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := autoPullDivergentGtidPrefix(tc.fg, tc.site)
			if got != tc.want {
				t.Fatalf("autoPullDivergentGtidPrefix = %q, want %q", got, tc.want)
			}
		})
	}
}
