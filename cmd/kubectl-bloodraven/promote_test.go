package main

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestBuildPlannedFailoverValue(t *testing.T) {
	tests := []struct {
		name        string
		site        string
		maxLag      time.Duration
		want        string
		wantErr     bool
		wantErrText string
	}{
		{
			name: "site only",
			site: "pdx",
			want: "pdx",
		},
		{
			name:   "maxLagWait override only",
			site:   "pdx",
			maxLag: 30 * time.Second,
			want:   "pdx:maxLagWait=30s",
		},
		{
			name:        "empty site is rejected",
			site:        "",
			wantErr:     true,
			wantErrText: "site is required",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildPlannedFailoverValue(tc.site, tc.maxLag)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; value=%q", got)
				}
				if !strings.Contains(err.Error(), tc.wantErrText) {
					t.Fatalf("err = %q, want substring %q", err.Error(), tc.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("value = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGroupHasSite(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{
				{Name: "iad"},
				{Name: "pdx"},
			},
		},
	}
	if !groupHasSite(fg, "iad") {
		t.Errorf("iad should be present")
	}
	if !groupHasSite(fg, "pdx") {
		t.Errorf("pdx should be present")
	}
	if groupHasSite(fg, "lhr") {
		t.Errorf("lhr should not be present")
	}
}
