package main

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestBuildPlannedFailoverValue(t *testing.T) {
	dur := func(d time.Duration) *time.Duration { return &d }
	tests := []struct {
		name        string
		site        string
		maxLag      *time.Duration
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
			maxLag: dur(30 * time.Second),
			want:   "pdx:maxLagWait=30s",
		},
		{
			name:   "lower bound is accepted",
			site:   "pdx",
			maxLag: dur(v1alpha1.PlannedFailoverMaxLagWaitMin),
			want:   "pdx:maxLagWait=1s",
		},
		{
			name:   "upper bound is accepted and parses back",
			site:   "pdx",
			maxLag: dur(v1alpha1.PlannedFailoverMaxLagWaitMax),
			want:   "pdx:maxLagWait=1h0m0s",
		},
		{
			name:   "fractional seconds stay in the annotation charset",
			site:   "pdx",
			maxLag: dur(1500 * time.Millisecond),
			want:   "pdx:maxLagWait=1.5s",
		},
		{
			name:        "below the lower bound is refused",
			site:        "pdx",
			maxLag:      dur(500 * time.Millisecond),
			wantErr:     true,
			wantErrText: "--max-lag-wait must be between",
		},
		{
			name:        "above the upper bound is refused",
			site:        "pdx",
			maxLag:      dur(2 * time.Hour),
			wantErr:     true,
			wantErrText: "--max-lag-wait must be between",
		},
		{
			name:        "an explicit zero is refused, not dropped",
			site:        "pdx",
			maxLag:      dur(0),
			wantErr:     true,
			wantErrText: "--max-lag-wait must be between",
		},
		{
			name:        "a negative override is refused, not dropped",
			site:        "pdx",
			maxLag:      dur(-time.Second),
			wantErr:     true,
			wantErrText: "--max-lag-wait must be between",
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
			// The written value parses back to the requested duration.
			if _, raw, ok := strings.Cut(got, ":maxLagWait="); ok {
				if d, err := time.ParseDuration(raw); err != nil || d != *tc.maxLag {
					t.Fatalf("maxLagWait=%q parses to %v, %v; want %v", raw, d, err, *tc.maxLag)
				}
			}
		})
	}
}

func TestGroupHasSite(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{
				{Name: "iad", ServerID: 101},
				{Name: "pdx", ServerID: 102},
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
