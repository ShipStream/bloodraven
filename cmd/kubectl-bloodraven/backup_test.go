package main

import (
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestBuildBackupCR(t *testing.T) {
	tests := []struct {
		name        string
		group       string
		profile     string
		sourceSite  string
		triggeredBy string
		wantPrefix  string
		wantOverride bool
	}{
		{
			name:       "manual backup without source override",
			group:      "orders",
			profile:    "nightly",
			wantPrefix: "orders-nightly-",
		},
		{
			name:         "manual backup with source override",
			group:        "orders",
			profile:      "ondemand",
			sourceSite:   "pdx",
			wantPrefix:   "orders-ondemand-",
			wantOverride: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := buildBackupCR("default", tc.group, tc.profile, tc.sourceSite, tc.triggeredBy)
			if b.Namespace != "default" {
				t.Errorf("namespace = %q, want default", b.Namespace)
			}
			if !strings.HasPrefix(b.GenerateName, tc.wantPrefix) {
				t.Errorf("GenerateName = %q, want prefix %q", b.GenerateName, tc.wantPrefix)
			}
			if b.Spec.FailoverGroupRef.Name != tc.group {
				t.Errorf("FailoverGroupRef.Name = %q, want %q", b.Spec.FailoverGroupRef.Name, tc.group)
			}
			if b.Spec.ProfileName != tc.profile {
				t.Errorf("ProfileName = %q, want %q", b.Spec.ProfileName, tc.profile)
			}
			if tc.wantOverride && b.Spec.SourceSiteOverride != tc.sourceSite {
				t.Errorf("SourceSiteOverride = %q, want %q", b.Spec.SourceSiteOverride, tc.sourceSite)
			}
			if !tc.wantOverride && b.Spec.SourceSiteOverride != "" {
				t.Errorf("SourceSiteOverride = %q, want empty", b.Spec.SourceSiteOverride)
			}
			if got := b.Labels["shipstream.io/failover-group"]; got != tc.group {
				t.Errorf("failover-group label = %q, want %q", got, tc.group)
			}
			if got := b.Labels["shipstream.io/backup-profile"]; got != tc.profile {
				t.Errorf("backup-profile label = %q, want %q", got, tc.profile)
			}
			if got := b.Labels["app.kubernetes.io/managed-by"]; got != "bloodraven" {
				t.Errorf("managed-by label = %q, want bloodraven", got)
			}
		})
	}
}

func TestBackupGenerateName(t *testing.T) {
	const maxPrefix = 253 - 6

	cases := []struct {
		name    string
		group   string
		profile string
		// wantLen is the exact expected length (0 means "no exact
		// constraint; just check the upper bound").
		wantLen        int
		wantSuffixDash bool
		wantPrefix     string // when set, asserts strings.HasPrefix
	}{
		{
			name:           "short inputs are returned verbatim",
			group:          "orders",
			profile:        "nightly",
			wantLen:        len("orders-nightly-"),
			wantSuffixDash: true,
			wantPrefix:     "orders-nightly-",
		},
		{
			name:           "prefix exactly at the budget is not trimmed",
			group:          strings.Repeat("g", maxPrefix/2-1),
			profile:        strings.Repeat("p", maxPrefix-(maxPrefix/2-1)-2),
			wantSuffixDash: true,
		},
		{
			name:           "200/200 input is trimmed and ends in a single dash",
			group:          strings.Repeat("a", 200),
			profile:        strings.Repeat("b", 200),
			wantSuffixDash: true,
		},
		{
			name:           "trailing-dash collapse: trim that lands on a run of dashes yields a single trailing dash",
			group:          strings.Repeat("a", maxPrefix-4) + "----",
			profile:        "x",
			wantSuffixDash: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := backupGenerateName(tc.group, tc.profile)
			if len(got) > maxPrefix {
				t.Errorf("GenerateName length = %d, want <= %d (got %q)", len(got), maxPrefix, got)
			}
			if tc.wantLen != 0 && len(got) != tc.wantLen {
				t.Errorf("GenerateName length = %d, want exactly %d (got %q)", len(got), tc.wantLen, got)
			}
			if tc.wantSuffixDash {
				if !strings.HasSuffix(got, "-") {
					t.Errorf("GenerateName must end in '-'; got %q", got)
				}
				if strings.HasSuffix(got, "--") {
					t.Errorf("GenerateName must not end in '--'; got %q", got)
				}
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(got, tc.wantPrefix) {
				t.Errorf("GenerateName = %q, want prefix %q", got, tc.wantPrefix)
			}
		})
	}
}

func TestGenerateNameWithInfix(t *testing.T) {
	const maxPrefix = 253 - 6

	if got := generateNameWithInfix("orders", "nightly", ""); got != "orders-nightly-" {
		t.Errorf("backup form = %q, want %q", got, "orders-nightly-")
	}
	if got := generateNameWithInfix("orders", "nightly", "verify"); got != "orders-nightly-verify-" {
		t.Errorf("verify form = %q, want %q", got, "orders-nightly-verify-")
	}
	long := strings.Repeat("a", 200)
	got := generateNameWithInfix(long, long, "verify")
	if len(got) > maxPrefix {
		t.Errorf("verify form long input length = %d, want <= %d", len(got), maxPrefix)
	}
	if !strings.HasSuffix(got, "-") || strings.HasSuffix(got, "--") {
		t.Errorf("verify form must end in a single '-'; got %q", got)
	}
}

func TestGroupHasBackupProfile(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Backup: &v1alpha1.BackupSpec{
				Profiles: []v1alpha1.BackupProfile{
					{Name: "nightly"},
					{Name: "weekly"},
				},
			},
		},
	}
	if !groupHasBackupProfile(fg, "nightly") {
		t.Errorf("nightly should match")
	}
	if groupHasBackupProfile(fg, "monthly") {
		t.Errorf("monthly should not match")
	}
	if groupHasBackupProfile(&v1alpha1.MysqlFailoverGroup{}, "any") {
		t.Errorf("nil backup spec should never match")
	}
}
