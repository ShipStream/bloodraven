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

func TestBackupGenerateNameTruncatesLongPrefix(t *testing.T) {
	long := strings.Repeat("a", 200)
	got := backupGenerateName(long, long, "manual")
	if len(got) > 253-6 {
		t.Errorf("GenerateName length = %d, want <= %d", len(got), 253-6)
	}
	if !strings.HasSuffix(got, "-") {
		t.Errorf("GenerateName must end in '-'; got %q", got)
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
