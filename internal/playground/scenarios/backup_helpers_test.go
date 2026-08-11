package scenarios

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func TestBackupRunStamp(t *testing.T) {
	env := &runner.Env{StartTime: time.Date(2026, 6, 1, 2, 3, 4, 0, time.FixedZone("offset", -7*3600))}
	if got, want := backupRunStamp(env), "20260601t090304z"; got != want {
		t.Fatalf("backupRunStamp()=%q, want %q", got, want)
	}
}

func TestBackupProfileSpec(t *testing.T) {
	for _, tc := range []struct {
		name string
		pitr bool
	}{
		{name: "without PITR", pitr: false},
		{name: "with PITR", pitr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := backupProfileSpec("e2e/test/run", tc.pitr)
			if spec.Image == "" || spec.ActiveDeadlineSeconds == 0 || len(spec.Profiles) != 1 {
				t.Fatalf("backupProfileSpec returned incomplete spec: %+v", spec)
			}
			profile := spec.Profiles[0]
			if profile.Name != backupE2EProfile {
				t.Fatalf("profile name=%q, want %q", profile.Name, backupE2EProfile)
			}
			if profile.Storage.Type != v1alpha1.BackupStorageS3 || profile.Storage.S3 == nil {
				t.Fatalf("storage=%+v, want S3 config", profile.Storage)
			}
			if profile.Storage.S3.Bucket != backupE2EBucket || profile.Storage.S3.Prefix != "e2e/test/run" || profile.Storage.S3.EndpointURL != backupE2EEndpoint || profile.Storage.S3.CredentialsSecret != backupE2ECredsSecret {
				t.Fatalf("unexpected S3 config: %+v", profile.Storage.S3)
			}
			if (spec.PITR != nil) != tc.pitr {
				t.Fatalf("PITR presence=%v, want %v", spec.PITR != nil, tc.pitr)
			}
			if tc.pitr && (!spec.PITR.Enabled || spec.PITR.ProfileName != backupE2EProfile || spec.PITR.ArchivePollInterval == nil) {
				t.Fatalf("unexpected PITR config: %+v", spec.PITR)
			}
		})
	}
}

func TestQuoteSQLString(t *testing.T) {
	if got, want := quoteSQLString("s31-o'clock"), "'s31-o''clock'"; got != want {
		t.Fatalf("quoteSQLString()=%q, want %q", got, want)
	}
}

func TestConditionTrue(t *testing.T) {
	conds := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}, {Type: "Verified", Status: metav1.ConditionTrue}}
	if conditionTrue(conds, "Ready") {
		t.Fatalf("Ready should not be true")
	}
	if !conditionTrue(conds, "Verified") {
		t.Fatalf("Verified should be true")
	}
	if conditionTrue(conds, "Missing") {
		t.Fatalf("Missing condition should not be true")
	}
}

func TestConditionTrueForGeneration(t *testing.T) {
	conds := []metav1.Condition{{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: 4,
	}}
	tests := []struct {
		name       string
		condType   string
		generation int64
		want       bool
	}{
		{name: "stale", condType: "Ready", generation: 5, want: false},
		{name: "matching", condType: "Ready", generation: 4, want: true},
		{name: "missing type", condType: "Missing", generation: 4, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := conditionTrueForGeneration(conds, tt.condType, tt.generation); got != tt.want {
				t.Fatalf("conditionTrueForGeneration(%q, %d)=%v, want %v", tt.condType, tt.generation, got, tt.want)
			}
		})
	}
}

func TestBackupProfileRolloutReady(t *testing.T) {
	mfg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Generation: 5},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Conditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: 4,
			}},
		},
	}

	if !backupProfileRolloutReady(mfg, false) {
		t.Fatal("plain backup profile should not wait for an unrelated topology status rewrite")
	}
	if backupProfileRolloutReady(mfg, true) {
		t.Fatal("PITR profile should reject Ready=True from an older generation")
	}

	mfg.Status.UpdatePhase = "UpdateReplica"
	if backupProfileRolloutReady(mfg, false) {
		t.Fatal("plain backup profile should wait for an in-progress topology update")
	}
	mfg.Status.UpdatePhase = ""
	mfg.Status.ActiveSite = ""
	if backupProfileRolloutReady(mfg, false) {
		t.Fatal("plain backup profile should wait until an active site is available")
	}

	mfg.Status.ActiveSite = "iad"
	mfg.Status.Conditions[0].ObservedGeneration = mfg.Generation
	if !backupProfileRolloutReady(mfg, true) {
		t.Fatal("PITR profile should accept a completed current-generation rollout")
	}
}
