package scenarios

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
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
			if profile.Storage.S3.Bucket != backupE2EBucket || profile.Storage.S3.Prefix != "e2e/test/run" || profile.Storage.S3.EndpointURL != backupE2EEndpoint || profile.Storage.S3.CredentialsSecretName != backupE2ECredsSecret {
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
	stale := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 4}}
	cases := []struct {
		name   string
		active string
		phase  string
		want   bool
	}{
		// A plain profile is inert: status is not rewritten for it, so a
		// Ready from an older generation must not hold the wait.
		{"stable topology, stale Ready", "iad", "", true},
		{"ordered update in progress", "iad", "UpdateReplica", false},
		{"no active site", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mfg := &v1alpha1.MysqlFailoverGroup{
				ObjectMeta: metav1.ObjectMeta{Generation: 5},
				Status:     v1alpha1.MysqlFailoverGroupStatus{ActiveSite: tc.active, UpdatePhase: v1alpha1.UpdatePhase(tc.phase), Conditions: stale},
			}
			if got := backupProfileRolloutReady(mfg); got != tc.want {
				t.Fatalf("backupProfileRolloutReady = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBackupArtifactPrefix(t *testing.T) {
	succeeded := func(storage *v1alpha1.BackupArtifactStorage) *v1alpha1.MysqlBackup {
		if storage != nil {
			storage.Type = v1alpha1.BackupStorageS3
		}
		return &v1alpha1.MysqlBackup{Status: v1alpha1.MysqlBackupStatus{
			Phase: v1alpha1.BackupPhaseSucceeded, ArtifactStorage: storage,
		}}
	}
	for _, tc := range []struct {
		name           string
		backup         *v1alpha1.MysqlBackup
		bucket, prefix string
		ok, wantErr    bool
	}{
		{name: "plain", backup: succeeded(&v1alpha1.BackupArtifactStorage{Location: "e2e/s30/x/b1", Bucket: "bk"}), bucket: "bk", prefix: "e2e/s30/x/b1/", ok: true},
		{name: "encrypted", backup: succeeded(&v1alpha1.BackupArtifactStorage{Location: "s3://bk/e2e/s30e/x/b1/", Bucket: "bk"}), bucket: "bk", prefix: "e2e/s30e/x/b1/", ok: true},
		{name: "encrypted, other bucket", backup: succeeded(&v1alpha1.BackupArtifactStorage{Location: "s3://other/x/b1/", Bucket: "bk"}), wantErr: true},
		{name: "no recorded destination", backup: succeeded(nil)},
		{name: "recorded S3 without bucket", backup: succeeded(&v1alpha1.BackupArtifactStorage{Location: "x/b1"}), wantErr: true},
		{name: "recorded S3 without location", backup: succeeded(&v1alpha1.BackupArtifactStorage{Bucket: "bk"}), wantErr: true},
		{name: "PVC", backup: &v1alpha1.MysqlBackup{Status: v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded,
			ArtifactStorage: &v1alpha1.BackupArtifactStorage{Type: v1alpha1.BackupStoragePVC, Location: "/backups/b1"}}}},
		{name: "failed backup", backup: &v1alpha1.MysqlBackup{Status: v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseFailed,
			ArtifactStorage: &v1alpha1.BackupArtifactStorage{Type: v1alpha1.BackupStorageS3, Location: "x/b1", Bucket: "bk"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket, prefix, ok, err := backupArtifactPrefix(tc.backup)
			if (err != nil) != tc.wantErr || ok != tc.ok || bucket != tc.bucket || prefix != tc.prefix {
				t.Fatalf("got (%q, %q, %v, %v)", bucket, prefix, ok, err)
			}
		})
	}
}

func TestPITRBaselineRestored(t *testing.T) {
	profile := v1alpha1.BackupProfile{Name: "orig", Storage: v1alpha1.BackupStorage{
		Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{Bucket: "bk", Prefix: "orig"},
	}}
	origDest, err := pitrprofile.DestinationFor(&v1alpha1.MysqlFailoverGroup{}, &profile)
	if err != nil {
		t.Fatal(err)
	}
	scenarioDest, err := pitrarchive.NewS3Destination("bk", "e2e/s31", "")
	if err != nil {
		t.Fatal(err)
	}
	group := func(generation, readyGeneration int64, pitrOn bool, status *v1alpha1.PITRStatus) *v1alpha1.MysqlFailoverGroup {
		mfg := &v1alpha1.MysqlFailoverGroup{
			ObjectMeta: metav1.ObjectMeta{Generation: generation},
			Spec:       v1alpha1.MysqlFailoverGroupSpec{Backup: &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{profile}}},
			Status: v1alpha1.MysqlFailoverGroupStatus{
				ActiveSite: "iad",
				PITR:       status,
				Conditions: []metav1.Condition{{Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: readyGeneration}},
			},
		}
		if pitrOn {
			mfg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "orig"}
		}
		return mfg
	}
	observing := func(dest string) *v1alpha1.PITRStatus {
		return &v1alpha1.PITRStatus{Archive: &v1alpha1.PITRArchiveObservation{Destination: dest, State: v1alpha1.PITRArchiveVerified}}
	}
	leftover := observing(scenarioDest.String())
	for _, tc := range []struct {
		name        string
		mfg         *v1alpha1.MysqlFailoverGroup
		wantPITR    bool
		want        bool
		wantMessage string
	}{
		// The patch has landed but the operator has not reconciled it:
		// Ready still names the previous generation.
		{name: "disable, Ready from the previous generation", mfg: group(6, 5, false, nil), wantMessage: "readyAtGeneration=false"},
		{name: "disable, status.pitr still reported", mfg: group(6, 6, false, leftover), wantMessage: "status.pitr not yet removed"},
		{name: "disable, reconciled", mfg: group(6, 6, false, nil), want: true},
		{name: "re-enable, Ready from the previous generation", mfg: group(6, 5, true, observing(origDest.String())), wantPITR: true, wantMessage: "readyAtGeneration=false"},
		{name: "re-enable, leftover scenario archive", mfg: group(6, 6, true, leftover), wantPITR: true, wantMessage: "want " + origDest.String()},
		{name: "re-enable, no observation", mfg: group(6, 6, true, &v1alpha1.PITRStatus{}), wantPITR: true, wantMessage: "no status.pitr.archive"},
		{name: "re-enable, spec not yet restored", mfg: group(6, 6, false, observing(origDest.String())), wantPITR: true, wantMessage: "spec PITR profile"},
		{name: "re-enable, reconciled", mfg: group(6, 6, true, observing(origDest.String())), wantPITR: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, msg := pitrBaselineRestored(tc.mfg, tc.wantPITR, "orig")
			if got != tc.want || !strings.Contains(msg, tc.wantMessage) {
				t.Fatalf("pitrBaselineRestored = %v (%s), want %v containing %q", got, msg, tc.want, tc.wantMessage)
			}
		})
	}
	ordered := group(6, 6, true, observing(origDest.String()))
	ordered.Status.UpdatePhase = "UpdateReplica"
	if got, msg := pitrBaselineRestored(ordered, true, "orig"); got {
		t.Fatalf("an ordered update in progress must hold the cleanup: %s", msg)
	}
}
