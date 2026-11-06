//go:build envtest

package envtest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func pitrRestoreFG(ns string, pit *v1alpha1.PointInTimeSpec) *v1alpha1.MysqlFailoverGroup {
	fg := newVerificationFG(ns)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source:      v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"}},
		PointInTime: pit,
	}
	return fg
}

// D-10: initFromBackup.pointInTime must name its archive profile, which must
// be a profile-shaped name.
func TestPITRInitFromBackupRequiresArchiveProfile(t *testing.T) {
	for i, tc := range []struct {
		profile string
		want    string // "" = accepted
	}{
		{profile: "nightly-s3"},
		{profile: "", want: "archiveProfileName is required"},
		{profile: "Upper_Case", want: "archiveProfileName"},
		{profile: strings.Repeat("a", 64), want: "archiveProfileName"},
	} {
		ns := fmt.Sprintf("pitr-archive-profile-%d", i)
		ensureNamespace(t, ns)
		err := k8sClient.Create(ctx, pitrRestoreFG(ns, &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z", ArchiveProfileName: tc.profile}))
		if tc.want == "" {
			if err != nil {
				t.Fatalf("%q refused: %v", tc.profile, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%q: err = %v, want %q", tc.profile, err, tc.want)
		}
	}
	// A dump-only restore needs no archive profile.
	ensureNamespace(t, "pitr-archive-profile-none")
	if err := k8sClient.Create(ctx, pitrRestoreFG("pitr-archive-profile-none", nil)); err != nil {
		t.Fatalf("dump-only restore refused: %v", err)
	}
}

// D-9: pointInTime.sites, allowUnverifiedCrossSite and verification sites are
// gone from the schema; the API server prunes them.
func TestPITRRemovedFieldsArePruned(t *testing.T) {
	ns := "pitr-removed-fields"
	ensureNamespace(t, ns)
	fg := pitrRestoreFG(ns, &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z", ArchiveProfileName: "nightly-s3"})
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(fg)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: raw}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))
	pit := u.Object["spec"].(map[string]any)["initFromBackup"].(map[string]any)["pointInTime"].(map[string]any)
	pit["sites"] = []any{"iad"}
	pit["allowUnverifiedCrossSite"] = true
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(u.GroupVersionKind())
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, stored); err != nil {
		t.Fatal(err)
	}
	got, _, _ := unstructured.NestedMap(stored.Object, "spec", "initFromBackup", "pointInTime")
	if _, ok := got["sites"]; ok {
		t.Fatalf("pointInTime.sites survived: %v", got)
	}
	if _, ok := got["allowUnverifiedCrossSite"]; ok {
		t.Fatalf("pointInTime.allowUnverifiedCrossSite survived: %v", got)
	}

	v := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(), "kind": "MysqlBackupVerification",
		"metadata": map[string]any{"name": "verify", "namespace": ns},
		"spec": map[string]any{"failoverGroupRef": map[string]any{"name": "lion"}, "profileName": "nightly-s3",
			"pointInTime": map[string]any{"mode": "latest", "sites": []any{"iad"}}},
	}}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	storedV := &unstructured.Unstructured{}
	storedV.SetGroupVersionKind(v.GroupVersionKind())
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "verify"}, storedV); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := unstructured.NestedMap(storedV.Object, "spec", "pointInTime"); got["sites"] != nil {
		t.Fatalf("verification sites survived: %v", got)
	}
}

// MysqlBackup status carries the identity the Job pinned; pitrArchiveID is
// pattern-checked.
func TestMysqlBackupStatusArchiveIdentity(t *testing.T) {
	ns := "pitr-backup-identity"
	ensureNamespace(t, ns)
	b := newSucceededBackup(ns, "lion", "nightly-s3")
	if err := k8sClient.Create(ctx, b); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id string
		ok bool
	}{{"ar1-abcdefghijklmnopqrstuvwxyz", true}, {"", true}, {"ar1-ABCDEFGHIJKLMNOPQRSTUVWXYZ", false}, {"ar1-short", false}} {
		cur := &v1alpha1.MysqlBackup{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: b.Name}, cur); err != nil {
			t.Fatal(err)
		}
		cur.Status.FailoverGroupUID = "fg-uid"
		cur.Status.PITRArchiveID = tc.id
		err := k8sClient.Status().Update(ctx, cur)
		if (err == nil) != tc.ok {
			t.Fatalf("pitrArchiveID %q: %v", tc.id, err)
		}
		if tc.ok {
			got := &v1alpha1.MysqlBackup{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: b.Name}, got); err != nil {
				t.Fatal(err)
			}
			if got.Status.FailoverGroupUID != "fg-uid" || got.Status.PITRArchiveID != tc.id {
				t.Fatalf("status identity did not round-trip: %+v", got.Status)
			}
		}
	}
}

// status.pitr.archive: a worst-case 4096-character destination and a µs
// observedTime round-trip; the state and reason enums are closed.
func TestPITRStatusArchiveObservation(t *testing.T) {
	ns := "pitr-status-archive"
	ensureNamespace(t, ns)
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	observed := metav1.NewMicroTime(time.Date(2026, 11, 4, 12, 0, 0, 123456000, time.UTC))
	dest := "s3://b/" + strings.Repeat("%2F", (4096-len("s3://b/?endpoint=aws"))/3) + "?endpoint=aws"
	dest += strings.Repeat("x", 4096-len(dest))
	for _, tc := range []struct {
		obs v1alpha1.PITRArchiveObservation
		ok  bool
	}{
		{obs: v1alpha1.PITRArchiveObservation{Destination: dest, ArchiveID: "ar1-abcdefghijklmnopqrstuvwxyz", EncryptionMode: "Passphrase", State: "Verified", KeyRevision: "mysql-lion-pitr-key-0123456789", ObservedTime: &observed}, ok: true},
		{obs: v1alpha1.PITRArchiveObservation{Destination: "pvc://ns/c/binlogs", State: "Refused", Reason: "ArchiveUnsafeHistory"}, ok: true},
		{obs: v1alpha1.PITRArchiveObservation{Destination: dest + "x", State: "Verified"}},
		{obs: v1alpha1.PITRArchiveObservation{Destination: "d", State: "Bogus"}},
		{obs: v1alpha1.PITRArchiveObservation{Destination: "d", State: "Refused", Reason: "PITRArchiveLegacy"}},
	} {
		cur := &v1alpha1.MysqlFailoverGroup{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, cur); err != nil {
			t.Fatal(err)
		}
		obs := tc.obs
		cur.Status.PITR = &v1alpha1.PITRStatus{Enabled: true, ProfileName: "nightly-s3", Archive: &obs}
		err := k8sClient.Status().Update(ctx, cur)
		if (err == nil) != tc.ok {
			t.Fatalf("observation %+v: %v", tc.obs.State, err)
		}
		if !tc.ok {
			continue
		}
		got := &v1alpha1.MysqlFailoverGroup{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, got); err != nil {
			t.Fatal(err)
		}
		a := got.Status.PITR.Archive
		if a == nil || a.Destination != tc.obs.Destination || a.State != tc.obs.State || a.Reason != tc.obs.Reason {
			t.Fatalf("archive observation did not round-trip: %+v", a)
		}
		if tc.obs.ObservedTime != nil && (a.ObservedTime == nil || a.ObservedTime.UTC().Format("15:04:05.000000") != "12:00:00.123456") {
			t.Fatalf("observedTime lost microseconds: %v", a.ObservedTime)
		}
	}
}

// §6.1 / A-39: stopDatetime and the verification timestamp accept a whole
// second only (RFC 3339 or the MySQL form, bounded offset, no fraction);
// excludeGtids is bounded at 131072 bytes (A-23), and verifications accept
// it.
func TestPITRTargetPatternAndExclusions(t *testing.T) {
	for i, tc := range []struct {
		stop    string
		exclude string
		ok      bool
	}{
		{stop: "2030-01-01T00:00:00Z", ok: true},
		{stop: "2030-01-01 00:00:00", ok: true},
		{stop: "2030-01-01T02:00:00+02:00", ok: true},
		{stop: "2030-01-01T02:00:00-23:59", ok: true},
		{stop: "2030-01-01T00:00:00.123456Z"},
		{stop: "2030-01-01 00:00:00.5"},
		{stop: "2030-01-01T02:00:00.000001+02:00"},
		{stop: "2030-01-01T00:00:00.1234567Z"},
		{stop: "2030-01-01T00:00:00,5Z"},
		{stop: "2030-01-01T00:00:00+25:00"},
		{stop: "2030-01-01T00:00:00+05:99"},
		{stop: "yesterday"},
		{stop: "2030-01-01T00:00:00Z", exclude: strings.Repeat("a", 131072), ok: true},
		{stop: "2030-01-01T00:00:00Z", exclude: strings.Repeat("a", 131073)},
	} {
		ns := fmt.Sprintf("pitr-target-pattern-%d", i)
		ensureNamespace(t, ns)
		err := k8sClient.Create(ctx, pitrRestoreFG(ns, &v1alpha1.PointInTimeSpec{StopDatetime: tc.stop, ArchiveProfileName: "nightly-s3", ExcludeGtids: tc.exclude}))
		if (err == nil) != tc.ok {
			t.Fatalf("restore stop %q exclude %d chars: %v", tc.stop, len(tc.exclude), err)
		}
		v := &v1alpha1.MysqlBackupVerification{
			ObjectMeta: metav1.ObjectMeta{Name: "verify", Namespace: ns},
			Spec: v1alpha1.MysqlBackupVerificationSpec{
				FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3",
				PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: tc.stop, ExcludeGtids: tc.exclude},
			},
		}
		err = k8sClient.Create(ctx, v)
		if (err == nil) != tc.ok {
			t.Fatalf("verification timestamp %q exclude %d chars: %v", tc.stop, len(tc.exclude), err)
		}
		if tc.ok && tc.exclude != "" {
			got := &v1alpha1.MysqlBackupVerification{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "verify"}, got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.PointInTime.ExcludeGtids != tc.exclude {
				t.Fatal("verification excludeGtids did not round-trip")
			}
		}
	}
	ensureNamespace(t, "pitr-target-pattern-long")
	if err := k8sClient.Create(ctx, pitrRestoreFG("pitr-target-pattern-long", &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00+02:00" + strings.Repeat(" ", 20), ArchiveProfileName: "nightly-s3"})); err == nil {
		t.Fatal("over-long stopDatetime accepted")
	}
}

func pointInTimeFixture() *v1alpha1.PointInTimeResult {
	stop := metav1.NewMicroTime(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	newest := metav1.NewMicroTime(time.Date(2029, 12, 31, 23, 59, 59, 654321000, time.UTC))
	return &v1alpha1.PointInTimeResult{
		Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: &stop, Outcome: v1alpha1.PointInTimeTargetCovered,
		Archive: &v1alpha1.PITRArchiveIdentity{ArchiveID: "ar1-abcdefghijklmnopqrstuvwxyz", Destination: "s3://b/p/binlogs?endpoint=aws",
			Source: v1alpha1.PITRArchiveSource{Namespace: "ns", Name: "src", UID: "src-uid"}, EncryptionMode: v1alpha1.PITRArchiveEncryptionPassphrase,
			Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM},
		ReplaySites: []string{"a", "b"}, SegmentCount: 3, ReplayedBinlogCount: 2,
		LastReplayedBinlog: &v1alpha1.ReplayedBinlogRef{Site: "a", Name: "mysql-bin.000010"}, ReplayedTransactionCount: 1234,
		NewestReplayedCommitTime: &newest, ExcludedDivergentTransactionCount: 2, OnHistoryExclusions: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:7",
		OnHistoryExclusionCount: 1, GtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-6:8-20",
		GtidExecutedSHA256: strings.Repeat("ab", 32),
	}
}

func assertPointInTimeRoundTrip(t *testing.T, where string, got *v1alpha1.PointInTimeResult) {
	t.Helper()
	want := pointInTimeFixture()
	if got == nil || got.Outcome != want.Outcome || got.Archive == nil || got.Archive.ArchiveID != want.Archive.ArchiveID ||
		got.GtidExecuted != want.GtidExecuted || got.LastReplayedBinlog == nil || got.LastReplayedBinlog.Name != want.LastReplayedBinlog.Name {
		t.Fatalf("%s: pointInTime did not round-trip: %+v", where, got)
	}
	if got.StopTime == nil || got.StopTime.UTC().Format("15:04:05.000000") != "00:00:00.000000" ||
		got.NewestReplayedCommitTime == nil || got.NewestReplayedCommitTime.UTC().Format("15:04:05.000000") != "23:59:59.654321" {
		t.Fatalf("%s: MicroTime lost microseconds: %v %v", where, got.StopTime, got.NewestReplayedCommitTime)
	}
}

// PointInTimeResult (D-29) round-trips at µs on every carrier, and its
// enums and patterns are closed.
func TestPointInTimeResultShape(t *testing.T) {
	ns := "pitr-result-shape"
	ensureNamespace(t, ns)

	fg := pitrRestoreFG(ns, &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z", ArchiveProfileName: "nightly-s3"})
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.PointInTimeResult)
		ok     bool
	}{
		{name: "valid", mutate: func(*v1alpha1.PointInTimeResult) {}, ok: true},
		{name: "outcome enum", mutate: func(r *v1alpha1.PointInTimeResult) { r.Outcome = "Covered" }},
		{name: "mode enum", mutate: func(r *v1alpha1.PointInTimeResult) { r.Mode = "timestamp" }},
		{name: "sha pattern", mutate: func(r *v1alpha1.PointInTimeResult) { r.GtidExecutedSHA256 = "ABC" }},
		{name: "site pattern", mutate: func(r *v1alpha1.PointInTimeResult) { r.ReplaySites = []string{"A_b"} }},
		{name: "gtidExecuted bound", mutate: func(r *v1alpha1.PointInTimeResult) { r.GtidExecuted = strings.Repeat("a", 2049) }},
		{name: "replay site bound", mutate: func(r *v1alpha1.PointInTimeResult) {
			r.ReplaySites = strings.Split(strings.Repeat("s,", 16)+"s", ",")
		}},
	} {
		cur := &v1alpha1.MysqlFailoverGroup{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, cur); err != nil {
			t.Fatal(err)
		}
		res := pointInTimeFixture()
		tc.mutate(res)
		cur.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseSucceeded, PointInTime: res}
		err := k8sClient.Status().Update(ctx, cur)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
	got := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, got); err != nil {
		t.Fatal(err)
	}
	assertPointInTimeRoundTrip(t, "status.restore", got.Status.Restore.PointInTime)

	// The flat pitr* restore fields are gone from the schema.
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, u); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(u.Object, "2030-01-01 00:00:00", "status", "restore", "pitrStopDatetime"); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Status().Update(ctx, u); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: fg.Name}, u); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(u.Object, "status", "restore", "pitrStopDatetime"); found {
		t.Fatal("status.restore.pitrStopDatetime survived")
	}

	// MBV status.pointInTime.
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify", Namespace: ns},
		Spec:       v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
	}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	v.Status.Phase = v1alpha1.VerificationPhaseSucceeded
	v.Status.PointInTime = pointInTimeFixture()
	if err := k8sClient.Status().Update(ctx, v); err != nil {
		t.Fatal(err)
	}
	gotV := &v1alpha1.MysqlBackupVerification{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "verify"}, gotV); err != nil {
		t.Fatal(err)
	}
	assertPointInTimeRoundTrip(t, "verification status", gotV.Status.PointInTime)

	// C6: status.activation carries no pointInTime (no activation writes
	// one, and A-61 deletes status.activation); the apiserver prunes it.
	msc := minimalEnvtestStandby("standby", ns)
	if err := k8sClient.Create(ctx, msc); err != nil {
		t.Fatal(err)
	}
	u = &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlStandbyCluster"))
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "standby"}, u); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(u.Object, map[string]any{"phase": "Validating", "pointInTime": map[string]any{"mode": "Latest"}}, "status", "activation"); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Status().Update(ctx, u); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "standby"}, u); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(u.Object, "status", "activation", "pointInTime"); found {
		t.Fatal("status.activation.pointInTime survived")
	}
}
