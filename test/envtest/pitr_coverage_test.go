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
