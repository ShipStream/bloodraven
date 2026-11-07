//go:build envtest

package envtest

import (
	"fmt"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// E9 (A-55): one retention shape. A profile's retention and its
// verification.retention are defaulted as a whole, an explicit 0 where 0
// means "keep none" is stored as 0, and the released shapes are refused.

// retentionProfileObject is the first profile of a newVerificationFG group as
// an unstructured map, so a test can write values a typed client would omit.
func retentionProfileObject(t *testing.T, ns, name string, edit func(fg *v1alpha1.MysqlFailoverGroup)) (client.Object, map[string]any) {
	t.Helper()
	fg := newVerificationFG(ns)
	fg.Name = name
	if edit != nil {
		edit(fg)
	}
	u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
	spec := u.Object["spec"].(map[string]any)
	profile := spec["backup"].(map[string]any)["profiles"].([]any)[0].(map[string]any)
	return u, profile
}

func TestEnvtest_RetentionDefaults(t *testing.T) {
	ns := createNamespace(t, "e9-retention")
	fg := newVerificationFG(ns)
	fg.Name = "defaults"
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 5 * * *"}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	got := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), got); err != nil {
		t.Fatal(err)
	}
	rt := got.Spec.Backup.Profiles[0].Retention
	if rt.KeepLast != 7 || rt.KeepWithinDays != nil || rt.KeepFailed == nil || *rt.KeepFailed != 10 {
		t.Errorf("profile retention defaults = %+v, want keepLast 7, no keepWithinDays, keepFailed 10", rt)
	}
	vrt := got.Spec.Backup.Profiles[0].Verification.Retention
	if vrt.KeepLast == nil || *vrt.KeepLast != 30 || vrt.KeepFailed == nil || *vrt.KeepFailed != 10 {
		t.Errorf("verification retention defaults = %+v, want keepLast 30, keepFailed 10", vrt)
	}

	// An explicit 0 means keep none and survives admission and the typed
	// round trip (F-26).
	fg = newVerificationFG(ns)
	fg.Name = "zeros"
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 1, KeepWithinDays: new(int32(3)), KeepFailed: new(int32(0))}
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 5 * * *",
		Retention: v1alpha1.VerificationRetention{KeepLast: new(int32(0)), KeepFailed: new(int32(0))}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), got); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Update(ctx, got); err != nil {
		t.Fatalf("typed round trip: %v", err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), got); err != nil {
		t.Fatal(err)
	}
	rt = got.Spec.Backup.Profiles[0].Retention
	if rt.KeepLast != 1 || rt.KeepWithinDays == nil || *rt.KeepWithinDays != 3 || rt.KeepFailed == nil || *rt.KeepFailed != 0 {
		t.Errorf("profile retention = %+v, want keepLast 1, keepWithinDays 3, keepFailed 0 kept", rt)
	}
	vrt = got.Spec.Backup.Profiles[0].Verification.Retention
	if vrt.KeepLast == nil || *vrt.KeepLast != 0 || vrt.KeepFailed == nil || *vrt.KeepFailed != 0 {
		t.Errorf("verification retention = %+v, want both 0 kept", vrt)
	}
}

func TestEnvtest_RetentionRefusals(t *testing.T) {
	ns := createNamespace(t, "e9-retention-refused")
	withVerification := func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 5 * * *"}
	}
	for i, tc := range []struct {
		name  string
		field string
		edit  func(fg *v1alpha1.MysqlFailoverGroup)
		set   func(profile map[string]any)
	}{
		// keepLast >= 1 keeps the last good backup: no "keep none".
		{name: "keepLast 0", field: "keepLast", set: func(p map[string]any) { p["retention"] = map[string]any{"keepLast": int64(0)} }},
		{name: "keepWithinDays 0", field: "keepWithinDays", set: func(p map[string]any) { p["retention"] = map[string]any{"keepWithinDays": int64(0)} }},
		{name: "keepFailed negative", field: "keepFailed", set: func(p map[string]any) { p["retention"] = map[string]any{"keepFailed": int64(-1)} }},
		{name: "verification keepLast negative", field: "keepLast", edit: withVerification, set: func(p map[string]any) {
			p["verification"].(map[string]any)["retention"] = map[string]any{"keepLast": int64(-1)}
		}},
		{name: "verification keepFailed negative", field: "keepFailed", edit: withVerification, set: func(p map[string]any) {
			p["verification"].(map[string]any)["retention"] = map[string]any{"keepFailed": int64(-1)}
		}},
		// The released shorthand integer is refused, not read as a count.
		{name: "legacy retention integer", field: "retention", set: func(p map[string]any) { p["retention"] = int64(7) }},
	} {
		obj, profile := retentionProfileObject(t, ns, fmt.Sprintf("refused-%d", i), tc.edit)
		tc.set(profile)
		wantRefused(t, tc.name, k8sClient.Create(ctx, obj), tc.field)
	}
}
