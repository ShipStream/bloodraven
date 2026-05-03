package main

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestEscapeSQLString(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "replicator", want: "replicator"},
		{name: "single quote", in: "rep'licator", want: "rep''licator"},
		{name: "backslash", in: `rep\licator`, want: `rep\\licator`},
		{name: "backslash quote", in: `rep\'licator`, want: `rep\\''licator`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := escapeSQLString(tc.in); got != tc.want {
				t.Fatalf("escapeSQLString(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNodeStorageWipePathsUsesFailoverGroup(t *testing.T) {
	r := &resetter{fg: "dragonfly"}
	sites := []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}

	got := r.nodeStorageWipePaths(sites)
	want := []string{
		"/var/lib/rancher/k3s/storage/pvc-*",
		"'/var/lib/rancher/k3s/storage/manual-mysql-dragonfly-iad-data'",
		"'/var/lib/rancher/k3s/storage/manual-mysql-dragonfly-pdx-data'",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("nodeStorageWipePaths() = %#v, want %#v", got, want)
	}
	for _, path := range got {
		if path == "'/var/lib/rancher/k3s/storage/manual-mysql-playground-*'" {
			t.Fatalf("wipe path still targets default playground glob: %#v", got)
		}
	}
}

func TestPVClaimScopeMatchesOwnedPVCsOnly(t *testing.T) {
	owned := map[string]struct{}{"mysql-dragonfly-iad-data": {}}
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "bloodraven-playground", Name: "mysql-dragonfly-iad-data"},
		},
	}
	if !pvClaimOwnedBy(pv, "bloodraven-playground", owned) {
		t.Fatal("expected PV claim to match owned MySQL PVC")
	}

	pv.Spec.ClaimRef.Name = "counter-app-data"
	if pvClaimOwnedBy(pv, "bloodraven-playground", owned) {
		t.Fatal("unrelated PVC in same namespace must not be selected for PV deletion")
	}
}

func TestStuckWithFinalizersRequiresDeletionTimestamp(t *testing.T) {
	meta := metav1.ObjectMeta{Finalizers: []string{"kubernetes.io/pvc-protection"}}
	if stuckWithFinalizers(meta) {
		t.Fatal("resource with finalizers but no deletion timestamp must not be force-patched")
	}
	now := metav1.Now()
	meta.DeletionTimestamp = &now
	if !stuckWithFinalizers(meta) {
		t.Fatal("terminating resource with finalizers should be force-patch eligible")
	}
}

func TestPrepareSecretForApplyAlwaysOverridesNamespace(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "bloodraven-playground",
			ResourceVersion: "123",
			UID:             types.UID("old"),
			ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
		},
	}

	prepareSecretForApply(secret, "custom-ns")

	if secret.Namespace != "custom-ns" {
		t.Fatalf("secret namespace = %q, want custom-ns", secret.Namespace)
	}
	if secret.ResourceVersion != "" || secret.UID != "" || secret.ManagedFields != nil {
		t.Fatalf("secret identity fields were not cleared: rv=%q uid=%q managed=%v", secret.ResourceVersion, secret.UID, secret.ManagedFields)
	}
}
