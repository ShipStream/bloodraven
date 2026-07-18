package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestResetNormalizationPhaseOrdering(t *testing.T) {
	r := &resetter{}
	resetPhases := r.resetPhases()
	if len(resetPhases) < 2 {
		t.Fatalf("reset phase count = %d, want at least 2", len(resetPhases))
	}
	last := []string{resetPhases[len(resetPhases)-2].name, resetPhases[len(resetPhases)-1].name}
	if !reflect.DeepEqual(last, []string{"wait healthy baseline", "normalize fresh baseline"}) {
		t.Fatalf("final reset phases = %v, want initial baseline then normalization", last)
	}

	normalization := r.normalizationPhases()
	var names []string
	for _, phase := range normalization {
		names = append(names, phase.name)
	}
	want := []string{"scale operator down", "clear MFG status", "start operator", "wait healthy baseline"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("normalization phases = %v, want %v", names, want)
	}
}

func TestRunResetPhasesStopsOnFirstError(t *testing.T) {
	r := &resetter{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	boom := errors.New("boom")
	var calls []string
	phase := func(name string, err error) resetPhase {
		return resetPhase{name: name, fn: func(context.Context, []v1alpha1.SiteSpec) error {
			calls = append(calls, name)
			return err
		}}
	}
	err := r.runPhases(context.Background(), nil, []resetPhase{
		phase("scale operator down", nil),
		phase("clear MFG status", boom),
		phase("start operator", nil),
	})
	if !errors.Is(err, boom) {
		t.Fatalf("runPhases() error = %v, want %v", err, boom)
	}
	if !reflect.DeepEqual(calls, []string{"scale operator down", "clear MFG status"}) {
		t.Fatalf("phase calls = %v, want stop at first error", calls)
	}
}

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
		if path == "/var/lib/rancher/k3s/storage/pvc-*" {
			t.Fatalf("wipe path must not delete unrelated local-path PVC data: %#v", got)
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
