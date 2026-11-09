//go:build envtest

package envtest

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// E13 at admission: the desired state that moved out of annotations and
// status into spec, and the persisted defaults that went away.

func e13DragonflyFG(ns, name string, df *v1alpha1.DragonflySpec) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG(ns)
	fg.Name = name
	fg.Spec.Dragonfly = df
	return fg
}

// A-48: SnapshotRestore needs snapshot.dir. The rule guards each step with
// has(), so `snapshot: {}` gets the rule's message, not a CEL evaluation
// error ("no such key").
func TestEnvtest_DragonflySnapshotRestoreNeedsSnapshotDir(t *testing.T) {
	ns := "envtest-e13-dragonfly"
	ensureNamespace(t, ns)
	const image = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0"
	const rule = "spec.dragonfly.snapshot.dir is required when spec.dragonfly.upgradeStrategy is SnapshotRestore"

	for _, tc := range []struct {
		name     string
		snapshot *v1alpha1.DragonflySnapshotSpec
	}{
		{name: "no-snapshot"},
		{name: "empty-snapshot", snapshot: &v1alpha1.DragonflySnapshotSpec{}},
		{name: "snapshot-without-dir", snapshot: &v1alpha1.DragonflySnapshotSpec{ServiceAccountName: "dragonfly"}},
	} {
		fg := e13DragonflyFG(ns, tc.name, &v1alpha1.DragonflySpec{Enabled: true, Image: image,
			UpgradeStrategy: v1alpha1.DragonflyUpgradeStrategySnapshotRestore, Snapshot: tc.snapshot})
		err := k8sClient.Create(ctx, fg)
		if err == nil {
			t.Errorf("%s: accepted, want refused", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), rule) {
			t.Errorf("%s: refused without the rule's message: %v", tc.name, err)
		}
		if strings.Contains(err.Error(), "no such key") {
			t.Errorf("%s: the rule failed to evaluate: %v", tc.name, err)
		}
	}

	ok := e13DragonflyFG(ns, "snapshot-restore-ok", &v1alpha1.DragonflySpec{Enabled: true, Image: image,
		UpgradeStrategy: v1alpha1.DragonflyUpgradeStrategySnapshotRestore,
		Snapshot:        &v1alpha1.DragonflySnapshotSpec{Dir: "s3://bucket/prefix"}})
	if err := k8sClient.Create(ctx, ok); err != nil {
		t.Fatalf("SnapshotRestore with snapshot.dir: %v", err)
	}

	// Rolling is the default and needs no snapshot.
	rolling := e13DragonflyFG(ns, "rolling-default", &v1alpha1.DragonflySpec{Enabled: true, Image: image})
	if err := k8sClient.Create(ctx, rolling); err != nil {
		t.Fatalf("default strategy without a snapshot: %v", err)
	}
	if rolling.Spec.Dragonfly.UpgradeStrategy != v1alpha1.DragonflyUpgradeStrategyRolling {
		t.Errorf("upgradeStrategy defaulted to %q, want Rolling", rolling.Spec.Dragonfly.UpgradeStrategy)
	}

	bad := e13DragonflyFG(ns, "bad-strategy", &v1alpha1.DragonflySpec{Enabled: true, Image: image, UpgradeStrategy: "Recreate"})
	wantRefused(t, "upgradeStrategy=Recreate", k8sClient.Create(ctx, bad), "upgradeStrategy")
}

// A-52: no persisted sidecar image. spec.sidecar defaults to {}, so a
// manifest without the block still gets the lease defaults and the
// struct-level rules run; spec.sidecar.image stays empty.
func TestEnvtest_SidecarHasNoPersistedImage(t *testing.T) {
	ns := "envtest-e13-sidecar"
	ensureNamespace(t, ns)

	// A manifest without the block: a Go client always sends the value
	// struct, so the key is dropped from the unstructured form.
	u := inertFieldsObject(t, newTestFG(ns), "MysqlFailoverGroup")
	u.SetName("no-sidecar-block")
	unstructured.RemoveNestedField(u.Object, "spec", "sidecar")
	if err := k8sClient.Create(ctx, u); err != nil {
		t.Fatalf("create: %v", err)
	}
	fg := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "no-sidecar-block"}, fg); err != nil {
		t.Fatal(err)
	}
	if fg.Spec.Sidecar.Image != "" {
		t.Errorf("spec.sidecar.image = %q, want no persisted default", fg.Spec.Sidecar.Image)
	}
	if fg.Spec.Sidecar.LeaseTimeout == nil || fg.Spec.Sidecar.LeaseTimeout.Duration != 20*time.Second {
		t.Errorf("spec.sidecar.leaseTimeout = %v, want the 20s default", fg.Spec.Sidecar.LeaseTimeout)
	}
	if fg.Spec.Sidecar.PeerCheckInterval == nil || fg.Spec.Sidecar.PeerCheckInterval.Duration != 5*time.Second {
		t.Errorf("spec.sidecar.peerCheckInterval = %v, want the 5s default", fg.Spec.Sidecar.PeerCheckInterval)
	}

	// The 3x rule runs on the defaulted block too.
	tight := newTestFG(ns)
	tight.Name = "tight-lease"
	tight.Spec.Sidecar.PeerCheckInterval = &metav1.Duration{Duration: 10 * time.Second}
	wantRefused(t, "leaseTimeout default below 3x peerCheckInterval", k8sClient.Create(ctx, tight), "3x")

	pinned := newTestFG(ns)
	pinned.Name = "pinned-sidecar"
	pinned.Spec.Sidecar.Image = "registry.example/bloodraven-sidecar:1.7.0"
	if err := k8sClient.Create(ctx, pinned); err != nil {
		t.Fatalf("pinned sidecar image: %v", err)
	}
	if pinned.Spec.Sidecar.Image != "registry.example/bloodraven-sidecar:1.7.0" {
		t.Errorf("pin = %q", pinned.Spec.Sidecar.Image)
	}
}

// A-51: adoption is a spec policy, Refuse by default.
func TestEnvtest_EncryptionExistingDataPolicy(t *testing.T) {
	ns := "envtest-e13-encryption"
	ensureNamespace(t, ns)

	fg := newEncryptionFG(ns, "refuse-default")
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create: %v", err)
	}
	if fg.Spec.EncryptionAtRest.ExistingData != v1alpha1.EncryptionExistingDataRefuse {
		t.Errorf("existingData defaulted to %q, want Refuse", fg.Spec.EncryptionAtRest.ExistingData)
	}

	bad := newEncryptionFG(ns, "existing-data-confirm")
	bad.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true, ExistingData: "confirm"}
	wantRefused(t, "existingData=confirm", k8sClient.Create(ctx, bad), "existingData")
}

// A-98: MysqlBackup deletionPolicy defaults to Delete; Retain is accepted
// and can be set up to the deletion.
func TestEnvtest_MysqlBackupDeletionPolicy(t *testing.T) {
	ns := "envtest-e13-backup"
	ensureNamespace(t, ns)

	mb := newSucceededBackup(ns, "lion", "nightly")
	mb.Name = "policy-default"
	if err := k8sClient.Create(ctx, mb); err != nil {
		t.Fatalf("create: %v", err)
	}
	if mb.Spec.DeletionPolicy != v1alpha1.DeletionPolicyDelete {
		t.Errorf("deletionPolicy defaulted to %q, want Delete", mb.Spec.DeletionPolicy)
	}
	mb.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain
	if err := k8sClient.Update(ctx, mb); err != nil {
		t.Fatalf("switch to Retain: %v", err)
	}

	bad := newSucceededBackup(ns, "lion", "nightly")
	bad.Name = "policy-orphan"
	bad.Spec.DeletionPolicy = "Orphan"
	wantRefused(t, "deletionPolicy=Orphan", k8sClient.Create(ctx, bad), "deletionPolicy")
}
