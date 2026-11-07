//go:build envtest

package envtest

import (
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestCRDsInstallWithRatchetingGateOff: the CRDs install, and a group with
// a point-in-time initFromBackup is created and updated, on an API server
// with the CRDValidationRatcheting feature gate off. The gate is beta (on
// by default) in 1.31 and 1.32, the supported floor, and GA from 1.33. With
// it off the API server drops optionalOldSelf from a new CRD, so a
// create-only rule becomes a transition rule and must still compile with
// oldSelf as the object itself.
func TestCRDsInstallWithRatchetingGateOff(t *testing.T) {
	if statusWritesRatchet(t, cfg) {
		t.Skip("CRDValidationRatcheting is GA and locked on from Kubernetes 1.33")
	}
	gateScheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(gateScheme); err != nil {
			t.Fatal(err)
		}
	}
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		Scheme:            gateScheme,
	}
	env.ControlPlane.GetAPIServer().Configure().Append("feature-gates", "CRDValidationRatcheting=false")
	gateCfg, err := env.Start()
	if err != nil {
		t.Fatalf("install the CRDs on an API server with CRDValidationRatcheting=false: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	c, err := client.New(gateCfg, client.Options{Scheme: gateScheme})
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ratcheting-gate-off"}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	fg := newTestFG(ns.Name)
	fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{pitrS3Profile("source", "orders", ""), pitrS3Profile("own", "p1", "")}}
	fg.Spec.InitFromBackup = e16InitPITR("source")
	if err := c.Create(ctx, fg); err != nil {
		t.Fatalf("create a group with a point-in-time initFromBackup: %v", err)
	}
	cur := &v1alpha1.MysqlFailoverGroup{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(fg), cur); err != nil {
		t.Fatal(err)
	}
	cur.Spec.Backup.Profiles = cur.Spec.Backup.Profiles[1:]
	if err := c.Update(ctx, cur); err != nil {
		t.Errorf("remove the initFromBackup source profile after creation: %v", err)
	}
}
