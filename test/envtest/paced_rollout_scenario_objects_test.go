//go:build envtest

package envtest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/scenarios"
)

// The paced-rollout scenarios (73, 74) create two auxiliary failover groups
// and pin and unpin the playground group's sidecar image with JSON patches.
// Both go through CRD validation on a cluster. This submits the same
// objects and the same patches to the API server, so a schema or CEL rule
// that refuses them fails here and not ten minutes into an E2E job.
func TestPacedRolloutScenarioObjectsAdmitted(t *testing.T) {
	pacedRolloutScenarioObjectsAdmitted(t, k8sClient)
}

// The upgrade E2E (scenario 74, playground/upgrade-e2e.sh) creates the
// auxiliary groups while the CRDs of the published v1.7.0 chart are
// installed, then applies this build's CRDs and upgrades the operator.
// testdata/crd-v1.7.0 is the MysqlFailoverGroup CRD of that chart, as
// published. Against it:
//
//   - the auxiliary groups and the pin patches are admitted;
//   - status.rollout is pruned (the reason the CRDs go first: the pacer
//     admits nothing while the record is not stored);
//   - after this build's CRD is applied over it, the objects are still
//     there and status.rollout is stored.
func TestPacedRolloutScenarioObjectsAcrossTheUpgradeFromV170(t *testing.T) {
	upgradeScheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme, apiextensionsv1.AddToScheme} {
		if err := add(upgradeScheme); err != nil {
			t.Fatal(err)
		}
	}
	oldEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("testdata", "crd-v1.7.0")},
		ErrorIfCRDPathMissing: true,
		Scheme:                upgradeScheme,
	}
	oldCfg, err := oldEnv.Start()
	if err != nil {
		t.Fatalf("start a second envtest with the v1.7.0 CRD: %v", err)
	}
	t.Cleanup(func() { _ = oldEnv.Stop() })
	c, err := client.New(oldCfg, client.Options{Scheme: upgradeScheme})
	if err != nil {
		t.Fatal(err)
	}

	ns, groups := pacedRolloutScenarioObjectsAdmitted(t, c)
	key := types.NamespacedName{Namespace: ns, Name: groups[0]}
	record := func() *v1alpha1.RolloutStatus {
		now := metav1.Now()
		return &v1alpha1.RolloutStatus{Renderer: "0123456789abcdef", Phase: v1alpha1.RolloutPhaseAdmitted, Reason: "Admitted", AdmittedTime: &now}
	}
	writeRecord := func() *v1alpha1.RolloutStatus {
		t.Helper()
		var fg v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, key, &fg); err != nil {
			t.Fatal(err)
		}
		fg.Status.Rollout = record()
		if err := c.Status().Update(ctx, &fg); err != nil {
			t.Fatalf("write status.rollout: %v", err)
		}
		var stored v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, key, &stored); err != nil {
			t.Fatal(err)
		}
		return stored.Status.Rollout
	}

	// Under the v1.7.0 CRD the record is not stored.
	if got := writeRecord(); got != nil {
		t.Fatalf("the v1.7.0 CRD stored status.rollout (%+v); it has no such field", got)
	}

	// `kubectl apply -f charts/bloodraven/crds/`, as upgrade-e2e.sh does
	// before `helm upgrade`.
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "bloodraven", "crds", "shipstream.io_mysqlfailovergroups.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var next apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(raw, &next); err != nil {
		t.Fatal(err)
	}
	var live apiextensionsv1.CustomResourceDefinition
	if err := c.Get(ctx, types.NamespacedName{Name: next.Name}, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec = next.Spec
	if err := c.Update(ctx, &live); err != nil {
		t.Fatalf("apply this build's CRD over the v1.7.0 one: %v", err)
	}

	// The objects created under v1.7.0 are still served, and the record
	// is stored once the API server serves the new schema.
	for _, name := range groups {
		var fg v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &fg); err != nil {
			t.Fatalf("group %s after the CRD upgrade: %v", name, err)
		}
		if len(fg.Spec.Sites) != 2 {
			t.Fatalf("group %s after the CRD upgrade: %d sites", name, len(fg.Spec.Sites))
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := writeRecord()
		if got != nil && got.Phase == v1alpha1.RolloutPhaseAdmitted && got.AdmittedTime != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status.rollout is still not stored 30s after this build's CRD was applied: %+v", got)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// pacedRolloutScenarioObjectsAdmitted creates the auxiliary groups in a new
// namespace and applies the pin and its removal to one of them. It returns
// the namespace and the group names.
func pacedRolloutScenarioObjectsAdmitted(t *testing.T, c client.Client) (string, []string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "paced-aux-"}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	groups, err := scenarios.PacedRolloutAuxGroups(ns.Name, "zone-iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("%d auxiliary groups, want 2", len(groups))
	}
	var names []string
	for _, fg := range groups {
		if err := c.Create(ctx, fg); err != nil {
			t.Fatalf("auxiliary group %s refused: %v", fg.Name, err)
		}
		names = append(names, fg.Name)
	}

	// The pin and its removal, as pacedPinSidecar and pacedUnpinSidecar
	// send them (JSON patches on /spec/sidecar/image).
	target := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: groups[0].Name}}
	patch := func(ops []map[string]any) error {
		body, err := json.Marshal(ops)
		if err != nil {
			t.Fatal(err)
		}
		return c.Patch(ctx, target, client.RawPatch(types.JSONPatchType, body))
	}
	if err := patch([]map[string]any{{"op": "add", "path": "/spec/sidecar/image", "value": "bloodraven-sidecar:playground"}}); err != nil {
		t.Fatalf("pin refused: %v", err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, client.ObjectKeyFromObject(target), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Sidecar.Image != "bloodraven-sidecar:playground" {
		t.Fatalf("spec.sidecar.image = %q after the pin", got.Spec.Sidecar.Image)
	}
	if err := patch([]map[string]any{{"op": "remove", "path": "/spec/sidecar/image"}}); err != nil {
		t.Fatalf("removing the pin refused: %v", err)
	}
	got = v1alpha1.MysqlFailoverGroup{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(target), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Sidecar.Image != "" {
		t.Fatalf("spec.sidecar.image = %q after the pin was removed", got.Spec.Sidecar.Image)
	}
	return ns.Name, names
}
