//go:build envtest

package envtest

import (
	"encoding/json"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

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

// The upgrade E2E (scenario 74) creates the auxiliary groups while the
// previous release's CRDs are installed. Set
// BLOODRAVEN_ENVTEST_PREVIOUS_CRD_DIR to that release's crds/ directory
// (for example the one `helm pull --untar` of the published chart
// unpacks) to check them against it.
func TestPacedRolloutScenarioObjectsAdmittedByPreviousRelease(t *testing.T) {
	dir := os.Getenv("BLOODRAVEN_ENVTEST_PREVIOUS_CRD_DIR")
	if dir == "" {
		t.Skip("BLOODRAVEN_ENVTEST_PREVIOUS_CRD_DIR is not set")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{dir}, ErrorIfCRDPathMissing: true, Scheme: scheme}
	oldCfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest with the CRDs of %s: %v", dir, err)
	}
	defer func() { _ = env.Stop() }()
	c, err := client.New(oldCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	pacedRolloutScenarioObjectsAdmitted(t, c)
}

func pacedRolloutScenarioObjectsAdmitted(t *testing.T, c client.Client) {
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
	for _, fg := range groups {
		if err := c.Create(ctx, fg); err != nil {
			t.Fatalf("auxiliary group %s refused: %v", fg.Name, err)
		}
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
	if err := c.Get(ctx, client.ObjectKeyFromObject(target), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Sidecar.Image != "" {
		t.Fatalf("spec.sidecar.image = %q after the pin was removed", got.Spec.Sidecar.Image)
	}
}
