//go:build envtest

package envtest

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/crdcompat"
	"github.com/shipstream/bloodraven/internal/storagemigration"
)

// olderSchemaCRDs loads the CRDs of testdata/<dir>, which were frozen
// before v1.8.0 and serve shipstream.io/v1alpha1 only, with that version
// renamed to the one the operator talks. The result is an older schema at
// the operator's version: what "a CRD older than the operator" is from
// v1.8.0 on, and what the status writer's fallbacks for values an older
// schema refuses are written for.
func olderSchemaCRDs(t *testing.T, dir string) []*apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", dir, "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no CRDs in testdata/%s (%v)", dir, err)
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal(raw, crd); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Name != v1beta1.DeprecatedGroupVersion.Version {
			t.Fatalf("%s: want a CRD frozen with the single version %s", f, v1beta1.DeprecatedGroupVersion.Version)
		}
		crd.Spec.Versions[0].Name = v1beta1.GroupVersion.Version
		out = append(out, crd)
	}
	return out
}

// schemeAt returns a scheme with the Bloodraven kinds registered at gv,
// beside the built-in kinds and CustomResourceDefinition.
func schemeAt(t *testing.T, gv schema.GroupVersion) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, apiextensionsv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := v1beta1.AddToSchemeAt(s, gv); err != nil {
		t.Fatal(err)
	}
	return s
}

// buildCRD reads one CRD of this build from the chart.
func buildCRD(t *testing.T, plural string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "bloodraven", "crds", "shipstream.io_"+plural+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.UnmarshalStrict(raw, crd); err != nil {
		t.Fatal(err)
	}
	return crd
}

// upgradeEnv is a second API server that starts with the CRDs an older
// release published, for the tests of what happens when this build's CRDs
// are applied over them.
type upgradeEnv struct {
	t   *testing.T
	cfg *rest.Config
	env *envtest.Environment
	// admin talks the storage version; dyn is the dynamic client.
	admin client.Client
	dyn   dynamic.Interface
}

// startUpgradeEnv starts an API server with the CRDs of testdata/<crdDir>
// exactly as published (they serve shipstream.io/v1alpha1 only). An empty
// crdDir starts with this build's CRDs.
func startUpgradeEnv(t *testing.T, crdDir string) *upgradeEnv {
	t.Helper()
	dir := filepath.Join("..", "..", "config", "crd", "bases")
	if crdDir != "" {
		dir = filepath.Join("testdata", crdDir)
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{dir}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start an API server with the CRDs of %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	return newUpgradeEnv(t, cfg, env)
}

func newUpgradeEnv(t *testing.T, cfg *rest.Config, env *envtest.Environment) *upgradeEnv {
	t.Helper()
	admin, err := client.New(cfg, client.Options{Scheme: schemeAt(t, v1beta1.GroupVersion)})
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &upgradeEnv{t: t, cfg: cfg, env: env, admin: admin, dyn: dyn}
}

// namespace creates a namespace and returns its name.
func (e *upgradeEnv) namespace(prefix string) string {
	e.t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix + "-"}}
	if err := e.admin.Create(ctx, ns); err != nil {
		e.t.Fatal(err)
	}
	return ns.Name
}

// applyBuildCRDs replaces the spec of every installed Bloodraven CRD with
// this build's, as `kubectl apply -f charts/bloodraven/crds/` does on an
// upgrade, and waits until the API server serves the storage version of
// each.
func (e *upgradeEnv) applyBuildCRDs() {
	e.t.Helper()
	for _, r := range crdcompat.Resources {
		next := buildCRD(e.t, r.Plural)
		var live apiextensionsv1.CustomResourceDefinition
		if err := e.admin.Get(ctx, types.NamespacedName{Name: next.Name}, &live); err != nil {
			e.t.Fatal(err)
		}
		live.Spec = next.Spec
		if err := e.admin.Update(ctx, &live); err != nil {
			e.t.Fatalf("apply this build's %s over the installed one: %v", next.Name, err)
		}
	}
	e.waitServed(v1beta1.GroupVersion.Version)
}

// waitServed waits until every kind can be listed at version.
func (e *upgradeEnv) waitServed(version string) {
	e.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for _, r := range crdcompat.Resources {
		for {
			_, err := e.dyn.Resource(r.GVR(version)).List(ctx, metav1.ListOptions{Limit: 1})
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				e.t.Fatalf("%s is not served at %s: %v", r.Plural, version, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// storedVersions returns status.storedVersions of every Bloodraven CRD.
func (e *upgradeEnv) storedVersions() map[string][]string {
	e.t.Helper()
	out := map[string][]string{}
	for _, r := range crdcompat.Resources {
		var crd apiextensionsv1.CustomResourceDefinition
		if err := e.admin.Get(ctx, types.NamespacedName{Name: r.CRDName()}, &crd); err != nil {
			e.t.Fatal(err)
		}
		out[r.CRDName()] = crd.Status.StoredVersions
	}
	return out
}

// loadObject reads one object from a YAML file, decoded as the API
// machinery decodes a response.
func loadObject(t *testing.T, path string) *unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	asJSON, err := yaml.YAMLToJSON(raw)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(asJSON); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return obj
}

// resourceOf returns the Bloodraven resource of obj's kind.
func resourceOf(t *testing.T, obj *unstructured.Unstructured) crdcompat.Resource {
	t.Helper()
	for _, r := range crdcompat.Resources {
		if r.Kind == obj.GetKind() {
			return r
		}
	}
	t.Fatalf("%s is not a Bloodraven kind", obj.GetKind())
	return crdcompat.Resource{}
}

// createAt creates obj in ns through version, as the file gives it (its
// status is dropped by the API server, as on every create), and returns the
// stored object.
func (e *upgradeEnv) createAt(obj *unstructured.Unstructured, ns, version string) *unstructured.Unstructured {
	e.t.Helper()
	r := resourceOf(e.t, obj)
	body := obj.DeepCopy()
	body.SetAPIVersion(crdcompat.Group + "/" + version)
	body.SetNamespace(ns)
	out, err := e.dyn.Resource(r.GVR(version)).Namespace(ns).Create(ctx, body, metav1.CreateOptions{FieldValidation: "Strict"})
	if err != nil {
		e.t.Fatalf("create %s %s through %s: %v", r.Kind, obj.GetName(), version, err)
	}
	return out
}

// allObjects reads every Bloodraven object through the storage version, by
// "<Kind> <namespace>/<name>".
func (e *upgradeEnv) allObjects() map[string]*unstructured.Unstructured {
	e.t.Helper()
	out := map[string]*unstructured.Unstructured{}
	for _, r := range crdcompat.Resources {
		list, err := e.dyn.Resource(r.GVR(v1beta1.GroupVersion.Version)).List(ctx, metav1.ListOptions{})
		if err != nil {
			e.t.Fatalf("list %s: %v", r.Plural, err)
		}
		for i := range list.Items {
			o := &list.Items[i]
			out[r.Kind+" "+o.GetNamespace()+"/"+o.GetName()] = o
		}
	}
	return out
}

// migrate runs migrate-storage as cfg's user and returns its report.
func (e *upgradeEnv) migrate(cfg *rest.Config, dryRun bool) (string, error) {
	e.t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{apiextensionsv1.AddToScheme, authorizationv1.AddToScheme} {
		if err := add(s); err != nil {
			e.t.Fatal(err)
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		e.t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	var out bytes.Buffer
	err = storagemigration.Run(ctx, storagemigration.Options{Client: c, Dynamic: dyn, DryRun: dryRun, Out: &out})
	e.t.Logf("migrate-storage (dry run %v):\n%s", dryRun, out.String())
	return out.String(), err
}

// loadCRDFile reads one CRD file.
func loadCRDFile(t *testing.T, path string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(raw, crd); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return crd
}

// installedCRDs reads the installed Bloodraven CRDs by name.
func installedCRDs(e *upgradeEnv) map[string]*apiextensionsv1.CustomResourceDefinition {
	e.t.Helper()
	out := map[string]*apiextensionsv1.CustomResourceDefinition{}
	for _, r := range crdcompat.Resources {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := e.admin.Get(ctx, types.NamespacedName{Name: r.CRDName()}, crd); err != nil {
			e.t.Fatal(err)
		}
		out[r.CRDName()] = crd
	}
	return out
}
