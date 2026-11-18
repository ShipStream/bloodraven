package storagemigration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	"github.com/shipstream/bloodraven/internal/crdcompat"
)

// fakeCluster is this build's CRDs with storedVersions [v1alpha1 v1beta1]
// (a cluster upgraded from v1.7.0), an account that may do everything, and
// the objects of test/fixtures/v1alpha1 as the API server would return
// them at v1beta1.
func fakeCluster(t *testing.T) (client.Client, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{apiextensionsv1.AddToScheme, authorizationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	built, err := crdcompat.BuiltCRDs()
	if err != nil {
		t.Fatal(err)
	}
	var crds []client.Object
	for _, crd := range built {
		crd = crd.DeepCopy()
		crd.Status.StoredVersions = []string{crdcompat.DeprecatedVersion, crdcompat.StorageVersion}
		crds = append(crds, crd)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(crds...).
		WithStatusSubresource(&apiextensionsv1.CustomResourceDefinition{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if review, ok := obj.(*authorizationv1.SelfSubjectAccessReview); ok {
					review.Status.Allowed = true
					return nil
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()

	listKinds := map[schema.GroupVersionResource]string{}
	var objects []runtime.Object
	for _, r := range crdcompat.Resources {
		listKinds[r.GVR(crdcompat.StorageVersion)] = r.Kind + "List"
		raw, err := os.ReadFile(filepath.Join("..", "..", "test", "fixtures", "v1alpha1", strings.ToLower(r.Kind)+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		asJSON, err := yaml.YAMLToJSON(raw)
		if err != nil {
			t.Fatal(err)
		}
		obj := &unstructured.Unstructured{}
		if err := obj.UnmarshalJSON(asJSON); err != nil {
			t.Fatal(err)
		}
		obj.SetAPIVersion(crdcompat.Group + "/" + crdcompat.StorageVersion)
		obj.SetNamespace("orders")
		objects = append(objects, obj)
	}
	return c, dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objects...)
}

func storedVersions(t *testing.T, c client.Client, name string) string {
	t.Helper()
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, crd); err != nil {
		t.Fatal(err)
	}
	return strings.Join(crd.Status.StoredVersions, ",")
}

func TestRun_MigratesEveryKind(t *testing.T) {
	c, dyn := fakeCluster(t)
	var out bytes.Buffer
	if err := Run(context.Background(), Options{Client: c, Dynamic: dyn, Out: &out}); err != nil {
		t.Fatalf("Run: %v\n%s", err, out.String())
	}
	for _, r := range crdcompat.Resources {
		if got := storedVersions(t, c, r.CRDName()); got != crdcompat.StorageVersion {
			t.Errorf("%s storedVersions = [%s]", r.CRDName(), got)
		}
	}
	updates := 0
	for _, a := range dyn.Actions() {
		switch a.GetVerb() {
		case "patch":
			updates++
		case "delete", "create", "update", "delete-collection":
			t.Errorf("the migration made a %s of %s: it only ever writes an object back", a.GetVerb(), a.GetResource().Resource)
		}
	}
	if updates != len(crdcompat.Resources) {
		t.Errorf("%d objects were written back, want %d", updates, len(crdcompat.Resources))
	}
}

// U-12: a write that keeps losing to another writer is not a schema
// problem. It is reported as such, the kind's storedVersions stays as it
// was, and the kinds that did finish are trimmed.
func TestRun_PersistentConflictIsTransient(t *testing.T) {
	c, dyn := fakeCluster(t)
	busy := crdcompat.Resources[0]
	attempts := 0
	dyn.PrependReactor("patch", busy.Plural, func(clienttesting.Action) (bool, runtime.Object, error) {
		attempts++
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: crdcompat.Group, Resource: busy.Plural}, "playground", errors.New("the object has been modified"))
	})
	var out bytes.Buffer
	err := Run(context.Background(), Options{Client: c, Dynamic: dyn, Out: &out, conflictRetries: 3, conflictBackoff: time.Millisecond})
	var refusal *Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("Run = %v, want a refusal\n%s", err, out.String())
	}
	if attempts != 4 {
		t.Errorf("%d write attempts, want the first and 3 retries", attempts)
	}
	report := out.String()
	for _, want := range []string{
		"MysqlFailoverGroup orders/playground: still changing after 4 attempts",
		"this is not a schema problem, run the command again",
		"Not finished:",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("the report lacks %q:\n%s", want, report)
		}
	}
	if strings.Contains(report, "do not conform") {
		t.Errorf("a conflict is reported as an object that does not conform:\n%s", report)
	}
	if got := storedVersions(t, c, busy.CRDName()); got != crdcompat.DeprecatedVersion+","+crdcompat.StorageVersion {
		t.Errorf("%s storedVersions = [%s]: it was trimmed although an object was not rewritten", busy.CRDName(), got)
	}
	for _, r := range crdcompat.Resources[1:] {
		if got := storedVersions(t, c, r.CRDName()); got != crdcompat.StorageVersion {
			t.Errorf("%s storedVersions = [%s], want it trimmed: every object of the kind was rewritten", r.CRDName(), got)
		}
	}
}

// A write the API server refuses for another reason is reported with the
// server's message, and nothing is trimmed for that kind.
func TestRun_RefusedWriteKeepsStoredVersions(t *testing.T) {
	c, dyn := fakeCluster(t)
	kind := crdcompat.Resources[1]
	dyn.PrependReactor("patch", kind.Plural, func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: crdcompat.Group, Resource: kind.Plural}, "upgrade-e2e", errors.New("denied by a policy"))
	})
	var out bytes.Buffer
	err := Run(context.Background(), Options{Client: c, Dynamic: dyn, Out: &out})
	var refusal *Refusal
	if !errors.As(err, &refusal) || !strings.Contains(out.String(), "the API server refused the write") || !strings.Contains(out.String(), "denied by a policy") {
		t.Fatalf("Run = %v, want the refused write reported:\n%s", err, out.String())
	}
	if got := storedVersions(t, c, kind.CRDName()); got != crdcompat.DeprecatedVersion+","+crdcompat.StorageVersion {
		t.Errorf("%s storedVersions = [%s]", kind.CRDName(), got)
	}
}

// A CRD that does not store the version this command migrates to refuses
// the whole run: the release's CRDs come first.
func TestRun_RefusesCRDsThatDoNotStoreTheTargetVersion(t *testing.T) {
	c, dyn := fakeCluster(t)
	crd := &apiextensionsv1.CustomResourceDefinition{}
	name := crdcompat.Resources[2].CRDName()
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, crd); err != nil {
		t.Fatal(err)
	}
	for i := range crd.Spec.Versions {
		crd.Spec.Versions[i].Storage = crd.Spec.Versions[i].Name == crdcompat.DeprecatedVersion
	}
	if err := c.Update(context.Background(), crd); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := Run(context.Background(), Options{Client: c, Dynamic: dyn, Out: &out})
	var refusal *Refusal
	if !errors.As(err, &refusal) || !strings.Contains(out.String(), name+" stores objects as v1alpha1, and this command migrates to v1beta1") ||
		!strings.Contains(out.String(), "Nothing was changed") {
		t.Fatalf("Run = %v:\n%s", err, out.String())
	}
	for _, a := range dyn.Actions() {
		if a.GetVerb() != "list" && a.GetVerb() != "get" {
			t.Errorf("a refused run made a %s", a.GetVerb())
		}
	}
}
