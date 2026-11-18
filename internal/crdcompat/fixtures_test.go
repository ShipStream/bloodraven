package crdcompat

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
)

// fixtureTypes maps a kind to the Go type it decodes into.
var fixtureTypes = map[string]func() runtime.Object{
	"MysqlFailoverGroup":      func() runtime.Object { return &v1beta1.MysqlFailoverGroup{} },
	"MysqlDatabase":           func() runtime.Object { return &v1beta1.MysqlDatabase{} },
	"MysqlBackup":             func() runtime.Object { return &v1beta1.MysqlBackup{} },
	"MysqlBackupVerification": func() runtime.Object { return &v1beta1.MysqlBackupVerification{} },
	"MysqlStandbyCluster":     func() runtime.Object { return &v1beta1.MysqlStandbyCluster{} },
}

// C-6: the objects v1.7.0 stored (test/fixtures/v1alpha1, one real object
// per kind with the status its operator wrote) fit this build: they decode
// into the Go types without an unknown field, and they conform to the
// schema of both served versions. A failure here is a schema change that
// breaks objects existing clusters hold.
func TestV170FixturesFitThisBuild(t *testing.T) {
	crds := builtCRDs(t)
	for _, r := range Resources {
		t.Run(r.Kind, func(t *testing.T) {
			path := filepath.Join("..", "..", "test", "fixtures", "v1alpha1", toLower(r.Kind)+".yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("every kind needs a fixture: %v", err)
			}
			asJSON, err := yaml.YAMLToJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			obj := &unstructured.Unstructured{}
			if err := obj.UnmarshalJSON(asJSON); err != nil {
				t.Fatal(err)
			}
			if obj.GetKind() != r.Kind || obj.GetAPIVersion() != v1beta1.DeprecatedGroupVersion.String() {
				t.Fatalf("%s holds a %s %s, want a %s %s", path, obj.GetAPIVersion(), obj.GetKind(), v1beta1.DeprecatedGroupVersion, r.Kind)
			}
			if _, found, _ := unstructured.NestedMap(obj.Object, "status"); !found {
				t.Fatalf("%s has no status: the fixture must be an object the v1.7.0 operator reconciled", path)
			}

			typed := fixtureTypes[r.Kind]()
			if err := yaml.UnmarshalStrict(raw, typed); err != nil {
				t.Errorf("the v1.7.0 %s does not decode into this build's type: %v", r.Kind, err)
			}
			for _, version := range []string{StorageVersion, DeprecatedVersion} {
				v, err := NewValidator(crds[r.CRDName()], version)
				if err != nil {
					t.Fatal(err)
				}
				if p, bad := v.Problem(context.Background(), r, obj); bad {
					t.Errorf("the v1.7.0 %s does not conform to this build's %s schema: %s", r.Kind, version, p)
				}
			}
		})
	}
}

func toLower(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'A' && c <= 'Z' {
			out[i] = c + 'a' - 'A'
		}
	}
	return string(out)
}
