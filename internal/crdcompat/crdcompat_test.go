package crdcompat

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"
)

// maxCRDBytes bounds a CRD's compact JSON with every served version (C-4
// of proposals/20-api-v1beta1.md). etcd's default request limit is 1.5
// MiB, and a server-side apply adds managedFields of about the size of
// the object's key set, so 900 KB leaves room for both.
const maxCRDBytes = 900 * 1000

func builtCRDs(t *testing.T) map[string]*apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	crds, err := BuiltCRDs()
	if err != nil {
		t.Fatal(err)
	}
	return crds
}

// The D1 invariant: both versions are served with one schema, so the API
// server converts between them by rewriting apiVersion only (strategy
// None). The first difference between the two schemas means a conversion
// webhook is required before that change can ship.
func TestServedVersionsShareOneSchema(t *testing.T) {
	for _, r := range Resources {
		crd := builtCRDs(t)[r.CRDName()]
		if crd.Spec.Conversion != nil && crd.Spec.Conversion.Strategy != apiextensionsv1.NoneConverter {
			t.Errorf("%s: conversion strategy %q, want None", crd.Name, crd.Spec.Conversion.Strategy)
		}
		if len(crd.Spec.Versions) != 2 {
			t.Fatalf("%s: %d versions, want %s and %s", crd.Name, len(crd.Spec.Versions), StorageVersion, DeprecatedVersion)
		}
		stored, old := Version(crd, StorageVersion), Version(crd, DeprecatedVersion)
		if stored == nil || old == nil {
			t.Fatalf("%s: want versions %s and %s", crd.Name, StorageVersion, DeprecatedVersion)
		}
		if !stored.Served || !stored.Storage || stored.Deprecated {
			t.Errorf("%s: %s must be served, stored and not deprecated: %+v", crd.Name, StorageVersion, summary(stored))
		}
		if !old.Served || old.Storage || !old.Deprecated || old.DeprecationWarning == nil || !strings.Contains(*old.DeprecationWarning, StorageVersion) {
			t.Errorf("%s: %s must be served, not stored, and deprecated with a warning that names %s: %+v",
				crd.Name, DeprecatedVersion, StorageVersion, summary(old))
		}
		if !equality.Semantic.DeepEqual(stored.Schema, old.Schema) {
			t.Errorf("%s: the schemas of %s and %s differ. Under conversion strategy None they must be identical; a real difference needs a conversion webhook first (C-1)",
				crd.Name, StorageVersion, DeprecatedVersion)
		}
		if !equality.Semantic.DeepEqual(stored.Subresources, old.Subresources) ||
			!equality.Semantic.DeepEqual(stored.AdditionalPrinterColumns, old.AdditionalPrinterColumns) {
			t.Errorf("%s: the subresources or printer columns of %s and %s differ", crd.Name, StorageVersion, DeprecatedVersion)
		}
	}
}

func summary(v *apiextensionsv1.CustomResourceDefinitionVersion) map[string]any {
	return map[string]any{"name": v.Name, "served": v.Served, "storage": v.Storage, "deprecated": v.Deprecated}
}

func TestCRDsFitTheRequestLimit(t *testing.T) {
	for name, crd := range builtCRDs(t) {
		raw, err := json.Marshal(crd)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d bytes of compact JSON", name, len(raw))
		if len(raw) > maxCRDBytes {
			t.Errorf("%s is %d bytes of compact JSON with every served version, over the %d limit: an apply of it can exceed etcd's request limit",
				name, len(raw), maxCRDBytes)
		}
	}
}

// The chart installs the same CRDs the build embeds.
func TestChartCRDsMatchTheBuild(t *testing.T) {
	for _, r := range Resources {
		name := "shipstream.io_" + r.Plural + ".yaml"
		generated, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", name))
		if err != nil {
			t.Fatal(err)
		}
		chart, err := os.ReadFile(filepath.Join("..", "..", "charts", "bloodraven", "crds", name))
		if err != nil {
			t.Fatal(err)
		}
		if string(generated) != string(chart) {
			t.Errorf("charts/bloodraven/crds/%s differs from config/crd/bases/%s; run `make manifests`", name, name)
		}
	}
}

func TestCompare(t *testing.T) {
	mfg := Resources[0].CRDName()
	edit := func(fn func(root *apiextensionsv1.JSONSchemaProps)) *apiextensionsv1.CustomResourceDefinition {
		crd := builtCRDs(t)[mfg].DeepCopy()
		fn(Version(crd, StorageVersion).Schema.OpenAPIV3Schema)
		return crd
	}
	spec := func(root *apiextensionsv1.JSONSchemaProps) apiextensionsv1.JSONSchemaProps {
		return root.Properties["spec"]
	}
	built := builtCRDs(t)[mfg]

	t.Run("same", func(t *testing.T) {
		if d := Compare(built, built.DeepCopy()); len(d.Older) != 0 || len(d.Newer) != 0 {
			t.Fatalf("a CRD differs from itself: %+v", d)
		}
	})
	t.Run("description only", func(t *testing.T) {
		installed := edit(func(root *apiextensionsv1.JSONSchemaProps) {
			s := spec(root)
			s.Description = "reworded"
			root.Properties["spec"] = s
		})
		if d := Compare(built, installed); len(d.Older) != 0 || len(d.Newer) != 0 {
			t.Fatalf("a reworded description is a difference: %+v", d)
		}
	})
	t.Run("installed lacks a field", func(t *testing.T) {
		installed := edit(func(root *apiextensionsv1.JSONSchemaProps) {
			status := root.Properties["status"]
			delete(status.Properties, "rollout")
		})
		d := Compare(built, installed)
		if len(d.Older) != 1 || d.Older[0] != "status.rollout is missing" || len(d.Newer) != 0 {
			t.Fatalf("difference = %+v, want status.rollout missing", d)
		}
	})
	t.Run("installed has a field more", func(t *testing.T) {
		installed := edit(func(root *apiextensionsv1.JSONSchemaProps) {
			spec(root).Properties["futureField"] = apiextensionsv1.JSONSchemaProps{Type: "string"}
		})
		d := Compare(built, installed)
		if len(d.Older) != 0 || len(d.Newer) != 1 || d.Newer[0] != "spec.futureField is not known to this build" {
			t.Fatalf("difference = %+v, want one newer field", d)
		}
	})
	t.Run("enum values", func(t *testing.T) {
		installed := edit(func(root *apiextensionsv1.JSONSchemaProps) {
			sites := root.Properties["status"].Properties["sites"]
			state := sites.Items.Schema.Properties["recoveryState"]
			var kept []apiextensionsv1.JSON
			for _, v := range state.Enum {
				if string(v.Raw) != `"RecloneRequired"` {
					kept = append(kept, v)
				}
			}
			if len(kept) == len(state.Enum) {
				t.Fatal("the test's enum value is gone from the schema")
			}
			state.Enum = append(kept, apiextensionsv1.JSON{Raw: []byte(`"FutureState"`)})
			sites.Items.Schema.Properties["recoveryState"] = state
		})
		d := Compare(built, installed)
		if len(d.Older) != 1 || !strings.Contains(d.Older[0], `status.sites[].recoveryState does not allow the value "RecloneRequired"`) {
			t.Fatalf("older = %v, want the missing enum value", d.Older)
		}
		if len(d.Newer) != 1 || !strings.Contains(d.Newer[0], `"FutureState"`) {
			t.Fatalf("newer = %v, want the extra enum value", d.Newer)
		}
	})
	t.Run("type", func(t *testing.T) {
		installed := edit(func(root *apiextensionsv1.JSONSchemaProps) {
			s := spec(root)
			f := s.Properties["cloneTimeout"]
			f.Type = "integer"
			s.Properties["cloneTimeout"] = f
		})
		d := Compare(built, installed)
		if len(d.Older) != 1 || d.Older[0] != "spec.cloneTimeout is integer, this build needs string" {
			t.Fatalf("older = %v, want the type difference", d.Older)
		}
	})
	t.Run("a CRD that serves only the deprecated version", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join("..", "..", "test", "envtest", "testdata", "crd-v1.7.0", "shipstream.io_mysqlfailovergroups.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		old := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal(raw, old); err != nil {
			t.Fatal(err)
		}
		d := Compare(built, old)
		if len(d.Older) != 1 || d.Older[0] != "shipstream.io/v1beta1 is not served (served: v1alpha1)" {
			t.Fatalf("older = %v, want the version that is not served", d.Older)
		}
	})
}

func exampleGroup(t *testing.T) map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "examples", "minimal-failovergroup.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	// Decoded as the API machinery decodes a response: integers are
	// int64, not float64.
	asJSON, err := yaml.YAMLToJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(asJSON); err != nil {
		t.Fatal(err)
	}
	return obj.Object
}

func TestValidator(t *testing.T) {
	v, err := NewValidator(builtCRDs(t)[Resources[0].CRDName()], StorageVersion)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	validate := func(edit func(obj map[string]interface{})) (structural, rules field.ErrorList) {
		obj := exampleGroup(t)
		edit(obj)
		before, _ := json.Marshal(obj)
		p, _ := v.Problem(ctx, Resources[0], &unstructured.Unstructured{Object: obj})
		if after, _ := json.Marshal(obj); string(before) != string(after) {
			t.Fatal("Validate changed the object")
		}
		return p.Structural, p.Rules
	}
	spec := func(obj map[string]interface{}) map[string]interface{} { return obj["spec"].(map[string]interface{}) }

	t.Run("the example conforms", func(t *testing.T) {
		if s, r := validate(func(map[string]interface{}) {}); len(s)+len(r) != 0 {
			t.Fatalf("structural %v, rules %v", s, r)
		}
	})
	// U-10: an object whose only incompatibility is a missing required
	// field, which an update would ratchet.
	t.Run("a required field is missing", func(t *testing.T) {
		s, _ := validate(func(obj map[string]interface{}) { delete(spec(obj), "dns") })
		if len(s) != 1 || s[0].Type != field.ErrorTypeRequired || s[0].Field != "spec.dns" {
			t.Fatalf("structural = %v, want spec.dns required", s)
		}
	})
	// U-10: an object whose only incompatibility is a type change with no
	// CEL rule on it: v1.6.0 stored cloneTimeout as an integer.
	t.Run("a field has another type", func(t *testing.T) {
		s, _ := validate(func(obj map[string]interface{}) { spec(obj)["cloneTimeout"] = int64(3600) })
		if len(s) == 0 || s[0].Type != field.ErrorTypeTypeInvalid || s[0].Field != "spec.cloneTimeout" {
			t.Fatalf("structural = %v, want spec.cloneTimeout of the wrong type", s)
		}
	})
	t.Run("a value outside an enum", func(t *testing.T) {
		s, _ := validate(func(obj map[string]interface{}) {
			spec(obj)["sites"].([]interface{})[2].(map[string]interface{})["role"] = "read-only"
		})
		if len(s) != 1 || s[0].Type != field.ErrorTypeNotSupported {
			t.Fatalf("structural = %v, want the unsupported role", s)
		}
	})
	t.Run("a duplicate list-map key", func(t *testing.T) {
		s, _ := validate(func(obj map[string]interface{}) {
			spec(obj)["sites"].([]interface{})[1].(map[string]interface{})["name"] = "iad"
		})
		found := false
		for _, e := range s {
			found = found || e.Type == field.ErrorTypeDuplicate
		}
		if !found {
			t.Fatalf("structural = %v, want the duplicate site name", s)
		}
	})
	t.Run("a value rule is not structural", func(t *testing.T) {
		s, r := validate(func(obj map[string]interface{}) { spec(obj)["cloneTimeout"] = "1ns" })
		if len(s) != 0 || len(r) == 0 {
			t.Fatalf("structural %v, rules %v: want a value-rule error only", s, r)
		}
	})
}
