package crdcompat

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	structurallisttype "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	schemaobjectmeta "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/client-go/dynamic"
)

// Validator validates objects of one kind against one version's schema of
// an installed CRD, the way the API server validates a create: the OpenAPI
// schema (types, required fields, enums, formats, bounds), the list-map and
// set invariants, and the CEL rules that are not transition rules. Nothing
// is ratcheted, because there is no old object.
type Validator struct {
	schema     apiservervalidation.SchemaValidator
	structural *structuralschema.Structural
	cel        *cel.Validator
}

// NewValidator builds the validator for version of crd.
func NewValidator(crd *apiextensionsv1.CustomResourceDefinition, version string) (*Validator, error) {
	v := Version(crd, version)
	if v == nil || v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
		return nil, fmt.Errorf("%s has no schema for %s", crd.Name, version)
	}
	internal := &apiextensions.JSONSchemaProps{}
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v.Schema.OpenAPIV3Schema, internal, nil); err != nil {
		return nil, fmt.Errorf("%s %s: %w", crd.Name, version, err)
	}
	structural, err := structuralschema.NewStructural(internal)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", crd.Name, version, err)
	}
	schemaValidator, _, err := apiservervalidation.NewSchemaValidator(internal)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", crd.Name, version, err)
	}
	return &Validator{
		schema:     schemaValidator,
		structural: structural,
		cel:        cel.NewValidator(structural, true, celconfig.PerCallLimit),
	}, nil
}

// Validate returns what a create of obj would be refused for. obj is an
// object as the API server returns it; it is not changed. Defaults are
// applied to a copy first, as the API server does on every read and write,
// so an object taken from a file is judged like the stored one.
func (v *Validator) Validate(ctx context.Context, obj map[string]interface{}) field.ErrorList {
	obj = runtime.DeepCopyJSON(obj)
	structuraldefaulting.Default(obj, v.structural)

	var errs field.ErrorList
	errs = append(errs, apiservervalidation.ValidateCustomResource(nil, obj, v.schema)...)
	errs = append(errs, schemaobjectmeta.Validate(ctx, nil, obj, v.structural, false)...)
	errs = append(errs, structurallisttype.ValidateListSetsAndMaps(nil, v.structural, obj)...)
	if v.cel != nil && !hasBlockingError(errs) {
		// The CEL rules assume the shape the schema describes, as in the
		// API server: they are not run on an object whose list or map
		// sizes it already refuses.
		celErrs, _ := v.cel.Validate(ctx, nil, v.structural, obj, nil, celconfig.RuntimeCELCostBudget)
		errs = append(errs, celErrs...)
	}
	return errs
}

func hasBlockingError(errs field.ErrorList) bool {
	for _, err := range errs {
		if err.Type == field.ErrorTypeNotSupported || err.Type == field.ErrorTypeRequired || err.Type == field.ErrorTypeTooLong ||
			err.Type == field.ErrorTypeTooMany || err.Type == field.ErrorTypeTypeInvalid {
			return true
		}
	}
	return false
}

// Structural reports whether err means the object does not have the shape
// the schema describes: a field of another type, a required field that is
// missing, a value outside an enum, a duplicate list-map key. A typed
// client cannot decode such an object, or decodes it into something its
// author did not write. Every other error is a value rule (a bound, a
// pattern, a CEL rule) that an object can break and still be read
// correctly; the API server itself keeps accepting writes to an object that
// broke one before the rule was tightened.
func Structural(err *field.Error) bool {
	switch err.Type {
	case field.ErrorTypeTypeInvalid, field.ErrorTypeRequired, field.ErrorTypeNotSupported, field.ErrorTypeDuplicate:
		return true
	}
	return false
}

// ObjectProblem is one stored object that does not conform to the
// installed schema.
type ObjectProblem struct {
	Resource  Resource
	Namespace string
	Name      string
	// Structural and Rules split the errors as Structural does.
	Structural field.ErrorList
	Rules      field.ErrorList
}

// String names the object and its errors on one line.
func (p ObjectProblem) String() string {
	var parts []string
	for _, e := range append(append(field.ErrorList{}, p.Structural...), p.Rules...) {
		parts = append(parts, e.Error())
	}
	return fmt.Sprintf("%s %s/%s: %s", p.Resource.Kind, p.Namespace, p.Name, strings.Join(parts, "; "))
}

// Lister lists every object of a resource, in every namespace, a page at a
// time.
type Lister func(ctx context.Context, r Resource, opts metav1.ListOptions) (*unstructured.UnstructuredList, error)

// DynamicLister lists through the dynamic client at the storage version.
// The typed client is not used: it cannot decode an object that does not
// fit the schema, which is what the check is looking for.
func DynamicLister(c dynamic.Interface) Lister {
	return func(ctx context.Context, r Resource, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
		return c.Resource(r.GVR(StorageVersion)).List(ctx, opts)
	}
}

const listPageSize = 250

// ObjectCheck is the result of CheckObjects.
type ObjectCheck struct {
	// Checked counts the objects read, by kind.
	Checked map[string]int
	// Problems lists the objects that do not conform, in kind,
	// namespace and name order.
	Problems []ObjectProblem
}

// HasStructural reports whether any object has a structural error.
func (c ObjectCheck) HasStructural() bool {
	for _, p := range c.Problems {
		if len(p.Structural) > 0 {
			return true
		}
	}
	return false
}

// CheckObjects reads every object of the five kinds and validates each
// against the storage version's schema of its installed CRD. It writes
// nothing. installed maps a CRD name to the installed CRD; a kind whose CRD
// is absent from it is skipped.
func CheckObjects(ctx context.Context, list Lister, installed map[string]*apiextensionsv1.CustomResourceDefinition) (ObjectCheck, error) {
	out := ObjectCheck{Checked: map[string]int{}}
	for _, r := range Resources {
		crd := installed[r.CRDName()]
		if crd == nil {
			continue
		}
		validator, err := NewValidator(crd, StorageVersion)
		if err != nil {
			return out, err
		}
		err = EachObject(ctx, list, r, func(obj *unstructured.Unstructured) error {
			out.Checked[r.Kind]++
			if p, bad := validator.Problem(ctx, r, obj); bad {
				out.Problems = append(out.Problems, p)
			}
			return nil
		})
		if err != nil {
			return out, err
		}
	}
	sort.SliceStable(out.Problems, func(i, j int) bool {
		a, b := out.Problems[i], out.Problems[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	// Keep the kinds in Resources order (the sort above is stable within
	// what the loop appended per kind).
	sort.SliceStable(out.Problems, func(i, j int) bool {
		return resourceIndex(out.Problems[i].Resource) < resourceIndex(out.Problems[j].Resource)
	})
	return out, nil
}

func resourceIndex(r Resource) int {
	for i := range Resources {
		if Resources[i] == r {
			return i
		}
	}
	return len(Resources)
}

// Problem validates obj and returns its problem, if it has one.
func (v *Validator) Problem(ctx context.Context, r Resource, obj *unstructured.Unstructured) (ObjectProblem, bool) {
	errs := v.Validate(ctx, obj.Object)
	if len(errs) == 0 {
		return ObjectProblem{}, false
	}
	p := ObjectProblem{Resource: r, Namespace: obj.GetNamespace(), Name: obj.GetName()}
	for _, e := range errs {
		if Structural(e) {
			p.Structural = append(p.Structural, e)
		} else {
			p.Rules = append(p.Rules, e)
		}
	}
	return p, true
}

// EachObject calls fn for every object of r, in every namespace.
func EachObject(ctx context.Context, list Lister, r Resource, fn func(*unstructured.Unstructured) error) error {
	opts := metav1.ListOptions{Limit: listPageSize}
	for {
		page, err := list(ctx, r, opts)
		if err != nil {
			return fmt.Errorf("list %s: %w", r.Plural, err)
		}
		for i := range page.Items {
			if err := fn(&page.Items[i]); err != nil {
				return err
			}
		}
		if page.GetContinue() == "" {
			return nil
		}
		opts.Continue = page.GetContinue()
	}
}
