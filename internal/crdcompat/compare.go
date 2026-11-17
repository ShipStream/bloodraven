package crdcompat

import (
	"fmt"
	"sort"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

// Difference is what Compare found between the CRD this build was generated
// with and the installed one.
type Difference struct {
	// Older lists what this build needs and the installed CRD lacks: the
	// served version, a field, an enum value, a field's type. An operator
	// must not run against such a CRD: the API server prunes the fields
	// and refuses the values it would write.
	Older []string
	// Newer lists what the installed CRD has and this build does not
	// know: a field, an enum value. It is what an operator sees between
	// the two steps of an upgrade ("CRDs first, then the chart") and after
	// a rollback of the operator alone.
	Newer []string
}

// Compare compares the storage version's schema of built (this build's CRD)
// with installed, field by field: the set of fields, each field's type and
// each enum's values. Descriptions, defaults, bounds and validation rules
// are not compared: a difference in them makes neither side older in a way
// that can be told from the schema.
func Compare(built, installed *apiextensionsv1.CustomResourceDefinition) Difference {
	var d Difference
	want := Version(built, StorageVersion)
	if want == nil || want.Schema == nil || want.Schema.OpenAPIV3Schema == nil {
		d.Older = append(d.Older, fmt.Sprintf("this build has no %s schema for %s", StorageVersion, built.Name))
		return d
	}
	have := Version(installed, StorageVersion)
	if have == nil || !have.Served {
		var served []string
		for _, v := range installed.Spec.Versions {
			if v.Served {
				served = append(served, v.Name)
			}
		}
		d.Older = append(d.Older, fmt.Sprintf("%s/%s is not served (served: %s)", Group, StorageVersion, strings.Join(served, ", ")))
		return d
	}
	if have.Schema == nil || have.Schema.OpenAPIV3Schema == nil {
		d.Older = append(d.Older, fmt.Sprintf("%s has no schema", StorageVersion))
		return d
	}
	if want.Subresources != nil && want.Subresources.Status != nil && (have.Subresources == nil || have.Subresources.Status == nil) {
		d.Older = append(d.Older, "the status subresource is not enabled")
	}
	compareSchema("", want.Schema.OpenAPIV3Schema, have.Schema.OpenAPIV3Schema, &d)
	sort.Strings(d.Older)
	sort.Strings(d.Newer)
	return d
}

func compareSchema(path string, want, have *apiextensionsv1.JSONSchemaProps, d *Difference) {
	at := path
	if at == "" {
		at = "the object"
	}
	if wt, ht := schemaType(want), schemaType(have); wt != ht {
		d.Older = append(d.Older, fmt.Sprintf("%s is %s, this build needs %s", at, ht, wt))
		return
	}
	wantEnum, haveEnum := enumValues(want), enumValues(have)
	switch {
	case len(wantEnum) > 0 && len(haveEnum) > 0:
		for _, v := range wantEnum {
			if !contains(haveEnum, v) {
				d.Older = append(d.Older, fmt.Sprintf("%s does not allow the value %s", at, v))
			}
		}
		for _, v := range haveEnum {
			if !contains(wantEnum, v) {
				d.Newer = append(d.Newer, fmt.Sprintf("%s allows the value %s", at, v))
			}
		}
	case len(haveEnum) > 0:
		d.Older = append(d.Older, fmt.Sprintf("%s is restricted to %s, this build does not restrict it", at, strings.Join(haveEnum, ", ")))
	case len(wantEnum) > 0:
		d.Newer = append(d.Newer, fmt.Sprintf("%s is not restricted to this build's values", at))
	}
	for name, w := range want.Properties {
		child := join(path, name)
		if path == "" && name == "metadata" {
			continue
		}
		h, ok := have.Properties[name]
		if !ok {
			d.Older = append(d.Older, fmt.Sprintf("%s is missing", child))
			continue
		}
		compareSchema(child, &w, &h, d)
	}
	for name := range have.Properties {
		if _, ok := want.Properties[name]; !ok {
			d.Newer = append(d.Newer, fmt.Sprintf("%s is not known to this build", join(path, name)))
		}
	}
	if want.Items != nil && want.Items.Schema != nil && have.Items != nil && have.Items.Schema != nil {
		compareSchema(path+"[]", want.Items.Schema, have.Items.Schema, d)
	}
	if want.AdditionalProperties != nil && want.AdditionalProperties.Schema != nil &&
		have.AdditionalProperties != nil && have.AdditionalProperties.Schema != nil {
		compareSchema(path+"{}", want.AdditionalProperties.Schema, have.AdditionalProperties.Schema, d)
	}
}

func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

// schemaType names what a value of s is: its JSON type, with the two
// markers that change what the API server accepts and keeps.
func schemaType(s *apiextensionsv1.JSONSchemaProps) string {
	t := s.Type
	switch {
	case s.XIntOrString:
		t = "int-or-string"
	case t == "":
		t = "untyped"
	}
	if s.Format != "" && (s.Type == "integer" || s.Type == "number") {
		t += "(" + s.Format + ")"
	}
	if s.XPreserveUnknownFields != nil && *s.XPreserveUnknownFields {
		t += " with unknown fields kept"
	}
	return t
}

func enumValues(s *apiextensionsv1.JSONSchemaProps) []string {
	out := make([]string, 0, len(s.Enum))
	for _, v := range s.Enum {
		out = append(out, string(v.Raw))
	}
	return out
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// Summary joins up to max entries of list, saying how many more there are.
func Summary(list []string, max int) string {
	if len(list) <= max {
		return strings.Join(list, "; ")
	}
	return fmt.Sprintf("%s; and %d more", strings.Join(list[:max], "; "), len(list)-max)
}
