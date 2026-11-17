// Package crdcompat answers two questions about a cluster's Bloodraven
// CRDs and the objects stored under them, for the operator's startup check
// and for `kubectl bloodraven migrate-storage` (proposals/20-api-v1beta1.md,
// C-7 and C-14, requirements U-10 to U-13):
//
//   - Are the installed CRDs older or newer than the ones this build was
//     generated with (Compare)?
//   - Does every stored object conform to the installed schema (Validator,
//     CheckObjects)?
//
// The object check validates what the API server returns for a read against
// the installed schema as a create would be validated. It does not go
// through an update, because an update ratchets: since Kubernetes 1.30 the
// API server accepts every schema error on a field the update did not
// change, type and required-field errors included, so a no-op or dry-run
// update of an object from before a breaking schema change succeeds.
package crdcompat

import (
	"fmt"
	"io/fs"
	"sort"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	crdfiles "github.com/shipstream/bloodraven/config/crd"
)

// Group is the API group of every Bloodraven kind.
const Group = "shipstream.io"

var (
	// StorageVersion is the version this build reads and writes, and the
	// one the API server stores.
	StorageVersion = v1beta1.GroupVersion.Version
	// DeprecatedVersion is the version served beside the storage version
	// with the same schema until it is removed.
	DeprecatedVersion = v1beta1.DeprecatedGroupVersion.Version
)

// Resource is one Bloodraven kind.
type Resource struct {
	Kind   string
	Plural string
}

// Resources lists the five kinds, in the order they are checked and
// reported.
var Resources = []Resource{
	{Kind: "MysqlFailoverGroup", Plural: "mysqlfailovergroups"},
	{Kind: "MysqlDatabase", Plural: "mysqldatabases"},
	{Kind: "MysqlBackup", Plural: "mysqlbackups"},
	{Kind: "MysqlBackupVerification", Plural: "mysqlbackupverifications"},
	{Kind: "MysqlStandbyCluster", Plural: "mysqlstandbyclusters"},
}

// CRDName is the name of r's CustomResourceDefinition.
func (r Resource) CRDName() string { return r.Plural + "." + Group }

// GVR is r at version.
func (r Resource) GVR(version string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: Group, Version: version, Resource: r.Plural}
}

// BuiltCRDs returns the CRDs this build was generated with, by CRD name.
func BuiltCRDs() (map[string]*apiextensionsv1.CustomResourceDefinition, error) {
	files, err := fs.Glob(crdfiles.Bases, "bases/*.yaml")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	out := make(map[string]*apiextensionsv1.CustomResourceDefinition, len(files))
	for _, f := range files {
		raw, err := fs.ReadFile(crdfiles.Bases, f)
		if err != nil {
			return nil, err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.UnmarshalStrict(raw, crd); err != nil {
			return nil, fmt.Errorf("embedded CRD %s: %w", f, err)
		}
		out[crd.Name] = crd
	}
	for _, r := range Resources {
		if out[r.CRDName()] == nil {
			return nil, fmt.Errorf("no embedded CRD for %s", r.CRDName())
		}
	}
	return out, nil
}

// Version returns crd's entry for version, or nil.
func Version(crd *apiextensionsv1.CustomResourceDefinition, version string) *apiextensionsv1.CustomResourceDefinitionVersion {
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == version {
			return &crd.Spec.Versions[i]
		}
	}
	return nil
}

// StoredAs returns the version the API server stores crd's objects at.
func StoredAs(crd *apiextensionsv1.CustomResourceDefinition) string {
	for _, v := range crd.Spec.Versions {
		if v.Storage {
			return v.Name
		}
	}
	return ""
}
