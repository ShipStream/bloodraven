package controller

import (
	"sort"
	"strings"
	"testing"
)

// unboundedLeaves returns the path of every string without a maxLength or
// an enum, int-or-string (a Quantity) without a maxLength, and list
// without a maxItems, at or below s. The CEL cost estimator sizes such a
// value by the request size limit, so a rule that compares it is costed as
// if it were megabytes long.
func unboundedLeaves(s apiCRDSchema, path string, out *[]string) {
	switch {
	case s.IntOrString:
		if s.MaxLength == nil {
			*out = append(*out, path+" (int-or-string)")
		}
	case s.Type == "string":
		if s.MaxLength == nil && len(s.Enum) == 0 {
			*out = append(*out, path)
		}
	case s.Type == "array":
		if s.MaxItems == nil {
			*out = append(*out, path+" (list)")
		}
		if s.Items != nil {
			unboundedLeaves(*s.Items, path+"[]", out)
		}
	}
	for name, p := range s.Properties {
		unboundedLeaves(p, path+"."+name, out)
	}
}

// TestTransitionRuleInputsAreBounded: every string and list the kind-root
// transition rules compare has a maxLength or maxItems, so each rule's
// estimated cost stays within the per-rule budget (the envtest that
// installs the CRDs fails on a cost error too).
func TestTransitionRuleInputsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		file  string
		paths [][]string
	}{
		{"shipstream.io_mysqlfailovergroups.yaml", [][]string{
			{"spec", "initFromBackup"},
			{"spec", "sites", "[]", "name"},
			{"spec", "sites", "[]", "zone"},
			{"spec", "sites", "[]", "storage"},
			{"spec", "encryptionAtRest", "keyring", "dataFileDir"},
			{"spec", "encryptionAtRest", "keyring", "mysqldDir"},
			{"spec", "encryptionAtRest", "keyring", "pluginDir"},
			{"spec", "backup", "profiles", "[]", "name"},
			{"spec", "backup", "profiles", "[]", "storage"},
			{"spec", "backup", "profiles", "[]", "encryption"},
			{"spec", "backup", "pitr", "profileName"},
		}},
		{"shipstream.io_mysqlbackups.yaml", [][]string{
			{"spec", "failoverGroupRef"},
			{"spec", "profileName"},
			{"spec", "sourceSite"},
		}},
		{"shipstream.io_mysqlbackupverifications.yaml", [][]string{
			{"spec", "failoverGroupRef"},
			{"spec", "profileName"},
			{"spec", "backupRef"},
			{"spec", "pointInTime"},
			{"spec", "sanityCheck"},
		}},
	} {
		for crdPath, root := range apiCRDRoots(t, tc.file) {
			var unbounded []string
			for _, p := range tc.paths {
				s, ok := root.at(p)
				if !ok {
					t.Errorf("%s: no schema at %s", crdPath, strings.Join(p, "."))
					continue
				}
				unboundedLeaves(s, strings.Join(p, "."), &unbounded)
			}
			sort.Strings(unbounded)
			for _, u := range unbounded {
				t.Errorf("%s: %s has no maxLength or maxItems", crdPath, u)
			}
		}
	}
}
