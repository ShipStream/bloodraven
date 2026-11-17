// Command crd-served-versions adds the deprecated shipstream.io/v1alpha1
// version to the CRDs controller-gen generates.
//
// The Go types live in api/v1beta1 only, so controller-gen writes CRDs with
// one version. Until v1alpha1 is removed, every CRD also serves v1alpha1
// with the same schema and conversion strategy None (decision D1 of
// proposals/20-api-v1beta1.md): the API server then only rewrites
// apiVersion between the two, and no conversion webhook is needed. This
// command writes that second version as a copy of the first, so the two
// schemas cannot differ. `make manifests` runs it after controller-gen.
//
// The copy is made on the YAML text, not on a decoded object, so that the
// v1beta1 entry stays byte for byte what controller-gen wrote. The result
// is then decoded and checked (see check).
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/yaml"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
)

var (
	// storageVersion is the version controller-gen generates and the API
	// server stores.
	storageVersion = v1beta1.GroupVersion.Version
	// deprecatedVersion is served with the same schema and marked
	// deprecated.
	deprecatedVersion = v1beta1.DeprecatedGroupVersion.Version
)

func deprecationWarning(kind string) string {
	return fmt.Sprintf("shipstream.io/%s %s is deprecated; use shipstream.io/%s %s, which has the same schema",
		deprecatedVersion, kind, storageVersion, kind)
}

var (
	versionsLine = regexp.MustCompile(`(?m)^  versions:\n`)
	nameLine     = regexp.MustCompile(`(?m)^(  - |    )name: ` + storageVersion + `\n`)
	storageLine  = regexp.MustCompile(`(?m)^    storage: true\n`)
)

func main() {
	dir := flag.String("dir", "config/crd/bases", "directory of the CRDs controller-gen generated")
	flag.Parse()
	files, err := filepath.Glob(filepath.Join(*dir, "*.yaml"))
	if err != nil || len(files) == 0 {
		fail("no CRD files in %s (%v)", *dir, err)
	}
	for _, f := range files {
		in, err := os.ReadFile(f)
		if err != nil {
			fail("%v", err)
		}
		out, err := addDeprecatedVersion(in)
		if err != nil {
			fail("%s: %v", f, err)
		}
		if err := os.WriteFile(f, out, 0o644); err != nil {
			fail("%v", err)
		}
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "crd-served-versions: "+format+"\n", args...)
	os.Exit(1)
}

// addDeprecatedVersion returns the CRD in with a second entry in
// spec.versions: the first entry's text, renamed, not stored and marked
// deprecated. in must be controller-gen output with exactly one version,
// the storage version.
func addDeprecatedVersion(in []byte) ([]byte, error) {
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(in, &crd); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Name != storageVersion {
		var names []string
		for _, v := range crd.Spec.Versions {
			names = append(names, v.Name)
		}
		return nil, fmt.Errorf("want controller-gen output with the single version %s, found %v (run controller-gen first; this command is not idempotent)", storageVersion, names)
	}

	loc := versionsLine.FindIndex(in)
	if loc == nil {
		return nil, fmt.Errorf("no spec.versions line")
	}
	// spec.versions is the last key of the last top-level key, so its one
	// entry runs to the end of the file.
	head, entry := in[:loc[1]], in[loc[1]:]
	if !bytes.HasPrefix(entry, []byte("  - ")) || bytes.Contains(entry, []byte("\n  - ")) {
		return nil, fmt.Errorf("spec.versions is not one entry that ends the file")
	}
	for _, line := range bytes.Split(bytes.TrimRight(entry, "\n"), []byte("\n")) {
		if len(line) > 0 && !bytes.HasPrefix(line, []byte("  - ")) && !bytes.HasPrefix(line, []byte("    ")) {
			return nil, fmt.Errorf("spec.versions is followed by another key: %q", line)
		}
	}
	if n := len(nameLine.FindAll(entry, -1)); n != 1 {
		return nil, fmt.Errorf("found %d name lines of the version entry, want 1", n)
	}
	if n := len(storageLine.FindAll(entry, -1)); n != 1 {
		return nil, fmt.Errorf("found %d storage lines of the version entry, want 1", n)
	}

	// The keys of an entry are sorted, and "deprecated" and
	// "deprecationWarning" sort right before "name".
	// A JSON string is a YAML double-quoted scalar, on one line.
	warning := strconv.Quote(deprecationWarning(crd.Spec.Names.Kind))
	deprecated := nameLine.ReplaceAll(entry, []byte(
		"${1}deprecated: true\n    deprecationWarning: "+strings.ReplaceAll(warning, "$", "$$")+"\n    name: "+deprecatedVersion+"\n"))
	deprecated = storageLine.ReplaceAll(deprecated, []byte("    storage: false\n"))

	out := append(append(append([]byte{}, head...), entry...), deprecated...)
	return out, check(out)
}

// check decodes the result and verifies what the text edit was meant to
// produce: two served versions with the same schema, printer columns and
// subresources, of which only the first is stored and only the second is
// deprecated.
func check(out []byte) error {
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(out, &crd); err != nil {
		return fmt.Errorf("the result does not decode: %w", err)
	}
	if len(crd.Spec.Versions) != 2 {
		return fmt.Errorf("the result has %d versions, want 2", len(crd.Spec.Versions))
	}
	stored, old := crd.Spec.Versions[0], crd.Spec.Versions[1]
	switch {
	case stored.Name != storageVersion || !stored.Served || !stored.Storage || stored.Deprecated:
		return fmt.Errorf("the first version is %+v, want %s served and stored", versionSummary(stored), storageVersion)
	case old.Name != deprecatedVersion || !old.Served || old.Storage || !old.Deprecated || old.DeprecationWarning == nil:
		return fmt.Errorf("the second version is %+v, want %s served, not stored, deprecated", versionSummary(old), deprecatedVersion)
	case !equality.Semantic.DeepEqual(stored.Schema, old.Schema):
		return fmt.Errorf("the two versions' schemas differ")
	case !equality.Semantic.DeepEqual(stored.AdditionalPrinterColumns, old.AdditionalPrinterColumns):
		return fmt.Errorf("the two versions' printer columns differ")
	case !equality.Semantic.DeepEqual(stored.Subresources, old.Subresources):
		return fmt.Errorf("the two versions' subresources differ")
	}
	return nil
}

func versionSummary(v apiextensionsv1.CustomResourceDefinitionVersion) string {
	return fmt.Sprintf("{name:%s served:%v storage:%v deprecated:%v}", v.Name, v.Served, v.Storage, v.Deprecated)
}
