package doctorscripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// leaseSchemaVersionLine is how deployment-probe.sh declares the Lease
// record schemaVersion it reports as readable.
var leaseSchemaVersionLine = regexp.MustCompile(`(?m)^LEASE_RECORD_SCHEMA_VERSION=(\d+)$`)

// TestDeploymentProbeLeaseSchemaVersionMatchesOperator keeps the probe's
// readable flag on the record version the operator decodes: a stale value
// would tell an operator to delete a Lease the running release reads.
func TestDeploymentProbeLeaseSchemaVersionMatchesOperator(t *testing.T) {
	m := leaseSchemaVersionLine.FindStringSubmatch(readScript(t, "deployment-probe.sh"))
	if m == nil {
		t.Fatal("deployment-probe.sh: LEASE_RECORD_SCHEMA_VERSION=<n> not found; update the test with the script")
	}
	_, file, _, _ := runtime.Caller(0)
	src := filepath.Join(filepath.Dir(file), "..", "controller", "deployment_leases.go")
	f, err := parser.ParseFile(token.NewFileSet(), src, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var want string
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if name.Name == "deploymentLeaseRecordSchemaVersion" && i < len(vs.Values) {
				if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.INT {
					want = lit.Value
				}
			}
		}
		return true
	})
	if want == "" {
		t.Fatalf("%s: deploymentLeaseRecordSchemaVersion integer constant not found", src)
	}
	if m[1] != want {
		t.Errorf("deployment-probe.sh LEASE_RECORD_SCHEMA_VERSION=%s, operator reads schemaVersion %s", m[1], want)
	}
}
