//go:build envtest

package envtest

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
)

// statusWritesRatchet reports whether the API server at restCfg applies CRD
// validation ratcheting to writes through the status subresource. Kubernetes
// 1.33 added it (apiextensions-apiserver status_strategy.go); 1.31 and 1.32,
// the supported floor (A-115), validate the status schema and every CEL rule
// of the object, spec rules included, on each status write without it.
func statusWritesRatchet(t *testing.T, restCfg *rest.Config) bool {
	t.Helper()
	dc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		t.Fatal(err)
	}
	info, err := dc.ServerVersion()
	if err != nil {
		t.Fatal(err)
	}
	v, err := version.ParseGeneric(info.GitVersion)
	if err != nil {
		t.Fatalf("parse API server version %q: %v", info.GitVersion, err)
	}
	return v.AtLeast(version.MajorMinor(1, 33))
}
