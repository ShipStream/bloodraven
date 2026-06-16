package kube

import (
	"os"
	"path/filepath"
	"testing"
)

// minimalKubeconfig is a valid-but-unconnectable kubeconfig whose context
// name passes the playground guard (k3d- prefix). clientcmd builds a
// rest.Config from it without contacting the server.
const minimalKubeconfig = `apiVersion: v1
kind: Config
current-context: k3d-test
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: k3d-test
  context:
    cluster: test
    user: test
users:
- name: test
  user:
    token: deadbeef
`

// TestNewRaisesClientRateLimits asserts that New() lifts the client-side
// QPS/Burst above the client-go defaults (5/10). At the defaults the chaos
// runner's fan-out of reads/patches queues behind the limiter and surfaces
// as "client rate limiter Wait returned an error: context deadline
// exceeded", which fails cleanup/reset and aborts E2E runs.
func TestNewRaisesClientRateLimits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(path, []byte(minimalKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	c, err := New(LoadOptions{Kubeconfig: path})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if c.Config.QPS < 50 {
		t.Errorf("rest.Config.QPS = %v, want >= 50", c.Config.QPS)
	}
	if c.Config.Burst < 100 {
		t.Errorf("rest.Config.Burst = %v, want >= 100", c.Config.Burst)
	}
}
