package kube

import (
	"fmt"
	"os"
	"strings"
)

// allowedExactContexts mirrors the default allowlist hard-coded in
// playground/_guard.sh.
var allowedExactContexts = []string{
	"k3d-bloodraven",
	"kind-bloodraven",
	"minikube",
}

// allowedPrefixes are the prefix matches granted automatically (any
// k3d-, kind-, or minikube-cluster name).
var allowedPrefixes = []string{"k3d-", "kind-", "minikube"}

// RequirePlaygroundContext returns nil when the supplied kubectl
// context name is on the playground allowlist, otherwise an error
// suitable for direct user display.
//
// The allowlist mirrors playground/_guard.sh so that local k3d/kind/
// minikube developers Just Work, while a kubeconfig that drifted to
// a remote prod context (the M7 audit finding) refuses to mutate.
func RequirePlaygroundContext(ctx string) error {
	if ctx == "" {
		return fmt.Errorf("playground: no kubectl current-context set — refusing to run")
	}
	for _, p := range allowedPrefixes {
		if strings.HasPrefix(ctx, p) {
			return nil
		}
	}
	allow := append([]string{}, allowedExactContexts...)
	if extra := strings.TrimSpace(os.Getenv("BLOODRAVEN_PLAYGROUND_CONTEXTS")); extra != "" {
		allow = append(allow, strings.Fields(extra)...)
	}
	for _, name := range allow {
		if ctx == name {
			return nil
		}
	}
	return fmt.Errorf(
		"playground: context %q is not in the allowlist (%s, prefixes: %s); set BLOODRAVEN_PLAYGROUND_CONTEXTS to override",
		ctx,
		strings.Join(allow, ", "),
		strings.Join(allowedPrefixes, "*, ")+"*",
	)
}
