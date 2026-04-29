// Package kube wires up the Kubernetes clients used by the chaos
// runner and enforces the playground-context safety guard.
package kube

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// PlaygroundNamespace is the default namespace used by every
// playground manifest under playground/manifests/.
const PlaygroundNamespace = "bloodraven-playground"

// FailoverGroupName is the default MysqlFailoverGroup name created by
// the playground setup.
const FailoverGroupName = "playground"

// Client bundles the typed and untyped clients the runner needs. The
// raw rest.Config is retained so packages that build their own clients
// (port-forward, log streaming) can mint dialers without re-loading
// kubeconfig.
type Client struct {
	Config        *rest.Config
	RawConfig     clientcmdapi.Config
	CurrentCtx    string
	Kubernetes    kubernetes.Interface
	Controller    client.Client
	NamespaceHint string
}

// LoadOptions controls how a Client is constructed.
type LoadOptions struct {
	// Kubeconfig overrides the default kubeconfig discovery (KUBECONFIG /
	// ~/.kube/config). Empty means use defaults.
	Kubeconfig string
	// Context selects a specific kubeconfig context. Empty means use the
	// current-context from the loaded kubeconfig.
	Context string
	// SkipGuard bypasses the playground-context allowlist. The CLI uses
	// this for the `list` subcommand only.
	SkipGuard bool
}

// New loads a Client honoring LoadOptions and (unless SkipGuard) the
// playground context allowlist.
func New(opts LoadOptions) (*Client, error) {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	if opts.Kubeconfig != "" {
		loader.ExplicitPath = opts.Kubeconfig
	}

	overrides := &clientcmd.ConfigOverrides{}
	if opts.Context != "" {
		overrides.CurrentContext = opts.Context
	}

	cfgBuilder := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides)
	raw, err := cfgBuilder.RawConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}

	currentCtx := opts.Context
	if currentCtx == "" {
		currentCtx = raw.CurrentContext
	}

	if !opts.SkipGuard {
		if err := RequirePlaygroundContext(currentCtx); err != nil {
			return nil, err
		}
	}

	restCfg, err := cfgBuilder.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("build rest config: %w", err)
	}

	kc, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}

	scheme, err := newScheme()
	if err != nil {
		return nil, fmt.Errorf("build scheme: %w", err)
	}

	cc, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("build controller client: %w", err)
	}

	return &Client{
		Config:        restCfg,
		RawConfig:     raw,
		CurrentCtx:    currentCtx,
		Kubernetes:    kc,
		Controller:    cc,
		NamespaceHint: PlaygroundNamespace,
	}, nil
}

func newScheme() (*runtimeScheme, error) {
	s := defaultScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}
