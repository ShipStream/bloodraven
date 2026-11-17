// Package kube wires up the Kubernetes clients used by the chaos
// runner and enforces the playground-context safety guard.
package kube

import (
	"fmt"
	"os"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
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
	// APIVersion is the shipstream.io version Controller reads and writes
	// the Bloodraven kinds at: v1beta1, or the deprecated v1alpha1 when
	// the cluster does not serve v1beta1 yet (see apiGroupVersion).
	APIVersion schema.GroupVersion
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

	// Raise the client-side rate limits well above the client-go defaults
	// (5 QPS / 10 burst). The chaos runner fans out many short-lived reads
	// and patches (poll loops, log tailers, cleanup reverters, reset's
	// scale/sync/patch sequence) against a single client; at the defaults
	// those queue behind the limiter and surface as
	//   "client rate limiter Wait returned an error: context deadline exceeded"
	// which fails cleanup/reset and aborts otherwise-passing E2E runs.
	if restCfg.QPS < 50 {
		restCfg.QPS = 50
	}
	if restCfg.Burst < 100 {
		restCfg.Burst = 100
	}

	// Print each API server warning once. The upgrade E2E talks the
	// deprecated version for a whole run, and the default handler would
	// repeat its deprecation warning on every request.
	if restCfg.WarningHandler == nil {
		restCfg.WarningHandler = rest.NewWarningWriter(os.Stderr, rest.WarningWriterOptions{Deduplicate: true})
	}

	kc, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}

	gv := apiGroupVersion(kc.Discovery())
	scheme, err := newScheme(gv)
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
		APIVersion:    gv,
	}, nil
}

// apiGroupVersion picks the shipstream.io version the runner talks: the
// one the Go types are written for, unless the cluster serves only the
// deprecated one. That is the upgrade E2E, which starts on a release from
// before v1.8.0 and upgrades it under the running scenario; both versions
// have one schema, and the deprecated one stays served after the upgrade,
// so the client chosen at the start works to the end.
//
// A cluster that cannot be asked gets the default: the commands that need
// no cluster (list) must still work.
func apiGroupVersion(disc discovery.DiscoveryInterface) schema.GroupVersion {
	if _, err := disc.ServerResourcesForGroupVersion(v1beta1.GroupVersion.String()); !apierrors.IsNotFound(err) {
		return v1beta1.GroupVersion
	}
	if _, err := disc.ServerResourcesForGroupVersion(v1beta1.DeprecatedGroupVersion.String()); err == nil {
		return v1beta1.DeprecatedGroupVersion
	}
	return v1beta1.GroupVersion
}

// newScheme returns the runner's scheme with the Bloodraven kinds
// registered at gv.
func newScheme(gv schema.GroupVersion) (*runtimeScheme, error) {
	s := defaultScheme()
	return s, v1beta1.AddToSchemeAt(s, gv)
}
