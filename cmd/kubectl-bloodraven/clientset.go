package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// commonFlags is the bag of kubeconfig/namespace/output flags shared by
// every subcommand. It registers the flags onto fs and returns a
// resolver that produces a controller-runtime client plus the
// effective namespace.
type commonFlags struct {
	kubeconfig string
	context    string
	namespace  string
	output     string
}

func registerCommonFlags(fs *flag.FlagSet, cf *commonFlags) {
	// Leave the default empty so resolve() can defer to
	// NewDefaultClientConfigLoadingRules(), which already honours
	// $KUBECONFIG including the colon/semicolon-separated path-list
	// form (`a:b:c`). Defaulting to os.Getenv("KUBECONFIG") and then
	// stuffing the value into loader.ExplicitPath would break that case
	// — ExplicitPath only accepts a single file.
	fs.StringVar(&cf.kubeconfig, "kubeconfig", "", "path to kubeconfig (defaults to $KUBECONFIG or ~/.kube/config; accepts a single file)")
	fs.StringVar(&cf.context, "context", "", "kubeconfig context to use (default: current-context)")
	fs.StringVar(&cf.namespace, "namespace", "", "namespace (default: context namespace, or \"default\")")
	fs.StringVar(&cf.namespace, "n", "", "namespace (short flag for --namespace)")
	fs.StringVar(&cf.output, "output", "table", `output format: "table", "wide", "json", or "yaml" (where supported)`)
	fs.StringVar(&cf.output, "o", "table", "output format (short flag for --output)")
}

// resolve builds the rest config and namespace from common flags,
// reading defaults from kubeconfig the same way kubectl does. We use
// clientcmd directly rather than client-go's `InClusterConfig`
// fallback because this plugin always runs outside the cluster.
//
// Kubeconfig source precedence (mirrors kubectl):
//  1. --kubeconfig (if a single path is given, set as ExplicitPath; if
//     the user explicitly hands us a path-list, split it into the
//     loader Precedence the same way client-go does for $KUBECONFIG).
//  2. $KUBECONFIG via NewDefaultClientConfigLoadingRules() — already
//     handles the path-list form natively.
//  3. ~/.kube/config (the default home rule from the loader).
func (cf *commonFlags) resolve() (*rest.Config, string, error) {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	if cf.kubeconfig != "" {
		if paths := splitKubeconfigPathList(cf.kubeconfig); len(paths) > 1 {
			loader.Precedence = paths
		} else {
			loader.ExplicitPath = cf.kubeconfig
		}
	}
	overrides := &clientcmd.ConfigOverrides{}
	if cf.context != "" {
		overrides.CurrentContext = cf.context
	}
	cb := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides)

	ns := cf.namespace
	if ns == "" {
		var err error
		ns, _, err = cb.Namespace()
		if err != nil || ns == "" {
			ns = "default"
		}
	}
	cfg, err := cb.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load kubeconfig: %w", err)
	}
	return cfg, ns, nil
}

// newClient builds a controller-runtime client with the bloodraven
// CRDs registered. Returns the client, the resolved namespace, and any
// error encountered.
func (cf *commonFlags) newClient() (client.Client, string, error) {
	cfg, ns, err := cf.resolve()
	if err != nil {
		return nil, "", err
	}
	scheme := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, "", fmt.Errorf("build client: %w", err)
	}
	return cl, ns, nil
}

// splitKubeconfigPathList splits a $KUBECONFIG-style path list using the
// OS-appropriate separator (`:` on Unix, `;` on Windows). Empty segments
// are dropped, matching client-go's loader behaviour. Returns a single-
// element slice when the input contains no separator so callers can use
// `len(...) > 1` as a quick "is this a list" check.
func splitKubeconfigPathList(s string) []string {
	parts := strings.Split(s, string(filepath.ListSeparator))
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
