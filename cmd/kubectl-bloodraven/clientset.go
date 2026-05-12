package main

import (
	"flag"
	"fmt"
	"os"

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
	fs.StringVar(&cf.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "path to kubeconfig (defaults to $KUBECONFIG or ~/.kube/config)")
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
func (cf *commonFlags) resolve() (*rest.Config, string, error) {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	if cf.kubeconfig != "" {
		loader.ExplicitPath = cf.kubeconfig
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
