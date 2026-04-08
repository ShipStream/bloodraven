//go:build envtest

// Package envtest validates real controller-runtime behavior against a test
// API server. This layer catches wiring issues that fake-client tests miss.
package envtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

var (
	testEnv   *envtest.Environment
	k8sClient client.Client
	cfg       *rest.Config
	scheme    *runtime.Scheme
	ctx       context.Context
	cancel    context.CancelFunc
)

func TestMain(m *testing.M) {
	log.SetLogger(zap.New(zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.Background())

	scheme = runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
		},
		Scheme: scheme,
	}

	var err error
	cfg, err = testEnv.Start()
	if err != nil {
		panic("failed to start envtest: " + err.Error())
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic("failed to create client: " + err.Error())
	}

	code := m.Run()

	cancel()
	if err := testEnv.Stop(); err != nil {
		os.Exit(1)
	}
	os.Exit(code)
}
