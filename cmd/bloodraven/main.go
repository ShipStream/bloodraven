package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/platform"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))

	scheme := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	restConfig := ctrl.GetConfigOrDie()

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: ":8081",
		Metrics: metricsserver.Options{
			BindAddress: ":8080",
		},
		LeaderElection:   true,
		LeaderElectionID: "bloodraven.shipstream.io",
	})
	if err != nil {
		logger.Error("unable to start manager", "error", err)
		os.Exit(1)
	}

	// Create a typed clientset for operations that need it (e.g. node tainting).
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		logger.Error("unable to create clientset", "error", err)
		os.Exit(1)
	}

	// Register metrics
	metrics.Register(prometheus.DefaultRegisterer)

	// Create the hub for websocket connections
	hub := platform.NewHub(logger)

	// Create and register the topology manager runner.
	// This is leader-election-aware: polling and failover only run on the leader.
	runner := controller.NewTopologyManagerRunner(mgr.GetClient(), clientset, hub, logger)

	// Create and register the reconciler
	tainter := platform.NewNodeTainter(clientset, logger)
	reconciler := &controller.MysqlReplicaPairReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("bloodraven"),
		Runner:   runner,
		Tainter:  tainter,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		logger.Error("unable to create controller", "error", err)
		os.Exit(1)
	}
	if err := mgr.Add(runner); err != nil {
		logger.Error("unable to add topology manager runner", "error", err)
		os.Exit(1)
	}

	// Add health/ready probes
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error("unable to set up health check", "error", err)
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Error("unable to set up readyz check", "error", err)
		os.Exit(1)
	}

	// Start a separate HTTP server for websocket and status on :8082
	// (controller-runtime handles metrics on :8080 and probes on :8081)
	mux := newAuxMux(runner, hub)

	auxSrv := &http.Server{
		Addr:    ":8082",
		Handler: mux,
	}

	// Add the auxiliary HTTP server as a runnable
	if err := mgr.Add(runnableFunc(func(ctx context.Context) error {
		errCh := make(chan error, 1)
		go func() {
			logger.Info("starting auxiliary HTTP server", "addr", ":8082")
			if err := auxSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
			close(errCh)
		}()

		select {
		case err := <-errCh:
			return fmt.Errorf("auxiliary HTTP server failed: %w", err)
		case <-ctx.Done():
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return auxSrv.Shutdown(shutdownCtx)
	})); err != nil {
		logger.Error("unable to add auxiliary server", "error", err)
		os.Exit(1)
	}

	logger.Info("starting bloodraven manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error("manager exited with error", "error", err)
		os.Exit(1)
	}
}

// runnableFunc adapts a function to the manager.Runnable interface.
type runnableFunc func(ctx context.Context) error

func (f runnableFunc) Start(ctx context.Context) error {
	return f(ctx)
}

func newAuxMux(runner *controller.TopologyManagerRunner, hub *platform.Hub) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})
	mux.HandleFunc("/status", func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		statuses := runner.AllStatuses()
		if len(statuses) == 0 {
			json.NewEncoder(rw).Encode(map[string]string{"status": "no active pairs"})
			return
		}
		json.NewEncoder(rw).Encode(statuses)
	})
	mux.HandleFunc("/ws/status", hub.HandleWS)
	return mux
}
