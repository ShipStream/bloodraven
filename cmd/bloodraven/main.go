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
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/platform"
)

func main() {
	// Subcommand dispatcher. Each subcommand bypasses manager setup
	// and runs as a standalone process — the same binary is shipped
	// to CronJob pods, restore init containers, etc., so one image
	// covers every control-plane-adjacent workload.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "trigger-backup":
			// Invoked by scheduled CronJob pods to POST a MysqlBackup
			// CR. See trigger.go.
			runTriggerBackup(os.Args[2:])
			return
		case "pitr-download":
			// Invoked by restore Job init containers to download
			// archived binlog files into the shared emptyDir before
			// the mysqlsh container runs replay. See pitr_download.go.
			runPITRDownload(os.Args[2:])
			return
		case "trigger-verification":
			// Invoked by scheduled verification CronJob pods to POST a
			// MysqlBackupVerification CR. See trigger_verification.go.
			runTriggerVerification(os.Args[2:])
			return
		}
	}

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
	recorder := mgr.GetEventRecorderFor("bloodraven")
	runner := controller.NewTopologyManagerRunner(mgr.GetClient(), clientset, hub, recorder, logger)

	// Create and register the reconciler
	tainter := platform.NewNodeTainter(clientset, logger)
	reconciler := &controller.MysqlFailoverGroupReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
		Recorder:  recorder,
		Runner:    runner,
		Tainter:   tainter,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		logger.Error("unable to create controller", "error", err)
		os.Exit(1)
	}
	// Wire the back-reference so the topology runner can call
	// reconciler.ReconcileSiteDeployment for ordered rolling updates.
	runner.SetDeploymentReconciler(reconciler)

	// Register the MysqlBackup reconciler alongside the failover-group
	// reconciler. Both share the manager, so they share leader election.
	backupReconciler := &controller.MysqlBackupReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("bloodraven-backup"),
		Clientset: clientset,
	}
	if err := backupReconciler.SetupWithManager(mgr); err != nil {
		logger.Error("unable to create backup controller", "error", err)
		os.Exit(1)
	}

	// Register the MysqlBackupVerification reconciler alongside the
	// backup reconciler. Verification runs are short-lived Jobs owned
	// by the verification CR; the reconciler cleans them up on
	// success and retains them on failure for inspection.
	verificationReconciler := &controller.MysqlBackupVerificationReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("bloodraven-verification"),
		Clientset: clientset,
	}
	if err := verificationReconciler.SetupWithManager(mgr); err != nil {
		logger.Error("unable to create verification controller", "error", err)
		os.Exit(1)
	}

	// Tell the schedule reconciler which operator image and ServiceAccount
	// to embed into the CronJob pods it creates. These can be overridden
	// via env vars to support kind/k3d playground runs where the operator
	// image tag is something like "bloodraven:playground".
	operatorImage := os.Getenv("BLOODRAVEN_OPERATOR_IMAGE")
	if operatorImage == "" {
		operatorImage = "bloodraven:latest"
	}
	operatorSA := os.Getenv("BLOODRAVEN_OPERATOR_SA")
	controller.SetOperatorImageDefaults(operatorImage, operatorSA)

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
	mux := newAuxMux(runner, hub, mgr.GetClient())

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

func newAuxMux(runner *controller.TopologyManagerRunner, hub *platform.Hub, k8sClient client.Client) *http.ServeMux {
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
			json.NewEncoder(rw).Encode(map[string]string{"status": "no active failover groups"})
			return
		}
		json.NewEncoder(rw).Encode(statuses)
	})
	mux.HandleFunc("/active-site", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		ns := r.URL.Query().Get("namespace")
		group := r.URL.Query().Get("group")
		if ns == "" || group == "" {
			rw.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(rw).Encode(map[string]string{"error": "namespace and group query parameters are required"})
			return
		}
		nn := types.NamespacedName{Namespace: ns, Name: group}
		status, found := runner.Status(nn)
		if !found {
			if len(runner.AllStatuses()) == 0 {
				rw.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(rw).Encode(map[string]string{"error": "no active topology managers"})
				return
			}
			rw.WriteHeader(http.StatusNotFound)
			json.NewEncoder(rw).Encode(map[string]string{"error": "failover group not found"})
			return
		}
		json.NewEncoder(rw).Encode(map[string]string{
			"namespace":  ns,
			"group":      group,
			"activeSite": status.ActiveSite,
		})
	})
	// /pitr-cutoff is consumed by the sidecar's binlog archiver.
	// Given a (namespace, group, profile) triple it returns the
	// completion time of the OLDEST successful MysqlBackup retained
	// for that profile. Anything older than this cutoff is safe to
	// purge from the PITR archive — its replay window is already
	// covered by the oldest-surviving full dump. The archiver runs
	// this lookup on its own cadence and handles the actual deletes
	// (it has storage credentials; the operator pod does not).
	mux.HandleFunc("/pitr-cutoff", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		ns := r.URL.Query().Get("namespace")
		group := r.URL.Query().Get("group")
		profile := r.URL.Query().Get("profile")
		if ns == "" || group == "" || profile == "" {
			rw.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(rw).Encode(map[string]string{"error": "namespace, group, and profile query parameters are required"})
			return
		}
		// Guard against callers (notably tests) that build the mux
		// without a real client. Returning 503 keeps the contract
		// consistent with other "operator not ready" responses.
		if k8sClient == nil {
			rw.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(rw).Encode(map[string]string{"error": "operator k8s client not configured"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var list v1alpha1.MysqlBackupList
		if err := k8sClient.List(ctx, &list, client.InNamespace(ns)); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(rw).Encode(map[string]string{"error": err.Error()})
			return
		}

		// Minimum CompletionTime across successful, non-deleting
		// MysqlBackups that match (group, profile). Entries without a
		// CompletionTime are ignored (they can't establish a cutoff
		// anyway). If there are no retained backups we return empty
		// so the archiver treats "nothing to prune yet".
		var cutoff *time.Time
		for i := range list.Items {
			b := &list.Items[i]
			if !b.DeletionTimestamp.IsZero() {
				continue
			}
			if b.Spec.FailoverGroupRef.Name != group {
				continue
			}
			if b.Spec.ProfileName != profile {
				continue
			}
			if b.Status.Phase != v1alpha1.BackupPhaseSucceeded {
				continue
			}
			if b.Status.CompletionTime == nil {
				continue
			}
			t := b.Status.CompletionTime.Time
			if cutoff == nil || t.Before(*cutoff) {
				c := t
				cutoff = &c
			}
		}
		resp := map[string]any{"namespace": ns, "group": group, "profile": profile}
		if cutoff != nil {
			resp["cutoffTime"] = cutoff.UTC().Format(time.RFC3339)
		}
		json.NewEncoder(rw).Encode(resp)
	})
	mux.HandleFunc("/ws/status", hub.HandleWS)
	return mux
}
