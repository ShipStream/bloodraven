package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/license"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/util"
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
		case "encrypt-upload":
			// Invoked as the main container of an encrypted backup
			// Job: reads the staged dump from the mysqlsh init
			// container, encrypts every file with AES-256-GCM, and
			// uploads the ciphertext to S3 or a PVC. See
			// encrypt_upload.go.
			runEncryptUpload(os.Args[2:])
			return
		case "decrypt-download":
			// Invoked as an init container of a restore / verification
			// Job whose source backup was encrypted: downloads the
			// ciphertext prefix and decrypts it into a shared emptyDir
			// ahead of mysqlsh's loadDump. See encrypt_upload.go.
			runDecryptDownload(os.Args[2:])
			return
		}
	}

	// Flag parsing after the subcommand dispatcher so subcommand argv
	// isn't interpreted here. The Helm chart writes --leader-elect on
	// the operator deployment based on the values.leaderElection.enabled
	// knob; wiring the flag through is what makes that knob honest
	// (AUDIT H4).
	fs := flag.NewFlagSet("bloodraven", flag.ExitOnError)
	leaderElect := fs.Bool("leader-elect", true, "enable manager leader election")
	licenseToken := fs.String("license", os.Getenv("BLOODRAVEN_LICENSE"), "optional organization license JWT (public assertion, not a secret)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "parse flags: %v\n", err)
		os.Exit(2)
	}

	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo)
	controller.LogOperatorLicense(logger, *licenseToken, license.ProductionKey, time.Now())
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
		LeaderElection:   *leaderElect,
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

	// Register operator metrics into controller-runtime's registry so
	// they appear on the same `/metrics` endpoint controller-runtime
	// already serves at metrics.bindAddress (default ":8080"). The
	// ctrl-runtime registry is a private prometheus.Registry distinct
	// from prometheus.DefaultRegisterer; if we registered there, our
	// custom metrics would never reach the dashboards documented at
	// charts/bloodraven/dashboards/README.md.
	metrics.Register(ctrlmetrics.Registry)

	// Create the hub for websocket connections
	hub := platform.NewHub(logger)

	// Create and register the topology manager runner.
	// This is leader-election-aware: polling and failover only run on the leader.
	recorder := mgr.GetEventRecorderFor("bloodraven")
	runner := controller.NewTopologyManagerRunner(mgr.GetClient(), clientset, hub, recorder, logger)

	// Create and register the reconciler
	tainter := platform.NewNodeTainter(clientset, logger)
	reconciler := &controller.MysqlFailoverGroupReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		Recorder:        recorder,
		Runner:          runner,
		Tainter:         tainter,
		Clientset:       clientset,
		Logger:          logger,
		OperatorLicense: *licenseToken,
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

	// Register the MysqlStandbyCluster reconciler. Phase 1 ships the
	// passive bucket-discovery loop (BucketReadable + SourceConfigKnown
	// conditions). Activation (Phase 3) and continuous verification
	// (Phase 2) are follow-up PRs. See WISHLIST/#7.
	standbyReconciler := &controller.MysqlStandbyClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("bloodraven-standby"),
	}
	if err := standbyReconciler.SetupWithManager(mgr); err != nil {
		logger.Error("unable to create standby controller", "error", err)
		os.Exit(1)
	}

	// Register the MysqlDatabase reconciler. This is the tenant-database
	// API: a caller with namespaced RBAC on mysqldatabases provisions a
	// database, its owning user and grant-only entries without holding a
	// MySQL credential or Secret access. It shares the admin connection
	// path with the credential reconciler by design — see
	// openAdminConnection in internal/controller/credentials.go.
	databaseReconciler := &controller.MysqlDatabaseReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("bloodraven-database"),
	}
	if err := databaseReconciler.SetupWithManager(mgr); err != nil {
		logger.Error("unable to create database controller", "error", err)
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
	mux := newAuxMuxWithLogger(runner, hub, mgr.GetClient(), logger)

	auxSrv := &http.Server{
		Addr:              ":8082",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16, // 64 KiB — matches net/http's default-ish cap and still fits auth headers
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

	escrowCertFile := strings.TrimSpace(os.Getenv("BLOODRAVEN_ESCROW_TLS_CERT_FILE"))
	escrowKeyFile := strings.TrimSpace(os.Getenv("BLOODRAVEN_ESCROW_TLS_KEY_FILE"))
	if escrowCertFile != "" || escrowKeyFile != "" {
		if escrowCertFile == "" || escrowKeyFile == "" {
			logger.Error("escrow TLS requires both certificate and key files")
			os.Exit(1)
		}
		_, err := tls.LoadX509KeyPair(escrowCertFile, escrowKeyFile)
		if err != nil {
			logger.Error("unable to load escrow TLS certificate", "error", err)
			os.Exit(1)
		}
		escrowAddr := strings.TrimSpace(os.Getenv("BLOODRAVEN_ESCROW_TLS_ADDR"))
		if escrowAddr == "" {
			escrowAddr = ":8443"
		}
		escrowSrv := &http.Server{
			Addr:              escrowAddr,
			Handler:           newEscrowMux(mgr.GetClient(), logger),
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    1 << 16,
			TLSConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
					certificate, err := tls.LoadX509KeyPair(escrowCertFile, escrowKeyFile)
					if err != nil {
						return nil, fmt.Errorf("reload escrow TLS certificate: %w", err)
					}
					return &certificate, nil
				},
			},
		}
		if err := mgr.Add(runnableFunc(func(ctx context.Context) error {
			errCh := make(chan error, 1)
			go func() {
				logger.Info("starting keyring escrow TLS server", "addr", escrowAddr)
				if err := escrowSrv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
					errCh <- err
				}
				close(errCh)
			}()
			select {
			case err := <-errCh:
				return fmt.Errorf("keyring escrow TLS server failed: %w", err)
			case <-ctx.Done():
			}
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return escrowSrv.Shutdown(shutdownCtx)
		})); err != nil {
			logger.Error("unable to add keyring escrow TLS server", "error", err)
			os.Exit(1)
		}
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

// auxLoggingMiddleware emits one structured log line per request
// against the aux server (method, path, status, duration, remote IP)
// and increments Prometheus RED counters. A stuck handler or probing
// attacker otherwise produces zero signal (AUDIT M4/M6).
func auxLoggingMiddleware(logger *slog.Logger, handler string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: rw, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		dur := time.Since(start)
		method := auxMetricMethod(r.Method)
		logger.Info("aux http",
			"method", r.Method,
			"path", r.URL.Path,
			"handler", handler,
			"status", sw.status,
			"duration_ms", dur.Milliseconds(),
			"remote", r.RemoteAddr,
		)
		metrics.HTTPRequestsTotal.WithLabelValues("aux", handler, method, metrics.StatusClass(sw.status)).Inc()
		// Skip the request-latency histogram for hijacked connections
		// (e.g. the /ws/status WebSocket): their "duration" is the whole
		// session lifetime, not request-handling latency, and would
		// otherwise swamp the aux latency percentiles. The connection is
		// still counted in HTTPRequestsTotal above.
		if !sw.hijacked {
			metrics.HTTPRequestDurationSeconds.WithLabelValues("aux", handler, method).Observe(dur.Seconds())
		}
	})
}

func auxMetricMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

// statusRecorder captures the HTTP status so the logging middleware
// can record it. net/http doesn't offer this out of the box. It also
// notes when the connection was hijacked (WebSocket upgrade) so the
// middleware can skip the request-latency histogram for long-lived
// connections whose "duration" is really the session lifetime.
type statusRecorder struct {
	http.ResponseWriter
	status   int
	hijacked bool
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying response writer does not support hijacking")
	}
	conn, rw, err := h.Hijack()
	if err == nil {
		r.hijacked = true
	}
	return conn, rw, err
}

func (r *statusRecorder) ReadFrom(src io.Reader) (int64, error) {
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	return io.Copy(r.ResponseWriter, src)
}

// pitrCutoffCache is a tiny TTL cache in front of the /pitr-cutoff
// handler. Keyed by (namespace, group, profile). Concurrent-safe.
type pitrCutoffCache struct {
	ttl  time.Duration
	mu   sync.Mutex
	data map[string]pitrCutoffEntry
}

type pitrCutoffEntry struct {
	value map[string]any
	at    time.Time
}

func newPITRCutoffCache(ttl time.Duration) *pitrCutoffCache {
	return &pitrCutoffCache{ttl: ttl, data: make(map[string]pitrCutoffEntry)}
}

func (c *pitrCutoffCache) key(ns, group, profile string) string {
	return ns + "/" + group + "/" + profile
}

func (c *pitrCutoffCache) get(ns, group, profile string) (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.data[c.key(ns, group, profile)]
	if !ok || time.Since(e.at) > c.ttl {
		return nil, false
	}
	return e.value, true
}

func (c *pitrCutoffCache) set(ns, group, profile string, value map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[c.key(ns, group, profile)] = pitrCutoffEntry{value: value, at: time.Now()}
}

func newAuxMux(runner *controller.TopologyManagerRunner, hub *platform.Hub, k8sClient client.Client) *http.ServeMux {
	return newAuxMuxWithLogger(runner, hub, k8sClient, slog.Default())
}

func newAuxMuxWithLogger(runner *controller.TopologyManagerRunner, hub *platform.Hub, k8sClient client.Client, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/healthz", auxLoggingMiddleware(logger, "healthz", http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})))
	mux.Handle("/readyz", auxLoggingMiddleware(logger, "readyz", http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})))
	mux.Handle("/status", auxLoggingMiddleware(logger, "status", http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		statuses := runner.AllStatuses()
		if len(statuses) == 0 {
			json.NewEncoder(rw).Encode(map[string]string{"status": "no active failover groups"})
			return
		}
		json.NewEncoder(rw).Encode(statuses)
	})))
	mux.Handle("/active-site", auxLoggingMiddleware(logger, "active-site", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
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
	})))
	// /pitr-cutoff is consumed by the sidecar's binlog archiver.
	// Given a (namespace, group, profile) triple it returns the
	// completion time of the OLDEST successful MysqlBackup retained
	// for that profile. Anything older than this cutoff is safe to
	// purge from the PITR archive — its replay window is already
	// covered by the oldest-surviving full dump. The archiver runs
	// this lookup on its own cadence and handles the actual deletes
	// (it has storage credentials; the operator pod does not).
	// /pitr-cutoff is a hot path — the sidecar archiver polls it from
	// every primary. A per-(ns, group, profile) TTL cache keeps the
	// operator's client from List'ing the whole namespace on every
	// call, which also bounds the DoS surface on the unauthenticated
	// endpoint (AUDIT H2).
	pitrCache := newPITRCutoffCache(30 * time.Second)
	mux.Handle("/pitr-cutoff", auxLoggingMiddleware(logger, "pitr-cutoff", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
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
		if cached, ok := pitrCache.get(ns, group, profile); ok {
			_ = json.NewEncoder(rw).Encode(cached)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		var list v1alpha1.MysqlBackupList
		// Scope the List by our well-known labels so the API server
		// filters for us; falls back to full-namespace only when
		// labels are absent (handled in the filter loop below).
		if err := k8sClient.List(ctx, &list, client.InNamespace(ns), client.MatchingLabels{
			"shipstream.io/failover-group": group,
			"shipstream.io/backup-profile": profile,
		}); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			// Don't echo raw API errors to unauthenticated callers.
			json.NewEncoder(rw).Encode(map[string]string{"error": "internal error resolving PITR cutoff"})
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
		pitrCache.set(ns, group, profile, resp)
		json.NewEncoder(rw).Encode(resp)
	})))
	mux.Handle("/ws/status", auxLoggingMiddleware(logger, "ws-status", http.HandlerFunc(hub.HandleWS)))
	mux.Handle("/", auxLoggingMiddleware(logger, "notfound", http.NotFoundHandler()))
	return mux
}

func newEscrowMux(k8sClient client.Client, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/keyring/escrow", auxLoggingMiddleware(logger, "keyring-escrow",
		controller.NewKeyringEscrowHandler(k8sClient, logger)))
	mux.Handle("/", auxLoggingMiddleware(logger, "notfound", http.NotFoundHandler()))
	return mux
}
