// Package telemetry sends the operator's anonymous usage report: once a day,
// a random install ID, the operator version and three counts (failover
// groups, their sites, and cluster nodes). The response names the latest
// release, which the operator logs and exports when it is newer than itself.
//
// The report is on by default and is turned off with --telemetry=false
// (BLOODRAVEN_TELEMETRY=false, the chart's telemetry.enabled) or
// DO_NOT_TRACK=1. Nothing else is sent, and a report that fails changes
// nothing: the reporter has no effect on reconciling or failover. The
// payload is documented in site/content/docs/4.configuration/13.usage-reporting.md;
// keep the two in step.
package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/mod/semver"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/metrics"
)

const (
	// DefaultEndpoint receives the reports.
	DefaultEndpoint = "https://telemetry.bloodraven.dev/v1/report"

	// ConfigMapName holds the install ID in the operator's namespace. The
	// operator creates it and nothing owns it, so it outlives a Helm
	// uninstall and a reinstall keeps counting as the same install.
	ConfigMapName = "bloodraven-telemetry"
	installIDKey  = "installID"

	// DefaultInterval is the time between reports.
	DefaultInterval = 24 * time.Hour

	requestTimeout   = 10 * time.Second
	attemptTimeout   = time.Minute
	maxResponseBytes = 4 << 10
	listPageSize     = 500

	msgEnabled         = "anonymous usage reporting enabled"
	msgDisabled        = "anonymous usage reporting disabled"
	msgUpdateAvailable = "newer bloodraven release available"
	msgFailed          = "usage report failed"
)

// Report is the whole payload. Adding a field is a change to the published
// privacy contract: update the usage-reporting page in the same change.
type Report struct {
	InstallID string `json:"installId"`
	Version   string `json:"version"`
	Groups    int    `json:"groups"`
	Sites     int    `json:"sites"`
	Nodes     int    `json:"nodes"`
}

type response struct {
	LatestVersion string `json:"latestVersion"`
}

// Settings is the resolved configuration.
type Settings struct {
	Enabled bool
	// Reason says why the report is off; empty when Enabled.
	Reason string
	// Invalid is set when the report is off because a setting could not be
	// read.
	Invalid  bool
	Endpoint string
}

// ResolveSettings reads the opt-out. telemetryFlag is the --telemetry value
// (true unless the operator was told otherwise) and flagErr the error from
// reading its BLOODRAVEN_TELEMETRY default. DO_NOT_TRACK set to anything but
// empty, "0" or "false" also turns the report off, as the convention at
// consoledonottrack.com asks. BLOODRAVEN_TELEMETRY_ENDPOINT replaces the
// endpoint, for a proxy or a test server.
//
// A setting that cannot be read turns the report off rather than stopping
// the operator: the report must never keep failover from running, and when
// in doubt it is not sent. An unreadable endpoint is never replaced by the
// default, since it was meant to route the report elsewhere.
func ResolveSettings(telemetryFlag bool, flagErr error, getenv func(string) string) Settings {
	if flagErr != nil {
		return Settings{Reason: flagErr.Error(), Invalid: true}
	}
	if !telemetryFlag {
		return Settings{Reason: "--telemetry=false"}
	}
	if dnt := strings.TrimSpace(getenv("DO_NOT_TRACK")); dnt != "" {
		if off, err := strconv.ParseBool(dnt); err != nil || off {
			return Settings{Reason: "DO_NOT_TRACK is set"}
		}
	}
	endpoint := strings.TrimSpace(getenv("BLOODRAVEN_TELEMETRY_ENDPOINT"))
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return Settings{Reason: fmt.Sprintf("BLOODRAVEN_TELEMETRY_ENDPOINT %q is not an http(s) URL", endpoint), Invalid: true}
	}
	return Settings{Enabled: true, Endpoint: endpoint}
}

// LogSettings writes the one startup line that tells an administrator
// whether the report is on and how to turn it off.
func LogSettings(logger *slog.Logger, s Settings) {
	if s.Invalid {
		logger.Warn(msgDisabled, "reason", s.Reason)
		return
	}
	if !s.Enabled {
		logger.Info(msgDisabled, "reason", s.Reason)
		return
	}
	endpoint := s.Endpoint
	if u, err := url.Parse(endpoint); err == nil {
		endpoint = u.Redacted()
	}
	logger.Info(msgEnabled,
		"endpoint", endpoint,
		"fields", "installId,version,groups,sites,nodes",
		"disable", "helm value telemetry.enabled=false, --telemetry=false or DO_NOT_TRACK=1",
		"docs", "https://bloodraven.dev/docs/configuration/usage-reporting")
}

// Reporter sends the report while its manager holds the leader lease, so a
// highly available operator reports once.
type Reporter struct {
	// Reader reads uncached (the install ID ConfigMap and node metadata):
	// one daily read must not start an informer.
	Reader client.Reader
	// Groups lists the failover groups, from the manager's cache, which
	// already watches them. Nil uses Reader.
	Groups client.Reader
	// Writer creates and repairs the install ID ConfigMap.
	Writer client.Writer
	// HTTP sends the report. Nil uses a client with a 10s timeout that
	// honours HTTPS_PROXY.
	HTTP      *http.Client
	Logger    *slog.Logger
	Endpoint  string
	Version   string
	Namespace string

	// Interval between reports; zero is DefaultInterval.
	Interval time.Duration
	// FirstDelay returns the wait before the first report. Nil waits a
	// random 2 to 10 minutes, so a crash-looping pod does not report.
	FirstDelay func() time.Duration
	// Jitter returns the offset added to each later wait. Nil is a random
	// value within an hour either way, so installs do not report in step.
	Jitter func() time.Duration

	mu       sync.Mutex
	latest   string
	failed   bool
	client   *http.Client
	initOnce sync.Once
}

// NeedLeaderElection makes the manager run the reporter on the leader only.
func (r *Reporter) NeedLeaderElection() bool { return true }

// Start reports until ctx ends. It never returns an error: a failed report
// is logged at debug level and retried at the next interval.
func (r *Reporter) Start(ctx context.Context) error {
	// A replica that stops reporting (it lost the lease) stops exporting
	// what its last report said.
	defer metrics.UpdateAvailable.Reset()
	wait := r.firstDelay()
	for {
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
		if err := r.reportWithDeadline(ctx); err != nil && ctx.Err() == nil {
			r.logFailure(err)
		}
		wait = r.interval() + r.jitter()
		if wait < time.Minute {
			wait = time.Minute
		}
	}
}

func (r *Reporter) reportWithDeadline(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	return r.ReportOnce(ctx)
}

// logFailure says once per process, at INFO, that a report failed, so an
// administrator who blocked egress can see the report is not going out.
// Later failures are DEBUG.
func (r *Reporter) logFailure(err error) {
	r.mu.Lock()
	first := !r.failed
	r.failed = true
	r.mu.Unlock()
	if first {
		r.logger().Info(msgFailed, "error", err.Error(), "next", "retried daily; later failures are logged at debug level")
		return
	}
	r.logger().Debug(msgFailed, "error", err.Error())
}

// ReportOnce builds and sends one report and acts on the response.
func (r *Reporter) ReportOnce(ctx context.Context) error {
	report, err := r.Build(ctx)
	if err != nil {
		return err
	}
	latest, err := r.send(ctx, report)
	if err != nil {
		return err
	}
	r.observeLatest(latest)
	r.logger().Debug("usage report sent", "version", report.Version, "groups", report.Groups, "sites", report.Sites, "nodes", report.Nodes)
	return nil
}

// Build reads the install ID and the counts. It fails rather than send a
// report under an ID it could not store, which would count one install many
// times.
func (r *Reporter) Build(ctx context.Context) (Report, error) {
	id, err := r.installID(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("install ID: %w", err)
	}
	groups, sites, err := r.countGroups(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("count failover groups: %w", err)
	}
	nodes, err := r.countNodes(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("count nodes: %w", err)
	}
	return Report{InstallID: id, Version: r.Version, Groups: groups, Sites: sites, Nodes: nodes}, nil
}

func (r *Reporter) installID(ctx context.Context) (string, error) {
	if r.Namespace == "" {
		return "", errors.New("operator namespace unknown")
	}
	key := types.NamespacedName{Namespace: r.Namespace, Name: ConfigMapName}
	for attempt := 0; attempt < 3; attempt++ {
		var cm corev1.ConfigMap
		err := r.Reader.Get(ctx, key, &cm)
		switch {
		case apierrors.IsNotFound(err):
			id := uuid.NewString()
			created := corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ConfigMapName,
					Namespace: r.Namespace,
					Labels:    map[string]string{"app.kubernetes.io/managed-by": "bloodraven"},
				},
				Data: map[string]string{installIDKey: id},
			}
			if err := r.Writer.Create(ctx, &created); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				return "", err
			}
			return id, nil
		case err != nil:
			return "", err
		}
		if id := cm.Data[installIDKey]; validInstallID(id) {
			return id, nil
		}
		id := uuid.NewString()
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[installIDKey] = id
		if err := r.Writer.Update(ctx, &cm); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return "", err
		}
		return id, nil
	}
	return "", errors.New("install ID ConfigMap kept changing")
}

func validInstallID(id string) bool {
	parsed, err := uuid.Parse(id)
	return err == nil && parsed.String() == id
}

func (r *Reporter) countGroups(ctx context.Context) (groups, sites int, err error) {
	if r.Groups != nil {
		// The cache is in memory and returns no continue token, so a
		// paged read of it would stop at the first page.
		var list v1beta1.MysqlFailoverGroupList
		if err := r.Groups.List(ctx, &list); err != nil {
			return 0, 0, err
		}
		for i := range list.Items {
			groups++
			sites += len(list.Items[i].Spec.Sites)
		}
		return groups, sites, nil
	}
	opts := []client.ListOption{client.Limit(listPageSize)}
	for {
		var list v1beta1.MysqlFailoverGroupList
		if err := r.Reader.List(ctx, &list, opts...); err != nil {
			return 0, 0, err
		}
		for i := range list.Items {
			groups++
			sites += len(list.Items[i].Spec.Sites)
		}
		if list.Continue == "" {
			return groups, sites, nil
		}
		opts = []client.ListOption{client.Limit(listPageSize), client.Continue(list.Continue)}
	}
}

func (r *Reporter) countNodes(ctx context.Context) (int, error) {
	n := 0
	opts := []client.ListOption{client.Limit(listPageSize)}
	for {
		list := metav1.PartialObjectMetadataList{}
		list.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("NodeList"))
		if err := r.Reader.List(ctx, &list, opts...); err != nil {
			return 0, err
		}
		n += len(list.Items)
		if list.Continue == "" {
			return n, nil
		}
		opts = []client.ListOption{client.Limit(listPageSize), client.Continue(list.Continue)}
	}
}

func (r *Reporter) send(ctx context.Context, report Report) (string, error) {
	body, err := json.Marshal(report)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "bloodraven/"+r.Version)
	resp, err := r.httpClient().Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("endpoint answered %s", resp.Status)
	}
	var out response
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			return "", fmt.Errorf("decode response: %w", err)
		}
	}
	return strings.TrimSpace(out.LatestVersion), nil
}

// observeLatest logs a newer release once per version and sets
// bloodraven_update_available.
func (r *Reporter) observeLatest(latest string) {
	newer := Newer(latest, r.Version)
	r.mu.Lock()
	changed := latest != r.latest
	r.latest = latest
	r.mu.Unlock()
	if !changed {
		return
	}
	metrics.UpdateAvailable.Reset()
	if !newer {
		return
	}
	metrics.UpdateAvailable.WithLabelValues(canonical(latest)).Set(1)
	r.logger().Info(msgUpdateAvailable, "version", r.Version, "latestVersion", canonical(latest),
		"releaseNotes", "https://github.com/shipstream/bloodraven/releases")
}

// Newer reports whether latest is a later semantic version than current.
// A development build ("dev") or an unparsable version is never behind.
func Newer(latest, current string) bool {
	l, c := "v"+canonical(latest), "v"+canonical(current)
	if !semver.IsValid(l) || !semver.IsValid(c) {
		return false
	}
	return semver.Compare(l, c) > 0
}

func canonical(v string) string { return strings.TrimPrefix(strings.TrimSpace(v), "v") }

func (r *Reporter) httpClient() *http.Client {
	if r.HTTP != nil {
		return r.HTTP
	}
	r.initOnce.Do(func() {
		r.client = &http.Client{
			Timeout: requestTimeout,
			// One request a day: no connection is kept open between them.
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
				TLSHandshakeTimeout: 5 * time.Second,
				DisableKeepAlives:   true,
			},
			// The report goes to the configured endpoint or nowhere: a
			// redirect would replay it to another origin.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	})
	return r.client
}

func (r *Reporter) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.New(slog.DiscardHandler)
}

func (r *Reporter) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return DefaultInterval
}

func (r *Reporter) firstDelay() time.Duration {
	if r.FirstDelay != nil {
		return r.FirstDelay()
	}
	return 2*time.Minute + rand.N(8*time.Minute)
}

func (r *Reporter) jitter() time.Duration {
	if r.Jitter != nil {
		return r.Jitter()
	}
	return rand.N(2*time.Hour) - time.Hour
}

// OperatorNamespace is the namespace the operator runs in: POD_NAMESPACE,
// else the service account's namespace file. Empty when neither is known.
func OperatorNamespace(getenv func(string) string) string {
	if ns := strings.TrimSpace(getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
