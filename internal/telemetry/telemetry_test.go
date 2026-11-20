package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/metrics"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestResolveSettings(t *testing.T) {
	bad := map[string]string{"BLOODRAVEN_TELEMETRY_ENDPOINT": "telemetry.example"}
	cases := []struct {
		name     string
		flag     bool
		flagErr  error
		vars     map[string]string
		enabled  bool
		invalid  bool
		endpoint string
	}{
		{name: "default on", flag: true, enabled: true, endpoint: DefaultEndpoint},
		{name: "flag off", flag: false},
		{name: "do not track 1", flag: true, vars: map[string]string{"DO_NOT_TRACK": "1"}},
		{name: "do not track true", flag: true, vars: map[string]string{"DO_NOT_TRACK": "true"}},
		{name: "do not track word", flag: true, vars: map[string]string{"DO_NOT_TRACK": "yes"}},
		{name: "do not track 0", flag: true, vars: map[string]string{"DO_NOT_TRACK": "0"}, enabled: true, endpoint: DefaultEndpoint},
		{name: "do not track false", flag: true, vars: map[string]string{"DO_NOT_TRACK": "false"}, enabled: true, endpoint: DefaultEndpoint},
		{name: "endpoint override", flag: true, vars: map[string]string{"BLOODRAVEN_TELEMETRY_ENDPOINT": "http://proxy.local/r"}, enabled: true, endpoint: "http://proxy.local/r"},
		// Unreadable settings turn the report off and never stop the operator.
		{name: "bad endpoint", flag: true, vars: bad, invalid: true},
		{name: "bad BLOODRAVEN_TELEMETRY", flag: true, flagErr: errors.New(`BLOODRAVEN_TELEMETRY="off" is not a boolean`), invalid: true},
		// An opt-out wins over a bad endpoint, which is then not a problem.
		{name: "bad endpoint, flag off", flag: false, vars: bad},
		{name: "bad endpoint, do not track", flag: true, vars: map[string]string{"BLOODRAVEN_TELEMETRY_ENDPOINT": "x", "DO_NOT_TRACK": "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := ResolveSettings(tc.flag, tc.flagErr, env(tc.vars))
			if s.Enabled != tc.enabled || s.Invalid != tc.invalid || s.Endpoint != tc.endpoint {
				t.Fatalf("got %+v", s)
			}
			if !s.Enabled && s.Reason == "" {
				t.Fatal("a disabled report must say why")
			}
		})
	}
}

func TestNewer(t *testing.T) {
	cases := []struct {
		latest, current string
		want            bool
	}{
		{"1.9.0", "1.8.0", true},
		{"v1.9.0", "1.8.0", true},
		{"1.8.0", "1.8.0", false},
		{"1.8.0", "1.9.0", false},
		{"1.10.0", "1.9.0", true},
		{"1.9.0", "1.9.0-rc.1", true},
		{"1.9.0", "dev", false},
		{"", "1.8.0", false},
		{"garbage", "1.8.0", false},
	}
	for _, tc := range cases {
		if got := Newer(tc.latest, tc.current); got != tc.want {
			t.Errorf("Newer(%q, %q) = %v, want %v", tc.latest, tc.current, got, tc.want)
		}
	}
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func group(ns, name string, sites int) *v1beta1.MysqlFailoverGroup {
	g := &v1beta1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	for i := 0; i < sites; i++ {
		g.Spec.Sites = append(g.Spec.Sites, v1beta1.SiteSpec{Name: string(rune('a' + i))})
	}
	return g
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func newReporter(t *testing.T, c client.Client, endpoint string) *Reporter {
	t.Helper()
	return &Reporter{Reader: c, Writer: c, Endpoint: endpoint, Version: "1.8.0", Namespace: "bloodraven-system"}
}

func TestBuildCreatesAndKeepsInstallID(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(group("a", "orders", 2), group("b", "billing", 3), node("n1"), node("n2"), node("n3")).Build()
	r := newReporter(t, c, DefaultEndpoint)

	first, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !validInstallID(first.InstallID) {
		t.Fatalf("install ID %q is not a UUID", first.InstallID)
	}
	want := Report{InstallID: first.InstallID, Version: "1.8.0", Groups: 2, Sites: 5, Nodes: 3}
	if first != want {
		t.Fatalf("got %+v, want %+v", first, want)
	}

	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "bloodraven-system", Name: ConfigMapName}, &cm); err != nil {
		t.Fatal(err)
	}
	if cm.Data[installIDKey] != first.InstallID {
		t.Fatalf("ConfigMap holds %q, report sent %q", cm.Data[installIDKey], first.InstallID)
	}
	if len(cm.OwnerReferences) != 0 {
		t.Fatal("the install ID must outlive the operator's Deployment")
	}

	second, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.InstallID != first.InstallID {
		t.Fatalf("install ID changed between reports: %q then %q", first.InstallID, second.InstallID)
	}
}

func TestBuildRepairsInvalidInstallID(t *testing.T) {
	bad := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "bloodraven-system", Name: ConfigMapName},
		Data:       map[string]string{installIDKey: "hand-edited"},
	}
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(bad).Build()
	r := newReporter(t, c, DefaultEndpoint)
	got, err := r.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !validInstallID(got.InstallID) {
		t.Fatalf("install ID %q is not a UUID", got.InstallID)
	}
	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "bloodraven-system", Name: ConfigMapName}, &cm); err != nil {
		t.Fatal(err)
	}
	if cm.Data[installIDKey] != got.InstallID {
		t.Fatal("the repaired ID was not stored")
	}
}

func TestBuildWithoutNamespaceSendsNothing(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer srv.Close()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	r := newReporter(t, c, srv.URL)
	r.Namespace = ""
	if err := r.ReportOnce(context.Background()); err == nil {
		t.Fatal("expected an error without a namespace to store the install ID in")
	}
	if hits != 0 {
		t.Fatal("a report without a stored install ID was sent")
	}
}

type captured struct {
	mu     sync.Mutex
	bodies []map[string]any
	agents []string
}

func (c *captured) server(t *testing.T, latest string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request %s %q", r.Method, r.Header.Get("Content-Type"))
		}
		data, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(data, &body); err != nil {
			t.Errorf("body is not JSON: %v", err)
		}
		c.mu.Lock()
		c.bodies = append(c.bodies, body)
		c.agents = append(c.agents, r.Header.Get("User-Agent"))
		c.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"latestVersion":"` + latest + `"}`))
	}))
}

func TestReportOnceSendsOnlyTheDocumentedFields(t *testing.T) {
	var got captured
	srv := got.server(t, "1.8.0", http.StatusOK)
	defer srv.Close()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(group("a", "orders", 2), node("n1")).Build()
	r := newReporter(t, c, srv.URL)
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(got.bodies) != 1 {
		t.Fatalf("sent %d reports", len(got.bodies))
	}
	var keys []string
	for k := range got.bodies[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// The usage-reporting docs page lists exactly these fields.
	if strings.Join(keys, ",") != "groups,installId,nodes,sites,version" {
		t.Fatalf("payload fields %v differ from the documented set", keys)
	}
	if got.bodies[0]["version"] != "1.8.0" || got.bodies[0]["groups"] != float64(1) || got.bodies[0]["sites"] != float64(2) || got.bodies[0]["nodes"] != float64(1) {
		t.Fatalf("payload %v", got.bodies[0])
	}
	if got.agents[0] != "bloodraven/1.8.0" {
		t.Fatalf("User-Agent %q", got.agents[0])
	}
}

func TestReportOnceObservesNewerRelease(t *testing.T) {
	metrics.UpdateAvailable.Reset()
	t.Cleanup(metrics.UpdateAvailable.Reset)
	var got captured
	srv := got.server(t, "1.9.0", http.StatusOK)
	defer srv.Close()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	var logs bytes.Buffer
	r := newReporter(t, c, srv.URL)
	r.Logger = slog.New(slog.NewJSONHandler(&logs, nil))

	for i := 0; i < 2; i++ {
		if err := r.ReportOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if v := testutil.ToFloat64(metrics.UpdateAvailable.WithLabelValues("1.9.0")); v != 1 {
		t.Fatalf("bloodraven_update_available{latest_version=1.9.0} = %v", v)
	}
	if n := strings.Count(logs.String(), msgUpdateAvailable); n != 1 {
		t.Fatalf("logged the newer release %d times, want once:\n%s", n, logs.String())
	}

	r.Version = "1.9.0"
	r.latest = ""
	if err := r.ReportOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(metrics.UpdateAvailable); n != 0 {
		t.Fatalf("a current operator still exports %d update series", n)
	}
}

func TestReportOnceFailsOnServerError(t *testing.T) {
	var got captured
	srv := got.server(t, "9.9.9", http.StatusServiceUnavailable)
	defer srv.Close()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	r := newReporter(t, c, srv.URL)
	if err := r.ReportOnce(context.Background()); err == nil {
		t.Fatal("expected an error for a 503")
	}
	if r.latest != "" {
		t.Fatal("a failed report's body must not be trusted")
	}
}

func TestStartReportsUntilCancelled(t *testing.T) {
	var got captured
	srv := got.server(t, "1.8.0", http.StatusOK)
	defer srv.Close()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	r := newReporter(t, c, srv.URL)
	r.FirstDelay = func() time.Duration { return time.Millisecond }
	// Start floors the wait at a minute; a negative jitter shows it does.
	r.Interval = time.Millisecond
	r.Jitter = func() time.Duration { return -time.Hour }
	if !r.NeedLeaderElection() {
		t.Fatal("the reporter must run on the leader only")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got.mu.Lock()
		n := len(got.bodies)
		got.mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no report sent")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
	got.mu.Lock()
	defer got.mu.Unlock()
	if len(got.bodies) != 1 {
		t.Fatalf("sent %d reports within the minute floor", len(got.bodies))
	}
}

func TestOperatorNamespacePrefersPodNamespace(t *testing.T) {
	if got := OperatorNamespace(env(map[string]string{"POD_NAMESPACE": " ops "})); got != "ops" {
		t.Fatalf("got %q", got)
	}
}

func TestReportRefusesRedirects(t *testing.T) {
	var redirected int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected++ }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	r := newReporter(t, c, srv.URL)
	if err := r.ReportOnce(context.Background()); err == nil {
		t.Fatal("a redirect must fail the report")
	}
	if redirected != 0 {
		t.Fatal("the report was replayed to the redirect target")
	}
}

func TestFailureLoggedAtInfoOnce(t *testing.T) {
	var logs bytes.Buffer
	r := &Reporter{Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	r.logFailure(errors.New("dial tcp: no route"))
	r.logFailure(errors.New("dial tcp: no route"))
	if n := strings.Count(logs.String(), msgFailed); n != 1 {
		t.Fatalf("logged %d failures at INFO, want 1:\n%s", n, logs.String())
	}
}

func TestStartClearsUpdateMetricWhenItStops(t *testing.T) {
	metrics.UpdateAvailable.Reset()
	metrics.UpdateAvailable.WithLabelValues("9.9.9").Set(1)
	r := &Reporter{FirstDelay: func() time.Duration { return time.Hour }}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(metrics.UpdateAvailable); n != 0 {
		t.Fatalf("%d update series left after the reporter stopped", n)
	}
}
