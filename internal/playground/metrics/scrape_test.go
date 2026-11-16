package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

const sampleMetrics = `
# HELP bloodraven_failovers_total Total number of failovers executed.
# TYPE bloodraven_failovers_total counter
bloodraven_failovers_total{target_site="iad"} 0
bloodraven_failovers_total{target_site="pdx"} 1
# HELP bloodraven_site_state Current site state as a state-set.
# TYPE bloodraven_site_state gauge
bloodraven_site_state{site="iad",state="writable"} 0
bloodraven_site_state{site="iad",state="read-only"} 1
bloodraven_site_state{site="pdx",state="writable"} 1
bloodraven_site_state{site="pdx",state="read-only"} 0
# HELP bloodraven_planned_failover_duration_seconds Wall-clock duration.
# TYPE bloodraven_planned_failover_duration_seconds histogram
bloodraven_planned_failover_duration_seconds_bucket{target_site="pdx",le="1"} 0
bloodraven_planned_failover_duration_seconds_bucket{target_site="pdx",le="2"} 1
bloodraven_planned_failover_duration_seconds_bucket{target_site="pdx",le="+Inf"} 1
bloodraven_planned_failover_duration_seconds_sum{target_site="pdx"} 1.5
bloodraven_planned_failover_duration_seconds_count{target_site="pdx"} 1
`

func TestSnapshotLookups(t *testing.T) {
	snap, err := ParseSnapshot([]byte(sampleMetrics))
	if err != nil {
		t.Fatalf("ParseSnapshot: %v", err)
	}

	if v, ok := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": "pdx"}); !ok || v != 1 {
		t.Fatalf("counter pdx = %v ok=%v want 1 true", v, ok)
	}
	if v, ok := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": "missing"}); ok {
		t.Fatalf("expected no match for missing, got %v", v)
	}
	if v, ok := snap.Gauge("bloodraven_site_state", map[string]string{"site": "pdx", "state": "writable"}); !ok || v != 1 {
		t.Fatalf("gauge pdx writable = %v ok=%v want 1 true", v, ok)
	}
	if c, ok := snap.HistogramSampleCount("bloodraven_planned_failover_duration_seconds", map[string]string{"target_site": "pdx"}); !ok || c != 1 {
		t.Fatalf("histogram count = %d ok=%v want 1 true", c, ok)
	}
}

// closedLocalURL is a /metrics URL on a local port nothing listens on, as
// the scraper holds after its operator pod is gone.
func closedLocalURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return "http://" + addr + "/metrics"
}

func operatorDeployment(replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: OperatorDeployment},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
}

// With the operator scaled to 0 no pod is coming. Scrape used to spend its
// whole redial budget (30s) waiting for one on every call: a scenario that
// holds the operator down, and the forensic capture after it, paid that per
// scrape.
func TestScrape_OperatorScaledDownFailsAtOnce(t *testing.T) {
	s := &Scraper{
		k:         &pgkube.Client{Kubernetes: fake.NewSimpleClientset(operatorDeployment(0))},
		namespace: "ns",
		cli:       &http.Client{Timeout: time.Second},
		url:       closedLocalURL(t),
	}
	start := time.Now()
	_, err := s.Scrape(context.Background())
	if !errors.Is(err, ErrOperatorScaledDown) {
		t.Fatalf("Scrape error = %v, want ErrOperatorScaledDown", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Scrape took %s with the operator scaled to 0, want it to fail at once", took)
	}
}

// A Deployment that still asks for a pod is an operator being replaced:
// the scraper keeps waiting for the new pod within its budget.
func TestDial_WaitsForAPodTheDeploymentWants(t *testing.T) {
	s := &Scraper{
		k:         &pgkube.Client{Kubernetes: fake.NewSimpleClientset(operatorDeployment(1))},
		namespace: "ns",
	}
	start := time.Now()
	err := s.dial(context.Background(), 700*time.Millisecond)
	if err == nil || errors.Is(err, ErrOperatorScaledDown) {
		t.Fatalf("dial error = %v, want the pod lookup error after the budget", err)
	}
	if took := time.Since(start); took < 700*time.Millisecond {
		t.Fatalf("dial gave up after %s, before its budget ended", took)
	}
}
