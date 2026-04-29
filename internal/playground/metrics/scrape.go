// Package metrics scrapes the operator's Prometheus /metrics endpoint
// via a port-forward, parses it with expfmt, and offers typed lookup
// helpers for the assertions chaos scenarios make.
package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// OperatorPodSelector is the deployment label used by the Helm chart.
const OperatorPodSelector = "app.kubernetes.io/name=bloodraven"

// Scraper port-forwards the operator pod's :8080 once and reuses the
// tunnel for every Scrape. Callers Close() it at the end of a scenario.
type Scraper struct {
	pf  *pgkube.PortForward
	cli *http.Client
	url string
}

// NewScraper opens a port-forward to the operator pod (port 8080,
// where controller-runtime serves /metrics) and returns a Scraper.
func NewScraper(ctx context.Context, k *pgkube.Client, namespace string) (*Scraper, error) {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	pod, err := k.FindPodWithLabel(ctx, namespace, OperatorPodSelector)
	if err != nil {
		return nil, fmt.Errorf("find operator pod: %w", err)
	}
	pf, err := k.PortForwardPod(ctx, namespace, pod.Name, 8080)
	if err != nil {
		return nil, fmt.Errorf("port-forward operator metrics: %w", err)
	}
	return &Scraper{
		pf:  pf,
		cli: &http.Client{Timeout: 4 * time.Second},
		url: fmt.Sprintf("http://127.0.0.1:%d/metrics", pf.LocalPort),
	}, nil
}

// Close releases the SPDY tunnel.
func (s *Scraper) Close() {
	if s.pf != nil {
		s.pf.Stop()
	}
}

// Snapshot is one scrape cycle's parsed metric families, keyed by
// metric name (e.g. "bloodraven_failovers_total").
type Snapshot struct {
	Raw      []byte
	Families map[string]*dto.MetricFamily
}

// Scrape pulls /metrics and parses it. The raw text payload is
// retained so forensic capture can dump it verbatim on failure.
func (s *Scraper) Scrape(ctx context.Context) (*Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.cli.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http GET /metrics: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read /metrics: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("/metrics returned %d", resp.StatusCode)
	}
	return ParseSnapshot(body)
}

// ParseSnapshot parses raw Prometheus text output into a Snapshot.
// Exposed so tests can feed golden fixtures.
func ParseSnapshot(body []byte) (*Snapshot, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	return &Snapshot{Raw: body, Families: families}, nil
}
