// Package metrics scrapes the operator's Prometheus /metrics endpoint
// via a port-forward, parses it with expfmt, and offers typed lookup
// helpers for the assertions chaos scenarios make.
package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// OperatorPodSelector is the deployment label used by the Helm chart.
const OperatorPodSelector = "app.kubernetes.io/name=bloodraven"

// OperatorDeployment is the name of the operator's Deployment in the
// playground.
const OperatorDeployment = "bloodraven"

// ErrOperatorScaledDown is returned (wrapped) when there is no operator pod
// to scrape and its Deployment asks for none: no pod is coming, so the
// scraper does not wait for one.
var ErrOperatorScaledDown = errors.New("operator Deployment is scaled to 0")

// Scraper port-forwards the operator pod's :8080 and reuses the tunnel for
// every Scrape. A scrape whose tunnel fails (the operator pod was replaced,
// or the tunnel went to a pod that was terminating) opens a new one to the
// newest live operator pod and retries. Callers Close() it at the end of a
// scenario.
type Scraper struct {
	k         *pgkube.Client
	namespace string

	mu  sync.Mutex
	pf  *pgkube.PortForward
	cli *http.Client
	url string
}

// scrapeRedialBudget bounds how long Scrape re-opens the tunnel after a
// transport error: an operator pod that was just replaced needs a few
// seconds to serve /metrics. It is not spent on an operator that is scaled
// to 0 (ErrOperatorScaledDown).
const scrapeRedialBudget = 30 * time.Second

// NewScraper opens a port-forward to the operator pod (port 8080,
// where controller-runtime serves /metrics) and returns a Scraper.
func NewScraper(ctx context.Context, k *pgkube.Client, namespace string) (*Scraper, error) {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	s := &Scraper{k: k, namespace: namespace, cli: &http.Client{Timeout: 4 * time.Second}}
	if err := s.dial(ctx, 20*time.Second); err != nil {
		return nil, err
	}
	return s, nil
}

// dial replaces the tunnel with one to the newest live operator pod.
func (s *Scraper) dial(ctx context.Context, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	var last error
	for {
		pod, err := s.k.NewestLivePod(ctx, s.namespace, OperatorPodSelector)
		if err != nil {
			last = fmt.Errorf("find operator pod: %w", err)
			if s.operatorScaledDown(ctx) {
				return fmt.Errorf("%w (%v)", ErrOperatorScaledDown, last)
			}
		} else {
			pf, err := s.k.PortForwardPod(ctx, s.namespace, pod, 8080)
			if err == nil {
				s.mu.Lock()
				old := s.pf
				s.pf = pf
				s.url = fmt.Sprintf("http://127.0.0.1:%d/metrics", pf.LocalPort)
				s.mu.Unlock()
				if old != nil {
					old.Stop()
				}
				return nil
			}
			last = fmt.Errorf("port-forward operator metrics: %w", err)
			if !retryablePortForwardError(err) {
				return last
			}
		}
		if time.Now().After(deadline) {
			return last
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("operator metrics scraper: %w (last: %v)", ctx.Err(), last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// operatorScaledDown reports whether the operator's Deployment was read and
// asks for no replicas. A Deployment that cannot be read (or has another
// name, outside the playground) answers false: the caller keeps waiting
// for a pod, as it did before.
func (s *Scraper) operatorScaledDown(ctx context.Context) bool {
	dep, err := s.k.GetDeployment(ctx, s.namespace, OperatorDeployment)
	if err != nil {
		return false
	}
	return dep.Spec.Replicas != nil && *dep.Spec.Replicas == 0
}

func retryablePortForwardError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "pod not found") ||
		strings.Contains(msg, "unable to upgrade connection") ||
		strings.Contains(msg, "failed before ready")
}

// Close releases the SPDY tunnel.
func (s *Scraper) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	deadline := time.Now().Add(scrapeRedialBudget)
	for {
		body, transport, err := s.get(ctx)
		if err == nil {
			return ParseSnapshot(body)
		}
		if !transport || time.Now().After(deadline) || ctx.Err() != nil {
			return nil, err
		}
		// An operator pod that is Running but not serving /metrics yet
		// fails at once; do not open a tunnel per attempt in a tight loop.
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(500 * time.Millisecond):
		}
		if derr := s.dial(ctx, time.Until(deadline)); derr != nil {
			return nil, fmt.Errorf("%w (re-opening the tunnel: %w)", err, derr)
		}
	}
}

// get reads /metrics once; transport reports a failure of the tunnel or
// the connection rather than an answer.
func (s *Scraper) get(ctx context.Context) (body []byte, transport bool, err error) {
	s.mu.Lock()
	url := s.url
	s.mu.Unlock()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, err
	}
	resp, err := s.cli.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("http GET /metrics: %w", err)
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, true, fmt.Errorf("read /metrics: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("/metrics returned %d", resp.StatusCode)
	}
	return body, false, nil
}

// ParseSnapshot parses raw Prometheus text output into a Snapshot.
// Exposed so tests can feed golden fixtures.
func ParseSnapshot(body []byte) (*Snapshot, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	return &Snapshot{Raw: body, Families: families}, nil
}
