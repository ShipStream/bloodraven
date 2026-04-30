package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"sigs.k8s.io/yaml"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
)

// Capture is the per-scenario forensic directory. Failure-only by
// default: the executor calls Persist() once a step returns an error,
// at which point all the snapshot helpers below dump their state to
// disk. On success, the directory is empty and removed.
type Capture struct {
	Dir string

	mu       sync.Mutex
	notes    []string
	persisted bool
}

// Note appends a structured line to the runner's scenario.log
// in-memory buffer. Safe to call from any goroutine.
func (c *Capture) Note(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notes = append(c.notes, line)
}

// EnsureDir creates the capture directory lazily. Idempotent.
func (c *Capture) EnsureDir() error {
	if c.Dir == "" {
		return fmt.Errorf("capture: empty Dir")
	}
	return os.MkdirAll(c.Dir, 0o755)
}

// WriteFile writes a relative file under the capture dir.
func (c *Capture) WriteFile(name string, body []byte) error {
	if err := c.EnsureDir(); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(c.Dir, name), body, 0o644)
}

// Persist gathers cluster state into the capture dir. Best-effort:
// errors fetching individual artifacts are recorded in scenario.log
// rather than aborting capture.
func (c *Capture) Persist(ctx context.Context, k *pgkube.Client, namespace string, scraper *pgmetrics.Scraper, tailers map[string]*pglogs.Tailer, failure string) error {
	c.mu.Lock()
	if c.persisted {
		c.mu.Unlock()
		return nil
	}
	c.persisted = true
	c.mu.Unlock()

	if err := c.EnsureDir(); err != nil {
		return err
	}

	if mfg, err := k.GetMFG(ctx, namespace); err == nil {
		if data, err := yaml.Marshal(mfg); err == nil {
			_ = c.WriteFile("cluster.yaml", data)
		} else {
			c.Note("failed to marshal MFG: " + err.Error())
		}
	} else {
		c.Note("failed to fetch MFG: " + err.Error())
	}

	if pods, err := k.Kubernetes.CoreV1().Pods(namespace).List(ctx, listOpts()); err == nil {
		if data, err := yaml.Marshal(pods); err == nil {
			_ = c.WriteFile("pods.yaml", data)
		}
	} else {
		c.Note("failed to list pods: " + err.Error())
	}

	if events, err := k.RecentEvents(ctx, namespace, 200); err == nil {
		if data, err := yaml.Marshal(events); err == nil {
			_ = c.WriteFile("events.yaml", data)
		}
	} else {
		c.Note("failed to list events: " + err.Error())
	}

	if scraper != nil {
		if snap, err := scraper.Scrape(ctx); err == nil {
			_ = c.WriteFile("metrics.txt", snap.Raw)
		} else {
			c.Note("failed to scrape metrics: " + err.Error())
		}
	}

	for label, tail := range tailers {
		ms := tail.Snapshot()
		buf := make([]byte, 0, 64*1024)
		for _, m := range ms {
			buf = append(buf, []byte(m.Time.Format("2006-01-02T15:04:05Z07:00"))...)
			buf = append(buf, ' ')
			buf = append(buf, []byte(m.Line)...)
			buf = append(buf, '\n')
		}
		_ = c.WriteFile(label+".log", buf)
	}

	c.mu.Lock()
	notes := append([]string{}, c.notes...)
	c.mu.Unlock()
	scenarioLog := make([]byte, 0, 4096)
	for _, n := range notes {
		scenarioLog = append(scenarioLog, []byte(n)...)
		scenarioLog = append(scenarioLog, '\n')
	}
	_ = c.WriteFile("scenario.log", scenarioLog)

	if failure != "" {
		_ = c.WriteFile("failure.txt", []byte(failure+"\n"))
	}
	return nil
}

func listOpts() metav1ListOpts { return metav1ListOpts{} }
