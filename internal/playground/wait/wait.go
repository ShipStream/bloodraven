// Package wait provides deadlined polling primitives for chaos
// scenarios. Every primitive returns a structured TimeoutError on
// expiration so the runner's forensic capture can record the last
// observed state.
package wait

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

// TimeoutError is returned by any primitive that timed out before its
// predicate became satisfied. Embeds the last observed message so
// the runner can include it in failure.txt.
type TimeoutError struct {
	What        string
	LastMessage string
	Elapsed     time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("wait timed out after %s: %s (last: %s)", e.Elapsed.Round(time.Millisecond), e.What, e.LastMessage)
}

// IsTimeout reports whether err (or anything it wraps) is a TimeoutError.
func IsTimeout(err error) bool {
	var te *TimeoutError
	return errors.As(err, &te)
}

// Helper bundles the dependencies the primitives need: a kube client
// for CR/event polling, a logger for progress lines, and a default
// poll interval.
type Helper struct {
	Kube     *pgkube.Client
	Logger   *slog.Logger
	Interval time.Duration
}

// NewHelper builds a Helper with sensible defaults.
func NewHelper(k *pgkube.Client, logger *slog.Logger) *Helper {
	if logger == nil {
		logger = slog.Default()
	}
	return &Helper{Kube: k, Logger: logger, Interval: time.Second}
}

func (h *Helper) interval() time.Duration {
	if h.Interval > 0 {
		return h.Interval
	}
	return time.Second
}

// CRCondition is the predicate body for UntilCR. It returns whether
// the wait is satisfied, a human-readable message describing the
// current observation, and a hard error (which terminates the wait).
type CRCondition func(*v1alpha1.MysqlFailoverGroup) (done bool, message string, err error)

// UntilCR polls the playground MFG until cond returns done=true, ctx
// expires, or cond returns a hard error.
func (h *Helper) UntilCR(ctx context.Context, namespace, what string, cond CRCondition) (*v1alpha1.MysqlFailoverGroup, error) {
	start := time.Now()
	tick := time.NewTicker(h.interval())
	defer tick.Stop()

	last := "no observation yet"
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()

	check := func() (*v1alpha1.MysqlFailoverGroup, bool, error) {
		mfg, err := h.Kube.GetMFG(ctx, namespace)
		if err != nil {
			last = "get MFG failed: " + err.Error()
			return nil, false, nil
		}
		done, msg, ferr := cond(mfg)
		if ferr != nil {
			return mfg, false, ferr
		}
		last = msg
		return mfg, done, nil
	}

	if mfg, done, err := check(); err != nil {
		return mfg, err
	} else if done {
		return mfg, nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil, &TimeoutError{What: what, LastMessage: last, Elapsed: time.Since(start)}
		case <-progress.C:
			h.Logger.Info("wait", "what", what, "last", last, "elapsed", time.Since(start).Round(time.Second))
		case <-tick.C:
			if mfg, done, err := check(); err != nil {
				return mfg, err
			} else if done {
				return mfg, nil
			}
		}
	}
}

// LogCondition is the predicate body for UntilLog.
type LogCondition = pglogs.Predicate

// UntilLog blocks until the tailer observes a line satisfying pred,
// scoped to lines emitted at or after since.
func (h *Helper) UntilLog(ctx context.Context, t *pglogs.Tailer, since time.Time, what string, pred LogCondition) (pglogs.Match, error) {
	start := time.Now()
	m, err := t.Wait(ctx, since, pred)
	if err != nil {
		return pglogs.Match{}, &TimeoutError{What: what, LastMessage: "no matching log line observed", Elapsed: time.Since(start)}
	}
	h.Logger.Info("matched log", "what", what, "line", m.Line, "elapsed", time.Since(start).Round(time.Millisecond))
	return m, nil
}

// MetricCondition is the predicate body for UntilMetric.
type MetricCondition func(*pgmetrics.Snapshot) (done bool, message string)

// UntilMetric polls /metrics until cond returns done=true.
func (h *Helper) UntilMetric(ctx context.Context, scraper *pgmetrics.Scraper, what string, cond MetricCondition) error {
	start := time.Now()
	tick := time.NewTicker(h.interval())
	defer tick.Stop()
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()

	last := "no scrape yet"
	check := func() (bool, error) {
		snap, err := scraper.Scrape(ctx)
		if err != nil {
			last = "scrape failed: " + err.Error()
			return false, nil
		}
		done, msg := cond(snap)
		last = msg
		return done, nil
	}
	if done, err := check(); err != nil {
		return err
	} else if done {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return &TimeoutError{What: what, LastMessage: last, Elapsed: time.Since(start)}
		case <-progress.C:
			h.Logger.Info("wait", "what", what, "last", last, "elapsed", time.Since(start).Round(time.Second))
		case <-tick.C:
			if done, err := check(); err != nil {
				return err
			} else if done {
				return nil
			}
		}
	}
}

// SidecarCondition is the predicate body for UntilSidecarStatus.
type SidecarCondition func(*pgsidecar.StatusResponse) (done bool, message string)

// UntilSidecarStatus polls a sidecar's /status until cond returns
// done=true. Used to assert super_read_only flips during partition
// scenarios.
func (h *Helper) UntilSidecarStatus(ctx context.Context, p *pgsidecar.Probe, what string, cond SidecarCondition) error {
	start := time.Now()
	tick := time.NewTicker(h.interval())
	defer tick.Stop()
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()

	last := "no probe yet"
	check := func() (bool, error) {
		st, err := p.Status(ctx)
		if err != nil {
			last = "probe failed: " + err.Error()
			return false, nil
		}
		done, msg := cond(st)
		last = msg
		return done, nil
	}
	if done, err := check(); err != nil {
		return err
	} else if done {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return &TimeoutError{What: what, LastMessage: last, Elapsed: time.Since(start)}
		case <-progress.C:
			h.Logger.Info("wait", "what", what, "last", last, "elapsed", time.Since(start).Round(time.Second))
		case <-tick.C:
			if done, err := check(); err != nil {
				return err
			} else if done {
				return nil
			}
		}
	}
}
