package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// archiverPollInterval is how often the operator queries each sidecar
// for its PITR archiver status. 30s is well below the default 1h PITR
// retention cadence but slow enough that the aggregate query cost of
// recomputing manifest bounds on the sidecar stays negligible.
const archiverPollInterval = 30 * time.Second

// siteArchiverClient pairs a site's name with a sidecar HTTP client.
// Kept as a small struct so the poller owns nothing mutable beyond the
// cached snapshot.
type siteArchiverClient struct {
	name   string
	client *internalmysql.SidecarClient
}

// archiverPoller periodically scrapes each sidecar's /archiver/status
// endpoint, emits Prometheus gauges, and caches the latest snapshot so
// TopologyManagerRunner.updateCRStatus can copy the data into the
// MysqlFailoverGroup.status.pitr field without an extra network call.
//
// Lifecycle mirrors the TopologyManager it lives alongside: one poller
// per FG, cancelled together with the manager when the CR disappears
// or the operator loses leader election.
type archiverPoller struct {
	nn       nameKey
	clients  []siteArchiverClient
	logger   *slog.Logger
	interval time.Duration

	mu        sync.RWMutex
	snapshots map[string]*internalmysql.ArchiverStatus
}

// nameKey is a small alias used purely to keep the metric-label
// signature consistent between the poller and updateCRStatus.
type nameKey struct {
	Namespace string
	Name      string
}

func newArchiverPoller(ns, name string, sites []string, clients []*internalmysql.SidecarClient, logger *slog.Logger) *archiverPoller {
	pairs := make([]siteArchiverClient, len(sites))
	for i, s := range sites {
		pairs[i] = siteArchiverClient{name: s, client: clients[i]}
	}
	return &archiverPoller{
		nn:        nameKey{Namespace: ns, Name: name},
		clients:   pairs,
		logger:    logger.With("component", "archiver-poller"),
		interval:  archiverPollInterval,
		snapshots: make(map[string]*internalmysql.ArchiverStatus, len(pairs)),
	}
}

// Run blocks until ctx is cancelled. One immediate scan + ticker.
func (p *archiverPoller) Run(ctx context.Context) {
	p.scan(ctx)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.clearMetrics()
			return
		case <-t.C:
			p.scan(ctx)
		}
	}
}

// scan queries each sidecar once. Per-site errors log at Debug — the
// sidecar HTTP surface is best-effort observability, not a reconcile
// gate; a flaky poll shouldn't spam operator logs.
func (p *archiverPoller) scan(ctx context.Context) {
	for _, pair := range p.clients {
		reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		status, err := pair.client.GetArchiverStatus(reqCtx)
		cancel()
		if err != nil {
			p.logger.Debug("archiver status poll failed", "site", pair.name, "error", err)
			p.store(pair.name, nil)
			// Drop the site's gauge labels so scrapes see absent
			// rather than a stale "last known good" backlog value.
			// alert rules can use absent()/up() semantics during
			// sidecar outages.
			p.clearSiteMetrics(pair.name)
			continue
		}
		p.store(pair.name, status)
		p.emitMetrics(pair.name, status)
	}
}

// clearSiteMetrics drops all archiver gauges for a single site. Used
// when a poll fails, so Prometheus won't report stale values during a
// sidecar outage.
func (p *archiverPoller) clearSiteMetrics(site string) {
	metrics.ArchiverUploadFailures.DeleteLabelValues(p.nn.Namespace, p.nn.Name, site)
	metrics.ArchiverLastUploadTimestamp.DeleteLabelValues(p.nn.Namespace, p.nn.Name, site)
	metrics.ArchiverBacklogFiles.DeleteLabelValues(p.nn.Namespace, p.nn.Name, site)
}

func (p *archiverPoller) store(site string, s *internalmysql.ArchiverStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots[site] = s
}

// Snapshots returns a shallow copy of the cached per-site snapshots.
// Callers treat the map as read-only; the poller owns the values.
func (p *archiverPoller) Snapshots() map[string]*internalmysql.ArchiverStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]*internalmysql.ArchiverStatus, len(p.snapshots))
	for k, v := range p.snapshots {
		out[k] = v
	}
	return out
}

func (p *archiverPoller) emitMetrics(site string, s *internalmysql.ArchiverStatus) {
	if s == nil || !s.Enabled {
		metrics.ArchiverUploadFailures.WithLabelValues(p.nn.Namespace, p.nn.Name, site).Set(0)
		metrics.ArchiverLastUploadTimestamp.WithLabelValues(p.nn.Namespace, p.nn.Name, site).Set(0)
		metrics.ArchiverBacklogFiles.WithLabelValues(p.nn.Namespace, p.nn.Name, site).Set(0)
		return
	}
	metrics.ArchiverUploadFailures.WithLabelValues(p.nn.Namespace, p.nn.Name, site).Set(float64(s.UploadFailures))
	var lastUpload float64
	if !s.LastUploadAt.IsZero() {
		lastUpload = float64(s.LastUploadAt.Unix())
	}
	metrics.ArchiverLastUploadTimestamp.WithLabelValues(p.nn.Namespace, p.nn.Name, site).Set(lastUpload)
	metrics.ArchiverBacklogFiles.WithLabelValues(p.nn.Namespace, p.nn.Name, site).Set(float64(s.BacklogFiles))
}

// clearMetrics wipes this poller's gauge values on shutdown so a
// stopped FG's labels don't linger in /metrics forever.
func (p *archiverPoller) clearMetrics() {
	for _, pair := range p.clients {
		p.clearSiteMetrics(pair.name)
	}
}

// buildSidecarClients constructs one SidecarClient per site using the
// same in-cluster DNS pattern that the reconciler uses for the sidecar
// peer address. Returned slice is aligned with fg.Spec.Sites.
func buildSidecarClients(fg *v1alpha1.MysqlFailoverGroup) []*internalmysql.SidecarClient {
	out := make([]*internalmysql.SidecarClient, len(fg.Spec.Sites))
	for i, site := range fg.Spec.Sites {
		url := fmt.Sprintf("http://mysql-%s-%s.%s.svc.cluster.local:%d",
			fg.Name, site.Name, fg.Namespace, sidecarPort)
		out[i] = internalmysql.NewSidecarClient(url)
	}
	return out
}
