package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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
// the runner's status writers can copy the data into the
// MysqlFailoverGroup.status.pitr field without an extra network call.
// It also owns the observation history behind the PITRArchiveReady
// condition (pitrArchiveTracker), and after every completed round runs
// onRound, the runner's PITR status writer.
//
// Lifecycle mirrors the TopologyManager it lives alongside: one poller
// per FG, cancelled together with the manager when the CR disappears
// or the operator loses leader election.
type archiverPoller struct {
	nn       nameKey
	clients  []siteArchiverClient
	logger   *slog.Logger
	interval time.Duration
	now      func() time.Time
	onRound  func(ctx context.Context)
	// awaitingRollout returns the sites whose ordered update is still
	// pending (the topology manager's spec-drift set). nil means none.
	awaitingRollout func() []string
	// rolloutKnown reports whether awaitingRollout's set has been
	// computed yet. nil means it always has.
	rolloutKnown func() bool
	// uid is the group's UID, so a replacement poller only inherits
	// history from a poller of the same object.
	uid types.UID

	mu        sync.RWMutex
	snapshots map[string]*internalmysql.ArchiverStatus
	// lastGood is each site's latest successful snapshot, kept so
	// status.pitr coverage stays last-known through a missed poll.
	lastGood   map[string]*internalmysql.ArchiverStatus
	pollErrors map[string]string
	tracker    *pitrArchiveTracker
	// statusWrittenAt is when the poller's status writer last wrote, for
	// rate-limiting message-only changes.
	statusWrittenAt time.Time
	// rolloutRenders records whether the current PITR spec renders an
	// enabled archiver; only then can a disabled sidecar be awaiting its
	// rollout. Refreshed from every object a status writer fetches.
	rolloutRenders bool
	// anchorReadFailing is set while uncached reads for anchoring fail,
	// so the failure is logged once per episode. Set before Run, then
	// used only by the poller's goroutine.
	anchorReadFailing bool
}

// nameKey is a small alias used purely to keep the metric-label
// signature consistent between the poller and updateCRStatus.
type nameKey struct {
	Namespace string
	Name      string
}

// newArchiverPoller builds a poller for sites starting from tracker (see
// TopologyManagerRunner.seedPITRArchiveTracker).
func newArchiverPoller(ns, name string, sites []string, clients []*internalmysql.SidecarClient, tracker *pitrArchiveTracker, logger *slog.Logger) *archiverPoller {
	if tracker == nil {
		tracker = newPITRArchiveTracker(nil)
	}
	pairs := make([]siteArchiverClient, len(sites))
	for i, s := range sites {
		pairs[i] = siteArchiverClient{name: s, client: clients[i]}
	}
	return &archiverPoller{
		nn:         nameKey{Namespace: ns, Name: name},
		clients:    pairs,
		logger:     logger.With("component", "archiver-poller"),
		interval:   archiverPollInterval,
		now:        time.Now,
		snapshots:  make(map[string]*internalmysql.ArchiverStatus, len(pairs)),
		lastGood:   make(map[string]*internalmysql.ArchiverStatus, len(pairs)),
		pollErrors: make(map[string]string, len(pairs)),
		tracker:    tracker,
	}
}

// Run blocks until ctx is cancelled. One immediate round + ticker.
func (p *archiverPoller) Run(ctx context.Context) {
	p.round(ctx)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.clearMetrics()
			return
		case <-t.C:
			p.round(ctx)
		}
	}
}

// round polls every site once and, if the round completed, runs onRound.
func (p *archiverPoller) round(ctx context.Context) {
	if !p.scan(ctx) || p.onRound == nil {
		return
	}
	p.onRound(ctx)
}

// scan queries each sidecar once and records the round. Per-site errors
// log at Debug — the sidecar HTTP surface is best-effort observability,
// not a reconcile gate; a flaky poll shouldn't spam operator logs. A
// round cut short by shutdown is discarded rather than counted as
// missed polls; scan then returns false.
func (p *archiverPoller) scan(ctx context.Context) bool {
	// Stamp the round with the tick time, not the time the last poll
	// returned, so a slow first poll does not shorten an unreachable span.
	at := p.now()
	awaiting := map[string]bool{}
	p.mu.RLock()
	renders := p.rolloutRenders
	p.mu.RUnlock()
	if renders {
		if p.rolloutKnown != nil && !p.rolloutKnown() {
			// Drift not computed yet: any site may still be rolling.
			for _, c := range p.clients {
				awaiting[c.name] = true
			}
		} else if p.awaitingRollout != nil {
			for _, site := range p.awaitingRollout() {
				awaiting[site] = true
			}
		}
	}
	results := make([]archiverPollResult, 0, len(p.clients))
	for _, pair := range p.clients {
		var (
			status *internalmysql.ArchiverStatus
			err    error
		)
		if pair.client == nil {
			err = fmt.Errorf("no sidecar client")
		} else {
			reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			status, err = pair.client.GetArchiverStatus(reqCtx)
			cancel()
		}
		if err != nil {
			p.logger.Debug("archiver status poll failed", "site", pair.name, "error", err)
			// Drop the site's gauge labels so scrapes see absent
			// rather than a stale "last known good" backlog value.
			// alert rules can use absent()/up() semantics during
			// sidecar outages.
			p.clearSiteMetrics(pair.name)
			results = append(results, archiverPollResult{site: pair.name, err: err, awaitingRollout: awaiting[pair.name]})
			continue
		}
		p.emitMetrics(pair.name, status)
		results = append(results, archiverPollResult{site: pair.name, status: status, awaitingRollout: awaiting[pair.name]})
	}
	if ctx.Err() != nil {
		return false
	}
	p.record(at, results)
	return true
}

// record stores one completed round and advances the PITRArchiveReady
// tracker, under one lock so readers never see one without the other.
func (p *archiverPoller) record(at time.Time, results []archiverPollResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastGood == nil {
		p.lastGood = map[string]*internalmysql.ArchiverStatus{}
	}
	if p.pollErrors == nil {
		p.pollErrors = map[string]string{}
	}
	if p.tracker == nil {
		p.tracker = newPITRArchiveTracker(nil)
	}
	for _, r := range results {
		p.snapshots[r.site] = r.status
		if r.status != nil {
			p.lastGood[r.site] = r.status
			delete(p.pollErrors, r.site)
		} else if r.err != nil {
			p.pollErrors[r.site] = r.err.Error()
		}
	}
	p.tracker.observe(at, results)
}

// archiveVerdict returns the PITRArchiveReady verdict for the rounds
// recorded so far.
func (p *archiverPoller) archiveVerdict() pitrArchiveVerdict {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.tracker == nil {
		return newPITRArchiveTracker(nil).current()
	}
	return p.tracker.current()
}

// archiveCondition returns the verdict, and false while the tracker has
// not been anchored on an authoritative read (see anchorPITRArchiveTracker):
// until then the writers leave the persisted condition alone.
func (p *archiverPoller) archiveCondition() (pitrArchiveVerdict, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.tracker == nil {
		return newPITRArchiveTracker(nil).current(), true
	}
	return p.tracker.current(), p.tracker.anchored
}

func (p *archiverPoller) isAnchored() bool {
	_, anchored := p.archiveCondition()
	return anchored
}

// anchor reconciles the tracker with a condition read authoritatively.
func (p *archiverPoller) anchor(persisted *metav1.Condition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tracker == nil {
		p.tracker = newPITRArchiveTracker(nil)
	}
	p.tracker.anchor(persisted)
}

// observeSpec refreshes what the poller takes from the group's spec: the
// archiver scan interval behind the stuck-error bound, and whether the
// PITR spec renders an enabled archiver.
func (p *archiverPoller) observeSpec(fg *v1alpha1.MysqlFailoverGroup) {
	renders := pitrSpecRendersArchiver(fg)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rolloutRenders = renders
	if p.tracker != nil {
		p.tracker.scanInterval = pitrArchiveScanInterval(fg)
	}
}

func (p *archiverPoller) lastStatusWrite() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.statusWrittenAt
}

func (p *archiverPoller) setLastStatusWrite(t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statusWrittenAt = t
}

// handoff returns the poller's history for a replacement. Call only
// after the poller has stopped; the replacement takes ownership.
func (p *archiverPoller) handoff() pitrArchiveHandoff {
	p.mu.Lock()
	defer p.mu.Unlock()
	lastGood := make(map[string]*internalmysql.ArchiverStatus, len(p.lastGood))
	for k, v := range p.lastGood {
		lastGood[k] = v
	}
	return pitrArchiveHandoff{uid: p.uid, sites: p.siteNamesLocked(), tracker: p.tracker, lastGood: lastGood}
}

// siteNames returns the polled sites, sorted.
func (p *archiverPoller) siteNames() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.siteNamesLocked()
}

func (p *archiverPoller) siteNamesLocked() []string {
	var names []string
	if len(p.clients) > 0 {
		for _, c := range p.clients {
			names = append(names, c.name)
		}
	} else {
		for name := range p.snapshots {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// resetArchiveTracker forgets the observation history, used while PITR
// is disabled so a later enable starts from Unknown.
func (p *archiverPoller) resetArchiveTracker() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tracker != nil {
		p.tracker.reset()
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

// Snapshots returns a shallow copy of the cached per-site snapshots.
// Callers treat the map as read-only; the poller owns the values.
func (p *archiverPoller) Snapshots() map[string]*internalmysql.ArchiverStatus {
	snaps, _, _ := p.view()
	return snaps
}

// view returns copies of the latest snapshots (nil for a failed poll),
// each site's last successful snapshot, and the latest poll error of each
// site whose last poll failed.
func (p *archiverPoller) view() (snaps, lastGood map[string]*internalmysql.ArchiverStatus, pollErrors map[string]string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snaps = make(map[string]*internalmysql.ArchiverStatus, len(p.snapshots))
	for k, v := range p.snapshots {
		snaps[k] = v
	}
	lastGood = make(map[string]*internalmysql.ArchiverStatus, len(p.lastGood))
	for k, v := range p.lastGood {
		lastGood[k] = v
	}
	pollErrors = make(map[string]string, len(p.pollErrors))
	for k, v := range p.pollErrors {
		pollErrors[k] = v
	}
	return snaps, lastGood, pollErrors
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
		url := fmt.Sprintf("http://%s:%d", internalSiteServiceHost(fg.Name, site.Name, fg.Namespace), sidecarPort)
		out[i] = internalmysql.NewSidecarClient(url)
	}
	return out
}
