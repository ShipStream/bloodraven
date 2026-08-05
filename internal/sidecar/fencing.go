package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
)

// Fencer abstracts MySQL operations needed by the fencing monitor.
type Fencer interface {
	IsReadOnly(ctx context.Context) (bool, error)
	SetSuperReadOnly(ctx context.Context) error
	KillConnections(ctx context.Context) (int, error)
}

// FencingMonitor polls Bloodraven and every peer sidecar, and self-
// fences (sets super_read_only=ON) when one of two conditions holds:
//
//  1. Topology mismatch: the operator-authoritative active site
//     disagrees with this sidecar's own site. Fires immediately,
//     regardless of lease timing. Closes the "stale primary returns
//     after a failover while the operator is unreachable to it but
//     reachable to the peer" gap (WISHLIST item #4).
//
//  2. Lease expiry: both the operator AND every peer are unreachable
//     beyond leaseTimeout. The classic fallback for when the sidecar
//     can't learn anything authoritative.
//
// Multi-site semantics for rule #2: the monitor tracks a per-peer
// "last-seen" timestamp. The site is considered to have peer
// connectivity as long as *any* peer answered within the lease
// timeout. Self-fencing fires only when every peer is silent.
//
// Topology-aware fencing (rule #1) is only active when mySite,
// namespace, and group are all non-empty AND a TopologyCache is
// attached. Otherwise the monitor degrades to lease-only behavior,
// which matches historical behavior and keeps single-site test
// harnesses simple.
type FencingMonitor struct {
	mysql            Fencer
	bloodravenAddr   string
	peerAddrs        []string
	checkInterval    time.Duration
	leaseTimeout     time.Duration
	lastBloodravenOK time.Time
	lastPeerOK       map[string]time.Time
	// fenced is written by the monitor goroutine and read by the sidecar
	// HTTP handler that reports self-fenced state, so it is atomic rather
	// than a plain bool.
	fenced     atomic.Bool
	logger     *slog.Logger
	httpClient *http.Client
	clock      clock.Clock

	// Topology-aware fields. Populated by WithTopology; zero values
	// disable rule #1 above. mySite/namespace/group are the identity
	// this sidecar uses when hitting the operator's /active-site
	// endpoint and when comparing against the cached authoritative
	// answer.
	mySite    string
	namespace string
	group     string
	topology  *TopologyCache
}

// NewFencingMonitor creates a new FencingMonitor.
func NewFencingMonitor(
	mysql Fencer,
	bloodravenAddr string,
	peerAddrs []string,
	checkInterval time.Duration,
	leaseTimeout time.Duration,
	logger *slog.Logger,
) *FencingMonitor {
	return NewFencingMonitorWithClock(mysql, bloodravenAddr, peerAddrs, checkInterval, leaseTimeout, logger, clock.RealClock{})
}

// NewFencingMonitorWithClock creates a FencingMonitor with an injectable clock for testing.
func NewFencingMonitorWithClock(
	mysql Fencer,
	bloodravenAddr string,
	peerAddrs []string,
	checkInterval time.Duration,
	leaseTimeout time.Duration,
	logger *slog.Logger,
	clk clock.Clock,
) *FencingMonitor {
	return NewFencingMonitorFull(mysql, bloodravenAddr, peerAddrs, checkInterval, leaseTimeout, logger, clk, nil)
}

// NewFencingMonitorFull creates a FencingMonitor with all injectable dependencies.
// Pass nil for httpClient to use a default client with a 2s timeout.
func NewFencingMonitorFull(
	mysql Fencer,
	bloodravenAddr string,
	peerAddrs []string,
	checkInterval time.Duration,
	leaseTimeout time.Duration,
	logger *slog.Logger,
	clk clock.Clock,
	httpClient *http.Client,
) *FencingMonitor {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 2 * time.Second}
	}
	// Filter empty entries so the monitor does not treat "" as a peer
	// that will never answer and keep the primary fenced forever.
	var cleaned []string
	for _, p := range peerAddrs {
		if p != "" {
			cleaned = append(cleaned, p)
		}
	}
	return &FencingMonitor{
		mysql:          mysql,
		bloodravenAddr: bloodravenAddr,
		peerAddrs:      cleaned,
		checkInterval:  checkInterval,
		leaseTimeout:   leaseTimeout,
		logger:         logger,
		clock:          clk,
		httpClient:     httpClient,
		lastPeerOK:     make(map[string]time.Time, len(cleaned)),
	}
}

// WithTopology enables topology-aware fencing. Call once after
// construction and before Run. Returns the receiver so it can be
// chained off NewFencingMonitor(...).
//
// mySite, namespace, and group identify this sidecar to the
// operator's /active-site endpoint. cache is shared with the sidecar
// HTTP server so peers can relay the view through /peer/active-site.
// Passing "" for any identity field, or nil for cache, leaves
// topology-aware fencing disabled and the monitor continues to use
// the lease-expiry rule only.
func (f *FencingMonitor) WithTopology(mySite, namespace, group string, cache *TopologyCache) *FencingMonitor {
	f.mySite = mySite
	f.namespace = namespace
	f.group = group
	f.topology = cache
	return f
}

// topologyEnabled reports whether the monitor has everything it
// needs to fetch and compare authoritative topology state.
func (f *FencingMonitor) topologyEnabled() bool {
	return f.topology != nil && f.mySite != "" && f.namespace != "" && f.group != ""
}

// SeedStartupGrace stamps every reachability signal as seen right now, so
// rule #2 cannot fire until a full lease has elapsed since this call.
//
// Run does this before its first tick, which is the only thing that keeps a
// freshly started sidecar from fencing its own primary on tick one: an
// unseeded monitor reads a zero lastBloodravenOK, computes an unbounded
// silence, and self-fences immediately. Callers that drive Check directly
// instead of Run — deterministic harnesses — must call this first or they
// are testing a monitor no production process ever is.
//
// The reachability timestamps this writes are owned by a single goroutine:
// Run (or, in harnesses, whichever goroutine drives Check). They are
// deliberately unsynchronized — the only cross-goroutine field on the
// monitor is the atomic fenced flag — so SeedStartupGrace must not be
// called concurrently with Run or Check.
func (f *FencingMonitor) SeedStartupGrace(now time.Time) {
	f.lastBloodravenOK = now
	for _, addr := range f.peerAddrs {
		f.lastPeerOK[addr] = now
	}
}

// Run starts the fencing monitor loop. Blocks until ctx is cancelled.
func (f *FencingMonitor) Run(ctx context.Context) {
	f.SeedStartupGrace(f.clock.Now())

	ticker := f.clock.NewTicker(f.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			f.Check(ctx)
		}
	}
}

// Check performs a single fencing check cycle. Exported for deterministic testing.
func (f *FencingMonitor) Check(ctx context.Context) {
	f.checkBloodraven(ctx)
	if f.topologyEnabled() {
		f.checkActiveSite(ctx)
	}
	for _, addr := range f.peerAddrs {
		f.checkPeer(ctx, addr)
		if f.topologyEnabled() {
			f.checkPeerTopology(ctx, addr)
		}
	}
	f.evaluate(ctx)
}

func (f *FencingMonitor) checkBloodraven(ctx context.Context) {
	if f.bloodravenAddr == "" {
		return
	}

	url := fmt.Sprintf("http://%s/healthz", f.bloodravenAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		f.logger.Warn("fencing: failed to create bloodraven request", "error", err)
		return
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		f.logger.Debug("fencing: bloodraven unreachable", "addr", f.bloodravenAddr, "error", err)
		return
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		f.lastBloodravenOK = f.clock.Now()
	}
}

func (f *FencingMonitor) checkPeer(ctx context.Context, addr string) {
	if addr == "" {
		return
	}

	url := fmt.Sprintf("http://%s/peer/ping", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		f.logger.Warn("fencing: failed to create peer request", "peer", addr, "error", err)
		return
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		f.logger.Debug("fencing: peer unreachable", "peer", addr, "error", err)
		return
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		f.lastPeerOK[addr] = f.clock.Now()
	}
}

// checkActiveSite fetches the operator's authoritative view of the
// current active site for this failover group. On success the cache
// is overwritten (operator is always authoritative, so we never
// compare timestamps here). Failures are silent — the cache ages
// naturally and peer relays can keep it fresh.
func (f *FencingMonitor) checkActiveSite(ctx context.Context) {
	if f.bloodravenAddr == "" {
		return
	}

	endpoint := fmt.Sprintf("http://%s/active-site?namespace=%s&group=%s",
		f.bloodravenAddr, url.QueryEscape(f.namespace), url.QueryEscape(f.group))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		f.logger.Warn("fencing: failed to create active-site request", "error", err)
		return
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		f.logger.Debug("fencing: operator /active-site unreachable", "error", err)
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		// 404 (group not found yet) or 503 (operator not ready) just
		// mean no authoritative view available — not a fencing signal
		// on its own. Log at debug.
		f.logger.Debug("fencing: operator /active-site non-200", "status", resp.StatusCode)
		return
	}

	var body struct {
		ActiveSite string `json:"activeSite"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		f.logger.Warn("fencing: decode /active-site response", "error", err)
		return
	}
	if body.ActiveSite == "" {
		// Operator admits it has no active site (e.g. quorum not yet
		// established). Clear any older active-site view so a stale
		// cache cannot self-fence a newly promoted site during the
		// promotion-confirmation gap.
		f.topology.Set("", f.clock.Now())
		return
	}
	f.topology.Set(body.ActiveSite, f.clock.Now())
}

// checkPeerTopology asks a peer sidecar for its last-known authoritative
// view of the active site. This is the partition-tolerant path: if
// this sidecar's link to the operator is broken but a peer's link
// still works, the peer's cache stays fresh and we can adopt it.
// Adoption only happens when the peer's observedAt is strictly newer
// than our own, so a stale peer never drags us backwards.
func (f *FencingMonitor) checkPeerTopology(ctx context.Context, addr string) {
	if addr == "" {
		return
	}

	endpoint := fmt.Sprintf("http://%s/peer/active-site", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		f.logger.Warn("fencing: failed to create peer active-site request", "peer", addr, "error", err)
		return
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		f.logger.Debug("fencing: peer /peer/active-site unreachable", "peer", addr, "error", err)
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		// 404 means the peer predates this endpoint (rolling upgrade);
		// we silently fall back to operator-only topology data.
		return
	}

	var snap TopologySnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		f.logger.Warn("fencing: decode /peer/active-site response", "peer", addr, "error", err)
		return
	}
	if snap.ActiveSite == "" || snap.ObservedAt.IsZero() {
		return
	}
	if f.topology.Adopt(snap.ActiveSite, snap.ObservedAt) {
		f.logger.Info("fencing: adopted active-site view from peer",
			"peer", addr, "activeSite", snap.ActiveSite, "observedAt", snap.ObservedAt)
	}
}

// latestPeerSeen returns the most recent time at which any peer was
// observed healthy. Returns the zero time when no peers are configured.
func (f *FencingMonitor) latestPeerSeen() time.Time {
	var latest time.Time
	for _, t := range f.lastPeerOK {
		if t.After(latest) {
			latest = t
		}
	}
	return latest
}

func (f *FencingMonitor) evaluate(ctx context.Context) {
	// Only the primary should self-fence. If MySQL is already read-only, skip.
	readOnly, err := f.mysql.IsReadOnly(ctx)
	if err != nil {
		f.logger.Warn("fencing: could not check read_only status", "error", err)
		return
	}
	if f.fenced.Load() {
		if readOnly {
			return
		}
		// Writability after a self-fence means an actor with SUPER
		// privileges restored it — by contract ("only Bloodraven can
		// restore") that is the operator promoting or re-asserting this
		// site. Treat the restore as fresh evidence of controller
		// intervention and grant a full lease window before rule #2 can
		// fire again, exactly like process startup. Without this reset
		// the monitor re-evaluates against pre-outage timestamps and
		// instantly re-fences a freshly promoted primary whose operator
		// is driving MySQL but not yet reachable through its auxiliary
		// Service (endpoint readiness lags an operator restart), wedging
		// the group in a no-writable-site state.
		f.logger.Info("fencing: MySQL is writable after prior self-fence; rearming monitor")
		f.fenced.Store(false)
		now := f.clock.Now()
		f.lastBloodravenOK = now
		for addr := range f.lastPeerOK {
			f.lastPeerOK[addr] = now
		}
		if f.topologyEnabled() {
			f.topology.Set("", now)
		}
	}
	if readOnly {
		return
	}

	// Rule #1 — topology mismatch. If the cached authoritative
	// active site is known and disagrees with our site, fence
	// immediately. This catches the "stale primary returns from a
	// partition to find the operator has failed over to the peer"
	// scenario even when the peer is reachable (peer reachability
	// alone would otherwise keep rule #2 quiet).
	if f.topologyEnabled() {
		snap := f.topology.Snapshot()
		if snap.ActiveSite != "" && snap.ActiveSite != f.mySite {
			f.logger.Error("SELF-FENCING: topology mismatch — operator-authoritative active site disagrees with our site, setting super_read_only=ON",
				"site", f.mySite,
				"authoritativeActiveSite", snap.ActiveSite,
				"observedAt", snap.ObservedAt,
			)
			f.doFence(ctx)
			return
		}
	}

	// Rule #2 — lease expiry on every reachability signal.
	now := f.clock.Now()
	bloodravenDown := now.Sub(f.lastBloodravenOK) > f.leaseTimeout

	// Peers: if no peers are configured, treat peers as "down" so the
	// operator-only signal drives fencing. Otherwise require *every*
	// peer to be silent beyond the lease.
	peersDown := true
	if len(f.peerAddrs) > 0 {
		latest := f.latestPeerSeen()
		peersDown = latest.IsZero() || now.Sub(latest) > f.leaseTimeout
	}

	if !bloodravenDown || !peersDown {
		return
	}

	f.logger.Error("SELF-FENCING: Bloodraven and every peer unreachable beyond lease timeout, setting super_read_only=ON",
		"bloodravenLastOk", f.lastBloodravenOK,
		"latestPeerOk", f.latestPeerSeen(),
		"peers", f.peerAddrs,
		"leaseTimeout", f.leaseTimeout,
	)

	f.doFence(ctx)
}

// fenceTimeout bounds the whole fence sequence — the super_read_only
// write and the connection eviction that follows it — because both run
// on the monitor's own goroutine. Either one blocking on an unresponsive
// server would stop every subsequent fencing check for this site,
// indefinitely, and losing the tick loop is worse than losing this
// attempt: a fence that times out is retried on the next tick, whereas a
// wedged loop never fences again.
//
// Chosen well above a healthy sequence (one SET GLOBAL plus a handful of
// KILLs against local MySQL) and well below the point where a stalled
// monitor stops mattering.
const fenceTimeout = 20 * time.Second

// fenceProbeTimeout bounds the read that resolves an ambiguous fence
// write. Short: it runs after the fence sequence already spent its own
// budget, and a server too slow to answer one `SELECT @@read_only` is
// better left to the next tick than allowed to extend the stall.
const fenceProbeTimeout = 5 * time.Second

// doFence performs the actual SET GLOBAL super_read_only=ON +
// KILL-app-connections step and flips the fenced flag. Separated so
// both fencing rules share the same write sequence.
func (f *FencingMonitor) doFence(ctx context.Context) {
	fenceCtx, cancelFence := context.WithTimeout(ctx, fenceTimeout)
	defer cancelFence()

	if err := f.mysql.SetSuperReadOnly(fenceCtx); err != nil {
		// A write that reports an error may still have landed. Cancelling
		// the context tears down the client connection; it does not roll
		// back a SET GLOBAL the server already applied, and the same is
		// true of a connection lost while waiting for the reply. So an
		// error here means "unknown", not "did not fence".
		//
		// Assuming failure on an ambiguous result is not the safe default,
		// because the flag is what arms the rearm branch in evaluate().
		// Left false against an instance that is genuinely read-only, the
		// monitor skips that branch when the operator later restores
		// writability, never refreshes its lease timestamps, and re-fences
		// the site it just promoted — the no-writable-site wedge the rearm
		// path exists to prevent. Resolve it with a fresh read instead.
		if !f.fenceLanded(ctx) {
			f.logger.Error("SELF-FENCING FAILED: could not set super_read_only", "error", err)
			return
		}
		// The fence holds, so record it and emit the terminal event. The
		// eviction is skipped deliberately: fenceCtx is spent, every KILL
		// on it would fail immediately, and eviction is cleanup on a fence
		// that already holds — the same surviving-session cost documented
		// below for a partial eviction, with the same bound (reads only).
		f.fenced.Store(true)
		f.logger.Warn("SELF-FENCING: super_read_only write failed but the fence is in place; skipping connection eviction",
			"error", err)
		f.logger.Error("SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore")
		return
	}
	// The fence *is* super_read_only=ON, so record it the moment that
	// write lands rather than after the eviction below. Eviction is
	// cleanup on a fence that already holds, and it runs under the shared
	// fenceTimeout below — a site draining a slow KILL would otherwise
	// report self_fenced=false while demonstrably fenced.
	f.fenced.Store(true)

	// KillConnections reports a partial eviction as (killed>0, err): the
	// sessions it could identify are gone, but it could not enumerate or
	// kill them all. Carry the count so the warning says how far the fence
	// got.
	//
	// Deliberately not retried on later ticks. A surviving session cannot
	// write — super_read_only=ON is set above and holds — so what is left
	// is a stale-read window, and closing it from here means issuing KILLs
	// against a site the operator may be promoting. There is no
	// interleaving that makes that safe from inside the sidecar: the
	// operator holds a persistent pooled connection to every site
	// (internal/mysql/checker.go), so its future promotion session can
	// already be in the process list at fence time, and killing it aborts
	// the promotion mid-sequence. Draining stragglers with retries is the
	// operator's job, where it can be sequenced against its own promotion
	// — see the planned-failover Draining phase and spec.plannedFailover
	// .drainTimeout, which kills every second until the count reaches zero.
	//
	// That covers planned failover only. An autonomous self-fence (rule #1
	// or #2) has no operator-side follow-up, so a session that survives it
	// keeps reading stale data until the site is next promoted or demoted.
	// That is the accepted cost: it is bounded to reads, and the warning
	// below carries the causes so an operator can act on it. Do not "fix"
	// it with a retry here — see the promotion race above.
	if killed, err := f.mysql.KillConnections(fenceCtx); err != nil {
		f.logger.Warn("SELF-FENCING: failed to kill connections after fencing", "error", err, "count", killed)
	} else {
		f.logger.Info("SELF-FENCING: killed app connections", "count", killed)
	}

	f.logger.Error("SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore")
}

// fenceLanded reports whether super_read_only is in place after a fence
// write that returned an error, so an ambiguous result is resolved by
// asking the server rather than by assuming.
//
// read_only is the right signal even though the write targets
// super_read_only: setting super_read_only implies read_only, and it is
// the same variable evaluate() and the rearm branch already treat as
// "this instance is fenced". A false positive is possible in principle —
// another actor could have set read_only in the same instant — but it
// costs only a fresh lease window on the next restore, whereas the false
// negative costs the wedge described at the call site.
//
// The probe gets its own context: the one that produced the error is
// spent. It stays a child of ctx so a shutting-down sidecar fails fast
// instead of spending the probe budget on its way out.
func (f *FencingMonitor) fenceLanded(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, fenceProbeTimeout)
	defer cancel()

	readOnly, err := f.mysql.IsReadOnly(probeCtx)
	if err != nil {
		f.logger.Warn("fencing: could not confirm whether the super_read_only write landed", "error", err)
		return false
	}
	return readOnly
}

// IsFenced reports whether this monitor has self-fenced and not yet
// rearmed. Safe to call from any goroutine.
func (f *FencingMonitor) IsFenced() bool {
	return f.fenced.Load()
}
