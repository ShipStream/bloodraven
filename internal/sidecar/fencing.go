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
// cloneDetector is implemented by Fencers that can tell whether a CLONE
// statement is executing on the server.
type cloneDetector interface {
	CloneInProgress(ctx context.Context) (bool, error)
}

type Fencer interface {
	IsReadOnly(ctx context.Context) (bool, error)
	IsSuperReadOnly(ctx context.Context) (bool, error)
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
//
// With WithSafetyNet, the monitor also finishes the startup safety net
// when startup could not, and re-applies it after a long MySQL outage
// (see resolveSafetyNet). Every MySQL call and every HTTP request in a
// tick is bounded, so a hung mysqld costs the loop a few seconds per tick
// and never stops it.
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
	// than a plain bool. fencePending is monitor-goroutine-only: it records
	// an ambiguous SET whose confirmation probe also failed, so a later tick
	// can resolve the outcome without attributing an older process's fence
	// to this monitor.
	fenced       atomic.Bool
	fencePending bool
	logger       *slog.Logger
	httpClient   *http.Client
	clock        clock.Clock

	// Topology-aware fields. Populated by WithTopology; zero values
	// disable rule #1 above. mySite/namespace/group are the identity
	// this sidecar uses when hitting the operator's /active-site
	// endpoint and when comparing against the cached authoritative
	// answer.
	mySite    string
	namespace string
	group     string
	topology  *TopologyCache

	// operatorAnswered and operatorActiveSite record what the operator
	// itself, not a peer relay, said on this tick's /active-site poll. Check
	// resets them each tick. The deferred safety net trusts only this
	// direct answer, as RunSafetyNet does at startup.
	operatorAnswered   bool
	operatorActiveSite string

	// Deferred safety net, populated by WithSafetyNet and owned by the
	// monitor goroutine. safetyNetPending means the startup guarantee
	// (super-fenced unless the operator confirms this site is active) is
	// not established yet: RunSafetyNet could not reach MySQL, or MySQL
	// has since been unreachable long enough to have restarted.
	// mysqlDownSince is when the current run of failed MySQL probes began,
	// and is zero while MySQL answers.
	safetyNetClearer SuperReadOnlyClearer
	safetyNetPending bool
	mysqlDownSince   time.Time

	// sawReadOnly is true when the latest MySQL probe answered read_only=ON
	// and no probe has failed since. A writable answer after it is an
	// operator promotion (only an actor with SUPER clears read_only), which
	// evaluate treats like the restore after a self-fence. A failed probe
	// clears it: mysqld may have restarted, and a restart is not a
	// promotion, even when it comes back writable (a pod that predates the
	// read_only=ON boot flag). Monitor goroutine only.
	sawReadOnly bool

	// heartbeat is the f.clock reading, in Unix nanoseconds, of the loop's
	// latest progress: Run's start and the start and end of every Check.
	// It is zero until the loop starts, and atomic because /livez reads it.
	heartbeat atomic.Int64
}

// SuperReadOnlyClearer is the one write the deferred safety net needs
// beyond Fencer: restoring writability when the operator confirms this
// site is active, as RunSafetyNet's last step does.
type SuperReadOnlyClearer interface {
	ClearSuperReadOnly(ctx context.Context) error
}

const (
	// mysqlProbeTimeout bounds each read the monitor issues against local
	// MySQL. A hung mysqld (SIGSTOP, long crash recovery, a CLONE restart)
	// costs one bounded probe per tick, never the loop.
	mysqlProbeTimeout = 5 * time.Second

	// httpRequestTimeout bounds each operator and peer request on the
	// monitor goroutine, whatever http.Client was injected.
	httpRequestTimeout = 2 * time.Second

	// evaluateTimeout caps all MySQL work in one tick: the read_only
	// probe, one super_read_only probe, and one fence or clear with its
	// confirming read. Each step has its own shorter bound too; the cap
	// keeps a tick bounded if a later change adds a step.
	evaluateTimeout = 2*mysqlProbeTimeout + fenceTimeout + fenceProbeTimeout

	// safetyNetOutageRearm is how long MySQL must stay unreachable before
	// the monitor re-arms the startup safety net. A mysqld gone this long
	// may have restarted. It boots read_only=ON, which stops application
	// users but not SUPER clients, and a pod that predates that flag boots
	// writable; the safety net adds super_read_only unless the operator
	// confirms this site is active. While sidecar liveness probed MySQL, an
	// outage this long (three failed 10s probes) restarted the sidecar and
	// re-ran the safety net as a side effect. The re-arm keeps that
	// protection without the restart, and without the gap in which no
	// fencing agent runs.
	safetyNetOutageRearm = 30 * time.Second
)

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

// topologyMismatch reports whether the cached authoritative view names an
// active site other than this one (rule #1).
func (f *FencingMonitor) topologyMismatch() (TopologySnapshot, bool) {
	if !f.topologyEnabled() {
		return TopologySnapshot{}, false
	}
	snap := f.topology.Snapshot()
	return snap, snap.ActiveSite != "" && snap.ActiveSite != f.mySite
}

// WithSafetyNet enables the deferred startup safety net. Call once after
// WithTopology and before Run. Returns the receiver for chaining.
//
// With it enabled, the monitor keeps RunSafetyNet's guarantee past the
// point where startup could establish it: MySQL stays super-fenced unless
// the operator confirms this site is active. ArmSafetyNet hands over a
// startup check that did not settle, and an outage of
// safetyNetOutageRearm or longer re-arms the check. It needs the same
// identity and operator address RunSafetyNet does, and stays disabled
// without them.
func (f *FencingMonitor) WithSafetyNet(clearer SuperReadOnlyClearer) *FencingMonitor {
	f.safetyNetClearer = clearer
	return f
}

func (f *FencingMonitor) safetyNetEnabled() bool {
	return f.safetyNetClearer != nil && f.bloodravenAddr != "" && f.topologyEnabled()
}

// ArmSafetyNet records that the startup safety net did not settle (the
// initial fence or the active-site clear was not confirmed), so the monitor
// finishes it once MySQL answers. Call before Run, from the goroutine that
// starts Run. It is a no-op while the safety net is disabled.
func (f *FencingMonitor) ArmSafetyNet() {
	if f.safetyNetEnabled() {
		f.safetyNetPending = true
	}
}

// SafetyNetPending reports whether the deferred safety net is armed. It is
// not safe to call concurrently with Run.
func (f *FencingMonitor) SafetyNetPending() bool { return f.safetyNetPending }

func (f *FencingMonitor) beat() { f.heartbeat.Store(f.clock.Now().UnixNano()) }

// StallThreshold is how long the loop may go without progress before
// /livez reports it wedged. Every request and every MySQL call in a tick
// has its own timeout, so a healthy loop makes progress well inside this
// window even against a hung mysqld and unreachable peers. Exceeding it
// means a call ignored its context, and only a restart recovers from that.
func (f *FencingMonitor) StallThreshold() time.Duration {
	requests := 2 + 2*len(f.peerAddrs)
	maxCheck := time.Duration(requests)*httpRequestTimeout + evaluateTimeout
	return 2 * (f.checkInterval + maxCheck)
}

// LoopHealth reports how long ago the loop last made progress, and whether
// it has started. Safe to call from any goroutine.
func (f *FencingMonitor) LoopHealth() (sinceProgress time.Duration, started bool) {
	ns := f.heartbeat.Load()
	if ns == 0 {
		return 0, false
	}
	return f.clock.Now().Sub(time.Unix(0, ns)), true
}

// Stalled reports whether the loop started and then went longer than
// StallThreshold without progress. Safe to call from any goroutine.
func (f *FencingMonitor) Stalled() bool {
	since, started := f.LoopHealth()
	return started && since > f.StallThreshold()
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
	f.beat()

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
	f.beat()
	defer f.beat()

	f.operatorAnswered = false
	f.operatorActiveSite = ""
	f.checkBloodraven(ctx)
	if f.topologyEnabled() {
		f.checkActiveSite(ctx)
	}
	for _, addr := range f.peerAddrs {
		f.checkPeer(ctx, addr)
		// A peer's view is a relay of the operator's, so it only fills in
		// when the operator did not answer this tick.
		if f.topologyEnabled() && !f.operatorAnswered {
			f.checkPeerTopology(ctx, addr)
		}
	}
	f.evaluate(ctx)
}

func (f *FencingMonitor) checkBloodraven(ctx context.Context) {
	if f.bloodravenAddr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, httpRequestTimeout)
	defer cancel()
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

	ctx, cancel := context.WithTimeout(ctx, httpRequestTimeout)
	defer cancel()
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

	ctx, cancel := context.WithTimeout(ctx, httpRequestTimeout)
	defer cancel()
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
	f.operatorAnswered = true
	f.operatorActiveSite = body.ActiveSite
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
// than our own, so a stale peer never drags us backwards. Check skips
// it in ticks where the operator answered.
func (f *FencingMonitor) checkPeerTopology(ctx context.Context, addr string) {
	if addr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, httpRequestTimeout)
	defer cancel()
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
	// observedAt is the peer's clock. A view from our future (clock skew
	// or a forged response) would otherwise outrank every later view and
	// pin the cache, so it counts as observed now.
	if now := f.clock.Now(); snap.ObservedAt.After(now) {
		snap.ObservedAt = now
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
	ctx, cancel := context.WithTimeout(ctx, evaluateTimeout)
	defer cancel()

	// Only the primary should self-fence. If MySQL is already read-only, skip.
	readOnly, err := f.probeReadOnly(ctx)
	if err != nil {
		f.noteMySQLUnreachable()
		f.logger.Warn("fencing: could not check read_only status", "error", err)
		return
	}
	f.noteMySQLReachable()
	promoted := f.sawReadOnly && !readOnly
	f.sawReadOnly = readOnly
	if f.safetyNetPending && f.resolveSafetyNet(ctx, readOnly) {
		return
	}
	if f.fencePending {
		superReadOnly, probeErr := f.probeSuperReadOnly(ctx)
		if probeErr != nil {
			f.logger.Warn("fencing: could not resolve pending super_read_only write", "error", probeErr)
			return
		}
		f.fencePending = false
		if superReadOnly {
			f.fenced.Store(true)
			f.logger.Warn("SELF-FENCING: previously unconfirmed super_read_only write is now confirmed")
			f.logger.Error("SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore")
			return
		}
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
		f.rearm()
	} else if promoted {
		// The same reasoning holds for a promotion this monitor did not
		// fence before. Every active-site view it holds predates the
		// promotion and may still name the old primary: the operator
		// answered that until it promoted this site. If the operator then
		// becomes unreachable before the next tick (a restart right after
		// a failover), rule #1 would fence the new primary on that view,
		// and with the old primary down nothing promotes it again. A live
		// operator's next answer is newer than the reset, so a mistaken
		// manual unfence is still fenced on the next tick.
		f.logger.Info("fencing: MySQL turned writable; discarding active-site view from before the promotion")
		f.rearm()
	}
	if readOnly {
		return
	}

	// A running CLONE is the operator replacing this site's data; the site
	// restarts afterwards and the safety net fences it then. Fencing now
	// would kill the clone after it has dropped the data.
	if cd, ok := f.mysql.(cloneDetector); ok {
		if cloning, err := cd.CloneInProgress(ctx); err == nil && cloning {
			f.logger.Info("fencing: clone in progress on this site; not fencing")
			return
		}
	}

	// Rule #1 — topology mismatch. If the cached authoritative
	// active site is known and disagrees with our site, fence
	// immediately. This catches the "stale primary returns from a
	// partition to find the operator has failed over to the peer"
	// scenario even when the peer is reachable (peer reachability
	// alone would otherwise keep rule #2 quiet).
	if snap, mismatch := f.topologyMismatch(); mismatch {
		f.logger.Error("SELF-FENCING: topology mismatch — operator-authoritative active site disagrees with our site, setting super_read_only=ON",
			"site", f.mySite,
			"authoritativeActiveSite", snap.ActiveSite,
			"observedAt", snap.ObservedAt,
		)
		f.doFence(ctx)
		return
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

// rearm grants a full lease window and drops the cached active-site view,
// as at process startup. Called when an actor with SUPER made MySQL
// writable: that is fresh evidence of operator intervention, and every
// view held from before it is stale. Peer views older than the reset are
// not adopted afterwards (TopologyCache.Adopt needs a newer observation).
func (f *FencingMonitor) rearm() {
	now := f.clock.Now()
	f.lastBloodravenOK = now
	for addr := range f.lastPeerOK {
		f.lastPeerOK[addr] = now
	}
	if f.topologyEnabled() {
		f.topology.Set("", now)
	}
}

// probeReadOnly reads @@read_only within mysqlProbeTimeout.
func (f *FencingMonitor) probeReadOnly(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, mysqlProbeTimeout)
	defer cancel()
	return f.mysql.IsReadOnly(ctx)
}

// probeSuperReadOnly reads @@super_read_only within mysqlProbeTimeout.
func (f *FencingMonitor) probeSuperReadOnly(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, mysqlProbeTimeout)
	defer cancel()
	return f.mysql.IsSuperReadOnly(ctx)
}

// noteMySQLUnreachable records the start of a run of failed MySQL probes.
func (f *FencingMonitor) noteMySQLUnreachable() {
	f.sawReadOnly = false
	if f.mysqlDownSince.IsZero() {
		f.mysqlDownSince = f.clock.Now()
	}
}

// noteMySQLReachable ends a run of failed MySQL probes. If the run lasted
// safetyNetOutageRearm or longer, it re-arms the safety net, because the
// mysqld now answering may be a restarted one that came up writable.
//
// A pending ambiguous fence write is left to its own resolution path: it
// already decides whether this monitor's fence holds.
func (f *FencingMonitor) noteMySQLReachable() {
	if f.mysqlDownSince.IsZero() {
		return
	}
	down := f.clock.Now().Sub(f.mysqlDownSince)
	f.mysqlDownSince = time.Time{}
	if down < safetyNetOutageRearm || !f.safetyNetEnabled() || f.safetyNetPending || f.fencePending {
		return
	}
	f.safetyNetPending = true
	f.logger.Warn("safety net: MySQL was unreachable long enough to have restarted; re-arming",
		"unreachableFor", down, "threshold", safetyNetOutageRearm)
}

// resolveSafetyNet finishes an armed safety net on a tick where MySQL
// answered. It applies RunSafetyNet's rule to the state MySQL is in now:
// super-fenced, unless the operator itself confirms on this tick that this
// site is active. A peer-relayed view is not enough, the same as at
// startup. It returns true when it handled the tick and false to hand the
// tick to the normal rules.
//
// While armed, the safety net takes precedence over the lease rule, since
// it fences in every case the lease rule would. The exception is a
// writable instance whose authoritative view names another active site:
// that is rule #1, which also evicts sessions and records the self-fence.
// The tick goes to rule #1 rather than fencing twice.
//
// The fence here does not evict sessions or set self_fenced, the same as
// RunSafetyNet. Eviction on a site the operator has not ruled on could kill
// an in-flight promotion session (see doFence).
//
// The safety net never clears a fence this monitor set itself. Once a
// self-fence is in place, only the operator lifts it.
func (f *FencingMonitor) resolveSafetyNet(ctx context.Context, readOnly bool) bool {
	superReadOnly, err := f.probeSuperReadOnly(ctx)
	if err != nil {
		f.logger.Warn("safety net: could not read super_read_only; will retry", "error", err)
		return true
	}

	if f.operatorAnswered && f.operatorActiveSite == f.mySite {
		switch {
		case !readOnly && !superReadOnly:
			f.logger.Info("safety net: operator confirms this is the active site; MySQL is already writable",
				"site", f.mySite)
		case f.fenced.Load():
			f.logger.Info("safety net: operator confirms this is the active site; leaving the self-fence for the operator to clear",
				"site", f.mySite)
		default:
			f.logger.Info("safety net: this is the active site, clearing super_read_only", "site", f.mySite)
			clearCtx, cancel := context.WithTimeout(ctx, fenceTimeout)
			defer cancel()
			if err := f.safetyNetClearer.ClearSuperReadOnly(clearCtx); err != nil {
				f.logger.Error("safety net: failed to clear super_read_only on active site", "error", err)
				return true
			}
		}
		f.safetyNetPending = false
		return true
	}

	if superReadOnly {
		f.safetyNetPending = false
		f.logger.Info("safety net: super_read_only=ON confirmed; staying fenced",
			"site", f.mySite, "activeSite", f.operatorActiveSite, "operatorAnswered", f.operatorAnswered)
		return true
	}

	if !readOnly && !f.fenced.Load() {
		if _, mismatch := f.topologyMismatch(); mismatch {
			f.safetyNetPending = false
			return false
		}
	}

	fenceCtx, cancel := context.WithTimeout(ctx, fenceTimeout)
	defer cancel()
	if err := f.mysql.SetSuperReadOnly(fenceCtx); err != nil {
		// As in doFence, an error means the outcome is unknown, so ask the
		// server. Anything short of a confirmed fence stays armed and is
		// retried on the next tick.
		landed, probeErr := f.fenceLanded(ctx)
		if probeErr != nil || !landed {
			f.logger.Error("safety net: could not set super_read_only; will retry", "error", err, "probeError", probeErr)
			return true
		}
	}
	f.safetyNetPending = false
	f.logger.Info("safety net: set super_read_only=ON after MySQL became reachable",
		"site", f.mySite, "activeSite", f.operatorActiveSite, "operatorAnswered", f.operatorAnswered)
	return true
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
		// Resolve the ambiguity with a fresh super_read_only read. If that
		// probe is unavailable, remember this process's pending write and
		// retry the confirmation on the next monitor tick. Without that
		// marker, evaluate would see read_only=ON and return forever with
		// self_fenced=false, so a later operator promotion could be
		// immediately re-fenced against stale lease timestamps.
		landed, probeErr := f.fenceLanded(ctx)
		if probeErr != nil {
			f.fencePending = true
			f.logger.Error("SELF-FENCING: super_read_only write outcome is unconfirmed; will probe again", "error", err, "probeError", probeErr)
			return
		}
		if !landed {
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
	// operator's job, where it can be sequenced after promotion and before
	// old-primary recovery is marked complete. Planned failover has its own
	// Draining phase; emergency failover and autonomous self-fence recovery
	// use spec.connectionDrainTimeout. Do not retry from the sidecar — only
	// the operator can prove that no promotion session is in flight.
	if killed, err := f.mysql.KillConnections(fenceCtx); err != nil {
		f.logger.Warn("SELF-FENCING: failed to kill connections after fencing", "error", err, "count", killed)
	} else {
		f.logger.Info("SELF-FENCING: killed app connections", "count", killed)
	}

	f.logger.Error("SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore")
}

// fenceLanded reports whether super_read_only is in place after a fence
// write that returned an error, so an ambiguous result is resolved by
// asking the server rather than by assuming. It returns the probe error
// separately so the caller can retry an unknown outcome on a later tick.
//
// The probe must read super_read_only itself. read_only is insufficient:
// another actor may set read_only without establishing the super fence,
// which must not be mistaken for this monitor's successful write.
//
// The probe gets its own context: the one that produced the error is
// spent. It stays a child of ctx so a shutting-down sidecar fails fast
// instead of spending the probe budget on its way out.
func (f *FencingMonitor) fenceLanded(ctx context.Context) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, fenceProbeTimeout)
	defer cancel()

	superReadOnly, err := f.mysql.IsSuperReadOnly(probeCtx)
	if err != nil {
		f.logger.Warn("fencing: could not confirm whether the super_read_only write landed", "error", err)
		return false, err
	}
	return superReadOnly, nil
}

// IsFenced reports whether this monitor has self-fenced and not yet
// rearmed. Safe to call from any goroutine.
func (f *FencingMonitor) IsFenced() bool {
	return f.fenced.Load()
}
