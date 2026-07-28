package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
)

// Fencer abstracts MySQL operations needed by the fencing monitor.
type Fencer interface {
	IsReadOnly(ctx context.Context) (bool, error)
	// CheckSuperReadOnly reports @@super_read_only specifically. It is not
	// redundant with IsReadOnly, which reads @@read_only: promotion clears
	// super_read_only before read_only, so the two disagree for the width
	// of that window and only this one answers "is our fence still on".
	CheckSuperReadOnly(ctx context.Context) (bool, error)
	SetSuperReadOnly(ctx context.Context) error
	KillConnections(ctx context.Context) (EvictionResult, error)
	KillSessions(ctx context.Context, ids []int64) (EvictionResult, error)
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
	fenced           bool

	// evictionPending records that a fence landed but could not evict
	// every session. Once fenced, evaluate() returns on the first
	// read-only check, so without this the surviving sessions would
	// never be retried and could stay connected to the fenced site
	// indefinitely. Retries are bounded by evictionAttempts: a session
	// that refuses to die three ticks running is not going to, and
	// re-killing forever would just be noise.
	evictionPending   bool
	evictionAttempts  int
	evictionSurvivors []int64
	logger            *slog.Logger
	httpClient        *http.Client
	clock             clock.Clock

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

// Run starts the fencing monitor loop. Blocks until ctx is cancelled.
func (f *FencingMonitor) Run(ctx context.Context) {
	// Initialize last-seen times to now (grace period on startup)
	now := f.clock.Now()
	f.lastBloodravenOK = now
	for _, addr := range f.peerAddrs {
		f.lastPeerOK[addr] = now
	}

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
	if f.fenced {
		if readOnly {
			f.retryEviction(ctx)
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
		f.fenced = false
		// The site is writable again, so the sessions the previous fence
		// failed to evict are no longer the previous fence's problem. A
		// future fence starts its own eviction from scratch.
		f.evictionPending = false
		f.evictionAttempts = 0
		f.evictionSurvivors = nil
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

// doFence performs the actual SET GLOBAL super_read_only=ON +
// KILL-app-connections step and flips the fenced flag. Separated so
// both fencing rules share the same write sequence.
func (f *FencingMonitor) doFence(ctx context.Context) {
	if err := f.mysql.SetSuperReadOnly(ctx); err != nil {
		f.logger.Error("SELF-FENCING FAILED: could not set super_read_only", "error", err)
		return
	}

	// KillConnections reports a partial eviction as (killed>0, err): the
	// sessions it could identify are gone, but it could not enumerate or
	// kill them all. Carry the count so the warning says how far the fence
	// got, and arm a retry — see retryEviction.
	res, err := f.mysql.KillConnections(ctx)
	if err != nil {
		f.logger.Warn("SELF-FENCING: failed to kill connections after fencing", "error", err, "count", res.Killed)
		// Only sessions we identified can be retried by ID. An eviction
		// that failed purely because enumeration broke leaves nothing to
		// aim at, so it is reported and dropped rather than armed as a
		// retry that could not do anything.
		f.evictionSurvivors = res.Survivors
		f.evictionPending = len(res.Survivors) > 0
		f.evictionAttempts = 0
	} else {
		f.logger.Info("SELF-FENCING: killed app connections", "count", res.Killed)
		f.evictionPending = false
		f.evictionSurvivors = nil
	}

	f.fenced = true
	f.logger.Error("SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore")
}

// maxEvictionRetries bounds how many later ticks will re-attempt an
// eviction that the fence itself could not finish.
const maxEvictionRetries = 3

// retryEviction re-attempts an incomplete eviction on a later tick.
//
// The fence's safety property — super_read_only=ON — is already held by
// the time this runs, so a surviving session cannot write. What it can
// do is keep reading from a site the operator has demoted, which is the
// stale-read the eviction exists to prevent. Once f.fenced is set,
// evaluate() returns on its first read-only check, so nothing else would
// ever revisit those sessions.
//
// Bounded on purpose. A KILL that is refused three ticks running is
// being refused for a structural reason (a privilege the sidecar does
// not hold, a session the server will not let it touch), and retrying
// forever would emit a warning every tick without changing anything.
func (f *FencingMonitor) retryEviction(ctx context.Context) {
	if !f.evictionPending {
		return
	}
	// Our fence is super_read_only=ON, and only that flag says whether it
	// still holds. Promotion clears super_read_only (failover.go step 7)
	// before read_only (step 8), so a tick landing between them still sees
	// @@read_only=1 while the fence is already lifted. The fence we were
	// finishing is gone, so there is nothing left to evict.
	//
	// This check is not what makes the retry safe against a promotion —
	// it cannot be, since promotion could clear the flag in the moment
	// between reading it and issuing the KILLs. Safety comes from the
	// retry only ever targeting evictionSurvivors, sessions the original
	// fence identified and failed to kill. The operator's promotion
	// connection, and every client it admits, are opened after the fence
	// and are therefore not in that list at any interleaving. The check
	// below just stops pointless work once the fence is lifted.
	superReadOnly, err := f.mysql.CheckSuperReadOnly(ctx)
	if err != nil {
		f.logger.Warn("fencing: could not check super_read_only before eviction retry", "error", err)
		return
	}
	if !superReadOnly {
		f.clearPendingEviction()
		f.logger.Info("SELF-FENCING: abandoning eviction retry, fence already lifted")
		return
	}
	f.evictionAttempts++
	res, err := f.mysql.KillSessions(ctx, f.evictionSurvivors)
	if err == nil {
		f.clearPendingEviction()
		f.logger.Info("SELF-FENCING: killed app connections", "count", res.Killed)
		return
	}
	// Narrow the target list to whoever is still holding on, so each
	// attempt aims at strictly fewer sessions than the last.
	f.evictionSurvivors = res.Survivors
	if f.evictionAttempts >= maxEvictionRetries || len(res.Survivors) == 0 {
		attempts := f.evictionAttempts
		f.clearPendingEviction()
		f.logger.Warn("SELF-FENCING: giving up on evicting connections after fencing",
			"error", err, "count", res.Killed, "attempts", attempts)
		return
	}
	f.logger.Warn("SELF-FENCING: failed to kill connections after fencing", "error", err, "count", res.Killed)
}

func (f *FencingMonitor) clearPendingEviction() {
	f.evictionPending = false
	f.evictionAttempts = 0
	f.evictionSurvivors = nil
}

// IsFenced returns whether the monitor has self-fenced.
func (f *FencingMonitor) IsFenced() bool {
	return f.fenced
}
