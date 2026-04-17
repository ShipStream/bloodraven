package sidecar

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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
// fences (sets super_read_only=ON) when the primary is truly isolated
// from both the operator and every peer.
//
// Multi-site semantics: the monitor tracks a per-peer "last-seen"
// timestamp. The site is considered to have peer connectivity as long
// as *any* peer answered within the lease timeout. Self-fencing fires
// only when the operator is unreachable beyond the lease AND every
// peer is unreachable beyond the lease.
type FencingMonitor struct {
	mysql            Fencer
	bloodravenAddr   string
	peerAddrs        []string
	checkInterval    time.Duration
	leaseTimeout     time.Duration
	lastBloodravenOK time.Time
	lastPeerOK       map[string]time.Time
	fenced           bool
	logger           *slog.Logger
	httpClient       *http.Client
	clock            clock.Clock
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
	for _, addr := range f.peerAddrs {
		f.checkPeer(ctx, addr)
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
	// Already fenced, nothing more to do.
	if f.fenced {
		return
	}

	// Only the primary should self-fence. If MySQL is already read-only, skip.
	readOnly, err := f.mysql.IsReadOnly(ctx)
	if err != nil {
		f.logger.Warn("fencing: could not check read_only status", "error", err)
		return
	}
	if readOnly {
		return
	}

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
		"bloodraven_last_ok", f.lastBloodravenOK,
		"latest_peer_ok", f.latestPeerSeen(),
		"peers", f.peerAddrs,
		"lease_timeout", f.leaseTimeout,
	)

	if err := f.mysql.SetSuperReadOnly(ctx); err != nil {
		f.logger.Error("SELF-FENCING FAILED: could not set super_read_only", "error", err)
		return
	}

	if killed, err := f.mysql.KillConnections(ctx); err != nil {
		f.logger.Warn("SELF-FENCING: failed to kill connections after fencing", "error", err)
	} else {
		f.logger.Info("SELF-FENCING: killed app connections", "count", killed)
	}

	f.fenced = true
	f.logger.Error("SELF-FENCED: super_read_only=ON has been set, only Bloodraven can restore")
}

// IsFenced returns whether the monitor has self-fenced.
func (f *FencingMonitor) IsFenced() bool {
	return f.fenced
}
