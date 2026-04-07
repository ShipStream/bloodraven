package controller

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/config"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// dcTracker tracks debounce counters and current state for one DC.
type dcTracker struct {
	name          string
	zone          string // topology.kubernetes.io/zone value
	lbIP          string
	mysql         mysql.Checker
	state         state.DCState
	failCount     int
	recoveryCount int
}

// TopologyManager is the main control loop.
type TopologyManager struct {
	cfg     config.Config
	dc1     dcTracker
	dc2     dcTracker
	tainter platform.NodeTainter
	hub     *platform.Hub
	dns     platform.DNSUpdater
	logger  *slog.Logger

	// Promotion state: tracks which DC was promoted and is pending DNS flip.
	promotedDC string // empty = no pending promotion

	mu           sync.RWMutex
	lastPollTime time.Time
	ready        bool
}

// StatusResponse is returned by the /status endpoint.
type StatusResponse struct {
	DC1      string `json:"dc1"`
	DC1State string `json:"dc1_state"`
	DC2      string `json:"dc2"`
	DC2State string `json:"dc2_state"`
	PollTime string `json:"poll_time"`
}

func NewTopologyManager(cfg config.Config, dc1MySQL, dc2MySQL mysql.Checker, tainter platform.NodeTainter, hub *platform.Hub, dns platform.DNSUpdater, logger *slog.Logger) *TopologyManager {
	return &TopologyManager{
		cfg: cfg,
		dc1: dcTracker{
			name:  cfg.DC1.Name,
			zone:  cfg.AZ + "-" + cfg.DC1.Name,
			lbIP:  cfg.DC1.LBIP,
			mysql: dc1MySQL,
			state: state.StateUnknown,
		},
		dc2: dcTracker{
			name:  cfg.DC2.Name,
			zone:  cfg.AZ + "-" + cfg.DC2.Name,
			lbIP:  cfg.DC2.LBIP,
			mysql: dc2MySQL,
			state: state.StateUnknown,
		},
		tainter: tainter,
		hub:     hub,
		dns:     dns,
		logger:  logger,
	}
}

func (tm *TopologyManager) Status() StatusResponse {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return StatusResponse{
		DC1:      tm.dc1.name,
		DC1State: tm.dc1.state.String(),
		DC2:      tm.dc2.name,
		DC2State: tm.dc2.state.String(),
		PollTime: tm.lastPollTime.Format(time.RFC3339),
	}
}

// Ready returns true after the first successful poll cycle.
func (tm *TopologyManager) Ready() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.ready
}

// Run starts the polling loop. Blocks until ctx is cancelled.
func (tm *TopologyManager) Run(ctx context.Context) {
	ticker := time.NewTicker(tm.cfg.PollInterval)
	defer ticker.Stop()

	// Do an initial poll immediately.
	tm.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tm.poll(ctx)
		}
	}
}

func (tm *TopologyManager) poll(ctx context.Context) {
	// Poll both DCs in parallel.
	type pollResult struct {
		readOnly bool
		err      error
		duration time.Duration
	}

	pollDC := func(dc *dcTracker) pollResult {
		start := time.Now()
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ro, err := dc.mysql.CheckReadOnly(pollCtx)
		return pollResult{readOnly: ro, err: err, duration: time.Since(start)}
	}

	var r1, r2 pollResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r1 = pollDC(&tm.dc1) }()
	go func() { defer wg.Done(); r2 = pollDC(&tm.dc2) }()
	wg.Wait()

	metrics.PollLatency.WithLabelValues(tm.dc1.name).Observe(r1.duration.Seconds())
	metrics.PollLatency.WithLabelValues(tm.dc2.name).Observe(r2.duration.Seconds())

	// Compute new debounced states.
	dc1New := tm.computeState(&tm.dc1, r1.readOnly, r1.err)
	dc2New := tm.computeState(&tm.dc2, r2.readOnly, r2.err)

	dc1Prev := tm.dc1.state
	dc2Prev := tm.dc2.state

	// Apply per-DC transitions.
	if dc1New != dc1Prev {
		tm.logger.Info("state transition", "dc", tm.dc1.name, "from", dc1Prev, "to", dc1New)
		metrics.StateTransitions.WithLabelValues(tm.dc1.name, dc1Prev.String(), dc1New.String()).Inc()
		tm.dc1.state = dc1New
		tm.applyPerDCAction(ctx, &tm.dc1, state.EvalPerDCTransition(dc1Prev, dc1New))
	}

	if dc2New != dc2Prev {
		tm.logger.Info("state transition", "dc", tm.dc2.name, "from", dc2Prev, "to", dc2New)
		metrics.StateTransitions.WithLabelValues(tm.dc2.name, dc2Prev.String(), dc2New.String()).Inc()
		tm.dc2.state = dc2New
		tm.applyPerDCAction(ctx, &tm.dc2, state.EvalPerDCTransition(dc2Prev, dc2New))
	}

	// Check for pending promotion confirmation: if we promoted a DC and it's
	// now writable, flip DNS.
	if tm.promotedDC != "" {
		dc := tm.getDC(tm.promotedDC)
		if dc != nil && dc.state == state.StateWritable {
			tm.logger.Info("promotion confirmed, flipping DNS", "dc", dc.name, "ip", dc.lbIP)
			if err := tm.dns.UpdateAZRecord(ctx, dc.lbIP); err != nil {
				tm.logger.Error("DNS flip failed", "dc", dc.name, "error", err)
			} else {
				metrics.DNSFlipCount.WithLabelValues(dc.name).Inc()
			}
			tm.promotedDC = ""
		}
	}

	// Cross-DC evaluation (only on state transitions to avoid repeated actions).
	if dc1New != dc1Prev || dc2New != dc2Prev {
		cross := state.EvalCrossDC(tm.dc1.state, tm.dc2.state, dc1Prev, dc2Prev, tm.dc1.name, tm.dc2.name)
		tm.applyCrossDCAction(ctx, cross)
	}

	// Update status.
	tm.mu.Lock()
	tm.lastPollTime = time.Now()
	tm.ready = true
	tm.mu.Unlock()

	metrics.WSClientCount.Set(float64(tm.hub.ClientCount()))
}

// computeState applies debounce logic and returns the new state for a DC.
func (tm *TopologyManager) computeState(dc *dcTracker, readOnly bool, err error) state.DCState {
	if err != nil {
		dc.recoveryCount = 0
		dc.failCount++
		if dc.failCount >= tm.cfg.FailureThreshold {
			return state.StateUnreachable
		}
		return dc.state // not enough failures yet, keep current state
	}

	// Successful poll.
	dc.failCount = 0

	if readOnly {
		dc.recoveryCount = 0
		return state.StateReadOnly
	}

	// read_only=0 (writable)
	if dc.state != state.StateWritable {
		dc.recoveryCount++
		if dc.recoveryCount >= tm.cfg.RecoveryThreshold {
			return state.StateWritable
		}
		return dc.state // not enough recoveries yet
	}

	return state.StateWritable
}

func (tm *TopologyManager) applyPerDCAction(ctx context.Context, dc *dcTracker, action state.Action) {
	if action.Taint != nil {
		if err := tm.tainter.SetTaint(ctx, dc.zone, *action.Taint); err != nil {
			tm.logger.Error("taint operation failed", "dc", dc.name, "apply", *action.Taint, "error", err)
		} else {
			op := "remove"
			if *action.Taint {
				op = "apply"
			}
			metrics.TaintOperations.WithLabelValues(dc.name, op).Inc()
		}
	}

	if action.Broadcast != "" {
		tm.hub.Broadcast(platform.WSMessage{DC: dc.name, Status: action.Broadcast})
	}
}

func (tm *TopologyManager) applyCrossDCAction(ctx context.Context, action state.CrossDCAction) {
	if action.Alert != "" {
		tm.logger.Warn("ALERT", "message", action.Alert)
	}

	if action.PromoteDC != "" && tm.promotedDC == "" {
		dc := tm.getDC(action.PromoteDC)
		if dc != nil {
			tm.logger.Info("promoting DC", "dc", dc.name)
			if err := dc.mysql.Promote(ctx); err != nil {
				tm.logger.Error("promotion failed", "dc", dc.name, "error", err)
				return
			}
			// DNS flip deferred until next poll confirms read_only=0.
			tm.promotedDC = dc.name
		}
	}
}

func (tm *TopologyManager) getDC(name string) *dcTracker {
	if name == tm.dc1.name {
		return &tm.dc1
	}
	if name == tm.dc2.name {
		return &tm.dc2
	}
	return nil
}
