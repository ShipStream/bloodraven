package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// dcTracker tracks debounce counters and current state for one DC.
type dcTracker struct {
	name          string
	zone          string // topology.kubernetes.io/zone value
	lbIP          string
	mysql         MySQLChecker
	state         DCState
	failCount     int
	recoveryCount int
}

// Watcher is the main control loop.
type Watcher struct {
	cfg     Config
	dc1     dcTracker
	dc2     dcTracker
	tainter NodeTainter
	hub     *Hub
	dns     DNSUpdater
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

func NewWatcher(cfg Config, dc1MySQL, dc2MySQL MySQLChecker, tainter NodeTainter, hub *Hub, dns DNSUpdater, logger *slog.Logger) *Watcher {
	return &Watcher{
		cfg: cfg,
		dc1: dcTracker{
			name:  cfg.DC1.Name,
			zone:  cfg.AZ + "-" + cfg.DC1.Name,
			lbIP:  cfg.DC1.LBIP,
			mysql: dc1MySQL,
			state: StateUnknown,
		},
		dc2: dcTracker{
			name:  cfg.DC2.Name,
			zone:  cfg.AZ + "-" + cfg.DC2.Name,
			lbIP:  cfg.DC2.LBIP,
			mysql: dc2MySQL,
			state: StateUnknown,
		},
		tainter: tainter,
		hub:     hub,
		dns:     dns,
		logger:  logger,
	}
}

func (w *Watcher) Status() StatusResponse {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return StatusResponse{
		DC1:      w.dc1.name,
		DC1State: w.dc1.state.String(),
		DC2:      w.dc2.name,
		DC2State: w.dc2.state.String(),
		PollTime: w.lastPollTime.Format(time.RFC3339),
	}
}

// Ready returns true after the first successful poll cycle.
func (w *Watcher) Ready() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ready
}

// Run starts the polling loop. Blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	// Do an initial poll immediately.
	w.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

func (w *Watcher) poll(ctx context.Context) {
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
	go func() { defer wg.Done(); r1 = pollDC(&w.dc1) }()
	go func() { defer wg.Done(); r2 = pollDC(&w.dc2) }()
	wg.Wait()

	pollLatency.WithLabelValues(w.dc1.name).Observe(r1.duration.Seconds())
	pollLatency.WithLabelValues(w.dc2.name).Observe(r2.duration.Seconds())

	// Compute new debounced states.
	dc1New := w.computeState(&w.dc1, r1.readOnly, r1.err)
	dc2New := w.computeState(&w.dc2, r2.readOnly, r2.err)

	dc1Prev := w.dc1.state
	dc2Prev := w.dc2.state

	// Apply per-DC transitions.
	if dc1New != dc1Prev {
		w.logger.Info("state transition", "dc", w.dc1.name, "from", dc1Prev, "to", dc1New)
		stateTransitions.WithLabelValues(w.dc1.name, dc1Prev.String(), dc1New.String()).Inc()
		w.dc1.state = dc1New
		w.applyPerDCAction(ctx, &w.dc1, EvalPerDCTransition(dc1Prev, dc1New))
	}

	if dc2New != dc2Prev {
		w.logger.Info("state transition", "dc", w.dc2.name, "from", dc2Prev, "to", dc2New)
		stateTransitions.WithLabelValues(w.dc2.name, dc2Prev.String(), dc2New.String()).Inc()
		w.dc2.state = dc2New
		w.applyPerDCAction(ctx, &w.dc2, EvalPerDCTransition(dc2Prev, dc2New))
	}

	// Check for pending promotion confirmation: if we promoted a DC and it's
	// now writable, flip DNS.
	if w.promotedDC != "" {
		dc := w.getDC(w.promotedDC)
		if dc != nil && dc.state == StateWritable {
			w.logger.Info("promotion confirmed, flipping DNS", "dc", dc.name, "ip", dc.lbIP)
			if err := w.dns.UpdateAZRecord(ctx, dc.lbIP); err != nil {
				w.logger.Error("DNS flip failed", "dc", dc.name, "error", err)
			} else {
				dnsFlipCount.WithLabelValues(dc.name).Inc()
			}
			w.promotedDC = ""
		}
	}

	// Cross-DC evaluation (only on state transitions to avoid repeated actions).
	if dc1New != dc1Prev || dc2New != dc2Prev {
		cross := EvalCrossDC(w.dc1.state, w.dc2.state, dc1Prev, dc2Prev, w.dc1.name, w.dc2.name)
		w.applyCrossDCAction(ctx, cross)
	}

	// Update status.
	w.mu.Lock()
	w.lastPollTime = time.Now()
	w.ready = true
	w.mu.Unlock()

	wsClientCount.Set(float64(w.hub.ClientCount()))
}

// computeState applies debounce logic and returns the new state for a DC.
func (w *Watcher) computeState(dc *dcTracker, readOnly bool, err error) DCState {
	if err != nil {
		dc.recoveryCount = 0
		dc.failCount++
		if dc.failCount >= w.cfg.FailureThreshold {
			return StateUnreachable
		}
		return dc.state // not enough failures yet, keep current state
	}

	// Successful poll.
	dc.failCount = 0

	if readOnly {
		dc.recoveryCount = 0
		return StateReadOnly
	}

	// read_only=0 (writable)
	if dc.state != StateWritable {
		dc.recoveryCount++
		if dc.recoveryCount >= w.cfg.RecoveryThreshold {
			return StateWritable
		}
		return dc.state // not enough recoveries yet
	}

	return StateWritable
}

func (w *Watcher) applyPerDCAction(ctx context.Context, dc *dcTracker, action Action) {
	if action.Taint != nil {
		if err := w.tainter.SetTaint(ctx, dc.zone, *action.Taint); err != nil {
			w.logger.Error("taint operation failed", "dc", dc.name, "apply", *action.Taint, "error", err)
		} else {
			op := "remove"
			if *action.Taint {
				op = "apply"
			}
			taintOperations.WithLabelValues(dc.name, op).Inc()
		}
	}

	if action.Broadcast != "" {
		w.hub.Broadcast(WSMessage{DC: dc.name, Status: action.Broadcast})
	}
}

func (w *Watcher) applyCrossDCAction(ctx context.Context, action CrossDCAction) {
	if action.Alert != "" {
		w.logger.Warn("ALERT", "message", action.Alert)
	}

	if action.PromoteDC != "" && w.promotedDC == "" {
		dc := w.getDC(action.PromoteDC)
		if dc != nil {
			w.logger.Info("promoting DC", "dc", dc.name)
			if err := dc.mysql.Promote(ctx); err != nil {
				w.logger.Error("promotion failed", "dc", dc.name, "error", err)
				return
			}
			// DNS flip deferred until next poll confirms read_only=0.
			w.promotedDC = dc.name
		}
	}
}

func (w *Watcher) getDC(name string) *dcTracker {
	if name == w.dc1.name {
		return &w.dc1
	}
	if name == w.dc2.name {
		return &w.dc2
	}
	return nil
}
