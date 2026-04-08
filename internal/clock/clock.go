// Package clock provides a time abstraction for deterministic testing.
//
// Production code uses RealClock. Tests inject FakeClock to eliminate
// wall-clock dependencies and remove the need for time.Sleep.
package clock

import (
	"sync"
	"time"
)

// Clock abstracts time operations used by topology manager and fencing monitor.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTicker(d time.Duration) Ticker
}

// Ticker abstracts time.Ticker so tests can control tick delivery.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// ---------------------------------------------------------------------------
// RealClock — production implementation
// ---------------------------------------------------------------------------

// RealClock delegates to the standard time package.
type RealClock struct{}

func (RealClock) Now() time.Time                   { return time.Now() }
func (RealClock) Since(t time.Time) time.Duration  { return time.Since(t) }
func (RealClock) NewTicker(d time.Duration) Ticker { return &realTicker{t: time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

// ---------------------------------------------------------------------------
// FakeClock — deterministic test implementation
// ---------------------------------------------------------------------------

// FakeClock is a manually-controlled clock for tests.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*FakeTicker
}

// NewFakeClock creates a FakeClock starting at the given time.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *FakeClock) Since(t time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now.Sub(t)
}

// Advance moves the clock forward by d and fires any tickers whose interval
// has elapsed. This is the primary test-driving method.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now
	tickers := make([]*FakeTicker, len(f.tickers))
	copy(tickers, f.tickers)
	f.mu.Unlock()

	for _, ft := range tickers {
		ft.maybeFire(now)
	}
}

// Set moves the clock to an absolute time and fires tickers.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	f.now = t
	now := f.now
	tickers := make([]*FakeTicker, len(f.tickers))
	copy(tickers, f.tickers)
	f.mu.Unlock()

	for _, ft := range tickers {
		ft.maybeFire(now)
	}
}

func (f *FakeClock) NewTicker(d time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	ft := &FakeTicker{
		ch:       make(chan time.Time, 1),
		interval: d,
		nextFire: f.now.Add(d),
	}
	f.tickers = append(f.tickers, ft)
	return ft
}

// FakeTicker is a ticker driven by FakeClock.Advance.
type FakeTicker struct {
	mu       sync.Mutex
	ch       chan time.Time
	interval time.Duration
	nextFire time.Time
	stopped  bool
}

func (ft *FakeTicker) C() <-chan time.Time { return ft.ch }

func (ft *FakeTicker) Stop() {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.stopped = true
}

func (ft *FakeTicker) maybeFire(now time.Time) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if ft.stopped {
		return
	}
	for !now.Before(ft.nextFire) {
		select {
		case ft.ch <- ft.nextFire:
		default:
		}
		ft.nextFire = ft.nextFire.Add(ft.interval)
	}
}
