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
	NewTimer(d time.Duration) Timer
}

// Ticker abstracts time.Ticker so tests can control tick delivery.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Timer abstracts time.Timer so tests can control one-shot delays.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// ---------------------------------------------------------------------------
// RealClock — production implementation
// ---------------------------------------------------------------------------

// RealClock delegates to the standard time package.
type RealClock struct{}

func (RealClock) Now() time.Time                   { return time.Now() }
func (RealClock) Since(t time.Time) time.Duration  { return time.Since(t) }
func (RealClock) NewTicker(d time.Duration) Ticker { return &realTicker{t: time.NewTicker(d)} }
func (RealClock) NewTimer(d time.Duration) Timer   { return &realTimer{t: time.NewTimer(d)} }

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }

// ---------------------------------------------------------------------------
// FakeClock — deterministic test implementation
// ---------------------------------------------------------------------------

// FakeClock is a manually-controlled clock for tests.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*FakeTicker
	timers  []*FakeTimer
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

// Advance moves the clock forward by d and fires any tickers/timers whose
// interval has elapsed. This is the primary test-driving method.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now
	tickers := make([]*FakeTicker, len(f.tickers))
	copy(tickers, f.tickers)
	timers := make([]*FakeTimer, len(f.timers))
	copy(timers, f.timers)
	f.mu.Unlock()

	for _, ft := range tickers {
		ft.maybeFire(now)
	}
	for _, ft := range timers {
		ft.maybeFire(now)
	}

	f.pruneTimers()
}

// Set moves the clock to an absolute time and fires tickers/timers.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	f.now = t
	now := f.now
	tickers := make([]*FakeTicker, len(f.tickers))
	copy(tickers, f.tickers)
	timers := make([]*FakeTimer, len(f.timers))
	copy(timers, f.timers)
	f.mu.Unlock()

	for _, ft := range tickers {
		ft.maybeFire(now)
	}
	for _, ft := range timers {
		ft.maybeFire(now)
	}

	f.pruneTimers()
}

// pruneTimers removes fired or stopped timers from the slice.
func (f *FakeClock) pruneTimers() {
	f.mu.Lock()
	defer f.mu.Unlock()
	active := f.timers[:0]
	for _, ft := range f.timers {
		ft.mu.Lock()
		inactive := ft.fired || ft.stopped
		ft.mu.Unlock()
		if !inactive {
			active = append(active, ft)
		}
	}
	f.timers = active
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

func (f *FakeClock) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	ft := &FakeTimer{
		ch:   make(chan time.Time, 1),
		fire: f.now.Add(d),
	}
	f.timers = append(f.timers, ft)
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

// FakeTimer is a one-shot timer driven by FakeClock.Advance.
type FakeTimer struct {
	mu      sync.Mutex
	ch      chan time.Time
	fire    time.Time
	stopped bool
	fired   bool
}

func (ft *FakeTimer) C() <-chan time.Time { return ft.ch }

func (ft *FakeTimer) Stop() bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	wasActive := !ft.stopped && !ft.fired
	ft.stopped = true
	return wasActive
}

func (ft *FakeTimer) maybeFire(now time.Time) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if ft.stopped || ft.fired {
		return
	}
	if !now.Before(ft.fire) {
		select {
		case ft.ch <- ft.fire:
		default:
		}
		ft.fired = true
	}
}
