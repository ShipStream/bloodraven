package clock

import (
	"testing"
	"time"
)

func TestRealClock(t *testing.T) {
	c := RealClock{}

	before := time.Now()
	got := c.Now()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("RealClock.Now() = %v, want between %v and %v", got, before, after)
	}

	past := time.Now().Add(-5 * time.Second)
	d := c.Since(past)
	if d < 4*time.Second || d > 6*time.Second {
		t.Errorf("RealClock.Since() = %v, want ~5s", d)
	}
}

func TestFakeClockNow(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	if got := fc.Now(); !got.Equal(start) {
		t.Errorf("Now() = %v, want %v", got, start)
	}
}

func TestFakeClockAdvance(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	fc.Advance(10 * time.Second)

	want := start.Add(10 * time.Second)
	if got := fc.Now(); !got.Equal(want) {
		t.Errorf("Now() after Advance = %v, want %v", got, want)
	}
}

func TestFakeClockSince(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)
	fc.Advance(30 * time.Second)

	got := fc.Since(start)
	if got != 30*time.Second {
		t.Errorf("Since() = %v, want 30s", got)
	}
}

func TestFakeClockSet(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	target := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	fc.Set(target)

	if got := fc.Now(); !got.Equal(target) {
		t.Errorf("Now() after Set = %v, want %v", got, target)
	}
}

func TestFakeTickerFires(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	ticker := fc.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Advance less than interval — should not fire.
	fc.Advance(5 * time.Second)
	select {
	case <-ticker.C():
		t.Error("ticker fired before interval elapsed")
	default:
	}

	// Advance past interval — should fire once.
	fc.Advance(5 * time.Second)
	select {
	case <-ticker.C():
		// expected
	default:
		t.Error("ticker did not fire at interval")
	}

	// Advance by 2x interval — should fire twice (but channel is buffered 1,
	// so we get at least one).
	fc.Advance(20 * time.Second)
	select {
	case <-ticker.C():
	default:
		t.Error("ticker did not fire after 2x interval advance")
	}
}

func TestFakeTickerStop(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	ticker := fc.NewTicker(10 * time.Second)
	ticker.Stop()

	fc.Advance(20 * time.Second)
	select {
	case <-ticker.C():
		t.Error("stopped ticker should not fire")
	default:
	}
}

func TestFakeTimerFires(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	timer := fc.NewTimer(10 * time.Second)

	// Advance less than duration — should not fire.
	fc.Advance(5 * time.Second)
	select {
	case <-timer.C():
		t.Error("timer fired before duration elapsed")
	default:
	}

	// Advance past duration — should fire once.
	fc.Advance(5 * time.Second)
	select {
	case <-timer.C():
		// expected
	default:
		t.Error("timer did not fire at duration")
	}

	// Advance further — should NOT fire again (one-shot).
	fc.Advance(20 * time.Second)
	select {
	case <-timer.C():
		t.Error("timer fired a second time (should be one-shot)")
	default:
	}
}

func TestFakeTimerStop(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	timer := fc.NewTimer(10 * time.Second)

	// Stop before firing — should return true (was active).
	if !timer.Stop() {
		t.Error("Stop() should return true for an active timer")
	}

	// Advance past duration — should NOT fire.
	fc.Advance(20 * time.Second)
	select {
	case <-timer.C():
		t.Error("stopped timer should not fire")
	default:
	}
}

func TestFakeTimerStopAfterFired(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	timer := fc.NewTimer(5 * time.Second)
	fc.Advance(5 * time.Second)
	<-timer.C()

	// Stop after firing — should return false (was not active).
	if timer.Stop() {
		t.Error("Stop() should return false for an already-fired timer")
	}
}

func TestFakeTimerPruning(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	// Create and fire several timers.
	for i := 0; i < 10; i++ {
		fc.NewTimer(time.Duration(i+1) * time.Second)
	}

	// Advance past all of them — they should all fire and be pruned.
	fc.Advance(11 * time.Second)

	fc.mu.Lock()
	remaining := len(fc.timers)
	fc.mu.Unlock()

	if remaining != 0 {
		t.Errorf("expected 0 timers after pruning, got %d", remaining)
	}
}
