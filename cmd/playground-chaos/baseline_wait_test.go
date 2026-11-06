package main

import (
	"context"
	"errors"
	"flag"
	"testing"
	"time"
)

func TestWaitForBaseline_HealthyImmediately(t *testing.T) {
	progressCalls := 0
	err := waitForBaseline(context.Background(), time.Second, time.Millisecond,
		func(context.Context) error { return nil },
		func(error, time.Duration) { progressCalls++ },
	)
	if err != nil {
		t.Fatalf("expected nil for an immediately healthy baseline, got %v", err)
	}
	if progressCalls != 0 {
		t.Errorf("expected no progress calls for a healthy baseline, got %d", progressCalls)
	}
}

func TestWaitForBaseline_ConvergesAfterRetries(t *testing.T) {
	checks := 0
	progressCalls := 0
	err := waitForBaseline(context.Background(), time.Second, time.Millisecond,
		func(context.Context) error {
			checks++
			if checks < 3 {
				return errors.New("site pdx state=\"unreachable\"")
			}
			return nil
		},
		func(error, time.Duration) { progressCalls++ },
	)
	if err != nil {
		t.Fatalf("expected nil once the baseline converges, got %v", err)
	}
	if checks != 3 {
		t.Errorf("expected 3 checks, got %d", checks)
	}
	if progressCalls != 2 {
		t.Errorf("expected 2 progress calls (one per failed check), got %d", progressCalls)
	}
}

func TestWaitForBaseline_TimeoutReturnsLastError(t *testing.T) {
	sentinel := errors.New("baseline unhealthy: never converges")
	err := waitForBaseline(context.Background(), 10*time.Millisecond, time.Millisecond,
		func(context.Context) error { return sentinel },
		func(error, time.Duration) {},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the last check error on timeout, got %v", err)
	}
}

func TestWaitForBaseline_ContextCancelledReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sentinel := errors.New("baseline unhealthy: still converging")
	done := make(chan error, 1)
	go func() {
		done <- waitForBaseline(ctx, time.Hour, time.Hour,
			func(context.Context) error { return sentinel },
			func(error, time.Duration) {},
		)
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatalf("expected the last check error on cancellation, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitForBaseline did not return promptly after context cancellation")
	}
}

// TestRunBaselineWait pins that `run` waits for a baseline only when
// --baseline-wait is given explicitly (the CI scenario loop passes it on the
// first scenario), while its 5m default still applies to run-all.
func TestRunBaselineWait(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want time.Duration
	}{
		{nil, 0},
		{[]string{"--baseline-wait=2m"}, 2 * time.Minute},
		{[]string{"--baseline-wait=0"}, 0},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		wait := fs.Duration("baseline-wait", 5*time.Minute, "")
		if err := fs.Parse(tc.args); err != nil {
			t.Fatal(err)
		}
		if got := runBaselineWait(fs, *wait); got != tc.want {
			t.Errorf("args %v: runBaselineWait = %s, want %s", tc.args, got, tc.want)
		}
	}
}
