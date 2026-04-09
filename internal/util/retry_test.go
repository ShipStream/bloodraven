package util

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestRetryWithBackoff_SucceedsFirstTry(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), slog.Default(), 3, 10*time.Millisecond, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryWithBackoff_SucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), slog.Default(), 3, 10*time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryWithBackoff_GivesUpAfterMaxRetries(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), slog.Default(), 2, 10*time.Millisecond, func() error {
		calls++
		return errors.New("persistent error")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// maxRetries=2 means initial attempt + 2 retries = 3 calls
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if !errors.Is(err, errors.Unwrap(err)) {
		// Just check the error message contains our wrapping
	}
	expected := "failed after 2 retries: persistent error"
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestRetryWithBackoff_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := RetryWithBackoff(ctx, slog.Default(), 5, 500*time.Millisecond, func() error {
		calls++
		if calls == 1 {
			cancel() // Cancel during backoff wait
		}
		return errors.New("error")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled in error chain, got %v", err)
	}
	if calls > 2 {
		t.Fatalf("expected at most 2 calls due to cancellation, got %d", calls)
	}
}
