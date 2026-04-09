package util

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	k8sretry "k8s.io/client-go/util/retry"
)

// PermanentError wraps an error to indicate it should not be retried.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// RetryWithBackoff retries fn with exponential backoff until it succeeds,
// maxRetries is exhausted, or the context is cancelled.
// Delays are: baseDelay, baseDelay*2, baseDelay*4, etc.
// If fn returns a *PermanentError, retries stop immediately.
func RetryWithBackoff(ctx context.Context, logger *slog.Logger, maxRetries int, baseDelay time.Duration, fn func() error) error {
	if logger == nil {
		logger = slog.Default()
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("context cancelled after %d attempts (last error: %w): %w", attempt, lastErr, err)
			}
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		// Stop immediately on permanent errors.
		var permanent *PermanentError
		if errors.As(lastErr, &permanent) {
			return permanent.Err
		}

		if attempt < maxRetries {
			delay := baseDelay * (1 << uint(attempt))
			logger.Warn("retrying after error", "attempt", attempt+1, "maxRetries", maxRetries, "delay", delay, "error", lastErr)

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during backoff (last error: %w): %w", lastErr, ctx.Err())
			case <-time.After(delay):
			}
		}
	}
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// RetryOnConflict retries fn using the default Kubernetes conflict retry backoff.
// This is a convenience wrapper around k8s.io/client-go/util/retry.RetryOnConflict.
func RetryOnConflict(fn func() error) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, fn)
}
