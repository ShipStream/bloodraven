package util

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// RetryWithBackoff retries fn with exponential backoff until it succeeds,
// maxRetries is exhausted, or the context is cancelled.
// Delays are: baseDelay, baseDelay*2, baseDelay*4, etc.
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
