package logs

import (
	"context"
	"testing"
	"time"
)

func TestTailerWaitMatchesAfterInjection(t *testing.T) {
	tail := New(Source{Pod: "p"}, 16)

	// Pre-existing line BEFORE the since cutoff.
	tail.append(Match{Time: time.Now().Add(-10 * time.Second), Line: `msg="failover complete"`})
	since := time.Now()

	// Line that arrives after since but does not match.
	go func() {
		time.Sleep(10 * time.Millisecond)
		tail.append(Match{Time: time.Now(), Line: `msg="state transition"`})
		time.Sleep(10 * time.Millisecond)
		tail.append(Match{Time: time.Now(), Line: `msg="failover complete" target="pdx"`})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := tail.Wait(ctx, since, Substring(`msg="failover complete"`))
	if err != nil {
		t.Fatalf("Wait err: %v", err)
	}
	if got.Time.Before(since) {
		t.Fatalf("returned match predates since cutoff: %v < %v", got.Time, since)
	}
	if got.Line == "" {
		t.Fatalf("returned match has empty line")
	}
}

func TestTailerWaitTimeoutReturnsCtxErr(t *testing.T) {
	tail := New(Source{Pod: "p"}, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := tail.Wait(ctx, time.Now(), Substring("never appears"))
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}
