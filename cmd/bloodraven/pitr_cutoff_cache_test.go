package main

import (
	"fmt"
	"testing"
	"time"
)

// TestPITRCutoffCacheIsBounded pins that unauthenticated callers cannot
// grow the /pitr-cutoff cache without bound, and that expired entries
// make room again.
func TestPITRCutoffCacheIsBounded(t *testing.T) {
	c := newPITRCutoffCache(time.Hour)
	for i := 0; i < pitrCutoffCacheMaxEntries+100; i++ {
		c.set("ns", fmt.Sprintf("g%d", i), "p", map[string]any{})
	}
	if got := len(c.data); got != pitrCutoffCacheMaxEntries {
		t.Fatalf("cache size = %d, want %d", got, pitrCutoffCacheMaxEntries)
	}
	// Existing keys can still be refreshed at the cap.
	c.set("ns", "g0", "p", map[string]any{"retentionDeferredReason": "backupInFlight"})
	if v, ok := c.get("ns", "g0", "p"); !ok || v["retentionDeferredReason"] != "backupInFlight" {
		t.Fatalf("refresh at cap failed: %v %v", v, ok)
	}

	// Once entries expire, a new key evicts them instead of being dropped.
	for k, e := range c.data {
		e.at = time.Now().Add(-2 * time.Hour)
		c.data[k] = e
	}
	c.set("ns", "fresh", "p", map[string]any{})
	if _, ok := c.get("ns", "fresh", "p"); !ok {
		t.Fatal("new key not cached after expired entries were evictable")
	}
	if got := len(c.data); got != 1 {
		t.Fatalf("cache size after eviction = %d, want 1", got)
	}
}

func TestPITRCutoffCallerBoundDoesNotDenyNewCaller(t *testing.T) {
	l := newPITRCallerLimiter(time.Minute)
	for i := 0; i < maxPITRCutoffCallers+100; i++ {
		if !l.admit(fmt.Sprintf("10.0.%d.%d:1000", i/256, i%256)) {
			t.Fatal("different caller refused")
		}
	}
	if len(l.callers) != maxPITRCutoffCallers {
		t.Fatalf("unbounded caller map: %d", len(l.callers))
	}
	if !l.admit("192.0.2.2:2000") {
		t.Fatal("full map denied a healthy new caller")
	}
	if l.admit("192.0.2.2:3000") || l.admit("[::ffff:192.0.2.2]:4000") {
		t.Fatal("a new connection or IPv4 alias bypassed caller throttling")
	}
	l.callers["192.0.2.2"] = time.Now().Add(-2 * time.Minute)
	if !l.admit("192.0.2.2:5000") {
		t.Fatal("caller did not regain its own admission after expiry")
	}
}
