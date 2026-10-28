package main

import (
	"net"
	"net/netip"
	"sync"
	"time"
)

// pitrCutoffCache stores safe deferrals, keyed by namespace/group/profile.
// Positive authority and lookup failures are never cached.
type pitrCutoffCache struct {
	ttl  time.Duration
	mu   sync.Mutex
	data map[string]pitrCutoffEntry
}

type pitrCutoffEntry struct {
	value map[string]any
	at    time.Time
}

func newPITRCutoffCache(ttl time.Duration) *pitrCutoffCache {
	return &pitrCutoffCache{ttl: ttl, data: make(map[string]pitrCutoffEntry)}
}

func (c *pitrCutoffCache) key(ns, group, profile string) string {
	return ns + "/" + group + "/" + profile
}

func (c *pitrCutoffCache) get(ns, group, profile string) (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.data[c.key(ns, group, profile)]
	if !ok || e.value == nil || time.Since(e.at) > c.ttl {
		return nil, false
	}
	return e.value, true
}

// Bound safe-deferral memory across configured groups. Cache saturation skips
// storage of a result; it never refuses the current request.
const pitrCutoffCacheMaxEntries = 1024

func (c *pitrCutoffCache) set(ns, group, profile string, value map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	key := c.key(ns, group, profile)
	if _, exists := c.data[key]; !exists && len(c.data) >= pitrCutoffCacheMaxEntries {
		for k, e := range c.data {
			if now.Sub(e.at) > c.ttl {
				delete(c.data, k)
			}
		}
		if len(c.data) >= pitrCutoffCacheMaxEntries {
			return
		}
	}
	c.data[key] = pitrCutoffEntry{value: value, at: now}
}

// Admission follows the transport's source IP, never attacker-supplied query
// keys or forwarding headers. A full map evicts its oldest caller rather than
// denying a new caller. Entries are shared across that caller's connections.
type pitrCallerLimiter struct {
	mu      sync.Mutex
	ttl     time.Duration
	callers map[string]time.Time
}

const maxPITRCutoffCallers = 1024

func newPITRCallerLimiter(ttl time.Duration) *pitrCallerLimiter {
	return &pitrCallerLimiter{ttl: ttl, callers: make(map[string]time.Time)}
}
func (l *pitrCallerLimiter) admit(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		host = addr.Unmap().String()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if last, ok := l.callers[host]; ok && now.Sub(last) < l.ttl {
		return false
	}
	if len(l.callers) >= maxPITRCutoffCallers {
		oldest := ""
		var oldestAt time.Time
		for key, at := range l.callers {
			if now.Sub(at) >= l.ttl {
				delete(l.callers, key)
				continue
			}
			if oldest == "" || at.Before(oldestAt) {
				oldest, oldestAt = key, at
			}
		}
		if len(l.callers) >= maxPITRCutoffCallers {
			delete(l.callers, oldest)
		}
	}
	l.callers[host] = now
	return true
}
