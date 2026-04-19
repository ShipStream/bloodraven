package sidecar

import (
	"sync"
	"time"
)

// TopologyCache is the sidecar's cached view of the
// operator-authoritative active site for this failover group.
//
// The FencingMonitor writes the cache from two sources:
//  1. Direct polls of the operator's /active-site endpoint.
//  2. Peer-relayed reads via /peer/active-site on peer sidecars, used
//     when this sidecar is partitioned from the operator but can still
//     reach a peer that is not.
//
// The Server reads the cache to serve /peer/active-site. The zero
// value is "no cached view yet" — ActiveSite == "" and ObservedAt
// is zero. That state is deliberately indistinguishable from a
// sidecar that has never been told anything, so callers treat it as
// "unknown, make no fencing decision".
type TopologyCache struct {
	mu         sync.RWMutex
	activeSite string
	observedAt time.Time
}

// TopologySnapshot is the on-the-wire shape of /peer/active-site
// responses and the return type of Snapshot(). ObservedAt is the
// sender's clock reading at the moment the value was last updated
// from an authoritative source.
type TopologySnapshot struct {
	ActiveSite string    `json:"activeSite"`
	ObservedAt time.Time `json:"observedAt"`
}

// Set unconditionally overwrites the cached view. Use for values
// read directly from the operator — the operator is always
// authoritative, so we never compare timestamps.
func (c *TopologyCache) Set(activeSite string, observedAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeSite = activeSite
	c.observedAt = observedAt
}

// Adopt updates the cache only if observedAt is strictly newer
// than the current value. Used when merging a peer's view: this
// prevents a stale peer from dragging our cache backwards and
// avoids flapping when peers disagree briefly.
func (c *TopologyCache) Adopt(activeSite string, observedAt time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !observedAt.After(c.observedAt) {
		return false
	}
	c.activeSite = activeSite
	c.observedAt = observedAt
	return true
}

// Snapshot returns the current cached view. ActiveSite == ""
// means "no cached view yet".
func (c *TopologyCache) Snapshot() TopologySnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return TopologySnapshot{ActiveSite: c.activeSite, ObservedAt: c.observedAt}
}
