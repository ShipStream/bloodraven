package sidecar

import (
	"testing"
	"time"
)

func TestTopologyCache_ZeroValueIsEmpty(t *testing.T) {
	var c TopologyCache
	snap := c.Snapshot()
	if snap.ActiveSite != "" {
		t.Errorf("zero-value activeSite = %q, want \"\"", snap.ActiveSite)
	}
	if !snap.ObservedAt.IsZero() {
		t.Errorf("zero-value observedAt = %v, want zero", snap.ObservedAt)
	}
}

func TestTopologyCache_SetOverwrites(t *testing.T) {
	c := &TopologyCache{}
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c.Set("iad", t1)
	// Set with an older timestamp must still overwrite — the
	// operator is always authoritative.
	t0 := t1.Add(-1 * time.Hour)
	c.Set("pdx", t0)
	snap := c.Snapshot()
	if snap.ActiveSite != "pdx" {
		t.Errorf("activeSite = %q, want pdx", snap.ActiveSite)
	}
	if !snap.ObservedAt.Equal(t0) {
		t.Errorf("observedAt = %v, want %v", snap.ObservedAt, t0)
	}
}

func TestTopologyCache_AdoptNewerWins(t *testing.T) {
	c := &TopologyCache{}
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c.Set("iad", t1)

	t2 := t1.Add(5 * time.Second)
	if !c.Adopt("pdx", t2) {
		t.Fatal("Adopt returned false for strictly-newer observedAt")
	}
	snap := c.Snapshot()
	if snap.ActiveSite != "pdx" {
		t.Errorf("activeSite = %q, want pdx", snap.ActiveSite)
	}
	if !snap.ObservedAt.Equal(t2) {
		t.Errorf("observedAt = %v, want %v", snap.ObservedAt, t2)
	}
}

func TestTopologyCache_AdoptOlderIgnored(t *testing.T) {
	c := &TopologyCache{}
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c.Set("iad", t1)

	t0 := t1.Add(-5 * time.Second)
	if c.Adopt("pdx", t0) {
		t.Fatal("Adopt returned true for older observedAt")
	}
	snap := c.Snapshot()
	if snap.ActiveSite != "iad" {
		t.Errorf("activeSite = %q, want iad (unchanged)", snap.ActiveSite)
	}
}

func TestTopologyCache_AdoptEqualIgnored(t *testing.T) {
	// Strict "after" means equal timestamps don't adopt. This keeps
	// adoption stable and prevents a ping-pong between peers that
	// happen to have identical cached observedAt values.
	c := &TopologyCache{}
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c.Set("iad", t1)

	if c.Adopt("pdx", t1) {
		t.Fatal("Adopt returned true for equal observedAt")
	}
	snap := c.Snapshot()
	if snap.ActiveSite != "iad" {
		t.Errorf("activeSite = %q, want iad (unchanged)", snap.ActiveSite)
	}
}
