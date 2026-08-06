package dst

import (
	"context"
	"errors"
	"sync"

	"github.com/shipstream/bloodraven/internal/platform"
)

// The production code reaches these optional interfaces through runtime type
// assertions, which fail silently — the sim would just stop exercising the
// read-back / schema paths. Assert them at compile time so a signature change
// on the production side breaks the build instead.
var (
	_ platform.DNSUpdater      = (*simDNS)(nil)
	_ platform.DNSRecordReader = (*simDNS)(nil)
	_ platform.NodeTainter     = (*simTainter)(nil)
)

// simDNS implements platform.DNSUpdater and platform.DNSRecordReader against
// the cluster. The record read-back path matters: reconcileDNS prefers the
// live record, and its restart-heal behavior depends on it.
type simDNS struct {
	c  *Cluster
	mu sync.Mutex

	record string
	found  bool
}

func newSimDNS(c *Cluster) *simDNS { return &simDNS{c: c} }

func (d *simDNS) UpdateDNSRecord(_ context.Context, ip string) error {
	d.c.mu.Lock()
	if d.c.operatorDead {
		d.c.mu.Unlock()
		return errOperatorDead
	}
	denied := d.c.dnsDenied
	site := d.c.byLBIP[ip]
	if !denied {
		d.c.event(site, EvDNSSet, "ip="+ip, "")
	} else {
		d.c.event(site, EvDNSSet, "ip="+ip, "denied")
	}
	d.c.mu.Unlock()
	if denied {
		return errors.New("dnsendpoints.externaldns.k8s.io is forbidden (sim: outage)")
	}
	d.mu.Lock()
	d.record = ip
	d.found = true
	d.mu.Unlock()
	return nil
}

func (d *simDNS) CurrentDNSRecord(_ context.Context) (string, bool, error) {
	d.c.mu.Lock()
	dead := d.c.operatorDead
	denied := d.c.dnsDenied
	d.c.mu.Unlock()
	if dead {
		return "", false, errOperatorDead
	}
	if denied {
		return "", false, errors.New("dnsendpoints.externaldns.k8s.io get forbidden (sim: outage)")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.record, d.found, nil
}

// Record returns the current DNS target ip and the site it maps to.
func (d *simDNS) Record() (ip, site string, found bool) {
	d.mu.Lock()
	ip, found = d.record, d.found
	d.mu.Unlock()
	d.c.mu.Lock()
	site = d.c.byLBIP[ip]
	d.c.mu.Unlock()
	return ip, site, found
}

// simTainter implements platform.NodeTainter, recording taint state.
type simTainter struct {
	c      *Cluster
	mu     sync.Mutex
	taints map[string]bool
}

func newSimTainter(c *Cluster) *simTainter {
	return &simTainter{c: c, taints: make(map[string]bool)}
}

func (t *simTainter) SetTaint(_ context.Context, selector, _ string, taint bool) error {
	if t.c.OperatorDead() {
		return errOperatorDead
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.taints[selector] = taint
	return nil
}
