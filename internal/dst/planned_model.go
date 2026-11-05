package dst

import (
	"context"
	"fmt"
)

// ---------------------------------------------------------------------------
// Actor attribution
// ---------------------------------------------------------------------------

// Actor names the operator-side (or sidecar) actor that issued a statement.
// The harness tags each actor's ctx with WithActor, simChecker reads the tag
// back on every statement, and the planned-failover invariants attribute
// mutations by it.
type Actor string

const (
	// ActorTM is the topology manager's Poll. An untagged ctx counts as it.
	ActorTM Actor = "tm"
	// ActorPF is the planned-failover engine and its topology facade.
	ActorPF Actor = "pf"
	// ActorUpdate is the ordered-update step actor.
	ActorUpdate Actor = "upd"
	// ActorSidecar is a site's fencing monitor.
	ActorSidecar Actor = "sidecar"
	// ActorRogue is writability applied by a human at a yield point.
	ActorRogue Actor = "rogue"
)

type actorKey struct{}

// WithActor tags ctx with the actor whose statements it carries. Values
// survive context.WithTimeout and friends, so a facade that derives its own
// statement deadlines keeps the tag.
func WithActor(ctx context.Context, a Actor) context.Context {
	return context.WithValue(ctx, actorKey{}, a)
}

// actorFrom returns ctx's actor tag; "" for an untagged ctx (nil included).
func actorFrom(ctx context.Context) Actor {
	if ctx == nil {
		return ""
	}
	a, _ := ctx.Value(actorKey{}).(Actor)
	return a
}

// effectiveActor maps an event's raw tag onto the actor it stands for: an
// untagged statement is the topology manager's.
func effectiveActor(a Actor) Actor {
	if a == "" {
		return ActorTM
	}
	return a
}

// ---------------------------------------------------------------------------
// Pod replacement and restarts
// ---------------------------------------------------------------------------

// historyDepth bounds the executed-set snapshots a regressing pod
// replacement can roll back to.
const historyDepth = 32

// recordHistoryLocked appends this tick's executed sets. Must be called
// with c.mu held.
func (c *Cluster) recordHistoryLocked() {
	for _, s := range c.sites {
		s.history = append(s.history, s.executed.clone())
		if len(s.history) > historyDepth {
			s.history = s.history[len(s.history)-historyDepth:]
		}
	}
}

// replacementUUID derives the server_uuid a regressed data directory boots
// with: the original uuid with its first group replaced by the incarnation,
// so it stays a valid UUID that mysql.ParseGTIDSet accepts.
func replacementUUID(orig string, incarnation int) string {
	if len(orig) < 8 {
		return fmt.Sprintf("%08x%s", incarnation, orig)
	}
	return fmt.Sprintf("%08x%s", incarnation, orig[8:])
}

// ReplacePod replaces the site's MySQL pod with a new one behind the same
// Service: a new server incarnation (every pinned session from the old one
// fails on its next statement), booted the way mysqld boots, read_only=ON
// with super_read_only OFF, its replica threads auto-starting when channel
// metadata exists.
//
// With back > 0 the new pod comes up on a regressed data directory, the
// executed set of back ticks ago, under a fresh server_uuid (#218: a target
// replaced between the lag proof and the promotion). A regress is applied
// only to a site that was not writable, and only when every transaction it
// rolls back is still held by another site: rolling back a writable
// primary, or the last copy of an acknowledged write, destroys data no
// operator can recover, which the end-state invariants would rightly flag
// as the model's doing, not the operator's.
func (c *Cluster) ReplacePod(site string, back int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.byName[site]
	wasWritable := !s.crashed && !s.readOnly && !s.superReadOnly
	s.crashed = false
	s.incarnation++
	s.readOnly = true
	s.superReadOnly = false
	detail := fmt.Sprintf("incarnation=%d", s.incarnation)
	switch {
	case back <= 0:
	case wasWritable:
		detail += " regress=skipped(writable)"
	case len(s.history) == 0:
		detail += " regress=skipped(no history)"
	case !c.heldElsewhereLocked(s, s.history[max(len(s.history)-1-back, 0)]):
		detail += " regress=skipped(sole holder)"
	default:
		s.executed = s.history[max(len(s.history)-1-back, 0)].clone()
		s.retrieved = s.executed.clone()
		s.serverUUID = replacementUUID(s.uuid, s.incarnation)
		detail += fmt.Sprintf(" regress=%d server_uuid=%s", back, s.serverUUID)
	}
	if s.replConfigured {
		s.ioWant = true
		s.sqlWant = true
		s.ioErr = ""
		s.sqlErr = ""
	}
	c.event(site, EvReplacePod, detail, "")
}

// heldElsewhereLocked reports whether every transaction s would lose by
// rolling back to snap is held by some other site. Must be called with c.mu
// held.
func (c *Cluster) heldElsewhereLocked(s *siteData, snap gtidVec) bool {
	for u, n := range s.executed {
		if n <= snap[u] {
			continue
		}
		held := false
		for _, o := range c.sites {
			if o != s && o.executed[u] >= n {
				held = true
				break
			}
		}
		if !held {
			return false
		}
	}
	return true
}

// RestartSite restarts mysqld in place, keeping its data: a new
// incarnation booted read_only=ON with super_read_only OFF, replica
// threads auto-starting when channel metadata exists. A crashed site is
// left alone (Recover brings it back).
func (c *Cluster) RestartSite(site string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.byName[site]
	if s.crashed {
		return
	}
	s.incarnation++
	s.readOnly = true
	s.superReadOnly = false
	if s.replConfigured {
		s.ioWant = true
		s.sqlWant = true
		s.ioErr = ""
		s.sqlErr = ""
	}
	c.event(site, EvRestartSite, fmt.Sprintf("incarnation=%d", s.incarnation), "")
}

// Incarnation returns the site's current server incarnation.
func (c *Cluster) Incarnation(site string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byName[site].incarnation
}

// ServerUUID returns the site's current @@server_uuid.
func (c *Cluster) ServerUUID(site string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byName[site].serverUUID
}

// plannedHooks are the Cluster's planned-failover actor hooks; every field
// may be nil.
type plannedHooks struct {
	yield      func(point string)
	slow       func(site string, kind EventKind, actor Actor) bool
	onMutation func(e Event, before, after SiteTruth)
	onEvent    func(e Event)
}

// setPlannedHooks installs the planned-failover actor hooks. The harness
// calls it once, before the first poll. The observers run with c.mu held
// and must not call back into the Cluster's locking methods.
func (c *Cluster) setPlannedHooks(h plannedHooks) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.yield = h.yield
	c.slow = h.slow
	c.onMutation = h.onMutation
	c.onEvent = h.onEvent
}

// tryLockCluster reports whether c.mu is free, leaving it unlocked. The
// yield discipline check uses it: the harness is one goroutine, so a failed
// TryLock means the current stack holds the lock.
func (c *Cluster) tryLockCluster() bool {
	if !c.mu.TryLock() {
		return false
	}
	c.mu.Unlock()
	return true
}
