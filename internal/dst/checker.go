package dst

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// simChecker implements mysql.Checker against one simulated site. All state
// lives in the shared Cluster; methods are safe for the concurrent read
// probes Poll issues (per-site goroutines) because every access holds the
// cluster lock and mutations only ever arrive from the operator's main poll
// goroutine.
type simChecker struct {
	c *Cluster
	s *siteData
}

// NewChecker returns the mysql.Checker for the named site.
func (c *Cluster) NewChecker(site string) mysql.Checker {
	s := c.byName[site]
	if s == nil {
		panic("dst: unknown site " + site)
	}
	return &simChecker{c: c, s: s}
}

var errNotApplied = errors.New("read tcp: i/o timeout (sim: statement not applied)")
var errAmbiguous = errors.New("read tcp: i/o timeout (sim: statement applied, response lost)")

// reachableLocked returns nil when the operator can talk to this site.
func (m *simChecker) reachableLocked() error {
	if m.s.crashed {
		return fmt.Errorf("dial tcp %s:3306: connect: connection refused", m.s.host)
	}
	if m.c.opLinkDown[m.s.name] {
		return fmt.Errorf("dial tcp %s:3306: i/o timeout", m.s.host)
	}
	return nil
}

// mutate runs a state-changing statement under the fault rules: reachability,
// then fail-without-apply, then apply (which may itself return a semantic
// MySQL error), then ambiguous apply-but-error.
func (m *simChecker) mutate(kind EventKind, detail string, fn func() error) error {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		m.c.event(m.s.name, kind, detail, "unreachable")
		return err
	}
	if m.c.failMuts[m.s.name] > 0 {
		m.c.failMuts[m.s.name]--
		m.c.event(m.s.name, kind, detail, "failed")
		return errNotApplied
	}
	if err := fn(); err != nil {
		m.c.event(m.s.name, kind, detail, "rejected:"+err.Error())
		return err
	}
	if m.c.ambiguousMuts[m.s.name] > 0 {
		m.c.ambiguousMuts[m.s.name]--
		m.c.event(m.s.name, kind, detail, "ambiguous")
		return errAmbiguous
	}
	m.c.event(m.s.name, kind, detail, "")
	return nil
}

func (m *simChecker) CheckReadOnly(_ context.Context) (bool, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return false, err
	}
	return m.s.readOnly, nil
}

func (m *simChecker) Promote(_ context.Context) error {
	// STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only = 0.
	return m.mutate(EvPromote, "", func() error {
		m.s.ioWant = false
		m.s.sqlWant = false
		m.s.ioErr = ""
		m.s.sqlErr = ""
		m.s.replConfigured = false
		m.s.sourceHost = ""
		m.s.retrieved = make(gtidVec)
		m.s.readOnly = false
		m.s.superReadOnly = false // read_only=OFF implies super_read_only=OFF
		return nil
	})
}

func (m *simChecker) Close() error { return nil }

func (m *simChecker) SetSuperReadOnly(_ context.Context, on bool) error {
	return m.mutate(EvSetSuperRO, fmt.Sprintf("on=%v", on), func() error {
		if on {
			m.s.superReadOnly = true
			m.s.readOnly = true // super_read_only=ON implies read_only=ON
		} else {
			m.s.superReadOnly = false
		}
		return nil
	})
}

func (m *simChecker) KillAppConnections(_ context.Context) (int, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return 0, err
	}
	m.c.event(m.s.name, EvKillConns, "", "")
	return 0, nil
}

func (m *simChecker) StopReplica(_ context.Context) error {
	return m.mutate(EvStopReplica, "", func() error {
		m.s.ioWant = false
		m.s.sqlWant = false
		return nil
	})
}

func (m *simChecker) ResetReplicaAll(_ context.Context) error {
	return m.mutate(EvResetReplica, "", func() error {
		if m.s.ioWant || m.s.sqlWant {
			return errors.New("Error 3081: replica must be stopped before RESET REPLICA ALL")
		}
		m.s.replConfigured = false
		m.s.sourceHost = ""
		m.s.ioErr = ""
		m.s.sqlErr = ""
		m.s.retrieved = make(gtidVec)
		return nil
	})
}

func (m *simChecker) SetReadOnly(_ context.Context, on bool) error {
	return m.mutate(EvSetRO, fmt.Sprintf("on=%v", on), func() error {
		if on {
			m.s.readOnly = true
		} else {
			m.s.readOnly = false
			m.s.superReadOnly = false // read_only=OFF implies super_read_only=OFF
		}
		return nil
	})
}

func (m *simChecker) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return nil, err
	}
	if !m.s.replConfigured {
		return nil, nil
	}
	io, sql := m.c.effectiveThreadsLocked(m.s)
	rs := &mysql.ReplicaStatus{
		IORunning:        io,
		SQLRunning:       sql,
		LastError:        firstNonEmpty(m.s.sqlErr, m.s.ioErr),
		SourceHost:       m.s.sourceHost,
		ExecutedGtidSet:  m.s.executed.String(),
		RetrievedGtidSet: m.s.retrieved.String(),
	}
	if io && sql {
		var behind int64
		if src := m.c.byHost[canonicalHost(m.s.sourceHost)]; src != nil {
			behind = m.s.executed.deficit(src.executed)
		}
		rs.SecondsBehindSource = &behind
	}
	return rs, nil
}

func (m *simChecker) ChangeReplicationSource(_ context.Context, opts mysql.ReplicationSourceOpts) error {
	return m.mutate(EvChangeSource, "host="+opts.Host, func() error {
		if m.s.ioWant || m.s.sqlWant {
			return errors.New("Error 3081: replica must be stopped before CHANGE REPLICATION SOURCE")
		}
		// Model-level safety check: pointing this site at a source that lacks
		// transactions this site has already executed means the operator is
		// about to wedge (or worse, believes it recovered) a divergent site
		// without blocking on the divergence. The gates in recovery and
		// source convergence are supposed to make this unreachable.
		if src := m.c.byHost[canonicalHost(opts.Host)]; src != nil && src != m.s {
			if !src.executed.contains(m.s.executed) {
				m.c.violate("RepointDivergent", fmt.Sprintf(
					"site %s repointed at %s which lacks %d of its transactions (site=%s source=%s)",
					m.s.name, opts.Host, m.s.executed.deficit(src.executed), m.s.executed, src.executed))
			}
		}
		m.s.replConfigured = true
		m.s.sourceHost = opts.Host
		m.s.ioErr = ""
		m.s.sqlErr = ""
		// Real MySQL purges the relay logs when CHANGE REPLICATION SOURCE
		// changes the connection metadata; without this, stale relay entries
		// fetched from the OLD source could be applied after a repoint.
		m.s.retrieved = make(gtidVec)
		return nil
	})
}

func (m *simChecker) StartReplica(_ context.Context) error {
	return m.mutate(EvStartReplica, "", func() error {
		if !m.s.replConfigured {
			return errors.New("Error 1200: the server is not configured as a replica")
		}
		m.s.ioWant = true
		m.s.sqlWant = true
		m.s.ioErr = ""
		m.s.sqlErr = ""
		return nil
	})
}

func (m *simChecker) StartReplicaSQLThread(_ context.Context) error {
	return m.mutate(EvStartSQL, "", func() error {
		if !m.s.replConfigured {
			return errors.New("Error 1200: the server is not configured as a replica")
		}
		m.s.sqlWant = true
		m.s.sqlErr = ""
		return nil
	})
}

// WaitForRelayLogDrain resolves instantly against the model, mirroring the
// real implementation's decision tree: abort on a replica error, otherwise
// apply the local relay backlog (restarting the SQL thread if needed).
func (m *simChecker) WaitForRelayLogDrain(_ context.Context, timeout time.Duration) error {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		m.c.event(m.s.name, EvDrain, "", "unreachable")
		return err
	}
	// A stalled SQL apply means the backlog cannot drain within the timeout,
	// exactly like an explicit drain stall — the real WaitForRelayLogDrain
	// would poll until the deadline and report a timeout, and the promotion
	// proceeds with the backlog unapplied.
	if m.c.drainStalled[m.s.name] || m.c.applyStalled[m.s.name] {
		m.c.event(m.s.name, EvDrain, "", "timeout")
		return fmt.Errorf("relay log drain timed out after %s", timeout)
	}
	if !m.s.replConfigured {
		m.c.event(m.s.name, EvDrain, "no channel", "")
		return nil
	}
	if lastErr := firstNonEmpty(m.s.sqlErr, m.s.ioErr); lastErr != "" {
		m.c.event(m.s.name, EvDrain, "", "aborted")
		return fmt.Errorf("relay log drain aborted: SQL thread error: %s", lastErr)
	}
	if !m.s.sqlWant {
		// SQL thread stopped: pending relay logs restart it, else done.
		if m.s.executed.contains(m.s.retrieved) {
			m.c.event(m.s.name, EvDrain, "nothing pending", "")
			return nil
		}
		m.s.sqlWant = true
	}
	var applied int64
	for u, n := range m.s.retrieved {
		if n > m.s.executed[u] {
			applied += n - m.s.executed[u]
			m.s.executed[u] = n
		}
	}
	m.c.event(m.s.name, EvDrain, fmt.Sprintf("applied=%d", applied), "")
	return nil
}

func (m *simChecker) GetGtidExecuted(_ context.Context) (string, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return "", err
	}
	return m.s.executed.String(), nil
}

// HasUserSchemas satisfies the operator's userSchemaChecker type assertion.
func (m *simChecker) HasUserSchemas(_ context.Context) (bool, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return false, err
	}
	return m.s.hasData, nil
}

// Clone methods are unreachable in trials (bootstrap controller is disabled);
// loud errors guard the assumption.
func (m *simChecker) EnsureClonePlugin(_ context.Context) error {
	return errors.New("dst: CLONE not modeled (bootstrap must stay disabled in trials)")
}

func (m *simChecker) SetCloneDonorList(_ context.Context, _ string) error {
	return errors.New("dst: CLONE not modeled (bootstrap must stay disabled in trials)")
}

func (m *simChecker) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
	return errors.New("dst: CLONE not modeled (bootstrap must stay disabled in trials)")
}
