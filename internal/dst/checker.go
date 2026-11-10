package dst

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// simChecker implements mysql.Checker against one simulated site. All state
// lives in the shared Cluster; methods are safe for the concurrent read
// probes Poll issues (per-site goroutines) because every access holds the
// cluster lock and mutations only ever arrive from the operator's main poll
// goroutine.
//
// A pinned simChecker (pinned=true) is the connection behind a simSession:
// it captured the site's server incarnation at pin time and fails every call
// once the incarnation has moved, the way a pinned MySQL connection fails
// after its server restarted or its pod was replaced.
type simChecker struct {
	c *Cluster
	s *siteData

	pinned bool
	pinInc int
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

// errSessionLost is what a pinned session's call sees once its server is
// gone. go-sql-driver/mysql reports a broken connection as "invalid
// connection"; the session never redials.
var errSessionLost = errors.New("invalid connection (sim: the pinned session's server is gone)")

// reachableLocked returns nil when the operator can talk to this site.
//
// A process killed mid-Execute fails here first: the remainder of the dying
// Poll must not be able to read or write anything, or the model would credit
// a dead operator with work it never did. A pinned connection whose server
// incarnation has moved fails next, whatever the address reaches now.
func (m *simChecker) reachableLocked() error {
	if m.c.operatorDead {
		return errOperatorDead
	}
	if m.pinned && m.s.incarnation != m.pinInc {
		return errSessionLost
	}
	if m.s.crashed {
		return fmt.Errorf("dial tcp %s:3306: connect: connection refused", m.s.host)
	}
	if m.c.opLinkDown[m.s.name] {
		return fmt.Errorf("dial tcp %s:3306: i/o timeout", m.s.host)
	}
	return nil
}

// stillConnectedLocked reports whether a statement issued at incarnation
// inc still has its connection: the server it reached has not been
// replaced, crashed or cut off since. Must be called with c.mu held.
func (m *simChecker) stillConnectedLocked(inc int) bool {
	return m.reachableLocked() == nil && m.s.incarnation == inc
}

// mutate runs a state-changing statement under the fault rules: reachability,
// then the armed mid-Execute crash, then fail-without-apply, then apply
// (which may itself return a semantic MySQL error), then ambiguous
// apply-but-error.
//
// A statement the trial marks slow (Cluster.slow) yields twice with c.mu
// released: between issue and apply, and between apply and acknowledgement,
// so another actor can act while this one is blocked inside the statement.
// If the server's incarnation moves (or it crashes, or the operator link is
// cut) during the first yield the statement never applies; during the second
// it applied and the reply is lost.
func (m *simChecker) mutate(ctx context.Context, kind EventKind, detail string, fn func() error) error {
	actor := actorFrom(ctx)
	m.c.mu.Lock()
	locked := true
	defer func() {
		if locked {
			m.c.mu.Unlock()
		}
	}()
	relock := func(point string) {
		m.c.mu.Unlock()
		locked = false
		m.c.yield(point)
		m.c.mu.Lock()
		locked = true
	}
	if err := m.reachableLocked(); err != nil {
		m.c.eventBy(m.s.name, kind, detail, "unreachable", actor)
		return err
	}
	// The crash countdown is consumed by every mutation the operator gets
	// as far as sending, whether or not the statement then succeeds — that
	// is what makes the death land at an arbitrary point in a sequence
	// rather than only at points that happen to be healthy.
	if m.c.crashNowLocked() {
		if m.c.crashPreApply {
			m.c.eventBy(m.s.name, kind, detail, "operatorDied", actor)
			m.c.killOperatorLocked(m.s.name, kind, false)
			return errOperatorDead
		}
		// The statement reaches the server and applies; the reply never
		// gets back to a process that no longer exists. Semantic rejections
		// still reject — a dying operator does not get to violate MySQL.
		before := m.observeLocked()
		if err := fn(); err != nil {
			m.c.eventBy(m.s.name, kind, detail, "rejected:"+err.Error(), actor)
			m.c.killOperatorLocked(m.s.name, kind, false)
			return err
		}
		e := m.c.eventBy(m.s.name, kind, detail, "appliedThenOperatorDied", actor)
		m.notifyLocked(e, before)
		m.c.killOperatorLocked(m.s.name, kind, true)
		return errOperatorDead
	}
	if m.c.failMuts[m.s.name] > 0 {
		m.c.failMuts[m.s.name]--
		m.c.eventBy(m.s.name, kind, detail, "failed", actor)
		return errNotApplied
	}
	slow := m.c.yield != nil && m.c.slow != nil && m.c.slow(m.s.name, kind, actor)
	inc := m.s.incarnation
	if slow {
		relock(fmt.Sprintf("stmt.issued:%s:%s", m.s.name, kind))
		if !m.stillConnectedLocked(inc) {
			m.c.eventBy(m.s.name, kind, detail, "lostBeforeApply", actor)
			return errSessionLost
		}
	}
	before := m.observeLocked()
	if err := fn(); err != nil {
		m.c.eventBy(m.s.name, kind, detail, "rejected:"+err.Error(), actor)
		return err
	}
	if slow {
		// The statement applied: the event (a clean apply, which every
		// invariant counts as landed) and the observers see it at the
		// instant it landed. What the acknowledgement brings back is a
		// second, non-effective event, so nothing counts the statement
		// twice.
		e := m.c.eventBy(m.s.name, kind, detail, "", actor)
		m.notifyLocked(e, before)
		relock(fmt.Sprintf("stmt.applied:%s:%s", m.s.name, kind))
		if !m.stillConnectedLocked(inc) {
			m.c.eventBy(m.s.name, kind, detail, "ack:responseLost", actor)
			return errAmbiguous
		}
		if m.c.ambiguousMuts[m.s.name] > 0 {
			m.c.ambiguousMuts[m.s.name]--
			m.c.eventBy(m.s.name, kind, detail, "ack:ambiguous", actor)
			return errAmbiguous
		}
		return nil
	}
	if m.c.ambiguousMuts[m.s.name] > 0 {
		m.c.ambiguousMuts[m.s.name]--
		e := m.c.eventBy(m.s.name, kind, detail, "ambiguous", actor)
		m.notifyLocked(e, before)
		return errAmbiguous
	}
	e := m.c.eventBy(m.s.name, kind, detail, "", actor)
	m.notifyLocked(e, before)
	return nil
}

// observeLocked snapshots the site for the mutation observer; the zero
// value when no observer is installed, so trials without actors pay
// nothing. Must be called with c.mu held.
func (m *simChecker) observeLocked() SiteTruth {
	if m.c.onMutation == nil {
		return SiteTruth{}
	}
	return m.c.truthLocked(m.s)
}

// notifyLocked hands an applied mutation to the observer. Must be called
// with c.mu held.
func (m *simChecker) notifyLocked(e Event, before SiteTruth) {
	if m.c.onMutation == nil {
		return
	}
	m.c.onMutation(e, before, m.c.truthLocked(m.s))
}

func (m *simChecker) CheckReadOnly(_ context.Context) (bool, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return false, err
	}
	return m.s.readOnly, nil
}

// CheckSuperReadOnly lets the operator tell a fence from the read_only=ON
// boot, as the real checker does.
func (m *simChecker) CheckSuperReadOnly(_ context.Context) (bool, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return false, err
	}
	return m.s.superReadOnly, nil
}

func (m *simChecker) Promote(ctx context.Context) error {
	// STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only = 0.
	return m.mutate(ctx, EvPromote, "", func() error {
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

func (m *simChecker) SetSuperReadOnly(ctx context.Context, on bool) error {
	return m.mutate(ctx, EvSetSuperRO, fmt.Sprintf("on=%v", on), func() error {
		if on {
			m.s.superReadOnly = true
			m.s.readOnly = true // super_read_only=ON implies read_only=ON
		} else {
			m.s.superReadOnly = false
		}
		return nil
	})
}

func (m *simChecker) KillAppConnections(ctx context.Context) (int, error) {
	actor := actorFrom(ctx)
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return 0, err
	}
	// Consumes the crash countdown like any other statement, so the process
	// can die between fencing the old primary and draining the candidate.
	// The kill mutates no model state either way, but the outcome string
	// must still track the applied flag: the checker's death classification
	// splits the tallies on it.
	if m.c.crashNowLocked() {
		applied := !m.c.crashPreApply
		outcome := "operatorDied"
		if applied {
			outcome = "appliedThenOperatorDied"
		}
		m.c.eventBy(m.s.name, EvKillConns, "", outcome, actor)
		m.c.killOperatorLocked(m.s.name, EvKillConns, applied)
		return 0, errOperatorDead
	}
	// The kill is attributed per actor (a planned run's KILL on its target
	// is a RepeatedPromotion), and observed like a mutation so the planned
	// invariants see it, though it changes no model state.
	before := m.observeLocked()
	e := m.c.eventBy(m.s.name, EvKillConns, "", "", actor)
	m.notifyLocked(e, before)
	return 0, nil
}

func (m *simChecker) StopReplica(ctx context.Context) error {
	return m.mutate(ctx, EvStopReplica, "", func() error {
		m.s.ioWant = false
		m.s.sqlWant = false
		return nil
	})
}

func (m *simChecker) ResetReplicaAll(ctx context.Context) error {
	return m.mutate(ctx, EvResetReplica, "", func() error {
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

func (m *simChecker) SetReadOnly(ctx context.Context, on bool) error {
	return m.mutate(ctx, EvSetRO, fmt.Sprintf("on=%v", on), func() error {
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
	ioErr := m.s.ioErr
	if ioErr == "" && m.s.ioWant && !io {
		// A receiver retrying an unreachable source reports a connect
		// error, as real MySQL does, while its relay log stays applicable.
		ioErr = "error connecting to source '" + m.s.sourceHost + "'"
	}
	rs := &mysql.ReplicaStatus{
		IORunning:        io,
		SQLRunning:       sql,
		LastError:        firstNonEmpty(m.s.sqlErr, ioErr),
		LastIOError:      ioErr,
		LastSQLError:     m.s.sqlErr,
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

func (m *simChecker) ChangeReplicationSource(ctx context.Context, opts mysql.ReplicationSourceOpts) error {
	return m.mutate(ctx, EvChangeSource, "host="+opts.Host, func() error {
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

func (m *simChecker) StartReplica(ctx context.Context) error {
	return m.mutate(ctx, EvStartReplica, "", func() error {
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

// WaitForRelayLogDrain resolves instantly against the model, mirroring the
// real implementation's decision tree: abort on an applier error, otherwise
// apply the local relay backlog (restarting the SQL thread if needed). A
// receiver error does not abort: the relay log already fetched is applicable.
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
	if m.s.sqlErr != "" {
		m.c.event(m.s.name, EvDrain, "", "aborted")
		return fmt.Errorf("relay log drain aborted: SQL thread error: %s", m.s.sqlErr)
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

// errFlushFailed is an injected FLUSH BINARY LOGS failure: nothing rotated.
var errFlushFailed = errors.New("Error 1227: access denied (sim: FLUSH BINARY LOGS failed, nothing rotated)")

// FlushBinaryLogs rotates the site's modelled binary log. It is the
// operator's best-effort rotation (FailoverController.rotateBinlog), which
// calls it from a goroutine of its own and waits on the trial's fake clock;
// the caller is parked in that wait for as long as this runs, so the model
// still sees one actor at a time.
//
// The statement runs on a connection of its own in production, so here it
// reaches whichever server the site runs now, never fails a pinned session,
// and consumes neither the mutation-fault counters nor the mid-Execute
// crash countdown (a promotion statement's fault must not be spent on it).
// Its own faults:
//
//   - flushFail: an error, nothing rotated.
//   - flushSlow: the statement outlives the operator's bound. The call
//     advances the fake clock by controller.BinlogRotationTimeout, which
//     fires the operator's timer, and then blocks until the operator
//     cancels it. With flushLate the server completes the rotation at the
//     next tick, after the operator has moved on (the promotion may have
//     enabled writes by then); otherwise it never happens.
//
// Nothing happens to the model after the cancellation, so the abandoned
// goroutine cannot race the operator's next statement.
func (m *simChecker) FlushBinaryLogs(ctx context.Context) error {
	actor := actorFrom(ctx)
	m.c.mu.Lock()
	if err := m.reachableLocked(); err != nil {
		m.c.eventBy(m.s.name, EvFlushBinlog, "", "unreachable", actor)
		m.c.mu.Unlock()
		return err
	}
	if m.c.flushFail[m.s.name] > 0 {
		m.c.flushFail[m.s.name]--
		m.c.eventBy(m.s.name, EvFlushBinlog, "", "failed", actor)
		m.c.mu.Unlock()
		return errFlushFailed
	}
	if m.c.flushSlow[m.s.name] > 0 {
		m.c.flushSlow[m.s.name]--
		late := m.c.flushLate[m.s.name]
		if late {
			m.c.lateFlushes[m.s.name]++
		}
		m.c.eventBy(m.s.name, EvFlushBinlog, fmt.Sprintf("late=%v", late), "timeout", actor)
		advance := m.c.advanceClock
		m.c.mu.Unlock()
		if advance != nil {
			advance(controller.BinlogRotationTimeout)
		}
		<-ctx.Done()
		return ctx.Err()
	}
	before := m.observeLocked()
	m.c.rotateBinlogLocked(m.s)
	e := m.c.eventBy(m.s.name, EvFlushBinlog, fmt.Sprintf("file=%d prev=%s", m.s.binlogSeq, m.s.binlogPrev), "", actor)
	m.notifyLocked(e, before)
	m.c.mu.Unlock()
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

// userSchemaChecker mirrors the operator's unexported interface of the same
// name. It cannot be referenced directly across the package boundary, so this
// copy is the compile-time guard that simChecker keeps matching the shape the
// controller type-asserts for; the shapes must be changed together.
type userSchemaChecker interface {
	HasUserSchemas(context.Context) (bool, error)
}

// gtidSnapshotReader mirrors the operator's unexported interface of the same
// name, for the same reason.
type gtidSnapshotReader interface {
	ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error)
}

var (
	_ mysql.Checker      = (*simChecker)(nil)
	_ userSchemaChecker  = (*simChecker)(nil)
	_ gtidSnapshotReader = (*simChecker)(nil)
)

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

func (m *simChecker) CloneStatus(_ context.Context) (*mysql.CloneStatus, error) {
	return nil, nil
}

// PinSession pins a connection to the site's current server incarnation
// (see simSession). Pinning dials, so it fails like a connect when the site
// is unreachable.
func (m *simChecker) PinSession(ctx context.Context) (mysql.Session, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return nil, fmt.Errorf("pin connection: %w", err)
	}
	if a := actorFrom(ctx); a == ActorPF || a == ActorUpdate {
		m.c.eventBy(m.s.name, EvPinSession, fmt.Sprintf("incarnation=%d", m.s.incarnation), "", a)
	}
	return &simSession{m: &simChecker{c: m.c, s: m.s, pinned: true, pinInc: m.s.incarnation}}, nil
}

// ReadGTIDSnapshot reads @@server_uuid and gtid_executed in one statement,
// as the real checker does. The replica GTID audit and the planned
// failover's grant probe type-assert for it.
func (m *simChecker) ReadGTIDSnapshot(_ context.Context) (mysql.GTIDSnapshot, error) {
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if err := m.reachableLocked(); err != nil {
		return mysql.GTIDSnapshot{}, err
	}
	return mysql.GTIDSnapshot{ServerUUID: m.s.serverUUID, GtidExecuted: m.s.executed.String()}, nil
}

func (m *simChecker) EnsurePartialRevokes(_ context.Context) error { return nil }

func (m *simChecker) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
	return errors.New("dst: CLONE not modeled (bootstrap must stay disabled in trials)")
}
