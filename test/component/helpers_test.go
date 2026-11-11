package component

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// ---------------------------------------------------------------------------
// mockMySQL implements mysql.Checker with configurable behaviour.
//
// It models one Service address: the server behind it can be restarted
// (respawn) or replaced (replacePod), each of which starts a new server
// incarnation. Pooled calls reach whichever server is current; a pinned
// Session (PinSession) captures the incarnation at pin time and fails every
// call once it has moved, exactly as a pinned MySQL connection fails after
// its server is gone, however the address resolves now.
//
// Every statement runs through stmt, which applies the per-statement
// failure counters (failNext) and blocking calls (blockOn) before taking
// m.mu, so a test can hold an actor inside a statement — before it applies
// or after it applied but before it returns — while other actors act.
// ---------------------------------------------------------------------------

type mockMySQL struct {
	mu       sync.Mutex
	readOnly bool
	err      error
	promoted bool

	// superROFailN makes the next N SetSuperReadOnly calls fail without
	// applying, to exercise fence-retry behavior.
	superROFailN int
	// stopReplicaFailN makes the next N StopReplica calls fail, to abort a
	// promotion sequence partway.
	stopReplicaFailN int

	// Tracking fields for verification.
	superReadOnly         bool
	stoppedReplica        bool
	resetReplicaAll       bool
	replicationSourceSet  bool
	replicaStarted        bool
	cloneDonorList        string
	cloneInstanceCalled   bool
	cloneInstanceErr      error
	changeReplicationOpts mysql.ReplicationSourceOpts
	gtidExecuted          string
	killConnectionResults []int
	killConnectionCalls   int
	changeSourceCalls     int
	setReadOnlyFalseN     int
	setSuperROFalseN      int

	// replicaStatus is returned from ShowReplicaStatus. nil (default) means
	// "never had replication configured", which is the fresh-deploy signature.
	replicaStatus *mysql.ReplicaStatus

	// hasUserSchemas overrides HasUserSchemas when non-nil; the default
	// derives from gtidExecuted so existing fixtures keep their meaning
	// (data implies schemas, empty implies none).
	hasUserSchemas *bool

	// onClone, when set, runs inside CloneInstance (without m.mu held), so a
	// test can change another site's state while a clone is running.
	onClone func()

	// incarnation counts server processes behind the address (respawn and
	// replacePod start a new one); a pinned session fails once it moves.
	incarnation int
	// serverUUID is @@server_uuid of the current server (the pinned
	// session's ReadGTIDSnapshot, and the pooled one via withServerUUID).
	serverUUID string
	// clearChannelOnReset makes RESET REPLICA ALL remove the channel, so
	// ShowReplicaStatus returns nil afterwards, as MySQL does. Off by
	// default: older fixtures assert on the status after a reset.
	clearChannelOnReset bool
	// failNext makes the next N calls of a statement (by method name, e.g.
	// "SetReadOnly") fail without applying.
	failNext map[string]int
	// blocks are the pending blocking calls per statement, FIFO.
	blocks map[string][]*blockedCall
	// calls counts every statement call by method name, including failed
	// and lost ones.
	calls map[string]int
	// stopClearsThreads makes STOP REPLICA stop the reported threads, as
	// MySQL does. Off by default: older fixtures assert on the status after
	// a stop.
	stopClearsThreads bool
	// stmtLog records every applied mutation as "<Method>[:on|:off]", in
	// order (exactly-once assertions).
	stmtLog []string
	// binlogPrevGtids is the executed set at each applied FLUSH BINARY
	// LOGS (see FlushBinaryLogs).
	binlogPrevGtids []string
	// binlogFileErr fails RotateBinaryLog's read of the active file's
	// name after an applied flush.
	binlogFileErr error
	// rotateBinaryLogFn, when set, is RotateBinaryLog's body (run without
	// the mock's lock), so a test can make the pre-reclone rotation hang.
	rotateBinaryLogFn func(ctx context.Context) (string, error)
}

// mutatingStmt are the statements stmtLog records by name; the read-only
// setters record themselves with their value ("SetReadOnly:off").
var mutatingStmt = map[string]bool{
	"Promote": true, "StopReplica": true, "ResetReplicaAll": true,
	"ChangeReplicationSource": true, "StartReplica": true, "KillAppConnections": true,
}

func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

// statements returns the applied mutations so far (see stmtLog).
func (m *mockMySQL) statements() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.stmtLog...)
}

// errMockSessionLost is what a pinned session's call sees once its server
// is gone (go-sql-driver/mysql: "invalid connection").
var errMockSessionLost = errors.New("invalid connection (mock: the pinned session's server is gone)")

// errMockStatementFailed is a failNext failure.
var errMockStatementFailed = errors.New("transient: i/o timeout (mock: statement not applied)")

// blockedCall holds the next call of one statement inside the statement
// until Release. Entered is closed when the call arrives.
type blockedCall struct {
	afterApply bool
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

// Entered is closed once the blocked call is inside the statement.
func (b *blockedCall) Entered() <-chan struct{} { return b.entered }

// Release lets the blocked call continue. Safe to call more than once.
func (b *blockedCall) Release() { b.once.Do(func() { close(b.release) }) }

func (b *blockedCall) hold() {
	close(b.entered)
	<-b.release
}

// blockOn makes the next call of stmt block inside the statement: before it
// applies (afterApply false) or after it applied and before it returns
// (afterApply true). The block is released at test cleanup if the test did
// not release it.
func (m *mockMySQL) blockOn(t *testing.T, stmt string, afterApply bool) *blockedCall {
	t.Helper()
	b := &blockedCall{afterApply: afterApply, entered: make(chan struct{}), release: make(chan struct{})}
	m.mu.Lock()
	if m.blocks == nil {
		m.blocks = map[string][]*blockedCall{}
	}
	m.blocks[stmt] = append(m.blocks[stmt], b)
	m.mu.Unlock()
	t.Cleanup(b.Release)
	return b
}

// failNextCalls makes the next n calls of stmt fail without applying.
func (m *mockMySQL) failNextCalls(stmt string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext == nil {
		m.failNext = map[string]int{}
	}
	m.failNext[stmt] += n
}

// callCount returns how many times stmt was called.
func (m *mockMySQL) callCount(stmt string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[stmt]
}

// stmt runs one statement: pin and failure checks, an optional block before
// apply, apply under m.mu, and an optional block after apply. pin is the
// pinned session's incarnation (nil for a pooled call). A replacement while
// blocked before apply keeps the statement from applying; after apply, the
// statement landed and its reply is lost.
func (m *mockMySQL) stmt(name string, pin *int, apply func() error) error {
	m.mu.Lock()
	if m.calls == nil {
		m.calls = map[string]int{}
	}
	m.calls[name]++
	if pin != nil && *pin != m.incarnation {
		m.mu.Unlock()
		return errMockSessionLost
	}
	if m.failNext[name] > 0 {
		m.failNext[name]--
		m.mu.Unlock()
		return errMockStatementFailed
	}
	var b *blockedCall
	if q := m.blocks[name]; len(q) > 0 {
		b, m.blocks[name] = q[0], q[1:]
	}
	m.mu.Unlock()

	if b != nil && !b.afterApply {
		b.hold()
	}
	m.mu.Lock()
	if pin != nil && *pin != m.incarnation {
		m.mu.Unlock()
		return errMockSessionLost
	}
	err := apply()
	if err == nil && mutatingStmt[name] {
		m.stmtLog = append(m.stmtLog, name)
	}
	m.mu.Unlock()
	if b != nil && b.afterApply {
		b.hold()
		m.mu.Lock()
		lost := pin != nil && *pin != m.incarnation
		m.mu.Unlock()
		if lost {
			return errMockSessionLost
		}
	}
	return err
}

// HasUserSchemas satisfies the operator's userSchemaChecker assertion so the
// production schema-based emptiness paths are exercised, not just the GTID
// fallback.
func (m *mockMySQL) HasUserSchemas(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hasUserSchemas != nil {
		return *m.hasUserSchemas, nil
	}
	return m.gtidExecuted != "", nil
}

func (m *mockMySQL) CheckReadOnly(_ context.Context) (bool, error) { return m.checkReadOnly(nil) }

func (m *mockMySQL) checkReadOnly(pin *int) (ro bool, err error) {
	err = m.stmt("CheckReadOnly", pin, func() error {
		ro = m.readOnly
		return m.err
	})
	return ro, err
}

func (m *mockMySQL) Promote(_ context.Context) error {
	return m.stmt("Promote", nil, func() error {
		m.promoted = true
		m.readOnly = false
		return nil
	})
}

func (m *mockMySQL) Close() error { return nil }

func (m *mockMySQL) SetSuperReadOnly(_ context.Context, on bool) error {
	return m.setSuperReadOnly(nil, on)
}

func (m *mockMySQL) setSuperReadOnly(pin *int, on bool) error {
	return m.stmt("SetSuperReadOnly", pin, func() error {
		if m.superROFailN > 0 {
			m.superROFailN--
			return errors.New("transient: connection reset by peer")
		}
		m.superReadOnly = on
		if on {
			m.readOnly = true
		} else {
			m.setSuperROFalseN++
		}
		m.stmtLog = append(m.stmtLog, "SetSuperReadOnly:"+onOff(on))
		return nil
	})
}

func (m *mockMySQL) KillAppConnections(_ context.Context) (killed int, err error) {
	err = m.stmt("KillAppConnections", nil, func() error {
		i := m.killConnectionCalls
		m.killConnectionCalls++
		if i < len(m.killConnectionResults) {
			killed = m.killConnectionResults[i]
		}
		return nil
	})
	return killed, err
}

func (m *mockMySQL) StopReplica(_ context.Context) error { return m.stopReplica(nil) }

func (m *mockMySQL) stopReplica(pin *int) error {
	return m.stmt("StopReplica", pin, func() error {
		if m.stopReplicaFailN > 0 {
			m.stopReplicaFailN--
			return errors.New("transient: i/o timeout")
		}
		m.stoppedReplica = true
		if m.stopClearsThreads && m.replicaStatus != nil {
			m.replicaStatus.IORunning = false
			m.replicaStatus.SQLRunning = false
		}
		return nil
	})
}

func (m *mockMySQL) ResetReplicaAll(_ context.Context) error { return m.resetReplica(nil) }

func (m *mockMySQL) resetReplica(pin *int) error {
	return m.stmt("ResetReplicaAll", pin, func() error {
		m.resetReplicaAll = true
		if m.clearChannelOnReset {
			m.replicaStatus = nil
		}
		return nil
	})
}

func (m *mockMySQL) SetReadOnly(_ context.Context, on bool) error { return m.setReadOnlyStmt(nil, on) }

func (m *mockMySQL) setReadOnlyStmt(pin *int, on bool) error {
	return m.stmt("SetReadOnly", pin, func() error {
		m.readOnly = on
		if !on {
			// As in MySQL: clearing read_only clears super_read_only too.
			m.superReadOnly = false
			m.setReadOnlyFalseN++
		}
		m.stmtLog = append(m.stmtLog, "SetReadOnly:"+onOff(on))
		return nil
	})
}

// CheckSuperReadOnly lets the operator tell a fence (super_read_only=ON)
// from the read_only=ON mysqld boots with.
func (m *mockMySQL) CheckSuperReadOnly(_ context.Context) (bool, error) {
	return m.checkSuperReadOnly(nil)
}

func (m *mockMySQL) checkSuperReadOnly(pin *int) (sro bool, err error) {
	err = m.stmt("CheckSuperReadOnly", pin, func() error {
		sro = m.superReadOnly
		return m.err
	})
	return sro, err
}

func (m *mockMySQL) writableGrants() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setReadOnlyFalseN + m.setSuperROFalseN
}

// ShowReplicaStatus returns a deep copy of the replica status.
//
// Returning m.replicaStatus directly hands the caller a pointer into state that
// StartReplica and ChangeReplicationSource mutate under m.mu. The caller reads
// those fields outside the lock, so the mutex protects the pointer but not the
// pointee, and -race flags it once bootstrap runs on its own goroutine (see
// TestBootstrap_FailAndRetry). Copying keeps the mock's locking honest.
func (m *mockMySQL) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	return m.showReplicaStatus(nil)
}

func (m *mockMySQL) showReplicaStatus(pin *int) (out *mysql.ReplicaStatus, err error) {
	err = m.stmt("ShowReplicaStatus", pin, func() error {
		if m.replicaStatus == nil {
			return nil
		}
		status := *m.replicaStatus
		if m.replicaStatus.SecondsBehindSource != nil {
			lag := *m.replicaStatus.SecondsBehindSource
			status.SecondsBehindSource = &lag
		}
		out = &status
		return nil
	})
	return out, err
}

func (m *mockMySQL) ChangeReplicationSource(_ context.Context, opts mysql.ReplicationSourceOpts) error {
	return m.changeSource(nil, opts)
}

func (m *mockMySQL) changeSource(pin *int, opts mysql.ReplicationSourceOpts) error {
	return m.stmt("ChangeReplicationSource", pin, func() error {
		m.replicationSourceSet = true
		m.changeSourceCalls++
		m.changeReplicationOpts = opts
		if m.replicaStatus == nil {
			m.replicaStatus = &mysql.ReplicaStatus{}
		}
		m.replicaStatus.SourceHost = opts.Host
		return nil
	})
}

func (m *mockMySQL) StartReplica(_ context.Context) error { return m.startReplica(nil) }

func (m *mockMySQL) startReplica(pin *int) error {
	return m.stmt("StartReplica", pin, func() error {
		m.replicaStarted = true
		if m.replicaStatus != nil {
			m.replicaStatus.IORunning = true
			m.replicaStatus.SQLRunning = true
		}
		return nil
	})
}

func (m *mockMySQL) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	return nil
}

func (m *mockMySQL) EnsureClonePlugin(_ context.Context) error {
	return nil
}

func (m *mockMySQL) SetCloneDonorList(_ context.Context, donor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cloneDonorList = donor
	return nil
}

// FlushBinaryLogs runs through stmt like every statement (failNextCalls
// and blockOn apply), on no pinned connection: the real checker dials a
// connection of its own for it. A call blocked before apply returns the
// context's error once ctx ends, as a cancelled statement does. Each flush
// that applies records the executed set it found, which is the
// Previous_gtids header of the file it opened.
func (m *mockMySQL) FlushBinaryLogs(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- m.stmt("FlushBinaryLogs", nil, func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.binlogPrevGtids = append(m.binlogPrevGtids, m.gtidExecuted)
			return nil
		})
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RotateBinaryLog is FlushBinaryLogs plus the active file's name: the mock
// numbers its files from binlog.000001, one more per applied flush.
// binlogFileErr fails the name read of an acknowledged flush.
func (m *mockMySQL) RotateBinaryLog(ctx context.Context) (string, error) {
	m.mu.Lock()
	if m.calls == nil {
		m.calls = map[string]int{}
	}
	m.calls["RotateBinaryLog"]++
	fn := m.rotateBinaryLogFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	if err := m.FlushBinaryLogs(ctx); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.binlogFileErr != nil {
		return "", m.binlogFileErr
	}
	return fmt.Sprintf("binlog.%06d", 1+len(m.binlogPrevGtids)), nil
}

// flushedAt returns the executed set each applied FLUSH BINARY LOGS found.
func (m *mockMySQL) flushedAt() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.binlogPrevGtids...)
}

func (m *mockMySQL) GetGtidExecuted(_ context.Context) (string, error) { return m.getGtidExecuted(nil) }

func (m *mockMySQL) getGtidExecuted(pin *int) (gtid string, err error) {
	err = m.stmt("GetGtidExecuted", pin, func() error {
		gtid = m.gtidExecuted
		return nil
	})
	return gtid, err
}

func (m *mockMySQL) readGTIDSnapshot(pin *int) (snap mysql.GTIDSnapshot, err error) {
	err = m.stmt("ReadGTIDSnapshot", pin, func() error {
		snap = mysql.GTIDSnapshot{ServerUUID: m.serverUUID, GtidExecuted: m.gtidExecuted}
		return m.err
	})
	return snap, err
}

func (m *mockMySQL) CloneStatus(_ context.Context) (*mysql.CloneStatus, error) {
	return nil, nil
}

// PinSession pins the current server incarnation (see mockSession).
func (m *mockMySQL) PinSession(context.Context) (mysql.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &mockSession{m: m, inc: m.incarnation}, nil
}

func (m *mockMySQL) EnsurePartialRevokes(_ context.Context) error { return nil }

func (m *mockMySQL) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
	m.mu.Lock()
	m.cloneInstanceCalled = true
	err, onClone := m.cloneInstanceErr, m.onClone
	m.mu.Unlock()
	if onClone != nil {
		onClone()
	}
	return err
}

// setReadOnly sets read-only state and clears any error.
func (m *mockMySQL) setReadOnly(ro bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly = ro
	m.err = nil
}

// setError makes subsequent CheckReadOnly calls return this error.
func (m *mockMySQL) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// isPromoted returns whether Promote was called.
func (m *mockMySQL) isPromoted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.promoted
}

// isReadOnly returns the current readOnly state.
func (m *mockMySQL) isReadOnly() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly
}

// isSuperReadOnly returns the current superReadOnly state.
func (m *mockMySQL) isSuperReadOnly() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.superReadOnly
}

// failNextSuperRO makes the next n SetSuperReadOnly calls fail transiently.
func (m *mockMySQL) failNextSuperRO(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.superROFailN = n
}

// failNextStopReplica makes the next n StopReplica calls fail transiently.
func (m *mockMySQL) failNextStopReplica(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopReplicaFailN = n
}

// respawn simulates a MySQL restart: reachable again, read_only per the
// server config, super_read_only cleared (it is never persisted). It is a
// new server incarnation, so pinned sessions from before it fail.
func (m *mockMySQL) respawn(readOnly bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = nil
	m.readOnly = readOnly
	m.superReadOnly = false
	m.incarnation++
}

// podReplacement describes the server a replacePod brings up.
type podReplacement struct {
	// serverUUID is the new server's @@server_uuid; "" keeps the current
	// one (the same data directory on a new pod).
	serverUUID string
	// gtidExecuted, when non-nil, replaces the executed set (a restored or
	// regressed data directory).
	gtidExecuted *string
	// noChannel boots with no replication channel configured.
	noChannel bool
}

// replacePod replaces the server behind the address: a new incarnation
// booted the way mysqld boots, read_only=ON and super_read_only OFF,
// reachable, with its replica threads running when it has a channel.
func (m *mockMySQL) replacePod(p podReplacement) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incarnation++
	m.err = nil
	m.readOnly = true
	m.superReadOnly = false
	if p.serverUUID != "" {
		m.serverUUID = p.serverUUID
	}
	if p.gtidExecuted != nil {
		m.gtidExecuted = *p.gtidExecuted
	}
	switch {
	case p.noChannel:
		m.replicaStatus = nil
	case m.replicaStatus != nil:
		m.replicaStatus.IORunning = true
		m.replicaStatus.SQLRunning = true
	}
}

// dropConnections breaks every connection to the server without changing
// its state (a network blip, a proxy restart): pinned sessions fail from
// their next call, pooled calls redial.
func (m *mockMySQL) dropConnections() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incarnation++
}

// setRO sets read_only and super_read_only directly, as an actor outside
// the operator would (a human, the sidecar).
func (m *mockMySQL) setRO(ro, sro bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly, m.superReadOnly = ro, sro
}

// setServerUUID sets @@server_uuid of the current server.
func (m *mockMySQL) setServerUUID(uuid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.serverUUID = uuid
}

// setGtidExecuted replaces the reported GTID_EXECUTED set.
func (m *mockMySQL) setGtidExecuted(gtid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gtidExecuted = gtid
}

// gtidWithExtras appends uuid:from-to to a GTID set: own extras when uuid is
// the server's own, foreign extras otherwise.
func gtidWithExtras(base, uuid string, from, to int64) string {
	extra := fmt.Sprintf("%s:%d-%d", uuid, from, to)
	if from == to {
		extra = fmt.Sprintf("%s:%d", uuid, from)
	}
	if base == "" {
		return extra
	}
	return base + "," + extra
}

// mockWithUUID adds the pooled ReadGTIDSnapshot to a mockMySQL. It is a
// separate type because the operator type-asserts for it (the replica GTID
// audit, the planned failover's grant probe); adding it to every mockMySQL
// would change the audit path of every existing test.
type mockWithUUID struct{ *mockMySQL }

// withServerUUID returns m as a checker whose pooled ReadGTIDSnapshot
// reports uuid.
func withServerUUID(m *mockMySQL, uuid string) mockWithUUID {
	m.setServerUUID(uuid)
	return mockWithUUID{m}
}

func (m mockWithUUID) ReadGTIDSnapshot(_ context.Context) (mysql.GTIDSnapshot, error) {
	return m.readGTIDSnapshot(nil)
}

// mockSession is mockMySQL's pinned Session: every call is on the server
// incarnation pinned at PinSession and fails with errMockSessionLost once
// the server is gone.
type mockSession struct {
	m      *mockMySQL
	inc    int
	closed bool
}

var _ mysql.Session = (*mockSession)(nil)

var errMockSessionClosed = errors.New("sql: connection is already closed")

func (s *mockSession) pin() (*int, error) {
	if s.closed {
		return nil, errMockSessionClosed
	}
	return &s.inc, nil
}

func (s *mockSession) StopReplica(context.Context) error {
	pin, err := s.pin()
	if err != nil {
		return err
	}
	return s.m.stopReplica(pin)
}

func (s *mockSession) ResetReplicaAll(context.Context) error {
	pin, err := s.pin()
	if err != nil {
		return err
	}
	return s.m.resetReplica(pin)
}

func (s *mockSession) GetGtidExecuted(context.Context) (string, error) {
	pin, err := s.pin()
	if err != nil {
		return "", err
	}
	return s.m.getGtidExecuted(pin)
}

func (s *mockSession) SetSuperReadOnly(_ context.Context, on bool) error {
	pin, err := s.pin()
	if err != nil {
		return err
	}
	return s.m.setSuperReadOnly(pin, on)
}

func (s *mockSession) SetReadOnly(_ context.Context, on bool) error {
	pin, err := s.pin()
	if err != nil {
		return err
	}
	return s.m.setReadOnlyStmt(pin, on)
}

func (s *mockSession) CheckReadOnly(context.Context) (bool, error) {
	pin, err := s.pin()
	if err != nil {
		return false, err
	}
	return s.m.checkReadOnly(pin)
}

func (s *mockSession) CheckSuperReadOnly(context.Context) (bool, error) {
	pin, err := s.pin()
	if err != nil {
		return false, err
	}
	return s.m.checkSuperReadOnly(pin)
}

func (s *mockSession) ShowReplicaStatus(context.Context) (*mysql.ReplicaStatus, error) {
	pin, err := s.pin()
	if err != nil {
		return nil, err
	}
	return s.m.showReplicaStatus(pin)
}

func (s *mockSession) ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error) {
	pin, err := s.pin()
	if err != nil {
		return mysql.GTIDSnapshot{}, err
	}
	return s.m.readGTIDSnapshot(pin)
}

func (s *mockSession) StartReplica(context.Context) error {
	pin, err := s.pin()
	if err != nil {
		return err
	}
	return s.m.startReplica(pin)
}

func (s *mockSession) ChangeReplicationSource(_ context.Context, opts mysql.ReplicationSourceOpts) error {
	pin, err := s.pin()
	if err != nil {
		return err
	}
	return s.m.changeSource(pin, opts)
}

// Close releases the connection; later calls fail.
func (s *mockSession) Close() error {
	s.closed = true
	return nil
}

// ---------------------------------------------------------------------------
// mockTainter implements platform.NodeTainter.
// ---------------------------------------------------------------------------

type mockTainter struct {
	mu     sync.Mutex
	taints map[string]bool // selector -> tainted
}

func newMockTainter() *mockTainter {
	return &mockTainter{taints: make(map[string]bool)}
}

func (m *mockTainter) SetTaint(_ context.Context, selector, _, _ string, taint bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taints[selector] = taint
	return nil
}

func (m *mockTainter) isTainted(selector string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taints[selector]
}

// ---------------------------------------------------------------------------
// mockDNS implements platform.DNSUpdater.
// ---------------------------------------------------------------------------

type mockDNS struct {
	mu     sync.Mutex
	lastIP string
	calls  int
	err    error
}

func (m *mockDNS) UpdateDNSRecord(_ context.Context, ip string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.lastIP = ip
	m.calls++
	return nil
}

func (m *mockDNS) getLastIP() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIP
}

func (m *mockDNS) getCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// ---------------------------------------------------------------------------
// testHarness wires up all components for integration testing.
// ---------------------------------------------------------------------------

type testHarness struct {
	tm       *controller.TopologyManager
	dc1MySQL *mockMySQL
	dc2MySQL *mockMySQL
	tainter  *mockTainter
	dns      *mockDNS
	hub      *platform.Hub
	logger   *slog.Logger
	clock    *clock.FakeClock
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()
	return newTestHarnessWithMySQL(t, &mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
}

func newTestHarnessWithMySQL(t *testing.T, dc1, dc2 *mockMySQL) *testHarness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	cfg := controller.TopologyConfig{
		Name:              "lion",
		Sites:             defaultTwoSiteConfig(),
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		// The API default; the manager uses the config as given (A-37).
		FailoverCooldown: int64(5 * time.Minute),
	}

	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)

	return &testHarness{
		tm:       tm,
		dc1MySQL: dc1,
		dc2MySQL: dc2,
		tainter:  tainter,
		dns:      dns,
		hub:      hub,
		logger:   logger,
		clock:    clk,
	}
}

// defaultTwoSiteConfig returns the canonical dc1/dc2 site slice used by
// the component test harness helpers.
func defaultTwoSiteConfig() []controller.SiteTopologyConfig {
	return []controller.SiteTopologyConfig{
		{Name: "dc1", Zone: "lion-dc1", LoadBalancerIP: "1.1.1.1", Role: state.SiteRolePrimaryCandidate,
			TaintSelector: taintSelector("dc1"), Host: "mysql-lion-dc1.default.svc.cluster.local"},
		{Name: "dc2", Zone: "lion-dc2", LoadBalancerIP: "2.2.2.2", Role: state.SiteRolePrimaryCandidate,
			TaintSelector: taintSelector("dc2"), Host: "mysql-lion-dc2.default.svc.cluster.local"},
	}
}

func taintSelector(siteName string) string {
	return "bloodraven.shipstream.io/failover-group.lion=true,bloodraven.shipstream.io/site.lion=" + siteName
}

// newTestHarnessWithPriorities creates a harness where both sites start
// writable and spec.splitBrainPolicy.sitePriorities is set to the given list.
func newTestHarnessWithPriorities(t *testing.T, priorities []string) *testHarness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: false}
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	cfg := controller.TopologyConfig{
		Name:              "lion",
		Sites:             defaultTwoSiteConfig(),
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		FailoverCooldown:  0,
		SitePriorities:    priorities,
	}

	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)

	return &testHarness{
		tm:       tm,
		dc1MySQL: dc1,
		dc2MySQL: dc2,
		tainter:  tainter,
		dns:      dns,
		hub:      hub,
		logger:   logger,
		clock:    clk,
	}
}

// newTestHarnessWithCooldown creates a harness with a specified failover cooldown.
func newTestHarnessWithCooldown(t *testing.T, cooldown time.Duration) *testHarness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	cfg := controller.TopologyConfig{
		Name:              "lion",
		Sites:             defaultTwoSiteConfig(),
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		FailoverCooldown:  int64(cooldown),
	}

	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)

	return &testHarness{
		tm:       tm,
		dc1MySQL: dc1,
		dc2MySQL: dc2,
		tainter:  tainter,
		dns:      dns,
		hub:      hub,
		logger:   logger,
		clock:    clk,
	}
}

// pollN runs n poll cycles synchronously.
func (h *testHarness) pollN(n int) {
	ctx := context.Background()
	for i := 0; i < n; i++ {
		h.tm.Poll(ctx)
	}
}

// errDown is a convenience error for simulating a down MySQL instance.
var errDown = errors.New("connection refused")

// ---------------------------------------------------------------------------
// mockSidecarMySQL implements sidecar.Fencer for partition tests.
// ---------------------------------------------------------------------------

type mockSidecarMySQL struct {
	mu            sync.Mutex
	readOnly      bool
	superReadOnly bool
}

func newMockSidecarMySQL(readOnly bool) *mockSidecarMySQL {
	return &mockSidecarMySQL{readOnly: readOnly}
}

func (m *mockSidecarMySQL) IsReadOnly(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly, nil
}

func (m *mockSidecarMySQL) IsSuperReadOnly(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.superReadOnly, nil
}

func (m *mockSidecarMySQL) SetSuperReadOnly(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.superReadOnly = true
	m.readOnly = true
	return nil
}

func (m *mockSidecarMySQL) KillConnections(_ context.Context) (int, error) { return 0, nil }

func (m *mockSidecarMySQL) isSuperReadOnly() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.superReadOnly
}

// ---------------------------------------------------------------------------
// startMockHTTPServer starts a simple test HTTP server that responds 200 OK
// on the given path. Returns the "host:port" address.
// ---------------------------------------------------------------------------

func startMockHTTPServer(t *testing.T, path string) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// httptest.Server URL is "http://host:port"; extract "host:port".
	return srv.Listener.Addr().String()
}
