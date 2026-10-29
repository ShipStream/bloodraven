package testutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// FakeEndpoint is one MySQL address (a per-site Service) served by
// whichever FakeReplica currently backs it. Switch models a pod
// replacement: new connections reach the replacement, and every connection
// opened before the switch breaks, as the old pod's TCP connections do.
// Break drops the open connections without changing the server (a network
// blip). Broken connections report driver.ErrBadConn before running
// anything, so a database/sql pool silently retries on a fresh connection
// while a pinned *sql.Conn fails, which is the difference
// mysql.Checker.PinSession exists for.
type FakeEndpoint struct {
	mu      sync.Mutex
	current *FakeReplica
	gen     int
	hook    func(query string)
}

// NewFakeEndpoint returns an endpoint served by r.
func NewFakeEndpoint(r *FakeReplica) *FakeEndpoint {
	return &FakeEndpoint{current: r}
}

// Checker returns a real mysql.Checker connected to this endpoint.
func (e *FakeEndpoint) Checker() mysql.Checker {
	return mysql.NewCheckerFromPools(sql.OpenDB(e), sql.OpenDB(e))
}

// Switch routes new connections to r and breaks every open one.
func (e *FakeEndpoint) Switch(r *FakeReplica) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.current = r
	e.gen++
}

// Break drops every open connection; new ones reach the same server.
func (e *FakeEndpoint) Break() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.gen++
}

// OnStatement registers fn to run after each statement or query a live
// connection completes, so a test can switch or break the endpoint at an
// exact point in a sequence.
func (e *FakeEndpoint) OnStatement(fn func(query string)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hook = fn
}

// Connect implements driver.Connector.
func (e *FakeEndpoint) Connect(ctx context.Context) (driver.Conn, error) {
	e.mu.Lock()
	r, gen := e.current, e.gen
	e.mu.Unlock()
	c, err := r.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &endpointConn{e: e, gen: gen, conn: c.(*fakeReplicaConn)}, nil
}

// Driver implements driver.Connector.
func (e *FakeEndpoint) Driver() driver.Driver { return fakeEndpointDriver{} }

type fakeEndpointDriver struct{}

func (fakeEndpointDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake endpoint: open through sql.OpenDB(*FakeEndpoint)")
}

type endpointConn struct {
	e    *FakeEndpoint
	gen  int
	conn *fakeReplicaConn
}

func (c *endpointConn) live() (func(string), bool) {
	c.e.mu.Lock()
	defer c.e.mu.Unlock()
	return c.e.hook, c.gen == c.e.gen
}

func (c *endpointConn) Prepare(query string) (driver.Stmt, error) { return c.conn.Prepare(query) }
func (c *endpointConn) Close() error                              { return c.conn.Close() }
func (c *endpointConn) Begin() (driver.Tx, error)                 { return c.conn.Begin() }

// ResetSession lets the pool discard a broken connection before reuse.
func (c *endpointConn) ResetSession(context.Context) error {
	if _, ok := c.live(); !ok {
		return driver.ErrBadConn
	}
	return nil
}

func (c *endpointConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if _, ok := c.live(); !ok {
		return nil, driver.ErrBadConn
	}
	res, err := c.conn.ExecContext(ctx, query, args)
	if hook, _ := c.live(); hook != nil && err == nil {
		hook(query)
	}
	return res, err
}

func (c *endpointConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if _, ok := c.live(); !ok {
		return nil, driver.ErrBadConn
	}
	rows, err := c.conn.QueryContext(ctx, query, args)
	if hook, _ := c.live(); hook != nil && err == nil {
		hook(query)
	}
	return rows, err
}
