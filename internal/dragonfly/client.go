package dragonfly

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"time"
)

// DefaultDialTimeout is the connection-establishment timeout used when
// none is provided by the caller. Kept short because the operator polls
// each site frequently and a stuck dial should never hold the reconcile
// loop hostage.
const DefaultDialTimeout = 3 * time.Second

// DefaultIOTimeout is the per-operation read/write deadline used when
// none is provided by the caller. Bounded so the operator can recover
// from pathologically slow Dragonfly responses.
const DefaultIOTimeout = 5 * time.Second

// Config configures a Client.
type Config struct {
	// Addr is "host:port".
	Addr string

	// Password is the AUTH password. Empty means no AUTH.
	Password string

	// DialTimeout caps the time spent establishing a connection. Zero
	// means DefaultDialTimeout.
	DialTimeout time.Duration

	// IOTimeout caps per-operation read+write latency. Zero means
	// DefaultIOTimeout.
	IOTimeout time.Duration
}

// Client is a single-connection Dragonfly client. Clients are not safe
// for concurrent use; callers should create one per logical caller (the
// DragonflyManager creates one per site per poll).
//
// Lifecycle: New -> [methods]* -> Close. Close is idempotent.
type Client struct {
	cfg  Config
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

// New dials a fresh connection and authenticates if a password is set.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("dragonfly: empty addr")
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = DefaultDialTimeout
	}
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("dragonfly: dial %s: %w", cfg.Addr, err)
	}
	c := &Client{
		cfg:  cfg,
		conn: conn,
		br:   bufio.NewReader(conn),
		bw:   bufio.NewWriter(conn),
	}
	if cfg.Password != "" {
		if err := c.exec(ctx, "AUTH", cfg.Password); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("dragonfly: AUTH: %w", err)
		}
	}
	return c, nil
}

// Close releases the underlying connection. Safe to call multiple times.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// Ping issues a PING and expects PONG.
func (c *Client) Ping(ctx context.Context) error {
	reply, err := c.execReply(ctx, "PING")
	if err != nil {
		return err
	}
	if reply != "PONG" {
		return fmt.Errorf("dragonfly: unexpected PING reply %q", reply)
	}
	return nil
}

// InfoReplication issues `INFO replication` and parses the body.
func (c *Client) InfoReplication(ctx context.Context) (ReplicationInfo, error) {
	body, err := c.execReply(ctx, "INFO", "replication")
	if err != nil {
		return ReplicationInfo{}, err
	}
	return ParseInfoReplication(body), nil
}

// InfoPersistence issues `INFO persistence` and parses the body.
func (c *Client) InfoPersistence(ctx context.Context) (PersistenceInfo, error) {
	body, err := c.execReply(ctx, "INFO", "persistence")
	if err != nil {
		return PersistenceInfo{}, err
	}
	return ParseInfoPersistence(body), nil
}

// ReplicaOf wires this Dragonfly to follow the given master endpoint.
// Uses the modern REPLICAOF verb (Dragonfly accepts SLAVEOF as an alias
// but REPLICAOF is the canonical form).
func (c *Client) ReplicaOf(ctx context.Context, host string, port int32) error {
	return c.exec(ctx, "REPLICAOF", host, strconv.Itoa(int(port)))
}

// ReplicaOfNoOne breaks the replication link and turns this Dragonfly
// back into a standalone master. Used as a fallback promotion primitive
// when REPLTAKEOVER is unavailable or fails.
func (c *Client) ReplicaOfNoOne(ctx context.Context) error {
	return c.exec(ctx, "REPLICAOF", "NO", "ONE")
}

// ReplTakeover atomically promotes this replica to master, waiting up
// to `timeout` for the replication stream to drain before promoting.
// This is the canonical planned-promotion primitive on Dragonfly v1.x+.
//
// timeout is rounded up to the next millisecond and clamped to a
// non-zero positive value (Dragonfly rejects 0).
//
// The per-call client read deadline is set to timeout + 5s so that the
// client does not time out before the server has used its full drain
// budget. Otherwise the client could give up while the server is still
// (or has just) accepted the takeover, leaving the connection in an
// unknown state and the caller unsure whether promotion happened.
func (c *Client) ReplTakeover(ctx context.Context, timeout time.Duration) error {
	ms := timeout.Milliseconds()
	if ms <= 0 {
		ms = 1
	}
	_, err := c.execReplyWithIOTimeout(ctx, timeout+5*time.Second, "REPLTAKEOVER", strconv.FormatInt(ms, 10))
	return err
}

// ClientKillType issues `CLIENT KILL TYPE <kind>` against the connected
// instance, which terminates all client connections of the given kind.
// Used after a planned-failover Dragonfly promotion to force application
// clients off the (now-demoted) old master so they reconnect through the
// active Service and land on the new master.
//
// Best-effort: callers ignore errors from this call. Dragonfly returns
// the count of killed connections; the count is discarded — the operator
// only cares that the kick was attempted.
func (c *Client) ClientKillType(ctx context.Context, kind string) error {
	return c.exec(ctx, "CLIENT", "KILL", "TYPE", kind)
}

// exec sends a command and expects "+OK\r\n" or any non-error reply.
func (c *Client) exec(ctx context.Context, args ...string) error {
	_, err := c.execReply(ctx, args...)
	return err
}

// execReply sends a command and returns the reply body using the
// configured per-call IOTimeout.
func (c *Client) execReply(ctx context.Context, args ...string) (string, error) {
	return c.execReplyWithIOTimeout(ctx, c.cfg.IOTimeout, args...)
}

// execReplyWithIOTimeout is the core write-then-read primitive. Callers
// that need a different read deadline than DefaultIOTimeout — e.g.
// ReplTakeover, which legitimately blocks for tens of seconds while the
// server drains replication — pass an explicit ioTimeout. Zero or
// negative ioTimeout falls back to DefaultIOTimeout.
//
// ctx cancellation is honored mid-I/O: a watchdog goroutine fires
// SetDeadline(now) on cancel so a blocked read or write unwinds to a
// timeout error promptly instead of waiting for the full ioTimeout.
func (c *Client) execReplyWithIOTimeout(ctx context.Context, ioTimeout time.Duration, args ...string) (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("dragonfly: client closed")
	}
	if ioTimeout <= 0 {
		ioTimeout = DefaultIOTimeout
	}
	deadline := time.Now().Add(ioTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return "", err
	}
	stopWatch := c.watchCancel(ctx)
	defer stopWatch()
	if err := writeCommand(c.bw, args...); err != nil {
		return "", err
	}
	return readReply(c.br)
}

// watchCancel arms a goroutine that aborts the in-flight I/O on
// ctx.Done(). Returns a stop function the caller defers to release the
// goroutine on the happy path. The abort sets a deadline in the past,
// which causes any pending Read/Write on the underlying conn to return
// promptly with a deadline-exceeded error.
func (c *Client) watchCancel(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			if c.conn != nil {
				_ = c.conn.SetDeadline(time.Unix(1, 0))
			}
		case <-stop:
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}
