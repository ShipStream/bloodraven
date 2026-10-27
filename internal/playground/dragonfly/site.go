// Package dragonfly opens an authenticated Dragonfly client to a
// per-site Dragonfly pod in the playground via a port-forwarded SPDY
// tunnel. Reuses the operator's RESP framing where the surface
// matches; adds the small set of read/write commands the chaos
// scenarios need (SET/GET/INCR/DBSIZE) so we can prove session
// preservation across a planned failover.
package dragonfly

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"

	dfclient "github.com/shipstream/bloodraven/internal/dragonfly"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// SiteClient is a port-forwarded Dragonfly RESP client to a single
// site pod. Close it when the scenario step is done.
type SiteClient struct {
	Site string

	pf   *pgkube.PortForward
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

// Close releases the SPDY tunnel and the underlying TCP connection.
func (c *SiteClient) Close() error {
	var firstErr error
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			firstErr = err
		}
		c.conn = nil
	}
	if c.pf != nil {
		c.pf.Stop()
	}
	return firstErr
}

// Open builds a SiteClient by port-forwarding the per-site Dragonfly
// pod on the configured client port (defaults to 6379 — we don't read
// spec.dragonfly.port here because every playground manifest in this
// repo uses 6379 and a divergent port is a config bug worth surfacing
// via dial failure rather than silent wrong-pod targeting).
//
// password is the value from the auth Secret if AUTH is configured;
// pass "" when no password is set.
func Open(ctx context.Context, k *pgkube.Client, namespace, fg, site, password string) (*SiteClient, error) {
	if namespace == "" {
		namespace = pgkube.PlaygroundNamespace
	}
	pod, err := k.GetSiteDragonflyPod(ctx, namespace, fg, site)
	if err != nil {
		return nil, err
	}
	pf, err := k.PortForwardPod(ctx, namespace, pod.Name, 6379)
	if err != nil {
		return nil, fmt.Errorf("port-forward dragonfly for site %s: %w", site, err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", pf.LocalPort)
	d := net.Dialer{Timeout: dfclient.DefaultDialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		pf.Stop()
		return nil, fmt.Errorf("dial dragonfly %s: %w", addr, err)
	}
	c := &SiteClient{
		Site: site,
		pf:   pf,
		conn: conn,
		br:   bufio.NewReader(conn),
		bw:   bufio.NewWriter(conn),
	}
	if password != "" {
		if _, err := c.execReply(ctx, "AUTH", password); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("AUTH: %w", err)
		}
	}
	return c, nil
}

// Ping verifies the connection is live.
func (c *SiteClient) Ping(ctx context.Context) error {
	r, err := c.execReply(ctx, "PING")
	if err != nil {
		return err
	}
	if r != "PONG" {
		return fmt.Errorf("unexpected PING reply %q", r)
	}
	return nil
}

// Set writes a key. Returns the server reply ("OK" on success).
func (c *SiteClient) Set(ctx context.Context, key, value string) (string, error) {
	return c.execReply(ctx, "SET", key, value)
}

// Get reads a key. Returns ok=false if the key is absent (RESP nil
// bulk-string).
func (c *SiteClient) Get(ctx context.Context, key string) (value string, ok bool, err error) {
	v, err := c.execReplyAllowNil(ctx, "GET", key)
	if err != nil {
		return "", false, err
	}
	if v == nil {
		return "", false, nil
	}
	return *v, true, nil
}

// InfoReplication issues `INFO replication` and parses the body using
// the operator's parser so this client and the operator stay
// bug-for-bug consistent on field interpretation.
func (c *SiteClient) InfoReplication(ctx context.Context) (dfclient.ReplicationInfo, error) {
	body, err := c.execReply(ctx, "INFO", "replication")
	if err != nil {
		return dfclient.ReplicationInfo{}, err
	}
	return dfclient.ParseInfoReplication(body), nil
}

// execReply mirrors the operator's execReply but on this package's
// own conn. Returns the reply body as a string; nil bulk-strings
// surface as the empty string with no error (use execReplyAllowNil
// when nil-vs-empty distinction matters).
func (c *SiteClient) execReply(ctx context.Context, args ...string) (string, error) {
	v, err := c.execReplyAllowNil(ctx, args...)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", nil
	}
	return *v, nil
}

// execReplyAllowNil distinguishes a nil bulk-string reply (key not
// found) from an empty bulk-string reply.
func (c *SiteClient) execReplyAllowNil(ctx context.Context, args ...string) (*string, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("dragonfly site client closed")
	}
	deadline := time.Now().Add(dfclient.DefaultIOTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if err := writeArrayOfBulkStrings(c.bw, args...); err != nil {
		return nil, err
	}
	return readReplyAllowNil(c.br)
}
