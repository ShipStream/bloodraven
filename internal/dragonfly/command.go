package dragonfly

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// HasCommand reports whether the connected instance advertises name in
// its command table. It issues COMMAND INFO <name> and inspects the
// typed reply. This probes the capability, not a version string.
//
// A missing command returns (false, nil). I/O, AUTH, and timeout
// failures return an error so callers do not treat a flaky probe as
// "unsupported".
func (c *Client) HasCommand(ctx context.Context, name string) (bool, error) {
	val, err := c.execValue(ctx, "COMMAND", "INFO", name)
	if err != nil {
		if IsUnknownCommand(err) {
			return false, nil
		}
		var se *ServerError
		if errors.As(err, &se) {
			// Older Dragonfly accepts COMMAND but not COMMAND INFO.
			// Scan the full table rather than treating a syntax error
			// as a failed probe (which would leave status unset).
			table, tErr := c.execValue(ctx, "COMMAND")
			if tErr != nil {
				if IsUnknownCommand(tErr) {
					return false, nil
				}
				return false, tErr
			}
			return commandInfoReports(table, name), nil
		}
		return false, err
	}
	return commandInfoReports(val, name), nil
}

// execValue writes a command and reads a typed RESP value. Used by
// HasCommand; the string-oriented execReply path stays unchanged.
func (c *Client) execValue(ctx context.Context, args ...string) (respValue, error) {
	if c.conn == nil {
		return respValue{}, fmt.Errorf("dragonfly: client closed")
	}
	ioTimeout := c.cfg.IOTimeout
	if ioTimeout <= 0 {
		ioTimeout = DefaultIOTimeout
	}
	deadline := time.Now().Add(ioTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return respValue{}, err
	}
	stopWatch := c.watchCancel(ctx)
	defer stopWatch()
	if err := writeCommand(c.bw, args...); err != nil {
		return respValue{}, err
	}
	return readValue(c.br, 0)
}

// commandInfoReports is true when a COMMAND INFO reply contains an
// entry whose first bulk string is name. Handles both the filtered
// shape (*1 + metadata or *1 + null) and an unfiltered command table.
func commandInfoReports(v respValue, name string) bool {
	if v.null || v.typ != '*' {
		return false
	}
	want := strings.ToUpper(name)
	for _, elt := range v.array {
		if elt.null || elt.typ != '*' || len(elt.array) == 0 {
			continue
		}
		if strings.EqualFold(elt.array[0].str, want) {
			return true
		}
	}
	return false
}
