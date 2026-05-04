// Minimal RESP client for talking to Dragonfly.
//
// Pulled in instead of github.com/redis/go-redis to keep the playground
// counter-app's dep tree small. Supports just enough RESP to issue INCR
// and GET against an integer key.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

type redisClient struct {
	addr    string
	mu      sync.Mutex
	conn    net.Conn
	rd      *bufio.Reader
	dialErr error
}

func newRedisClient(addr string) *redisClient {
	c := &redisClient{addr: addr}
	c.dialErr = errors.New("connecting to Dragonfly...")
	return c
}

// dial establishes a fresh connection. Caller must hold c.mu.
func (c *redisClient) dial() error {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.rd = nil
	}
	conn, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
	if err != nil {
		c.dialErr = err
		return err
	}
	c.conn = conn
	c.rd = bufio.NewReader(conn)
	c.dialErr = nil
	return nil
}

// do sends an inline command and reads one reply. On any I/O or
// protocol error the connection is closed and the next call re-dials.
func (c *redisClient) do(args ...string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		if err := c.dial(); err != nil {
			return nil, err
		}
	}

	if err := c.conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return nil, err
	}

	// Encode RESP array.
	buf := []byte{}
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, a := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(a)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, a...)
		buf = append(buf, '\r', '\n')
	}
	if _, err := c.conn.Write(buf); err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return nil, err
	}

	reply, err := readReply(c.rd)
	if err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return nil, err
	}
	return reply, nil
}

func readReply(rd *bufio.Reader) (any, error) {
	prefix, err := rd.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("malformed RESP line")
	}
	body := line[:len(line)-2]
	switch prefix {
	case '+':
		return body, nil
	case '-':
		return nil, fmt.Errorf("redis: %s", body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, perr := strconv.Atoi(body)
		if perr != nil {
			return nil, perr
		}
		if n < 0 {
			return nil, nil // null bulk
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rd, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func readFull(rd *bufio.Reader, p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := rd.Read(p[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// incr issues INCR and returns the new value as int64.
func (c *redisClient) incr(key string) (int64, error) {
	v, err := c.do("INCR", key)
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("INCR returned non-integer reply: %T", v)
	}
	return n, nil
}

// get returns the integer value at key, or 0 if the key does not exist.
func (c *redisClient) get(key string) (int64, error) {
	v, err := c.do("GET", key)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("GET returned non-string reply: %T", v)
	}
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// host returns the configured Dragonfly endpoint string for display.
func (c *redisClient) host() string { return c.addr }
