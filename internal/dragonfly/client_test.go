package dragonfly

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer is a one-shot RESP server scripted by the test. Each
// connection reads commands as RESP arrays of bulk strings and replies
// from a programmed response table. Unknown commands return a -ERR.
type fakeServer struct {
	t          *testing.T
	listener   net.Listener
	mu         sync.Mutex
	password   string // when non-empty, AUTH is required first
	responses  map[string]string
	authedOnly map[string]bool
	// delays maps the uppercased first command word ("REPLTAKEOVER") to
	// a sleep duration the server inserts before replying. Used to
	// simulate slow server-side operations (replication drain) without
	// fixed delays in tests that don't need them.
	delays map[string]time.Duration

	commandsCh chan []string
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{
		t:          t,
		listener:   l,
		responses:  map[string]string{},
		authedOnly: map[string]bool{},
		delays:     map[string]time.Duration{},
		commandsCh: make(chan []string, 64),
	}
	go s.serve()
	t.Cleanup(func() { _ = l.Close() })
	return s
}

func (s *fakeServer) addr() string {
	return s.listener.Addr().String()
}

// reply scripts a response for the given command. Key is the
// uppercased, space-joined command (e.g. "INFO REPLICATION").
func (s *fakeServer) reply(cmd, response string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses[strings.ToUpper(cmd)] = response
}

func (s *fakeServer) requireAuth(password string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.password = password
}

// delay registers a sleep applied before the server replies to any
// command whose first word matches verb (case-insensitive). Used by
// TestClientReplTakeoverHonorsServerDrainBudget to simulate a slow
// REPLTAKEOVER without sleeping in unrelated tests.
func (s *fakeServer) delay(verb string, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delays[strings.ToUpper(verb)] = d
}

func (s *fakeServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	authed := false
	for {
		args, err := readArrayCommand(br)
		if err != nil {
			return // EOF or any read error: connection closed.
		}
		if len(args) == 0 {
			continue
		}
		select {
		case s.commandsCh <- args:
		default:
		}
		key := strings.ToUpper(strings.Join(args, " "))

		s.mu.Lock()
		needPassword := s.password != ""
		password := s.password
		canned, hasCanned := s.responses[key]
		delay := s.delays[strings.ToUpper(args[0])]
		s.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}

		if needPassword && !authed && strings.ToUpper(args[0]) != "AUTH" {
			_, _ = bw.WriteString("-NOAUTH Authentication required.\r\n")
			_ = bw.Flush()
			continue
		}
		if strings.ToUpper(args[0]) == "AUTH" {
			if len(args) < 2 || args[1] != password {
				_, _ = bw.WriteString("-WRONGPASS invalid username-password pair\r\n")
				_ = bw.Flush()
				continue
			}
			authed = true
			_, _ = bw.WriteString("+OK\r\n")
			_ = bw.Flush()
			continue
		}
		if hasCanned {
			_, _ = bw.WriteString(canned)
			_ = bw.Flush()
			continue
		}
		// Default: simple OK.
		_, _ = bw.WriteString("+OK\r\n")
		_ = bw.Flush()
	}
}

func readArrayCommand(br *bufio.Reader) ([]string, error) {
	prefix, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, fmt.Errorf("expected '*', got %q", string(prefix))
	}
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		bp, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		if bp != '$' {
			return nil, fmt.Errorf("expected '$', got %q", string(bp))
		}
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		if _, err := br.Discard(2); err != nil {
			return nil, err
		}
		args = append(args, string(buf))
	}
	return args, nil
}

func bulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func TestClientPing(t *testing.T) {
	srv := newFakeServer(t)
	srv.reply("PING", "+PONG\r\n")
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestClientAuth(t *testing.T) {
	srv := newFakeServer(t)
	srv.requireAuth("hunter2")
	srv.reply("PING", "+PONG\r\n")
	c := mustNewClient(t, srv.addr(), "hunter2")
	defer c.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping after AUTH: %v", err)
	}
}

func TestClientAuthWrongPassword(t *testing.T) {
	srv := newFakeServer(t)
	srv.requireAuth("hunter2")
	_, err := New(context.Background(), Config{Addr: srv.addr(), Password: "wrong"})
	if err == nil {
		t.Fatal("expected AUTH error, got nil")
	}
	if !IsServerError(err, "WRONGPASS") {
		t.Errorf("expected WRONGPASS, got %v", err)
	}
}

func TestClientNoAuthRequired(t *testing.T) {
	srv := newFakeServer(t)
	srv.requireAuth("hunter2")
	// Connect without password; AUTH never sent; PING should fail with NOAUTH.
	c, err := New(context.Background(), Config{Addr: srv.addr()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	err = c.Ping(context.Background())
	if !IsServerError(err, "NOAUTH") {
		t.Errorf("expected NOAUTH, got %v", err)
	}
}

func TestClientInfoReplication(t *testing.T) {
	srv := newFakeServer(t)
	body := "# Replication\r\nrole:master\r\nmaster_repl_offset:42\r\n"
	srv.reply("INFO REPLICATION", bulkString(body))
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	info, err := c.InfoReplication(context.Background())
	if err != nil {
		t.Fatalf("InfoReplication: %v", err)
	}
	if info.Role != "master" {
		t.Errorf("Role = %q, want master", info.Role)
	}
	if info.MasterReplOffset != 42 {
		t.Errorf("MasterReplOffset = %d, want 42", info.MasterReplOffset)
	}
}

func TestClientReplicaOf(t *testing.T) {
	srv := newFakeServer(t)
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	if err := c.ReplicaOf(context.Background(), "10.0.0.1", 6379); err != nil {
		t.Fatalf("ReplicaOf: %v", err)
	}
	got := <-srv.commandsCh
	if want := []string{"REPLICAOF", "10.0.0.1", "6379"}; !equalSlices(got, want) {
		t.Errorf("got command %v, want %v", got, want)
	}
}

func TestClientReplicaOfNoOne(t *testing.T) {
	srv := newFakeServer(t)
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	if err := c.ReplicaOfNoOne(context.Background()); err != nil {
		t.Fatalf("ReplicaOfNoOne: %v", err)
	}
	got := <-srv.commandsCh
	if want := []string{"REPLICAOF", "NO", "ONE"}; !equalSlices(got, want) {
		t.Errorf("got command %v, want %v", got, want)
	}
}

func TestClientReplTakeover(t *testing.T) {
	srv := newFakeServer(t)
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	if err := c.ReplTakeover(context.Background(), 30*time.Second); err != nil {
		t.Fatalf("ReplTakeover: %v", err)
	}
	got := <-srv.commandsCh
	if want := []string{"REPLTAKEOVER", "30000"}; !equalSlices(got, want) {
		t.Errorf("got command %v, want %v", got, want)
	}
}

func TestClientKillType(t *testing.T) {
	srv := newFakeServer(t)
	// Real Dragonfly returns the count of killed connections as an
	// integer reply. Programme that explicitly so we exercise the
	// integer-reply branch in readReply rather than falling through to
	// the default "+OK\r\n".
	srv.reply("CLIENT KILL TYPE NORMAL", ":7\r\n")
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	if err := c.ClientKillType(context.Background(), "NORMAL"); err != nil {
		t.Fatalf("ClientKillType: %v", err)
	}
	got := <-srv.commandsCh
	if want := []string{"CLIENT", "KILL", "TYPE", "NORMAL"}; !equalSlices(got, want) {
		t.Errorf("got command %v, want %v", got, want)
	}
}

// TestClientReplTakeoverHonorsServerDrainBudget regression-tests the
// fix for the bug where REPLTAKEOVER inherited DefaultIOTimeout (5s)
// even though its `timeout` argument is the server-side drain budget
// (up to 30s). Without the fix, the client times out at 5s while the
// server may have already accepted the takeover, leaving the operator
// uncertain about promotion state.
//
// The fake server sleeps for slightly over the client's IOTimeout
// before replying. With the fix, ReplTakeover's per-call deadline is
// timeout+5s, so the call succeeds. Without it, the call times out.
func TestClientReplTakeoverHonorsServerDrainBudget(t *testing.T) {
	const (
		ioTimeout    = 200 * time.Millisecond
		serverDelay  = 400 * time.Millisecond
		drainBudget  = 1 * time.Second
	)
	srv := newFakeServer(t)
	srv.delay("REPLTAKEOVER", serverDelay)
	srv.reply("REPLTAKEOVER 1000", "+OK\r\n")
	c, err := New(context.Background(), Config{Addr: srv.addr(), DialTimeout: time.Second, IOTimeout: ioTimeout})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	start := time.Now()
	if err := c.ReplTakeover(context.Background(), drainBudget); err != nil {
		t.Fatalf("ReplTakeover: %v", err)
	}
	if elapsed := time.Since(start); elapsed < serverDelay {
		t.Errorf("ReplTakeover returned in %s, expected at least %s server delay", elapsed, serverDelay)
	}
}

func TestClientReplTakeoverClampsZero(t *testing.T) {
	srv := newFakeServer(t)
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	if err := c.ReplTakeover(context.Background(), 0); err != nil {
		t.Fatalf("ReplTakeover(0): %v", err)
	}
	got := <-srv.commandsCh
	if got[1] == "0" {
		t.Errorf("ReplTakeover(0) sent %q, want positive ms", got[1])
	}
}

// TestClientRespBulkStringSizeCap regression-tests B19: a wedged or
// malicious server returning an oversized bulk-string length used to
// allocate that many bytes before the read failed. The client now
// caps the allocation at MaxBulkStringSize and returns an error
// before the make().
func TestClientRespBulkStringSizeCap(t *testing.T) {
	srv := newFakeServer(t)
	// Reply with a length larger than the cap, but only send 0 bytes
	// of body. The client must error out on the length check before
	// it tries to read the body.
	huge := MaxBulkStringSize + 1
	srv.reply("PING", fmt.Sprintf("$%d\r\n", huge))
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	err := c.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error from oversized bulk-string reply")
	}
	if !strings.Contains(err.Error(), "exceeds cap") {
		t.Errorf("expected cap-exceeded error, got %v", err)
	}
}

// TestClientHonorsContextCancellation regression-tests B18: ctx
// cancellation used to be ignored mid-I/O — only the deadline was
// propagated. A cancelled ctx now aborts the in-flight read promptly
// via SetDeadline(now-in-the-past).
func TestClientHonorsContextCancellation(t *testing.T) {
	srv := newFakeServer(t)
	// Server delays beyond what we want to wait for; cancellation
	// should cut the call short well before the delay elapses.
	srv.delay("PING", 5*time.Second)
	c, err := New(context.Background(), Config{Addr: srv.addr(), DialTimeout: time.Second, IOTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if err := c.Ping(ctx); err == nil {
		t.Fatalf("expected error from cancelled ctx; nil after %s", time.Since(start))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Ping returned after %s; want <2s (ctx cancel must abort I/O promptly)", elapsed)
	}
}

func TestClientCloseIdempotent(t *testing.T) {
	srv := newFakeServer(t)
	c := mustNewClient(t, srv.addr(), "")
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := c.Ping(context.Background()); err == nil {
		t.Errorf("expected Ping after Close to error")
	}
}

func mustNewClient(t *testing.T, addr, password string) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{Addr: addr, Password: password, DialTimeout: 2 * time.Second, IOTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
