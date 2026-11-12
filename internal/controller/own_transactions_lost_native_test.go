package controller

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// TestOwnLossLagProofNative is opt-in (BLOODRAVEN_MYSQL_NATIVE_TEST=1,
// docker; CI job test-pitr-native). It runs the position proof that tells a
// hole from replication lag (proveOwnLossHole, "A hole, or only lag?" in
// own_transactions_lost.go) against two servers of the default MySQL
// image, through the production checker. The unit tests of the proof model
// the server; the rule itself rests on what a real server does:
//
//   - a transaction issued under a site's server_uuid on the primary (a
//     replay from a decoded binary log, or a hand-set gtid_next, which is
//     what this test uses) is logged with the primary's server_id, and the
//     site receives it;
//   - SHOW BINARY LOG STATUS reports coordinates and an executed set that
//     belong together, and once the site's applier has executed past those
//     coordinates the site holds every transaction of that set that
//     replication delivers;
//   - a transaction the primary received from the site by replication
//     carries the site's server_id, the site's receiver drops it, and the
//     site's applier still reaches the primary's position.
//
// Two phases, on one pair of servers:
//
//  1. Lag. Transactions under B's server_uuid are committed on A while A
//     takes ordinary writes and rotates its binary log, and B replicates
//     with a one-second applier delay, so at every comparison B lacks
//     transactions under its own server_uuid that A holds. The proof must
//     never call that a hole. When the writes stop, B holds them all.
//  2. A hole. B is made the primary, issues transactions, A receives
//     them, and B then comes back without the last three (its GTID state
//     is reset to an earlier set, as after a volume that came back older).
//     B replicates from A again. The proof must report exactly those
//     three, and only once B's applier has passed A's snapshot.
func TestOwnLossLagProofNative(t *testing.T) {
	if os.Getenv("BLOODRAVEN_MYSQL_NATIVE_TEST") != "1" {
		t.Skip("opt-in native own-loss lag proof test (BLOODRAVEN_MYSQL_NATIVE_TEST=1, docker)")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("docker is required")
	}
	image := os.Getenv("BLOODRAVEN_MYSQL_NATIVE_IMAGE")
	if image == "" {
		image = defaultMySQLImage
	}
	env := newLagNativeEnv(t, docker, image)
	a, b := env.start("a", 1), env.start("b", 2)
	a.waitUp()
	b.waitUp()
	for _, s := range []*lagNativeServer{a, b} {
		s.session("SET sql_log_bin = 0",
			"CREATE USER 'repl'@'%' IDENTIFIED BY '"+lagNativePassword+"'",
			"GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'",
			// The connection goes back to the pool.
			"SET sql_log_bin = 1")
		s.exec("RESET BINARY LOGS AND GTIDS")
	}
	aUUID, bUUID := a.query("SELECT @@server_uuid"), b.query("SELECT @@server_uuid")

	cfg := testTopologyConfig()
	cfg.Sites[0].Host, cfg.Sites[1].Host = a.alias, b.alias
	tm := NewTopologyManager(cfg, []mysql.Checker{a.checker, b.checker}, NewFailoverController(testLogger()), nil,
		NewBootstrapController(testLogger()), BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	primary, site := tm.getSite("dc1"), tm.getSite("dc2")
	ctx := context.Background()
	uuids := []string{bUUID}
	// compare is the audit's comparison: the primary's set, then the
	// site's, and what the site lacks of its own transactions.
	compare := func() mysql.GTIDSet {
		_, primarySet, err := readGTIDSetRaw(ctx, a.checker)
		if err != nil {
			t.Fatalf("read the primary's GTID set: %v", err)
		}
		_, siteSet, ok := tm.readOwnLossSubject(ctx, site)
		if !ok {
			t.Fatal("read the site's server_uuid and GTID set")
		}
		return ownLostTransactions(primarySet, siteSet, uuids)
	}

	// Phase 1: lag.
	a.exec("CREATE DATABASE t")
	const writers = 3
	for w := 0; w <= writers; w++ {
		a.exec(fmt.Sprintf("CREATE TABLE t.w%d (id BIGINT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(16))", w))
	}
	b.replicateFrom(a, 1)

	loadCtx, stopLoad := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var written, replayed atomic.Int64
	for w := 1; w <= writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for loadCtx.Err() == nil {
				if _, err := a.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO t.w%d (v) VALUES ('x')", w)); err == nil {
					written.Add(1)
				}
				time.Sleep(4 * time.Millisecond)
			}
		}()
	}
	// The replay: transactions under B's server_uuid, committed on A.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := a.db.Conn(ctx)
		if err != nil {
			t.Errorf("replay connection: %v", err)
			return
		}
		defer conn.Close()
		for n := int64(1); loadCtx.Err() == nil; n++ {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET gtid_next = '%s:%d'", bUUID, n)); err != nil {
				t.Errorf("replay: set gtid_next: %v", err)
				return
			}
			if _, err := conn.ExecContext(ctx, "INSERT INTO t.w0 (v) VALUES ('replayed')"); err != nil {
				t.Errorf("replay: transaction %d: %v", n, err)
				return
			}
			replayed.Store(n)
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := conn.ExecContext(ctx, "SET gtid_next = 'AUTOMATIC'"); err != nil {
			t.Errorf("replay: reset gtid_next: %v", err)
		}
	}()
	// The primary's binary log rotates under the proof.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for loadCtx.Err() == nil {
			time.Sleep(700 * time.Millisecond)
			if _, err := a.db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
				t.Errorf("flush binary logs: %v", err)
			}
		}
	}()

	var deficits, unproven, lagProven, falseHoles int
	for end := time.Now().Add(15 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		lost := compare()
		if lost.IsEmpty() {
			continue
		}
		deficits++
		hole, proven := tm.proveOwnLossHole(ctx, primary, site, aUUID, bUUID, uuids, lost)
		switch {
		case !proven:
			unproven++
		case hole.IsEmpty():
			lagProven++
		default:
			falseHoles++
			t.Errorf("lag was taken for a hole: the proof reports %s while B replicates and A only replays", hole.String())
		}
	}
	stopLoad()
	wg.Wait()
	t.Logf("phase 1: %d writes and %d replayed transactions on A; %d comparisons saw a deficit: %d not proven, %d proven to be lag, %d false holes",
		written.Load(), replayed.Load(), deficits, unproven, lagProven, falseHoles)
	if falseHoles > 0 {
		t.Fatalf("B's channel: %s", b.replicationErrors())
	}
	if deficits == 0 || replayed.Load() == 0 {
		t.Fatal("the site never lacked a replayed transaction at a comparison: the phase proved nothing")
	}
	lagNativeWait(t, "B to hold every transaction A replayed", 30*time.Second, func() bool { return compare().IsEmpty() })
	if got := b.query("SELECT COUNT(*) FROM t.w0"); got != fmt.Sprint(replayed.Load()) {
		t.Fatalf("B holds %s replayed rows, want %d: replication did not deliver the transactions issued under its own server_uuid", got, replayed.Load())
	}

	// Phase 2: a hole. B becomes the primary and issues ten transactions,
	// which A receives by replication, with B's server_id.
	b.exec("STOP REPLICA")
	b.exec("RESET REPLICA ALL")
	for i := 0; i < 10; i++ {
		b.exec("INSERT INTO t.w0 (v) VALUES ('issued by b')")
	}
	bSet := b.query("SELECT @@global.gtid_executed")
	a.replicateFrom(b, 0)
	lagNativeWait(t, "A to receive B's transactions", 30*time.Second, func() bool {
		return a.query("SELECT GTID_SUBSET('"+bSet+"', @@global.gtid_executed)") == "1"
	})
	a.exec("STOP REPLICA")
	a.exec("RESET REPLICA ALL")
	// B comes back without its last three.
	last := replayed.Load() + 10
	wantHole := fmt.Sprintf("%s:%d-%d", bUUID, last-2, last)
	kept := b.query("SELECT GTID_SUBTRACT(@@global.gtid_executed, '" + wantHole + "')")
	b.exec("RESET BINARY LOGS AND GTIDS")
	b.exec("SET GLOBAL gtid_purged = '" + strings.ReplaceAll(kept, "\n", "") + "'")
	if lost := compare(); lost.String() != wantHole {
		t.Fatalf("B lacks %q of its own transactions before it replicates, want %s", lost.String(), wantHole)
	}
	b.replicateFrom(a, 0)
	// A receiver that is still connecting is no running channel, and a
	// site without one is reported at once: wait, so that the hole below
	// is proven by the applier's position.
	lagNativeWait(t, "B's channel from A to run", 30*time.Second, func() bool {
		rs, err := b.checker.ShowReplicaStatus(ctx)
		return err == nil && rs != nil && rs.IORunning && rs.SQLRunning
	})
	a.exec("INSERT INTO t.w1 (v) VALUES ('after the loss')")
	a.exec("INSERT INTO t.w1 (v) VALUES ('after the loss')")

	var hole mysql.GTIDSet
	passes := 0
	lagNativeWait(t, "the hole to be proven", 60*time.Second, func() bool {
		lost := compare()
		if lost.String() != wantHole {
			t.Fatalf("B lacks %q of its own transactions, want %s: its receiver must drop them", lost.String(), wantHole)
		}
		passes++
		var proven bool
		hole, proven = tm.proveOwnLossHole(ctx, primary, site, aUUID, bUUID, uuids, lost)
		return proven
	})
	if hole.String() != wantHole {
		t.Fatalf("proven hole = %q, want %s", hole.String(), wantHole)
	}
	rs, err := b.checker.ShowReplicaStatus(ctx)
	if err != nil || rs == nil || !rs.IORunning || !rs.SQLRunning {
		t.Fatalf("B's channel = %+v, %v; want both threads running over the hole", rs, err)
	}
	t.Logf("phase 2: the hole %s was proven after %d comparisons, with B's applier at %s:%d and both threads running",
		wantHole, passes, rs.ExecSourceLogFile, rs.ExecSourceLogPos)
}

const lagNativePassword = "lag-native-root"

type lagNativeEnv struct {
	t       *testing.T
	docker  string
	image   string
	network string
}

type lagNativeServer struct {
	env      *lagNativeEnv
	name     string // container name
	alias    string // network alias: the host the other server replicates from
	serverID int
	db       *sql.DB
	checker  mysql.Checker
}

func newLagNativeEnv(t *testing.T, docker, image string) *lagNativeEnv {
	t.Helper()
	env := &lagNativeEnv{t: t, docker: docker, image: image,
		network: fmt.Sprintf("bloodraven-ownloss-native-%d", time.Now().UnixNano())}
	if out, err := exec.Command(docker, "network", "create", env.network).CombinedOutput(); err != nil {
		t.Fatalf("docker network create: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command(docker, "network", "rm", env.network).Run() })
	return env
}

// start runs a server with the replication settings of a site (GTIDs, a
// binary log named like the operator's, replica updates logged, no
// replication started at boot) and its port published on the loopback
// interface, where the test's checker connects.
func (env *lagNativeEnv) start(alias string, serverID int) *lagNativeServer {
	env.t.Helper()
	s := &lagNativeServer{env: env, name: env.network + "-" + alias, alias: "ownloss-" + alias, serverID: serverID}
	out, err := exec.Command(env.docker, "run", "-d", "--name", s.name, "--network", env.network, "--network-alias", s.alias,
		"-p", "127.0.0.1::3306", "-e", "MYSQL_ROOT_PASSWORD="+lagNativePassword, "-e", "MYSQL_ROOT_HOST=%", env.image,
		fmt.Sprintf("--server-id=%d", serverID), "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--log-bin=mysql-bin",
		"--log-replica-updates=ON", "--skip-replica-start=ON").CombinedOutput()
	if err != nil {
		env.t.Fatalf("docker run %s: %v\n%s", s.name, err, out)
	}
	env.t.Cleanup(func() {
		if env.t.Failed() {
			if logs, err := exec.Command(env.docker, "logs", "--tail", "40", s.name).CombinedOutput(); err == nil {
				env.t.Logf("%s log:\n%s", s.name, logs)
			}
		}
		if s.db != nil {
			_ = s.db.Close()
		}
		if s.checker != nil {
			_ = s.checker.Close()
		}
		_ = exec.Command(env.docker, "rm", "-f", "-v", s.name).Run()
	})
	return s
}

// waitUp waits for the server the image starts after its initialization
// (the temporary one does not listen on TCP) and opens the connections.
func (s *lagNativeServer) waitUp() {
	t := s.env.t
	t.Helper()
	var dsn string
	lagNativeWait(t, s.name+" to accept connections", 3*time.Minute, func() bool {
		out, err := exec.Command(s.env.docker, "port", s.name, "3306/tcp").Output()
		if err != nil {
			return false
		}
		addr := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
		if addr == "" {
			return false
		}
		dsn = fmt.Sprintf("root:%s@tcp(%s)/", lagNativePassword, addr)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return false
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var id int
		if err := db.QueryRowContext(pingCtx, "SELECT @@server_id").Scan(&id); err != nil || id != s.serverID {
			_ = db.Close()
			return false
		}
		s.db = db
		return true
	})
	checker, err := mysql.NewChecker(dsn)
	if err != nil {
		t.Fatalf("checker for %s: %v", s.name, err)
	}
	s.checker = checker
}

func (s *lagNativeServer) exec(q string) {
	s.env.t.Helper()
	if _, err := s.db.ExecContext(context.Background(), q); err != nil {
		s.env.t.Fatalf("%s: %s: %v", s.name, q, err)
	}
}

// session runs statements on one connection (session variables).
func (s *lagNativeServer) session(statements ...string) {
	s.env.t.Helper()
	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		s.env.t.Fatalf("%s: connection: %v", s.name, err)
	}
	defer conn.Close()
	for _, q := range statements {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			s.env.t.Fatalf("%s: %s: %v", s.name, q, err)
		}
	}
}

func (s *lagNativeServer) query(q string) string {
	s.env.t.Helper()
	var out sql.NullString
	if err := s.db.QueryRowContext(context.Background(), q).Scan(&out); err != nil {
		s.env.t.Fatalf("%s: %s: %v", s.name, q, err)
	}
	return out.String
}

// replicationErrors returns the last errors of the server's receiver and
// applier, for a failure message.
func (s *lagNativeServer) replicationErrors() string {
	s.env.t.Helper()
	return s.query(`SELECT CONCAT_WS(' | ',
		CONCAT('receiver ', (SELECT CONCAT(SERVICE_STATE, ' ', LAST_ERROR_MESSAGE) FROM performance_schema.replication_connection_status LIMIT 1)),
		CONCAT('coordinator ', (SELECT CONCAT(SERVICE_STATE, ' ', LAST_ERROR_MESSAGE) FROM performance_schema.replication_applier_status_by_coordinator LIMIT 1)),
		CONCAT('workers ', (SELECT GROUP_CONCAT(DISTINCT CONCAT(SERVICE_STATE, ' ', LAST_ERROR_MESSAGE) SEPARATOR '; ') FROM performance_schema.replication_applier_status_by_worker)))`)
}

// replicateFrom points the server at source with auto-positioning and
// starts both threads; delay is the applier delay in seconds.
func (s *lagNativeServer) replicateFrom(source *lagNativeServer, delay int) {
	s.env.t.Helper()
	s.exec(fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_USER='repl', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1, SOURCE_DELAY=%d",
		source.alias, lagNativePassword, delay))
	s.exec("START REPLICA")
}

// lagNativeWait polls cond every 200ms until it holds or timeout passes.
func lagNativeWait(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); !cond(); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
