package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// productionShapedChecker is a real Checker over the fake whose primary
// pool has the production pool's size (two connections), so a test can
// exhaust it.
func productionShapedChecker(t *testing.T, r *testutil.FakeReplica) mysql.Checker {
	t.Helper()
	db := sql.OpenDB(r)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	c := mysql.NewCheckerFromPools(db, sql.OpenDB(r), r)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestFlushBinaryLogs_UsesNoPooledConnection: with every connection of the
// primary pool pinned, FlushBinaryLogs still runs: it dials a connection of
// its own. A pooled statement, by contrast, waits for a connection.
func TestFlushBinaryLogs_UsesNoPooledConnection(t *testing.T) {
	r := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", ReadOnly: true, SuperReadOnly: true})
	c := productionShapedChecker(t, r)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		sess, err := c.PinSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer sess.Close()
	}
	// The pool is exhausted: a pooled probe cannot get a connection.
	probeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err := c.CheckReadOnly(probeCtx)
	cancel()
	if err == nil {
		t.Fatal("a pooled probe ran although both pooled connections are pinned; the fixture does not exhaust the pool")
	}
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.FlushBinaryLogs(flushCtx); err != nil {
		t.Fatalf("FlushBinaryLogs with the pool exhausted: %v", err)
	}
	st := r.State()
	if len(st.BinlogPreviousGtids) != 1 || st.BinlogPreviousGtids[0] != "uuid1:1-100" {
		t.Fatalf("rotations = %v, want one at uuid1:1-100", st.BinlogPreviousGtids)
	}
	if !st.ReadOnly || !st.SuperReadOnly || st.ExecutedGtidSet != "uuid1:1-100" {
		t.Fatalf("the rotation changed the server: %+v", st)
	}
}

// TestFlushBinaryLogs_CancelledFlushLeavesThePinnedSessionAndPoolUsable: a
// flush abandoned on a cancelled context breaks only its own connection.
// The pinned session that carries a promotion still runs its next
// statement, and the pool still serves probes.
func TestFlushBinaryLogs_CancelledFlushLeavesThePinnedSessionAndPoolUsable(t *testing.T) {
	r := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", ReadOnly: true, SuperReadOnly: true})
	c := productionShapedChecker(t, r)
	ctx := context.Background()
	sess, err := c.PinSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	entered := make(chan struct{})
	r.OnFlush(func(flushCtx context.Context) error {
		close(entered)
		<-flushCtx.Done()
		return flushCtx.Err()
	})
	flushCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.FlushBinaryLogs(flushCtx) }()
	<-entered
	// While the flush is in flight, the pool's other connection serves
	// health probes and the pinned session keeps working.
	if _, err := c.CheckReadOnly(ctx); err != nil {
		t.Fatalf("pooled probe during a slow flush: %v", err)
	}
	if _, err := sess.GetGtidExecuted(ctx); err != nil {
		t.Fatalf("pinned session during a slow flush: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("FlushBinaryLogs = %v, want its context's cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled FlushBinaryLogs did not return")
	}
	if err := sess.SetSuperReadOnly(ctx, false); err != nil {
		t.Fatalf("the pinned session after an abandoned flush: %v", err)
	}
	if err := sess.SetReadOnly(ctx, false); err != nil {
		t.Fatalf("the pinned session after an abandoned flush: %v", err)
	}
	if ro, err := c.CheckReadOnly(ctx); err != nil || ro {
		t.Fatalf("pooled probe after an abandoned flush = %v, %v; want writable", ro, err)
	}
	if got := r.State().BinlogPreviousGtids; len(got) != 0 {
		t.Fatalf("an abandoned flush rotated the fake's binary log: %v", got)
	}
}

// TestFlushBinaryLogs_ErrorIsWrapped: the statement's error comes back
// wrapped, and nothing rotates.
func TestFlushBinaryLogs_ErrorIsWrapped(t *testing.T) {
	r := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", ReadOnly: true})
	c := productionShapedChecker(t, r)
	denied := errors.New("Error 1227 (42000): Access denied; you need (at least one of) the RELOAD privilege(s) for this operation")
	r.OnFlush(func(context.Context) error { return denied })
	err := c.FlushBinaryLogs(context.Background())
	if !errors.Is(err, denied) || !strings.HasPrefix(err.Error(), "flush binary logs: ") {
		t.Fatalf("FlushBinaryLogs = %v, want the wrapped statement error", err)
	}
}

// TestRealMySQL_FlushBinaryLogs runs FlushBinaryLogs against a real server
// with binary logging and GTIDs on. It needs
// BLOODRAVEN_TEST_MYSQL_ADMIN_DSN (an account with ALL PRIVILEGES, as the
// operator's):
//
//	docker run -d --name br-flush -e MYSQL_ROOT_PASSWORD=root -p 33308:3306 mysql:9.7 --gtid-mode=ON --enforce-gtid-consistency=ON --server-id=7
//	BLOODRAVEN_TEST_MYSQL_ADMIN_DSN='root:root@tcp(127.0.0.1:33308)/' go test ./internal/mysql -run RealMySQL_FlushBinaryLogs -v
//
// It proves what the promotion path relies on: the statement works under
// super_read_only, adds no GTID, and the file it opens has a Previous_gtids
// header equal to gtid_executed; it needs RELOAD, which SUPER does not
// imply; and it runs while both pooled connections are pinned.
func TestRealMySQL_FlushBinaryLogs(t *testing.T) {
	dsn := os.Getenv("BLOODRAVEN_TEST_MYSQL_ADMIN_DSN")
	if dsn == "" {
		t.Skip("BLOODRAVEN_TEST_MYSQL_ADMIN_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var logBin, gtidMode string
	if err := admin.QueryRowContext(ctx, "SELECT @@log_bin, @@gtid_mode").Scan(&logBin, &gtidMode); err != nil {
		t.Fatal(err)
	}
	if logBin != "1" || gtidMode != "ON" {
		t.Skipf("server has log_bin=%s gtid_mode=%s; the test needs binary logging with GTIDs", logBin, gtidMode)
	}
	exec := func(q string) {
		t.Helper()
		if _, err := admin.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE DATABASE IF NOT EXISTS br_flush_test")
	exec("CREATE TABLE IF NOT EXISTS br_flush_test.t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
	exec("INSERT INTO br_flush_test.t (v) VALUES (1)")
	exec("DROP USER IF EXISTS 'br_noreload'@'%'")
	exec("CREATE USER 'br_noreload'@'%' IDENTIFIED BY 'pw'")
	exec("GRANT SUPER, SYSTEM_VARIABLES_ADMIN, REPLICATION CLIENT ON *.* TO 'br_noreload'@'%'")
	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, q := range []string{"SET GLOBAL read_only = OFF", "DROP USER IF EXISTS 'br_noreload'@'%'", "DROP DATABASE IF EXISTS br_flush_test"} {
			_, _ = admin.ExecContext(cleanCtx, q)
		}
	})

	checker, err := mysql.NewChecker(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer checker.Close()
	if err := checker.SetSuperReadOnly(ctx, true); err != nil {
		t.Fatal(err)
	}
	// Both connections of the primary pool are pinned, as during a
	// promotion with a concurrent probe.
	for i := 0; i < 2; i++ {
		sess, err := checker.PinSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer sess.Close()
	}
	status := func() (file, gtid string) {
		t.Helper()
		rows, err := admin.QueryContext(ctx, "SHOW BINARY LOG STATUS")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		vals := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range vals {
			dest[i] = &vals[i]
		}
		if !rows.Next() {
			t.Fatal("SHOW BINARY LOG STATUS returned no row")
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		file = string(vals[0])
		if err := admin.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&gtid); err != nil {
			t.Fatal(err)
		}
		return file, strings.ReplaceAll(gtid, "\n", "")
	}
	fileBefore, gtidBefore := status()

	start := time.Now()
	if err := checker.FlushBinaryLogs(ctx); err != nil {
		t.Fatalf("FlushBinaryLogs under super_read_only with the pool pinned: %v", err)
	}
	t.Logf("FlushBinaryLogs (one-shot connection + statement) took %s", time.Since(start))

	fileAfter, gtidAfter := status()
	if fileAfter == fileBefore {
		t.Fatalf("the active binary log is still %s", fileAfter)
	}
	if gtidAfter != gtidBefore {
		t.Fatalf("gtid_executed changed from %q to %q: the flush was logged", gtidBefore, gtidAfter)
	}
	var logName, eventType, info string
	var pos, serverID, endPos int64
	err = admin.QueryRowContext(ctx, "SHOW BINLOG EVENTS IN '"+fileAfter+"' LIMIT 1, 1").Scan(&logName, &pos, &eventType, &serverID, &endPos, &info)
	if err != nil {
		t.Fatal(err)
	}
	if eventType != "Previous_gtids" || strings.ReplaceAll(info, "\n", "") != gtidBefore {
		t.Fatalf("the new file's second event is %s %q, want Previous_gtids %q", eventType, info, gtidBefore)
	}

	// RELOAD is required, and SUPER does not imply it.
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Passwd = "br_noreload", "pw"
	limited, err := mysql.NewChecker(cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	err = limited.FlushBinaryLogs(ctx)
	var myErr *mysqldriver.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1227 {
		t.Fatalf("FlushBinaryLogs without RELOAD = %v, want error 1227", err)
	}
}
