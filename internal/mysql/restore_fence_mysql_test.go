package mysql

import (
	"context"
	"database/sql"
	"os"
	"slices"
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
)

// TestRestoreFence_RealMySQL exercises the restore fence against a real
// server. It needs BLOODRAVEN_TEST_MYSQL_ADMIN_DSN (an account with
// CREATE USER and CONNECTION_ADMIN, e.g. root) and is skipped otherwise.
//
//	docker run -d --name brfence -e MYSQL_ROOT_PASSWORD=root -p 33307:3306 mysql:9.7
//	BLOODRAVEN_TEST_MYSQL_ADMIN_DSN='root:root@tcp(127.0.0.1:33307)/' go test ./internal/mysql -run RealMySQL
func TestRestoreFence_RealMySQL(t *testing.T) {
	dsn := os.Getenv("BLOODRAVEN_TEST_MYSQL_ADMIN_DSN")
	if dsn == "" {
		t.Skip("BLOODRAVEN_TEST_MYSQL_ADMIN_DSN not set")
	}
	ctx := context.Background()
	c, err := NewChecker(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	f := c.(RestoreFencer)
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	for _, stmt := range []string{
		"DROP USER IF EXISTS 'fenceapp'@'%', 'fenceadm'@'%', 'fencerole'@'%', 'fenceop'@'%'",
		"DROP ROLE IF EXISTS 'fenceconn'",
		"CREATE USER 'fenceapp'@'%' IDENTIFIED BY 'p'",
		"CREATE USER 'fenceadm'@'%' IDENTIFIED BY 'p'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'fenceadm'@'%'",
		"CREATE ROLE 'fenceconn'",
		"GRANT CONNECTION_ADMIN, SYSTEM_VARIABLES_ADMIN, PROCESS, REPLICATION_SLAVE_ADMIN ON *.* TO 'fenceconn'",
		"CREATE USER 'fencerole'@'%' IDENTIFIED BY 'p'",
		"GRANT 'fenceconn' TO 'fencerole'@'%'",
		"SET DEFAULT ROLE ALL TO 'fencerole'@'%'",
		"CREATE USER 'fenceop'@'%' IDENTIFIED BY 'p'",
		"GRANT CONNECTION_ADMIN, SYSTEM_VARIABLES_ADMIN, PROCESS, REPLICATION_SLAVE_ADMIN ON *.* TO 'fenceop'@'%'",
		"SET GLOBAL offline_mode = OFF",
	} {
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(ctx, "SET GLOBAL offline_mode = OFF")
		_, _ = admin.ExecContext(ctx, "DROP USER IF EXISTS 'fenceapp'@'%', 'fenceadm'@'%', 'fencerole'@'%', 'fenceop'@'%'")
		_, _ = admin.ExecContext(ctx, "DROP ROLE IF EXISTS 'fenceconn'")
	})
	userDSN := func(user string) string {
		cfg, err := mysqldrv.ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.User, cfg.Passwd = user, "p"
		return cfg.FormatDSN()
	}
	open := func(user string) *sql.Conn {
		db, err := sql.Open("mysql", userDSN(user))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("connect %s: %v", user, err)
		}
		return conn
	}

	if missing, err := f.MissingRestoreFencePrivileges(ctx); err != nil || len(missing) != 0 {
		t.Fatalf("admin account MissingRestoreFencePrivileges = %v, %v", missing, err)
	}
	for user, want := range map[string][]string{
		"fenceapp":  RestoreFencePrivileges,
		"fenceadm":  {"SYSTEM_VARIABLES_ADMIN", "PROCESS", "REPLICATION_SLAVE_ADMIN"},
		"fencerole": nil,
	} {
		uc, err := NewChecker(userDSN(user))
		if err != nil {
			t.Fatal(err)
		}
		got, err := uc.(RestoreFencer).MissingRestoreFencePrivileges(ctx)
		uc.Close()
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("%s MissingRestoreFencePrivileges = %v, %v; want %v", user, got, err, want)
		}
	}

	appConn := open("fenceapp")
	admConn := open("fenceadm")
	if _, err := appConn.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}

	if on, err := f.GetOfflineMode(ctx); err != nil || on {
		t.Fatalf("offline_mode before = %v, %v", on, err)
	}
	if err := f.SetOfflineMode(ctx, true); err != nil {
		t.Fatal(err)
	}
	remaining, protected, err := f.EvictSessions(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if remaining != 0 || len(protected) != 0 {
		t.Fatalf("after eviction remaining = %d protected = %v, want 0 and none", remaining, protected)
	}
	if _, err := appConn.ExecContext(ctx, "SELECT 1"); err == nil {
		t.Fatal("an application session survived the fence")
	}
	appDB, _ := sql.Open("mysql", userDSN("fenceapp"))
	defer appDB.Close()
	if err := appDB.PingContext(ctx); err == nil {
		t.Fatal("a new application connection was admitted under offline_mode")
	}
	_ = admConn.Close() // killed by the sweep; an admin may reconnect
	admDB, _ := sql.Open("mysql", userDSN("fenceadm"))
	defer admDB.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := admDB.PingContext(pingCtx); err != nil {
		t.Fatalf("a CONNECTION_ADMIN account must still connect: %v", err)
	}

	// An operator account without SYSTEM_USER cannot kill a SYSTEM_USER
	// (root) session: it is reported, not counted as remaining.
	rootConn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConn.Close()
	if _, err := rootConn.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	opc, err := NewChecker(userDSN("fenceop"))
	if err != nil {
		t.Fatal(err)
	}
	defer opc.Close()
	remaining, protected, err = opc.(RestoreFencer).EvictSessions(ctx, nil)
	if err != nil || remaining != 0 || !slices.Contains(protected, "root") {
		t.Fatalf("eviction without SYSTEM_USER: remaining %d protected %v err %v, want 0, [root], nil", remaining, protected, err)
	}

	if err := f.SetOfflineMode(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err := appDB.PingContext(ctx); err != nil {
		t.Fatalf("application connections must work again after release: %v", err)
	}
}
