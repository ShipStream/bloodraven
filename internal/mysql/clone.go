package mysql

import (
	"context"
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func (m *checker) SetCloneDonorList(ctx context.Context, donor string) error {
	_, err := m.db.ExecContext(ctx, "SET GLOBAL clone_valid_donor_list = ?", donor)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 {
			// MySQL 8.4+ no longer exposes clone_valid_donor_list even when
			// the clone plugin is available. Older versions require this allowlist;
			// newer versions can proceed directly to CLONE INSTANCE.
			return nil
		}
		return fmt.Errorf("set clone donor list: %w", err)
	}
	return nil
}

func (m *checker) CloneInstance(ctx context.Context, user, host, password string, useSSL bool, cloneTimeoutSec int) error {
	if cloneTimeoutSec <= 0 {
		cloneTimeoutSec = 3600
	}

	if err := m.EnsureClonePlugin(ctx); err != nil {
		return fmt.Errorf("ensure clone plugin: %w", err)
	}

	// Set connection-level and global timeouts before cloning.
	// net_read_timeout and net_write_timeout are session-scoped and prevent the
	// server from dropping the connection during a long clone transfer.
	// clone_ddl_timeout is GLOBAL-only (session scope is not supported) and
	// controls how long the clone waits on conflicting DDL statements.
	// The session-scoped timeouts and CLONE INSTANCE itself run on the
	// dedicated clone pool (m.cloneDB), which has no driver-level read deadline
	// — a clone legitimately blocks for minutes, so it cannot use the primary
	// pool's statusNetTimeout. m.cloneDB is pinned to a single connection so the
	// SET SESSION values below apply to the connection that runs the clone.
	timeoutStmts := []string{
		fmt.Sprintf("SET SESSION net_read_timeout = %d", cloneTimeoutSec),
		fmt.Sprintf("SET SESSION net_write_timeout = %d", cloneTimeoutSec),
		fmt.Sprintf("SET GLOBAL clone_ddl_timeout = %d", cloneTimeoutSec),
	}
	for _, s := range timeoutStmts {
		if _, err := m.cloneDB.ExecContext(ctx, s); err != nil {
			var mysqlErr *mysqldriver.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 && s == fmt.Sprintf("SET GLOBAL clone_ddl_timeout = %d", cloneTimeoutSec) {
				// clone_ddl_timeout was removed in newer MySQL releases; the
				// connection-level timeouts above still apply, so continue.
				continue
			}
			return fmt.Errorf("set clone timeout (%s): %w", s, err)
		}
	}

	stmt := fmt.Sprintf("CLONE INSTANCE FROM '%s'@'%s':3306 IDENTIFIED BY '%s'",
		escapeSingleQuotes(user), escapeSingleQuotes(host), escapeSingleQuotes(password))
	if useSSL {
		stmt += " REQUIRE SSL"
	}
	// Clone may take a very long time, use the context for cancellation
	_, err := m.cloneDB.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("clone instance: %w", err)
	}
	return nil
}

func (m *checker) EnsureClonePlugin(ctx context.Context) error {
	var installed int
	if err := m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'clone'").Scan(&installed); err != nil {
		return fmt.Errorf("check clone plugin: %w", err)
	}
	if installed > 0 {
		return nil
	}

	_, err := m.db.ExecContext(ctx, "INSTALL PLUGIN clone SONAME 'mysql_clone.so'")
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1125 {
			// Another bootstrap/setup path may have installed the plugin between
			// the INFORMATION_SCHEMA check and this statement.
			return nil
		}
		return fmt.Errorf("install clone plugin: %w", err)
	}
	return nil
}
