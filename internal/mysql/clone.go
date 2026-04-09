package mysql

import (
	"context"
	"fmt"
)

func (m *checker) SetCloneDonorList(ctx context.Context, donor string) error {
	_, err := m.db.ExecContext(ctx, "SET GLOBAL clone_valid_donor_list = ?", donor)
	if err != nil {
		return fmt.Errorf("set clone donor list: %w", err)
	}
	return nil
}

func (m *checker) CloneInstance(ctx context.Context, user, host, password string, useSSL bool, cloneTimeoutSec int) error {
	if cloneTimeoutSec <= 0 {
		cloneTimeoutSec = 3600
	}

	// Set session-level timeouts so MySQL does not kill the connection
	// independently of the Go context deadline during long-running clones.
	timeoutStmts := []string{
		fmt.Sprintf("SET SESSION net_read_timeout = %d", cloneTimeoutSec),
		fmt.Sprintf("SET SESSION net_write_timeout = %d", cloneTimeoutSec),
		fmt.Sprintf("SET GLOBAL clone_ddl_timeout = %d", cloneTimeoutSec),
	}
	for _, s := range timeoutStmts {
		if _, err := m.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("set clone timeout (%s): %w", s, err)
		}
	}

	stmt := fmt.Sprintf("CLONE INSTANCE FROM '%s'@'%s':3306 IDENTIFIED BY '%s'",
		escapeSingleQuotes(user), escapeSingleQuotes(host), escapeSingleQuotes(password))
	if useSSL {
		stmt += " REQUIRE SSL"
	}
	// Clone may take a very long time, use the context for cancellation
	_, err := m.db.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("clone instance: %w", err)
	}
	return nil
}
