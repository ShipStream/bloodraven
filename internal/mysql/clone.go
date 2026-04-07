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

func (m *checker) CloneInstance(ctx context.Context, user, host, password string, useSSL bool) error {
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
