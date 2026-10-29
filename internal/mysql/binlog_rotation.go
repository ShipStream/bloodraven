package mysql

import (
	"context"
	"fmt"
)

// FlushBinaryLogs seals replica-era files without adding a GTID.
func (m *checker) FlushBinaryLogs(ctx context.Context) error {
	if _, err := m.db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
		return fmt.Errorf("flush binary logs: %w", err)
	}
	return nil
}
