package sidecar

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// historyIntegrityProber checks the settings under which this server's
// binlogs can certify history (D-34). It returns the first violation, or
// "" when the server is clean.
type historyIntegrityProber interface {
	HistoryIntegrityViolation(ctx context.Context) (pitrarchive.HistoryViolation, string, error)
}

var _ historyIntegrityProber = (*LiveMysql)(nil)

// HistoryIntegrityViolation runs the D-34 checks in a fixed order:
// replication applier filters (per channel and global), the binlog's
// Binlog_Do_DB/Binlog_Ignore_DB, binlog_format=ROW, binlog_row_image=FULL
// and replica_preserve_commit_order=ON.
func (m *LiveMysql) HistoryIntegrityViolation(ctx context.Context) (pitrarchive.HistoryViolation, string, error) {
	for _, table := range []string{"replication_applier_filters", "replication_applier_global_filters"} {
		var n int64
		if err := m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema."+table).Scan(&n); err != nil {
			return "", "", fmt.Errorf("query %s: %w", table, err)
		}
		if n > 0 {
			return pitrarchive.ViolationReplicationFilter, fmt.Sprintf("performance_schema.%s has %d rows", table, n), nil
		}
	}
	doDB, ignoreDB, err := m.binlogFilters(ctx)
	if err != nil {
		return "", "", err
	}
	if doDB != "" || ignoreDB != "" {
		return pitrarchive.ViolationBinlogFilter, fmt.Sprintf("Binlog_Do_DB=%q Binlog_Ignore_DB=%q", doDB, ignoreDB), nil
	}
	var format, rowImage, commitOrder sql.NullString
	if err := m.db.QueryRowContext(ctx,
		"SELECT @@global.binlog_format, @@global.binlog_row_image, @@global.replica_preserve_commit_order").
		Scan(&format, &rowImage, &commitOrder); err != nil {
		return "", "", fmt.Errorf("query binlog settings: %w", err)
	}
	return historySettingsViolation(format, rowImage, commitOrder)
}

// historySettingsViolation checks the three server variables D-34 owns.
func historySettingsViolation(format, rowImage, commitOrder sql.NullString) (pitrarchive.HistoryViolation, string, error) {
	if !format.Valid || !strings.EqualFold(strings.TrimSpace(format.String), "ROW") {
		return pitrarchive.ViolationBinlogFormat, "binlog_format=" + format.String, nil
	}
	if !rowImage.Valid || !strings.EqualFold(strings.TrimSpace(rowImage.String), "FULL") {
		return pitrarchive.ViolationBinlogRowImage, "binlog_row_image=" + rowImage.String, nil
	}
	if !mysqlBool(commitOrder) {
		return pitrarchive.ViolationCommitOrder, "replica_preserve_commit_order=" + commitOrder.String, nil
	}
	return "", "", nil
}

// binlogFilters reads Binlog_Do_DB and Binlog_Ignore_DB from SHOW BINARY
// LOG STATUS, by column name.
func (m *LiveMysql) binlogFilters(ctx context.Context) (string, string, error) {
	rows, err := m.db.QueryContext(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		return "", "", fmt.Errorf("show binary log status: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", "", err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", "", err
		}
		return "", "", fmt.Errorf("show binary log status returned no row; binary logging is off")
	}
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return "", "", err
	}
	var doDB, ignoreDB string
	found := 0
	for i, c := range cols {
		switch c {
		case "Binlog_Do_DB":
			doDB, found = values[i].String, found+1
		case "Binlog_Ignore_DB":
			ignoreDB, found = values[i].String, found+1
		}
	}
	if found != 2 {
		return "", "", fmt.Errorf("show binary log status has no Binlog_Do_DB/Binlog_Ignore_DB columns")
	}
	return doDB, ignoreDB, rows.Err()
}

// unsafeHistoryError is the LastError of a marked site (§3.7).
func unsafeHistoryError(site string, marker *HistoryIntegrity) error {
	return fmt.Errorf("%s: %s %s %s; this site's binlogs cannot certify history; start a new archive prefix and take a full backup",
		pitrarchive.TokenArchiveUnsafeHistory, site, marker.Violation, marker.Detail)
}

// checkHistoryIntegrity runs at every scan on every site (D-34). A marker
// already in this site's manifest stands, whatever the server says now. A
// new violation is persisted in the site's own manifest as soon as the
// session is verified, and held in memory until then. It returns nil only
// for a clean, unmarked site; any error keeps the site from uploading.
func (a *BinlogArchiver) checkHistoryIntegrity(ctx context.Context) error {
	if a.session != nil && a.session.Verified() {
		if _, err := a.loadManifest(ctx); err != nil {
			return fmt.Errorf("history integrity: load manifest: %w", err)
		}
	}
	marker := a.currentMarker()
	if marker == nil {
		prober, ok := a.mysql.(historyIntegrityProber)
		if !ok {
			return fmt.Errorf("history integrity: the MySQL probe cannot check replication filters and binlog settings")
		}
		violation, detail, err := prober.HistoryIntegrityViolation(ctx)
		if err != nil {
			return fmt.Errorf("history integrity: %w", err)
		}
		if violation == "" {
			return nil
		}
		marker = &HistoryIntegrity{Violation: violation, Detail: detail, At: a.now().UTC().Truncate(time.Microsecond)}
		a.pendingMarker = marker
	}
	if !a.unsafeLogged {
		a.unsafeLogged = true
		a.logger.Warn("archive history unsafe", "violation", marker.Violation, "detail", marker.Detail)
	}
	if a.pendingMarker != nil && a.session != nil && a.session.Verified() {
		if err := a.persistMarker(ctx); err != nil {
			a.logger.Warn("persist history integrity marker", "error", err)
		}
	}
	a.mu.Lock()
	a.historyIntegrity = marker
	a.mu.Unlock()
	return unsafeHistoryError(a.site, marker)
}

// currentMarker is the persisted marker, else a pending one.
func (a *BinlogArchiver) currentMarker() *HistoryIntegrity {
	if a.persistedMarker != nil {
		return a.persistedMarker
	}
	return a.pendingMarker
}

// persistMarker writes the pending marker unless the manifest already has
// one; the first marker stands (set once, never cleared).
func (a *BinlogArchiver) persistMarker(ctx context.Context) error {
	m, err := a.loadManifest(ctx)
	if err != nil {
		return err
	}
	if m.HistoryIntegrity == nil {
		marker := *a.pendingMarker
		m.HistoryIntegrity = &marker
		if err := a.saveManifest(ctx, m); err != nil {
			return err
		}
	}
	a.persistedMarker = m.HistoryIntegrity
	a.pendingMarker = nil
	return nil
}
